mod common;

const FILE_SIZE: usize = 50 * 1024 * 1024; // 50 MB
const WRITE_SIZE: usize = 500 * 1024 * 1024; // 500 MB for write benchmarks

#[tokio::test]
async fn test_bench() {
    let test_filename = format!("bench_{}.bin", std::process::id());
    let expected = common::generate_pattern(FILE_SIZE);

    let (token, bucket_id, hub) =
        match common::setup_bucket_with_file("bench", &test_filename, &expected).await {
            Some(cfg) => cfg,
            None => return,
        };

    let pid = std::process::id();

    // --- FUSE benchmark (read + write) ---
    let fuse_mount = format!("/tmp/hf-bench-fuse-{}", pid);
    let fuse_cache = format!("/tmp/hf-bench-fuse-cache-{}", pid);

    let fuse_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &fuse_mount, &fuse_cache, &[]);
        let r = common::bench::run_read_benchmarks(&fuse_mount, &test_filename, &expected);

        // Write benchmark: 50 MB sequential write
        let write_result = common::bench::run_write_benchmark(&fuse_mount, WRITE_SIZE);

        common::unmount(&fuse_mount, child, 60);
        (r, write_result)
    }));

    std::fs::remove_dir_all(&fuse_mount).ok();
    std::fs::remove_dir_all(&fuse_cache).ok();

    // --- NFS benchmark (read only) ---
    let nfs_mount = format!("/tmp/hf-bench-nfs-{}", pid);
    let nfs_cache = format!("/tmp/hf-bench-nfs-cache-{}", pid);

    let nfs_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(
            &bucket_id,
            &nfs_mount,
            &nfs_cache,
            &["--backend=nfs", "--read-only"],
        );
        let r = common::bench::run_read_benchmarks(&nfs_mount, &test_filename, &expected);
        common::unmount_nfs(&nfs_mount, child, 5);
        r
    }));

    std::fs::remove_dir_all(&nfs_mount).ok();
    std::fs::remove_dir_all(&nfs_cache).ok();

    // --- Raw cleaner benchmark (no FUSE, no kernel) ---
    let raw_mbps = {
        let write_config = common::build_write_config(&hub).await;
        let download_session = data::FileDownloadSession::new(write_config.clone(), None, None)
            .await
            .expect("download session");
        let xet_sessions = hf_mount::xet::XetSessions::new(download_session, Some(write_config));
        match common::bench::run_raw_cleaner_benchmark(&xet_sessions, WRITE_SIZE).await {
            Ok(mbps) => Some(mbps),
            Err(e) => {
                eprintln!("Raw cleaner bench error: {}", e);
                None
            }
        }
    };

    // Cleanup bucket
    common::delete_bucket(common::ENDPOINT, &token, &bucket_id).await;

    // --- Extract results ---
    let (fuse, write) = match fuse_result {
        Ok((Ok(r), w)) => (Some(r), w),
        Ok((Err(e), _)) => {
            eprintln!("FUSE bench error: {}", e);
            (None, Err(e))
        }
        Err(e) => std::panic::resume_unwind(e),
    };

    let nfs = match nfs_result {
        Ok(Ok(r)) => Some(r),
        Ok(Err(e)) => {
            eprintln!("NFS bench error: {}", e);
            None
        }
        Err(e) => std::panic::resume_unwind(e),
    };

    // --- Print comparison table ---
    eprintln!("\n============================================================");
    eprintln!("  FUSE vs NFS Benchmark — 50 MB file");
    eprintln!("============================================================");
    eprintln!("  {:30} {:>12} {:>12}", "Metric", "FUSE", "NFS");
    eprintln!("  {:-<30} {:-<12} {:-<12}", "", "", "");

    if let (Some(f), Some(n)) = (&fuse, &nfs) {
        eprintln!(
            "  {:30} {:>9.1} MB/s {:>9.1} MB/s",
            "Sequential read", f.seq_read_mbps, n.seq_read_mbps
        );
        eprintln!(
            "  {:30} {:>9.1} MB/s {:>9.1} MB/s",
            "Sequential re-read", f.seq_reread_mbps, n.seq_reread_mbps
        );
        eprintln!(
            "  {:30} {:>9.1} ms   {:>9.1} ms",
            "Range read (1MB@25MB)", f.range_read_ms, n.range_read_ms
        );
        eprintln!(
            "  {:30} {:>9.1} ms   {:>9.1} ms",
            "Random reads (100x4KB avg)", f.random_avg_ms, n.random_avg_ms
        );
        let f_speedup = if f.neg_cache_second_ms > 0.0 {
            f.neg_cache_first_ms / f.neg_cache_second_ms
        } else {
            f64::INFINITY
        };
        let n_speedup = if n.neg_cache_second_ms > 0.0 {
            n.neg_cache_first_ms / n.neg_cache_second_ms
        } else {
            f64::INFINITY
        };
        eprintln!(
            "  {:30} {:>9.1}x     {:>9.1}x",
            "Neg cache speedup", f_speedup, n_speedup
        );
    }

    if let Ok(ref w) = write {
        eprintln!(
            "  {:30} {:>9.1} MB/s",
            "Sequential write (500MB)", w.write_mbps
        );
        eprintln!(
            "  {:30} {:>9.3} s",
            "Close latency (CAS+Hub)", w.close_secs
        );
        eprintln!(
            "  {:30} {:>9.1} MB/s",
            "Write end-to-end", w.total_mbps
        );
        eprintln!(
            "  {:30} {:>9.1} MB/s",
            "Dedup write (same data)", w.dedup_write_mbps
        );
        eprintln!(
            "  {:30} {:>9.3} s",
            "Dedup close latency", w.dedup_close_secs
        );
        eprintln!(
            "  {:30} {:>9.1} MB/s",
            "Dedup end-to-end", w.dedup_total_mbps
        );
    } else {
        eprintln!("  {:30} {:>12}", "Write benchmark", "FAIL");
    }
    if let Some(raw) = raw_mbps {
        eprintln!(
            "  {:30} {:>9.1} MB/s",
            "Raw add_data() (no FUSE)", raw
        );
    }
    eprintln!("============================================================\n");

    assert!(fuse.is_some(), "FUSE benchmark failed");
    assert!(nfs.is_some(), "NFS benchmark failed");
    write.expect("Write benchmark failed");
}
