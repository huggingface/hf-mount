mod common;

const BENCH_SIZES: &[(usize, &str)] = &[
    (50 * 1024 * 1024, "50MB"),
    (200 * 1024 * 1024, "200MB"),
    (500 * 1024 * 1024, "500MB"),
];

#[tokio::test]
async fn test_bench() {
    // Upload files of each size to the same bucket
    let (token, bucket_id, hub) = match common::setup_bucket("bench").await {
        Some(cfg) => cfg,
        None => return,
    };

    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for &(size, label) in BENCH_SIZES {
        let filename = format!("bench_{}.bin", label);
        let data = common::generate_pattern(size);
        let write_config = common::build_write_config(&hub).await;

        let tmp_dir = std::env::temp_dir().join(format!("hf-mount-bench-setup-{}", label));
        std::fs::create_dir_all(&tmp_dir).ok();
        let staging_path = tmp_dir.join(&filename);
        std::fs::write(&staging_path, &data).expect("write staging file");

        let file_info = common::upload_file(write_config, &staging_path).await;
        let xet_hash = file_info.hash().to_string();
        eprintln!("Uploaded {}: xet_hash={}", label, xet_hash);

        let mtime_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        hub.batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
            path: filename.clone(),
            xet_hash,
            mtime: mtime_ms,
            content_type: None,
        }])
        .await
        .expect("batch add failed");

        std::fs::remove_dir_all(&tmp_dir).ok();
        files.push((filename, data));
    }

    let pid = std::process::id();

    // --- FUSE benchmark (read + write per size) ---
    let fuse_mount = format!("/tmp/hf-bench-fuse-{}", pid);
    let fuse_cache = format!("/tmp/hf-bench-fuse-cache-{}", pid);

    let fuse_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &fuse_mount, &fuse_cache, &[]);

        let mut reads = Vec::new();
        for (filename, expected) in &files {
            reads.push(common::bench::run_read_benchmarks(&fuse_mount, filename, expected));
        }

        let mut writes = Vec::new();
        for &(size, _) in BENCH_SIZES {
            writes.push(common::bench::run_write_benchmark(&fuse_mount, size));
        }

        common::unmount(&fuse_mount, child, 60);
        (reads, writes)
    }));

    std::fs::remove_dir_all(&fuse_mount).ok();
    std::fs::remove_dir_all(&fuse_cache).ok();

    // --- NFS benchmark (read only) ---
    let nfs_mount = format!("/tmp/hf-bench-nfs-{}", pid);
    let nfs_cache = format!("/tmp/hf-bench-nfs-cache-{}", pid);

    let nfs_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket_nfs(&bucket_id, &nfs_mount, &nfs_cache, &["--read-only"]);
        let mut reads = Vec::new();
        for (filename, expected) in &files {
            reads.push(common::bench::run_read_benchmarks(&nfs_mount, filename, expected));
        }
        common::unmount_nfs(&nfs_mount, child, 5);
        reads
    }));

    std::fs::remove_dir_all(&nfs_mount).ok();
    std::fs::remove_dir_all(&nfs_cache).ok();

    // Cleanup
    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;

    // --- Extract results ---
    let (fuse_reads, fuse_writes) = match fuse_result {
        Ok((reads, writes)) => (reads, writes),
        Err(e) => std::panic::resume_unwind(e),
    };

    let nfs_reads = match nfs_result {
        Ok(reads) => reads,
        Err(e) => std::panic::resume_unwind(e),
    };

    // --- Print tables per size ---
    for (i, &(_, label)) in BENCH_SIZES.iter().enumerate() {
        let fuse = fuse_reads[i].as_ref().ok();
        let nfs = nfs_reads[i].as_ref().ok();
        let write = fuse_writes[i].as_ref().ok();

        eprintln!("\n============================================================");
        eprintln!("  Benchmark — {}", label);
        eprintln!("------------------------------------------------------------");
        eprintln!("  {:30} {:>12} {:>12}", "Metric", "FUSE", "NFS");
        eprintln!("  {:-<30} {:-<12} {:-<12}", "", "", "");

        if let (Some(f), Some(n)) = (fuse, nfs) {
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
        } else {
            if fuse.is_none() {
                eprintln!("  FUSE read: FAILED");
            }
            if nfs.is_none() {
                eprintln!("  NFS read: FAILED");
            }
        }

        if let Some(w) = write {
            eprintln!("  {:30} {:>9.1} MB/s", "Sequential write (FUSE)", w.write_mbps);
            eprintln!("  {:30} {:>9.3} s", "Close latency (CAS+Hub)", w.close_secs);
            eprintln!("  {:30} {:>9.1} MB/s", "Write end-to-end", w.total_mbps);
            eprintln!("  {:30} {:>9.1} MB/s", "Dedup write", w.dedup_write_mbps);
            eprintln!("  {:30} {:>9.3} s", "Dedup close latency", w.dedup_close_secs);
            eprintln!("  {:30} {:>9.1} MB/s", "Dedup end-to-end", w.dedup_total_mbps);
        } else {
            eprintln!("  Write: FAILED");
        }
        eprintln!("============================================================");
    }
    eprintln!();

    // Assertions
    for (i, &(_, label)) in BENCH_SIZES.iter().enumerate() {
        assert!(fuse_reads[i].is_ok(), "FUSE read benchmark failed for {}", label);
        assert!(nfs_reads[i].is_ok(), "NFS read benchmark failed for {}", label);
        assert!(fuse_writes[i].is_ok(), "Write benchmark failed for {}", label);
    }
}
