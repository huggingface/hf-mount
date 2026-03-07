mod common;

use std::io::Read;
use std::time::Instant;

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

// ── Safetensor concurrent pread benchmark ───────────────────────────

const SAFETENSOR_REPO: &str = "openai-community/gpt2";
const SAFETENSOR_FILE: &str = "model.safetensors";
const SAFETENSOR_THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

/// Parse safetensor header → Vec<(name, offset, size)>.
fn parse_safetensor_header(path: &str) -> Vec<(String, u64, usize)> {
    let mut f = std::fs::File::open(path).expect("open safetensor file");
    let mut header_len_buf = [0u8; 8];
    f.read_exact(&mut header_len_buf).expect("read header length");
    let header_len = u64::from_le_bytes(header_len_buf) as usize;
    let mut header_buf = vec![0u8; header_len];
    f.read_exact(&mut header_buf).expect("read header");
    let header: serde_json::Value = serde_json::from_slice(&header_buf).expect("parse header JSON");
    let data_offset = 8 + header_len as u64;
    let mut tensors = Vec::new();
    if let Some(obj) = header.as_object() {
        for (name, info) in obj {
            if name == "__metadata__" {
                continue;
            }
            if let Some(offsets) = info.get("data_offsets").and_then(|v| v.as_array()) {
                let start = offsets[0].as_u64().unwrap();
                let end = offsets[1].as_u64().unwrap();
                let size = (end - start) as usize;
                if size > 0 {
                    tensors.push((name.clone(), data_offset + start, size));
                }
            }
        }
    }
    tensors.sort_by_key(|t| t.1);
    tensors
}

/// Read all tensors via pread with N threads on a shared fd.
/// Uses dynamic dispatch (work-stealing) so all threads advance through the
/// file together, matching the pattern of real safetensor loaders.
fn bench_pread_tensors(path: &str, tensors: &[(String, u64, usize)], num_threads: usize) -> (f64, u64) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let fd = std::fs::File::open(path).expect("open safetensor file");
    let fd_raw = std::os::unix::io::AsRawFd::as_raw_fd(&fd);
    let next = AtomicUsize::new(0);

    let t0 = Instant::now();
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for _ in 0..num_threads {
            handles.push(s.spawn(|| {
                let mut total = 0u64;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= tensors.len() {
                        break;
                    }
                    let offset = tensors[i].1;
                    let size = tensors[i].2;
                    let mut buf = vec![0u8; size];
                    let n = unsafe { libc::pread(fd_raw, buf.as_mut_ptr() as *mut libc::c_void, size, offset as i64) };
                    assert!(n > 0, "pread failed at offset {}", offset);
                    total += n as u64;
                }
                total
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).sum::<u64>()
    });
    let elapsed = t0.elapsed().as_secs_f64();
    let total: u64 = tensors.iter().map(|t| t.2 as u64).sum();
    (elapsed, total)
}

/// Mount openai-community/gpt2 and benchmark concurrent safetensor reads
/// with 1, 2, 4, and 8 threads using pread (shared fd).
#[tokio::test]
async fn test_safetensor_bench() {
    if std::env::var("HF_TOKEN").is_err() {
        eprintln!("Skipping: HF_TOKEN not set");
        return;
    }

    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-bench-safetensor-{}", pid);
    let cache_dir = format!("/tmp/hf-bench-safetensor-cache-{}", pid);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_repo(SAFETENSOR_REPO, &mount_point, &cache_dir, &["--read-only"]);

        let safetensor_path = format!("{}/{}", mount_point, SAFETENSOR_FILE);
        let file_size = std::fs::metadata(&safetensor_path).expect("stat safetensor").len();
        let tensors = parse_safetensor_header(&safetensor_path);
        let total_tensor_bytes: u64 = tensors.iter().map(|t| t.2 as u64).sum();

        eprintln!();
        eprintln!("============================================================");
        eprintln!("  Safetensor Benchmark -- {}", SAFETENSOR_REPO);
        eprintln!(
            "  File: {} ({:.1} MB, {} tensors, {:.1} MB data)",
            SAFETENSOR_FILE,
            file_size as f64 / 1e6,
            tensors.len(),
            total_tensor_bytes as f64 / 1e6
        );
        eprintln!("------------------------------------------------------------");
        eprintln!("  {:>8} {:>10} {:>10}", "Threads", "Wall (s)", "MB/s");
        eprintln!("  {:-<8} {:-<10} {:-<10}", "", "", "");

        // Clear xorb cache dir between thread counts for cold reads.
        let xorbs_dir = format!("{}/xorbs", cache_dir);
        for &n in SAFETENSOR_THREAD_COUNTS {
            // Drop kernel page cache
            #[cfg(target_os = "linux")]
            {
                let _ = std::process::Command::new("sudo")
                    .args(["sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"])
                    .status();
            }
            std::fs::remove_dir_all(&xorbs_dir).ok();
            std::thread::sleep(std::time::Duration::from_millis(500));

            let (elapsed, total) = bench_pread_tensors(&safetensor_path, &tensors, n);
            let mbps = total as f64 / 1e6 / elapsed;
            eprintln!("  {:>8} {:>10.3} {:>10.1}", n, elapsed, mbps);
        }
        eprintln!("============================================================");
        eprintln!();

        common::unmount(&mount_point, child, 10);
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
