mod common;

use std::io::{Read, Seek, SeekFrom};
use std::process::{Child, Command};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ENDPOINT: &str = "https://huggingface.co";
const FILE_SIZE: usize = 50 * 1024 * 1024; // 50 MB

/// Mount with --backend=nfs --read-only.
fn mount_nfs_bucket(bucket_id: &str, mount_point: &str, cache_dir: &str) -> Child {
    common::mount_bucket(bucket_id, mount_point, cache_dir, &["--backend=nfs", "--read-only"])
}

/// Unmount NFS (uses umount instead of fusermount).
fn unmount_nfs(mount_point: &str, mut child: Child, graceful_secs: u64) {
    let _ = Command::new("umount").arg(mount_point).status();

    for _ in 0..graceful_secs {
        if let Ok(Some(status)) = child.try_wait() {
            eprintln!("hf-mount exited: {}", status);
            return;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    child.kill().ok();
    match child.wait() {
        Ok(status) => eprintln!("hf-mount killed: {}", status),
        Err(e) => eprintln!("wait error: {}", e),
    }
}

struct BenchResult {
    seq_read_mbps: f64,
    seq_reread_mbps: f64,
    range_read_ms: f64,
    random_avg_ms: f64,
    neg_cache_first_ms: f64,
    neg_cache_second_ms: f64,
}

fn run_read_benchmarks(
    mount_point: &str,
    test_filename: &str,
    expected: &[u8],
) -> Result<BenchResult, Box<dyn std::error::Error + Send + Sync>> {
    let file_path = format!("{}/{}", mount_point, test_filename);
    let size_mb = FILE_SIZE as f64 / (1024.0 * 1024.0);

    // 1. Sequential read (cold)
    let seq_read_mbps;
    {
        let t = Instant::now();
        let data = std::fs::read(&file_path)?;
        let elapsed = t.elapsed();
        seq_read_mbps = size_mb / elapsed.as_secs_f64();
        assert_eq!(data.len(), expected.len(), "size mismatch");
        assert!(data == expected, "content mismatch on sequential read");
    }

    // 2. Sequential re-read
    let seq_reread_mbps;
    {
        let t = Instant::now();
        let data = std::fs::read(&file_path)?;
        let elapsed = t.elapsed();
        seq_reread_mbps = size_mb / elapsed.as_secs_f64();
        assert_eq!(data.len(), expected.len());
    }

    // 3. Range read: 1MB at 25MB
    let range_read_ms;
    {
        let offset = 25 * 1024 * 1024_usize;
        let read_size = 1024 * 1024_usize;
        let mut f = std::fs::File::open(&file_path)?;
        let t = Instant::now();
        f.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = vec![0u8; read_size];
        f.read_exact(&mut buf)?;
        range_read_ms = t.elapsed().as_secs_f64() * 1000.0;
        assert!(common::verify_pattern(&buf, offset), "range read content mismatch");
    }

    // 4. Random reads: 100x 4KB
    let random_avg_ms;
    {
        let read_size = 4096_usize;
        let max_offset = FILE_SIZE - read_size;
        let mut rng_state: u64 = 42;
        let mut total = Duration::ZERO;
        for _ in 0..100 {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let offset = (rng_state % max_offset as u64) as usize;

            let mut f = std::fs::File::open(&file_path)?;
            let t = Instant::now();
            f.seek(SeekFrom::Start(offset as u64))?;
            let mut buf = vec![0u8; read_size];
            f.read_exact(&mut buf)?;
            total += t.elapsed();

            assert!(
                common::verify_pattern(&buf, offset),
                "random read mismatch at offset {}",
                offset
            );
        }
        random_avg_ms = total.as_secs_f64() * 1000.0 / 100.0;
    }

    // 5. Negative cache
    let neg_cache_first_ms;
    let neg_cache_second_ms;
    {
        let fake_paths: Vec<String> = (0..20)
            .map(|i| format!("{}/nonexistent_file_{}.txt", mount_point, i))
            .collect();

        let t1 = Instant::now();
        for p in &fake_paths {
            let _ = std::fs::metadata(p);
        }
        neg_cache_first_ms = t1.elapsed().as_secs_f64() * 1000.0;

        let t2 = Instant::now();
        for p in &fake_paths {
            let _ = std::fs::metadata(p);
        }
        neg_cache_second_ms = t2.elapsed().as_secs_f64() * 1000.0;
    }

    Ok(BenchResult {
        seq_read_mbps,
        seq_reread_mbps,
        range_read_ms,
        random_avg_ms,
        neg_cache_first_ms,
        neg_cache_second_ms,
    })
}

#[tokio::test]
async fn test_bench_compare() {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping bench: HF_TOKEN not set");
            return;
        }
    };

    let username = common::whoami(ENDPOINT, &token).await;
    let bucket_id = format!("{}/hf-mount-bench-cmp-{}", username, std::process::id());

    common::create_bucket(ENDPOINT, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = std::sync::Arc::new(hf_mount::hub_api::HubApiClient::new(ENDPOINT, &token));

    // Generate and upload 50 MB test file
    let test_filename = format!("bench_{}.bin", std::process::id());
    let expected = common::generate_pattern(FILE_SIZE);

    let tmp_dir = std::env::temp_dir().join("hf-mount-bench-cmp-setup");
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging_path = tmp_dir.join(&test_filename);
    std::fs::write(&staging_path, &expected).expect("write staging file");

    eprintln!("Uploading {} MB file...", FILE_SIZE / 1024 / 1024);
    let write_config = common::build_write_config(&hub, &bucket_id).await;
    let file_info = common::upload_file(write_config, &staging_path).await;
    let xet_hash = file_info.hash().to_string();
    eprintln!("Uploaded: xet_hash={}, size={}", xet_hash, file_info.file_size());

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(
        &bucket_id,
        &[hf_mount::hub_api::BatchOp::AddFile {
            path: test_filename.clone(),
            xet_hash,
            mtime: mtime_ms,
            content_type: None,
        }],
    )
    .await
    .expect("batch add failed");

    std::fs::remove_dir_all(&tmp_dir).ok();

    // --- FUSE benchmark ---
    let fuse_mount = format!("/tmp/hf-bench-cmp-fuse-{}", std::process::id());
    let fuse_cache = format!("/tmp/hf-bench-cmp-fuse-cache-{}", std::process::id());

    let fuse_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &fuse_mount, &fuse_cache, &["--read-only"]);
        let r = run_read_benchmarks(&fuse_mount, &test_filename, &expected);
        common::unmount(&fuse_mount, child, 5);
        r
    }));

    std::fs::remove_dir_all(&fuse_mount).ok();
    std::fs::remove_dir_all(&fuse_cache).ok();

    // --- NFS benchmark ---
    let nfs_mount = format!("/tmp/hf-bench-cmp-nfs-{}", std::process::id());
    let nfs_cache = format!("/tmp/hf-bench-cmp-nfs-cache-{}", std::process::id());

    let nfs_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = mount_nfs_bucket(&bucket_id, &nfs_mount, &nfs_cache);
        let r = run_read_benchmarks(&nfs_mount, &test_filename, &expected);
        unmount_nfs(&nfs_mount, child, 5);
        r
    }));

    std::fs::remove_dir_all(&nfs_mount).ok();
    std::fs::remove_dir_all(&nfs_cache).ok();

    // Cleanup bucket
    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;

    // --- Print results ---
    let fuse = match fuse_result {
        Ok(Ok(r)) => Some(r),
        Ok(Err(e)) => {
            eprintln!("FUSE bench error: {}", e);
            None
        }
        Err(e) => {
            eprintln!("FUSE bench panicked");
            std::panic::resume_unwind(e);
        }
    };

    let nfs = match nfs_result {
        Ok(Ok(r)) => Some(r),
        Ok(Err(e)) => {
            eprintln!("NFS bench error: {}", e);
            None
        }
        Err(e) => {
            eprintln!("NFS bench panicked");
            std::panic::resume_unwind(e);
        }
    };

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

    eprintln!("============================================================\n");

    assert!(fuse.is_some(), "FUSE benchmark failed");
    assert!(nfs.is_some(), "NFS benchmark failed");
}
