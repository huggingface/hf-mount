mod common;

use std::io::{Read, Seek, SeekFrom};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ENDPOINT: &str = "https://huggingface.co";
const FILE_SIZE: usize = 50 * 1024 * 1024; // 50 MB

#[tokio::test]
async fn test_bench() {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping bench: HF_TOKEN not set");
            return;
        }
    };

    let username = common::whoami(ENDPOINT, &token).await;
    let bucket_id = format!("{}/hf-mount-bench-{}", username, std::process::id());

    common::create_bucket(ENDPOINT, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = std::sync::Arc::new(hf_mount::hub_api::HubApiClient::new(ENDPOINT, &token));

    // Generate and upload 50 MB test file
    let test_filename = format!("bench_{}.bin", std::process::id());
    let expected = common::generate_pattern(FILE_SIZE);

    let tmp_dir = std::env::temp_dir().join("hf-mount-bench-setup");
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

    let mount_point = format!("/tmp/hf-mount-bench-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-bench-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &[]);
        let r = run_benchmarks(&mount_point, &test_filename, &expected);
        common::unmount(&mount_point, child, 5);
        r
    }));

    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Benchmark failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

fn run_benchmarks(
    mount_point: &str,
    test_filename: &str,
    expected: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file_path = format!("{}/{}", mount_point, test_filename);
    let size_mb = FILE_SIZE as f64 / (1024.0 * 1024.0);

    eprintln!("\n=== hf-mount benchmark ({:.0} MB file) ===\n", size_mb);

    // 1. Sequential read
    {
        let t = Instant::now();
        let data = std::fs::read(&file_path)?;
        let elapsed = t.elapsed();
        let mbps = size_mb / elapsed.as_secs_f64();
        assert_eq!(data.len(), expected.len(), "size mismatch");
        assert!(data == expected, "content mismatch on sequential read");
        eprintln!(
            "Sequential read:       {:6.1} MB/s  ({:.2}s, checksum OK)",
            mbps,
            elapsed.as_secs_f64()
        );
    }

    // 2. Sequential re-read (fresh file handle → new PrefetchState)
    {
        let t = Instant::now();
        let data = std::fs::read(&file_path)?;
        let elapsed = t.elapsed();
        let mbps = size_mb / elapsed.as_secs_f64();
        assert_eq!(data.len(), expected.len());
        eprintln!(
            "Sequential re-read:    {:6.1} MB/s  ({:.2}s)",
            mbps,
            elapsed.as_secs_f64()
        );
    }

    // 3. Range read: seek to 25 MB, read 1 MB
    {
        let offset = 25 * 1024 * 1024_usize;
        let read_size = 1024 * 1024_usize;
        let mut f = std::fs::File::open(&file_path)?;
        let t = Instant::now();
        f.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = vec![0u8; read_size];
        f.read_exact(&mut buf)?;
        let elapsed = t.elapsed();
        assert!(
            common::verify_pattern(&buf, offset),
            "range read content mismatch at offset {}",
            offset
        );
        eprintln!(
            "Range read (1MB@25MB): {:6.1} ms    (verified)",
            elapsed.as_secs_f64() * 1000.0
        );
    }

    // 4. Random reads: 100x 4KB at pseudo-random offsets
    {
        let read_size = 4096_usize;
        let max_offset = FILE_SIZE - read_size;
        // Simple LCG for deterministic pseudo-random offsets
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
        let avg_ms = total.as_secs_f64() * 1000.0 / 100.0;
        eprintln!(
            "Random reads (100x4KB):{:6.1} ms avg ({:.2}s total)",
            avg_ms,
            total.as_secs_f64()
        );
    }

    // 5. Negative cache: stat 20 nonexistent paths twice
    {
        let fake_paths: Vec<String> = (0..20)
            .map(|i| format!("{}/nonexistent_file_{}.txt", mount_point, i))
            .collect();

        let t1 = Instant::now();
        for p in &fake_paths {
            let _ = std::fs::metadata(p);
        }
        let first = t1.elapsed();

        let t2 = Instant::now();
        for p in &fake_paths {
            let _ = std::fs::metadata(p);
        }
        let second = t2.elapsed();

        let speedup = if second.as_nanos() > 0 {
            first.as_secs_f64() / second.as_secs_f64()
        } else {
            f64::INFINITY
        };
        eprintln!(
            "Neg cache (20 paths):  1st={:.1}ms  2nd={:.1}ms  speedup={:.1}x",
            first.as_secs_f64() * 1000.0,
            second.as_secs_f64() * 1000.0,
            speedup
        );
    }

    // 6. Write + readback (1 MB)
    {
        let write_size = 1024 * 1024;
        let write_data = common::generate_pattern(write_size);
        let write_path = format!("{}/bench_write_test.bin", mount_point);

        std::fs::write(&write_path, &write_data)?;
        let read_back = std::fs::read(&write_path)?;
        assert_eq!(read_back, write_data, "write+readback content mismatch");
        eprintln!("Write + readback (1MB): OK");
    }

    eprintln!("\n=== benchmark complete ===\n");
    Ok(())
}
