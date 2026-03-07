// Integration test: parallel pread scaling on a real FUSE mount.
//
// Simulates the safetensors / transformers access pattern:
//   - One fd shared across N threads
//   - Each thread calls pread() at its own strided offset
//
// Run on EC2 with cold cache for accurate results:
//   echo 3 | sudo tee /proc/sys/vm/drop_caches
//   rm -rf <cache_dir>/xorbs/
//   cargo test --release --test pread_scaling -- --nocapture

mod common;

use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::Instant;

const FILE_SIZE: usize = 200 * 1024 * 1024; // 200 MB
const READ_CHUNK: usize = 1024 * 1024; // 1 MB per pread (matches typical safetensors shard)

/// Run N threads sharing one fd, each reading the whole file in strides.
/// Returns aggregate MB/s.
fn run_parallel_pread(file: Arc<std::fs::File>, file_size: usize, n_threads: usize) -> f64 {
    let mut handles = Vec::new();
    let start = Instant::now();

    for t in 0..n_threads {
        let f = file.clone();
        handles.push(std::thread::spawn(move || {
            let stride = READ_CHUNK * n_threads;
            let mut offset = t * READ_CHUNK;
            let mut total = 0usize;
            let mut buf = vec![0u8; READ_CHUNK];
            while offset < file_size {
                let n = READ_CHUNK.min(file_size - offset);
                let got = f.read_at(&mut buf[..n], offset as u64).expect("pread failed");
                total += got;
                offset += stride;
            }
            total
        }));
    }

    let total_bytes: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let elapsed = start.elapsed();
    let mbps = total_bytes as f64 / 1_048_576.0 / elapsed.as_secs_f64();
    eprintln!(
        "  {} thread(s): {:.0} MB in {:.2}s = {:.1} MB/s",
        n_threads,
        total_bytes as f64 / 1_048_576.0,
        elapsed.as_secs_f64(),
        mbps,
    );
    mbps
}

#[tokio::test]
async fn test_parallel_pread_scaling() {
    let filename = format!("pread_scale_{}.bin", std::process::id());
    let data = common::generate_pattern(FILE_SIZE);

    let (token, bucket_id, hub) = match common::setup_bucket_with_file("pread-scale", &filename, &data).await {
        Some(cfg) => cfg,
        None => return, // HF_TOKEN not set
    };

    let mount_point = format!("/tmp/hf-pread-scale-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-pread-scale-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--read-only"]);

        let file_path = format!("{}/{}", mount_point, filename);
        let file = Arc::new(std::fs::File::open(&file_path).expect("open file"));

        eprintln!("\n=== Parallel pread scaling ({} MB) ===", FILE_SIZE / (1024 * 1024));
        eprintln!("NOTE: for accurate cold-cache numbers:");
        eprintln!("  echo 3 | sudo tee /proc/sys/vm/drop_caches");
        eprintln!("  rm -rf {}/xorbs/", cache_dir);

        let mbps_1 = run_parallel_pread(file.clone(), FILE_SIZE, 1);
        let mbps_2 = run_parallel_pread(file.clone(), FILE_SIZE, 2);
        let mbps_4 = run_parallel_pread(file.clone(), FILE_SIZE, 4);

        eprintln!("\nScaling vs 1 thread:");
        eprintln!("  2 threads: {:.2}x", mbps_2 / mbps_1);
        eprintln!("  4 threads: {:.2}x", mbps_4 / mbps_1);

        // Pre-fix: 2 threads → ~0.03x (30x serial collapse)
        // Post-fix: each thread gets its own slot → ≥ 0.3x even on cold cache
        assert!(
            mbps_2 / mbps_1 >= 0.3,
            "REGRESSION: 2-thread pread {:.1} MB/s is <30% of 1-thread {:.1} MB/s ({:.3}x). \
             Reads on a shared fd are serialised by the prefetch mutex.",
            mbps_2,
            mbps_1,
            mbps_2 / mbps_1,
        );

        common::unmount(&mount_point, child, 60);
    }));

    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(()) => {}
        Err(e) => std::panic::resume_unwind(e),
    }
}
