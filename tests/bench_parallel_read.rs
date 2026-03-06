mod common;

use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Barrier};
use std::time::Instant;

/// Number of random reads each thread performs.
const READS_PER_THREAD: usize = 50;
/// Size of each random read (1 MiB — typical tensor chunk).
const READ_SIZE: usize = 1_048_576;
/// File size: 256 MiB.
const FILE_SIZE: usize = 256 * 1024 * 1024;
/// Thread counts to benchmark.
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

/// Generate deterministic "random" offsets for a given thread index.
/// Offsets are spread across the file to simulate safetensor-like access.
fn generate_offsets(thread_idx: usize, count: usize, file_size: usize, read_size: usize) -> Vec<u64> {
    let max_offset = (file_size - read_size) as u64;
    let mut state: u64 = 42u64.wrapping_add(thread_idx as u64 * 7919);
    (0..count)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            // Align to 4 KiB for realistic I/O
            (state % max_offset) & !0xFFF
        })
        .collect()
}

#[tokio::test]
async fn bench_parallel_read() {
    // --- Setup: upload a 256 MiB file ---
    let (token, bucket_id, hub) = match common::setup_bucket("parbench").await {
        Some(cfg) => cfg,
        None => return,
    };

    let filename = "bench_256m.bin";
    let data = common::generate_pattern(FILE_SIZE);
    let write_config = common::build_write_config(&hub).await;

    let tmp_dir = std::env::temp_dir().join("hf-mount-parbench-setup");
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging_path = tmp_dir.join(filename);
    std::fs::write(&staging_path, &data).expect("write staging file");

    let file_info = common::upload_file(write_config, &staging_path).await;
    let xet_hash = file_info.hash().to_string();
    eprintln!("Uploaded {}: xet_hash={}", filename, xet_hash);

    let mtime_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
        path: filename.to_string(),
        xet_hash,
        mtime: mtime_ms,
        content_type: None,
    }])
    .await
    .expect("batch add failed");

    std::fs::remove_dir_all(&tmp_dir).ok();

    // --- Mount ---
    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-parbench-{}", pid);
    let cache_dir = format!("/tmp/hf-parbench-cache-{}", pid);
    let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--read-only"]);

    let file_path = format!("{}/{}", mount_point, filename);

    // --- Benchmark: parallel random reads with different thread counts ---
    eprintln!("\n============================================================");
    eprintln!("  Parallel Random Read Benchmark");
    eprintln!(
        "  File: {} MiB, Read size: {} KiB, Reads/thread: {}",
        FILE_SIZE / (1024 * 1024),
        READ_SIZE / 1024,
        READS_PER_THREAD
    );
    eprintln!("------------------------------------------------------------");
    eprintln!(
        "  {:>8}  {:>12}  {:>12}  {:>12}",
        "Threads", "Wall (s)", "MB/s", "Scaling"
    );
    eprintln!("  {:->8}  {:->12}  {:->12}  {:->12}", "", "", "", "");

    let mut baseline_mbps = 0.0_f64;

    for &n_threads in THREAD_COUNTS {
        // Drop kernel page cache between runs (best-effort, needs sudo)
        let _ = std::process::Command::new("sudo")
            .args(["sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"])
            .status();

        // Small pause to let things settle
        std::thread::sleep(std::time::Duration::from_millis(500));

        let elapsed = run_parallel_reads(&file_path, n_threads);

        let total_bytes = n_threads * READS_PER_THREAD * READ_SIZE;
        let mbps = (total_bytes as f64) / (1024.0 * 1024.0) / elapsed;

        if n_threads == 1 {
            baseline_mbps = mbps;
        }
        let scaling = if baseline_mbps > 0.0 { mbps / baseline_mbps } else { 1.0 };

        eprintln!(
            "  {:>8}  {:>12.3}  {:>9.1} MB/s  {:>11.2}x",
            n_threads, elapsed, mbps, scaling
        );
    }

    eprintln!("============================================================\n");

    // --- Cleanup ---
    common::unmount(&mount_point, child, 5);
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();
    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;
}

/// Run parallel random reads from `n_threads` threads on the SAME file descriptor.
/// All threads share one fd (= one FUSE file handle = one PrefetchState mutex).
/// This reproduces the safetensor loading pattern where PyTorch threads pread() on a shared fd.
/// Returns wall-clock seconds for all threads to complete.
fn run_parallel_reads(file_path: &str, n_threads: usize) -> f64 {
    let barrier = Arc::new(Barrier::new(n_threads + 1));
    // Open the file ONCE — all threads share this fd
    let file = std::fs::File::open(file_path).expect("open failed");
    let shared_fd = file.as_raw_fd();
    let mut handles = Vec::new();

    for thread_idx in 0..n_threads {
        let barrier = barrier.clone();

        handles.push(std::thread::spawn(move || {
            let fd = shared_fd;
            let offsets = generate_offsets(thread_idx, READS_PER_THREAD, FILE_SIZE, READ_SIZE);

            // Wait for all threads to be ready
            barrier.wait();

            for &offset in &offsets {
                let mut buf = vec![0u8; READ_SIZE];
                // SAFETY: fd is valid, buf is correctly sized
                let n = unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut libc::c_void, READ_SIZE, offset as i64) };
                assert!(
                    n > 0,
                    "pread failed at offset {}: errno={}",
                    offset,
                    std::io::Error::last_os_error()
                );
                let n = n as usize;
                assert!(
                    common::verify_pattern(&buf[..n], offset as usize),
                    "data mismatch at offset {} (thread {})",
                    offset,
                    thread_idx,
                );
            }
        }));
    }

    // Start all threads simultaneously
    barrier.wait();
    let start = Instant::now();

    for h in handles {
        h.join().expect("thread panicked");
    }

    start.elapsed().as_secs_f64()
}
