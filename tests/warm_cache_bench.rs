/// Benchmark: xorb reconstruction cache warm-up benefit on sequential reads.
///
/// Each open() reads the full file and reports:
///   TTFB  — time to first byte (blocked on warm_reconstruction_cache)
///   MB/s  — end-to-end sequential throughput including TTFB
///
/// Open #1 = cold: FUSE process fetches the reconstruction plan from CAS.
/// Open #2+ = warm: in-process CachedXetClient cache hit, plan already stored.
///
/// Run on EC2:
///   cargo test --release --test warm_cache_bench -- --nocapture
mod common;

use std::io::Read;
use std::time::Instant;

const FILE_SIZE: usize = 100 * 1024 * 1024; // 100 MB
const N_OPENS: usize = 5;
const READ_CHUNK: usize = 4 * 1024 * 1024; // 4 MB per read syscall

#[tokio::test]
async fn bench_xorb_reconstruction_cache() {
    let (token, bucket_id, hub) = match common::setup_bucket("warm-cache-bench").await {
        Some(cfg) => cfg,
        None => return,
    };

    let filename = "seq_100mb.bin";
    let data = common::generate_pattern(FILE_SIZE);
    let write_config = common::build_write_config(&hub).await;

    let tmp = std::env::temp_dir().join(format!("hf-warm-bench-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).ok();
    let staging = tmp.join(filename);
    std::fs::write(&staging, &data).expect("write staging");

    let file_info = common::upload_file(write_config, &staging).await;
    let xet_hash = file_info.hash().to_string();
    eprintln!(
        "Uploaded {} ({} MB), xet_hash={}",
        filename,
        FILE_SIZE / (1024 * 1024),
        xet_hash
    );
    std::fs::remove_dir_all(&tmp).ok();

    hub.batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
        path: filename.to_string(),
        xet_hash,
        mtime: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        content_type: None,
    }])
    .await
    .expect("batch add");

    let pid = std::process::id();
    let mount = format!("/tmp/hf-warm-bench-{}", pid);
    // Use /dev/shm (tmpfs) on Linux for RAM-speed cache reads, falling back to /tmp.
    let shm_dir = format!("/dev/shm/hf-warm-cache-{}", pid);
    let cache = if cfg!(target_os = "linux") && std::fs::create_dir_all(&shm_dir).is_ok() {
        shm_dir
    } else {
        format!("/tmp/hf-warm-cache-{}", pid)
    };

    eprintln!("Cache dir: {}", cache);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount, &cache, &["--read-only"]);
        let file_path = format!("{}/{}", mount, filename);
        let size_mb = FILE_SIZE as f64 / (1024.0 * 1024.0);

        eprintln!(
            "\n=== Xorb reconstruction cache: sequential read ({} MB) ===",
            FILE_SIZE / (1024 * 1024)
        );
        eprintln!(
            "  {:>6}  {:>10}  {:>10}  {:>10}  {}",
            "Open#", "TTFB (ms)", "Total (s)", "MB/s", "cache"
        );
        eprintln!("  {:-<6}  {:-<10}  {:-<10}  {:-<10}  {:-<5}", "", "", "", "", "");

        let mut results: Vec<(f64, f64, f64)> = Vec::new(); // (ttfb_ms, total_s, mbps)
        let mut buf = vec![0u8; READ_CHUNK];

        for i in 0..N_OPENS {
            let t0 = Instant::now();
            let mut f = std::fs::File::open(&file_path).expect("open");

            // First read: blocks until warm_reconstruction_cache completes (cold = CAS round-trip)
            let n = f.read(&mut buf).expect("first read");
            let ttfb = t0.elapsed();
            assert!(n > 0, "empty first read");
            assert_eq!(buf[0], 0u8, "data mismatch at open {}", i + 1);

            // Drain the rest
            let mut total = n;
            loop {
                let n = f.read(&mut buf).expect("seq read");
                if n == 0 {
                    break;
                }
                total += n;
            }
            let elapsed = t0.elapsed();
            assert_eq!(total, FILE_SIZE, "size mismatch at open {}", i + 1);

            let ttfb_ms = ttfb.as_secs_f64() * 1000.0;
            let total_s = elapsed.as_secs_f64();
            let mbps = size_mb / total_s;
            let label = if i == 0 { "cold" } else { "warm" };

            eprintln!(
                "  {:>6}  {:>10.1}  {:>10.2}  {:>10.1}  {}",
                i + 1,
                ttfb_ms,
                total_s,
                mbps,
                label,
            );
            results.push((ttfb_ms, total_s, mbps));
        }

        common::unmount(&mount, child, 10);
        results
    }));

    std::fs::remove_dir_all(&mount).ok();
    std::fs::remove_dir_all(&cache).ok();
    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;

    let results = match result {
        Ok(r) => r,
        Err(e) => std::panic::resume_unwind(e),
    };

    let (cold_ttfb, _, cold_mbps) = results[0];
    let warm_ttfb_avg = results[1..].iter().map(|(t, _, _)| t).sum::<f64>() / (results.len() - 1) as f64;
    let warm_mbps_avg = results[1..].iter().map(|(_, _, m)| m).sum::<f64>() / (results.len() - 1) as f64;

    eprintln!(
        "\nSummary: cold TTFB {:.0} ms / {:.1} MB/s  →  warm TTFB {:.0} ms / {:.1} MB/s",
        cold_ttfb, cold_mbps, warm_ttfb_avg, warm_mbps_avg,
    );
}
