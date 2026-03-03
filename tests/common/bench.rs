use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

type BenchError = Box<dyn std::error::Error + Send + Sync>;

pub struct BenchResult {
    pub seq_read_mbps: f64,
    pub seq_reread_mbps: f64,
    pub range_read_ms: f64,
    pub random_avg_ms: f64,
    pub neg_cache_first_ms: f64,
    pub neg_cache_second_ms: f64,
}

pub struct WriteBenchResult {
    pub size_mb: f64,
    pub write_mbps: f64,
    pub close_secs: f64,
    pub total_mbps: f64,
    /// Second write of identical data (dedup should make close near-instant).
    pub dedup_write_mbps: f64,
    pub dedup_close_secs: f64,
    pub dedup_total_mbps: f64,
    /// Raw add_data() throughput without FUSE overhead.
    pub raw_add_data_mbps: f64,
}

/// Run the standard read benchmark suite on a mounted filesystem.
/// Returns structured results for comparison or printing.
pub fn run_read_benchmarks(mount_point: &str, test_filename: &str, expected: &[u8]) -> Result<BenchResult, BenchError> {
    let file_path = format!("{}/{}", mount_point, test_filename);
    let file_size = expected.len();
    let size_mb = file_size as f64 / (1024.0 * 1024.0);

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
        assert!(super::verify_pattern(&buf, offset), "range read content mismatch");
    }

    // 4. Random reads: 100x 4KB
    let random_avg_ms;
    {
        let read_size = 4096_usize;
        let max_offset = file_size - read_size;
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
                super::verify_pattern(&buf, offset),
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

/// Write benchmark: create a file of `size_bytes` via 1 MB chunks, measure
/// write throughput and close latency (close = CAS finalize + Hub commit).
pub fn run_write_benchmark(mount_point: &str, size_bytes: usize) -> Result<WriteBenchResult, BenchError> {
    let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
    let path = format!("{}/bench_write_{}.bin", mount_point, std::process::id());
    let chunk = super::generate_pattern(1024 * 1024); // 1 MB chunk

    // Write phase: measure wall time for all write() calls
    let mut f = std::fs::File::create(&path)?;
    let write_start = Instant::now();
    let mut remaining = size_bytes;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        f.write_all(&chunk[..n])?;
        remaining -= n;
    }
    let write_elapsed = write_start.elapsed();

    // Close phase: flush + drop triggers CAS finalize + Hub commit
    let close_start = Instant::now();
    drop(f);
    let close_elapsed = close_start.elapsed();

    let total = write_elapsed + close_elapsed;
    let write_mbps = size_mb / write_elapsed.as_secs_f64();
    let total_mbps = size_mb / total.as_secs_f64();

    // Verify size
    let meta = std::fs::metadata(&path)?;
    assert_eq!(meta.len(), size_bytes as u64, "written file size mismatch");

    // Second write: identical data → CAS dedup should make close near-instant
    let path2 = format!("{}/bench_write_dedup_{}.bin", mount_point, std::process::id());
    let mut f2 = std::fs::File::create(&path2)?;
    let write2_start = Instant::now();
    let mut remaining = size_bytes;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        f2.write_all(&chunk[..n])?;
        remaining -= n;
    }
    let write2_elapsed = write2_start.elapsed();

    let close2_start = Instant::now();
    drop(f2);
    let close2_elapsed = close2_start.elapsed();

    let total2 = write2_elapsed + close2_elapsed;

    Ok(WriteBenchResult {
        size_mb,
        write_mbps,
        close_secs: close_elapsed.as_secs_f64(),
        total_mbps,
        dedup_write_mbps: size_mb / write2_elapsed.as_secs_f64(),
        dedup_close_secs: close2_elapsed.as_secs_f64(),
        dedup_total_mbps: size_mb / total2.as_secs_f64(),
        raw_add_data_mbps: 0.0, // filled by run_raw_cleaner_benchmark
    })
}

/// Measure raw SingleFileCleaner::add_data() throughput — no FUSE, no kernel.
/// This is the theoretical ceiling for streaming writes.
pub async fn run_raw_cleaner_benchmark(xet: &hf_mount::xet::XetSessions, size_bytes: usize) -> Result<f64, BenchError> {
    let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
    let chunk = super::generate_pattern(1024 * 1024);

    let mut writer = xet.create_streaming_writer().await.map_err(|e| e.to_string())?;

    let start = Instant::now();
    let mut remaining = size_bytes;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        writer.write(&chunk[..n]).await.map_err(|e| e.to_string())?;
        remaining -= n;
    }
    let elapsed = start.elapsed();
    let mbps = size_mb / elapsed.as_secs_f64();

    // Finish to clean up the session
    let _ = writer.finish().await;

    Ok(mbps)
}
