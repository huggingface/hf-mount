use std::io::{Read, Seek, SeekFrom};
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

/// Run the standard read benchmark suite on a mounted filesystem.
/// Returns structured results for comparison or printing.
pub fn run_read_benchmarks(
    mount_point: &str,
    test_filename: &str,
    expected: &[u8],
) -> Result<BenchResult, BenchError> {
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
        assert!(
            super::verify_pattern(&buf, offset),
            "range read content mismatch"
        );
    }

    // 4. Random reads: 100x 4KB
    let random_avg_ms;
    {
        let read_size = 4096_usize;
        let max_offset = file_size - read_size;
        let mut rng_state: u64 = 42;
        let mut total = Duration::ZERO;
        for _ in 0..100 {
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1);
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
