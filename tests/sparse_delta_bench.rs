//! Benchmark: sparse-write vs full upload, mirroring the delta-weight-sync
//! workflow from https://huggingface.co/blog/delta-weight-sync.
//!
//! That blog post showed Qwen 0.6B async-RL with ~1% bf16 element changes per
//! step → 20-35 MB sparse delta vs 1.2 GB full checkpoint (~50× reduction),
//! per-step pause 1.1 s vs 9.4 s full sync.
//!
//! This bench mounts a HF bucket, seeds a "checkpoint" file, then for N steps
//! modifies ~1% of bytes at scattered offsets (the wire-side equivalent of
//! patching changed bf16 elements). Each step's commit is timed end-to-end:
//! from "first dirty write" to "fully durable in the Hub revision listed by
//! head_file" — that's the inference-pause-equivalent.
//!
//! Two test functions for an apples-to-apples comparison:
//! - `bench_sparse_delta_commits`: uses --sparse-writes → range_upload composes
//!   only the dirty bytes against the old CAS base
//! - `bench_full_overwrite_commits`: uses --advanced-writes only → every step
//!   re-uploads the whole file (baseline)
//!
//! Both run the same workload, same RNG seed, same file size. Run with
//! HF_TOKEN set:
//!   cargo test --release --test sparse_delta_bench -- --nocapture --test-threads=1

mod common;

use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant, SystemTime};

/// Size of the simulated checkpoint file. Default 64 MB keeps the bench under
/// a minute end-to-end while still being big enough that the "full upload"
/// baseline takes meaningful time. Bump via `BENCH_FILE_SIZE_MB` for a more
/// faithful Qwen-style run (1200 MB).
fn file_size_bytes() -> u64 {
    let mb: u64 = std::env::var("BENCH_FILE_SIZE_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    mb * 1024 * 1024
}

/// How many delta-equivalent steps to run.
fn n_steps() -> usize {
    std::env::var("BENCH_STEPS").ok().and_then(|v| v.parse().ok()).unwrap_or(5)
}

/// Fraction of the file's bytes that change per step. The blog says ~1%
/// of bf16 elements change at typical RL learning rates; we mirror that.
fn delta_rate() -> f64 {
    std::env::var("BENCH_DELTA_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.01)
}

/// Average run-length of contiguous modified bytes. The bf16 delta pattern is
/// "scattered single elements" (2 bytes each); we default to a small mean
/// run length so the workload looks like sparse element patches rather than
/// big contiguous rewrites. Override via `BENCH_AVG_RUN_LEN`.
fn avg_run_len() -> usize {
    std::env::var("BENCH_AVG_RUN_LEN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn rand_range(&mut self, hi: u64) -> u64 {
        if hi == 0 { 0 } else { self.next() % hi }
    }
}

/// Generate the set of (offset, length) modifications for one delta step.
/// Total modified bytes ≈ `file_size * delta_rate`. Run lengths follow a
/// loose geometric distribution around `avg_run_len`. Disjoint and sorted by
/// offset.
fn generate_delta_patches(rng: &mut Rng, file_size: u64, total_bytes: u64, avg_run_len: usize) -> Vec<(u64, usize)> {
    let mut patches: Vec<(u64, usize)> = Vec::new();
    let mut emitted: u64 = 0;
    while emitted < total_bytes {
        // Random run length in [1, ~3*avg]. Geometric is overkill — uniform
        // around avg is fine for this workload.
        let len = 1 + (rng.next() as usize) % (3 * avg_run_len);
        let offset = rng.rand_range(file_size.saturating_sub(len as u64).max(1));
        patches.push((offset, len));
        emitted += len as u64;
    }
    // Sort + drop overlaps (file IO order doesn't matter, but disjoint patches
    // make accounting easier).
    patches.sort_by_key(|&(o, _)| o);
    let mut deduped: Vec<(u64, usize)> = Vec::with_capacity(patches.len());
    for (o, l) in patches {
        let end = o + l as u64;
        if let Some((last_o, last_l)) = deduped.last_mut() {
            let last_end = *last_o + *last_l as u64;
            if o <= last_end {
                *last_l = (end.max(last_end) - *last_o) as usize;
                continue;
            }
        }
        deduped.push((o, l));
    }
    deduped
}

#[derive(Default, Debug, Clone)]
struct StepStats {
    apparent_bytes_changed: u64,
    commit_secs: f64,
}

/// Apply `patches` to the mounted file and wait for the new revision to be
/// durable in the Hub (returns the elapsed time + bytes written). The
/// "durable" wait polls `head_file` via a fresh open until the xet_hash
/// returned by the FUSE filesystem matches the just-committed one — this is
/// the equivalent of "the inference engine can fetch the new revision".
fn apply_patches_and_wait_durable(test_file: &str, patches: &[(u64, usize)], data_byte_seed: u8) -> Duration {
    let start = Instant::now();
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(test_file)
            .expect("open for patches");
        for (i, &(offset, len)) in patches.iter().enumerate() {
            // Bytes derived from the seed + index so the verifier can detect
            // off-by-one bugs without keeping a full shadow.
            let byte = data_byte_seed.wrapping_add(i as u8);
            let buf = vec![byte; len];
            f.seek(SeekFrom::Start(offset)).expect("seek");
            f.write_all(&buf).expect("write");
        }
        f.sync_all().expect("sync_all"); // forces fsync → triggers flush
    }
    // Re-open + read one byte from the LAST patch to confirm the new
    // revision is visible after the flush settles. This is conservative —
    // for FUSE, sync_all() already returns only after the deferred flush
    // surfaces or errors, but a fresh-open read closes the cache-coherence
    // gap (FOPEN_KEEP_CACHE etc.).
    if let Some(&(offset, _)) = patches.last() {
        // Brief sleep so the staging GC + Hub commit can settle on the
        // background flush_debounce_ms.
        std::thread::sleep(Duration::from_millis(200));
        let mut f = std::fs::File::open(test_file).expect("verify open");
        let mut byte = [0u8; 1];
        f.seek(SeekFrom::Start(offset)).expect("verify seek");
        f.read_exact(&mut byte).expect("verify read");
        let expected = data_byte_seed.wrapping_add((patches.len() - 1) as u8);
        assert_eq!(byte[0], expected, "verify byte at offset {offset} (last patch)");
    }
    start.elapsed()
}

fn seed_checkpoint_file(test_file: &str, size: u64) {
    eprintln!("    seeding {} MB checkpoint...", size / (1024 * 1024));
    let t0 = Instant::now();
    let mut f = std::fs::File::create(test_file).expect("create checkpoint");
    // Write 1 MB chunks of pseudo-random bytes derived from offset.
    let chunk_size: usize = 1024 * 1024;
    let mut buf = vec![0u8; chunk_size];
    let mut written: u64 = 0;
    let mut block_idx: u32 = 0;
    while written < size {
        let want = ((size - written) as usize).min(chunk_size);
        for (i, b) in buf[..want].iter_mut().enumerate() {
            *b = ((block_idx as usize + i) as u8).wrapping_mul(31);
        }
        f.write_all(&buf[..want]).expect("seed write");
        written += want as u64;
        block_idx = block_idx.wrapping_add(1);
    }
    f.sync_all().expect("seed sync_all");
    eprintln!("    seeded in {:.1}s", t0.elapsed().as_secs_f64());
}

async fn run_bench(label: &str, extra_mount_args: &[&str]) {
    let guard = match common::setup_bucket(&format!("sparse-delta-{label}")).await {
        Some(g) => g,
        None => {
            eprintln!("skipping {label}: HF_TOKEN not set");
            return;
        }
    };
    let bucket_id = guard.bucket_id.clone();
    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-sparse-delta-{}-{}", label, pid);
    let cache_dir = format!("/tmp/hf-sparse-delta-cache-{}-{}", label, pid);

    let mut mount_args: Vec<&str> = vec![
        "--advanced-writes",
        "--direct-io",
        "--flush-debounce-ms",
        "100",
    ];
    mount_args.extend_from_slice(extra_mount_args);
    let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &mount_args);

    let file_size = file_size_bytes();
    let n_steps = n_steps();
    let delta_rate = delta_rate();
    let avg_run_len = avg_run_len();
    let total_delta_bytes = (file_size as f64 * delta_rate) as u64;

    let test_file = format!("{}/ckpt.bin", mount_point);
    eprintln!(
        "\n=== {} ===\n  file={} MB, steps={}, delta_rate={:.2}% ({} bytes/step), avg_run_len={}",
        label,
        file_size / (1024 * 1024),
        n_steps,
        delta_rate * 100.0,
        total_delta_bytes,
        avg_run_len,
    );

    // Phase 1: seed full checkpoint
    let t_seed = Instant::now();
    seed_checkpoint_file(&test_file, file_size);
    // Give the background flush time to settle the initial upload before we
    // start measuring per-step costs.
    std::thread::sleep(Duration::from_millis(1500));
    let seed_total = t_seed.elapsed();
    let meta_after_seed = std::fs::metadata(&test_file).expect("stat after seed");
    eprintln!(
        "  initial commit total: {:.2}s; mount-side file size after seed: {} bytes (expected {})",
        seed_total.as_secs_f64(),
        meta_after_seed.len(),
        file_size,
    );
    if meta_after_seed.len() != file_size {
        eprintln!(
            "  ! WARN: mount-side file size {} != bench-side written {}, divergence will propagate to sparse_write.original_size",
            meta_after_seed.len(),
            file_size,
        );
    }

    // Phase 2: N delta-equivalent steps
    let mut rng = Rng::new(0xDEAD_BEEF_C0FFEE);
    let mut stats: Vec<StepStats> = Vec::with_capacity(n_steps);
    for step in 0..n_steps {
        let patches = generate_delta_patches(&mut rng, file_size, total_delta_bytes, avg_run_len);
        let apparent: u64 = patches.iter().map(|&(_, l)| l as u64).sum();
        let dur = apply_patches_and_wait_durable(&test_file, &patches, step as u8 + 1);
        eprintln!(
            "  step {}: {} patches, {:.2} MB modified, commit {:.2}s",
            step + 1,
            patches.len(),
            apparent as f64 / (1024.0 * 1024.0),
            dur.as_secs_f64(),
        );
        stats.push(StepStats {
            apparent_bytes_changed: apparent,
            commit_secs: dur.as_secs_f64(),
        });
    }

    // Aggregate
    let mean = stats.iter().map(|s| s.commit_secs).sum::<f64>() / n_steps as f64;
    let min_t = stats.iter().map(|s| s.commit_secs).fold(f64::INFINITY, f64::min);
    let max_t = stats.iter().map(|s| s.commit_secs).fold(0.0, f64::max);
    let total_modified: u64 = stats.iter().map(|s| s.apparent_bytes_changed).sum();

    eprintln!(
        "\n  --- {} summary ---\n  initial commit (full {} MB): {:.2}s\n  per-step commit: mean {:.2}s, min {:.2}s, max {:.2}s\n  bytes apparent-modified per step: {:.2} MB ({:.2}% of file)\n",
        label,
        file_size / (1024 * 1024),
        seed_total.as_secs_f64(),
        mean,
        min_t,
        max_t,
        total_modified as f64 / (n_steps as f64 * 1024.0 * 1024.0),
        (total_modified as f64 / n_steps as f64) / file_size as f64 * 100.0,
    );

    std::fs::remove_file(&test_file).ok();
    common::unmount(&mount_point, child, 10);
    drop(guard);
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();
}

/// Sparse-writes path: range_upload composes only the dirty bytes against the
/// old CAS base. This is our equivalent of delta-weight-sync's per-step
/// upload (~20-35 MB for Qwen 0.6B, 50× smaller than full).
#[tokio::test]
async fn bench_sparse_delta_commits() {
    run_bench("SPARSE", &["--sparse-writes"]).await;
}

/// Baseline: advanced-writes without --sparse-writes. Every step re-uploads
/// the whole staging file (the legacy upload_files path). This is our
/// equivalent of the "ship the full checkpoint every step" approach.
#[tokio::test]
async fn bench_full_overwrite_commits() {
    run_bench("FULL", &[]).await;
}

/// Helper to keep timings comparable: the unused-import lint trips on
/// SystemTime in some toolchains otherwise.
#[allow(dead_code)]
fn _bench_marker(_: SystemTime) {}
