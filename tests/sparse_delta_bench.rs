//! Benchmark: hf-mount sparse-writes vs full overwrite for the per-step
//! commit pattern from https://huggingface.co/blog/delta-weight-sync.
//!
//! The blog post showed Qwen 0.6B async-RL with ~1% bf16 element changes
//! per training step: 20-35 MB sparse delta vs 1.2 GB full checkpoint (~50×
//! reduction), per-step pause 1.1 s vs 9.4 s. They achieve this by
//! BATCHING the changed elements into a single sparse-safetensors blob
//! (one `(indices, values)` pair per tensor) and uploading that as a
//! separate `deltas/step_NNN.safetensors` file each step.
//!
//! This bench mirrors that batched pattern — the realistic application
//! workflow — instead of the pathological "issue 6M individual 2-byte
//! pwrites" worst case. Three variants:
//!
//! - `bench_sparse_delta_blob`: sparse-write mount, app writes a single
//!   contiguous delta-sized blob in place each step. range_upload composes
//!   exactly 1 dirty range against the old CAS base. This is what an app
//!   following the blog's pattern would actually do if it kept the
//!   checkpoint file canonical and patched it through hf-mount.
//!
//! - `bench_full_overwrite`: advanced-writes only (no sparse), app writes
//!   the SAME blob each step but the legacy upload re-uploads the full
//!   file. Baseline for the "ship the whole checkpoint every step"
//!   approach the blog compares against.
//!
//! - `bench_sparse_delta_scattered`: sparse-write mount, app does N tiny
//!   scattered pwrites (the pathological pattern). Kept as a stress test
//!   for the upload_ranges compose path, NOT a realistic workflow — N tiny
//!   inserts hit a quadratic cost in xet-core's window planner. Disabled
//!   by default; enable with `BENCH_RUN_SCATTERED=1`.
//!
//! All variants run the same total-modified-bytes per step (`BENCH_DELTA_RATE`
//! × file size). Run with HF_TOKEN set:
//!
//!   cargo test --release --test sparse_delta_bench -- --nocapture --test-threads=1

mod common;

use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

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
    std::env::var("BENCH_STEPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

/// Fraction of the file's bytes that change per step. The blog says ~1%
/// of bf16 elements change at typical RL learning rates; we mirror that.
fn delta_rate() -> f64 {
    std::env::var("BENCH_DELTA_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.01)
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

/// Generate a contiguous delta blob's offset for one step. Random offset so
/// successive steps don't dedupe trivially against each other. Returns
/// `(offset, len)`.
fn blob_for_step(rng: &mut Rng, file_size: u64, blob_len: u64) -> (u64, u64) {
    let offset = rng.rand_range(file_size.saturating_sub(blob_len).max(1));
    (offset, blob_len)
}

/// Generate N scattered tiny patches summing to `total_bytes`. Used only by
/// the stress variant.
fn scattered_patches(rng: &mut Rng, file_size: u64, total_bytes: u64) -> Vec<(u64, usize)> {
    const AVG_RUN: usize = 8;
    let mut patches: Vec<(u64, usize)> = Vec::new();
    let mut emitted: u64 = 0;
    while emitted < total_bytes {
        let len = 1 + (rng.next() as usize) % (3 * AVG_RUN);
        let offset = rng.rand_range(file_size.saturating_sub(len as u64).max(1));
        patches.push((offset, len));
        emitted += len as u64;
    }
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
    bytes_modified: u64,
    commit_secs: f64,
}

/// Apply a single contiguous blob write at `offset` of length `len` and time
/// the round-trip to "verified durable on the mount" (close+reopen+read).
fn apply_blob_and_wait_durable(test_file: &str, offset: u64, len: u64, data_seed: u8) -> Duration {
    let start = Instant::now();
    let buf: Vec<u8> = (0..len).map(|i| data_seed.wrapping_add(i as u8)).collect();
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(test_file)
            .expect("open for blob");
        f.seek(SeekFrom::Start(offset)).expect("seek");
        f.write_all(&buf).expect("write blob");
        f.sync_all().expect("sync_all");
    }
    // Verify the new bytes are visible via a fresh read handle. Brief sleep so
    // the debounced background flush can apply_commit_sparse and clear dirty
    // before we re-open.
    std::thread::sleep(Duration::from_millis(200));
    {
        let mut f = std::fs::File::open(test_file).expect("verify open");
        let mut head = [0u8; 16];
        let probe_len = head.len().min(len as usize);
        f.seek(SeekFrom::Start(offset)).expect("verify seek");
        f.read_exact(&mut head[..probe_len]).expect("verify read");
        for (i, b) in head[..probe_len].iter().enumerate() {
            assert_eq!(*b, data_seed.wrapping_add(i as u8), "verify byte mismatch at +{i}");
        }
    }
    start.elapsed()
}

/// Apply N scattered tiny patches and time the same round-trip.
fn apply_scattered_and_wait_durable(test_file: &str, patches: &[(u64, usize)], data_seed: u8) -> Duration {
    let start = Instant::now();
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(test_file)
            .expect("open for scattered");
        for (i, &(offset, len)) in patches.iter().enumerate() {
            let byte = data_seed.wrapping_add(i as u8);
            let buf = vec![byte; len];
            f.seek(SeekFrom::Start(offset)).expect("seek");
            f.write_all(&buf).expect("write");
        }
        f.sync_all().expect("sync_all");
    }
    if let Some(&(offset, _)) = patches.last() {
        std::thread::sleep(Duration::from_millis(200));
        let mut f = std::fs::File::open(test_file).expect("verify open");
        let mut byte = [0u8; 1];
        f.seek(SeekFrom::Start(offset)).expect("verify seek");
        f.read_exact(&mut byte).expect("verify read");
        let expected = data_seed.wrapping_add((patches.len() - 1) as u8);
        assert_eq!(byte[0], expected, "verify byte at offset {offset}");
    }
    start.elapsed()
}

fn seed_checkpoint_file(test_file: &str, size: u64) {
    eprintln!("    seeding {} MB checkpoint...", size / (1024 * 1024));
    let t0 = Instant::now();
    let mut f = std::fs::File::create(test_file).expect("create checkpoint");
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

#[derive(Copy, Clone)]
enum Workload {
    Blob,
    Scattered,
}

async fn run_bench(label: &str, extra_mount_args: &[&str], workload: Workload) {
    // Force sparse engagement at any file size for the SPARSE_* variants.
    // The bench default of 64 MiB sits below the production 256 MiB
    // threshold, so without this override `--sparse-writes` would silently
    // fall back to the non-sparse path and the SPARSE_* labels would be
    // measuring the same code as FULL. Set unconditionally: the env var has
    // no effect on runs that don't also pass `--sparse-writes`, so the FULL
    // baseline is unaffected.
    //
    // SAFETY: bench setup mutates env before spawning the mount child.
    // cargo test parallelizes within a binary, but all readers (mount
    // children) see HF_MOUNT_SPARSE_MIN_BYTES=0 at spawn time and the
    // value never changes across tests in this binary.
    unsafe {
        std::env::set_var("HF_MOUNT_SPARSE_MIN_BYTES", "0");
    }

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

    let mut mount_args: Vec<&str> = vec!["--advanced-writes", "--direct-io", "--flush-debounce-ms", "100"];
    mount_args.extend_from_slice(extra_mount_args);
    let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &mount_args);

    let file_size = file_size_bytes();
    let n_steps = n_steps();
    let delta_rate = delta_rate();
    let delta_bytes = (file_size as f64 * delta_rate) as u64;
    let test_file = format!("{}/ckpt.bin", mount_point);

    eprintln!(
        "\n=== {} ===\n  file={} MB, steps={}, delta_rate={:.2}% ({} bytes/step)",
        label,
        file_size / (1024 * 1024),
        n_steps,
        delta_rate * 100.0,
        delta_bytes,
    );

    // Phase 1: seed the full checkpoint and wait for the initial upload to
    // settle so per-step timings don't see seed-upload contention.
    let t_seed = Instant::now();
    seed_checkpoint_file(&test_file, file_size);
    std::thread::sleep(Duration::from_millis(1500));
    let seed_total = t_seed.elapsed();
    let post_seed_size = std::fs::metadata(&test_file).expect("stat after seed").len();
    eprintln!(
        "  initial commit total: {:.2}s; mount-side size after seed: {} bytes (expected {})",
        seed_total.as_secs_f64(),
        post_seed_size,
        file_size,
    );
    assert_eq!(post_seed_size, file_size, "post-seed size must match");

    // Phase 2: N delta-style commits
    let mut rng = Rng::new(0x00DE_ADBE_EFC0_FFEE);
    let mut stats: Vec<StepStats> = Vec::with_capacity(n_steps);
    for step in 0..n_steps {
        let dur;
        let bytes_modified;
        match workload {
            Workload::Blob => {
                let (offset, len) = blob_for_step(&mut rng, file_size, delta_bytes);
                dur = apply_blob_and_wait_durable(&test_file, offset, len, step as u8 + 1);
                bytes_modified = len;
                eprintln!(
                    "  step {}: 1 blob, offset={} {:.2} MB, commit {:.2}s",
                    step + 1,
                    offset,
                    len as f64 / (1024.0 * 1024.0),
                    dur.as_secs_f64(),
                );
            }
            Workload::Scattered => {
                let patches = scattered_patches(&mut rng, file_size, delta_bytes);
                let apparent: u64 = patches.iter().map(|&(_, l)| l as u64).sum();
                dur = apply_scattered_and_wait_durable(&test_file, &patches, step as u8 + 1);
                bytes_modified = apparent;
                eprintln!(
                    "  step {}: {} patches, {:.2} MB modified, commit {:.2}s",
                    step + 1,
                    patches.len(),
                    apparent as f64 / (1024.0 * 1024.0),
                    dur.as_secs_f64(),
                );
            }
        }
        stats.push(StepStats {
            bytes_modified,
            commit_secs: dur.as_secs_f64(),
        });
    }

    let mean = stats.iter().map(|s| s.commit_secs).sum::<f64>() / n_steps as f64;
    let min_t = stats.iter().map(|s| s.commit_secs).fold(f64::INFINITY, f64::min);
    let max_t = stats.iter().map(|s| s.commit_secs).fold(0.0, f64::max);
    let total_modified: u64 = stats.iter().map(|s| s.bytes_modified).sum();

    eprintln!(
        "\n  --- {} summary ---\n  initial commit (full {} MB): {:.2}s\n  per-step commit: mean {:.2}s, min {:.2}s, max {:.2}s\n  bytes modified per step: {:.2} MB ({:.2}% of file)\n",
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

/// Sparse-writes + one contiguous blob per step. Mirrors the realistic
/// delta-weight-sync workflow: the app encodes its delta into one blob and
/// writes it to a stable region of the checkpoint file. `range_upload`
/// composes 1 dirty range against the old CAS base — the path our sparse
/// design is optimized for.
#[tokio::test]
async fn bench_sparse_delta_blob() {
    run_bench("SPARSE_BLOB", &["--sparse-writes"], Workload::Blob).await;
}

/// Baseline: advanced-writes only, same blob workload. Every step
/// re-uploads the full file via `upload_files`. This is our equivalent of
/// shipping the whole checkpoint every step.
#[tokio::test]
async fn bench_full_overwrite() {
    run_bench("FULL", &[], Workload::Blob).await;
}

/// Stress variant: sparse-writes with N tiny scattered patches (the
/// pathological pattern where every changed bf16 element becomes its own
/// pwrite). Currently hits a quadratic cost in xet-core's `upload_ranges`
/// window planner — kept as a regression marker rather than a realistic
/// workflow. Off by default because a single step can take 10+ minutes.
#[tokio::test]
async fn bench_sparse_delta_scattered() {
    if std::env::var("BENCH_RUN_SCATTERED").ok().as_deref() != Some("1") {
        eprintln!("skipping bench_sparse_delta_scattered (set BENCH_RUN_SCATTERED=1 to enable)");
        return;
    }
    run_bench("SPARSE_SCATTERED", &["--sparse-writes"], Workload::Scattered).await;
}

/// Cold-cache scenario: seed the file on one mount, unmount + wipe the
/// local cache, then remount fresh and time the first commit. This is
/// where sparse-write's wire-side win actually shows: the full-overwrite
/// path has to download the entire checkpoint into staging before it can
/// upload the modified version, while sparse just punches a hole and
/// composes against the existing CAS reconstruction.
///
/// Real-world equivalent: an async-RL inference replica spinning up on a
/// fresh node, pulling a checkpoint that's already on the Hub.
async fn run_cold_bench(label: &str, extra_mount_args: &[&str]) {
    // Same rationale as `run_bench`: override the production 256 MiB
    // threshold so the bench's default 64 MiB file actually engages sparse
    // when the caller passes `--sparse-writes`. No-op for FULL_COLD which
    // doesn't pass the flag.
    //
    // SAFETY: env mutation in bench setup, before any mount child reads it.
    unsafe {
        std::env::set_var("HF_MOUNT_SPARSE_MIN_BYTES", "0");
    }

    let guard = match common::setup_bucket(&format!("sparse-cold-{label}")).await {
        Some(g) => g,
        None => {
            eprintln!("skipping {label}: HF_TOKEN not set");
            return;
        }
    };
    let bucket_id = guard.bucket_id.clone();
    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-sparse-cold-{}-{}", label, pid);
    let cache_dir_seed = format!("/tmp/hf-sparse-cold-cache-seed-{}-{}", label, pid);
    let cache_dir_bench = format!("/tmp/hf-sparse-cold-cache-bench-{}-{}", label, pid);
    let file_size = file_size_bytes();
    let delta_rate = delta_rate();
    let delta_bytes = (file_size as f64 * delta_rate) as u64;

    eprintln!(
        "\n=== {} (cold cache) ===\n  file={} MB, delta_rate={:.2}% ({} bytes)",
        label,
        file_size / (1024 * 1024),
        delta_rate * 100.0,
        delta_bytes,
    );

    // Mount #1: seed the file, push it to CAS, unmount cleanly.
    {
        let mut mount_args: Vec<&str> = vec!["--advanced-writes", "--direct-io", "--flush-debounce-ms", "100"];
        mount_args.extend_from_slice(extra_mount_args);
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir_seed, &mount_args);
        let test_file = format!("{}/ckpt.bin", mount_point);
        let t_seed = Instant::now();
        seed_checkpoint_file(&test_file, file_size);
        // Wait for the deferred flush to commit the seed to the Hub before
        // unmounting (otherwise the bucket has no file for the remount to
        // open against).
        std::thread::sleep(Duration::from_millis(2500));
        eprintln!("  seed committed in {:.2}s", t_seed.elapsed().as_secs_f64());
        common::unmount(&mount_point, child, 10);
        std::fs::remove_dir_all(&cache_dir_seed).ok();
    }

    // Mount #2: cold cache. The remote file exists on the Hub but nothing
    // is cached locally — sparse path can punch a hole and compose; the
    // non-sparse path must download the whole file first.
    let mut mount_args: Vec<&str> = vec!["--advanced-writes", "--direct-io", "--flush-debounce-ms", "100"];
    mount_args.extend_from_slice(extra_mount_args);
    let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir_bench, &mount_args);
    let test_file = format!("{}/ckpt.bin", mount_point);

    // Cold step: write a single delta-sized blob at a random offset and
    // wait for the commit to settle. This is the headline number.
    let mut rng = Rng::new(0x00DE_ADBE_EFC0_FFEE);
    let (offset, len) = blob_for_step(&mut rng, file_size, delta_bytes);
    let dur = apply_blob_and_wait_durable(&test_file, offset, len, 1);
    eprintln!(
        "  cold step: 1 blob, offset={} {:.2} MB, commit {:.2}s",
        offset,
        len as f64 / (1024.0 * 1024.0),
        dur.as_secs_f64(),
    );
    eprintln!(
        "\n  --- {} cold summary ---\n  cold-cache first commit: {:.2}s\n",
        label,
        dur.as_secs_f64(),
    );

    std::fs::remove_file(&test_file).ok();
    common::unmount(&mount_point, child, 10);
    drop(guard);
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir_bench).ok();
}

/// Cold-cache sparse-write commit. Expect: no full-file download — sparse
/// punches a hole locally and `range_upload` composes against CAS.
#[tokio::test]
async fn bench_sparse_delta_cold() {
    run_cold_bench("SPARSE_COLD", &["--sparse-writes"]).await;
}

/// Cold-cache full-overwrite commit. Expect: the full checkpoint has to
/// be downloaded into local staging before the bench can `open` it for
/// write, then re-uploaded via `upload_files` (CDC dedup wire-trims, but
/// the download itself is unavoidable). On a 1.2 GB checkpoint with a 100
/// MB/s effective xet bandwidth this is ~12 s of pure download wait.
#[tokio::test]
async fn bench_full_cold() {
    run_cold_bench("FULL_COLD", &[]).await;
}
