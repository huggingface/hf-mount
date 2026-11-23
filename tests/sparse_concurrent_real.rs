//! Multi-worker concurrent sparse-write stress against real CAS.
//!
//! Companion to `fsx_paranoid` (single-threaded random ops) and the mock-based
//! verified-integrity stress (no network). This one combines both axes:
//!
//!   - N tokio workers writing concurrently on a SINGLE inode
//!   - Real FUSE kernel path + real hf-mount-fuse async handler + real CAS
//!     upload / `range_upload` composition / Hub commit
//!   - Disjoint per-worker slots so the final byte content is deterministic
//!     and we can byte-compare against the concatenation of per-worker shadows
//!
//! After workers finish: unmount and REMOUNT with a fresh cache so the
//! read-back forces a CAS-side load. Anything corrupted in the upload path
//! (lost write, torn range_upload composition, dirty_generation guard
//! missing a write) surfaces as a byte-level mismatch with offset + owner.
//!
//! Slow (real network): ~30 s default at 6 workers × 12 ops × 1 MiB. Bump
//! `CONCURRENT_REAL_WORKERS` / `CONCURRENT_REAL_OPS_PER_WORKER` for longer
//! runs. Requires `HF_TOKEN`. Run with:
//!   cargo test --release --test sparse_concurrent_real -- --nocapture

mod common;

use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const DEFAULT_WORKERS: usize = 6;
const DEFAULT_OPS_PER_WORKER: usize = 12;
/// 1 MiB per slot default — large enough that the sparse path's per-window
/// upload (range_upload) has measurable wire savings vs. the full re-upload
/// baseline, while keeping the test under ~30 s. Override via
/// `CONCURRENT_REAL_SLOT_KB` to push toward GiB-scale files.
const DEFAULT_SLOT_KB: usize = 1024;
/// Default: no intermediate fsync (one flush at end of run). Set
/// `CONCURRENT_REAL_FSYNC_EVERY_N` to force a `sync_all` every N ops
/// per worker, simulating periodic checkpoint saves in a real training
/// workload and exercising multiple apply_commit_sparse cycles within a
/// single test run.
const DEFAULT_FSYNC_EVERY_N: usize = 0;

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
    fn rand_byte(&mut self) -> u8 {
        self.next() as u8
    }
    fn rand_range(&mut self, hi: usize) -> usize {
        if hi == 0 { 0 } else { (self.next() as usize) % hi }
    }
}

struct Worker {
    path: String,
    worker_id: usize,
    seed: u64,
    n_ops: usize,
    slot_start: usize,
    slot_end: usize,
    shadow: Vec<u8>,
    writes_counter: Arc<AtomicU64>,
    fsyncs_counter: Arc<AtomicU64>,
    fsync_every_n: usize,
}

/// One worker: opens the shared file at `path` for write, does `n_ops` random
/// writes within its slot, and returns the final shadow for its slot.
///
/// All file I/O is synchronous (std::fs + FileExt::write_at) — wrapped in
/// spawn_blocking from the caller so the tokio executor isn't starved.
fn run_worker(w: Worker) -> Result<Vec<u8>, String> {
    let Worker {
        path,
        worker_id,
        seed,
        n_ops,
        slot_start,
        slot_end,
        mut shadow,
        writes_counter,
        fsyncs_counter,
        fsync_every_n,
    } = w;
    let mut rng = Rng::new(seed);
    let slot_len = slot_end - slot_start;

    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .map_err(|e| format!("[w{worker_id}] open: {e}"))?;

    for op_idx in 0..n_ops {
        let len = 1024 + rng.rand_range(64 * 1024 - 1024);
        let len = len.min(slot_len);
        let off_in_slot = rng.rand_range(slot_len - len + 1);
        let file_offset = slot_start + off_in_slot;

        let buf: Vec<u8> = (0..len).map(|_| rng.rand_byte()).collect();
        file.write_at(&buf, file_offset as u64)
            .map_err(|e| format!("[w{worker_id}] op {op_idx} write_at({file_offset},{len}): {e}"))?;
        shadow[off_in_slot..off_in_slot + len].copy_from_slice(&buf);
        writes_counter.fetch_add(1, Ordering::Relaxed);

        // Periodic fsync — when enabled, simulates the training-loop
        // pattern where the app saves a checkpoint every N steps. Each
        // sync_all triggers a flush + apply_commit_sparse cycle, so the
        // test exercises the per-cycle sparse paths multiple times
        // rather than just the final flush. Concurrent fsyncs from N
        // workers are batched by the FlushManager's debounce window.
        if fsync_every_n > 0 && (op_idx + 1) % fsync_every_n == 0 {
            file.sync_all()
                .map_err(|e| format!("[w{worker_id}] op {op_idx} mid-run sync_all: {e}"))?;
            fsyncs_counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    file.sync_all()
        .map_err(|e| format!("[w{worker_id}] final sync_all: {e}"))?;
    fsyncs_counter.fetch_add(1, Ordering::Relaxed);
    Ok(shadow)
}

fn read_full(path: &Path) -> std::io::Result<Vec<u8>> {
    std::fs::read(path)
}

#[tokio::test]
async fn test_sparse_concurrent_real_cas() {
    let n_workers: usize = std::env::var("CONCURRENT_REAL_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_WORKERS);
    let ops_per_worker: usize = std::env::var("CONCURRENT_REAL_OPS_PER_WORKER")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_OPS_PER_WORKER);
    let slot_kb: usize = std::env::var("CONCURRENT_REAL_SLOT_KB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SLOT_KB);
    let slot_size: usize = slot_kb * 1024;
    let fsync_every_n: usize = std::env::var("CONCURRENT_REAL_FSYNC_EVERY_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_FSYNC_EVERY_N);

    let guard = match common::setup_bucket("sparse-concurrent").await {
        Some(g) => g,
        None => {
            eprintln!("skipping: HF_TOKEN not set");
            return;
        }
    };
    let bucket_id = guard.bucket_id.clone();

    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-sparse-concurrent-{}", pid);
    let cache_dir_a = format!("/tmp/hf-sparse-concurrent-cache-a-{}", pid);
    let cache_dir_b = format!("/tmp/hf-sparse-concurrent-cache-b-{}", pid);

    let file_size = n_workers * slot_size;
    let test_file = format!("{}/shared.bin", mount_point);

    // Sparse engagement at any file size via HF_MOUNT_SPARSE_MIN_BYTES=0 on
    // the child. direct-io forces every read through the FUSE handler so the
    // read-back after remount can't be served by the kernel page cache.
    let mount_args = &[
        "--advanced-writes",
        "--sparse-writes",
        "--direct-io",
        "--flush-debounce-ms",
        "100",
    ];
    let env = &[("HF_MOUNT_SPARSE_MIN_BYTES", "0")];

    // ── Mount #1: seed + concurrent workers + final flush ──
    let child = common::mount_bucket_with_env(&bucket_id, &mount_point, &cache_dir_a, mount_args, env);

    // mount_bucket_with_env only warns on a failed wait_for_mount and returns
    // the Child regardless — without this check, a mount failure (e.g. stale
    // FUSE state on the host) would let the test pass trivially: std::fs::write
    // would land on the bare /tmp dir, std::fs::read would read it back from
    // the same /tmp dir, and the byte-compare would match because no remote
    // upload ever happened. Fail loud instead.
    if !common::is_mounted(&mount_point) {
        common::unmount(&mount_point, child, 5);
        panic!("mount #1 not live at {mount_point} — see hf-mount-fuse output above");
    }

    // Seed: write a known-content file. Each slot starts with a per-worker
    // signature so a regression that swaps slot contents is immediately
    // visible at byte-compare time. The shadow each worker carries into
    // run_worker is initialised to this same signature so the byte-compare
    // accounts for un-touched bytes inside the slot.
    let mut seed_buf = vec![0u8; file_size];
    for w in 0..n_workers {
        let start = w * slot_size;
        let end = start + slot_size;
        for (i, b) in seed_buf[start..end].iter_mut().enumerate() {
            *b = ((w * 17 + i) % 251) as u8;
        }
    }
    std::fs::write(&test_file, &seed_buf).expect("seed write");
    // Wait for the seed flush to settle so the workers start from a clean
    // committed base. flush_debounce_ms=100 + upload + Hub commit ≈ 1.5 s.
    std::thread::sleep(Duration::from_millis(2000));

    eprintln!(
        "sparse-concurrent-real: {n_workers} workers × {ops_per_worker} ops/worker, slot={} KiB, file={} MiB, fsync_every_n={}",
        slot_size / 1024,
        file_size / (1024 * 1024),
        fsync_every_n,
    );

    let writes_counter = Arc::new(AtomicU64::new(0));
    let fsyncs_counter = Arc::new(AtomicU64::new(0));
    let wall_start = Instant::now();

    let seed_base = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        | 1;

    let mut join_handles = Vec::with_capacity(n_workers);
    for w in 0..n_workers {
        let path = test_file.clone();
        let slot_start = w * slot_size;
        let slot_end = slot_start + slot_size;
        let shadow_slot = seed_buf[slot_start..slot_end].to_vec();
        let worker_seed = seed_base.wrapping_add(w as u64).wrapping_mul(0x9E3779B97F4A7C15);
        let writes_counter = writes_counter.clone();
        let fsyncs_counter = fsyncs_counter.clone();
        join_handles.push(tokio::task::spawn_blocking(move || {
            run_worker(Worker {
                path,
                worker_id: w,
                seed: worker_seed,
                n_ops: ops_per_worker,
                slot_start,
                slot_end,
                shadow: shadow_slot,
                writes_counter,
                fsyncs_counter,
                fsync_every_n,
            })
        }));
    }

    let mut errors = Vec::new();
    let mut expected = Vec::with_capacity(file_size);
    for h in join_handles {
        match h.await.expect("worker join panic") {
            Ok(slot_shadow) => expected.extend_from_slice(&slot_shadow),
            Err(e) => errors.push(e),
        }
    }
    let workers_elapsed = wall_start.elapsed();
    if !errors.is_empty() {
        common::unmount(&mount_point, child, 10);
        panic!(
            "concurrent-real stress FAILED ({} workers):\n  {}",
            errors.len(),
            errors.join("\n  ")
        );
    }
    assert_eq!(expected.len(), file_size, "shadow concatenation must equal file_size");

    eprintln!(
        "  workers done: {} writes + {} fsyncs total in {:.1}s ({:.1} writes/s)",
        writes_counter.load(Ordering::Relaxed),
        fsyncs_counter.load(Ordering::Relaxed),
        workers_elapsed.as_secs_f64(),
        writes_counter.load(Ordering::Relaxed) as f64 / workers_elapsed.as_secs_f64(),
    );

    // Give the flush manager plenty of time to drain the post-worker dirty
    // state to CAS + Hub. The bench's flush_debounce_ms=100 + per-upload
    // network latency means a few seconds is a comfortable upper bound.
    std::thread::sleep(Duration::from_millis(5000));
    common::unmount(&mount_point, child, 30);

    // ── Mount #2: fresh cache → read-back forces CAS load ──
    let child = common::mount_bucket_with_env(&bucket_id, &mount_point, &cache_dir_b, mount_args, env);
    if !common::is_mounted(&mount_point) {
        common::unmount(&mount_point, child, 5);
        panic!("mount #2 not live at {mount_point} — see hf-mount-fuse output above");
    }

    let test_file_b = format!("{}/shared.bin", mount_point);
    let actual = match read_full(Path::new(&test_file_b)) {
        Ok(v) => v,
        Err(e) => {
            common::unmount(&mount_point, child, 10);
            panic!("read after remount failed: {e}");
        }
    };

    if actual != expected {
        let first_diff = actual
            .iter()
            .zip(expected.iter())
            .position(|(a, e)| a != e)
            .unwrap_or(actual.len().min(expected.len()));
        let owner = first_diff / slot_size;
        common::unmount(&mount_point, child, 10);
        panic!(
            "BYTE-LEVEL MISMATCH at offset {} (owner=w{}, slot offset={}), \
             actual=0x{:02x} expected=0x{:02x}, actual_len={}, expected_len={}",
            first_diff,
            owner,
            first_diff % slot_size,
            actual.get(first_diff).copied().unwrap_or(0),
            expected.get(first_diff).copied().unwrap_or(0),
            actual.len(),
            expected.len(),
        );
    }

    eprintln!(
        "sparse-concurrent-real: PASSED {} ops total (file_size={} bytes, total_elapsed={:.1}s)",
        n_workers * ops_per_worker,
        file_size,
        wall_start.elapsed().as_secs_f64(),
    );

    std::fs::remove_file(&test_file_b).ok();
    common::unmount(&mount_point, child, 10);
    drop(guard);
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir_a).ok();
    std::fs::remove_dir_all(&cache_dir_b).ok();
}
