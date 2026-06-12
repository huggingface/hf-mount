use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Once, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::hub_api::{BatchOp, HubOps};
use crate::xet::{RangeSnapshot, XetOps};
use bytes::Bytes;

use super::inode::{InodeTable, SparseWriteState};
use super::staging::StagingCoordinator;
use std::os::unix::fs::FileExt;
use xet_data::processing::XetFileInfo;

enum FlushSignal {
    /// Flush a dirty inode.
    Dirty(u64),
    /// Wake the loop to drain pending remote deletes (no dirty inode attached).
    WakeDeletes,
}

// ── FlushManager ──────────────────────────────────────────────────────

pub(crate) struct FlushManager {
    tx: Mutex<Option<mpsc::UnboundedSender<FlushSignal>>>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    errors: Arc<Mutex<HashMap<u64, String>>>,
    /// Queued remote delete paths. Shared with flush_loop, flushed in batch cycle and shutdown.
    pending_deletes: Arc<Mutex<Vec<String>>>,
    hub_client: Arc<dyn HubOps>,
    /// Ensures shutdown runs exactly once. Subsequent calls are no-ops.
    shutdown_once: Once,
}

impl FlushManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        xet_sessions: Arc<dyn XetOps>,
        staging: Arc<StagingCoordinator>,
        hub_client: Arc<dyn HubOps>,
        inodes: Arc<RwLock<InodeTable>>,
        runtime: &tokio::runtime::Handle,
        debounce: Duration,
        max_batch_window: Duration,
        snapshot_budget_bytes: u64,
    ) -> Self {
        let errors = Arc::new(Mutex::new(HashMap::new()));
        let pending_deletes = Arc::new(Mutex::new(Vec::new()));

        let (tx, rx) = mpsc::unbounded_channel::<FlushSignal>();
        let bg_errors = errors.clone();
        let bg_deletes = pending_deletes.clone();
        let bg_hub = hub_client.clone();
        let handle = runtime.spawn(flush_loop(
            rx,
            xet_sessions,
            staging,
            bg_hub,
            inodes,
            bg_errors,
            debounce,
            max_batch_window,
            bg_deletes,
            snapshot_budget_bytes,
        ));

        Self {
            tx: Mutex::new(Some(tx)),
            handle: Mutex::new(Some(handle)),
            errors,
            pending_deletes,
            hub_client,
            shutdown_once: Once::new(),
        }
    }

    /// Enqueue a dirty inode for flush.
    pub(crate) fn enqueue(&self, ino: u64) {
        if let Some(tx) = self.tx.lock().expect("flush_tx poisoned").as_ref()
            && tx.send(FlushSignal::Dirty(ino)).is_err()
        {
            error!("Flush channel closed, cannot enqueue ino={}", ino);
        }
    }

    pub(crate) fn check_error(&self, ino: u64) -> Option<String> {
        self.errors.lock().expect("flush_errors poisoned").remove(&ino)
    }

    // ── Remote delete queue ─────────────────────────────────────────

    /// Queue a path for batched remote deletion (flushed in next flush_loop cycle).
    pub(crate) fn enqueue_delete(&self, path: String) {
        self.pending_deletes
            .lock()
            .expect("pending_deletes poisoned")
            .push(path);
        // Wake flush_loop so it drains pending_deletes even without dirty inodes.
        if let Some(tx) = self.tx.lock().expect("flush_tx poisoned").as_ref() {
            let _ = tx.send(FlushSignal::WakeDeletes);
        }
    }

    /// Cancel a queued delete (e.g. when a new file is created at the same path).
    pub(crate) fn cancel_delete(&self, path: &str) {
        let mut q = self.pending_deletes.lock().expect("pending_deletes poisoned");
        if !q.is_empty() {
            q.retain(|p| p != path);
        }
    }

    /// Cancel all queued deletes under a directory prefix (e.g. `rm -rf dir && mv newdir dir`).
    pub(crate) fn cancel_delete_prefix(&self, prefix: &str) {
        let mut q = self.pending_deletes.lock().expect("pending_deletes poisoned");
        if !q.is_empty() {
            q.retain(|p| !p.starts_with(prefix));
        }
    }

    /// Flush all queued remote deletes in a single batch API call.
    pub(crate) async fn flush_deletes(&self) {
        flush_pending_deletes(&self.pending_deletes, &*self.hub_client).await;
    }

    // ── Shutdown ────────────────────────────────────────────────────

    /// Graceful shutdown drain, **bounded by `timeout`**.
    ///
    /// On SIGTERM the sidecar drives this to persist dirty data before the
    /// process exits. The drain MUST be bounded: if the Hub/CAS upload backend
    /// is slow or hung, an unbounded wait keeps the process alive past the pod's
    /// termination grace period, the FUSE fd never closes, the kernel never
    /// tears down the connection, and any process touching the mount stays
    /// blocked in uninterruptible (`D`-state) I/O — which strands the pod
    /// (kubelet `FailedKillPod`) and leaks the node. On timeout we abandon the
    /// remaining dirty data and return so the caller can exit; losing unflushed
    /// data is strictly better than wedging the pod indefinitely.
    pub(crate) fn shutdown(&self, dirty_inos: Vec<u64>, runtime: &tokio::runtime::Handle, timeout: Duration) {
        // Run shutdown exactly once. Subsequent calls (e.g. signal handler
        // racing with destroy()) are no-ops.
        self.shutdown_once.call_once(|| {
            let dirty_count = dirty_inos.len();
            // Enqueue all remaining dirty files
            if let Some(tx) = self.tx.lock().expect("flush_tx poisoned").as_ref() {
                for ino in dirty_inos {
                    let _ = tx.send(FlushSignal::Dirty(ino));
                }
            }
            // Drop the sender to signal the flush loop to drain and exit
            self.tx.lock().expect("flush_tx poisoned").take();

            // Single deadline shared across both drain phases so total shutdown
            // time is bounded by `timeout`, not 2×`timeout`.
            let deadline = Instant::now() + timeout;

            // Take the handle then drop the guard before blocking.
            let handle = self.handle.lock().expect("flush_handle poisoned").take();
            if let Some(handle) = handle {
                let abort = handle.abort_handle();
                // Use block_in_place so this is safe even when called from within
                // the tokio runtime (e.g. NFS shutdown path).
                run_blocking(|| {
                    runtime.block_on(async {
                        match tokio::time::timeout(timeout, handle).await {
                            Ok(Ok(())) => {},
                            Ok(Err(err)) if err.is_cancelled() => {},
                            Ok(Err(err)) => error!("Flush task panicked: {}", err),
                            Err(_) => {
                                warn!(
                                    "Flush drain exceeded shutdown timeout ({:?}); abandoning ~{} dirty inode(s) so the process can exit and the FUSE connection is torn down. Unflushed data for this mount is lost.",
                                    timeout, dirty_count
                                );
                                abort.abort();
                            },
                        }
                    });
                });
            }
            // Flush remaining queued deletes within whatever time is left.
            let remaining = deadline.saturating_duration_since(Instant::now());
            run_blocking(|| {
                runtime.block_on(async {
                    if tokio::time::timeout(remaining, self.flush_deletes()).await.is_err() {
                        warn!("Remote-delete flush exceeded shutdown timeout; abandoning queued deletes.");
                    }
                });
            });
        });
    }
}

/// Run a blocking closure, using `block_in_place` when called from within the
/// Tokio runtime (e.g. NFS shutdown) and directly otherwise (e.g. FUSE destroy).
fn run_blocking<F: FnOnce()>(f: F) {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(f);
    } else {
        f();
    }
}

// ── Background tasks ──────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn flush_loop(
    mut rx: mpsc::UnboundedReceiver<FlushSignal>,
    xet_sessions: Arc<dyn XetOps>,
    staging: Arc<StagingCoordinator>,
    hub_client: Arc<dyn HubOps>,
    inodes: Arc<RwLock<InodeTable>>,
    flush_errors: Arc<Mutex<HashMap<u64, String>>>,
    debounce: Duration,
    max_batch_window: Duration,
    pending_deletes: Arc<Mutex<Vec<String>>>,
    snapshot_budget_bytes: u64,
) {
    // Inos from a previous cycle that stayed dirty because their Hub commit
    // was aborted (a sibling's upload failure, a commit failure, or their own
    // upload failure). The loop is signal-driven; without re-arming these
    // here they would sit in limbo — dirty, possibly durable in CAS, but
    // never committed — until an unrelated event on the same inode or
    // unmount (and a crash before that would lose the commit silently).
    //
    // Retry pacing: carryover items are only merged into a batch once
    // `retry_due` has passed — a busy mount's sibling signals must not
    // collapse the retry interval to the debounce window. Consecutive
    // all-failed cycles back off exponentially (capped) so a permanent
    // failure (revoked token, quota) settles into a slow, logged cadence
    // instead of hammering the backend forever at full speed.
    let mut carryover: Vec<u64> = Vec::new();
    let mut consecutive_failed_cycles: u32 = 0;
    let mut retry_due = tokio::time::Instant::now();
    // Floor for the carryover wake-up: max_batch_window is user-configurable
    // down to 0, which must not turn the retry wait into a zero-delay spin.
    const RETRY_FLOOR: Duration = Duration::from_millis(500);
    const RETRY_BACKOFF_CAP: u32 = 4; // max multiplier 2^4 = 16× the window
    loop {
        // Wait for the first signal. With carryover pending, wake when the
        // retry is due even if no new signal arrives, so aborted commits
        // retry on their own.
        let mut signals = Vec::new();
        if carryover.is_empty() {
            match rx.recv().await {
                Some(sig) => signals.push(sig),
                None => return, // channel closed, exit
            }
        } else {
            let wait = retry_due
                .saturating_duration_since(tokio::time::Instant::now())
                .max(RETRY_FLOOR);
            match tokio::time::timeout(wait, rx.recv()).await {
                Ok(Some(sig)) => signals.push(sig),
                // Channel closed with retries pending: abandon them. The
                // shutdown path already enqueued and drained every dirty ino
                // once, and FlushManager::shutdown bounds the whole drain —
                // re-running a flush that just failed would spend the pod's
                // termination grace on a near-zero success probability.
                Ok(None) => return,
                Err(_) => {} // retry due — flush carryover
            }
        }

        // Debounce: keep collecting for debounce duration after each new item,
        // but cap total wait at max_batch_window to avoid unbounded delay.
        if !signals.is_empty() {
            let window_deadline = tokio::time::Instant::now() + max_batch_window;
            loop {
                let remaining = window_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let timeout = debounce.min(remaining);
                match tokio::time::timeout(timeout, rx.recv()).await {
                    Ok(Some(sig)) => signals.push(sig),
                    _ => break, // timeout (debounce expired) or channel closed
                }
            }
        }

        // Flush queued remote deletes alongside dirty writes.
        flush_pending_deletes(&pending_deletes, &*hub_client).await;

        // Extract dirty inode IDs (WakeDeletes signals carry no inode),
        // merging the retry carryover only once its due time has passed.
        let mut dirty_inos: Vec<u64> = Vec::new();
        if tokio::time::Instant::now() >= retry_due {
            dirty_inos.append(&mut carryover);
        }
        dirty_inos.extend(signals.into_iter().filter_map(|sig| match sig {
            FlushSignal::Dirty(ino) => Some(ino),
            FlushSignal::WakeDeletes => None,
        }));

        if !dirty_inos.is_empty() {
            info!("Flushing batch of {} dirty file(s)", dirty_inos.len());
            let failed = flush_batch(
                dirty_inos,
                &*xet_sessions,
                &staging,
                &*hub_client,
                &inodes,
                &flush_errors,
                snapshot_budget_bytes,
            )
            .await;

            // Update the retry pacing. Any fully-successful cycle resets the
            // backoff; a cycle with failures extends it (exponentially,
            // capped). Failures are merged into the carryover, which may
            // still hold not-yet-due items from a previous cycle.
            if failed.is_empty() {
                consecutive_failed_cycles = 0;
            } else {
                consecutive_failed_cycles = (consecutive_failed_cycles + 1).min(RETRY_BACKOFF_CAP);
                let backoff_multiplier = 1u32 << (consecutive_failed_cycles - 1);
                retry_due = tokio::time::Instant::now() + max_batch_window.max(RETRY_FLOOR) * backoff_multiplier;
                warn!(
                    "flush: {} item(s) failed; retrying in {:?} (consecutive failed cycles: {})",
                    failed.len(),
                    max_batch_window.max(RETRY_FLOOR) * backoff_multiplier,
                    consecutive_failed_cycles
                );
            }
            for ino in failed {
                if !carryover.contains(&ino) {
                    carryover.push(ino);
                }
            }
        }
    }
}

/// Drain delete queue and send as a single batch_operations call.
/// Re-queues paths on transient failure.
///
/// Note: there is a small race window between `take()` and the Hub response where
/// `cancel_delete` cannot revoke an in-flight delete. The same race exists for dirty
/// writes (flush_batch reads inode state, then a concurrent rename can change the path).
/// In both cases the next flush cycle corrects the state: recreated files are dirty and
/// get re-uploaded, clean renames send their own batch ops. The window is bounded by a
/// single Hub round-trip (~100ms). Could be fixed with an in-flight set, but not worth
/// the complexity given the self-healing behavior.
async fn flush_pending_deletes(queue: &Mutex<Vec<String>>, hub_client: &dyn HubOps) {
    let paths: Vec<String> = {
        let mut pending = queue.lock().expect("pending_deletes poisoned");
        if pending.is_empty() {
            return;
        }
        std::mem::take(&mut *pending)
    };
    let count = paths.len();
    let ops: Vec<BatchOp> = paths.into_iter().map(|path| BatchOp::DeleteFile { path }).collect();
    info!("Flushing {count} queued remote deletes");
    if let Err(e) = hub_client.batch_operations(&ops).await {
        error!("Batch remote delete failed: {}", e);
        // Re-queue paths from the failed ops for retry.
        let mut pending = queue.lock().expect("pending_deletes poisoned");
        for op in ops {
            if let BatchOp::DeleteFile { path } = op {
                pending.push(path);
            }
        }
        warn!("{count} remote delete(s) re-queued for retry");
    }
}

/// How a flush item's content reaches CAS. Decided once per item at routing
/// time and consulted by Pass A, Pass B membership, and the commit-apply
/// dispatch — never re-derived.
#[derive(Clone, Copy, PartialEq, Eq)]
enum UploadRoute {
    /// Whole staging file via the batched `upload_files` path: non-sparse
    /// items, and fully-covered sparse items (staging IS the content).
    /// Commit applies via `apply_commit` — staging becomes a valid byte
    /// cache for the next open.
    FullStaging,
    /// Budget-bounded `range_upload` compose against the CAS base: sparse
    /// items whose coverage has holes. Commit applies via
    /// `apply_commit_sparse` — staging stays a partial mirror.
    SparseCompose,
}

struct FlushItem {
    ino: u64,
    full_path: String,
    staging_path: PathBuf,
    pending_deletes: Vec<String>,
    dirty_generation: u64,
    /// Hash from the last successful commit, used to skip redundant Hub commits
    /// when the CAS upload produces the same hash (content unchanged).
    prev_xet_hash: Option<String>,
    /// Current file size at snapshot time. Captured under the inode read
    /// lock together with the sparse_write Arc, so range_upload composes a
    /// file of the right shape even if a concurrent setattr races later.
    file_size: u64,
    /// Sparse-write snapshot. When Some, the flush composes a new CAS file
    /// from this state's dirty ranges via range_upload (no re-upload of the
    /// unchanged prefix/suffix). When None, the flush takes the regular path
    /// and uploads the entire staging file via upload_files.
    sparse_snapshot: Option<Arc<SparseWriteState>>,
}

/// Peak bytes of dirty-range content materialized in RAM per `range_upload`
/// call. Dirty sets larger than this are composed iteratively (see
/// `chunk_dirty_ranges`): each round snapshots at most this many bytes and
/// range-uploads them against the previous round's hash. Without a budget, a
/// sparse item with tens of GiB of dirty ranges would buffer them all
/// simultaneously and hold them across the upload await — an OOM risk the
/// pre-sparse upload_files path (which streams from disk) never had.
pub(crate) const SPARSE_SNAPSHOT_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Split sorted, non-overlapping dirty ranges into groups whose total byte
/// length is at most `budget` (a single range larger than the budget is split
/// across groups). Empty input yields one empty group so the caller still
/// issues exactly one `range_upload` (needed for the pure-truncate case where
/// the size changed but no dirty bytes exist).
fn chunk_dirty_ranges(ranges: &[(u64, u64)], budget: u64) -> Vec<Vec<(u64, u64)>> {
    let budget = budget.max(1);
    if ranges.is_empty() {
        return vec![Vec::new()];
    }
    let mut groups = Vec::new();
    let mut current: Vec<(u64, u64)> = Vec::new();
    let mut current_bytes = 0u64;
    for &(start, end) in ranges {
        let mut cursor = start;
        while cursor < end {
            if current_bytes == budget {
                groups.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            let take = (end - cursor).min(budget - current_bytes);
            current.push((cursor, cursor + take));
            current_bytes += take;
            cursor += take;
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Read each given dirty range from the staging file at `staging_path`,
/// producing a Vec of `RangeSnapshot` ready for `range_upload`. Each range is
/// read under the per-inode io_lock so a concurrent pwrite cannot interleave
/// with our reads.
fn snapshot_dirty_bytes(
    staging: &StagingCoordinator,
    ino: u64,
    staging_path: &std::path::Path,
    dirty_ranges: &[(u64, u64)],
) -> std::io::Result<Vec<RangeSnapshot>> {
    if dirty_ranges.is_empty() {
        return Ok(Vec::new());
    }
    let file = std::fs::File::open(staging_path)?;
    let io_lock = staging.io_lock(ino);
    let _io_guard = io_lock.lock().expect("staging io_lock poisoned");
    let mut snapshots = Vec::with_capacity(dirty_ranges.len());
    for &(start, end) in dirty_ranges {
        let len = (end - start) as usize;
        let mut buf = vec![0u8; len];
        // The staging file MUST hold every byte the dirty range claims: the
        // flush holds `staging.lock(ino)` across this snapshot (see flush_batch),
        // and `setattr`-shrink takes the same lock before truncating, so it
        // cannot shrink staging underneath us. A short read therefore signals a
        // real invariant violation, not a benign race; surface it as an error
        // (the dirty_generation guard makes the next flush retry) instead of
        // silently composing a wrong-length file from a truncated buffer.
        // Translate UnexpectedEof into an explicit invariant-violation message
        // so the failure mode is greppable in logs (the default Display is
        // just "failed to fill whole buffer" which leaks zero context).
        file.read_exact_at(&mut buf, start).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                std::io::Error::other(format!(
                    "staging short-read at dirty range [{start}, {end}) — staging file shorter than dirty_ranges claims (invariant violation)"
                ))
            } else {
                e
            }
        })?;
        snapshots.push(RangeSnapshot {
            offset: start,
            data: Bytes::from(buf),
        });
    }
    Ok(snapshots)
}

/// Flush a batch of dirty inodes. Returns the inos that must be RETRIED by
/// the flush loop: items left dirty because their upload failed or because a
/// batch-level abort (sibling upload failure, Hub commit failure) skipped
/// their commit. The loop re-arms them itself — nothing else re-enqueues an
/// inode whose handles are already closed.
#[allow(clippy::too_many_arguments)]
async fn flush_batch(
    pending: Vec<u64>,
    xet_sessions: &dyn XetOps,
    staging: &StagingCoordinator,
    hub_client: &dyn HubOps,
    inodes: &RwLock<InodeTable>,
    flush_errors: &Mutex<HashMap<u64, String>>,
    snapshot_budget_bytes: u64,
) -> Vec<u64> {
    let staging_dir = staging.dir().expect("flush_batch requires staging directory");
    // Walk backwards so the last request per ino wins, then reverse in-place.
    let mut seen = HashSet::new();
    let mut deduped: Vec<u64> = pending.into_iter().rev().filter(|ino| seen.insert(*ino)).collect();
    deduped.reverse();

    debug!("flush_batch: deduped inos = {:?}", deduped);

    // Hold per-inode staging locks across snapshot + upload + commit so concurrent
    // O_TRUNC (open_advanced_write, setattr) and unlink/rename → drop_staging
    // can't truncate or remove the file xet-core is reading. Acquired before the
    // snapshot to also catch unlink that races between snapshot and upload.
    // Acquired in deduped order, which is consistent across concurrent callers.
    let mut _staging_guards: Vec<tokio::sync::OwnedMutexGuard<()>> = Vec::with_capacity(deduped.len());
    for ino in &deduped {
        _staging_guards.push(staging.lock(*ino).lock_owned().await);
    }

    // Dirty inos whose staging file is gone (external tmp-cleaner, manual rm
    // of the staging dir). Resolved to a terminal state below.
    let mut missing_staging: Vec<(u64, String)> = Vec::new();

    // Snapshot dirty_generation so we only clear dirty if no concurrent writer advanced it.
    let to_flush: Vec<FlushItem> = {
        let inode_table = inodes.read().expect("inodes poisoned");
        deduped
            .into_iter()
            .filter_map(|ino| {
                let entry = match inode_table.get(ino) {
                    Some(e) => e,
                    None => {
                        debug!("flush: ino={} not found in inode table, skipping", ino);
                        return None;
                    }
                };
                if entry.nlink == 0 {
                    debug!("flush: ino={} unlinked, skipping", ino);
                    return None;
                }
                if !entry.is_dirty() {
                    debug!("flush: ino={} path={} not dirty, skipping", ino, entry.full_path);
                    return None;
                }
                let staging_path = staging_dir.path(ino);
                if !staging_path.exists() {
                    missing_staging.push((ino, entry.full_path.to_string()));
                    return None;
                }
                Some(FlushItem {
                    ino,
                    full_path: entry.full_path.to_string(),
                    staging_path,
                    pending_deletes: entry.pending_deletes.clone(),
                    dirty_generation: entry.dirty_generation,
                    prev_xet_hash: entry.xet_hash.clone(),
                    file_size: entry.size,
                    sparse_snapshot: entry.sparse_write.clone(),
                })
            })
            .collect()
    };

    // Resolve missing-staging inos to a TERMINAL state. The dirty bytes are
    // unrecoverable (the staging file is gone), so retrying can never
    // succeed; stranding the inode dirty forever would also block every
    // future remote rotation for that path. Revert the inode to
    // remote-backed (clear dirty + sparse state) and park the error for the
    // next fsync/close to surface. Lock order: flush_errors is taken in its
    // own scope, never while holding the inode table (see the lock-order
    // notes in mod.rs).
    if !missing_staging.is_empty() {
        {
            let mut inode_table = inodes.write().expect("inodes poisoned");
            for (ino, path) in &missing_staging {
                error!(
                    "flush: ino={} staging file missing for {} — dirty bytes are lost; \
                     reverting the inode to remote-backed",
                    ino, path
                );
                if let Some(entry) = inode_table.get_mut(*ino) {
                    let generation = entry.dirty_generation;
                    entry.clear_dirty_if(generation);
                    entry.sparse_write = None;
                    entry.staging_is_current = false;
                }
            }
        }
        let mut errs = flush_errors.lock().expect("flush_errors poisoned");
        for (ino, path) in &missing_staging {
            errs.insert(*ino, format!("staging file missing for {path}; local changes lost"));
        }
    }

    if to_flush.is_empty() {
        // Drop guards before gc — gc_one acquires the same per-inode locks.
        drop(_staging_guards);
        // Still reclaim if other clean staging files pushed us over budget.
        staging.gc(inodes).await;
        return Vec::new();
    }

    // Inos to re-arm in the flush loop (upload failures + batch aborts).
    let mut retry: Vec<u64> = Vec::new();

    // Upload in chunks to bound FD usage (xet-core opens all staging files per
    // upload session), but accumulate all batch ops for a single Hub commit to
    // preserve the global adds-before-deletes ordering required by the Hub API.
    //
    // Sparse items take the per-item range_upload path: dirty bytes are
    // snapshotted under the per-inode io_lock, then composed against the
    // existing CAS reconstruction without re-uploading the unchanged prefix/
    // suffix. Regular items go through the batched upload_files path.
    const UPLOAD_CHUNK_SIZE: usize = 500;
    let mut upload_results: Vec<Option<XetFileInfo>> = vec![None; to_flush.len()];

    // Pass A: per-item range_upload for sparse items whose coverage has
    // HOLES (the staging file is incomplete and we want the wire savings
    // of composing against the old CAS base). A failure on one sparse item
    // is recorded per-inode and the loop continues — independent sparse
    // items are not head-of-line-blocked by a sibling's sticky failure.
    // The failing item leaves its slot in `upload_results` as None:
    // downstream commit-apply skips it, clear_dirty_if is never invoked,
    // the next flush retries it.
    //
    // Route decision, computed ONCE per item and consulted by Pass A, Pass B
    // membership, and the commit-apply dispatch. A single value (instead of
    // boolean algebra re-derived at each consumer) makes it impossible for
    // the three sites to disagree — a mismatch would either commit a holey
    // staging file as content or mark a full upload as a partial mirror.
    //
    // FullStaging covers non-sparse items AND fully-covered sparse items
    // with dirty ranges: when coverage spans the whole file, the staging
    // file IS the committed content, and the batched upload_files path costs
    // the same wire bytes (xet CDC dedup re-uses existing xorbs for
    // unchanged chunks) while skipping range_upload's per-window CAS
    // re-fetches. Bench at 1.2 GB / 12 MB blob: ~1.37 s vs ~1.5 s, and the
    // saving grows with file size. A fully-covered sparse item WITHOUT
    // dirty ranges stays on SparseCompose: range_upload short-circuits to
    // the original hash unchanged ("no-op flush"); forcing upload_files
    // would re-hash the staging into a new CDC-equivalent hash and lose
    // that semantic.
    let routes: Vec<UploadRoute> = to_flush
        .iter()
        .map(|item| match item.sparse_snapshot.as_ref() {
            Some(sw) if sw.is_fully_covered(item.file_size) && !sw.dirty_ranges.is_empty() => UploadRoute::FullStaging,
            Some(_) => UploadRoute::SparseCompose,
            None => UploadRoute::FullStaging,
        })
        .collect();

    for (idx, item) in to_flush.iter().enumerate() {
        if routes[idx] != UploadRoute::SparseCompose {
            continue;
        }
        let Some(sw) = item.sparse_snapshot.as_ref() else {
            continue;
        };
        // Compose in budget-bounded rounds: each round snapshots at most
        // `snapshot_budget_bytes` of dirty content under the io_lock (so a
        // concurrent pwrite cannot interleave with the reads) and
        // range-uploads it against the previous round's hash. One round for
        // the common case; large dirty sets stay bounded in RAM instead of
        // being materialized wholesale and held across the upload await.
        // Each intermediate hash is a real composed CAS file, so using it as
        // the next base is the same pattern apply_commit_sparse already
        // relies on between flushes (xet-core registers the shards
        // synchronously in upload_ranges' finalize, and chained compose is
        // covered by xet-core's own stress tests).
        let groups = chunk_dirty_ranges(&sw.dirty_ranges, snapshot_budget_bytes);
        let mut base_hash = sw.original_hash.clone();
        let mut base_size = sw.original_size;
        let mut last_info: Option<XetFileInfo> = None;
        for (group_idx, group) in groups.iter().enumerate() {
            // Multi-round only: a write landing BETWEEN rounds (write takes
            // io_lock, not the staging.lock this flush holds) would make the
            // composed revision mix pre- and post-write bytes — a state that
            // never existed locally, published to remote consumers until the
            // next flush converges. The single-round common case is immune
            // (one atomic io_lock snapshot). Detect the interleave via the
            // dirty generation and abort the compose; the item stays dirty
            // and the retry recomposes a consistent snapshot.
            if group_idx > 0 {
                let live_generation = inodes
                    .read()
                    .expect("inodes poisoned")
                    .get(item.ino)
                    .map(|entry| entry.dirty_generation);
                if live_generation != Some(item.dirty_generation) {
                    debug!(
                        "sparse flush: ino={} dirty generation changed mid-compose (round {}/{}); \
                         aborting to avoid publishing a torn revision",
                        item.ino,
                        group_idx + 1,
                        groups.len()
                    );
                    retry.push(item.ino);
                    last_info = None;
                    break;
                }
            }
            let snapshots = match snapshot_dirty_bytes(staging, item.ino, &item.staging_path, group) {
                Ok(s) => s,
                Err(e) => {
                    error!(
                        "sparse flush: snapshot failed for ino={} path={}: {}",
                        item.ino, item.full_path, e
                    );
                    flush_errors
                        .lock()
                        .expect("flush_errors poisoned")
                        .insert(item.ino, format!("snapshot failed: {e}"));
                    retry.push(item.ino);
                    last_info = None;
                    break;
                }
            };
            // Intermediate composes never reference bytes past what the base
            // and this group define (a grow's zero-fill gap is itself a dirty
            // range, so ascending groups extend the file contiguously); the
            // final group always lands exactly on item.file_size.
            let group_end = group.last().map(|&(_, end)| end).unwrap_or(0);
            let target_size = item.file_size.min(base_size.max(group_end));
            match xet_sessions
                .range_upload(&base_hash, base_size, target_size, snapshots)
                .await
            {
                Ok(info) => {
                    base_hash = info.hash().to_string();
                    base_size = target_size;
                    last_info = Some(info);
                }
                Err(e) => {
                    error!(
                        "sparse flush: range_upload failed for ino={} path={}: {}",
                        item.ino, item.full_path, e
                    );
                    flush_errors
                        .lock()
                        .expect("flush_errors poisoned")
                        .insert(item.ino, format!("range_upload failed: {e}"));
                    retry.push(item.ino);
                    last_info = None;
                    break;
                }
            }
        }
        if let Some(info) = last_info {
            debug_assert_eq!(
                base_size, item.file_size,
                "iterative sparse compose must land on the snapshot size"
            );
            upload_results[idx] = Some(info);
        }
    }

    // Pass B: batched upload_files for every FullStaging-routed item.
    let regular_indices: Vec<usize> = routes
        .iter()
        .enumerate()
        .filter(|(_, route)| **route == UploadRoute::FullStaging)
        .map(|(i, _)| i)
        .collect();
    for chunk in regular_indices.chunks(UPLOAD_CHUNK_SIZE) {
        let staging_paths: Vec<&std::path::Path> = chunk.iter().map(|&i| to_flush[i].staging_path.as_path()).collect();
        match xet_sessions.upload_files(&staging_paths).await {
            Ok(results) => {
                assert_eq!(
                    results.len(),
                    chunk.len(),
                    "upload_files returned {} results for {} inputs",
                    results.len(),
                    chunk.len()
                );
                for (slot_idx, info) in chunk.iter().zip(results) {
                    upload_results[*slot_idx] = Some(info);
                }
            }
            Err(e) => {
                error!("Batch upload failed, aborting flush: {}", e);
                let msg = format!("upload failed: {e}");
                let mut errs = flush_errors.lock().expect("flush_errors poisoned");
                // The whole batch is aborted: no Hub commit is sent for any
                // item. Mark only items that have NO upload result yet — the
                // ones that actually failed (this chunk or earlier Pass A
                // failures) or that the early return prevented from running.
                // Items with `upload_results[i] = Some(_)` already pushed
                // their CAS xorb successfully (Pass A range_upload or an
                // earlier Pass B chunk); the data is durable, only the Hub
                // commit is missing. They stay dirty (no apply_commit runs)
                // — fsync must not surface an "upload failed" error for
                // them. `or_insert_with` still preserves the more specific
                // Pass A error message on items that did fail there.
                // EVERY item is returned for retry: the loop is signal-
                // driven, so without re-arming, an item whose handles are
                // already closed would never get its Hub commit.
                for (i, item) in to_flush.iter().enumerate() {
                    if upload_results[i].is_none() {
                        errs.entry(item.ino).or_insert_with(|| msg.clone());
                    }
                }
                return to_flush.iter().map(|item| item.ino).collect();
            }
        }
    }

    // Uploads are done — drop the staging locks so unlink/truncate and the
    // gc() calls below (which acquire the same per-inode locks) can proceed.
    drop(_staging_guards);

    if upload_results.iter().all(|r| r.is_none()) {
        // Every upload failed; all items are already in `retry`.
        return retry;
    }

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Hub API requires all adds before all deletes.
    let mut ops = Vec::new();
    let mut delete_ops = Vec::new();
    let mut unchanged = vec![false; to_flush.len()];

    for (i, (item, file_info_opt)) in to_flush.iter().zip(upload_results.iter()).enumerate() {
        // Skip items whose upload failed in Pass A (sparse range_upload error).
        // They stay dirty and are retried on the next flush.
        let Some(file_info) = file_info_opt else {
            continue;
        };
        if item.pending_deletes.is_empty() && item.prev_xet_hash.as_deref() == Some(file_info.hash()) {
            debug!(
                "flush_batch: unchanged ino={} path={} (hash {})",
                item.ino,
                item.full_path,
                file_info.hash()
            );
            unchanged[i] = true;
            continue;
        }
        // Use `item.file_size` (= entry.size at snapshot time = the actual
        // staging file length under staging.lock) instead of
        // `file_info.file_size()`. The xet-core upload_files path has been
        // observed to return an inflated file_size from deduplication_metrics
        // (a 64 MiB file gets reported as ~65.75 MiB), which would propagate
        // into entry.size via apply_commit and cause subsequent range_upload
        // calls to fail with "caller said original_size=X but reconstruction
        // info reports Y". The on-CAS file is correct — only the returned
        // XetFileInfo.file_size lies — so the safe authoritative value is
        // the size we actually uploaded from staging.
        info!(
            "Uploaded file ino={} path={} xet_hash={} size={}",
            item.ino,
            item.full_path,
            file_info.hash(),
            item.file_size,
        );
        ops.push(BatchOp::AddFile {
            path: item.full_path.clone(),
            xet_hash: file_info.hash().to_string(),
            mtime: mtime_ms,
            content_type: None,
        });
        for old_path in &item.pending_deletes {
            delete_ops.push(BatchOp::DeleteFile { path: old_path.clone() });
        }
    }

    // Clear dirty on unchanged files without waiting for the Hub round-trip.
    {
        let mut errs = flush_errors.lock().expect("flush_errors poisoned");
        let mut inode_table = inodes.write().expect("inodes poisoned");
        for (i, item) in to_flush.iter().enumerate() {
            if unchanged[i]
                && let Some(entry) = inode_table.get_mut(item.ino)
            {
                // Same hash + no pending_deletes: nothing changed remotely.
                // Coverage and sparse_write stay intact. A stale error from a
                // previous failed cycle no longer applies.
                errs.remove(&item.ino);
                entry.apply_noop_commit(item.dirty_generation);
            }
        }
    }

    ops.append(&mut delete_ops);

    if ops.is_empty() {
        let gc_count = staging.gc(inodes).await;
        info!(
            "Batch flush: all {} file(s) unchanged, no Hub commit needed, {} staging file(s) reclaimed",
            to_flush.len(),
            gc_count
        );
        return retry;
    }

    if let Err(e) = hub_client.batch_operations(&ops).await {
        error!("Batch commit failed: {}", e);
        let msg = format!("commit failed: {e}");
        let mut errs = flush_errors.lock().expect("flush_errors poisoned");
        for (i, item) in to_flush.iter().enumerate() {
            if !unchanged[i] {
                // Preserve a more specific Pass A error (range_upload/
                // snapshot failure) over the generic commit failure: items
                // without an upload result had no op in this commit, so
                // "commit failed" would mislabel their real root cause.
                if upload_results[i].is_some() {
                    // Uploaded but not committed: not yet in `retry` (the
                    // Pass A failure paths pushed the result-less ones).
                    errs.insert(item.ino, msg.clone());
                    retry.push(item.ino);
                } else {
                    errs.entry(item.ino).or_insert_with(|| msg.clone());
                }
            }
        }
        return retry;
    }

    {
        let mut errs = flush_errors.lock().expect("flush_errors poisoned");
        let mut inode_table = inodes.write().expect("inodes poisoned");
        for (i, (item, file_info_opt)) in to_flush.iter().zip(upload_results.iter()).enumerate() {
            let Some(file_info) = file_info_opt else {
                continue; // Pass A failure — leave dirty for retry.
            };
            // This item's commit landed: a flush_errors entry left over from
            // a previous failed cycle no longer reflects reality and would
            // surface a stale EIO on the caller's next fsync.
            errs.remove(&item.ino);
            if !unchanged[i]
                && let Some(entry) = inode_table.get_mut(item.ino)
            {
                // Authoritative size = item.file_size (entry.size captured at
                // flush snapshot under staging.lock + the staging file's
                // on-disk length at upload time, since concurrent O_TRUNC and
                // setattr-shrink also take staging.lock). NOT
                // file_info.file_size() — xet-core's upload_files has been
                // observed to over-count via deduplication_metrics.total_bytes,
                // returning ~65.75 MiB for a true 64 MiB file. Trusting that
                // value would set entry.size > on-disk staging length and
                // poison every subsequent range_upload with a
                // caller/reconstruction size mismatch.
                let size = item.file_size;
                match routes[i] {
                    UploadRoute::SparseCompose => {
                        // range_upload compose: staging holds covered + dirty
                        // bytes that match the new hash, holes outside
                        // coverage do not. apply_commit_sparse re-keys (or
                        // drops, when no handle is left) the sparse state.
                        entry.apply_commit_sparse(file_info.hash(), size, item.dirty_generation);
                    }
                    UploadRoute::FullStaging => {
                        // upload_files: staging IS the committed content
                        // byte-for-byte, so drop sparse_write and flag
                        // staging_is_current. Without this, fully-covered
                        // sparse items would force a full re-download on the
                        // next open even though staging already matches the
                        // committed hash.
                        entry.apply_commit(file_info.hash(), size, item.dirty_generation);
                    }
                }
            }
        }
    }

    let gc_count = staging.gc(inodes).await;

    let changed_count = unchanged.iter().filter(|u| !**u).count();
    let unchanged_count = unchanged.len() - changed_count;
    info!(
        "Batch flush completed: {} file(s) committed, {} unchanged, {} staging file(s) reclaimed",
        changed_count, unchanged_count, gc_count
    );
    // Only per-item upload failures remain to retry; everything else
    // committed. NOTE: this commit was partial when `retry` is non-empty —
    // siblings committed while the failed items' adds (and their rename
    // pending_deletes) did not. The retry loop converges the remote to the
    // full state; a crash before convergence leaves the partial commit
    // (cross-cycle rename consistency was never guaranteed: clean children
    // rename in their own immediate commit already).
    retry
}

#[cfg(test)]
mod tests {
    use super::chunk_dirty_ranges;

    fn group_bytes(group: &[(u64, u64)]) -> u64 {
        group.iter().map(|&(s, e)| e - s).sum()
    }

    #[test]
    fn chunk_dirty_ranges_empty_input_yields_one_empty_group() {
        // Pure-truncate flushes (size changed, no dirty bytes) still need
        // exactly one range_upload call.
        assert_eq!(chunk_dirty_ranges(&[], 8), vec![Vec::new()]);
    }

    #[test]
    fn chunk_dirty_ranges_under_budget_single_group() {
        let ranges = vec![(0, 4), (10, 14)];
        assert_eq!(chunk_dirty_ranges(&ranges, 100), vec![ranges.clone()]);
    }

    #[test]
    fn chunk_dirty_ranges_groups_bounded_and_lossless() {
        let ranges = vec![(0, 10), (15, 25), (30, 38)];
        let groups = chunk_dirty_ranges(&ranges, 8);
        for group in &groups {
            assert!(group_bytes(group) <= 8, "group over budget: {group:?}");
        }
        // Concatenated groups must re-coalesce to the input exactly.
        let mut flat: Vec<(u64, u64)> = groups.concat();
        flat.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (s, e) in flat {
            match merged.last_mut() {
                Some(last) if last.1 == s => last.1 = e,
                _ => merged.push((s, e)),
            }
        }
        assert_eq!(merged, ranges);
    }

    #[test]
    fn chunk_dirty_ranges_splits_single_oversized_range() {
        let groups = chunk_dirty_ranges(&[(0, 20)], 8);
        assert_eq!(groups, vec![vec![(0, 8)], vec![(8, 16)], vec![(16, 20)]]);
    }

    #[test]
    fn chunk_dirty_ranges_zero_budget_clamped() {
        // budget 0 must not loop forever; clamps to 1.
        let groups = chunk_dirty_ranges(&[(0, 2)], 0);
        assert_eq!(groups, vec![vec![(0, 1)], vec![(1, 2)]]);
    }
}
