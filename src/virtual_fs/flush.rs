use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Once, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::hub_api::{BatchOp, HubOps};
use crate::xet::XetOps;

use super::inode::InodeTable;
use super::staging::StagingCoordinator;

enum FlushSignal {
    /// Flush a dirty inode.
    Dirty(u64),
    /// Wake the loop to drain pending remote deletes (no dirty inode attached).
    WakeDeletes,
    /// Tell `flush_loop` to drop its self-referencing sender clone so the
    /// channel can close once the outer `FlushManager::tx` is dropped. Sent
    /// by `FlushManager::shutdown` just before it drops the outer tx —
    /// without this, the clone `flush_loop` holds (for `requeue_siblings`)
    /// keeps the channel alive forever and shutdown deadlocks waiting on
    /// the loop's join handle.
    Shutdown,
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
    ) -> Self {
        let errors = Arc::new(Mutex::new(HashMap::new()));
        let pending_deletes = Arc::new(Mutex::new(Vec::new()));

        let (tx, rx) = mpsc::unbounded_channel::<FlushSignal>();
        let bg_errors = errors.clone();
        let bg_deletes = pending_deletes.clone();
        let bg_hub = hub_client.clone();
        let bg_tx = tx.clone();
        let handle = runtime.spawn(flush_loop(
            rx,
            bg_tx,
            xet_sessions,
            staging,
            bg_hub,
            inodes,
            bg_errors,
            debounce,
            max_batch_window,
            bg_deletes,
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

    pub(crate) fn shutdown(&self, dirty_inos: Vec<u64>, runtime: &tokio::runtime::Handle) {
        // Run shutdown exactly once. Subsequent calls (e.g. signal handler
        // racing with destroy()) are no-ops.
        self.shutdown_once.call_once(|| {
            // Enqueue all remaining dirty files
            if let Some(tx) = self.tx.lock().expect("flush_tx poisoned").as_ref() {
                for ino in dirty_inos {
                    let _ = tx.send(FlushSignal::Dirty(ino));
                }
                // Tell flush_loop to drop its self-referencing sender so the
                // channel can close after we drop ours below. flush_loop
                // holds a clone of `tx` (for `requeue_siblings` on abort);
                // without this signal, that clone would keep the channel
                // alive forever and shutdown would deadlock.
                let _ = tx.send(FlushSignal::Shutdown);
            }
            // Drop the sender to signal the flush loop to drain and exit
            self.tx.lock().expect("flush_tx poisoned").take();

            // Take the handle then drop the guard before blocking.
            let handle = self.handle.lock().expect("flush_handle poisoned").take();
            if let Some(handle) = handle {
                // Use block_in_place so this is safe even when called from within
                // the tokio runtime (e.g. NFS shutdown path).
                run_blocking(|| {
                    if let Err(err) = runtime.block_on(handle) {
                        error!("Flush task panicked: {}", err);
                    }
                });
            }
            // Flush remaining queued deletes.
            run_blocking(|| runtime.block_on(self.flush_deletes()));
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
/// `signal_tx`: self-referencing sender — passed to `flush_batch` so it can
/// re-enqueue still-dirty siblings when a batch aborts mid-upload. Without
/// this, an abort leaves the surviving items dirty but invisible to the loop
/// until some other code path enqueues them (which may never happen,
/// stranding the bytes on disk and never committing them to the Hub).
async fn flush_loop(
    mut rx: mpsc::UnboundedReceiver<FlushSignal>,
    signal_tx: mpsc::UnboundedSender<FlushSignal>,
    xet_sessions: Arc<dyn XetOps>,
    staging: Arc<StagingCoordinator>,
    hub_client: Arc<dyn HubOps>,
    inodes: Arc<RwLock<InodeTable>>,
    flush_errors: Arc<Mutex<HashMap<u64, String>>>,
    debounce: Duration,
    max_batch_window: Duration,
    pending_deletes: Arc<Mutex<Vec<String>>>,
) {
    // Wrap the self-referencing sender in Option so we can drop it on
    // `Shutdown`. Keeping it alive past shutdown would prevent the channel
    // from closing and `rx.recv()` would block forever.
    let mut signal_tx = Some(signal_tx);
    loop {
        // Wait for the first signal
        let first = match rx.recv().await {
            Some(FlushSignal::Shutdown) => {
                // Drop our sender clone so the channel can close after the
                // outer FlushManager.tx is dropped. Continue draining any
                // remaining signals already in the buffer (Dirty/WakeDeletes
                // queued before Shutdown) — those still represent real work.
                signal_tx = None;
                continue;
            }
            Some(sig) => sig,
            None => return, // channel closed, exit
        };

        let mut signals = vec![first];

        // Debounce: keep collecting for debounce duration after each new item,
        // but cap total wait at max_batch_window to avoid unbounded delay.
        let window_deadline = tokio::time::Instant::now() + max_batch_window;
        loop {
            let remaining = window_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let timeout = debounce.min(remaining);
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(FlushSignal::Shutdown)) => {
                    // Same as the outer arm: drop our sender clone, continue
                    // draining the rest of this debounce window so queued
                    // dirty work still gets flushed before the loop exits.
                    signal_tx = None;
                }
                Ok(Some(sig)) => signals.push(sig),
                _ => break, // timeout (debounce expired) or channel closed
            }
        }

        // Flush queued remote deletes alongside dirty writes.
        flush_pending_deletes(&pending_deletes, &*hub_client).await;

        // Extract dirty inode IDs (WakeDeletes/Shutdown carry no inode).
        let dirty_inos: Vec<u64> = signals
            .into_iter()
            .filter_map(|sig| match sig {
                FlushSignal::Dirty(ino) => Some(ino),
                FlushSignal::WakeDeletes | FlushSignal::Shutdown => None,
            })
            .collect();

        if !dirty_inos.is_empty() {
            info!("Flushing batch of {} dirty file(s)", dirty_inos.len());
            flush_batch(
                dirty_inos,
                &*xet_sessions,
                &staging,
                &*hub_client,
                &inodes,
                &flush_errors,
                signal_tx.as_ref(),
            )
            .await;
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

/// Mark the given items with `msg` in the flush error map. Pass only the items
/// whose own upload genuinely failed — siblings whose CAS upload succeeded in
/// a prior chunk (or were never reached) stay dirty and will retry on the next
/// flush cycle; surfacing an error on them would produce spurious EIO on files
/// whose bytes are already in CAS.
fn abort_batch(items: &[FlushItem], flush_errors: &Mutex<HashMap<u64, String>>, msg: String) {
    error!("Aborting flush ({} item(s) affected): {}", items.len(), msg);
    let mut errs = flush_errors.lock().expect("flush_errors poisoned");
    for it in items {
        errs.insert(it.ino, msg.clone());
    }
}

/// Re-enqueue dirty siblings after a batch abort.
///
/// `to_flush` is the entire batch that was being processed; `failed_inos`
/// are the ones whose own upload genuinely failed (now in `flush_errors`).
/// Everything else is still dirty in the inode table but has lost its
/// queue signal — without re-enqueueing, those bytes would sit on disk
/// indefinitely with nothing to wake the flush loop.
///
/// This includes BOTH items already uploaded earlier in the batch (which
/// never got their Hub commit because we aborted before that step) and
/// items not yet reached. Both still need a fresh flush cycle.
fn requeue_siblings(
    to_flush: &[FlushItem],
    failed_inos: &[u64],
    signal_tx: Option<&mpsc::UnboundedSender<FlushSignal>>,
) {
    let Some(tx) = signal_tx else {
        // Shutdown in progress — don't try to re-enqueue, the loop is exiting.
        return;
    };
    let failed: HashSet<u64> = failed_inos.iter().copied().collect();
    let mut requeued = 0;
    for it in to_flush {
        if failed.contains(&it.ino) {
            continue;
        }
        if tx.send(FlushSignal::Dirty(it.ino)).is_err() {
            // Channel closed (rare — outer FlushManager.tx was dropped
            // without a Shutdown signal). The siblings stay visible as
            // dirty in the inode table but won't be flushed this run.
            return;
        }
        requeued += 1;
    }
    if requeued > 0 {
        warn!("Re-enqueued {} sibling(s) after batch abort", requeued);
    }
}

/// Length of the contiguous run of non-sparse FlushItems starting at `start`,
/// capped to `max` items. `start` must point to a non-sparse item — sparse
/// items are dispatched one-by-one through `range_upload` and never get
/// included in a batched `upload_files` chunk.
fn find_regular_run_end(items: &[FlushItem], start: usize, max: usize) -> usize {
    debug_assert!(items[start].sparse_write.is_none());
    let upper = (start + max).min(items.len());
    items[start..upper]
        .iter()
        .take_while(|it| it.sparse_write.is_none())
        .count()
        + start
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
    /// Size of the file as the user sees it (including sparse holes).
    file_size: u64,
    /// Set when the staging file is sparse and only the dirty windows should be
    /// re-uploaded via `range_upload` (composing CAS prefix/suffix).
    sparse_write: Option<Arc<crate::virtual_fs::inode::SparseWriteState>>,
}

#[allow(clippy::too_many_arguments)]
/// `signal_tx`: `None` after `Shutdown` was observed — siblings won't be
/// re-enqueued because the loop is winding down anyway. They stay dirty on
/// disk and are picked up by the next mount.
async fn flush_batch(
    pending: Vec<u64>,
    xet_sessions: &dyn XetOps,
    staging: &StagingCoordinator,
    hub_client: &dyn HubOps,
    inodes: &RwLock<InodeTable>,
    flush_errors: &Mutex<HashMap<u64, String>>,
    signal_tx: Option<&mpsc::UnboundedSender<FlushSignal>>,
) {
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
                    let msg = format!("staging file missing for {}", entry.full_path);
                    error!("flush: ino={} {}, skipping", ino, msg);
                    flush_errors.lock().expect("flush_errors poisoned").insert(ino, msg);
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
                    sparse_write: entry.sparse_write.clone(),
                })
            })
            .collect()
    };

    if to_flush.is_empty() {
        // Drop guards before gc — gc_one acquires the same per-inode locks.
        drop(_staging_guards);
        // Still reclaim if other clean staging files pushed us over budget.
        staging.gc(inodes).await;
        return;
    }

    // Sparse items (opened for write without downloading) go through `range_upload`
    // which composes CAS prefix/suffix segments with re-chunked dirty windows; regular
    // items go through the batched `upload_files` path. We walk in order, batching
    // contiguous runs of regular items so a single Hub commit preserves the original
    // adds-before-deletes ordering. Chunk size bounds FD usage (xet-core opens all
    // staging files per upload session).
    const UPLOAD_CHUNK_SIZE: usize = 500;
    let mut upload_results: Vec<xet_data::processing::XetFileInfo> = Vec::with_capacity(to_flush.len());

    let mut i = 0;
    while i < to_flush.len() {
        let item = &to_flush[i];
        if let Some(sw) = &item.sparse_write {
            match xet_sessions.range_upload(sw, &item.staging_path, item.file_size).await {
                Ok(file_info) => {
                    debug!(
                        "flush: range_upload ino={} path={} hash={} size={}",
                        item.ino,
                        item.full_path,
                        file_info.hash(),
                        file_info.file_size().unwrap_or(0)
                    );
                    upload_results.push(file_info);
                }
                Err(e) => {
                    // Only the failing sparse item gets the error. Sibling
                    // items in this batch keep their dirty state and will be
                    // retried on the next flush cycle (CAS dedup makes the
                    // re-upload cheap); marking them errored here would
                    // surface spurious EIO on files whose data is fine.
                    let failed_ino = item.ino;
                    abort_batch(
                        std::slice::from_ref(item),
                        flush_errors,
                        format!("range_upload failed (ino={} path={}): {e}", item.ino, item.full_path),
                    );
                    requeue_siblings(&to_flush, &[failed_ino], signal_tx);
                    return;
                }
            }
            i += 1;
            continue;
        }

        let chunk_end = find_regular_run_end(&to_flush, i, UPLOAD_CHUNK_SIZE);
        let chunk = &to_flush[i..chunk_end];
        let staging_paths: Vec<&std::path::Path> = chunk.iter().map(|it| it.staging_path.as_path()).collect();
        match xet_sessions.upload_files(&staging_paths).await {
            Ok(results) => {
                assert_eq!(
                    results.len(),
                    chunk.len(),
                    "upload_files returned {} results for {} inputs",
                    results.len(),
                    chunk.len()
                );
                upload_results.extend(results);
            }
            Err(e) => {
                // Only mark the chunk that actually failed; items in other
                // chunks (already uploaded or not yet reached) stay dirty
                // and need to be re-enqueued so a future flush cycle picks
                // them up — otherwise they'd sit dirty forever with no
                // signal to wake the flush loop.
                let failed_inos: Vec<u64> = chunk.iter().map(|it| it.ino).collect();
                abort_batch(chunk, flush_errors, format!("upload failed: {e}"));
                requeue_siblings(&to_flush, &failed_inos, signal_tx);
                return;
            }
        }
        i = chunk_end;
    }

    // Uploads are done — drop the staging locks so unlink/truncate and the
    // gc() calls below (which acquire the same per-inode locks) can proceed.
    drop(_staging_guards);

    if upload_results.is_empty() {
        return;
    }

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Hub API requires all adds before all deletes.
    let mut ops = Vec::new();
    let mut delete_ops = Vec::new();
    let mut unchanged = vec![false; to_flush.len()];

    for (i, (item, file_info)) in to_flush.iter().zip(upload_results.iter()).enumerate() {
        // Was this a no-op upload? Two cases:
        //  * Sparse: `range_upload` returns `sparse_write.original_hash` when
        //    dirty_ranges is empty and the size matches — i.e. the open made no
        //    actual modifications since the snapshot. Compare against the
        //    snapshot, NOT `entry.xet_hash`, because `poll_remote_changes` may
        //    have updated the inode mid-flight; using prev_xet_hash here would
        //    treat the no-op as a change and roll the remote back to the snapshot.
        //  * Regular: full upload preserves the prior hash when content is
        //    identical (idempotent edits).
        let is_no_op = if let Some(sw) = &item.sparse_write {
            file_info.hash() == sw.original_hash
        } else {
            item.prev_xet_hash.as_deref() == Some(file_info.hash())
        };
        if item.pending_deletes.is_empty() && is_no_op {
            debug!(
                "flush_batch: unchanged ino={} path={} (hash {})",
                item.ino,
                item.full_path,
                file_info.hash()
            );
            unchanged[i] = true;
            continue;
        }
        info!(
            "Uploaded file ino={} path={} xet_hash={} size={}",
            item.ino,
            item.full_path,
            file_info.hash(),
            file_info.file_size().expect("upload returned XetFileInfo without size")
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
    // Use apply_noop_commit (not apply_commit) so we don't rewrite xet_hash/size
    // — they may have legitimately been updated by poll_remote_changes during
    // the open window.
    {
        let mut inode_table = inodes.write().expect("inodes poisoned");
        for (i, item) in to_flush.iter().enumerate() {
            if unchanged[i]
                && let Some(entry) = inode_table.get_mut(item.ino)
            {
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
        return;
    }

    if let Err(e) = hub_client.batch_operations(&ops).await {
        error!("Batch commit failed: {}", e);
        let msg = format!("commit failed: {e}");
        let mut errs = flush_errors.lock().expect("flush_errors poisoned");
        for (i, item) in to_flush.iter().enumerate() {
            if !unchanged[i] {
                errs.insert(item.ino, msg.clone());
            }
        }
        return;
    }

    {
        let mut inode_table = inodes.write().expect("inodes poisoned");
        for (i, (item, file_info)) in to_flush.iter().zip(upload_results.iter()).enumerate() {
            if !unchanged[i]
                && let Some(entry) = inode_table.get_mut(item.ino)
            {
                entry.apply_commit(
                    file_info.hash(),
                    file_info.file_size().expect("upload returned XetFileInfo without size"),
                    item.dirty_generation,
                    item.sparse_write.is_some(),
                );
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
}
