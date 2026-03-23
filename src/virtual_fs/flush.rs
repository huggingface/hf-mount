use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::hub_api::{BatchOp, HubOps};
use crate::xet::{StagingDir, XetOps};

use super::inode::{InodeTable, SparseWriteState};

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
}

impl FlushManager {
    pub(crate) fn new(
        xet_sessions: Arc<dyn XetOps>,
        staging_dir: StagingDir,
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
        let handle = runtime.spawn(flush_loop(
            rx,
            xet_sessions,
            staging_dir,
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
        // Enqueue all remaining dirty files
        if let Some(tx) = self.tx.lock().expect("flush_tx poisoned").as_ref() {
            for ino in dirty_inos {
                let _ = tx.send(FlushSignal::Dirty(ino));
            }
        }
        // Drop the sender to signal the flush loop to drain and exit
        self.tx.lock().expect("flush_tx poisoned").take();
        // Wait for the flush task to complete.
        // Use block_in_place so this is safe even when called from within the tokio runtime
        // (e.g. NFS shutdown path which runs inside an async context).
        if let Some(handle) = self.handle.lock().expect("flush_handle poisoned").take() {
            run_blocking(|| {
                if let Err(e) = runtime.block_on(handle) {
                    error!("Flush task panicked: {}", e);
                }
            });
        }
        // Flush remaining queued deletes.
        run_blocking(|| runtime.block_on(self.flush_deletes()));
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
    staging_dir: StagingDir,
    hub_client: Arc<dyn HubOps>,
    inodes: Arc<RwLock<InodeTable>>,
    flush_errors: Arc<Mutex<HashMap<u64, String>>>,
    debounce: Duration,
    max_batch_window: Duration,
    pending_deletes: Arc<Mutex<Vec<String>>>,
) {
    loop {
        // Wait for the first signal
        let first = match rx.recv().await {
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
                Ok(Some(sig)) => signals.push(sig),
                _ => break, // timeout (debounce expired) or channel closed
            }
        }

        // Flush queued remote deletes alongside dirty writes.
        flush_pending_deletes(&pending_deletes, &*hub_client).await;

        // Extract dirty inode IDs (WakeDeletes signals carry no inode).
        let dirty_inos: Vec<u64> = signals
            .into_iter()
            .filter_map(|sig| match sig {
                FlushSignal::Dirty(ino) => Some(ino),
                FlushSignal::WakeDeletes => None,
            })
            .collect();

        if !dirty_inos.is_empty() {
            info!("Flushing batch of {} dirty file(s)", dirty_inos.len());
            flush_batch(
                dirty_inos,
                &*xet_sessions,
                &staging_dir,
                &*hub_client,
                &inodes,
                &flush_errors,
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

/// Info extracted from inode table for a single dirty file to flush.
struct FlushEntry {
    ino: u64,
    full_path: String,
    staging_path: PathBuf,
    pending_deletes: Vec<String>,
    /// If set, the file was opened for write without downloading the original (sparse staging).
    sparse_write: Option<Arc<SparseWriteState>>,
    /// Current file size from the inode (avoids a stat syscall during upload).
    file_size: u64,
    /// Snapshot of flush_generation at the time we read the inode.
    flush_generation: u64,
}

async fn flush_batch(
    pending: Vec<u64>,
    xet_sessions: &dyn XetOps,
    staging_dir: &StagingDir,
    hub_client: &dyn HubOps,
    inodes: &RwLock<InodeTable>,
    flush_errors: &Mutex<HashMap<u64, String>>,
) {
    // Dedup by inode (keep last request per ino)
    let mut seen = HashSet::new();
    let deduped: Vec<u64> = pending
        .into_iter()
        .rev()
        .filter(|ino| seen.insert(*ino))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    debug!("flush_batch: deduped inos = {:?}", deduped);

    // Resolve paths from inode table, skip deleted/non-dirty/unlinked inodes
    let to_flush: Vec<FlushEntry> = {
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
                if !entry.dirty {
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
                Some(FlushEntry {
                    ino,
                    full_path: entry.full_path.clone(),
                    staging_path,
                    pending_deletes: entry.pending_deletes.clone(),
                    sparse_write: entry.sparse_write.clone(),
                    file_size: entry.size,
                    flush_generation: entry.flush_generation,
                })
            })
            .collect()
    };

    if to_flush.is_empty() {
        return;
    }

    // Partition into sparse files (can use range_upload) and regular files (full upload).
    let (sparse_files, regular_files): (Vec<&FlushEntry>, Vec<&FlushEntry>) =
        to_flush.iter().partition(|e| e.sparse_write.is_some());

    // Upload results: (ino, full_path, pending_deletes, file_info)
    let mut upload_results: Vec<(&FlushEntry, xet_data::processing::XetFileInfo)> = Vec::new();

    // Process sparse files individually (range_upload)
    for entry in &sparse_files {
        let sw = entry.sparse_write.as_ref().unwrap();
        match xet_sessions
            .range_upload(sw, &entry.staging_path, entry.file_size)
            .await
        {
            Ok(file_info) => {
                info!(
                    "Range-uploaded file ino={} path={} xet_hash={} size={}",
                    entry.ino,
                    entry.full_path,
                    file_info.hash(),
                    file_info.file_size()
                );
                upload_results.push((entry, file_info));
            }
            Err(e) => {
                // Don't fall back to download_to_file: that would overwrite the staging
                // file (which contains the user's dirty writes) with the original CAS
                // content, silently losing data. Let the error propagate so the flush
                // can be retried.
                let msg = format!("range_upload failed: {e}");
                error!("flush: ino={} path={} {}", entry.ino, entry.full_path, msg);
                flush_errors
                    .lock()
                    .expect("flush_errors poisoned")
                    .insert(entry.ino, msg);
            }
        }
    }

    // Batch upload regular files
    if !regular_files.is_empty() {
        let staging_paths: Vec<&std::path::Path> = regular_files.iter().map(|e| e.staging_path.as_path()).collect();
        match xet_sessions.upload_files(&staging_paths).await {
            Ok(results) => {
                for (entry, file_info) in regular_files.iter().zip(results.into_iter()) {
                    info!(
                        "Uploaded file ino={} path={} xet_hash={} size={}",
                        entry.ino,
                        entry.full_path,
                        file_info.hash(),
                        file_info.file_size()
                    );
                    upload_results.push((entry, file_info));
                }
            }
            Err(e) => {
                error!("Batch upload failed: {}", e);
                let msg = format!("upload failed: {e}");
                let mut errs = flush_errors.lock().expect("flush_errors poisoned");
                for entry in &regular_files {
                    errs.insert(entry.ino, msg.clone());
                }
                // If append uploads succeeded, we still commit those below
                if upload_results.is_empty() {
                    return;
                }
            }
        }
    }

    if upload_results.is_empty() {
        return;
    }

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Build batch operations — Hub API requires all adds before all deletes
    let mut ops = Vec::with_capacity(upload_results.len());
    let mut delete_ops = Vec::new();
    struct FlushSuccess {
        ino: u64,
        xet_hash: String,
        size: u64,
        flush_generation: u64,
        flushed_path: String,
    }
    let mut successes: Vec<FlushSuccess> = Vec::new();

    for (entry, file_info) in &upload_results {
        ops.push(BatchOp::AddFile {
            path: entry.full_path.clone(),
            xet_hash: file_info.hash().to_string(),
            mtime: mtime_ms,
            content_type: None,
        });

        // Delete old remote paths left behind by rename of dirty files
        for old_path in &entry.pending_deletes {
            delete_ops.push(BatchOp::DeleteFile { path: old_path.clone() });
        }

        successes.push(FlushSuccess {
            ino: entry.ino,
            xet_hash: file_info.hash().to_string(),
            size: file_info.file_size(),
            flush_generation: entry.flush_generation,
            flushed_path: entry.full_path.clone(),
        });
    }

    ops.append(&mut delete_ops);

    // Single batch commit (retry once on transient failure since CAS upload already succeeded).
    // Note: the retry reuses the original ops without re-reading inode state. In theory a
    // rename/unlink could happen during the 2s window, but that's extremely unlikely and
    // the next flush cycle would correct it anyway.
    if let Err(e) = hub_client.batch_operations(&ops).await {
        error!("Batch commit failed, retrying in 2s: {}", e);
        tokio::time::sleep(Duration::from_secs(2)).await;

        if let Err(e2) = hub_client.batch_operations(&ops).await {
            error!("Batch commit retry failed: {}", e2);
            let msg = format!("commit failed after retry: {e2}");
            let mut errs = flush_errors.lock().expect("flush_errors poisoned");
            for (entry, _) in &upload_results {
                errs.insert(entry.ino, msg.clone());
            }
            // Files remain dirty -- will be re-uploaded on next flush or shutdown
            return;
        }
        info!("Batch commit retry succeeded");
    }

    // Update inodes. Only clear dirty/sparse_write if no writes happened since
    // the snapshot (flush_generation matches). Otherwise leave the file dirty
    // so the next flush picks up the concurrent writes.
    let mut inode_table = inodes.write().expect("inodes poisoned");
    let now = SystemTime::now();
    for success in successes {
        if let Some(entry) = inode_table.get_mut(success.ino) {
            if entry.flush_generation == success.flush_generation {
                // No concurrent writes since we snapshotted the inode: the remote
                // now matches the staging file, so clear the dirty flag and update
                // the xet_hash to point to the newly uploaded CAS object.
                entry.xet_hash = Some(success.xet_hash);
                entry.size = success.size;
                entry.dirty = false;
                entry.mtime = now;
                entry.ctime = now;
                entry.pending_deletes.clear();
                entry.sparse_write = None;
            } else {
                // A concurrent write (or rename/truncate) bumped flush_generation
                // after our snapshot. The remote now has the version we uploaded,
                // but the local staging file has newer data. Keep the inode dirty
                // so the next flush cycle picks up the concurrent changes.
                //
                // If the file was also renamed, the batch committed at the old path.
                // Queue a delete for that stale remote entry so the next flush
                // (which will use the new path) cleans it up.
                if entry.full_path != success.flushed_path {
                    entry.pending_deletes.push(success.flushed_path);
                }
                debug!(
                    "flush: ino={} generation mismatch (flushed={}, current={}), keeping dirty",
                    success.ino, success.flush_generation, entry.flush_generation
                );
            }
        }
    }

    info!("Batch flush completed: {} file(s) committed", upload_results.len());
}
