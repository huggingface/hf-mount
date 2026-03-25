use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::hub_api::{BatchOp, HubOps};
use crate::xet::{StagingDir, XetOps};

use super::inode::InodeTable;

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

/// Flush a single dirty inode synchronously: upload staging file, commit to Hub,
/// update inode table with generation-aware dirty clear. Returns Ok(()) on success
/// or if the inode is not dirty / doesn't exist / has no staging file.
pub(crate) async fn flush_one(
    ino: u64,
    xet_sessions: &dyn XetOps,
    staging_dir: &StagingDir,
    hub_client: &dyn HubOps,
    inodes: &RwLock<InodeTable>,
) -> Result<(), i32> {
    let item = {
        let inode_table = inodes.read().expect("inodes poisoned");
        let entry = match inode_table.get(ino) {
            Some(e) if e.is_dirty() && e.nlink > 0 => e,
            _ => return Ok(()),
        };
        let staging_path = staging_dir.path(ino);
        if !staging_path.exists() {
            return Ok(());
        }
        FlushItem {
            ino,
            full_path: entry.full_path.clone(),
            staging_path,
            pending_deletes: entry.pending_deletes.clone(),
            dirty_generation: entry.dirty_generation,
        }
    };

    let upload_results = xet_sessions
        .upload_files(&[item.staging_path.as_path()])
        .await
        .map_err(|e| {
            error!("flush_one upload failed for ino={}: {}", ino, e);
            libc::EIO
        })?;
    let file_info = &upload_results[0];

    // Skip Hub commit if content hasn't changed (same hash as last commit).
    // This avoids redundant round-trips when fsync is called repeatedly
    // without new writes (e.g. xfstests write+fsync loops).
    let same_content = {
        let inode_table = inodes.read().expect("inodes poisoned");
        inode_table
            .get(ino)
            .is_some_and(|e| e.xet_hash.as_deref() == Some(file_info.hash()))
    };
    if same_content && item.pending_deletes.is_empty() {
        let mut inode_table = inodes.write().expect("inodes poisoned");
        if let Some(entry) = inode_table.get_mut(ino) {
            entry.apply_commit(file_info.hash(), file_info.file_size(), item.dirty_generation);
        }
        debug!("flush_one: skipped commit for ino={} (same hash)", ino);
        return Ok(());
    }

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut ops = vec![BatchOp::AddFile {
        path: item.full_path.clone(),
        xet_hash: file_info.hash().to_string(),
        mtime: mtime_ms,
        content_type: None,
    }];
    for old_path in &item.pending_deletes {
        ops.push(BatchOp::DeleteFile { path: old_path.clone() });
    }
    hub_client.batch_operations(&ops).await.map_err(|e| {
        error!("flush_one commit failed for {}: {}", item.full_path, e);
        libc::EIO
    })?;

    let mut inode_table = inodes.write().expect("inodes poisoned");
    if let Some(entry) = inode_table.get_mut(ino) {
        entry.apply_commit(file_info.hash(), file_info.file_size(), item.dirty_generation);
    }

    info!("flush_one: committed ino={} path={}", ino, item.full_path);
    Ok(())
}

struct FlushItem {
    ino: u64,
    full_path: String,
    staging_path: PathBuf,
    pending_deletes: Vec<String>,
    dirty_generation: u64,
}

async fn flush_batch(
    pending: Vec<u64>,
    xet_sessions: &dyn XetOps,
    staging_dir: &StagingDir,
    hub_client: &dyn HubOps,
    inodes: &RwLock<InodeTable>,
    flush_errors: &Mutex<HashMap<u64, String>>,
) {
    // Dedup by inode (keep last request per ino).
    // Walk backwards so last occurrence wins, then reverse in-place.
    let mut seen = HashSet::new();
    let mut deduped: Vec<u64> = pending.into_iter().rev().filter(|ino| seen.insert(*ino)).collect();
    deduped.reverse();

    debug!("flush_batch: deduped inos = {:?}", deduped);

    // Resolve paths from inode table, skip deleted/non-dirty/unlinked inodes.
    // Snapshot the dirty_generation so we only clear dirty if no concurrent writer advanced it.
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
                if !entry.is_dirty() || entry.nlink == 0 {
                    debug!(
                        "flush: ino={} path={} not dirty or unlinked, skipping",
                        ino, entry.full_path
                    );
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
                    full_path: entry.full_path.clone(),
                    staging_path,
                    pending_deletes: entry.pending_deletes.clone(),
                    dirty_generation: entry.dirty_generation,
                })
            })
            .collect()
    };

    if to_flush.is_empty() {
        return;
    }

    // Upload all files through a single upload session
    let staging_paths: Vec<&std::path::Path> = to_flush.iter().map(|item| item.staging_path.as_path()).collect();
    let upload_results = match xet_sessions.upload_files(&staging_paths).await {
        Ok(results) => results,
        Err(e) => {
            error!("Batch upload failed: {}", e);
            let msg = format!("upload failed: {e}");
            let mut errs = flush_errors.lock().expect("flush_errors poisoned");
            for item in &to_flush {
                errs.insert(item.ino, msg.clone());
            }
            return;
        }
    };

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Build batch operations -- Hub API requires all adds before all deletes
    let mut ops = Vec::with_capacity(to_flush.len());
    let mut delete_ops = Vec::new();

    for (item, file_info) in to_flush.iter().zip(upload_results.iter()) {
        info!(
            "Uploaded file ino={} path={} xet_hash={} size={}",
            item.ino,
            item.full_path,
            file_info.hash(),
            file_info.file_size()
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

    ops.append(&mut delete_ops);

    // Single batch commit. Transient errors (429, 5xx) are retried by the hub layer.
    if let Err(e) = hub_client.batch_operations(&ops).await {
        error!("Batch commit failed: {}", e);
        let msg = format!("commit failed: {e}");
        let mut errs = flush_errors.lock().expect("flush_errors poisoned");
        for item in &to_flush {
            errs.insert(item.ino, msg.clone());
        }
        return;
    }

    let mut inode_table = inodes.write().expect("inodes poisoned");
    for (item, file_info) in to_flush.iter().zip(upload_results.iter()) {
        if let Some(entry) = inode_table.get_mut(item.ino) {
            entry.apply_commit(file_info.hash(), file_info.file_size(), item.dirty_generation);
        }
    }

    info!("Batch flush completed: {} file(s) committed", to_flush.len());
}
