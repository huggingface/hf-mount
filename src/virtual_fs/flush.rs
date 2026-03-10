use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::hub_api::{BatchOp, HubOps};
use crate::xet::{StagingDir, XetOps};

use super::inode::InodeTable;

type FlushRequest = u64;

// ── FlushManager ──────────────────────────────────────────────────────

pub(crate) struct FlushManager {
    /// Channel for dirty file flush requests (None when advanced_writes is off).
    tx: Mutex<Option<mpsc::UnboundedSender<FlushRequest>>>,
    /// Background flush_loop task handle (None when advanced_writes is off).
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Per-inode flush errors surfaced to callers via check_error().
    errors: Arc<Mutex<HashMap<u64, String>>>,
    /// Queued remote delete paths. Drained in flush_loop, poll, rmdir, and shutdown.
    pending_deletes: Arc<Mutex<Vec<String>>>,
    hub_client: Arc<dyn HubOps>,
    /// True when flush_loop is running (advanced_writes mode). Set once at construction.
    has_flush_loop: bool,
}

impl FlushManager {
    /// Create a FlushManager. Always created, but the background flush_loop
    /// only runs when `advanced_writes` is true.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        xet_sessions: Arc<dyn XetOps>,
        staging_dir: Option<StagingDir>,
        hub_client: Arc<dyn HubOps>,
        inodes: Arc<RwLock<InodeTable>>,
        runtime: &tokio::runtime::Handle,
        debounce: Duration,
        max_batch_window: Duration,
        advanced_writes: bool,
    ) -> Self {
        let errors = Arc::new(Mutex::new(HashMap::new()));
        let pending_deletes = Arc::new(Mutex::new(Vec::new()));

        let (tx, handle) = if advanced_writes {
            let sd = staging_dir.expect("--advanced-writes requires a staging directory");
            let (tx, rx) = mpsc::unbounded_channel::<FlushRequest>();
            let bg_errors = errors.clone();
            let bg_deletes = pending_deletes.clone();
            let bg_hub = hub_client.clone();
            let handle = runtime.spawn(flush_loop(
                rx,
                xet_sessions,
                sd,
                bg_hub,
                inodes,
                bg_errors,
                debounce,
                max_batch_window,
                bg_deletes,
            ));
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        let has_flush_loop = handle.is_some();
        Self {
            tx: Mutex::new(tx),
            handle: Mutex::new(handle),
            errors,
            pending_deletes,
            hub_client,
            has_flush_loop,
        }
    }

    /// Enqueue a dirty inode for flush (advanced_writes only).
    pub(crate) fn enqueue(&self, ino: u64) {
        if let Some(tx) = self.tx.lock().expect("flush_tx poisoned").as_ref()
            && tx.send(ino).is_err()
        {
            error!("Flush channel closed, cannot enqueue ino={}", ino);
        }
    }

    pub(crate) fn check_error(&self, ino: u64) -> Option<String> {
        self.errors.lock().expect("flush_errors poisoned").remove(&ino)
    }

    /// Whether the background flush_loop is running (advanced_writes mode).
    pub(crate) fn has_flush_loop(&self) -> bool {
        self.has_flush_loop
    }

    // ── Remote delete queue ─────────────────────────────────────────

    /// Queue a path for batched remote deletion.
    pub(crate) fn enqueue_delete(&self, path: String) {
        self.pending_deletes
            .lock()
            .expect("pending_deletes poisoned")
            .push(path);
    }

    /// Cancel a queued delete (e.g. when a new file is created at the same path).
    pub(crate) fn cancel_delete(&self, path: &str) {
        let mut q = self.pending_deletes.lock().expect("pending_deletes poisoned");
        if !q.is_empty() {
            q.retain(|p| p != path);
        }
    }

    /// Flush all queued remote deletes in a single batch API call.
    pub(crate) async fn flush_deletes(&self) {
        Self::flush_delete_queue(&self.pending_deletes, &*self.hub_client).await;
    }

    /// Get an Arc clone of the delete queue for the poll loop.
    pub(crate) fn delete_queue(&self) -> Arc<Mutex<Vec<String>>> {
        self.pending_deletes.clone()
    }

    /// Drain `queue` and send all paths as a single `batch_operations` delete.
    /// Re-queues paths on transient failure. Used by flush_deletes(),
    /// the flush_loop, and the poll loop.
    pub(crate) async fn flush_delete_queue(queue: &Mutex<Vec<String>>, hub_client: &dyn HubOps) {
        let paths: Vec<String> = {
            let mut pending = queue.lock().expect("pending_deletes poisoned");
            if pending.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };
        let count = paths.len();
        let ops: Vec<BatchOp> = paths
            .iter()
            .map(|path| BatchOp::DeleteFile { path: path.clone() })
            .collect();
        info!("Flushing {count} queued remote deletes");
        if let Err(e) = hub_client.batch_operations(&ops).await {
            error!("Batch remote delete failed: {}", e);
            let mut pending = queue.lock().expect("pending_deletes poisoned");
            pending.extend(paths);
        }
    }

    // ── Shutdown ────────────────────────────────────────────────────

    pub(crate) fn shutdown(&self, dirty_inos: Vec<u64>, runtime: &tokio::runtime::Handle) {
        // Enqueue all remaining dirty files
        if let Some(tx) = self.tx.lock().expect("flush_tx poisoned").as_ref() {
            for ino in dirty_inos {
                let _ = tx.send(ino);
            }
        }
        // Drop the sender to signal the flush loop to drain and exit
        self.tx.lock().expect("flush_tx poisoned").take();
        // Wait for the flush task to complete.
        // Use block_in_place so this is safe even when called from within the tokio runtime
        // (e.g. NFS shutdown path which runs inside an async context).
        if let Some(handle) = self.handle.lock().expect("flush_handle poisoned").take() {
            let wait = || {
                if let Err(e) = runtime.block_on(handle) {
                    error!("Flush task panicked: {}", e);
                }
            };
            // block_in_place is required when called from within the Tokio runtime
            // (e.g. NFS shutdown), but panics on non-Tokio threads (e.g. FUSE destroy).
            if tokio::runtime::Handle::try_current().is_ok() {
                tokio::task::block_in_place(wait);
            } else {
                wait();
            }
        }
        // Flush remaining queued deletes (flush_loop already drained its share on exit,
        // but deletes queued after the loop exited still need to be sent).
        runtime.block_on(self.flush_deletes());
    }
}

// ── Background tasks ──────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn flush_loop(
    mut rx: mpsc::UnboundedReceiver<FlushRequest>,
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
        // Wait for the first request
        let first = match rx.recv().await {
            Some(req) => req,
            None => return, // channel closed, exit
        };

        let mut pending = vec![first];

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
                Ok(Some(req)) => pending.push(req),
                _ => break, // timeout (debounce expired) or channel closed
            }
        }

        // Flush queued remote deletes alongside dirty writes.
        FlushManager::flush_delete_queue(&pending_deletes, &*hub_client).await;

        let count = pending.len();
        info!("Flushing batch of {} dirty file(s)", count);

        flush_batch(
            pending,
            &*xet_sessions,
            &staging_dir,
            &*hub_client,
            &inodes,
            &flush_errors,
        )
        .await;
    }
}

async fn flush_batch(
    pending: Vec<FlushRequest>,
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
    let to_flush: Vec<(u64, String, PathBuf, Vec<String>)> = {
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
                Some((
                    ino,
                    entry.full_path.clone(),
                    staging_path,
                    entry.pending_deletes.clone(),
                ))
            })
            .collect()
    };

    if to_flush.is_empty() {
        return;
    }

    // Upload all files through a single upload session
    let staging_paths: Vec<&std::path::Path> = to_flush.iter().map(|(_, _, p, _)| p.as_path()).collect();
    let upload_results = match xet_sessions.upload_files(&staging_paths).await {
        Ok(results) => results,
        Err(e) => {
            error!("Batch upload failed: {}", e);
            let msg = format!("upload failed: {e}");
            let mut errs = flush_errors.lock().expect("flush_errors poisoned");
            for (ino, _, _, _) in &to_flush {
                errs.insert(*ino, msg.clone());
            }
            return;
        }
    };

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Build batch operations — Hub API requires all adds before all deletes
    let mut ops = Vec::with_capacity(to_flush.len());
    let mut delete_ops = Vec::new();
    let mut successes: Vec<(u64, String, u64)> = Vec::new();

    for ((ino, full_path, _, pending_deletes), file_info) in to_flush.iter().zip(upload_results.iter()) {
        info!(
            "Uploaded file ino={} path={} xet_hash={} size={}",
            ino,
            full_path,
            file_info.hash(),
            file_info.file_size()
        );

        ops.push(BatchOp::AddFile {
            path: full_path.clone(),
            xet_hash: file_info.hash().to_string(),
            mtime: mtime_ms,
            content_type: None,
        });

        // Delete old remote paths left behind by rename of dirty files
        for old_path in pending_deletes {
            delete_ops.push(BatchOp::DeleteFile { path: old_path.clone() });
        }

        successes.push((*ino, file_info.hash().to_string(), file_info.file_size()));
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
            for (ino, _, _, _) in &to_flush {
                errs.insert(*ino, msg.clone());
            }
            // Files remain dirty -- will be re-uploaded on next flush or shutdown
            return;
        }
        info!("Batch commit retry succeeded");
    }

    // Update inodes.
    // BUG: setting dirty=false unconditionally can lose writes from a concurrent
    // writable handle on the same inode. If handle A flushes and clears dirty,
    // handle B's subsequent release sees !dirty and skips the flush. Fix requires
    // a dirty generation counter instead of a bool.
    let mut inode_table = inodes.write().expect("inodes poisoned");
    let now = SystemTime::now();
    for (ino, xet_hash, size) in successes {
        if let Some(entry) = inode_table.get_mut(ino) {
            entry.xet_hash = Some(xet_hash);
            entry.size = size;
            entry.dirty = false;
            entry.mtime = now;
            entry.ctime = now;
            entry.pending_deletes.clear();
        }
    }

    info!("Batch flush completed: {} file(s) committed", to_flush.len());
}
