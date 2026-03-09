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
    tx: Mutex<Option<mpsc::UnboundedSender<FlushRequest>>>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    errors: Arc<Mutex<HashMap<u64, String>>>,
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
        let (tx, rx) = mpsc::unbounded_channel::<FlushRequest>();

        let bg_errors = errors.clone();
        let handle = runtime.spawn(flush_loop(
            rx,
            xet_sessions,
            staging_dir,
            hub_client,
            inodes,
            bg_errors,
            debounce,
            max_batch_window,
        ));

        Self {
            tx: Mutex::new(Some(tx)),
            handle: Mutex::new(Some(handle)),
            errors,
        }
    }

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
