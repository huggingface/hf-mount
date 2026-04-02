use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Once, RwLock};
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
    /// Ensures shutdown runs exactly once. Subsequent calls are no-ops.
    shutdown_once: Once,
}

impl FlushManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        xet_sessions: Arc<dyn XetOps>,
        staging_dir: StagingDir,
        hub_client: Arc<dyn HubOps>,
        inodes: Arc<RwLock<InodeTable>>,
        runtime: &tokio::runtime::Handle,
        debounce: Duration,
        max_batch_window: Duration,
        #[cfg(feature = "encrypt")] encryption_config: Option<crate::crypto::EncryptionConfig>,
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
            #[cfg(feature = "encrypt")]
            encryption_config,
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
    #[cfg(feature = "encrypt")] encryption_config: Option<crate::crypto::EncryptionConfig>,
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
                #[cfg(feature = "encrypt")]
                &encryption_config,
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

#[cfg(feature = "encrypt")]
fn encrypt_staging(
    staging_path: &std::path::Path,
    ino: u64,
    inodes: &RwLock<InodeTable>,
    config: &crate::crypto::EncryptionConfig,
) -> crate::error::Result<PathBuf> {
    let algorithm = inodes
        .read()
        .expect("inodes poisoned")
        .get(ino)
        .and_then(|e| e.file_algorithm)
        .unwrap_or(config.algorithm);
    let derived_key = config.derive_key(algorithm);
    let enc_path = staging_path.with_extension("enc");
    crate::crypto::encrypt_file(staging_path, &enc_path, &derived_key, algorithm, config.chunk_size)?;
    Ok(enc_path)
}

#[cfg(feature = "encrypt")]
fn build_flush_content_type(
    ino: u64,
    plaintext_size: u64,
    ciphertext_size: u64,
    inodes: &RwLock<InodeTable>,
    config: &crate::crypto::EncryptionConfig,
) -> String {
    let algorithm = inodes
        .read()
        .expect("inodes poisoned")
        .get(ino)
        .and_then(|e| e.file_algorithm)
        .unwrap_or(config.algorithm);
    crate::crypto::format_content_type(&crate::crypto::EncryptedFileInfo {
        algorithm,
        plaintext_size,
        ciphertext_size,
        chunk_size: config.chunk_size,
    })
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
    #[cfg(feature = "encrypt")] encryption_config: &Option<crate::crypto::EncryptionConfig>,
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
            plaintext_size: entry.size,
            staging_path,
            pending_deletes: entry.pending_deletes.clone(),
            dirty_generation: entry.dirty_generation,
        }
    };

    #[cfg(feature = "encrypt")]
    let enc_path = encryption_config
        .as_ref()
        .map(|config| encrypt_staging(&item.staging_path, ino, inodes, config))
        .transpose()
        .map_err(|e| {
            error!("flush_one: encryption failed for ino={}: {}", ino, e);
            libc::EIO
        })?;

    #[cfg(feature = "encrypt")]
    let upload_path = enc_path.as_deref().unwrap_or(item.staging_path.as_path());
    #[cfg(not(feature = "encrypt"))]
    let upload_path = item.staging_path.as_path();

    let upload_results = xet_sessions.upload_files(&[upload_path]).await.map_err(|e| {
        error!("flush_one upload failed for ino={}: {}", ino, e);
        libc::EIO
    })?;
    let file_info = &upload_results[0];

    // Clean up temp encrypted file
    #[cfg(feature = "encrypt")]
    if let Some(ref p) = enc_path {
        let _ = std::fs::remove_file(p);
    }

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
            entry.apply_commit(file_info.hash(), item.plaintext_size, item.dirty_generation);
        }
        debug!("flush_one: skipped commit for ino={} (same hash)", ino);
        return Ok(());
    }

    #[cfg(feature = "encrypt")]
    let content_type = enc_path.as_ref().map(|_| {
        build_flush_content_type(
            ino,
            item.plaintext_size,
            file_info.file_size(),
            inodes,
            encryption_config.as_ref().unwrap(),
        )
    });
    #[cfg(not(feature = "encrypt"))]
    let content_type: Option<String> = None;

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut ops = vec![BatchOp::AddFile {
        path: item.full_path.clone(),
        xet_hash: file_info.hash().to_string(),
        mtime: mtime_ms,
        content_type,
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
        entry.apply_commit(file_info.hash(), item.plaintext_size, item.dirty_generation);
        #[cfg(feature = "encrypt")]
        if let (Some(_), Some(config)) = (&enc_path, encryption_config.as_ref()) {
            entry.mark_encrypted(file_info.file_size(), config.algorithm);
        }
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
    plaintext_size: u64,
}

async fn flush_batch(
    pending: Vec<u64>,
    xet_sessions: &dyn XetOps,
    staging_dir: &StagingDir,
    hub_client: &dyn HubOps,
    inodes: &RwLock<InodeTable>,
    flush_errors: &Mutex<HashMap<u64, String>>,
    #[cfg(feature = "encrypt")] encryption_config: &Option<crate::crypto::EncryptionConfig>,
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
                    plaintext_size: entry.size,
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

    #[cfg(feature = "encrypt")]
    let encrypted_paths: Vec<Option<PathBuf>> = if let Some(config) = encryption_config {
        to_flush
            .iter()
            .map(
                |item| match encrypt_staging(&item.staging_path, item.ino, inodes, config) {
                    Ok(p) => Some(p),
                    Err(e) => {
                        error!("flush: encryption failed for ino={}: {}", item.ino, e);
                        flush_errors
                            .lock()
                            .expect("flush_errors poisoned")
                            .insert(item.ino, format!("encryption failed: {e}"));
                        None
                    }
                },
            )
            .collect()
    } else {
        to_flush.iter().map(|_| None).collect()
    };

    // Upload in chunks to bound FD usage (xet-core opens all staging files per
    // upload session), but accumulate all batch ops for a single Hub commit to
    // preserve the global adds-before-deletes ordering required by the Hub API.
    const UPLOAD_CHUNK_SIZE: usize = 500;
    let mut all_results = Vec::new();

    for (chunk_idx, chunk) in to_flush.chunks(UPLOAD_CHUNK_SIZE).enumerate() {
        // Determine upload paths: encrypted temp files or plaintext staging files
        #[cfg(feature = "encrypt")]
        let upload_paths: Vec<&std::path::Path> = {
            let start = chunk_idx * UPLOAD_CHUNK_SIZE;
            chunk
                .iter()
                .zip(encrypted_paths[start..start + chunk.len()].iter())
                .map(|(item, enc)| enc.as_deref().unwrap_or(item.staging_path.as_path()))
                .collect()
        };
        #[cfg(not(feature = "encrypt"))]
        let upload_paths: Vec<&std::path::Path> = chunk.iter().map(|item| item.staging_path.as_path()).collect();

        match xet_sessions.upload_files(&upload_paths).await {
            Ok(results) => all_results.push((chunk_idx, results)),
            Err(e) => {
                // Abort the entire batch: committing partial results could apply
                // deletes without the corresponding adds from this failed chunk.
                error!("Batch upload failed (chunk {}), aborting flush: {}", chunk_idx, e);
                let msg = format!("upload failed: {e}");
                let mut errs = flush_errors.lock().expect("flush_errors poisoned");
                for item in &to_flush {
                    errs.insert(item.ino, msg.clone());
                }
                return;
            }
        }
    }

    if all_results.is_empty() {
        return;
    }

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Build batch operations -- Hub API requires all adds before all deletes.
    let mut ops = Vec::new();
    let mut delete_ops = Vec::new();

    for (chunk_idx, results) in &all_results {
        let chunk_start = chunk_idx * UPLOAD_CHUNK_SIZE;
        let chunk = &to_flush[chunk_start..chunk_start + results.len()];
        #[allow(unused_variables)]
        for (i, (item, file_info)) in chunk.iter().zip(results.iter()).enumerate() {
            info!(
                "Uploaded file ino={} path={} xet_hash={} size={}",
                item.ino,
                item.full_path,
                file_info.hash(),
                file_info.file_size()
            );

            #[cfg(feature = "encrypt")]
            let content_type = {
                let global_idx = chunk_start + i;
                encrypted_paths[global_idx].as_ref().map(|_| {
                    build_flush_content_type(
                        item.ino,
                        item.plaintext_size,
                        file_info.file_size(),
                        inodes,
                        encryption_config.as_ref().unwrap(),
                    )
                })
            };
            #[cfg(not(feature = "encrypt"))]
            let content_type: Option<String> = None;

            ops.push(BatchOp::AddFile {
                path: item.full_path.clone(),
                xet_hash: file_info.hash().to_string(),
                mtime: mtime_ms,
                content_type,
            });

            for old_path in &item.pending_deletes {
                delete_ops.push(BatchOp::DeleteFile { path: old_path.clone() });
            }
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
    for (chunk_idx, results) in &all_results {
        let chunk_start = chunk_idx * UPLOAD_CHUNK_SIZE;
        let chunk = &to_flush[chunk_start..chunk_start + results.len()];
        #[allow(unused_variables)]
        for (i, (item, file_info)) in chunk.iter().zip(results.iter()).enumerate() {
            if let Some(entry) = inode_table.get_mut(item.ino) {
                entry.apply_commit(file_info.hash(), item.plaintext_size, item.dirty_generation);
                #[cfg(feature = "encrypt")]
                if let Some(config) = encryption_config.as_ref() {
                    let global_idx = chunk_start + i;
                    if encrypted_paths[global_idx].is_some() {
                        entry.mark_encrypted(file_info.file_size(), config.algorithm);
                    }
                }
            }
        }
    }

    // Clean up temp encrypted files
    #[cfg(feature = "encrypt")]
    for path in encrypted_paths.iter().flatten() {
        let _ = std::fs::remove_file(path);
    }

    info!("Batch flush completed: {} file(s) committed", to_flush.len());
}
