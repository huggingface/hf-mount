use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use data::{DownloadStream, XetFileInfo};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::cache::FileCache;
use crate::hub_api::{BatchOp, HubApiClient};
use crate::inode::{InodeEntry, InodeKind, InodeTable};

// ── VFS types ──────────────────────────────────────────────────────────

pub type VfsError = i32;
pub type VfsResult<T> = Result<T, VfsError>;

pub struct VfsAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub mtime: SystemTime,
    pub kind: InodeKind,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
}

pub struct VfsDirEntry {
    pub ino: u64,
    pub kind: InodeKind,
    pub name: String,
}

// ── Constants ──────────────────────────────────────────────────────────

const BLOCK_SIZE: u32 = 512;
const DEBOUNCE_DURATION: Duration = Duration::from_secs(2);
const MAX_BATCH_WINDOW: Duration = Duration::from_secs(30);
const NEG_CACHE_CAPACITY: usize = 10_000;
const NEG_CACHE_TTL: Duration = Duration::from_secs(30);

// Prefetch buffer constants
const INITIAL_WINDOW: u64 = 8 * 1_048_576; // 8 MiB
const MAX_WINDOW: u64 = 128 * 1_048_576; // 128 MiB
const SEEK_WINDOW: usize = 1_048_576; // 1 MiB backward seek buffer
const FORWARD_SKIP: u64 = 16 * 1_048_576; // 16 MiB skip without reset

// ── Internal types ─────────────────────────────────────────────────────

/// An open file handle — either a local fd or a lazy remote reference.
enum OpenFile {
    /// Local file (staging for writes, or dirty reads).
    Local { file: Arc<File>, writable: bool },
    /// Lazy remote — data fetched on-demand with adaptive prefetch buffer.
    Lazy { prefetch: Arc<Mutex<PrefetchState>> },
}

/// What to do in read() after releasing the open_files lock.
enum ReadTarget {
    /// Hold an Arc<File> so the FD stays alive even if release() runs concurrently.
    LocalFd(Arc<File>),
    Remote {
        prefetch: Arc<Mutex<PrefetchState>>,
    },
}

/// Wraps `Arc<Mutex<Vec<u8>>>` to implement `Write + Send + 'static`
/// for use with `FileDownloadSession::download_to_writer`.
struct SharedBufWriter(Arc<Mutex<Vec<u8>>>);

impl Write for SharedBufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Per-file-handle prefetch state with adaptive window sizing.
struct PrefetchState {
    xet_hash: String,
    file_size: u64,
    // Forward buffer: bytes [buf_start .. buf_start + data.len())
    data: Vec<u8>,
    buf_start: u64,
    // Backward seek window: bytes [seek_start .. seek_start + seek_data.len())
    seek_data: VecDeque<u8>,
    seek_start: u64,
    // Adaptive window size
    window_size: u64,
    // Full-file stream for sequential reads (reuses one FileReconstructor)
    stream: Option<DownloadStream>,
}

impl PrefetchState {
    fn new(xet_hash: String, file_size: u64) -> Self {
        Self {
            xet_hash,
            file_size,
            data: Vec::new(),
            buf_start: 0,
            seek_data: VecDeque::new(),
            seek_start: 0,
            window_size: INITIAL_WINDOW,
            stream: None,
        }
    }

    /// Try to serve a read from the forward buffer.
    /// Returns the slice if the requested range is fully contained.
    fn try_serve_forward(&self, offset: u64, size: u32) -> Option<&[u8]> {
        if self.data.is_empty() {
            return None;
        }
        let buf_end = self.buf_start + self.data.len() as u64;
        if offset >= self.buf_start && offset < buf_end {
            let local_off = (offset - self.buf_start) as usize;
            let avail = self.data.len() - local_off;
            let to_read = (size as usize).min(avail);
            Some(&self.data[local_off..local_off + to_read])
        } else {
            None
        }
    }

    /// Try to serve a read from the backward seek window.
    fn try_serve_seek(&mut self, offset: u64, size: u32) -> Option<&[u8]> {
        if self.seek_data.is_empty() {
            return None;
        }
        let seek_end = self.seek_start + self.seek_data.len() as u64;
        if offset >= self.seek_start && offset < seek_end {
            let local_off = (offset - self.seek_start) as usize;
            let avail = self.seek_data.len() - local_off;
            let to_read = (size as usize).min(avail);
            let slice = self.seek_data.make_contiguous();
            Some(&slice[local_off..local_off + to_read])
        } else {
            None
        }
    }

    /// Move consumed bytes from the front of the forward buffer into the seek window.
    fn drain_to_seek(&mut self, consumed: usize) {
        if consumed == 0 {
            return;
        }
        let to_move = consumed.min(self.data.len());
        // Only append to seek window if contiguous with forward buffer
        let seek_end = self.seek_start + self.seek_data.len() as u64;
        if self.seek_data.is_empty() || seek_end == self.buf_start {
            if to_move > SEEK_WINDOW {
                // Fast path: only copy the last SEEK_WINDOW bytes instead of all
                // to_move bytes (which can be up to 128 MiB).
                let copy_start = to_move - SEEK_WINDOW;
                self.seek_data.clear();
                self.seek_start = self.buf_start + copy_start as u64;
                self.seek_data.extend(&self.data[copy_start..to_move]);
            } else {
                self.seek_data.extend(&self.data[..to_move]);
                if self.seek_data.len() > SEEK_WINDOW {
                    let excess = self.seek_data.len() - SEEK_WINDOW;
                    drop(self.seek_data.drain(..excess));
                    self.seek_start += excess as u64;
                }
            }
        } else {
            // Gap between seek window and forward buffer — reset seek window
            let keep = to_move.min(SEEK_WINDOW);
            self.seek_data.clear();
            self.seek_data.extend(&self.data[to_move - keep..to_move]);
            self.seek_start = self.buf_start + (to_move - keep) as u64;
        }
        // Remove consumed bytes from forward buffer
        if to_move == self.data.len() {
            self.data.clear();
        } else {
            self.data.drain(..to_move);
        }
        self.buf_start += to_move as u64;
    }
}

/// A request to flush a dirty file to CAS + bucket.
/// Only carries the inode number; full_path and staging_path are resolved
/// at flush time from the inode table to handle renames/unlinks correctly.
struct FlushRequest {
    ino: u64,
}

// ── HfVfsCore ──────────────────────────────────────────────────────────

pub struct HfVfsCore {
    rt: tokio::runtime::Handle,
    hub_client: Arc<HubApiClient>,
    bucket_id: String,
    cache: Arc<FileCache>,
    read_only: bool,
    inodes: Arc<Mutex<InodeTable>>,
    /// Maps fh → OpenFile (local fd or lazy remote reference).
    open_files: Mutex<HashMap<u64, OpenFile>>,
    next_fh: Mutex<u64>,
    uid: u32,
    gid: u32,
    /// Negative lookup cache: paths known to not exist (TTL-based).
    neg_cache: Arc<Mutex<HashMap<String, Instant>>>,
    /// Channel to send dirty files for debounced batch flush.
    flush_tx: Mutex<Option<mpsc::UnboundedSender<FlushRequest>>>,
    /// Handle to the background flush task, used for graceful shutdown.
    flush_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Handle to the background polling task, used for graceful shutdown.
    poll_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Per-inode flush errors reported by the background flush loop.
    /// Checked and cleared by flush to propagate errors to the application.
    flush_errors: Arc<Mutex<HashMap<u64, String>>>,
}

impl HfVfsCore {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rt: tokio::runtime::Handle,
        hub_client: Arc<HubApiClient>,
        bucket_id: String,
        cache: Arc<FileCache>,
        read_only: bool,
        uid: u32,
        gid: u32,
        poll_interval_secs: u64,
    ) -> Self {
        let inodes = Arc::new(Mutex::new(InodeTable::new()));
        let neg_cache = Arc::new(Mutex::new(HashMap::new()));

        let flush_errors = Arc::new(Mutex::new(HashMap::new()));

        let (flush_tx, flush_handle) = if !read_only {
            let (tx, rx) = mpsc::unbounded_channel::<FlushRequest>();
            let bg_cache = cache.clone();
            let bg_hub = hub_client.clone();
            let bg_bucket = bucket_id.clone();
            let bg_inodes = inodes.clone();
            let bg_flush_errors = flush_errors.clone();

            let handle = rt.spawn(Self::flush_loop(
                rx,
                bg_cache,
                bg_hub,
                bg_bucket,
                bg_inodes,
                bg_flush_errors,
            ));

            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        // Spawn remote change polling task (if interval > 0)
        let poll_handle = if poll_interval_secs > 0 {
            let bg_hub = hub_client.clone();
            let bg_bucket = bucket_id.clone();
            let bg_inodes = inodes.clone();
            let bg_cache = cache.clone();
            let bg_neg_cache = neg_cache.clone();
            let interval = Duration::from_secs(poll_interval_secs);

            Some(rt.spawn(Self::poll_remote_changes(
                bg_hub,
                bg_bucket,
                bg_inodes,
                bg_cache,
                bg_neg_cache,
                interval,
            )))
        } else {
            None
        };

        Self {
            rt,
            hub_client,
            bucket_id,
            cache,
            read_only,
            inodes,
            open_files: Mutex::new(HashMap::new()),
            next_fh: Mutex::new(1),
            uid,
            gid,
            neg_cache,
            flush_tx: Mutex::new(flush_tx),
            flush_handle: Mutex::new(flush_handle),
            poll_handle: Mutex::new(poll_handle),
            flush_errors,
        }
    }

    /// Graceful shutdown: abort polling, drain flush queue, wait for completion.
    pub fn shutdown(&self) {
        info!("Shutting down VFS, flushing pending writes...");
        // Abort the polling task.
        if let Some(handle) = self.poll_handle.lock().unwrap().take() {
            handle.abort();
        }
        // Drop the sender to signal the flush loop to drain and exit.
        self.flush_tx.lock().unwrap().take();
        // Wait for the flush task to complete (processes remaining items).
        if let Some(handle) = self.flush_handle.lock().unwrap().take()
            && let Err(e) = self.rt.block_on(handle)
        {
            error!("Flush task panicked: {}", e);
        }
        info!("Flush loop finished, VFS shut down.");
    }

    // ── Background tasks ───────────────────────────────────────────────

    /// Background task: accumulates flush requests with debounce,
    /// then uploads all files in a single session + single batch call.
    async fn flush_loop(
        mut rx: mpsc::UnboundedReceiver<FlushRequest>,
        cache: Arc<FileCache>,
        hub_client: Arc<HubApiClient>,
        bucket_id: String,
        inodes: Arc<Mutex<InodeTable>>,
        flush_errors: Arc<Mutex<HashMap<u64, String>>>,
    ) {
        loop {
            // Wait for the first request
            let first = match rx.recv().await {
                Some(req) => req,
                None => return, // channel closed, exit
            };

            let mut pending = vec![first];

            // Debounce: keep collecting for DEBOUNCE_DURATION after each new item,
            // but cap total wait at MAX_BATCH_WINDOW to avoid unbounded delay.
            let window_deadline = tokio::time::Instant::now() + MAX_BATCH_WINDOW;
            loop {
                let remaining = window_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let timeout = DEBOUNCE_DURATION.min(remaining);
                match tokio::time::timeout(timeout, rx.recv()).await {
                    Ok(Some(req)) => pending.push(req),
                    _ => break, // timeout (debounce expired) or channel closed
                }
            }

            let count = pending.len();
            info!("Flushing batch of {} dirty file(s)", count);

            Self::flush_batch(pending, &cache, &hub_client, &bucket_id, &inodes, &flush_errors).await;
        }
    }

    /// Upload a batch of files in a single session, then commit via a single /batch call.
    /// Resolves paths from the inode table at flush time (handles renames/unlinks).
    async fn flush_batch(
        pending: Vec<FlushRequest>,
        cache: &FileCache,
        hub_client: &HubApiClient,
        bucket_id: &str,
        inodes: &Mutex<InodeTable>,
        flush_errors: &Mutex<HashMap<u64, String>>,
    ) {
        // Dedup by inode (keep last request per ino)
        let mut seen = std::collections::HashSet::new();
        let deduped: Vec<u64> = pending
            .into_iter()
            .rev()
            .filter(|r| seen.insert(r.ino))
            .map(|r| r.ino)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        // Resolve paths from inode table, skip deleted/non-dirty inodes
        let to_flush: Vec<(u64, String, PathBuf, Vec<String>)> = {
            let inode_table = inodes.lock().unwrap();
            deduped
                .into_iter()
                .filter_map(|ino| {
                    let entry = inode_table.get(ino)?;
                    if !entry.dirty {
                        return None;
                    }
                    let staging_path = cache.staging_path(ino);
                    if !staging_path.exists() {
                        error!("Staging file missing for ino={}, skipping", ino);
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
        let upload_results = match cache.upload_files(&staging_paths).await {
            Ok(results) => results,
            Err(e) => {
                error!("Batch upload failed: {}", e);
                let msg = format!("upload failed: {e}");
                let mut errs = flush_errors.lock().unwrap();
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

        // Single batch commit
        if let Err(e) = hub_client.batch_operations(bucket_id, &ops).await {
            error!("Batch commit failed: {}", e);
            let msg = format!("commit failed: {e}");
            let mut errs = flush_errors.lock().unwrap();
            for (ino, _, _, _) in &to_flush {
                errs.insert(*ino, msg.clone());
            }
            return;
        }

        // Update inodes
        let mut inode_table = inodes.lock().unwrap();
        let now = SystemTime::now();
        for (ino, xet_hash, size) in successes {
            if let Some(entry) = inode_table.get_mut(ino) {
                entry.xet_hash = Some(xet_hash);
                entry.size = size;
                entry.dirty = false;
                entry.mtime = now;
                entry.pending_deletes.clear();
            }
        }

        info!("Batch flush completed: {} file(s) committed", to_flush.len());
    }

    /// Background task: polls Hub API tree listing to detect remote changes.
    async fn poll_remote_changes(
        hub_client: Arc<HubApiClient>,
        bucket_id: String,
        inodes: Arc<Mutex<InodeTable>>,
        cache: Arc<FileCache>,
        neg_cache: Arc<Mutex<HashMap<String, Instant>>>,
        interval: Duration,
    ) {
        loop {
            tokio::time::sleep(interval).await;

            let remote_entries = match hub_client.list_tree(&bucket_id, "").await {
                Ok(entries) => entries,
                Err(e) => {
                    warn!("Remote poll failed: {}", e);
                    continue;
                }
            };

            let remote_map: HashMap<String, _> = remote_entries
                .iter()
                .filter(|e| e.entry_type == "file")
                .map(|e| (e.path.clone(), e))
                .collect();

            // Take snapshot under lock, then release to avoid blocking VFS ops
            let snapshot = inodes.lock().unwrap().file_snapshot();

            // Phase 1: Compute diff (no lock held)
            struct Update {
                ino: u64,
                hash: Option<String>,
                size: u64,
                mtime: SystemTime,
            }
            let mut updates = Vec::new();
            let mut deletions = Vec::new();

            for (ino, path, local_hash, local_size, is_dirty) in &snapshot {
                if *is_dirty {
                    continue;
                }
                match remote_map.get(path.as_str()) {
                    Some(remote) => {
                        let remote_hash = remote.xet_hash.as_deref();
                        let remote_size = remote.size.unwrap_or(0);
                        let hash_changed = remote_hash != local_hash.as_deref();
                        let size_changed = remote_size != *local_size;

                        if hash_changed || size_changed {
                            let mtime = remote
                                .mtime
                                .as_deref()
                                .map(HubApiClient::mtime_from_str)
                                .unwrap_or(SystemTime::now());
                            updates.push(Update {
                                ino: *ino,
                                hash: remote_hash.map(|s| s.to_string()),
                                size: remote_size,
                                mtime,
                            });
                            info!("Remote update detected: {}", path);
                        }
                    }
                    None => {
                        info!("Remote deletion detected: {}", path);
                        deletions.push(*ino);
                    }
                }
            }

            // Phase 2: Apply mutations under lock
            {
                let mut inode_table = inodes.lock().unwrap();

                for upd in updates {
                    inode_table.update_remote_file(upd.ino, upd.hash, upd.size, upd.mtime);
                }

                for ino in &deletions {
                    inode_table.remove(*ino);
                }

                // Phase 3: New remote files → invalidate parent dir + negative cache
                // Walk up the path to find the nearest existing ancestor directory
                // and invalidate it so the next readdir/lookup discovers the new entries.
                let mut dirs_to_invalidate = HashSet::new();
                let mut dir_paths_to_invalidate = Vec::new();
                for path in remote_map.keys() {
                    if inode_table.get_by_path(path).is_none() {
                        // Walk up to find the nearest existing ancestor directory
                        let mut ancestor = path.as_str();
                        loop {
                            ancestor = match ancestor.rsplit_once('/') {
                                Some((parent, _)) => parent,
                                None => "",
                            };
                            if let Some(dir_ino) = inode_table.get_dir_ino(ancestor) {
                                if dirs_to_invalidate.insert(dir_ino) {
                                    dir_paths_to_invalidate.push(ancestor.to_string());
                                }
                                break;
                            }
                            if ancestor.is_empty() {
                                // Root always exists; invalidate it
                                if dirs_to_invalidate.insert(crate::inode::ROOT_INODE) {
                                    dir_paths_to_invalidate.push(String::new());
                                }
                                break;
                            }
                        }
                    }
                }

                for dir_ino in dirs_to_invalidate {
                    inode_table.invalidate_children(dir_ino);
                }

                // Invalidate negative cache entries under changed directories
                if !dir_paths_to_invalidate.is_empty() {
                    let mut nc = neg_cache.lock().unwrap();
                    for dir_path in &dir_paths_to_invalidate {
                        let prefix = if dir_path.is_empty() {
                            String::new()
                        } else {
                            format!("{}/", dir_path)
                        };
                        nc.retain(|k, _| {
                            if dir_path.is_empty() {
                                false
                            } else {
                                !k.starts_with(&prefix) && k != dir_path
                            }
                        });
                    }
                }
            }

            // Clean up staging files for deleted inodes (outside lock scope)
            for ino in deletions {
                let staging_path = cache.staging_path(ino);
                if staging_path.exists() {
                    std::fs::remove_file(&staging_path).ok();
                }
            }
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn make_vfs_attr(&self, entry: &InodeEntry) -> VfsAttr {
        let perm = if self.read_only {
            match entry.kind {
                InodeKind::File => 0o444,
                InodeKind::Directory => 0o555,
            }
        } else {
            match entry.kind {
                InodeKind::File => 0o644,
                InodeKind::Directory => 0o755,
            }
        };
        let nlink = match entry.kind {
            InodeKind::File => 1,
            InodeKind::Directory => 2,
        };

        VfsAttr {
            ino: entry.inode,
            size: entry.size,
            blocks: entry.size.div_ceil(BLOCK_SIZE as u64),
            mtime: entry.mtime,
            kind: entry.kind,
            perm,
            nlink,
            uid: self.uid,
            gid: self.gid,
        }
    }

    /// Ensure children of a directory inode are loaded from the Hub API.
    fn ensure_children_loaded(&self, parent_ino: u64) {
        let prefix = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.get(parent_ino) {
                Some(e) if !e.children_loaded && e.kind == InodeKind::Directory => e.full_path.clone(),
                _ => return,
            }
        };

        let hub = self.hub_client.clone();
        let bucket_id = self.bucket_id.clone();

        let entries = match self.rt.block_on(hub.list_tree(&bucket_id, &prefix)) {
            Ok(entries) => entries,
            Err(e) => {
                error!("Failed to list tree for prefix '{}': {}", prefix, e);
                return;
            }
        };

        let mut inodes = self.inodes.lock().unwrap();

        let mut seen_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();

        for entry in entries {
            let rel_path = if prefix.is_empty() {
                entry.path.as_str()
            } else {
                entry
                    .path
                    .strip_prefix(&prefix)
                    .and_then(|p| p.strip_prefix('/'))
                    .unwrap_or(&entry.path)
            };

            if let Some(slash_pos) = rel_path.find('/') {
                let dir_name = &rel_path[..slash_pos];
                if seen_dirs.insert(dir_name.to_string()) {
                    let dir_full_path = if prefix.is_empty() {
                        dir_name.to_string()
                    } else {
                        format!("{}/{}", prefix, dir_name)
                    };
                    inodes.insert(
                        parent_ino,
                        dir_name.to_string(),
                        dir_full_path,
                        InodeKind::Directory,
                        0,
                        UNIX_EPOCH,
                        None,
                    );
                }
            } else {
                let kind = if entry.entry_type == "directory" {
                    InodeKind::Directory
                } else {
                    InodeKind::File
                };
                let size = entry.size.unwrap_or(0);
                let mtime = entry
                    .mtime
                    .as_deref()
                    .map(HubApiClient::mtime_from_str)
                    .unwrap_or(UNIX_EPOCH);

                inodes.insert(
                    parent_ino,
                    rel_path.to_string(),
                    entry.path,
                    kind,
                    size,
                    mtime,
                    entry.xet_hash,
                );
            }
        }

        if let Some(parent) = inodes.get_mut(parent_ino) {
            parent.children_loaded = true;
        }
    }

    pub fn alloc_fh(&self) -> u64 {
        let mut fh = self.next_fh.lock().unwrap();
        let val = *fh;
        *fh += 1;
        val
    }

    /// Open a local file as read-only and return the file handle.
    fn open_local_readonly(&self, path: &PathBuf) -> VfsResult<u64> {
        match File::open(path) {
            Ok(file) => {
                let fh = self.alloc_fh();
                self.open_files.lock().unwrap().insert(
                    fh,
                    OpenFile::Local {
                        file: Arc::new(file),
                        writable: false,
                    },
                );
                Ok(fh)
            }
            Err(e) => {
                error!("Failed to open file {:?}: {}", path, e);
                Err(libc::EIO)
            }
        }
    }

    /// Check if a path is in the negative cache (and not expired).
    fn neg_cache_check(&self, path: &str) -> bool {
        let cache = self.neg_cache.lock().unwrap();
        matches!(cache.get(path), Some(inserted) if inserted.elapsed() < NEG_CACHE_TTL)
    }

    /// Insert a path into the negative cache, evicting if at capacity.
    fn neg_cache_insert(&self, path: String) {
        let mut cache = self.neg_cache.lock().unwrap();
        let now = Instant::now();
        if cache.len() >= NEG_CACHE_CAPACITY {
            // Evict expired entries first
            cache.retain(|_, ts| ts.elapsed() < NEG_CACHE_TTL);
            // If still full, evict the oldest entry
            if cache.len() >= NEG_CACHE_CAPACITY
                && let Some(oldest_key) = cache.iter().min_by_key(|(_, ts)| **ts).map(|(k, _)| k.clone())
            {
                cache.remove(&oldest_key);
            }
        }
        cache.insert(path, now);
    }

    /// Enqueue a dirty file for debounced batch flush.
    fn enqueue_flush(&self, ino: u64) {
        if let Some(tx) = self.flush_tx.lock().unwrap().as_ref()
            && tx.send(FlushRequest { ino }).is_err()
        {
            error!("Flush channel closed, cannot enqueue ino={}", ino);
        }
    }

    // ── VFS operations ─────────────────────────────────────────────────

    pub fn lookup(&self, parent: u64, name: &str) -> VfsResult<VfsAttr> {
        debug!("lookup: parent={}, name={}", parent, name);

        // Build full path and check negative cache under a single inode lock
        let (full_path, needs_load) = {
            let inodes = self.inodes.lock().unwrap();
            let parent_entry = match inodes.get(parent) {
                Some(e) => e,
                None => return Err(libc::ENOENT),
            };
            let fp = if parent_entry.full_path.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", parent_entry.full_path, name)
            };
            let needs_load = !parent_entry.children_loaded && parent_entry.kind == InodeKind::Directory;
            (fp, needs_load)
        };

        // Check negative cache
        if self.neg_cache_check(&full_path) {
            debug!("negative cache hit: {}", full_path);
            return Err(libc::ENOENT);
        }

        if needs_load {
            self.ensure_children_loaded(parent);
        }

        let inodes = self.inodes.lock().unwrap();
        match inodes.lookup_child(parent, name) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => {
                drop(inodes);
                self.neg_cache_insert(full_path);
                Err(libc::ENOENT)
            }
        }
    }

    pub fn getattr(&self, ino: u64) -> VfsResult<VfsAttr> {
        debug!("getattr: ino={}", ino);

        let inodes = self.inodes.lock().unwrap();
        match inodes.get(ino) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => Err(libc::ENOENT),
        }
    }

    pub fn readdir(&self, ino: u64) -> VfsResult<Vec<VfsDirEntry>> {
        debug!("readdir: ino={}", ino);

        self.ensure_children_loaded(ino);

        let inodes = self.inodes.lock().unwrap();
        let entry = match inodes.get(ino) {
            Some(e) => e,
            None => return Err(libc::ENOENT),
        };

        let mut entries = Vec::new();
        entries.push(VfsDirEntry {
            ino,
            kind: InodeKind::Directory,
            name: ".".to_string(),
        });
        entries.push(VfsDirEntry {
            ino: entry.parent,
            kind: InodeKind::Directory,
            name: "..".to_string(),
        });

        for &child_ino in &entry.children {
            if let Some(child) = inodes.get(child_ino) {
                entries.push(VfsDirEntry {
                    ino: child.inode,
                    kind: child.kind,
                    name: child.name.clone(),
                });
            }
        }

        Ok(entries)
    }

    pub fn open(&self, ino: u64, writable: bool, truncate: bool) -> VfsResult<(u64, bool)> {
        debug!("open: ino={}, writable={}, truncate={}", ino, writable, truncate);

        if writable && self.read_only {
            return Err(libc::EROFS);
        }

        let (full_path, xet_hash, size, is_dirty) = {
            let inodes = self.inodes.lock().unwrap();
            let entry = match inodes.get(ino) {
                Some(e) if e.kind == InodeKind::File => e,
                _ => return Err(libc::ENOENT),
            };

            (
                entry.full_path.clone(),
                entry.xet_hash.clone().unwrap_or_default(),
                entry.size,
                entry.dirty,
            )
        };

        if writable {
            let staging_path = self.cache.staging_path(ino);

            if is_dirty && staging_path.exists() {
                // Reuse existing staging file (preserves pending local edits)
            } else if truncate {
                if let Err(e) = File::create(&staging_path) {
                    error!("Failed to create staging file: {}", e);
                    return Err(libc::EIO);
                }
            } else if !xet_hash.is_empty() && size > 0 {
                let cache = self.cache.clone();
                if let Err(e) = self.rt.block_on(cache.download_to_file(&xet_hash, size, &staging_path)) {
                    error!("Failed to download file for write: {}", e);
                    return Err(libc::EIO);
                }
            } else if let Err(e) = File::create(&staging_path) {
                error!("Failed to create staging file: {}", e);
                return Err(libc::EIO);
            }

            match OpenOptions::new().read(true).write(true).open(&staging_path) {
                Ok(file) => {
                    let fh = self.alloc_fh();
                    {
                        let mut inodes = self.inodes.lock().unwrap();
                        if let Some(entry) = inodes.get_mut(ino) {
                            entry.dirty = true;
                            if truncate {
                                entry.size = 0;
                            }
                        }
                    }
                    self.open_files.lock().unwrap().insert(
                        fh,
                        OpenFile::Local {
                            file: Arc::new(file),
                            writable: true,
                        },
                    );
                    Ok((fh, false))
                }
                Err(e) => {
                    error!("Failed to open staging file: {}", e);
                    Err(libc::EIO)
                }
            }
        } else {
            // Read-only open: check dirty staging first, then use lazy range reads
            let staging_path = self.cache.staging_path(ino);

            if is_dirty && staging_path.exists() {
                // Dirty file: read from staging area (handles files not yet flushed)
                let fh = self.open_local_readonly(&staging_path)?;
                Ok((fh, false))
            } else if xet_hash.is_empty() {
                if size == 0 {
                    // Empty file with no hash: create a temp empty file to open
                    let empty_path = self.cache.staging_path(ino);
                    if !empty_path.exists() {
                        File::create(&empty_path).ok();
                    }
                    let fh = self.open_local_readonly(&empty_path)?;
                    Ok((fh, false))
                } else {
                    error!("No xet hash for non-empty, non-dirty file {}", full_path);
                    Err(libc::EIO)
                }
            } else {
                // Lazy remote: create prefetch buffer, fetch on read()
                let prefetch = Arc::new(Mutex::new(PrefetchState::new(xet_hash, size)));
                let fh = self.alloc_fh();
                self.open_files.lock().unwrap().insert(fh, OpenFile::Lazy { prefetch });
                // Bypass kernel page cache so all reads hit our prefetch buffer
                Ok((fh, true))
            }
        }
    }

    pub fn read(&self, fh: u64, offset: u64, size: u32) -> VfsResult<Vec<u8>> {
        debug!("read: fh={}, offset={}, size={}", fh, offset, size);

        // Extract what we need under the lock, then release it.
        // Clone Arc<File> so the FD stays alive even if release() runs concurrently.
        let read_target = {
            let files = self.open_files.lock().unwrap();
            match files.get(&fh) {
                Some(OpenFile::Local { file, .. }) => ReadTarget::LocalFd(file.clone()),
                Some(OpenFile::Lazy { prefetch }) => ReadTarget::Remote {
                    prefetch: prefetch.clone(),
                },
                None => return Err(libc::EBADF),
            }
        };

        match read_target {
            ReadTarget::LocalFd(file) => {
                let fd = file.as_raw_fd();
                let mut buf = vec![0u8; size as usize];
                let n = unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut libc::c_void, size as usize, offset as i64) };
                if n < 0 {
                    Err(libc::EIO)
                } else {
                    buf.truncate(n as usize);
                    Ok(buf)
                }
            }
            ReadTarget::Remote { prefetch } => {
                let mut ps = prefetch.lock().unwrap();

                // Past EOF
                if offset >= ps.file_size {
                    return Ok(vec![]);
                }

                // Try forward buffer
                if let Some(data) = ps.try_serve_forward(offset, size) {
                    debug!("prefetch hit (forward): offset={}, len={}", offset, data.len());
                    return Ok(data.to_vec());
                }

                // Try seek window
                if let Some(data) = ps.try_serve_seek(offset, size) {
                    debug!("prefetch hit (seek): offset={}, len={}", offset, data.len());
                    return Ok(data.to_vec());
                }

                // Cache miss — compute window adjustment before draining
                let old_buf_end = ps.buf_start + ps.data.len() as u64;
                let is_first_fetch = ps.data.is_empty() && ps.buf_start == 0;

                // Drain consumed forward bytes to seek window
                if !ps.data.is_empty() {
                    let consumed = if offset >= ps.buf_start { ps.data.len() } else { 0 };
                    ps.drain_to_seek(consumed);
                }

                // Classify the access pattern and adjust window
                let is_sequential;
                if is_first_fetch {
                    // First fetch: use initial window as-is
                    is_sequential = true;
                } else if offset >= old_buf_end && offset <= old_buf_end + FORWARD_SKIP {
                    // Sequential or small forward skip: double window (TCP slow-start)
                    ps.window_size = (ps.window_size * 2).min(MAX_WINDOW);
                    debug!("prefetch window doubled to {}", ps.window_size);
                    is_sequential = true;
                } else {
                    // Far seek: reset to initial window, cancel stream
                    ps.window_size = INITIAL_WINDOW;
                    debug!("prefetch window reset to {}", ps.window_size);
                    is_sequential = false;
                    if let Some(s) = ps.stream.take() {
                        debug!("prefetch: cancelling stream (far seek)");
                        drop(s);
                    }
                }

                // Fetch: download max(needed, window_size) bytes
                let needed = (size as u64).min(ps.file_size - offset);
                let fetch_size = needed.max(ps.window_size).min(ps.file_size - offset);

                // Try streaming for pure sequential reads (offset must match
                // stream position exactly — no gaps or skips).
                let streamed = if is_sequential && offset == ps.buf_start {
                    if ps.stream.is_none() && is_first_fetch && offset == 0 {
                        let fi = XetFileInfo::new(ps.xet_hash.clone(), ps.file_size);
                        match self.cache.download_session().download_stream(&fi, None) {
                            Ok(stream) => {
                                debug!("prefetch: started full-file stream");
                                ps.stream = Some(stream);
                            }
                            Err(e) => {
                                warn!("prefetch: stream start failed: {}", e);
                            }
                        }
                    }
                    if let Some(mut stream) = ps.stream.take() {
                        // Enter tokio runtime context so blocking_next() can
                        // spawn tasks on FUSE threads (which lack a runtime).
                        let _guard = self.rt.enter();
                        let mut buf = Vec::with_capacity(fetch_size as usize);
                        let mut stream_eof = false;
                        while (buf.len() as u64) < fetch_size {
                            match stream.blocking_next() {
                                Ok(Some(chunk)) => {
                                    buf.extend_from_slice(&chunk);
                                }
                                Ok(None) => {
                                    stream_eof = true;
                                    break;
                                }
                                Err(e) => {
                                    error!("prefetch: stream error: {}", e);
                                    break;
                                }
                            }
                        }
                        if !stream_eof {
                            // Put stream back for next read
                            ps.stream = Some(stream);
                        }
                        if !buf.is_empty() { Some(buf) } else { None }
                    } else {
                        None
                    }
                } else {
                    // Non-sequential: cancel stale stream
                    if let Some(s) = ps.stream.take() {
                        drop(s);
                    }
                    None
                };

                // Fall back to range download if streaming wasn't used
                let fetched = if let Some(buf) = streamed {
                    debug!(
                        "prefetch stream: offset={}, got={}, window={}",
                        offset,
                        buf.len(),
                        ps.window_size
                    );
                    buf
                } else {
                    let fetch_end = offset + fetch_size;
                    let file_info = XetFileInfo::new(ps.xet_hash.clone(), ps.file_size);
                    let buf = Arc::new(Mutex::new(Vec::with_capacity(fetch_size as usize)));
                    let writer = SharedBufWriter(buf.clone());

                    debug!(
                        "prefetch fetch: offset={}, size={}, window={}",
                        offset, fetch_size, ps.window_size
                    );

                    match self.rt.block_on(self.cache.download_session().download_to_writer(
                        &file_info,
                        offset..fetch_end,
                        writer,
                        None,
                    )) {
                        Ok(_) => match Arc::try_unwrap(buf) {
                            Ok(mutex) => mutex.into_inner().unwrap_or_default(),
                            Err(arc) => arc.lock().unwrap_or_else(|e| e.into_inner()).clone(),
                        },
                        Err(e) => {
                            error!("Prefetch range read failed: {}", e);
                            return Err(libc::EIO);
                        }
                    }
                };

                let to_read = (size as usize).min(fetched.len());
                if to_read == fetched.len() {
                    // App wants everything — return the buffer directly, no copy
                    ps.data = Vec::new();
                    ps.buf_start = offset;
                    Ok(fetched)
                } else {
                    // App wants a prefix — copy the slice, keep rest in buffer
                    let result = fetched[..to_read].to_vec();
                    ps.data = fetched;
                    ps.buf_start = offset;
                    Ok(result)
                }
            }
        }
    }

    pub fn write(&self, ino: u64, fh: u64, offset: u64, data: &[u8]) -> VfsResult<u32> {
        debug!("write: ino={}, fh={}, offset={}, len={}", ino, fh, offset, data.len());

        if self.read_only {
            return Err(libc::EROFS);
        }

        // Clone Arc<File> so the FD stays alive even if release() runs concurrently.
        let file = {
            let files = self.open_files.lock().unwrap();
            match files.get(&fh) {
                Some(OpenFile::Local { file, writable: true }) => file.clone(),
                Some(OpenFile::Local { writable: false, .. }) | Some(OpenFile::Lazy { .. }) => {
                    return Err(libc::EBADF);
                }
                None => return Err(libc::EBADF),
            }
        };
        let fd = file.as_raw_fd();

        let n = unsafe { libc::pwrite(fd, data.as_ptr() as *const libc::c_void, data.len(), offset as i64) };

        if n < 0 {
            Err(libc::EIO)
        } else {
            let written = n as u32;
            let new_end = offset + written as u64;
            let mut inodes = self.inodes.lock().unwrap();
            if let Some(entry) = inodes.get_mut(ino)
                && new_end > entry.size
            {
                entry.size = new_end;
            }
            Ok(written)
        }
    }

    pub fn flush(&self, ino: u64) -> VfsResult<()> {
        debug!("flush: ino={}", ino);
        // Check if a previous async flush failed for this inode
        if let Some(err_msg) = self.flush_errors.lock().unwrap().remove(&ino) {
            error!("Deferred flush error for ino={}: {}", ino, err_msg);
            return Err(libc::EIO);
        }
        Ok(())
    }

    pub fn release(&self, ino: u64, fh: u64) {
        debug!("release: ino={}, fh={}", ino, fh);

        let was_writable = {
            let files = self.open_files.lock().unwrap();
            matches!(files.get(&fh), Some(OpenFile::Local { writable: true, .. }))
        };

        if was_writable && !self.read_only {
            self.enqueue_flush(ino);
        }

        self.open_files.lock().unwrap().remove(&fh);
    }

    pub fn create(&self, parent: u64, name: &str) -> VfsResult<(VfsAttr, u64)> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("create: parent={}, name={}", parent, name);

        let parent_path = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.get(parent) {
                Some(e) if e.kind == InodeKind::Directory => e.full_path.clone(),
                _ => return Err(libc::ENOENT),
            }
        };

        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        // Remove from negative cache since this path is being created
        self.neg_cache.lock().unwrap().remove(&full_path);

        let now = SystemTime::now();
        let ino = {
            let mut inodes = self.inodes.lock().unwrap();
            let ino = inodes.insert(parent, name.to_string(), full_path, InodeKind::File, 0, now, None);
            if let Some(entry) = inodes.get_mut(ino) {
                entry.dirty = true;
            }
            ino
        };

        let staging_path = self.cache.staging_path(ino);
        match OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&staging_path)
        {
            Ok(file) => {
                let fh = self.alloc_fh();
                self.open_files.lock().unwrap().insert(
                    fh,
                    OpenFile::Local {
                        file: Arc::new(file),
                        writable: true,
                    },
                );

                let inodes = self.inodes.lock().unwrap();
                let attr = self.make_vfs_attr(inodes.get(ino).unwrap());
                Ok((attr, fh))
            }
            Err(e) => {
                error!("Failed to create staging file: {}", e);
                // Rollback: remove the inode we just inserted
                self.inodes.lock().unwrap().remove(ino);
                Err(libc::EIO)
            }
        }
    }

    pub fn mkdir(&self, parent: u64, name: &str) -> VfsResult<VfsAttr> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("mkdir: parent={}, name={}", parent, name);

        self.ensure_children_loaded(parent);

        let parent_path = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.get(parent) {
                Some(e) if e.kind == InodeKind::Directory => e.full_path.clone(),
                _ => return Err(libc::ENOENT),
            }
        };

        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        // Remove from negative cache since this path is being created
        self.neg_cache.lock().unwrap().remove(&full_path);

        let mut inodes = self.inodes.lock().unwrap();

        if inodes.lookup_child(parent, name).is_some() {
            return Err(libc::EEXIST);
        }

        let now = SystemTime::now();
        let ino = inodes.insert(parent, name.to_string(), full_path, InodeKind::Directory, 0, now, None);

        if let Some(entry) = inodes.get_mut(ino) {
            entry.children_loaded = true;
        }

        Ok(self.make_vfs_attr(inodes.get(ino).unwrap()))
    }

    pub fn unlink(&self, parent: u64, name: &str) -> VfsResult<()> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("unlink: parent={}, name={}", parent, name);

        self.ensure_children_loaded(parent);

        let (ino, full_path, was_dirty) = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.lookup_child(parent, name) {
                Some(entry) if entry.kind == InodeKind::File => (entry.inode, entry.full_path.clone(), entry.dirty),
                Some(_) => return Err(libc::EISDIR),
                None => return Err(libc::ENOENT),
            }
        };

        let needs_remote_delete = !was_dirty || {
            let inodes = self.inodes.lock().unwrap();
            inodes.get(ino).and_then(|e| e.xet_hash.as_ref()).is_some()
        };

        if needs_remote_delete {
            let hub = self.hub_client.clone();
            let bucket_id = self.bucket_id.clone();
            let path = full_path.clone();

            if let Err(e) = self
                .rt
                .block_on(async { hub.batch_operations(&bucket_id, &[BatchOp::DeleteFile { path }]).await })
            {
                error!("Failed to delete file {}: {}", full_path, e);
                return Err(libc::EIO);
            }
        }

        let staging_path = self.cache.staging_path(ino);
        std::fs::remove_file(&staging_path).ok();

        self.inodes.lock().unwrap().remove(ino);

        info!("Deleted file: {}", full_path);
        Ok(())
    }

    pub fn rmdir(&self, parent: u64, name: &str) -> VfsResult<()> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("rmdir: parent={}, name={}", parent, name);

        self.ensure_children_loaded(parent);

        let ino = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.lookup_child(parent, name) {
                Some(entry) if entry.kind == InodeKind::Directory => {
                    if !entry.children.is_empty() {
                        return Err(libc::ENOTEMPTY);
                    }
                    entry.inode
                }
                Some(_) => return Err(libc::ENOTDIR),
                None => return Err(libc::ENOENT),
            }
        };

        self.ensure_children_loaded(ino);

        {
            let inodes = self.inodes.lock().unwrap();
            if let Some(entry) = inodes.get(ino)
                && !entry.children.is_empty()
            {
                return Err(libc::ENOTEMPTY);
            }
        }

        self.inodes.lock().unwrap().remove(ino);
        Ok(())
    }

    pub fn rename(&self, parent: u64, name: &str, newparent: u64, newname: &str, no_replace: bool) -> VfsResult<()> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!(
            "rename: parent={}, name={}, newparent={}, newname={}",
            parent, name, newparent, newname
        );

        self.ensure_children_loaded(parent);
        if parent != newparent {
            self.ensure_children_loaded(newparent);
        }

        let (ino, old_path, kind, xet_hash, is_dirty) = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.lookup_child(parent, name) {
                Some(entry) => (
                    entry.inode,
                    entry.full_path.clone(),
                    entry.kind,
                    entry.xet_hash.clone(),
                    entry.dirty,
                ),
                None => return Err(libc::ENOENT),
            }
        };

        let new_parent_path = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.get(newparent) {
                Some(e) => e.full_path.clone(),
                None => return Err(libc::ENOENT),
            }
        };

        let new_full_path = if new_parent_path.is_empty() {
            newname.to_string()
        } else {
            format!("{}/{}", new_parent_path, newname)
        };

        if kind == InodeKind::File && !is_dirty {
            if let Some(hash) = xet_hash {
                let hub = self.hub_client.clone();
                let bucket_id = self.bucket_id.clone();

                let mtime_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let ops = vec![
                    BatchOp::AddFile {
                        path: new_full_path.clone(),
                        xet_hash: hash,
                        mtime: mtime_ms,
                        content_type: None,
                    },
                    BatchOp::DeleteFile { path: old_path.clone() },
                ];

                if let Err(e) = self.rt.block_on(hub.batch_operations(&bucket_id, &ops)) {
                    error!("Failed to rename {} -> {}: {}", old_path, new_full_path, e);
                    return Err(libc::EIO);
                }
            }
        } else if is_dirty && kind == InodeKind::File && xet_hash.is_some() {
            // Dirty file with a remote presence: record old path for deletion at flush time
            let mut inodes = self.inodes.lock().unwrap();
            if let Some(entry) = inodes.get_mut(ino) {
                entry.pending_deletes.push(old_path.clone());
            }
        }

        {
            let mut inodes = self.inodes.lock().unwrap();

            // Remove existing destination inode if it exists (POSIX rename replaces target)
            if let Some(existing) = inodes.lookup_child(newparent, newname) {
                // RENAME_NOREPLACE: refuse if the target already exists
                if no_replace {
                    return Err(libc::EEXIST);
                }
                let existing_ino = existing.inode;
                let existing_kind = existing.kind;
                // Don't allow replacing a non-empty directory
                if existing_kind == InodeKind::Directory && !existing.children.is_empty() {
                    return Err(libc::ENOTEMPTY);
                }
                inodes.remove(existing_ino);
            }

            if let Some(old_parent) = inodes.get_mut(parent) {
                old_parent.children.retain(|&c| c != ino);
            }

            // Update this inode's name/parent, then recursively fix all descendant paths
            if let Some(entry) = inodes.get_mut(ino) {
                entry.name = newname.to_string();
                entry.parent = newparent;
            }
            inodes.update_subtree_paths(ino, new_full_path.clone());

            if let Some(new_parent) = inodes.get_mut(newparent) {
                new_parent.children.push(ino);
            }
        }

        Ok(())
    }

    pub fn setattr(&self, ino: u64, size: Option<u64>) -> VfsResult<VfsAttr> {
        debug!("setattr: ino={}, size={:?}", ino, size);

        if self.read_only {
            return Err(libc::EROFS);
        }

        if let Some(new_size) = size {
            let staging_path = self.cache.staging_path(ino);

            if new_size == 0 {
                if let Err(e) = File::create(&staging_path) {
                    error!("Failed to truncate staging file: {}", e);
                    return Err(libc::EIO);
                }
            } else {
                // If staging file doesn't exist, download from remote first
                if !staging_path.exists() {
                    let (xet_hash, file_size) = {
                        let inodes = self.inodes.lock().unwrap();
                        match inodes.get(ino) {
                            Some(e) => (e.xet_hash.clone().unwrap_or_default(), e.size),
                            None => return Err(libc::ENOENT),
                        }
                    };
                    if !xet_hash.is_empty() && file_size > 0 {
                        let cache = self.cache.clone();
                        if let Err(e) = self
                            .rt
                            .block_on(cache.download_to_file(&xet_hash, file_size, &staging_path))
                        {
                            error!("Failed to download file for truncate: {}", e);
                            return Err(libc::EIO);
                        }
                    } else if let Err(e) = File::create(&staging_path) {
                        error!("Failed to create staging file for truncate: {}", e);
                        return Err(libc::EIO);
                    }
                }
                if let Err(e) = OpenOptions::new()
                    .write(true)
                    .open(&staging_path)
                    .and_then(|f| f.set_len(new_size))
                {
                    error!("Failed to set staging file length: {}", e);
                    return Err(libc::EIO);
                }
            }

            let mut inodes = self.inodes.lock().unwrap();
            if let Some(entry) = inodes.get_mut(ino) {
                entry.size = new_size;
                entry.dirty = true;
            }

            // Schedule flush so the truncation is committed to CAS/bucket
            drop(inodes);
            self.enqueue_flush(ino);
        }

        let inodes = self.inodes.lock().unwrap();
        match inodes.get(ino) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => Err(libc::ENOENT),
        }
    }
}
