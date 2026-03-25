use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use tracing::{debug, error, info, warn};
use xet_data::processing::XetFileInfo;

use crate::hub_api::{BatchOp, HubOps};

mod flush;
pub mod inode;
mod poll;
mod prefetch;
use crate::xet::{StagingDir, StreamingWriterOps, XetOps};
use inode::{InodeEntry, InodeKind, InodeTable};
use prefetch::{FetchPlan, PrefetchState};

// ── Constants ──────────────────────────────────────────────────────────

/// Block size reported in stat(2) for `st_blocks` calculation.
const BLOCK_SIZE: u32 = 512;
/// Maximum entries in the negative-lookup cache (parent_path/name → Instant).
/// Prevents repeated Hub API calls for paths known to not exist.
const NEG_CACHE_CAPACITY: usize = 10_000;
/// How long a negative-cache entry stays valid before being re-checked.
const NEG_CACHE_TTL: Duration = Duration::from_secs(30);

type Invalidator = Arc<Mutex<Option<Box<dyn Fn(u64) + Send + Sync>>>>;
type CommitHookTx = tokio::sync::watch::Sender<Option<Result<(), i32>>>;
type CommitHookRx = tokio::sync::watch::Receiver<Option<Result<(), i32>>>;

/// Returns `true` for OS-generated junk files that should not be synced to remote storage.
fn is_os_junk(name: &str) -> bool {
    matches!(
        name,
        ".DS_Store" | ".Spotlight-V100" | ".Trashes" | ".fseventsd" | "__MACOSX" | "Thumbs.db" | "desktop.ini"
    ) || name.starts_with("._")
}

// ── VirtualFs ──────────────────────────────────────────────────────────

/// Configuration for [`VirtualFs`], grouping all tunable options.
pub struct VfsConfig {
    pub read_only: bool,
    pub advanced_writes: bool,
    pub uid: u32,
    pub gid: u32,
    pub poll_interval_secs: u64,
    pub metadata_ttl: Duration,
    pub serve_lookup_from_cache: bool,
    pub filter_os_files: bool,
    pub direct_io: bool,
    pub flush_debounce: Duration,
    pub flush_max_batch_window: Duration,
}

/// Lock ordering (acquire in this order to prevent deadlocks):
///
///   dir_loading_locks[ino]      (tokio::sync::Mutex, per-directory)
///     → inode_table             (RwLock, read or write)
///
///   staging_locks[ino]          (tokio::sync::Mutex, per-inode)
///     → inode_table             (RwLock, read or write)
///         → open_files          (RwLock, read only — via has_open_handles)
///         → negative_cache      (RwLock, write — in poll_remote_changes)
///
///   StreamingChannel.commit_hook (Mutex)
///     → pending_commits          (Mutex)
///
/// General discipline: locks are held briefly and never across await points
/// (except tokio::sync::Mutex in staging_locks). Most paths acquire a lock,
/// extract data, drop the lock, perform async I/O, then re-acquire to apply.
pub struct VirtualFs {
    runtime: tokio::runtime::Handle,
    hub_client: Arc<dyn HubOps>,
    xet_sessions: Arc<dyn XetOps>,
    staging_dir: Option<StagingDir>,
    read_only: bool,
    advanced_writes: bool,
    inode_table: Arc<RwLock<InodeTable>>,
    /// Maps file_handle → OpenFile (local fd or lazy remote reference).
    open_files: RwLock<HashMap<u64, OpenFile>>,
    next_file_handle: AtomicU64,
    uid: u32,
    gid: u32,
    /// Negative lookup cache: paths known to not exist (TTL-based).
    negative_cache: Arc<RwLock<HashMap<String, Instant>>>,
    /// Per-inode locks for staging file preparation (prevents concurrent open races).
    staging_locks: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-directory loading locks: serializes concurrent ensure_children_loaded() calls
    /// for the same directory so only one HTTP request is made (prevents thundering herd
    /// when Finder/Spotlight send many lookups on mount).
    dir_loading_locks: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-inode pending commit receivers. release() publishes the commit result here;
    /// open() awaits it instead of blindly blocking on staging_lock.
    pending_commits: Mutex<HashMap<u64, CommitHookRx>>,
    /// Batched flush pipeline: dirty file writes + remote delete queue.
    /// Only present in advanced_writes mode.
    flush_manager: Option<flush::FlushManager>,
    /// Background poll task handle, aborted in shutdown().
    poll_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Kernel cache invalidation callback. Set via `set_invalidator()` after mount.
    /// The poll loop calls this to actively invalidate stale inodes when remote changes
    /// are detected, allowing the kernel page cache to be used instead of DIRECT_IO.
    invalidator: Invalidator,
    /// How long a file's metadata is trusted before re-checking via HEAD.
    /// Matches the kernel metadata TTL so HEAD is called at most once per TTL window.
    metadata_ttl: Duration,
    /// When false (minimal mode), every lookup triggers a HEAD request regardless
    /// of `last_revalidated`.
    /// When true, lookups within the metadata TTL window skip HEAD.
    serve_lookup_from_cache: bool,
    /// When true, reject creation of OS junk files (.DS_Store, Thumbs.db, etc.).
    filter_os_files: bool,
    /// When true, prefetch buffers drain after serving (forward-only, no re-read cache).
    direct_io: bool,
}

/// Where to read file content from when opening read-only.
impl VirtualFs {
    pub fn new(
        runtime: tokio::runtime::Handle,
        hub_client: Arc<dyn HubOps>,
        xet_sessions: Arc<dyn XetOps>,
        staging_dir: Option<StagingDir>,
        config: VfsConfig,
    ) -> Arc<Self> {
        let inodes = Arc::new(RwLock::new(InodeTable::new()));
        let negative_cache = Arc::new(RwLock::new(HashMap::new()));

        let flush_manager = if !config.read_only && config.advanced_writes {
            let sd = staging_dir
                .as_ref()
                .expect("--advanced-writes requires a staging directory");
            Some(flush::FlushManager::new(
                xet_sessions.clone(),
                sd.clone(),
                hub_client.clone(),
                inodes.clone(),
                &runtime,
                config.flush_debounce,
                config.flush_max_batch_window,
            ))
        } else {
            None
        };

        // Spawn remote change polling task (if interval > 0)
        let invalidator: Invalidator = Arc::new(Mutex::new(None));
        let poll_handle = if config.poll_interval_secs > 0 {
            let bg_hub = hub_client.clone();
            let bg_inodes = inodes.clone();
            let bg_neg_cache = negative_cache.clone();
            let bg_invalidator = invalidator.clone();
            let interval = Duration::from_secs(config.poll_interval_secs);

            Some(runtime.spawn(Self::poll_remote_changes(
                bg_hub,
                bg_inodes,
                bg_neg_cache,
                bg_invalidator,
                interval,
            )))
        } else {
            None
        };

        let vfs = Arc::new(Self {
            runtime,
            hub_client,
            xet_sessions,
            staging_dir,
            read_only: config.read_only,
            advanced_writes: config.advanced_writes,
            inode_table: inodes,
            open_files: RwLock::new(HashMap::new()),
            next_file_handle: AtomicU64::new(1),
            uid: config.uid,
            gid: config.gid,
            negative_cache,
            staging_locks: Mutex::new(HashMap::new()),
            dir_loading_locks: Mutex::new(HashMap::new()),
            pending_commits: Mutex::new(HashMap::new()),
            flush_manager,
            poll_handle: Mutex::new(poll_handle),
            invalidator,
            metadata_ttl: config.metadata_ttl,
            serve_lookup_from_cache: config.serve_lookup_from_cache,
            filter_os_files: config.filter_os_files,
            direct_io: config.direct_io,
        });

        // Set root inode mtime and ownership (repos use the last commit date).
        {
            let mut inodes = vfs.inode_table.write().expect("inodes poisoned");
            if let Some(root) = inodes.get_mut(inode::ROOT_INODE) {
                root.mtime = vfs.hub_client.default_mtime();
                root.atime = root.mtime;
                root.uid = vfs.uid;
                root.gid = vfs.gid;
            }
        }

        // Pre-load root directory so `ls /mount` is instant.
        // Subdirectories are lazy-loaded on first access.
        if let Err(e) = vfs.runtime.block_on(vfs.ensure_children_loaded(inode::ROOT_INODE)) {
            error!("Failed to pre-load root directory: errno={}", e);
        }

        vfs
    }

    /// Set the kernel cache invalidation callback. Called after mount setup
    /// so the poll loop can actively invalidate stale inodes on remote changes.
    pub fn set_invalidator(&self, f: Box<dyn Fn(u64) + Send + Sync>) {
        *self.invalidator.lock().expect("invalidator poisoned") = Some(f);
    }

    /// Graceful shutdown: abort polling, drain flush queue, wait for completion.
    pub fn shutdown(&self) {
        info!("Shutting down VFS, flushing pending writes...");
        // Abort background tasks.
        if let Some(handle) = self.poll_handle.lock().expect("poll_handle poisoned").take() {
            handle.abort();
        }
        // Flush all dirty files + queued deletes.
        if let Some(fm) = &self.flush_manager {
            let dirty = self.inode_table.read().expect("inodes poisoned").dirty_inos();
            fm.shutdown(dirty, &self.runtime);
        }
        info!("Flush loop finished, VFS shut down.");
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn make_vfs_attr(&self, entry: &InodeEntry) -> VirtualFsAttr {
        let perm = if self.read_only {
            match entry.kind {
                InodeKind::File => 0o444,
                InodeKind::Directory => 0o555,
                InodeKind::Symlink => 0o777,
            }
        } else {
            match entry.kind {
                InodeKind::Symlink => 0o777,
                _ => entry.mode,
            }
        };

        VirtualFsAttr {
            ino: entry.inode,
            size: entry.size,
            blocks: entry.size.div_ceil(BLOCK_SIZE as u64),
            mtime: entry.mtime,
            atime: entry.atime,
            ctime: entry.ctime,
            kind: entry.kind,
            perm,
            nlink: entry.nlink,
            uid: entry.uid,
            gid: entry.gid,
        }
    }

    /// Revalidate a remote file by checking the Hub for metadata changes.
    /// Skips HEAD if the inode was validated within `metadata_ttl`.
    /// If the file's xet_hash changed, updates the inode and invalidates kernel cache.
    /// If the file was deleted (404), removes the inode.
    /// On network errors, silently returns (graceful degradation).
    async fn revalidate_file(&self, ino: u64, full_path: &str, current_hash: Option<&str>, current_etag: Option<&str>) {
        // When serve_lookup_from_cache is true, skip HEAD if recently validated.
        // When false (minimal mode), always HEAD on every lookup.
        if self.serve_lookup_from_cache {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            if let Some(entry) = inodes.get(ino)
                && let Some(last) = entry.last_revalidated
                && last.elapsed() < self.metadata_ttl
            {
                return;
            }
        }

        let remote = match self.hub_client.head_file(full_path).await {
            Ok(r) => r,
            Err(e) => {
                debug!("head_file({}) failed, using cached: {}", full_path, e);
                return;
            }
        };

        match remote {
            None => {
                // File deleted remotely → remove from inode table
                info!("Remote deletion detected via HEAD: {}", full_path);
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                if let Some(entry) = inodes.get(ino) {
                    let parent_ino = entry.parent;
                    if let Some(invalidate) = self.invalidator.lock().expect("invalidator poisoned").as_ref() {
                        invalidate(parent_ino);
                        invalidate(ino);
                    }
                }
                inodes.remove(ino);
            }
            Some(head_info) => {
                let remote_hash = head_info.xet_hash.as_deref();
                let remote_etag = head_info.etag.as_deref();
                // Detect changes via xet_hash (preferred) or ETag.
                let changed = if current_hash.is_some() || remote_hash.is_some() {
                    remote_hash != current_hash
                } else {
                    remote_etag != current_etag
                };
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                if changed {
                    let remote_size = match head_info.size {
                        Some(s) => s,
                        None => {
                            warn!("HEAD response missing size for {}, skipping update", full_path);
                            return;
                        }
                    };
                    let remote_mtime = head_info
                        .last_modified
                        .as_deref()
                        .map(crate::hub_api::mtime_from_http_date)
                        .unwrap_or(SystemTime::now());
                    debug!("Remote change detected via HEAD: {}", full_path);
                    inodes.update_remote_file(
                        ino,
                        remote_hash.map(|s| s.to_string()),
                        remote_etag.map(|s| s.to_string()),
                        remote_size,
                        remote_mtime,
                    );
                    if let Some(invalidate) = self.invalidator.lock().expect("invalidator poisoned").as_ref() {
                        invalidate(ino);
                    }
                } else {
                    // Update stored etag even when content didn't change,
                    // so future revalidations have the latest value.
                    if let Some(entry) = inodes.get_mut(ino)
                        && remote_etag.is_some()
                    {
                        entry.etag = remote_etag.map(|s| s.to_string());
                    }
                }
                // Stamp revalidation time regardless of whether content changed
                if let Some(entry) = inodes.get_mut(ino) {
                    entry.last_revalidated = Some(Instant::now());
                }
            }
        }
    }

    /// Ensure children of a directory inode are loaded from the Hub API.
    /// Fetch remote children for `parent_ino` if not already loaded.
    /// Uses a per-directory lock to prevent thundering herd: concurrent callers
    /// for the same directory wait on the lock rather than making duplicate HTTP calls.
    /// Returns ENOENT if the inode doesn't exist, ENOTDIR if it's not a directory.
    async fn ensure_children_loaded(&self, parent_ino: u64) -> VirtualFsResult<()> {
        // Fast path: already loaded (no lock needed).
        {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            match inodes.get(parent_ino) {
                Some(e) if e.kind != InodeKind::Directory => return Err(libc::ENOTDIR),
                Some(e) if e.children_loaded => return Ok(()),
                None => return Err(libc::ENOENT),
                _ => {}
            }
        }

        // Serialize concurrent loads for the same directory.
        let dir_lock = self.dir_loading_lock(parent_ino);
        let _guard = dir_lock.lock().await;

        // Re-check: another task may have loaded while we waited.
        let prefix = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            match inodes.get(parent_ino) {
                Some(e) if e.kind != InodeKind::Directory => return Err(libc::ENOTDIR),
                Some(e) if e.children_loaded => return Ok(()),
                Some(e) => e.full_path.clone(),
                None => return Err(libc::ENOENT),
            }
        };

        let entries = match self.hub_client.list_tree(&prefix).await {
            Ok(entries) => entries,
            Err(e) => {
                error!("Failed to list tree for prefix '{}': {}", prefix, e);
                return Err(libc::EIO);
            }
        };

        let mut inodes = self.inode_table.write().expect("inodes poisoned");
        match inodes.get(parent_ino) {
            Some(e) if e.children_loaded => return Ok(()),
            Some(e) if e.kind != InodeKind::Directory => return Err(libc::ENOTDIR),
            None => return Err(libc::ENOENT),
            _ => {}
        }
        let mut seen_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        for entry in entries {
            // Convert absolute bucket path to path relative to current directory.
            // e.g. prefix="models" → "models/bert/config.json" → "bert/config.json"
            let rel_path = if prefix.is_empty() {
                entry.path.clone()
            } else {
                entry
                    .path
                    .strip_prefix(&prefix)
                    .and_then(|p| p.strip_prefix('/'))
                    .unwrap_or(&entry.path)
                    .to_string()
            };

            if let Some(slash_pos) = rel_path.find('/') {
                // Nested path → create the immediate subdirectory only (lazy-loaded later)
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
                        self.hub_client.default_mtime(),
                        None,
                        0o755,
                        self.uid,
                        self.gid,
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
                    .map(crate::hub_api::mtime_from_str)
                    .unwrap_or_else(|| self.hub_client.default_mtime());
                let default_mode = if kind == InodeKind::Directory { 0o755 } else { 0o644 };

                let ino = inodes.insert(
                    parent_ino,
                    rel_path.to_string(),
                    entry.path,
                    kind,
                    size,
                    mtime,
                    entry.xet_hash,
                    default_mode,
                    self.uid,
                    self.gid,
                );
                let rel_name = rel_path.to_string();
                seen_names.insert(rel_name);
                if let Some(oid) = entry.oid
                    && let Some(e) = inodes.get_mut(ino)
                {
                    e.etag = Some(oid);
                }
            }
        }

        // Remove stale children: entries that existed locally but are no longer
        // in the Hub listing. Skip dirty files (local writes take precedence) and
        // files with open handles (in-flight reads/writes).
        if let Some(parent) = inodes.get(parent_ino) {
            let stale: Vec<u64> = parent
                .children
                .iter()
                .filter(|c| {
                    let in_listing = seen_names.contains(&c.name) || seen_dirs.contains(&c.name);
                    if in_listing {
                        return false;
                    }
                    inodes.get(c.ino).is_some_and(|e| !e.is_dirty())
                })
                .map(|c| c.ino)
                .collect();
            for ino in stale {
                // Don't remove if the inode or any descendant is dirty or has open handles.
                let has_dirty_or_open = inodes.has_dirty_descendants(ino) || self.has_open_handles(ino);
                if !has_dirty_or_open {
                    inodes.remove(ino);
                }
            }
        }

        if let Some(parent) = inodes.get_mut(parent_ino) {
            parent.children_loaded = true;
        }
        Ok(())
    }

    /// Recursively load all descendants of a directory so in-memory state
    /// is complete. Used before directory rename to build accurate remote ops.
    async fn ensure_subtree_loaded(&self, ino: u64) -> VirtualFsResult<()> {
        self.ensure_children_loaded(ino).await?;
        // Collect child directories under read lock, then load each recursively
        let child_dirs: Vec<u64> = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            match inodes.get(ino) {
                Some(entry) => entry
                    .children
                    .iter()
                    .filter(|c| inodes.get(c.ino).is_some_and(|e| e.kind == InodeKind::Directory))
                    .map(|c| c.ino)
                    .collect(),
                None => return Ok(()),
            }
        };
        for child_dir in child_dirs {
            Box::pin(self.ensure_subtree_loaded(child_dir)).await?;
        }
        Ok(())
    }

    pub fn alloc_file_handle(&self) -> u64 {
        self.next_file_handle.fetch_add(1, Ordering::Relaxed)
    }

    /// Check if any open file handle references the given inode.
    fn has_open_handles(&self, ino: u64) -> bool {
        self.open_files
            .read()
            .expect("open_files poisoned")
            .values()
            .any(|of| match of {
                OpenFile::Local { ino: i, .. } | OpenFile::Lazy { ino: i, .. } | OpenFile::Streaming { ino: i, .. } => {
                    *i == ino
                }
            })
    }

    /// Get or create a per-directory lock for serializing ensure_children_loaded().
    fn dir_loading_lock(&self, ino: u64) -> Arc<tokio::sync::Mutex<()>> {
        self.dir_loading_locks
            .lock()
            .expect("dir_loading_locks poisoned")
            .entry(ino)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Get or create a per-inode lock for staging file preparation.
    fn staging_lock(&self, ino: u64) -> Arc<tokio::sync::Mutex<()>> {
        self.staging_locks
            .lock()
            .expect("staging_locks poisoned")
            .entry(ino)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Install a pending commit watch hook on a streaming channel.
    /// Called in flush() before commit or deferral so open() can await the result.
    /// No-op if a hook is already installed (prevents replacing a receiver that
    /// an open() caller may already be awaiting).
    fn install_commit_hook(&self, ino: u64, channel: &StreamingChannel) {
        let mut hook = channel.commit_hook.lock().expect("commit_hook poisoned");
        if hook.is_some() {
            return; // already installed — don't replace
        }
        let (tx, rx) = tokio::sync::watch::channel(None);
        *hook = Some(tx);
        self.pending_commits
            .lock()
            .expect("pending_commits poisoned")
            .insert(ino, rx);
    }

    /// Fulfill the pending commit hook with a result, then clean up the map.
    fn fulfill_commit_hook(&self, ino: u64, channel: &StreamingChannel, result: Result<(), i32>) {
        if let Some(tx) = channel.commit_hook.lock().expect("commit_hook poisoned").take() {
            let _ = tx.send(Some(result));
        }
        self.pending_commits
            .lock()
            .expect("pending_commits poisoned")
            .remove(&ino);
    }

    /// Wait for any in-flight streaming commit on this inode to complete.
    /// Returns Ok(()) if no pending commit or commit succeeded, Err(errno) if it failed.
    async fn await_pending_commit(&self, ino: u64) -> VirtualFsResult<()> {
        let pending_rx = self
            .pending_commits
            .lock()
            .expect("pending_commits poisoned")
            .get(&ino)
            .cloned();

        if let Some(mut rx) = pending_rx {
            while rx.borrow().is_none() {
                if rx.changed().await.is_err() {
                    // Sender dropped without publishing a result — treat as failure.
                    error!("await_pending_commit: sender dropped for ino={}", ino);
                    return Err(libc::EIO);
                }
            }
            if let Some(Err(e)) = &*rx.borrow() {
                debug!("await_pending_commit: commit failed for ino={}: errno={}", ino, e);
                // Inode was reverted — not an error for the caller, just informational.
            }
        }
        Ok(())
    }

    /// Set up a new streaming writer + channel. Returns (file_handle, channel).
    /// Used by both create() and open(O_TRUNC) in simple mode.
    async fn setup_streaming_writer(
        &self,
        pid: Option<u32>,
        snapshot: InodeSnapshot,
        dirty_generation_at_open: u64,
    ) -> VirtualFsResult<(u64, Arc<StreamingChannel>)> {
        let streaming_writer = self.xet_sessions.create_streaming_writer().await.map_err(|e| {
            error!("Failed to create streaming writer: {}", e);
            libc::EIO
        })?;

        // Bounded channel provides backpressure so a fast writer doesn't queue
        // unbounded memory. 32 slots × ~128KB FUSE write = ~4MB max in-flight.
        // blocking_send is safe here: FUSE threads are not tokio workers.
        let (tx, rx) = tokio::sync::mpsc::channel::<WriteMsg>(32);
        let error: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        self.runtime
            .spawn(streaming_worker(streaming_writer, rx, error.clone()));

        let channel = Arc::new(StreamingChannel {
            tx,
            bytes_written: AtomicU64::new(0),
            error,
            state: std::sync::Mutex::new(CommitState::Writing),
            pending_info: std::sync::Mutex::new(None),
            open_pid: pid,
            snapshot,
            dirty_generation_at_open: AtomicU64::new(dirty_generation_at_open),
            commit_hook: std::sync::Mutex::new(None),
        });

        let file_handle = self.alloc_file_handle();
        Ok((file_handle, channel))
    }

    /// Open a local file as read-only and return the file handle.
    fn open_local_readonly(&self, ino: u64, path: &PathBuf) -> VirtualFsResult<u64> {
        match File::open(path) {
            Ok(file) => {
                let file_handle = self.alloc_file_handle();
                self.open_files.write().expect("open_files poisoned").insert(
                    file_handle,
                    OpenFile::Local {
                        ino,
                        file: Arc::new(file),
                        writable: false,
                    },
                );
                Ok(file_handle)
            }
            Err(e) => {
                error!("Failed to open file {:?}: {}", path, e);
                Err(libc::EIO)
            }
        }
    }

    /// Check if a path is in the negative cache (and not expired).
    fn negative_cache_check(&self, path: &str) -> bool {
        let cache = self.negative_cache.read().expect("negative_cache poisoned");
        matches!(cache.get(path), Some(inserted) if inserted.elapsed() < NEG_CACHE_TTL)
    }

    /// Remove a path from the negative cache (e.g. after create/rename).
    fn negative_cache_remove(&self, path: &str) {
        self.negative_cache.write().expect("neg_cache poisoned").remove(path);
    }

    /// Insert a path into the negative cache, evicting if at capacity.
    /// Eviction is amortized: instead of scanning all entries, we sample a
    /// bounded batch to keep the write lock duration constant regardless of cache size.
    fn negative_cache_insert(&self, path: String) {
        let mut cache = self.negative_cache.write().expect("neg_cache poisoned");
        let now = Instant::now();
        if cache.len() >= NEG_CACHE_CAPACITY {
            // Evict up to 128 expired entries (bounded scan, not full retain).
            let expired: Vec<String> = cache
                .iter()
                .filter(|(_, ts)| ts.elapsed() >= NEG_CACHE_TTL)
                .take(128)
                .map(|(k, _)| k.clone())
                .collect();
            for key in &expired {
                cache.remove(key);
            }
            // If still full after evicting expired, drop the oldest sampled entry.
            if cache.len() >= NEG_CACHE_CAPACITY
                && let Some(oldest_key) = cache
                    .iter()
                    .take(128)
                    .min_by_key(|(_, ts)| **ts)
                    .map(|(k, _)| k.clone())
            {
                cache.remove(&oldest_key);
            }
        }
        cache.insert(path, now);
    }

    // ── VFS operations ─────────────────────────────────────────────────

    pub async fn lookup(&self, parent: u64, name: &str) -> VirtualFsResult<VirtualFsAttr> {
        debug!("lookup: parent={}, name={}", parent, name);

        // Fast path: children already loaded → lookup directly, no allocation needed.
        // Revalidation info extracted from the lock scope so we can await outside it.
        enum FastResult {
            Hit(VirtualFsAttr),
            NeedsRevalidation {
                ino: u64,
                full_path: String,
                current_hash: Option<String>,
                current_etag: Option<String>,
            },
            Miss(String), // full_path for negative cache
            NotLoaded,
        }
        let fast = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let parent_entry = inodes.get(parent).ok_or(libc::ENOENT)?;

            if parent_entry.kind != InodeKind::Directory {
                return Err(libc::ENOTDIR);
            }

            if parent_entry.children_loaded {
                if let Some(entry) = inodes.lookup_child(parent, name) {
                    // Revalidate remote files via HEAD (xet-backed and plain git/LFS).
                    if entry.kind == InodeKind::File && !entry.is_dirty() {
                        FastResult::NeedsRevalidation {
                            ino: entry.inode,
                            full_path: entry.full_path.clone(),
                            current_hash: entry.xet_hash.clone(),
                            current_etag: entry.etag.clone(),
                        }
                    } else {
                        FastResult::Hit(self.make_vfs_attr(entry))
                    }
                } else {
                    let parent_path = &parent_entry.full_path;
                    let full_path = if parent_path.is_empty() {
                        name.to_string()
                    } else {
                        format!("{}/{}", parent_path, name)
                    };
                    FastResult::Miss(full_path)
                }
            } else {
                FastResult::NotLoaded
            }
        }; // inodes lock dropped here

        match fast {
            FastResult::Hit(attr) => return Ok(attr),
            FastResult::NeedsRevalidation {
                ino,
                full_path,
                current_hash,
                current_etag,
            } => {
                self.revalidate_file(ino, &full_path, current_hash.as_deref(), current_etag.as_deref())
                    .await;
                let inodes = self.inode_table.read().expect("inodes poisoned");
                return match inodes.get(ino) {
                    Some(entry) => Ok(self.make_vfs_attr(entry)),
                    None => Err(libc::ENOENT),
                };
            }
            FastResult::Miss(full_path) => {
                self.negative_cache_insert(full_path);
                return Err(libc::ENOENT);
            }
            FastResult::NotLoaded => {} // fall through to slow path
        }

        // Slow path: children not loaded yet, fetch from Hub API.
        let full_path = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            match inodes.get(parent) {
                Some(e) => inode::child_path(&e.full_path, name),
                None => return Err(libc::ENOENT),
            }
        };

        if self.negative_cache_check(&full_path) {
            debug!("negative cache hit: {}", full_path);
            return Err(libc::ENOENT);
        }

        self.ensure_children_loaded(parent).await?;

        let inodes = self.inode_table.read().expect("inodes poisoned");
        match inodes.lookup_child(parent, name) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => {
                drop(inodes);
                self.negative_cache_insert(full_path);
                Err(libc::ENOENT)
            }
        }
    }

    pub fn default_uid(&self) -> u32 {
        self.uid
    }

    pub fn default_gid(&self) -> u32 {
        self.gid
    }

    /// Schedule a debounced flush for a dirty inode.
    /// Used by NFS (which has no close/flush RPC) to ensure writes
    /// eventually get committed to the Hub.
    pub fn schedule_flush(&self, ino: u64) {
        if let Some(fm) = &self.flush_manager {
            fm.enqueue(ino);
        }
    }

    /// Synchronous fsync: ensures data is committed to Hub before returning.
    /// - Streaming handles: no-op (data is committed synchronously on close).
    ///   Cannot delegate to flush() because it sends Finish which tears down the writer.
    /// - Advanced-writes handles: uploads staging file and commits immediately.
    /// - Read-only / lazy handles: no-op.
    pub async fn fsync(&self, ino: u64, file_handle: u64, _pid: Option<u32>) -> VirtualFsResult<()> {
        {
            let files = self.open_files.read().expect("open_files poisoned");
            if matches!(files.get(&file_handle), Some(OpenFile::Streaming { .. })) {
                return Ok(());
            }
        }
        // Advanced-writes: flush staging file to Hub immediately.
        // Note: if the flush_loop is concurrently processing this inode, both may
        // upload the same content. This is benign (idempotent commit, generation-aware
        // clear ensures only one clears dirty).
        let staging_dir = match &self.staging_dir {
            Some(sd) => sd,
            None => return Ok(()),
        };
        flush::flush_one(
            ino,
            &*self.xet_sessions,
            staging_dir,
            &*self.hub_client,
            &self.inode_table,
        )
        .await
    }

    pub fn getattr(&self, ino: u64) -> VirtualFsResult<VirtualFsAttr> {
        debug!("getattr: ino={}", ino);

        let inodes = self.inode_table.read().expect("inodes poisoned");
        match inodes.get(ino) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => Err(libc::ENOENT),
        }
    }

    pub async fn readdir(&self, ino: u64) -> VirtualFsResult<Vec<VirtualFsDirEntry>> {
        debug!("readdir: ino={}", ino);

        self.ensure_children_loaded(ino).await?;

        let inodes = self.inode_table.read().expect("inodes poisoned");
        let entry = inodes.get(ino).ok_or(libc::ENOENT)?;

        let mut entries = Vec::with_capacity(2 + entry.children.len());
        entries.push(VirtualFsDirEntry {
            ino,
            kind: InodeKind::Directory,
            name: ".".to_string(),
        });
        entries.push(VirtualFsDirEntry {
            ino: entry.parent,
            kind: InodeKind::Directory,
            name: "..".to_string(),
        });

        for child_ref in &entry.children {
            if let Some(child) = inodes.get(child_ref.ino) {
                entries.push(VirtualFsDirEntry {
                    ino: child.inode,
                    kind: child.kind,
                    name: child_ref.name.clone(),
                });
            }
        }

        Ok(entries)
    }

    pub async fn open(&self, ino: u64, writable: bool, truncate: bool, pid: Option<u32>) -> VirtualFsResult<u64> {
        debug!(
            "open: ino={}, writable={}, truncate={}, pid={:?}",
            ino, writable, truncate, pid
        );

        if writable && self.read_only {
            return Err(libc::EROFS);
        }

        let file_entry = self.get_file_entry(ino)?;
        let staging_path = self.staging_dir.as_ref().map(|sd| sd.path(ino));

        if writable && self.advanced_writes {
            // Staging file + async flush (supports random writes and seek)
            self.open_advanced_write(ino, &file_entry.xet_hash, file_entry.size, staging_path, truncate)
                .await
        } else if writable && truncate {
            // Simple streaming write (append-only, synchronous commit on close)
            self.open_streaming_write(ino, pid).await
        } else if writable {
            // Simple mode without O_TRUNC: random writes not supported
            Err(libc::EPERM)
        } else {
            self.open_readonly(ino, file_entry, staging_path).await
        }
    }

    /// Advanced writes: prepare a staging file and open it for read-write.
    async fn open_advanced_write(
        &self,
        ino: u64,
        xet_hash: &str,
        size: u64,
        staging_path: Option<PathBuf>,
        truncate: bool,
    ) -> VirtualFsResult<u64> {
        let staging_path = staging_path.expect("staging_dir required for advanced writes");

        // Serialize staging preparation per inode (prevents concurrent download races)
        let staging_mutex = self.staging_lock(ino);
        let _staging_guard = staging_mutex.lock().await;

        // Re-read dirty state under the staging lock (may have changed since first check)
        let is_dirty = self
            .inode_table
            .read()
            .expect("inodes poisoned")
            .get(ino)
            .ok_or(libc::ENOENT)?
            .is_dirty();

        // Reuse existing dirty staging file (unless truncating)
        if !(is_dirty && staging_path.exists() && !truncate) {
            if !truncate && !xet_hash.is_empty() && size > 0 {
                // Download remote content for read-modify-write
                self.xet_sessions
                    .download_to_file(xet_hash, size, &staging_path)
                    .await
                    .map_err(|e| {
                        error!("Failed to download file for write: {}", e);
                        libc::EIO
                    })?;
            } else {
                // Truncate, new file, or empty remote → empty staging file
                File::create(&staging_path).map_err(|e| {
                    error!("Failed to create staging file: {}", e);
                    libc::EIO
                })?;
            }
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&staging_path)
            .map_err(|e| {
                error!("Failed to open staging file: {}", e);
                libc::EIO
            })?;

        // Re-check inode still exists before committing the open
        {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            let entry = inodes.get_mut(ino).ok_or(libc::ENOENT)?;
            entry.set_dirty();
            if truncate {
                entry.size = 0;
                // POSIX: O_TRUNC must update mtime and ctime
                let now = SystemTime::now();
                entry.mtime = now;
                entry.ctime = now;
            }
        }

        let file_handle = self.alloc_file_handle();
        self.open_files.write().expect("open_files poisoned").insert(
            file_handle,
            OpenFile::Local {
                ino,
                file: Arc::new(file),
                writable: true,
            },
        );
        Ok(file_handle)
    }

    /// Simple streaming write: truncate existing file and set up a new streaming writer.
    async fn open_streaming_write(&self, ino: u64, pid: Option<u32>) -> VirtualFsResult<u64> {
        // Wait for any in-flight commit to complete before starting a new writer.
        self.await_pending_commit(ino).await?;

        let staging_mutex = self.staging_lock(ino);
        let _staging_guard = staging_mutex.lock().await;

        // Capture inode snapshot before mutation (for revert on commit failure)
        let snapshot = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let entry = inodes.get(ino).ok_or(libc::ENOENT)?;
            InodeSnapshot {
                xet_hash: entry.xet_hash.clone(),
                size: entry.size,
                mtime: entry.mtime,
                pending_deletes: entry.pending_deletes.clone(),
                existed_before: true,
            }
        };

        let (file_handle, channel) = self.setup_streaming_writer(pid, snapshot, 0).await?;

        {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            if let Some(entry) = inodes.get_mut(ino) {
                entry.set_dirty();
                entry.size = 0;
                entry.xet_hash = None;
                channel
                    .dirty_generation_at_open
                    .store(entry.dirty_generation, Ordering::Relaxed);
            }
        }

        self.open_files
            .write()
            .expect("open_files poisoned")
            .insert(file_handle, OpenFile::Streaming { ino, channel });
        Ok(file_handle)
    }

    /// Open a file for reading. Dispatches based on where the content lives.
    async fn open_readonly(&self, ino: u64, fe: FileEntry, staging_path: Option<PathBuf>) -> VirtualFsResult<u64> {
        match (fe.is_dirty, &staging_path) {
            // Advanced write in progress — read from local staging file.
            (true, Some(path)) if path.exists() => self.open_local_readonly(ino, path),

            // Dirty file but staging file is missing — should not happen.
            (true, Some(_)) => {
                error!("Dirty file ino={} has missing staging file", ino);
                Err(libc::EIO)
            }

            // Streaming write in progress (simple mode) — wait for commit.
            (true, None) if fe.xet_hash.is_empty() && fe.size > 0 => {
                self.await_pending_commit(ino).await?;
                let fe = self.get_file_entry(ino)?;
                self.open_lazy(ino, fe.xet_hash, fe.size)
            }

            // Remote xet-backed file — lazy CAS range reads.
            _ if !fe.xet_hash.is_empty() => self.open_lazy(ino, fe.xet_hash, fe.size),

            // Plain LFS/git file without xet hash — HTTP download to staging cache.
            _ if fe.size > 0 => {
                let staging = self.staging_dir.as_ref().ok_or_else(|| {
                    error!("No staging dir for HTTP download of ino={}", ino);
                    libc::EIO
                })?;
                let path_hash = {
                    use std::hash::{Hash, Hasher};
                    let mut h = std::collections::hash_map::DefaultHasher::new();
                    self.hub_client.source().to_string().hash(&mut h);
                    fe.full_path.hash(&mut h);
                    h.finish()
                };
                let dest = staging.root().join(format!("http_{:x}", path_hash));
                {
                    let lock = self.staging_lock(ino);
                    let _guard = lock.lock().await;
                    // download_file_http uses ETag-based conditional requests,
                    // so this is cheap when the cached file is still valid (304).
                    self.hub_client
                        .download_file_http(&fe.full_path, &dest)
                        .await
                        .map_err(|e| {
                            error!("HTTP download failed for {}: {}", fe.full_path, e);
                            libc::EIO
                        })?;
                }
                self.open_local_readonly(ino, &dest)
            }

            // Empty file (size=0, no hash).
            _ => self.open_lazy(ino, String::new(), 0),
        }
    }

    /// Allocate a lazy file handle backed by a prefetch buffer.
    fn open_lazy(&self, ino: u64, xet_hash: String, size: u64) -> VirtualFsResult<u64> {
        let prefetch = Arc::new(tokio::sync::Mutex::new(PrefetchState::new(
            xet_hash,
            size,
            self.direct_io,
        )));
        let file_handle = self.alloc_file_handle();
        self.open_files
            .write()
            .expect("open_files poisoned")
            .insert(file_handle, OpenFile::Lazy { ino, prefetch });
        Ok(file_handle)
    }

    /// Fetch data for the prefetch buffer. Uses the persistent stream for sequential
    /// reads (with automatic (re)start), or opens a temporary stream with retries
    /// for range/seek access. Returns EIO if all attempts fail.
    async fn fetch_data(
        &self,
        prefetch_state: &mut PrefetchState,
        cursor: u64,
        plan: &FetchPlan,
        file_size: u64,
    ) -> std::result::Result<(VecDeque<Bytes>, usize), i32> {
        const MAX_ATTEMPTS: u32 = 3;
        let file_info = XetFileInfo::new(prefetch_state.xet_hash.clone(), prefetch_state.file_size);

        // Take the persistent stream before the loop (sequential reads).
        // first_stream.take() only returns Some on the first iteration.
        let mut first_stream = if plan.strategy.is_stream() {
            prefetch_state.stream.take()
        } else {
            None
        };

        for attempt in 0..MAX_ATTEMPTS {
            let stream = match first_stream.take() {
                Some(s) => Some(s),
                None => match self.xet_sessions.download_stream_boxed(
                    &file_info,
                    cursor,
                    // For range downloads (random reads / seeks), bound the request
                    // so CAS only returns terms covering the needed bytes.
                    // For streaming, leave unbounded so the stream can continue.
                    if plan.strategy == prefetch::FetchStrategy::RangeDownload {
                        Some(cursor + plan.fetch_size)
                    } else {
                        None
                    },
                ) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        warn!(
                            "prefetch: stream open failed: cursor={}, attempt={}/{}: {}",
                            cursor,
                            attempt + 1,
                            MAX_ATTEMPTS,
                            e,
                        );
                        None
                    }
                },
            };

            if let Some(mut stream) = stream {
                // Read chunks until fetch_size bytes
                let mut chunks = VecDeque::new();
                let mut total = 0usize;
                let mut failed = false;
                let mut stream_eof = false;
                while (total as u64) < plan.fetch_size {
                    match stream.next().await {
                        Ok(Some(chunk)) => {
                            total += chunk.len();
                            chunks.push_back(chunk);
                        }
                        Ok(None) => {
                            stream_eof = true;
                            break;
                        }
                        Err(e) => {
                            warn!(
                                "prefetch: stream read error at cursor={}, got={}/{}, attempt={}/{}: {}",
                                cursor,
                                total,
                                plan.fetch_size,
                                attempt + 1,
                                MAX_ATTEMPTS,
                                e,
                            );
                            failed = true;
                            break;
                        }
                    }
                }

                // Early EOF sanity check: only meaningful when we got some data
                // (zero-data EOF is just a failed attempt, handled by retry below)
                if stream_eof && total > 0 {
                    let stream_total = cursor + total as u64;
                    if stream_total < file_size {
                        error!(
                            "prefetch: stream EOF at cursor={} after {} bytes (file_size={}, \
                             shortfall={}, hash={})",
                            cursor,
                            total,
                            file_size,
                            file_size - stream_total,
                            prefetch_state.xet_hash,
                        );
                        debug_assert!(
                            false,
                            "stream EOF before file_size: got {stream_total}, expected {file_size}"
                        );
                    }
                } else if plan.strategy.is_stream() && !failed {
                    // Keep stream alive for future sequential reads
                    prefetch_state.stream = Some(stream);
                }

                if total > 0 {
                    debug!(
                        "prefetch fetch: cursor={}, got={}, window={}",
                        cursor, total, prefetch_state.window_size,
                    );
                    return Ok((chunks, total));
                }
            }

            if attempt + 1 < MAX_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
            }
        }

        error!(
            "prefetch: all {} fetch attempts failed: cursor={}, hash={}",
            MAX_ATTEMPTS, cursor, prefetch_state.xet_hash,
        );
        Err(libc::EIO)
    }

    /// Read data from an open file. Returns `(data, eof)`.
    pub async fn read(&self, file_handle: u64, offset: u64, size: u32) -> VirtualFsResult<(Bytes, bool)> {
        debug!("read: fh={}, offset={}, size={}", file_handle, offset, size);

        // Extract what we need under the lock, then release it.
        // Clone Arc<File> so the FD stays alive even if release() runs concurrently.
        let read_target = {
            let files = self.open_files.read().expect("open_files poisoned");
            match files.get(&file_handle) {
                Some(OpenFile::Local { file, .. }) => ReadTarget::LocalFd(file.clone()),
                Some(OpenFile::Lazy { prefetch, .. }) => ReadTarget::Remote {
                    prefetch: prefetch.clone(),
                },
                // write-only, not readable
                Some(OpenFile::Streaming { .. }) => return Err(libc::EBADF), // write-only, not readable
                None => return Err(libc::EBADF), // handle already closed (race with release)
            }
        };

        match read_target {
            ReadTarget::LocalFd(file) => {
                let file_descriptor = file.as_raw_fd();
                let mut buf = BytesMut::zeroed(size as usize);
                // SAFETY: fd is valid (Arc<File> keeps it alive), buf is correctly sized.
                // pread is thread-safe (atomic offset, no shared seek cursor).
                let n = unsafe {
                    libc::pread(
                        file_descriptor,
                        buf.as_mut_ptr() as *mut libc::c_void,
                        size as usize,
                        offset as i64,
                    )
                };
                if n < 0 {
                    Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO))
                } else {
                    buf.truncate(n as usize);
                    let eof = (n as u32) < size;
                    Ok((buf.freeze(), eof))
                }
            }
            ReadTarget::Remote { prefetch } => {
                let mut prefetch_state = prefetch.lock().await;

                let file_size = prefetch_state.file_size;

                // Past EOF
                if offset >= file_size {
                    return Ok((Bytes::new(), true));
                }

                // Maximum bytes we should return (capped at file boundary).
                let to_read = ((size as u64).min(file_size - offset)) as usize;

                // IMPORTANT: Never return a short read unless at real EOF.
                // The Linux FUSE kernel module shrinks i_size on short reads
                // (fuse_read_update_size), which makes subsequent reads return
                // 0 bytes and truncates the file from the application's view.

                let mut response = BytesMut::with_capacity(to_read);
                let mut cursor = offset;

                // Fast path: forward buffer has enough data (common case, zero-copy).
                if let Some(data) = prefetch_state.try_serve_forward(offset, size) {
                    if data.len() == to_read {
                        debug!("prefetch hit (forward): offset={}, len={}", offset, data.len());
                        let eof = offset + data.len() as u64 >= file_size;
                        return Ok((data, eof));
                    }
                    cursor += data.len() as u64;
                    response.extend_from_slice(&data);
                } else if let Some(data) = prefetch_state.try_serve_seek(offset, size) {
                    // Seek window: only reachable when forward buffer has no data
                    // at this offset (backward seek). Zero-copy on full hit.
                    if data.len() == to_read {
                        debug!("prefetch hit (seek): offset={}, len={}", offset, data.len());
                        let eof = offset + data.len() as u64 >= file_size;
                        return Ok((data, eof));
                    }
                    cursor += data.len() as u64;
                    response.extend_from_slice(&data);
                }

                // Assembly loop: fetch more data until we have to_read bytes.
                // Only reached at prefetch window boundaries or cache misses.
                while response.len() < to_read {
                    let remaining = (to_read - response.len()) as u32;
                    let plan = prefetch_state.prepare_fetch(cursor, remaining);
                    debug!(
                        "prefetch miss: cursor={}, remaining={}, strategy={:?}, fetch_size={}, \
                         buf_start={}, file_size={}, has_stream={}",
                        cursor,
                        remaining,
                        plan.strategy,
                        plan.fetch_size,
                        prefetch_state.buf_start,
                        file_size,
                        prefetch_state.stream.is_some(),
                    );

                    let (chunks, total) = self.fetch_data(&mut prefetch_state, cursor, &plan, file_size).await?;

                    prefetch_state.store_fetched(cursor, chunks, total);

                    // Drain freshly filled buffer into response
                    let remaining = (to_read - response.len()) as u32;
                    if let Some(data) = prefetch_state.try_serve_forward(cursor, remaining) {
                        cursor += data.len() as u64;
                        response.extend_from_slice(&data);
                    }
                }

                let eof = cursor == file_size;
                Ok((response.freeze(), eof))
            }
        }
    }

    pub fn write(&self, ino: u64, file_handle: u64, offset: u64, data: &[u8]) -> VirtualFsResult<u32> {
        debug!(
            "write: ino={}, fh={}, offset={}, len={}",
            ino,
            file_handle,
            offset,
            data.len()
        );

        if self.read_only {
            return Err(libc::EROFS);
        }

        // Resolve write target: Local = staging file on disk (--advanced-writes),
        // Streaming = append-only channel to CAS (default mode).
        // Clone out of the map so we release the RwLock before doing I/O.
        enum WriteTarget {
            Local { file: Arc<File>, ino: u64 },
            Streaming { ino: u64, channel: Arc<StreamingChannel> },
        }

        let target = {
            let files = self.open_files.read().expect("open_files poisoned");
            match files.get(&file_handle) {
                Some(OpenFile::Local {
                    ino,
                    file,
                    writable: true,
                }) => WriteTarget::Local {
                    file: file.clone(),
                    ino: *ino,
                },
                Some(OpenFile::Streaming { ino, channel }) => WriteTarget::Streaming {
                    ino: *ino,
                    channel: channel.clone(),
                },
                // Read-only handle, lazy handle, or unknown fh
                _ => return Err(libc::EBADF),
            }
        };

        match target {
            WriteTarget::Local { file, ino: handle_ino } => {
                let file_descriptor = file.as_raw_fd();
                let n = unsafe {
                    libc::pwrite(
                        file_descriptor,
                        data.as_ptr() as *const libc::c_void,
                        data.len(),
                        offset as i64,
                    )
                };

                if n < 0 {
                    Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO))
                } else {
                    let written = n as u32;
                    let new_end = offset + written as u64;
                    let mut inodes = self.inode_table.write().expect("inodes poisoned");
                    if let Some(entry) = inodes.get_mut(handle_ino) {
                        if new_end > entry.size {
                            entry.size = new_end;
                        }
                        entry.set_dirty();
                    }
                    Ok(written)
                }
            }
            WriteTarget::Streaming {
                ino: handle_ino,
                channel,
            } => {
                // Check for previous worker error
                if channel.error.lock().expect("error poisoned").is_some() {
                    return Err(libc::EIO);
                }

                // Enforce append-only: offset must match bytes written so far
                let expected = channel.bytes_written.load(Ordering::Relaxed);
                if offset != expected {
                    debug!(
                        "streaming write: non-sequential offset={} (expected {}), ino={}",
                        offset, expected, handle_ino
                    );
                    return Err(libc::EINVAL);
                }

                let len = data.len();
                channel.tx.blocking_send(WriteMsg::Data(data.to_vec())).map_err(|_| {
                    error!("streaming channel closed for ino={}", handle_ino);
                    libc::EIO
                })?;
                channel.bytes_written.fetch_add(len as u64, Ordering::Relaxed);

                let new_size = offset + len as u64;
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                if let Some(entry) = inodes.get_mut(handle_ino) {
                    entry.size = new_size;
                }

                Ok(len as u32)
            }
        }
    }

    pub async fn flush(&self, ino: u64, file_handle: u64, pid: Option<u32>) -> VirtualFsResult<()> {
        debug!("flush: ino={}, fh={}, pid={:?}", ino, file_handle, pid);

        // Check if this is a streaming handle → synchronous upload + commit
        let streaming_channel = {
            let files = self.open_files.read().expect("open_files poisoned");
            match files.get(&file_handle) {
                Some(OpenFile::Streaming { channel, .. }) => Some(channel.clone()),
                _ => None,
            }
        };

        if let Some(channel) = streaming_channel {
            // Check for worker errors first.
            if channel.error.lock().expect("error poisoned").is_some() {
                return Err(libc::EIO);
            }

            // Check current commit state.
            {
                let state = channel.state.lock().expect("state poisoned");
                match &*state {
                    CommitState::Committed => return Ok(()),
                    CommitState::Failed(msg) => {
                        debug!("flush: already failed for ino={}: {}", ino, msg);
                        return Err(libc::EIO);
                    }
                    CommitState::Writing | CommitState::Deferred => {}
                }
            }

            // PID-aware deferral: if the flushing process isn't the opener,
            // this is a dup'd fd (e.g. shell redirection) — defer to release().
            if let (Some(open_pid), Some(flush_pid)) = (channel.open_pid, pid)
                && !same_process(open_pid, flush_pid)
            {
                debug!(
                    "flush: deferring commit for ino={} (open_pid={}, flush_pid={})",
                    ino, open_pid, flush_pid
                );
                self.install_commit_hook(ino, &channel);
                *channel.state.lock().expect("state poisoned") = CommitState::Deferred;
                return Ok(());
            }

            // Secondary gate: skip commit if no data was written (covers NFS
            // and zero-write cases like `touch`). release() will handle it.
            if channel.bytes_written.load(Ordering::Relaxed) == 0 {
                self.install_commit_hook(ino, &channel);
                *channel.state.lock().expect("state poisoned") = CommitState::Deferred;
                return Ok(());
            }

            // Install hook before commit so concurrent open() can wait on us.
            self.install_commit_hook(ino, &channel);

            match self.streaming_commit(ino, &channel).await {
                Ok(()) => {
                    *channel.state.lock().expect("state poisoned") = CommitState::Committed;
                    self.fulfill_commit_hook(ino, &channel, Ok(()));
                }
                Err(e) => {
                    // CAS upload may have succeeded — file_info is preserved
                    // in pending_info for retry in release(). Don't fulfill the
                    // hook here: release() will retry and publish the final outcome.
                    // Keeping the hook active ensures concurrent open(O_TRUNC) waits
                    // for release() instead of racing with the retry.
                    return Err(e);
                }
            }
            return Ok(());
        }

        // Advanced writes mode: check if a previous async flush failed
        if let Some(fm) = &self.flush_manager
            && let Some(err_msg) = fm.check_error(ino)
        {
            error!("Deferred flush error for ino={}: {}", ino, err_msg);
            return Err(libc::EIO);
        }
        Ok(())
    }

    pub async fn release(&self, file_handle: u64) -> VirtualFsResult<()> {
        debug!("release: fh={}", file_handle);

        let removed = self
            .open_files
            .write()
            .expect("open_files poisoned")
            .remove(&file_handle);

        let released_ino = match &removed {
            Some(OpenFile::Local { ino, .. })
            | Some(OpenFile::Lazy { ino, .. })
            | Some(OpenFile::Streaming { ino, .. }) => Some(*ino),
            _ => None,
        };

        let mut release_error: Option<i32> = None;

        match removed {
            Some(OpenFile::Local {
                ino, writable: true, ..
            }) => {
                // Advanced writes: enqueue for async flush (skip unlinked files —
                // user deleted the file, no point uploading it to remote).
                let is_unlinked = self
                    .inode_table
                    .read()
                    .expect("inodes poisoned")
                    .get(ino)
                    .is_some_and(|e| e.nlink == 0);
                if !is_unlinked && let Some(fm) = &self.flush_manager {
                    fm.enqueue(ino);
                }
            }
            Some(OpenFile::Streaming { ino, channel }) => {
                let needs_commit = {
                    let state = channel.state.lock().expect("state poisoned");
                    matches!(&*state, CommitState::Writing | CommitState::Deferred)
                };
                if needs_commit {
                    // If flush() didn't defer (e.g. Writing state from a direct
                    // release without flush), install the hook now.
                    if channel.commit_hook.lock().expect("commit_hook poisoned").is_none() {
                        self.install_commit_hook(ino, &channel);
                    }

                    let result = match self.streaming_commit(ino, &channel).await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            // Retry only if CAS upload succeeded but Hub commit failed
                            // (pending_info is preserved). If the worker itself died,
                            // there's nothing to retry -- the data is gone.
                            if channel.pending_info.lock().expect("pending_info poisoned").is_some() {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                self.streaming_commit(ino, &channel).await
                            } else {
                                Err(e)
                            }
                        }
                    };

                    match &result {
                        Ok(()) => {
                            *channel.state.lock().expect("state poisoned") = CommitState::Committed;
                        }
                        Err(e) => {
                            let is_unlinked = self
                                .inode_table
                                .read()
                                .expect("inodes poisoned")
                                .get(ino)
                                .is_none_or(|entry| entry.nlink == 0);
                            if is_unlinked {
                                debug!("streaming commit failed for unlinked ino={} (expected)", ino);
                            } else {
                                error!("DATA LOSS: streaming commit failed for ino={}: errno={}", ino, e);
                            }
                            self.revert_inode(ino, &channel.snapshot);
                            *channel.state.lock().expect("state poisoned") =
                                CommitState::Failed("commit failed".into());
                            release_error = Some(*e);
                        }
                    }

                    self.fulfill_commit_hook(ino, &channel, result);
                }
            }
            _ => {}
        }

        // Clean up orphan inodes (nlink == 0) when no more handles reference them.
        if let Some(ino) = released_ino
            && !self.has_open_handles(ino)
        {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            inodes.remove_orphan(ino);
        }

        // staging_locks entries are intentionally not cleaned up here: removing
        // while another open() may hold the Arc would break serialization.
        // Entries are tiny (Arc<Mutex<()>>) and bounded by unique inodes opened.

        match release_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Revert an inode to its pre-write state after a failed streaming commit.
    fn revert_inode(&self, ino: u64, snapshot: &InodeSnapshot) {
        let mut inodes = self.inode_table.write().expect("inodes poisoned");
        if !snapshot.existed_before {
            // File was created in this session — remove it entirely.
            inodes.remove(ino);
            info!("Reverted ino={}: removed (was newly created)", ino);
        } else if let Some(entry) = inodes.get_mut(ino) {
            // Overwrite — restore to pre-truncate state.
            entry.xet_hash = snapshot.xet_hash.clone();
            entry.size = snapshot.size;
            entry.mtime = snapshot.mtime;
            entry.pending_deletes = snapshot.pending_deletes.clone();
            entry.dirty_generation = 0;
            info!(
                "Reverted ino={}: restored (hash={:?}, size={})",
                ino, snapshot.xet_hash, snapshot.size
            );
        }
    }

    /// Finalize a streaming write: send Finish to the worker, await CAS upload, commit to Hub.
    async fn streaming_commit(&self, ino: u64, channel: &StreamingChannel) -> Result<(), i32> {
        // Unlinked files (nlink=0) must not be re-committed on close —
        // user deleted the file, uploading would resurrect it.
        if self
            .inode_table
            .read()
            .expect("inodes poisoned")
            .get(ino)
            .is_some_and(|e| e.nlink == 0)
        {
            debug!("streaming_commit: skipping unlinked ino={}", ino);
            return Ok(());
        }

        let file_info = {
            let pending = channel.pending_info.lock().expect("pending_info poisoned").take();
            if let Some(info) = pending {
                info
            } else {
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                if channel.tx.send(WriteMsg::Finish(result_tx)).await.is_err() {
                    // Channel closed: only treat as success if already committed.
                    // If the worker died from an error, this is a real failure.
                    let already_committed =
                        matches!(&*channel.state.lock().expect("state poisoned"), CommitState::Committed);
                    if already_committed {
                        debug!("streaming_commit: channel closed but already committed for ino={}", ino);
                        return Ok(());
                    }
                    error!("streaming_commit: channel closed with no prior commit for ino={}", ino);
                    return Err(libc::EIO);
                }
                match result_rx.await {
                    Ok(Ok(info)) => info,
                    Ok(Err(e)) => {
                        error!("Streaming upload failed for ino={}: {}", ino, e);
                        return Err(libc::EIO);
                    }
                    Err(_) => {
                        error!("Streaming worker dropped for ino={}", ino);
                        return Err(libc::EIO);
                    }
                }
            }
        };

        // Commit to Hub
        let (full_path, pending_deletes) = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let entry = inodes.get(ino).ok_or(libc::ENOENT)?;
            (entry.full_path.clone(), entry.pending_deletes.clone())
        };

        let mtime_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut ops: Vec<BatchOp> = vec![BatchOp::AddFile {
            path: full_path.clone(),
            xet_hash: file_info.hash().to_string(),
            mtime: mtime_ms,
            content_type: None,
        }];
        for old_path in &pending_deletes {
            ops.push(BatchOp::DeleteFile { path: old_path.clone() });
        }

        if let Err(e) = self.hub_client.batch_operations(&ops).await {
            error!("Failed to commit file {}: {}", full_path, e);
            // CAS upload succeeded — preserve file_info for retry in release()
            *channel.pending_info.lock().expect("pending_info poisoned") = Some(file_info);
            return Err(libc::EIO);
        }

        let mut inodes = self.inode_table.write().expect("inodes poisoned");
        if let Some(entry) = inodes.get_mut(ino) {
            entry.apply_commit(
                file_info.hash(),
                file_info.file_size(),
                channel.dirty_generation_at_open.load(Ordering::Relaxed),
            );
        }

        info!(
            "Committed file: {} (hash={}, size={})",
            full_path,
            file_info.hash(),
            file_info.file_size(),
        );

        Ok(())
    }

    pub async fn create(
        &self,
        parent: u64,
        name: &str,
        mode: u16,
        caller_uid: u32,
        caller_gid: u32,
        pid: Option<u32>,
    ) -> VirtualFsResult<(VirtualFsAttr, u64)> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if self.filter_os_files && is_os_junk(name) {
            debug!("create: rejecting OS junk file: {}", name);
            return Err(libc::EACCES);
        }

        debug!("create: parent={}, name={}", parent, name);

        // Fetch remote children so we can detect name collisions (also validates parent)
        self.ensure_children_loaded(parent).await?;

        let now = SystemTime::now();
        // Re-check parent + EEXIST + insert under one write lock (TOCTOU guard)
        let (ino, full_path, dirty_gen) = {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            let parent_entry = match inodes.get(parent) {
                Some(e) if e.kind == InodeKind::Directory => e,
                Some(_) => return Err(libc::ENOTDIR),
                None => return Err(libc::ENOENT),
            };
            let full_path = inode::child_path(&parent_entry.full_path, name);
            if inodes.lookup_child(parent, name).is_some() {
                return Err(libc::EEXIST);
            }
            // Insert as dirty — content lives in local staging until flushed
            let ino = inodes.insert(
                parent,
                name.to_string(),
                full_path.clone(),
                InodeKind::File,
                0,
                now,
                None,
                mode,
                caller_uid,
                caller_gid,
            );
            let dirty_gen = if let Some(entry) = inodes.get_mut(ino) {
                entry.set_dirty();
                entry.dirty_generation
            } else {
                0
            };
            inodes.touch_parent(parent, now);
            (ino, full_path, dirty_gen)
        };

        self.negative_cache_remove(&full_path);
        if let Some(fm) = &self.flush_manager {
            fm.cancel_delete(&full_path);
        }

        if self.advanced_writes {
            // Advanced mode: staging file on disk + async flush
            let staging_path = self
                .staging_dir
                .as_ref()
                .expect("staging_dir required for advanced writes")
                .path(ino);
            match OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&staging_path)
            {
                Ok(file) => {
                    let file_handle = self.alloc_file_handle();
                    self.open_files.write().expect("open_files poisoned").insert(
                        file_handle,
                        OpenFile::Local {
                            ino,
                            file: Arc::new(file),
                            writable: true,
                        },
                    );

                    let inodes = self.inode_table.read().expect("inodes poisoned");
                    let attr = self.make_vfs_attr(inodes.get(ino).ok_or(libc::ENOENT)?);
                    Ok((attr, file_handle))
                }
                Err(e) => {
                    error!("Failed to create staging file: {}", e);
                    self.inode_table.write().expect("inodes poisoned").remove(ino);
                    Err(libc::EIO)
                }
            }
        } else {
            // Simple mode: streaming writer with channel-based decoupling.
            let snapshot = InodeSnapshot {
                xet_hash: None,
                size: 0,
                mtime: now,
                pending_deletes: Vec::new(),
                existed_before: false,
            };
            let (file_handle, channel) = match self.setup_streaming_writer(pid, snapshot, dirty_gen).await {
                Ok(r) => r,
                Err(e) => {
                    self.inode_table.write().expect("inodes poisoned").remove(ino);
                    return Err(e);
                }
            };

            self.open_files
                .write()
                .expect("open_files poisoned")
                .insert(file_handle, OpenFile::Streaming { ino, channel });

            let inodes = self.inode_table.read().expect("inodes poisoned");
            let attr = self.make_vfs_attr(inodes.get(ino).ok_or(libc::ENOENT)?);
            Ok((attr, file_handle))
        }
    }

    pub async fn mkdir(
        &self,
        parent: u64,
        name: &str,
        mode: u16,
        caller_uid: u32,
        caller_gid: u32,
    ) -> VirtualFsResult<VirtualFsAttr> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if self.filter_os_files && is_os_junk(name) {
            debug!("mkdir: rejecting OS junk directory: {}", name);
            return Err(libc::EACCES);
        }

        debug!("mkdir: parent={}, name={}", parent, name);

        // Fetch remote children so we can detect name collisions (also validates parent)
        self.ensure_children_loaded(parent).await?;

        let now = SystemTime::now();
        // Re-check parent + EEXIST + insert under one write lock (TOCTOU guard)
        let (ino, full_path) = {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            let parent_entry = match inodes.get(parent) {
                Some(e) if e.kind == InodeKind::Directory => e,
                Some(_) => return Err(libc::ENOTDIR),
                None => return Err(libc::ENOENT),
            };
            let full_path = inode::child_path(&parent_entry.full_path, name);
            if inodes.lookup_child(parent, name).is_some() {
                return Err(libc::EEXIST);
            }
            // New dir starts with children_loaded=true (empty, nothing to fetch)
            let ino = inodes.insert(
                parent,
                name.to_string(),
                full_path.clone(),
                InodeKind::Directory,
                0,
                now,
                None,
                mode,
                caller_uid,
                caller_gid,
            );
            if let Some(entry) = inodes.get_mut(ino) {
                entry.children_loaded = true;
            }
            // nlink already incremented by insert()
            inodes.touch_parent(parent, now);
            (ino, full_path)
        };

        self.negative_cache_remove(&full_path);

        let inodes = self.inode_table.read().expect("inodes poisoned");
        Ok(self.make_vfs_attr(inodes.get(ino).ok_or(libc::ENOENT)?))
    }

    pub async fn unlink(&self, parent: u64, name: &str) -> VirtualFsResult<()> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("unlink: parent={}, name={}", parent, name);

        self.ensure_children_loaded(parent).await?;

        let (ino, full_path, needs_remote_delete) = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let entry = match inodes.lookup_child(parent, name) {
                Some(entry) if entry.kind != InodeKind::Directory => entry,
                Some(_) => return Err(libc::EISDIR),
                None => return Err(libc::ENOENT),
            };
            // Remote delete only when last link is removed and file exists on the hub
            let needs_remote = entry.xet_hash.is_some() && entry.nlink <= 1;
            (entry.inode, entry.full_path.clone(), needs_remote)
        };

        // In streaming mode, block unlink while the file has any open handles.
        // This prevents editors (vim) from deleting a file they can't rewrite
        // (streaming mode returns EPERM for O_RDWR without O_TRUNC), which
        // would cause silent data loss. Same approach as mountpoint-s3.
        if !self.advanced_writes && self.has_open_handles(ino) {
            debug!("unlink: blocked for ino={} (has open handles in streaming mode)", ino);
            return Err(libc::EPERM);
        }

        // Advanced writes: queue delete for batched flush in the flush_loop.
        // Simple mode: delete synchronously (one HTTP call per unlink).
        if needs_remote_delete {
            if let Some(fm) = &self.flush_manager {
                fm.enqueue_delete(full_path.clone());
            } else if let Err(e) = self
                .hub_client
                .batch_operations(&[BatchOp::DeleteFile {
                    path: full_path.clone(),
                }])
                .await
            {
                error!("Remote delete failed for {}: {}", full_path, e);
                return Err(libc::EIO);
            }
        }

        // Remote succeeded (or no remote needed) — now unlink locally.
        // unlink_one decrements nlink; the inode stays in the table with nlink=0
        // so open file handles can still fstat() it.
        let inode_removed = {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            let last_link = inodes
                .unlink_one(parent, name)
                .map(|(removed, _)| removed)
                .unwrap_or(false);
            // Update parent mtime/ctime (POSIX: directory was modified)
            let now = SystemTime::now();
            inodes.touch_parent(parent, now);
            // If last link gone and no open handles, remove the orphan immediately
            if last_link && !self.has_open_handles(ino) {
                inodes.remove_orphan(ino);
            }
            last_link
        };

        // Clean up staging file only if inode was fully removed (no remaining hard links)
        if inode_removed && let Some(ref staging_dir) = self.staging_dir {
            let staging_path = staging_dir.path(ino);
            if let Err(e) = std::fs::remove_file(&staging_path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                warn!("Failed to remove staging file for ino={}: {}", ino, e);
            }
        }

        info!("Deleted file: {}", full_path);
        Ok(())
    }

    pub async fn symlink(
        &self,
        parent: u64,
        name: &str,
        target: &str,
        mode: u16,
        caller_uid: u32,
        caller_gid: u32,
    ) -> VirtualFsResult<VirtualFsAttr> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("symlink: parent={}, name={}, target={}", parent, name, target);

        self.ensure_children_loaded(parent).await?;

        let now = SystemTime::now();
        let (ino, full_path) = {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            let parent_entry = match inodes.get(parent) {
                Some(e) if e.kind == InodeKind::Directory => e,
                Some(_) => return Err(libc::ENOTDIR),
                None => return Err(libc::ENOENT),
            };
            let full_path = inode::child_path(&parent_entry.full_path, name);
            if inodes.lookup_child(parent, name).is_some() {
                return Err(libc::EEXIST);
            }
            let ino = inodes.insert(
                parent,
                name.to_string(),
                full_path.clone(),
                InodeKind::Symlink,
                target.len() as u64,
                now,
                None,
                mode,
                caller_uid,
                caller_gid,
            );
            if let Some(entry) = inodes.get_mut(ino) {
                entry.symlink_target = Some(target.to_string());
            }
            inodes.touch_parent(parent, now);
            (ino, full_path)
        };

        self.negative_cache_remove(&full_path);
        let inodes = self.inode_table.read().expect("inodes poisoned");
        Ok(self.make_vfs_attr(inodes.get(ino).ok_or(libc::ENOENT)?))
    }

    pub fn readlink(&self, ino: u64) -> VirtualFsResult<String> {
        let inodes = self.inode_table.read().expect("inodes poisoned");
        match inodes.get(ino) {
            Some(entry) if entry.kind == InodeKind::Symlink => entry.symlink_target.clone().ok_or(libc::EINVAL),
            Some(_) => Err(libc::EINVAL),
            None => Err(libc::ENOENT),
        }
    }

    pub async fn link(&self, _ino: u64, _new_parent: u64, _new_name: &str) -> VirtualFsResult<VirtualFsAttr> {
        // Hard links are not supported — they are ephemeral (in-memory only) and never
        // persisted to the hub, which makes them a source of subtle bugs with no benefit.
        Err(libc::ENOTSUP)
    }

    pub async fn rmdir(&self, parent: u64, name: &str) -> VirtualFsResult<()> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("rmdir: parent={}, name={}", parent, name);

        // Flush any queued remote deletes (e.g. from rm -rf that unlinked
        // files before calling rmdir on the now-empty directory).
        if let Some(fm) = &self.flush_manager {
            fm.flush_deletes().await;
        }

        self.ensure_children_loaded(parent).await?;

        let ino = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
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

        // Load remote children so we don't miss any before the emptiness check
        self.ensure_children_loaded(ino).await?;

        // Re-check ENOTEMPTY + remove under a single write lock to prevent
        // a concurrent create/mkdir from inserting a child in between.
        let mut inodes = self.inode_table.write().expect("inodes poisoned");
        match inodes.get(ino) {
            Some(entry) if !entry.children.is_empty() => return Err(libc::ENOTEMPTY),
            None => return Err(libc::ENOENT),
            _ => {}
        }
        inodes.remove(ino);
        // nlink adjusted by remove()
        inodes.touch_parent(parent, SystemTime::now());
        Ok(())
    }

    pub async fn rename(
        &self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        no_replace: bool,
    ) -> VirtualFsResult<()> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if self.filter_os_files && is_os_junk(newname) {
            debug!("rename: rejecting rename to OS junk name: {}", newname);
            return Err(libc::EACCES);
        }

        debug!(
            "rename: parent={}, name={}, newparent={}, newname={}",
            parent, name, newparent, newname
        );

        // Noop: renaming to same location
        if parent == newparent && name == newname {
            return Ok(());
        }

        self.ensure_children_loaded(parent).await?;
        if parent != newparent {
            self.ensure_children_loaded(newparent).await?;
        }

        // Pre-load lazy directories before the sync validate phase:
        // - Source subtree: so descendant_files is complete for remote rename ops
        // - Destination dir (if exists): so children.is_empty() check is accurate
        let (src_dir, dst_dir) = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let src = inodes
                .lookup_child(parent, name)
                .filter(|e| e.kind == InodeKind::Directory)
                .map(|e| e.inode);
            let dst = inodes
                .lookup_child(newparent, newname)
                .filter(|e| e.kind == InodeKind::Directory)
                .map(|e| e.inode);
            (src, dst)
        };
        if let Some(src_ino) = src_dir {
            self.ensure_subtree_loaded(src_ino).await?;
        }
        if let Some(dst_ino) = dst_dir {
            self.ensure_children_loaded(dst_ino).await?;
        }

        // Phase 1: validate under read lock, collect everything we need
        let info = self.rename_validate(parent, name, newparent, newname, no_replace)?;

        // POSIX: rename(a, b) where a and b are hard links to the same inode is a no-op.
        // Check before remote sync to avoid spurious backend mutations.
        {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            if inodes
                .lookup_child(newparent, newname)
                .is_some_and(|e| e.inode == info.ino)
            {
                return Ok(());
            }
        }

        // Phase 2: sync to remote (add + delete ops).
        // TOCTOU note: between this remote commit and the local apply below, a
        // concurrent unlink/create can change local state. If that happens,
        // rename_apply_local re-validates and returns ENOENT (source gone) or
        // EEXIST (destination appeared). The remote state may then have a
        // stale add+delete, but the next poll cycle or flush corrects it.
        // A true fix would require a transactional rename API on the Hub.
        self.rename_remote(&info).await?;

        // Phase 3: apply to local inode table under write lock
        self.rename_apply_local(info, parent, name, newparent, newname, no_replace)
    }

    /// Phase 1: validate rename under inode read lock, return all info needed for phases 2+3.
    fn rename_validate(
        &self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        no_replace: bool,
    ) -> VirtualFsResult<RenameInfo> {
        let inodes = self.inode_table.read().expect("inodes poisoned");

        let src = inodes.lookup_child(parent, name).ok_or(libc::ENOENT)?;

        // Destination parent must exist
        let new_parent_entry = inodes.get(newparent).ok_or(libc::ENOENT)?;
        let new_full_path = inode::child_path(&new_parent_entry.full_path, newname);

        // Prevent moving a directory into its own subtree (would create a cycle)
        if src.kind == InodeKind::Directory {
            let mut ancestor = newparent;
            while ancestor != 1 {
                if ancestor == src.inode {
                    return Err(libc::EINVAL);
                }
                ancestor = match inodes.get(ancestor) {
                    Some(e) => e.parent,
                    None => break,
                };
            }
        }

        // Check destination conflicts
        if let Some(existing) = inodes.lookup_child(newparent, newname) {
            if no_replace {
                return Err(libc::EEXIST);
            }
            match (src.kind, existing.kind) {
                (InodeKind::File | InodeKind::Symlink, InodeKind::Directory) => return Err(libc::EISDIR),
                (InodeKind::Directory, InodeKind::File | InodeKind::Symlink) => return Err(libc::ENOTDIR),
                (InodeKind::Directory, InodeKind::Directory) if !existing.children.is_empty() => {
                    return Err(libc::ENOTEMPTY);
                }
                _ => {}
            }
        }

        // For directories, collect all descendant clean files for remote rename
        let descendant_files = if src.kind == InodeKind::Directory {
            let mut files = Vec::new();
            let mut stack = vec![src.inode];
            debug!(
                "rename_validate: dir ino={} children_loaded={} children_count={}",
                src.inode,
                inodes.is_children_loaded(src.inode),
                src.children.len(),
            );
            while let Some(dir_ino) = stack.pop() {
                if let Some(entry) = inodes.get(dir_ino) {
                    for child_ref in &entry.children {
                        if let Some(child) = inodes.get(child_ref.ino) {
                            debug!(
                                "rename_validate: child ino={} path={} dirty={} xet_hash={:?}",
                                child_ref.ino,
                                child.full_path,
                                child.is_dirty(),
                                child.xet_hash.as_deref()
                            );
                            match child.kind {
                                InodeKind::File if !child.is_dirty() && child.xet_hash.is_some() => {
                                    files.push((
                                        child.full_path.clone(),
                                        child.xet_hash.clone().expect("checked is_some above"),
                                    ));
                                }
                                InodeKind::Directory => stack.push(child_ref.ino),
                                _ => {}
                            }
                        }
                    }
                }
            }
            files
        } else {
            Vec::new()
        };

        Ok(RenameInfo {
            ino: src.inode,
            old_path: src.full_path.clone(),
            kind: src.kind,
            xet_hash: src.xet_hash.clone(),
            is_dirty: src.is_dirty(),
            new_full_path,
            descendant_files,
        })
    }

    /// Phase 2: send batch rename ops to the Hub (add new paths + delete old ones).
    async fn rename_remote(&self, info: &RenameInfo) -> VirtualFsResult<()> {
        let mtime_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let ops: Vec<BatchOp> = if info.kind == InodeKind::File
            && !info.is_dirty
            && let Some(ref hash) = info.xet_hash
        {
            vec![
                BatchOp::AddFile {
                    path: info.new_full_path.clone(),
                    xet_hash: hash.clone(),
                    mtime: mtime_ms,
                    content_type: None,
                },
                BatchOp::DeleteFile {
                    path: info.old_path.clone(),
                },
            ]
        } else if info.kind == InodeKind::Directory && !info.descendant_files.is_empty() {
            // Hub batch API requires all adds before all deletes
            let mut ops = Vec::with_capacity(info.descendant_files.len() * 2);
            let mut deletes = Vec::with_capacity(info.descendant_files.len());
            for (child_old_path, child_hash) in &info.descendant_files {
                let suffix = child_old_path.strip_prefix(&info.old_path).unwrap_or(child_old_path);
                let child_new_path = format!("{}{}", info.new_full_path, suffix);
                ops.push(BatchOp::AddFile {
                    path: child_new_path,
                    xet_hash: child_hash.clone(),
                    mtime: mtime_ms,
                    content_type: None,
                });
                deletes.push(BatchOp::DeleteFile {
                    path: child_old_path.clone(),
                });
            }
            ops.append(&mut deletes);
            ops
        } else {
            return Ok(());
        };

        debug!(
            "rename_remote: {} -> {} ops={}",
            info.old_path,
            info.new_full_path,
            ops.len()
        );
        if let Err(e) = self.hub_client.batch_operations(&ops).await {
            error!("Failed to rename {} -> {}: {}", info.old_path, info.new_full_path, e);
            return Err(libc::EIO);
        }
        debug!("rename_remote: success");
        Ok(())
    }

    /// Phase 3: apply rename to local inode table under write lock.
    fn rename_apply_local(
        &self,
        info: RenameInfo,
        parent: u64,
        oldname: &str,
        newparent: u64,
        newname: &str,
        no_replace: bool,
    ) -> VirtualFsResult<()> {
        self.negative_cache_remove(&info.new_full_path);
        // Cancel any queued remote delete for the destination path (e.g. rm a && mv b a).
        // For directories, also cancel descendant deletes (e.g. rm -rf dir && mv newdir dir).
        if let Some(fm) = &self.flush_manager {
            fm.cancel_delete(&info.new_full_path);
            if info.kind == InodeKind::Directory {
                fm.cancel_delete_prefix(&format!("{}/", info.new_full_path));
            }
        }

        let mut inodes = self.inode_table.write().expect("inodes poisoned");

        // Re-validate source still exists (could have been unlinked concurrently)
        if inodes.get(info.ino).is_none() {
            return Err(libc::ENOENT);
        }

        // Re-check destination under write lock (a concurrent create could have
        // inserted one between phase 1 read-lock and this write-lock).
        let replace_target = if let Some(existing) = inodes.lookup_child(newparent, newname) {
            // POSIX: rename(a, b) where a and b are hard links to the same inode is a no-op
            if existing.inode == info.ino {
                return Ok(());
            }
            if no_replace {
                return Err(libc::EEXIST);
            }
            match (info.kind, existing.kind) {
                // POSIX: non-directory cannot replace directory
                (InodeKind::File | InodeKind::Symlink, InodeKind::Directory) => return Err(libc::EISDIR),
                // POSIX: directory cannot replace non-directory
                (InodeKind::Directory, InodeKind::File | InodeKind::Symlink) => return Err(libc::ENOTDIR),
                (InodeKind::Directory, InodeKind::Directory) if !existing.children.is_empty() => {
                    return Err(libc::ENOTEMPTY);
                }
                _ => {}
            }
            Some((existing.inode, existing.kind))
        } else {
            None
        };
        if let Some((existing_ino, existing_kind)) = replace_target {
            if existing_kind == InodeKind::Directory {
                // Directories can't be hard-linked, so remove() is correct
                // (remove() adjusts parent nlink for directories)
                inodes.remove(existing_ino);
            } else {
                inodes.unlink_one(newparent, newname);
                if !self.has_open_handles(existing_ino) {
                    inodes.remove_orphan(existing_ino);
                }
            }
        }

        // Dirty file with a remote presence: record old path for deletion at flush time.
        if info.is_dirty
            && info.kind == InodeKind::File
            && info.xet_hash.is_some()
            && let Some(entry) = inodes.get_mut(info.ino)
        {
            entry.pending_deletes.push(info.old_path.clone());
        }

        // Dirty descendants of a renamed directory: record their old remote paths
        // for deletion at flush time (clean descendants are handled in rename_remote).
        if info.kind == InodeKind::Directory {
            let mut stack = vec![info.ino];
            while let Some(dir_ino) = stack.pop() {
                let children: Vec<inode::DirChild> =
                    inodes.get(dir_ino).map(|e| e.children.clone()).unwrap_or_default();
                for child_ref in children {
                    if let Some(child) = inodes.get(child_ref.ino) {
                        match child.kind {
                            InodeKind::File if child.is_dirty() && child.xet_hash.is_some() => {
                                let old_path = child.full_path.clone();
                                if let Some(child_mut) = inodes.get_mut(child_ref.ino) {
                                    child_mut.pending_deletes.push(old_path);
                                }
                            }
                            InodeKind::Directory => stack.push(child_ref.ino),
                            _ => {}
                        }
                    }
                }
            }
        }

        // Remove old path mapping for the renamed entry
        inodes.remove_path(&info.old_path);

        inodes.move_child(info.ino, parent, oldname, newparent, newname);
        inodes.update_subtree_paths(info.ino, info.new_full_path);

        // Update parent mtime/ctime for both old and new parents (POSIX: directories modified)
        let now = SystemTime::now();
        inodes.touch_parent(parent, now);
        if parent != newparent {
            inodes.touch_parent(newparent, now);
        }
        // Update source ctime (POSIX: inode metadata changed)
        if let Some(e) = inodes.get_mut(info.ino) {
            e.ctime = now;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn setattr(
        &self,
        ino: u64,
        size: Option<u64>,
        mode: Option<u16>,
        uid: Option<u32>,
        gid: Option<u32>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> VirtualFsResult<VirtualFsAttr> {
        debug!("setattr: ino={}, size={:?}", ino, size);

        // EROFS check blocks all changes on RO mounts
        if self.read_only {
            return Err(libc::EROFS);
        }

        if let Some(new_size) = size {
            // Validate inode exists and is a file before any side effects
            {
                let inodes = self.inode_table.read().expect("inodes poisoned");
                match inodes.get(ino) {
                    Some(e) if e.kind != InodeKind::File => return Err(libc::EISDIR),
                    None => return Err(libc::ENOENT),
                    _ => {}
                }
            }

            if !self.advanced_writes {
                // Simple mode: ftruncate via setattr is silently ignored.
                // Real truncation goes through open(O_TRUNC) which is handled separately.
            } else {
                // Advanced mode: truncation is applied to the staging file on disk
                let staging_mutex = self.staging_lock(ino);
                let _staging_guard = staging_mutex.lock().await;

                let staging_path = self
                    .staging_dir
                    .as_ref()
                    .expect("staging_dir required for advanced writes")
                    .path(ino);

                if new_size == 0 {
                    if let Err(e) = File::create(&staging_path) {
                        error!("Failed to truncate staging file: {}", e);
                        return Err(libc::EIO);
                    }
                } else {
                    // If staging file doesn't exist, download from remote first
                    if !staging_path.exists() {
                        let (xet_hash, file_size) = {
                            let inodes = self.inode_table.read().expect("inodes poisoned");
                            let entry = inodes.get(ino).ok_or(libc::ENOENT)?;
                            (entry.xet_hash.clone().unwrap_or_default(), entry.size)
                        };
                        if !xet_hash.is_empty() && file_size > 0 {
                            if let Err(e) = self
                                .xet_sessions
                                .download_to_file(&xet_hash, file_size, &staging_path)
                                .await
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

                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                if let Some(entry) = inodes.get_mut(ino) {
                    entry.size = new_size;
                    entry.mtime = SystemTime::now();
                    entry.ctime = entry.mtime;
                    entry.set_dirty();
                    if new_size == 0 {
                        entry.xet_hash = None;
                    }
                }

                // Schedule flush so the truncation is committed to CAS/bucket
                drop(inodes);
                if let Some(fm) = &self.flush_manager {
                    fm.enqueue(ino);
                }
            }
        }

        // Apply metadata-only changes (mode, uid, gid, atime, mtime)
        if mode.is_some() || uid.is_some() || gid.is_some() || atime.is_some() || mtime.is_some() {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            if let Some(entry) = inodes.get_mut(ino) {
                if let Some(m) = mode {
                    entry.mode = m;
                }
                if let Some(u) = uid {
                    entry.uid = u;
                }
                if let Some(g) = gid {
                    entry.gid = g;
                }
                if let Some(a) = atime {
                    entry.atime = a;
                }
                if let Some(m) = mtime {
                    entry.mtime = m;
                }
                entry.ctime = SystemTime::now();
            }
        }

        let inodes = self.inode_table.read().expect("inodes poisoned");
        match inodes.get(ino) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => Err(libc::ENOENT),
        }
    }

    /// Read a file inode's fields into a `FileEntry` snapshot.
    fn get_file_entry(&self, ino: u64) -> VirtualFsResult<FileEntry> {
        let inodes = self.inode_table.read().expect("inodes poisoned");
        let entry = match inodes.get(ino) {
            Some(e) if e.kind == InodeKind::File => e,
            _ => return Err(libc::ENOENT),
        };
        Ok(FileEntry {
            xet_hash: entry.xet_hash.clone().unwrap_or_default(),
            size: entry.size,
            is_dirty: entry.is_dirty(),
            full_path: entry.full_path.clone(),
        })
    }
}

// ── VFS types ──────────────────────────────────────────────────────────
pub type VirtualFsError = i32;
pub type VirtualFsResult<T> = std::result::Result<T, VirtualFsError>;

#[derive(Debug)]
pub struct VirtualFsAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub mtime: SystemTime,
    pub atime: SystemTime,
    pub ctime: SystemTime,
    pub kind: InodeKind,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug)]
pub struct VirtualFsDirEntry {
    pub ino: u64,
    pub kind: InodeKind,
    pub name: String,
}

/// Snapshot of rename state collected under read lock (phase 1),
/// consumed by remote sync (phase 2) and local apply (phase 3).
struct RenameInfo {
    ino: u64,
    old_path: String,
    kind: InodeKind,
    xet_hash: Option<String>,
    is_dirty: bool,
    new_full_path: String,
    /// Descendant clean files to rename on the remote (dir renames only).
    descendant_files: Vec<(String, String)>,
}

// ── Internal types ─────────────────────────────────────────────────────

/// Snapshot of file inode fields captured under a short-lived read lock.
struct FileEntry {
    xet_hash: String,
    size: u64,
    is_dirty: bool,
    full_path: String,
}

/// State machine for streaming writes.
/// Message sent from the write() caller to the background streaming worker.
enum WriteMsg {
    Data(Vec<u8>),
    Finish(tokio::sync::oneshot::Sender<crate::error::Result<XetFileInfo>>),
}

/// Snapshot of inode state captured when a streaming writer is opened.
/// Used to revert the inode on commit failure (data loss recovery).
struct InodeSnapshot {
    xet_hash: Option<String>,
    size: u64,
    mtime: SystemTime,
    pending_deletes: Vec<String>,
    /// false for create() (new file), true for open(O_TRUNC) (overwrite).
    existed_before: bool,
}

/// Lifecycle state of a streaming write channel's commit.
enum CommitState {
    /// Streaming writes in progress. Worker is running.
    Writing,
    /// flush() deferred commit (dup'd fd or zero writes). release() will handle it.
    Deferred,
    /// Commit completed successfully.
    Committed,
    /// Unrecoverable error — inode has been reverted.
    Failed(String),
}

/// Channel-based streaming handle. Decouples the sync write() caller from the
/// async add_data() pipeline: writes enqueue data into a bounded channel
/// (avoids deadlock when tokio worker threads are saturated by FUSE block_on calls),
/// a background tokio task drains it and feeds the CAS cleaner.
struct StreamingChannel {
    tx: tokio::sync::mpsc::Sender<WriteMsg>,
    bytes_written: AtomicU64,
    /// Set by the background worker if add_data() fails. Shared with worker via Arc.
    error: Arc<std::sync::Mutex<Option<String>>>,
    /// Commit lifecycle state machine.
    state: std::sync::Mutex<CommitState>,
    /// CAS upload succeeded but Hub commit failed — stored for retry.
    pending_info: std::sync::Mutex<Option<XetFileInfo>>,
    /// PID of the process that opened this file (for dup'd fd detection).
    open_pid: Option<u32>,
    /// Pre-write inode snapshot for revert on commit failure.
    snapshot: InodeSnapshot,
    /// Dirty generation at open time, used by streaming_commit to avoid
    /// clobbering concurrent writers via clear_dirty_if. AtomicU64 so it
    /// can be updated after Arc construction (set after inode mutation).
    dirty_generation_at_open: AtomicU64,
    /// Watch sender for the pending commit hook. Created in flush() on deferral,
    /// fulfilled in release() when the commit completes (or fails).
    commit_hook: std::sync::Mutex<Option<CommitHookTx>>,
}

/// An open file handle — either a local fd, lazy remote reference, or streaming writer.
enum OpenFile {
    /// Local file (staging for writes, or dirty reads).
    Local { ino: u64, file: Arc<File>, writable: bool },
    /// Lazy remote — data fetched on-demand with adaptive prefetch buffer.
    Lazy {
        ino: u64,
        prefetch: Arc<tokio::sync::Mutex<PrefetchState>>,
    },
    /// Streaming append-only writer (default write mode).
    Streaming { ino: u64, channel: Arc<StreamingChannel> },
}

/// Check whether two PIDs belong to the same process.
/// On Linux, compares thread-group IDs via /proc to handle PID namespaces.
/// On macOS (no /proc), falls back to direct PID comparison.
fn same_process(pid_a: u32, pid_b: u32) -> bool {
    if pid_a == pid_b {
        return true;
    }

    #[cfg(target_os = "linux")]
    {
        fn read_tgid(pid: u32) -> Option<u32> {
            let path = format!("/proc/{}/status", pid);
            let status = std::fs::read_to_string(path).ok()?;
            for line in status.lines() {
                if let Some(val) = line.strip_prefix("Tgid:\t") {
                    return val.trim().parse().ok();
                }
            }
            None
        }
        match (read_tgid(pid_a), read_tgid(pid_b)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Background task: drains the write channel and feeds data into the CAS streaming writer.
/// Runs until a Finish message is received or the channel is dropped.
async fn streaming_worker(
    mut writer: Box<dyn StreamingWriterOps>,
    mut rx: tokio::sync::mpsc::Receiver<WriteMsg>,
    error: Arc<std::sync::Mutex<Option<String>>>,
) {
    let mut failed = false;
    while let Some(msg) = rx.recv().await {
        match msg {
            WriteMsg::Data(data) => {
                if failed {
                    continue; // drain remaining messages
                }
                if let Err(e) = writer.write(&data).await {
                    *error.lock().unwrap() = Some(e.to_string());
                    failed = true;
                }
            }
            WriteMsg::Finish(reply) => {
                let result = if failed {
                    Err(crate::error::Error::hub("streaming write failed"))
                } else {
                    writer.finish_boxed().await
                };
                let _ = reply.send(result);
                return;
            }
        }
    }
    // Channel closed without Finish → data is lost (same as close-without-flush)
}

/// What to do in read() after releasing the open_files lock.
enum ReadTarget {
    /// Hold an Arc<File> so the FD stays alive even if release() runs concurrently.
    LocalFd(Arc<File>),
    Remote {
        prefetch: Arc<tokio::sync::Mutex<PrefetchState>>,
    },
}

#[cfg(test)]
mod tests;
