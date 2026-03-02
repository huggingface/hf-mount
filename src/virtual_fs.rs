use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use data::XetFileInfo;
use tracing::{debug, error, info, warn};

use crate::hub_api::{BatchOp, HubApiClient};
use crate::inode::{InodeEntry, InodeKind, InodeTable};
use crate::prefetch::PrefetchState;
use crate::staging::FileStaging;

// ── Constants ──────────────────────────────────────────────────────────

/// Block size reported in stat(2) for `st_blocks` calculation.
const BLOCK_SIZE: u32 = 512;
/// Maximum entries in the negative-lookup cache (parent_path/name → Instant).
/// Prevents repeated Hub API calls for paths known to not exist.
const NEG_CACHE_CAPACITY: usize = 10_000;
/// How long a negative-cache entry stays valid before being re-checked.
const NEG_CACHE_TTL: Duration = Duration::from_secs(30);

// ── VirtualFs ──────────────────────────────────────────────────────────

pub struct VirtualFs {
    runtime: tokio::runtime::Handle,
    hub_client: Arc<HubApiClient>,
    file_staging: Arc<FileStaging>,
    read_only: bool,
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
    /// Debounced batch flush pipeline (None when read_only).
    flush_manager: Option<crate::flush::FlushManager>,
    /// Background poll task handle, taken in shutdown().
    poll_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Kernel cache invalidation callback. Set via `set_invalidator()` after mount.
    /// The poll loop calls this to actively invalidate stale inodes when remote changes
    /// are detected, allowing the kernel page cache to be used instead of DIRECT_IO.
    invalidator: Arc<Mutex<Option<Box<dyn Fn(u64) + Send + Sync>>>>,
}

impl VirtualFs {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: tokio::runtime::Handle,
        hub_client: Arc<HubApiClient>,
        file_staging: Arc<FileStaging>,
        read_only: bool,
        uid: u32,
        gid: u32,
        poll_interval_secs: u64,
    ) -> Arc<Self> {
        let inodes = Arc::new(RwLock::new(InodeTable::new()));
        let negative_cache = Arc::new(RwLock::new(HashMap::new()));

        let flush_manager = if !read_only {
            Some(crate::flush::FlushManager::new(
                file_staging.clone(),
                hub_client.clone(),
                inodes.clone(),
                &runtime,
            ))
        } else {
            None
        };

        // Spawn remote change polling task (if interval > 0)
        let invalidator: Arc<Mutex<Option<Box<dyn Fn(u64) + Send + Sync>>>> = Arc::new(Mutex::new(None));
        let poll_handle = if poll_interval_secs > 0 {
            let bg_hub = hub_client.clone();
            let bg_inodes = inodes.clone();
            let bg_staging = file_staging.clone();
            let bg_neg_cache = negative_cache.clone();
            let bg_invalidator = invalidator.clone();
            let interval = Duration::from_secs(poll_interval_secs);

            Some(runtime.spawn(Self::poll_remote_changes(
                bg_hub,
                bg_inodes,
                bg_staging,
                bg_neg_cache,
                bg_invalidator,
                interval,
            )))
        } else {
            None
        };

        Arc::new(Self {
            runtime,
            hub_client,
            file_staging,
            read_only,
            inode_table: inodes,
            open_files: RwLock::new(HashMap::new()),
            next_file_handle: AtomicU64::new(1),
            uid,
            gid,
            negative_cache,
            staging_locks: Mutex::new(HashMap::new()),
            flush_manager,
            poll_handle: Mutex::new(poll_handle),
            invalidator,
        })
    }

    /// Set the kernel cache invalidation callback. Called after mount setup
    /// so the poll loop can actively invalidate stale inodes on remote changes.
    pub fn set_invalidator(&self, f: Box<dyn Fn(u64) + Send + Sync>) {
        *self.invalidator.lock().expect("invalidator poisoned") = Some(f);
    }

    /// Graceful shutdown: abort polling, drain flush queue, wait for completion.
    pub fn shutdown(&self) {
        info!("Shutting down VFS, flushing pending writes...");
        // Abort the polling task.
        if let Some(handle) = self.poll_handle.lock().expect("poll_handle poisoned").take() {
            handle.abort();
        }
        // Flush all dirty files that may not have received a release() yet.
        if let Some(fm) = &self.flush_manager {
            let dirty = self.inode_table.read().expect("inodes poisoned").dirty_inos();
            fm.shutdown(dirty, &self.runtime);
        }
        info!("Flush loop finished, VFS shut down.");
    }

    // ── Background tasks ───────────────────────────────────────────────

    /// Background task: polls Hub API tree listing to detect remote changes.
    async fn poll_remote_changes(
        hub_client: Arc<HubApiClient>,
        inodes: Arc<RwLock<InodeTable>>,
        staging: Arc<FileStaging>,
        negative_cache: Arc<RwLock<HashMap<String, Instant>>>,
        invalidator: Arc<Mutex<Option<Box<dyn Fn(u64) + Send + Sync>>>>,
        interval: Duration,
    ) {
        loop {
            tokio::time::sleep(interval).await;

            let remote_entries = match hub_client.list_tree("").await {
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
            let snapshot = inodes.read().expect("inodes poisoned").file_snapshot();

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
                // Skip locally-modified files: local writes take precedence until flushed.
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

            // Phase 2: Apply mutations under lock, collect inodes to invalidate
            let mut inos_to_invalidate: Vec<u64> = Vec::new();
            let dirs_to_invalidate_kernel: Vec<u64>;
            {
                let mut inode_table = inodes.write().expect("inodes poisoned");

                for update in &updates {
                    inode_table.update_remote_file(update.ino, update.hash.clone(), update.size, update.mtime);
                    inos_to_invalidate.push(update.ino);
                }

                for ino in &deletions {
                    inode_table.remove(*ino);
                }

                // Phase 3: New remote files → invalidate parent dir + negative cache
                // New remote files: instead of creating inodes directly, invalidate the
                // nearest known ancestor directory so the next readdir/lookup discovers them.
                // We walk up the path because intermediate dirs may not exist locally either
                // (e.g. remote has "a/b/c/file.txt" but we only know "a/" → invalidate "a/").
                let mut dirs_to_invalidate = HashSet::new();
                let mut dir_paths_to_invalidate = Vec::new();
                for path in remote_map.keys() {
                    if inode_table.get_by_path(path).is_none() {
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

                // Clear cached children so next readdir re-fetches from Hub API,
                // then invalidate kernel page cache (done outside lock).
                dirs_to_invalidate_kernel = dirs_to_invalidate.into_iter().collect();
                for dir_ino in &dirs_to_invalidate_kernel {
                    inode_table.invalidate_children(*dir_ino);
                }

                // Invalidate negative cache entries under changed directories
                if !dir_paths_to_invalidate.is_empty() {
                    let mut nc = negative_cache.write().expect("neg_cache poisoned");
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

            // Phase 4: Invalidate kernel page cache (outside lock scope)
            if let Some(invalidate) = invalidator.lock().expect("invalidator poisoned").as_ref() {
                for ino in &inos_to_invalidate {
                    invalidate(*ino);
                }
                for dir_ino in &dirs_to_invalidate_kernel {
                    invalidate(*dir_ino);
                }
            }

            // Clean up staging files for deleted inodes (outside lock scope)
            for ino in deletions {
                let staging_path = staging.staging_path(ino);
                if staging_path.exists() {
                    std::fs::remove_file(&staging_path).ok();
                }
            }
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn make_vfs_attr(&self, entry: &InodeEntry) -> VirtualFsAttr {
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

        VirtualFsAttr {
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
    /// Fetch remote children for `parent_ino` if not already loaded.
    /// Returns ENOENT if the inode doesn't exist, ENOTDIR if it's not a directory.
    async fn ensure_children_loaded(&self, parent_ino: u64) -> VirtualFsResult<()> {
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
        // Re-check: another task may have loaded children while we were fetching.
        match inodes.get(parent_ino) {
            Some(e) if e.children_loaded => return Ok(()),
            Some(e) if e.kind != InodeKind::Directory => return Err(libc::ENOTDIR),
            None => return Err(libc::ENOENT),
            _ => {}
        }
        let mut seen_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();

        for entry in entries {
            // Convert absolute bucket path to path relative to current directory.
            // e.g. prefix="models" → "models/bert/config.json" → "bert/config.json"
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
        Ok(())
    }

    pub fn alloc_file_handle(&self) -> u64 {
        self.next_file_handle.fetch_add(1, Ordering::Relaxed)
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
    fn negative_cache_insert(&self, path: String) {
        let mut cache = self.negative_cache.write().expect("neg_cache poisoned");
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

    // ── VFS operations ─────────────────────────────────────────────────

    pub async fn lookup(&self, parent: u64, name: &str) -> VirtualFsResult<VirtualFsAttr> {
        debug!("lookup: parent={}, name={}", parent, name);

        // Fast path: children already loaded → lookup directly, no allocation needed.
        {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let parent_entry = inodes.get(parent).ok_or(libc::ENOENT)?;

            if parent_entry.kind != InodeKind::Directory {
                return Err(libc::ENOTDIR);
            }

            if parent_entry.children_loaded {
                if let Some(entry) = inodes.lookup_child(parent, name) {
                    return Ok(self.make_vfs_attr(entry));
                }
                // Children loaded but child absent → cache miss, return ENOENT.
                // Clone path and drop inode lock before taking neg cache write lock.
                let parent_path = parent_entry.full_path.clone();
                drop(inodes);

                let full_path = if parent_path.is_empty() {
                    name.to_string()
                } else {
                    format!("{}/{}", parent_path, name)
                };
                self.negative_cache_insert(full_path);
                return Err(libc::ENOENT);
            }
        }

        // Slow path: children not loaded yet, fetch from Hub API.
        let full_path = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            match inodes.get(parent) {
                Some(e) if e.full_path.is_empty() => name.to_string(),
                Some(e) => format!("{}/{}", e.full_path, name),
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

        for &child_ino in &entry.children {
            if let Some(child) = inodes.get(child_ino) {
                entries.push(VirtualFsDirEntry {
                    ino: child.inode,
                    kind: child.kind,
                    name: child.name.clone(),
                });
            }
        }

        Ok(entries)
    }

    pub async fn open(&self, ino: u64, writable: bool, truncate: bool) -> VirtualFsResult<(u64, bool)> {
        debug!("open: ino={}, writable={}, truncate={}", ino, writable, truncate);

        if writable && self.read_only {
            return Err(libc::EROFS);
        }

        let (xet_hash, size, is_dirty) = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let entry = match inodes.get(ino) {
                Some(e) if e.kind == InodeKind::File => e,
                _ => return Err(libc::ENOENT),
            };

            (entry.xet_hash.clone().unwrap_or_default(), entry.size, entry.dirty)
        };

        let staging_path = self.file_staging.staging_path(ino);

        if writable {
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
                .dirty;

            // Reuse existing dirty staging file (unless truncating)
            if !(is_dirty && staging_path.exists() && !truncate) {
                if !truncate && !xet_hash.is_empty() && size > 0 {
                    // Download remote content for read-modify-write
                    self.file_staging
                        .download_to_file(&xet_hash, size, &staging_path)
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
                entry.dirty = true;
                if truncate {
                    entry.size = 0;
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
            Ok((file_handle, false))
        } else {
            // Dirty file not yet flushed → read from local staging
            if is_dirty && staging_path.exists() {
                let file_handle = self.open_local_readonly(ino, &staging_path)?;
                return Ok((file_handle, false));
            }

            // Remote file with xet hash → lazy range reads via prefetch buffer
            if !xet_hash.is_empty() {
                let prefetch = Arc::new(tokio::sync::Mutex::new(PrefetchState::new(xet_hash, size)));
                let file_handle = self.alloc_file_handle();
                self.open_files
                    .write()
                    .expect("open_files poisoned")
                    .insert(file_handle, OpenFile::Lazy { prefetch });
                // keep_cache=true: kernel page cache preserved across re-opens,
                // the poll loop calls notify_inval_inode if the remote file changes.
                return Ok((file_handle, true));
            }

            // No hash + non-empty → inconsistent state
            if size > 0 {
                error!("No xet hash for non-empty, non-dirty file ino={}", ino);
                return Err(libc::EIO);
            }

            // Empty file (size=0, no hash) → temp empty file on disk
            if !staging_path.exists() {
                File::create(&staging_path).ok();
            }
            let file_handle = self.open_local_readonly(ino, &staging_path)?;
            Ok((file_handle, false))
        }
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
                None => return Err(libc::EBADF),
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

                // Try forward buffer (zero-copy when read fits in one chunk)
                if let Some(data) = prefetch_state.try_serve_forward(offset, size) {
                    debug!("prefetch hit (forward): offset={}, len={}", offset, data.len());
                    let eof = offset + data.len() as u64 >= file_size;
                    return Ok((data, eof));
                }

                // Try seek window
                if let Some(data) = prefetch_state.try_serve_seek(offset, size) {
                    debug!("prefetch hit (seek): offset={}, len={}", offset, data.len());
                    let eof = offset + data.len() as u64 >= file_size;
                    return Ok((data, eof));
                }

                // Cache miss — drain, classify access pattern, adjust window
                let plan = prefetch_state.prepare_fetch(offset, size);

                // Start a new stream if this is the first sequential read from offset 0
                if matches!(plan.strategy, crate::prefetch::FetchStrategy::StartStream) {
                    let xet_file_info = XetFileInfo::new(prefetch_state.xet_hash.clone(), prefetch_state.file_size);
                    match self
                        .file_staging
                        .download_session()
                        .download_stream(&xet_file_info, None)
                    {
                        Ok(stream) => {
                            debug!("prefetch: started full-file stream");
                            prefetch_state.stream = Some(stream);
                        }
                        Err(e) => {
                            warn!("prefetch: stream start failed: {}", e);
                        }
                    }
                }

                // Try reading from the stream for sequential access
                let new_chunks = if plan.strategy.is_stream() {
                    if let Some(mut stream) = prefetch_state.stream.take() {
                        let mut chunks = VecDeque::new();
                        let mut total = 0usize;
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
                                    error!("prefetch: stream error: {}", e);
                                    break;
                                }
                            }
                        }
                        if !stream_eof {
                            prefetch_state.stream = Some(stream);
                        }
                        if total > 0 {
                            debug!(
                                "prefetch stream: offset={}, got={}, window={}",
                                offset, total, prefetch_state.window_size
                            );
                            Some((chunks, total))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Fall back to range download if streaming wasn't used
                let (chunks, total) = if let Some(fetched) = new_chunks {
                    fetched
                } else {
                    let fetch_end = offset + plan.fetch_size;
                    let file_info = XetFileInfo::new(prefetch_state.xet_hash.clone(), prefetch_state.file_size);
                    let buf = Arc::new(Mutex::new(Vec::with_capacity(plan.fetch_size as usize)));
                    let writer = SharedBufWriter(buf.clone());

                    debug!(
                        "prefetch fetch: offset={}, size={}, window={}",
                        offset, plan.fetch_size, prefetch_state.window_size
                    );

                    match self
                        .file_staging
                        .download_session()
                        .download_to_writer(&file_info, offset..fetch_end, writer, None)
                        .await
                    {
                        Ok(_) => {
                            let vec = match Arc::try_unwrap(buf) {
                                Ok(mutex) => mutex.into_inner().unwrap_or_default(),
                                Err(arc) => arc.lock().unwrap_or_else(|e| e.into_inner()).clone(),
                            };
                            let len = vec.len();
                            let chunk = Bytes::from(vec);
                            (VecDeque::from([chunk]), len)
                        }
                        Err(e) => {
                            error!("Prefetch range read failed: {}", e);
                            return Err(libc::EIO);
                        }
                    }
                };

                // Store fetched chunks and serve the read
                prefetch_state.store_fetched(offset, chunks, total);

                // Serve the read from the freshly filled buffer
                let result = prefetch_state.try_serve_forward(offset, size).unwrap_or_default();
                let eof = offset + result.len() as u64 >= file_size;
                Ok((result, eof))
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

        // Clone Arc<File> + resolve the authoritative ino from the handle.
        let (file, handle_ino) = {
            let files = self.open_files.read().expect("open_files poisoned");
            match files.get(&file_handle) {
                Some(OpenFile::Local {
                    ino,
                    file,
                    writable: true,
                }) => (file.clone(), *ino),
                _ => return Err(libc::EBADF),
            }
        };
        let fd = file.as_raw_fd();

        let n = unsafe { libc::pwrite(fd, data.as_ptr() as *const libc::c_void, data.len(), offset as i64) };

        if n < 0 {
            Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO))
        } else {
            let written = n as u32;
            let new_end = offset + written as u64;
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            if let Some(entry) = inodes.get_mut(handle_ino)
                && new_end > entry.size
            {
                entry.size = new_end;
            }
            Ok(written)
        }
    }

    pub fn flush(&self, ino: u64) -> VirtualFsResult<()> {
        debug!("flush: ino={}", ino);
        // Check if a previous async flush failed for this inode
        if let Some(fm) = &self.flush_manager
            && let Some(err_msg) = fm.check_error(ino)
        {
            error!("Deferred flush error for ino={}: {}", ino, err_msg);
            return Err(libc::EIO);
        }
        Ok(())
    }

    pub fn release(&self, file_handle: u64) {
        debug!("release: fh={}", file_handle);

        let removed = self
            .open_files
            .write()
            .expect("open_files poisoned")
            .remove(&file_handle);

        if let Some(OpenFile::Local {
            ino, writable: true, ..
        }) = removed
        {
            if let Some(fm) = &self.flush_manager {
                fm.enqueue(ino);
            }
        }
        // staging_locks entries are intentionally not cleaned up here: removing
        // while another open() may hold the Arc would break serialization.
        // Entries are tiny (Arc<Mutex<()>>) and bounded by unique inodes opened.
    }

    pub async fn create(&self, parent: u64, name: &str) -> VirtualFsResult<(VirtualFsAttr, u64)> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("create: parent={}, name={}", parent, name);

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
            let full_path = if parent_entry.full_path.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", parent_entry.full_path, name)
            };
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
            );
            if let Some(entry) = inodes.get_mut(ino) {
                entry.dirty = true;
            }
            (ino, full_path)
        };

        self.negative_cache_remove(&full_path);

        // Create an empty staging file and open it for read+write
        let staging_path = self.file_staging.staging_path(ino);
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
                let attr = self.make_vfs_attr(inodes.get(ino).unwrap());
                Ok((attr, file_handle))
            }
            Err(e) => {
                error!("Failed to create staging file: {}", e);
                // Staging failed — rollback the inode to avoid a ghost entry
                self.inode_table.write().expect("inodes poisoned").remove(ino);
                Err(libc::EIO)
            }
        }
    }

    pub async fn mkdir(&self, parent: u64, name: &str) -> VirtualFsResult<VirtualFsAttr> {
        if self.read_only {
            return Err(libc::EROFS);
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
            let full_path = if parent_entry.full_path.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", parent_entry.full_path, name)
            };
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
            );
            if let Some(entry) = inodes.get_mut(ino) {
                entry.children_loaded = true;
            }
            (ino, full_path)
        };

        self.negative_cache_remove(&full_path);

        let inodes = self.inode_table.read().expect("inodes poisoned");
        Ok(self.make_vfs_attr(inodes.get(ino).unwrap()))
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
                Some(entry) if entry.kind == InodeKind::File => entry,
                Some(_) => return Err(libc::EISDIR),
                None => return Err(libc::ENOENT),
            };
            // Remote delete needed if the file exists on the hub (has a xet_hash)
            let needs_remote = entry.xet_hash.is_some();
            (entry.inode, entry.full_path.clone(), needs_remote)
        };

        // Remove inode + staging BEFORE the remote call so a concurrent
        // flush_batch sees the inode as gone and skips it.
        self.inode_table.write().expect("inodes poisoned").remove(ino);
        let staging_path = self.file_staging.staging_path(ino);
        std::fs::remove_file(&staging_path).ok();

        // Delete from remote. Inode is already gone locally — if this fails,
        // EIO is returned but no local rollback: the next poll will re-materialize
        // the file from the remote source of truth.
        if needs_remote_delete {
            if let Err(e) = self
                .hub_client
                .batch_operations(&[BatchOp::DeleteFile { path: full_path.clone() }])
                .await
            {
                error!("Remote delete failed for {}: {}", full_path, e);
                return Err(libc::EIO);
            }
        }

        info!("Deleted file: {}", full_path);
        Ok(())
    }

    pub async fn rmdir(&self, parent: u64, name: &str) -> VirtualFsResult<()> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        debug!("rmdir: parent={}, name={}", parent, name);

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

        debug!(
            "rename: parent={}, name={}, newparent={}, newname={}",
            parent, name, newparent, newname
        );

        self.ensure_children_loaded(parent).await?;
        if parent != newparent {
            self.ensure_children_loaded(newparent).await?;
        }

        // ── Phase 1: validate everything locally before any side effects ──

        let (ino, old_path, kind, xet_hash, is_dirty, new_full_path) = {
            let inodes = self.inode_table.read().expect("inodes poisoned");

            // Source must exist (checked before noop so rename("missing","missing") → ENOENT)
            let src = inodes.lookup_child(parent, name).ok_or(libc::ENOENT)?;

            // Noop: renaming to same location (source verified to exist)
            if parent == newparent && name == newname {
                return Ok(());
            }
            let ino = src.inode;
            let old_path = src.full_path.clone();
            let kind = src.kind;
            let xet_hash = src.xet_hash.clone();
            let is_dirty = src.dirty;

            // Destination parent must exist
            let new_parent_entry = inodes.get(newparent).ok_or(libc::ENOENT)?;
            let new_full_path = if new_parent_entry.full_path.is_empty() {
                newname.to_string()
            } else {
                format!("{}/{}", new_parent_entry.full_path, newname)
            };

            // Prevent moving a directory into its own subtree (would create a cycle)
            if kind == InodeKind::Directory {
                let mut ancestor = newparent;
                while ancestor != 1 {
                    if ancestor == ino {
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
                // POSIX: file can only replace file, dir can only replace empty dir
                match (kind, existing.kind) {
                    (InodeKind::File, InodeKind::Directory) => return Err(libc::EISDIR),
                    (InodeKind::Directory, InodeKind::File) => return Err(libc::ENOTDIR),
                    (InodeKind::Directory, InodeKind::Directory) if !existing.children.is_empty() => {
                        return Err(libc::ENOTEMPTY);
                    }
                    _ => {}
                }
            }

            (ino, old_path, kind, xet_hash, is_dirty, new_full_path)
        };

        // ── Phase 2: remote side effects (only for clean files with a hash) ──

        if kind == InodeKind::File && !is_dirty {
            if let Some(ref hash) = xet_hash {
                let mtime_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let ops = vec![
                    BatchOp::AddFile {
                        path: new_full_path.clone(),
                        xet_hash: hash.clone(),
                        mtime: mtime_ms,
                        content_type: None,
                    },
                    BatchOp::DeleteFile { path: old_path.clone() },
                ];

                if let Err(e) = self.hub_client.batch_operations(&ops).await {
                    error!("Failed to rename {} -> {}: {}", old_path, new_full_path, e);
                    return Err(libc::EIO);
                }
            }
        }

        // ── Phase 3: apply local mutations under one write lock ──

        self.negative_cache_remove(&new_full_path);

        {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");

            // Re-validate source still exists (could have been unlinked concurrently)
            if inodes.get(ino).is_none() {
                return Err(libc::ENOENT);
            }

            // Re-check destination under write lock (a concurrent create could have
            // inserted one between phase 1 read-lock and this write-lock).
            if let Some(existing) = inodes.lookup_child(newparent, newname) {
                if no_replace {
                    return Err(libc::EEXIST);
                }
                // POSIX type compat: file→dir = EISDIR, dir→file = ENOTDIR
                match (kind, existing.kind) {
                    (InodeKind::File, InodeKind::Directory) => return Err(libc::EISDIR),
                    (InodeKind::Directory, InodeKind::File) => return Err(libc::ENOTDIR),
                    (InodeKind::Directory, InodeKind::Directory) if !existing.children.is_empty() => {
                        return Err(libc::ENOTEMPTY);
                    }
                    _ => {}
                }
                let existing_ino = existing.inode;
                inodes.remove(existing_ino);
            }

            // Dirty file with a remote presence: record old path for deletion at flush time
            if is_dirty && kind == InodeKind::File && xet_hash.is_some() {
                if let Some(entry) = inodes.get_mut(ino) {
                    entry.pending_deletes.push(old_path.clone());
                }
            }

            // Detach from old parent
            if let Some(old_parent) = inodes.get_mut(parent) {
                old_parent.children.retain(|&c| c != ino);
            }

            // Update name/parent, then recursively fix all descendant paths
            if let Some(entry) = inodes.get_mut(ino) {
                entry.name = newname.to_string();
                entry.parent = newparent;
            }
            inodes.update_subtree_paths(ino, new_full_path.clone());

            // Attach to new parent
            if let Some(new_parent) = inodes.get_mut(newparent) {
                new_parent.children.push(ino);
            }
        }

        Ok(())
    }

    pub async fn setattr(&self, ino: u64, size: Option<u64>) -> VirtualFsResult<VirtualFsAttr> {
        debug!("setattr: ino={}, size={:?}", ino, size);

        if self.read_only {
            return Err(libc::EROFS);
        }

        // Validate inode exists and is a file before any side effects
        {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            match inodes.get(ino) {
                Some(e) if size.is_some() && e.kind != InodeKind::File => return Err(libc::EISDIR),
                Some(_) => {}
                None => return Err(libc::ENOENT),
            }
        }

        if let Some(new_size) = size {
            // Serialize with open() staging preparation on the same inode
            let staging_mutex = self.staging_lock(ino);
            let _staging_guard = staging_mutex.lock().await;

            let staging_path = self.file_staging.staging_path(ino);

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
                            .file_staging
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
                entry.dirty = true;
            }

            // Schedule flush so the truncation is committed to CAS/bucket
            drop(inodes);
            if let Some(fm) = &self.flush_manager {
                fm.enqueue(ino);
            }
        }

        let inodes = self.inode_table.read().expect("inodes poisoned");
        match inodes.get(ino) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => Err(libc::ENOENT),
        }
    }
}

// ── VFS types ──────────────────────────────────────────────────────────
pub type VirtualFsError = i32;
pub type VirtualFsResult<T> = std::result::Result<T, VirtualFsError>;

pub struct VirtualFsAttr {
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

pub struct VirtualFsDirEntry {
    pub ino: u64,
    pub kind: InodeKind,
    pub name: String,
}

// ── Internal types ─────────────────────────────────────────────────────

/// An open file handle — either a local fd or a lazy remote reference.
enum OpenFile {
    /// Local file (staging for writes, or dirty reads).
    Local { ino: u64, file: Arc<File>, writable: bool },
    /// Lazy remote — data fetched on-demand with adaptive prefetch buffer.
    Lazy {
        prefetch: Arc<tokio::sync::Mutex<PrefetchState>>,
    },
}

/// What to do in read() after releasing the open_files lock.
enum ReadTarget {
    /// Hold an Arc<File> so the FD stays alive even if release() runs concurrently.
    LocalFd(Arc<File>),
    Remote {
        prefetch: Arc<tokio::sync::Mutex<PrefetchState>>,
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
