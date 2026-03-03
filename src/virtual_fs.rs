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
use crate::xet::{StagingDir, StreamingWriter, XetSessions};

// ── Constants ──────────────────────────────────────────────────────────

/// Block size reported in stat(2) for `st_blocks` calculation.
const BLOCK_SIZE: u32 = 512;
/// Maximum entries in the negative-lookup cache (parent_path/name → Instant).
/// Prevents repeated Hub API calls for paths known to not exist.
const NEG_CACHE_CAPACITY: usize = 10_000;
/// How long a negative-cache entry stays valid before being re-checked.
const NEG_CACHE_TTL: Duration = Duration::from_secs(30);

type Invalidator = Arc<Mutex<Option<Box<dyn Fn(u64) + Send + Sync>>>>;

// ── VirtualFs ──────────────────────────────────────────────────────────

pub struct VirtualFs {
    runtime: tokio::runtime::Handle,
    hub_client: Arc<HubApiClient>,
    xet_sessions: Arc<XetSessions>,
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
    /// Debounced batch flush pipeline (None when read_only).
    flush_manager: Option<crate::flush::FlushManager>,
    /// Background poll task handle, taken in shutdown().
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
}

impl VirtualFs {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: tokio::runtime::Handle,
        hub_client: Arc<HubApiClient>,
        xet_sessions: Arc<XetSessions>,
        staging_dir: Option<StagingDir>,
        read_only: bool,
        advanced_writes: bool,
        uid: u32,
        gid: u32,
        poll_interval_secs: u64,
        metadata_ttl: Duration,
        serve_lookup_from_cache: bool,
    ) -> Arc<Self> {
        let inodes = Arc::new(RwLock::new(InodeTable::new()));
        let negative_cache = Arc::new(RwLock::new(HashMap::new()));

        let flush_manager = if !read_only && advanced_writes {
            let sd = staging_dir
                .as_ref()
                .expect("--advanced-writes requires a staging directory");
            Some(crate::flush::FlushManager::new(
                xet_sessions.clone(),
                sd.clone(),
                hub_client.clone(),
                inodes.clone(),
                &runtime,
            ))
        } else {
            None
        };

        // Spawn remote change polling task (if interval > 0)
        let invalidator: Invalidator = Arc::new(Mutex::new(None));
        let poll_handle = if poll_interval_secs > 0 {
            let bg_hub = hub_client.clone();
            let bg_inodes = inodes.clone();
            let bg_neg_cache = negative_cache.clone();
            let bg_invalidator = invalidator.clone();
            let interval = Duration::from_secs(poll_interval_secs);

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

        Arc::new(Self {
            runtime,
            hub_client,
            xet_sessions,
            staging_dir,
            read_only,
            advanced_writes,
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
            metadata_ttl,
            serve_lookup_from_cache,
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
        negative_cache: Arc<RwLock<HashMap<String, Instant>>>,
        invalidator: Invalidator,
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
                    // Invalidate the deleted inode and its parent dir so the
                    // kernel drops the dentry and page cache for this file.
                    if let Some(entry) = inode_table.get(*ino) {
                        let parent_ino = entry.parent;
                        inos_to_invalidate.push(parent_ino);
                    }
                    inos_to_invalidate.push(*ino);
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

    /// Revalidate a remote file by checking the Hub for metadata changes.
    /// Skips HEAD if the inode was validated within `metadata_ttl`.
    /// If the file's xet_hash changed, updates the inode and invalidates kernel cache.
    /// If the file was deleted (404), removes the inode.
    /// On network errors, silently returns (graceful degradation).
    async fn revalidate_file(&self, ino: u64, full_path: &str, current_hash: &str) {
        // When serve_lookup_from_cache is true, skip HEAD if recently validated.
        // When false (minimal mode), always HEAD on every lookup.
        if self.serve_lookup_from_cache {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            if let Some(entry) = inodes.get(ino) {
                if let Some(last) = entry.last_revalidated {
                    if last.elapsed() < self.metadata_ttl {
                        return;
                    }
                }
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
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                if remote_hash != Some(current_hash) {
                    let remote_size = head_info.size.unwrap_or(0);
                    let remote_mtime = head_info
                        .last_modified
                        .as_deref()
                        .map(HubApiClient::mtime_from_http_date)
                        .unwrap_or(SystemTime::now());
                    debug!("Remote change detected via HEAD: {} (hash changed)", full_path);
                    inodes.update_remote_file(ino, remote_hash.map(|s| s.to_string()), remote_size, remote_mtime);
                    if let Some(invalidate) = self.invalidator.lock().expect("invalidator poisoned").as_ref() {
                        invalidate(ino);
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
                    .filter(|&&c| inodes.get(c).is_some_and(|e| e.kind == InodeKind::Directory))
                    .copied()
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
        // Revalidation info extracted from the lock scope so we can await outside it.
        enum FastResult {
            Hit(VirtualFsAttr),
            NeedsRevalidation {
                ino: u64,
                full_path: String,
                current_hash: String,
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
                    // Revalidate remote files via HEAD
                    // Only for non-dirty files that have a remote xet_hash.
                    if entry.kind == InodeKind::File && !entry.dirty && entry.xet_hash.is_some() {
                        FastResult::NeedsRevalidation {
                            ino: entry.inode,
                            full_path: entry.full_path.clone(),
                            current_hash: entry.xet_hash.clone().unwrap(),
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
            } => {
                self.revalidate_file(ino, &full_path, &current_hash).await;
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

    pub async fn open(&self, ino: u64, writable: bool, truncate: bool) -> VirtualFsResult<u64> {
        debug!("open: ino={}, writable={}, truncate={}", ino, writable, truncate);

        if writable && self.read_only {
            return Err(libc::EROFS);
        }

        // Simple mode: open(writable) on existing files is not supported.
        // Only create() produces writable handles. EPERM.
        if writable && !self.advanced_writes {
            return Err(libc::EPERM);
        }

        let (xet_hash, size, is_dirty) = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let entry = match inodes.get(ino) {
                Some(e) if e.kind == InodeKind::File => e,
                _ => return Err(libc::ENOENT),
            };

            (entry.xet_hash.clone().unwrap_or_default(), entry.size, entry.dirty)
        };

        let staging_path = self.staging_dir.as_ref().map(|sd| sd.path(ino));

        if writable {
            // Advanced writes mode: staging file + async flush
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
                .dirty;

            // Reuse existing dirty staging file (unless truncating)
            if !(is_dirty && staging_path.exists() && !truncate) {
                if !truncate && !xet_hash.is_empty() && size > 0 {
                    // Download remote content for read-modify-write
                    self.xet_sessions
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
            Ok(file_handle)
        } else {
            // Dirty file not yet flushed → read from local staging (advanced mode only)
            if let Some(ref staging_path) = staging_path
                && is_dirty
                && staging_path.exists()
            {
                let file_handle = self.open_local_readonly(ino, staging_path)?;
                return Ok(file_handle);
            }

            // Remote file with xet hash → lazy range reads via prefetch buffer
            if !xet_hash.is_empty() {
                let prefetch = Arc::new(tokio::sync::Mutex::new(PrefetchState::new(xet_hash, size)));
                let file_handle = self.alloc_file_handle();
                self.open_files
                    .write()
                    .expect("open_files poisoned")
                    .insert(file_handle, OpenFile::Lazy { prefetch });
                return Ok(file_handle);
            }

            // No hash + non-empty → inconsistent state
            if size > 0 {
                error!("No xet hash for non-empty, non-dirty file ino={}", ino);
                return Err(libc::EIO);
            }

            // Empty file (size=0, no hash) → serve as lazy with 0 bytes
            let prefetch = Arc::new(tokio::sync::Mutex::new(PrefetchState::new(String::new(), 0)));
            let file_handle = self.alloc_file_handle();
            self.open_files
                .write()
                .expect("open_files poisoned")
                .insert(file_handle, OpenFile::Lazy { prefetch });
            Ok(file_handle)
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
                    match self.xet_sessions.download_stream(&xet_file_info) {
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
                        .xet_sessions
                        .download_to_writer(&file_info, offset..fetch_end, writer)
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
                    if let Some(entry) = inodes.get_mut(handle_ino)
                        && new_end > entry.size
                    {
                        entry.size = new_end;
                    }
                    Ok(written)
                }
            }
            WriteTarget::Streaming {
                ino: handle_ino,
                channel,
            } => {
                // Check for previous worker error
                if channel.error.lock().unwrap().is_some() {
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

                // Enqueue data to the background worker (blocks if channel buffer is full = backpressure)
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

    pub async fn flush(&self, ino: u64, file_handle: u64) -> VirtualFsResult<()> {
        debug!("flush: ino={}, fh={}", ino, file_handle);

        // Check if this is a streaming handle → synchronous upload + commit
        let streaming_channel = {
            let files = self.open_files.read().expect("open_files poisoned");
            match files.get(&file_handle) {
                Some(OpenFile::Streaming { channel, .. }) => Some(channel.clone()),
                _ => None,
            }
        };

        if let Some(channel) = streaming_channel {
            // Check for pending_info first (retry after previous Hub commit failure)
            let file_info = {
                let pending = channel.pending_info.lock().unwrap().take();
                if let Some(info) = pending {
                    info
                } else {
                    // Send Finish to the background worker and await the CAS upload result.
                    // If the channel is closed, the worker already finished (previous flush
                    // succeeded) — flush() can be called multiple times (dup/fork), so this
                    // is a valid no-op.
                    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                    if channel.tx.send(WriteMsg::Finish(result_tx)).await.is_err() {
                        debug!("flush: channel already closed for ino={}, already committed", ino);
                        return Ok(());
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
                // CAS upload succeeded but Hub commit failed — preserve file_info for retry
                *channel.pending_info.lock().unwrap() = Some(file_info);
                return Err(libc::EIO);
            }

            // Update inode: clean, with hash
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            if let Some(entry) = inodes.get_mut(ino) {
                entry.xet_hash = Some(file_info.hash().to_string());
                entry.size = file_info.file_size();
                entry.dirty = false;
                entry.mtime = SystemTime::now();
                entry.pending_deletes.clear();
            }

            info!(
                "Committed file: {} (hash={}, size={})",
                full_path,
                file_info.hash(),
                file_info.file_size()
            );

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

    pub fn release(&self, file_handle: u64) {
        debug!("release: fh={}", file_handle);

        let removed = self
            .open_files
            .write()
            .expect("open_files poisoned")
            .remove(&file_handle);

        match removed {
            Some(OpenFile::Local {
                ino, writable: true, ..
            }) => {
                // Advanced writes: enqueue for async flush
                if let Some(fm) = &self.flush_manager {
                    fm.enqueue(ino);
                }
            }
            Some(OpenFile::Streaming { .. }) => {
                // Simple mode: upload already happened in flush(), just drop the handle.
            }
            _ => {}
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
                    let attr = self.make_vfs_attr(inodes.get(ino).unwrap());
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
            // Data enqueued by write() is drained by a background tokio task
            // that feeds the CAS cleaner — no block_on() on the write hot path.
            let streaming_writer = match self.xet_sessions.create_streaming_writer().await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to create streaming writer: {}", e);
                    self.inode_table.write().expect("inodes poisoned").remove(ino);
                    return Err(libc::EIO);
                }
            };

            let (tx, rx) = tokio::sync::mpsc::channel::<WriteMsg>(4);
            let error: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
            self.runtime
                .spawn(streaming_worker(streaming_writer, rx, error.clone()));

            let channel = Arc::new(StreamingChannel {
                tx,
                bytes_written: AtomicU64::new(0),
                error,
                pending_info: std::sync::Mutex::new(None),
            });

            let file_handle = self.alloc_file_handle();
            self.open_files
                .write()
                .expect("open_files poisoned")
                .insert(file_handle, OpenFile::Streaming { ino, channel });

            let inodes = self.inode_table.read().expect("inodes poisoned");
            let attr = self.make_vfs_attr(inodes.get(ino).unwrap());
            Ok((attr, file_handle))
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

        // Delete from remote first — if this fails, local state is untouched and
        // the caller gets EIO with a consistent filesystem.
        if needs_remote_delete
            && let Err(e) = self
                .hub_client
                .batch_operations(&[BatchOp::DeleteFile {
                    path: full_path.clone(),
                }])
                .await
        {
            error!("Remote delete failed for {}: {}", full_path, e);
            return Err(libc::EIO);
        }

        // Remote succeeded (or no remote needed) — now remove locally.
        self.inode_table.write().expect("inodes poisoned").remove(ino);
        // Clean up staging file if present (advanced writes only)
        if let Some(ref staging_dir) = self.staging_dir {
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

        // Phase 2: sync to remote (add + delete ops)
        self.rename_remote(&info).await?;

        // Phase 3: apply to local inode table under write lock
        self.rename_apply_local(info, parent, newparent, newname, no_replace)
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
        let new_full_path = if new_parent_entry.full_path.is_empty() {
            newname.to_string()
        } else {
            format!("{}/{}", new_parent_entry.full_path, newname)
        };

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
                (InodeKind::File, InodeKind::Directory) => return Err(libc::EISDIR),
                (InodeKind::Directory, InodeKind::File) => return Err(libc::ENOTDIR),
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
            while let Some(dir_ino) = stack.pop() {
                if let Some(entry) = inodes.get(dir_ino) {
                    for &child_ino in &entry.children {
                        if let Some(child) = inodes.get(child_ino) {
                            match child.kind {
                                InodeKind::File if !child.dirty && child.xet_hash.is_some() => {
                                    files.push((child.full_path.clone(), child.xet_hash.clone().unwrap()));
                                }
                                InodeKind::Directory => stack.push(child_ino),
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
            is_dirty: src.dirty,
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

        if let Err(e) = self.hub_client.batch_operations(&ops).await {
            error!("Failed to rename {} -> {}: {}", info.old_path, info.new_full_path, e);
            return Err(libc::EIO);
        }
        Ok(())
    }

    /// Phase 3: apply rename to local inode table under write lock.
    fn rename_apply_local(
        &self,
        info: RenameInfo,
        parent: u64,
        newparent: u64,
        newname: &str,
        no_replace: bool,
    ) -> VirtualFsResult<()> {
        self.negative_cache_remove(&info.new_full_path);

        let mut inodes = self.inode_table.write().expect("inodes poisoned");

        // Re-validate source still exists (could have been unlinked concurrently)
        if inodes.get(info.ino).is_none() {
            return Err(libc::ENOENT);
        }

        // Re-check destination under write lock (a concurrent create could have
        // inserted one between phase 1 read-lock and this write-lock).
        if let Some(existing) = inodes.lookup_child(newparent, newname) {
            if no_replace {
                return Err(libc::EEXIST);
            }
            match (info.kind, existing.kind) {
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
                let children: Vec<u64> = inodes.get(dir_ino).map(|e| e.children.clone()).unwrap_or_default();
                for child_ino in children {
                    if let Some(child) = inodes.get(child_ino) {
                        match child.kind {
                            InodeKind::File if child.dirty && child.xet_hash.is_some() => {
                                let old_path = child.full_path.clone();
                                if let Some(child_mut) = inodes.get_mut(child_ino) {
                                    child_mut.pending_deletes.push(old_path);
                                }
                            }
                            InodeKind::Directory => stack.push(child_ino),
                            _ => {}
                        }
                    }
                }
            }
        }

        // Detach from old parent
        if let Some(old_parent) = inodes.get_mut(parent) {
            old_parent.children.retain(|&c| c != info.ino);
        }

        // Update name/parent, then recursively fix all descendant paths
        if let Some(entry) = inodes.get_mut(info.ino) {
            entry.name = newname.to_string();
            entry.parent = newparent;
        }
        inodes.update_subtree_paths(info.ino, info.new_full_path);

        // Attach to new parent
        if let Some(new_parent) = inodes.get_mut(newparent) {
            new_parent.children.push(info.ino);
        }

        Ok(())
    }

    pub async fn setattr(&self, ino: u64, size: Option<u64>) -> VirtualFsResult<VirtualFsAttr> {
        debug!("setattr: ino={}, size={:?}", ino, size);

        if self.read_only {
            return Err(libc::EROFS);
        }

        // Simple mode: truncate via setattr not supported
        if size.is_some() && !self.advanced_writes {
            return Err(libc::EPERM);
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

/// State machine for streaming writes.
/// Message sent from the write() caller to the background streaming worker.
enum WriteMsg {
    Data(Vec<u8>),
    Finish(tokio::sync::oneshot::Sender<crate::error::Result<XetFileInfo>>),
}

/// Channel-based streaming handle. Decouples the sync write() caller from the
/// async add_data() pipeline: writes enqueue data into a bounded channel, a background
/// tokio task drains it and feeds the CAS cleaner.
struct StreamingChannel {
    tx: tokio::sync::mpsc::Sender<WriteMsg>,
    bytes_written: AtomicU64,
    /// Set by the background worker if add_data() fails. Shared with worker via Arc.
    error: Arc<std::sync::Mutex<Option<String>>>,
    /// CAS upload succeeded but Hub commit failed — stored for retry on next flush().
    pending_info: std::sync::Mutex<Option<XetFileInfo>>,
}

/// An open file handle — either a local fd, lazy remote reference, or streaming writer.
enum OpenFile {
    /// Local file (staging for writes, or dirty reads).
    Local { ino: u64, file: Arc<File>, writable: bool },
    /// Lazy remote — data fetched on-demand with adaptive prefetch buffer.
    Lazy {
        prefetch: Arc<tokio::sync::Mutex<PrefetchState>>,
    },
    /// Streaming append-only writer (default write mode).
    Streaming { ino: u64, channel: Arc<StreamingChannel> },
}

/// Background task: drains the write channel and feeds data into the CAS streaming writer.
/// Runs until a Finish message is received or the channel is dropped.
async fn streaming_worker(
    mut writer: StreamingWriter,
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
                    Err(crate::error::Error::Hub("streaming write failed".into()))
                } else {
                    writer.finish().await
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
