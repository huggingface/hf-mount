use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const ROOT_INODE: u64 = 1;

/// Build a child's full path given the parent's full_path and child name.
pub fn child_path(parent_path: &str, name: &str) -> String {
    if parent_path.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", parent_path, name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeKind {
    File,
    Directory,
    Symlink,
}

/// A directory child entry: stores the name on the parent→child edge.
#[derive(Debug, Clone)]
pub struct DirChild {
    pub ino: u64,
    pub name: String,
}

/// Eviction-control bookkeeping. All atomics so the FUSE hot path can
/// update them under a read lock on `InodeTable` — no writer contention
/// on `lookup`/`getattr`/`forget`.
#[derive(Debug, Default)]
pub struct EvictionState {
    /// Kernel lookup refcount (FUSE `nlookup`). Bumped on every
    /// `reply.entry()` / `reply.created()`; dropped by the value the
    /// kernel passes to `forget()`. `> 0` means the kernel still holds
    /// a dentry — polite eviction must invalidate it first.
    pub nlookup: AtomicU64,
    /// Snapshot of `InodeTable.touch_counter` at last lookup/getattr.
    /// LRU recency key — oldest values evict first.
    pub last_touched: AtomicU64,
    /// Set when `forget()` wanted to evict but an open handle blocked it.
    /// Checked by `release()` after the last handle closes so the entry
    /// isn't pinned forever (the kernel has already given up on it and
    /// won't send another `forget`).
    pub evict_pending: AtomicBool,
    /// Number of live FUSE file handles referencing this inode. Bumped by
    /// `open`/`create`, dropped by `release`. Eviction refuses any entry
    /// with `open_handles > 0` — a racing read/write would silently lose
    /// data if the inode disappeared under it.
    pub open_handles: AtomicU32,
}

impl Clone for EvictionState {
    fn clone(&self) -> Self {
        Self {
            nlookup: AtomicU64::new(self.nlookup.load(Ordering::Relaxed)),
            last_touched: AtomicU64::new(self.last_touched.load(Ordering::Relaxed)),
            evict_pending: AtomicBool::new(self.evict_pending.load(Ordering::Relaxed)),
            open_handles: AtomicU32::new(self.open_handles.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(any(feature = "fuse", test))]
impl EvictionState {
    /// Bump the kernel lookup refcount. Relaxed is sufficient: the refcount
    /// only gates eviction, which is guarded separately by the inode table's
    /// write lock.
    pub(crate) fn bump_nlookup(&self) {
        self.nlookup.fetch_add(1, Ordering::Relaxed);
    }

    /// Drop the kernel lookup refcount by `n`, saturating at 0. Returns true
    /// if the refcount reached 0 (safe-to-evict).
    pub(crate) fn drop_nlookup(&self, n: u64) -> bool {
        let prev = self
            .nlookup
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| Some(cur.saturating_sub(n)))
            .expect("fetch_update closure always returns Some");
        // A forget(n) larger than the current count is a kernel- or us-side
        // protocol bug. Surface it in debug builds without crashing prod.
        debug_assert!(n <= prev, "drop_nlookup({n}) exceeded current {prev}");
        prev.saturating_sub(n) == 0
    }
}

#[derive(Debug, Clone)]
pub struct InodeEntry {
    pub inode: u64,
    pub parent: u64,
    pub name: String,
    /// Full path from the mount root.
    pub full_path: Arc<str>,
    pub kind: InodeKind,
    pub size: u64,
    pub mtime: SystemTime,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub atime: SystemTime,
    pub ctime: SystemTime,
    pub nlink: u32,
    pub symlink_target: Option<String>,
    pub xet_hash: Option<String>,
    /// True when the on-disk staging file is known to match the remote at
    /// `xet_hash`. Set by a successful commit, or by a download whose pre/post
    /// `xet_hash` matched (so we didn't race with `poll_remote_changes`).
    /// Cleared whenever poll advances `xet_hash` out of band.
    pub staging_is_current: bool,
    /// ETag from the last HEAD revalidation (used for non-xet plain git/LFS files).
    pub etag: Option<String>,
    /// Dirty generation counter. 0 = clean. Each mutation increments the counter.
    /// `flush_batch` snapshots the generation before upload; after commit it only
    /// clears dirty (resets to 0) if the generation hasn't advanced, preventing
    /// concurrent writers from silently losing their data.
    pub dirty_generation: u64,
    pub children_loaded: bool,
    pub children: Vec<DirChild>,
    /// Old remote paths that should be deleted on next flush (set by rename of dirty files).
    pub pending_deletes: Vec<String>,
    /// When this inode's metadata was last validated against the remote (via HEAD).
    /// Used to avoid redundant HEAD requests within the revalidation TTL.
    pub last_revalidated: Option<Instant>,
    /// Eviction bookkeeping (kernel refcount, LRU recency, pending flag, pinning).
    pub eviction: EvictionState,
}

impl InodeEntry {
    /// Returns true if this inode has uncommitted changes.
    pub fn is_dirty(&self) -> bool {
        self.dirty_generation > 0
    }

    /// Mark the inode as dirty, incrementing the generation counter.
    pub fn set_dirty(&mut self) {
        self.dirty_generation = self.dirty_generation.saturating_add(1);
    }

    /// Clear the dirty flag, but only if the generation matches the snapshot
    /// taken before the flush. Returns true if cleared, false if a concurrent
    /// writer advanced the generation (meaning the inode is still dirty).
    pub fn clear_dirty_if(&mut self, snapshot_generation: u64) -> bool {
        if self.dirty_generation == snapshot_generation {
            self.dirty_generation = 0;
            true
        } else {
            false
        }
    }

    /// Apply a successful commit: update hash, size, timestamps, and
    /// conditionally clear dirty + pending_deletes if the generation matches.
    pub fn apply_commit(&mut self, hash: &str, size: u64, dirty_generation: u64) {
        if self.clear_dirty_if(dirty_generation) {
            // Only update metadata when the generation matches. A concurrent
            // writer may have advanced the generation with newer content;
            // overwriting size/hash here would clobber the in-progress data.
            self.xet_hash = Some(hash.to_string());
            // The on-disk staging file is the just-uploaded content — valid
            // cache for the next write-open.
            self.staging_is_current = true;
            self.size = size;
            self.pending_deletes.clear();
        }
        let now = SystemTime::now();
        self.mtime = now;
        self.ctime = now;
        // Mark as recently validated so subsequent lookups skip HEAD revalidation
        // for the duration of metadata_ttl (we just committed this exact hash).
        self.last_revalidated = Some(Instant::now());
    }
}

pub struct InodeTable {
    inodes: HashMap<u64, InodeEntry>,
    /// Path → inode reverse lookup. Key is shared with `InodeEntry.full_path`
    /// (same `Arc`) so each path is allocated once.
    path_to_inode: HashMap<Arc<str>, u64>,
    next_inode: AtomicU64,
    /// Monotonic counter snapshotted into `InodeEntry.last_touched` — used
    /// by the LRU evictor to order entries by recency without a wall-clock
    /// syscall on the hot path.
    touch_counter: AtomicU64,
    /// Target size for the LRU evictor. 0 = disabled. When non-zero, the
    /// async sweep in `VirtualFs::lru_sweep_loop` picks the oldest-touched
    /// entries and asks the kernel (via FUSE `inval_entry`) to drop their
    /// dentries; the kernel's subsequent `forget()` then shrinks the table.
    /// The table may temporarily overshoot the cap between sweeps.
    soft_limit: AtomicUsize,
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new(0)
    }
}

impl InodeTable {
    pub fn new(soft_limit: usize) -> Self {
        let mut table = Self {
            inodes: HashMap::new(),
            path_to_inode: HashMap::new(),
            next_inode: AtomicU64::new(2),
            touch_counter: AtomicU64::new(0),
            soft_limit: AtomicUsize::new(soft_limit),
        };

        // Create root inode
        let root_path: Arc<str> = Arc::from("");
        let root = InodeEntry {
            inode: ROOT_INODE,
            parent: ROOT_INODE,
            name: String::new(),
            full_path: root_path.clone(),
            kind: InodeKind::Directory,
            size: 0,
            mtime: UNIX_EPOCH,
            mode: 0o755,
            uid: 0,
            gid: 0,
            atime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            nlink: 2,
            symlink_target: None,
            xet_hash: None,
            staging_is_current: false,
            etag: None,
            dirty_generation: 0,
            children_loaded: false,
            children: Vec::new(),
            pending_deletes: Vec::new(),
            last_revalidated: None,
            eviction: EvictionState::default(),
        };
        table.inodes.insert(ROOT_INODE, root);
        table.path_to_inode.insert(root_path, ROOT_INODE);

        table
    }

    pub fn get(&self, inode: u64) -> Option<&InodeEntry> {
        self.inodes.get(&inode)
    }

    pub fn get_mut(&mut self, inode: u64) -> Option<&mut InodeEntry> {
        self.inodes.get_mut(&inode)
    }

    /// Bump the kernel lookup refcount on `inode`. Shared `&self` because the
    /// refcount itself is atomic. Calling this on an unknown inode is a bug
    /// (we just returned that ino to the kernel), but a missing entry here
    /// would be a race we can't recover from, so we log+return.
    #[cfg(any(feature = "fuse", test))]
    pub(crate) fn bump_nlookup(&self, inode: u64) {
        match self.inodes.get(&inode) {
            Some(entry) => entry.eviction.bump_nlookup(),
            None => debug_assert!(false, "bump_nlookup: unknown inode {inode}"),
        }
    }

    /// Drop the kernel lookup refcount on `inode` by `n`. Returns true if the
    /// refcount reached 0 (safe-to-evict); false if unknown / still held.
    #[cfg(any(feature = "fuse", test))]
    pub(crate) fn drop_nlookup(&self, inode: u64, n: u64) -> bool {
        self.inodes
            .get(&inode)
            .map(|e| e.eviction.drop_nlookup(n))
            .unwrap_or(false)
    }

    /// Bump the per-inode open-handle refcount. Called on every `open` /
    /// `create` to pin the entry against eviction for as long as a FUSE
    /// file handle references it.
    pub(crate) fn bump_open_handles(&self, ino: u64) {
        if let Some(entry) = self.inodes.get(&ino) {
            entry.eviction.open_handles.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drop the per-inode open-handle refcount. Called on `release` once
    /// the handle has been removed from `VirtualFs::open_files`.
    pub(crate) fn drop_open_handles(&self, ino: u64) {
        if let Some(entry) = self.inodes.get(&ino) {
            entry.eviction.open_handles.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Is there at least one live FUSE file handle on this inode?
    pub(crate) fn has_open_handles(&self, ino: u64) -> bool {
        self.inodes
            .get(&ino)
            .is_some_and(|e| e.eviction.open_handles.load(Ordering::Relaxed) > 0)
    }

    pub fn len(&self) -> usize {
        self.inodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inodes.is_empty()
    }

    /// Snapshot a fresh counter value onto `inode.last_touched`. Atomic so
    /// the FUSE reader pool never contends a writer. Early-out when LRU is
    /// disabled so the common prod config pays nothing.
    pub(crate) fn touch(&self, inode: u64) {
        if self.soft_limit.load(Ordering::Relaxed) == 0 {
            return;
        }
        if let Some(entry) = self.inodes.get(&inode) {
            let seq = self.touch_counter.fetch_add(1, Ordering::Relaxed);
            entry.eviction.last_touched.store(seq, Ordering::Relaxed);
        }
    }

    /// Mark an inode as "wanted to evict but blocked" so `release()` can
    /// finish the job once the last open handle closes. No-op on unknown
    /// inode (the entry may have been removed on a racing path).
    #[cfg(any(feature = "fuse", test))]
    pub(crate) fn mark_evict_pending(&self, ino: u64) {
        if let Some(entry) = self.inodes.get(&ino) {
            entry.eviction.evict_pending.store(true, Ordering::Relaxed);
        }
    }

    /// Atomically test-and-clear the flag. Returns true if the caller now
    /// owns the deferred eviction.
    pub(crate) fn take_evict_pending(&self, ino: u64) -> bool {
        self.inodes
            .get(&ino)
            .is_some_and(|e| e.eviction.evict_pending.swap(false, Ordering::Relaxed))
    }

    /// Return up to `max` oldest-touched file inodes eligible for eviction
    /// (regular files with `nlink > 0`, clean, no pending renames). The
    /// caller hands these `(parent, name)` pairs to `inval_entry`; the
    /// kernel drops its dentry, `forget()` fires, and `evict_if_safe`
    /// reclaims the inode.
    ///
    /// Uses a bounded max-heap so we only clone names for the `max` entries
    /// we actually return — O(N log max) with max cloned Strings, vs the
    /// naive sort-and-truncate that allocates for every filter-passing
    /// entry before discarding most of them.
    pub(crate) fn lru_candidates(&self, max: usize) -> Vec<(u64, String)> {
        if max == 0 {
            return Vec::new();
        }
        // BinaryHeap is a max-heap on the first tuple element (timestamp).
        // Keep size ≤ max by popping the newest when we overflow, so what
        // remains is the `max` oldest.
        let mut heap: BinaryHeap<(u64, u64)> = BinaryHeap::with_capacity(max + 1);
        for e in self.inodes.values() {
            if e.kind != InodeKind::File || e.nlink == 0 || e.is_dirty() || !e.pending_deletes.is_empty() {
                continue;
            }
            heap.push((e.eviction.last_touched.load(Ordering::Relaxed), e.inode));
            if heap.len() > max {
                heap.pop();
            }
        }
        // Resolve names in a second pass (oldest-first). One allocation
        // per returned candidate, not per table entry.
        let mut picks: Vec<(u64, u64)> = heap.into_vec();
        picks.sort_by_key(|&(ts, _)| ts);
        picks
            .into_iter()
            .filter_map(|(_, ino)| self.inodes.get(&ino).map(|e| (e.parent, e.name.clone())))
            .collect()
    }

    /// Evict a file inode from the table if it's safe to do so. Returns true
    /// if evicted. Mirrors mountpoint-s3's forget-driven eviction: only files
    /// (directories hold children_loaded state we can't easily restore),
    /// only when the kernel has fully released the dentry (`nlookup == 0`),
    /// only when there's no unflushed data (`!is_dirty`) and no pending
    /// rename-delete (`pending_deletes.is_empty`).
    ///
    /// Caller is responsible for checking that no open file handles reference
    /// this inode (see `VirtualFs::has_open_handles`).
    ///
    /// After eviction the parent directory is marked `children_loaded = false`
    /// so a subsequent `readdir`/`lookup` re-fetches the listing and
    /// re-materializes the inode.
    pub(crate) fn evict_if_safe(&mut self, ino: u64) -> bool {
        let should_evict = self.inodes.get(&ino).is_some_and(|e| {
            e.kind == InodeKind::File
                && e.eviction.nlookup.load(Ordering::Relaxed) == 0
                && !e.is_dirty()
                && e.pending_deletes.is_empty()
                && e.nlink > 0
        });
        if !should_evict {
            return false;
        }

        let Some(entry) = self.inodes.remove(&ino) else {
            return false;
        };
        self.path_to_inode.remove(&*entry.full_path);

        if let Some(parent) = self.inodes.get_mut(&entry.parent) {
            parent.children.retain(|c| c.ino != ino);
            // Force readdir to re-fetch so the inode can be re-materialized
            // on next access. Without this the kernel would ask for a child
            // that no longer exists in our in-memory tree.
            parent.children_loaded = false;
        }
        true
    }

    pub fn get_by_path(&self, path: &str) -> Option<&InodeEntry> {
        self.path_to_inode.get(path).and_then(|ino| self.inodes.get(ino))
    }

    /// Find a child of `parent` by name.
    /// Skips stale DirChild entries whose inode has been removed.
    pub fn lookup_child(&self, parent: u64, name: &str) -> Option<&InodeEntry> {
        let parent_entry = self.inodes.get(&parent)?;
        for child in &parent_entry.children {
            if child.name == name
                && let Some(entry) = self.inodes.get(&child.ino)
            {
                return Some(entry);
            }
        }
        None
    }

    /// Insert a new inode, returning its inode number.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        parent: u64,
        name: String,
        full_path: String,
        kind: InodeKind,
        size: u64,
        mtime: SystemTime,
        xet_hash: Option<String>,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> u64 {
        // Check if already exists
        if let Some(&existing_ino) = self.path_to_inode.get(full_path.as_str()) {
            debug_assert!(
                self.inodes.get(&existing_ino).map(|e| e.kind) == Some(kind),
                "insert(): path '{}' exists with different kind",
                full_path
            );
            // Stamp revalidation so subsequent lookups skip the HEAD request.
            if let Some(entry) = self.inodes.get_mut(&existing_ino) {
                entry.last_revalidated = Some(Instant::now());
            }
            return existing_ino;
        }

        debug_assert!(
            self.inodes.contains_key(&parent),
            "insert(): parent inode {} does not exist (path '{}')",
            parent,
            full_path
        );

        let now = SystemTime::now();
        let nlink = match kind {
            InodeKind::Directory => 2,
            _ => 1,
        };

        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        let touch_seq = self.touch_counter.fetch_add(1, Ordering::Relaxed);
        let child_name = name.clone();
        let full_path_arc: Arc<str> = Arc::from(full_path);
        let entry = InodeEntry {
            inode,
            parent,
            name,
            full_path: full_path_arc.clone(),
            kind,
            size,
            mtime,
            mode,
            uid,
            gid,
            atime: mtime,
            ctime: now,
            nlink,
            symlink_target: None,
            xet_hash,
            staging_is_current: false,
            etag: None,
            dirty_generation: 0,
            children_loaded: kind != InodeKind::Directory, // only dirs have children to load
            children: Vec::new(),
            pending_deletes: Vec::new(),
            last_revalidated: Some(Instant::now()),
            eviction: EvictionState {
                last_touched: AtomicU64::new(touch_seq),
                ..Default::default()
            },
        };

        self.inodes.insert(inode, entry);
        self.path_to_inode.insert(full_path_arc, inode);

        // Add to parent's children
        if let Some(parent_entry) = self.inodes.get_mut(&parent) {
            parent_entry.children.push(DirChild {
                ino: inode,
                name: child_name,
            });
            // POSIX: new subdirectory's ".." links to parent
            if kind == InodeKind::Directory {
                parent_entry.nlink += 1;
            }
        }

        inode
    }

    /// Remove a path → inode mapping.
    pub fn remove_path(&mut self, path: &str) {
        self.path_to_inode.remove(path);
    }

    /// Insert a path → inode mapping.
    pub fn insert_path(&mut self, path: String, inode: u64) {
        self.path_to_inode.insert(Arc::from(path), inode);
    }

    /// Return inodes of all dirty files (excludes symlinks — they have no content to flush).
    pub fn dirty_inos(&self) -> Vec<u64> {
        self.inodes
            .values()
            .filter(|e| e.kind == InodeKind::File && e.is_dirty() && e.nlink > 0)
            .map(|e| e.inode)
            .collect()
    }

    /// Snapshot of all file entries: (ino, full_path, xet_hash, etag, size, is_dirty)
    #[allow(clippy::type_complexity)]
    pub fn file_snapshot(&self) -> Vec<(u64, String, Option<String>, Option<String>, u64, bool)> {
        self.inodes
            .values()
            .filter(|e| e.kind == InodeKind::File)
            .map(|e| {
                (
                    e.inode,
                    e.full_path.to_string(),
                    e.xet_hash.clone(),
                    e.etag.clone(),
                    e.size,
                    e.is_dirty(),
                )
            })
            .collect()
    }

    /// Update remote file metadata (only if not dirty). Returns true if updated.
    pub fn update_remote_file(
        &mut self,
        ino: u64,
        new_hash: Option<String>,
        new_etag: Option<String>,
        new_size: u64,
        new_mtime: SystemTime,
    ) -> bool {
        if let Some(entry) = self.inodes.get_mut(&ino) {
            if entry.is_dirty() {
                return false;
            }
            entry.xet_hash = new_hash;
            entry.etag = new_etag;
            entry.size = new_size;
            entry.mtime = new_mtime;
            // Remote moved under us; the staging cache (if any) no longer
            // matches xet_hash. An in-flight download observes this under
            // its post-check and won't re-flag the cache.
            entry.staging_is_current = false;
            true
        } else {
            false
        }
    }

    /// Reset children_loaded to false so the next readdir/lookup re-fetches.
    pub fn invalidate_children(&mut self, ino: u64) {
        if let Some(entry) = self.inodes.get_mut(&ino) {
            entry.children_loaded = false;
        }
    }

    /// Check whether a directory's children have been loaded from the Hub API.
    pub fn is_children_loaded(&self, ino: u64) -> bool {
        self.inodes.get(&ino).is_some_and(|e| e.children_loaded)
    }

    /// Check whether an inode or any of its descendants is dirty.
    pub fn has_dirty_descendants(&self, ino: u64) -> bool {
        let mut stack = vec![ino];
        while let Some(current) = stack.pop() {
            if let Some(entry) = self.inodes.get(&current) {
                if entry.is_dirty() {
                    return true;
                }
                for child in &entry.children {
                    stack.push(child.ino);
                }
            }
        }
        false
    }

    /// Return the full_path of every directory whose children have been loaded.
    /// Used by the poll loop to only re-fetch directories the user has visited.
    pub fn loaded_dir_prefixes(&self) -> Vec<String> {
        self.inodes
            .values()
            .filter(|e| e.kind == InodeKind::Directory && e.children_loaded)
            .map(|e| e.full_path.to_string())
            .collect()
    }

    /// Get directory inode by path.
    pub fn get_dir_ino(&self, path: &str) -> Option<u64> {
        self.path_to_inode.get(path).copied().and_then(|ino| {
            self.inodes
                .get(&ino)
                .filter(|e| e.kind == InodeKind::Directory)
                .map(|e| e.inode)
        })
    }

    /// Recursively update full_path and path_to_inode for an inode and all its descendants.
    /// Used after a rename to keep the path mappings consistent.
    /// Hard-linked inodes whose full_path doesn't match their expected position in the tree
    /// are skipped (they belong elsewhere and their canonical path must not be rewritten).
    pub fn update_subtree_paths(&mut self, inode: u64, new_full_path: String) {
        if let Some(entry) = self.inodes.get(&inode) {
            let old_path: Arc<str> = entry.full_path.clone();
            self.path_to_inode.remove(&*old_path);
        }

        let new_full_path_arc: Arc<str> = Arc::from(new_full_path.as_str());
        let children = if let Some(entry) = self.inodes.get_mut(&inode) {
            entry.full_path = new_full_path_arc.clone();
            self.path_to_inode.insert(new_full_path_arc, inode);
            entry.children.clone()
        } else {
            return;
        };

        // Recursively update children
        for child in children {
            let child_path = child_path(&new_full_path, &child.name);
            self.update_subtree_paths(child.ino, child_path);
        }
    }

    /// Detach `child_ino` from its parent's `children` list and fix up
    /// POSIX bookkeeping. If `invalidate_children_loaded` is true, the
    /// parent will re-fetch on next readdir (the caller removed the child
    /// preemptively so the parent's cached listing is now stale).
    fn detach_from_parent(
        &mut self,
        parent_ino: u64,
        child_ino: u64,
        child_kind: InodeKind,
        invalidate_children_loaded: bool,
    ) {
        if let Some(parent) = self.inodes.get_mut(&parent_ino) {
            parent.children.retain(|c| c.ino != child_ino);
            if invalidate_children_loaded {
                parent.children_loaded = false;
            }
            // POSIX: removing a subdirectory drops the parent's ".." nlink.
            if child_kind == InodeKind::Directory {
                parent.nlink = parent.nlink.saturating_sub(1);
            }
        }
    }

    /// Remove an inode from the table (also removes from parent's children list).
    /// If the inode is a directory, all descendants are removed recursively to
    /// prevent orphaned entries in the table.
    pub fn remove(&mut self, inode: u64) -> Option<InodeEntry> {
        let entry = self.inodes.remove(&inode)?;
        self.path_to_inode.remove(&*entry.full_path);
        self.detach_from_parent(entry.parent, inode, entry.kind, false);

        // Recursively remove all descendants to avoid orphans
        let mut stack: Vec<u64> = entry.children.iter().map(|c| c.ino).collect();
        while let Some(child_ino) = stack.pop() {
            if let Some(child) = self.inodes.remove(&child_ino) {
                self.path_to_inode.remove(&*child.full_path);
                stack.extend(child.children.iter().map(|c| c.ino));
            }
        }

        Some(entry)
    }

    /// Remove one directory entry by name from `parent`. Decrements nlink.
    /// Returns `(inode_removed, entry_snapshot)` where `inode_removed` is true
    /// if nlink reached 0 and the inode was fully removed.
    pub fn unlink_one(&mut self, parent: u64, name: &str) -> Option<(bool, InodeEntry)> {
        // Find child ino and build full_path in a single parent lookup
        let (child_ino, full_path) = {
            let parent_entry = self.inodes.get(&parent)?;
            let ino = parent_entry.children.iter().find(|c| c.name == name).map(|c| c.ino)?;
            let path = child_path(&parent_entry.full_path, name);
            (ino, path)
        };

        // Remove the DirChild from parent
        if let Some(parent_entry) = self.inodes.get_mut(&parent)
            && let Some(pos) = parent_entry.children.iter().position(|c| c.name == name)
        {
            parent_entry.children.remove(pos);
        }

        self.path_to_inode.remove(full_path.as_str());

        // Decrement nlink
        let entry_snapshot = {
            let entry = self.inodes.get_mut(&child_ino)?;
            entry.nlink = entry.nlink.saturating_sub(1);
            entry.ctime = SystemTime::now();
            entry.clone()
        };

        Some((entry_snapshot.nlink == 0, entry_snapshot))
    }

    /// Move a child entry from one parent to another (or rename within the same parent).
    /// Detaches from old parent's children, attaches to new parent's children, updates the
    /// child's `parent`/`name`, and adjusts nlink for cross-parent directory moves.
    /// Does NOT update path mappings (caller should use `update_subtree_paths` separately).
    pub fn move_child(&mut self, ino: u64, old_parent: u64, old_name: &str, new_parent: u64, new_name: &str) {
        let is_dir = self.inodes.get(&ino).is_some_and(|e| e.kind == InodeKind::Directory);

        // Detach from old parent
        if let Some(old_p) = self.inodes.get_mut(&old_parent)
            && let Some(pos) = old_p.children.iter().position(|c| c.ino == ino && c.name == old_name)
        {
            old_p.children.remove(pos);
        }

        // Attach to new parent
        if let Some(new_p) = self.inodes.get_mut(&new_parent) {
            new_p.children.push(DirChild {
                ino,
                name: new_name.to_string(),
            });
        }

        // Adjust parent nlink for cross-parent directory moves
        if is_dir && old_parent != new_parent {
            if let Some(old_p) = self.inodes.get_mut(&old_parent) {
                old_p.nlink = old_p.nlink.saturating_sub(1);
            }
            if let Some(new_p) = self.inodes.get_mut(&new_parent) {
                new_p.nlink += 1;
            }
        }

        // Update child's parent/name
        if let Some(entry) = self.inodes.get_mut(&ino) {
            entry.parent = new_parent;
            entry.name = new_name.to_string();
        }
    }

    /// Update mtime and ctime on a parent directory (POSIX: directory modified).
    pub fn touch_parent(&mut self, parent: u64, now: SystemTime) {
        if let Some(p) = self.inodes.get_mut(&parent) {
            p.mtime = now;
            p.ctime = now;
        }
    }

    /// Remove an inode whose last link is gone (`nlink == 0`).
    /// Returns true if an entry was removed.
    pub fn remove_orphan(&mut self, ino: u64) -> bool {
        if let Some(entry) = self.inodes.get(&ino)
            && entry.nlink == 0
        {
            let path = entry.full_path.clone();
            self.inodes.remove(&ino);
            self.path_to_inode.remove(&path);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_lookup() {
        let mut table = InodeTable::new(0);

        let ino = table.insert(
            ROOT_INODE,
            "hello.txt".to_string(),
            "hello.txt".to_string(),
            InodeKind::File,
            42,
            UNIX_EPOCH,
            Some("abc123".to_string()),
            0o644,
            0,
            0,
        );

        // Lookup by child name from root
        let found = table.lookup_child(ROOT_INODE, "hello.txt");
        assert!(found.is_some(), "should find child by name");
        let found = found.unwrap();
        assert_eq!(found.inode, ino);
        assert_eq!(found.name, "hello.txt");
        assert_eq!(found.full_path.as_ref(), "hello.txt");
        assert_eq!(found.kind, InodeKind::File);
        assert_eq!(found.size, 42);
        assert_eq!(found.xet_hash, Some("abc123".to_string()));

        // Also verify get and get_by_path
        assert!(table.get(ino).is_some());
        assert!(table.get_by_path("hello.txt").is_some());
        assert_eq!(table.get_by_path("hello.txt").unwrap().inode, ino);

        // Non-existent lookup returns None
        assert!(table.lookup_child(ROOT_INODE, "missing.txt").is_none());
    }

    #[test]
    fn test_dirty_flag() {
        let mut table = InodeTable::new(0);

        let ino = table.insert(
            ROOT_INODE,
            "data.bin".to_string(),
            "data.bin".to_string(),
            InodeKind::File,
            0,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Newly inserted inodes are not dirty
        assert!(!table.get(ino).unwrap().is_dirty());

        // Set dirty (advances generation)
        table.get_mut(ino).unwrap().set_dirty();
        assert!(table.get(ino).unwrap().is_dirty());
        let dirty_gen = table.get(ino).unwrap().dirty_generation;
        assert_eq!(dirty_gen, 1);

        // Clear dirty with matching generation
        assert!(table.get_mut(ino).unwrap().clear_dirty_if(dirty_gen));
        assert!(!table.get(ino).unwrap().is_dirty());

        // Set dirty twice, clear with stale generation fails
        table.get_mut(ino).unwrap().set_dirty(); // gen=1
        table.get_mut(ino).unwrap().set_dirty(); // gen=2
        assert_eq!(table.get(ino).unwrap().dirty_generation, 2);
        assert!(!table.get_mut(ino).unwrap().clear_dirty_if(1)); // stale snapshot
        assert!(table.get(ino).unwrap().is_dirty()); // still dirty
    }

    #[test]
    fn apply_commit_clears_dirty_and_updates_metadata() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "test".to_string(),
            "test".to_string(),
            InodeKind::File,
            100,
            UNIX_EPOCH,
            Some("old_hash".to_string()),
            0o644,
            0,
            0,
        );
        let entry = table.get_mut(ino).unwrap();
        entry.set_dirty();
        entry.pending_deletes.push("old_path".to_string());
        let snap = entry.dirty_generation;

        entry.apply_commit("new_hash", 200, snap);

        assert!(!entry.is_dirty());
        assert_eq!(entry.xet_hash.as_deref(), Some("new_hash"));
        assert_eq!(entry.size, 200);
        assert!(entry.pending_deletes.is_empty());
        assert!(entry.mtime > UNIX_EPOCH);
    }

    #[test]
    fn apply_commit_preserves_state_on_generation_mismatch() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "test".to_string(),
            "test".to_string(),
            InodeKind::File,
            100,
            UNIX_EPOCH,
            Some("old_hash".to_string()),
            0o644,
            0,
            0,
        );
        let entry = table.get_mut(ino).unwrap();
        entry.set_dirty(); // gen=1
        entry.pending_deletes.push("old_path".to_string());
        entry.set_dirty(); // gen=2 (simulates concurrent writer)

        entry.apply_commit("new_hash", 200, 1); // stale snapshot

        // Generation mismatch: dirty stays, and size/hash must NOT be overwritten
        // (a concurrent writer may have newer content in staging).
        assert!(entry.is_dirty());
        assert_eq!(
            entry.xet_hash.as_deref(),
            Some("old_hash"),
            "hash should not be clobbered by stale flush"
        );
        assert_eq!(entry.size, 100, "size should not be clobbered by stale flush");
        assert_eq!(entry.pending_deletes.len(), 1, "pending_deletes should be preserved");
    }

    #[test]
    fn set_dirty_saturates() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "test".to_string(),
            "test".to_string(),
            InodeKind::File,
            0,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        let entry = table.get_mut(ino).unwrap();
        entry.dirty_generation = u64::MAX;
        entry.set_dirty();
        assert_eq!(entry.dirty_generation, u64::MAX); // saturated, not wrapped to 0
        assert!(entry.is_dirty());
    }

    #[test]
    fn test_remove() {
        let mut table = InodeTable::new(0);

        let ino = table.insert(
            ROOT_INODE,
            "remove_me.txt".to_string(),
            "remove_me.txt".to_string(),
            InodeKind::File,
            100,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Verify it exists in parent's children
        let root = table.get(ROOT_INODE).unwrap();
        assert!(root.children.iter().any(|c| c.ino == ino));

        // Verify path mapping exists
        assert!(table.get_by_path("remove_me.txt").is_some());

        // Remove it
        let removed = table.remove(ino);
        assert!(removed.is_some());
        let removed = removed.unwrap();
        assert_eq!(removed.inode, ino);
        assert_eq!(removed.name, "remove_me.txt");

        // Parent's children no longer contains the inode
        let root = table.get(ROOT_INODE).unwrap();
        assert!(!root.children.iter().any(|c| c.ino == ino));

        // Path mapping is gone
        assert!(table.get_by_path("remove_me.txt").is_none());

        // Direct lookup is gone
        assert!(table.get(ino).is_none());

        // Removing a non-existent inode returns None
        assert!(table.remove(9999).is_none());
    }

    #[test]
    fn test_remove_path_insert_path() {
        let mut table = InodeTable::new(0);

        let ino = table.insert(
            ROOT_INODE,
            "old_name.txt".to_string(),
            "old_name.txt".to_string(),
            InodeKind::File,
            50,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Verify original path works
        assert_eq!(table.get_by_path("old_name.txt").unwrap().inode, ino);

        // Simulate a rename: remove old path, insert new path
        table.remove_path("old_name.txt");
        assert!(table.get_by_path("old_name.txt").is_none());

        table.insert_path("new_name.txt".to_string(), ino);
        assert_eq!(table.get_by_path("new_name.txt").unwrap().inode, ino);

        // The inode itself still exists
        assert!(table.get(ino).is_some());
    }

    #[test]
    fn test_insert_duplicate_path() {
        let mut table = InodeTable::new(0);

        let ino1 = table.insert(
            ROOT_INODE,
            "dup.txt".to_string(),
            "dup.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Inserting the same full_path again should return the existing inode
        let ino2 = table.insert(
            ROOT_INODE,
            "dup.txt".to_string(),
            "dup.txt".to_string(),
            InodeKind::File,
            20,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        assert_eq!(ino1, ino2, "duplicate path insert should return existing inode");

        // The original entry should be unchanged (size still 10, not 20)
        assert_eq!(table.get(ino1).unwrap().size, 10);

        // Root should only have one child (not two)
        let root = table.get(ROOT_INODE).unwrap();
        assert_eq!(
            root.children.iter().filter(|c| c.ino == ino1).count(),
            1,
            "parent should have exactly one child reference"
        );
    }

    #[test]
    fn test_update_subtree_paths() {
        let mut table = InodeTable::new(0);

        // Build: root / old_dir / child.txt
        //                       / subdir / deep.txt
        let dir_ino = table.insert(
            ROOT_INODE,
            "old_dir".to_string(),
            "old_dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        let child_ino = table.insert(
            dir_ino,
            "child.txt".to_string(),
            "old_dir/child.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        let subdir_ino = table.insert(
            dir_ino,
            "subdir".to_string(),
            "old_dir/subdir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        let deep_ino = table.insert(
            subdir_ino,
            "deep.txt".to_string(),
            "old_dir/subdir/deep.txt".to_string(),
            InodeKind::File,
            5,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Rename old_dir → new_dir
        table.update_subtree_paths(dir_ino, "new_dir".to_string());

        // Verify all paths updated
        assert_eq!(table.get(dir_ino).unwrap().full_path.as_ref(), "new_dir");
        assert_eq!(table.get(child_ino).unwrap().full_path.as_ref(), "new_dir/child.txt");
        assert_eq!(table.get(subdir_ino).unwrap().full_path.as_ref(), "new_dir/subdir");
        assert_eq!(
            table.get(deep_ino).unwrap().full_path.as_ref(),
            "new_dir/subdir/deep.txt"
        );

        // Verify path_to_inode updated (old paths gone, new paths work)
        assert!(table.get_by_path("old_dir").is_none());
        assert!(table.get_by_path("old_dir/child.txt").is_none());
        assert!(table.get_by_path("old_dir/subdir/deep.txt").is_none());

        assert_eq!(table.get_by_path("new_dir").unwrap().inode, dir_ino);
        assert_eq!(table.get_by_path("new_dir/child.txt").unwrap().inode, child_ino);
        assert_eq!(table.get_by_path("new_dir/subdir/deep.txt").unwrap().inode, deep_ino);
    }

    #[test]
    fn test_get_dir_ino() {
        let mut table = InodeTable::new(0);

        // Root is a directory
        assert_eq!(table.get_dir_ino(""), Some(ROOT_INODE));

        // Insert a file — should NOT be returned by get_dir_ino
        let file_ino = table.insert(
            ROOT_INODE,
            "file.txt".to_string(),
            "file.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        assert!(table.get_dir_ino("file.txt").is_none());

        // Insert a directory — should be returned
        let dir_ino = table.insert(
            ROOT_INODE,
            "mydir".to_string(),
            "mydir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        assert_eq!(table.get_dir_ino("mydir"), Some(dir_ino));

        // Non-existent path
        assert!(table.get_dir_ino("nope").is_none());

        let _ = file_ino;
    }

    #[test]
    fn test_file_snapshot() {
        let mut table = InodeTable::new(0);

        let ino1 = table.insert(
            ROOT_INODE,
            "a.txt".to_string(),
            "a.txt".to_string(),
            InodeKind::File,
            100,
            UNIX_EPOCH,
            Some("hash_a".to_string()),
            0o644,
            0,
            0,
        );
        table.get_mut(ino1).unwrap().set_dirty();

        let ino2 = table.insert(
            ROOT_INODE,
            "b.txt".to_string(),
            "b.txt".to_string(),
            InodeKind::File,
            200,
            UNIX_EPOCH,
            Some("hash_b".to_string()),
            0o644,
            0,
            0,
        );

        // Insert a directory — should NOT appear in file_snapshot
        table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        let snapshot = table.file_snapshot();
        assert_eq!(snapshot.len(), 2, "only files, not directories");

        let a = snapshot.iter().find(|(ino, ..)| *ino == ino1).unwrap();
        assert_eq!(a.1, "a.txt");
        assert_eq!(a.2, Some("hash_a".to_string()));
        assert_eq!(a.3, None); // etag
        assert_eq!(a.4, 100);
        assert!(a.5, "a.txt should be dirty");

        let b = snapshot.iter().find(|(ino, ..)| *ino == ino2).unwrap();
        assert_eq!(b.1, "b.txt");
        assert!(!b.5, "b.txt should not be dirty");
    }

    #[test]
    fn test_update_remote_file() {
        let mut table = InodeTable::new(0);

        let ino = table.insert(
            ROOT_INODE,
            "remote.txt".to_string(),
            "remote.txt".to_string(),
            InodeKind::File,
            100,
            UNIX_EPOCH,
            Some("old_hash".to_string()),
            0o644,
            0,
            0,
        );

        let new_mtime = UNIX_EPOCH + std::time::Duration::from_secs(1000);

        // Update succeeds on non-dirty file
        assert!(table.update_remote_file(ino, Some("new_hash".to_string()), None, 200, new_mtime));
        let entry = table.get(ino).unwrap();
        assert_eq!(entry.xet_hash, Some("new_hash".to_string()));
        assert_eq!(entry.size, 200);
        assert_eq!(entry.mtime, new_mtime);

        // Mark dirty — update should fail
        table.get_mut(ino).unwrap().set_dirty();
        assert!(!table.update_remote_file(ino, Some("ignored".to_string()), None, 999, UNIX_EPOCH));
        assert_eq!(table.get(ino).unwrap().size, 200, "dirty file should not be updated");

        // Non-existent inode
        assert!(!table.update_remote_file(9999, None, None, 0, UNIX_EPOCH));
    }

    #[test]
    fn test_invalidate_children() {
        let mut table = InodeTable::new(0);

        let dir_ino = table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        // Mark as loaded
        table.get_mut(dir_ino).unwrap().children_loaded = true;
        assert!(table.get(dir_ino).unwrap().children_loaded);

        // Invalidate
        table.invalidate_children(dir_ino);
        assert!(!table.get(dir_ino).unwrap().children_loaded);

        // Invalidating non-existent inode is a no-op
        table.invalidate_children(9999);
    }

    #[test]
    fn test_pending_deletes() {
        let mut table = InodeTable::new(0);

        let ino = table.insert(
            ROOT_INODE,
            "file.txt".to_string(),
            "file.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            Some("hash".to_string()),
            0o644,
            0,
            0,
        );

        // Initially empty
        assert!(table.get(ino).unwrap().pending_deletes.is_empty());

        // Simulate rename recording old path
        table
            .get_mut(ino)
            .unwrap()
            .pending_deletes
            .push("old_path.txt".to_string());

        assert_eq!(table.get(ino).unwrap().pending_deletes, vec!["old_path.txt"]);

        // Clear after flush
        table.get_mut(ino).unwrap().pending_deletes.clear();
        assert!(table.get(ino).unwrap().pending_deletes.is_empty());
    }

    #[test]
    fn test_remove_non_empty_dir_cleans_descendants() {
        let mut table = InodeTable::new(0);

        // Build: root / dir / child.txt
        //                    / subdir / deep.txt
        let dir_ino = table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let child_ino = table.insert(
            dir_ino,
            "child.txt".to_string(),
            "dir/child.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        let subdir_ino = table.insert(
            dir_ino,
            "subdir".to_string(),
            "dir/subdir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let deep_ino = table.insert(
            subdir_ino,
            "deep.txt".to_string(),
            "dir/subdir/deep.txt".to_string(),
            InodeKind::File,
            5,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Remove the top-level dir
        let removed = table.remove(dir_ino);
        assert!(removed.is_some());

        // All descendants must be gone from both maps
        assert!(table.get(dir_ino).is_none());
        assert!(table.get(child_ino).is_none());
        assert!(table.get(subdir_ino).is_none());
        assert!(table.get(deep_ino).is_none());
        assert!(table.get_by_path("dir").is_none());
        assert!(table.get_by_path("dir/child.txt").is_none());
        assert!(table.get_by_path("dir/subdir").is_none());
        assert!(table.get_by_path("dir/subdir/deep.txt").is_none());

        // Root no longer references the removed dir
        assert!(!table.get(ROOT_INODE).unwrap().children.iter().any(|c| c.ino == dir_ino));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "parent inode")]
    fn test_insert_with_missing_parent_panics() {
        let mut table = InodeTable::new(0);
        // Parent inode 999 does not exist — should panic in debug
        table.insert(
            999,
            "orphan.txt".to_string(),
            "orphan.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "different kind")]
    fn test_insert_duplicate_path_different_kind_panics() {
        let mut table = InodeTable::new(0);
        table.insert(
            ROOT_INODE,
            "name".to_string(),
            "name".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        // Same path but Directory kind — should panic in debug
        table.insert(
            ROOT_INODE,
            "name".to_string(),
            "name".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
    }

    #[test]
    fn test_dirty_inos_excludes_directories() {
        let mut table = InodeTable::new(0);
        let file_ino = table.insert(
            ROOT_INODE,
            "dirty.txt".to_string(),
            "dirty.txt".to_string(),
            InodeKind::File,
            1,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        let dir_ino = table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        table.get_mut(file_ino).unwrap().set_dirty();
        table.get_mut(dir_ino).unwrap().set_dirty();

        let dirty = table.dirty_inos();
        assert_eq!(dirty, vec![file_ino]);
    }

    #[test]
    fn test_update_subtree_paths_missing_inode_is_noop() {
        let mut table = InodeTable::new(0);
        table.insert(
            ROOT_INODE,
            "file.txt".to_string(),
            "file.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        let before = table.file_snapshot();
        table.update_subtree_paths(999_999, "does/not/matter".to_string());
        let after = table.file_snapshot();

        assert_eq!(before, after);
        assert!(table.get_by_path("file.txt").is_some());
    }

    #[test]
    fn test_remove_directory_with_already_removed_child() {
        let mut table = InodeTable::new(0);
        let dir_ino = table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let child_ino = table.insert(
            dir_ino,
            "child.txt".to_string(),
            "dir/child.txt".to_string(),
            InodeKind::File,
            1,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Simulate a stale children list entry in parent by removing child first.
        table.remove(child_ino).unwrap();
        assert!(table.get(child_ino).is_none());

        // Removing parent should still succeed and clean mappings.
        table.remove(dir_ino).unwrap();
        assert!(table.get(dir_ino).is_none());
        assert!(table.get_by_path("dir").is_none());
    }

    #[test]
    fn test_remove_directory_adjusts_parent_nlink() {
        let mut table = InodeTable::new(0);
        let nlink_before = table.get(ROOT_INODE).unwrap().nlink;

        let dir_ino = table.insert(
            ROOT_INODE,
            "sub".to_string(),
            "sub".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        // insert() increments parent nlink for new subdirectory
        assert_eq!(table.get(ROOT_INODE).unwrap().nlink, nlink_before + 1);

        // remove() should decrement it back
        table.remove(dir_ino);
        assert_eq!(table.get(ROOT_INODE).unwrap().nlink, nlink_before);
    }

    #[test]
    fn test_move_child_same_parent() {
        let mut table = InodeTable::new(0);

        let file_ino = table.insert(
            ROOT_INODE,
            "old.txt".to_string(),
            "old.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        let nlink_before = table.get(ROOT_INODE).unwrap().nlink;

        table.move_child(file_ino, ROOT_INODE, "old.txt", ROOT_INODE, "new.txt");

        // Child removed under old name, added under new name
        assert!(table.lookup_child(ROOT_INODE, "old.txt").is_none());
        assert_eq!(table.lookup_child(ROOT_INODE, "new.txt").unwrap().inode, file_ino);

        // name/parent updated on the inode
        let entry = table.get(file_ino).unwrap();
        assert_eq!(entry.name, "new.txt");
        assert_eq!(entry.parent, ROOT_INODE);

        // nlink unchanged (same parent, file not directory)
        assert_eq!(table.get(ROOT_INODE).unwrap().nlink, nlink_before);
    }

    #[test]
    fn test_move_child_cross_parent_file() {
        let mut table = InodeTable::new(0);

        let dir_a = table.insert(
            ROOT_INODE,
            "a".to_string(),
            "a".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let dir_b = table.insert(
            ROOT_INODE,
            "b".to_string(),
            "b".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let file_ino = table.insert(
            dir_a,
            "f.txt".to_string(),
            "a/f.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        let nlink_a = table.get(dir_a).unwrap().nlink;
        let nlink_b = table.get(dir_b).unwrap().nlink;

        table.move_child(file_ino, dir_a, "f.txt", dir_b, "g.txt");

        // Detached from dir_a, attached to dir_b
        assert!(table.lookup_child(dir_a, "f.txt").is_none());
        assert_eq!(table.lookup_child(dir_b, "g.txt").unwrap().inode, file_ino);

        // name/parent updated
        let entry = table.get(file_ino).unwrap();
        assert_eq!(entry.name, "g.txt");
        assert_eq!(entry.parent, dir_b);

        // nlink unchanged on both parents (file move, not directory)
        assert_eq!(table.get(dir_a).unwrap().nlink, nlink_a);
        assert_eq!(table.get(dir_b).unwrap().nlink, nlink_b);
    }

    #[test]
    fn test_move_child_cross_parent_directory() {
        let mut table = InodeTable::new(0);

        let dir_a = table.insert(
            ROOT_INODE,
            "a".to_string(),
            "a".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let dir_b = table.insert(
            ROOT_INODE,
            "b".to_string(),
            "b".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let sub = table.insert(
            dir_a,
            "sub".to_string(),
            "a/sub".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        let nlink_a = table.get(dir_a).unwrap().nlink;
        let nlink_b = table.get(dir_b).unwrap().nlink;

        table.move_child(sub, dir_a, "sub", dir_b, "sub");

        // nlink: old parent --, new parent ++
        assert_eq!(table.get(dir_a).unwrap().nlink, nlink_a - 1);
        assert_eq!(table.get(dir_b).unwrap().nlink, nlink_b + 1);
    }

    #[test]
    fn test_touch_parent() {
        let mut table = InodeTable::new(0);
        let before = table.get(ROOT_INODE).unwrap().mtime;

        let now = UNIX_EPOCH + std::time::Duration::from_secs(12345);
        table.touch_parent(ROOT_INODE, now);

        let root = table.get(ROOT_INODE).unwrap();
        assert_eq!(root.mtime, now);
        assert_eq!(root.ctime, now);
        assert_ne!(root.mtime, before);
    }

    #[test]
    fn test_unlink_one_basic() {
        let mut table = InodeTable::new(0);

        let file_ino = table.insert(
            ROOT_INODE,
            "f.txt".to_string(),
            "f.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        let result = table.unlink_one(ROOT_INODE, "f.txt");
        assert!(result.is_some());
        let (last_link, snapshot) = result.unwrap();
        assert!(last_link, "should be last link");
        assert_eq!(snapshot.inode, file_ino);
        assert_eq!(snapshot.nlink, 0);

        // Child removed from parent
        assert!(table.lookup_child(ROOT_INODE, "f.txt").is_none());
        // Path mapping removed
        assert!(table.get_by_path("f.txt").is_none());
    }

    #[test]
    fn test_unlink_one_last_link() {
        let mut table = InodeTable::new(0);

        let file_ino = table.insert(
            ROOT_INODE,
            "doomed.txt".to_string(),
            "doomed.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        let (last_link, _) = table.unlink_one(ROOT_INODE, "doomed.txt").unwrap();
        assert!(last_link);

        // Inode still in table (for open file handles), but nlink == 0
        let entry = table.get(file_ino).unwrap();
        assert_eq!(entry.nlink, 0);

        // remove_orphan should clean up both inodes map and path_to_inode
        let path = entry.full_path.clone();
        table.remove_orphan(file_ino);
        assert!(table.get(file_ino).is_none());
        assert!(table.get_by_path(&path).is_none(), "path_to_inode should be cleaned up");
    }

    #[test]
    fn test_nlookup_starts_at_zero() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "f.txt".to_string(),
            "f.txt".to_string(),
            InodeKind::File,
            0,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        assert_eq!(table.get(ino).unwrap().eviction.nlookup.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_nlookup_bump_drop() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "f.txt".to_string(),
            "f.txt".to_string(),
            InodeKind::File,
            0,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        table.bump_nlookup(ino);
        table.bump_nlookup(ino);
        table.bump_nlookup(ino);
        assert_eq!(table.get(ino).unwrap().eviction.nlookup.load(Ordering::Relaxed), 3);

        assert!(!table.drop_nlookup(ino, 1));
        assert_eq!(table.get(ino).unwrap().eviction.nlookup.load(Ordering::Relaxed), 2);

        // Reaching zero returns true.
        assert!(table.drop_nlookup(ino, 2));
        assert_eq!(table.get(ino).unwrap().eviction.nlookup.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_nlookup_saturates_on_underflow() {
        // debug_assert! fires in debug builds, so we can only exercise the
        // saturation branch in release.
        if cfg!(debug_assertions) {
            return;
        }
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "f.txt".to_string(),
            "f.txt".to_string(),
            InodeKind::File,
            0,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        assert!(table.drop_nlookup(ino, 42));
        assert_eq!(table.get(ino).unwrap().eviction.nlookup.load(Ordering::Relaxed), 0);

        table.bump_nlookup(ino);
        assert!(table.drop_nlookup(ino, 1000));
        assert_eq!(table.get(ino).unwrap().eviction.nlookup.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_nlookup_unknown_inode_drop_returns_false() {
        let table = InodeTable::new(0);
        assert!(!table.drop_nlookup(9999, 1));
    }

    #[test]
    fn test_evict_if_safe_removes_clean_file() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "f.txt".to_string(),
            "f.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Default nlookup is 0, not dirty, no pending deletes → safe to evict.
        assert!(table.evict_if_safe(ino));
        assert!(table.get(ino).is_none());
        assert!(table.get_by_path("f.txt").is_none());

        // Parent's children_loaded is reset so readdir will re-fetch.
        let root = table.get(ROOT_INODE).unwrap();
        assert!(!root.children_loaded);
        assert!(!root.children.iter().any(|c| c.ino == ino));
    }

    #[test]
    fn test_evict_if_safe_refuses_when_nlookup_held() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "f.txt".to_string(),
            "f.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        table.bump_nlookup(ino);

        assert!(!table.evict_if_safe(ino));
        assert!(
            table.get(ino).is_some(),
            "entry must survive when kernel still holds dentry"
        );
    }

    #[test]
    fn test_evict_if_safe_refuses_when_dirty() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "f.txt".to_string(),
            "f.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        table.get_mut(ino).unwrap().set_dirty();

        assert!(!table.evict_if_safe(ino));
        assert!(
            table.get(ino).is_some(),
            "dirty entry must not be evicted — unflushed data"
        );
    }

    #[test]
    fn test_evict_if_safe_refuses_when_pending_deletes() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "f.txt".to_string(),
            "f.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        table.get_mut(ino).unwrap().pending_deletes.push("old_path".to_string());

        assert!(!table.evict_if_safe(ino));
        assert!(table.get(ino).is_some());
    }

    #[test]
    fn test_evict_if_safe_refuses_directory() {
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "d".to_string(),
            "d".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        assert!(!table.evict_if_safe(ino), "directories must never be evicted");
        assert!(table.get(ino).is_some());
    }

    #[test]
    fn test_evict_if_safe_refuses_unlinked_orphan() {
        // An unlinked-but-still-open file (nlink == 0) must stay in the table
        // until release() cleans it up via remove_orphan — evicting it would
        // drop the xet_hash / path a racing read still needs.
        let mut table = InodeTable::new(0);
        let ino = table.insert(
            ROOT_INODE,
            "doomed.txt".to_string(),
            "doomed.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        table.unlink_one(ROOT_INODE, "doomed.txt");
        assert_eq!(table.get(ino).unwrap().nlink, 0);

        assert!(!table.evict_if_safe(ino));
        assert!(table.get(ino).is_some());
    }

    #[test]
    fn test_evict_if_safe_unknown_inode() {
        let mut table = InodeTable::new(0);
        assert!(!table.evict_if_safe(9999));
    }

    // ── insert / touch ──────────────────────────────────────────────

    fn mk_table_with_soft_limit(soft: usize) -> InodeTable {
        InodeTable::new(soft)
    }

    fn mk_file(table: &mut InodeTable, name: &str) -> u64 {
        table.insert(
            ROOT_INODE,
            name.to_string(),
            name.to_string(),
            InodeKind::File,
            1,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        )
    }

    #[test]
    fn test_insert_never_evicts_even_over_cap() {
        // insert() no longer evicts synchronously (see `soft_limit` doc):
        // the async sweep is the sole backstop. So the table is free to
        // overshoot the soft limit; every inserted entry must survive.
        let mut table = mk_table_with_soft_limit(100);
        for i in 0..500 {
            mk_file(&mut table, &format!("f{i}.txt"));
        }
        assert_eq!(table.len(), 1 + 500, "insert must not evict");
    }

    #[test]
    fn test_soft_limit_zero_gates_touch() {
        // touch() is a no-op when LRU is disabled (soft_limit==0), so the
        // hot path stays cheap in the common case.
        let mut table = InodeTable::new(0);
        let f = mk_file(&mut table, "f.txt");
        let before = table.get(f).unwrap().eviction.last_touched.load(Ordering::Relaxed);
        table.touch(f);
        let after = table.get(f).unwrap().eviction.last_touched.load(Ordering::Relaxed);
        assert_eq!(before, after, "touch on soft_limit=0 table must be a no-op");
    }

    #[test]
    fn test_soft_limit_nonzero_enables_touch() {
        let mut table = InodeTable::new(100);
        let f = mk_file(&mut table, "f.txt");
        let before = table.get(f).unwrap().eviction.last_touched.load(Ordering::Relaxed);
        table.touch(f);
        let after = table.get(f).unwrap().eviction.last_touched.load(Ordering::Relaxed);
        assert_ne!(before, after, "touch on LRU-enabled table must bump last_touched");
    }

    // ── evict_pending (deferred eviction on close) ──────────────────

    #[test]
    fn test_evict_pending_flag_toggle() {
        let mut table = InodeTable::new(0);
        let f = mk_file(&mut table, "f.txt");
        assert!(!table.take_evict_pending(f), "flag starts false");
        table.mark_evict_pending(f);
        assert!(table.take_evict_pending(f), "flag now true");
        assert!(!table.take_evict_pending(f), "take() clears the flag");
    }

    #[test]
    fn test_evict_pending_unknown_inode() {
        let table = InodeTable::new(0);
        table.mark_evict_pending(9999); // no-op, must not panic
        assert!(!table.take_evict_pending(9999));
    }

    // ── open handles refcount ──────────────────────────────────────

    #[test]
    fn test_open_handles_refcount_tracking() {
        let mut table = InodeTable::new(0);
        let busy = mk_file(&mut table, "busy.txt");

        assert!(!table.has_open_handles(busy));
        table.bump_open_handles(busy);
        assert!(table.has_open_handles(busy));
        table.drop_open_handles(busy);
        assert!(!table.has_open_handles(busy));
    }
}
