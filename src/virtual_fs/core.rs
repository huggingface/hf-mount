use super::*;

impl VirtualFs {
    pub fn new(
        runtime: tokio::runtime::Handle,
        hub_client: Arc<dyn HubOps>,
        xet_sessions: Arc<dyn XetOps>,
        staging_dir: Option<StagingDir>,
        file_cache: Option<Arc<FileCache>>,
        overlay_backing: Option<OverlayBacking>,
        config: VfsConfig,
    ) -> Arc<Self> {
        let inodes = Arc::new(RwLock::new(InodeTable::new(config.inode_soft_limit > 0)));
        let negative_cache = Arc::new(RwLock::new(HashMap::new()));

        let staging = Arc::new(StagingCoordinator::new(staging_dir));
        let overlay_backing = overlay_backing.map(Arc::new);
        let overlay = overlay_backing.is_some();

        // Staging GC orders eviction by `last_touched`. When the inode evictor
        // is off (`soft_limit==0`) the touch hook is a no-op, so arm it
        // explicitly when a staging budget is configured — otherwise the GC
        // would fall back to arbitrary HashMap order.
        if staging.dir().is_some_and(|sd| sd.has_budget()) {
            inodes.read().expect("inodes poisoned").enable_touch_tracking();
        }

        // Overlay mode keeps writes local: skip the flush pipeline entirely.
        let flush_manager = if !config.read_only && config.advanced_writes && !overlay {
            staging.dir().expect("--advanced-writes requires a staging directory");
            Some(flush::FlushManager::new(
                xet_sessions.clone(),
                staging.clone(),
                hub_client.clone(),
                inodes.clone(),
                &runtime,
                config.flush_debounce,
                config.flush_max_batch_window,
            ))
        } else {
            None
        };

        // Create open_files before poll task so we can share with it
        let open_files: Arc<RwLock<HashMap<u64, OpenFile>>> = Arc::new(RwLock::new(HashMap::new()));

        // Spawn remote change polling task (if interval > 0)
        let invalidator: Invalidator = Arc::new(OnceLock::new());
        let poll_handle = if config.poll_interval_secs > 0 {
            let bg_hub = hub_client.clone();
            let bg_inodes = inodes.clone();
            let bg_neg_cache = negative_cache.clone();
            let bg_invalidator = invalidator.clone();
            let interval = Duration::from_secs(config.poll_interval_secs);
            // Clamp to >= 1 in case a caller (library use) constructs VfsConfig directly.
            let listing_concurrency = config.poll_listing_concurrency.max(1);

            Some(runtime.spawn(Self::poll_remote_changes(
                bg_hub,
                bg_inodes,
                bg_neg_cache,
                bg_invalidator,
                interval,
                listing_concurrency,
            )))
        } else {
            None
        };

        let entry_invalidator: EntryInvalidator = Arc::new(OnceLock::new());
        let vfs = Arc::new(Self {
            runtime,
            hub_client,
            xet_sessions,
            staging,
            overlay_backing,
            read_only: config.read_only,
            // Overlay implies advanced_writes (random writes via local backing file).
            advanced_writes: config.advanced_writes || overlay,
            inode_table: inodes,
            open_files,
            next_file_handle: AtomicU64::new(1),
            uid: config.uid,
            gid: config.gid,
            negative_cache,
            dir_loading_locks: Mutex::new(HashMap::new()),
            pending_commits: Mutex::new(HashMap::new()),
            flush_manager,
            poll_handle: Mutex::new(poll_handle),
            invalidator,
            entry_invalidator,
            lru_handle: Mutex::new(None),
            metadata_ttl: config.metadata_ttl,
            serve_lookup_from_cache: config.serve_lookup_from_cache,
            filter_os_files: config.filter_os_files,
            direct_io: config.direct_io,
            file_cache,
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

        // Pre-load root directory so `ls /mount` is instant. The LRU cap is
        // armed at `InodeTable::new(soft_limit)` above, so a flat root with
        // thousands of direct children won't blow past the budget here.
        // Subdirectories are lazy-loaded on first access.
        if let Err(e) = vfs.runtime.block_on(vfs.ensure_children_loaded(inode::ROOT_INODE)) {
            error!("Failed to pre-load root directory: errno={}", e);
        }

        // Spawn LRU evictor. `Weak` so the task exits when the outer Arc is
        // dropped; still aborted in shutdown() for determinism.
        if config.inode_soft_limit > 0 {
            let weak = Arc::downgrade(&vfs);
            let soft_limit = config.inode_soft_limit;
            let sweep_interval = config.lru_sweep_interval;
            let handle = vfs
                .runtime
                .spawn(Self::lru_sweep_loop(weak, soft_limit, sweep_interval));
            *vfs.lru_handle.lock().expect("lru_handle poisoned") = Some(handle);
            info!(
                "LRU evictor enabled: soft_limit={} inodes, sweep every {:?}",
                soft_limit, sweep_interval
            );
        }

        vfs
    }
    /// True when the VFS is in overlay mode (writes stay local, no remote
    /// mutations). Derived from the presence of an overlay backing.
    pub(super) fn overlay(&self) -> bool {
        self.overlay_backing.is_some()
    }

    /// Splice local overlay entries into the inode table for `parent_ino`,
    /// overriding remote entries on name conflict (kind mismatch removes the
    /// remote inode first, then re-inserts as local). No-op when not in
    /// overlay mode.
    pub(super) fn merge_overlay_entries(&self, inodes: &mut InodeTable, parent_ino: u64) {
        let Some(overlay) = &self.overlay_backing else {
            return;
        };
        let dir_path = inodes.get(parent_ino).map(|e| e.full_path.clone()).unwrap_or_default();
        let Ok(entries) = overlay.read_dir(&dir_path) else {
            return;
        };
        for entry in entries {
            if entry.is_symlink || (self.filter_os_files && is_os_junk(&entry.name)) {
                continue;
            }
            let kind = if entry.is_dir {
                InodeKind::Directory
            } else {
                InodeKind::File
            };
            let full_path = inode::child_path(&dir_path, &entry.name);
            // If existing entry has a different kind (e.g. remote file vs
            // local dir), remove it first so local wins cleanly.
            let conflict = inodes
                .lookup_child(parent_ino, &entry.name)
                .filter(|e| e.kind != kind)
                .map(|e| e.inode);
            if let Some(old_ino) = conflict {
                inodes.remove(old_ino);
            }
            let ino = inodes.insert(
                parent_ino,
                entry.name,
                full_path,
                kind,
                entry.size,
                entry.mtime,
                None,
                entry.mode,
                self.uid,
                self.gid,
            );
            // Local overrides remote: clear remote identity, mark dirty.
            if let Some(e) = inodes.get_mut(ino) {
                e.size = entry.size;
                e.mtime = entry.mtime;
                e.mode = entry.mode;
                e.xet_hash = None;
                if !e.is_dirty() {
                    e.set_dirty();
                }
                if kind == InodeKind::Directory {
                    e.children_loaded_at = None;
                }
            }
        }
    }

    /// In overlay mode, signal that `full_path` exists locally and a remote
    /// HEAD/list probe must be skipped to preserve local-overrides-remote.
    /// On hit, re-runs `merge_overlay_entries` on the parent so the entry
    /// shows up in the inode table even when it was added to the backing
    /// after the parent's listing was loaded.
    pub(super) fn overlay_lookup_takes_precedence(&self, parent: u64, full_path: &str) -> VirtualFsResult<bool> {
        let Some(overlay) = &self.overlay_backing else {
            return Ok(false);
        };
        let exists = overlay.exists(full_path).map_err(|e| {
            error!("Failed to stat overlay path {}: {}", full_path, e);
            libc::EIO
        })?;
        if exists {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            self.merge_overlay_entries(&mut inodes, parent);
        }
        Ok(exists)
    }

    /// True if the entry is a clean remote entry that overlay mode treats as immutable.
    pub(super) fn is_overlay_immutable(&self, entry: &inode::InodeEntry) -> bool {
        self.overlay() && !entry.is_dirty()
    }

    /// Set the kernel cache invalidation callback. Called after mount setup
    /// so the poll loop can actively invalidate stale inodes on remote changes.
    /// First call wins; later calls are no-ops.
    pub fn set_invalidator(&self, f: InvalidatorFn) {
        let _ = self.invalidator.set(f);
    }

    /// First call wins; later calls are no-ops.
    pub fn set_entry_invalidator(&self, f: EntryInvalidatorFn) {
        let _ = self.entry_invalidator.set(f);
    }

    pub(super) async fn lru_sweep_loop(weak: std::sync::Weak<Self>, soft_limit: usize, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            let Some(vfs) = weak.upgrade() else { return };
            let table_len = vfs.inode_table.read().expect("inodes poisoned").len();
            let evicted = vfs.lru_evict_sweep(soft_limit).await;
            if evicted > 0 || table_len > soft_limit {
                info!(
                    "lru_sweep: table={} soft_limit={} evicted={}",
                    table_len, soft_limit, evicted
                );
            }
        }
    }

    /// Candidates are collected under a read lock, then released before
    /// invoking `cb` — `cb` writes to the FUSE notify channel and returns
    /// `false` on EAGAIN/ENOMEM, which acts as the natural batch throttle.
    ///
    /// After `inval_entry`, we also evict our own table entry if `evict_if_safe`
    /// lets us: when the inode was materialized from a readdir the kernel
    /// never cached a dentry for it (nlookup stays at 0), so no `forget()`
    /// will ever come back. Waiting on one leaks the entry forever.
    ///
    /// The invalidation loop runs on `spawn_blocking` in chunks of
    /// `INVAL_BATCH_SIZE` with a `INVAL_BATCH_PAUSE` between chunks so the
    /// kernel's reverse-notify path doesn't starve concurrent FUSE ops.
    pub(super) async fn lru_evict_sweep(&self, soft_limit: usize) -> usize {
        let candidates = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let len = inodes.len();
            if len <= soft_limit {
                return 0;
            }
            inodes.lru_candidates(len - soft_limit)
        };
        if candidates.is_empty() {
            return 0;
        }
        if self.entry_invalidator.get().is_none() {
            return 0;
        }

        let mut to_evict = Vec::with_capacity(candidates.len());
        let mut iter = candidates.into_iter();
        loop {
            let chunk: Vec<(u64, u64, Arc<str>)> = iter.by_ref().take(INVAL_BATCH_SIZE).collect();
            if chunk.is_empty() {
                break;
            }
            // A short chunk means the iterator was exhausted while filling
            // it — no need to pause after, there is nothing more to process.
            let is_last = chunk.len() < INVAL_BATCH_SIZE;
            let invalidator = self.entry_invalidator.clone();
            let result = tokio::task::spawn_blocking(move || {
                let Some(invalidate_entry) = invalidator.get() else {
                    return (Vec::new(), true);
                };
                let mut evicted = Vec::with_capacity(chunk.len());
                for (ino, parent, name) in chunk {
                    // `invalidate_entry` returns false when the FUSE notify
                    // channel is saturated (EAGAIN/ENOMEM) — backpressure,
                    // stop the sweep.
                    if !invalidate_entry(parent, &name) {
                        return (evicted, true);
                    }
                    evicted.push(ino);
                }
                (evicted, false)
            })
            .await;
            let stop = match result {
                Ok((mut chunk_inos, stop)) => {
                    to_evict.append(&mut chunk_inos);
                    stop
                }
                Err(e) => {
                    warn!("lru_sweep: invalidation batch joined with error: {e}");
                    true
                }
            };
            if stop || is_last {
                break;
            }
            // Pause so the kernel can drain reverse-notify and concurrent
            // FUSE ops can make progress.
            tokio::time::sleep(INVAL_BATCH_PAUSE).await;
        }

        if to_evict.is_empty() {
            return 0;
        }
        // Evict in chunks so the write lock is released between batches —
        // a single huge batch could hold it for hundreds of ms, stalling
        // every concurrent lookup / readdir / forget. `evict_batch_if_safe`
        // also collapses the per-eviction `parent.children.retain()` into
        // one pass per touched parent, which matters when many evicted
        // siblings share the same dir.
        const EVICT_CHUNK: usize = 4096;
        let mut total_evicted = 0;
        for chunk in to_evict.chunks(EVICT_CHUNK) {
            let evicted_inos = {
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                inodes.evict_batch_if_safe(chunk)
            };
            // Reclaim per-inode staging files, mirroring forget()/release().
            for ino in &evicted_inos {
                self.drop_staging(*ino);
            }
            total_evicted += evicted_inos.len();
        }
        total_evicted
    }

    /// Graceful shutdown: abort polling, drain flush queue, wait for completion.
    pub fn shutdown(&self) {
        info!("Shutting down VFS, flushing pending writes...");
        // Abort background tasks.
        if let Some(handle) = self.poll_handle.lock().expect("poll_handle poisoned").take() {
            handle.abort();
        }
        if let Some(handle) = self.lru_handle.lock().expect("lru_handle poisoned").take() {
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

    pub(super) fn make_vfs_attr(&self, entry: &InodeEntry) -> VirtualFsAttr {
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
    pub(super) async fn revalidate_file(&self, ino: u64, full_path: &str, current_hash: Option<&str>, current_etag: Option<&str>) {
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

        // Collect kernel invalidations to issue after we drop the inode_table
        // write lock. `inval_inode` issues a synchronous writev to /dev/fuse that
        // ends up in `invalidate_inode_pages2_range` on the kernel side, which
        // blocks on per-folio waits. Calling it under the global write lock can
        // stall the whole VFS (and has been observed to deadlock production
        // pods). Apply the mutation, drop the lock, then notify.
        let mut to_invalidate: Vec<u64> = Vec::new();
        match remote {
            None => {
                // File deleted remotely → remove from inode table
                info!("Remote deletion detected via HEAD: {}", full_path);
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                if let Some(entry) = inodes.get(ino) {
                    to_invalidate.push(entry.parent);
                    to_invalidate.push(ino);
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
                    to_invalidate.push(ino);
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

        // Drop happened at scope end above. Notify after the new state is
        // published so a racing lookup repopulating the kernel cache cannot
        // beat the invalidation.
        if !to_invalidate.is_empty()
            && let Some(invalidate) = self.invalidator.get()
        {
            for ino in to_invalidate {
                invalidate(ino);
            }
        }
    }

    /// Ensure children of a directory inode are loaded from the Hub API.
    /// Fetch remote children for `parent_ino` if not already loaded.
    /// Uses a per-directory lock to prevent thundering herd: concurrent callers
    /// for the same directory wait on the lock rather than making duplicate HTTP calls.
    /// Returns ENOENT if the inode doesn't exist, ENOTDIR if it's not a directory.
    pub(super) async fn ensure_children_loaded(&self, parent_ino: u64) -> VirtualFsResult<()> {
        // Fast path: already loaded (no lock needed).
        {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            match inodes.get(parent_ino) {
                Some(e) if e.kind != InodeKind::Directory => return Err(libc::ENOTDIR),
                Some(e) if e.children_loaded() => return Ok(()),
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
                Some(e) if e.children_loaded() => return Ok(()),
                Some(e) => e.full_path.to_string(),
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
            Some(e) if e.children_loaded() => return Ok(()),
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
                    let in_listing = seen_names.contains(&*c.name) || seen_dirs.contains(&*c.name);
                    if in_listing {
                        return false;
                    }
                    inodes.get(c.ino).is_some_and(|e| !e.is_dirty())
                })
                .map(|c| c.ino)
                .collect();
            for ino in stale {
                if !inodes.has_dirty_or_open_descendants(ino) {
                    inodes.remove(ino);
                }
            }
        }

        self.merge_overlay_entries(&mut inodes, parent_ino);

        if let Some(parent) = inodes.get_mut(parent_ino) {
            // Vec growth doubles capacity; for a one-shot readdir of a stable
            // directory we'd otherwise keep ~50% slack forever. Trim now,
            // since regrowth on rare child mutations is cheap.
            parent.children.shrink_to_fit();
            parent.children_loaded_at = Some(Instant::now());
            parent.children_from_remote = true;
        }
        Ok(())
    }

    /// Recursively load all descendants of a directory so in-memory state
    /// is complete. Used before directory rename to build accurate remote ops.
    pub(super) async fn ensure_subtree_loaded(&self, ino: u64) -> VirtualFsResult<()> {
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

}
