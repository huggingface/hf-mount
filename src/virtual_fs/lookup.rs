use super::*;

impl VirtualFs {
    /// Check if a path is in the negative cache (and not expired).
    pub(super) fn negative_cache_check(&self, path: &str) -> bool {
        let cache = self.negative_cache.read().expect("negative_cache poisoned");
        matches!(cache.get(path), Some(inserted) if inserted.elapsed() < NEG_CACHE_TTL)
    }

    /// Remove a path from the negative cache (e.g. after create/rename).
    pub(super) fn negative_cache_remove(&self, path: &str) {
        self.negative_cache.write().expect("neg_cache poisoned").remove(path);
    }

    /// Insert a path into the negative cache, evicting if at capacity.
    /// Eviction is amortized: instead of scanning all entries, we sample a
    /// bounded batch to keep the write lock duration constant regardless of cache size.
    pub(super) fn negative_cache_insert(&self, path: String) {
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
                full_path: Arc<str>,
                current_hash: Option<String>,
                current_etag: Option<String>,
            },
            Miss {
                full_path: String,
                /// True when the parent has no remote presence (locally
                /// created in this session); HEAD/list_tree probes are
                /// pointless and can be skipped.
                local_only: bool,
            },
            NotLoaded,
        }
        let fast = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let parent_entry = inodes.get(parent).ok_or(libc::ENOENT)?;

            if parent_entry.kind != InodeKind::Directory {
                return Err(libc::ENOTDIR);
            }

            // Try in-memory first regardless of `children_loaded`: an inode
            // inserted via the HEAD point-lookup path or still present after
            // a partial eviction can be served without a full re-list.
            //
            // For directories we still require `children_loaded` to trust the
            // cached entry: without the parent listing, we can't tell whether
            // the dir was removed or replaced remotely (files revalidate
            // individually via HEAD, dirs don't).
            match inodes.lookup_child(parent, name) {
                // Clean cached file: HEAD-revalidate (gated by `metadata_ttl`)
                // so size/hash/mtime stay fresh between poll cycles.
                Some(entry) if entry.kind == InodeKind::File && !entry.is_dirty() => FastResult::NeedsRevalidation {
                    ino: entry.inode,
                    full_path: entry.full_path.clone(),
                    current_hash: entry.xet_hash.clone(),
                    current_etag: entry.etag.clone(),
                },
                // Either a dirty file (local writes win until flushed) or any
                // entry under a fully-listed parent we can trust as-is.
                Some(entry) if entry.kind == InodeKind::File || parent_entry.children_loaded() => {
                    FastResult::Hit(self.make_vfs_attr(entry))
                }
                // Cached non-file under an unloaded parent: we can't HEAD-probe
                // a directory to check it still exists (resolve endpoint only
                // serves files), so defer to the slow path which lists.
                Some(_) => FastResult::NotLoaded,
                // No cached entry but the parent listing is authoritative →
                // the name really doesn't exist; populate the negative cache.
                None if parent_entry.children_loaded() => {
                    let parent_path = &parent_entry.full_path;
                    let full_path = if parent_path.is_empty() {
                        name.to_string()
                    } else {
                        format!("{}/{}", parent_path, name)
                    };
                    // Skip the HEAD-on-miss revalidation when the listing was
                    // just refreshed: workloads that rapidly look up unique
                    // names (tarball extract, build systems, xfstests) would
                    // otherwise pay one HEAD + list_tree per name despite the
                    // cache being authoritative.
                    FastResult::Miss {
                        full_path,
                        local_only: !parent_entry.children_from_remote,
                    }
                }
                // No entry, no listing → slow path (HEAD then list_tree).
                None => FastResult::NotLoaded,
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
            FastResult::Miss { full_path, local_only } => {
                // A cached listing can still hide entries added remotely after
                // we listed. Re-validate before serving ENOENT, gated by the
                // negative cache so repeated misses stay free.
                if self.negative_cache_check(&full_path) {
                    return Err(libc::ENOENT);
                }
                // Skip HEAD/list probes for purely-local directories
                // (locally-mkdir'd this session, never seen on remote):
                // there's no remote state to discover, so the cached listing
                // is authoritative. Hot path for tarball extract / xfstests
                // creating thousands of unique names under fresh dirs.
                //
                // Do NOT seed the global negative cache here — its TTL is
                // longer than metadata_ttl, and remote churn could materialize
                // these names while the entry hides them. The listing itself
                // is the cache for these misses.
                if local_only {
                    return Err(libc::ENOENT);
                }
                // Overlay precedence: a file added to the overlay backing
                // after the parent listing was loaded must shadow whatever the
                // remote HEAD returns. Otherwise a clean remote inode wins,
                // and a later open(write) would truncate the local backing.
                if self.overlay_lookup_takes_precedence(parent, &full_path)? {
                    let inodes = self.inode_table.read().expect("inodes poisoned");
                    return inodes
                        .lookup_child(parent, name)
                        .map(|e| self.make_vfs_attr(e))
                        .ok_or(libc::ENOENT);
                }
                // HEAD probe catches files added remotely.
                if let Ok(Some(head)) = self.hub_client.head_file(&full_path).await
                    && head.size.is_some()
                {
                    return self.insert_file_from_head(parent, name, &full_path, head);
                }
                // The resolve endpoint returns 404 for directories, so a HEAD
                // miss could still be a remotely-added dir. Targeted listing
                // catches that; non-empty result means the dir exists.
                if let Ok(entries) = self.hub_client.list_tree(&full_path).await
                    && !entries.is_empty()
                {
                    return self.insert_dir(parent, name, &full_path);
                }
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

        // Overlay precedence (see FastResult::Miss): defer to the parent
        // listing + merge so the local entry is what we resolve.
        if self.overlay_lookup_takes_precedence(parent, &full_path)? {
            self.ensure_children_loaded(parent).await?;
            let inodes = self.inode_table.read().expect("inodes poisoned");
            return inodes
                .lookup_child(parent, name)
                .map(|e| self.make_vfs_attr(e))
                .ok_or(libc::ENOENT);
        }

        // Resolve the single requested path via HEAD instead of listing the
        // whole parent — for point-access workloads (no `readdir`) this keeps
        // the inode table scoped to what the caller actually touches.
        match self.hub_client.head_file(&full_path).await {
            // Without size, `open_readonly` would take the empty-file shortcut
            // and expose a zero-byte file. Fall back to list_tree which has
            // the authoritative size from the tree index.
            Ok(Some(head)) if head.size.is_some() => {
                return self.insert_file_from_head(parent, name, &full_path, head);
            }
            // 404 may mean "doesn't exist" or "it's a directory" (the resolve
            // endpoint only handles files), so the listing has the final word.
            Ok(_) => {}
            Err(e) => debug!("HEAD lookup {} failed, falling back to list: {}", full_path, e),
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

    /// Insert a file inode resolved via HEAD, without touching its siblings.
    /// Used by the lookup slow path to avoid listing the whole parent dir
    /// when only one file is needed. The parent's `children_loaded` stays
    /// `false` so a later `readdir` still triggers a full listing.
    pub(super) fn insert_file_from_head(
        &self,
        parent: u64,
        name: &str,
        full_path: &str,
        head: crate::hub_api::HeadFileInfo,
    ) -> VirtualFsResult<VirtualFsAttr> {
        let size = head.size.unwrap_or(0);
        let mtime = head
            .last_modified
            .as_deref()
            .map(crate::hub_api::mtime_from_http_date)
            .unwrap_or_else(|| self.hub_client.default_mtime());

        let mut inodes = self.inode_table.write().expect("inodes poisoned");
        match inodes.get(parent) {
            None => return Err(libc::ENOENT),
            Some(e) if e.kind != InodeKind::Directory => return Err(libc::ENOTDIR),
            Some(_) => {}
        }
        if let Some(existing) = inodes.lookup_child(parent, name) {
            // A concurrent HEAD lookup may have just inserted the same file —
            // return its attr instead of redoing the work.
            if existing.kind == InodeKind::File {
                return Ok(self.make_vfs_attr(existing));
            }
            // The cached entry is a directory but HEAD just confirmed the
            // remote path is a file: the dir was replaced. Evict the stale
            // subtree, unless dirty/open descendants would be lost — in that
            // case keep serving the stale dir (the next sweep / explicit
            // close will eventually clear the way).
            let stale_ino = existing.inode;
            if inodes.has_dirty_or_open_descendants(stale_ino) {
                return Ok(self.make_vfs_attr(existing));
            }
            inodes.remove(stale_ino);
        }
        let ino = inodes.insert(
            parent,
            name.to_string(),
            full_path.to_string(),
            InodeKind::File,
            size,
            mtime,
            head.xet_hash,
            0o644,
            self.uid,
            self.gid,
        );
        if let Some(etag) = head.etag
            && let Some(entry) = inodes.get_mut(ino)
        {
            entry.etag = Some(etag);
        }
        match inodes.get(ino) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => Err(libc::EIO),
        }
    }

    /// Insert a directory inode discovered via a targeted listing, without
    /// touching siblings. Mirrors `insert_file_from_head` for the dir case.
    /// Parent's `children_loaded` stays as-is so a later `readdir` still
    /// triggers a full listing.
    pub(super) fn insert_dir(&self, parent: u64, name: &str, full_path: &str) -> VirtualFsResult<VirtualFsAttr> {
        let mut inodes = self.inode_table.write().expect("inodes poisoned");
        match inodes.get(parent) {
            None => return Err(libc::ENOENT),
            Some(e) if e.kind != InodeKind::Directory => return Err(libc::ENOTDIR),
            Some(_) => {}
        }
        if let Some(existing) = inodes.lookup_child(parent, name) {
            return Ok(self.make_vfs_attr(existing));
        }
        let ino = inodes.insert(
            parent,
            name.to_string(),
            full_path.to_string(),
            InodeKind::Directory,
            0,
            self.hub_client.default_mtime(),
            None,
            0o755,
            self.uid,
            self.gid,
        );
        match inodes.get(ino) {
            Some(entry) => Ok(self.make_vfs_attr(entry)),
            None => Err(libc::EIO),
        }
    }
}
