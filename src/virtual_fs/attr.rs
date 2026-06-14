use super::*;

impl VirtualFs {
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

    /// Enqueues the inode for background flush rather than blocking on a
    /// synchronous Hub upload. Data is already durable in the local staging
    /// file after write(); the background flush loop commits to Hub.
    ///
    /// - Streaming handles: no-op (committed synchronously on close).
    /// - Advanced-writes handles: enqueues for background flush.
    /// - Read-only / lazy handles: no-op.
    pub async fn fsync(&self, ino: u64, file_handle: u64, _pid: Option<u32>) -> VirtualFsResult<()> {
        let overlay_file = {
            let files = self.open_files.read().expect("open_files poisoned");
            match files.get(&file_handle) {
                Some(OpenFile::Streaming { .. }) => return Ok(()),
                // Overlay: sync local fd for durability, skip remote upload.
                Some(OpenFile::Local { file, .. }) if self.overlay() => Some(Arc::clone(file)),
                _ => None,
            }
        };
        if let Some(file) = overlay_file {
            let file = file.lock().expect("local file mutex poisoned");
            return file.sync_all().map_err(|e| {
                error!("Overlay fsync failed for ino={}: {}", ino, e);
                libc::EIO
            });
        }
        self.schedule_flush(ino);
        Ok(())
    }

    pub fn getattr(&self, ino: u64) -> VirtualFsResult<VirtualFsAttr> {
        debug!("getattr: ino={}", ino);

        let inodes = self.inode_table.read().expect("inodes poisoned");
        match inodes.get(ino) {
            Some(entry) => {
                inodes.touch(ino);
                Ok(self.make_vfs_attr(entry))
            }
            None => Err(libc::ENOENT),
        }
    }

    /// Must be called after every `reply.entry()` / `reply.created()`, or
    /// the kernel and our `nlookup` will drift and a later `forget()` will
    /// underflow. Also touches for LRU so actively-looked-up inodes aren't
    /// immediately evicted.
    #[cfg(any(feature = "fuse", test))]
    pub(crate) fn bump_nlookup(&self, ino: u64) {
        let inodes = self.inode_table.read().expect("inodes poisoned");
        inodes.bump_nlookup(ino);
        inodes.touch(ino);
    }

    /// Handle a FUSE `forget(ino, nlookup)`: drop the refcount by `nlookup`,
    /// and evict the inode if it's now safe (kernel no longer holds the
    /// dentry, no open handles, nothing dirty). Eviction keeps `InodeTable`
    /// bounded — without it the table grows for every file ever looked up.
    #[cfg(any(feature = "fuse", test))]
    pub(crate) fn forget(&self, ino: u64, nlookup: u64) {
        debug!("forget: ino={} nlookup={}", ino, nlookup);

        // Shared lock for the hot path: dropping the refcount is an atomic op,
        // so readers (lookup/getattr/read) aren't blocked on the common case
        // where the refcount stays > 0.
        let reached_zero = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            inodes.drop_nlookup(ino, nlookup)
        };
        if !reached_zero {
            return;
        }

        // A racing open() would insert into `open_files` before calling
        // `bump_nlookup`, so checking handles here (after the refcount hit 0)
        // is safe: any concurrent lookup will re-bump the count, and any
        // concurrent read/write holds an open handle we'll see.
        if self.has_open_handles(ino) {
            // The kernel has given up the dentry but our handle keeps the
            // inode alive. Mark it so `release()` finishes the eviction
            // once the last handle closes — otherwise it would leak.
            self.inode_table
                .read()
                .expect("inodes poisoned")
                .mark_evict_pending(ino);
            return;
        }

        let evicted = {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            inodes.evict_if_safe(ino)
        };
        if evicted {
            self.drop_staging(ino);
        }
    }

    /// Remove any on-disk staging file for `ino`. Safe to call for inodes that
    /// never had a staging file — NotFound is ignored. Keeps `StagingDir`'s
    /// byte budget accurate via `try_remove`.
    pub(super) fn drop_staging(&self, ino: u64) {
        if let Some(sd) = self.staging.dir() {
            sd.try_remove(ino);
        }
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
            let full_path = {
                let inodes = self.inode_table.read().expect("inodes poisoned");
                match inodes.get(ino) {
                    Some(e) if e.kind != InodeKind::File => return Err(libc::EISDIR),
                    Some(e) => e.full_path.clone(),
                    None => return Err(libc::ENOENT),
                }
            };

            if !self.advanced_writes {
                // Simple mode: ftruncate via setattr is silently ignored.
                // Real truncation goes through open(O_TRUNC) which is handled separately.
            } else {
                // Advanced mode: truncation is applied to the staging file on disk
                let staging_mutex = self.staging.lock(ino);
                let _staging_guard = staging_mutex.lock().await;

                let local_exists = self.local_backing_exists(ino, &full_path).map_err(|e| {
                    error!("Failed to check local backing file for ino={}: {}", ino, e);
                    e.raw_os_error().unwrap_or(libc::EIO)
                })?;

                if self.overlay() && !local_exists {
                    return Err(libc::EPERM);
                }

                // GC accounting (non-overlay only): snapshot staging bytes before
                // any mutation so the size delta is applied correctly at the end.
                let old_staging_size = self
                    .staging
                    .dir()
                    .filter(|_| !self.overlay())
                    .map(|sd| sd.file_size(ino))
                    .unwrap_or(0);

                if !local_exists {
                    if new_size > 0 {
                        let staging_path = self
                            .staging
                            .path(ino)
                            .expect("staging directory required for advanced writes");
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
                        } else if let Err(e) = self.open_local_backing_file(ino, &full_path, true, true, true, true) {
                            error!("Failed to create staging file for truncate: {}", e);
                            return Err(libc::EIO);
                        }
                    } else if let Err(e) = self.open_local_backing_file(ino, &full_path, true, true, true, true) {
                        error!("Failed to create local backing file for truncate: {}", e);
                        return Err(libc::EIO);
                    }
                }

                // Apply the size change under the same write lock so write() cannot
                // race between the local truncate and inode metadata update.
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                let size_result = if new_size == 0 {
                    self.open_local_backing_file(ino, &full_path, true, true, true, true)
                        .map(|_| ())
                } else {
                    self.open_local_backing_file(ino, &full_path, false, true, false, false)
                        .and_then(|file| file.set_len(new_size))
                };
                if let Err(e) = size_result {
                    error!("Failed to set local backing file length: {}", e);
                    return Err(libc::EIO);
                }
                if !self.overlay()
                    && let Some(sd) = self.staging.dir()
                {
                    sd.resize_bytes(old_staging_size, sd.file_size(ino));
                }
                if let Some(entry) = inodes.get_mut(ino) {
                    entry.size = new_size;
                    entry.mtime = SystemTime::now();
                    entry.ctime = entry.mtime;
                    entry.set_dirty();
                    if new_size == 0 {
                        entry.xet_hash = None;
                    }
                }
                drop(inodes);

                // Schedule flush so the truncation is committed to CAS/bucket
                if let Some(fm) = &self.flush_manager {
                    fm.enqueue(ino);
                }
            }
        }

        // Apply metadata-only changes (mode, uid, gid, atime, mtime)
        if mode.is_some() || uid.is_some() || gid.is_some() || atime.is_some() || mtime.is_some() {
            if self.overlay() {
                let full_path = {
                    let inodes = self.inode_table.read().expect("inodes poisoned");
                    inodes.get(ino).ok_or(libc::ENOENT)?.full_path.clone()
                };
                // Reject any metadata-only mutation on a clean remote entry:
                // there is no local backing to record it on, and the change
                // would silently disappear at the next remote refresh.
                let local_exists = self.local_backing_exists(ino, &full_path).map_err(|e| {
                    error!("Failed to check local backing file for ino={}: {}", ino, e);
                    e.raw_os_error().unwrap_or(libc::EIO)
                })?;
                if !local_exists {
                    return Err(libc::EPERM);
                }
                if let Some(new_mode) = mode {
                    self.set_local_backing_mode(&full_path, new_mode).map_err(|e| {
                        error!("Failed to update local backing mode for ino={}: {}", ino, e);
                        e.raw_os_error().unwrap_or(libc::EIO)
                    })?;
                }
            }

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
    pub(super) fn get_file_entry(&self, ino: u64) -> VirtualFsResult<FileEntry> {
        let inodes = self.inode_table.read().expect("inodes poisoned");
        let entry = match inodes.get(ino) {
            Some(e) if e.kind == InodeKind::File => e,
            _ => return Err(libc::ENOENT),
        };
        Ok(FileEntry {
            xet_hash: entry.xet_hash.clone().unwrap_or_default(),
            size: entry.size,
            is_dirty: entry.is_dirty(),
            full_path: entry.full_path.to_string(),
        })
    }
}
