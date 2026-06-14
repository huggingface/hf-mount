use super::*;

impl VirtualFs {
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
                    name: child_ref.name.to_string(),
                });
            }
        }

        Ok(entries)
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
            inodes.touch(ino);
            (ino, full_path, dirty_gen)
        };

        self.negative_cache_remove(&full_path);
        if let Some(fm) = &self.flush_manager {
            fm.cancel_delete(&full_path);
        }

        if self.advanced_writes {
            // Advanced mode: staging file on disk + async flush (or overlay file in overlay mode).
            match self.open_local_backing_file(ino, &full_path, true, true, true, true) {
                Ok(file) => {
                    if let Err(e) = self.set_local_backing_mode(&full_path, mode) {
                        error!("Failed to set local backing mode for {}: {}", full_path, e);
                        self.inode_table.write().expect("inodes poisoned").remove(ino);
                        return Err(libc::EIO);
                    }
                    let file_handle = self.alloc_file_handle();
                    let inodes = self.inode_table.read().expect("inodes poisoned");
                    inodes.bump_open_handles(ino);
                    self.open_files.write().expect("open_files poisoned").insert(
                        file_handle,
                        OpenFile::Local {
                            ino,
                            file: Arc::new(Mutex::new(file)),
                            writable: true,
                        },
                    );

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

            let inodes = self.inode_table.read().expect("inodes poisoned");
            inodes.bump_open_handles(ino);
            self.open_files
                .write()
                .expect("open_files poisoned")
                .insert(file_handle, OpenFile::Streaming { ino, channel });

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
                entry.children_loaded_at = Some(Instant::now());
                // children_from_remote stays false: a freshly-mkdir'd dir has
                // no remote presence yet, so lookup-miss can serve ENOENT
                // without HEAD/list_tree probes.
                if self.overlay() {
                    entry.set_dirty();
                }
            }
            // nlink already incremented by insert()
            inodes.touch_parent(parent, now);
            (ino, full_path)
        };

        self.negative_cache_remove(&full_path);

        // Overlay: persist directory to disk.
        if let Some(overlay) = &self.overlay_backing {
            if let Err(e) = overlay.create_parent_dirs(&full_path) {
                error!("Failed to create overlay parent directories for {}: {}", full_path, e);
                self.inode_table.write().expect("inodes poisoned").remove(ino);
                return Err(libc::EIO);
            }
            if let Err(e) = overlay.create_dir(&full_path, mode) {
                error!("Failed to create overlay directory {}: {}", full_path, e);
                self.inode_table.write().expect("inodes poisoned").remove(ino);
                return Err(e.raw_os_error().unwrap_or(libc::EIO));
            }
        }

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
            // Overlay: cannot delete clean remote entries (no whiteout support).
            // Deleting dirty (local) entries is allowed; remote reappears on remount.
            if self.is_overlay_immutable(entry) {
                return Err(libc::EPERM);
            }
            // Remote delete only when last link is removed and file exists on the hub.
            // Skipped in overlay mode (writes never propagate to remote).
            let needs_remote = !self.overlay() && entry.xet_hash.is_some() && entry.nlink <= 1;
            (entry.inode, entry.full_path.to_string(), needs_remote)
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
        let inode_fully_removed = {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            let last_link = inodes
                .unlink_one(parent, name)
                .map(|(removed, _)| removed)
                .unwrap_or(false);
            // Update parent mtime/ctime (POSIX: directory was modified)
            let now = SystemTime::now();
            inodes.touch_parent(parent, now);
            // If last link gone and no open handles, remove the orphan immediately.
            // Otherwise leave it for release() so an open fd can keep writing
            // (POSIX unlink-while-open) without us yanking the staging file +
            // debiting the budget twice.
            let no_handles = !inodes.has_open_handles(ino);
            if last_link && no_handles {
                inodes.remove_orphan(ino);
            }
            last_link && no_handles
        };

        // Seed the negative cache before any await: with the inode already
        // gone from the table, a concurrent lookup under a stale parent would
        // otherwise HEAD the still-existing remote (the delete is only queued)
        // and resurrect this name locally during the drop_locked await window.
        self.negative_cache_insert(full_path.clone());

        // Clean up staging file only when the inode is actually gone. With an
        // open fd, drop_staging waits until release() so writes through the
        // surviving fd can still update bytes_used coherently.
        if inode_fully_removed {
            if self.overlay() {
                if let Err(e) = self.remove_local_backing_file(ino, &full_path)
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    warn!("Failed to remove overlay file for ino={}: {}", ino, e);
                }
            } else {
                self.staging.drop_locked(ino).await;
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

        let (ino, full_path) = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            match inodes.lookup_child(parent, name) {
                Some(entry) if entry.kind == InodeKind::Directory => {
                    // Overlay: cannot remove clean remote directories.
                    if self.is_overlay_immutable(entry) {
                        return Err(libc::EPERM);
                    }
                    if !entry.children.is_empty() {
                        return Err(libc::ENOTEMPTY);
                    }
                    (entry.inode, entry.full_path.to_string())
                }
                Some(_) => return Err(libc::ENOTDIR),
                None => return Err(libc::ENOENT),
            }
        };

        // Load remote children so we don't miss any before the emptiness check
        self.ensure_children_loaded(ino).await?;

        // Overlay: remove on-disk dir before mutating the inode table so a
        // failure can't leave the inode tree out of sync with the backing.
        if let Some(overlay) = &self.overlay_backing
            && let Err(e) = overlay.remove_dir(&full_path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(e.raw_os_error().unwrap_or(libc::EIO));
        }

        // Re-check ENOTEMPTY + remove under a single write lock to prevent
        // a concurrent create/mkdir from inserting a child in between.
        {
            let mut inodes = self.inode_table.write().expect("inodes poisoned");
            match inodes.get(ino) {
                Some(entry) if !entry.children.is_empty() => return Err(libc::ENOTEMPTY),
                None => return Err(libc::ENOENT),
                _ => {}
            }
            inodes.remove(ino);
            // nlink adjusted by remove()
            inodes.touch_parent(parent, SystemTime::now());
        }
        self.negative_cache_insert(full_path);
        Ok(())
    }

}
