use super::*;

impl VirtualFs {
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

        // Overlay: cannot rename clean remote entries (no whiteout for old path).
        if let Some(src) = self
            .inode_table
            .read()
            .expect("inodes poisoned")
            .lookup_child(parent, name)
            && self.is_overlay_immutable(src)
        {
            return Err(libc::EPERM);
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
            // Overlay shadow guard: a local dir that shares its name with a
            // remote dir is dirty (set by merge_overlay_entries), so the
            // top-level immutable check above lets it through. After loading
            // the subtree we can spot remote descendants — moving them
            // locally without a remote rename would leave them dangling on
            // the new local path while the remote tree keeps the old one.
            if self.overlay()
                && self
                    .inode_table
                    .read()
                    .expect("inodes poisoned")
                    .has_clean_descendants(src_ino)
            {
                return Err(libc::EPERM);
            }
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

        // Phase 2: sync to remote (add + delete ops). Skipped in overlay mode
        // since writes never propagate beyond the local backing.
        let remote_mutated = if self.overlay() {
            false
        } else {
            self.rename_remote(&info).await?
        };
        let old_path = info.old_path.clone();

        // Overlay: move the on-disk file to match the new path.
        if let Some(overlay) = &self.overlay_backing {
            let old_exists = overlay.exists(&info.old_path).map_err(|e| {
                error!("Overlay rename: failed to stat {}: {}", info.old_path, e);
                libc::EIO
            })?;
            if !old_exists {
                return Err(libc::EPERM);
            }
            if let Err(e) = overlay.create_parent_dirs(&info.new_full_path) {
                error!("Overlay rename: failed to create destination parents: {}", e);
                return Err(e.raw_os_error().unwrap_or(libc::EIO));
            }
            if let Err(e) = overlay.rename(&info.old_path, &info.new_full_path) {
                error!(
                    "Overlay rename {} -> {} failed: {}",
                    info.old_path, info.new_full_path, e
                );
                return Err(e.raw_os_error().unwrap_or(libc::EIO));
            }
        }

        // Phase 3: apply to local inode table under write lock.
        // If Phase 2 mutated the remote and Phase 3 fails with ENOENT (source
        // concurrently unlinked), swallow the error and let poll reconcile.
        // Destination-conflict errors (EEXIST, EISDIR, etc.) are propagated
        // since poll may not fix a dirty local inode at the destination path.
        match self.rename_apply_local(info, parent, name, newparent, newname, no_replace) {
            Ok(replaced_staging_ino) => {
                // Seed the negative cache before any await: with the source
                // already moved locally, a concurrent lookup of `old_path`
                // would otherwise HEAD the still-existing remote object
                // during the drop_locked await window and resurrect it.
                self.negative_cache_insert(old_path);
                if let Some(ino) = replaced_staging_ino {
                    self.staging.drop_locked(ino).await;
                }
                Ok(())
            }
            Err(libc::ENOENT) if remote_mutated || self.overlay() => {
                warn!("rename: source gone after rename; next dir load will reconcile");
                // Invalidate parents so the next lookup re-fetches from remote
                // instead of trusting the stale children_loaded state.
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                inodes.invalidate_children(parent);
                if parent != newparent {
                    inodes.invalidate_children(newparent);
                }
                Ok(())
            }
            Err(errno) => Err(errno),
        }
    }

    /// Phase 1: validate rename under inode read lock, return all info needed for phases 2+3.
    pub(super) fn rename_validate(
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
                                        child.full_path.to_string(),
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
            old_path: src.full_path.to_string(),
            kind: src.kind,
            xet_hash: src.xet_hash.clone(),
            is_dirty: src.is_dirty(),
            new_full_path,
            descendant_files,
        })
    }

    /// Phase 2: send batch rename ops to the Hub (add new paths + delete old ones).
    /// Returns true if remote operations were actually sent, false if skipped
    /// (e.g. dirty file with no remote presence).
    pub(super) async fn rename_remote(&self, info: &RenameInfo) -> VirtualFsResult<bool> {
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
            return Ok(false);
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
        Ok(true)
    }

    /// Phase 3: apply rename to local inode table under write lock.
    /// Returns `Ok(Some(ino))` when a staging file for a replaced target needs
    /// to be dropped; the caller must do that asynchronously under the
    /// per-inode staging lock to serialize with in-flight flush uploads.
    pub(super) fn rename_apply_local(
        &self,
        info: RenameInfo,
        parent: u64,
        oldname: &str,
        newparent: u64,
        newname: &str,
        no_replace: bool,
    ) -> VirtualFsResult<Option<u64>> {
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
                return Ok(None);
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
        let mut replaced_staging_ino: Option<u64> = None;
        if let Some((existing_ino, existing_kind)) = replace_target {
            if existing_kind == InodeKind::Directory {
                // Directories can't be hard-linked, so remove() is correct
                // (remove() adjusts parent nlink for directories)
                inodes.remove(existing_ino);
            } else {
                inodes.unlink_one(newparent, newname);
                if !inodes.has_open_handles(existing_ino) && inodes.remove_orphan(existing_ino) {
                    replaced_staging_ino = Some(existing_ino);
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
                                let old_path = child.full_path.to_string();
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
        drop(inodes);

        Ok(replaced_staging_ino)
    }
}
