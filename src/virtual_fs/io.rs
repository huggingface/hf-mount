use super::*;

impl VirtualFs {
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
                let mut file = file.lock().expect("local file mutex poisoned");
                let mut buf = BytesMut::zeroed(size as usize);
                file.seek(SeekFrom::Start(offset)).map_err(|_| libc::EIO)?;
                let n = file.read(&mut buf).map_err(|_| libc::EIO)?;
                buf.truncate(n);
                let eof = (n as u32) < size;
                Ok((buf.freeze(), eof))
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
            Local { file: Arc<Mutex<File>>, ino: u64 },
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
                    file: Arc::clone(file),
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
                let mut file = file.lock().expect("local file mutex poisoned");
                file.seek(SeekFrom::Start(offset)).map_err(|_| libc::EIO)?;
                file.write_all(data).map_err(|_| libc::EIO)?;
                let written = data.len() as u32;
                let new_end = offset + written as u64;
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                if let Some(entry) = inodes.get_mut(handle_ino) {
                    if new_end > entry.size {
                        if let Some(sd) = self.staging.dir() {
                            sd.resize_bytes(entry.size, new_end);
                        }
                        entry.size = new_end;
                    }
                    entry.set_dirty();
                }
                inodes.touch(handle_ino);
                Ok(written)
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
        if let Some(ino) = released_ino {
            self.inode_table.read().expect("inodes poisoned").drop_open_handles(ino);
        }

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

        if let Some(ino) = released_ino
            && !self.has_open_handles(ino)
        {
            let (removed, is_clean, overlay_path) = {
                let mut inodes = self.inode_table.write().expect("inodes poisoned");
                // Capture path before removal so we can clean up the overlay
                // backing file for an unlink-while-open case (POSIX delete on
                // last close): unlink() saw open handles and skipped the
                // overlay-side remove, so we have to do it here.
                let overlay_path = self
                    .overlay_backing
                    .is_some()
                    .then(|| inodes.get(ino).map(|e| e.full_path.clone()))
                    .flatten();
                let orphan = inodes.remove_orphan(ino);
                let evicted = inodes.take_evict_pending(ino) && inodes.evict_if_safe(ino);
                let removed = orphan || evicted;
                let is_clean = !removed && inodes.get(ino).is_some_and(|entry| !entry.is_dirty());
                (removed, is_clean, overlay_path.filter(|_| removed))
            };
            if removed {
                if let Some(path) = overlay_path {
                    if let Err(e) = self.remove_local_backing_file(ino, &path)
                        && e.kind() != std::io::ErrorKind::NotFound
                    {
                        warn!("Failed to remove overlay file for ino={} on release: {}", ino, e);
                    }
                } else {
                    self.drop_staging(ino);
                }
            } else if is_clean && self.staging.gc_one(ino, &self.inode_table).await {
                debug!("staging GC: removed ino={} on release", ino);
            }
        }

        // Per-inode staging locks are intentionally not cleaned up here:
        // removing while another open() may hold the Arc would break
        // serialization. Entries are tiny and bounded by inodes ever staged.

        match release_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Revert an inode to its pre-write state after a failed streaming commit.
    pub(super) fn revert_inode(&self, ino: u64, snapshot: &InodeSnapshot) {
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
    pub(super) async fn streaming_commit(&self, ino: u64, channel: &StreamingChannel) -> Result<(), i32> {
        assert!(!self.overlay(), "overlay forces advanced_writes; streaming unreachable");

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
            (entry.full_path.to_string(), entry.pending_deletes.clone())
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
                file_info.file_size().expect("upload returned XetFileInfo without size"),
                channel.dirty_generation_at_open.load(Ordering::Relaxed),
            );
        }

        info!(
            "Committed file: {} (hash={}, size={})",
            full_path,
            file_info.hash(),
            file_info.file_size().expect("upload returned XetFileInfo without size"),
        );

        Ok(())
    }
}
