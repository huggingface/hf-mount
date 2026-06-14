use super::*;

impl VirtualFs {
    pub async fn open(&self, ino: u64, writable: bool, truncate: bool, pid: Option<u32>) -> VirtualFsResult<u64> {
        debug!(
            "open: ino={}, writable={}, truncate={}, pid={:?}",
            ino, writable, truncate, pid
        );

        if writable && self.read_only {
            return Err(libc::EROFS);
        }

        let file_entry = self.get_file_entry(ino)?;
        let staging_path = self.staging.path(ino);

        if writable && self.advanced_writes {
            // Staging file + async flush (supports random writes and seek)
            self.open_advanced_write(
                ino,
                &file_entry.full_path,
                &file_entry.xet_hash,
                file_entry.size,
                truncate,
            )
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
    pub(super) async fn open_advanced_write(
        &self,
        ino: u64,
        full_path: &str,
        xet_hash: &str,
        size: u64,
        truncate: bool,
    ) -> VirtualFsResult<u64> {
        // Serialize staging preparation per inode (prevents concurrent download races)
        let staging_mutex = self.staging.lock(ino);
        let _staging_guard = staging_mutex.lock().await;

        // Reuse the staging file when either (a) it has pending dirty writes,
        // or (b) it's a clean cache flagged as current. In both cases its
        // content is the right starting point for this open.
        let (is_dirty, staging_is_current) = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let entry = inodes.get(ino).ok_or(libc::ENOENT)?;
            (entry.is_dirty(), entry.staging_is_current)
        };
        let local_exists = self.local_backing_exists(ino, full_path).map_err(|e| {
            error!("Failed to check local backing file for ino={}: {}", ino, e);
            libc::EIO
        })?;
        // Overlay never downloads from remote — only existing local files (or dirty drafts) are writable.
        if self.overlay() && !is_dirty && !local_exists {
            return Err(libc::EPERM);
        }

        let can_reuse_staging = !truncate && (is_dirty || staging_is_current) && local_exists;

        if !can_reuse_staging {
            // Clear the flag before touching disk so a partial failure (e.g.
            // mid-download CAS error, async cancel) never leaves the cache
            // reusable.
            if let Some(entry) = self.inode_table.write().expect("inodes poisoned").get_mut(ino) {
                entry.staging_is_current = false;
            }
            // GC accounting only matters for non-overlay (overlay files live
            // in user dir, so file_size returns 0 here on miss).
            let old_size = self.staging.dir().map(|sd| sd.file_size(ino)).unwrap_or(0);
            let needs_download = !self.overlay() && !truncate && !xet_hash.is_empty() && size > 0;
            let new_size = if needs_download {
                let staging_path = self
                    .staging
                    .path(ino)
                    .expect("staging directory required for advanced writes");
                self.xet_sessions
                    .download_to_file(xet_hash, size, &staging_path)
                    .await
                    .map_err(|e| {
                        error!("Failed to download file for write: {}", e);
                        libc::EIO
                    })?;
                size
            } else {
                self.open_local_backing_file(ino, full_path, true, true, true, true)
                    .map_err(|e| {
                        error!("Failed to create local backing file: {}", e);
                        libc::EIO
                    })?;
                0
            };
            if !self.overlay()
                && let Some(sd) = self.staging.dir()
            {
                sd.resize_bytes(old_size, new_size);
            }
            // Flag the cache as current only when the staging actually mirrors
            // the remote. Cases to exclude:
            // - truncated hashed file: empty staging, non-empty xet_hash.
            // - non-Xet file with `size > 0` and no xet_hash: File::create
            //   leaves empty staging, which does not match the remote.
            // - race with poll: `xet_hash` moved between `open()` reading the
            //   inode and here, so the downloaded hash is now stale — detected
            //   by the `entry.xet_hash == xet_hash` post-check.
            // Skip in overlay mode: there's no remote materialization concept.
            let materializes_remote = !self.overlay() && (needs_download || (xet_hash.is_empty() && size == 0));
            if materializes_remote
                && let Some(entry) = self.inode_table.write().expect("inodes poisoned").get_mut(ino)
                && entry.xet_hash.as_deref().unwrap_or("") == xet_hash
            {
                entry.staging_is_current = true;
            }
        }
        let file = self
            .open_local_backing_file(ino, full_path, true, true, false, false)
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

        self.install_local_handle(ino, Arc::new(Mutex::new(file)), true)
    }

    /// Simple streaming write: truncate existing file and set up a new streaming writer.
    pub(super) async fn open_streaming_write(&self, ino: u64, pid: Option<u32>) -> VirtualFsResult<u64> {
        // Wait for any in-flight commit to complete before starting a new writer.
        self.await_pending_commit(ino).await?;

        let staging_mutex = self.staging.lock(ino);
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

        self.inode_table.read().expect("inodes poisoned").bump_open_handles(ino);
        self.open_files
            .write()
            .expect("open_files poisoned")
            .insert(file_handle, OpenFile::Streaming { ino, channel });
        Ok(file_handle)
    }

    /// Open a file for reading. Dispatches based on where the content lives.
    pub(super) async fn open_readonly(
        &self,
        ino: u64,
        fe: FileEntry,
        staging_path: Option<PathBuf>,
    ) -> VirtualFsResult<u64> {
        // For dirty-staging reads, hold the per-inode staging lock across
        // both the existence check and the open so the flush-path GC can't
        // unlink the file in between (it takes the same lock in gc_one).
        let _staging_guard = if fe.is_dirty && staging_path.is_some() {
            Some(self.staging.lock(ino).lock_owned().await)
        } else {
            None
        };

        // Overlay mode: dirty file is in the overlay backing, not the staging path.
        if fe.is_dirty && self.overlay() {
            let local_exists = self.local_backing_exists(ino, &fe.full_path).map_err(|e| {
                error!("Failed to check local backing file for {}: {}", fe.full_path, e);
                libc::EIO
            })?;
            if local_exists {
                let file = self
                    .open_local_backing_file(ino, &fe.full_path, true, false, false, false)
                    .map_err(|e| {
                        error!("Failed to open local backing file {:?}: {}", fe.full_path, e);
                        libc::EIO
                    })?;
                return self.install_local_handle(ino, Arc::new(Mutex::new(file)), false);
            }
            error!("Dirty overlay file ino={} has missing local backing file", ino);
            return Err(libc::EIO);
        }

        // If the dirty snapshot pointed at a now-missing staging file, the
        // flush-path GC may have raced before we took the lock above: the
        // inode was flushed clean and its staging reclaimed between the
        // snapshot and here. Re-read the entry so we dispatch on current
        // state instead of returning EIO.
        let fe = match (fe.is_dirty, &staging_path) {
            (true, Some(p)) if !p.exists() => self.get_file_entry(ino)?,
            _ => fe,
        };
        match (fe.is_dirty, &staging_path) {
            // Advanced write in progress — read from local staging file.
            (true, Some(path)) if path.exists() => self.open_local_readonly(ino, path),

            // Dirty file but staging file is missing — should not happen.
            (true, Some(_)) => {
                error!("Dirty file ino={} has missing staging file", ino);
                Err(libc::EIO)
            }

            // Dirty without staging path — committed since the snapshot read.
            // Re-fetch entry and dispatch on the now-clean state.
            (true, None) => {
                let fe = self.get_file_entry(ino)?;
                if !fe.xet_hash.is_empty() {
                    return self.open_lazy(ino, fe.xet_hash, fe.size);
                }
                self.open_lazy(ino, String::new(), 0)
            }

            // Remote xet-backed file — try the whole-file cache first, then
            // fall back to lazy CAS range reads.
            _ if !fe.xet_hash.is_empty() => {
                if let Some(fc) = &self.file_cache {
                    if let Some(file) = fc.try_open(&fe.xet_hash).await {
                        let cloned = std::fs::File::try_clone(&file).map_err(|_| libc::EIO)?;
                        return self.install_local_handle(ino, Arc::new(Mutex::new(cloned)), false);
                    }
                    self.spawn_populate_file_cache(fe.xet_hash.clone(), fe.size);
                }
                self.open_lazy(ino, fe.xet_hash, fe.size)
            }

            // Non-Xet file — HTTP download to staging cache.
            //
            // TODO(staging-gc): http_<hash> files are not counted in
            // StagingDir::bytes_used and not picked up by try_remove(ino), so
            // a repo with large non-Xet files can exceed --max-staging-size
            // without the GC noticing. Either resize_bytes() after download +
            // index http_* paths in StagingCoordinator, or document that the
            // budget covers writes only.
            _ if fe.size > 0 => {
                let staging = self.staging.dir().ok_or_else(|| {
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
                    let lock = self.staging.lock(ino);
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
    pub(super) fn open_lazy(&self, ino: u64, xet_hash: String, size: u64) -> VirtualFsResult<u64> {
        let prefetch = Arc::new(tokio::sync::Mutex::new(PrefetchState::new(
            xet_hash,
            size,
            self.direct_io,
        )));
        let file_handle = self.alloc_file_handle();
        self.inode_table.read().expect("inodes poisoned").bump_open_handles(ino);
        self.open_files
            .write()
            .expect("open_files poisoned")
            .insert(file_handle, OpenFile::Lazy { ino, prefetch });
        Ok(file_handle)
    }

    /// Fetch data for the prefetch buffer. Uses the persistent stream for sequential
    /// reads (with automatic (re)start), or opens a temporary stream with retries
    /// for range/seek access. Returns EIO if all attempts fail.
    pub(super) async fn fetch_data(
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
}
