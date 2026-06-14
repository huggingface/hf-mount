use super::*;

impl VirtualFs {
    pub fn alloc_file_handle(&self) -> u64 {
        self.next_file_handle.fetch_add(1, Ordering::Relaxed)
    }

    /// Bump the per-inode open-handle refcount. Used by the FUSE adapter
    /// on `opendir` so a directory with an active readdir can't be evicted.
    #[cfg(feature = "fuse")]
    pub(crate) fn bump_open_handles(&self, ino: u64) {
        self.inode_table.read().expect("inodes poisoned").bump_open_handles(ino);
    }

    /// Counterpart to `bump_open_handles`, called from `releasedir`.
    #[cfg(feature = "fuse")]
    pub(crate) fn drop_open_handles(&self, ino: u64) {
        self.inode_table.read().expect("inodes poisoned").drop_open_handles(ino);
    }

    /// Check if any open file handle references the given inode.
    pub(super) fn has_open_handles(&self, ino: u64) -> bool {
        self.inode_table.read().expect("inodes poisoned").has_open_handles(ino)
    }

    /// Get or create a per-directory lock for serializing ensure_children_loaded().
    pub(super) fn dir_loading_lock(&self, ino: u64) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.dir_loading_locks.lock().expect("dir_loading_locks poisoned");
        if let Some(weak) = locks.get(&ino)
            && let Some(arc) = weak.upgrade()
        {
            return arc;
        }
        locks.retain(|_, w| w.strong_count() > 0);
        let arc = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(ino, Arc::downgrade(&arc));
        arc
    }

    pub(super) fn local_backing_exists(&self, ino: u64, full_path: &str) -> std::io::Result<bool> {
        match self.overlay_backing.as_deref() {
            Some(overlay) => overlay.exists(full_path),
            None => Ok(self
                .staging
                .path(ino)
                .expect("staging directory required for local backing")
                .exists()),
        }
    }

    pub(super) fn open_local_backing_file(
        &self,
        ino: u64,
        full_path: &str,
        read: bool,
        write: bool,
        create: bool,
        truncate: bool,
    ) -> std::io::Result<File> {
        match self.overlay_backing.as_deref() {
            Some(overlay) => {
                // Overlay paths are hierarchical; parents may exist as inodes
                // (from a remote listing) without being materialized locally.
                // Staging is flat ino-keyed, so this branch never applies.
                if create {
                    overlay.create_parent_dirs(full_path)?;
                }
                overlay.open_file(full_path, read, write, create, truncate)
            }
            None => {
                let path = self
                    .staging
                    .path(ino)
                    .expect("staging directory required for local backing");
                let mut opts = OpenOptions::new();
                opts.read(read).write(write);
                if create {
                    opts.create(true);
                }
                if truncate {
                    opts.truncate(true);
                }
                opts.open(&path)
            }
        }
    }

    pub(super) fn set_local_backing_mode(&self, full_path: &str, mode: u16) -> std::io::Result<()> {
        match self.overlay_backing.as_deref() {
            Some(overlay) => overlay.set_mode(full_path, mode),
            None => Ok(()),
        }
    }

    pub(super) fn remove_local_backing_file(&self, ino: u64, full_path: &str) -> std::io::Result<()> {
        match self.overlay_backing.as_deref() {
            Some(overlay) => overlay.remove_file(full_path),
            None => {
                let path = self
                    .staging
                    .path(ino)
                    .expect("staging directory required for local backing");
                std::fs::remove_file(&path)
            }
        }
    }

    /// Install a pending commit watch hook on a streaming channel.
    /// Called in flush() before commit or deferral so open() can await the result.
    /// No-op if a hook is already installed (prevents replacing a receiver that
    /// an open() caller may already be awaiting).
    pub(super) fn install_commit_hook(&self, ino: u64, channel: &StreamingChannel) {
        let mut hook = channel.commit_hook.lock().expect("commit_hook poisoned");
        if hook.is_some() {
            return; // already installed — don't replace
        }
        let (tx, rx) = tokio::sync::watch::channel(None);
        *hook = Some(tx);
        self.pending_commits
            .lock()
            .expect("pending_commits poisoned")
            .insert(ino, rx);
    }

    /// Fulfill the pending commit hook with a result, then clean up the map.
    pub(super) fn fulfill_commit_hook(&self, ino: u64, channel: &StreamingChannel, result: Result<(), i32>) {
        if let Some(tx) = channel.commit_hook.lock().expect("commit_hook poisoned").take() {
            let _ = tx.send(Some(result));
        }
        self.pending_commits
            .lock()
            .expect("pending_commits poisoned")
            .remove(&ino);
    }

    /// Wait for any in-flight streaming commit on this inode to complete.
    /// Returns Ok(()) if no pending commit or commit succeeded, Err(errno) if it failed.
    pub(super) async fn await_pending_commit(&self, ino: u64) -> VirtualFsResult<()> {
        let pending_rx = self
            .pending_commits
            .lock()
            .expect("pending_commits poisoned")
            .get(&ino)
            .cloned();

        if let Some(mut rx) = pending_rx {
            while rx.borrow().is_none() {
                if rx.changed().await.is_err() {
                    // Sender dropped without publishing a result — treat as failure.
                    error!("await_pending_commit: sender dropped for ino={}", ino);
                    return Err(libc::EIO);
                }
            }
            if let Some(Err(e)) = &*rx.borrow() {
                debug!("await_pending_commit: commit failed for ino={}: errno={}", ino, e);
                // Inode was reverted — not an error for the caller, just informational.
            }
        }
        Ok(())
    }

    /// Set up a new streaming writer + channel. Returns (file_handle, channel).
    /// Used by both create() and open(O_TRUNC) in simple mode.
    pub(super) async fn setup_streaming_writer(
        &self,
        pid: Option<u32>,
        snapshot: InodeSnapshot,
        dirty_generation_at_open: u64,
    ) -> VirtualFsResult<(u64, Arc<StreamingChannel>)> {
        let streaming_writer = self.xet_sessions.create_streaming_writer().await.map_err(|e| {
            error!("Failed to create streaming writer: {}", e);
            libc::EIO
        })?;

        // Bounded channel provides backpressure so a fast writer doesn't queue
        // unbounded memory. 32 slots × ~128KB FUSE write = ~4MB max in-flight.
        // blocking_send is safe here: FUSE threads are not tokio workers.
        let (tx, rx) = tokio::sync::mpsc::channel::<WriteMsg>(32);
        let error: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        self.runtime
            .spawn(streaming_worker(streaming_writer, rx, error.clone()));

        let channel = Arc::new(StreamingChannel {
            tx,
            bytes_written: AtomicU64::new(0),
            error,
            state: std::sync::Mutex::new(CommitState::Writing),
            pending_info: std::sync::Mutex::new(None),
            open_pid: pid,
            snapshot,
            dirty_generation_at_open: AtomicU64::new(dirty_generation_at_open),
            commit_hook: std::sync::Mutex::new(None),
        });

        let file_handle = self.alloc_file_handle();
        Ok((file_handle, channel))
    }

    /// Open a local file as read-only and return the file handle.
    pub(super) fn open_local_readonly(&self, ino: u64, path: &PathBuf) -> VirtualFsResult<u64> {
        match File::open(path) {
            Ok(file) => self.install_local_handle(ino, Arc::new(Mutex::new(file)), false),
            Err(e) => {
                error!("Failed to open file {:?}: {}", path, e);
                Err(libc::EIO)
            }
        }
    }

    /// Register an already-opened `File` as a read-only `OpenFile::Local`
    /// handle. Used by the file_cache fast-path so the read fd stays alive
    /// even if eviction unlinks the on-disk copy after the open.
    pub(super) fn install_local_handle(
        &self,
        ino: u64,
        file: Arc<Mutex<File>>,
        writable: bool,
    ) -> VirtualFsResult<u64> {
        let file_handle = self.alloc_file_handle();
        {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            inodes.bump_open_handles(ino);
            inodes.touch(ino);
        }
        self.open_files
            .write()
            .expect("open_files poisoned")
            .insert(file_handle, OpenFile::Local { ino, file, writable });
        Ok(file_handle)
    }

    /// Fire-and-forget background populate of the whole-file cache. Failures
    /// are logged inside `FileCache::populate`; the read path still works
    /// via the lazy CAS handle returned to the caller.
    pub(super) fn spawn_populate_file_cache(&self, xet_hash: String, file_size: u64) {
        let Some(fc) = self.file_cache.clone() else { return };
        let xet = self.xet_sessions.clone();
        self.runtime.spawn(async move {
            let hash_for_dl = xet_hash.clone();
            let _ = fc
                .populate(&xet_hash, Some(file_size), move |dest| async move {
                    xet.download_to_file(&hash_for_dl, file_size, &dest).await
                })
                .await;
        });
    }
}
