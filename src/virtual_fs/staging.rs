use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use tracing::debug;

use crate::xet::StagingDir;

use super::inode::InodeTable;

/// Bundles the on-disk staging area with per-inode async locks so subsystems
/// outside `VirtualFs` (e.g. the flush-path GC) can take the same lock as
/// `open_advanced_write` / `setattr(truncate)` to serialize staging I/O.
pub(crate) struct StagingCoordinator {
    dir: Option<StagingDir>,
    locks: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
}

impl StagingCoordinator {
    pub(crate) fn new(dir: Option<StagingDir>) -> Self {
        Self {
            dir,
            locks: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn dir(&self) -> Option<&StagingDir> {
        self.dir.as_ref()
    }

    pub(crate) fn path(&self, ino: u64) -> Option<PathBuf> {
        self.dir.as_ref().map(|sd| sd.path(ino))
    }

    /// Get or create the per-inode async lock. Held across awaits (download,
    /// unlink) so concurrent opens and flush-path GC can't interleave.
    pub(crate) fn lock(&self, ino: u64) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .expect("staging locks poisoned")
            .entry(ino)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Reclaim a single clean inode's staging file when usage exceeds the
    /// disk budget. Takes the per-inode lock so in-flight opens can't race
    /// the unlink, then re-checks `is_dirty` under the inode read lock to
    /// catch a concurrent writer that set dirty while we waited. Returns
    /// true when the file was actually removed.
    pub(crate) async fn gc_one(&self, ino: u64, inodes: &RwLock<InodeTable>) -> bool {
        let Some(dir) = self.dir() else { return false };
        if !dir.is_over_limit() {
            return false;
        }
        let lock = self.lock(ino);
        let _guard = lock.lock().await;
        let still_clean = inodes
            .read()
            .expect("inodes poisoned")
            .get(ino)
            .is_some_and(|entry| !entry.is_dirty());
        still_clean && dir.try_remove(ino)
    }

    /// Reclaim staging files by least-recently-touched order until the disk
    /// budget is satisfied. Ranks eligible inodes (clean, no open handles,
    /// no pending renames) by `last_touched` ascending so actively-accessed
    /// files stay cached. No-op when under the budget.
    pub(crate) async fn gc(&self, inodes: &RwLock<InodeTable>) -> usize {
        let Some(dir) = self.dir() else { return 0 };
        if !dir.is_over_limit() {
            return 0;
        }
        let candidates = inodes.read().expect("inodes poisoned").staging_gc_candidates();
        let mut count = 0;
        for ino in candidates {
            if !dir.is_over_limit() {
                break;
            }
            // Only pay the try_remove cost if this inode actually has a staging file.
            if dir.file_size(ino) == 0 {
                continue;
            }
            if self.gc_one(ino, inodes).await {
                count += 1;
                debug!("staging GC: removed ino={}", ino);
            }
        }
        count
    }
}
