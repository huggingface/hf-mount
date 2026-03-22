use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use tracing::{info, warn};

use crate::hub_api::HubOps;

use super::Invalidator;
use super::inode::{self, InodeTable};

impl super::VirtualFs {
    /// Background task: polls Hub API tree listing to detect remote changes.
    pub(super) async fn poll_remote_changes(
        hub_client: Arc<dyn HubOps>,
        inodes: Arc<RwLock<InodeTable>>,
        negative_cache: Arc<RwLock<HashMap<String, Instant>>>,
        invalidator: Invalidator,
        interval: Duration,
    ) {
        loop {
            tokio::time::sleep(interval).await;

            let remote_entries = match hub_client.list_tree("", true).await {
                Ok(entries) => entries,
                Err(e) => {
                    warn!("Remote poll failed: {}", e);
                    continue;
                }
            };

            let remote_map: HashMap<String, _> = remote_entries
                .iter()
                .filter(|e| e.entry_type == "file")
                .map(|e| (e.path.clone(), e))
                .collect();

            // Take snapshot under lock, then release to avoid blocking VFS ops
            let snapshot = inodes.read().expect("inodes poisoned").file_snapshot();

            // Phase 1: Compute diff (no lock held)
            struct Update {
                ino: u64,
                hash: Option<String>,
                etag: Option<String>,
                size: u64,
                mtime: SystemTime,
            }
            let mut updates = Vec::new();
            let mut deletions = Vec::new();

            for (ino, path, local_hash, local_etag, local_size, is_dirty) in &snapshot {
                // Skip locally-modified files: local writes take precedence until flushed.
                if *is_dirty {
                    continue;
                }
                match remote_map.get(path.as_str()) {
                    Some(remote) => {
                        let remote_hash = remote.xet_hash.as_deref();
                        let remote_oid = remote.oid.as_deref();
                        let remote_size = remote.size.unwrap_or(0);
                        // Detect changes via xet_hash (preferred) or oid (= etag).
                        let changed = if local_hash.is_some() || remote_hash.is_some() {
                            remote_hash != local_hash.as_deref()
                        } else {
                            remote_oid != local_etag.as_deref()
                        };

                        if changed || remote_size != *local_size {
                            let mtime = remote
                                .mtime
                                .as_deref()
                                .map(crate::hub_api::mtime_from_str)
                                .unwrap_or(SystemTime::now());
                            updates.push(Update {
                                ino: *ino,
                                hash: remote_hash.map(|s| s.to_string()),
                                etag: remote_oid.map(|s| s.to_string()),
                                size: remote_size,
                                mtime,
                            });
                            info!("Remote update detected: {}", path);
                        }
                    }
                    None => {
                        info!("Remote deletion detected: {}", path);
                        deletions.push(*ino);
                    }
                }
            }

            // Phase 2: Apply mutations under lock, collect inodes to invalidate
            let mut inos_to_invalidate: Vec<u64> = Vec::new();
            let dirs_to_invalidate_kernel: Vec<u64>;
            {
                let mut inode_table = inodes.write().expect("inodes poisoned");

                for update in &updates {
                    inode_table.update_remote_file(
                        update.ino,
                        update.hash.clone(),
                        update.etag.clone(),
                        update.size,
                        update.mtime,
                    );
                    inos_to_invalidate.push(update.ino);
                }

                for ino in &deletions {
                    // Invalidate the deleted inode and its parent dir so the
                    // kernel drops the dentry and page cache for this file.
                    if let Some(entry) = inode_table.get(*ino) {
                        let parent_ino = entry.parent;
                        inos_to_invalidate.push(parent_ino);
                    }
                    inos_to_invalidate.push(*ino);
                    inode_table.remove(*ino);
                }

                // Phase 3: New remote files -> invalidate parent dir + negative cache
                let mut dirs_to_invalidate = HashSet::new();
                let mut dir_paths_to_invalidate = Vec::new();
                for path in remote_map.keys() {
                    if inode_table.get_by_path(path).is_none() {
                        let mut ancestor = path.as_str();
                        loop {
                            ancestor = match ancestor.rsplit_once('/') {
                                Some((parent, _)) => parent,
                                None => "",
                            };
                            if let Some(dir_ino) = inode_table.get_dir_ino(ancestor) {
                                if dirs_to_invalidate.insert(dir_ino) {
                                    dir_paths_to_invalidate.push(ancestor.to_string());
                                }
                                break;
                            }
                            if ancestor.is_empty() {
                                if dirs_to_invalidate.insert(inode::ROOT_INODE) {
                                    dir_paths_to_invalidate.push(String::new());
                                }
                                break;
                            }
                        }
                    }
                }

                // Clear cached children so next readdir re-fetches from Hub API,
                // then invalidate kernel page cache (done outside lock).
                dirs_to_invalidate_kernel = dirs_to_invalidate.into_iter().collect();
                for dir_ino in &dirs_to_invalidate_kernel {
                    inode_table.invalidate_children(*dir_ino);
                }

                // Invalidate negative cache entries under changed directories
                if !dir_paths_to_invalidate.is_empty() {
                    let mut nc = negative_cache.write().expect("neg_cache poisoned");
                    for dir_path in &dir_paths_to_invalidate {
                        let prefix = if dir_path.is_empty() {
                            String::new()
                        } else {
                            format!("{}/", dir_path)
                        };
                        nc.retain(|k, _| {
                            if dir_path.is_empty() {
                                false
                            } else {
                                !k.starts_with(&prefix) && k != dir_path
                            }
                        });
                    }
                }
            }

            // Phase 4: Invalidate kernel page cache (outside lock scope)
            if let Some(invalidate) = invalidator.lock().expect("invalidator poisoned").as_ref() {
                for ino in &inos_to_invalidate {
                    invalidate(*ino);
                }
                for dir_ino in &dirs_to_invalidate_kernel {
                    invalidate(*dir_ino);
                }
            }
        }
    }
}
