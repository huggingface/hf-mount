use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use futures::stream::{self, StreamExt};
use tracing::{info, warn};

use crate::error::Error;
use crate::hub_api::HubOps;

use super::Invalidator;
use super::inode::{self, InodeTable};

/// Cap on the exponential-backoff multiplier applied to the poll interval
/// when we keep getting 401s. With `interval = 30s` and `MAX_AUTH_BACKOFF_EXP = 6`,
/// the max delay between polls becomes `30s * 2^6 = 32 min`.
const MAX_AUTH_BACKOFF_EXP: u32 = 6;

impl super::VirtualFs {
    /// Background task: polls Hub API tree listing to detect remote changes.
    ///
    /// `listing_concurrency` caps concurrent tree-listing requests per poll
    /// round. Without this, every loaded directory prefix is fetched in
    /// parallel, which produces a thundering-herd burst against the Hub API
    /// and triggers 504s on large mounts (e.g. transformers/docs). It is also
    /// the main knob to throttle hf-mount's load on the Hub `/api` endpoint
    /// when many mounts share the same upstream (e.g. Spaces).
    pub(super) async fn poll_remote_changes(
        hub_client: Arc<dyn HubOps>,
        inodes: Arc<RwLock<InodeTable>>,
        negative_cache: Arc<RwLock<HashMap<String, Instant>>>,
        invalidator: Invalidator,
        interval: Duration,
        listing_concurrency: usize,
    ) {
        // Exponent applied to `interval` when the Hub returns 401 (token expired
        // or revoked). Reset to 0 as soon as we see a successful round.
        let mut auth_backoff_exp: u32 = 0;
        // Last successful revision probe (repo commit sha or bucket updatedAt).
        // When the next probe matches this, the tree fan-out is skipped — most
        // mounts are inactive most of the time, so this collapses the per-round
        // cost from N requests to 1.
        //
        // Primed with an initial probe at startup so the first round after the
        // initial mount-time listing doesn't redundantly fan out when nothing
        // has changed. On probe error we leave it at None, the first round will
        // do a full fan-out and re-prime.
        let mut last_revision: Option<String> = hub_client.probe_revision().await.ok();
        loop {
            tokio::time::sleep(interval.saturating_mul(1u32 << auth_backoff_exp)).await;

            // Cheap probe: if the source hasn't changed since last poll, skip the
            // full fan-out entirely. On probe error, fall through to the full
            // fan-out — the probe is an optimization, not a gate.
            match hub_client.probe_revision().await {
                Ok(rev) => {
                    if last_revision.as_ref() == Some(&rev) {
                        continue;
                    }
                    last_revision = Some(rev);
                }
                Err(e) => {
                    if matches!(e, Error::Hub { status: Some(401), .. }) {
                        auth_backoff_exp = (auth_backoff_exp + 1).min(MAX_AUTH_BACKOFF_EXP);
                        warn!(
                            "Revision probe saw 401 Unauthorized; backing off next poll to {:?}",
                            interval.saturating_mul(1u32 << auth_backoff_exp)
                        );
                        continue;
                    }
                    warn!("Revision probe failed, falling back to full poll: {e}");
                }
            }

            // Only poll directories the user has actually visited (children_loaded).
            // This avoids fetching the entire tree for large repos where most
            // directories have never been accessed.
            let prefixes = inodes.read().expect("inodes poisoned").loaded_dir_prefixes();
            // buffer_unordered yields out of order, so carry the prefix alongside the result.
            let results: Vec<(String, _)> = stream::iter(prefixes)
                .map(|prefix| {
                    let client = hub_client.clone();
                    async move {
                        let result = client.list_tree(&prefix).await;
                        (prefix, result)
                    }
                })
                .buffer_unordered(listing_concurrency)
                .collect()
                .await;
            let mut all_entries = Vec::new();
            let mut polled_prefixes = HashSet::new();
            let mut failed_prefixes = Vec::new();
            let mut saw_auth_failure = false;
            for (prefix, result) in results {
                match result {
                    Ok(entries) => {
                        polled_prefixes.insert(prefix);
                        all_entries.extend(entries);
                    }
                    Err(e) => {
                        if matches!(e, Error::Hub { status: Some(401), .. }) {
                            saw_auth_failure = true;
                        }
                        warn!("Remote poll failed for prefix '{prefix}': {e}");
                        failed_prefixes.push(prefix);
                    }
                }
            }
            if saw_auth_failure {
                auth_backoff_exp = (auth_backoff_exp + 1).min(MAX_AUTH_BACKOFF_EXP);
                warn!(
                    "Remote poll saw 401 Unauthorized; backing off next poll to {:?}",
                    interval.saturating_mul(1u32 << auth_backoff_exp)
                );
            } else if auth_backoff_exp > 0 {
                info!("Remote poll auth recovered, resetting backoff");
                auth_backoff_exp = 0;
            }
            // For failed prefixes, check if the parent was polled successfully
            // and the dir no longer appears in its listing. If so, the dir was
            // deleted remotely — mark it as polled so its files get cleaned up.
            // Sort by depth (parents first) so nested deletions cascade correctly.
            failed_prefixes.sort_by_key(|p| p.matches('/').count());
            for failed in &failed_prefixes {
                let parent = failed.rsplit_once('/').map_or("", |(p, _)| p);
                if polled_prefixes.contains(parent) {
                    let dir_still_exists = all_entries
                        .iter()
                        .any(|e| e.entry_type == "directory" && e.path == *failed);
                    if !dir_still_exists {
                        info!("Remote directory deletion detected: {}", failed);
                        polled_prefixes.insert(failed.clone());
                    }
                }
            }
            Self::apply_poll_diff(all_entries, &polled_prefixes, &inodes, &negative_cache, &invalidator);
        }
    }

    /// Apply a single poll diff: compare remote entries against the inode table,
    /// detect updates/deletions/creations, and invalidate affected directories.
    /// Extracted from the poll loop for testability.
    /// `polled_prefixes`: the set of directory prefixes that were successfully fetched.
    /// Only files under these prefixes are eligible for deletion detection. This prevents
    /// spurious deletions when a prefix fetch fails or when a directory was invalidated
    /// between poll cycles.
    pub(super) fn apply_poll_diff(
        remote_entries: Vec<crate::hub_api::TreeEntry>,
        polled_prefixes: &HashSet<String>,
        inodes: &Arc<RwLock<InodeTable>>,
        negative_cache: &Arc<RwLock<HashMap<String, Instant>>>,
        invalidator: &Invalidator,
    ) {
        let remote_map: HashMap<String, _> = remote_entries
            .iter()
            .filter(|e| e.entry_type == "file")
            .map(|e| (e.path.clone(), e))
            .collect();

        // All remote paths (including directories) for new-entry detection.
        // Non-recursive listings return subdirs as directory entries, not nested file paths.
        let all_remote_paths: HashSet<&str> = remote_entries.iter().map(|e| e.path.as_str()).collect();

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
                    // Only treat as deleted if the file's parent directory was
                    // successfully polled. Otherwise the file is simply in a dir
                    // whose fetch failed or that was invalidated between cycles.
                    let parent_prefix = path.rsplit_once('/').map_or("", |(p, _)| p);
                    if polled_prefixes.contains(parent_prefix) {
                        info!("Remote deletion detected: {}", path);
                        deletions.push(*ino);
                    }
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
                // Re-check under write lock: inode may have been removed or
                // dirtied between the read-lock snapshot and now.
                let (parent_ino, name) = match inode_table.get(*ino) {
                    Some(entry) if entry.is_dirty() => continue,
                    Some(entry) => (entry.parent, entry.name.clone()),
                    None => continue,
                };
                if inode_table.has_open_handles(*ino) {
                    // Unlink the pathname but keep the inode as orphan (nlink=0)
                    // so open handles can still read/fstat. release() will clean
                    // up the orphan. Without this, the file stays visible by name
                    // and a recreated file at the same path would collide.
                    inode_table.unlink_one(parent_ino, &name);
                    info!(
                        "Remote deletion of ino={}: unlinked path, kept orphan (open handles)",
                        ino
                    );
                } else {
                    inode_table.remove(*ino);
                }
                inos_to_invalidate.push(parent_ino);
                inos_to_invalidate.push(*ino);
            }

            // Phase 3: New remote entries (files AND directories) -> invalidate parent dir.
            // Use all_remote_paths (not just files) so new subdirectories also trigger
            // parent invalidation. Only invalidate directories whose children have been
            // loaded — unloaded dirs contain entries that are simply unexplored, not new.
            let mut dirs_to_invalidate = HashSet::new();
            let mut dir_paths_to_invalidate = Vec::new();
            for path in &all_remote_paths {
                if inode_table.get_by_path(path).is_none() {
                    let mut ancestor: &str = path;
                    loop {
                        ancestor = match ancestor.rsplit_once('/') {
                            Some((parent, _)) => parent,
                            None => "",
                        };
                        if let Some(dir_ino) = inode_table.get_dir_ino(ancestor) {
                            // Only invalidate if this directory was already loaded.
                            // If not loaded, the "missing" file is just unexplored.
                            if inode_table.is_children_loaded(dir_ino) && dirs_to_invalidate.insert(dir_ino) {
                                dir_paths_to_invalidate.push(ancestor.to_string());
                            }
                            break;
                        }
                        if ancestor.is_empty() {
                            if inode_table.is_children_loaded(inode::ROOT_INODE)
                                && dirs_to_invalidate.insert(inode::ROOT_INODE)
                            {
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
        if let Some(invalidate) = invalidator.get() {
            for ino in &inos_to_invalidate {
                invalidate(*ino);
            }
            for dir_ino in &dirs_to_invalidate_kernel {
                invalidate(*dir_ino);
            }
        }
    }
}
