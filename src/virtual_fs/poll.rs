use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use futures::stream::{self, StreamExt};
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::follow::{FollowChange, FollowEvent, FollowOp};
use crate::hub_api::HubOps;

use super::inode::{InodeKind, InodeTable};
use super::{InvalKind, Invalidator};

/// Cap on the exponential-backoff multiplier applied to the poll interval
/// when the Hub keeps failing with 401 (token expired) or a transient status
/// (429/5xx — polling harder only feeds the storm and starves interactive
/// lookups of quota). With `interval = 30s` and `MAX_BACKOFF_EXP = 6`, the
/// max delay between polls becomes `30s * 2^6 = 32 min`.
const MAX_BACKOFF_EXP: u32 = 6;

/// Record a remote metadata change on a materialized file and queue its
/// kernel invalidation. An inode with open handles may have an in-flight
/// read holding its folio locks; a full page invalidation would block in
/// the kernel waiting on that lock and deadlock against the read (#195), so
/// such inodes get an attribute-only invalidation that never touches pages.
fn update_remote_file(
    inode_table: &mut InodeTable,
    ino: u64,
    hash: Option<String>,
    etag: Option<String>,
    size: u64,
    mtime: SystemTime,
    inos_to_invalidate: &mut Vec<(u64, InvalKind)>,
) {
    inode_table.update_remote_file(ino, hash, etag, size, mtime);
    let kind = if inode_table.has_open_handles(ino) {
        InvalKind::AttrOnly
    } else {
        InvalKind::Pages
    };
    inos_to_invalidate.push((ino, kind));
}

/// Drop a remotely deleted file from the inode table and queue the parent
/// directory + inode invalidations. Returns `false` when the inode is gone
/// or dirty (local writes take precedence until flushed).
fn remove_remote_file(inode_table: &mut InodeTable, ino: u64, inos_to_invalidate: &mut Vec<(u64, InvalKind)>) -> bool {
    let (parent_ino, name) = match inode_table.get(ino) {
        Some(entry) if entry.is_dirty() => return false,
        Some(entry) => (entry.parent, entry.name.clone()),
        None => return false,
    };
    let ino_kind = if inode_table.has_open_handles(ino) {
        // Unlink the pathname but keep the inode as orphan (nlink=0)
        // so open handles can still read/fstat. release() will clean
        // up the orphan. Without this, the file stays visible by name
        // and a recreated file at the same path would collide.
        inode_table.unlink_one(parent_ino, &name);
        info!("Remote deletion of ino={ino}: unlinked path, kept orphan (open handles)");
        // Open handles → an in-flight read may hold this inode's
        // folios; never issue a blocking page invalidation against it (#195).
        InvalKind::AttrOnly
    } else {
        inode_table.remove(ino);
        InvalKind::Pages
    };
    inos_to_invalidate.push((parent_ino, InvalKind::Pages));
    inos_to_invalidate.push((ino, ino_kind));
    true
}

/// Statuses that should slow the poll loop down: expired token (401) or any
/// transient failure (rate limit / server overload) where re-polling at full
/// rate only makes things worse.
fn should_back_off(e: &Error) -> bool {
    matches!(e.status(), Some(401)) || e.is_transient()
}

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
        live_follow: bool,
    ) {
        // None forces a full fan-out next round; primed with an initial probe so
        // a freshly mounted source doesn't redundantly re-list once.
        let mut last_revision: Option<String> = hub_client.probe_revision().await.ok();

        // Prefer the Hub's bucket live-follow SSE feed: while the stream is
        // healthy, per-file changes are applied as they happen and the
        // probe + fan-out below never runs. `follow_remote_changes` returns
        // only when the endpoint is permanently unavailable (400/404: older
        // Hub deployment, or a repo source) — then the interval poll below
        // takes over unchanged. The follow loop uses the probe above as its
        // own baseline and doesn't re-probe before the first connect.
        if live_follow {
            Self::follow_remote_changes(
                &hub_client,
                &inodes,
                &negative_cache,
                &invalidator,
                listing_concurrency,
                &mut last_revision,
            )
            .await;
            info!("Live-follow endpoint unavailable for this source; using interval polling");
        }

        // Exponent applied to `interval` while the Hub keeps failing (401 or
        // transient statuses). Reset to 0 as soon as we see a successful round.
        let mut backoff_exp: u32 = 0;
        loop {
            tokio::time::sleep(interval.saturating_mul(1u32 << backoff_exp)).await;

            match hub_client.probe_revision().await {
                Ok(rev) => {
                    // A successful probe means the Hub recovered: reset the
                    // backoff even when the revision is unchanged, otherwise a
                    // healthy-but-quiet mount stays at the max poll interval
                    // until the next remote change.
                    if backoff_exp > 0 {
                        info!("Revision probe recovered, resetting backoff");
                        backoff_exp = 0;
                    }
                    if last_revision.as_ref() == Some(&rev) {
                        debug!("Revision unchanged ({rev}); skipping tree fan-out");
                        continue;
                    }
                    debug!("Revision changed to {rev}; running full poll");
                    last_revision = Some(rev);
                }
                Err(e) => {
                    if should_back_off(&e) {
                        backoff_exp = (backoff_exp + 1).min(MAX_BACKOFF_EXP);
                        warn!(
                            "Revision probe saw {}; backing off next poll to {:?}",
                            e,
                            interval.saturating_mul(1u32 << backoff_exp)
                        );
                        continue;
                    }
                    warn!("Revision probe failed, falling back to full poll: {e}");
                }
            }

            let saw_backoff_status =
                Self::poll_round(&hub_client, &inodes, &negative_cache, &invalidator, listing_concurrency).await;
            if saw_backoff_status {
                backoff_exp = (backoff_exp + 1).min(MAX_BACKOFF_EXP);
                warn!(
                    "Remote poll saw 401/transient failures; backing off next poll to {:?}",
                    interval.saturating_mul(1u32 << backoff_exp)
                );
            } else if backoff_exp > 0 {
                info!("Remote poll recovered, resetting backoff");
                backoff_exp = 0;
            }
        }
    }

    /// One full reconcile round: list every loaded directory prefix and diff
    /// the result against the inode table. Returns `true` when a listing
    /// failed with a status the caller should back off on (401/transient).
    pub(super) async fn poll_round(
        hub_client: &Arc<dyn HubOps>,
        inodes: &Arc<RwLock<InodeTable>>,
        negative_cache: &Arc<RwLock<HashMap<String, Instant>>>,
        invalidator: &Invalidator,
        listing_concurrency: usize,
    ) -> bool {
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
        let mut saw_backoff_status = false;
        for (prefix, result) in results {
            match result {
                Ok(entries) => {
                    polled_prefixes.insert(prefix);
                    all_entries.extend(entries);
                }
                Err(e) => {
                    if should_back_off(&e) {
                        saw_backoff_status = true;
                    }
                    warn!("Remote poll failed for prefix '{prefix}': {e}");
                    failed_prefixes.push(prefix);
                }
            }
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
        Self::apply_poll_diff(all_entries, &polled_prefixes, inodes, negative_cache, invalidator);
        saw_backoff_status
    }

    /// Drive the bucket live-follow SSE feed (see `crate::follow`). Returns
    /// only when the endpoint is permanently unavailable (400/404) so the
    /// caller can fall back to interval polling; every other failure retries
    /// here. While a stream is healthy this fully replaces the periodic
    /// probe + fan-out.
    ///
    /// Resume protocol: reconnect with the last received cursor (server
    /// resumes strictly after it). Without a cursor — the server buffer
    /// expired (`reset`), or a stream ended before handing one out — re-probe
    /// `updatedAt`: if the bucket moved since `last_revision`, one full poll
    /// round reconciles, then the subscription starts from the fresh
    /// `updatedAt` (`since=`) so nothing between the round and the
    /// subscription is missed. The first connect skips the probe: the caller
    /// just primed `last_revision` against a freshly listed mount.
    async fn follow_remote_changes(
        hub_client: &Arc<dyn HubOps>,
        inodes: &Arc<RwLock<InodeTable>>,
        negative_cache: &Arc<RwLock<HashMap<String, Instant>>>,
        invalidator: &Invalidator,
        listing_concurrency: usize,
        last_revision: &mut Option<String>,
    ) {
        /// Wait before reconnecting after a failed connect (503 without a
        /// `Retry-After` hint, network error, …).
        const DEFAULT_RETRY: Duration = Duration::from_secs(10);
        /// A stream that dies this quickly after connecting (without the
        /// server directing the end) is not healthy: pause before
        /// reconnecting so a broken feed doesn't become a hot loop against
        /// the Hub.
        const MIN_SESSION: Duration = Duration::from_secs(2);
        const RECONNECT_PAUSE: Duration = Duration::from_secs(5);

        let mut cursor: Option<String> = None;
        let mut first_connect = true;
        loop {
            // The caller primed `last_revision` right before the first
            // connect; only re-probe there if that priming failed.
            let primed = first_connect && last_revision.is_some();
            if cursor.is_none() && !primed {
                match hub_client.probe_revision().await {
                    Ok(rev) => {
                        if last_revision.as_ref() != Some(&rev) {
                            debug!("live-follow: no cursor and revision moved; running a full poll round");
                            Self::poll_round(hub_client, inodes, negative_cache, invalidator, listing_concurrency)
                                .await;
                        }
                        *last_revision = Some(rev);
                    }
                    Err(e) => debug!("live-follow: revision probe failed ({e}); resuming from the last known one"),
                }
            }
            first_connect = false;
            let since = if cursor.is_none() { last_revision.clone() } else { None };
            let mut stream = match hub_client.follow_events(cursor.as_deref(), since.as_deref()).await {
                Ok(stream) => stream,
                // Older Hub deployment (404) or a request it refuses to serve
                // (400): the feed will not appear mid-mount, fall back for good.
                Err(e) if matches!(e.status(), Some(400) | Some(404)) => {
                    debug!("live-follow: endpoint not available ({e})");
                    return;
                }
                Err(e) => {
                    let delay = e.retry_after().unwrap_or(DEFAULT_RETRY);
                    warn!("live-follow: connect failed ({e}); retrying in {delay:?}");
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };
            let connected_at = Instant::now();
            // reset/reconnect are server-directed ends: reconnect immediately
            // (their cadence is server-controlled). Anything else that ends
            // the stream gets the MIN_SESSION guard below.
            let server_directed_end = loop {
                match stream.next_event().await {
                    // An absent cursor on ready/reconnect means the feed has
                    // seen no change yet: keep the current resume point.
                    Ok(Some(FollowEvent::Ready { cursor: c })) => {
                        debug!("live-follow: ready (cursor={c:?})");
                        cursor = c.or(cursor);
                    }
                    Ok(Some(FollowEvent::Changes { cursor: c, changes })) => {
                        debug!("live-follow: applying {} change(s)", changes.len());
                        Self::apply_follow_changes(&changes, inodes, negative_cache, invalidator);
                        cursor = Some(c);
                    }
                    Ok(Some(FollowEvent::Reset)) => {
                        info!("live-follow: resume point older than the server buffer; re-listing");
                        cursor = None; // next connect: probe + full round + fresh `since`
                        break true;
                    }
                    Ok(Some(FollowEvent::Reconnect { cursor: c })) => {
                        debug!("live-follow: server asked to reconnect (cursor={c:?})");
                        cursor = c.or(cursor);
                        break true;
                    }
                    // Any other end of stream (TCP close, read timeout after
                    // missed pings, transport error) = reconnect with the
                    // last cursor received.
                    Ok(None) => {
                        debug!("live-follow: stream ended; reconnecting");
                        break false;
                    }
                    Err(e) => {
                        warn!("live-follow: stream error ({e}); reconnecting");
                        break false;
                    }
                }
            };
            if !server_directed_end && connected_at.elapsed() < MIN_SESSION {
                tokio::time::sleep(RECONNECT_PAUSE).await;
            }
        }
    }

    /// Apply one live-follow `changes` batch.
    ///
    /// v1 semantics, mirroring what `apply_poll_diff` derives from a full
    /// re-list — but per changed path instead of per loaded directory:
    /// - Files already materialized in the inode table are updated/removed
    ///   in place through the same mutation primitives as the poll diff
    ///   (including the open-handles `AttrOnly` invalidation rule from #195)
    ///   so content hand-off doesn't wait for a TTL. Dirty inodes are never
    ///   touched — local writes win until flushed.
    /// - Every other path (not materialized, or a directory) drops the
    ///   cached listing of its nearest *loaded* ancestor directory
    ///   (`invalidate_children`), so the next lookup/readdir re-lists just
    ///   that directory. In-place mutations keep the listing consistent, so
    ///   a hot directory isn't re-listed on every batch.
    /// - `add`/`update` clear negative-cache entries for the path and every
    ///   ancestor, so a consumer that probed the path before it existed sees
    ///   it on the next lookup instead of after `--negative-ttl-ms`.
    pub(super) fn apply_follow_changes(
        changes: &[FollowChange],
        inodes: &Arc<RwLock<InodeTable>>,
        negative_cache: &Arc<RwLock<HashMap<String, Instant>>>,
        invalidator: &Invalidator,
    ) {
        let mut inos_to_invalidate: Vec<(u64, InvalKind)> = Vec::new();
        let mut dirs_to_invalidate: HashSet<u64> = HashSet::new();
        {
            let mut inode_table = inodes.write().expect("inodes poisoned");
            for change in changes {
                let existing = inode_table
                    .get_by_path(&change.path)
                    .filter(|e| e.kind == InodeKind::File)
                    .map(|e| {
                        (
                            e.inode,
                            e.is_dirty(),
                            e.xet_hash.clone(),
                            e.etag.clone(),
                            e.size,
                            e.mtime,
                        )
                    });
                let Some((ino, is_dirty, local_hash, local_etag, local_size, local_mtime)) = existing else {
                    // Not materialized: an entry appeared or vanished in the
                    // nearest existing ancestor directory (possibly behind an
                    // intermediate dir we never listed), so its cached listing
                    // is stale. Unloaded dirs need nothing: their first listing
                    // will be fresh.
                    let dir_ino = inode_table.nearest_dir_ancestor(&change.path);
                    if inode_table.is_children_loaded(dir_ino) {
                        dirs_to_invalidate.insert(dir_ino);
                    }
                    continue;
                };
                if is_dirty {
                    continue; // local writes take precedence until flushed
                }
                match change.op {
                    FollowOp::Delete => {
                        if remove_remote_file(&mut inode_table, ino, &mut inos_to_invalidate) {
                            info!("live-follow: remote deletion of {}", change.path);
                        }
                    }
                    FollowOp::Add | FollowOp::Update => {
                        // An update carries only the fields that changed;
                        // absent fields keep their current value (an omitted
                        // xetHash means "unchanged or not readable with this
                        // token", never "cleared").
                        let new_hash = change.xet_hash.clone().or(local_hash);
                        let new_size = change.size.unwrap_or(local_size);
                        let mtime = change
                            .mtime
                            .as_deref()
                            .or(change.uploaded_at.as_deref())
                            .map_or(local_mtime, crate::hub_api::mtime_from_str);
                        update_remote_file(
                            &mut inode_table,
                            ino,
                            new_hash,
                            local_etag,
                            new_size,
                            mtime,
                            &mut inos_to_invalidate,
                        );
                        info!("live-follow: remote update of {}", change.path);
                    }
                }
            }

            for dir_ino in &dirs_to_invalidate {
                inode_table.invalidate_children(*dir_ino);
            }
        }

        // An added/updated path must be visible on the next lookup: clear
        // the negative cache for the path and every ancestor dir (any of
        // them may have been probed and cached as missing before existing).
        {
            let mut nc = negative_cache.write().expect("neg_cache poisoned");
            for change in changes.iter().filter(|c| c.op != FollowOp::Delete) {
                let mut path: &str = &change.path;
                loop {
                    nc.remove(path);
                    match path.rsplit_once('/') {
                        Some((parent, _)) => path = parent,
                        None => break,
                    }
                }
            }
        }

        // Kernel invalidation outside the lock scope (see apply_poll_diff):
        // directories drop their cached readdir pages so the next readdir
        // re-fetches through the now-invalidated listing.
        if let Some(invalidate) = invalidator.get() {
            for (ino, kind) in &inos_to_invalidate {
                invalidate(*ino, *kind);
            }
            for dir_ino in &dirs_to_invalidate {
                invalidate(*dir_ino, InvalKind::Pages);
            }
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
            match remote_map.get(path.as_ref()) {
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

        // Phase 2: Apply mutations under lock, collect inodes to invalidate.
        let mut inos_to_invalidate: Vec<(u64, InvalKind)> = Vec::new();
        let dirs_to_invalidate_kernel: Vec<u64>;
        {
            let mut inode_table = inodes.write().expect("inodes poisoned");

            for update in updates {
                update_remote_file(
                    &mut inode_table,
                    update.ino,
                    update.hash,
                    update.etag,
                    update.size,
                    update.mtime,
                    &mut inos_to_invalidate,
                );
            }

            // Re-checked under the write lock by the helper: the inode may
            // have been removed or dirtied since the read-lock snapshot.
            for ino in &deletions {
                remove_remote_file(&mut inode_table, *ino, &mut inos_to_invalidate);
            }

            // Phase 3: New remote entries (files AND directories) -> invalidate parent dir.
            // Use all_remote_paths (not just files) so new subdirectories also trigger
            // parent invalidation. Only invalidate directories whose children have been
            // loaded — unloaded dirs contain entries that are simply unexplored, not new.
            let mut dirs_to_invalidate = HashSet::new();
            let mut dir_paths_to_invalidate = Vec::new();
            for path in &all_remote_paths {
                if inode_table.get_by_path(path).is_none() {
                    let dir_ino = inode_table.nearest_dir_ancestor(path);
                    // Only invalidate if this directory was already loaded.
                    // If not loaded, the "missing" file is just unexplored.
                    if inode_table.is_children_loaded(dir_ino) && dirs_to_invalidate.insert(dir_ino) {
                        let dir_path = inode_table.get(dir_ino).map(|e| e.full_path.to_string());
                        dir_paths_to_invalidate.push(dir_path.unwrap_or_default());
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
            for (ino, kind) in &inos_to_invalidate {
                invalidate(*ino, *kind);
            }
            // Directories: drop the cached readdir so the next readdir re-fetches.
            // readdir doesn't hold the folio-read locks that make page drops
            // deadlock, so a full invalidation is safe here.
            for dir_ino in &dirs_to_invalidate_kernel {
                invalidate(*dir_ino, InvalKind::Pages);
            }
        }
    }
}
