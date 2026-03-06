use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const ROOT_INODE: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeKind {
    File,
    Directory,
    Symlink,
}

/// A directory child entry: stores the name on the edge (needed for hard links,
/// where the same inode can appear under different names in different directories).
#[derive(Debug, Clone)]
pub struct DirChild {
    pub ino: u64,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct InodeEntry {
    pub inode: u64,
    pub parent: u64,
    pub name: String,
    pub full_path: String,
    pub kind: InodeKind,
    pub size: u64,
    pub mtime: SystemTime,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub atime: SystemTime,
    pub ctime: SystemTime,
    pub nlink: u32,
    pub symlink_target: Option<String>,
    pub xet_hash: Option<String>,
    /// ETag from the last HEAD revalidation (used for non-xet plain git/LFS files).
    pub etag: Option<String>,
    pub dirty: bool,
    pub children_loaded: bool,
    pub children: Vec<DirChild>,
    /// Old remote paths that should be deleted on next flush (set by rename of dirty files).
    pub pending_deletes: Vec<String>,
    /// When this inode's metadata was last validated against the remote (via HEAD).
    /// Used to avoid redundant HEAD requests within the revalidation TTL.
    pub last_revalidated: Option<Instant>,
}

pub struct InodeTable {
    inodes: HashMap<u64, InodeEntry>,
    path_to_inode: HashMap<String, u64>,
    next_inode: AtomicU64,
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}

impl InodeTable {
    pub fn new() -> Self {
        let mut table = Self {
            inodes: HashMap::new(),
            path_to_inode: HashMap::new(),
            next_inode: AtomicU64::new(2),
        };

        // Create root inode
        let root = InodeEntry {
            inode: ROOT_INODE,
            parent: ROOT_INODE,
            name: String::new(),
            full_path: String::new(),
            kind: InodeKind::Directory,
            size: 0,
            mtime: UNIX_EPOCH,
            mode: 0o755,
            uid: 0,
            gid: 0,
            atime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            nlink: 2,
            symlink_target: None,
            xet_hash: None,
            etag: None,
            dirty: false,
            children_loaded: false,
            children: Vec::new(),
            pending_deletes: Vec::new(),
            last_revalidated: None,
        };
        table.inodes.insert(ROOT_INODE, root);
        table.path_to_inode.insert(String::new(), ROOT_INODE);

        table
    }

    pub fn get(&self, inode: u64) -> Option<&InodeEntry> {
        self.inodes.get(&inode)
    }

    pub fn get_mut(&mut self, inode: u64) -> Option<&mut InodeEntry> {
        self.inodes.get_mut(&inode)
    }

    pub fn get_by_path(&self, path: &str) -> Option<&InodeEntry> {
        self.path_to_inode.get(path).and_then(|ino| self.inodes.get(ino))
    }

    /// Find a child of `parent` by name.
    pub fn lookup_child(&self, parent: u64, name: &str) -> Option<&InodeEntry> {
        let parent_entry = self.inodes.get(&parent)?;
        for child in &parent_entry.children {
            if child.name == name {
                return self.inodes.get(&child.ino);
            }
        }
        None
    }

    /// Insert a new inode, returning its inode number.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        parent: u64,
        name: String,
        full_path: String,
        kind: InodeKind,
        size: u64,
        mtime: SystemTime,
        xet_hash: Option<String>,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> u64 {
        // Check if already exists
        if let Some(&existing_ino) = self.path_to_inode.get(&full_path) {
            debug_assert!(
                self.inodes.get(&existing_ino).map(|e| e.kind) == Some(kind),
                "insert(): path '{}' exists with different kind",
                full_path
            );
            // Stamp revalidation so subsequent lookups skip the HEAD request.
            if let Some(entry) = self.inodes.get_mut(&existing_ino) {
                entry.last_revalidated = Some(Instant::now());
            }
            return existing_ino;
        }

        debug_assert!(
            self.inodes.contains_key(&parent),
            "insert(): parent inode {} does not exist (path '{}')",
            parent,
            full_path
        );

        let now = SystemTime::now();
        let nlink = match kind {
            InodeKind::Directory => 2,
            _ => 1,
        };

        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        let child_name = name.clone();
        let entry = InodeEntry {
            inode,
            parent,
            name,
            full_path: full_path.clone(),
            kind,
            size,
            mtime,
            mode,
            uid,
            gid,
            atime: mtime,
            ctime: now,
            nlink,
            symlink_target: None,
            xet_hash,
            etag: None,
            dirty: false,
            children_loaded: kind != InodeKind::Directory, // only dirs have children to load
            children: Vec::new(),
            pending_deletes: Vec::new(),
            last_revalidated: Some(Instant::now()),
        };

        self.inodes.insert(inode, entry);
        self.path_to_inode.insert(full_path, inode);

        // Add to parent's children
        if let Some(parent_entry) = self.inodes.get_mut(&parent) {
            parent_entry.children.push(DirChild {
                ino: inode,
                name: child_name,
            });
            // POSIX: new subdirectory's ".." links to parent
            if kind == InodeKind::Directory {
                parent_entry.nlink += 1;
            }
        }

        inode
    }

    /// Remove a path → inode mapping.
    pub fn remove_path(&mut self, path: &str) {
        self.path_to_inode.remove(path);
    }

    /// Insert a path → inode mapping.
    pub fn insert_path(&mut self, path: String, inode: u64) {
        self.path_to_inode.insert(path, inode);
    }

    /// Return inodes of all dirty files (excludes symlinks — they have no content to flush).
    pub fn dirty_inos(&self) -> Vec<u64> {
        self.inodes
            .values()
            .filter(|e| e.kind == InodeKind::File && e.dirty && e.nlink > 0)
            .map(|e| e.inode)
            .collect()
    }

    /// Snapshot of all file entries: (ino, full_path, xet_hash, etag, size, dirty)
    #[allow(clippy::type_complexity)]
    pub fn file_snapshot(&self) -> Vec<(u64, String, Option<String>, Option<String>, u64, bool)> {
        self.inodes
            .values()
            .filter(|e| e.kind == InodeKind::File)
            .map(|e| {
                (
                    e.inode,
                    e.full_path.clone(),
                    e.xet_hash.clone(),
                    e.etag.clone(),
                    e.size,
                    e.dirty,
                )
            })
            .collect()
    }

    /// Update remote file metadata (only if not dirty). Returns true if updated.
    pub fn update_remote_file(
        &mut self,
        ino: u64,
        new_hash: Option<String>,
        new_etag: Option<String>,
        new_size: u64,
        new_mtime: SystemTime,
    ) -> bool {
        if let Some(entry) = self.inodes.get_mut(&ino) {
            if entry.dirty {
                return false;
            }
            entry.xet_hash = new_hash;
            entry.etag = new_etag;
            entry.size = new_size;
            entry.mtime = new_mtime;
            true
        } else {
            false
        }
    }

    /// Reset children_loaded to false so the next readdir/lookup re-fetches.
    pub fn invalidate_children(&mut self, ino: u64) {
        if let Some(entry) = self.inodes.get_mut(&ino) {
            entry.children_loaded = false;
        }
    }

    /// Get directory inode by path.
    pub fn get_dir_ino(&self, path: &str) -> Option<u64> {
        self.path_to_inode.get(path).copied().and_then(|ino| {
            self.inodes
                .get(&ino)
                .filter(|e| e.kind == InodeKind::Directory)
                .map(|e| e.inode)
        })
    }

    /// Recursively update full_path and path_to_inode for an inode and all its descendants.
    /// Used after a rename to keep the path mappings consistent.
    /// Hard-linked inodes whose full_path doesn't match their expected position in the tree
    /// are skipped (they belong elsewhere and their canonical path must not be rewritten).
    pub fn update_subtree_paths(&mut self, inode: u64, new_full_path: String) {
        // Remove old path mapping
        if let Some(entry) = self.inodes.get(&inode) {
            let old_path = entry.full_path.clone();
            self.path_to_inode.remove(&old_path);
        }

        // Update this inode's path
        let (old_full_path, children) = if let Some(entry) = self.inodes.get_mut(&inode) {
            let old = std::mem::replace(&mut entry.full_path, new_full_path.clone());
            self.path_to_inode.insert(new_full_path.clone(), inode);
            (old, entry.children.clone())
        } else {
            return;
        };

        // Recursively update children (DirChild carries the name directly)
        for child in children {
            let expected_old = if old_full_path.is_empty() {
                child.name.clone()
            } else {
                format!("{}/{}", old_full_path, child.name)
            };
            let child_path = if new_full_path.is_empty() {
                child.name
            } else {
                format!("{}/{}", new_full_path, child.name)
            };
            // Hard-linked inodes whose canonical path is elsewhere: update the alias
            // mapping in path_to_inode but don't rewrite the inode's canonical full_path.
            if let Some(child_entry) = self.inodes.get(&child.ino)
                && child_entry.full_path != expected_old
            {
                self.path_to_inode.remove(&expected_old);
                self.path_to_inode.insert(child_path, child.ino);
                continue;
            }
            self.update_subtree_paths(child.ino, child_path);
        }
    }

    /// Remove an inode from the table (also removes from parent's children list).
    /// If the inode is a directory, all descendants are removed recursively to
    /// prevent orphaned entries in the table.
    pub fn remove(&mut self, inode: u64) -> Option<InodeEntry> {
        let entry = self.inodes.remove(&inode)?;
        self.path_to_inode.remove(&entry.full_path);

        // Remove from parent's children
        if let Some(parent) = self.inodes.get_mut(&entry.parent) {
            parent.children.retain(|c| c.ino != inode);
            // POSIX: removing a directory removes its ".." link to the parent
            if entry.kind == InodeKind::Directory {
                parent.nlink = parent.nlink.saturating_sub(1);
            }
        }

        // Recursively remove all descendants to avoid orphans
        let mut stack: Vec<u64> = entry.children.iter().map(|c| c.ino).collect();
        while let Some(child_ino) = stack.pop() {
            if let Some(child) = self.inodes.remove(&child_ino) {
                self.path_to_inode.remove(&child.full_path);
                stack.extend(child.children.iter().map(|c| c.ino));
            }
        }

        Some(entry)
    }

    /// Create a hard link: add a DirChild entry in `new_parent` pointing to `ino`,
    /// register the new path, and increment nlink. Does NOT validate (caller must check).
    pub fn link(&mut self, ino: u64, new_parent: u64, new_name: String, new_full_path: String) {
        // Add DirChild to parent
        if let Some(parent_entry) = self.inodes.get_mut(&new_parent) {
            parent_entry.children.push(DirChild { ino, name: new_name });
        }

        // Register path mapping
        self.path_to_inode.insert(new_full_path, ino);

        // Increment nlink and update ctime
        if let Some(entry) = self.inodes.get_mut(&ino) {
            entry.nlink += 1;
            entry.ctime = SystemTime::now();
        }
    }

    /// Remove one directory entry by name from `parent`. Decrements nlink.
    /// Returns `(inode_removed, entry_snapshot)` where `inode_removed` is true
    /// if nlink reached 0 and the inode was fully removed.
    pub fn unlink_one(&mut self, parent: u64, name: &str) -> Option<(bool, InodeEntry)> {
        // Find the child ino by name in parent's children
        let child_ino = {
            let parent_entry = self.inodes.get(&parent)?;
            parent_entry.children.iter().find(|c| c.name == name).map(|c| c.ino)?
        };

        // Build full_path for path_to_inode removal
        let full_path = {
            let parent_entry = self.inodes.get(&parent)?;
            if parent_entry.full_path.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", parent_entry.full_path, name)
            }
        };

        // Remove the DirChild from parent
        if let Some(parent_entry) = self.inodes.get_mut(&parent) {
            // Remove only the first matching entry (there might be other links with different names)
            if let Some(pos) = parent_entry.children.iter().position(|c| c.name == name) {
                parent_entry.children.remove(pos);
            }
        }

        // Remove path mapping
        self.path_to_inode.remove(&full_path);

        // Decrement nlink
        let (nlink, entry_snapshot) = {
            let entry = self.inodes.get_mut(&child_ino)?;
            entry.nlink = entry.nlink.saturating_sub(1);
            entry.ctime = SystemTime::now();
            (entry.nlink, entry.clone())
        };

        // If links remain and the removed entry was the canonical path,
        // update the inode's parent/name to point at a surviving link (for stat correctness).
        // Keep full_path unchanged for remote-backed files (xet_hash set) since it drives
        // remote operations (HEAD revalidation, flush, commit).
        if nlink > 0
            && entry_snapshot.parent == parent
            && entry_snapshot.name == name
            && let Some((new_parent_ino, new_name, new_full_path)) = self.find_link(child_ino)
            && let Some(entry) = self.inodes.get_mut(&child_ino)
        {
            entry.parent = new_parent_ino;
            entry.name = new_name;
            if entry.xet_hash.is_none() {
                // Locally-created file: update full_path since there's no remote path to preserve
                entry.full_path = new_full_path.clone();
                self.path_to_inode.insert(new_full_path, child_ino);
            }
        }

        // Return true when last link is gone (inode stays in table for open file handles;
        // callers clean it up via remove_orphan() after the last handle is released).
        Some((nlink == 0, entry_snapshot))
    }

    /// Find any surviving directory entry referencing `ino`.
    /// Returns `(parent_ino, child_name, full_path)` if found.
    fn find_link(&self, ino: u64) -> Option<(u64, String, String)> {
        for entry in self.inodes.values() {
            if entry.kind != InodeKind::Directory {
                continue;
            }
            for child in &entry.children {
                if child.ino == ino {
                    let full_path = if entry.full_path.is_empty() {
                        child.name.clone()
                    } else {
                        format!("{}/{}", entry.full_path, child.name)
                    };
                    return Some((entry.inode, child.name.clone(), full_path));
                }
            }
        }
        None
    }

    /// Move a child entry from one parent to another (or rename within the same parent).
    /// Detaches from old parent's children, attaches to new parent's children, updates the
    /// child's `parent`/`name`, and adjusts nlink for cross-parent directory moves.
    /// Does NOT update path mappings (caller should use `update_subtree_paths` separately).
    pub fn move_child(&mut self, ino: u64, old_parent: u64, old_name: &str, new_parent: u64, new_name: &str) {
        let is_dir = self.inodes.get(&ino).is_some_and(|e| e.kind == InodeKind::Directory);

        // Detach from old parent
        if let Some(old_p) = self.inodes.get_mut(&old_parent)
            && let Some(pos) = old_p.children.iter().position(|c| c.ino == ino && c.name == old_name)
        {
            old_p.children.remove(pos);
        }

        // Attach to new parent
        if let Some(new_p) = self.inodes.get_mut(&new_parent) {
            new_p.children.push(DirChild {
                ino,
                name: new_name.to_string(),
            });
        }

        // Adjust parent nlink for cross-parent directory moves
        if is_dir && old_parent != new_parent {
            if let Some(old_p) = self.inodes.get_mut(&old_parent) {
                old_p.nlink = old_p.nlink.saturating_sub(1);
            }
            if let Some(new_p) = self.inodes.get_mut(&new_parent) {
                new_p.nlink += 1;
            }
        }

        // Update child's parent/name
        if let Some(entry) = self.inodes.get_mut(&ino) {
            entry.parent = new_parent;
            entry.name = new_name.to_string();
        }
    }

    /// Update mtime and ctime on a parent directory (POSIX: directory modified).
    pub fn touch_parent(&mut self, parent: u64, now: SystemTime) {
        if let Some(p) = self.inodes.get_mut(&parent) {
            p.mtime = now;
            p.ctime = now;
        }
    }

    /// Remove an orphan inode (nlink == 0, no remaining file handles).
    /// Called from release() after the last open handle is closed.
    pub fn remove_orphan(&mut self, ino: u64) {
        if self.inodes.get(&ino).is_some_and(|e| e.nlink == 0) {
            self.inodes.remove(&ino);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_lookup() {
        let mut table = InodeTable::new();

        let ino = table.insert(
            ROOT_INODE,
            "hello.txt".to_string(),
            "hello.txt".to_string(),
            InodeKind::File,
            42,
            UNIX_EPOCH,
            Some("abc123".to_string()),
            0o644,
            0,
            0,
        );

        // Lookup by child name from root
        let found = table.lookup_child(ROOT_INODE, "hello.txt");
        assert!(found.is_some(), "should find child by name");
        let found = found.unwrap();
        assert_eq!(found.inode, ino);
        assert_eq!(found.name, "hello.txt");
        assert_eq!(found.full_path, "hello.txt");
        assert_eq!(found.kind, InodeKind::File);
        assert_eq!(found.size, 42);
        assert_eq!(found.xet_hash, Some("abc123".to_string()));

        // Also verify get and get_by_path
        assert!(table.get(ino).is_some());
        assert!(table.get_by_path("hello.txt").is_some());
        assert_eq!(table.get_by_path("hello.txt").unwrap().inode, ino);

        // Non-existent lookup returns None
        assert!(table.lookup_child(ROOT_INODE, "missing.txt").is_none());
    }

    #[test]
    fn test_dirty_flag() {
        let mut table = InodeTable::new();

        let ino = table.insert(
            ROOT_INODE,
            "data.bin".to_string(),
            "data.bin".to_string(),
            InodeKind::File,
            0,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Newly inserted inodes are not dirty
        assert!(!table.get(ino).unwrap().dirty);

        // Set dirty
        table.get_mut(ino).unwrap().dirty = true;
        assert!(table.get(ino).unwrap().dirty);

        // Clear dirty
        table.get_mut(ino).unwrap().dirty = false;
        assert!(!table.get(ino).unwrap().dirty);
    }

    #[test]
    fn test_remove() {
        let mut table = InodeTable::new();

        let ino = table.insert(
            ROOT_INODE,
            "remove_me.txt".to_string(),
            "remove_me.txt".to_string(),
            InodeKind::File,
            100,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Verify it exists in parent's children
        let root = table.get(ROOT_INODE).unwrap();
        assert!(root.children.iter().any(|c| c.ino == ino));

        // Verify path mapping exists
        assert!(table.get_by_path("remove_me.txt").is_some());

        // Remove it
        let removed = table.remove(ino);
        assert!(removed.is_some());
        let removed = removed.unwrap();
        assert_eq!(removed.inode, ino);
        assert_eq!(removed.name, "remove_me.txt");

        // Parent's children no longer contains the inode
        let root = table.get(ROOT_INODE).unwrap();
        assert!(!root.children.iter().any(|c| c.ino == ino));

        // Path mapping is gone
        assert!(table.get_by_path("remove_me.txt").is_none());

        // Direct lookup is gone
        assert!(table.get(ino).is_none());

        // Removing a non-existent inode returns None
        assert!(table.remove(9999).is_none());
    }

    #[test]
    fn test_remove_path_insert_path() {
        let mut table = InodeTable::new();

        let ino = table.insert(
            ROOT_INODE,
            "old_name.txt".to_string(),
            "old_name.txt".to_string(),
            InodeKind::File,
            50,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Verify original path works
        assert_eq!(table.get_by_path("old_name.txt").unwrap().inode, ino);

        // Simulate a rename: remove old path, insert new path
        table.remove_path("old_name.txt");
        assert!(table.get_by_path("old_name.txt").is_none());

        table.insert_path("new_name.txt".to_string(), ino);
        assert_eq!(table.get_by_path("new_name.txt").unwrap().inode, ino);

        // The inode itself still exists
        assert!(table.get(ino).is_some());
    }

    #[test]
    fn test_insert_duplicate_path() {
        let mut table = InodeTable::new();

        let ino1 = table.insert(
            ROOT_INODE,
            "dup.txt".to_string(),
            "dup.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Inserting the same full_path again should return the existing inode
        let ino2 = table.insert(
            ROOT_INODE,
            "dup.txt".to_string(),
            "dup.txt".to_string(),
            InodeKind::File,
            20,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        assert_eq!(ino1, ino2, "duplicate path insert should return existing inode");

        // The original entry should be unchanged (size still 10, not 20)
        assert_eq!(table.get(ino1).unwrap().size, 10);

        // Root should only have one child (not two)
        let root = table.get(ROOT_INODE).unwrap();
        assert_eq!(
            root.children.iter().filter(|c| c.ino == ino1).count(),
            1,
            "parent should have exactly one child reference"
        );
    }

    #[test]
    fn test_update_subtree_paths() {
        let mut table = InodeTable::new();

        // Build: root / old_dir / child.txt
        //                       / subdir / deep.txt
        let dir_ino = table.insert(
            ROOT_INODE,
            "old_dir".to_string(),
            "old_dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        let child_ino = table.insert(
            dir_ino,
            "child.txt".to_string(),
            "old_dir/child.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        let subdir_ino = table.insert(
            dir_ino,
            "subdir".to_string(),
            "old_dir/subdir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        let deep_ino = table.insert(
            subdir_ino,
            "deep.txt".to_string(),
            "old_dir/subdir/deep.txt".to_string(),
            InodeKind::File,
            5,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Rename old_dir → new_dir
        table.update_subtree_paths(dir_ino, "new_dir".to_string());

        // Verify all paths updated
        assert_eq!(table.get(dir_ino).unwrap().full_path, "new_dir");
        assert_eq!(table.get(child_ino).unwrap().full_path, "new_dir/child.txt");
        assert_eq!(table.get(subdir_ino).unwrap().full_path, "new_dir/subdir");
        assert_eq!(table.get(deep_ino).unwrap().full_path, "new_dir/subdir/deep.txt");

        // Verify path_to_inode updated (old paths gone, new paths work)
        assert!(table.get_by_path("old_dir").is_none());
        assert!(table.get_by_path("old_dir/child.txt").is_none());
        assert!(table.get_by_path("old_dir/subdir/deep.txt").is_none());

        assert_eq!(table.get_by_path("new_dir").unwrap().inode, dir_ino);
        assert_eq!(table.get_by_path("new_dir/child.txt").unwrap().inode, child_ino);
        assert_eq!(table.get_by_path("new_dir/subdir/deep.txt").unwrap().inode, deep_ino);
    }

    #[test]
    fn test_get_dir_ino() {
        let mut table = InodeTable::new();

        // Root is a directory
        assert_eq!(table.get_dir_ino(""), Some(ROOT_INODE));

        // Insert a file — should NOT be returned by get_dir_ino
        let file_ino = table.insert(
            ROOT_INODE,
            "file.txt".to_string(),
            "file.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        assert!(table.get_dir_ino("file.txt").is_none());

        // Insert a directory — should be returned
        let dir_ino = table.insert(
            ROOT_INODE,
            "mydir".to_string(),
            "mydir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        assert_eq!(table.get_dir_ino("mydir"), Some(dir_ino));

        // Non-existent path
        assert!(table.get_dir_ino("nope").is_none());

        let _ = file_ino;
    }

    #[test]
    fn test_file_snapshot() {
        let mut table = InodeTable::new();

        let ino1 = table.insert(
            ROOT_INODE,
            "a.txt".to_string(),
            "a.txt".to_string(),
            InodeKind::File,
            100,
            UNIX_EPOCH,
            Some("hash_a".to_string()),
            0o644,
            0,
            0,
        );
        table.get_mut(ino1).unwrap().dirty = true;

        let ino2 = table.insert(
            ROOT_INODE,
            "b.txt".to_string(),
            "b.txt".to_string(),
            InodeKind::File,
            200,
            UNIX_EPOCH,
            Some("hash_b".to_string()),
            0o644,
            0,
            0,
        );

        // Insert a directory — should NOT appear in file_snapshot
        table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        let snapshot = table.file_snapshot();
        assert_eq!(snapshot.len(), 2, "only files, not directories");

        let a = snapshot.iter().find(|(ino, ..)| *ino == ino1).unwrap();
        assert_eq!(a.1, "a.txt");
        assert_eq!(a.2, Some("hash_a".to_string()));
        assert_eq!(a.3, None); // etag
        assert_eq!(a.4, 100);
        assert!(a.5, "a.txt should be dirty");

        let b = snapshot.iter().find(|(ino, ..)| *ino == ino2).unwrap();
        assert_eq!(b.1, "b.txt");
        assert!(!b.5, "b.txt should not be dirty");
    }

    #[test]
    fn test_update_remote_file() {
        let mut table = InodeTable::new();

        let ino = table.insert(
            ROOT_INODE,
            "remote.txt".to_string(),
            "remote.txt".to_string(),
            InodeKind::File,
            100,
            UNIX_EPOCH,
            Some("old_hash".to_string()),
            0o644,
            0,
            0,
        );

        let new_mtime = UNIX_EPOCH + std::time::Duration::from_secs(1000);

        // Update succeeds on non-dirty file
        assert!(table.update_remote_file(ino, Some("new_hash".to_string()), None, 200, new_mtime));
        let entry = table.get(ino).unwrap();
        assert_eq!(entry.xet_hash, Some("new_hash".to_string()));
        assert_eq!(entry.size, 200);
        assert_eq!(entry.mtime, new_mtime);

        // Mark dirty — update should fail
        table.get_mut(ino).unwrap().dirty = true;
        assert!(!table.update_remote_file(ino, Some("ignored".to_string()), None, 999, UNIX_EPOCH));
        assert_eq!(table.get(ino).unwrap().size, 200, "dirty file should not be updated");

        // Non-existent inode
        assert!(!table.update_remote_file(9999, None, None, 0, UNIX_EPOCH));
    }

    #[test]
    fn test_invalidate_children() {
        let mut table = InodeTable::new();

        let dir_ino = table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );

        // Mark as loaded
        table.get_mut(dir_ino).unwrap().children_loaded = true;
        assert!(table.get(dir_ino).unwrap().children_loaded);

        // Invalidate
        table.invalidate_children(dir_ino);
        assert!(!table.get(dir_ino).unwrap().children_loaded);

        // Invalidating non-existent inode is a no-op
        table.invalidate_children(9999);
    }

    #[test]
    fn test_pending_deletes() {
        let mut table = InodeTable::new();

        let ino = table.insert(
            ROOT_INODE,
            "file.txt".to_string(),
            "file.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            Some("hash".to_string()),
            0o644,
            0,
            0,
        );

        // Initially empty
        assert!(table.get(ino).unwrap().pending_deletes.is_empty());

        // Simulate rename recording old path
        table
            .get_mut(ino)
            .unwrap()
            .pending_deletes
            .push("old_path.txt".to_string());

        assert_eq!(table.get(ino).unwrap().pending_deletes, vec!["old_path.txt"]);

        // Clear after flush
        table.get_mut(ino).unwrap().pending_deletes.clear();
        assert!(table.get(ino).unwrap().pending_deletes.is_empty());
    }

    #[test]
    fn test_remove_non_empty_dir_cleans_descendants() {
        let mut table = InodeTable::new();

        // Build: root / dir / child.txt
        //                    / subdir / deep.txt
        let dir_ino = table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let child_ino = table.insert(
            dir_ino,
            "child.txt".to_string(),
            "dir/child.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        let subdir_ino = table.insert(
            dir_ino,
            "subdir".to_string(),
            "dir/subdir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let deep_ino = table.insert(
            subdir_ino,
            "deep.txt".to_string(),
            "dir/subdir/deep.txt".to_string(),
            InodeKind::File,
            5,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Remove the top-level dir
        let removed = table.remove(dir_ino);
        assert!(removed.is_some());

        // All descendants must be gone from both maps
        assert!(table.get(dir_ino).is_none());
        assert!(table.get(child_ino).is_none());
        assert!(table.get(subdir_ino).is_none());
        assert!(table.get(deep_ino).is_none());
        assert!(table.get_by_path("dir").is_none());
        assert!(table.get_by_path("dir/child.txt").is_none());
        assert!(table.get_by_path("dir/subdir").is_none());
        assert!(table.get_by_path("dir/subdir/deep.txt").is_none());

        // Root no longer references the removed dir
        assert!(!table.get(ROOT_INODE).unwrap().children.iter().any(|c| c.ino == dir_ino));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "parent inode")]
    fn test_insert_with_missing_parent_panics() {
        let mut table = InodeTable::new();
        // Parent inode 999 does not exist — should panic in debug
        table.insert(
            999,
            "orphan.txt".to_string(),
            "orphan.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "different kind")]
    fn test_insert_duplicate_path_different_kind_panics() {
        let mut table = InodeTable::new();
        table.insert(
            ROOT_INODE,
            "name".to_string(),
            "name".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        // Same path but Directory kind — should panic in debug
        table.insert(
            ROOT_INODE,
            "name".to_string(),
            "name".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
    }

    #[test]
    fn test_dirty_inos_excludes_directories() {
        let mut table = InodeTable::new();
        let file_ino = table.insert(
            ROOT_INODE,
            "dirty.txt".to_string(),
            "dirty.txt".to_string(),
            InodeKind::File,
            1,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );
        let dir_ino = table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        table.get_mut(file_ino).unwrap().dirty = true;
        table.get_mut(dir_ino).unwrap().dirty = true;

        let dirty = table.dirty_inos();
        assert_eq!(dirty, vec![file_ino]);
    }

    #[test]
    fn test_update_subtree_paths_missing_inode_is_noop() {
        let mut table = InodeTable::new();
        table.insert(
            ROOT_INODE,
            "file.txt".to_string(),
            "file.txt".to_string(),
            InodeKind::File,
            10,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        let before = table.file_snapshot();
        table.update_subtree_paths(999_999, "does/not/matter".to_string());
        let after = table.file_snapshot();

        assert_eq!(before, after);
        assert!(table.get_by_path("file.txt").is_some());
    }

    #[test]
    fn test_remove_directory_with_already_removed_child() {
        let mut table = InodeTable::new();
        let dir_ino = table.insert(
            ROOT_INODE,
            "dir".to_string(),
            "dir".to_string(),
            InodeKind::Directory,
            0,
            UNIX_EPOCH,
            None,
            0o755,
            0,
            0,
        );
        let child_ino = table.insert(
            dir_ino,
            "child.txt".to_string(),
            "dir/child.txt".to_string(),
            InodeKind::File,
            1,
            UNIX_EPOCH,
            None,
            0o644,
            0,
            0,
        );

        // Simulate a stale children list entry in parent by removing child first.
        table.remove(child_ino).unwrap();
        assert!(table.get(child_ino).is_none());

        // Removing parent should still succeed and clean mappings.
        table.remove(dir_ino).unwrap();
        assert!(table.get(dir_ino).is_none());
        assert!(table.get_by_path("dir").is_none());
    }
}
