use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const ROOT_INODE: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeKind {
    File,
    Directory,
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
    pub xet_hash: Option<String>,
    pub dirty: bool,
    pub children_loaded: bool,
    pub children: Vec<u64>,
}

pub struct InodeTable {
    inodes: HashMap<u64, InodeEntry>,
    path_to_inode: HashMap<String, u64>,
    next_inode: AtomicU64,
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
            xet_hash: None,
            dirty: false,
            children_loaded: false,
            children: Vec::new(),
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
        self.path_to_inode
            .get(path)
            .and_then(|ino| self.inodes.get(ino))
    }

    /// Find a child of `parent` by name.
    pub fn lookup_child(&self, parent: u64, name: &str) -> Option<&InodeEntry> {
        let parent_entry = self.inodes.get(&parent)?;
        for &child_ino in &parent_entry.children {
            if let Some(child) = self.inodes.get(&child_ino) {
                if child.name == name {
                    return Some(child);
                }
            }
        }
        None
    }

    /// Insert a new inode, returning its inode number.
    pub fn insert(
        &mut self,
        parent: u64,
        name: String,
        full_path: String,
        kind: InodeKind,
        size: u64,
        mtime: SystemTime,
        xet_hash: Option<String>,
    ) -> u64 {
        // Check if already exists
        if let Some(existing) = self.path_to_inode.get(&full_path) {
            return *existing;
        }

        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        let entry = InodeEntry {
            inode,
            parent,
            name,
            full_path: full_path.clone(),
            kind,
            size,
            mtime,
            xet_hash,
            dirty: false,
            children_loaded: kind == InodeKind::File, // files don't have children to load
            children: Vec::new(),
        };

        self.inodes.insert(inode, entry);
        self.path_to_inode.insert(full_path, inode);

        // Add to parent's children
        if let Some(parent_entry) = self.inodes.get_mut(&parent) {
            parent_entry.children.push(inode);
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

    /// Remove an inode from the table (also removes from parent's children list).
    pub fn remove(&mut self, inode: u64) -> Option<InodeEntry> {
        let entry = self.inodes.remove(&inode)?;
        self.path_to_inode.remove(&entry.full_path);

        // Remove from parent's children
        if let Some(parent) = self.inodes.get_mut(&entry.parent) {
            parent.children.retain(|&c| c != inode);
        }

        Some(entry)
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
        );

        // Verify it exists in parent's children
        let root = table.get(ROOT_INODE).unwrap();
        assert!(root.children.contains(&ino));

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
        assert!(!root.children.contains(&ino));

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
        );

        assert_eq!(ino1, ino2, "duplicate path insert should return existing inode");

        // The original entry should be unchanged (size still 10, not 20)
        assert_eq!(table.get(ino1).unwrap().size, 10);

        // Root should only have one child (not two)
        let root = table.get(ROOT_INODE).unwrap();
        assert_eq!(
            root.children.iter().filter(|&&c| c == ino1).count(),
            1,
            "parent should have exactly one child reference"
        );
    }
}
