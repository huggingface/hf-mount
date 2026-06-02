use std::path::Path;
use std::time::SystemTime;

use cap_std::fs::{Dir, OpenOptions};
#[cfg(unix)]
use cap_std::fs::{DirBuilder, DirBuilderExt, Permissions, PermissionsExt};

#[derive(Debug, Clone)]
pub struct OverlayDirEntry {
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub mtime: SystemTime,
    pub mode: u16,
}

/// Overlay-local writable backing. All paths resolve within the directory fd;
/// `cap-std` enforces sandboxing (no escape via absolute paths, `..`, or
/// symlinks pointing outside the root).
pub struct OverlayBacking {
    dir: Dir,
}

impl OverlayBacking {
    pub fn new(fd: std::fs::File) -> Self {
        Self {
            dir: Dir::from_std_file(fd),
        }
    }

    pub fn exists(&self, full_path: &str) -> std::io::Result<bool> {
        let rel = validate_rel_path(full_path)?;
        if rel.as_os_str().is_empty() {
            return Ok(true);
        }
        match self.dir.symlink_metadata(rel) {
            Ok(meta) => Ok(!meta.file_type().is_symlink()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn open_file(
        &self,
        full_path: &str,
        read: bool,
        write: bool,
        create: bool,
        truncate: bool,
    ) -> std::io::Result<std::fs::File> {
        if !read && !write {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "overlay open requires read or write access",
            ));
        }
        let rel = validate_rel_path(full_path)?;
        let mut opts = OpenOptions::new();
        opts.read(read).write(write);
        if create {
            opts.create(true);
        }
        if truncate {
            opts.truncate(true);
        }
        Ok(self.dir.open_with(rel, &opts)?.into_std())
    }

    pub fn create_parent_dirs(&self, full_path: &str) -> std::io::Result<()> {
        let rel = validate_rel_path(full_path)?;
        let Some(parent) = rel.parent().filter(|p| !p.as_os_str().is_empty()) else {
            return Ok(());
        };
        self.dir.create_dir_all(parent)
    }

    pub fn create_dir(&self, full_path: &str, mode: u16) -> std::io::Result<()> {
        let rel = validate_rel_path(full_path)?;
        #[cfg(unix)]
        {
            let mut builder = DirBuilder::new();
            builder.mode(mode as u32);
            self.dir.create_dir_with(rel, &builder)
        }
        #[cfg(not(unix))]
        {
            let _ = mode;
            self.dir.create_dir(rel)
        }
    }

    pub fn set_mode(&self, full_path: &str, mode: u16) -> std::io::Result<()> {
        let rel = validate_rel_path(full_path)?;
        let target: &Path = if rel.as_os_str().is_empty() {
            Path::new(".")
        } else {
            rel
        };
        // chmod on a symlink would chase the target; reject explicitly so
        // overlay never mutates anything outside its sandbox via a symlink.
        let meta = self.dir.symlink_metadata(target)?;
        if meta.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "overlay chmod rejects symlinks",
            ));
        }
        #[cfg(unix)]
        {
            self.dir.set_permissions(target, Permissions::from_mode(mode as u32))
        }
        #[cfg(windows)]
        {
            let _ = mode;
            Ok(())
        }
    }

    pub fn remove_file(&self, full_path: &str) -> std::io::Result<()> {
        let rel = validate_rel_path(full_path)?;
        self.dir.remove_file(rel)
    }

    pub fn remove_dir(&self, full_path: &str) -> std::io::Result<()> {
        let rel = validate_rel_path(full_path)?;
        self.dir.remove_dir(rel)
    }

    pub fn rename(&self, old_path: &str, new_path: &str) -> std::io::Result<()> {
        let old_rel = validate_rel_path(old_path)?;
        let new_rel = validate_rel_path(new_path)?;
        self.dir.rename(old_rel, &self.dir, new_rel)
    }

    pub fn read_dir(&self, full_path: &str) -> std::io::Result<Vec<OverlayDirEntry>> {
        let rel = validate_rel_path(full_path)?;
        let target: &Path = if rel.as_os_str().is_empty() {
            Path::new(".")
        } else {
            rel
        };
        let mut out = Vec::new();
        for entry in self.dir.read_dir(target)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let Ok(ft) = entry.file_type() else { continue };
            // Symlinks are skipped by mod.rs callers; size/mtime/mode of the
            // target are irrelevant. Avoid `entry.metadata()` (which follows
            // the symlink) so we never `stat` a path outside the sandbox.
            if ft.is_symlink() {
                out.push(OverlayDirEntry {
                    name,
                    is_dir: false,
                    is_symlink: true,
                    size: 0,
                    mtime: SystemTime::UNIX_EPOCH,
                    mode: 0,
                });
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            out.push(OverlayDirEntry {
                name,
                is_dir: ft.is_dir(),
                is_symlink: false,
                size: meta.len(),
                mtime: meta.modified().map(|t| t.into_std()).unwrap_or(SystemTime::UNIX_EPOCH),
                #[allow(clippy::unnecessary_cast)]
                #[cfg(unix)]
                mode: (meta.permissions().mode() & 0o777) as u16,
                #[cfg(windows)]
                mode: 0o644,
            });
        }
        Ok(out)
    }
}

fn validate_rel_path(full_path: &str) -> std::io::Result<&Path> {
    let path = Path::new(full_path);
    if path.has_root()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("overlay path must be a safe relative path, got: {full_path:?}"),
        ));
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    use std::time::UNIX_EPOCH;

    fn fresh_temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("hf_overlay_backing_{}_{}_{}", name, std::process::id(), nanos));
        std::fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    #[cfg(unix)]
    #[test]
    fn overlay_open_file_sets_cloexec() {
        let root = fresh_temp_dir("cloexec_root");
        std::fs::write(root.join("file.txt"), b"hello").unwrap();

        let overlay = OverlayBacking::new(std::fs::File::open(&root).unwrap());
        let file = overlay.open_file("file.txt", true, false, false, false).unwrap();

        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0, "F_GETFD should succeed");
        assert_ne!(flags & libc::FD_CLOEXEC, 0, "overlay file fd should be close-on-exec");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn overlay_open_file_rejects_final_symlink() {
        let root = fresh_temp_dir("final_symlink_root");
        let outside = fresh_temp_dir("final_symlink_outside");
        let outside_file = outside.join("outside.txt");
        std::fs::write(&outside_file, b"outside").unwrap();
        std::os::unix::fs::symlink(&outside_file, root.join("link.txt")).unwrap();

        let overlay = OverlayBacking::new(std::fs::File::open(&root).unwrap());
        let err = overlay
            .open_file("link.txt", true, false, false, false)
            .expect_err("overlay open should not follow final symlink");
        assert!(
            err.kind() != std::io::ErrorKind::NotFound,
            "final symlink should be rejected, not treated as a normal missing file"
        );
        assert_eq!(std::fs::read(&outside_file).unwrap(), b"outside");

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn overlay_create_parent_dirs_rejects_symlink_parent() {
        let root = fresh_temp_dir("parent_symlink_root");
        let outside = fresh_temp_dir("parent_symlink_outside");
        std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();

        let overlay = OverlayBacking::new(std::fs::File::open(&root).unwrap());
        overlay
            .create_parent_dirs("escape/nested/file.txt")
            .expect_err("overlay mkdir walk should not follow parent symlink");
        assert!(
            !outside.join("nested").exists(),
            "symlinked parent should not allow directory creation outside the overlay root"
        );

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
    }

    #[test]
    fn overlay_exists_returns_false_for_missing_parent() {
        let root = fresh_temp_dir("missing_parent_root");

        let overlay = OverlayBacking::new(std::fs::File::open(&root).unwrap());
        assert!(!overlay.exists("missing/child.txt").unwrap());

        std::fs::remove_dir_all(root).unwrap();
    }
}
