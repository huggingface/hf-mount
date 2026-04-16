use std::ffi::{CStr, CString};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct OverlayDirEntry {
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub mtime: SystemTime,
    pub mode: u16,
}

/// Overlay-local writable backing rooted at the pre-mount directory fd.
pub struct OverlayBacking {
    /// Kept alive so the pre-mount directory remains reachable and, on macOS,
    /// provides the dirfd used by fd-relative helper calls.
    dirfd: std::fs::File,
}

impl OverlayBacking {
    pub fn new(fd: std::fs::File) -> Self {
        Self { dirfd: fd }
    }

    pub fn exists(&self, full_path: &str) -> std::io::Result<bool> {
        let rel = Self::validate_rel_path(full_path)?;
        if rel.as_os_str().is_empty() {
            return Ok(true);
        }
        let (parent, name) = Self::split_parent_and_name(rel)?;
        let dir = match self.walk_rel_dir(parent, false) {
            Ok(dir) => dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err),
        };
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let rc = unsafe {
            libc::fstatat(
                dir.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc == 0 {
            let stat = unsafe { stat.assume_init() };
            return Ok((stat.st_mode & libc::S_IFMT) != libc::S_IFLNK);
        }
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::NotFound {
            Ok(false)
        } else {
            Err(err)
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
        let rel = Self::validate_rel_path(full_path)?;
        let (parent, name) = Self::split_parent_and_name(rel)?;
        let dir = self.walk_rel_dir(parent, false)?;
        let access_mode = match (read, write) {
            (true, true) => libc::O_RDWR,
            (true, false) => libc::O_RDONLY,
            (false, true) => libc::O_WRONLY,
            (false, false) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "overlay open requires read or write access",
                ));
            }
        };
        let mut flags = access_mode;
        if create {
            flags |= libc::O_CREAT;
        }
        if truncate {
            flags |= libc::O_TRUNC;
        }
        flags |= libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let fd = unsafe { libc::openat(dir.as_raw_fd(), name.as_ptr(), flags, 0o666) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { std::fs::File::from_raw_fd(fd) })
    }

    pub fn create_parent_dirs(&self, full_path: &str) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        let Some(parent) = rel.parent().filter(|p| !p.as_os_str().is_empty()) else {
            return Ok(());
        };

        self.walk_rel_dir(parent, true).map(|_| ())
    }

    pub fn create_dir(&self, full_path: &str, mode: u16) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        let (parent, name) = Self::split_parent_and_name(rel)?;
        let dir = self.walk_rel_dir(parent, false)?;
        let rc = unsafe { libc::mkdirat(dir.as_raw_fd(), name.as_ptr(), mode as libc::mode_t) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn set_mode(&self, full_path: &str, mode: u16) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        if rel.as_os_str().is_empty() {
            let rc = unsafe { libc::fchmod(self.dirfd(), mode as libc::mode_t) };
            return if rc == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            };
        }

        let (parent, name) = Self::split_parent_and_name(rel)?;
        let dir = self.walk_rel_dir(parent, false)?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let stat_rc = unsafe {
            libc::fstatat(
                dir.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if stat_rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let stat = unsafe { stat.assume_init() };
        if (stat.st_mode & libc::S_IFMT) == libc::S_IFLNK {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "overlay chmod rejects symlinks",
            ));
        }

        let rc = unsafe { libc::fchmodat(dir.as_raw_fd(), name.as_ptr(), mode as libc::mode_t, 0) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn remove_file(&self, full_path: &str) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        let (parent, name) = Self::split_parent_and_name(rel)?;
        let dir = self.walk_rel_dir(parent, false)?;
        let rc = unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn remove_dir(&self, full_path: &str) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        let (parent, name) = Self::split_parent_and_name(rel)?;
        let dir = self.walk_rel_dir(parent, false)?;
        let rc = unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn rename(&self, old_path: &str, new_path: &str) -> std::io::Result<()> {
        let old_rel = Self::validate_rel_path(old_path)?;
        let new_rel = Self::validate_rel_path(new_path)?;
        let (old_parent, old_name) = Self::split_parent_and_name(old_rel)?;
        let (new_parent, new_name) = Self::split_parent_and_name(new_rel)?;
        let old_dir = self.walk_rel_dir(old_parent, false)?;
        let new_dir = self.walk_rel_dir(new_parent, false)?;
        let rc = unsafe {
            libc::renameat(
                old_dir.as_raw_fd(),
                old_name.as_ptr(),
                new_dir.as_raw_fd(),
                new_name.as_ptr(),
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn read_dir(&self, full_path: &str) -> std::io::Result<Vec<OverlayDirEntry>> {
        let rel = Self::validate_rel_path(full_path)?;
        let dir_file = if rel.as_os_str().is_empty() {
            self.dup_dirfd()?
        } else {
            self.walk_rel_dir(rel, false)?
        };
        let fd = dir_file.into_raw_fd();

        let dir = unsafe { libc::fdopendir(fd) };
        if dir.is_null() {
            let err = std::io::Error::last_os_error();
            unsafe {
                libc::close(fd);
            }
            return Err(err);
        }

        struct DirGuard(*mut libc::DIR);
        impl Drop for DirGuard {
            fn drop(&mut self) {
                unsafe {
                    libc::closedir(self.0);
                }
            }
        }

        let guard = DirGuard(dir);
        let dir_fd = unsafe { libc::dirfd(guard.0) };
        let mut entries = Vec::new();

        loop {
            clear_errno();
            let entry = unsafe { libc::readdir(guard.0) };
            if entry.is_null() {
                if current_errno() == 0 {
                    break;
                }
                return Err(std::io::Error::last_os_error());
            }

            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }
                .to_string_lossy()
                .into_owned();
            if name == "." || name == ".." {
                continue;
            }

            let name_cstr = CString::new(name.as_bytes())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "overlay entry contains NUL"))?;
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            let rc = unsafe { libc::fstatat(dir_fd, name_cstr.as_ptr(), stat.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) };
            if rc != 0 {
                continue;
            }
            let stat = unsafe { stat.assume_init() };
            let kind = stat.st_mode & libc::S_IFMT;
            entries.push(OverlayDirEntry {
                name,
                is_dir: kind == libc::S_IFDIR,
                is_symlink: kind == libc::S_IFLNK,
                size: stat.st_size as u64,
                mtime: stat_mtime(&stat),
                mode: (stat.st_mode & 0o777) as u16,
            });
        }

        Ok(entries)
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

    fn split_parent_and_name(rel: &Path) -> std::io::Result<(&Path, CString)> {
        let name = rel.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("overlay path must name an entry, got: {rel:?}"),
            )
        })?;
        Ok((rel.parent().unwrap_or_else(|| Path::new("")), os_str_to_cstring(name)?))
    }

    fn dup_dirfd(&self) -> std::io::Result<std::fs::File> {
        let fd = unsafe { libc::fcntl(self.dirfd(), libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(unsafe { std::fs::File::from_raw_fd(fd) })
        }
    }

    fn walk_rel_dir(&self, rel_dir: &Path, create_missing: bool) -> std::io::Result<std::fs::File> {
        let mut current = self.dup_dirfd()?;
        for component in rel_dir.components() {
            let std::path::Component::Normal(part) = component else {
                continue;
            };

            if create_missing {
                let path = os_str_to_cstring(part)?;
                let rc = unsafe { libc::mkdirat(current.as_raw_fd(), path.as_ptr(), 0o755) };
                if rc != 0 {
                    let err = std::io::Error::last_os_error();
                    if err.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(err);
                    }
                }
            }

            current = Self::open_dir_nofollow(current.as_raw_fd(), Path::new(part))?;
        }
        Ok(current)
    }

    fn open_dir_nofollow(dirfd: libc::c_int, rel: &Path) -> std::io::Result<std::fs::File> {
        let path = path_to_cstring(rel)?;
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let fd = unsafe { libc::openat(dirfd, path.as_ptr(), flags) };
        if fd < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(unsafe { std::fs::File::from_raw_fd(fd) })
        }
    }

    fn dirfd(&self) -> libc::c_int {
        self.dirfd.as_raw_fd()
    }
}

fn path_to_cstring(path: &Path) -> std::io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "overlay path contains NUL"))
}

fn os_str_to_cstring(value: &std::ffi::OsStr) -> std::io::Result<CString> {
    CString::new(value.as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "overlay path contains NUL"))
}

fn stat_mtime(stat: &libc::stat) -> SystemTime {
    if stat.st_mtime < 0 {
        UNIX_EPOCH
    } else {
        UNIX_EPOCH + Duration::new(stat.st_mtime as u64, stat.st_mtime_nsec as u32)
    }
}

#[cfg(target_os = "linux")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

fn clear_errno() {
    unsafe {
        *errno_location() = 0;
    }
}

fn current_errno() -> libc::c_int {
    unsafe { *errno_location() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("hf_overlay_backing_{}_{}_{}", name, std::process::id(), nanos));
        std::fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

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
