use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// State directory for daemon files (~/.hf-mount/).
fn state_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".hf-mount")
}

/// Canonicalize a mount point path, falling back to a normalized absolute path
/// if the target doesn't exist yet. The result is used to derive PID/log filenames.
fn canonical_mount_point(mount_point: &Path) -> PathBuf {
    if let Ok(p) = std::fs::canonicalize(mount_point) {
        return p;
    }

    let abs = if mount_point.is_absolute() {
        mount_point.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(mount_point)
    };

    // Normalize away `.` and `..` components so `./mnt` and `mnt` match.
    let mut normalized = PathBuf::new();
    for component in abs.components() {
        match component {
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::CurDir => {}
            c => normalized.push(c),
        }
    }
    normalized
}

/// Encode a path as a filename-safe string using percent-encoding.
/// `/` becomes `%2F`, `%` becomes `%25`, producing an injective mapping.
fn encode_path(path: &Path) -> String {
    path.to_string_lossy()
        .trim_matches('/')
        .replace('%', "%25")
        .replace('/', "%2F")
}

fn pid_path(mount_point: &Path) -> PathBuf {
    let key = encode_path(&canonical_mount_point(mount_point));
    state_dir().join("pids").join(format!("{key}.pid"))
}

fn log_path(mount_point: &Path) -> PathBuf {
    let key = encode_path(&canonical_mount_point(mount_point));
    state_dir().join("logs").join(format!("{key}.log"))
}

/// Check if a PID belongs to a live hf-mount daemon process.
/// On Linux, verifies via /proc/<pid>/exe. Elsewhere, just checks liveness.
fn is_our_daemon(pid: i32) -> bool {
    if (unsafe { libc::kill(pid, 0) }) != 0 {
        return false;
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(exe) = std::fs::read_link(format!("/proc/{pid}/exe")) {
            let name = exe.file_name().unwrap_or_default().to_string_lossy();
            return name.starts_with("hf-mount");
        }
    }

    true
}

/// Check if a PID is alive (any process, no identity check).
fn pid_alive(pid: i32) -> bool {
    (unsafe { libc::kill(pid, 0) }) == 0
}

/// Read a PID file. Returns the PID and optional mount path.
/// PID file format: line 1 = PID, line 2 = canonical mount path.
fn read_pid_file(pid_file: &Path) -> Option<(i32, Option<String>)> {
    let content = std::fs::read_to_string(pid_file).ok()?;
    let mut lines = content.lines();
    let pid: i32 = lines.next()?.trim().parse().ok()?;
    let mount = lines.next().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    Some((pid, mount))
}

/// Guard held by the daemon child process. Signals readiness to the parent
/// via a pipe and cleans up the PID file on drop.
pub struct DaemonGuard {
    write_fd: i32,
    pid_file: PathBuf,
    notified: bool,
}

impl DaemonGuard {
    /// Path to the PID file managed by this guard.
    pub fn pid_file(&self) -> &Path {
        &self.pid_file
    }

    /// Signal the parent that the mount is active and it can exit.
    pub fn notify_ready(&mut self) {
        if !self.notified {
            unsafe {
                libc::write(self.write_fd, b"R".as_ptr() as *const _, 1);
                libc::close(self.write_fd);
            }
            self.notified = true;
        }
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if !self.notified {
            unsafe {
                libc::write(self.write_fd, b"E".as_ptr() as *const _, 1);
                libc::close(self.write_fd);
            }
        }
        let _ = std::fs::remove_file(&self.pid_file);
    }
}

/// Try to unmount a mount point using all available strategies.
#[allow(clippy::needless_return)]
fn try_unmount(mount_point: &Path) -> bool {
    let mount_str = mount_point.to_string_lossy();
    #[cfg(target_os = "macos")]
    {
        return std::process::Command::new("umount")
            .arg(&*mount_str)
            .status()
            .is_ok_and(|s| s.success());
    }
    #[cfg(target_os = "linux")]
    {
        if unsafe { libc::getuid() } == 0 {
            return std::process::Command::new("umount")
                .arg(&*mount_str)
                .status()
                .is_ok_and(|s| s.success());
        }
        // Non-root: try umount, sudo -n umount (NFS), fusermount3/fusermount (FUSE).
        return std::process::Command::new("umount")
            .arg(&*mount_str)
            .status()
            .is_ok_and(|s| s.success())
            || std::process::Command::new("sudo")
                .args(["-n", "umount", &mount_str])
                .status()
                .is_ok_and(|s| s.success())
            || std::process::Command::new("fusermount3")
                .args(["-u", "-z", &mount_str])
                .status()
                .is_ok_and(|s| s.success())
            || std::process::Command::new("fusermount")
                .args(["-u", "-z", &mount_str])
                .status()
                .is_ok_and(|s| s.success());
    }
}

/// Stop a running daemon by unmounting and waiting for the process to exit.
pub fn stop_daemon(mount_point: &Path) -> std::io::Result<()> {
    let pid_file = pid_path(mount_point);
    let canonical = canonical_mount_point(mount_point);
    let (pid, stored_mount) = read_pid_file(&pid_file).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no daemon found for {:?} (pid file: {})",
                mount_point,
                pid_file.display()
            ),
        )
    })?;

    // Check if the process is actually our daemon for this mount point.
    let daemon_alive = is_our_daemon(pid) && stored_mount.as_deref().is_none_or(|m| Path::new(m) == canonical);
    if !daemon_alive {
        eprintln!("Daemon (pid={pid}) is not running, cleaning up stale pid file");
        let _ = std::fs::remove_file(&pid_file);
        // Still try to unmount in case the mount survived a crash.
        if !try_unmount(mount_point) {
            eprintln!("Warning: could not unmount {:?} (may need manual cleanup)", mount_point);
        }
        return Ok(());
    }

    eprintln!("Stopping daemon (pid={pid}), unmounting {:?}...", mount_point);

    if !try_unmount(mount_point) {
        eprintln!("Warning: unmount command failed, waiting for daemon to exit...");
    }

    // Wait up to 30s for the daemon to exit. The daemon may spend time
    // in shutdown() flushing dirty data after unmount.
    for _ in 0..60 {
        if !pid_alive(pid) {
            eprintln!("Daemon stopped");
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Still running, send SIGTERM.
    eprintln!("Daemon still running, sending SIGTERM...");
    unsafe { libc::kill(pid, libc::SIGTERM) };
    for _ in 0..10 {
        if !pid_alive(pid) {
            eprintln!("Daemon stopped");
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("daemon (pid={pid}) did not stop after 35s"),
    ))
}

/// Fork the process into a background daemon.
///
/// - Parent: waits for the child to confirm the mount is active, prints
///   daemon info (PID, log path, unmount command), then exits.
/// - Child: detaches from the terminal (setsid), redirects stderr to a log
///   file, writes a PID file, and returns a `DaemonGuard`. The caller must
///   call `guard.notify_ready()` once the mount is confirmed.
///
/// Returns an error if a daemon is already running for this mount point.
///
/// # Safety
/// Must be called before any threads are spawned (before tokio runtime).
pub fn daemonize(mount_point: &Path) -> std::io::Result<DaemonGuard> {
    let log_file = log_path(mount_point);
    let pid_file = pid_path(mount_point);
    let canonical = canonical_mount_point(mount_point);

    std::fs::create_dir_all(log_file.parent().unwrap())?;
    std::fs::create_dir_all(pid_file.parent().unwrap())?;

    // Refuse to start if a daemon is already running for this mount point.
    if let Some((existing_pid, stored_mount)) = read_pid_file(&pid_file) {
        let is_ours = is_our_daemon(existing_pid) && stored_mount.as_deref().is_none_or(|m| Path::new(m) == canonical);
        if is_ours {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "daemon already running (pid={existing_pid}) for {:?}. Stop it first with: hf-mount-daemon stop {}",
                    mount_point,
                    mount_point.display()
                ),
            ));
        }
        // Stale PID file from a dead process, remove it.
        let _ = std::fs::remove_file(&pid_file);
    }

    // Pipe for ready notification: child writes, parent reads.
    // Set CLOEXEC so helpers (mount.nfs, sudo) don't inherit the write end,
    // which would keep the pipe open and block the parent if the child dies.
    let (read_fd, write_fd) = {
        let mut fds = [0i32; 2];
        #[cfg(target_os = "linux")]
        {
            if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            for &fd in &fds {
                unsafe {
                    let flags = libc::fcntl(fd, libc::F_GETFD);
                    libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
                }
            }
        }
        (fds[0], fds[1])
    };

    match unsafe { libc::fork() } {
        -1 => Err(std::io::Error::last_os_error()),
        0 => {
            // ── Child ──
            unsafe { libc::close(read_fd) };

            if unsafe { libc::setsid() } == -1 {
                return Err(std::io::Error::last_os_error());
            }

            // Redirect stdout + stderr to the log file.
            let log = std::fs::OpenOptions::new().create(true).append(true).open(&log_file)?;
            let log_fd = log.as_raw_fd();
            unsafe {
                libc::dup2(log_fd, libc::STDOUT_FILENO);
                libc::dup2(log_fd, libc::STDERR_FILENO);
                libc::close(libc::STDIN_FILENO);
                libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY);
            }
            drop(log);

            // Write PID file with mount path for identity validation.
            std::fs::write(&pid_file, format!("{}\n{}\n", std::process::id(), canonical.display()))?;

            Ok(DaemonGuard {
                write_fd,
                pid_file,
                notified: false,
            })
        }
        child_pid => {
            // ── Parent ──
            unsafe { libc::close(write_fd) };

            let mut buf = [0u8; 1];
            let n = unsafe { libc::read(read_fd, buf.as_mut_ptr() as *mut _, 1) };
            unsafe { libc::close(read_fd) };

            if n == 1 && buf[0] == b'R' {
                eprintln!("Daemon started (pid={child_pid}), logs: {}", log_file.display());
                eprintln!("Stop with: hf-mount-daemon stop {}", canonical.display());
                std::process::exit(0);
            } else {
                eprintln!("Daemon failed to start. Log file: {}", log_file.display());
                if let Ok(log) = std::fs::read_to_string(&log_file) {
                    let log = log.trim();
                    if !log.is_empty() {
                        eprintln!("\n{log}");
                    }
                }
                std::process::exit(1);
            }
        }
    }
}
