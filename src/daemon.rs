use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// State directory for daemon files (~/.hf-mount/).
fn state_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".hf-mount")
}

/// Canonicalize a mount point path, falling back to the absolute path if
/// the target doesn't exist yet. The result is used to derive PID/log filenames.
fn canonical_mount_point(mount_point: &Path) -> PathBuf {
    std::fs::canonicalize(mount_point).unwrap_or_else(|_| {
        if mount_point.is_absolute() {
            mount_point.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("/"))
                .join(mount_point)
        }
    })
}

/// Encode a path as a filename-safe string. Uses `-` as separator instead
/// of replacing `/` with `_` (which would cause collisions like `/a_b/c`
/// vs `/a/b_c`). Leading `/` is stripped.
fn encode_path(path: &Path) -> String {
    path.to_string_lossy().trim_matches('/').replace('/', "-")
}

fn pid_path(mount_point: &Path) -> PathBuf {
    let key = encode_path(&canonical_mount_point(mount_point));
    state_dir().join("pids").join(format!("{key}.pid"))
}

fn log_path(mount_point: &Path) -> PathBuf {
    let key = encode_path(&canonical_mount_point(mount_point));
    state_dir().join("logs").join(format!("{key}.log"))
}

/// Check if a PID is alive.
fn pid_alive(pid: i32) -> bool {
    (unsafe { libc::kill(pid, 0) }) == 0
}

/// Read a PID from a PID file. Returns None if the file doesn't exist or
/// contains garbage.
fn read_pid(pid_file: &Path) -> Option<i32> {
    std::fs::read_to_string(pid_file)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// Guard held by the daemon child process. Signals readiness to the parent
/// via a pipe and cleans up the PID file on drop.
pub struct DaemonGuard {
    write_fd: i32,
    pid_file: PathBuf,
    notified: bool,
}

impl DaemonGuard {
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
            // Daemon failed before becoming ready, signal error to parent.
            unsafe {
                libc::write(self.write_fd, b"E".as_ptr() as *const _, 1);
                libc::close(self.write_fd);
            }
        }
        let _ = std::fs::remove_file(&self.pid_file);
    }
}

/// Stop a running daemon by unmounting and waiting for the process to exit.
/// Reads the PID file, unmounts the mount point, then waits for the process.
pub fn stop_daemon(mount_point: &Path) -> std::io::Result<()> {
    let pid_file = pid_path(mount_point);
    let pid_str = std::fs::read_to_string(&pid_file).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!(
                "no daemon found for {:?} (pid file: {}): {e}",
                mount_point,
                pid_file.display()
            ),
        )
    })?;
    let pid: i32 = pid_str
        .trim()
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("invalid pid file: {e}")))?;

    // Check if the process is actually running.
    if !pid_alive(pid) {
        eprintln!("Daemon (pid={pid}) is not running, cleaning up stale pid file");
        let _ = std::fs::remove_file(&pid_file);
        return Ok(());
    }

    eprintln!("Stopping daemon (pid={pid}), unmounting {:?}...", mount_point);

    // Unmount (this triggers the daemon's clean shutdown).
    let mount_str = mount_point.to_string_lossy();
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("umount").arg(&*mount_str).status();
    }
    #[cfg(target_os = "linux")]
    {
        if unsafe { libc::getuid() } == 0 {
            let _ = std::process::Command::new("umount").arg(&*mount_str).status();
        } else {
            let _ = std::process::Command::new("sudo")
                .args(["-n", "umount", &mount_str])
                .status();
        }
    }

    // Wait up to 10s for the daemon to exit.
    for _ in 0..20 {
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
        format!("daemon (pid={pid}) did not stop after 15s"),
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

    std::fs::create_dir_all(log_file.parent().unwrap())?;
    std::fs::create_dir_all(pid_file.parent().unwrap())?;

    // Refuse to start if a daemon is already running for this mount point.
    if let Some(existing_pid) = read_pid(&pid_file) {
        if pid_alive(existing_pid) {
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
    let (read_fd, write_fd) = {
        let mut fds = [0i32; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        (fds[0], fds[1])
    };

    match unsafe { libc::fork() } {
        -1 => Err(std::io::Error::last_os_error()),
        0 => {
            // ── Child ──
            unsafe { libc::close(read_fd) };

            // Detach from terminal.
            if unsafe { libc::setsid() } == -1 {
                return Err(std::io::Error::last_os_error());
            }

            // Redirect stdout + stderr to the log file.
            let log = std::fs::OpenOptions::new().create(true).append(true).open(&log_file)?;
            let log_fd = log.as_raw_fd();
            unsafe {
                libc::dup2(log_fd, libc::STDOUT_FILENO);
                libc::dup2(log_fd, libc::STDERR_FILENO);
                // Redirect stdin from /dev/null.
                libc::close(libc::STDIN_FILENO);
                libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY);
            }
            // Drop the original fd (stdout/stderr now own the file).
            drop(log);

            // Write PID file.
            std::fs::write(&pid_file, format!("{}\n", std::process::id()))?;

            Ok(DaemonGuard {
                write_fd,
                pid_file,
                notified: false,
            })
        }
        child_pid => {
            // ── Parent ──
            unsafe { libc::close(write_fd) };

            // Wait for the child to signal readiness (or error/EOF).
            let mut buf = [0u8; 1];
            let n = unsafe { libc::read(read_fd, buf.as_mut_ptr() as *mut _, 1) };
            unsafe { libc::close(read_fd) };

            if n == 1 && buf[0] == b'R' {
                eprintln!("Daemon started (pid={child_pid}), logs: {}", log_file.display());
                eprintln!("Stop with: hf-mount-daemon stop {}", mount_point.display());
                std::process::exit(0);
            } else {
                eprintln!("Daemon failed to start. Check logs: {}", log_file.display());
                std::process::exit(1);
            }
        }
    }
}
