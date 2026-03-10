use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// State directory for daemon files (~/.hf-mount/).
fn state_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".hf-mount")
}

fn sanitize_path(mount_point: &Path) -> String {
    mount_point.to_string_lossy().trim_matches('/').replace('/', "_")
}

fn pid_path(mount_point: &Path) -> PathBuf {
    state_dir()
        .join("pids")
        .join(format!("{}.pid", sanitize_path(mount_point)))
}

fn log_path(mount_point: &Path) -> PathBuf {
    state_dir()
        .join("logs")
        .join(format!("{}.log", sanitize_path(mount_point)))
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

/// Fork the process into a background daemon.
///
/// - Parent: waits for the child to confirm the mount is active, prints
///   daemon info (PID, log path, unmount command), then exits.
/// - Child: detaches from the terminal (setsid), redirects stderr to a log
///   file, writes a PID file, and returns a `DaemonGuard`. The caller must
///   call `guard.notify_ready()` once the mount is confirmed.
///
/// # Safety
/// Must be called before any threads are spawned (before tokio runtime).
pub fn daemonize(mount_point: &Path) -> std::io::Result<DaemonGuard> {
    let log_file = log_path(mount_point);
    let pid_file = pid_path(mount_point);

    std::fs::create_dir_all(log_file.parent().unwrap())?;
    std::fs::create_dir_all(pid_file.parent().unwrap())?;

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
                eprintln!("Unmount with: umount {}", mount_point.display());
                std::process::exit(0);
            } else {
                eprintln!("Daemon failed to start. Check logs: {}", log_file.display());
                std::process::exit(1);
            }
        }
    }
}
