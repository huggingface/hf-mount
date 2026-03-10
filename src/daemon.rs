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
///
/// When the full path doesn't exist, we canonicalize the longest existing ancestor
/// (resolving symlinks) and append the remaining non-existent components. This
/// ensures `/tmp/link/mnt` (where `/tmp/link` is a symlink) produces the same key
/// regardless of whether `mnt` exists yet.
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

    // Normalize away `.` and `..` components.
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

    // Try to canonicalize the longest existing prefix (resolves symlinks in
    // parents), then append the non-existent tail.
    let mut existing = normalized.clone();
    let mut tail = Vec::new();
    while !existing.as_os_str().is_empty() && existing != Path::new("/") {
        if let Ok(real) = std::fs::canonicalize(&existing) {
            let mut result = real;
            for part in tail.into_iter().rev() {
                result.push(part);
            }
            return result;
        }
        if let Some(name) = existing.file_name().map(|n| n.to_os_string()) {
            tail.push(name);
        }
        existing.pop();
    }
    normalized
}

/// Encode a path as a filename-safe string using percent-encoding.
/// `/` becomes `%2F`, `%` becomes `%25`, producing an injective mapping
/// (no two distinct paths map to the same key).
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
/// Uses kill(pid, 0) to check liveness, then verifies the process is
/// actually ours (not a recycled PID) by checking the executable name.
fn is_our_daemon(pid: i32) -> bool {
    if (unsafe { libc::kill(pid, 0) }) != 0 {
        return false;
    }

    // Verify process identity to avoid acting on recycled PIDs.
    #[cfg(target_os = "linux")]
    {
        // /proc/<pid>/exe is a symlink to the executable.
        if let Ok(exe) = std::fs::read_link(format!("/proc/{pid}/exe")) {
            let name = exe.file_name().unwrap_or_default().to_string_lossy();
            return name.starts_with("hf-mount");
        }
        // /proc not available (container?), fall through to assume ours.
    }

    #[cfg(target_os = "macos")]
    {
        // Use sysctl KERN_PROCARGS2 to get the executable path.
        let mut buf = [0u8; 4096];
        let mut len = buf.len();
        let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
        let ret = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                3,
                buf.as_mut_ptr() as *mut _,
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 && len > 4 {
            // First 4 bytes are argc, then the executable path as a NUL-terminated string.
            if let Some(end) = buf[4..len].iter().position(|&b| b == 0) {
                let exe = String::from_utf8_lossy(&buf[4..4 + end]);
                if let Some(name) = Path::new(exe.as_ref()).file_name() {
                    return name.to_string_lossy().starts_with("hf-mount");
                }
            }
        }
        // sysctl failed (permissions?), fall through to assume ours.
    }

    true
}

/// Check if a PID is alive (any process, no identity check).
fn pid_alive(pid: i32) -> bool {
    (unsafe { libc::kill(pid, 0) }) == 0
}

/// Read a PID file. Returns the PID, optional mount path, and optional start time.
/// PID file format: line 1 = PID, line 2 = canonical mount path, line 3 = start time nonce.
fn read_pid_file(pid_file: &Path) -> Option<(i32, Option<String>, Option<String>)> {
    let content = std::fs::read_to_string(pid_file).ok()?;
    let mut lines = content.lines();
    let pid: i32 = lines.next()?.trim().parse().ok()?;
    let mount = lines.next().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let start_time = lines.next().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    Some((pid, mount, start_time))
}

/// Get a process's start time as a string for identity verification.
/// Returns None if the info is unavailable (not an error, just skip the check).
fn process_start_time(_pid: i32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string(format!("/proc/{_pid}/stat")).ok()?;
        // Fields are space-separated, but field 2 (comm) can contain spaces and parens.
        // Find the last ')' to skip past comm, then parse remaining fields.
        let after_comm = stat.rfind(')')? + 2;
        let fields: Vec<&str> = stat[after_comm..].split_whitespace().collect();
        // starttime is field 22 (1-indexed), which is index 19 after comm (fields 3..=N).
        return fields.get(19).map(|s| s.to_string());
    }
    #[allow(unreachable_code)]
    None
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
            // Daemon failed before becoming ready, signal error to parent.
            unsafe {
                libc::write(self.write_fd, b"E".as_ptr() as *const _, 1);
                libc::close(self.write_fd);
            }
        }
        let _ = std::fs::remove_file(&self.pid_file);
    }
}

/// Try to unmount a mount point using all available strategies.
/// Returns true if any unmount command succeeded.
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
        let is_root = unsafe { libc::getuid() } == 0;
        if is_root {
            return std::process::Command::new("umount")
                .arg(&*mount_str)
                .status()
                .is_ok_and(|s| s.success());
        }
        // Try multiple strategies for non-root:
        // 1. umount (may work for some mount types)
        // 2. sudo -n umount (NFS mounts created with sudo mount.nfs)
        // 3. fusermount3/fusermount (FUSE user mounts)
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
/// Reads the PID file, unmounts the mount point, then waits for the process.
pub fn stop_daemon(mount_point: &Path) -> std::io::Result<()> {
    let pid_file = pid_path(mount_point);
    let canonical = canonical_mount_point(mount_point);
    let (pid, stored_mount, stored_start) = read_pid_file(&pid_file).ok_or_else(|| {
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
    // Verify: executable is hf-mount, mount path matches, start time matches.
    let daemon_alive = is_our_daemon(pid)
        && stored_mount.as_deref().is_none_or(|m| Path::new(m) == canonical)
        && stored_start
            .as_deref()
            .is_none_or(|stored| process_start_time(pid).as_deref() == Some(stored));
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

    // Unmount (this triggers the daemon's clean shutdown).
    if !try_unmount(mount_point) {
        eprintln!("Warning: unmount command failed, waiting for daemon to exit...");
    }

    // Wait up to 60s for the daemon to exit. The daemon may spend significant
    // time in shutdown() flushing dirty data after unmount.
    for _ in 0..120 {
        if !pid_alive(pid) {
            eprintln!("Daemon stopped");
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Re-validate before sending SIGTERM: PID could have been recycled during
    // the 60s wait.
    if !is_our_daemon(pid) {
        eprintln!("Daemon (pid={pid}) exited (PID recycled), cleaning up");
        let _ = std::fs::remove_file(&pid_file);
        return Ok(());
    }

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
        format!("daemon (pid={pid}) did not stop after 65s"),
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
    if let Some((existing_pid, stored_mount, stored_start)) = read_pid_file(&pid_file) {
        let is_ours = is_our_daemon(existing_pid)
            && stored_mount.as_deref().is_none_or(|m| Path::new(m) == canonical)
            && stored_start
                .as_deref()
                .is_none_or(|stored| process_start_time(existing_pid).as_deref() == Some(stored));
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

    // Claim the PID file atomically (O_EXCL) so concurrent starts don't race.
    // We write a placeholder PID (parent's) that the child overwrites after fork.
    use std::io::Write;
    let mut pid_claim = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pid_file)
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("another daemon start is in progress for {:?}", mount_point),
                )
            } else {
                e
            }
        })?;
    let self_start = process_start_time(std::process::id() as i32).unwrap_or_default();
    writeln!(
        pid_claim,
        "{}\n{}\n{}",
        std::process::id(),
        canonical.display(),
        self_start
    )?;
    drop(pid_claim);

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
            // Set CLOEXEC via fcntl on platforms without pipe2.
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

            // Write PID file with mount path and start time for identity validation.
            let child_start = process_start_time(std::process::id() as i32).unwrap_or_default();
            std::fs::write(
                &pid_file,
                format!("{}\n{}\n{}\n", std::process::id(), canonical.display(), child_start),
            )?;

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
                eprintln!("Stop with: hf-mount-daemon stop {}", canonical.display());
                std::process::exit(0);
            } else {
                eprintln!("Daemon failed to start. Check logs: {}", log_file.display());
                std::process::exit(1);
            }
        }
    }
}
