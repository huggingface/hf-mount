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

    // Resolve symlinks in the parent directory (e.g. macOS /tmp → /private/tmp)
    // so that start and stop derive the same PID file key even when the mount
    // point doesn't exist yet.
    if let Some(parent) = normalized.parent()
        && let Ok(real_parent) = std::fs::canonicalize(parent)
        && let Some(name) = normalized.file_name()
    {
        return real_parent.join(name);
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

/// Check if a PID file describes a live daemon for the given canonical mount point.
fn is_daemon_alive(pf: &PidFileContents, canonical: &Path) -> bool {
    is_our_daemon(pf.pid) && pf.mount.as_deref().is_none_or(|m| Path::new(m) == canonical)
}

/// Check if a PID is alive (any process, no identity check).
fn pid_alive(pid: i32) -> bool {
    (unsafe { libc::kill(pid, 0) }) == 0
}

/// Parsed contents of a PID file.
struct PidFileContents {
    pid: i32,
    mount: Option<String>,
    source: Option<String>,
}

/// Read a PID file.
/// Format: line 1 = PID, line 2 = canonical mount path, line 3 = source label.
fn read_pid_file(pid_file: &Path) -> Option<PidFileContents> {
    let content = std::fs::read_to_string(pid_file).ok()?;
    let mut lines = content.lines();
    let pid: i32 = lines.next()?.trim().parse().ok()?;
    let mount = lines.next().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let source = lines.next().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    Some(PidFileContents { pid, mount, source })
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

    /// The write end of the ready-notification pipe.
    pub fn write_fd(&self) -> i32 {
        self.write_fd
    }

    /// Signal the parent that the mount is active and it can exit.
    pub fn notify_ready(&mut self) {
        if !self.notified {
            let ret = unsafe { libc::write(self.write_fd, b"R".as_ptr() as *const _, 1) };
            if ret < 0 {
                eprintln!("daemon: failed to notify parent (write_fd={})", self.write_fd);
            }
            unsafe { libc::close(self.write_fd) };
            self.notified = true;
        }
    }

    /// Create a DaemonGuard from env vars set by the `hf-mount` daemon.
    /// Returns None if not running as a daemon subprocess.
    pub fn from_env() -> Option<Self> {
        let fd: i32 = std::env::var("HF_MOUNT_DAEMON_FD").ok()?.parse().ok()?;
        let pid_file = std::env::var("HF_MOUNT_DAEMON_PID_FILE").ok().map(PathBuf::from)?;
        // SAFETY: called at startup before any threads are spawned.
        unsafe {
            std::env::remove_var("HF_MOUNT_DAEMON_FD");
            std::env::remove_var("HF_MOUNT_DAEMON_PID_FILE");
        }
        Some(Self {
            write_fd: fd,
            pid_file,
            notified: false,
        })
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if !self.notified {
            // Best-effort: parent may already be gone (broken pipe).
            let _ = unsafe { libc::write(self.write_fd, b"E".as_ptr() as *const _, 1) };
            unsafe { libc::close(self.write_fd) };
        }
        let _ = std::fs::remove_file(&self.pid_file);
    }
}

/// Run an unmount command, logging the attempt.
fn run_unmount(cmd: &str, args: &[&str]) -> bool {
    eprintln!("  trying: {} {}", cmd, args.join(" "));
    std::process::Command::new(cmd)
        .args(args)
        .status()
        .is_ok_and(|s| s.success())
}

/// Try to unmount a mount point using all available strategies.
#[allow(clippy::needless_return)]
fn try_unmount(mount_point: &Path) -> bool {
    let mount_str = mount_point.to_string_lossy();
    #[cfg(target_os = "macos")]
    {
        return run_unmount("umount", &[&mount_str]) || run_unmount("diskutil", &["unmount", &mount_str]);
    }
    #[cfg(target_os = "linux")]
    {
        if unsafe { libc::getuid() } == 0 {
            return run_unmount("umount", &[&mount_str]);
        }
        return run_unmount("umount", &[&mount_str])
            || run_unmount("sudo", &["-n", "umount", &mount_str])
            || run_unmount("fusermount3", &["-u", "-z", &mount_str])
            || run_unmount("fusermount", &["-u", "-z", &mount_str]);
    }
}

/// Stop a running daemon by unmounting and waiting for the process to exit.
pub fn stop_daemon(mount_point: &Path) -> std::io::Result<()> {
    let pid_file = pid_path(mount_point);
    let canonical = canonical_mount_point(mount_point);
    let pf = read_pid_file(&pid_file).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no daemon found for {:?} (pid file: {})",
                mount_point,
                pid_file.display()
            ),
        )
    })?;
    let pid = pf.pid;

    // Check if the process is actually our daemon for this mount point.
    let daemon_alive = is_daemon_alive(&pf, &canonical);
    if !daemon_alive {
        eprintln!("Daemon (pid={pid}) is not running, cleaning up stale pid file");
        let _ = std::fs::remove_file(&pid_file);
        // Still try to unmount in case the mount survived a crash.
        if !try_unmount(mount_point) {
            eprintln!("Warning: could not unmount {:?} (may need manual cleanup)", mount_point);
        }
        return Ok(());
    }

    eprint!("Stopping daemon (pid={pid})");

    if !try_unmount(mount_point) {
        eprintln!();
        return Err(std::io::Error::other(format!(
            "failed to unmount {mount_point:?} (is something still using it?)"
        )));
    }

    // Wait up to 30s for the daemon to exit. The daemon may spend time
    // in shutdown() flushing dirty data after unmount.
    for _ in 0..60 {
        if !pid_alive(pid) {
            eprintln!(" done");
            return Ok(());
        }
        eprint!(".");
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Still running, send SIGTERM.
    eprint!(" sending SIGTERM");
    unsafe { libc::kill(pid, libc::SIGTERM) };
    for _ in 0..10 {
        if !pid_alive(pid) {
            eprintln!(" done");
            return Ok(());
        }
        eprint!(".");
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    eprintln!();

    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("daemon (pid={pid}) did not stop after 35s"),
    ))
}

/// Decode a percent-encoded filename back to a path.
fn decode_path(encoded: &str) -> String {
    let mut result = String::with_capacity(encoded.len());
    let mut chars = encoded.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            if let (Some(hi), Some(lo)) = (chars.next(), chars.next())
                && let Ok(byte) = u8::from_str_radix(&format!("{hi}{lo}"), 16)
            {
                result.push(byte as char);
                continue;
            }
            // Truncated or invalid sequence: keep the literal '%'
            result.push('%');
        } else {
            result.push(c);
        }
    }
    format!("/{result}")
}

/// Information about a running daemon.
pub struct DaemonInfo {
    pub pid: i32,
    pub mount_point: String,
    pub source: Option<String>,
    pub log_file: PathBuf,
}

/// List all running daemons by scanning PID files.
pub fn list_daemons() -> Vec<DaemonInfo> {
    let pids_dir = state_dir().join("pids");
    let Ok(entries) = std::fs::read_dir(&pids_dir) else {
        return vec![];
    };

    let mut daemons = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("pid") {
            continue;
        }
        let Some(pf) = read_pid_file(&path) else {
            continue;
        };
        if !is_our_daemon(pf.pid) {
            continue;
        }

        // Derive mount point from stored path or from the filename.
        let mount_point = pf.mount.unwrap_or_else(|| {
            let stem = path.file_stem().unwrap_or_default().to_string_lossy();
            decode_path(&stem)
        });

        let log_file = state_dir().join("logs").join(format!(
            "{}.log",
            path.file_stem().unwrap_or_default().to_string_lossy()
        ));

        daemons.push(DaemonInfo {
            pid: pf.pid,
            mount_point,
            source: pf.source,
            log_file,
        });
    }

    daemons
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
pub fn daemonize(mount_point: &Path, source_label: &str) -> std::io::Result<DaemonGuard> {
    let log_file = log_path(mount_point);
    let pid_file = pid_path(mount_point);
    let canonical = canonical_mount_point(mount_point);

    std::fs::create_dir_all(log_file.parent().expect("log_file has parent"))?;
    std::fs::create_dir_all(pid_file.parent().expect("pid_file has parent"))?;

    // Refuse to start if a daemon is already running for this mount point.
    if let Some(existing) = read_pid_file(&pid_file) {
        if is_daemon_alive(&existing, &canonical) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "daemon already running (pid={}) for {:?}. Stop it first with: hf-mount stop {}",
                    existing.pid,
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

            // Write PID file: line 1 = PID, line 2 = mount path, line 3 = source.
            std::fs::write(
                &pid_file,
                format!("{}\n{}\n{}\n", std::process::id(), canonical.display(), source_label),
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

            eprint!("Starting daemon (pid={child_pid})");

            // Poll the pipe with a timeout so we can print dots while waiting.
            let mut pfd = libc::pollfd {
                fd: read_fd,
                events: libc::POLLIN,
                revents: 0,
            };
            loop {
                let ret = unsafe { libc::poll(&raw mut pfd, 1, 1000) };
                if ret > 0 {
                    break; // data or hangup ready
                }
                if ret < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.kind() == std::io::ErrorKind::Interrupted {
                        continue; // EINTR, retry
                    }
                    eprintln!(" poll error: {err}");
                    std::process::exit(1);
                }
                eprint!(".");
            }

            let mut buf = [0u8; 1];
            let n = unsafe { libc::read(read_fd, buf.as_mut_ptr() as *mut _, 1) };
            unsafe { libc::close(read_fd) };

            if n == 1 && buf[0] == b'R' {
                eprintln!(" ready");
                eprintln!("  logs: {}", log_file.display());
                eprintln!("  stop: hf-mount stop {}", canonical.display());
                std::process::exit(0);
            } else {
                eprintln!(" failed");
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
