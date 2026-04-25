//! CSI sidecar mounter: connects to the CSI driver's socket, receives the FUSE
//! fd via SCM_RIGHTS, and runs hf-mount-fuse in-process with Session::from_fd().
//!
//! The sidecar runs UNPRIVILEGED as a native init container (KEP-753). The CSI
//! driver (privileged DaemonSet) opens /dev/fuse and does the kernel mount.
//!
//! Each volume's config is a plain args file (one flag per line) using the same
//! CLI syntax as hf-mount-fuse, written by the CSI driver to a shared emptyDir.

use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use tracing::{error, info, warn};

use hf_mount::fuse::mount_fuse;
use hf_mount::setup::{Args as MountArgs, build, init_tracing, raise_fd_limit};
use hf_mount::virtual_fs::VirtualFs;

/// Set of running mounts, exposed to the SIGTERM handler so it can drain
/// dirty data before the process exits.
type VfsRegistry = Arc<Mutex<Vec<Arc<VirtualFs>>>>;

#[derive(Parser)]
#[command(about = "CSI sidecar mounter for HF volumes")]
struct Args {
    /// Shared emptyDir where the CSI driver writes per-volume args files.
    #[arg(long, default_value = "/hf-csi-tmp")]
    tmp_dir: PathBuf,

    /// How often to scan for new volume configs (seconds).
    #[arg(long, default_value_t = 2)]
    poll_secs: u64,

    /// Give up if no configs appear within this duration (seconds).
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,

    /// Expected number of mounts (set by the webhook). Discovery returns
    /// immediately once this many configs are found.
    #[arg(long)]
    expected_mounts: usize,
}

struct PendingMount {
    mount_args: MountArgs,
    socket_path: PathBuf,
}

fn main() {
    let args = Args::parse();
    raise_fd_limit();
    init_tracing(false);

    // SIGTERM handler: drain every running VFS (flushes dirty inodes to the
    // Hub via the flush manager) in parallel, then exit. The empty-registry
    // case (signal arrives during config discovery) is just a fast no-op exit.
    //
    // We can't unmount from here — the CSI driver did the kernel mount()
    // against the host kubelet path, which the sidecar can't see (the args
    // file uses the "/tmp" placeholder). fuser's tokio signal handler would
    // try `umount2(/tmp, MNT_DETACH)`, fail, and `bg.join()` would block
    // forever; kubelet would then SIGKILL at the full grace period and any
    // dirty data would be lost.
    //
    // Driving `VirtualFs::shutdown()` ourselves uses the grace window to
    // flush, then we exit; the kernel ends the FUSE connection on fd close
    // and CSI's NodeUnpublishVolume does the host-side umount.
    let vfs_registry: VfsRegistry = Arc::new(Mutex::new(Vec::new()));
    {
        let vfs_registry = Arc::clone(&vfs_registry);
        ctrlc::set_handler(move || {
            let vfs_list: Vec<Arc<VirtualFs>> = vfs_registry.lock().expect("vfs_registry poisoned").clone();
            if vfs_list.is_empty() {
                info!("Received shutdown signal, exiting");
                std::process::exit(0);
            }
            info!("Received shutdown signal, flushing {} mount(s)", vfs_list.len());
            // Drain in parallel — total wall time is max(flush), not sum(flush).
            let drain_handles: Vec<_> = vfs_list
                .into_iter()
                .map(|vfs| std::thread::spawn(move || vfs.shutdown()))
                .collect();
            for h in drain_handles {
                let _ = h.join();
            }
            info!("Flush complete, exiting");
            std::process::exit(0);
        })
        .expect("failed to install signal handler");
    }

    // Clear stale global markers from a previous sidecar attempt (restart).
    let _ = std::fs::remove_file(args.tmp_dir.join(".ready"));
    let _ = std::fs::remove_file(args.tmp_dir.join(".error"));

    info!("HF mount sidecar starting, watching {}", args.tmp_dir.display());

    let pending = wait_for_configs(&args.tmp_dir, args.poll_secs, args.timeout_secs, args.expected_mounts);
    if pending.is_empty() {
        error!("No mount configs found after {}s, exiting", args.timeout_secs);
        std::process::exit(1);
    }

    info!("Discovered {} pending mount(s)", pending.len());

    let mut handles = Vec::with_capacity(pending.len());
    let mut error_paths = Vec::with_capacity(pending.len());
    for mount in pending {
        let label = mount.mount_args.source.label();
        let error_path = mount.socket_path.with_file_name("error");

        // Clear stale markers from a previous sidecar attempt (restart).
        let _ = std::fs::remove_file(&error_path);
        let _ = std::fs::remove_file(error_path.with_file_name("ready"));

        let fuse_fds = match connect_and_receive_fds(&mount.socket_path, 60) {
            Ok(fds) if fds.is_empty() => {
                write_error(&error_path, &format!("Received zero fds for {}", label));
                continue;
            }
            Ok(fds) => fds,
            Err(err) => {
                write_error(&error_path, &format!("Failed to receive fds for {}: {}", label, err));
                continue;
            }
        };

        info!("Received {} fd(s) for {}", fuse_fds.len(), label);

        error_paths.push(error_path.clone());
        let vfs_registry = Arc::clone(&vfs_registry);
        handles.push(std::thread::spawn(move || {
            run_mount(fuse_fds, mount.mount_args, error_path, vfs_registry);
        }));
    }

    // Wait for all FUSE daemons to signal ready (or write an error file).
    let mut ok_count = 0;
    for error_path in &error_paths {
        let ready_path = error_path.with_file_name("ready");
        while !ready_path.exists() && !error_path.exists() {
            std::thread::sleep(Duration::from_millis(100));
        }
        if ready_path.exists() {
            ok_count += 1;
        }
    }

    if ok_count == args.expected_mounts {
        let ready_path = args.tmp_dir.join(".ready");
        if let Err(err) = std::fs::write(&ready_path, "") {
            error!("Failed to write ready file {}: {}", ready_path.display(), err);
        } else {
            info!("Ready ({} mount(s) active)", ok_count);
        }
    } else {
        let msg = format!(
            "{}/{} mounts failed",
            args.expected_mounts - ok_count,
            args.expected_mounts
        );
        let error_path = args.tmp_dir.join(".error");
        let _ = std::fs::write(&error_path, &msg);
        error!("{}, wrote {}", msg, error_path.display());
    }

    for handle in handles {
        if let Err(err) = handle.join() {
            error!("Mount thread panicked: {:?}", err);
        }
    }

    info!("All mounts exited");
}

fn wait_for_configs(tmp_dir: &Path, poll_secs: u64, timeout_secs: u64, expected: usize) -> Vec<PendingMount> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        match discover_pending(tmp_dir) {
            Ok(mounts) if !mounts.is_empty() => {
                if mounts.len() >= expected {
                    return mounts;
                }
                info!("Found {} config(s), expected {}", mounts.len(), expected);
            }
            Ok(_) => {}
            Err(err) => {
                error!("Config discovery error: {}", err);
            }
        }

        if Instant::now() >= deadline {
            return Vec::new();
        }

        std::thread::sleep(Duration::from_secs(poll_secs));
    }
}

fn discover_pending(tmp_dir: &Path) -> io::Result<Vec<PendingMount>> {
    let volumes_dir = tmp_dir.join(".volumes");
    let entries = match std::fs::read_dir(&volumes_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let mut mounts = Vec::new();
    for entry in entries.filter_map(|e| e.ok()) {
        let args_path = entry.path().join("args");
        let data = match std::fs::read_to_string(&args_path) {
            Ok(data) => data,
            Err(_) => continue,
        };
        let tokens: Vec<&str> = data.lines().filter(|l| !l.is_empty()).collect();
        let mount_args = match MountArgs::try_parse_from(tokens) {
            Ok(args) => args,
            Err(err) => {
                warn!("Skipping {}: {}", args_path.display(), err);
                continue;
            }
        };
        let socket_path = entry.path().join("s");
        mounts.push(PendingMount {
            mount_args,
            socket_path,
        });
    }
    Ok(mounts)
}

/// Connect to the CSI driver's Unix socket and receive the FUSE fds via SCM_RIGHTS.
/// Retries on ENOENT/ECONNREFUSED until the socket appears or timeout expires.
///
/// Wire format (set by the CSI driver's Go SendMsg):
///   - iov[0]: optional data payload (unused, we just need the fds)
///   - cmsg:   single SCM_RIGHTS carrying N i32 fds (1 primary + N-1 cloned).
///     The CSI driver decides the count; here we accept any non-zero N up
///     to `MAX_FUSE_FDS`.
const MAX_FUSE_FDS: usize = 32;

fn connect_and_receive_fds(socket_path: &Path, timeout_secs: u64) -> io::Result<Vec<OwnedFd>> {
    info!("Connecting to CSI driver socket at {}", socket_path.display());
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let stream = loop {
        match UnixStream::connect(socket_path) {
            Ok(s) => break s,
            Err(err) if matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused) => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("socket {} not available after {}s", socket_path.display(), timeout_secs),
                    ));
                }
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(err) => return Err(err),
        }
    };
    info!("Connected to CSI driver");

    let fd = stream.as_raw_fd();

    // Data buffer (iov) for the regular message payload.
    let mut buf = [0u8; 4096];
    // Control message buffer, sized for up to MAX_FUSE_FDS fds.
    let cmsg_capacity = unsafe { libc::CMSG_SPACE((size_of::<i32>() * MAX_FUSE_FDS) as u32) as usize };
    let mut cmsg_buf = vec![0u8; cmsg_capacity];

    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };

    let mut msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: &mut iov,
        msg_iovlen: 1,
        msg_control: cmsg_buf.as_mut_ptr() as *mut libc::c_void,
        msg_controllen: cmsg_buf.len() as _,
        msg_flags: 0,
    };

    // recvmsg reads both the data payload and the ancillary control message.
    let n = unsafe { libc::recvmsg(fd, &mut msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed before receiving fd",
        ));
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "control message truncated"));
    }

    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no control message received",
        ));
    }

    let cmsg_ref = unsafe { &*cmsg };
    if cmsg_ref.cmsg_level != libc::SOL_SOCKET || cmsg_ref.cmsg_type != libc::SCM_RIGHTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected control message type",
        ));
    }
    // cmsg_len includes the cmsghdr; the fds occupy the rest.
    let header_len = unsafe { libc::CMSG_LEN(0) as usize };
    let payload_len = (cmsg_ref.cmsg_len as usize).saturating_sub(header_len);
    if payload_len == 0 || payload_len % size_of::<i32>() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("malformed SCM_RIGHTS payload ({payload_len} bytes)"),
        ));
    }
    let fd_count = payload_len / size_of::<i32>();
    let data_ptr = unsafe { libc::CMSG_DATA(cmsg) as *const i32 };

    let mut fds = Vec::with_capacity(fd_count);
    for i in 0..fd_count {
        let raw_fd = unsafe { *data_ptr.add(i) };
        // SAFETY: raw_fd was just received via SCM_RIGHTS and is a valid open fd.
        fds.push(unsafe { OwnedFd::from_raw_fd(raw_fd) });
    }
    Ok(fds)
}

/// Log an error and write it to the error file for the CSI driver to read.
fn write_error(path: &Path, msg: &str) {
    error!("{}", msg);
    let _ = std::fs::write(path, msg);
}

fn run_mount(fuse_fds: Vec<OwnedFd>, mount_args: MountArgs, error_path: PathBuf, vfs_registry: VfsRegistry) {
    let label = mount_args.source.label();

    // build() panics on auth/config errors (e.g. invalid token, CAS 401).
    // Catch the panic so we can write the error to the error file for the
    // CSI driver to report as FailedMount.
    let setup = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        build(mount_args.source, mount_args.options, false)
    })) {
        Ok(s) => s,
        Err(panic) => {
            let msg = match panic.downcast_ref::<String>() {
                Some(s) => s.clone(),
                None => match panic.downcast_ref::<&str>() {
                    Some(s) => s.to_string(),
                    None => "unknown panic".to_string(),
                },
            };
            write_error(&error_path, &format!("Setup failed for {}: {}", label, msg));
            return;
        }
    };

    // Register the VFS so the SIGTERM handler can drain it on shutdown.
    vfs_registry
        .lock()
        .expect("vfs_registry poisoned")
        .push(setup.virtual_fs.clone());

    info!("Starting FUSE mount for {} ({} fd(s))", label, fuse_fds.len());

    let session = match mount_fuse(&setup, None, fuse_fds) {
        Ok(s) => s,
        Err(err) => {
            write_error(&error_path, &format!("FUSE mount failed for {}: {}", label, err));
            return;
        }
    };

    // FUSE is now serving. Write per-volume ready file.
    let _ = std::fs::write(error_path.with_file_name("ready"), "");

    session.wait();
    info!("FUSE session ended for {}", label);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixListener;
    use std::thread;

    /// Open `n` file descriptors, send them all in one SCM_RIGHTS over a Unix
    /// socket, and verify the receiver reconstructs the same count of valid
    /// fds backed by the same files.
    fn send_fds_and_receive(n: usize) -> Vec<OwnedFd> {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket_path = dir.path().join("s");

        // Each fd points to a file we can identify by content after recv.
        let payloads: Vec<String> = (0..n).map(|i| format!("fd_{i}_marker")).collect();
        let mut tmp_files = Vec::with_capacity(n);
        let mut raw_fds = Vec::with_capacity(n);
        for payload in &payloads {
            let mut tmp = tempfile::tempfile().unwrap();
            tmp.write_all(payload.as_bytes()).unwrap();
            raw_fds.push(tmp.as_raw_fd());
            tmp_files.push(tmp);
        }

        let listener = UnixListener::bind(&socket_path).expect("bind socket");
        let socket_path_clone = socket_path.clone();

        let recv = thread::spawn(move || connect_and_receive_fds(&socket_path_clone, 5).expect("recv"));

        let (server, _) = listener.accept().expect("accept");
        let server_fd = server.as_raw_fd();

        let mut data: u8 = 1;
        let mut iov = libc::iovec {
            iov_base: &mut data as *mut u8 as *mut libc::c_void,
            iov_len: 1,
        };
        let cmsg_space = unsafe { libc::CMSG_SPACE((size_of::<i32>() * n) as u32) as usize };
        let mut cmsg_buf = vec![0u8; cmsg_space];
        let msg = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut iov,
            msg_iovlen: 1,
            msg_control: cmsg_buf.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_buf.len() as _,
            msg_flags: 0,
        };
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN((size_of::<i32>() * n) as u32) as _;
            let data_ptr = libc::CMSG_DATA(cmsg) as *mut i32;
            for (i, raw) in raw_fds.iter().enumerate() {
                *data_ptr.add(i) = *raw;
            }
            let sent = libc::sendmsg(server_fd, &msg, 0);
            assert!(sent > 0, "sendmsg failed: {}", io::Error::last_os_error());
        }
        // Close server side so receiver can return.
        drop(server);
        drop(tmp_files); // we kept them alive only until sendmsg ran.

        recv.join().unwrap()
    }

    #[test]
    fn receives_one_fd() {
        let fds = send_fds_and_receive(1);
        assert_eq!(fds.len(), 1);
    }

    #[test]
    fn receives_multiple_fds() {
        let fds = send_fds_and_receive(4);
        assert_eq!(fds.len(), 4);
    }

    #[test]
    fn receives_max_fds() {
        let fds = send_fds_and_receive(MAX_FUSE_FDS);
        assert_eq!(fds.len(), MAX_FUSE_FDS);
    }
}
