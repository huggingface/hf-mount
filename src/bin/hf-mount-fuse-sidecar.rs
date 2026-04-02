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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use tracing::{error, info, warn};

use hf_mount::fuse::mount_fuse;
use hf_mount::setup::{Args as MountArgs, build, init_tracing, raise_fd_limit};

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

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = Arc::clone(&shutdown);
        ctrlc::set_handler(move || {
            info!("Received shutdown signal");
            shutdown.store(true, Ordering::Relaxed);
        })
        .expect("failed to install signal handler");
    }

    // Clear stale global markers from a previous sidecar attempt (restart).
    let _ = std::fs::remove_file(args.tmp_dir.join(".ready"));
    let _ = std::fs::remove_file(args.tmp_dir.join(".error"));

    info!("HF mount sidecar starting, watching {}", args.tmp_dir.display());

    let pending = wait_for_configs(
        &args.tmp_dir,
        args.poll_secs,
        args.timeout_secs,
        args.expected_mounts,
        &shutdown,
    );
    if pending.is_empty() {
        if shutdown.load(Ordering::Relaxed) {
            info!("Shutting down before any mounts started");
            return;
        }
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

        let fuse_fd = match connect_and_receive_fd(&mount.socket_path, 60) {
            Ok(fd) => fd,
            Err(err) => {
                write_error(&error_path, &format!("Failed to receive fd for {}: {}", label, err));
                continue;
            }
        };

        info!("Received fd={:?} for {}", fuse_fd, label);

        error_paths.push(error_path.clone());
        handles.push(std::thread::spawn(move || {
            run_mount(fuse_fd, mount.mount_args, error_path);
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

fn wait_for_configs(
    tmp_dir: &Path,
    poll_secs: u64,
    timeout_secs: u64,
    expected: usize,
    shutdown: &AtomicBool,
) -> Vec<PendingMount> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("Shutdown requested during config discovery");
            return Vec::new();
        }

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

/// Connect to the CSI driver's Unix socket and receive the FUSE fd via SCM_RIGHTS.
/// Retries on ENOENT/ECONNREFUSED until the socket appears or timeout expires.
///
/// Wire format (set by the CSI driver's Go SendMsg):
///   - iov[0]: optional data payload (unused, we just need the fd)
///   - cmsg:   single SCM_RIGHTS carrying one i32 fd
fn connect_and_receive_fd(socket_path: &Path, timeout_secs: u64) -> io::Result<OwnedFd> {
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
    // Control message buffer, sized for exactly one fd (one i32).
    let mut cmsg_buf = vec![0u8; unsafe { libc::CMSG_SPACE(size_of::<i32>() as u32) as usize }];

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

    let raw_fd = unsafe {
        let cmsg_ref = &*cmsg;
        if cmsg_ref.cmsg_level != libc::SOL_SOCKET || cmsg_ref.cmsg_type != libc::SCM_RIGHTS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected control message type",
            ));
        }
        // The fd is stored right after the cmsg header.
        *(libc::CMSG_DATA(cmsg) as *const i32)
    };

    // SAFETY: raw_fd was just received via SCM_RIGHTS and is a valid open fd.
    Ok(unsafe { OwnedFd::from_raw_fd(raw_fd) })
}

/// Log an error and write it to the error file for the CSI driver to read.
fn write_error(path: &Path, msg: &str) {
    error!("{}", msg);
    let _ = std::fs::write(path, msg);
}

fn run_mount(fuse_fd: OwnedFd, mount_args: MountArgs, error_path: PathBuf) {
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

    info!("Starting FUSE mount for {} (fd={:?})", label, fuse_fd);

    let session = match mount_fuse(&setup, None, Some(fuse_fd)) {
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
