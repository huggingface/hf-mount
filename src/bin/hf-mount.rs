use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(about = "Mount Hugging Face Buckets and repos as local filesystems")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Start a mount as a background daemon
    Start {
        /// Use FUSE backend instead of NFS (default: NFS)
        #[arg(long)]
        fuse: bool,

        /// Remaining arguments passed to the backend (hf-mount-nfs or hf-mount-fuse)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Stop a running daemon
    Stop {
        /// Mount point of the daemon to stop
        mount_point: PathBuf,
    },
    /// List running daemons
    Status,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Stop { mount_point } => match hf_mount::daemon::stop_daemon(&mount_point) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        },
        Command::Status => {
            let daemons = hf_mount::daemon::list_daemons();
            if daemons.is_empty() {
                eprintln!("No running daemons");
            } else {
                for d in &daemons {
                    let source = d.source.as_deref().unwrap_or("?");
                    eprintln!("pid={:<8} {} → {}", d.pid, source, d.mount_point);
                }
            }
        }
        Command::Start { fuse, args } => {
            let backend = if fuse { "hf-mount-fuse" } else { "hf-mount-nfs" };

            // Find the backend binary next to this binary, or in PATH.
            let backend_path = std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|dir| dir.join(backend)))
                .filter(|p| p.exists())
                .unwrap_or_else(|| PathBuf::from(backend));

            // Extract mount point from args (last positional arg, the one after bucket/repo + id).
            let mount_point = args
                .iter()
                .rev()
                .find(|a| !a.starts_with('-') && std::path::Path::new(a).is_absolute())
                .or_else(|| args.last())
                .map(PathBuf::from);

            let mount_point = match mount_point {
                Some(p) => p,
                None => {
                    eprintln!("Error: could not determine mount point from arguments");
                    eprintln!(
                        "Usage: hf-mount start [--fuse] [backend-options...] <subcommand> <source> <mount-point>"
                    );
                    std::process::exit(1);
                }
            };

            // Extract source label from args (e.g. "bucket myuser/mybucket" or "repo gpt2").
            let source_label = args
                .iter()
                .position(|a| a == "bucket" || a == "repo")
                .and_then(|i| args.get(i + 1).map(|id| format!("{} {}", args[i], id)))
                .unwrap_or_else(|| "unknown".to_string());

            // Daemonize: fork, setsid, redirect logs, write PID file.
            let guard = match hf_mount::daemon::daemonize(&mount_point, &source_label) {
                Ok(g) => g,
                Err(e) => {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            };

            // Exec the backend binary. This replaces the current process.
            // The DaemonGuard's PID file will be cleaned up when the backend exits
            // (the PID file contains this process's PID, which is now the backend's PID).
            // We need to pass the daemon pipe fd so the backend can notify readiness.
            let err = exec_backend(&backend_path, &args, &guard);
            eprintln!("Error: failed to exec {}: {err}", backend_path.display());
            std::process::exit(1);
        }
    }
}

/// Replace the current process with the backend binary via execvp.
/// Passes the ready-notification fd via the HF_MOUNT_DAEMON_FD env var.
fn exec_backend(backend: &std::path::Path, args: &[String], guard: &hf_mount::daemon::DaemonGuard) -> std::io::Error {
    use std::os::unix::process::CommandExt;

    // Clear CLOEXEC so the fd survives exec.
    unsafe {
        let flags = libc::fcntl(guard.write_fd(), libc::F_GETFD);
        libc::fcntl(guard.write_fd(), libc::F_SETFD, flags & !libc::FD_CLOEXEC);
    }

    // Tell the backend about the daemon pipe fd so it can notify readiness.
    let mut cmd = std::process::Command::new(backend);
    cmd.args(args);
    cmd.env("HF_MOUNT_DAEMON_FD", guard.write_fd().to_string());
    cmd.env("HF_MOUNT_DAEMON_PID_FILE", guard.pid_file().to_string_lossy().as_ref());

    // exec replaces the process, so this only returns on error.
    cmd.exec()
}
