use std::path::PathBuf;

use clap::Parser;
use tracing::{error, info};

use hf_mount::setup::{MountOptions, Source};

#[derive(Parser)]
#[command(about = "Manage hf-mount background daemons")]
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

        #[command(flatten)]
        options: Box<MountOptions>,

        #[command(subcommand)]
        source: Source,
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
        Command::Start { fuse, options, source } => {
            // Use a wrapper so DaemonGuard is always dropped (cleaning up
            // the PID file), even on error. process::exit skips destructors.
            let code = start_daemon(fuse, *options, source);
            std::process::exit(code);
        }
    }
}

/// Run the daemon start flow. Returns an exit code so the caller can exit
/// after all locals (including DaemonGuard) have been dropped.
fn start_daemon(fuse: bool, options: MountOptions, source: Source) -> i32 {
    let is_nfs = !fuse;
    let mount_point = source.mount_point().to_path_buf();
    let source_label = source.label();

    // Phase 1: init tracing (no threads yet).
    hf_mount::setup::init_tracing(true);

    // No HTTP requests before fork! reqwest/TLS leaves global state that
    // corrupts the forked child (401s, connection pool issues). Hub validation
    // happens after fork in build(), errors are surfaced via the daemon log.

    // Phase 2: fork before tokio runtime.
    let mut daemon_guard = match hf_mount::daemon::daemonize(&mount_point, &source_label) {
        Ok(guard) => guard,
        Err(e) => {
            error!("Failed to daemonize: {e}");
            return 1;
        }
    };

    // Phase 3: build runtime + VFS (includes Hub validation).
    let s = hf_mount::setup::build(source, options, is_nfs);

    // Phase 4: mount and serve.
    if is_nfs {
        if let Err(e) = s.runtime.block_on(hf_mount::nfs::mount_nfs(
            s.virtual_fs,
            &s.mount_point,
            s.metadata_ttl_ms,
            s.read_only,
            Some(&mut daemon_guard),
        )) {
            error!("NFS mount failed: {e}");
            return 1;
        }
    } else if !hf_mount::fuse::mount_fuse(
        s.virtual_fs,
        &s.mount_point,
        s.metadata_ttl,
        s.read_only,
        s.advanced_writes,
        s.direct_io,
        s.max_threads,
        &s.runtime,
        Some(&mut daemon_guard),
    ) {
        return 1;
    }

    info!("Unmounted cleanly");
    0
}
