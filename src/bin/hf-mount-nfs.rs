use tracing::{error, info};

fn main() {
    // Phase 1: parse args + init tracing (no threads spawned yet).
    let args = hf_mount::setup::init();

    // Phase 2: daemonize if requested. Must happen before tokio runtime
    // creation because fork() is unsafe with multiple threads.
    let mut daemon_guard = if args.daemon {
        match hf_mount::daemon::daemonize(args.mount_point()) {
            Ok(guard) => Some(guard),
            Err(e) => {
                error!("Failed to daemonize: {e}");
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // Phase 3: build tokio runtime, CAS client, VFS (spawns threads).
    let s = hf_mount::setup::build(args, true);

    // Phase 4: mount and serve.
    if let Err(e) = s.runtime.block_on(hf_mount::nfs::mount_nfs(
        s.virtual_fs,
        &s.mount_point,
        s.metadata_ttl_ms,
        s.read_only,
        daemon_guard.as_mut(),
    )) {
        error!("NFS mount failed: {}", e);
        // daemon_guard drops here, signaling error to parent if in daemon mode.
        std::process::exit(1);
    }

    info!("Unmounted cleanly");
    // daemon_guard drops here, cleaning up PID file.
}
