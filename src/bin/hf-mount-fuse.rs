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
    let s = hf_mount::setup::build(args, false);

    // Phase 4: mount and serve.
    hf_mount::fuse::mount_fuse(
        s.virtual_fs,
        &s.mount_point,
        s.metadata_ttl,
        s.read_only,
        s.advanced_writes,
        s.direct_io,
        s.max_threads,
        &s.runtime,
        daemon_guard.as_mut(),
    );

    info!("Unmounted cleanly");
}
