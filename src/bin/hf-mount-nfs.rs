use tracing::{error, info};

use hf_mount::setup::setup;

fn main() {
    let s = setup(true);

    #[cfg(unix)]
    let mut daemon_guard = hf_mount::daemon::DaemonGuard::from_env();

    #[cfg(unix)]
    let notify_ready: Option<Box<dyn FnMut() + '_>> = match daemon_guard.as_mut() {
        Some(g) => Some(Box::new(|| g.notify_ready())),
        None => None,
    };
    #[cfg(not(unix))]
    let notify_ready: Option<Box<dyn FnMut() + '_>> = None;

    if let Err(e) = s.runtime.block_on(hf_mount::nfs::mount_nfs(
        s.virtual_fs,
        &s.mount_point,
        s.metadata_ttl_ms,
        s.read_only,
        &s.nfs_bind,
        notify_ready,
    )) {
        error!("NFS mount failed: {}", e);
        std::process::exit(1);
    }

    info!("Unmounted cleanly");
}
