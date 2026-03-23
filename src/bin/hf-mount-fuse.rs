use tracing::info;

use hf_mount::fuse::mount_fuse;
use hf_mount::setup::setup;

fn main() {
    let s = setup(false);
    let mut daemon_guard = hf_mount::daemon::DaemonGuard::from_env();

    if !mount_fuse(
        s.virtual_fs,
        &s.mount_point,
        s.metadata_ttl,
        s.read_only,
        s.advanced_writes,
        s.direct_io,
        s.max_threads,
        &s.runtime,
        daemon_guard.as_mut(),
    ) {
        std::process::exit(1);
    }

    info!("Unmounted cleanly");
}
