use tracing::{error, info};

use hf_mount::setup::setup;

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
#[global_allocator]
static GLOBAL: hf_mount::heap_profiling::Jemalloc = hf_mount::heap_profiling::Jemalloc;

fn main() {
    let s = setup(true);
    hf_mount::heap_profiling::maybe_spawn_periodic_dumps();
    let mut daemon_guard = hf_mount::daemon::DaemonGuard::from_env();

    if let Err(e) = s.runtime.block_on(hf_mount::nfs::mount_nfs(
        s.virtual_fs,
        &s.mount_point,
        s.metadata_ttl_ms,
        s.read_only,
        daemon_guard.as_mut(),
    )) {
        error!("NFS mount failed: {}", e);
        std::process::exit(1);
    }

    info!("Unmounted cleanly");
}
