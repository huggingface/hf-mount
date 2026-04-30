use tracing::info;

use hf_mount::fuse::mount_fuse;
use hf_mount::setup::setup;

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
#[global_allocator]
static GLOBAL: hf_mount::heap_profiling::Jemalloc = hf_mount::heap_profiling::Jemalloc;

fn main() {
    let s = setup(false);
    hf_mount::heap_profiling::maybe_spawn_periodic_dumps();
    let mut daemon_guard = hf_mount::daemon::DaemonGuard::from_env();

    let session = match mount_fuse(&s, daemon_guard.as_mut(), Vec::new()) {
        Ok(session) => session,
        Err(err) => {
            tracing::error!("FUSE mount failed: {}", err);
            std::process::exit(1);
        }
    };

    session.wait();
    info!("Unmounted cleanly");
}
