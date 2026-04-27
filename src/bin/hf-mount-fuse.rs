#[cfg(not(unix))]
fn main() {
    eprintln!("hf-mount-fuse is Unix-only (FUSE). Use hf-mount-nfs.exe on Windows.");
    std::process::exit(1);
}

#[cfg(unix)]
use tracing::info;

#[cfg(unix)]
use hf_mount::fuse::mount_fuse;
#[cfg(unix)]
use hf_mount::setup::setup;

#[cfg(unix)]
fn main() {
    let s = setup(false);
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
