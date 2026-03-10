use tracing::{error, info};

use hf_mount::setup::setup;

fn main() {
    let s = setup(true);

    if let Err(e) = s.runtime.block_on(hf_mount::nfs::mount_nfs(
        s.virtual_fs,
        &s.mount_point,
        s.metadata_ttl_ms,
        s.read_only,
        None,
    )) {
        error!("NFS mount failed: {}", e);
        std::process::exit(1);
    }

    info!("Unmounted cleanly");
}
