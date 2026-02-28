mod auth;
mod cache;
mod error;
mod fs;
mod hub_api;
mod inode;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use data::data_client::default_config;
use data::FileDownloadSession;
use tracing::{error, info};

use crate::auth::{HubTokenRefresher, HubWriteTokenRefresher};
use crate::cache::FileCache;
use crate::fs::HfFs;
use crate::hub_api::HubApiClient;

#[derive(Parser)]
#[command(name = "hf-mount", about = "Mount a HuggingFace bucket as a FUSE filesystem")]
struct Args {
    #[arg(long)]
    bucket_id: String,

    #[arg(long)]
    mount_point: PathBuf,

    #[arg(long, env = "HF_TOKEN")]
    hf_token: String,

    #[arg(long, default_value = "https://huggingface.co")]
    hub_endpoint: String,

    #[arg(long, default_value = "/tmp/hf-mount-cache")]
    cache_dir: PathBuf,

    #[arg(long)]
    uid: Option<u32>,

    #[arg(long)]
    gid: Option<u32>,

    #[arg(long, default_value_t = false)]
    read_only: bool,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("hf_mount=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    // Build tokio runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let hub_client = Arc::new(HubApiClient::new(&args.hub_endpoint, &args.hf_token));

    // Get initial CAS read JWT
    let cas_jwt = rt
        .block_on(hub_client.get_cas_token(&args.bucket_id))
        .expect("Failed to get CAS token");

    info!("Got CAS token for endpoint: {}", cas_jwt.cas_url);

    // Build read token refresher
    let refresher = Arc::new(HubTokenRefresher::new(
        hub_client.clone(),
        args.bucket_id.clone(),
    ));

    // Build read TranslatorConfig
    let read_config = default_config(
        cas_jwt.cas_url.clone(),
        None,
        Some((cas_jwt.access_token, cas_jwt.exp)),
        Some(refresher),
        None,
    )
    .expect("Failed to build TranslatorConfig");

    let read_config = Arc::new(read_config);

    // Create FileDownloadSession
    let download_session = rt
        .block_on(FileDownloadSession::new(read_config, None))
        .expect("Failed to create download session");

    // Create upload config if not read-only
    let upload_config = if !args.read_only {
        let write_jwt = rt
            .block_on(hub_client.get_cas_write_token(&args.bucket_id))
            .expect("Failed to get CAS write token");

        info!("Got CAS write token for endpoint: {}", write_jwt.cas_url);

        let write_refresher = Arc::new(HubWriteTokenRefresher::new(
            hub_client.clone(),
            args.bucket_id.clone(),
        ));

        let write_config = default_config(
            write_jwt.cas_url,
            None,
            Some((write_jwt.access_token, write_jwt.exp)),
            Some(write_refresher),
            None,
        )
        .expect("Failed to build write TranslatorConfig");

        Some(Arc::new(write_config))
    } else {
        None
    };

    // Create file cache
    let cache = Arc::new(FileCache::new(args.cache_dir, download_session, upload_config));

    // Determine uid/gid
    let uid = args.uid.unwrap_or_else(|| unsafe { libc::getuid() });
    let gid = args.gid.unwrap_or_else(|| unsafe { libc::getgid() });

    // Create FUSE filesystem
    let hf_fs = HfFs::new(
        rt.handle().clone(),
        hub_client,
        args.bucket_id.clone(),
        cache,
        args.read_only,
        uid,
        gid,
    );

    // Ensure mount point exists
    std::fs::create_dir_all(&args.mount_point).ok();

    let mode = if args.read_only { "read-only" } else { "read-write" };
    info!("Mounting bucket {} at {:?} ({})", args.bucket_id, args.mount_point, mode);

    // Mount options
    let mut fuse_config = fuser::Config::default();
    fuse_config.mount_options = vec![
        fuser::MountOption::FSName("hf-mount".to_string()),
        fuser::MountOption::DefaultPermissions,
    ];
    if args.read_only {
        fuse_config.mount_options.push(fuser::MountOption::RO);
    }
    fuse_config.acl = fuser::SessionACL::All;

    // Mount (this blocks until unmount)
    if let Err(e) = fuser::mount2(hf_fs, &args.mount_point, &fuse_config) {
        error!("FUSE mount failed: {}", e);
        std::process::exit(1);
    }

    info!("Unmounted cleanly");
}
