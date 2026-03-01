use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use data::FileDownloadSession;
use data::data_client::default_config;
use tracing::{error, info};

use hf_mount::auth::HubTokenRefresher;
use hf_mount::cache::FileCache;
use hf_mount::caching_client::CachingClient;
use hf_mount::fs::HfFs;
use hf_mount::hub_api::HubApiClient;

#[derive(Parser)]
#[command(name = "hf-mount", about = "Mount a HuggingFace bucket as a filesystem")]
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

    /// Interval in seconds for polling remote changes (0 to disable)
    #[arg(long, default_value_t = 30)]
    poll_interval_secs: u64,

    /// Mount backend: "fuse" (default) or "nfs" (requires --features nfs)
    #[arg(long, default_value = "fuse")]
    backend: String,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("hf_mount=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    // Tune xet-core for interactive FUSE reads (not batch downloads).
    // Only set if not already overridden by the user.
    for (k, v) in [
        // Start with 16 concurrent connections instead of ramping from 1
        ("HF_XET_CLIENT_AC_INITIAL_DOWNLOAD_CONCURRENCY", "16"),
        // Allow concurrency adjustments after 4 MB (vs 20 MB default) for faster ramp-up
        ("HF_XET_CLIENT_AC_MIN_BYTES_REQUIRED_FOR_ADJUSTMENT", "4194304"),
        // Fetch blocks matching VFS initial window (8 MB vs 256 MB default)
        ("HF_XET_RECONSTRUCTION_MIN_RECONSTRUCTION_FETCH_SIZE", "8388608"),
        // Prefetch buffer (8 MB) — kept small to avoid buffering latency on first fetch
        ("HF_XET_RECONSTRUCTION_MIN_PREFETCH_BUFFER", "8388608"),
        // Target 30s block completion (vs 15 min default) for better prefetch sizing
        ("HF_XET_RECONSTRUCTION_TARGET_BLOCK_COMPLETION_TIME", "30"),
        // Download buffer sized for large VFS windows (128 MB vs 2 GB default)
        ("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE", "134217728"),
        ("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_LIMIT", "268435456"),
    ] {
        if std::env::var(k).is_err() {
            // SAFETY: called before any threads are spawned.
            unsafe { std::env::set_var(k, v) };
        }
    }

    // Build tokio runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let hub_client = HubApiClient::new(&args.hub_endpoint, &args.hf_token);

    // Build read CAS config
    let read_refresher = Arc::new(HubTokenRefresher::for_read(hub_client.clone(), args.bucket_id.clone()));
    let read_config = Arc::new(build_cas_config(&rt, &read_refresher, "read"));

    // Create on-disk xorb chunk cache for cross-file deduplication.
    let xorb_cache = {
        let config = data::CacheConfig {
            cache_directory: args.cache_dir.join("xorbs"),
            cache_size: 10_000_000_000,
        };
        data::get_cache(&config).expect("Failed to create xorb cache")
    };

    // Create CAS client with reconstruction cache wrapper
    let raw_client = rt
        .block_on(data::create_remote_client(&read_config, "hf-mount", false))
        .expect("Failed to create CAS client");
    let caching_client = CachingClient::new(raw_client);

    let download_session = FileDownloadSession::from_client(caching_client, None, Some(xorb_cache));

    // Build write CAS config (if not read-only)
    let upload_config = if !args.read_only {
        let write_refresher = Arc::new(HubTokenRefresher::for_write(hub_client.clone(), args.bucket_id.clone()));
        Some(Arc::new(build_cas_config(&rt, &write_refresher, "write")))
    } else {
        None
    };

    // Create file cache
    let cache = FileCache::new(args.cache_dir, download_session, upload_config);

    // Determine uid/gid
    let uid = args.uid.unwrap_or_else(|| unsafe { libc::getuid() });
    let gid = args.gid.unwrap_or_else(|| unsafe { libc::getgid() });

    // Ensure mount point exists
    std::fs::create_dir_all(&args.mount_point).ok();

    let mode = if args.read_only { "read-only" } else { "read-write" };
    info!(
        "Mounting bucket {} at {:?} ({}, backend={})",
        args.bucket_id, args.mount_point, mode, args.backend
    );

    match args.backend.as_str() {
        "fuse" => {
            let hf_fs = HfFs::new(
                rt.handle().clone(),
                hub_client,
                args.bucket_id.clone(),
                cache,
                args.read_only,
                uid,
                gid,
                args.poll_interval_secs,
            );

            let mut fuse_config = fuser::Config::default();
            fuse_config.mount_options = vec![
                fuser::MountOption::FSName("hf-mount".to_string()),
                fuser::MountOption::DefaultPermissions,
            ];
            if args.read_only {
                fuse_config.mount_options.push(fuser::MountOption::RO);
            }
            fuse_config.acl = fuser::SessionACL::All;
            fuse_config.clone_fd = true;
            fuse_config.n_threads = Some(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4)
                    .max(4),
            );

            if let Err(e) = fuser::mount2(hf_fs, &args.mount_point, &fuse_config) {
                error!("FUSE mount failed: {}", e);
                std::process::exit(1);
            }
        }
        #[cfg(feature = "nfs")]
        "nfs" => {
            use hf_mount::vfs::HfVfsCore;

            let vfs = HfVfsCore::new(
                rt.handle().clone(),
                hub_client,
                args.bucket_id.clone(),
                cache,
                args.read_only,
                uid,
                gid,
                args.poll_interval_secs,
            );

            if let Err(e) = rt.block_on(hf_mount::nfs::mount_nfs(vfs, &args.mount_point)) {
                error!("NFS mount failed: {}", e);
                std::process::exit(1);
            }
        }
        #[cfg(not(feature = "nfs"))]
        "nfs" => {
            error!("NFS backend requires building with --features nfs");
            std::process::exit(1);
        }
        other => {
            error!("Unknown backend: {other}. Use \"fuse\" or \"nfs\".");
            std::process::exit(1);
        }
    }

    info!("Unmounted cleanly");
}

fn build_cas_config(
    rt: &tokio::runtime::Runtime,
    refresher: &Arc<HubTokenRefresher>,
    label: &str,
) -> data::configurations::TranslatorConfig {
    let jwt = rt.block_on(refresher.fetch_initial()).unwrap_or_else(|e| {
        panic!("Failed to get CAS {label} token: {e}");
    });
    info!("Got CAS {label} token for endpoint: {}", jwt.cas_url);
    default_config(
        jwt.cas_url,
        None,
        Some((jwt.access_token, jwt.exp)),
        Some(refresher.clone()),
        None,
    )
    .unwrap_or_else(|e| panic!("Failed to build {label} TranslatorConfig: {e}"))
}
