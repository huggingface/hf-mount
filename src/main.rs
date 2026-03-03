use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use data::FileDownloadSession;
use data::data_client::default_config;
use tracing::{error, info};

use hf_mount::cached_xet_client::CachedXetClient;
use hf_mount::fuse::FuseAdapter;
use hf_mount::hub_api::{HubApiClient, HubTokenRefresher};
use hf_mount::xet::{StagingDir, XetSessions};

#[derive(Parser)]
#[command(name = "hf-mount", about = "Mount a HuggingFace bucket as a filesystem")]
struct Args {
    /// HuggingFace bucket ID (e.g. "username/my-bucket")
    #[arg(long)]
    bucket_id: String,

    /// Local directory where the bucket will be mounted
    #[arg(long)]
    mount_point: PathBuf,

    /// HuggingFace API token (also read from HF_TOKEN env var)
    #[arg(long, env = "HF_TOKEN")]
    hf_token: String,

    /// HuggingFace Hub endpoint URL
    #[arg(long, default_value = "https://huggingface.co")]
    hub_endpoint: String,

    /// Directory for on-disk caches (xorb chunks, staging files)
    #[arg(long, default_value = "/tmp/hf-mount-cache")]
    cache_dir: PathBuf,

    /// Override the UID for all files and directories (defaults to current user)
    #[arg(long)]
    uid: Option<u32>,

    /// Override the GID for all files and directories (defaults to current group)
    #[arg(long)]
    gid: Option<u32>,

    /// Mount in read-only mode (no writes allowed)
    #[arg(long, default_value_t = false)]
    read_only: bool,

    /// Use staging files + async flush for writes (supports random writes and seek).
    /// Default mode is append-only with synchronous close.
    #[arg(long, default_value_t = false)]
    advanced_writes: bool,

    /// Interval in seconds for polling remote changes (0 to disable).
    /// The poll loop detects remote file additions, modifications, and deletions,
    /// and invalidates the kernel cache accordingly.
    #[arg(long, default_value_t = 30)]
    poll_interval_secs: u64,

    /// Maximum size in bytes for the on-disk xorb chunk cache.
    /// This cache stores CAS chunks locally to avoid re-downloading across files
    /// that share deduplicated content.
    #[arg(long, default_value_t = 10_000_000_000)]
    cache_size: u64,

    /// Kernel metadata cache TTL in milliseconds. The kernel caches file/directory
    /// attributes for this duration before re-validating. Lower values detect remote
    /// changes faster, higher values reduce kernel-to-FUSE round-trips.
    #[arg(long, default_value_t = 100)]
    metadata_ttl_ms: u64,

    /// Minimal metadata mode: always HEAD on every lookup to check for remote changes,
    /// even within the metadata TTL window.
    /// `--metadata-ttl minimal` behavior. Without this flag, lookups within the TTL
    /// window skip HEAD and serve from the in-memory cache (faster re-reads).
    #[arg(long, default_value_t = false)]
    metadata_ttl_minimal: bool,

    /// Maximum number of FUSE worker threads
    #[arg(long, default_value_t = 16)]
    max_threads: usize,

    /// Mount backend: "fuse" (default) or "nfs" (requires --features nfs).
    /// NFS is read-only and does not support writes.
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
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let hub_client = HubApiClient::new(&args.hub_endpoint, &args.hf_token, &args.bucket_id);

    // Build CAS config — use a write token when read-write (it can also read).
    // NFS backend is always read-only, so force read token regardless of flag.
    let cas_read_only = args.read_only || args.backend == "nfs";
    let refresher = hub_client.token_refresher(cas_read_only);
    let cas_config = build_cas_config(&runtime, &refresher);

    // Ensure cache directory exists and is writable by the current user.
    std::fs::create_dir_all(&args.cache_dir)
        .unwrap_or_else(|e| panic!("Failed to create cache dir {:?}: {e}", args.cache_dir));
    let xorbs_dir = args.cache_dir.join("xorbs");
    std::fs::create_dir_all(&xorbs_dir).unwrap_or_else(|e| panic!("Failed to create xorbs dir {:?}: {e}", xorbs_dir));
    // Check write access — a previous run as a different user (e.g. root) may have
    // left directories owned by that user, causing cryptic errors later.
    let probe = xorbs_dir.join(".write-check");
    if let Err(e) = std::fs::write(&probe, b"") {
        let user = std::env::var("USER").unwrap_or_default();
        panic!(
            "Cache dir {:?} is not writable: {e}\n\
             Fix with: sudo chown -R {user} {:?}",
            xorbs_dir, args.cache_dir
        );
    }
    std::fs::remove_file(&probe).ok();

    // Create on-disk xorb chunk cache for cross-file deduplication.
    let xorb_cache = {
        let config = data::CacheConfig {
            cache_directory: args.cache_dir.join("xorbs"),
            cache_size: args.cache_size,
        };
        data::get_cache(&config).expect("Failed to create xorb cache")
    };

    // Create CAS client with reconstruction cache wrapper
    let raw_client = runtime
        .block_on(data::create_remote_client(
            &cas_config,
            &uuid::Uuid::new_v4().to_string(),
            false,
        ))
        .expect("Failed to create CAS client");
    let cached_client = CachedXetClient::new(raw_client);

    let download_session = FileDownloadSession::from_client(cached_client, None, Some(xorb_cache));

    let upload_config = if args.read_only { None } else { Some(cas_config) };

    let xet_sessions = XetSessions::new(download_session, upload_config);
    let staging_dir = if args.advanced_writes {
        Some(StagingDir::new(&args.cache_dir))
    } else {
        None
    };

    // Determine uid/gid
    let uid = args.uid.unwrap_or_else(|| unsafe { libc::getuid() });
    let gid = args.gid.unwrap_or_else(|| unsafe { libc::getgid() });

    std::fs::create_dir_all(&args.mount_point)
        .unwrap_or_else(|e| panic!("Failed to create mount point {:?}: {e}", args.mount_point));

    info!(
        "Mounting bucket {} at {:?} ({}, backend={})",
        hub_client.bucket_id(),
        args.mount_point,
        if args.read_only { "read-only" } else { "read-write" },
        args.backend
    );

    match args.backend.as_str() {
        "fuse" => {
            use hf_mount::virtual_fs::VirtualFs;
            use std::time::Duration;

            let metadata_ttl = Duration::from_millis(args.metadata_ttl_ms);

            let virtual_fs = VirtualFs::new(
                runtime.handle().clone(),
                hub_client,
                xet_sessions.clone(),
                staging_dir,
                args.read_only,
                args.advanced_writes,
                uid,
                gid,
                args.poll_interval_secs,
                metadata_ttl,
                !args.metadata_ttl_minimal,
            );

            let fuse_adapter = FuseAdapter::new(runtime.handle().clone(), virtual_fs.clone(), metadata_ttl);

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
            fuse_config.n_threads = Some(args.max_threads);

            let session = match fuser::Session::new(fuse_adapter, &args.mount_point, &fuse_config) {
                Ok(s) => s,
                Err(e) => {
                    error!("FUSE session failed: {}", e);
                    std::process::exit(1);
                }
            };
            let notifier = session.notifier();
            virtual_fs.set_invalidator(Box::new(move |ino| {
                if let Err(e) = notifier.inval_inode(fuser::INodeNo(ino), 0, -1) {
                    tracing::debug!("inval_inode({}) failed: {}", ino, e);
                }
            }));
            let bg = match session.spawn() {
                Ok(bg) => bg,
                Err(e) => {
                    error!("FUSE spawn failed: {}", e);
                    std::process::exit(1);
                }
            };
            let _ = bg.join();
        }
        #[cfg(feature = "nfs")]
        "nfs" => {
            use hf_mount::virtual_fs::VirtualFs;

            if args.advanced_writes {
                error!("--advanced-writes is not supported with NFS backend");
                std::process::exit(1);
            }

            let virtual_fs = VirtualFs::new(
                runtime.handle().clone(),
                hub_client,
                xet_sessions,
                None,  // NFS is read-only, no staging dir
                true,  // NFS backend is always read-only
                false, // advanced_writes not supported on NFS
                uid,
                gid,
                args.poll_interval_secs,
                std::time::Duration::from_millis(args.metadata_ttl_ms),
                !args.metadata_ttl_minimal,
            );

            if let Err(e) = runtime.block_on(hf_mount::nfs::mount_nfs(
                virtual_fs,
                &args.mount_point,
                args.metadata_ttl_ms,
            )) {
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
    runtime: &tokio::runtime::Runtime,
    refresher: &Arc<HubTokenRefresher>,
) -> Arc<data::configurations::TranslatorConfig> {
    let jwt = runtime.block_on(refresher.fetch_initial()).unwrap_or_else(|e| {
        panic!("Failed to get CAS token: {e}");
    });
    info!("Got CAS token for endpoint: {}", jwt.cas_url);
    Arc::new(
        default_config(
            jwt.cas_url,
            None,
            Some((jwt.access_token, jwt.exp)),
            Some(refresher.clone()),
            None,
        )
        .unwrap_or_else(|e| panic!("Failed to build TranslatorConfig: {e}")),
    )
}
