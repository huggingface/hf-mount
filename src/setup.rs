use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use data::FileDownloadSession;
use data::data_client::default_config;
use tracing::info;

use crate::cached_xet_client::CachedXetClient;
use crate::hub_api::{HubApiClient, HubTokenRefresher, SourceKind, parse_repo_id};
use crate::virtual_fs::VirtualFs;
use crate::xet::{StagingDir, XetSessions};

#[derive(clap::Subcommand)]
pub enum Source {
    /// Mount a HuggingFace bucket (read-write by default)
    Bucket {
        /// Bucket ID (e.g. "username/my-bucket")
        bucket_id: String,
        /// Local directory where the filesystem will be mounted
        mount_point: PathBuf,
    },
    /// Mount a HuggingFace repo read-only (type auto-detected from prefix)
    Repo {
        /// Repo ID (e.g. "user/model", "datasets/user/ds", "spaces/user/app")
        repo_id: String,
        /// Local directory where the filesystem will be mounted
        mount_point: PathBuf,
        /// Git revision to mount
        #[arg(long, default_value = "main")]
        revision: String,
    },
}

#[derive(Parser)]
#[command(about = "Mount a HuggingFace bucket or repo as a filesystem")]
pub struct Args {
    #[command(subcommand)]
    pub source: Source,

    /// HuggingFace API token (also read from HF_TOKEN env var)
    #[arg(long, env = "HF_TOKEN")]
    pub hf_token: String,

    /// HuggingFace Hub endpoint URL
    #[arg(long, default_value = "https://huggingface.co")]
    pub hub_endpoint: String,

    /// Directory for on-disk caches (xorb chunks, staging files)
    #[arg(long, default_value = "/tmp/hf-mount-cache")]
    pub cache_dir: PathBuf,

    /// Override the UID for all files and directories (defaults to current user)
    #[arg(long)]
    pub uid: Option<u32>,

    /// Override the GID for all files and directories (defaults to current group)
    #[arg(long)]
    pub gid: Option<u32>,

    /// Mount in read-only mode (no writes allowed)
    #[arg(long, default_value_t = false)]
    pub read_only: bool,

    /// Use staging files + async flush for writes (supports random writes and seek).
    /// Default mode is append-only with synchronous close.
    #[arg(long, default_value_t = false)]
    pub advanced_writes: bool,

    /// Interval in seconds for polling remote changes (0 to disable).
    #[arg(long, default_value_t = 30)]
    pub poll_interval_secs: u64,

    /// Maximum size in bytes for the on-disk xorb chunk cache.
    #[arg(long, default_value_t = 10_000_000_000)]
    pub cache_size: u64,

    /// Kernel metadata cache TTL in milliseconds.
    #[arg(long, default_value_t = 100)]
    pub metadata_ttl_ms: u64,

    /// Always HEAD on every lookup (skip in-memory TTL cache).
    #[arg(long, default_value_t = false)]
    pub metadata_ttl_minimal: bool,

    /// Maximum number of FUSE worker threads
    #[arg(long, default_value_t = 16)]
    pub max_threads: usize,
}

/// Everything needed to run a mount backend (FUSE or NFS).
pub struct MountSetup {
    pub runtime: tokio::runtime::Runtime,
    pub virtual_fs: Arc<VirtualFs>,
    pub mount_point: PathBuf,
    pub read_only: bool,
    pub advanced_writes: bool,
    pub metadata_ttl: std::time::Duration,
    pub max_threads: usize,
    pub metadata_ttl_ms: u64,
}

/// Parse CLI args, build VFS and all dependencies.
/// `is_nfs` controls whether advanced writes are forced (NFS has no open/close).
pub fn setup(is_nfs: bool) -> MountSetup {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("hf_mount=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    // Tune xet-core for interactive FUSE reads (not batch downloads).
    for (k, v) in [
        ("HF_XET_CLIENT_AC_INITIAL_DOWNLOAD_CONCURRENCY", "16"),
        ("HF_XET_CLIENT_AC_MIN_BYTES_REQUIRED_FOR_ADJUSTMENT", "4194304"),
        ("HF_XET_RECONSTRUCTION_MIN_RECONSTRUCTION_FETCH_SIZE", "8388608"),
        ("HF_XET_RECONSTRUCTION_MIN_PREFETCH_BUFFER", "8388608"),
        ("HF_XET_RECONSTRUCTION_TARGET_BLOCK_COMPLETION_TIME", "30"),
        ("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE", "134217728"),
        ("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_LIMIT", "268435456"),
    ] {
        if std::env::var(k).is_err() {
            // SAFETY: called before any threads are spawned.
            unsafe { std::env::set_var(k, v) };
        }
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let (mount_point, source) = match args.source {
        Source::Bucket { bucket_id, mount_point } => (mount_point, SourceKind::Bucket { bucket_id }),
        Source::Repo {
            repo_id,
            mount_point,
            revision,
        } => {
            let (repo_type, repo_id) = parse_repo_id(&repo_id);
            (
                mount_point,
                SourceKind::Repo {
                    repo_id,
                    repo_type,
                    revision,
                },
            )
        }
    };

    let hub_client = runtime.block_on(async {
        HubApiClient::from_source(&args.hub_endpoint, &args.hf_token, source)
            .await
            .unwrap_or_else(|e| panic!("Failed to initialize Hub client: {e}"))
    });

    let read_only = args.read_only || hub_client.is_repo();
    if hub_client.is_repo() && !args.read_only {
        info!("Repo mounts are always read-only");
    }

    let refresher = hub_client.token_refresher(read_only);
    let cas_config = build_cas_config(&runtime, &refresher);

    // Ensure cache directory exists and is writable.
    std::fs::create_dir_all(&args.cache_dir)
        .unwrap_or_else(|e| panic!("Failed to create cache dir {:?}: {e}", args.cache_dir));
    let xorbs_dir = args.cache_dir.join("xorbs");
    std::fs::create_dir_all(&xorbs_dir).unwrap_or_else(|e| panic!("Failed to create xorbs dir {:?}: {e}", xorbs_dir));
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

    let xorb_cache = {
        let config = data::CacheConfig {
            cache_directory: args.cache_dir.join("xorbs"),
            cache_size: args.cache_size,
        };
        data::get_cache(&config).expect("Failed to create xorb cache")
    };

    let raw_client = runtime
        .block_on(data::create_remote_client(
            &cas_config,
            &uuid::Uuid::new_v4().to_string(),
            false,
        ))
        .expect("Failed to create CAS client");
    let cached_client = CachedXetClient::new(raw_client);
    let download_session = FileDownloadSession::from_client(cached_client, None, Some(xorb_cache));
    let upload_config = if read_only { None } else { Some(cas_config) };
    let xet_sessions = XetSessions::new(download_session, upload_config);

    let advanced_writes = args.advanced_writes || (is_nfs && !read_only);
    // Repos need a staging dir for HTTP download cache (open_readonly),
    // even when advanced_writes is disabled.
    let staging_dir = if advanced_writes || hub_client.is_repo() {
        Some(StagingDir::new(&args.cache_dir))
    } else {
        None
    };

    let uid = args.uid.unwrap_or_else(|| unsafe { libc::getuid() });
    let gid = args.gid.unwrap_or_else(|| unsafe { libc::getgid() });

    // Ignore EEXIST: the directory may already exist from a previous (possibly
    // stale) mount. FUSE/NFS will fail at mount time if it's actually busy.
    if let Err(e) = std::fs::create_dir_all(&mount_point)
        && e.raw_os_error() != Some(libc::EEXIST)
    {
        panic!("Failed to create mount point {:?}: {e}", mount_point);
    }

    let backend_name = if is_nfs { "nfs" } else { "fuse" };
    info!(
        "Mounting {} at {:?} ({}, backend={})",
        hub_client.source(),
        mount_point,
        if read_only { "read-only" } else { "read-write" },
        backend_name,
    );

    let metadata_ttl = std::time::Duration::from_millis(args.metadata_ttl_ms);

    let virtual_fs = VirtualFs::new(
        runtime.handle().clone(),
        hub_client,
        xet_sessions,
        staging_dir,
        read_only,
        advanced_writes,
        uid,
        gid,
        args.poll_interval_secs,
        metadata_ttl,
        !args.metadata_ttl_minimal,
    );

    MountSetup {
        runtime,
        virtual_fs,
        mount_point,
        read_only,
        advanced_writes,
        metadata_ttl,
        max_threads: args.max_threads,
        metadata_ttl_ms: args.metadata_ttl_ms,
    }
}

fn build_cas_config(
    runtime: &tokio::runtime::Runtime,
    refresher: &Arc<HubTokenRefresher>,
) -> Arc<data::configurations::TranslatorConfig> {
    let jwt = runtime
        .block_on(refresher.fetch_initial())
        .unwrap_or_else(|e| panic!("Failed to get CAS token: {e}"));
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
