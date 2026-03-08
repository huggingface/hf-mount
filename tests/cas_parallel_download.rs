// Integration test: parallel CAS/S3 downloads without any FUSE/VFS layer.
//
// Purpose: determine whether xet-core's FileDownloadSession can serve N
// concurrent download streams in parallel, or whether there is internal
// serialisation (e.g. a shared lock on the reconstruction cache or the
// S3 HTTP client connection pool).
//
// If N tasks downloading independent 50 MB slices of gpt2/model.safetensors
// achieve ~N× the throughput of 1 task, the bottleneck is NOT in xet-core and
// lies elsewhere (e.g. in our prefetch slot logic).  If throughput stays flat,
// xet-core has internal serialisation that needs to be investigated upstream.
//
// Run on EC2 with cold cache for accurate results:
//   echo 3 | sudo tee /proc/sys/vm/drop_caches
//   cargo test --release --test cas_parallel_download -- --nocapture

mod common;

use std::sync::Arc;
use std::time::Instant;

use hf_mount::xet::{DownloadStreamOps, XetOps, XetSessions};

const REPO_ID: &str = "openai-community/gpt2";
const SAFETENSOR_FILE: &str = "model.safetensors";
const CHUNK_SIZE: u64 = 50 * 1024 * 1024; // 50 MB per concurrent task
const TASK_COUNTS: &[usize] = &[1, 2, 4, 8];

#[tokio::test(flavor = "multi_thread")]
async fn test_cas_parallel_download() {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return;
        }
    };

    let ep = common::endpoint();

    // Mirror hf-mount's production settings so the adaptive concurrency controller
    // starts at 16 concurrent S3 connections instead of the default 1.
    // Without this the controller serialises all downloads on startup, making the
    // parallelism benchmark meaningless.
    for (k, v) in [
        ("HF_XET_CLIENT_AC_INITIAL_DOWNLOAD_CONCURRENCY", "16"),
        ("HF_XET_CLIENT_AC_MIN_BYTES_REQUIRED_FOR_ADJUSTMENT", "4194304"),
        ("HF_XET_RECONSTRUCTION_MIN_RECONSTRUCTION_FETCH_SIZE", "8388608"),
        ("HF_XET_RECONSTRUCTION_MIN_PREFETCH_BUFFER", "8388608"),
        ("HF_XET_RECONSTRUCTION_TARGET_BLOCK_COMPLETION_TIME", "30"),
        // Do NOT cap the download buffer — hf-mount's 256 MiB limit causes the first
        // task to monopolize the buffer semaphore, starving all other concurrent tasks.
        // Use the xet-core defaults (2 GiB base, 8 GiB limit) for a fair benchmark.
    ] {
        if std::env::var(k).is_err() {
            // SAFETY: called before any threads read these vars.
            unsafe { std::env::set_var(k, v) };
        }
    }

    // Create a HubApiClient for the gpt2 model repo.
    let hub = hf_mount::hub_api::HubApiClient::from_source(
        &ep,
        Some(&token),
        hf_mount::hub_api::SourceKind::Repo {
            repo_id: REPO_ID.to_string(),
            repo_type: hf_mount::hub_api::RepoType::Model,
            revision: "main".to_string(),
        },
    )
    .await
    .expect("Failed to create hub client");

    // Get xet_hash + size for the safetensors file.
    let head = hub
        .head_file(SAFETENSOR_FILE)
        .await
        .expect("head_file failed")
        .expect("file not found on Hub");
    let xet_hash = head.xet_hash.expect("file has no xet_hash (not stored in xet)");
    let file_size = head.size.expect("file has no size in HEAD response");
    let file_info = Arc::new(data::XetFileInfo::new(xet_hash.clone(), file_size));

    eprintln!("\n=== CAS parallel download benchmark (no FUSE) ===");
    eprintln!(
        "  {}/{} — {:.1} MB, hash={}…",
        REPO_ID,
        SAFETENSOR_FILE,
        file_size as f64 / 1e6,
        &xet_hash[..8]
    );

    // Build a read-only CAS session via hub token refresher.
    let refresher = hub.token_refresher(true);
    let jwt = refresher.fetch_initial().await.expect("fetch_initial failed");

    let config = Arc::new(
        data::data_client::default_config(
            jwt.cas_url,
            None,
            Some((jwt.access_token, jwt.exp)),
            Some(refresher),
            None,
        )
        .expect("default_config failed"),
    );

    let raw_client = data::create_remote_client(&config, &uuid::Uuid::new_v4().to_string(), false)
        .await
        .expect("create_remote_client failed");
    let cached_client = hf_mount::cached_xet_client::CachedXetClient::new(raw_client);
    let session = data::FileDownloadSession::from_client(cached_client.clone(), None, None);
    // XetSessions::new() already wraps in Arc.
    let xet_sessions: Arc<XetSessions> = XetSessions::new(session, None, cached_client);

    eprintln!("  {:>8} {:>10} {:>10} {:>10}", "Tasks", "Wall (s)", "MB/s", "Scale");
    eprintln!("  {:-<8} {:-<10} {:-<10} {:-<10}", "", "", "", "");

    let mut mbps_1 = 0.0f64;

    for &n in TASK_COUNTS {
        let t0 = Instant::now();
        let mut handles = Vec::new();

        for i in 0..n {
            let sessions = xet_sessions.clone();
            let fi = file_info.clone();
            // Each task starts at a distinct 50 MB offset so they download independent slices.
            let offset = (i as u64 * CHUNK_SIZE).min(file_size.saturating_sub(1));

            handles.push(tokio::spawn(async move {
                let mut stream: Box<dyn DownloadStreamOps> = sessions
                    .download_stream_boxed(&fi, offset)
                    .expect("download_stream_boxed failed");
                let mut total = 0u64;
                loop {
                    match stream.next().await {
                        Ok(Some(chunk)) => {
                            total += chunk.len() as u64;
                            if total >= CHUNK_SIZE {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => panic!("stream error at offset={offset}: {e}"),
                    }
                }
                total
            }));
        }

        let total_bytes: u64 = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.expect("task panicked"))
            .sum();

        let elapsed = t0.elapsed().as_secs_f64();
        let mbps = total_bytes as f64 / 1e6 / elapsed;

        if n == 1 {
            mbps_1 = mbps;
            eprintln!("  {:>8} {:>10.3} {:>10.1} {:>10}", n, elapsed, mbps, "1.00x");
        } else {
            let scale = if mbps_1 > 0.0 { mbps / mbps_1 } else { 0.0 };
            eprintln!("  {:>8} {:>10.3} {:>10.1} {:>9.2}x", n, elapsed, mbps, scale);
        }
    }

    eprintln!("===");
    eprintln!();
}
