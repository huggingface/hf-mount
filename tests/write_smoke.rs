mod common;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const ENDPOINT: &str = "https://huggingface.co";

/// Create a fresh bucket and return (token, hub, bucket_id).
/// Skips the test (returns None) if HF_TOKEN is not set.
async fn setup_bucket(suffix: &str) -> Option<(String, Arc<hf_mount::hub_api::HubApiClient>, String)> {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return None;
        }
    };

    let username = common::whoami(ENDPOINT, &token).await;
    let bucket_id = format!("{}/hf-mount-{}-{}", username, suffix, std::process::id());

    common::create_bucket(ENDPOINT, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = Arc::new(hf_mount::hub_api::HubApiClient::new(ENDPOINT, &token));

    Some((token, hub, bucket_id))
}

#[tokio::test]
async fn test_write_token() {
    let (token, hub, bucket_id) = match setup_bucket("write-token").await {
        Some(cfg) => cfg,
        None => return,
    };

    let result = async {
        let write_token = hub.get_cas_write_token(&bucket_id).await?;

        assert!(!write_token.cas_url.is_empty(), "cas_url should not be empty");
        assert!(!write_token.access_token.is_empty(), "access_token should not be empty");
        assert!(write_token.exp > 0, "exp should be a positive timestamp");

        eprintln!(
            "Write token OK: cas_url={}, exp={}",
            write_token.cas_url, write_token.exp
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;
    result.unwrap();
}

#[tokio::test]
async fn test_batch_add_and_delete() {
    let (token, hub, bucket_id) = match setup_bucket("batch").await {
        Some(cfg) => cfg,
        None => return,
    };

    let result = test_batch_add_and_delete_inner(&hub, &bucket_id).await;
    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;
    result.unwrap();
}

async fn test_batch_add_and_delete_inner(
    hub: &Arc<hf_mount::hub_api::HubApiClient>,
    bucket_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let write_config = common::build_write_config(hub, bucket_id).await;

    let test_filename = format!("_test_write_{}.txt", std::process::id());
    let cache_dir = std::env::temp_dir().join("hf-mount-write-test");
    std::fs::create_dir_all(&cache_dir).ok();
    let staging_path = cache_dir.join(&test_filename);
    std::fs::write(&staging_path, "hello from write test\n")?;

    let file_info = common::upload_file(write_config, &staging_path).await;

    let xet_hash = file_info.hash().to_string();
    eprintln!(
        "Uploaded test file: xet_hash={}, size={}",
        xet_hash,
        file_info.file_size()
    );

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(
        bucket_id,
        &[hf_mount::hub_api::BatchOp::AddFile {
            path: test_filename.clone(),
            xet_hash: xet_hash.clone(),
            mtime: mtime_ms,
            content_type: None,
        }],
    )
    .await?;

    eprintln!("Batch add OK: {}", test_filename);

    let entries = hub.list_tree(bucket_id, "").await?;

    let found = entries.iter().find(|e| e.path == test_filename);
    assert!(
        found.is_some(),
        "added file '{}' should appear in tree listing",
        test_filename
    );
    eprintln!("Verified file present in tree listing");

    hub.batch_operations(
        bucket_id,
        &[hf_mount::hub_api::BatchOp::DeleteFile {
            path: test_filename.clone(),
        }],
    )
    .await?;

    eprintln!("Batch delete OK: {}", test_filename);

    let entries = hub.list_tree(bucket_id, "").await?;

    let found = entries.iter().find(|e| e.path == test_filename);
    assert!(
        found.is_none(),
        "deleted file '{}' should not appear in tree listing",
        test_filename
    );
    eprintln!("Verified file removed from tree listing");

    std::fs::remove_dir_all(&cache_dir).ok();
    Ok(())
}

/// Full round-trip: write a file → upload to CAS → commit to bucket → download back → verify content.
#[tokio::test]
async fn test_write_then_read_back() {
    let (token, hub, bucket_id) = match setup_bucket("roundtrip").await {
        Some(cfg) => cfg,
        None => return,
    };

    let result = test_write_then_read_back_inner(&hub, &bucket_id).await;
    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;
    result.unwrap();
}

async fn test_write_then_read_back_inner(
    hub: &Arc<hf_mount::hub_api::HubApiClient>,
    bucket_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let write_config = common::build_write_config(hub, bucket_id).await;

    let read_jwt = hub.get_cas_token(bucket_id).await?;

    let read_refresher = Arc::new(hf_mount::auth::HubTokenRefresher::new(
        hub.clone(),
        bucket_id.to_string(),
    ));

    let read_config = data::data_client::default_config(
        read_jwt.cas_url,
        None,
        Some((read_jwt.access_token, read_jwt.exp)),
        Some(read_refresher),
        None,
    )?;

    let download_session = data::FileDownloadSession::new(Arc::new(read_config), None, None).await?;

    let cache_dir = std::env::temp_dir().join("hf-mount-roundtrip-test");
    let cache = Arc::new(hf_mount::cache::FileCache::new(
        cache_dir.clone(),
        download_session,
        None,
    ));

    let test_content = "round-trip test content 🚀\nline 2\nline 3\n";
    let test_filename = format!("_test_roundtrip_{}.txt", std::process::id());
    let staging_path = cache_dir.join(&test_filename);
    std::fs::create_dir_all(&cache_dir).ok();
    std::fs::write(&staging_path, test_content)?;

    let file_info = common::upload_file(write_config, &staging_path).await;

    let xet_hash = file_info.hash().to_string();
    let file_size = file_info.file_size();
    eprintln!("Uploaded: xet_hash={}, size={}", xet_hash, file_size);

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(
        bucket_id,
        &[hf_mount::hub_api::BatchOp::AddFile {
            path: test_filename.clone(),
            xet_hash: xet_hash.clone(),
            mtime: mtime_ms,
            content_type: Some("text/plain".to_string()),
        }],
    )
    .await?;

    eprintln!("Committed to bucket: {}", test_filename);

    let downloaded_path = std::env::temp_dir().join(format!("hf_mount_test_{}", xet_hash));
    cache.download_to_file(&xet_hash, file_size, &downloaded_path).await?;

    let downloaded_content = std::fs::read_to_string(&downloaded_path)?;

    assert_eq!(
        downloaded_content, test_content,
        "downloaded content should match what was uploaded"
    );
    eprintln!("Round-trip verified: content matches!");

    let entries = hub.list_tree(bucket_id, "").await?;

    let found = entries.iter().find(|e| e.path == test_filename);
    assert!(found.is_some(), "file should appear in tree");
    let found = found.unwrap();
    assert_eq!(found.size, Some(file_size), "size should match");
    assert_eq!(
        found.xet_hash.as_deref(),
        Some(xet_hash.as_str()),
        "xet_hash should match"
    );

    std::fs::remove_dir_all(&cache_dir).ok();
    Ok(())
}
