use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Integration tests for write support (CAS write token + batch add/delete).
/// Requires HF_TOKEN and TEST_WRITE_BUCKET_ID env vars. Skips gracefully if not set.

fn get_write_test_config() -> Option<(String, String)> {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return None;
        }
    };
    let bucket_id = match std::env::var("TEST_WRITE_BUCKET_ID") {
        Ok(b) => b,
        Err(_) => {
            eprintln!("Skipping: TEST_WRITE_BUCKET_ID not set");
            return None;
        }
    };
    Some((token, bucket_id))
}

#[tokio::test]
async fn test_write_token() {
    let (token, bucket_id) = match get_write_test_config() {
        Some(cfg) => cfg,
        None => return,
    };

    let hub = Arc::new(hf_mount::hub_api::HubApiClient::new(
        "https://huggingface.co",
        &token,
    ));

    let write_token = hub
        .get_cas_write_token(&bucket_id)
        .await
        .expect("get_cas_write_token failed");

    assert!(!write_token.cas_url.is_empty(), "cas_url should not be empty");
    assert!(
        !write_token.access_token.is_empty(),
        "access_token should not be empty"
    );
    assert!(write_token.exp > 0, "exp should be a positive timestamp");

    eprintln!(
        "Write token OK: cas_url={}, exp={}",
        write_token.cas_url, write_token.exp
    );
}

#[tokio::test]
async fn test_batch_add_and_delete() {
    let (token, bucket_id) = match get_write_test_config() {
        Some(cfg) => cfg,
        None => return,
    };

    let hub = Arc::new(hf_mount::hub_api::HubApiClient::new(
        "https://huggingface.co",
        &token,
    ));

    // Build upload config for CAS upload
    let write_jwt = hub
        .get_cas_write_token(&bucket_id)
        .await
        .expect("get_cas_write_token failed");

    let write_refresher = Arc::new(hf_mount::auth::HubWriteTokenRefresher::new(
        hub.clone(),
        bucket_id.clone(),
    ));

    let write_config = data::data_client::default_config(
        write_jwt.cas_url,
        None,
        Some((write_jwt.access_token, write_jwt.exp)),
        Some(write_refresher),
        None,
    )
    .expect("default_config failed");

    let write_config = Arc::new(write_config);

    // Also build a read config (needed for FileCache)
    let read_jwt = hub
        .get_cas_token(&bucket_id)
        .await
        .expect("get_cas_token failed");

    let read_refresher = Arc::new(hf_mount::auth::HubTokenRefresher::new(
        hub.clone(),
        bucket_id.clone(),
    ));

    let read_config = data::data_client::default_config(
        read_jwt.cas_url,
        None,
        Some((read_jwt.access_token, read_jwt.exp)),
        Some(read_refresher),
        None,
    )
    .expect("default_config failed");

    let download_session = data::FileDownloadSession::new(Arc::new(read_config), None)
        .await
        .expect("FileDownloadSession::new failed");

    // Create a temp cache dir
    let cache_dir = std::env::temp_dir().join("hf-mount-write-test");
    let cache = Arc::new(hf_mount::cache::FileCache::new(
        cache_dir.clone(),
        download_session,
        Some(write_config),
    ));

    // Create a small test file on disk
    let test_filename = format!("_test_write_{}.txt", std::process::id());
    let staging_path = cache_dir.join(&test_filename);
    std::fs::create_dir_all(&cache_dir).ok();
    std::fs::write(&staging_path, "hello from write test\n").expect("write staging file");

    // Upload via CAS
    let file_info = cache
        .upload_file(&staging_path)
        .await
        .expect("upload_file failed");

    let xet_hash = file_info.hash().to_string();
    eprintln!("Uploaded test file: xet_hash={}, size={}", xet_hash, file_info.file_size());

    // Batch add the file to the bucket
    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(
        &bucket_id,
        &[hf_mount::hub_api::BatchOp::AddFile {
            path: test_filename.clone(),
            xet_hash: xet_hash.clone(),
            mtime: mtime_ms,
            content_type: None,
        }],
    )
    .await
    .expect("batch add failed");

    eprintln!("Batch add OK: {}", test_filename);

    // Verify the file appears in tree listing
    let entries = hub
        .list_tree(&bucket_id, "")
        .await
        .expect("list_tree failed");

    let found = entries.iter().find(|e| e.path == test_filename);
    assert!(
        found.is_some(),
        "added file '{}' should appear in tree listing",
        test_filename
    );
    eprintln!("Verified file present in tree listing");

    // Now delete it via batch
    hub.batch_operations(
        &bucket_id,
        &[hf_mount::hub_api::BatchOp::DeleteFile {
            path: test_filename.clone(),
        }],
    )
    .await
    .expect("batch delete failed");

    eprintln!("Batch delete OK: {}", test_filename);

    // Verify it's gone from tree listing
    let entries = hub
        .list_tree(&bucket_id, "")
        .await
        .expect("list_tree failed after delete");

    let found = entries.iter().find(|e| e.path == test_filename);
    assert!(
        found.is_none(),
        "deleted file '{}' should not appear in tree listing",
        test_filename
    );
    eprintln!("Verified file removed from tree listing");

    // Cleanup
    std::fs::remove_file(&staging_path).ok();
    std::fs::remove_dir_all(&cache_dir).ok();
}

/// Full round-trip: write a file → upload to CAS → commit to bucket → download back → verify content.
#[tokio::test]
async fn test_write_then_read_back() {
    let (token, bucket_id) = match get_write_test_config() {
        Some(cfg) => cfg,
        None => return,
    };

    let hub = Arc::new(hf_mount::hub_api::HubApiClient::new(
        "https://huggingface.co",
        &token,
    ));

    // Build write config
    let write_jwt = hub
        .get_cas_write_token(&bucket_id)
        .await
        .expect("get_cas_write_token failed");

    let write_refresher = Arc::new(hf_mount::auth::HubWriteTokenRefresher::new(
        hub.clone(),
        bucket_id.clone(),
    ));

    let write_config = data::data_client::default_config(
        write_jwt.cas_url,
        None,
        Some((write_jwt.access_token, write_jwt.exp)),
        Some(write_refresher),
        None,
    )
    .expect("default_config failed");

    let write_config = Arc::new(write_config);

    // Build read config + download session
    let read_jwt = hub
        .get_cas_token(&bucket_id)
        .await
        .expect("get_cas_token failed");

    let read_refresher = Arc::new(hf_mount::auth::HubTokenRefresher::new(
        hub.clone(),
        bucket_id.clone(),
    ));

    let read_config = data::data_client::default_config(
        read_jwt.cas_url,
        None,
        Some((read_jwt.access_token, read_jwt.exp)),
        Some(read_refresher),
        None,
    )
    .expect("default_config failed");

    let download_session = data::FileDownloadSession::new(Arc::new(read_config), None)
        .await
        .expect("FileDownloadSession::new failed");

    // Create cache
    let cache_dir = std::env::temp_dir().join("hf-mount-roundtrip-test");
    let cache = Arc::new(hf_mount::cache::FileCache::new(
        cache_dir.clone(),
        download_session,
        Some(write_config),
    ));

    // Write a test file with known content
    let test_content = "round-trip test content 🚀\nline 2\nline 3\n";
    let test_filename = format!("_test_roundtrip_{}.txt", std::process::id());
    let staging_path = cache_dir.join(&test_filename);
    std::fs::create_dir_all(&cache_dir).ok();
    std::fs::write(&staging_path, test_content).expect("write staging file");

    // Upload to CAS
    let file_info = cache
        .upload_file(&staging_path)
        .await
        .expect("upload_file failed");

    let xet_hash = file_info.hash().to_string();
    let file_size = file_info.file_size();
    eprintln!("Uploaded: xet_hash={}, size={}", xet_hash, file_size);

    // Commit to bucket
    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(
        &bucket_id,
        &[hf_mount::hub_api::BatchOp::AddFile {
            path: test_filename.clone(),
            xet_hash: xet_hash.clone(),
            mtime: mtime_ms,
            content_type: Some("text/plain".to_string()),
        }],
    )
    .await
    .expect("batch add failed");

    eprintln!("Committed to bucket: {}", test_filename);

    // Download back via FileCache
    let downloaded_path = cache
        .ensure_cached(&xet_hash, file_size)
        .await
        .expect("ensure_cached failed");

    let downloaded_content =
        std::fs::read_to_string(&downloaded_path).expect("read downloaded file");

    assert_eq!(
        downloaded_content, test_content,
        "downloaded content should match what was uploaded"
    );
    eprintln!("Round-trip verified: content matches!");

    // Also verify via tree listing
    let entries = hub
        .list_tree(&bucket_id, "")
        .await
        .expect("list_tree failed");

    let found = entries.iter().find(|e| e.path == test_filename);
    assert!(found.is_some(), "file should appear in tree");
    let found = found.unwrap();
    assert_eq!(found.size, Some(file_size), "size should match");
    assert_eq!(
        found.xet_hash.as_deref(),
        Some(xet_hash.as_str()),
        "xet_hash should match"
    );

    // Cleanup: delete from bucket
    hub.batch_operations(
        &bucket_id,
        &[hf_mount::hub_api::BatchOp::DeleteFile {
            path: test_filename.clone(),
        }],
    )
    .await
    .expect("batch delete cleanup failed");

    eprintln!("Cleaned up: deleted {} from bucket", test_filename);

    // Cleanup local
    std::fs::remove_dir_all(&cache_dir).ok();
}
