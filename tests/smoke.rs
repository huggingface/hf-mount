use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Smoke test: writes a file to the bucket, then reads it back and verifies content.
/// Self-contained: does not depend on pre-existing bucket state.
/// Requires HF_TOKEN env var. Uses TEST_WRITE_BUCKET_ID or defaults to XciD/hf-mount-test-bucket.
#[tokio::test]
async fn test_write_then_download() {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return;
        }
    };
    let bucket_id = std::env::var("TEST_WRITE_BUCKET_ID")
        .unwrap_or_else(|_| "XciD/hf-mount-test-bucket".to_string());

    let hub = Arc::new(hf_mount::hub_api::HubApiClient::new(
        "https://huggingface.co",
        &token,
    ));

    // --- Write phase: upload a file to CAS then commit to bucket ---

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
    .expect("write default_config failed");

    let cache_dir = std::env::temp_dir().join("hf-mount-smoke-test");
    std::fs::create_dir_all(&cache_dir).ok();

    let test_content = "smoke test content\n";
    let test_filename = format!("_smoke_test_{}.txt", std::process::id());
    let staging_path = cache_dir.join(&test_filename);
    std::fs::write(&staging_path, test_content).expect("write staging file");

    let upload_session =
        data::FileUploadSession::new(Arc::new(write_config), None)
            .await
            .expect("FileUploadSession::new failed");

    let mut results = upload_session
        .upload_files(vec![(staging_path.clone(), None::<mdb_shard::Sha256>)])
        .await
        .expect("upload_files failed");

    let file_info = results.pop().expect("upload returned no file info");

    upload_session
        .finalize()
        .await
        .expect("finalize failed");

    let xet_hash = file_info.hash().to_string();
    let file_size = file_info.file_size();
    eprintln!("Uploaded: xet_hash={}, size={}", xet_hash, file_size);

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

    eprintln!("Committed to bucket: {}", test_filename);

    // --- Verify in tree listing ---

    let entries = hub
        .list_tree(&bucket_id, "")
        .await
        .expect("list_tree failed");

    let found = entries.iter().find(|e| e.path == test_filename);
    assert!(found.is_some(), "file should appear in tree listing");
    let found = found.unwrap();
    assert_eq!(found.size, Some(file_size));
    assert!(found.xet_hash.is_some());
    eprintln!("Tree listing OK: found {} with size={}", test_filename, file_size);

    // --- Read phase: download via CAS and verify content ---

    let cas_info = hub
        .get_cas_token(&bucket_id)
        .await
        .expect("get_cas_token failed");

    let read_refresher = Arc::new(hf_mount::auth::HubTokenRefresher::new(
        hub.clone(),
        bucket_id.clone(),
    ));

    let read_config = data::data_client::default_config(
        cas_info.cas_url,
        None,
        Some((cas_info.access_token, cas_info.exp)),
        Some(read_refresher),
        None,
    )
    .expect("read default_config failed");

    let session = data::FileDownloadSession::new(Arc::new(read_config), None)
        .await
        .expect("FileDownloadSession::new failed");

    let download_path = cache_dir.join(format!("downloaded_{}", test_filename));
    let dl_file_info = data::XetFileInfo::new(xet_hash.clone(), file_size);
    session
        .download_file(&dl_file_info, &download_path, None)
        .await
        .expect("download_file failed");

    let content = std::fs::read_to_string(&download_path).expect("read downloaded file");
    assert_eq!(content, test_content, "downloaded content should match");
    eprintln!("Download OK: content matches");

    // --- Cleanup: delete from bucket + local files ---

    hub.batch_operations(
        &bucket_id,
        &[hf_mount::hub_api::BatchOp::DeleteFile {
            path: test_filename.clone(),
        }],
    )
    .await
    .expect("batch delete cleanup failed");

    eprintln!("Cleaned up: deleted {} from bucket", test_filename);
    std::fs::remove_dir_all(&cache_dir).ok();
}
