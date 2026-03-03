mod common;

/// Test default (simple/streaming) write mode: append-only, synchronous close.
#[tokio::test]
async fn test_fuse_simple_writes() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let (token, bucket_id, hub) =
        match common::setup_bucket_with_file("fuse-simple", &remote_file, test_content.as_bytes()).await {
            Some(cfg) => cfg,
            None => return,
        };

    let mount_point = format!("/tmp/hf-mount-fuse-simple-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-simple-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &[]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content)
            .and_then(|_| common::fs_tests::run_simple_write_tests(&mount_point, &remote_file));
        common::unmount(&mount_point, child, 30);
        r
    }));

    let hub_check = common::fs_tests::verify_simple_hub_state(&hub, &remote_file, test_content.len() as u64).await;

    common::delete_bucket(common::ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("FUSE simple write test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
    if let Err(e) = hub_check {
        panic!("Hub state check failed: {}", e);
    }
}

/// Test --advanced-writes mode: staging files, async flush, truncate, overwrite.
#[tokio::test]
async fn test_fuse_advanced_writes() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let (token, bucket_id, hub) =
        match common::setup_bucket_with_file("fuse-adv", &remote_file, test_content.as_bytes()).await {
            Some(cfg) => cfg,
            None => return,
        };

    let mount_point = format!("/tmp/hf-mount-fuse-adv-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-adv-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--advanced-writes"]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content)
            .and_then(|_| common::fs_tests::run_write_tests(&mount_point, &remote_file, &test_content));
        common::unmount(&mount_point, child, 30);
        r
    }));

    let trunc_size = test_content.find("BBBB_MIDDLE_BBBB").unwrap() as u64;
    let hub_check = common::fs_tests::verify_hub_state(&hub, &remote_file, trunc_size).await;

    common::delete_bucket(common::ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("FUSE advanced write test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
    if let Err(e) = hub_check {
        panic!("Hub state check failed: {}", e);
    }
}

/// Test HEAD revalidation: remote file changes are detected via lookup() HEAD
/// and the kernel page cache is invalidated so re-reads return fresh content.
#[tokio::test]
async fn test_fuse_revalidation() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let (token, bucket_id, hub) =
        match common::setup_bucket_with_file("fuse-reval", &remote_file, test_content.as_bytes()).await {
            Some(cfg) => cfg,
            None => return,
        };

    let mount_point = format!("/tmp/hf-mount-fuse-reval-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-reval-cache-{}", std::process::id());

    let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &[]);

    let result = common::fs_tests::run_revalidation_test(
        &mount_point,
        &remote_file,
        &test_content,
        &hub,
        100, // metadata_ttl_ms (default)
    )
    .await;

    common::unmount(&mount_point, child, 30);
    common::delete_bucket(common::ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    if let Err(e) = result {
        panic!("FUSE revalidation test failed: {}", e);
    }
}
