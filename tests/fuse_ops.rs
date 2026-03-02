mod common;

#[tokio::test]
async fn test_fuse_operations() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let (token, bucket_id, hub) =
        match common::setup_bucket_with_file("fuse", &remote_file, test_content.as_bytes()).await {
            Some(cfg) => cfg,
            None => return,
        };

    let mount_point = format!("/tmp/hf-mount-fuse-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &[]);
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

    // Check FUSE operations result first (shows real error if steps failed)
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("FUSE test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
    // Then check Hub state (only meaningful if all write steps passed)
    if let Err(e) = hub_check {
        panic!("Hub state check failed: {}", e);
    }
}
