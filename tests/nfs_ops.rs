mod common;

#[tokio::test]
async fn test_nfs_read_only() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let (token, bucket_id, _hub) =
        match common::setup_bucket_with_file("nfs", &remote_file, test_content.as_bytes()).await {
            Some(cfg) => cfg,
            None => return,
        };

    let mount_point = format!("/tmp/hf-mount-nfs-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--backend=nfs", "--read-only"]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content).map(|_| {
            // Read-only enforcement: writes must fail
            eprintln!("  [nfs] read-only enforcement");
            let result = std::fs::write(format!("{}/should_fail.txt", mount_point), "nope");
            assert!(result.is_err(), "write should fail on read-only NFS mount");
            eprintln!("  [nfs] write correctly rejected: {:?}", result.unwrap_err().kind());
        });
        common::unmount_nfs(&mount_point, child, 5);
        r
    }));

    common::delete_bucket(common::ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("All NFS read-only tests passed!"),
        Ok(Err(e)) => panic!("NFS test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// NFS writable mount — uses advanced_writes (staging files) automatically.
#[tokio::test]
async fn test_nfs_writes() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let (token, bucket_id, hub) =
        match common::setup_bucket_with_file("nfs-w", &remote_file, test_content.as_bytes()).await {
            Some(cfg) => cfg,
            None => return,
        };

    let mount_point = format!("/tmp/hf-mount-nfs-w-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-w-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--backend=nfs"]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content)
            .and_then(|_| common::fs_tests::run_write_tests(&mount_point, &remote_file, &test_content));
        common::unmount_nfs(&mount_point, child, 30);
        r
    }));

    let trunc_size = test_content.find("BBBB_MIDDLE_BBBB").unwrap() as u64;
    let hub_check = common::fs_tests::verify_hub_state(&hub, &remote_file, trunc_size).await;

    common::delete_bucket(common::ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("All NFS write tests passed!"),
        Ok(Err(e)) => panic!("NFS write test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
    if let Err(e) = hub_check {
        panic!("NFS write Hub verification failed: {}", e);
    }
}
