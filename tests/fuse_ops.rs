mod common;

/// Test default (simple/streaming) write mode: append-only, synchronous close.
#[tokio::test]
async fn test_fuse_simple_writes() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let guard = match common::setup_bucket_with_file("fuse-simple", &remote_file, test_content.as_bytes()).await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-fuse-simple-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-simple-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &[]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content)
            .and_then(|_| common::fs_tests::run_simple_write_tests(&mount_point, &remote_file));
        common::unmount(&mount_point, child, 30);
        r
    }));

    let hub_check =
        common::fs_tests::verify_simple_hub_state(&guard.hub, &remote_file, test_content.len() as u64).await;

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
    let guard = match common::setup_bucket_with_file("fuse-adv", &remote_file, test_content.as_bytes()).await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-fuse-adv-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-adv-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--advanced-writes"]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content)
            .and_then(|_| common::fs_tests::run_write_tests(&mount_point, &remote_file, &test_content));
        common::unmount(&mount_point, child, 30);
        r
    }));

    let trunc_size = test_content.find("BBBB_MIDDLE_BBBB").unwrap() as u64;
    let hub_check = common::fs_tests::verify_hub_state(&guard.hub, &remote_file, trunc_size).await;

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

/// Read a file under a large subdir without any prior `readdir`: the VFS
/// slow path should resolve the file via HEAD alone, without materializing
/// the sibling inodes. A successful content match proves the HEAD path
/// wires up correctly end-to-end.
#[tokio::test]
async fn test_fuse_point_lookup_skips_list_tree() {
    let guard = match common::setup_bucket("fuse-point-lookup").await {
        Some(g) => g,
        None => return,
    };
    let (target_rel, target_content) = common::seed_big_dir_with_target(&guard.hub, "fuse-pl").await;

    let mount_point = format!("/tmp/hf-mount-pl-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-pl-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let op = common::fs_tests::assert_point_read(&mount_point, &target_rel, target_content);
        common::unmount(&mount_point, child, 30);
        op
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("point lookup test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// Read a file via a cold deep path — every intermediate directory must
/// resolve correctly through the HEAD-then-list-tree fallback chain.
#[tokio::test]
async fn test_fuse_deep_cold_read() {
    let guard = match common::setup_bucket("fuse-deep-read").await {
        Some(g) => g,
        None => return,
    };
    let (deep_rel, payload) = common::seed_deep_tree(&guard.hub, "fuse-dr").await;

    let mount_point = format!("/tmp/hf-mount-dr-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-dr-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let op = common::fs_tests::assert_deep_read_and_intermediate_readdir(&mount_point, &deep_rel, payload);
        common::unmount(&mount_point, child, 30);
        op
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("deep cold read test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// Test HEAD revalidation: remote file changes are detected via lookup() HEAD
/// A directory uploaded remotely after the parent's listing was cached
/// (poll disabled, so no background refresh) must still be discoverable
/// via a path lookup. Exercises the FastResult::Miss → list_tree probe
/// path against a real bucket.
#[tokio::test]
async fn test_fuse_remote_dir_appears_after_listing() {
    let guard = match common::setup_bucket_with_file("fuse-new-dir", "initial.txt", b"initial content").await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-fuse-new-dir-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-new-dir-cache-{}", std::process::id());

    let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Cache the root listing (and prove the initial file is visible).
        let entries: Vec<String> = std::fs::read_dir(&mount_point)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(entries.contains(&"initial.txt".to_string()));
        assert!(!entries.contains(&"newdir".to_string()));
        Ok::<_, std::io::Error>(())
    }));

    if let Ok(Err(e)) = &result {
        common::unmount(&mount_point, child, 30);
        panic!("initial readdir failed: {}", e);
    }

    // Add a directory remotely (a file under a previously-absent prefix).
    let write_config = common::build_write_config(&guard.hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-newdir-stage-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging = tmp_dir.join("payload.txt");
    std::fs::write(&staging, b"new content").unwrap();
    let info = common::upload_file(write_config, &staging).await;
    guard
        .hub
        .batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
            path: "newdir/payload.txt".to_string(),
            xet_hash: info.hash().to_string(),
            mtime: 0,
            content_type: None,
        }])
        .await
        .expect("batch add failed");
    std::fs::remove_dir_all(&tmp_dir).ok();

    let probe = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // `ls newdir/` triggers lookup(parent=root, name="newdir"). The Miss
        // arm probes via list_tree, finds entries, and inserts the dir.
        let listed: Vec<String> = std::fs::read_dir(format!("{}/newdir", mount_point))?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(listed, vec!["payload.txt".to_string()]);
        let content = std::fs::read_to_string(format!("{}/newdir/payload.txt", mount_point))?;
        assert_eq!(content, "new content");
        Ok::<_, std::io::Error>(())
    }));

    common::unmount(&mount_point, child, 30);
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match probe {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("post-add probe failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// and the kernel page cache is invalidated so re-reads return fresh content.
#[tokio::test]
async fn test_fuse_revalidation() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let guard = match common::setup_bucket_with_file("fuse-reval", &remote_file, test_content.as_bytes()).await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-fuse-reval-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-reval-cache-{}", std::process::id());

    let child = common::mount_bucket(
        &guard.bucket_id,
        &mount_point,
        &cache_dir,
        &["--metadata-ttl-ms", "100"],
    );

    let result = common::fs_tests::run_revalidation_test(
        &mount_point,
        &remote_file,
        &test_content,
        &guard.hub,
        100, // metadata_ttl_ms
    )
    .await;

    common::unmount(&mount_point, child, 30);
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    if let Err(e) = result {
        panic!("FUSE revalidation test failed: {}", e);
    }
}

/// Many cold sequential readers at once, more readers than FUSE worker
/// threads, files larger than the initial prefetch window so every stream
/// outlives its first read. Regression test for #234: streams must not
/// starve each other for download buffer, and no read may time out or fail.
#[tokio::test]
async fn test_fuse_parallel_cold_reads() {
    const FILE_COUNT: usize = 24;
    const CHUNK: usize = 1 << 20;
    const FILE_SIZE: usize = 64 * CHUNK;

    let guard = match common::setup_bucket("fuse-par-read").await {
        Some(g) => g,
        None => return,
    };
    let rel_paths = common::seed_parallel_read_files(&guard.hub, "fuse-pr", FILE_COUNT, FILE_SIZE).await;

    let mount_point = format!("/tmp/hf-mount-pr-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-pr-cache-{}", std::process::id());
    let log_path = format!("/tmp/hf-mount-pr-log-{}.txt", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket_logged(
            &guard.bucket_id,
            &mount_point,
            &cache_dir,
            // Few workers and a short fetch timeout so a wedge shows up as
            // EIO within seconds instead of minutes.
            &["--read-only", "--max-threads", "4", "--read-fetch-timeout-ms", "5000"],
            &log_path,
        );

        let started = std::time::Instant::now();
        let readers: Vec<_> = rel_paths
            .iter()
            .enumerate()
            .map(|(i, rel)| {
                let path = format!("{}/{}", mount_point, rel);
                std::thread::spawn(move || -> Result<(), String> {
                    use std::io::Read;
                    let mut file = std::fs::File::open(&path).map_err(|e| format!("{path}: open: {e}"))?;
                    let mut expected_gen = common::PseudoRandomBytes::new(i as u64);
                    let mut got = vec![0u8; CHUNK];
                    let mut expected = vec![0u8; CHUNK];
                    for chunk in 0..FILE_SIZE / CHUNK {
                        file.read_exact(&mut got)
                            .map_err(|e| format!("{path}: read at {}: {e}", chunk * CHUNK))?;
                        expected_gen.fill(&mut expected);
                        if got != expected {
                            return Err(format!("{path}: content mismatch at {}", chunk * CHUNK));
                        }
                    }
                    if file.read(&mut got).map_err(|e| format!("{path}: read at eof: {e}"))? != 0 {
                        return Err(format!("{path}: longer than {FILE_SIZE}"));
                    }
                    Ok(())
                })
            })
            .collect();

        let mut errors = Vec::new();
        for reader in readers {
            match reader.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => errors.push(e),
                Err(_) => errors.push("reader thread panicked".to_string()),
            }
        }
        eprintln!(
            "{} readers x {} MiB done in {:?}",
            FILE_COUNT,
            FILE_SIZE / CHUNK,
            started.elapsed()
        );

        common::unmount(&mount_point, child, 30);

        // A single timeout is retried and would otherwise be invisible.
        let log_text = std::fs::read_to_string(&log_path).unwrap_or_default();
        let timeouts = log_text.matches("stream read timed out").count();
        if !errors.is_empty() || timeouts > 0 {
            let log_tail: Vec<&str> = log_text.lines().rev().take(20).collect();
            return Err(format!(
                "{} read errors, {} stream timeouts\nerrors: {:#?}\ndaemon log tail:\n{}",
                errors.len(),
                timeouts,
                errors,
                log_tail.into_iter().rev().collect::<Vec<_>>().join("\n"),
            ));
        }
        Ok(())
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();
    std::fs::remove_file(&log_path).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("parallel cold read test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}
