mod common;

use std::fs::{self, OpenOptions};
use std::io::Error;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn ensure(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(Error::other(message).into())
    }
}

fn run_repo_overlay_tests(mount_point: &str) -> TestResult {
    eprintln!("  [overlay] readdir");
    let entries: Vec<String> = fs::read_dir(mount_point)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect();
    eprintln!("  [overlay] root entries: {:?}", entries);

    ensure(
        entries.iter().any(|entry| entry == "config.json"),
        "remote repo file missing from overlay mount",
    )?;
    ensure(
        entries.iter().any(|entry| entry == "local_preexisting.txt"),
        "pre-existing local file missing from overlay mount",
    )?;

    eprintln!("  [overlay] verify pre-existing local file");
    let local_before = fs::read_to_string(format!("{}/local_preexisting.txt", mount_point))?;
    ensure(
        local_before == "local before mount",
        "pre-existing local file content changed after mount",
    )?;
    eprintln!("  [overlay] local_preexisting.txt: {} bytes", local_before.len());

    eprintln!("  [overlay] read config.json (remote)");
    let config = fs::read_to_string(format!("{}/config.json", mount_point))?;
    ensure(
        config.contains("\"model_type\""),
        "remote repo file did not contain expected metadata",
    )?;
    eprintln!("  [overlay] config.json: {} bytes", config.len());

    // NFS does not expose a distinct open-for-write check the same way FUSE does,
    // so assert immutability through an actual truncation attempt instead.
    eprintln!("  [overlay] verify remote file remains immutable");
    let remote_truncate = OpenOptions::new()
        .write(true)
        .open(format!("{}/config.json", mount_point))
        .and_then(|file| file.set_len(0));
    ensure(
        remote_truncate.is_err(),
        "clean remote files should reject truncation in overlay mode",
    )?;
    ensure(
        fs::read_to_string(format!("{}/config.json", mount_point))? == config,
        "remote repo file changed after failed truncation attempt",
    )?;
    eprintln!("  [overlay] remote truncate correctly rejected");

    eprintln!("  [overlay] create new local file");
    let new_local_path = format!("{}/new_local.txt", mount_point);
    fs::write(&new_local_path, "local after mount")?;
    ensure(
        fs::read_to_string(&new_local_path)? == "local after mount",
        "new local overlay file was not readable after write",
    )?;
    eprintln!("  [overlay] new_local.txt written and readable");

    eprintln!("  [overlay] all passed");

    Ok(())
}

#[test]
fn test_repo_overlay_local_persistence() {
    let binary = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("hf-mount-nfs");
    if !binary.exists() {
        eprintln!("Skipping: hf-mount-nfs binary not found");
        return;
    }

    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-mount-repo-overlay-nfs-{}", pid);
    let cache_dir = format!("/tmp/hf-mount-repo-overlay-nfs-cache-{}", pid);

    fs::create_dir_all(&mount_point).ok();
    fs::write(format!("{}/local_preexisting.txt", mount_point), "local before mount").unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_repo_nfs("openai-community/gpt2", &mount_point, &cache_dir, &["--overlay"]);
        let test_result = run_repo_overlay_tests(&mount_point);
        common::unmount_nfs(&mount_point, child, 30);
        test_result
    }));

    let local_after = fs::read_to_string(format!("{}/local_preexisting.txt", mount_point));
    let new_local_after = fs::read_to_string(format!("{}/new_local.txt", mount_point));
    let remote_after = fs::metadata(format!("{}/config.json", mount_point));

    fs::remove_dir_all(&mount_point).ok();
    fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("All overlay repo tests passed!"),
        Ok(Err(error)) => panic!("Overlay repo integration test failed: {}", error),
        Err(error) => std::panic::resume_unwind(error),
    }

    assert_eq!(local_after.unwrap(), "local before mount");
    assert_eq!(new_local_after.unwrap(), "local after mount");
    assert!(
        remote_after.is_err(),
        "remote repo files should disappear after unmount"
    );
}
