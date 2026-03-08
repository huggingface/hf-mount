#![allow(dead_code)]

pub mod bench;
pub mod fs_tests;

use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use data::{FileUploadSession, XetFileInfo};
use reqwest::Client;

pub fn endpoint() -> String {
    std::env::var("HF_ENDPOINT").unwrap_or_else(|_| "https://huggingface.co".to_string())
}

/// Create a bucket and return (token, bucket_id, hub). Returns None if HF_TOKEN not set.
/// Use this for multi-file setups (e.g. fio benchmarks) where you upload files yourself.
pub async fn setup_bucket(test_name: &str) -> Option<(String, String, Arc<hf_mount::hub_api::HubApiClient>)> {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return None;
        }
    };

    let ep = endpoint();
    let username = whoami(&ep, &token).await;
    let bucket_id = format!("{}/hf-mount-{}-{}", username, test_name, std::process::id());

    create_bucket(&ep, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = hf_mount::hub_api::HubApiClient::new(&ep, Some(&token), &bucket_id);
    Some((token, bucket_id, hub))
}

/// Create a bucket, upload a single file, return (token, bucket_id, hub).
/// For multi-file setups, use `setup_bucket` + `upload_file` directly.
pub async fn setup_bucket_with_file(
    test_name: &str,
    filename: &str,
    content: &[u8],
) -> Option<(String, String, Arc<hf_mount::hub_api::HubApiClient>)> {
    let (token, bucket_id, hub) = setup_bucket(test_name).await?;
    let write_config = build_write_config(&hub).await;

    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-{}-setup", test_name));
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging_path = tmp_dir.join(filename);
    std::fs::write(&staging_path, content).expect("write staging file");

    let file_info = upload_file(write_config, &staging_path).await;
    let xet_hash = file_info.hash().to_string();
    eprintln!("Uploaded: xet_hash={}, size={}", xet_hash, file_info.file_size());

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
        path: filename.to_string(),
        xet_hash,
        mtime: mtime_ms,
        content_type: None,
    }])
    .await
    .expect("batch add failed");

    std::fs::remove_dir_all(&tmp_dir).ok();

    Some((token, bucket_id, hub))
}

/// Create a bucket on the Hub. Ignores 409 (already exists).
pub async fn create_bucket(endpoint: &str, token: &str, bucket_id: &str) {
    let resp = Client::new()
        .post(format!("{}/api/buckets/{}", endpoint, bucket_id))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create_bucket request failed");

    if resp.status() != reqwest::StatusCode::CONFLICT && !resp.status().is_success() {
        panic!(
            "create_bucket failed: {} {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
}

/// Delete a bucket from the Hub.
pub async fn delete_bucket(endpoint: &str, token: &str, bucket_id: &str) {
    match Client::new()
        .delete(format!("{}/api/buckets/{}", endpoint, bucket_id))
        .bearer_auth(token)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            eprintln!("Cleaned up bucket: {}", bucket_id);
        }
        Ok(resp) => {
            eprintln!(
                "Warning: failed to delete bucket {}: {} {}",
                bucket_id,
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        Err(e) => {
            eprintln!("Warning: failed to delete bucket {}: {}", bucket_id, e);
        }
    }
}

/// Get the username for the current token.
pub async fn whoami(endpoint: &str, token: &str) -> String {
    let resp = Client::new()
        .get(format!("{}/api/whoami-v2", endpoint))
        .bearer_auth(token)
        .send()
        .await
        .expect("whoami request failed");

    assert!(resp.status().is_success(), "whoami failed: {}", resp.status());

    let body: serde_json::Value = resp.json().await.expect("whoami json parse failed");
    body["name"].as_str().expect("whoami: missing 'name' field").to_string()
}

/// Build an Arc<TranslatorConfig> for CAS writes.
pub async fn build_write_config(
    hub: &Arc<hf_mount::hub_api::HubApiClient>,
) -> Arc<data::configurations::TranslatorConfig> {
    let write_jwt = hub.get_cas_write_token().await.expect("get_cas_write_token failed");

    let write_refresher = hub.token_refresher(false);

    Arc::new(
        data::data_client::default_config(
            write_jwt.cas_url,
            None,
            Some((write_jwt.access_token, write_jwt.exp)),
            Some(write_refresher),
            None,
        )
        .expect("write default_config failed"),
    )
}

/// Upload a single file to CAS via an upload session.
pub async fn upload_file(
    config: std::sync::Arc<data::configurations::TranslatorConfig>,
    staged_path: &Path,
) -> XetFileInfo {
    let upload_session = FileUploadSession::new(config, None)
        .await
        .expect("FileUploadSession::new failed");

    let files = vec![(staged_path.to_path_buf(), None::<mdb_shard::Sha256>, ulid::Ulid::new())];
    let mut results = upload_session.upload_files(files).await.expect("upload_files failed");

    let file_info = results.pop().expect("upload returned no file info");

    upload_session.finalize().await.expect("finalize failed");

    file_info
}

/// Spawn hf-mount-fuse as a child process, wait until the mountpoint is live.
/// `extra_args` are appended to the command (e.g. `&["--read-only"]`).
pub fn mount_bucket(bucket_id: &str, mount_point: &str, cache_dir: &str, extra_args: &[&str]) -> Child {
    let token = std::env::var("HF_TOKEN").unwrap();

    let binary = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("hf-mount-fuse");

    eprintln!("Mounting with binary: {:?}", binary);

    std::fs::create_dir_all(mount_point).ok();
    std::fs::create_dir_all(cache_dir).ok();

    let ep = endpoint();
    let child = Command::new(binary)
        .args([
            "--hf-token",
            &token,
            "--hub-endpoint",
            &ep,
            "--cache-dir",
            cache_dir,
            "--poll-interval-secs",
            "0",
        ])
        .args(extra_args)
        .args(["bucket", bucket_id, mount_point])
        .spawn()
        .expect("Failed to spawn hf-mount-fuse");

    for i in 0..30 {
        std::thread::sleep(Duration::from_millis(500));
        if let Ok(mounts) = std::fs::read_to_string("/proc/mounts")
            && mounts.lines().any(|line| line.contains(mount_point))
        {
            eprintln!("Mount ready after {}ms", (i + 1) * 500);
            return child;
        }
    }

    eprintln!("Warning: mount may not be ready after 15s");
    child
}

/// Spawn hf-mount-fuse to mount a repo as read-only, wait until the mountpoint is live.
pub fn mount_repo(repo_id: &str, mount_point: &str, cache_dir: &str, extra_args: &[&str]) -> Child {
    let token = std::env::var("HF_TOKEN").ok();

    let binary = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("hf-mount-fuse");

    eprintln!("Mounting repo with binary: {:?}", binary);

    std::fs::create_dir_all(mount_point).ok();
    std::fs::create_dir_all(cache_dir).ok();

    let ep = endpoint();
    let mut cmd = Command::new(binary);
    if let Some(ref t) = token {
        cmd.args(["--hf-token", t]);
    }
    let child = cmd
        .args([
            "--hub-endpoint",
            &ep,
            "--cache-dir",
            cache_dir,
            "--poll-interval-secs",
            "0",
        ])
        .args(extra_args)
        .args(["repo", repo_id, mount_point])
        .spawn()
        .expect("Failed to spawn hf-mount-fuse");

    for i in 0..30 {
        std::thread::sleep(Duration::from_millis(500));
        if let Ok(mounts) = std::fs::read_to_string("/proc/mounts")
            && mounts.lines().any(|line| line.contains(mount_point))
        {
            eprintln!("Mount ready after {}ms", (i + 1) * 500);
            return child;
        }
    }

    eprintln!("Warning: mount may not be ready after 15s");
    child
}

/// Spawn hf-mount-nfs to mount a bucket via NFS.
pub fn mount_bucket_nfs(bucket_id: &str, mount_point: &str, cache_dir: &str, extra_args: &[&str]) -> Child {
    let token = std::env::var("HF_TOKEN").unwrap();

    let binary = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("hf-mount-nfs");

    eprintln!("Mounting NFS with binary: {:?}", binary);

    if !binary.exists() {
        panic!("hf-mount-nfs binary not found, run cargo build --release first");
    }

    std::fs::create_dir_all(mount_point).ok();
    std::fs::create_dir_all(cache_dir).ok();

    let ep = endpoint();
    let child = Command::new(binary)
        .args([
            "--hf-token",
            &token,
            "--hub-endpoint",
            &ep,
            "--cache-dir",
            cache_dir,
            "--poll-interval-secs",
            "0",
        ])
        .args(extra_args)
        .args(["bucket", bucket_id, mount_point])
        .spawn()
        .expect("Failed to spawn hf-mount-nfs");

    for i in 0..30 {
        std::thread::sleep(Duration::from_millis(500));
        if let Ok(mounts) = std::fs::read_to_string("/proc/mounts")
            && mounts.lines().any(|line| line.contains(mount_point))
        {
            eprintln!("Mount ready after {}ms", (i + 1) * 500);
            return child;
        }
    }

    eprintln!("Warning: mount may not be ready after 15s");
    child
}

/// Unmount FUSE and wait for hf-mount to exit. Waits up to `graceful_secs`
/// for a clean exit (destroy() may flush + upload) before force-killing.
pub fn unmount(mount_point: &str, child: Child, graceful_secs: u64) {
    unmount_with(mount_point, child, graceful_secs, &["fusermount", "-u"]);
}

/// Unmount NFS and wait for hf-mount to exit.
pub fn unmount_nfs(mount_point: &str, child: Child, graceful_secs: u64) {
    unmount_with(mount_point, child, graceful_secs, &["sudo", "umount"]);
}

fn unmount_with(mount_point: &str, mut child: Child, graceful_secs: u64, cmd: &[&str]) {
    match Command::new(cmd[0]).args(&cmd[1..]).arg(mount_point).status() {
        Ok(s) if !s.success() => eprintln!("Warning: unmount command exited with {}", s),
        Err(e) => eprintln!("Warning: unmount command failed: {}", e),
        _ => {}
    }

    for _ in 0..graceful_secs {
        if let Ok(Some(status)) = child.try_wait() {
            eprintln!("hf-mount exited: {}", status);
            return;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    child.kill().ok();
    match child.wait() {
        Ok(status) => eprintln!("hf-mount killed: {}", status),
        Err(e) => eprintln!("wait error: {}", e),
    }
}

/// Build test content with recognizable header/middle/footer and padding to 4 KB.
/// Layout: "AAAA_HEADER_AAAA|BBBB_MIDDLE_BBBB|CCCC_FOOTER_CCCC|" + 'X' padding + "END"
pub fn test_content() -> String {
    let prefix = "AAAA_HEADER_AAAA|BBBB_MIDDLE_BBBB|CCCC_FOOTER_CCCC|";
    let suffix = "END";
    let pad_len = 4096 - prefix.len() - suffix.len();
    format!("{}{}{}", prefix, "X".repeat(pad_len), suffix)
}

/// Generate deterministic content: byte[i] = (i % 251) as u8
pub fn generate_pattern(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Verify content matches the deterministic pattern at a given offset.
pub fn verify_pattern(data: &[u8], offset: usize) -> bool {
    data.iter().enumerate().all(|(i, &b)| b == ((offset + i) % 251) as u8)
}
