#![allow(dead_code)]

pub mod bench;
pub mod fs_tests;

use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use data::{FileUploadSession, XetFileInfo};
use reqwest::Client;

pub const ENDPOINT: &str = "https://huggingface.co";

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

    let username = whoami(ENDPOINT, &token).await;
    let bucket_id = format!("{}/hf-mount-{}-{}", username, test_name, std::process::id());

    create_bucket(ENDPOINT, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = hf_mount::hub_api::HubApiClient::new(ENDPOINT, &token, &bucket_id);
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

    let files = vec![(staged_path.to_path_buf(), None::<mdb_shard::Sha256>)];
    let mut results = upload_session.upload_files(files).await.expect("upload_files failed");

    let file_info = results.pop().expect("upload returned no file info");

    upload_session.finalize().await.expect("finalize failed");

    file_info
}

/// Spawn hf-mount as a child process, wait until the mountpoint is live.
/// `extra_args` are appended to the command (e.g. `&["--read-only"]`).
pub fn mount_bucket(bucket_id: &str, mount_point: &str, cache_dir: &str, extra_args: &[&str]) -> Child {
    let token = std::env::var("HF_TOKEN").unwrap();

    let binary = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("hf-mount");

    eprintln!("Mounting with binary: {:?}", binary);

    std::fs::create_dir_all(mount_point).ok();
    std::fs::create_dir_all(cache_dir).ok();

    let child = Command::new(binary)
        .args([
            "--bucket-id",
            bucket_id,
            "--mount-point",
            mount_point,
            "--hf-token",
            &token,
            "--cache-dir",
            cache_dir,
            "--poll-interval-secs",
            "0",
        ])
        .args(extra_args)
        .spawn()
        .expect("Failed to spawn hf-mount");

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

// ── mount-s3 reference benchmark helpers ─────────────────────────────

/// Check if mount-s3 and AWS credentials are available.
pub fn s3_bench_available() -> bool {
    Command::new("mount-s3").arg("--version").output().is_ok() && std::env::var("AWS_ACCESS_KEY_ID").is_ok()
}

/// Create an S3 bucket, upload a pattern file, return the bucket name.
/// Caller must call `cleanup_s3_bench` after use.
pub fn setup_s3_bench(data: &[u8]) -> String {
    let bucket = format!("hf-mount-bench-s3-{}", std::process::id());

    let status = Command::new("aws")
        .args(["s3", "mb", &format!("s3://{bucket}"), "--region", "us-east-1"])
        .status()
        .expect("aws s3 mb failed");
    assert!(status.success(), "failed to create S3 bucket");

    let tmp_path = std::env::temp_dir().join("s3_bench_file.bin");
    std::fs::write(&tmp_path, data).expect("write s3 bench file");

    let status = Command::new("aws")
        .args([
            "s3",
            "cp",
            tmp_path.to_str().unwrap(),
            &format!("s3://{bucket}/bench_file.bin"),
        ])
        .status()
        .expect("aws s3 cp failed");
    assert!(status.success(), "failed to upload to S3");

    std::fs::remove_file(&tmp_path).ok();
    eprintln!("Created S3 bucket: {bucket}");
    bucket
}

/// Mount an S3 bucket with mount-s3 (read-only). Returns the child process.
pub fn mount_s3(bucket: &str, mount_point: &str) -> Child {
    std::fs::create_dir_all(mount_point).ok();

    let child = Command::new("mount-s3")
        .args(["--read-only", bucket, mount_point])
        .spawn()
        .expect("failed to spawn mount-s3");

    for i in 0..30 {
        std::thread::sleep(Duration::from_millis(500));
        if let Ok(mounts) = std::fs::read_to_string("/proc/mounts")
            && mounts.lines().any(|line| line.contains(mount_point))
        {
            eprintln!("mount-s3 ready after {}ms", (i + 1) * 500);
            return child;
        }
    }
    eprintln!("Warning: mount-s3 may not be ready after 15s");
    child
}

/// Unmount mount-s3 and clean up the S3 bucket.
pub fn cleanup_s3_bench(mount_point: &str, child: Child, bucket: &str) {
    unmount_with(mount_point, child, 5, &["fusermount", "-u"]);
    std::fs::remove_dir_all(mount_point).ok();

    let _ = Command::new("aws")
        .args(["s3", "rb", "--force", &format!("s3://{bucket}")])
        .status();
    eprintln!("Cleaned up S3 bucket: {bucket}");
}
