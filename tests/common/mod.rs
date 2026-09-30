#![allow(dead_code)]

pub mod bench;
pub mod fs_tests;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::Client;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::data_client::default_config;
use xet_data::processing::{FileUploadSession, Sha256Policy, XetFileInfo};
use xet_runtime::core::XetContext;

pub fn endpoint() -> String {
    std::env::var("HF_ENDPOINT").unwrap_or_else(|_| "https://huggingface.co".to_string())
}

/// RAII guard that deletes the bucket when dropped. Cleanup runs even if the test
/// panics, so we don't leak buckets on production Hub.
pub struct BucketGuard {
    pub bucket_id: String,
    pub hub: Arc<hf_mount::hub_api::HubApiClient>,
    token: String,
    endpoint: String,
}

impl Drop for BucketGuard {
    fn drop(&mut self) {
        let endpoint = self.endpoint.clone();
        let token = self.token.clone();
        let bucket_id = self.bucket_id.clone();
        // Drop may run from a tokio worker; spawn a thread with a fresh runtime
        // so we can block_on the async delete without nested-runtime panics.
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build cleanup runtime");
            rt.block_on(delete_bucket(&endpoint, &token, &bucket_id));
        })
        .join();
    }
}

/// Create a bucket and return a guard that auto-deletes on drop.
/// Returns None if HF_TOKEN not set.
/// Use this for multi-file setups (e.g. fio benchmarks) where you upload files yourself.
pub async fn setup_bucket(test_name: &str) -> Option<BucketGuard> {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return None;
        }
    };

    let ep = endpoint();
    // whoami also validates the token. Bucket namespace defaults to the token
    // owner but is overridable (HF_BUCKET_NAMESPACE) so CI creates scratch buckets
    // under a shared org (infra-workloads), not a maintainer's personal account.
    let username = whoami(&ep, &token).await;
    let namespace = std::env::var("HF_BUCKET_NAMESPACE").unwrap_or(username);
    let bucket_id = format!("{}/hf-mount-{}-{}", namespace, test_name, std::process::id());

    create_bucket(&ep, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = hf_mount::hub_api::HubApiClient::new(&ep, Some(&token), &bucket_id, "test");
    Some(BucketGuard {
        token,
        bucket_id,
        hub,
        endpoint: ep,
    })
}

/// Create a bucket, upload a single file, return a guard that auto-deletes on drop.
/// For multi-file setups, use `setup_bucket` + `upload_file` directly.
pub async fn setup_bucket_with_file(test_name: &str, filename: &str, content: &[u8]) -> Option<BucketGuard> {
    let guard = setup_bucket(test_name).await?;
    let write_config = build_write_config(&guard.hub).await;

    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-{}-setup", test_name));
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging_path = tmp_dir.join(filename);
    std::fs::write(&staging_path, content).expect("write staging file");

    let file_info = upload_file(write_config, &staging_path).await;
    let xet_hash = file_info.hash().to_string();
    eprintln!(
        "Uploaded: xet_hash={}, size={}",
        xet_hash,
        file_info.file_size().expect("upload returned XetFileInfo without size")
    );

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    guard
        .hub
        .batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
            path: filename.to_string(),
            xet_hash,
            mtime: mtime_ms,
            content_type: None,
        }])
        .await
        .expect("batch add failed");

    std::fs::remove_dir_all(&tmp_dir).ok();

    Some(guard)
}

/// Seed a bucket with `big_dir/sib_NN.txt` (20 siblings) + `big_dir/target.txt`.
/// Returns the target's relative path under the mount + its expected content.
/// Used by the point-lookup integration tests to exercise the HEAD-based
/// slow path against a populated sibling set.
pub async fn seed_big_dir_with_target(
    hub: &Arc<hf_mount::hub_api::HubApiClient>,
    tmp_dir_tag: &str,
) -> (String, &'static [u8]) {
    const TARGET_CONTENT: &[u8] = b"hello from the target";
    let write_config = build_write_config(hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-{}-{}", tmp_dir_tag, std::process::id()));
    std::fs::create_dir_all(&tmp_dir).ok();

    let mut ops = Vec::with_capacity(21);
    for i in 0..20 {
        let path = tmp_dir.join(format!("sib_{i:02}.txt"));
        std::fs::write(&path, format!("sib_{i:02}")).unwrap();
        let info = upload_file(write_config.clone(), &path).await;
        ops.push(hf_mount::hub_api::BatchOp::AddFile {
            path: format!("big_dir/sib_{i:02}.txt"),
            xet_hash: info.hash().to_string(),
            mtime: 0,
            content_type: None,
        });
    }
    let target_path = tmp_dir.join("target.txt");
    std::fs::write(&target_path, TARGET_CONTENT).unwrap();
    let info = upload_file(write_config, &target_path).await;
    ops.push(hf_mount::hub_api::BatchOp::AddFile {
        path: "big_dir/target.txt".to_string(),
        xet_hash: info.hash().to_string(),
        mtime: 0,
        content_type: None,
    });
    hub.batch_operations(&ops).await.expect("batch add failed");
    std::fs::remove_dir_all(&tmp_dir).ok();
    ("big_dir/target.txt".to_string(), TARGET_CONTENT)
}

/// Seed a bucket with a single deep file `a/b/c/d/payload.txt`.
/// Returns the relative path + payload for cold-read integration tests.
pub async fn seed_deep_tree(hub: &Arc<hf_mount::hub_api::HubApiClient>, tmp_dir_tag: &str) -> (String, &'static [u8]) {
    const PAYLOAD: &[u8] = b"deep payload";
    let write_config = build_write_config(hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-{}-{}", tmp_dir_tag, std::process::id()));
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging = tmp_dir.join("payload.txt");
    std::fs::write(&staging, PAYLOAD).unwrap();
    let info = upload_file(write_config, &staging).await;
    hub.batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
        path: "a/b/c/d/payload.txt".to_string(),
        xet_hash: info.hash().to_string(),
        mtime: 0,
        content_type: None,
    }])
    .await
    .expect("batch add failed");
    std::fs::remove_dir_all(&tmp_dir).ok();
    ("a/b/c/d/payload.txt".to_string(), PAYLOAD)
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
    // Bounded timeout so BucketGuard's Drop can't block the test thread
    // indefinitely if the Hub is slow or unreachable.
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build delete_bucket client");
    match client
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
pub async fn build_write_config(hub: &Arc<hf_mount::hub_api::HubApiClient>) -> Arc<TranslatorConfig> {
    let write_jwt = hub.get_cas_write_token().await.expect("get_cas_write_token failed");

    let write_refresher = hub.token_refresher(false);
    let ctx = XetContext::default().expect("XetContext::default failed");

    Arc::new(
        default_config(
            &ctx,
            write_jwt.cas_url,
            Some((write_jwt.access_token, write_jwt.exp)),
            Some(write_refresher),
            None,
        )
        .expect("write default_config failed"),
    )
}

/// Upload files to CAS in one upload session.
pub async fn upload_files(config: Arc<TranslatorConfig>, staged_paths: &[PathBuf]) -> Vec<XetFileInfo> {
    let upload_session = FileUploadSession::new(config)
        .await
        .expect("FileUploadSession::new failed");

    let files = staged_paths.iter().map(|path| (path.clone(), Sha256Policy::Skip));
    let file_infos = upload_session.upload_files(files).await.expect("upload_files failed");

    upload_session.finalize().await.expect("finalize failed");

    file_infos
}

/// Upload a single file to CAS via an upload session.
pub async fn upload_file(config: Arc<TranslatorConfig>, staged_path: &Path) -> XetFileInfo {
    upload_files(config, &[staged_path.to_path_buf()])
        .await
        .pop()
        .expect("upload returned no file info")
}

/// Spawn hf-mount-fuse as a child process, wait until the mountpoint is live.
/// `extra_args` are appended to the command (e.g. `&["--read-only"]`).
pub fn mount_bucket(bucket_id: &str, mount_point: &str, cache_dir: &str, extra_args: &[&str]) -> Child {
    spawn_mount_bucket(
        bucket_id,
        mount_point,
        cache_dir,
        extra_args,
        Stdio::inherit(),
        Stdio::inherit(),
    )
}

/// Like `mount_bucket`, but with the daemon's output written to `log_path`
/// so a test can assert on what it logged. Tracing goes to stdout, so both
/// stdout and stderr are captured.
pub fn mount_bucket_logged(
    bucket_id: &str,
    mount_point: &str,
    cache_dir: &str,
    extra_args: &[&str],
    log_path: &str,
) -> Child {
    let log = std::fs::File::create(log_path).expect("create daemon log file");
    let log_err = log.try_clone().expect("clone daemon log file");
    spawn_mount_bucket(
        bucket_id,
        mount_point,
        cache_dir,
        extra_args,
        log.into(),
        log_err.into(),
    )
}

fn spawn_mount_bucket(
    bucket_id: &str,
    mount_point: &str,
    cache_dir: &str,
    extra_args: &[&str],
    stdout: Stdio,
    stderr: Stdio,
) -> Child {
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
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "hf_mount=warn".to_string()),
        )
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
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .expect("Failed to spawn hf-mount-fuse");

    if wait_for_mount(mount_point) {
        return child;
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

    if wait_for_mount(mount_point) {
        return child;
    }

    eprintln!("Warning: mount may not be ready after 15s");
    child
}

/// Spawn hf-mount-nfs to mount a bucket via NFS.
pub fn mount_bucket_nfs(bucket_id: &str, mount_point: &str, cache_dir: &str, extra_args: &[&str]) -> Child {
    mount_nfs("bucket", bucket_id, mount_point, cache_dir, extra_args)
}

/// Spawn hf-mount-nfs to mount a repo.
pub fn mount_repo_nfs(repo_id: &str, mount_point: &str, cache_dir: &str, extra_args: &[&str]) -> Child {
    mount_nfs("repo", repo_id, mount_point, cache_dir, extra_args)
}

fn mount_nfs(source_kind: &str, source_id: &str, mount_point: &str, cache_dir: &str, extra_args: &[&str]) -> Child {
    let token = std::env::var("HF_TOKEN").ok();
    let binary = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("hf-mount-nfs");

    eprintln!("Mounting NFS with binary: {:?}", binary);

    if !binary.exists() {
        panic!("hf-mount-nfs binary not found, run cargo build --features nfs --bin hf-mount-nfs first");
    }

    std::fs::create_dir_all(mount_point).ok();
    std::fs::create_dir_all(cache_dir).ok();

    let ep = endpoint();
    let mut cmd = Command::new(binary);
    cmd.env(
        "RUST_LOG",
        std::env::var("RUST_LOG").unwrap_or_else(|_| "hf_mount=warn".to_string()),
    );
    if let Some(ref token) = token {
        cmd.args(["--hf-token", token]);
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
        .args([source_kind, source_id, mount_point])
        .spawn()
        .expect("Failed to spawn hf-mount-nfs");

    if wait_for_mount(mount_point) {
        return child;
    }

    eprintln!("Warning: mount may not be ready after 15s");
    child
}

/// Unmount FUSE and wait for hf-mount to exit. Waits up to `graceful_secs`
/// for a clean exit (destroy() may flush + upload) before force-killing.
#[cfg(target_os = "macos")]
pub fn unmount(mount_point: &str, child: Child, graceful_secs: u64) {
    unmount_with(mount_point, child, graceful_secs, &["umount"]);
}

#[cfg(not(target_os = "macos"))]
pub fn unmount(mount_point: &str, child: Child, graceful_secs: u64) {
    unmount_with(mount_point, child, graceful_secs, &["fusermount", "-u"]);
}

/// Unmount NFS and wait for hf-mount to exit.
#[cfg(target_os = "macos")]
pub fn unmount_nfs(mount_point: &str, child: Child, graceful_secs: u64) {
    unmount_with(mount_point, child, graceful_secs, &["umount"]);
}

#[cfg(not(target_os = "macos"))]
pub fn unmount_nfs(mount_point: &str, child: Child, graceful_secs: u64) {
    unmount_with(mount_point, child, graceful_secs, &["sudo", "umount"]);
}

fn wait_for_mount(mount_point: &str) -> bool {
    for i in 0..30 {
        std::thread::sleep(Duration::from_millis(500));
        if is_mounted(mount_point) {
            eprintln!("Mount ready after {}ms", (i + 1) * 500);
            return true;
        }
    }
    false
}

#[cfg(target_os = "linux")]
fn decode_proc_mount_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && bytes[index + 1].is_ascii_digit()
            && bytes[index + 2].is_ascii_digit()
            && bytes[index + 3].is_ascii_digit()
        {
            let octal = &path[index + 1..index + 4];
            if let Ok(value) = u8::from_str_radix(octal, 8) {
                decoded.push(value);
                index += 4;
                continue;
            }
        }

        decoded.push(bytes[index]);
        index += 1;
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

fn is_mounted(mount_point: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        let mount_point = std::fs::canonicalize(mount_point)
            .unwrap_or_else(|_| Path::new(mount_point).to_path_buf())
            .to_string_lossy()
            .into_owned();
        if let Ok(mounts) = std::fs::read_to_string("/proc/mounts")
            && mounts.lines().any(|line| {
                let mut fields = line.split_whitespace();
                let _source = fields.next();
                fields
                    .next()
                    .map(decode_proc_mount_path)
                    .is_some_and(|mounted_on| mounted_on == mount_point)
            })
        {
            return true;
        }
    }

    #[cfg(target_os = "macos")]
    {
        use std::ffi::{CStr, CString};
        use std::mem::MaybeUninit;
        use std::os::unix::ffi::OsStrExt;

        let canonical_path = match std::fs::canonicalize(mount_point) {
            Ok(path) => path,
            Err(_) => return false,
        };
        let c_path = match CString::new(canonical_path.as_os_str().as_bytes()) {
            Ok(path) => path,
            Err(_) => return false,
        };

        unsafe {
            let mut buf = MaybeUninit::<libc::statfs>::uninit();
            if libc::statfs(c_path.as_ptr(), buf.as_mut_ptr()) != 0 {
                return false;
            }

            let buf = buf.assume_init();
            let mounted_on = CStr::from_ptr(buf.f_mntonname.as_ptr()).to_bytes();
            return mounted_on == canonical_path.as_os_str().as_bytes();
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        return Command::new("mount")
            .output()
            .ok()
            .map(|output| {
                let mounts = String::from_utf8_lossy(&output.stdout);
                let needle = format!(" on {} ", mount_point);
                mounts.lines().any(|line| line.contains(&needle))
            })
            .unwrap_or(false);
    }

    #[allow(unreachable_code)]
    false
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

/// Deterministic pseudo-random byte generator (xorshift64). Each seeded file
/// has distinct content so nothing dedups across files, and readers can
/// regenerate the expected bytes chunk by chunk instead of holding them.
pub struct PseudoRandomBytes(u64);

impl PseudoRandomBytes {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            let bytes = x.to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

/// Seed a bucket with `count` files of `size` bytes each under `par/`, all
/// uploaded in one session. Returns the relative paths; file `i` regenerates
/// from `PseudoRandomBytes::new(i)`.
pub async fn seed_parallel_read_files(
    hub: &Arc<hf_mount::hub_api::HubApiClient>,
    tmp_dir_tag: &str,
    count: usize,
    size: usize,
) -> Vec<String> {
    let write_config = build_write_config(hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-{}-{}", tmp_dir_tag, std::process::id()));
    std::fs::create_dir_all(&tmp_dir).ok();

    let mut buf = vec![0u8; size];
    let staged: Vec<_> = (0..count)
        .map(|i| {
            let path = tmp_dir.join(format!("f_{i:02}.bin"));
            PseudoRandomBytes::new(i as u64).fill(&mut buf);
            std::fs::write(&path, &buf).unwrap();
            path
        })
        .collect();

    let infos = upload_files(write_config, &staged).await;
    assert_eq!(infos.len(), count);

    let rel_paths: Vec<String> = (0..count).map(|i| format!("par/f_{i:02}.bin")).collect();
    let ops: Vec<_> = rel_paths
        .iter()
        .zip(&infos)
        .map(|(rel, info)| hf_mount::hub_api::BatchOp::AddFile {
            path: rel.clone(),
            xet_hash: info.hash().to_string(),
            mtime: 0,
            content_type: None,
        })
        .collect();
    hub.batch_operations(&ops).await.expect("batch add failed");
    std::fs::remove_dir_all(&tmp_dir).ok();
    rel_paths
}
