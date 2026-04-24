//! Integration tests that verify the Hub API behaves the way the VFS slow
//! path assumes:
//!
//! - `head_file(file)` → `Ok(Some(HeadFileInfo))` with a populated `size`.
//! - `head_file(directory)` → `Ok(None)` so the VFS can fall back to
//!   `list_tree` for directory resolution.
//! - `head_file(missing)` → `Ok(None)` so a negative lookup is distinguishable
//!   from a transient error.
//! - `list_tree(dir)` → non-recursive direct children (dirs + files).
//!
//! These run against the production Hub using `HF_TOKEN`. They are skipped
//! silently when the token is missing (same pattern as the FUSE/NFS tests).

mod common;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hf_mount::hub_api::BatchOp;

/// Upload a single file inline into an existing bucket under `hub_path`.
async fn upload_into(
    hub: &Arc<hf_mount::hub_api::HubApiClient>,
    test_name: &str,
    hub_path: &str,
    content: &[u8],
) {
    let write_config = common::build_write_config(hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-{}-{}", test_name, std::process::id()));
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging = tmp_dir.join(hub_path.replace('/', "_"));
    std::fs::write(&staging, content).expect("write staging");
    let info = common::upload_file(write_config, &staging).await;

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(&[BatchOp::AddFile {
        path: hub_path.to_string(),
        xet_hash: info.hash().to_string(),
        mtime: mtime_ms,
        content_type: None,
    }])
    .await
    .expect("batch add failed");

    std::fs::remove_dir_all(&tmp_dir).ok();
}

/// head_file on a file returns HeadFileInfo with a valid size.
#[tokio::test]
async fn head_file_returns_size_for_file() {
    let content = b"hello hub";
    let guard = match common::setup_bucket_with_file("hub-api-head-file", "root.txt", content).await {
        Some(g) => g,
        None => return,
    };

    let head = guard.hub.head_file("root.txt").await.expect("head_file failed");
    let info = head.expect("expected Some(HeadFileInfo) for an existing file");

    assert_eq!(
        info.size,
        Some(content.len() as u64),
        "HEAD must expose size so lookup can insert a correctly-sized inode"
    );
    assert!(
        info.xet_hash.is_some() || info.etag.is_some(),
        "HEAD should return at least one content identifier (xet_hash or etag)"
    );
}

/// head_file on a directory path returns Ok(None). The VFS relies on this
/// to fall back to list_tree for directory resolution.
#[tokio::test]
async fn head_file_returns_none_for_directory() {
    let guard = match common::setup_bucket("hub-api-head-dir").await {
        Some(g) => g,
        None => return,
    };
    upload_into(&guard.hub, "hub-api-head-dir", "subdir/child.txt", b"ok").await;

    let head = guard
        .hub
        .head_file("subdir")
        .await
        .expect("head_file must not error for a directory path");
    assert!(
        head.is_none(),
        "HEAD on a directory must return Ok(None); got {:?}",
        head
    );
}

/// head_file on a nonexistent path returns Ok(None) (not an Err).
#[tokio::test]
async fn head_file_returns_none_for_missing_path() {
    let guard = match common::setup_bucket("hub-api-head-missing").await {
        Some(g) => g,
        None => return,
    };

    let head = guard
        .hub
        .head_file("does_not_exist.txt")
        .await
        .expect("head_file must not error for a missing file");
    assert!(head.is_none(), "HEAD on a missing path must return Ok(None)");
}

/// list_tree on a subdir returns only its direct children.
#[tokio::test]
async fn list_tree_returns_direct_children_only() {
    let guard = match common::setup_bucket("hub-api-list-tree").await {
        Some(g) => g,
        None => return,
    };
    upload_into(&guard.hub, "hub-api-list-tree", "a/1.txt", b"1").await;
    upload_into(&guard.hub, "hub-api-list-tree", "a/2.txt", b"2").await;
    upload_into(&guard.hub, "hub-api-list-tree", "a/b/deep.txt", b"3").await;

    let root = guard.hub.list_tree("").await.expect("list_tree root failed");
    let root_names: Vec<_> = root.iter().map(|e| e.path.as_str()).collect();
    assert!(root_names.contains(&"a"), "root should contain 'a', got {:?}", root_names);

    let a = guard.hub.list_tree("a").await.expect("list_tree a failed");
    let a_paths: Vec<_> = a.iter().map(|e| (e.path.as_str(), e.entry_type.as_str())).collect();

    // Direct children only: the two files and the subdir `b`, not `a/b/deep.txt`.
    assert!(a_paths.iter().any(|(p, _)| *p == "a/1.txt"), "missing a/1.txt in {:?}", a_paths);
    assert!(a_paths.iter().any(|(p, _)| *p == "a/2.txt"), "missing a/2.txt in {:?}", a_paths);
    assert!(
        a_paths.iter().any(|(p, t)| *p == "a/b" && *t == "directory"),
        "missing a/b (directory) in {:?}",
        a_paths
    );
    assert!(
        !a_paths.iter().any(|(p, _)| *p == "a/b/deep.txt"),
        "list_tree must be non-recursive; unexpected a/b/deep.txt in {:?}",
        a_paths
    );
}
