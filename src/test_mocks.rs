//! Mock implementations for unit testing VirtualFs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use data::XetFileInfo;

use crate::error::{Error, Result};
use crate::hub_api::{BatchOp, HeadFileInfo, HubOps, SourceKind, TreeEntry};
use crate::xet::{DownloadStreamOps, StagingDir, StreamingWriterOps, XetOps};

// ── MockHub ───────────────────────────────────────────────────────────

pub struct MockHub {
    tree: Mutex<Vec<TreeEntry>>,
    head_responses: Mutex<HashMap<String, Option<HeadFileInfo>>>,
    pub batch_log: Mutex<Vec<Vec<BatchOp>>>,
    batch_fail_count: AtomicU32,
    batch_barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    head_fail: AtomicBool,
    download_fail: AtomicBool,
    source: SourceKind,
    default_mtime: SystemTime,
}

#[allow(dead_code)]
impl MockHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tree: Mutex::new(Vec::new()),
            head_responses: Mutex::new(HashMap::new()),
            batch_log: Mutex::new(Vec::new()),
            batch_fail_count: AtomicU32::new(0),
            batch_barrier: Mutex::new(None),
            head_fail: AtomicBool::new(false),
            download_fail: AtomicBool::new(false),
            source: SourceKind::Bucket {
                bucket_id: "test-bucket".to_string(),
            },
            default_mtime: UNIX_EPOCH,
        })
    }

    pub fn new_repo() -> Arc<Self> {
        Arc::new(Self {
            tree: Mutex::new(Vec::new()),
            head_responses: Mutex::new(HashMap::new()),
            batch_log: Mutex::new(Vec::new()),
            batch_fail_count: AtomicU32::new(0),
            batch_barrier: Mutex::new(None),
            head_fail: AtomicBool::new(false),
            download_fail: AtomicBool::new(false),
            source: SourceKind::Repo {
                repo_id: "test/repo".to_string(),
                repo_type: crate::hub_api::RepoType::Model,
                revision: "main".to_string(),
            },
            default_mtime: UNIX_EPOCH,
        })
    }

    pub fn add_file(&self, path: &str, size: u64, xet_hash: Option<&str>, oid: Option<&str>) {
        self.tree.lock().unwrap().push(TreeEntry {
            path: path.to_string(),
            entry_type: "file".to_string(),
            size: Some(size),
            xet_hash: xet_hash.map(|s| s.to_string()),
            oid: oid.map(|s| s.to_string()),
            mtime: None,
        });
        if let Some(hash) = xet_hash {
            self.head_responses.lock().unwrap().insert(
                path.to_string(),
                Some(HeadFileInfo {
                    xet_hash: Some(hash.to_string()),
                    etag: oid.map(|s| s.to_string()),
                    size: Some(size),
                    last_modified: None,
                }),
            );
        }
    }

    pub fn add_dir(&self, path: &str) {
        self.tree.lock().unwrap().push(TreeEntry {
            path: path.to_string(),
            entry_type: "directory".to_string(),
            size: None,
            xet_hash: None,
            oid: None,
            mtime: None,
        });
    }

    pub fn remove_file(&self, path: &str) {
        self.tree.lock().unwrap().retain(|e| e.path != path);
        self.head_responses.lock().unwrap().remove(path);
    }

    pub fn set_head(&self, path: &str, info: Option<HeadFileInfo>) {
        self.head_responses.lock().unwrap().insert(path.to_string(), info);
    }

    pub fn fail_next_batch(&self, n: u32) {
        self.batch_fail_count.store(n, Ordering::SeqCst);
    }

    pub fn fail_next_head(&self) {
        self.head_fail.store(true, Ordering::SeqCst);
    }

    pub fn fail_next_download(&self) {
        self.download_fail.store(true, Ordering::SeqCst);
    }

    pub fn set_batch_barrier(&self, barrier: Arc<tokio::sync::Barrier>) {
        *self.batch_barrier.lock().unwrap() = Some(barrier);
    }

    pub fn take_batch_log(&self) -> Vec<Vec<BatchOp>> {
        std::mem::take(&mut *self.batch_log.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl HubOps for MockHub {
    async fn list_tree(&self, prefix: &str, recursive: bool) -> Result<Vec<TreeEntry>> {
        let tree = self.tree.lock().unwrap();
        Ok(tree
            .iter()
            .filter(|e| {
                if recursive {
                    prefix.is_empty() || e.path.starts_with(&format!("{}/", prefix))
                } else if prefix.is_empty() {
                    !e.path.contains('/')
                } else {
                    e.path.starts_with(&format!("{}/", prefix)) && !e.path[prefix.len() + 1..].contains('/')
                }
            })
            .map(|e| TreeEntry {
                path: e.path.clone(),
                entry_type: e.entry_type.clone(),
                size: e.size,
                xet_hash: e.xet_hash.clone(),
                oid: e.oid.clone(),
                mtime: e.mtime.clone(),
            })
            .collect())
    }

    async fn head_file(&self, path: &str) -> Result<Option<HeadFileInfo>> {
        if self.head_fail.swap(false, Ordering::SeqCst) {
            return Err(Error::Hub("mock head_file failure".into()));
        }
        let responses = self.head_responses.lock().unwrap();
        match responses.get(path) {
            Some(info) => Ok(info.as_ref().map(|i| HeadFileInfo {
                xet_hash: i.xet_hash.clone(),
                etag: i.etag.clone(),
                size: i.size,
                last_modified: i.last_modified.clone(),
            })),
            None => Ok(None),
        }
    }

    async fn batch_operations(&self, ops: &[BatchOp]) -> Result<()> {
        // Await barrier if set (for TOCTOU tests)
        let barrier = self.batch_barrier.lock().unwrap().clone();
        if let Some(b) = barrier {
            b.wait().await;
        }

        let prev = self.batch_fail_count.load(Ordering::SeqCst);
        if prev > 0 {
            self.batch_fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::Hub("mock batch_operations failure".into()));
        }
        self.batch_log.lock().unwrap().push(ops.to_vec());
        Ok(())
    }

    async fn download_file_http(&self, _path: &str, dest: &Path) -> Result<()> {
        if self.download_fail.swap(false, Ordering::SeqCst) {
            return Err(Error::Hub("mock download failure".into()));
        }
        // Create an empty file at dest so open_local_readonly can open it.
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(dest, b"").map_err(Error::Io)?;
        Ok(())
    }

    fn default_mtime(&self) -> SystemTime {
        self.default_mtime
    }

    fn source(&self) -> &SourceKind {
        &self.source
    }

    fn is_repo(&self) -> bool {
        matches!(self.source, SourceKind::Repo { .. })
    }
}

// ── MockXet ───────────────────────────────────────────────────────────

pub struct MockXet {
    files: Mutex<HashMap<String, Vec<u8>>>,
    next_hash: AtomicU64,
    writer_create_fail: AtomicBool,
    upload_fail: AtomicBool,
    download_fail: AtomicBool,
    writer_fail_after: AtomicU64,
    /// Number of range download calls that should fail before succeeding.
    range_fail_count: AtomicU32,
    /// Number of range download calls that should return empty before succeeding.
    range_empty_count: AtomicU32,
    /// How many times warm_reconstruction_cache was called.
    pub warm_call_count: AtomicU32,
    /// Optional delay injected into warm_reconstruction_cache (simulates CAS round-trip).
    warm_delay_ms: AtomicU64,
}

impl MockXet {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            files: Mutex::new(HashMap::new()),
            next_hash: AtomicU64::new(1),
            writer_create_fail: AtomicBool::new(false),
            upload_fail: AtomicBool::new(false),
            download_fail: AtomicBool::new(false),
            writer_fail_after: AtomicU64::new(u64::MAX),
            range_fail_count: AtomicU32::new(0),
            range_empty_count: AtomicU32::new(0),
            warm_call_count: AtomicU32::new(0),
            warm_delay_ms: AtomicU64::new(0),
        })
    }

    pub fn set_warm_delay_ms(&self, ms: u64) {
        self.warm_delay_ms.store(ms, Ordering::SeqCst);
    }

    pub fn add_file(&self, hash: &str, content: &[u8]) {
        self.files.lock().unwrap().insert(hash.to_string(), content.to_vec());
    }

    pub fn fail_next_writer_create(&self) {
        self.writer_create_fail.store(true, Ordering::SeqCst);
    }

    pub fn fail_upload(&self) {
        self.upload_fail.store(true, Ordering::SeqCst);
    }

    pub fn fail_writer_after(&self, bytes: u64) {
        self.writer_fail_after.store(bytes, Ordering::SeqCst);
    }

    /// Make the next N range downloads return Err before succeeding.
    pub fn fail_range_downloads(&self, n: u32) {
        self.range_fail_count.store(n, Ordering::SeqCst);
    }

    /// Make the next N range downloads return Ok(empty) before succeeding.
    pub fn empty_range_downloads(&self, n: u32) {
        self.range_empty_count.store(n, Ordering::SeqCst);
    }

    fn next_hash_string(&self) -> String {
        format!("mock_hash_{}", self.next_hash.fetch_add(1, Ordering::SeqCst))
    }
}

#[async_trait::async_trait]
impl XetOps for MockXet {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>> {
        if self.writer_create_fail.swap(false, Ordering::SeqCst) {
            return Err(Error::Xet("mock writer creation failure".into()));
        }
        let fail_after = self.writer_fail_after.swap(u64::MAX, Ordering::SeqCst);
        Ok(Box::new(MockStreamingWriter {
            data: Vec::new(),
            hash: self.next_hash_string(),
            fail_after,
        }))
    }

    async fn download_to_file(&self, xet_hash: &str, _file_size: u64, dest: &Path) -> Result<()> {
        if self.download_fail.swap(false, Ordering::SeqCst) {
            return Err(Error::Xet("mock download failure".into()));
        }
        let files = self.files.lock().unwrap();
        if let Some(content) = files.get(xet_hash) {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::write(dest, content).map_err(Error::Io)?;
        }
        Ok(())
    }

    async fn upload_files(&self, paths: &[&Path]) -> Result<Vec<XetFileInfo>> {
        if self.upload_fail.swap(false, Ordering::SeqCst) {
            return Err(Error::Xet("mock upload failure".into()));
        }
        let mut results = Vec::new();
        for path in paths {
            let content = std::fs::read(path).map_err(Error::Io)?;
            let hash = self.next_hash_string();
            let size = content.len() as u64;
            self.files.lock().unwrap().insert(hash.clone(), content);
            results.push(XetFileInfo::new(hash, size));
        }
        Ok(results)
    }

    async fn warm_reconstruction_cache(&self, _xet_hash: &str) {
        self.warm_call_count.fetch_add(1, Ordering::SeqCst);
        let delay = self.warm_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }

    fn download_stream_boxed(&self, file_info: &XetFileInfo, offset: u64) -> Result<Box<dyn DownloadStreamOps>> {
        let prev_fail = self.range_fail_count.load(Ordering::SeqCst);
        if prev_fail > 0 {
            self.range_fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::Xet("mock stream open failure".into()));
        }
        let prev_empty = self.range_empty_count.load(Ordering::SeqCst);
        if prev_empty > 0 {
            self.range_empty_count.fetch_sub(1, Ordering::SeqCst);
            return Ok(Box::new(MockDownloadStream {
                data: Vec::new(),
                offset: 0,
                chunk_size: 4096,
            }));
        }
        let files = self.files.lock().unwrap();
        let content = files.get(file_info.hash()).cloned().unwrap_or_default();
        Ok(Box::new(MockDownloadStream {
            data: content,
            offset: offset as usize,
            chunk_size: 4096,
        }))
    }
}

// ── MockStreamingWriter ───────────────────────────────────────────────

pub struct MockStreamingWriter {
    data: Vec<u8>,
    hash: String,
    fail_after: u64,
}

#[async_trait::async_trait]
impl StreamingWriterOps for MockStreamingWriter {
    async fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.data.len() as u64 + data.len() as u64 > self.fail_after {
            return Err(Error::Xet("mock writer failure after N bytes".into()));
        }
        self.data.extend_from_slice(data);
        Ok(())
    }

    async fn finish_boxed(self: Box<Self>) -> Result<XetFileInfo> {
        let size = self.data.len() as u64;
        Ok(XetFileInfo::new(self.hash.clone(), size))
    }

    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// ── MockDownloadStream ────────────────────────────────────────────────

pub struct MockDownloadStream {
    data: Vec<u8>,
    offset: usize,
    chunk_size: usize,
}

#[async_trait::async_trait]
impl DownloadStreamOps for MockDownloadStream {
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.offset >= self.data.len() {
            return Ok(None);
        }
        let end = (self.offset + self.chunk_size).min(self.data.len());
        let chunk = Bytes::copy_from_slice(&self.data[self.offset..end]);
        self.offset = end;
        Ok(Some(chunk))
    }
}

// ── Test VFS builder ──────────────────────────────────────────────────

pub struct TestOpts {
    pub read_only: bool,
    pub advanced_writes: bool,
    pub serve_lookup_from_cache: bool,
    pub metadata_ttl: Duration,
}

impl Default for TestOpts {
    fn default() -> Self {
        Self {
            read_only: false,
            advanced_writes: false,
            serve_lookup_from_cache: false,
            metadata_ttl: Duration::from_secs(1),
        }
    }
}

/// Build a VirtualFs for testing. Must be called from a sync context
/// (not inside #[tokio::test]) because VirtualFs::new() calls block_on internally.
pub fn make_test_vfs(
    hub: Arc<MockHub>,
    xet: Arc<MockXet>,
    opts: TestOpts,
    runtime: &tokio::runtime::Runtime,
) -> Arc<crate::virtual_fs::VirtualFs> {
    // Repos need a staging dir for HTTP download cache (open_readonly),
    // even when advanced_writes is disabled (mirrors setup.rs logic).
    let staging_dir = if opts.advanced_writes || hub.is_repo() {
        let path = std::env::temp_dir().join(format!("hf_mount_test_{}", std::process::id()));
        std::fs::create_dir_all(&path).expect("failed to create temp staging dir");
        Some(StagingDir::new(&path))
    } else {
        None
    };

    crate::virtual_fs::VirtualFs::new(
        runtime.handle().clone(),
        hub,
        xet,
        staging_dir,
        opts.read_only,
        opts.advanced_writes,
        1000, // uid
        1000, // gid
        0,    // poll_interval_secs = 0 (disabled)
        opts.metadata_ttl,
        opts.serve_lookup_from_cache,
        true,                       // filter_os_files
        false,                      // direct_io
        Duration::from_millis(100), // fast debounce for tests
        Duration::from_secs(1),     // fast batch window for tests
    )
}
