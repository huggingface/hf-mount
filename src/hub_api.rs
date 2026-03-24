use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::info;
use xet_client::cas_client::auth::{AuthError, TokenInfo, TokenRefresher};

use crate::error::{Error, Result};

// ── HubOps trait ──────────────────────────────────────────────────────

/// Trait abstracting the Hub API operations used by VirtualFs and FlushManager.
/// Production code uses `HubApiClient`; tests inject mocks.
#[async_trait::async_trait]
pub trait HubOps: Send + Sync {
    async fn list_tree(&self, prefix: &str) -> Result<Vec<TreeEntry>>;
    async fn head_file(&self, path: &str) -> Result<Option<HeadFileInfo>>;
    async fn batch_operations(&self, ops: &[BatchOp]) -> Result<()>;
    async fn download_file_http(&self, path: &str, dest: &Path) -> Result<()>;
    fn default_mtime(&self) -> SystemTime;
    fn source(&self) -> &SourceKind;
    fn is_repo(&self) -> bool;
}

// ── Repo / Bucket types ───────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum RepoType {
    Model,
    Dataset,
    Space,
}

impl RepoType {
    /// Path segment used in `/api/{type}/{id}/…` API routes.
    pub fn api_prefix(&self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
        }
    }

    /// Path segment used in `/{type}/{id}/resolve/…` user-facing routes.
    pub fn resolve_prefix(&self) -> &'static str {
        // Models don't have a type prefix in resolve URLs (e.g. /user/repo/resolve/main/file)
        // Datasets and spaces do (e.g. /datasets/user/repo/resolve/main/file)
        match self {
            Self::Model => "",
            Self::Dataset => "datasets/",
            Self::Space => "spaces/",
        }
    }
}

impl std::str::FromStr for RepoType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "model" => Ok(Self::Model),
            "dataset" => Ok(Self::Dataset),
            "space" => Ok(Self::Space),
            _ => Err(format!("unknown repo type: {s} (expected model, dataset, or space)")),
        }
    }
}

impl std::fmt::Display for RepoType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Model => f.write_str("model"),
            Self::Dataset => f.write_str("dataset"),
            Self::Space => f.write_str("space"),
        }
    }
}

/// Identifies whether we're talking to a bucket or a repo.
/// Also serves as the clap subcommand for the CLI.
#[derive(Debug, Clone)]
pub enum SourceKind {
    Bucket {
        bucket_id: String,
    },
    Repo {
        repo_id: String,
        repo_type: RepoType,
        revision: String,
    },
}

impl std::fmt::Display for SourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bucket { bucket_id } => write!(f, "bucket/{bucket_id}"),
            Self::Repo {
                repo_id,
                repo_type,
                revision,
            } => write!(f, "{repo_type}/{repo_id}/{revision}"),
        }
    }
}

// ── Shared data types ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BatchOp {
    #[serde(rename_all = "camelCase")]
    AddFile {
        path: String,
        xet_hash: String,
        mtime: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    DeleteFile { path: String },
}

/// Unified tree entry exposed to the rest of the codebase.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub size: Option<u64>,
    pub xet_hash: Option<String>,
    /// Git blob OID (same value as ETag on resolve endpoint).
    #[serde(default)]
    pub oid: Option<String>,
    pub mtime: Option<String>,
}

/// Raw tree entry from the repo `/tree` API (different shape from bucket tree).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RepoTreeEntry {
    path: String,
    #[serde(rename = "type")]
    entry_type: String,
    size: Option<u64>,
    #[serde(default)]
    oid: Option<String>,
    #[serde(default)]
    xet_hash: Option<String>,
    #[serde(default)]
    lfs: Option<LfsInfo>,
    #[serde(default)]
    last_commit: Option<CommitInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommitInfo {
    date: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LfsInfo {
    size: u64,
    #[allow(dead_code)]
    pointer_size: Option<u64>,
}

/// Metadata returned by HEAD on the resolve endpoint
#[derive(Debug)]
pub struct HeadFileInfo {
    pub xet_hash: Option<String>,
    pub etag: Option<String>,
    pub size: Option<u64>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CasTokenInfo {
    pub cas_url: String,
    pub exp: u64,
    pub access_token: String,
}

// ── HubApiClient ──────────────────────────────────────────────────────

/// How often the token file is re-read from disk.
const TOKEN_FILE_REFRESH: std::time::Duration = std::time::Duration::from_secs(30);

pub struct HubApiClient {
    client: Client,
    /// Client that does NOT follow redirects — used for HEAD requests where we
    /// need response headers from the Hub (not from the CAS redirect target).
    head_client: Client,
    endpoint: String,
    token: Option<String>,
    /// Path to a file containing the API token. Re-read periodically so
    /// the CSI driver can refresh credentials without remounting.
    /// Takes precedence over `token` when the file exists and is non-empty.
    token_file: Option<PathBuf>,
    /// Cached token from `token_file`, refreshed every 30s.
    token_file_cache: std::sync::Mutex<Option<(std::time::Instant, String)>>,
    source: SourceKind,
    /// Last modification time (from repo/bucket info endpoint).
    /// Used as default mtime when per-file mtime is unavailable.
    last_modified: SystemTime,
    /// Optional subfolder prefix. When non-empty, all API calls transparently
    /// prepend this to outgoing paths and strip it from incoming TreeEntry paths.
    path_prefix: String,
}

/// Parse a repo ID, extracting the type from an optional prefix.
/// "datasets/user/ds" → (Dataset, "user/ds")
/// "spaces/user/app" → (Space, "user/app")
/// "user/model" → (Model, "user/model")
pub fn parse_repo_id(repo_id: &str) -> (RepoType, String) {
    if let Some(rest) = repo_id.strip_prefix("datasets/") {
        (RepoType::Dataset, rest.to_string())
    } else if let Some(rest) = repo_id.strip_prefix("spaces/") {
        (RepoType::Space, rest.to_string())
    } else {
        (RepoType::Model, repo_id.to_string())
    }
}

/// Split a raw identifier into `(id, path_prefix)` after the first 2 `/`-separated segments.
/// E.g. `split_path_prefix("user/bucket/a/b")` → `("user/bucket", "a/b")`.
/// Trailing slashes are trimmed, and `.`/`..` components in the prefix are rejected.
pub fn split_path_prefix(raw: &str) -> std::result::Result<(&str, &str), &'static str> {
    let raw = raw.trim_end_matches('/');
    let mut end = 0;
    let mut count = 0;
    for (i, ch) in raw.char_indices() {
        if ch == '/' {
            count += 1;
            if count == 2 {
                end = i;
                break;
            }
        }
    }
    if count < 2 {
        // Not enough segments — entire string is the ID, no prefix.
        Ok((raw, ""))
    } else {
        let prefix = &raw[end + 1..];
        if prefix.split('/').any(|s| s == "." || s == "..") {
            return Err("path prefix must not contain '.' or '..' components");
        }
        Ok((&raw[..end], prefix))
    }
}

fn make_clients() -> (Client, Client) {
    let client = Client::new();
    let head_client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build head_client");
    (client, head_client)
}

impl HubApiClient {
    /// Create a client from a `SourceKind` (bucket or repo).
    /// Create a client from a source kind. For repos, resolves aliases
    /// (e.g. "gpt2" → "openai-community/gpt2") and fetches repo metadata.
    pub async fn from_source(
        endpoint: &str,
        token: Option<&str>,
        token_file: Option<PathBuf>,
        source: SourceKind,
        path_prefix: String,
    ) -> Result<Arc<Self>> {
        let (client, head_client) = make_clients();
        let endpoint = endpoint.trim_end_matches('/').to_string();

        // Read token from file if no inline token was provided.
        let file_token = if token.is_none() {
            token_file
                .as_ref()
                .and_then(|p| std::fs::read_to_string(p).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        } else {
            None
        };
        let effective_token: Option<&str> = token.or(file_token.as_deref());

        let auth = |req: reqwest::RequestBuilder| -> reqwest::RequestBuilder {
            match effective_token {
                Some(t) => req.bearer_auth(t),
                None => req,
            }
        };

        let (source, last_modified) = match source {
            SourceKind::Repo {
                repo_id,
                repo_type,
                revision,
            } => {
                let url = format!("{}/api/{}/{}", endpoint, repo_type.api_prefix(), repo_id);
                let resp = auth(client.get(&url)).send().await?;
                if !resp.status().is_success() {
                    return Err(Error::Hub(format!(
                        "Failed to resolve repo {}: {} {}",
                        repo_id,
                        resp.status(),
                        resp.text().await.unwrap_or_default()
                    )));
                }
                let body: serde_json::Value = resp.json().await?;
                let resolved_id = body["id"]
                    .as_str()
                    .ok_or_else(|| Error::Hub("repo info missing 'id' field".to_string()))?;
                if resolved_id != repo_id {
                    info!("Resolved repo alias: {} → {}", repo_id, resolved_id);
                }
                let last_modified = body["lastModified"].as_str().map(mtime_from_str).unwrap_or(UNIX_EPOCH);
                (
                    SourceKind::Repo {
                        repo_id: resolved_id.to_string(),
                        repo_type,
                        revision,
                    },
                    last_modified,
                )
            }
            SourceKind::Bucket { bucket_id } => {
                let url = format!("{}/api/buckets/{}", endpoint, bucket_id);
                let resp = auth(client.get(&url)).send().await?;
                if !resp.status().is_success() {
                    return Err(Error::Hub(format!(
                        "Bucket not found: {} ({})",
                        bucket_id,
                        resp.status(),
                    )));
                }
                let body: serde_json::Value = resp.json().await?;
                let last_modified = body["updatedAt"].as_str().map(mtime_from_str).unwrap_or(UNIX_EPOCH);
                (SourceKind::Bucket { bucket_id }, last_modified)
            }
        };

        Ok(Arc::new(Self {
            client,
            head_client,
            endpoint,
            token: token.map(|t| t.to_string()),
            token_file,
            token_file_cache: std::sync::Mutex::new(None),
            source,
            last_modified,
            path_prefix,
        }))
    }

    /// Create a client for a HuggingFace bucket.
    pub fn new(endpoint: &str, token: Option<&str>, bucket_id: &str) -> Arc<Self> {
        let (client, head_client) = make_clients();
        Arc::new(Self {
            client,
            head_client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.map(|t| t.to_string()),
            token_file: None,
            token_file_cache: std::sync::Mutex::new(None),
            source: SourceKind::Bucket {
                bucket_id: bucket_id.to_string(),
            },
            last_modified: UNIX_EPOCH,
            path_prefix: String::new(),
        })
    }

    /// Attach bearer auth to a request if a token is configured.
    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(path) = &self.token_file {
            // Check TTL under lock, but do file I/O outside it to avoid
            // blocking concurrent requests on slow storage.
            let need_refresh = {
                let cache = self.token_file_cache.lock().expect("token_file_cache poisoned");
                match &*cache {
                    Some((at, _)) => at.elapsed() >= TOKEN_FILE_REFRESH,
                    None => true,
                }
            };
            if need_refresh && let Ok(contents) = std::fs::read_to_string(path) {
                let token = contents.trim().to_string();
                if !token.is_empty() {
                    let mut cache = self.token_file_cache.lock().expect("token_file_cache poisoned");
                    *cache = Some((std::time::Instant::now(), token));
                }
            }
            let cache = self.token_file_cache.lock().expect("token_file_cache poisoned");
            if let Some((_, ref token)) = *cache {
                return req.bearer_auth(token);
            }
        }
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    pub fn source(&self) -> &SourceKind {
        &self.source
    }

    /// Default mtime when per-file mtime is unavailable.
    pub fn default_mtime(&self) -> SystemTime {
        self.last_modified
    }

    pub fn is_repo(&self) -> bool {
        matches!(self.source, SourceKind::Repo { .. })
    }

    /// Return the path prefix (subfolder) this client is scoped to.
    pub fn path_prefix(&self) -> &str {
        &self.path_prefix
    }

    /// Join `path_prefix` and `path` into the full API path.
    fn prefixed_path(&self, path: &str) -> String {
        if self.path_prefix.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            self.path_prefix.clone()
        } else {
            format!("{}/{}", self.path_prefix, path)
        }
    }

    /// Strip `path_prefix` from the beginning of `full`. Returns `None` if the
    /// path doesn't start with the prefix (shouldn't happen for correctly scoped results).
    fn strip_path_prefix<'a>(&self, full: &'a str) -> Option<&'a str> {
        if self.path_prefix.is_empty() {
            return Some(full);
        }
        if full == self.path_prefix {
            // The prefix directory itself — caller should filter this out.
            return Some("");
        }
        full.strip_prefix(&self.path_prefix)
            .and_then(|rest| rest.strip_prefix('/'))
    }

    /// Validate that the path prefix exists on the remote.
    /// Calls list_tree("") which internally prepends the prefix.
    pub async fn validate_path_prefix(&self) -> Result<()> {
        if self.path_prefix.is_empty() {
            return Ok(());
        }
        let entries = self.list_tree("").await.map_err(|e| {
            Error::Hub(format!(
                "Subfolder '{}' not found in {}: {e}",
                self.path_prefix, self.source,
            ))
        })?;
        if entries.is_empty() {
            return Err(Error::Hub(format!(
                "Subfolder '{}' is empty or does not exist in {}",
                self.path_prefix, self.source,
            )));
        }
        Ok(())
    }

    /// List tree entries at the given prefix (single directory level).
    /// Follows `Link` header pagination. For repos, includes `expand=true`
    /// to get per-file lastCommit (mtime). For buckets, passes `recursive=false`.
    pub async fn list_tree(&self, prefix: &str) -> Result<Vec<TreeEntry>> {
        let api_prefix = self.prefixed_path(prefix);
        let mut entries = match &self.source {
            SourceKind::Bucket { bucket_id } => self.list_tree_bucket(bucket_id, &api_prefix).await?,
            SourceKind::Repo {
                repo_id,
                repo_type,
                revision,
            } => self.list_tree_repo(repo_id, *repo_type, revision, &api_prefix).await?,
        };

        // Strip path prefix from returned entries and filter out the prefix dir itself.
        if !self.path_prefix.is_empty() {
            entries.retain_mut(|e| {
                match self.strip_path_prefix(&e.path) {
                    Some(stripped) if !stripped.is_empty() => {
                        e.path = stripped.to_string();
                        true
                    }
                    _ => false, // filter out prefix dir itself or unrelated entries
                }
            });
        }

        Ok(entries)
    }

    async fn list_tree_bucket(&self, bucket_id: &str, prefix: &str) -> Result<Vec<TreeEntry>> {
        let mut all_entries = Vec::new();
        let recursive_param = "?recursive=false";
        let mut url = if prefix.is_empty() {
            format!("{}/api/buckets/{}/tree{recursive_param}", self.endpoint, bucket_id)
        } else {
            format!(
                "{}/api/buckets/{}/tree/{}{recursive_param}",
                self.endpoint, bucket_id, prefix
            )
        };

        loop {
            let resp = self.auth(self.client.get(&url)).send().await?;

            if !resp.status().is_success() {
                return Err(Error::Hub(format!(
                    "tree listing failed: {} {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                )));
            }

            let next_url = resp
                .headers()
                .get("link")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_link_next);

            let entries: Vec<TreeEntry> = resp.json().await?;
            all_entries.extend(entries);

            match next_url {
                Some(next) => url = next,
                None => break,
            }
        }

        Ok(all_entries)
    }

    async fn list_tree_repo(
        &self,
        repo_id: &str,
        repo_type: RepoType,
        revision: &str,
        prefix: &str,
    ) -> Result<Vec<TreeEntry>> {
        let mut all_entries = Vec::new();
        // expand=true fetches per-file lastCommit (mtime) from Gitaly.
        // Acceptable for non-recursive listings where entry count is small.
        let params = "?expand=true";
        let mut url = if prefix.is_empty() {
            format!(
                "{}/api/{}/{}/tree/{}{params}",
                self.endpoint,
                repo_type.api_prefix(),
                repo_id,
                revision,
            )
        } else {
            format!(
                "{}/api/{}/{}/tree/{}/{}{params}",
                self.endpoint,
                repo_type.api_prefix(),
                repo_id,
                revision,
                prefix,
            )
        };

        loop {
            let resp = self.auth(self.client.get(&url)).send().await?;

            if !resp.status().is_success() {
                return Err(Error::Hub(format!(
                    "repo tree listing failed: {} {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                )));
            }

            let next_url = resp
                .headers()
                .get("link")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_link_next);

            let raw_entries: Vec<RepoTreeEntry> = resp.json().await?;
            for raw in raw_entries {
                // For LFS files, the top-level `size` is the pointer size;
                // the real file size lives in `lfs.size`.
                let size = if let Some(ref lfs) = raw.lfs {
                    Some(lfs.size)
                } else {
                    raw.size
                };

                all_entries.push(TreeEntry {
                    path: raw.path,
                    entry_type: raw.entry_type,
                    size,
                    xet_hash: raw.xet_hash,
                    oid: raw.oid,
                    mtime: raw.last_commit.and_then(|c| c.date),
                });
            }

            match next_url {
                Some(next) => url = next,
                None => break,
            }
        }

        Ok(all_entries)
    }

    /// Fetch metadata for a single file via HEAD on the resolve endpoint.
    /// Returns `None` if 404 (file does not exist remotely).
    pub async fn head_file(&self, path: &str) -> Result<Option<HeadFileInfo>> {
        let api_path = self.prefixed_path(path);
        let url = match &self.source {
            // Buckets: /buckets/{id}/resolve/{path} (no /api/ prefix)
            SourceKind::Bucket { bucket_id } => {
                format!("{}/buckets/{}/resolve/{}", self.endpoint, bucket_id, api_path)
            }
            // Repos: /{resolve_prefix}{id}/resolve/{revision}/{path}
            SourceKind::Repo {
                repo_id,
                repo_type,
                revision,
            } => {
                format!(
                    "{}/{}{}/resolve/{}/{}",
                    self.endpoint,
                    repo_type.resolve_prefix(),
                    repo_id,
                    revision,
                    api_path,
                )
            }
        };
        let resp = self.auth(self.head_client.head(&url)).send().await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() && !resp.status().is_redirection() {
            return Err(Error::Hub(format!("head_file failed: {}", resp.status())));
        }

        let headers = resp.headers();
        let xet_hash = headers
            .get("x-xet-hash")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let size = headers
            .get("x-linked-size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let etag = headers
            .get("x-linked-etag")
            .or_else(|| headers.get("etag"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());
        let last_modified = headers
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(Some(HeadFileInfo {
            xet_hash,
            etag,
            size,
            last_modified,
        }))
    }

    /// Get a CAS read token.
    pub async fn get_cas_token(&self) -> Result<CasTokenInfo> {
        let url = match &self.source {
            SourceKind::Bucket { bucket_id } => {
                format!("{}/api/buckets/{}/xet-read-token", self.endpoint, bucket_id)
            }
            SourceKind::Repo {
                repo_id,
                repo_type,
                revision,
            } => {
                format!(
                    "{}/api/{}/{}/xet-read-token/{}",
                    self.endpoint,
                    repo_type.api_prefix(),
                    repo_id,
                    revision,
                )
            }
        };

        let resp = self.auth(self.client.get(&url)).send().await?;

        if !resp.status().is_success() {
            return Err(Error::Hub(format!(
                "CAS token request failed: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        let info: CasTokenInfo = resp.json().await?;
        Ok(info)
    }

    /// Get a CAS write token (buckets only).
    pub async fn get_cas_write_token(&self) -> Result<CasTokenInfo> {
        let bucket_id = match &self.source {
            SourceKind::Bucket { bucket_id } => bucket_id,
            SourceKind::Repo { .. } => {
                return Err(Error::Hub("write tokens not supported for repos".to_string()));
            }
        };
        let url = format!("{}/api/buckets/{}/xet-write-token", self.endpoint, bucket_id);

        let resp = self.auth(self.client.get(&url)).send().await?;

        if !resp.status().is_success() {
            return Err(Error::Hub(format!(
                "CAS write token request failed: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        let info: CasTokenInfo = resp.json().await?;
        Ok(info)
    }

    /// Execute batch operations (add/delete files) on the bucket.
    pub async fn batch_operations(&self, ops: &[BatchOp]) -> Result<()> {
        let bucket_id = match &self.source {
            SourceKind::Bucket { bucket_id } => bucket_id,
            SourceKind::Repo { .. } => {
                return Err(Error::Hub("batch operations not supported for repos".to_string()));
            }
        };
        let url = format!("{}/api/buckets/{}/batch", self.endpoint, bucket_id);

        // Build NDJSON body, prepending path_prefix to each op's path.
        let mut body = String::new();
        for op in ops {
            if self.path_prefix.is_empty() {
                body.push_str(&serde_json::to_string(op)?);
            } else {
                let prefixed = match op {
                    BatchOp::AddFile {
                        path,
                        xet_hash,
                        mtime,
                        content_type,
                    } => BatchOp::AddFile {
                        path: self.prefixed_path(path),
                        xet_hash: xet_hash.clone(),
                        mtime: *mtime,
                        content_type: content_type.clone(),
                    },
                    BatchOp::DeleteFile { path } => BatchOp::DeleteFile {
                        path: self.prefixed_path(path),
                    },
                };
                body.push_str(&serde_json::to_string(&prefixed)?);
            }
            body.push('\n');
        }

        let resp = self
            .auth(self.client.post(&url))
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(Error::Hub(format!(
                "batch operation failed: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        Ok(())
    }

    /// Download a file via HTTP GET on the resolve endpoint and write it to `dest`.
    /// Used for plain LFS / plain git files in repos (no xet hash).
    ///
    /// Supports ETag-based conditional requests: if `dest` already exists and a
    /// sidecar `{dest}.etag` file is present, sends `If-None-Match`. On 304 the
    /// existing cached file is kept as-is.
    pub async fn download_file_http(&self, path: &str, dest: &Path) -> Result<()> {
        let api_path = self.prefixed_path(path);
        let url = match &self.source {
            SourceKind::Bucket { bucket_id } => {
                format!("{}/buckets/{}/resolve/{}", self.endpoint, bucket_id, api_path)
            }
            SourceKind::Repo {
                repo_id,
                repo_type,
                revision,
            } => {
                format!(
                    "{}/{}{}/resolve/{}/{}",
                    self.endpoint,
                    repo_type.resolve_prefix(),
                    repo_id,
                    revision,
                    api_path,
                )
            }
        };

        // Read cached ETag if present.
        let etag_path = dest.with_extension("etag");
        let cached_etag = if dest.exists() {
            std::fs::read_to_string(&etag_path).ok()
        } else {
            None
        };

        let mut req = self.auth(self.client.get(&url));
        if let Some(ref etag) = cached_etag {
            // Re-quote for RFC 7232 compliance (sidecar stores unquoted value).
            req = req.header("If-None-Match", format!("\"{}\"", etag.trim()));
        }

        info!("HTTP download: {} → {:?}", path, dest);
        let resp = req.send().await?;

        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            info!("HTTP cache hit (304): {}", path);
            return Ok(());
        }

        if !resp.status().is_success() {
            return Err(Error::Hub(format!(
                "HTTP download failed for {}: {} {}",
                path,
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        let new_etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());

        // Stream response body to a temp file, then atomic-rename to dest.
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = dest.with_extension(format!("tmp.{}", std::process::id()));
        let result: std::result::Result<(), Error> = async {
            let mut file = tokio::fs::File::create(&tmp).await?;
            let mut stream = resp.bytes_stream();
            use futures::StreamExt;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk).await?;
            }
            file.shutdown().await?;
            drop(file);
            tokio::fs::rename(&tmp, dest).await?;
            if let Some(etag) = &new_etag {
                tokio::fs::write(&etag_path, etag).await.ok();
            } else {
                tokio::fs::remove_file(&etag_path).await.ok();
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            tokio::fs::remove_file(&tmp).await.ok();
        }
        result
    }

    /// Create a token refresher for this source.
    /// Uses a write token when `read_only` is false (write tokens can also read).
    pub fn token_refresher(self: &Arc<Self>, read_only: bool) -> Arc<HubTokenRefresher> {
        let kind = if read_only { TokenKind::Read } else { TokenKind::Write };
        Arc::new(HubTokenRefresher {
            hub_client: self.clone(),
            kind,
        })
    }
}

pub fn mtime_from_str(s: &str) -> SystemTime {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .and_then(|dt| u64::try_from(dt.timestamp()).ok())
        .map(|secs| UNIX_EPOCH + std::time::Duration::from_secs(secs))
        .unwrap_or(UNIX_EPOCH)
}

/// Parse HTTP-date format (e.g. "Sat, 28 Feb 2026 14:52:39 GMT") from Last-Modified header.
pub fn mtime_from_http_date(s: &str) -> SystemTime {
    chrono::DateTime::parse_from_rfc2822(s)
        .ok()
        .and_then(|dt| u64::try_from(dt.timestamp()).ok())
        .map(|secs| UNIX_EPOCH + std::time::Duration::from_secs(secs))
        .unwrap_or(UNIX_EPOCH)
}

#[async_trait::async_trait]
impl HubOps for HubApiClient {
    async fn list_tree(&self, prefix: &str) -> Result<Vec<TreeEntry>> {
        self.list_tree(prefix).await
    }
    async fn head_file(&self, path: &str) -> Result<Option<HeadFileInfo>> {
        self.head_file(path).await
    }
    async fn batch_operations(&self, ops: &[BatchOp]) -> Result<()> {
        self.batch_operations(ops).await
    }
    async fn download_file_http(&self, path: &str, dest: &Path) -> Result<()> {
        self.download_file_http(path, dest).await
    }
    fn default_mtime(&self) -> SystemTime {
        self.default_mtime()
    }
    fn source(&self) -> &SourceKind {
        self.source()
    }
    fn is_repo(&self) -> bool {
        self.is_repo()
    }
}

/// Parse `Link` header to extract the URL with `rel="next"`.
/// Format: `<https://example.com/page2>; rel="next", <...>; rel="prev"`
fn parse_link_next(header: &str) -> Option<String> {
    for part in header.split(',') {
        let part = part.trim();
        if part.contains("rel=\"next\"")
            && let Some(start) = part.find('<')
            && let Some(end) = part.find('>')
        {
            return Some(part[start + 1..end].to_string());
        }
    }
    None
}

// ── Token refresh ─────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum TokenKind {
    Read,
    Write,
}

pub struct HubTokenRefresher {
    hub_client: Arc<HubApiClient>,
    kind: TokenKind,
}

impl HubTokenRefresher {
    pub async fn fetch_initial(&self) -> Result<CasTokenInfo> {
        match self.kind {
            TokenKind::Read => self.hub_client.get_cas_token().await,
            TokenKind::Write => self.hub_client.get_cas_write_token().await,
        }
    }
}

#[async_trait::async_trait]
impl TokenRefresher for HubTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let jwt = self
            .fetch_initial()
            .await
            .map_err(|e| AuthError::TokenRefreshFailure(e.to_string()))?;
        Ok((jwt.access_token, jwt.exp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_op_add_file_serialization() {
        let op = BatchOp::AddFile {
            path: "data/file.bin".to_string(),
            xet_hash: "abc123def456".to_string(),
            mtime: 1700000000000,
            content_type: Some("application/octet-stream".to_string()),
        };

        let json = serde_json::to_string(&op).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify tag is camelCase "addFile"
        assert_eq!(parsed["type"], "addFile");
        assert_eq!(parsed["path"], "data/file.bin");
        assert_eq!(parsed["xetHash"], "abc123def456");
        assert_eq!(parsed["mtime"], 1700000000000u64);
        assert_eq!(parsed["contentType"], "application/octet-stream");
    }

    #[test]
    fn test_batch_op_delete_file_serialization() {
        let op = BatchOp::DeleteFile {
            path: "old/file.txt".to_string(),
        };

        let json = serde_json::to_string(&op).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["type"], "deleteFile");
        assert_eq!(parsed["path"], "old/file.txt");

        // Should not contain addFile-specific fields
        assert!(parsed.get("xetHash").is_none());
        assert!(parsed.get("mtime").is_none());
    }

    #[test]
    fn test_batch_op_add_file_no_content_type() {
        let op = BatchOp::AddFile {
            path: "readme.txt".to_string(),
            xet_hash: "hash999".to_string(),
            mtime: 1234567890000,
            content_type: None,
        };

        let json = serde_json::to_string(&op).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // contentType should be absent (skip_serializing_if = "Option::is_none")
        assert!(
            parsed.get("contentType").is_none(),
            "contentType should be omitted when None"
        );

        // Other fields should still be present
        assert_eq!(parsed["type"], "addFile");
        assert_eq!(parsed["path"], "readme.txt");
        assert_eq!(parsed["xetHash"], "hash999");
        assert_eq!(parsed["mtime"], 1234567890000u64);
    }

    #[test]
    fn test_parse_link_next_picks_next_relation() {
        let header = r#"<https://api.example.com/page=1>; rel="prev", <https://api.example.com/page=3>; rel="next""#;
        assert_eq!(
            parse_link_next(header),
            Some("https://api.example.com/page=3".to_string())
        );
    }

    #[test]
    fn test_parse_link_next_returns_none_when_missing() {
        let header = r#"<https://api.example.com/page=1>; rel="prev", <https://api.example.com/page=2>; rel="last""#;
        assert_eq!(parse_link_next(header), None);
    }

    #[test]
    fn test_mtime_parsers_fallback_to_unix_epoch_on_invalid_input() {
        assert_eq!(mtime_from_str("not-a-date"), UNIX_EPOCH);
        assert_eq!(mtime_from_http_date("still-not-a-date"), UNIX_EPOCH);
    }

    #[test]
    fn test_mtime_parsers_support_valid_formats() {
        let rfc3339 = mtime_from_str("2026-02-28T14:52:39Z");
        let http_date = mtime_from_http_date("Sat, 28 Feb 2026 14:52:39 GMT");
        assert!(rfc3339 > UNIX_EPOCH);
        assert!(http_date > UNIX_EPOCH);
        assert_eq!(rfc3339, http_date);
    }

    #[test]
    fn test_repo_type_from_str() {
        assert_eq!("model".parse::<RepoType>().unwrap(), RepoType::Model);
        assert_eq!("dataset".parse::<RepoType>().unwrap(), RepoType::Dataset);
        assert_eq!("space".parse::<RepoType>().unwrap(), RepoType::Space);
        assert!("unknown".parse::<RepoType>().is_err());
    }

    #[test]
    fn test_repo_type_api_prefix() {
        assert_eq!(RepoType::Model.api_prefix(), "models");
        assert_eq!(RepoType::Dataset.api_prefix(), "datasets");
        assert_eq!(RepoType::Space.api_prefix(), "spaces");
    }

    #[test]
    fn test_repo_type_resolve_prefix() {
        assert_eq!(RepoType::Model.resolve_prefix(), "");
        assert_eq!(RepoType::Dataset.resolve_prefix(), "datasets/");
        assert_eq!(RepoType::Space.resolve_prefix(), "spaces/");
    }

    // ── split_path_prefix tests ───────────────────────────────────────

    #[test]
    fn test_split_path_prefix_bucket_no_subfolder() {
        let (id, prefix) = split_path_prefix("user/bucket").unwrap();
        assert_eq!(id, "user/bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_split_path_prefix_bucket_with_subfolder() {
        let (id, prefix) = split_path_prefix("user/bucket/a/b").unwrap();
        assert_eq!(id, "user/bucket");
        assert_eq!(prefix, "a/b");
    }

    #[test]
    fn test_split_path_prefix_bucket_single_subfolder() {
        let (id, prefix) = split_path_prefix("user/bucket/checkpoints").unwrap();
        assert_eq!(id, "user/bucket");
        assert_eq!(prefix, "checkpoints");
    }

    #[test]
    fn test_split_path_prefix_single_segment() {
        let (id, prefix) = split_path_prefix("gpt2").unwrap();
        assert_eq!(id, "gpt2");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_split_path_prefix_repo_with_subfolder() {
        let (id, prefix) = split_path_prefix("user/model/ckpt/v2").unwrap();
        assert_eq!(id, "user/model");
        assert_eq!(prefix, "ckpt/v2");
    }

    #[test]
    fn test_split_path_prefix_trailing_slash() {
        let (id, prefix) = split_path_prefix("user/bucket/checkpoints/").unwrap();
        assert_eq!(id, "user/bucket");
        assert_eq!(prefix, "checkpoints");
    }

    #[test]
    fn test_split_path_prefix_rejects_dotdot() {
        assert!(split_path_prefix("user/bucket/../other").is_err());
    }

    #[test]
    fn test_split_path_prefix_rejects_dot() {
        assert!(split_path_prefix("user/bucket/./foo").is_err());
    }

    // ── prefixed_path / strip_path_prefix tests ───────────────────────

    fn make_test_client(prefix: &str, token_file: Option<PathBuf>) -> HubApiClient {
        let (client, head_client) = make_clients();
        HubApiClient {
            client,
            head_client,
            endpoint: "https://huggingface.co".to_string(),
            token: Some("static-token".to_string()),
            token_file,
            token_file_cache: std::sync::Mutex::new(None),
            source: SourceKind::Bucket {
                bucket_id: "user/bucket".to_string(),
            },
            last_modified: UNIX_EPOCH,
            path_prefix: prefix.to_string(),
        }
    }

    #[test]
    fn test_prefixed_path_empty_prefix() {
        let c = make_test_client("", None);
        assert_eq!(c.prefixed_path("file.txt"), "file.txt");
        assert_eq!(c.prefixed_path("a/b"), "a/b");
        assert_eq!(c.prefixed_path(""), "");
    }

    #[test]
    fn test_prefixed_path_with_prefix() {
        let c = make_test_client("sub/dir", None);
        assert_eq!(c.prefixed_path("file.txt"), "sub/dir/file.txt");
        assert_eq!(c.prefixed_path("a/b"), "sub/dir/a/b");
        assert_eq!(c.prefixed_path(""), "sub/dir");
    }

    #[test]
    fn test_strip_path_prefix_empty_prefix() {
        let c = make_test_client("", None);
        assert_eq!(c.strip_path_prefix("file.txt"), Some("file.txt"));
        assert_eq!(c.strip_path_prefix("a/b/c"), Some("a/b/c"));
    }

    #[test]
    fn test_strip_path_prefix_with_prefix() {
        let c = make_test_client("sub/dir", None);
        assert_eq!(c.strip_path_prefix("sub/dir/file.txt"), Some("file.txt"));
        assert_eq!(c.strip_path_prefix("sub/dir/a/b"), Some("a/b"));
        // The prefix directory itself → empty string
        assert_eq!(c.strip_path_prefix("sub/dir"), Some(""));
        // Unrelated path → None
        assert_eq!(c.strip_path_prefix("other/file.txt"), None);
    }

    // ── token file cache tests ────────────────────────────────────────

    fn cached_token(client: &HubApiClient) -> Option<String> {
        client.token_file_cache.lock().unwrap().as_ref().map(|(_, t)| t.clone())
    }

    fn test_token_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hf-mount-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn token_file_populates_cache_on_first_auth() {
        let dir = test_token_dir();
        let path = dir.join("token-pop");
        std::fs::write(&path, "file-token\n").unwrap();

        let client = make_test_client("", Some(path.clone()));
        let req = client.client.get("http://example.com");
        let _ = client.auth(req);

        assert_eq!(cached_token(&client).as_deref(), Some("file-token"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn token_file_cache_reused_within_ttl() {
        let dir = test_token_dir();
        let path = dir.join("token-ttl");
        std::fs::write(&path, "token-v1").unwrap();

        let client = make_test_client("", Some(path.clone()));
        let req = client.client.get("http://example.com");
        let _ = client.auth(req);
        assert_eq!(cached_token(&client).as_deref(), Some("token-v1"));

        // Update file, but cache should still serve old value.
        std::fs::write(&path, "token-v2").unwrap();
        let req = client.client.get("http://example.com");
        let _ = client.auth(req);
        assert_eq!(cached_token(&client).as_deref(), Some("token-v1"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn token_file_refreshes_after_ttl() {
        let dir = test_token_dir();
        let path = dir.join("token-refresh");
        std::fs::write(&path, "token-v1").unwrap();

        let client = make_test_client("", Some(path.clone()));
        let req = client.client.get("http://example.com");
        let _ = client.auth(req);

        // Force cache expiry by backdating the timestamp.
        {
            let mut cache = client.token_file_cache.lock().unwrap();
            if let Some((ref mut at, _)) = *cache {
                *at -= TOKEN_FILE_REFRESH + std::time::Duration::from_secs(1);
            }
        }

        std::fs::write(&path, "token-v2").unwrap();
        let req = client.client.get("http://example.com");
        let _ = client.auth(req);
        assert_eq!(cached_token(&client).as_deref(), Some("token-v2"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn token_file_missing_falls_back_to_static() {
        let path = PathBuf::from("/tmp/nonexistent-token-file-test");
        let client = make_test_client("", Some(path));
        let req = client.client.get("http://example.com");
        let _ = client.auth(req);
        assert!(cached_token(&client).is_none());
    }
}
