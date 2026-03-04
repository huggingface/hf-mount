use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::info;
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

use crate::error::{Error, Result};

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

#[derive(Debug, Serialize)]
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
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub size: Option<u64>,
    pub xet_hash: Option<String>,
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
    xet_hash: Option<String>,
    #[serde(default)]
    lfs: Option<LfsInfo>,
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

pub struct HubApiClient {
    client: Client,
    /// Client that does NOT follow redirects — used for HEAD requests where we
    /// need response headers from the Hub (not from the CAS redirect target).
    head_client: Client,
    endpoint: String,
    token: String,
    source: SourceKind,
}

/// Parse a repo ID, extracting the type from an optional prefix.
/// "datasets/user/ds" → (Dataset, "user/ds")
/// "spaces/user/app" → (Space, "user/app")
/// "user/model" → (Model, "user/model")
fn parse_repo_id(repo_id: &str) -> (RepoType, String) {
    if let Some(rest) = repo_id.strip_prefix("datasets/") {
        (RepoType::Dataset, rest.to_string())
    } else if let Some(rest) = repo_id.strip_prefix("spaces/") {
        (RepoType::Space, rest.to_string())
    } else {
        (RepoType::Model, repo_id.to_string())
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
    /// For repos, auto-detects the repo type from the repo_id prefix
    /// (e.g. "datasets/user/ds" → Dataset, "spaces/user/app" → Space, else Model).
    pub fn from_source(endpoint: &str, token: &str, source: SourceKind) -> Arc<Self> {
        let source = match source {
            SourceKind::Repo { repo_id, revision, .. } => {
                let (repo_type, repo_id) = parse_repo_id(&repo_id);
                SourceKind::Repo {
                    repo_id,
                    repo_type,
                    revision,
                }
            }
            other => other,
        };
        let (client, head_client) = make_clients();
        Arc::new(Self {
            client,
            head_client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            source,
        })
    }

    /// Create a client for a HuggingFace bucket.
    pub fn new(endpoint: &str, token: &str, bucket_id: &str) -> Arc<Self> {
        let (client, head_client) = make_clients();
        Arc::new(Self {
            client,
            head_client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            source: SourceKind::Bucket {
                bucket_id: bucket_id.to_string(),
            },
        })
    }

    /// Create a client for a HuggingFace repo (model/dataset/space).
    pub fn new_repo(endpoint: &str, token: &str, repo_id: &str, repo_type: RepoType, revision: &str) -> Arc<Self> {
        let (client, head_client) = make_clients();
        Arc::new(Self {
            client,
            head_client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            source: SourceKind::Repo {
                repo_id: repo_id.to_string(),
                repo_type,
                revision: revision.to_string(),
            },
        })
    }

    pub fn source(&self) -> &SourceKind {
        &self.source
    }

    pub fn is_repo(&self) -> bool {
        matches!(self.source, SourceKind::Repo { .. })
    }

    /// List tree entries at the given prefix (follows `Link` header pagination).
    pub async fn list_tree(&self, prefix: &str) -> Result<Vec<TreeEntry>> {
        match &self.source {
            SourceKind::Bucket { bucket_id } => self.list_tree_bucket(bucket_id, prefix).await,
            SourceKind::Repo {
                repo_id,
                repo_type,
                revision,
            } => self.list_tree_repo(repo_id, *repo_type, revision, prefix).await,
        }
    }

    async fn list_tree_bucket(&self, bucket_id: &str, prefix: &str) -> Result<Vec<TreeEntry>> {
        let mut all_entries = Vec::new();
        let mut url = if prefix.is_empty() {
            format!("{}/api/buckets/{}/tree", self.endpoint, bucket_id)
        } else {
            format!("{}/api/buckets/{}/tree/{}", self.endpoint, bucket_id, prefix)
        };

        loop {
            let resp = self.client.get(&url).bearer_auth(&self.token).send().await?;

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
        // /api/{type}/{id}/tree/{revision}[/{prefix}]
        let mut url = if prefix.is_empty() {
            format!(
                "{}/api/{}/{}/tree/{}",
                self.endpoint,
                repo_type.api_prefix(),
                repo_id,
                revision,
            )
        } else {
            format!(
                "{}/api/{}/{}/tree/{}/{}",
                self.endpoint,
                repo_type.api_prefix(),
                repo_id,
                revision,
                prefix,
            )
        };

        loop {
            let resp = self.client.get(&url).bearer_auth(&self.token).send().await?;

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
                    // Repos don't expose per-file mtime in the tree listing
                    mtime: None,
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
        let url = match &self.source {
            // Buckets: /buckets/{id}/resolve/{path} (no /api/ prefix)
            SourceKind::Bucket { bucket_id } => {
                format!("{}/buckets/{}/resolve/{}", self.endpoint, bucket_id, path)
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
                    path,
                )
            }
        };
        let resp = self.head_client.head(&url).bearer_auth(&self.token).send().await?;

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
        let last_modified = headers
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(Some(HeadFileInfo {
            xet_hash,
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

        let resp = self.client.get(&url).bearer_auth(&self.token).send().await?;

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

        let resp = self.client.get(&url).bearer_auth(&self.token).send().await?;

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

        // Build NDJSON body
        let mut body = String::new();
        for op in ops {
            body.push_str(&serde_json::to_string(op)?);
            body.push('\n');
        }

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
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
    pub async fn download_file_http(&self, path: &str, dest: &Path) -> Result<()> {
        let url = match &self.source {
            SourceKind::Bucket { bucket_id } => {
                format!("{}/buckets/{}/resolve/{}", self.endpoint, bucket_id, path)
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
                    path,
                )
            }
        };

        info!("HTTP download: {} → {:?}", path, dest);

        let resp = self.client.get(&url).bearer_auth(&self.token).send().await?;

        if !resp.status().is_success() {
            return Err(Error::Hub(format!(
                "HTTP download failed for {}: {} {}",
                path,
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        // Stream response body to a temp file, then atomic-rename to dest.
        // This prevents partial/corrupt files from being served after interrupted downloads.
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = dest.with_extension(format!("tmp.{}", std::process::id()));
        let mut file = tokio::fs::File::create(&tmp).await?;
        let mut stream = resp.bytes_stream();
        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        drop(file);
        tokio::fs::rename(&tmp, dest).await?;

        Ok(())
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
        assert_eq!(HubApiClient::mtime_from_str("not-a-date"), UNIX_EPOCH);
        assert_eq!(HubApiClient::mtime_from_http_date("still-not-a-date"), UNIX_EPOCH);
    }

    #[test]
    fn test_mtime_parsers_support_valid_formats() {
        let rfc3339 = HubApiClient::mtime_from_str("2026-02-28T14:52:39Z");
        let http_date = HubApiClient::mtime_from_http_date("Sat, 28 Feb 2026 14:52:39 GMT");
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
}
