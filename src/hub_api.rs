use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

use crate::error::{Error, Result};

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

pub struct HubApiClient {
    client: Client,
    /// Client that does NOT follow redirects — used for HEAD requests where we
    /// need response headers from the Hub (not from the CAS redirect target).
    head_client: Client,
    endpoint: String,
    token: String,
    bucket_id: String,
}

impl HubApiClient {
    pub fn new(endpoint: &str, token: &str, bucket_id: &str) -> Arc<Self> {
        Arc::new(Self {
            client: Client::new(),
            head_client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("failed to build head_client"),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            bucket_id: bucket_id.to_string(),
        })
    }

    pub fn bucket_id(&self) -> &str {
        &self.bucket_id
    }

    /// List tree entries at the given prefix (follows `Link` header pagination).
    pub async fn list_tree(&self, prefix: &str) -> Result<Vec<TreeEntry>> {
        let mut all_entries = Vec::new();
        let mut url = if prefix.is_empty() {
            format!("{}/api/buckets/{}/tree", self.endpoint, self.bucket_id)
        } else {
            format!("{}/api/buckets/{}/tree/{}", self.endpoint, self.bucket_id, prefix)
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

            // Extract next page URL from Link header: <url>; rel="next"
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

    /// Fetch metadata for a single file via HEAD on the resolve endpoint
    /// Returns xet_hash, size, and last_modified from response
    /// headers without downloading the file body.
    /// Returns `None` if 404 (file does not exist remotely).
    pub async fn head_file(&self, path: &str) -> Result<Option<HeadFileInfo>> {
        // Resolve endpoint: /{repoType}/{namespace}/{repo}/resolve/{path}
        // (no /api/ prefix — it's a user-facing route that also serves HEAD)
        let url = format!("{}/buckets/{}/resolve/{}", self.endpoint, self.bucket_id, path);
        let resp = self.head_client.head(&url).bearer_auth(&self.token).send().await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        // Follow 302 redirects — reqwest follows by default, but HEAD on a
        // redirect returns the redirect response itself. We only need headers
        // from the *original* response (X-Xet-Hash, X-Linked-Size, Last-Modified).
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

    /// Get a CAS read token for the bucket.
    pub async fn get_cas_token(&self) -> Result<CasTokenInfo> {
        let url = format!("{}/api/buckets/{}/xet-read-token", self.endpoint, self.bucket_id);

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

    /// Get a CAS write token for the bucket.
    pub async fn get_cas_write_token(&self) -> Result<CasTokenInfo> {
        let url = format!("{}/api/buckets/{}/xet-write-token", self.endpoint, self.bucket_id);

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
        let url = format!("{}/api/buckets/{}/batch", self.endpoint, self.bucket_id);

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

    /// Create a token refresher for this bucket.
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
}
