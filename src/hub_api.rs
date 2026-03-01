use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CasTokenInfo {
    pub cas_url: String,
    pub exp: u64,
    pub access_token: String,
}

pub struct HubApiClient {
    client: Client,
    endpoint: String,
    token: String,
}

impl HubApiClient {
    pub fn new(endpoint: &str, token: &str) -> Self {
        Self {
            client: Client::new(),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
        }
    }

    /// List tree entries at the given prefix (follows `Link` header pagination).
    pub async fn list_tree(&self, bucket_id: &str, prefix: &str) -> Result<Vec<TreeEntry>> {
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

    /// Get a CAS read token for the bucket.
    pub async fn get_cas_token(&self, bucket_id: &str) -> Result<CasTokenInfo> {
        let url = format!("{}/api/buckets/{}/xet-read-token", self.endpoint, bucket_id);

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
    pub async fn get_cas_write_token(&self, bucket_id: &str) -> Result<CasTokenInfo> {
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
    pub async fn batch_operations(&self, bucket_id: &str, ops: &[BatchOp]) -> Result<()> {
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

    pub fn mtime_from_str(s: &str) -> SystemTime {
        chrono::DateTime::parse_from_rfc3339(s)
            .map(|dt: chrono::DateTime<chrono::FixedOffset>| {
                UNIX_EPOCH + std::time::Duration::from_secs(dt.timestamp() as u64)
            })
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
}
