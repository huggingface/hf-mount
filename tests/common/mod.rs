use std::path::Path;
use std::sync::Arc;

use data::{FileUploadSession, XetFileInfo};
use reqwest::Client;

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

    assert!(
        resp.status().is_success(),
        "whoami failed: {}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("whoami json parse failed");
    body["name"]
        .as_str()
        .expect("whoami: missing 'name' field")
        .to_string()
}

/// Build an Arc<TranslatorConfig> for CAS writes.
pub async fn build_write_config(
    hub: &Arc<hf_mount::hub_api::HubApiClient>,
    bucket_id: &str,
) -> Arc<data::configurations::TranslatorConfig> {
    let write_jwt = hub
        .get_cas_write_token(bucket_id)
        .await
        .expect("get_cas_write_token failed");

    let write_refresher = Arc::new(hf_mount::auth::HubWriteTokenRefresher::new(
        hub.clone(),
        bucket_id.to_string(),
    ));

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
    let mut results = upload_session
        .upload_files(files)
        .await
        .expect("upload_files failed");

    let file_info = results.pop().expect("upload returned no file info");

    upload_session
        .finalize()
        .await
        .expect("finalize failed");

    file_info
}
