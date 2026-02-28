use std::sync::Arc;

use utils::auth::{AuthConfig, TokenInfo, TokenRefresher};
use utils::errors::AuthError;

use crate::hub_api::HubApiClient;

pub struct HubTokenRefresher {
    hub_client: Arc<HubApiClient>,
    bucket_id: String,
}

impl HubTokenRefresher {
    pub fn new(hub_client: Arc<HubApiClient>, bucket_id: String) -> Self {
        Self {
            hub_client,
            bucket_id,
        }
    }
}

#[async_trait::async_trait]
impl TokenRefresher for HubTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let jwt = self
            .hub_client
            .get_cas_token(&self.bucket_id)
            .await
            .map_err(|e| AuthError::TokenRefreshFailure(e.to_string()))?;
        Ok((jwt.access_token, jwt.exp))
    }
}

pub struct HubWriteTokenRefresher {
    hub_client: Arc<HubApiClient>,
    bucket_id: String,
}

impl HubWriteTokenRefresher {
    pub fn new(hub_client: Arc<HubApiClient>, bucket_id: String) -> Self {
        Self {
            hub_client,
            bucket_id,
        }
    }
}

#[async_trait::async_trait]
impl TokenRefresher for HubWriteTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let jwt = self
            .hub_client
            .get_cas_write_token(&self.bucket_id)
            .await
            .map_err(|e| AuthError::TokenRefreshFailure(e.to_string()))?;
        Ok((jwt.access_token, jwt.exp))
    }
}

/// Build an AuthConfig from a CAS token + refresher.
pub fn build_auth_config(
    token: String,
    exp: u64,
    refresher: Arc<dyn TokenRefresher>,
) -> Option<AuthConfig> {
    AuthConfig::maybe_new(Some(token), Some(exp), Some(refresher))
}
