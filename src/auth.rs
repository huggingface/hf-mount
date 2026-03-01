use std::sync::Arc;

use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

use crate::hub_api::{CasTokenInfo, HubApiClient};

#[derive(Clone, Copy)]
enum TokenKind {
    Read,
    Write,
}

pub struct HubTokenRefresher {
    hub_client: Arc<HubApiClient>,
    bucket_id: String,
    kind: TokenKind,
}

impl HubTokenRefresher {
    pub fn for_read(hub_client: Arc<HubApiClient>, bucket_id: String) -> Self {
        Self {
            hub_client,
            bucket_id,
            kind: TokenKind::Read,
        }
    }

    pub fn for_write(hub_client: Arc<HubApiClient>, bucket_id: String) -> Self {
        Self {
            hub_client,
            bucket_id,
            kind: TokenKind::Write,
        }
    }

    pub async fn fetch_initial(&self) -> Result<CasTokenInfo, crate::error::Error> {
        match self.kind {
            TokenKind::Read => self.hub_client.get_cas_token(&self.bucket_id).await,
            TokenKind::Write => self.hub_client.get_cas_write_token(&self.bucket_id).await,
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
