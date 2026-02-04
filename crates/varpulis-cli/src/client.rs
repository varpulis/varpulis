//! HTTP client for interacting with a remote Varpulis server.

use crate::api::{
    DeployPipelineRequest, DeployPipelineResponse, PipelineListResponse, UsageResponse,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },
}

/// Client for the Varpulis REST API.
pub struct VarpulisClient {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl VarpulisClient {
    pub fn new(base_url: &str, api_key: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
        }
    }

    /// Deploy a pipeline to the remote server.
    pub async fn deploy_pipeline(
        &self,
        name: &str,
        source: &str,
    ) -> Result<DeployPipelineResponse, ClientError> {
        let url = format!("{}/api/v1/pipelines", self.base_url);
        let body = DeployPipelineRequest {
            name: name.to_string(),
            source: source.to_string(),
        };
        let resp = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::Api {
                status,
                message: text,
            });
        }

        Ok(resp.json().await?)
    }

    /// List all pipelines for the authenticated tenant.
    pub async fn list_pipelines(&self) -> Result<PipelineListResponse, ClientError> {
        let url = format!("{}/api/v1/pipelines", self.base_url);
        let resp = self
            .client
            .get(&url)
            .header("x-api-key", &self.api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::Api {
                status,
                message: text,
            });
        }

        Ok(resp.json().await?)
    }

    /// Delete a pipeline by ID.
    pub async fn delete_pipeline(&self, pipeline_id: &str) -> Result<(), ClientError> {
        let url = format!("{}/api/v1/pipelines/{}", self.base_url, pipeline_id);
        let resp = self
            .client
            .delete(&url)
            .header("x-api-key", &self.api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::Api {
                status,
                message: text,
            });
        }

        Ok(())
    }

    /// Get usage statistics for the authenticated tenant.
    pub async fn get_usage(&self) -> Result<UsageResponse, ClientError> {
        let url = format!("{}/api/v1/usage", self.base_url);
        let resp = self
            .client
            .get(&url)
            .header("x-api-key", &self.api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::Api {
                status,
                message: text,
            });
        }

        Ok(resp.json().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use varpulis_runtime::tenant::{TenantManager, TenantQuota};
    use warp::Filter;

    async fn start_test_server() -> (String, String) {
        let mut mgr = TenantManager::new();
        let api_key = "test-client-key".to_string();
        mgr.create_tenant("Test".into(), api_key.clone(), TenantQuota::default())
            .unwrap();
        let manager = Arc::new(RwLock::new(mgr));
        let routes = api::api_routes(manager, None).recover(crate::auth::handle_rejection);

        let (addr, server) = warp::serve(routes).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::spawn(server);

        let base_url = format!("http://{}", addr);
        (base_url, api_key)
    }

    #[tokio::test]
    async fn test_client_deploy_and_list() {
        let (base_url, api_key) = start_test_server().await;
        let client = VarpulisClient::new(&base_url, &api_key);

        // Deploy
        let resp = client
            .deploy_pipeline("Test Pipeline", "stream A = Events .where(x > 1)")
            .await
            .unwrap();
        assert_eq!(resp.name, "Test Pipeline");
        assert_eq!(resp.status, "running");

        // List
        let list = client.list_pipelines().await.unwrap();
        assert_eq!(list.total, 1);
        assert_eq!(list.pipelines[0].name, "Test Pipeline");
    }

    #[tokio::test]
    async fn test_client_delete_pipeline() {
        let (base_url, api_key) = start_test_server().await;
        let client = VarpulisClient::new(&base_url, &api_key);

        let resp = client
            .deploy_pipeline("ToDelete", "stream B = Events .where(y > 2)")
            .await
            .unwrap();

        client.delete_pipeline(&resp.id).await.unwrap();

        let list = client.list_pipelines().await.unwrap();
        assert_eq!(list.total, 0);
    }

    #[tokio::test]
    async fn test_client_get_usage() {
        let (base_url, api_key) = start_test_server().await;
        let client = VarpulisClient::new(&base_url, &api_key);

        let usage = client.get_usage().await.unwrap();
        assert_eq!(usage.active_pipelines, 0);
    }

    #[tokio::test]
    async fn test_client_invalid_api_key() {
        let (base_url, _) = start_test_server().await;
        let client = VarpulisClient::new(&base_url, "wrong-key");

        let result = client.list_pipelines().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ClientError::Api { status, .. } => assert_eq!(status, 401),
            other => panic!("Expected Api error, got: {:?}", other),
        }
    }
}
