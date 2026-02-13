//! HTTP client for the Varpulis coordinator API.

use crate::error::CoordinatorError;
use serde_json::Value;

/// HTTP client that proxies requests to the Varpulis coordinator.
#[derive(Clone)]
pub struct CoordinatorClient {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
}

impl CoordinatorClient {
    pub fn new(base_url: String, api_key: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");

        Self {
            client,
            base_url,
            api_key,
        }
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/api/v1/cluster/{}", self.base_url, path)
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(key) => req.header("x-api-key", key),
            None => req,
        }
    }

    async fn check_response(&self, resp: reqwest::Response) -> Result<Value, CoordinatorError> {
        let status = resp.status().as_u16();
        match status {
            200..=299 => Ok(resp.json().await?),
            401 => Err(CoordinatorError::Unauthorized),
            404 => {
                let body: Value = resp.json().await.unwrap_or_default();
                let msg = body["error"].as_str().unwrap_or("Not found").to_string();
                Err(CoordinatorError::NotFound(msg))
            }
            400 => {
                let body: Value = resp.json().await.unwrap_or_default();
                let msg = body["error"].as_str().unwrap_or("Bad request").to_string();
                Err(CoordinatorError::BadRequest(msg))
            }
            _ => {
                let body = resp.text().await.unwrap_or_default();
                Err(CoordinatorError::ApiError {
                    status,
                    message: body,
                })
            }
        }
    }

    async fn get(&self, path: &str) -> Result<Value, CoordinatorError> {
        let url = self.api_url(path);
        let resp = self
            .add_auth(self.client.get(&url))
            .send()
            .await
            .map_err(|e| CoordinatorError::Unreachable {
                url: url.clone(),
                source: e,
            })?;
        self.check_response(resp).await
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value, CoordinatorError> {
        let url = self.api_url(path);
        let resp = self
            .add_auth(self.client.post(&url))
            .json(body)
            .send()
            .await
            .map_err(|e| CoordinatorError::Unreachable {
                url: url.clone(),
                source: e,
            })?;
        self.check_response(resp).await
    }

    // ─── Pipeline Groups ─────────────────────────────────────────────

    pub async fn list_pipeline_groups(&self) -> Result<Value, CoordinatorError> {
        self.get("pipeline-groups").await
    }

    pub async fn get_pipeline_group(&self, id: &str) -> Result<Value, CoordinatorError> {
        self.get(&format!("pipeline-groups/{}", id)).await
    }

    pub async fn deploy_pipeline_group(&self, spec: &Value) -> Result<Value, CoordinatorError> {
        self.post("pipeline-groups", spec).await
    }

    // ─── Event Injection ─────────────────────────────────────────────

    pub async fn inject_event(
        &self,
        group_id: &str,
        event: &Value,
    ) -> Result<Value, CoordinatorError> {
        self.post(&format!("pipeline-groups/{}/inject", group_id), event)
            .await
    }

    // ─── Validation ──────────────────────────────────────────────────

    pub async fn validate(&self, source: &str) -> Result<Value, CoordinatorError> {
        self.post("validate", &serde_json::json!({ "source": source }))
            .await
    }

    // ─── Metrics / Health ────────────────────────────────────────────

    pub async fn get_metrics(&self) -> Result<Value, CoordinatorError> {
        self.get("metrics").await
    }

    pub async fn get_summary(&self) -> Result<Value, CoordinatorError> {
        self.get("summary").await
    }

    pub async fn list_workers(&self) -> Result<Value, CoordinatorError> {
        self.get("workers").await
    }

    pub async fn get_topology(&self) -> Result<Value, CoordinatorError> {
        self.get("topology").await
    }

    pub async fn get_scaling(&self) -> Result<Value, CoordinatorError> {
        self.get("scaling").await
    }

    pub async fn list_models(&self) -> Result<Value, CoordinatorError> {
        self.get("models").await
    }

    pub async fn get_prometheus(&self) -> Result<Value, CoordinatorError> {
        // Prometheus endpoint returns text, not JSON
        let url = self.api_url("prometheus");
        let resp = self
            .add_auth(self.client.get(&url))
            .send()
            .await
            .map_err(|e| CoordinatorError::Unreachable {
                url: url.clone(),
                source: e,
            })?;
        let status = resp.status().as_u16();
        if status == 401 {
            return Err(CoordinatorError::Unauthorized);
        }
        let text = resp.text().await?;
        Ok(Value::String(text))
    }
}

/// Check if the coordinator is reachable (best-effort, non-blocking).
pub async fn is_coordinator_reachable(client: &CoordinatorClient) -> bool {
    client.get_summary().await.is_ok()
}
