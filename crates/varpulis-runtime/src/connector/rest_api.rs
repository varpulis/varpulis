//! REST API client and sink connector

use super::helpers::json_to_event;
use super::types::{ConnectorError, SinkConnector};
use crate::event::Event;
use async_trait::async_trait;
use indexmap::IndexMap;

/// REST API client configuration
#[derive(Debug, Clone)]
pub struct RestApiConfig {
    pub base_url: String,
    pub headers: IndexMap<String, String>,
    pub timeout_ms: u64,
    pub retry_count: u32,
}

impl RestApiConfig {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            headers: IndexMap::new(),
            timeout_ms: 5000,
            retry_count: 3,
        }
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_bearer_token(mut self, token: &str) -> Self {
        self.headers
            .insert("Authorization".to_string(), format!("Bearer {}", token));
        self
    }

    pub fn with_api_key(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// REST API client for calling external APIs
pub struct RestApiClient {
    #[allow(dead_code)]
    name: String,
    config: RestApiConfig,
    client: reqwest::Client,
}

impl RestApiClient {
    pub fn new(name: &str, config: RestApiConfig) -> Result<Self, ConnectorError> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (key, value) in &config.headers {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(key.as_bytes())
                    .map_err(|e| ConnectorError::ConfigError(e.to_string()))?,
                reqwest::header::HeaderValue::from_str(value)
                    .map_err(|e| ConnectorError::ConfigError(e.to_string()))?,
            );
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| ConnectorError::ConfigError(e.to_string()))?;

        Ok(Self {
            name: name.to_string(),
            config,
            client,
        })
    }

    /// GET request returning JSON as Event
    pub async fn get(&self, path: &str) -> Result<Event, ConnectorError> {
        let url = format!("{}{}", self.config.base_url, path);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ConnectorError::ReceiveFailed(format!(
                "HTTP {}: {}",
                response.status(),
                url
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        Ok(json_to_event("ApiResponse", &json))
    }

    /// POST request with Event data
    pub async fn post(&self, path: &str, event: &Event) -> Result<Event, ConnectorError> {
        let url = format!("{}{}", self.config.base_url, path);
        let response = self
            .client
            .post(&url)
            .json(event)
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ConnectorError::SendFailed(format!(
                "HTTP {}: {}",
                response.status(),
                url
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        Ok(json_to_event("ApiResponse", &json))
    }

    /// PUT request with Event data
    pub async fn put(&self, path: &str, event: &Event) -> Result<Event, ConnectorError> {
        let url = format!("{}{}", self.config.base_url, path);
        let response = self
            .client
            .put(&url)
            .json(event)
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ConnectorError::SendFailed(format!(
                "HTTP {}: {}",
                response.status(),
                url
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        Ok(json_to_event("ApiResponse", &json))
    }

    /// DELETE request
    pub async fn delete(&self, path: &str) -> Result<(), ConnectorError> {
        let url = format!("{}{}", self.config.base_url, path);
        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ConnectorError::SendFailed(format!(
                "HTTP {}: {}",
                response.status(),
                url
            )));
        }

        Ok(())
    }
}

/// REST API sink that POSTs events to an endpoint
pub struct RestApiSink {
    name: String,
    client: RestApiClient,
    path: String,
}

impl RestApiSink {
    pub fn new(name: &str, config: RestApiConfig, path: &str) -> Result<Self, ConnectorError> {
        Ok(Self {
            name: name.to_string(),
            client: RestApiClient::new(name, config)?,
            path: path.to_string(),
        })
    }
}

#[async_trait]
impl SinkConnector for RestApiSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        self.client.post(&self.path, event).await?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
