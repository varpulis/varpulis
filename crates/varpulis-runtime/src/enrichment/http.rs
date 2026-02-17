//! HTTP enrichment provider — performs GET requests and parses JSON responses.

use async_trait::async_trait;
use std::collections::HashMap;
use varpulis_core::Value;

use super::{EnrichmentError, EnrichmentProvider, EnrichmentResult};

/// HTTP-based enrichment provider.
///
/// Performs `GET base_url?key=<encoded_value>` and parses the JSON response,
/// extracting the requested fields.
pub struct HttpEnrichmentProvider {
    base_url: String,
    method: String,
    client: reqwest::Client,
}

impl HttpEnrichmentProvider {
    pub fn new(config: &crate::connector::ConnectorConfig) -> Self {
        let method = config
            .properties
            .get("method")
            .cloned()
            .unwrap_or_else(|| "GET".to_string());
        Self {
            base_url: config.url.clone(),
            method,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl EnrichmentProvider for HttpEnrichmentProvider {
    async fn lookup(
        &self,
        key: &Value,
        fields: &[String],
    ) -> Result<EnrichmentResult, EnrichmentError> {
        let key_str = match key {
            Value::Str(s) => s.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            other => format!("{:?}", other),
        };

        // Use reqwest's query parameter builder for proper URL encoding
        let request_url = reqwest::Url::parse_with_params(&self.base_url, &[("key", &key_str)])
            .map_err(|e| EnrichmentError::Connection(format!("invalid URL: {}", e)))?;

        let response = match self.method.to_uppercase().as_str() {
            "POST" => self
                .client
                .post(request_url)
                .send()
                .await
                .map_err(|e| EnrichmentError::Connection(e.to_string()))?,
            _ => self
                .client
                .get(request_url)
                .send()
                .await
                .map_err(|e| EnrichmentError::Connection(e.to_string()))?,
        };

        if !response.status().is_success() {
            return Err(EnrichmentError::NotFound(format!(
                "HTTP {} for key '{}'",
                response.status(),
                key_str
            )));
        }

        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| EnrichmentError::Parse(e.to_string()))?;

        let mut result_fields = HashMap::new();
        if let serde_json::Value::Object(map) = &body {
            for field in fields {
                if let Some(val) = map.get(field.as_str()) {
                    result_fields.insert(field.clone(), json_to_value(val));
                }
            }
        }

        Ok(EnrichmentResult {
            fields: result_fields,
            cached: false,
        })
    }

    fn provider_name(&self) -> &str {
        "http"
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    // Delegate to the centralized bounded converter
    crate::connector::helpers::json_to_value(v).unwrap_or(Value::Null)
}
