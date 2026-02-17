//! Redis enrichment provider — performs GET or HGETALL lookups.

use async_trait::async_trait;
use std::collections::HashMap;
use varpulis_core::Value;

use super::{EnrichmentError, EnrichmentProvider, EnrichmentResult};

/// Redis-based enrichment provider.
///
/// Performs `GET key` (parses JSON) or `HGETALL key` (maps hash fields)
/// depending on the response type.
pub struct RedisEnrichmentProvider {
    url: String,
}

impl RedisEnrichmentProvider {
    pub fn new(config: &crate::connector::ConnectorConfig) -> Result<Self, String> {
        Ok(Self {
            url: config.url.clone(),
        })
    }
}

#[async_trait]
impl EnrichmentProvider for RedisEnrichmentProvider {
    async fn lookup(
        &self,
        key: &Value,
        fields: &[String],
    ) -> Result<EnrichmentResult, EnrichmentError> {
        use redis::AsyncCommands;

        let client = redis::Client::open(self.url.as_str())
            .map_err(|e| EnrichmentError::Connection(e.to_string()))?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| EnrichmentError::Connection(e.to_string()))?;

        let key_str = match key {
            Value::Str(s) => s.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            other => format!("{:?}", other),
        };

        // Try HGETALL first (for hash keys)
        let hash_result: redis::RedisResult<HashMap<String, String>> = conn.hgetall(&key_str).await;

        if let Ok(hash) = hash_result {
            if !hash.is_empty() {
                let mut result_fields = HashMap::new();
                for field in fields {
                    if let Some(val) = hash.get(field) {
                        result_fields
                            .insert(field.clone(), Value::Str(val.clone().into_boxed_str()));
                    }
                }
                return Ok(EnrichmentResult {
                    fields: result_fields,
                    cached: false,
                });
            }
        }

        // Fall back to GET (string key, parse as JSON)
        let val: redis::RedisResult<String> = conn.get(&key_str).await;
        match val {
            Ok(json_str) => {
                let parsed: serde_json::Value = serde_json::from_str(&json_str)
                    .map_err(|e| EnrichmentError::Parse(e.to_string()))?;

                let mut result_fields = HashMap::new();
                if let serde_json::Value::Object(map) = &parsed {
                    for field in fields {
                        if let Some(v) = map.get(field.as_str()) {
                            result_fields.insert(field.clone(), json_to_value(v));
                        }
                    }
                }

                Ok(EnrichmentResult {
                    fields: result_fields,
                    cached: false,
                })
            }
            Err(_) => Err(EnrichmentError::NotFound(key_str)),
        }
    }

    fn provider_name(&self) -> &str {
        "redis"
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    // Delegate to the centralized bounded converter
    crate::connector::helpers::json_to_value(v).unwrap_or(Value::Null)
}
