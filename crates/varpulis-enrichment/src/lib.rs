//! External connector enrichment for VPL streams.
//!
//! This crate provides the [`EnrichmentProvider`] trait and concrete implementations
//! for HTTP, SQL, and Redis connectors, plus a [`EnrichmentCache`] for TTL-based caching.

mod cache;
mod http;
#[cfg(feature = "redis")]
mod redis_provider;
#[cfg(feature = "database")]
mod sql;

use std::collections::HashMap;

use async_trait::async_trait;
pub use cache::EnrichmentCache;
use varpulis_core::Value;

/// Result of an enrichment lookup.
#[derive(Debug, Clone)]
pub struct EnrichmentResult {
    /// Extracted fields from the response
    pub fields: HashMap<String, Value>,
    /// Whether this result came from cache
    pub cached: bool,
}

/// Error during enrichment lookup.
#[derive(Debug, thiserror::Error)]
pub enum EnrichmentError {
    /// Timeout waiting for enrichment response.
    #[error("timeout after {0}ms")]
    Timeout(u64),
    /// Connection error to enrichment backend.
    #[error("connection error: {0}")]
    Connection(String),
    /// Error parsing enrichment response.
    #[error("parse error: {0}")]
    Parse(String),
    /// Key not found in enrichment backend.
    #[error("not found for key: {0}")]
    NotFound(String),
}

/// Trait for enrichment data providers.
#[async_trait]
pub trait EnrichmentProvider: Send + Sync {
    /// Look up enrichment data by key.
    async fn lookup(
        &self,
        key: &Value,
        fields: &[String],
    ) -> Result<EnrichmentResult, EnrichmentError>;

    /// Provider name for logging.
    fn provider_name(&self) -> &str;
}

/// Create an enrichment provider from a connector configuration.
pub fn create_provider(
    connector_config: &varpulis_connectors::ConnectorConfig,
) -> Result<Box<dyn EnrichmentProvider>, String> {
    match connector_config.connector_type.as_str() {
        "http" => Ok(Box::new(http::HttpEnrichmentProvider::new(
            connector_config,
        ))),
        #[cfg(feature = "database")]
        "database" => Ok(Box::new(sql::SqlEnrichmentProvider::new(connector_config)?)),
        #[cfg(not(feature = "database"))]
        "database" => Err(
            ".enrich() with database connector requires the 'database' feature flag".to_string(),
        ),
        #[cfg(feature = "redis")]
        "redis" => Ok(Box::new(redis_provider::RedisEnrichmentProvider::new(
            connector_config,
        )?)),
        #[cfg(not(feature = "redis"))]
        "redis" => {
            Err(".enrich() with redis connector requires the 'redis' feature flag".to_string())
        }
        other => Err(format!(
            ".enrich() is not supported for connector type '{other}' — use http, database, or redis"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_provider_unknown_type() {
        let config = varpulis_connectors::ConnectorConfig::new("ftp", "ftp://example.com");
        let result = create_provider(&config);
        match result {
            Err(err) => {
                assert!(
                    err.contains("ftp"),
                    "error should mention the unsupported type, got: {err}"
                );
                assert!(
                    err.contains("not supported"),
                    "error should indicate lack of support, got: {err}"
                );
            }
            Ok(_) => panic!("expected Err for unsupported connector type 'ftp'"),
        }
    }

    #[test]
    fn test_enrichment_error_display() {
        let timeout_err = EnrichmentError::Timeout(5000);
        let display = format!("{timeout_err}");
        assert!(display.contains("5000"));
        assert!(display.contains("timeout"));

        let conn_err = EnrichmentError::Connection("refused".to_string());
        let display = format!("{conn_err}");
        assert!(display.contains("refused"));
        assert!(display.contains("connection error"));

        let parse_err = EnrichmentError::Parse("invalid json".to_string());
        let display = format!("{parse_err}");
        assert!(display.contains("invalid json"));
        assert!(display.contains("parse error"));

        let not_found = EnrichmentError::NotFound("user:999".to_string());
        let display = format!("{not_found}");
        assert!(display.contains("user:999"));
        assert!(display.contains("not found"));
    }

    #[test]
    fn test_enrichment_result_fields() {
        let mut fields = HashMap::new();
        fields.insert("city".to_string(), Value::Str("Riga".into()));
        fields.insert("pop".to_string(), Value::Int(614_618));

        let result = EnrichmentResult {
            fields: fields.clone(),
            cached: true,
        };

        assert!(result.cached);
        assert_eq!(result.fields.len(), 2);
        assert_eq!(result.fields.get("city"), Some(&Value::Str("Riga".into())));
        assert_eq!(result.fields.get("pop"), Some(&Value::Int(614_618)));

        // Verify Clone works
        let cloned = result.clone();
        assert_eq!(cloned.cached, result.cached);
        assert_eq!(cloned.fields, result.fields);

        // Verify Debug works
        let debug = format!("{result:?}");
        assert!(debug.contains("EnrichmentResult"));
    }
}
