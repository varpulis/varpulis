//! External connector enrichment for VPL streams.
//!
//! This module provides the [`EnrichmentProvider`] trait and concrete implementations
//! for HTTP, SQL, and Redis connectors, plus a [`EnrichmentCache`] for TTL-based caching.

mod cache;
mod http;
#[cfg(feature = "redis")]
mod redis_provider;
#[cfg(feature = "database")]
mod sql;

pub use cache::EnrichmentCache;

use async_trait::async_trait;
use std::collections::HashMap;
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
    #[error("timeout after {0}ms")]
    Timeout(u64),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("parse error: {0}")]
    Parse(String),
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
    connector_config: &crate::connector::ConnectorConfig,
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
            ".enrich() is not supported for connector type '{}' — use http, database, or redis",
            other
        )),
    }
}
