//! Elasticsearch sink connector.
//!
//! This crate provides full Elasticsearch connectivity for the Varpulis CEP engine
//! via the `elasticsearch` crate. It includes bulk indexing with configurable
//! batching and flush intervals.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use elasticsearch::auth::Credentials;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::{BulkParts, Elasticsearch};
use tokio::sync::Mutex;
use tracing::info;
use varpulis_connector_api::{
    ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory, SinkConnector,
};
use varpulis_core::security::SecretString;
use varpulis_core::Event;

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

static ES_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "elasticsearch",
    display_name: "Elasticsearch",
    description: "Elasticsearch indexing sink",
    feature_flag: "elasticsearch",
    supports_source: false,
    supports_sink: true,
    supports_managed: false,
    config_params: &[],
};

struct ElasticsearchFactory;

impl ConnectorFactory for ElasticsearchFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &ES_INFO
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let index = config.topic.clone().unwrap_or_else(|| "events".to_string());
        let sink = ElasticsearchSink::new(
            "elasticsearch",
            ElasticsearchConfig::new(&config.url, &index),
        )?;
        Ok(Box::new(sink))
    }
}

inventory::submit! { &ElasticsearchFactory as &dyn ConnectorFactory }

// =============================================================================
// Configuration
// =============================================================================

/// Elasticsearch sink configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ElasticsearchConfig {
    /// Elasticsearch URL(s), comma-separated
    pub urls: String,
    /// Index name or pattern (e.g., "events-{yyyy.MM.dd}")
    pub index: String,
    /// Document type (deprecated in ES 7+, use "_doc")
    pub doc_type: String,
    /// Batch size for bulk indexing
    pub batch_size: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Username for authentication (optional)
    pub username: Option<String>,
    /// Password for authentication (zeroized on drop)
    pub password: Option<SecretString>,
    /// API key for authentication (zeroized on drop)
    pub api_key: Option<SecretString>,
}

impl ElasticsearchConfig {
    /// Create a new Elasticsearch configuration
    pub fn new(urls: &str, index: &str) -> Self {
        Self {
            urls: urls.to_string(),
            index: index.to_string(),
            doc_type: "_doc".to_string(),
            batch_size: 100,
            flush_interval_ms: 1000,
            username: None,
            password: None,
            api_key: None,
        }
    }

    /// Set batch size for bulk indexing
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Set flush interval in milliseconds
    pub const fn with_flush_interval(mut self, ms: u64) -> Self {
        self.flush_interval_ms = ms;
        self
    }

    /// Set username for basic authentication
    pub fn with_username(mut self, username: &str) -> Self {
        self.username = Some(username.to_string());
        self
    }

    /// Set password for basic authentication
    pub fn with_password(mut self, password: &str) -> Self {
        self.password = Some(SecretString::new(password));
        self
    }

    /// Set API key for authentication
    pub fn with_api_key(mut self, key: &str) -> Self {
        self.api_key = Some(SecretString::new(key));
        self
    }
}

// =============================================================================
// Elasticsearch Sink
// =============================================================================

struct BulkBuffer {
    operations: Vec<serde_json::Value>,
    last_flush: Instant,
}

impl BulkBuffer {
    fn new() -> Self {
        Self {
            operations: Vec::with_capacity(200),
            last_flush: Instant::now(),
        }
    }
}

/// Full Elasticsearch sink implementation
pub struct ElasticsearchSink {
    name: String,
    config: ElasticsearchConfig,
    client: Elasticsearch,
    buffer: Arc<Mutex<BulkBuffer>>,
}

impl std::fmt::Debug for ElasticsearchSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticsearchSink")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl ElasticsearchSink {
    /// Create a new Elasticsearch sink
    pub fn new(name: &str, config: ElasticsearchConfig) -> Result<Self, ConnectorError> {
        let url = config
            .urls
            .split(',')
            .next()
            .ok_or_else(|| ConnectorError::ConfigError("No URL provided".into()))?;

        let url = url
            .parse()
            .map_err(|e| ConnectorError::ConfigError(format!("Invalid URL: {}", e)))?;

        let pool = SingleNodeConnectionPool::new(url);
        let mut builder = TransportBuilder::new(pool);

        if let Some(ref api_key) = config.api_key {
            builder = builder.auth(Credentials::ApiKey(api_key.expose().to_string(), "".into()));
        } else if let (Some(ref username), Some(ref password)) =
            (&config.username, &config.password)
        {
            builder = builder.auth(Credentials::Basic(
                username.clone(),
                password.expose().to_string(),
            ));
        }

        let transport = builder
            .build()
            .map_err(|e| ConnectorError::ConfigError(e.to_string()))?;

        let client = Elasticsearch::new(transport);

        Ok(Self {
            name: name.to_string(),
            config,
            client,
            buffer: Arc::new(Mutex::new(BulkBuffer::new())),
        })
    }

    fn expand_index(&self, _event: &Event) -> String {
        let now = chrono::Utc::now();
        self.config
            .index
            .replace("{yyyy}", &now.format("%Y").to_string())
            .replace("{MM}", &now.format("%m").to_string())
            .replace("{dd}", &now.format("%d").to_string())
    }

    async fn flush_buffer(&self, buffer: &mut BulkBuffer) -> Result<(), ConnectorError> {
        if buffer.operations.is_empty() {
            return Ok(());
        }

        let operations = std::mem::take(&mut buffer.operations);
        let count = operations.len() / 2;

        let body: Vec<JsonBody<_>> = operations.into_iter().map(JsonBody::new).collect();

        let response = self
            .client
            .bulk(BulkParts::None)
            .body(body)
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        if !response.status_code().is_success() {
            return Err(ConnectorError::SendFailed(format!(
                "Bulk request failed: {}",
                response.status_code()
            )));
        }

        info!(
            "Elasticsearch sink {} indexed {} documents",
            self.name, count
        );

        buffer.last_flush = Instant::now();
        Ok(())
    }
}

#[async_trait]
impl SinkConnector for ElasticsearchSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let mut buffer = self.buffer.lock().await;

        let index = self.expand_index(event);

        let action = serde_json::json!({
            "index": {
                "_index": index
            }
        });
        buffer.operations.push(action);

        let doc =
            serde_json::to_value(event).map_err(|e| ConnectorError::SendFailed(e.to_string()))?;
        buffer.operations.push(doc);

        let should_flush = buffer.operations.len() >= self.config.batch_size * 2
            || buffer.last_flush.elapsed().as_millis() as u64 >= self.config.flush_interval_ms;

        if should_flush {
            self.flush_buffer(&mut buffer).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        let mut buffer = self.buffer.lock().await;
        self.flush_buffer(&mut buffer).await
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elasticsearch_config_new() {
        let config = ElasticsearchConfig::new("http://localhost:9200", "events");
        assert_eq!(config.urls, "http://localhost:9200");
        assert_eq!(config.index, "events");
        assert_eq!(config.doc_type, "_doc");
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.flush_interval_ms, 1000);
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.api_key.is_none());
    }

    #[test]
    fn test_elasticsearch_config_with_batch_size() {
        let config = ElasticsearchConfig::new("http://es:9200", "idx").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn test_elasticsearch_config_batch_size_minimum_one() {
        let config = ElasticsearchConfig::new("http://es:9200", "idx").with_batch_size(0);
        assert_eq!(config.batch_size, 1);
    }

    #[test]
    fn test_elasticsearch_config_with_flush_interval() {
        let config = ElasticsearchConfig::new("http://es:9200", "idx").with_flush_interval(5000);
        assert_eq!(config.flush_interval_ms, 5000);
    }

    #[test]
    fn test_elasticsearch_config_with_basic_auth() {
        let config = ElasticsearchConfig::new("http://es:9200", "idx")
            .with_username("elastic")
            .with_password("changeme");
        assert_eq!(config.username.as_deref(), Some("elastic"));
        assert_eq!(config.password.as_ref().unwrap().expose(), "changeme");
    }

    #[test]
    fn test_elasticsearch_config_with_api_key() {
        let config =
            ElasticsearchConfig::new("http://es:9200", "idx").with_api_key("base64encodedkey");
        assert_eq!(
            config.api_key.as_ref().unwrap().expose(),
            "base64encodedkey"
        );
    }

    #[test]
    fn test_elasticsearch_config_serialization_roundtrip() {
        let config = ElasticsearchConfig::new("http://es:9200", "events-{yyyy.MM.dd}")
            .with_batch_size(200)
            .with_username("user");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ElasticsearchConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.urls, "http://es:9200");
        assert_eq!(deserialized.index, "events-{yyyy.MM.dd}");
        assert_eq!(deserialized.batch_size, 200);
        assert_eq!(deserialized.username.as_deref(), Some("user"));
    }

    #[test]
    fn test_elasticsearch_sink_new_valid_url() {
        let config = ElasticsearchConfig::new("http://localhost:9200", "events");
        let result = ElasticsearchSink::new("es-sink", config);
        assert!(result.is_ok());
        let sink = result.unwrap();
        assert_eq!(sink.name, "es-sink");
    }

    #[test]
    fn test_elasticsearch_sink_new_invalid_url() {
        let config = ElasticsearchConfig::new("not a valid url", "events");
        let result = ElasticsearchSink::new("es-sink", config);
        assert!(result.is_err());
    }

    #[test]
    fn test_elasticsearch_sink_expand_index_static() {
        let config = ElasticsearchConfig::new("http://es:9200", "my-index");
        let sink = ElasticsearchSink::new("test", config).unwrap();
        let event = varpulis_core::Event::new("TestEvent");
        // Static index name should be unchanged
        assert_eq!(sink.expand_index(&event), "my-index");
    }

    #[test]
    fn test_elasticsearch_info_static() {
        assert_eq!(ES_INFO.connector_type, "elasticsearch");
        assert!(!ES_INFO.supports_source);
        assert!(ES_INFO.supports_sink);
        assert!(!ES_INFO.supports_managed);
    }
}
