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
