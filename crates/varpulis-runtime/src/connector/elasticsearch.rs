//! Elasticsearch sink connector

use super::types::{ConnectorError, SinkConnector};
use crate::event::Event;
use async_trait::async_trait;
use tracing::warn;
use varpulis_core::security::SecretString;

/// Elasticsearch sink configuration
///
/// Configuration for indexing events in Elasticsearch.
///
/// # Example
///
/// ```rust
/// use varpulis_runtime::connector::ElasticsearchConfig;
///
/// let config = ElasticsearchConfig::new("http://localhost:9200", "events")
///     .with_batch_size(100)
///     .with_username("elastic")
///     .with_password("changeme");
/// ```
#[derive(Debug, Clone)]
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
    pub fn with_flush_interval(mut self, ms: u64) -> Self {
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

/// Elasticsearch sink connector (stub implementation)
pub struct ElasticsearchSink {
    name: String,
    config: ElasticsearchConfig,
}

impl ElasticsearchSink {
    /// Create a new Elasticsearch sink
    pub fn new(name: &str, config: ElasticsearchConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[async_trait]
impl SinkConnector for ElasticsearchSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        warn!(
            "Elasticsearch sink {} send (stub) to {}/{} - event: {}",
            self.name, self.config.urls, self.config.index, event.event_type
        );

        Err(ConnectorError::NotAvailable(
            "Elasticsearch connector requires 'elasticsearch' feature. Enable with: cargo build --features elasticsearch".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Elasticsearch with elasticsearch crate feature enabled
// -----------------------------------------------------------------------------
#[cfg(feature = "elasticsearch")]
mod elasticsearch_impl {
    use super::*;
    use elasticsearch::auth::Credentials;
    use elasticsearch::http::request::JsonBody;
    use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
    use elasticsearch::{BulkParts, Elasticsearch};
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::Mutex;
    use tracing::info;

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
    pub struct ElasticsearchSinkImpl {
        name: String,
        config: ElasticsearchConfig,
        client: Elasticsearch,
        buffer: Arc<Mutex<BulkBuffer>>,
    }

    impl ElasticsearchSinkImpl {
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

            // Add authentication if configured
            if let Some(ref api_key) = config.api_key {
                builder =
                    builder.auth(Credentials::ApiKey(api_key.expose().to_string(), "".into()));
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
            // Simple date expansion: replace {yyyy}, {MM}, {dd} with current date
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
            let count = operations.len() / 2; // Each doc has action + source

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
    impl SinkConnector for ElasticsearchSinkImpl {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let mut buffer = self.buffer.lock().await;

            let index = self.expand_index(event);

            // Add index action
            let action = serde_json::json!({
                "index": {
                    "_index": index
                }
            });
            buffer.operations.push(action);

            // Add document source
            let doc = serde_json::to_value(event)
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;
            buffer.operations.push(doc);

            // Check if we should flush
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
}

#[cfg(feature = "elasticsearch")]
pub use elasticsearch_impl::ElasticsearchSinkImpl as ElasticsearchSinkFull;
