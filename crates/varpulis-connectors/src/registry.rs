//! Legacy connector registry for source/sink management

use indexmap::IndexMap;

use super::console::ConsoleSink;
use super::rest_api::{RestApiConfig, RestApiSink};
use super::types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};

/// Registry of available connectors
pub struct ConnectorRegistry {
    sources: IndexMap<String, Box<dyn SourceConnector>>,
    sinks: IndexMap<String, Box<dyn SinkConnector>>,
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorRegistry").finish_non_exhaustive()
    }
}

impl ConnectorRegistry {
    /// Create an empty connector registry.
    pub fn new() -> Self {
        Self {
            sources: IndexMap::new(),
            sinks: IndexMap::new(),
        }
    }

    /// Register a source connector under the given name.
    pub fn register_source(&mut self, name: &str, source: Box<dyn SourceConnector>) {
        self.sources.insert(name.to_string(), source);
    }

    /// Register a sink connector under the given name.
    pub fn register_sink(&mut self, name: &str, sink: Box<dyn SinkConnector>) {
        self.sinks.insert(name.to_string(), sink);
    }

    /// Get a mutable reference to a registered source connector by name.
    pub fn get_source(&mut self, name: &str) -> Option<&mut Box<dyn SourceConnector>> {
        self.sources.get_mut(name)
    }

    /// Get a reference to a registered sink connector by name.
    pub fn get_sink(&self, name: &str) -> Option<&dyn SinkConnector> {
        self.sinks.get(name).map(|b| b.as_ref())
    }

    /// Create a connector from configuration.
    ///
    /// Tries the inventory-based `find_factory()` first, then falls back to the
    /// match-arm dispatch for connectors that haven't been migrated yet.
    pub async fn create_from_config(
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        // Try inventory-based factory first
        if let Some(factory) = super::component::find_factory(&config.connector_type) {
            if factory.info().supports_sink {
                match factory.create_sink_connector(config) {
                    Ok(sink) => return Ok(sink),
                    Err(ConnectorError::NotAvailable(_)) => {} // fall through
                    Err(e) => return Err(e),
                }
            }
        }

        // Fallback to match-arm dispatch for legacy types
        match config.connector_type.as_str() {
            "console" => Ok(Box::new(ConsoleSink::new("console"))),
            "rest" | "api" => {
                let path = config
                    .topic
                    .clone()
                    .unwrap_or_else(|| "/events".to_string());
                let api_config = RestApiConfig::new(&config.url);
                Ok(Box::new(RestApiSink::new("rest", api_config, &path)?))
            }
            #[cfg(feature = "redis")]
            "redis" => {
                let channel = config.topic.clone().unwrap_or_else(|| "events".to_string());
                let sink =
                    crate::RedisSink::new("redis", crate::RedisConfig::new(&config.url, &channel))
                        .await?;
                Ok(Box::new(sink))
            }
            #[cfg(feature = "database")]
            "database" | "postgres" | "mysql" | "sqlite" => {
                let table = config.topic.clone().unwrap_or_else(|| "events".to_string());
                let sink = crate::DatabaseSink::new(
                    "database",
                    crate::DatabaseConfig::new(&config.url, &table)?,
                )
                .await?;
                Ok(Box::new(sink))
            }
            #[cfg(feature = "kinesis")]
            "kinesis" => {
                let stream = config.topic.clone().unwrap_or_else(|| "events".to_string());
                let region = config
                    .properties
                    .get("region")
                    .cloned()
                    .unwrap_or_else(|| "us-east-1".to_string());
                let sink =
                    crate::KinesisSink::new("kinesis", crate::KinesisConfig::new(&stream, &region))
                        .await?;
                Ok(Box::new(sink))
            }
            #[cfg(feature = "s3")]
            "s3" => {
                let prefix = config
                    .topic
                    .clone()
                    .unwrap_or_else(|| "events/".to_string());
                let region = config
                    .properties
                    .get("region")
                    .cloned()
                    .unwrap_or_else(|| "us-east-1".to_string());
                let sink =
                    crate::S3Sink::new("s3", crate::S3Config::new(&config.url, &prefix, &region))
                        .await?;
                Ok(Box::new(sink))
            }
            #[cfg(feature = "redis")]
            "redis_stream" => {
                let stream_key = config.topic.clone().unwrap_or_else(|| "events".to_string());
                let sink = crate::RedisStreamSink::new(
                    "redis_stream",
                    crate::RedisStreamConfig::new(&config.url, &stream_key),
                )
                .await?;
                Ok(Box::new(sink))
            }
            _ => Err(ConnectorError::ConfigError(format!(
                "Unknown connector type: {}",
                config.connector_type
            ))),
        }
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
