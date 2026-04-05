//! Core connector types, traits, and error definitions

use async_trait::async_trait;
use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use varpulis_core::Event;

/// Generic connector configuration.
///
/// This struct provides a unified configuration format for all connector types,
/// allowing dynamic connector creation based on configuration files.
///
/// # Fields
///
/// - `connector_type`: The type of connector (`"mqtt"`, `"kafka"`, `"http"`, etc.)
/// - `url`: Connection URL or address
/// - `topic`: Optional topic, channel, or path for the connection
/// - `properties`: Additional key-value properties specific to the connector type
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorConfig {
    /// Connector type identifier (e.g., "mqtt", "kafka", "http", "file")
    pub connector_type: String,
    /// Connection URL or address (format depends on connector type)
    pub url: String,
    /// Topic, channel, queue name, or file path (optional, depends on connector)
    pub topic: Option<String>,
    /// Additional connector-specific properties
    pub properties: IndexMap<String, String>,
}

impl ConnectorConfig {
    /// Create a new connector configuration with the given type and URL.
    pub fn new(connector_type: &str, url: &str) -> Self {
        Self {
            connector_type: connector_type.to_string(),
            url: url.to_string(),
            topic: None,
            properties: IndexMap::new(),
        }
    }

    /// Set the topic, channel, or path for this connector.
    pub fn with_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }

    /// Add a connector-specific property.
    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }
}

/// Trait for source connectors that ingest events from external systems.
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Returns the name/identifier of this connector instance.
    fn name(&self) -> &str;

    /// Start receiving events and forward them to the provided channel.
    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError>;

    /// Stop the connector gracefully.
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Check if the connector is currently running.
    fn is_running(&self) -> bool;

    /// Return the current health status of this connector.
    fn health_check(&self) -> ConnectorHealth {
        if self.is_running() {
            ConnectorHealth::healthy(0)
        } else {
            ConnectorHealth::unhealthy("not running")
        }
    }
}

/// Trait for sink connectors that send events to external systems.
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Returns the name/identifier of this connector instance.
    fn name(&self) -> &str;

    /// Establish connection to the external system.
    async fn connect(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Send an event to the external system.
    async fn send(&self, event: &Event) -> Result<(), ConnectorError>;

    /// Send a batch of events to a specific topic (for dynamic routing).
    async fn send_to_topic(
        &self,
        events: &[std::sync::Arc<Event>],
        _topic: &str,
    ) -> Result<(), ConnectorError> {
        for event in events {
            self.send(event).await?;
        }
        Ok(())
    }

    /// Flush any internally buffered events.
    async fn flush(&self) -> Result<(), ConnectorError>;

    /// Close the connector and release resources.
    async fn close(&self) -> Result<(), ConnectorError>;

    /// Return the current health status of this connector.
    fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth::healthy(0)
    }

    // ---- Two-Phase Commit (2PC) for exactly-once delivery ----

    /// Whether this connector supports exactly-once 2PC semantics.
    fn supports_exactly_once(&self) -> bool {
        false
    }

    /// Begin a new checkpoint epoch.
    async fn begin_epoch(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Pre-commit: finalize data but don't make visible.
    async fn prepare_commit(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Commit: make pre-committed data visible.
    async fn commit(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Abort: discard uncommitted data for the current epoch.
    async fn abort(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Health status returned by connector health checks.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorHealth {
    /// Whether the connector is operational.
    pub healthy: bool,
    /// Human-readable status message.
    pub message: String,
    /// Number of events processed since the connector started.
    pub events_processed: u64,
    /// Number of errors encountered since the connector started.
    pub errors: u64,
}

impl ConnectorHealth {
    /// Create a healthy status.
    pub fn healthy(events_processed: u64) -> Self {
        Self {
            healthy: true,
            message: "ok".to_string(),
            events_processed,
            errors: 0,
        }
    }

    /// Create an unhealthy status with an error message.
    pub fn unhealthy(message: impl Into<String>) -> Self {
        Self {
            healthy: false,
            message: message.into(),
            events_processed: 0,
            errors: 0,
        }
    }
}

/// Errors that can occur during connector operations.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Failed to establish connection to the external system.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Failed to send/publish an event.
    #[error("Send failed: {0}")]
    SendFailed(String),

    /// Failed to receive an event from the source.
    #[error("Receive failed: {0}")]
    ReceiveFailed(String),

    /// Invalid or incomplete configuration.
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Operation attempted on a disconnected connector.
    #[error("Not connected")]
    NotConnected,

    /// Requested connector type is not available.
    #[error("Connector not available: {0}")]
    NotAvailable(String),
}
