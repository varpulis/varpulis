//! Core connector types, traits, and error definitions

use crate::event::Event;
use async_trait::async_trait;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

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
///
/// # Example
///
/// ```rust
/// use varpulis_runtime::connector::ConnectorConfig;
///
/// let config = ConnectorConfig::new("mqtt", "localhost:1883")
///     .with_topic("sensors/#")
///     .with_property("qos", "1")
///     .with_property("client_id", "varpulis-prod");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub fn new(connector_type: &str, url: &str) -> Self {
        Self {
            connector_type: connector_type.to_string(),
            url: url.to_string(),
            topic: None,
            properties: IndexMap::new(),
        }
    }

    pub fn with_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }

    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }
}

/// Trait for source connectors that ingest events from external systems.
///
/// Source connectors receive events from external systems (MQTT brokers, Kafka,
/// HTTP webhooks, files) and forward them to the Varpulis engine via a channel.
///
/// # Lifecycle
///
/// 1. Create the connector with configuration
/// 2. Call [`start`](Self::start) with a channel sender
/// 3. Connector runs asynchronously, sending events to the channel
/// 4. Call [`stop`](Self::stop) to gracefully shutdown
///
/// # Implementing a Custom Source
///
/// ```rust,ignore
/// use varpulis_runtime::connector::{SourceConnector, ConnectorError};
/// use varpulis_runtime::Event;
/// use async_trait::async_trait;
/// use tokio::sync::mpsc;
///
/// struct MySource {
///     name: String,
///     running: bool,
/// }
///
/// #[async_trait]
/// impl SourceConnector for MySource {
///     fn name(&self) -> &str { &self.name }
///
///     async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
///         self.running = true;
///         // Spawn task to receive events and send to tx
///         Ok(())
///     }
///
///     async fn stop(&mut self) -> Result<(), ConnectorError> {
///         self.running = false;
///         Ok(())
///     }
///
///     fn is_running(&self) -> bool { self.running }
/// }
/// ```
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Returns the name/identifier of this connector instance.
    fn name(&self) -> &str;

    /// Start receiving events and forward them to the provided channel.
    ///
    /// This method should spawn background tasks to receive events from the
    /// external system. Events are sent via the `tx` channel to the engine.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConnectionFailed` if unable to connect.
    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError>;

    /// Stop the connector gracefully.
    ///
    /// This should stop all background tasks and close connections.
    /// Outstanding events may be lost.
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Check if the connector is currently running.
    fn is_running(&self) -> bool;
}

/// Trait for sink connectors that send events to external systems.
///
/// Sink connectors receive events from the Varpulis engine and forward them
/// to external systems (HTTP webhooks, MQTT, Kafka, files, databases).
///
/// # Thread Safety
///
/// Sink connectors must be `Send + Sync` to allow sharing across async tasks.
/// The `send` method takes `&self` to allow concurrent sends.
///
/// # Implementing a Custom Sink
///
/// ```rust,ignore
/// use varpulis_runtime::connector::{SinkConnector, ConnectorError};
/// use varpulis_runtime::Event;
/// use async_trait::async_trait;
///
/// struct MySink {
///     name: String,
///     client: reqwest::Client,
///     url: String,
/// }
///
/// #[async_trait]
/// impl SinkConnector for MySink {
///     fn name(&self) -> &str { &self.name }
///
///     async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
///         self.client.post(&self.url)
///             .json(event)
///             .send()
///             .await
///             .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;
///         Ok(())
///     }
///
///     async fn flush(&self) -> Result<(), ConnectorError> { Ok(()) }
///     async fn close(&self) -> Result<(), ConnectorError> { Ok(()) }
/// }
/// ```
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Returns the name/identifier of this connector instance.
    fn name(&self) -> &str;

    /// Establish connection to the external system.
    ///
    /// Called once after sink creation to establish any necessary connections.
    /// Implementations that connect eagerly in `new()` can return Ok(()) here.
    /// This method may be called multiple times; implementations should handle
    /// reconnection gracefully.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConnectionFailed` if connection cannot be established.
    async fn connect(&mut self) -> Result<(), ConnectorError> {
        // Default: no-op for sinks that connect eagerly
        Ok(())
    }

    /// Send an event to the external system.
    ///
    /// This method should be idempotent if possible. Implementations may
    /// buffer events internally for batching.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::SendFailed` on transmission errors.
    /// Returns `ConnectorError::NotConnected` if not connected.
    async fn send(&self, event: &Event) -> Result<(), ConnectorError>;

    /// Flush any internally buffered events.
    ///
    /// Call this to ensure all events have been transmitted before shutdown.
    async fn flush(&self) -> Result<(), ConnectorError>;

    /// Close the connector and release resources.
    ///
    /// This should flush remaining events and close connections.
    async fn close(&self) -> Result<(), ConnectorError>;
}

/// Errors that can occur during connector operations.
///
/// This enum covers all error conditions for both source and sink connectors,
/// including connection, transmission, and configuration errors.
///
/// # Error Handling Example
///
/// ```rust,ignore
/// use varpulis_runtime::connector::{SinkConnector, ConnectorError};
///
/// async fn send_with_retry(sink: &impl SinkConnector, event: &Event) -> Result<(), ConnectorError> {
///     for attempt in 0..3 {
///         match sink.send(event).await {
///             Ok(()) => return Ok(()),
///             Err(ConnectorError::SendFailed(msg)) if attempt < 2 => {
///                 eprintln!("Retry {}: {}", attempt + 1, msg);
///                 tokio::time::sleep(Duration::from_millis(100)).await;
///             }
///             Err(e) => return Err(e),
///         }
///     }
///     unreachable!()
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Failed to establish connection to the external system.
    /// Contains a description of the connection failure.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Failed to send/publish an event.
    /// This may be a transient error (network) or permanent (invalid event).
    #[error("Send failed: {0}")]
    SendFailed(String),

    /// Failed to receive an event from the source.
    /// May indicate connection loss or message parsing errors.
    #[error("Receive failed: {0}")]
    ReceiveFailed(String),

    /// Invalid or incomplete configuration.
    /// Check the connector documentation for required settings.
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Operation attempted on a disconnected connector.
    /// Call `start()` or `connect()` first.
    #[error("Not connected")]
    NotConnected,

    /// Requested connector type is not available.
    /// May require enabling a feature flag (e.g., `mqtt`, `kafka`).
    #[error("Connector not available: {0}")]
    NotAvailable(String),
}
