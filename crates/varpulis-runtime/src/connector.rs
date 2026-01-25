//! External system connectors (Kafka, MQTT, HTTP, etc.)
//!
//! Provides source and sink abstractions for connecting to external systems.

use crate::event::Event;
use async_trait::async_trait;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Connector type (kafka, mqtt, http, file, etc.)
    pub connector_type: String,
    /// Connection URL/address
    pub url: String,
    /// Topic/channel/path
    pub topic: Option<String>,
    /// Additional properties
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

/// Trait for source connectors (ingest events from external systems)
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Name of this connector
    fn name(&self) -> &str;

    /// Start receiving events and send them to the channel
    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError>;

    /// Stop the connector
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Check if the connector is running
    fn is_running(&self) -> bool;
}

/// Trait for sink connectors (send events to external systems)
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Name of this connector
    fn name(&self) -> &str;

    /// Send an event to the external system
    async fn send(&self, event: &Event) -> Result<(), ConnectorError>;

    /// Flush any buffered events
    async fn flush(&self) -> Result<(), ConnectorError>;

    /// Close the connector
    async fn close(&self) -> Result<(), ConnectorError>;
}

/// Connector errors
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Send failed: {0}")]
    SendFailed(String),

    #[error("Receive failed: {0}")]
    ReceiveFailed(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Not connected")]
    NotConnected,

    #[error("Connector not available: {0}")]
    NotAvailable(String),
}

// =============================================================================
// Console Connector (for testing/debugging)
// =============================================================================

/// Console source - reads events from stdin (for testing)
pub struct ConsoleSource {
    name: String,
    running: bool,
}

impl ConsoleSource {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            running: false,
        }
    }
}

#[async_trait]
impl SourceConnector for ConsoleSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        self.running = true;
        info!("Console source started: {}", self.name);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running = false;
        info!("Console source stopped: {}", self.name);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }
}

/// Console sink - writes events to stdout
pub struct ConsoleSink {
    name: String,
    pretty: bool,
}

impl ConsoleSink {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            pretty: true,
        }
    }

    pub fn compact(mut self) -> Self {
        self.pretty = false;
        self
    }
}

#[async_trait]
impl SinkConnector for ConsoleSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        if self.pretty {
            println!(
                "[{}] {} | {:?}",
                event.timestamp.format("%H:%M:%S"),
                event.event_type,
                event.data
            );
        } else {
            println!("{}", serde_json::to_string(event).unwrap_or_default());
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// HTTP Webhook Connector
// =============================================================================

/// HTTP webhook sink - sends events to an HTTP endpoint
pub struct HttpSink {
    name: String,
    url: String,
    client: reqwest::Client,
    headers: IndexMap<String, String>,
}

impl HttpSink {
    pub fn new(name: &str, url: &str) -> Self {
        Self {
            name: name.to_string(),
            url: url.to_string(),
            client: reqwest::Client::new(),
            headers: IndexMap::new(),
        }
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }
}

#[async_trait]
impl SinkConnector for HttpSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let mut req = self.client.post(&self.url);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/json");
        req = req.json(event);

        match req.send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    warn!("HTTP sink {} got status {}", self.name, resp.status());
                }
                Ok(())
            }
            Err(e) => {
                error!("HTTP sink {} error: {}", self.name, e);
                Err(ConnectorError::SendFailed(e.to_string()))
            }
        }
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// Kafka Connector (stub - requires rdkafka feature)
// =============================================================================

/// Kafka configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: Option<String>,
    pub properties: IndexMap<String, String>,
}

impl KafkaConfig {
    pub fn new(brokers: &str, topic: &str) -> Self {
        Self {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            group_id: None,
            properties: IndexMap::new(),
        }
    }

    pub fn with_group_id(mut self, group_id: &str) -> Self {
        self.group_id = Some(group_id.to_string());
        self
    }
}

/// Kafka source connector (stub implementation)
pub struct KafkaSource {
    name: String,
    config: KafkaConfig,
    running: bool,
}

impl KafkaSource {
    pub fn new(name: &str, config: KafkaConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: false,
        }
    }
}

#[async_trait]
impl SourceConnector for KafkaSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        // NOTE: Full implementation requires rdkafka crate
        // This is a stub that shows the interface
        warn!(
            "Kafka source {} starting (stub implementation - requires rdkafka feature)",
            self.name
        );
        warn!("  Brokers: {}", self.config.brokers);
        warn!("  Topic: {}", self.config.topic);

        self.running = true;

        // In a real implementation, this would:
        // 1. Create a Kafka consumer
        // 2. Subscribe to the topic
        // 3. Poll for messages and convert to Events
        // 4. Send events through the tx channel

        Err(ConnectorError::NotAvailable(
            "Kafka connector requires 'kafka' feature. Enable with: cargo build --features kafka"
                .to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running = false;
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }
}

/// Kafka sink connector (stub implementation)
pub struct KafkaSink {
    name: String,
    config: KafkaConfig,
}

impl KafkaSink {
    pub fn new(name: &str, config: KafkaConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[async_trait]
impl SinkConnector for KafkaSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        // NOTE: Full implementation requires rdkafka crate
        warn!(
            "Kafka sink {} send (stub) to {}/{} - event: {}",
            self.name, self.config.brokers, self.config.topic, event.event_type
        );

        Err(ConnectorError::NotAvailable(
            "Kafka connector requires 'kafka' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// MQTT Connector (stub - requires rumqttc feature)
// =============================================================================

/// MQTT configuration
#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub broker: String,
    pub port: u16,
    pub topic: String,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl MqttConfig {
    pub fn new(broker: &str, topic: &str) -> Self {
        Self {
            broker: broker.to_string(),
            port: 1883,
            topic: topic.to_string(),
            client_id: None,
            username: None,
            password: None,
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_credentials(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(password.to_string());
        self
    }
}

/// MQTT source connector (stub implementation)
pub struct MqttSource {
    name: String,
    config: MqttConfig,
    running: bool,
}

impl MqttSource {
    pub fn new(name: &str, config: MqttConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: false,
        }
    }
}

#[async_trait]
impl SourceConnector for MqttSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        warn!(
            "MQTT source {} starting (stub implementation - requires mqtt feature)",
            self.name
        );
        warn!("  Broker: {}:{}", self.config.broker, self.config.port);
        warn!("  Topic: {}", self.config.topic);

        self.running = true;

        Err(ConnectorError::NotAvailable(
            "MQTT connector requires 'mqtt' feature. Enable with: cargo build --features mqtt"
                .to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running = false;
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }
}

/// MQTT sink connector (stub implementation)
pub struct MqttSink {
    name: String,
    config: MqttConfig,
}

impl MqttSink {
    pub fn new(name: &str, config: MqttConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[async_trait]
impl SinkConnector for MqttSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        warn!(
            "MQTT sink {} send (stub) to {}:{}/{} - event: {}",
            self.name, self.config.broker, self.config.port, self.config.topic, event.event_type
        );

        Err(ConnectorError::NotAvailable(
            "MQTT connector requires 'mqtt' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// Connector Registry
// =============================================================================

/// Registry of available connectors
pub struct ConnectorRegistry {
    sources: IndexMap<String, Box<dyn SourceConnector>>,
    sinks: IndexMap<String, Box<dyn SinkConnector>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            sources: IndexMap::new(),
            sinks: IndexMap::new(),
        }
    }

    pub fn register_source(&mut self, name: &str, source: Box<dyn SourceConnector>) {
        self.sources.insert(name.to_string(), source);
    }

    pub fn register_sink(&mut self, name: &str, sink: Box<dyn SinkConnector>) {
        self.sinks.insert(name.to_string(), sink);
    }

    pub fn get_source(&mut self, name: &str) -> Option<&mut Box<dyn SourceConnector>> {
        self.sources.get_mut(name)
    }

    pub fn get_sink(&self, name: &str) -> Option<&dyn SinkConnector> {
        self.sinks.get(name).map(|b| b.as_ref())
    }

    /// Create a connector from configuration
    pub fn create_from_config(
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        match config.connector_type.as_str() {
            "console" => Ok(Box::new(ConsoleSink::new("console"))),
            "http" => Ok(Box::new(HttpSink::new("http", &config.url))),
            "kafka" => {
                let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
                Ok(Box::new(KafkaSink::new(
                    "kafka",
                    KafkaConfig::new(&config.url, &topic),
                )))
            }
            "mqtt" => {
                let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
                Ok(Box::new(MqttSink::new(
                    "mqtt",
                    MqttConfig::new(&config.url, &topic),
                )))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_console_sink() {
        let sink = ConsoleSink::new("test");
        let event = Event::new("TestEvent").with_field("value", 42i64);
        assert!(sink.send(&event).await.is_ok());
    }

    #[test]
    fn test_connector_config() {
        let config = ConnectorConfig::new("kafka", "localhost:9092")
            .with_topic("events")
            .with_property("group.id", "test-group");

        assert_eq!(config.connector_type, "kafka");
        assert_eq!(config.url, "localhost:9092");
        assert_eq!(config.topic, Some("events".to_string()));
        assert_eq!(
            config.properties.get("group.id"),
            Some(&"test-group".to_string())
        );
    }

    #[test]
    fn test_kafka_config() {
        let config = KafkaConfig::new("broker:9092", "my-topic").with_group_id("my-group");

        assert_eq!(config.brokers, "broker:9092");
        assert_eq!(config.topic, "my-topic");
        assert_eq!(config.group_id, Some("my-group".to_string()));
    }

    #[test]
    fn test_mqtt_config() {
        let config = MqttConfig::new("mqtt.example.com", "sensors/#")
            .with_port(8883)
            .with_credentials("user", "pass");

        assert_eq!(config.broker, "mqtt.example.com");
        assert_eq!(config.port, 8883);
        assert_eq!(config.topic, "sensors/#");
        assert_eq!(config.username, Some("user".to_string()));
    }

    #[test]
    fn test_registry() {
        let mut registry = ConnectorRegistry::new();
        registry.register_sink("console", Box::new(ConsoleSink::new("console")));
        assert!(registry.get_sink("console").is_some());
        assert!(registry.get_sink("unknown").is_none());
    }

    #[test]
    fn test_create_from_config() {
        let config = ConnectorConfig::new("console", "");
        let sink = ConnectorRegistry::create_from_config(&config);
        assert!(sink.is_ok());

        let config = ConnectorConfig::new("unknown", "");
        let sink = ConnectorRegistry::create_from_config(&config);
        assert!(sink.is_err());
    }
}
