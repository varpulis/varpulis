//! External system connectors (MQTT, HTTP, Kafka, etc.)
//!
//! This module provides source and sink abstractions for connecting Varpulis
//! to external systems for event ingestion and output.
//!
//! # Architecture
//!
//! ```text
//! External System ─────> SourceConnector ─────> Engine ─────> SinkConnector ─────> External System
//!   (MQTT broker)          (MqttSource)                        (HttpSink)           (Webhook)
//! ```
//!
//! # Available Connectors
//!
//! | Connector | Feature Flag | Description |
//! |-----------|--------------|-------------|
//! | `MqttSource`/`MqttSink` | `mqtt` | MQTT broker connectivity |
//! | `HttpSink` | default | HTTP webhook output |
//! | `ConsoleSource`/`ConsoleSink` | default | Debug/testing connectors |
//!
//! # Example: MQTT Source
//!
//! ```rust,ignore
//! use varpulis_runtime::connector::{MqttConfig, MqttSource, SourceConnector};
//! use tokio::sync::mpsc;
//!
//! let config = MqttConfig::new("localhost", "events/#")
//!     .with_port(1883)
//!     .with_client_id("my-client");
//!
//! let mut source = MqttSource::new("mqtt-in", config);
//! let (tx, mut rx) = mpsc::channel(100);
//!
//! source.start(tx).await?;
//!
//! while let Some(event) = rx.recv().await {
//!     println!("Received: {:?}", event);
//! }
//! ```
//!
//! # Example: HTTP Sink
//!
//! ```rust,ignore
//! use varpulis_runtime::connector::{HttpSink, SinkConnector};
//! use varpulis_runtime::Event;
//!
//! let sink = HttpSink::new("webhook", "https://example.com/events");
//! let event = Event::new("Alert").with_field("message", "High temperature");
//!
//! sink.send(&event).await?;
//! ```

// Sub-modules
mod console;
mod database;
mod elasticsearch;
pub(crate) mod helpers;
mod http;
mod kafka;
mod kinesis;
mod mqtt;
mod redis;
mod registry;
mod rest_api;
mod s3;
mod types;

// Managed connector abstractions (Phase 2)
mod managed;
#[cfg(feature = "kafka")]
mod managed_kafka;
mod managed_mqtt;
mod managed_registry;

// Re-export everything for backwards compatibility
// (use varpulis_runtime::connector::* still works)

// Core types and traits
pub use types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};

// Console connectors
pub use console::{ConsoleSink, ConsoleSource};

// HTTP connectors
pub use http::{HttpSink, HttpWebhookConfig, HttpWebhookSource};

// Kafka connectors
pub use kafka::{KafkaConfig, KafkaSink, KafkaSource};
#[cfg(feature = "kafka")]
pub use kafka::{KafkaSinkFull, KafkaSourceFull};

// MQTT connectors
pub use mqtt::{MqttConfig, MqttSink, MqttSource};

// Kinesis connectors
pub use kinesis::{KinesisConfig, KinesisSink, KinesisSource};
#[cfg(feature = "kinesis")]
pub use kinesis::{KinesisSinkFull, KinesisSourceFull};

// S3 connectors
#[cfg(feature = "s3")]
pub use s3::S3SinkFull;
pub use s3::{S3Config, S3OutputFormat, S3Sink};

// Elasticsearch connectors
#[cfg(feature = "elasticsearch")]
pub use elasticsearch::ElasticsearchSinkFull;
pub use elasticsearch::{ElasticsearchConfig, ElasticsearchSink};

// REST API connectors
pub use rest_api::{RestApiClient, RestApiConfig, RestApiSink};

// Database connectors
pub use database::{DatabaseConfig, DatabaseSink, DatabaseSource};

// Redis connectors
pub use redis::{RedisConfig, RedisSink, RedisSource};

// Legacy ConnectorRegistry
pub use registry::ConnectorRegistry;

// Managed connector abstractions (Phase 2)
pub use managed::ManagedConnector;
#[cfg(feature = "kafka")]
pub use managed_kafka::ManagedKafkaConnector;
pub use managed_mqtt::ManagedMqttConnector;
pub use managed_registry::ManagedConnectorRegistry;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

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

    #[tokio::test]
    async fn test_create_from_config() {
        let config = ConnectorConfig::new("console", "");
        let sink = ConnectorRegistry::create_from_config(&config).await;
        assert!(sink.is_ok());

        let config = ConnectorConfig::new("unknown", "");
        let sink = ConnectorRegistry::create_from_config(&config).await;
        assert!(sink.is_err());
    }

    // ==========================================================================
    // ConsoleSource Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_console_source_lifecycle() {
        let mut source = ConsoleSource::new("test_console");
        assert_eq!(source.name(), "test_console");
        assert!(!source.is_running());

        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let result = source.start(tx).await;
        assert!(result.is_ok());
        assert!(source.is_running());

        let result = source.stop().await;
        assert!(result.is_ok());
        assert!(!source.is_running());
    }
}
