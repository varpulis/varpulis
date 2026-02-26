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
mod nats;
pub mod postgres_cdc;
mod pulsar;
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
mod managed_nats;
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

// NATS connectors
pub use nats::{NatsConfig, NatsSink, NatsSource};

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
pub use redis::{RedisStreamConfig, RedisStreamSink, RedisStreamSinkStub, RedisStreamSource};

// PostgreSQL CDC connector
pub use postgres_cdc::{CdcOperation, PostgresCdcConfig, PostgresCdcSource};

// Pulsar connectors
pub use pulsar::{PulsarConfig, PulsarSink, PulsarSource};

// Legacy ConnectorRegistry
pub use registry::ConnectorRegistry;

// Managed connector abstractions (Phase 2)
pub use managed::{ConnectorHealthReport, ManagedConnector};
#[cfg(feature = "kafka")]
pub use managed_kafka::ManagedKafkaConnector;
pub use managed_mqtt::ManagedMqttConnector;
pub use managed_nats::ManagedNatsConnector;
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
    fn test_nats_config() {
        let config = NatsConfig::new("nats://localhost:4222", "events.>")
            .with_queue_group("varpulis")
            .with_credentials("user", "pass");

        assert_eq!(config.servers, "nats://localhost:4222");
        assert_eq!(config.subject, "events.>");
        assert_eq!(config.queue_group, Some("varpulis".to_string()));
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

    // ==========================================================================
    // json_to_event / json_to_value edge cases (helpers module)
    // ==========================================================================

    #[test]
    fn test_json_to_event_max_fields_enforced() {
        // Build JSON with more fields than MAX_FIELDS_PER_EVENT (1024)
        let mut obj = serde_json::Map::new();
        obj.insert("event_type".to_string(), serde_json::json!("Test"));
        for i in 0..1100 {
            obj.insert(format!("field_{}", i), serde_json::json!(i));
        }
        let json = serde_json::Value::Object(obj);

        let event = helpers::json_to_event("Test", &json);
        // Should have at most MAX_FIELDS_PER_EVENT fields (event_type excluded from count)
        assert!(
            event.data.len() <= crate::limits::MAX_FIELDS_PER_EVENT,
            "Expected at most {} fields, got {}",
            crate::limits::MAX_FIELDS_PER_EVENT,
            event.data.len()
        );
    }

    #[test]
    fn test_json_to_value_deep_nesting_returns_none() {
        // Create JSON nested deeper than MAX_JSON_DEPTH (32)
        let mut json = serde_json::json!(42);
        for _ in 0..40 {
            json = serde_json::json!({"nested": json});
        }

        // The top-level call should succeed, but deep nesting returns None for
        // values beyond the depth limit
        let result = helpers::json_to_value(&json);
        assert!(result.is_some(), "Top-level should parse");

        // Drill into the result — at depth 32+ the inner values become Null
        // (since json_to_value_bounded returns None which gets skipped in maps)
        let mut current = result.unwrap();
        let mut depth = 0;
        while let varpulis_core::Value::Map(map) = current {
            if let Some(inner) = map.get("nested") {
                current = inner.clone();
                depth += 1;
            } else {
                break;
            }
        }
        // Should stop before reaching the full 40 levels
        assert!(
            depth < 40,
            "Depth limiting should prevent full 40-level nesting, stopped at {}",
            depth
        );
    }

    #[test]
    fn test_json_to_value_long_string_truncated() {
        // Create a string longer than MAX_STRING_VALUE_BYTES (256 KB)
        let long_string = "a".repeat(crate::limits::MAX_STRING_VALUE_BYTES + 1000);
        let json = serde_json::json!(long_string);

        let result = helpers::json_to_value(&json);
        assert!(result.is_some());
        if let varpulis_core::Value::Str(s) = result.unwrap() {
            assert!(
                s.len() <= crate::limits::MAX_STRING_VALUE_BYTES,
                "String should be truncated to {} bytes, got {}",
                crate::limits::MAX_STRING_VALUE_BYTES,
                s.len()
            );
        } else {
            panic!("Expected Str value");
        }
    }

    #[test]
    fn test_json_to_value_large_array_capped() {
        // Create array with more elements than MAX_ARRAY_ELEMENTS (10_000)
        let arr: Vec<serde_json::Value> = (0..11_000).map(|i| serde_json::json!(i)).collect();
        let json = serde_json::Value::Array(arr);

        let result = helpers::json_to_value(&json);
        assert!(result.is_some());
        if let varpulis_core::Value::Array(values) = result.unwrap() {
            assert!(
                values.len() <= crate::limits::MAX_ARRAY_ELEMENTS,
                "Array should be capped at {} elements, got {}",
                crate::limits::MAX_ARRAY_ELEMENTS,
                values.len()
            );
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_json_to_value_null_and_mixed_types() {
        // Null
        let result = helpers::json_to_value(&serde_json::json!(null));
        assert!(matches!(result, Some(varpulis_core::Value::Null)));

        // Bool
        let result = helpers::json_to_value(&serde_json::json!(true));
        assert!(matches!(result, Some(varpulis_core::Value::Bool(true))));

        // Integer
        let result = helpers::json_to_value(&serde_json::json!(42));
        assert!(matches!(result, Some(varpulis_core::Value::Int(42))));

        // Float
        let result = helpers::json_to_value(&serde_json::json!(1.5));
        if let Some(varpulis_core::Value::Float(f)) = result {
            assert!((f - 1.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected Float value");
        }

        // String
        let result = helpers::json_to_value(&serde_json::json!("hello"));
        if let Some(varpulis_core::Value::Str(s)) = result {
            assert_eq!(&*s, "hello");
        } else {
            panic!("Expected Str value");
        }
    }
}
