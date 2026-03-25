//! External system connectors (MQTT, HTTP, Kafka, etc.)
//!
//! This crate provides source and sink abstractions for connecting Varpulis
//! to external systems for event ingestion and output.
//!
//! # Architecture
//!
//! ```text
//! External System -----> SourceConnector -----> Engine -----> SinkConnector -----> External System
//!   (MQTT broker)          (MqttSource)                        (HttpSink)           (Webhook)
//! ```

// Core modules (kept in this crate for backward compatibility)
pub mod circuit_breaker;
pub mod component;
mod console;
pub mod converter;
pub mod credentials;
mod database;
mod elasticsearch;
pub mod helpers;
mod http;
mod kafka;
mod kinesis;
pub mod limits;
mod nats;
pub mod postgres_cdc;
mod pulsar;
mod redis;
mod registry;
mod rest_api;
mod s3;
pub mod schema;
pub mod sink;
pub mod types;

// Managed connector abstractions
mod managed;
#[cfg(feature = "kafka")]
mod managed_kafka;
mod managed_nats;
mod managed_registry;

// Core types and traits
// Console connectors
pub use console::{ConsoleSink, ConsoleSource};
// Database connectors
pub use database::{DatabaseConfig, DatabaseSink, DatabaseSource};
// Elasticsearch connectors
#[cfg(feature = "elasticsearch")]
pub use elasticsearch::ElasticsearchSinkFull;
pub use elasticsearch::{ElasticsearchConfig, ElasticsearchSink};
// HTTP connectors
pub use http::{HttpSink, HttpWebhookConfig, HttpWebhookSource};
// Kafka connectors
pub use kafka::{KafkaConfig, KafkaSink, KafkaSource};
#[cfg(feature = "kafka")]
pub use kafka::{KafkaSinkFull, KafkaSourceFull};
// Kinesis connectors
pub use kinesis::{KinesisConfig, KinesisSink, KinesisSource};
#[cfg(feature = "kinesis")]
pub use kinesis::{KinesisSinkFull, KinesisSourceFull};
// Managed connector abstractions
pub use managed::{ConnectorHealthReport, ManagedConnector};
#[cfg(feature = "kafka")]
pub use managed_kafka::ManagedKafkaConnector;
pub use managed_nats::ManagedNatsConnector;
pub use managed_registry::ManagedConnectorRegistry;
// NATS connectors
pub use nats::{NatsConfig, NatsSink, NatsSource};
// PostgreSQL CDC connector
pub use postgres_cdc::{CdcOperation, PostgresCdcConfig, PostgresCdcSource};
// Pulsar connectors
pub use pulsar::{PulsarConfig, PulsarSink, PulsarSource};
// Redis connectors
pub use redis::{RedisConfig, RedisSink, RedisSource};
pub use redis::{RedisStreamConfig, RedisStreamSink, RedisStreamSinkStub, RedisStreamSource};
// Legacy ConnectorRegistry
pub use registry::ConnectorRegistry;
// REST API connectors
pub use rest_api::{RestApiClient, RestApiConfig, RestApiSink};
// S3 connectors
#[cfg(feature = "s3")]
pub use s3::S3SinkFull;
pub use s3::{S3Config, S3OutputFormat, S3Sink};
// Sink trait, error, and adapter
pub use sink::{Sink, SinkConnectorAdapter, SinkError};
pub use types::{ConnectorConfig, ConnectorError, ConnectorHealth, SinkConnector, SourceConnector};
// MQTT connectors (from varpulis-connector-mqtt crate)
#[cfg(feature = "mqtt")]
pub use varpulis_connector_mqtt::{ManagedMqttConnector, MqttConfig, MqttSink, MqttSource};

#[cfg(test)]
mod tests {
    use varpulis_core::Event;

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

    #[cfg(feature = "mqtt")]
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

    #[test]
    fn test_json_to_event_max_fields_enforced() {
        let mut obj = serde_json::Map::new();
        obj.insert("event_type".to_string(), serde_json::json!("Test"));
        for i in 0..1100 {
            obj.insert(format!("field_{i}"), serde_json::json!(i));
        }
        let json = serde_json::Value::Object(obj);

        let event = helpers::json_to_event("Test", &json);
        assert!(
            event.data.len() <= crate::limits::MAX_FIELDS_PER_EVENT,
            "Expected at most {} fields, got {}",
            crate::limits::MAX_FIELDS_PER_EVENT,
            event.data.len()
        );
    }

    #[test]
    fn test_json_to_value_deep_nesting_returns_none() {
        let mut json = serde_json::json!(42);
        for _ in 0..40 {
            json = serde_json::json!({"nested": json});
        }

        let result = helpers::json_to_value(&json);
        assert!(result.is_some(), "Top-level should parse");

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
        assert!(
            depth < 40,
            "Depth limiting should prevent full 40-level nesting, stopped at {depth}"
        );
    }

    #[test]
    fn test_json_to_value_long_string_truncated() {
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
        let result = helpers::json_to_value(&serde_json::json!(null));
        assert!(matches!(result, Some(varpulis_core::Value::Null)));

        let result = helpers::json_to_value(&serde_json::json!(true));
        assert!(matches!(result, Some(varpulis_core::Value::Bool(true))));

        let result = helpers::json_to_value(&serde_json::json!(42));
        assert!(matches!(result, Some(varpulis_core::Value::Int(42))));

        let result = helpers::json_to_value(&serde_json::json!(1.5));
        if let Some(varpulis_core::Value::Float(f)) = result {
            assert!((f - 1.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected Float value");
        }

        let result = helpers::json_to_value(&serde_json::json!("hello"));
        if let Some(varpulis_core::Value::Str(s)) = result {
            assert_eq!(&*s, "hello");
        } else {
            panic!("Expected Str value");
        }
    }

    #[test]
    fn test_json_schema_generation() {
        let schema = schemars::schema_for!(ConnectorConfig);
        let json = serde_json::to_string_pretty(&schema).unwrap();
        assert!(json.contains("connector_type"));
        assert!(json.contains("url"));
        assert!(json.contains("topic"));
    }

    #[test]
    fn test_kafka_config_schema() {
        let schema = schemars::schema_for!(KafkaConfig);
        let json = serde_json::to_string_pretty(&schema).unwrap();
        assert!(json.contains("brokers"));
        assert!(json.contains("topic"));
    }

    #[cfg(feature = "mqtt")]
    #[test]
    fn test_mqtt_config_schema() {
        let schema = schemars::schema_for!(MqttConfig);
        let json = serde_json::to_string_pretty(&schema).unwrap();
        assert!(json.contains("broker"));
        assert!(json.contains("port"));
        assert!(json.contains("password"));
    }
}
