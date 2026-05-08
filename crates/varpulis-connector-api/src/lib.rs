//! Shared traits and types for Varpulis connector crates.
//!
//! This crate defines the public API that individual connector crates
//! (e.g., `varpulis-connector-mqtt`) implement against. It contains:
//!
//! - Core connector traits ([`SourceConnector`], [`SinkConnector`])
//! - Managed connector trait ([`ManagedConnector`])
//! - Sink trait and adapter ([`Sink`], [`SinkConnectorAdapter`])
//! - Circuit breaker ([`circuit_breaker`])
//! - Resource limits ([`limits`])
//! - Helper functions ([`helpers`])
//! - Declarative component registration ([`ConnectorFactory`])
//! - Converter trait ([`converter`])

pub mod circuit_breaker;
pub mod component;
pub mod converter;
pub mod helpers;
pub mod limits;
pub mod managed;
pub mod sink;
pub mod types;

// Re-export commonly used items at top level
pub use component::{ConfigParamInfo, ConnectorComponentInfo, ConnectorFactory};
pub use managed::{ConnectorHealthReport, ManagedConnector};
pub use sink::{Sink, SinkConnectorAdapter, SinkError};
pub use types::{
    ConnectorConfig, ConnectorError, ConnectorHealth, EngineOffsetRegistry, SinkConnector,
    SourceConnector,
};

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ConnectorConfig tests ----

    #[test]
    fn test_connector_config_new() {
        let config = ConnectorConfig::new("kafka", "localhost:9092");
        assert_eq!(config.connector_type, "kafka");
        assert_eq!(config.url, "localhost:9092");
        assert!(config.topic.is_none());
        assert!(config.properties.is_empty());
    }

    #[test]
    fn test_connector_config_with_topic() {
        let config = ConnectorConfig::new("kafka", "localhost:9092").with_topic("events");
        assert_eq!(config.topic.as_deref(), Some("events"));
    }

    #[test]
    fn test_connector_config_with_property() {
        let config = ConnectorConfig::new("kafka", "localhost:9092")
            .with_property("group_id", "my-group")
            .with_property("batch_size", "1000");
        assert_eq!(config.properties.len(), 2);
        assert_eq!(config.properties.get("group_id").unwrap(), "my-group");
        assert_eq!(config.properties.get("batch_size").unwrap(), "1000");
    }

    #[test]
    fn test_connector_config_serialization_roundtrip() {
        let config = ConnectorConfig::new("mqtt", "tcp://broker:1883")
            .with_topic("sensors/#")
            .with_property("qos", "1");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ConnectorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.connector_type, "mqtt");
        assert_eq!(deserialized.url, "tcp://broker:1883");
        assert_eq!(deserialized.topic.as_deref(), Some("sensors/#"));
        assert_eq!(deserialized.properties.get("qos").unwrap(), "1");
    }

    // ---- ConnectorHealth tests ----

    #[test]
    fn test_connector_health_healthy() {
        let health = ConnectorHealth::healthy(42);
        assert!(health.healthy);
        assert_eq!(health.message, "ok");
        assert_eq!(health.events_processed, 42);
        assert_eq!(health.errors, 0);
    }

    #[test]
    fn test_connector_health_unhealthy() {
        let health = ConnectorHealth::unhealthy("connection refused");
        assert!(!health.healthy);
        assert_eq!(health.message, "connection refused");
        assert_eq!(health.events_processed, 0);
    }

    // ---- ConnectorError tests ----

    #[test]
    fn test_connector_error_display() {
        let err = ConnectorError::ConnectionFailed("timeout".to_string());
        assert_eq!(format!("{}", err), "Connection failed: timeout");

        let err = ConnectorError::SendFailed("queue full".to_string());
        assert_eq!(format!("{}", err), "Send failed: queue full");

        let err = ConnectorError::ConfigError("missing url".to_string());
        assert_eq!(format!("{}", err), "Configuration error: missing url");

        let err = ConnectorError::NotConnected;
        assert_eq!(format!("{}", err), "Not connected");

        let err = ConnectorError::NotAvailable("grpc".to_string());
        assert_eq!(format!("{}", err), "Connector not available: grpc");
    }

    // ---- CircuitBreaker tests ----

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = circuit_breaker::CircuitBreaker::new(circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: std::time::Duration::from_secs(30),
        });
        assert_eq!(cb.state(), circuit_breaker::State::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = circuit_breaker::CircuitBreaker::new(circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: std::time::Duration::from_mins(1),
        });

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), circuit_breaker::State::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), circuit_breaker::State::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let cb = circuit_breaker::CircuitBreaker::new(circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: std::time::Duration::from_mins(1),
        });

        cb.record_failure();
        cb.record_failure();
        cb.record_success();
        assert_eq!(cb.consecutive_failures(), 0);
        assert_eq!(cb.state(), circuit_breaker::State::Closed);

        // Can sustain more failures now
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), circuit_breaker::State::Closed);
    }

    #[test]
    fn test_circuit_breaker_default_config() {
        let config = circuit_breaker::CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.reset_timeout, std::time::Duration::from_secs(30));
    }

    #[test]
    fn test_circuit_breaker_state_display() {
        assert_eq!(format!("{}", circuit_breaker::State::Closed), "closed");
        assert_eq!(format!("{}", circuit_breaker::State::Open), "open");
        assert_eq!(format!("{}", circuit_breaker::State::HalfOpen), "half_open");
    }

    // ---- ConnectorHealthReport tests ----

    #[test]
    fn test_connector_health_report_default() {
        let report = managed::ConnectorHealthReport::default();
        assert!(report.connected);
        assert!(report.last_error.is_none());
        assert_eq!(report.messages_received, 0);
        assert_eq!(report.circuit_breaker_state, "closed");
    }

    // ---- SinkError tests ----

    #[test]
    fn test_sink_error_other() {
        let err = sink::SinkError::other("custom error message");
        assert_eq!(format!("{}", err), "custom error message");
    }

    #[test]
    fn test_sink_error_from_connector_error() {
        let ce = ConnectorError::SendFailed("test".to_string());
        let se: sink::SinkError = ce.into();
        assert!(format!("{}", se).contains("test"));
    }

    // ---- Limits tests ----

    #[test]
    fn test_limits_are_reasonable() {
        // Verify limits are non-zero and consistent
        assert_ne!(limits::MAX_EVENT_PAYLOAD_BYTES, 0);
        assert_ne!(limits::MAX_FIELDS_PER_EVENT, 0);
        assert_ne!(limits::MAX_STRING_VALUE_BYTES, 0);
        assert_ne!(limits::MAX_JSON_DEPTH, 0);
        assert_ne!(limits::MAX_ARRAY_ELEMENTS, 0);
        // Payload should be >= string value limit
        let payload_max = limits::MAX_EVENT_PAYLOAD_BYTES;
        let string_max = limits::MAX_STRING_VALUE_BYTES;
        assert!(payload_max >= string_max);
    }

    // ---- JSON Converter tests ----

    #[test]
    fn test_json_converter_roundtrip() {
        use converter::Converter;
        let conv = converter::json::JsonConverter;

        let event = varpulis_core::Event::new("TestEvent")
            .with_field("name", varpulis_core::Value::Str("alice".into()))
            .with_field("count", varpulis_core::Value::Int(42));

        let bytes = conv.serialize(&event).unwrap();
        let events = conv.deserialize("TestEvent", &bytes).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type.as_ref(), "TestEvent");
    }

    #[test]
    fn test_json_converter_deserialize_array() {
        use converter::Converter;
        let conv = converter::json::JsonConverter;

        let payload = r#"[{"event_type":"A","x":1},{"event_type":"B","y":2}]"#;
        let events = conv.deserialize("Default", payload.as_bytes()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type.as_ref(), "A");
        assert_eq!(events[1].event_type.as_ref(), "B");
    }

    #[test]
    fn test_json_converter_deserialize_invalid() {
        use converter::Converter;
        let conv = converter::json::JsonConverter;

        let result = conv.deserialize("Ev", b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_json_converter_name() {
        use converter::Converter;
        let conv = converter::json::JsonConverter;
        assert_eq!(conv.name(), "json");
    }

    // ---- helpers::json_to_event tests ----

    #[test]
    fn test_json_to_event_basic() {
        let json = serde_json::json!({"event_type": "Login", "user": "alice"});
        let event = helpers::json_to_event("Login", &json);
        assert_eq!(event.event_type.as_ref(), "Login");
        assert!(event.get("user").is_some());
        // event_type field should be excluded from data fields
        assert!(event.get("event_type").is_none());
    }

    #[test]
    fn test_json_to_value_types() {
        assert_eq!(
            helpers::json_to_value(&serde_json::json!(null)),
            Some(varpulis_core::Value::Null)
        );
        assert_eq!(
            helpers::json_to_value(&serde_json::json!(true)),
            Some(varpulis_core::Value::Bool(true))
        );
        assert_eq!(
            helpers::json_to_value(&serde_json::json!(42)),
            Some(varpulis_core::Value::Int(42))
        );
        assert_eq!(
            helpers::json_to_value(&serde_json::json!(2.78)),
            Some(varpulis_core::Value::Float(2.78))
        );
    }
}
