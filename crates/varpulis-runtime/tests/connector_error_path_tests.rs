//! Connector error-path tests.
//!
//! All tests run without live services or feature flags. CI-safe.
//! Tests cover: stub error returns, database config validation,
//! REST API header validation, and managed registry error paths.

use varpulis_runtime::connector::*;

// =============================================================================
// Feature-disabled stub error returns
// =============================================================================

#[tokio::test]
async fn test_mqtt_source_stub_returns_not_available() {
    // Only runs when the `mqtt` feature is disabled (default CI build)
    if cfg!(feature = "mqtt") {
        return; // Skip — real implementation doesn't return NotAvailable
    }
    let config = MqttConfig::new("localhost", "test/topic");
    let mut source = MqttSource::new("test", config);
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let result = source.start(tx).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, ConnectorError::NotAvailable(_)),
        "Expected NotAvailable, got: {}",
        err
    );
}

#[tokio::test]
async fn test_mqtt_sink_stub_returns_not_available() {
    if cfg!(feature = "mqtt") {
        return;
    }
    let config = MqttConfig::new("localhost", "test/topic");
    let sink = MqttSink::new("test", config);
    let event = varpulis_runtime::event::Event::new("TestEvent");
    let result = sink.send(&event).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::NotAvailable(_)
    ));
}

#[tokio::test]
async fn test_kafka_source_stub_returns_not_available() {
    if cfg!(feature = "kafka") {
        return;
    }
    let config = KafkaConfig::new("localhost:9092", "test-topic");
    let mut source = KafkaSource::new("test", config);
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let result = source.start(tx).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::NotAvailable(_)
    ));
}

#[tokio::test]
async fn test_kafka_sink_stub_returns_not_available() {
    if cfg!(feature = "kafka") {
        return;
    }
    let config = KafkaConfig::new("localhost:9092", "test-topic");
    let sink = KafkaSink::new("test", config);
    let event = varpulis_runtime::event::Event::new("TestEvent");
    let result = sink.send(&event).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::NotAvailable(_)
    ));
}

#[tokio::test]
async fn test_nats_source_stub_returns_not_available() {
    if cfg!(feature = "nats") {
        return;
    }
    let config = NatsConfig::new("nats://localhost:4222", "test.subject");
    let mut source = NatsSource::new("test", config);
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let result = source.start(tx).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::NotAvailable(_)
    ));
}

#[tokio::test]
async fn test_nats_sink_stub_returns_not_available() {
    if cfg!(feature = "nats") {
        return;
    }
    let config = NatsConfig::new("nats://localhost:4222", "test.subject");
    let sink = NatsSink::new("test", config);
    let event = varpulis_runtime::event::Event::new("TestEvent");
    let result = sink.send(&event).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::NotAvailable(_)
    ));
}

// =============================================================================
// Database config validation (validate_table_name is called in DatabaseConfig::new)
// =============================================================================

#[test]
fn test_database_config_empty_table_name() {
    let result = DatabaseConfig::new("postgres://localhost/test", "");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, ConnectorError::ConfigError(msg) if msg.contains("empty")),
        "Expected 'empty' in error, got: {}",
        err
    );
}

#[test]
fn test_database_config_sql_injection_attempt() {
    // Classic SQL injection: DROP TABLE via semicolon
    let result = DatabaseConfig::new("postgres://localhost/test", "events; DROP TABLE users--");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::ConfigError(_)
    ));

    // SQL injection via quotes
    let result = DatabaseConfig::new("postgres://localhost/test", "events' OR '1'='1");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::ConfigError(_)
    ));
}

#[test]
fn test_database_config_spaces_rejected() {
    let result = DatabaseConfig::new("postgres://localhost/test", "my table");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::ConfigError(_)
    ));
}

#[test]
fn test_database_config_leading_digit_rejected() {
    let result = DatabaseConfig::new("postgres://localhost/test", "123events");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::ConfigError(_)
    ));
}

#[test]
fn test_database_config_schema_qualified_name_ok() {
    // Schema-qualified names like "public.events" are valid
    let result = DatabaseConfig::new("postgres://localhost/test", "public.events");
    assert!(result.is_ok());

    // Underscores valid
    let result = DatabaseConfig::new("postgres://localhost/test", "_my_table");
    assert!(result.is_ok());

    // Simple name valid
    let result = DatabaseConfig::new("postgres://localhost/test", "events");
    assert!(result.is_ok());
}

// =============================================================================
// REST API config validation (header name and value errors)
// =============================================================================

#[test]
fn test_rest_api_client_invalid_header_name_errors() {
    // HTTP header names cannot contain spaces or special characters
    let config =
        RestApiConfig::new("http://localhost:8080").with_header("Invalid Header Name", "value");
    let result = RestApiClient::new("test", config);
    // Can't use expect_err() because RestApiClient doesn't implement Debug
    #[allow(clippy::err_expect)]
    let err = result
        .err()
        .expect("Expected error for invalid header name");
    assert!(
        matches!(&err, ConnectorError::ConfigError(msg) if !msg.is_empty()),
        "Expected ConfigError, got: {}",
        err
    );
}

#[test]
fn test_rest_api_client_invalid_header_value_errors() {
    // HTTP header values cannot contain control characters (e.g., newlines)
    let config = RestApiConfig::new("http://localhost:8080")
        .with_header("X-Custom", "value\r\nInjection: attack");
    let result = RestApiClient::new("test", config);
    // Can't use expect_err() because RestApiClient doesn't implement Debug
    #[allow(clippy::err_expect)]
    let err = result
        .err()
        .expect("Expected error for invalid header value");
    assert!(
        matches!(&err, ConnectorError::ConfigError(msg) if !msg.is_empty()),
        "Expected ConfigError, got: {}",
        err
    );
}

// =============================================================================
// Managed connector registry error paths
// =============================================================================

#[tokio::test]
async fn test_managed_registry_start_source_unknown_connector() {
    let configs = rustc_hash::FxHashMap::default();
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let params = std::collections::HashMap::new();
    let result = registry
        .start_source("nonexistent", "topic", tx, &params)
        .await;
    let err = result.expect_err("Expected error for unknown connector");
    assert!(
        matches!(&err, ConnectorError::ConfigError(msg) if msg.contains("Unknown")),
        "Expected ConfigError with 'Unknown', got: {}",
        err
    );
}

#[test]
fn test_managed_registry_create_sink_unknown_connector() {
    let configs = rustc_hash::FxHashMap::default();
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    let params = std::collections::HashMap::new();
    let result = registry.create_sink("nonexistent", "topic", &params);
    match result {
        Err(ConnectorError::ConfigError(msg)) => {
            assert!(msg.contains("Unknown"), "Expected 'Unknown' in: {}", msg);
        }
        Err(other) => panic!("Expected ConfigError, got: {}", other),
        Ok(_) => panic!("Expected error for unknown connector"),
    }
}

// =============================================================================
// Managed registry: unsupported connector type
// =============================================================================

#[test]
fn test_managed_registry_unsupported_connector_type() {
    let mut configs = rustc_hash::FxHashMap::default();
    configs.insert(
        "my_redis".to_string(),
        ConnectorConfig::new("redis", "redis://localhost:6379"),
    );
    let result = ManagedConnectorRegistry::from_configs(&configs);
    match result {
        Err(ConnectorError::NotAvailable(_)) => {} // expected
        Err(other) => panic!("Expected NotAvailable, got: {}", other),
        Ok(_) => panic!("Expected error for unsupported connector type"),
    }
}

// =============================================================================
// Database source/sink stubs (when database feature disabled)
// =============================================================================

#[tokio::test]
async fn test_database_source_stub_returns_not_available() {
    if cfg!(feature = "database") {
        return;
    }
    let config = DatabaseConfig::new("postgres://localhost/test", "events").unwrap();
    let mut source = DatabaseSource::new("test", config);
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let result = source.start(tx).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::NotAvailable(_)
    ));
}

#[tokio::test]
async fn test_database_sink_stub_returns_not_available() {
    if cfg!(feature = "database") {
        return;
    }
    let config = DatabaseConfig::new("postgres://localhost/test", "events").unwrap();
    let sink = DatabaseSink::new("test", config).await.unwrap();
    let event = varpulis_runtime::event::Event::new("TestEvent");
    let result = sink.send(&event).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::NotAvailable(_)
    ));
}
