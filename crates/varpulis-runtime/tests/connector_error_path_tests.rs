//! Connector error-path tests.
//!
//! Tests cover: database config validation, REST API header validation,
//! and managed registry error paths. Stub tests for feature-gated connectors
//! are only compiled when the corresponding feature is enabled.

use varpulis_runtime::connector::*;

// =============================================================================
// Database config validation (validate_table_name is called in DatabaseConfig::new)
// =============================================================================

#[cfg(feature = "database")]
#[test]
fn test_database_config_empty_table_name() {
    let result = DatabaseConfig::new("postgres://localhost/test", "");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, ConnectorError::ConfigError(msg) if msg.contains("empty")),
        "Expected 'empty' in error, got: {err}"
    );
}

#[cfg(feature = "database")]
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

#[cfg(feature = "database")]
#[test]
fn test_database_config_spaces_rejected() {
    let result = DatabaseConfig::new("postgres://localhost/test", "my table");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::ConfigError(_)
    ));
}

#[cfg(feature = "database")]
#[test]
fn test_database_config_leading_digit_rejected() {
    let result = DatabaseConfig::new("postgres://localhost/test", "123events");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConnectorError::ConfigError(_)
    ));
}

#[cfg(feature = "database")]
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
        "Expected ConfigError, got: {err}"
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
        "Expected ConfigError, got: {err}"
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
        "Expected ConfigError with 'Unknown', got: {err}"
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
            assert!(msg.contains("Unknown"), "Expected 'Unknown' in: {msg}");
        }
        Err(other) => panic!("Expected ConfigError, got: {other}"),
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
        Err(other) => panic!("Expected NotAvailable, got: {other}"),
        Ok(_) => panic!("Expected error for unsupported connector type"),
    }
}
