//! Integration tests for connector registry, connector types, and managed registry.
//!
//! Covers:
//!   - ConnectorRegistry: new, register_source, register_sink, get_source, get_sink, Default
//!   - ConnectorRegistry::create_from_config for every connector type
//!   - ConnectorConfig: construction, builders, defaults, serde round-trip
//!   - ConnectorError: all variants and Display impls
//!   - ManagedConnectorRegistry: from_configs, unknown connector errors, health_reports, shutdown
//!   - ConnectorHealthReport: Default impl

use rustc_hash::FxHashMap;
use varpulis_runtime::connector::{
    ConnectorConfig, ConnectorError, ConnectorHealthReport, ConnectorRegistry, ConsoleSink,
    ConsoleSource, ElasticsearchConfig, ElasticsearchSink, HttpSink, KafkaConfig, KafkaSource,
    KinesisConfig, KinesisSink, ManagedConnectorRegistry, MqttConfig, MqttSink, MqttSource,
    RedisConfig, RedisSink, RedisSource, RestApiConfig, RestApiSink, S3Config, S3Sink,
    SinkConnector, SourceConnector,
};
use varpulis_runtime::event::Event;

// ==========================================================================
// ConnectorRegistry: new / Default
// ==========================================================================

#[test]
fn test_registry_new_is_empty() {
    let registry = ConnectorRegistry::new();
    assert!(registry.get_sink("anything").is_none());
}

#[test]
fn test_registry_default_equals_new() {
    let registry = ConnectorRegistry::default();
    assert!(registry.get_sink("x").is_none());
}

// ==========================================================================
// ConnectorRegistry: register_sink / get_sink
// ==========================================================================

#[test]
fn test_register_and_get_sink() {
    let mut registry = ConnectorRegistry::new();
    registry.register_sink("console", Box::new(ConsoleSink::new("console")));
    let sink = registry.get_sink("console");
    assert!(sink.is_some());
    assert_eq!(sink.unwrap().name(), "console");
}

#[test]
fn test_get_sink_missing_returns_none() {
    let registry = ConnectorRegistry::new();
    assert!(registry.get_sink("nonexistent").is_none());
}

#[test]
fn test_register_multiple_sinks() {
    let mut registry = ConnectorRegistry::new();
    registry.register_sink("console", Box::new(ConsoleSink::new("c1")));
    registry.register_sink("http", Box::new(HttpSink::new("h1", "http://localhost")));

    assert!(registry.get_sink("console").is_some());
    assert!(registry.get_sink("http").is_some());
    assert!(registry.get_sink("kafka").is_none());
}

#[test]
fn test_register_sink_overwrites() {
    let mut registry = ConnectorRegistry::new();
    registry.register_sink("console", Box::new(ConsoleSink::new("first")));
    registry.register_sink("console", Box::new(ConsoleSink::new("second")));

    let sink = registry.get_sink("console").unwrap();
    assert_eq!(sink.name(), "second");
}

// ==========================================================================
// ConnectorRegistry: register_source / get_source
// ==========================================================================

#[test]
fn test_register_and_get_source() {
    let mut registry = ConnectorRegistry::new();
    registry.register_source("console", Box::new(ConsoleSource::new("cs")));
    let source = registry.get_source("console");
    assert!(source.is_some());
    assert_eq!(source.unwrap().name(), "cs");
}

#[test]
fn test_get_source_missing_returns_none() {
    let mut registry = ConnectorRegistry::new();
    assert!(registry.get_source("nope").is_none());
}

#[test]
fn test_register_multiple_sources() {
    let mut registry = ConnectorRegistry::new();
    registry.register_source("console", Box::new(ConsoleSource::new("c1")));
    let kafka_cfg = KafkaConfig::new("broker:9092", "t");
    registry.register_source("kafka", Box::new(KafkaSource::new("k1", kafka_cfg)));

    assert!(registry.get_source("console").is_some());
    assert!(registry.get_source("kafka").is_some());
    assert!(registry.get_source("mqtt").is_none());
}

#[test]
fn test_register_source_overwrites() {
    let mut registry = ConnectorRegistry::new();
    registry.register_source("src", Box::new(ConsoleSource::new("first")));
    registry.register_source("src", Box::new(ConsoleSource::new("second")));

    let src = registry.get_source("src").unwrap();
    assert_eq!(src.name(), "second");
}

// ==========================================================================
// ConnectorRegistry: sources and sinks are independent namespaces
// ==========================================================================

#[test]
fn test_source_and_sink_independent_namespaces() {
    let mut registry = ConnectorRegistry::new();
    registry.register_source("x", Box::new(ConsoleSource::new("source_x")));
    registry.register_sink("x", Box::new(ConsoleSink::new("sink_x")));

    assert_eq!(registry.get_source("x").unwrap().name(), "source_x");
    assert_eq!(registry.get_sink("x").unwrap().name(), "sink_x");
}

// ==========================================================================
// ConnectorRegistry::create_from_config — console
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_console() {
    let config = ConnectorConfig::new("console", "");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    let sink = sink.unwrap();
    assert_eq!(sink.name(), "console");
}

// ==========================================================================
// ConnectorRegistry::create_from_config — http
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_http() {
    let config = ConnectorConfig::new("http", "http://localhost:8080/events");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "http");
}

// ==========================================================================
// ConnectorRegistry::create_from_config — kafka (stub)
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_kafka_default_topic() {
    let config = ConnectorConfig::new("kafka", "broker:9092");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "kafka");
}

#[tokio::test]
async fn test_create_from_config_kafka_custom_topic() {
    let config = ConnectorConfig::new("kafka", "broker:9092").with_topic("my-topic");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "kafka");
}

// ==========================================================================
// ConnectorRegistry::create_from_config — mqtt (stub)
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_mqtt_default_topic() {
    let config = ConnectorConfig::new("mqtt", "localhost");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "mqtt");
}

#[tokio::test]
async fn test_create_from_config_mqtt_custom_topic() {
    let config = ConnectorConfig::new("mqtt", "localhost").with_topic("sensors/#");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
}

// ==========================================================================
// ConnectorRegistry::create_from_config — rest / api
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_rest() {
    let config = ConnectorConfig::new("rest", "http://localhost:3000");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "rest");
}

#[tokio::test]
async fn test_create_from_config_api_alias() {
    let config = ConnectorConfig::new("api", "http://localhost:3000").with_topic("/custom/path");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "rest");
}

// ==========================================================================
// ConnectorRegistry::create_from_config — redis (stub)
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_redis_default_topic() {
    let config = ConnectorConfig::new("redis", "redis://localhost:6379");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "redis");
}

#[tokio::test]
async fn test_create_from_config_redis_custom_topic() {
    let config = ConnectorConfig::new("redis", "redis://localhost:6379").with_topic("my-channel");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
}

// ==========================================================================
// ConnectorRegistry::create_from_config — database (stub)
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_database() {
    let config =
        ConnectorConfig::new("database", "postgres://localhost/test").with_topic("my_table");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "database");
}

#[tokio::test]
async fn test_create_from_config_postgres_alias() {
    let config = ConnectorConfig::new("postgres", "postgres://localhost/test");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
}

#[tokio::test]
async fn test_create_from_config_mysql_alias() {
    let config = ConnectorConfig::new("mysql", "mysql://localhost/test");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
}

#[tokio::test]
async fn test_create_from_config_sqlite_alias() {
    let config = ConnectorConfig::new("sqlite", "sqlite::memory:");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
}

// ==========================================================================
// ConnectorRegistry::create_from_config — kinesis (stub)
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_kinesis_default() {
    let config = ConnectorConfig::new("kinesis", "my-stream");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "kinesis");
}

#[tokio::test]
async fn test_create_from_config_kinesis_custom_region() {
    let config = ConnectorConfig::new("kinesis", "my-stream")
        .with_topic("my-stream-name")
        .with_property("region", "eu-west-1");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
}

// ==========================================================================
// ConnectorRegistry::create_from_config — s3 (stub)
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_s3_default() {
    let config = ConnectorConfig::new("s3", "my-bucket");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "s3");
}

#[tokio::test]
async fn test_create_from_config_s3_custom_prefix_and_region() {
    let config = ConnectorConfig::new("s3", "my-bucket")
        .with_topic("logs/2024/")
        .with_property("region", "ap-southeast-1");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
}

// ==========================================================================
// ConnectorRegistry::create_from_config — elasticsearch / es (stub)
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_elasticsearch() {
    let config = ConnectorConfig::new("elasticsearch", "http://localhost:9200");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "elasticsearch");
}

#[tokio::test]
async fn test_create_from_config_es_alias() {
    let config = ConnectorConfig::new("es", "http://localhost:9200").with_topic("my-index");
    let sink = ConnectorRegistry::create_from_config(&config).await;
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "elasticsearch");
}

// ==========================================================================
// ConnectorRegistry::create_from_config — unknown type
// ==========================================================================

#[tokio::test]
async fn test_create_from_config_unknown_type() {
    let config = ConnectorConfig::new("nosql_magic", "somewhere");
    let result = ConnectorRegistry::create_from_config(&config).await;
    assert!(result.is_err());
    let msg = match result {
        Err(e) => format!("{}", e),
        Ok(_) => panic!("Expected error"),
    };
    assert!(
        msg.contains("Unknown connector type: nosql_magic"),
        "Unexpected error message: {}",
        msg
    );
}

// ==========================================================================
// ConnectorConfig: construction and builders
// ==========================================================================

#[test]
fn test_connector_config_new_defaults() {
    let config = ConnectorConfig::new("mqtt", "localhost:1883");
    assert_eq!(config.connector_type, "mqtt");
    assert_eq!(config.url, "localhost:1883");
    assert!(config.topic.is_none());
    assert!(config.properties.is_empty());
}

#[test]
fn test_connector_config_with_topic() {
    let config = ConnectorConfig::new("kafka", "broker:9092").with_topic("events");
    assert_eq!(config.topic, Some("events".to_string()));
}

#[test]
fn test_connector_config_with_property() {
    let config = ConnectorConfig::new("mqtt", "localhost")
        .with_property("qos", "1")
        .with_property("client_id", "varpulis");
    assert_eq!(config.properties.get("qos"), Some(&"1".to_string()));
    assert_eq!(
        config.properties.get("client_id"),
        Some(&"varpulis".to_string())
    );
}

#[test]
fn test_connector_config_chained_builders() {
    let config = ConnectorConfig::new("mqtt", "localhost")
        .with_topic("sensors/#")
        .with_property("qos", "1")
        .with_property("retain", "false");

    assert_eq!(config.connector_type, "mqtt");
    assert_eq!(config.url, "localhost");
    assert_eq!(config.topic, Some("sensors/#".to_string()));
    assert_eq!(config.properties.len(), 2);
}

#[test]
fn test_connector_config_serde_roundtrip() {
    let original = ConnectorConfig::new("kafka", "broker:9092")
        .with_topic("events")
        .with_property("group.id", "my-group")
        .with_property("auto.offset.reset", "earliest");

    let json = serde_json::to_string(&original).unwrap();
    let deserialized: ConnectorConfig = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.connector_type, original.connector_type);
    assert_eq!(deserialized.url, original.url);
    assert_eq!(deserialized.topic, original.topic);
    assert_eq!(deserialized.properties.len(), original.properties.len());
    assert_eq!(
        deserialized.properties.get("group.id"),
        Some(&"my-group".to_string())
    );
}

#[test]
fn test_connector_config_clone() {
    let config = ConnectorConfig::new("http", "http://example.com")
        .with_topic("/events")
        .with_property("timeout", "5000");

    let cloned = config.clone();
    assert_eq!(cloned.connector_type, config.connector_type);
    assert_eq!(cloned.url, config.url);
    assert_eq!(cloned.topic, config.topic);
    assert_eq!(cloned.properties, config.properties);
}

#[test]
fn test_connector_config_debug() {
    let config = ConnectorConfig::new("console", "");
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("console"));
}

// ==========================================================================
// ConnectorError: all variants and Display
// ==========================================================================

#[test]
fn test_connector_error_connection_failed() {
    let err = ConnectorError::ConnectionFailed("timeout after 5s".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Connection failed: timeout after 5s");
}

#[test]
fn test_connector_error_send_failed() {
    let err = ConnectorError::SendFailed("network unreachable".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Send failed: network unreachable");
}

#[test]
fn test_connector_error_receive_failed() {
    let err = ConnectorError::ReceiveFailed("parse error".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Receive failed: parse error");
}

#[test]
fn test_connector_error_config_error() {
    let err = ConnectorError::ConfigError("missing url".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Configuration error: missing url");
}

#[test]
fn test_connector_error_not_connected() {
    let err = ConnectorError::NotConnected;
    let msg = format!("{}", err);
    assert_eq!(msg, "Not connected");
}

#[test]
fn test_connector_error_not_available() {
    let err = ConnectorError::NotAvailable("kafka requires feature flag".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Connector not available: kafka requires feature flag");
}

#[test]
fn test_connector_error_debug() {
    let err = ConnectorError::NotConnected;
    let debug = format!("{:?}", err);
    assert!(debug.contains("NotConnected"));
}

// ==========================================================================
// SinkConnector default connect() method
// ==========================================================================

#[tokio::test]
async fn test_sink_connector_default_connect() {
    let mut sink = ConsoleSink::new("test");
    // The default connect() implementation is a no-op that returns Ok(())
    let result = sink.connect().await;
    assert!(result.is_ok());
}

// ==========================================================================
// ConsoleSink: send, flush, close
// ==========================================================================

#[tokio::test]
async fn test_console_sink_send_event() {
    let sink = ConsoleSink::new("test-console");
    assert_eq!(sink.name(), "test-console");

    let event = Event::new("TestEvent").with_field("temperature", 25.5);
    let result = sink.send(&event).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_console_sink_compact_mode() {
    let sink = ConsoleSink::new("compact").compact();
    let event = Event::new("CompactEvent").with_field("value", 42i64);
    let result = sink.send(&event).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_console_sink_flush_and_close() {
    let sink = ConsoleSink::new("test");
    assert!(sink.flush().await.is_ok());
    assert!(sink.close().await.is_ok());
}

// ==========================================================================
// ConsoleSource: lifecycle
// ==========================================================================

#[tokio::test]
async fn test_console_source_lifecycle() {
    let mut source = ConsoleSource::new("test-src");
    assert_eq!(source.name(), "test-src");
    assert!(!source.is_running());

    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    assert!(source.start(tx).await.is_ok());
    assert!(source.is_running());

    assert!(source.stop().await.is_ok());
    assert!(!source.is_running());
}

// ==========================================================================
// HttpSink: construction
// ==========================================================================

#[test]
fn test_http_sink_name() {
    let sink = HttpSink::new("my-http", "http://localhost:8080/events");
    assert_eq!(sink.name(), "my-http");
}

#[tokio::test]
async fn test_http_sink_flush_and_close() {
    let sink = HttpSink::new("http-test", "http://localhost:8080");
    assert!(sink.flush().await.is_ok());
    assert!(sink.close().await.is_ok());
}

// ==========================================================================
// KafkaSource and KafkaSink stubs
// ==========================================================================

#[test]
fn test_kafka_source_name() {
    let config = KafkaConfig::new("broker:9092", "events");
    let source = KafkaSource::new("k-src", config);
    assert_eq!(source.name(), "k-src");
    assert!(!source.is_running());
}

#[test]
fn test_kafka_config_builders() {
    let config = KafkaConfig::new("broker:9092", "events")
        .with_group_id("my-group")
        .with_properties(indexmap::IndexMap::new());

    assert_eq!(config.brokers, "broker:9092");
    assert_eq!(config.topic, "events");
    assert_eq!(config.group_id, Some("my-group".to_string()));
}

// ==========================================================================
// MqttConfig builders
// ==========================================================================

#[test]
fn test_mqtt_config_defaults() {
    let config = MqttConfig::new("localhost", "test/#");
    assert_eq!(config.broker, "localhost");
    assert_eq!(config.port, 1883);
    assert_eq!(config.topic, "test/#");
    assert!(config.client_id.is_none());
    assert!(config.username.is_none());
    assert!(config.password.is_none());
    assert_eq!(config.qos, 0);
}

#[test]
fn test_mqtt_config_all_builders() {
    let config = MqttConfig::new("broker.example.com", "sensors/#")
        .with_port(8883)
        .with_client_id("my-client")
        .with_credentials("user", "pass")
        .with_qos(2);

    assert_eq!(config.port, 8883);
    assert_eq!(config.client_id, Some("my-client".to_string()));
    assert_eq!(config.username, Some("user".to_string()));
    assert_eq!(config.password.as_ref().map(|s| s.expose()), Some("pass"));
    assert_eq!(config.qos, 2);
}

#[test]
fn test_mqtt_config_qos_clamped() {
    let config = MqttConfig::new("localhost", "t").with_qos(5);
    assert_eq!(config.qos, 2); // max is 2
}

// ==========================================================================
// RestApiConfig builders
// ==========================================================================

#[test]
fn test_rest_api_config_defaults() {
    let config = RestApiConfig::new("http://api.example.com");
    assert_eq!(config.base_url, "http://api.example.com");
    assert!(config.headers.is_empty());
    assert_eq!(config.timeout_ms, 5000);
    assert_eq!(config.retry_count, 3);
}

#[test]
fn test_rest_api_config_with_bearer_token() {
    let config = RestApiConfig::new("http://api.example.com").with_bearer_token("my-token");
    assert_eq!(
        config.headers.get("Authorization"),
        Some(&"Bearer my-token".to_string())
    );
}

#[test]
fn test_rest_api_config_with_api_key() {
    let config = RestApiConfig::new("http://api.example.com").with_api_key("X-API-Key", "secret");
    assert_eq!(config.headers.get("X-API-Key"), Some(&"secret".to_string()));
}

#[test]
fn test_rest_api_config_with_header_and_timeout() {
    let config = RestApiConfig::new("http://api.example.com")
        .with_header("Accept", "application/json")
        .with_timeout(10000);

    assert_eq!(
        config.headers.get("Accept"),
        Some(&"application/json".to_string())
    );
    assert_eq!(config.timeout_ms, 10000);
}

// ==========================================================================
// RestApiSink: construction (no real server needed)
// ==========================================================================

#[test]
fn test_rest_api_sink_construction() {
    let api_config = RestApiConfig::new("http://localhost:9999");
    let sink = RestApiSink::new("rest-test", api_config, "/events");
    assert!(sink.is_ok());
    assert_eq!(sink.unwrap().name(), "rest-test");
}

// ==========================================================================
// RedisConfig builders
// ==========================================================================

#[test]
fn test_redis_config_defaults() {
    let config = RedisConfig::new("redis://localhost:6379", "events");
    assert_eq!(config.url, "redis://localhost:6379");
    assert_eq!(config.channel, "events");
    assert!(config.key_prefix.is_none());
}

#[test]
fn test_redis_config_with_key_prefix() {
    let config = RedisConfig::new("redis://localhost", "ch").with_key_prefix("myapp");
    assert_eq!(config.key_prefix, Some("myapp".to_string()));
}

// ==========================================================================
// KinesisConfig builders
// ==========================================================================

#[test]
fn test_kinesis_config_defaults() {
    let config = KinesisConfig::new("my-stream", "us-east-1");
    assert_eq!(config.stream_name, "my-stream");
    assert_eq!(config.region, "us-east-1");
    assert_eq!(config.shard_iterator_type, "LATEST");
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.poll_interval_ms, 200);
    assert!(config.partition_key.is_none());
    assert!(config.consumer_name.is_none());
    assert!(config.profile.is_none());
}

#[test]
fn test_kinesis_config_all_builders() {
    let config = KinesisConfig::new("my-stream", "eu-west-1")
        .with_shard_iterator_type("TRIM_HORIZON")
        .with_batch_size(500)
        .with_poll_interval(100)
        .with_partition_key("pk1")
        .with_consumer_name("my-consumer")
        .with_profile("production");

    assert_eq!(config.shard_iterator_type, "TRIM_HORIZON");
    assert_eq!(config.batch_size, 500);
    assert_eq!(config.poll_interval_ms, 100);
    assert_eq!(config.partition_key, Some("pk1".to_string()));
    assert_eq!(config.consumer_name, Some("my-consumer".to_string()));
    assert_eq!(config.profile, Some("production".to_string()));
}

#[test]
fn test_kinesis_config_batch_size_clamped() {
    let config = KinesisConfig::new("s", "r").with_batch_size(20000);
    assert_eq!(config.batch_size, 10000);

    let config2 = KinesisConfig::new("s", "r").with_batch_size(0);
    assert_eq!(config2.batch_size, 1);
}

// ==========================================================================
// S3Config builders
// ==========================================================================

#[test]
fn test_s3_config_defaults() {
    let config = S3Config::new("my-bucket", "events/", "us-east-1");
    assert_eq!(config.bucket, "my-bucket");
    assert_eq!(config.prefix, "events"); // trailing slash is trimmed
    assert_eq!(config.region, "us-east-1");
    assert_eq!(config.file_rotation_size, 100 * 1024 * 1024);
    assert_eq!(config.file_rotation_seconds, 3600);
    assert!(!config.compression);
    assert!(config.profile.is_none());
}

#[test]
fn test_s3_config_all_builders() {
    use varpulis_runtime::connector::S3OutputFormat;

    let config = S3Config::new("bucket", "prefix", "eu-west-1")
        .with_format(S3OutputFormat::Csv)
        .with_file_rotation_size(50 * 1024 * 1024)
        .with_file_rotation_seconds(1800)
        .with_compression()
        .with_profile("staging");

    assert_eq!(config.file_rotation_size, 50 * 1024 * 1024);
    assert_eq!(config.file_rotation_seconds, 1800);
    assert!(config.compression);
    assert_eq!(config.profile, Some("staging".to_string()));
}

// ==========================================================================
// ElasticsearchConfig builders
// ==========================================================================

#[test]
fn test_elasticsearch_config_defaults() {
    let config = ElasticsearchConfig::new("http://localhost:9200", "events");
    assert_eq!(config.urls, "http://localhost:9200");
    assert_eq!(config.index, "events");
    assert_eq!(config.doc_type, "_doc");
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.flush_interval_ms, 1000);
    assert!(config.username.is_none());
    assert!(config.password.is_none());
    assert!(config.api_key.is_none());
}

#[test]
fn test_elasticsearch_config_all_builders() {
    let config = ElasticsearchConfig::new("http://es:9200", "logs")
        .with_batch_size(500)
        .with_flush_interval(2000)
        .with_username("elastic")
        .with_password("changeme")
        .with_api_key("my-api-key");

    assert_eq!(config.batch_size, 500);
    assert_eq!(config.flush_interval_ms, 2000);
    assert_eq!(config.username, Some("elastic".to_string()));
    assert_eq!(
        config.password.as_ref().map(|s| s.expose()),
        Some("changeme")
    );
    assert_eq!(
        config.api_key.as_ref().map(|s| s.expose()),
        Some("my-api-key")
    );
}

#[test]
fn test_elasticsearch_config_batch_size_minimum() {
    let config = ElasticsearchConfig::new("http://es:9200", "idx").with_batch_size(0);
    assert_eq!(config.batch_size, 1); // max(1)
}

// ==========================================================================
// DatabaseConfig builders
// ==========================================================================

#[test]
fn test_database_config_defaults() {
    use varpulis_runtime::connector::DatabaseConfig;
    let config = DatabaseConfig::new("postgres://localhost/test", "events").unwrap();
    assert_eq!(
        config.connection_string.expose(),
        "postgres://localhost/test"
    );
    assert_eq!(config.table, "events");
    assert_eq!(config.max_connections, 5);
}

#[test]
fn test_database_config_with_max_connections() {
    use varpulis_runtime::connector::DatabaseConfig;
    let config = DatabaseConfig::new("postgres://localhost/test", "events")
        .unwrap()
        .with_max_connections(20);
    assert_eq!(config.max_connections, 20);
}

#[test]
fn test_database_config_valid_table_names() {
    use varpulis_runtime::connector::DatabaseConfig;
    // Simple name
    assert!(DatabaseConfig::new("postgres://localhost/test", "events").is_ok());
    // Underscore prefix
    assert!(DatabaseConfig::new("postgres://localhost/test", "_temp").is_ok());
    // Schema-qualified
    assert!(DatabaseConfig::new("postgres://localhost/test", "public.events").is_ok());
    // Mixed case
    assert!(DatabaseConfig::new("postgres://localhost/test", "MyTable_123").is_ok());
}

#[test]
fn test_database_config_invalid_table_names() {
    use varpulis_runtime::connector::DatabaseConfig;
    // Empty
    assert!(DatabaseConfig::new("postgres://localhost/test", "").is_err());
    // SQL injection: semicolon
    assert!(DatabaseConfig::new("postgres://localhost/test", "events; DROP TABLE users").is_err());
    // SQL injection: comment
    assert!(DatabaseConfig::new("postgres://localhost/test", "events--").is_err());
    // Starts with number
    assert!(DatabaseConfig::new("postgres://localhost/test", "123table").is_err());
    // Space
    assert!(DatabaseConfig::new("postgres://localhost/test", "my table").is_err());
    // Parentheses
    assert!(DatabaseConfig::new("postgres://localhost/test", "fn()").is_err());
}

// ==========================================================================
// Stub connectors: send returns NotAvailable errors
// ==========================================================================

#[tokio::test]
async fn test_s3_sink_stub_send_returns_not_available() {
    let config = S3Config::new("bucket", "prefix", "us-east-1");
    let sink = S3Sink::new("s3-test", config);
    assert_eq!(sink.name(), "s3-test");

    let event = Event::new("TestEvent");
    let result = sink.send(&event).await;
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("S3") || msg.contains("s3"));
}

#[tokio::test]
async fn test_s3_sink_stub_flush_and_close_ok() {
    let config = S3Config::new("bucket", "prefix", "us-east-1");
    let sink = S3Sink::new("s3-test", config);
    assert!(sink.flush().await.is_ok());
    assert!(sink.close().await.is_ok());
}

#[tokio::test]
async fn test_elasticsearch_sink_stub_send_returns_not_available() {
    let config = ElasticsearchConfig::new("http://localhost:9200", "events");
    let sink = ElasticsearchSink::new("es-test", config);
    assert_eq!(sink.name(), "es-test");

    let event = Event::new("TestEvent");
    let result = sink.send(&event).await;
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("elasticsearch") || msg.contains("Elasticsearch"));
}

#[tokio::test]
async fn test_elasticsearch_sink_stub_flush_and_close_ok() {
    let config = ElasticsearchConfig::new("http://localhost:9200", "events");
    let sink = ElasticsearchSink::new("es-test", config);
    assert!(sink.flush().await.is_ok());
    assert!(sink.close().await.is_ok());
}

#[tokio::test]
async fn test_kinesis_sink_stub_send_returns_not_available() {
    let config = KinesisConfig::new("stream", "us-east-1");
    let sink = KinesisSink::new("kinesis-test", config);
    assert_eq!(sink.name(), "kinesis-test");

    let event = Event::new("TestEvent");
    let result = sink.send(&event).await;
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("kinesis") || msg.contains("Kinesis"));
}

#[tokio::test]
async fn test_kinesis_sink_stub_flush_and_close_ok() {
    let config = KinesisConfig::new("stream", "us-east-1");
    let sink = KinesisSink::new("kinesis-test", config);
    assert!(sink.flush().await.is_ok());
    assert!(sink.close().await.is_ok());
}

// ==========================================================================
// ManagedConnectorRegistry: from_configs with MQTT
// ==========================================================================

#[test]
fn test_managed_registry_from_configs_mqtt() {
    let mut configs = FxHashMap::default();
    configs.insert(
        "broker".to_string(),
        ConnectorConfig::new("mqtt", "localhost")
            .with_property("port", "1883")
            .with_property("client_id", "test-client"),
    );

    let registry = ManagedConnectorRegistry::from_configs(&configs);
    assert!(registry.is_ok());
}

#[test]
fn test_managed_registry_from_configs_empty() {
    let configs = FxHashMap::default();
    let registry = ManagedConnectorRegistry::from_configs(&configs);
    assert!(registry.is_ok());
}

#[test]
fn test_managed_registry_from_configs_unsupported_type() {
    let mut configs = FxHashMap::default();
    configs.insert(
        "my-ftp".to_string(),
        ConnectorConfig::new("ftp", "ftp://files.example.com"),
    );

    let result = ManagedConnectorRegistry::from_configs(&configs);
    assert!(result.is_err());
    let msg = match result {
        Err(e) => format!("{}", e),
        Ok(_) => panic!("Expected error"),
    };
    assert!(
        msg.contains("ftp"),
        "Error should mention the unsupported type: {}",
        msg
    );
}

// ==========================================================================
// ManagedConnectorRegistry: start_source with unknown connector name
// ==========================================================================

#[tokio::test]
async fn test_managed_registry_start_source_unknown_name() {
    let configs = FxHashMap::default();
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    let result = registry
        .start_source(
            "nonexistent",
            "topic",
            tx,
            &std::collections::HashMap::new(),
        )
        .await;

    assert!(result.is_err());
    let msg = match result {
        Err(e) => format!("{}", e),
        Ok(_) => panic!("Expected error"),
    };
    assert!(
        msg.contains("nonexistent"),
        "Error should mention the unknown connector name: {}",
        msg
    );
}

// ==========================================================================
// ManagedConnectorRegistry: create_sink with unknown connector name
// ==========================================================================

#[test]
fn test_managed_registry_create_sink_unknown_name() {
    let configs = FxHashMap::default();
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    let result = registry.create_sink("nonexistent", "topic", &std::collections::HashMap::new());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => format!("{}", e),
        Ok(_) => panic!("Expected error"),
    };
    assert!(msg.contains("nonexistent"));
}

// ==========================================================================
// ManagedConnectorRegistry: health_reports on empty registry
// ==========================================================================

#[test]
fn test_managed_registry_health_reports_empty() {
    let configs = FxHashMap::default();
    let registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    let reports = registry.health_reports();
    assert!(reports.is_empty());
}

// ==========================================================================
// ManagedConnectorRegistry: health_reports with a connector
// ==========================================================================

#[test]
fn test_managed_registry_health_reports_with_connector() {
    let mut configs = FxHashMap::default();
    configs.insert(
        "broker".to_string(),
        ConnectorConfig::new("mqtt", "localhost"),
    );

    let registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();
    let reports = registry.health_reports();
    assert_eq!(reports.len(), 1);

    let (name, conn_type, health) = &reports[0];
    assert_eq!(*name, "broker");
    assert_eq!(*conn_type, "mqtt");
    // Default health report
    let _ = health.connected; // just verify it's accessible
}

// ==========================================================================
// ManagedConnectorRegistry: shutdown on empty registry
// ==========================================================================

#[tokio::test]
async fn test_managed_registry_shutdown_empty() {
    let configs = FxHashMap::default();
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();
    // Should not panic
    registry.shutdown().await;
}

// ==========================================================================
// ManagedConnectorRegistry: shutdown with a connector
// ==========================================================================

#[tokio::test]
async fn test_managed_registry_shutdown_with_connector() {
    let mut configs = FxHashMap::default();
    configs.insert(
        "broker".to_string(),
        ConnectorConfig::new("mqtt", "localhost"),
    );

    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();
    // Should not panic even though we never connected
    registry.shutdown().await;
}

// ==========================================================================
// ManagedConnectorRegistry: from_configs with MQTT properties
// ==========================================================================

#[test]
fn test_managed_registry_mqtt_properties_propagation() {
    let mut configs = FxHashMap::default();
    configs.insert(
        "broker".to_string(),
        ConnectorConfig::new("mqtt", "mqtt.example.com")
            .with_property("port", "8883")
            .with_property("client_id", "test-client")
            .with_property("qos", "1"),
    );

    let registry = ManagedConnectorRegistry::from_configs(&configs);
    assert!(registry.is_ok());
}

#[test]
fn test_managed_registry_mqtt_invalid_port_ignored() {
    let mut configs = FxHashMap::default();
    configs.insert(
        "broker".to_string(),
        ConnectorConfig::new("mqtt", "localhost").with_property("port", "not_a_number"),
    );

    // Invalid port should be silently ignored, using the default
    let registry = ManagedConnectorRegistry::from_configs(&configs);
    assert!(registry.is_ok());
}

#[test]
fn test_managed_registry_mqtt_invalid_qos_ignored() {
    let mut configs = FxHashMap::default();
    configs.insert(
        "broker".to_string(),
        ConnectorConfig::new("mqtt", "localhost").with_property("qos", "abc"),
    );

    let registry = ManagedConnectorRegistry::from_configs(&configs);
    assert!(registry.is_ok());
}

// ==========================================================================
// ConnectorHealthReport: Default
// ==========================================================================

#[test]
fn test_connector_health_report_default() {
    let report = ConnectorHealthReport::default();
    assert!(report.connected);
    assert!(report.last_error.is_none());
    assert_eq!(report.messages_received, 0);
    assert_eq!(report.seconds_since_last_message, 0);
}

#[test]
fn test_connector_health_report_debug() {
    let report = ConnectorHealthReport::default();
    let debug = format!("{:?}", report);
    assert!(debug.contains("ConnectorHealthReport"));
    assert!(debug.contains("connected: true"));
}

#[test]
fn test_connector_health_report_clone() {
    let report = ConnectorHealthReport {
        connected: false,
        last_error: Some("some error".to_string()),
        messages_received: 42,
        seconds_since_last_message: 10,
    };

    let cloned = report.clone();
    assert!(!cloned.connected);
    assert_eq!(cloned.last_error, Some("some error".to_string()));
    assert_eq!(cloned.messages_received, 42);
    assert_eq!(cloned.seconds_since_last_message, 10);
}

// ==========================================================================
// Stub source connectors: behavior tests
// ==========================================================================

#[tokio::test]
async fn test_redis_source_stub_is_not_running() {
    let config = RedisConfig::new("redis://localhost", "ch");
    let source = RedisSource::new("redis-src", config);
    assert_eq!(source.name(), "redis-src");
    assert!(!source.is_running());
}

#[tokio::test]
async fn test_kinesis_source_stub_name() {
    use varpulis_runtime::connector::KinesisSource;
    let config = KinesisConfig::new("stream", "us-east-1");
    let source = KinesisSource::new("kinesis-src", config);
    assert_eq!(source.name(), "kinesis-src");
    assert!(!source.is_running());
}

// ==========================================================================
// MqttSink and MqttSource stub names
// ==========================================================================

#[test]
fn test_mqtt_source_stub_name() {
    let config = MqttConfig::new("localhost", "t");
    let source = MqttSource::new("mqtt-src", config);
    assert_eq!(source.name(), "mqtt-src");
}

#[test]
fn test_mqtt_sink_stub_name() {
    let config = MqttConfig::new("localhost", "t");
    let sink = MqttSink::new("mqtt-sink", config);
    assert_eq!(sink.name(), "mqtt-sink");
}

// ==========================================================================
// DatabaseSource and DatabaseSink stubs
// ==========================================================================

#[test]
fn test_database_source_stub_name() {
    use varpulis_runtime::connector::{DatabaseConfig, DatabaseSource};
    let config = DatabaseConfig::new("postgres://localhost/test", "events").unwrap();
    let source = DatabaseSource::new("db-src", config);
    assert_eq!(source.name(), "db-src");
}

#[tokio::test]
async fn test_database_sink_stub_construction() {
    use varpulis_runtime::connector::{DatabaseConfig, DatabaseSink};
    let config = DatabaseConfig::new("postgres://localhost/test", "events").unwrap();
    let result = DatabaseSink::new("db-sink", config).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().name(), "db-sink");
}

// ==========================================================================
// RedisSink stub
// ==========================================================================

#[test]
fn test_redis_sink_stub_name() {
    let config = RedisConfig::new("redis://localhost", "ch");
    let sink = RedisSink::new("redis-sink", config);
    assert_eq!(sink.name(), "redis-sink");
}

// ==========================================================================
// Multiple connectors in ManagedConnectorRegistry
// ==========================================================================

#[test]
fn test_managed_registry_multiple_mqtt_connectors() {
    let mut configs = FxHashMap::default();
    configs.insert("broker1".to_string(), ConnectorConfig::new("mqtt", "host1"));
    configs.insert("broker2".to_string(), ConnectorConfig::new("mqtt", "host2"));

    let registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();
    let reports = registry.health_reports();
    assert_eq!(reports.len(), 2);
}
