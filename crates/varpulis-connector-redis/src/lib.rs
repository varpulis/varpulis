//! Redis source and sink connectors.
//!
//! This crate provides full Redis connectivity for the Varpulis CEP engine
//! including pub/sub and Redis Streams (XADD/XREADGROUP) support.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use redis::aio::ConnectionManager;
#[allow(unused_imports)]
use redis::Commands;
use redis::{AsyncCommands, Value as RedisValue};
use tokio::sync::mpsc;
use tracing::{info, warn};
use varpulis_connector_api::helpers::json_to_event;
use varpulis_connector_api::{
    ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory, SinkConnector,
    SourceConnector,
};
use varpulis_core::Event;

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

static REDIS_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "redis",
    display_name: "Redis",
    description: "Redis pub/sub and streams connector",
    feature_flag: "redis",
    supports_source: true,
    supports_sink: true,
    supports_managed: false,
    config_params: &[],
};

struct RedisFactory;

impl ConnectorFactory for RedisFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &REDIS_INFO
    }

    fn create_sink_connector(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        Err(ConnectorError::ConfigError(
            "Use async create_from_config for redis".to_string(),
        ))
    }
}

inventory::submit! { &RedisFactory as &dyn ConnectorFactory }

// =============================================================================
// Redis Configuration
// =============================================================================

/// Mask any `user:password` userinfo embedded in a `redis://…@host` connection
/// URL so a password in the connection string never reaches logs, error
/// messages, or panic output via `{:?}`. URLs without credentials pass through
/// unchanged.
fn redact_redis_url(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after = scheme_end + 3;
        if let Some(at_rel) = url[after..].find('@') {
            let at = after + at_rel;
            if at > after {
                return format!("{}://***REDACTED***{}", &url[..scheme_end], &url[at..]);
            }
        }
    }
    url.to_string()
}

/// Redis configuration
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct RedisConfig {
    /// Redis server URL (e.g., `"redis://localhost:6379"`).
    pub url: String,
    /// Channel name for pub/sub operations.
    pub channel: String,
    /// Optional key prefix for key-value operations.
    pub key_prefix: Option<String>,
}

/// Hand-written so a password embedded in `url` (`redis://:pw@host`) never
/// leaks via `{:?}`. The derived `Debug` printed `url` verbatim.
impl std::fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConfig")
            .field("url", &redact_redis_url(&self.url))
            .field("channel", &self.channel)
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

impl RedisConfig {
    /// Create a new Redis configuration with the given URL and channel.
    pub fn new(url: &str, channel: &str) -> Self {
        Self {
            url: url.to_string(),
            channel: channel.to_string(),
            key_prefix: None,
        }
    }

    /// Set a key prefix for key-value operations.
    pub fn with_key_prefix(mut self, prefix: &str) -> Self {
        self.key_prefix = Some(prefix.to_string());
        self
    }
}

// =============================================================================
// Redis Source
// =============================================================================

/// Redis source that subscribes to a channel
#[derive(Debug)]
pub struct RedisSource {
    name: String,
    config: RedisConfig,
    running: Arc<AtomicBool>,
}

impl RedisSource {
    /// Creates a new Redis pub/sub source connector.
    pub fn new(name: &str, config: RedisConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl SourceConnector for RedisSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        let client = redis::Client::open(self.config.url.as_str())
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let mut pubsub = client
            .get_async_pubsub()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        pubsub
            .subscribe(&self.config.channel)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let name = self.name.clone();

        tokio::spawn(async move {
            info!("Redis source {} started, subscribed to channel", name);

            let mut stream = pubsub.on_message();

            while running.load(Ordering::SeqCst) {
                match tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
                    .await
                {
                    Ok(Some(msg)) => {
                        let payload: String = match msg.get_payload() {
                            Ok(p) => p,
                            Err(e) => {
                                warn!("Redis source {}: failed to get payload: {}", name, e);
                                continue;
                            }
                        };

                        match serde_json::from_str::<serde_json::Value>(&payload) {
                            Ok(json) => {
                                let event_type = json
                                    .get("event_type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("RedisEvent")
                                    .to_string();

                                let event = json_to_event(&event_type, &json);

                                if tx.send(event).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("Redis source {}: failed to parse JSON: {}", name, e);
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {} // Timeout
                }
            }

            info!("Redis source {} stopped", name);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

// =============================================================================
// Redis Sink
// =============================================================================

/// Redis sink that publishes to a channel or sets keys
pub struct RedisSink {
    name: String,
    config: RedisConfig,
    conn: ConnectionManager,
}

impl std::fmt::Debug for RedisSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSink")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl RedisSink {
    /// Creates a new Redis pub/sub sink connector.
    pub async fn new(name: &str, config: RedisConfig) -> Result<Self, ConnectorError> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            name: name.to_string(),
            config,
            conn,
        })
    }

    /// Set a key-value pair
    pub async fn set(&mut self, key: &str, value: &str) -> Result<(), ConnectorError> {
        let full_key = match &self.config.key_prefix {
            Some(prefix) => format!("{}:{}", prefix, key),
            None => key.to_string(),
        };

        self.conn
            .set::<_, _, ()>(&full_key, value)
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Get a value by key
    pub async fn get(&mut self, key: &str) -> Result<Option<String>, ConnectorError> {
        let full_key = match &self.config.key_prefix {
            Some(prefix) => format!("{}:{}", prefix, key),
            None => key.to_string(),
        };

        let value: Option<String> = self
            .conn
            .get(&full_key)
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        Ok(value)
    }
}

#[async_trait]
impl SinkConnector for RedisSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let payload = String::from_utf8(event.to_sink_payload())
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        let mut conn = self.conn.clone();
        conn.publish::<_, _, ()>(&self.config.channel, &payload)
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// ============================================================================
// Redis Streams (XADD / XREADGROUP) support
// ============================================================================

/// Configuration for Redis Streams (XADD/XREADGROUP)
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct RedisStreamConfig {
    /// Redis server URL.
    pub url: String,
    /// Redis stream key name.
    pub stream_key: String,
    /// Consumer group name (optional, defaults to `"varpulis"`).
    pub group_name: Option<String>,
    /// Consumer name within the group (optional, auto-generated if not set).
    pub consumer_name: Option<String>,
    /// Number of messages to read per XREADGROUP call (default: 100).
    pub batch_size: usize,
    /// Block timeout in milliseconds for XREADGROUP (default: 1000).
    pub block_ms: usize,
    /// Maximum stream length for XADD MAXLEN trimming (optional).
    pub max_len: Option<usize>,
}

/// Hand-written so a password embedded in `url` never leaks via `{:?}`.
impl std::fmt::Debug for RedisStreamConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStreamConfig")
            .field("url", &redact_redis_url(&self.url))
            .field("stream_key", &self.stream_key)
            .field("group_name", &self.group_name)
            .field("consumer_name", &self.consumer_name)
            .field("batch_size", &self.batch_size)
            .field("block_ms", &self.block_ms)
            .field("max_len", &self.max_len)
            .finish()
    }
}

impl RedisStreamConfig {
    /// Create a new Redis Streams configuration with the given URL and stream key.
    pub fn new(url: &str, stream_key: &str) -> Self {
        Self {
            url: url.to_string(),
            stream_key: stream_key.to_string(),
            group_name: None,
            consumer_name: None,
            batch_size: 100,
            block_ms: 1000,
            max_len: None,
        }
    }

    /// Set the consumer group name.
    pub fn with_group(mut self, group: &str) -> Self {
        self.group_name = Some(group.to_string());
        self
    }

    /// Set the consumer name within the group.
    pub fn with_consumer(mut self, consumer: &str) -> Self {
        self.consumer_name = Some(consumer.to_string());
        self
    }

    /// Set the number of messages to read per batch.
    pub const fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the block timeout in milliseconds for XREADGROUP.
    pub const fn with_block_ms(mut self, ms: usize) -> Self {
        self.block_ms = ms;
        self
    }

    /// Set the maximum stream length for XADD MAXLEN trimming.
    pub const fn with_max_len(mut self, max: usize) -> Self {
        self.max_len = Some(max);
        self
    }
}

/// Redis Streams source using XREADGROUP for consumer-group processing
#[derive(Debug)]
pub struct RedisStreamSource {
    name: String,
    config: RedisStreamConfig,
    running: Arc<AtomicBool>,
}

impl RedisStreamSource {
    /// Creates a new Redis Streams source connector.
    pub fn new(name: &str, config: RedisStreamConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Try to create the consumer group (ignore errors if it already exists)
    async fn ensure_group(
        conn: &mut ConnectionManager,
        key: &str,
        group: &str,
    ) -> Result<(), ConnectorError> {
        let result: Result<(), redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(key)
            .arg(group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(conn)
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
            Err(e) => Err(ConnectorError::ConnectionFailed(format!(
                "XGROUP CREATE failed: {}",
                e
            ))),
        }
    }
}

#[async_trait]
impl SourceConnector for RedisStreamSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        let client = redis::Client::open(self.config.url.as_str())
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let mut conn = ConnectionManager::new(client)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let group = self
            .config
            .group_name
            .clone()
            .unwrap_or_else(|| "varpulis".to_string());
        let consumer = self
            .config
            .consumer_name
            .clone()
            .unwrap_or_else(|| format!("varpulis-{}", self.name));

        Self::ensure_group(&mut conn, &self.config.stream_key, &group).await?;

        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let name = self.name.clone();
        let stream_key = self.config.stream_key.clone();
        let batch_size = self.config.batch_size;
        let block_ms = self.config.block_ms;

        tokio::spawn(async move {
            info!(
                "Redis Streams source {} started, reading from {}",
                name, stream_key
            );

            while running.load(Ordering::SeqCst) {
                let result: Result<RedisValue, redis::RedisError> = redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(&group)
                    .arg(&consumer)
                    .arg("COUNT")
                    .arg(batch_size)
                    .arg("BLOCK")
                    .arg(block_ms)
                    .arg("STREAMS")
                    .arg(&stream_key)
                    .arg(">")
                    .query_async(&mut conn)
                    .await;

                match result {
                    Ok(RedisValue::Array(streams)) => {
                        for stream_entry in streams {
                            if let RedisValue::Array(ref parts) = stream_entry {
                                if parts.len() < 2 {
                                    continue;
                                }
                                if let RedisValue::Array(ref messages) = parts[1] {
                                    for msg in messages {
                                        if let RedisValue::Array(ref msg_parts) = msg {
                                            if msg_parts.len() < 2 {
                                                continue;
                                            }
                                            let msg_id = match &msg_parts[0] {
                                                RedisValue::BulkString(b) => {
                                                    String::from_utf8_lossy(b).to_string()
                                                }
                                                _ => continue,
                                            };

                                            let event = parse_stream_fields(&msg_parts[1]);
                                            if let Some(event) = event {
                                                if tx.send(event).await.is_err() {
                                                    return;
                                                }
                                            }

                                            let _: Result<(), redis::RedisError> =
                                                redis::cmd("XACK")
                                                    .arg(&stream_key)
                                                    .arg(&group)
                                                    .arg(&msg_id)
                                                    .query_async(&mut conn)
                                                    .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(RedisValue::Nil) => {}
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Redis Streams source {}: XREADGROUP error: {}", name, e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }

            info!("Redis Streams source {} stopped", name);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Parse Redis stream field-value pairs into an Event
fn parse_stream_fields(fields_value: &RedisValue) -> Option<Event> {
    if let RedisValue::Array(ref pairs) = fields_value {
        let mut json_map = serde_json::Map::new();
        let mut i = 0;
        while i + 1 < pairs.len() {
            let key = match &pairs[i] {
                RedisValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                _ => {
                    i += 2;
                    continue;
                }
            };
            let val = match &pairs[i + 1] {
                RedisValue::BulkString(b) => {
                    let s = String::from_utf8_lossy(b);
                    serde_json::from_str::<serde_json::Value>(&s)
                        .unwrap_or_else(|_| serde_json::Value::String(s.to_string()))
                }
                _ => serde_json::Value::Null,
            };
            json_map.insert(key, val);
            i += 2;
        }

        let event_type = json_map
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("RedisStreamEvent")
            .to_string();

        let json = serde_json::Value::Object(json_map);
        Some(json_to_event(&event_type, &json))
    } else {
        None
    }
}

/// Redis Streams sink using XADD
pub struct RedisStreamSink {
    name: String,
    config: RedisStreamConfig,
    conn: ConnectionManager,
}

impl std::fmt::Debug for RedisStreamSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStreamSink")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl RedisStreamSink {
    /// Creates a new Redis Streams sink connector.
    pub async fn new(name: &str, config: RedisStreamConfig) -> Result<Self, ConnectorError> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            name: name.to_string(),
            config,
            conn,
        })
    }
}

#[async_trait]
impl SinkConnector for RedisStreamSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let payload = String::from_utf8(event.to_sink_payload())
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        let mut conn = self.conn.clone();

        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.config.stream_key);

        if let Some(max_len) = self.config.max_len {
            cmd.arg("MAXLEN").arg("~").arg(max_len);
        }

        cmd.arg("*");
        cmd.arg("data").arg(&payload);
        cmd.arg("event_type").arg(event.event_type.as_ref());

        cmd.query_async::<String>(&mut conn)
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Deferred-connect Redis Streams sink for use in sync sink factory.
pub struct RedisStreamSinkStub {
    name: String,
    config: RedisStreamConfig,
    inner: tokio::sync::Mutex<Option<RedisStreamSink>>,
}

impl std::fmt::Debug for RedisStreamSinkStub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStreamSinkStub")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl RedisStreamSinkStub {
    /// Creates a new lazy-initialized Redis Streams sink stub.
    pub fn new(name: &str, config: RedisStreamConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            inner: tokio::sync::Mutex::new(None),
        }
    }

    async fn ensure_connected(&self) -> Result<(), ConnectorError> {
        let mut guard = self.inner.lock().await;
        if guard.is_none() {
            let sink = RedisStreamSink::new(&self.name, self.config.clone()).await?;
            *guard = Some(sink);
        }
        Ok(())
    }
}

#[async_trait]
impl SinkConnector for RedisStreamSinkStub {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        self.ensure_connected().await?;
        let guard = self.inner.lock().await;
        guard.as_ref().unwrap().send(event).await
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        let guard = self.inner.lock().await;
        if let Some(ref sink) = *guard {
            sink.flush().await
        } else {
            Ok(())
        }
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        let guard = self.inner.lock().await;
        if let Some(ref sink) = *guard {
            sink.close().await
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_config_debug_redacts_url_password() {
        // A password embedded in the connection URL must never appear in `{:?}`
        // output (logs, error chains, panic messages).
        let config = RedisConfig::new("redis://alice:s3cr3t-pw@redis.internal:6379", "events");
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("s3cr3t-pw"),
            "RedisConfig Debug leaked the URL password: {rendered}"
        );
        assert!(
            rendered.contains("***REDACTED***"),
            "expected a redaction marker in place of credentials: {rendered}"
        );
        // Non-secret fields stay visible for diagnostics.
        assert!(
            rendered.contains("events"),
            "channel should still be shown: {rendered}"
        );
    }

    #[test]
    fn redis_stream_config_debug_redacts_url_password() {
        let config = RedisStreamConfig::new("redis://:top-secret@host:6379", "mystream");
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("top-secret"),
            "RedisStreamConfig Debug leaked the URL password: {rendered}"
        );
        assert!(
            rendered.contains("***REDACTED***"),
            "expected redaction: {rendered}"
        );
        assert!(
            rendered.contains("mystream"),
            "stream_key should still be shown: {rendered}"
        );
    }

    #[test]
    fn redact_leaves_credential_free_url_intact() {
        // No userinfo → nothing to hide, URL passes through verbatim.
        assert_eq!(
            redact_redis_url("redis://localhost:6379"),
            "redis://localhost:6379"
        );
    }

    #[test]
    fn test_redis_config_new() {
        let config = RedisConfig::new("redis://localhost:6379", "my-channel");
        assert_eq!(config.url, "redis://localhost:6379");
        assert_eq!(config.channel, "my-channel");
        assert!(config.key_prefix.is_none());
    }

    #[test]
    fn test_redis_config_with_key_prefix() {
        let config =
            RedisConfig::new("redis://localhost:6379", "events").with_key_prefix("varpulis");
        assert_eq!(config.key_prefix.as_deref(), Some("varpulis"));
    }

    #[test]
    fn test_redis_config_serialization_roundtrip() {
        let config = RedisConfig::new("redis://host:6379", "ch1").with_key_prefix("pfx");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RedisConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.url, "redis://host:6379");
        assert_eq!(deserialized.channel, "ch1");
        assert_eq!(deserialized.key_prefix.as_deref(), Some("pfx"));
    }

    #[test]
    fn test_redis_stream_config_new() {
        let config = RedisStreamConfig::new("redis://localhost:6379", "mystream");
        assert_eq!(config.url, "redis://localhost:6379");
        assert_eq!(config.stream_key, "mystream");
        assert!(config.group_name.is_none());
        assert!(config.consumer_name.is_none());
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.block_ms, 1000);
        assert!(config.max_len.is_none());
    }

    #[test]
    fn test_redis_stream_config_builder_chain() {
        let config = RedisStreamConfig::new("redis://localhost:6379", "events")
            .with_group("my-group")
            .with_consumer("worker-1")
            .with_batch_size(500)
            .with_block_ms(2000)
            .with_max_len(100_000);

        assert_eq!(config.group_name.as_deref(), Some("my-group"));
        assert_eq!(config.consumer_name.as_deref(), Some("worker-1"));
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.block_ms, 2000);
        assert_eq!(config.max_len, Some(100_000));
    }

    #[test]
    fn test_redis_stream_config_serialization_roundtrip() {
        let config = RedisStreamConfig::new("redis://r:6379", "stream1")
            .with_group("g")
            .with_max_len(5000);
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RedisStreamConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.stream_key, "stream1");
        assert_eq!(deserialized.group_name.as_deref(), Some("g"));
        assert_eq!(deserialized.max_len, Some(5000));
    }

    #[test]
    fn test_redis_source_initial_state() {
        let config = RedisConfig::new("redis://localhost:6379", "ch");
        let source = RedisSource::new("redis-src", config);
        assert_eq!(source.name, "redis-src");
        assert!(!source.is_running());
    }

    #[test]
    fn test_redis_stream_source_initial_state() {
        let config = RedisStreamConfig::new("redis://localhost:6379", "stream");
        let source = RedisStreamSource::new("stream-src", config);
        assert_eq!(source.name, "stream-src");
        assert!(!source.is_running());
    }

    #[test]
    fn test_parse_stream_fields_valid() {
        let fields = RedisValue::Array(vec![
            RedisValue::BulkString(b"event_type".to_vec()),
            RedisValue::BulkString(b"LoginEvent".to_vec()),
            RedisValue::BulkString(b"user".to_vec()),
            RedisValue::BulkString(b"alice".to_vec()),
        ]);
        let event = parse_stream_fields(&fields);
        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.event_type.as_ref(), "LoginEvent");
    }

    #[test]
    fn test_parse_stream_fields_no_event_type() {
        let fields = RedisValue::Array(vec![
            RedisValue::BulkString(b"user".to_vec()),
            RedisValue::BulkString(b"bob".to_vec()),
        ]);
        let event = parse_stream_fields(&fields);
        assert!(event.is_some());
        // Falls back to default "RedisStreamEvent"
        assert_eq!(event.unwrap().event_type.as_ref(), "RedisStreamEvent");
    }

    #[test]
    fn test_parse_stream_fields_non_array() {
        let fields = RedisValue::Nil;
        assert!(parse_stream_fields(&fields).is_none());
    }

    #[test]
    fn test_parse_stream_fields_json_value() {
        let fields = RedisValue::Array(vec![
            RedisValue::BulkString(b"count".to_vec()),
            RedisValue::BulkString(b"42".to_vec()),
        ]);
        let event = parse_stream_fields(&fields).unwrap();
        // "42" should be parsed as JSON number
        assert_eq!(event.get("count"), Some(&varpulis_core::Value::Int(42)));
    }

    #[test]
    fn test_redis_info_static() {
        assert_eq!(REDIS_INFO.connector_type, "redis");
        assert!(REDIS_INFO.supports_source);
        assert!(REDIS_INFO.supports_sink);
        assert!(!REDIS_INFO.supports_managed);
    }
}
