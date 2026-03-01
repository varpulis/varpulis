//! Redis connector

use super::component::{ConnectorComponentInfo, ConnectorFactory};
#[cfg(feature = "redis")]
use super::helpers::json_to_event;
use super::types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};
use async_trait::async_trait;
use tokio::sync::mpsc;
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
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        #[cfg(not(feature = "redis"))]
        {
            let channel = config.topic.clone().unwrap_or_else(|| "events".to_string());
            Ok(Box::new(RedisSink::new(
                "redis",
                RedisConfig::new(&config.url, &channel),
            )))
        }
        #[cfg(feature = "redis")]
        {
            let _ = config;
            Err(ConnectorError::ConfigError(
                "Use async create_from_config for redis with feature enabled".to_string(),
            ))
        }
    }
}

inventory::submit! { &RedisFactory as &dyn ConnectorFactory }

/// Redis configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct RedisConfig {
    /// Redis server URL (e.g., `"redis://localhost:6379"`).
    pub url: String,
    /// Channel name for pub/sub operations.
    pub channel: String,
    /// Optional key prefix for key-value operations.
    pub key_prefix: Option<String>,
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

#[cfg(feature = "redis")]
mod redis_impl {
    use super::*;
    use redis::aio::ConnectionManager;
    use redis::AsyncCommands;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tracing::{info, warn};

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

                use futures_util::StreamExt;
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
}

#[cfg(feature = "redis")]
pub use redis_impl::{RedisSink, RedisSource};

/// Redis source stub (requires `redis` feature for full functionality).
#[cfg(not(feature = "redis"))]
#[derive(Debug)]
pub struct RedisSource {
    name: String,
    #[allow(dead_code)]
    config: RedisConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisSource {
    /// Create a new Redis source stub.
    pub fn new(name: &str, config: RedisConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "redis"))]
#[async_trait]
impl SourceConnector for RedisSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Redis connector requires 'redis' feature".to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn is_running(&self) -> bool {
        false
    }
}

/// Redis sink stub (requires `redis` feature for full functionality).
#[cfg(not(feature = "redis"))]
#[derive(Debug)]
pub struct RedisSink {
    name: String,
    #[allow(dead_code)]
    config: RedisConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisSink {
    /// Create a new Redis sink stub.
    pub fn new(name: &str, config: RedisConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "redis"))]
#[async_trait]
impl SinkConnector for RedisSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Redis connector requires 'redis' feature".to_string(),
        ))
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
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

#[cfg(feature = "redis")]
mod redis_stream_impl {
    use super::*;
    use redis::aio::ConnectionManager;
    use redis::Value as RedisValue;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tracing::{info, warn};

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
                Err(e) if e.to_string().contains("BUSYGROUP") => {
                    // Group already exists — that's fine
                    Ok(())
                }
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
                    // XREADGROUP GROUP {group} {consumer} COUNT {n} BLOCK {ms} STREAMS {key} >
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
                        Ok(RedisValue::Bulk(streams)) => {
                            for stream_entry in streams {
                                if let RedisValue::Bulk(ref parts) = stream_entry {
                                    if parts.len() < 2 {
                                        continue;
                                    }
                                    // parts[1] = array of message entries
                                    if let RedisValue::Bulk(ref messages) = parts[1] {
                                        for msg in messages {
                                            if let RedisValue::Bulk(ref msg_parts) = msg {
                                                if msg_parts.len() < 2 {
                                                    continue;
                                                }
                                                let msg_id = match &msg_parts[0] {
                                                    RedisValue::Data(b) => {
                                                        String::from_utf8_lossy(b).to_string()
                                                    }
                                                    _ => continue,
                                                };

                                                // Parse field-value pairs into JSON
                                                let event = parse_stream_fields(&msg_parts[1]);
                                                if let Some(event) = event {
                                                    if tx.send(event).await.is_err() {
                                                        return;
                                                    }
                                                }

                                                // XACK
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
                        Ok(RedisValue::Nil) => {
                            // No messages available (BLOCK timed out)
                        }
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
        if let RedisValue::Bulk(ref pairs) = fields_value {
            // Fields come as [key1, val1, key2, val2, ...]
            let mut json_map = serde_json::Map::new();
            let mut i = 0;
            while i + 1 < pairs.len() {
                let key = match &pairs[i] {
                    RedisValue::Data(b) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        i += 2;
                        continue;
                    }
                };
                let val = match &pairs[i + 1] {
                    RedisValue::Data(b) => {
                        let s = String::from_utf8_lossy(b);
                        // Try to parse as JSON, fall back to string
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

            // Build XADD command with optional MAXLEN
            let mut cmd = redis::cmd("XADD");
            cmd.arg(&self.config.stream_key);

            if let Some(max_len) = self.config.max_len {
                cmd.arg("MAXLEN").arg("~").arg(max_len);
            }

            // Auto-generated ID
            cmd.arg("*");
            // Single field: "data" -> JSON payload
            cmd.arg("data").arg(&payload);
            cmd.arg("event_type").arg(event.event_type.as_ref());

            cmd.query_async::<_, String>(&mut conn)
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
    /// Connects lazily on first `send()`.
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
}

#[cfg(feature = "redis")]
pub use redis_stream_impl::{RedisStreamSink, RedisStreamSinkStub, RedisStreamSource};

/// Redis Streams source stub (requires `redis` feature for full functionality).
#[cfg(not(feature = "redis"))]
#[derive(Debug)]
pub struct RedisStreamSource {
    name: String,
    #[allow(dead_code)]
    config: RedisStreamConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisStreamSource {
    /// Create a new Redis Streams source stub.
    pub fn new(name: &str, config: RedisStreamConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "redis"))]
#[async_trait]
impl SourceConnector for RedisStreamSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Redis Streams connector requires 'redis' feature".to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn is_running(&self) -> bool {
        false
    }
}

/// Redis Streams sink stub (requires `redis` feature for full functionality).
#[cfg(not(feature = "redis"))]
#[derive(Debug)]
pub struct RedisStreamSink {
    name: String,
    #[allow(dead_code)]
    config: RedisStreamConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisStreamSink {
    /// Create a new Redis Streams sink stub.
    pub fn new(name: &str, config: RedisStreamConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "redis"))]
#[async_trait]
impl SinkConnector for RedisStreamSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Redis Streams connector requires 'redis' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Deferred-connect Redis Streams sink stub (requires `redis` feature for full functionality).
#[cfg(not(feature = "redis"))]
#[derive(Debug)]
pub struct RedisStreamSinkStub {
    name: String,
    #[allow(dead_code)]
    config: RedisStreamConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisStreamSinkStub {
    /// Create a new deferred-connect Redis Streams sink stub.
    pub fn new(name: &str, config: RedisStreamConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "redis"))]
#[async_trait]
impl SinkConnector for RedisStreamSinkStub {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Redis Streams connector requires 'redis' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
