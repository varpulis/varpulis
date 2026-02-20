//! Redis connector

#[cfg(feature = "redis")]
use super::helpers::json_to_event;
use super::types::{ConnectorError, SinkConnector, SourceConnector};
use crate::event::Event;
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Redis configuration
#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub channel: String,
    pub key_prefix: Option<String>,
}

impl RedisConfig {
    pub fn new(url: &str, channel: &str) -> Self {
        Self {
            url: url.to_string(),
            channel: channel.to_string(),
            key_prefix: None,
        }
    }

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
    pub struct RedisSource {
        name: String,
        config: RedisConfig,
        running: Arc<AtomicBool>,
    }

    impl RedisSource {
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

                use futures::StreamExt;
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

    impl RedisSink {
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

#[cfg(not(feature = "redis"))]
pub struct RedisSource {
    name: String,
    #[allow(dead_code)]
    config: RedisConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisSource {
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

#[cfg(not(feature = "redis"))]
pub struct RedisSink {
    name: String,
    #[allow(dead_code)]
    config: RedisConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisSink {
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
