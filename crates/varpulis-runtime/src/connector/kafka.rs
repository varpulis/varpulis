//! Kafka connector for event ingestion and output
//!
//! Provides both stub implementations (always available) and full implementations
//! (requires the `kafka` feature flag with rdkafka).

use super::types::{ConnectorError, SinkConnector, SourceConnector};
use crate::event::Event;
use async_trait::async_trait;
use indexmap::IndexMap;
use tokio::sync::mpsc;
use tracing::warn;

// =============================================================================
// Kafka Configuration
// =============================================================================

/// Kafka configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: Option<String>,
    pub properties: IndexMap<String, String>,
}

impl KafkaConfig {
    pub fn new(brokers: &str, topic: &str) -> Self {
        Self {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            group_id: None,
            properties: IndexMap::new(),
        }
    }

    pub fn with_group_id(mut self, group_id: &str) -> Self {
        self.group_id = Some(group_id.to_string());
        self
    }
}

// =============================================================================
// Kafka Source (stub - requires rdkafka feature)
// =============================================================================

/// Kafka source connector (stub implementation)
pub struct KafkaSource {
    name: String,
    config: KafkaConfig,
    running: bool,
}

impl KafkaSource {
    pub fn new(name: &str, config: KafkaConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: false,
        }
    }
}

#[async_trait]
impl SourceConnector for KafkaSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        // NOTE: Full implementation requires rdkafka crate
        // This is a stub that shows the interface
        warn!(
            "Kafka source {} starting (stub implementation - requires rdkafka feature)",
            self.name
        );
        warn!("  Brokers: {}", self.config.brokers);
        warn!("  Topic: {}", self.config.topic);

        self.running = true;

        // In a real implementation, this would:
        // 1. Create a Kafka consumer
        // 2. Subscribe to the topic
        // 3. Poll for messages and convert to Events
        // 4. Send events through the tx channel

        Err(ConnectorError::NotAvailable(
            "Kafka connector requires 'kafka' feature. Enable with: cargo build --features kafka"
                .to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running = false;
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }
}

// =============================================================================
// Kafka Sink (stub - requires rdkafka feature)
// =============================================================================

/// Kafka sink connector (stub implementation)
pub struct KafkaSink {
    name: String,
    config: KafkaConfig,
}

impl KafkaSink {
    pub fn new(name: &str, config: KafkaConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[async_trait]
impl SinkConnector for KafkaSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        // NOTE: Full implementation requires rdkafka crate
        warn!(
            "Kafka sink {} send (stub) to {}/{} - event: {}",
            self.name, self.config.brokers, self.config.topic, event.event_type
        );

        Err(ConnectorError::NotAvailable(
            "Kafka connector requires 'kafka' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// Kafka Connector (full implementation with rdkafka feature)
// =============================================================================

#[cfg(feature = "kafka")]
mod kafka_impl {
    use super::*;
    use crate::connector::helpers::json_to_value;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    use rdkafka::Message;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tracing::{error, info, warn};

    /// Kafka source connector with rdkafka
    pub struct KafkaSourceImpl {
        name: String,
        config: KafkaConfig,
        running: Arc<AtomicBool>,
    }

    impl KafkaSourceImpl {
        pub fn new(name: &str, config: KafkaConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[async_trait]
    impl SourceConnector for KafkaSourceImpl {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            let group_id = self.config.group_id.clone().unwrap_or_else(|| {
                // Generate a unique group ID using process ID and timestamp
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                format!("varpulis-{}-{}", std::process::id(), ts)
            });

            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.brokers)
                .set("group.id", &group_id)
                .set("enable.auto.commit", "true")
                .set("auto.offset.reset", "latest")
                .create()
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            consumer
                .subscribe(&[&self.config.topic])
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.running.store(true, Ordering::SeqCst);
            let running = self.running.clone();
            let name = self.name.clone();

            tokio::spawn(async move {
                info!("Kafka source {} started, consuming from topic", name);

                use futures::StreamExt;
                let mut stream = consumer.stream();

                while running.load(Ordering::SeqCst) {
                    match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                        Ok(Some(Ok(msg))) => {
                            if let Some(payload) = msg.payload() {
                                match serde_json::from_slice::<serde_json::Value>(payload) {
                                    Ok(json) => {
                                        let event_type = json
                                            .get("event_type")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("KafkaEvent")
                                            .to_string();

                                        let mut event = Event::new(event_type);

                                        if let Some(obj) = json.as_object() {
                                            for (key, value) in obj {
                                                if key != "event_type" {
                                                    if let Some(v) = json_to_value(value) {
                                                        event = event.with_field(key.as_str(), v);
                                                    }
                                                }
                                            }
                                        }

                                        if tx.send(event).await.is_err() {
                                            warn!("Kafka source {}: channel closed", name);
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Kafka source {}: failed to parse JSON: {}", name, e);
                                    }
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            error!("Kafka source {} error: {}", name, e);
                        }
                        Ok(None) => break,
                        Err(_) => {} // Timeout, continue
                    }
                }

                info!("Kafka source {} stopped", name);
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

    /// Kafka sink connector with rdkafka
    pub struct KafkaSinkImpl {
        name: String,
        config: KafkaConfig,
        producer: FutureProducer,
    }

    impl KafkaSinkImpl {
        pub fn new(name: &str, config: KafkaConfig) -> Result<Self, ConnectorError> {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &config.brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            Ok(Self {
                name: name.to_string(),
                config,
                producer,
            })
        }
    }

    #[async_trait]
    impl SinkConnector for KafkaSinkImpl {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let payload = serde_json::to_string(event)
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            let record = FutureRecord::to(&self.config.topic)
                .payload(&payload)
                .key(&*event.event_type);

            self.producer
                .send(record, Duration::from_secs(5))
                .await
                .map_err(|(e, _)| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            self.producer
                .flush(Duration::from_secs(5))
                .map_err(|e| ConnectorError::SendFailed(format!("Flush failed: {}", e)))
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            self.flush().await
        }
    }
}

#[cfg(feature = "kafka")]
pub use kafka_impl::{KafkaSinkImpl as KafkaSinkFull, KafkaSourceImpl as KafkaSourceFull};
