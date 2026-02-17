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
    /// When set, enables Kafka exactly-once semantics via transactional producer.
    /// The value must be unique per application instance.
    pub transactional_id: Option<String>,
}

impl KafkaConfig {
    pub fn new(brokers: &str, topic: &str) -> Self {
        Self {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            group_id: None,
            properties: IndexMap::new(),
            transactional_id: None,
        }
    }

    pub fn with_group_id(mut self, group_id: &str) -> Self {
        self.group_id = Some(group_id.to_string());
        self
    }

    pub fn with_properties(mut self, props: IndexMap<String, String>) -> Self {
        self.properties = props;
        self
    }

    pub fn with_transactional_id(mut self, id: &str) -> Self {
        self.transactional_id = Some(id.to_string());
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
    use crate::connector::helpers::json_to_event;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    use rdkafka::Message;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tracing::{error, info, warn};

    /// Apply user-provided properties to a ClientConfig, skipping keys
    /// that are already explicitly set by our code.
    fn apply_properties(client_config: &mut ClientConfig, props: &IndexMap<String, String>) {
        for (k, v) in props {
            // Skip keys managed internally — users set these via dedicated config fields
            match k.as_str() {
                "bootstrap.servers" | "group.id" => continue,
                _ => {
                    client_config.set(k, v);
                }
            }
        }
    }

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
            // Stable group ID: defaults to varpulis-{connector_name} so offsets
            // survive process restarts (users can override via group_id config).
            let group_id = self
                .config
                .group_id
                .clone()
                .unwrap_or_else(|| format!("varpulis-{}", self.name));

            let mut client_config = ClientConfig::new();
            client_config
                .set("bootstrap.servers", &self.config.brokers)
                .set("group.id", &group_id)
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "latest");

            // Apply user-provided properties (can override auto.offset.reset, etc.)
            apply_properties(&mut client_config, &self.config.properties);

            let consumer: StreamConsumer = client_config
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
                let mut consecutive_errors: u32 = 0;

                while running.load(Ordering::SeqCst) {
                    match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                        Ok(Some(Ok(msg))) => {
                            consecutive_errors = 0;

                            if let Some(payload) = msg.payload() {
                                // Enforce payload size limit
                                if payload.len() > crate::limits::MAX_EVENT_PAYLOAD_BYTES {
                                    warn!(
                                        "Kafka source {}: payload too large ({} bytes, max {}), skipped",
                                        name,
                                        payload.len(),
                                        crate::limits::MAX_EVENT_PAYLOAD_BYTES
                                    );
                                } else {
                                    match serde_json::from_slice::<serde_json::Value>(payload) {
                                        Ok(json) => {
                                            let event = json_to_event(
                                                json.get("event_type")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("KafkaEvent"),
                                                &json,
                                            );

                                            if tx.send(event).await.is_err() {
                                                warn!("Kafka source {}: channel closed", name);
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Kafka source {}: failed to parse JSON: {}",
                                                name, e
                                            );
                                        }
                                    }
                                }
                            }

                            // Commit offset after successful processing
                            if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                                warn!("Kafka source {}: offset commit failed: {}", name, e);
                            }
                        }
                        Ok(Some(Err(e))) => {
                            consecutive_errors += 1;
                            let backoff =
                                Duration::from_millis(100 * 2u64.pow(consecutive_errors.min(7)));
                            error!("Kafka source {} error (backoff {:?}): {}", name, backoff, e);
                            tokio::time::sleep(backoff).await;
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

    /// Kafka sink connector with rdkafka.
    ///
    /// Supports two delivery modes:
    /// - **At-least-once** (default): fire-and-forget with `acks=all`
    /// - **Exactly-once**: transactional producer when `transactional_id` is set
    pub struct KafkaSinkImpl {
        name: String,
        config: KafkaConfig,
        producer: FutureProducer,
        /// True when the producer was initialized with a transactional.id.
        transactional: bool,
    }

    impl KafkaSinkImpl {
        pub fn new(name: &str, config: KafkaConfig) -> Result<Self, ConnectorError> {
            let transactional = config.transactional_id.is_some();

            let mut client_config = ClientConfig::new();
            client_config
                .set("bootstrap.servers", &config.brokers)
                .set("message.timeout.ms", "30000")
                // Production defaults for batching throughput
                .set("linger.ms", "5")
                .set("batch.size", "65536")
                .set("compression.type", "lz4")
                .set("acks", "all");

            if let Some(tid) = &config.transactional_id {
                client_config.set("transactional.id", tid);
                // Exactly-once requires idempotence
                client_config.set("enable.idempotence", "true");
            }

            // Apply user-provided properties (can override any of the above)
            apply_properties(&mut client_config, &config.properties);

            let producer: FutureProducer = client_config
                .create()
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            if transactional {
                producer
                    .init_transactions(Duration::from_secs(30))
                    .map_err(|e| {
                        ConnectorError::ConnectionFailed(format!(
                            "Failed to init transactions: {}",
                            e
                        ))
                    })?;
                info!(
                    "Kafka sink '{}' initialized with exactly-once semantics",
                    name
                );
            }

            Ok(Self {
                name: name.to_string(),
                config,
                producer,
                transactional,
            })
        }

        /// Whether this sink uses transactional (exactly-once) delivery.
        pub fn is_transactional(&self) -> bool {
            self.transactional
        }

        /// Send a batch of events within a single Kafka transaction.
        ///
        /// All events are enqueued, then the transaction is committed atomically.
        /// On failure the transaction is aborted and no events are visible to consumers.
        pub async fn send_batch_transactional(
            &self,
            events: &[std::sync::Arc<Event>],
        ) -> Result<(), ConnectorError> {
            self.producer
                .begin_transaction()
                .map_err(|e| ConnectorError::SendFailed(format!("begin_transaction: {}", e)))?;

            for event in events {
                let payload = event.to_sink_payload();
                let record = FutureRecord::to(&self.config.topic)
                    .payload(&payload)
                    .key(&*event.event_type);

                if let Err((e, _)) = self.producer.send(record, Duration::ZERO).await {
                    // Abort the transaction — none of the batch will be visible
                    let _ = self.producer.abort_transaction(Duration::from_secs(10));
                    return Err(ConnectorError::SendFailed(format!(
                        "send in transaction: {}",
                        e
                    )));
                }
            }

            self.producer
                .commit_transaction(Duration::from_secs(30))
                .map_err(|e| {
                    let _ = self.producer.abort_transaction(Duration::from_secs(10));
                    ConnectorError::SendFailed(format!("commit_transaction: {}", e))
                })?;

            Ok(())
        }
    }

    #[async_trait]
    impl SinkConnector for KafkaSinkImpl {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let payload = event.to_sink_payload();

            let record = FutureRecord::to(&self.config.topic)
                .payload(&payload)
                .key(&*event.event_type);

            if self.transactional {
                // Wrap single event in a transaction for exactly-once
                self.producer
                    .begin_transaction()
                    .map_err(|e| ConnectorError::SendFailed(format!("begin_transaction: {}", e)))?;

                if let Err((e, _)) = self.producer.send(record, Duration::ZERO).await {
                    let _ = self.producer.abort_transaction(Duration::from_secs(10));
                    return Err(ConnectorError::SendFailed(e.to_string()));
                }

                self.producer
                    .commit_transaction(Duration::from_secs(30))
                    .map_err(|e| {
                        let _ = self.producer.abort_transaction(Duration::from_secs(10));
                        ConnectorError::SendFailed(format!("commit_transaction: {}", e))
                    })?;
            } else {
                // Fire-and-forget: enqueue into librdkafka's internal batch buffer.
                // The library handles batching via linger.ms / batch.size settings.
                // We use Duration::ZERO so send() returns immediately once queued.
                self.producer
                    .send(record, Duration::ZERO)
                    .await
                    .map_err(|(e, _)| ConnectorError::SendFailed(e.to_string()))?;
            }

            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            self.producer
                .flush(Duration::from_secs(10))
                .map_err(|e| ConnectorError::SendFailed(format!("Flush failed: {}", e)))
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            self.flush().await
        }
    }
}

#[cfg(feature = "kafka")]
pub use kafka_impl::{KafkaSinkImpl as KafkaSinkFull, KafkaSourceImpl as KafkaSourceFull};
