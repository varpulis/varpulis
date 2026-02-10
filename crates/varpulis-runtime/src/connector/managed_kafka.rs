//! Managed Kafka connector — shares a single producer across all sinks

use super::kafka::KafkaConfig;
use super::managed::ManagedConnector;
use super::types::ConnectorError;
use crate::event::Event;
use crate::sink::Sink;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::helpers::json_to_event;
use anyhow::anyhow;
use tracing::info;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{error, warn};

/// Managed Kafka connector that owns a single producer connection.
///
/// - **Sources**: each `start_source` call creates a new `StreamConsumer`
///   (Kafka consumers are not Clone).
/// - **Sinks**: the `FutureProducer` is Clone-able and shared across all sinks.
pub struct ManagedKafkaConnector {
    connector_name: String,
    config: KafkaConfig,
    producer: Option<FutureProducer>,
    running: Arc<AtomicBool>,
}

impl ManagedKafkaConnector {
    pub fn new(name: &str, config: KafkaConfig) -> Self {
        Self {
            connector_name: name.to_string(),
            config,
            producer: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn ensure_producer(&mut self) -> Result<FutureProducer, ConnectorError> {
        if let Some(producer) = &self.producer {
            return Ok(producer.clone());
        }

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("message.timeout.ms", "30000")
            .set("linger.ms", "5")
            .set("batch.size", "65536")
            .set("compression.type", "lz4")
            .set("acks", "all");

        // Apply user-provided properties (can override any of the above)
        for (k, v) in &self.config.properties {
            if k != "bootstrap.servers" && k != "group.id" {
                client_config.set(k, v);
            }
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        self.producer = Some(producer.clone());
        self.running.store(true, Ordering::SeqCst);
        info!(
            "Managed Kafka {} producer connected to {}",
            self.connector_name, self.config.brokers
        );
        Ok(producer)
    }
}

#[async_trait]
impl ManagedConnector for ManagedKafkaConnector {
    fn name(&self) -> &str {
        &self.connector_name
    }

    fn connector_type(&self) -> &str {
        "kafka"
    }

    async fn start_source(
        &mut self,
        topic: &str,
        tx: mpsc::Sender<Event>,
    ) -> Result<(), ConnectorError> {
        // Each source gets its own consumer (consumers are not Clone)
        let group_id = self
            .config
            .group_id
            .clone()
            .unwrap_or_else(|| format!("varpulis-{}", self.connector_name));

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest");

        // Apply user-provided properties
        for (k, v) in &self.config.properties {
            if k != "bootstrap.servers" && k != "group.id" {
                client_config.set(k, v);
            }
        }

        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        consumer
            .subscribe(&[topic])
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let name = self.connector_name.clone();
        let topic_owned = topic.to_string();

        tokio::spawn(async move {
            use futures::StreamExt;
            info!(
                "Managed Kafka {} consumer started on topic {}",
                name, topic_owned
            );

            let stream = consumer.stream();
            tokio::pin!(stream);
            let mut consecutive_errors: u32 = 0;

            while running.load(Ordering::SeqCst) {
                match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                    Ok(Some(Ok(msg))) => {
                        consecutive_errors = 0;

                        if let Some(payload) = msg.payload() {
                            if let Ok(text) = std::str::from_utf8(payload) {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
                                    let event_type = json
                                        .get("event_type")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("KafkaEvent");
                                    let event = json_to_event(event_type, &json);
                                    if tx.send(event).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }

                        // Commit offset after successful processing
                        if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                            warn!("Managed Kafka {} offset commit failed: {}", name, e);
                        }
                    }
                    Ok(Some(Err(e))) => {
                        consecutive_errors += 1;
                        let backoff =
                            Duration::from_millis(100 * 2u64.pow(consecutive_errors.min(7)));
                        error!(
                            "Managed Kafka {} consumer error (backoff {:?}): {}",
                            name, backoff, e
                        );
                        tokio::time::sleep(backoff).await;
                    }
                    Ok(None) => break,
                    Err(_) => {} // timeout
                }
            }
            info!("Managed Kafka {} consumer stopped", name);
        });

        // Also ensure producer is available for sinks
        let _ = self.ensure_producer()?;

        info!(
            "Managed Kafka {} source started on topic: {}",
            self.connector_name, topic
        );
        Ok(())
    }

    fn create_sink(&mut self, topic: &str) -> Result<Arc<dyn Sink>, ConnectorError> {
        let producer = self.ensure_producer()?;
        Ok(Arc::new(KafkaSharedSink {
            sink_name: format!("{}::{}", self.connector_name, topic),
            topic: topic.to_string(),
            producer,
        }))
    }

    async fn shutdown(&mut self) -> Result<(), ConnectorError> {
        self.running.store(false, Ordering::SeqCst);
        self.producer = None;
        info!("Managed Kafka {} shut down", self.connector_name);
        Ok(())
    }
}

/// Lightweight sink handle that publishes via a shared `FutureProducer`.
struct KafkaSharedSink {
    sink_name: String,
    topic: String,
    producer: FutureProducer,
}

#[async_trait]
impl Sink for KafkaSharedSink {
    fn name(&self) -> &str {
        &self.sink_name
    }

    async fn send(&self, event: &Event) -> anyhow::Result<()> {
        let payload = event.to_sink_payload();

        let record = FutureRecord::to(&self.topic)
            .payload(&payload)
            .key(&*event.event_type);

        // Fire-and-forget: enqueue into librdkafka's internal batch buffer
        self.producer
            .send(record, Duration::ZERO)
            .await
            .map_err(|(e, _)| anyhow!("kafka send: {}", e))?;

        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.producer
            .flush(Duration::from_secs(10))
            .map_err(|e| anyhow!("kafka flush: {}", e))
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.flush().await
    }
}
