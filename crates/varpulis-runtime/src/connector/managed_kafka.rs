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
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::warn;

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

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.brokers)
            .set("message.timeout.ms", "5000")
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

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &group_id)
            .set("auto.offset.reset", "latest")
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

            while running.load(Ordering::SeqCst) {
                match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                    Ok(Some(Ok(msg))) => {
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
                    }
                    Ok(Some(Err(e))) => {
                        warn!("Managed Kafka {} consumer error: {}", name, e);
                    }
                    Ok(None) => break,
                    Err(_) => {} // timeout
                }
            }
            info!("Managed Kafka {} consumer stopped", name);
        });

        // Also ensure producer is available for sinks
        self.ensure_producer()?;

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
        let payload = serde_json::to_string(event).map_err(|e| anyhow!("serialize: {}", e))?;

        let record: FutureRecord<'_, str, String> = FutureRecord::to(&self.topic).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| anyhow!("kafka send: {}", e))?;

        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
