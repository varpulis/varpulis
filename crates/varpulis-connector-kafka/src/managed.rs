//! Managed Kafka connector -- shares a single producer across all sinks

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::Message;
use tokio::sync::mpsc;
use tracing::{error, info};
use varpulis_connector_api::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use varpulis_connector_api::helpers::json_to_event;
use varpulis_connector_api::sink::{Sink, SinkError};
use varpulis_connector_api::{ConnectorError, ManagedConnector};
use varpulis_core::Event;

use crate::KafkaConfig;

/// Returns true if the given property name is a VPL-level concept that
/// should NOT be forwarded directly to librdkafka. These keys are either
/// translated to dot-notation equivalents (handled separately) or used
/// only on the VPL side (e.g. group_id is consumed before client config
/// construction).
fn is_vpl_only_property(key: &str) -> bool {
    matches!(
        key,
        "bootstrap.servers"
            | "group.id"
            | "auto.offset.reset"
            | "group_id"
            | "auto_offset_reset"
            | "brokers"
            | "topic"
    )
}

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

impl std::fmt::Debug for ManagedKafkaConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedKafkaConnector")
            .field("connector_name", &self.connector_name)
            .field("running", &self.running)
            .finish_non_exhaustive()
    }
}

impl ManagedKafkaConnector {
    /// Creates a new managed Kafka connector.
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
            // Producer throughput tuning. linger.ms=20 lets librdkafka coalesce
            // many small per-event sends into a single broker write — without
            // it, throughput is bounded at one round-trip per event (~6 ms ≈
            // 150 eps). Combined with acks=1 (leader-only ack, instead of
            // acks=all) this matches what high-throughput producers (Arroyo,
            // Flink, kafka-python's `acks=1` default) use for benchmark mode.
            .set("linger.ms", "20")
            .set("batch.size", "1048576") // 1 MiB per batch
            .set("compression.type", "lz4")
            .set("queue.buffering.max.messages", "1000000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("acks", "1");

        // Apply user-provided properties (can override any of the above).
        // Skip VPL-specific keys that shouldn't be passed to librdkafka.
        for (k, v) in &self.config.properties {
            if is_vpl_only_property(k) {
                continue;
            }
            client_config.set(k, v);
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

    #[tracing::instrument(level = "debug", skip(self, tx, params))]
    async fn start_source(
        &mut self,
        topic: &str,
        tx: mpsc::Sender<Vec<Event>>,
        params: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let group_id = params
            .get("group_id")
            .cloned()
            .or_else(|| self.config.group_id.clone())
            .unwrap_or_else(|| format!("varpulis-{}", self.connector_name));

        // Honor `auto_offset_reset` if the user passed it; default to "latest"
        // so existing pipelines keep current behaviour.
        let offset_reset = params
            .get("auto_offset_reset")
            .or_else(|| self.config.properties.get("auto_offset_reset"))
            .cloned()
            .unwrap_or_else(|| "latest".to_string());

        // librdkafka defaults are tuned for low-latency, *not* high
        // throughput. The values below switch the consumer into
        // "throughput mode": the broker accumulates ~1MB or 100ms before
        // responding to a fetch, and the local C-library queue holds up
        // to 100k messages so the rust-side stream can drain in big
        // batches. Combined with consumer-side `Vec<Event>` batching this
        // brings sustained throughput from ~700 eps into the 50-150k eps
        // range. See docs/development/kafka-source-batching.md.
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", &offset_reset)
            // Throughput tuning ----------------------------------------
            // Wait for at least 1 MiB per fetch...
            .set("fetch.min.bytes", "1048576")
            // ...or up to 100 ms, whichever comes first.
            .set("fetch.wait.max.ms", "100")
            // Up to 8 MiB per partition per fetch (default is 1 MiB).
            .set("max.partition.fetch.bytes", "8388608")
            // Local C-library queue: hold up to 100k messages or 64 MiB
            // before applying back-pressure to the broker. Default is
            // 100000 messages but only 64 MiB; we keep both generous.
            .set("queued.min.messages", "100000")
            .set("queued.max.messages.kbytes", "65536");

        for (k, v) in &self.config.properties {
            // Skip VPL-specific keys (already translated above) and keys that
            // we set explicitly. Anything else is forwarded to librdkafka.
            if is_vpl_only_property(k) {
                continue;
            }
            client_config.set(k, v);
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
            use futures_util::StreamExt;

            info!(
                "Managed Kafka {} consumer started on topic {}",
                name, topic_owned
            );

            let stream = consumer.stream();
            tokio::pin!(stream);
            let cb = CircuitBreaker::new(CircuitBreakerConfig {
                failure_threshold: 10,
                reset_timeout: Duration::from_secs(30),
            });

            // Consumer-side batching: accumulate up to BATCH_MAX events and
            // flush either when full or every BATCH_FLUSH_MS, whichever
            // comes first. Sending batches (instead of single events) over
            // the run-loop channel amortizes the per-event async wake-up
            // cost — at high event rates this is the dominant overhead.
            // See docs/development/kafka-source-batching.md for the full
            // analysis. Target throughput: 50-150k eps.
            const BATCH_MAX: usize = 256;
            const BATCH_FLUSH_MS: u64 = 5;
            let mut batch: Vec<Event> = Vec::with_capacity(BATCH_MAX);

            // Periodic ticker handles two concerns at once:
            //   1. Flush partial batches every BATCH_FLUSH_MS so latency
            //      stays bounded under low input rates.
            //   2. Check the `running` flag so shutdown happens promptly.
            // We deliberately do NOT wrap stream.next() in a timeout — doing
            // so registers and deregisters a tokio timer per event, which is
            // the dominant cost (~300x slowdown) at high throughput.
            let mut flush_ticker = tokio::time::interval(Duration::from_millis(BATCH_FLUSH_MS));
            flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            'consume: loop {
                if !cb.allow_request() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    continue;
                }

                tokio::select! {
                    biased;
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(msg)) => {
                                cb.record_success();

                                if let Some(payload) = msg.payload() {
                                    if let Ok(text) = std::str::from_utf8(payload) {
                                        if let Ok(json) =
                                            serde_json::from_str::<serde_json::Value>(text)
                                        {
                                            let event_type = json
                                                .get("event_type")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("KafkaEvent");
                                            let event = json_to_event(event_type, &json);
                                            batch.push(event);

                                            // Flush as soon as the batch is full.
                                            if batch.len() >= BATCH_MAX {
                                                let drained = std::mem::replace(
                                                    &mut batch,
                                                    Vec::with_capacity(BATCH_MAX),
                                                );
                                                if tx.send(drained).await.is_err() {
                                                    break 'consume;
                                                }
                                            }
                                        }
                                    }
                                }
                                // Note: per-message offset commit deliberately omitted.
                                // Per-message async commits create lock contention with
                                // the consumer poll loop and dominate throughput on
                                // high-volume topics. Offsets are flushed implicitly on
                                // rebalance / close; for at-least-once delivery, callers
                                // should periodically call commit_consumer_state().
                                let _ = &msg;
                            }
                            Some(Err(e)) => {
                                cb.record_failure();
                                let failures = cb.consecutive_failures();
                                let backoff = Duration::from_millis(100 * 2u64.pow(failures.min(7)));
                                error!(
                                    "Managed Kafka {} consumer error (cb_state={}, failures={}, backoff {:?}): {}",
                                    name, cb.state(), failures, backoff, e
                                );
                                tokio::time::sleep(backoff).await;
                            }
                            None => break 'consume,
                        }
                    }
                    _ = flush_ticker.tick() => {
                        // Flush any partial batch on the timer to keep
                        // latency bounded under low input rates.
                        if !batch.is_empty() {
                            let drained = std::mem::replace(
                                &mut batch,
                                Vec::with_capacity(BATCH_MAX),
                            );
                            if tx.send(drained).await.is_err() {
                                break 'consume;
                            }
                        }
                        if !running.load(Ordering::SeqCst) {
                            break 'consume;
                        }
                    }
                }
            }

            // Final flush on shutdown so we don't lose buffered events.
            if !batch.is_empty() {
                let _ = tx.send(batch).await;
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

    fn create_sink(
        &mut self,
        topic: &str,
        _params: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let producer = self.ensure_producer()?;
        Ok(Arc::new(KafkaSharedSink {
            sink_name: format!("{}::{}", self.connector_name, topic),
            topic: topic.to_string(),
            producer,
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
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

    async fn send(&self, event: &Event) -> Result<(), SinkError> {
        let payload = event.to_sink_payload();

        let record = FutureRecord::to(&self.topic)
            .payload(&payload)
            .key(&*event.event_type);

        // Non-blocking enqueue: hand the record to librdkafka's internal queue
        // and rely on `linger.ms` + `batch.size` to coalesce many enqueued
        // records into a single broker round-trip. Awaiting on every send
        // serialises producer throughput at the broker round-trip latency
        // (~10ms = ~100 events/sec ceiling), which negates batching entirely.
        // Errors at enqueue-time are returned synchronously; in-flight
        // delivery errors are reported by the producer's background thread.
        match self.producer.send_result(record) {
            Ok(_delivery_future) => Ok(()),
            Err((e, _)) => Err(SinkError::other(format!("kafka enqueue: {e}"))),
        }
    }

    async fn flush(&self) -> Result<(), SinkError> {
        self.producer
            .flush(Duration::from_secs(10))
            .map_err(|e| SinkError::other(format!("kafka flush: {}", e)))
    }

    async fn close(&self) -> Result<(), SinkError> {
        self.flush().await
    }
}
