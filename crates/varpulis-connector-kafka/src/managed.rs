//! Managed Kafka connector -- shares a single producer across all sinks

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use tokio::sync::mpsc;
use tracing::{error, info};
use varpulis_connector_api::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use varpulis_connector_api::decode::EventDecoder;
use varpulis_connector_api::sink::{Sink, SinkError};
use varpulis_connector_api::{
    ConnectorError, ConnectorHealthReport, EngineOffsetRegistry, ManagedConnector,
};
use varpulis_core::Event;
use varpulis_dead_letter::DeadLetterQueue;

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
            | "exactly_once"
            | "transactional_id"
    )
}

/// Per-topic source state: the shared consumer handle used by
/// `commit_source_offsets` to translate a per-partition offset map into a
/// `TopicPartitionList` and issue a sync commit.
///
/// The highest-observed offset per partition is maintained in a separate
/// map inside the consume task, mirrored into the engine-wide registry,
/// and the offset values flow into this connector via the `offsets`
/// argument to `commit_source_offsets` — so we deliberately don't store a
/// second copy here.
struct SourceState {
    consumer: Arc<StreamConsumer>,
}

/// Apply the pending per-partition offsets collected in the current batch
/// to both the source-local `offsets` map and (optionally) the engine-wide
/// registry keyed by connector name. Called only after `tx.send(batch)` has
/// successfully handed ownership of the events to the engine.
fn commit_pending_offsets(
    pending: &mut HashMap<i32, i64>,
    offsets: &Arc<Mutex<HashMap<i32, i64>>>,
    engine_offsets: Option<&EngineOffsetRegistry>,
    connector_name: &str,
) {
    if pending.is_empty() {
        return;
    }
    if let Ok(mut map) = offsets.lock() {
        for (partition, offset) in pending.iter() {
            let slot = map.entry(*partition).or_insert(*offset);
            if *offset > *slot {
                *slot = *offset;
            }
        }
    }
    if let Some(registry) = engine_offsets {
        if let Ok(mut reg) = registry.lock() {
            let entry = reg.entry(connector_name.to_string()).or_default();
            for (partition, offset) in pending.iter() {
                let slot = entry.entry(*partition).or_insert(*offset);
                if *offset > *slot {
                    *slot = *offset;
                }
            }
        }
    }
    pending.clear();
}

/// Shared transaction coordination for all `KafkaSharedSink` instances
/// that share a single `FutureProducer`.
///
/// Kafka transactions are producer-scoped: `begin_transaction`,
/// `commit_transaction`, and `abort_transaction` must be called exactly
/// once per epoch, not once per sink. The engine's `commit_sinks()` loop
/// iterates ALL sinks and calls each 2PC method on each, so we use CAS
/// on an atomic `phase` to ensure only the first caller per transition
/// reaches librdkafka.
///
/// Phase state machine: `0 (idle)` → `1 (active)` → `2 (prepared)` → `0`.
struct SharedTxnState {
    epoch: AtomicU64,
    phase: AtomicU8,
    /// Consumer offsets staged via `stage_txn_offsets`, to be folded into the
    /// current transaction on `commit` (C4 — committed atomically with the
    /// buffered output). Drained by the sink that wins the commit CAS.
    pending_offsets: Mutex<Vec<(Arc<StreamConsumer>, TopicPartitionList)>>,
}

impl SharedTxnState {
    fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            phase: AtomicU8::new(0),
            pending_offsets: Mutex::new(Vec::new()),
        }
    }
}

/// Managed Kafka connector that owns a single producer connection.
///
/// - **Sources**: each `start_source` call creates a new `StreamConsumer`
///   (Kafka consumers are not Clone) and registers its state in
///   `sources` so the connector can snapshot and commit offsets per topic.
/// - **Sinks**: the `FutureProducer` is Clone-able and shared across all
///   sinks. When `exactly_once` is enabled the producer is transactional
///   and all sinks share a `SharedTxnState` to coordinate 2PC calls.
pub struct ManagedKafkaConnector {
    connector_name: String,
    config: KafkaConfig,
    producer: Option<FutureProducer>,
    running: Arc<AtomicBool>,
    /// topic → source state (consumer + offsets map).
    sources: Arc<RwLock<HashMap<String, SourceState>>>,
    /// Optional engine-wide registry that mirrors offsets into a shared
    /// map keyed by connector name, so the engine's checkpoint can pick
    /// them up without polling each source.
    engine_offsets: Option<EngineOffsetRegistry>,
    /// Shared transaction state for exactly-once sinks. `None` when
    /// operating in fire-and-forget (at-least-once) mode.
    txn_state: Option<Arc<SharedTxnState>>,
    /// Cooperative source-pause flag shared with the engine. While set, the
    /// consumer loop stops pulling new records (after flushing what it holds)
    /// so the checkpoint barrier can drain in-flight events and snapshot a
    /// coherent offset set. `None` until the driver wires it at startup.
    source_paused: Option<Arc<AtomicBool>>,
    /// Count of source records that could not be turned into an `Event` — a
    /// malformed payload that failed to decode, or an oversized record rejected
    /// before decode. Shared with the spawned consume task, which increments it,
    /// and read back via [`decode_failures`](Self::decode_failures) and the
    /// connector health report. Audit HIGH: these used to be dropped silently
    /// (a bare `debug!`) while the offset advanced past them; the counter makes
    /// every drop observable.
    decode_failures: Arc<AtomicU64>,
    /// Optional dead-letter sink for raw, undecodable source payloads. When set
    /// (via [`with_dead_letter_queue`](Self::with_dead_letter_queue)) the
    /// original bytes of every dropped record are preserved here for inspection
    /// or replay instead of being lost. `None` = count + warn only.
    dead_letter: Option<Arc<DeadLetterQueue>>,
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
            sources: Arc::new(RwLock::new(HashMap::new())),
            engine_offsets: None,
            txn_state: None,
            source_paused: None,
            decode_failures: Arc::new(AtomicU64::new(0)),
            dead_letter: None,
        }
    }

    /// Attach a dead-letter queue for raw source payloads that fail to decode
    /// (malformed) or exceed the size limit. Without one, such records are still
    /// counted and logged; with one, their original bytes are additionally
    /// preserved for inspection or replay so nothing is silently dropped.
    pub fn with_dead_letter_queue(mut self, dlq: Arc<DeadLetterQueue>) -> Self {
        self.dead_letter = Some(dlq);
        self
    }

    /// Number of source records dropped because they could not be decoded into
    /// an `Event` or exceeded the payload size limit. A non-zero value means
    /// poison records were observed (and dead-lettered, if a DLQ is attached),
    /// never silently discarded.
    pub fn decode_failures(&self) -> u64 {
        self.decode_failures.load(Ordering::Relaxed)
    }

    fn ensure_producer(&mut self) -> Result<FutureProducer, ConnectorError> {
        if let Some(producer) = &self.producer {
            return Ok(producer.clone());
        }

        let transactional = self.config.transactional_id.is_some();

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("message.timeout.ms", "30000")
            .set("linger.ms", "20")
            .set("batch.size", "1048576")
            .set("compression.type", "lz4")
            .set("queue.buffering.max.messages", "1000000")
            .set("queue.buffering.max.kbytes", "1048576");

        if transactional {
            let tid = self.config.transactional_id.as_deref().unwrap();
            client_config
                .set("transactional.id", tid)
                .set("enable.idempotence", "true")
                .set("acks", "all");
        } else {
            client_config.set("acks", "1");
        }

        for (k, v) in &self.config.properties {
            if is_vpl_only_property(k) {
                continue;
            }
            client_config.set(k, v);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        if transactional {
            producer
                .init_transactions(Duration::from_secs(30))
                .map_err(|e| ConnectorError::ConnectionFailed(format!("init_transactions: {e}")))?;
            self.txn_state = Some(Arc::new(SharedTxnState::new()));
            info!(
                "Managed Kafka {} producer initialized with exactly-once semantics",
                self.connector_name
            );
        }

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

    fn health(&self) -> ConnectorHealthReport {
        ConnectorHealthReport {
            decode_failures: self.decode_failures.load(Ordering::Relaxed),
            ..Default::default()
        }
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

        let consumer = Arc::new(consumer);

        if let Ok(mut map) = self.sources.write() {
            map.insert(
                topic.to_string(),
                SourceState {
                    consumer: consumer.clone(),
                },
            );
        }

        // Task-local offset map; mirrored into the engine-wide registry
        // (if any) after each successful batch send.
        let offsets: Arc<Mutex<HashMap<i32, i64>>> = Arc::new(Mutex::new(HashMap::new()));

        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let name = self.connector_name.clone();
        let topic_owned = topic.to_string();
        let engine_offsets = self.engine_offsets.clone();
        let source_paused = self.source_paused.clone();
        let decode_failures = self.decode_failures.clone();
        let dead_letter = self.dead_letter.clone();

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
            // Larger batches amortize the per-batch overhead in the
            // run-loop's mpsc channel + tokio::select! + process_batch
            // call chain. The Kafka I/O agent showed the run.rs channel
            // interaction costs ~30% (104k passthrough → 71k end-to-end).
            // Widening from 256/5ms to 1024/20ms reduces the number of
            // channel sends by 4× and lets process_batch see fatter
            // batches. Latency impact: +15ms worst-case per batch.
            const BATCH_MAX: usize = 1024;
            const BATCH_FLUSH_MS: u64 = 20;
            let mut batch: Vec<Event> = Vec::with_capacity(BATCH_MAX);
            // Highest offset per partition seen in the current batch, applied
            // to the shared `offsets` map only after tx.send() succeeds — so
            // the checkpoint snapshot always refers to records that are
            // already in the engine's pipeline.
            let mut pending_offsets: HashMap<i32, i64> = HashMap::new();

            // Periodic ticker handles two concerns at once:
            //   1. Flush partial batches every BATCH_FLUSH_MS so latency
            //      stays bounded under low input rates.
            //   2. Check the `running` flag so shutdown happens promptly.
            // We deliberately do NOT wrap stream.next() in a timeout — doing
            // so registers and deregisters a tokio timer per event, which is
            // the dominant cost (~300x slowdown) at high throughput.
            let mut flush_ticker = tokio::time::interval(Duration::from_millis(BATCH_FLUSH_MS));
            flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Streaming JSON→Event decoder with per-task key/type interning:
            // steady-state schemas decode without allocating field names.
            let mut decoder = EventDecoder::new();

            // Tracks whether the breaker has unresolved failures. In the
            // steady state this stays false and the loop skips both
            // `allow_request()` and `record_success()` — each takes a
            // mutex — saving two locks per message. The breaker still
            // gates consumption after errors exactly as before.
            let mut cb_degraded = false;

            'consume: loop {
                // C3 barrier alignment: while the engine has paused sources for
                // a checkpoint drain, flush what we've already decoded (so it
                // becomes drainable and its offsets are mirrored) and stop
                // pulling new records until the barrier resumes us. Without this
                // the barrier's drain could never reach an empty channel under
                // sustained input.
                if let Some(ref paused) = source_paused {
                    if paused.load(Ordering::Relaxed) {
                        if !batch.is_empty() {
                            let drained =
                                std::mem::replace(&mut batch, Vec::with_capacity(BATCH_MAX));
                            if tx.send(drained).await.is_err() {
                                break 'consume;
                            }
                            commit_pending_offsets(
                                &mut pending_offsets,
                                &offsets,
                                engine_offsets.as_ref(),
                                &name,
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        if !running.load(Ordering::SeqCst) {
                            break 'consume;
                        }
                        continue;
                    }
                }

                if cb_degraded && !cb.allow_request() {
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
                                if cb_degraded {
                                    cb.record_success();
                                    cb_degraded = false;
                                }

                                let partition = msg.partition();
                                let msg_offset = msg.offset();
                                let mut pushed = false;

                                if let Some(payload) = msg.payload() {
                                    if payload.len()
                                        > varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES
                                    {
                                        // Oversized record: reject before decode, but do NOT
                                        // drop it silently. Count it, warn with its
                                        // coordinates, and dead-letter the raw bytes when a
                                        // DLQ is attached so it stays recoverable.
                                        decode_failures.fetch_add(1, Ordering::Relaxed);
                                        tracing::warn!(
                                            topic = %topic_owned,
                                            partition,
                                            offset = msg_offset,
                                            payload_len = payload.len(),
                                            limit = varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES,
                                            "Managed Kafka {}: payload too large, dead-lettered and skipped",
                                            name,
                                        );
                                        if let Some(ref dlq) = dead_letter {
                                            dlq.write_raw(
                                                &name,
                                                "kafka source: payload exceeds size limit",
                                                &format!(
                                                    "topic={topic_owned} partition={partition} offset={msg_offset}"
                                                ),
                                                payload,
                                            );
                                        }
                                    } else {
                                        match decoder.decode("KafkaEvent", payload) {
                                            Ok(event) => {
                                                batch.push(event);
                                                pushed = true;
                                            }
                                            Err(e) => {
                                                // Audit HIGH: a decode failure used to be a
                                                // bare `debug!` while the offset advanced past
                                                // the record (via later records' high-water
                                                // mark) — a silent, unrecoverable gap that
                                                // breaks the "never silently dropped" promise.
                                                // Now: count it, `warn!` with the exact
                                                // topic/partition/offset, and dead-letter the
                                                // raw bytes when a DLQ is attached.
                                                decode_failures.fetch_add(1, Ordering::Relaxed);
                                                tracing::warn!(
                                                    topic = %topic_owned,
                                                    partition,
                                                    offset = msg_offset,
                                                    error = %e,
                                                    "Managed Kafka {}: decode failed, dead-lettered and skipped",
                                                    name,
                                                );
                                                if let Some(ref dlq) = dead_letter {
                                                    dlq.write_raw(
                                                        &name,
                                                        &format!(
                                                            "kafka source: decode failed: {e}"
                                                        ),
                                                        &format!(
                                                            "topic={topic_owned} partition={partition} offset={msg_offset}"
                                                        ),
                                                        payload,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }

                                // Track the highest offset per partition in
                                // the pending batch. Committed only after
                                // the batch is accepted by the engine channel.
                                if pushed {
                                    let slot = pending_offsets.entry(partition).or_insert(msg_offset);
                                    if msg_offset > *slot {
                                        *slot = msg_offset;
                                    }
                                }

                                // Flush as soon as the batch is full.
                                if batch.len() >= BATCH_MAX {
                                    let drained = std::mem::replace(
                                        &mut batch,
                                        Vec::with_capacity(BATCH_MAX),
                                    );
                                    if tx.send(drained).await.is_err() {
                                        break 'consume;
                                    }
                                    commit_pending_offsets(
                                        &mut pending_offsets,
                                        &offsets,
                                        engine_offsets.as_ref(),
                                        &name,
                                    );
                                }
                            }
                            Some(Err(e)) => {
                                cb.record_failure();
                                cb_degraded = true;
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
                            commit_pending_offsets(
                                &mut pending_offsets,
                                &offsets,
                                engine_offsets.as_ref(),
                                &name,
                            );
                        }
                        if !running.load(Ordering::SeqCst) {
                            break 'consume;
                        }
                    }
                }
            }

            // Final flush on shutdown so we don't lose buffered events.
            if !batch.is_empty() && tx.send(batch).await.is_ok() {
                commit_pending_offsets(
                    &mut pending_offsets,
                    &offsets,
                    engine_offsets.as_ref(),
                    &name,
                );
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
            txn_state: self.txn_state.clone(),
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn shutdown(&mut self) -> Result<(), ConnectorError> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(ref state) = self.txn_state {
            if state.phase.load(Ordering::SeqCst) > 0 {
                if let Some(ref producer) = self.producer {
                    let _ = producer.abort_transaction(Duration::from_secs(5));
                }
                state.phase.store(0, Ordering::SeqCst);
            }
        }
        self.producer = None;
        if let Ok(mut map) = self.sources.write() {
            map.clear();
        }
        info!("Managed Kafka {} shut down", self.connector_name);
        Ok(())
    }

    fn set_engine_offsets_registry(&mut self, registry: EngineOffsetRegistry) {
        self.engine_offsets = Some(registry);
    }

    fn set_source_pause_handle(&mut self, handle: Arc<AtomicBool>) {
        self.source_paused = Some(handle);
    }

    async fn commit_source_offsets(
        &self,
        topic: &str,
        offsets: &HashMap<i32, i64>,
    ) -> Result<(), ConnectorError> {
        if offsets.is_empty() {
            return Ok(());
        }
        let consumer = {
            let guard = self
                .sources
                .read()
                .map_err(|_| ConnectorError::SendFailed("sources lock poisoned".into()))?;
            match guard.get(topic) {
                Some(state) => state.consumer.clone(),
                None => {
                    return Err(ConnectorError::SendFailed(format!(
                        "commit_source_offsets called before start_source for topic {topic}"
                    )));
                }
            }
        };

        let mut tpl = TopicPartitionList::new();
        for (&partition, &offset) in offsets {
            // Kafka commit semantics: committed offset = next offset to read.
            // We track the highest offset we've accepted, so commit offset + 1.
            tpl.add_partition_offset(topic, partition, Offset::Offset(offset + 1))
                .map_err(|e| ConnectorError::SendFailed(format!("build TPL for commit: {e}")))?;
        }

        consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| ConnectorError::SendFailed(format!("commit offsets: {e}")))
    }

    async fn stage_txn_offsets(
        &self,
        topic: &str,
        offsets: &HashMap<i32, i64>,
    ) -> Result<(), ConnectorError> {
        if offsets.is_empty() {
            return Ok(());
        }
        // Only transactional connectors stage offsets into a producer txn; a
        // non-transactional connector has no txn to fold them into.
        let Some(ref txn_state) = self.txn_state else {
            return Ok(());
        };
        let consumer = {
            let guard = self
                .sources
                .read()
                .map_err(|_| ConnectorError::SendFailed("sources lock poisoned".into()))?;
            match guard.get(topic) {
                Some(state) => state.consumer.clone(),
                None => {
                    return Err(ConnectorError::SendFailed(format!(
                        "stage_txn_offsets called before start_source for topic {topic}"
                    )));
                }
            }
        };

        let mut tpl = TopicPartitionList::new();
        for (&partition, &offset) in offsets {
            // Commit offset = next offset to read (highest accepted + 1).
            tpl.add_partition_offset(topic, partition, Offset::Offset(offset + 1))
                .map_err(|e| ConnectorError::SendFailed(format!("build TPL for stage: {e}")))?;
        }

        txn_state
            .pending_offsets
            .lock()
            .map_err(|_| ConnectorError::SendFailed("pending_offsets lock poisoned".into()))?
            .push((consumer, tpl));
        Ok(())
    }
}

/// Lightweight sink handle that publishes via a shared `FutureProducer`.
///
/// When `txn_state` is `Some`, the sink participates in exactly-once 2PC.
/// Multiple sinks sharing the same producer coordinate via CAS on the
/// shared `SharedTxnState` so that `begin_transaction` /
/// `commit_transaction` / `abort_transaction` are called exactly once
/// per epoch regardless of how many sinks the engine iterates.
struct KafkaSharedSink {
    sink_name: String,
    topic: String,
    producer: FutureProducer,
    txn_state: Option<Arc<SharedTxnState>>,
}

impl KafkaSharedSink {
    /// Fire-and-forget enqueue of a whole batch. One serialization buffer is
    /// reused across the batch (librdkafka copies the payload on enqueue),
    /// avoiding a `Vec` allocation and an async round-trip per event.
    fn enqueue_batch(&self, events: &[Arc<Event>], topic: &str) -> Result<(), SinkError> {
        let mut payload = Vec::with_capacity(256);
        for event in events {
            payload.clear();
            event.write_sink_payload(&mut payload);
            let record = FutureRecord::to(topic)
                .payload(&payload)
                .key(&*event.event_type);
            if let Err((e, _)) = self.producer.send_result(record) {
                return Err(SinkError::other(format!("kafka enqueue: {e}")));
            }
        }
        Ok(())
    }
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

        match self.producer.send_result(record) {
            Ok(_delivery_future) => Ok(()),
            Err((e, _)) => Err(SinkError::other(format!("kafka enqueue: {e}"))),
        }
    }

    async fn send_batch(&self, events: &[std::sync::Arc<Event>]) -> Result<(), SinkError> {
        self.enqueue_batch(events, &self.topic)
    }

    async fn send_batch_to_topic(
        &self,
        events: &[std::sync::Arc<Event>],
        topic: &str,
    ) -> Result<(), SinkError> {
        self.enqueue_batch(events, topic)
    }

    async fn flush(&self) -> Result<(), SinkError> {
        self.producer
            .flush(Duration::from_secs(10))
            .map_err(|e| SinkError::other(format!("kafka flush: {}", e)))
    }

    async fn close(&self) -> Result<(), SinkError> {
        self.flush().await
    }

    fn supports_exactly_once(&self) -> bool {
        self.txn_state.is_some()
    }

    async fn begin_epoch(&self, checkpoint_id: u64) -> Result<(), SinkError> {
        if let Some(ref state) = self.txn_state {
            if state
                .phase
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                state.epoch.store(checkpoint_id, Ordering::SeqCst);
                self.producer
                    .begin_transaction()
                    .map_err(|e| SinkError::other(format!("begin_transaction: {e}")))?;
            }
        }
        Ok(())
    }

    async fn prepare_commit(&self, _checkpoint_id: u64) -> Result<(), SinkError> {
        if let Some(ref state) = self.txn_state {
            if state
                .phase
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.producer
                    .flush(Duration::from_secs(30))
                    .map_err(|e| SinkError::other(format!("prepare_commit flush: {e}")))?;
            }
        }
        Ok(())
    }

    async fn commit(&self, _checkpoint_id: u64) -> Result<(), SinkError> {
        if let Some(ref state) = self.txn_state {
            if state
                .phase
                .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // C4 — fold any staged consumer offsets into THIS transaction so
                // they commit atomically with the buffered output. On crash the
                // transaction coordinator aborts both together; there is no
                // window where output is visible but the offset has not advanced.
                let staged = {
                    let mut guard = state
                        .pending_offsets
                        .lock()
                        .map_err(|_| SinkError::other("pending_offsets lock poisoned"))?;
                    std::mem::take(&mut *guard)
                };
                // Send offsets + commit; on any failure restore the phase to 2
                // (prepared) so the barrier's abort() actually aborts the still
                // open transaction instead of no-op'ing on phase 0 (§7 hardening).
                // These librdkafka calls are synchronous, so a plain closure with
                // `?` captures the first error without crossing an await.
                let outcome = (|| {
                    for (consumer, tpl) in &staged {
                        let cgm = consumer.group_metadata().ok_or_else(|| {
                            SinkError::other("consumer group_metadata unavailable")
                        })?;
                        self.producer
                            .send_offsets_to_transaction(tpl, &cgm, Duration::from_secs(30))
                            .map_err(|e| {
                                SinkError::other(format!("send_offsets_to_transaction: {e}"))
                            })?;
                    }
                    self.producer
                        .commit_transaction(Duration::from_secs(30))
                        .map_err(|e| SinkError::other(format!("commit_transaction: {e}")))
                })();
                if outcome.is_err() {
                    state.phase.store(2, Ordering::SeqCst);
                }
                return outcome;
            }
        }
        Ok(())
    }

    async fn abort(&self, _checkpoint_id: u64) -> Result<(), SinkError> {
        if let Some(ref state) = self.txn_state {
            // Discard any offsets staged for the aborted transaction so they
            // don't leak into the next epoch.
            if let Ok(mut guard) = state.pending_offsets.lock() {
                guard.clear();
            }
            if state.phase.swap(0, Ordering::SeqCst) > 0 {
                self.producer
                    .abort_transaction(Duration::from_secs(10))
                    .map_err(|e| SinkError::other(format!("abort_transaction: {e}")))?;
            }
        }
        Ok(())
    }
}
