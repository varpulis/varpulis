//! Kafka source and sink connectors using rdkafka.
//!
//! This crate provides full Kafka connectivity for the Varpulis CEP engine
//! via the `rdkafka` library. It includes source connectors for consuming
//! from Kafka topics and sink connectors for producing events.

pub mod managed;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
pub use managed::ManagedKafkaConnector;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use varpulis_connector_api::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use varpulis_connector_api::helpers::json_to_event;
use varpulis_connector_api::{
    ConfigParamInfo, ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory,
    EngineOffsetRegistry, ManagedConnector, Sink, SinkConnector, SinkConnectorAdapter,
    SourceConnector,
};
use varpulis_core::Event;

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

static KAFKA_PARAMS: &[ConfigParamInfo] = &[
    ConfigParamInfo {
        name: "brokers",
        description: "Kafka broker addresses",
        required: true,
        default_value: None,
    },
    ConfigParamInfo {
        name: "topic",
        description: "Kafka topic",
        required: true,
        default_value: None,
    },
    ConfigParamInfo {
        name: "group_id",
        description: "Consumer group ID",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "auto_offset_reset",
        description: "Where to start consuming if no offset is committed: 'earliest' or 'latest'",
        required: false,
        default_value: Some("latest"),
    },
    ConfigParamInfo {
        name: "exactly_once",
        description: "Enable exactly-once semantics",
        required: false,
        default_value: Some("false"),
    },
    ConfigParamInfo {
        name: "transactional_id",
        description: "Transactional producer ID",
        required: false,
        default_value: None,
    },
    // Security
    ConfigParamInfo {
        name: "profile",
        description: "Credentials profile name from external credentials file",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "security_protocol",
        description: "Protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL",
        required: false,
        default_value: Some("PLAINTEXT"),
    },
    ConfigParamInfo {
        name: "sasl_mechanism",
        description: "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "sasl_username",
        description: "SASL username (use credentials file for production)",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "sasl_password",
        description: "SASL password (use credentials file for production)",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "ssl_ca_location",
        description: "Path to CA certificate (PEM)",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "ssl_certificate_location",
        description: "Path to client certificate (PEM)",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "ssl_key_location",
        description: "Path to client private key (PEM)",
        required: false,
        default_value: None,
    },
];

static KAFKA_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "kafka",
    display_name: "Apache Kafka",
    description: "High-throughput distributed event streaming platform",
    feature_flag: "kafka",
    supports_source: true,
    supports_sink: true,
    supports_managed: true,
    config_params: KAFKA_PARAMS,
};

struct KafkaFactory;

impl ConnectorFactory for KafkaFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &KAFKA_INFO
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
        let kafka_config = KafkaConfig::new(&config.url, &topic);
        let sink = KafkaSink::new("kafka", kafka_config)?;
        Ok(Box::new(sink))
    }

    fn create_managed(
        &self,
        name: &str,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn ManagedConnector>, ConnectorError> {
        let topic = config.topic.as_deref().unwrap_or("events");
        let brokers = if config.url.is_empty() {
            config
                .properties
                .get("brokers")
                .cloned()
                .unwrap_or_default()
        } else {
            config.url.clone()
        };
        let mut kafka_config =
            KafkaConfig::new(&brokers, topic).with_properties(config.properties.clone());
        if let Some(group_id) = config.properties.get("group_id") {
            kafka_config = kafka_config.with_group_id(group_id);
        }
        if config
            .properties
            .get("exactly_once")
            .is_some_and(|v| v == "true")
        {
            let tid = config
                .properties
                .get("transactional_id")
                .cloned()
                .unwrap_or_else(|| format!("varpulis-{name}"));
            kafka_config = kafka_config.with_transactional_id(&tid);
        }
        Ok(Box::new(ManagedKafkaConnector::new(name, kafka_config)))
    }

    fn create_engine_sink(
        &self,
        name: &str,
        config: &ConnectorConfig,
        topic_override: Option<&str>,
        _context_name: Option<&str>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let topic = topic_override
            .map(|s| s.to_string())
            .or_else(|| config.topic.clone())
            .unwrap_or_else(|| format!("{name}-output"));
        let kafka_config =
            KafkaConfig::new(&config.url, &topic).with_properties(config.properties.clone());
        let sink = KafkaSink::new(name, kafka_config)?;
        Ok(Arc::new(SinkConnectorAdapter::new(name, Box::new(sink))))
    }
}

inventory::submit! { &KafkaFactory as &dyn ConnectorFactory }

// =============================================================================
// Kafka Configuration
// =============================================================================

/// Kafka configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct KafkaConfig {
    /// Kafka broker addresses (comma-separated).
    pub brokers: String,
    /// Kafka topic name.
    pub topic: String,
    /// Consumer group ID (optional).
    pub group_id: Option<String>,
    /// Additional rdkafka client properties.
    pub properties: indexmap::IndexMap<String, String>,
    /// When set, enables Kafka exactly-once semantics via transactional producer.
    /// The value must be unique per application instance.
    pub transactional_id: Option<String>,
}

impl KafkaConfig {
    /// Create a new Kafka configuration with broker addresses and topic.
    pub fn new(brokers: &str, topic: &str) -> Self {
        Self {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            group_id: None,
            properties: indexmap::IndexMap::new(),
            transactional_id: None,
        }
    }

    /// Set the consumer group ID.
    pub fn with_group_id(mut self, group_id: &str) -> Self {
        self.group_id = Some(group_id.to_string());
        self
    }

    /// Set additional rdkafka client properties.
    pub fn with_properties(mut self, props: indexmap::IndexMap<String, String>) -> Self {
        self.properties = props;
        self
    }

    /// Enable exactly-once semantics with the given transactional producer ID.
    pub fn with_transactional_id(mut self, id: &str) -> Self {
        self.transactional_id = Some(id.to_string());
        self
    }
}

// =============================================================================
// Property translation helpers
// =============================================================================

/// Translate VPL underscore-convention property names to rdkafka dot-notation.
static PARAM_MAPPING: &[(&str, &str)] = &[
    ("batch_size", "batch.size"),
    ("linger_ms", "linger.ms"),
    ("compression_type", "compression.type"),
    ("message_timeout_ms", "message.timeout.ms"),
    // SASL/SCRAM authentication
    ("security_protocol", "security.protocol"),
    ("sasl_mechanism", "sasl.mechanism"),
    ("sasl_username", "sasl.username"),
    ("sasl_password", "sasl.password"),
    // TLS / SSL
    ("ssl_ca_location", "ssl.ca.location"),
    ("ssl_certificate_location", "ssl.certificate.location"),
    ("ssl_key_location", "ssl.key.location"),
    ("ssl_key_password", "ssl.key.password"),
    (
        "ssl_endpoint_identification_algorithm",
        "ssl.endpoint.identification.algorithm",
    ),
];

/// Apply user-provided properties to a ClientConfig, translating
/// VPL underscore names to rdkafka dot-notation and skipping keys
/// that are already explicitly set by our code.
pub(crate) fn apply_properties(
    client_config: &mut ClientConfig,
    props: &indexmap::IndexMap<String, String>,
) {
    let mut translated = indexmap::IndexMap::new();
    for (k, v) in props {
        match k.as_str() {
            "bootstrap.servers" | "group.id" | "brokers" | "topic" | "group_id"
            | "exactly_once" | "transactional_id" => continue,
            _ => {}
        }

        let rdkafka_key = PARAM_MAPPING
            .iter()
            .find(|(vpl, _)| *vpl == k.as_str())
            .map(|(_, rdkafka)| *rdkafka)
            .unwrap_or(k.as_str());

        translated.insert(rdkafka_key.to_string(), v.clone());
    }

    for (k, v) in &translated {
        client_config.set(k, v);
    }
}

// =============================================================================
// Kafka Source
// =============================================================================

/// Kafka source connector with rdkafka.
///
/// ## Offset tracking and exactly-once
///
/// The consumer is created with `enable.auto.commit = false`, and this source
/// does **not** commit offsets on a per-message basis. Instead, every record
/// that has been successfully handed off to the engine channel updates an
/// in-memory `offsets: HashMap<partition, last_offset>`. The engine snapshots
/// that map into `EngineCheckpoint::source_offsets` at barrier time, and on
/// 2PC commit it calls [`SourceConnector::commit_offsets`], which translates
/// the map into a `TopicPartitionList` and commits it synchronously to the
/// consumer group coordinator. A subsequent recovery reads from those
/// committed offsets, giving end-to-end exactly-once when paired with a
/// transactional sink.
pub struct KafkaSource {
    name: String,
    config: KafkaConfig,
    running: Arc<AtomicBool>,
    /// Shared consumer handle, set during `start()`. Used by both the
    /// consume task and `commit_offsets()`.
    consumer: Arc<std::sync::RwLock<Option<Arc<StreamConsumer>>>>,
    /// Latest consumed offset per partition, updated only after the
    /// corresponding event has been accepted by the engine channel.
    offsets: Arc<Mutex<HashMap<i32, i64>>>,
    /// Optional engine-level registry. When set, the consume task mirrors
    /// `offsets` into `engine_offsets[self.name]` so the engine sees a
    /// consistent snapshot without having to poll the source.
    engine_offsets: Option<EngineOffsetRegistry>,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("name", &self.name)
            .field("topic", &self.config.topic)
            .field("running", &self.running.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl KafkaSource {
    /// Creates a new Kafka source connector.
    pub fn new(name: &str, config: KafkaConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: Arc::new(AtomicBool::new(false)),
            consumer: Arc::new(std::sync::RwLock::new(None)),
            offsets: Arc::new(Mutex::new(HashMap::new())),
            engine_offsets: None,
        }
    }

    /// Bind this source to an engine-wide offset registry so that the
    /// engine's checkpoint snapshots see this source's latest offsets
    /// without having to reach into the source itself.
    pub fn with_engine_offsets(mut self, registry: EngineOffsetRegistry) -> Self {
        self.engine_offsets = Some(registry);
        self
    }
}

#[async_trait]
impl SourceConnector for KafkaSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
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

        apply_properties(&mut client_config, &self.config.properties);

        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        consumer
            .subscribe(&[&self.config.topic])
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let consumer = Arc::new(consumer);
        if let Ok(mut slot) = self.consumer.write() {
            *slot = Some(consumer.clone());
        }

        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let name = self.name.clone();
        let offsets = self.offsets.clone();
        let engine_offsets = self.engine_offsets.clone();

        tokio::spawn(async move {
            info!("Kafka source {} started, consuming from topic", name);

            use futures_util::StreamExt;

            let mut stream = consumer.stream();
            let cb = CircuitBreaker::new(CircuitBreakerConfig {
                failure_threshold: 10,
                reset_timeout: Duration::from_secs(30),
            });

            while running.load(Ordering::SeqCst) {
                if !cb.allow_request() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                    Ok(Some(Ok(msg))) => {
                        cb.record_success();

                        let partition = msg.partition();
                        let msg_offset = msg.offset();
                        let mut accepted = false;

                        if let Some(payload) = msg.payload() {
                            if payload.len()
                                > varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES
                            {
                                warn!(
                                    "Kafka source {}: payload too large ({} bytes, max {}), skipped",
                                    name,
                                    payload.len(),
                                    varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES
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
                                        accepted = true;
                                    }
                                    Err(e) => {
                                        warn!("Kafka source {}: failed to parse JSON: {}", name, e);
                                    }
                                }
                            }
                        }

                        // Record the offset only once the engine has accepted
                        // the event. Offsets committed at checkpoint time will
                        // therefore always refer to records whose effects are
                        // already in engine state.
                        if accepted {
                            if let Ok(mut map) = offsets.lock() {
                                let slot = map.entry(partition).or_insert(msg_offset);
                                if msg_offset > *slot {
                                    *slot = msg_offset;
                                }
                            }
                            if let Some(registry) = &engine_offsets {
                                if let Ok(mut reg) = registry.lock() {
                                    let entry = reg.entry(name.clone()).or_default();
                                    let slot = entry.entry(partition).or_insert(msg_offset);
                                    if msg_offset > *slot {
                                        *slot = msg_offset;
                                    }
                                }
                            }
                        }
                    }
                    Ok(Some(Err(e))) => {
                        cb.record_failure();
                        let failures = cb.consecutive_failures();
                        let backoff = Duration::from_millis(100 * 2u64.pow(failures.min(7)));
                        error!(
                            "Kafka source {} error (cb_state={}, failures={}, backoff {:?}): {}",
                            name,
                            cb.state(),
                            failures,
                            backoff,
                            e
                        );
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

    fn supports_offset_checkpoint(&self) -> bool {
        true
    }

    fn snapshot_offsets(&self) -> HashMap<i32, i64> {
        self.offsets.lock().map(|g| g.clone()).unwrap_or_default()
    }

    async fn commit_offsets(&self, offsets: &HashMap<i32, i64>) -> Result<(), ConnectorError> {
        if offsets.is_empty() {
            return Ok(());
        }
        let consumer = {
            let guard = self
                .consumer
                .read()
                .map_err(|_| ConnectorError::SendFailed("consumer lock poisoned".into()))?;
            match guard.as_ref() {
                Some(c) => c.clone(),
                None => {
                    return Err(ConnectorError::SendFailed(
                        "commit_offsets called before start()".into(),
                    ));
                }
            }
        };

        let mut tpl = TopicPartitionList::new();
        for (&partition, &offset) in offsets {
            // Kafka commit semantics: committed offset = next offset to read.
            // We track last-consumed offset, so commit offset + 1.
            tpl.add_partition_offset(&self.config.topic, partition, Offset::Offset(offset + 1))
                .map_err(|e| ConnectorError::SendFailed(format!("build TPL for commit: {e}")))?;
        }

        consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| ConnectorError::SendFailed(format!("commit offsets: {e}")))
    }
}

// =============================================================================
// Kafka Sink
// =============================================================================

/// Kafka sink connector with rdkafka.
///
/// Supports two delivery modes:
/// - **At-least-once** (default): fire-and-forget with `acks=all`
/// - **Exactly-once**: transactional producer when `transactional_id` is set
pub struct KafkaSink {
    name: String,
    config: KafkaConfig,
    producer: FutureProducer,
    /// True when the producer was initialized with a transactional.id.
    transactional: bool,
}

impl std::fmt::Debug for KafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("name", &self.name)
            .field("transactional", &self.transactional)
            .finish_non_exhaustive()
    }
}

impl KafkaSink {
    /// Creates a new Kafka sink connector.
    pub fn new(name: &str, config: KafkaConfig) -> Result<Self, ConnectorError> {
        let transactional = config.transactional_id.is_some();

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "30000")
            .set("linger.ms", "5")
            .set("batch.size", "65536")
            .set("compression.type", "lz4")
            .set("acks", "all");

        if let Some(tid) = &config.transactional_id {
            client_config.set("transactional.id", tid);
            client_config.set("enable.idempotence", "true");
        }

        apply_properties(&mut client_config, &config.properties);

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        if transactional {
            producer
                .init_transactions(Duration::from_secs(30))
                .map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("Failed to init transactions: {}", e))
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

    /// Send a batch of events concurrently (non-transactional).
    pub async fn send_batch(&self, events: &[std::sync::Arc<Event>]) -> Result<(), ConnectorError> {
        let mut futures = Vec::with_capacity(events.len());

        for event in events {
            let payload = event.to_sink_payload();
            let record = FutureRecord::to(&self.config.topic)
                .payload(&payload)
                .key(&*event.event_type);

            match self.producer.send_result(record) {
                Ok(delivery_future) => futures.push(delivery_future),
                Err((e, _)) => {
                    return Err(ConnectorError::SendFailed(format!("enqueue failed: {}", e)));
                }
            }
        }

        let results = futures_util::future::join_all(futures).await;
        for result in results {
            match result {
                Ok(Ok(_)) => {}
                Ok(Err((e, _))) => {
                    return Err(ConnectorError::SendFailed(format!(
                        "delivery failed: {}",
                        e
                    )));
                }
                Err(_cancelled) => {
                    return Err(ConnectorError::SendFailed(
                        "delivery future cancelled".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl SinkConnector for KafkaSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let payload = event.to_sink_payload();

        let record = FutureRecord::to(&self.config.topic)
            .payload(&payload)
            .key(&*event.event_type);

        if self.transactional {
            // Transactional path still needs synchronous commit to provide
            // exactly-once semantics. Throughput here is intentionally bounded
            // by the transaction round-trip.
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
            // Non-transactional path: non-blocking enqueue via `send_result`.
            // Awaiting on every `producer.send(...).await` serialises
            // throughput at the broker round-trip latency (~5–10 ms ≈
            // 100–200 eps). Instead, hand the record to librdkafka's
            // internal queue and let `linger.ms` + `batch.size` coalesce
            // many records per broker round-trip. Delivery errors after
            // enqueue are reported via the producer's background thread
            // (and, when `DlqConfig` is configured at the engine layer,
            // routed to the DLQ). This matches the pattern used by
            // Arroyo's Kafka sink.
            //
            // See docs/development/kafka-source-batching.md for the full
            // throughput analysis.
            match self.producer.send_result(record) {
                Ok(_delivery_future) => {}
                Err((e, _)) => {
                    return Err(ConnectorError::SendFailed(format!("kafka enqueue: {e}")))
                }
            }
        }

        Ok(())
    }

    async fn send_to_topic(
        &self,
        events: &[std::sync::Arc<Event>],
        topic: &str,
    ) -> Result<(), ConnectorError> {
        let mut futures = Vec::with_capacity(events.len());
        for event in events {
            let payload = event.to_sink_payload();
            let record = FutureRecord::to(topic)
                .payload(&payload)
                .key(&*event.event_type);
            match self.producer.send_result(record) {
                Ok(delivery_future) => futures.push(delivery_future),
                Err((e, _)) => {
                    return Err(ConnectorError::SendFailed(format!("enqueue failed: {}", e)));
                }
            }
        }
        let results = futures_util::future::join_all(futures).await;
        for result in results {
            match result {
                Ok(Ok(_)) => {}
                Ok(Err((e, _))) => {
                    return Err(ConnectorError::SendFailed(format!(
                        "delivery failed: {}",
                        e
                    )));
                }
                Err(_cancelled) => {
                    return Err(ConnectorError::SendFailed(
                        "delivery future cancelled".to_string(),
                    ));
                }
            }
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

    fn supports_exactly_once(&self) -> bool {
        self.transactional
    }

    async fn begin_epoch(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        if self.transactional {
            self.producer
                .begin_transaction()
                .map_err(|e| ConnectorError::SendFailed(format!("begin_transaction: {e}")))?;
        }
        Ok(())
    }

    async fn prepare_commit(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        if self.transactional {
            // Flush ensures all enqueued messages are sent to the broker.
            // The transaction remains open — data is not yet visible to consumers.
            self.producer
                .flush(Duration::from_secs(30))
                .map_err(|e| ConnectorError::SendFailed(format!("prepare_commit flush: {e}")))?;
        }
        Ok(())
    }

    async fn commit(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        if self.transactional {
            self.producer
                .commit_transaction(Duration::from_secs(30))
                .map_err(|e| ConnectorError::SendFailed(format!("commit_transaction: {e}")))?;
        }
        Ok(())
    }

    async fn abort(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        if self.transactional {
            self.producer
                .abort_transaction(Duration::from_secs(10))
                .map_err(|e| ConnectorError::SendFailed(format!("abort_transaction: {e}")))?;
        }
        Ok(())
    }
}

/// Backward-compatible alias for [`KafkaSink`].
pub type KafkaSinkFull = KafkaSink;
/// Backward-compatible alias for [`KafkaSource`].
pub type KafkaSourceFull = KafkaSource;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config_new() {
        let config = KafkaConfig::new("localhost:9092", "my-topic");
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "my-topic");
        assert!(config.group_id.is_none());
        assert!(config.properties.is_empty());
        assert!(config.transactional_id.is_none());
    }

    #[test]
    fn test_kafka_config_with_group_id() {
        let config = KafkaConfig::new("broker1:9092,broker2:9092", "events")
            .with_group_id("my-consumer-group");
        assert_eq!(config.group_id.as_deref(), Some("my-consumer-group"));
    }

    #[test]
    fn test_kafka_config_with_transactional_id() {
        let config =
            KafkaConfig::new("localhost:9092", "output").with_transactional_id("txn-producer-1");
        assert_eq!(config.transactional_id.as_deref(), Some("txn-producer-1"));
    }

    #[test]
    fn test_kafka_config_with_properties() {
        let mut props = indexmap::IndexMap::new();
        props.insert("batch_size".to_string(), "16384".to_string());
        props.insert("linger_ms".to_string(), "10".to_string());

        let config = KafkaConfig::new("localhost:9092", "t").with_properties(props);
        assert_eq!(config.properties.len(), 2);
        assert_eq!(config.properties.get("batch_size").unwrap(), "16384");
    }

    #[test]
    fn test_kafka_config_serialization_roundtrip() {
        let config = KafkaConfig::new("broker:9092", "topic1")
            .with_group_id("grp")
            .with_transactional_id("txn-1");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: KafkaConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.brokers, "broker:9092");
        assert_eq!(deserialized.topic, "topic1");
        assert_eq!(deserialized.group_id.as_deref(), Some("grp"));
        assert_eq!(deserialized.transactional_id.as_deref(), Some("txn-1"));
    }

    #[test]
    fn test_apply_properties_translates_vpl_names() {
        let mut client_config = ClientConfig::new();
        let mut props = indexmap::IndexMap::new();
        props.insert("batch_size".to_string(), "32768".to_string());
        props.insert("linger_ms".to_string(), "5".to_string());
        props.insert("compression_type".to_string(), "snappy".to_string());
        props.insert("security_protocol".to_string(), "SASL_SSL".to_string());

        apply_properties(&mut client_config, &props);

        // Verify the properties were translated (ClientConfig doesn't expose
        // getters, but this validates no panic and the mapping runs)
    }

    #[test]
    fn test_apply_properties_skips_reserved_keys() {
        let mut client_config = ClientConfig::new();
        let mut props = indexmap::IndexMap::new();
        props.insert("bootstrap.servers".to_string(), "ignored".to_string());
        props.insert("group.id".to_string(), "ignored".to_string());
        props.insert("brokers".to_string(), "ignored".to_string());
        props.insert("topic".to_string(), "ignored".to_string());
        props.insert("group_id".to_string(), "ignored".to_string());
        props.insert("exactly_once".to_string(), "ignored".to_string());
        props.insert("transactional_id".to_string(), "ignored".to_string());

        // Should not panic; all keys are skipped
        apply_properties(&mut client_config, &props);
    }

    #[test]
    fn test_apply_properties_passthrough_unknown_keys() {
        let mut client_config = ClientConfig::new();
        let mut props = indexmap::IndexMap::new();
        props.insert("custom.setting".to_string(), "value".to_string());

        // Unknown keys are passed through as-is (no translation)
        apply_properties(&mut client_config, &props);
    }

    #[test]
    fn test_kafka_source_new() {
        let config = KafkaConfig::new("localhost:9092", "input-topic");
        let source = KafkaSource::new("my-source", config);
        assert_eq!(source.name, "my-source");
        assert!(!source.is_running());
    }

    #[test]
    fn test_kafka_info_static() {
        assert_eq!(KAFKA_INFO.connector_type, "kafka");
        assert_eq!(KAFKA_INFO.display_name, "Apache Kafka");
        assert!(KAFKA_INFO.supports_source);
        assert!(KAFKA_INFO.supports_sink);
        assert!(KAFKA_INFO.supports_managed);
        assert!(!KAFKA_INFO.config_params.is_empty());
    }

    #[test]
    fn test_kafka_params_contains_required_fields() {
        let required: Vec<&str> = KAFKA_PARAMS
            .iter()
            .filter(|p| p.required)
            .map(|p| p.name)
            .collect();
        assert!(required.contains(&"brokers"));
        assert!(required.contains(&"topic"));
    }
}
