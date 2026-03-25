//! Managed MQTT connector -- single connection shared across all sources and sinks

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use rustc_hash::{FxBuildHasher, FxHashSet};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use varpulis_connector_api::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use varpulis_connector_api::helpers::json_to_value;
use varpulis_connector_api::sink::{Sink, SinkError};
use varpulis_connector_api::{ConnectorError, ConnectorHealthReport, ManagedConnector};
use varpulis_core::event::{FieldKey, FxIndexMap};
use varpulis_core::Event;

use crate::{apply_tls, MqttConfig};

fn qos_from_u8(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

/// Managed MQTT connector with separate connections for source and sink.
///
/// Uses two MQTT connections to avoid eventloop contention:
/// - Source connection: dedicated to receiving subscribed messages
/// - Sink connection: dedicated to publishing output events
pub struct ManagedMqttConnector {
    connector_name: String,
    config: MqttConfig,
    /// Source connection (subscriptions + event receive)
    source_client: Option<AsyncClient>,
    /// Sink connection (publish only, no subscriptions)
    sink_client: Option<AsyncClient>,
    running: Arc<AtomicBool>,
    subscribed_topics: FxHashSet<String>,
    /// Dedicated clients created via `client_id` param (kept alive for cleanup)
    dedicated_clients: Vec<AsyncClient>,
    /// Health tracking: total messages received across all source loops
    messages_received: Arc<AtomicU64>,
    /// Health tracking: last error string
    last_error: Arc<Mutex<Option<String>>>,
    /// Health tracking: time of last received message
    last_message_time: Arc<Mutex<Option<Instant>>>,
}

impl std::fmt::Debug for ManagedMqttConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedMqttConnector")
            .finish_non_exhaustive()
    }
}

impl ManagedMqttConnector {
    /// Create a new managed MQTT connector with the given configuration.
    pub fn new(name: &str, config: MqttConfig) -> Self {
        Self {
            connector_name: name.to_string(),
            config,
            source_client: None,
            sink_client: None,
            running: Arc::new(AtomicBool::new(false)),
            subscribed_topics: FxHashSet::default(),
            dedicated_clients: Vec::new(),
            messages_received: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
            last_message_time: Arc::new(Mutex::new(None)),
        }
    }

    /// Ensure the source MQTT client and event loop are running.
    fn ensure_source_connected(
        &mut self,
        tx: mpsc::Sender<Event>,
    ) -> Result<AsyncClient, ConnectorError> {
        if let Some(client) = &self.source_client {
            return Ok(client.clone());
        }

        let client_id = self.config.client_id.clone().unwrap_or_else(|| {
            let worker = std::env::var("VARPULIS_WORKER_ID")
                .or_else(|_| std::env::var("HOSTNAME"))
                .unwrap_or_else(|_| format!("p{}", std::process::id()));
            format!("{}-{}", self.connector_name, worker)
        });

        let mut mqtt_opts = MqttOptions::new(&client_id, &self.config.broker, self.config.port);
        mqtt_opts.set_keep_alive(60);

        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            mqtt_opts.set_credentials(user, pass.expose());
        }

        apply_tls(&mut mqtt_opts, &self.config);

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);

        self.source_client = Some(client.clone());
        self.running.store(true, Ordering::SeqCst);

        let running = self.running.clone();
        let name = self.connector_name.clone();
        let msg_counter = self.messages_received.clone();
        let last_err = self.last_error.clone();
        let last_msg_time = self.last_message_time.clone();

        // Spawn the source event loop task
        tokio::spawn(async move {
            let cb = CircuitBreaker::new(CircuitBreakerConfig {
                failure_threshold: 10,
                reset_timeout: Duration::from_secs(30),
            });

            while running.load(Ordering::SeqCst) {
                if !cb.allow_request() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                        cb.record_success();
                        msg_counter.fetch_add(1, Ordering::Relaxed);
                        *last_msg_time.lock().await = Some(Instant::now());
                        if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                            let topic = std::str::from_utf8(&publish.topic).unwrap_or("");
                            if let Some(event) = parse_mqtt_payload(payload, topic) {
                                if tx.send(event).await.is_err() {
                                    warn!("Managed MQTT {} source channel closed", name);
                                    break;
                                }
                            }
                        }
                    }
                    Ok(_) => {
                        cb.record_success();
                    }
                    Err(e) => {
                        cb.record_failure();
                        *last_err.lock().await = Some(format!("{e:?}"));
                        let failures = cb.consecutive_failures();
                        let backoff_secs = (1u64 << (failures.saturating_sub(1)).min(5)).min(30);
                        warn!(
                            "Managed MQTT {} error (cb_state={}, failures={}): {:?}, retrying in {}s",
                            name, cb.state(), failures, e, backoff_secs
                        );
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    }
                }
            }
            info!("Managed MQTT {} source eventloop stopped", name);
        });

        info!(
            "Managed MQTT {} source connected to {}:{}",
            self.connector_name, self.config.broker, self.config.port
        );

        Ok(client)
    }

    /// Create a dedicated source connection with a specific client ID.
    fn create_dedicated_source(
        &mut self,
        client_id: &str,
        tx: mpsc::Sender<Event>,
    ) -> Result<AsyncClient, ConnectorError> {
        let mut mqtt_opts = MqttOptions::new(client_id, &self.config.broker, self.config.port);
        mqtt_opts.set_keep_alive(60);

        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            mqtt_opts.set_credentials(user, pass.expose());
        }

        apply_tls(&mut mqtt_opts, &self.config);

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);
        self.dedicated_clients.push(client.clone());
        self.running.store(true, Ordering::SeqCst);

        let running = self.running.clone();
        let name = format!("{}/{}", self.connector_name, client_id);

        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                        if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                            let topic = std::str::from_utf8(&publish.topic).unwrap_or("");
                            if let Some(event) = parse_mqtt_payload(payload, topic) {
                                if tx.send(event).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Dedicated MQTT {} error: {:?}", name, e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            info!("Dedicated MQTT {} source eventloop stopped", name);
        });

        info!(
            "Managed MQTT {} dedicated source '{}' connected to {}:{}",
            self.connector_name, client_id, self.config.broker, self.config.port
        );

        Ok(client)
    }

    /// Create a dedicated sink connection with a specific client ID.
    fn create_dedicated_sink(&mut self, client_id: &str) -> Result<AsyncClient, ConnectorError> {
        let sink_id = format!("{client_id}-sink");
        let mut mqtt_opts = MqttOptions::new(&sink_id, &self.config.broker, self.config.port);
        mqtt_opts.set_keep_alive(60);

        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            mqtt_opts.set_credentials(user, pass.expose());
        }

        apply_tls(&mut mqtt_opts, &self.config);

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);
        self.dedicated_clients.push(client.clone());

        let name = format!("{}/{}", self.connector_name, sink_id);
        let running = self.running.clone();

        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                match eventloop.poll().await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Dedicated MQTT {} sink error: {}", name, e);
                        break;
                    }
                }
            }
            info!("Dedicated MQTT {} sink eventloop stopped", name);
        });

        info!(
            "Managed MQTT {} dedicated sink '{}' connected to {}:{}",
            self.connector_name, sink_id, self.config.broker, self.config.port
        );

        Ok(client)
    }

    /// Ensure the sink MQTT client and event loop are running.
    fn ensure_sink_connected(&mut self) -> Result<AsyncClient, ConnectorError> {
        if let Some(client) = &self.sink_client {
            return Ok(client.clone());
        }

        let base_id = self.config.client_id.clone().unwrap_or_else(|| {
            let worker = std::env::var("VARPULIS_WORKER_ID")
                .or_else(|_| std::env::var("HOSTNAME"))
                .unwrap_or_else(|_| format!("p{}", std::process::id()));
            format!("{}-{}", self.connector_name, worker)
        });
        let client_id = format!("{base_id}-sink");

        let mut mqtt_opts = MqttOptions::new(&client_id, &self.config.broker, self.config.port);
        mqtt_opts.set_keep_alive(60);

        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            mqtt_opts.set_credentials(user, pass.expose());
        }

        apply_tls(&mut mqtt_opts, &self.config);

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);

        self.sink_client = Some(client.clone());
        self.running.store(true, Ordering::SeqCst);

        let name = self.connector_name.clone();
        let running = self.running.clone();

        // Spawn the sink event loop task (only drives outgoing publishes)
        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                match eventloop.poll().await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Managed MQTT {} sink eventloop error: {}", name, e);
                        break;
                    }
                }
            }
            info!("Managed MQTT {} sink eventloop stopped", name);
        });

        info!(
            "Managed MQTT {} sink connected to {}:{}",
            self.connector_name, self.config.broker, self.config.port
        );

        Ok(client)
    }
}

#[async_trait]
impl ManagedConnector for ManagedMqttConnector {
    fn name(&self) -> &str {
        &self.connector_name
    }

    fn connector_type(&self) -> &'static str {
        "mqtt"
    }

    fn health(&self) -> ConnectorHealthReport {
        let connected = self.running.load(Ordering::SeqCst);
        let messages_received = self.messages_received.load(Ordering::Relaxed);
        // Use try_lock to avoid blocking -- if lock is held, use defaults
        let last_error = self
            .last_error
            .try_lock()
            .ok()
            .and_then(|guard| guard.clone());
        let seconds_since_last_message = self
            .last_message_time
            .try_lock()
            .ok()
            .and_then(|guard| *guard)
            .map_or(0, |t| t.elapsed().as_secs());
        ConnectorHealthReport {
            connected,
            last_error,
            messages_received,
            seconds_since_last_message,
            ..Default::default()
        }
    }

    #[tracing::instrument(level = "debug", skip(self, tx, params))]
    async fn start_source(
        &mut self,
        topic: &str,
        tx: mpsc::Sender<Event>,
        params: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let qos_override = params
            .get("qos")
            .and_then(|v| v.parse::<u8>().ok())
            .map(qos_from_u8);
        let qos = qos_override.unwrap_or_else(|| qos_from_u8(self.config.qos));

        // If client_id is specified, create a dedicated connection
        if let Some(dedicated_id) = params.get("client_id") {
            let client = self.create_dedicated_source(dedicated_id, tx)?;
            client
                .subscribe(topic, qos)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;
            info!(
                "Managed MQTT {} dedicated client '{}' subscribed to: {}",
                self.connector_name, dedicated_id, topic
            );
            return Ok(());
        }

        let client = self.ensure_source_connected(tx)?;

        if self.subscribed_topics.insert(topic.to_string()) {
            client
                .subscribe(topic, qos)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;
            info!(
                "Managed MQTT {} subscribed to: {}",
                self.connector_name, topic
            );
        }

        Ok(())
    }

    fn create_sink(
        &mut self,
        topic: &str,
        params: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let qos_override = params
            .get("qos")
            .and_then(|v| v.parse::<u8>().ok())
            .map(qos_from_u8);
        let qos = qos_override.unwrap_or_else(|| qos_from_u8(self.config.qos));

        // If client_id is specified, create a dedicated sink connection
        let client = if let Some(dedicated_id) = params.get("client_id") {
            self.create_dedicated_sink(dedicated_id)?
        } else {
            self.ensure_sink_connected()?
        };

        Ok(Arc::new(MqttSharedSink {
            sink_name: format!("{}::{}", self.connector_name, topic),
            topic: topic.to_string(),
            client,
            qos,
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn shutdown(&mut self) -> Result<(), ConnectorError> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(client) = &self.source_client {
            let _ = client.disconnect().await;
        }
        if let Some(client) = &self.sink_client {
            let _ = client.disconnect().await;
        }
        for client in &self.dedicated_clients {
            let _ = client.disconnect().await;
        }
        self.source_client = None;
        self.sink_client = None;
        self.dedicated_clients.clear();
        self.subscribed_topics.clear();
        info!("Managed MQTT {} shut down", self.connector_name);
        Ok(())
    }
}

impl Drop for ManagedMqttConnector {
    fn drop(&mut self) {
        // Signal the event loop to stop when the connector is dropped
        self.running.store(false, Ordering::SeqCst);
    }
}

/// Lightweight sink handle that publishes via a shared `AsyncClient`.
struct MqttSharedSink {
    sink_name: String,
    topic: String,
    client: AsyncClient,
    qos: QoS,
}

#[async_trait]
impl Sink for MqttSharedSink {
    fn name(&self) -> &str {
        &self.sink_name
    }

    async fn send(&self, event: &Event) -> Result<(), SinkError> {
        let buf = event.to_sink_payload();

        self.client
            .try_publish(&self.topic, self.qos, false, buf)
            .map_err(|e| SinkError::other(format!("mqtt publish: {e}")))?;

        Ok(())
    }

    async fn send_batch(&self, events: &[Arc<Event>]) -> Result<(), SinkError> {
        for event in events {
            let buf = event.to_sink_payload();
            self.client
                .try_publish(&self.topic, self.qos, false, buf)
                .map_err(|e| SinkError::other(format!("mqtt publish: {e}")))?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), SinkError> {
        Ok(())
    }
}

/// Parse JSON payload directly into an Event.
fn parse_mqtt_payload(payload: &str, topic: &str) -> Option<Event> {
    let map: indexmap::IndexMap<Arc<str>, serde_json::Value> =
        serde_json::from_str(payload).ok()?;

    let event_type: Arc<str> = map
        .get("event_type" as &str)
        .or_else(|| map.get("type" as &str))
        .and_then(|v| v.as_str())
        .map(Arc::from)
        .unwrap_or_else(|| {
            Arc::from(
                topic
                    .rsplit('/')
                    .next()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("Unknown"),
            )
        });

    let has_data = map
        .get("data" as &str)
        .and_then(|v| v.as_object())
        .is_some();

    if has_data {
        let data_obj = map.get("data" as &str).unwrap().as_object().unwrap();
        let mut fields: FxIndexMap<FieldKey, varpulis_core::Value> =
            indexmap::IndexMap::with_capacity_and_hasher(data_obj.len(), FxBuildHasher);
        for (k, v) in data_obj {
            fields.insert(Arc::from(k.as_str()), json_value_to_native(v));
        }
        Some(Event::from_fields(event_type, fields))
    } else {
        let capacity = map.len().saturating_sub(1);
        let mut fields: FxIndexMap<FieldKey, varpulis_core::Value> =
            indexmap::IndexMap::with_capacity_and_hasher(capacity, FxBuildHasher);
        for (k, v) in &map {
            let ks: &str = k;
            if ks != "event_type" && ks != "type" {
                fields.insert(k.clone(), json_value_to_native(v));
            }
        }
        Some(Event::from_fields(event_type, fields))
    }
}

#[inline]
fn json_value_to_native(v: &serde_json::Value) -> varpulis_core::Value {
    json_to_value(v).unwrap_or(varpulis_core::Value::Null)
}
