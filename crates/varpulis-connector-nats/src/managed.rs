//! Managed NATS connector -- single connection shared across all sources and sinks

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use rustc_hash::FxBuildHasher;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use varpulis_connector_api::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use varpulis_connector_api::helpers::json_to_value;
use varpulis_connector_api::sink::{Sink, SinkError};
use varpulis_connector_api::{ConnectorError, ConnectorHealthReport, ManagedConnector};
use varpulis_core::event::{FieldKey, FxIndexMap};
use varpulis_core::Event;

use crate::NatsConfig;

/// Managed NATS connector with a single shared client.
pub struct ManagedNatsConnector {
    connector_name: String,
    config: NatsConfig,
    client: Option<async_nats::Client>,
    running: Arc<AtomicBool>,
    messages_received: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    last_message_time: Arc<Mutex<Option<Instant>>>,
}

impl std::fmt::Debug for ManagedNatsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedNatsConnector")
            .field("connector_name", &self.connector_name)
            .field("running", &self.running)
            .finish_non_exhaustive()
    }
}

impl ManagedNatsConnector {
    /// Creates a new managed NATS connector.
    pub fn new(name: &str, config: NatsConfig) -> Self {
        Self {
            connector_name: name.to_string(),
            config,
            client: None,
            running: Arc::new(AtomicBool::new(false)),
            messages_received: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
            last_message_time: Arc::new(Mutex::new(None)),
        }
    }

    /// Ensure the NATS client is connected (sync check).
    fn ensure_connected(&mut self) -> Result<async_nats::Client, ConnectorError> {
        if let Some(client) = &self.client {
            return Ok(client.clone());
        }
        Err(ConnectorError::NotConnected)
    }

    /// Actually connect (async).
    #[tracing::instrument(level = "debug", skip(self))]
    async fn connect_async(&mut self) -> Result<async_nats::Client, ConnectorError> {
        if let Some(client) = &self.client {
            return Ok(client.clone());
        }

        let opts = crate::build_connect_options(&self.config);

        let client = async_nats::connect_with_options(&self.config.servers, opts)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        self.client = Some(client.clone());
        self.running.store(true, Ordering::SeqCst);

        info!(
            "Managed NATS {} connected to {}",
            self.connector_name, self.config.servers
        );

        Ok(client)
    }
}

#[async_trait]
impl ManagedConnector for ManagedNatsConnector {
    fn name(&self) -> &str {
        &self.connector_name
    }

    fn connector_type(&self) -> &str {
        "nats"
    }

    fn health(&self) -> ConnectorHealthReport {
        let connected = self.running.load(Ordering::SeqCst);
        let messages_received = self.messages_received.load(Ordering::Relaxed);
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
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0);
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
        let client = self.connect_async().await?;

        let queue_group = params
            .get("queue_group")
            .cloned()
            .or_else(|| self.config.queue_group.clone());

        let subscriber = if let Some(group) = &queue_group {
            client
                .queue_subscribe(topic.to_string(), group.clone())
                .await
        } else {
            client.subscribe(topic.to_string()).await
        }
        .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let running = self.running.clone();
        let name = self.connector_name.clone();
        let msg_counter = self.messages_received.clone();
        let last_err = self.last_error.clone();
        let last_msg_time = self.last_message_time.clone();
        let topic_str = topic.to_string();

        tokio::spawn(async move {
            let mut subscriber = subscriber;
            let cb = CircuitBreaker::new(CircuitBreakerConfig {
                failure_threshold: 10,
                reset_timeout: Duration::from_secs(30),
            });

            while running.load(Ordering::SeqCst) {
                if !cb.allow_request() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                match tokio::time::timeout(Duration::from_secs(30), subscriber.next()).await {
                    Ok(Some(message)) => {
                        cb.record_success();
                        msg_counter.fetch_add(1, Ordering::Relaxed);
                        *last_msg_time.lock().await = Some(Instant::now());
                        if let Ok(payload) = std::str::from_utf8(&message.payload) {
                            let subject = message.subject.as_str();
                            if let Some(event) = parse_nats_payload(payload, subject) {
                                if tx.send(event).await.is_err() {
                                    warn!("Managed NATS {} source channel closed", name);
                                    break;
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        cb.record_failure();
                        *last_err.lock().await = Some("subscription ended".to_string());
                        warn!(
                            "Managed NATS {} subscription ended (cb_state={}), backing off",
                            name,
                            cb.state()
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
            info!(
                "Managed NATS {} source loop stopped (subject: {})",
                name, topic_str
            );
        });

        info!(
            "Managed NATS {} subscribed to: {}",
            self.connector_name, topic
        );

        Ok(())
    }

    fn create_sink(
        &mut self,
        topic: &str,
        _params: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let client = self.ensure_connected()?;

        Ok(Arc::new(NatsSharedSink {
            sink_name: format!("{}::{}", self.connector_name, topic),
            subject: topic.to_string(),
            client,
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn shutdown(&mut self) -> Result<(), ConnectorError> {
        self.running.store(false, Ordering::SeqCst);
        self.client = None;
        info!("Managed NATS {} shut down", self.connector_name);
        Ok(())
    }
}

impl Drop for ManagedNatsConnector {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

/// Lightweight sink handle that publishes via a shared async-nats Client.
struct NatsSharedSink {
    sink_name: String,
    subject: String,
    client: async_nats::Client,
}

#[async_trait]
impl Sink for NatsSharedSink {
    fn name(&self) -> &str {
        &self.sink_name
    }

    async fn send(&self, event: &Event) -> Result<(), SinkError> {
        let buf = event.to_sink_payload();

        self.client
            .publish(self.subject.clone(), buf.into())
            .await
            .map_err(|e| SinkError::other(format!("nats publish: {}", e)))?;

        Ok(())
    }

    async fn send_batch(&self, events: &[Arc<Event>]) -> Result<(), SinkError> {
        for event in events {
            let buf = event.to_sink_payload();
            self.client
                .publish(self.subject.clone(), buf.into())
                .await
                .map_err(|e| SinkError::other(format!("nats publish: {}", e)))?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        self.client
            .flush()
            .await
            .map_err(|e| SinkError::other(format!("nats flush: {}", e)))?;
        Ok(())
    }

    async fn close(&self) -> Result<(), SinkError> {
        Ok(())
    }
}

/// Parse JSON payload directly into an Event.
fn parse_nats_payload(payload: &str, subject: &str) -> Option<Event> {
    let map: indexmap::IndexMap<Arc<str>, serde_json::Value> =
        serde_json::from_str(payload).ok()?;

    let event_type: Arc<str> = map
        .get("event_type" as &str)
        .or_else(|| map.get("type" as &str))
        .and_then(|v| v.as_str())
        .map(Arc::from)
        .unwrap_or_else(|| {
            Arc::from(
                subject
                    .rsplit('.')
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
