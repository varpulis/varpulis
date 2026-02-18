//! Managed NATS connector — single connection shared across all sources and sinks

#[cfg(feature = "nats")]
use super::managed::ConnectorHealthReport;
use super::managed::ManagedConnector;
use super::nats::NatsConfig;
use super::types::ConnectorError;
use crate::event::Event;
use crate::sink::Sink;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

#[cfg(feature = "nats")]
mod nats_managed_impl {
    use super::*;
    use crate::event::{FieldKey, FxIndexMap};
    use anyhow::anyhow;
    use futures::StreamExt;
    use rustc_hash::FxBuildHasher;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};
    use tokio::sync::Mutex;
    use tracing::{info, warn};

    /// Managed NATS connector with a single shared client.
    ///
    /// Unlike MQTT, async-nats multiplexes subscriptions and publishes over a
    /// single connection, so one `Client` handles both source and sink.
    pub struct ManagedNatsConnector {
        connector_name: String,
        config: NatsConfig,
        client: Option<async_nats::Client>,
        running: Arc<AtomicBool>,
        /// Health tracking: total messages received
        messages_received: Arc<AtomicU64>,
        /// Health tracking: last error string
        last_error: Arc<Mutex<Option<String>>>,
        /// Health tracking: time of last received message
        last_message_time: Arc<Mutex<Option<Instant>>>,
    }

    impl ManagedNatsConnector {
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

        /// Ensure the NATS client is connected.
        fn ensure_connected(&mut self) -> Result<async_nats::Client, ConnectorError> {
            if let Some(client) = &self.client {
                return Ok(client.clone());
            }

            // async-nats connect is async, but we need it in a sync context.
            // We'll do lazy connect in start_source / create_sink instead.
            Err(ConnectorError::NotConnected)
        }

        /// Actually connect (async).
        async fn connect_async(&mut self) -> Result<async_nats::Client, ConnectorError> {
            if let Some(client) = &self.client {
                return Ok(client.clone());
            }

            let mut opts = async_nats::ConnectOptions::new();
            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                opts = opts.user_and_password(user.clone(), pass.expose().to_string());
            }
            if let Some(token) = &self.config.token {
                opts = opts.token(token.expose().to_string());
            }

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
            }
        }

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

                while running.load(Ordering::SeqCst) {
                    match tokio::time::timeout(Duration::from_secs(30), subscriber.next()).await {
                        Ok(Some(message)) => {
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
                            *last_err.lock().await = Some("subscription ended".to_string());
                            info!("Managed NATS {} subscription ended", name);
                            break;
                        }
                        Err(_) => {
                            // Timeout — loop back to check running flag
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

        async fn shutdown(&mut self) -> Result<(), ConnectorError> {
            self.running.store(false, Ordering::SeqCst);
            // async-nats Client is reference-counted; dropping it closes the connection
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

        async fn send(&self, event: &Event) -> anyhow::Result<()> {
            let buf = event.to_sink_payload();

            self.client
                .publish(self.subject.clone(), buf.into())
                .await
                .map_err(|e| anyhow!("nats publish: {}", e))?;

            Ok(())
        }

        async fn send_batch(&self, events: &[Arc<Event>]) -> anyhow::Result<()> {
            for event in events {
                let buf = event.to_sink_payload();
                self.client
                    .publish(self.subject.clone(), buf.into())
                    .await
                    .map_err(|e| anyhow!("nats publish: {}", e))?;
            }
            Ok(())
        }

        async fn flush(&self) -> anyhow::Result<()> {
            self.client
                .flush()
                .await
                .map_err(|e| anyhow!("nats flush: {}", e))?;
            Ok(())
        }

        async fn close(&self) -> anyhow::Result<()> {
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
        match v {
            serde_json::Value::Bool(b) => varpulis_core::Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    varpulis_core::Value::Int(i)
                } else {
                    varpulis_core::Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => varpulis_core::Value::Str(s.clone().into()),
            _ => varpulis_core::Value::Null,
        }
    }
}

#[cfg(feature = "nats")]
pub use nats_managed_impl::ManagedNatsConnector;

// Stub when nats feature is disabled
#[cfg(not(feature = "nats"))]
pub struct ManagedNatsConnector {
    name: String,
}

#[cfg(not(feature = "nats"))]
impl ManagedNatsConnector {
    pub fn new(name: &str, _config: NatsConfig) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[cfg(not(feature = "nats"))]
#[async_trait]
impl ManagedConnector for ManagedNatsConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> &str {
        "nats"
    }

    async fn start_source(
        &mut self,
        _topic: &str,
        _tx: mpsc::Sender<Event>,
        _params: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "NATS requires 'nats' feature".to_string(),
        ))
    }

    fn create_sink(
        &mut self,
        _topic: &str,
        _params: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "NATS requires 'nats' feature".to_string(),
        ))
    }

    async fn shutdown(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
