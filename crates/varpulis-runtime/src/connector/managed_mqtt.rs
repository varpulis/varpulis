//! Managed MQTT connector — single connection shared across all sources and sinks

use super::managed::ManagedConnector;
use super::mqtt::MqttConfig;
use super::types::ConnectorError;
use crate::event::Event;
use crate::sink::Sink;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

#[cfg(feature = "mqtt")]
mod mqtt_managed_impl {
    use super::*;
    use crate::event::{FieldKey, FxIndexMap};
    use anyhow::anyhow;
    use rumqttc::{AsyncClient, MqttOptions, QoS};
    use rustc_hash::{FxBuildHasher, FxHashSet};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tracing::{error, info, warn};

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
    }

    impl ManagedMqttConnector {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                connector_name: name.to_string(),
                config,
                source_client: None,
                sink_client: None,
                running: Arc::new(AtomicBool::new(false)),
                subscribed_topics: FxHashSet::default(),
                dedicated_clients: Vec::new(),
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
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

            let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);

            self.source_client = Some(client.clone());
            self.running.store(true, Ordering::SeqCst);

            let running = self.running.clone();
            let name = self.connector_name.clone();

            // Spawn the source event loop task
            tokio::spawn(async move {
                let mut consecutive_errors: u32 = 0;
                const MAX_CONSECUTIVE_ERRORS: u32 = 10;
                const MAX_BACKOFF_SECS: u64 = 30;

                while running.load(Ordering::SeqCst) {
                    match eventloop.poll().await {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                            consecutive_errors = 0;
                            if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                                if let Some(event) = parse_mqtt_payload(payload, &publish.topic) {
                                    if tx.send(event).await.is_err() {
                                        warn!("Managed MQTT {} source channel closed", name);
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            consecutive_errors = 0;
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                error!(
                                    "Managed MQTT {} exceeded max errors ({}), stopping",
                                    name, MAX_CONSECUTIVE_ERRORS
                                );
                                running.store(false, Ordering::SeqCst);
                                break;
                            }
                            let backoff_secs =
                                (1u64 << (consecutive_errors - 1).min(5)).min(MAX_BACKOFF_SECS);
                            warn!(
                                "Managed MQTT {} error ({}/{}): {:?}, retrying in {}s",
                                name, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e, backoff_secs
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
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

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
                                if let Some(event) = parse_mqtt_payload(payload, &publish.topic) {
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
        fn create_dedicated_sink(
            &mut self,
            client_id: &str,
        ) -> Result<AsyncClient, ConnectorError> {
            let sink_id = format!("{}-sink", client_id);
            let mut mqtt_opts = MqttOptions::new(&sink_id, &self.config.broker, self.config.port);
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

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
            let client_id = format!("{}-sink", base_id);

            let mut mqtt_opts = MqttOptions::new(&client_id, &self.config.broker, self.config.port);
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

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

        fn connector_type(&self) -> &str {
            "mqtt"
        }

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

        async fn send(&self, event: &Event) -> anyhow::Result<()> {
            let buf = event.to_sink_payload();

            self.client
                .try_publish(&self.topic, self.qos, false, buf)
                .map_err(|e| anyhow!("mqtt publish: {}", e))?;

            Ok(())
        }

        async fn send_batch(&self, events: &[Arc<Event>]) -> anyhow::Result<()> {
            for event in events {
                let buf = event.to_sink_payload();
                self.client
                    .try_publish(&self.topic, self.qos, false, buf)
                    .map_err(|e| anyhow!("mqtt publish: {}", e))?;
            }
            Ok(())
        }

        async fn flush(&self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Parse JSON payload directly into an Event, avoiding intermediate serde_json::Value.
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

#[cfg(feature = "mqtt")]
pub use mqtt_managed_impl::ManagedMqttConnector;

// Stub when mqtt feature is disabled
#[cfg(not(feature = "mqtt"))]
pub struct ManagedMqttConnector {
    name: String,
}

#[cfg(not(feature = "mqtt"))]
impl ManagedMqttConnector {
    pub fn new(name: &str, _config: MqttConfig) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[cfg(not(feature = "mqtt"))]
#[async_trait]
impl ManagedConnector for ManagedMqttConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> &str {
        "mqtt"
    }

    async fn start_source(
        &mut self,
        _topic: &str,
        _tx: mpsc::Sender<Event>,
        _params: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "MQTT requires 'mqtt' feature".to_string(),
        ))
    }

    fn create_sink(
        &mut self,
        _topic: &str,
        _params: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "MQTT requires 'mqtt' feature".to_string(),
        ))
    }

    async fn shutdown(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
