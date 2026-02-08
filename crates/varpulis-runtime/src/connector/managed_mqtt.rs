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
    use anyhow::anyhow;
    use rumqttc::{AsyncClient, MqttOptions, QoS};
    use rustc_hash::FxHashSet;
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

    /// Managed MQTT connector that owns a single rumqttc connection.
    pub struct ManagedMqttConnector {
        connector_name: String,
        config: MqttConfig,
        client: Option<AsyncClient>,
        running: Arc<AtomicBool>,
        subscribed_topics: FxHashSet<String>,
    }

    impl ManagedMqttConnector {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                connector_name: name.to_string(),
                config,
                client: None,
                running: Arc::new(AtomicBool::new(false)),
                subscribed_topics: FxHashSet::default(),
            }
        }

        /// Ensure the MQTT client and event loop are running.
        /// Returns the shared `AsyncClient`.
        fn ensure_connected(
            &mut self,
            tx: Option<mpsc::Sender<Event>>,
        ) -> Result<AsyncClient, ConnectorError> {
            if let Some(client) = &self.client {
                return Ok(client.clone());
            }

            let client_id = self
                .config
                .client_id
                .clone()
                .unwrap_or_else(|| format!("varpulis-{}", self.connector_name));

            let mut mqtt_opts = MqttOptions::new(&client_id, &self.config.broker, self.config.port);
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

            let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);

            self.client = Some(client.clone());
            self.running.store(true, Ordering::SeqCst);

            let running = self.running.clone();
            let name = self.connector_name.clone();

            // Spawn the event loop task
            tokio::spawn(async move {
                let mut consecutive_errors: u32 = 0;
                const MAX_CONSECUTIVE_ERRORS: u32 = 10;
                const MAX_BACKOFF_SECS: u64 = 30;

                while running.load(Ordering::SeqCst) {
                    match eventloop.poll().await {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                            consecutive_errors = 0;
                            if let Some(tx) = &tx {
                                if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                                    if let Ok(json) =
                                        serde_json::from_str::<serde_json::Value>(payload)
                                    {
                                        let topic = &publish.topic;
                                        let event = json_to_event_with_topic(&json, topic);
                                        if tx.send(event).await.is_err() {
                                            warn!("Managed MQTT {} channel closed", name);
                                            break;
                                        }
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
                info!("Managed MQTT {} eventloop stopped", name);
            });

            info!(
                "Managed MQTT {} connected to {}:{}",
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
        ) -> Result<(), ConnectorError> {
            let client = self.ensure_connected(Some(tx))?;

            if self.subscribed_topics.insert(topic.to_string()) {
                client
                    .subscribe(topic, qos_from_u8(self.config.qos))
                    .await
                    .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;
                info!(
                    "Managed MQTT {} subscribed to: {}",
                    self.connector_name, topic
                );
            }

            Ok(())
        }

        fn create_sink(&mut self, topic: &str) -> Result<Arc<dyn Sink>, ConnectorError> {
            let client = self.ensure_connected(None)?;
            let qos = qos_from_u8(self.config.qos);
            Ok(Arc::new(MqttSharedSink {
                sink_name: format!("{}::{}", self.connector_name, topic),
                topic: topic.to_string(),
                client,
                qos,
            }))
        }

        async fn shutdown(&mut self) -> Result<(), ConnectorError> {
            self.running.store(false, Ordering::SeqCst);
            if let Some(client) = &self.client {
                let _ = client.disconnect().await;
            }
            self.client = None;
            self.subscribed_topics.clear();
            info!("Managed MQTT {} shut down", self.connector_name);
            Ok(())
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
            let payload = serde_json::to_vec(event).map_err(|e| anyhow!("serialize: {}", e))?;

            self.client
                .publish(&self.topic, self.qos, false, payload)
                .await
                .map_err(|e| anyhow!("mqtt publish: {}", e))?;

            Ok(())
        }

        async fn flush(&self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Convert JSON to Event, using the MQTT topic to determine event type
    fn json_to_event_with_topic(json: &serde_json::Value, topic: &str) -> Event {
        let event_type = json
            .get("event_type")
            .or_else(|| json.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                topic
                    .rsplit('/')
                    .next()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("Unknown")
                    .to_string()
            });

        let mut event = Event::new(event_type);

        if let Some(data) = json.get("data").and_then(|v| v.as_object()) {
            for (k, v) in data {
                event = event.with_field(k.as_str(), json_value_to_native(v));
            }
        } else if let Some(obj) = json.as_object() {
            for (k, v) in obj {
                if k != "event_type" && k != "type" {
                    event = event.with_field(k.as_str(), json_value_to_native(v));
                }
            }
        }

        event
    }

    fn json_value_to_native(v: &serde_json::Value) -> impl Into<varpulis_core::Value> {
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
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "MQTT requires 'mqtt' feature".to_string(),
        ))
    }

    fn create_sink(&mut self, _topic: &str) -> Result<Arc<dyn Sink>, ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "MQTT requires 'mqtt' feature".to_string(),
        ))
    }

    async fn shutdown(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
