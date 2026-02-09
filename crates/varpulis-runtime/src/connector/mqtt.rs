//! MQTT source and sink connectors using rumqttc.
//!
//! When the `mqtt` feature is enabled, this module provides full MQTT
//! connectivity via `rumqttc`.  When the feature is disabled, stub
//! implementations return `ConnectorError::NotAvailable`.

use super::types::{ConnectorError, SinkConnector, SourceConnector};
use crate::event::Event;
use async_trait::async_trait;
use tokio::sync::mpsc;
#[cfg(feature = "mqtt")]
use tracing::{error, info, warn};

// =============================================================================
// MQTT Configuration (always available, not feature-gated)
// =============================================================================

/// MQTT configuration
#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub broker: String,
    pub port: u16,
    pub topic: String,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub qos: u8,
}

impl MqttConfig {
    pub fn new(broker: &str, topic: &str) -> Self {
        Self {
            broker: broker.to_string(),
            port: 1883,
            topic: topic.to_string(),
            client_id: None,
            username: None,
            password: None,
            qos: 0,
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_client_id(mut self, client_id: &str) -> Self {
        self.client_id = Some(client_id.to_string());
        self
    }

    pub fn with_credentials(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(password.to_string());
        self
    }

    pub fn with_qos(mut self, qos: u8) -> Self {
        self.qos = qos.min(2);
        self
    }
}

// -----------------------------------------------------------------------------
// MQTT with rumqttc feature enabled
// -----------------------------------------------------------------------------
#[cfg(feature = "mqtt")]
mod mqtt_impl {
    use super::*;
    use crate::event::{FieldKey, FxIndexMap};
    use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS};
    use rustc_hash::FxBuildHasher;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn qos_from_u8(qos: u8) -> QoS {
        match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        }
    }

    /// MQTT source connector with rumqttc
    pub struct MqttSource {
        name: String,
        config: MqttConfig,
        running: Arc<AtomicBool>,
        client: Option<AsyncClient>,
    }

    impl MqttSource {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: Arc::new(AtomicBool::new(false)),
                client: None,
            }
        }

        /// Get a clone of the underlying MQTT client (for sharing with a sink).
        pub fn client(&self) -> Option<AsyncClient> {
            self.client.clone()
        }

        /// Subscribe to an additional topic on the existing connection.
        ///
        /// Use this when multiple `.from()` bindings reference the same
        /// connector — call [`start`] once, then `subscribe` for each
        /// extra topic.
        pub async fn subscribe(&self, topic: &str) -> Result<(), ConnectorError> {
            let client = self.client.as_ref().ok_or(ConnectorError::NotConnected)?;
            client
                .subscribe(topic, qos_from_u8(self.config.qos))
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;
            info!("MQTT source {} subscribed to: {}", self.name, topic);
            Ok(())
        }
    }

    #[async_trait]
    impl SourceConnector for MqttSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            let client_id = self
                .config
                .client_id
                .clone()
                .unwrap_or_else(|| format!("varpulis-src-{}", std::process::id()));

            let mut mqtt_opts = MqttOptions::new(client_id, &self.config.broker, self.config.port);
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

            let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);

            client
                .subscribe(&self.config.topic, qos_from_u8(self.config.qos))
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.client = Some(client);
            self.running.store(true, Ordering::SeqCst);

            info!(
                "MQTT source {} connected to {}:{}",
                self.name, self.config.broker, self.config.port
            );
            info!("  Subscribed to: {}", self.config.topic);

            let running = self.running.clone();
            let name = self.name.clone();

            tokio::spawn(async move {
                let mut consecutive_errors: u32 = 0;
                const MAX_CONSECUTIVE_ERRORS: u32 = 10;
                const MAX_BACKOFF_SECS: u64 = 30;

                while running.load(Ordering::SeqCst) {
                    match eventloop.poll().await {
                        Ok(MqttEvent::Incoming(Packet::Publish(publish))) => {
                            // Reset error counter on successful message
                            consecutive_errors = 0;
                            if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                                if let Some(event) = parse_mqtt_payload(payload, &publish.topic) {
                                    if tx.send(event).await.is_err() {
                                        warn!("MQTT source {} channel closed", name);
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            // Other successful events (ConnAck, SubAck, etc.) reset error counter
                            consecutive_errors = 0;
                        }
                        Err(e) => {
                            consecutive_errors += 1;

                            // Check if we've exceeded the error limit
                            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                error!(
                                    "MQTT source {} exceeded max consecutive errors ({}), stopping",
                                    name, MAX_CONSECUTIVE_ERRORS
                                );
                                running.store(false, Ordering::SeqCst);
                                break;
                            }

                            // Exponential backoff: 1s, 2s, 4s, 8s, ... up to MAX_BACKOFF_SECS
                            let backoff_secs =
                                (1u64 << (consecutive_errors - 1).min(5)).min(MAX_BACKOFF_SECS);

                            warn!(
                                "MQTT source {} error (attempt {}/{}): {:?}, retrying in {}s",
                                name, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e, backoff_secs
                            );

                            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        }
                    }
                }
                info!("MQTT source {} eventloop stopped", name);
            });

            Ok(())
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running.store(false, Ordering::SeqCst);
            if let Some(client) = &self.client {
                let _ = client.disconnect().await;
            }
            info!("MQTT source {} stopped", self.name);
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    /// MQTT sink connector with rumqttc
    pub struct MqttSink {
        name: String,
        config: MqttConfig,
        client: Option<AsyncClient>,
    }

    impl MqttSink {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                client: None,
            }
        }

        /// Create a sink that shares an existing MQTT client (from a source).
        /// The shared client publishes through the source's eventloop, so no
        /// separate connection or eventloop is needed.
        pub fn with_client(name: &str, config: MqttConfig, client: AsyncClient) -> Self {
            Self {
                name: name.to_string(),
                config,
                client: Some(client),
            }
        }

        pub async fn connect(&mut self) -> Result<(), ConnectorError> {
            // Already connected via shared client — nothing to do.
            if self.client.is_some() {
                return Ok(());
            }

            let client_id = self
                .config
                .client_id
                .clone()
                .unwrap_or_else(|| format!("varpulis-sink-{}", std::process::id()));

            let mut mqtt_opts = MqttOptions::new(client_id, &self.config.broker, self.config.port);
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

            let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);
            self.client = Some(client);

            // Spawn eventloop handler
            let sink_name = self.name.clone();
            tokio::spawn(async move {
                loop {
                    match eventloop.poll().await {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("MQTT sink {} eventloop error: {}", sink_name, e);
                            break;
                        }
                    }
                }
            });

            info!(
                "MQTT sink {} connected to {}:{}",
                self.name, self.config.broker, self.config.port
            );
            Ok(())
        }
    }

    #[async_trait]
    impl SinkConnector for MqttSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn connect(&mut self) -> Result<(), ConnectorError> {
            // Delegate to the inherent connect method
            MqttSink::connect(self).await
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let client = self.client.as_ref().ok_or(ConnectorError::NotConnected)?;

            // Serialize directly into a pre-sized buffer to avoid reallocation
            let mut buf = Vec::with_capacity(256);
            serde_json::to_writer(&mut buf, event)
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            // With QoS 0, try_publish avoids the async overhead of .await
            // (just pushes to rumqttc's internal channel synchronously)
            client
                .try_publish(&self.config.topic, qos_from_u8(self.config.qos), false, buf)
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            if let Some(client) = &self.client {
                let _ = client.disconnect().await;
            }
            Ok(())
        }
    }

    /// Parse JSON payload directly into an Event, avoiding intermediate serde_json::Value.
    ///
    /// Uses serde_json::from_str into a HashMap first, then builds the Event
    /// with pre-allocated capacity. Falls back to topic-based event type
    /// if not found in the payload.
    fn parse_mqtt_payload(payload: &str, topic: &str) -> Option<Event> {
        // Parse into ordered map directly — avoids the generic Value tree
        let map: indexmap::IndexMap<Arc<str>, serde_json::Value> =
            serde_json::from_str(payload).ok()?;

        // Extract event type: check "event_type" then "type" then topic
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

        // Check for nested "data" object
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
            // Build fields from top-level, excluding type keys
            let capacity = map.len().saturating_sub(1); // minus event_type
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

// -----------------------------------------------------------------------------
// MQTT stub when feature disabled
// -----------------------------------------------------------------------------
#[cfg(not(feature = "mqtt"))]
mod mqtt_impl {
    use super::*;

    pub struct MqttSource {
        name: String,
        #[allow(dead_code)]
        config: MqttConfig,
        running: bool,
    }

    impl MqttSource {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: false,
            }
        }
    }

    #[async_trait]
    impl SourceConnector for MqttSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            Err(ConnectorError::NotAvailable(
                "MQTT requires 'mqtt' feature. Build with: cargo build --features mqtt".to_string(),
            ))
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running = false;
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running
        }
    }

    pub struct MqttSink {
        name: String,
        #[allow(dead_code)]
        config: MqttConfig,
    }

    impl MqttSink {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
            }
        }
    }

    #[async_trait]
    impl SinkConnector for MqttSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
            Err(ConnectorError::NotAvailable(
                "MQTT requires 'mqtt' feature".to_string(),
            ))
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }
}

pub use mqtt_impl::{MqttSink, MqttSource};
