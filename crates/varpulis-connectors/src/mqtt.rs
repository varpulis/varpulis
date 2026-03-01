//! MQTT source and sink connectors using rumqttc.
//!
//! When the `mqtt` feature is enabled, this module provides full MQTT
//! connectivity via `rumqttc`.  When the feature is disabled, stub
//! implementations return `ConnectorError::NotAvailable`.

use super::component::{ConfigParamInfo, ConnectorComponentInfo, ConnectorFactory};
use super::types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};
use async_trait::async_trait;
use tokio::sync::mpsc;
#[cfg(feature = "mqtt")]
use tracing::{info, warn};
use varpulis_core::security::SecretString;
use varpulis_core::Event;

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

static MQTT_PARAMS: &[ConfigParamInfo] = &[
    ConfigParamInfo {
        name: "host",
        description: "MQTT broker hostname",
        required: true,
        default_value: None,
    },
    ConfigParamInfo {
        name: "port",
        description: "MQTT broker port",
        required: false,
        default_value: Some("1883"),
    },
    ConfigParamInfo {
        name: "topic",
        description: "MQTT topic to subscribe/publish",
        required: true,
        default_value: None,
    },
    ConfigParamInfo {
        name: "client_id",
        description: "MQTT client identifier",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "qos",
        description: "Quality of service (0, 1, 2)",
        required: false,
        default_value: Some("0"),
    },
];

static MQTT_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "mqtt",
    display_name: "MQTT",
    description: "MQTT broker connectivity for IoT and messaging",
    feature_flag: "mqtt",
    supports_source: true,
    supports_sink: true,
    supports_managed: true,
    config_params: MQTT_PARAMS,
};

struct MqttFactory;

impl ConnectorFactory for MqttFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &MQTT_INFO
    }

    fn create_managed(
        &self,
        name: &str,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn super::managed::ManagedConnector>, ConnectorError> {
        let mut mqtt_config = MqttConfig::new(&config.url, config.topic.as_deref().unwrap_or("#"));
        if let Some(port) = config.properties.get("port") {
            if let Ok(p) = port.parse::<u16>() {
                mqtt_config = mqtt_config.with_port(p);
            }
        }
        if let Some(client_id) = config.properties.get("client_id") {
            mqtt_config = mqtt_config.with_client_id(client_id);
        }
        if let Some(qos) = config.properties.get("qos") {
            if let Ok(q) = qos.parse::<u8>() {
                mqtt_config = mqtt_config.with_qos(q);
            }
        }
        Ok(Box::new(super::managed_mqtt::ManagedMqttConnector::new(
            name,
            mqtt_config,
        )))
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
        Ok(Box::new(MqttSink::new(
            "mqtt",
            MqttConfig::new(&config.url, &topic),
        )))
    }

    #[cfg(feature = "mqtt")]
    fn create_engine_sink(
        &self,
        name: &str,
        config: &ConnectorConfig,
        topic_override: Option<&str>,
        context_name: Option<&str>,
    ) -> Result<std::sync::Arc<dyn crate::sink::Sink>, ConnectorError> {
        let broker = config.url.clone();
        let port: u16 = config
            .properties
            .get("port")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1883);
        let topic = topic_override
            .map(|s| s.to_string())
            .or_else(|| config.topic.clone())
            .unwrap_or_else(|| format!("{name}-output"));
        let base_id = config
            .properties
            .get("client_id")
            .cloned()
            .unwrap_or_else(|| name.to_string());
        let client_id = match context_name {
            Some(ctx) => format!("{base_id}-{ctx}"),
            None => base_id,
        };
        let mqtt_config = MqttConfig::new(&broker, &topic)
            .with_port(port)
            .with_client_id(&client_id);
        let sink = MqttSink::new(name, mqtt_config);
        Ok(std::sync::Arc::new(crate::sink::SinkConnectorAdapter::new(
            name,
            Box::new(sink),
        )))
    }
}

inventory::submit! { &MqttFactory as &dyn ConnectorFactory }

// =============================================================================
// MQTT Configuration (always available, not feature-gated)
// =============================================================================

/// MQTT configuration
#[derive(Debug, Clone)]
pub struct MqttConfig {
    /// MQTT broker hostname or IP address.
    pub broker: String,
    /// MQTT broker port (default: 1883).
    pub port: u16,
    /// MQTT topic to subscribe to or publish on.
    pub topic: String,
    /// MQTT client identifier (optional, auto-generated if not set).
    pub client_id: Option<String>,
    /// Username for authentication (optional).
    pub username: Option<String>,
    /// Password for authentication (zeroized on drop).
    pub password: Option<SecretString>,
    /// Quality of Service level (0, 1, or 2).
    pub qos: u8,
}

impl MqttConfig {
    /// Create a new MQTT configuration with the given broker and topic.
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

    /// Set the broker port.
    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the MQTT client identifier.
    pub fn with_client_id(mut self, client_id: &str) -> Self {
        self.client_id = Some(client_id.to_string());
        self
    }

    /// Set username and password for authentication.
    pub fn with_credentials(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(SecretString::new(password));
        self
    }

    /// Set the QoS level (clamped to 0-2).
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
    use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS};
    use rustc_hash::FxBuildHasher;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use varpulis_core::event::{FieldKey, FxIndexMap};

    const fn qos_from_u8(qos: u8) -> QoS {
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
        /// Create a new MQTT source connector with the given configuration.
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
        /// connector — call `start` once, then `subscribe` for each
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
                mqtt_opts.set_credentials(user, pass.expose());
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
                use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
                let cb = CircuitBreaker::new(CircuitBreakerConfig {
                    failure_threshold: 10,
                    reset_timeout: Duration::from_secs(30),
                });

                while running.load(Ordering::SeqCst) {
                    if !cb.allow_request() {
                        // Circuit is open — wait before probing
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    match eventloop.poll().await {
                        Ok(MqttEvent::Incoming(Packet::Publish(publish))) => {
                            cb.record_success();
                            // Enforce payload size limit
                            if publish.payload.len() > crate::limits::MAX_EVENT_PAYLOAD_BYTES {
                                warn!(
                                    "MQTT source {}: payload too large ({} bytes, max {}), skipped",
                                    name,
                                    publish.payload.len(),
                                    crate::limits::MAX_EVENT_PAYLOAD_BYTES
                                );
                            } else if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                                if let Some(event) = parse_mqtt_payload(payload, &publish.topic) {
                                    if tx.send(event).await.is_err() {
                                        warn!("MQTT source {} channel closed", name);
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
                            let failures = cb.consecutive_failures();

                            // Exponential backoff: 1s, 2s, 4s, 8s, ... up to 30s
                            let backoff_secs =
                                (1u64 << (failures.saturating_sub(1)).min(5)).min(30);

                            warn!(
                                "MQTT source {} error (cb_state={}, failures={}): {:?}, retrying in {}s",
                                name, cb.state(), failures, e, backoff_secs
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
        /// Create a new MQTT sink connector with the given configuration.
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

        /// Establish an MQTT connection (no-op if already connected via shared client).
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
                mqtt_opts.set_credentials(user, pass.expose());
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
            Self::connect(self).await
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let client = self.client.as_ref().ok_or(ConnectorError::NotConnected)?;

            let buf = event.to_sink_payload();

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
    /// if not found in the payload. Enforces field count limits.
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

        let max_fields = crate::limits::MAX_FIELDS_PER_EVENT;

        if let Some(data_obj) = map.get("data" as &str).and_then(|v| v.as_object()) {
            let cap = data_obj.len().min(max_fields);
            let mut fields: FxIndexMap<FieldKey, varpulis_core::Value> =
                indexmap::IndexMap::with_capacity_and_hasher(cap, FxBuildHasher);
            for (k, v) in data_obj {
                if fields.len() >= max_fields {
                    break;
                }
                fields.insert(
                    Arc::from(k.as_str()),
                    json_value_to_native(v, crate::limits::MAX_JSON_DEPTH),
                );
            }
            Some(Event::from_fields(event_type, fields))
        } else {
            // Build fields from top-level, excluding type keys
            let capacity = map.len().saturating_sub(1).min(max_fields);
            let mut fields: FxIndexMap<FieldKey, varpulis_core::Value> =
                indexmap::IndexMap::with_capacity_and_hasher(capacity, FxBuildHasher);
            for (k, v) in &map {
                let ks: &str = k;
                if ks != "event_type" && ks != "type" {
                    if fields.len() >= max_fields {
                        break;
                    }
                    fields.insert(
                        k.clone(),
                        json_value_to_native(v, crate::limits::MAX_JSON_DEPTH),
                    );
                }
            }
            Some(Event::from_fields(event_type, fields))
        }
    }

    #[inline]
    fn json_value_to_native(v: &serde_json::Value, depth: usize) -> varpulis_core::Value {
        if depth == 0 {
            return varpulis_core::Value::Null;
        }
        match v {
            serde_json::Value::Bool(b) => varpulis_core::Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    varpulis_core::Value::Int(i)
                } else {
                    varpulis_core::Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => {
                if s.len() > crate::limits::MAX_STRING_VALUE_BYTES {
                    let truncated =
                        &s[..s.floor_char_boundary(crate::limits::MAX_STRING_VALUE_BYTES)];
                    varpulis_core::Value::Str(truncated.into())
                } else {
                    varpulis_core::Value::Str(s.clone().into())
                }
            }
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

    /// MQTT source stub (requires `mqtt` feature for full functionality).
    pub struct MqttSource {
        name: String,
        #[allow(dead_code)]
        config: MqttConfig,
        running: bool,
    }

    impl MqttSource {
        /// Create a new MQTT source stub.
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

    /// MQTT sink stub (requires `mqtt` feature for full functionality).
    pub struct MqttSink {
        name: String,
        #[allow(dead_code)]
        config: MqttConfig,
    }

    impl MqttSink {
        /// Create a new MQTT sink stub.
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
