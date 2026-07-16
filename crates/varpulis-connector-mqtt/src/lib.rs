//! MQTT source and sink connectors using rumqttc.
//!
//! This crate provides full MQTT connectivity for the Varpulis CEP engine
//! via the `rumqttc` library. It includes source connectors for subscribing
//! to MQTT topics and sink connectors for publishing events.

mod managed;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
pub use managed::ManagedMqttConnector;
use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS};
use rustc_hash::FxBuildHasher;
use tokio::sync::mpsc;
use tracing::{info, warn};
use varpulis_connector_api::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use varpulis_connector_api::helpers::json_to_value;
use varpulis_connector_api::{
    ConfigParamInfo, ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory,
    ManagedConnector, Sink, SinkConnector, SinkConnectorAdapter, SourceConnector,
};
use varpulis_core::event::{FieldKey, FxIndexMap};
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
    // Security
    ConfigParamInfo {
        name: "profile",
        description: "Credentials profile name from external credentials file",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "use_tls",
        description: "Enable TLS (true/false)",
        required: false,
        default_value: Some("false"),
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
    ConfigParamInfo {
        name: "username",
        description: "MQTT username",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "password",
        description: "MQTT password (use credentials file for production)",
        required: false,
        default_value: None,
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
    ) -> Result<Box<dyn ManagedConnector>, ConnectorError> {
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
        // Security properties
        if let Some(username) = config.properties.get("username") {
            if let Some(password) = config.properties.get("password") {
                mqtt_config = mqtt_config.with_credentials(username, password);
            }
        }
        if config
            .properties
            .get("use_tls")
            .is_some_and(|v| v == "true")
        {
            mqtt_config = mqtt_config.with_tls(true);
        }
        if let Some(ca) = config.properties.get("ssl_ca_location") {
            mqtt_config = mqtt_config.with_ca_cert(ca);
        }
        if let (Some(cert), Some(key)) = (
            config.properties.get("ssl_certificate_location"),
            config.properties.get("ssl_key_location"),
        ) {
            mqtt_config = mqtt_config.with_client_cert(cert, key);
        }
        Ok(Box::new(ManagedMqttConnector::new(name, mqtt_config)))
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
        let mut mqtt_config = MqttConfig::new(&config.url, &topic);
        if let Some(port) = config.properties.get("port") {
            if let Ok(p) = port.parse::<u16>() {
                mqtt_config = mqtt_config.with_port(p);
            }
        }
        if let Some(username) = config.properties.get("username") {
            if let Some(password) = config.properties.get("password") {
                mqtt_config = mqtt_config.with_credentials(username, password);
            }
        }
        if config
            .properties
            .get("use_tls")
            .is_some_and(|v| v == "true")
        {
            mqtt_config = mqtt_config.with_tls(true);
        }
        if let Some(ca) = config.properties.get("ssl_ca_location") {
            mqtt_config = mqtt_config.with_ca_cert(ca);
        }
        if let (Some(cert), Some(key)) = (
            config.properties.get("ssl_certificate_location"),
            config.properties.get("ssl_key_location"),
        ) {
            mqtt_config = mqtt_config.with_client_cert(cert, key);
        }
        Ok(Box::new(MqttSink::new("mqtt", mqtt_config)))
    }

    fn create_engine_sink(
        &self,
        name: &str,
        config: &ConnectorConfig,
        topic_override: Option<&str>,
        context_name: Option<&str>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
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
        let mut mqtt_config = MqttConfig::new(&broker, &topic)
            .with_port(port)
            .with_client_id(&client_id);
        // Security
        if let Some(username) = config.properties.get("username") {
            if let Some(password) = config.properties.get("password") {
                mqtt_config = mqtt_config.with_credentials(username, password);
            }
        }
        if config
            .properties
            .get("use_tls")
            .is_some_and(|v| v == "true")
        {
            mqtt_config = mqtt_config.with_tls(true);
        }
        if let Some(ca) = config.properties.get("ssl_ca_location") {
            mqtt_config = mqtt_config.with_ca_cert(ca);
        }
        if let (Some(cert), Some(key)) = (
            config.properties.get("ssl_certificate_location"),
            config.properties.get("ssl_key_location"),
        ) {
            mqtt_config = mqtt_config.with_client_cert(cert, key);
        }
        let sink = MqttSink::new(name, mqtt_config);
        Ok(Arc::new(SinkConnectorAdapter::new(name, Box::new(sink))))
    }
}

inventory::submit! { &MqttFactory as &dyn ConnectorFactory }

// =============================================================================
// MQTT Configuration
// =============================================================================

/// MQTT configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
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
    /// Enable TLS (MQTTS). Default: false.
    pub use_tls: bool,
    /// Path to CA certificate file (PEM format).
    pub ssl_ca_location: Option<String>,
    /// Path to client certificate file (PEM format) for mTLS.
    pub ssl_certificate_location: Option<String>,
    /// Path to client private key file (PEM format) for mTLS.
    pub ssl_key_location: Option<String>,
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
            use_tls: false,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
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

    /// Enable TLS for the MQTT connection.
    pub const fn with_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    /// Set the path to the CA certificate (PEM).
    pub fn with_ca_cert(mut self, path: &str) -> Self {
        self.ssl_ca_location = Some(path.to_string());
        self
    }

    /// Set client certificate and key paths for mTLS.
    pub fn with_client_cert(mut self, cert: &str, key: &str) -> Self {
        self.ssl_certificate_location = Some(cert.to_string());
        self.ssl_key_location = Some(key.to_string());
        self
    }
}

// =============================================================================
// Helper functions
// =============================================================================

const fn qos_from_u8(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

/// Apply TLS configuration to `MqttOptions` if enabled in the config.
pub(crate) fn apply_tls(mqtt_opts: &mut MqttOptions, config: &MqttConfig) {
    if !config.use_tls {
        return;
    }
    // Read CA cert if provided, otherwise use empty (system certs not supported in Simple mode)
    let ca = config
        .ssl_ca_location
        .as_ref()
        .map_or_else(Vec::new, |path| {
            std::fs::read(path).unwrap_or_else(|e| {
                warn!("Failed to read CA cert '{}': {}", path, e);
                Vec::new()
            })
        });

    let client_auth = match (&config.ssl_certificate_location, &config.ssl_key_location) {
        (Some(cert_path), Some(key_path)) => {
            let cert = std::fs::read(cert_path).unwrap_or_else(|e| {
                warn!("Failed to read client cert '{}': {}", cert_path, e);
                Vec::new()
            });
            let key = std::fs::read(key_path).unwrap_or_else(|e| {
                warn!("Failed to read client key '{}': {}", key_path, e);
                Vec::new()
            });
            Some((cert, key))
        }
        _ => None,
    };

    mqtt_opts.set_transport(rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Simple {
        ca,
        alpn: None,
        client_auth,
    }));
}

// =============================================================================
// MQTT Source
// =============================================================================

/// MQTT source connector with rumqttc
pub struct MqttSource {
    name: String,
    config: MqttConfig,
    running: Arc<AtomicBool>,
    client: Option<AsyncClient>,
}

impl std::fmt::Debug for MqttSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttSource").finish_non_exhaustive()
    }
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
        mqtt_opts.set_keep_alive(60);

        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            mqtt_opts.set_credentials(user, pass.expose());
        }

        apply_tls(&mut mqtt_opts, &self.config);

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 10_000);

        client
            .subscribe(&self.config.topic, qos_from_u8(self.config.qos))
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        self.client = Some(client.clone());
        self.running.store(true, Ordering::SeqCst);

        info!(
            "MQTT source {} connected to {}:{}",
            self.name, self.config.broker, self.config.port
        );
        info!("  Subscribed to: {}", self.config.topic);

        let running = self.running.clone();
        let name = self.name.clone();
        // Captured so the event loop can re-issue SUBSCRIBE on every reconnect.
        let resub_topic = self.config.topic.clone();
        let resub_qos = qos_from_u8(self.config.qos);

        tokio::spawn(async move {
            let cb = CircuitBreaker::new(CircuitBreakerConfig {
                failure_threshold: 10,
                reset_timeout: Duration::from_secs(30),
            });

            while running.load(Ordering::SeqCst) {
                if !cb.allow_request() {
                    // Circuit is open -- wait before probing
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                match eventloop.poll().await {
                    Ok(MqttEvent::Incoming(Packet::ConnAck(_))) => {
                        cb.record_success();
                        // rumqttc transparently reconnects after a dropped
                        // connection, but with a clean session the broker forgets
                        // our subscriptions. ConnAck fires on the first connect
                        // and on every reconnect, so re-issue SUBSCRIBE here --
                        // otherwise the source goes silently deaf after any broker
                        // restart or network blip while health() still reports
                        // connected (silent data loss).
                        match client.subscribe(&resub_topic, resub_qos).await {
                            Ok(()) => info!(
                                "MQTT source {} (re)subscribed to {} on connect",
                                name, resub_topic
                            ),
                            Err(e) => warn!(
                                "MQTT source {} failed to re-subscribe to {}: {}",
                                name, resub_topic, e
                            ),
                        }
                    }
                    Ok(MqttEvent::Incoming(Packet::Publish(publish))) => {
                        cb.record_success();
                        // Enforce payload size limit
                        if publish.payload.len()
                            > varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES
                        {
                            warn!(
                                "MQTT source {}: payload too large ({} bytes, max {}), skipped",
                                name,
                                publish.payload.len(),
                                varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES
                            );
                        } else if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                            let topic = std::str::from_utf8(&publish.topic).unwrap_or("");
                            if let Some(event) = parse_mqtt_payload(payload, topic) {
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
                        let backoff_secs = (1u64 << (failures.saturating_sub(1)).min(5)).min(30);

                        warn!(
                            "MQTT source {} error (cb_state={}, failures={}): {:?}, retrying in {}s",
                            name,
                            cb.state(),
                            failures,
                            e,
                            backoff_secs
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

// =============================================================================
// MQTT Sink
// =============================================================================

/// MQTT sink connector with rumqttc
pub struct MqttSink {
    name: String,
    config: MqttConfig,
    client: Option<AsyncClient>,
}

impl std::fmt::Debug for MqttSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttSink").finish_non_exhaustive()
    }
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
    pub fn with_client(name: &str, config: MqttConfig, client: AsyncClient) -> Self {
        Self {
            name: name.to_string(),
            config,
            client: Some(client),
        }
    }

    /// Establish an MQTT connection (no-op if already connected via shared client).
    pub async fn connect(&mut self) -> Result<(), ConnectorError> {
        // Already connected via shared client -- nothing to do.
        if self.client.is_some() {
            return Ok(());
        }

        let client_id = self
            .config
            .client_id
            .clone()
            .unwrap_or_else(|| format!("varpulis-sink-{}", std::process::id()));

        let mut mqtt_opts = MqttOptions::new(client_id, &self.config.broker, self.config.port);
        mqtt_opts.set_keep_alive(60);

        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            mqtt_opts.set_credentials(user, pass.expose());
        }

        apply_tls(&mut mqtt_opts, &self.config);

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
        client
            .try_publish(&self.config.topic, qos_from_u8(self.config.qos), false, buf)
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn send_to_topic(
        &self,
        events: &[std::sync::Arc<Event>],
        topic: &str,
    ) -> Result<(), ConnectorError> {
        let client = self.client.as_ref().ok_or(ConnectorError::NotConnected)?;
        for event in events {
            let buf = event.to_sink_payload();
            client
                .try_publish(topic, qos_from_u8(self.config.qos), false, buf)
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;
        }
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

// =============================================================================
// Payload parsing
// =============================================================================

/// Parse JSON payload directly into an Event, avoiding intermediate `serde_json::Value`.
fn parse_mqtt_payload(payload: &str, topic: &str) -> Option<Event> {
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

    let max_fields = varpulis_connector_api::limits::MAX_FIELDS_PER_EVENT;

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
                json_value_to_native(v, varpulis_connector_api::limits::MAX_JSON_DEPTH),
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
                    json_value_to_native(v, varpulis_connector_api::limits::MAX_JSON_DEPTH),
                );
            }
        }
        Some(Event::from_fields(event_type, fields))
    }
}

#[inline]
fn json_value_to_native(v: &serde_json::Value, _depth: usize) -> varpulis_core::Value {
    json_to_value(v).unwrap_or(varpulis_core::Value::Null)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that creating an MQTT source with TLS enabled does not panic
    /// due to a missing rustls crypto provider.
    #[tokio::test]
    async fn mqtt_tls_crypto_provider_installed() {
        // Install the crypto provider (mirrors what main.rs does)
        let _ = rustls::crypto::ring::default_provider().install_default();

        let config = MqttConfig::new("localhost", "test/topic")
            .with_port(8883)
            .with_tls(true);

        let mut source = MqttSource::new("tls-test", config);

        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let result = source.start(tx).await;

        // Connection refused is expected; a crypto provider panic is not.
        drop(result);
        let _ = source.stop().await;
    }

    /// Verify that creating an MQTT sink with TLS does not panic on connect.
    #[tokio::test]
    async fn mqtt_sink_tls_no_panic() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let config = MqttConfig::new("localhost", "test/output")
            .with_port(8883)
            .with_tls(true);

        let mut sink = MqttSink::new("tls-sink-test", config);

        let result = sink.connect().await;
        drop(result);
        let _ = sink.close().await;
    }

    // =========================================================================
    // Real-broker reconnect gate
    //
    // Requires a mosquitto broker on 127.0.0.1:1883 (anonymous) plus a docker
    // container named `varpulis-mosq` to restart. If the broker is unreachable
    // the test skips gracefully (mirrors the cluster chaos tests) so CI without
    // a broker stays green. Deterministic: it polls for delivery against bounded
    // deadlines rather than sleeping-then-asserting.
    // =========================================================================

    const BROKER_ADDR: &str = "127.0.0.1:1883";

    /// True if a TCP connection to the broker succeeds within 500ms.
    fn broker_reachable() -> bool {
        use std::net::{TcpStream, ToSocketAddrs};
        BROKER_ADDR
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .and_then(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(500)).ok())
            .is_some()
    }

    /// Spawn an independent publisher whose event loop is driven in the
    /// background, so it reconnects on its own after the broker restarts.
    fn spawn_publisher(client_id: &str) -> AsyncClient {
        let mut opts = MqttOptions::new(client_id, "127.0.0.1", 1883);
        opts.set_keep_alive(5);
        let (client, mut eventloop) = AsyncClient::new(opts, 100);
        tokio::spawn(async move {
            loop {
                if eventloop.poll().await.is_err() {
                    // Broker down (e.g. during docker restart) -- back off and
                    // let rumqttc reconnect on the next poll.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        });
        client
    }

    /// Publish `marker`-tagged messages until the source delivers one carrying
    /// that marker, or the bounded `deadline` elapses. Returns whether a
    /// matching message was received. Ignores stale messages from other phases.
    async fn publish_until_marker(
        publisher: &AsyncClient,
        topic: &str,
        rx: &mut tokio::sync::mpsc::Receiver<Event>,
        marker: &str,
        deadline: Duration,
    ) -> bool {
        let start = std::time::Instant::now();
        let mut seq = 0u64;
        while start.elapsed() < deadline {
            seq += 1;
            let payload = format!(r#"{{"type":"Reading","marker":"{marker}","seq":{seq}}}"#);
            let _ = publisher
                .publish(topic, QoS::AtLeastOnce, false, payload.into_bytes())
                .await;
            match tokio::time::timeout(Duration::from_millis(400), rx.recv()).await {
                Ok(Some(ev)) if ev.get_str("marker") == Some(marker) => return true,
                Ok(Some(_)) => {}         // stale message from an earlier phase
                Ok(None) => return false, // channel closed
                Err(_) => {}              // timed out; publish again
            }
        }
        false
    }

    /// A broker restart drops the connection; rumqttc reconnects but a
    /// clean-session broker forgets the subscription. The source must re-issue
    /// SUBSCRIBE on ConnAck, otherwise it goes silently deaf while `health()`
    /// still reports connected. Fail-before/pass-after gate against a real
    /// mosquitto broker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mqtt_source_resubscribes_after_broker_restart() {
        if !broker_reachable() {
            eprintln!("[skip] mosquitto not reachable on {BROKER_ADDR}");
            return;
        }

        let topic = "varpulis/test/resub";

        // Source under test (subscribes at QoS 0 per config default).
        let config = MqttConfig::new("127.0.0.1", topic)
            .with_port(1883)
            .with_client_id("varpulis-resub-src");
        let mut source = MqttSource::new("resub-test", config);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
        source.start(tx).await.expect("source start");

        let publisher = spawn_publisher("varpulis-resub-pub");

        // (1) Baseline: prove the pipeline works before any reconnect. This
        //     passes via the initial subscribe regardless of the ConnAck fix,
        //     isolating the reconnect path as the thing under test.
        let baseline = publish_until_marker(
            &publisher,
            topic,
            &mut rx,
            "phase-1",
            Duration::from_secs(15),
        )
        .await;
        assert!(
            baseline,
            "baseline: source never received a message before any reconnect -- \
             pipeline broken independent of the fix"
        );

        // (2) Force a real reconnect by restarting the broker container.
        let restart = tokio::task::spawn_blocking(|| {
            std::process::Command::new("docker")
                .args(["restart", "varpulis-mosq"])
                .output()
        })
        .await
        .expect("join docker restart task");
        match restart {
            Ok(out) if out.status.success() => {}
            Ok(out) => {
                eprintln!(
                    "[skip] could not restart broker container varpulis-mosq: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                let _ = source.stop().await;
                return;
            }
            Err(e) => {
                eprintln!("[skip] docker unavailable to restart broker: {e}");
                let _ = source.stop().await;
                return;
            }
        }

        // Drain anything buffered through the restart so a post-restart receipt
        // is unambiguous (the marker check already guards this too).
        while rx.try_recv().is_ok() {}

        // (3) After reconnect, the source must receive again. With the ConnAck
        //     re-subscribe it does; without it this times out (silent deafness).
        let after = publish_until_marker(
            &publisher,
            topic,
            &mut rx,
            "phase-2",
            Duration::from_secs(30),
        )
        .await;
        assert!(
            after,
            "source received no message within 30s AFTER broker restart -- it failed \
             to re-subscribe on reconnect (silent data loss)"
        );

        let _ = source.stop().await;
    }
}
