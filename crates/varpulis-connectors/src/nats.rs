//! NATS source and sink connectors using async-nats.
//!
//! When the `nats` feature is enabled, this module provides full NATS
//! connectivity via `async-nats`.  When the feature is disabled, stub
//! implementations return `ConnectorError::NotAvailable`.

use async_trait::async_trait;
use tokio::sync::mpsc;
#[cfg(feature = "nats")]
use tracing::{info, warn};
use varpulis_core::security::SecretString;
use varpulis_core::Event;

use super::component::{ConfigParamInfo, ConnectorComponentInfo, ConnectorFactory};
use super::types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

static NATS_PARAMS: &[ConfigParamInfo] = &[
    ConfigParamInfo {
        name: "servers",
        description: "NATS server URL(s), comma-separated",
        required: true,
        default_value: None,
    },
    ConfigParamInfo {
        name: "subject",
        description: "NATS subject to subscribe/publish",
        required: true,
        default_value: None,
    },
    ConfigParamInfo {
        name: "queue_group",
        description: "Queue group for load-balanced consumption",
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
        name: "username",
        description: "NATS username",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "password",
        description: "NATS password (use credentials file for production)",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "token",
        description: "NATS auth token (use credentials file for production)",
        required: false,
        default_value: None,
    },
    ConfigParamInfo {
        name: "use_tls",
        description: "Require TLS (true/false)",
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
];

static NATS_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "nats",
    display_name: "NATS",
    description: "Cloud-native messaging system",
    feature_flag: "nats",
    supports_source: true,
    supports_sink: true,
    supports_managed: true,
    config_params: NATS_PARAMS,
};

struct NatsFactory;

impl ConnectorFactory for NatsFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &NATS_INFO
    }

    fn create_managed(
        &self,
        name: &str,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn super::managed::ManagedConnector>, ConnectorError> {
        let servers = if config.url.is_empty() {
            config
                .properties
                .get("servers")
                .cloned()
                .unwrap_or_else(|| "nats://localhost:4222".to_string())
        } else {
            config.url.clone()
        };
        let subject = config.topic.as_deref().unwrap_or(">");
        let mut nats_config = NatsConfig::new(&servers, subject);
        if let Some(queue_group) = config.properties.get("queue_group") {
            nats_config = nats_config.with_queue_group(queue_group);
        }
        // Security properties
        if let Some(username) = config.properties.get("username") {
            if let Some(password) = config.properties.get("password") {
                nats_config = nats_config.with_credentials(username, password);
            }
        }
        if let Some(token) = config.properties.get("token") {
            nats_config = nats_config.with_token(token);
        }
        if config
            .properties
            .get("use_tls")
            .is_some_and(|v| v == "true")
        {
            nats_config = nats_config.with_tls(true);
        }
        if let Some(ca) = config.properties.get("ssl_ca_location") {
            nats_config = nats_config.with_ca_cert(ca);
        }
        if let (Some(cert), Some(key)) = (
            config.properties.get("ssl_certificate_location"),
            config.properties.get("ssl_key_location"),
        ) {
            nats_config = nats_config.with_client_cert(cert, key);
        }
        Ok(Box::new(super::managed_nats::ManagedNatsConnector::new(
            name,
            nats_config,
        )))
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let subject = config.topic.clone().unwrap_or_else(|| "events".to_string());
        let servers = if config.url.is_empty() {
            config
                .properties
                .get("servers")
                .cloned()
                .unwrap_or_else(|| "nats://localhost:4222".to_string())
        } else {
            config.url.clone()
        };
        let mut nats_config = NatsConfig::new(&servers, &subject);
        // Security properties
        if let Some(username) = config.properties.get("username") {
            if let Some(password) = config.properties.get("password") {
                nats_config = nats_config.with_credentials(username, password);
            }
        }
        if let Some(token) = config.properties.get("token") {
            nats_config = nats_config.with_token(token);
        }
        if config
            .properties
            .get("use_tls")
            .is_some_and(|v| v == "true")
        {
            nats_config = nats_config.with_tls(true);
        }
        if let Some(ca) = config.properties.get("ssl_ca_location") {
            nats_config = nats_config.with_ca_cert(ca);
        }
        if let (Some(cert), Some(key)) = (
            config.properties.get("ssl_certificate_location"),
            config.properties.get("ssl_key_location"),
        ) {
            nats_config = nats_config.with_client_cert(cert, key);
        }
        Ok(Box::new(NatsSink::new("nats", nats_config)))
    }
}

inventory::submit! { &NatsFactory as &dyn ConnectorFactory }

// =============================================================================
// NATS Configuration (always available, not feature-gated)
// =============================================================================

/// NATS configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct NatsConfig {
    /// NATS server URL(s), comma-separated.
    pub servers: String,
    /// NATS subject to subscribe to or publish on.
    pub subject: String,
    /// Queue group for load-balanced consumption (optional).
    pub queue_group: Option<String>,
    /// Username for authentication (optional).
    pub username: Option<String>,
    /// Password for authentication (zeroized on drop).
    pub password: Option<SecretString>,
    /// Authentication token (zeroized on drop).
    pub token: Option<SecretString>,
    /// Require TLS for the connection. Default: false.
    pub use_tls: bool,
    /// Path to CA certificate file (PEM format).
    pub ssl_ca_location: Option<String>,
    /// Path to client certificate file (PEM format) for mTLS.
    pub ssl_certificate_location: Option<String>,
    /// Path to client private key file (PEM format) for mTLS.
    pub ssl_key_location: Option<String>,
}

impl NatsConfig {
    /// Create a new NATS configuration with the given server(s) and subject.
    pub fn new(servers: &str, subject: &str) -> Self {
        Self {
            servers: servers.to_string(),
            subject: subject.to_string(),
            queue_group: None,
            username: None,
            password: None,
            token: None,
            use_tls: false,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
        }
    }

    /// Set the queue group for load-balanced consumption.
    pub fn with_queue_group(mut self, group: &str) -> Self {
        self.queue_group = Some(group.to_string());
        self
    }

    /// Set username and password for authentication.
    pub fn with_credentials(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(SecretString::new(password));
        self
    }

    /// Set an authentication token.
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = Some(SecretString::new(token));
        self
    }

    /// Require TLS for the NATS connection.
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

// -----------------------------------------------------------------------------
// NATS with async-nats feature enabled
// -----------------------------------------------------------------------------
#[cfg(feature = "nats")]
mod nats_impl {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::StreamExt;
    use rustc_hash::FxBuildHasher;
    use varpulis_core::event::{FieldKey, FxIndexMap};

    use super::*;

    /// NATS source connector with async-nats
    #[derive(Debug)]
    pub struct NatsSource {
        name: String,
        config: NatsConfig,
        running: Arc<AtomicBool>,
    }

    impl NatsSource {
        /// Creates a new NATS source connector.
        pub fn new(name: &str, config: NatsConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[async_trait]
    impl SourceConnector for NatsSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            let opts = build_connect_options(&self.config);
            let client = async_nats::connect_with_options(&self.config.servers, opts)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            let subscriber = if let Some(group) = &self.config.queue_group {
                client
                    .queue_subscribe(self.config.subject.clone(), group.clone())
                    .await
            } else {
                client.subscribe(self.config.subject.clone()).await
            }
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.running.store(true, Ordering::SeqCst);

            info!(
                "NATS source {} connected to {}",
                self.name, self.config.servers
            );
            info!("  Subscribed to: {}", self.config.subject);

            let running = self.running.clone();
            let name = self.name.clone();

            tokio::spawn(async move {
                use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
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
                            if message.payload.len() > crate::limits::MAX_EVENT_PAYLOAD_BYTES {
                                warn!(
                                    "NATS source {}: payload too large ({} bytes, max {}), skipped",
                                    name,
                                    message.payload.len(),
                                    crate::limits::MAX_EVENT_PAYLOAD_BYTES
                                );
                            } else if let Ok(payload) = std::str::from_utf8(&message.payload) {
                                let subject = message.subject.as_str();
                                if let Some(event) = parse_nats_payload(payload, subject) {
                                    if tx.send(event).await.is_err() {
                                        warn!("NATS source {} channel closed", name);
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            cb.record_failure();
                            warn!(
                                "NATS source {} subscription ended (cb_state={}), backing off",
                                name,
                                cb.state()
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Err(_) => {
                            // Timeout — just loop back to check running flag
                            continue;
                        }
                    }
                }
                info!("NATS source {} receive loop stopped", name);
            });

            Ok(())
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running.store(false, Ordering::SeqCst);
            info!("NATS source {} stopped", self.name);
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    /// NATS sink connector with async-nats
    pub struct NatsSink {
        name: String,
        config: NatsConfig,
        client: Option<async_nats::Client>,
    }

    impl std::fmt::Debug for NatsSink {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("NatsSink")
                .field("name", &self.name)
                .finish_non_exhaustive()
        }
    }

    impl NatsSink {
        /// Creates a new NATS sink connector.
        pub fn new(name: &str, config: NatsConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                client: None,
            }
        }
    }

    #[async_trait]
    impl SinkConnector for NatsSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn connect(&mut self) -> Result<(), ConnectorError> {
            if self.client.is_some() {
                return Ok(());
            }

            let opts = build_connect_options(&self.config);
            let client = async_nats::connect_with_options(&self.config.servers, opts)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.client = Some(client);
            info!(
                "NATS sink {} connected to {}",
                self.name, self.config.servers
            );
            Ok(())
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let client = self.client.as_ref().ok_or(ConnectorError::NotConnected)?;
            let buf = event.to_sink_payload();

            client
                .publish(self.config.subject.clone(), buf.into())
                .await
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        async fn send_to_topic(
            &self,
            events: &[std::sync::Arc<Event>],
            subject: &str,
        ) -> Result<(), ConnectorError> {
            let client = self.client.as_ref().ok_or(ConnectorError::NotConnected)?;
            let subject: async_nats::Subject = subject.into();
            for event in events {
                let buf = event.to_sink_payload();
                client
                    .publish(subject.clone(), buf.into())
                    .await
                    .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;
            }
            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            if let Some(client) = &self.client {
                client
                    .flush()
                    .await
                    .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;
            }
            Ok(())
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            // async-nats Client is dropped automatically; no explicit disconnect
            Ok(())
        }
    }

    /// Build async-nats ConnectOptions from NatsConfig.
    fn build_connect_options(config: &NatsConfig) -> async_nats::ConnectOptions {
        let mut opts = async_nats::ConnectOptions::new();
        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            opts = opts.user_and_password(user.clone(), pass.expose().to_string());
        }
        if let Some(token) = &config.token {
            opts = opts.token(token.expose().to_string());
        }
        // TLS
        if config.use_tls {
            opts = opts.require_tls(true);
        }
        if let Some(ca_path) = &config.ssl_ca_location {
            opts = opts.add_root_certificates(std::path::PathBuf::from(ca_path));
        }
        if let (Some(cert_path), Some(key_path)) =
            (&config.ssl_certificate_location, &config.ssl_key_location)
        {
            opts = opts.add_client_certificate(
                std::path::PathBuf::from(cert_path),
                std::path::PathBuf::from(key_path),
            );
        }
        opts
    }

    /// Parse JSON payload directly into an Event.
    ///
    /// Uses the same pattern as MQTT parser. Falls back to the last
    /// `.`-delimited subject segment for event_type (NATS uses `.` as
    /// separator, unlike MQTT's `/`).
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

        let max_fields = crate::limits::MAX_FIELDS_PER_EVENT;

        if has_data {
            let data_obj = map.get("data" as &str).unwrap().as_object().unwrap();
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
    fn json_value_to_native(v: &serde_json::Value, _depth: usize) -> varpulis_core::Value {
        crate::helpers::json_to_value(v).unwrap_or(varpulis_core::Value::Null)
    }
}

// -----------------------------------------------------------------------------
// NATS stub when feature disabled
// -----------------------------------------------------------------------------
#[cfg(not(feature = "nats"))]
mod nats_impl {
    use super::*;

    /// NATS source stub (requires `nats` feature for full functionality).
    #[derive(Debug)]
    pub struct NatsSource {
        name: String,
        #[allow(dead_code)]
        config: NatsConfig,
        running: bool,
    }

    impl NatsSource {
        /// Create a new NATS source stub.
        pub fn new(name: &str, config: NatsConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: false,
            }
        }
    }

    #[async_trait]
    impl SourceConnector for NatsSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            Err(ConnectorError::NotAvailable(
                "NATS requires 'nats' feature. Build with: cargo build --features nats".to_string(),
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

    /// NATS sink stub (requires `nats` feature for full functionality).
    #[derive(Debug)]
    pub struct NatsSink {
        name: String,
        #[allow(dead_code)]
        config: NatsConfig,
    }

    impl NatsSink {
        /// Create a new NATS sink stub.
        pub fn new(name: &str, config: NatsConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
            }
        }
    }

    #[async_trait]
    impl SinkConnector for NatsSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
            Err(ConnectorError::NotAvailable(
                "NATS requires 'nats' feature".to_string(),
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

pub use nats_impl::{NatsSink, NatsSource};
