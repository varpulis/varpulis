//! NATS source and sink connectors using async-nats.
//!
//! When the `nats` feature is enabled, this module provides full NATS
//! connectivity via `async-nats`.  When the feature is disabled, stub
//! implementations return `ConnectorError::NotAvailable`.

use super::types::{ConnectorError, SinkConnector, SourceConnector};
use crate::event::Event;
use async_trait::async_trait;
use tokio::sync::mpsc;
#[cfg(feature = "nats")]
use tracing::{info, warn};
use varpulis_core::security::SecretString;

// =============================================================================
// NATS Configuration (always available, not feature-gated)
// =============================================================================

/// NATS configuration
#[derive(Debug, Clone)]
pub struct NatsConfig {
    pub servers: String,
    pub subject: String,
    pub queue_group: Option<String>,
    pub username: Option<String>,
    pub password: Option<SecretString>,
    pub token: Option<SecretString>,
}

impl NatsConfig {
    pub fn new(servers: &str, subject: &str) -> Self {
        Self {
            servers: servers.to_string(),
            subject: subject.to_string(),
            queue_group: None,
            username: None,
            password: None,
            token: None,
        }
    }

    pub fn with_queue_group(mut self, group: &str) -> Self {
        self.queue_group = Some(group.to_string());
        self
    }

    pub fn with_credentials(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(SecretString::new(password));
        self
    }

    pub fn with_token(mut self, token: &str) -> Self {
        self.token = Some(SecretString::new(token));
        self
    }
}

// -----------------------------------------------------------------------------
// NATS with async-nats feature enabled
// -----------------------------------------------------------------------------
#[cfg(feature = "nats")]
mod nats_impl {
    use super::*;
    use crate::event::{FieldKey, FxIndexMap};
    use futures::StreamExt;
    use rustc_hash::FxBuildHasher;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// NATS source connector with async-nats
    pub struct NatsSource {
        name: String,
        config: NatsConfig,
        running: Arc<AtomicBool>,
    }

    impl NatsSource {
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
                let mut subscriber = subscriber;

                while running.load(Ordering::SeqCst) {
                    match tokio::time::timeout(Duration::from_secs(30), subscriber.next()).await {
                        Ok(Some(message)) => {
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
                            // Subscription ended
                            info!("NATS source {} subscription ended", name);
                            break;
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

    impl NatsSink {
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
// NATS stub when feature disabled
// -----------------------------------------------------------------------------
#[cfg(not(feature = "nats"))]
mod nats_impl {
    use super::*;

    pub struct NatsSource {
        name: String,
        #[allow(dead_code)]
        config: NatsConfig,
        running: bool,
    }

    impl NatsSource {
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

    pub struct NatsSink {
        name: String,
        #[allow(dead_code)]
        config: NatsConfig,
    }

    impl NatsSink {
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
