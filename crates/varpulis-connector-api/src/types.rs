//! Core connector types, traits, and error definitions

use async_trait::async_trait;
use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use varpulis_core::Event;

/// Generic connector configuration.
///
/// This struct provides a unified configuration format for all connector types,
/// allowing dynamic connector creation based on configuration files.
///
/// # Fields
///
/// - `connector_type`: The type of connector (`"mqtt"`, `"kafka"`, `"http"`, etc.)
/// - `url`: Connection URL or address
/// - `topic`: Optional topic, channel, or path for the connection
/// - `properties`: Additional key-value properties specific to the connector type
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorConfig {
    /// Connector type identifier (e.g., "mqtt", "kafka", "http", "file")
    pub connector_type: String,
    /// Connection URL or address (format depends on connector type)
    pub url: String,
    /// Topic, channel, queue name, or file path (optional, depends on connector)
    pub topic: Option<String>,
    /// Additional connector-specific properties
    pub properties: IndexMap<String, String>,
}

/// Mask the password in a `scheme://user:password@host…` URL.
///
/// Returns the URL unchanged when there is no userinfo password. Connection URLs
/// (Redis, AMQP, Postgres, Mongo…) routinely embed the password this way.
fn redact_url_password(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    // Userinfo is everything before the first '@' within the authority (up to
    // the first '/').
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let Some(at) = rest[..authority_end].find('@') else {
        return url.to_string();
    };
    let userinfo = &rest[..at];
    let Some(colon) = userinfo.find(':') else {
        return url.to_string();
    };
    format!(
        "{}://{}:***REDACTED***@{}",
        &url[..scheme_end],
        &userinfo[..colon],
        &rest[at + 1..]
    )
}

/// True when a property key names a secret that must not be logged.
fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k.contains("password")
        || k.contains("secret")
        || k.contains("token")
        || k.contains("apikey")
        || k.contains("api_key")
}

/// Hand-written so credentials never reach logs / errors / panic output via
/// `{:?}`. The derived Debug printed the raw `properties` map (which carries
/// password/token/sasl_password before they become typed SecretStrings) and the
/// `url` (which can embed `user:password@`).
impl std::fmt::Debug for ConnectorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted_props: IndexMap<&String, &str> = self
            .properties
            .iter()
            .map(|(k, v)| {
                if is_secret_key(k) {
                    (k, "***REDACTED***")
                } else {
                    (k, v.as_str())
                }
            })
            .collect();
        f.debug_struct("ConnectorConfig")
            .field("connector_type", &self.connector_type)
            .field("url", &redact_url_password(&self.url))
            .field("topic", &self.topic)
            .field("properties", &redacted_props)
            .finish()
    }
}

impl ConnectorConfig {
    /// Create a new connector configuration with the given type and URL.
    pub fn new(connector_type: &str, url: &str) -> Self {
        Self {
            connector_type: connector_type.to_string(),
            url: url.to_string(),
            topic: None,
            properties: IndexMap::new(),
        }
    }

    /// Set the topic, channel, or path for this connector.
    pub fn with_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }

    /// Add a connector-specific property.
    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }
}

/// Engine-wide source offset registry.
///
/// Maps connector name → (partition → last-consumed offset). Source
/// connectors update entries as events flow into the engine; the engine
/// snapshots the whole map at checkpoint time and the driver commits the
/// per-source entries as part of the 2PC commit phase.
pub type EngineOffsetRegistry = std::sync::Arc<
    std::sync::Mutex<std::collections::HashMap<String, std::collections::HashMap<i32, i64>>>,
>;

/// Trait for source connectors that ingest events from external systems.
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Returns the name/identifier of this connector instance.
    fn name(&self) -> &str;

    /// Start receiving events and forward them to the provided channel.
    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError>;

    /// Stop the connector gracefully.
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Check if the connector is currently running.
    fn is_running(&self) -> bool;

    /// Return the current health status of this connector.
    fn health_check(&self) -> ConnectorHealth {
        if self.is_running() {
            ConnectorHealth::healthy(0)
        } else {
            ConnectorHealth::unhealthy("not running")
        }
    }

    // ---- Checkpoint-aligned source offset tracking ----
    //
    // These hooks form the input-side half of end-to-end exactly-once,
    // paired with the 2PC sink interface. Sources that support replayable
    // offsets (Kafka, Pulsar, file-based sources with seekable positions)
    // override them; others keep the no-op defaults and fall back to
    // at-least-once.

    /// Whether this source tracks replayable per-partition offsets.
    fn supports_offset_checkpoint(&self) -> bool {
        false
    }

    /// Return the latest consumed offset per partition since `start()`.
    ///
    /// The snapshot must be consistent with what has already been pushed
    /// into the engine's event channel — i.e. every returned offset
    /// corresponds to a record whose event has successfully flowed into
    /// the pipeline.
    fn snapshot_offsets(&self) -> std::collections::HashMap<i32, i64> {
        std::collections::HashMap::new()
    }

    /// Commit the given offsets back to the source's consumer group
    /// (or equivalent) as part of a 2PC checkpoint commit.
    async fn commit_offsets(
        &self,
        _offsets: &std::collections::HashMap<i32, i64>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Trait for sink connectors that send events to external systems.
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Returns the name/identifier of this connector instance.
    fn name(&self) -> &str;

    /// Establish connection to the external system.
    async fn connect(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Send an event to the external system.
    async fn send(&self, event: &Event) -> Result<(), ConnectorError>;

    /// Send a batch of events to a specific topic (for dynamic routing).
    async fn send_to_topic(
        &self,
        events: &[std::sync::Arc<Event>],
        _topic: &str,
    ) -> Result<(), ConnectorError> {
        for event in events {
            self.send(event).await?;
        }
        Ok(())
    }

    /// Flush any internally buffered events.
    async fn flush(&self) -> Result<(), ConnectorError>;

    /// Close the connector and release resources.
    async fn close(&self) -> Result<(), ConnectorError>;

    /// Return the current health status of this connector.
    fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth::healthy(0)
    }

    // ---- Two-Phase Commit (2PC) for exactly-once delivery ----

    /// Whether this connector supports exactly-once 2PC semantics.
    fn supports_exactly_once(&self) -> bool {
        false
    }

    /// Begin a new checkpoint epoch.
    async fn begin_epoch(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Pre-commit: finalize data but don't make visible.
    async fn prepare_commit(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Commit: make pre-committed data visible.
    async fn commit(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Abort: discard uncommitted data for the current epoch.
    async fn abort(&self, _checkpoint_id: u64) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Health status returned by connector health checks.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorHealth {
    /// Whether the connector is operational.
    pub healthy: bool,
    /// Human-readable status message.
    pub message: String,
    /// Number of events processed since the connector started.
    pub events_processed: u64,
    /// Number of errors encountered since the connector started.
    pub errors: u64,
}

impl ConnectorHealth {
    /// Create a healthy status.
    pub fn healthy(events_processed: u64) -> Self {
        Self {
            healthy: true,
            message: "ok".to_string(),
            events_processed,
            errors: 0,
        }
    }

    /// Create an unhealthy status with an error message.
    pub fn unhealthy(message: impl Into<String>) -> Self {
        Self {
            healthy: false,
            message: message.into(),
            events_processed: 0,
            errors: 0,
        }
    }
}

/// Errors that can occur during connector operations.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Failed to establish connection to the external system.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Failed to send/publish an event.
    #[error("Send failed: {0}")]
    SendFailed(String),

    /// Failed to receive an event from the source.
    #[error("Receive failed: {0}")]
    ReceiveFailed(String),

    /// Invalid or incomplete configuration.
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Operation attempted on a disconnected connector.
    #[error("Not connected")]
    NotConnected,

    /// Requested connector type is not available.
    #[error("Connector not available: {0}")]
    NotAvailable(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_config_debug_redacts_url_and_property_secrets() {
        let config = ConnectorConfig::new("redis", "redis://user:s3cr3t-pw@host:6379")
            .with_property("password", "prop-secret")
            .with_property("auth_token", "tok-xyz")
            .with_property("sasl_username", "svc-account");
        let rendered = format!("{config:?}");
        // URL password masked.
        assert!(
            !rendered.contains("s3cr3t-pw"),
            "url password leaked: {rendered}"
        );
        // Secret-keyed properties masked.
        assert!(
            !rendered.contains("prop-secret"),
            "property password leaked: {rendered}"
        );
        assert!(
            !rendered.contains("tok-xyz"),
            "property token leaked: {rendered}"
        );
        assert!(rendered.contains("***REDACTED***"));
        // Non-secret property + the URL user stay visible.
        assert!(
            rendered.contains("svc-account"),
            "non-secret should stay: {rendered}"
        );
        assert!(
            rendered.contains("user:***REDACTED***@host"),
            "url user kept, pw masked: {rendered}"
        );
    }

    #[test]
    fn redact_url_password_leaves_plain_urls_and_masks_userinfo() {
        assert_eq!(
            redact_url_password("redis://host:6379"),
            "redis://host:6379"
        );
        assert_eq!(redact_url_password("http://host/path"), "http://host/path");
        assert_eq!(
            redact_url_password("amqp://:pw@host"),
            "amqp://:***REDACTED***@host"
        );
    }
}
