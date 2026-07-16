//! Apache Pulsar source and sink connectors.
//!
//! This crate provides full Pulsar connectivity for the Varpulis CEP engine
//! via the `pulsar` crate.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use pulsar::proto::command_subscribe::SubType;
use pulsar::{
    producer, Authentication, Consumer, DeserializeMessage, Payload, Producer, Pulsar,
    SerializeMessage, TokioExecutor,
};
use tokio::sync::mpsc;
use tracing::{info, warn};
use varpulis_connector_api::helpers::json_to_event;
use varpulis_connector_api::{
    ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory, SinkConnector,
    SourceConnector,
};
use varpulis_core::Event;

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

static PULSAR_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "pulsar",
    display_name: "Apache Pulsar",
    description: "Apache Pulsar messaging connector",
    feature_flag: "pulsar",
    supports_source: true,
    supports_sink: true,
    supports_managed: false,
    config_params: &[],
};

struct PulsarFactory;

impl ConnectorFactory for PulsarFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &PULSAR_INFO
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
        Ok(Box::new(PulsarSink::new(
            "pulsar",
            PulsarConfig::new(&config.url, &topic),
        )))
    }
}

inventory::submit! { &PulsarFactory as &dyn ConnectorFactory }

// =============================================================================
// Configuration
// =============================================================================

/// Pulsar configuration
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct PulsarConfig {
    /// Pulsar service URL (e.g., `"pulsar://localhost:6650"`).
    pub service_url: String,
    /// Pulsar topic to consume from or produce to.
    pub topic: String,
    /// Subscription name for consuming (optional, auto-generated if not set).
    pub subscription: Option<String>,
    /// Consumer name for identification (optional).
    pub consumer_name: Option<String>,
    /// Number of messages to fetch per batch (default: 100).
    pub batch_size: usize,
    /// Authentication token (optional).
    pub token: Option<String>,
}

/// Hand-written so the auth `token` never reaches logs / errors / panic output
/// via `{:?}`. The derived Debug printed it in full; Some/None is preserved so
/// it's still visible whether a token is configured.
impl std::fmt::Debug for PulsarConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarConfig")
            .field("service_url", &self.service_url)
            .field("topic", &self.topic)
            .field("subscription", &self.subscription)
            .field("consumer_name", &self.consumer_name)
            .field("batch_size", &self.batch_size)
            .field("token", &self.token.as_ref().map(|_| "***REDACTED***"))
            .finish()
    }
}

impl PulsarConfig {
    /// Create a new Pulsar configuration with the given service URL and topic.
    pub fn new(service_url: &str, topic: &str) -> Self {
        Self {
            service_url: service_url.to_string(),
            topic: topic.to_string(),
            subscription: None,
            consumer_name: None,
            batch_size: 100,
            token: None,
        }
    }

    /// Set the subscription name for consuming.
    pub fn with_subscription(mut self, subscription: &str) -> Self {
        self.subscription = Some(subscription.to_string());
        self
    }

    /// Set the consumer name for identification.
    pub fn with_consumer_name(mut self, name: &str) -> Self {
        self.consumer_name = Some(name.to_string());
        self
    }

    /// Set the number of messages to fetch per batch.
    pub const fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the authentication token.
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = Some(token.to_string());
        self
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Message wrapper for Pulsar deserialization.
struct JsonMessage;

impl DeserializeMessage for JsonMessage {
    type Output = Result<JsonMessage, serde_json::Error>;

    fn deserialize_message(_payload: &Payload) -> Self::Output {
        Ok(JsonMessage)
    }
}

/// Message wrapper for Pulsar serialization
struct JsonPayload {
    data: Vec<u8>,
}

impl SerializeMessage for JsonPayload {
    fn serialize_message(input: Self) -> Result<producer::Message, pulsar::Error> {
        Ok(producer::Message {
            payload: input.data,
            ..Default::default()
        })
    }
}

async fn build_client(config: &PulsarConfig) -> Result<Pulsar<TokioExecutor>, ConnectorError> {
    let mut builder = Pulsar::builder(&config.service_url, TokioExecutor);

    if let Some(ref token) = config.token {
        builder = builder.with_auth(Authentication {
            name: "token".to_string(),
            data: token.as_bytes().to_vec(),
        });
    }

    builder
        .build()
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("Pulsar: {}", e)))
}

// =============================================================================
// Pulsar Source
// =============================================================================

/// Pulsar source that consumes from a topic
#[derive(Debug)]
pub struct PulsarSource {
    name: String,
    config: PulsarConfig,
    running: Arc<AtomicBool>,
}

impl PulsarSource {
    /// Creates a new Pulsar source connector.
    pub fn new(name: &str, config: PulsarConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl SourceConnector for PulsarSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        let client = build_client(&self.config).await?;

        let subscription = self
            .config
            .subscription
            .clone()
            .unwrap_or_else(|| format!("varpulis-{}", self.name));

        let mut consumer_builder = client
            .consumer()
            .with_topic(&self.config.topic)
            .with_subscription(&subscription)
            .with_subscription_type(SubType::Shared);

        if let Some(ref cname) = self.config.consumer_name {
            consumer_builder = consumer_builder.with_consumer_name(cname);
        }

        consumer_builder = consumer_builder.with_batch_size(self.config.batch_size as u32);

        let mut consumer: Consumer<JsonMessage, TokioExecutor> = consumer_builder
            .build()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("Pulsar consumer: {}", e)))?;

        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let name = self.name.clone();

        tokio::spawn(async move {
            info!("Pulsar source {} started, consuming from topic", name);

            while running.load(Ordering::SeqCst) {
                match tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    consumer.try_next(),
                )
                .await
                {
                    Ok(Ok(Some(msg))) => {
                        let data = &msg.payload.data;
                        if data.len() > varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES {
                            warn!(
                                "Pulsar source {}: payload too large ({} bytes), skipped",
                                name,
                                data.len()
                            );
                            let _ = consumer.ack(&msg).await;
                            continue;
                        }

                        match serde_json::from_slice::<serde_json::Value>(data) {
                            Ok(json) => {
                                let event_type = json
                                    .get("event_type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("PulsarEvent")
                                    .to_string();

                                let event = json_to_event(&event_type, &json);

                                if tx.send(event).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("Pulsar source {}: failed to parse JSON: {}", name, e);
                            }
                        }

                        let _ = consumer.ack(&msg).await;
                    }
                    Ok(Ok(None)) => break,
                    Ok(Err(e)) => {
                        warn!("Pulsar source {}: consumer error: {}", name, e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    Err(_) => {} // Timeout
                }
            }

            info!("Pulsar source {} stopped", name);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

// =============================================================================
// Pulsar Sink
// =============================================================================

/// Pulsar sink that produces to a topic
pub struct PulsarSink {
    name: String,
    config: PulsarConfig,
    producer: tokio::sync::Mutex<Option<Producer<TokioExecutor>>>,
}

impl std::fmt::Debug for PulsarSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarSink")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl PulsarSink {
    /// Creates a new Pulsar sink connector.
    pub fn new(name: &str, config: PulsarConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            producer: tokio::sync::Mutex::new(None),
        }
    }
}

#[async_trait]
#[allow(clippy::large_futures)]
impl SinkConnector for PulsarSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(&mut self) -> Result<(), ConnectorError> {
        let client = build_client(&self.config).await?;
        let producer = client
            .producer()
            .with_topic(&self.config.topic)
            .build()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("Pulsar producer: {}", e)))?;
        *self.producer.lock().await = Some(producer);
        Ok(())
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let mut guard = self.producer.lock().await;
        let producer = guard
            .as_mut()
            .ok_or_else(|| ConnectorError::SendFailed("Pulsar producer not connected".into()))?;

        let payload = event.to_sink_payload();

        producer
            .send_non_blocking(JsonPayload { data: payload })
            .await
            .map_err(|e| ConnectorError::SendFailed(format!("Pulsar send: {}", e)))?
            .await
            .map_err(|e| ConnectorError::SendFailed(format!("Pulsar send receipt: {}", e)))?;

        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pulsar_config_debug_redacts_token() {
        let config = PulsarConfig::new("pulsar://localhost:6650", "topic")
            .with_token("super-secret-jwt-token");
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("super-secret-jwt-token"),
            "auth token leaked in Debug: {rendered}"
        );
        assert!(
            rendered.contains("***REDACTED***"),
            "configured token should be masked"
        );
        // No token → None (not REDACTED).
        let no_token = format!(
            "{:?}",
            PulsarConfig::new("pulsar://localhost:6650", "topic")
        );
        assert!(no_token.contains("token: None"), "got: {no_token}");
    }

    #[test]
    fn test_pulsar_config_new() {
        let config = PulsarConfig::new(
            "pulsar://localhost:6650",
            "persistent://public/default/events",
        );
        assert_eq!(config.service_url, "pulsar://localhost:6650");
        assert_eq!(config.topic, "persistent://public/default/events");
        assert!(config.subscription.is_none());
        assert!(config.consumer_name.is_none());
        assert_eq!(config.batch_size, 100);
        assert!(config.token.is_none());
    }

    #[test]
    fn test_pulsar_config_with_subscription() {
        let config = PulsarConfig::new("pulsar://host:6650", "topic").with_subscription("my-sub");
        assert_eq!(config.subscription.as_deref(), Some("my-sub"));
    }

    #[test]
    fn test_pulsar_config_with_consumer_name() {
        let config =
            PulsarConfig::new("pulsar://host:6650", "topic").with_consumer_name("worker-0");
        assert_eq!(config.consumer_name.as_deref(), Some("worker-0"));
    }

    #[test]
    fn test_pulsar_config_with_batch_size() {
        let config = PulsarConfig::new("pulsar://host:6650", "topic").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn test_pulsar_config_with_token() {
        let config =
            PulsarConfig::new("pulsar://host:6650", "topic").with_token("eyJhbGciOiJSUzI1NiJ9...");
        assert_eq!(config.token.as_deref(), Some("eyJhbGciOiJSUzI1NiJ9..."));
    }

    #[test]
    fn test_pulsar_config_serialization_roundtrip() {
        let config = PulsarConfig::new("pulsar://host:6650", "my-topic")
            .with_subscription("sub1")
            .with_batch_size(200);
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PulsarConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.service_url, "pulsar://host:6650");
        assert_eq!(deserialized.topic, "my-topic");
        assert_eq!(deserialized.subscription.as_deref(), Some("sub1"));
        assert_eq!(deserialized.batch_size, 200);
    }

    #[test]
    fn test_pulsar_source_initial_state() {
        let config = PulsarConfig::new("pulsar://host:6650", "topic");
        let source = PulsarSource::new("pulsar-src", config);
        assert_eq!(source.name, "pulsar-src");
        assert!(!source.is_running());
    }

    #[test]
    fn test_pulsar_sink_initial_state() {
        let config = PulsarConfig::new("pulsar://host:6650", "topic");
        let sink = PulsarSink::new("pulsar-sink", config);
        assert_eq!(sink.name, "pulsar-sink");
    }

    #[test]
    fn test_pulsar_info_static() {
        assert_eq!(PULSAR_INFO.connector_type, "pulsar");
        assert!(PULSAR_INFO.supports_source);
        assert!(PULSAR_INFO.supports_sink);
        assert!(!PULSAR_INFO.supports_managed);
    }

    #[test]
    fn test_json_payload_serialize() {
        let payload = JsonPayload {
            data: b"hello world".to_vec(),
        };
        let msg = JsonPayload::serialize_message(payload).unwrap();
        assert_eq!(msg.payload, b"hello world");
    }

    #[test]
    fn test_json_message_deserialize() {
        let payload = Payload {
            data: b"{}".to_vec(),
            metadata: Default::default(),
        };
        let result = JsonMessage::deserialize_message(&payload);
        assert!(result.is_ok());
    }
}
