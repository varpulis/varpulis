//! Apache Pulsar connector

use super::component::{ConnectorComponentInfo, ConnectorFactory};
#[cfg(feature = "pulsar")]
use super::helpers::json_to_event;
use super::types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};
use async_trait::async_trait;
use tokio::sync::mpsc;
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

/// Pulsar configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
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

#[cfg(feature = "pulsar")]
mod pulsar_impl {
    use super::*;
    use futures_util::TryStreamExt;
    use pulsar::{
        proto::command_subscribe::SubType, Authentication, Consumer, DeserializeMessage, Payload,
        Producer, Pulsar, SerializeMessage, TokioExecutor,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tracing::{info, warn};

    /// Message wrapper for Pulsar deserialization.
    /// We access the raw payload bytes via `msg.payload.data` directly.
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

    use pulsar::producer;

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
                            if data.len() > crate::limits::MAX_EVENT_PAYLOAD_BYTES {
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
                        Err(_) => {} // Timeout — loop back
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
    #[allow(clippy::large_futures)] // Pulsar library produces large internal futures
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
            let producer = guard.as_mut().ok_or_else(|| {
                ConnectorError::SendFailed("Pulsar producer not connected".into())
            })?;

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
}

#[cfg(feature = "pulsar")]
pub use pulsar_impl::{PulsarSink, PulsarSource};

// ============================================================================
// Stub implementations (no feature gate — used when pulsar feature is disabled)
// ============================================================================

/// Pulsar source stub (requires `pulsar` feature for full functionality).
#[cfg(not(feature = "pulsar"))]
#[derive(Debug)]
pub struct PulsarSource {
    name: String,
    #[allow(dead_code)]
    config: PulsarConfig,
}

#[cfg(not(feature = "pulsar"))]
impl PulsarSource {
    /// Create a new Pulsar source stub.
    pub fn new(name: &str, config: PulsarConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "pulsar"))]
#[async_trait]
impl SourceConnector for PulsarSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Pulsar connector requires 'pulsar' feature".to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn is_running(&self) -> bool {
        false
    }
}

/// Pulsar sink stub (requires `pulsar` feature for full functionality).
#[cfg(not(feature = "pulsar"))]
#[derive(Debug)]
pub struct PulsarSink {
    name: String,
    #[allow(dead_code)]
    config: PulsarConfig,
}

#[cfg(not(feature = "pulsar"))]
impl PulsarSink {
    /// Create a new Pulsar sink stub.
    pub fn new(name: &str, config: PulsarConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "pulsar"))]
#[async_trait]
impl SinkConnector for PulsarSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Pulsar connector requires 'pulsar' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
