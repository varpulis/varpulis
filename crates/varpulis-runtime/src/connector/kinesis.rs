//! AWS Kinesis connector

use super::types::{ConnectorError, SinkConnector, SourceConnector};
use crate::event::Event;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::warn;
#[cfg(feature = "kinesis")]
use tracing::{error, info};

/// AWS Kinesis configuration
///
/// Configuration for connecting to AWS Kinesis Data Streams.
///
/// # Example
///
/// ```rust
/// use varpulis_runtime::connector::KinesisConfig;
///
/// let config = KinesisConfig::new("my-stream", "us-east-1")
///     .with_shard_iterator_type("LATEST")
///     .with_batch_size(100);
/// ```
#[derive(Debug, Clone)]
pub struct KinesisConfig {
    /// Kinesis stream name
    pub stream_name: String,
    /// AWS region (e.g., "us-east-1")
    pub region: String,
    /// Shard iterator type: "TRIM_HORIZON", "LATEST", "AT_TIMESTAMP"
    pub shard_iterator_type: String,
    /// Maximum records per GetRecords call (1-10000, default 100)
    pub batch_size: i32,
    /// Poll interval in milliseconds
    pub poll_interval_ms: u64,
    /// Partition key for producing (optional, defaults to UUID)
    pub partition_key: Option<String>,
    /// Consumer group name (for enhanced fan-out)
    pub consumer_name: Option<String>,
    /// AWS profile name (optional, uses default credentials chain if not set)
    pub profile: Option<String>,
}

impl KinesisConfig {
    /// Create a new Kinesis configuration
    ///
    /// # Arguments
    /// * `stream_name` - The name of the Kinesis stream
    /// * `region` - The AWS region (e.g., "us-east-1")
    pub fn new(stream_name: &str, region: &str) -> Self {
        Self {
            stream_name: stream_name.to_string(),
            region: region.to_string(),
            shard_iterator_type: "LATEST".to_string(),
            batch_size: 100,
            poll_interval_ms: 200,
            partition_key: None,
            consumer_name: None,
            profile: None,
        }
    }

    /// Set the shard iterator type
    ///
    /// - "TRIM_HORIZON": Start from the oldest record
    /// - "LATEST": Start from the newest record (default)
    /// - "AT_TIMESTAMP": Start from a specific timestamp
    pub fn with_shard_iterator_type(mut self, iterator_type: &str) -> Self {
        self.shard_iterator_type = iterator_type.to_string();
        self
    }

    /// Set the batch size for GetRecords calls (1-10000)
    pub fn with_batch_size(mut self, size: i32) -> Self {
        self.batch_size = size.clamp(1, 10000);
        self
    }

    /// Set the poll interval in milliseconds
    pub fn with_poll_interval(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms;
        self
    }

    /// Set a fixed partition key for producing
    pub fn with_partition_key(mut self, key: &str) -> Self {
        self.partition_key = Some(key.to_string());
        self
    }

    /// Set the consumer name for enhanced fan-out
    pub fn with_consumer_name(mut self, name: &str) -> Self {
        self.consumer_name = Some(name.to_string());
        self
    }

    /// Set the AWS profile name
    pub fn with_profile(mut self, profile: &str) -> Self {
        self.profile = Some(profile.to_string());
        self
    }
}

/// Kinesis source connector (stub implementation)
///
/// This stub is always available. For full functionality, enable the `kinesis` feature.
pub struct KinesisSource {
    name: String,
    config: KinesisConfig,
    running: bool,
}

impl KinesisSource {
    /// Create a new Kinesis source
    pub fn new(name: &str, config: KinesisConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: false,
        }
    }
}

#[async_trait]
impl SourceConnector for KinesisSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        warn!(
            "Kinesis source {} starting (stub implementation - requires kinesis feature)",
            self.name
        );
        warn!("  Stream: {}", self.config.stream_name);
        warn!("  Region: {}", self.config.region);

        self.running = true;

        Err(ConnectorError::NotAvailable(
            "Kinesis connector requires 'kinesis' feature. Enable with: cargo build --features kinesis"
                .to_string(),
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

/// Kinesis sink connector (stub implementation)
///
/// This stub is always available. For full functionality, enable the `kinesis` feature.
pub struct KinesisSink {
    name: String,
    config: KinesisConfig,
}

impl KinesisSink {
    /// Create a new Kinesis sink
    pub fn new(name: &str, config: KinesisConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[async_trait]
impl SinkConnector for KinesisSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        warn!(
            "Kinesis sink {} send (stub) to {}/{} - event: {}",
            self.name, self.config.region, self.config.stream_name, event.event_type
        );

        Err(ConnectorError::NotAvailable(
            "Kinesis connector requires 'kinesis' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Kinesis with AWS SDK feature enabled
// -----------------------------------------------------------------------------
#[cfg(feature = "kinesis")]
mod kinesis_impl {
    use super::*;
    use aws_config::BehaviorVersion;
    use aws_sdk_kinesis::primitives::Blob;
    use aws_sdk_kinesis::types::ShardIteratorType;
    use aws_sdk_kinesis::Client as KinesisClient;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    /// Full Kinesis source implementation with AWS SDK
    pub struct KinesisSourceImpl {
        name: String,
        config: KinesisConfig,
        running: Arc<AtomicBool>,
        client: Option<KinesisClient>,
    }

    impl KinesisSourceImpl {
        /// Create a new Kinesis source with AWS SDK
        pub async fn new(name: &str, config: KinesisConfig) -> Result<Self, ConnectorError> {
            let aws_config = if let Some(ref profile) = config.profile {
                aws_config::defaults(BehaviorVersion::latest())
                    .profile_name(profile)
                    .region(aws_config::Region::new(config.region.clone()))
                    .load()
                    .await
            } else {
                aws_config::defaults(BehaviorVersion::latest())
                    .region(aws_config::Region::new(config.region.clone()))
                    .load()
                    .await
            };

            let client = KinesisClient::new(&aws_config);

            Ok(Self {
                name: name.to_string(),
                config,
                running: Arc::new(AtomicBool::new(false)),
                client: Some(client),
            })
        }
    }

    #[async_trait]
    impl SourceConnector for KinesisSourceImpl {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            let client = self
                .client
                .take()
                .ok_or_else(|| ConnectorError::ConnectionFailed("Client already started".into()))?;

            self.running.store(true, Ordering::SeqCst);
            let running = self.running.clone();
            let config = self.config.clone();
            let name = self.name.clone();

            info!("Kinesis source {} starting on {}", name, config.stream_name);

            tokio::spawn(async move {
                if let Err(e) = consume_kinesis_stream(client, config, tx, running.clone()).await {
                    error!("Kinesis source {} error: {}", name, e);
                }
                running.store(false, Ordering::SeqCst);
            });

            Ok(())
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running.store(false, Ordering::SeqCst);
            info!("Kinesis source {} stopped", self.name);
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    async fn consume_kinesis_stream(
        client: KinesisClient,
        config: KinesisConfig,
        tx: mpsc::Sender<Event>,
        running: Arc<AtomicBool>,
    ) -> Result<(), ConnectorError> {
        // Describe stream to get shards
        let describe_result = client
            .describe_stream()
            .stream_name(&config.stream_name)
            .send()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let stream_desc = describe_result
            .stream_description()
            .ok_or_else(|| ConnectorError::ConnectionFailed("No stream description".into()))?;

        let shards = stream_desc.shards();

        // Get iterator type
        let iterator_type = match config.shard_iterator_type.as_str() {
            "TRIM_HORIZON" => ShardIteratorType::TrimHorizon,
            "AT_TIMESTAMP" => ShardIteratorType::AtTimestamp,
            _ => ShardIteratorType::Latest,
        };

        // Start consuming from each shard
        for shard in shards {
            let shard_id = shard.shard_id();

            // Get shard iterator
            let iterator_result = client
                .get_shard_iterator()
                .stream_name(&config.stream_name)
                .shard_id(shard_id)
                .shard_iterator_type(iterator_type.clone())
                .send()
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            let mut shard_iterator = iterator_result.shard_iterator().map(|s| s.to_string());

            while running.load(Ordering::SeqCst) {
                let Some(ref iterator) = shard_iterator else {
                    break;
                };

                let records_result = client
                    .get_records()
                    .shard_iterator(iterator)
                    .limit(config.batch_size)
                    .send()
                    .await
                    .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

                // Process records
                for record in records_result.records() {
                    let data = record.data();
                    let json_str = String::from_utf8_lossy(data.as_ref());

                    // Try to parse as JSON event
                    let event =
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                            json_to_event_from_json(&json)
                        } else {
                            // Create event with raw data
                            let mut event = Event::new("KinesisRecord");
                            event.data.insert(
                                "data".into(),
                                varpulis_core::Value::str(json_str.to_string()),
                            );
                            let partition_key = record.partition_key();
                            if !partition_key.is_empty() {
                                event.data.insert(
                                    "partition_key".into(),
                                    varpulis_core::Value::str(partition_key),
                                );
                            }
                            event
                        };

                    if tx.send(event).await.is_err() {
                        warn!("Channel closed, stopping Kinesis consumer");
                        return Ok(());
                    }
                }

                // Update iterator for next batch
                shard_iterator = records_result.next_shard_iterator().map(|s| s.to_string());

                // Rate limit polling
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
            }
        }

        Ok(())
    }

    /// Full Kinesis sink implementation with AWS SDK
    pub struct KinesisSinkImpl {
        name: String,
        config: KinesisConfig,
        client: Arc<Mutex<KinesisClient>>,
    }

    impl KinesisSinkImpl {
        /// Create a new Kinesis sink with AWS SDK
        pub async fn new(name: &str, config: KinesisConfig) -> Result<Self, ConnectorError> {
            let aws_config = if let Some(ref profile) = config.profile {
                aws_config::defaults(BehaviorVersion::latest())
                    .profile_name(profile)
                    .region(aws_config::Region::new(config.region.clone()))
                    .load()
                    .await
            } else {
                aws_config::defaults(BehaviorVersion::latest())
                    .region(aws_config::Region::new(config.region.clone()))
                    .load()
                    .await
            };

            let client = KinesisClient::new(&aws_config);

            Ok(Self {
                name: name.to_string(),
                config,
                client: Arc::new(Mutex::new(client)),
            })
        }
    }

    #[async_trait]
    impl SinkConnector for KinesisSinkImpl {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let client = self.client.lock().await;

            // Serialize event to JSON
            let data = serde_json::to_vec(event)
                .map_err(|e| ConnectorError::SendFailed(format!("Serialization error: {}", e)))?;

            // Generate or use configured partition key
            let partition_key = self
                .config
                .partition_key
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            // Put record
            client
                .put_record()
                .stream_name(&self.config.stream_name)
                .partition_key(&partition_key)
                .data(Blob::new(data))
                .send()
                .await
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            // Kinesis PutRecord is synchronous, no batching to flush
            Ok(())
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    /// Convert JSON to Event (helper for Kinesis)
    fn json_to_event_from_json(json: &serde_json::Value) -> Event {
        let event_type = json
            .get("event_type")
            .or_else(|| json.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("WebhookEvent")
            .to_string();

        let mut event = Event::new(event_type);

        // Handle nested "data" field or top-level fields
        let fields = json.get("data").or(Some(json));
        if let Some(obj) = fields.and_then(|v| v.as_object()) {
            for (key, value) in obj {
                if key != "event_type" && key != "type" {
                    if let Some(v) = crate::connector::helpers::json_to_value(value) {
                        event = event.with_field(key.as_str(), v);
                    }
                }
            }
        }

        event
    }
}

#[cfg(feature = "kinesis")]
pub use kinesis_impl::{
    KinesisSinkImpl as KinesisSinkFull, KinesisSourceImpl as KinesisSourceFull,
};
