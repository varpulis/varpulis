//! AWS Kinesis source and sink connectors.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::ShardIteratorType;
use aws_sdk_kinesis::Client as KinesisClient;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};
use varpulis_connector_api::helpers::json_to_value;
use varpulis_connector_api::{
    ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory, SinkConnector,
    SourceConnector,
};
use varpulis_core::Event;

static KINESIS_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "kinesis",
    display_name: "AWS Kinesis",
    description: "Amazon Kinesis Data Streams connector",
    feature_flag: "kinesis",
    supports_source: true,
    supports_sink: true,
    supports_managed: false,
    config_params: &[],
};

struct KinesisFactory;

impl ConnectorFactory for KinesisFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &KINESIS_INFO
    }

    fn create_sink_connector(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        Err(ConnectorError::ConfigError(
            "Use async create_from_config for kinesis".to_string(),
        ))
    }
}

inventory::submit! { &KinesisFactory as &dyn ConnectorFactory }

/// AWS Kinesis configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
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
    /// AWS profile name (optional)
    pub profile: Option<String>,
}

impl KinesisConfig {
    /// Create a new Kinesis configuration
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
    pub const fn with_poll_interval(mut self, ms: u64) -> Self {
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

/// Full Kinesis source implementation with AWS SDK
pub struct KinesisSource {
    name: String,
    config: KinesisConfig,
    running: Arc<AtomicBool>,
    client: Option<KinesisClient>,
}

impl std::fmt::Debug for KinesisSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisSource")
            .field("name", &self.name)
            .field("running", &self.running)
            .finish_non_exhaustive()
    }
}

impl KinesisSource {
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
impl SourceConnector for KinesisSource {
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

    let iterator_type = match config.shard_iterator_type.as_str() {
        "TRIM_HORIZON" => ShardIteratorType::TrimHorizon,
        "AT_TIMESTAMP" => ShardIteratorType::AtTimestamp,
        _ => ShardIteratorType::Latest,
    };

    for shard in shards {
        let shard_id = shard.shard_id();

        let iterator_result = client
            .get_shard_iterator()
            .stream_name(&config.stream_name)
            .shard_id(shard_id)
            .shard_iterator_type(iterator_type.clone())
            .send()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let mut shard_iterator = iterator_result.shard_iterator().map(String::from);

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

            for record in records_result.records() {
                let data = record.data();
                let json_str = String::from_utf8_lossy(data.as_ref());

                let event = if let Ok(json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                    json_to_event_from_json(&json)
                } else {
                    let mut event = Event::new("KinesisRecord");
                    event
                        .data
                        .insert("data".into(), varpulis_core::Value::str(&json_str));
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

            shard_iterator = records_result.next_shard_iterator().map(String::from);
            tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
        }
    }

    Ok(())
}

/// Full Kinesis sink implementation with AWS SDK
pub struct KinesisSink {
    name: String,
    config: KinesisConfig,
    client: Arc<Mutex<KinesisClient>>,
}

impl std::fmt::Debug for KinesisSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisSink")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl KinesisSink {
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
impl SinkConnector for KinesisSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let client = self.client.lock().await;

        let data = serde_json::to_vec(event)
            .map_err(|e| ConnectorError::SendFailed(format!("Serialization error: {}", e)))?;

        let partition_key = self
            .config
            .partition_key
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

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

    let fields = json.get("data").or(Some(json));
    if let Some(obj) = fields.and_then(|v| v.as_object()) {
        for (key, value) in obj {
            if key != "event_type" && key != "type" {
                if let Some(v) = json_to_value(value) {
                    event = event.with_field(key.as_str(), v);
                }
            }
        }
    }

    event
}
