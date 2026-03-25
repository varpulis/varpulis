//! AWS S3 sink connector.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use tokio::sync::Mutex;
use tracing::info;
use varpulis_connector_api::{
    ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory, SinkConnector,
};
use varpulis_core::Event;

static S3_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "s3",
    display_name: "AWS S3",
    description: "Amazon S3 object storage sink",
    feature_flag: "s3",
    supports_source: false,
    supports_sink: true,
    supports_managed: false,
    config_params: &[],
};

struct S3Factory;

impl ConnectorFactory for S3Factory {
    fn info(&self) -> &ConnectorComponentInfo {
        &S3_INFO
    }

    fn create_sink_connector(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        Err(ConnectorError::ConfigError(
            "Use async create_from_config for s3".to_string(),
        ))
    }
}

inventory::submit! { &S3Factory as &dyn ConnectorFactory }

/// S3 output format
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub enum S3OutputFormat {
    /// JSON Lines (one JSON object per line)
    #[default]
    JsonLines,
    /// CSV with header row
    Csv,
}

/// S3 sink configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// Key prefix (folder path)
    pub prefix: String,
    /// AWS region
    pub region: String,
    /// Output format
    pub format: S3OutputFormat,
    /// File rotation size in bytes (default 100MB)
    pub file_rotation_size: usize,
    /// File rotation time in seconds (default 3600 = 1 hour)
    pub file_rotation_seconds: u64,
    /// Compression (gzip for JSON/CSV)
    pub compression: bool,
    /// AWS profile name (optional)
    pub profile: Option<String>,
}

impl S3Config {
    /// Create a new S3 configuration
    pub fn new(bucket: &str, prefix: &str, region: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: prefix.trim_end_matches('/').to_string(),
            region: region.to_string(),
            format: S3OutputFormat::JsonLines,
            file_rotation_size: 100 * 1024 * 1024,
            file_rotation_seconds: 3600,
            compression: false,
            profile: None,
        }
    }

    /// Set the output format
    pub const fn with_format(mut self, format: S3OutputFormat) -> Self {
        self.format = format;
        self
    }

    /// Set file rotation size in bytes
    pub const fn with_file_rotation_size(mut self, bytes: usize) -> Self {
        self.file_rotation_size = bytes;
        self
    }

    /// Set file rotation time in seconds
    pub const fn with_file_rotation_seconds(mut self, seconds: u64) -> Self {
        self.file_rotation_seconds = seconds;
        self
    }

    /// Enable gzip compression
    pub const fn with_compression(mut self) -> Self {
        self.compression = true;
        self
    }

    /// Set AWS profile name
    pub fn with_profile(mut self, profile: &str) -> Self {
        self.profile = Some(profile.to_string());
        self
    }
}

struct BufferState {
    buffer: Vec<u8>,
    event_count: usize,
    start_time: Instant,
    file_number: u64,
}

impl BufferState {
    fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024 * 1024),
            event_count: 0,
            start_time: Instant::now(),
            file_number: 0,
        }
    }
}

/// Full S3 sink implementation with AWS SDK
pub struct S3Sink {
    name: String,
    config: S3Config,
    client: S3Client,
    state: Arc<Mutex<BufferState>>,
}

impl std::fmt::Debug for S3Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Sink")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl S3Sink {
    /// Create a new S3 sink with AWS SDK
    pub async fn new(name: &str, config: S3Config) -> Result<Self, ConnectorError> {
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

        let client = S3Client::new(&aws_config);

        Ok(Self {
            name: name.to_string(),
            config,
            client,
            state: Arc::new(Mutex::new(BufferState::new())),
        })
    }

    fn generate_key(&self, state: &BufferState) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let extension = match self.config.format {
            S3OutputFormat::JsonLines => {
                if self.config.compression {
                    "jsonl.gz"
                } else {
                    "jsonl"
                }
            }
            S3OutputFormat::Csv => {
                if self.config.compression {
                    "csv.gz"
                } else {
                    "csv"
                }
            }
        };

        format!(
            "{}/{}_{:08}_{}.{}",
            self.config.prefix, timestamp, state.file_number, self.name, extension
        )
    }

    async fn should_rotate(&self, state: &BufferState) -> bool {
        state.buffer.len() >= self.config.file_rotation_size
            || state.start_time.elapsed() >= Duration::from_secs(self.config.file_rotation_seconds)
    }

    async fn upload_buffer(&self, state: &mut BufferState) -> Result<(), ConnectorError> {
        if state.buffer.is_empty() {
            return Ok(());
        }

        let key = self.generate_key(state);
        let data = std::mem::take(&mut state.buffer);

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        info!(
            "S3 sink {} uploaded {} events to s3://{}/{}",
            self.name, state.event_count, self.config.bucket, key
        );

        state.event_count = 0;
        state.start_time = Instant::now();
        state.file_number += 1;

        Ok(())
    }
}

#[async_trait]
impl SinkConnector for S3Sink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let mut state = self.state.lock().await;

        match self.config.format {
            S3OutputFormat::JsonLines => {
                let buf = event.to_sink_payload();
                state.buffer.extend_from_slice(&buf);
                state.buffer.push(b'\n');
            }
            S3OutputFormat::Csv => {
                if state.event_count == 0 {
                    let header: Vec<&str> = event.data.keys().map(|s| &**s).collect();
                    state.buffer.extend_from_slice(header.join(",").as_bytes());
                    state.buffer.push(b'\n');
                }
                let values: Vec<String> = event
                    .data
                    .values()
                    .map(|v| match v {
                        varpulis_core::Value::Str(s) => {
                            format!("\"{}\"", s.replace('"', "\"\""))
                        }
                        _ => v.to_string(),
                    })
                    .collect();
                state.buffer.extend_from_slice(values.join(",").as_bytes());
                state.buffer.push(b'\n');
            }
        }

        state.event_count += 1;

        if self.should_rotate(&state).await {
            self.upload_buffer(&mut state).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        let mut state = self.state.lock().await;
        self.upload_buffer(&mut state).await
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        self.flush().await
    }
}
