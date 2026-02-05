//! Sink implementations for outputting processed events

use crate::event::Event;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use indexmap::IndexMap;
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, warn};

/// Trait for event sinks
#[async_trait]
pub trait Sink: Send + Sync {
    /// Name of this sink
    fn name(&self) -> &str;

    /// Establish connection to the external system.
    ///
    /// Called once after sink creation to establish any necessary connections.
    /// The default implementation is a no-op for sinks that connect eagerly.
    async fn connect(&self) -> Result<()> {
        Ok(())
    }

    /// Send an event to this sink
    async fn send(&self, event: &Event) -> Result<()>;

    /// Flush any buffered data
    async fn flush(&self) -> Result<()>;

    /// Close the sink
    async fn close(&self) -> Result<()>;
}

/// Console sink - prints to stdout
pub struct ConsoleSink {
    name: String,
    pretty: bool,
}

impl ConsoleSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pretty: true,
        }
    }

    pub fn compact(mut self) -> Self {
        self.pretty = false;
        self
    }
}

#[async_trait]
impl Sink for ConsoleSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<()> {
        if self.pretty {
            println!(
                "[{}] {} | {:?}",
                event.timestamp.format("%H:%M:%S"),
                event.event_type,
                event.data
            );
        } else {
            println!("{}", serde_json::to_string(event)?);
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// File sink - writes JSON lines to a file
pub struct FileSink {
    name: String,
    path: PathBuf,
    file: Arc<Mutex<File>>,
}

impl FileSink {
    /// Get the file path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            name: name.into(),
            path,
            file: Arc::new(Mutex::new(file)),
        })
    }
}

#[async_trait]
impl Sink for FileSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<()> {
        let json = serde_json::to_string(event)?;
        let mut file = self.file.lock().await;
        writeln!(file, "{}", json)?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut file = self.file.lock().await;
        file.flush()?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.flush().await
    }
}

/// Async file sink - writes JSON lines using tokio::fs (non-blocking)
///
/// Unlike `FileSink`, this implementation uses async I/O and does not block
/// the tokio runtime. It also includes buffering for better throughput.
///
/// # Example
/// ```ignore
/// let sink = AsyncFileSink::new("output", "/tmp/events.jsonl").await?;
/// sink.send(&event).await?;
/// sink.flush().await?;
/// ```
pub struct AsyncFileSink {
    name: String,
    path: PathBuf,
    file: Arc<Mutex<tokio::fs::File>>,
    buffer: Arc<Mutex<Vec<u8>>>,
    buffer_size: usize,
}

impl AsyncFileSink {
    /// Default buffer size (64KB)
    pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

    /// Get the file path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Create a new async file sink with default buffer size
    pub async fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Result<Self> {
        Self::with_buffer_size(name, path, Self::DEFAULT_BUFFER_SIZE).await
    }

    /// Create a new async file sink with custom buffer size
    pub async fn with_buffer_size(
        name: impl Into<String>,
        path: impl Into<PathBuf>,
        buffer_size: usize,
    ) -> Result<Self> {
        use tokio::fs::OpenOptions;

        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Self {
            name: name.into(),
            path,
            file: Arc::new(Mutex::new(file)),
            buffer: Arc::new(Mutex::new(Vec::with_capacity(buffer_size))),
            buffer_size,
        })
    }
}

#[async_trait]
impl Sink for AsyncFileSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<()> {
        let json = serde_json::to_string(event)?;

        let should_flush = {
            let mut buffer = self.buffer.lock().await;
            buffer.extend_from_slice(json.as_bytes());
            buffer.push(b'\n');
            buffer.len() >= self.buffer_size
        };

        if should_flush {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let data = {
            let mut buffer = self.buffer.lock().await;
            std::mem::take(&mut *buffer)
        };

        if !data.is_empty() {
            let mut file = self.file.lock().await;
            file.write_all(&data).await?;
            file.flush().await?;
        }

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.flush().await
    }
}

/// HTTP webhook sink
pub struct HttpSink {
    name: String,
    url: String,
    client: reqwest::Client,
    headers: IndexMap<String, String>,
}

impl HttpSink {
    pub fn new(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            client: reqwest::Client::new(),
            headers: IndexMap::new(),
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

#[async_trait]
impl Sink for HttpSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<()> {
        let mut req = self.client.post(&self.url);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/json");
        req = req.json(event);

        match req.send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    error!("HTTP sink {} got status {}", self.name, resp.status());
                }
            }
            Err(e) => {
                error!("HTTP sink {} error: {}", self.name, e);
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Configuration for HTTP sink retry behavior
#[derive(Debug, Clone)]
pub struct HttpRetryConfig {
    /// Maximum number of retry attempts (0 = no retries)
    pub max_retries: usize,
    /// Initial delay between retries (doubles each attempt)
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Request timeout
    pub timeout: Duration,
}

impl Default for HttpRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            timeout: Duration::from_secs(30),
        }
    }
}

/// HTTP webhook sink with retry logic
///
/// Unlike `HttpSink`, this implementation retries failed requests with exponential
/// backoff. It distinguishes between retryable errors (5xx, timeouts, network errors)
/// and non-retryable errors (4xx client errors).
///
/// # Example
/// ```ignore
/// let sink = HttpSinkWithRetry::new("webhook", "https://api.example.com/events")
///     .with_header("Authorization", "Bearer token123")
///     .with_retry_config(HttpRetryConfig {
///         max_retries: 5,
///         ..Default::default()
///     });
/// sink.send(&event).await?;
/// ```
pub struct HttpSinkWithRetry {
    name: String,
    url: String,
    client: reqwest::Client,
    headers: IndexMap<String, String>,
    retry_config: HttpRetryConfig,
}

impl HttpSinkWithRetry {
    pub fn new(name: impl Into<String>, url: impl Into<String>) -> Self {
        let config = HttpRetryConfig::default();
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            name: name.into(),
            url: url.into(),
            client,
            headers: IndexMap::new(),
            retry_config: config,
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn with_retry_config(mut self, config: HttpRetryConfig) -> Self {
        self.retry_config = config;
        // Rebuild client with new timeout
        self.client = reqwest::Client::builder()
            .timeout(self.retry_config.timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        self
    }

    /// Send with retry logic
    async fn send_with_retry(&self, body: Vec<u8>) -> Result<()> {
        let mut attempt = 0;
        let mut delay = self.retry_config.initial_delay;

        loop {
            let mut req = self.client.post(&self.url);
            for (k, v) in &self.headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req = req.header("Content-Type", "application/json");
            req = req.body(body.clone());

            match req.send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        return Ok(());
                    } else if resp.status().is_server_error() {
                        // 5xx: retryable
                        if attempt >= self.retry_config.max_retries {
                            return Err(anyhow!(
                                "HTTP sink {} failed with status {} after {} retries",
                                self.name,
                                resp.status(),
                                attempt
                            ));
                        }
                        warn!(
                            "HTTP sink {} got {}, retrying ({}/{})",
                            self.name,
                            resp.status(),
                            attempt + 1,
                            self.retry_config.max_retries
                        );
                    } else {
                        // 4xx: not retryable (client error)
                        return Err(anyhow!(
                            "HTTP sink {} got client error status {}",
                            self.name,
                            resp.status()
                        ));
                    }
                }
                Err(e) => {
                    // Network errors and timeouts are retryable
                    if e.is_timeout() || e.is_connect() || e.is_request() {
                        if attempt >= self.retry_config.max_retries {
                            return Err(anyhow!(
                                "HTTP sink {} failed with error {} after {} retries",
                                self.name,
                                e,
                                attempt
                            ));
                        }
                        warn!(
                            "HTTP sink {} error: {}, retrying ({}/{})",
                            self.name,
                            e,
                            attempt + 1,
                            self.retry_config.max_retries
                        );
                    } else {
                        // Other errors (e.g., serialization) are not retryable
                        return Err(e.into());
                    }
                }
            }

            // Exponential backoff
            attempt += 1;
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(self.retry_config.max_delay);
        }
    }
}

#[async_trait]
impl Sink for HttpSinkWithRetry {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<()> {
        let body = serde_json::to_vec(event)?;
        self.send_with_retry(body).await
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Multi-sink that broadcasts to multiple sinks
pub struct MultiSink {
    name: String,
    sinks: Vec<Box<dyn Sink>>,
}

impl MultiSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sinks: Vec::new(),
        }
    }

    pub fn with_sink(mut self, sink: Box<dyn Sink>) -> Self {
        self.sinks.push(sink);
        self
    }
}

#[async_trait]
impl Sink for MultiSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<()> {
        for sink in &self.sinks {
            if let Err(e) = sink.send(event).await {
                error!("Sink {} error: {}", sink.name(), e);
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        for sink in &self.sinks {
            sink.flush().await?;
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        for sink in &self.sinks {
            sink.close().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    // ==========================================================================
    // ConsoleSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_console_sink() {
        let sink = ConsoleSink::new("test");
        let event = Event::new("TestEvent").with_field("value", 42i64);
        assert!(sink.send(&event).await.is_ok());
    }

    #[tokio::test]
    async fn test_console_sink_name() {
        let sink = ConsoleSink::new("my_console");
        assert_eq!(sink.name(), "my_console");
    }

    #[tokio::test]
    async fn test_console_sink_compact() {
        let sink = ConsoleSink::new("test").compact();
        assert!(!sink.pretty);
        let event = Event::new("TestEvent").with_field("value", 42i64);
        assert!(sink.send(&event).await.is_ok());
    }

    #[tokio::test]
    async fn test_console_sink_flush_close() {
        let sink = ConsoleSink::new("test");
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());
    }

    // ==========================================================================
    // FileSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_file_sink() {
        let temp_file = NamedTempFile::new().unwrap();
        let sink = FileSink::new("test_file", temp_file.path()).unwrap();

        let event = Event::new("TestEvent").with_field("value", 42i64);
        assert!(sink.send(&event).await.is_ok());

        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());

        // Verify file contains the event
        let contents = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(contents.contains("TestEvent"));
    }

    #[tokio::test]
    async fn test_file_sink_name() {
        let temp_file = NamedTempFile::new().unwrap();
        let sink = FileSink::new("my_file", temp_file.path()).unwrap();
        assert_eq!(sink.name(), "my_file");
    }

    // ==========================================================================
    // HttpSink Tests (no actual network calls)
    // ==========================================================================

    #[test]
    fn test_http_sink_new() {
        let sink = HttpSink::new("http_test", "http://localhost:8080/webhook");
        assert_eq!(sink.name(), "http_test");
        assert_eq!(sink.url, "http://localhost:8080/webhook");
    }

    #[test]
    fn test_http_sink_with_header() {
        let sink = HttpSink::new("http_test", "http://localhost:8080")
            .with_header("Authorization", "Bearer token123")
            .with_header("X-Custom", "value");

        assert_eq!(sink.headers.len(), 2);
        assert_eq!(
            sink.headers.get("Authorization"),
            Some(&"Bearer token123".to_string())
        );
    }

    #[tokio::test]
    async fn test_http_sink_flush_close() {
        let sink = HttpSink::new("http_test", "http://localhost:8080");
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());
    }

    // ==========================================================================
    // MultiSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_multi_sink_empty() {
        let sink = MultiSink::new("multi");
        assert_eq!(sink.name(), "multi");

        let event = Event::new("Test");
        assert!(sink.send(&event).await.is_ok());
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_multi_sink_with_console() {
        let multi = MultiSink::new("multi")
            .with_sink(Box::new(ConsoleSink::new("console1")))
            .with_sink(Box::new(ConsoleSink::new("console2")));

        let event = Event::new("Test").with_field("x", 1i64);
        assert!(multi.send(&event).await.is_ok());

        assert!(multi.flush().await.is_ok());
        assert!(multi.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_multi_sink_with_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_sink = FileSink::new("file", temp_file.path()).unwrap();

        let multi = MultiSink::new("multi").with_sink(Box::new(file_sink));

        let event = Event::new("MultiEvent").with_field("val", 100i64);
        assert!(multi.send(&event).await.is_ok());
        assert!(multi.flush().await.is_ok());

        let contents = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(contents.contains("MultiEvent"));
    }

    // ==========================================================================
    // Additional FileSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_file_sink_path() {
        let temp_file = NamedTempFile::new().unwrap();
        let expected_path = temp_file.path().to_path_buf();
        let sink = FileSink::new("test", temp_file.path()).unwrap();

        assert_eq!(sink.path(), &expected_path);
    }

    #[tokio::test]
    async fn test_file_sink_multiple_events() {
        let temp_file = NamedTempFile::new().unwrap();
        let sink = FileSink::new("test", temp_file.path()).unwrap();

        // Write multiple events
        for i in 0..5 {
            let event = Event::new("Event").with_field("id", i as i64);
            sink.send(&event).await.unwrap();
        }
        sink.flush().await.unwrap();

        // Read and verify all events are written
        let contents = std::fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 5);
    }

    // ==========================================================================
    // Additional MultiSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_multi_sink_three_sinks() {
        let temp1 = NamedTempFile::new().unwrap();
        let temp2 = NamedTempFile::new().unwrap();

        let multi = MultiSink::new("triple")
            .with_sink(Box::new(ConsoleSink::new("console")))
            .with_sink(Box::new(FileSink::new("file1", temp1.path()).unwrap()))
            .with_sink(Box::new(FileSink::new("file2", temp2.path()).unwrap()));

        let event = Event::new("TripleEvent");
        multi.send(&event).await.unwrap();
        multi.flush().await.unwrap();
        multi.close().await.unwrap();

        // Verify both files got the event
        let contents1 = std::fs::read_to_string(temp1.path()).unwrap();
        let contents2 = std::fs::read_to_string(temp2.path()).unwrap();
        assert!(contents1.contains("TripleEvent"));
        assert!(contents2.contains("TripleEvent"));
    }

    // ==========================================================================
    // Error Handling Tests
    // ==========================================================================

    #[test]
    fn test_file_sink_invalid_path() {
        // Trying to create a file in a non-existent directory should fail
        let result = FileSink::new("test", "/nonexistent/path/file.json");
        assert!(result.is_err());
    }

    // ==========================================================================
    // AsyncFileSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_async_file_sink_basic() {
        let temp_file = NamedTempFile::new().unwrap();
        let sink = AsyncFileSink::new("test_async", temp_file.path())
            .await
            .unwrap();

        let event = Event::new("AsyncTestEvent").with_field("value", 123i64);
        assert!(sink.send(&event).await.is_ok());
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());

        // Verify file contains the event
        let contents = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(contents.contains("AsyncTestEvent"));
        assert!(contents.contains("123"));
    }

    #[tokio::test]
    async fn test_async_file_sink_name() {
        let temp_file = NamedTempFile::new().unwrap();
        let sink = AsyncFileSink::new("my_async_file", temp_file.path())
            .await
            .unwrap();
        assert_eq!(sink.name(), "my_async_file");
    }

    #[tokio::test]
    async fn test_async_file_sink_path() {
        let temp_file = NamedTempFile::new().unwrap();
        let expected_path = temp_file.path().to_path_buf();
        let sink = AsyncFileSink::new("test", temp_file.path()).await.unwrap();
        assert_eq!(sink.path(), &expected_path);
    }

    #[tokio::test]
    async fn test_async_file_sink_multiple_events() {
        let temp_file = NamedTempFile::new().unwrap();
        let sink = AsyncFileSink::new("test_async", temp_file.path())
            .await
            .unwrap();

        // Write multiple events
        for i in 0..5 {
            let event = Event::new("AsyncEvent").with_field("id", i as i64);
            sink.send(&event).await.unwrap();
        }
        sink.flush().await.unwrap();

        // Read and verify all events are written
        let contents = std::fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 5);
    }

    #[tokio::test]
    async fn test_async_file_sink_custom_buffer_size() {
        let temp_file = NamedTempFile::new().unwrap();
        // Use a small buffer to trigger auto-flush
        let sink = AsyncFileSink::with_buffer_size("test", temp_file.path(), 50)
            .await
            .unwrap();

        // Write events that should trigger buffer flush
        for i in 0..10 {
            let event = Event::new("BufferTest").with_field("id", i as i64);
            sink.send(&event).await.unwrap();
        }
        // Final flush to ensure all data is written
        sink.flush().await.unwrap();

        let contents = std::fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 10);
    }

    #[tokio::test]
    async fn test_async_file_sink_invalid_path() {
        let result = AsyncFileSink::new("test", "/nonexistent/path/file.json").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_async_file_sink_in_multi_sink() {
        let temp = NamedTempFile::new().unwrap();
        let async_sink = AsyncFileSink::new("async", temp.path()).await.unwrap();

        let multi = MultiSink::new("multi_with_async").with_sink(Box::new(async_sink));

        let event = Event::new("MultiAsyncEvent");
        multi.send(&event).await.unwrap();
        multi.flush().await.unwrap();

        let contents = std::fs::read_to_string(temp.path()).unwrap();
        assert!(contents.contains("MultiAsyncEvent"));
    }

    // ==========================================================================
    // HttpSinkWithRetry Tests
    // ==========================================================================

    #[test]
    fn test_http_retry_config_default() {
        let config = HttpRetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay, std::time::Duration::from_millis(100));
        assert_eq!(config.max_delay, std::time::Duration::from_secs(5));
        assert_eq!(config.timeout, std::time::Duration::from_secs(30));
    }

    #[test]
    fn test_http_sink_with_retry_creation() {
        let sink = HttpSinkWithRetry::new("retry_test", "http://localhost:8080/webhook");
        assert_eq!(sink.name(), "retry_test");
        assert_eq!(sink.url, "http://localhost:8080/webhook");
    }

    #[test]
    fn test_http_sink_with_retry_headers() {
        let sink = HttpSinkWithRetry::new("test", "http://localhost:8080")
            .with_header("Authorization", "Bearer token123")
            .with_header("X-Custom", "value");

        assert_eq!(sink.headers.len(), 2);
        assert_eq!(
            sink.headers.get("Authorization"),
            Some(&"Bearer token123".to_string())
        );
    }

    #[test]
    fn test_http_sink_with_retry_custom_config() {
        let config = HttpRetryConfig {
            max_retries: 5,
            initial_delay: std::time::Duration::from_millis(200),
            max_delay: std::time::Duration::from_secs(10),
            timeout: std::time::Duration::from_secs(60),
        };
        let sink =
            HttpSinkWithRetry::new("test", "http://localhost:8080").with_retry_config(config);

        assert_eq!(sink.retry_config.max_retries, 5);
        assert_eq!(
            sink.retry_config.initial_delay,
            std::time::Duration::from_millis(200)
        );
    }

    #[tokio::test]
    async fn test_http_sink_with_retry_flush_close() {
        let sink = HttpSinkWithRetry::new("test", "http://localhost:8080");
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());
    }
}
