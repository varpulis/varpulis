//! Sink implementations for outputting processed events

use crate::engine::Alert;
use crate::event::Event;
use anyhow::Result;
use async_trait::async_trait;
use indexmap::IndexMap;
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;

/// Trait for event sinks
#[async_trait]
pub trait Sink: Send + Sync {
    /// Name of this sink
    fn name(&self) -> &str;

    /// Send an event to this sink
    async fn send(&self, event: &Event) -> Result<()>;

    /// Send an alert to this sink
    async fn send_alert(&self, alert: &Alert) -> Result<()>;

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

    async fn send_alert(&self, alert: &Alert) -> Result<()> {
        println!(
            "🚨 ALERT [{}] {}: {}",
            alert.severity, alert.alert_type, alert.message
        );
        for (k, v) in &alert.data {
            println!("   {}: {}", k, v);
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
#[allow(dead_code)]
pub struct FileSink {
    name: String,
    path: PathBuf,
    file: Arc<Mutex<File>>,
}

impl FileSink {
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

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

    async fn send_alert(&self, alert: &Alert) -> Result<()> {
        let json = serde_json::to_string(alert)?;
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

    async fn send_alert(&self, alert: &Alert) -> Result<()> {
        let mut req = self.client.post(&self.url);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/json");
        req = req.json(alert);

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

    pub fn add(mut self, sink: Box<dyn Sink>) -> Self {
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

    async fn send_alert(&self, alert: &Alert) -> Result<()> {
        for sink in &self.sinks {
            if let Err(e) = sink.send_alert(alert).await {
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
    use indexmap::IndexMap;
    use varpulis_core::Value;

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
    async fn test_console_sink_alert() {
        let sink = ConsoleSink::new("test");
        let alert = Alert {
            alert_type: "test_alert".to_string(),
            severity: "warning".to_string(),
            message: "Test message".to_string(),
            data: IndexMap::new(),
        };
        assert!(sink.send_alert(&alert).await.is_ok());
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

    #[tokio::test]
    async fn test_file_sink_alert() {
        let temp_file = NamedTempFile::new().unwrap();
        let sink = FileSink::new("test_file", temp_file.path()).unwrap();
        
        let mut data = IndexMap::new();
        data.insert("key".to_string(), Value::Str("value".to_string()));
        
        let alert = Alert {
            alert_type: "test_alert".to_string(),
            severity: "critical".to_string(),
            message: "Critical issue".to_string(),
            data,
        };
        assert!(sink.send_alert(&alert).await.is_ok());
        assert!(sink.flush().await.is_ok());
        
        let contents = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(contents.contains("test_alert"));
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
        assert_eq!(sink.headers.get("Authorization"), Some(&"Bearer token123".to_string()));
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
            .add(Box::new(ConsoleSink::new("console1")))
            .add(Box::new(ConsoleSink::new("console2")));
        
        let event = Event::new("Test").with_field("x", 1i64);
        assert!(multi.send(&event).await.is_ok());
        
        let alert = Alert {
            alert_type: "test".to_string(),
            severity: "info".to_string(),
            message: "msg".to_string(),
            data: IndexMap::new(),
        };
        assert!(multi.send_alert(&alert).await.is_ok());
        
        assert!(multi.flush().await.is_ok());
        assert!(multi.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_multi_sink_with_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_sink = FileSink::new("file", temp_file.path()).unwrap();
        
        let multi = MultiSink::new("multi")
            .add(Box::new(file_sink));
        
        let event = Event::new("MultiEvent").with_field("val", 100i64);
        assert!(multi.send(&event).await.is_ok());
        assert!(multi.flush().await.is_ok());
        
        let contents = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(contents.contains("MultiEvent"));
    }
}
