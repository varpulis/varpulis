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
use tracing::{debug, error, info};
use varpulis_core::Value;

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

    #[tokio::test]
    async fn test_console_sink() {
        let sink = ConsoleSink::new("test");
        let event = Event::new("TestEvent").with_field("value", 42i64);
        assert!(sink.send(&event).await.is_ok());
    }
}
