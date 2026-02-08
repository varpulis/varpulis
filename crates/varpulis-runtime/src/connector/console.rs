//! Console connector for testing and debugging

use super::types::{ConnectorError, SinkConnector, SourceConnector};
use crate::event::Event;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::info;

/// Console source - reads events from stdin (for testing)
pub struct ConsoleSource {
    name: String,
    running: bool,
}

impl ConsoleSource {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            running: false,
        }
    }
}

#[async_trait]
impl SourceConnector for ConsoleSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        self.running = true;
        info!("Console source started: {}", self.name);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running = false;
        info!("Console source stopped: {}", self.name);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }
}

/// Console sink - writes events to stdout
pub struct ConsoleSink {
    name: String,
    pretty: bool,
}

impl ConsoleSink {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            pretty: true,
        }
    }

    pub fn compact(mut self) -> Self {
        self.pretty = false;
        self
    }
}

#[async_trait]
impl SinkConnector for ConsoleSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        if self.pretty {
            println!(
                "[{}] {} | {:?}",
                event.timestamp.format("%H:%M:%S"),
                event.event_type,
                event.data
            );
        } else {
            println!("{}", serde_json::to_string(event).unwrap_or_default());
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
