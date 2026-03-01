//! Console connector for testing and debugging

use super::component::{ConnectorComponentInfo, ConnectorFactory};
use super::types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;
use varpulis_core::Event;

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

static CONSOLE_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "console",
    display_name: "Console",
    description: "Debug connector that reads from stdin and writes to stdout",
    feature_flag: "",
    supports_source: true,
    supports_sink: true,
    supports_managed: false,
    config_params: &[],
};

struct ConsoleFactory;

impl ConnectorFactory for ConsoleFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &CONSOLE_INFO
    }

    fn create_sink_connector(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        Ok(Box::new(ConsoleSink::new("console")))
    }

    fn create_engine_sink(
        &self,
        name: &str,
        _config: &ConnectorConfig,
        _topic_override: Option<&str>,
        _context_name: Option<&str>,
    ) -> Result<Arc<dyn crate::sink::Sink>, ConnectorError> {
        Ok(Arc::new(crate::sink::SinkConnectorAdapter::new(
            name,
            Box::new(ConsoleSink::new(name)),
        )))
    }
}

inventory::submit! { &ConsoleFactory as &dyn ConnectorFactory }

/// Console source - reads events from stdin (for testing)
pub struct ConsoleSource {
    name: String,
    running: bool,
}

impl ConsoleSource {
    /// Create a new console source with the given name.
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
    /// Create a new console sink with the given name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            pretty: true,
        }
    }

    /// Switch to compact (single-line JSON) output format.
    pub const fn compact(mut self) -> Self {
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
