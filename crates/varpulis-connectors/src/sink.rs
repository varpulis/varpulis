//! Sink trait and error types for outputting processed events
//!
//! This module defines the core `Sink` trait that all event output destinations
//! must implement, along with the `SinkError` error type.

use std::sync::Arc;

use async_trait::async_trait;
use varpulis_core::Event;

use crate::types::ConnectorError;

/// Errors produced by sink operations.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    /// I/O error (file writes, network, etc.)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error (JSON encoding)
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// HTTP request error
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Connector-level error
    #[error("connector error: {0}")]
    Connector(#[from] ConnectorError),

    /// Generic error with message
    #[error("{0}")]
    Other(String),
}

impl SinkError {
    /// Create a generic error from a displayable value.
    pub fn other(msg: impl std::fmt::Display) -> Self {
        Self::Other(msg.to_string())
    }
}

/// Trait for event sinks
#[async_trait]
pub trait Sink: Send + Sync {
    /// Name of this sink
    fn name(&self) -> &str;

    /// Establish connection to the external system.
    ///
    /// Called once after sink creation to establish any necessary connections.
    /// The default implementation is a no-op for sinks that connect eagerly.
    async fn connect(&self) -> Result<(), SinkError> {
        Ok(())
    }

    /// Send an event to this sink
    async fn send(&self, event: &Event) -> Result<(), SinkError>;

    /// Send a batch of events to this sink.
    ///
    /// Default implementation calls `send()` for each event.
    /// Connectors should override this to amortize lock/syscall overhead.
    async fn send_batch(&self, events: &[Arc<Event>]) -> Result<(), SinkError> {
        for event in events {
            self.send(event).await?;
        }
        Ok(())
    }

    /// Flush any buffered data
    async fn flush(&self) -> Result<(), SinkError>;

    /// Close the sink
    async fn close(&self) -> Result<(), SinkError>;
}

/// Adapter: wraps a SinkConnector as a Sink for use in the sink registry.
pub struct SinkConnectorAdapter {
    name: String,
    inner: tokio::sync::Mutex<Box<dyn crate::types::SinkConnector>>,
}

impl std::fmt::Debug for SinkConnectorAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkConnectorAdapter")
            .finish_non_exhaustive()
    }
}

impl SinkConnectorAdapter {
    /// Create a new adapter wrapping a SinkConnector.
    pub fn new(name: &str, connector: Box<dyn crate::types::SinkConnector>) -> Self {
        Self {
            name: name.to_string(),
            inner: tokio::sync::Mutex::new(connector),
        }
    }
}

#[async_trait]
impl Sink for SinkConnectorAdapter {
    fn name(&self) -> &str {
        &self.name
    }
    async fn connect(&self) -> Result<(), SinkError> {
        let mut inner = self.inner.lock().await;
        inner.connect().await.map_err(SinkError::from)
    }
    async fn send(&self, event: &Event) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        inner.send(event).await.map_err(SinkError::from)
    }
    async fn send_batch(&self, events: &[Arc<Event>]) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        for event in events {
            inner.send(event).await.map_err(SinkError::from)?;
        }
        Ok(())
    }
    async fn flush(&self) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        inner.flush().await.map_err(SinkError::from)
    }
    async fn close(&self) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        inner.close().await.map_err(SinkError::from)
    }
}
