//! Sink trait and error types for outputting processed events

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
    async fn connect(&self) -> Result<(), SinkError> {
        Ok(())
    }

    /// Send an event to this sink
    async fn send(&self, event: &Event) -> Result<(), SinkError>;

    /// Send a batch of events to this sink.
    async fn send_batch(&self, events: &[Arc<Event>]) -> Result<(), SinkError> {
        for event in events {
            self.send(event).await?;
        }
        Ok(())
    }

    /// Send a batch of events to a specific topic (for dynamic routing).
    async fn send_batch_to_topic(
        &self,
        events: &[Arc<Event>],
        _topic: &str,
    ) -> Result<(), SinkError> {
        self.send_batch(events).await
    }

    /// Flush any buffered data
    async fn flush(&self) -> Result<(), SinkError>;

    /// Close the sink
    async fn close(&self) -> Result<(), SinkError>;
}

/// Adapter: wraps a `SinkConnector` as a `Sink` for use in the sink registry.
pub struct SinkConnectorAdapter {
    /// The name of this adapter.
    pub name: String,
    /// The inner sink connector.
    pub inner: tokio::sync::Mutex<Box<dyn crate::types::SinkConnector>>,
}

impl std::fmt::Debug for SinkConnectorAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkConnectorAdapter")
            .finish_non_exhaustive()
    }
}

impl SinkConnectorAdapter {
    /// Create a new adapter wrapping a `SinkConnector`.
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
    async fn send_batch_to_topic(
        &self,
        events: &[Arc<Event>],
        topic: &str,
    ) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        inner
            .send_to_topic(events, topic)
            .await
            .map_err(SinkError::from)
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
