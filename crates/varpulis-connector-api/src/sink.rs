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

    // =========================================================================
    // Two-Phase Commit (2PC) for exactly-once delivery
    // =========================================================================
    // These methods integrate sink delivery with the checkpoint lifecycle.
    // Default implementations are no-ops, so existing sinks compile unchanged.
    // Override these + `supports_exactly_once()` for exactly-once guarantees.

    /// Whether this sink supports exactly-once 2PC semantics.
    ///
    /// When `true`, the checkpoint coordinator will call `begin_epoch`,
    /// `prepare_commit`, `commit`, and `abort` at appropriate lifecycle points.
    fn supports_exactly_once(&self) -> bool {
        false
    }

    /// Begin a new epoch. Events sent after this call are part of the given
    /// checkpoint epoch. For Kafka: `begin_transaction()`.
    async fn begin_epoch(&self, _checkpoint_id: u64) -> Result<(), SinkError> {
        Ok(())
    }

    /// Pre-commit: finalize buffered data for the current epoch but don't make
    /// it visible to consumers. For Kafka: `flush()` (transaction still open).
    /// For S3: upload remaining buffer as final part (MPU not yet completed).
    async fn prepare_commit(&self, _checkpoint_id: u64) -> Result<(), SinkError> {
        Ok(())
    }

    /// Commit: make pre-committed data visible. Called only after the checkpoint
    /// is durably persisted. For Kafka: `commit_transaction()`.
    /// For S3: `complete_multipart_upload()`.
    async fn commit(&self, _checkpoint_id: u64) -> Result<(), SinkError> {
        Ok(())
    }

    /// Abort: discard uncommitted data for the current epoch. Called during
    /// recovery or when a checkpoint fails. For Kafka: `abort_transaction()`.
    /// For S3: `abort_multipart_upload()`.
    async fn abort(&self, _checkpoint_id: u64) -> Result<(), SinkError> {
        Ok(())
    }
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

    fn supports_exactly_once(&self) -> bool {
        // Check inner synchronously via try_lock — safe here since this is a
        // config query, not a hot-path operation.
        self.inner
            .try_lock()
            .map(|inner| inner.supports_exactly_once())
            .unwrap_or(false)
    }

    async fn begin_epoch(&self, checkpoint_id: u64) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        inner
            .begin_epoch(checkpoint_id)
            .await
            .map_err(SinkError::from)
    }

    async fn prepare_commit(&self, checkpoint_id: u64) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        inner
            .prepare_commit(checkpoint_id)
            .await
            .map_err(SinkError::from)
    }

    async fn commit(&self, checkpoint_id: u64) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        inner.commit(checkpoint_id).await.map_err(SinkError::from)
    }

    async fn abort(&self, checkpoint_id: u64) -> Result<(), SinkError> {
        let inner = self.inner.lock().await;
        inner.abort(checkpoint_id).await.map_err(SinkError::from)
    }
}
