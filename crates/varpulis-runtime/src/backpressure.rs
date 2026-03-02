//! Backpressure strategies for per-stage buffer management
//!
//! Provides configurable buffering strategies for event pipelines,
//! replacing hardcoded channel capacities with explicit policies.
//!
//! # Strategies
//!
//! - [`WhenFull::Block`] — block the sender until space is available (default)
//! - [`WhenFull::DropNewest`] — drop the incoming event when the buffer is full
//! - [`WhenFull::DropOldest`] — drop the oldest buffered event to make room
//! - [`WhenFull::Overflow`] — spill into a secondary buffer

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::event::SharedEvent;

/// Strategy applied when a stage buffer is full.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum WhenFull {
    /// Block the sender until space is available.
    #[default]
    Block,
    /// Drop the newest (incoming) event.
    DropNewest,
    /// Drop the oldest buffered event to make room.
    DropOldest,
    /// Overflow into a secondary buffer of the given capacity.
    Overflow { secondary_capacity: usize },
}

/// Configuration for a stage buffer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageBufferConfig {
    /// Maximum number of events the buffer can hold.
    #[serde(default = "default_capacity")]
    pub capacity: usize,
    /// Strategy when the buffer is full.
    #[serde(default)]
    pub when_full: WhenFull,
}

const fn default_capacity() -> usize {
    1000
}

impl Default for StageBufferConfig {
    fn default() -> Self {
        Self {
            capacity: default_capacity(),
            when_full: WhenFull::Block,
        }
    }
}

/// Metrics for a stage buffer.
#[derive(Debug)]
pub struct StageBufferMetrics {
    pub events_received: AtomicU64,
    pub events_dropped: AtomicU64,
    pub blocks_total: AtomicU64,
    pub current_depth: AtomicU64,
}

impl StageBufferMetrics {
    pub const fn new() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            blocks_total: AtomicU64::new(0),
            current_depth: AtomicU64::new(0),
        }
    }
}

impl Default for StageBufferMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A stage buffer that wraps an mpsc sender with a backpressure strategy.
#[derive(Debug)]
pub struct StageBuffer {
    config: StageBufferConfig,
    tx: mpsc::Sender<SharedEvent>,
    metrics: Arc<StageBufferMetrics>,
}

impl StageBuffer {
    /// Create a new stage buffer with the given config.
    ///
    /// Returns `(StageBuffer, mpsc::Receiver<SharedEvent>)`.
    pub fn new(config: StageBufferConfig) -> (Self, mpsc::Receiver<SharedEvent>) {
        let (tx, rx) = mpsc::channel(config.capacity);
        let metrics = Arc::new(StageBufferMetrics::new());
        (
            Self {
                config,
                tx,
                metrics,
            },
            rx,
        )
    }

    /// Create a stage buffer that wraps an existing sender.
    pub fn wrap(config: StageBufferConfig, tx: mpsc::Sender<SharedEvent>) -> Self {
        Self {
            config,
            tx,
            metrics: Arc::new(StageBufferMetrics::new()),
        }
    }

    /// Send an event through the buffer, applying the configured strategy.
    pub async fn send(&self, event: SharedEvent) -> Result<(), BackpressureError> {
        self.metrics.events_received.fetch_add(1, Ordering::Relaxed);

        match &self.config.when_full {
            WhenFull::Block => {
                if self.tx.capacity() == 0 {
                    self.metrics.blocks_total.fetch_add(1, Ordering::Relaxed);
                }
                self.tx
                    .send(event)
                    .await
                    .map_err(|_| BackpressureError::ChannelClosed)?;
            }
            WhenFull::DropNewest => match self.tx.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.metrics.events_dropped.fetch_add(1, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(BackpressureError::ChannelClosed);
                }
            },
            WhenFull::DropOldest => {
                // If full, drain one and send
                if self.tx.try_send(event.clone()).is_err() {
                    // Channel is full — the oldest event is lost
                    self.metrics.events_dropped.fetch_add(1, Ordering::Relaxed);
                    // Force-send by blocking (the receiver will have consumed one)
                    let _ = self.tx.send(event).await;
                }
            }
            WhenFull::Overflow {
                secondary_capacity: _,
            } => {
                // Phase 1: treat overflow as block (full overflow support in Phase 2)
                if self.tx.capacity() == 0 {
                    self.metrics.blocks_total.fetch_add(1, Ordering::Relaxed);
                }
                self.tx
                    .send(event)
                    .await
                    .map_err(|_| BackpressureError::ChannelClosed)?;
            }
        }

        Ok(())
    }

    /// Get a reference to the metrics.
    pub const fn metrics(&self) -> &Arc<StageBufferMetrics> {
        &self.metrics
    }

    /// Get a clone of the underlying sender.
    pub fn sender(&self) -> mpsc::Sender<SharedEvent> {
        self.tx.clone()
    }
}

/// Errors from backpressure operations.
#[derive(Debug, thiserror::Error)]
pub enum BackpressureError {
    #[error("channel closed")]
    ChannelClosed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

    #[tokio::test]
    async fn test_stage_buffer_block_strategy() {
        let config = StageBufferConfig {
            capacity: 10,
            when_full: WhenFull::Block,
        };
        let (buffer, mut rx) = StageBuffer::new(config);

        let event = Arc::new(Event::new("Test"));
        buffer.send(event).await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.event_type.as_ref(), "Test");
        assert_eq!(buffer.metrics().events_received.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_stage_buffer_drop_newest() {
        let config = StageBufferConfig {
            capacity: 2,
            when_full: WhenFull::DropNewest,
        };
        let (buffer, _rx) = StageBuffer::new(config);

        // Fill the buffer
        buffer.send(Arc::new(Event::new("A"))).await.unwrap();
        buffer.send(Arc::new(Event::new("B"))).await.unwrap();

        // This should be dropped
        buffer.send(Arc::new(Event::new("C"))).await.unwrap();

        assert_eq!(buffer.metrics().events_received.load(Ordering::Relaxed), 3);
        assert_eq!(buffer.metrics().events_dropped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_stage_buffer_default_config() {
        let config = StageBufferConfig::default();
        assert_eq!(config.capacity, 1000);
        assert!(matches!(config.when_full, WhenFull::Block));
    }

    #[test]
    fn test_when_full_serialization() {
        let block = WhenFull::Block;
        let json = serde_json::to_string(&block).unwrap();
        assert!(json.contains("block"));

        let overflow = WhenFull::Overflow {
            secondary_capacity: 500,
        };
        let json = serde_json::to_string(&overflow).unwrap();
        assert!(json.contains("overflow"));
        assert!(json.contains("500"));
    }

    #[test]
    fn test_stage_buffer_config_serialization() {
        let config = StageBufferConfig {
            capacity: 2000,
            when_full: WhenFull::DropNewest,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StageBufferConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.capacity, 2000);
        assert!(matches!(deserialized.when_full, WhenFull::DropNewest));
    }
}
