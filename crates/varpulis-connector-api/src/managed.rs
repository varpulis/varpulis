//! Managed connector trait for unified connection management
//!
//! A `ManagedConnector` owns a single connection to an external system and
//! hands out shared source/sink handles through a uniform interface.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::mpsc;
use varpulis_core::Event;

use crate::sink::Sink;
use crate::types::{ConnectorError, EngineOffsetRegistry};

/// Health report from a managed connector.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectorHealthReport {
    /// Whether the connector is currently connected.
    pub connected: bool,
    /// Last error message, if any.
    pub last_error: Option<String>,
    /// Total number of messages received since start.
    pub messages_received: u64,
    /// Seconds elapsed since the last message was received.
    pub seconds_since_last_message: u64,
    /// Current circuit breaker state (`"closed"`, `"open"`, or `"half_open"`).
    pub circuit_breaker_state: String,
    /// Total number of circuit breaker failures recorded.
    pub circuit_breaker_failures: u64,
    /// Total number of requests rejected by the circuit breaker.
    pub circuit_breaker_rejections: u64,
    /// Total number of inbound records dropped because they could not be
    /// decoded into an `Event` (malformed payload) or exceeded the payload
    /// size limit. A non-zero value means poison records were observed (and
    /// dead-lettered, when a DLQ is attached) rather than silently skipped.
    pub decode_failures: u64,
}

impl Default for ConnectorHealthReport {
    fn default() -> Self {
        Self {
            connected: true,
            last_error: None,
            messages_received: 0,
            seconds_since_last_message: 0,
            circuit_breaker_state: "closed".to_string(),
            circuit_breaker_failures: 0,
            circuit_breaker_rejections: 0,
            decode_failures: 0,
        }
    }
}

/// A connector that manages a single shared connection.
#[async_trait]
pub trait ManagedConnector: Send + Sync {
    /// Connector instance name (matches the VPL `connector` declaration).
    fn name(&self) -> &str;

    /// Connector type identifier (e.g. `"mqtt"`, `"kafka"`, `"console"`).
    fn connector_type(&self) -> &str;

    /// Start receiving events on `topic`, forwarding them to `tx` as **batches**.
    ///
    /// Implementations should accumulate events and send them in `Vec<Event>`
    /// batches (typically up to ~256 events or 5ms, whichever comes first).
    /// Batching here amortizes the per-event async wake-up cost between the
    /// connector task and the run-loop, which is the dominant overhead at
    /// high event rates. Even single-event sources should wrap each event in
    /// `vec![event]` so the run-loop only deals with batches.
    async fn start_source(
        &mut self,
        topic: &str,
        tx: mpsc::Sender<Vec<Event>>,
        params: &HashMap<String, String>,
    ) -> Result<(), ConnectorError>;

    /// Create a sink that publishes to `topic` using the shared connection.
    fn create_sink(
        &mut self,
        topic: &str,
        params: &HashMap<String, String>,
    ) -> Result<Arc<dyn Sink>, ConnectorError>;

    /// Report the health of this connector.
    fn health(&self) -> ConnectorHealthReport {
        ConnectorHealthReport::default()
    }

    /// Return the converter used for (de)serializing events.
    fn converter(&self) -> Box<dyn crate::converter::Converter> {
        Box::new(crate::converter::json::JsonConverter)
    }

    /// Disconnect everything and release resources.
    async fn shutdown(&mut self) -> Result<(), ConnectorError>;

    // ---- Checkpoint-aligned source offset tracking ----

    /// Bind this connector to an engine-wide offset registry. Replayable
    /// sources (Kafka) mirror their consumed offsets into the registry so
    /// the engine can snapshot them at checkpoint time. Non-replayable
    /// connectors (MQTT, NATS) ignore this.
    fn set_engine_offsets_registry(&mut self, _registry: EngineOffsetRegistry) {}

    /// Bind this connector to the engine's cooperative source-pause flag.
    /// Replayable sources check it before each upstream poll and stop pulling
    /// while it is set, so the checkpoint barrier can drain in-flight events and
    /// snapshot a coherent (applied == committed) offset set. Non-replayable
    /// connectors ignore it.
    fn set_source_pause_handle(&mut self, _handle: std::sync::Arc<std::sync::atomic::AtomicBool>) {}

    /// Commit the given per-partition offsets back to the external system
    /// (e.g. Kafka consumer group coordinator) as part of a 2PC checkpoint
    /// commit. Default no-op for connectors without replayable offsets.
    async fn commit_source_offsets(
        &self,
        _topic: &str,
        _offsets: &HashMap<i32, i64>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Stage per-partition consumer offsets to be committed *inside* the sink's
    /// transaction (audit C4 — so offset advance and output visibility commit
    /// atomically). Called by the barrier between prepare and commit for
    /// exactly-once sinks. Default no-op for connectors without transactional
    /// offset support.
    async fn stage_txn_offsets(
        &self,
        _topic: &str,
        _offsets: &HashMap<i32, i64>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
}
