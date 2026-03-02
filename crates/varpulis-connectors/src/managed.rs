//! Managed connector trait for unified connection management
//!
//! A `ManagedConnector` owns a single connection to an external system and
//! hands out shared source/sink handles through a uniform interface.
//!
//! Connectors now integrate with the actor framework via [`ConnectorObservableState`],
//! enabling health observation through the supervisor infrastructure.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::mpsc;
use varpulis_core::Event;

use super::types::ConnectorError;
use crate::sink::Sink;

/// Health report from a managed connector.
///
/// This type also serves as the `Actor::ObservableState` for connector actors,
/// enabling health monitoring through the actor framework's observation API.
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
        }
    }
}

/// A connector that manages a single shared connection.
///
/// First call to [`start_source`](Self::start_source) or
/// [`create_sink`](Self::create_sink) establishes the connection; subsequent
/// calls add subscriptions or create additional sink handles that share the
/// same underlying transport.
#[async_trait]
pub trait ManagedConnector: Send + Sync {
    /// Connector instance name (matches the VPL `connector` declaration).
    fn name(&self) -> &str;

    /// Connector type identifier (e.g. `"mqtt"`, `"kafka"`, `"console"`).
    fn connector_type(&self) -> &str;

    /// Start receiving events on `topic`, forwarding them to `tx`.
    ///
    /// The first call establishes the connection; subsequent calls add
    /// subscriptions on the existing connection.
    ///
    /// `params` contains extra per-stream parameters (e.g., `client_id`, `qos`).
    async fn start_source(
        &mut self,
        topic: &str,
        tx: mpsc::Sender<Event>,
        params: &HashMap<String, String>,
    ) -> Result<(), ConnectorError>;

    /// Create a sink that publishes to `topic` using the shared connection.
    ///
    /// If no source has been started yet, the connection is established lazily
    /// (supports sink-only connectors).
    ///
    /// `params` contains extra per-stream parameters (e.g., `client_id`, `qos`).
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
    ///
    /// Defaults to [`JsonConverter`](crate::converter::json::JsonConverter).
    fn converter(&self) -> Box<dyn crate::converter::Converter> {
        Box::new(crate::converter::json::JsonConverter)
    }

    /// Disconnect everything and release resources.
    async fn shutdown(&mut self) -> Result<(), ConnectorError>;
}
