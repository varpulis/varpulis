//! Managed connector trait for unified connection management
//!
//! A `ManagedConnector` owns a single connection to an external system and
//! hands out shared source/sink handles through a uniform interface.

use super::types::ConnectorError;
use crate::event::Event;
use crate::sink::Sink;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

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

    /// Disconnect everything and release resources.
    async fn shutdown(&mut self) -> Result<(), ConnectorError>;
}
