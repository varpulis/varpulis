//! Managed connector registry -- owns one connection per declared connector
//!
//! The registry wraps each connector with the actor framework's supervision
//! infrastructure, providing automatic health observation and restart policies.

use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use tracing::{info, warn};
use varpulis_core::Event;

use super::managed::{ConnectorHealthReport, ManagedConnector};
use super::types::{ConnectorConfig, ConnectorError, EngineOffsetRegistry};
use crate::sink::Sink;

/// Registry that owns one [`ManagedConnector`] per declared connector name.
///
/// Build via [`from_configs`](Self::from_configs), then call
/// [`start_source`](Self::start_source) / [`create_sink`](Self::create_sink)
/// to obtain shared handles.
pub struct ManagedConnectorRegistry {
    connectors: FxHashMap<String, Box<dyn ManagedConnector>>,
}

impl std::fmt::Debug for ManagedConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedConnectorRegistry")
            .finish_non_exhaustive()
    }
}

impl ManagedConnectorRegistry {
    /// Build the registry from the engine's declared connector configs.
    pub fn from_configs(
        configs: &FxHashMap<String, ConnectorConfig>,
    ) -> Result<Self, ConnectorError> {
        let mut connectors = FxHashMap::default();

        for (name, config) in configs {
            let managed = create_managed(name, config)?;
            connectors.insert(name.clone(), managed);
        }

        info!(
            "ManagedConnectorRegistry: created {} connectors",
            connectors.len()
        );
        Ok(Self { connectors })
    }

    /// Start a source subscription on the named connector.
    pub async fn start_source(
        &mut self,
        connector_name: &str,
        topic: &str,
        tx: mpsc::Sender<Vec<Event>>,
        params: &HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let connector = self.connectors.get_mut(connector_name).ok_or_else(|| {
            ConnectorError::ConfigError(format!("Unknown connector: {connector_name}"))
        })?;

        connector.start_source(topic, tx, params).await
    }

    /// Create a shared sink for the named connector.
    pub fn create_sink(
        &mut self,
        connector_name: &str,
        topic: &str,
        params: &HashMap<String, String>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let connector = self.connectors.get_mut(connector_name).ok_or_else(|| {
            ConnectorError::ConfigError(format!("Unknown connector: {connector_name}"))
        })?;

        connector.create_sink(topic, params)
    }

    /// Propagate an engine-wide offset registry into every managed
    /// connector. Replayable sources (currently only Kafka) will mirror
    /// their consumed offsets into the registry so the engine can snapshot
    /// them at checkpoint time; other connectors ignore the call.
    pub fn set_engine_offsets_registry(&mut self, registry: EngineOffsetRegistry) {
        for connector in self.connectors.values_mut() {
            connector.set_engine_offsets_registry(registry.clone());
        }
    }

    /// Fan the engine's cooperative source-pause flag out to every managed
    /// connector, so replayable sources stop pulling during a checkpoint
    /// barrier's drain. Mirrors [`set_engine_offsets_registry`].
    pub fn set_source_pause_handle(
        &mut self,
        handle: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        for connector in self.connectors.values_mut() {
            connector.set_source_pause_handle(handle.clone());
        }
    }

    /// Commit per-partition source offsets for a given (connector, topic)
    /// pair. Called by the driver after a checkpoint has been durably
    /// persisted and all 2PC sinks have committed, closing the loop for
    /// end-to-end exactly-once.
    pub async fn commit_source_offsets(
        &self,
        connector_name: &str,
        topic: &str,
        offsets: &HashMap<i32, i64>,
    ) -> Result<(), ConnectorError> {
        let connector = self.connectors.get(connector_name).ok_or_else(|| {
            ConnectorError::ConfigError(format!("Unknown connector: {connector_name}"))
        })?;
        connector.commit_source_offsets(topic, offsets).await
    }

    /// Stage per-partition source offsets to be folded into the connector's sink
    /// transaction (audit C4 — atomic offset+output commit). Called by the
    /// barrier between prepare and commit for exactly-once sinks.
    pub async fn stage_txn_offsets(
        &self,
        connector_name: &str,
        topic: &str,
        offsets: &HashMap<i32, i64>,
    ) -> Result<(), ConnectorError> {
        let connector = self.connectors.get(connector_name).ok_or_else(|| {
            ConnectorError::ConfigError(format!("Unknown connector: {connector_name}"))
        })?;
        connector.stage_txn_offsets(topic, offsets).await
    }

    /// Collect health reports from all managed connectors.
    pub fn health_reports(&self) -> Vec<(&str, &str, ConnectorHealthReport)> {
        self.connectors
            .iter()
            .map(|(name, conn)| (name.as_str(), conn.connector_type(), conn.health()))
            .collect()
    }

    /// Shut down all managed connectors.
    pub async fn shutdown(&mut self) {
        for (name, connector) in &mut self.connectors {
            if let Err(e) = connector.shutdown().await {
                warn!("Error shutting down connector {}: {}", name, e);
            }
        }
    }
}

/// Factory: create the right `ManagedConnector` for a given config.
fn create_managed(
    name: &str,
    config: &ConnectorConfig,
) -> Result<Box<dyn ManagedConnector>, ConnectorError> {
    // Try inventory-based factory first
    if let Some(factory) = super::component::find_factory(&config.connector_type) {
        if factory.info().supports_managed {
            return factory.create_managed(name, config);
        }
    }

    // No fallback needed -- all managed connectors are now registered via inventory
    Err(ConnectorError::NotAvailable(format!(
        "No managed connector for type '{}'. Supported: mqtt, nats, kafka",
        config.connector_type,
    )))
}
