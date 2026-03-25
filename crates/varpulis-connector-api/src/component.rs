//! Declarative component registration for connectors
//!
//! Uses the `inventory` crate to provide zero-boilerplate global static
//! registration. Each connector module conditionally submits its factory via
//! `inventory::submit!`, which works naturally with `#[cfg(feature = ...)]`.

use std::sync::Arc;

use crate::managed::ManagedConnector;
use crate::sink::Sink;
use crate::types::{ConnectorConfig, ConnectorError, SinkConnector};

/// Metadata describing a connector component.
#[derive(Debug, Clone)]
pub struct ConnectorComponentInfo {
    /// Connector type identifier used in VPL (e.g., `"mqtt"`, `"kafka"`)
    pub connector_type: &'static str,
    /// Human-readable display name
    pub display_name: &'static str,
    /// Short description of the connector
    pub description: &'static str,
    /// Cargo feature flag required (empty if always available)
    pub feature_flag: &'static str,
    /// Whether this connector supports source mode
    pub supports_source: bool,
    /// Whether this connector supports sink mode
    pub supports_sink: bool,
    /// Whether this connector supports managed mode (actor-supervised)
    pub supports_managed: bool,
    /// Configuration parameters accepted by this connector
    pub config_params: &'static [ConfigParamInfo],
}

/// Metadata for a single configuration parameter.
#[derive(Debug, Clone)]
pub struct ConfigParamInfo {
    /// Parameter name as used in VPL
    pub name: &'static str,
    /// Human-readable description
    pub description: &'static str,
    /// Whether this parameter is required
    pub required: bool,
    /// Default value (if any)
    pub default_value: Option<&'static str>,
}

/// Factory trait for creating connector instances from configuration.
///
/// Implementations are registered globally via `inventory::submit!` and
/// discovered at runtime by [`find_factory`].
pub trait ConnectorFactory: Send + Sync {
    /// Component metadata for this connector.
    fn info(&self) -> &ConnectorComponentInfo;

    /// Create a managed connector instance.
    fn create_managed(
        &self,
        name: &str,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn ManagedConnector>, ConnectorError> {
        let _ = (name, config);
        Err(ConnectorError::NotAvailable(format!(
            "Connector '{}' does not support managed mode",
            self.info().connector_type
        )))
    }

    /// Create a sink connector instance (legacy registry path).
    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let _ = config;
        Err(ConnectorError::NotAvailable(format!(
            "Connector '{}' does not support sink connector mode",
            self.info().connector_type
        )))
    }

    /// Create an engine sink (`Arc<dyn Sink>`) for use in the sink registry.
    fn create_engine_sink(
        &self,
        name: &str,
        config: &ConnectorConfig,
        topic_override: Option<&str>,
        context_name: Option<&str>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let _ = (name, config, topic_override, context_name);
        Err(ConnectorError::NotAvailable(format!(
            "Connector '{}' does not support engine sink mode",
            self.info().connector_type
        )))
    }
}

inventory::collect!(&'static dyn ConnectorFactory);

/// Find a registered factory by connector type identifier.
pub fn find_factory(connector_type: &str) -> Option<&'static dyn ConnectorFactory> {
    inventory::iter::<&'static dyn ConnectorFactory>
        .into_iter()
        .copied()
        .find(|f| f.info().connector_type == connector_type)
}

/// List all registered connector components and their metadata.
pub fn list_components() -> Vec<&'static ConnectorComponentInfo> {
    inventory::iter::<&'static dyn ConnectorFactory>
        .into_iter()
        .map(|f| f.info())
        .collect()
}
