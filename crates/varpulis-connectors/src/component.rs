//! Declarative component registration for connectors
//!
//! Uses the `inventory` crate to provide zero-boilerplate global static
//! registration. Each connector module conditionally submits its factory via
//! `inventory::submit!`, which works naturally with `#[cfg(feature = ...)]`.
//!
//! # Adding a new connector
//!
//! 1. Implement [`ConnectorFactory`] in your connector module
//! 2. Call `inventory::submit!` to register it
//! 3. Done — no match arms to edit anywhere
//!
//! ```rust,ignore
//! struct MyConnectorFactory;
//!
//! impl ConnectorFactory for MyConnectorFactory {
//!     fn info(&self) -> &ConnectorComponentInfo { &INFO }
//!     fn create_sink_connector(&self, config: &ConnectorConfig)
//!         -> Result<Box<dyn SinkConnector>, ConnectorError> { ... }
//! }
//!
//! inventory::submit! { &MyConnectorFactory as &dyn ConnectorFactory }
//! ```

use crate::managed::ManagedConnector;
use crate::sink::Sink;
use crate::types::{ConnectorConfig, ConnectorError, SinkConnector};
use std::sync::Arc;

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
    ///
    /// Returns `Err(NotAvailable)` if this connector does not support managed mode.
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
    ///
    /// Returns `Err(NotAvailable)` if this connector does not support sink mode.
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
    ///
    /// This is the primary sink creation path used by `sink_factory.rs`.
    /// Returns `Err(NotAvailable)` if this connector does not support sink mode.
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
///
/// Scans all factories registered via `inventory::submit!` and returns
/// the first one whose `info().connector_type` matches.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_components_returns_registered() {
        // At minimum, console should always be registered (no feature flag)
        let components = list_components();
        let console = components.iter().find(|c| c.connector_type == "console");
        assert!(console.is_some(), "Console connector should be registered");
        let info = console.unwrap();
        assert!(info.supports_source);
        assert!(info.supports_sink);
        assert!(info.feature_flag.is_empty());
    }

    #[test]
    fn test_find_factory_console() {
        let factory = find_factory("console");
        assert!(factory.is_some());
        assert_eq!(factory.unwrap().info().connector_type, "console");
    }

    #[test]
    fn test_find_factory_unknown() {
        assert!(find_factory("nonexistent_connector_xyz").is_none());
    }
}
