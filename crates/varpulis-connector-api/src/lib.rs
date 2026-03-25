//! Shared traits and types for Varpulis connector crates.
//!
//! This crate defines the public API that individual connector crates
//! (e.g., `varpulis-connector-mqtt`) implement against. It contains:
//!
//! - Core connector traits ([`SourceConnector`], [`SinkConnector`])
//! - Managed connector trait ([`ManagedConnector`])
//! - Sink trait and adapter ([`Sink`], [`SinkConnectorAdapter`])
//! - Circuit breaker ([`circuit_breaker`])
//! - Resource limits ([`limits`])
//! - Helper functions ([`helpers`])
//! - Declarative component registration ([`ConnectorFactory`])
//! - Converter trait ([`converter`])

pub mod circuit_breaker;
pub mod component;
pub mod converter;
pub mod helpers;
pub mod limits;
pub mod managed;
pub mod sink;
pub mod types;

// Re-export commonly used items at top level
pub use component::{ConfigParamInfo, ConnectorComponentInfo, ConnectorFactory};
pub use managed::{ConnectorHealthReport, ManagedConnector};
pub use sink::{Sink, SinkConnectorAdapter, SinkError};
pub use types::{ConnectorConfig, ConnectorError, ConnectorHealth, SinkConnector, SourceConnector};
