//! Connector API traits and types for Varpulis
//!
//! This crate defines the shared interfaces that all Varpulis connectors implement.
//! It contains the core traits (`Sink`, `SourceConnector`, `SinkConnector`,
//! `ManagedConnector`), configuration types, and utility modules (circuit breaker,
//! converters, helpers, resource limits).
//!
//! Individual connector implementations (MQTT, Kafka, NATS, etc.) live in
//! separate crates and depend on this API crate.

pub mod circuit_breaker;
pub mod component;
pub mod converter;
pub mod helpers;
pub mod limits;
pub mod managed;
pub mod sink;
pub mod types;

// Re-export key types at crate root for convenience
pub use component::{
    find_factory, list_components, ConfigParamInfo, ConnectorComponentInfo, ConnectorFactory,
};
pub use managed::{ConnectorHealthReport, ManagedConnector};
pub use sink::{Sink, SinkConnectorAdapter, SinkError};
pub use types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};
