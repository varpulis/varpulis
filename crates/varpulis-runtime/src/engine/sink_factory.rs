//! Sink construction and management for the Varpulis engine
//!
//! This module provides functionality to create sinks from connector configurations
//! and manage a registry of active sinks.

use std::sync::Arc;

use indexmap::IndexMap;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug, warn};
use varpulis_core::ast::ConnectorParam;

use crate::connector;

/// Convert AST ConnectorParams to a runtime ConnectorConfig
pub fn connector_params_to_config(
    connector_type: &str,
    params: &[ConnectorParam],
) -> connector::ConnectorConfig {
    let mut url = String::new();
    let mut topic = None;
    let mut properties = IndexMap::new();

    for param in params {
        let value_str = match &param.value {
            varpulis_core::ast::ConfigValue::Str(s) => s.clone(),
            varpulis_core::ast::ConfigValue::Ident(s) => s.clone(),
            varpulis_core::ast::ConfigValue::Int(i) => i.to_string(),
            varpulis_core::ast::ConfigValue::Float(f) => f.to_string(),
            varpulis_core::ast::ConfigValue::Bool(b) => b.to_string(),
            varpulis_core::ast::ConfigValue::Duration(d) => format!("{d}ns"),
            // Arrays of strings (e.g. `brokers: ["k1:9092", "k2:9092"]`) are
            // joined with commas — the standard format for librdkafka's
            // bootstrap.servers and equivalent multi-host config keys.
            varpulis_core::ast::ConfigValue::Array(arr) => {
                let parts: Vec<String> = arr
                    .iter()
                    .filter_map(|v| match v {
                        varpulis_core::ast::ConfigValue::Str(s) => Some(s.clone()),
                        varpulis_core::ast::ConfigValue::Ident(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                if parts.is_empty() {
                    continue;
                }
                parts.join(",")
            }
            varpulis_core::ast::ConfigValue::Map(_) => continue,
            varpulis_core::ast::ConfigValue::Concat(_) => continue, // dynamic — resolved at runtime
        };
        match param.name.as_str() {
            "url" | "host" | "brokers" | "servers" => url = value_str,
            "topic" => topic = Some(value_str),
            other => {
                properties.insert(other.to_string(), value_str);
            }
        }
    }

    let mut config = connector::ConnectorConfig::new(connector_type, &url);
    if let Some(t) = topic {
        config = config.with_topic(&t);
    }
    config.properties = properties;
    config
}

/// Adapter: wraps a SinkConnector as a Sink for use in the sink registry
pub struct SinkConnectorAdapter {
    name: String,
    inner: tokio::sync::Mutex<Box<dyn connector::SinkConnector>>,
}

impl std::fmt::Debug for SinkConnectorAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkConnectorAdapter")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl SinkConnectorAdapter {
    /// Create a new adapter wrapping a SinkConnector.
    pub fn new(name: &str, connector: Box<dyn connector::SinkConnector>) -> Self {
        Self {
            name: name.to_string(),
            inner: tokio::sync::Mutex::new(connector),
        }
    }
}

#[async_trait::async_trait]
impl crate::sink::Sink for SinkConnectorAdapter {
    fn name(&self) -> &str {
        &self.name
    }
    async fn connect(&self) -> Result<(), crate::sink::SinkError> {
        let mut inner = self.inner.lock().await;
        inner.connect().await.map_err(crate::sink::SinkError::from)
    }
    async fn send(&self, event: &crate::event::Event) -> Result<(), crate::sink::SinkError> {
        let inner = self.inner.lock().await;
        inner
            .send(event)
            .await
            .map_err(crate::sink::SinkError::from)
    }
    async fn send_batch(
        &self,
        events: &[std::sync::Arc<crate::event::Event>],
    ) -> Result<(), crate::sink::SinkError> {
        // Acquire lock once for the entire batch
        let inner = self.inner.lock().await;
        for event in events {
            inner
                .send(event)
                .await
                .map_err(crate::sink::SinkError::from)?;
        }
        Ok(())
    }
    async fn flush(&self) -> Result<(), crate::sink::SinkError> {
        let inner = self.inner.lock().await;
        inner.flush().await.map_err(crate::sink::SinkError::from)
    }
    async fn close(&self) -> Result<(), crate::sink::SinkError> {
        let inner = self.inner.lock().await;
        inner.close().await.map_err(crate::sink::SinkError::from)
    }
}

/// Create a sink from a ConnectorConfig, with an optional topic override from .to() params.
///
/// Tries the inventory-based `find_factory()` first, then falls back to the
/// match-arm dispatch for connectors that haven't been migrated yet.
///
/// `transactional_id_prefix` is composed by the cluster worker as
/// `"{group_id}-{pipeline_name}-{worker_id}"` and is used to disambiguate
/// auto-generated Kafka `transactional.id` values across replicas of the
/// same pipeline. When `None`, the legacy `"varpulis-{name}"` format is
/// used (single-process deployments).
#[allow(unused_variables)]
pub fn create_sink_from_config(
    name: &str,
    config: &connector::ConnectorConfig,
    topic_override: Option<&str>,
    context_name: Option<&str>,
    topic_prefix: Option<&str>,
    transactional_id_prefix: Option<&str>,
) -> Option<Arc<dyn crate::sink::Sink>> {
    // Try inventory-based factory first
    if let Some(factory) = connector::component::find_factory(&config.connector_type) {
        if factory.info().supports_sink {
            match factory.create_engine_sink(name, config, topic_override, context_name) {
                Ok(sink) => return Some(sink),
                Err(connector::ConnectorError::NotAvailable(_)) => {} // fall through
                Err(e) => {
                    warn!("Factory error creating sink '{}': {}", name, e);
                    return None;
                }
            }
        }
    }

    // Fallback to match-arm dispatch
    match config.connector_type.as_str() {
        "console" => Some(Arc::new(crate::sink::ConsoleSink::new(name))),
        "file" => {
            let path = if config.url.is_empty() {
                config
                    .properties
                    .get("path")
                    .cloned()
                    .unwrap_or_else(|| format!("{name}.jsonl"))
            } else {
                config.url.clone()
            };
            match crate::sink::FileSink::new(name, &path) {
                Ok(sink) => Some(Arc::new(sink)),
                Err(e) => {
                    warn!("Failed to create file sink '{}': {}", name, e);
                    None
                }
            }
        }
        "http" => {
            let url = config.url.clone();
            if url.is_empty() {
                warn!("HTTP connector '{}' has no URL configured", name);
                None
            } else {
                Some(Arc::new(crate::sink::HttpSink::new(name, &url)))
            }
        }
        other => {
            debug!(
                "Connector '{}' (type '{}') does not support sink output",
                name, other
            );
            None
        }
    }
}

/// Registry for managing sink instances.
///
/// Handles:
/// - Building sinks from connector configurations
/// - Caching created sinks by their keys
/// - Connecting all registered sinks
pub struct SinkRegistry {
    cache: FxHashMap<String, Arc<dyn crate::sink::Sink>>,
}

impl SinkRegistry {
    /// Create a new empty sink registry
    pub fn new() -> Self {
        Self {
            cache: FxHashMap::default(),
        }
    }

    /// Insert a pre-built sink into the registry.
    pub fn insert(&mut self, key: String, sink: Arc<dyn crate::sink::Sink>) {
        self.cache.insert(key, sink);
    }

    /// Get the internal cache (for compatibility with existing code)
    pub fn cache(&self) -> &FxHashMap<String, Arc<dyn crate::sink::Sink>> {
        &self.cache
    }

    /// Get the internal cache mutably (for hot reload)
    pub fn cache_mut(&mut self) -> &mut FxHashMap<String, Arc<dyn crate::sink::Sink>> {
        &mut self.cache
    }

    /// Build sinks from connector declarations, only for referenced sink keys.
    ///
    /// Creating unreferenced sinks (e.g. a base connector entry when only
    /// topic-override entries are used) wastes resources and — for MQTT —
    /// causes duplicate client_id conflicts that disconnect the useful sink.
    pub fn build_from_connectors(
        &mut self,
        connectors: &FxHashMap<String, connector::ConnectorConfig>,
        referenced_keys: &FxHashSet<String>,
        topic_overrides: &[(String, String, String)],
        context_name: Option<&str>,
        topic_prefix: Option<&str>,
        transactional_id_prefix: Option<&str>,
    ) {
        // Create sinks for directly referenced connectors
        for (name, config) in connectors {
            if referenced_keys.contains(name) {
                if let Some(sink) = create_sink_from_config(
                    name,
                    config,
                    None,
                    context_name,
                    topic_prefix,
                    transactional_id_prefix,
                ) {
                    self.cache.insert(name.clone(), sink);
                }
            }
        }

        // Create sinks for topic-override keys
        for (sink_key, connector_name, topic) in topic_overrides {
            if !self.cache.contains_key(sink_key) {
                if let Some(config) = connectors.get(connector_name) {
                    if let Some(sink) = create_sink_from_config(
                        connector_name,
                        config,
                        Some(topic),
                        context_name,
                        topic_prefix,
                        transactional_id_prefix,
                    ) {
                        self.cache.insert(sink_key.clone(), sink);
                    }
                }
            }
        }
    }

    /// Get a sink by its key
    #[allow(dead_code)]
    pub fn get(&self, key: &str) -> Option<&Arc<dyn crate::sink::Sink>> {
        self.cache.get(key)
    }

    /// Connect all sinks that require explicit connection.
    ///
    /// Call this after building sinks to establish connections to external systems
    /// like MQTT brokers, databases, etc.
    pub async fn connect_all(&self) -> Result<(), String> {
        for (name, sink) in &self.cache {
            if let Err(e) = sink.connect().await {
                return Err(format!("Failed to connect sink '{name}': {e}"));
            }
        }
        Ok(())
    }

    /// Wrap all registered sinks with circuit breaker + DLQ protection.
    ///
    /// Call after `build_from_connectors()` to add resilience to every sink.
    /// Events that fail delivery (or are rejected by the circuit breaker)
    /// are routed to the DLQ file instead of being silently dropped.
    pub fn wrap_with_resilience(
        &mut self,
        cb_config: crate::circuit_breaker::CircuitBreakerConfig,
        dlq: Option<Arc<crate::dead_letter::DeadLetterQueue>>,
        metrics: Option<crate::metrics::Metrics>,
    ) {
        let old_cache = std::mem::take(&mut self.cache);
        for (key, sink) in old_cache {
            let cb = Arc::new(crate::circuit_breaker::CircuitBreaker::new(
                cb_config.clone(),
            ));
            let resilient = Arc::new(crate::sink::ResilientSink::new(
                sink,
                cb,
                dlq.clone(),
                metrics.clone(),
            ));
            self.cache.insert(key, resilient);
        }
    }
}

impl Default for SinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_new() {
        let registry = SinkRegistry::new();
        assert!(registry.cache.is_empty());
    }

    #[test]
    fn test_console_sink_creation() {
        let config = connector::ConnectorConfig::new("console", "");
        let sink = create_sink_from_config("test_console", &config, None, None, None, None);
        assert!(sink.is_some());
    }

    #[test]
    fn test_unknown_connector_returns_none() {
        let config = connector::ConnectorConfig::new("unknown_type", "");
        let sink = create_sink_from_config("test", &config, None, None, None, None);
        assert!(sink.is_none());
    }

    #[test]
    fn test_connector_params_extracts_exactly_once() {
        use varpulis_core::ast::{ConfigValue, ConnectorParam};

        let params = vec![
            ConnectorParam {
                name: "brokers".to_string(),
                value: ConfigValue::Str("localhost:9092".to_string()),
            },
            ConnectorParam {
                name: "topic".to_string(),
                value: ConfigValue::Str("my-topic".to_string()),
            },
            ConnectorParam {
                name: "exactly_once".to_string(),
                value: ConfigValue::Bool(true),
            },
        ];

        let config = connector_params_to_config("kafka", &params);
        assert_eq!(config.url, "localhost:9092");
        assert_eq!(config.topic, Some("my-topic".to_string()));
        // exactly_once=true should be stored in properties
        assert_eq!(
            config.properties.get("exactly_once"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_connector_params_extracts_transactional_id() {
        use varpulis_core::ast::{ConfigValue, ConnectorParam};

        let params = vec![
            ConnectorParam {
                name: "brokers".to_string(),
                value: ConfigValue::Str("localhost:9092".to_string()),
            },
            ConnectorParam {
                name: "transactional_id".to_string(),
                value: ConfigValue::Str("my-app-txn".to_string()),
            },
        ];

        let config = connector_params_to_config("kafka", &params);
        assert_eq!(
            config.properties.get("transactional_id"),
            Some(&"my-app-txn".to_string())
        );
    }
}
