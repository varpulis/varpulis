//! Sink construction and management for the Varpulis engine
//!
//! This module provides functionality to create sinks from connector configurations
//! and manage a registry of active sinks.

use crate::connector;
use indexmap::IndexMap;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;
use tracing::{debug, warn};
use varpulis_core::ast::ConnectorParam;

/// Convert AST ConnectorParams to a runtime ConnectorConfig
pub(crate) fn connector_params_to_config(
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
            varpulis_core::ast::ConfigValue::Duration(d) => format!("{}ns", d),
            varpulis_core::ast::ConfigValue::Array(_) => continue,
            varpulis_core::ast::ConfigValue::Map(_) => continue,
        };
        match param.name.as_str() {
            "url" | "host" => url = value_str,
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
#[allow(dead_code)]
pub(super) struct SinkConnectorAdapter {
    name: String,
    inner: tokio::sync::Mutex<Box<dyn connector::SinkConnector>>,
}

#[async_trait::async_trait]
impl crate::sink::Sink for SinkConnectorAdapter {
    fn name(&self) -> &str {
        &self.name
    }
    async fn connect(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        inner.connect().await.map_err(|e| anyhow::anyhow!("{}", e))
    }
    async fn send(&self, event: &crate::event::Event) -> anyhow::Result<()> {
        let inner = self.inner.lock().await;
        inner
            .send(event)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
    async fn flush(&self) -> anyhow::Result<()> {
        let inner = self.inner.lock().await;
        inner.flush().await.map_err(|e| anyhow::anyhow!("{}", e))
    }
    async fn close(&self) -> anyhow::Result<()> {
        let inner = self.inner.lock().await;
        inner.close().await.map_err(|e| anyhow::anyhow!("{}", e))
    }
}

/// Create a sink from a ConnectorConfig, with an optional topic override from .to() params
#[allow(unused_variables)]
pub(crate) fn create_sink_from_config(
    name: &str,
    config: &connector::ConnectorConfig,
    topic_override: Option<&str>,
    context_name: Option<&str>,
) -> Option<Arc<dyn crate::sink::Sink>> {
    match config.connector_type.as_str() {
        "console" => Some(Arc::new(crate::sink::ConsoleSink::new(name))),
        "file" => {
            let path = if config.url.is_empty() {
                config
                    .properties
                    .get("path")
                    .cloned()
                    .unwrap_or_else(|| format!("{}.jsonl", name))
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
        "kafka" => {
            #[cfg(feature = "kafka")]
            {
                let brokers = config
                    .properties
                    .get("brokers")
                    .cloned()
                    .unwrap_or_else(|| config.url.clone());
                let topic = topic_override
                    .map(|s| s.to_string())
                    .or_else(|| config.topic.clone())
                    .unwrap_or_else(|| format!("{}-output", name));
                let kafka_config = connector::KafkaConfig::new(&brokers, &topic);
                match connector::KafkaSinkFull::new(name, kafka_config) {
                    Ok(sink) => Some(Arc::new(SinkConnectorAdapter {
                        name: name.to_string(),
                        inner: tokio::sync::Mutex::new(Box::new(sink)),
                    })),
                    Err(e) => {
                        warn!("Failed to create Kafka sink '{}': {}", name, e);
                        None
                    }
                }
            }
            #[cfg(not(feature = "kafka"))]
            {
                warn!("Kafka connector '{}' requires 'kafka' feature flag", name);
                None
            }
        }
        "mqtt" => {
            #[cfg(feature = "mqtt")]
            {
                let broker = config.url.clone();
                let port: u16 = config
                    .properties
                    .get("port")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1883);
                let topic = topic_override
                    .map(|s| s.to_string())
                    .or_else(|| config.topic.clone())
                    .unwrap_or_else(|| format!("{}-output", name));
                // Append "-sink" to the configured client_id to avoid collisions
                // with the source connector that uses the same client_id.
                let base_id = config
                    .properties
                    .get("client_id")
                    .map(|id| format!("{}-sink", id))
                    .unwrap_or_else(|| format!("{}-sink", name));
                let client_id = match context_name {
                    Some(ctx) => format!("{}-{}", base_id, ctx),
                    None => base_id,
                };
                let mqtt_config = connector::MqttConfig::new(&broker, &topic)
                    .with_port(port)
                    .with_client_id(&client_id);
                let sink = connector::MqttSink::new(name, mqtt_config);
                Some(Arc::new(SinkConnectorAdapter {
                    name: name.to_string(),
                    inner: tokio::sync::Mutex::new(Box::new(sink)),
                }))
            }
            #[cfg(not(feature = "mqtt"))]
            {
                warn!("MQTT connector '{}' requires 'mqtt' feature flag", name);
                None
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
pub(crate) struct SinkRegistry {
    cache: FxHashMap<String, Arc<dyn crate::sink::Sink>>,
}

impl SinkRegistry {
    /// Create a new empty sink registry
    pub fn new() -> Self {
        Self {
            cache: FxHashMap::default(),
        }
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
    ) {
        // Create sinks for directly referenced connectors
        for (name, config) in connectors {
            if referenced_keys.contains(name) {
                if let Some(sink) = create_sink_from_config(name, config, None, context_name) {
                    self.cache.insert(name.clone(), sink);
                }
            }
        }

        // Create sinks for topic-override keys
        for (sink_key, connector_name, topic) in topic_overrides {
            if !self.cache.contains_key(sink_key) {
                if let Some(config) = connectors.get(connector_name) {
                    if let Some(sink) =
                        create_sink_from_config(connector_name, config, Some(topic), context_name)
                    {
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
                return Err(format!("Failed to connect sink '{}': {}", name, e));
            }
        }
        Ok(())
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
        let sink = create_sink_from_config("test_console", &config, None, None);
        assert!(sink.is_some());
    }

    #[test]
    fn test_unknown_connector_returns_none() {
        let config = connector::ConnectorConfig::new("unknown_type", "");
        let sink = create_sink_from_config("test", &config, None, None);
        assert!(sink.is_none());
    }
}
