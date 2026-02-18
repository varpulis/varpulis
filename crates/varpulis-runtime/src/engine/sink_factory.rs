//! Sink construction and management for the Varpulis engine
//!
//! This module provides functionality to create sinks from connector configurations
//! and manage a registry of active sinks.

use crate::connector;
#[cfg(feature = "kafka")]
use crate::connector::SinkConnector;
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
    async fn send_batch(
        &self,
        events: &[std::sync::Arc<crate::event::Event>],
    ) -> anyhow::Result<()> {
        // Acquire lock once for the entire batch
        let inner = self.inner.lock().await;
        for event in events {
            inner
                .send(event)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;
        }
        Ok(())
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

/// Adapter for Kafka sinks using transactional (exactly-once) delivery.
///
/// Wraps `KafkaSinkFull` directly (not via the `SinkConnector` trait) to access
/// the transactional batch API. Single sends and batches are both wrapped in
/// Kafka transactions so that consumers with `isolation.level=read_committed`
/// see atomic, exactly-once delivery.
#[cfg(feature = "kafka")]
pub struct TransactionalKafkaSinkAdapter {
    name: String,
    inner: tokio::sync::Mutex<connector::KafkaSinkFull>,
}

#[cfg(feature = "kafka")]
impl TransactionalKafkaSinkAdapter {
    pub fn new(name: &str, sink: connector::KafkaSinkFull) -> Self {
        Self {
            name: name.to_string(),
            inner: tokio::sync::Mutex::new(sink),
        }
    }
}

#[cfg(feature = "kafka")]
#[async_trait::async_trait]
impl crate::sink::Sink for TransactionalKafkaSinkAdapter {
    fn name(&self) -> &str {
        &self.name
    }
    async fn connect(&self) -> anyhow::Result<()> {
        Ok(()) // Producer is connected at construction time
    }
    async fn send(&self, event: &crate::event::Event) -> anyhow::Result<()> {
        let inner = self.inner.lock().await;
        inner
            .send(event)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
    async fn send_batch(
        &self,
        events: &[std::sync::Arc<crate::event::Event>],
    ) -> anyhow::Result<()> {
        let inner = self.inner.lock().await;
        inner
            .send_batch_transactional(events)
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

                // Extract transactional_id from properties or auto-generate when
                // exactly_once is requested
                let transactional_id =
                    config
                        .properties
                        .get("transactional_id")
                        .cloned()
                        .or_else(|| {
                            config
                                .properties
                                .get("exactly_once")
                                .filter(|v| v == &"true")
                                .map(|_| format!("varpulis-{}", name))
                        });

                let mut kafka_config = connector::KafkaConfig::new(&brokers, &topic)
                    .with_properties(config.properties.clone());
                if let Some(tid) = transactional_id {
                    kafka_config = kafka_config.with_transactional_id(&tid);
                }

                match connector::KafkaSinkFull::new(name, kafka_config) {
                    Ok(sink) => {
                        if sink.is_transactional() {
                            Some(Arc::new(TransactionalKafkaSinkAdapter::new(name, sink)))
                        } else {
                            Some(Arc::new(SinkConnectorAdapter {
                                name: name.to_string(),
                                inner: tokio::sync::Mutex::new(Box::new(sink)),
                            }))
                        }
                    }
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
                let base_id = config
                    .properties
                    .get("client_id")
                    .cloned()
                    .unwrap_or_else(|| name.to_string());
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
        "nats" => {
            #[cfg(feature = "nats")]
            {
                let servers = if config.url.is_empty() {
                    config
                        .properties
                        .get("servers")
                        .cloned()
                        .unwrap_or_else(|| "nats://localhost:4222".to_string())
                } else {
                    config.url.clone()
                };
                let subject = topic_override
                    .map(|s| s.to_string())
                    .or_else(|| config.topic.clone())
                    .unwrap_or_else(|| format!("{}-output", name));
                let nats_config = connector::NatsConfig::new(&servers, &subject);
                let sink = connector::NatsSink::new(name, nats_config);
                Some(Arc::new(SinkConnectorAdapter {
                    name: name.to_string(),
                    inner: tokio::sync::Mutex::new(Box::new(sink)),
                }))
            }
            #[cfg(not(feature = "nats"))]
            {
                warn!("NATS connector '{}' requires 'nats' feature flag", name);
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

    /// Wrap all registered sinks with circuit breaker + DLQ protection.
    ///
    /// Call after `build_from_connectors()` to add resilience to every sink.
    /// Events that fail delivery (or are rejected by the circuit breaker)
    /// are routed to the DLQ file instead of being silently dropped.
    pub fn wrap_with_resilience(
        &mut self,
        cb_config: crate::circuit_breaker::CircuitBreakerConfig,
        dlq: Option<Arc<crate::dead_letter::DeadLetterQueue>>,
    ) {
        let old_cache = std::mem::take(&mut self.cache);
        for (key, sink) in old_cache {
            let cb = Arc::new(crate::circuit_breaker::CircuitBreaker::new(
                cb_config.clone(),
            ));
            let resilient = Arc::new(crate::sink::ResilientSink::new(sink, cb, dlq.clone()));
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
        let sink = create_sink_from_config("test_console", &config, None, None);
        assert!(sink.is_some());
    }

    #[test]
    fn test_unknown_connector_returns_none() {
        let config = connector::ConnectorConfig::new("unknown_type", "");
        let sink = create_sink_from_config("test", &config, None, None);
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
