//! Managed connector registry — owns one connection per declared connector

use super::managed::ManagedConnector;
use super::managed_mqtt::ManagedMqttConnector;
use super::mqtt::MqttConfig;
use super::types::{ConnectorConfig, ConnectorError};
use crate::event::Event;
use crate::sink::Sink;
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Registry that owns one [`ManagedConnector`] per declared connector name.
///
/// Build via [`from_configs`](Self::from_configs), then call
/// [`start_source`](Self::start_source) / [`create_sink`](Self::create_sink)
/// to obtain shared handles.
pub struct ManagedConnectorRegistry {
    connectors: FxHashMap<String, Box<dyn ManagedConnector>>,
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
    ///
    /// The first call per connector establishes the connection; subsequent
    /// calls add subscriptions on the existing connection.
    pub async fn start_source(
        &mut self,
        connector_name: &str,
        topic: &str,
        tx: mpsc::Sender<Event>,
        params: &HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let connector = self.connectors.get_mut(connector_name).ok_or_else(|| {
            ConnectorError::ConfigError(format!("Unknown connector: {}", connector_name))
        })?;

        connector.start_source(topic, tx, params).await
    }

    /// Create a shared sink for the named connector.
    ///
    /// If no source has been started yet, the connection is established lazily.
    pub fn create_sink(
        &mut self,
        connector_name: &str,
        topic: &str,
        params: &HashMap<String, String>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let connector = self.connectors.get_mut(connector_name).ok_or_else(|| {
            ConnectorError::ConfigError(format!("Unknown connector: {}", connector_name))
        })?;

        connector.create_sink(topic, params)
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
    match config.connector_type.as_str() {
        "mqtt" => {
            let mut mqtt_config =
                MqttConfig::new(&config.url, config.topic.as_deref().unwrap_or("#"));

            // Apply properties from ConnectorConfig
            if let Some(port) = config.properties.get("port") {
                if let Ok(p) = port.parse::<u16>() {
                    mqtt_config = mqtt_config.with_port(p);
                }
            }
            if let Some(client_id) = config.properties.get("client_id") {
                mqtt_config = mqtt_config.with_client_id(client_id);
            }
            if let Some(qos) = config.properties.get("qos") {
                if let Ok(q) = qos.parse::<u8>() {
                    mqtt_config = mqtt_config.with_qos(q);
                }
            }

            Ok(Box::new(ManagedMqttConnector::new(name, mqtt_config)))
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            use super::kafka::KafkaConfig;
            use super::managed_kafka::ManagedKafkaConnector;

            let topic = config.topic.as_deref().unwrap_or("events");
            let brokers = if config.url.is_empty() {
                config
                    .properties
                    .get("brokers")
                    .cloned()
                    .unwrap_or_default()
            } else {
                config.url.clone()
            };
            let mut kafka_config =
                KafkaConfig::new(&brokers, topic).with_properties(config.properties.clone());

            if let Some(group_id) = config.properties.get("group_id") {
                kafka_config = kafka_config.with_group_id(group_id);
            }

            Ok(Box::new(ManagedKafkaConnector::new(name, kafka_config)))
        }
        other => Err(ConnectorError::NotAvailable(format!(
            "No managed connector for type '{}'. Supported: mqtt{}",
            other,
            if cfg!(feature = "kafka") {
                ", kafka"
            } else {
                ""
            },
        ))),
    }
}
