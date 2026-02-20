//! Legacy connector registry for source/sink management

use super::console::ConsoleSink;
use super::database::{DatabaseConfig, DatabaseSink};
use super::http::HttpSink;
use super::kafka::{KafkaConfig, KafkaSink};
use super::kinesis::KinesisConfig;
#[cfg(not(feature = "kinesis"))]
use super::kinesis::KinesisSink;
use super::mqtt::{MqttConfig, MqttSink};
use super::redis::RedisConfig;
#[cfg(not(feature = "redis"))]
use super::redis::RedisSink;
use super::rest_api::{RestApiConfig, RestApiSink};
use super::types::{ConnectorConfig, ConnectorError, SinkConnector, SourceConnector};
use indexmap::IndexMap;

#[cfg(feature = "kinesis")]
use super::kinesis::KinesisSinkFull;
#[cfg(feature = "redis")]
use super::redis::RedisSink as RedisSinkReal;
#[cfg(feature = "s3")]
use super::s3::S3SinkFull;

use super::elasticsearch::ElasticsearchConfig;
use super::s3::S3Config;

#[cfg(feature = "elasticsearch")]
use super::elasticsearch::ElasticsearchSinkFull;

#[cfg(not(feature = "elasticsearch"))]
use super::elasticsearch::ElasticsearchSink;
#[cfg(not(feature = "s3"))]
use super::s3::S3Sink;

/// Registry of available connectors
pub struct ConnectorRegistry {
    sources: IndexMap<String, Box<dyn SourceConnector>>,
    sinks: IndexMap<String, Box<dyn SinkConnector>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            sources: IndexMap::new(),
            sinks: IndexMap::new(),
        }
    }

    pub fn register_source(&mut self, name: &str, source: Box<dyn SourceConnector>) {
        self.sources.insert(name.to_string(), source);
    }

    pub fn register_sink(&mut self, name: &str, sink: Box<dyn SinkConnector>) {
        self.sinks.insert(name.to_string(), sink);
    }

    pub fn get_source(&mut self, name: &str) -> Option<&mut Box<dyn SourceConnector>> {
        self.sources.get_mut(name)
    }

    pub fn get_sink(&self, name: &str) -> Option<&dyn SinkConnector> {
        self.sinks.get(name).map(|b| b.as_ref())
    }

    /// Create a connector from configuration
    pub async fn create_from_config(
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        match config.connector_type.as_str() {
            "console" => Ok(Box::new(ConsoleSink::new("console"))),
            "http" => Ok(Box::new(HttpSink::new("http", &config.url))),
            "kafka" => {
                let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
                Ok(Box::new(KafkaSink::new(
                    "kafka",
                    KafkaConfig::new(&config.url, &topic),
                )))
            }
            "mqtt" => {
                let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
                Ok(Box::new(MqttSink::new(
                    "mqtt",
                    MqttConfig::new(&config.url, &topic),
                )))
            }
            "rest" | "api" => {
                let path = config
                    .topic
                    .clone()
                    .unwrap_or_else(|| "/events".to_string());
                let api_config = RestApiConfig::new(&config.url);
                Ok(Box::new(RestApiSink::new("rest", api_config, &path)?))
            }
            "redis" => {
                let channel = config.topic.clone().unwrap_or_else(|| "events".to_string());
                #[cfg(feature = "redis")]
                {
                    let sink = RedisSinkReal::new("redis", RedisConfig::new(&config.url, &channel))
                        .await?;
                    Ok(Box::new(sink))
                }
                #[cfg(not(feature = "redis"))]
                {
                    Ok(Box::new(RedisSink::new(
                        "redis",
                        RedisConfig::new(&config.url, &channel),
                    )))
                }
            }
            "database" | "postgres" | "mysql" | "sqlite" => {
                let table = config.topic.clone().unwrap_or_else(|| "events".to_string());
                let sink = DatabaseSink::new("database", DatabaseConfig::new(&config.url, &table)?)
                    .await?;
                Ok(Box::new(sink))
            }
            "kinesis" => {
                let stream = config.topic.clone().unwrap_or_else(|| "events".to_string());
                let region = config
                    .properties
                    .get("region")
                    .cloned()
                    .unwrap_or_else(|| "us-east-1".to_string());
                #[cfg(feature = "kinesis")]
                {
                    let sink =
                        KinesisSinkFull::new("kinesis", KinesisConfig::new(&stream, &region))
                            .await?;
                    Ok(Box::new(sink))
                }
                #[cfg(not(feature = "kinesis"))]
                {
                    Ok(Box::new(KinesisSink::new(
                        "kinesis",
                        KinesisConfig::new(&stream, &region),
                    )))
                }
            }
            "s3" => {
                let prefix = config
                    .topic
                    .clone()
                    .unwrap_or_else(|| "events/".to_string());
                let region = config
                    .properties
                    .get("region")
                    .cloned()
                    .unwrap_or_else(|| "us-east-1".to_string());
                #[cfg(feature = "s3")]
                {
                    let sink =
                        S3SinkFull::new("s3", S3Config::new(&config.url, &prefix, &region)).await?;
                    Ok(Box::new(sink))
                }
                #[cfg(not(feature = "s3"))]
                {
                    Ok(Box::new(S3Sink::new(
                        "s3",
                        S3Config::new(&config.url, &prefix, &region),
                    )))
                }
            }
            "elasticsearch" | "es" => {
                let index = config.topic.clone().unwrap_or_else(|| "events".to_string());
                #[cfg(feature = "elasticsearch")]
                {
                    let sink = ElasticsearchSinkFull::new(
                        "elasticsearch",
                        ElasticsearchConfig::new(&config.url, &index),
                    )?;
                    Ok(Box::new(sink))
                }
                #[cfg(not(feature = "elasticsearch"))]
                {
                    Ok(Box::new(ElasticsearchSink::new(
                        "elasticsearch",
                        ElasticsearchConfig::new(&config.url, &index),
                    )))
                }
            }
            _ => Err(ConnectorError::ConfigError(format!(
                "Unknown connector type: {}",
                config.connector_type
            ))),
        }
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
