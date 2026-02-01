//! Configuration file support for Varpulis
//!
//! Supports both YAML and TOML configuration files.
//!
//! # Example YAML configuration:
//! ```yaml
//! # Varpulis configuration file
//!
//! # Query file to run
//! query_file: /path/to/queries.vql
//!
//! # Server settings
//! server:
//!   port: 9000
//!   bind: "0.0.0.0"
//!   metrics_enabled: true
//!   metrics_port: 9090
//!
//! # Kafka connector settings
//! kafka:
//!   bootstrap_servers: "localhost:9092"
//!   consumer_group: "varpulis-consumer"
//!
//! # Logging settings
//! logging:
//!   level: info
//!   format: json
//! ```

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    /// Path to the query file (.vql)
    pub query_file: Option<PathBuf>,

    /// Server configuration
    pub server: ServerConfig,

    /// Simulation configuration
    pub simulation: SimulationConfig,

    /// Kafka connector configuration
    pub kafka: Option<KafkaConfig>,

    /// HTTP webhook configuration
    pub http_webhook: Option<HttpWebhookConfig>,

    /// Logging configuration
    pub logging: LoggingConfig,

    /// Processing configuration
    pub processing: ProcessingConfig,

    /// TLS configuration
    pub tls: Option<TlsConfig>,

    /// Authentication configuration
    pub auth: Option<AuthConfig>,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Server port
    pub port: u16,

    /// Bind address
    pub bind: String,

    /// Enable metrics endpoint
    pub metrics_enabled: bool,

    /// Metrics port
    pub metrics_port: u16,

    /// Working directory
    pub workdir: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 9000,
            bind: "127.0.0.1".to_string(),
            metrics_enabled: false,
            metrics_port: 9090,
            workdir: None,
        }
    }
}

/// Simulation configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SimulationConfig {
    /// Run in immediate mode (no timing delays)
    pub immediate: bool,

    /// Preload events into memory
    pub preload: bool,

    /// Verbose output
    pub verbose: bool,

    /// Event file path
    pub events_file: Option<PathBuf>,
}

/// Kafka connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// Kafka bootstrap servers
    pub bootstrap_servers: String,

    /// Consumer group ID
    pub consumer_group: Option<String>,

    /// Input topic
    pub input_topic: Option<String>,

    /// Output topic
    pub output_topic: Option<String>,

    /// Enable auto-commit
    pub auto_commit: bool,

    /// Auto offset reset (earliest, latest)
    pub auto_offset_reset: String,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            consumer_group: None,
            input_topic: None,
            output_topic: None,
            auto_commit: true,
            auto_offset_reset: "latest".to_string(),
        }
    }
}

/// HTTP webhook configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpWebhookConfig {
    /// Enable HTTP webhook input
    pub enabled: bool,

    /// Webhook port
    pub port: u16,

    /// Bind address
    pub bind: String,

    /// API key for authentication
    pub api_key: Option<String>,

    /// Rate limit (requests per second, 0 = unlimited)
    pub rate_limit: u32,

    /// Maximum batch size
    pub max_batch_size: usize,
}

impl Default for HttpWebhookConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 8080,
            bind: "0.0.0.0".to_string(),
            api_key: None,
            rate_limit: 0,
            max_batch_size: 1000,
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    pub level: String,

    /// Log format (text, json)
    pub format: String,

    /// Include timestamps
    pub timestamps: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "text".to_string(),
            timestamps: true,
        }
    }
}

/// Processing configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProcessingConfig {
    /// Number of worker threads
    pub workers: Option<usize>,

    /// Partition key field
    pub partition_by: Option<String>,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to certificate file
    pub cert_file: PathBuf,

    /// Path to private key file
    pub key_file: PathBuf,
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// API key
    pub api_key: Option<String>,
}

impl Config {
    /// Load configuration from a file (YAML or TOML, auto-detected by extension)
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::IoError(path.to_path_buf(), e.to_string()))?;

        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        match extension.as_str() {
            "yaml" | "yml" => Self::from_yaml(&content),
            "toml" => Self::from_toml(&content),
            _ => {
                // Try YAML first, then TOML
                Self::from_yaml(&content).or_else(|_| Self::from_toml(&content))
            }
        }
    }

    /// Parse configuration from YAML string
    pub fn from_yaml(content: &str) -> Result<Self, ConfigError> {
        serde_yaml::from_str(content).map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    /// Parse configuration from TOML string
    pub fn from_toml(content: &str) -> Result<Self, ConfigError> {
        toml::from_str(content).map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    /// Merge another config into this one (other values take precedence if set)
    pub fn merge(&mut self, other: Config) {
        if other.query_file.is_some() {
            self.query_file = other.query_file;
        }

        // Merge server config
        if other.server.port != ServerConfig::default().port {
            self.server.port = other.server.port;
        }
        if other.server.bind != ServerConfig::default().bind {
            self.server.bind = other.server.bind;
        }
        if other.server.metrics_enabled {
            self.server.metrics_enabled = true;
        }
        if other.server.metrics_port != ServerConfig::default().metrics_port {
            self.server.metrics_port = other.server.metrics_port;
        }
        if other.server.workdir.is_some() {
            self.server.workdir = other.server.workdir;
        }

        // Merge processing config
        if other.processing.workers.is_some() {
            self.processing.workers = other.processing.workers;
        }
        if other.processing.partition_by.is_some() {
            self.processing.partition_by = other.processing.partition_by;
        }

        // Replace optional configs if provided
        if other.kafka.is_some() {
            self.kafka = other.kafka;
        }
        if other.http_webhook.is_some() {
            self.http_webhook = other.http_webhook;
        }
        if other.tls.is_some() {
            self.tls = other.tls;
        }
        if other.auth.is_some() {
            self.auth = other.auth;
        }
    }

    /// Create an example configuration
    pub fn example() -> Self {
        Self {
            query_file: Some(PathBuf::from("/app/queries/queries.vql")),
            server: ServerConfig {
                port: 9000,
                bind: "0.0.0.0".to_string(),
                metrics_enabled: true,
                metrics_port: 9090,
                workdir: Some(PathBuf::from("/app")),
            },
            simulation: SimulationConfig::default(),
            kafka: Some(KafkaConfig {
                bootstrap_servers: "kafka:9092".to_string(),
                consumer_group: Some("varpulis-consumer".to_string()),
                input_topic: Some("events".to_string()),
                output_topic: Some("alerts".to_string()),
                ..Default::default()
            }),
            http_webhook: Some(HttpWebhookConfig {
                enabled: true,
                port: 8080,
                bind: "0.0.0.0".to_string(),
                api_key: Some("your-api-key-here".to_string()),
                rate_limit: 1000,
                max_batch_size: 100,
            }),
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "json".to_string(),
                timestamps: true,
            },
            processing: ProcessingConfig {
                workers: Some(4),
                partition_by: Some("source_id".to_string()),
            },
            tls: None,
            auth: Some(AuthConfig {
                api_key: Some("your-websocket-api-key".to_string()),
            }),
        }
    }

    /// Generate example YAML configuration
    pub fn example_yaml() -> String {
        serde_yaml::to_string(&Self::example()).unwrap_or_default()
    }

    /// Generate example TOML configuration
    pub fn example_toml() -> String {
        toml::to_string_pretty(&Self::example()).unwrap_or_default()
    }
}

/// Configuration error types
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config file {0}: {1}")]
    IoError(PathBuf, String),

    #[error("Failed to parse config: {0}")]
    ParseError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.port, 9000);
        assert_eq!(config.server.bind, "127.0.0.1");
    }

    #[test]
    fn test_yaml_parsing() {
        let yaml = r#"
query_file: /app/queries.vql
server:
  port: 8080
  bind: "0.0.0.0"
  metrics_enabled: true
processing:
  workers: 8
"#;
        let config = Config::from_yaml(yaml).unwrap();
        assert_eq!(config.query_file, Some(PathBuf::from("/app/queries.vql")));
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.server.bind, "0.0.0.0");
        assert!(config.server.metrics_enabled);
        assert_eq!(config.processing.workers, Some(8));
    }

    #[test]
    fn test_toml_parsing() {
        let toml = r#"
query_file = "/app/queries.vql"

[server]
port = 8080
bind = "0.0.0.0"
metrics_enabled = true

[processing]
workers = 8
"#;
        let config = Config::from_toml(toml).unwrap();
        assert_eq!(config.query_file, Some(PathBuf::from("/app/queries.vql")));
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.server.bind, "0.0.0.0");
        assert!(config.server.metrics_enabled);
        assert_eq!(config.processing.workers, Some(8));
    }

    #[test]
    fn test_config_merge() {
        let mut base = Config::default();
        let override_config = Config {
            server: ServerConfig {
                port: 8888,
                ..Default::default()
            },
            ..Default::default()
        };

        base.merge(override_config);
        assert_eq!(base.server.port, 8888);
    }
}
