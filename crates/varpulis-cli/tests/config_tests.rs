//! Coverage-focused tests for varpulis-cli: config.rs and rate_limit.rs modules.
//!
//! Exercises config parsing (YAML, TOML), defaults, merge behavior, example
//! generation, error handling, and rate limiter token bucket mechanics.

use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;
use varpulis_cli::config::*;
use varpulis_cli::rate_limit::*;

// =============================================================================
// Config defaults
// =============================================================================

#[test]
fn config_default_server_port() {
    let cfg = Config::default();
    assert_eq!(cfg.server.port, 9000);
}

#[test]
fn config_default_server_bind() {
    let cfg = Config::default();
    assert_eq!(cfg.server.bind, "127.0.0.1");
}

#[test]
fn config_default_metrics_disabled() {
    let cfg = Config::default();
    assert!(!cfg.server.metrics_enabled);
}

#[test]
fn config_default_metrics_port() {
    let cfg = Config::default();
    assert_eq!(cfg.server.metrics_port, 9090);
}

#[test]
fn config_default_no_query_file() {
    let cfg = Config::default();
    assert!(cfg.query_file.is_none());
}

#[test]
fn config_default_no_kafka() {
    let cfg = Config::default();
    assert!(cfg.kafka.is_none());
}

#[test]
fn config_default_no_tls() {
    let cfg = Config::default();
    assert!(cfg.tls.is_none());
}

#[test]
fn config_default_no_auth() {
    let cfg = Config::default();
    assert!(cfg.auth.is_none());
}

#[test]
fn config_default_no_webhook() {
    let cfg = Config::default();
    assert!(cfg.http_webhook.is_none());
}

#[test]
fn config_default_logging() {
    let cfg = Config::default();
    assert_eq!(cfg.logging.level, "info");
    assert_eq!(cfg.logging.format, "text");
    assert!(cfg.logging.timestamps);
}

#[test]
fn config_default_processing() {
    let cfg = Config::default();
    assert!(cfg.processing.workers.is_none());
    assert!(cfg.processing.partition_by.is_none());
}

#[test]
fn config_default_simulation() {
    let cfg = Config::default();
    assert!(!cfg.simulation.immediate);
    assert!(!cfg.simulation.preload);
    assert!(!cfg.simulation.verbose);
    assert!(cfg.simulation.events_file.is_none());
}

// =============================================================================
// Config YAML parsing
// =============================================================================

#[test]
fn config_yaml_full() {
    let yaml = r#"
query_file: /app/queries.vql
server:
  port: 8080
  bind: "0.0.0.0"
  metrics_enabled: true
  metrics_port: 9191
  workdir: /tmp/work
simulation:
  immediate: true
  preload: true
  verbose: true
  events_file: /data/events.csv
kafka:
  bootstrap_servers: "kafka:9092"
  consumer_group: "my-group"
  input_topic: "input"
  output_topic: "output"
  auto_commit: false
  auto_offset_reset: "earliest"
http_webhook:
  enabled: true
  port: 8888
  bind: "127.0.0.1"
  api_key: "secret"
  rate_limit: 500
  max_batch_size: 200
logging:
  level: debug
  format: json
  timestamps: false
processing:
  workers: 16
  partition_by: "device_id"
auth:
  api_key: "auth-key"
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    assert_eq!(cfg.query_file, Some(PathBuf::from("/app/queries.vql")));
    assert_eq!(cfg.server.port, 8080);
    assert_eq!(cfg.server.bind, "0.0.0.0");
    assert!(cfg.server.metrics_enabled);
    assert_eq!(cfg.server.metrics_port, 9191);
    assert_eq!(cfg.server.workdir, Some(PathBuf::from("/tmp/work")));
    assert!(cfg.simulation.immediate);
    assert!(cfg.simulation.preload);
    assert!(cfg.simulation.verbose);
    assert_eq!(
        cfg.simulation.events_file,
        Some(PathBuf::from("/data/events.csv"))
    );

    let kafka = cfg.kafka.unwrap();
    assert_eq!(kafka.bootstrap_servers, "kafka:9092");
    assert_eq!(kafka.consumer_group, Some("my-group".into()));
    assert_eq!(kafka.input_topic, Some("input".into()));
    assert_eq!(kafka.output_topic, Some("output".into()));
    assert!(!kafka.auto_commit);
    assert_eq!(kafka.auto_offset_reset, "earliest");

    let webhook = cfg.http_webhook.unwrap();
    assert!(webhook.enabled);
    assert_eq!(webhook.port, 8888);
    assert_eq!(webhook.bind, "127.0.0.1");
    assert_eq!(webhook.api_key, Some("secret".into()));
    assert_eq!(webhook.rate_limit, 500);
    assert_eq!(webhook.max_batch_size, 200);

    assert_eq!(cfg.logging.level, "debug");
    assert_eq!(cfg.logging.format, "json");
    assert!(!cfg.logging.timestamps);
    assert_eq!(cfg.processing.workers, Some(16));
    assert_eq!(cfg.processing.partition_by, Some("device_id".into()));
    assert_eq!(cfg.auth.unwrap().api_key, Some("auth-key".into()));
}

#[test]
fn config_yaml_minimal() {
    let yaml = "{}";
    let cfg = Config::from_yaml(yaml).unwrap();
    // All defaults should apply
    assert_eq!(cfg.server.port, 9000);
    assert_eq!(cfg.server.bind, "127.0.0.1");
}

#[test]
fn config_yaml_invalid() {
    let yaml = "not: [valid: yaml: {{";
    let result = Config::from_yaml(yaml);
    assert!(result.is_err());
    match result.unwrap_err() {
        ConfigError::ParseError(msg) => {
            assert!(!msg.is_empty(), "Parse error should have a message");
        }
        other => panic!("Expected ParseError, got: {:?}", other),
    }
}

// =============================================================================
// Config TOML parsing
// =============================================================================

#[test]
fn config_toml_full() {
    let toml = r#"
query_file = "/app/queries.vql"

[server]
port = 7070
bind = "0.0.0.0"
metrics_enabled = true
metrics_port = 9191
workdir = "/var/lib/varpulis"

[simulation]
immediate = true
preload = true
verbose = true
events_file = "/data/events.csv"

[kafka]
bootstrap_servers = "kafka:9092"
consumer_group = "group-1"
input_topic = "in"
output_topic = "out"
auto_commit = false
auto_offset_reset = "earliest"

[logging]
level = "warn"
format = "json"
timestamps = false

[processing]
workers = 8
partition_by = "region"
"#;
    let cfg = Config::from_toml(toml).unwrap();
    assert_eq!(cfg.query_file, Some(PathBuf::from("/app/queries.vql")));
    assert_eq!(cfg.server.port, 7070);
    assert_eq!(cfg.server.bind, "0.0.0.0");
    assert!(cfg.server.metrics_enabled);
    assert_eq!(cfg.server.metrics_port, 9191);
    assert!(cfg.simulation.immediate);
    assert!(cfg.simulation.preload);

    let kafka = cfg.kafka.unwrap();
    assert_eq!(kafka.bootstrap_servers, "kafka:9092");
    assert!(!kafka.auto_commit);

    assert_eq!(cfg.logging.level, "warn");
    assert_eq!(cfg.processing.workers, Some(8));
    assert_eq!(cfg.processing.partition_by, Some("region".into()));
}

#[test]
fn config_toml_minimal() {
    let toml = "";
    let cfg = Config::from_toml(toml).unwrap();
    assert_eq!(cfg.server.port, 9000);
}

#[test]
fn config_toml_invalid() {
    let toml = "[invalid\nnot toml at all {{{}}}";
    let result = Config::from_toml(toml);
    assert!(result.is_err());
    match result.unwrap_err() {
        ConfigError::ParseError(msg) => {
            assert!(!msg.is_empty());
        }
        other => panic!("Expected ParseError, got: {:?}", other),
    }
}

// =============================================================================
// Config file loading
// =============================================================================

#[test]
fn config_load_yaml_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    std::fs::write(
        &path,
        r#"
server:
  port: 7777
  bind: "0.0.0.0"
"#,
    )
    .unwrap();
    let cfg = Config::load(&path).unwrap();
    assert_eq!(cfg.server.port, 7777);
}

#[test]
fn config_load_yml_extension() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yml");
    std::fs::write(
        &path,
        r#"
server:
  port: 6666
"#,
    )
    .unwrap();
    let cfg = Config::load(&path).unwrap();
    assert_eq!(cfg.server.port, 6666);
}

#[test]
fn config_load_toml_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        r#"
[server]
port = 5555
bind = "0.0.0.0"
"#,
    )
    .unwrap();
    let cfg = Config::load(&path).unwrap();
    assert_eq!(cfg.server.port, 5555);
}

#[test]
fn config_load_unknown_extension_tries_yaml_then_toml() {
    let dir = tempfile::tempdir().unwrap();
    // Write valid YAML with .conf extension
    let path = dir.path().join("config.conf");
    std::fs::write(
        &path,
        r#"
server:
  port: 4444
"#,
    )
    .unwrap();
    let cfg = Config::load(&path).unwrap();
    assert_eq!(cfg.server.port, 4444);
}

#[test]
fn config_load_nonexistent_file() {
    let result = Config::load("/nonexistent/path/config.yaml");
    assert!(result.is_err());
    match result.unwrap_err() {
        ConfigError::IoError(path, _msg) => {
            assert_eq!(path, PathBuf::from("/nonexistent/path/config.yaml"));
        }
        other => panic!("Expected IoError, got: {:?}", other),
    }
}

// =============================================================================
// Config merge
// =============================================================================

#[test]
fn config_merge_query_file() {
    let mut base = Config::default();
    let other = Config {
        query_file: Some(PathBuf::from("/new/query.vql")),
        ..Default::default()
    };
    base.merge(other);
    assert_eq!(base.query_file, Some(PathBuf::from("/new/query.vql")));
}

#[test]
fn config_merge_server_port() {
    let mut base = Config::default();
    let other = Config {
        server: ServerConfig {
            port: 8888,
            ..Default::default()
        },
        ..Default::default()
    };
    base.merge(other);
    assert_eq!(base.server.port, 8888);
}

#[test]
fn config_merge_server_bind() {
    let mut base = Config::default();
    let other = Config {
        server: ServerConfig {
            bind: "0.0.0.0".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    base.merge(other);
    assert_eq!(base.server.bind, "0.0.0.0");
}

#[test]
fn config_merge_metrics_enabled() {
    let mut base = Config::default();
    assert!(!base.server.metrics_enabled);
    let other = Config {
        server: ServerConfig {
            metrics_enabled: true,
            ..Default::default()
        },
        ..Default::default()
    };
    base.merge(other);
    assert!(base.server.metrics_enabled);
}

#[test]
fn config_merge_metrics_port() {
    let mut base = Config::default();
    let other = Config {
        server: ServerConfig {
            metrics_port: 3333,
            ..Default::default()
        },
        ..Default::default()
    };
    base.merge(other);
    assert_eq!(base.server.metrics_port, 3333);
}

#[test]
fn config_merge_workdir() {
    let mut base = Config::default();
    let other = Config {
        server: ServerConfig {
            workdir: Some(PathBuf::from("/work")),
            ..Default::default()
        },
        ..Default::default()
    };
    base.merge(other);
    assert_eq!(base.server.workdir, Some(PathBuf::from("/work")));
}

#[test]
fn config_merge_processing() {
    let mut base = Config::default();
    let other = Config {
        processing: ProcessingConfig {
            workers: Some(12),
            partition_by: Some("user_id".into()),
        },
        ..Default::default()
    };
    base.merge(other);
    assert_eq!(base.processing.workers, Some(12));
    assert_eq!(base.processing.partition_by, Some("user_id".into()));
}

#[test]
fn config_merge_kafka() {
    let mut base = Config::default();
    assert!(base.kafka.is_none());
    let other = Config {
        kafka: Some(KafkaConfig {
            bootstrap_servers: "kafka:9092".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    base.merge(other);
    assert!(base.kafka.is_some());
    assert_eq!(base.kafka.unwrap().bootstrap_servers, "kafka:9092");
}

#[test]
fn config_merge_webhook() {
    let mut base = Config::default();
    let other = Config {
        http_webhook: Some(HttpWebhookConfig {
            enabled: true,
            port: 9999,
            ..Default::default()
        }),
        ..Default::default()
    };
    base.merge(other);
    assert!(base.http_webhook.is_some());
    assert!(base.http_webhook.as_ref().unwrap().enabled);
}

#[test]
fn config_merge_tls() {
    let mut base = Config::default();
    let other = Config {
        tls: Some(TlsConfig {
            cert_file: PathBuf::from("/certs/cert.pem"),
            key_file: PathBuf::from("/certs/key.pem"),
        }),
        ..Default::default()
    };
    base.merge(other);
    assert!(base.tls.is_some());
    assert_eq!(
        base.tls.as_ref().unwrap().cert_file,
        PathBuf::from("/certs/cert.pem")
    );
}

#[test]
fn config_merge_auth() {
    let mut base = Config::default();
    let other = Config {
        auth: Some(AuthConfig {
            api_key: Some("new-key".into()),
        }),
        ..Default::default()
    };
    base.merge(other);
    assert!(base.auth.is_some());
    assert_eq!(base.auth.unwrap().api_key, Some("new-key".into()));
}

#[test]
fn config_merge_does_not_override_with_defaults() {
    let mut base = Config {
        query_file: Some(PathBuf::from("/original.vql")),
        server: ServerConfig {
            port: 8080,
            bind: "0.0.0.0".into(),
            metrics_enabled: true,
            metrics_port: 3000,
            workdir: Some(PathBuf::from("/original")),
        },
        ..Default::default()
    };
    // Merge with all defaults (should not override non-default values
    // except where default == default, but port/bind are "default" in the
    // other so should NOT override base)
    let other = Config::default();
    base.merge(other);
    // query_file should remain (other is None)
    assert_eq!(base.query_file, Some(PathBuf::from("/original.vql")));
    // port: other.port == default(9000) which IS the default, so no override
    assert_eq!(base.server.port, 8080);
    // bind: other.bind == default("127.0.0.1") which IS the default, so no override
    assert_eq!(base.server.bind, "0.0.0.0");
    // workdir: other.workdir == None, so no override
    assert_eq!(base.server.workdir, Some(PathBuf::from("/original")));
}

// =============================================================================
// Config example generation
// =============================================================================

#[test]
fn config_example_has_expected_values() {
    let ex = Config::example();
    assert!(ex.query_file.is_some());
    assert_eq!(ex.server.port, 9000);
    assert_eq!(ex.server.bind, "0.0.0.0");
    assert!(ex.server.metrics_enabled);
    assert!(ex.kafka.is_some());
    assert!(ex.http_webhook.is_some());
    assert!(ex.processing.workers.is_some());
    assert!(ex.auth.is_some());
}

#[test]
fn config_example_yaml_is_parseable() {
    let yaml = Config::example_yaml();
    assert!(!yaml.is_empty(), "Example YAML should not be empty");
    let parsed = Config::from_yaml(&yaml);
    assert!(
        parsed.is_ok(),
        "Example YAML should be parseable: {:?}",
        parsed.err()
    );
}

#[test]
fn config_example_toml_is_parseable() {
    let toml = Config::example_toml();
    assert!(!toml.is_empty(), "Example TOML should not be empty");
    let parsed = Config::from_toml(&toml);
    assert!(
        parsed.is_ok(),
        "Example TOML should be parseable: {:?}",
        parsed.err()
    );
}

// =============================================================================
// Config serialization roundtrip
// =============================================================================

#[test]
fn config_yaml_roundtrip() {
    let original = Config::example();
    let yaml = serde_yml::to_string(&original).unwrap();
    let restored: Config = serde_yml::from_str(&yaml).unwrap();
    assert_eq!(restored.server.port, original.server.port);
    assert_eq!(restored.server.bind, original.server.bind);
    assert_eq!(restored.query_file, original.query_file);
    assert_eq!(restored.processing.workers, original.processing.workers);
}

#[test]
fn config_toml_roundtrip() {
    let original = Config::example();
    let toml_str = toml::to_string_pretty(&original).unwrap();
    let restored: Config = toml::from_str(&toml_str).unwrap();
    assert_eq!(restored.server.port, original.server.port);
    assert_eq!(restored.server.bind, original.server.bind);
    assert_eq!(restored.query_file, original.query_file);
}

// =============================================================================
// KafkaConfig defaults
// =============================================================================

#[test]
fn kafka_config_defaults() {
    let kc = KafkaConfig::default();
    assert_eq!(kc.bootstrap_servers, "localhost:9092");
    assert!(kc.consumer_group.is_none());
    assert!(kc.input_topic.is_none());
    assert!(kc.output_topic.is_none());
    assert!(kc.auto_commit);
    assert_eq!(kc.auto_offset_reset, "latest");
}

// =============================================================================
// HttpWebhookConfig defaults
// =============================================================================

#[test]
fn webhook_config_defaults() {
    let wc = HttpWebhookConfig::default();
    assert!(!wc.enabled);
    assert_eq!(wc.port, 8080);
    assert_eq!(wc.bind, "0.0.0.0");
    assert!(wc.api_key.is_none());
    assert_eq!(wc.rate_limit, 0);
    assert_eq!(wc.max_batch_size, 1000);
}

// =============================================================================
// ConfigError display
// =============================================================================

#[test]
fn config_error_io_display() {
    let err = ConfigError::IoError(PathBuf::from("/bad/path"), "file not found".into());
    let msg = err.to_string();
    assert!(msg.contains("/bad/path"), "IoError display: {}", msg);
    assert!(msg.contains("file not found"), "IoError display: {}", msg);
}

#[test]
fn config_error_parse_display() {
    let err = ConfigError::ParseError("unexpected token at line 5".into());
    let msg = err.to_string();
    assert!(
        msg.contains("unexpected token at line 5"),
        "ParseError display: {}",
        msg
    );
}

// =============================================================================
// Rate limiter: RateLimitConfig
// =============================================================================

#[test]
fn rate_limit_config_disabled() {
    let cfg = RateLimitConfig::disabled();
    assert!(!cfg.enabled);
    assert_eq!(cfg.requests_per_second, 0);
    assert_eq!(cfg.burst_size, 0);
}

#[test]
fn rate_limit_config_default_is_disabled() {
    let cfg = RateLimitConfig::default();
    assert!(!cfg.enabled);
}

#[test]
fn rate_limit_config_new() {
    let cfg = RateLimitConfig::new(100);
    assert!(cfg.enabled);
    assert_eq!(cfg.requests_per_second, 100);
    assert_eq!(cfg.burst_size, 200); // 2x
}

#[test]
fn rate_limit_config_with_burst() {
    let cfg = RateLimitConfig::with_burst(50, 75);
    assert!(cfg.enabled);
    assert_eq!(cfg.requests_per_second, 50);
    assert_eq!(cfg.burst_size, 75);
}

#[test]
fn rate_limit_config_new_overflow_protection() {
    // u32::MAX * 2 would overflow; saturating_mul should handle it
    let cfg = RateLimitConfig::new(u32::MAX);
    assert!(cfg.enabled);
    assert_eq!(cfg.burst_size, u32::MAX); // saturating
}

// =============================================================================
// Rate limiter: RateLimiter async tests
// =============================================================================

#[tokio::test]
async fn rate_limiter_disabled_always_allows() {
    let limiter = RateLimiter::new(RateLimitConfig::disabled());
    let ip: IpAddr = "10.0.0.1".parse().unwrap();
    for _ in 0..100 {
        match limiter.check(ip).await {
            RateLimitResult::Allowed { remaining, .. } => {
                assert_eq!(remaining, u32::MAX);
            }
            RateLimitResult::Limited { .. } => panic!("Disabled limiter should never limit"),
        }
    }
}

#[tokio::test]
async fn rate_limiter_exhausts_burst() {
    let limiter = RateLimiter::new(RateLimitConfig::with_burst(10, 3));
    let ip: IpAddr = "10.0.0.1".parse().unwrap();

    // First 3 should pass
    for _ in 0..3 {
        match limiter.check(ip).await {
            RateLimitResult::Allowed { .. } => {}
            RateLimitResult::Limited { .. } => panic!("Should be allowed within burst"),
        }
    }
    // 4th should be limited
    match limiter.check(ip).await {
        RateLimitResult::Allowed { .. } => panic!("Should be limited after burst exhausted"),
        RateLimitResult::Limited { retry_after } => {
            assert!(
                retry_after.as_millis() > 0,
                "retry_after should be positive"
            );
        }
    }
}

#[tokio::test]
async fn rate_limiter_per_ip_isolation() {
    let limiter = RateLimiter::new(RateLimitConfig::with_burst(10, 1));
    let ip1: IpAddr = "10.0.0.1".parse().unwrap();
    let ip2: IpAddr = "10.0.0.2".parse().unwrap();

    // Exhaust ip1
    limiter.check(ip1).await;
    match limiter.check(ip1).await {
        RateLimitResult::Allowed { .. } => panic!("ip1 should be limited"),
        RateLimitResult::Limited { .. } => {}
    }

    // ip2 should still work
    match limiter.check(ip2).await {
        RateLimitResult::Allowed { .. } => {}
        RateLimitResult::Limited { .. } => panic!("ip2 should not be limited"),
    }
}

#[tokio::test]
async fn rate_limiter_client_count() {
    let limiter = RateLimiter::new(RateLimitConfig::new(10));
    assert_eq!(limiter.client_count().await, 0);

    let ip1: IpAddr = "10.0.0.1".parse().unwrap();
    let ip2: IpAddr = "10.0.0.2".parse().unwrap();
    limiter.check(ip1).await;
    assert_eq!(limiter.client_count().await, 1);
    limiter.check(ip2).await;
    assert_eq!(limiter.client_count().await, 2);
}

#[tokio::test]
async fn rate_limiter_cleanup_removes_stale() {
    let limiter = RateLimiter::new(RateLimitConfig::new(10));
    let ip: IpAddr = "10.0.0.1".parse().unwrap();
    limiter.check(ip).await;
    assert_eq!(limiter.client_count().await, 1);

    // Cleanup with extremely short max_age should remove the entry
    limiter.cleanup(Duration::from_nanos(1)).await;
    assert_eq!(limiter.client_count().await, 0);
}

#[tokio::test]
async fn rate_limiter_cleanup_keeps_recent() {
    let limiter = RateLimiter::new(RateLimitConfig::new(10));
    let ip: IpAddr = "10.0.0.1".parse().unwrap();
    limiter.check(ip).await;

    // Cleanup with long max_age should keep the entry
    limiter.cleanup(Duration::from_secs(3600)).await;
    assert_eq!(limiter.client_count().await, 1);
}

#[tokio::test]
async fn rate_limiter_ipv6() {
    let limiter = RateLimiter::new(RateLimitConfig::with_burst(10, 2));
    let ip: IpAddr = "::1".parse().unwrap();

    match limiter.check(ip).await {
        RateLimitResult::Allowed { .. } => {}
        RateLimitResult::Limited { .. } => panic!("IPv6 should work"),
    }
}

// =============================================================================
// Rate limiter: handle_rate_limit_rejection
// =============================================================================

#[test]
fn handle_rate_limit_rejection_with_matching_rejection() {
    let rejection = warp::reject::custom(RateLimitRejection {
        retry_after_secs: 5,
    });
    let result = handle_rate_limit_rejection(&rejection);
    assert!(result.is_some(), "Should handle RateLimitRejection");
}

#[test]
fn handle_rate_limit_rejection_with_non_matching() {
    let rejection = warp::reject::not_found();
    let result = handle_rate_limit_rejection(&rejection);
    assert!(
        result.is_none(),
        "Should return None for non-rate-limit rejections"
    );
}
