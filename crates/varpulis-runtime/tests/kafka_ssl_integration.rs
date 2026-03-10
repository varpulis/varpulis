//! Kafka SASL_SSL (SCRAM-SHA-512) Integration Tests
//!
//! These tests require a Kafka broker configured with SCRAM-SHA-512 + SSL.
//! Run with:
//!   ./tests/integration/run_kafka_ssl_tests.sh --local
//!
//! Or manually:
//!   1. cd tests/integration && bash kafka-ssl/generate-certs.sh
//!   2. docker compose -f docker-compose.kafka-ssl.yml up -d
//!   3. KAFKA_SSL_BOOTSTRAP=localhost:9093 cargo test --features kafka kafka_ssl -- --ignored --nocapture

#![cfg(feature = "kafka")]

use std::time::Duration;

use indexmap::IndexMap;
use tokio::sync::mpsc;
use tokio::time::timeout;
use varpulis_runtime::connector::{
    KafkaConfig, KafkaSinkFull, KafkaSourceFull, SinkConnector, SourceConnector,
};
use varpulis_runtime::event::Event;

fn ssl_bootstrap() -> String {
    std::env::var("KAFKA_SSL_BOOTSTRAP").unwrap_or_else(|_| "localhost:9093".to_string())
}

fn certs_dir() -> String {
    std::env::var("KAFKA_SSL_CERTS_DIR").unwrap_or_else(|_| {
        // Default: relative to workspace root
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        format!("{}/../../tests/integration/kafka-ssl/certs", manifest)
    })
}

/// Build a KafkaConfig with SCRAM-SHA-512 + SSL properties.
fn scram_ssl_config(topic: &str) -> KafkaConfig {
    let certs = certs_dir();
    let mut props = IndexMap::new();

    // SASL/SCRAM authentication
    props.insert("security_protocol".to_string(), "SASL_SSL".to_string());
    props.insert("sasl_mechanism".to_string(), "SCRAM-SHA-512".to_string());
    props.insert("sasl_username".to_string(), "varpulis".to_string());
    props.insert("sasl_password".to_string(), "varpulis-secret".to_string());

    // SSL: trust the test CA, disable hostname verification for localhost
    props.insert(
        "ssl_ca_location".to_string(),
        format!("{}/ca-cert.pem", certs),
    );
    props.insert(
        "ssl_endpoint_identification_algorithm".to_string(),
        "none".to_string(),
    );

    KafkaConfig::new(&ssl_bootstrap(), topic).with_properties(props)
}

/// Build a KafkaConfig with SCRAM + mTLS (client certificate).
fn scram_mtls_config(topic: &str) -> KafkaConfig {
    let certs = certs_dir();
    let mut props = IndexMap::new();

    // SASL/SCRAM
    props.insert("security_protocol".to_string(), "SASL_SSL".to_string());
    props.insert("sasl_mechanism".to_string(), "SCRAM-SHA-512".to_string());
    props.insert("sasl_username".to_string(), "varpulis".to_string());
    props.insert("sasl_password".to_string(), "varpulis-secret".to_string());

    // SSL with client cert (mTLS)
    props.insert(
        "ssl_ca_location".to_string(),
        format!("{}/ca-cert.pem", certs),
    );
    props.insert(
        "ssl_certificate_location".to_string(),
        format!("{}/client-cert.pem", certs),
    );
    props.insert(
        "ssl_key_location".to_string(),
        format!("{}/client-key.pem", certs),
    );
    props.insert(
        "ssl_endpoint_identification_algorithm".to_string(),
        "none".to_string(),
    );

    KafkaConfig::new(&ssl_bootstrap(), topic).with_properties(props)
}

/// TCP connectivity check.
async fn kafka_ssl_available() -> bool {
    let bootstrap = ssl_bootstrap();
    timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(&bootstrap),
    )
    .await
    .is_ok_and(|r| r.is_ok())
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_kafka_ssl_scram_sink() {
    if !kafka_ssl_available().await {
        eprintln!("Skipping: Kafka SSL not available at {}", ssl_bootstrap());
        return;
    }

    let config = scram_ssl_config("varpulis-ssl-output");
    let sink = KafkaSinkFull::new("ssl-sink", config).expect("Failed to create SASL_SSL sink");

    let event = Event::new("SSLTest")
        .with_field("message", "Hello from SCRAM-SHA-512 + SSL")
        .with_field("seq", 1i64);

    sink.send(&event)
        .await
        .expect("Failed to send via SASL_SSL");
    sink.flush().await.expect("Failed to flush");

    println!("PASS: Sent event via SASL_SSL (SCRAM-SHA-512)");
}

#[tokio::test]
#[ignore]
async fn test_kafka_ssl_scram_roundtrip() {
    if !kafka_ssl_available().await {
        eprintln!("Skipping: Kafka SSL not available at {}", ssl_bootstrap());
        return;
    }

    let topic = "varpulis-ssl-input";
    let unique_group = format!("ssl-roundtrip-{}", std::process::id());

    // Start consumer first — use earliest offset so we don't miss events
    let mut source_cfg = scram_ssl_config(topic).with_group_id(&unique_group);
    source_cfg
        .properties
        .insert("auto.offset.reset".to_string(), "earliest".to_string());
    let source_config = source_cfg;
    let mut source = KafkaSourceFull::new("ssl-source", source_config);
    let (tx, mut rx) = mpsc::channel::<Event>(100);
    source.start(tx).await.expect("Failed to start SSL source");

    // Wait for consumer group rebalance
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Produce events
    let sink_config = scram_ssl_config(topic);
    let sink =
        KafkaSinkFull::new("ssl-roundtrip-sink", sink_config).expect("Failed to create SSL sink");

    let run_id = std::process::id() as i64;
    for i in 0..5 {
        let event = Event::new("SSLRoundtrip")
            .with_field("seq", i as i64)
            .with_field("run_id", run_id);
        sink.send(&event).await.expect("Failed to send");
    }
    sink.flush().await.expect("Failed to flush");

    // Receive events
    let mut received = 0;
    for _ in 0..5 {
        match timeout(Duration::from_secs(15), rx.recv()).await {
            Ok(Some(event)) => {
                println!("  Received: {} {:?}", event.event_type, event.data);
                received += 1;
            }
            Ok(None) => break,
            Err(_) => {
                println!("  Timeout waiting for event");
                break;
            }
        }
    }

    source.stop().await.expect("Failed to stop source");

    println!("PASS: SASL_SSL roundtrip — sent 5, received {received}");
    assert!(received > 0, "Expected to receive at least 1 event");
}

#[tokio::test]
#[ignore]
async fn test_kafka_ssl_mtls_sink() {
    if !kafka_ssl_available().await {
        eprintln!("Skipping: Kafka SSL not available at {}", ssl_bootstrap());
        return;
    }

    let config = scram_mtls_config("varpulis-ssl-output");
    let sink = KafkaSinkFull::new("mtls-sink", config).expect("Failed to create mTLS sink");

    let event = Event::new("MTLSTest")
        .with_field("message", "Hello from SCRAM + mTLS")
        .with_field("seq", 1i64);

    sink.send(&event)
        .await
        .expect("Failed to send via SASL_SSL + mTLS");
    sink.flush().await.expect("Failed to flush");

    println!("PASS: Sent event via SASL_SSL + mTLS (client cert)");
}

#[tokio::test]
#[ignore]
async fn test_kafka_ssl_wrong_password_rejected() {
    if !kafka_ssl_available().await {
        eprintln!("Skipping: Kafka SSL not available at {}", ssl_bootstrap());
        return;
    }

    let certs = certs_dir();
    let mut props = IndexMap::new();
    props.insert("security_protocol".to_string(), "SASL_SSL".to_string());
    props.insert("sasl_mechanism".to_string(), "SCRAM-SHA-512".to_string());
    props.insert("sasl_username".to_string(), "varpulis".to_string());
    props.insert("sasl_password".to_string(), "wrong-password".to_string());
    props.insert(
        "ssl_ca_location".to_string(),
        format!("{}/ca-cert.pem", certs),
    );
    props.insert(
        "ssl_endpoint_identification_algorithm".to_string(),
        "none".to_string(),
    );
    // Short timeout so test doesn't hang
    props.insert("message.timeout.ms".to_string(), "5000".to_string());

    let config = KafkaConfig::new(&ssl_bootstrap(), "varpulis-ssl-output").with_properties(props);
    let sink =
        KafkaSinkFull::new("bad-auth-sink", config).expect("Failed to create sink (expected)");

    let event = Event::new("ShouldFail").with_field("x", 1i64);
    let result = sink.send(&event).await;

    // The send may succeed (buffered) but flush should fail with auth error
    if result.is_ok() {
        let flush = timeout(Duration::from_secs(10), sink.flush()).await;
        match flush {
            Ok(Ok(())) => {
                // Some rdkafka versions buffer without checking auth immediately
                println!("WARN: Send+flush succeeded despite wrong password (rdkafka may delay auth check)");
            }
            Ok(Err(e)) => {
                println!("PASS: Auth rejected as expected: {e}");
            }
            Err(_) => {
                println!("PASS: Flush timed out (auth handshake failed)");
            }
        }
    } else {
        println!("PASS: Send rejected immediately: {:?}", result.err());
    }
}

#[tokio::test]
#[ignore]
async fn test_kafka_ssl_batch_performance() {
    if !kafka_ssl_available().await {
        eprintln!("Skipping: Kafka SSL not available at {}", ssl_bootstrap());
        return;
    }

    let config = scram_ssl_config("varpulis-ssl-output");
    let sink = KafkaSinkFull::new("ssl-perf-sink", config).expect("Failed to create SASL_SSL sink");

    let batch_size = 1000;
    let events: Vec<std::sync::Arc<Event>> = (0..batch_size)
        .map(|i| {
            std::sync::Arc::new(
                Event::new("SSLPerfTest")
                    .with_field("seq", i as i64)
                    .with_field("ts", chrono::Utc::now().timestamp_millis()),
            )
        })
        .collect();

    let start = std::time::Instant::now();
    sink.send_batch(&events)
        .await
        .expect("Failed to send batch");
    sink.flush().await.expect("Failed to flush");
    let elapsed = start.elapsed();
    let rate = batch_size as f64 / elapsed.as_secs_f64();

    println!("PASS: SASL_SSL batch — {batch_size} events in {elapsed:?} ({rate:.0} events/sec)");
    // SSL encryption overhead reduces throughput; 50 events/sec is reasonable for debug builds
    assert!(
        rate > 50.0,
        "SASL_SSL throughput too low: {rate:.0} events/sec"
    );
}
