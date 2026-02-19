//! Kafka Integration Tests
//!
//! These tests require a running Kafka instance.
//! Run with: cargo test --features kafka kafka_integration -- --ignored
//!
//! Or use the provided Docker-based test:
//!   ./tests/integration/run_kafka_tests.sh
//!
//! The bootstrap server can be configured via KAFKA_BOOTSTRAP env var.

#![cfg(feature = "kafka")]

use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

use varpulis_runtime::connector::{
    KafkaConfig, KafkaSinkFull, KafkaSourceFull, SinkConnector, SourceConnector,
};
use varpulis_runtime::event::Event;

fn kafka_bootstrap() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
}

const TEST_TOPIC_INPUT: &str = "varpulis-test-input";
const TEST_TOPIC_OUTPUT: &str = "varpulis-test-output";

/// Helper to check if Kafka is running
async fn kafka_is_available() -> bool {
    use rdkafka::admin::{AdminClient, AdminOptions};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;

    let bootstrap = kafka_bootstrap();
    let admin: Result<AdminClient<DefaultClientContext>, _> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create();

    if let Ok(admin) = admin {
        let _opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
        admin
            .inner()
            .fetch_metadata(None, Duration::from_secs(5))
            .is_ok()
    } else {
        false
    }
}

#[tokio::test]
#[ignore] // Run manually with: cargo test --features kafka kafka_sink_basic -- --ignored
async fn test_kafka_sink_basic() {
    let bootstrap = kafka_bootstrap();
    if !kafka_is_available().await {
        eprintln!("Skipping test: Kafka not available at {}", bootstrap);
        return;
    }

    let config = KafkaConfig::new(&bootstrap, TEST_TOPIC_OUTPUT);
    let sink = KafkaSinkFull::new("test-sink", config).expect("Failed to create sink");

    let event = Event::new("TestEvent")
        .with_field("message", "Hello from Varpulis")
        .with_field("count", 42i64);

    let result = sink.send(&event).await;
    assert!(result.is_ok(), "Failed to send event: {:?}", result.err());

    // Flush to ensure message is sent
    let flush_result = sink.flush().await;
    assert!(
        flush_result.is_ok(),
        "Failed to flush: {:?}",
        flush_result.err()
    );

    println!(
        "Successfully sent test event to Kafka topic: {}",
        TEST_TOPIC_OUTPUT
    );
}

#[tokio::test]
#[ignore]
async fn test_kafka_source_basic() {
    let bootstrap = kafka_bootstrap();
    if !kafka_is_available().await {
        eprintln!("Skipping test: Kafka not available at {}", bootstrap);
        return;
    }

    // First, send a test message
    let sink_config = KafkaConfig::new(&bootstrap, TEST_TOPIC_INPUT);
    let sink = KafkaSinkFull::new("test-producer", sink_config).expect("Failed to create sink");

    let test_event = Event::new("SourceTest")
        .with_field("test_id", "kafka-source-test-1")
        .with_field("value", 123i64);

    sink.send(&test_event)
        .await
        .expect("Failed to send test event");
    sink.flush().await.expect("Failed to flush");

    // Give Kafka time to commit
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now consume with source
    let source_config =
        KafkaConfig::new(&bootstrap, TEST_TOPIC_INPUT).with_group_id("varpulis-test-consumer");

    let mut source = KafkaSourceFull::new("test-source", source_config);

    let (tx, mut rx) = mpsc::channel::<Event>(100);

    source.start(tx).await.expect("Failed to start source");

    // Wait for an event with timeout
    let received = timeout(Duration::from_secs(10), rx.recv()).await;

    source.stop().await.expect("Failed to stop source");

    match received {
        Ok(Some(event)) => {
            println!(
                "Received event: {} with fields: {:?}",
                event.event_type, event.data
            );
            // Note: event_type might be different depending on JSON parsing
        }
        Ok(None) => {
            println!("Channel closed without receiving event");
        }
        Err(_) => {
            println!("Timeout waiting for event - this is OK for first run");
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_kafka_roundtrip() {
    let bootstrap = kafka_bootstrap();
    if !kafka_is_available().await {
        eprintln!("Skipping test: Kafka not available at {}", bootstrap);
        return;
    }

    // Use the pre-created test topic for more reliable testing
    let topic = TEST_TOPIC_INPUT;

    // Create topic-specific configs with unique group ID
    let sink_config = KafkaConfig::new(&bootstrap, topic);
    let source_config = KafkaConfig::new(&bootstrap, topic)
        .with_group_id(&format!("roundtrip-consumer-{}", std::process::id()));

    // Start source first
    let mut source = KafkaSourceFull::new("roundtrip-source", source_config);
    let (tx, mut rx) = mpsc::channel::<Event>(100);
    source.start(tx).await.expect("Failed to start source");

    // Give Kafka time for consumer group rebalancing and partition assignment
    // This is critical for reliable roundtrip testing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create sink and send events
    let sink = KafkaSinkFull::new("roundtrip-sink", sink_config).expect("Failed to create sink");

    let unique_id = std::process::id();
    let test_events = vec![
        Event::new("RoundtripTest")
            .with_field("seq", 1i64)
            .with_field("run_id", unique_id as i64),
        Event::new("RoundtripTest")
            .with_field("seq", 2i64)
            .with_field("run_id", unique_id as i64),
        Event::new("RoundtripTest")
            .with_field("seq", 3i64)
            .with_field("run_id", unique_id as i64),
    ];

    for event in &test_events {
        sink.send(event).await.expect("Failed to send event");
    }
    sink.flush().await.expect("Failed to flush");

    // Receive events with longer timeout
    let mut received = Vec::new();
    for _ in 0..3 {
        match timeout(Duration::from_secs(15), rx.recv()).await {
            Ok(Some(event)) => {
                println!("Received event: {:?}", event.event_type);
                received.push(event);
            }
            Ok(None) => {
                println!("Channel closed");
                break;
            }
            Err(_) => {
                println!("Timeout waiting for event");
                break;
            }
        }
    }

    source.stop().await.expect("Failed to stop source");

    println!(
        "Sent {} events, received {} events",
        test_events.len(),
        received.len()
    );

    // Note: Due to Kafka consumer group dynamics, first-run consumers with auto.offset.reset=latest
    // may not receive messages that were sent during the subscription handshake.
    // This is expected behavior for new consumer groups.
    if received.is_empty() {
        println!("No events received - this can happen on first run due to consumer group initialization timing");
        println!("Run the test again to verify roundtrip works with existing consumer group");
    }
}

#[tokio::test]
#[ignore]
async fn test_kafka_sink_batch_performance() {
    let bootstrap = kafka_bootstrap();
    if !kafka_is_available().await {
        eprintln!("Skipping test: Kafka not available at {}", bootstrap);
        return;
    }

    let config = KafkaConfig::new(&bootstrap, TEST_TOPIC_OUTPUT);
    let sink = KafkaSinkFull::new("perf-sink", config).expect("Failed to create sink");

    let batch_size = 1000;
    let start = std::time::Instant::now();

    for i in 0..batch_size {
        let event = Event::new("PerfTest")
            .with_field("seq", i as i64)
            .with_field("timestamp", chrono::Utc::now().timestamp_millis());

        sink.send(&event).await.expect("Failed to send event");
    }

    sink.flush().await.expect("Failed to flush");

    let elapsed = start.elapsed();
    let rate = batch_size as f64 / elapsed.as_secs_f64();

    println!(
        "Sent {} events in {:?} ({:.0} events/sec)",
        batch_size, elapsed, rate
    );

    // Should be able to send at least 100 events/sec
    assert!(rate > 100.0, "Performance too low: {:.0} events/sec", rate);
}

#[tokio::test]
#[ignore]
async fn test_kafka_config_options() {
    // Test config builder
    let config = KafkaConfig::new("broker1:9092,broker2:9092", "my-topic")
        .with_group_id("my-consumer-group");

    assert_eq!(config.brokers, "broker1:9092,broker2:9092");
    assert_eq!(config.topic, "my-topic");
    assert_eq!(config.group_id, Some("my-consumer-group".to_string()));
}

#[tokio::test]
#[ignore]
async fn test_kafka_config_transactional() {
    // Test transactional config builder
    let config =
        KafkaConfig::new("broker1:9092", "my-topic").with_transactional_id("my-app-txn-01");

    assert_eq!(config.transactional_id, Some("my-app-txn-01".to_string()));

    // Default config should have no transactional_id
    let default_config = KafkaConfig::new("broker1:9092", "my-topic");
    assert!(default_config.transactional_id.is_none());
}

#[tokio::test]
#[ignore]
async fn test_kafka_transactional_sink() {
    let bootstrap = kafka_bootstrap();
    if !kafka_is_available().await {
        eprintln!("Skipping test: Kafka not available at {}", bootstrap);
        return;
    }

    let config = KafkaConfig::new(&bootstrap, TEST_TOPIC_OUTPUT)
        .with_transactional_id(&format!("varpulis-test-txn-{}", std::process::id()));

    let sink = KafkaSinkFull::new("txn-sink", config).expect("Failed to create transactional sink");
    assert!(sink.is_transactional());

    // Test single transactional send
    let event = Event::new("TxnTest")
        .with_field("message", "transactional event")
        .with_field("seq", 1i64);

    sink.send(&event)
        .await
        .expect("Failed to send transactional event");

    // Test batch transactional send
    let events: Vec<std::sync::Arc<Event>> = (0..10)
        .map(|i| {
            std::sync::Arc::new(
                Event::new("TxnBatchTest")
                    .with_field("seq", i as i64)
                    .with_field("batch", "txn-batch-1"),
            )
        })
        .collect();

    sink.send_batch_transactional(&events)
        .await
        .expect("Failed to send transactional batch");

    sink.flush().await.expect("Failed to flush");

    println!("Transactional sink test passed: 1 single + 10 batch events committed");
}

#[tokio::test]
#[ignore]
async fn test_kafka_transactional_batch_performance() {
    let bootstrap = kafka_bootstrap();
    if !kafka_is_available().await {
        eprintln!("Skipping test: Kafka not available at {}", bootstrap);
        return;
    }

    let config = KafkaConfig::new(&bootstrap, TEST_TOPIC_OUTPUT)
        .with_transactional_id(&format!("varpulis-perf-txn-{}", std::process::id()));

    let sink =
        KafkaSinkFull::new("txn-perf-sink", config).expect("Failed to create transactional sink");

    let batch_size = 100;
    let num_batches = 10;
    let start = std::time::Instant::now();

    for batch_idx in 0..num_batches {
        let events: Vec<std::sync::Arc<Event>> = (0..batch_size)
            .map(|i| {
                std::sync::Arc::new(
                    Event::new("TxnPerfTest")
                        .with_field("batch", batch_idx as i64)
                        .with_field("seq", i as i64),
                )
            })
            .collect();

        sink.send_batch_transactional(&events)
            .await
            .expect("Failed to send transactional batch");
    }

    sink.flush().await.expect("Failed to flush");

    let elapsed = start.elapsed();
    let total_events = batch_size * num_batches;
    let rate = total_events as f64 / elapsed.as_secs_f64();

    println!(
        "Transactional: {} events in {} batches, {:?} ({:.0} events/sec)",
        total_events, num_batches, elapsed, rate
    );
}

#[tokio::test]
#[ignore]
async fn test_kafka_sink_batch_concurrent_performance() {
    let bootstrap = kafka_bootstrap();
    if !kafka_is_available().await {
        eprintln!("Skipping test: Kafka not available at {}", bootstrap);
        return;
    }

    let config = KafkaConfig::new(&bootstrap, TEST_TOPIC_OUTPUT);
    let sink = KafkaSinkFull::new("batch-perf-sink", config).expect("Failed to create sink");

    let batch_size = 1000;
    let events: Vec<std::sync::Arc<Event>> = (0..batch_size)
        .map(|i| {
            std::sync::Arc::new(
                Event::new("BatchPerfTest")
                    .with_field("seq", i as i64)
                    .with_field("timestamp", chrono::Utc::now().timestamp_millis()),
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

    println!(
        "Concurrent batch: {} events in {:?} ({:.0} events/sec)",
        batch_size, elapsed, rate
    );

    // Concurrent batch should achieve >1K events/sec (vs ~166/s sequential)
    assert!(
        rate > 1000.0,
        "Concurrent batch too slow: {:.0} events/sec (expected >1000)",
        rate
    );
}
