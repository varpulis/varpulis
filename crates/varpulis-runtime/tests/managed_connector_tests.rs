//! Managed Connector Integration Tests
//!
//! These tests require a running Mosquitto MQTT broker on port 11883.
//! Run with: cargo test -p varpulis-runtime --features mqtt managed_connector -- --ignored
//!
//! Or use the provided Docker-based test:
//!   ./tests/integration/run_managed_connector_tests.sh

#![cfg(feature = "mqtt")]

use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, MqttOptions, QoS};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use tokio::time::timeout;

use varpulis_runtime::connector::{ConnectorConfig, ManagedConnectorRegistry};
use varpulis_runtime::event::Event;
use varpulis_runtime::sink::Sink;

// ===========================================================================
// Test helpers
// ===========================================================================

fn mqtt_port() -> u16 {
    std::env::var("MQTT_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(11883)
}

/// Build a `FxHashMap<String, ConnectorConfig>` for one MQTT connector.
fn make_mqtt_connector_config(
    name: &str,
    port: u16,
    client_id: &str,
) -> FxHashMap<String, ConnectorConfig> {
    let config = ConnectorConfig::new("mqtt", "localhost")
        .with_property("port", &port.to_string())
        .with_property("client_id", client_id);

    let mut map = FxHashMap::default();
    map.insert(name.to_string(), config);
    map
}

/// Publish a single JSON message to an MQTT topic using an independent client.
async fn publish_json(port: u16, topic: &str, json: &serde_json::Value) {
    let client_id = format!("e2e-pub-{}", uuid());
    let mut opts = MqttOptions::new(&client_id, "localhost", port);
    opts.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(opts, 100);

    // Spawn eventloop driver
    let handle = tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });

    // Give the client time to connect
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = serde_json::to_vec(json).unwrap();
    client
        .publish(topic, QoS::AtLeastOnce, false, payload)
        .await
        .expect("publish_json failed");

    // Small delay to let the publish be sent on the wire
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _ = client.disconnect().await;
    handle.abort();
}

/// Subscribe to an MQTT topic and collect up to `count` messages or until `dur` elapses.
async fn subscribe_and_collect(
    port: u16,
    topic: &str,
    count: usize,
    dur: Duration,
) -> Vec<serde_json::Value> {
    let client_id = format!("e2e-sub-{}", uuid());
    let mut opts = MqttOptions::new(&client_id, "localhost", port);
    opts.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(opts, 100);
    client
        .subscribe(topic, QoS::AtLeastOnce)
        .await
        .expect("subscribe failed");

    let mut collected = Vec::new();
    let deadline = tokio::time::Instant::now() + dur;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() || collected.len() >= count {
            break;
        }
        match timeout(remaining, eventloop.poll()).await {
            Ok(Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)))) => {
                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&publish.payload) {
                    collected.push(val);
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) => break,
            Err(_) => break, // timeout
        }
    }

    let _ = client.disconnect().await;
    collected
}

/// Simple unique suffix for client IDs / topic namespacing.
fn uuid() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let n = CTR.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("{pid}-{n}")
}

/// Check if Mosquitto is reachable on the expected port.
async fn mqtt_is_available(port: u16) -> bool {
    let client_id = format!("e2e-probe-{}", uuid());
    let mut opts = MqttOptions::new(&client_id, "localhost", port);
    opts.set_keep_alive(Duration::from_secs(5));

    let (_client, mut eventloop) = AsyncClient::new(opts, 10);
    matches!(
        timeout(Duration::from_secs(3), eventloop.poll()).await,
        Ok(Ok(_))
    )
}

// ===========================================================================
// Test 1: Single source + single sink
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_managed_connector_single_source_single_sink() {
    let port = mqtt_port();
    if !mqtt_is_available(port).await {
        eprintln!("Skipping: Mosquitto not available on port {port}");
        return;
    }

    let configs = make_mqtt_connector_config("Broker", port, "e2e-test-1");
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    // Start source — subscribes to input topic
    let (tx, mut rx) = mpsc::channel(100);
    registry
        .start_source("Broker", "e2e/1/input", tx)
        .await
        .unwrap();

    // Create sink — publishes to output topic
    let sink: Arc<dyn Sink> = registry.create_sink("Broker", "e2e/1/output").unwrap();

    // Allow subscriptions to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // External subscriber on the output topic
    let output_collector = tokio::spawn(async move {
        subscribe_and_collect(port, "e2e/1/output", 1, Duration::from_secs(10)).await
    });

    // Publish a test event externally to the input topic
    let input_event = serde_json::json!({
        "event_type": "SensorReading",
        "temperature": 23.5,
        "sensor_id": "s1"
    });
    publish_json(port, "e2e/1/input", &input_event).await;

    // Receive from the source's mpsc channel
    let received = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for source event")
        .expect("channel closed");

    println!(
        "Source received: {} {:?}",
        received.event_type, received.data
    );
    assert_eq!(received.event_type.as_ref(), "SensorReading");

    // Send through sink
    let out_event = Event::new("Alert")
        .with_field("message", "high temp")
        .with_field("value", 42i64);
    sink.send(&out_event).await.unwrap();

    // Verify external subscriber got the output
    let collected = output_collector.await.unwrap();
    assert!(
        !collected.is_empty(),
        "Expected at least 1 message on output topic"
    );
    let first = &collected[0];
    assert_eq!(first["event_type"], "Alert");

    registry.shutdown().await;
    println!("test_managed_connector_single_source_single_sink PASSED");
}

// ===========================================================================
// Test 2: Two sources on the same connector, different topics
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_managed_connector_two_sources_same_connector() {
    let port = mqtt_port();
    if !mqtt_is_available(port).await {
        eprintln!("Skipping: Mosquitto not available on port {port}");
        return;
    }

    let configs = make_mqtt_connector_config("Broker", port, "e2e-test-2");
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    // Both sources share the same mpsc channel
    let (tx, mut rx) = mpsc::channel(100);
    registry
        .start_source("Broker", "e2e/2/temperature", tx.clone())
        .await
        .unwrap();
    registry
        .start_source("Broker", "e2e/2/humidity", tx)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish to both topics
    let temp_event = serde_json::json!({
        "event_type": "Temperature",
        "value": 25.0
    });
    let humid_event = serde_json::json!({
        "event_type": "Humidity",
        "value": 60.0
    });
    publish_json(port, "e2e/2/temperature", &temp_event).await;
    publish_json(port, "e2e/2/humidity", &humid_event).await;

    // Collect both events from the shared channel
    let mut received = Vec::new();
    for _ in 0..2 {
        match timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(ev)) => {
                println!("Received: {} {:?}", ev.event_type, ev.data);
                received.push(ev);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert_eq!(received.len(), 2, "Expected 2 events from 2 topics");
    let types: Vec<&str> = received.iter().map(|e| e.event_type.as_ref()).collect();
    assert!(types.contains(&"Temperature"), "Missing Temperature event");
    assert!(types.contains(&"Humidity"), "Missing Humidity event");

    registry.shutdown().await;
    println!("test_managed_connector_two_sources_same_connector PASSED");
}

// ===========================================================================
// Test 3: Two sinks on the same connector, different topics
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_managed_connector_two_sinks_same_connector() {
    let port = mqtt_port();
    if !mqtt_is_available(port).await {
        eprintln!("Skipping: Mosquitto not available on port {port}");
        return;
    }

    let configs = make_mqtt_connector_config("Broker", port, "e2e-test-3");
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    // Need a source to establish the connection (or sink-only would also work)
    let (tx, _rx) = mpsc::channel(100);
    registry
        .start_source("Broker", "e2e/3/dummy", tx)
        .await
        .unwrap();

    let sink_a: Arc<dyn Sink> = registry.create_sink("Broker", "e2e/3/alerts").unwrap();
    let sink_b: Arc<dyn Sink> = registry.create_sink("Broker", "e2e/3/metrics").unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // External subscribers on each output topic
    let alerts_collector = tokio::spawn(async move {
        subscribe_and_collect(port, "e2e/3/alerts", 1, Duration::from_secs(10)).await
    });
    let metrics_collector = tokio::spawn(async move {
        subscribe_and_collect(port, "e2e/3/metrics", 1, Duration::from_secs(10)).await
    });

    // Give subscribers time to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send to each sink
    let alert_event = Event::new("Alert").with_field("severity", "high");
    let metric_event = Event::new("Metric").with_field("cpu", 85i64);

    sink_a.send(&alert_event).await.unwrap();
    sink_b.send(&metric_event).await.unwrap();

    let alerts = alerts_collector.await.unwrap();
    let metrics = metrics_collector.await.unwrap();

    assert!(
        !alerts.is_empty(),
        "Expected at least 1 message on alerts topic"
    );
    assert!(
        !metrics.is_empty(),
        "Expected at least 1 message on metrics topic"
    );
    assert_eq!(alerts[0]["event_type"], "Alert");
    assert_eq!(metrics[0]["event_type"], "Metric");

    registry.shutdown().await;
    println!("test_managed_connector_two_sinks_same_connector PASSED");
}

// ===========================================================================
// Test 4: Multiple sources + multiple sinks (full routing)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_managed_connector_multiple_sources_and_sinks() {
    let port = mqtt_port();
    if !mqtt_is_available(port).await {
        eprintln!("Skipping: Mosquitto not available on port {port}");
        return;
    }

    let configs = make_mqtt_connector_config("Broker", port, "e2e-test-4");
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    // Two sources on different topics, shared channel
    let (tx, mut rx) = mpsc::channel(100);
    registry
        .start_source("Broker", "e2e/4/input_a", tx.clone())
        .await
        .unwrap();
    registry
        .start_source("Broker", "e2e/4/input_b", tx)
        .await
        .unwrap();

    // Two sinks on different topics
    let sink_x: Arc<dyn Sink> = registry.create_sink("Broker", "e2e/4/output_x").unwrap();
    let sink_y: Arc<dyn Sink> = registry.create_sink("Broker", "e2e/4/output_y").unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // External subscribers
    let out_x_collector = tokio::spawn(async move {
        subscribe_and_collect(port, "e2e/4/output_x", 1, Duration::from_secs(10)).await
    });
    let out_y_collector = tokio::spawn(async move {
        subscribe_and_collect(port, "e2e/4/output_y", 1, Duration::from_secs(10)).await
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish to both input topics
    publish_json(
        port,
        "e2e/4/input_a",
        &serde_json::json!({"event_type": "TypeA", "val": 1}),
    )
    .await;
    publish_json(
        port,
        "e2e/4/input_b",
        &serde_json::json!({"event_type": "TypeB", "val": 2}),
    )
    .await;

    // Collect 2 events from the shared source channel
    let mut source_events = Vec::new();
    for _ in 0..2 {
        match timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(ev)) => source_events.push(ev),
            _ => break,
        }
    }
    assert_eq!(source_events.len(), 2, "Expected 2 source events");

    // Route them to sinks
    sink_x
        .send(&Event::new("ResultX").with_field("from", "input_a"))
        .await
        .unwrap();
    sink_y
        .send(&Event::new("ResultY").with_field("from", "input_b"))
        .await
        .unwrap();

    let out_x = out_x_collector.await.unwrap();
    let out_y = out_y_collector.await.unwrap();

    assert!(!out_x.is_empty(), "Expected output on output_x topic");
    assert!(!out_y.is_empty(), "Expected output on output_y topic");
    assert_eq!(out_x[0]["event_type"], "ResultX");
    assert_eq!(out_y[0]["event_type"], "ResultY");

    registry.shutdown().await;
    println!("test_managed_connector_multiple_sources_and_sinks PASSED");
}

// ===========================================================================
// Test 5: Sink-only connector (no start_source, lazy connection)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_managed_connector_sink_only() {
    let port = mqtt_port();
    if !mqtt_is_available(port).await {
        eprintln!("Skipping: Mosquitto not available on port {port}");
        return;
    }

    let configs = make_mqtt_connector_config("Broker", port, "e2e-test-5");
    let mut registry = ManagedConnectorRegistry::from_configs(&configs).unwrap();

    // Only create a sink — no start_source()
    let sink: Arc<dyn Sink> = registry.create_sink("Broker", "e2e/5/output").unwrap();

    // External subscriber
    let collector = tokio::spawn(async move {
        subscribe_and_collect(port, "e2e/5/output", 1, Duration::from_secs(10)).await
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send through the sink
    let event = Event::new("SinkOnly").with_field("test", "lazy-connect");
    sink.send(&event).await.unwrap();

    let collected = collector.await.unwrap();
    assert!(
        !collected.is_empty(),
        "Expected at least 1 message from sink-only connector"
    );
    assert_eq!(collected[0]["event_type"], "SinkOnly");

    registry.shutdown().await;
    println!("test_managed_connector_sink_only PASSED");
}
