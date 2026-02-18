//! End-to-end integration tests for the NATS connector.
//!
//! These tests require a real `nats-server` running at `nats://localhost:4222`.
//! In CI this is provided by a `services: nats` container; locally run
//! `nats-server &` before executing.
//!
//! Each test uses a unique subject (UUID-based) to avoid cross-test interference.

#![cfg(feature = "nats")]

use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use futures::StreamExt;
use varpulis_runtime::connector::{
    ManagedConnector, ManagedNatsConnector, NatsConfig, NatsSink, NatsSource, SinkConnector,
    SourceConnector,
};
use varpulis_runtime::Event;

const NATS_URL: &str = "nats://localhost:4222";

/// Helper: connect a raw async-nats client for publishing/subscribing in tests.
async fn raw_client() -> async_nats::Client {
    async_nats::connect(NATS_URL)
        .await
        .expect("Failed to connect to nats-server — is it running on localhost:4222?")
}

/// Helper: unique subject per test invocation.
fn unique_subject(prefix: &str) -> String {
    format!("test.{}.{}", prefix, Uuid::new_v4())
}

// ============================================================================
// Test 1: NatsSource receives events published by a raw client
// ============================================================================

#[tokio::test]
async fn test_nats_source_receives_events() {
    let subject = unique_subject("source_recv");
    let config = NatsConfig::new(NATS_URL, &subject);
    let mut source = NatsSource::new("test-source", config);

    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();

    // Give subscriber time to establish
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish 3 events via raw client
    let client = raw_client().await;
    for i in 0..3 {
        let payload = format!(
            r#"{{"event_type":"Sensor","reading":{i},"location":"zone-a"}}"#,
            i = i
        );
        client
            .publish(subject.clone(), payload.into())
            .await
            .unwrap();
    }
    client.flush().await.unwrap();

    // Receive and verify
    for i in 0..3 {
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout waiting for event")
            .expect("channel closed");
        assert_eq!(event.event_type.as_ref(), "Sensor");
        assert_eq!(event.get_int("reading"), Some(i));
        assert_eq!(event.get_str("location"), Some("zone-a"));
    }

    source.stop().await.unwrap();
    assert!(!source.is_running());
}

// ============================================================================
// Test 2: NatsSink publishes events that a raw subscriber receives
// ============================================================================

#[tokio::test]
async fn test_nats_sink_publishes_events() {
    let subject = unique_subject("sink_pub");
    let config = NatsConfig::new(NATS_URL, &subject);
    let mut sink = NatsSink::new("test-sink", config);
    sink.connect().await.unwrap();

    // Subscribe via raw client before publishing
    let client = raw_client().await;
    let mut sub = client.subscribe(subject.clone()).await.unwrap();
    // Wait for subscription to propagate to nats-server
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send events through the sink
    for i in 0..3 {
        let event = Event::new("Alert")
            .with_field("severity", i as i64)
            .with_field("msg", "test alert");
        sink.send(&event).await.unwrap();
    }
    sink.flush().await.unwrap();

    // Verify on raw subscriber
    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("timeout")
            .expect("subscription ended");
        let payload: serde_json::Value =
            serde_json::from_slice(&msg.payload).expect("invalid JSON");
        assert_eq!(payload["event_type"], "Alert");
        assert_eq!(payload["severity"], i);
        assert_eq!(payload["msg"], "test alert");
    }
}

// ============================================================================
// Test 3: Roundtrip — Source ← Sink on the same subject
// ============================================================================

#[tokio::test]
async fn test_nats_source_sink_roundtrip() {
    let subject = unique_subject("roundtrip");

    // Start source
    let src_config = NatsConfig::new(NATS_URL, &subject);
    let mut source = NatsSource::new("rt-source", src_config);
    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create sink
    let sink_config = NatsConfig::new(NATS_URL, &subject);
    let mut sink = NatsSink::new("rt-sink", sink_config);
    sink.connect().await.unwrap();

    // Send through sink
    let event = Event::new("Temp")
        .with_field("value", 42.5)
        .with_field("unit", "C");
    sink.send(&event).await.unwrap();
    sink.flush().await.unwrap();

    // Receive via source
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(received.event_type.as_ref(), "Temp");
    assert_eq!(received.get_float("value"), Some(42.5));
    assert_eq!(received.get_str("unit"), Some("C"));

    source.stop().await.unwrap();
}

// ============================================================================
// Test 4: JSON parsing variants — flat vs nested `data` envelope
// ============================================================================

#[tokio::test]
async fn test_nats_source_json_parsing_variants() {
    let subject = unique_subject("json_variants");
    let config = NatsConfig::new(NATS_URL, &subject);
    let mut source = NatsSource::new("json-src", config);
    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = raw_client().await;

    // Variant A: flat payload
    let flat = r#"{"event_type":"Login","user":"alice","ip":"10.0.0.1"}"#;
    client.publish(subject.clone(), flat.into()).await.unwrap();

    // Variant B: nested `data` envelope
    let nested = r#"{"event_type":"Logout","data":{"user":"bob","ip":"10.0.0.2"}}"#;
    client
        .publish(subject.clone(), nested.into())
        .await
        .unwrap();

    client.flush().await.unwrap();

    // Verify flat
    let ev1 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(ev1.event_type.as_ref(), "Login");
    assert_eq!(ev1.get_str("user"), Some("alice"));
    assert_eq!(ev1.get_str("ip"), Some("10.0.0.1"));

    // Verify nested
    let ev2 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(ev2.event_type.as_ref(), "Logout");
    assert_eq!(ev2.get_str("user"), Some("bob"));
    assert_eq!(ev2.get_str("ip"), Some("10.0.0.2"));

    source.stop().await.unwrap();
}

// ============================================================================
// Test 5: Subject-based event_type fallback (no event_type field in JSON)
// ============================================================================

#[tokio::test]
async fn test_nats_source_subject_fallback_event_type() {
    let base = unique_subject("fallback");
    let subject = format!("{base}.Temperature");
    let config = NatsConfig::new(NATS_URL, &subject);
    let mut source = NatsSource::new("fallback-src", config);
    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = raw_client().await;

    // JSON without event_type — should fall back to last subject segment
    let payload = r#"{"sensor_id":"s1","value":23.4}"#;
    client
        .publish(subject.clone(), payload.into())
        .await
        .unwrap();
    client.flush().await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(event.event_type.as_ref(), "Temperature");
    assert_eq!(event.get_float("value"), Some(23.4));
    assert_eq!(event.get_str("sensor_id"), Some("s1"));

    source.stop().await.unwrap();
}

// ============================================================================
// Test 6: ManagedNatsConnector — start_source + create_sink end-to-end
// ============================================================================

#[tokio::test]
async fn test_managed_nats_source_and_sink() {
    let subject = unique_subject("managed");
    let config = NatsConfig::new(NATS_URL, &subject);
    let mut managed = ManagedNatsConnector::new("mgd", config);

    // Start source
    let (tx, mut rx) = mpsc::channel(64);
    managed
        .start_source(&subject, tx, &HashMap::new())
        .await
        .unwrap();

    // Create sink (connection already established by start_source)
    let sink = managed.create_sink(&subject, &HashMap::new()).unwrap();

    // Send through managed sink
    let event = Event::new("ManagedEvt").with_field("key", "val");
    sink.send(&event).await.unwrap();
    sink.flush().await.unwrap();

    // Receive via managed source
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(received.event_type.as_ref(), "ManagedEvt");
    assert_eq!(received.get_str("key"), Some("val"));

    // Verify health
    let health = managed.health();
    assert!(health.connected);
    assert!(health.messages_received >= 1);

    managed.shutdown().await.unwrap();
}

// ============================================================================
// Test 7: Queue group — two sources share load (no duplicates)
// ============================================================================

#[tokio::test]
async fn test_nats_source_queue_group() {
    let subject = unique_subject("qgroup");
    let queue = "test-workers";

    let config1 = NatsConfig::new(NATS_URL, &subject).with_queue_group(queue);
    let config2 = NatsConfig::new(NATS_URL, &subject).with_queue_group(queue);
    let mut source1 = NatsSource::new("qg-1", config1);
    let mut source2 = NatsSource::new("qg-2", config2);

    let (tx1, mut rx1) = mpsc::channel(256);
    let (tx2, mut rx2) = mpsc::channel(256);
    source1.start(tx1).await.unwrap();
    source2.start(tx2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let n = 50;
    let client = raw_client().await;
    for i in 0..n {
        let payload = format!(r#"{{"event_type":"QEvt","seq":{i}}}"#, i = i);
        client
            .publish(subject.clone(), payload.into())
            .await
            .unwrap();
    }
    client.flush().await.unwrap();

    // Drain both receivers with a short timeout
    let mut count1 = 0u64;
    let mut count2 = 0u64;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            Some(_) = rx1.recv() => count1 += 1,
            Some(_) = rx2.recv() => count2 += 1,
            _ = tokio::time::sleep_until(deadline) => break,
        }
        if count1 + count2 == n {
            break;
        }
    }

    // Both should have received some, total == n
    assert_eq!(count1 + count2, n, "total events must equal {n}");
    // With queue group, load should be distributed (both > 0).
    assert!(count1 > 0, "source1 should have received some events");
    assert!(count2 > 0, "source2 should have received some events");

    source1.stop().await.unwrap();
    source2.stop().await.unwrap();
}
