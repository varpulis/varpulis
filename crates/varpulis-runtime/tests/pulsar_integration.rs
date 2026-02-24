//! Pulsar integration tests.
//!
//! These tests require a running Pulsar instance and the `pulsar` feature.
//! All tests are marked `#[ignore]` so they only run explicitly via:
//!   PULSAR_URL=pulsar://localhost:6650 cargo test -p varpulis-runtime --features pulsar pulsar_integration -- --ignored
//!
//! Or via Docker Compose:
//!   ./tests/integration/run_pulsar_tests.sh

#![cfg(feature = "pulsar")]

use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use varpulis_runtime::connector::{
    PulsarConfig, PulsarSink, PulsarSource, SinkConnector, SourceConnector,
};
use varpulis_runtime::event::Event;

fn pulsar_url() -> String {
    std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".to_string())
}

#[tokio::test]
#[ignore]
async fn test_pulsar_source_receives_events() {
    let url = pulsar_url();
    let topic = format!("persistent://public/default/test-source-{}", uuid_suffix());

    // Start source
    let config = PulsarConfig::new(&url, &topic).with_subscription("test-sub");
    let mut source = PulsarSource::new("pulsar-src", config);
    let (tx, mut rx) = mpsc::channel(100);
    source.start(tx).await.expect("PulsarSource start failed");

    // Give source time to subscribe
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Produce events via a separate sink
    let sink_config = PulsarConfig::new(&url, &topic);
    let sink = PulsarSink::new("pulsar-producer", sink_config);

    for i in 0..10 {
        let event = Event::new("TestEvent").with_field("seq", i as f64);
        sink.send(&event).await.expect("PulsarSink send failed");
    }

    // Receive and verify
    let mut received = 0;
    let deadline = timeout(Duration::from_secs(30), async {
        while received < 10 {
            if rx.recv().await.is_some() {
                received += 1;
            }
        }
    })
    .await;

    source.stop().await.ok();
    assert!(
        deadline.is_ok(),
        "Timed out waiting for events, received {}/10",
        received
    );
    assert_eq!(received, 10);
}

#[tokio::test]
#[ignore]
async fn test_pulsar_sink_publishes_events() {
    let url = pulsar_url();
    let topic = format!("persistent://public/default/test-sink-{}", uuid_suffix());

    let config = PulsarConfig::new(&url, &topic);
    let sink = PulsarSink::new("pulsar-sink", config);

    // Send 10 events
    for i in 0..10 {
        let event = Event::new("TestEvent")
            .with_field("seq", i as f64)
            .with_field("data", format!("msg-{}", i));
        sink.send(&event).await.expect("PulsarSink send failed");
    }

    sink.flush().await.expect("flush failed");

    // Verify by consuming with a source
    let source_config = PulsarConfig::new(&url, &topic).with_subscription("verify-sub");
    let mut source = PulsarSource::new("pulsar-verify", source_config);
    let (tx, mut rx) = mpsc::channel(100);
    source.start(tx).await.expect("PulsarSource start failed");

    let mut received = 0;
    let deadline = timeout(Duration::from_secs(30), async {
        while received < 10 {
            if rx.recv().await.is_some() {
                received += 1;
            }
        }
    })
    .await;

    source.stop().await.ok();
    assert!(
        deadline.is_ok(),
        "Timed out waiting for events, received {}/10",
        received
    );
}

#[tokio::test]
#[ignore]
async fn test_pulsar_roundtrip() {
    let url = pulsar_url();
    let input_topic = format!("persistent://public/default/test-rt-in-{}", uuid_suffix());
    let output_topic = format!("persistent://public/default/test-rt-out-{}", uuid_suffix());

    // Start source from input topic
    let source_config = PulsarConfig::new(&url, &input_topic).with_subscription("roundtrip-sub");
    let mut source = PulsarSource::new("pulsar-in", source_config);
    let (source_tx, mut source_rx) = mpsc::channel(100);
    source.start(source_tx).await.expect("source start failed");

    // Create output sink
    let sink_config = PulsarConfig::new(&url, &output_topic);
    let sink = PulsarSink::new("pulsar-out", sink_config);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Produce input events
    let input_sink_config = PulsarConfig::new(&url, &input_topic);
    let input_sink = PulsarSink::new("pulsar-producer", input_sink_config);
    for i in 0..5 {
        let event = Event::new("InputEvent").with_field("value", (i * 10) as f64);
        input_sink.send(&event).await.expect("input send failed");
    }

    // Consume from source, transform, and forward to output sink
    let mut processed = 0;
    let _ = timeout(Duration::from_secs(15), async {
        while processed < 5 {
            if let Some(event) = source_rx.recv().await {
                // Transform: add a processed marker
                let output = Event::new("OutputEvent")
                    .with_field("original_type", event.event_type.as_str())
                    .with_field("processed", true);
                sink.send(&output).await.expect("output send failed");
                processed += 1;
            }
        }
    })
    .await;

    source.stop().await.ok();
    assert_eq!(
        processed, 5,
        "Should have processed 5 events through the roundtrip"
    );
}

fn uuid_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", ts)
}
