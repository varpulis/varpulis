//! Redis Streams integration tests.
//!
//! These tests require a running Redis instance and the `redis` feature.
//! All tests are marked `#[ignore]` so they only run explicitly via:
//!   REDIS_URL=redis://localhost:6379 cargo test -p varpulis-runtime --features redis redis_stream_integration -- --ignored
//!
//! Or via Docker Compose:
//!   ./tests/integration/run_redis_tests.sh

#![cfg(feature = "redis")]

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;
use varpulis_runtime::connector::{
    RedisStreamConfig, RedisStreamSink, RedisStreamSource, SinkConnector, SourceConnector,
};
use varpulis_runtime::event::Event;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

fn unique_stream() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("test-stream-{:x}", ts)
}

#[tokio::test]
#[ignore]
async fn test_redis_stream_source_xreadgroup() {
    let url = redis_url();
    let stream_key = unique_stream();

    // First, XADD events externally using a sink
    let sink_config = RedisStreamConfig::new(&url, &stream_key);
    let sink = RedisStreamSink::new("redis-producer", sink_config)
        .await
        .expect("RedisStreamSink creation failed");

    for i in 0..10 {
        let event = Event::new("TestEvent")
            .with_field("seq", i as f64)
            .with_field("data", format!("payload-{}", i));
        sink.send(&event).await.expect("XADD failed");
    }

    // Now consume with a source using consumer group
    let source_config = RedisStreamConfig::new(&url, &stream_key)
        .with_group("test-group")
        .with_consumer("consumer-0");
    let mut source = RedisStreamSource::new("redis-src", source_config);
    let (tx, mut rx) = mpsc::channel(100);
    source
        .start(tx)
        .await
        .expect("RedisStreamSource start failed");

    let mut received = 0;
    let deadline = timeout(Duration::from_secs(15), async {
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
        "Timed out: received {}/10 events",
        received
    );
}

#[tokio::test]
#[ignore]
async fn test_redis_stream_sink_xadd() {
    let url = redis_url();
    let stream_key = unique_stream();

    let sink_config = RedisStreamConfig::new(&url, &stream_key).with_max_len(1000);
    let sink = RedisStreamSink::new("redis-sink", sink_config)
        .await
        .expect("RedisStreamSink creation failed");

    // XADD 10 events
    for i in 0..10 {
        let event = Event::new("SinkTest")
            .with_field("index", i as f64)
            .with_field("message", format!("hello-{}", i));
        sink.send(&event).await.expect("XADD failed");
    }

    sink.flush().await.expect("flush failed");

    // Verify by reading back with a source
    let source_config = RedisStreamConfig::new(&url, &stream_key)
        .with_group("verify-group")
        .with_consumer("verify-0");
    let mut source = RedisStreamSource::new("redis-verify", source_config);
    let (tx, mut rx) = mpsc::channel(100);
    source.start(tx).await.expect("source start failed");

    let mut received = 0;
    let deadline = timeout(Duration::from_secs(10), async {
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
        "Timed out: received {}/10 events",
        received
    );
}

#[tokio::test]
#[ignore]
async fn test_redis_stream_roundtrip() {
    let url = redis_url();
    let input_stream = unique_stream();
    let output_stream = format!("{}-out", input_stream);

    // Create input events via sink
    let input_sink_config = RedisStreamConfig::new(&url, &input_stream);
    let input_sink = RedisStreamSink::new("input-sink", input_sink_config)
        .await
        .expect("input sink creation failed");

    for i in 0..5 {
        let event = Event::new("RawEvent").with_field("value", (i * 100) as f64);
        input_sink.send(&event).await.expect("input XADD failed");
    }

    // Start source
    let source_config = RedisStreamConfig::new(&url, &input_stream)
        .with_group("roundtrip-group")
        .with_consumer("rt-consumer");
    let mut source = RedisStreamSource::new("redis-in", source_config);
    let (source_tx, mut source_rx) = mpsc::channel(100);
    source.start(source_tx).await.expect("source start failed");

    // Create output sink
    let output_sink_config = RedisStreamConfig::new(&url, &output_stream);
    let output_sink = RedisStreamSink::new("redis-out", output_sink_config)
        .await
        .expect("output sink creation failed");

    // Process: read from input, transform, write to output
    let mut processed = 0;
    let _ = timeout(Duration::from_secs(15), async {
        while processed < 5 {
            if let Some(event) = source_rx.recv().await {
                let output = Event::new("ProcessedEvent")
                    .with_field("original", &*event.event_type)
                    .with_field("processed", true);
                output_sink.send(&output).await.expect("output send failed");
                processed += 1;
            }
        }
    })
    .await;

    source.stop().await.ok();
    assert_eq!(
        processed, 5,
        "Should have processed 5 events through roundtrip"
    );
}

#[tokio::test]
#[ignore]
async fn test_redis_pubsub_source_receives() {
    use varpulis_runtime::connector::{RedisConfig, RedisSource};

    let url = redis_url();
    let channel = format!("test-channel-{}", unique_stream());

    let config = RedisConfig::new(&url, &channel);
    let mut source = RedisSource::new("redis-pubsub-src", config);
    let (tx, mut rx) = mpsc::channel(100);
    source.start(tx).await.expect("RedisSource start failed");

    // Give subscriber time to connect
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish events using Redis PUBLISH (via a sink)
    use varpulis_runtime::connector::RedisSink;
    let sink_config = RedisConfig::new(&url, &channel);
    let sink = RedisSink::new("redis-pub", sink_config)
        .await
        .expect("RedisSink creation failed");

    for i in 0..5 {
        let event = Event::new("PubSubEvent").with_field("seq", i as f64);
        sink.send(&event).await.expect("PUBLISH failed");
    }

    let mut received = 0;
    let deadline = timeout(Duration::from_secs(10), async {
        while received < 5 {
            if rx.recv().await.is_some() {
                received += 1;
            }
        }
    })
    .await;

    source.stop().await.ok();
    assert!(
        deadline.is_ok(),
        "Timed out: received {}/5 pubsub events",
        received
    );
}

#[tokio::test]
#[ignore]
async fn test_redis_pubsub_sink_publishes() {
    use varpulis_runtime::connector::{RedisConfig, RedisSink, RedisSource};

    let url = redis_url();
    let channel = format!("test-pub-{}", unique_stream());

    // Start subscriber first
    let sub_config = RedisConfig::new(&url, &channel);
    let mut source = RedisSource::new("redis-sub", sub_config);
    let (tx, mut rx) = mpsc::channel(100);
    source.start(tx).await.expect("subscriber start failed");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish via sink
    let pub_config = RedisConfig::new(&url, &channel);
    let sink = RedisSink::new("redis-publisher", pub_config)
        .await
        .expect("RedisSink creation failed");

    for i in 0..5 {
        let event = Event::new("PublishTest").with_field("idx", i as f64);
        sink.send(&event).await.expect("publish failed");
    }

    let mut received = 0;
    let deadline = timeout(Duration::from_secs(10), async {
        while received < 5 {
            if rx.recv().await.is_some() {
                received += 1;
            }
        }
    })
    .await;

    source.stop().await.ok();
    assert!(
        deadline.is_ok(),
        "Timed out: received {}/5 publish events",
        received
    );
}
