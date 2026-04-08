//! Throughput smoke test for the Kafka sink against a real Redpanda broker.
//!
//! This test exists because we repeatedly burned rebuild cycles trying to
//! diagnose "why is `varpulis run` bounded at ~150 eps on Kafka". The real
//! answer was found inside the engine loop, but it took ~30 min of full CLI
//! rebuilds to get there. Having a narrow test that exercises ONLY the
//! sink path lets us iterate in seconds.
//!
//! The test:
//!   1. builds a `ManagedKafkaConnector`
//!   2. creates a `KafkaSharedSink` via `create_sink`
//!   3. times how long it takes to push 50k events via `Sink::send`
//!
//! `#[ignore]` by default. Run with a live Redpanda:
//!
//! ```bash
//! docker compose -f benchmarks/arroyo-comparison/docker/docker-compose.yml up -d
//! cargo test -p varpulis-connector-kafka --test sink_throughput_smoke \
//!     --features varpulis-runtime/kafka -- --ignored --nocapture
//! ```
//!
//! The first event-per-second number is the **floor** — fire-and-forget
//! non-blocking enqueue via `producer.send_result` should sustain >50k
//! eps locally. If this number ever drops below ~10k eps that's a
//! regression back to the per-event broker round-trip we accidentally
//! shipped in commit 62c6d81.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use varpulis_connector_api::types::SinkConnector;
use varpulis_connector_api::ManagedConnector;
use varpulis_connector_kafka::{KafkaConfig, KafkaSink, ManagedKafkaConnector};
use varpulis_core::Event;

const BROKER: &str = "localhost:29092";
const TOPIC: &str = "sink-throughput-smoke";

async fn broker_available() -> bool {
    use tokio::net::TcpStream;
    tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(BROKER))
        .await
        .ok()
        .and_then(|r| r.ok())
        .is_some()
}

fn make_event(i: u64) -> Event {
    Event::new("Tick")
        .with_field("ts", i as i64)
        .with_field("symbol", "AAPL".to_string())
        .with_field("price", 42.5_f64)
        .with_field("volume", 1_000_i64)
}

/// `KafkaSharedSink::send` (the managed path) must sustain at least
/// ~10k eps of fire-and-forget enqueue against a local Redpanda broker.
/// Anything below that means we regressed to blocking-per-event delivery.
#[tokio::test]
#[ignore = "requires live Redpanda on localhost:29092"]
async fn kafka_shared_sink_fire_and_forget_is_fast() {
    if !broker_available().await {
        eprintln!("Redpanda not reachable at {BROKER} — skipping");
        return;
    }

    let cfg = KafkaConfig::new(BROKER, TOPIC);
    let mut managed = ManagedKafkaConnector::new("SmokeOut", cfg);

    let sink = managed
        .create_sink(TOPIC, &HashMap::new())
        .expect("create_sink should succeed");

    // Warm up: ~1000 events so the producer thread and connection are
    // hot before we start timing.
    for i in 0..1_000 {
        sink.send(&make_event(i)).await.unwrap();
    }
    sink.flush().await.unwrap();

    // Measured run.
    const N: u64 = 50_000;
    let start = Instant::now();
    for i in 0..N {
        sink.send(&make_event(i)).await.unwrap();
    }
    sink.flush().await.unwrap();
    let elapsed = start.elapsed();
    let eps = N as f64 / elapsed.as_secs_f64();

    eprintln!(
        "\nKafkaSharedSink::send  {} events in {:?}  ({:.0} eps, {:.1} µs/event)",
        N,
        elapsed,
        eps,
        elapsed.as_secs_f64() * 1e6 / N as f64
    );

    managed.shutdown().await.unwrap();

    assert!(
        eps > 10_000.0,
        "KafkaSharedSink::send regressed to blocking path: only {:.0} eps (expected > 10k)",
        eps
    );
}

/// `Sink::send_batch` with a fat batch (256 events per call) should be
/// even faster since it amortises per-event overhead. If it's slower
/// than `send`, something is wrong with the default send_batch impl or
/// we've introduced an accidental per-event await point.
#[tokio::test]
#[ignore = "requires live Redpanda on localhost:29092"]
async fn kafka_shared_sink_send_batch_matches_send() {
    if !broker_available().await {
        eprintln!("Redpanda not reachable at {BROKER} — skipping");
        return;
    }

    let cfg = KafkaConfig::new(BROKER, TOPIC);
    let mut managed = ManagedKafkaConnector::new("SmokeOut", cfg);
    let sink = managed
        .create_sink(TOPIC, &HashMap::new())
        .expect("create_sink should succeed");

    // Warm up
    for i in 0..1_000 {
        sink.send(&make_event(i)).await.unwrap();
    }
    sink.flush().await.unwrap();

    const N: u64 = 50_000;
    const BATCH: usize = 256;
    let start = Instant::now();
    let mut sent = 0u64;
    while sent < N {
        // Refresh per-call: this mimics the run-loop path where every
        // batch is fresh. `send_batch` takes `&[Arc<Event>]`, so wrap.
        let arced: Vec<Arc<Event>> = (0..BATCH as u64).map(|i| Arc::new(make_event(i))).collect();
        sink.send_batch(&arced).await.unwrap();
        sent += BATCH as u64;
    }
    sink.flush().await.unwrap();
    let elapsed = start.elapsed();
    let eps = sent as f64 / elapsed.as_secs_f64();

    eprintln!(
        "\nKafkaSharedSink::send_batch(256)  {} events in {:?}  ({:.0} eps)",
        sent, elapsed, eps
    );

    managed.shutdown().await.unwrap();

    assert!(
        eps > 10_000.0,
        "send_batch regressed: only {:.0} eps (expected > 10k)",
        eps
    );
}

/// Legacy `KafkaSink::send` (SinkConnector trait, used by the engine's
/// default sink factory when `inject_sink` is not called) must also
/// sustain ≥10k eps. Before commit 62c6d81 this path used the blocking
/// `producer.send(record, Duration::ZERO).await`, which bounded
/// throughput at one broker round-trip per event (~6 ms ≈ 150 eps).
/// The fix switched it to fire-and-forget `send_result` — same pattern
/// as the managed sink and as Arroyo.
#[tokio::test]
#[ignore = "requires live Redpanda on localhost:29092"]
async fn kafka_legacy_sink_fire_and_forget_is_fast() {
    if !broker_available().await {
        eprintln!("Redpanda not reachable at {BROKER} — skipping");
        return;
    }

    let cfg = KafkaConfig::new(BROKER, TOPIC);
    let sink = KafkaSink::new("LegacySmokeOut", cfg).expect("KafkaSink::new should succeed");

    // Warm up so the producer thread and connection are hot.
    for i in 0..1_000 {
        sink.send(&make_event(i)).await.unwrap();
    }

    const N: u64 = 50_000;
    let start = Instant::now();
    for i in 0..N {
        sink.send(&make_event(i)).await.unwrap();
    }
    let elapsed = start.elapsed();
    let eps = N as f64 / elapsed.as_secs_f64();

    eprintln!(
        "\nKafkaSink::send (legacy SinkConnector)  {} events in {:?}  ({:.0} eps, {:.1} µs/event)",
        N,
        elapsed,
        eps,
        elapsed.as_secs_f64() * 1e6 / N as f64
    );

    assert!(
        eps > 10_000.0,
        "KafkaSink::send regressed to blocking broker-ACK path: only {:.0} eps (expected > 10k)",
        eps
    );
}
