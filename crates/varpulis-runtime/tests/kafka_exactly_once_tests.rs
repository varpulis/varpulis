//! Kafka 2PC Crash Recovery Tests
//!
//! Verifies exactly-once delivery by simulating process crashes between
//! the phases of a two-phase commit (2PC) barrier:
//!
//! 1. **After prepare, before commit** — the Kafka transaction is flushed
//!    but not committed. On restart, `init_transactions` fences the old
//!    producer epoch and aborts the pending transaction. A `read_committed`
//!    consumer must see zero events from the aborted epoch.
//!
//! 2. **After commit, before offset-commit** — the Kafka transaction is
//!    committed (events visible) but the consumer group offsets were not
//!    updated. A `read_committed` consumer must see exactly the committed
//!    events — the commit is durable regardless of offset state.
//!
//! 3. **Clean multi-epoch 2PC** — multiple consecutive epochs each run a
//!    full begin → send → prepare → commit cycle. Verifies no gaps or
//!    duplicates across epoch boundaries.
//!
//! ## Running
//!
//! ```bash
//! # Start Kafka:
//! docker compose -f tests/integration/docker-compose.kafka.yml up -d zookeeper kafka kafka-setup
//! # Run tests:
//! cargo test -p varpulis-runtime --features kafka kafka_exactly_once -- --ignored --nocapture
//! ```
//!
//! ## Acceptance
//!
//! 3 scenarios × 3 runs each = 9 test executions. Every run must report
//! **0 duplicates** and **0 gaps**.

#![cfg(feature = "kafka")]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use varpulis_runtime::connector::{KafkaConfig, ManagedConnector, ManagedKafkaConnector};
use varpulis_runtime::event::Event;
use varpulis_runtime::Sink;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Events per epoch in each test run.
const EVENTS_PER_EPOCH: usize = 100;

/// Repetitions per scenario.
const RUNS: usize = 3;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bootstrap() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
}

async fn kafka_available() -> bool {
    let bs = bootstrap();
    tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(&bs))
        .await
        .is_ok_and(|r| r.is_ok())
}

/// Build a batch of events tagged with `run_id` and sequential `seq` numbers.
fn make_events(run_id: &str, start_seq: usize, count: usize) -> Vec<Event> {
    (start_seq..start_seq + count)
        .map(|i| {
            Event::new("EoTest")
                .with_field("run_id", run_id)
                .with_field("seq", i as i64)
        })
        .collect()
}

/// Create a transactional Kafka sink via [`ManagedKafkaConnector`].
///
/// Returns the connector (must stay alive while the sink is in use) and
/// the sink handle. `init_transactions` is called during `create_sink`.
fn create_txn_sink(
    name: &str,
    topic: &str,
    txn_id: &str,
) -> (ManagedKafkaConnector, Arc<dyn Sink>) {
    let config = KafkaConfig::new(&bootstrap(), topic).with_transactional_id(txn_id);
    let mut connector = ManagedKafkaConnector::new(name, config);
    let sink = connector
        .create_sink(topic, &HashMap::new())
        .expect("create_sink");
    (connector, sink)
}

/// Send all events through the sink.
async fn send_events(sink: &dyn Sink, events: &[Event]) {
    for event in events {
        sink.send(event).await.expect("sink send");
    }
}

/// Drain committed events from `topic` using `isolation.level=read_committed`.
///
/// Returns events collected until either `expected` events are found or
/// `timeout` expires, plus a 2-second straggler window.
async fn drain_committed(
    topic: &str,
    group: &str,
    expected: usize,
    timeout: Duration,
) -> Vec<Event> {
    let mut config = KafkaConfig::new(&bootstrap(), topic).with_group_id(group);
    config
        .properties
        .insert("isolation.level".to_string(), "read_committed".to_string());

    let mut connector = ManagedKafkaConnector::new("drain", config);
    let (tx, mut rx) = mpsc::channel::<Vec<Event>>(4096);

    let mut params = HashMap::new();
    params.insert("auto_offset_reset".to_string(), "earliest".to_string());

    connector
        .start_source(topic, tx, &params)
        .await
        .expect("drain start_source");

    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;

    while let Ok(Some(batch)) = tokio::time::timeout_at(deadline, rx.recv()).await {
        events.extend(batch);
        if expected > 0 && events.len() >= expected {
            break;
        }
    }

    // Straggler window — catch any late-arriving batches.
    let extra = tokio::time::Instant::now() + Duration::from_secs(2);
    while let Ok(Some(batch)) = tokio::time::timeout_at(extra, rx.recv()).await {
        events.extend(batch);
    }

    connector.shutdown().await.ok();
    events
}

/// Extract sequence numbers for a given `run_id` from consumed events.
fn extract_seqs(events: &[Event], run_id: &str) -> Vec<i64> {
    events
        .iter()
        .filter(|e| e.data.get("run_id").and_then(|v| v.as_str()) == Some(run_id))
        .filter_map(|e| e.data.get("seq").and_then(|v| v.as_int()))
        .collect()
}

/// Assert exactly-once: correct count, no duplicates, no gaps in 0..expected.
fn assert_exactly_once(seqs: &[i64], expected: usize, label: &str, run: usize) {
    assert_eq!(
        seqs.len(),
        expected,
        "[{label} run {run}] expected {expected} events, got {}",
        seqs.len()
    );

    let unique: HashSet<i64> = seqs.iter().copied().collect();
    assert_eq!(
        unique.len(),
        expected,
        "[{label} run {run}] {} duplicates detected",
        seqs.len() - unique.len()
    );

    for i in 0..expected as i64 {
        assert!(unique.contains(&i), "[{label} run {run}] missing seq {i}");
    }
}

// ===========================================================================
// Scenario 1 — Crash after prepare, before commit
// ===========================================================================

/// Simulates a crash after `prepare_commit` (flush to open Kafka txn) but
/// before `commit_transaction`.
///
/// On restart with the same `transactional.id`, `init_transactions` fences
/// the old producer epoch and aborts the pending transaction.
///
/// The test then re-sends the same events in a new epoch and commits.
///
/// Expectation: `read_committed` consumer sees exactly N events (from the
/// committed epoch), zero events from the aborted epoch.
#[tokio::test]
#[ignore]
async fn crash_after_prepare_before_commit() {
    if !kafka_available().await {
        eprintln!("Skipping: Kafka not available at {}", bootstrap());
        return;
    }

    let pid = std::process::id();

    for run in 0..RUNS {
        let topic = format!("eo-prepare-{pid}-{run}");
        let txn_id = format!("eo-txn-prepare-{pid}-{run}");
        let run_id = format!("prepare-{pid}-{run}");
        let events = make_events(&run_id, 0, EVENTS_PER_EPOCH);

        // ---- Epoch 1: produce + prepare, then "crash" (no commit) ----
        //
        // The sink flushes all records into the open Kafka transaction but
        // the process "dies" before commit_transaction is called.
        // Dropping the connector simulates the crash — the FutureProducer
        // is destroyed without committing or aborting.
        {
            let (_connector, sink) = create_txn_sink("crash", &topic, &txn_id);

            sink.begin_epoch(1).await.unwrap();
            send_events(&*sink, &events).await;
            sink.prepare_commit(1).await.unwrap();

            // ← DROP here: no commit, no abort, no shutdown.
        }

        // ---- Restart: new producer with same txn_id ----
        //
        // create_sink → ensure_producer → init_transactions. This fences
        // the old epoch and aborts the pending transaction from epoch 1.
        // Then we re-send and commit.
        {
            let (mut connector, sink) = create_txn_sink("recover", &topic, &txn_id);

            sink.begin_epoch(2).await.unwrap();
            send_events(&*sink, &events).await;
            sink.prepare_commit(2).await.unwrap();
            sink.commit(2).await.unwrap();

            connector.shutdown().await.ok();
        }

        // ---- Verify ----
        let group = format!("verify-prepare-{pid}-{run}");
        let received =
            drain_committed(&topic, &group, EVENTS_PER_EPOCH, Duration::from_secs(15)).await;
        let seqs = extract_seqs(&received, &run_id);
        assert_exactly_once(&seqs, EVENTS_PER_EPOCH, "crash_after_prepare", run);

        println!(
            "  [crash_after_prepare run {}/{}] PASS — {} events, 0 dups, 0 gaps",
            run + 1,
            RUNS,
            seqs.len()
        );
    }
}

// ===========================================================================
// Scenario 2 — Crash after commit, before offset-commit
// ===========================================================================

/// Simulates a crash after `commit_transaction` succeeds but before
/// consumer group offsets are committed.
///
/// The committed events are durable and visible to `read_committed`
/// consumers regardless of the offset state. The source offset loss is a
/// separate concern handled by engine checkpoints (not tested here).
///
/// Expectation: `read_committed` consumer sees exactly N committed events.
#[tokio::test]
#[ignore]
async fn crash_after_commit_before_offset_commit() {
    if !kafka_available().await {
        eprintln!("Skipping: Kafka not available at {}", bootstrap());
        return;
    }

    let pid = std::process::id();

    for run in 0..RUNS {
        let topic = format!("eo-commit-{pid}-{run}");
        let txn_id = format!("eo-txn-commit-{pid}-{run}");
        let run_id = format!("commit-{pid}-{run}");
        let events = make_events(&run_id, 0, EVENTS_PER_EPOCH);

        // ---- Full prepare + commit, then "crash" before offset commit ----
        {
            let (mut connector, sink) = create_txn_sink("commit-crash", &topic, &txn_id);

            sink.begin_epoch(1).await.unwrap();
            send_events(&*sink, &events).await;
            sink.prepare_commit(1).await.unwrap();
            sink.commit(1).await.unwrap();

            // Source offset commit (phase 4) would happen here. We skip it
            // to simulate a crash between phase 3 and phase 4.

            connector.shutdown().await.ok();
        }

        // ---- Verify: committed events are durable ----
        let group = format!("verify-commit-{pid}-{run}");
        let received =
            drain_committed(&topic, &group, EVENTS_PER_EPOCH, Duration::from_secs(15)).await;
        let seqs = extract_seqs(&received, &run_id);
        assert_exactly_once(&seqs, EVENTS_PER_EPOCH, "crash_after_commit", run);

        println!(
            "  [crash_after_commit run {}/{}] PASS — {} events, 0 dups, 0 gaps",
            run + 1,
            RUNS,
            seqs.len()
        );
    }
}

// ===========================================================================
// Scenario 3 — Full 2PC, clean multi-epoch
// ===========================================================================

/// Runs 3 consecutive 2PC epochs on the same transactional producer, each
/// committing a separate batch with sequential sequence numbers.
///
/// Verifies that all events across all epoch boundaries are visible with
/// no gaps or duplicates — the standard exactly-once happy path.
#[tokio::test]
#[ignore]
async fn full_2pc_clean_multi_epoch() {
    if !kafka_available().await {
        eprintln!("Skipping: Kafka not available at {}", bootstrap());
        return;
    }

    let pid = std::process::id();
    let num_epochs: usize = 3;
    let total_events = num_epochs * EVENTS_PER_EPOCH;

    for run in 0..RUNS {
        let topic = format!("eo-clean-{pid}-{run}");
        let txn_id = format!("eo-txn-clean-{pid}-{run}");
        let run_id = format!("clean-{pid}-{run}");

        {
            let (mut connector, sink) = create_txn_sink("clean", &topic, &txn_id);

            for epoch in 0..num_epochs {
                let start_seq = epoch * EVENTS_PER_EPOCH;
                let events = make_events(&run_id, start_seq, EVENTS_PER_EPOCH);

                sink.begin_epoch(epoch as u64 + 1).await.unwrap();
                send_events(&*sink, &events).await;
                sink.prepare_commit(epoch as u64 + 1).await.unwrap();
                sink.commit(epoch as u64 + 1).await.unwrap();
            }

            connector.shutdown().await.ok();
        }

        // ---- Verify: all epochs visible, no gaps, no dups ----
        let group = format!("verify-clean-{pid}-{run}");
        let received = drain_committed(&topic, &group, total_events, Duration::from_secs(20)).await;
        let seqs = extract_seqs(&received, &run_id);
        assert_exactly_once(&seqs, total_events, "full_2pc_clean", run);

        println!(
            "  [full_2pc_clean run {}/{}] PASS — {} events across {} epochs, 0 dups, 0 gaps",
            run + 1,
            RUNS,
            seqs.len(),
            num_epochs
        );
    }
}
