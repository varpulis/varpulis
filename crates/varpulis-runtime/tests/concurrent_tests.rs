//! E2E tests for the `.concurrent()` parallel processing operator.
//!
//! These tests exercise the full engine pipeline: parse VPL → load → process events → collect output.
//! The `.concurrent()` operator partitions events across a Rayon thread pool, then merges results
//! back for downstream operators (`.where()`, `.emit()`, `.select()`).

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

/// Parse VPL, create engine, process events via async path, collect outputs.
async fn run(code: &str, events: Vec<Event>) -> Vec<Event> {
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    for e in events {
        engine.process(e).await.unwrap();
    }
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

/// Parse VPL, create engine, process events via sync batch path, collect outputs.
fn run_sync(code: &str, events: Vec<Event>) -> Vec<Event> {
    let count = events.len();
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(count + 1024);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    engine.process_batch_sync(events).unwrap();
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

// =============================================================================
// .concurrent() runtime E2E tests
// =============================================================================

#[tokio::test]
async fn test_concurrent_where_emit() {
    let code = r"stream Filtered = Data
    .concurrent(workers: 4)
    .where(value > 50)
    .emit(value: value)";

    let events: Vec<Event> = (1..=100)
        .map(|i| Event::new("Data").with_field("value", i as f64))
        .collect();

    let results = run(code, events).await;

    // Sequential reference: values 51..=100 pass the filter → 50 events
    assert_eq!(
        results.len(),
        50,
        "Expected 50 events with value > 50, got {}",
        results.len()
    );

    for event in &results {
        let v = event.get_float("value").expect("value field should exist");
        assert!(
            v > 50.0,
            "All emitted events should have value > 50, got {v}"
        );
    }
}

#[tokio::test]
async fn test_concurrent_partition_key_ordering() {
    let code = r#"stream Partitioned = SensorReading
    .concurrent(workers: 4, partition_key: "sensor_id")
    .where(value > 0)
    .emit(sensor_id: sensor_id, value: value, seq: seq)"#;

    let mut events = Vec::new();
    for seq in 0..20 {
        for sensor in &["s1", "s2", "s3", "s4"] {
            events.push(
                Event::new("SensorReading")
                    .with_field("sensor_id", *sensor)
                    .with_field("value", 10.0f64)
                    .with_field(
                        "seq",
                        (seq * 4
                            + ["s1", "s2", "s3", "s4"]
                                .iter()
                                .position(|s| s == sensor)
                                .unwrap()) as f64,
                    ),
            );
        }
    }

    let results = run(code, events).await;
    assert_eq!(results.len(), 80, "All 80 events should pass through");

    // Verify per-sensor ordering is preserved
    for sensor in &["s1", "s2", "s3", "s4"] {
        let sensor_seqs: Vec<f64> = results
            .iter()
            .filter(|e| e.get_str("sensor_id").is_some_and(|s| s == *sensor))
            .filter_map(|e| e.get_float("seq"))
            .collect();

        assert_eq!(sensor_seqs.len(), 20, "Each sensor should have 20 events");

        // Check monotonically increasing
        for pair in sensor_seqs.windows(2) {
            assert!(
                pair[0] < pair[1],
                "Per-sensor ordering violated for {}: {} >= {}",
                sensor,
                pair[0],
                pair[1]
            );
        }
    }
}

#[tokio::test]
async fn test_concurrent_single_event_passthrough() {
    // Single event should skip parallelism (early return at pipeline.rs:925)
    let code = r"stream S = Data
    .concurrent(workers: 4)
    .where(value > 0)
    .emit(value: value)";

    let events = vec![Event::new("Data").with_field("value", 42.0f64)];
    let results = run(code, events).await;

    assert_eq!(results.len(), 1);
    let v = results[0].get_float("value").unwrap();
    assert!((v - 42.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_concurrent_empty_events() {
    // Zero events should not panic
    let code = r"stream S = Data
    .concurrent(workers: 4)
    .where(value > 0)
    .emit(value: value)";

    let results = run(code, vec![]).await;
    assert!(results.is_empty());
}

#[test]
fn test_concurrent_workers_1_matches_sequential() {
    // `.concurrent(workers: 1)` should produce identical output to no `.concurrent()`
    let code_concurrent = r"stream S = Data
    .concurrent(workers: 1)
    .where(value > 25)
    .emit(value: value)";

    let code_sequential = r"stream S = Data
    .where(value > 25)
    .emit(value: value)";

    let events: Vec<Event> = (1..=50)
        .map(|i| Event::new("Data").with_field("value", i as f64))
        .collect();

    let concurrent_results = run_sync(code_concurrent, events.clone());
    let sequential_results = run_sync(code_sequential, events);

    assert_eq!(
        concurrent_results.len(),
        sequential_results.len(),
        "workers:1 should produce same count as sequential"
    );

    for (c, s) in concurrent_results.iter().zip(sequential_results.iter()) {
        let cv = c.get_float("value").unwrap();
        let sv = s.get_float("value").unwrap();
        assert!(
            (cv - sv).abs() < f64::EPSILON,
            "workers:1 output should match sequential: {cv} vs {sv}"
        );
    }
}

#[test]
fn test_concurrent_large_batch() {
    let code = r"stream S = Data
    .concurrent(workers: 4)
    .where(value > 0)
    .emit(value: value)";

    let events: Vec<Event> = (1..=10_000)
        .map(|i| Event::new("Data").with_field("value", i as f64))
        .collect();

    let results = run_sync(code, events);
    assert_eq!(
        results.len(),
        10_000,
        "All 10K events should be processed without drops"
    );
}

#[tokio::test]
async fn test_concurrent_with_emit_projection() {
    // .concurrent() followed by .emit() that selects specific fields (projection)
    let code = r"stream S = Data
    .concurrent(workers: 2)
    .where(value > 0)
    .emit(field1: field1, field2: field2)";

    let events: Vec<Event> = (0..10)
        .map(|i| {
            Event::new("Data")
                .with_field("field1", format!("a{i}"))
                .with_field("field2", i as f64)
                .with_field("field3", "should_be_dropped")
                .with_field("value", 1.0f64)
        })
        .collect();

    let results = run(code, events).await;
    assert_eq!(results.len(), 10);

    for event in &results {
        assert!(
            event.get_str("field1").is_some(),
            "field1 should be projected"
        );
        assert!(
            event.get_float("field2").is_some(),
            "field2 should be projected"
        );
        // .emit() only includes named fields
        assert!(
            event.data.get("field3").is_none(),
            "field3 should NOT be in emitted output"
        );
    }
}

#[tokio::test]
async fn test_concurrent_sequence_then_concurrent() {
    // Sequence pattern followed by `.concurrent().where().emit()`
    let code = r"stream S = EventA as a -> EventB where id == a.id as b .within(5m)
    .concurrent(workers: 2)
    .where(b.value > 10)
    .emit(id: a.id, value: b.value)";

    let events = vec![
        Event::new("EventA")
            .with_field("id", "x1")
            .with_field("info", "start"),
        Event::new("EventB")
            .with_field("id", "x1")
            .with_field("value", 50.0f64),
        Event::new("EventA")
            .with_field("id", "x2")
            .with_field("info", "start"),
        Event::new("EventB")
            .with_field("id", "x2")
            .with_field("value", 5.0f64), // Below threshold — should be filtered
    ];

    let results = run(code, events).await;

    // Only the x1 sequence should pass the where(b.value > 10) filter
    assert_eq!(
        results.len(),
        1,
        "Only one sequence match should pass the filter"
    );
    assert_eq!(results[0].get_str("id").unwrap_or_default(), "x1");
}
