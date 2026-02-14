//! Extended engine tests targeting uncovered paths in engine/mod.rs and engine/pipeline.rs.
//!
//! Covers: distinct, limit, group_by (partition_by), filter alias, first shorthand,
//! session/sliding/count windows, window+aggregate combos, hot reload, sync processing,
//! pattern matching, edge cases, benchmark mode, add_filter, and more.

use chrono::{TimeZone, Utc};
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ts_ms(ms: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).unwrap()
}

/// Parse + load + process events, collect output.
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

/// Parse + load + process events via process_batch_sync, collect output.
fn run_sync(code: &str, events: Vec<Event>) -> Vec<Event> {
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    engine.process_batch_sync(events).unwrap();
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

// ===========================================================================
// 1. Distinct operation — various scenarios
// ===========================================================================

#[tokio::test]
async fn distinct_whole_event_dedup() {
    // When no field is specified the entire event data is hashed for dedup.
    let code = r#"
        stream S = Tick
            .distinct()
            .emit(x: x, y: y)
    "#;
    let events = vec![
        Event::new("Tick")
            .with_field("x", Value::Int(1))
            .with_field("y", Value::Int(2)),
        Event::new("Tick")
            .with_field("x", Value::Int(1))
            .with_field("y", Value::Int(2)),
        Event::new("Tick")
            .with_field("x", Value::Int(1))
            .with_field("y", Value::Int(3)),
    ];
    let out = run(code, events).await;
    assert_eq!(out.len(), 2, "Second duplicate should be dropped");
}

#[tokio::test]
async fn distinct_field_across_batches() {
    // Distinct state persists across individual process() calls.
    let code = r#"
        stream S = Tick
            .distinct(id)
            .emit(id: id)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    engine
        .process(Event::new("Tick").with_field("id", Value::Int(1)))
        .await
        .unwrap();
    engine
        .process(Event::new("Tick").with_field("id", Value::Int(1)))
        .await
        .unwrap();
    engine
        .process(Event::new("Tick").with_field("id", Value::Int(2)))
        .await
        .unwrap();

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert_eq!(out.len(), 2, "Only first of each id should pass");
}

// ===========================================================================
// 2. Limit operation — edge cases
// ===========================================================================

#[tokio::test]
async fn limit_zero_blocks_all() {
    let code = r#"
        stream S = Tick
            .limit(0)
            .emit(val: x)
    "#;
    let events = vec![
        Event::new("Tick").with_field("x", Value::Int(1)),
        Event::new("Tick").with_field("x", Value::Int(2)),
    ];
    let out = run(code, events).await;
    assert_eq!(out.len(), 0, "Limit 0 should block everything");
}

#[tokio::test]
async fn limit_larger_than_input() {
    let code = r#"
        stream S = Tick
            .limit(100)
            .emit(val: x)
    "#;
    let events: Vec<Event> = (1..=5)
        .map(|i| Event::new("Tick").with_field("x", Value::Int(i)))
        .collect();
    let out = run(code, events).await;
    assert_eq!(out.len(), 5, "Limit > input count should pass all");
}

#[tokio::test]
async fn first_shorthand_is_limit_one() {
    let code = r#"
        stream S = Tick
            .first()
            .emit(val: x)
    "#;
    let events: Vec<Event> = (1..=5)
        .map(|i| Event::new("Tick").with_field("x", Value::Int(i)))
        .collect();
    let out = run(code, events).await;
    assert_eq!(out.len(), 1, ".first() should emit exactly 1 event");
    assert_eq!(out[0].data.get("val"), Some(&Value::Int(1)));
}

// ===========================================================================
// 3. Partition_by with count window + aggregate
// ===========================================================================

#[tokio::test]
async fn partition_by_count_window_aggregate() {
    let code = r#"
        stream S = Reading
            .partition_by(sensor)
            .window(2)
            .aggregate(total: sum(value))
            .emit(sensor: sensor, total: total)
    "#;
    let events = vec![
        Event::new("Reading")
            .with_field("sensor", Value::str("A"))
            .with_field("value", Value::Float(10.0)),
        Event::new("Reading")
            .with_field("sensor", Value::str("B"))
            .with_field("value", Value::Float(20.0)),
        Event::new("Reading")
            .with_field("sensor", Value::str("A"))
            .with_field("value", Value::Float(30.0)),
        Event::new("Reading")
            .with_field("sensor", Value::str("B"))
            .with_field("value", Value::Float(40.0)),
    ];
    let out = run(code, events).await;
    // Each partition fills its count window of 2 => 2 outputs
    assert_eq!(out.len(), 2, "Expected 2 partitioned results");
}

// ===========================================================================
// 4. Sliding count window
// ===========================================================================

#[tokio::test]
async fn sliding_count_window_produces_multiple_outputs() {
    let code = r#"
        stream S = Reading
            .window(3, sliding: 1)
            .aggregate(total: sum(value))
            .emit(total: total)
    "#;
    let events: Vec<Event> = (1..=5)
        .map(|i| Event::new("Reading").with_field("value", Value::Float(i as f64 * 10.0)))
        .collect();
    let out = run(code, events).await;
    // Window size 3, slide 1: first output after 3 events, then one per event => 3 outputs
    assert!(
        out.len() >= 3,
        "Sliding count window should produce multiple outputs, got {}",
        out.len()
    );
}

// ===========================================================================
// 5. Count window basic
// ===========================================================================

#[tokio::test]
async fn count_window_exact_fill() {
    let code = r#"
        stream S = Reading
            .window(3)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let events: Vec<Event> = (1..=6)
        .map(|i| Event::new("Reading").with_field("value", Value::Int(i)))
        .collect();
    let out = run(code, events).await;
    assert_eq!(out.len(), 2, "6 events / window of 3 = 2 outputs");
}

// ===========================================================================
// 6. Session window (event-time based)
// ===========================================================================

#[tokio::test]
async fn session_window_with_aggregate() {
    let code = r#"
        stream S = Click
            .window(session: 5s)
            .aggregate(clicks: count())
    "#;
    let events = vec![
        Event::new("Click")
            .with_field("user", Value::str("u1"))
            .with_timestamp(ts_ms(1000)),
        Event::new("Click")
            .with_field("user", Value::str("u1"))
            .with_timestamp(ts_ms(3000)),
        // Large gap — triggers session close
        Event::new("Click")
            .with_field("user", Value::str("u1"))
            .with_timestamp(ts_ms(20000)),
    ];
    let out = run(code, events).await;
    // The session may or may not produce output inline (depends on sweep),
    // but it should compile and run without errors.
    let _ = out;
}

// ===========================================================================
// 7. Hot reload — add / remove / update streams
// ===========================================================================

#[tokio::test]
async fn reload_adds_new_stream() {
    let code_v1 = r#"
        stream A = Tick
            .where(x > 0)
            .emit(val: x)
    "#;
    let code_v2 = r#"
        stream A = Tick
            .where(x > 0)
            .emit(val: x)

        stream B = Tock
            .emit(val: y)
    "#;

    let program_v1 = parse(code_v1).expect("parse v1");
    let program_v2 = parse(code_v2).expect("parse v2");

    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program_v1).unwrap();

    assert_eq!(engine.stream_names().len(), 1);

    let report = engine.reload(&program_v2).unwrap();
    assert!(
        report.streams_added.contains(&"B".to_string()),
        "B should be added"
    );
    assert_eq!(engine.stream_names().len(), 2);

    // Process through the new stream
    engine
        .process(Event::new("Tock").with_field("y", Value::Int(42)))
        .await
        .unwrap();
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert!(!out.is_empty(), "New stream B should produce output");
}

#[tokio::test]
async fn reload_removes_stream() {
    let code_v1 = r#"
        stream A = Tick
            .emit(val: x)

        stream B = Tock
            .emit(val: y)
    "#;
    let code_v2 = r#"
        stream A = Tick
            .emit(val: x)
    "#;

    let program_v1 = parse(code_v1).expect("parse v1");
    let program_v2 = parse(code_v2).expect("parse v2");

    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program_v1).unwrap();

    assert_eq!(engine.stream_names().len(), 2);

    let report = engine.reload(&program_v2).unwrap();
    assert!(
        report.streams_removed.contains(&"B".to_string()),
        "B should be removed"
    );
    assert_eq!(engine.stream_names().len(), 1);
}

#[tokio::test]
async fn reload_updates_stream_with_structural_change() {
    // When the ops count changes, reload replaces the stream definition
    let code_v1 = r#"
        stream S = Tick
            .where(x > 10)
            .emit(val: x)
    "#;
    // v2 adds a .distinct() operation — structural change (3 ops vs 2)
    let code_v2 = r#"
        stream S = Tick
            .where(x > 50)
            .distinct(x)
            .emit(val: x)
    "#;

    let program_v1 = parse(code_v1).expect("parse");
    let program_v2 = parse(code_v2).expect("parse");

    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program_v1).unwrap();

    // Under v1 filter, x=30 passes
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(30)))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok(), "x=30 should pass v1 filter");

    // Reload with structurally different program
    let report = engine.reload(&program_v2).unwrap();
    assert!(
        report.streams_updated.contains(&"S".to_string()),
        "S should be listed as updated"
    );
    assert!(
        report.state_reset.contains(&"S".to_string()),
        "S state should be reset"
    );

    // Under v2 filter, x=30 should NOT pass (> 50)
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(30)))
        .await
        .unwrap();
    assert!(
        rx.try_recv().is_err(),
        "x=30 should not pass v2 filter (> 50)"
    );

    // x=60 should pass
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(60)))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok(), "x=60 should pass v2 filter");
}

#[tokio::test]
async fn reload_empty_report_for_identical_program() {
    let code = r#"
        stream S = Tick
            .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let report = engine.reload(&program).unwrap();
    assert!(
        report.streams_added.is_empty() && report.streams_removed.is_empty(),
        "Identical reload should not add/remove streams"
    );
}

// ===========================================================================
// 8. stream_names() / has_sink_operations()
// ===========================================================================

#[tokio::test]
async fn stream_names_returns_all() {
    let code = r#"
        stream Alpha = X .emit(v: v)
        stream Beta  = Y .emit(v: v)
        stream Gamma = Z .emit(v: v)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let names = engine.stream_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"Alpha"));
    assert!(names.contains(&"Beta"));
    assert!(names.contains(&"Gamma"));
}

#[tokio::test]
async fn has_sink_operations_false_without_to() {
    let code = r#"
        stream S = Tick .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert!(
        !engine.has_sink_operations(),
        "No .to() => has_sink_operations should be false"
    );
}

// ===========================================================================
// 9. Sync processing — process_batch_sync
// ===========================================================================

#[tokio::test]
async fn process_batch_sync_filter_and_emit() {
    let code = r#"
        stream S = Tick
            .where(x > 5)
            .emit(val: x)
    "#;
    let events: Vec<Event> = (1..=10)
        .map(|i| Event::new("Tick").with_field("x", Value::Int(i)))
        .collect();
    let out = run_sync(code, events);
    assert_eq!(out.len(), 5, "Only x=6..10 should pass filter");
}

#[tokio::test]
async fn process_batch_sync_empty_batch() {
    let code = r#"
        stream S = Tick .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process_batch_sync(vec![]).unwrap();
    assert!(rx.try_recv().is_err(), "Empty batch => no output");
}

#[tokio::test]
async fn process_batch_sync_with_sequence() {
    let code = r#"
        stream S = A as a -> B as b
            .emit(status: "matched")
    "#;
    let events = vec![Event::new("A"), Event::new("B")];
    let out = run_sync(code, events);
    assert!(
        !out.is_empty(),
        "Sync batch should handle sequence patterns"
    );
}

#[tokio::test]
async fn process_batch_sync_with_distinct_and_limit() {
    let code = r#"
        stream S = Tick
            .distinct(id)
            .limit(2)
            .emit(id: id)
    "#;
    let events = vec![
        Event::new("Tick").with_field("id", Value::Int(1)),
        Event::new("Tick").with_field("id", Value::Int(1)),
        Event::new("Tick").with_field("id", Value::Int(2)),
        Event::new("Tick").with_field("id", Value::Int(3)),
        Event::new("Tick").with_field("id", Value::Int(4)),
    ];
    let out = run_sync(code, events);
    assert_eq!(
        out.len(),
        2,
        "distinct dedup + limit 2 should produce exactly 2"
    );
}

// ===========================================================================
// 10. Pattern matching — sequence with within + cross-event refs
// ===========================================================================

#[tokio::test]
async fn sequence_with_within_cross_ref() {
    let code = r#"
        stream S = Order as order
            -> Payment where amount == order.total as pay
            .within(10s)
            .emit(order_id: order.id, paid: pay.amount)
    "#;
    let events = vec![
        Event::new("Order")
            .with_field("id", Value::Int(100))
            .with_field("total", Value::Float(50.0))
            .with_timestamp(ts_ms(1000)),
        Event::new("Payment")
            .with_field("amount", Value::Float(50.0))
            .with_timestamp(ts_ms(2000)),
    ];
    let out = run(code, events).await;
    assert!(!out.is_empty(), "Should match Payment with matching amount");
}

#[tokio::test]
async fn sequence_three_step_chain() {
    let code = r#"
        stream Pipeline = Start as s
            -> Middle as m
            -> End as e
            .within(30s)
            .emit(label: "complete")
    "#;
    let events = vec![
        Event::new("Start").with_timestamp(ts_ms(1000)),
        Event::new("Middle").with_timestamp(ts_ms(2000)),
        Event::new("End").with_timestamp(ts_ms(3000)),
    ];
    let out = run(code, events).await;
    assert!(!out.is_empty(), "Three-step sequence should match");
    assert_eq!(
        out[0].data.get("label"),
        Some(&Value::Str("complete".into()))
    );
}

#[tokio::test]
async fn sequence_no_match_when_out_of_order() {
    let code = r#"
        stream S = A -> B -> C
            .emit(ok: "yes")
    "#;
    let events = vec![Event::new("C"), Event::new("B"), Event::new("A")];
    let out = run(code, events).await;
    assert!(out.is_empty(), "Reversed order should not match");
}

// ===========================================================================
// 11. Edge cases
// ===========================================================================

#[tokio::test]
async fn processing_event_with_no_matching_stream() {
    let code = r#"
        stream S = Alpha
            .emit(val: x)
    "#;
    let events = vec![
        Event::new("Beta").with_field("x", Value::Int(1)),
        Event::new("Gamma").with_field("x", Value::Int(2)),
    ];
    let out = run(code, events).await;
    assert!(
        out.is_empty(),
        "Unmatched event types should produce no output"
    );
}

#[tokio::test]
async fn empty_program_processes_events() {
    let code = "";
    let events = vec![Event::new("Anything")];
    let out = run(code, events).await;
    assert!(out.is_empty(), "Empty program should produce no output");
}

#[tokio::test]
async fn program_with_only_event_decls() {
    let code = r#"
        event Sensor:
            temp: float
            humidity: float
    "#;
    let events = vec![Event::new("Sensor")
        .with_field("temp", Value::Float(25.0))
        .with_field("humidity", Value::Float(60.0))];
    let out = run(code, events).await;
    assert!(
        out.is_empty(),
        "Event-only program with no streams should produce no output"
    );
}

#[tokio::test]
async fn multiple_streams_consuming_same_event_type() {
    let code = r#"
        stream Hot  = Temp .where(value > 100.0) .emit(kind: "hot",  v: value)
        stream Cold = Temp .where(value < 0.0)   .emit(kind: "cold", v: value)
        stream Warm = Temp .where(value >= 0.0 and value <= 100.0) .emit(kind: "warm", v: value)
    "#;
    let events = vec![
        Event::new("Temp").with_field("value", Value::Float(150.0)),
        Event::new("Temp").with_field("value", Value::Float(-10.0)),
        Event::new("Temp").with_field("value", Value::Float(50.0)),
    ];
    let out = run(code, events).await;
    assert_eq!(out.len(), 3, "Each event should match exactly one stream");

    let kinds: Vec<&Value> = out.iter().filter_map(|e| e.data.get("kind")).collect();
    assert!(kinds.contains(&&Value::Str("hot".into())));
    assert!(kinds.contains(&&Value::Str("cold".into())));
    assert!(kinds.contains(&&Value::Str("warm".into())));
}

// ===========================================================================
// 12. Benchmark mode (no output channel)
// ===========================================================================

#[tokio::test]
async fn benchmark_engine_processes_without_panic() {
    let code = r#"
        stream S = Tick
            .where(x > 0)
            .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let mut engine = Engine::new_benchmark();
    engine.load(&program).unwrap();

    for i in 1..=100 {
        engine
            .process(Event::new("Tick").with_field("x", Value::Int(i)))
            .await
            .unwrap();
    }

    let m = engine.metrics();
    assert_eq!(m.events_processed, 100);
    // In benchmark mode output_events_emitted is still counted
    assert!(m.output_events_emitted > 0);
}

#[tokio::test]
async fn benchmark_engine_sync_batch() {
    let code = r#"
        stream S = Tick
            .where(x > 0)
            .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let mut engine = Engine::new_benchmark();
    engine.load(&program).unwrap();

    let events: Vec<Event> = (1..=50)
        .map(|i| Event::new("Tick").with_field("x", Value::Int(i)))
        .collect();
    engine.process_batch_sync(events).unwrap();

    let m = engine.metrics();
    assert_eq!(m.events_processed, 50);
    assert!(m.output_events_emitted > 0);
}

// ===========================================================================
// 13. add_filter() programmatic API
// ===========================================================================

#[tokio::test]
async fn add_filter_closure_to_stream() {
    let code = r#"
        stream S = Tick
            .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Add a programmatic closure filter that only passes even x values
    engine
        .add_filter("S", |e| {
            e.data
                .get("x")
                .and_then(|v| v.as_int())
                .map(|n| n % 2 == 0)
                .unwrap_or(false)
        })
        .unwrap();

    for i in 1..=6 {
        engine
            .process(Event::new("Tick").with_field("x", Value::Int(i)))
            .await
            .unwrap();
    }

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert_eq!(out.len(), 3, "Only even x (2,4,6) should pass");
}

#[tokio::test]
async fn add_filter_to_nonexistent_stream_returns_error() {
    let code = r#"
        stream S = Tick .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let result = engine.add_filter("NonExistent", |_| true);
    assert!(result.is_err(), "add_filter on missing stream should fail");
}

// ===========================================================================
// 14. Variables — immutable assignment error
// ===========================================================================

#[tokio::test]
async fn immutable_variable_rejects_set() {
    let code = r#"
        let threshold: int = 10
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert_eq!(engine.get_variable("threshold"), Some(&Value::Int(10)));

    let result = engine.set_variable("threshold", Value::Int(20));
    assert!(result.is_err(), "Setting an immutable variable should fail");
}

#[tokio::test]
async fn mutable_variable_allows_set() {
    let code = r#"
        var counter: int = 0
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.set_variable("counter", Value::Int(42)).unwrap();
    assert_eq!(engine.get_variable("counter"), Some(&Value::Int(42)));
}

// ===========================================================================
// 15. Engine metrics tracking
// ===========================================================================

#[tokio::test]
async fn metrics_count_events_and_outputs() {
    let code = r#"
        stream S = Tick
            .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    for i in 1..=5 {
        engine
            .process(Event::new("Tick").with_field("x", Value::Int(i)))
            .await
            .unwrap();
    }

    let m = engine.metrics();
    assert_eq!(m.events_processed, 5);
    assert_eq!(m.output_events_emitted, 5);
    assert_eq!(m.streams_count, 1);

    // Drain output
    while rx.try_recv().is_ok() {}
}

#[tokio::test]
async fn event_counters_method() {
    let code = r#"
        stream S = Tick .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("Tick").with_field("x", Value::Int(1)))
        .await
        .unwrap();
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(2)))
        .await
        .unwrap();

    let (events_in, events_out) = engine.event_counters();
    assert_eq!(events_in, 2);
    assert_eq!(events_out, 2);

    while rx.try_recv().is_ok() {}
}

// ===========================================================================
// 16. Window + aggregate combos
// ===========================================================================

#[tokio::test]
async fn count_window_with_multiple_aggregates() {
    let code = r#"
        stream S = Sensor
            .window(4)
            .aggregate(
                cnt: count(),
                s: sum(value),
                mn: min(value),
                mx: max(value),
                av: avg(value)
            )
            .emit(cnt: cnt, s: s, mn: mn, mx: mx, av: av)
    "#;
    let events: Vec<Event> = vec![10.0, 20.0, 30.0, 40.0]
        .into_iter()
        .map(|v| Event::new("Sensor").with_field("value", Value::Float(v)))
        .collect();
    let out = run(code, events).await;
    assert_eq!(out.len(), 1, "Window of 4 with 4 events => 1 output");
    if let Some(Value::Float(av)) = out[0].data.get("av") {
        assert!((*av - 25.0).abs() < 0.001, "avg(10,20,30,40) should be 25");
    }
    if let Some(Value::Float(mn)) = out[0].data.get("mn") {
        assert!((*mn - 10.0).abs() < 0.001);
    }
    if let Some(Value::Float(mx)) = out[0].data.get("mx") {
        assert!((*mx - 40.0).abs() < 0.001);
    }
}

#[tokio::test]
async fn window_with_having_filter() {
    let code = r#"
        stream S = Sensor
            .window(3)
            .aggregate(total: sum(value))
            .having(total > 100.0)
            .emit(total: total)
    "#;
    let events: Vec<Event> = vec![10.0, 20.0, 30.0, 50.0, 60.0, 40.0]
        .into_iter()
        .map(|v| Event::new("Sensor").with_field("value", Value::Float(v)))
        .collect();
    let out = run(code, events).await;
    // Window 1: 10+20+30=60 (filtered), Window 2: 50+60+40=150 (passes)
    assert_eq!(out.len(), 1, "Only second window should pass having");
    if let Some(Value::Float(t)) = out[0].data.get("total") {
        assert!((*t - 150.0).abs() < 0.001);
    }
}

// ===========================================================================
// 17. Select projection
// ===========================================================================

#[tokio::test]
async fn select_projects_specific_fields() {
    let code = r#"
        stream S = Data
            .select(a: x, doubled: x * 2)
            .emit(a: a, doubled: doubled)
    "#;
    let events = vec![Event::new("Data")
        .with_field("x", Value::Int(5))
        .with_field("y", Value::Int(100))
        .with_field("z", Value::str("noise"))];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].data.get("a"), Some(&Value::Int(5)));
    assert_eq!(out[0].data.get("doubled"), Some(&Value::Int(10)));
}

// ===========================================================================
// 18. Derived stream consumed in sequence
// ===========================================================================

#[tokio::test]
async fn derived_stream_in_sequence() {
    let code = r#"
        stream HighTemp = Reading
            .where(temp > 100.0)

        stream Alert = HighTemp as ht
            -> Ack as ack
            .emit(msg: "alert acknowledged")
    "#;
    let events = vec![
        Event::new("Reading").with_field("temp", Value::Float(200.0)),
        Event::new("Ack"),
    ];
    let out = run(code, events).await;
    // The HighTemp stream produces an output event that then triggers the sequence
    assert!(!out.is_empty(), "Derived stream should feed into sequence");
}

// ===========================================================================
// 19. is_stateless() check
// ===========================================================================

#[tokio::test]
async fn stateless_pipeline_detected() {
    let code = r#"
        stream S = Tick
            .where(x > 0)
            .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert!(
        engine.is_stateless(),
        "Filter+emit pipeline should be stateless"
    );
}

#[tokio::test]
async fn stateful_pipeline_with_window() {
    let code = r#"
        stream S = Tick
            .window(3)
            .aggregate(c: count())
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert!(!engine.is_stateless(), "Window pipeline should be stateful");
}

// ===========================================================================
// 20. partition_key()
// ===========================================================================

#[tokio::test]
async fn partition_key_detected() {
    let code = r#"
        stream S = Reading
            .partition_by(region)
            .window(3)
            .aggregate(c: count())
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert_eq!(
        engine.partition_key(),
        Some("region".to_string()),
        "partition_key should detect partition_by(region)"
    );
}

// ===========================================================================
// 21. Emit with expression (EmitExpr path)
// ===========================================================================

#[tokio::test]
async fn emit_with_arithmetic_expression() {
    let code = r#"
        stream S = Tick
            .emit(doubled: x * 2, sum: x + y, label: "computed")
    "#;
    let events = vec![Event::new("Tick")
        .with_field("x", Value::Int(5))
        .with_field("y", Value::Int(3))];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].data.get("doubled"), Some(&Value::Int(10)));
    assert_eq!(out[0].data.get("sum"), Some(&Value::Int(8)));
    assert_eq!(
        out[0].data.get("label"),
        Some(&Value::Str("computed".into()))
    );
}

// ===========================================================================
// 22. Log and Print passthrough
// ===========================================================================

#[tokio::test]
async fn log_and_print_do_not_filter() {
    let code = r#"
        stream S = Tick
            .log(level: "debug", message: "test")
            .print()
            .emit(val: x)
    "#;
    let events = vec![Event::new("Tick").with_field("x", Value::Int(1))];
    let out = run(code, events).await;
    assert_eq!(
        out.len(),
        1,
        "Log and print should pass events through unmodified"
    );
}

// ===========================================================================
// 23. Shared event channel (new_shared constructor)
// ===========================================================================

#[tokio::test]
async fn shared_event_channel_works() {
    use varpulis_runtime::event::SharedEvent;

    let code = r#"
        stream S = Tick .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel::<SharedEvent>(100);
    let mut engine = Engine::new_shared(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("Tick").with_field("x", Value::Int(99)))
        .await
        .unwrap();

    let shared_event = rx.try_recv().expect("Should receive shared event");
    assert_eq!(shared_event.data.get("val"), Some(&Value::Int(99)));
}

// ===========================================================================
// 24. new_with_optional_output — None case
// ===========================================================================

#[tokio::test]
async fn optional_output_none() {
    let code = r#"
        stream S = Tick .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let mut engine = Engine::new_with_optional_output(None);
    engine.load(&program).unwrap();

    // Should not panic even though there is no output channel
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(1)))
        .await
        .unwrap();

    let m = engine.metrics();
    assert_eq!(m.events_processed, 1);
}

// ===========================================================================
// 25. Chain depth — derived streams feeding each other
// ===========================================================================

#[tokio::test]
async fn chain_depth_three_levels() {
    let code = r#"
        stream Level1 = Raw
            .where(x > 0)

        stream Level2 = Level1
            .where(x > 5)

        stream Level3 = Level2
            .emit(val: x)
    "#;
    let events = vec![
        Event::new("Raw").with_field("x", Value::Int(10)),
        Event::new("Raw").with_field("x", Value::Int(3)),
        Event::new("Raw").with_field("x", Value::Int(0)),
    ];
    let out = run(code, events).await;
    assert_eq!(
        out.len(),
        1,
        "Only x=10 should pass through all three levels"
    );
    assert_eq!(out[0].data.get("val"), Some(&Value::Int(10)));
}

// ===========================================================================
// 26. process_batch (async) with bulk events
// ===========================================================================

#[tokio::test]
async fn process_batch_async_produces_output() {
    let code = r#"
        stream S = Tick
            .where(x > 5)
            .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let events: Vec<Event> = (1..=20)
        .map(|i| Event::new("Tick").with_field("x", Value::Int(i)))
        .collect();
    engine.process_batch(events).await.unwrap();

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert_eq!(out.len(), 15, "x=6..20 should pass filter (15 events)");
}

// ===========================================================================
// 27. has_session_windows / min_session_gap
// ===========================================================================

#[tokio::test]
async fn has_session_windows_detected() {
    let code = r#"
        stream S = Click
            .window(session: 10s)
            .aggregate(c: count())
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert!(engine.has_session_windows());
    let gap = engine.min_session_gap();
    assert!(gap.is_some(), "Should detect session gap");
}

#[tokio::test]
async fn no_session_windows_when_count_based() {
    let code = r#"
        stream S = Tick
            .window(5)
            .aggregate(c: count())
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert!(!engine.has_session_windows());
    assert!(engine.min_session_gap().is_none());
}

// ===========================================================================
// 28. Filter alias (.filter = .where)
// ===========================================================================

#[tokio::test]
async fn filter_alias_behaves_like_where() {
    let code = r#"
        stream S = Tick
            .filter(x > 5)
            .emit(val: x)
    "#;
    let events: Vec<Event> = (1..=10)
        .map(|i| Event::new("Tick").with_field("x", Value::Int(i)))
        .collect();
    let out = run(code, events).await;
    assert_eq!(out.len(), 5, ".filter(x>5) should pass x=6..10");
}

// ===========================================================================
// 29. Unsupported operations produce errors
// ===========================================================================

#[tokio::test]
async fn map_operation_returns_error() {
    let code = r#"
        stream S = Tick
            .map(x * 2)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    let result = engine.load(&program);
    assert!(result.is_err(), ".map() should return an error");
    assert!(
        result.unwrap_err().contains(".map()"),
        "Error should mention .map()"
    );
}

// ===========================================================================
// 30. Partitioned sliding count window
// ===========================================================================

#[tokio::test]
async fn partitioned_sliding_count_window() {
    let code = r#"
        stream S = Reading
            .partition_by(sensor)
            .window(3, sliding: 1)
            .aggregate(s: sum(value))
            .emit(total: s)
    "#;
    let events = vec![
        Event::new("Reading")
            .with_field("sensor", Value::str("A"))
            .with_field("value", Value::Float(1.0)),
        Event::new("Reading")
            .with_field("sensor", Value::str("A"))
            .with_field("value", Value::Float(2.0)),
        Event::new("Reading")
            .with_field("sensor", Value::str("A"))
            .with_field("value", Value::Float(3.0)),
        Event::new("Reading")
            .with_field("sensor", Value::str("A"))
            .with_field("value", Value::Float(4.0)),
        Event::new("Reading")
            .with_field("sensor", Value::str("A"))
            .with_field("value", Value::Float(5.0)),
    ];
    let out = run(code, events).await;
    // Sliding window of 3 with step 1: first output after 3 events, then one per event
    // => at least 3 outputs
    assert!(
        out.len() >= 3,
        "Partitioned sliding window should produce multiple outputs, got {}",
        out.len()
    );
}

// ===========================================================================
// 31. Function declaration and lookup
// ===========================================================================

#[tokio::test]
async fn function_declaration_accessible() {
    let code = r#"
        fn double(x: int) -> int:
            return x * 2

        fn triple(x: int) -> int:
            return x * 3

        stream S = Tick .emit(val: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert!(engine.get_function("double").is_some());
    assert!(engine.get_function("triple").is_some());
    assert!(engine.get_function("nonexistent").is_none());

    let names = engine.function_names();
    assert!(names.contains(&"double"));
    assert!(names.contains(&"triple"));
}

// ===========================================================================
// 32. Process with function that emits multiple events
// ===========================================================================

#[tokio::test]
async fn process_function_multiple_emits() {
    let code = r#"
        fn fan_out():
            emit Low(v: value * 0.5)
            emit Mid(v: value * 1.0)
            emit High(v: value * 1.5)

        stream S = Input
            .process(fan_out())
    "#;
    let events = vec![Event::new("Input").with_field("value", Value::Float(100.0))];
    let out = run(code, events).await;
    assert_eq!(out.len(), 3, "fan_out should emit 3 events per input");
}

// ===========================================================================
// 33. Variables map is accessible
// ===========================================================================

#[tokio::test]
async fn variables_map_accessible() {
    let code = r#"
        var x: int = 10
        var y: float = 3.14
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let vars = engine.variables();
    assert!(vars.contains_key("x"));
    assert!(vars.contains_key("y"));
    assert_eq!(vars.get("x"), Some(&Value::Int(10)));
}

// ===========================================================================
// 34. Reload preserves variables
// ===========================================================================

#[tokio::test]
async fn reload_preserves_user_set_variables() {
    let code_v1 = r#"
        var threshold: int = 10
        stream S = Tick .emit(val: x)
    "#;
    let code_v2 = r#"
        var threshold: int = 10
        stream S = Tick .where(x > 0) .emit(val: x)
    "#;

    let program_v1 = parse(code_v1).expect("parse v1");
    let program_v2 = parse(code_v2).expect("parse v2");

    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program_v1).unwrap();

    engine.set_variable("threshold", Value::Int(99)).unwrap();
    assert_eq!(engine.get_variable("threshold"), Some(&Value::Int(99)));

    engine.reload(&program_v2).unwrap();

    // Variable should be preserved (not reset to initial value)
    assert_eq!(
        engine.get_variable("threshold"),
        Some(&Value::Int(99)),
        "Variables should be preserved across reload"
    );
}

// ===========================================================================
// 35. Aggregate with partition_by and having
// ===========================================================================

#[tokio::test]
async fn partitioned_aggregate_with_having() {
    let code = r#"
        stream S = Reading
            .partition_by(region)
            .window(2)
            .aggregate(total: sum(value))
            .having(total > 50.0)
            .emit(region: region, total: total)
    "#;
    let events = vec![
        // Region "east": 10 + 20 = 30 (fails having)
        Event::new("Reading")
            .with_field("region", Value::str("east"))
            .with_field("value", Value::Float(10.0)),
        Event::new("Reading")
            .with_field("region", Value::str("east"))
            .with_field("value", Value::Float(20.0)),
        // Region "west": 40 + 60 = 100 (passes having)
        Event::new("Reading")
            .with_field("region", Value::str("west"))
            .with_field("value", Value::Float(40.0)),
        Event::new("Reading")
            .with_field("region", Value::str("west"))
            .with_field("value", Value::Float(60.0)),
    ];
    let out = run(code, events).await;
    // Only "west" partition total (100) passes having > 50
    assert!(
        !out.is_empty(),
        "At least west partition should pass having filter"
    );
}

// ===========================================================================
// 36. load_with_source semantic validation
// ===========================================================================

#[tokio::test]
async fn load_with_source_validates() {
    let code = r#"
        stream S = Reading
            .emit(val: value)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    // load_with_source should succeed for a valid program
    engine.load_with_source(code, &program).unwrap();

    assert_eq!(engine.stream_names().len(), 1);
}

// ===========================================================================
// 37. Connector declaration
// ===========================================================================

#[tokio::test]
async fn connector_declaration_accessible() {
    let code = r#"
        connector my_mqtt = mqtt(url: "tcp://localhost:1883", topic: "data")
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    assert!(engine.get_connector("my_mqtt").is_some());
    let configs = engine.connector_configs();
    assert!(configs.contains_key("my_mqtt"));
}

// ===========================================================================
// 38. set_context_name
// ===========================================================================

#[tokio::test]
async fn set_context_name_does_not_panic() {
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.set_context_name("worker-1");
    // Just verify it doesn't panic
}
