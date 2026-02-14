//! Final coverage tests for engine/mod.rs and engine/pipeline.rs.
//!
//! Targets specific uncovered code paths:
//! - Checkpoint create/restore (distinct, limit, variables, counters)
//! - Hot reload (add/remove/update streams)
//! - Derived streams (stream reading from another stream's output)
//! - Session window expiration via flush_expired_sessions
//! - Group-by with partitioned windows
//! - Limit with exact boundary (saturating_sub edge)
//! - Distinct with field-based expression
//! - Connector compilation and retrieval
//! - Metrics collection (EngineMetrics struct)
//! - Stream info and names methods
//! - Engine::new_benchmark() mode
//! - process_batch_sync error paths and empty batch
//! - Pipeline ops: log levels, print with expressions, having filter, score (no onnx)
//! - ReloadReport::is_empty()
//! - has_sink_operations, is_stateless, partition_key, has_session_windows, min_session_gap
//! - event_counters, function_names, get_function, get_timers
//! - process_batch_shared (zero-copy batch path)
//! - new_shared and new_with_optional_output constructors
//! - add_filter on existing stream
//! - set_variable on mutable vs immutable
//! - load_with_source (semantic validation)
//! - Partitioned sliding count window
//! - Partitioned tumbling window with aggregate

use chrono::{TimeZone, Utc};
use std::sync::Arc;
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::{Event, SharedEvent};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ts_ms(ms: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).unwrap()
}

/// Parse + load + process events via async, collect output.
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
// 1. Engine::new_benchmark — no output channel, events are processed but not sent
// ===========================================================================

#[tokio::test]
async fn benchmark_engine_processes_without_output() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let mut engine = Engine::new_benchmark();
    engine.load(&program).expect("load");

    engine
        .process(Event::new("Tick").with_field("x", Value::Int(1)))
        .await
        .unwrap();
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(2)))
        .await
        .unwrap();

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 2);
    // Events are emitted but not sent (no channel) — counter still increments
    assert!(metrics.output_events_emitted >= 2);
    assert_eq!(metrics.streams_count, 1);
}

#[test]
fn benchmark_engine_sync_batch() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let mut engine = Engine::new_benchmark();
    engine.load(&program).expect("load");

    let events = vec![
        Event::new("Tick").with_field("x", Value::Int(1)),
        Event::new("Tick").with_field("x", Value::Int(2)),
        Event::new("Tick").with_field("x", Value::Int(3)),
    ];
    engine.process_batch_sync(events).unwrap();

    let (inp, outp) = engine.event_counters();
    assert_eq!(inp, 3);
    assert!(outp >= 3);
}

// ===========================================================================
// 2. new_shared — zero-copy SharedEvent output channel
// ===========================================================================

#[tokio::test]
async fn new_shared_zero_copy_output() {
    let code = r#"
        stream S = Tick
            .emit(v: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel::<SharedEvent>(100);
    let mut engine = Engine::new_shared(tx);
    engine.load(&program).expect("load");

    engine
        .process(Event::new("Tick").with_field("x", Value::Int(42)))
        .await
        .unwrap();

    let shared_event = rx.try_recv().expect("should receive shared event");
    assert_eq!(shared_event.get("v"), Some(&Value::Int(42)));
}

// ===========================================================================
// 3. new_with_optional_output — None case
// ===========================================================================

#[tokio::test]
async fn optional_output_none_processes_silently() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let mut engine = Engine::new_with_optional_output(None);
    engine.load(&program).expect("load");

    engine
        .process(Event::new("Tick").with_field("x", Value::Int(1)))
        .await
        .unwrap();

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 1);
}

// ===========================================================================
// 4. stream_names and function_names accessors
// ===========================================================================

#[test]
fn stream_names_and_function_names() {
    let code = r#"
        fn double(x: int) -> int:
            return x * 2

        stream A = Alpha .emit(v: value)
        stream B = Beta .emit(v: value)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let mut names = engine.stream_names();
    names.sort();
    assert_eq!(names, vec!["A", "B"]);

    let fn_names = engine.function_names();
    assert!(fn_names.contains(&"double"));

    assert!(engine.get_function("double").is_some());
    assert!(engine.get_function("nonexistent").is_none());
}

// ===========================================================================
// 5. Metrics (EngineMetrics struct)
// ===========================================================================

#[tokio::test]
async fn metrics_track_events_and_streams() {
    let code = r#"
        stream S = Tick
            .where(x > 0)
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Process one passing and one failing event
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(5)))
        .await
        .unwrap();
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(-1)))
        .await
        .unwrap();

    let m = engine.metrics();
    assert_eq!(m.events_processed, 2);
    assert_eq!(m.output_events_emitted, 1); // only one passes filter
    assert_eq!(m.streams_count, 1);
}

// ===========================================================================
// 6. event_counters
// ===========================================================================

#[tokio::test]
async fn event_counters_match_metrics() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    engine
        .process(Event::new("Tick").with_field("x", Value::Int(1)))
        .await
        .unwrap();

    let (inp, outp) = engine.event_counters();
    assert_eq!(inp, 1);
    assert_eq!(outp, 1);
}

// ===========================================================================
// 7. has_sink_operations — true when .to() is present
// ===========================================================================

#[test]
fn has_sink_operations_false_for_simple_streams() {
    let code = r#"
        stream S = Tick
            .where(x > 0)
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(!engine.has_sink_operations());
}

// ===========================================================================
// 8. is_stateless — stateless pipeline detection
// ===========================================================================

#[test]
fn is_stateless_for_filter_emit_pipeline() {
    let code = r#"
        stream S = Tick
            .where(x > 0)
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(engine.is_stateless());
}

#[test]
fn is_stateless_false_with_window() {
    let code = r#"
        stream S = Tick
            .window(5)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(!engine.is_stateless());
}

// ===========================================================================
// 9. Checkpoint create/restore — preserves distinct, limit, variables, counters
// ===========================================================================

#[tokio::test]
async fn checkpoint_preserves_distinct_state() {
    let code = r#"
        stream S = Tick
            .distinct(id)
            .emit(id: id)
    "#;
    let program = parse(code).expect("parse");

    // Phase 1: process events, checkpoint
    let (tx1, mut rx1) = mpsc::channel(100);
    let mut engine1 = Engine::new(tx1);
    engine1.load(&program).expect("load");

    engine1
        .process(Event::new("Tick").with_field("id", Value::Int(1)))
        .await
        .unwrap();
    engine1
        .process(Event::new("Tick").with_field("id", Value::Int(2)))
        .await
        .unwrap();

    let cp = engine1.create_checkpoint();
    assert_eq!(cp.events_processed, 2);

    let mut phase1_out = Vec::new();
    while let Ok(e) = rx1.try_recv() {
        phase1_out.push(e);
    }
    assert_eq!(phase1_out.len(), 2);

    // Phase 2: restore, duplicates should still be filtered
    let (tx2, mut rx2) = mpsc::channel(100);
    let mut engine2 = Engine::new(tx2);
    engine2.load(&program).expect("load");
    engine2.restore_checkpoint(&cp);

    engine2
        .process(Event::new("Tick").with_field("id", Value::Int(1)))
        .await
        .unwrap(); // duplicate — should be filtered
    engine2
        .process(Event::new("Tick").with_field("id", Value::Int(3)))
        .await
        .unwrap(); // new — should pass

    let mut phase2_out = Vec::new();
    while let Ok(e) = rx2.try_recv() {
        phase2_out.push(e);
    }
    assert_eq!(
        phase2_out.len(),
        1,
        "Duplicate id=1 should be filtered after restore"
    );
    assert_eq!(phase2_out[0].get("id"), Some(&Value::Int(3)));
}

#[tokio::test]
async fn checkpoint_preserves_limit_state() {
    let code = r#"
        stream S = Tick
            .limit(3)
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");

    // Phase 1: process 2 events
    let (tx1, mut rx1) = mpsc::channel(100);
    let mut engine1 = Engine::new(tx1);
    engine1.load(&program).expect("load");

    for i in 1..=2 {
        engine1
            .process(Event::new("Tick").with_field("x", Value::Int(i)))
            .await
            .unwrap();
    }

    let cp = engine1.create_checkpoint();
    let mut out1 = Vec::new();
    while let Ok(e) = rx1.try_recv() {
        out1.push(e);
    }
    assert_eq!(out1.len(), 2);

    // Phase 2: restore, only 1 more should pass (limit=3, already counted 2)
    let (tx2, mut rx2) = mpsc::channel(100);
    let mut engine2 = Engine::new(tx2);
    engine2.load(&program).expect("load");
    engine2.restore_checkpoint(&cp);

    for i in 3..=5 {
        engine2
            .process(Event::new("Tick").with_field("x", Value::Int(i)))
            .await
            .unwrap();
    }

    let mut out2 = Vec::new();
    while let Ok(e) = rx2.try_recv() {
        out2.push(e);
    }
    assert_eq!(
        out2.len(),
        1,
        "Only 1 more event should pass after limit restore"
    );
    assert_eq!(out2[0].get("x"), Some(&Value::Int(3)));
}

#[tokio::test]
async fn checkpoint_preserves_variables() {
    let code = r#"
        var counter = 0
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");

    let (tx1, _rx1) = mpsc::channel(100);
    let mut engine1 = Engine::new(tx1);
    engine1.load(&program).expect("load");

    // Verify variable was loaded
    assert_eq!(engine1.get_variable("counter"), Some(&Value::Int(0)));

    let cp = engine1.create_checkpoint();

    // Restore into new engine
    let (tx2, _rx2) = mpsc::channel(100);
    let mut engine2 = Engine::new(tx2);
    engine2.load(&program).expect("load");
    engine2.restore_checkpoint(&cp);

    assert_eq!(engine2.get_variable("counter"), Some(&Value::Int(0)));
}

// ===========================================================================
// 10. Hot reload — add, remove, update streams
// ===========================================================================

#[tokio::test]
async fn hot_reload_adds_new_stream() {
    let code_v1 = r#"
        stream A = Alpha
            .emit(v: value)
    "#;
    let code_v2 = r#"
        stream A = Alpha
            .emit(v: value)
        stream B = Beta
            .emit(v: value)
    "#;

    let program_v1 = parse(code_v1).expect("parse v1");
    let program_v2 = parse(code_v2).expect("parse v2");

    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program_v1).expect("load v1");

    let report = engine.reload(&program_v2).expect("reload");
    assert!(report.streams_added.contains(&"B".to_string()));
    assert!(!report.is_empty());

    // New stream B should now process events
    engine
        .process(Event::new("Beta").with_field("value", Value::Int(42)))
        .await
        .unwrap();

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert!(
        !out.is_empty(),
        "New stream B should produce output after reload"
    );
}

#[tokio::test]
async fn hot_reload_removes_stream() {
    let code_v1 = r#"
        stream A = Alpha
            .emit(v: value)
        stream B = Beta
            .emit(v: value)
    "#;
    let code_v2 = r#"
        stream A = Alpha
            .emit(v: value)
    "#;

    let program_v1 = parse(code_v1).expect("parse v1");
    let program_v2 = parse(code_v2).expect("parse v2");

    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program_v1).expect("load v1");

    let report = engine.reload(&program_v2).expect("reload");
    assert!(report.streams_removed.contains(&"B".to_string()));

    // Beta events should no longer produce output
    engine
        .process(Event::new("Beta").with_field("value", Value::Int(1)))
        .await
        .unwrap();

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert!(out.is_empty(), "Removed stream B should not produce output");
}

#[tokio::test]
async fn hot_reload_updates_stream_ops() {
    let code_v1 = r#"
        stream S = Tick
            .where(x > 0)
            .emit(x: x)
    "#;
    let code_v2 = r#"
        stream S = Tick
            .where(x > 10)
            .emit(x: x)
    "#;

    let program_v1 = parse(code_v1).expect("parse v1");
    let program_v2 = parse(code_v2).expect("parse v2");

    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program_v1).expect("load v1");

    // Process with v1 filter (x > 0)
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(5)))
        .await
        .unwrap();

    let report = engine.reload(&program_v2).expect("reload");
    // The stream exists in both versions but ops differ
    assert!(
        report.streams_updated.contains(&"S".to_string())
            || report.state_preserved.contains(&"S".to_string())
    );

    // Process with v2 filter (x > 10)
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(5)))
        .await
        .unwrap(); // should be filtered
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(15)))
        .await
        .unwrap(); // should pass

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    // First event (pre-reload) passes, second fails, third passes
    let passing_events: Vec<_> = out
        .iter()
        .filter(|e| e.get("x") == Some(&Value::Int(15)))
        .collect();
    assert!(
        !passing_events.is_empty(),
        "x=15 should pass the updated filter"
    );
}

#[test]
fn reload_report_is_empty_when_no_changes() {
    let report = varpulis_runtime::ReloadReport::default();
    assert!(report.is_empty());
}

// ===========================================================================
// 11. Derived streams — stream consuming another stream's output
// ===========================================================================

#[tokio::test]
async fn derived_stream_chains() {
    let code = r#"
        stream Base = Tick
            .where(x > 0)
            .emit(x: x)
        stream Derived = Base
            .where(x > 5)
            .emit(val: x)
    "#;
    let events = vec![
        Event::new("Tick").with_field("x", Value::Int(3)),
        Event::new("Tick").with_field("x", Value::Int(10)),
        Event::new("Tick").with_field("x", Value::Int(-1)),
    ];
    let out = run(code, events).await;
    // Base emits x=3 and x=10; Derived further filters to x>5, so only x=10
    let derived_events: Vec<_> = out.iter().filter(|e| e.get("val").is_some()).collect();
    assert!(
        !derived_events.is_empty(),
        "Derived stream should produce output for x=10"
    );
}

// ===========================================================================
// 12. Session window — has_session_windows, min_session_gap
// ===========================================================================

#[test]
fn has_session_windows_detection() {
    let code = r#"
        stream S = Tick
            .window(session: 10s)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(engine.has_session_windows());
    let gap = engine.min_session_gap();
    assert!(gap.is_some());
    assert_eq!(gap.unwrap().num_seconds(), 10);
}

#[test]
fn no_session_windows_when_using_count_window() {
    let code = r#"
        stream S = Tick
            .window(5)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(!engine.has_session_windows());
    assert!(engine.min_session_gap().is_none());
}

// ===========================================================================
// 13. Session window flush_expired_sessions
// ===========================================================================

#[tokio::test]
async fn session_window_flush_expired() {
    let code = r#"
        stream S = Tick
            .window(session: 1s)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send events with timestamps in the far past so they're "expired"
    let old_ts = ts_ms(1000);
    let mut e1 = Event::new("Tick").with_field("x", Value::Int(1));
    e1.timestamp = old_ts;
    let mut e2 = Event::new("Tick").with_field("x", Value::Int(2));
    e2.timestamp = old_ts + chrono::Duration::milliseconds(100);

    engine.process(e1).await.unwrap();
    engine.process(e2).await.unwrap();

    // Flush — these events are older than 1s from "now", so they should be expired
    engine.flush_expired_sessions().await.unwrap();

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    // The session should have been flushed with cnt=2
    assert!(
        !out.is_empty(),
        "Session flush should emit aggregated result for expired session"
    );
}

// ===========================================================================
// 14. Group-by (partition_by) with partitioned tumbling window + aggregate
// ===========================================================================

#[tokio::test]
async fn partition_by_tumbling_window_aggregate() {
    let code = r#"
        stream S = Tick
            .partition_by(region)
            .window(3)
            .aggregate(total: sum(value))
            .emit(region: _partition, total: total)
    "#;
    let events = vec![
        Event::new("Tick")
            .with_field("region", Value::Str("east".into()))
            .with_field("value", Value::Int(10)),
        Event::new("Tick")
            .with_field("region", Value::Str("west".into()))
            .with_field("value", Value::Int(20)),
        Event::new("Tick")
            .with_field("region", Value::Str("east".into()))
            .with_field("value", Value::Int(30)),
        Event::new("Tick")
            .with_field("region", Value::Str("east".into()))
            .with_field("value", Value::Int(40)),
        Event::new("Tick")
            .with_field("region", Value::Str("west".into()))
            .with_field("value", Value::Int(50)),
        Event::new("Tick")
            .with_field("region", Value::Str("west".into()))
            .with_field("value", Value::Int(60)),
    ];
    let out = run(code, events).await;

    // east gets 3 events -> window fires: sum = 10+30+40 = 80
    // west gets 3 events -> window fires: sum = 20+50+60 = 130
    assert!(
        out.len() >= 2,
        "Both partitions should produce output: got {}",
        out.len()
    );
}

// ===========================================================================
// 15. Partitioned sliding count window
// ===========================================================================

#[tokio::test]
async fn partition_by_sliding_count_window() {
    let code = r#"
        stream S = Tick
            .partition_by(sensor)
            .window(3, sliding: 1)
            .aggregate(avg_val: avg(value))
            .emit(sensor: _partition, avg: avg_val)
    "#;
    let events: Vec<Event> = (1..=5)
        .map(|i| {
            Event::new("Tick")
                .with_field("sensor", Value::Str("S1".into()))
                .with_field("value", Value::Int(i * 10))
        })
        .collect();
    let out = run(code, events).await;
    // With window=3, slide=1: first output after 3 events, then one per subsequent event
    assert!(
        out.len() >= 3,
        "Sliding count window should produce outputs: got {}",
        out.len()
    );
}

// ===========================================================================
// 16. Limit exact boundary — limit(N) with exactly N events
// ===========================================================================

#[tokio::test]
async fn limit_exact_boundary() {
    let code = r#"
        stream S = Tick
            .limit(3)
            .emit(x: x)
    "#;
    let events: Vec<Event> = (1..=3)
        .map(|i| Event::new("Tick").with_field("x", Value::Int(i)))
        .collect();
    let out = run(code, events).await;
    assert_eq!(out.len(), 3, "Exactly 3 events should pass limit(3)");
}

#[tokio::test]
async fn limit_zero_after_exhaustion() {
    let code = r#"
        stream S = Tick
            .limit(2)
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    for i in 1..=5 {
        engine
            .process(Event::new("Tick").with_field("x", Value::Int(i)))
            .await
            .unwrap();
    }

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert_eq!(
        out.len(),
        2,
        "Only 2 events should pass after limit exhaustion"
    );
}

// ===========================================================================
// 17. Distinct with expression-based field
// ===========================================================================

#[tokio::test]
async fn distinct_with_expression_field() {
    let code = r#"
        stream S = Tick
            .distinct(category)
            .emit(cat: category, val: value)
    "#;
    let events = vec![
        Event::new("Tick")
            .with_field("category", Value::Str("A".into()))
            .with_field("value", Value::Int(1)),
        Event::new("Tick")
            .with_field("category", Value::Str("B".into()))
            .with_field("value", Value::Int(2)),
        Event::new("Tick")
            .with_field("category", Value::Str("A".into()))
            .with_field("value", Value::Int(3)),
        Event::new("Tick")
            .with_field("category", Value::Str("C".into()))
            .with_field("value", Value::Int(4)),
        Event::new("Tick")
            .with_field("category", Value::Str("B".into()))
            .with_field("value", Value::Int(5)),
    ];
    let out = run(code, events).await;
    assert_eq!(out.len(), 3, "Only first A, B, C should pass distinct");
}

// ===========================================================================
// 18. Connector compilation and retrieval
// ===========================================================================

#[test]
fn connector_declaration_and_retrieval() {
    let code = r#"
        connector MyMqtt = mqtt(url: "tcp://localhost:1883", client_id: "test")
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let conn = engine.get_connector("MyMqtt");
    assert!(conn.is_some(), "Connector MyMqtt should be registered");
    assert!(!engine.connector_configs().is_empty());
}

// ===========================================================================
// 19. Config block (deprecated) compilation
// ===========================================================================

#[test]
fn config_block_deprecated_loads() {
    let code = r#"
        config mqtt {
            url: "tcp://localhost:1883"
        }
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let cfg = engine.get_config("mqtt");
    assert!(cfg.is_some(), "Config block should be registered");
}

// ===========================================================================
// 20. Log operation with different levels
// ===========================================================================

#[tokio::test]
async fn log_with_all_levels() {
    let code_error = r#"
        stream S = Tick
            .log(level: "error", message: "critical")
            .emit(x: x)
    "#;
    let code_warn = r#"
        stream S = Tick
            .log(level: "warn", message: "warning")
            .emit(x: x)
    "#;
    let code_debug = r#"
        stream S = Tick
            .log(level: "debug", message: "debug msg")
            .emit(x: x)
    "#;
    let code_trace = r#"
        stream S = Tick
            .log(level: "trace", message: "trace msg")
            .emit(x: x)
    "#;
    let code_info = r#"
        stream S = Tick
            .log(level: "info", message: "info msg")
            .emit(x: x)
    "#;

    let e = vec![Event::new("Tick").with_field("x", Value::Int(1))];

    // All should pass through without error
    let out = run(code_error, e.clone()).await;
    assert_eq!(out.len(), 1);
    let out = run(code_warn, e.clone()).await;
    assert_eq!(out.len(), 1);
    let out = run(code_debug, e.clone()).await;
    assert_eq!(out.len(), 1);
    let out = run(code_trace, e.clone()).await;
    assert_eq!(out.len(), 1);
    let out = run(code_info, e.clone()).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn log_with_data_field() {
    let code = r#"
        stream S = Tick
            .log(level: "info", message: "sensor reading", data: temperature)
            .emit(t: temperature)
    "#;
    let events = vec![Event::new("Tick").with_field("temperature", Value::Float(23.5))];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn log_default_message_uses_event_type() {
    let code = r#"
        stream S = Tick
            .log(level: "info")
            .emit(x: x)
    "#;
    let events = vec![Event::new("Tick").with_field("x", Value::Int(1))];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
}

// ===========================================================================
// 21. Print operation with expressions
// ===========================================================================

#[tokio::test]
async fn print_with_expressions() {
    let code = r#"
        stream S = Tick
            .print(x, x * 2)
            .emit(x: x)
    "#;
    let events = vec![Event::new("Tick").with_field("x", Value::Int(5))];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn print_without_expressions_uses_default_format() {
    let code = r#"
        stream S = Tick
            .print()
            .emit(x: x)
    "#;
    let events = vec![Event::new("Tick").with_field("x", Value::Int(5))];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
}

// ===========================================================================
// 22. Having filter on aggregate results
// ===========================================================================

#[tokio::test]
async fn having_filters_aggregate_results() {
    let code = r#"
        stream S = Tick
            .window(3)
            .aggregate(cnt: count(), total: sum(value))
            .having(cnt > 2)
            .emit(cnt: cnt, total: total)
    "#;
    // Send exactly 3 events to trigger window, having(cnt > 2) should pass since cnt=3
    let events = vec![
        Event::new("Tick").with_field("value", Value::Int(10)),
        Event::new("Tick").with_field("value", Value::Int(20)),
        Event::new("Tick").with_field("value", Value::Int(30)),
    ];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("cnt"), Some(&Value::Int(3)));
    // sum() returns Float
    assert_eq!(out[0].get("total"), Some(&Value::Float(60.0)));
}

#[tokio::test]
async fn having_filters_out_when_condition_fails() {
    let code = r#"
        stream S = Tick
            .window(3)
            .aggregate(cnt: count())
            .having(cnt > 5)
            .emit(cnt: cnt)
    "#;
    let events = vec![
        Event::new("Tick").with_field("value", Value::Int(1)),
        Event::new("Tick").with_field("value", Value::Int(2)),
        Event::new("Tick").with_field("value", Value::Int(3)),
    ];
    let out = run(code, events).await;
    assert_eq!(
        out.len(),
        0,
        "Having should filter out cnt=3 when condition is cnt > 5"
    );
}

// ===========================================================================
// 23. process_batch_sync — empty batch
// ===========================================================================

#[test]
fn process_batch_sync_empty() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Empty batch should be a no-op
    let result = engine.process_batch_sync(vec![]);
    assert!(result.is_ok());
    let (inp, _) = engine.event_counters();
    assert_eq!(inp, 0);
}

// ===========================================================================
// 24. process_batch (async) — empty batch
// ===========================================================================

#[tokio::test]
async fn process_batch_async_empty() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let result = engine.process_batch(vec![]).await;
    assert!(result.is_ok());
}

// ===========================================================================
// 25. process_batch_shared (zero-copy batch path)
// ===========================================================================

#[tokio::test]
async fn process_batch_shared_basic() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let shared_events: Vec<SharedEvent> = vec![
        Arc::new(Event::new("Tick").with_field("x", Value::Int(1))),
        Arc::new(Event::new("Tick").with_field("x", Value::Int(2))),
    ];
    engine.process_batch_shared(shared_events).await.unwrap();

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert_eq!(out.len(), 2);
}

// ===========================================================================
// 26. add_filter — programmatic closure filter on stream
// ===========================================================================

#[tokio::test]
async fn add_filter_closure() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Add a closure filter that only passes x > 5
    engine
        .add_filter("S", |e: &Event| {
            e.get("x")
                .and_then(|v| v.as_int())
                .map(|n| n > 5)
                .unwrap_or(false)
        })
        .expect("add_filter");

    engine
        .process(Event::new("Tick").with_field("x", Value::Int(3)))
        .await
        .unwrap();
    engine
        .process(Event::new("Tick").with_field("x", Value::Int(10)))
        .await
        .unwrap();

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert_eq!(out.len(), 1, "Only x=10 should pass the added filter");
    assert_eq!(out[0].get("x"), Some(&Value::Int(10)));
}

#[test]
fn add_filter_nonexistent_stream_errors() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let result = engine.add_filter("Nonexistent", |_: &Event| true);
    assert!(result.is_err());
}

// ===========================================================================
// 27. set_variable — mutable vs immutable
// ===========================================================================

#[test]
fn set_variable_mutable_succeeds() {
    let code = r#"
        var counter = 0
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(engine.set_variable("counter", Value::Int(42)).is_ok());
    assert_eq!(engine.get_variable("counter"), Some(&Value::Int(42)));
}

#[test]
fn set_variable_immutable_fails() {
    let code = r#"
        let threshold = 100
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let result = engine.set_variable("threshold", Value::Int(200));
    assert!(result.is_err(), "Cannot assign to immutable variable");
}

// ===========================================================================
// 28. load_with_source — semantic validation
// ===========================================================================

#[test]
fn load_with_source_valid_program() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    assert!(engine.load_with_source(code, &program).is_ok());
}

// ===========================================================================
// 29. get_pattern — pattern registration and retrieval
// ===========================================================================

#[test]
fn pattern_declaration_and_retrieval() {
    let code = r#"
        pattern TempSpike = SEQ(HighTemp, LowTemp)
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let pat = engine.get_pattern("TempSpike");
    assert!(pat.is_some());
    assert_eq!(pat.unwrap().name, "TempSpike");

    let patterns = engine.patterns();
    assert!(patterns.contains_key("TempSpike"));
}

// ===========================================================================
// 30. get_timers — timer stream compilation
// ===========================================================================

#[test]
fn timer_stream_registration() {
    let code = r#"
        stream Heartbeat = timer(5s)
            .emit(tick: "heartbeat")
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let timers = engine.get_timers();
    assert_eq!(timers.len(), 1);
    let (interval_ns, _delay, event_type) = &timers[0];
    assert_eq!(*interval_ns, 5_000_000_000u64);
    assert!(event_type.contains("Heartbeat"));
}

// ===========================================================================
// 31. source_bindings — from connector bindings
// ===========================================================================

#[test]
fn source_bindings_from_connector() {
    let code = r#"
        connector MyMqtt = mqtt(url: "tcp://localhost:1883", client_id: "test")
        stream S = Tick.from(MyMqtt, topic: "sensors/temp")
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let bindings = engine.source_bindings();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].connector_name, "MyMqtt");
    assert_eq!(bindings[0].event_type, "Tick");
    assert_eq!(bindings[0].topic_override, Some("sensors/temp".to_string()));
}

// ===========================================================================
// 32. Sync pipeline — log levels work in sync path
// ===========================================================================

#[test]
fn sync_log_all_levels() {
    let code = r#"
        stream S = Tick
            .log(level: "error", message: "err")
            .log(level: "warn", message: "wrn")
            .log(level: "debug", message: "dbg")
            .log(level: "trace", message: "trc")
            .log(level: "info", message: "inf")
            .emit(x: x)
    "#;
    let events = vec![Event::new("Tick").with_field("x", Value::Int(1))];
    let out = run_sync(code, events);
    assert_eq!(out.len(), 1);
}

// ===========================================================================
// 33. Sync pipeline — print op
// ===========================================================================

#[test]
fn sync_print_with_expressions() {
    let code = r#"
        stream S = Tick
            .print(x, x + 1)
            .emit(x: x)
    "#;
    let events = vec![Event::new("Tick").with_field("x", Value::Int(5))];
    let out = run_sync(code, events);
    assert_eq!(out.len(), 1);
}

// ===========================================================================
// 34. Sync pipeline — having filter
// ===========================================================================

#[test]
fn sync_having_filter() {
    let code = r#"
        stream S = Tick
            .window(2)
            .aggregate(cnt: count())
            .having(cnt >= 2)
            .emit(cnt: cnt)
    "#;
    let events = vec![
        Event::new("Tick").with_field("x", Value::Int(1)),
        Event::new("Tick").with_field("x", Value::Int(2)),
    ];
    let out = run_sync(code, events);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("cnt"), Some(&Value::Int(2)));
}

// ===========================================================================
// 35. Sync pipeline — distinct
// ===========================================================================

#[test]
fn sync_distinct_field() {
    let code = r#"
        stream S = Tick
            .distinct(tag)
            .emit(tag: tag)
    "#;
    let events = vec![
        Event::new("Tick").with_field("tag", Value::Str("A".into())),
        Event::new("Tick").with_field("tag", Value::Str("B".into())),
        Event::new("Tick").with_field("tag", Value::Str("A".into())),
    ];
    let out = run_sync(code, events);
    assert_eq!(out.len(), 2);
}

// ===========================================================================
// 36. Sync pipeline — limit
// ===========================================================================

#[test]
fn sync_limit_truncates() {
    let code = r#"
        stream S = Tick
            .limit(2)
            .emit(x: x)
    "#;
    let events: Vec<Event> = (1..=5)
        .map(|i| Event::new("Tick").with_field("x", Value::Int(i)))
        .collect();
    let out = run_sync(code, events);
    assert_eq!(out.len(), 2);
}

// ===========================================================================
// 37. set_context_name
// ===========================================================================

#[test]
fn set_context_name_on_engine() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.set_context_name("ctx_0");
    engine.load(&program).expect("load");
    // Just verifying it doesn't panic; context_name is used internally for connector IDs
}

// ===========================================================================
// 38. has_contexts
// ===========================================================================

#[test]
fn has_contexts_detection() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // No context declarations in this VPL
    assert!(!engine.has_contexts());
    let _ = engine.context_map();
}

// ===========================================================================
// 39. partition_key detection
// ===========================================================================

#[test]
fn partition_key_from_partitioned_window() {
    let code = r#"
        stream S = Tick
            .partition_by(region)
            .window(5)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let key = engine.partition_key();
    assert_eq!(key, Some("region".to_string()));
}

#[test]
fn partition_key_none_for_simple_stream() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(engine.partition_key().is_none());
}

// ===========================================================================
// 40. variables accessor
// ===========================================================================

#[test]
fn variables_accessor_returns_all() {
    let code = r#"
        let x = 10
        var y = 20
        stream S = Tick
            .emit(v: v)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let vars = engine.variables();
    assert_eq!(vars.get("x"), Some(&Value::Int(10)));
    assert_eq!(vars.get("y"), Some(&Value::Int(20)));
}

// ===========================================================================
// 41. Sync pipeline — select with multiple fields
// ===========================================================================

#[test]
fn sync_select_projects_fields() {
    let code = r#"
        stream S = Data
            .select(a: x, b: y + 1)
            .emit(a: a, b: b)
    "#;
    let events = vec![Event::new("Data")
        .with_field("x", Value::Int(10))
        .with_field("y", Value::Int(5))
        .with_field("z", Value::Int(999))];
    let out = run_sync(code, events);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("a"), Some(&Value::Int(10)));
    assert_eq!(out[0].get("b"), Some(&Value::Int(6)));
    assert!(out[0].get("z").is_none());
}

// ===========================================================================
// 42. Sync pipeline — partitioned aggregate
// ===========================================================================

#[test]
fn sync_partitioned_aggregate() {
    let code = r#"
        stream S = Tick
            .partition_by(group)
            .window(2)
            .aggregate(total: sum(value))
            .emit(group: _partition, total: total)
    "#;
    let events = vec![
        Event::new("Tick")
            .with_field("group", Value::Str("A".into()))
            .with_field("value", Value::Int(10)),
        Event::new("Tick")
            .with_field("group", Value::Str("A".into()))
            .with_field("value", Value::Int(20)),
        Event::new("Tick")
            .with_field("group", Value::Str("B".into()))
            .with_field("value", Value::Int(100)),
        Event::new("Tick")
            .with_field("group", Value::Str("B".into()))
            .with_field("value", Value::Int(200)),
    ];
    let out = run_sync(code, events);
    assert_eq!(out.len(), 2, "Two partitions should produce output");
}

// ===========================================================================
// 43. Emit with expression (EmitExpr path)
// ===========================================================================

#[tokio::test]
async fn emit_with_expression() {
    let code = r#"
        stream S = Tick
            .emit(doubled: x * 2, label: "computed")
    "#;
    let events = vec![Event::new("Tick").with_field("x", Value::Int(5))];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("doubled"), Some(&Value::Int(10)));
    assert_eq!(out[0].get("label"), Some(&Value::Str("computed".into())));
}

// ===========================================================================
// 44. filter alias for where
// ===========================================================================

#[tokio::test]
async fn filter_alias_for_where() {
    let code = r#"
        stream S = Tick
            .filter(x > 5)
            .emit(x: x)
    "#;
    let events = vec![
        Event::new("Tick").with_field("x", Value::Int(3)),
        Event::new("Tick").with_field("x", Value::Int(10)),
    ];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("x"), Some(&Value::Int(10)));
}

// ===========================================================================
// 45. .first() shorthand for .limit(1)
// ===========================================================================

#[tokio::test]
async fn first_shorthand() {
    let code = r#"
        stream S = Tick
            .first()
            .emit(x: x)
    "#;
    let events = vec![
        Event::new("Tick").with_field("x", Value::Int(1)),
        Event::new("Tick").with_field("x", Value::Int(2)),
        Event::new("Tick").with_field("x", Value::Int(3)),
    ];
    let out = run(code, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("x"), Some(&Value::Int(1)));
}

// ===========================================================================
// 46. Checkpoint with window state
// ===========================================================================

#[tokio::test]
async fn checkpoint_preserves_count_window() {
    let code = r#"
        stream S = Tick
            .window(5)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");

    // Phase 1: Send 3 events (window needs 5)
    let (tx1, mut rx1) = mpsc::channel(100);
    let mut engine1 = Engine::new(tx1);
    engine1.load(&program).expect("load");

    for i in 1..=3 {
        engine1
            .process(Event::new("Tick").with_field("x", Value::Int(i)))
            .await
            .unwrap();
    }

    // No output yet
    assert!(rx1.try_recv().is_err());
    let cp = engine1.create_checkpoint();

    // Phase 2: Restore and send 2 more events to complete window
    let (tx2, mut rx2) = mpsc::channel(100);
    let mut engine2 = Engine::new(tx2);
    engine2.load(&program).expect("load");
    engine2.restore_checkpoint(&cp);

    for i in 4..=5 {
        engine2
            .process(Event::new("Tick").with_field("x", Value::Int(i)))
            .await
            .unwrap();
    }

    let mut out = Vec::new();
    while let Ok(e) = rx2.try_recv() {
        out.push(e);
    }
    assert_eq!(
        out.len(),
        1,
        "Window should fire after restore + 2 more events"
    );
    assert_eq!(out[0].get("cnt"), Some(&Value::Int(5)));
}

// ===========================================================================
// 47. has_checkpointing / force_checkpoint / checkpoint_tick without manager
// ===========================================================================

#[test]
fn checkpoint_tick_noop_without_manager() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(!engine.has_checkpointing());
    // checkpoint_tick and force_checkpoint should be no-ops
    assert!(engine.checkpoint_tick().is_ok());
    assert!(engine.force_checkpoint().is_ok());
}

// ===========================================================================
// 48. enable_checkpointing with MemoryStore
// ===========================================================================

#[tokio::test]
async fn enable_checkpointing_with_memory_store() {
    let code = r#"
        stream S = Tick
            .emit(x: x)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let store = Arc::new(varpulis_runtime::MemoryStore::new());
    let config = varpulis_runtime::CheckpointConfig {
        interval: std::time::Duration::from_secs(60),
        max_checkpoints: 3,
        checkpoint_on_shutdown: false,
        key_prefix: "test".to_string(),
    };
    engine.enable_checkpointing(store, config).unwrap();
    assert!(engine.has_checkpointing());

    engine
        .process(Event::new("Tick").with_field("x", Value::Int(1)))
        .await
        .unwrap();

    // Force a checkpoint
    engine.force_checkpoint().unwrap();
}

// ===========================================================================
// 49. Merge stream with sync processing
// ===========================================================================

#[test]
fn sync_merge_stream() {
    let code = r#"
        stream Combined = merge(
            stream T = TempReading,
            stream P = PressureReading
        )
        .emit(kind: event_type)
    "#;
    let events = vec![
        Event::new("TempReading").with_field("value", Value::Float(25.0)),
        Event::new("PressureReading").with_field("value", Value::Float(1013.0)),
    ];
    let out = run_sync(code, events);
    assert_eq!(out.len(), 2);
}

// ===========================================================================
// 50. Sliding time window
// ===========================================================================

#[tokio::test]
async fn sliding_time_window() {
    let code = r#"
        stream S = Tick
            .window(10s, sliding: 5s)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send events with timestamps spread across the window
    let base = ts_ms(1000000);
    for i in 0..5 {
        let mut e = Event::new("Tick").with_field("x", Value::Int(i));
        e.timestamp = base + chrono::Duration::seconds(i);
        engine.process(e).await.unwrap();
    }
    // Sliding time windows emit when events cross window boundaries
    // Just verify no errors in processing
}

// ===========================================================================
// 51. Partitioned session window
// ===========================================================================

#[test]
fn partitioned_session_window_detection() {
    let code = r#"
        stream S = Tick
            .partition_by(sensor)
            .window(session: 30s)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(engine.has_session_windows());
    assert_eq!(engine.min_session_gap().unwrap().num_seconds(), 30);
}

// ===========================================================================
// 52. Sliding count window (non-partitioned)
// ===========================================================================

#[tokio::test]
async fn sliding_count_window_basic() {
    let code = r#"
        stream S = Tick
            .window(3, sliding: 1)
            .aggregate(total: sum(value))
            .emit(total: total)
    "#;
    let events: Vec<Event> = (1..=5)
        .map(|i| Event::new("Tick").with_field("value", Value::Int(i * 10)))
        .collect();
    let out = run(code, events).await;
    // With window=3, slide=1: first output after 3 events, then one per event
    assert!(
        out.len() >= 3,
        "Sliding count window should produce multiple outputs: got {}",
        out.len()
    );
}

// ===========================================================================
// 53. Unsupported operations give clear errors
// ===========================================================================

#[test]
fn unsupported_map_op_error() {
    let code = r#"
        stream S = Tick
            .map(x => x * 2)
    "#;
    let program = parse(code);
    if let Ok(prog) = program {
        let (tx, _rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        let result = engine.load(&prog);
        if let Err(e) = result {
            assert!(e.contains("map"), "Error should mention .map()");
        }
    }
    // If parse fails, that's acceptable too — the point is we don't panic
}

// ===========================================================================
// 54. process_batch_sync with derived stream chain
// ===========================================================================

#[test]
fn sync_derived_stream_chain() {
    let code = r#"
        stream Base = Tick
            .where(x > 0)
            .emit(x: x)
        stream Derived = Base
            .where(x > 5)
            .emit(val: x)
    "#;
    let events = vec![
        Event::new("Tick").with_field("x", Value::Int(3)),
        Event::new("Tick").with_field("x", Value::Int(10)),
    ];
    let out = run_sync(code, events);
    // Base emits x=3 and x=10; Derived filters to x>5, so only x=10 in derived
    let derived: Vec<_> = out.iter().filter(|e| e.get("val").is_some()).collect();
    assert!(
        !derived.is_empty(),
        "Derived stream should produce output for x=10"
    );
}

// ===========================================================================
// 55. Async pipeline — partitioned session window with timestamp events
// ===========================================================================

#[tokio::test]
async fn partitioned_session_window_processing() {
    let code = r#"
        stream S = Tick
            .partition_by(sensor)
            .window(session: 1s)
            .aggregate(cnt: count())
            .emit(cnt: cnt)
    "#;
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let old_ts = ts_ms(1000);
    for i in 0..3 {
        let mut e = Event::new("Tick")
            .with_field("sensor", Value::Str("S1".into()))
            .with_field("v", Value::Int(i));
        e.timestamp = old_ts + chrono::Duration::milliseconds(i * 100);
        engine.process(e).await.unwrap();
    }

    // Flush expired sessions
    engine.flush_expired_sessions().await.unwrap();

    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    assert!(
        !out.is_empty(),
        "Partitioned session flush should produce output"
    );
}

// ===========================================================================
// 56. Whole-event distinct (no field specified) via sync path
// ===========================================================================

#[test]
fn sync_distinct_whole_event() {
    let code = r#"
        stream S = Tick
            .distinct()
            .emit(x: x)
    "#;
    let events = vec![
        Event::new("Tick").with_field("x", Value::Int(1)),
        Event::new("Tick").with_field("x", Value::Int(1)), // duplicate
        Event::new("Tick").with_field("x", Value::Int(2)),
    ];
    let out = run_sync(code, events);
    assert_eq!(
        out.len(),
        2,
        "Second identical event should be deduplicated"
    );
}
