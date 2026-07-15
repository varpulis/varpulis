//! Integration tests for watermark features
//!
//! Each test is backed by a `.vpl` file in `tests/scenarios/`.
//! Tests validate:
//! - VPL `.watermark()` and `.allowed_lateness()` syntax parsing
//! - Per-source watermark tracking integration with the engine
//! - Watermark-triggered window closure

use chrono::{Duration, TimeZone, Utc};
use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::{Engine, Event};

/// Path to the test scenarios directory (relative to workspace root)
const SCENARIOS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/scenarios");

/// Load a VPL program from a scenario file
fn load_vpl(filename: &str) -> varpulis_core::ast::Program {
    let path = format!("{SCENARIOS_DIR}/{filename}");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"));
    parse(&source).unwrap_or_else(|e| panic!("Failed to parse {path}: {e:?}"))
}

/// Helper to create a test engine and output receiver
fn create_engine() -> (Engine, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel::<Event>(1000);
    (Engine::new(tx), rx)
}

#[test]
fn test_watermark_syntax_parses() {
    // Uses: watermark_windowed.vpl (has .watermark() + .window() + .aggregate())
    let path = format!("{SCENARIOS_DIR}/watermark_windowed.vpl");
    let source = std::fs::read_to_string(&path).expect("Failed to read VPL");
    let result = parse(&source);
    assert!(
        result.is_ok(),
        "Failed to parse .watermark(): {:?}",
        result.err()
    );
}

#[test]
fn test_allowed_lateness_syntax_parses() {
    // Uses: watermark_lateness.vpl (has .watermark() + .allowed_lateness() + .window())
    let path = format!("{SCENARIOS_DIR}/watermark_lateness.vpl");
    let source = std::fs::read_to_string(&path).expect("Failed to read VPL");
    let result = parse(&source);
    assert!(
        result.is_ok(),
        "Failed to parse .allowed_lateness(): {:?}",
        result.err()
    );
}

#[test]
fn test_watermark_and_lateness_combined_parses() {
    // Uses: watermark_lateness.vpl (combines both watermark + lateness)
    load_vpl("watermark_lateness.vpl");
    // If load_vpl succeeds, the combined syntax parses correctly
}

#[tokio::test]
async fn test_watermark_enables_tracking() {
    // Uses: watermark_basic.vpl
    let program = load_vpl("watermark_basic.vpl");
    let (mut engine, _rx) = create_engine();
    engine.load(&program).expect("Failed to load");

    // The engine should have watermark tracking enabled
    let event = Event::new("SensorEvent")
        .with_field("value", 42_i64)
        .with_timestamp(Utc::now());
    engine
        .process(event)
        .await
        .expect("Should process with watermark tracking");
}

#[tokio::test]
async fn test_watermark_advance_triggers_window() {
    // Uses: watermark_windowed.vpl + watermark_windowed.evt
    let program = load_vpl("watermark_windowed.vpl");
    let (mut engine, mut rx) = create_engine();
    engine.load(&program).expect("Failed to load");

    let base_time = Utc::now();

    // Add events within first window
    for i in 0..3 {
        let event = Event::new("SensorEvent")
            .with_field("value", i)
            .with_timestamp(base_time + Duration::seconds(i));
        engine.process(event).await.expect("Failed to process");
    }

    // Advance watermark past window boundary
    let event = Event::new("SensorEvent")
        .with_field("value", 99_i64)
        .with_timestamp(base_time + Duration::seconds(8));
    engine.process(event).await.expect("Failed to process");

    // Collect any output - the key thing is no crash and events are processed
    let mut outputs = Vec::new();
    while let Ok(event) = rx.try_recv() {
        outputs.push(event);
    }
}

#[tokio::test]
async fn test_per_source_watermark_with_engine() {
    // Uses: watermark_basic.vpl (but configures watermark programmatically for deeper test)
    let program = load_vpl("checkpoint_passthrough.vpl");
    let (mut engine, _rx) = create_engine();
    engine.load(&program).expect("Failed to load");

    // Enable watermark tracking programmatically
    engine.enable_watermark_tracking();
    engine.register_watermark_source("TestEvent", Duration::seconds(5));

    // Process events
    let base_time = Utc::now();
    for i in 0..5 {
        let event = Event::new("TestEvent")
            .with_field("value", i)
            .with_timestamp(base_time + Duration::seconds(i));
        engine.process(event).await.expect("Failed to process");
    }
}

// =============================================================================
// Audit C2a — watermark observation + late-data handling were wired ONLY into
// the single-event `process_inner` path. The batch paths (process_batch /
// process_batch_sync — the default `run`/`simulate` ingestion) silently skipped
// them: late events were folded into windows as if on-time, and the watermark
// never advanced under batch ingestion. Fix: a shared `pre_dispatch_watermark`
// pre-pass runs on every dispatch path.
// =============================================================================

/// Fixed event-time clock (deterministic, no `Utc::now()`).
fn c2_ts(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
}

#[test]
fn late_event_via_process_batch_sync_is_dropped() {
    // out_of_order:0 + allowed_lateness:0 → any event behind the watermark is
    // dropped. Feeding a late event through the SYNC batch path must exclude it
    // from the window aggregate. Before C2a the sync path never checked
    // lateness, so the count came out 4 instead of 3.
    let code = r"
        stream Windowed = SensorEvent
            .watermark(out_of_order: 0s)
            .allowed_lateness(0s)
            .window(10s)
            .aggregate(n: count())
            .emit(count: n)
    ";
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel::<Event>(1000);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let ev = |secs: i64, v: i64| {
        Event::new("SensorEvent")
            .with_field("value", v)
            .with_timestamp(c2_ts(secs))
    };
    engine
        .process_batch_sync(vec![
            ev(1, 1),
            ev(2, 2),
            ev(3, 3),  // effective watermark now at t=3s
            ev(1, 99), // LATE: t=1s < watermark, lateness 0 → must be dropped
            ev(11, 4), // crosses the 10s boundary → closes window [1s, 11s)
        ])
        .expect("process");

    let out = rx.try_recv().expect("window should close and emit");
    assert_eq!(
        out.data.get("count"),
        Some(&varpulis_core::Value::Int(3)),
        "late event must be excluded (would be 4 without the drop); got {:?}",
        out.data.get("count")
    );
}

#[test]
fn effective_watermark_advances_via_batch_ingestion() {
    // The batch paths now observe events into the watermark tracker, so the
    // effective watermark advances under batch ingestion. Before C2a only the
    // single-event process() path observed, so this accessor stayed None.
    let code = r"
        stream W = SensorEvent
            .watermark(out_of_order: 0s)
            .window(1m)
            .aggregate(n: count())
            .emit(count: n)
    ";
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel::<Event>(1000);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    assert!(
        engine.effective_watermark().is_none(),
        "no events observed yet → no watermark"
    );

    let ev = |secs: i64| {
        Event::new("SensorEvent")
            .with_field("value", 1_i64)
            .with_timestamp(c2_ts(secs))
    };
    engine
        .process_batch_sync(vec![ev(1), ev(2), ev(3)])
        .expect("process");

    assert!(
        engine.effective_watermark().is_some(),
        "batch ingestion must advance the effective watermark (was None before C2a)"
    );
    assert_eq!(
        engine.effective_watermark(),
        Some(c2_ts(3)),
        "watermark should equal max observed ts (out_of_order 0)"
    );
}
