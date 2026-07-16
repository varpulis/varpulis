//! Audit C2b — core event-time windowing semantics.
//!
//! Before C2b, time windows fired on EVENT ARRIVAL (`add_shared` emitted the
//! moment an event's timestamp crossed the window boundary), so
//! `.watermark(out_of_order: D)` was inert for windowing: an out-of-order
//! event whose window had "closed" one event ago was silently folded into
//! the WRONG window, corrupting aggregates. With C2b, a `.watermark()`
//! stream files events into bins by their own timestamp and a window closes
//! only when the watermark (`max_seen − out_of_order`) passes its end.
//!
//! This file intentionally uses only APIs that exist pre-C2b (engine load,
//! batch dispatch, output channel) so it can be run against the old code to
//! demonstrate fail-before/pass-after:
//! - the `oo_*` and `closes_on_watermark_*` tests FAIL on pre-C2b code;
//! - the `regression_guard_*` tests PASS unchanged on both (N1: zero change
//!   without `.watermark()`).

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::{Engine, Event};

/// Epoch-aligned event-time base (multiple of 1s and 10s window grids).
const BASE_MS: i64 = 1_700_000_000_000;

fn ts(offset_ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(BASE_MS + offset_ms).unwrap()
}

fn ev(offset_ms: i64, value: i64) -> Event {
    Event::new("SensorEvent")
        .with_field("value", value)
        .with_timestamp(ts(offset_ms))
}

fn engine_with(code: &str) -> (Engine, mpsc::Receiver<Event>) {
    let program = parse(code).expect("parse");
    let (tx, rx) = mpsc::channel::<Event>(1000);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    (engine, rx)
}

/// Numeric field extractor tolerant to Int/Float aggregation results.
fn num(event: &Event, field: &str) -> i64 {
    match event.data.get(field) {
        Some(varpulis_core::Value::Int(i)) => *i,
        Some(varpulis_core::Value::Float(f)) => *f as i64,
        other => panic!("missing numeric field {field}: {other:?}"),
    }
}

/// Drain the output channel into (count, sum) pairs.
fn drain_n_s(rx: &mut mpsc::Receiver<Event>) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push((num(&e, "n"), num(&e, "s")));
    }
    out
}

const WM_TUMBLING: &str = r"
    stream W = SensorEvent
        .watermark(out_of_order: 2s)
        .window(1s)
        .aggregate(n: count(), s: sum(value))
        .emit(n: n, s: s)
";

// =============================================================================
// (a) Out-of-order event lands in its own (still-open) window
// =============================================================================

/// Arrival order 0s, 1s, 3s, 2s with out_of_order=2s and a 1s window: the
/// ts=2s event arrives AFTER ts=3s crossed its boundary, but the watermark
/// (max−2s) has not passed 3s yet, so it must land in window [2s,3s).
/// A trailing ts=10s event pushes the watermark to 8s, graduating all four
/// windows. Pre-C2b the window fired on arrival: ts=2s was folded in with
/// ts=3s and the output was (1,1),(1,2),(2,7).
#[test]
fn oo_event_lands_in_correct_window_sync() {
    let (mut engine, mut rx) = engine_with(WM_TUMBLING);
    engine
        .process_batch_sync(vec![
            ev(0, 1),
            ev(1_000, 2),
            ev(3_000, 4),
            ev(2_000, 3), // out-of-order: belongs to [2s,3s)
            ev(10_000, 99),
        ])
        .expect("process");

    assert_eq!(
        drain_n_s(&mut rx),
        vec![(1, 1), (1, 2), (1, 3), (1, 4)],
        "each 1s window must contain exactly its own event; the out-of-order \
         ts=2s event must NOT be folded into a later window"
    );
}

/// Same as [`oo_event_lands_in_correct_window_sync`] through the async batch
/// path.
#[tokio::test]
async fn oo_event_lands_in_correct_window_async() {
    let (mut engine, mut rx) = engine_with(WM_TUMBLING);
    engine
        .process_batch(vec![
            ev(0, 1),
            ev(1_000, 2),
            ev(3_000, 4),
            ev(2_000, 3),
            ev(10_000, 99),
        ])
        .await
        .expect("process");

    assert_eq!(drain_n_s(&mut rx), vec![(1, 1), (1, 2), (1, 3), (1, 4)]);
}

// =============================================================================
// (d) Window closes on watermark passage, not on event arrival
// =============================================================================

/// With out_of_order=2s, an event at ts=2.9s must NOT close window [0s,1s)
/// (watermark = 0.9s < 1s). Only an event at ts=3.1s (watermark 1.1s) may.
/// Pre-C2b the window fired the moment ts=2.9s crossed the 1s boundary.
#[test]
fn closes_on_watermark_not_arrival_sync() {
    let (mut engine, mut rx) = engine_with(WM_TUMBLING);

    engine
        .process_batch_sync(vec![ev(0, 1), ev(2_900, 2)])
        .expect("process");
    assert_eq!(
        drain_n_s(&mut rx),
        Vec::<(i64, i64)>::new(),
        "watermark 0.9s has not passed window end 1s — nothing may emit yet"
    );

    engine
        .process_batch_sync(vec![ev(3_100, 3)])
        .expect("process");
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(1, 1)],
        "watermark 1.1s passed window end 1s — [0s,1s) must emit exactly once"
    );
}

/// Same as [`closes_on_watermark_not_arrival_sync`] through the async path.
#[tokio::test]
async fn closes_on_watermark_not_arrival_async() {
    let (mut engine, mut rx) = engine_with(WM_TUMBLING);

    engine
        .process_batch(vec![ev(0, 1), ev(2_900, 2)])
        .await
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), Vec::<(i64, i64)>::new());

    engine
        .process_batch(vec![ev(3_100, 3)])
        .await
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), vec![(1, 1)]);
}

// =============================================================================
// (e) N1 regression guards — ZERO change when no `.watermark()` is declared
// =============================================================================
//
// Golden outputs captured from pre-C2b `main`. These must stay byte-identical:
// the arrival-driven window paths are structurally untouched by C2b.

/// Tumbling+aggregate (the arrow feature fuses this shape into the streaming
/// columnar op on default builds — golden reflects that path).
#[test]
fn regression_guard_tumbling_no_watermark() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch_sync(vec![
            ev(0, 1),
            ev(1_000, 2),
            ev(3_000, 4),
            ev(2_000, 3),
            ev(10_000, 99),
        ])
        .expect("process");

    // NOTE: the fused columnar op advances its internal watermark to the
    // running max event time and flushes per ingest, so the out-of-order
    // ts=2s event (value 3) lands in an already-flushed bin and is DROPPED.
    // That is pre-C2b behavior and stays untouched without `.watermark()` —
    // declaring `.watermark(out_of_order: …)` is exactly what fixes it.
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(1, 1), (1, 2), (1, 4)],
        "no-watermark tumbling output must be identical to pre-C2b main"
    );
}

/// Low-ratio sliding window (never fused; the classic arrival-driven
/// SlidingWindow path).
#[test]
fn regression_guard_sliding_no_watermark() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .window(2s, sliding: 1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch_sync(vec![ev(0, 1), ev(500, 2), ev(1_500, 3), ev(3_000, 4)])
        .expect("process");

    assert_eq!(
        drain_n_s(&mut rx),
        vec![(1, 1), (3, 6), (2, 7)],
        "no-watermark sliding output must be identical to pre-C2b main"
    );
}

/// Session window (arrival-driven gap close).
#[test]
fn regression_guard_session_no_watermark() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .window(session: 2s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch_sync(vec![ev(0, 1), ev(1_000, 2), ev(5_000, 4)])
        .expect("process");

    assert_eq!(
        drain_n_s(&mut rx),
        vec![(2, 3)],
        "no-watermark session output must be identical to pre-C2b main"
    );
}

/// Partitioned tumbling+aggregate (fused on default builds).
#[test]
fn regression_guard_partitioned_tumbling_no_watermark() {
    let program = parse(
        r"
        stream W = SensorEvent
            .partition_by(dev)
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    )
    .expect("parse");
    let (tx, mut rx) = mpsc::channel::<Event>(1000);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let pev = |off: i64, v: i64, dev: &str| {
        Event::new("SensorEvent")
            .with_field("value", v)
            .with_field("dev", varpulis_core::Value::Str(dev.into()))
            .with_timestamp(ts(off))
    };
    engine
        .process_batch_sync(vec![
            pev(0, 1, "a"),
            pev(300, 2, "b"),
            pev(600, 3, "a"),
            pev(5_000, 99, "a"),
        ])
        .expect("process");

    let mut got = drain_n_s(&mut rx);
    got.sort_unstable();
    assert_eq!(
        got,
        vec![(1, 2), (2, 4)],
        "no-watermark partitioned tumbling output must be identical to pre-C2b main"
    );
}
