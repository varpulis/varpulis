//! Audit C2b — full event-time windowing matrix.
//!
//! Covers, per the C2b design: (b) late-within-lateness admission,
//! (c) beyond-lateness drop / side-output routing, (f) end-of-input drain
//! and idle-source heartbeat, (g) no double-emit, (h) multi-source
//! min-watermark, (i) checkpoint/restore of binned mode, plus the sliding /
//! session / partitioned window variants and (j) a property test asserting
//! order-independence of emissions for events within `out_of_order`.
//!
//! (The fail-before/pass-after core cases (a)/(d)/(e) live in
//! `event_time_core.rs`, which compiles against pre-C2b code.)

use chrono::{DateTime, Utc};
use proptest::prelude::*;
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

fn num(event: &Event, field: &str) -> i64 {
    match event.data.get(field) {
        Some(varpulis_core::Value::Int(i)) => *i,
        Some(varpulis_core::Value::Float(f)) => *f as i64,
        other => panic!("missing numeric field {field}: {other:?}"),
    }
}

fn drain_n_s(rx: &mut mpsc::Receiver<Event>) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push((num(&e, "n"), num(&e, "s")));
    }
    out
}

// =============================================================================
// (b) Late-within-lateness: admitted to the resident bin, never re-emitted
// =============================================================================

#[test]
fn late_within_lateness_admitted_but_window_not_reemitted() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 0s)
            .allowed_lateness(2s)
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );

    // Watermark reaches 2s: window [0s,1s) graduates with two events.
    engine
        .process_batch_sync(vec![ev(0, 1), ev(500, 2), ev(2_000, 3)])
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), vec![(2, 3)]);

    // ts=0.7s is behind the watermark (2s) but within allowed_lateness (2s):
    // it passes the late-data gate and is admitted into the resident bin.
    // v1 semantics: the already-graduated window is NOT re-emitted.
    engine
        .process_batch_sync(vec![ev(700, 50)])
        .expect("process");
    assert_eq!(
        drain_n_s(&mut rx),
        Vec::<(i64, i64)>::new(),
        "graduated windows never re-emit (v1: accumulate, emit once)"
    );

    // Later windows must not contain the late event either.
    engine
        .process_batch_sync(vec![ev(3_500, 4)])
        .expect("process");
    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(1, 3), (1, 4)],
        "the admitted-late value 50 must not leak into any later window"
    );
}

// =============================================================================
// (c) Beyond-lateness: dropped, or routed to the configured side-output
// =============================================================================

#[test]
fn beyond_lateness_routed_to_side_output_within_lateness_admitted() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 0s)
            .allowed_lateness(1s)
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    // VPL cannot set the side-output stream yet (tracked follow-up) —
    // configure it through the engine API, as the C2b design prescribes.
    engine.set_late_data_side_output("W", "LateEvents");

    engine
        .process_batch_sync(vec![ev(0, 1), ev(2_000, 2)])
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), vec![(1, 1)]);

    // ts=0.5s < watermark(2s) − lateness(1s): beyond lateness → side-output.
    engine
        .process_batch_sync(vec![ev(500, 77)])
        .expect("process");
    let side: Vec<Event> = {
        let mut out = Vec::new();
        while let Ok(e) = rx.try_recv() {
            out.push(e);
        }
        out
    };
    assert_eq!(
        side.len(),
        1,
        "beyond-lateness event must reach the side-output"
    );
    assert_eq!(&*side[0].event_type, "LateEvents");
    assert_eq!(num(&side[0], "value"), 77);

    // ts=1.5s ≥ watermark − lateness: within lateness → admitted silently
    // (its window [1s,2s) already graduated → no emission, no side-output).
    engine
        .process_batch_sync(vec![ev(1_500, 3)])
        .expect("process");
    assert!(
        rx.try_recv().is_err(),
        "within-lateness event must not side-output"
    );

    // The final drain shows neither late event contaminated a later window.
    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(drain_n_s(&mut rx), vec![(1, 2)]);
}

// =============================================================================
// (f) Bounded-stream drain and idle-source heartbeat
// =============================================================================

#[test]
fn final_drain_graduates_open_windows_sync() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 2s)
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch_sync(vec![ev(0, 1), ev(500, 2)])
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), Vec::<(i64, i64)>::new());

    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(2, 3)],
        "end-of-input drain must graduate the still-open window"
    );

    // Idempotent: nothing left to drain.
    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(drain_n_s(&mut rx), Vec::<(i64, i64)>::new());
}

#[tokio::test]
async fn final_drain_graduates_open_windows_async() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 2s)
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch(vec![ev(0, 1), ev(500, 2)])
        .await
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), Vec::<(i64, i64)>::new());

    engine.flush_final_watermark().await.expect("drain");
    assert_eq!(drain_n_s(&mut rx), vec![(2, 3)]);
}

/// An idle-source heartbeat (external watermark advance) must close a
/// stalled window with no further events — the G4 case.
#[tokio::test]
async fn external_heartbeat_closes_stalled_window() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 0s)
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch(vec![ev(0, 1), ev(500, 2)])
        .await
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), Vec::<(i64, i64)>::new());

    engine
        .advance_external_watermark("SensorEvent", BASE_MS + 5_000)
        .await
        .expect("heartbeat");
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(2, 3)],
        "idle-source heartbeat must graduate the stalled window"
    );
}

// =============================================================================
// (g) No double-emit across the arrival/watermark/final-drain drivers
// =============================================================================

#[test]
fn no_double_emit_across_flush_drivers() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 0s)
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch_sync(vec![ev(0, 1), ev(1_000, 2)])
        .expect("process");
    // Redundant explicit flushes must not re-emit (monotonic gate).
    engine.flush_watermark_sync().expect("flush");
    engine.flush_watermark_sync().expect("flush");
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(1, 1)],
        "[0s,1s) emits exactly once"
    );

    engine.flush_final_watermark_sync().expect("drain");
    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(1, 2)],
        "[1s,2s) emits exactly once"
    );
}

// =============================================================================
// (h) Multi-source min-watermark
// =============================================================================

/// Two watermarked sources: the effective watermark is the MIN across
/// sources, so a fast source must not prematurely close windows a slow
/// source still feeds.
#[test]
fn multi_source_min_watermark_gates_window_close() {
    let program = parse(
        r"
        stream A = EvA
            .watermark(out_of_order: 0s)
            .window(1s)
            .aggregate(na: count())
            .emit(na: na)

        stream B = EvB
            .watermark(out_of_order: 0s)
            .window(1s)
            .aggregate(nb: count())
            .emit(nb: nb)
        ",
    )
    .expect("parse");
    let (tx, mut rx) = mpsc::channel::<Event>(1000);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let eva = |off: i64| {
        Event::new("EvA")
            .with_field("value", 1i64)
            .with_timestamp(ts(off))
    };
    let evb = |off: i64| {
        Event::new("EvB")
            .with_field("value", 1i64)
            .with_timestamp(ts(off))
    };

    // EvA races ahead to 5s, EvB sits at 0.5s → effective watermark 0.5s:
    // A's window [0s,1s) must NOT close yet.
    engine
        .process_batch_sync(vec![evb(500), eva(200), eva(800), eva(5_000)])
        .expect("process");
    assert!(
        rx.try_recv().is_err(),
        "effective watermark is min(5s, 0.5s) = 0.5s — no window may close"
    );

    // EvB advances to 2s → effective watermark 2s → both [0s,1s) windows close.
    engine
        .process_batch_sync(vec![evb(2_000)])
        .expect("process");
    let mut got: Vec<(String, i64)> = Vec::new();
    while let Ok(e) = rx.try_recv() {
        if let Some(v) = e.data.get("na") {
            got.push(("na".into(), v.as_int().unwrap_or(-1)));
        }
        if let Some(v) = e.data.get("nb") {
            got.push(("nb".into(), v.as_int().unwrap_or(-1)));
        }
    }
    got.sort();
    assert_eq!(
        got,
        vec![("na".to_string(), 2), ("nb".to_string(), 1)],
        "min watermark passage must close both streams' [0s,1s) windows"
    );
}

// =============================================================================
// (i) Checkpoint/restore of binned event-time state
// =============================================================================

#[test]
fn checkpoint_restore_mid_window_preserves_event_time_state() {
    let code = r"
        stream W = SensorEvent
            .watermark(out_of_order: 2s)
            .window(1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
    ";
    let (mut engine1, mut rx1) = engine_with(code);
    engine1
        .process_batch_sync(vec![ev(0, 1), ev(1_000, 2)])
        .expect("process");
    assert_eq!(
        drain_n_s(&mut rx1),
        Vec::<(i64, i64)>::new(),
        "nothing graduated yet"
    );

    // Snapshot mid-window, restore into a fresh engine.
    let cp = engine1.create_checkpoint();
    let (mut engine2, mut rx2) = engine_with(code);
    engine2.restore_checkpoint(&cp).expect("restore");

    // Feed the remainder to both engines; outputs must be identical.
    for engine in [&mut engine1, &mut engine2] {
        engine
            .process_batch_sync(vec![ev(3_500, 3)])
            .expect("process");
        engine.flush_final_watermark_sync().expect("drain");
    }
    let out1 = drain_n_s(&mut rx1);
    let out2 = drain_n_s(&mut rx2);
    assert_eq!(out1, out2, "restored engine must emit identically");
    assert_eq!(
        out1,
        vec![(1, 1), (1, 2), (1, 3)],
        "each 1s window holds exactly its own event across the restore"
    );
}

// =============================================================================
// Sliding / session / partitioned event-time variants
// =============================================================================

/// Low-ratio sliding (redirected to the binned implementation when
/// watermark-driven): overlapping 2s windows on a 1s grid.
#[test]
fn sliding_event_time_windows_overlap_correctly() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 0s)
            .window(2s, sliding: 1s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch_sync(vec![ev(500, 1), ev(1_500, 2), ev(3_500, 3)])
        .expect("process");
    // wm=3.5s → windows [-1s,1s), [0s,2s), [1s,3s) graduate.
    assert_eq!(drain_n_s(&mut rx), vec![(1, 1), (2, 3), (1, 2)]);

    engine.flush_final_watermark_sync().expect("drain");
    // [2s,4s) and [3s,5s) both hold the ts=3.5s event.
    assert_eq!(drain_n_s(&mut rx), vec![(1, 3), (1, 3)]);
}

/// Event-time sessions: out-of-order arrivals build the correct session set
/// (the arrival-driven session would mis-split and mis-merge these).
#[test]
fn session_event_time_out_of_order_sessions() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 10s)
            .window(session: 1500ms)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    // Arrival order 0s, 5s, 1s, 3s — event-time sessions with gap 1.5s are
    // {0s,1s}, {3s}, {5s}. out_of_order=10s keeps the watermark behind, so
    // everything graduates on the final drain, in session-start order.
    engine
        .process_batch_sync(vec![ev(0, 1), ev(5_000, 2), ev(1_000, 3), ev(3_000, 4)])
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), Vec::<(i64, i64)>::new());

    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(2, 4), (1, 4), (1, 2)],
        "sessions must be {{0,1}}, {{3}}, {{5}} regardless of arrival order"
    );
}

/// Event-time sessions close when the watermark passes end+gap, not on the
/// arrival of a later event.
#[test]
fn session_event_time_closes_on_watermark() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 1s)
            .window(session: 2s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    // ts=10s does NOT close session {0,1} on arrival; watermark is 9s which
    // does pass 1s+2s → the session graduates in the same batch's flush.
    engine
        .process_batch_sync(vec![ev(0, 1), ev(1_000, 2)])
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), Vec::<(i64, i64)>::new());

    engine
        .process_batch_sync(vec![ev(10_000, 3)])
        .expect("process");
    assert_eq!(drain_n_s(&mut rx), vec![(2, 3)]);

    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(drain_n_s(&mut rx), vec![(1, 3)]);
}

/// Partitioned event-time tumbling: per-key windows graduate separately and
/// deterministically (window start, then key).
#[test]
fn partitioned_event_time_windows_graduate_per_key() {
    let program = parse(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 0s)
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
    // Key "a" gets an out-of-order event (0.6s arrives after b's 0.3s and
    // a's 5s would have closed an arrival-driven window).
    engine
        .process_batch_sync(vec![
            pev(0, 1, "a"),
            pev(300, 2, "b"),
            pev(5_000, 99, "a"),
            pev(600, 3, "a"),
        ])
        .expect("process");
    // wm=5s → [0s,1s)×a (events 1,3 — including the out-of-order one) and
    // [0s,1s)×b graduate, ordered (window, key).
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(2, 4), (1, 2)],
        "per-key windows must graduate separately, keyed by event time"
    );

    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(drain_n_s(&mut rx), vec![(1, 99)]);
}

/// Partitioned event-time sessions.
#[test]
fn partitioned_event_time_sessions() {
    let program = parse(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 10s)
            .partition_by(dev)
            .window(session: 1s)
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
    // a: {0, 0.5}, {3}; b: {0.2} — interleaved arrival, one out-of-order.
    engine
        .process_batch_sync(vec![
            pev(0, 1, "a"),
            pev(200, 2, "b"),
            pev(3_000, 3, "a"),
            pev(500, 4, "a"),
        ])
        .expect("process");
    engine.flush_final_watermark_sync().expect("drain");
    assert_eq!(
        drain_n_s(&mut rx),
        vec![(2, 5), (1, 2), (1, 3)],
        "sessions ordered by (session start, key): a@0s, b@0.2s, a@3s"
    );
}

/// High-ratio sliding (≥10, the shape that already compiled to the binned
/// window): in watermark mode it emits grid windows, gated by the watermark.
#[test]
fn high_ratio_sliding_event_time_grid() {
    let (mut engine, mut rx) = engine_with(
        r"
        stream W = SensorEvent
            .watermark(out_of_order: 0s)
            .window(20s, sliding: 2s)
            .aggregate(n: count(), s: sum(value))
            .emit(n: n, s: s)
        ",
    );
    engine
        .process_batch_sync(vec![ev(1_000, 1), ev(21_000, 2)])
        .expect("process");
    // wm=21s → the 10 grid windows [-18s,2s)…[0s,20s) (all containing ts=1s)
    // graduate; every one holds exactly the ts=1s event.
    let first = drain_n_s(&mut rx);
    assert_eq!(first.len(), 10);
    assert!(first.iter().all(|&e| e == (1, 1)));

    engine.flush_final_watermark_sync().expect("drain");
    // The 10 grid windows [2s,22s)…[20s,40s) hold only ts=21s (the events
    // are exactly 20s apart, so no half-open 20s window contains both).
    let rest = drain_n_s(&mut rx);
    assert_eq!(rest.len(), 10);
    assert!(rest.iter().all(|&e| e == (1, 2)));
}

// =============================================================================
// (j) Property: emissions are order-independent within out_of_order
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// For any event set whose arrival displacement is bounded by
    /// `out_of_order`, the final window emissions equal those of the fully
    /// sorted feed — the defining correctness property of event-time
    /// windowing.
    #[test]
    fn emissions_are_arrival_order_independent(
        base in proptest::collection::vec(0i64..10_000, 4..24),
        deltas in proptest::collection::vec(-1_000i64..=1_000, 24),
    ) {
        // Sorted base arrival order, each timestamp perturbed by ±1s: the
        // displacement vs. fully-sorted order is < 2s = out_of_order, so no
        // event is ever late and no window may lose events.
        let mut base = base;
        base.sort_unstable();
        let arrival: Vec<i64> = base
            .iter()
            .zip(&deltas)
            .map(|(b, d)| b + d + 1_000)
            .collect();
        let mut sorted = arrival.clone();
        sorted.sort_unstable();

        let code = r"
            stream W = SensorEvent
                .watermark(out_of_order: 2s)
                .window(1s)
                .aggregate(n: count(), s: sum(value))
                .emit(n: n, s: s)
        ";
        let run = |feed: &[i64]| -> Vec<(i64, i64)> {
            let (mut engine, mut rx) = engine_with(code);
            let events: Vec<Event> = feed.iter().map(|&off| ev(off, off)).collect();
            engine.process_batch_sync(events).expect("process");
            engine.flush_final_watermark_sync().expect("drain");
            drain_n_s(&mut rx)
        };

        prop_assert_eq!(run(&arrival), run(&sorted));
    }
}
