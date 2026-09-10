//! The end-of-input drain must emit each closed window exactly once.
//!
//! `execute_pipeline` documents its contract: when `emitted_events` is
//! non-empty, `output_events` is a pass-through duplicate and the caller must
//! ignore it. The steady-state dispatch loop honours that. The end-of-input
//! drain, added so a bounded run stops losing its final window, did not — so
//! every window on a stream with an `.emit()` after its aggregate went out
//! twice.

use tokio::sync::mpsc;
use varpulis_core::{Event, Value};
use varpulis_runtime::engine::Engine;

/// Three 5-second tumbling windows over six events, with an `.emit()` after
/// the aggregate — the shape of the shipped `05_tumbling_window` example.
///
/// Fail-before: the final window is emitted twice, so this returns 4.
#[tokio::test]
async fn each_window_is_emitted_exactly_once_at_end_of_input() {
    let program = varpulis_parser::parse(
        r"event SensorReading:
    temperature: float

stream AvgTemperature = SensorReading
    .window(5s)
    .aggregate(avg_temp: avg(temperature), readings: count())
    .emit(avg_temp: avg_temp, readings: readings)
",
    )
    .expect("program must parse");

    let (tx, mut rx) = mpsc::channel::<Event>(256);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("program must load");

    // 0s,2s,4s | 6s,8s | 12s  →  three windows, the last still open at EOF.
    let batch: Vec<Event> = [
        (0i64, 20.0),
        (2, 22.0),
        (4, 25.0),
        (6, 30.0),
        (8, 28.0),
        (12, 18.0),
    ]
    .into_iter()
    .map(|(secs, t)| {
        let mut e = Event::new("SensorReading");
        e.timestamp = chrono::DateTime::from_timestamp(secs, 0).expect("valid timestamp");
        e.data.insert("temperature".into(), Value::Float(t));
        e
    })
    .collect();

    engine.process_batch(batch).await.expect("processing");
    engine
        .flush_end_of_input()
        .await
        .expect("end-of-input drain");
    drop(engine);

    let mut out = Vec::new();
    while let Some(e) = rx.recv().await {
        out.push(e);
    }

    let counts: Vec<i64> = out
        .iter()
        .map(|e| match e.data.get("readings") {
            Some(Value::Int(n)) => *n,
            other => panic!("expected a readings count, got {other:?}"),
        })
        .collect();

    assert_eq!(
        out.len(),
        3,
        "three windows over this input, each emitted once; got {counts:?}"
    );
    assert_eq!(
        counts,
        vec![3, 2, 1],
        "windows hold 3, 2 and 1 readings in order"
    );
}

/// The drain must still deliver a terminal aggregate that has no `.emit()`,
/// which is the case it was added for: there `emitted_events` is empty and
/// `output_events` carries the result.
#[tokio::test]
async fn a_terminal_aggregate_without_emit_still_reaches_the_channel() {
    let program = varpulis_parser::parse(
        r"event E:
    v: int

stream W = E
    .window(10s)
    .aggregate(n: count(), s: sum(v))
",
    )
    .expect("program must parse");

    let (tx, mut rx) = mpsc::channel::<Event>(64);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("program must load");

    let batch: Vec<Event> = (0..3)
        .map(|i| {
            let mut e = Event::new("E");
            e.timestamp = chrono::DateTime::from_timestamp(i, 0).expect("valid timestamp");
            e.data.insert("v".into(), Value::Int(i + 1));
            e
        })
        .collect();

    engine.process_batch(batch).await.expect("processing");
    engine
        .flush_end_of_input()
        .await
        .expect("end-of-input drain");
    drop(engine);

    let mut out = Vec::new();
    while let Some(e) = rx.recv().await {
        out.push(e);
    }

    assert_eq!(out.len(), 1, "one open window, drained once; got {out:?}");
    assert_eq!(out[0].data.get("n"), Some(&Value::Int(3)));
}
