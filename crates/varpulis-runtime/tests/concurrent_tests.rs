//! `.concurrent()` is refused at compile time.
//!
//! This file used to assert that `.concurrent()` "worked": it partitioned
//! events across a rayon thread pool and merged the results. It passed, and it
//! proved nothing — the operator was a no-op. The pipeline arm returned
//! immediately for `len() <= 1`, the dispatcher never routed a `Concurrent`
//! stream to the only path that passes more than one event at a time, and the
//! "parallel" body was `.into_par_iter().map(|p| p)`. Every assertion here was
//! satisfied by the sequential path it was meant to be compared against, while
//! each stream still paid for a thread pool at load time.
//!
//! So the operator is now rejected the way the other unimplemented operators
//! are, and this file asserts the rejection — in every shape the old tests
//! used, so the day someone implements it these go red and force the record to
//! be updated rather than quietly passing again.

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

/// Load a program into a bare engine and return the load error, if any.
fn load_error(code: &str) -> Option<String> {
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel::<Event>(16);
    let mut engine = Engine::new(tx);
    engine.load(&program).err().map(|e| format!("{e}"))
}

fn assert_refused(label: &str, code: &str) {
    let err = load_error(code).unwrap_or_else(|| {
        panic!("{label}: .concurrent() must not compile, but the engine loaded it")
    });
    assert!(
        err.contains(".concurrent()") && err.contains("not yet implemented"),
        "{label}: error must say .concurrent() is not implemented, got: {err}"
    );
    assert!(
        err.contains("partition_by") || err.contains("context"),
        "{label}: error must name the alternative, got: {err}"
    );
}

#[test]
fn concurrent_where_emit_is_refused() {
    assert_refused(
        "where+emit",
        r"stream Filtered = Data
    .concurrent(workers: 4)
    .where(value > 50)
    .emit(value: value)",
    );
}

#[test]
fn concurrent_with_partition_key_is_refused() {
    assert_refused(
        "partition_key",
        r#"stream Partitioned = SensorReading
    .concurrent(workers: 4, partition_key: "sensor_id")
    .where(value > 0)
    .emit(sensor_id: sensor_id, value: value, seq: seq)"#,
    );
}

#[test]
fn concurrent_workers_1_is_refused_too() {
    // `workers: 1` was the shape that "proved" concurrency matched sequential
    // output. It matched because both were sequential.
    assert_refused(
        "workers:1",
        r"stream S = Data
    .concurrent(workers: 1)
    .where(value > 25)
    .emit(value: value)",
    );
}

#[test]
fn concurrent_with_projection_is_refused() {
    assert_refused(
        "projection",
        r"stream S = Data
    .concurrent(workers: 2)
    .where(value > 0)
    .emit(field1: field1, field2: field2)",
    );
}

#[test]
fn concurrent_after_a_sequence_is_refused() {
    assert_refused(
        "after sequence",
        r"stream S = EventA as a -> EventB where id == a.id as b .within(5m)
    .concurrent(workers: 2)
    .where(b.value > 10)
    .emit(id: a.id, value: b.value)",
    );
}

#[test]
fn the_same_program_without_concurrent_still_loads() {
    // The rejection is about `.concurrent()`, not about the pipeline it sits
    // in: the sequential form of the first case must still compile.
    assert_eq!(
        load_error(
            r"stream Filtered = Data
    .where(value > 50)
    .emit(value: value)"
        ),
        None,
        "removing .concurrent() must leave a program that loads"
    );
}
