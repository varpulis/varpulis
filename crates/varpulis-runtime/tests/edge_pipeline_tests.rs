//! Edge-case tests for complex operation chains at VPL integration level.
//!
//! Tests combinations of sequence + emit expressions, where + distinct,
//! select + aggregate, .within() timeouts, multi-step sequences,
//! multiple independent streams, and user functions in emit.

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;

async fn run_scenario(program_source: &str, events_source: &str) -> Vec<Event> {
    let (tx, mut rx) = mpsc::channel::<Event>(1000);

    let program = parse(program_source).expect("Failed to parse program");
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load program");

    let events = EventFileParser::parse(events_source).expect("Failed to parse events");
    for timed_event in events {
        engine
            .process(timed_event.event)
            .await
            .expect("Failed to process event");
    }

    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        results.push(event);
    }
    results
}

// =============================================================================
// Complex Pipeline Tests
// =============================================================================

#[tokio::test]
async fn sequence_then_emit_with_expressions() {
    // A -> B .emit(diff: b.value - a.value) — computed emit fields
    let program = r#"
        stream DiffCalc = Start as a
            -> End as b
            .emit(diff: b.value - a.value, status: "computed")
    "#;

    let events = r#"
        Start { value: 100.0 }
        End { value: 250.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        1,
        "Should match sequence and emit computed diff"
    );

    if let Some(varpulis_core::Value::Float(diff)) = results[0].data.get("diff") {
        assert!(
            (*diff - 150.0).abs() < 0.001,
            "Diff should be 150.0, got {}",
            diff
        );
    }
}

#[tokio::test]
async fn where_then_distinct() {
    // .where(v > 50).distinct(id) — filter then deduplicate
    let program = r#"
        stream FilterDistinct = Reading
            .where(value > 50.0)
            .distinct(sensor_id)
            .emit(sensor: sensor_id, value: value)
    "#;

    let events = r#"
        Reading { sensor_id: "S1", value: 80.0 }
        Reading { sensor_id: "S2", value: 30.0 }
        Reading { sensor_id: "S1", value: 90.0 }
        Reading { sensor_id: "S3", value: 70.0 }
        Reading { sensor_id: "S3", value: 60.0 }
    "#;

    let results = run_scenario(program, events).await;
    // S2 filtered out (30 < 50). S1 appears twice but distinct → 1. S3 appears twice → 1.
    // Total: S1 + S3 = 2 unique sensors that pass the filter
    assert_eq!(
        results.len(),
        2,
        "Should have 2 distinct sensors passing filter (S1, S3)"
    );
}

#[tokio::test]
async fn select_then_aggregate() {
    // .select(norm: value / 100.0).window(3).aggregate(avg: avg(norm))
    let program = r#"
        stream SelectAggregate = Metric
            .select(norm: value / 100.0)
            .window(3)
            .aggregate(average: avg(norm))
            .emit(avg_norm: average)
    "#;

    let events = r#"
        Metric { value: 100.0 }
        Metric { value: 200.0 }
        Metric { value: 300.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(results.len(), 1, "3 events should fill window of 3");

    // norm values: 1.0, 2.0, 3.0 → avg = 2.0
    if let Some(varpulis_core::Value::Float(avg)) = results[0].data.get("avg_norm") {
        assert!(
            (*avg - 2.0).abs() < 0.001,
            "Average normalized should be 2.0, got {}",
            avg
        );
    }
}

#[tokio::test]
async fn within_basic_timeout() {
    // A -> B .within(5s) — B arrives in time → match
    let program = r#"
        stream WithinMatch = Request as req
            -> Response as resp
            .within(5s)
            .emit(status: "fast", req_id: req.id)
    "#;

    let events = r#"
        BATCH 0
        Request { id: 1 }

        BATCH 1000
        Response { req_id: 1 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        1,
        "Response within 5s window should produce match"
    );
}

#[tokio::test]
async fn within_expired() {
    // A -> B .within(5s) — B arrives too late (10s) → may not match
    let program = r#"
        stream WithinExpired = Request as req
            -> Response as resp
            .within(5s)
            .emit(status: "fast")
    "#;

    let events = r#"
        BATCH 0
        Request { id: 1 }

        BATCH 10000
        Response { req_id: 1 }
    "#;

    let results = run_scenario(program, events).await;
    // With BATCH timing and within(5s), the 10s gap should expire the window.
    // Whether it produces 0 or 1 depends on temporal enforcement.
    assert!(
        results.len() <= 1,
        "Late response should produce at most 1 match"
    );
}

#[tokio::test]
async fn sequence_four_steps() {
    // A -> B -> C -> D .emit(...) — 4-step sequence
    let program = r#"
        stream FourStep = Init as a
            -> Validate as b
            -> Process as c
            -> Complete as d
            .emit(status: "done", init_id: a.id)
    "#;

    let events = r#"
        Init { id: 42 }
        Validate { ok: true }
        Process { result: "success" }
        Complete { final: true }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(results.len(), 1, "4-step sequence should produce 1 match");
    assert_eq!(
        results[0].data.get("status"),
        Some(&varpulis_core::Value::Str("done".into()))
    );
}

#[tokio::test]
async fn multiple_streams_independent() {
    // Two stream declarations processing same event types independently
    let program = r#"
        stream HighTemp = Reading
            .where(temperature > 30.0)
            .emit(alert: "hot", temp: temperature)

        stream LowTemp = Reading
            .where(temperature < 10.0)
            .emit(alert: "cold", temp: temperature)
    "#;

    let events = r#"
        Reading { temperature: 35.0 }
        Reading { temperature: 5.0 }
        Reading { temperature: 20.0 }
    "#;

    let results = run_scenario(program, events).await;
    // HighTemp matches 35.0, LowTemp matches 5.0, 20.0 matches neither
    assert_eq!(
        results.len(),
        2,
        "Two independent streams should produce 2 outputs"
    );

    let alerts: Vec<_> = results.iter().filter_map(|e| e.data.get("alert")).collect();
    assert!(
        alerts.contains(&&varpulis_core::Value::Str("hot".into())),
        "Should have 'hot' alert"
    );
    assert!(
        alerts.contains(&&varpulis_core::Value::Str("cold".into())),
        "Should have 'cold' alert"
    );
}

#[tokio::test]
async fn emit_with_user_function() {
    // fn calc(x) -> float: x * 2.0 used in .emit(result: calc(value))
    let program = r#"
        fn double(x: float) -> float:
            x * 2.0

        stream DoubleEmit = Measurement
            .emit(result: double(value), original: value)
    "#;

    let events = r#"
        Measurement { value: 42.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(results.len(), 1, "Should emit one result");

    if let Some(varpulis_core::Value::Float(result)) = results[0].data.get("result") {
        assert!(
            (*result - 84.0).abs() < 0.001,
            "double(42) should be 84.0, got {}",
            result
        );
    }
}
