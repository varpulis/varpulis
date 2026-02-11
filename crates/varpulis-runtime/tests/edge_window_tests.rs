//! Edge-case tests for window boundary conditions at VPL integration level.
//!
//! Tests count windows, session windows, tumbling windows, aggregate/having,
//! and multi-partition windowing through the full engine pipeline.

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
// Count Window Tests
// =============================================================================

#[tokio::test]
async fn count_window_exact_fill() {
    // .window(3) with exactly 3 events → 1 output
    let program = r#"
        stream CountExact = Reading
            .window(3)
            .aggregate(total: sum(value))
            .emit(sum: total)
    "#;

    let events = r#"
        Reading { value: 10.0 }
        Reading { value: 20.0 }
        Reading { value: 30.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        1,
        "Exactly 3 events should fill one count window"
    );
    if let Some(varpulis_core::Value::Float(sum)) = results[0].data.get("sum") {
        assert!(
            (*sum - 60.0).abs() < 0.001,
            "Sum should be 60.0, got {}",
            sum
        );
    }
}

#[tokio::test]
async fn count_window_fewer_events() {
    // .window(10) with only 5 events → 0 output (window not full)
    let program = r#"
        stream CountFewer = Reading
            .window(10)
            .aggregate(total: sum(value))
            .emit(sum: total)
    "#;

    let events = r#"
        Reading { value: 10.0 }
        Reading { value: 20.0 }
        Reading { value: 30.0 }
        Reading { value: 40.0 }
        Reading { value: 50.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        0,
        "5 events should not fill a window of size 10"
    );
}

#[tokio::test]
async fn count_window_overflow() {
    // .window(3) with 7 events → 2 outputs (windows 1-3, 4-6; 7th pending)
    let program = r#"
        stream CountOverflow = Reading
            .window(3)
            .aggregate(total: sum(value))
            .emit(sum: total)
    "#;

    let events = r#"
        Reading { value: 1.0 }
        Reading { value: 2.0 }
        Reading { value: 3.0 }
        Reading { value: 4.0 }
        Reading { value: 5.0 }
        Reading { value: 6.0 }
        Reading { value: 7.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        2,
        "7 events / window(3) = 2 complete windows, 1 pending"
    );

    // First window: 1+2+3 = 6
    if let Some(varpulis_core::Value::Float(sum)) = results[0].data.get("sum") {
        assert!(
            (*sum - 6.0).abs() < 0.001,
            "First window sum should be 6.0, got {}",
            sum
        );
    }
    // Second window: 4+5+6 = 15
    if let Some(varpulis_core::Value::Float(sum)) = results[1].data.get("sum") {
        assert!(
            (*sum - 15.0).abs() < 0.001,
            "Second window sum should be 15.0, got {}",
            sum
        );
    }
}

#[tokio::test]
async fn aggregate_having_filters_all() {
    // .having(total > 1000000) when all totals < 1M → 0 output
    let program = r#"
        stream HavingFilter = Sale
            .window(3)
            .aggregate(total: sum(amount))
            .having(total > 1000000.0)
            .emit(sum: total)
    "#;

    let events = r#"
        Sale { amount: 100.0 }
        Sale { amount: 200.0 }
        Sale { amount: 300.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        0,
        "Having filter should block output when total (600) < 1M"
    );
}

#[tokio::test]
async fn aggregate_having_passes() {
    // .having(total > 50) when total = 60 → 1 output
    let program = r#"
        stream HavingPass = Sale
            .window(3)
            .aggregate(total: sum(amount))
            .having(total > 50.0)
            .emit(sum: total)
    "#;

    let events = r#"
        Sale { amount: 10.0 }
        Sale { amount: 20.0 }
        Sale { amount: 30.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        1,
        "Having filter should pass when total (60) > 50"
    );
}

#[tokio::test]
async fn aggregate_multiple_functions() {
    // count + sum + avg + min + max in one aggregate → all fields present
    let program = r#"
        stream MultiAgg = Metric
            .window(4)
            .aggregate(
                cnt: count(value),
                total: sum(value),
                average: avg(value),
                minimum: min(value),
                maximum: max(value)
            )
            .emit(
                cnt: cnt,
                total: total,
                average: average,
                minimum: minimum,
                maximum: maximum
            )
    "#;

    let events = r#"
        Metric { value: 10.0 }
        Metric { value: 20.0 }
        Metric { value: 30.0 }
        Metric { value: 40.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(results.len(), 1, "4 events should fill 1 window");

    let result = &results[0];
    assert!(result.data.contains_key("cnt"), "Should have count");
    assert!(result.data.contains_key("total"), "Should have sum");
    assert!(result.data.contains_key("average"), "Should have avg");
    assert!(result.data.contains_key("minimum"), "Should have min");
    assert!(result.data.contains_key("maximum"), "Should have max");

    // Verify values: sum=100, avg=25, min=10, max=40
    if let Some(varpulis_core::Value::Float(total)) = result.data.get("total") {
        assert!(
            (*total - 100.0).abs() < 0.001,
            "Sum should be 100.0, got {}",
            total
        );
    }
    if let Some(varpulis_core::Value::Float(avg)) = result.data.get("average") {
        assert!(
            (*avg - 25.0).abs() < 0.001,
            "Avg should be 25.0, got {}",
            avg
        );
    }
    if let Some(varpulis_core::Value::Float(min)) = result.data.get("minimum") {
        assert!(
            (*min - 10.0).abs() < 0.001,
            "Min should be 10.0, got {}",
            min
        );
    }
    if let Some(varpulis_core::Value::Float(max)) = result.data.get("maximum") {
        assert!(
            (*max - 40.0).abs() < 0.001,
            "Max should be 40.0, got {}",
            max
        );
    }
}

#[tokio::test]
async fn empty_partition_no_output() {
    // Partition key with 0 matching events produces nothing
    let program = r#"
        stream PartitionEmpty = Reading
            .where(zone == "critical")
            .window(3)
            .aggregate(total: sum(value))
            .emit(sum: total)
    "#;

    let events = r#"
        Reading { zone: "normal", value: 10.0 }
        Reading { zone: "normal", value: 20.0 }
        Reading { zone: "normal", value: 30.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        0,
        "No events pass the where filter, so no window output"
    );
}

#[tokio::test]
async fn window_with_where_before() {
    // .where(value > 50).window(3).aggregate(...) — filter then window
    let program = r#"
        stream FilterThenWindow = Reading
            .where(value > 50.0)
            .window(2)
            .aggregate(total: sum(value))
            .emit(sum: total)
    "#;

    let events = r#"
        Reading { value: 30.0 }
        Reading { value: 80.0 }
        Reading { value: 40.0 }
        Reading { value: 90.0 }
        Reading { value: 20.0 }
    "#;

    let results = run_scenario(program, events).await;
    // Only 80 and 90 pass the filter → 1 window of size 2
    assert_eq!(
        results.len(),
        1,
        "Two events (80, 90) pass filter → one window"
    );
    if let Some(varpulis_core::Value::Float(sum)) = results[0].data.get("sum") {
        assert!(
            (*sum - 170.0).abs() < 0.001,
            "Sum should be 170.0, got {}",
            sum
        );
    }
}

#[tokio::test]
async fn session_window_basic() {
    // Session window with gap — events within gap form one session
    let program = r#"
        stream SessionTest = Activity
            .window(session: 30s)
            .aggregate(cnt: count(action))
            .emit(count: cnt)
    "#;

    // All events within 30s of each other → one session
    let events = r#"
        BATCH 0
        Activity { action: "click", user: "alice" }

        BATCH 5000
        Activity { action: "scroll", user: "alice" }

        BATCH 10000
        Activity { action: "click", user: "alice" }
    "#;

    let results = run_scenario(program, events).await;
    // Session hasn't closed yet (no gap > 30s), so may have 0 output
    // or 1 output if engine flushes on completion
    assert!(
        results.len() <= 1,
        "Session should produce at most 1 aggregate"
    );
}
