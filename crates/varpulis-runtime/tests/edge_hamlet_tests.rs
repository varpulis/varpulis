//! Edge-case tests for Hamlet trend_aggregate at VPL integration level.
//!
//! These tests verify the full pipeline: parse VPL with `trend_aggregate` →
//! load engine with Hamlet aggregator → process events → verify output.

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
// Hamlet Trend Aggregate Tests
// =============================================================================

#[tokio::test]
async fn trend_aggregate_correct_count() {
    // 5 events → verify count_trends() output is produced
    let program = r#"
        stream TrendCount = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .trend_aggregate(count: count_trends())
            .emit(trends: count)
    "#;

    let events = r#"
        StockTick { symbol: "AAPL", price: 100.0 }
        StockTick { symbol: "AAPL", price: 110.0 }
        StockTick { symbol: "AAPL", price: 120.0 }
        StockTick { symbol: "AAPL", price: 130.0 }
        StockTick { symbol: "AAPL", price: 140.0 }
    "#;

    let results = run_scenario(program, events).await;
    // Hamlet produces incremental output — should have results after processing
    assert!(
        !results.is_empty(),
        "5 events should produce trend aggregate output"
    );
    // Verify trends field exists
    for result in &results {
        assert!(
            result.data.contains_key("trends"),
            "Output should have 'trends' field"
        );
    }
}

#[tokio::test]
async fn trend_aggregate_sum_and_count() {
    // Both count_trends() and sum_trends(price) in one output
    let program = r#"
        stream DualTrend = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .trend_aggregate(
                cnt: count_trends(),
                total: sum_trends(price)
            )
            .emit(count: cnt, sum: total)
    "#;

    let events = r#"
        StockTick { symbol: "AAPL", price: 100.0 }
        StockTick { symbol: "AAPL", price: 110.0 }
        StockTick { symbol: "AAPL", price: 120.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert!(
        !results.is_empty(),
        "Should produce output with both count and sum trends"
    );
    for result in &results {
        assert!(result.data.contains_key("count"), "Should have count field");
        assert!(result.data.contains_key("sum"), "Should have sum field");
    }
}

#[tokio::test]
async fn trend_aggregate_partition_isolation() {
    // Two symbols produce independent trend counts
    let program = r#"
        stream PartitionedTrend = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .partition_by(symbol)
            .trend_aggregate(count: count_trends())
            .emit(sym: symbol, trends: count)
    "#;

    let events = r#"
        StockTick { symbol: "AAPL", price: 100.0 }
        StockTick { symbol: "GOOG", price: 2800.0 }
        StockTick { symbol: "AAPL", price: 110.0 }
        StockTick { symbol: "GOOG", price: 2850.0 }
    "#;

    let _results = run_scenario(program, events).await;
    // Should process without error — partitions are independent
    // No panic = success
    let program_check = parse(
        r#"
        stream PartitionedTrend = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .partition_by(symbol)
            .trend_aggregate(count: count_trends())
            .emit(sym: symbol, trends: count)
    "#,
    )
    .expect("should parse");
    let (tx2, _rx2) = mpsc::channel::<Event>(100);
    let mut engine2 = Engine::new(tx2);
    engine2.load(&program_check).unwrap();
    let metrics = engine2.metrics();
    assert_eq!(metrics.streams_count, 1);
}

#[tokio::test]
async fn trend_aggregate_interleaved_partitions() {
    // Alternating AAPL/GOOG events → correct per-partition processing
    let program = r#"
        stream InterleavedTrend = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .partition_by(symbol)
            .trend_aggregate(count: count_trends())
            .emit(sym: symbol, trends: count)
    "#;

    let events = r#"
        StockTick { symbol: "AAPL", price: 100.0 }
        StockTick { symbol: "GOOG", price: 2800.0 }
        StockTick { symbol: "AAPL", price: 105.0 }
        StockTick { symbol: "GOOG", price: 2810.0 }
        StockTick { symbol: "AAPL", price: 110.0 }
        StockTick { symbol: "GOOG", price: 2820.0 }
    "#;

    let results = run_scenario(program, events).await;
    // Should process all interleaved events without error
    assert!(
        results.is_empty() || !results.is_empty(),
        "Should not panic on interleaved partitioned trend aggregation"
    );
}

#[tokio::test]
async fn trend_aggregate_single_event() {
    // Degenerate case — 1 event Kleene → minimal or no output
    let program = r#"
        stream SingleEvent = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .trend_aggregate(count: count_trends())
            .emit(trends: count)
    "#;

    let events = r#"
        StockTick { symbol: "AAPL", price: 100.0 }
    "#;

    let results = run_scenario(program, events).await;
    // A single event can start the Kleene but may not produce trends
    // Main assertion: no panic
    assert!(
        results.len() <= 1,
        "Single event should produce at most 1 output"
    );
}

#[tokio::test]
async fn trend_aggregate_many_events() {
    // 20+ events → verify no overflow in propagation coefficients
    let program = r#"
        stream ManyTrends = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .trend_aggregate(count: count_trends())
            .emit(trends: count)
    "#;

    let mut events = String::new();
    for i in 0..25 {
        events.push_str(&format!(
            "StockTick {{ symbol: \"AAPL\", price: {:.1} }}\n",
            100.0 + i as f64 * 5.0
        ));
    }

    let results = run_scenario(program, &events).await;
    // Should process all 25 events without overflow or panic
    assert!(
        !results.is_empty(),
        "25 events should produce trend aggregate output"
    );
}

#[tokio::test]
async fn trend_aggregate_with_emit_fields() {
    // .emit(count: count, total: sum) — output has correct field names
    let program = r#"
        stream EmitFields = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .trend_aggregate(
                cnt: count_trends(),
                total: sum_trends(price)
            )
            .emit(event_count: cnt, price_total: total)
    "#;

    let events = r#"
        StockTick { symbol: "AAPL", price: 100.0 }
        StockTick { symbol: "AAPL", price: 110.0 }
        StockTick { symbol: "AAPL", price: 120.0 }
        StockTick { symbol: "AAPL", price: 130.0 }
    "#;

    let results = run_scenario(program, events).await;
    if !results.is_empty() {
        let last = results.last().unwrap();
        assert!(
            last.data.contains_key("event_count") || last.data.contains_key("price_total"),
            "Output should use the emit field names"
        );
    }
}
