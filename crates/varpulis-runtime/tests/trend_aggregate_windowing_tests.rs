//! Regression test for the TrendAggregate unbounded-`accumulated` fix.
//!
//! Audit finding (Phase 2, resource-safety): `TrendAggregateConfig.accumulated`
//! grew without limit — every event was pushed and never evicted, so the
//! per-event count/sum re-scan was O(n²) and memory grew forever (OOM). The op
//! is windowed (WITHIN), so `accumulated` must be bounded to that same
//! event-time window. Observable consequence: the field aggregates
//! (`sum_trends`/`count_events`) reflect only the window, not all time.

use tokio::sync::mpsc;
use varpulis_core::Value;
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

/// A trend aggregate's `accumulated` buffer — the source of its `sum_trends` /
/// `count_events` field aggregates — must be bounded to the WITHIN window, not
/// grow for all time. We prove it via the observable sum across a window
/// boundary: batch A (price 100) at t=0s expires before batch B (price 1) at
/// t=100s under a 1s window, so a batch-B emit must sum ONLY batch B.
#[tokio::test]
async fn trend_accumulated_is_bounded_to_within_window() {
    let program = r"
        stream WindowedSum = StockTick as first
            -> all StockTick as rising
            .within(1s)
            .trend_aggregate(total: sum_trends(rising.price))
            .emit(sum: total)
    ";

    let events = r#"
        @0s   StockTick { symbol: "AAPL", price: 100.0 }
        @0s   StockTick { symbol: "AAPL", price: 100.0 }
        @0s   StockTick { symbol: "AAPL", price: 100.0 }
        @100s StockTick { symbol: "AAPL", price: 1.0 }
        @100s StockTick { symbol: "AAPL", price: 1.0 }
        @100s StockTick { symbol: "AAPL", price: 1.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert!(!results.is_empty(), "trend aggregate should emit results");

    let last_sum = results
        .last()
        .and_then(|r| r.data.get("sum"))
        .and_then(|v| match v {
            Value::Float(f) => Some(*f),
            _ => None,
        })
        .expect("final result must carry a float `sum`");

    // Windowed: batch A (300.0) has aged out; only batch B (3 * 1.0 = 3.0)
    // remains. If `accumulated` were unbounded (the bug), batch A would leak
    // into this emit and the sum would be 303.0.
    assert!(
        last_sum < 100.0,
        "sum after the window boundary must reflect ONLY the current window \
         (expected 3.0 from batch B); got {last_sum} — `accumulated` was not \
         bounded to the WITHIN window, so all-time events leaked in"
    );
}
