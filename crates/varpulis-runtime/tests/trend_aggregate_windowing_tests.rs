//! A trend aggregate counts the trends of one window at a time.
//!
//! `.trend_aggregate()` once kept every event it had ever seen in a buffer
//! that nothing bounded: the field aggregates (`sum_trends`, `count_events`)
//! re-scanned it per event, O(n²), and it grew until the process died. The
//! window of the pattern's `.within()` is the bound: a trend lies inside
//! one window, and a window's result is computed from its events alone.

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
    engine
        .flush_end_of_input()
        .await
        .expect("Failed to close the windows");

    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        results.push(event);
    }
    results
}

/// Three ticks at 0s and three at 100s under a 1s window: the first window
/// closes when the 100s tick arrives, the second at the end of the input,
/// and each sums its own ticks.
///
/// Three ticks `a b c` with no condition: from `a`, the subsets of `{b, c}`
/// (three trends, `b` in two of them and `c` in two); from `b`, `{c}` (one
/// trend); from `c`, none. `b` takes part in two trends and `c` in three,
/// so `sum_trends(rising.price)` is `5 * price`: 500 for the first window,
/// 5 for the second. Were the buffer unbounded, the second window would
/// sum 505.
#[tokio::test]
async fn a_window_sums_its_own_events_only() {
    let program = r"
        stream WindowedSum = StockTick as first
            -> all StockTick as rising
            .within(1s)
            .trend_aggregate(trends: count_trends(), total: sum_trends(rising.price))
            .emit(trends: trends, sum: total)
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
    let sums: Vec<(Option<&Value>, Option<&Value>)> = results
        .iter()
        .map(|r| (r.data.get("trends"), r.data.get("sum")))
        .collect();
    assert_eq!(
        sums,
        vec![
            (Some(&Value::Int(4)), Some(&Value::Float(500.0))),
            (Some(&Value::Int(4)), Some(&Value::Float(5.0))),
        ],
        "{results:#?}"
    );
}
