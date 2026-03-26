//! Integration tests for the pipeline trace/explain mode.

use tokio::sync::mpsc;
use varpulis_core::ast::Program;
use varpulis_runtime::engine::trace::TraceEntry;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

fn parse_program(source: &str) -> Program {
    varpulis_parser::parse(source).expect("Failed to parse")
}

#[tokio::test]
async fn test_trace_records_stream_match_for_filter_pipeline() {
    let source = r"
        event Tick:
            price: float

        stream HighTick = Tick
            .where(price > 100.0)
            .emit(price: price)
    ";

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();
    engine.set_trace_enabled(true);

    // Send event that passes the filter
    let tick = Event::new("Tick").with_field("price", 150.0);
    engine.process(tick).await.unwrap();

    let entries = engine.drain_trace();
    assert!(!entries.is_empty(), "trace should have entries");

    // First entry should be StreamMatched
    let matched = entries
        .iter()
        .find(|e| matches!(e, TraceEntry::StreamMatched { .. }));
    assert!(matched.is_some(), "should have a StreamMatched entry");
    if let Some(TraceEntry::StreamMatched {
        stream_name,
        event_type,
    }) = matched
    {
        assert_eq!(stream_name, "HighTick");
        assert_eq!(event_type, "Tick");
    }

    // Should have an EventEmitted entry (filter passed, emit produced output)
    let emitted = entries
        .iter()
        .find(|e| matches!(e, TraceEntry::EventEmitted { .. }));
    assert!(emitted.is_some(), "should have an EventEmitted entry");
}

#[tokio::test]
async fn test_trace_records_filter_block() {
    let source = r"
        event Tick:
            price: float

        stream HighTick = Tick
            .where(price > 100.0)
            .emit(price: price)
    ";

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();
    engine.set_trace_enabled(true);

    // Send event that does NOT pass the filter
    let tick = Event::new("Tick").with_field("price", 50.0);
    engine.process(tick).await.unwrap();

    let entries = engine.drain_trace();
    assert!(
        !entries.is_empty(),
        "trace should have entries even on filter block"
    );

    // Should still have StreamMatched (the event was routed)
    let matched = entries
        .iter()
        .any(|e| matches!(e, TraceEntry::StreamMatched { .. }));
    assert!(matched, "should have StreamMatched entry");

    // Filter should report BLOCK (passed=false)
    let filter_entry = entries
        .iter()
        .find(|e| matches!(e, TraceEntry::OperatorResult { passed: false, .. }));
    assert!(
        filter_entry.is_some(),
        "should have OperatorResult with passed=false"
    );

    // Should NOT have any EventEmitted (filter blocked)
    let emitted = entries
        .iter()
        .any(|e| matches!(e, TraceEntry::EventEmitted { .. }));
    assert!(!emitted, "should not have EventEmitted when filter blocks");
}

#[tokio::test]
async fn test_trace_records_sequence_pattern_state() {
    let source = r#"
        stream OrderPayment = Order
            -> Payment as payment
            .emit(status: "matched")
    "#;

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();
    engine.set_trace_enabled(true);

    // First event starts a partial run
    let order = Event::new("Order").with_field("id", 1i64);
    engine.process(order).await.unwrap();

    let entries = engine.drain_trace();
    let pattern = entries
        .iter()
        .find(|e| matches!(e, TraceEntry::PatternState { .. }));
    assert!(
        pattern.is_some(),
        "should have PatternState entry after first sequence event"
    );

    // Second event completes the sequence
    let payment = Event::new("Payment").with_field("order_id", 1i64);
    engine.process(payment).await.unwrap();

    let entries = engine.drain_trace();
    // Should have emitted event
    let emitted = entries
        .iter()
        .any(|e| matches!(e, TraceEntry::EventEmitted { .. }));
    assert!(emitted, "should have EventEmitted after sequence completes");
}

#[tokio::test]
async fn test_trace_disabled_produces_no_entries() {
    let source = r"
        event Tick:
            price: float

        stream HighTick = Tick
            .where(price > 100.0)
            .emit(price: price)
    ";

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();
    // Do NOT enable trace

    let tick = Event::new("Tick").with_field("price", 150.0);
    engine.process(tick).await.unwrap();

    let entries = engine.drain_trace();
    assert!(
        entries.is_empty(),
        "trace should produce no entries when disabled"
    );
}

#[tokio::test]
async fn test_trace_no_matching_stream() {
    let source = r"
        event Tick:
            price: float

        stream HighTick = Tick
            .where(price > 100.0)
            .emit(price: price)
    ";

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();
    engine.set_trace_enabled(true);

    // Send an event type that doesn't match any stream
    let unknown = Event::new("Unknown").with_field("x", 1i64);
    engine.process(unknown).await.unwrap();

    let entries = engine.drain_trace();
    // No stream matched, so no trace entries
    assert!(
        entries.is_empty(),
        "no entries when event matches no stream"
    );
}

#[tokio::test]
async fn test_trace_drain_clears_entries() {
    let source = r"
        event Tick:
            price: float

        stream HighTick = Tick
            .emit(price: price)
    ";

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();
    engine.set_trace_enabled(true);

    let tick1 = Event::new("Tick").with_field("price", 100.0);
    engine.process(tick1).await.unwrap();
    let entries1 = engine.drain_trace();
    assert!(!entries1.is_empty());

    // Second drain before processing another event should be empty
    let entries_empty = engine.drain_trace();
    assert!(entries_empty.is_empty(), "drain should clear entries");

    // Process another event
    let tick2 = Event::new("Tick").with_field("price", 200.0);
    engine.process(tick2).await.unwrap();
    let entries2 = engine.drain_trace();
    assert!(!entries2.is_empty(), "new entries after second event");
}
