//! Tests for nested query functionality
//!
//! Nested queries allow streams to reference other streams as their source,
//! enabling multi-stage event processing pipelines.

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

/// Helper to create an event with data
fn make_event(event_type: &str, data: Vec<(&str, Value)>) -> Event {
    let mut event = Event::new(event_type);
    for (k, v) in data {
        event.data.insert(k.to_string(), v);
    }
    event
}

#[tokio::test]
async fn test_basic_stream_reference() {
    // Stream B references Stream A
    let code = r#"
        stream Ticks from Tick

        stream FilteredTicks = Ticks
            .where(symbol == "IBM")
            .emit(symbol: symbol, price: price)
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Send IBM tick - should pass through both streams
    let ibm_tick = make_event(
        "Tick",
        vec![
            ("symbol", Value::Str("IBM".to_string())),
            ("price", Value::Float(150.0)),
        ],
    );

    engine
        .process(ibm_tick)
        .await
        .expect("Process should succeed");

    // Send AAPL tick - should be filtered out
    let aapl_tick = make_event(
        "Tick",
        vec![
            ("symbol", Value::Str("AAPL".to_string())),
            ("price", Value::Float(175.0)),
        ],
    );

    engine
        .process(aapl_tick)
        .await
        .expect("Process should succeed");

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 2);
}

#[tokio::test]
async fn test_three_stage_pipeline() {
    // Three-stage pipeline: Raw -> Filtered -> Aggregated
    let code = r#"
        stream RawTicks from Tick

        stream FilteredTicks = RawTicks
            .where(price > 100.0)

        stream AggregatedTicks = FilteredTicks
            .window(3)
            .aggregate(count: count(), avg_price: avg(price))
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Send events that pass the filter (price > 100)
    for price in [150.0, 160.0, 170.0] {
        let tick = make_event(
            "Tick",
            vec![
                ("symbol", Value::Str("IBM".to_string())),
                ("price", Value::Float(price)),
            ],
        );
        engine.process(tick).await.expect("Process should succeed");
    }

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 3);
}

#[tokio::test]
async fn test_nested_with_output_event() {
    // Nested stream that generates output events
    let code = r#"
        stream Ticks from Tick

        stream HighPriceTicks = Ticks
            .where(price > 200.0)
            .emit(
                alert_type: "HighPrice",
                severity: "warning",
                message: "High price detected",
                symbol: symbol,
                price: price
            )
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, mut output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Send high price tick
    let tick = make_event(
        "Tick",
        vec![
            ("symbol", Value::Str("NVDA".to_string())),
            ("price", Value::Float(250.0)),
        ],
    );

    engine.process(tick).await.expect("Process should succeed");

    // Check output event was generated
    let output = output_rx.try_recv();
    assert!(output.is_ok(), "Should receive an output event");
}

#[tokio::test]
async fn test_parallel_derived_streams() {
    // Two streams derive from the same source
    let code = r#"
        stream Ticks from Tick

        stream HighTicks = Ticks
            .where(price > 150.0)

        stream LowTicks = Ticks
            .where(price < 50.0)
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Send ticks at different price levels
    for price in [25.0, 100.0, 200.0] {
        let tick = make_event("Tick", vec![("price", Value::Float(price))]);
        engine.process(tick).await.expect("Process should succeed");
    }

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 3);
}

#[tokio::test]
async fn test_diamond_dependency() {
    // Diamond pattern: A -> B, A -> C, B+C -> D (join)
    let code = r#"
        stream Source from Event

        stream Branch1 = Source
            .where(type == "a")

        stream Branch2 = Source
            .where(type == "b")
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Send events
    let event_a = make_event("Event", vec![("type", Value::Str("a".to_string()))]);
    let event_b = make_event("Event", vec![("type", Value::Str("b".to_string()))]);

    engine
        .process(event_a)
        .await
        .expect("Process should succeed");
    engine
        .process(event_b)
        .await
        .expect("Process should succeed");

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 2);
}

#[tokio::test]
async fn test_deep_nesting() {
    // Deep pipeline: 5 stages
    let code = r#"
        stream L1 from Event
        stream L2 = L1.where(level >= 1)
        stream L3 = L2.where(level >= 2)
        stream L4 = L3.where(level >= 3)
        stream L5 = L4.where(level >= 4)
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Event with level 5 should pass all stages
    let event = make_event("Event", vec![("level", Value::Int(5))]);

    engine.process(event).await.expect("Process should succeed");

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 1);
}

#[tokio::test]
async fn test_nested_with_aggregation_window() {
    // Nested stream with windowing and aggregation
    let code = r#"
        stream Trades from Trade

        stream BigTrades = Trades
            .where(amount > 1000.0)

        stream BigTradeStats = BigTrades
            .window(5)
            .aggregate(
                total: sum(amount),
                avg_amount: avg(amount),
                count: count()
            )
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Send big trades
    for i in 1..=5 {
        let trade = make_event(
            "Trade",
            vec![
                ("amount", Value::Float(2000.0 + (i as f64 * 100.0))),
                ("symbol", Value::Str("BTC".to_string())),
            ],
        );
        engine.process(trade).await.expect("Process should succeed");
    }

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 5);
}

#[tokio::test]
async fn test_nested_with_partition() {
    // Nested stream with partitioning
    let code = r#"
        stream Orders from Order

        stream HighValueOrders = Orders
            .where(total > 500.0)

        stream OrdersByCustomer = HighValueOrders
            .partition_by(customer_id)
            .window(10)
            .aggregate(order_count: count(), total_spent: sum(total))
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Send orders from different customers
    for (customer, total) in [("C1", 600.0), ("C1", 700.0), ("C2", 550.0)] {
        let order = make_event(
            "Order",
            vec![
                ("customer_id", Value::Str(customer.to_string())),
                ("total", Value::Float(total)),
            ],
        );
        engine.process(order).await.expect("Process should succeed");
    }

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 3);
}

#[tokio::test]
async fn test_stream_from_event_type_vs_stream() {
    // Verify that stream source is correctly resolved
    let code = r#"
        # Direct from event type
        stream DirectStream from SensorReading

        # From another stream
        stream DerivedStream = DirectStream
            .where(value > 0.0)
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    let reading = make_event(
        "SensorReading",
        vec![
            ("sensor_id", Value::Str("S1".to_string())),
            ("value", Value::Float(42.0)),
        ],
    );

    engine
        .process(reading)
        .await
        .expect("Process should succeed");

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 1);
}

#[tokio::test]
async fn test_chained_transforms() {
    // Multiple transforms in a chain
    let code = r#"
        stream Raw from Measurement

        stream Filtered = Raw
            .where(quality > 0.5)

        stream Enriched = Filtered
            .select(
                sensor: sensor_id,
                reading: value,
                quality_score: quality * 100.0
            )
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    let measurement = make_event(
        "Measurement",
        vec![
            ("sensor_id", Value::Str("TEMP01".to_string())),
            ("value", Value::Float(23.5)),
            ("quality", Value::Float(0.95)),
        ],
    );

    engine
        .process(measurement)
        .await
        .expect("Process should succeed");

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 1);
}
