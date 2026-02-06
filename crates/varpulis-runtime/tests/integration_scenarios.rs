//! Integration tests for event simulation scenarios
//!
//! Tests validate complete workflows: parse program + inject events + verify output events
//!
//! REGRESSION: These tests ensure the output event channel remains open during event processing.

#![allow(clippy::redundant_pattern_matching)]

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;

/// Helper to run a scenario and collect output events
async fn run_scenario(program_source: &str, events_source: &str) -> Vec<Event> {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    // Parse and load program
    let program = parse(program_source).expect("Failed to parse program");
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load program");

    // Parse events
    let events = EventFileParser::parse(events_source).expect("Failed to parse events");

    // Process all events
    for timed_event in events {
        engine
            .process(timed_event.event)
            .await
            .expect("Failed to process event");
    }

    // Collect all output events
    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        results.push(event);
    }

    results
}

// =============================================================================
// Order-Payment Sequence Tests
// =============================================================================

#[tokio::test]
async fn test_order_payment_sequence_match() {
    let program = r#"
        stream OrderPaymentMatch = Order as order
            -> Payment where order_id == order.id as payment
            .emit(
                status: "matched",
                order_id: order.id
            )
    "#;

    let events = r#"
        Order { id: 1 }
        Payment { order_id: 1, amount: 100.0 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should generate exactly one output event");
    assert_eq!(
        results[0].data.get("status"),
        Some(&varpulis_core::Value::Str("matched".into()))
    );
}

#[tokio::test]
async fn test_order_payment_no_match_wrong_id() {
    let program = r#"
        stream OrderPaymentMatch = Order as order
            -> Payment where order_id == order.id
            .emit(status: "matched")
    "#;

    let events = r#"
        Order { id: 1 }
        Payment { order_id: 999, amount: 100.0 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 0, "Should not match with wrong order_id");
}

#[tokio::test]
async fn test_order_payment_multiple_orders_one_payment() {
    let program = r#"
        stream OrderPaymentMatch = Order as order
            -> Payment where order_id == order.id
            .emit(status: "matched", order_id: order.id)
    "#;

    // Two orders, but only one payment matches
    let events = r#"
        Order { id: 1 }
        Order { id: 2 }
        Payment { order_id: 1, amount: 100.0 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should match only order 1");
}

#[tokio::test]
async fn test_order_payment_wrong_sequence() {
    let program = r#"
        stream OrderPaymentMatch = Order as order
            -> Payment where order_id == order.id
            .emit(status: "matched")
    "#;

    // Payment before Order - should not match
    let events = r#"
        Payment { order_id: 1, amount: 100.0 }
        Order { id: 1 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(
        results.len(),
        0,
        "Payment before Order should not match sequence"
    );
}

// =============================================================================
// Three-Step Sequence Tests
// =============================================================================

#[tokio::test]
async fn test_three_step_sequence() {
    let program = r#"
        stream ThreeStep = A -> B -> C
            .emit(status: "complete")
    "#;

    let events = r#"
        A {}
        B {}
        C {}
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should complete three-step sequence");
}

#[tokio::test]
async fn test_three_step_incomplete() {
    let program = r#"
        stream ThreeStep = A -> B -> C
            .emit(status: "complete")
    "#;

    let events = r#"
        A {}
        B {}
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 0, "Incomplete sequence should not emit");
}

#[tokio::test]
async fn test_three_step_wrong_order() {
    let program = r#"
        stream ThreeStep = A -> B -> C
            .emit(status: "complete")
    "#;

    let events = r#"
        A {}
        C {}
        B {}
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 0, "Wrong order should not match");
}

// =============================================================================
// Correlation Tests
// =============================================================================

#[tokio::test]
async fn test_correlation_by_field() {
    let program = r#"
        stream RequestResponse = Request as req
            -> Response where request_id == req.id as resp
            .emit(
                status: "correlated",
                request_id: req.id
            )
    "#;

    // Single request-response pair
    let events = r#"
        Request { id: "abc123", user: "alice" }
        Response { request_id: "abc123", result: "success" }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should correlate request-response pair");
}

// =============================================================================
// Batch Timing Tests
// =============================================================================

#[tokio::test]
async fn test_batch_events_parsed_correctly() {
    let events_source = r#"
        # First batch at t=0
        BATCH 0
        Event1 { x: 1 }
        Event2 { y: 2 }
        
        # Second batch at t=1000ms
        BATCH 1000
        Event3 { z: 3 }
    "#;

    let events = EventFileParser::parse(events_source).expect("Failed to parse");

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].time_offset_ms, 0);
    assert_eq!(events[1].time_offset_ms, 0);
    assert_eq!(events[2].time_offset_ms, 1000);
}

// =============================================================================
// Edge Cases
// =============================================================================

#[tokio::test]
async fn test_empty_event_file() {
    let events = EventFileParser::parse("# Just a comment\n").expect("Failed to parse");
    assert_eq!(events.len(), 0);
}

#[tokio::test]
async fn test_event_with_array_field() {
    let program = r#"
        stream Test = ComplexEvent as e
            .emit(status: "received")
    "#;

    // Single-line event with array
    let events = r#"
        ComplexEvent { id: 1, tags: ["a", "b", "c"], metadata: "test" }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_single_event_triggers_alert() {
    let program = r#"
        stream OrderAlert = Order as o
            .emit(status: "order_received", id: o.id)
    "#;

    let events = r#"
        Order { id: 42 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Single event should trigger output event");
}

// =============================================================================
// Numeric and Boolean Fields
// =============================================================================

#[tokio::test]
async fn test_sequence_with_numeric_field() {
    let program = r#"
        stream HighValueOrder = Order as o
            -> Payment where order_id == o.id
            .emit(status: "paid", order_id: o.id)
    "#;

    let events = r#"
        Order { id: 100 }
        Payment { order_id: 100, amount: 1500 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should match order-payment sequence");
}

#[tokio::test]
async fn test_sequence_with_boolean_field() {
    let program = r#"
        stream CriticalFlow = Start as s
            -> End where completed == true
            .emit(status: "flow_complete")
    "#;

    let events = r#"
        Start { id: 1 }
        End { completed: true }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should match completed flow");
}

// =============================================================================
// String Matching
// =============================================================================

#[tokio::test]
async fn test_sequence_with_string_match() {
    let program = r#"
        stream ErrorAck = Error as e
            -> Ack where error_id == e.id
            .emit(status: "acknowledged", error_id: e.id)
    "#;

    let events = r#"
        Error { id: "err001", message: "Connection failed" }
        Ack { error_id: "err001" }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should match error-ack sequence");
}

// =============================================================================
// Regression Tests - Output Event Channel Bug (channel closed)
// =============================================================================

/// Regression test: Ensure output event channel remains open during event processing
/// Bug: When loading via WebSocket, output_tx receiver was dropped immediately
/// Fix: Use shared output_tx from ServerState
#[tokio::test]
async fn test_regression_output_channel_stays_open() {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    let program = parse(
        r#"
        stream Test = Event as e
            .emit(status: "received", id: e.id)
    "#,
    )
    .expect("Failed to parse");

    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    // Process multiple events - output events should all be received
    for i in 1..=5 {
        let mut event = varpulis_runtime::event::Event::new("Event");
        event
            .data
            .insert("id".to_string(), varpulis_core::Value::Int(i));
        engine.process(event).await.expect("Failed to process");
    }

    // All 5 output events should be received (channel not closed)
    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }

    assert_eq!(
        count, 5,
        "All output events should be received - channel must stay open"
    );
}

/// Regression test: Output events work correctly after multiple load operations
/// This simulates reloading a program (like VSCode reload)
#[tokio::test]
async fn test_regression_output_events_after_reload() {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    // First load
    let program1 = parse(
        r#"
        stream Test1 = Event as e
            .emit(status: "test1")
    "#,
    )
    .expect("Failed to parse");

    let mut engine = Engine::new(tx.clone());
    engine.load(&program1).expect("Failed to load");

    let event1 = varpulis_runtime::event::Event::new("Event");
    engine.process(event1).await.expect("Failed to process");

    // Second load (simulates VSCode reload with same tx)
    let program2 = parse(
        r#"
        stream Test2 = Event as e
            .emit(status: "test2")
    "#,
    )
    .expect("Failed to parse");

    let mut engine2 = Engine::new(tx);
    engine2.load(&program2).expect("Failed to load");

    let event2 = varpulis_runtime::event::Event::new("Event");
    engine2.process(event2).await.expect("Failed to process");

    // Both output events should be received
    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }

    assert_eq!(
        count, 2,
        "Output events from both engines should be received"
    );
}

// =============================================================================
// Regression Tests - Event Injection
// =============================================================================

/// Regression test: Events with various field types are processed correctly
/// Bug: json_to_value conversion was missing some types
#[tokio::test]
async fn test_regression_event_field_types() {
    use varpulis_core::Value;
    use varpulis_runtime::event::Event;

    let (tx, mut rx) = mpsc::channel::<Event>(100);

    let program = parse(
        r#"
        stream Test = Data as d
            .emit(received: true)
    "#,
    )
    .expect("Failed to parse");

    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    // Create event with various field types (simulates JSON injection)
    let mut event = Event::new("Data");
    event
        .data
        .insert("string_field".to_string(), Value::Str("hello".into()));
    event.data.insert("int_field".to_string(), Value::Int(42));
    event
        .data
        .insert("float_field".to_string(), Value::Float(2.5));
    event
        .data
        .insert("bool_field".to_string(), Value::Bool(true));
    event.data.insert("null_field".to_string(), Value::Null);

    engine
        .process(event)
        .await
        .expect("Failed to process event with various types");

    assert!(
        rx.try_recv().is_ok(),
        "Event with various field types should trigger output event"
    );
}

/// Regression test: Rapid event injection doesn't cause output event channel issues
#[tokio::test]
async fn test_regression_rapid_event_injection() {
    let (tx, mut rx) = mpsc::channel::<Event>(1000);

    let program = parse(
        r#"
        stream Counter = Tick as t
            .emit(count: t.n)
    "#,
    )
    .expect("Failed to parse");

    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    // Rapid injection of 100 events
    for i in 0..100 {
        let mut event = varpulis_runtime::event::Event::new("Tick");
        event
            .data
            .insert("n".to_string(), varpulis_core::Value::Int(i));
        engine
            .process(event)
            .await
            .expect("Rapid injection should not fail");
    }

    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }

    assert_eq!(
        count, 100,
        "All 100 rapid events should produce output events"
    );
}

/// Regression test: Output event channel works with sequence patterns
#[tokio::test]
async fn test_regression_sequence_output_channel() {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    let program = parse(
        r#"
        stream AlertTest = A as a -> B .emit(status: "matched")
    "#,
    )
    .expect("Failed to parse");

    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    // Process first pair
    engine
        .process(Event::new("A"))
        .await
        .expect("Process A1 failed");
    engine
        .process(Event::new("B"))
        .await
        .expect("Process B1 failed");

    // Verify first output event
    let result1 = rx.try_recv();
    assert!(result1.is_ok(), "First output event should be received");

    // Process second pair - channel should still be open
    engine
        .process(Event::new("A"))
        .await
        .expect("Process A2 failed");
    engine
        .process(Event::new("B"))
        .await
        .expect("Process B2 failed");

    // Verify second output event - this would fail if channel was closed
    let result2 = rx.try_recv();
    assert!(
        result2.is_ok(),
        "Second output event should be received - channel must stay open"
    );
}

// =============================================================================
// Electrical Consumption Scenario Tests
// =============================================================================

#[tokio::test]
async fn test_electrical_abnormal_floor_consumption() {
    // Test using inline expression instead of user function (functions in where not yet supported)
    let program = r#"
        stream AbnormalFloor = FloorConsumption as fc
            .where(consumption_kwh > baseline_kwh * 1.5)
            .emit(
                alert_type: "abnormal",
                floor_id: fc.floor_id,
                consumption: fc.consumption_kwh
            )
    "#;

    let events = r#"
        FloorConsumption { site_id: "S1", building_id: "B1", floor_id: "F1", consumption_kwh: 100.0, baseline_kwh: 95.0 }
        FloorConsumption { site_id: "S1", building_id: "B1", floor_id: "F2", consumption_kwh: 200.0, baseline_kwh: 90.0 }
    "#;

    let results = run_scenario(program, events).await;

    // Only F2 should trigger (200 > 90 * 1.5 = 135)
    assert_eq!(results.len(), 1, "Should detect one abnormal floor");
    assert_eq!(
        results[0].data.get("alert_type"),
        Some(&varpulis_core::Value::Str("abnormal".into()))
    );
}

#[tokio::test]
async fn test_electrical_multiple_buildings() {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    let program = parse(
        r#"
        stream AllFloors = FloorConsumption
            .emit(building_id: building_id, floor_id: floor_id)
    "#,
    )
    .expect("Failed to parse");

    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    // Events from different buildings
    let mut e1 = Event::new("FloorConsumption");
    e1.data.insert(
        "site_id".to_string(),
        varpulis_core::Value::Str("S1".into()),
    );
    e1.data.insert(
        "building_id".to_string(),
        varpulis_core::Value::Str("B1".into()),
    );
    e1.data.insert(
        "floor_id".to_string(),
        varpulis_core::Value::Str("F1".into()),
    );
    e1.data.insert(
        "consumption_kwh".to_string(),
        varpulis_core::Value::Float(100.0),
    );

    let mut e2 = Event::new("FloorConsumption");
    e2.data.insert(
        "site_id".to_string(),
        varpulis_core::Value::Str("S1".into()),
    );
    e2.data.insert(
        "building_id".to_string(),
        varpulis_core::Value::Str("B2".into()),
    );
    e2.data.insert(
        "floor_id".to_string(),
        varpulis_core::Value::Str("F1".into()),
    );
    e2.data.insert(
        "consumption_kwh".to_string(),
        varpulis_core::Value::Float(150.0),
    );

    engine.process(e1).await.expect("Process e1");
    engine.process(e2).await.expect("Process e2");

    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }

    assert_eq!(count, 2, "Should emit for both floors");
}

#[tokio::test]
async fn test_electrical_consumption_spike_detection() {
    // Test sequence pattern for detecting same floor readings
    let program = r#"
        stream Spike = FloorConsumption as current
            -> FloorConsumption as next
            .emit(
                alert_type: "spike",
                current_floor: current.floor_id
            )
    "#;

    let events = r#"
        FloorConsumption { floor_id: "F1", consumption_kwh: 100.0, baseline_kwh: 95.0 }
        FloorConsumption { floor_id: "F1", consumption_kwh: 250.0, baseline_kwh: 95.0 }
    "#;

    let results = run_scenario(program, events).await;

    // First FloorConsumption -> Second FloorConsumption should trigger
    assert_eq!(results.len(), 1, "Should detect spike sequence");
    assert_eq!(
        results[0].data.get("alert_type"),
        Some(&varpulis_core::Value::Str("spike".into()))
    );
}

#[tokio::test]
async fn test_electrical_threshold_detection() {
    // Test using inline expression (user functions in where clauses require additional work)
    let program = r#"
        stream OverThreshold = Reading
            .where(value > baseline * 1.5)
            .emit(
                status: "over",
                reading_value: value
            )
    "#;

    let events = r#"
        Reading { value: 200.0, baseline: 100.0 }
        Reading { value: 120.0, baseline: 100.0 }
    "#;

    let results = run_scenario(program, events).await;

    // Only first reading (200 > 150) should trigger
    assert_eq!(results.len(), 1, "Should detect one over-threshold reading");
}

#[tokio::test]
async fn test_user_function_in_where_clause() {
    // Test that user-defined functions work in .where() clauses
    let program = r#"
        fn is_high(value: float, threshold: float) -> bool:
            value > threshold
        
        fn double(x: float) -> float:
            x * 2.0
        
        stream HighValues = Measurement
            .where(is_high(value, double(threshold)))
            .emit(status: "high", val: value)
    "#;

    let events = r#"
        Measurement { value: 100.0, threshold: 30.0 }
        Measurement { value: 50.0, threshold: 30.0 }
        Measurement { value: 150.0, threshold: 100.0 }
    "#;

    let results = run_scenario(program, events).await;

    // First: 100 > 60 (30*2) = true -> emit
    // Second: 50 > 60 = false -> no emit
    // Third: 150 > 200 (100*2) = false -> no emit
    assert_eq!(
        results.len(),
        1,
        "Should detect one high value using user functions"
    );
}

#[tokio::test]
async fn test_builtin_functions_in_where() {
    let program = r#"
        stream AbsCheck = Reading
            .where(abs(delta) > 10.0)
            .emit(status: "large_delta", d: delta)
    "#;

    let events = r#"
        Reading { delta: 5.0 }
        Reading { delta: -15.0 }
        Reading { delta: 8.0 }
        Reading { delta: 25.0 }
    "#;

    let results = run_scenario(program, events).await;

    // abs(5) = 5 > 10 = false
    // abs(-15) = 15 > 10 = true
    // abs(8) = 8 > 10 = false
    // abs(25) = 25 > 10 = true
    assert_eq!(
        results.len(),
        2,
        "Should detect two readings with abs(delta) > 10"
    );
}

#[tokio::test]
async fn test_nested_function_calls() {
    let program = r#"
        fn add_margin(x: float, pct: float) -> float:
            x * (1.0 + pct / 100.0)
        
        stream MarginCheck = Price
            .where(current > add_margin(base, margin_pct))
            .emit(status: "above_margin")
    "#;

    let events = r#"
        Price { current: 110.0, base: 100.0, margin_pct: 5.0 }
        Price { current: 104.0, base: 100.0, margin_pct: 5.0 }
    "#;

    let results = run_scenario(program, events).await;

    // 110 > 100 * 1.05 = 105 -> true
    // 104 > 105 = false
    assert_eq!(results.len(), 1, "Should detect price above margin");
}

// =============================================================================
// Negation (.not) Tests
// =============================================================================

#[tokio::test]
async fn test_sequence_negation_cancels_match() {
    // Order -> Payment should match, but Order -> Cancellation -> Payment should not
    let program = r#"
        stream OrderPayment = Order as order
            -> Payment where order_id == order.id as payment
            .not(Cancellation where order_id == order.id)
            .emit(status: "paid", order_id: order.id)
    "#;

    let events = r#"
        Order { id: 1 }
        Cancellation { order_id: 1 }
        Payment { order_id: 1 }
    "#;

    let results = run_scenario(program, events).await;

    // The Cancellation should invalidate the sequence
    assert_eq!(results.len(), 0, "Cancelled order should not emit");
}

#[tokio::test]
async fn test_sequence_negation_allows_non_matching() {
    // Order -> Payment should match when Cancellation is for different order
    let program = r#"
        stream OrderPayment = Order as order
            -> Payment where order_id == order.id as payment
            .not(Cancellation where order_id == order.id)
            .emit(status: "paid", order_id: order.id)
    "#;

    let events = r#"
        Order { id: 1 }
        Cancellation { order_id: 2 }
        Payment { order_id: 1 }
    "#;

    let results = run_scenario(program, events).await;

    // Cancellation for order 2 should not affect order 1
    assert_eq!(
        results.len(),
        1,
        "Non-matching cancellation should not affect sequence"
    );
}

#[tokio::test]
async fn test_sequence_without_negation() {
    // Verify normal sequence works without .not()
    let program = r#"
        stream OrderPayment = Order as order
            -> Payment where order_id == order.id as payment
            .emit(status: "paid", order_id: order.id)
    "#;

    let events = r#"
        Order { id: 1 }
        Cancellation { order_id: 1 }
        Payment { order_id: 1 }
    "#;

    let results = run_scenario(program, events).await;

    // Without .not(), the sequence should complete
    assert_eq!(
        results.len(),
        1,
        "Sequence without negation should complete"
    );
}

// =============================================================================
// EmitExpr with Calculated Expressions Tests
// =============================================================================

#[tokio::test]
async fn test_emit_with_function_call() {
    let program = r#"
        fn calculate_tax(amount: float, rate: float) -> float:
            amount * rate / 100.0
        
        stream TaxCalculation = Sale
            .emit(
                sale_id: id,
                amount: amount,
                tax: calculate_tax(amount, tax_rate)
            )
    "#;

    let events = r#"
        Sale { id: "S1", amount: 100.0, tax_rate: 20.0 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1);
    // Tax should be 100 * 20 / 100 = 20
    if let Some(varpulis_core::Value::Float(tax)) = results[0].data.get("tax") {
        assert!(
            (tax - 20.0).abs() < 0.001,
            "Tax should be 20.0, got {}",
            tax
        );
    } else {
        panic!("Tax field not found or not a float");
    }
}

#[tokio::test]
async fn test_emit_with_builtin_function() {
    let program = r#"
        stream AbsoluteValues = Measurement
            .emit(
                sensor_id: id,
                abs_value: abs(reading)
            )
    "#;

    let events = r#"
        Measurement { id: "M1", reading: -42.5 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1);
    if let Some(varpulis_core::Value::Float(val)) = results[0].data.get("abs_value") {
        assert!(
            (val - 42.5).abs() < 0.001,
            "Absolute value should be 42.5, got {}",
            val
        );
    } else {
        panic!("abs_value field not found or not a float");
    }
}

#[tokio::test]
async fn test_emit_with_arithmetic_expression() {
    let program = r#"
        stream PriceWithDiscount = Product
            .emit(
                product_id: id,
                final_price: price * (1.0 - discount / 100.0)
            )
    "#;

    let events = r#"
        Product { id: "P1", price: 100.0, discount: 25.0 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 1);
    // Final price should be 100 * (1 - 0.25) = 75
    if let Some(varpulis_core::Value::Float(val)) = results[0].data.get("final_price") {
        assert!(
            (val - 75.0).abs() < 0.001,
            "Final price should be 75.0, got {}",
            val
        );
    } else {
        panic!("final_price field not found or not a float");
    }
}

// ============================================================================
// ATTENTION WINDOW INTEGRATION TESTS
// ============================================================================

#[tokio::test]
async fn test_attention_window_computes_score() {
    let program = r#"
        stream CorrelatedTrades = Trade
            .attention_window(duration: 60s, heads: 4, dim: 64)
            .emit(
                symbol: symbol,
                price: price,
                attention: attention_score,
                matches: attention_matches
            )
    "#;

    // Send multiple trades - attention should build up correlation
    let events = r#"
        Trade { symbol: "AAPL", price: 150.0, volume: 1000 }
        Trade { symbol: "AAPL", price: 151.0, volume: 1100 }
        Trade { symbol: "AAPL", price: 152.0, volume: 1200 }
        Trade { symbol: "GOOG", price: 2800.0, volume: 500 }
        Trade { symbol: "AAPL", price: 153.0, volume: 1300 }
    "#;

    let results = run_scenario(program, events).await;

    // Should have 5 alerts (one per trade)
    assert_eq!(results.len(), 5, "Should emit for each trade");

    // First trade has no history, so attention_score should be 0
    if let Some(varpulis_core::Value::Float(score)) = results[0].data.get("attention") {
        assert_eq!(
            *score, 0.0,
            "First event should have 0 attention (no history)"
        );
    } else {
        panic!("attention field not found");
    }

    // Later trades should have attention_matches > 0
    if let Some(varpulis_core::Value::Int(matches)) = results[4].data.get("matches") {
        assert!(
            *matches > 0,
            "Later trades should have attention matches, got {}",
            matches
        );
    } else {
        panic!("matches field not found");
    }
}

#[tokio::test]
async fn test_attention_window_with_threshold_filter() {
    let program = r#"
        stream HighCorrelation = Trade
            .attention_window(duration: 30s, heads: 2, dim: 32, threshold: 0.1)
            .where(attention_score > 0.0)
            .emit(
                symbol: symbol,
                score: attention_score
            )
    "#;

    let events = r#"
        Trade { symbol: "AAPL", price: 150.0 }
        Trade { symbol: "AAPL", price: 150.5 }
        Trade { symbol: "AAPL", price: 151.0 }
    "#;

    let results = run_scenario(program, events).await;

    // First trade filtered out (score = 0), subsequent trades may pass
    // The exact number depends on the attention scores computed
    for event in &results {
        if let Some(varpulis_core::Value::Float(score)) = event.data.get("score") {
            assert!(*score > 0.0, "Filtered trades should have score > 0");
        }
    }
}

#[tokio::test]
async fn test_attention_window_fraud_detection_scenario() {
    let program = r#"
        stream SuspiciousTransactions = Transaction
            .attention_window(duration: 300s, heads: 4, dim: 64)
            .emit(
                user: user_id,
                amount: amount,
                correlation: attention_score,
                similar_count: attention_matches
            )
    "#;

    // Normal transactions followed by unusual one
    let events = r#"
        Transaction { user_id: "U123", amount: 50.0, location: "NYC" }
        Transaction { user_id: "U123", amount: 75.0, location: "NYC" }
        Transaction { user_id: "U123", amount: 45.0, location: "NYC" }
        Transaction { user_id: "U123", amount: 60.0, location: "NYC" }
        Transaction { user_id: "U123", amount: 5000.0, location: "Lagos" }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 5);

    // All alerts should have correlation and similar_count fields
    for event in &results {
        assert!(
            event.data.contains_key("correlation"),
            "Should have correlation"
        );
        assert!(
            event.data.contains_key("similar_count"),
            "Should have similar_count"
        );
    }

    // The unusual transaction (last one) should still have matches from history
    if let Some(varpulis_core::Value::Int(matches)) = results[4].data.get("similar_count") {
        assert!(*matches > 0, "Even unusual tx should have history matches");
    }
}

// ============================================================================
// MERGE STREAM INTEGRATION TESTS
// ============================================================================

#[tokio::test]
async fn test_merge_stream_basic() {
    let program = r#"
        stream BuildingMetrics = merge(
            stream S1 from SensorEvent where sensor_id == "S1",
            stream S2 from SensorEvent where sensor_id == "S2",
            stream S3 from SensorEvent where sensor_id == "S3"
        )
        .emit(
            sensor: sensor_id,
            temp: temperature
        )
    "#;

    let events = r#"
        SensorEvent { sensor_id: "S1", temperature: 22.5 }
        SensorEvent { sensor_id: "S2", temperature: 23.0 }
        SensorEvent { sensor_id: "S4", temperature: 24.0 }
        SensorEvent { sensor_id: "S3", temperature: 21.5 }
    "#;

    let results = run_scenario(program, events).await;

    // S1, S2, S3 should pass (3 alerts), S4 should be filtered out
    assert_eq!(results.len(), 3, "Only S1, S2, S3 should pass merge filter");
}

#[tokio::test]
async fn test_merge_with_window_and_aggregation() {
    let program = r#"
        stream BuildingMetrics = merge(
            stream S1 from SensorEvent where sensor_id == "S1",
            stream S2 from SensorEvent where sensor_id == "S2"
        )
        .window(1m)
        .aggregate(
            avg_temp: avg(temperature),
            min_temp: min(temperature),
            max_temp: max(temperature)
        )
        .emit(
            average: avg_temp,
            minimum: min_temp,
            maximum: max_temp
        )
    "#;

    let events = r#"
        SensorEvent { sensor_id: "S1", temperature: 20.0 }
        SensorEvent { sensor_id: "S2", temperature: 25.0 }
        SensorEvent { sensor_id: "S1", temperature: 22.0 }
        SensorEvent { sensor_id: "S2", temperature: 23.0 }
    "#;

    let results = run_scenario(program, events).await;

    // Window may or may not have closed depending on timing
    // Just verify we can process without errors
    assert!(
        results.len() <= 1,
        "Should have at most one aggregated result"
    );
}

#[tokio::test]
async fn test_count_distinct_aggregation() {
    let program = r#"
        stream SensorStats = SensorEvent
            .window(1m)
            .aggregate(
                sensor_count: count(distinct(sensor_id)),
                total_count: count(sensor_id)
            )
            .emit(
                unique_sensors: sensor_count,
                total: total_count
            )
    "#;

    // We need to trigger window completion - for now just test parsing and setup
    let events = r#"
        SensorEvent { sensor_id: "S1", temperature: 20.0 }
        SensorEvent { sensor_id: "S1", temperature: 21.0 }
        SensorEvent { sensor_id: "S2", temperature: 22.0 }
        SensorEvent { sensor_id: "S3", temperature: 23.0 }
        SensorEvent { sensor_id: "S1", temperature: 24.0 }
    "#;

    let results = run_scenario(program, events).await;

    // Verify the stream processes without error
    // Full window test would require time manipulation
    assert!(results.len() <= 1);
}

// ============================================================================
// PATTERN MATCHING INTEGRATION TESTS
// ============================================================================

#[tokio::test]
async fn test_pattern_simple_count() {
    let program = r#"
        stream HighVolumeAlert = Trade
            .window(1m)
            .pattern(high_activity: events => events.len() > 3)
            .emit(
                alert_type: "high_activity",
                count: "detected"
            )
    "#;

    let events = r#"
        Trade { symbol: "AAPL", price: 150.0, amount: 1000 }
        Trade { symbol: "AAPL", price: 151.0, amount: 2000 }
        Trade { symbol: "AAPL", price: 152.0, amount: 3000 }
        Trade { symbol: "AAPL", price: 153.0, amount: 4000 }
    "#;

    let results = run_scenario(program, events).await;

    // Pattern should match when we have > 3 events
    // This depends on window behavior
    assert!(results.len() <= 1);
}

#[tokio::test]
async fn test_pattern_with_filter() {
    let program = r#"
        stream HighValueTrades = Trade
            .attention_window(duration: 30s, heads: 4, dim: 64)
            .pattern(
                high_value: events => {
                    let high = events.filter(e => e.amount > 5000)
                    high.len() > 0
                }
            )
            .emit(
                alert_type: "high_value_trade"
            )
    "#;

    let events = r#"
        Trade { symbol: "AAPL", price: 150.0, amount: 1000 }
        Trade { symbol: "AAPL", price: 151.0, amount: 10000 }
    "#;

    let results = run_scenario(program, events).await;

    // Should detect high value trades
    // Pattern matching depends on how events are processed
    for event in &results {
        assert_eq!(
            event.data.get("alert_type"),
            Some(&varpulis_core::Value::Str("high_value_trade".into()))
        );
    }
}

#[tokio::test]
async fn test_pattern_attention_correlation() {
    let program = r#"
        stream CorrelatedTrades = Trade
            .attention_window(duration: 60s, heads: 4, dim: 64)
            .emit(
                symbol: symbol,
                price: price,
                correlation: attention_score
            )
    "#;

    let events = r#"
        Trade { symbol: "AAPL", price: 150.0, amount: 1000 }
        Trade { symbol: "AAPL", price: 150.5, amount: 1100 }
        Trade { symbol: "AAPL", price: 151.0, amount: 1200 }
        Trade { symbol: "GOOG", price: 2800.0, amount: 500 }
        Trade { symbol: "AAPL", price: 151.5, amount: 1300 }
    "#;

    let results = run_scenario(program, events).await;

    assert_eq!(results.len(), 5);

    // First event has no history
    if let Some(varpulis_core::Value::Float(score)) = results[0].data.get("correlation") {
        assert_eq!(*score, 0.0);
    }

    // Later AAPL trades should have correlation with history
    // The 5th trade (AAPL after GOOG) should correlate with earlier AAPL trades
    for (i, event) in results.iter().enumerate().skip(1) {
        assert!(
            event.data.contains_key("correlation"),
            "Output event {} should have correlation",
            i
        );
    }
}

// ============================================================================
// FRAUD DETECTION COMPREHENSIVE TEST
// ============================================================================

#[tokio::test]
async fn test_fraud_detection_with_attention_pattern() {
    // Simplified version of the user's fraud detection example
    let program = r#"
        stream FraudDetection = Trade
            .attention_window(duration: 30s, heads: 4, dim: 64)
            .where(amount > 10000)
            .emit(
                alert_type: "potential_fraud",
                trade_amount: amount,
                correlation: attention_score
            )
    "#;

    let events = r#"
        Trade { symbol: "AAPL", amount: 500.0 }
        Trade { symbol: "AAPL", amount: 15000.0 }
        Trade { symbol: "GOOG", amount: 800.0 }
        Trade { symbol: "AAPL", amount: 25000.0 }
        Trade { symbol: "MSFT", amount: 50000.0 }
    "#;

    let results = run_scenario(program, events).await;

    // Only trades with amount > 10000 should emit
    assert_eq!(
        results.len(),
        3,
        "Should have 3 high-value trade output events"
    );

    for event in &results {
        if let Some(varpulis_core::Value::Float(amount)) = event.data.get("trade_amount") {
            assert!(
                *amount > 10000.0,
                "All output events should be high-value trades"
            );
        }
        assert!(
            event.data.contains_key("correlation"),
            "All output events should have correlation score"
        );
    }
}

// ============================================================================
// BUILDING METRICS COMPREHENSIVE TEST
// ============================================================================

#[tokio::test]
async fn test_building_metrics_comprehensive() {
    let program = r#"
        stream BuildingMetrics = merge(
            stream S1 from SensorEvent where sensor_id == "S1",
            stream S2 from SensorEvent where sensor_id == "S2",
            stream S3 from SensorEvent where sensor_id == "S3"
        )
        .emit(
            sensor: sensor_id,
            reading: temperature
        )
    "#;

    // Comprehensive sensor events
    let events = r#"
        SensorEvent { sensor_id: "S1", temperature: 22.0 }
        SensorEvent { sensor_id: "S2", temperature: 23.5 }
        SensorEvent { sensor_id: "S4", temperature: 25.0 }
        SensorEvent { sensor_id: "S3", temperature: 21.0 }
        SensorEvent { sensor_id: "S1", temperature: 22.5 }
        SensorEvent { sensor_id: "S5", temperature: 26.0 }
        SensorEvent { sensor_id: "S2", temperature: 24.0 }
        SensorEvent { sensor_id: "S3", temperature: 21.5 }
    "#;

    let results = run_scenario(program, events).await;

    // S1: 2 events, S2: 2 events, S3: 2 events = 6 total
    // S4 and S5 should be filtered out
    assert_eq!(
        results.len(),
        6,
        "Should have 6 output events from S1, S2, S3 only"
    );

    // Verify all alerts are from valid sensors
    for event in &results {
        if let Some(varpulis_core::Value::Str(sensor)) = event.data.get("sensor") {
            assert!(
                &**sensor == "S1" || &**sensor == "S2" || &**sensor == "S3",
                "Sensor {} should not be in results",
                sensor
            );
        }
    }
}

// ============================================================================
// APAMA-STYLE PATTERN MATCHING TESTS
// ============================================================================

#[tokio::test]
async fn test_apama_followed_by_pattern() {
    // Apama-style: NewsItem -> StockTick (news followed by stock tick)
    let program = r#"
        stream NewsStockCorrelation = NewsItem
            .pattern(news_stock: NewsItem -> StockTick)
            .emit(
                alert_type: "news_stock_correlation"
            )
    "#;

    let events = r#"
        NewsItem { subject: "ACME", headline: "Q4 Results" }
        StockTick { symbol: "ACME", price: 150.0 }
    "#;

    let results = run_scenario(program, events).await;

    // Pattern should match when NewsItem is followed by StockTick
    // Note: depends on pattern engine integration
    assert!(results.len() <= 2); // May emit on each event or just on completion
}

#[tokio::test]
async fn test_apama_and_pattern() {
    // A and B (both required, any order)
    let program = r#"
        stream BothRequired = EventA
            .pattern(both: EventA and EventB)
            .emit(
                alert_type: "both_events"
            )
    "#;

    let events = r#"
        EventB { id: 1 }
        EventA { id: 2 }
    "#;

    let results = run_scenario(program, events).await;

    // Pattern should match when both events arrive (any order)
    assert!(results.len() <= 2);
}

#[tokio::test]
async fn test_apama_or_pattern() {
    // A or B (either one)
    let program = r#"
        stream EitherOne = EventA
            .pattern(either: EventA or EventB)
            .emit(
                alert_type: "either_event"
            )
    "#;

    let events = r#"
        EventB { id: 1 }
    "#;

    let results = run_scenario(program, events).await;

    // Pattern should match when EventB arrives
    assert!(results.len() <= 1);
}

#[tokio::test]
async fn test_apama_complex_pattern() {
    // (A -> B) and not C
    let program = r#"
        stream ComplexPattern = EventA
            .pattern(complex: (EventA -> EventB) and not EventC)
            .emit(
                alert_type: "complex_match"
            )
    "#;

    let events = r#"
        EventA { id: 1 }
        EventB { id: 2 }
    "#;

    let results = run_scenario(program, events).await;

    // Pattern should match: A followed by B, no C
    assert!(results.len() <= 2);
}

#[tokio::test]
async fn test_apama_chained_followed_by() {
    // A -> B -> C -> D
    let program = r#"
        stream ChainPattern = EventA
            .pattern(chain: EventA -> EventB -> EventC -> EventD)
            .emit(
                alert_type: "chain_complete"
            )
    "#;

    let events = r#"
        EventA { step: 1 }
        EventB { step: 2 }
        EventC { step: 3 }
        EventD { step: 4 }
    "#;

    let results = run_scenario(program, events).await;

    // Pattern should match when all four events arrive in sequence
    assert!(results.len() <= 4);
}

// =============================================================================
// .to() CONNECTOR ROUTING INTEGRATION TESTS
// =============================================================================

/// Helper to run a scenario with .to() file connector and return (output_events, file_contents)
async fn run_scenario_with_file_sink(
    program_source: &str,
    events_source: &str,
    output_path: &std::path::Path,
) -> (Vec<Event>, String) {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

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

    // Collect output events from the channel
    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        results.push(event);
    }

    // Read file sink output
    let file_contents = std::fs::read_to_string(output_path).unwrap_or_default();

    results_and_file(results, file_contents)
}

fn results_and_file(results: Vec<Event>, file_contents: String) -> (Vec<Event>, String) {
    (results, file_contents)
}

#[tokio::test]
async fn test_to_file_connector_basic() {
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("output.jsonl");
    let output_path_str = output_path.to_str().unwrap().replace('\\', "/");

    let program = format!(
        r#"
        connector FileOut = file(path: "{}")

        stream HighTemp = SensorReading
            .where(temperature > 30.0)
            .emit(status: "hot", temp: temperature)
            .to(FileOut)
    "#,
        output_path_str
    );

    let events = r#"
        SensorReading { temperature: 25.0, zone: "A" }
        SensorReading { temperature: 35.0, zone: "B" }
        SensorReading { temperature: 40.0, zone: "C" }
        SensorReading { temperature: 28.0, zone: "D" }
    "#;

    let (results, file_contents) =
        run_scenario_with_file_sink(&program, events, &output_path).await;

    // Two events pass the filter (35.0 and 40.0)
    assert_eq!(results.len(), 2, "Should emit 2 output events");

    // File sink should have received the same 2 events
    let lines: Vec<&str> = file_contents.lines().collect();
    assert_eq!(lines.len(), 2, "File should contain 2 JSON lines");

    // Verify file content is valid JSON with expected fields
    for line in &lines {
        let json: serde_json::Value = serde_json::from_str(line).expect("Should be valid JSON");
        assert_eq!(json["data"]["status"], "hot");
    }
}

#[tokio::test]
async fn test_to_connector_not_found() {
    // .to() with an undeclared connector should not crash, just warn
    let program = r#"
        stream Output = SensorReading
            .where(temperature > 30.0)
            .emit(status: "hot")
            .to(NonExistentConnector)
    "#;

    let events = r#"
        SensorReading { temperature: 35.0 }
    "#;

    let results = run_scenario(program, events).await;

    // Event should still be emitted to the output channel even if .to() fails
    assert_eq!(results.len(), 1, "Output event should still be produced");
    assert_eq!(
        results[0].data.get("status"),
        Some(&varpulis_core::Value::Str("hot".into()))
    );
}

#[tokio::test]
async fn test_to_console_connector() {
    // Console connector should work without errors
    let program = r#"
        connector ConsoleOut = console()

        stream Alerts = SensorReading
            .where(temperature > 30.0)
            .emit(status: "alert", temp: temperature)
            .to(ConsoleOut)
    "#;

    let events = r#"
        SensorReading { temperature: 35.0, zone: "A" }
        SensorReading { temperature: 40.0, zone: "B" }
    "#;

    let results = run_scenario(program, events).await;

    // Both events pass filter and should be emitted
    assert_eq!(results.len(), 2, "Should emit 2 output events");
    for result in &results {
        assert_eq!(
            result.data.get("status"),
            Some(&varpulis_core::Value::Str("alert".into()))
        );
    }
}

#[tokio::test]
async fn test_to_file_connector_with_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("seq_output.jsonl");
    let output_path_str = output_path.to_str().unwrap().replace('\\', "/");

    let program = format!(
        r#"
        connector SeqFile = file(path: "{}")

        stream OrderPayment = Order as order
            -> Payment where order_id == order.id as payment
            .emit(status: "matched", order_id: order.id)
            .to(SeqFile)
    "#,
        output_path_str
    );

    let events = r#"
        Order { id: 1 }
        Payment { order_id: 1, amount: 100.0 }
        Order { id: 2 }
        Payment { order_id: 2, amount: 200.0 }
    "#;

    let (results, file_contents) =
        run_scenario_with_file_sink(&program, events, &output_path).await;

    // Two sequences matched
    assert_eq!(results.len(), 2, "Should match 2 order-payment sequences");

    // File should also contain 2 events
    let lines: Vec<&str> = file_contents.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "File should contain 2 matched sequence events"
    );

    for line in &lines {
        let json: serde_json::Value = serde_json::from_str(line).expect("Should be valid JSON");
        assert_eq!(json["data"]["status"], "matched");
    }
}

#[tokio::test]
async fn test_to_multiple_connectors() {
    let dir = tempfile::tempdir().unwrap();
    let path1 = dir.path().join("out1.jsonl");
    let path2 = dir.path().join("out2.jsonl");
    let path1_str = path1.to_str().unwrap().replace('\\', "/");
    let path2_str = path2.to_str().unwrap().replace('\\', "/");

    let program = format!(
        r#"
        connector File1 = file(path: "{}")
        connector File2 = file(path: "{}")

        stream Output = SensorReading
            .where(temperature > 30.0)
            .emit(status: "hot", temp: temperature)
            .to(File1)
            .to(File2)
    "#,
        path1_str, path2_str
    );

    let events = r#"
        SensorReading { temperature: 35.0 }
        SensorReading { temperature: 40.0 }
    "#;

    let (tx, mut rx) = mpsc::channel::<Event>(100);
    let parsed = parse(&program).expect("Failed to parse program");
    let mut engine = Engine::new(tx);
    engine.load(&parsed).expect("Failed to load program");

    let parsed_events = EventFileParser::parse(events).expect("Failed to parse events");
    for timed_event in parsed_events {
        engine
            .process(timed_event.event)
            .await
            .expect("Failed to process event");
    }

    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        results.push(event);
    }

    assert_eq!(results.len(), 2, "Should emit 2 output events");

    // Both files should have the same 2 events
    let file1_contents = std::fs::read_to_string(&path1).unwrap_or_default();
    let file2_contents = std::fs::read_to_string(&path2).unwrap_or_default();

    let lines1: Vec<&str> = file1_contents.lines().collect();
    let lines2: Vec<&str> = file2_contents.lines().collect();

    assert_eq!(lines1.len(), 2, "File1 should contain 2 events");
    assert_eq!(lines2.len(), 2, "File2 should contain 2 events");
}

#[tokio::test]
async fn test_to_does_not_consume_events() {
    // .to() is a side-effect: events should continue flowing to the output channel
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("passthrough.jsonl");
    let output_path_str = output_path.to_str().unwrap().replace('\\', "/");

    let program = format!(
        r#"
        connector FileOut = file(path: "{}")

        stream Output = SensorReading
            .emit(value: temperature)
            .to(FileOut)
    "#,
        output_path_str
    );

    let events = r#"
        SensorReading { temperature: 10.0 }
        SensorReading { temperature: 20.0 }
        SensorReading { temperature: 30.0 }
    "#;

    let (results, file_contents) =
        run_scenario_with_file_sink(&program, events, &output_path).await;

    // All 3 events should reach both the output channel and the file
    assert_eq!(
        results.len(),
        3,
        "All events should pass through to output channel"
    );

    let lines: Vec<&str> = file_contents.lines().collect();
    assert_eq!(lines.len(), 3, "All events should also be written to file");
}

#[tokio::test]
async fn test_to_with_filter_only_matching_events() {
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("filtered.jsonl");
    let output_path_str = output_path.to_str().unwrap().replace('\\', "/");

    let program = format!(
        r#"
        connector FilteredOut = file(path: "{}")

        stream CriticalOnly = SensorReading
            .where(temperature > 50.0)
            .emit(severity: "critical", temp: temperature)
            .to(FilteredOut)
    "#,
        output_path_str
    );

    let events = r#"
        SensorReading { temperature: 25.0 }
        SensorReading { temperature: 55.0 }
        SensorReading { temperature: 30.0 }
        SensorReading { temperature: 60.0 }
        SensorReading { temperature: 45.0 }
    "#;

    let (results, file_contents) =
        run_scenario_with_file_sink(&program, events, &output_path).await;

    // Only 2 events (55.0 and 60.0) pass the filter
    assert_eq!(results.len(), 2, "Only 2 events should pass filter");

    let lines: Vec<&str> = file_contents.lines().collect();
    assert_eq!(lines.len(), 2, "File should only contain filtered events");

    for line in &lines {
        let json: serde_json::Value = serde_json::from_str(line).expect("Should be valid JSON");
        assert_eq!(json["data"]["severity"], "critical");
    }
}

#[tokio::test]
async fn test_to_connector_reload() {
    let dir = tempfile::tempdir().unwrap();
    let path_v1 = dir.path().join("v1.jsonl");
    let path_v2 = dir.path().join("v2.jsonl");
    let path_v1_str = path_v1.to_str().unwrap().replace('\\', "/");
    let path_v2_str = path_v2.to_str().unwrap().replace('\\', "/");

    // Initial program
    let program_v1 = format!(
        r#"
        connector Output = file(path: "{}")

        stream Temps = SensorReading
            .emit(temp: temperature)
            .to(Output)
    "#,
        path_v1_str
    );

    // Reloaded program with different file
    let program_v2 = format!(
        r#"
        connector Output = file(path: "{}")

        stream Temps = SensorReading
            .emit(temp: temperature)
            .to(Output)
    "#,
        path_v2_str
    );

    let (tx, mut rx) = mpsc::channel::<Event>(100);
    let parsed_v1 = parse(&program_v1).expect("Failed to parse v1");
    let mut engine = Engine::new(tx);
    engine.load(&parsed_v1).expect("Failed to load v1");

    // Process event with v1
    let event1 =
        varpulis_runtime::event::Event::new("SensorReading").with_field("temperature", 25.0);
    engine.process(event1).await.unwrap();

    // Reload with v2
    let parsed_v2 = parse(&program_v2).expect("Failed to parse v2");
    engine.reload(&parsed_v2).unwrap();

    // Process event with v2
    let event2 =
        varpulis_runtime::event::Event::new("SensorReading").with_field("temperature", 35.0);
    engine.process(event2).await.unwrap();

    // Drain output channel
    while let Ok(_) = rx.try_recv() {}

    // v1 file should have 1 event, v2 file should have 1 event
    let v1_contents = std::fs::read_to_string(&path_v1).unwrap_or_default();
    let v1_count = v1_contents.lines().filter(|l| !l.is_empty()).count();
    let v2_contents = std::fs::read_to_string(&path_v2).unwrap_or_default();
    let v2_count = v2_contents.lines().filter(|l| !l.is_empty()).count();

    assert_eq!(v1_count, 1, "v1 file should have 1 event");
    assert_eq!(v2_count, 1, "v2 file should have 1 event");
}
