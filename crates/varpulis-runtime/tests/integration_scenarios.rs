//! Integration tests for event simulation scenarios
//!
//! Tests validate complete workflows: parse program + inject events + verify alerts
//!
//! REGRESSION: These tests ensure the alert channel remains open during event processing.

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::{Alert, Engine};
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;

/// Helper to run a scenario and collect alerts
async fn run_scenario(program_source: &str, events_source: &str) -> Vec<Alert> {
    let (tx, mut rx) = mpsc::channel::<Alert>(100);
    
    // Parse and load program
    let program = parse(program_source).expect("Failed to parse program");
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load program");
    
    // Parse events
    let events = EventFileParser::parse(events_source).expect("Failed to parse events");
    
    // Process all events
    for timed_event in events {
        engine.process(timed_event.event).await.expect("Failed to process event");
    }
    
    // Collect all alerts
    let mut alerts = Vec::new();
    while let Ok(alert) = rx.try_recv() {
        alerts.push(alert);
    }
    
    alerts
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1, "Should generate exactly one alert");
    assert_eq!(
        alerts[0].data.get("status"),
        Some(&varpulis_core::Value::Str("matched".to_string()))
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 0, "Should not match with wrong order_id");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1, "Should match only order 1");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 0, "Payment before Order should not match sequence");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1, "Should complete three-step sequence");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 0, "Incomplete sequence should not emit");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 0, "Wrong order should not match");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1, "Should correlate request-response pair");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1);
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1, "Single event should trigger alert");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1, "Should match order-payment sequence");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1, "Should match completed flow");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1, "Should match error-ack sequence");
}

// =============================================================================
// Regression Tests - Alert Channel Bug (channel closed)
// =============================================================================

/// Regression test: Ensure alert channel remains open during event processing
/// Bug: When loading via WebSocket, alert_tx receiver was dropped immediately
/// Fix: Use shared alert_tx from ServerState
#[tokio::test]
async fn test_regression_alert_channel_stays_open() {
    let (tx, mut rx) = mpsc::channel::<Alert>(100);
    
    let program = parse(r#"
        stream Test = Event as e
            .emit(status: "received", id: e.id)
    "#).expect("Failed to parse");
    
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");
    
    // Process multiple events - alerts should all be received
    for i in 1..=5 {
        let mut event = varpulis_runtime::event::Event::new("Event");
        event.data.insert("id".to_string(), varpulis_core::Value::Int(i));
        engine.process(event).await.expect("Failed to process");
    }
    
    // All 5 alerts should be received (channel not closed)
    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }
    
    assert_eq!(count, 5, "All alerts should be received - channel must stay open");
}

/// Regression test: Alerts work correctly after multiple load operations
/// This simulates reloading a program (like VSCode reload)
#[tokio::test]
async fn test_regression_alerts_after_reload() {
    let (tx, mut rx) = mpsc::channel::<Alert>(100);
    
    // First load
    let program1 = parse(r#"
        stream Test1 = Event as e
            .emit(status: "test1")
    "#).expect("Failed to parse");
    
    let mut engine = Engine::new(tx.clone());
    engine.load(&program1).expect("Failed to load");
    
    let event1 = varpulis_runtime::event::Event::new("Event");
    engine.process(event1).await.expect("Failed to process");
    
    // Second load (simulates VSCode reload with same tx)
    let program2 = parse(r#"
        stream Test2 = Event as e
            .emit(status: "test2")
    "#).expect("Failed to parse");
    
    let mut engine2 = Engine::new(tx);
    engine2.load(&program2).expect("Failed to load");
    
    let event2 = varpulis_runtime::event::Event::new("Event");
    engine2.process(event2).await.expect("Failed to process");
    
    // Both alerts should be received
    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }
    
    assert_eq!(count, 2, "Alerts from both engines should be received");
}

// =============================================================================
// Regression Tests - Event Injection
// =============================================================================

/// Regression test: Events with various field types are processed correctly
/// Bug: json_to_value conversion was missing some types
#[tokio::test]
async fn test_regression_event_field_types() {
    use varpulis_runtime::event::Event;
    use varpulis_core::Value;
    
    let (tx, mut rx) = mpsc::channel::<Alert>(100);
    
    let program = parse(r#"
        stream Test = Data as d
            .emit(received: true)
    "#).expect("Failed to parse");
    
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");
    
    // Create event with various field types (simulates JSON injection)
    let mut event = Event::new("Data");
    event.data.insert("string_field".to_string(), Value::Str("hello".to_string()));
    event.data.insert("int_field".to_string(), Value::Int(42));
    event.data.insert("float_field".to_string(), Value::Float(3.14));
    event.data.insert("bool_field".to_string(), Value::Bool(true));
    event.data.insert("null_field".to_string(), Value::Null);
    
    engine.process(event).await.expect("Failed to process event with various types");
    
    assert!(rx.try_recv().is_ok(), "Event with various field types should trigger alert");
}

/// Regression test: Rapid event injection doesn't cause channel issues
#[tokio::test]
async fn test_regression_rapid_event_injection() {
    let (tx, mut rx) = mpsc::channel::<Alert>(1000);
    
    let program = parse(r#"
        stream Counter = Tick as t
            .emit(count: t.n)
    "#).expect("Failed to parse");
    
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");
    
    // Rapid injection of 100 events
    for i in 0..100 {
        let mut event = varpulis_runtime::event::Event::new("Tick");
        event.data.insert("n".to_string(), varpulis_core::Value::Int(i));
        engine.process(event).await.expect("Rapid injection should not fail");
    }
    
    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }
    
    assert_eq!(count, 100, "All 100 rapid events should produce alerts");
}

/// Regression test: Alert channel works with sequence patterns
#[tokio::test]
async fn test_regression_sequence_alert_channel() {
    let (tx, mut rx) = mpsc::channel::<Alert>(100);
    
    let program = parse(r#"
        stream AlertTest = A as a -> B .emit(status: "matched")
    "#).expect("Failed to parse");
    
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");
    
    // Process first pair
    engine.process(Event::new("A")).await.expect("Process A1 failed");
    engine.process(Event::new("B")).await.expect("Process B1 failed");
    
    // Verify first alert
    let alert1 = rx.try_recv();
    assert!(alert1.is_ok(), "First alert should be received");
    
    // Process second pair - channel should still be open
    engine.process(Event::new("A")).await.expect("Process A2 failed");
    engine.process(Event::new("B")).await.expect("Process B2 failed");
    
    // Verify second alert - this would fail if channel was closed
    let alert2 = rx.try_recv();
    assert!(alert2.is_ok(), "Second alert should be received - channel must stay open");
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
    
    let alerts = run_scenario(program, events).await;
    
    // Only F2 should trigger (200 > 90 * 1.5 = 135)
    assert_eq!(alerts.len(), 1, "Should detect one abnormal floor");
    assert_eq!(
        alerts[0].data.get("alert_type"),
        Some(&varpulis_core::Value::Str("abnormal".to_string()))
    );
}

#[tokio::test]
async fn test_electrical_multiple_buildings() {
    let (tx, mut rx) = mpsc::channel::<Alert>(100);
    
    let program = parse(r#"
        stream AllFloors = FloorConsumption
            .emit(building_id: building_id, floor_id: floor_id)
    "#).expect("Failed to parse");
    
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");
    
    // Events from different buildings
    let mut e1 = Event::new("FloorConsumption");
    e1.data.insert("site_id".to_string(), varpulis_core::Value::Str("S1".to_string()));
    e1.data.insert("building_id".to_string(), varpulis_core::Value::Str("B1".to_string()));
    e1.data.insert("floor_id".to_string(), varpulis_core::Value::Str("F1".to_string()));
    e1.data.insert("consumption_kwh".to_string(), varpulis_core::Value::Float(100.0));
    
    let mut e2 = Event::new("FloorConsumption");
    e2.data.insert("site_id".to_string(), varpulis_core::Value::Str("S1".to_string()));
    e2.data.insert("building_id".to_string(), varpulis_core::Value::Str("B2".to_string()));
    e2.data.insert("floor_id".to_string(), varpulis_core::Value::Str("F1".to_string()));
    e2.data.insert("consumption_kwh".to_string(), varpulis_core::Value::Float(150.0));
    
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
    
    let alerts = run_scenario(program, events).await;
    
    // First FloorConsumption -> Second FloorConsumption should trigger
    assert_eq!(alerts.len(), 1, "Should detect spike sequence");
    assert_eq!(
        alerts[0].data.get("alert_type"),
        Some(&varpulis_core::Value::Str("spike".to_string()))
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
    
    let alerts = run_scenario(program, events).await;
    
    // Only first reading (200 > 150) should trigger
    assert_eq!(alerts.len(), 1, "Should detect one over-threshold reading");
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
    
    let alerts = run_scenario(program, events).await;
    
    // First: 100 > 60 (30*2) = true -> emit
    // Second: 50 > 60 = false -> no emit
    // Third: 150 > 200 (100*2) = false -> no emit
    assert_eq!(alerts.len(), 1, "Should detect one high value using user functions");
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
    
    let alerts = run_scenario(program, events).await;
    
    // abs(5) = 5 > 10 = false
    // abs(-15) = 15 > 10 = true
    // abs(8) = 8 > 10 = false
    // abs(25) = 25 > 10 = true
    assert_eq!(alerts.len(), 2, "Should detect two readings with abs(delta) > 10");
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
    
    let alerts = run_scenario(program, events).await;
    
    // 110 > 100 * 1.05 = 105 -> true
    // 104 > 105 = false
    assert_eq!(alerts.len(), 1, "Should detect price above margin");
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
    
    let alerts = run_scenario(program, events).await;
    
    // The Cancellation should invalidate the sequence
    assert_eq!(alerts.len(), 0, "Cancelled order should not emit");
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
    
    let alerts = run_scenario(program, events).await;
    
    // Cancellation for order 2 should not affect order 1
    assert_eq!(alerts.len(), 1, "Non-matching cancellation should not affect sequence");
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
    
    let alerts = run_scenario(program, events).await;
    
    // Without .not(), the sequence should complete
    assert_eq!(alerts.len(), 1, "Sequence without negation should complete");
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1);
    // Tax should be 100 * 20 / 100 = 20
    if let Some(varpulis_core::Value::Float(tax)) = alerts[0].data.get("tax") {
        assert!((tax - 20.0).abs() < 0.001, "Tax should be 20.0, got {}", tax);
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1);
    if let Some(varpulis_core::Value::Float(val)) = alerts[0].data.get("abs_value") {
        assert!((val - 42.5).abs() < 0.001, "Absolute value should be 42.5, got {}", val);
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 1);
    // Final price should be 100 * (1 - 0.25) = 75
    if let Some(varpulis_core::Value::Float(val)) = alerts[0].data.get("final_price") {
        assert!((val - 75.0).abs() < 0.001, "Final price should be 75.0, got {}", val);
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
    
    let alerts = run_scenario(program, events).await;
    
    // Should have 5 alerts (one per trade)
    assert_eq!(alerts.len(), 5, "Should emit for each trade");
    
    // First trade has no history, so attention_score should be 0
    if let Some(varpulis_core::Value::Float(score)) = alerts[0].data.get("attention") {
        assert_eq!(*score, 0.0, "First event should have 0 attention (no history)");
    } else {
        panic!("attention field not found");
    }
    
    // Later trades should have attention_matches > 0
    if let Some(varpulis_core::Value::Int(matches)) = alerts[4].data.get("matches") {
        assert!(*matches > 0, "Later trades should have attention matches, got {}", matches);
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
    
    let alerts = run_scenario(program, events).await;
    
    // First trade filtered out (score = 0), subsequent trades may pass
    // The exact number depends on the attention scores computed
    for alert in &alerts {
        if let Some(varpulis_core::Value::Float(score)) = alert.data.get("score") {
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
    
    let alerts = run_scenario(program, events).await;
    
    assert_eq!(alerts.len(), 5);
    
    // All alerts should have correlation and similar_count fields
    for alert in &alerts {
        assert!(alert.data.contains_key("correlation"), "Should have correlation");
        assert!(alert.data.contains_key("similar_count"), "Should have similar_count");
    }
    
    // The unusual transaction (last one) should still have matches from history
    if let Some(varpulis_core::Value::Int(matches)) = alerts[4].data.get("similar_count") {
        assert!(*matches > 0, "Even unusual tx should have history matches");
    }
}
