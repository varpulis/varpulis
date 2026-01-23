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
