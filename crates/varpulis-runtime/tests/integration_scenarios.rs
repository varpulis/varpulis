//! Integration tests for event simulation scenarios
//!
//! Tests validate complete workflows: parse program + inject events + verify alerts

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::{Alert, Engine};
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
