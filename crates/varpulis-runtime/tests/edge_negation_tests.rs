//! Edge-case tests for negation (`.not()`) at VPL integration level.
//!
//! Extends the 3 existing `.not()` tests in integration_scenarios.rs with
//! partition-awareness, multi-cancel, combined patterns, and position variants.

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
// Negation Edge Cases
// =============================================================================

#[tokio::test]
async fn negation_wrong_partition_key() {
    // .not(C where user_id == a.user_id): C with different user_id does NOT cancel
    let program = r#"
        stream NegPartition = Login as a
            -> Purchase where user_id == a.user_id as b
            .not(Cancellation where user_id == a.user_id)
            .emit(status: "purchased", user: a.user_id)
    "#;

    let events = r#"
        Login { user_id: "alice" }
        Cancellation { user_id: "bob" }
        Purchase { user_id: "alice", amount: 50.0 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        1,
        "Cancellation for bob should NOT cancel alice's sequence"
    );
}

#[tokio::test]
async fn negation_multiple_cancel_events() {
    // Two C events — first is sufficient to cancel
    let program = r#"
        stream NegMultiple = Order as a
            -> Payment where order_id == a.id as b
            .not(Cancel where order_id == a.id)
            .emit(status: "paid")
    "#;

    let events = r#"
        Order { id: 1 }
        Cancel { order_id: 1 }
        Cancel { order_id: 1 }
        Payment { order_id: 1 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        0,
        "First Cancel should be sufficient to block the match"
    );
}

#[tokio::test]
async fn negation_after_match_no_retraction() {
    // Match already emitted, then C arrives — no retraction
    let program = r#"
        stream NegAfterMatch = Order as a
            -> Payment where order_id == a.id as b
            .not(Cancel where order_id == a.id)
            .emit(status: "paid")
    "#;

    let events = r#"
        Order { id: 1 }
        Payment { order_id: 1 }
        Cancel { order_id: 1 }
    "#;

    let results = run_scenario(program, events).await;
    // Match should have been emitted before Cancel arrives
    assert_eq!(
        results.len(),
        1,
        "Cancel after match completion should not retract the match"
    );
}

#[tokio::test]
async fn negation_preserves_other_partitions() {
    // Partition A cancelled, partition B still matches
    let program = r#"
        stream NegPartitions = Request as a
            -> Response where req_id == a.id as b
            .not(Timeout where req_id == a.id)
            .emit(status: "responded", req: a.id)
    "#;

    let events = r#"
        Request { id: 1 }
        Request { id: 2 }
        Timeout { req_id: 1 }
        Response { req_id: 2 }
        Response { req_id: 1 }
    "#;

    let results = run_scenario(program, events).await;
    // Request 1 is timed out (negated), Request 2 should still match
    assert!(
        !results.is_empty(),
        "Request 2 should still match despite Request 1 being negated"
    );
    // Verify the surviving match is for request 2
    if let Some(result) = results.first() {
        if let Some(varpulis_core::Value::Int(req_id)) = result.data.get("req") {
            assert_eq!(*req_id, 2, "Surviving match should be request 2");
        }
    }
}

#[tokio::test]
async fn negation_at_sequence_end() {
    // .not() after last step vs between steps — positioned after the final correlated step
    let program = r#"
        stream NegEnd = A as a
            -> B as b
            .not(Poison)
            .emit(status: "clean")
    "#;

    // Poison arrives between A and B
    let events = r#"
        A { id: 1 }
        Poison { toxin: "x" }
        B { id: 2 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        0,
        "Poison between A and B should cancel the match"
    );
}

#[tokio::test]
async fn negation_no_cancel_event_allows_match() {
    // When no cancellation event arrives, the match should proceed normally
    let program = r#"
        stream NegClean = Start as a
            -> End where session_id == a.id as b
            .not(Error where session_id == a.id)
            .emit(status: "success", session: a.id)
    "#;

    let events = r#"
        Start { id: 42 }
        End { session_id: 42 }
    "#;

    let results = run_scenario(program, events).await;
    assert_eq!(
        results.len(),
        1,
        "Without any Error event, sequence should match normally"
    );
}
