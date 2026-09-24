//! `.not(C)` cancels a run of a named pattern as it does an inline sequence.
//!
//! A stream reading a named pattern compiled the pattern and routed `C` to
//! it, but never gave the pattern the cancellation: `AThenB.not(C)` fired on
//! A, C, B as if `.not(C)` were not there.

use varpulis_engine::Program;

fn feed(program: &mut Program, event_type: &str, json: &str) -> Vec<serde_json::Value> {
    program
        .feed_json(event_type, json.as_bytes())
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

fn at(time: &str) -> String {
    format!(r#"{{"@timestamp": "2026-09-24T{time}Z", "id": "a1"}}"#)
}

#[test]
fn not_cancels_a_run_of_a_named_pattern() {
    for vpl in [
        // Guard: the inline sequence, which always did.
        r"
stream X = A as a
    -> B as b
    .not(C)
    .within(5m)
    .emit(first: a.id)
",
        r"
pattern AThenB =
    A as a
    -> B as b
    within 5m

stream X = AThenB
    .not(C)
    .emit(first: a.id)
",
    ] {
        let mut program = Program::compile(vpl).unwrap();
        let mut out = Vec::new();
        out.extend(feed(&mut program, "A", &at("10:00:00")));
        out.extend(feed(&mut program, "C", &at("10:01:00")));
        out.extend(feed(&mut program, "B", &at("10:02:00")));
        assert!(out.is_empty(), "C came between A and B: {out:?}\n{vpl}");

        // Without C, the pattern still fires.
        let mut program = Program::compile(vpl).unwrap();
        feed(&mut program, "A", &at("10:00:00"));
        let out = feed(&mut program, "B", &at("10:02:00"));
        assert_eq!(out.len(), 1, "{out:?}\n{vpl}");
    }
}

const UNLESS_CANCELLED: &str = r"
pattern Unacked =
    Order as o
    -> NOT Ack where id == o.id
    within 4h
    partition by id

stream Alerts = Unacked
    .not(Cancel)
    .emit(order: o.id)
";

fn order(program: &mut Program, event_type: &str, time: &str, id: &str) -> Vec<serde_json::Value> {
    feed(
        program,
        event_type,
        &format!(r#"{{"@timestamp": "2026-09-24T{time}Z", "id": "{id}"}}"#),
    )
}

#[test]
fn a_cancelled_order_raises_no_absence_and_the_others_still_do() {
    // Partitioned by id: cancelling o-1 cancels o-1, not every open order.
    let mut program = Program::compile(UNLESS_CANCELLED).unwrap();
    order(&mut program, "Order", "10:00:00", "o-1");
    order(&mut program, "Order", "10:00:00", "o-2");
    assert!(order(&mut program, "Cancel", "13:00:00", "o-1").is_empty());
    let out = order(&mut program, "Order", "14:00:01", "o-3");
    let orders: Vec<_> = out.iter().map(|e| e["order"].clone()).collect();
    assert_eq!(orders, vec![serde_json::json!("o-2")], "{out:?}");
}
