//! Array methods that take a lambda, in the expressions a stream evaluates.
//!
//! `arr.filter(x => cond)` and `arr.map(x => expr)` are documented, and any
//! program using one aborted the process with a stack overflow: the lambda,
//! evaluated as an ordinary argument, fell to the evaluator's fallback arm,
//! which called the evaluator again with the same expression. An abort is not
//! a panic, so a host (Vejas) went down with every unit it was running.

use varpulis_engine::Program;

fn emit_of(expr: &str) -> serde_json::Value {
    let src = format!("stream R = T\n    .emit(v: {expr})\n");
    let mut program = Program::compile(&src).unwrap();
    let emits = program
        .feed_json(
            "T",
            br#"{"type": "T", "@timestamp": "2026-09-22T10:00:00Z", "n": 2}"#,
        )
        .unwrap();
    emits[0].to_json()["v"].clone()
}

#[test]
fn filter_and_map_take_their_lambda() {
    assert_eq!(emit_of("len([1, 2, 3].filter(x => x > 1))"), 2);
    assert_eq!(emit_of("[1, 2].map(x => x * 10).first()"), 10);
    // The lambda sees the event's fields too.
    assert_eq!(emit_of("len([1, 2, 3].filter(x => x > n))"), 1);
}

/// The same fallback took `a?.b` and timestamp literals down with it.
#[test]
fn optional_member_access_and_timestamp_literals_evaluate() {
    let src = "stream R = T\n    .where(n > 1)\n    .emit(a: shipping?.country, t: @2026-09-22T10:00:00Z, gone: missing?.field)\n";
    let mut program = Program::compile(src).unwrap();
    let emits = program
        .feed_json(
            "T",
            br#"{"type": "T", "@timestamp": "2026-09-22T10:00:00Z", "n": 2, "shipping": {"country": "FR"}}"#,
        )
        .unwrap();
    let json = emits[0].to_json();
    assert_eq!(json["a"], "FR", "{json}");
    assert!(json.get("t").is_some(), "{json}");
    assert!(json.get("gone").is_none(), "{json}");
}
