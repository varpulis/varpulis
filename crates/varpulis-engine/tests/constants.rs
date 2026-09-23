//! A top-level `let` or `const`, read by the expressions a stream evaluates.
//!
//! `.where(x > threshold)` read a field named `threshold` from the event: the
//! program's own constant was not in scope, and `const` was not even loaded.
//! A rule with a named threshold compared against nothing, or against
//! whatever an event carried under that name.

use varpulis_engine::Program;

fn fires(src: &str, json: &str) -> bool {
    let mut program = Program::compile(src).unwrap_or_else(|e| panic!("{src}\n{e}"));
    !program.feed_json("T", json.as_bytes()).unwrap().is_empty()
}

#[test]
fn a_let_is_the_programs_value_not_the_events_field() {
    let src = "let threshold = 100\n\nstream R = T\n    .where(x > threshold)\n    .emit(x: x)\n";
    assert!(fires(src, r#"{"type": "T", "x": 150}"#), "150 > 100");
    assert!(!fires(src, r#"{"type": "T", "x": 50}"#), "50 < 100");
    // An event carrying its own `threshold` does not move the rule's.
    assert!(fires(src, r#"{"type": "T", "x": 150, "threshold": 999}"#));
}

#[test]
fn a_const_declared_below_the_stream_is_seen() {
    let src = "stream R = T\n    .where(lower(Image) == BAD)\n    .emit(i: Image)\n\nconst BAD = 'mimikatz.exe'\n";
    assert!(fires(src, r#"{"type": "T", "Image": "MIMIKATZ.EXE"}"#));
}

#[test]
fn a_constant_may_use_an_earlier_one() {
    let src = "let base = 10\nlet limit = base * 2\n\nstream R = T\n    .where(x >= limit)\n    .emit(x: x)\n";
    assert!(fires(src, r#"{"type": "T", "x": 20}"#));
    assert!(!fires(src, r#"{"type": "T", "x": 19}"#));
}

#[test]
fn a_sequence_step_reads_it_and_an_alias_of_the_same_name_wins() {
    let src = "const LIMIT = 3\nlet a = 'not the alias'\n\nstream R = T as a\n    -> T where k == a.k and n > LIMIT as b\n    .within(1m)\n    .emit(k: a.k, n: b.n)\n";
    let mut program = Program::compile(src).unwrap();
    program
        .feed_json(
            "T",
            br#"{"type": "T", "@timestamp": "2026-09-22T10:00:00Z", "k": "x", "n": 0}"#,
        )
        .unwrap();
    let emits = program
        .feed_json(
            "T",
            br#"{"type": "T", "@timestamp": "2026-09-22T10:00:01Z", "k": "x", "n": 5}"#,
        )
        .unwrap();
    assert_eq!(emits.len(), 1, "5 > 3 and the alias a is the first event");
    assert_eq!(emits[0].to_json()["k"], "x");
}

#[test]
fn a_function_or_a_lambda_parameter_of_the_same_name_is_left_alone() {
    let src = "let lower = 1\nlet v = 99\n\nstream R = T\n    .where(lower(Image) == 'x' and len([1, 2, 3].filter(v => v > lower)) == 2)\n    .emit(i: Image)\n";
    assert!(fires(src, r#"{"type": "T", "Image": "X"}"#));
}
