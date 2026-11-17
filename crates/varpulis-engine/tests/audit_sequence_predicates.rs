//! A step's `where` is a condition, whatever operator it uses.
//!
//! The sequence compiler turned a comparison into a native predicate and
//! anything else it did not list — `in`, `not in`, a modulo, a condition
//! wrapped in `not` — into *no predicate at all*: the step then took every
//! event of its type. A detection written `-> Process where image in [...]`
//! checked "ok" and matched every process.

mod common;

use common::{at, feed, only};
use varpulis_engine::Program;

const EVENTS: &str = r"
event A:
    k: str
event B:
    k: str
    status: str
    ip: str
";

fn a(k: &str) -> String {
    at("10:00:00", &format!(r#""k": "{k}""#))
}

fn b(second: u32, k: &str, status: &str, ip: &str) -> String {
    at(
        &format!("10:00:{second:02}"),
        &format!(r#""k": "{k}", "status": "{status}", "ip": "{ip}""#),
    )
}

/// Feed A then each B, returning the `status` of every emit.
fn statuses(program: &str, bs: &[(&str, &str)]) -> Vec<String> {
    let mut p = Program::compile(program).unwrap_or_else(|e| panic!("{e}"));
    assert!(feed(&mut p, "A", &a("x")).is_empty());
    let mut out = Vec::new();
    for (i, (status, ip)) in bs.iter().enumerate() {
        for emit in feed(&mut p, "B", &b(i as u32 + 1, "x", status, ip)) {
            out.push(emit["status"].as_str().unwrap_or("<none>").to_string());
        }
    }
    out
}

#[test]
fn a_membership_test_in_a_named_pattern_step_is_a_condition() {
    let program = format!(
        r#"{EVENTS}
pattern Allowed = A as a -> B where status in ["ok", "done"] as b within 1h
stream S = Allowed
    .emit(k: a.k, status: b.status)
"#
    );
    assert_eq!(
        statuses(&program, &[("fail", "1"), ("done", "1")]),
        vec!["done"],
        "a status outside the list must not complete the step"
    );
}

#[test]
fn a_membership_test_in_an_inline_step_is_a_condition() {
    let program = format!(
        r#"{EVENTS}
stream S = A as a
    -> B where status in ["ok", "done"] as b
    .within(1h)
    .emit(k: a.k, status: b.status)
"#
    );
    assert_eq!(
        statuses(&program, &[("fail", "1"), ("ok", "1")]),
        vec!["ok"]
    );
}

#[test]
fn a_membership_test_against_a_constant_list_is_a_condition() {
    let program = format!(
        r#"{EVENTS}
let allowed = ["ok", "done"]
stream S = A as a
    -> B where status in allowed as b
    .within(1h)
    .emit(k: a.k, status: b.status)
"#
    );
    assert_eq!(
        statuses(&program, &[("fail", "1"), ("done", "1")]),
        vec!["done"]
    );
}

#[test]
fn a_negated_membership_test_in_a_step_is_a_condition() {
    let program = format!(
        r#"{EVENTS}
stream S = A as a
    -> B where status not in ["cancelled"] as b
    .within(1h)
    .emit(k: a.k, status: b.status)
"#
    );
    assert_eq!(
        statuses(&program, &[("cancelled", "1"), ("shipped", "1")]),
        vec!["shipped"]
    );
}

#[test]
fn a_membership_test_inside_an_and_is_a_condition() {
    // `ip in [...]` used to fail to compile as a predicate, and `and` then
    // took the whole condition down with it: the step accepted every B.
    let program = format!(
        r#"{EVENTS}
stream S = A as a
    -> B where status == "fail" and ip in ["1.1.1.1"] as b
    .within(1h)
    .emit(k: a.k, status: b.status)
"#
    );
    let mut p = Program::compile(&program).unwrap();
    assert!(feed(&mut p, "A", &a("x")).is_empty());
    assert!(
        feed(&mut p, "B", &b(1, "x", "fail", "9.9.9.9")).is_empty(),
        "an address outside the list must not complete the step"
    );
    assert!(
        feed(&mut p, "B", &b(2, "x", "ok", "1.1.1.1")).is_empty(),
        "a status other than fail must not complete the step"
    );
    let out = only(feed(&mut p, "B", &b(3, "x", "fail", "1.1.1.1")));
    assert_eq!(out["status"], "fail");
}

#[test]
fn a_not_wrapping_an_unsupported_operator_is_a_condition() {
    let program = format!(
        r#"{EVENTS}
pattern P = A as a -> B where not (status in ["ok"]) as b within 1h
stream S = P
    .emit(status: b.status)
"#
    );
    assert_eq!(
        statuses(&program, &[("ok", "1"), ("fail", "1")]),
        vec!["fail"]
    );
}

#[test]
fn a_modulo_in_a_step_predicate_is_a_condition() {
    let program = r"
event A:
    k: str
event B:
    k: str
    n: int
stream S = A as a
    -> B where n % 2 == 0 as b
    .within(1h)
    .emit(n: b.n)
";
    let mut p = Program::compile(program).unwrap();
    assert!(feed(&mut p, "A", &a("x")).is_empty());
    assert!(feed(&mut p, "B", &at("10:00:01", r#""k": "x", "n": 3"#)).is_empty());
    let out = only(feed(&mut p, "B", &at("10:00:02", r#""k": "x", "n": 4"#)));
    assert_eq!(out["n"], 4);
}

#[test]
fn a_string_order_comparison_means_the_same_in_where_and_in_a_step() {
    let program = r#"
event A:
    name: str
event B:
    name: str
stream W = A
    .where(name < "m")
    .emit(name: name)
stream Q = A as a -> B where name < "m" as b
    .within(1h)
    .emit(name: b.name)
"#;
    let mut p = Program::compile(program).unwrap();
    let out = feed(&mut p, "A", &at("10:00:00", r#""name": "alice""#));
    assert_eq!(out.len(), 1, "alice sorts before m: {out:?}");
    assert_eq!(out[0]["event_type"], "W");
    let out = only(feed(&mut p, "B", &at("10:00:01", r#""name": "bob""#)));
    assert_eq!(out["event_type"], "Q");
    assert!(
        feed(&mut p, "A", &at("10:00:02", r#""name": "zed""#)).is_empty(),
        "zed sorts after m"
    );
    assert!(feed(&mut p, "B", &at("10:00:03", r#""name": "zoe""#)).is_empty());
}

#[test]
fn a_self_referencing_closure_predicate_with_arithmetic_captures() {
    // `temperature > rising.temperature` compared each reading with the one
    // before; add `+ 0.5` and the predicate became an expression the engine
    // did not recognise as self-referencing, so the closure never opened.
    let program = r"
event TempReading:
    sensor_id: str
    temperature: float
stream S = TempReading as first
    -> all TempReading where temperature > rising.temperature + 0.5 as rising
    .within(1h)
    .partition_by(sensor_id)
    .longest()
    .emit(sensor: first.sensor_id, n: count(rising), last: rising.temperature)
";
    let mut p = Program::compile(program).unwrap();
    let reading = |second: u32, t: f64| {
        at(
            &format!("10:00:{second:02}"),
            &format!(r#""sensor_id": "s1", "temperature": {t:?}"#),
        )
    };
    assert!(feed(&mut p, "TempReading", &reading(0, 20.0)).is_empty());
    // 20.4 is not more than half a degree above the anchor: not captured.
    assert!(feed(&mut p, "TempReading", &reading(1, 20.4)).is_empty());
    assert!(feed(&mut p, "TempReading", &reading(2, 21.0)).is_empty());
    assert!(feed(&mut p, "TempReading", &reading(3, 21.6)).is_empty());
    // 21.9 is not half a degree above 21.6: the trend ends, one match.
    let out = feed(&mut p, "TempReading", &reading(4, 21.9));
    let matches: Vec<&serde_json::Value> = out.iter().filter(|e| e["event_type"] == "S").collect();
    assert_eq!(matches.len(), 1, "{out:?}");
    assert_eq!(
        matches[0]["n"], 2,
        "21.0 and 21.6 were captured, 20.4 was not"
    );
    assert_eq!(matches[0]["last"], 21.6);
}
