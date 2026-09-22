//! The checker's list of built-in functions is the evaluator's, both ways.
//!
//! They drifted apart once: the checker listed `to_lower`, `concat`, `now`,
//! which the engine never evaluated, and did not know `lower`, `is_null` or
//! `substring`, which it does. With E050 checking every expression a stream
//! evaluates, a name missing from the list refuses a working rule, and a name
//! listed but not evaluated checks "ok" and answers nothing at run time.

use std::collections::BTreeSet;

use varpulis_core::validate::builtins::BUILTIN_FUNCTIONS;

/// The names `eval_builtin_function` matches, read from its source.
fn evaluated() -> BTreeSet<String> {
    let src = include_str!("../src/engine/evaluator.rs");
    let start = src
        .find("fn eval_builtin_function")
        .expect("eval_builtin_function is in evaluator.rs");
    let body = &src[start..];
    let body = &body[..body.find("\n}\n").expect("the function ends")];
    let arm = regex::Regex::new(
        r#"(?m)^\s*((?:"[a-z_0-9]+"\s*\|\s*)*"[a-z_0-9]+")\s*(?:if\b[^\n]*?)?=>"#,
    )
    .unwrap();
    let name = regex::Regex::new(r#""([a-z_0-9]+)""#).unwrap();
    let names: BTreeSet<String> = arm
        .captures_iter(body)
        .flat_map(|c| {
            name.captures_iter(&c[1])
                .map(|n| n[1].to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(names.len() > 40, "the arms were not read: {names:?}");
    names
}

/// Evaluated in `eval_expr_with_functions` itself, over the events a sequence
/// captured (`count(alias)`, `distinct_count(a.f)`, `collect(a.f)`).
const OVER_CAPTURED_EVENTS: &[&str] = &["count", "distinct_count", "collect"];

#[test]
fn every_function_the_engine_evaluates_is_known_to_the_checker() {
    let listed: BTreeSet<&str> = BUILTIN_FUNCTIONS.iter().copied().collect();
    let unknown: Vec<String> = evaluated()
        .into_iter()
        .filter(|n| !listed.contains(n.as_str()))
        .collect();
    assert!(
        unknown.is_empty(),
        "evaluated, but E050 would refuse them: {unknown:?}"
    );
}

#[test]
fn every_function_the_checker_accepts_is_evaluated() {
    let evaluated = evaluated();
    let phantom: Vec<&&str> = BUILTIN_FUNCTIONS
        .iter()
        .filter(|n| !evaluated.contains(**n) && !OVER_CAPTURED_EVENTS.contains(n))
        .collect();
    assert!(
        phantom.is_empty(),
        "accepted by varpulis check, never evaluated: {phantom:?}"
    );
}
