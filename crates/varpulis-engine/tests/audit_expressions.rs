//! Expression operators and builtins the language reference lists.
//!
//! Each of these parsed, checked "ok", and computed nothing or something
//! else: the bitwise operators evaluated to no value, `round(x, 2)` ignored
//! the 2, `??` did not parse, `x is int` read a field called `int`, and a
//! user function could not see the program's constants.

mod common;

use common::{at, feed, only};
use varpulis_engine::Program;

fn emit_of(fields: &str, payload: &str) -> serde_json::Value {
    let program = format!(
        r"
event T:
    n: int
    f: float
    s: str
stream S = T
    .emit({fields})
"
    );
    let mut p = Program::compile(&program).unwrap_or_else(|e| panic!("{program}\n{e}"));
    only(feed(&mut p, "T", &at("10:00:00", payload)))
}

const T: &str = r#""n": 5, "f": 1.23456, "s": "Hello""#;

#[test]
fn bitwise_operators_compute() {
    let out = emit_of(
        "a: n & 4, o: n | 8, x: n ^ 1, l: n << 1, r: n >> 1, c: ~n",
        T,
    );
    assert_eq!(out["a"], 4);
    assert_eq!(out["o"], 13);
    assert_eq!(out["x"], 4);
    assert_eq!(out["l"], 10);
    assert_eq!(out["r"], 2);
    assert_eq!(out["c"], -6);
}

#[test]
fn a_bitwise_test_in_a_filter_filters() {
    let program = r"
event T:
    n: int
stream S = T
    .where((n & 4) == 4)
    .emit(n: n)
";
    let mut p = Program::compile(program).unwrap();
    assert_eq!(
        only(feed(&mut p, "T", &at("10:00:00", r#""n": 5"#)))["n"],
        5
    );
    assert!(feed(&mut p, "T", &at("10:00:01", r#""n": 2"#)).is_empty());
}

#[test]
fn round_honours_its_number_of_decimals() {
    let out = emit_of("r2: round(f, 2), r0: round(f), r1: round(2.25, 1)", T);
    assert_eq!(out["r2"], 1.23);
    assert_eq!(out["r0"], 1);
    assert_eq!(out["r1"], 2.3);
}

#[test]
fn null_coalescing_takes_the_default_for_a_missing_field() {
    let out = emit_of(r#"a: missing ?? "dflt", b: n ?? 0"#, T);
    assert_eq!(out["a"], "dflt");
    assert_eq!(out["b"], 5);
}

#[test]
fn null_coalescing_in_a_filter_filters() {
    let program = r"
event T:
    n: int
stream S = T
    .where((missing ?? 0) == 0 and (n ?? 0) == 5)
    .emit(n: n)
";
    let mut p = Program::compile(program).unwrap();
    assert_eq!(
        only(feed(&mut p, "T", &at("10:00:00", r#""n": 5"#)))["n"],
        5
    );
    assert!(feed(&mut p, "T", &at("10:00:01", r#""n": 6"#)).is_empty());
}

#[test]
fn is_tests_the_type_of_a_value() {
    let out = emit_of(
        "a: n is int, b: n is str, c: s is str, d: f is float, e: missing is null, g: n is null",
        T,
    );
    assert_eq!(out["a"], true);
    assert_eq!(out["b"], false);
    assert_eq!(out["c"], true);
    assert_eq!(out["d"], true);
    assert_eq!(out["e"], true, "a field the event lacks is null");
    assert_eq!(out["g"], false);
}

#[test]
fn a_type_test_in_a_filter_filters() {
    let program = r"
event T:
    n: int
stream S = T
    .where(n is int)
    .emit(n: n)
";
    let mut p = Program::compile(program).unwrap();
    assert_eq!(
        only(feed(&mut p, "T", &at("10:00:00", r#""n": 5"#)))["n"],
        5
    );
    assert!(feed(&mut p, "T", &at("10:00:01", r#""n": "five""#)).is_empty());
}

#[test]
fn a_user_function_reads_the_programs_constants() {
    let program = r"
const LIMIT = 100
let factor = 2
fn times_factor(x: int) -> int:
    return x * factor
fn over(x: int) -> bool:
    return x > LIMIT
event T:
    v: int
stream S = T
    .emit(v: v, tf: times_factor(v), o: over(v))
";
    let mut p = Program::compile(program).unwrap();
    let out = only(feed(&mut p, "T", &at("10:00:00", r#""v": 60"#)));
    assert_eq!(out["tf"], 120);
    assert_eq!(out["o"], false);
    let out = only(feed(&mut p, "T", &at("10:00:01", r#""v": 160"#)));
    assert_eq!(out["tf"], 320);
    assert_eq!(out["o"], true);
}

#[test]
fn a_user_function_parameter_shadows_a_constant() {
    let program = r"
const x = 7
fn twice(x: int) -> int:
    return x * 2
event T:
    v: int
stream S = T
    .emit(t: twice(v))
";
    let mut p = Program::compile(program).unwrap();
    assert_eq!(
        only(feed(&mut p, "T", &at("10:00:00", r#""v": 5"#)))["t"],
        10
    );
}
