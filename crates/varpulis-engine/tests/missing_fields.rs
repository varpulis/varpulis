//! A condition on a field the event does not carry.
//!
//! Real logs are uneven: one Sysmon version has `OriginalFileName`, the next
//! host's does not, a proxy omits `Referer`. A rule such as
//! `Image.ends_with("\psexec.exe") or OriginalFileName == "psexec.c"` must
//! still fire on the image when the second field is absent, and a filter
//! (`selection and not filter`) must not swallow every event that lacks the
//! filter's field. A `->` sequence step always behaved that way (varpulis-sase
//! evaluates predicates two-valued); `.where()` returned "no value" for the
//! whole expression instead, so the same predicate answered differently in
//! the two places.

use varpulis_engine::Program;

/// `{"a": "x"}`: `a` is there, `b` is not.
const WITHOUT_B: &[u8] =
    br#"{"type": "T", "@timestamp": "2026-09-22T10:00:01Z", "k": "1", "a": "x"}"#;
const START: &[u8] = br#"{"type": "Start", "@timestamp": "2026-09-22T10:00:00Z", "k": "1"}"#;

const EVENTS: &str = r"
event Start:
    k: str

event T:
    k: str
    a: str
    b: str
";

fn where_fires(predicate: &str) -> bool {
    let src = format!("{EVENTS}\nstream R = T\n    .where({predicate})\n    .emit(a: a)\n");
    let mut program = Program::compile(&src).unwrap_or_else(|e| panic!("{predicate}: {e}"));
    !program.feed_json("T", WITHOUT_B).unwrap().is_empty()
}

fn sequence_step_fires(predicate: &str) -> bool {
    let src = format!(
        "{EVENTS}\nstream R = Start as s\n    -> T where k == s.k and ({predicate}) as t\n    .within(1m)\n    .emit(a: t.a)\n"
    );
    let mut program = Program::compile(&src).unwrap_or_else(|e| panic!("{predicate}: {e}"));
    let mut emits = program.feed_json("Start", START).unwrap();
    emits.extend(program.feed_json("T", WITHOUT_B).unwrap());
    !emits.is_empty()
}

#[test]
fn or_holds_when_its_other_side_names_a_missing_field() {
    assert!(where_fires(r#"a == "x" or ends_with(b, "y")"#));
    assert!(where_fires(r#"ends_with(b, "y") or a == "x""#));
    assert!(where_fires(r#"a == "x" or b == "y""#));
}

#[test]
fn a_filter_on_a_missing_field_does_not_exclude_the_event() {
    assert!(where_fires(r#"a == "x" and not ends_with(b, "y")"#));
    assert!(where_fires(r#"a == "x" and not (b == "y")"#));
}

#[test]
fn a_condition_on_a_missing_field_is_false_not_true() {
    assert!(!where_fires(r#"ends_with(b, "y")"#));
    assert!(!where_fires(r#"a == "x" and ends_with(b, "y")"#));
    assert!(!where_fires(r#"b == "y""#));
    // `!=` on a field that is not there is a condition on that field, so it
    // is false too; `not (b == "y")` is the way to say "unless b is y".
    assert!(!where_fires(r#"b != "y""#));
}

#[test]
fn a_predicate_means_the_same_in_where_and_in_a_sequence_step() {
    for predicate in [
        r#"a == "x" or ends_with(b, "y")"#,
        r#"ends_with(b, "y") or a == "x""#,
        r#"a == "x" and not ends_with(b, "y")"#,
        r#"a == "x" and not (b == "y")"#,
        r#"a == "x" and ends_with(b, "y")"#,
        r#"not ends_with(b, "y")"#,
        r#"b != "y""#,
        r#"b == "y""#,
    ] {
        assert_eq!(
            where_fires(predicate),
            sequence_step_fires(predicate),
            "`{predicate}` answers differently in .where() and in a -> step"
        );
    }
}

/// A missing field passed to a function is `null`, in its own position. It
/// used to be dropped from the argument list, so `is_null(b)` saw no argument
/// at all and answered nothing, `not is_null(b)` then held for an event
/// without `b`, and a user function received its arguments shifted.
#[test]
fn a_missing_field_is_null_when_passed_to_a_function() {
    assert!(where_fires("is_null(b)"));
    assert!(!where_fires("not is_null(b)"));
    assert!(where_fires("b.is_null()"));
    assert!(where_fires(r#"coalesce(b, "none") == "none""#));
    assert!(where_fires(r#"coalesce(b, a) == "x""#));
    assert!(!where_fires("is_null(a)"));
    for predicate in ["is_null(b)", "not is_null(b)", "not is_null(a)"] {
        assert_eq!(
            where_fires(predicate),
            sequence_step_fires(predicate),
            "`{predicate}` answers differently in .where() and in a -> step"
        );
    }
}

#[test]
fn a_user_function_receives_its_arguments_in_place() {
    let src = format!(
        "{EVENTS}\nfn second(x: str, y: str) -> str:\n    return y\n\nstream R = T\n    .where(second(b, a) == \"x\")\n    .emit(a: a)\n"
    );
    let mut program = Program::compile(&src).unwrap();
    assert_eq!(program.feed_json("T", WITHOUT_B).unwrap().len(), 1);
}

#[test]
fn regex_match_searches_with_rust_regex_syntax() {
    let src = r"
stream R = T
    .where(regex_match(Image, '(?i)\\svc[a-z]+\.exe$') and not regex_match(CommandLine, '-nop'))
    .emit(image: Image)
";
    let mut program = Program::compile(src).unwrap();
    let fires = |program: &mut Program, json: &str| {
        !program.feed_json("T", json.as_bytes()).unwrap().is_empty()
    };
    assert!(fires(
        &mut program,
        r#"{"type": "T", "Image": "C:\\Tools\\SVCUpdate.exe", "CommandLine": "x"}"#
    ));
    assert!(!fires(
        &mut program,
        r#"{"type": "T", "Image": "C:\\Tools\\SVCUpdate.exe", "CommandLine": "x -nop"}"#
    ));
    assert!(!fires(
        &mut program,
        r#"{"type": "T", "Image": "C:\\Tools\\update.exe", "CommandLine": "x"}"#
    ));
    // The regex is searched for, not anchored: anchor it with ^ and $ to match whole.
    assert!(fires(
        &mut program,
        r#"{"type": "T", "Image": "D:\\svcx.exe", "CommandLine": ""}"#
    ));
    // No Image at all: the condition is false, and so is the rule.
    assert!(!fires(&mut program, r#"{"type": "T", "CommandLine": "x"}"#));
}

/// Documented as implemented since the builtins table was written; they were
/// not, and a call answered nothing.
#[test]
fn unique_and_clamp_do_what_the_builtins_table_says() {
    let src = r#"
stream R = T
    .emit(n: len(unique(split(a, ","))), c: clamp(to_int(k), 0, 10), f: clamp(to_float(k), 0.5, 2.5))
"#;
    let mut program = Program::compile(src).unwrap();
    let emits = program
        .feed_json("T", br#"{"type": "T", "a": "x,y,x,z,y", "k": "42"}"#)
        .unwrap();
    let json = emits[0].to_json();
    assert_eq!(json["n"], 3, "{json}");
    assert_eq!(json["c"], 10, "{json}");
    assert_eq!(json["f"], 2.5, "{json}");
}
