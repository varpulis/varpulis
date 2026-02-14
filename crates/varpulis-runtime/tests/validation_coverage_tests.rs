//! Coverage tests for semantic validation (validate/checks.rs).
//!
//! Uses the parser for reliable AST construction, then runs validate().

use varpulis_core::validate::{validate, Severity};
use varpulis_parser::parse;

/// Parse and validate, return error codes.
fn validate_vpl(code: &str) -> Vec<(Severity, Option<&'static str>, String)> {
    let program = parse(code).expect("Parse failed");
    let result = validate(code, &program);
    result
        .diagnostics
        .iter()
        .map(|d| (d.severity, d.code, d.message.clone()))
        .collect()
}

fn has_error(diagnostics: &[(Severity, Option<&'static str>, String)], code: &str) -> bool {
    diagnostics
        .iter()
        .any(|(sev, c, _)| *sev == Severity::Error && *c == Some(code))
}

fn has_warning(diagnostics: &[(Severity, Option<&'static str>, String)], code: &str) -> bool {
    diagnostics
        .iter()
        .any(|(sev, c, _)| *sev == Severity::Warning && *c == Some(code))
}

fn has_no_errors(diagnostics: &[(Severity, Option<&'static str>, String)]) -> bool {
    !diagnostics
        .iter()
        .any(|(sev, _, _)| *sev == Severity::Error)
}

// =============================================================================
// Duplicate declarations
// =============================================================================

#[test]
fn duplicate_event_declaration() {
    let diags = validate_vpl("event Foo: x: int\nevent Foo: y: float");
    assert!(has_error(&diags, "E001"), "Duplicate event: {:?}", diags);
}

#[test]
fn duplicate_stream_declaration() {
    let diags = validate_vpl(
        r#"
        stream S = A
        stream S = B
    "#,
    );
    assert!(has_error(&diags, "E002"), "Duplicate stream: {:?}", diags);
}

#[test]
fn duplicate_function_declaration() {
    let diags = validate_vpl("fn f() -> int:\n    return 1\nfn f() -> int:\n    return 2\n");
    assert!(has_error(&diags, "E003"), "Duplicate function: {:?}", diags);
}

#[test]
fn duplicate_connector_declaration() {
    let diags = validate_vpl(
        r#"
        connector C = mqtt(topic: "a")
        connector C = kafka(topic: "b")
    "#,
    );
    assert!(
        has_error(&diags, "E004"),
        "Duplicate connector: {:?}",
        diags
    );
}

// =============================================================================
// Having without aggregate
// =============================================================================

#[test]
fn having_without_aggregate() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .having(cnt > 0)
    "#,
    );
    assert!(
        has_error(&diags, "E010"),
        "Having without aggregate: {:?}",
        diags
    );
}

// =============================================================================
// Duplicate aggregate / window
// =============================================================================

#[test]
fn duplicate_aggregate() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(c: count())
            .aggregate(s: sum(value))
    "#,
    );
    assert!(
        has_error(&diags, "E011"),
        "Duplicate aggregate: {:?}",
        diags
    );
}

#[test]
fn duplicate_window() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .window(10)
    "#,
    );
    assert!(has_error(&diags, "E012"), "Duplicate window: {:?}", diags);
}

// =============================================================================
// Within outside sequence
// =============================================================================

#[test]
fn within_outside_sequence() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .within(5s)
    "#,
    );
    assert!(
        has_error(&diags, "E020"),
        "Within outside sequence: {:?}",
        diags
    );
}

// =============================================================================
// Assignment to immutable
// =============================================================================

#[test]
fn assignment_to_immutable() {
    let diags = validate_vpl(
        r#"
        let x = 1
        x := 2
    "#,
    );
    assert!(
        has_error(&diags, "E040"),
        "Immutable assignment: {:?}",
        diags
    );
}

// =============================================================================
// Unknown function call
// =============================================================================

#[test]
fn unknown_function_call() {
    let diags = validate_vpl("let x = nonexistent_func(1)");
    assert!(has_error(&diags, "E050"), "Unknown function: {:?}", diags);
}

// =============================================================================
// Where with non-boolean literal
// =============================================================================

#[test]
fn where_non_boolean_literal() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .where(42)
    "#,
    );
    assert!(has_error(&diags, "E060"), "Where non-bool: {:?}", diags);
}

#[test]
fn where_string_literal() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .where("hello")
    "#,
    );
    assert!(has_error(&diags, "E060"), "Where string: {:?}", diags);
}

// =============================================================================
// Within with non-duration
// =============================================================================

#[test]
fn within_non_duration() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: x: int\nstream S = A as a\n    -> B as b\n    .within(3.14)",
    );
    assert!(
        has_error(&diags, "E061"),
        "Within non-duration: {:?}",
        diags
    );
}

// =============================================================================
// Unknown aggregate function
// =============================================================================

#[test]
fn unknown_aggregate_function() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(x: bogus_agg(value))
    "#,
    );
    assert!(has_error(&diags, "E070"), "Unknown aggregate: {:?}", diags);
}

// =============================================================================
// Aggregate missing field argument
// =============================================================================

#[test]
fn aggregate_missing_field_arg() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(s: sum())
    "#,
    );
    assert!(has_error(&diags, "E071"), "Missing field arg: {:?}", diags);
}

// =============================================================================
// Bare field in aggregate
// =============================================================================

#[test]
fn bare_field_in_aggregate() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(v: value)
    "#,
    );
    assert!(has_error(&diags, "E073"), "Bare field: {:?}", diags);
}

// =============================================================================
// Aggregate without window (warning)
// =============================================================================

#[test]
fn aggregate_without_window_warning() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .aggregate(c: count())
    "#,
    );
    assert!(
        has_warning(&diags, "W001"),
        "Aggregate without window warning: {:?}",
        diags
    );
}

// =============================================================================
// Arithmetic in where (warning)
// =============================================================================

#[test]
fn arithmetic_in_where_warning() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .where(x + 1)
    "#,
    );
    assert!(
        has_warning(&diags, "W060"),
        "Arithmetic in where: {:?}",
        diags
    );
}

// =============================================================================
// Valid programs produce no errors
// =============================================================================

#[test]
fn valid_simple_stream() {
    let diags = validate_vpl(
        r#"
        stream S = Reading
            .where(value > 0.0)
    "#,
    );
    assert!(has_no_errors(&diags), "Valid stream: {:?}", diags);
}

#[test]
fn valid_aggregate_with_window() {
    let diags = validate_vpl(
        r#"
        stream S = Reading
            .window(5)
            .aggregate(c: count(), s: sum(value), a: avg(value))
    "#,
    );
    assert!(has_no_errors(&diags), "Valid aggregate: {:?}", diags);
}

#[test]
fn valid_sequence() {
    let diags = validate_vpl(
        r#"
        stream S = A as a
            -> B as b
            .within(10s)
    "#,
    );
    assert!(has_no_errors(&diags), "Valid sequence: {:?}", diags);
}

#[test]
fn valid_function_declaration() {
    let diags = validate_vpl("fn add(a: int, b: int):\n    return a + b\n");
    assert!(has_no_errors(&diags), "Valid function: {:?}", diags);
}

#[test]
fn valid_mutable_variable() {
    let diags = validate_vpl(
        r#"
        var x = 1
        x := 2
    "#,
    );
    assert!(has_no_errors(&diags), "Valid mutable var: {:?}", diags);
}

#[test]
fn valid_event_declaration() {
    let diags = validate_vpl("event Reading: sensor_id: str value: float timestamp: int");
    assert!(has_no_errors(&diags), "Valid event decl: {:?}", diags);
}

#[test]
fn empty_program_is_valid() {
    let diags = validate_vpl("");
    assert!(has_no_errors(&diags), "Empty program: {:?}", diags);
}

// =============================================================================
// Format output
// =============================================================================

#[test]
fn format_includes_error_info() {
    let code = "event Foo: x: int\nevent Foo: y: float";
    let program = parse(code).expect("Parse failed");
    let result = validate(code, &program);
    let formatted = result.format(code);
    assert!(!formatted.is_empty());
    assert!(
        formatted.contains("error") || formatted.contains("E001"),
        "Format: {}",
        formatted
    );
}

// =============================================================================
// Undeclared event/stream warnings
// =============================================================================

#[test]
fn undeclared_event_type_warning() {
    let diags = validate_vpl(
        r#"
        stream S = NonExistentEvent
            .where(value > 0)
    "#,
    );
    // W030 warns about undeclared event types
    assert!(
        has_warning(&diags, "W030"),
        "Undeclared event warning: {:?}",
        diags
    );
}

// =============================================================================
// Connector reference in .to()
// =============================================================================

#[test]
fn undefined_connector_in_to() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .to(UndefinedConnector, topic: "test")
    "#,
    );
    assert!(
        has_error(&diags, "E030"),
        "Undefined connector: {:?}",
        diags
    );
}

// =============================================================================
// Valid connector reference
// =============================================================================

#[test]
fn valid_connector_reference() {
    let diags = validate_vpl(
        r#"
        connector MyMqtt = mqtt(topic: "test")
        stream S = A
            .to(MyMqtt, topic: "output")
    "#,
    );
    let has_e030 = has_error(&diags, "E030");
    assert!(
        !has_e030,
        "Valid connector ref should not error: {:?}",
        diags
    );
}

// =============================================================================
// Function arity mismatch
// =============================================================================

#[test]
fn function_arity_mismatch() {
    let diags = validate_vpl("fn add(a: int, b: int):\n    return a + b\nlet x = add(1)");
    assert!(has_error(&diags, "E051"), "Arity mismatch: {:?}", diags);
}

// =============================================================================
// EMA missing two arguments
// =============================================================================

#[test]
fn ema_missing_arguments() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(e: ema())
    "#,
    );
    assert!(has_error(&diags, "E072"), "EMA missing args: {:?}", diags);
}

// =============================================================================
// Partition by after window (warning)
// =============================================================================

#[test]
fn partition_after_window_warning() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .partition_by(region)
    "#,
    );
    assert!(
        has_warning(&diags, "W002"),
        "Partition after window: {:?}",
        diags
    );
}

// =============================================================================
// Duplicate type alias
// =============================================================================

#[test]
fn duplicate_type_alias() {
    let diags = validate_vpl("type Temp = int\ntype Temp = float");
    assert!(has_error(&diags, "E007"), "Duplicate type: {:?}", diags);
}

// =============================================================================
// Count requires no field argument
// =============================================================================

#[test]
fn count_takes_no_field_arg() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(c: count())
    "#,
    );
    // count() with no args is valid
    assert!(has_no_errors(&diags), "count() no args: {:?}", diags);
}

// =============================================================================
// Various valid programs
// =============================================================================

#[test]
fn valid_sequence_with_within() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(5s)",
    );
    assert!(
        has_no_errors(&diags),
        "Valid sequence with within: {:?}",
        diags
    );
}

#[test]
fn valid_stream_with_partition_before_window() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .partition_by(region)
            .window(5)
            .aggregate(c: count())
    "#,
    );
    // partition_by BEFORE window is fine (no W002)
    let has_w002 = has_warning(&diags, "W002");
    assert!(!has_w002, "No warning expected: {:?}", diags);
}

#[test]
fn valid_connector_declaration() {
    let diags = validate_vpl(
        r#"
        connector K = kafka(brokers: "localhost:9092", topic: "test")
    "#,
    );
    assert!(has_no_errors(&diags), "Valid connector: {:?}", diags);
}

#[test]
fn unimplemented_distinct_warns() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .distinct()
    "#,
    );
    assert!(
        has_error(&diags, "E090"),
        "Distinct should report E090: {:?}",
        diags
    );
}

#[test]
fn unimplemented_limit_warns() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .limit(100)
    "#,
    );
    assert!(
        has_error(&diags, "E090"),
        "Limit should report E090: {:?}",
        diags
    );
}

#[test]
fn valid_var_declaration_and_assignment() {
    let diags = validate_vpl(
        r#"
        var x = 0
        x := x + 1
        var y = "hello"
        y := y + " world"
    "#,
    );
    assert!(has_no_errors(&diags), "Valid var ops: {:?}", diags);
}

#[test]
fn valid_let_declaration() {
    let diags = validate_vpl(
        r#"
        let x = 42
        let y = "hello"
        let z = true
    "#,
    );
    assert!(has_no_errors(&diags), "Valid let decls: {:?}", diags);
}

#[test]
fn within_with_string_literal() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(\"bad\")",
    );
    assert!(has_error(&diags, "E061"), "Within with string: {:?}", diags);
}

#[test]
fn within_with_bool_literal() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(true)",
    );
    assert!(has_error(&diags, "E061"), "Within with bool: {:?}", diags);
}

#[test]
fn where_with_boolean_literal_true() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .where(true)
    "#,
    );
    // true is a valid boolean expression for .where()
    assert!(has_no_errors(&diags), "Where with true: {:?}", diags);
}

#[test]
fn where_with_float_literal() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .where(3.14)
    "#,
    );
    assert!(has_error(&diags, "E060"), "Where float: {:?}", diags);
}
