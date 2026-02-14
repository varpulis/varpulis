//! Integration tests for semantic validation checks (validate/checks.rs).
//!
//! Each test constructs a VPL program string, parses it, and runs validation
//! to verify that the correct diagnostic codes are emitted.

use varpulis_core::validate::{validate, Severity};
use varpulis_parser::parse;

// =============================================================================
// Helpers
// =============================================================================

/// Parse and validate a VPL source, returning (severity, code, message) triples.
fn validate_vpl(code: &str) -> Vec<(Severity, Option<&'static str>, String)> {
    let program =
        parse(code).unwrap_or_else(|e| panic!("Parse failed for:\n{}\nError: {}", code, e));
    let result = validate(code, &program);
    result
        .diagnostics
        .iter()
        .map(|d| (d.severity, d.code, d.message.clone()))
        .collect()
}

fn has_code(diagnostics: &[(Severity, Option<&'static str>, String)], code: &str) -> bool {
    diagnostics.iter().any(|(_, c, _)| *c == Some(code))
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

fn error_count(diagnostics: &[(Severity, Option<&'static str>, String)], code: &str) -> usize {
    diagnostics
        .iter()
        .filter(|(sev, c, _)| *sev == Severity::Error && *c == Some(code))
        .count()
}

// =============================================================================
// E001: Duplicate event declarations
// =============================================================================

#[test]
fn e001_duplicate_event_same_fields() {
    let diags = validate_vpl("event Temp: x: int\nevent Temp: x: int");
    assert!(has_error(&diags, "E001"), "Expected E001: {:?}", diags);
}

#[test]
fn e001_duplicate_event_different_fields() {
    let diags = validate_vpl("event Sensor: id: str\nevent Sensor: value: float");
    assert!(has_error(&diags, "E001"), "Expected E001: {:?}", diags);
}

#[test]
fn e001_no_error_distinct_events() {
    let diags = validate_vpl("event Alpha: x: int\nevent Beta: y: float");
    assert!(
        !has_code(&diags, "E001"),
        "Should have no E001: {:?}",
        diags
    );
}

// =============================================================================
// E002: Duplicate stream declarations
// =============================================================================

#[test]
fn e002_duplicate_stream() {
    let diags = validate_vpl("stream Output = A\nstream Output = B");
    assert!(has_error(&diags, "E002"), "Expected E002: {:?}", diags);
}

#[test]
fn e002_no_error_distinct_streams() {
    let diags = validate_vpl("stream First = A\nstream Second = B");
    assert!(
        !has_code(&diags, "E002"),
        "Should have no E002: {:?}",
        diags
    );
}

// =============================================================================
// E003: Duplicate function declarations
// =============================================================================

#[test]
fn e003_duplicate_function() {
    let diags = validate_vpl(
        "fn calc(a: int) -> int:\n    return a\nfn calc(b: int) -> int:\n    return b\n",
    );
    assert!(has_error(&diags, "E003"), "Expected E003: {:?}", diags);
}

#[test]
fn e003_no_error_distinct_functions() {
    let diags = validate_vpl(
        "fn add(a: int) -> int:\n    return a\nfn sub(b: int) -> int:\n    return b\n",
    );
    assert!(
        !has_code(&diags, "E003"),
        "Should have no E003: {:?}",
        diags
    );
}

// =============================================================================
// E004: Duplicate connector declarations
// =============================================================================

#[test]
fn e004_duplicate_connector() {
    let diags =
        validate_vpl("connector Out = mqtt(topic: \"a\")\nconnector Out = kafka(topic: \"b\")");
    assert!(has_error(&diags, "E004"), "Expected E004: {:?}", diags);
}

#[test]
fn e004_no_error_distinct_connectors() {
    let diags = validate_vpl(
        "connector MqttOut = mqtt(topic: \"a\")\nconnector KafkaOut = kafka(topic: \"b\")",
    );
    assert!(
        !has_code(&diags, "E004"),
        "Should have no E004: {:?}",
        diags
    );
}

// =============================================================================
// E005: Duplicate context declarations
// =============================================================================

#[test]
fn e005_duplicate_context() {
    let diags =
        validate_vpl("context ingestion (cores: [0, 1])\ncontext ingestion (cores: [2, 3])");
    assert!(has_error(&diags, "E005"), "Expected E005: {:?}", diags);
}

#[test]
fn e005_no_error_distinct_contexts() {
    let diags = validate_vpl("context ingest (cores: [0])\ncontext process (cores: [1])");
    assert!(
        !has_code(&diags, "E005"),
        "Should have no E005: {:?}",
        diags
    );
}

// =============================================================================
// E006: Duplicate pattern declarations
// =============================================================================

#[test]
fn e006_duplicate_pattern() {
    let diags = validate_vpl("pattern P = SEQ(A, B)\npattern P = SEQ(C, D)");
    assert!(has_error(&diags, "E006"), "Expected E006: {:?}", diags);
}

#[test]
fn e006_no_error_distinct_patterns() {
    let diags = validate_vpl("pattern P1 = SEQ(A, B)\npattern P2 = SEQ(C, D)");
    assert!(
        !has_code(&diags, "E006"),
        "Should have no E006: {:?}",
        diags
    );
}

// =============================================================================
// E007: Duplicate type alias declarations
// =============================================================================

#[test]
fn e007_duplicate_type_alias() {
    let diags = validate_vpl("type Temp = int\ntype Temp = float");
    assert!(has_error(&diags, "E007"), "Expected E007: {:?}", diags);
}

#[test]
fn e007_no_error_distinct_types() {
    let diags = validate_vpl("type Temp = int\ntype Humidity = float");
    assert!(
        !has_code(&diags, "E007"),
        "Should have no E007: {:?}",
        diags
    );
}

// =============================================================================
// E010: Having without prior aggregate
// =============================================================================

#[test]
fn e010_having_without_aggregate() {
    let diags = validate_vpl(
        r#"
        stream S = Events
            .having(total > 100)
    "#,
    );
    assert!(has_error(&diags, "E010"), "Expected E010: {:?}", diags);
}

#[test]
fn e010_having_after_aggregate_ok() {
    let diags = validate_vpl(
        r#"
        stream S = Events
            .window(5)
            .aggregate(total: sum(amount))
            .having(total > 100)
    "#,
    );
    assert!(
        !has_code(&diags, "E010"),
        "Should have no E010: {:?}",
        diags
    );
}

// =============================================================================
// E011: Duplicate aggregate
// =============================================================================

#[test]
fn e011_duplicate_aggregate() {
    let diags = validate_vpl(
        r#"
        stream S = Events
            .window(5)
            .aggregate(c: count())
            .aggregate(s: sum(val))
    "#,
    );
    assert!(has_error(&diags, "E011"), "Expected E011: {:?}", diags);
}

// =============================================================================
// E012: Duplicate window
// =============================================================================

#[test]
fn e012_duplicate_window() {
    let diags = validate_vpl(
        r#"
        stream S = Events
            .window(5)
            .window(10)
    "#,
    );
    assert!(has_error(&diags, "E012"), "Expected E012: {:?}", diags);
}

// =============================================================================
// E020: Within outside sequence context
// =============================================================================

#[test]
fn e020_within_outside_sequence() {
    let diags = validate_vpl(
        r#"
        stream S = Events
            .where(x > 0)
            .within(10s)
    "#,
    );
    assert!(has_error(&diags, "E020"), "Expected E020: {:?}", diags);
}

#[test]
fn e020_within_after_followed_by_ok() {
    let diags = validate_vpl(
        r#"
        stream S = A as a
            -> B as b
            .within(10s)
    "#,
    );
    assert!(
        !has_code(&diags, "E020"),
        "Should have no E020: {:?}",
        diags
    );
}

// =============================================================================
// E030: Undefined connector reference
// =============================================================================

#[test]
fn e030_undefined_connector_in_to() {
    let diags = validate_vpl(
        r#"
        stream S = Events
            .to(MissingConnector, topic: "out")
    "#,
    );
    assert!(has_error(&diags, "E030"), "Expected E030: {:?}", diags);
}

#[test]
fn e030_undefined_connector_in_from() {
    let diags = validate_vpl(
        r#"
        stream S = SensorEvent.from(MissingConnector, topic: "in")
    "#,
    );
    assert!(has_error(&diags, "E030"), "Expected E030: {:?}", diags);
}

#[test]
fn e030_valid_connector_reference_no_error() {
    let diags = validate_vpl(
        r#"
        connector MqttOut = mqtt(topic: "default")
        stream S = Events
            .to(MqttOut, topic: "output")
    "#,
    );
    assert!(
        !has_error(&diags, "E030"),
        "Should have no E030: {:?}",
        diags
    );
}

// =============================================================================
// E031: Undefined context reference
// =============================================================================

#[test]
fn e031_undefined_context() {
    let diags = validate_vpl(
        r#"
        stream S = Events
            .context(missing_ctx)
    "#,
    );
    assert!(has_error(&diags, "E031"), "Expected E031: {:?}", diags);
}

#[test]
fn e031_valid_context_reference_no_error() {
    let diags = validate_vpl(
        r#"
        context my_ctx (cores: [0, 1])
        stream S = Events
            .context(my_ctx)
    "#,
    );
    assert!(
        !has_error(&diags, "E031"),
        "Should have no E031: {:?}",
        diags
    );
}

// =============================================================================
// E040: Assignment to immutable variable
// =============================================================================

#[test]
fn e040_assign_to_let_variable() {
    let diags = validate_vpl("let x = 10\nx := 20");
    assert!(has_error(&diags, "E040"), "Expected E040: {:?}", diags);
}

#[test]
fn e040_assign_to_const() {
    let diags = validate_vpl("const LIMIT = 100\nLIMIT := 200");
    assert!(has_error(&diags, "E040"), "Expected E040: {:?}", diags);
}

#[test]
fn e040_mutable_var_assignment_ok() {
    let diags = validate_vpl("var counter = 0\ncounter := counter + 1");
    assert!(
        !has_code(&diags, "E040"),
        "Should have no E040: {:?}",
        diags
    );
}

// =============================================================================
// E050: Unknown function
// =============================================================================

#[test]
fn e050_unknown_function_in_let() {
    let diags = validate_vpl("let x = totally_fake_fn(42)");
    assert!(has_error(&diags, "E050"), "Expected E050: {:?}", diags);
}

#[test]
fn e050_builtin_function_ok() {
    let diags = validate_vpl("let x = abs(-5)");
    assert!(
        !has_code(&diags, "E050"),
        "Should have no E050: {:?}",
        diags
    );
}

#[test]
fn e050_user_declared_function_ok() {
    let diags = validate_vpl("fn double(x: int) -> int:\n    return x * 2\nlet y = double(3)");
    assert!(
        !has_code(&diags, "E050"),
        "Should have no E050: {:?}",
        diags
    );
}

// =============================================================================
// E051: Function arity mismatch
// =============================================================================

#[test]
fn e051_too_few_arguments() {
    let diags = validate_vpl("fn add(a: int, b: int) -> int:\n    return a + b\nlet x = add(1)");
    assert!(has_error(&diags, "E051"), "Expected E051: {:?}", diags);
}

#[test]
fn e051_too_many_arguments() {
    let diags = validate_vpl("fn inc(a: int) -> int:\n    return a + 1\nlet x = inc(1, 2)");
    assert!(has_error(&diags, "E051"), "Expected E051: {:?}", diags);
}

#[test]
fn e051_correct_arity_ok() {
    let diags = validate_vpl("fn add(a: int, b: int) -> int:\n    return a + b\nlet x = add(1, 2)");
    assert!(
        !has_code(&diags, "E051"),
        "Should have no E051: {:?}",
        diags
    );
}

// =============================================================================
// E060: Non-boolean expression in boolean context
// =============================================================================

#[test]
fn e060_where_with_integer() {
    let diags = validate_vpl("stream S = A\n    .where(42)");
    assert!(has_error(&diags, "E060"), "Expected E060: {:?}", diags);
}

#[test]
fn e060_where_with_string() {
    let diags = validate_vpl("stream S = A\n    .where(\"hello\")");
    assert!(has_error(&diags, "E060"), "Expected E060: {:?}", diags);
}

#[test]
fn e060_where_with_float() {
    let diags = validate_vpl("stream S = A\n    .where(3.14)");
    assert!(has_error(&diags, "E060"), "Expected E060: {:?}", diags);
}

#[test]
fn e060_where_with_null() {
    let diags = validate_vpl("stream S = A\n    .where(null)");
    assert!(has_error(&diags, "E060"), "Expected E060: {:?}", diags);
}

#[test]
fn e060_having_with_integer() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(c: count())
            .having(99)
    "#,
    );
    assert!(has_error(&diags, "E060"), "Expected E060: {:?}", diags);
}

#[test]
fn e060_where_with_comparison_ok() {
    let diags = validate_vpl("stream S = A\n    .where(value > 10)");
    assert!(
        !has_code(&diags, "E060"),
        "Should have no E060: {:?}",
        diags
    );
}

#[test]
fn e060_where_with_bool_literal_ok() {
    let diags = validate_vpl("stream S = A\n    .where(true)");
    assert!(
        !has_code(&diags, "E060"),
        "Should have no E060: {:?}",
        diags
    );
}

// =============================================================================
// E061: Invalid duration in duration context
// =============================================================================

#[test]
fn e061_within_with_string() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(\"bad\")",
    );
    assert!(has_error(&diags, "E061"), "Expected E061: {:?}", diags);
}

#[test]
fn e061_within_with_boolean() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(true)",
    );
    assert!(has_error(&diags, "E061"), "Expected E061: {:?}", diags);
}

#[test]
fn e061_within_with_float() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(3.14)",
    );
    assert!(has_error(&diags, "E061"), "Expected E061: {:?}", diags);
}

#[test]
fn e061_within_with_null() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(null)",
    );
    assert!(has_error(&diags, "E061"), "Expected E061: {:?}", diags);
}

#[test]
fn e061_within_with_duration_ok() {
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(10s)",
    );
    assert!(
        !has_code(&diags, "E061"),
        "Should have no E061: {:?}",
        diags
    );
}

#[test]
fn e061_within_with_integer_ok() {
    // Integer is allowed as count-based
    let diags = validate_vpl(
        "event A: x: int\nevent B: y: int\nstream S = A as a\n    -> B as b\n    .within(5)",
    );
    assert!(
        !has_code(&diags, "E061"),
        "Should have no E061: {:?}",
        diags
    );
}

#[test]
fn e061_allowed_lateness_with_string() {
    let diags = validate_vpl("stream S = A\n    .allowed_lateness(\"bad\")");
    assert!(has_error(&diags, "E061"), "Expected E061: {:?}", diags);
}

// =============================================================================
// E070: Unknown aggregate function
// =============================================================================

#[test]
fn e070_unknown_aggregate_function() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(x: percentile(value))
    "#,
    );
    assert!(has_error(&diags, "E070"), "Expected E070: {:?}", diags);
}

#[test]
fn e070_valid_aggregate_functions_ok() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(
                c: count(),
                s: sum(value),
                a: avg(value),
                mn: min(value),
                mx: max(value),
                sd: stddev(value),
                f: first(value),
                l: last(value)
            )
    "#,
    );
    assert!(
        !has_code(&diags, "E070"),
        "Should have no E070: {:?}",
        diags
    );
}

// =============================================================================
// E071: Aggregate function missing required field argument
// =============================================================================

#[test]
fn e071_sum_without_field() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(s: sum())
    "#,
    );
    assert!(has_error(&diags, "E071"), "Expected E071: {:?}", diags);
}

#[test]
fn e071_avg_without_field() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(a: avg())
    "#,
    );
    assert!(has_error(&diags, "E071"), "Expected E071: {:?}", diags);
}

#[test]
fn e071_min_without_field() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(m: min())
    "#,
    );
    assert!(has_error(&diags, "E071"), "Expected E071: {:?}", diags);
}

#[test]
fn e071_max_without_field() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(m: max())
    "#,
    );
    assert!(has_error(&diags, "E071"), "Expected E071: {:?}", diags);
}

#[test]
fn e071_stddev_without_field() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(s: stddev())
    "#,
    );
    assert!(has_error(&diags, "E071"), "Expected E071: {:?}", diags);
}

#[test]
fn e071_sum_with_field_ok() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(s: sum(amount))
    "#,
    );
    assert!(
        !has_code(&diags, "E071"),
        "Should have no E071: {:?}",
        diags
    );
}

// =============================================================================
// E072: Aggregate function requiring two arguments (ema)
// =============================================================================

#[test]
fn e072_ema_no_arguments() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(e: ema())
    "#,
    );
    assert!(has_error(&diags, "E072"), "Expected E072: {:?}", diags);
}

#[test]
fn e072_ema_one_argument() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(e: ema(value))
    "#,
    );
    assert!(has_error(&diags, "E072"), "Expected E072: {:?}", diags);
}

#[test]
fn e072_ema_two_arguments_ok() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(e: ema(value, 10))
    "#,
    );
    assert!(
        !has_code(&diags, "E072"),
        "Should have no E072: {:?}",
        diags
    );
}

// =============================================================================
// E073: Bare field reference in aggregate
// =============================================================================

#[test]
fn e073_bare_field_in_aggregate() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(v: value)
    "#,
    );
    assert!(has_error(&diags, "E073"), "Expected E073: {:?}", diags);
}

// =============================================================================
// E080: Unknown named parameter
// =============================================================================

#[test]
fn e080_unknown_log_parameter() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .log(severity: "info")
    "#,
    );
    assert!(has_error(&diags, "E080"), "Expected E080: {:?}", diags);
}

#[test]
fn e080_valid_log_parameters_ok() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .log(level: "info", message: "hello")
    "#,
    );
    assert!(
        !has_code(&diags, "E080"),
        "Should have no E080: {:?}",
        diags
    );
}

// =============================================================================
// E090: Unimplemented operations
// =============================================================================

#[test]
fn e090_map_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .map(x => x + 1)");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for map: {:?}",
        diags
    );
}

#[test]
fn e090_filter_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .filter(x => x > 0)");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for filter: {:?}",
        diags
    );
}

#[test]
fn e090_collect_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .collect()");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for collect: {:?}",
        diags
    );
}

#[test]
fn e090_distinct_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .distinct()");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for distinct: {:?}",
        diags
    );
}

#[test]
fn e090_order_by_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .order_by(ts)");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for order_by: {:?}",
        diags
    );
}

#[test]
fn e090_limit_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .limit(100)");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for limit: {:?}",
        diags
    );
}

#[test]
fn e090_first_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .first()");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for first: {:?}",
        diags
    );
}

#[test]
fn e090_all_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .all()");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for all: {:?}",
        diags
    );
}

#[test]
fn e090_concurrent_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .concurrent(workers: 4)");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for concurrent: {:?}",
        diags
    );
}

#[test]
fn e090_on_error_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .on_error(x => x)");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for on_error: {:?}",
        diags
    );
}

// =============================================================================
// W001: Aggregate without window
// =============================================================================

#[test]
fn w001_aggregate_without_window() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .aggregate(c: count())
    "#,
    );
    assert!(has_warning(&diags, "W001"), "Expected W001: {:?}", diags);
}

#[test]
fn w001_aggregate_with_window_no_warning() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(c: count())
    "#,
    );
    assert!(
        !has_code(&diags, "W001"),
        "Should have no W001: {:?}",
        diags
    );
}

// =============================================================================
// W002: Partition after window
// =============================================================================

#[test]
fn w002_partition_after_window() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .partition_by(region)
    "#,
    );
    assert!(has_warning(&diags, "W002"), "Expected W002: {:?}", diags);
}

#[test]
fn w002_partition_before_window_ok() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .partition_by(region)
            .window(5)
            .aggregate(c: count())
    "#,
    );
    assert!(
        !has_code(&diags, "W002"),
        "Should have no W002: {:?}",
        diags
    );
}

// =============================================================================
// W030: Undeclared event type or stream reference
// =============================================================================

#[test]
fn w030_undeclared_event_type() {
    let diags = validate_vpl("stream S = UndeclaredEvent\n    .where(x > 0)");
    assert!(has_warning(&diags, "W030"), "Expected W030: {:?}", diags);
}

#[test]
fn w030_declared_event_no_warning() {
    let diags = validate_vpl(
        "event SensorReading: value: float\nstream S = SensorReading\n    .where(value > 0.0)",
    );
    assert!(
        !has_code(&diags, "W030"),
        "Should have no W030: {:?}",
        diags
    );
}

#[test]
fn w030_reference_to_declared_stream_no_warning() {
    let diags = validate_vpl("stream Base = A\nstream Filtered = Base\n    .where(x > 0)");
    // Base is declared, so Filtered referencing Base should not produce W030 for 'Base'
    let w030_for_base = diags.iter().any(|(sev, c, msg)| {
        *sev == Severity::Warning && *c == Some("W030") && msg.contains("Base")
    });
    assert!(
        !w030_for_base,
        "Should not warn for declared stream Base: {:?}",
        diags
    );
}

// =============================================================================
// W031: Emit as undeclared type
// =============================================================================

#[test]
fn w031_emit_as_undeclared_type() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .emit as UnknownType (x: 1)
    "#,
    );
    assert!(has_warning(&diags, "W031"), "Expected W031: {:?}", diags);
}

#[test]
fn w031_emit_as_declared_event_no_warning() {
    let diags =
        validate_vpl("event Alert: msg: str\nstream S = A\n    .emit as Alert (msg: \"hi\")");
    assert!(
        !has_code(&diags, "W031"),
        "Should have no W031: {:?}",
        diags
    );
}

// =============================================================================
// W060: Arithmetic expression in boolean context
// =============================================================================

#[test]
fn w060_arithmetic_in_where() {
    let diags = validate_vpl("stream S = A\n    .where(x + 1)");
    assert!(has_warning(&diags, "W060"), "Expected W060: {:?}", diags);
}

#[test]
fn w060_multiplication_in_where() {
    let diags = validate_vpl("stream S = A\n    .where(x * 2)");
    assert!(has_warning(&diags, "W060"), "Expected W060: {:?}", diags);
}

#[test]
fn w060_subtraction_in_where() {
    let diags = validate_vpl("stream S = A\n    .where(x - 1)");
    assert!(has_warning(&diags, "W060"), "Expected W060: {:?}", diags);
}

// =============================================================================
// Combined / edge-case tests
// =============================================================================

#[test]
fn multiple_errors_in_single_stream() {
    // Stream with both unknown aggregate and duplicate window
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .window(10)
            .aggregate(x: bogus(val))
    "#,
    );
    assert!(has_error(&diags, "E012"), "Expected E012: {:?}", diags);
    assert!(has_error(&diags, "E070"), "Expected E070: {:?}", diags);
}

#[test]
fn sequence_with_within_valid() {
    let diags = validate_vpl(
        "event Start: id: int\nevent End: id: int\nstream S = Start as s\n    -> End as e\n    .within(30s)",
    );
    assert!(has_no_errors(&diags), "Should have no errors: {:?}", diags);
}

#[test]
fn complex_valid_program() {
    let diags = validate_vpl(
        r#"
        event SensorReading: sensor_id: str value: float
        connector MqttIn = mqtt(topic: "sensors/#")
        context ingest (cores: [0, 1])
        stream Filtered = SensorReading.from(MqttIn, topic: "sensors/temp")
            .context(ingest)
            .where(value > 0.0)
            .partition_by(sensor_id)
            .window(60s)
            .aggregate(avg_val: avg(value), cnt: count())
            .having(cnt > 5)
            .emit(sensor: sensor_id, average: avg_val)
    "#,
    );
    assert!(
        has_no_errors(&diags),
        "Valid complex program should have no errors: {:?}",
        diags
    );
}

#[test]
fn valid_empty_program() {
    let diags = validate_vpl("");
    assert!(has_no_errors(&diags), "Empty program: {:?}", diags);
}

#[test]
fn e040_const_cannot_be_reassigned() {
    let diags = validate_vpl("const MAX = 50\nMAX := 100");
    assert!(has_error(&diags, "E040"), "Expected E040: {:?}", diags);
}

#[test]
fn valid_count_distinct_aggregate() {
    let diags = validate_vpl(
        r#"
        stream S = A
            .window(5)
            .aggregate(cd: count_distinct(user_id))
    "#,
    );
    assert!(
        !has_code(&diags, "E070"),
        "count_distinct should be recognized: {:?}",
        diags
    );
}

#[test]
fn e050_nested_unknown_function() {
    // Unknown function nested in an expression
    let diags = validate_vpl("let x = abs(fake_func(1))");
    assert!(
        has_error(&diags, "E050"),
        "Expected E050 for nested call: {:?}",
        diags
    );
}

#[test]
fn pattern_with_undeclared_event_refs() {
    let diags = validate_vpl("pattern P = SEQ(UndeclaredA, UndeclaredB)");
    // W030 should be emitted for undeclared event types in patterns
    assert!(
        has_warning(&diags, "W030"),
        "Expected W030 for pattern refs: {:?}",
        diags
    );
}

#[test]
fn pattern_with_declared_event_refs_no_warning() {
    let diags = validate_vpl(
        "event Login: user: str\nevent Purchase: user: str\npattern P = SEQ(Login, Purchase)",
    );
    let w030_for_login = diags.iter().any(|(sev, c, msg)| {
        *sev == Severity::Warning && *c == Some("W030") && msg.contains("Login")
    });
    let w030_for_purchase = diags.iter().any(|(sev, c, msg)| {
        *sev == Severity::Warning && *c == Some("W030") && msg.contains("Purchase")
    });
    assert!(
        !w030_for_login && !w030_for_purchase,
        "Declared events should not produce W030: {:?}",
        diags
    );
}

#[test]
fn validation_result_format_includes_codes() {
    let code = "event Dup: x: int\nevent Dup: y: float";
    let program = parse(code).expect("Parse failed");
    let result = validate(code, &program);
    let formatted = result.format(code);
    assert!(
        formatted.contains("E001"),
        "Formatted output should contain E001: {}",
        formatted
    );
    assert!(
        formatted.contains("duplicate"),
        "Formatted output should contain 'duplicate': {}",
        formatted
    );
}

#[test]
fn validation_result_has_errors_method() {
    let code = "event X: a: int\nevent X: b: int";
    let program = parse(code).expect("Parse failed");
    let result = validate(code, &program);
    assert!(
        result.has_errors(),
        "Program with E001 should have_errors()"
    );
}

#[test]
fn validation_result_no_errors_for_valid() {
    let code = "event X: a: int";
    let program = parse(code).expect("Parse failed");
    let result = validate(code, &program);
    assert!(
        !result.has_errors(),
        "Valid program should not have_errors()"
    );
}

#[test]
fn three_duplicate_events_produce_two_e001() {
    let diags = validate_vpl("event E: x: int\nevent E: y: float\nevent E: z: str");
    assert_eq!(
        error_count(&diags, "E001"),
        2,
        "Three duplicate events should produce exactly two E001 errors: {:?}",
        diags
    );
}

// =============================================================================
// E090: fork, any, to(expr) — previously untested
// =============================================================================

#[test]
fn e090_fork_not_implemented() {
    let diags =
        validate_vpl("stream S = A\n    .fork(branch1: .where(x > 0), branch2: .where(x < 0))");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for fork: {:?}",
        diags
    );
}

#[test]
fn e090_any_not_implemented() {
    let diags = validate_vpl("stream S = A\n    .any(3)");
    assert!(
        has_error(&diags, "E090"),
        "Expected E090 for any: {:?}",
        diags
    );
}

// =============================================================================
// Stream operations that should NOT produce errors
// =============================================================================

#[test]
fn no_error_for_select() {
    let diags = validate_vpl("stream S = A\n    .select(x, y)");
    assert!(
        !has_error(&diags, "E090"),
        "select should not produce E090: {:?}",
        diags
    );
}

#[test]
fn no_error_for_print() {
    let diags = validate_vpl("stream S = A\n    .print()");
    assert!(
        !has_error(&diags, "E090"),
        "print should not produce E090: {:?}",
        diags
    );
}

#[test]
fn no_error_for_emit() {
    let diags = validate_vpl("stream S = A\n    .emit()");
    assert!(
        has_no_errors(&diags),
        "emit should not produce errors: {:?}",
        diags
    );
}

#[test]
fn no_error_for_where() {
    let diags = validate_vpl("stream S = A\n    .where(x > 0)");
    assert!(
        !has_error(&diags, "E090"),
        "where should not produce E090: {:?}",
        diags
    );
}

// =============================================================================
// E010: multiple having without aggregate
// =============================================================================

#[test]
fn e010_multiple_having_without_aggregate() {
    let diags = validate_vpl("stream S = A\n    .having(x > 1)\n    .having(y > 2)");
    assert!(
        error_count(&diags, "E010") >= 1,
        "Expected at least one E010 for having without aggregate: {:?}",
        diags
    );
}

// =============================================================================
// Scope: SymbolTable helper methods
// =============================================================================

#[test]
fn scope_all_names_collects_all_declaration_types() {
    let code = r#"
event MyEvent:
    x: int

connector MyConn = mqtt(host: "localhost")

stream MyStream = MyEvent
    .emit()

fn myFunc(a: int) -> int:
    return a
"#;
    let program = parse(code).expect("Parse failed");
    let result = validate(code, &program);
    // If valid, symbols were collected correctly
    assert!(
        !result.has_errors(),
        "Should have no errors: {:?}",
        result.diagnostics
    );
}

// =============================================================================
// W030: Unused event/stream declarations
// =============================================================================

#[test]
fn w030_undeclared_source_produces_warning() {
    let diags = validate_vpl("stream S = UndeclaredEvent\n    .emit()");
    assert!(
        has_warning(&diags, "W030"),
        "Expected W030 for undeclared source: {:?}",
        diags
    );
}

#[test]
fn w030_no_warning_when_event_declared() {
    let diags = validate_vpl("event Used:\n    x: int\nstream S = Used\n    .emit()");
    assert!(
        !has_code(&diags, "W030"),
        "Declared event should not produce W030: {:?}",
        diags
    );
}

// =============================================================================
// E050: Undefined function calls
// =============================================================================

#[test]
fn e050_undefined_function_in_var_decl() {
    let diags = validate_vpl("var x = nonexistent_func(42)");
    assert!(
        has_error(&diags, "E050"),
        "Expected E050 for undefined function: {:?}",
        diags
    );
}

// =============================================================================
// E011: Duplicate aggregate
// =============================================================================

#[test]
fn e011_duplicate_aggregate_on_same_stream() {
    let diags = validate_vpl(
        "stream S = A\n    .window(5)\n    .aggregate(c: count())\n    .aggregate(s: sum(x))",
    );
    assert!(
        has_error(&diags, "E011"),
        "Expected E011 for duplicate aggregate: {:?}",
        diags
    );
}

// =============================================================================
// E012: Duplicate window
// =============================================================================

#[test]
fn e012_duplicate_window_on_same_stream() {
    let diags = validate_vpl("stream S = A\n    .window(5)\n    .window(10)");
    assert!(
        has_error(&diags, "E012"),
        "Expected E012 for duplicate window: {:?}",
        diags
    );
}

// =============================================================================
// E060/E061: Aggregate function checks
// =============================================================================

#[test]
fn e071_aggregate_without_field_for_sum() {
    let diags = validate_vpl("stream S = A\n    .window(5)\n    .aggregate(s: sum())");
    assert!(
        has_error(&diags, "E071"),
        "Expected E071 for sum() without field: {:?}",
        diags
    );
}

#[test]
fn e070_aggregate_unknown_function() {
    let diags = validate_vpl("stream S = A\n    .window(5)\n    .aggregate(m: median(x))");
    assert!(
        has_error(&diags, "E070"),
        "Expected E070 for unknown aggregate function: {:?}",
        diags
    );
}

// =============================================================================
// Connector parameter validation
// =============================================================================

#[test]
fn valid_mqtt_connector_with_required_params() {
    let diags = validate_vpl("connector mqtt_in = mqtt(host: \"localhost\", port: 1883)");
    assert!(
        !has_error(&diags, "W080"),
        "Valid MQTT should not produce W080: {:?}",
        diags
    );
}

// =============================================================================
// Complex expression validation
// =============================================================================

#[test]
fn valid_nested_expressions_in_where() {
    let diags = validate_vpl("stream S = A\n    .where(x > 0 and y < 100 or z == \"active\")");
    // No E090 or other errors for valid where clause
    assert!(
        !has_error(&diags, "E090"),
        "Valid where should not produce E090: {:?}",
        diags
    );
}
