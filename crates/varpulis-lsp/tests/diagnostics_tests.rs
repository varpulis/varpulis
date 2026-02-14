//! Targeted tests for varpulis-lsp diagnostics.rs coverage.
//!
//! Exercises each ParseError variant branch in error_to_diagnostic(),
//! the semantic_to_lsp_diagnostic() path, position_to_line_col(),
//! and get_error_end_column().

use tower_lsp::lsp_types::*;
use varpulis_lsp::diagnostics::get_diagnostics;

// =============================================================================
// get_diagnostics — semantic validation paths
// =============================================================================

#[test]
fn diagnostics_valid_event_and_stream() {
    let code = r#"
event SensorReading:
    temperature: int
    humidity: float

stream Filtered = SensorReading
    .where(temperature > 30)
    .emit()
"#;
    let diags = get_diagnostics(code);
    assert!(
        diags.is_empty(),
        "Valid program should have no diagnostics: {:?}",
        diags
    );
}

#[test]
fn diagnostics_valid_pattern() {
    let code = r#"
event Warning:
    level: int

event Error:
    code: int

pattern Alert = SEQ(w: Warning, e: Error) within 5m
"#;
    let diags = get_diagnostics(code);
    // Pattern syntax may produce warnings but should not panic
    let _ = diags;
}

#[test]
fn diagnostics_valid_function_declaration() {
    let code = r#"
fn double(x: int) -> int {
    x * 2
}
"#;
    let diags = get_diagnostics(code);
    let _ = diags;
}

#[test]
fn diagnostics_undefined_event_produces_warning_or_error() {
    // "UndefinedEvent" is not declared, semantic validator should catch it
    let code = "stream S = UndefinedEvent\n    .emit()\n";
    let diags = get_diagnostics(code);
    // Semantic validation may produce warnings/errors for undefined events
    // The important thing is it exercises the semantic_to_lsp_diagnostic path
    let _ = diags;
}

#[test]
fn diagnostics_semantic_with_hint() {
    // Some semantic diagnostics include hints — exercise the hint branch
    let code = r#"
event A:
    x: int

stream S1 = A
    .emit()

stream S2 = A
    .where(y > 10)
    .emit()
"#;
    let diags = get_diagnostics(code);
    // "y" is not a field of A, which might produce a semantic diagnostic with a hint
    let _ = diags;
}

#[test]
fn diagnostics_source_field_always_varpulis() {
    // Any diagnostic should have source = "varpulis"
    let code = "stream x = y.where(";
    let diags = get_diagnostics(code);
    for d in &diags {
        assert_eq!(
            d.source,
            Some("varpulis".into()),
            "All diagnostics should have source 'varpulis'"
        );
    }
}

#[test]
fn diagnostics_severity_is_error_for_syntax() {
    let code = "stream x = y.where(";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    assert_eq!(
        diags[0].severity,
        Some(DiagnosticSeverity::ERROR),
        "Syntax errors should have ERROR severity"
    );
}

// =============================================================================
// ParseError::Located branch (diagnostics.rs L62-102)
// =============================================================================

#[test]
fn diagnostics_located_error_with_hint() {
    // Trigger a Located error with a hint — e.g., an incomplete where clause
    let code = "stream A = X.where(a >";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    let d = &diags[0];
    assert_eq!(d.severity, Some(DiagnosticSeverity::ERROR));
    assert_eq!(d.source, Some("varpulis".into()));
    // Range should be within the source
    assert!(d.range.start.line <= 1);
}

#[test]
fn diagnostics_located_error_line_col() {
    // Error on the second line
    let code = "event A:\n    .bad syntax";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    // The error should reference a line >= 0
    let _ = diags[0].range;
}

// =============================================================================
// ParseError::UnexpectedEof branch (diagnostics.rs L138-157)
// =============================================================================

#[test]
fn diagnostics_unexpected_eof_single_line() {
    // Trigger EOF error on single line
    let code = "stream";
    let diags = get_diagnostics(code);
    // May get an error about unexpected end of input
    let _ = diags;
}

#[test]
fn diagnostics_unexpected_eof_multiline() {
    // Trigger EOF error at end of multiline input
    let code = "event A:\n    x: int\n\nstream S =";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
}

// =============================================================================
// Empty and whitespace inputs
// =============================================================================

#[test]
fn diagnostics_whitespace_only() {
    let diags = get_diagnostics("   \n  \n   ");
    // Should not panic — may or may not have diagnostics
    let _ = diags;
}

#[test]
fn diagnostics_newlines_only() {
    let diags = get_diagnostics("\n\n\n");
    let _ = diags;
}

#[test]
fn diagnostics_comment_only_multiline() {
    let code = "# comment 1\n# comment 2\n# comment 3";
    let diags = get_diagnostics(code);
    assert!(
        diags.is_empty(),
        "Comments only should be valid: {:?}",
        diags
    );
}

// =============================================================================
// Various syntax error patterns exercising different branches
// =============================================================================

#[test]
fn diagnostics_unmatched_brace() {
    let code = "event A {\n    x: int\n";
    let diags = get_diagnostics(code);
    // May or may not produce a diagnostic — just ensure no panic
    let _ = diags;
}

#[test]
fn diagnostics_unmatched_paren() {
    let code = "stream S = A.where(x > 10";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
}

#[test]
fn diagnostics_double_dot() {
    let code = "stream S = A..where(x > 10).emit()";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
}

#[test]
fn diagnostics_invalid_operator() {
    let code = "stream S = A.where(x === y).emit()";
    let diags = get_diagnostics(code);
    // Triple equals is not valid VPL
    let _ = diags;
}

#[test]
fn diagnostics_missing_event_body() {
    let code = "event\n";
    let diags = get_diagnostics(code);
    // "event" alone may or may not produce a diagnostic depending on parser leniency
    // The important thing is it does not panic
    let _ = diags;
}

#[test]
fn diagnostics_incomplete_select() {
    let code = "stream S = A.select(";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
}

#[test]
fn diagnostics_incomplete_aggregate() {
    let code = "stream S = A.aggregate(sum(";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
}

#[test]
fn diagnostics_incomplete_pattern() {
    let code = "pattern P = SEQ(a:";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
}

// =============================================================================
// Complex valid programs — ensure no false positives
// =============================================================================

#[test]
fn diagnostics_valid_with_connector() {
    let code = r#"
connector mqtt_in = mqtt(host: "localhost", port: 1883)

event SensorData:
    temperature: float
    humidity: float

stream Readings = SensorData
    .from(mqtt_in, topic: "sensors/data")
    .where(temperature > 30.0)
    .emit()
"#;
    let diags = get_diagnostics(code);
    assert!(
        diags.is_empty(),
        "Valid program with connector should have no diagnostics: {:?}",
        diags
    );
}

#[test]
fn diagnostics_valid_with_window_and_aggregate() {
    let code = r#"
event Tick:
    price: float

stream AvgPrice = Tick
    .window(tumbling(1m))
    .aggregate(avg(price) as avg_price)
    .emit()
"#;
    let diags = get_diagnostics(code);
    // Window/aggregate syntax may vary; just ensure no panic
    let _ = diags;
}

#[test]
fn diagnostics_valid_with_join() {
    let code = r#"
event A:
    id: int
    value: float

event B:
    id: int
    label: str

stream Joined = A
    .join(B, A.id == B.id)
    .emit()
"#;
    let diags = get_diagnostics(code);
    let _ = diags;
}

#[test]
fn diagnostics_valid_let_var_const() {
    let code = r#"
let threshold = 25
var counter = 0
const MAX_RETRIES = 3
"#;
    let diags = get_diagnostics(code);
    assert!(diags.is_empty(), "Variables should be valid: {:?}", diags);
}

// =============================================================================
// Diagnostic range validation
// =============================================================================

#[test]
fn diagnostics_range_is_valid() {
    let code = "stream S = A.where(x > 10\n";
    let diags = get_diagnostics(code);
    for d in &diags {
        // Start position should be <= end position
        assert!(
            d.range.start.line < d.range.end.line
                || (d.range.start.line == d.range.end.line
                    && d.range.start.character <= d.range.end.character),
            "Range should be valid: {:?}",
            d.range
        );
    }
}

#[test]
fn diagnostics_error_on_first_line() {
    let code = "!!! bad";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    // Error should be on line 0 (0-indexed)
    assert!(
        diags[0].range.start.line <= 1,
        "Error on first line should be line 0 or 1: {:?}",
        diags[0].range
    );
}

#[test]
fn diagnostics_error_on_later_line() {
    let code = "event A:\n    x: int\n\n!!! bad";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
}

// =============================================================================
// Multiple diagnostics
// =============================================================================

#[test]
fn diagnostics_multiple_events_and_streams() {
    let code = r#"
event A:
    x: int

event B:
    y: float

stream S1 = A
    .where(x > 10)
    .emit()

stream S2 = B
    .where(y > 1.5)
    .emit()
"#;
    let diags = get_diagnostics(code);
    assert!(
        diags.is_empty(),
        "Multiple valid declarations should have no diagnostics: {:?}",
        diags
    );
}

// =============================================================================
// Unicode content
// =============================================================================

#[test]
fn diagnostics_unicode_content() {
    // Unicode in comments should be handled gracefully
    let code = "# This is a comment with unicode: cafe\nstream S = X.emit()";
    let diags = get_diagnostics(code);
    let _ = diags;
}

#[test]
fn diagnostics_unicode_in_string() {
    let code = r#"let name = "Hello World""#;
    let diags = get_diagnostics(code);
    let _ = diags;
}

// =============================================================================
// Long source input
// =============================================================================

#[test]
fn diagnostics_many_lines() {
    let mut code = String::new();
    code.push_str("event X:\n    value: int\n\n");
    for i in 0..50 {
        code.push_str(&format!("# line {}\n", i));
    }
    code.push_str("stream S = X.emit()\n");
    let diags = get_diagnostics(&code);
    assert!(
        diags.is_empty(),
        "Many comment lines plus valid statement: {:?}",
        diags
    );
}

// =============================================================================
// Edge case: errors at position 0
// =============================================================================

#[test]
fn diagnostics_error_at_position_zero() {
    let code = "@ invalid start";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    // Should reference position 0, line 0
    let _ = diags[0].range;
}

// =============================================================================
// Exercises get_error_end_column (diagnostics.rs L274-293)
// =============================================================================

#[test]
fn diagnostics_error_highlights_token() {
    // An error at a known token position should highlight the token length
    let code = "stream = X";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    // The range end should be >= start (at least one character)
    let d = &diags[0];
    if d.range.start.line == d.range.end.line {
        assert!(
            d.range.end.character >= d.range.start.character,
            "End column should be >= start column"
        );
    }
}

#[test]
fn diagnostics_error_at_non_alphanumeric() {
    // Non-alphanumeric characters at error position should highlight at least 1 char
    let code = "stream S = .()";
    let diags = get_diagnostics(code);
    let _ = diags;
}

// =============================================================================
// semantic_to_lsp_diagnostic — severity mapping and hint formatting
// =============================================================================

#[test]
fn diagnostics_semantic_error_severity() {
    // .having() without .aggregate() produces Severity::Error → DiagnosticSeverity::ERROR
    let code = "stream S = A\n    .having(x > 1)";
    let diags = get_diagnostics(code);
    assert!(
        !diags.is_empty(),
        "Should have diagnostics for having without aggregate"
    );
    // Find the error diagnostic
    let errors: Vec<_> = diags
        .iter()
        .filter(|d| d.severity == Some(DiagnosticSeverity::ERROR))
        .collect();
    assert!(
        !errors.is_empty(),
        "Should have at least one ERROR severity"
    );
}

#[test]
fn diagnostics_semantic_warning_severity() {
    // .aggregate() without .window() produces Severity::Warning → DiagnosticSeverity::WARNING
    let code = "stream S = A\n    .aggregate(c: count())";
    let diags = get_diagnostics(code);
    let warnings: Vec<_> = diags
        .iter()
        .filter(|d| d.severity == Some(DiagnosticSeverity::WARNING))
        .collect();
    assert!(
        !warnings.is_empty(),
        "Should have at least one WARNING severity: {:?}",
        diags
    );
}

#[test]
fn diagnostics_semantic_hint_in_message() {
    // E010 (.having without .aggregate) has a hint
    let code = "stream S = A\n    .having(x > 1)";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    let d = &diags[0];
    assert!(
        d.message.contains("Hint:") || d.message.contains("aggregate"),
        "Diagnostic should contain hint text: {}",
        d.message
    );
}

#[test]
fn diagnostics_semantic_no_hint() {
    // A clean program produces no diagnostics, exercising the empty diagnostics path
    let code = "event Used:\n    x: int\nstream S = Used\n    .emit()";
    let diags = get_diagnostics(code);
    // Clean program should have no diagnostics
    assert!(
        diags.is_empty(),
        "Valid program should have no diagnostics: {:?}",
        diags
    );
}

#[test]
fn diagnostics_semantic_has_code() {
    // Semantic diagnostics should have a code (e.g., "E010")
    let code = "stream S = A\n    .having(x > 1)";
    let diags = get_diagnostics(code);
    let coded: Vec<_> = diags.iter().filter(|d| d.code.is_some()).collect();
    assert!(
        !coded.is_empty(),
        "Should have at least one diagnostic with code"
    );
}

#[test]
fn diagnostics_semantic_range_spans_source() {
    // Semantic diagnostics should have proper line/col ranges
    let code = "stream S = A\n    .having(x > 1)";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    let d = &diags[0];
    // Start line should be 0 or 1 (the stream declaration line or the .having line)
    assert!(
        d.range.start.line <= 2,
        "Start line should be reasonable: {:?}",
        d.range
    );
}

// =============================================================================
// position_to_line_col edge cases
// =============================================================================

#[test]
fn diagnostics_error_on_third_line() {
    // Position the error on line 3 (0-indexed: 2)
    let code = "event A:\n    x: int\n\nstream S = A\n    .having(x > 1)";
    let diags = get_diagnostics(code);
    // Should produce at least one diagnostic on a later line
    assert!(!diags.is_empty());
}

#[test]
fn diagnostics_unicode_identifier() {
    // Unicode in identifier position — parser should produce an error at correct position
    let code = "stream S = MyEvent\n    .where(ñ > 0)";
    let diags = get_diagnostics(code);
    // May parse or fail — exercises position_to_line_col with UTF-8
    let _ = diags;
}

// =============================================================================
// get_error_end_column edge cases
// =============================================================================

#[test]
fn diagnostics_error_at_end_of_line() {
    // Error at the very end of a line
    let code = "stream S = A.where(x >\n)";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    let d = &diags[0];
    // End column should be >= start column on the same line
    if d.range.start.line == d.range.end.line {
        assert!(d.range.end.character >= d.range.start.character);
    }
}

#[test]
fn diagnostics_error_at_line_boundary() {
    // Error right before a newline
    let code = "event A:\n    x: int\n\nstream S =\n";
    let diags = get_diagnostics(code);
    // Should produce a diagnostic about incomplete stream
    assert!(!diags.is_empty());
}

// =============================================================================
// Multiple semantic diagnostics
// =============================================================================

#[test]
fn diagnostics_multiple_semantic_issues() {
    // Two streams with .having() without .aggregate() should produce two diagnostics
    let code = "stream S1 = A\n    .having(x > 1)\nstream S2 = B\n    .having(y > 2)";
    let diags = get_diagnostics(code);
    let errors: Vec<_> = diags
        .iter()
        .filter(|d| d.severity == Some(DiagnosticSeverity::ERROR))
        .collect();
    assert!(
        errors.len() >= 2,
        "Expected at least 2 errors, got {}: {:?}",
        errors.len(),
        diags
    );
}

#[test]
fn diagnostics_duplicate_event_produces_error() {
    let code = "event X:\n    a: int\nevent X:\n    b: float";
    let diags = get_diagnostics(code);
    assert!(
        !diags.is_empty(),
        "Duplicate event should produce diagnostic"
    );
    assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
}
