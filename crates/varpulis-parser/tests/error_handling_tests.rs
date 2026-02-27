//! Tests for parser error handling

use varpulis_parser::parse;

#[test]
fn test_invalid_syntax_returns_error_not_panic() {
    // These should return errors, not panic
    let invalid_inputs = [
        "stream = ",                    // incomplete stream
        "stream X = Y.window(",         // unclosed paren
        "stream X = Event.where(a ==)", // incomplete comparison
    ];

    for input in &invalid_inputs {
        let result = parse(input);
        assert!(
            result.is_err(),
            "Should return error for invalid input: {:?}",
            input
        );
    }
}

#[test]
fn test_incomplete_stream_descriptive_error() {
    let input = "stream MyStream = ";
    let result = parse(input);
    assert!(result.is_err());

    let err = result.unwrap_err();
    // Error should mention what was expected
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("Expected")
            || err_msg.contains("expected")
            || err_msg.contains("Unexpected"),
        "Error should be descriptive: {}",
        err_msg
    );
}

#[test]
fn test_missing_field_type_error_with_location() {
    let input = r#"
event BadEvent:
    name:
    value: float
"#;
    let result = parse(input);
    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    // Error should include line/column information
    assert!(
        err_msg.contains("line") || err_msg.contains("3") || err_msg.contains("column"),
        "Error should include location: {}",
        err_msg
    );
}

#[test]
fn test_unclosed_block_error() {
    let input = r#"
stream X = Event
    .where(value > 0
    .emit(event_type: "Alert")
"#;
    let result = parse(input);
    assert!(result.is_err());

    let err = result.unwrap_err();
    // Should not panic but return a descriptive error
    let err_msg = err.to_string();
    assert!(!err_msg.is_empty(), "Error message should not be empty");
}

#[test]
fn test_fuzz_input_no_panic() {
    // Random/garbage input should not cause panics
    let fuzz_inputs = [
        "",                             // empty
        "   ",                          // whitespace only
        "123456",                       // just numbers
        "!@#$%^&*()",                   // special characters
        "stream",                       // keyword only
        "event",                        // keyword only
        "where where where",            // repeated keywords
        "...",                          // dots
        "->->->",                       // arrows
        "== != <= >= < >",              // operators only
        "((((()))))",                   // unbalanced/nested parens
        "stream X = event where where", // malformed
        "\0\0\0",                       // null bytes
        "stream\nstream\nstream",       // repeated keyword lines
    ];

    for input in &fuzz_inputs {
        // Should not panic - either parse successfully or return error
        let result = std::panic::catch_unwind(|| parse(input));
        assert!(result.is_ok(), "Parser panicked on input: {:?}", input);
    }
}

#[test]
fn test_unicode_input_no_panic() {
    // Unicode and non-ASCII input should not cause panics
    let unicode_inputs = [
        "stream 日本語 = Event",     // Japanese
        "event Événement: nom: str", // French accents
        "stream 🚀 = Data",          // Emoji
        "# Comment with émojis 🎉",  // Emoji in comment
    ];

    for input in &unicode_inputs {
        let result = std::panic::catch_unwind(|| parse(input));
        assert!(
            result.is_ok(),
            "Parser panicked on unicode input: {:?}",
            input
        );
    }
}

#[test]
fn test_valid_syntax_still_works() {
    // Ensure valid syntax still parses correctly after error handling changes
    let valid_inputs = [
        r#"
event Temperature:
    sensor_id: str
    value: float

stream TempStream = Temperature
"#,
        r#"
stream Filtered = SomeEvent
    .where(value > 100)
    .emit(event_type: "Alert")
"#,
        r#"
# A comment
event Simple:
    data: str
"#,
    ];

    for input in &valid_inputs {
        let result = parse(input);
        assert!(
            result.is_ok(),
            "Valid syntax should parse: {:?}, error: {:?}",
            input,
            result.err()
        );
    }
}

#[test]
fn test_deeply_nested_expression_no_overflow() {
    // Deeply nested expressions should not cause stack overflow
    // Note: Using 15 levels (under the 16-level nesting limit) to test deep but valid parsing
    let mut nested = "stream X = Event.where(".to_string();
    for _ in 0..14 {
        nested.push_str("(value + ");
    }
    nested.push('1');
    for _ in 0..14 {
        nested.push(')');
    }
    nested.push(')');

    // Should either parse or return error, not overflow
    let result = std::panic::catch_unwind(|| parse(&nested));
    assert!(result.is_ok(), "Parser should not overflow on nested input");
}

/// Regression test for fuzzer-discovered timeout (run 22473258567).
/// Deeply nested unclosed brackets with `if`/`[` cause O(k^depth) backtracking
/// in pest. The nesting depth pre-scan must reject these in O(n) time.
#[test]
fn test_fuzz_timeout_regression_nested_unclosed_brackets() {
    use std::time::Instant;

    // Exact input from fuzzer timeout-91f24904a035f087af20e69f70df9309c5ca6c74
    let timeout_input = "( ifg\n[ifg\n[if( ifq\n[ifg\n[if( ifg\n[ifg\n[ifg\n[\n[ifg\n[if( ifq\n[ifg\n[if( ifg\n[ifg\n[ifg\n[igfigfg u";

    // Exact input from fuzzer slow-unit-ea2c9b801d6f18edd355d087fd38055e97e4c5e7
    let slow_input = "conr(ififg\n(2 [ifgr(ififg\n(2 [ifga(ififg\n(2[ ifg\n [ifgr(ififg\n(2 [ifga(ififg\n(2[ ifg\nsa(ififg\n(2[ sa(ififg\n(2[ ifg\ns";

    for (label, input) in [("timeout", timeout_input), ("slow-unit", slow_input)] {
        let start = Instant::now();
        let result = parse(input);
        let elapsed = start.elapsed();

        // Must be rejected (nesting depth exceeded)
        assert!(result.is_err(), "{}: should be rejected", label);

        // Must complete in <1 second (was >1200s before fix)
        assert!(
            elapsed.as_secs() < 1,
            "{}: took {:?}, should be rejected instantly by nesting check",
            label,
            elapsed
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Nesting depth"),
            "{}: error should mention nesting depth, got: {}",
            label,
            err_msg
        );
    }
}
