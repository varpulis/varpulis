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

stream TempStream from Temperature
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
    let mut nested = "stream X = Event.where(".to_string();
    for _ in 0..50 {
        nested.push_str("(value + ");
    }
    nested.push_str("1");
    for _ in 0..50 {
        nested.push(')');
    }
    nested.push(')');

    // Should either parse or return error, not overflow
    let result = std::panic::catch_unwind(|| parse(&nested));
    assert!(result.is_ok(), "Parser should not overflow on nested input");
}
