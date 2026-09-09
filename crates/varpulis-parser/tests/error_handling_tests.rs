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
            "Should return error for invalid input: {input:?}"
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
        "Error should be descriptive: {err_msg}"
    );
}

#[test]
fn test_missing_field_type_error_with_location() {
    let input = r"
event BadEvent:
    name:
    value: float
";
    let result = parse(input);
    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    // Error should include line/column information
    assert!(
        err_msg.contains("line") || err_msg.contains('3') || err_msg.contains("column"),
        "Error should include location: {err_msg}"
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
        assert!(result.is_ok(), "Parser panicked on input: {input:?}");
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
            "Parser panicked on unicode input: {input:?}"
        );
    }
}

#[test]
fn test_valid_syntax_still_works() {
    // Ensure valid syntax still parses correctly after error handling changes
    let valid_inputs = [
        r"
event Temperature:
    sensor_id: str
    value: float

stream TempStream = Temperature
",
        r#"
stream Filtered = SomeEvent
    .where(value > 100)
    .emit(event_type: "Alert")
"#,
        r"
# A comment
event Simple:
    data: str
",
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
    // Note: Using 9 levels (under the 10-level nesting limit) to test deep but valid parsing
    let mut nested = "stream X = Event.where(".to_string();
    for _ in 0..8 {
        nested.push_str("(value + ");
    }
    nested.push('1');
    for _ in 0..8 {
        nested.push(')');
    }
    nested.push(')');

    // Should either parse or return error, not overflow
    let result = std::panic::catch_unwind(|| parse(&nested));
    assert!(result.is_ok(), "Parser should not overflow on nested input");
}

/// Regression tests for fuzzer-discovered timeouts.
/// Deeply nested unclosed brackets with `if`/`[` cause O(2.35^depth)
/// backtracking in pest. The nesting depth pre-scan must reject these in O(n).
///
/// Crash inputs from fuzz runs:
/// - run 22473258567: timeout (depth 20, >1200s)
/// - run 22473258567: slow-unit (depth 22)
/// - run 22485603473: timeout (depth 16, 39s with -timeout=30)
#[test]
fn test_fuzz_timeout_regression_nested_unclosed_brackets() {
    use std::time::Instant;

    // Exact input from fuzzer timeout-91f24904a035f087af20e69f70df9309c5ca6c74
    // 20 opening brackets, 0 closers
    let timeout_input = "( ifg\n[ifg\n[if( ifq\n[ifg\n[if( ifg\n[ifg\n[ifg\n[\n[ifg\n[if( ifq\n[ifg\n[if( ifg\n[ifg\n[ifg\n[igfigfg u";

    // Exact input from fuzzer slow-unit-ea2c9b801d6f18edd355d087fd38055e97e4c5e7
    // 22 opening brackets, 0 closers
    let slow_input = "conr(ififg\n(2 [ifgr(ififg\n(2 [ifga(ififg\n(2[ ifg\n [ifgr(ififg\n(2 [ifga(ififg\n(2[ ifg\nsa(ififg\n(2[ sa(ififg\n(2[ ifg\ns";

    // Exact input from fuzzer timeout-a3f48e35955b8e60d081d5b7d1185c316f142ba5
    // 16 opening brackets, 0 closers (hit 39s timeout at depth=16 limit)
    let timeout_input_2 = "ile&ifr0wt&0&it0wt&0%wt&tresa-we[mresa-we[m[rgKeream/w[K[d[Zjjjj[tream   .et   .et(eim[rgKeream/w[K[d[Zjjjj[tream   .et   .et(eim[i&i0wt&0%wt&ifr0wt&tjwhile&ifr0wt&1&it0fr0w&0%wt&i0wt&0%wt&ifr0wt&tjwhile&ifr0wt&1&it0wt&0%wt&ifr0w&v(stuyl andA\0ju";

    for (label, input) in [
        ("timeout-depth20", timeout_input),
        ("slow-unit-depth22", slow_input),
        ("timeout-depth16", timeout_input_2),
    ] {
        let start = Instant::now();
        let result = parse(input);
        let elapsed = start.elapsed();

        // Must be rejected (nesting depth exceeded)
        assert!(result.is_err(), "{label}: should be rejected");

        // Must complete in <1 second (was >1200s / 39s before fix)
        assert!(
            elapsed.as_secs() < 1,
            "{label}: took {elapsed:?}, should be rejected instantly by nesting check"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Nesting depth"),
            "{label}: error should mention nesting depth, got: {err_msg}"
        );
    }
}

/// Keyword recursion must be bounded, not just bracket nesting.
///
/// `if_expr` recurses through `expr` back into `primary_expr` with no bracket,
/// so the bracket-only pre-scan let `if if if ... 1` through. At ~18 pest stack
/// frames per token that overflows the stack, which aborts the process: not a
/// catchable panic, so the parser's own guard thread and 10s timeout cannot
/// intervene. Reachable from two unauthenticated HTTP routes.
///
/// Fail-before: with only bracket counting, this input aborts the test binary.
#[test]
fn keyword_recursion_is_bounded() {
    let bomb = format!(
        "event E:\n    v: int\n\nstream S = E\n    .where({}1)\n",
        "if ".repeat(1000)
    );
    let start = std::time::Instant::now();
    let result = varpulis_parser::parse(&bomb);
    let elapsed = start.elapsed();

    let err = result.expect_err("1000 nested `if` keywords must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("Conditional nesting"),
        "error should name conditional nesting, got: {msg}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "must be rejected by the O(n) pre-scan, took {elapsed:?}"
    );
}

/// The guard must not fire on legitimate code: `if`/`else` pair, so sequential
/// and modestly nested conditionals stay well under the limit, and identifiers
/// that merely start with the keyword are not keywords.
#[test]
fn keyword_recursion_guard_allows_real_programs() {
    let nested = r#"event E:
    v: int

stream S = E
    .emit(r: if v > 10 then "hi" else if v > 5 then "mid" else "lo")
"#;
    assert!(
        varpulis_parser::parse(nested).is_ok(),
        "nested if/else within the limit must parse"
    );

    let wordlike = r"event E:
    notify: int
    elsewhere: int

stream S = E
    .where(notify > 1 and elsewhere < 2)
";
    assert!(
        varpulis_parser::parse(wordlike).is_ok(),
        "identifiers containing `if`/`else` must not count as keywords"
    );
}
