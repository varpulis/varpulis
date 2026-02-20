//! Integration tests for VPL LSP coverage
//!
//! Targets: hover, semantic tokens, diagnostics, completion

use tower_lsp::lsp_types::*;
use varpulis_lsp::completion::get_completions;
use varpulis_lsp::diagnostics::get_diagnostics;
use varpulis_lsp::hover::get_hover;
use varpulis_lsp::semantic::{get_document_symbols, get_semantic_tokens};

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn pos(line: u32, character: u32) -> Position {
    Position { line, character }
}

/// Returns the hover markdown content at the given position, or None.
fn hover_text(source: &str, line: u32, col: u32) -> Option<String> {
    get_hover(source, pos(line, col)).map(|h| match h.contents {
        HoverContents::Markup(m) => m.value,
        _ => String::new(),
    })
}

// ===========================================================================
// 1. HOVER PROVIDER (target: hover.rs)
// ===========================================================================

#[test]
fn hover_keyword_stream() {
    let text = "stream SensorData from \"mqtt://localhost\"";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("stream"));
    assert!(h.contains("Declares a data stream"));
}

#[test]
fn hover_keyword_event() {
    let text = "event TempReading { value: float }";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("event"));
    assert!(h.contains("Declares an event type"));
}

#[test]
fn hover_keyword_pattern() {
    let text = "pattern Alert = SEQ(a: Warning, b: Error) within 5m";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("pattern"));
    assert!(h.contains("SASE+"));
}

#[test]
fn hover_keyword_from() {
    let text = "from connector";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("from"));
}

#[test]
fn hover_keyword_if() {
    let text = "if condition then expr";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("if"));
    assert!(h.contains("Conditional"));
}

#[test]
fn hover_keyword_match() {
    let text = "match value { }";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("match"));
    assert!(h.contains("Pattern matching"));
}

#[test]
fn hover_keyword_let() {
    let text = "let x = 5";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("let"));
    assert!(h.contains("immutable"));
}

#[test]
fn hover_keyword_var() {
    let text = "var count = 0";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("var"));
    assert!(h.contains("mutable"));
}

#[test]
fn hover_keyword_const() {
    let text = "const MAX = 100";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("const"));
    assert!(h.contains("compile-time"));
}

#[test]
fn hover_keyword_fn() {
    let text = "fn process(x: int) -> int { x }";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("fn"));
    assert!(h.contains("Declares a function"));
}

#[test]
fn hover_stream_ops_where() {
    let text = "where condition";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("where"));
    assert!(h.contains("Filter"));
}

#[test]
fn hover_stream_ops_select() {
    let text = "select expr";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("select"));
    assert!(h.contains("Projects"));
}

#[test]
fn hover_stream_ops_aggregate() {
    let text = "aggregate something";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("aggregate"));
    assert!(h.contains("Aggregate"));
}

#[test]
fn hover_stream_ops_window() {
    let text = "window something";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("window"));
    assert!(h.contains("Window types"));
}

#[test]
fn hover_stream_ops_emit() {
    let text = "emit something";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("emit"));
    assert!(h.contains("Output"));
}

#[test]
fn hover_stream_ops_to() {
    let text = "to sink";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("to"));
    assert!(h.contains("sink"));
}

#[test]
fn hover_stream_ops_forecast() {
    let text = "forecast params";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("forecast"));
    assert!(h.contains("forecasting"));
}

#[test]
fn hover_stream_ops_enrich() {
    let text = "enrich connector";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("enrich"));
    assert!(h.contains("Enriches"));
}

#[test]
fn hover_stream_ops_partition_by() {
    let text = "partition_by key";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("partition_by"));
    assert!(h.contains("Partitions"));
}

#[test]
fn hover_stream_ops_join() {
    let text = "join other";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("join"));
    assert!(h.contains("Joins two streams"));
}

#[test]
fn hover_sase_seq() {
    let text = "SEQ(a: EventA)";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("SEQ"));
    assert!(h.contains("Sequence"));
}

#[test]
fn hover_sase_and() {
    let text = "AND(a: EventA, b: EventB)";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("AND"));
    assert!(h.contains("Conjunction"));
}

#[test]
fn hover_sase_or() {
    let text = "OR(a: EventA, b: EventB)";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("OR"));
    assert!(h.contains("Disjunction"));
}

#[test]
fn hover_sase_not() {
    let text = "NOT(EventB)";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("NOT"));
    assert!(h.contains("Negation"));
}

#[test]
fn hover_within() {
    let text = "within 5m";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("within"));
    assert!(h.contains("time window"));
}

#[test]
fn hover_agg_functions() {
    for (name, expected) in [
        ("sum", "sum"),
        ("avg", "average"),
        ("count", "count"),
        ("min", "minimum"),
        ("max", "maximum"),
        ("stddev", "standard deviation"),
        ("variance", "variance"),
        ("first", "first"),
        ("last", "last"),
        ("collect", "collects"),
        ("distinct", "distinct"),
    ] {
        let text = format!("{} something", name);
        let h =
            hover_text(&text, 0, 0).unwrap_or_else(|| panic!("hover should exist for '{}'", name));
        assert!(
            h.to_lowercase().contains(expected),
            "hover for '{}' should contain '{}', got: {}",
            name,
            expected,
            h
        );
    }
}

#[test]
fn hover_window_types() {
    for (name, expected) in [
        ("tumbling", "non-overlapping"),
        ("sliding", "overlapping"),
        ("session_window", "session"),
    ] {
        let text = format!("{} something", name);
        let h =
            hover_text(&text, 0, 0).unwrap_or_else(|| panic!("hover should exist for '{}'", name));
        assert!(
            h.to_lowercase().contains(expected),
            "hover for '{}' should contain '{}', got: {}",
            name,
            expected,
            h
        );
    }
}

#[test]
fn hover_builtin_functions() {
    for (name, expected) in [
        ("now", "current timestamp"),
        ("len", "length"),
        ("abs", "absolute"),
        ("sqrt", "square root"),
        ("pow", "power"),
        ("floor", "Rounds down"),
        ("ceil", "Rounds up"),
        ("round", "Rounds"),
    ] {
        let text = format!("{} something", name);
        let h =
            hover_text(&text, 0, 0).unwrap_or_else(|| panic!("hover should exist for '{}'", name));
        assert!(
            h.to_lowercase().contains(&expected.to_lowercase()),
            "hover for '{}' should contain '{}', got: {}",
            name,
            expected,
            h
        );
    }
}

#[test]
fn hover_types() {
    for (name, expected) in [
        ("int", "integer"),
        ("float", "floating-point"),
        ("bool", "Boolean"),
        ("str", "String type"),
        ("timestamp", "Timestamp"),
        ("duration", "Duration"),
        ("list", "list"),
        ("map", "map"),
    ] {
        let text = format!("{} something", name);
        let h =
            hover_text(&text, 0, 0).unwrap_or_else(|| panic!("hover should exist for '{}'", name));
        assert!(
            h.contains(expected),
            "hover for '{}' should contain '{}', got: {}",
            name,
            expected,
            h
        );
    }
}

#[test]
fn hover_stream_type() {
    let text = "Stream something";
    let h = hover_text(text, 0, 0).unwrap();
    assert!(h.contains("Stream<T>"));
}

#[test]
fn hover_collection_functions() {
    for (name, expected) in [
        ("filter", "Filters a collection"),
        ("reduce", "Reduces a collection"),
    ] {
        let text = format!("{} something", name);
        let h =
            hover_text(&text, 0, 0).unwrap_or_else(|| panic!("hover should exist for '{}'", name));
        assert!(
            h.contains(expected),
            "hover for '{}' should contain '{}', got: {}",
            name,
            expected,
            h
        );
    }
}

#[test]
fn hover_logical_operators() {
    for (name, expected) in [
        ("and", "Logical AND"),
        ("or", "Logical OR"),
        ("not", "Logical NOT"),
    ] {
        let text = format!("{} something", name);
        let h =
            hover_text(&text, 0, 0).unwrap_or_else(|| panic!("hover should exist for '{}'", name));
        assert!(
            h.contains(expected),
            "hover for '{}' should contain '{}', got: {}",
            name,
            expected,
            h
        );
    }
}

#[test]
fn hover_literals() {
    assert!(hover_text("true", 0, 0).unwrap().contains("true"));
    assert!(hover_text("false", 0, 0).unwrap().contains("false"));
    assert!(hover_text("null", 0, 0).unwrap().contains("Null"));
}

#[test]
fn hover_no_result_for_unknown_word() {
    let text = "foobar something";
    assert!(hover_text(text, 0, 0).is_none());
}

#[test]
fn hover_no_result_past_end_of_line() {
    let text = "stream X";
    // column past end of line
    assert!(hover_text(text, 0, 100).is_none());
}

#[test]
fn hover_no_result_on_empty_line() {
    let text = "   ";
    assert!(hover_text(text, 0, 0).is_none());
}

#[test]
fn hover_on_second_line() {
    let text = "# comment\nstream X";
    let h = hover_text(text, 1, 0).unwrap();
    assert!(h.contains("stream"));
}

#[test]
fn hover_word_in_middle_of_line() {
    let text = "let x = stream Y";
    // Position on "stream"
    let h = hover_text(text, 0, 9).unwrap();
    assert!(h.contains("stream"));
}

#[test]
fn hover_word_boundary_at_space_returns_previous_word() {
    let text = "stream X";
    // Position 6 is the space, but get_word_at_position scans backwards
    // and finds "stream" (chars[0..6]). This is expected behavior.
    let h = hover_text(text, 0, 6);
    assert!(h.is_some());
    assert!(h.unwrap().contains("stream"));
}

#[test]
fn hover_multiline_at_various_positions() {
    let text = "event Foo {\n    let bar = 5\n    var baz = 10\n}";
    assert!(hover_text(text, 0, 0).unwrap().contains("event"));
    assert!(hover_text(text, 1, 4).unwrap().contains("let"));
    assert!(hover_text(text, 2, 4).unwrap().contains("var"));
}

// ===========================================================================
// 2. SEMANTIC TOKENS (target: semantic.rs)
// ===========================================================================

#[test]
fn semantic_tokens_keyword() {
    let tokens = get_semantic_tokens("stream X");
    assert!(!tokens.is_empty());
    // "stream" should be token type 0 (KEYWORD)
    assert_eq!(tokens[0].token_type, 0);
    assert_eq!(tokens[0].length, 6);
}

#[test]
fn semantic_tokens_comment() {
    let tokens = get_semantic_tokens("# this is a comment");
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].token_type, 7); // TOKEN_COMMENT
}

#[test]
fn semantic_tokens_string_double_quote() {
    let tokens = get_semantic_tokens("let x = \"hello\"");
    // Should contain a string token (type 4)
    let string_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 4).collect();
    assert!(!string_tokens.is_empty());
}

#[test]
fn semantic_tokens_string_single_quote() {
    let tokens = get_semantic_tokens("let x = 'hello'");
    let string_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 4).collect();
    assert!(!string_tokens.is_empty());
}

#[test]
fn semantic_tokens_number_integer() {
    let tokens = get_semantic_tokens("let x = 42");
    let num_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 5).collect();
    assert!(!num_tokens.is_empty());
    assert_eq!(num_tokens[0].length, 2); // "42"
}

#[test]
fn semantic_tokens_number_float() {
    let tokens = get_semantic_tokens("let x = 3.14");
    let num_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 5).collect();
    assert!(!num_tokens.is_empty());
    assert_eq!(num_tokens[0].length, 4); // "3.14"
}

#[test]
fn semantic_tokens_duration() {
    let tokens = get_semantic_tokens("let x = 5m");
    let num_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 5).collect();
    assert!(!num_tokens.is_empty());
    assert_eq!(num_tokens[0].length, 2); // "5m"
}

#[test]
fn semantic_tokens_duration_ms() {
    let tokens = get_semantic_tokens("let x = 100ms");
    let num_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 5).collect();
    assert!(!num_tokens.is_empty());
    assert_eq!(num_tokens[0].length, 5); // "100ms"
}

#[test]
fn semantic_tokens_timestamp_literal() {
    let tokens = get_semantic_tokens("let x = @2024-01-01T12:00:00Z");
    let num_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 5).collect();
    assert!(!num_tokens.is_empty());
}

#[test]
fn semantic_tokens_type_names() {
    for ty in [
        "int",
        "float",
        "bool",
        "str",
        "timestamp",
        "duration",
        "list",
        "map",
        "any",
    ] {
        let text = format!("let x: {} = 0", ty);
        let tokens = get_semantic_tokens(&text);
        let type_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 1).collect();
        assert!(!type_tokens.is_empty(), "type token expected for '{}'", ty);
    }
}

#[test]
fn semantic_tokens_builtin_function_call() {
    let tokens = get_semantic_tokens("let x = sum(y)");
    let func_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 2).collect();
    assert!(
        !func_tokens.is_empty(),
        "expected function token for sum(y)"
    );
}

#[test]
fn semantic_tokens_function_needs_paren() {
    // "sum" without parens should NOT be a function token
    let tokens = get_semantic_tokens("let x = sum");
    let func_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 2).collect();
    assert!(
        func_tokens.is_empty(),
        "sum without parens should not be a function token"
    );
}

#[test]
fn semantic_tokens_operator() {
    let tokens = get_semantic_tokens("let x = a => b");
    let op_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 6).collect();
    assert!(!op_tokens.is_empty());
}

#[test]
fn semantic_tokens_pascal_case_class() {
    let tokens = get_semantic_tokens("let x = SensorReading");
    // PascalCase should be token type 8 (TOKEN_CLASS)
    let class_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 8).collect();
    assert!(!class_tokens.is_empty());
}

#[test]
fn semantic_tokens_identifier_variable() {
    let tokens = get_semantic_tokens("let my_var = 5");
    // "my_var" should be token type 3 (TOKEN_VARIABLE)
    let var_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 3).collect();
    assert!(!var_tokens.is_empty());
}

#[test]
fn semantic_tokens_multiline() {
    let text = "stream X\nlet y = 5\n# comment";
    let tokens = get_semantic_tokens(text);
    // Should have tokens across multiple lines
    assert!(tokens.len() >= 3);
    // Check delta_line for tokens on different lines
    let mut total_line = 0u32;
    for t in &tokens {
        total_line += t.delta_line;
    }
    assert!(total_line >= 2, "should span at least 3 lines");
}

#[test]
fn semantic_tokens_empty_input() {
    let tokens = get_semantic_tokens("");
    assert!(tokens.is_empty());
}

#[test]
fn semantic_tokens_whitespace_only() {
    let tokens = get_semantic_tokens("   \n   \n   ");
    assert!(tokens.is_empty());
}

#[test]
fn semantic_tokens_all_keywords() {
    for kw in [
        "stream", "event", "pattern", "from", "let", "var", "const", "fn", "config", "if", "elif",
        "else", "then", "match", "for", "while", "in", "break", "continue", "return", "and", "or",
        "not", "true", "false", "null", "within", "SEQ", "AND", "OR", "NOT",
    ] {
        let text = format!("{} something", kw);
        let tokens = get_semantic_tokens(&text);
        assert!(
            !tokens.is_empty(),
            "should have tokens for keyword '{}'",
            kw
        );
        assert_eq!(
            tokens[0].token_type, 0,
            "keyword '{}' should be token type 0 (KEYWORD)",
            kw
        );
    }
}

#[test]
fn semantic_tokens_multiple_operators() {
    let text = "a == b != c <= d >= e";
    let tokens = get_semantic_tokens(text);
    let op_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 6).collect();
    assert_eq!(
        op_tokens.len(),
        4,
        "should have 4 operator tokens (==, !=, <=, >=)"
    );
}

#[test]
fn semantic_tokens_arrow_operators() {
    let text = "a => b -> c";
    let tokens = get_semantic_tokens(text);
    let op_tokens: Vec<_> = tokens.iter().filter(|t| t.token_type == 6).collect();
    assert_eq!(op_tokens.len(), 2, "should have 2 operator tokens (=>, ->)");
}

// -- Document symbols -------------------------------------------------------

#[test]
fn document_symbols_stream() {
    let text = "stream SensorData = X.emit()";
    let symbols = get_document_symbols(text);
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0].name, "SensorData");
    assert_eq!(symbols[0].kind, SymbolKind::VARIABLE);
}

#[test]
fn document_symbols_event() {
    let text = "event TempReading { value: float }";
    let symbols = get_document_symbols(text);
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0].name, "TempReading");
    assert_eq!(symbols[0].kind, SymbolKind::CLASS);
}

#[test]
fn document_symbols_pattern() {
    let text = "pattern Alert = SEQ(a: Warning) within 5m";
    let symbols = get_document_symbols(text);
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0].name, "Alert");
    assert_eq!(symbols[0].kind, SymbolKind::FUNCTION);
}

#[test]
fn document_symbols_function() {
    let text = "fn process(x: int) -> int { x }";
    let symbols = get_document_symbols(text);
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0].name, "process");
    assert_eq!(symbols[0].kind, SymbolKind::FUNCTION);
}

#[test]
fn document_symbols_let_var_const() {
    let text = "let x = 1\nvar y = 2\nconst Z = 3";
    let symbols = get_document_symbols(text);
    assert_eq!(symbols.len(), 3);
    assert_eq!(symbols[0].name, "x");
    assert_eq!(symbols[1].name, "y");
    assert_eq!(symbols[2].name, "Z");
    for s in &symbols {
        assert_eq!(s.kind, SymbolKind::VARIABLE);
    }
}

#[test]
fn document_symbols_empty() {
    let symbols = get_document_symbols("");
    assert!(symbols.is_empty());
}

#[test]
fn document_symbols_ignores_comments() {
    let text = "# stream NotAStream\nstream RealStream = X";
    let symbols = get_document_symbols(text);
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0].name, "RealStream");
}

#[test]
fn document_symbols_multiple_declarations() {
    let text = r#"
event SensorReading:
    temperature: int

stream SensorData = SensorReading
    .emit()

pattern Alert = SEQ(a: SensorReading) within 5m

fn process(x: int) -> int { x * 2 }

let threshold = 25
"#;
    let symbols = get_document_symbols(text);
    assert_eq!(symbols.len(), 5);
}

// ===========================================================================
// 3. DIAGNOSTICS (target: diagnostics.rs)
// ===========================================================================

#[test]
fn diagnostics_valid_program_no_errors() {
    let code = r#"
event SensorReading:
    temperature: int

stream SensorData = SensorReading
    .where(temperature > 25)
    .emit()
"#;
    let diags = get_diagnostics(code);
    assert!(
        diags.is_empty(),
        "valid code should have no diagnostics, got: {:?}",
        diags
    );
}

#[test]
fn diagnostics_syntax_error() {
    let code = "stream x = y.where(";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
}

#[test]
fn diagnostics_source_is_varpulis() {
    let code = "stream x = y.where(";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    assert_eq!(diags[0].source, Some("varpulis".to_string()));
}

#[test]
fn diagnostics_has_range() {
    let code = "stream x = y.where(";
    let diags = get_diagnostics(code);
    assert!(!diags.is_empty());
    // Diagnostic should have a valid range (non-default)
    let range = diags[0].range;
    // The range should be set (line could be 0 or 1 depending on parser error recovery)
    assert!(
        range.start.line <= 1,
        "error line should be within the source"
    );
}

#[test]
fn diagnostics_empty_input() {
    let diags = get_diagnostics("");
    // Empty input may parse as empty program or give an error
    // Either way, it shouldn't panic
    let _ = diags;
}

#[test]
fn diagnostics_comment_only() {
    let diags = get_diagnostics("# just a comment");
    // Comments alone should be valid
    assert!(
        diags.is_empty(),
        "comment-only should be valid, got: {:?}",
        diags
    );
}

#[test]
fn diagnostics_multiple_valid_statements() {
    let code = r#"
event A:
    x: int

event B:
    y: float

stream S1 = A
    .emit()

stream S2 = B
    .emit()
"#;
    let diags = get_diagnostics(code);
    assert!(
        diags.is_empty(),
        "multiple valid statements should have no diagnostics, got: {:?}",
        diags
    );
}

#[test]
fn diagnostics_semantic_undefined_event() {
    // Use an undefined event type in a stream
    let code = r#"
stream S = UndefinedEvent
    .emit()
"#;
    let diags = get_diagnostics(code);
    // Should have a semantic warning/error about undefined event
    // (depends on the validator's behavior)
    // The important thing is it doesn't panic
    let _ = diags;
}

// ===========================================================================
// 4. COMPLETION (target: completion.rs)
// ===========================================================================

#[test]
fn completion_top_level_empty() {
    let items = get_completions("", pos(0, 0));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    assert!(labels.contains(&"stream"));
    assert!(labels.contains(&"event"));
    assert!(labels.contains(&"pattern"));
    assert!(labels.contains(&"let"));
    assert!(labels.contains(&"var"));
    assert!(labels.contains(&"const"));
    assert!(labels.contains(&"fn"));
    assert!(labels.contains(&"config"));
}

#[test]
fn completion_after_dot() {
    let text = "stream X = Y.";
    let items = get_completions(text, pos(0, 13));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    assert!(labels.contains(&"where"));
    assert!(labels.contains(&"select"));
    assert!(labels.contains(&"aggregate"));
    assert!(labels.contains(&"window"));
    assert!(labels.contains(&"emit"));
    assert!(labels.contains(&"to"));
    assert!(labels.contains(&"join"));
    assert!(labels.contains(&"distinct"));
    assert!(labels.contains(&"limit"));
    assert!(labels.contains(&"order_by"));
    assert!(labels.contains(&"tap"));
    assert!(labels.contains(&"partition_by"));
    assert!(labels.contains(&"forecast"));
    assert!(labels.contains(&"enrich"));
}

#[test]
fn completion_after_at() {
    let text = "let x = @";
    let items = get_completions(text, pos(0, 9));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    assert!(labels.contains(&"timestamp"));
    assert!(labels.contains(&"date"));
}

#[test]
fn completion_in_window() {
    let text = "stream X = Y.window(";
    let items = get_completions(text, pos(0, 20));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    assert!(labels.contains(&"tumbling"));
    assert!(labels.contains(&"sliding"));
    assert!(labels.contains(&"session_window"));
}

#[test]
fn completion_in_aggregate() {
    let text = "stream X = Y.aggregate(";
    let items = get_completions(text, pos(0, 23));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    for func in [
        "sum", "avg", "count", "min", "max", "stddev", "variance", "first", "last", "collect",
        "distinct",
    ] {
        assert!(
            labels.contains(&func),
            "aggregate completions should contain '{}'",
            func
        );
    }
}

#[test]
fn completion_in_pattern() {
    let text = "pattern X = SEQ(";
    let items = get_completions(text, pos(0, 16));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    assert!(labels.contains(&"SEQ"));
    assert!(labels.contains(&"AND"));
    assert!(labels.contains(&"OR"));
    assert!(labels.contains(&"NOT"));
    assert!(labels.contains(&"within"));
}

#[test]
fn completion_type_after_colon() {
    let text = "let x:";
    let items = get_completions(text, pos(0, 6));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    for ty in [
        "int",
        "float",
        "bool",
        "str",
        "timestamp",
        "duration",
        "list",
        "map",
        "any",
        "Stream",
    ] {
        assert!(
            labels.contains(&ty),
            "type completions should contain '{}'",
            ty
        );
    }
}

#[test]
fn completion_expression_context() {
    // Indented line that isn't after a dot/colon/special context
    let text = "event X:\n    something";
    let items = get_completions(text, pos(1, 13));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    assert!(labels.contains(&"if"));
    assert!(labels.contains(&"match"));
    assert!(labels.contains(&"true"));
    assert!(labels.contains(&"false"));
    assert!(labels.contains(&"null"));
    assert!(labels.contains(&"now"));
    assert!(labels.contains(&"len"));
}

#[test]
fn completion_snippet_format() {
    let items = get_completions("", pos(0, 0));
    for item in &items {
        assert_eq!(
            item.insert_text_format,
            Some(InsertTextFormat::SNIPPET),
            "all completion items should use snippet format"
        );
    }
}

#[test]
fn completion_from_params_mqtt() {
    let text = "connector MyMqtt = mqtt(url: \"localhost\")\nstream X = Event.from(MyMqtt, ";
    let items = get_completions(text, pos(1, 30));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    assert!(labels.contains(&"topic"));
    assert!(labels.contains(&"client_id"));
    assert!(labels.contains(&"qos"));
}

#[test]
fn completion_to_params_kafka() {
    let text = "connector K = kafka(url: \"localhost\")\nstream X = Event\n    .to(K, ";
    let items = get_completions(text, pos(2, 10));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    assert!(labels.contains(&"topic"));
    // group_id is source-only
    assert!(!labels.contains(&"group_id"));
}

#[test]
fn completion_unknown_connector_type_empty() {
    let text = "connector C = unknown_type(url: \"x\")\nstream X = Event.from(C, ";
    let items = get_completions(text, pos(1, 25));
    assert!(
        items.is_empty(),
        "unknown connector type should yield no param completions"
    );
}

#[test]
fn completion_from_with_just_connector_name() {
    // Cursor right at the end of connector name (no comma yet).
    // The LSP still provides param completions since the connector is recognized.
    let text = "connector MyMqtt = mqtt(url: \"localhost\")\nstream X = Event.from(MyMqtt";
    let items = get_completions(text, pos(1, 28));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    // The context-aware completion still resolves the connector type
    assert!(labels.contains(&"topic"));
}

#[test]
fn completion_from_closed_paren_no_params() {
    // .from() is closed, cursor after it
    let text = "connector M = mqtt(url: \"x\")\nstream X = Event.from(M).";
    let items = get_completions(text, pos(1, 25));
    let labels: Vec<&str> = items.iter().map(|c| c.label.as_str()).collect();
    // Should be stream operations, not connector params
    assert!(labels.contains(&"where") || labels.contains(&"emit"));
}

// ===========================================================================
// 5. SEMANTIC TOKEN LEGEND (target: semantic.rs statics)
// ===========================================================================

#[test]
fn semantic_legend_has_all_token_types() {
    use varpulis_lsp::semantic::SEMANTIC_TOKEN_TYPES;
    assert!(SEMANTIC_TOKEN_TYPES.len() >= 12);
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::KEYWORD));
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::TYPE));
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::FUNCTION));
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::VARIABLE));
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::STRING));
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::NUMBER));
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::OPERATOR));
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::COMMENT));
    assert!(SEMANTIC_TOKEN_TYPES.contains(&SemanticTokenType::CLASS));
}

#[test]
fn semantic_legend_modifiers_empty() {
    use varpulis_lsp::semantic::SEMANTIC_TOKEN_MODIFIERS;
    assert!(SEMANTIC_TOKEN_MODIFIERS.is_empty());
}

#[test]
fn semantic_legend_struct() {
    use varpulis_lsp::semantic::SEMANTIC_TOKEN_LEGEND;
    assert!(!SEMANTIC_TOKEN_LEGEND.token_types.is_empty());
    assert!(SEMANTIC_TOKEN_LEGEND.token_modifiers.is_empty());
}

// ===========================================================================
// 6. NAVIGATION: GO-TO-DEFINITION (target: navigation.rs)
// ===========================================================================

fn test_uri() -> Url {
    Url::parse("file:///test.vpl").unwrap()
}

#[test]
fn goto_definition_event_type() {
    // Cursor on "Trade" in the stream source should jump to `event Trade`
    let code = "event Trade:\n    price: float\n\nstream Filtered = Trade\n    .where(price > 100)\n    .emit()";
    // "Trade" in "stream Filtered = Trade" is on line 3, col 18
    let loc = varpulis_lsp::navigation::get_definition(code, pos(3, 18), &test_uri());
    assert!(loc.is_some(), "should find definition for event Trade");
    let loc = loc.unwrap();
    // Definition should point to line 0 (where `event Trade:` is)
    assert_eq!(loc.range.start.line, 0, "event Trade is on line 0");
}

#[test]
fn goto_definition_stream_name() {
    // Cursor on "SensorData" used as source in another stream
    let code = "event Sensor:\n    temp: int\n\nstream SensorData = Sensor\n    .emit()\n\nstream Alerts = SensorData\n    .where(temp > 100)\n    .emit()";
    // "SensorData" in "stream Alerts = SensorData" is on line 6, col 16
    let loc = varpulis_lsp::navigation::get_definition(code, pos(6, 16), &test_uri());
    assert!(
        loc.is_some(),
        "should find definition for stream SensorData"
    );
    let loc = loc.unwrap();
    // "stream SensorData = ..." is on line 3
    assert_eq!(loc.range.start.line, 3, "stream SensorData is on line 3");
}

#[test]
fn goto_definition_connector() {
    let code = "connector MyMqtt = mqtt(url: \"localhost\")\n\nevent Sensor:\n    temp: int\n\nstream S = Sensor\n    .to(MyMqtt, topic: \"out\")";
    // "MyMqtt" in ".to(MyMqtt, ...)" is on line 6
    let loc = varpulis_lsp::navigation::get_definition(code, pos(6, 8), &test_uri());
    assert!(loc.is_some(), "should find definition for connector MyMqtt");
    let loc = loc.unwrap();
    assert_eq!(loc.range.start.line, 0, "connector MyMqtt is on line 0");
}

#[test]
fn goto_definition_function() {
    let code = "fn double(x: int) -> int:\n    return x * 2\n\nlet result = double(5)";
    // "double" in "let result = double(5)" is on line 3, col 13
    let loc = varpulis_lsp::navigation::get_definition(code, pos(3, 13), &test_uri());
    assert!(loc.is_some(), "should find definition for function double");
    let loc = loc.unwrap();
    assert_eq!(loc.range.start.line, 0, "fn double is on line 0");
}

#[test]
fn goto_definition_variable() {
    let code = "let threshold = 25\nlet x = threshold";
    // "threshold" on line 1, col 8
    let loc = varpulis_lsp::navigation::get_definition(code, pos(1, 8), &test_uri());
    assert!(
        loc.is_some(),
        "should find definition for variable threshold"
    );
    let loc = loc.unwrap();
    assert_eq!(loc.range.start.line, 0, "let threshold is on line 0");
}

#[test]
fn goto_definition_unknown_returns_none() {
    let code = "event Trade:\n    price: float";
    // "price" is a field, not a symbol; or put cursor on unknown word
    let loc = varpulis_lsp::navigation::get_definition(code, pos(0, 6), &test_uri());
    // "Trade" IS defined, so that finds it. Let's try an unknown word.
    let code2 = "let x = unknown_symbol";
    let loc2 = varpulis_lsp::navigation::get_definition(code2, pos(0, 10), &test_uri());
    assert!(loc2.is_none(), "unknown symbol should return None");
    let _ = loc; // suppress unused warning
}

#[test]
fn goto_definition_on_definition_itself() {
    // Cursor on the name in the declaration itself should still find it
    let code = "event Trade:\n    price: float";
    let loc = varpulis_lsp::navigation::get_definition(code, pos(0, 7), &test_uri());
    assert!(
        loc.is_some(),
        "definition should resolve even at declaration site"
    );
}

#[test]
fn goto_definition_pattern_event_ref() {
    // Cursor on "Warning" in a pattern should jump to the event declaration
    let code = "event Warning:\n    level: int\n\nevent Error:\n    msg: str\n\npattern Alert = SEQ(Warning, Error) within 5m";
    // "Warning" in SEQ on line 6, col 20
    let loc = varpulis_lsp::navigation::get_definition(code, pos(6, 20), &test_uri());
    assert!(
        loc.is_some(),
        "should find definition for Warning event in pattern"
    );
    let loc = loc.unwrap();
    assert_eq!(loc.range.start.line, 0, "event Warning is on line 0");
}

#[test]
fn goto_definition_empty_document() {
    let code = "";
    let loc = varpulis_lsp::navigation::get_definition(code, pos(0, 0), &test_uri());
    assert!(loc.is_none(), "empty document should return None");
}

#[test]
fn goto_definition_parse_error_returns_none() {
    let code = "stream x = y.where(";
    let loc = varpulis_lsp::navigation::get_definition(code, pos(0, 0), &test_uri());
    assert!(loc.is_none(), "parse errors should return None gracefully");
}

// ===========================================================================
// 7. NAVIGATION: FIND REFERENCES (target: navigation.rs)
// ===========================================================================

#[test]
fn references_event_type_in_stream_source() {
    let code = "event Trade:\n    price: float\n\nstream Filtered = Trade\n    .where(price > 100)\n    .emit()";
    // Cursor on "Trade" in "event Trade:" (line 0, col 6)
    let refs = varpulis_lsp::navigation::get_references(code, pos(0, 6), &test_uri());
    assert!(refs.is_some(), "should find references for Trade");
    let refs = refs.unwrap();
    // Should have at least 2: the definition + the use in stream source
    assert!(
        refs.len() >= 2,
        "expected at least 2 references for Trade, got {}",
        refs.len()
    );
}

#[test]
fn references_connector_in_to() {
    let code = "connector MyMqtt = mqtt(url: \"localhost\")\n\nevent Sensor:\n    temp: int\n\nstream S = Sensor\n    .to(MyMqtt, topic: \"out\")";
    // Cursor on "MyMqtt" in connector declaration
    let refs = varpulis_lsp::navigation::get_references(code, pos(0, 10), &test_uri());
    assert!(refs.is_some(), "should find references for MyMqtt");
    let refs = refs.unwrap();
    assert!(
        refs.len() >= 2,
        "expected at least 2 references for MyMqtt, got {}",
        refs.len()
    );
}

#[test]
fn references_stream_used_as_source() {
    let code = "event Sensor:\n    temp: int\n\nstream SensorData = Sensor\n    .emit()\n\nstream Alerts = SensorData\n    .where(temp > 100)\n    .emit()";
    // Cursor on "SensorData" in its declaration (line 3, col 7)
    let refs = varpulis_lsp::navigation::get_references(code, pos(3, 7), &test_uri());
    assert!(refs.is_some(), "should find references for SensorData");
    let refs = refs.unwrap();
    assert!(
        refs.len() >= 2,
        "expected at least 2 references for SensorData (definition + use), got {}",
        refs.len()
    );
}

#[test]
fn references_unknown_symbol_returns_none() {
    let code = "let x = totally_unknown";
    let refs = varpulis_lsp::navigation::get_references(code, pos(0, 10), &test_uri());
    assert!(
        refs.is_none(),
        "unknown symbol should return None for references"
    );
}

#[test]
fn references_empty_document() {
    let code = "";
    let refs = varpulis_lsp::navigation::get_references(code, pos(0, 0), &test_uri());
    assert!(refs.is_none(), "empty document should return None");
}

#[test]
fn references_event_in_pattern() {
    let code = "event Warning:\n    level: int\n\nevent Error:\n    msg: str\n\npattern Alert = SEQ(Warning, Error) within 5m";
    // Cursor on "Warning" in event declaration (line 0, col 6)
    let refs = varpulis_lsp::navigation::get_references(code, pos(0, 6), &test_uri());
    assert!(refs.is_some(), "should find references for Warning");
    let refs = refs.unwrap();
    // Should include: declaration + pattern usage
    assert!(
        refs.len() >= 2,
        "expected at least 2 references for Warning, got {}",
        refs.len()
    );
}

#[test]
fn references_event_in_followed_by() {
    let code = "event A:\n    val: int\n\nevent B:\n    val: int\n\nstream S = A as a -> B as b .within(5m)\n    .emit()";
    // Cursor on "B" in "-> B as b"
    let refs = varpulis_lsp::navigation::get_references(code, pos(6, 21), &test_uri());
    assert!(refs.is_some(), "should find references for event B");
    let refs = refs.unwrap();
    assert!(
        refs.len() >= 2,
        "expected at least 2 references for B (declaration + followed_by), got {}",
        refs.len()
    );
}
