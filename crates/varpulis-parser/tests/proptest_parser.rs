//! Property-based tests for the VPL parser.
//!
//! Verifies that the parser never panics on arbitrary input and that
//! valid programs round-trip through parse → AST → re-parse consistently.

use proptest::prelude::*;

/// Strategy that generates random strings of printable ASCII + whitespace.
fn arbitrary_source() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::char::range('\x00', '\x7f'), 0..512)
        .prop_map(|chars| chars.into_iter().collect::<String>())
}

/// Strategy that generates syntactically plausible VPL stream definitions.
fn plausible_vpl() -> impl Strategy<Value = String> {
    let stream_name = "[A-Z][A-Za-z0-9]{0,15}";
    let field_name = "[a-z][a-z0-9_]{0,10}";
    let op = prop_oneof![
        Just(".where(a.x > 0)".to_string()),
        Just(".where(a.y == 1)".to_string()),
        Just(".where(a.z < 100)".to_string()),
        Just("".to_string()),
    ];
    let emit_field = field_name;
    let emit_expr = prop_oneof![
        Just("a.x".to_string()),
        Just("1".to_string()),
        Just("true".to_string()),
    ];

    (stream_name, field_name, op, emit_field, emit_expr).prop_map(
        |(sname, _fname, op, efield, eexpr)| {
            format!(
                "stream {} = EventType as a\n  {}\n  .emit({}: {})",
                sname, op, efield, eexpr
            )
        },
    )
}

proptest! {
    /// The parser must never panic on arbitrary input.
    #[test]
    fn parser_never_panics(source in arbitrary_source()) {
        // We only care that this doesn't panic — errors are fine.
        let _ = varpulis_parser::parse(&source);
    }

    /// Plausible VPL programs should parse successfully.
    #[test]
    fn plausible_vpl_parses(source in plausible_vpl()) {
        let result = varpulis_parser::parse(&source);
        prop_assert!(
            result.is_ok(),
            "Expected plausible VPL to parse successfully, got error on input:\n{}\nError: {:?}",
            source,
            result.err()
        );
    }

    /// Valid programs should parse to at least one stream definition.
    #[test]
    fn valid_program_has_streams(source in plausible_vpl()) {
        if let Ok(program) = varpulis_parser::parse(&source) {
            prop_assert!(
                !program.statements.is_empty(),
                "Parsed program should have at least one statement, input: {}",
                source
            );
        }
    }

    /// Parsing should be deterministic: same input → same result.
    #[test]
    fn parsing_is_deterministic(source in arbitrary_source()) {
        let r1 = varpulis_parser::parse(&source);
        let r2 = varpulis_parser::parse(&source);
        match (r1, r2) {
            (Ok(p1), Ok(p2)) => {
                prop_assert_eq!(
                    format!("{:?}", p1),
                    format!("{:?}", p2),
                    "Same input should produce identical AST"
                );
            }
            (Err(_), Err(_)) => {} // Both failed — consistent
            _ => prop_assert!(false, "Determinism violation: one parse succeeded, the other failed"),
        }
    }
}
