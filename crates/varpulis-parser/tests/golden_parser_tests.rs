//! Golden file snapshot tests for the VPL parser.
//!
//! Each `.vpl` file under `tests/golden/parser/` is parsed and its AST
//! is snapshot-tested using insta.
//!
//! Run `cargo insta review` to interactively approve snapshot changes.

use std::path::Path;

/// Macro to generate one test per parser fixture.
macro_rules! parser_golden_test {
    ($name:ident, $file:expr) => {
        #[test]
        fn $name() {
            let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/golden/parser")
                .join($file);
            let source = std::fs::read_to_string(&fixture_path)
                .unwrap_or_else(|e| panic!("Failed to read {}: {}", fixture_path.display(), e));

            let program = varpulis_parser::parse(&source)
                .unwrap_or_else(|e| panic!("Parse failed for {}: {}", fixture_path.display(), e));

            // Snapshot the statement count and stream names for stable output
            let stream_names: Vec<&str> = program
                .statements
                .iter()
                .filter_map(|s| match &s.node {
                    varpulis_core::Stmt::StreamDecl { name, .. } => Some(name.as_str()),
                    _ => None,
                })
                .collect();

            let fn_names: Vec<&str> = program
                .statements
                .iter()
                .filter_map(|s| match &s.node {
                    varpulis_core::Stmt::FnDecl { name, .. } => Some(name.as_str()),
                    _ => None,
                })
                .collect();

            let summary = serde_json::json!({
                "statement_count": program.statements.len(),
                "streams": stream_names,
                "functions": fn_names,
            });

            insta::assert_yaml_snapshot!(concat!("parser_", $file), summary);
        }
    };
}

parser_golden_test!(golden_parser_simple_filter, "simple_filter.vpl");
parser_golden_test!(golden_parser_function_decl, "function_decl.vpl");
parser_golden_test!(golden_parser_windowed_agg, "windowed_agg.vpl");
