//! Integration tests for compile-time loop expansion

use varpulis_parser::expand::expand_declaration_loops;
use varpulis_parser::parse;

#[test]
fn test_expand_mandelbrot_contexts() {
    let input = r#"for row in 0..4:
    for col in 0..4:
        context t{row}{col}
"#;
    let expanded = expand_declaration_loops(input);
    // Should produce 16 context declarations
    assert_eq!(expanded.matches("context t").count(), 16);
    // Spot-check specific ones
    assert!(expanded.contains("context t00"));
    assert!(expanded.contains("context t33"));
    assert!(expanded.contains("context t12"));
}

#[test]
fn test_expand_mandelbrot_streams() {
    let input = r#"for row in 0..4:
    for col in 0..4:
        stream Tile{row}{col} = ComputeTile{row}{col}
            .context(t{row}{col})
            .process(compute_tile({col} * 250, {row} * 250, 250, 256))
            .to(MqttOut, topic: "mandelbrot/pixels")
"#;
    let expanded = expand_declaration_loops(input);
    // Should produce 16 stream declarations
    assert_eq!(expanded.matches("stream Tile").count(), 16);
    // Spot-check substitutions
    assert!(expanded.contains("stream Tile00 = ComputeTile00"));
    assert!(expanded.contains(".context(t00)"));
    assert!(expanded.contains(".process(compute_tile(0 * 250, 0 * 250, 250, 256))"));
    assert!(expanded.contains("stream Tile33 = ComputeTile33"));
    assert!(expanded.contains(".process(compute_tile(3 * 250, 3 * 250, 250, 256))"));
}

#[test]
fn test_parse_expanded_contexts() {
    // Verify that expanded context declarations actually parse
    let input = r#"for i in 0..3:
    context c{i}
"#;
    let expanded = expand_declaration_loops(input);
    let result = parse(&expanded);
    assert!(
        result.is_ok(),
        "Expanded contexts should parse successfully: {:?}",
        result.err()
    );
    let program = result.unwrap();
    assert_eq!(program.statements.len(), 3);
}

#[test]
fn test_parse_actual_mandelbrot_vpl_file() {
    // Parse the real mandelbrot_parallel.vpl through the full pipeline
    let vpl = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/mandelbrot/web/mandelbrot_parallel.vpl"),
    )
    .expect("Failed to read mandelbrot_parallel.vpl");

    let result = parse(&vpl);
    assert!(
        result.is_ok(),
        "mandelbrot_parallel.vpl should parse successfully: {:?}",
        result.err()
    );

    let program = result.unwrap();

    // Count statement types
    let mut context_count = 0;
    let mut stream_count = 0;
    let mut fn_count = 0;
    let mut connector_count = 0;

    for spanned_stmt in &program.statements {
        match &spanned_stmt.node {
            varpulis_core::ast::Stmt::ContextDecl { .. } => context_count += 1,
            varpulis_core::ast::Stmt::StreamDecl { .. } => stream_count += 1,
            varpulis_core::ast::Stmt::FnDecl { .. } => fn_count += 1,
            varpulis_core::ast::Stmt::ConnectorDecl { .. } => connector_count += 1,
            _ => {}
        }
    }

    assert_eq!(
        context_count, 16,
        "Should have 16 context declarations (4x4 grid)"
    );
    assert_eq!(
        stream_count, 16,
        "Should have 16 stream declarations (4x4 grid)"
    );
    assert_eq!(
        fn_count, 2,
        "Should have 2 functions (mandelbrot + compute_tile)"
    );
    assert_eq!(connector_count, 1, "Should have 1 connector (MqttOut)");
}

#[test]
fn test_expanded_stream_arithmetic_args_parse() {
    // Verify that `0 * 250` style expressions in .process() args parse correctly.
    // This is what loop expansion produces (vs pre-computed literal values).
    let input = r#"
fn compute(x: int, y: int):
    emit Result(x: x, y: y)

for i in 0..2:
    stream S{i} = Trigger{i}
        .process(compute({i} * 100, {i} * 200))
"#;
    let result = parse(input);
    assert!(
        result.is_ok(),
        "Arithmetic expressions in .process() args should parse: {:?}",
        result.err()
    );
    let program = result.unwrap();
    // 1 function + 2 streams
    assert_eq!(program.statements.len(), 3);
}

#[test]
fn test_expanded_mandelbrot_text_matches_original_structure() {
    // Verify that the expanded compact VPL produces the same structure
    // as the original hand-written version
    let compact = r#"for row in 0..4:
    for col in 0..4:
        context t{row}{col}
"#;
    let expanded = expand_declaration_loops(compact);
    let lines: Vec<&str> = expanded.lines().filter(|l| !l.trim().is_empty()).collect();

    // Should produce exactly 16 lines, one per context
    assert_eq!(lines.len(), 16);

    // Verify ordering: row-major (t00, t01, t02, t03, t10, t11, ...)
    let expected = [
        "context t00",
        "context t01",
        "context t02",
        "context t03",
        "context t10",
        "context t11",
        "context t12",
        "context t13",
        "context t20",
        "context t21",
        "context t22",
        "context t23",
        "context t30",
        "context t31",
        "context t32",
        "context t33",
    ];
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(
            line.trim(),
            expected[i],
            "Line {} mismatch: got '{}', expected '{}'",
            i,
            line.trim(),
            expected[i]
        );
    }
}

#[test]
fn test_expanded_stream_args_match_original() {
    // Verify the expanded .process() args produce the correct offsets
    // Row 0: x_off = col*250, y_off = 0
    // Row 3: x_off = col*250, y_off = 3*250
    let input = r#"for row in 0..4:
    for col in 0..4:
        stream Tile{row}{col} = ComputeTile{row}{col}
            .process(compute_tile({col} * 250, {row} * 250, 250, 256))
"#;
    let expanded = expand_declaration_loops(input);

    // Spot-check corner and edge tiles
    assert!(
        expanded.contains(".process(compute_tile(0 * 250, 0 * 250, 250, 256))"),
        "Tile00 should have col=0, row=0 offsets"
    );
    assert!(
        expanded.contains(".process(compute_tile(3 * 250, 0 * 250, 250, 256))"),
        "Tile03 should have col=3, row=0 offsets"
    );
    assert!(
        expanded.contains(".process(compute_tile(0 * 250, 3 * 250, 250, 256))"),
        "Tile30 should have col=0, row=3 offsets"
    );
    assert!(
        expanded.contains(".process(compute_tile(3 * 250, 3 * 250, 250, 256))"),
        "Tile33 should have col=3, row=3 offsets"
    );
    assert!(
        expanded.contains(".process(compute_tile(2 * 250, 1 * 250, 250, 256))"),
        "Tile12 should have col=2, row=1 offsets"
    );
}
