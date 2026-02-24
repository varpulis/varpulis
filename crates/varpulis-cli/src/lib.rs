//! Varpulis CLI library - testable functions and modules
//!
//! This library provides the core functionality for the Varpulis CLI,
//! organized into dedicated modules for security, WebSocket handling, and more.

pub mod api;
pub mod auth;
pub mod billing;
pub mod client;
pub mod config;
pub mod oauth;
pub mod playground;
pub mod rate_limit;
pub mod security;
pub mod websocket;

use anyhow::Result;
use std::path::PathBuf;
use varpulis_core::ast::{Program, Stmt};
use varpulis_parser::parse;

/// Parse and validate VPL source code
pub fn check_syntax(source: &str) -> Result<()> {
    match parse(source) {
        Ok(program) => {
            println!("Syntax OK ({} statements)", program.statements.len());
            Ok(())
        }
        Err(e) => {
            println!("Syntax error: {}", e);
            Err(anyhow::anyhow!("Parse error: {}", e))
        }
    }
}

/// Parse and return statement count
pub fn parse_program(source: &str) -> Result<usize> {
    let program = parse(source).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    Ok(program.statements.len())
}

/// Validate program can be loaded by engine
pub fn validate_program(source: &str) -> Result<usize> {
    let program = parse(source).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    let statement_count = program.statements.len();

    // Try to create engine and load program
    let (output_tx, _output_rx) = tokio::sync::mpsc::channel(100);
    let mut engine = varpulis_runtime::engine::Engine::new(output_tx);
    engine
        .load(&program)
        .map_err(|e| anyhow::anyhow!("Load error: {}", e))?;

    Ok(statement_count)
}

// =============================================================================
// Import resolution (moved from main.rs for testability)
// =============================================================================

/// Maximum depth for nested imports to prevent stack overflow
pub const MAX_IMPORT_DEPTH: usize = 10;

/// Resolve `@import` statements by loading and parsing imported files.
///
/// Recursively processes imports with cycle detection and depth limiting.
/// Imported statements are prepended to the program (before main file statements).
pub fn resolve_imports(program: &mut Program, base_path: Option<&PathBuf>) -> Result<()> {
    use std::collections::HashSet;
    let mut visited = HashSet::new();
    resolve_imports_inner(program, base_path, 0, &mut visited)
}

fn resolve_imports_inner(
    program: &mut Program,
    base_path: Option<&PathBuf>,
    depth: usize,
    visited: &mut std::collections::HashSet<PathBuf>,
) -> Result<()> {
    if depth > MAX_IMPORT_DEPTH {
        anyhow::bail!(
            "Import depth limit exceeded (max {}). Check for circular imports.",
            MAX_IMPORT_DEPTH
        );
    }

    let mut imported_statements = Vec::new();
    let mut imports_to_process = Vec::new();

    for stmt in &program.statements {
        if let Stmt::Import { path, .. } = &stmt.node {
            imports_to_process.push(path.clone());
        }
    }

    for import_path in imports_to_process {
        let full_path = if let Some(base) = base_path {
            base.join(&import_path)
        } else {
            PathBuf::from(&import_path)
        };

        let canonical_path = full_path.canonicalize().map_err(|e| {
            anyhow::anyhow!("Failed to resolve import '{}': {}", full_path.display(), e)
        })?;

        if visited.contains(&canonical_path) {
            continue;
        }
        visited.insert(canonical_path.clone());

        let import_source = std::fs::read_to_string(&full_path).map_err(|e| {
            anyhow::anyhow!("Failed to read import '{}': {}", full_path.display(), e)
        })?;

        let import_program = parse(&import_source).map_err(|e| {
            anyhow::anyhow!("Parse error in import '{}': {}", full_path.display(), e)
        })?;

        let import_base = full_path.parent().map(|p| p.to_path_buf());
        let mut imported = import_program;
        resolve_imports_inner(&mut imported, import_base.as_ref(), depth + 1, visited)?;

        imported_statements.extend(imported.statements);
    }

    program
        .statements
        .retain(|stmt| !matches!(&stmt.node, Stmt::Import { .. }));

    let mut new_statements = imported_statements;
    new_statements.append(&mut program.statements);
    program.statements = new_statements;

    Ok(())
}

// =============================================================================
// Simulation from source (for testing)
// =============================================================================

/// Run a VPL program against a list of events and collect output events.
///
/// This is the testable core of the `simulate` command — parses VPL source,
/// loads it into an engine, processes all input events, and returns emitted events.
pub async fn simulate_from_source(
    vpl: &str,
    events: Vec<varpulis_runtime::event::Event>,
) -> Result<Vec<varpulis_runtime::event::Event>> {
    use varpulis_runtime::event::Event;

    let program = parse(vpl).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel::<Event>(10_000);
    let mut engine = varpulis_runtime::engine::Engine::new(output_tx);
    engine
        .load(&program)
        .map_err(|e| anyhow::anyhow!("Load error: {}", e))?;

    engine
        .process_batch_sync(events)
        .map_err(|e| anyhow::anyhow!("Process error: {}", e))?;

    // Drop the sender side so the receiver knows no more events are coming
    drop(engine);

    let mut results = Vec::new();
    while let Ok(event) = output_rx.try_recv() {
        results.push(event);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_syntax_valid() {
        let source = r#"
            stream HighTemp = TempReading
                .where(temperature > 30)
                .emit(alert_type: "high_temp")
        "#;
        assert!(check_syntax(source).is_ok());
    }

    #[test]
    fn test_check_syntax_invalid() {
        let source = r#"
            stream Invalid =
                .where(
        "#;
        assert!(check_syntax(source).is_err());
    }

    #[test]
    fn test_parse_program_valid() {
        let source = r#"
            stream Test = Events
                .where(value > 10)
        "#;
        let result = parse_program(source);
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed"), 1);
    }

    #[test]
    fn test_parse_program_invalid() {
        // Use truly invalid syntax (unclosed parenthesis)
        let source = "stream x = (";
        assert!(parse_program(source).is_err());
    }

    #[tokio::test]
    async fn test_validate_program_simple() {
        let source = r#"
            stream Simple = Events
                .where(x > 0)
                .emit(alert_type: "test")
        "#;
        let result = validate_program(source);
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed"), 1);
    }

    #[tokio::test]
    async fn test_validate_program_multiple_streams() {
        let source = r#"
            stream A = Events
                .where(event_type == "a")
                .emit(alert_type: "a")

            stream B = Events
                .where(event_type == "b")
                .emit(alert_type: "b")
        "#;
        let result = validate_program(source);
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed"), 2);
    }

    #[tokio::test]
    async fn test_validate_program_with_filter() {
        let source = r#"
            stream Filtered = Metrics
                .where(value > 100)
                .emit(alert_type: "high_value")
        "#;
        let result = validate_program(source);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_syntax_followed_by() {
        let source = r#"
            stream Pattern = Events
                .pattern(p: A -> B)
                .emit(alert_type: "sequence_match")
        "#;
        assert!(check_syntax(source).is_ok());
    }

    #[test]
    fn test_check_syntax_event_declaration() {
        let source = r#"
            event TempReading:
                sensor_id: str
                temperature: float
        "#;
        assert!(check_syntax(source).is_ok());
    }

    #[test]
    fn test_check_syntax_function_declaration() {
        let source = r#"
            fn celsius_to_fahrenheit(c: float) -> float:
                c * 9.0 / 5.0 + 32.0
        "#;
        assert!(check_syntax(source).is_ok());
    }

    #[test]
    fn test_check_syntax_pattern_matching() {
        let source = r#"
            stream PatternMatch = Events
                .pattern(p: A -> B)
                .emit(alert_type: "pattern")
        "#;
        assert!(check_syntax(source).is_ok());
    }

    #[test]
    fn test_check_syntax_merge() {
        let source = r#"
            stream Merged = merge(StreamA, StreamB)
                .emit(alert_type: "merged")
        "#;
        assert!(check_syntax(source).is_ok());
    }
}
