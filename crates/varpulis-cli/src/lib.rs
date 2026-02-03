//! Varpulis CLI library - testable functions and modules
//!
//! This library provides the core functionality for the Varpulis CLI,
//! organized into dedicated modules for security, WebSocket handling, and more.

pub mod api;
pub mod auth;
pub mod client;
pub mod config;
pub mod rate_limit;
pub mod security;
pub mod websocket;

use anyhow::Result;
use varpulis_parser::parse;

/// Parse and validate VarpulisQL source code
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
    let (alert_tx, _alert_rx) = tokio::sync::mpsc::channel(100);
    let mut engine = varpulis_runtime::engine::Engine::new(alert_tx);
    engine
        .load(&program)
        .map_err(|e| anyhow::anyhow!("Load error: {}", e))?;

    Ok(statement_count)
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

    #[test]
    fn test_check_syntax_attention_window() {
        let source = r#"
            stream Attention = Events
                .attention_window(duration: 1h, heads: 4)
                .emit(alert_type: "attention")
        "#;
        assert!(check_syntax(source).is_ok());
    }
}
