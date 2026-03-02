//! WASM bindings for the VPL parser and semantic validator.
//!
//! Provides `parse_vpl()` and `validate_vpl()` functions callable from JavaScript.
//! Returns structured JSON results with AST, diagnostics, and error information.
//!
//! ## SmartModule (feature: `smartmodule`)
//!
//! With the `smartmodule` feature enabled, this crate also provides a host
//! runtime for executing user-defined WASM filter and map functions. See the
//! [`smartmodule`] module for details.

use serde::Serialize;
use wasm_bindgen::prelude::*;

#[cfg(feature = "smartmodule")]
pub mod smartmodule;

/// A diagnostic message with location information.
#[derive(Serialize)]
struct WasmDiagnostic {
    severity: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    start_line: u32,
    start_col: u32,
    end_line: u32,
    end_col: u32,
}

/// Result of parsing VPL source code.
#[derive(Serialize)]
struct ParseResult {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    ast: Option<serde_json::Value>,
    diagnostics: Vec<WasmDiagnostic>,
}

/// Result of validating VPL source code (parse + semantic checks).
#[derive(Serialize)]
struct ValidateResult {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    ast: Option<serde_json::Value>,
    diagnostics: Vec<WasmDiagnostic>,
}

/// Parse VPL source code and return a JSON result.
///
/// Returns `{ ok: true, ast: {...}, diagnostics: [] }` on success,
/// or `{ ok: false, ast: null, diagnostics: [{...}] }` on parse error.
#[wasm_bindgen]
pub fn parse_vpl(input: &str) -> String {
    let result = match varpulis_parser::parse(input) {
        Ok(program) => {
            let ast = serde_json::to_value(&program).ok();
            ParseResult {
                ok: true,
                ast,
                diagnostics: vec![],
            }
        }
        Err(error) => {
            let diag = parse_error_to_diagnostic(input, &error);
            ParseResult {
                ok: false,
                ast: None,
                diagnostics: vec![diag],
            }
        }
    };
    serde_json::to_string(&result).unwrap_or_else(|_| r#"{"ok":false,"diagnostics":[]}"#.into())
}

/// Parse and validate VPL source code, returning a JSON result.
///
/// Runs the parser first, then if successful runs semantic validation.
/// Returns all diagnostics (errors and warnings) from both phases.
#[wasm_bindgen]
pub fn validate_vpl(input: &str) -> String {
    let result = match varpulis_parser::parse(input) {
        Ok(program) => {
            let ast = serde_json::to_value(&program).ok();
            let validation = varpulis_core::validate::validate(input, &program);
            let diagnostics: Vec<WasmDiagnostic> = validation
                .diagnostics
                .iter()
                .map(|d| semantic_diagnostic(input, d))
                .collect();
            let has_errors = diagnostics.iter().any(|d| d.severity == "error");
            ValidateResult {
                ok: !has_errors,
                ast,
                diagnostics,
            }
        }
        Err(error) => {
            let diag = parse_error_to_diagnostic(input, &error);
            ValidateResult {
                ok: false,
                ast: None,
                diagnostics: vec![diag],
            }
        }
    };
    serde_json::to_string(&result).unwrap_or_else(|_| r#"{"ok":false,"diagnostics":[]}"#.into())
}

fn semantic_diagnostic(source: &str, d: &varpulis_core::validate::Diagnostic) -> WasmDiagnostic {
    let (start_line, start_col) = position_to_line_col(source, d.span.start);
    let (end_line, end_col) = position_to_line_col(source, d.span.end);

    WasmDiagnostic {
        severity: match d.severity {
            varpulis_core::validate::Severity::Error => "error",
            varpulis_core::validate::Severity::Warning => "warning",
        },
        message: d.message.clone(),
        hint: d.hint.clone(),
        code: d.code.map(|c| c.to_string()),
        start_line: start_line as u32,
        start_col: start_col as u32,
        end_line: end_line as u32,
        end_col: end_col as u32,
    }
}

fn parse_error_to_diagnostic(source: &str, error: &varpulis_parser::ParseError) -> WasmDiagnostic {
    use varpulis_parser::ParseError;
    match error {
        ParseError::Located {
            line,
            column,
            message,
            hint,
            ..
        } => WasmDiagnostic {
            severity: "error",
            message: message.clone(),
            hint: hint.clone(),
            code: None,
            start_line: line.saturating_sub(1) as u32,
            start_col: column.saturating_sub(1) as u32,
            end_line: line.saturating_sub(1) as u32,
            end_col: *column as u32,
        },
        ParseError::UnexpectedToken {
            position,
            expected,
            found,
        } => {
            let (line, col) = position_to_line_col(source, *position);
            WasmDiagnostic {
                severity: "error",
                message: format!("Unexpected token: expected {expected}, found '{found}'"),
                hint: None,
                code: None,
                start_line: line as u32,
                start_col: col as u32,
                end_line: line as u32,
                end_col: (col + found.len()) as u32,
            }
        }
        ParseError::UnexpectedEof => {
            let line = source.lines().count().saturating_sub(1);
            let col = source.lines().last().map_or(0, |l| l.len());
            WasmDiagnostic {
                severity: "error",
                message: "Unexpected end of input".into(),
                hint: None,
                code: None,
                start_line: line as u32,
                start_col: col as u32,
                end_line: line as u32,
                end_col: col as u32,
            }
        }
        ParseError::InvalidToken { position, message } => {
            let (line, col) = position_to_line_col(source, *position);
            WasmDiagnostic {
                severity: "error",
                message: message.clone(),
                hint: None,
                code: None,
                start_line: line as u32,
                start_col: col as u32,
                end_line: line as u32,
                end_col: (col + 10) as u32,
            }
        }
        ParseError::InvalidNumber(msg)
        | ParseError::InvalidDuration(msg)
        | ParseError::InvalidTimestamp(msg)
        | ParseError::InvalidEscape(msg) => WasmDiagnostic {
            severity: "error",
            message: msg.clone(),
            hint: None,
            code: None,
            start_line: 0,
            start_col: 0,
            end_line: 0,
            end_col: 0,
        },
        ParseError::UnterminatedString(position) => {
            let (line, col) = position_to_line_col(source, *position);
            WasmDiagnostic {
                severity: "error",
                message: "Unterminated string literal".into(),
                hint: None,
                code: None,
                start_line: line as u32,
                start_col: col as u32,
                end_line: line as u32,
                end_col: source.lines().nth(line).map_or(col, |l| l.len()) as u32,
            }
        }
        ParseError::Custom { span, message } => {
            let (start_line, start_col) = position_to_line_col(source, span.start);
            let (end_line, end_col) = position_to_line_col(source, span.end);
            WasmDiagnostic {
                severity: "error",
                message: message.clone(),
                hint: None,
                code: None,
                start_line: start_line as u32,
                start_col: start_col as u32,
                end_line: end_line as u32,
                end_col: end_col as u32,
            }
        }
    }
}

fn position_to_line_col(source: &str, position: usize) -> (usize, usize) {
    let mut line = 0;
    let mut col = 0;
    let mut pos = 0;

    for ch in source.chars() {
        if pos >= position {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 0;
        } else {
            col += 1;
        }
        pos += ch.len_utf8();
    }

    (line, col)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_vpl() {
        let input = r"
event SensorReading:
    temperature: int

stream SensorData = SensorReading
    .where(temperature > 25)
    .emit()
";
        let result = parse_vpl(input);
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["ok"], true);
        assert!(json["ast"].is_object());
    }

    #[test]
    fn test_parse_invalid_vpl() {
        let input = "stream x = y.where(";
        let result = parse_vpl(input);
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["ok"], false);
        assert!(!json["diagnostics"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_validate_valid_vpl() {
        let input = r"
event SensorReading:
    temperature: int

stream SensorData = SensorReading
    .where(temperature > 25)
    .emit()
";
        let result = validate_vpl(input);
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["ok"], true);
    }

    #[test]
    fn test_validate_with_semantic_warning() {
        // This should parse but may produce semantic diagnostics
        let input = r"
event A:
    x: int

event A:
    y: int
";
        let result = validate_vpl(input);
        let _json: serde_json::Value = serde_json::from_str(&result).unwrap();
        // Just verify it returns valid JSON
    }
}
