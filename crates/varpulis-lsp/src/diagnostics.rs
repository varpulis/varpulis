//! Diagnostics generation from parser errors

use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use varpulis_parser::{parse, ParseError};

/// Convert parser errors to LSP diagnostics
pub fn get_diagnostics(text: &str) -> Vec<Diagnostic> {
    match parse(text) {
        Ok(_) => vec![],
        Err(error) => vec![error_to_diagnostic(text, &error)],
    }
}

fn error_to_diagnostic(source: &str, error: &ParseError) -> Diagnostic {
    match error {
        ParseError::Located {
            line,
            column,
            message,
            hint,
            ..
        } => {
            let line_0 = line.saturating_sub(1) as u32;
            let col_0 = column.saturating_sub(1) as u32;

            // Try to get the error span length from the source
            let end_col = get_error_end_column(source, line_0 as usize, col_0 as usize);

            let mut full_message = message.clone();
            if let Some(h) = hint {
                full_message.push_str("\n\nHint: ");
                full_message.push_str(h);
            }

            Diagnostic {
                range: Range {
                    start: Position {
                        line: line_0,
                        character: col_0,
                    },
                    end: Position {
                        line: line_0,
                        character: end_col as u32,
                    },
                },
                severity: Some(DiagnosticSeverity::ERROR),
                code: None,
                code_description: None,
                source: Some("varpulis".to_string()),
                message: full_message,
                related_information: None,
                tags: None,
                data: None,
            }
        }

        ParseError::UnexpectedToken {
            position,
            expected,
            found,
        } => {
            let (line, col) = position_to_line_col(source, *position);
            Diagnostic {
                range: Range {
                    start: Position {
                        line: line as u32,
                        character: col as u32,
                    },
                    end: Position {
                        line: line as u32,
                        character: (col + found.len()).min(
                            source
                                .lines()
                                .nth(line)
                                .map(|l| l.len())
                                .unwrap_or(col + 10),
                        ) as u32,
                    },
                },
                severity: Some(DiagnosticSeverity::ERROR),
                code: None,
                code_description: None,
                source: Some("varpulis".to_string()),
                message: format!("Unexpected token: expected {}, found '{}'", expected, found),
                related_information: None,
                tags: None,
                data: None,
            }
        }

        ParseError::UnexpectedEof => Diagnostic {
            range: Range {
                start: Position {
                    line: source.lines().count().saturating_sub(1) as u32,
                    character: source.lines().last().map(|l| l.len()).unwrap_or(0) as u32,
                },
                end: Position {
                    line: source.lines().count().saturating_sub(1) as u32,
                    character: source.lines().last().map(|l| l.len()).unwrap_or(0) as u32,
                },
            },
            severity: Some(DiagnosticSeverity::ERROR),
            code: None,
            code_description: None,
            source: Some("varpulis".to_string()),
            message: "Unexpected end of input".to_string(),
            related_information: None,
            tags: None,
            data: None,
        },

        ParseError::InvalidToken { position, message } => {
            let (line, col) = position_to_line_col(source, *position);
            Diagnostic {
                range: Range {
                    start: Position {
                        line: line as u32,
                        character: col as u32,
                    },
                    end: Position {
                        line: line as u32,
                        character: (col + 10) as u32,
                    },
                },
                severity: Some(DiagnosticSeverity::ERROR),
                code: None,
                code_description: None,
                source: Some("varpulis".to_string()),
                message: message.clone(),
                related_information: None,
                tags: None,
                data: None,
            }
        }

        ParseError::InvalidNumber(msg)
        | ParseError::InvalidDuration(msg)
        | ParseError::InvalidTimestamp(msg)
        | ParseError::InvalidEscape(msg) => Diagnostic {
            range: Range::default(),
            severity: Some(DiagnosticSeverity::ERROR),
            code: None,
            code_description: None,
            source: Some("varpulis".to_string()),
            message: msg.clone(),
            related_information: None,
            tags: None,
            data: None,
        },

        ParseError::UnterminatedString(position) => {
            let (line, col) = position_to_line_col(source, *position);
            Diagnostic {
                range: Range {
                    start: Position {
                        line: line as u32,
                        character: col as u32,
                    },
                    end: Position {
                        line: line as u32,
                        character: source.lines().nth(line).map(|l| l.len()).unwrap_or(col) as u32,
                    },
                },
                severity: Some(DiagnosticSeverity::ERROR),
                code: None,
                code_description: None,
                source: Some("varpulis".to_string()),
                message: "Unterminated string literal".to_string(),
                related_information: None,
                tags: None,
                data: None,
            }
        }

        ParseError::Custom { span, message } => {
            // Convert span to line/col
            let (start_line, start_col) = position_to_line_col(source, span.start);
            let (end_line, end_col) = position_to_line_col(source, span.end);

            Diagnostic {
                range: Range {
                    start: Position {
                        line: start_line as u32,
                        character: start_col as u32,
                    },
                    end: Position {
                        line: end_line as u32,
                        character: end_col as u32,
                    },
                },
                severity: Some(DiagnosticSeverity::ERROR),
                code: None,
                code_description: None,
                source: Some("varpulis".to_string()),
                message: message.clone(),
                related_information: None,
                tags: None,
                data: None,
            }
        }
    }
}

/// Convert byte position to line and column (0-indexed)
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

/// Get the end column for an error, trying to highlight the problematic token
fn get_error_end_column(source: &str, line: usize, start_col: usize) -> usize {
    if let Some(line_text) = source.lines().nth(line) {
        let remaining = &line_text[start_col.min(line_text.len())..];

        // Find the end of the current token
        let token_len = remaining
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .count();

        if token_len > 0 {
            start_col + token_len
        } else {
            // Highlight at least one character
            start_col + 1
        }
    } else {
        start_col + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_code_no_diagnostics() {
        let code = r#"
stream SensorData from "mqtt://localhost:1883/sensors"
    .where(temperature > 25)
    .emit()
"#;
        let diagnostics = get_diagnostics(code);
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_syntax_error_has_diagnostic() {
        // Use definitely invalid syntax - unmatched parenthesis
        let code = "stream x = y.where(";
        let diagnostics = get_diagnostics(code);
        assert!(!diagnostics.is_empty());
        assert_eq!(diagnostics[0].severity, Some(DiagnosticSeverity::ERROR));
    }
}
