//! Parser error types

use thiserror::Error;
use varpulis_core::Span;

/// Location in source code with line and column
#[derive(Debug, Clone)]
pub struct SourceLocation {
    pub line: usize,
    pub column: usize,
    pub position: usize,
}

impl SourceLocation {
    /// Convert a byte position to line/column using the source text
    pub fn from_position(source: &str, position: usize) -> Self {
        let mut line = 1;
        let mut column = 1;
        
        for (i, ch) in source.chars().enumerate() {
            if i >= position {
                break;
            }
            if ch == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        
        SourceLocation { line, column, position }
    }
}

#[derive(Debug, Error, Clone)]
pub enum ParseError {
    #[error("Line {line}, column {column}: {message}")]
    Located {
        line: usize,
        column: usize,
        position: usize,
        message: String,
        hint: Option<String>,
    },

    #[error("Unexpected token at position {position}: expected {expected}, found {found}")]
    UnexpectedToken {
        position: usize,
        expected: String,
        found: String,
    },

    #[error("Unexpected end of input")]
    UnexpectedEof,

    #[error("Invalid token at position {position}: {message}")]
    InvalidToken { position: usize, message: String },

    #[error("Invalid number literal: {0}")]
    InvalidNumber(String),

    #[error("Invalid duration literal: {0}")]
    InvalidDuration(String),

    #[error("Invalid timestamp literal: {0}")]
    InvalidTimestamp(String),

    #[error("Unterminated string starting at position {0}")]
    UnterminatedString(usize),

    #[error("Invalid escape sequence: {0}")]
    InvalidEscape(String),

    #[error("{message}")]
    Custom { span: Span, message: String },
}

impl ParseError {
    pub fn custom(span: Span, message: impl Into<String>) -> Self {
        ParseError::Custom {
            span,
            message: message.into(),
        }
    }
    
    /// Create an error with source location and optional hint
    pub fn at_location(source: &str, position: usize, message: impl Into<String>, hint: Option<String>) -> Self {
        let loc = SourceLocation::from_position(source, position);
        ParseError::Located {
            line: loc.line,
            column: loc.column,
            position,
            message: message.into(),
            hint,
        }
    }
}

/// Suggestions for common mistakes
pub fn suggest_fix(token: &str) -> Option<String> {
    match token.to_lowercase().as_str() {
        "string" => Some("Did you mean 'str'? VPL uses 'str' for string types.".to_string()),
        "integer" => Some("Did you mean 'int'? VPL uses 'int' for integer types.".to_string()),
        "boolean" => Some("Did you mean 'bool'? VPL uses 'bool' for boolean types.".to_string()),
        "&&" => Some("Use 'and' instead of '&&' for logical AND.".to_string()),
        "||" => Some("Use 'or' instead of '||' for logical OR.".to_string()),
        "!" => Some("Use 'not' instead of '!' for logical NOT.".to_string()),
        "function" | "func" | "def" => Some("Use 'fn' to declare functions.".to_string()),
        "class" | "struct" => Some("Use 'event' to declare event types.".to_string()),
        _ => None,
    }
}

pub type ParseResult<T> = Result<T, ParseError>;
