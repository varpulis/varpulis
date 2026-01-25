//! Parser error types

use thiserror::Error;
use varpulis_core::Span;

#[derive(Debug, Error, Clone)]
pub enum ParseError {
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
}

pub type ParseResult<T> = Result<T, ParseError>;
