//! Parser error types

use miette::{Diagnostic, NamedSource, SourceSpan};
use thiserror::Error as ThisError;
use varpulis_core::Span;

/// Location in source code with line and column
#[derive(Debug, Clone)]
pub struct SourceLocation {
    /// 1-based line number.
    pub line: usize,
    /// 1-based column number.
    pub column: usize,
    /// 0-based byte offset in the source string.
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

        Self {
            line,
            column,
            position,
        }
    }
}

/// Errors that can occur during VPL parsing.
#[derive(Debug, ThisError, Clone)]
pub enum ParseError {
    /// Error with precise source location and an optional hint for the user.
    #[error("Line {line}, column {column}: {message}")]
    Located {
        /// 1-based line number where the error occurred.
        line: usize,
        /// 1-based column number where the error occurred.
        column: usize,
        /// 0-based byte offset in the source string.
        position: usize,
        /// Human-readable description of the error.
        message: String,
        /// Optional suggestion for how to fix the error.
        hint: Option<String>,
    },

    /// An unexpected token was encountered during parsing.
    #[error("Unexpected token at position {position}: expected {expected}, found {found}")]
    UnexpectedToken {
        /// 0-based byte offset of the unexpected token.
        position: usize,
        /// Description of what was expected.
        expected: String,
        /// Description of what was found instead.
        found: String,
    },

    /// The input ended unexpectedly (e.g., unclosed parenthesis).
    #[error("Unexpected end of input")]
    UnexpectedEof,

    /// A token that does not belong to the VPL grammar was found.
    #[error("Invalid token at position {position}: {message}")]
    InvalidToken {
        /// 0-based byte offset of the invalid token.
        position: usize,
        /// Description of why the token is invalid.
        message: String,
    },

    /// A numeric literal could not be parsed.
    #[error("Invalid number literal: {0}")]
    InvalidNumber(String),

    /// A duration literal (e.g., `5s`, `100ms`) could not be parsed.
    #[error("Invalid duration literal: {0}")]
    InvalidDuration(String),

    /// A timestamp literal (e.g., `@2024-01-15`) could not be parsed.
    #[error("Invalid timestamp literal: {0}")]
    InvalidTimestamp(String),

    /// A string literal was not closed before end of input.
    #[error("Unterminated string starting at position {0}")]
    UnterminatedString(
        /// 0-based byte offset where the string started.
        usize,
    ),

    /// An unrecognized escape sequence was found inside a string literal.
    #[error("Invalid escape sequence: {0}")]
    InvalidEscape(String),

    /// A custom error with an associated source span.
    #[error("{message}")]
    Custom {
        /// Source span where the error occurred.
        span: Span,
        /// Human-readable error message.
        message: String,
    },
}

impl ParseError {
    /// Create a custom error from a source span and message.
    pub fn custom(span: Span, message: impl Into<String>) -> Self {
        Self::Custom {
            span,
            message: message.into(),
        }
    }

    /// Create an error with source location and optional hint
    pub fn at_location(
        source: &str,
        position: usize,
        message: impl Into<String>,
        hint: Option<String>,
    ) -> Self {
        let loc = SourceLocation::from_position(source, position);
        Self::Located {
            line: loc.line,
            column: loc.column,
            position,
            message: message.into(),
            hint,
        }
    }

    /// Extract the byte offset and length for highlighting in the source.
    ///
    /// Returns `(offset, length)` where length defaults to 1 when only a
    /// point position is available.
    pub fn source_span(&self, source: &str) -> (usize, usize) {
        match self {
            Self::Located { position, .. }
            | Self::UnexpectedToken { position, .. }
            | Self::InvalidToken { position, .. }
            | Self::UnterminatedString(position) => {
                let pos = (*position).min(source.len());
                // Highlight at least 1 char, or to end-of-line
                let rest = &source[pos..];
                let len = rest.find(['\n', '\r']).unwrap_or(rest.len()).max(1);
                (pos, len)
            }
            Self::Custom { span, .. } => {
                let start = span.start.min(source.len());
                let end = span.end.min(source.len());
                (start, (end - start).max(1))
            }
            // No position info — highlight the first character
            Self::UnexpectedEof
            | Self::InvalidNumber(_)
            | Self::InvalidDuration(_)
            | Self::InvalidTimestamp(_)
            | Self::InvalidEscape(_) => (source.len().saturating_sub(1), 1),
        }
    }

    /// Return the hint text, if the variant carries one.
    pub fn hint(&self) -> Option<&str> {
        match self {
            Self::Located { hint, .. } => hint.as_deref(),
            _ => None,
        }
    }
}

/// A [`ParseError`] bundled with source text for rich terminal rendering
/// via [`miette`].
///
/// Consumers that only need the plain error can continue to use [`ParseError`]
/// directly. The CLI wraps errors in this type to get coloured,
/// source-highlighted diagnostics.
#[derive(Debug)]
pub struct RichParseError {
    inner: ParseError,
    src: NamedSource<String>,
    span: SourceSpan,
    help: Option<String>,
}

impl std::fmt::Display for RichParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl std::error::Error for RichParseError {}

impl Diagnostic for RichParseError {
    fn help<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        self.help
            .as_ref()
            .map(|h| Box::new(h.clone()) as Box<dyn std::fmt::Display>)
    }

    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        Some(&self.src)
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        Some(Box::new(std::iter::once(
            miette::LabeledSpan::new_primary_with_span(Some("here".to_string()), self.span),
        )))
    }
}

impl RichParseError {
    /// Wrap a [`ParseError`] with the source text and filename so that
    /// `miette` can render a rich diagnostic.
    pub fn new(error: ParseError, source: &str, filename: &str) -> Self {
        let (offset, len) = error.source_span(source);
        let help = error.hint().map(String::from);
        Self {
            src: NamedSource::new(filename, source.to_string()),
            span: SourceSpan::new(offset.into(), len),
            help,
            inner: error,
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

/// Convenience alias for `Result<T, ParseError>`.
pub type ParseResult<T> = Result<T, ParseError>;
