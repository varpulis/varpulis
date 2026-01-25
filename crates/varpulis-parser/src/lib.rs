//! VarpulisQL Parser
//!
//! This crate provides lexing and parsing for the VarpulisQL language.
//!
//! Two parsers are available:
//! - `parser` (hand-written recursive descent) - currently the default
//! - `pest_parser` (pest PEG parser) - in development, will become default

pub mod error;
pub mod helpers;
pub mod indent;
pub mod lexer;
pub mod parser;
pub mod pest_parser;

pub use error::ParseError;
pub use lexer::Token;

// Use Pest parser as default (with indentation preprocessor)
pub use pest_parser::parse;

// Re-export old hand-written parser for compatibility
#[deprecated(note = "Use pest_parser::parse instead")]
pub use parser::parse as legacy_parse;
