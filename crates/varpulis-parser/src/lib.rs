//! VarpulisQL Parser
//!
//! This crate provides lexing and parsing for the VarpulisQL language.

pub mod lexer;
pub mod parser;
pub mod error;

pub use error::ParseError;
pub use lexer::Token;
pub use parser::parse;
