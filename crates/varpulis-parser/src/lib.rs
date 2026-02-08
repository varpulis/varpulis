//! # VPL Parser
//!
//! Lexing and parsing for the VPL streaming analytics language.
//!
//! This crate transforms VPL source code into an Abstract Syntax Tree (AST)
//! that can be executed by the runtime engine.
//!
//! ## Features
//!
//! - Complete VPL grammar support
//! - Detailed error messages with line/column information
//! - Syntax hints for common mistakes
//! - PEG-based parsing via Pest
//!
//! ## Modules
//!
//! - [`pest_parser`]: Main parser implementation using Pest PEG grammar
//! - [`lexer`]: Token definitions (used for syntax highlighting)
//! - [`error`]: Parse error types with location information
//! - [`helpers`]: Parsing utility functions
//! - [`indent`]: Indentation handling
//!
//! ## Quick Start
//!
//! ```rust
//! use varpulis_parser::parse;
//!
//! let source = r#"
//!     stream Readings = SensorReading
//!         .where(temperature > 100)
//!         .emit(alert("HighTemp", "Temperature exceeded threshold"))
//! "#;
//!
//! match parse(source) {
//!     Ok(program) => {
//!         println!("Parsed {} statements", program.statements.len());
//!     }
//!     Err(e) => {
//!         eprintln!("Parse error: {}", e);
//!     }
//! }
//! ```
//!
//! ## Error Handling
//!
//! Parse errors include detailed location information:
//!
//! ```rust
//! use varpulis_parser::{parse, ParseError};
//!
//! let result = parse("stream X form Y");  // Typo: "form" instead of "from"
//! if let Err(ParseError::Located { line, column, message, hint, .. }) = result {
//!     println!("Error at {}:{}: {}", line, column, message);
//!     if let Some(h) = hint {
//!         println!("Hint: {}", h);
//!     }
//! }
//! ```
//!
//! ## Grammar
//!
//! The VPL grammar is defined in `varpulis.pest` and supports:
//!
//! - Stream declarations with filtering, selection, windowing, and aggregation
//! - Event type definitions
//! - SASE+ pattern declarations (sequences, Kleene closures, negation)
//! - User-defined functions
//! - Configuration blocks
//! - Control flow (if/elif/else, for, while)
//! - Expressions with operators and function calls
//!
//! ## See Also
//!
//! - [`varpulis_core`](../varpulis_core): AST types produced by the parser
//! - [`varpulis_runtime`](../varpulis_runtime): Executing parsed programs

pub mod error;
pub mod expand;
pub mod helpers;
pub mod indent;
pub mod lexer;
pub mod optimize;
pub mod pest_parser;

pub use error::ParseError;
pub use lexer::Token;
pub use pest_parser::parse;
