// SPDX-License-Identifier: MIT OR Apache-2.0
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
//! - [`parse`]: the parser, a Pest PEG grammar behind one function
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

/// Parse error types with source location information.
/// Top-level constants substituted into the expressions streams evaluate.
mod constants;
pub mod error;
/// Compile-time expansion of top-level `for` loops in VPL source.
pub mod expand;
/// Helper functions for parsing literal values (durations, timestamps).
pub mod helpers;
/// Indentation preprocessor that converts Python-style blocks to explicit markers.
pub mod indent;
/// Logos-based lexer producing spanned tokens for VPL source.
pub mod lexer;
/// AST-level constant folding optimization pass.
pub mod optimize;
/// Rule-based logical plan optimizer (filter pushdown, window merge, etc.).
pub mod optimizer;
/// Pest PEG parser that transforms VPL source into an AST. Private: the
/// `Rule` enum `pest_derive` generates in it changes with every grammar rule,
/// and a public `Rule` made each new rule a breaking change of a type nobody
/// outside this crate uses. [`parse`] is the interface.
#[allow(missing_docs)]
mod pest_parser;

pub use error::{ParseError, RichParseError};
pub use lexer::Token;
pub use optimizer::optimize_plan;
pub use pest_parser::parse;

#[cfg(test)]
mod semver {
    /// The `[package.metadata.cargo-semver-checks.lints]` block in Cargo.toml
    /// allows the removal of the `pest_parser` module (and the generated
    /// `Rule` and `VarpulisParser` with it) from the public API, taken against
    /// the 0.11.0 baseline. Once the version moves past 0.11.x the baseline
    /// no longer has them and the block must go.
    #[test]
    fn semver_exceptions_expire_with_their_baseline() {
        let version = env!("CARGO_PKG_VERSION");
        assert!(
            version.starts_with("0.11."),
            "varpulis-parser is now {version}, past the 0.11.0 baseline the \
             cargo-semver-checks lint exceptions were taken against. Delete the \
             [package.metadata.cargo-semver-checks.lints] block in \
             crates/varpulis-parser/Cargo.toml and this test with it."
        );
    }
}
