//! # Varpulis Core
//!
//! Foundational types and AST definitions for the VarpulisQL language.
//!
//! This crate provides the core data structures used throughout the Varpulis
//! streaming analytics engine, including:
//!
//! - **AST (Abstract Syntax Tree)**: Complete representation of VarpulisQL programs
//! - **Type System**: Type definitions for the language
//! - **Values**: Runtime value representation
//! - **Source Spans**: Location tracking for error reporting
//!
//! ## Features
//!
//! - Zero-copy parsing support via spans
//! - Serialization support via `serde`
//! - Comprehensive AST for all VarpulisQL constructs
//!
//! ## Modules
//!
//! - [`ast`]: Program structure, statements, expressions, and SASE patterns
//! - [`types`]: Type system definitions (`Int`, `Float`, `String`, etc.)
//! - [`value`]: Runtime values with type coercion and operations
//! - [`span`]: Source location tracking for diagnostics
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use varpulis_core::{Program, Stmt, Value};
//!
//! // Values are the runtime representation
//! let int_val = Value::Int(42);
//! let float_val = Value::Float(3.14);
//! let str_val = Value::Str("hello".into());
//!
//! // Check value types
//! assert!(int_val.is_int());
//! assert!(float_val.is_float());
//! ```
//!
//! ## AST Structure
//!
//! A VarpulisQL program consists of statements:
//!
//! - `StreamDecl`: Stream definitions with operations
//! - `EventDecl`: Event type definitions
//! - `PatternDecl`: SASE+ pattern definitions
//! - `FnDecl`: User-defined functions
//! - `Config`: Configuration blocks
//!
//! ## See Also
//!
//! - [`varpulis_parser`](../varpulis_parser): Parsing VarpulisQL source code
//! - [`varpulis_runtime`](../varpulis_runtime): Executing VarpulisQL programs

pub mod ast;
pub mod span;
pub mod types;
pub mod validate;
pub mod value;

pub use ast::*;
pub use span::Span;
pub use types::*;
pub use value::Value;
