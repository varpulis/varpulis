// SPDX-License-Identifier: MIT OR Apache-2.0
#![warn(clippy::unwrap_used)]
//! # Varpulis Core
//!
//! Foundational types and AST definitions for the VPL language.
//!
//! This crate provides the core data structures used throughout the Varpulis
//! streaming analytics engine, including:
//!
//! - **AST (Abstract Syntax Tree)**: Complete representation of VPL programs
//! - **Type System**: Type definitions for the language
//! - **Values**: Runtime value representation
//! - **Source Spans**: Location tracking for error reporting
//!
//! ## Features
//!
//! - Zero-copy parsing support via spans
//! - Serialization support via `serde`
//! - Comprehensive AST for all VPL constructs
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
//! A VPL program consists of statements:
//!
//! - `StreamDecl`: Stream definitions with operations
//! - `EventDecl`: Event type definitions
//! - `PatternDecl`: SASE+ pattern definitions
//! - `FnDecl`: User-defined functions
//! - `Config`: Configuration blocks
//!
//! ## See Also
//!
//! - [`varpulis_parser`](../varpulis_parser): Parsing VPL source code
//! - [`varpulis_runtime`](../varpulis_runtime): Executing VPL programs

pub mod ast;
pub mod decode;
pub mod event;
pub mod limits;
pub mod pagination;
pub mod plan;
pub mod security;
pub mod span;
pub mod types;
pub mod validate;
pub mod value;

pub use ast::*;
pub use event::{Event, FieldKey, SharedEvent};
pub use span::Span;
pub use types::*;
pub use value::Value;

#[cfg(test)]
mod semver {
    /// The `[package.metadata.cargo-semver-checks.lints]` block in Cargo.toml
    /// allows `enum_variant_added`, for the `FollowedBy`, `And` and `Or`
    /// variants of `SasePatternExpr`, taken against the 0.11.0 baseline.
    ///
    /// The exception is crate-wide: while it stands it also covers every
    /// *other* public enum here. This test is the reminder to remove it. It
    /// fails on the release commit that bumps the version, which is when the
    /// baseline already holds the variants and the block must go.
    #[test]
    fn semver_exceptions_expire_with_their_baseline() {
        let version = env!("CARGO_PKG_VERSION");
        assert!(
            version.starts_with("0.11."),
            "varpulis-core is now {version}, past the 0.11.0 baseline the \
             cargo-semver-checks lint exceptions were taken against. Delete the \
             [package.metadata.cargo-semver-checks.lints] block in \
             crates/varpulis-core/Cargo.toml and this test with it."
        );
    }
}
