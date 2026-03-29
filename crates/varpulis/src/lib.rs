//! # Varpulis
//!
//! **Detect temporal patterns in event streams.**
//! In Rust, in milliseconds, in 10 lines.
//!
//! This is the meta-crate for the Varpulis streaming analytics engine.
//! It re-exports the core library types for use as a Rust dependency.
//!
//! ## Install the CLI
//!
//! ```bash
//! cargo install varpulis-cli
//! ```
//!
//! ## Use as a library
//!
//! ```toml
//! [dependencies]
//! varpulis = "0.9"
//! ```
//!
//! ## Links
//!
//! - [Documentation](https://www.varpulis-cep.com/docs/)
//! - [GitHub](https://github.com/varpulis/varpulis)

/// Core types: AST, values, types, validation.
pub use varpulis_core as core;
/// Runtime engine: streams, patterns, windows, aggregations.
pub use varpulis_runtime as runtime;
