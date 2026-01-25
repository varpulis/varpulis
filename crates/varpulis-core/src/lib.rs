//! Varpulis Core - Types and AST for VarpulisQL
//!
//! This crate provides the foundational types for the Varpulis streaming analytics engine.

pub mod ast;
pub mod span;
pub mod types;
pub mod value;

pub use ast::*;
pub use span::Span;
pub use types::*;
pub use value::Value;
