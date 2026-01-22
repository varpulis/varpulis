//! Varpulis Runtime - Execution engine for VarpulisQL
//!
//! This crate provides the runtime for executing VarpulisQL programs.

pub mod engine;
pub mod event;
pub mod stream;
pub mod window;
pub mod aggregation;
pub mod simulator;

pub use engine::Engine;
pub use event::Event;
pub use stream::Stream;
