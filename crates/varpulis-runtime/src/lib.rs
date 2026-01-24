//! Varpulis Runtime - Execution engine for VarpulisQL
//!
//! This crate provides the runtime for executing VarpulisQL programs.

pub mod aggregation;
pub mod attention;
pub mod connector;
pub mod engine;
pub mod event;
pub mod event_file;
pub mod metrics;
pub mod sequence;
pub mod sink;
pub mod simulator;
pub mod stream;
pub mod window;

pub use engine::Engine;
pub use event::Event;
pub use metrics::Metrics;
pub use sink::{ConsoleSink, FileSink, HttpSink, MultiSink, Sink};
pub use stream::Stream;
