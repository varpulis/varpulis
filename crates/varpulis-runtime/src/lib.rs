//! Varpulis Runtime - Execution engine for VarpulisQL
//!
//! This crate provides the runtime for executing VarpulisQL programs.

pub mod aggregation;
pub mod attention;
pub mod connector;
pub mod engine;
pub mod event;
pub mod event_file;
pub mod join;
pub mod metrics;
pub mod pattern;
pub mod persistence;
pub mod sase;
pub mod sequence;
pub mod simulator;
pub mod sink;
pub mod stream;
pub mod timer;
pub mod window;
pub mod worker_pool;

pub use engine::{Engine, ReloadReport};
pub use event::{Event, SharedEvent};
pub use event_file::StreamingEventReader;
pub use metrics::Metrics;
pub use sink::{ConsoleSink, FileSink, HttpSink, MultiSink, Sink};
pub use stream::Stream;
pub use timer::{spawn_timer, TimerManager};
pub use window::{
    CountWindow, DelayBuffer, PartitionedDelayBuffer, PartitionedPreviousValueTracker,
    PartitionedSlidingWindow, PartitionedTumblingWindow, PreviousValueTracker, SlidingCountWindow,
    SlidingWindow, TumblingWindow,
};
pub use worker_pool::{
    BackpressureError, BackpressureStrategy, WorkerPool, WorkerPoolConfig, WorkerPoolMetrics,
    WorkerState, WorkerStatus,
};

// Persistence exports (always available, RocksDB impl requires "persistence" feature)
#[cfg(feature = "persistence")]
pub use persistence::RocksDbStore;
pub use persistence::{
    Checkpoint, CheckpointConfig, CheckpointManager, MemoryStore, StateStore, StoreError,
};
