#![allow(missing_docs)]
//! # Varpulis Runtime
//!
//! High-performance execution engine for VPL programs.
//!
//! This crate is the heart of Varpulis, providing:
//!
//! - **Stream Processing**: Real-time event processing with filtering and transformation
//! - **SASE+ Pattern Matching**: Complex event detection with sequences, Kleene closures, and negation
//! - **Windowed Aggregations**: Time and count-based windows with SIMD-optimized aggregations
//! - **Connectors**: MQTT, HTTP, and file-based event sources/sinks
//!
//! ## Features
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `mqtt` | MQTT connector support (rumqttc) |
//! | `kafka` | Kafka connector support (rdkafka) |
//! | `persistence` | RocksDB state persistence |
//! | `database` | SQL database connectors (PostgreSQL, MySQL, SQLite) |
//! | `redis` | Redis connector support |
//! | `all-connectors` | Enable all connector features |
//!
//! ## Modules
//!
//! ### Core Processing
//! - [`engine`]: Main execution engine, compiles and runs VPL programs
//! - [`event`]: Event structure and field access
//! - [`stream`]: Stream abstraction for event flows
//!
//! ### Pattern Matching
//! - [`sase`]: SASE+ pattern matching (SEQ, AND, OR, NOT, Kleene+/*)
//! - [`sequence`]: Sequence pattern tracking
//!
//! ### Windowing & Aggregation
//! - [`window`]: Tumbling, sliding, and count-based windows
//! - [`aggregation`]: Aggregation functions (sum, avg, min, max, stddev, percentile)
//! - [`simd`]: SIMD-optimized operations using AVX2
//!
//! ### Advanced Features
//! //! - [`join`]: Multi-stream join operations
//!
//! ### Multi-Query Trend Aggregation
//! - [`greta`]: GRETA baseline aggregation (VLDB 2017)
//! - [`hamlet`]: Hamlet shared aggregation with graphlets (SIGMOD 2021) - **recommended**
//! - [`zdd_unified`]: ZDD-based aggregation (experimental, for research)
//!
//! ### I/O & Connectors
//! - [`connector`]: Source and sink connectors (MQTT, HTTP, Kafka)
//! - [`sink`]: Output sinks (console, file, HTTP webhook)
//! - [`event_file`]: Event file parsing and streaming
//!
//! ### Infrastructure
//! - [`worker_pool`]: Parallel processing with backpressure
//! - [`persistence`]: State checkpointing (RocksDB, memory)
//! - [`metrics`]: Prometheus metrics
//! - [`timer`]: Timer management for timeouts
//! - [`simulator`]: Event simulation for demos
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use varpulis_runtime::{Engine, Event};
//! use varpulis_parser::parse;
//! use tokio::sync::mpsc;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Parse a VPL program
//!     let program = parse(r#"
//!         stream HighTemp = SensorReading
//!             .where(temperature > 100)
//!             .emit(sensor: sensor_id, temp: temperature)
//!     "#).unwrap();
//!
//!     // Create engine with output channel
//!     let (output_tx, mut output_rx) = mpsc::channel(100);
//!     let mut engine = Engine::new(output_tx);
//!     engine.load(&program).unwrap();
//!
//!     // Process an event
//!     let event = Event::new("SensorReading")
//!         .with_field("temperature", 105.5)
//!         .with_field("sensor_id", "S1");
//!     engine.process(event).await.unwrap();
//!
//!     // Receive output event
//!     if let Some(output) = output_rx.recv().await {
//!         println!("Output: {} {:?}", output.event_type, output.data);
//!     }
//! }
//! ```
//!
//! ## Performance
//!
//! - SIMD-optimized aggregations (4x speedup with AVX2)
//! - Incremental aggregation for sliding windows
//! - Zero-copy event sharing via `Arc<Event>`
//! - Parallel worker pools with backpressure
//!
//! ## See Also
//!
//! - [`varpulis_core`](../varpulis_core): Core types and AST
//! - [`varpulis_parser`](../varpulis_parser): Parsing VPL
//! - [`varpulis_cli`](../varpulis_cli): Command-line interface

// ---- Core modules (always available, no async runtime needed) ----
pub mod aggregation;
#[cfg(feature = "arrow")]
pub mod arrow_bridge;
pub mod codec;
pub mod columnar;
pub mod limits;
pub use varpulis_dead_letter as dead_letter;
pub mod engine;
pub mod event;
pub mod event_file;
pub mod greta;
pub use varpulis_hamlet as hamlet;
pub mod health;
pub mod join;
pub mod persistence;
pub use varpulis_pst as pst;
pub use varpulis_sase as sase;
pub mod sase_persistence;
pub mod scoring;
pub mod sequence;
pub use varpulis_simd as simd;
pub mod udf;
pub mod vpl_test;
#[cfg(feature = "wasm-udf")]
pub mod wasm_udf;
pub mod watermark;
pub mod window;
pub mod zdd_unified;

// ---- Async-runtime modules (require tokio, not available in WASM) ----
#[cfg(feature = "async-runtime")]
pub mod backpressure;
#[cfg(feature = "async-runtime")]
pub mod context;
#[cfg(feature = "async-runtime")]
pub use varpulis_enrichment as enrichment;
#[cfg(feature = "async-runtime")]
pub mod interactive;
#[cfg(feature = "async-runtime")]
pub mod metrics;
#[cfg(feature = "async-runtime")]
pub mod simulator;
#[cfg(feature = "async-runtime")]
pub mod sink;
#[cfg(feature = "async-runtime")]
pub mod stream;
#[cfg(feature = "async-runtime")]
pub mod tenant;
#[cfg(feature = "async-runtime")]
pub mod testing;
#[cfg(feature = "async-runtime")]
pub mod timer;
#[cfg(feature = "async-runtime")]
pub mod worker_pool;

// ---- Core re-exports (always available) ----
pub use columnar::{Column, ColumnarAccess, ColumnarBuffer, ColumnarCheckpoint};
// ---- Async-runtime re-exports (require tokio) ----
#[cfg(feature = "async-runtime")]
pub use context::{
    CheckpointAck, CheckpointBarrier, CheckpointCoordinator, ContextConfig, ContextMap,
    ContextMessage, ContextOrchestrator, ContextRuntime, DispatchError, EventTypeRouter,
};
pub use engine::error::EngineError;
#[cfg(feature = "async-runtime")]
pub use engine::EngineBuilder;
pub use engine::{Engine, ReloadReport, SourceBinding};
pub use event::{Event, SharedEvent};
pub use event_file::StreamingEventReader;
#[cfg(feature = "async-runtime")]
pub use metrics::Metrics;
// Persistence exports (always available, RocksDB impl requires "persistence" feature)
#[cfg(feature = "persistence")]
pub use persistence::RocksDbStore;
pub use persistence::{
    Checkpoint, CheckpointConfig, CheckpointManager, FileStore, MemoryStore, StateStore, StoreError,
};
#[cfg(feature = "async-runtime")]
pub use sink::{ConsoleSink, FileSink, HttpSink, MultiSink};
#[cfg(feature = "async-runtime")]
pub use stream::Stream;
#[cfg(feature = "async-runtime")]
pub use tenant::{
    hash_api_key, shared_tenant_manager, shared_tenant_manager_with_store, Pipeline,
    PipelineSnapshot, PipelineStatus, SharedTenantManager, Tenant, TenantError, TenantId,
    TenantManager, TenantQuota, TenantSnapshot, TenantUsage,
};
#[cfg(feature = "async-runtime")]
pub use timer::{spawn_timer, TimerManager};
#[cfg(feature = "async-runtime")]
pub use varpulis_connectors as connector;
#[cfg(feature = "async-runtime")]
pub use varpulis_connectors::{circuit_breaker, converter, Sink, SinkError};
pub use window::{
    CountWindow, DelayBuffer, IncrementalAggregates, IncrementalSlidingWindow,
    PartitionedDelayBuffer, PartitionedPreviousValueTracker, PartitionedSessionWindow,
    PartitionedSlidingWindow, PartitionedTumblingWindow, PreviousValueTracker, SessionWindow,
    SlidingCountWindow, SlidingWindow, TumblingWindow,
};
#[cfg(feature = "async-runtime")]
pub use worker_pool::{
    BackpressureStrategy, PoolBackpressureError, WorkerPool, WorkerPoolConfig, WorkerPoolMetrics,
    WorkerState, WorkerStatus,
};
