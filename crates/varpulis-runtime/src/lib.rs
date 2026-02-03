//! # Varpulis Runtime
//!
//! High-performance execution engine for VarpulisQL programs.
//!
//! This crate is the heart of Varpulis, providing:
//!
//! - **Stream Processing**: Real-time event processing with filtering and transformation
//! - **SASE+ Pattern Matching**: Complex event detection with sequences, Kleene closures, and negation
//! - **Windowed Aggregations**: Time and count-based windows with SIMD-optimized aggregations
//! - **Attention Engine**: ML-inspired event correlation and anomaly detection
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
//! - [`engine`]: Main execution engine, compiles and runs VarpulisQL programs
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
//! - [`attention`]: Attention-based event correlation
//! - [`join`]: Multi-stream join operations
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
//! ```rust,ignore
//! use varpulis_runtime::{Engine, Event};
//! use varpulis_parser::parse;
//! use tokio::sync::mpsc;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Parse a VarpulisQL program
//!     let program = parse(r#"
//!         stream Alerts from SensorReading
//!             where temperature > 100
//!             emit alert("HighTemp", "Temperature exceeded threshold")
//!     "#).unwrap();
//!
//!     // Create engine with alert channel
//!     let (alert_tx, mut alert_rx) = mpsc::channel(100);
//!     let mut engine = Engine::new(alert_tx);
//!     engine.load(&program).unwrap();
//!
//!     // Process an event
//!     let event = Event::new("SensorReading")
//!         .with_field("temperature", 105.5);
//!     engine.process(event).await.unwrap();
//!
//!     // Receive alert
//!     if let Some(alert) = alert_rx.recv().await {
//!         println!("Alert: {}", alert.message);
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
//! - [`varpulis_parser`](../varpulis_parser): Parsing VarpulisQL
//! - [`varpulis_cli`](../varpulis_cli): Command-line interface

pub mod aggregation;
pub mod attention;
pub mod connector;
pub mod engine;
pub mod event;
pub mod event_file;
pub mod join;
pub mod metrics;
pub mod persistence;
pub mod sase;
pub mod sequence;
pub mod simd;
pub mod simulator;
pub mod sink;
pub mod stream;
pub mod tenant;
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
    CountWindow, DelayBuffer, IncrementalAggregates, IncrementalSlidingWindow,
    PartitionedDelayBuffer, PartitionedPreviousValueTracker, PartitionedSlidingWindow,
    PartitionedTumblingWindow, PreviousValueTracker, SlidingCountWindow, SlidingWindow,
    TumblingWindow,
};
pub use worker_pool::{
    BackpressureError, BackpressureStrategy, WorkerPool, WorkerPoolConfig, WorkerPoolMetrics,
    WorkerState, WorkerStatus,
};

// Multi-tenant SaaS support
pub use tenant::{
    shared_tenant_manager, shared_tenant_manager_with_store, Pipeline, PipelineSnapshot,
    PipelineStatus, SharedTenantManager, Tenant, TenantError, TenantId, TenantManager,
    TenantQuota, TenantSnapshot, TenantUsage,
};

// Persistence exports (always available, RocksDB impl requires "persistence" feature)
#[cfg(feature = "persistence")]
pub use persistence::RocksDbStore;
pub use persistence::{
    Checkpoint, CheckpointConfig, CheckpointManager, FileStore, MemoryStore, StateStore, StoreError,
};
