//! Main execution engine for Varpulis
//!
//! This module provides the core engine that processes events and executes
//! stream definitions written in VPL.

#[cfg(feature = "async-runtime")]
mod builder;
mod compilation;
pub mod compiler;
mod dispatch;
pub mod error;
pub mod evaluator;
pub mod graph;
mod pattern_analyzer;
pub mod physical_plan;
mod pipeline;
pub mod planner;
mod router;
#[cfg(feature = "async-runtime")]
mod sink_factory;
pub mod topology;
pub mod topology_builder;
pub mod trace;
mod types;

// Re-export public types
use std::sync::Arc;

#[cfg(feature = "async-runtime")]
pub use builder::EngineBuilder;
use chrono::{DateTime, Duration, Utc};
// Re-export evaluator for use by other modules (e.g., SASE+)
pub use evaluator::eval_filter_expr;
use rustc_hash::{FxHashMap, FxHashSet};
#[cfg(feature = "async-runtime")]
pub use sink_factory::SinkConnectorAdapter;
#[cfg(feature = "async-runtime")]
use tokio::sync::mpsc;
use tracing::{info, warn};
// Re-export NamedPattern from types
pub use types::NamedPattern;
#[cfg(feature = "arrow")]
pub use types::WindowedColumnarAggregateState;
pub use types::{
    AggregatorState, EngineConfig, EngineMetrics, PartitionedAggregatorState, ReloadReport,
    SourceBinding, UserFunction,
};
// Re-export internal types for use within the engine module
use types::{RuntimeOp, RuntimeSource, StreamDefinition, WindowType};
use varpulis_core::ast::{ConfigItem, Program, Stmt};
use varpulis_core::Value;

#[cfg(feature = "async-runtime")]
use crate::connector;
#[cfg(feature = "async-runtime")]
use crate::context::ContextMap;
use crate::event::{Event, SharedEvent};
#[cfg(feature = "async-runtime")]
use crate::metrics::Metrics;
use crate::sase_persistence::SaseCheckpointExt;
use crate::sequence::SequenceContext;
use crate::udf::UdfRegistry;
use crate::watermark::PerSourceWatermarkTracker;
use crate::window::CountWindow;

/// Output channel type enumeration for zero-copy or owned event sending.
/// Only available with async-runtime (requires tokio mpsc channels).
#[cfg(feature = "async-runtime")]
#[derive(Debug)]
pub(super) enum OutputChannel {
    /// Legacy channel that requires cloning (for backwards compatibility)
    Owned(mpsc::Sender<Event>),
    /// Zero-copy channel using SharedEvent (Arc<Event>)
    Shared(mpsc::Sender<SharedEvent>),
}

/// The main Varpulis engine
pub struct Engine {
    /// Registered stream definitions
    pub(super) streams: FxHashMap<String, StreamDefinition>,
    /// Event routing: maps event types to stream names
    pub(super) router: router::EventRouter,
    /// User-defined functions
    pub(super) functions: FxHashMap<String, UserFunction>,
    /// Named patterns for reuse
    pub(super) patterns: FxHashMap<String, NamedPattern>,
    /// Configuration blocks (e.g., mqtt, kafka)
    pub(super) configs: FxHashMap<String, EngineConfig>,
    /// Mutable variables accessible across events
    pub(super) variables: FxHashMap<String, Value>,
    /// Tracks which variables are declared as mutable (var vs let)
    pub(super) mutable_vars: FxHashSet<String>,
    /// Declared connectors from VPL (async-runtime only)
    #[cfg(feature = "async-runtime")]
    pub(super) connectors: FxHashMap<String, connector::ConnectorConfig>,
    /// Source connector bindings from .from() declarations
    pub(super) source_bindings: Vec<SourceBinding>,
    /// Sink registry for .to() operations (async-runtime only)
    #[cfg(feature = "async-runtime")]
    pub(super) sinks: sink_factory::SinkRegistry,
    /// Output event sender (None for benchmark/quiet mode - skips cloning overhead).
    /// Only available with async-runtime (requires tokio mpsc channels).
    #[cfg(feature = "async-runtime")]
    pub(super) output_channel: Option<OutputChannel>,
    /// Collected output events buffer for sync/WASM mode (used by process_batch_sync_collect).
    pub(super) collected_outputs: Vec<Event>,
    /// Metrics
    pub(super) events_processed: u64,
    pub(super) output_events_emitted: u64,
    /// Prometheus metrics (async-runtime only)
    #[cfg(feature = "async-runtime")]
    pub(super) metrics: Option<Metrics>,
    /// Context assignments for multi-threaded execution (async-runtime only)
    #[cfg(feature = "async-runtime")]
    pub(super) context_map: ContextMap,
    /// Per-source watermark tracker for event-time processing
    pub(super) watermark_tracker: Option<PerSourceWatermarkTracker>,
    /// Last applied watermark (for detecting advances)
    pub(super) last_applied_watermark: Option<DateTime<Utc>>,
    /// Late data configurations per stream
    pub(super) late_data_configs: FxHashMap<String, types::LateDataConfig>,
    /// Context name when running inside a context thread (used for unique connector IDs)
    pub(super) context_name: Option<String>,
    /// Topic prefix for multi-tenant Kafka/MQTT isolation (prepended to all topic names)
    pub(super) topic_prefix: Option<String>,
    /// Cluster identity prefix for transactional sink IDs (e.g. Kafka `transactional.id`).
    ///
    /// When the same pipeline runs on multiple workers (replicas), each worker's
    /// sink must have a globally unique `transactional.id` — otherwise the Kafka
    /// broker's transaction coordinator fences one producer the moment another
    /// with the same id calls `init_transactions`. The cluster worker sets this
    /// prefix to `"{group_id}-{pipeline_name}-{worker_id}"` so the auto-generated
    /// id is `"varpulis-{group_id}-{pipeline_name}-{worker_id}-{name}"`. When
    /// `None`, the legacy `"varpulis-{name}"` format is used (single-process
    /// deployments — no replica conflict possible).
    pub(super) transactional_id_prefix: Option<String>,
    /// Shared Hamlet aggregators for multi-query optimization
    pub(super) shared_hamlet_aggregators:
        Vec<std::sync::Arc<std::sync::Mutex<crate::hamlet::HamletAggregator>>>,
    /// Auto-checkpointing manager (None = checkpointing disabled)
    pub(super) checkpoint_manager: Option<crate::persistence::CheckpointManager>,
    /// Async checkpoint manager for non-blocking persistence (Chandy-Lamport mode)
    #[cfg(feature = "async-runtime")]
    pub(super) async_checkpoint_manager: Option<crate::persistence::AsyncCheckpointManager>,
    /// Connector credentials store for secure profile resolution (async-runtime only)
    #[cfg(feature = "async-runtime")]
    pub(super) credentials_store: Option<Arc<connector::credentials::CredentialsStore>>,
    /// Custom DLQ file path (defaults to "varpulis-dlq.jsonl")
    pub(super) dlq_path: Option<std::path::PathBuf>,
    /// DLQ configuration (rotation limits etc.)
    pub(super) dlq_config: crate::dead_letter::DlqConfig,
    /// Shared DLQ instance (created during load())
    pub(super) dlq: Option<Arc<crate::dead_letter::DeadLetterQueue>>,
    /// Physical plan snapshot for introspection (built during load_program)
    pub(super) physical_plan: Option<physical_plan::PhysicalPlan>,
    /// Registry for native Rust UDFs (scalar + aggregate)
    pub(super) udf_registry: UdfRegistry,
    /// Pipeline trace collector (zero-cost when disabled)
    pub(super) trace_collector: trace::TraceCollector,
    /// Source offset registry: connector name → (partition → last-consumed offset).
    ///
    /// Sources (currently only Kafka) push their highest-consumed offset per
    /// partition into this map as events flow into the engine. At checkpoint
    /// time the map is snapshotted into `EngineCheckpoint::source_offsets`,
    /// and on commit the source is asked to persist those offsets to its
    /// own group-coordinator or equivalent. This is the input-side half of
    /// end-to-end exactly-once, paired with the 2PC sink interface.
    pub(super) source_offsets: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, std::collections::HashMap<i32, i64>>>,
    >,
    /// Source pause flag for distributed checkpoint barrier alignment.
    ///
    /// Set via [`pause_sources`](Self::pause_sources) when a checkpoint barrier
    /// arrives at the worker; source connectors that have cloned the handle
    /// via [`source_pause_handle`](Self::source_pause_handle) observe the flag
    /// in their poll loop and stop pulling new events from external systems
    /// (Kafka, MQTT, etc.). In-flight batches already submitted via
    /// `process_batch*` continue draining so the next checkpoint snapshot is
    /// coherent. Pause window target: <50ms (drain current 256-event batch +
    /// checkpoint + prepare).
    pub(super) source_paused: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Count of events that entered the engine via `process_batch*` (or
    /// `process` / `process_shared`) while [`source_paused`] was set.
    /// Observability hook — lets tests and operators verify that the pause
    /// signal reached the connectors. A non-zero value means a source pushed
    /// events past the pause flag, either because it was already mid-batch
    /// when the flag flipped (expected) or because it ignored the flag.
    pub(super) events_ingested_while_paused: u64,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Engine");
        s.field("streams", &self.streams.keys().collect::<Vec<_>>())
            .field("functions", &self.functions.keys().collect::<Vec<_>>())
            .field("patterns", &self.patterns.keys().collect::<Vec<_>>())
            .field("configs", &self.configs.keys().collect::<Vec<_>>());
        #[cfg(feature = "async-runtime")]
        s.field("connectors", &self.connectors.keys().collect::<Vec<_>>())
            .field("context_map", &self.context_map);
        s.field("events_processed", &self.events_processed)
            .field("output_events_emitted", &self.output_events_emitted)
            .field("context_name", &self.context_name)
            .field("topic_prefix", &self.topic_prefix)
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Create an [`EngineBuilder`] for fluent engine construction (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn builder() -> EngineBuilder {
        EngineBuilder::new()
    }

    /// Create engine with an owned output channel (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn new(output_tx: mpsc::Sender<Event>) -> Self {
        Self::new_internal(Some(OutputChannel::Owned(output_tx)))
    }

    /// Create engine without output channel (for benchmarking - skips Event cloning overhead).
    /// Available in both async and WASM builds.
    pub fn new_benchmark() -> Self {
        #[cfg(feature = "async-runtime")]
        {
            Self::new_internal(None)
        }
        #[cfg(not(feature = "async-runtime"))]
        {
            Self::new_sync()
        }
    }

    /// Create engine with optional output channel (async-runtime only, legacy API).
    #[cfg(feature = "async-runtime")]
    pub fn new_with_optional_output(output_tx: Option<mpsc::Sender<Event>>) -> Self {
        Self::new_internal(output_tx.map(OutputChannel::Owned))
    }

    /// Create engine with zero-copy SharedEvent output channel (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn new_shared(output_tx: mpsc::Sender<SharedEvent>) -> Self {
        Self::new_internal(Some(OutputChannel::Shared(output_tx)))
    }

    /// Set the connector credentials store for profile resolution (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn set_credentials_store(&mut self, store: Arc<connector::credentials::CredentialsStore>) {
        self.credentials_store = Some(store);
    }

    /// Internal constructor for async-runtime builds with output channel support.
    #[cfg(feature = "async-runtime")]
    fn new_internal(output_channel: Option<OutputChannel>) -> Self {
        Self {
            streams: FxHashMap::default(),
            router: router::EventRouter::new(),
            functions: FxHashMap::default(),
            patterns: FxHashMap::default(),
            configs: FxHashMap::default(),
            variables: FxHashMap::default(),
            mutable_vars: FxHashSet::default(),
            connectors: FxHashMap::default(),
            source_bindings: Vec::new(),
            sinks: sink_factory::SinkRegistry::new(),
            output_channel,
            events_processed: 0,
            output_events_emitted: 0,
            metrics: None,
            context_map: ContextMap::new(),
            watermark_tracker: None,
            last_applied_watermark: None,
            late_data_configs: FxHashMap::default(),
            context_name: None,
            topic_prefix: None,
            transactional_id_prefix: None,
            shared_hamlet_aggregators: Vec::new(),
            checkpoint_manager: None,
            #[cfg(feature = "async-runtime")]
            async_checkpoint_manager: None,
            credentials_store: None,
            dlq_path: None,
            dlq_config: crate::dead_letter::DlqConfig::default(),
            dlq: None,
            physical_plan: None,
            udf_registry: UdfRegistry::new(),
            trace_collector: trace::TraceCollector::new(),
            collected_outputs: Vec::new(),
            source_offsets: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            source_paused: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            events_ingested_while_paused: 0,
        }
    }

    /// Constructor for sync-only / WASM builds (no tokio, no output channel).
    /// Outputs are collected internally and returned via `process_batch_sync_collect()`.
    pub fn new_sync() -> Self {
        #[cfg(feature = "async-runtime")]
        {
            let mut engine = Self::new_internal(None);
            engine.collected_outputs = Vec::new();
            engine
        }
        #[cfg(not(feature = "async-runtime"))]
        {
            Self {
                streams: FxHashMap::default(),
                router: router::EventRouter::new(),
                functions: FxHashMap::default(),
                patterns: FxHashMap::default(),
                configs: FxHashMap::default(),
                variables: FxHashMap::default(),
                mutable_vars: FxHashSet::default(),
                source_bindings: Vec::new(),
                collected_outputs: Vec::new(),
                events_processed: 0,
                output_events_emitted: 0,
                watermark_tracker: None,
                last_applied_watermark: None,
                late_data_configs: FxHashMap::default(),
                context_name: None,
                topic_prefix: None,
                transactional_id_prefix: None,
                shared_hamlet_aggregators: Vec::new(),
                checkpoint_manager: None,
                dlq_path: None,
                dlq_config: crate::dead_letter::DlqConfig::default(),
                dlq: None,
                physical_plan: None,
                udf_registry: UdfRegistry::new(),
                trace_collector: trace::TraceCollector::new(),
                source_offsets: std::sync::Arc::new(std::sync::Mutex::new(
                    std::collections::HashMap::new(),
                )),
                source_paused: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                events_ingested_while_paused: 0,
            }
        }
    }

    /// Clone the output channel for use in engine reload (async-runtime only).
    #[cfg(feature = "async-runtime")]
    fn clone_output_channel(&self) -> Option<OutputChannel> {
        match &self.output_channel {
            Some(OutputChannel::Owned(tx)) => Some(OutputChannel::Owned(tx.clone())),
            Some(OutputChannel::Shared(tx)) => Some(OutputChannel::Shared(tx.clone())),
            None => None,
        }
    }

    /// Send an output event to the output channel (if configured).
    ///
    /// **Backpressure**: When the output channel is full, this function applies
    /// cooperative backpressure via a `try_send` retry loop with `yield_now()`.
    /// Events are NEVER silently dropped — the worker thread blocks until either
    /// the channel has space or the receiver is gone. This is critical for
    /// correctness when the consumer is slower than the producer (e.g., piping
    /// `varpulis simulate` output through `jq` or to a slow file).
    ///
    /// On multi-threaded tokio runtimes, the receiver task on another thread
    /// drains the channel while this loop yields.
    ///
    /// In benchmark mode (no output channel), this is a no-op to avoid cloning.
    /// PERF: Uses zero-copy for SharedEvent channels, clones only for legacy Owned.
    #[cfg(feature = "async-runtime")]
    #[inline]
    pub(super) fn send_output_shared(&mut self, event: &SharedEvent) {
        use tokio::sync::mpsc::error::TrySendError;
        match &self.output_channel {
            Some(OutputChannel::Shared(tx)) => {
                // PERF: Zero-copy - just increment Arc refcount
                let mut item = Arc::clone(event);
                loop {
                    match tx.try_send(item) {
                        Ok(()) => break,
                        Err(TrySendError::Full(returned)) => {
                            item = returned;
                            // Cooperative backpressure: yield to the scheduler
                            // so the receiver task can drain the channel.
                            std::thread::yield_now();
                        }
                        Err(TrySendError::Closed(_)) => {
                            warn!("output channel closed; dropping output event");
                            break;
                        }
                    }
                }
            }
            Some(OutputChannel::Owned(tx)) => {
                // Legacy path: must clone the Event
                let mut item = (**event).clone();
                loop {
                    match tx.try_send(item) {
                        Ok(()) => break,
                        Err(TrySendError::Full(returned)) => {
                            item = returned;
                            std::thread::yield_now();
                        }
                        Err(TrySendError::Closed(_)) => {
                            warn!("output channel closed; dropping output event");
                            break;
                        }
                    }
                }
            }
            None => {
                // No channel: collect into buffer for sync_collect
                self.collected_outputs.push((**event).clone());
            }
        }
    }

    /// Async variant of [`send_output_shared`] that yields to the
    /// **tokio task scheduler** (via `tokio::task::yield_now().await`)
    /// instead of the OS thread scheduler (via `std::thread::yield_now()`)
    /// when the output channel is full. The sync version deadlocks on
    /// tokio when a single `process_batch` call emits more events than
    /// the channel capacity because `std::thread::yield_now()` does not
    /// allow the drain task to be polled.
    #[cfg(feature = "async-runtime")]
    #[inline]
    #[allow(dead_code)] // Foundation for async batch dispatch — not yet wired into process_batch
    pub(super) async fn send_output_shared_async(&mut self, event: &SharedEvent) {
        use tokio::sync::mpsc::error::TrySendError;
        match &self.output_channel {
            Some(OutputChannel::Shared(tx)) => {
                let mut item = Arc::clone(event);
                loop {
                    match tx.try_send(item) {
                        Ok(()) => break,
                        Err(TrySendError::Full(returned)) => {
                            item = returned;
                            tokio::task::yield_now().await;
                        }
                        Err(TrySendError::Closed(_)) => {
                            warn!("output channel closed; dropping output event");
                            break;
                        }
                    }
                }
            }
            Some(OutputChannel::Owned(tx)) => {
                let mut item = (**event).clone();
                loop {
                    match tx.try_send(item) {
                        Ok(()) => break,
                        Err(TrySendError::Full(returned)) => {
                            item = returned;
                            tokio::task::yield_now().await;
                        }
                        Err(TrySendError::Closed(_)) => {
                            warn!("output channel closed; dropping output event");
                            break;
                        }
                    }
                }
            }
            None => {
                self.collected_outputs.push((**event).clone());
            }
        }
    }

    /// Collect output event into internal buffer (WASM/sync-only mode).
    #[cfg(not(feature = "async-runtime"))]
    #[inline]
    pub(super) fn send_output_shared(&mut self, event: &SharedEvent) {
        self.collected_outputs.push((**event).clone());
    }

    /// Send an output event to the output channel (if configured).
    ///
    /// Applies cooperative backpressure when the channel is full — see
    /// [`send_output_shared`] for details. Events are NEVER silently dropped.
    /// When no channel (benchmark/sync mode), collects into internal buffer.
    #[cfg(feature = "async-runtime")]
    #[inline]
    pub(super) fn send_output(&mut self, event: Event) {
        use tokio::sync::mpsc::error::TrySendError;
        match &self.output_channel {
            Some(OutputChannel::Shared(tx)) => {
                let mut item = Arc::new(event);
                loop {
                    match tx.try_send(item) {
                        Ok(()) => break,
                        Err(TrySendError::Full(returned)) => {
                            item = returned;
                            std::thread::yield_now();
                        }
                        Err(TrySendError::Closed(_)) => {
                            warn!("output channel closed; dropping output event");
                            break;
                        }
                    }
                }
            }
            Some(OutputChannel::Owned(tx)) => {
                let mut item = event;
                loop {
                    match tx.try_send(item) {
                        Ok(()) => break,
                        Err(TrySendError::Full(returned)) => {
                            item = returned;
                            std::thread::yield_now();
                        }
                        Err(TrySendError::Closed(_)) => {
                            warn!("output channel closed; dropping output event");
                            break;
                        }
                    }
                }
            }
            None => {
                // No channel: collect into buffer for sync_collect
                self.collected_outputs.push(event);
            }
        }
    }

    /// Collect output event into internal buffer (WASM/sync-only mode).
    #[cfg(not(feature = "async-runtime"))]
    #[inline]
    #[allow(dead_code)]
    pub(super) fn send_output(&mut self, event: Event) {
        self.collected_outputs.push(event);
    }

    /// Process events synchronously and return collected outputs directly.
    /// Used by WASM builds and tests where mpsc channels are not needed.
    pub fn process_batch_sync_collect(
        &mut self,
        events: Vec<Event>,
    ) -> Result<Vec<Event>, error::EngineError> {
        self.collected_outputs.clear();
        self.process_batch_sync(events)?;
        Ok(std::mem::take(&mut self.collected_outputs))
    }

    /// Set the context name for this engine instance.
    pub fn set_context_name(&mut self, name: &str) {
        self.context_name = Some(name.to_string());
    }

    /// Set a topic prefix for multi-tenant isolation (call before `load()`).
    ///
    /// When set, all Kafka and MQTT topic names will be prefixed with
    /// `{prefix}.` to enforce per-tenant topic isolation.
    pub fn set_topic_prefix(&mut self, prefix: &str) {
        self.topic_prefix = Some(prefix.to_string());
    }

    /// Set the transactional-id prefix for replica disambiguation (call before `load()`).
    ///
    /// The cluster worker should pass `"{group_id}-{pipeline_name}-{worker_id}"`
    /// so the auto-generated Kafka `transactional.id` becomes
    /// `"varpulis-{group_id}-{pipeline_name}-{worker_id}-{name}"` — globally
    /// unique across replicas of the same pipeline. Without this, two workers
    /// running the same pipeline would collide on `transactional.id` and the
    /// transaction coordinator would fence one of them mid-commit.
    pub fn set_transactional_id_prefix(&mut self, prefix: &str) {
        self.transactional_id_prefix = Some(prefix.to_string());
    }

    /// Enable or disable pipeline trace collection.
    ///
    /// When enabled, the engine records which streams each event is routed to,
    /// which operators pass or block the event, SASE pattern state, and emitted
    /// output events. Use [`drain_trace`](Self::drain_trace) to retrieve entries.
    pub fn set_trace_enabled(&mut self, enabled: bool) {
        self.trace_collector.set_enabled(enabled);
    }

    /// Whether pipeline trace collection is currently enabled.
    #[inline]
    pub fn is_trace_enabled(&self) -> bool {
        self.trace_collector.is_enabled()
    }

    /// Drain all collected trace entries since the last drain.
    pub fn drain_trace(&mut self) -> Vec<trace::TraceEntry> {
        self.trace_collector.drain()
    }

    /// Set a custom DLQ file path (call before `load()`).
    pub fn set_dlq_path(&mut self, path: std::path::PathBuf) {
        self.dlq_path = Some(path);
    }

    /// Set custom DLQ configuration (call before `load()`).
    pub const fn set_dlq_config(&mut self, config: crate::dead_letter::DlqConfig) {
        self.dlq_config = config;
    }

    /// Access the shared DLQ instance (created during `load()`).
    pub const fn dlq(&self) -> Option<&Arc<crate::dead_letter::DeadLetterQueue>> {
        self.dlq.as_ref()
    }

    /// Get a named pattern by name
    pub fn get_pattern(&self, name: &str) -> Option<&NamedPattern> {
        self.patterns.get(name)
    }

    /// Get all registered patterns
    pub const fn patterns(&self) -> &FxHashMap<String, NamedPattern> {
        &self.patterns
    }

    /// Get a configuration block by name
    pub fn get_config(&self, name: &str) -> Option<&EngineConfig> {
        self.configs.get(name)
    }

    /// Get a declared connector by name (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn get_connector(&self, name: &str) -> Option<&connector::ConnectorConfig> {
        self.connectors.get(name)
    }

    /// Get all declared connector configs (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub const fn connector_configs(&self) -> &FxHashMap<String, connector::ConnectorConfig> {
        &self.connectors
    }

    /// Get source connector bindings from .from() declarations
    pub fn source_bindings(&self) -> &[SourceBinding] {
        &self.source_bindings
    }

    /// Get a variable value by name
    pub fn get_variable(&self, name: &str) -> Option<&Value> {
        self.variables.get(name)
    }

    /// Set a variable value (must be mutable or new)
    pub fn set_variable(&mut self, name: &str, value: Value) -> Result<(), error::EngineError> {
        if self.variables.contains_key(name) && !self.mutable_vars.contains(name) {
            return Err(error::EngineError::Compilation(format!(
                "Cannot assign to immutable variable '{name}'. Use 'var' instead of 'let' to declare mutable variables."
            )));
        }
        self.variables.insert(name.to_string(), value);
        Ok(())
    }

    /// Get all variables (for debugging/testing)
    pub const fn variables(&self) -> &FxHashMap<String, Value> {
        &self.variables
    }

    /// Get the context map (for orchestrator setup, async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub const fn context_map(&self) -> &ContextMap {
        &self.context_map
    }

    /// Check if the loaded program declares any contexts (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn has_contexts(&self) -> bool {
        self.context_map.has_contexts()
    }

    /// Enable Prometheus metrics (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Add a programmatic filter to a stream using a closure
    pub fn add_filter<F>(&mut self, stream_name: &str, filter: F) -> Result<(), error::EngineError>
    where
        F: Fn(&Event) -> bool + Send + Sync + 'static,
    {
        if let Some(stream) = self.streams.get_mut(stream_name) {
            let wrapped = move |e: &SharedEvent| filter(e.as_ref());
            stream
                .operations
                .insert(0, RuntimeOp::WhereClosure(Box::new(wrapped)));
            Ok(())
        } else {
            Err(error::EngineError::StreamNotFound(stream_name.to_string()))
        }
    }

    /// Load a program into the engine with semantic validation.
    pub fn load_with_source(
        &mut self,
        source: &str,
        program: &Program,
    ) -> Result<(), error::EngineError> {
        let validation = varpulis_core::validate::validate(source, program);
        if validation.has_errors() {
            return Err(error::EngineError::Compilation(validation.format(source)));
        }
        for warning in validation
            .diagnostics
            .iter()
            .filter(|d| d.severity == varpulis_core::validate::Severity::Warning)
        {
            warn!("{}", warning.message);
        }
        self.load_program(program)
    }

    /// Load a program into the engine (no semantic validation).
    pub fn load(&mut self, program: &Program) -> Result<(), error::EngineError> {
        self.load_program(program)
    }

    fn load_program(&mut self, program: &Program) -> Result<(), error::EngineError> {
        for stmt in &program.statements {
            match &stmt.node {
                Stmt::StreamDecl {
                    name, source, ops, ..
                } => {
                    self.register_stream(name, source, ops)?;
                }
                Stmt::EventDecl { name, fields, .. } => {
                    info!(
                        "Registered event type: {} with {} fields",
                        name,
                        fields.len()
                    );
                }
                Stmt::FnDecl {
                    name,
                    params,
                    ret,
                    body,
                } => {
                    let user_fn = UserFunction {
                        name: name.clone(),
                        params: params
                            .iter()
                            .map(|p| (p.name.clone(), p.ty.clone()))
                            .collect(),
                        return_type: ret.clone(),
                        body: body.clone(),
                    };
                    info!(
                        "Registered function: {}({} params)",
                        name,
                        user_fn.params.len()
                    );
                    self.functions.insert(name.clone(), user_fn);
                }
                Stmt::Config { name, items } => {
                    warn!(
                        "DEPRECATED: 'config {}' block syntax is deprecated. \
                         Use 'connector' declarations instead: \
                         connector MyConn = {} (...)",
                        name, name
                    );
                    let mut values = std::collections::HashMap::new();
                    for item in items {
                        if let ConfigItem::Value(key, val) = item {
                            values.insert(key.clone(), val.clone());
                        }
                    }
                    info!(
                        "Registered config block: {} with {} items",
                        name,
                        values.len()
                    );
                    self.configs.insert(
                        name.clone(),
                        EngineConfig {
                            name: name.clone(),
                            values,
                        },
                    );
                }
                Stmt::PatternDecl {
                    name,
                    expr,
                    within,
                    partition_by,
                } => {
                    let named_pattern = NamedPattern {
                        name: name.clone(),
                        expr: expr.clone(),
                        within: within.clone(),
                        partition_by: partition_by.clone(),
                    };
                    info!(
                        "Registered SASE+ pattern: {} (within: {}, partition: {})",
                        name,
                        within.is_some(),
                        partition_by.is_some()
                    );
                    self.patterns.insert(name.clone(), named_pattern);
                }
                Stmt::Import { path, alias } => {
                    warn!(
                        "Unresolved import '{}' (alias: {:?}) — imports must be resolved before engine.load()",
                        path, alias
                    );
                }
                #[cfg(feature = "wasm-udf")]
                Stmt::ImportWasmFn {
                    name,
                    params,
                    ret,
                    wasm_path,
                } => {
                    use crate::udf::{Signature, TypeConstraint};

                    let wasm_bytes = std::fs::read(wasm_path).map_err(|e| {
                        error::EngineError::Compilation(format!(
                            "Failed to read WASM module '{}': {e}",
                            wasm_path
                        ))
                    })?;

                    let input_types: Vec<TypeConstraint> = params
                        .iter()
                        .map(|p| TypeConstraint::Exact(p.ty.clone()))
                        .collect();
                    let return_type = ret.clone().unwrap_or(varpulis_core::Type::Any);
                    let signature = Signature {
                        input_types,
                        return_type,
                        variadic: None,
                    };

                    let udf = crate::wasm_udf::WasmScalarUDF::new(
                        name,
                        &wasm_bytes,
                        signature,
                        Some(1_000_000), // Default fuel limit
                    )
                    .map_err(|e| {
                        error::EngineError::Compilation(format!(
                            "Failed to load WASM UDF '{}': {e}",
                            name
                        ))
                    })?;

                    self.udf_registry.register_scalar(std::sync::Arc::new(udf));
                    info!("Registered WASM UDF '{name}' from {wasm_path}");
                }
                #[cfg(not(feature = "wasm-udf"))]
                Stmt::ImportWasmFn { name, .. } => {
                    return Err(error::EngineError::Compilation(format!(
                        "WASM UDF '{}' requires the 'wasm-udf' feature",
                        name
                    )));
                }
                Stmt::VarDecl {
                    mutable,
                    name,
                    value,
                    ..
                } => {
                    let dummy_event = Event::new("__init__");
                    let empty_ctx = SequenceContext::new();
                    let initial_value = evaluator::eval_expr_with_functions(
                        value,
                        &dummy_event,
                        &empty_ctx,
                        &self.functions,
                        &self.variables,
                    )
                    .ok_or_else(|| {
                        error::EngineError::Compilation(format!(
                            "Failed to evaluate initial value for variable '{name}'"
                        ))
                    })?;

                    info!(
                        "Registered {} variable: {} = {:?}",
                        if *mutable { "mutable" } else { "immutable" },
                        name,
                        initial_value
                    );

                    self.variables.insert(name.clone(), initial_value);
                    if *mutable {
                        self.mutable_vars.insert(name.clone());
                    }
                }
                Stmt::Assignment { name, value } => {
                    let dummy_event = Event::new("__assign__");
                    let empty_ctx = SequenceContext::new();
                    let new_value = evaluator::eval_expr_with_functions(
                        value,
                        &dummy_event,
                        &empty_ctx,
                        &self.functions,
                        &self.variables,
                    )
                    .ok_or_else(|| {
                        error::EngineError::Compilation(format!(
                            "Failed to evaluate assignment value for '{name}'"
                        ))
                    })?;

                    if self.variables.contains_key(name) && !self.mutable_vars.contains(name) {
                        return Err(error::EngineError::Compilation(format!(
                            "Cannot assign to immutable variable '{name}'. Use 'var' instead of 'let'."
                        )));
                    }

                    if !self.variables.contains_key(name) {
                        self.mutable_vars.insert(name.clone());
                    }

                    info!("Assigned variable: {} = {:?}", name, new_value);
                    self.variables.insert(name.clone(), new_value);
                }
                #[cfg(feature = "async-runtime")]
                Stmt::ContextDecl { name, cores } => {
                    use crate::context::ContextConfig;
                    info!("Registered context: {} (cores: {:?})", name, cores);
                    self.context_map.register_context(ContextConfig {
                        name: name.clone(),
                        cores: cores.clone(),
                    });
                }
                #[cfg(not(feature = "async-runtime"))]
                Stmt::ContextDecl { .. } => {
                    warn!("Context declarations require async-runtime feature; ignoring");
                }
                #[cfg(feature = "async-runtime")]
                Stmt::ConnectorDecl {
                    name,
                    connector_type,
                    params,
                } => {
                    let mut config =
                        sink_factory::connector_params_to_config(connector_type, params);

                    // Resolve credentials profile if specified
                    if let Some(profile_name) = config.properties.swap_remove("profile") {
                        if let Some(ref store) = self.credentials_store {
                            store
                                .merge_profile(&profile_name, &mut config)
                                .map_err(|e| {
                                    error::EngineError::Compilation(format!(
                                        "Failed to resolve credentials profile '{}' for connector '{}': {}",
                                        profile_name, name, e
                                    ))
                                })?;
                            info!(
                                "Registered connector: {} (type: {}, profile: {})",
                                name, connector_type, profile_name
                            );
                        } else {
                            return Err(error::EngineError::Compilation(format!(
                                "Connector '{}' references profile '{}' but no credentials file was provided. \
                                 Use --credentials or set VARPULIS_CREDENTIALS.",
                                name, profile_name
                            )));
                        }
                    } else {
                        info!("Registered connector: {} (type: {})", name, connector_type);
                    }

                    self.connectors.insert(name.clone(), config);
                }
                #[cfg(not(feature = "async-runtime"))]
                Stmt::ConnectorDecl { name, .. } => {
                    warn!(
                        "Connector '{}' declarations require async-runtime feature; ignoring",
                        name
                    );
                }
                _ => {
                    tracing::debug!("Skipping statement: {:?}", stmt.node);
                }
            }
        }

        // Build sinks and connectors (async-runtime only — requires tokio for async I/O)
        #[cfg(feature = "async-runtime")]
        {
            // Collect all sink_keys actually referenced by stream operations
            let mut referenced_sink_keys: FxHashSet<String> = FxHashSet::default();
            let mut topic_overrides: Vec<(String, String, String)> = Vec::new();
            for stream in self.streams.values() {
                for op in &stream.operations {
                    if let RuntimeOp::To(to_config) = op {
                        referenced_sink_keys.insert(to_config.sink_key.clone());
                        match &to_config.topic {
                            Some(types::TopicSpec::Static(topic)) => {
                                topic_overrides.push((
                                    to_config.sink_key.clone(),
                                    to_config.connector_name.clone(),
                                    topic.clone(),
                                ));
                            }
                            Some(types::TopicSpec::Dynamic(_)) => {
                                // Dynamic topics use the base connector — ensure it's registered
                                referenced_sink_keys.insert(to_config.connector_name.clone());
                            }
                            None => {}
                        }
                    }
                }
            }

            // Build sinks using the registry
            self.sinks.build_from_connectors(
                &self.connectors,
                &referenced_sink_keys,
                &topic_overrides,
                self.context_name.as_deref(),
                self.topic_prefix.as_deref(),
                self.transactional_id_prefix.as_deref(),
            );

            // Wrap sinks with circuit breaker + DLQ when sink operations exist
            if !self.sinks.cache().is_empty() {
                let dlq_path = self
                    .dlq_path
                    .clone()
                    .unwrap_or_else(|| std::path::PathBuf::from("varpulis-dlq.jsonl"));
                let dlq = crate::dead_letter::DeadLetterQueue::open_with_config(
                    &dlq_path,
                    self.dlq_config.clone(),
                )
                .map(Arc::new)
                .ok();
                if dlq.is_some() {
                    tracing::info!(
                        "Dead letter queue enabled at {} for {} sink(s)",
                        dlq_path.display(),
                        self.sinks.cache().len()
                    );
                }
                self.dlq = dlq.clone();
                self.sinks.wrap_with_resilience(
                    crate::circuit_breaker::CircuitBreakerConfig::default(),
                    dlq,
                    self.metrics.clone(),
                );
            }
        }

        // Phase 2: Detect multi-query Hamlet sharing opportunities
        self.setup_hamlet_sharing();

        // Build physical plan snapshot for introspection
        let mut plan = physical_plan::PhysicalPlan::new();
        let mut stream_event_types: FxHashMap<String, Vec<String>> = FxHashMap::default();
        for (event_type, targets) in self.router.all_routes() {
            for target in targets.iter() {
                stream_event_types
                    .entry(target.clone())
                    .or_default()
                    .push(event_type.clone());
            }
        }
        for (name, stream_def) in &self.streams {
            let op_summary = stream_def
                .operations
                .iter()
                .map(|op| op.summary_name())
                .collect::<Vec<_>>()
                .join(" → ");
            plan.add_stream(physical_plan::PhysicalStream {
                name: name.clone(),
                operation_count: stream_def.operations.len(),
                operation_summary: op_summary,
                logical_id: plan.stream_count() as u32,
                registered_event_types: stream_event_types
                    .remove(name.as_str())
                    .unwrap_or_default(),
            });
        }
        self.physical_plan = Some(plan);

        Ok(())
    }

    /// Detect overlapping Kleene patterns across streams using `.trend_aggregate()`
    /// and replace per-stream aggregators with shared ones.
    fn setup_hamlet_sharing(&mut self) {
        use crate::hamlet::template::TemplateBuilder;
        use crate::hamlet::{HamletAggregator, HamletConfig, QueryRegistration};

        let hamlet_streams: Vec<String> = self
            .streams
            .iter()
            .filter(|(_, s)| s.hamlet_aggregator.is_some())
            .map(|(name, _)| name.clone())
            .collect();

        if hamlet_streams.len() < 2 {
            return;
        }

        let mut kleene_groups: FxHashMap<Vec<String>, Vec<String>> = FxHashMap::default();

        for stream_name in &hamlet_streams {
            if let Some(stream) = self.streams.get(stream_name) {
                let mut kleene_types = Vec::new();
                for op in &stream.operations {
                    if let RuntimeOp::TrendAggregate(_) = op {
                        if let Some(ref agg) = stream.hamlet_aggregator {
                            let tmpl = agg.merged_template();
                            for query in agg.registered_queries() {
                                for &kt in &query.kleene_types {
                                    let type_name =
                                        tmpl.type_name(kt).unwrap_or("unknown").to_string();
                                    if !kleene_types.contains(&type_name) {
                                        kleene_types.push(type_name);
                                    }
                                }
                            }
                        }
                    }
                }
                kleene_types.sort();
                kleene_groups
                    .entry(kleene_types)
                    .or_default()
                    .push(stream_name.clone());
            }
        }

        for (kleene_key, group_streams) in &kleene_groups {
            if group_streams.len() < 2 || kleene_key.is_empty() {
                continue;
            }

            info!(
                "Hamlet sharing detected: {} streams share Kleene patterns {:?}: {:?}",
                group_streams.len(),
                kleene_key,
                group_streams,
            );

            let mut builder = TemplateBuilder::new();
            type SharingEntry = (
                String,
                QueryRegistration,
                Vec<(String, crate::greta::GretaAggregate)>,
            );
            let mut all_registrations: Vec<SharingEntry> = Vec::new();
            let mut next_query_id: crate::greta::QueryId = 0;

            for stream_name in group_streams {
                if let Some(stream) = self.streams.get(stream_name) {
                    if let Some(ref agg) = stream.hamlet_aggregator {
                        let tmpl = agg.merged_template();
                        for query in agg.registered_queries() {
                            let new_id = next_query_id;
                            next_query_id += 1;

                            // Resolve type indices to actual event type names
                            let event_names: Vec<String> = query
                                .event_types
                                .iter()
                                .map(|&idx| tmpl.type_name(idx).unwrap_or("unknown").to_string())
                                .collect();
                            let name_strs: Vec<&str> =
                                event_names.iter().map(String::as_str).collect();

                            // Record base state before adding states for this query
                            let base_state = builder.num_states() as u16;
                            builder.add_sequence(new_id, &name_strs);

                            for &kt in &query.kleene_types {
                                let type_name = tmpl.type_name(kt).unwrap_or("unknown").to_string();
                                let position = event_names
                                    .iter()
                                    .position(|n| *n == type_name)
                                    .unwrap_or(0);
                                // Self-loop at target state (base + position + 1)
                                // since add_sequence creates states starting at base_state
                                let kleene_state = base_state + position as u16 + 1;
                                builder.add_kleene(new_id, &type_name, kleene_state);
                            }

                            let fields: Vec<(String, crate::greta::GretaAggregate)> = stream
                                .operations
                                .iter()
                                .find_map(|op| {
                                    if let RuntimeOp::TrendAggregate(config) = op {
                                        Some(config.fields.clone())
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or_default();

                            all_registrations.push((
                                stream_name.clone(),
                                QueryRegistration {
                                    id: new_id,
                                    event_types: query.event_types.clone(),
                                    kleene_types: query.kleene_types.clone(),
                                    aggregate: query.aggregate,
                                },
                                fields,
                            ));
                        }
                    }
                }
            }

            let template = builder.build();
            let config = HamletConfig {
                window_ms: 60_000,
                incremental: true,
                ..Default::default()
            };
            let mut shared_agg = HamletAggregator::new(config, template);

            for (_, registration, _) in &all_registrations {
                shared_agg.register_query(registration.clone());
            }

            let shared_ref = std::sync::Arc::new(std::sync::Mutex::new(shared_agg));
            self.shared_hamlet_aggregators.push(shared_ref.clone());

            for (stream_name, registration, fields) in &all_registrations {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    stream.hamlet_aggregator = None;
                    stream.shared_hamlet_ref = Some(shared_ref.clone());

                    for op in &mut stream.operations {
                        if let RuntimeOp::TrendAggregate(config) = op {
                            config.query_id = registration.id;
                            config.fields = fields.clone();
                        }
                    }
                }
            }

            info!(
                "Created shared Hamlet aggregator with {} queries",
                next_query_id,
            );
        }
    }

    /// Connect all sinks that require explicit connection (async-runtime only).
    #[cfg(feature = "async-runtime")]
    #[tracing::instrument(skip(self))]
    pub async fn connect_sinks(&self) -> Result<(), error::EngineError> {
        self.sinks
            .connect_all()
            .await
            .map_err(error::EngineError::Pipeline)
    }

    /// Call `prepare_commit` on all exactly-once sinks for the given checkpoint epoch.
    #[cfg(feature = "async-runtime")]
    pub async fn prepare_commit_sinks(&self, checkpoint_id: u64) {
        for (key, sink) in self.sinks.cache() {
            if sink.supports_exactly_once() {
                if let Err(e) = sink.prepare_commit(checkpoint_id).await {
                    tracing::error!("Sink '{key}' prepare_commit failed: {e}");
                }
            }
        }
    }

    /// Call `commit` on all exactly-once sinks, then `begin_epoch` for the next epoch.
    #[cfg(feature = "async-runtime")]
    pub async fn commit_sinks(&self, checkpoint_id: u64) {
        for (key, sink) in self.sinks.cache() {
            if sink.supports_exactly_once() {
                if let Err(e) = sink.commit(checkpoint_id).await {
                    tracing::error!("Sink '{key}' commit failed: {e}");
                }
                // Begin the next epoch immediately after commit
                if let Err(e) = sink.begin_epoch(checkpoint_id + 1).await {
                    tracing::error!("Sink '{key}' begin_epoch failed: {e}");
                }
            }
        }
    }

    /// Call `abort` on all exactly-once sinks (recovery path).
    #[cfg(feature = "async-runtime")]
    pub async fn abort_sinks(&self, checkpoint_id: u64) {
        for (key, sink) in self.sinks.cache() {
            if sink.supports_exactly_once() {
                if let Err(e) = sink.abort(checkpoint_id).await {
                    tracing::error!("Sink '{key}' abort failed: {e}");
                }
            }
        }
    }

    /// Call `begin_epoch` on all exactly-once sinks to start a transaction epoch.
    ///
    /// Must be called once after sinks are connected but before events start
    /// flowing so that the first events land inside an open transaction.
    /// Subsequent epochs are opened by `commit_sinks(id)` which calls
    /// `begin_epoch(id + 1)` after each `commit(id)`.
    #[cfg(feature = "async-runtime")]
    pub async fn begin_epoch_sinks(&self, checkpoint_id: u64) {
        for (key, sink) in self.sinks.cache() {
            if sink.supports_exactly_once() {
                if let Err(e) = sink.begin_epoch(checkpoint_id).await {
                    tracing::error!("Sink '{key}' begin_epoch failed: {e}");
                }
            }
        }
    }

    /// Inject a pre-built sink into the engine's registry (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn inject_sink(&mut self, key: &str, sink: Arc<dyn crate::sink::Sink>) {
        self.sinks.insert(key.to_string(), sink);
    }

    /// Check whether a given key has a registered sink (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn has_sink(&self, key: &str) -> bool {
        self.sinks.cache().contains_key(key)
    }

    /// Return all sink keys that belong to a given connector name (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn sink_keys_for_connector(&self, connector_name: &str) -> Vec<String> {
        let prefix = format!("{connector_name}::");
        self.sinks
            .cache()
            .keys()
            .filter(|k| *k == connector_name || k.starts_with(&prefix))
            .cloned()
            .collect()
    }

    // =========================================================================
    // Query / Introspection
    // =========================================================================

    /// Check if any registered stream uses `.to()` or `.enrich()` operations.
    pub fn has_sink_operations(&self) -> bool {
        self.streams.values().any(|s| {
            s.operations
                .iter()
                .any(|op| matches!(op, RuntimeOp::To(_) | RuntimeOp::Enrich(_)))
        })
    }

    /// Returns (events_in, events_out) counters for this engine.
    pub const fn event_counters(&self) -> (u64, u64) {
        (self.events_processed, self.output_events_emitted)
    }

    /// Check if all streams are stateless (safe for round-robin distribution).
    pub fn is_stateless(&self) -> bool {
        self.streams.values().all(|s| {
            s.sase_engine.is_none()
                && s.join_buffer.is_none()
                && s.hamlet_aggregator.is_none()
                && s.shared_hamlet_ref.is_none()
                && s.operations.iter().all(|op| {
                    matches!(
                        op,
                        RuntimeOp::WhereExpr(_)
                            | RuntimeOp::WhereClosure(_)
                            | RuntimeOp::Select(_)
                            | RuntimeOp::Emit(_)
                            | RuntimeOp::EmitExpr(_)
                            | RuntimeOp::Print(_)
                            | RuntimeOp::Log(_)
                            | RuntimeOp::Having(_)
                            | RuntimeOp::Process(_)
                            | RuntimeOp::Pattern(_)
                            | RuntimeOp::To(_)
                    )
                })
        })
    }

    /// Return the partition key used by partitioned operations, if any.
    pub fn partition_key(&self) -> Option<String> {
        for stream in self.streams.values() {
            if let Some(ref sase) = stream.sase_engine {
                if let Some(key) = sase.partition_by() {
                    return Some(key.to_string());
                }
            }
            for op in &stream.operations {
                match op {
                    RuntimeOp::PartitionedWindow(pw) => return Some(pw.partition_key.clone()),
                    RuntimeOp::PartitionedSlidingCountWindow(pw) => {
                        return Some(pw.partition_key.clone())
                    }
                    RuntimeOp::PartitionedAggregate(pa) => return Some(pa.partition_key.clone()),
                    _ => {}
                }
            }
            if stream.sase_engine.is_some() {
                for op in &stream.operations {
                    if let RuntimeOp::WhereExpr(expr) = op {
                        if let Some(key) = compilation::extract_equality_join_key(expr) {
                            return Some(key);
                        }
                    }
                }
            }
        }
        None
    }

    /// Check if any registered stream has session windows.
    pub fn has_session_windows(&self) -> bool {
        self.streams.values().any(|s| {
            s.operations.iter().any(|op| {
                matches!(
                    op,
                    RuntimeOp::Window(WindowType::Session(_))
                        | RuntimeOp::Window(WindowType::PartitionedSession(_))
                )
            })
        })
    }

    /// Return the smallest session gap across all streams (used as sweep interval).
    pub fn min_session_gap(&self) -> Option<chrono::Duration> {
        let mut min_gap: Option<chrono::Duration> = None;
        for stream in self.streams.values() {
            for op in &stream.operations {
                if let RuntimeOp::Window(window) = op {
                    let gap = match window {
                        WindowType::Session(w) => Some(w.gap()),
                        WindowType::PartitionedSession(w) => Some(w.gap()),
                        _ => None,
                    };
                    if let Some(g) = gap {
                        min_gap = Some(match min_gap {
                            Some(current) if g < current => g,
                            Some(current) => current,
                            None => g,
                        });
                    }
                }
            }
        }
        min_gap
    }

    /// Get metrics
    pub fn metrics(&self) -> EngineMetrics {
        EngineMetrics {
            events_processed: self.events_processed,
            output_events_emitted: self.output_events_emitted,
            streams_count: self.streams.len(),
        }
    }

    /// Get the names of all loaded streams.
    pub fn stream_names(&self) -> Vec<&str> {
        self.streams.keys().map(|s| s.as_str()).collect()
    }

    /// Get the physical plan summary (available after `load()`).
    pub fn physical_plan_summary(&self) -> Option<String> {
        self.physical_plan.as_ref().map(|p| p.summary())
    }

    pub fn explain(&self, program: &Program) -> Result<String, error::EngineError> {
        let logical = planner::logical_plan(program).map_err(error::EngineError::Compilation)?;
        let optimized = varpulis_parser::optimize_plan(logical);
        Ok(optimized.explain())
    }

    /// Build a topology snapshot of the currently loaded streams and routes.
    pub fn topology(&self) -> topology::Topology {
        let mut builder = topology_builder::TopologyBuilder::new();
        for (name, stream_def) in &self.streams {
            builder = builder.add_stream(name, stream_def);
        }
        builder = builder.add_routes(self.router.all_routes());
        builder.build()
    }

    /// Get a user-defined function by name
    pub fn get_function(&self, name: &str) -> Option<&UserFunction> {
        self.functions.get(name)
    }

    /// Get all registered function names
    pub fn function_names(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }

    /// Get all timer configurations for spawning timer tasks
    pub fn get_timers(&self) -> Vec<(u64, Option<u64>, String)> {
        let mut timers = Vec::new();
        for stream in self.streams.values() {
            if let RuntimeSource::Timer(config) = &stream.source {
                timers.push((
                    config.interval_ns,
                    config.initial_delay_ns,
                    config.timer_event_type.clone(),
                ));
            }
        }
        timers
    }

    /// Register a native scalar UDF.
    pub fn register_scalar_udf(&mut self, udf: std::sync::Arc<dyn crate::udf::ScalarUDF>) {
        self.udf_registry.register_scalar(udf);
    }

    /// Register a native aggregate UDF.
    pub fn register_aggregate_udf(&mut self, udf: std::sync::Arc<dyn crate::udf::AggregateUDF>) {
        self.udf_registry.register_aggregate(udf);
    }

    /// Get a reference to the UDF registry.
    pub const fn udf_registry(&self) -> &UdfRegistry {
        &self.udf_registry
    }

    // =========================================================================
    // Hot Reload
    // =========================================================================

    /// Reload program without losing state where possible (async-runtime only).
    #[cfg(feature = "async-runtime")]
    pub fn reload(&mut self, program: &Program) -> Result<ReloadReport, error::EngineError> {
        let mut report = ReloadReport::default();

        let old_streams: FxHashSet<String> = self.streams.keys().cloned().collect();

        let mut new_engine = Self::new_internal(self.clone_output_channel());
        new_engine.credentials_store = self.credentials_store.clone();
        new_engine.load(program)?;

        let new_streams: FxHashSet<String> = new_engine.streams.keys().cloned().collect();

        for name in new_streams.difference(&old_streams) {
            report.streams_added.push(name.clone());
        }

        for name in old_streams.difference(&new_streams) {
            report.streams_removed.push(name.clone());
        }

        for name in old_streams.intersection(&new_streams) {
            let old_stream = self.streams.get(name).unwrap();
            let new_stream = new_engine.streams.get(name).unwrap();

            let source_changed = !Self::sources_compatible(&old_stream.source, &new_stream.source);
            let ops_changed = old_stream.operations.len() != new_stream.operations.len();

            if source_changed || ops_changed {
                report.streams_updated.push(name.clone());
                report.state_reset.push(name.clone());
            } else {
                report.state_preserved.push(name.clone());
            }
        }

        for name in &report.streams_removed {
            self.streams.remove(name);
        }

        self.router.clear();

        for name in &report.streams_added {
            if let Some(stream) = new_engine.streams.remove(name) {
                self.streams.insert(name.clone(), stream);
            }
        }

        for name in &report.streams_updated {
            if let Some(stream) = new_engine.streams.remove(name) {
                self.streams.insert(name.clone(), stream);
            }
        }

        let registrations: Vec<(String, String)> = self
            .streams
            .iter()
            .flat_map(|(name, stream)| {
                let mut pairs = Vec::new();
                match &stream.source {
                    RuntimeSource::EventType(et) => {
                        pairs.push((et.clone(), name.clone()));
                    }
                    RuntimeSource::Stream(s) => {
                        pairs.push((s.clone(), name.clone()));
                    }
                    RuntimeSource::Merge(sources) => {
                        for ms in sources {
                            pairs.push((ms.event_type.clone(), name.clone()));
                        }
                    }
                    RuntimeSource::Join(_) => {}
                    RuntimeSource::Timer(config) => {
                        pairs.push((config.timer_event_type.clone(), name.clone()));
                    }
                }
                pairs
            })
            .collect();

        for (event_type, stream_name) in registrations {
            self.router.add_route(&event_type, &stream_name);
        }

        self.functions = new_engine.functions;
        self.patterns = new_engine.patterns;
        self.configs = new_engine.configs;
        self.context_map = new_engine.context_map;
        self.connectors = new_engine.connectors;
        self.source_bindings = new_engine.source_bindings;
        *self.sinks.cache_mut() = std::mem::take(new_engine.sinks.cache_mut());

        for (name, value) in new_engine.variables {
            if !self.variables.contains_key(&name) {
                self.variables.insert(name.clone(), value);
                self.mutable_vars
                    .extend(new_engine.mutable_vars.iter().cloned());
            }
        }

        info!(
            "Hot reload complete: +{} -{} ~{} streams",
            report.streams_added.len(),
            report.streams_removed.len(),
            report.streams_updated.len()
        );

        Ok(report)
    }

    // =========================================================================
    // Checkpointing
    // =========================================================================

    /// Return a shared handle to the source-offset registry.
    ///
    /// Source connectors (e.g. Kafka) clone this handle and update the map
    /// under their own connector name as events flow in, so that the next
    /// checkpoint snapshot reflects exactly which input records are already
    /// reflected in engine state. On commit the driver should call the
    /// source's commit API with the snapshotted offsets to close the 2PC loop.
    pub fn source_offsets_handle(
        &self,
    ) -> std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, std::collections::HashMap<i32, i64>>>,
    > {
        self.source_offsets.clone()
    }

    // -------------------------------------------------------------------------
    // Source pause / resume (Flink-style barrier alignment)
    // -------------------------------------------------------------------------

    /// Pause source ingestion ahead of a checkpoint barrier.
    ///
    /// Source connectors that have cloned [`source_pause_handle`](Self::source_pause_handle)
    /// observe the flag in their poll loop and stop pulling new events from
    /// external systems (Kafka, MQTT, NATS, etc.). Events already in-flight
    /// — submitted via `process_batch*` before the pause — continue to drain
    /// so the next checkpoint captures a coherent snapshot.
    ///
    /// Cheap O(1) atomic store; safe to call from any thread.
    #[inline]
    pub fn pause_sources(&self) {
        self.source_paused
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Resume source ingestion after a checkpoint completes (or aborts).
    ///
    /// Counterpart to [`pause_sources`](Self::pause_sources).
    #[inline]
    pub fn resume_sources(&self) {
        self.source_paused
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Whether source ingestion is currently paused.
    #[inline]
    pub fn is_sources_paused(&self) -> bool {
        self.source_paused
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Shared handle to the source pause flag.
    ///
    /// Source connectors should clone this handle once at startup and check it
    /// before each poll batch with a `Relaxed` load — the engine flips the
    /// flag with `Release` ordering, so a single `Relaxed` load on the
    /// connector side observes any flip published by the time the next poll
    /// runs. While the flag is set, the connector should stop polling
    /// upstream (sleep briefly — the pause window targets <50ms — and
    /// re-check) so the engine can drain in-flight events and snapshot.
    pub fn source_pause_handle(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.source_paused.clone()
    }

    /// Number of events that arrived via `process_batch*` (or `process` /
    /// `process_shared`) while [`is_sources_paused`](Self::is_sources_paused)
    /// was true.
    ///
    /// Observability hook: a non-zero value is expected and benign — it
    /// represents the in-flight 256-event batch being drained after the
    /// pause flag was raised. A persistently growing value while the flag
    /// is held would mean a connector is ignoring the flag.
    #[inline]
    pub const fn events_ingested_while_paused(&self) -> u64 {
        self.events_ingested_while_paused
    }

    /// Create a checkpoint of the engine state (windows, SASE engines, joins, variables).
    pub fn create_checkpoint(&self) -> crate::persistence::EngineCheckpoint {
        use crate::persistence::{EngineCheckpoint, WindowCheckpoint};

        let mut window_states = std::collections::HashMap::new();
        let mut sase_states = std::collections::HashMap::new();
        let mut join_states = std::collections::HashMap::new();
        let mut distinct_states = std::collections::HashMap::new();
        let mut limit_states = std::collections::HashMap::new();

        for (name, stream) in &self.streams {
            for op in &stream.operations {
                match op {
                    RuntimeOp::Window(wt) => {
                        let cp = match wt {
                            WindowType::Tumbling(w) => w.checkpoint(),
                            WindowType::Sliding(w) => w.checkpoint(),
                            WindowType::Count(w) => w.checkpoint(),
                            WindowType::SlidingCount(w) => w.checkpoint(),
                            WindowType::Session(w) => w.checkpoint(),
                            WindowType::PartitionedSession(w) => w.checkpoint(),
                            WindowType::PartitionedTumbling(w) => w.checkpoint(),
                            WindowType::PartitionedSliding(w) => w.checkpoint(),
                            WindowType::BinnedSliding(w) => w.checkpoint(),
                            WindowType::PartitionedBinnedSliding(w) => w.checkpoint(),
                        };
                        window_states.insert(name.clone(), cp);
                    }
                    RuntimeOp::PartitionedWindow(pw) => {
                        let mut partitions = std::collections::HashMap::new();
                        for (key, cw) in &pw.windows {
                            let sub_cp = cw.checkpoint();
                            partitions.insert(
                                key.clone(),
                                crate::persistence::PartitionedWindowCheckpoint {
                                    events: sub_cp.events,
                                    window_start_ms: sub_cp.window_start_ms,
                                },
                            );
                        }
                        window_states.insert(
                            name.clone(),
                            WindowCheckpoint {
                                events: Vec::new(),
                                window_start_ms: None,
                                last_emit_ms: None,
                                partitions,
                            },
                        );
                    }
                    RuntimeOp::Distinct(state) => {
                        let keys: Vec<String> =
                            state.seen.iter().rev().map(|(k, ())| k.clone()).collect();
                        distinct_states.insert(
                            name.clone(),
                            crate::persistence::DistinctCheckpoint { keys },
                        );
                    }
                    RuntimeOp::Limit(state) => {
                        limit_states.insert(
                            name.clone(),
                            crate::persistence::LimitCheckpoint {
                                max: state.max,
                                count: state.count,
                            },
                        );
                    }
                    _ => {}
                }
            }

            if let Some(ref sase) = stream.sase_engine {
                sase_states.insert(name.clone(), sase.checkpoint());
            }

            if let Some(ref jb) = stream.join_buffer {
                join_states.insert(name.clone(), jb.checkpoint());
            }
        }

        let variables = self
            .variables
            .iter()
            .map(|(k, v)| (k.clone(), crate::persistence::value_to_ser(v)))
            .collect();

        let watermark_state = self.watermark_tracker.as_ref().map(|t| t.checkpoint());

        let source_offsets = self
            .source_offsets
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default();

        EngineCheckpoint {
            version: crate::persistence::CHECKPOINT_VERSION,
            window_states,
            sase_states,
            join_states,
            variables,
            events_processed: self.events_processed,
            output_events_emitted: self.output_events_emitted,
            watermark_state,
            distinct_states,
            limit_states,
            source_offsets,
        }
    }

    /// Restore engine state from a checkpoint.
    pub fn restore_checkpoint(
        &mut self,
        cp: &crate::persistence::EngineCheckpoint,
    ) -> Result<(), crate::persistence::StoreError> {
        let mut migrated = cp.clone();
        migrated.validate_and_migrate()?;

        self.events_processed = cp.events_processed;
        self.output_events_emitted = cp.output_events_emitted;

        if let Ok(mut guard) = self.source_offsets.lock() {
            *guard = cp.source_offsets.clone();
        }

        for (k, sv) in &cp.variables {
            self.variables
                .insert(k.clone(), crate::persistence::ser_to_value(sv.clone()));
        }

        for (name, stream) in &mut self.streams {
            if let Some(wcp) = cp.window_states.get(name) {
                for op in &mut stream.operations {
                    match op {
                        RuntimeOp::Window(wt) => match wt {
                            WindowType::Tumbling(w) => w.restore(wcp),
                            WindowType::Sliding(w) => w.restore(wcp),
                            WindowType::Count(w) => w.restore(wcp),
                            WindowType::SlidingCount(w) => w.restore(wcp),
                            WindowType::Session(w) => w.restore(wcp),
                            WindowType::PartitionedSession(w) => w.restore(wcp),
                            WindowType::PartitionedTumbling(w) => w.restore(wcp),
                            WindowType::PartitionedSliding(w) => w.restore(wcp),
                            WindowType::BinnedSliding(w) => w.restore(wcp),
                            WindowType::PartitionedBinnedSliding(w) => w.restore(wcp),
                        },
                        RuntimeOp::PartitionedWindow(pw) => {
                            for (key, pcp) in &wcp.partitions {
                                let sub_wcp = crate::persistence::WindowCheckpoint {
                                    events: pcp.events.clone(),
                                    window_start_ms: pcp.window_start_ms,
                                    last_emit_ms: None,
                                    partitions: std::collections::HashMap::new(),
                                };
                                let window = pw
                                    .windows
                                    .entry(key.clone())
                                    .or_insert_with(|| CountWindow::new(pw.window_size));
                                window.restore(&sub_wcp);
                            }
                        }
                        _ => {}
                    }
                }
            }

            if let Some(scp) = cp.sase_states.get(name) {
                if let Some(ref mut sase) = stream.sase_engine {
                    sase.restore(scp);
                }
            }

            if let Some(jcp) = cp.join_states.get(name) {
                if let Some(ref mut jb) = stream.join_buffer {
                    jb.restore(jcp);
                }
            }

            if let Some(dcp) = cp.distinct_states.get(name) {
                for op in &mut stream.operations {
                    if let RuntimeOp::Distinct(state) = op {
                        state.seen.clear();
                        for key in dcp.keys.iter().rev() {
                            state.seen.insert(key.clone(), ());
                        }
                    }
                }
            }

            if let Some(lcp) = cp.limit_states.get(name) {
                for op in &mut stream.operations {
                    if let RuntimeOp::Limit(state) = op {
                        state.max = lcp.max;
                        state.count = lcp.count;
                    }
                }
            }
        }

        if let Some(ref wcp) = cp.watermark_state {
            if self.watermark_tracker.is_none() {
                self.watermark_tracker = Some(PerSourceWatermarkTracker::new());
            }
            if let Some(ref mut tracker) = self.watermark_tracker {
                tracker.restore(wcp);
                self.last_applied_watermark = wcp
                    .effective_watermark_ms
                    .and_then(DateTime::from_timestamp_millis);
            }
        }

        info!(
            "Engine restored: {} events processed, {} streams with state (schema v{})",
            cp.events_processed,
            cp.window_states.len() + cp.sase_states.len() + cp.join_states.len(),
            cp.version
        );

        Ok(())
    }

    /// Enable auto-checkpointing with the given store and config.
    pub fn enable_checkpointing(
        &mut self,
        store: std::sync::Arc<dyn crate::persistence::StateStore>,
        config: crate::persistence::CheckpointConfig,
    ) -> Result<(), crate::persistence::StoreError> {
        let manager = crate::persistence::CheckpointManager::new(store, config)?;

        if let Some(cp) = manager.recover()? {
            if let Some(engine_cp) = cp.context_states.get("main") {
                self.restore_checkpoint(engine_cp)?;
                info!(
                    "Auto-restored from checkpoint {} ({} events)",
                    cp.id, cp.events_processed
                );
            }
        }

        self.checkpoint_manager = Some(manager);
        Ok(())
    }

    /// Check if a checkpoint is due and create one if needed.
    pub fn checkpoint_tick(&mut self) -> Result<(), crate::persistence::StoreError> {
        let should = self
            .checkpoint_manager
            .as_ref()
            .is_some_and(|m| m.should_checkpoint());

        if should {
            self.force_checkpoint()?;
        }
        Ok(())
    }

    /// Force an immediate checkpoint regardless of the interval.
    pub fn force_checkpoint(&mut self) -> Result<(), crate::persistence::StoreError> {
        if self.checkpoint_manager.is_none() {
            return Ok(());
        }

        let engine_cp = self.create_checkpoint();
        let events_processed = self.events_processed;

        let mut context_states = std::collections::HashMap::new();
        context_states.insert("main".to_string(), engine_cp);

        let cp = crate::persistence::Checkpoint {
            id: 0,
            timestamp_ms: 0,
            events_processed,
            window_states: std::collections::HashMap::new(),
            pattern_states: std::collections::HashMap::new(),
            metadata: std::collections::HashMap::new(),
            context_states,
        };

        self.checkpoint_manager.as_mut().unwrap().checkpoint(cp)?;
        Ok(())
    }

    /// Returns true if auto-checkpointing is enabled.
    pub const fn has_checkpointing(&self) -> bool {
        self.checkpoint_manager.is_some()
    }

    /// Enable async checkpoint persistence for single-engine mode.
    ///
    /// After calling this, `force_checkpoint_async()` will offload serialization
    /// and persistence to a background task, allowing the event loop to continue
    /// processing events immediately after state capture.
    #[cfg(feature = "async-runtime")]
    pub fn enable_async_checkpoints(&mut self) {
        if let Some(ref manager) = self.checkpoint_manager {
            let async_mgr = crate::persistence::AsyncCheckpointManager::new(
                crate::persistence::CheckpointManager::new(
                    std::sync::Arc::clone(manager.store()),
                    crate::persistence::CheckpointConfig::default(),
                )
                .expect("Failed to create async checkpoint manager"),
            );
            self.async_checkpoint_manager = Some(async_mgr);
        }
    }

    /// Force a non-blocking checkpoint: capture state synchronously (fast),
    /// then persist asynchronously in a background task.
    ///
    /// Returns a `oneshot::Receiver` that resolves when persistence is complete.
    /// The caller can continue processing events immediately.
    #[cfg(feature = "async-runtime")]
    pub fn force_checkpoint_async(
        &mut self,
    ) -> Option<tokio::sync::oneshot::Receiver<Result<(), crate::persistence::StoreError>>> {
        let async_mgr = self.async_checkpoint_manager.as_ref()?;

        // Fast synchronous state capture
        let engine_cp = self.create_checkpoint();
        let events_processed = self.events_processed;

        let mut context_states = std::collections::HashMap::new();
        context_states.insert("main".to_string(), engine_cp);

        let cp = crate::persistence::Checkpoint {
            id: 0,
            timestamp_ms: 0,
            events_processed,
            window_states: std::collections::HashMap::new(),
            pattern_states: std::collections::HashMap::new(),
            metadata: std::collections::HashMap::new(),
            context_states,
        };

        // Offload persistence to background
        Some(async_mgr.checkpoint_async(cp))
    }

    // =========================================================================
    // Watermark Management
    // =========================================================================

    /// Enable per-source watermark tracking for this engine.
    pub fn enable_watermark_tracking(&mut self) {
        if self.watermark_tracker.is_none() {
            self.watermark_tracker = Some(PerSourceWatermarkTracker::new());
        }
    }

    /// Register a source for watermark tracking with its max out-of-orderness.
    pub fn register_watermark_source(&mut self, source: &str, max_ooo: Duration) {
        if let Some(ref mut tracker) = self.watermark_tracker {
            tracker.register_source(source, max_ooo);
        }
    }

    /// Advance the watermark from an external source (async-runtime only).
    #[cfg(feature = "async-runtime")]
    #[tracing::instrument(skip(self))]
    pub async fn advance_external_watermark(
        &mut self,
        source_context: &str,
        watermark_ms: i64,
    ) -> Result<(), error::EngineError> {
        if let Some(ref mut tracker) = self.watermark_tracker {
            if let Some(wm) = DateTime::from_timestamp_millis(watermark_ms) {
                tracker.advance_source_watermark(source_context, wm);

                if let Some(new_wm) = tracker.effective_watermark() {
                    if self.last_applied_watermark.is_none_or(|last| new_wm > last) {
                        self.apply_watermark_to_windows(new_wm).await?;
                        self.last_applied_watermark = Some(new_wm);
                    }
                }
            }
        }
        Ok(())
    }

    /// Check if two runtime sources are compatible for state preservation (used by reload).
    #[cfg(feature = "async-runtime")]
    fn sources_compatible(a: &RuntimeSource, b: &RuntimeSource) -> bool {
        match (a, b) {
            (RuntimeSource::EventType(a), RuntimeSource::EventType(b)) => a == b,
            (RuntimeSource::Stream(a), RuntimeSource::Stream(b)) => a == b,
            (RuntimeSource::Timer(a), RuntimeSource::Timer(b)) => {
                a.interval_ns == b.interval_ns && a.timer_event_type == b.timer_event_type
            }
            (RuntimeSource::Merge(a), RuntimeSource::Merge(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|(x, y)| x.event_type == y.event_type)
            }
            (RuntimeSource::Join(a), RuntimeSource::Join(b)) => a == b,
            _ => false,
        }
    }
}
