//! Type definitions for the Varpulis engine
//!
//! This module contains all the structs, enums and type definitions used by the engine.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use rustc_hash::FxHashMap;
use varpulis_core::ast::{ConfigValue, Expr, SasePatternExpr};
use varpulis_core::Value;

use crate::aggregation::Aggregator;
use crate::event::SharedEvent;
use crate::join::JoinBuffer;
use crate::sase::SaseEngine;
use crate::window::{
    BinnedSlidingWindow, CountWindow, PartitionedBinnedSlidingWindow, PartitionedSessionWindow,
    PartitionedSlidingWindow, PartitionedTumblingWindow, SessionWindow, SlidingCountWindow,
    SlidingWindow, TumblingWindow,
};

// =============================================================================
// Public Types (exported from crate)
// =============================================================================

/// Configuration block parsed from VPL
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub name: String,
    pub values: HashMap<String, ConfigValue>,
}

/// User-defined function
#[derive(Debug, Clone)]
pub struct UserFunction {
    pub name: String,
    pub params: Vec<(String, varpulis_core::Type)>, // (name, type)
    pub return_type: Option<varpulis_core::Type>,
    pub body: Vec<varpulis_core::span::Spanned<varpulis_core::Stmt>>,
}

/// Engine performance metrics
#[derive(Debug, Clone)]
pub struct EngineMetrics {
    pub events_processed: u64,
    pub output_events_emitted: u64,
    pub streams_count: usize,
}

/// Report from a hot reload operation
#[derive(Debug, Clone, Default)]
pub struct ReloadReport {
    /// Streams that were added
    pub streams_added: Vec<String>,
    /// Streams that were removed
    pub streams_removed: Vec<String>,
    /// Streams that were updated (definition changed)
    pub streams_updated: Vec<String>,
    /// Streams whose state was preserved (windows, aggregators)
    pub state_preserved: Vec<String>,
    /// Streams whose state had to be reset
    pub state_reset: Vec<String>,
    /// Non-fatal warnings during reload
    pub warnings: Vec<String>,
}

impl ReloadReport {
    pub const fn is_empty(&self) -> bool {
        self.streams_added.is_empty()
            && self.streams_removed.is_empty()
            && self.streams_updated.is_empty()
    }
}

/// Source connector binding from .from() declarations
#[derive(Debug, Clone)]
pub struct SourceBinding {
    pub connector_name: String,
    pub event_type: String,
    pub topic_override: Option<String>,
    /// Extra parameters from .from() (e.g., client_id, qos) — excludes topic
    pub extra_params: HashMap<String, String>,
}

/// Named SASE+ pattern definition
#[derive(Debug, Clone)]
pub struct NamedPattern {
    /// Pattern name
    pub name: String,
    /// SASE+ pattern expression (SEQ, AND, OR, NOT)
    pub expr: SasePatternExpr,
    /// Optional time constraint
    pub within: Option<Expr>,
    /// Optional partition key expression
    pub partition_by: Option<Expr>,
}

// =============================================================================
// Internal Types (crate-visible)
// =============================================================================

/// Runtime stream definition
#[allow(dead_code)]
pub struct StreamDefinition {
    pub name: String,
    /// Cached `Arc<str>` of the stream name — avoids repeated `String→Arc<str>` conversions
    /// in the hot path (output event renaming, emit/sequence event construction).
    pub name_arc: Arc<str>,
    pub source: RuntimeSource,
    pub operations: Vec<RuntimeOp>,
    /// SASE+ pattern matching engine (NFA-based, primary engine for sequences)
    pub sase_engine: Option<SaseEngine>,
    /// Join buffer for correlating events from multiple sources
    pub join_buffer: Option<JoinBuffer>,
    /// Mapping from event type to join source name (for join streams)
    pub event_type_to_source: FxHashMap<String, String>,
    /// Hamlet aggregator for trend aggregation mode
    pub hamlet_aggregator: Option<crate::hamlet::HamletAggregator>,
    /// Shared Hamlet aggregator reference (when multi-query sharing is active)
    pub shared_hamlet_ref: Option<Arc<std::sync::Mutex<crate::hamlet::HamletAggregator>>>,
    /// PST-based pattern forecaster (when .forecast() is used)
    pub pst_forecaster: Option<crate::pst::PatternMarkovChain>,
    /// Last raw event that entered the pipeline (used by Forecast op to learn
    /// from every event even when the Sequence op clears current_events).
    pub last_raw_event: Option<crate::event::SharedEvent>,
    /// Enrichment provider + cache (when .enrich() is used, async-runtime only)
    #[cfg(feature = "async-runtime")]
    pub enrichment: Option<(
        Arc<dyn crate::enrichment::EnrichmentProvider>,
        Arc<crate::enrichment::EnrichmentCache>,
    )>,
    /// Optional backpressure buffer configuration for this stream's input channel (async-runtime only)
    #[cfg(feature = "async-runtime")]
    #[allow(dead_code)]
    pub buffer_config: Option<crate::backpressure::StageBufferConfig>,
}

/// Source of events for a stream
pub enum RuntimeSource {
    EventType(String),
    Stream(String),
    Join(Vec<String>),
    /// Merge multiple event types with optional filters
    Merge(Vec<MergeSource>),
    /// Periodic timer source
    Timer(TimerConfig),
}

impl RuntimeSource {
    /// Get a description of this source for logging
    pub fn describe(&self) -> String {
        match self {
            Self::EventType(t) => format!("event:{t}"),
            Self::Stream(s) => format!("stream:{s}"),
            Self::Join(sources) => format!("join:{}", sources.join(",")),
            Self::Merge(sources) => format!("merge:{} sources", sources.len()),
            Self::Timer(config) => {
                format!("timer:{}ms", config.interval_ns / 1_000_000)
            }
        }
    }
}

/// Configuration for a periodic timer source
pub struct TimerConfig {
    /// Interval between timer fires in nanoseconds
    pub interval_ns: u64,
    /// Optional initial delay before first fire in nanoseconds
    pub initial_delay_ns: Option<u64>,
    /// Event type name for timer events (e.g., "Timer_MyStream")
    pub timer_event_type: String,
}

/// A source in a merge construct with optional filter
#[allow(dead_code)]
pub struct MergeSource {
    pub name: String,
    pub event_type: String,
    pub filter: Option<varpulis_core::ast::Expr>,
}

/// Runtime operations that can be applied to a stream
#[allow(dead_code)]
pub enum RuntimeOp {
    /// Filter with closure (for sequence filters with context)
    WhereClosure(Box<dyn Fn(&SharedEvent) -> bool + Send + Sync>),
    /// Filter with expression (evaluated at runtime with user functions)
    WhereExpr(varpulis_core::ast::Expr),
    /// Window with optional partition support
    Window(WindowType),
    /// Partitioned window - maintains separate windows per partition key (tumbling)
    PartitionedWindow(PartitionedWindowState),
    /// Partitioned sliding count window - maintains separate sliding windows per partition key
    PartitionedSlidingCountWindow(PartitionedSlidingCountWindowState),
    Aggregate(Aggregator),
    /// Partitioned aggregate - maintains separate aggregators per partition key
    PartitionedAggregate(PartitionedAggregatorState),
    /// Phase-2 fused operator: tumbling window + partitioned aggregate, with
    /// columnar streaming accumulator state living per `(bin, group_idx)`
    /// across arriving event batches. The compiler emits this in place of
    /// `Window(PartitionedTumbling) → PartitionedAggregate` whenever the
    /// `arrow` feature is enabled and the aggregator only uses
    /// columnar-supported functions (sum/avg/min/max/count).
    ///
    /// Semantics differ from `PartitionedTumbling` in that bins are
    /// **absolutely aligned** (`bin_start = floor(ts / bin_ms) * bin_ms`)
    /// rather than per-partition-origin. This matches Flink/Arroyo and
    /// most user expectations of "tumble every N seconds of wall clock".
    #[cfg(feature = "arrow")]
    PartitionedWindowedColumnarAggregate(PartitionedWindowedColumnarAggregateState),
    /// Having filter - filter aggregation results (post-aggregate filtering)
    Having(varpulis_core::ast::Expr),
    /// Select/projection with computed fields
    Select(SelectConfig),
    Emit(EmitConfig),
    /// Emit with expression evaluation for computed fields
    EmitExpr(EmitExprConfig),
    /// Print to stdout
    Print(PrintConfig),
    /// Log with level
    Log(LogConfig),
    /// Sequence operation - index into sequence_tracker steps
    Sequence,
    /// Pattern matching with lambda expression
    Pattern(PatternConfig),
    /// Process with expression: `.process(expr)` - evaluates for side effects (emit)
    Process(varpulis_core::ast::Expr),
    /// Send to connector: `.to(ConnectorName)`
    To(ToConfig),
    /// Trend aggregation via Hamlet engine (replaces Sequence for this stream)
    TrendAggregate(TrendAggregateConfig),
    /// ONNX model scoring: extract input fields, run inference, add output fields
    #[allow(dead_code)]
    Score(ScoreConfig),
    /// PST-based pattern forecasting: predict pattern completion probability and time
    Forecast(ForecastConfig),
    /// External connector enrichment: lookup reference data and inject fields
    Enrich(EnrichConfig),
    /// Alert notification: send webhook with event data as side-effect
    Alert(AlertConfig),
    /// Deduplicate events by expression value (or entire event if None)
    Distinct(DistinctState),
    /// Pass at most N events, then stop the stream
    Limit(LimitState),
    /// Parallel processing: partition events across a Rayon thread pool (async-runtime only)
    #[cfg(feature = "async-runtime")]
    Concurrent(ConcurrentConfig),
}

impl RuntimeOp {
    /// Return a short human-readable name for this operation.
    ///
    /// Used by the topology builder to create operation summaries.
    pub const fn summary_name(&self) -> &'static str {
        match self {
            Self::WhereClosure(_) => "Filter",
            Self::WhereExpr(_) => "Filter",
            Self::Window(_) => "Window",
            Self::PartitionedWindow(_) => "PartitionedWindow",
            Self::PartitionedSlidingCountWindow(_) => "PartitionedSlidingCountWindow",
            Self::Aggregate(_) => "Aggregate",
            Self::PartitionedAggregate(_) => "PartitionedAggregate",
            #[cfg(feature = "arrow")]
            Self::PartitionedWindowedColumnarAggregate(_) => "PartitionedWindowedColumnarAggregate",
            Self::Having(_) => "Having",
            Self::Select(_) => "Select",
            Self::Emit(_) => "Emit",
            Self::EmitExpr(_) => "EmitExpr",
            Self::Print(_) => "Print",
            Self::Log(_) => "Log",
            Self::Sequence => "Sequence",
            Self::Pattern(_) => "Pattern",
            Self::Process(_) => "Process",
            Self::To(_) => "Sink",
            Self::TrendAggregate(_) => "TrendAggregate",
            Self::Score(_) => "Score",
            Self::Forecast(_) => "Forecast",
            Self::Enrich(_) => "Enrich",
            Self::Alert(_) => "Alert",
            Self::Distinct(_) => "Distinct",
            Self::Limit(_) => "Limit",
            #[cfg(feature = "async-runtime")]
            Self::Concurrent(_) => "Concurrent",
        }
    }
}

/// Configuration for `.concurrent()` parallel processing.
///
/// Production-ready, opt-in via `.concurrent()` in VPL.  Creates a rayon
/// thread pool that partitions events across workers by key or round-robin.
/// Only available with async-runtime (requires rayon).
#[cfg(feature = "async-runtime")]
pub struct ConcurrentConfig {
    pub workers: usize,
    pub partition_key: Option<String>,
    pub thread_pool: std::sync::Arc<rayon::ThreadPool>,
}

/// Configuration for trend aggregation via Hamlet engine
pub struct TrendAggregateConfig {
    /// Fields to compute: (output_alias, aggregate_function)
    pub fields: Vec<(String, crate::greta::GretaAggregate)>,
    /// Query ID in the Hamlet aggregator
    pub query_id: crate::greta::QueryId,
    /// Field-based aggregate info for runtime computation (sum/avg/min/max)
    pub field_aggregates: Vec<FieldAggregateInfo>,
    /// Maps type index (u16) to event type name (for count_events resolution)
    pub type_index_to_name: Vec<String>,
    /// Accumulated events across invocations (persists between execute_op calls)
    pub accumulated: Vec<SharedEvent>,
}

/// Info for computing field-based aggregates at runtime.
/// Populated at compile time from `sum_trends(alias.field)` etc.
pub struct FieldAggregateInfo {
    /// Output alias in the emitted event (e.g., "total")
    pub output_alias: String,
    /// Aggregate function name: "sum", "avg", "min", "max"
    pub func: String,
    /// Event type to filter on (e.g., "sensor_reading")
    pub event_type: String,
    /// Field name to aggregate (e.g., "temperature")
    pub field_name: String,
}

/// Configuration for PST-based pattern forecasting
#[allow(dead_code)]
pub struct ForecastConfig {
    /// Minimum probability to emit forecast events.
    pub confidence_threshold: f64,
    /// Forecast horizon in nanoseconds.
    pub horizon_ns: u64,
    /// Events before forecasting starts.
    pub warmup_events: u64,
    /// Maximum PST context depth.
    pub max_depth: usize,
    /// Whether Hawkes intensity modulation is enabled.
    pub hawkes: bool,
    /// Whether conformal prediction intervals are enabled.
    pub conformal: bool,
}

/// Configuration for `.alert()` webhook notifications
pub struct AlertConfig {
    /// Webhook URL to POST alert payload
    pub webhook_url: Option<String>,
    /// Optional message template with `{field}` interpolation
    pub message_template: Option<String>,
}

/// Configuration for external connector enrichment
#[allow(dead_code)]
pub struct EnrichConfig {
    /// Name of the connector to use for lookups
    pub connector_name: String,
    /// Expression to evaluate as the lookup key
    pub key_expr: varpulis_core::ast::Expr,
    /// Fields to extract from the enrichment response
    pub fields: Vec<String>,
    /// Cache TTL in nanoseconds (None = no caching)
    pub cache_ttl_ns: Option<u64>,
    /// Timeout in nanoseconds (default 5s)
    pub timeout_ns: u64,
    /// Fallback value when lookup fails (None = skip event)
    pub fallback: Option<varpulis_core::Value>,
}

/// Configuration for ONNX model scoring
#[allow(dead_code)]
pub struct ScoreConfig {
    #[cfg(feature = "onnx")]
    pub model: std::sync::Arc<crate::scoring::OnnxModel>,
    pub input_fields: Vec<String>,
    pub output_fields: Vec<String>,
    pub batch_size: usize,
}

/// Topic specification for `.to()` operations — static or dynamic
#[allow(dead_code)]
pub enum TopicSpec {
    /// Static topic string resolved at compile time (current behavior)
    Static(String),
    /// Dynamic topic resolved per event from an expression
    Dynamic(varpulis_core::ast::Expr),
}

/// Configuration for .to() connector routing
#[allow(dead_code)]
pub struct ToConfig {
    pub connector_name: String,
    /// Topic specification: static string, dynamic expression, or None (use connector default)
    pub topic: Option<TopicSpec>,
    /// Cache key for sink lookup (connector_name or connector_name::topic)
    pub sink_key: String,
    /// Extra parameters from .to() (e.g., client_id, qos) — excludes topic
    #[allow(dead_code)]
    pub extra_params: HashMap<String, String>,
}

/// Default capacity for distinct state LRU cache
pub const DISTINCT_LRU_CAPACITY: usize = 100_000;

/// State for .distinct() — tracks seen values to deduplicate events
pub struct DistinctState {
    /// Optional expression to evaluate for distinct key; None = entire event
    pub expr: Option<varpulis_core::ast::Expr>,
    /// LRU cache of seen value representations (bounded to prevent unbounded growth)
    pub seen: hashlink::LruCache<String, ()>,
}

/// State for .limit(n) — passes at most `max` events
pub struct LimitState {
    pub max: usize,
    pub count: usize,
}

/// Configuration for select/projection operation
pub struct SelectConfig {
    /// Fields to include: (output_name, expression)
    pub fields: Vec<(String, varpulis_core::ast::Expr)>,
}

/// Result of processing a stream
pub struct StreamProcessResult {
    /// Events produced by .emit() — sent to output channel AND downstream
    pub emitted_events: Vec<SharedEvent>,
    /// Output events to feed to dependent streams (with stream name as event_type)
    pub output_events: Vec<SharedEvent>,
    /// Number of events sent to connector sinks via .to() operations
    pub sink_events_sent: u64,
}

/// State for partitioned windows - maintains separate windows per partition key
pub struct PartitionedWindowState {
    pub partition_key: String,
    pub window_size: usize, // For count-based windows
    pub windows: FxHashMap<String, CountWindow>,
}

impl PartitionedWindowState {
    pub fn new(partition_key: String, window_size: usize) -> Self {
        Self {
            partition_key,
            window_size,
            windows: FxHashMap::default(),
        }
    }

    pub fn add(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let key = event.get(&self.partition_key).map_or_else(
            || "default".to_string(),
            |v| v.to_partition_key().into_owned(),
        );

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| CountWindow::new(self.window_size));

        window.add_shared(event)
    }
}

/// State for partitioned sliding count windows - maintains separate sliding windows per partition key
pub struct PartitionedSlidingCountWindowState {
    pub partition_key: String,
    pub window_size: usize,
    pub slide_size: usize,
    pub windows: FxHashMap<String, SlidingCountWindow>,
}

impl PartitionedSlidingCountWindowState {
    pub fn new(partition_key: String, window_size: usize, slide_size: usize) -> Self {
        Self {
            partition_key,
            window_size,
            slide_size,
            windows: FxHashMap::default(),
        }
    }

    pub fn add(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let key = event.get(&self.partition_key).map_or_else(
            || "default".to_string(),
            |v| v.to_partition_key().into_owned(),
        );

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| SlidingCountWindow::new(self.window_size, self.slide_size));

        window.add_shared(event)
    }
}

/// State for partitioned aggregators
#[derive(Debug)]
pub struct PartitionedAggregatorState {
    pub partition_key: String,
    pub aggregator_template: Aggregator,
    /// Schema cache reused across calls so every fire doesn't re-infer the
    /// event schema. Only populated on the columnar fast path.
    #[cfg(feature = "arrow")]
    pub(crate) schema_cache: crate::arrow_bridge::SchemaCache,
}

impl PartitionedAggregatorState {
    pub fn new(partition_key: String, aggregator: Aggregator) -> Self {
        Self {
            partition_key,
            aggregator_template: aggregator,
            #[cfg(feature = "arrow")]
            schema_cache: crate::arrow_bridge::SchemaCache::new(),
        }
    }

    /// Entry point called by the pipeline executor. Dispatches between the
    /// row-oriented path (always available) and the streaming columnar
    /// path (behind `arrow` feature, gated on batch size and supported
    /// aggregate-function set).
    pub fn apply(&mut self, events: &[SharedEvent]) -> Vec<(String, IndexMap<String, Value>)> {
        #[cfg(feature = "arrow")]
        {
            // Columnar fast path. Criteria:
            //   1. Batch is large enough to amortize the RecordBatch
            //      conversion overhead (`ARROW_BATCH_THRESHOLD` events).
            //   2. Every aggregation function is one of sum/avg/min/max/count
            //      (the phase-1 supported set — `StdDev`, `Percentile`,
            //      `First`, `Last`, etc. still go row-oriented).
            if events.len() >= crate::arrow_bridge::ARROW_BATCH_THRESHOLD
                && self.aggregator_template.supported_for_columnar()
            {
                if let Some(results) = self.apply_columnar(events) {
                    return results;
                }
                // Fall through to the row path on any error (schema
                // inference failure, unexpected column type, etc.). The
                // row path is always correct, just slower.
            }
        }
        self.apply_row(events)
    }

    /// Row-oriented path — the original implementation. Preserved both as
    /// a fallback and for aggregation functions the columnar path doesn't
    /// support yet. This is the hot path for batches smaller than
    /// `ARROW_BATCH_THRESHOLD` and for pipelines that use StdDev /
    /// Percentile / First / Last / etc.
    ///
    /// Exposed as `#[doc(hidden)] pub` so internal tests and benchmarks
    /// can measure each path directly. NOT part of the public API and
    /// may change without notice.
    #[doc(hidden)]
    pub fn apply_row(&mut self, events: &[SharedEvent]) -> Vec<(String, IndexMap<String, Value>)> {
        // Group events by partition key - use Arc::clone to avoid deep clones
        let mut partitions: FxHashMap<String, Vec<SharedEvent>> = FxHashMap::default();

        for event in events {
            let key = event.get(&self.partition_key).map_or_else(
                || "default".to_string(),
                |v| v.to_partition_key().into_owned(),
            );
            partitions.entry(key).or_default().push(Arc::clone(event));
        }

        // Apply aggregator to each partition using apply_shared
        let mut results = Vec::new();
        for (key, partition_events) in partitions {
            let result = self.aggregator_template.apply_shared(&partition_events);
            results.push((key, result));
        }

        results
    }

    /// Streaming columnar path. Converts `events` to a RecordBatch, runs
    /// one bulk `arrow_row::RowConverter` hash pass over the partition-key
    /// column, and updates per-group accumulators in SIMD-friendly loops.
    ///
    /// Returns `None` on any failure (unknown field name, schema mismatch,
    /// unsupported column type); the caller falls back to the row path.
    ///
    /// Exposed as `#[doc(hidden)] pub` so internal tests and benchmarks
    /// can measure each path directly. NOT part of the public API and
    /// may change without notice.
    #[doc(hidden)]
    #[cfg(feature = "arrow")]
    pub fn apply_columnar(
        &mut self,
        events: &[SharedEvent],
    ) -> Option<Vec<(String, IndexMap<String, Value>)>> {
        use crate::arrow_aggregate::grouped::AggSpec;
        use crate::arrow_aggregate::{make_accumulator_for, ColumnarGroupedAggregator};

        // 1. Infer schema & build RecordBatch for all events.
        let schema = self.schema_cache.get_or_infer(events);
        let batch = crate::arrow_bridge::events_to_record_batch(events, &schema).ok()?;

        // 2. Locate the partition key field in the inferred schema. If
        //    the field isn't present (e.g. all events had it null and
        //    `infer_schema` skipped it), fall back to the row path —
        //    which handles that case via `map_or_else("default", ..)`.
        let key_field = schema.field_with_name(&self.partition_key).ok()?.clone();

        // 3. Build one accumulator per (alias, func_name, field) spec.
        //    `supported_for_columnar()` already guaranteed every func is
        //    in the sum/avg/min/max/count set, so `make_accumulator_for`
        //    should never return None — but we still handle it to avoid
        //    panicking inside an aggregator.
        let specs: Vec<AggSpec> = self
            .aggregator_template
            .iter_specs()
            .map(|(alias, func_name, field)| {
                let accumulator = make_accumulator_for(func_name)?;
                Some(AggSpec {
                    alias: alias.clone(),
                    accumulator,
                    field: field.clone(),
                })
            })
            .collect::<Option<Vec<_>>>()?;

        let mut agg = ColumnarGroupedAggregator::try_new(&[key_field], specs).ok()?;

        // 4. Feed the batch (one call in phase 1; phase 2 streams).
        agg.update(&batch).ok()?;
        Some(agg.drain_as_row_results())
    }
}

/// State for the phase-2 fused tumbling-window + partitioned columnar
/// aggregate operator. Lives behind the `arrow` feature flag.
///
/// Wraps a [`crate::arrow_aggregate::streaming::StreamingPartitionedWindow`]
/// (the bin-keyed streaming aggregator) plus the per-bin duration. The
/// pipeline dispatcher hands incoming events to `ingest_and_flush` and
/// converts the row-shaped output back to `AggregationResult` events.
#[cfg(feature = "arrow")]
pub struct PartitionedWindowedColumnarAggregateState {
    pub partition_key: String,
    pub bin_duration_ms: i64,
    pub(crate) inner: crate::arrow_aggregate::streaming::StreamingPartitionedWindow,
}

#[cfg(feature = "arrow")]
impl std::fmt::Debug for PartitionedWindowedColumnarAggregateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedWindowedColumnarAggregateState")
            .field("partition_key", &self.partition_key)
            .field("bin_duration_ms", &self.bin_duration_ms)
            .field("inner", &self.inner)
            .finish()
    }
}

#[cfg(feature = "arrow")]
impl PartitionedWindowedColumnarAggregateState {
    /// Build the fused operator from a partition key, bin duration, and
    /// a phase-1 [`crate::aggregation::Aggregator`]. Returns `None` if
    /// the aggregator uses any function the columnar path doesn't
    /// support — callers must gate with `Aggregator::supported_for_columnar()`
    /// first; this is a defensive double-check.
    pub fn try_new(
        partition_key: String,
        bin_duration_ms: i64,
        aggregator: &crate::aggregation::Aggregator,
    ) -> Option<Self> {
        let template =
            crate::arrow_aggregate::streaming::StreamingAggregatorTemplate::from_aggregator(
                partition_key.clone(),
                aggregator,
            )?;
        let inner = crate::arrow_aggregate::streaming::StreamingPartitionedWindow::new(
            template,
            bin_duration_ms,
        );
        Some(Self {
            partition_key,
            bin_duration_ms,
            inner,
        })
    }

    /// Hand a batch of events to the streaming aggregator, drain any
    /// bins flushed by the watermark advance, and return them in
    /// `(bin_start_ms, partition_key, agg_results)` form.
    pub fn ingest_and_flush(
        &mut self,
        events: &[SharedEvent],
    ) -> Vec<(i64, String, IndexMap<String, Value>)> {
        self.inner.ingest_and_flush(events)
    }

    /// Force-flush all remaining bins (engine shutdown / end-of-stream).
    /// Wired into the engine teardown path so the final partial windows
    /// aren't lost. Currently unused on the steady-state hot path.
    #[allow(dead_code)]
    pub fn flush_all(&mut self) -> Vec<(i64, String, IndexMap<String, Value>)> {
        self.inner.flush_all()
    }
}

/// Configuration for pattern matching
#[allow(dead_code)]
pub struct PatternConfig {
    pub name: String,
    pub matcher: varpulis_core::ast::Expr,
}

/// Configuration for print operation
pub struct PrintConfig {
    pub exprs: Vec<varpulis_core::ast::Expr>,
}

/// Configuration for log operation
pub struct LogConfig {
    pub level: String,
    pub message: Option<String>,
    pub data_field: Option<String>,
}

/// Types of windows supported
pub enum WindowType {
    Tumbling(TumblingWindow),
    Sliding(SlidingWindow),
    Count(CountWindow),
    SlidingCount(SlidingCountWindow),
    PartitionedTumbling(PartitionedTumblingWindow),
    PartitionedSliding(PartitionedSlidingWindow),
    Session(SessionWindow),
    PartitionedSession(PartitionedSessionWindow),
    BinnedSliding(BinnedSlidingWindow),
    PartitionedBinnedSliding(PartitionedBinnedSlidingWindow),
}

/// Configuration for simple emit operation
#[allow(dead_code)]
pub struct EmitConfig {
    pub fields: Vec<(String, String)>, // (output_name, source_field or literal)
    pub target_context: Option<String>,
}

/// Configuration for emit with expressions
#[allow(dead_code)]
pub struct EmitExprConfig {
    pub fields: Vec<(String, varpulis_core::ast::Expr)>, // (output_name, expression)
    pub target_context: Option<String>,
}

/// Configuration for late data handling in watermark-based windowing.
#[allow(dead_code)]
pub struct LateDataConfig {
    /// How much lateness to tolerate beyond the watermark.
    /// Events arriving within this window after watermark advancement are still processed.
    pub allowed_lateness: chrono::Duration,
    /// Optional stream name to route late events that exceed allowed_lateness.
    /// If None, late events are dropped.
    pub side_output_stream: Option<String>,
}

// =============================================================================
// Parity tests — row-oriented vs columnar PartitionedAggregatorState::apply
// =============================================================================
//
// These tests sit inside the `types` module so they have access to the
// private `apply_row` and `apply_columnar` methods. The core guarantee:
// for any input, the two paths produce equivalent results (modulo float
// reassociation). This is the regression gate for the streaming columnar
// grouped aggregator.

#[cfg(all(test, feature = "arrow"))]
mod arrow_parity_tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::aggregation::{Aggregator, Avg, Count, Max, Min, Sum};
    use crate::event::Event;

    /// Normalise an `apply` result to a sorted-by-key representation so
    /// we can compare the two paths without caring about iteration order.
    fn normalise(
        results: Vec<(String, IndexMap<String, Value>)>,
    ) -> BTreeMap<String, BTreeMap<String, Value>> {
        results
            .into_iter()
            .map(|(k, inner)| (k, inner.into_iter().collect()))
            .collect()
    }

    /// Compare two normalised results, treating `Value::Float` equality
    /// via tolerance and every other variant via exact equality. Returns
    /// the first mismatch as a formatted string, or None on equality.
    fn diff(
        row: &BTreeMap<String, BTreeMap<String, Value>>,
        col: &BTreeMap<String, BTreeMap<String, Value>>,
        tol: f64,
    ) -> Option<String> {
        if row.len() != col.len() {
            return Some(format!(
                "partition count mismatch: row={}, col={}",
                row.len(),
                col.len()
            ));
        }
        for (k, row_inner) in row {
            let Some(col_inner) = col.get(k) else {
                return Some(format!("partition '{k}' missing in columnar output"));
            };
            if row_inner.len() != col_inner.len() {
                return Some(format!(
                    "partition '{k}' alias count mismatch: row={}, col={}",
                    row_inner.len(),
                    col_inner.len()
                ));
            }
            for (alias, row_v) in row_inner {
                let Some(col_v) = col_inner.get(alias) else {
                    return Some(format!("alias '{alias}' missing in partition '{k}'"));
                };
                match (row_v, col_v) {
                    (Value::Float(a), Value::Float(b)) => {
                        if !a.is_finite() && !b.is_finite() {
                            continue;
                        }
                        if (a - b).abs() > tol {
                            return Some(format!(
                                "float mismatch in '{k}'.'{alias}': row={a}, col={b}"
                            ));
                        }
                    }
                    (Value::Null, Value::Null) => {}
                    (Value::Int(a), Value::Int(b)) => {
                        if a != b {
                            return Some(format!(
                                "int mismatch in '{k}'.'{alias}': row={a}, col={b}"
                            ));
                        }
                    }
                    (a, b) => {
                        if a != b {
                            return Some(format!(
                                "value mismatch in '{k}'.'{alias}': row={a:?}, col={b:?}"
                            ));
                        }
                    }
                }
            }
        }
        None
    }

    /// Build a state freshly configured with sum/avg/min/max/count on `value`.
    fn make_state() -> PartitionedAggregatorState {
        PartitionedAggregatorState::new(
            "device_id".to_string(),
            Aggregator::new()
                .add("s", Box::new(Sum), Some("value".to_string()))
                .add("a", Box::new(Avg), Some("value".to_string()))
                .add("mn", Box::new(Min), Some("value".to_string()))
                .add("mx", Box::new(Max), Some("value".to_string()))
                .add("c", Box::new(Count), None),
        )
    }

    /// Synthetic events matching scenario 02's shape: `n_devices` distinct
    /// device ids, each receiving `per_device` events with sequential
    /// float values.
    fn make_events(n_devices: usize, per_device: usize) -> Vec<SharedEvent> {
        let mut events = Vec::with_capacity(n_devices * per_device);
        for i in 0..(n_devices * per_device) {
            let dev = i % n_devices;
            let v = (i as f64) * 1.5 + 10.0;
            let e = Event::new("Reading")
                .with_field("device_id", Value::Str(format!("d{dev}").into()))
                .with_field("value", Value::Float(v));
            events.push(Arc::new(e));
        }
        events
    }

    #[test]
    fn scenario_02_shape_row_matches_columnar() {
        // 100 devices × 1000 events per device = 100k events, 100 groups.
        // Matches `benchmarks/arroyo-comparison/scenarios/02_aggregation`.
        let events = make_events(100, 1000);
        let row = normalise(make_state().apply_row(&events));
        let col = normalise(
            make_state()
                .apply_columnar(&events)
                .expect("columnar path should succeed for a homogeneous schema"),
        );
        if let Some(msg) = diff(&row, &col, 1e-6) {
            panic!("scenario-02 parity failed: {msg}");
        }
    }

    #[test]
    fn sparse_groups_row_matches_columnar() {
        // Extreme sparsity: 50 devices × 1 event each — worst case for
        // row path (one event per group means per-event overhead dominates).
        let events = make_events(50, 1);
        let row = normalise(make_state().apply_row(&events));
        let col = normalise(
            make_state()
                .apply_columnar(&events)
                .expect("columnar path should succeed"),
        );
        if let Some(msg) = diff(&row, &col, 1e-9) {
            panic!("sparse groups parity failed: {msg}");
        }
    }

    #[test]
    fn dense_few_groups_row_matches_columnar() {
        // 2 devices × 5000 events each — the "analytics" shape.
        let events = make_events(2, 5000);
        let row = normalise(make_state().apply_row(&events));
        let col = normalise(
            make_state()
                .apply_columnar(&events)
                .expect("columnar path should succeed"),
        );
        if let Some(msg) = diff(&row, &col, 1e-5) {
            panic!("dense groups parity failed: {msg}");
        }
    }

    #[test]
    fn single_group_row_matches_columnar() {
        // Everyone is on "d0" — degenerate case, tests that the columnar
        // path handles `total_groups == 1` correctly.
        let mut events = Vec::new();
        for i in 0..200 {
            events.push(Arc::new(
                Event::new("Reading")
                    .with_field("device_id", Value::Str("d0".into()))
                    .with_field("value", Value::Float(i as f64)),
            ));
        }
        let row = normalise(make_state().apply_row(&events));
        let col = normalise(
            make_state()
                .apply_columnar(&events)
                .expect("columnar path should succeed"),
        );
        if let Some(msg) = diff(&row, &col, 1e-9) {
            panic!("single group parity failed: {msg}");
        }
    }

    #[test]
    fn handles_int_partition_key() {
        // Row path's to_partition_key() renders Int as its decimal
        // string; the columnar path hashes it through RowConverter on
        // Int64. Both should produce the same group identity.
        let mut events = Vec::new();
        for i in 0..100 {
            events.push(Arc::new(
                Event::new("Reading")
                    .with_field("device_id", Value::Int(i % 5))
                    .with_field("value", Value::Float(i as f64 * 2.0)),
            ));
        }
        let row = normalise(make_state().apply_row(&events));
        let col = normalise(
            make_state()
                .apply_columnar(&events)
                .expect("columnar path should succeed"),
        );
        if let Some(msg) = diff(&row, &col, 1e-9) {
            panic!("int partition key parity failed: {msg}");
        }
    }

    #[test]
    fn handles_nan_values() {
        // Scatter NaN into the value column. Both paths should skip
        // NaN when computing sum/avg/min/max.
        let mut events = Vec::new();
        for i in 0..200 {
            let v = if i % 10 == 0 { f64::NAN } else { i as f64 };
            events.push(Arc::new(
                Event::new("Reading")
                    .with_field("device_id", Value::Str(format!("d{}", i % 4).into()))
                    .with_field("value", Value::Float(v)),
            ));
        }
        let row = normalise(make_state().apply_row(&events));
        let col = normalise(
            make_state()
                .apply_columnar(&events)
                .expect("columnar path should succeed"),
        );
        if let Some(msg) = diff(&row, &col, 1e-6) {
            panic!("NaN handling parity failed: {msg}");
        }
    }

    #[test]
    fn empty_input_is_empty_output() {
        let events: Vec<SharedEvent> = vec![];
        // apply() dispatches to row path for empty input (below threshold).
        let out = make_state().apply(&events);
        assert!(out.is_empty());
    }
}
