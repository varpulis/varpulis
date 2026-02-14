//! Type definitions for the Varpulis engine
//!
//! This module contains all the structs, enums and type definitions used by the engine.

use crate::aggregation::Aggregator;
use crate::event::SharedEvent;
use crate::join::JoinBuffer;
use crate::sase::SaseEngine;
use crate::window::{
    CountWindow, PartitionedSessionWindow, PartitionedSlidingWindow, PartitionedTumblingWindow,
    SessionWindow, SlidingCountWindow, SlidingWindow, TumblingWindow,
};
use indexmap::IndexMap;
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::sync::Arc;
use varpulis_core::ast::{ConfigValue, Expr, SasePatternExpr};
use varpulis_core::Value;

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
#[derive(Clone)]
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
    pub fn is_empty(&self) -> bool {
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
pub(crate) struct StreamDefinition {
    pub name: String,
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
}

/// Source of events for a stream
pub(crate) enum RuntimeSource {
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
            RuntimeSource::EventType(t) => format!("event:{}", t),
            RuntimeSource::Stream(s) => format!("stream:{}", s),
            RuntimeSource::Join(sources) => format!("join:{}", sources.join(",")),
            RuntimeSource::Merge(sources) => format!("merge:{} sources", sources.len()),
            RuntimeSource::Timer(config) => {
                format!("timer:{}ms", config.interval_ns / 1_000_000)
            }
        }
    }
}

/// Configuration for a periodic timer source
pub(crate) struct TimerConfig {
    /// Interval between timer fires in nanoseconds
    pub interval_ns: u64,
    /// Optional initial delay before first fire in nanoseconds
    pub initial_delay_ns: Option<u64>,
    /// Event type name for timer events (e.g., "Timer_MyStream")
    pub timer_event_type: String,
}

/// A source in a merge construct with optional filter
pub(crate) struct MergeSource {
    pub name: String,
    pub event_type: String,
    pub filter: Option<varpulis_core::ast::Expr>,
}

/// Runtime operations that can be applied to a stream
pub(crate) enum RuntimeOp {
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
    /// Deduplicate events by expression value (or entire event if None)
    Distinct(DistinctState),
    /// Pass at most N events, then stop the stream
    Limit(LimitState),
}

/// Configuration for trend aggregation via Hamlet engine
pub(crate) struct TrendAggregateConfig {
    /// Fields to compute: (output_alias, aggregate_function)
    pub fields: Vec<(String, crate::greta::GretaAggregate)>,
    /// Query ID in the Hamlet aggregator
    pub query_id: crate::greta::QueryId,
}

/// Configuration for PST-based pattern forecasting
#[allow(dead_code)]
pub(crate) struct ForecastConfig {
    /// Minimum probability to emit forecast events.
    pub confidence_threshold: f64,
    /// Forecast horizon in nanoseconds.
    pub horizon_ns: u64,
    /// Events before forecasting starts.
    pub warmup_events: u64,
    /// Maximum PST context depth.
    pub max_depth: usize,
}

/// Configuration for ONNX model scoring
#[allow(dead_code)]
pub(crate) struct ScoreConfig {
    #[cfg(feature = "onnx")]
    pub model: std::sync::Arc<crate::scoring::OnnxModel>,
    pub input_fields: Vec<String>,
    pub output_fields: Vec<String>,
}

/// Configuration for .to() connector routing
pub(crate) struct ToConfig {
    pub connector_name: String,
    /// Topic override from .to() params (e.g., `.to(Conn, topic: "my-topic")`)
    pub topic_override: Option<String>,
    /// Cache key for sink lookup (connector_name or connector_name::topic)
    pub sink_key: String,
    /// Extra parameters from .to() (e.g., client_id, qos) — excludes topic
    #[allow(dead_code)]
    pub extra_params: HashMap<String, String>,
}

/// Default capacity for distinct state LRU cache
pub(crate) const DISTINCT_LRU_CAPACITY: usize = 100_000;

/// State for .distinct() — tracks seen values to deduplicate events
pub(crate) struct DistinctState {
    /// Optional expression to evaluate for distinct key; None = entire event
    pub expr: Option<varpulis_core::ast::Expr>,
    /// LRU cache of seen value representations (bounded to prevent unbounded growth)
    pub seen: lru::LruCache<String, ()>,
}

/// State for .limit(n) — passes at most `max` events
pub(crate) struct LimitState {
    pub max: usize,
    pub count: usize,
}

/// Configuration for select/projection operation
pub(crate) struct SelectConfig {
    /// Fields to include: (output_name, expression)
    pub fields: Vec<(String, varpulis_core::ast::Expr)>,
}

/// Result of processing a stream
pub(crate) struct StreamProcessResult {
    /// Events produced by .emit() — sent to output channel AND downstream
    pub emitted_events: Vec<SharedEvent>,
    /// Output events to feed to dependent streams (with stream name as event_type)
    pub output_events: Vec<SharedEvent>,
}

/// State for partitioned windows - maintains separate windows per partition key
pub(crate) struct PartitionedWindowState {
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
        let key = event
            .get(&self.partition_key)
            .map(|v| v.to_partition_key().into_owned())
            .unwrap_or_else(|| "default".to_string());

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| CountWindow::new(self.window_size));

        window.add_shared(event)
    }
}

/// State for partitioned sliding count windows - maintains separate sliding windows per partition key
pub(crate) struct PartitionedSlidingCountWindowState {
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
        let key = event
            .get(&self.partition_key)
            .map(|v| v.to_partition_key().into_owned())
            .unwrap_or_else(|| "default".to_string());

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| SlidingCountWindow::new(self.window_size, self.slide_size));

        window.add_shared(event)
    }
}

/// State for partitioned aggregators
pub(crate) struct PartitionedAggregatorState {
    pub partition_key: String,
    pub aggregator_template: Aggregator,
}

impl PartitionedAggregatorState {
    pub fn new(partition_key: String, aggregator: Aggregator) -> Self {
        Self {
            partition_key,
            aggregator_template: aggregator,
        }
    }

    pub fn apply(&mut self, events: &[SharedEvent]) -> Vec<(String, IndexMap<String, Value>)> {
        // Group events by partition key - use Arc::clone to avoid deep clones
        let mut partitions: FxHashMap<String, Vec<SharedEvent>> = FxHashMap::default();

        for event in events {
            let key = event
                .get(&self.partition_key)
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_else(|| "default".to_string());
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
}

/// Configuration for pattern matching
#[allow(dead_code)]
pub(crate) struct PatternConfig {
    pub name: String,
    pub matcher: varpulis_core::ast::Expr,
}

/// Configuration for print operation
pub(crate) struct PrintConfig {
    pub exprs: Vec<varpulis_core::ast::Expr>,
}

/// Configuration for log operation
pub(crate) struct LogConfig {
    pub level: String,
    pub message: Option<String>,
    pub data_field: Option<String>,
}

/// Types of windows supported
pub(crate) enum WindowType {
    Tumbling(TumblingWindow),
    Sliding(SlidingWindow),
    Count(CountWindow),
    SlidingCount(SlidingCountWindow),
    PartitionedTumbling(PartitionedTumblingWindow),
    PartitionedSliding(PartitionedSlidingWindow),
    Session(SessionWindow),
    PartitionedSession(PartitionedSessionWindow),
}

/// Configuration for simple emit operation
#[allow(dead_code)]
pub(crate) struct EmitConfig {
    pub fields: Vec<(String, String)>, // (output_name, source_field or literal)
    pub target_context: Option<String>,
}

/// Configuration for emit with expressions
#[allow(dead_code)]
pub(crate) struct EmitExprConfig {
    pub fields: Vec<(String, varpulis_core::ast::Expr)>, // (output_name, expression)
    pub target_context: Option<String>,
}

/// Configuration for late data handling in watermark-based windowing.
pub(crate) struct LateDataConfig {
    /// How much lateness to tolerate beyond the watermark.
    /// Events arriving within this window after watermark advancement are still processed.
    pub allowed_lateness: chrono::Duration,
    /// Optional stream name to route late events that exceed allowed_lateness.
    /// If None, late events are dropped.
    pub side_output_stream: Option<String>,
}
