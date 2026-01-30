//! Type definitions for the Varpulis engine
//!
//! This module contains all the structs, enums and type definitions used by the engine.

use crate::aggregation::Aggregator;
use crate::attention::AttentionWindow;
use crate::event::SharedEvent;
use crate::join::JoinBuffer;
use crate::pattern::PatternEngine;
use crate::sase::SaseEngine;
use crate::window::{
    CountWindow, PartitionedSlidingWindow, PartitionedTumblingWindow, SlidingCountWindow,
    SlidingWindow, TumblingWindow,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use varpulis_core::ast::ConfigValue;
use varpulis_core::Value;

// =============================================================================
// Public Types (exported from crate)
// =============================================================================

/// Alert emitted by the engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_type: String,
    pub severity: String,
    pub message: String,
    pub data: IndexMap<String, Value>,
}

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
    pub alerts_generated: u64,
    pub streams_count: usize,
}

// =============================================================================
// Internal Types (crate-visible)
// =============================================================================

/// Runtime stream definition
pub(crate) struct StreamDefinition {
    pub name: String,
    pub source: RuntimeSource,
    pub operations: Vec<RuntimeOp>,
    /// Attention window for correlation scoring
    pub attention_window: Option<AttentionWindow>,
    /// Pattern engine for Apama-style pattern matching (lambdas)
    pub pattern_engine: Option<PatternEngine>,
    /// SASE+ pattern matching engine (NFA-based, primary engine for sequences)
    pub sase_engine: Option<SaseEngine>,
    /// Join buffer for correlating events from multiple sources
    pub join_buffer: Option<JoinBuffer>,
    /// Mapping from event type to join source name (for join streams)
    pub event_type_to_source: std::collections::HashMap<String, String>,
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
    /// Attention window for correlation scoring
    AttentionWindow(AttentionWindowConfig),
    /// Pattern matching with lambda expression
    Pattern(PatternConfig),
}

/// Configuration for select/projection operation
pub(crate) struct SelectConfig {
    /// Fields to include: (output_name, expression)
    pub fields: Vec<(String, varpulis_core::ast::Expr)>,
}

/// Result of processing a stream: alerts to emit and output events for dependent streams
pub(crate) struct StreamProcessResult {
    pub alerts: Vec<Alert>,
    /// Output events to feed to dependent streams (with stream name as event_type)
    pub output_events: Vec<SharedEvent>,
}

/// State for partitioned windows - maintains separate windows per partition key
pub(crate) struct PartitionedWindowState {
    pub partition_key: String,
    pub window_size: usize, // For count-based windows
    pub windows: HashMap<String, CountWindow>,
}

impl PartitionedWindowState {
    pub fn new(partition_key: String, window_size: usize) -> Self {
        Self {
            partition_key,
            window_size,
            windows: HashMap::new(),
        }
    }

    pub fn add(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let key = event
            .get(&self.partition_key)
            .map(|v| format!("{}", v))
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
    pub windows: HashMap<String, SlidingCountWindow>,
}

impl PartitionedSlidingCountWindowState {
    pub fn new(partition_key: String, window_size: usize, slide_size: usize) -> Self {
        Self {
            partition_key,
            window_size,
            slide_size,
            windows: HashMap::new(),
        }
    }

    pub fn add(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let key = event
            .get(&self.partition_key)
            .map(|v| format!("{}", v))
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
        let mut partitions: HashMap<String, Vec<SharedEvent>> = HashMap::new();

        for event in events {
            let key = event
                .get(&self.partition_key)
                .map(|v| format!("{}", v))
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
pub(crate) struct PatternConfig {
    pub name: String,
    pub matcher: varpulis_core::ast::Expr,
}

/// Configuration for attention window
pub(crate) struct AttentionWindowConfig {
    pub duration_ns: u64,
    pub num_heads: usize,
    pub embedding_dim: usize,
    pub threshold: f32,
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
}

/// Configuration for simple emit operation
pub(crate) struct EmitConfig {
    pub fields: Vec<(String, String)>, // (output_name, source_field or literal)
}

/// Configuration for emit with expressions
pub(crate) struct EmitExprConfig {
    pub fields: Vec<(String, varpulis_core::ast::Expr)>, // (output_name, expression)
}
