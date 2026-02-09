//! Main execution engine for Varpulis
//!
//! This module provides the core engine that processes events and executes
//! stream definitions written in VPL.

mod compiler;
mod evaluator;
mod pattern_analyzer;
mod pipeline;
mod router;
mod sink_factory;
mod types;

#[cfg(test)]
mod tests;

// Re-export public types
pub use sink_factory::SinkConnectorAdapter;
pub use types::{EngineConfig, EngineMetrics, ReloadReport, SourceBinding, UserFunction};

// Re-export evaluator for use by other modules (e.g., SASE+)
pub use evaluator::eval_filter_expr;

// Re-export internal types for use within the engine module
use types::{
    AttentionWindowConfig, EmitConfig, EmitExprConfig, LogConfig, MergeSource,
    PartitionedAggregatorState, PartitionedSlidingCountWindowState, PartitionedWindowState,
    PatternConfig, PrintConfig, RuntimeOp, RuntimeSource, SelectConfig, StreamDefinition,
    StreamProcessResult, TimerConfig, ToConfig, TrendAggregateConfig, WindowType,
};

use crate::aggregation::Aggregator;
use crate::attention::{AttentionConfig, AttentionWindow, EmbeddingConfig};
use crate::connector;
use crate::context::ContextMap;
use crate::event::{Event, SharedEvent};
use crate::join::JoinBuffer;
use crate::metrics::Metrics;
use crate::sase::SaseEngine;
use crate::sequence::SequenceContext;
use crate::watermark::PerSourceWatermarkTracker;
use crate::window::{
    CountWindow, PartitionedSessionWindow, PartitionedSlidingWindow, PartitionedTumblingWindow,
    SessionWindow, SlidingCountWindow, SlidingWindow, TumblingWindow,
};
use chrono::Duration;
use chrono::{DateTime, Utc};
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use varpulis_core::ast::{ConfigItem, Program, Stmt, StreamOp, StreamSource};
use varpulis_core::Value;

// Re-export NamedPattern from types
pub use types::NamedPattern;

/// Output channel type enumeration for zero-copy or owned event sending
enum OutputChannel {
    /// Legacy channel that requires cloning (for backwards compatibility)
    Owned(mpsc::Sender<Event>),
    /// Zero-copy channel using SharedEvent (Arc<Event>)
    Shared(mpsc::Sender<SharedEvent>),
}

/// The main Varpulis engine
pub struct Engine {
    /// Registered stream definitions
    streams: FxHashMap<String, StreamDefinition>,
    /// Event routing: maps event types to stream names
    router: router::EventRouter,
    /// User-defined functions
    functions: FxHashMap<String, UserFunction>,
    /// Named patterns for reuse
    patterns: FxHashMap<String, NamedPattern>,
    /// Configuration blocks (e.g., mqtt, kafka)
    configs: FxHashMap<String, EngineConfig>,
    /// Mutable variables accessible across events
    variables: FxHashMap<String, Value>,
    /// Tracks which variables are declared as mutable (var vs let)
    mutable_vars: FxHashSet<String>,
    /// Declared connectors from VPL
    connectors: FxHashMap<String, connector::ConnectorConfig>,
    /// Source connector bindings from .from() declarations
    source_bindings: Vec<SourceBinding>,
    /// Sink registry for .to() operations
    sinks: sink_factory::SinkRegistry,
    /// Output event sender (None for benchmark/quiet mode - skips cloning overhead)
    output_channel: Option<OutputChannel>,
    /// Metrics
    events_processed: u64,
    output_events_emitted: u64,
    /// Prometheus metrics
    metrics: Option<Metrics>,
    /// Context assignments for multi-threaded execution
    context_map: ContextMap,
    /// Per-source watermark tracker for event-time processing
    watermark_tracker: Option<PerSourceWatermarkTracker>,
    /// Last applied watermark (for detecting advances)
    last_applied_watermark: Option<DateTime<Utc>>,
    /// Late data configurations per stream
    late_data_configs: FxHashMap<String, types::LateDataConfig>,
    /// Context name when running inside a context thread (used for unique connector IDs)
    context_name: Option<String>,
    /// Shared Hamlet aggregators for multi-query optimization
    shared_hamlet_aggregators:
        Vec<std::sync::Arc<std::sync::Mutex<crate::hamlet::HamletAggregator>>>,
}

impl Engine {
    pub fn new(output_tx: mpsc::Sender<Event>) -> Self {
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
            output_channel: Some(OutputChannel::Owned(output_tx)),
            events_processed: 0,
            output_events_emitted: 0,
            metrics: None,
            context_map: ContextMap::new(),
            watermark_tracker: None,
            last_applied_watermark: None,
            late_data_configs: FxHashMap::default(),
            context_name: None,
            shared_hamlet_aggregators: Vec::new(),
        }
    }

    /// Create engine without output channel (for benchmarking - skips Event cloning overhead)
    pub fn new_benchmark() -> Self {
        Self::new_internal(None)
    }

    /// Create engine with optional output channel (legacy API, requires cloning)
    pub fn new_with_optional_output(output_tx: Option<mpsc::Sender<Event>>) -> Self {
        Self::new_internal(output_tx.map(OutputChannel::Owned))
    }

    /// Create engine with zero-copy SharedEvent output channel (PERF: avoids cloning)
    pub fn new_shared(output_tx: mpsc::Sender<SharedEvent>) -> Self {
        Self::new_internal(Some(OutputChannel::Shared(output_tx)))
    }

    /// Internal constructor
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
            shared_hamlet_aggregators: Vec::new(),
        }
    }

    /// Clone the output channel for use in engine reload
    fn clone_output_channel(&self) -> Option<OutputChannel> {
        match &self.output_channel {
            Some(OutputChannel::Owned(tx)) => Some(OutputChannel::Owned(tx.clone())),
            Some(OutputChannel::Shared(tx)) => Some(OutputChannel::Shared(tx.clone())),
            None => None,
        }
    }

    /// Send an output event to the output channel (if configured).
    /// In benchmark mode (no output channel), this is a no-op to avoid cloning overhead.
    /// PERF: Uses zero-copy for SharedEvent channels, clones only for legacy Owned channels.
    #[inline]
    fn send_output_shared(&mut self, event: &SharedEvent) {
        match &self.output_channel {
            Some(OutputChannel::Shared(tx)) => {
                // PERF: Zero-copy - just increment Arc refcount
                if let Err(e) = tx.try_send(Arc::clone(event)) {
                    warn!("Failed to send output event: {}", e);
                }
            }
            Some(OutputChannel::Owned(tx)) => {
                // Legacy path: must clone the Event
                let owned = (**event).clone();
                if let Err(e) = tx.try_send(owned) {
                    warn!("Failed to send output event: {}", e);
                }
            }
            None => {
                // Benchmark mode: skip sending entirely - no clone!
            }
        }
    }

    /// Send an output event to the output channel (if configured).
    /// In benchmark mode (no output channel), this is a no-op to avoid cloning overhead.
    #[inline]
    fn send_output(&mut self, event: Event) {
        match &self.output_channel {
            Some(OutputChannel::Shared(tx)) => {
                // Wrap in Arc for shared channel
                if let Err(e) = tx.try_send(Arc::new(event)) {
                    warn!("Failed to send output event: {}", e);
                }
            }
            Some(OutputChannel::Owned(tx)) => {
                if let Err(e) = tx.try_send(event) {
                    warn!("Failed to send output event: {}", e);
                }
            }
            None => {
                // Benchmark mode: skip sending entirely
            }
        }
    }

    /// Set the context name for this engine instance.
    ///
    /// When set, connectors (e.g., MQTT) append the context name to their
    /// client IDs to avoid broker conflicts when multiple contexts share the
    /// same connector configuration. Without a context, the exact client_id
    /// from the VPL connector declaration is used unchanged.
    pub fn set_context_name(&mut self, name: &str) {
        self.context_name = Some(name.to_string());
    }

    /// Get a named pattern by name
    pub fn get_pattern(&self, name: &str) -> Option<&NamedPattern> {
        self.patterns.get(name)
    }

    /// Get all registered patterns
    pub fn patterns(&self) -> &FxHashMap<String, NamedPattern> {
        &self.patterns
    }

    /// Get a configuration block by name
    pub fn get_config(&self, name: &str) -> Option<&EngineConfig> {
        self.configs.get(name)
    }

    /// Get a declared connector by name
    pub fn get_connector(&self, name: &str) -> Option<&connector::ConnectorConfig> {
        self.connectors.get(name)
    }

    /// Get all declared connector configs (for building a ManagedConnectorRegistry).
    pub fn connector_configs(&self) -> &FxHashMap<String, connector::ConnectorConfig> {
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
    pub fn set_variable(&mut self, name: &str, value: Value) -> Result<(), String> {
        if self.variables.contains_key(name) && !self.mutable_vars.contains(name) {
            return Err(format!(
                "Cannot assign to immutable variable '{}'. Use 'var' instead of 'let' to declare mutable variables.",
                name
            ));
        }
        self.variables.insert(name.to_string(), value);
        Ok(())
    }

    /// Get all variables (for debugging/testing)
    pub fn variables(&self) -> &FxHashMap<String, Value> {
        &self.variables
    }

    /// Get the context map (for orchestrator setup)
    pub fn context_map(&self) -> &ContextMap {
        &self.context_map
    }

    /// Check if the loaded program declares any contexts
    pub fn has_contexts(&self) -> bool {
        self.context_map.has_contexts()
    }

    /// Enable Prometheus metrics
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Add a programmatic filter to a stream using a closure
    pub fn add_filter<F>(&mut self, stream_name: &str, filter: F) -> Result<(), String>
    where
        F: Fn(&Event) -> bool + Send + Sync + 'static,
    {
        if let Some(stream) = self.streams.get_mut(stream_name) {
            // Wrap the closure to accept SharedEvent (dereferences to &Event)
            let wrapped = move |e: &SharedEvent| filter(e.as_ref());
            stream
                .operations
                .insert(0, RuntimeOp::WhereClosure(Box::new(wrapped)));
            Ok(())
        } else {
            Err(format!("Stream '{}' not found", stream_name))
        }
    }

    /// Load a program into the engine with semantic validation.
    ///
    /// `source` is the original VPL source text (used for diagnostic formatting).
    /// Pass `""` if the source is unavailable.
    pub fn load_with_source(&mut self, source: &str, program: &Program) -> Result<(), String> {
        let validation = varpulis_core::validate::validate(source, program);
        if validation.has_errors() {
            return Err(validation.format(source));
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
    pub fn load(&mut self, program: &Program) -> Result<(), String> {
        self.load_program(program)
    }

    fn load_program(&mut self, program: &Program) -> Result<(), String> {
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
                    info!("Import statement: {} (alias: {:?})", path, alias);
                    // TODO: Load and merge imported file
                }
                Stmt::VarDecl {
                    mutable,
                    name,
                    value,
                    ..
                } => {
                    // Evaluate the initial value, using existing variables as bindings
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
                        format!("Failed to evaluate initial value for variable '{}'", name)
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
                    // Evaluate the new value, using existing variables as bindings
                    let dummy_event = Event::new("__assign__");
                    let empty_ctx = SequenceContext::new();
                    let new_value = evaluator::eval_expr_with_functions(
                        value,
                        &dummy_event,
                        &empty_ctx,
                        &self.functions,
                        &self.variables,
                    )
                    .ok_or_else(|| format!("Failed to evaluate assignment value for '{}'", name))?;

                    // Check if variable is mutable
                    if self.variables.contains_key(name) && !self.mutable_vars.contains(name) {
                        return Err(format!(
                            "Cannot assign to immutable variable '{}'. Use 'var' instead of 'let'.",
                            name
                        ));
                    }

                    // If variable doesn't exist, treat as implicit mutable declaration
                    if !self.variables.contains_key(name) {
                        self.mutable_vars.insert(name.clone());
                    }

                    info!("Assigned variable: {} = {:?}", name, new_value);
                    self.variables.insert(name.clone(), new_value);
                }
                Stmt::ContextDecl { name, cores } => {
                    use crate::context::ContextConfig;
                    info!("Registered context: {} (cores: {:?})", name, cores);
                    self.context_map.register_context(ContextConfig {
                        name: name.clone(),
                        cores: cores.clone(),
                    });
                }
                Stmt::ConnectorDecl {
                    name,
                    connector_type,
                    params,
                } => {
                    let config = sink_factory::connector_params_to_config(connector_type, params);
                    info!("Registered connector: {} (type: {})", name, connector_type);
                    self.connectors.insert(name.clone(), config);
                }
                _ => {
                    debug!("Skipping statement: {:?}", stmt.node);
                }
            }
        }

        // Collect all sink_keys actually referenced by stream operations
        let mut referenced_sink_keys: FxHashSet<String> = FxHashSet::default();
        let mut topic_overrides: Vec<(String, String, String)> = Vec::new(); // (sink_key, connector_name, topic)
        for stream in self.streams.values() {
            for op in &stream.operations {
                if let RuntimeOp::To(to_config) = op {
                    referenced_sink_keys.insert(to_config.sink_key.clone());
                    if let Some(ref topic) = to_config.topic_override {
                        topic_overrides.push((
                            to_config.sink_key.clone(),
                            to_config.connector_name.clone(),
                            topic.clone(),
                        ));
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
        );

        // Phase 2: Detect multi-query Hamlet sharing opportunities
        self.setup_hamlet_sharing();

        Ok(())
    }

    /// Detect overlapping Kleene patterns across streams using `.trend_aggregate()`
    /// and replace per-stream aggregators with shared ones.
    fn setup_hamlet_sharing(&mut self) {
        use crate::hamlet::template::TemplateBuilder;
        use crate::hamlet::{HamletAggregator, HamletConfig, QueryRegistration};

        // Collect streams that have Hamlet aggregators
        let hamlet_streams: Vec<String> = self
            .streams
            .iter()
            .filter(|(_, s)| s.hamlet_aggregator.is_some())
            .map(|(name, _)| name.clone())
            .collect();

        if hamlet_streams.len() < 2 {
            return; // No sharing possible with fewer than 2 queries
        }

        // Extract Kleene type info from each stream's TrendAggregate config
        // Group streams by their Kleene event types
        let mut kleene_groups: FxHashMap<Vec<String>, Vec<String>> = FxHashMap::default();

        for stream_name in &hamlet_streams {
            if let Some(stream) = self.streams.get(stream_name) {
                // Find TrendAggregate op and extract event types
                let mut kleene_types = Vec::new();
                for op in &stream.operations {
                    if let RuntimeOp::TrendAggregate(_) = op {
                        // Get event types from the stream's hamlet aggregator
                        if let Some(ref agg) = stream.hamlet_aggregator {
                            for query in agg.registered_queries() {
                                for &kt in query.kleene_types.iter() {
                                    let type_name = format!("type_{}", kt);
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

        // For groups with 2+ streams sharing Kleene patterns, create shared aggregators
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

            // Build a shared template from all queries in the group
            let mut builder = TemplateBuilder::new();
            // (stream_name, registration, fields_config)
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
                        for query in agg.registered_queries() {
                            // Re-register with a new query ID in the shared template
                            let new_id = next_query_id;
                            next_query_id += 1;

                            // Get event type names from template
                            let event_names: Vec<String> = query
                                .event_types
                                .iter()
                                .map(|&idx| format!("type_{}", idx))
                                .collect();
                            let name_strs: Vec<&str> =
                                event_names.iter().map(|s| s.as_str()).collect();

                            builder.add_sequence(new_id, &name_strs);

                            // Add Kleene patterns
                            for &kt in &query.kleene_types {
                                let type_name = format!("type_{}", kt);
                                // Find the state for this type in the sequence
                                let position = event_names
                                    .iter()
                                    .position(|n| *n == type_name)
                                    .unwrap_or(0);
                                builder.add_kleene(new_id, &type_name, position as u16);
                            }

                            // Get fields from the TrendAggregate config
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

            // Create shared aggregator
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

            // Update each stream to use the shared aggregator
            // (We replace the per-stream aggregator with None and update the TrendAggregateConfig query_id)
            for (stream_name, registration, fields) in &all_registrations {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    // Remove per-stream aggregator - shared one is used instead
                    stream.hamlet_aggregator = None;

                    // Update the TrendAggregate op's query_id
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

    /// Connect all sinks that require explicit connection.
    ///
    /// Call this after `load()` to establish connections to external systems
    /// like MQTT brokers, databases, etc. This must be called before processing
    /// events if any `.to()` operations are used.
    pub async fn connect_sinks(&self) -> Result<(), String> {
        self.sinks.connect_all().await
    }

    /// Inject a pre-built sink into the engine's registry.
    ///
    /// Use this to share connections — e.g. when a source and sink use the
    /// same MQTT broker, clone the source's client into a sink and inject it
    /// here so that `connect_sinks()` becomes a no-op for that sink.
    pub fn inject_sink(&mut self, key: &str, sink: Arc<dyn crate::sink::Sink>) {
        self.sinks.insert(key.to_string(), sink);
    }

    /// Check whether a given key has a registered sink.
    pub fn has_sink(&self, key: &str) -> bool {
        self.sinks.cache().contains_key(key)
    }

    /// Return all sink keys that belong to a given connector name.
    ///
    /// A key matches if it equals the connector name exactly, or if it
    /// starts with `connector_name::` (topic-override keys).
    pub fn sink_keys_for_connector(&self, connector_name: &str) -> Vec<String> {
        let prefix = format!("{}::", connector_name);
        self.sinks
            .cache()
            .keys()
            .filter(|k| *k == connector_name || k.starts_with(&prefix))
            .cloned()
            .collect()
    }

    fn register_stream(
        &mut self,
        name: &str,
        source: &StreamSource,
        ops: &[StreamOp],
    ) -> Result<(), String> {
        // Extract context assignments from stream ops
        for (emit_idx, op) in ops.iter().enumerate() {
            match op {
                StreamOp::Context(ctx_name) => {
                    self.context_map
                        .assign_stream(name.to_string(), ctx_name.clone());
                }
                StreamOp::Emit {
                    target_context: Some(ctx),
                    ..
                } => {
                    self.context_map.add_cross_context_emit(
                        name.to_string(),
                        emit_idx,
                        ctx.clone(),
                    );
                }
                StreamOp::Watermark(args) => {
                    // Configure per-source watermark tracking for this stream
                    self.enable_watermark_tracking();
                    let mut max_ooo = Duration::seconds(0);
                    for arg in args {
                        if arg.name == "out_of_order" {
                            if let varpulis_core::ast::Expr::Duration(ns) = &arg.value {
                                max_ooo = Duration::nanoseconds(*ns as i64);
                            }
                        }
                    }
                    let source_et = match source {
                        StreamSource::Ident(s) => Some(s.as_str()),
                        StreamSource::IdentWithAlias { name: et, .. } => Some(et.as_str()),
                        StreamSource::FromConnector { event_type, .. } => Some(event_type.as_str()),
                        _ => None,
                    };
                    if let Some(et) = source_et {
                        self.register_watermark_source(et, max_ooo);
                    }
                }
                StreamOp::AllowedLateness(expr) => {
                    // Configure late-data handling for this stream
                    let lateness_ns = match expr {
                        varpulis_core::ast::Expr::Duration(ns) => *ns as i64,
                        _ => 0,
                    };
                    self.late_data_configs.insert(
                        name.to_string(),
                        types::LateDataConfig {
                            allowed_lateness: Duration::nanoseconds(lateness_ns),
                            side_output_stream: None,
                        },
                    );
                }
                _ => {}
            }
        }

        // Check if we have sequence operations and build SASE+ engine
        let (runtime_ops, sase_engine, sequence_event_types, hamlet_aggregator) =
            self.compile_ops_with_sequences(source, ops)?;

        // Mapping from event_type to source name (for join streams)
        let mut event_type_to_source: FxHashMap<String, String> = FxHashMap::default();

        let runtime_source = match source {
            StreamSource::FromConnector {
                event_type,
                connector_name,
                params,
            } => {
                // EventType.from(Connector, topic: "...", ...)
                // Register for the event type, connector info will be used at runtime
                info!(
                    "Registering stream {} from connector {} for event type {}",
                    name, connector_name, event_type
                );
                let topic_override = params
                    .iter()
                    .find(|p| p.name == "topic")
                    .and_then(|p| p.value.as_string().map(|s| s.to_string()));
                self.source_bindings.push(SourceBinding {
                    connector_name: connector_name.clone(),
                    event_type: event_type.clone(),
                    topic_override,
                });
                self.router.add_route(event_type, name);
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::Ident(stream_name) => {
                // Check if this refers to a named SASE+ pattern
                if self.patterns.contains_key(stream_name) {
                    let event_types = compiler::extract_event_types_from_pattern_expr(
                        &self.patterns[stream_name].expr,
                    );
                    for et in &event_types {
                        self.router.add_route(et, name);
                    }
                    let first_type = event_types.first().cloned().unwrap_or_default();
                    RuntimeSource::EventType(first_type)
                } else {
                    // Regular stream reference
                    self.router.add_route(stream_name, name);
                    RuntimeSource::Stream(stream_name.clone())
                }
            }
            StreamSource::IdentWithAlias {
                name: event_type, ..
            } => {
                // Register for the event type (alias is handled in sequence)
                self.router.add_route(event_type, name);
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::AllWithAlias {
                name: event_type, ..
            } => {
                // Register for the event type (all + alias handled in sequence)
                self.router.add_route(event_type, name);
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::Sequence(decl) => {
                // Register for all event types in the sequence
                for step in &decl.steps {
                    self.router.add_route(&step.event_type, name);
                }
                // Use first event type as the primary source
                let first_type = decl
                    .steps
                    .first()
                    .map(|s| s.event_type.clone())
                    .unwrap_or_default();
                RuntimeSource::EventType(first_type)
            }
            StreamSource::Join(clauses) => {
                let sources: Vec<String> = clauses.iter().map(|c| c.source.clone()).collect();
                info!(
                    "Registering join stream {} from sources: {:?}",
                    name, sources
                );
                // For join sources, we register based on whether the source is a derived stream or an event type
                // - Derived streams (with operations like aggregate, window, etc.) output events with stream name as event_type
                // - Simple event streams need to receive the raw event type
                for source in &sources {
                    if let Some(stream_def) = self.streams.get(source) {
                        // Source is a registered stream
                        // Check if it has any transforming operations (aggregate, window, select, etc.)
                        let has_operations = !stream_def.operations.is_empty();

                        if has_operations {
                            // Derived stream with operations - register for the stream name
                            // because its output events have event_type = stream name
                            info!(
                                "Join source '{}' is a derived stream, registering for stream name",
                                source
                            );
                            self.router.add_route(source, name);
                            event_type_to_source.insert(source.clone(), source.clone());
                        } else {
                            // Simple passthrough stream - register for underlying event type
                            let event_type = match &stream_def.source {
                                RuntimeSource::EventType(et) => et.clone(),
                                RuntimeSource::Stream(s) => s.clone(),
                                _ => source.clone(),
                            };
                            info!(
                                "Join source '{}' is a passthrough stream, registering for event type '{}'",
                                source, event_type
                            );
                            self.router.add_route(&event_type, name);
                            event_type_to_source.insert(event_type, source.clone());
                        }
                    } else {
                        // Source stream not yet registered, assume it's an event type name
                        info!(
                            "Join source '{}' not found as stream, treating as event type",
                            source
                        );
                        self.router.add_route(source, name);
                        event_type_to_source.insert(source.clone(), source.clone());
                    }
                }
                RuntimeSource::Join(sources)
            }
            StreamSource::Merge(decls) => {
                let merge_sources: Vec<MergeSource> = decls
                    .iter()
                    .map(|d| MergeSource {
                        name: d.name.clone(),
                        event_type: d.source.clone(),
                        filter: d.filter.clone(),
                    })
                    .collect();

                // Register for all source event types
                for ms in &merge_sources {
                    self.router.add_route(&ms.event_type, name);
                }

                info!(
                    "Registering merge stream {} with {} sources",
                    name,
                    merge_sources.len()
                );
                RuntimeSource::Merge(merge_sources)
            }
            StreamSource::Timer(decl) => {
                // Extract interval from duration expression
                let interval_ns = match &decl.interval {
                    varpulis_core::ast::Expr::Duration(ns) => *ns,
                    _ => {
                        warn!("Timer interval must be a duration, defaulting to 1s");
                        1_000_000_000u64 // 1 second default
                    }
                };

                // Extract optional initial delay
                let initial_delay_ns =
                    decl.initial_delay
                        .as_ref()
                        .and_then(|expr| match expr.as_ref() {
                            varpulis_core::ast::Expr::Duration(ns) => Some(*ns),
                            _ => None,
                        });

                // Create timer event type based on stream name
                let timer_event_type = format!("Timer_{}", name);

                // Register this stream to receive timer events
                self.router.add_route(&timer_event_type, name);

                info!(
                    "Registering timer stream {} with interval {}ms{}",
                    name,
                    interval_ns / 1_000_000,
                    initial_delay_ns
                        .map(|d| format!(", initial_delay {}ms", d / 1_000_000))
                        .unwrap_or_default()
                );

                RuntimeSource::Timer(TimerConfig {
                    interval_ns,
                    initial_delay_ns,
                    timer_event_type,
                })
            }
        };

        // Register for all event types in sequence (avoid duplicates)
        for event_type in &sequence_event_types {
            self.router.add_route(event_type, name);
        }
        if !sequence_event_types.is_empty() {
            debug!(
                "Stream {} registered for sequence event types: {:?}",
                name, sequence_event_types
            );
        }

        // Extract attention window config from operations if present
        let attention_window = self.extract_attention_window(&runtime_ops);

        // Create JoinBuffer for Join sources
        let join_buffer = if let StreamSource::Join(clauses) = source {
            let join_sources: Vec<String> = clauses.iter().map(|c| c.source.clone()).collect();
            let join_keys = self.extract_join_keys(clauses, ops);
            let window_duration = self.extract_window_duration(ops);

            debug!(
                "Creating JoinBuffer for stream {} with sources {:?}, keys {:?}, window {:?}",
                name, join_sources, join_keys, window_duration
            );

            Some(JoinBuffer::new(join_sources, join_keys, window_duration))
        } else {
            None
        };

        // Log source description before moving
        let source_desc = runtime_source.describe();

        self.streams.insert(
            name.to_string(),
            StreamDefinition {
                name: name.to_string(),
                source: runtime_source,
                operations: runtime_ops,
                attention_window,
                sase_engine,
                join_buffer,
                event_type_to_source,
                hamlet_aggregator,
            },
        );

        info!("Registered stream: {} (source: {})", name, source_desc);
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn compile_ops_with_sequences(
        &self,
        source: &StreamSource,
        ops: &[StreamOp],
    ) -> Result<
        (
            Vec<RuntimeOp>,
            Option<SaseEngine>,
            Vec<String>,
            Option<crate::hamlet::HamletAggregator>,
        ),
        String,
    > {
        let mut runtime_ops = Vec::new();
        let mut sequence_event_types: Vec<String> = Vec::new();
        let mut partition_key: Option<String> = None;

        // For SASE+ pattern compilation
        let mut followed_by_clauses: Vec<varpulis_core::ast::FollowedByClause> = Vec::new();
        let mut negation_clauses: Vec<varpulis_core::ast::FollowedByClause> = Vec::new();
        let mut global_within: Option<std::time::Duration> = None;

        // For Hamlet trend aggregation
        let mut trend_agg_items: Option<Vec<varpulis_core::ast::TrendAggItem>> = None;
        let mut within_expr_for_hamlet: Option<varpulis_core::ast::Expr> = None;

        // Helper closure to resolve a stream/event name to the underlying event type
        let resolve_event_type = |name: &str| -> String {
            if let Some(stream_def) = self.streams.get(name) {
                // This is a registered stream - get its underlying event type
                match &stream_def.source {
                    RuntimeSource::EventType(et) => et.clone(),
                    RuntimeSource::Stream(s) => s.clone(),
                    _ => name.to_string(),
                }
            } else {
                // Not a registered stream - use as-is (it's an event type)
                name.to_string()
            }
        };

        // Collect sequence event types from source (with stream resolution)
        // Only add source event types when there are actual sequence operations
        // (followedBy, not, within). Without this guard, a derived stream like
        // `HighTempAlert = Temperatures .where(...)` would incorrectly register
        // for the underlying event type (TemperatureReading) in addition to the
        // stream name (Temperatures), causing duplicate processing.
        let has_sequence_ops = ops.iter().any(|op| {
            matches!(
                op,
                StreamOp::FollowedBy(_) | StreamOp::Not(_) | StreamOp::Within(_)
            )
        });

        match source {
            StreamSource::Sequence(decl) => {
                for step in &decl.steps {
                    let resolved = resolve_event_type(&step.event_type);
                    if !sequence_event_types.contains(&resolved) {
                        sequence_event_types.push(resolved);
                    }
                }
            }
            StreamSource::Ident(name) if self.patterns.contains_key(name) => {
                // Named pattern reference - extract event types from pattern
                let event_types =
                    compiler::extract_event_types_from_pattern_expr(&self.patterns[name].expr);
                for et in event_types {
                    let resolved = resolve_event_type(&et);
                    if !sequence_event_types.contains(&resolved) {
                        sequence_event_types.push(resolved);
                    }
                }
            }
            StreamSource::Ident(name) if has_sequence_ops => {
                // Initial source for a sequence pattern - resolve to underlying event type
                let resolved = resolve_event_type(name);
                if !sequence_event_types.contains(&resolved) {
                    sequence_event_types.push(resolved);
                }
            }
            StreamSource::IdentWithAlias { name, .. } | StreamSource::AllWithAlias { name, .. }
                if has_sequence_ops =>
            {
                let resolved = resolve_event_type(name);
                if !sequence_event_types.contains(&resolved) {
                    sequence_event_types.push(resolved);
                }
            }
            _ => {}
        }

        for op in ops {
            match op {
                StreamOp::FollowedBy(clause) => {
                    // Store raw clause for SASE+ compilation
                    followed_by_clauses.push(clause.clone());
                    // Resolve event type for routing registration
                    let resolved = resolve_event_type(&clause.event_type);
                    if !sequence_event_types.contains(&resolved) {
                        sequence_event_types.push(resolved);
                    }
                    continue;
                }
                StreamOp::Within(expr) => {
                    // Parse duration from expression
                    let duration_ns = match expr {
                        varpulis_core::ast::Expr::Duration(ns) => *ns,
                        _ => 300_000_000_000u64, // 5 minutes default
                    };
                    global_within = Some(std::time::Duration::from_nanos(duration_ns));
                    within_expr_for_hamlet = Some(expr.clone());
                    continue;
                }
                StreamOp::Not(clause) => {
                    // Store negation clause for SASE+ engine
                    negation_clauses.push(clause.clone());
                    // Add negation event type to sequence event types so it gets routed
                    let resolved = resolve_event_type(&clause.event_type);
                    if !sequence_event_types.contains(&resolved) {
                        sequence_event_types.push(resolved);
                    }
                    continue;
                }
                StreamOp::TrendAggregate(items) => {
                    trend_agg_items = Some(items.clone());
                    continue;
                }
                StreamOp::Context(_) => {
                    // Context assignment is metadata, not a runtime operation.
                    // Handled by the engine's load() method via context_map.
                    continue;
                }
                StreamOp::Watermark(_) | StreamOp::AllowedLateness(_) => {
                    // Handled in register_stream() as metadata ops
                    continue;
                }
                _ => {}
            }

            // Handle non-sequence operations
            match op {
                StreamOp::Window(args) => {
                    // Check for session window first
                    if let Some(ref gap_expr) = args.session_gap {
                        let gap_ns = match gap_expr {
                            varpulis_core::ast::Expr::Duration(ns) => *ns,
                            _ => 300_000_000_000, // 5 minute default
                        };
                        let gap = Duration::nanoseconds(gap_ns as i64);
                        if let Some(ref key) = partition_key {
                            runtime_ops.push(RuntimeOp::Window(WindowType::PartitionedSession(
                                PartitionedSessionWindow::new(key.clone(), gap),
                            )));
                        } else {
                            runtime_ops.push(RuntimeOp::Window(WindowType::Session(
                                SessionWindow::new(gap),
                            )));
                        }
                    } else {
                        // Check if this is a count-based or time-based window
                        match &args.duration {
                            varpulis_core::ast::Expr::Int(count) => {
                                // Count-based window
                                let count = *count as usize;

                                // Get slide amount if specified (default to window size for tumbling)
                                let slide = args.sliding.as_ref().map(|s| match s {
                                    varpulis_core::ast::Expr::Int(n) => *n as usize,
                                    _ => 1,
                                });

                                // If we have a partition key, use partitioned window
                                if let Some(ref key) = partition_key {
                                    if let Some(slide_size) = slide {
                                        // Partitioned sliding count window
                                        runtime_ops.push(RuntimeOp::PartitionedSlidingCountWindow(
                                            PartitionedSlidingCountWindowState::new(
                                                key.clone(),
                                                count,
                                                slide_size,
                                            ),
                                        ));
                                    } else {
                                        // Partitioned tumbling count window
                                        runtime_ops.push(RuntimeOp::PartitionedWindow(
                                            PartitionedWindowState::new(key.clone(), count),
                                        ));
                                    }
                                } else if let Some(slide_size) = slide {
                                    runtime_ops.push(RuntimeOp::Window(WindowType::SlidingCount(
                                        SlidingCountWindow::new(count, slide_size),
                                    )));
                                } else {
                                    runtime_ops.push(RuntimeOp::Window(WindowType::Count(
                                        CountWindow::new(count),
                                    )));
                                }
                            }
                            varpulis_core::ast::Expr::Duration(ns) => {
                                // Time-based window
                                let duration = Duration::nanoseconds(*ns as i64);
                                if let Some(ref key) = partition_key {
                                    // Partitioned time-based window
                                    if let Some(sliding) = &args.sliding {
                                        let slide_ns = match sliding {
                                            varpulis_core::ast::Expr::Duration(ns) => *ns,
                                            _ => 60_000_000_000, // 1 minute default
                                        };
                                        let slide = Duration::nanoseconds(slide_ns as i64);
                                        runtime_ops.push(RuntimeOp::Window(
                                            WindowType::PartitionedSliding(
                                                PartitionedSlidingWindow::new(
                                                    key.clone(),
                                                    duration,
                                                    slide,
                                                ),
                                            ),
                                        ));
                                    } else {
                                        runtime_ops.push(RuntimeOp::Window(
                                            WindowType::PartitionedTumbling(
                                                PartitionedTumblingWindow::new(
                                                    key.clone(),
                                                    duration,
                                                ),
                                            ),
                                        ));
                                    }
                                } else if let Some(sliding) = &args.sliding {
                                    let slide_ns = match sliding {
                                        varpulis_core::ast::Expr::Duration(ns) => *ns,
                                        _ => 60_000_000_000, // 1 minute default
                                    };
                                    let slide = Duration::nanoseconds(slide_ns as i64);
                                    runtime_ops.push(RuntimeOp::Window(WindowType::Sliding(
                                        SlidingWindow::new(duration, slide),
                                    )));
                                } else {
                                    runtime_ops.push(RuntimeOp::Window(WindowType::Tumbling(
                                        TumblingWindow::new(duration),
                                    )));
                                }
                            }
                            _ => {
                                // Default to 5 minute tumbling window
                                let duration = Duration::nanoseconds(300_000_000_000);
                                if let Some(ref key) = partition_key {
                                    runtime_ops.push(RuntimeOp::Window(
                                        WindowType::PartitionedTumbling(
                                            PartitionedTumblingWindow::new(key.clone(), duration),
                                        ),
                                    ));
                                } else {
                                    runtime_ops.push(RuntimeOp::Window(WindowType::Tumbling(
                                        TumblingWindow::new(duration),
                                    )));
                                }
                            }
                        }
                    } // close else (non-session)
                }
                StreamOp::PartitionBy(expr) => {
                    // Extract partition key field name
                    if let varpulis_core::ast::Expr::Ident(field) = expr {
                        partition_key = Some(field.clone());
                    }
                }
                StreamOp::Aggregate(items) => {
                    let mut aggregator = Aggregator::new();
                    for item in items {
                        if let Some((func, field)) = compiler::compile_agg_expr(&item.expr) {
                            aggregator = aggregator.add(item.alias.clone(), func, field);
                        }
                    }
                    // If we have a partition key, use partitioned aggregate
                    if let Some(ref key) = partition_key {
                        runtime_ops.push(RuntimeOp::PartitionedAggregate(
                            PartitionedAggregatorState::new(key.clone(), aggregator),
                        ));
                    } else {
                        runtime_ops.push(RuntimeOp::Aggregate(aggregator));
                    }
                }
                StreamOp::Select(items) => {
                    let fields: Vec<(String, varpulis_core::ast::Expr)> = items
                        .iter()
                        .map(|item| match item {
                            varpulis_core::ast::SelectItem::Field(name) => {
                                (name.clone(), varpulis_core::ast::Expr::Ident(name.clone()))
                            }
                            varpulis_core::ast::SelectItem::Alias(name, expr) => {
                                (name.clone(), expr.clone())
                            }
                        })
                        .collect();
                    runtime_ops.push(RuntimeOp::Select(SelectConfig { fields }));
                }
                StreamOp::Emit {
                    output_type: _,
                    fields: args,
                    target_context,
                } => {
                    // Check if any args have complex expressions (not just strings or idents)
                    let has_complex_expr = args.iter().any(|arg| {
                        !matches!(
                            &arg.value,
                            varpulis_core::ast::Expr::Str(_) | varpulis_core::ast::Expr::Ident(_)
                        )
                    });

                    if has_complex_expr {
                        // Use EmitExpr for complex expressions with function evaluation
                        let fields: Vec<(String, varpulis_core::ast::Expr)> = args
                            .iter()
                            .map(|arg| (arg.name.clone(), arg.value.clone()))
                            .collect();
                        runtime_ops.push(RuntimeOp::EmitExpr(EmitExprConfig {
                            fields,
                            target_context: target_context.clone(),
                        }));
                    } else {
                        // Use simple EmitConfig for string/ident only
                        let fields: Vec<(String, String)> = args
                            .iter()
                            .filter_map(|arg| {
                                let value = match &arg.value {
                                    varpulis_core::ast::Expr::Str(s) => s.clone(),
                                    varpulis_core::ast::Expr::Ident(s) => s.clone(),
                                    _ => return None,
                                };
                                Some((arg.name.clone(), value))
                            })
                            .collect();
                        runtime_ops.push(RuntimeOp::Emit(EmitConfig {
                            fields,
                            target_context: target_context.clone(),
                        }));
                    }
                }
                StreamOp::Print(exprs) => {
                    runtime_ops.push(RuntimeOp::Print(PrintConfig {
                        exprs: exprs.clone(),
                    }));
                }
                StreamOp::Log(args) => {
                    let mut level = "info".to_string();
                    let mut message = None;
                    let mut data_field = None;

                    for arg in args {
                        match arg.name.as_str() {
                            "level" => {
                                if let varpulis_core::ast::Expr::Str(s) = &arg.value {
                                    level = s.clone();
                                }
                            }
                            "message" => {
                                if let varpulis_core::ast::Expr::Str(s) = &arg.value {
                                    message = Some(s.clone());
                                }
                            }
                            "data" => {
                                if let varpulis_core::ast::Expr::Ident(s) = &arg.value {
                                    data_field = Some(s.clone());
                                }
                            }
                            _ => {}
                        }
                    }

                    runtime_ops.push(RuntimeOp::Log(LogConfig {
                        level,
                        message,
                        data_field,
                    }));
                }
                StreamOp::Where(expr) => {
                    // Store expression for runtime evaluation with user functions
                    runtime_ops.push(RuntimeOp::WhereExpr(expr.clone()));
                }
                StreamOp::AttentionWindow(args) => {
                    // Parse attention window configuration
                    let mut duration_ns = 60_000_000_000u64; // 1 minute default
                    let mut num_heads = 4;
                    let mut embedding_dim = 64;
                    let mut threshold = 0.0f32;

                    for arg in args {
                        match arg.name.as_str() {
                            "duration" => {
                                if let varpulis_core::ast::Expr::Duration(ns) = &arg.value {
                                    duration_ns = *ns;
                                }
                            }
                            "heads" | "num_heads" => {
                                if let varpulis_core::ast::Expr::Int(n) = &arg.value {
                                    num_heads = *n as usize;
                                }
                            }
                            "dim" | "embedding_dim" => {
                                if let varpulis_core::ast::Expr::Int(n) = &arg.value {
                                    embedding_dim = *n as usize;
                                }
                            }
                            "threshold" => {
                                if let varpulis_core::ast::Expr::Float(f) = &arg.value {
                                    threshold = *f as f32;
                                }
                            }
                            _ => {}
                        }
                    }

                    runtime_ops.push(RuntimeOp::AttentionWindow(AttentionWindowConfig {
                        duration_ns,
                        num_heads,
                        embedding_dim,
                        threshold,
                    }));
                }
                StreamOp::Pattern(def) => {
                    runtime_ops.push(RuntimeOp::Pattern(PatternConfig {
                        name: def.name.clone(),
                        matcher: def.matcher.clone(),
                    }));
                }
                StreamOp::Having(expr) => {
                    // Having filter - applied after aggregation
                    runtime_ops.push(RuntimeOp::Having(expr.clone()));
                }
                StreamOp::To {
                    connector_name,
                    params,
                } => {
                    let topic_override = params
                        .iter()
                        .find(|p| p.name == "topic")
                        .and_then(|p| p.value.as_string().map(|s| s.to_string()));
                    let sink_key = if let Some(ref topic) = topic_override {
                        format!("{}::{}", connector_name, topic)
                    } else {
                        connector_name.clone()
                    };
                    runtime_ops.push(RuntimeOp::To(ToConfig {
                        connector_name: connector_name.clone(),
                        topic_override,
                        sink_key,
                    }));
                }
                StreamOp::Process(expr) => {
                    runtime_ops.push(RuntimeOp::Process(expr.clone()));
                }
                StreamOp::On(_) => {
                    // Join condition - handled by extract_join_keys(), not a runtime op
                }
                other => {
                    return Err(format!(
                        "unsupported stream operation: {}",
                        stream_op_name(other)
                    ));
                }
            }
        }

        // Check if we're in trend aggregation mode (Hamlet) or detection mode (SASE)
        if let Some(ref agg_items) = trend_agg_items {
            // === Trend Aggregation Mode (Hamlet) ===
            // Build Hamlet aggregator instead of SASE engine.

            let event_types = pattern_analyzer::extract_event_types(source, &followed_by_clauses);
            let kleene_info = pattern_analyzer::extract_kleene_info(source, &followed_by_clauses);
            let window_ms = pattern_analyzer::extract_within_ms(within_expr_for_hamlet.as_ref());

            // Build a type_indices map for aggregate function resolution
            let mut type_indices_map = std::collections::HashMap::new();

            // Build MergedTemplate via TemplateBuilder
            use crate::hamlet::template::TemplateBuilder;
            let mut builder = TemplateBuilder::new();
            let query_id: crate::greta::QueryId = 0;

            // Register event types as a sequence
            let type_strs: Vec<&str> = event_types.iter().map(|s| s.as_str()).collect();
            builder.add_sequence(query_id, &type_strs);

            // Build type_indices from the template after adding sequence
            // (TemplateBuilder registers types internally)
            let template_preview = builder.build();
            for et in &event_types {
                if let Some(idx) = template_preview.type_index(et) {
                    type_indices_map.insert(et.clone(), idx);
                }
            }

            // Also map aliases to type indices
            // Source alias
            match source {
                StreamSource::IdentWithAlias { name, alias } => {
                    if let Some(idx) = type_indices_map.get(name) {
                        type_indices_map.insert(alias.clone(), *idx);
                    }
                }
                StreamSource::AllWithAlias {
                    name,
                    alias: Some(alias),
                } => {
                    if let Some(idx) = type_indices_map.get(name) {
                        type_indices_map.insert(alias.clone(), *idx);
                    }
                }
                _ => {}
            }
            for clause in &followed_by_clauses {
                if let Some(ref alias) = clause.alias {
                    if let Some(idx) = type_indices_map.get(&clause.event_type) {
                        type_indices_map.insert(alias.clone(), *idx);
                    }
                }
            }

            // Rebuild template with Kleene info
            let mut builder = TemplateBuilder::new();
            builder.add_sequence(query_id, &type_strs);

            for ki in &kleene_info {
                // The Kleene state in the template is at position ki.position
                // (the state index after the transition for that event type)
                let state = ki.position as u16;
                builder.add_kleene(query_id, &ki.event_type, state);
            }

            let template = builder.build();

            // Create Hamlet aggregator
            let config = crate::hamlet::HamletConfig {
                window_ms,
                incremental: true,
                ..Default::default()
            };
            let mut aggregator = crate::hamlet::HamletAggregator::new(config, template);

            // Convert trend_agg_items to GretaAggregate list
            let fields: Vec<(String, crate::greta::GretaAggregate)> = agg_items
                .iter()
                .map(|item| {
                    let agg = pattern_analyzer::trend_item_to_greta(item, &type_indices_map);
                    (item.alias.clone(), agg)
                })
                .collect();

            // Use the first aggregate for the query registration
            let primary_aggregate = fields
                .first()
                .map(|(_, agg)| *agg)
                .unwrap_or(crate::greta::GretaAggregate::CountTrends);

            // Build Kleene types
            let kleene_types: smallvec::SmallVec<[u16; 4]> = kleene_info
                .iter()
                .filter_map(|ki| type_indices_map.get(&ki.event_type).copied())
                .collect();

            // Build event type indices
            let event_type_indices: smallvec::SmallVec<[u16; 4]> = event_types
                .iter()
                .filter_map(|et| type_indices_map.get(et).copied())
                .collect();

            aggregator.register_query(crate::hamlet::QueryRegistration {
                id: query_id,
                event_types: event_type_indices,
                kleene_types,
                aggregate: primary_aggregate,
            });

            // Insert TrendAggregate op at the beginning (replaces Sequence)
            runtime_ops.insert(
                0,
                RuntimeOp::TrendAggregate(TrendAggregateConfig { fields, query_id }),
            );

            info!(
                "Created Hamlet aggregator for trend aggregation ({} event types, {} Kleene patterns)",
                event_types.len(),
                kleene_info.len()
            );

            return Ok((runtime_ops, None, sequence_event_types, Some(aggregator)));
        }

        // === Detection Mode (SASE) ===
        // Build SASE+ engine if we have sequence patterns or a named pattern reference
        let is_pattern_ref =
            matches!(source, StreamSource::Ident(name) if self.patterns.contains_key(name));
        let sase_engine =
            if !followed_by_clauses.is_empty() || matches!(source, StreamSource::Sequence(_)) {
                // Add Sequence operation marker at the beginning
                runtime_ops.insert(0, RuntimeOp::Sequence);

                // Create stream resolver for derived streams
                let stream_resolver = |name: &str| -> Option<compiler::DerivedStreamInfo> {
                    let stream_def = self.streams.get(name)?;

                    // Extract event type from the stream source
                    let event_type = match &stream_def.source {
                        RuntimeSource::EventType(et) => et.clone(),
                        RuntimeSource::Stream(s) => s.clone(),
                        _ => return None, // Join/Merge sources not supported as derived streams
                    };

                    // Find the first WhereExpr in operations (the stream's filter)
                    let filter = stream_def.operations.iter().find_map(|op| {
                        if let RuntimeOp::WhereExpr(expr) = op {
                            Some(expr.clone())
                        } else {
                            None
                        }
                    });

                    Some(compiler::DerivedStreamInfo { event_type, filter })
                };

                // Compile to SASE+ pattern with stream resolution
                if let Some(pattern) = compiler::compile_to_sase_pattern_with_resolver(
                    source,
                    &followed_by_clauses,
                    &negation_clauses,
                    global_within,
                    &stream_resolver,
                ) {
                    let mut engine = SaseEngine::new(pattern);

                    // Apply partition if specified
                    if let Some(ref key) = partition_key {
                        engine = engine.with_partition_by(key.clone());
                    }

                    // Add global negation conditions
                    for clause in &negation_clauses {
                        let predicate = clause
                            .filter
                            .as_ref()
                            .and_then(compiler::expr_to_sase_predicate);
                        engine.add_negation(clause.event_type.clone(), predicate);
                    }

                    info!("Created SASE+ engine for sequence pattern");
                    Some(engine)
                } else {
                    warn!("Failed to compile SASE+ pattern");
                    None
                }
            } else if is_pattern_ref {
                // Named pattern reference: compile the pattern's SasePatternExpr directly
                let pattern_name = match source {
                    StreamSource::Ident(name) => name,
                    _ => unreachable!(),
                };
                let named_pattern = &self.patterns[pattern_name];

                // Extract within duration from the pattern declaration
                let within_duration = named_pattern.within.as_ref().and_then(|expr| {
                    if let varpulis_core::ast::Expr::Duration(ns) = expr {
                        Some(std::time::Duration::from_nanos(*ns))
                    } else {
                        None
                    }
                });

                // Extract partition key from the pattern declaration
                if let Some(varpulis_core::ast::Expr::Ident(field)) =
                    named_pattern.partition_by.as_ref()
                {
                    partition_key = Some(field.clone());
                }

                // Add Sequence operation marker
                runtime_ops.insert(0, RuntimeOp::Sequence);

                // Compile the named pattern expression to a SASE pattern
                if let Some(pattern) =
                    compiler::compile_sase_pattern_expr(&named_pattern.expr, within_duration)
                {
                    let mut engine = SaseEngine::new(pattern);

                    if let Some(ref key) = partition_key {
                        engine = engine.with_partition_by(key.clone());
                    }

                    info!("Created SASE+ engine from named pattern '{}'", pattern_name);
                    Some(engine)
                } else {
                    warn!(
                        "Failed to compile named pattern '{}' to SASE+ engine",
                        pattern_name
                    );
                    None
                }
            } else {
                None
            };

        Ok((runtime_ops, sase_engine, sequence_event_types, None))
    }

    /// Extract and create AttentionWindow from runtime operations
    fn extract_attention_window(&self, ops: &[RuntimeOp]) -> Option<AttentionWindow> {
        for op in ops {
            if let RuntimeOp::AttentionWindow(config) = op {
                let attention_config = AttentionConfig {
                    num_heads: config.num_heads,
                    embedding_dim: config.embedding_dim,
                    threshold: config.threshold,
                    max_history: 1000,
                    embedding_config: EmbeddingConfig::default(),
                    cache_config: Default::default(),
                };
                let duration = std::time::Duration::from_nanos(config.duration_ns);
                return Some(AttentionWindow::new(attention_config, duration));
            }
        }
        None
    }

    /// Extract join keys from join clauses and operations
    /// Returns a map of source_name -> join_key_field
    fn extract_join_keys(
        &self,
        clauses: &[varpulis_core::ast::JoinClause],
        ops: &[StreamOp],
    ) -> FxHashMap<String, String> {
        let mut join_keys: FxHashMap<String, String> = FxHashMap::default();

        // First check clauses for on conditions
        for clause in clauses {
            if let Some(ref on_expr) = clause.on {
                if let Some((source, field)) = self.extract_field_from_expr(on_expr, &clause.source)
                {
                    join_keys.insert(source, field);
                }
            }
        }

        // Then check operations for StreamOp::On
        for op in ops {
            if let StreamOp::On(expr) = op {
                // Parse expressions like: EMA12.symbol == EMA26.symbol
                // or: A.key == B.key and B.key == C.key
                self.extract_join_keys_from_expr(expr, &mut join_keys);
            }
        }

        // Normalize join key source names to match clause source names.
        // The .on() expression may use event type names (e.g., "Transaction") while
        // the clause sources use stream names (e.g., "Transactions").
        let clause_sources: Vec<String> = clauses.iter().map(|c| c.source.clone()).collect();
        let mut normalized_keys: FxHashMap<String, String> = FxHashMap::default();
        for (src, field) in &join_keys {
            if clause_sources.contains(src) {
                // Already matches a clause source
                normalized_keys.insert(src.clone(), field.clone());
            } else {
                // Try to find a clause source whose underlying event type matches
                let mut found = false;
                for clause in clauses {
                    if let Some(stream_def) = self.streams.get(&clause.source) {
                        let matches = match &stream_def.source {
                            RuntimeSource::EventType(et) => et == src,
                            RuntimeSource::Stream(s) => s == src,
                            _ => false,
                        };
                        if matches {
                            normalized_keys.insert(clause.source.clone(), field.clone());
                            found = true;
                            break;
                        }
                    }
                }
                if !found {
                    normalized_keys.insert(src.clone(), field.clone());
                }
            }
        }

        // If no join keys found, use "symbol" as default (common join key)
        if normalized_keys.is_empty() {
            for clause in clauses {
                normalized_keys.insert(clause.source.clone(), "symbol".to_string());
            }
        }

        normalized_keys
    }

    /// Extract join keys from an expression (e.g., EMA12.symbol == EMA26.symbol)
    fn extract_join_keys_from_expr(
        &self,
        expr: &varpulis_core::ast::Expr,
        keys: &mut FxHashMap<String, String>,
    ) {
        use varpulis_core::ast::{BinOp, Expr};

        if let Expr::Binary { op, left, right } = expr {
            match op {
                BinOp::Eq => {
                    // Extract source.field from both sides
                    if let (Some((src1, field1)), Some((src2, field2))) = (
                        self.extract_source_field(left),
                        self.extract_source_field(right),
                    ) {
                        keys.insert(src1, field1);
                        keys.insert(src2, field2);
                    }
                }
                BinOp::And => {
                    // Recursively process both sides for compound conditions
                    self.extract_join_keys_from_expr(left, keys);
                    self.extract_join_keys_from_expr(right, keys);
                }
                _ => {}
            }
        }
    }

    /// Extract source name and field name from an expression like EMA12.symbol
    fn extract_source_field(
        &self,
        expr_node: &varpulis_core::ast::Expr,
    ) -> Option<(String, String)> {
        use varpulis_core::ast::Expr;

        match expr_node {
            Expr::Member { expr, member } => {
                if let Expr::Ident(source) = expr.as_ref() {
                    return Some((source.clone(), member.clone()));
                }
            }
            Expr::Ident(name) => {
                // Simple identifier - might be just a field name
                // Return as field only, source will be inferred
                return Some(("".to_string(), name.clone()));
            }
            _ => {}
        }
        None
    }

    /// Extract a field from an expression for a specific source
    fn extract_field_from_expr(
        &self,
        expr: &varpulis_core::ast::Expr,
        source: &str,
    ) -> Option<(String, String)> {
        use varpulis_core::ast::{BinOp, Expr};

        if let Expr::Binary {
            op: BinOp::Eq,
            left,
            right,
        } = expr
        {
            // Check left side
            if let Some((src, field)) = self.extract_source_field(left) {
                if src == source || src.is_empty() {
                    return Some((source.to_string(), field));
                }
            }
            // Check right side
            if let Some((src, field)) = self.extract_source_field(right) {
                if src == source || src.is_empty() {
                    return Some((source.to_string(), field));
                }
            }
        }
        None
    }

    /// Extract window duration from operations
    fn extract_window_duration(&self, ops: &[StreamOp]) -> Duration {
        for op in ops {
            if let StreamOp::Window(args) = op {
                if let varpulis_core::ast::Expr::Duration(ns) = &args.duration {
                    return Duration::nanoseconds(*ns as i64);
                }
            }
        }
        // Default to 1 minute if no window specified
        Duration::minutes(1)
    }

    /// Process an incoming event
    pub async fn process(&mut self, event: Event) -> Result<(), String> {
        self.events_processed += 1;
        self.process_inner(Arc::new(event)).await
    }

    /// Process a pre-wrapped SharedEvent (zero-copy path for context pipelines)
    pub async fn process_shared(&mut self, event: SharedEvent) -> Result<(), String> {
        self.events_processed += 1;
        self.process_inner(event).await
    }

    /// Internal processing logic shared by process() and process_shared()
    async fn process_inner(&mut self, event: SharedEvent) -> Result<(), String> {
        // Check for late data against the watermark
        if let Some(ref tracker) = self.watermark_tracker {
            if let Some(effective_wm) = tracker.effective_watermark() {
                if event.timestamp < effective_wm {
                    // Event is behind the watermark — check allowed lateness per stream
                    let mut allowed = false;
                    if let Some(stream_names) = self.router.get_routes(&event.event_type) {
                        for sn in stream_names.iter() {
                            if let Some(cfg) = self.late_data_configs.get(sn) {
                                if event.timestamp >= effective_wm - cfg.allowed_lateness {
                                    allowed = true;
                                    break;
                                }
                            }
                        }
                    }
                    if !allowed && !self.late_data_configs.is_empty() {
                        // Route to side-output if configured, otherwise drop
                        let mut routed = false;
                        if let Some(stream_names) = self.router.get_routes(&event.event_type) {
                            for sn in stream_names.iter() {
                                if let Some(cfg) = self.late_data_configs.get(sn) {
                                    if let Some(ref side_stream) = cfg.side_output_stream {
                                        debug!(
                                            "Routing late event to side-output '{}' type={} ts={}",
                                            side_stream, event.event_type, event.timestamp
                                        );
                                        // Create a late-data event with metadata
                                        let mut late_event = (*event).clone();
                                        late_event.event_type = side_stream.clone().into();
                                        self.send_output(late_event);
                                        routed = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if !routed {
                            debug!(
                                "Dropping late event type={} ts={} (watermark={})",
                                event.event_type, event.timestamp, effective_wm
                            );
                        }
                        return Ok(());
                    }
                }
            }
        }

        // Process events with depth limit to prevent infinite loops
        // Each event carries its depth level - use SharedEvent to avoid cloning
        let mut pending_events: Vec<(SharedEvent, usize)> = vec![(event.clone(), 0)];
        const MAX_CHAIN_DEPTH: usize = 10;

        // Observe event in watermark tracker (after processing to not block)
        if let Some(ref mut tracker) = self.watermark_tracker {
            tracker.observe_event(&event.event_type, event.timestamp);

            if let Some(new_wm) = tracker.effective_watermark() {
                if self.last_applied_watermark.is_none_or(|last| new_wm > last) {
                    self.last_applied_watermark = Some(new_wm);
                    // Note: we don't call apply_watermark_to_windows here to avoid
                    // double-mutable-borrow. The caller should periodically flush.
                }
            }
        }

        // Process events iteratively, feeding output to dependent streams
        while let Some((current_event, depth)) = pending_events.pop() {
            // Prevent infinite loops by limiting chain depth
            if depth >= MAX_CHAIN_DEPTH {
                debug!(
                    "Max chain depth reached for event type: {}",
                    current_event.event_type
                );
                continue;
            }

            // Collect stream names to avoid borrowing issues
            // PERF: Arc<[String]> clone is O(1) - just atomic increment, not deep copy
            let stream_names: Arc<[String]> = self
                .router
                .get_routes(&current_event.event_type)
                .cloned()
                .unwrap_or_else(|| Arc::from([]));

            for stream_name in stream_names.iter() {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    let result = Self::process_stream_with_functions(
                        stream,
                        Arc::clone(&current_event),
                        &self.functions,
                        self.sinks.cache(),
                    )
                    .await?;

                    // Check if we need to send output_events (has .process() but no .emit())
                    let send_outputs = result.emitted_events.is_empty()
                        && stream
                            .operations
                            .iter()
                            .any(|op| matches!(op, RuntimeOp::Process(_)));

                    // Send emitted events to output channel (non-blocking)
                    // PERF: Use send_output_shared for zero-copy when using SharedEvent channel
                    if self.output_channel.is_some() {
                        for emitted in &result.emitted_events {
                            self.output_events_emitted += 1;
                            self.send_output_shared(emitted);
                        }
                        if send_outputs {
                            for output in &result.output_events {
                                self.output_events_emitted += 1;
                                self.send_output_shared(output);
                            }
                        }
                    } else {
                        // Benchmark mode: just count, don't send
                        self.output_events_emitted += result.emitted_events.len() as u64;
                        if send_outputs {
                            self.output_events_emitted += result.output_events.len() as u64;
                        }
                    }

                    // Queue output events for processing by dependent streams
                    for output_event in result.output_events {
                        pending_events.push((output_event, depth + 1));
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a batch of events for improved throughput.
    /// More efficient than calling process() repeatedly because:
    /// - Pre-allocates SharedEvents in bulk
    /// - Collects output events and sends in batches
    /// - Amortizes async overhead
    pub async fn process_batch(&mut self, events: Vec<Event>) -> Result<(), String> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();
        self.events_processed += batch_size as u64;

        // Pre-allocate pending events with capacity for batch + some derived events
        // Use VecDeque so we can process in FIFO order (push_back + pop_front)
        let mut pending_events: std::collections::VecDeque<(SharedEvent, usize)> =
            std::collections::VecDeque::with_capacity(batch_size + batch_size / 4);

        // Convert all events to SharedEvents upfront
        for event in events {
            pending_events.push_back((Arc::new(event), 0));
        }

        const MAX_CHAIN_DEPTH: usize = 10;

        // Collect emitted events to send in batch
        let mut emitted_batch: Vec<SharedEvent> = Vec::with_capacity(batch_size / 10);

        // Process all events in FIFO order (critical for sequence patterns!)
        while let Some((current_event, depth)) = pending_events.pop_front() {
            if depth >= MAX_CHAIN_DEPTH {
                debug!(
                    "Max chain depth reached for event type: {}",
                    current_event.event_type
                );
                continue;
            }

            // Get stream names (Arc clone is O(1))
            let stream_names: Arc<[String]> = self
                .router
                .get_routes(&current_event.event_type)
                .cloned()
                .unwrap_or_else(|| Arc::from([]));

            for stream_name in stream_names.iter() {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    let result = Self::process_stream_with_functions(
                        stream,
                        Arc::clone(&current_event),
                        &self.functions,
                        self.sinks.cache(),
                    )
                    .await?;

                    // Collect emitted events for batch sending
                    self.output_events_emitted += result.emitted_events.len() as u64;
                    let has_emitted = !result.emitted_events.is_empty();
                    emitted_batch.extend(result.emitted_events);

                    // If .process() was used but no .emit(), send output_events too
                    if !has_emitted
                        && stream
                            .operations
                            .iter()
                            .any(|op| matches!(op, RuntimeOp::Process(_)))
                    {
                        self.output_events_emitted += result.output_events.len() as u64;
                        emitted_batch.extend(result.output_events.iter().map(Arc::clone));
                    }

                    // Queue output events (push_back to maintain order)
                    for output_event in result.output_events {
                        pending_events.push_back((output_event, depth + 1));
                    }
                }
            }
        }

        // Send all emitted events in batch (non-blocking to avoid async overhead)
        // PERF: Use send_output_shared to avoid cloning in benchmark mode
        for emitted in &emitted_batch {
            self.send_output_shared(emitted);
        }

        Ok(())
    }

    /// Synchronous batch processing for maximum throughput.
    /// Use this when no .to() sink operations are in the pipeline (e.g., benchmark mode).
    /// Avoids async runtime overhead completely.
    pub fn process_batch_sync(&mut self, events: Vec<Event>) -> Result<(), String> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();
        self.events_processed += batch_size as u64;

        // Pre-allocate pending events with capacity for batch + some derived events
        // Use VecDeque so we can process in FIFO order (push_back + pop_front)
        let mut pending_events: std::collections::VecDeque<(SharedEvent, usize)> =
            std::collections::VecDeque::with_capacity(batch_size + batch_size / 4);

        // Convert all events to SharedEvents upfront
        for event in events {
            pending_events.push_back((Arc::new(event), 0));
        }

        const MAX_CHAIN_DEPTH: usize = 10;

        // Collect emitted events to send in batch
        let mut emitted_batch: Vec<SharedEvent> = Vec::with_capacity(batch_size / 10);

        // Process all events in FIFO order (critical for sequence patterns!)
        while let Some((current_event, depth)) = pending_events.pop_front() {
            if depth >= MAX_CHAIN_DEPTH {
                debug!(
                    "Max chain depth reached for event type: {}",
                    current_event.event_type
                );
                continue;
            }

            // Get stream names (Arc clone is O(1))
            let stream_names: Arc<[String]> = self
                .router
                .get_routes(&current_event.event_type)
                .cloned()
                .unwrap_or_else(|| Arc::from([]));

            for stream_name in stream_names.iter() {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    // Skip output clone+rename when stream has no downstream routes
                    let skip_rename = self.router.get_routes(stream_name).is_none();
                    let result = Self::process_stream_sync(
                        stream,
                        Arc::clone(&current_event),
                        &self.functions,
                        skip_rename,
                    )?;

                    // Collect emitted events for batch sending
                    self.output_events_emitted += result.emitted_events.len() as u64;
                    let has_emitted = !result.emitted_events.is_empty();
                    emitted_batch.extend(result.emitted_events);

                    // If .process() was used but no .emit(), send output_events too
                    if !has_emitted
                        && stream
                            .operations
                            .iter()
                            .any(|op| matches!(op, RuntimeOp::Process(_)))
                    {
                        self.output_events_emitted += result.output_events.len() as u64;
                        emitted_batch.extend(result.output_events.iter().map(Arc::clone));
                    }

                    // Queue output events (push_back to maintain order)
                    for output_event in result.output_events {
                        pending_events.push_back((output_event, depth + 1));
                    }
                }
            }
        }

        // Send all emitted events in batch (non-blocking to avoid async overhead)
        // PERF: Use send_output_shared to avoid cloning in benchmark mode
        for emitted in &emitted_batch {
            self.send_output_shared(emitted);
        }

        Ok(())
    }

    /// Synchronous stream processing - no async operations.
    /// Skips .to() sink operations (which are the only async parts).
    /// When `skip_output_rename` is true, output events skip the clone+rename step.
    fn process_stream_sync(
        stream: &mut StreamDefinition,
        event: SharedEvent,
        functions: &FxHashMap<String, UserFunction>,
        skip_output_rename: bool,
    ) -> Result<StreamProcessResult, String> {
        // For merge sources, check if the event passes the appropriate filter
        if let RuntimeSource::Merge(ref sources) = stream.source {
            let mut passes_filter = false;
            for ms in sources {
                if ms.event_type == *event.event_type {
                    if let Some(ref filter) = ms.filter {
                        let ctx = SequenceContext::new();
                        if let Some(result) = evaluator::eval_expr_with_functions(
                            filter,
                            &event,
                            &ctx,
                            functions,
                            &FxHashMap::default(),
                        ) {
                            if result.as_bool().unwrap_or(false) {
                                passes_filter = true;
                                break;
                            }
                        }
                    } else {
                        passes_filter = true;
                        break;
                    }
                }
            }
            if !passes_filter {
                return Ok(StreamProcessResult {
                    emitted_events: vec![],
                    output_events: vec![],
                });
            }
        }

        // For join sources - return empty (join requires async in some paths)
        if matches!(stream.source, RuntimeSource::Join(_)) {
            return Ok(StreamProcessResult {
                emitted_events: vec![],
                output_events: vec![],
            });
        }

        // Process through attention window if present — skip clone when not needed
        let pipeline_event = if stream.attention_window.is_some() {
            let mut enriched_event = (*event).clone();
            if let Some(ref mut attention_window) = stream.attention_window {
                let result = attention_window.process((*event).clone());
                let attention_score = if result.scores.is_empty() {
                    0.0
                } else {
                    result
                        .scores
                        .iter()
                        .map(|(_, s)| *s)
                        .fold(f32::NEG_INFINITY, f32::max)
                };
                enriched_event.data.insert(
                    "attention_score".into(),
                    Value::Float(attention_score as f64),
                );
                let context_norm: f32 = result.context.iter().map(|x| x * x).sum::<f32>().sqrt();
                enriched_event.data.insert(
                    "attention_context_norm".into(),
                    Value::Float(context_norm as f64),
                );
                enriched_event.data.insert(
                    "attention_matches".into(),
                    Value::Int(result.scores.len() as i64),
                );
            }
            Arc::new(enriched_event)
        } else {
            event // zero-copy: reuse the existing Arc
        };

        // Use synchronous pipeline execution
        pipeline::execute_pipeline_sync(
            stream,
            vec![pipeline_event],
            0,
            pipeline::SkipFlags::none(),
            functions,
            skip_output_rename,
        )
    }

    /// Process a batch of pre-wrapped SharedEvents (zero-copy path for context pipelines)
    pub async fn process_batch_shared(&mut self, events: Vec<SharedEvent>) -> Result<(), String> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();
        self.events_processed += batch_size as u64;

        // Use VecDeque so we can process in FIFO order (critical for sequence patterns!)
        let mut pending_events: std::collections::VecDeque<(SharedEvent, usize)> =
            std::collections::VecDeque::with_capacity(batch_size + batch_size / 4);

        for event in events {
            pending_events.push_back((event, 0));
        }

        const MAX_CHAIN_DEPTH: usize = 10;

        let mut emitted_batch: Vec<SharedEvent> = Vec::with_capacity(batch_size / 10);

        // Process all events in FIFO order
        while let Some((current_event, depth)) = pending_events.pop_front() {
            if depth >= MAX_CHAIN_DEPTH {
                debug!(
                    "Max chain depth reached for event type: {}",
                    current_event.event_type
                );
                continue;
            }

            let stream_names: Arc<[String]> = self
                .router
                .get_routes(&current_event.event_type)
                .cloned()
                .unwrap_or_else(|| Arc::from([]));

            for stream_name in stream_names.iter() {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    let result = Self::process_stream_with_functions(
                        stream,
                        Arc::clone(&current_event),
                        &self.functions,
                        self.sinks.cache(),
                    )
                    .await?;

                    self.output_events_emitted += result.emitted_events.len() as u64;
                    let has_emitted = !result.emitted_events.is_empty();
                    emitted_batch.extend(result.emitted_events);

                    if !has_emitted
                        && stream
                            .operations
                            .iter()
                            .any(|op| matches!(op, RuntimeOp::Process(_)))
                    {
                        self.output_events_emitted += result.output_events.len() as u64;
                        emitted_batch.extend(result.output_events.iter().map(Arc::clone));
                    }

                    for output_event in result.output_events {
                        pending_events.push_back((output_event, depth + 1));
                    }
                }
            }
        }

        // PERF: Use send_output_shared to avoid cloning in benchmark mode
        for emitted in &emitted_batch {
            self.send_output_shared(emitted);
        }

        Ok(())
    }

    async fn process_stream_with_functions(
        stream: &mut StreamDefinition,
        event: SharedEvent,
        functions: &FxHashMap<String, UserFunction>,
        sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
    ) -> Result<StreamProcessResult, String> {
        // For merge sources, check if the event passes the appropriate filter
        if let RuntimeSource::Merge(ref sources) = stream.source {
            let mut passes_filter = false;
            let mut matched_source_name = None;
            for ms in sources {
                if ms.event_type == *event.event_type {
                    if let Some(ref filter) = ms.filter {
                        let ctx = SequenceContext::new();
                        if let Some(result) = evaluator::eval_expr_with_functions(
                            filter,
                            &event,
                            &ctx,
                            functions,
                            &FxHashMap::default(),
                        ) {
                            if result.as_bool().unwrap_or(false) {
                                passes_filter = true;
                                matched_source_name = Some(&ms.name);
                                break;
                            }
                        }
                    } else {
                        // No filter means it passes
                        passes_filter = true;
                        matched_source_name = Some(&ms.name);
                        break;
                    }
                }
            }
            if !passes_filter {
                return Ok(StreamProcessResult {
                    emitted_events: vec![],
                    output_events: vec![],
                });
            }
            // Log which merge source matched (uses ms.name)
            if let Some(source_name) = matched_source_name {
                tracing::trace!("Event matched merge source: {}", source_name);
            }
        }

        // For join sources, route through the JoinBuffer for correlation
        if let RuntimeSource::Join(ref _sources) = stream.source {
            if let Some(ref mut join_buffer) = stream.join_buffer {
                // Determine which source this event came from using the event_type_to_source mapping
                // This maps event types (e.g., "MarketATick") to source names (e.g., "MarketA")
                let source_name = stream
                    .event_type_to_source
                    .get(&*event.event_type)
                    .cloned()
                    .unwrap_or_else(|| event.event_type.to_string());

                tracing::debug!(
                    "Join stream {}: Adding event from source '{}' (event_type: {})",
                    stream.name,
                    source_name,
                    event.event_type
                );

                // Add event to join buffer and try to correlate (join still needs owned Event)
                match join_buffer.add_event(&source_name, (*event).clone()) {
                    Some(correlated_event) => {
                        tracing::debug!(
                            "Join stream {}: Correlated event with {} fields",
                            stream.name,
                            correlated_event.data.len()
                        );
                        // Continue processing with the correlated event
                        return Self::process_join_result(
                            stream,
                            Arc::new(correlated_event),
                            functions,
                            sinks,
                        )
                        .await;
                    }
                    None => {
                        // No correlation yet - need events from all sources
                        tracing::debug!(
                            "Join stream {}: No correlation yet, waiting for more events (buffer stats: {:?})",
                            stream.name,
                            join_buffer.stats()
                        );
                        return Ok(StreamProcessResult {
                            emitted_events: vec![],
                            output_events: vec![],
                        });
                    }
                }
            } else {
                tracing::warn!("Join stream {} has no JoinBuffer configured", stream.name);
                return Ok(StreamProcessResult {
                    emitted_events: vec![],
                    output_events: vec![],
                });
            }
        }

        // Process through attention window if present - compute and add attention_score
        // We need to enrich the event, so clone and modify
        let mut enriched_event = (*event).clone();
        if let Some(ref mut attention_window) = stream.attention_window {
            let result = attention_window.process((*event).clone());

            // Compute aggregate attention score (max of all scores)
            let attention_score = if result.scores.is_empty() {
                0.0
            } else {
                result
                    .scores
                    .iter()
                    .map(|(_, s)| *s)
                    .fold(f32::NEG_INFINITY, f32::max)
            };

            // Add attention_score to event data for use in expressions
            enriched_event.data.insert(
                "attention_score".into(),
                Value::Float(attention_score as f64),
            );

            // Add attention context vector norm as additional metric
            let context_norm: f32 = result.context.iter().map(|x| x * x).sum::<f32>().sqrt();
            enriched_event.data.insert(
                "attention_context_norm".into(),
                Value::Float(context_norm as f64),
            );

            // Add number of correlated events
            enriched_event.data.insert(
                "attention_matches".into(),
                Value::Int(result.scores.len() as i64),
            );
        }

        // Delegate to unified pipeline execution
        pipeline::execute_pipeline(
            stream,
            vec![Arc::new(enriched_event)],
            0,
            pipeline::SkipFlags::none(),
            functions,
            sinks,
        )
        .await
    }

    /// Process a join result through the stream operations (skipping join-specific handling)
    async fn process_join_result(
        stream: &mut StreamDefinition,
        correlated_event: SharedEvent,
        functions: &FxHashMap<String, UserFunction>,
        sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
    ) -> Result<StreamProcessResult, String> {
        // Delegate to unified pipeline with join-specific skip flags
        pipeline::execute_pipeline(
            stream,
            vec![correlated_event],
            0,
            pipeline::SkipFlags::for_join(),
            functions,
            sinks,
        )
        .await
    }

    // =========================================================================
    // Session Window Sweep
    // =========================================================================

    /// Check if any registered stream uses `.to()` sink operations.
    /// When no sinks are present, the sync processing path can be used safely.
    pub fn has_sink_operations(&self) -> bool {
        self.streams
            .values()
            .any(|s| s.operations.iter().any(|op| matches!(op, RuntimeOp::To(_))))
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

    /// Flush all expired session windows and process the resulting events
    /// through the remaining pipeline stages (aggregate, having, select, emit, etc.).
    pub async fn flush_expired_sessions(&mut self) -> Result<(), String> {
        let now = chrono::Utc::now();
        let stream_names: Vec<String> = self.streams.keys().cloned().collect();

        for stream_name in stream_names {
            // Step 1: Find the window op index and collect expired events
            let (window_idx, expired) = {
                let stream = self.streams.get_mut(&stream_name).unwrap();
                let mut result = Vec::new();
                let mut found_idx = None;

                for (idx, op) in stream.operations.iter_mut().enumerate() {
                    if let RuntimeOp::Window(window) = op {
                        match window {
                            WindowType::Session(w) => {
                                if let Some(events) = w.check_expired(now) {
                                    result = events;
                                }
                                found_idx = Some(idx);
                            }
                            WindowType::PartitionedSession(w) => {
                                for (_key, events) in w.check_expired(now) {
                                    result.extend(events);
                                }
                                found_idx = Some(idx);
                            }
                            _ => {}
                        }
                        // Only process the first session window op per stream
                        if found_idx.is_some() {
                            break;
                        }
                    }
                }
                (found_idx, result)
            };

            if expired.is_empty() {
                continue;
            }

            let window_idx = match window_idx {
                Some(idx) => idx,
                None => continue,
            };

            // Step 2: Process expired events through the post-window pipeline
            let result = Self::process_post_window(
                self.streams.get_mut(&stream_name).unwrap(),
                expired,
                window_idx,
                &self.functions,
                self.sinks.cache(),
            )
            .await?;

            // Step 3: Send emitted events to output channel
            for emitted in &result.emitted_events {
                self.output_events_emitted += 1;
                let owned = (**emitted).clone();
                self.send_output(owned);
            }
        }

        Ok(())
    }

    /// Process events through the pipeline operations that come after the window
    /// at `window_idx`. This runs aggregate, having, select, emit, etc.
    async fn process_post_window(
        stream: &mut StreamDefinition,
        events: Vec<SharedEvent>,
        window_idx: usize,
        functions: &FxHashMap<String, UserFunction>,
        sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
    ) -> Result<StreamProcessResult, String> {
        // Delegate to unified pipeline starting after the window with post-window skip flags
        pipeline::execute_pipeline(
            stream,
            events,
            window_idx + 1,
            pipeline::SkipFlags::for_post_window(),
            functions,
            sinks,
        )
        .await
    }

    /// Get metrics
    pub fn metrics(&self) -> EngineMetrics {
        EngineMetrics {
            events_processed: self.events_processed,
            output_events_emitted: self.output_events_emitted,
            streams_count: self.streams.len(),
        }
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
    /// Returns: Vec<(interval_ns, initial_delay_ns, timer_event_type)>
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

    // =========================================================================
    // Hot Reload
    // =========================================================================

    /// Reload program without losing state where possible.
    ///
    /// State preservation rules:
    /// - Filter changes: state preserved
    /// - Window size changes: state reset
    /// - Aggregation changes: state reset
    /// - New streams: added fresh
    /// - Removed streams: dropped
    ///
    /// # Example
    /// ```ignore
    /// let new_program = varpulis_parser::parse(&new_source)?;
    /// let report = engine.reload(&new_program)?;
    /// println!("Reload complete: {:?}", report);
    /// ```
    pub fn reload(&mut self, program: &Program) -> Result<ReloadReport, String> {
        let mut report = ReloadReport::default();

        // Collect current stream names
        let old_streams: FxHashSet<String> = self.streams.keys().cloned().collect();

        // Parse new program to get new stream definitions
        // We need to compile the new program to compare with existing streams
        let mut new_engine = Self::new_internal(self.clone_output_channel());
        new_engine.load(program)?;

        let new_streams: FxHashSet<String> = new_engine.streams.keys().cloned().collect();

        // Find added, removed, and potentially updated streams
        for name in new_streams.difference(&old_streams) {
            report.streams_added.push(name.clone());
        }

        for name in old_streams.difference(&new_streams) {
            report.streams_removed.push(name.clone());
        }

        // For streams that exist in both, check if they changed
        for name in old_streams.intersection(&new_streams) {
            let old_stream = self.streams.get(name).unwrap();
            let new_stream = new_engine.streams.get(name).unwrap();

            // Compare source types
            let source_changed = !Self::sources_compatible(&old_stream.source, &new_stream.source);

            // Compare operation counts (rough heuristic)
            let ops_changed = old_stream.operations.len() != new_stream.operations.len();

            if source_changed || ops_changed {
                report.streams_updated.push(name.clone());
                report.state_reset.push(name.clone());
            } else {
                // Source and ops count match - try to preserve state
                report.state_preserved.push(name.clone());
            }
        }

        // Now apply changes

        // Remove old streams
        for name in &report.streams_removed {
            self.streams.remove(name);
        }

        // Rebuild event_sources from scratch (simpler than trying to update Arc<[String]> incrementally)
        self.router.clear();

        // Add/update streams from new engine
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

        // Rebuild event_sources for all streams
        // First collect all (event_type, stream_name) pairs to avoid borrow issues
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
                    RuntimeSource::Join(_) => {
                        // Join sources handled separately
                    }
                    RuntimeSource::Timer(config) => {
                        pairs.push((config.timer_event_type.clone(), name.clone()));
                    }
                }
                pairs
            })
            .collect();

        // Now apply registrations
        for (event_type, stream_name) in registrations {
            self.router.add_route(&event_type, &stream_name);
        }

        // Update functions
        self.functions = new_engine.functions;

        // Update patterns
        self.patterns = new_engine.patterns;

        // Update configs
        self.configs = new_engine.configs;

        // Update context map
        self.context_map = new_engine.context_map;

        // Update connectors, source bindings, and sinks
        self.connectors = new_engine.connectors;
        self.source_bindings = new_engine.source_bindings;
        *self.sinks.cache_mut() = std::mem::take(new_engine.sinks.cache_mut());

        // Preserve variables (user might have set them)
        // Only add new variables from program, don't overwrite existing
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

    /// Create a checkpoint of the engine state (windows, SASE engines, joins, variables).
    pub fn create_checkpoint(&self) -> crate::persistence::EngineCheckpoint {
        use crate::persistence::{EngineCheckpoint, WindowCheckpoint};

        let mut window_states = std::collections::HashMap::new();
        let mut sase_states = std::collections::HashMap::new();
        let mut join_states = std::collections::HashMap::new();

        for (name, stream) in &self.streams {
            // Checkpoint windows
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
                        };
                        window_states.insert(name.clone(), cp);
                    }
                    RuntimeOp::PartitionedWindow(pw) => {
                        // Serialize partitioned count windows
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
                    _ => {}
                }
            }

            // Checkpoint SASE engines
            if let Some(ref sase) = stream.sase_engine {
                sase_states.insert(name.clone(), sase.checkpoint());
            }

            // Checkpoint join buffers
            if let Some(ref jb) = stream.join_buffer {
                join_states.insert(name.clone(), jb.checkpoint());
            }
        }

        // Checkpoint variables
        let variables = self
            .variables
            .iter()
            .map(|(k, v)| (k.clone(), crate::persistence::value_to_ser(v)))
            .collect();

        let watermark_state = self.watermark_tracker.as_ref().map(|t| t.checkpoint());

        EngineCheckpoint {
            window_states,
            sase_states,
            join_states,
            variables,
            events_processed: self.events_processed,
            output_events_emitted: self.output_events_emitted,
            watermark_state,
        }
    }

    /// Restore engine state from a checkpoint.
    ///
    /// Must be called after `load()` so that stream definitions exist.
    pub fn restore_checkpoint(&mut self, cp: &crate::persistence::EngineCheckpoint) {
        // Restore counters
        self.events_processed = cp.events_processed;
        self.output_events_emitted = cp.output_events_emitted;

        // Restore variables
        for (k, sv) in &cp.variables {
            self.variables
                .insert(k.clone(), crate::persistence::ser_to_value(sv.clone()));
        }

        // Restore per-stream state
        for (name, stream) in &mut self.streams {
            // Restore window state
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
                        },
                        RuntimeOp::PartitionedWindow(pw) => {
                            // Restore partitioned count windows
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

            // Restore SASE engine state
            if let Some(scp) = cp.sase_states.get(name) {
                if let Some(ref mut sase) = stream.sase_engine {
                    sase.restore(scp);
                }
            }

            // Restore join buffer state
            if let Some(jcp) = cp.join_states.get(name) {
                if let Some(ref mut jb) = stream.join_buffer {
                    jb.restore(jcp);
                }
            }
        }

        // Restore watermark tracker state
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
            "Engine restored: {} events processed, {} streams with state",
            cp.events_processed,
            cp.window_states.len() + cp.sase_states.len() + cp.join_states.len()
        );
    }

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

    /// Advance the watermark from an external source (e.g., upstream context).
    pub async fn advance_external_watermark(
        &mut self,
        source_context: &str,
        watermark_ms: i64,
    ) -> Result<(), String> {
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

    /// Apply a watermark advance to all windows, triggering closure of expired windows.
    async fn apply_watermark_to_windows(&mut self, wm: DateTime<Utc>) -> Result<(), String> {
        let stream_names: Vec<String> = self.streams.keys().cloned().collect();

        for stream_name in stream_names {
            let (window_idx, expired) = {
                let stream = self.streams.get_mut(&stream_name).unwrap();
                let mut result = Vec::new();
                let mut found_idx = None;

                for (idx, op) in stream.operations.iter_mut().enumerate() {
                    if let RuntimeOp::Window(window) = op {
                        let events: Option<Vec<SharedEvent>> = match window {
                            WindowType::Tumbling(w) => w.advance_watermark(wm),
                            WindowType::Sliding(w) => w.advance_watermark(wm),
                            WindowType::Session(w) => w.advance_watermark(wm),
                            WindowType::PartitionedTumbling(w) => {
                                let parts = w.advance_watermark(wm);
                                let all: Vec<_> = parts.into_iter().flat_map(|(_, e)| e).collect();
                                if all.is_empty() {
                                    None
                                } else {
                                    Some(all)
                                }
                            }
                            WindowType::PartitionedSliding(w) => {
                                let parts = w.advance_watermark(wm);
                                let all: Vec<_> = parts.into_iter().flat_map(|(_, e)| e).collect();
                                if all.is_empty() {
                                    None
                                } else {
                                    Some(all)
                                }
                            }
                            WindowType::PartitionedSession(w) => {
                                let parts = w.advance_watermark(wm);
                                let all: Vec<_> = parts.into_iter().flat_map(|(_, e)| e).collect();
                                if all.is_empty() {
                                    None
                                } else {
                                    Some(all)
                                }
                            }
                            _ => None, // Count-based windows don't use watermarks
                        };

                        if let Some(evts) = events {
                            result = evts;
                            found_idx = Some(idx);
                        }
                        break;
                    }
                }
                (found_idx, result)
            };

            if expired.is_empty() {
                continue;
            }

            let window_idx = match window_idx {
                Some(idx) => idx,
                None => continue,
            };

            let result = Self::process_post_window(
                self.streams.get_mut(&stream_name).unwrap(),
                expired,
                window_idx,
                &self.functions,
                self.sinks.cache(),
            )
            .await?;

            for emitted in &result.emitted_events {
                self.output_events_emitted += 1;
                let owned = (**emitted).clone();
                self.send_output(owned);
            }
        }

        Ok(())
    }

    /// Check if two runtime sources are compatible for state preservation
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

fn stream_op_name(op: &StreamOp) -> &'static str {
    match op {
        StreamOp::Where(_) => ".where()",
        StreamOp::Select(_) => ".select()",
        StreamOp::Window(_) => ".window()",
        StreamOp::Aggregate(_) => ".aggregate()",
        StreamOp::Having(_) => ".having()",
        StreamOp::PartitionBy(_) => ".partition_by()",
        StreamOp::OrderBy(_) => ".order_by()",
        StreamOp::Limit(_) => ".limit()",
        StreamOp::Distinct(_) => ".distinct()",
        StreamOp::Map(_) => ".map()",
        StreamOp::Filter(_) => ".filter()",
        StreamOp::Tap(_) => ".tap()",
        StreamOp::Print(_) => ".print()",
        StreamOp::Log(_) => ".log()",
        StreamOp::Emit { .. } => ".emit()",
        StreamOp::To { .. } => ".to()",
        StreamOp::ToExpr(_) => ".to()",
        StreamOp::Pattern(_) => ".pattern()",
        StreamOp::AttentionWindow(_) => ".attention_window()",
        StreamOp::Concurrent(_) => ".concurrent()",
        StreamOp::Process(_) => ".process()",
        StreamOp::OnError(_) => ".on_error()",
        StreamOp::Collect => ".collect()",
        StreamOp::On(_) => ".on()",
        StreamOp::FollowedBy(_) => "-> (followed_by)",
        StreamOp::Within(_) => ".within()",
        StreamOp::Not(_) => ".not()",
        StreamOp::Fork(_) => ".fork()",
        StreamOp::Any(_) => ".any()",
        StreamOp::All => ".all()",
        StreamOp::First => ".first()",
        StreamOp::Context(_) => ".context()",
        StreamOp::Watermark(_) => ".watermark()",
        StreamOp::AllowedLateness(_) => ".allowed_lateness()",
        StreamOp::TrendAggregate(_) => ".trend_aggregate()",
    }
}
