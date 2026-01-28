//! Main execution engine for Varpulis
//!
//! This module provides the core engine that processes events and executes
//! stream definitions written in VarpulisQL.

mod compiler;
mod evaluator;
mod types;

#[cfg(test)]
mod tests;

// Re-export public types
pub use types::{Alert, EngineConfig, EngineMetrics, UserFunction};

// Re-export evaluator for use by other modules (e.g., SASE+)
pub use evaluator::eval_filter_expr;

// Re-export internal types for use within the engine module
use types::{
    AttentionWindowConfig, EmitConfig, EmitExprConfig, LogConfig, MergeSource,
    PartitionedAggregatorState, PartitionedSlidingCountWindowState, PartitionedWindowState,
    PatternConfig, PrintConfig, RuntimeOp, RuntimeSource, SelectConfig, StreamDefinition,
    StreamProcessResult, WindowType,
};

use crate::aggregation::Aggregator;
use crate::attention::{AttentionConfig, AttentionWindow, EmbeddingConfig};
use crate::event::Event;
use crate::join::JoinBuffer;
use crate::metrics::Metrics;
use crate::pattern::PatternEngine;
use crate::sase::SaseEngine;
use crate::sequence::SequenceContext;
use crate::window::{
    CountWindow, PartitionedSlidingWindow, PartitionedTumblingWindow, SlidingCountWindow,
    SlidingWindow, TumblingWindow,
};
use chrono::Duration;
use indexmap::IndexMap;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use varpulis_core::ast::{ConfigItem, Program, Stmt, StreamOp, StreamSource};
use varpulis_core::Value;

/// The main Varpulis engine
pub struct Engine {
    /// Registered stream definitions
    streams: HashMap<String, StreamDefinition>,
    /// Event type to stream mapping
    event_sources: HashMap<String, Vec<String>>,
    /// User-defined functions
    functions: HashMap<String, UserFunction>,
    /// Configuration blocks (e.g., mqtt, kafka)
    configs: HashMap<String, EngineConfig>,
    /// Alert sender
    alert_tx: mpsc::Sender<Alert>,
    /// Metrics
    events_processed: u64,
    alerts_generated: u64,
    /// Prometheus metrics
    metrics: Option<Metrics>,
}

impl Engine {
    pub fn new(alert_tx: mpsc::Sender<Alert>) -> Self {
        Self {
            streams: HashMap::new(),
            event_sources: HashMap::new(),
            functions: HashMap::new(),
            configs: HashMap::new(),
            alert_tx,
            events_processed: 0,
            alerts_generated: 0,
            metrics: None,
        }
    }

    /// Get a configuration block by name
    pub fn get_config(&self, name: &str) -> Option<&EngineConfig> {
        self.configs.get(name)
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
            stream
                .operations
                .insert(0, RuntimeOp::WhereClosure(Box::new(filter)));
            Ok(())
        } else {
            Err(format!("Stream '{}' not found", stream_name))
        }
    }

    /// Load a program into the engine
    pub fn load(&mut self, program: &Program) -> Result<(), String> {
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
                    let mut values = HashMap::new();
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
                Stmt::Import { path, alias } => {
                    info!("Import statement: {} (alias: {:?})", path, alias);
                    // TODO: Load and merge imported file
                }
                _ => {
                    debug!("Skipping statement: {:?}", stmt.node);
                }
            }
        }
        Ok(())
    }

    fn register_stream(
        &mut self,
        name: &str,
        source: &StreamSource,
        ops: &[StreamOp],
    ) -> Result<(), String> {
        // Check if we have sequence operations and build SASE+ engine
        let (runtime_ops, sase_engine, sequence_event_types) =
            self.compile_ops_with_sequences(source, ops)?;

        let runtime_source = match source {
            StreamSource::From(event_type) => {
                self.event_sources
                    .entry(event_type.clone())
                    .or_default()
                    .push(name.to_string());
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::Ident(stream_name) => {
                // Register for the stream source event type
                self.event_sources
                    .entry(stream_name.clone())
                    .or_default()
                    .push(name.to_string());
                RuntimeSource::Stream(stream_name.clone())
            }
            StreamSource::IdentWithAlias {
                name: event_type, ..
            } => {
                // Register for the event type (alias is handled in sequence)
                self.event_sources
                    .entry(event_type.clone())
                    .or_default()
                    .push(name.to_string());
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::AllWithAlias {
                name: event_type, ..
            } => {
                // Register for the event type (all + alias handled in sequence)
                self.event_sources
                    .entry(event_type.clone())
                    .or_default()
                    .push(name.to_string());
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::Sequence(decl) => {
                // Register for all event types in the sequence
                for step in &decl.steps {
                    self.event_sources
                        .entry(step.event_type.clone())
                        .or_default()
                        .push(name.to_string());
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
                // Register for all join source streams
                for source in &sources {
                    self.event_sources
                        .entry(source.clone())
                        .or_default()
                        .push(name.to_string());
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
                    let streams = self.event_sources.entry(ms.event_type.clone()).or_default();
                    if !streams.contains(&name.to_string()) {
                        streams.push(name.to_string());
                    }
                }

                info!(
                    "Registering merge stream {} with {} sources",
                    name,
                    merge_sources.len()
                );
                RuntimeSource::Merge(merge_sources)
            }
        };

        // Register for all event types in sequence (avoid duplicates)
        for event_type in &sequence_event_types {
            let streams = self.event_sources.entry(event_type.clone()).or_default();
            if !streams.contains(&name.to_string()) {
                streams.push(name.to_string());
            }
        }
        if !sequence_event_types.is_empty() {
            debug!(
                "Stream {} registered for sequence event types: {:?}",
                name, sequence_event_types
            );
        }

        // Extract attention window config from operations if present
        let attention_window = self.extract_attention_window(&runtime_ops);

        // Extract pattern engine from operations if present
        let pattern_engine = self.extract_pattern_engine(&runtime_ops);

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
                pattern_engine,
                sase_engine,
                join_buffer,
            },
        );

        info!("Registered stream: {} (source: {})", name, source_desc);
        Ok(())
    }

    fn compile_ops_with_sequences(
        &self,
        source: &StreamSource,
        ops: &[StreamOp],
    ) -> Result<(Vec<RuntimeOp>, Option<SaseEngine>, Vec<String>), String> {
        let mut runtime_ops = Vec::new();
        let mut sequence_event_types: Vec<String> = Vec::new();
        let mut partition_key: Option<String> = None;

        // For SASE+ pattern compilation
        let mut followed_by_clauses: Vec<varpulis_core::ast::FollowedByClause> = Vec::new();
        let mut negation_clauses: Vec<varpulis_core::ast::FollowedByClause> = Vec::new();
        let mut global_within: Option<std::time::Duration> = None;

        // Collect sequence event types from source
        if let StreamSource::Sequence(decl) = source {
            for step in &decl.steps {
                sequence_event_types.push(step.event_type.clone());
            }
        }

        for op in ops {
            match op {
                StreamOp::FollowedBy(clause) => {
                    // Store raw clause for SASE+ compilation
                    followed_by_clauses.push(clause.clone());
                    sequence_event_types.push(clause.event_type.clone());
                    continue;
                }
                StreamOp::Within(expr) => {
                    // Parse duration from expression
                    let duration_ns = match expr {
                        varpulis_core::ast::Expr::Duration(ns) => *ns,
                        _ => 300_000_000_000u64, // 5 minutes default
                    };
                    global_within = Some(std::time::Duration::from_nanos(duration_ns));
                    continue;
                }
                StreamOp::Not(clause) => {
                    // Store negation clause for SASE+ engine
                    negation_clauses.push(clause.clone());
                    // Add negation event type to sequence event types so it gets routed
                    if !sequence_event_types.contains(&clause.event_type) {
                        sequence_event_types.push(clause.event_type.clone());
                    }
                    continue;
                }
                _ => {}
            }

            // Handle non-sequence operations
            match op {
                StreamOp::Window(args) => {
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
                                            PartitionedTumblingWindow::new(key.clone(), duration),
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
                StreamOp::Emit(args) => {
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
                        runtime_ops.push(RuntimeOp::EmitExpr(EmitExprConfig { fields }));
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
                        runtime_ops.push(RuntimeOp::Emit(EmitConfig { fields }));
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
                _ => {
                    debug!("Skipping operation: {:?}", op);
                }
            }
        }

        // Build SASE+ engine if we have sequence patterns
        let sase_engine =
            if !followed_by_clauses.is_empty() || matches!(source, StreamSource::Sequence(_)) {
                // Add Sequence operation marker at the beginning
                runtime_ops.insert(0, RuntimeOp::Sequence);

                // Compile to SASE+ pattern
                if let Some(pattern) = compiler::compile_to_sase_pattern(
                    source,
                    &followed_by_clauses,
                    &negation_clauses,
                    global_within,
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
            } else {
                None
            };

        Ok((runtime_ops, sase_engine, sequence_event_types))
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

    /// Extract and create PatternEngine from runtime operations
    fn extract_pattern_engine(&self, ops: &[RuntimeOp]) -> Option<PatternEngine> {
        for op in ops {
            if let RuntimeOp::Pattern(config) = op {
                // Try to convert the matcher expression to a PatternExpr
                if let Some(pattern_expr) = compiler::expr_to_pattern(&config.matcher) {
                    tracing::debug!("Created pattern engine for pattern: {}", config.name);
                    let mut engine = PatternEngine::new();
                    engine.track(pattern_expr);
                    return Some(engine);
                }
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
    ) -> HashMap<String, String> {
        let mut join_keys: HashMap<String, String> = HashMap::new();

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

        // If no join keys found, use "symbol" as default (common join key)
        if join_keys.is_empty() {
            for clause in clauses {
                join_keys.insert(clause.source.clone(), "symbol".to_string());
            }
        }

        join_keys
    }

    /// Extract join keys from an expression (e.g., EMA12.symbol == EMA26.symbol)
    fn extract_join_keys_from_expr(
        &self,
        expr: &varpulis_core::ast::Expr,
        keys: &mut HashMap<String, String>,
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

        // Process events with depth limit to prevent infinite loops
        // Each event carries its depth level
        let mut pending_events: Vec<(Event, usize)> = vec![(event, 0)];
        const MAX_CHAIN_DEPTH: usize = 10;

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

            let stream_names = self
                .event_sources
                .get(&current_event.event_type)
                .cloned()
                .unwrap_or_default();

            for stream_name in stream_names {
                if let Some(stream) = self.streams.get_mut(&stream_name) {
                    let result = Self::process_stream_with_functions(
                        stream,
                        current_event.clone(),
                        &self.functions,
                    )
                    .await?;

                    // Send alerts
                    for alert in result.alerts {
                        self.alerts_generated += 1;
                        if let Err(e) = self.alert_tx.send(alert).await {
                            warn!("Failed to send alert: {}", e);
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

    async fn process_stream_with_functions(
        stream: &mut StreamDefinition,
        event: Event,
        functions: &HashMap<String, UserFunction>,
    ) -> Result<StreamProcessResult, String> {
        // For merge sources, check if the event passes the appropriate filter
        if let RuntimeSource::Merge(ref sources) = stream.source {
            let mut passes_filter = false;
            let mut matched_source_name = None;
            for ms in sources {
                if ms.event_type == event.event_type {
                    if let Some(ref filter) = ms.filter {
                        let ctx = SequenceContext::new();
                        if let Some(result) = evaluator::eval_expr_with_functions(
                            filter,
                            &event,
                            &ctx,
                            functions,
                            &HashMap::new(),
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
                    alerts: vec![],
                    output_events: vec![],
                });
            }
            // Log which merge source matched (uses ms.name)
            if let Some(source_name) = matched_source_name {
                tracing::trace!("Event matched merge source: {}", source_name);
            }
        }

        // For join sources, route through the JoinBuffer for correlation
        if let RuntimeSource::Join(ref sources) = stream.source {
            if let Some(ref mut join_buffer) = stream.join_buffer {
                // Determine which source this event came from
                // The event_type should match one of the source stream names
                let source_name = sources
                    .iter()
                    .find(|s| **s == event.event_type)
                    .cloned()
                    .unwrap_or_else(|| event.event_type.clone());

                tracing::debug!(
                    "Join stream {}: Adding event from source '{}' (event_type: {})",
                    stream.name,
                    source_name,
                    event.event_type
                );

                // Add event to join buffer and try to correlate
                match join_buffer.add_event(&source_name, event.clone()) {
                    Some(correlated_event) => {
                        tracing::debug!(
                            "Join stream {}: Correlated event with {} fields",
                            stream.name,
                            correlated_event.data.len()
                        );
                        // Continue processing with the correlated event
                        return Self::process_join_result(stream, correlated_event, functions)
                            .await;
                    }
                    None => {
                        // No correlation yet - need events from all sources
                        tracing::trace!(
                            "Join stream {}: No correlation yet, waiting for more events",
                            stream.name
                        );
                        return Ok(StreamProcessResult {
                            alerts: vec![],
                            output_events: vec![],
                        });
                    }
                }
            } else {
                tracing::warn!("Join stream {} has no JoinBuffer configured", stream.name);
                return Ok(StreamProcessResult {
                    alerts: vec![],
                    output_events: vec![],
                });
            }
        }

        // Process through attention window if present - compute and add attention_score
        let mut enriched_event = event.clone();
        if let Some(ref mut attention_window) = stream.attention_window {
            let result = attention_window.process(event);

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
                "attention_score".to_string(),
                Value::Float(attention_score as f64),
            );

            // Add attention context vector norm as additional metric
            let context_norm: f32 = result.context.iter().map(|x| x * x).sum::<f32>().sqrt();
            enriched_event.data.insert(
                "attention_context_norm".to_string(),
                Value::Float(context_norm as f64),
            );

            // Add number of correlated events
            enriched_event.data.insert(
                "attention_matches".to_string(),
                Value::Int(result.scores.len() as i64),
            );
        }

        // Process pattern engine if present
        if let Some(ref mut pattern_engine) = stream.pattern_engine {
            let event_clone = enriched_event.clone();
            let matched_patterns = pattern_engine.process(&event_clone);
            if !matched_patterns.is_empty() {
                // Pattern matched - add matched event types to the event data
                enriched_event
                    .data
                    .insert("pattern_matched".to_string(), Value::Bool(true));
                let total_events: usize =
                    matched_patterns.iter().map(|ctx| ctx.captured.len()).sum();
                enriched_event.data.insert(
                    "pattern_events_count".to_string(),
                    Value::Int(total_events as i64),
                );
            }
        }

        let mut current_events = vec![enriched_event];
        let mut alerts = Vec::new();

        for op in &mut stream.operations {
            match op {
                RuntimeOp::WhereClosure(predicate) => {
                    current_events.retain(|e| predicate(e));
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::WhereExpr(expr) => {
                    let ctx = SequenceContext::new();
                    current_events.retain(|e| {
                        evaluator::eval_expr_with_functions(
                            expr,
                            e,
                            &ctx,
                            functions,
                            &HashMap::new(),
                        )
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    });
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::Window(window) => {
                    let mut window_results = Vec::new();
                    for event in current_events {
                        match window {
                            WindowType::Tumbling(w) => {
                                if let Some(completed) = w.add(event) {
                                    window_results = completed;
                                }
                            }
                            WindowType::Sliding(w) => {
                                if let Some(window_events) = w.add(event) {
                                    window_results = window_events;
                                }
                            }
                            WindowType::Count(w) => {
                                if let Some(completed) = w.add(event) {
                                    window_results = completed;
                                }
                            }
                            WindowType::SlidingCount(w) => {
                                if let Some(window_events) = w.add(event) {
                                    window_results = window_events;
                                }
                            }
                            WindowType::PartitionedTumbling(w) => {
                                if let Some(completed) = w.add(event) {
                                    window_results = completed;
                                }
                            }
                            WindowType::PartitionedSliding(w) => {
                                if let Some(window_events) = w.add(event) {
                                    window_results = window_events;
                                }
                            }
                        }
                    }
                    current_events = window_results;
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::PartitionedWindow(state) => {
                    let mut window_results = Vec::new();
                    for event in current_events {
                        if let Some(completed) = state.add(event) {
                            window_results.extend(completed);
                        }
                    }
                    current_events = window_results;
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::PartitionedSlidingCountWindow(state) => {
                    let mut window_results = Vec::new();
                    for event in current_events {
                        if let Some(completed) = state.add(event) {
                            window_results.extend(completed);
                        }
                    }
                    current_events = window_results;
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::Aggregate(aggregator) => {
                    let result = aggregator.apply(&current_events);
                    // Create synthetic event from aggregation result
                    let mut agg_event = Event::new("AggregationResult");
                    for (key, value) in result {
                        agg_event.data.insert(key, value);
                    }
                    current_events = vec![agg_event];
                }
                RuntimeOp::PartitionedAggregate(state) => {
                    let results = state.apply(&current_events);
                    // Create one synthetic event per partition
                    current_events = results
                        .into_iter()
                        .map(|(partition_key, result)| {
                            let mut agg_event = Event::new("AggregationResult");
                            agg_event
                                .data
                                .insert("_partition".to_string(), Value::Str(partition_key));
                            for (key, value) in result {
                                agg_event.data.insert(key, value);
                            }
                            agg_event
                        })
                        .collect();
                }
                RuntimeOp::Select(config) => {
                    // Transform events by evaluating expressions and creating new fields
                    let ctx = SequenceContext::new();
                    current_events = current_events
                        .into_iter()
                        .map(|event| {
                            let mut new_event = Event::new(&event.event_type);
                            new_event.timestamp = event.timestamp;
                            for (out_name, expr) in &config.fields {
                                if let Some(value) = evaluator::eval_expr_with_functions(
                                    expr,
                                    &event,
                                    &ctx,
                                    functions,
                                    &HashMap::new(),
                                ) {
                                    new_event.data.insert(out_name.clone(), value);
                                }
                            }
                            new_event
                        })
                        .collect();
                }
                RuntimeOp::Emit(config) => {
                    for event in &current_events {
                        let mut alert_data = IndexMap::new();
                        for (out_name, source) in &config.fields {
                            if let Some(value) = event.get(source) {
                                alert_data.insert(out_name.clone(), value.clone());
                            } else {
                                alert_data.insert(out_name.clone(), Value::Str(source.clone()));
                            }
                        }

                        let alert = Alert {
                            alert_type: "stream_output".to_string(),
                            severity: "info".to_string(),
                            message: format!("Output from stream {}", stream.name),
                            data: alert_data,
                        };
                        alerts.push(alert);
                    }
                }
                RuntimeOp::Print(config) => {
                    for event in &current_events {
                        let mut parts = Vec::new();
                        for expr in &config.exprs {
                            let value =
                                evaluator::eval_filter_expr(expr, event, &SequenceContext::new())
                                    .unwrap_or(Value::Null);
                            parts.push(format!("{}", value));
                        }
                        let output = if parts.is_empty() {
                            format!("[{}] {}: {:?}", stream.name, event.event_type, event.data)
                        } else {
                            parts.join(" ")
                        };
                        println!("[PRINT] {}", output);
                    }
                }
                RuntimeOp::Log(config) => {
                    for event in &current_events {
                        let msg = config
                            .message
                            .clone()
                            .unwrap_or_else(|| event.event_type.clone());
                        let data = if let Some(ref field) = config.data_field {
                            event
                                .get(field)
                                .map(|v| format!("{}", v))
                                .unwrap_or_default()
                        } else {
                            format!("{:?}", event.data)
                        };

                        match config.level.as_str() {
                            "error" => {
                                tracing::error!(stream = %stream.name, message = %msg, data = %data, "Stream log")
                            }
                            "warn" | "warning" => {
                                tracing::warn!(stream = %stream.name, message = %msg, data = %data, "Stream log")
                            }
                            "debug" => {
                                tracing::debug!(stream = %stream.name, message = %msg, data = %data, "Stream log")
                            }
                            "trace" => {
                                tracing::trace!(stream = %stream.name, message = %msg, data = %data, "Stream log")
                            }
                            _ => {
                                tracing::info!(stream = %stream.name, message = %msg, data = %data, "Stream log")
                            }
                        }
                    }
                }
                RuntimeOp::Sequence => {
                    // Process events through SASE+ engine (NFA-based pattern matching)
                    let mut sequence_results = Vec::new();

                    if let Some(ref mut sase) = stream.sase_engine {
                        for event in &current_events {
                            let matches = sase.process(event);
                            for match_result in matches {
                                // Create synthetic event from completed sequence
                                let mut seq_event = Event::new("SequenceMatch");
                                seq_event
                                    .data
                                    .insert("stream".to_string(), Value::Str(stream.name.clone()));
                                seq_event.data.insert(
                                    "match_duration_ms".to_string(),
                                    Value::Int(match_result.duration.as_millis() as i64),
                                );
                                // Add captured events to the result
                                for (alias, captured) in &match_result.captured {
                                    for (k, v) in &captured.data {
                                        seq_event
                                            .data
                                            .insert(format!("{}_{}", alias, k), v.clone());
                                    }
                                }
                                sequence_results.push(seq_event);
                            }
                        }
                    }

                    if sequence_results.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                    current_events = sequence_results;
                }
                RuntimeOp::EmitExpr(config) => {
                    let ctx = SequenceContext::new();
                    for event in &current_events {
                        let mut alert_data = IndexMap::new();
                        for (out_name, expr) in &config.fields {
                            if let Some(value) = evaluator::eval_expr_with_functions(
                                expr,
                                event,
                                &ctx,
                                functions,
                                &HashMap::new(),
                            ) {
                                alert_data.insert(out_name.clone(), value);
                            }
                        }

                        let alert = Alert {
                            alert_type: "stream_output".to_string(),
                            severity: "info".to_string(),
                            message: format!("Output from stream {}", stream.name),
                            data: alert_data,
                        };
                        alerts.push(alert);
                    }
                }
                RuntimeOp::AttentionWindow(_config) => {
                    // AttentionWindow is handled at stream level before operations
                }
                RuntimeOp::Pattern(config) => {
                    // Pattern matching: evaluate the matcher expression with events as context
                    // The matcher is a lambda: events => predicate
                    let ctx = SequenceContext::new();
                    let events_value = Value::Array(
                        current_events
                            .iter()
                            .map(|e| {
                                let mut map = IndexMap::new();
                                map.insert(
                                    "event_type".to_string(),
                                    Value::Str(e.event_type.clone()),
                                );
                                for (k, v) in &e.data {
                                    map.insert(k.clone(), v.clone());
                                }
                                Value::Map(map)
                            })
                            .collect(),
                    );

                    // Create a context with "events" bound
                    let mut pattern_vars = HashMap::new();
                    pattern_vars.insert("events".to_string(), events_value);

                    // Evaluate the pattern matcher
                    if let Some(result) = evaluator::eval_pattern_expr(
                        &config.matcher,
                        &current_events,
                        &ctx,
                        functions,
                        &pattern_vars,
                        stream.attention_window.as_ref(),
                    ) {
                        if !result.as_bool().unwrap_or(false) {
                            // Pattern didn't match, filter out all events
                            current_events.clear();
                            return Ok(StreamProcessResult {
                                alerts,
                                output_events: vec![],
                            });
                        }
                    }
                }
            }
        }

        // Return remaining events as output for dependent streams
        // Set their event_type to the stream name for routing
        let output_events: Vec<Event> = current_events
            .into_iter()
            .map(|mut e| {
                e.event_type = stream.name.clone();
                e
            })
            .collect();

        Ok(StreamProcessResult {
            alerts,
            output_events,
        })
    }

    /// Process a join result through the stream operations (skipping join-specific handling)
    async fn process_join_result(
        stream: &mut StreamDefinition,
        correlated_event: Event,
        functions: &HashMap<String, UserFunction>,
    ) -> Result<StreamProcessResult, String> {
        let mut current_events = vec![correlated_event];
        let mut alerts = Vec::new();

        for op in &mut stream.operations {
            match op {
                RuntimeOp::WhereClosure(predicate) => {
                    current_events.retain(|e| predicate(e));
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::WhereExpr(expr) => {
                    let ctx = SequenceContext::new();
                    current_events.retain(|e| {
                        evaluator::eval_expr_with_functions(
                            expr,
                            e,
                            &ctx,
                            functions,
                            &HashMap::new(),
                        )
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    });
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::Window(_window) => {
                    // For joins, we skip the window operation since it's already
                    // handled by the JoinBuffer's window duration
                    // Just pass events through
                }
                RuntimeOp::PartitionedWindow(state) => {
                    let mut window_results = Vec::new();
                    for event in current_events {
                        if let Some(completed) = state.add(event) {
                            window_results.extend(completed);
                        }
                    }
                    current_events = window_results;
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::PartitionedSlidingCountWindow(state) => {
                    let mut window_results = Vec::new();
                    for event in current_events {
                        if let Some(completed) = state.add(event) {
                            window_results.extend(completed);
                        }
                    }
                    current_events = window_results;
                    if current_events.is_empty() {
                        return Ok(StreamProcessResult {
                            alerts,
                            output_events: vec![],
                        });
                    }
                }
                RuntimeOp::Aggregate(aggregator) => {
                    let result = aggregator.apply(&current_events);
                    let mut agg_event = Event::new("AggregationResult");
                    for (key, value) in result {
                        agg_event.data.insert(key, value);
                    }
                    current_events = vec![agg_event];
                }
                RuntimeOp::PartitionedAggregate(state) => {
                    let results = state.apply(&current_events);
                    current_events = results
                        .into_iter()
                        .map(|(partition_key, result)| {
                            let mut agg_event = Event::new("AggregationResult");
                            agg_event
                                .data
                                .insert("_partition".to_string(), Value::Str(partition_key));
                            for (key, value) in result {
                                agg_event.data.insert(key, value);
                            }
                            agg_event
                        })
                        .collect();
                }
                RuntimeOp::Select(config) => {
                    let ctx = SequenceContext::new();
                    current_events = current_events
                        .into_iter()
                        .map(|event| {
                            let mut new_event = Event::new(&event.event_type);
                            new_event.timestamp = event.timestamp;
                            for (out_name, expr) in &config.fields {
                                if let Some(value) = evaluator::eval_expr_with_functions(
                                    expr,
                                    &event,
                                    &ctx,
                                    functions,
                                    &HashMap::new(),
                                ) {
                                    new_event.data.insert(out_name.clone(), value);
                                }
                            }
                            new_event
                        })
                        .collect();
                }
                RuntimeOp::Emit(config) => {
                    for event in &current_events {
                        let mut alert_data = IndexMap::new();
                        for (out_name, source_field) in &config.fields {
                            if out_name == "event_type" {
                                continue;
                            }
                            if let Some(value) = event.get(source_field) {
                                alert_data.insert(out_name.clone(), value.clone());
                            } else {
                                alert_data
                                    .insert(out_name.clone(), Value::Str(source_field.clone()));
                            }
                        }

                        let alert_type = config
                            .fields
                            .iter()
                            .find(|(name, _)| name == "event_type")
                            .map(|(_, val)| val.clone())
                            .unwrap_or_else(|| stream.name.clone());

                        let alert = Alert {
                            alert_type,
                            severity: "info".to_string(),
                            message: format!("Output from join stream {}", stream.name),
                            data: alert_data,
                        };
                        alerts.push(alert);
                    }
                }
                RuntimeOp::EmitExpr(config) => {
                    let ctx = SequenceContext::new();
                    for event in &current_events {
                        let mut alert_data = IndexMap::new();
                        let mut event_type = "stream_output".to_string();

                        for (out_name, expr) in &config.fields {
                            if let Some(value) = evaluator::eval_expr_with_functions(
                                expr,
                                event,
                                &ctx,
                                functions,
                                &HashMap::new(),
                            ) {
                                if out_name == "event_type" {
                                    if let Some(s) = value.as_str() {
                                        event_type = s.to_string();
                                    }
                                } else {
                                    alert_data.insert(out_name.clone(), value);
                                }
                            }
                        }

                        let alert = Alert {
                            alert_type: event_type,
                            severity: "info".to_string(),
                            message: format!("Output from join stream {}", stream.name),
                            data: alert_data,
                        };
                        alerts.push(alert);
                    }
                }
                RuntimeOp::Print(_)
                | RuntimeOp::Log(_)
                | RuntimeOp::Sequence
                | RuntimeOp::AttentionWindow(_)
                | RuntimeOp::Pattern(_) => {
                    // Skip these for join results
                }
            }
        }

        // Return remaining events as output for dependent streams
        let output_events: Vec<Event> = current_events
            .into_iter()
            .map(|mut e| {
                e.event_type = stream.name.clone();
                e
            })
            .collect();

        Ok(StreamProcessResult {
            alerts,
            output_events,
        })
    }

    /// Get metrics
    pub fn metrics(&self) -> EngineMetrics {
        EngineMetrics {
            events_processed: self.events_processed,
            alerts_generated: self.alerts_generated,
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
}
