//! Main execution engine for Varpulis

use crate::aggregation::{
    AggBinOp, Aggregator, Avg, Count, CountDistinct, Ema, ExprAggregate, First, Last, Max, Min,
    StdDev, Sum,
};
use crate::attention::{AttentionConfig, AttentionWindow, EmbeddingConfig};
use crate::event::Event;
use crate::metrics::Metrics;
use crate::pattern::{PatternBuilder, PatternEngine, PatternExpr};
use crate::sase::SaseEngine;
use crate::sequence::{SequenceContext, SequenceFilter, SequenceStep, SequenceTracker};
use crate::window::{SlidingWindow, TumblingWindow};
use chrono::Duration;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use varpulis_core::ast::{Program, Stmt, StreamOp, StreamSource};
use varpulis_core::Value;

/// Alert emitted by the engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_type: String,
    pub severity: String,
    pub message: String,
    pub data: IndexMap<String, Value>,
}

/// The main Varpulis engine
pub struct Engine {
    /// Registered stream definitions
    streams: HashMap<String, StreamDefinition>,
    /// Event type to stream mapping
    event_sources: HashMap<String, Vec<String>>,
    /// User-defined functions
    functions: HashMap<String, UserFunction>,
    /// Alert sender
    alert_tx: mpsc::Sender<Alert>,
    /// Metrics
    events_processed: u64,
    alerts_generated: u64,
    /// Prometheus metrics
    metrics: Option<Metrics>,
}

/// User-defined function
#[derive(Clone)]
pub struct UserFunction {
    pub name: String,
    pub params: Vec<(String, varpulis_core::Type)>, // (name, type)
    pub return_type: Option<varpulis_core::Type>,
    pub body: Vec<varpulis_core::span::Spanned<varpulis_core::Stmt>>,
}

/// Runtime stream definition
#[allow(dead_code)]
struct StreamDefinition {
    name: String,
    source: RuntimeSource,
    operations: Vec<RuntimeOp>,
    /// Sequence tracker for followed-by operations
    sequence_tracker: Option<SequenceTracker>,
    /// Attention window for correlation scoring
    attention_window: Option<AttentionWindow>,
    /// Pattern engine for Apama-style pattern matching (legacy)
    pattern_engine: Option<PatternEngine>,
    /// SASE+ pattern matching engine (new, more efficient)
    sase_engine: Option<SaseEngine>,
}

enum RuntimeSource {
    EventType(String),
    Stream(String),
    Join(Vec<String>),
    /// Merge multiple event types with optional filters
    Merge(Vec<MergeSource>),
}

impl RuntimeSource {
    /// Get a description of this source for logging
    fn describe(&self) -> String {
        match self {
            RuntimeSource::EventType(t) => format!("event:{}", t),
            RuntimeSource::Stream(s) => format!("stream:{}", s),
            RuntimeSource::Join(sources) => format!("join:{}", sources.join(",")),
            RuntimeSource::Merge(sources) => format!("merge:{} sources", sources.len()),
        }
    }
}

/// A source in a merge construct with optional filter
struct MergeSource {
    name: String,
    event_type: String,
    filter: Option<varpulis_core::ast::Expr>,
}

enum RuntimeOp {
    /// Filter with closure (for sequence filters with context)
    WhereClosure(Box<dyn Fn(&Event) -> bool + Send + Sync>),
    /// Filter with expression (evaluated at runtime with user functions)
    WhereExpr(varpulis_core::ast::Expr),
    Window(WindowType),
    Aggregate(Aggregator),
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

struct PatternConfig {
    name: String,
    matcher: varpulis_core::ast::Expr,
}

struct AttentionWindowConfig {
    duration_ns: u64,
    num_heads: usize,
    embedding_dim: usize,
    threshold: f32,
}

struct PrintConfig {
    exprs: Vec<varpulis_core::ast::Expr>,
}

struct LogConfig {
    level: String,
    message: Option<String>,
    data_field: Option<String>,
}

enum WindowType {
    Tumbling(TumblingWindow),
    Sliding(SlidingWindow),
}

struct EmitConfig {
    fields: Vec<(String, String)>, // (output_name, source_field or literal)
}

struct EmitExprConfig {
    fields: Vec<(String, varpulis_core::ast::Expr)>, // (output_name, expression)
}

impl Engine {
    pub fn new(alert_tx: mpsc::Sender<Alert>) -> Self {
        Self {
            streams: HashMap::new(),
            event_sources: HashMap::new(),
            functions: HashMap::new(),
            alert_tx,
            events_processed: 0,
            alerts_generated: 0,
            metrics: None,
        }
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
        // Check if we have sequence operations and build tracker
        let (runtime_ops, sequence_tracker, sequence_event_types) =
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
            debug!("Stream {} registered for sequence event types: {:?}", name, sequence_event_types);
        }

        // Extract attention window config from operations if present
        let attention_window = self.extract_attention_window(&runtime_ops);

        // Extract pattern engine from operations if present
        let pattern_engine = self.extract_pattern_engine(&runtime_ops);

        // Log source description before moving
        let source_desc = runtime_source.describe();

        self.streams.insert(
            name.to_string(),
            StreamDefinition {
                name: name.to_string(),
                source: runtime_source,
                operations: runtime_ops,
                sequence_tracker,
                attention_window,
                pattern_engine,
                sase_engine: None, // TODO: integrate SASE+ pattern matching
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
    ) -> Result<(Vec<RuntimeOp>, Option<SequenceTracker>, Vec<String>), String> {
        let mut runtime_ops = Vec::new();
        let mut sequence_steps: Vec<SequenceStep> = Vec::new();
        let mut sequence_event_types: Vec<String> = Vec::new();
        let mut match_all_first = false;
        let mut current_timeout: Option<std::time::Duration> = None;
        let mut negation_conditions: Vec<(String, Option<SequenceFilter>)> = Vec::new();

        // Handle sequence() construct - converts SequenceDecl to SequenceSteps
        if let StreamSource::Sequence(decl) = source {
            match_all_first = decl.match_all;
            for step in &decl.steps {
                let filter = step
                    .filter
                    .as_ref()
                    .map(|expr| Self::compile_sequence_filter(expr.clone()));
                let timeout = step.timeout.as_ref().and_then(|expr| {
                    if let varpulis_core::ast::Expr::Duration(ns) = expr.as_ref() {
                        Some(std::time::Duration::from_nanos(*ns))
                    } else {
                        None
                    }
                });
                sequence_steps.push(SequenceStep {
                    event_type: step.event_type.clone(),
                    filter,
                    alias: Some(step.alias.clone()),
                    timeout,
                    match_all: false,
                });
                sequence_event_types.push(step.event_type.clone());
            }
        }

        // Check if we have any FollowedBy operations - if so, add source as first step
        let has_followed_by = ops.iter().any(|op| matches!(op, StreamOp::FollowedBy(_)));
        if has_followed_by && sequence_steps.is_empty() {
            let (source_event_type, source_alias, source_match_all) = match source {
                StreamSource::Ident(name) => (name.clone(), None, false),
                StreamSource::IdentWithAlias { name, alias } => {
                    (name.clone(), Some(alias.clone()), false)
                }
                StreamSource::AllWithAlias { name, alias } => (name.clone(), alias.clone(), true),
                StreamSource::From(name) => (name.clone(), None, false),
                StreamSource::Sequence(_) => {
                    return Err("Sequence source already handled".to_string())
                }
                _ => return Err("Sequences require a single event type source".to_string()),
            };
            match_all_first = source_match_all;
            // First step is the source event type
            sequence_steps.push(SequenceStep {
                event_type: source_event_type,
                filter: None,
                alias: source_alias,
                timeout: None,
                match_all: false,
            });
        }

        for op in ops {
            match op {
                StreamOp::FollowedBy(clause) => {
                    let filter = clause
                        .filter
                        .as_ref()
                        .map(|expr| Self::compile_sequence_filter(expr.clone()));
                    let step = SequenceStep {
                        event_type: clause.event_type.clone(),
                        filter,
                        alias: clause.alias.clone(),
                        timeout: current_timeout.take(),
                        match_all: clause.match_all,
                    };
                    // Note: match_all_first is set from source (AllWithAlias), not from FollowedBy
                    sequence_event_types.push(clause.event_type.clone());
                    sequence_steps.push(step);
                    continue;
                }
                StreamOp::Within(expr) => {
                    // Parse duration from expression
                    let duration_ns = match expr {
                        varpulis_core::ast::Expr::Duration(ns) => *ns,
                        _ => 300_000_000_000u64, // 5 minutes default
                    };
                    current_timeout = Some(std::time::Duration::from_nanos(duration_ns));
                    continue;
                }
                StreamOp::Not(clause) => {
                    // Store negation for later addition to sequence tracker
                    let filter = clause
                        .filter
                        .as_ref()
                        .map(|expr| Self::compile_sequence_filter(expr.clone()));
                    // Add negation event type to sequence event types so it gets routed
                    if !sequence_event_types.contains(&clause.event_type) {
                        sequence_event_types.push(clause.event_type.clone());
                    }
                    negation_conditions.push((clause.event_type.clone(), filter));
                    continue;
                }
                _ => {}
            }

            // Handle non-sequence operations
            match op {
                StreamOp::Window(args) => {
                    // Parse duration from expression
                    // For MVP, assume it's a duration literal
                    let duration_ns = match &args.duration {
                        varpulis_core::ast::Expr::Duration(ns) => *ns,
                        _ => 300_000_000_000, // 5 minutes default
                    };
                    let duration = Duration::nanoseconds(duration_ns as i64);

                    if let Some(sliding) = &args.sliding {
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
                StreamOp::Aggregate(items) => {
                    let mut aggregator = Aggregator::new();
                    for item in items {
                        if let Some((func, field)) = Self::compile_agg_expr(&item.expr) {
                            aggregator = aggregator.add(item.alias.clone(), func, field);
                        }
                    }
                    runtime_ops.push(RuntimeOp::Aggregate(aggregator));
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

        // Build sequence tracker if we have sequence steps (more than just the source)
        let sequence_tracker = if sequence_steps.len() > 1 {
            // Add Sequence operation marker at the beginning
            runtime_ops.insert(0, RuntimeOp::Sequence);
            let mut tracker = SequenceTracker::new(sequence_steps, match_all_first);

            // Add negation conditions
            for (event_type, filter) in negation_conditions {
                tracker.add_negation(crate::sequence::NegationCondition { event_type, filter });
            }

            Some(tracker)
        } else {
            None
        };

        Ok((runtime_ops, sequence_tracker, sequence_event_types))
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
                if let Some(pattern_expr) = Self::expr_to_pattern(&config.matcher) {
                    tracing::debug!("Created pattern engine for pattern: {}", config.name);
                    let mut engine = PatternEngine::new();
                    engine.track(pattern_expr);
                    return Some(engine);
                }
            }
        }
        None
    }

    /// Convert an AST expression to a PatternExpr for the pattern engine
    fn expr_to_pattern(expr: &varpulis_core::ast::Expr) -> Option<PatternExpr> {
        use varpulis_core::ast::{BinOp, Expr, UnaryOp};

        match expr {
            // Identifier = event type
            Expr::Ident(name) => Some(PatternBuilder::event(name)),

            // Binary operators: ->, and, or, xor
            Expr::Binary { op, left, right } => {
                let left_pattern = Self::expr_to_pattern(left)?;
                let right_pattern = Self::expr_to_pattern(right)?;

                match op {
                    BinOp::FollowedBy => {
                        Some(PatternBuilder::followed_by(left_pattern, right_pattern))
                    }
                    BinOp::And => Some(PatternBuilder::and(left_pattern, right_pattern)),
                    BinOp::Or => Some(PatternBuilder::or(left_pattern, right_pattern)),
                    BinOp::Xor => Some(PatternBuilder::xor(left_pattern, right_pattern)),
                    _ => None,
                }
            }

            // Unary not
            Expr::Unary {
                op: UnaryOp::Not,
                expr: inner,
            } => {
                let inner_pattern = Self::expr_to_pattern(inner)?;
                Some(PatternBuilder::not(inner_pattern))
            }

            // Call expression: could be all(A) or within(A, 30s) or event with filter
            Expr::Call { func, args } => {
                if let Expr::Ident(func_name) = func.as_ref() {
                    match func_name.as_str() {
                        "all" => {
                            if let Some(varpulis_core::ast::Arg::Positional(inner)) = args.first() {
                                let inner_pattern = Self::expr_to_pattern(inner)?;
                                return Some(PatternBuilder::all(inner_pattern));
                            }
                        }
                        "within" => {
                            // within(pattern, duration)
                            if args.len() >= 2 {
                                if let (
                                    Some(varpulis_core::ast::Arg::Positional(inner)),
                                    Some(varpulis_core::ast::Arg::Positional(dur_expr)),
                                ) = (args.first(), args.get(1))
                                {
                                    let inner_pattern = Self::expr_to_pattern(inner)?;
                                    if let Expr::Duration(ns) = dur_expr {
                                        let duration = std::time::Duration::from_nanos(*ns);
                                        return Some(PatternBuilder::within(
                                            inner_pattern,
                                            duration,
                                        ));
                                    }
                                }
                            }
                        }
                        _ => {
                            // Treat as event type with filter
                            // For now, just treat as simple event
                            return Some(PatternBuilder::event(func_name));
                        }
                    }
                }
                None
            }

            // Lambda expressions in patterns are complex - skip for now
            _ => None,
        }
    }

    /// Compile a sequence filter expression into a runtime closure
    fn compile_sequence_filter(expr: varpulis_core::ast::Expr) -> SequenceFilter {
        Box::new(move |event: &Event, ctx: &SequenceContext| {
            Self::eval_filter_expr(&expr, event, ctx)
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        })
    }

    /// Evaluate a filter expression at runtime
    fn eval_filter_expr(
        expr: &varpulis_core::ast::Expr,
        event: &Event,
        ctx: &SequenceContext,
    ) -> Option<Value> {
        use varpulis_core::ast::{BinOp, Expr};

        match expr {
            Expr::Ident(name) => {
                // First check current event, then context
                event.get(name).cloned()
            }
            Expr::Int(n) => Some(Value::Int(*n)),
            Expr::Float(f) => Some(Value::Float(*f)),
            Expr::Str(s) => Some(Value::Str(s.clone())),
            Expr::Bool(b) => Some(Value::Bool(*b)),
            Expr::Member {
                expr: object,
                member,
            } => {
                // Handle alias.field access (e.g., order.id)
                if let Expr::Ident(alias) = object.as_ref() {
                    if let Some(captured) = ctx.get(alias.as_str()) {
                        return captured.get(member).cloned();
                    }
                }
                None
            }
            Expr::Binary { op, left, right } => {
                let left_val = Self::eval_filter_expr(left, event, ctx)?;
                let right_val = Self::eval_filter_expr(right, event, ctx)?;

                match op {
                    BinOp::Eq => Some(Value::Bool(left_val == right_val)),
                    BinOp::NotEq => Some(Value::Bool(left_val != right_val)),
                    BinOp::Lt => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Bool(a < b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Bool(a < b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Bool((*a as f64) < *b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Bool(*a < (*b as f64))),
                        _ => None,
                    },
                    BinOp::Le => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Bool(a <= b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Bool(a <= b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Bool((*a as f64) <= *b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Bool(*a <= (*b as f64))),
                        _ => None,
                    },
                    BinOp::Gt => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Bool(a > b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Bool(a > b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Bool((*a as f64) > *b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Bool(*a > (*b as f64))),
                        _ => None,
                    },
                    BinOp::Ge => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Bool(a >= b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Bool(a >= b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Bool((*a as f64) >= *b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Bool(*a >= (*b as f64))),
                        _ => None,
                    },
                    BinOp::And => {
                        let a = left_val.as_bool()?;
                        let b = right_val.as_bool()?;
                        Some(Value::Bool(a && b))
                    }
                    BinOp::Or => {
                        let a = left_val.as_bool()?;
                        let b = right_val.as_bool()?;
                        Some(Value::Bool(a || b))
                    }
                    BinOp::Add => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Int(a + b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a + b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Float(*a as f64 + b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a + *b as f64)),
                        _ => None,
                    },
                    BinOp::Sub => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Int(a - b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a - b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Float(*a as f64 - b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a - *b as f64)),
                        _ => None,
                    },
                    BinOp::Mul => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Int(a * b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a * b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Float(*a as f64 * b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a * *b as f64)),
                        _ => None,
                    },
                    BinOp::Div => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) if *b != 0 => Some(Value::Int(a / b)),
                        (Value::Float(a), Value::Float(b)) if *b != 0.0 => {
                            Some(Value::Float(a / b))
                        }
                        (Value::Int(a), Value::Float(b)) if *b != 0.0 => {
                            Some(Value::Float(*a as f64 / b))
                        }
                        (Value::Float(a), Value::Int(b)) if *b != 0 => {
                            Some(Value::Float(a / *b as f64))
                        }
                        _ => None,
                    },
                    _ => None,
                }
            }
            Expr::Call { func, args } => {
                // Handle function calls - both built-in and user-defined
                if let Expr::Ident(func_name) = func.as_ref() {
                    // Evaluate arguments
                    let arg_values: Vec<Value> = args
                        .iter()
                        .filter_map(|arg| match arg {
                            varpulis_core::ast::Arg::Positional(e) => {
                                Self::eval_filter_expr(e, event, ctx)
                            }
                            varpulis_core::ast::Arg::Named(_, e) => {
                                Self::eval_filter_expr(e, event, ctx)
                            }
                        })
                        .collect();

                    // Built-in functions
                    match func_name.as_str() {
                        "abs" => arg_values.first().and_then(|v| match v {
                            Value::Int(n) => Some(Value::Int(n.abs())),
                            Value::Float(f) => Some(Value::Float(f.abs())),
                            _ => None,
                        }),
                        "sqrt" => arg_values.first().and_then(|v| match v {
                            Value::Int(n) => Some(Value::Float((*n as f64).sqrt())),
                            Value::Float(f) => Some(Value::Float(f.sqrt())),
                            _ => None,
                        }),
                        "floor" => arg_values.first().and_then(|v| match v {
                            Value::Float(f) => Some(Value::Int(f.floor() as i64)),
                            Value::Int(n) => Some(Value::Int(*n)),
                            _ => None,
                        }),
                        "ceil" => arg_values.first().and_then(|v| match v {
                            Value::Float(f) => Some(Value::Int(f.ceil() as i64)),
                            Value::Int(n) => Some(Value::Int(*n)),
                            _ => None,
                        }),
                        "round" => arg_values.first().and_then(|v| match v {
                            Value::Float(f) => Some(Value::Int(f.round() as i64)),
                            Value::Int(n) => Some(Value::Int(*n)),
                            _ => None,
                        }),
                        "len" => arg_values.first().and_then(|v| match v {
                            Value::Str(s) => Some(Value::Int(s.len() as i64)),
                            Value::Array(a) => Some(Value::Int(a.len() as i64)),
                            _ => None,
                        }),
                        "to_string" => arg_values.first().map(|v| Value::Str(format!("{}", v))),
                        "to_int" => arg_values.first().and_then(|v| match v {
                            Value::Int(n) => Some(Value::Int(*n)),
                            Value::Float(f) => Some(Value::Int(*f as i64)),
                            Value::Str(s) => s.parse::<i64>().ok().map(Value::Int),
                            _ => None,
                        }),
                        "to_float" => arg_values.first().and_then(|v| match v {
                            Value::Int(n) => Some(Value::Float(*n as f64)),
                            Value::Float(f) => Some(Value::Float(*f)),
                            Value::Str(s) => s.parse::<f64>().ok().map(Value::Float),
                            _ => None,
                        }),
                        "min" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                            (Value::Int(a), Value::Int(b)) => Some(Value::Int(*a.min(b))),
                            (Value::Float(a), Value::Float(b)) => Some(Value::Float(a.min(*b))),
                            _ => None,
                        },
                        "max" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                            (Value::Int(a), Value::Int(b)) => Some(Value::Int(*a.max(b))),
                            (Value::Float(a), Value::Float(b)) => Some(Value::Float(a.max(*b))),
                            _ => None,
                        },
                        _ => {
                            // User-defined function - requires FunctionContext
                            // For now, log and return None (will be extended)
                            debug!("User function call: {}({:?})", func_name, arg_values);
                            None
                        }
                    }
                } else {
                    None
                }
            }
            Expr::Unary { op, expr: inner } => {
                let val = Self::eval_filter_expr(inner, event, ctx)?;
                match op {
                    varpulis_core::ast::UnaryOp::Neg => match val {
                        Value::Int(n) => Some(Value::Int(-n)),
                        Value::Float(f) => Some(Value::Float(-f)),
                        _ => None,
                    },
                    varpulis_core::ast::UnaryOp::Not => match val {
                        Value::Bool(b) => Some(Value::Bool(!b)),
                        _ => None,
                    },
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Evaluate expression with access to user-defined functions
    fn eval_expr_with_functions(
        expr: &varpulis_core::ast::Expr,
        event: &Event,
        ctx: &SequenceContext,
        functions: &HashMap<String, UserFunction>,
        bindings: &HashMap<String, Value>,
    ) -> Option<Value> {
        use varpulis_core::ast::{BinOp, Expr};

        match expr {
            Expr::Ident(name) => {
                // Check bindings first, then event, then context
                bindings
                    .get(name)
                    .cloned()
                    .or_else(|| event.get(name).cloned())
            }
            Expr::Int(n) => Some(Value::Int(*n)),
            Expr::Float(f) => Some(Value::Float(*f)),
            Expr::Str(s) => Some(Value::Str(s.clone())),
            Expr::Bool(b) => Some(Value::Bool(*b)),
            Expr::Call { func, args } => {
                if let Expr::Ident(func_name) = func.as_ref() {
                    // Evaluate arguments
                    let arg_values: Vec<Value> = args
                        .iter()
                        .filter_map(|arg| match arg {
                            varpulis_core::ast::Arg::Positional(e) => {
                                Self::eval_expr_with_functions(e, event, ctx, functions, bindings)
                            }
                            varpulis_core::ast::Arg::Named(_, e) => {
                                Self::eval_expr_with_functions(e, event, ctx, functions, bindings)
                            }
                        })
                        .collect();

                    // Check user-defined functions first
                    if let Some(user_fn) = functions.get(func_name) {
                        // Bind parameters to argument values
                        let mut fn_bindings = HashMap::new();
                        for (i, (param_name, _)) in user_fn.params.iter().enumerate() {
                            if let Some(val) = arg_values.get(i) {
                                fn_bindings.insert(param_name.clone(), val.clone());
                            }
                        }

                        // Evaluate function body (last expression in body)
                        if let Some(last_stmt) = user_fn.body.last() {
                            if let Stmt::Expr(body_expr) = &last_stmt.node {
                                return Self::eval_expr_with_functions(
                                    body_expr,
                                    event,
                                    ctx,
                                    functions,
                                    &fn_bindings,
                                );
                            }
                        }
                        return None;
                    }

                    // Built-in functions (same as eval_filter_expr)
                    match func_name.as_str() {
                        "abs" => arg_values.first().and_then(|v| match v {
                            Value::Int(n) => Some(Value::Int(n.abs())),
                            Value::Float(f) => Some(Value::Float(f.abs())),
                            _ => None,
                        }),
                        "sqrt" => arg_values.first().and_then(|v| match v {
                            Value::Int(n) => Some(Value::Float((*n as f64).sqrt())),
                            Value::Float(f) => Some(Value::Float(f.sqrt())),
                            _ => None,
                        }),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            Expr::Binary { op, left, right } => {
                let left_val =
                    Self::eval_expr_with_functions(left, event, ctx, functions, bindings)?;
                let right_val =
                    Self::eval_expr_with_functions(right, event, ctx, functions, bindings)?;

                match op {
                    BinOp::Add => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Int(a + b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a + b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Float(*a as f64 + b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a + *b as f64)),
                        _ => None,
                    },
                    BinOp::Sub => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Int(a - b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a - b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Float(*a as f64 - b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a - *b as f64)),
                        _ => None,
                    },
                    BinOp::Mul => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Int(a * b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a * b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Float(*a as f64 * b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a * *b as f64)),
                        _ => None,
                    },
                    BinOp::Div => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) if *b != 0 => Some(Value::Int(a / b)),
                        (Value::Float(a), Value::Float(b)) if *b != 0.0 => {
                            Some(Value::Float(a / b))
                        }
                        (Value::Int(a), Value::Float(b)) if *b != 0.0 => {
                            Some(Value::Float(*a as f64 / b))
                        }
                        (Value::Float(a), Value::Int(b)) if *b != 0 => {
                            Some(Value::Float(a / *b as f64))
                        }
                        _ => None,
                    },
                    BinOp::Eq => Some(Value::Bool(left_val == right_val)),
                    BinOp::NotEq => Some(Value::Bool(left_val != right_val)),
                    BinOp::Lt => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Bool(a < b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Bool(a < b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Bool((*a as f64) < *b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Bool(*a < *b as f64)),
                        _ => None,
                    },
                    BinOp::Le => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Bool(a <= b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Bool(a <= b)),
                        _ => None,
                    },
                    BinOp::Gt => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Bool(a > b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Bool(a > b)),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Bool((*a as f64) > *b)),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Bool(*a > *b as f64)),
                        _ => None,
                    },
                    BinOp::Ge => match (&left_val, &right_val) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Bool(a >= b)),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Bool(a >= b)),
                        _ => None,
                    },
                    BinOp::And => {
                        let a = left_val.as_bool()?;
                        let b = right_val.as_bool()?;
                        Some(Value::Bool(a && b))
                    }
                    BinOp::Or => {
                        let a = left_val.as_bool()?;
                        let b = right_val.as_bool()?;
                        Some(Value::Bool(a || b))
                    }
                    _ => None,
                }
            }
            _ => Self::eval_filter_expr(expr, event, ctx),
        }
    }

    /// Compile an aggregate expression into an AggregateFunc
    fn compile_agg_expr(
        expr: &varpulis_core::ast::Expr,
    ) -> Option<(Box<dyn crate::aggregation::AggregateFunc>, Option<String>)> {
        use varpulis_core::ast::{Arg, BinOp, Expr};

        match expr {
            // Simple function call: func(field) or func(field, param)
            Expr::Call { func, args } => {
                let func_name = match func.as_ref() {
                    Expr::Ident(s) => s.clone(),
                    _ => return None,
                };

                // Handle count(distinct(field)) pattern
                if func_name == "count" {
                    if let Some(Arg::Positional(Expr::Call {
                        func: inner_func,
                        args: inner_args,
                    })) = args.first()
                    {
                        if let Expr::Ident(inner_name) = inner_func.as_ref() {
                            if inner_name == "distinct" {
                                let field = inner_args.first().and_then(|a| match a {
                                    Arg::Positional(Expr::Ident(s)) => Some(s.clone()),
                                    _ => None,
                                });
                                return Some((Box::new(CountDistinct), field));
                            }
                        }
                    }
                }

                let field = args.first().and_then(|a| match a {
                    Arg::Positional(Expr::Ident(s)) => Some(s.clone()),
                    _ => None,
                });

                // Extract period for EMA
                let period = args
                    .get(1)
                    .and_then(|a| match a {
                        Arg::Positional(Expr::Int(n)) => Some(*n as usize),
                        _ => None,
                    })
                    .unwrap_or(12);

                let agg_func: Box<dyn crate::aggregation::AggregateFunc> = match func_name.as_str()
                {
                    "count" => Box::new(Count),
                    "sum" => Box::new(Sum),
                    "avg" => Box::new(Avg),
                    "min" => Box::new(Min),
                    "max" => Box::new(Max),
                    "last" => Box::new(Last),
                    "first" => Box::new(First),
                    "stddev" => Box::new(StdDev),
                    "ema" => Box::new(Ema::new(period)),
                    _ => {
                        warn!("Unknown aggregation function: {}", func_name);
                        return None;
                    }
                };

                Some((agg_func, field))
            }

            // Binary expression: left op right (e.g., last(x) - ema(x, 9))
            Expr::Binary { op, left, right } => {
                let agg_op = match op {
                    BinOp::Add => AggBinOp::Add,
                    BinOp::Sub => AggBinOp::Sub,
                    BinOp::Mul => AggBinOp::Mul,
                    BinOp::Div => AggBinOp::Div,
                    _ => {
                        warn!("Unsupported binary operator in aggregate: {:?}", op);
                        return None;
                    }
                };

                let (left_func, left_field) = Self::compile_agg_expr(left)?;
                let (right_func, right_field) = Self::compile_agg_expr(right)?;

                let expr_agg =
                    ExprAggregate::new(left_func, left_field, agg_op, right_func, right_field);

                Some((Box::new(expr_agg), None))
            }

            _ => {
                warn!("Unsupported aggregate expression: {:?}", expr);
                None
            }
        }
    }

    /// Process an incoming event
    pub async fn process(&mut self, event: Event) -> Result<(), String> {
        self.events_processed += 1;

        // Find streams that source from this event type
        let stream_names = self
            .event_sources
            .get(&event.event_type)
            .cloned()
            .unwrap_or_default();
        

        for stream_name in stream_names {
            if let Some(stream) = self.streams.get_mut(&stream_name) {
                let alerts =
                    Self::process_stream_with_functions(stream, event.clone(), &self.functions)
                        .await?;
                for alert in alerts {
                    self.alerts_generated += 1;
                    if let Err(e) = self.alert_tx.send(alert).await {
                        warn!("Failed to send alert: {}", e);
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
    ) -> Result<Vec<Alert>, String> {
        // For merge sources, check if the event passes the appropriate filter
        if let RuntimeSource::Merge(ref sources) = stream.source {
            let mut passes_filter = false;
            let mut matched_source_name = None;
            for ms in sources {
                if ms.event_type == event.event_type {
                    if let Some(ref filter) = ms.filter {
                        let ctx = SequenceContext::new();
                        if let Some(result) = Self::eval_expr_with_functions(
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
                return Ok(vec![]);
            }
            // Log which merge source matched (uses ms.name)
            if let Some(source_name) = matched_source_name {
                tracing::trace!("Event matched merge source: {}", source_name);
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
                        return Ok(alerts);
                    }
                }
                RuntimeOp::WhereExpr(expr) => {
                    let ctx = SequenceContext::new();
                    current_events.retain(|e| {
                        Self::eval_expr_with_functions(expr, e, &ctx, functions, &HashMap::new())
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    });
                    if current_events.is_empty() {
                        return Ok(alerts);
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
                        }
                    }
                    current_events = window_results;
                    if current_events.is_empty() {
                        return Ok(alerts);
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
                                Self::eval_filter_expr(expr, event, &SequenceContext::new())
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
                    // Process events through sequence tracker
                    if let Some(ref mut tracker) = stream.sequence_tracker {
                        let mut sequence_results = Vec::new();
                        for event in &current_events {
                            let completed = tracker.process(event);
                            for ctx in completed {
                                // Create synthetic event from completed sequence
                                let mut seq_event = Event::new("SequenceMatch");
                                seq_event
                                    .data
                                    .insert("stream".to_string(), Value::Str(stream.name.clone()));
                                // Add captured events to the result
                                for (alias, captured) in &ctx.captured {
                                    for (k, v) in &captured.data {
                                        seq_event
                                            .data
                                            .insert(format!("{}_{}", alias, k), v.clone());
                                    }
                                }
                                sequence_results.push(seq_event);
                            }
                        }
                        if sequence_results.is_empty() {
                            return Ok(alerts);
                        }
                        current_events = sequence_results;
                    }
                }
                RuntimeOp::EmitExpr(config) => {
                    let ctx = SequenceContext::new();
                    for event in &current_events {
                        let mut alert_data = IndexMap::new();
                        for (out_name, expr) in &config.fields {
                            if let Some(value) = Self::eval_expr_with_functions(
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
                    if let Some(result) = Self::eval_pattern_expr(
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
                            return Ok(alerts);
                        }
                    }
                }
            }
        }

        Ok(alerts)
    }

    /// Evaluate a pattern expression with support for attention_score builtin
    #[allow(clippy::only_used_in_recursion)]
    fn eval_pattern_expr(
        expr: &varpulis_core::ast::Expr,
        events: &[Event],
        ctx: &SequenceContext,
        functions: &HashMap<String, UserFunction>,
        pattern_vars: &HashMap<String, Value>,
        attention_window: Option<&AttentionWindow>,
    ) -> Option<Value> {
        use varpulis_core::ast::{Arg, Expr};

        match expr {
            // Lambda: params => body - evaluate body with params bound
            Expr::Lambda { params: _, body } => {
                // For pattern lambdas, the first param is typically "events"
                Self::eval_pattern_expr(
                    body,
                    events,
                    ctx,
                    functions,
                    pattern_vars,
                    attention_window,
                )
            }

            // Block with let bindings
            Expr::Block { stmts, result } => {
                let mut local_vars = pattern_vars.clone();
                for (name, _ty, value_expr, _mutable) in stmts {
                    if let Some(val) = Self::eval_pattern_expr(
                        value_expr,
                        events,
                        ctx,
                        functions,
                        &local_vars,
                        attention_window,
                    ) {
                        local_vars.insert(name.clone(), val);
                    }
                }
                Self::eval_pattern_expr(
                    result,
                    events,
                    ctx,
                    functions,
                    &local_vars,
                    attention_window,
                )
            }

            // Function call - check for builtins like attention_score
            Expr::Call { func, args } => {
                if let Expr::Ident(func_name) = func.as_ref() {
                    if func_name == "attention_score" && args.len() == 2 {
                        // attention_score(e1, e2) - compute attention between two events
                        if let Some(aw) = attention_window {
                            let e1_val = args.first().and_then(|a| match a {
                                Arg::Positional(e) => Self::eval_pattern_expr(
                                    e,
                                    events,
                                    ctx,
                                    functions,
                                    pattern_vars,
                                    Some(aw),
                                ),
                                _ => None,
                            });
                            let e2_val = args.get(1).and_then(|a| match a {
                                Arg::Positional(e) => Self::eval_pattern_expr(
                                    e,
                                    events,
                                    ctx,
                                    functions,
                                    pattern_vars,
                                    Some(aw),
                                ),
                                _ => None,
                            });

                            if let (Some(Value::Map(m1)), Some(Value::Map(m2))) = (e1_val, e2_val) {
                                // Convert maps back to events for attention scoring
                                let ev1 = Self::map_to_event(&m1);
                                let ev2 = Self::map_to_event(&m2);
                                let score = aw.attention_score(&ev1, &ev2);
                                return Some(Value::Float(score as f64));
                            }
                        }
                        return Some(Value::Float(0.0));
                    }

                    // len() on arrays
                    if func_name == "len" {
                        if let Some(Arg::Positional(arr_expr)) = args.first() {
                            if let Some(Value::Array(arr)) = Self::eval_pattern_expr(
                                arr_expr,
                                events,
                                ctx,
                                functions,
                                pattern_vars,
                                attention_window,
                            ) {
                                return Some(Value::Int(arr.len() as i64));
                            }
                        }
                    }
                }

                // Method calls on arrays: .filter(), .map(), .flatten()
                if let Expr::Member {
                    expr: receiver,
                    member,
                } = func.as_ref()
                {
                    let receiver_val = Self::eval_pattern_expr(
                        receiver,
                        events,
                        ctx,
                        functions,
                        pattern_vars,
                        attention_window,
                    )?;

                    match member.as_str() {
                        "filter" => {
                            if let Value::Array(arr) = receiver_val {
                                if let Some(Arg::Positional(Expr::Lambda { params, body })) =
                                    args.first()
                                {
                                    let param_name =
                                        params.first().cloned().unwrap_or_else(|| "x".to_string());
                                    let filtered: Vec<Value> = arr
                                        .into_iter()
                                        .filter(|item| {
                                            let mut local = pattern_vars.clone();
                                            local.insert(param_name.clone(), item.clone());
                                            Self::eval_pattern_expr(
                                                body,
                                                events,
                                                ctx,
                                                functions,
                                                &local,
                                                attention_window,
                                            )
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                        })
                                        .collect();
                                    return Some(Value::Array(filtered));
                                }
                            }
                        }
                        "map" => {
                            if let Value::Array(arr) = receiver_val {
                                if let Some(Arg::Positional(Expr::Lambda { params, body })) =
                                    args.first()
                                {
                                    let param_name =
                                        params.first().cloned().unwrap_or_else(|| "x".to_string());
                                    let mapped: Vec<Value> = arr
                                        .into_iter()
                                        .filter_map(|item| {
                                            let mut local = pattern_vars.clone();
                                            local.insert(param_name.clone(), item);
                                            Self::eval_pattern_expr(
                                                body,
                                                events,
                                                ctx,
                                                functions,
                                                &local,
                                                attention_window,
                                            )
                                        })
                                        .collect();
                                    return Some(Value::Array(mapped));
                                }
                            }
                        }
                        "flatten" => {
                            if let Value::Array(arr) = receiver_val {
                                let flattened: Vec<Value> = arr
                                    .into_iter()
                                    .flat_map(|item| {
                                        if let Value::Array(inner) = item {
                                            inner
                                        } else {
                                            vec![item]
                                        }
                                    })
                                    .collect();
                                return Some(Value::Array(flattened));
                            }
                        }
                        "len" => {
                            if let Value::Array(arr) = receiver_val {
                                return Some(Value::Int(arr.len() as i64));
                            }
                        }
                        _ => {}
                    }
                }

                None
            }

            // Member access
            Expr::Member {
                expr: receiver,
                member,
            } => {
                let recv_val = Self::eval_pattern_expr(
                    receiver,
                    events,
                    ctx,
                    functions,
                    pattern_vars,
                    attention_window,
                )?;
                match recv_val {
                    Value::Map(m) => m.get(member).cloned(),
                    _ => None,
                }
            }

            // Identifier lookup
            Expr::Ident(name) => pattern_vars.get(name).cloned(),

            // Literals
            Expr::Int(n) => Some(Value::Int(*n)),
            Expr::Float(f) => Some(Value::Float(*f)),
            Expr::Bool(b) => Some(Value::Bool(*b)),
            Expr::Str(s) => Some(Value::Str(s.clone())),

            // Binary comparison
            Expr::Binary { op, left, right } => {
                let l = Self::eval_pattern_expr(
                    left,
                    events,
                    ctx,
                    functions,
                    pattern_vars,
                    attention_window,
                )?;
                let r = Self::eval_pattern_expr(
                    right,
                    events,
                    ctx,
                    functions,
                    pattern_vars,
                    attention_window,
                )?;
                Self::eval_binary_op(op, &l, &r)
            }

            _ => None,
        }
    }

    /// Convert a Value::Map back to an Event for attention scoring
    fn map_to_event(map: &IndexMap<String, Value>) -> Event {
        let event_type = map
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let mut data = IndexMap::new();
        for (k, v) in map {
            if k != "event_type" {
                data.insert(k.clone(), v.clone());
            }
        }

        Event {
            event_type,
            timestamp: chrono::Utc::now(),
            data,
        }
    }

    /// Evaluate a binary operation
    fn eval_binary_op(
        op: &varpulis_core::ast::BinOp,
        left: &Value,
        right: &Value,
    ) -> Option<Value> {
        use varpulis_core::ast::BinOp;

        match op {
            BinOp::Gt => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Some(Value::Bool(l > r)),
                (Value::Float(l), Value::Float(r)) => Some(Value::Bool(l > r)),
                (Value::Int(l), Value::Float(r)) => Some(Value::Bool((*l as f64) > *r)),
                (Value::Float(l), Value::Int(r)) => Some(Value::Bool(*l > (*r as f64))),
                _ => None,
            },
            BinOp::Lt => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Some(Value::Bool(l < r)),
                (Value::Float(l), Value::Float(r)) => Some(Value::Bool(l < r)),
                (Value::Int(l), Value::Float(r)) => Some(Value::Bool((*l as f64) < *r)),
                (Value::Float(l), Value::Int(r)) => Some(Value::Bool(*l < (*r as f64))),
                _ => None,
            },
            BinOp::Ge => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Some(Value::Bool(l >= r)),
                (Value::Float(l), Value::Float(r)) => Some(Value::Bool(l >= r)),
                _ => None,
            },
            BinOp::Le => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Some(Value::Bool(l <= r)),
                (Value::Float(l), Value::Float(r)) => Some(Value::Bool(l <= r)),
                _ => None,
            },
            BinOp::Eq => Some(Value::Bool(left == right)),
            BinOp::NotEq => Some(Value::Bool(left != right)),
            BinOp::And => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Some(Value::Bool(l && r))
            }
            BinOp::Or => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Some(Value::Bool(l || r))
            }
            _ => None,
        }
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

#[derive(Debug, Clone)]
pub struct EngineMetrics {
    pub events_processed: u64,
    pub alerts_generated: u64,
    pub streams_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn parse_program(source: &str) -> Program {
        varpulis_parser::parse(source).expect("Failed to parse")
    }

    #[tokio::test]
    async fn test_engine_simple_sequence() {
        let source = r#"
            stream OrderPayment = Order
                -> Payment as payment
                .emit(status: "matched")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Send Order event
        let order = Event::new("Order").with_field("id", 1i64);
        engine.process(order).await.unwrap();

        // No alert yet - waiting for Payment
        assert!(rx.try_recv().is_err());

        // Send Payment event
        let payment = Event::new("Payment").with_field("order_id", 1i64);
        engine.process(payment).await.unwrap();

        // Should get alert now
        let alert = rx.try_recv().expect("Should have alert");
        assert_eq!(alert.alert_type, "stream_output");
    }

    #[tokio::test]
    async fn test_engine_sequence_with_alias() {
        let source = r#"
            stream TwoTicks = StockTick as first
                -> StockTick as second
                .emit(result: "two_ticks")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // First tick
        let tick1 = Event::new("StockTick").with_field("price", 100.0);
        engine.process(tick1).await.unwrap();
        assert!(rx.try_recv().is_err());

        // Second tick - completes sequence
        let tick2 = Event::new("StockTick").with_field("price", 101.0);
        engine.process(tick2).await.unwrap();

        let alert = rx.try_recv().expect("Should have alert");
        assert_eq!(
            alert.data.get("result"),
            Some(&Value::Str("two_ticks".to_string()))
        );
    }

    #[tokio::test]
    async fn test_engine_sequence_three_steps() {
        let source = r#"
            stream ABC = A as a -> B as b -> C as c
                .emit(status: "complete")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine.process(Event::new("A")).await.unwrap();
        assert!(rx.try_recv().is_err());

        engine.process(Event::new("B")).await.unwrap();
        assert!(rx.try_recv().is_err());

        engine.process(Event::new("C")).await.unwrap();
        let alert = rx.try_recv().expect("Should have alert");
        assert_eq!(
            alert.data.get("status"),
            Some(&Value::Str("complete".to_string()))
        );
    }

    #[tokio::test]
    async fn test_engine_sequence_wrong_order() {
        let source = r#"
            stream AB = A -> B .emit(done: "yes")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Send B before A - should not match
        engine.process(Event::new("B")).await.unwrap();
        engine.process(Event::new("A")).await.unwrap();
        assert!(rx.try_recv().is_err());

        // Now send B after A - should match
        engine.process(Event::new("B")).await.unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_sequence_with_filter() {
        let source = r#"
            stream OrderPaymentMatch = Order as order
                -> Payment where order_id == order.id as payment
                .emit(status: "matched")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Order 1
        engine
            .process(Event::new("Order").with_field("id", 1i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_err());

        // Payment for wrong order - should NOT match
        engine
            .process(Event::new("Payment").with_field("order_id", 999i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_err());

        // Payment for correct order - should match
        engine
            .process(Event::new("Payment").with_field("order_id", 1i64))
            .await
            .unwrap();
        let alert = rx.try_recv().expect("Should have alert");
        assert_eq!(
            alert.data.get("status"),
            Some(&Value::Str("matched".to_string()))
        );
    }

    #[tokio::test]
    async fn test_engine_sequence_with_timeout() {
        // Test that .within() timeout is parsed and stored
        let source = r#"
            stream QuickResponse = Request as req
                -> Response as resp
                .within(5s)
                .emit(status: "fast")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Request starts correlation
        engine
            .process(Event::new("Request").with_field("id", 1i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_err());

        // Response within timeout should match
        engine
            .process(Event::new("Response").with_field("id", 1i64))
            .await
            .unwrap();
        let alert = rx.try_recv().expect("Should have alert");
        assert_eq!(
            alert.data.get("status"),
            Some(&Value::Str("fast".to_string()))
        );
    }

    #[tokio::test]
    async fn test_engine_with_event_file() {
        use crate::event_file::EventFileParser;

        // Parse event file content
        let source = r#"
            # Order-Payment sequence test
            Order { id: 1, symbol: "AAPL" }
            
            BATCH 10
            Payment { order_id: 1, amount: 15000.0 }
        "#;

        let events = EventFileParser::parse(source).expect("Failed to parse");
        assert_eq!(events.len(), 2);

        // Setup engine with sequence pattern
        let program_source = r#"
            stream OrderPayment = Order as order
                -> Payment where order_id == order.id as payment
                .emit(status: "matched", order_id: order.id)
        "#;

        let program = parse_program(program_source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Send events from parsed file
        for timed_event in events {
            engine.process(timed_event.event).await.unwrap();
        }

        // Should have received alert
        let alert = rx.try_recv().expect("Should have alert");
        assert_eq!(
            alert.data.get("status"),
            Some(&Value::Str("matched".to_string()))
        );
    }

    #[tokio::test]
    async fn test_engine_sequence_with_not() {
        // Test .not() - detect absence of event
        // This tests the parsing; full negation semantics require timeout handling
        let source = r#"
            stream MissingPayment = Order as order
                -> Payment where order_id == order.id as payment
                .not(Cancellation where order_id == order.id)
                .emit(status: "payment_received")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Order and Payment without Cancellation
        engine
            .process(Event::new("Order").with_field("id", 1i64))
            .await
            .unwrap();
        engine
            .process(Event::new("Payment").with_field("order_id", 1i64))
            .await
            .unwrap();

        // Should get alert since no cancellation occurred
        let alert = rx.try_recv().expect("Should have alert");
        assert_eq!(
            alert.data.get("status"),
            Some(&Value::Str("payment_received".to_string()))
        );
    }

    #[tokio::test]
    async fn test_engine_all_in_source() {
        // Test 'all' in source position - starts multiple correlations
        let source = r#"
            stream AllNews = all News as news
                -> Tick as tick
                .emit(matched: "yes")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Multiple News events should each start a correlation
        engine
            .process(Event::new("News").with_field("id", 1i64))
            .await
            .unwrap();
        engine
            .process(Event::new("News").with_field("id", 2i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_err()); // No alerts yet

        // One Tick should complete BOTH correlations
        engine
            .process(Event::new("Tick").with_field("price", 100.0))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok()); // First match
        assert!(rx.try_recv().is_ok()); // Second match
    }

    #[tokio::test]
    async fn test_engine_match_all_sequence() {
        // Test match_all in FollowedBy position
        let source = r#"
            stream AllTicks = News as news
                -> all Tick as tick
                .emit(matched: "yes")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // One news event starts correlation
        engine
            .process(Event::new("News").with_field("id", 1i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_err());

        // Multiple ticks should each complete (match_all on second step)
        engine
            .process(Event::new("Tick").with_field("price", 100.0))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());

        // Correlation should still be active for more ticks
        // (match_all means it stays active)
        engine
            .process(Event::new("Tick").with_field("price", 101.0))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    // ==========================================================================
    // Builder and Configuration Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_engine_with_metrics() {
        let (tx, _rx) = mpsc::channel(100);
        let metrics = crate::metrics::Metrics::new();
        let engine = Engine::new(tx).with_metrics(metrics);
        assert!(engine.metrics.is_some());
    }

    #[tokio::test]
    async fn test_engine_metrics() {
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);

        let source = r#"
            stream Simple = A -> B .emit(done: "yes")
        "#;
        let program = parse_program(source);
        engine.load(&program).unwrap();

        // Process some events
        engine.process(Event::new("A")).await.unwrap();
        engine.process(Event::new("B")).await.unwrap();
        let _ = rx.try_recv();

        let metrics = engine.metrics();
        assert_eq!(metrics.events_processed, 2);
        assert!(metrics.alerts_generated >= 1);
        assert_eq!(metrics.streams_count, 1);
    }

    #[tokio::test]
    async fn test_engine_single_event_emit() {
        // Test single event stream with emit
        let source = r#"
            stream S = Order as o -> Confirm .emit(status: "confirmed")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("Order").with_field("id", 1i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_err()); // Need Confirm
        engine.process(Event::new("Confirm")).await.unwrap();
        assert!(rx.try_recv().is_ok());
    }

    // ==========================================================================
    // Filter Expression Tests - Arithmetic Operations
    // ==========================================================================

    #[tokio::test]
    async fn test_engine_filter_arithmetic_add() {
        let source = r#"
            stream Test = A as a
                -> B where value == a.base + 10 as b
                .emit(status: "matched")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("base", 5i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 15i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_arithmetic_sub() {
        let source = r#"
            stream Test = A as a
                -> B where value == a.base - 3 as b
                .emit(status: "matched")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("base", 10i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 7i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_arithmetic_mul() {
        let source = r#"
            stream Test = A as a
                -> B where value == a.base * 2 as b
                .emit(status: "matched")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("base", 5i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 10i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_arithmetic_div() {
        let source = r#"
            stream Test = A as a
                -> B where value == a.base / 2 as b
                .emit(status: "matched")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("base", 10i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 5i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    // ==========================================================================
    // Filter Expression Tests - Comparison Operations
    // ==========================================================================

    #[tokio::test]
    async fn test_engine_filter_comparison_lt() {
        let source = r#"
            stream Test = A as a
                -> B where value < a.threshold as b
                .emit(status: "below")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("threshold", 100i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 50i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_comparison_le() {
        let source = r#"
            stream Test = A as a
                -> B where value <= a.threshold as b
                .emit(status: "at_or_below")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("threshold", 100i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 100i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_comparison_gt() {
        let source = r#"
            stream Test = A as a
                -> B where value > a.threshold as b
                .emit(status: "above")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("threshold", 50i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 100i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_comparison_ge() {
        let source = r#"
            stream Test = A as a
                -> B where value >= a.threshold as b
                .emit(status: "at_or_above")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("threshold", 100i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 100i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_comparison_neq() {
        let source = r#"
            stream Test = A as a
                -> B where value != a.exclude as b
                .emit(status: "different")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("exclude", 42i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 100i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    // ==========================================================================
    // Filter Expression Tests - Logical Operations
    // ==========================================================================

    #[tokio::test]
    async fn test_engine_filter_logical_and() {
        let source = r#"
            stream Test = A as a
                -> B where value > 10 and value < 100 as b
                .emit(status: "in_range")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine.process(Event::new("A")).await.unwrap();
        engine
            .process(Event::new("B").with_field("value", 50i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_logical_or() {
        let source = r#"
            stream Test = A as a
                -> B where status == "active" or priority > 5 as b
                .emit(result: "matched")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine.process(Event::new("A")).await.unwrap();
        // priority > 5 is true (priority=10)
        engine
            .process(
                Event::new("B")
                    .with_field("status", "inactive")
                    .with_field("priority", 10i64),
            )
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    // ==========================================================================
    // Filter Expression Tests - Float/Mixed Type Operations
    // ==========================================================================

    #[tokio::test]
    async fn test_engine_filter_float_comparison() {
        let source = r#"
            stream Test = A as a
                -> B where price > a.min_price as b
                .emit(status: "above_min")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine
            .process(Event::new("A").with_field("min_price", 99.99f64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("price", 100.0f64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_int_float_mixed() {
        let source = r#"
            stream Test = A as a
                -> B where value > a.threshold as b
                .emit(status: "above")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // int threshold, float value
        engine
            .process(Event::new("A").with_field("threshold", 50i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 75.5f64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    // ==========================================================================
    // Literal Value Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_engine_filter_literal_int() {
        let source = r#"
            stream Test = A as a
                -> B where value == 42 as b
                .emit(status: "found")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine.process(Event::new("A")).await.unwrap();
        engine
            .process(Event::new("B").with_field("value", 42i64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_literal_string() {
        let source = r#"
            stream Test = A as a
                -> B where status == "active" as b
                .emit(result: "ok")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine.process(Event::new("A")).await.unwrap();
        engine
            .process(Event::new("B").with_field("status", "active"))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_literal_float() {
        let source = r#"
            stream Test = A as a
                -> B where price == 99.99 as b
                .emit(status: "exact")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine.process(Event::new("A")).await.unwrap();
        engine
            .process(Event::new("B").with_field("price", 99.99f64))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_engine_filter_literal_bool() {
        let source = r#"
            stream Test = A as a
                -> B where active == true as b
                .emit(status: "active")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        engine.process(Event::new("A")).await.unwrap();
        engine
            .process(Event::new("B").with_field("active", true))
            .await
            .unwrap();
        assert!(rx.try_recv().is_ok());
    }

    // ==========================================================================
    // Edge Cases and Error Handling
    // ==========================================================================

    #[tokio::test]
    async fn test_engine_unmatched_event_type() {
        let source = r#"
            stream AB = A -> B .emit(done: "yes")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Send unrelated events
        engine.process(Event::new("X")).await.unwrap();
        engine.process(Event::new("Y")).await.unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_engine_rapid_events() {
        // Test processing many events rapidly
        let source = r#"
            stream Rapid = A -> B .emit(done: "yes")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(1000);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // Process 100 A-B pairs
        for i in 0..100 {
            engine
                .process(Event::new("A").with_field("id", i as i64))
                .await
                .unwrap();
            engine
                .process(Event::new("B").with_field("id", i as i64))
                .await
                .unwrap();
        }

        // Should have 100 alerts
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 100);
    }

    #[tokio::test]
    async fn test_engine_div_by_zero() {
        // Division by zero should not crash, just not match
        let source = r#"
            stream Test = A as a
                -> B where value == a.x / a.y as b
                .emit(status: "computed")
        "#;

        let program = parse_program(source);
        let (tx, mut rx) = mpsc::channel(100);
        let mut engine = Engine::new(tx);
        engine.load(&program).unwrap();

        // y=0 causes division by zero
        engine
            .process(Event::new("A").with_field("x", 10i64).with_field("y", 0i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("value", 0i64))
            .await
            .unwrap();
        // Should not match because division by zero returns None
        assert!(rx.try_recv().is_err());
    }
}
