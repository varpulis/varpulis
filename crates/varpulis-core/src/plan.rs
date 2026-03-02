//! Logical plan types for the Varpulis query planner
//!
//! These types represent a serializable, inspectable intermediate representation
//! between the AST and the runtime execution engine. They contain no closures or
//! runtime state, enabling:
//!
//! - Plan inspection via `EXPLAIN`
//! - Rule-based optimization before materialization
//! - Serialization for distributed execution
//!
//! # Pipeline
//!
//! ```text
//! AST → LogicalPlanner → LogicalPlan → Optimizer → LogicalPlan (optimized)
//!     → PhysicalPlanner → PhysicalPlan → Engine materialization
//! ```

use serde::{Deserialize, Serialize};

use crate::ast::{
    AggItem, ConnectorParam, EnrichSpec, Expr, ForecastSpec, JoinClause, NamedArg, PatternDef,
    ScoreSpec, SelectItem, TrendAggItem, WindowArgs,
};

/// Top-level logical plan for a VPL program.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalPlan {
    /// Stream definitions in declaration order
    pub streams: Vec<LogicalStream>,
    /// User-defined functions (name, param count)
    pub functions: Vec<LogicalFunction>,
    /// Variable declarations (name, is_mutable)
    pub variables: Vec<LogicalVariable>,
    /// Connector declarations
    pub connectors: Vec<LogicalConnector>,
    /// Named SASE+ patterns
    pub patterns: Vec<LogicalPattern>,
    /// Event type declarations (name, field count)
    pub events: Vec<LogicalEvent>,
}

/// A stream in the logical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalStream {
    /// Unique identifier within the plan
    pub id: u32,
    /// Stream name from the VPL declaration
    pub name: String,
    /// Source of events for this stream
    pub source: LogicalSource,
    /// Ordered operations applied to the stream
    pub operations: Vec<LogicalOp>,
    /// Estimated output cardinality (None if unknown)
    pub estimated_cardinality: Option<u64>,
}

/// Source of events for a logical stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalSource {
    /// Events of a specific type
    EventType(String),
    /// Output of another stream
    Stream(String),
    /// Join of multiple sources
    Join(Vec<JoinClause>),
    /// Merge of multiple event types with optional filters
    Merge(Vec<LogicalMergeSource>),
    /// Periodic timer
    Timer {
        /// Timer interval expression.
        interval: Expr,
        /// Optional initial delay before first fire.
        initial_delay: Option<Expr>,
    },
    /// Sequence construct for temporal event correlation
    Sequence(crate::ast::SequenceDecl),
    /// Named pattern reference
    Pattern(String),
    /// From connector source
    FromConnector {
        /// Event type to receive.
        event_type: String,
        /// Connector name to read from.
        connector_name: String,
        /// Connector parameters.
        params: Vec<ConnectorParam>,
    },
}

/// A source in a merge construct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalMergeSource {
    /// Name alias for this merge source.
    pub name: String,
    /// Source event type or stream name.
    pub source: String,
    /// Optional filter condition.
    pub filter: Option<Expr>,
}

/// Operations in the logical plan — mirrors `StreamOp` but normalized.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalOp {
    /// Filter: `.where(cond)` or `.filter(fn)`
    Filter(Expr),
    /// Projection: `.select(fields)`
    Project(Vec<SelectItem>),
    /// Window: `.window(duration, ...)`
    Window(WindowArgs),
    /// Aggregation: `.aggregate(...)`
    Aggregate(Vec<AggItem>),
    /// Having: `.having(cond)`
    Having(Expr),
    /// Partitioning: `.partition_by(key)`
    PartitionBy(Expr),
    /// Emit output: `.emit(...)` with optional type cast
    Emit {
        /// Optional output event type cast.
        output_type: Option<String>,
        /// Output field mappings.
        fields: Vec<NamedArg>,
        /// Optional target context for cross-context emit.
        target_context: Option<String>,
    },
    /// Send to connector: `.to(Connector, ...)`
    Sink {
        /// Target connector name.
        connector_name: String,
        /// Sink parameters.
        params: Vec<ConnectorParam>,
    },
    /// Sequence step: `-> EventType where cond as alias`
    FollowedBy(crate::ast::FollowedByClause),
    /// Negation: `.not(EventType where cond)`
    Not(crate::ast::FollowedByClause),
    /// Time constraint: `.within(duration)`
    Within(Expr),
    /// Trend aggregation: `.trend_aggregate(...)`
    TrendAggregate(Vec<TrendAggItem>),
    /// PST forecasting: `.forecast(...)`
    Forecast(ForecastSpec),
    /// External enrichment: `.enrich(...)`
    Enrich(EnrichSpec),
    /// Deduplication: `.distinct(expr?)`
    Distinct(Option<Expr>),
    /// Limit: `.limit(n)`
    Limit(Expr),
    /// Print: `.print(...)`
    Print(Vec<Expr>),
    /// Log: `.log(...)`
    Log(Vec<NamedArg>),
    /// Process: `.process(fn)`
    Process(Expr),
    /// Concurrent: `.concurrent(...)`
    Concurrent(Vec<NamedArg>),
    /// Pattern matching: `.pattern(...)`
    Pattern(PatternDef),
    /// ONNX scoring: `.score(...)`
    Score(ScoreSpec),
    /// Map: `.map(fn)`
    Map(Expr),
    /// Order by: `.order_by(...)`
    OrderBy(Vec<crate::ast::OrderItem>),
    /// Context assignment: `.context(name)`
    Context(String),
    /// Watermark configuration: `.watermark(...)`
    Watermark(Vec<NamedArg>),
    /// Allowed lateness: `.allowed_lateness(duration)`
    AllowedLateness(Expr),
    /// Fork: `.fork(...)`
    Fork(Vec<crate::ast::ForkPath>),
}

/// Connector declaration in the logical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalConnector {
    /// Connector name.
    pub name: String,
    /// Connector protocol type (e.g., `"mqtt"`, `"kafka"`).
    pub connector_type: String,
    /// Connection parameters.
    pub params: Vec<ConnectorParam>,
}

/// Function declaration in the logical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalFunction {
    /// Function name.
    pub name: String,
    /// Number of parameters.
    pub param_count: usize,
    /// Whether a return type is declared.
    pub has_return_type: bool,
    /// Parameter types from the function declaration (empty if untyped).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub param_types: Vec<crate::Type>,
    /// Declared return type (None if untyped).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_type: Option<crate::Type>,
}

/// Variable declaration in the logical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalVariable {
    /// Variable name.
    pub name: String,
    /// Whether the variable is mutable (`var` vs `let`).
    pub is_mutable: bool,
}

/// Named SASE+ pattern in the logical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalPattern {
    /// Pattern name.
    pub name: String,
    /// SASE+ pattern expression.
    pub expr: crate::ast::SasePatternExpr,
    /// Optional time window constraint.
    pub within: Option<Expr>,
    /// Optional partition key expression.
    pub partition_by: Option<Expr>,
}

/// Event type declaration in the logical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalEvent {
    /// Event type name.
    pub name: String,
    /// Number of declared fields.
    pub field_count: usize,
    /// Parent event type if this event extends another.
    pub extends: Option<String>,
}

impl LogicalPlan {
    /// Produce a human-readable plan explanation.
    pub fn explain(&self) -> String {
        let mut out = String::new();

        if !self.connectors.is_empty() {
            out.push_str("Connectors:\n");
            for c in &self.connectors {
                out.push_str(&format!(
                    "  {} = {} ({} params)\n",
                    c.name,
                    c.connector_type,
                    c.params.len()
                ));
            }
            out.push('\n');
        }

        if !self.events.is_empty() {
            out.push_str("Events:\n");
            for e in &self.events {
                let ext = e
                    .extends
                    .as_deref()
                    .map(|e| format!(" extends {e}"))
                    .unwrap_or_default();
                out.push_str(&format!("  {} ({} fields{})\n", e.name, e.field_count, ext));
            }
            out.push('\n');
        }

        if !self.functions.is_empty() {
            out.push_str("Functions:\n");
            for f in &self.functions {
                out.push_str(&format!("  {}({} params)\n", f.name, f.param_count));
            }
            out.push('\n');
        }

        if !self.patterns.is_empty() {
            out.push_str("Patterns:\n");
            for p in &self.patterns {
                out.push_str(&format!("  {}\n", p.name));
            }
            out.push('\n');
        }

        out.push_str("Streams:\n");
        for stream in &self.streams {
            out.push_str(&format!("  [{}] {}\n", stream.id, stream.name));
            out.push_str(&format!(
                "    Source: {}\n",
                describe_source(&stream.source)
            ));
            for (i, op) in stream.operations.iter().enumerate() {
                out.push_str(&format!("    Op {}: {}\n", i, describe_op(op)));
            }
            if let Some(card) = stream.estimated_cardinality {
                out.push_str(&format!("    Est. cardinality: {card}\n"));
            }
        }

        out
    }
}

fn describe_source(source: &LogicalSource) -> String {
    match source {
        LogicalSource::EventType(t) => format!("EventType({t})"),
        LogicalSource::Stream(s) => format!("Stream({s})"),
        LogicalSource::Join(clauses) => {
            let names: Vec<_> = clauses.iter().map(|c| c.name.as_str()).collect();
            format!("Join({})", names.join(", "))
        }
        LogicalSource::Merge(sources) => {
            let names: Vec<_> = sources.iter().map(|s| s.name.as_str()).collect();
            format!("Merge({})", names.join(", "))
        }
        LogicalSource::Timer { .. } => "Timer".to_string(),
        LogicalSource::Sequence(decl) => {
            format!("Sequence({} steps)", decl.steps.len())
        }
        LogicalSource::Pattern(name) => format!("Pattern({name})"),
        LogicalSource::FromConnector {
            event_type,
            connector_name,
            ..
        } => {
            format!("FromConnector({event_type} via {connector_name})")
        }
    }
}

fn describe_op(op: &LogicalOp) -> String {
    match op {
        LogicalOp::Filter(_) => "Filter".to_string(),
        LogicalOp::Project(items) => format!("Project({} fields)", items.len()),
        LogicalOp::Window(args) => {
            if args.session_gap.is_some() {
                "Window(session)".to_string()
            } else if args.sliding.is_some() {
                "Window(sliding)".to_string()
            } else {
                "Window(tumbling)".to_string()
            }
        }
        LogicalOp::Aggregate(items) => format!("Aggregate({} fields)", items.len()),
        LogicalOp::Having(_) => "Having".to_string(),
        LogicalOp::PartitionBy(_) => "PartitionBy".to_string(),
        LogicalOp::Emit {
            output_type,
            fields,
            ..
        } => {
            let ty = output_type.as_deref().unwrap_or("default");
            format!("Emit({}, {} fields)", ty, fields.len())
        }
        LogicalOp::Sink { connector_name, .. } => format!("Sink({connector_name})"),
        LogicalOp::FollowedBy(clause) => format!("FollowedBy({})", clause.event_type),
        LogicalOp::Not(clause) => format!("Not({})", clause.event_type),
        LogicalOp::Within(_) => "Within".to_string(),
        LogicalOp::TrendAggregate(items) => format!("TrendAggregate({} fields)", items.len()),
        LogicalOp::Forecast(_) => "Forecast".to_string(),
        LogicalOp::Enrich(spec) => format!("Enrich({})", spec.connector_name),
        LogicalOp::Distinct(_) => "Distinct".to_string(),
        LogicalOp::Limit(_) => "Limit".to_string(),
        LogicalOp::Print(_) => "Print".to_string(),
        LogicalOp::Log(_) => "Log".to_string(),
        LogicalOp::Process(_) => "Process".to_string(),
        LogicalOp::Concurrent(_) => "Concurrent".to_string(),
        LogicalOp::Pattern(def) => format!("Pattern({})", def.name),
        LogicalOp::Score(_) => "Score".to_string(),
        LogicalOp::Map(_) => "Map".to_string(),
        LogicalOp::OrderBy(items) => format!("OrderBy({} keys)", items.len()),
        LogicalOp::Context(name) => format!("Context({name})"),
        LogicalOp::Watermark(_) => "Watermark".to_string(),
        LogicalOp::AllowedLateness(_) => "AllowedLateness".to_string(),
        LogicalOp::Fork(paths) => format!("Fork({} paths)", paths.len()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_plan_explain() {
        let plan = LogicalPlan {
            streams: vec![],
            functions: vec![],
            variables: vec![],
            connectors: vec![],
            patterns: vec![],
            events: vec![],
        };
        let text = plan.explain();
        assert!(text.contains("Streams:"));
    }

    #[test]
    fn test_plan_with_stream_explain() {
        let plan = LogicalPlan {
            streams: vec![LogicalStream {
                id: 0,
                name: "HighTemp".to_string(),
                source: LogicalSource::EventType("SensorReading".to_string()),
                operations: vec![
                    LogicalOp::Filter(Expr::Bool(true)),
                    LogicalOp::Emit {
                        output_type: Some("Alert".to_string()),
                        fields: vec![],
                        target_context: None,
                    },
                ],
                estimated_cardinality: Some(100),
            }],
            functions: vec![],
            variables: vec![],
            connectors: vec![],
            patterns: vec![],
            events: vec![],
        };
        let text = plan.explain();
        assert!(text.contains("[0] HighTemp"));
        assert!(text.contains("EventType(SensorReading)"));
        assert!(text.contains("Filter"));
        assert!(text.contains("Emit(Alert, 0 fields)"));
        assert!(text.contains("Est. cardinality: 100"));
    }

    #[test]
    fn test_plan_serialization_roundtrip() {
        let plan = LogicalPlan {
            streams: vec![LogicalStream {
                id: 0,
                name: "Test".to_string(),
                source: LogicalSource::EventType("E".to_string()),
                operations: vec![LogicalOp::Filter(Expr::Bool(true))],
                estimated_cardinality: None,
            }],
            functions: vec![],
            variables: vec![],
            connectors: vec![],
            patterns: vec![],
            events: vec![],
        };
        let json = serde_json::to_string(&plan).unwrap();
        let deserialized: LogicalPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.streams.len(), 1);
        assert_eq!(deserialized.streams[0].name, "Test");
    }
}
