//! Pipeline graph representation for the visual pipeline builder.
//!
//! Converts VPL programs to/from a graph of nodes and edges suitable for
//! rendering with Vue Flow (or any graph visualization library).

use serde::{Deserialize, Serialize};
use varpulis_core::ast::{ConnectorParam, Expr, Program, Stmt, StreamOp, StreamSource, WindowArgs};

// =============================================================================
// Graph Types
// =============================================================================

/// A pipeline represented as a directed graph of nodes and edges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

/// A single node in the pipeline graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    /// Unique identifier for this node.
    pub id: String,
    /// Human-readable label displayed in the UI.
    pub label: String,
    /// Node category: "source", "filter", "window", "aggregate", "emit",
    /// "sink", "pattern", "alert", "select", "partition", "having",
    /// "limit", "distinct", "map", "order", "print", "log", "tap",
    /// "score", "forecast", "enrich", "trend_aggregate", "sequence_step",
    /// "within", "not", "fork", "watermark", "process", "on_error".
    pub node_type: String,
    /// Operator-specific configuration (JSON object).
    #[serde(default)]
    pub config: serde_json::Value,
    /// Optional position hint for layout (x, y).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<(f64, f64)>,
}

/// A directed edge connecting two nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdge {
    /// Unique identifier for this edge.
    pub id: String,
    /// Source node id.
    pub source: String,
    /// Target node id.
    pub target: String,
}

// =============================================================================
// VPL → Graph Conversion
// =============================================================================

/// Convert a parsed VPL [`Program`] into a [`PipelineGraph`].
///
/// Each `StreamDecl` becomes a chain of nodes (source → op1 → op2 → …).
/// The nodes are auto-laid out vertically, one stream per column.
pub fn program_to_graph(program: &Program) -> PipelineGraph {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut node_counter: usize = 0;
    let mut stream_col: usize = 0;

    for spanned_stmt in &program.statements {
        let stmt = &spanned_stmt.node;
        if let Stmt::StreamDecl {
            name, source, ops, ..
        } = stmt
        {
            let x = stream_col as f64 * 300.0;
            let mut y = 0.0_f64;
            let y_step = 100.0;

            // 1. Source node
            let source_id = next_id(&mut node_counter);
            let (source_label, source_type, source_config) = source_to_node_info(source, name);
            nodes.push(GraphNode {
                id: source_id.clone(),
                label: source_label,
                node_type: source_type,
                config: source_config,
                position: Some((x, y)),
            });
            y += y_step;

            let mut prev_id = source_id;

            // 2. Operator nodes
            for op in ops {
                let op_id = next_id(&mut node_counter);
                let (label, node_type, config) = op_to_node_info(op);
                nodes.push(GraphNode {
                    id: op_id.clone(),
                    label,
                    node_type,
                    config,
                    position: Some((x, y)),
                });
                edges.push(GraphEdge {
                    id: format!("e{}-{}", prev_id, op_id),
                    source: prev_id.clone(),
                    target: op_id.clone(),
                });
                prev_id = op_id;
                y += y_step;
            }

            stream_col += 1;
        }
    }

    PipelineGraph { nodes, edges }
}

fn next_id(counter: &mut usize) -> String {
    let id = format!("n{counter}");
    *counter += 1;
    id
}

/// Extract node info from a [`StreamSource`].
fn source_to_node_info(
    source: &StreamSource,
    stream_name: &str,
) -> (String, String, serde_json::Value) {
    match source {
        StreamSource::Ident(name) => (
            format!("{stream_name} = {name}"),
            "source".to_string(),
            serde_json::json!({ "event_type": name, "stream_name": stream_name }),
        ),
        StreamSource::IdentWithAlias { name, alias } => (
            format!("{stream_name} = {name} as {alias}"),
            "source".to_string(),
            serde_json::json!({ "event_type": name, "alias": alias, "stream_name": stream_name }),
        ),
        StreamSource::AllWithAlias { name, alias } => {
            let label = match alias {
                Some(a) => format!("{stream_name} = all {name} as {a}"),
                None => format!("{stream_name} = all {name}"),
            };
            (
                label,
                "source".to_string(),
                serde_json::json!({ "event_type": name, "match_all": true, "alias": alias, "stream_name": stream_name }),
            )
        }
        StreamSource::FromConnector {
            event_type,
            connector_name,
            params,
        } => (
            format!("{stream_name} = {event_type}.from({connector_name})"),
            "source".to_string(),
            serde_json::json!({
                "event_type": event_type,
                "connector": connector_name,
                "params": params_to_json(params),
                "stream_name": stream_name,
            }),
        ),
        StreamSource::Sequence(decl) => {
            let steps: Vec<String> = decl
                .steps
                .iter()
                .map(|s| {
                    if let Some(ref _filter) = s.filter {
                        format!("{}: {} where ...", s.alias, s.event_type)
                    } else {
                        format!("{}: {}", s.alias, s.event_type)
                    }
                })
                .collect();
            let label = format!("Sequence: {}", steps.join(" -> "));
            (
                label,
                "source".to_string(),
                serde_json::json!({
                    "source_type": "sequence",
                    "steps": decl.steps.iter().map(|s| serde_json::json!({
                        "alias": s.alias,
                        "event_type": s.event_type,
                        "has_filter": s.filter.is_some(),
                    })).collect::<Vec<_>>(),
                    "stream_name": stream_name,
                }),
            )
        }
        StreamSource::Merge(streams) => {
            let names: Vec<&str> = streams.iter().map(|s| s.source.as_str()).collect();
            (
                format!("Merge: {}", names.join(", ")),
                "source".to_string(),
                serde_json::json!({
                    "source_type": "merge",
                    "streams": streams.iter().map(|s| serde_json::json!({
                        "name": s.name,
                        "source": s.source,
                    })).collect::<Vec<_>>(),
                    "stream_name": stream_name,
                }),
            )
        }
        StreamSource::Join(clauses) => {
            let names: Vec<&str> = clauses.iter().map(|c| c.source.as_str()).collect();
            (
                format!("Join: {}", names.join(", ")),
                "source".to_string(),
                serde_json::json!({
                    "source_type": "join",
                    "clauses": clauses.iter().map(|c| serde_json::json!({
                        "name": c.name,
                        "source": c.source,
                    })).collect::<Vec<_>>(),
                    "stream_name": stream_name,
                }),
            )
        }
        StreamSource::Timer(decl) => (
            format!("{stream_name} = timer(...)"),
            "source".to_string(),
            serde_json::json!({
                "source_type": "timer",
                "stream_name": stream_name,
                "has_initial_delay": decl.initial_delay.is_some(),
            }),
        ),
    }
}

/// Extract node info from a [`StreamOp`].
fn op_to_node_info(op: &StreamOp) -> (String, String, serde_json::Value) {
    match op {
        StreamOp::Where(expr) => (
            format!(".where({})", expr_to_short_string(expr)),
            "filter".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::Select(items) => {
            let fields: Vec<String> = items
                .iter()
                .map(|item| match item {
                    varpulis_core::ast::SelectItem::Field(f) => f.clone(),
                    varpulis_core::ast::SelectItem::Alias(a, _) => a.clone(),
                })
                .collect();
            (
                format!(".select({})", fields.join(", ")),
                "select".to_string(),
                serde_json::json!({ "fields": fields }),
            )
        }
        StreamOp::Window(args) => {
            let label = window_label(args);
            (
                label,
                "window".to_string(),
                serde_json::json!({
                    "duration": expr_to_short_string(&args.duration),
                    "sliding": args.sliding.as_ref().map(expr_to_short_string),
                    "session_gap": args.session_gap.as_ref().map(expr_to_short_string),
                }),
            )
        }
        StreamOp::Aggregate(items) => {
            let fields: Vec<String> = items
                .iter()
                .map(|item| format!("{}: {}", item.alias, expr_to_short_string(&item.expr)))
                .collect();
            (
                format!(".aggregate({})", fields.join(", ")),
                "aggregate".to_string(),
                serde_json::json!({
                    "items": items.iter().map(|i| serde_json::json!({
                        "alias": i.alias,
                        "expr": expr_to_short_string(&i.expr),
                    })).collect::<Vec<_>>(),
                }),
            )
        }
        StreamOp::Having(expr) => (
            format!(".having({})", expr_to_short_string(expr)),
            "having".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::PartitionBy(expr) => (
            format!(".partition_by({})", expr_to_short_string(expr)),
            "partition".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::OrderBy(items) => {
            let descs: Vec<String> = items
                .iter()
                .map(|i| {
                    let e = expr_to_short_string(&i.expr);
                    if i.descending {
                        format!("{e} desc")
                    } else {
                        e
                    }
                })
                .collect();
            (
                format!(".order_by({})", descs.join(", ")),
                "order".to_string(),
                serde_json::json!({ "items": descs }),
            )
        }
        StreamOp::Limit(expr) => (
            format!(".limit({})", expr_to_short_string(expr)),
            "limit".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::Distinct(expr) => {
            let label = match expr {
                Some(e) => format!(".distinct({})", expr_to_short_string(e)),
                None => ".distinct()".to_string(),
            };
            (
                label,
                "distinct".to_string(),
                serde_json::json!({ "expr": expr.as_ref().map(expr_to_short_string) }),
            )
        }
        StreamOp::Map(expr) => (
            format!(".map({})", expr_to_short_string(expr)),
            "map".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::Filter(expr) => (
            format!(".filter({})", expr_to_short_string(expr)),
            "filter".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::Emit {
            output_type,
            fields,
            target_context,
        } => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name, expr_to_short_string(&f.value)))
                .collect();
            let label = match output_type {
                Some(t) => format!(".emit as {t}({})", field_strs.join(", ")),
                None => format!(".emit({})", field_strs.join(", ")),
            };
            (
                label,
                "emit".to_string(),
                serde_json::json!({
                    "output_type": output_type,
                    "fields": fields.iter().map(|f| serde_json::json!({
                        "name": f.name,
                        "expr": expr_to_short_string(&f.value),
                    })).collect::<Vec<_>>(),
                    "target_context": target_context,
                }),
            )
        }
        StreamOp::To {
            connector_name,
            params,
        } => (
            format!(".to({connector_name})"),
            "sink".to_string(),
            serde_json::json!({
                "connector": connector_name,
                "params": params_to_json(params),
            }),
        ),
        StreamOp::ToExpr(expr) => (
            format!(".to({})", expr_to_short_string(expr)),
            "sink".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::Alert(args) => {
            let arg_strs: Vec<String> = args
                .iter()
                .map(|a| format!("{}: {}", a.name, expr_to_short_string(&a.value)))
                .collect();
            (
                format!(".alert({})", arg_strs.join(", ")),
                "alert".to_string(),
                serde_json::json!({
                    "args": args.iter().map(|a| serde_json::json!({
                        "name": a.name,
                        "expr": expr_to_short_string(&a.value),
                    })).collect::<Vec<_>>(),
                }),
            )
        }
        StreamOp::FollowedBy(clause) => {
            let label = match &clause.alias {
                Some(a) => format!("-> {} as {a}", clause.event_type),
                None => format!("-> {}", clause.event_type),
            };
            (
                label,
                "sequence_step".to_string(),
                serde_json::json!({
                    "event_type": clause.event_type,
                    "alias": clause.alias,
                    "has_filter": clause.filter.is_some(),
                    "match_all": clause.match_all,
                }),
            )
        }
        StreamOp::Within(expr) => (
            format!(".within({})", expr_to_short_string(expr)),
            "within".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::Not(clause) => (
            format!(".not({})", clause.event_type),
            "not".to_string(),
            serde_json::json!({
                "event_type": clause.event_type,
                "alias": clause.alias,
                "has_filter": clause.filter.is_some(),
            }),
        ),
        StreamOp::Print(exprs) => {
            let parts: Vec<String> = exprs.iter().map(expr_to_short_string).collect();
            (
                format!(".print({})", parts.join(", ")),
                "print".to_string(),
                serde_json::json!({ "exprs": parts }),
            )
        }
        StreamOp::Log(args) => (
            ".log(...)".to_string(),
            "log".to_string(),
            serde_json::json!({
                "args": args.iter().map(|a| serde_json::json!({
                    "name": a.name,
                    "expr": expr_to_short_string(&a.value),
                })).collect::<Vec<_>>(),
            }),
        ),
        StreamOp::Tap(args) => (
            ".tap(...)".to_string(),
            "tap".to_string(),
            serde_json::json!({
                "args": args.iter().map(|a| serde_json::json!({
                    "name": a.name,
                    "expr": expr_to_short_string(&a.value),
                })).collect::<Vec<_>>(),
            }),
        ),
        StreamOp::Pattern(def) => (
            format!(".pattern({})", def.name),
            "pattern".to_string(),
            serde_json::json!({ "name": def.name }),
        ),
        StreamOp::Score(spec) => (
            format!(".score(model: \"{}\")", spec.model_path),
            "score".to_string(),
            serde_json::json!({
                "model_path": spec.model_path,
                "inputs": spec.inputs,
                "outputs": spec.outputs,
                "gpu": spec.gpu,
                "batch_size": spec.batch_size,
            }),
        ),
        StreamOp::Forecast(spec) => (
            ".forecast(...)".to_string(),
            "forecast".to_string(),
            serde_json::json!({
                "confidence": spec.confidence.as_ref().map(expr_to_short_string),
                "horizon": spec.horizon.as_ref().map(expr_to_short_string),
                "warmup": spec.warmup.as_ref().map(expr_to_short_string),
                "max_depth": spec.max_depth.as_ref().map(expr_to_short_string),
            }),
        ),
        StreamOp::Enrich(spec) => (
            format!(".enrich({})", spec.connector_name),
            "enrich".to_string(),
            serde_json::json!({
                "connector": spec.connector_name,
                "key_expr": expr_to_short_string(&spec.key_expr),
                "fields": spec.fields,
                "cache_ttl": spec.cache_ttl.as_ref().map(expr_to_short_string),
            }),
        ),
        StreamOp::TrendAggregate(items) => {
            let parts: Vec<String> = items.iter().map(|i| i.alias.clone()).collect();
            (
                format!(".trend_aggregate({})", parts.join(", ")),
                "trend_aggregate".to_string(),
                serde_json::json!({
                    "items": items.iter().map(|i| serde_json::json!({
                        "alias": i.alias,
                        "func": i.func,
                        "has_arg": i.arg.is_some(),
                    })).collect::<Vec<_>>(),
                }),
            )
        }
        StreamOp::Fork(paths) => {
            let names: Vec<&str> = paths.iter().map(|p| p.name.as_str()).collect();
            (
                format!(".fork({})", names.join(", ")),
                "fork".to_string(),
                serde_json::json!({
                    "paths": names,
                }),
            )
        }
        StreamOp::Any(n) => (
            match n {
                Some(count) => format!(".any({count})"),
                None => ".any()".to_string(),
            },
            "any".to_string(),
            serde_json::json!({ "min_count": n }),
        ),
        StreamOp::All => (
            ".all()".to_string(),
            "all".to_string(),
            serde_json::json!({}),
        ),
        StreamOp::First => (
            ".first()".to_string(),
            "first".to_string(),
            serde_json::json!({}),
        ),
        StreamOp::Concurrent(args) => (
            ".concurrent(...)".to_string(),
            "concurrent".to_string(),
            serde_json::json!({
                "args": args.iter().map(|a| serde_json::json!({
                    "name": a.name,
                    "expr": expr_to_short_string(&a.value),
                })).collect::<Vec<_>>(),
            }),
        ),
        StreamOp::Process(expr) => (
            format!(".process({})", expr_to_short_string(expr)),
            "process".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::OnError(expr) => (
            format!(".on_error({})", expr_to_short_string(expr)),
            "on_error".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::Collect => (
            ".collect()".to_string(),
            "collect".to_string(),
            serde_json::json!({}),
        ),
        StreamOp::On(expr) => (
            format!(".on({})", expr_to_short_string(expr)),
            "on".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
        StreamOp::Context(name) => (
            format!(".context({name})"),
            "context".to_string(),
            serde_json::json!({ "name": name }),
        ),
        StreamOp::Watermark(args) => (
            ".watermark(...)".to_string(),
            "watermark".to_string(),
            serde_json::json!({
                "args": args.iter().map(|a| serde_json::json!({
                    "name": a.name,
                    "expr": expr_to_short_string(&a.value),
                })).collect::<Vec<_>>(),
            }),
        ),
        StreamOp::AllowedLateness(expr) => (
            format!(".allowed_lateness({})", expr_to_short_string(expr)),
            "allowed_lateness".to_string(),
            serde_json::json!({ "expr": expr_to_short_string(expr) }),
        ),
    }
}

/// Convert connector params to a JSON array.
fn params_to_json(params: &[ConnectorParam]) -> serde_json::Value {
    serde_json::json!(params
        .iter()
        .map(|p| serde_json::json!({
            "name": p.name,
            "value": config_value_to_string(&p.value),
        }))
        .collect::<Vec<_>>())
}

/// Render a [`ConfigValue`] as a string.
fn config_value_to_string(cv: &varpulis_core::ast::ConfigValue) -> String {
    use varpulis_core::ast::ConfigValue;
    match cv {
        ConfigValue::Bool(b) => b.to_string(),
        ConfigValue::Int(i) => i.to_string(),
        ConfigValue::Float(f) => f.to_string(),
        ConfigValue::Str(s) => format!("\"{s}\""),
        ConfigValue::Ident(s) => s.clone(),
        ConfigValue::Duration(ns) => format_duration_ns(*ns),
        ConfigValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(config_value_to_string).collect();
            format!("[{}]", items.join(", "))
        }
        ConfigValue::Map(entries) => {
            let items: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{k}: {}", config_value_to_string(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
        ConfigValue::Concat(parts) => {
            let items: Vec<String> = parts.iter().map(config_value_to_string).collect();
            items.join(" + ")
        }
    }
}

/// Create a window label from [`WindowArgs`].
fn window_label(args: &WindowArgs) -> String {
    let dur = expr_to_short_string(&args.duration);
    if let Some(ref session) = args.session_gap {
        format!(".window(session: {})", expr_to_short_string(session))
    } else if let Some(ref slide) = args.sliding {
        format!(".window({dur}, slide: {})", expr_to_short_string(slide))
    } else {
        format!(".window({dur})")
    }
}

/// Render an expression as a short human-readable string.
///
/// This is a best-effort formatter for use in graph node labels and configs.
/// It does not aim for round-trip fidelity but for readability.
fn expr_to_short_string(expr: &Expr) -> String {
    match expr {
        Expr::Null => "null".to_string(),
        Expr::Bool(b) => b.to_string(),
        Expr::Int(i) => i.to_string(),
        Expr::Float(f) => f.to_string(),
        Expr::Str(s) => format!("\"{s}\""),
        Expr::Duration(ns) => format_duration_ns(*ns),
        Expr::Timestamp(ns) => format!("@{ns}"),
        Expr::Ident(name) => name.clone(),
        Expr::Array(items) => {
            let parts: Vec<String> = items.iter().map(expr_to_short_string).collect();
            format!("[{}]", parts.join(", "))
        }
        Expr::Map(entries) => {
            let parts: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{k}: {}", expr_to_short_string(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        Expr::Binary { op, left, right } => {
            format!(
                "{} {} {}",
                expr_to_short_string(left),
                op.as_str(),
                expr_to_short_string(right)
            )
        }
        Expr::Unary { op, expr } => {
            format!("{}{}", op.as_str(), expr_to_short_string(expr))
        }
        Expr::Member { expr: e, member } => {
            format!("{}.{member}", expr_to_short_string(e))
        }
        Expr::OptionalMember { expr: e, member } => {
            format!("{}?.{member}", expr_to_short_string(e))
        }
        Expr::Index { expr: e, index } => {
            format!(
                "{}[{}]",
                expr_to_short_string(e),
                expr_to_short_string(index)
            )
        }
        Expr::Slice {
            expr: e,
            start,
            end,
        } => {
            let s = start
                .as_ref()
                .map(|s| expr_to_short_string(s))
                .unwrap_or_default();
            let en = end
                .as_ref()
                .map(|e| expr_to_short_string(e))
                .unwrap_or_default();
            format!("{}[{s}:{en}]", expr_to_short_string(e))
        }
        Expr::Call { func, args } => {
            let arg_strs: Vec<String> = args
                .iter()
                .map(|a| match a {
                    varpulis_core::ast::Arg::Positional(e) => expr_to_short_string(e),
                    varpulis_core::ast::Arg::Named(n, e) => {
                        format!("{n}: {}", expr_to_short_string(e))
                    }
                })
                .collect();
            format!("{}({})", expr_to_short_string(func), arg_strs.join(", "))
        }
        Expr::Lambda { params, body } => {
            if params.len() == 1 {
                format!("{} => {}", params[0], expr_to_short_string(body))
            } else {
                format!("({}) => {}", params.join(", "), expr_to_short_string(body))
            }
        }
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => {
            format!(
                "if {} then {} else {}",
                expr_to_short_string(cond),
                expr_to_short_string(then_branch),
                expr_to_short_string(else_branch)
            )
        }
        Expr::Coalesce { expr: e, default } => {
            format!(
                "{} ?? {}",
                expr_to_short_string(e),
                expr_to_short_string(default)
            )
        }
        Expr::Range {
            start,
            end,
            inclusive,
        } => {
            let op = if *inclusive { "..=" } else { ".." };
            format!(
                "{}{op}{}",
                expr_to_short_string(start),
                expr_to_short_string(end)
            )
        }
        Expr::Block { stmts, result } => {
            if stmts.is_empty() {
                expr_to_short_string(result)
            } else {
                format!("{{ ... {} }}", expr_to_short_string(result))
            }
        }
    }
}

/// Format a duration in nanoseconds to a human-readable string.
fn format_duration_ns(ns: u64) -> String {
    const NS_PER_MS: u64 = 1_000_000;
    const NS_PER_SEC: u64 = 1_000_000_000;
    const NS_PER_MIN: u64 = NS_PER_SEC * 60;
    const NS_PER_HOUR: u64 = NS_PER_MIN * 60;
    const NS_PER_DAY: u64 = NS_PER_HOUR * 24;

    if ns == 0 {
        return "0s".to_string();
    }
    if ns.is_multiple_of(NS_PER_DAY) {
        return format!("{}d", ns / NS_PER_DAY);
    }
    if ns.is_multiple_of(NS_PER_HOUR) {
        return format!("{}h", ns / NS_PER_HOUR);
    }
    if ns.is_multiple_of(NS_PER_MIN) {
        return format!("{}m", ns / NS_PER_MIN);
    }
    if ns.is_multiple_of(NS_PER_SEC) {
        return format!("{}s", ns / NS_PER_SEC);
    }
    if ns.is_multiple_of(NS_PER_MS) {
        return format!("{}ms", ns / NS_PER_MS);
    }
    format!("{ns}ns")
}

// =============================================================================
// Graph → VPL Generation (best-effort)
// =============================================================================

/// Generate VPL source from a [`PipelineGraph`].
///
/// This is a best-effort conversion that works for simple linear pipelines.
/// It finds source nodes (no incoming edges), walks the graph following edges,
/// and produces VPL stream declarations.
pub fn graph_to_vpl(graph: &PipelineGraph) -> String {
    use std::collections::{HashMap, HashSet};

    // Build adjacency: node_id → list of target node_ids (in insertion order)
    let mut successors: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut has_incoming: HashSet<&str> = HashSet::new();

    for edge in &graph.edges {
        successors
            .entry(edge.source.as_str())
            .or_default()
            .push(edge.target.as_str());
        has_incoming.insert(edge.target.as_str());
    }

    // Build node lookup
    let node_map: HashMap<&str, &GraphNode> =
        graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    // Find source nodes (no incoming edges)
    let source_ids: Vec<&str> = graph
        .nodes
        .iter()
        .filter(|n| !has_incoming.contains(n.id.as_str()))
        .map(|n| n.id.as_str())
        .collect();

    let mut output = String::new();

    for (stream_idx, &source_id) in source_ids.iter().enumerate() {
        if stream_idx > 0 {
            output.push('\n');
        }

        let source_node = match node_map.get(source_id) {
            Some(n) => n,
            None => continue,
        };

        // Generate stream declaration header
        let stream_name = source_node
            .config
            .get("stream_name")
            .and_then(|v| v.as_str())
            .unwrap_or("Stream");

        let source_vpl = generate_source_vpl(source_node);
        output.push_str(&format!("stream {stream_name} = {source_vpl}\n"));

        // Walk the chain of operators
        let mut current_id = source_id;
        while let Some(targets) = successors.get(current_id) {
            if let Some(&next_id) = targets.first() {
                if let Some(node) = node_map.get(next_id) {
                    let op_vpl = generate_op_vpl(node);
                    output.push_str(&format!("    {op_vpl}\n"));
                }
                current_id = next_id;
            } else {
                break;
            }
        }
    }

    output
}

/// Generate VPL source fragment for a source node.
fn generate_source_vpl(node: &GraphNode) -> String {
    let config = &node.config;

    // Check source_type for special sources
    if let Some(source_type) = config.get("source_type").and_then(|v| v.as_str()) {
        match source_type {
            "sequence" => {
                let steps = config
                    .get("steps")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|s| {
                                let alias = s.get("alias")?.as_str()?;
                                let event_type = s.get("event_type")?.as_str()?;
                                Some(format!("{alias}: {event_type}"))
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                return format!("sequence({steps})");
            }
            "merge" => {
                let streams = config
                    .get("streams")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|s| {
                                let name = s.get("name")?.as_str()?;
                                let source = s.get("source")?.as_str()?;
                                Some(format!("{name}: {source}"))
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                return format!("merge({streams})");
            }
            "join" => {
                let clauses = config
                    .get("clauses")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|c| {
                                let name = c.get("name")?.as_str()?;
                                let source = c.get("source")?.as_str()?;
                                Some(format!("{name}: {source}"))
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                return format!("join({clauses})");
            }
            "timer" => return "timer(...)".to_string(),
            _ => {}
        }
    }

    // Standard event source
    let event_type = config
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("UnknownEvent");

    let alias = config.get("alias").and_then(|v| v.as_str());
    let connector = config.get("connector").and_then(|v| v.as_str());

    if let Some(conn) = connector {
        let params = config
            .get("params")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let name = p.get("name")?.as_str()?;
                        let value = p.get("value")?.as_str()?;
                        Some(format!("{name}: {value}"))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        if params.is_empty() {
            format!("{event_type}.from({conn})")
        } else {
            format!("{event_type}.from({conn}, {params})")
        }
    } else if let Some(a) = alias {
        format!("{event_type} as {a}")
    } else {
        event_type.to_string()
    }
}

/// Generate VPL source fragment for an operator node.
fn generate_op_vpl(node: &GraphNode) -> String {
    let config = &node.config;

    match node.node_type.as_str() {
        "filter" => {
            let expr = config
                .get("expr")
                .and_then(|v| v.as_str())
                .unwrap_or("true");
            format!(".where({expr})")
        }
        "select" => {
            let fields = config
                .get("fields")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(".select({fields})")
        }
        "window" => {
            let dur = config
                .get("duration")
                .and_then(|v| v.as_str())
                .unwrap_or("1m");
            if let Some(session) = config.get("session_gap").and_then(|v| v.as_str()) {
                format!(".window(session: {session})")
            } else if let Some(slide) = config.get("sliding").and_then(|v| v.as_str()) {
                format!(".window({dur}, slide: {slide})")
            } else {
                format!(".window({dur})")
            }
        }
        "aggregate" => {
            let items = config
                .get("items")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|item| {
                            let alias = item.get("alias")?.as_str()?;
                            let expr = item.get("expr")?.as_str()?;
                            Some(format!("{alias}: {expr}"))
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(".aggregate({items})")
        }
        "having" => {
            let expr = config
                .get("expr")
                .and_then(|v| v.as_str())
                .unwrap_or("true");
            format!(".having({expr})")
        }
        "partition" => {
            let expr = config.get("expr").and_then(|v| v.as_str()).unwrap_or("id");
            format!(".partition_by({expr})")
        }
        "emit" => {
            let output_type = config.get("output_type").and_then(|v| v.as_str());
            let fields = config
                .get("fields")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|f| {
                            let name = f.get("name")?.as_str()?;
                            let expr = f.get("expr")?.as_str()?;
                            Some(format!("{name}: {expr}"))
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            match output_type {
                Some(t) => format!(".emit as {t} ({fields})"),
                None => format!(".emit({fields})"),
            }
        }
        "sink" => {
            let connector = config
                .get("connector")
                .and_then(|v| v.as_str())
                .unwrap_or("Output");
            let params = config
                .get("params")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|p| {
                            let name = p.get("name")?.as_str()?;
                            let value = p.get("value")?.as_str()?;
                            Some(format!("{name}: {value}"))
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            if params.is_empty() {
                format!(".to({connector})")
            } else {
                format!(".to({connector}, {params})")
            }
        }
        "alert" => {
            let args = config
                .get("args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|a| {
                            let name = a.get("name")?.as_str()?;
                            let expr = a.get("expr")?.as_str()?;
                            Some(format!("{name}: {expr}"))
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(".alert({args})")
        }
        "sequence_step" => {
            let event_type = config
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("Event");
            let alias = config.get("alias").and_then(|v| v.as_str());
            match alias {
                Some(a) => format!("-> {event_type} as {a}"),
                None => format!("-> {event_type}"),
            }
        }
        "within" => {
            let expr = config.get("expr").and_then(|v| v.as_str()).unwrap_or("1m");
            format!(".within({expr})")
        }
        "limit" => {
            let expr = config.get("expr").and_then(|v| v.as_str()).unwrap_or("10");
            format!(".limit({expr})")
        }
        "distinct" => {
            let expr = config.get("expr").and_then(|v| v.as_str());
            match expr {
                Some(e) => format!(".distinct({e})"),
                None => ".distinct()".to_string(),
            }
        }
        "order" => {
            let items = config
                .get("items")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(".order_by({items})")
        }
        "print" => {
            let exprs = config
                .get("exprs")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(".print({exprs})")
        }
        "map" => {
            let expr = config
                .get("expr")
                .and_then(|v| v.as_str())
                .unwrap_or("x => x");
            format!(".map({expr})")
        }
        "not" => {
            let event_type = config
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("Event");
            format!(".not({event_type})")
        }
        "score" => {
            let model = config
                .get("model_path")
                .and_then(|v| v.as_str())
                .unwrap_or("model.onnx");
            let inputs = config
                .get("inputs")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let outputs = config
                .get("outputs")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(".score(model: \"{model}\", inputs: [{inputs}], outputs: [{outputs}])")
        }
        "forecast" => ".forecast(...)".to_string(),
        "enrich" => {
            let connector = config
                .get("connector")
                .and_then(|v| v.as_str())
                .unwrap_or("Enricher");
            format!(".enrich({connector})")
        }
        "trend_aggregate" => {
            let items = config
                .get("items")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|item| {
                            let alias = item.get("alias")?.as_str()?;
                            let func = item.get("func")?.as_str()?;
                            Some(format!("{alias}: {func}()"))
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(".trend_aggregate({items})")
        }
        // Fallback: try to reconstruct from the label
        _ => node.label.clone(),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_vpl(source: &str) -> Program {
        varpulis_parser::parse(source).expect("Failed to parse VPL")
    }

    #[test]
    fn test_simple_filter_pipeline_to_graph() {
        let vpl = r"
            stream HighTemp = SensorReading
                .where(temperature > 100)
                .emit(sensor: sensor_id, temp: temperature)
        ";
        let program = parse_vpl(vpl);
        let graph = program_to_graph(&program);

        // Should have 3 nodes: source, filter, emit
        assert_eq!(graph.nodes.len(), 3);
        // Should have 2 edges: source->filter, filter->emit
        assert_eq!(graph.edges.len(), 2);

        assert_eq!(graph.nodes[0].node_type, "source");
        assert_eq!(graph.nodes[1].node_type, "filter");
        assert_eq!(graph.nodes[2].node_type, "emit");

        // Check edge connectivity
        assert_eq!(graph.edges[0].source, graph.nodes[0].id);
        assert_eq!(graph.edges[0].target, graph.nodes[1].id);
        assert_eq!(graph.edges[1].source, graph.nodes[1].id);
        assert_eq!(graph.edges[1].target, graph.nodes[2].id);
    }

    #[test]
    fn test_sequence_pipeline_to_graph() {
        let vpl = r"
            stream FraudDetect = Login as a
                -> Purchase as b
                .within(5m)
                .where(a.user_id == b.user_id)
                .emit(user: a.user_id, amount: b.amount)
        ";
        let program = parse_vpl(vpl);
        let graph = program_to_graph(&program);

        // Should have nodes: source, followed_by, within, filter, emit
        assert!(graph.nodes.len() >= 4);
        assert_eq!(graph.nodes[0].node_type, "source");

        // Check that we have a sequence_step node
        let has_seq_step = graph.nodes.iter().any(|n| n.node_type == "sequence_step");
        assert!(has_seq_step, "Should have a sequence_step node");
    }

    #[test]
    fn test_window_aggregate_pipeline_to_graph() {
        let vpl = r"
            stream AvgTemp = SensorReading
                .window(1m)
                .aggregate(avg_temp: avg(temperature), count: count())
                .emit(avg_temp: avg_temp, count: count)
        ";
        let program = parse_vpl(vpl);
        let graph = program_to_graph(&program);

        assert_eq!(graph.nodes.len(), 4);
        assert_eq!(graph.nodes[0].node_type, "source");
        assert_eq!(graph.nodes[1].node_type, "window");
        assert_eq!(graph.nodes[2].node_type, "aggregate");
        assert_eq!(graph.nodes[3].node_type, "emit");
    }

    #[test]
    fn test_sink_pipeline_to_graph() {
        #[allow(clippy::needless_raw_string_hashes)]
        let vpl = r#"
            connector MyKafka = kafka(brokers: "localhost:9092")
            stream Output = SensorReading
                .where(temperature > 50)
                .to(MyKafka, topic: "alerts")
        "#;
        let program = parse_vpl(vpl);
        let graph = program_to_graph(&program);

        // Connector decl is not a stream, only the stream decl produces nodes
        assert_eq!(graph.nodes.len(), 3);
        let sink_node = graph.nodes.iter().find(|n| n.node_type == "sink");
        assert!(sink_node.is_some(), "Should have a sink node");

        let sink = sink_node.unwrap();
        assert_eq!(
            sink.config.get("connector").and_then(|v| v.as_str()),
            Some("MyKafka")
        );
    }

    #[test]
    fn test_multiple_streams_to_graph() {
        let vpl = r"
            stream Hot = SensorReading
                .where(temperature > 100)
                .emit(sensor: sensor_id)

            stream Cold = SensorReading
                .where(temperature < 0)
                .emit(sensor: sensor_id)
        ";
        let program = parse_vpl(vpl);
        let graph = program_to_graph(&program);

        // 2 streams x 3 nodes each = 6 nodes, 2 streams x 2 edges = 4 edges
        assert_eq!(graph.nodes.len(), 6);
        assert_eq!(graph.edges.len(), 4);
    }

    #[test]
    fn test_graph_positions_auto_layout() {
        let vpl = r"
            stream A = Event1
                .where(x > 1)

            stream B = Event2
                .where(y > 2)
        ";
        let program = parse_vpl(vpl);
        let graph = program_to_graph(&program);

        // First stream: x=0, second stream: x=300
        let (x0, _) = graph.nodes[0].position.unwrap();
        let (x2, _) = graph.nodes[2].position.unwrap();
        assert!((x0 - 0.0).abs() < f64::EPSILON);
        assert!((x2 - 300.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_round_trip_preserves_structure() {
        let vpl = r"
            stream HighTemp = SensorReading
                .where(temperature > 100)
                .emit(sensor: sensor_id, temp: temperature)
        ";
        let program = parse_vpl(vpl);
        let graph = program_to_graph(&program);
        let generated = graph_to_vpl(&graph);

        // Parse the generated VPL back
        let program2 = parse_vpl(&generated);
        let graph2 = program_to_graph(&program2);

        // Same number of nodes and edges
        assert_eq!(graph.nodes.len(), graph2.nodes.len());
        assert_eq!(graph.edges.len(), graph2.edges.len());

        // Same node types in order
        for (n1, n2) in graph.nodes.iter().zip(graph2.nodes.iter()) {
            assert_eq!(n1.node_type, n2.node_type);
        }
    }

    #[test]
    fn test_graph_to_vpl_simple() {
        let graph = PipelineGraph {
            nodes: vec![
                GraphNode {
                    id: "n0".to_string(),
                    label: "Source".to_string(),
                    node_type: "source".to_string(),
                    config: serde_json::json!({
                        "event_type": "SensorReading",
                        "stream_name": "HighTemp",
                    }),
                    position: None,
                },
                GraphNode {
                    id: "n1".to_string(),
                    label: "Filter".to_string(),
                    node_type: "filter".to_string(),
                    config: serde_json::json!({ "expr": "temperature > 100" }),
                    position: None,
                },
                GraphNode {
                    id: "n2".to_string(),
                    label: "Emit".to_string(),
                    node_type: "emit".to_string(),
                    config: serde_json::json!({
                        "fields": [{"name": "temp", "expr": "temperature"}],
                    }),
                    position: None,
                },
            ],
            edges: vec![
                GraphEdge {
                    id: "e0".to_string(),
                    source: "n0".to_string(),
                    target: "n1".to_string(),
                },
                GraphEdge {
                    id: "e1".to_string(),
                    source: "n1".to_string(),
                    target: "n2".to_string(),
                },
            ],
        };

        let vpl = graph_to_vpl(&graph);
        assert!(vpl.contains("stream HighTemp = SensorReading"));
        assert!(vpl.contains(".where(temperature > 100)"));
        assert!(vpl.contains(".emit(temp: temperature)"));
    }

    #[test]
    fn test_empty_program_produces_empty_graph() {
        let vpl = "# just a comment\n";
        // Empty programs may not parse, but let's handle it gracefully
        if let Ok(program) = varpulis_parser::parse(vpl) {
            let graph = program_to_graph(&program);
            assert!(graph.nodes.is_empty());
            assert!(graph.edges.is_empty());
        }
    }

    #[test]
    fn test_expr_to_short_string_basic() {
        assert_eq!(expr_to_short_string(&Expr::Int(42)), "42");
        assert_eq!(expr_to_short_string(&Expr::Float(1.5)), "1.5");
        assert_eq!(
            expr_to_short_string(&Expr::Str("hello".into())),
            "\"hello\""
        );
        assert_eq!(expr_to_short_string(&Expr::Bool(true)), "true");
        assert_eq!(expr_to_short_string(&Expr::Null), "null");
        assert_eq!(expr_to_short_string(&Expr::Ident("x".into())), "x");
    }

    #[test]
    fn test_format_duration_ns() {
        assert_eq!(format_duration_ns(60_000_000_000), "1m");
        assert_eq!(format_duration_ns(5_000_000_000), "5s");
        assert_eq!(format_duration_ns(3_600_000_000_000), "1h");
        assert_eq!(format_duration_ns(86_400_000_000_000), "1d");
        assert_eq!(format_duration_ns(500_000_000), "500ms");
        assert_eq!(format_duration_ns(0), "0s");
    }
}
