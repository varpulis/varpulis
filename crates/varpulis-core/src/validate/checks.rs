//! Semantic check implementations for Pass 1 and Pass 2.

use crate::ast::*;
use crate::span::Span;

use super::builtins::{
    self, ParamContext, AGGREGATE_FUNCTIONS, AGGREGATE_REQUIRES_FIELD, AGGREGATE_REQUIRES_TWO_ARGS,
    LOG_PARAMS, WATERMARK_PARAMS,
};
use super::scope::*;
use super::suggest::{did_you_mean, suggest};
use super::{RelatedSpan, Severity, Validator};

// ---------------------------------------------------------------------------
// Pass 1: Declaration Collection
// ---------------------------------------------------------------------------

pub fn pass1_declarations(v: &mut Validator, program: &Program) {
    for stmt in &program.statements {
        let span = stmt.span;
        match &stmt.node {
            Stmt::EventDecl { name, fields, .. } => {
                if let Some(prev) = v.symbols.events.get(name) {
                    v.emit_with_related(
                        Severity::Error,
                        span,
                        "E001",
                        format!("duplicate event type '{}'", name),
                        vec![RelatedSpan {
                            span: prev.span,
                            message: "previously declared here".to_string(),
                        }],
                    );
                } else {
                    v.symbols.events.insert(
                        name.clone(),
                        EventInfo {
                            span,
                            field_names: fields.iter().map(|f| f.name.clone()).collect(),
                        },
                    );
                }
            }
            Stmt::StreamDecl { name, .. } => {
                if let Some(prev) = v.symbols.streams.get(name) {
                    v.emit_with_related(
                        Severity::Error,
                        span,
                        "E002",
                        format!("duplicate stream '{}'", name),
                        vec![RelatedSpan {
                            span: prev.span,
                            message: "previously declared here".to_string(),
                        }],
                    );
                } else {
                    v.symbols.streams.insert(name.clone(), StreamInfo { span });
                }
            }
            Stmt::FnDecl { name, params, .. } => {
                if let Some(prev) = v.symbols.functions.get(name) {
                    v.emit_with_related(
                        Severity::Error,
                        span,
                        "E003",
                        format!("duplicate function '{}'", name),
                        vec![RelatedSpan {
                            span: prev.span,
                            message: "previously declared here".to_string(),
                        }],
                    );
                } else {
                    v.symbols.functions.insert(
                        name.clone(),
                        FunctionInfo {
                            span,
                            param_count: params.len(),
                        },
                    );
                }
            }
            Stmt::ConnectorDecl {
                name,
                connector_type,
                ..
            } => {
                if let Some(prev) = v.symbols.connectors.get(name) {
                    v.emit_with_related(
                        Severity::Error,
                        span,
                        "E004",
                        format!("duplicate connector '{}'", name),
                        vec![RelatedSpan {
                            span: prev.span,
                            message: "previously declared here".to_string(),
                        }],
                    );
                } else {
                    v.symbols.connectors.insert(
                        name.clone(),
                        ConnectorInfo {
                            span,
                            connector_type: connector_type.clone(),
                        },
                    );
                }
            }
            Stmt::ContextDecl { name, .. } => {
                if let Some(prev) = v.symbols.contexts.get(name) {
                    v.emit_with_related(
                        Severity::Error,
                        span,
                        "E005",
                        format!("duplicate context '{}'", name),
                        vec![RelatedSpan {
                            span: prev.span,
                            message: "previously declared here".to_string(),
                        }],
                    );
                } else {
                    v.symbols
                        .contexts
                        .insert(name.clone(), ContextInfo { span });
                }
            }
            Stmt::PatternDecl { name, .. } => {
                if let Some(prev) = v.symbols.patterns.get(name) {
                    v.emit_with_related(
                        Severity::Error,
                        span,
                        "E006",
                        format!("duplicate pattern '{}'", name),
                        vec![RelatedSpan {
                            span: prev.span,
                            message: "previously declared here".to_string(),
                        }],
                    );
                } else {
                    v.symbols
                        .patterns
                        .insert(name.clone(), PatternInfo { span });
                }
            }
            Stmt::VarDecl { name, mutable, .. } => {
                v.symbols.variables.insert(
                    name.clone(),
                    VarInfo {
                        span,
                        mutable: *mutable,
                    },
                );
            }
            Stmt::ConstDecl { name, .. } => {
                v.symbols.variables.insert(
                    name.clone(),
                    VarInfo {
                        span,
                        mutable: false,
                    },
                );
            }
            Stmt::TypeDecl { name, .. } => {
                if let Some(prev) = v.symbols.types.get(name) {
                    v.emit_with_related(
                        Severity::Error,
                        span,
                        "E007",
                        format!("duplicate type alias '{}'", name),
                        vec![RelatedSpan {
                            span: prev.span,
                            message: "previously declared here".to_string(),
                        }],
                    );
                } else {
                    v.symbols.types.insert(name.clone(), TypeInfo { span });
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Pass 2: Semantic Checks
// ---------------------------------------------------------------------------

pub fn pass2_semantic(v: &mut Validator, program: &Program) {
    for stmt in &program.statements {
        let span = stmt.span;
        match &stmt.node {
            Stmt::StreamDecl { source, ops, .. } => {
                check_stream_source(v, source, span);
                check_stream_ops(v, ops, source, span);
            }
            Stmt::PatternDecl { expr, .. } => {
                check_sase_pattern_refs(v, expr, span);
            }
            Stmt::Assignment { name, value } => {
                check_assignment(v, name, span);
                check_expr_functions(v, value, span);
            }
            Stmt::VarDecl { value, .. } | Stmt::ConstDecl { value, .. } => {
                check_expr_functions(v, value, span);
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Assignment mutability checks
// ---------------------------------------------------------------------------

fn check_assignment(v: &mut Validator, name: &str, span: Span) {
    if let Some(var_info) = v.symbols.variables.get(name) {
        if !var_info.mutable {
            let decl_snippet = v
                .snippet(var_info.span)
                .unwrap_or("")
                .lines()
                .next()
                .unwrap_or("");
            let context = if decl_snippet.is_empty() {
                String::new()
            } else {
                format!(" (from: {})", decl_snippet.trim())
            };
            v.emit_with_related(
                Severity::Error,
                span,
                "E040",
                format!("cannot assign to immutable variable '{}'{}", name, context),
                vec![RelatedSpan {
                    span: var_info.span,
                    message: "declared as immutable here — use 'var' instead of 'let'".to_string(),
                }],
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Stream source checks
// ---------------------------------------------------------------------------

fn check_stream_source(v: &mut Validator, source: &StreamSource, span: Span) {
    match source {
        StreamSource::Ident(name) => {
            check_source_name(v, name, span);
        }
        StreamSource::IdentWithAlias { name, .. } | StreamSource::AllWithAlias { name, .. } => {
            check_source_name(v, name, span);
        }
        StreamSource::FromConnector {
            connector_name,
            params,
            ..
        } => {
            if !v.symbols.connectors.contains_key(connector_name) {
                let suggestion = did_you_mean(connector_name, &v.symbols.connector_names());
                v.emit_with_hint(
                    Severity::Error,
                    span,
                    "E030",
                    format!("undefined connector '{}'", connector_name),
                    format!(
                        "declare it with: connector {} = type (...){}",
                        connector_name, suggestion
                    ),
                );
            } else {
                let connector_type = v.symbols.connectors[connector_name].connector_type.clone();
                check_connector_params(
                    v,
                    params,
                    &connector_type,
                    ParamContext::Source,
                    ".from()",
                    span,
                );
            }
        }
        StreamSource::Merge(inline_streams) => {
            for s in inline_streams {
                check_source_name(v, &s.source, span);
            }
        }
        StreamSource::Join(clauses) => {
            for c in clauses {
                check_source_name(v, &c.source, span);
            }
        }
        StreamSource::Sequence(seq) => {
            for step in &seq.steps {
                check_source_name(v, &step.event_type, span);
            }
        }
        StreamSource::Timer(_) => {}
    }
}

fn check_source_name(v: &mut Validator, name: &str, span: Span) {
    // Only warn — implicit event types are valid
    if !v.symbols.events.contains_key(name)
        && !v.symbols.streams.contains_key(name)
        && !v.symbols.patterns.contains_key(name)
    {
        let suggestion = did_you_mean(name, &v.symbols.source_names());
        v.emit_with_hint(
            Severity::Warning,
            span,
            "W030",
            format!("reference to undeclared event type or stream '{}'", name),
            format!(
                "implicit event types are valid, but consider declaring it explicitly{}",
                suggestion
            ),
        );
    }
}

// ---------------------------------------------------------------------------
// Stream operations checks
// ---------------------------------------------------------------------------

fn check_stream_ops(v: &mut Validator, ops: &[StreamOp], source: &StreamSource, span: Span) {
    let mut seen_aggregate = false;
    let mut seen_window = false;
    let mut in_sequence = is_sequence_source(source);

    for op in ops {
        match op {
            // --- Unimplemented operations (E090) ---
            StreamOp::Map(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    span,
                    "E090",
                    ".map() is not implemented".to_string(),
                    "use .select() with expressions instead".to_string(),
                );
            }
            StreamOp::Filter(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    span,
                    "E090",
                    ".filter() is not implemented".to_string(),
                    "use .where() instead".to_string(),
                );
            }
            StreamOp::Concurrent(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    span,
                    "E090",
                    ".concurrent() is not yet implemented".to_string(),
                    "use .context() for parallel processing across cores".to_string(),
                );
            }
            StreamOp::OnError(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    span,
                    "E090",
                    ".on_error() is not yet implemented".to_string(),
                    "handle errors in your .where() or .select() logic".to_string(),
                );
            }
            StreamOp::Collect => {
                v.emit(
                    Severity::Error,
                    span,
                    "E090",
                    ".collect() is not yet implemented".to_string(),
                );
            }
            StreamOp::Fork(_) => {
                v.emit(
                    Severity::Error,
                    span,
                    "E090",
                    ".fork() is not yet implemented".to_string(),
                );
            }
            StreamOp::Any(_) => {
                v.emit(
                    Severity::Error,
                    span,
                    "E090",
                    ".any() is not yet implemented".to_string(),
                );
            }
            StreamOp::All => {
                v.emit(
                    Severity::Error,
                    span,
                    "E090",
                    ".all() is not yet implemented".to_string(),
                );
            }
            StreamOp::First => {
                v.emit(
                    Severity::Error,
                    span,
                    "E090",
                    ".first() is not yet implemented".to_string(),
                );
            }
            StreamOp::Distinct(_) => {
                v.emit(
                    Severity::Error,
                    span,
                    "E090",
                    ".distinct() is not yet implemented".to_string(),
                );
            }
            StreamOp::OrderBy(_) => {
                v.emit(
                    Severity::Error,
                    span,
                    "E090",
                    ".order_by() is not yet implemented".to_string(),
                );
            }
            StreamOp::Limit(_) => {
                v.emit(
                    Severity::Error,
                    span,
                    "E090",
                    ".limit() is not yet implemented".to_string(),
                );
            }
            StreamOp::ToExpr(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    span,
                    "E090",
                    ".to(expr) is not supported".to_string(),
                    "use .to(ConnectorName, ...) with a declared connector".to_string(),
                );
            }

            // --- Operation ordering ---
            StreamOp::Having(_) => {
                if !seen_aggregate {
                    v.emit_with_hint(
                        Severity::Error,
                        span,
                        "E010",
                        ".having() used without a prior .aggregate()".to_string(),
                        "add .aggregate(...) before .having()".to_string(),
                    );
                }
                check_boolean_expr(v, having_expr(op), ".having()", span);
            }
            StreamOp::Aggregate(items) => {
                if seen_aggregate {
                    v.emit(
                        Severity::Error,
                        span,
                        "E011",
                        "duplicate .aggregate() — only one aggregation per stream is allowed"
                            .to_string(),
                    );
                }
                if !seen_window {
                    v.emit_with_hint(
                        Severity::Warning,
                        span,
                        "W001",
                        ".aggregate() without a prior .window()".to_string(),
                        "results will accumulate indefinitely; add .window() for bounded aggregation".to_string(),
                    );
                }
                seen_aggregate = true;
                check_aggregate_items(v, items, span);
            }
            StreamOp::Window(_) => {
                if seen_window {
                    v.emit(
                        Severity::Error,
                        span,
                        "E012",
                        "duplicate .window() — only one window per stream is allowed".to_string(),
                    );
                }
                seen_window = true;
            }
            StreamOp::PartitionBy(_) => {
                if seen_window {
                    v.emit_with_hint(
                        Severity::Warning,
                        span,
                        "W002",
                        ".partition_by() after .window() — partitioning should come before windowing".to_string(),
                        "move .partition_by() before .window() for correct behavior".to_string(),
                    );
                }
            }
            StreamOp::Within(expr) => {
                if !in_sequence {
                    v.emit_with_hint(
                        Severity::Error,
                        span,
                        "E020",
                        ".within() used outside a sequence context".to_string(),
                        ".within() requires a sequence source or -> (followed_by) operators"
                            .to_string(),
                    );
                }
                check_duration_expr(v, expr, ".within()", span);
            }

            // --- Sequence tracking ---
            StreamOp::FollowedBy(_) | StreamOp::Not(_) => {
                in_sequence = true;
            }

            // --- Parameter validation ---
            StreamOp::Log(args) => {
                check_named_params(v, args, LOG_PARAMS, ".log()", span);
            }
            StreamOp::Watermark(args) => {
                check_named_params(v, args, WATERMARK_PARAMS, ".watermark()", span);
            }

            // --- Name resolution ---
            StreamOp::To {
                connector_name,
                params,
            } => {
                if !v.symbols.connectors.contains_key(connector_name) {
                    let suggestion = did_you_mean(connector_name, &v.symbols.connector_names());
                    // Include existing connector types in hint for context
                    let available = v
                        .symbols
                        .connectors
                        .values()
                        .map(|c| c.connector_type.as_str())
                        .collect::<Vec<_>>();
                    let avail_hint = if available.is_empty() {
                        String::new()
                    } else {
                        format!(" (declared connector types: {})", available.join(", "))
                    };
                    v.emit_with_hint(
                        Severity::Error,
                        span,
                        "E030",
                        format!("undefined connector '{}'", connector_name),
                        format!(
                            "declare it with: connector {} = type (...){}{}",
                            connector_name, suggestion, avail_hint
                        ),
                    );
                } else {
                    let connector_type =
                        v.symbols.connectors[connector_name].connector_type.clone();
                    check_connector_params(
                        v,
                        params,
                        &connector_type,
                        ParamContext::Sink,
                        ".to()",
                        span,
                    );
                }
            }
            StreamOp::Context(name) => {
                if !v.symbols.contexts.contains_key(name) {
                    let suggestion = did_you_mean(name, &v.symbols.context_names());
                    v.emit_with_hint(
                        Severity::Error,
                        span,
                        "E031",
                        format!("undefined context '{}'", name),
                        format!(
                            "declare it with: context {} (cores: [0, 1]){}",
                            name, suggestion
                        ),
                    );
                }
            }

            // --- Expression type checks ---
            StreamOp::Where(expr) => {
                check_boolean_expr(v, expr, ".where()", span);
            }
            StreamOp::AllowedLateness(expr) => {
                check_duration_expr(v, expr, ".allowed_lateness()", span);
            }

            // --- Emit field validation ---
            StreamOp::Emit { output_type, .. } => {
                if let Some(type_name) = output_type {
                    if let Some(event_info) = v.symbols.events.get(type_name) {
                        // Validate that emitted type is a known event
                        let _ = &event_info.field_names; // field_names used for future field-level validation
                    } else if !v.symbols.is_declared(type_name) {
                        let suggestion = did_you_mean(type_name, &v.symbols.all_names());
                        v.emit_with_hint(
                            Severity::Warning,
                            span,
                            "W031",
                            format!(".emit as '{}' references an undeclared type", type_name),
                            format!(
                                "consider declaring: event {} {{ ... }}{}",
                                type_name, suggestion
                            ),
                        );
                    }
                }
            }

            // --- Operations that are fine ---
            StreamOp::Select(_)
            | StreamOp::Tap(_)
            | StreamOp::Print(_)
            | StreamOp::Pattern(_)
            | StreamOp::Process(_)
            | StreamOp::On(_)
            | StreamOp::TrendAggregate(_)
            | StreamOp::Score(_)
            | StreamOp::Forecast(_) => {}
        }
    }
}

fn is_sequence_source(source: &StreamSource) -> bool {
    matches!(source, StreamSource::Sequence(_))
}

fn having_expr(op: &StreamOp) -> &Expr {
    match op {
        StreamOp::Having(e) => e,
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Expression type checks
// ---------------------------------------------------------------------------

fn check_boolean_expr(v: &mut Validator, expr: &Expr, context: &str, span: Span) {
    match expr {
        // Literal non-bools are errors
        Expr::Int(_)
        | Expr::Float(_)
        | Expr::Str(_)
        | Expr::Array(_)
        | Expr::Map(_)
        | Expr::Null
        | Expr::Duration(_)
        | Expr::Timestamp(_) => {
            v.emit_with_hint(
                Severity::Error,
                span,
                "E060",
                format!(
                    "{} condition must be a boolean expression, got {} literal",
                    context,
                    literal_type_name(expr)
                ),
                "use a comparison like field > value or a boolean expression".to_string(),
            );
        }
        // Arithmetic expressions are suspicious
        Expr::Binary { op, .. }
            if matches!(
                op,
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod | BinOp::Pow
            ) =>
        {
            v.emit_with_hint(
                Severity::Warning,
                span,
                "W060",
                format!(
                    "{} condition is an arithmetic expression ({}), expected boolean",
                    context,
                    op.as_str()
                ),
                "use a comparison operator (==, !=, <, >, <=, >=)".to_string(),
            );
        }
        _ => {} // Ident, comparison, logical, call — all ok
    }
}

fn check_duration_expr(v: &mut Validator, expr: &Expr, context: &str, span: Span) {
    match expr {
        Expr::Duration(_) | Expr::Ident(_) | Expr::Member { .. } | Expr::Call { .. } => {}
        Expr::Int(_) => {} // count-based is allowed
        Expr::Str(_) | Expr::Bool(_) | Expr::Float(_) | Expr::Array(_) | Expr::Null => {
            v.emit_with_hint(
                Severity::Error,
                span,
                "E061",
                format!(
                    "{} must be a duration, got {} literal",
                    context,
                    literal_type_name(expr)
                ),
                "use a duration like 5s, 1m, 1h".to_string(),
            );
        }
        _ => {} // expressions are ok
    }
}

fn literal_type_name(expr: &Expr) -> &'static str {
    match expr {
        Expr::Int(_) => "integer",
        Expr::Float(_) => "float",
        Expr::Str(_) => "string",
        Expr::Bool(_) => "boolean",
        Expr::Null => "null",
        Expr::Duration(_) => "duration",
        Expr::Timestamp(_) => "timestamp",
        Expr::Array(_) => "array",
        Expr::Map(_) => "map",
        _ => "expression",
    }
}

// ---------------------------------------------------------------------------
// Connector parameter validation
// ---------------------------------------------------------------------------

fn check_connector_params(
    v: &mut Validator,
    params: &[ConnectorParam],
    connector_type: &str,
    ctx: builtins::ParamContext,
    op_name: &str,
    span: Span,
) {
    let schema = match builtins::connector_params_for_type(connector_type) {
        Some(s) => s,
        None => return, // unknown connector type — skip validation for forward compat
    };

    let valid_names: Vec<&str> = schema
        .iter()
        .filter(|p| p.valid_in(ctx))
        .map(|p| p.name)
        .collect();

    for param in params {
        // Look up in full schema (any context)
        let def = schema.iter().find(|d| d.name == param.name);
        match def {
            None => {
                // Unknown parameter name
                let suggestion = did_you_mean(&param.name, &valid_names);
                v.emit_with_hint(
                    Severity::Warning,
                    span,
                    "W080",
                    format!(
                        "unknown parameter '{}' for {} connector in {}",
                        param.name, connector_type, op_name
                    ),
                    format!("valid parameters: {}{}", valid_names.join(", "), suggestion),
                );
            }
            Some(def) => {
                // Check context validity
                if !def.valid_in(ctx) {
                    let ctx_name = match ctx {
                        builtins::ParamContext::Source => "source (.from())",
                        builtins::ParamContext::Sink => "sink (.to())",
                        builtins::ParamContext::Both => "both",
                    };
                    v.emit_with_hint(
                        Severity::Warning,
                        span,
                        "W080",
                        format!(
                            "parameter '{}' is not valid in {} context",
                            param.name, ctx_name
                        ),
                        format!(
                            "'{}' is only valid for {}",
                            param.name,
                            match def.context {
                                builtins::ParamContext::Source => ".from() (source)",
                                builtins::ParamContext::Sink => ".to() (sink)",
                                builtins::ParamContext::Both => "both",
                            }
                        ),
                    );
                }

                // Check type match
                let type_ok = match def.param_type {
                    builtins::ParamType::Str => matches!(
                        param.value,
                        crate::ast::ConfigValue::Str(_) | crate::ast::ConfigValue::Ident(_)
                    ),
                    builtins::ParamType::Int => {
                        matches!(param.value, crate::ast::ConfigValue::Int(_))
                    }
                    builtins::ParamType::Bool => {
                        matches!(param.value, crate::ast::ConfigValue::Bool(_))
                    }
                };
                if !type_ok {
                    let expected = match def.param_type {
                        builtins::ParamType::Str => "string",
                        builtins::ParamType::Int => "integer",
                        builtins::ParamType::Bool => "boolean",
                    };
                    v.emit_with_hint(
                        Severity::Warning,
                        span,
                        "W081",
                        format!("parameter '{}' expects {} value", param.name, expected),
                        format!("{}: {}", def.name, def.description),
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Named parameter validation
// ---------------------------------------------------------------------------

fn check_named_params(
    v: &mut Validator,
    args: &[NamedArg],
    valid: &[&str],
    context: &str,
    span: Span,
) {
    for arg in args {
        if !valid.contains(&arg.name.as_str()) {
            let suggestion = did_you_mean(&arg.name, valid);
            v.emit_with_hint(
                Severity::Error,
                span,
                "E080",
                format!("unknown parameter '{}' for {}", arg.name, context),
                format!("valid parameters: {}{}", valid.join(", "), suggestion),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate validation
// ---------------------------------------------------------------------------

fn check_aggregate_items(v: &mut Validator, items: &[AggItem], span: Span) {
    for item in items {
        match &item.expr {
            Expr::Call { func, args } => {
                if let Some(func_name) = extract_ident(func) {
                    if !builtins::is_aggregate_function(&func_name) {
                        let suggestion = did_you_mean(&func_name, AGGREGATE_FUNCTIONS);
                        v.emit_with_hint(
                            Severity::Error,
                            span,
                            "E070",
                            format!(
                                "unknown aggregate function '{}' in alias '{}'",
                                func_name, item.alias
                            ),
                            format!(
                                "known aggregate functions: {}{}",
                                AGGREGATE_FUNCTIONS.join(", "),
                                suggestion
                            ),
                        );
                        continue;
                    }

                    // Check functions that require a field argument
                    if AGGREGATE_REQUIRES_FIELD.contains(&func_name.as_str()) && args.is_empty() {
                        v.emit_with_hint(
                            Severity::Error,
                            span,
                            "E071",
                            format!(
                                "aggregate function '{}' requires a field argument",
                                func_name
                            ),
                            format!("usage: {}(field_name)", func_name),
                        );
                    }

                    // Check functions that require two arguments
                    if AGGREGATE_REQUIRES_TWO_ARGS.contains(&func_name.as_str()) && args.len() < 2 {
                        v.emit_with_hint(
                            Severity::Error,
                            span,
                            "E072",
                            format!(
                                "aggregate function '{}' requires two arguments: field and period",
                                func_name
                            ),
                            format!("usage: {}(field_name, period)", func_name),
                        );
                    }
                }
            }
            Expr::Ident(name) => {
                // Bare field reference without aggregate function
                v.emit_with_hint(
                    Severity::Error,
                    span,
                    "E073",
                    format!(
                        "bare field reference '{}' in aggregate without an aggregate function",
                        name
                    ),
                    format!(
                        "wrap in an aggregate function, e.g. last({}), first({}), or sum({})",
                        name, name, name
                    ),
                );
            }
            _ => {
                // Complex expressions in aggregate are allowed (e.g. arithmetic)
            }
        }
    }
}

fn extract_ident(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Ident(name) => Some(name.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// SASE pattern reference checks
// ---------------------------------------------------------------------------

fn check_sase_pattern_refs(v: &mut Validator, expr: &SasePatternExpr, span: Span) {
    match expr {
        SasePatternExpr::Event(name) => {
            check_source_name(v, name, span);
        }
        SasePatternExpr::Seq(items) => {
            for item in items {
                check_source_name(v, &item.event_type, span);
            }
        }
        SasePatternExpr::And(a, b) | SasePatternExpr::Or(a, b) => {
            check_sase_pattern_refs(v, a, span);
            check_sase_pattern_refs(v, b, span);
        }
        SasePatternExpr::Not(inner) | SasePatternExpr::Group(inner) => {
            check_sase_pattern_refs(v, inner, span);
        }
    }
}

// ---------------------------------------------------------------------------
// Function call checks (for expressions)
// ---------------------------------------------------------------------------

fn check_function_call(v: &mut Validator, name: &str, args_len: usize, span: Span) {
    // Check user-declared functions first (with arity)
    if let Some(func_info) = v.symbols.functions.get(name) {
        if args_len != func_info.param_count {
            v.emit_with_related(
                Severity::Error,
                span,
                "E051",
                format!(
                    "function '{}' expects {} argument(s), but {} provided",
                    name, func_info.param_count, args_len
                ),
                vec![RelatedSpan {
                    span: func_info.span,
                    message: "function declared here".to_string(),
                }],
            );
        }
        return;
    }

    // Check builtins
    if builtins::is_known_function(name) {
        return;
    }

    // Unknown function
    let mut candidates: Vec<&str> = builtins::BUILTIN_FUNCTIONS.to_vec();
    candidates.extend(builtins::AGGREGATE_FUNCTIONS);
    candidates.extend(v.symbols.function_names());
    let suggestion = suggest(name, &candidates);
    let hint = match suggestion {
        Some(s) => format!("did you mean '{}'?", s),
        None => "check the function name or declare it with fn".to_string(),
    };
    v.emit_with_hint(
        Severity::Error,
        span,
        "E050",
        format!("unknown function '{}'", name),
        hint,
    );
}

// ---------------------------------------------------------------------------
// Expression walking — validates function calls within expressions
// ---------------------------------------------------------------------------

/// Recursively walk an expression to validate function calls.
pub fn check_expr_functions(v: &mut Validator, expr: &Expr, span: Span) {
    match expr {
        Expr::Call { func, args } => {
            if let Some(name) = extract_ident(func) {
                check_function_call(v, &name, count_positional_args(args), span);
            }
            // Walk arguments
            for arg in args {
                match arg {
                    Arg::Positional(e) | Arg::Named(_, e) => check_expr_functions(v, e, span),
                }
            }
        }
        Expr::Binary { left, right, .. } => {
            check_expr_functions(v, left, span);
            check_expr_functions(v, right, span);
        }
        Expr::Unary { expr: inner, .. } => {
            check_expr_functions(v, inner, span);
        }
        Expr::Member { expr: inner, .. } | Expr::OptionalMember { expr: inner, .. } => {
            check_expr_functions(v, inner, span);
        }
        Expr::Index { expr: e, index } => {
            check_expr_functions(v, e, span);
            check_expr_functions(v, index, span);
        }
        Expr::Slice {
            expr: e,
            start,
            end,
        } => {
            check_expr_functions(v, e, span);
            if let Some(s) = start {
                check_expr_functions(v, s, span);
            }
            if let Some(e) = end {
                check_expr_functions(v, e, span);
            }
        }
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => {
            check_expr_functions(v, cond, span);
            check_expr_functions(v, then_branch, span);
            check_expr_functions(v, else_branch, span);
        }
        Expr::Coalesce { expr: e, default } => {
            check_expr_functions(v, e, span);
            check_expr_functions(v, default, span);
        }
        Expr::Array(elems) => {
            for e in elems {
                check_expr_functions(v, e, span);
            }
        }
        Expr::Map(entries) => {
            for (_, e) in entries {
                check_expr_functions(v, e, span);
            }
        }
        Expr::Lambda { body, .. } => {
            check_expr_functions(v, body, span);
        }
        Expr::Range { start, end, .. } => {
            check_expr_functions(v, start, span);
            check_expr_functions(v, end, span);
        }
        Expr::Block { stmts, result } => {
            for (_, _, val, _) in stmts {
                check_expr_functions(v, val, span);
            }
            check_expr_functions(v, result, span);
        }
        // Leaves — no recursion needed
        Expr::Null
        | Expr::Bool(_)
        | Expr::Int(_)
        | Expr::Float(_)
        | Expr::Str(_)
        | Expr::Duration(_)
        | Expr::Timestamp(_)
        | Expr::Ident(_) => {}
    }
}

fn count_positional_args(args: &[Arg]) -> usize {
    args.iter()
        .filter(|a| matches!(a, Arg::Positional(_)))
        .count()
}
