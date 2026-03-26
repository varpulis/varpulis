//! Semantic check implementations for Pass 1 and Pass 2.

use std::collections::HashMap;

use super::builtins::{
    self, ParamContext, AGGREGATE_FUNCTIONS, AGGREGATE_REQUIRES_FIELD, AGGREGATE_REQUIRES_TWO_ARGS,
    ALERT_PARAMS, LOG_PARAMS, WATERMARK_PARAMS,
};
use super::scope::*;
use super::suggest::{did_you_mean, suggest};
use super::{RelatedSpan, Severity, Validator};
use crate::ast::*;
use crate::span::Span;

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
                        format!("duplicate event type '{name}'"),
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
                        format!("duplicate stream '{name}'"),
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
                        format!("duplicate function '{name}'"),
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
                        format!("duplicate connector '{name}'"),
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
                        format!("duplicate context '{name}'"),
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
                        format!("duplicate pattern '{name}'"),
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
            Stmt::TypeDecl { name, fields, .. } => {
                if let Some(prev) = v.symbols.types.get(name) {
                    v.emit_with_related(
                        Severity::Error,
                        span,
                        "E007",
                        format!("duplicate type alias '{name}'"),
                        vec![RelatedSpan {
                            span: prev.span,
                            message: "previously declared here".to_string(),
                        }],
                    );
                } else {
                    v.symbols.types.insert(
                        name.clone(),
                        TypeInfo {
                            span,
                            fields: fields
                                .iter()
                                .map(|f| (f.name.clone(), f.ty.clone()))
                                .collect(),
                        },
                    );
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
            Stmt::StreamDecl {
                source,
                ops,
                op_spans,
                ..
            } => {
                check_stream_source(v, source, span);
                check_stream_ops(v, ops, op_spans, source, span);
            }
            Stmt::ConnectorDecl {
                connector_type,
                params,
                ..
            } => {
                if !builtins::is_known_connector_type(connector_type) {
                    let known: Vec<&str> = builtins::KNOWN_CONNECTOR_TYPES.to_vec();
                    let suggestion = did_you_mean(connector_type, &known);
                    v.emit_with_hint(
                        Severity::Error,
                        span,
                        "E008",
                        format!("unknown connector type '{connector_type}'"),
                        format!(
                            "known types: {}{}",
                            builtins::KNOWN_CONNECTOR_TYPES.join(", "),
                            suggestion
                        ),
                    );
                } else {
                    check_connector_params(
                        v,
                        params,
                        connector_type,
                        ParamContext::Both,
                        "connector declaration",
                        span,
                    );
                }
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
                format!("cannot assign to immutable variable '{name}'{context}"),
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
                    format!("undefined connector '{connector_name}'"),
                    format!("declare it with: connector {connector_name} = type (...){suggestion}"),
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
    if !v.symbols.events.contains_key(name)
        && !v.symbols.streams.contains_key(name)
        && !v.symbols.patterns.contains_key(name)
    {
        let suggestion = did_you_mean(name, &v.symbols.source_names());
        v.emit_with_hint(
            Severity::Error,
            span,
            "E033",
            format!("undefined event type or stream '{name}'"),
            format!("declare it with: event {name} {{ ... }} or stream {name} = ...{suggestion}"),
        );
    }
}

// ---------------------------------------------------------------------------
// Stream operations checks
// ---------------------------------------------------------------------------

fn check_stream_ops(
    v: &mut Validator,
    ops: &[StreamOp],
    op_spans: &[Span],
    source: &StreamSource,
    span: Span,
) {
    let mut seen_aggregate = false;
    let mut seen_window = false;
    let mut in_sequence = is_sequence_source(source);
    // Build alias → event type mapping for field reference validation
    let mut alias_to_event: HashMap<String, String> = HashMap::new();
    match source {
        StreamSource::Ident(name) => {
            // Direct source: bare name can be used as qualifier
            if v.symbols.events.contains_key(name) {
                alias_to_event.insert(name.clone(), name.clone());
            }
        }
        StreamSource::IdentWithAlias { name, alias } => {
            alias_to_event.insert(alias.clone(), name.clone());
        }
        StreamSource::AllWithAlias { name, alias } => {
            if let Some(a) = alias {
                alias_to_event.insert(a.clone(), name.clone());
            } else {
                alias_to_event.insert(name.clone(), name.clone());
            }
        }
        StreamSource::FromConnector { event_type, .. } => {
            alias_to_event.insert(event_type.clone(), event_type.clone());
        }
        StreamSource::Merge(inline_streams) => {
            for s in inline_streams {
                // source is the event type name; name is the inline stream alias
                alias_to_event.insert(s.name.clone(), s.source.clone());
                // Also add source as bare name for unaliased lookups
                if s.name != s.source {
                    alias_to_event.insert(s.source.clone(), s.source.clone());
                }
            }
        }
        StreamSource::Join(clauses) => {
            for c in clauses {
                alias_to_event.insert(c.source.clone(), c.source.clone());
            }
        }
        StreamSource::Sequence(seq) => {
            for step in &seq.steps {
                alias_to_event.insert(step.alias.clone(), step.event_type.clone());
            }
        }
        _ => {}
    }
    for op in ops {
        if let StreamOp::FollowedBy(clause) = op {
            if let Some(alias) = &clause.alias {
                alias_to_event.insert(alias.clone(), clause.event_type.clone());
            } else {
                alias_to_event.insert(clause.event_type.clone(), clause.event_type.clone());
            }
        }
    }

    // Collect valid bare field names from all source event types
    let mut bare_fields: Vec<String> = Vec::new();
    for event_name in alias_to_event.values() {
        if let Some(fields) = v.symbols.event_field_names(event_name) {
            for f in fields {
                if !bare_fields.contains(f) {
                    bare_fields.push(f.clone());
                }
            }
        }
    }

    // Track which built-in variables are available based on ops
    let has_forecast = ops.iter().any(|op| matches!(op, StreamOp::Forecast(_)));
    let has_enrich = ops.iter().any(|op| matches!(op, StreamOp::Enrich(_)));

    for (op_idx, op) in ops.iter().enumerate() {
        // Use per-operation op_span if available, fall back to stream declaration op_span
        let op_span = op_spans.get(op_idx).copied().unwrap_or(span);
        match op {
            // --- Unimplemented operations (E090) ---
            StreamOp::Map(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".map() is not implemented".to_string(),
                    "use .select() with expressions instead".to_string(),
                );
            }
            StreamOp::Filter(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".filter() is not implemented".to_string(),
                    "use .where() instead".to_string(),
                );
            }
            StreamOp::Concurrent(ref args) => {
                // Validate parameters
                for arg in args {
                    if !crate::validate::builtins::CONCURRENT_PARAMS.contains(&arg.name.as_str()) {
                        v.emit(
                            Severity::Error,
                            op_span,
                            "E091",
                            format!(
                                ".concurrent() unknown parameter '{}'; expected one of: {}",
                                arg.name,
                                crate::validate::builtins::CONCURRENT_PARAMS.join(", ")
                            ),
                        );
                    }
                }

                // Validate workers value if present
                for arg in args {
                    if arg.name == "workers" {
                        if let crate::ast::Expr::Int(n) = &arg.value {
                            if *n < 1 || *n > 128 {
                                v.emit(
                                    Severity::Error,
                                    op_span,
                                    "E091",
                                    format!(
                                        ".concurrent(workers: {n}) out of range; must be 1–128"
                                    ),
                                );
                            }
                        }
                    }
                }

                // Check that no stateful ops follow .concurrent()
                let mut found_concurrent = false;
                for sop in ops {
                    if std::ptr::eq(sop, op) {
                        found_concurrent = true;
                        continue;
                    }
                    if found_concurrent {
                        let op_name = match sop {
                            StreamOp::Window { .. } => Some("Window"),
                            StreamOp::Aggregate(_) => Some("Aggregate"),
                            StreamOp::FollowedBy(_) => Some("Sequence"),
                            StreamOp::Forecast(_) => Some("Forecast"),
                            StreamOp::TrendAggregate(_) => Some("TrendAggregate"),
                            StreamOp::Distinct(_) => Some("Distinct"),
                            _ => None,
                        };
                        if let Some(name) = op_name {
                            v.emit(
                                Severity::Error,
                                op_span,
                                "E091",
                                format!(
                                    ".concurrent() cannot be followed by stateful .{}() operator",
                                    name.to_lowercase()
                                ),
                            );
                            break;
                        }
                    }
                }
            }
            StreamOp::OnError(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".on_error() is not yet implemented".to_string(),
                    "handle errors in your .where() or .select() logic".to_string(),
                );
            }
            StreamOp::Collect => {
                v.emit(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".collect() is not yet implemented".to_string(),
                );
            }
            StreamOp::Fork(_) => {
                v.emit(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".fork() is not yet implemented".to_string(),
                );
            }
            StreamOp::Any(_) => {
                v.emit(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".any() is not yet implemented".to_string(),
                );
            }
            StreamOp::All => {
                v.emit(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".all() is not yet implemented".to_string(),
                );
            }
            StreamOp::First => {
                v.emit(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".first() is not yet implemented".to_string(),
                );
            }
            StreamOp::Distinct(_) => {
                v.emit(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".distinct() is not yet implemented".to_string(),
                );
            }
            StreamOp::OrderBy(_) => {
                v.emit(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".order_by() is not yet implemented".to_string(),
                );
            }
            StreamOp::Limit(_) => {
                v.emit(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".limit() is not yet implemented".to_string(),
                );
            }
            StreamOp::ToExpr(_) => {
                v.emit_with_hint(
                    Severity::Error,
                    op_span,
                    "E090",
                    ".to(expr) is not supported".to_string(),
                    "use .to(ConnectorName, ...) with a declared connector".to_string(),
                );
            }

            // --- Operation ordering ---
            StreamOp::Having(expr) => {
                if !seen_aggregate {
                    v.emit_with_hint(
                        Severity::Error,
                        op_span,
                        "E010",
                        ".having() used without a prior .aggregate()".to_string(),
                        "add .aggregate(...) before .having()".to_string(),
                    );
                }
                check_boolean_expr(v, expr, ".having()", op_span);
                check_expr_field_refs(v, expr, &alias_to_event, op_span);
            }
            StreamOp::Aggregate(items) => {
                if seen_aggregate {
                    v.emit(
                        Severity::Error,
                        op_span,
                        "E011",
                        "duplicate .aggregate() — only one aggregation per stream is allowed"
                            .to_string(),
                    );
                }
                if !seen_window {
                    v.emit_with_hint(
                        Severity::Warning,
                        op_span,
                        "W001",
                        ".aggregate() without a prior .window()".to_string(),
                        "results will accumulate indefinitely; add .window() for bounded aggregation".to_string(),
                    );
                }
                seen_aggregate = true;
                check_aggregate_items(v, items, op_span);
            }
            StreamOp::Window(_) => {
                if seen_window {
                    v.emit(
                        Severity::Error,
                        op_span,
                        "E012",
                        "duplicate .window() — only one window per stream is allowed".to_string(),
                    );
                }
                seen_window = true;
            }
            StreamOp::PartitionBy(expr) => {
                if seen_window {
                    v.emit_with_hint(
                        Severity::Warning,
                        op_span,
                        "W002",
                        ".partition_by() after .window() — partitioning should come before windowing".to_string(),
                        "move .partition_by() before .window() for correct behavior".to_string(),
                    );
                }
                check_bare_ident_refs(
                    v,
                    expr,
                    &bare_fields,
                    &alias_to_event,
                    has_forecast,
                    has_enrich,
                    ".partition_by()",
                    op_span,
                );
            }
            StreamOp::Within(expr) => {
                if !in_sequence {
                    v.emit_with_hint(
                        Severity::Error,
                        op_span,
                        "E020",
                        ".within() used outside a sequence context".to_string(),
                        ".within() requires a sequence source or -> (followed_by) operators"
                            .to_string(),
                    );
                }
                check_duration_expr(v, expr, ".within()", op_span);
            }

            // --- Sequence tracking ---
            StreamOp::FollowedBy(clause) | StreamOp::Not(clause) => {
                check_source_name(v, &clause.event_type, op_span);
                in_sequence = true;
            }

            // --- Parameter validation ---
            StreamOp::Log(args) => {
                check_named_params(v, args, LOG_PARAMS, ".log()", op_span);
            }
            StreamOp::Alert(args) => {
                check_named_params(v, args, ALERT_PARAMS, ".alert()", op_span);
            }
            StreamOp::Watermark(args) => {
                check_named_params(v, args, WATERMARK_PARAMS, ".watermark()", op_span);
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
                        op_span,
                        "E030",
                        format!("undefined connector '{connector_name}'"),
                        format!(
                            "declare it with: connector {connector_name} = type (...){suggestion}{avail_hint}"
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
                        op_span,
                    );
                }
            }
            StreamOp::Context(name) => {
                if !v.symbols.contexts.contains_key(name) {
                    let suggestion = did_you_mean(name, &v.symbols.context_names());
                    v.emit_with_hint(
                        Severity::Error,
                        op_span,
                        "E031",
                        format!("undefined context '{name}'"),
                        format!("declare it with: context {name} (cores: [0, 1]){suggestion}"),
                    );
                }
            }

            // --- Expression type checks ---
            StreamOp::Where(expr) => {
                check_boolean_expr(v, expr, ".where()", op_span);
                check_expr_field_refs(v, expr, &alias_to_event, op_span);
                check_bare_ident_refs(
                    v,
                    expr,
                    &bare_fields,
                    &alias_to_event,
                    has_forecast,
                    has_enrich,
                    ".where()",
                    op_span,
                );
            }
            StreamOp::AllowedLateness(expr) => {
                check_duration_expr(v, expr, ".allowed_lateness()", op_span);
            }

            // --- Emit field validation ---
            StreamOp::Emit {
                output_type,
                fields,
                ..
            } => {
                for field in fields {
                    check_expr_field_refs(v, &field.value, &alias_to_event, op_span);
                }
                if let Some(type_name) = output_type {
                    if let Some(event_info) = v.symbols.events.get(type_name) {
                        // Validate that emitted type is a known event
                        let _ = &event_info.field_names; // field_names used for future field-level validation
                    } else if !v.symbols.is_declared(type_name) {
                        let suggestion = did_you_mean(type_name, &v.symbols.all_names());
                        v.emit_with_hint(
                            Severity::Error,
                            op_span,
                            "E034",
                            format!(".emit as '{type_name}' references an undeclared type"),
                            format!("declare it with: event {type_name} {{ ... }}{suggestion}"),
                        );
                    }
                }
            }

            // --- Enrich connector validation ---
            StreamOp::Enrich(spec) => {
                if !v.symbols.connectors.contains_key(&spec.connector_name) {
                    let suggestion =
                        did_you_mean(&spec.connector_name, &v.symbols.connector_names());
                    v.emit_with_hint(
                        Severity::Error,
                        op_span,
                        "E030",
                        format!("undefined connector '{}'", spec.connector_name),
                        format!(
                            "declare it with: connector {} = type (...){}",
                            spec.connector_name, suggestion
                        ),
                    );
                } else {
                    let connector_type = &v.symbols.connectors[&spec.connector_name].connector_type;
                    if !builtins::ENRICH_COMPATIBLE_TYPES.contains(&connector_type.as_str()) {
                        v.emit_with_hint(
                            Severity::Error,
                            op_span,
                            "E032",
                            format!(
                                ".enrich() is not compatible with '{}' connector type '{}'",
                                spec.connector_name, connector_type
                            ),
                            format!(
                                ".enrich() requires a request-response connector ({})",
                                builtins::ENRICH_COMPATIBLE_TYPES.join(", ")
                            ),
                        );
                    }
                }
                if spec.fields.is_empty() {
                    v.emit_with_hint(
                        Severity::Warning,
                        op_span,
                        "W032",
                        ".enrich() has no fields specified".to_string(),
                        "add fields: [field1, field2] to extract data from the enrichment response"
                            .to_string(),
                    );
                }
            }

            // --- Operations that need no extra validation ---
            StreamOp::Tap(_)
            | StreamOp::Print(_)
            | StreamOp::Select(_)
            | StreamOp::Pattern(_)
            | StreamOp::Process(_)
            | StreamOp::On(_)
            | StreamOp::TrendAggregate(_)
            | StreamOp::Score(_)
            | StreamOp::Forecast(_) => {}
        }
    }
}

const fn is_sequence_source(source: &StreamSource) -> bool {
    matches!(source, StreamSource::Sequence(_))
}

// ---------------------------------------------------------------------------
// Field reference validation
// ---------------------------------------------------------------------------

/// Collect a member chain from nested `Expr::Member` expressions.
/// Returns `Some(("alias", ["field1", "field2", ...]))` for `alias.field1.field2`.
fn collect_member_chain(expr: &Expr) -> Option<(String, Vec<String>)> {
    match expr {
        Expr::Member {
            expr: inner,
            member,
        } => {
            if let Expr::Ident(root) = inner.as_ref() {
                Some((root.clone(), vec![member.clone()]))
            } else if let Some((root, mut chain)) = collect_member_chain(inner) {
                chain.push(member.clone());
                Some((root, chain))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Walk an expression tree and warn about references to undeclared fields on known events.
fn check_expr_field_refs(
    v: &mut Validator,
    expr: &Expr,
    alias_to_event: &HashMap<String, String>,
    span: Span,
) {
    match expr {
        Expr::Member {
            expr: inner,
            member,
        } => {
            // Try to resolve the full member chain (e.g., alias.customer.address.city)
            if let Some((root, chain)) = collect_member_chain(expr) {
                if let Some(event_name) = alias_to_event.get(&root) {
                    // Validate the first field against the event
                    if let Some(fields) = v.symbols.event_field_names(event_name) {
                        if !fields.is_empty() && !fields.iter().any(|f| f == &chain[0]) {
                            let suggestion = did_you_mean(
                                &chain[0],
                                &fields.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                            );
                            v.emit_with_hint(
                                Severity::Warning,
                                span,
                                "W034",
                                format!(
                                    "reference to undeclared field '{}' on event '{event_name}'",
                                    chain[0]
                                ),
                                format!("declared fields: {}{}", fields.join(", "), suggestion),
                            );
                        }
                    }

                    // Validate deeper fields through struct type chain
                    // Find the type of the first field from the event declaration
                    let mut current_type_name: Option<String> = None;
                    if let Some(event_info) = v.symbols.events.get(event_name) {
                        // We only have field names in EventInfo, not types.
                        // Check if the event also has a matching struct type.
                        let _ = event_info;
                    }
                    // Also check if the event name itself is used as a type with fields
                    if let Some(type_info) = v.symbols.types.get(event_name) {
                        for (fname, fty) in &type_info.fields {
                            if fname == &chain[0] {
                                if let crate::types::Type::Named(ref n) = fty {
                                    current_type_name = Some(n.clone());
                                }
                                break;
                            }
                        }
                    }

                    // Walk remaining chain through struct types
                    for field in chain.iter().skip(1) {
                        let type_name = match current_type_name.take() {
                            Some(n) => n,
                            None => break,
                        };
                        // Clone the fields we need so we can call v.emit_with_hint later
                        let resolved = v.symbols.types.get(&type_name).map(|ti| {
                            let names: Vec<String> =
                                ti.fields.iter().map(|(n, _)| n.clone()).collect();
                            let next = ti.fields.iter().find_map(|(n, t)| {
                                if n == field {
                                    if let crate::types::Type::Named(ref tn) = t {
                                        Some(tn.clone())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            });
                            (names, next)
                        });
                        match resolved {
                            Some((type_fields, next_type)) => {
                                if !type_fields.is_empty()
                                    && !type_fields.iter().any(|f| f == field)
                                {
                                    let suggestion = did_you_mean(
                                        field,
                                        &type_fields.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                                    );
                                    v.emit_with_hint(
                                        Severity::Warning,
                                        span,
                                        "W034",
                                        format!(
                                            "reference to undeclared field '{field}' on type '{type_name}'"
                                        ),
                                        format!(
                                            "declared fields: {}{}",
                                            type_fields.join(", "),
                                            suggestion
                                        ),
                                    );
                                }
                                current_type_name = next_type;
                            }
                            None => break, // Type not found — skip
                        }
                    }
                }
            } else if let Expr::Ident(name) = inner.as_ref() {
                // Simple one-level access (fallback for non-chain expressions)
                if let Some(event_name) = alias_to_event.get(name) {
                    if let Some(fields) = v.symbols.event_field_names(event_name) {
                        if !fields.is_empty() && !fields.iter().any(|f| f == member) {
                            let suggestion = did_you_mean(
                                member,
                                &fields.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                            );
                            v.emit_with_hint(
                                Severity::Warning,
                                span,
                                "W034",
                                format!(
                                    "reference to undeclared field '{member}' on event '{event_name}'"
                                ),
                                format!("declared fields: {}{}", fields.join(", "), suggestion),
                            );
                        }
                    }
                }
            }
            // Also recurse into inner expression for nested checks
            check_expr_field_refs(v, inner, alias_to_event, span);
        }
        Expr::Binary { left, right, .. } => {
            check_expr_field_refs(v, left, alias_to_event, span);
            check_expr_field_refs(v, right, alias_to_event, span);
        }
        Expr::Unary { expr: inner, .. } => {
            check_expr_field_refs(v, inner, alias_to_event, span);
        }
        Expr::Call { func, args } => {
            check_expr_field_refs(v, func, alias_to_event, span);
            for arg in args {
                match arg {
                    Arg::Positional(e) | Arg::Named(_, e) => {
                        check_expr_field_refs(v, e, alias_to_event, span);
                    }
                }
            }
        }
        Expr::OptionalMember { expr: inner, .. } => {
            check_expr_field_refs(v, inner, alias_to_event, span);
        }
        Expr::Index { expr: e, index } => {
            check_expr_field_refs(v, e, alias_to_event, span);
            check_expr_field_refs(v, index, alias_to_event, span);
        }
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => {
            check_expr_field_refs(v, cond, alias_to_event, span);
            check_expr_field_refs(v, then_branch, alias_to_event, span);
            check_expr_field_refs(v, else_branch, alias_to_event, span);
        }
        Expr::Coalesce { expr: e, default } => {
            check_expr_field_refs(v, e, alias_to_event, span);
            check_expr_field_refs(v, default, alias_to_event, span);
        }
        Expr::Array(elems) => {
            for e in elems {
                check_expr_field_refs(v, e, alias_to_event, span);
            }
        }
        // Leaves — no recursion needed
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Bare identifier validation
// ---------------------------------------------------------------------------

/// Walk an expression and warn about bare identifiers that don't match any
/// known event field, alias, variable, or built-in variable.
///
/// This catches typos like `.where(temprature > 30)` when the field is `temperature`.
#[allow(clippy::too_many_arguments)]
fn check_bare_ident_refs(
    v: &mut Validator,
    expr: &Expr,
    bare_fields: &[String],
    alias_to_event: &HashMap<String, String>,
    has_forecast: bool,
    has_enrich: bool,
    context: &str,
    span: Span,
) {
    // Skip validation when we have no field information (can't tell valid from invalid)
    if bare_fields.is_empty() && !has_forecast && !has_enrich {
        return;
    }

    match expr {
        Expr::Ident(name) => {
            // Skip if it's a known alias (e.g., `a` in `a.field`)
            if alias_to_event.contains_key(name) {
                return;
            }
            // Skip if it's a known bare event field
            if bare_fields.iter().any(|f| f == name) {
                return;
            }
            // Skip if it's a declared variable/constant
            if v.symbols.variables.contains_key(name) {
                return;
            }
            // Skip if it's a built-in function name
            if builtins::is_known_function(name) {
                return;
            }
            // Skip boolean literals and common constants
            if matches!(name.as_str(), "true" | "false" | "null") {
                return;
            }
            // Skip forecast built-in variables
            if has_forecast && builtins::FORECAST_BUILTIN_VARS.contains(&name.as_str()) {
                return;
            }
            // Skip enrich built-in variables
            if has_enrich && builtins::ENRICH_BUILTIN_VARS.contains(&name.as_str()) {
                return;
            }
            // Unknown identifier — likely a typo
            let mut candidates: Vec<&str> = bare_fields.iter().map(|s| s.as_str()).collect();
            for alias in alias_to_event.keys() {
                candidates.push(alias);
            }
            if has_forecast {
                candidates.extend(builtins::FORECAST_BUILTIN_VARS);
            }
            if has_enrich {
                candidates.extend(builtins::ENRICH_BUILTIN_VARS);
            }
            let suggestion = did_you_mean(name, &candidates);
            v.emit_with_hint(
                Severity::Warning,
                span,
                "W035",
                format!("unknown field '{name}' in {context}"),
                format!("available fields: {}{}", candidates.join(", "), suggestion),
            );
        }
        // Skip member expressions — handled by check_expr_field_refs
        Expr::Member { .. } => {}
        // Recurse into sub-expressions
        Expr::Binary { left, right, .. } => {
            check_bare_ident_refs(
                v,
                left,
                bare_fields,
                alias_to_event,
                has_forecast,
                has_enrich,
                context,
                span,
            );
            check_bare_ident_refs(
                v,
                right,
                bare_fields,
                alias_to_event,
                has_forecast,
                has_enrich,
                context,
                span,
            );
        }
        Expr::Unary { expr: inner, .. } => {
            check_bare_ident_refs(
                v,
                inner,
                bare_fields,
                alias_to_event,
                has_forecast,
                has_enrich,
                context,
                span,
            );
        }
        Expr::Call { args, .. } => {
            // Don't recurse into func (it's a function name ident), only args
            for arg in args {
                match arg {
                    Arg::Positional(e) | Arg::Named(_, e) => {
                        check_bare_ident_refs(
                            v,
                            e,
                            bare_fields,
                            alias_to_event,
                            has_forecast,
                            has_enrich,
                            context,
                            span,
                        );
                    }
                }
            }
        }
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => {
            check_bare_ident_refs(
                v,
                cond,
                bare_fields,
                alias_to_event,
                has_forecast,
                has_enrich,
                context,
                span,
            );
            check_bare_ident_refs(
                v,
                then_branch,
                bare_fields,
                alias_to_event,
                has_forecast,
                has_enrich,
                context,
                span,
            );
            check_bare_ident_refs(
                v,
                else_branch,
                bare_fields,
                alias_to_event,
                has_forecast,
                has_enrich,
                context,
                span,
            );
        }
        Expr::Index { expr: e, index } => {
            check_bare_ident_refs(
                v,
                e,
                bare_fields,
                alias_to_event,
                has_forecast,
                has_enrich,
                context,
                span,
            );
            check_bare_ident_refs(
                v,
                index,
                bare_fields,
                alias_to_event,
                has_forecast,
                has_enrich,
                context,
                span,
            );
        }
        // Leaves and other nodes — no bare ident checking needed
        _ => {}
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
        // Bare identifier is not a boolean condition (except true/false)
        Expr::Ident(name) if !matches!(name.as_str(), "true" | "false") => {
            v.emit_with_hint(
                Severity::Warning,
                span,
                "W061",
                format!(
                    "{context} condition is a bare identifier '{name}', expected a boolean expression"
                ),
                format!(
                    "use a comparison like {name} > value or {name} == value"
                ),
            );
        }
        // Member access alone is not a boolean condition
        Expr::Member {
            expr: inner,
            member,
        } => {
            if let Expr::Ident(obj) = inner.as_ref() {
                v.emit_with_hint(
                    Severity::Warning,
                    span,
                    "W061",
                    format!(
                        "{context} condition is a field access '{obj}.{member}', expected a boolean expression"
                    ),
                    format!(
                        "use a comparison like {obj}.{member} > value or {obj}.{member} == value"
                    ),
                );
            }
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
        _ => {} // Bool literal, comparison, logical, call — all ok
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

const fn literal_type_name(expr: &Expr) -> &'static str {
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
                        crate::ast::ConfigValue::Str(_)
                            | crate::ast::ConfigValue::Ident(_)
                            | crate::ast::ConfigValue::Concat(_)
                    ),
                    builtins::ParamType::Int => {
                        matches!(param.value, crate::ast::ConfigValue::Int(_))
                    }
                    builtins::ParamType::Bool => {
                        matches!(param.value, crate::ast::ConfigValue::Bool(_))
                    }
                    builtins::ParamType::StrArray => {
                        matches!(param.value, crate::ast::ConfigValue::Array(_))
                    }
                };
                if !type_ok {
                    let expected = match def.param_type {
                        builtins::ParamType::Str => "string",
                        builtins::ParamType::Int => "integer",
                        builtins::ParamType::Bool => "boolean",
                        builtins::ParamType::StrArray => "array of strings",
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

    // Check for missing required parameters
    for def in schema.iter().filter(|p| p.required && p.valid_in(ctx)) {
        if !params.iter().any(|p| p.name == def.name) {
            v.emit_with_hint(
                Severity::Error,
                span,
                "E009",
                format!(
                    "missing required parameter '{}' for {} {}",
                    def.name, connector_type, op_name
                ),
                def.description.to_string(),
            );
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
                            format!("aggregate function '{func_name}' requires a field argument"),
                            format!("usage: {func_name}(field_name)"),
                        );
                    }

                    // Check functions that require two arguments
                    if AGGREGATE_REQUIRES_TWO_ARGS.contains(&func_name.as_str()) && args.len() < 2 {
                        v.emit_with_hint(
                            Severity::Error,
                            span,
                            "E072",
                            format!(
                                "aggregate function '{func_name}' requires two arguments: field and period"
                            ),
                            format!("usage: {func_name}(field_name, period)"),
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
                        "bare field reference '{name}' in aggregate without an aggregate function"
                    ),
                    format!(
                        "wrap in an aggregate function, e.g. last({name}), first({name}), or sum({name})"
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
        Some(s) => format!("did you mean '{s}'?"),
        None => "check the function name or declare it with fn".to_string(),
    };
    v.emit_with_hint(
        Severity::Error,
        span,
        "E050",
        format!("unknown function '{name}'"),
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
