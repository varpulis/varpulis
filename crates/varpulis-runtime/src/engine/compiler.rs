//! Compilation functions for the Varpulis engine
//!
//! This module contains functions for converting VPL AST elements into
//! runtime structures (aggregators, SASE+ patterns, sequence filters).

use std::time::Duration;

use tracing::warn;
use varpulis_core::ast::{FollowedByClause, SequenceStepDecl, StreamSource};

use crate::aggregation::{
    AggBinOp, Avg, Count, CountDistinct, Ema, ExprAggregate, First, Last, Max, Median, Min,
    Percentile, StdDev, Sum, P50, P95, P99,
};
use crate::sase::{CompareOp, Predicate, SasePattern};

/// Compile an aggregate expression into an AggregateFunc
pub fn compile_agg_expr(
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

            // Extract second argument as int (period for EMA) or float (quantile for percentile)
            let second_int = args
                .get(1)
                .and_then(|a| match a {
                    Arg::Positional(Expr::Int(n)) => Some(*n as usize),
                    _ => None,
                })
                .unwrap_or(12);

            let second_float = args.get(1).and_then(|a| match a {
                Arg::Positional(Expr::Float(f)) => Some(*f),
                _ => None,
            });

            let agg_func: Box<dyn crate::aggregation::AggregateFunc> = match func_name.as_str() {
                "count" => Box::new(Count),
                "sum" => Box::new(Sum),
                "avg" => Box::new(Avg),
                "min" => Box::new(Min),
                "max" => Box::new(Max),
                "last" => Box::new(Last),
                "first" => Box::new(First),
                "stddev" => Box::new(StdDev),
                "ema" => Box::new(Ema::new(second_int)),
                "count_distinct" => Box::new(CountDistinct),
                "median" => Box::new(Median),
                "p50" => Box::new(P50),
                "p95" => Box::new(P95),
                "p99" => Box::new(P99),
                "percentile" => Box::new(Percentile::new(second_float.unwrap_or(0.5))),
                other => {
                    // Fallback: check UdfRegistry for custom aggregate UDFs
                    // (checked by compile_agg_expr_with_udfs; standard path logs warning)
                    warn!("Unknown aggregation function: {}", other);
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

            let (left_func, left_field) = compile_agg_expr(left)?;
            let (right_func, right_field) = compile_agg_expr(right)?;

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

/// Compile an aggregate expression, checking the UDF registry for custom aggregates.
///
/// Falls through to [`compile_agg_expr`] for built-in functions. When a name
/// is not recognized as a built-in but exists in the UDF registry as an aggregate,
/// it is wrapped in an adapter that delegates to the [`Accumulator`](crate::udf::Accumulator).
pub fn compile_agg_expr_with_udfs(
    expr: &varpulis_core::ast::Expr,
    udf_registry: &crate::udf::UdfRegistry,
) -> Option<(Box<dyn crate::aggregation::AggregateFunc>, Option<String>)> {
    // Try built-in first
    if let Some(result) = compile_agg_expr(expr) {
        return Some(result);
    }

    // Fallback: check UDF registry for custom aggregate
    use varpulis_core::ast::{Arg, Expr};
    if let Expr::Call { func, args } = expr {
        if let Expr::Ident(func_name) = func.as_ref() {
            if let Some(agg_udf) = udf_registry.get_aggregate(func_name) {
                let field = args.first().and_then(|a| match a {
                    Arg::Positional(Expr::Ident(s)) => Some(s.clone()),
                    _ => None,
                });

                let adapter = UdfAggregateAdapter {
                    udf: agg_udf.clone(),
                };
                return Some((Box::new(adapter), field));
            }
        }
    }

    None
}

/// Adapter that wraps a UDF [`Accumulator`](crate::udf::Accumulator) as an [`AggregateFunc`](crate::aggregation::AggregateFunc).
struct UdfAggregateAdapter {
    udf: std::sync::Arc<dyn crate::udf::AggregateUDF>,
}

impl crate::aggregation::AggregateFunc for UdfAggregateAdapter {
    fn name(&self) -> &'static str {
        "udf_aggregate"
    }

    fn apply(&self, events: &[crate::event::Event], field: Option<&str>) -> varpulis_core::Value {
        let mut acc = self.udf.init();
        let field_name = field.unwrap_or("value");
        for event in events {
            if let Some(val) = event.get(field_name) {
                acc.update(val);
            }
        }
        acc.finish()
    }
}

// =============================================================================
// SASE+ Pattern Compilation
// =============================================================================

/// Information about a derived stream for pattern compilation
#[derive(Debug, Clone)]
pub struct DerivedStreamInfo {
    /// The underlying event type (e.g., "Transaction")
    pub event_type: String,
    /// Optional filter expression from the stream definition
    pub filter: Option<varpulis_core::ast::Expr>,
}

/// Stream resolver function type: given a stream name, returns derived stream info if found
pub type StreamResolver<'a> = &'a dyn Fn(&str) -> Option<DerivedStreamInfo>;

/// Compile a sequence source and operations into a SASE+ pattern with stream resolution
pub fn compile_to_sase_pattern_with_resolver(
    source: &StreamSource,
    followed_by_clauses: &[FollowedByClause],
    _negation_clauses: &[FollowedByClause],
    within_duration: Option<Duration>,
    stream_resolver: StreamResolver,
) -> Option<SasePattern> {
    let mut steps: Vec<SasePattern> = Vec::new();

    // Handle source
    match source {
        StreamSource::Sequence(decl) => {
            // sequence() construct with explicit steps
            for step in &decl.steps {
                let pattern = compile_sequence_step_to_sase(step);
                steps.push(pattern);
            }
        }
        StreamSource::Ident(name) => {
            // Check if this is a derived stream
            let (event_type, predicate) = if let Some(info) = stream_resolver(name) {
                let pred = info.filter.as_ref().and_then(expr_to_sase_predicate);
                (info.event_type, pred)
            } else {
                (name.clone(), None)
            };
            steps.push(SasePattern::Event {
                event_type,
                predicate,
                alias: None,
            });
        }
        StreamSource::IdentWithAlias { name, alias } => {
            // Check if this is a derived stream
            let (event_type, predicate) = if let Some(info) = stream_resolver(name) {
                let pred = info.filter.as_ref().and_then(expr_to_sase_predicate);
                (info.event_type, pred)
            } else {
                (name.clone(), None)
            };
            steps.push(SasePattern::Event {
                event_type,
                predicate,
                alias: Some(alias.clone()),
            });
        }
        StreamSource::IdentWithFilterAndAlias {
            name,
            filter,
            alias,
        } => {
            // Inline filter becomes a SASE predicate
            let (event_type, mut predicate) = if let Some(info) = stream_resolver(name) {
                let pred = info.filter.as_ref().and_then(expr_to_sase_predicate);
                (info.event_type, pred)
            } else {
                (name.clone(), None)
            };
            // The inline filter takes precedence / merges with derived stream filter
            if let Some(inline_pred) = expr_to_sase_predicate(filter) {
                predicate = Some(inline_pred);
            }
            steps.push(SasePattern::Event {
                event_type,
                predicate,
                alias: alias.clone(),
            });
        }
        StreamSource::AllWithAlias { name, alias } => {
            // Check if this is a derived stream
            let (event_type, predicate) = if let Some(info) = stream_resolver(name) {
                let pred = info.filter.as_ref().and_then(expr_to_sase_predicate);
                (info.event_type, pred)
            } else {
                (name.clone(), None)
            };
            // match_all -> Kleene+
            let event_pattern = SasePattern::Event {
                event_type,
                predicate,
                alias: alias.clone(),
            };
            steps.push(SasePattern::KleenePlus(Box::new(event_pattern)));
        }
        _ => return None,
    }

    // Add followed_by clauses
    for clause in followed_by_clauses {
        // Check if event_type is a derived stream
        let (resolved_event_type, stream_predicate) =
            if let Some(info) = stream_resolver(&clause.event_type) {
                (info.event_type, info.filter)
            } else {
                (clause.event_type.clone(), None)
            };

        // Combine stream filter with clause filter
        let clause_predicate = clause.filter.as_ref().and_then(expr_to_sase_predicate);
        let stream_pred = stream_predicate.as_ref().and_then(expr_to_sase_predicate);

        let mut predicate = match (stream_pred, clause_predicate) {
            (Some(sp), Some(cp)) => Some(Predicate::And(Box::new(sp), Box::new(cp))),
            (Some(sp), None) => Some(sp),
            (None, Some(cp)) => Some(cp),
            (None, None) => None,
        };

        // Monotonic operators generate self-referencing CompareRef predicates
        if let Some(ref mono) = clause.monotonic {
            let alias = clause
                .alias
                .as_deref()
                .unwrap_or(&resolved_event_type)
                .to_string();
            let (field, op) = match mono {
                varpulis_core::ast::MonotonicOp::Increasing(f) => (f.clone(), CompareOp::Gt),
                varpulis_core::ast::MonotonicOp::Decreasing(f) => (f.clone(), CompareOp::Lt),
            };
            let mono_pred = Predicate::CompareRef {
                field: field.clone(),
                op,
                ref_alias: alias,
                ref_field: field,
            };
            predicate = Some(match predicate {
                Some(existing) => Predicate::And(Box::new(existing), Box::new(mono_pred)),
                None => mono_pred,
            });
        }

        let event_pattern = SasePattern::Event {
            event_type: resolved_event_type,
            predicate,
            alias: clause.alias.clone(),
        };

        // Handle match_all (or monotonic which implies match_all)
        let pattern = if clause.match_all || clause.monotonic.is_some() {
            SasePattern::KleenePlus(Box::new(event_pattern))
        } else {
            event_pattern
        };

        steps.push(pattern);
    }

    // Build the final pattern
    if steps.is_empty() {
        return None;
    }

    let pattern = if steps.len() == 1 {
        // Safe: we just checked steps is not empty
        steps.pop()?
    } else {
        SasePattern::Seq(steps)
    };

    // Apply within constraint if specified
    match within_duration {
        Some(duration) => Some(SasePattern::Within(Box::new(pattern), duration)),
        None => Some(pattern),
    }
}

/// Compile a sequence step declaration to a SASE pattern
fn compile_sequence_step_to_sase(step: &SequenceStepDecl) -> SasePattern {
    let predicate = step.filter.as_ref().and_then(expr_to_sase_predicate);

    SasePattern::Event {
        event_type: step.event_type.clone(),
        predicate,
        alias: Some(step.alias.clone()),
    }
}

/// Convert a VPL expression to a SASE predicate
pub fn expr_to_sase_predicate(expr: &varpulis_core::ast::Expr) -> Option<Predicate> {
    use varpulis_core::ast::{BinOp, Expr, UnaryOp};

    match expr {
        // Binary comparison: field == value
        Expr::Binary { op, left, right } => {
            let compare_op = match op {
                BinOp::Eq => Some(CompareOp::Eq),
                BinOp::NotEq => Some(CompareOp::NotEq),
                BinOp::Lt => Some(CompareOp::Lt),
                BinOp::Le => Some(CompareOp::Le),
                BinOp::Gt => Some(CompareOp::Gt),
                BinOp::Ge => Some(CompareOp::Ge),
                BinOp::And => {
                    let left_pred = expr_to_sase_predicate(left)?;
                    let right_pred = expr_to_sase_predicate(right)?;
                    return Some(Predicate::And(Box::new(left_pred), Box::new(right_pred)));
                }
                BinOp::Or => {
                    let left_pred = expr_to_sase_predicate(left)?;
                    let right_pred = expr_to_sase_predicate(right)?;
                    return Some(Predicate::Or(Box::new(left_pred), Box::new(right_pred)));
                }
                _ => None,
            }?;

            // Handle cross-event reference comparisons (e.g., order_id == order.id)
            // Left: current event field, Right: reference to captured event
            if let (
                Expr::Ident(field),
                Expr::Member {
                    expr: ref_expr,
                    member: ref_field,
                },
            ) = (left.as_ref(), right.as_ref())
            {
                if let Expr::Ident(ref_alias) = ref_expr.as_ref() {
                    return Some(Predicate::CompareRef {
                        field: field.clone(),
                        op: compare_op,
                        ref_alias: ref_alias.clone(),
                        ref_field: ref_field.clone(),
                    });
                }
            }

            // Extract field name from left side for simple comparisons
            let field = match left.as_ref() {
                Expr::Ident(name) => name.clone(),
                _ => {
                    // Fall back to runtime expression evaluation for complex left-side
                    return Some(Predicate::Expr(Box::new(expr.clone())));
                }
            };

            // Extract value from right side
            if let Some(value) = expr_to_value(right) {
                Some(Predicate::Compare {
                    field,
                    op: compare_op,
                    value,
                })
            } else {
                // Right side is complex (e.g., another field or expression)
                // Fall back to runtime expression evaluation
                Some(Predicate::Expr(Box::new(expr.clone())))
            }
        }

        // Unary not
        Expr::Unary {
            op: UnaryOp::Not,
            expr: inner,
        } => {
            let inner_pred = expr_to_sase_predicate(inner)?;
            Some(Predicate::Not(Box::new(inner_pred)))
        }

        // Fall back to storing the expression for runtime evaluation
        _ => Some(Predicate::Expr(Box::new(expr.clone()))),
    }
}

/// Compile a `SasePatternExpr` (from a named `pattern` declaration) into a runtime `SasePattern`.
pub fn compile_sase_pattern_expr(
    expr: &varpulis_core::ast::SasePatternExpr,
    within: Option<Duration>,
) -> Option<SasePattern> {
    use varpulis_core::ast::SasePatternExpr;

    let pattern = match expr {
        SasePatternExpr::Seq(items) => {
            let steps: Vec<SasePattern> = items.iter().map(compile_sase_pattern_item).collect();
            if steps.len() == 1 {
                steps.into_iter().next().unwrap()
            } else {
                SasePattern::Seq(steps)
            }
        }
        SasePatternExpr::And(left, right) => {
            let l = compile_sase_pattern_expr(left, None)?;
            let r = compile_sase_pattern_expr(right, None)?;
            SasePattern::And(Box::new(l), Box::new(r))
        }
        SasePatternExpr::Or(left, right) => {
            let l = compile_sase_pattern_expr(left, None)?;
            let r = compile_sase_pattern_expr(right, None)?;
            SasePattern::Or(Box::new(l), Box::new(r))
        }
        SasePatternExpr::Not(inner) => {
            let i = compile_sase_pattern_expr(inner, None)?;
            SasePattern::Not(Box::new(i))
        }
        SasePatternExpr::Event(name) => SasePattern::Event {
            event_type: name.clone(),
            predicate: None,
            alias: None,
        },
        SasePatternExpr::Group(inner) => {
            return compile_sase_pattern_expr(inner, within);
        }
    };

    // Wrap with Within if specified
    if let Some(duration) = within {
        Some(SasePattern::Within(Box::new(pattern), duration))
    } else {
        Some(pattern)
    }
}

/// Compile a single `SasePatternItem` to a `SasePattern`, handling Kleene operators.
fn compile_sase_pattern_item(item: &varpulis_core::ast::SasePatternItem) -> SasePattern {
    let mut predicate = item.filter.as_ref().and_then(expr_to_sase_predicate);

    // Monotonic operators generate self-referencing CompareRef predicates:
    //   .increasing(temp) as r → where temp > r.temp  (Kleene+)
    //   .decreasing(temp) as r → where temp < r.temp  (Kleene+)
    if let Some(ref mono) = item.monotonic {
        let alias = item
            .alias
            .as_deref()
            .unwrap_or(&item.event_type)
            .to_string();
        let (field, op) = match mono {
            varpulis_core::ast::MonotonicOp::Increasing(f) => (f.clone(), CompareOp::Gt),
            varpulis_core::ast::MonotonicOp::Decreasing(f) => (f.clone(), CompareOp::Lt),
        };
        let mono_pred = Predicate::CompareRef {
            field: field.clone(),
            op,
            ref_alias: alias,
            ref_field: field,
        };
        // Combine with existing predicate if any
        predicate = Some(match predicate {
            Some(existing) => Predicate::And(Box::new(existing), Box::new(mono_pred)),
            None => mono_pred,
        });
    }

    let base = SasePattern::Event {
        event_type: item.event_type.clone(),
        predicate,
        alias: item.alias.clone(),
    };

    match &item.kleene {
        Some(varpulis_core::ast::KleeneOp::Plus) => SasePattern::KleenePlus(Box::new(base)),
        Some(varpulis_core::ast::KleeneOp::Star) => SasePattern::KleeneStar(Box::new(base)),
        Some(varpulis_core::ast::KleeneOp::Optional) => {
            // Optional is equivalent to Or(base, empty match) — use KleeneStar for now
            SasePattern::KleeneStar(Box::new(base))
        }
        None => base,
    }
}

/// Extract all event type names from a `SasePatternExpr`.
pub fn extract_event_types_from_pattern_expr(
    expr: &varpulis_core::ast::SasePatternExpr,
) -> Vec<String> {
    use varpulis_core::ast::SasePatternExpr;

    let mut types = Vec::new();
    match expr {
        SasePatternExpr::Seq(items) => {
            for item in items {
                if !types.contains(&item.event_type) {
                    types.push(item.event_type.clone());
                }
            }
        }
        SasePatternExpr::And(left, right) | SasePatternExpr::Or(left, right) => {
            for t in extract_event_types_from_pattern_expr(left) {
                if !types.contains(&t) {
                    types.push(t);
                }
            }
            for t in extract_event_types_from_pattern_expr(right) {
                if !types.contains(&t) {
                    types.push(t);
                }
            }
        }
        SasePatternExpr::Not(inner) | SasePatternExpr::Group(inner) => {
            types = extract_event_types_from_pattern_expr(inner);
        }
        SasePatternExpr::Event(name) => {
            types.push(name.clone());
        }
    }
    types
}

/// Convert an AST expression to a Value (for predicates)
fn expr_to_value(expr: &varpulis_core::ast::Expr) -> Option<varpulis_core::Value> {
    use varpulis_core::ast::Expr;
    use varpulis_core::Value;

    match expr {
        Expr::Int(n) => Some(Value::Int(*n)),
        Expr::Float(f) => Some(Value::Float(*f)),
        Expr::Str(s) => Some(Value::Str(s.clone().into())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        _ => None,
    }
}
