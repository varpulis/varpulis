//! Compilation functions for the Varpulis engine
//!
//! This module contains functions for converting VPL AST elements into
//! runtime structures (aggregators, SASE+ patterns, sequence filters).

use crate::aggregation::{
    AggBinOp, Avg, Count, CountDistinct, Ema, ExprAggregate, First, Last, Max, Min, StdDev, Sum,
};
use crate::pattern::{PatternBuilder, PatternExpr};
use crate::sase::{CompareOp, Predicate, SasePattern};
use std::time::Duration;
use tracing::warn;
use varpulis_core::ast::{FollowedByClause, SequenceStepDecl, StreamSource};

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

            // Extract period for EMA
            let period = args
                .get(1)
                .and_then(|a| match a {
                    Arg::Positional(Expr::Int(n)) => Some(*n as usize),
                    _ => None,
                })
                .unwrap_or(12);

            let agg_func: Box<dyn crate::aggregation::AggregateFunc> = match func_name.as_str() {
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

/// Convert an AST expression to a PatternExpr for the pattern engine
pub fn expr_to_pattern(expr: &varpulis_core::ast::Expr) -> Option<PatternExpr> {
    use varpulis_core::ast::{BinOp, Expr, UnaryOp};

    match expr {
        // Identifier = event type
        Expr::Ident(name) => Some(PatternBuilder::event(name)),

        // Binary operators: ->, and, or, xor
        Expr::Binary { op, left, right } => {
            let left_pattern = expr_to_pattern(left)?;
            let right_pattern = expr_to_pattern(right)?;

            match op {
                BinOp::FollowedBy => Some(PatternBuilder::followed_by(left_pattern, right_pattern)),
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
            let inner_pattern = expr_to_pattern(inner)?;
            Some(PatternBuilder::not(inner_pattern))
        }

        // Call expression: could be all(A) or within(A, 30s) or event with filter
        Expr::Call { func, args } => {
            if let Expr::Ident(func_name) = func.as_ref() {
                match func_name.as_str() {
                    "all" => {
                        if let Some(varpulis_core::ast::Arg::Positional(inner)) = args.first() {
                            let inner_pattern = expr_to_pattern(inner)?;
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
                                let inner_pattern = expr_to_pattern(inner)?;
                                if let Expr::Duration(ns) = dur_expr {
                                    let duration = std::time::Duration::from_nanos(*ns);
                                    return Some(PatternBuilder::within(inner_pattern, duration));
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
        StreamSource::Ident(name) | StreamSource::From(name) => {
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

        let predicate = match (stream_pred, clause_predicate) {
            (Some(sp), Some(cp)) => Some(Predicate::And(Box::new(sp), Box::new(cp))),
            (Some(sp), None) => Some(sp),
            (None, Some(cp)) => Some(cp),
            (None, None) => None,
        };

        let event_pattern = SasePattern::Event {
            event_type: resolved_event_type,
            predicate,
            alias: clause.alias.clone(),
        };

        // Handle match_all
        let pattern = if clause.match_all {
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

/// Convert an AST expression to a Value (for predicates)
fn expr_to_value(expr: &varpulis_core::ast::Expr) -> Option<varpulis_core::Value> {
    use varpulis_core::ast::Expr;
    use varpulis_core::Value;

    match expr {
        Expr::Int(n) => Some(Value::Int(*n)),
        Expr::Float(f) => Some(Value::Float(*f)),
        Expr::Str(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        _ => None,
    }
}
