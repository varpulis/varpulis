//! Compilation functions for the Varpulis engine
//!
//! This module contains functions for converting VPL AST elements into
//! runtime structures (aggregators, pattern expressions, sequence filters).

use crate::aggregation::{
    AggBinOp, Avg, Count, CountDistinct, Ema, ExprAggregate, First, Last, Max, Min, StdDev, Sum,
};
use crate::pattern::{PatternBuilder, PatternExpr};
use crate::sequence::{SequenceContext, SequenceFilter};
use crate::Event;
use tracing::warn;

use super::evaluator;

/// Compile a sequence filter expression into a runtime closure
pub fn compile_sequence_filter(expr: varpulis_core::ast::Expr) -> SequenceFilter {
    Box::new(move |event: &Event, ctx: &SequenceContext| {
        evaluator::eval_filter_expr(&expr, event, ctx)
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    })
}

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
