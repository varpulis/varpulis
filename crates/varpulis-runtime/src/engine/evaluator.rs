//! Expression evaluation functions for the Varpulis engine
//!
//! This module contains all the expression evaluation logic used at runtime
//! to evaluate filter expressions, pattern expressions, and user-defined functions.

use crate::attention::AttentionWindow;
use crate::sequence::SequenceContext;
use crate::Event;
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::debug;
use varpulis_core::Value;

use super::UserFunction;

/// Evaluate a filter expression at runtime
pub fn eval_filter_expr(
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
            // Handle alias.field access (e.g., order.id, EMA12.symbol)
            if let Expr::Ident(alias) = object.as_ref() {
                // First check sequence context for captured events
                if let Some(captured) = ctx.get(alias.as_str()) {
                    return captured.get(member).cloned();
                }
                // Then check event data with prefixed field name (for join results)
                // Try both dot notation (alias.field) and underscore notation (alias_field)
                let prefixed_dot = format!("{}.{}", alias, member);
                if let Some(value) = event.get(&prefixed_dot) {
                    return Some(value.clone());
                }
                let prefixed_underscore = format!("{}_{}", alias, member);
                if let Some(value) = event.get(&prefixed_underscore) {
                    return Some(value.clone());
                }
                // Also try the member directly if alias matches event type
                if alias == &event.event_type {
                    return event.get(member).cloned();
                }
            }
            None
        }
        Expr::Binary { op, left, right } => {
            let left_val = eval_filter_expr(left, event, ctx)?;
            let right_val = eval_filter_expr(right, event, ctx)?;

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
                    (Value::Float(a), Value::Float(b)) if *b != 0.0 => Some(Value::Float(a / b)),
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
                        varpulis_core::ast::Arg::Positional(e) => eval_filter_expr(e, event, ctx),
                        varpulis_core::ast::Arg::Named(_, e) => eval_filter_expr(e, event, ctx),
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
            let val = eval_filter_expr(inner, event, ctx)?;
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
pub fn eval_expr_with_functions(
    expr: &varpulis_core::ast::Expr,
    event: &Event,
    ctx: &SequenceContext,
    functions: &HashMap<String, UserFunction>,
    bindings: &HashMap<String, Value>,
) -> Option<Value> {
    use varpulis_core::ast::{BinOp, Expr};
    use varpulis_core::Stmt;

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
        Expr::Member {
            expr: object,
            member,
        } => {
            // Handle alias.field access (e.g., order.id, MarketA.price)
            if let Expr::Ident(alias) = object.as_ref() {
                // First check sequence context for captured events
                if let Some(captured) = ctx.get(alias.as_str()) {
                    return captured.get(member).cloned();
                }
                // Check bindings for captured event
                if let Some(Value::Map(m)) = bindings.get(alias) {
                    if let Some(v) = m.get(member) {
                        return Some(v.clone());
                    }
                }
                // Then check event data with prefixed field name (for join results)
                // Try dot notation (alias.field) - this is how JoinBuffer stores fields
                let prefixed_dot = format!("{}.{}", alias, member);
                if let Some(value) = event.get(&prefixed_dot) {
                    return Some(value.clone());
                }
                // Try underscore notation (alias_field)
                let prefixed_underscore = format!("{}_{}", alias, member);
                if let Some(value) = event.get(&prefixed_underscore) {
                    return Some(value.clone());
                }
                // Also try the member directly if alias matches event type
                if alias == &event.event_type {
                    return event.get(member).cloned();
                }
            }
            None
        }
        Expr::Call { func, args } => {
            if let Expr::Ident(func_name) = func.as_ref() {
                // Evaluate arguments
                let arg_values: Vec<Value> = args
                    .iter()
                    .filter_map(|arg| match arg {
                        varpulis_core::ast::Arg::Positional(e) => {
                            eval_expr_with_functions(e, event, ctx, functions, bindings)
                        }
                        varpulis_core::ast::Arg::Named(_, e) => {
                            eval_expr_with_functions(e, event, ctx, functions, bindings)
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
                            return eval_expr_with_functions(
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
                    "min" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Int(*a.min(b))),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a.min(*b))),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Float((*a as f64).min(*b))),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a.min(*b as f64))),
                        _ => None,
                    },
                    "max" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                        (Value::Int(a), Value::Int(b)) => Some(Value::Int(*a.max(b))),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a.max(*b))),
                        (Value::Int(a), Value::Float(b)) => Some(Value::Float((*a as f64).max(*b))),
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a.max(*b as f64))),
                        _ => None,
                    },
                    _ => None,
                }
            } else {
                None
            }
        }
        Expr::Binary { op, left, right } => {
            let left_val = eval_expr_with_functions(left, event, ctx, functions, bindings)?;
            let right_val = eval_expr_with_functions(right, event, ctx, functions, bindings)?;

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
                    (Value::Float(a), Value::Float(b)) if *b != 0.0 => Some(Value::Float(a / b)),
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
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => {
            // Evaluate condition
            let cond_val = eval_expr_with_functions(cond, event, ctx, functions, bindings)?;
            if cond_val.as_bool().unwrap_or(false) {
                eval_expr_with_functions(then_branch, event, ctx, functions, bindings)
            } else {
                eval_expr_with_functions(else_branch, event, ctx, functions, bindings)
            }
        }
        _ => eval_filter_expr(expr, event, ctx),
    }
}

/// Evaluate a pattern expression with support for attention_score builtin
#[allow(clippy::only_used_in_recursion)]
pub fn eval_pattern_expr(
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
            eval_pattern_expr(body, events, ctx, functions, pattern_vars, attention_window)
        }

        // Block with let bindings
        Expr::Block { stmts, result } => {
            let mut local_vars = pattern_vars.clone();
            for (name, _ty, value_expr, _mutable) in stmts {
                if let Some(val) = eval_pattern_expr(
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
            eval_pattern_expr(
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
                            Arg::Positional(e) => {
                                eval_pattern_expr(e, events, ctx, functions, pattern_vars, Some(aw))
                            }
                            _ => None,
                        });
                        let e2_val = args.get(1).and_then(|a| match a {
                            Arg::Positional(e) => {
                                eval_pattern_expr(e, events, ctx, functions, pattern_vars, Some(aw))
                            }
                            _ => None,
                        });

                        if let (Some(Value::Map(m1)), Some(Value::Map(m2))) = (e1_val, e2_val) {
                            // Convert maps back to events for attention scoring
                            let ev1 = map_to_event(&m1);
                            let ev2 = map_to_event(&m2);
                            let score = aw.attention_score(&ev1, &ev2);
                            return Some(Value::Float(score as f64));
                        }
                    }
                    return Some(Value::Float(0.0));
                }

                // len() on arrays
                if func_name == "len" {
                    if let Some(Arg::Positional(arr_expr)) = args.first() {
                        if let Some(Value::Array(arr)) = eval_pattern_expr(
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

                // first() - get first element of array
                if func_name == "first" {
                    if let Some(Arg::Positional(arr_expr)) = args.first() {
                        if let Some(Value::Array(arr)) = eval_pattern_expr(
                            arr_expr,
                            events,
                            ctx,
                            functions,
                            pattern_vars,
                            attention_window,
                        ) {
                            return arr.first().cloned();
                        }
                    }
                }

                // last() - get last element of array
                if func_name == "last" {
                    if let Some(Arg::Positional(arr_expr)) = args.first() {
                        if let Some(Value::Array(arr)) = eval_pattern_expr(
                            arr_expr,
                            events,
                            ctx,
                            functions,
                            pattern_vars,
                            attention_window,
                        ) {
                            return arr.last().cloned();
                        }
                    }
                }

                // avg() - average of array
                if func_name == "avg" {
                    if let Some(Arg::Positional(arr_expr)) = args.first() {
                        if let Some(Value::Array(arr)) = eval_pattern_expr(
                            arr_expr,
                            events,
                            ctx,
                            functions,
                            pattern_vars,
                            attention_window,
                        ) {
                            let nums: Vec<f64> = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .collect();
                            if nums.is_empty() {
                                return Some(Value::Float(0.0));
                            }
                            let avg = nums.iter().sum::<f64>() / nums.len() as f64;
                            return Some(Value::Float(avg));
                        }
                    }
                }

                // variance() - variance of array
                if func_name == "variance" {
                    if let Some(Arg::Positional(arr_expr)) = args.first() {
                        if let Some(Value::Array(arr)) = eval_pattern_expr(
                            arr_expr,
                            events,
                            ctx,
                            functions,
                            pattern_vars,
                            attention_window,
                        ) {
                            let nums: Vec<f64> = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .collect();
                            if nums.is_empty() {
                                return Some(Value::Float(0.0));
                            }
                            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
                            let variance = nums.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                                / nums.len() as f64;
                            return Some(Value::Float(variance));
                        }
                    }
                }

                // sum() - sum of array
                if func_name == "sum" {
                    if let Some(Arg::Positional(arr_expr)) = args.first() {
                        if let Some(Value::Array(arr)) = eval_pattern_expr(
                            arr_expr,
                            events,
                            ctx,
                            functions,
                            pattern_vars,
                            attention_window,
                        ) {
                            let sum: f64 = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .sum();
                            return Some(Value::Float(sum));
                        }
                    }
                }

                // min() - minimum of array
                if func_name == "min" {
                    if let Some(Arg::Positional(arr_expr)) = args.first() {
                        if let Some(Value::Array(arr)) = eval_pattern_expr(
                            arr_expr,
                            events,
                            ctx,
                            functions,
                            pattern_vars,
                            attention_window,
                        ) {
                            let min = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .fold(f64::INFINITY, f64::min);
                            if min.is_infinite() {
                                return None;
                            }
                            return Some(Value::Float(min));
                        }
                    }
                }

                // max() - maximum of array
                if func_name == "max" {
                    if let Some(Arg::Positional(arr_expr)) = args.first() {
                        if let Some(Value::Array(arr)) = eval_pattern_expr(
                            arr_expr,
                            events,
                            ctx,
                            functions,
                            pattern_vars,
                            attention_window,
                        ) {
                            let max = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .fold(f64::NEG_INFINITY, f64::max);
                            if max.is_infinite() {
                                return None;
                            }
                            return Some(Value::Float(max));
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
                let receiver_val = eval_pattern_expr(
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
                                        eval_pattern_expr(
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
                                let mapped: Vec<Value> = arr
                                    .into_iter()
                                    .filter_map(|item| {
                                        let mut local = pattern_vars.clone();

                                        // Handle multi-param lambdas for sliding_pairs: (e1, e2) => ...
                                        if params.len() >= 2 {
                                            if let Value::Array(pair) = &item {
                                                if pair.len() >= 2 {
                                                    local
                                                        .insert(params[0].clone(), pair[0].clone());
                                                    local
                                                        .insert(params[1].clone(), pair[1].clone());
                                                } else {
                                                    return None;
                                                }
                                            } else {
                                                return None;
                                            }
                                        } else {
                                            // Single param lambda
                                            let param_name = params
                                                .first()
                                                .cloned()
                                                .unwrap_or_else(|| "x".to_string());
                                            local.insert(param_name, item);
                                        }

                                        eval_pattern_expr(
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
                    "len" | "count" => {
                        if let Value::Array(arr) = receiver_val {
                            return Some(Value::Int(arr.len() as i64));
                        }
                    }
                    "sliding_pairs" => {
                        // Generate pairs of consecutive elements: [a, b, c] -> [[a, b], [b, c]]
                        if let Value::Array(arr) = receiver_val {
                            if arr.len() < 2 {
                                return Some(Value::Array(vec![]));
                            }
                            let pairs: Vec<Value> = arr
                                .windows(2)
                                .map(|pair| Value::Array(pair.to_vec()))
                                .collect();
                            return Some(Value::Array(pairs));
                        }
                    }
                    "first" => {
                        if let Value::Array(arr) = receiver_val {
                            return arr.first().cloned();
                        }
                    }
                    "last" => {
                        if let Value::Array(arr) = receiver_val {
                            return arr.last().cloned();
                        }
                    }
                    "sum" => {
                        if let Value::Array(arr) = receiver_val {
                            let sum: f64 = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .sum();
                            return Some(Value::Float(sum));
                        }
                    }
                    "avg" => {
                        if let Value::Array(arr) = receiver_val {
                            let nums: Vec<f64> = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .collect();
                            if nums.is_empty() {
                                return Some(Value::Float(0.0));
                            }
                            let avg = nums.iter().sum::<f64>() / nums.len() as f64;
                            return Some(Value::Float(avg));
                        }
                    }
                    "min" => {
                        if let Value::Array(arr) = receiver_val {
                            let min = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .fold(f64::INFINITY, f64::min);
                            if min.is_infinite() {
                                return None;
                            }
                            return Some(Value::Float(min));
                        }
                    }
                    "max" => {
                        if let Value::Array(arr) = receiver_val {
                            let max = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .fold(f64::NEG_INFINITY, f64::max);
                            if max.is_infinite() {
                                return None;
                            }
                            return Some(Value::Float(max));
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
            let recv_val = eval_pattern_expr(
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
            let l =
                eval_pattern_expr(left, events, ctx, functions, pattern_vars, attention_window)?;
            let r = eval_pattern_expr(
                right,
                events,
                ctx,
                functions,
                pattern_vars,
                attention_window,
            )?;
            eval_binary_op(op, &l, &r)
        }

        _ => None,
    }
}

/// Convert a Value::Map back to an Event for attention scoring
pub fn map_to_event(map: &IndexMap<String, Value>) -> Event {
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
pub fn eval_binary_op(
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
