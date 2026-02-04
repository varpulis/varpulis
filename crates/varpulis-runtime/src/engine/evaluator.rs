//! Expression evaluation functions for the Varpulis engine
//!
//! This module contains all the expression evaluation logic used at runtime
//! to evaluate filter expressions, pattern expressions, and user-defined functions.
//!
//! ## Imperative Programming Support
//!
//! This module supports full imperative programming including:
//! - For loops: `for x in items { ... }`
//! - While loops: `while condition { ... }`
//! - If/elif/else statements
//! - Break/Continue for loop control
//! - Return statements for early exit
//! - Variable declarations and assignments

use crate::attention::AttentionWindow;
use crate::sequence::SequenceContext;
use crate::Event;
use indexmap::IndexMap;
use std::cell::RefCell;
use std::collections::HashMap;
use tracing::debug;
use varpulis_core::span::Spanned;
use varpulis_core::{Stmt, Value};

use super::UserFunction;

// =============================================================================
// Thread-local emit collector
// =============================================================================

thread_local! {
    static EMIT_COLLECTOR: RefCell<Option<Vec<Event>>> = const { RefCell::new(None) };
}

/// Install an emit collector, run `f`, return (result, emitted_events).
/// Supports nested calls (saves/restores previous collector).
pub fn with_emit_collector<F, R>(f: F) -> (R, Vec<Event>)
where
    F: FnOnce() -> R,
{
    EMIT_COLLECTOR.with(|c| {
        let prev = c.borrow_mut().take();
        *c.borrow_mut() = Some(Vec::new());
        let result = f();
        let events = c.borrow_mut().take().unwrap_or_default();
        *c.borrow_mut() = prev;
        (result, events)
    })
}

/// Push an event to the active emit collector (no-op if none installed).
fn collect_emitted_event(event: Event) {
    EMIT_COLLECTOR.with(|c| {
        if let Some(ref mut v) = *c.borrow_mut() {
            v.push(event);
        }
    });
}

// =============================================================================
// Control Flow
// =============================================================================

/// Result of statement execution, supporting control flow
#[derive(Debug, Clone)]
pub enum StmtResult {
    /// Continue to next statement
    Continue,
    /// Break out of loop
    Break,
    /// Continue to next iteration
    ContinueLoop,
    /// Return from function with optional value
    Return(Option<Value>),
}

// =============================================================================
// Statement Evaluation
// =============================================================================

/// Evaluate a sequence of statements, returning the final value or control flow result
pub fn eval_stmts(
    stmts: &[Spanned<Stmt>],
    event: &Event,
    ctx: &SequenceContext,
    functions: &HashMap<String, UserFunction>,
    bindings: &mut HashMap<String, Value>,
) -> (StmtResult, Option<Value>) {
    let mut last_value = None;

    for stmt in stmts {
        let (result, value) = eval_stmt(&stmt.node, event, ctx, functions, bindings);
        last_value = value;

        match result {
            StmtResult::Continue => {}
            StmtResult::Break | StmtResult::ContinueLoop | StmtResult::Return(_) => {
                return (result, last_value);
            }
        }
    }

    (StmtResult::Continue, last_value)
}

/// Evaluate a single statement
pub fn eval_stmt(
    stmt: &Stmt,
    event: &Event,
    ctx: &SequenceContext,
    functions: &HashMap<String, UserFunction>,
    bindings: &mut HashMap<String, Value>,
) -> (StmtResult, Option<Value>) {
    match stmt {
        // Expression statement - evaluate and return value
        Stmt::Expr(expr) => {
            let value = eval_expr_with_functions(expr, event, ctx, functions, bindings);
            (StmtResult::Continue, value)
        }

        // Variable declaration
        Stmt::VarDecl {
            mutable: _,
            name,
            ty: _,
            value,
        } => {
            let val = eval_expr_with_functions(value, event, ctx, functions, bindings);
            if let Some(v) = val {
                bindings.insert(name.clone(), v);
            }
            (StmtResult::Continue, None)
        }

        // Constant declaration (treated as immutable variable)
        Stmt::ConstDecl { name, ty: _, value } => {
            let val = eval_expr_with_functions(value, event, ctx, functions, bindings);
            if let Some(v) = val {
                bindings.insert(name.clone(), v);
            }
            (StmtResult::Continue, None)
        }

        // Variable assignment
        Stmt::Assignment { name, value } => {
            let val = eval_expr_with_functions(value, event, ctx, functions, bindings);
            if let Some(v) = val {
                bindings.insert(name.clone(), v);
            }
            (StmtResult::Continue, None)
        }

        // For loop
        Stmt::For { var, iter, body } => {
            let iter_val = eval_expr_with_functions(iter, event, ctx, functions, bindings);

            if let Some(Value::Array(items)) = iter_val {
                for item in items {
                    bindings.insert(var.clone(), item);
                    let (result, _) = eval_stmts(body, event, ctx, functions, bindings);

                    match result {
                        StmtResult::Break => break,
                        StmtResult::ContinueLoop => continue,
                        StmtResult::Return(v) => return (StmtResult::Return(v), None),
                        StmtResult::Continue => {}
                    }
                }
            } else if let Some(Value::Map(map)) = iter_val {
                // Iterate over map entries as [key, value] pairs
                for (key, value) in map {
                    let pair = Value::Array(vec![Value::Str(key), value]);
                    bindings.insert(var.clone(), pair);
                    let (result, _) = eval_stmts(body, event, ctx, functions, bindings);

                    match result {
                        StmtResult::Break => break,
                        StmtResult::ContinueLoop => continue,
                        StmtResult::Return(v) => return (StmtResult::Return(v), None),
                        StmtResult::Continue => {}
                    }
                }
            }
            // Also support iterating over ranges (0..n)
            else if let Some(Value::Int(n)) = iter_val {
                for i in 0..n {
                    bindings.insert(var.clone(), Value::Int(i));
                    let (result, _) = eval_stmts(body, event, ctx, functions, bindings);

                    match result {
                        StmtResult::Break => break,
                        StmtResult::ContinueLoop => continue,
                        StmtResult::Return(v) => return (StmtResult::Return(v), None),
                        StmtResult::Continue => {}
                    }
                }
            }

            (StmtResult::Continue, None)
        }

        // While loop
        Stmt::While { cond, body } => {
            let max_iterations = 10_000; // Safety limit
            let mut iterations = 0;

            loop {
                iterations += 1;
                if iterations > max_iterations {
                    debug!("While loop exceeded max iterations ({})", max_iterations);
                    break;
                }

                let cond_val = eval_expr_with_functions(cond, event, ctx, functions, bindings);
                if !cond_val.and_then(|v| v.as_bool()).unwrap_or(false) {
                    break;
                }

                let (result, _) = eval_stmts(body, event, ctx, functions, bindings);

                match result {
                    StmtResult::Break => break,
                    StmtResult::ContinueLoop => continue,
                    StmtResult::Return(v) => return (StmtResult::Return(v), None),
                    StmtResult::Continue => {}
                }
            }

            (StmtResult::Continue, None)
        }

        // If statement
        Stmt::If {
            cond,
            then_branch,
            elif_branches,
            else_branch,
        } => {
            let cond_val = eval_expr_with_functions(cond, event, ctx, functions, bindings);

            if cond_val.and_then(|v| v.as_bool()).unwrap_or(false) {
                return eval_stmts(then_branch, event, ctx, functions, bindings);
            }

            // Check elif branches
            for (elif_cond, elif_body) in elif_branches {
                let elif_val = eval_expr_with_functions(elif_cond, event, ctx, functions, bindings);
                if elif_val.and_then(|v| v.as_bool()).unwrap_or(false) {
                    return eval_stmts(elif_body, event, ctx, functions, bindings);
                }
            }

            // Else branch
            if let Some(else_body) = else_branch {
                return eval_stmts(else_body, event, ctx, functions, bindings);
            }

            (StmtResult::Continue, None)
        }

        // Break statement
        Stmt::Break => (StmtResult::Break, None),

        // Continue statement
        Stmt::Continue => (StmtResult::ContinueLoop, None),

        // Return statement
        Stmt::Return(expr) => {
            let value = expr
                .as_ref()
                .and_then(|e| eval_expr_with_functions(e, event, ctx, functions, bindings));
            (StmtResult::Return(value.clone()), value)
        }

        // Emit event statement
        Stmt::Emit { event_type, fields } => {
            let mut new_event = Event::new(event_type);
            new_event.timestamp = event.timestamp;
            for arg in fields {
                if let Some(value) =
                    eval_expr_with_functions(&arg.value, event, ctx, functions, bindings)
                {
                    new_event.data.insert(arg.name.clone(), value);
                }
            }
            collect_emitted_event(new_event);
            (StmtResult::Continue, None)
        }

        // Other statements (StreamDecl, EventDecl, etc.) are not executed at runtime
        _ => (StmtResult::Continue, None),
    }
}

/// Execute a user-defined function with proper statement evaluation
pub fn call_user_function(
    func: &UserFunction,
    args: &[Value],
    event: &Event,
    ctx: &SequenceContext,
    functions: &HashMap<String, UserFunction>,
) -> Option<Value> {
    // Bind parameters to argument values
    let mut bindings = HashMap::new();
    for (i, (param_name, _)) in func.params.iter().enumerate() {
        if let Some(val) = args.get(i) {
            bindings.insert(param_name.clone(), val.clone());
        }
    }

    // Execute function body
    let (result, last_value) = eval_stmts(&func.body, event, ctx, functions, &mut bindings);

    // Return value from explicit return or last expression
    match result {
        StmtResult::Return(v) => v,
        _ => last_value,
    }
}

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

    match expr {
        Expr::Ident(name) => {
            // Check bindings first, then event, then context
            bindings
                .get(name)
                .cloned()
                .or_else(|| event.get(name).cloned())
        }
        Expr::Null => Some(Value::Null),
        Expr::Int(n) => Some(Value::Int(*n)),
        Expr::Float(f) => Some(Value::Float(*f)),
        Expr::Str(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::Duration(ns) => Some(Value::Duration(*ns)),

        // Array literal: [1, 2, 3]
        Expr::Array(items) => {
            let values: Vec<Value> = items
                .iter()
                .filter_map(|e| eval_expr_with_functions(e, event, ctx, functions, bindings))
                .collect();
            Some(Value::Array(values))
        }

        // Map literal: { "key": value, ... }
        Expr::Map(entries) => {
            let mut map = IndexMap::new();
            for (key, value_expr) in entries {
                if let Some(value) =
                    eval_expr_with_functions(value_expr, event, ctx, functions, bindings)
                {
                    map.insert(key.clone(), value);
                }
            }
            Some(Value::Map(map))
        }

        // Index access: arr[0] or map["key"]
        Expr::Index {
            expr: container,
            index,
        } => {
            let container_val =
                eval_expr_with_functions(container, event, ctx, functions, bindings)?;
            let index_val = eval_expr_with_functions(index, event, ctx, functions, bindings)?;

            match (&container_val, &index_val) {
                (Value::Array(arr), Value::Int(idx)) => {
                    let idx = if *idx < 0 {
                        (arr.len() as i64 + idx) as usize
                    } else {
                        *idx as usize
                    };
                    arr.get(idx).cloned()
                }
                (Value::Map(m), Value::Str(key)) => m.get(key).cloned(),
                (Value::Str(s), Value::Int(idx)) => {
                    let chars: Vec<char> = s.chars().collect();
                    let idx = if *idx < 0 {
                        (chars.len() as i64 + idx) as usize
                    } else {
                        *idx as usize
                    };
                    chars.get(idx).map(|c| Value::Str(c.to_string()))
                }
                _ => None,
            }
        }

        // Slice: arr[1:3] or str[0:5]
        Expr::Slice {
            expr: container,
            start,
            end,
        } => {
            let container_val =
                eval_expr_with_functions(container, event, ctx, functions, bindings)?;
            let start_val = start
                .as_ref()
                .and_then(|e| eval_expr_with_functions(e, event, ctx, functions, bindings))
                .and_then(|v| v.as_int())
                .unwrap_or(0) as usize;

            match container_val {
                Value::Array(arr) => {
                    let end_val = end
                        .as_ref()
                        .and_then(|e| eval_expr_with_functions(e, event, ctx, functions, bindings))
                        .and_then(|v| v.as_int())
                        .unwrap_or(arr.len() as i64) as usize;
                    let end_val = end_val.min(arr.len());
                    if start_val <= end_val {
                        Some(Value::Array(arr[start_val..end_val].to_vec()))
                    } else {
                        Some(Value::Array(vec![]))
                    }
                }
                Value::Str(s) => {
                    let chars: Vec<char> = s.chars().collect();
                    let end_val = end
                        .as_ref()
                        .and_then(|e| eval_expr_with_functions(e, event, ctx, functions, bindings))
                        .and_then(|v| v.as_int())
                        .unwrap_or(chars.len() as i64) as usize;
                    let end_val = end_val.min(chars.len());
                    if start_val <= end_val {
                        Some(Value::Str(chars[start_val..end_val].iter().collect()))
                    } else {
                        Some(Value::Str(String::new()))
                    }
                }
                _ => None,
            }
        }

        // Range: 0..10 or 0..=10
        Expr::Range {
            start,
            end,
            inclusive,
        } => {
            let start_val = eval_expr_with_functions(start, event, ctx, functions, bindings)
                .and_then(|v| v.as_int())?;
            let end_val = eval_expr_with_functions(end, event, ctx, functions, bindings)
                .and_then(|v| v.as_int())?;

            let range: Vec<Value> = if *inclusive {
                (start_val..=end_val).map(Value::Int).collect()
            } else {
                (start_val..end_val).map(Value::Int).collect()
            };
            Some(Value::Array(range))
        }

        // Null coalescing: expr ?? default
        Expr::Coalesce { expr: e, default } => {
            let val = eval_expr_with_functions(e, event, ctx, functions, bindings);
            match val {
                Some(Value::Null) | None => {
                    eval_expr_with_functions(default, event, ctx, functions, bindings)
                }
                v => v,
            }
        }

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

                // Check user-defined functions first - use full statement evaluation
                if let Some(user_fn) = functions.get(func_name) {
                    return call_user_function(user_fn, &arg_values, event, ctx, functions);
                }

                // Built-in functions
                match func_name.as_str() {
                    // Math functions
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
                    "pow" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                        (Value::Int(a), Value::Int(b)) => {
                            Some(Value::Int((*a as f64).powi(*b as i32) as i64))
                        }
                        (Value::Float(a), Value::Int(b)) => Some(Value::Float(a.powi(*b as i32))),
                        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a.powf(*b))),
                        (Value::Int(a), Value::Float(b)) => {
                            Some(Value::Float((*a as f64).powf(*b)))
                        }
                        _ => None,
                    },
                    "log" => arg_values.first().and_then(|v| match v {
                        Value::Int(n) => Some(Value::Float((*n as f64).ln())),
                        Value::Float(f) => Some(Value::Float(f.ln())),
                        _ => None,
                    }),
                    "log10" => arg_values.first().and_then(|v| match v {
                        Value::Int(n) => Some(Value::Float((*n as f64).log10())),
                        Value::Float(f) => Some(Value::Float(f.log10())),
                        _ => None,
                    }),
                    "exp" => arg_values.first().and_then(|v| match v {
                        Value::Int(n) => Some(Value::Float((*n as f64).exp())),
                        Value::Float(f) => Some(Value::Float(f.exp())),
                        _ => None,
                    }),
                    "sin" => arg_values.first().and_then(|v| match v {
                        Value::Int(n) => Some(Value::Float((*n as f64).sin())),
                        Value::Float(f) => Some(Value::Float(f.sin())),
                        _ => None,
                    }),
                    "cos" => arg_values.first().and_then(|v| match v {
                        Value::Int(n) => Some(Value::Float((*n as f64).cos())),
                        Value::Float(f) => Some(Value::Float(f.cos())),
                        _ => None,
                    }),
                    "tan" => arg_values.first().and_then(|v| match v {
                        Value::Int(n) => Some(Value::Float((*n as f64).tan())),
                        Value::Float(f) => Some(Value::Float(f.tan())),
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

                    // Array/Collection functions
                    "len" => arg_values.first().and_then(|v| match v {
                        Value::Str(s) => Some(Value::Int(s.len() as i64)),
                        Value::Array(a) => Some(Value::Int(a.len() as i64)),
                        Value::Map(m) => Some(Value::Int(m.len() as i64)),
                        _ => None,
                    }),
                    "first" => arg_values.first().and_then(|v| match v {
                        Value::Array(a) => a.first().cloned(),
                        _ => None,
                    }),
                    "last" => arg_values.first().and_then(|v| match v {
                        Value::Array(a) => a.last().cloned(),
                        _ => None,
                    }),
                    "push" if arg_values.len() == 2 => {
                        if let Value::Array(mut arr) = arg_values[0].clone() {
                            arr.push(arg_values[1].clone());
                            Some(Value::Array(arr))
                        } else {
                            None
                        }
                    }
                    "pop" => arg_values.first().and_then(|v| match v {
                        Value::Array(arr) => {
                            let mut arr = arr.clone();
                            arr.pop().map(|_| Value::Array(arr))
                        }
                        _ => None,
                    }),
                    "reverse" => arg_values.first().and_then(|v| match v {
                        Value::Array(arr) => {
                            let mut arr = arr.clone();
                            arr.reverse();
                            Some(Value::Array(arr))
                        }
                        Value::Str(s) => Some(Value::Str(s.chars().rev().collect())),
                        _ => None,
                    }),
                    "sort" => arg_values.first().and_then(|v| match v {
                        Value::Array(arr) => {
                            let mut arr = arr.clone();
                            arr.sort_by(|a, b| match (a, b) {
                                (Value::Int(x), Value::Int(y)) => x.cmp(y),
                                (Value::Float(x), Value::Float(y)) => {
                                    x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                                }
                                (Value::Str(x), Value::Str(y)) => x.cmp(y),
                                _ => std::cmp::Ordering::Equal,
                            });
                            Some(Value::Array(arr))
                        }
                        _ => None,
                    }),
                    "contains" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                        (Value::Array(arr), val) => Some(Value::Bool(arr.contains(val))),
                        (Value::Str(s), Value::Str(sub)) => {
                            Some(Value::Bool(s.contains(sub.as_str())))
                        }
                        (Value::Map(m), Value::Str(key)) => Some(Value::Bool(m.contains_key(key))),
                        _ => None,
                    },
                    "keys" => arg_values.first().and_then(|v| match v {
                        Value::Map(m) => Some(Value::Array(
                            m.keys().map(|k| Value::Str(k.clone())).collect(),
                        )),
                        _ => None,
                    }),
                    "values" => arg_values.first().and_then(|v| match v {
                        Value::Map(m) => Some(Value::Array(m.values().cloned().collect())),
                        _ => None,
                    }),
                    "get" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                        (Value::Array(arr), Value::Int(idx)) => arr.get(*idx as usize).cloned(),
                        (Value::Map(m), Value::Str(key)) => m.get(key).cloned(),
                        _ => None,
                    },
                    "set" if arg_values.len() == 3 => {
                        match (&arg_values[0], &arg_values[1], &arg_values[2]) {
                            (Value::Array(arr), Value::Int(idx), val) => {
                                let mut arr = arr.clone();
                                let idx = *idx as usize;
                                if idx < arr.len() {
                                    arr[idx] = val.clone();
                                }
                                Some(Value::Array(arr))
                            }
                            (Value::Map(m), Value::Str(key), val) => {
                                let mut m = m.clone();
                                m.insert(key.clone(), val.clone());
                                Some(Value::Map(m))
                            }
                            _ => None,
                        }
                    }
                    "range" if !arg_values.is_empty() => {
                        let (start, end) = if arg_values.len() >= 2 {
                            match (&arg_values[0], &arg_values[1]) {
                                (Value::Int(s), Value::Int(e)) => (*s, *e),
                                _ => return None,
                            }
                        } else if let Value::Int(n) = &arg_values[0] {
                            (0, *n)
                        } else {
                            return None;
                        };
                        Some(Value::Array((start..end).map(Value::Int).collect()))
                    }
                    "sum" => arg_values.first().and_then(|v| match v {
                        Value::Array(arr) => {
                            let sum: f64 = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .sum();
                            Some(Value::Float(sum))
                        }
                        _ => None,
                    }),
                    "avg" => arg_values.first().and_then(|v| match v {
                        Value::Array(arr) => {
                            let nums: Vec<f64> = arr
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Int(n) => Some(*n as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                })
                                .collect();
                            if nums.is_empty() {
                                Some(Value::Float(0.0))
                            } else {
                                Some(Value::Float(nums.iter().sum::<f64>() / nums.len() as f64))
                            }
                        }
                        _ => None,
                    }),

                    // String functions
                    "to_string" => arg_values.first().map(|v| Value::Str(format!("{}", v))),
                    "to_int" => arg_values.first().and_then(|v| match v {
                        Value::Int(n) => Some(Value::Int(*n)),
                        Value::Float(f) => Some(Value::Int(*f as i64)),
                        Value::Str(s) => s.parse::<i64>().ok().map(Value::Int),
                        Value::Bool(b) => Some(Value::Int(if *b { 1 } else { 0 })),
                        _ => None,
                    }),
                    "to_float" => arg_values.first().and_then(|v| match v {
                        Value::Int(n) => Some(Value::Float(*n as f64)),
                        Value::Float(f) => Some(Value::Float(*f)),
                        Value::Str(s) => s.parse::<f64>().ok().map(Value::Float),
                        _ => None,
                    }),
                    "trim" => arg_values.first().and_then(|v| match v {
                        Value::Str(s) => Some(Value::Str(s.trim().to_string())),
                        _ => None,
                    }),
                    "lower" | "lowercase" => arg_values.first().and_then(|v| match v {
                        Value::Str(s) => Some(Value::Str(s.to_lowercase())),
                        _ => None,
                    }),
                    "upper" | "uppercase" => arg_values.first().and_then(|v| match v {
                        Value::Str(s) => Some(Value::Str(s.to_uppercase())),
                        _ => None,
                    }),
                    "split" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                        (Value::Str(s), Value::Str(sep)) => Some(Value::Array(
                            s.split(sep.as_str())
                                .map(|p| Value::Str(p.to_string()))
                                .collect(),
                        )),
                        _ => None,
                    },
                    "join" if arg_values.len() == 2 => match (&arg_values[0], &arg_values[1]) {
                        (Value::Array(arr), Value::Str(sep)) => {
                            let strs: Vec<String> = arr.iter().map(|v| format!("{}", v)).collect();
                            Some(Value::Str(strs.join(sep)))
                        }
                        _ => None,
                    },
                    "replace" if arg_values.len() == 3 => {
                        match (&arg_values[0], &arg_values[1], &arg_values[2]) {
                            (Value::Str(s), Value::Str(from), Value::Str(to)) => {
                                Some(Value::Str(s.replace(from.as_str(), to.as_str())))
                            }
                            _ => None,
                        }
                    }
                    "starts_with" if arg_values.len() == 2 => {
                        match (&arg_values[0], &arg_values[1]) {
                            (Value::Str(s), Value::Str(prefix)) => {
                                Some(Value::Bool(s.starts_with(prefix.as_str())))
                            }
                            _ => None,
                        }
                    }
                    "ends_with" if arg_values.len() == 2 => {
                        match (&arg_values[0], &arg_values[1]) {
                            (Value::Str(s), Value::Str(suffix)) => {
                                Some(Value::Bool(s.ends_with(suffix.as_str())))
                            }
                            _ => None,
                        }
                    }
                    "substring" if arg_values.len() >= 2 => match &arg_values[0] {
                        Value::Str(s) => {
                            let start = match &arg_values[1] {
                                Value::Int(n) => *n as usize,
                                _ => return None,
                            };
                            let end = if arg_values.len() >= 3 {
                                match &arg_values[2] {
                                    Value::Int(n) => *n as usize,
                                    _ => return None,
                                }
                            } else {
                                s.len()
                            };
                            let chars: Vec<char> = s.chars().collect();
                            (start <= end && end <= chars.len())
                                .then(|| Value::Str(chars[start..end].iter().collect()))
                        }
                        _ => None,
                    },

                    // Type checking
                    "type_of" => arg_values.first().map(|v| {
                        Value::Str(
                            match v {
                                Value::Int(_) => "int",
                                Value::Float(_) => "float",
                                Value::Str(_) => "string",
                                Value::Bool(_) => "bool",
                                Value::Array(_) => "array",
                                Value::Map(_) => "map",
                                Value::Null => "null",
                                Value::Duration(_) => "duration",
                                Value::Timestamp(_) => "timestamp",
                            }
                            .to_string(),
                        )
                    }),
                    "is_null" => arg_values
                        .first()
                        .map(|v| Value::Bool(matches!(v, Value::Null))),
                    "is_int" => arg_values
                        .first()
                        .map(|v| Value::Bool(matches!(v, Value::Int(_)))),
                    "is_float" => arg_values
                        .first()
                        .map(|v| Value::Bool(matches!(v, Value::Float(_)))),
                    "is_string" => arg_values
                        .first()
                        .map(|v| Value::Bool(matches!(v, Value::Str(_)))),
                    "is_bool" => arg_values
                        .first()
                        .map(|v| Value::Bool(matches!(v, Value::Bool(_)))),
                    "is_array" => arg_values
                        .first()
                        .map(|v| Value::Bool(matches!(v, Value::Array(_)))),
                    "is_map" => arg_values
                        .first()
                        .map(|v| Value::Bool(matches!(v, Value::Map(_)))),

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
                BinOp::Mod => match (&left_val, &right_val) {
                    (Value::Int(a), Value::Int(b)) if *b != 0 => Some(Value::Int(a % b)),
                    (Value::Float(a), Value::Float(b)) if *b != 0.0 => Some(Value::Float(a % b)),
                    (Value::Int(a), Value::Float(b)) if *b != 0.0 => {
                        Some(Value::Float(*a as f64 % b))
                    }
                    (Value::Float(a), Value::Int(b)) if *b != 0 => {
                        Some(Value::Float(a % *b as f64))
                    }
                    _ => None,
                },
                BinOp::Pow => match (&left_val, &right_val) {
                    (Value::Int(a), Value::Int(b)) => {
                        Some(Value::Int((*a as f64).powi(*b as i32) as i64))
                    }
                    (Value::Float(a), Value::Int(b)) => Some(Value::Float(a.powi(*b as i32))),
                    (Value::Float(a), Value::Float(b)) => Some(Value::Float(a.powf(*b))),
                    (Value::Int(a), Value::Float(b)) => Some(Value::Float((*a as f64).powf(*b))),
                    _ => None,
                },
                BinOp::In => match (&left_val, &right_val) {
                    (val, Value::Array(arr)) => Some(Value::Bool(arr.contains(val))),
                    (Value::Str(key), Value::Map(m)) => Some(Value::Bool(m.contains_key(key))),
                    (Value::Str(sub), Value::Str(s)) => Some(Value::Bool(s.contains(sub.as_str()))),
                    _ => None,
                },
                BinOp::NotIn => match (&left_val, &right_val) {
                    (val, Value::Array(arr)) => Some(Value::Bool(!arr.contains(val))),
                    (Value::Str(key), Value::Map(m)) => Some(Value::Bool(!m.contains_key(key))),
                    (Value::Str(sub), Value::Str(s)) => {
                        Some(Value::Bool(!s.contains(sub.as_str())))
                    }
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
                BinOp::Xor => {
                    let a = left_val.as_bool()?;
                    let b = right_val.as_bool()?;
                    Some(Value::Bool(a ^ b))
                }
                _ => None,
            }
        }

        // Unary operations
        Expr::Unary { op, expr: inner } => {
            let val = eval_expr_with_functions(inner, event, ctx, functions, bindings)?;
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
