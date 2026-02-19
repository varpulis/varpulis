//! Direct unit-level tests for evaluator functions in
//! `crates/varpulis-runtime/src/engine/evaluator.rs`.
//!
//! Covers: eval_binary_op, eval_pattern_expr, with_emit_collector,
//! eval_filter_expr, eval_expr_with_functions, eval_stmts, eval_stmt,
//! call_user_function, eval_expr_ctx.

use indexmap::IndexMap;
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::sync::Arc;
use varpulis_core::ast::{Arg, BinOp, Expr, NamedArg};
use varpulis_core::span::Spanned;
use varpulis_core::{Stmt, Type, Value};
use varpulis_runtime::engine::evaluator::*;
use varpulis_runtime::engine::UserFunction;
use varpulis_runtime::event::Event;
use varpulis_runtime::sequence::SequenceContext;

// =============================================================================
// Helpers
// =============================================================================

fn empty_functions() -> FxHashMap<String, UserFunction> {
    FxHashMap::default()
}

fn empty_bindings() -> FxHashMap<String, Value> {
    FxHashMap::default()
}

fn empty_seq_ctx() -> SequenceContext {
    SequenceContext::new()
}

fn empty_event() -> Event {
    Event::new("TestEvent")
}

fn make_map(pairs: &[(&str, Value)]) -> IndexMap<Arc<str>, Value, FxBuildHasher> {
    let mut m: IndexMap<Arc<str>, Value, FxBuildHasher> = IndexMap::with_hasher(FxBuildHasher);
    for (k, v) in pairs {
        m.insert(Arc::from(*k), v.clone());
    }
    m
}

// =============================================================================
// 1. eval_binary_op
// =============================================================================

mod binary_op_tests {
    use super::*;

    // ---- Gt ----

    #[test]
    fn gt_int_int_true() {
        let r = eval_binary_op(&BinOp::Gt, &Value::Int(5), &Value::Int(3));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn gt_int_int_false() {
        let r = eval_binary_op(&BinOp::Gt, &Value::Int(2), &Value::Int(3));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn gt_float_float_true() {
        let r = eval_binary_op(&BinOp::Gt, &Value::Float(3.5), &Value::Float(2.1));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn gt_float_float_false() {
        let r = eval_binary_op(&BinOp::Gt, &Value::Float(1.0), &Value::Float(2.0));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn gt_int_float() {
        let r = eval_binary_op(&BinOp::Gt, &Value::Int(5), &Value::Float(4.9));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn gt_float_int() {
        let r = eval_binary_op(&BinOp::Gt, &Value::Float(5.1), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    // ---- Lt ----

    #[test]
    fn lt_int_int_true() {
        let r = eval_binary_op(&BinOp::Lt, &Value::Int(2), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn lt_int_int_false() {
        let r = eval_binary_op(&BinOp::Lt, &Value::Int(5), &Value::Int(2));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn lt_float_float() {
        let r = eval_binary_op(&BinOp::Lt, &Value::Float(1.5), &Value::Float(2.5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn lt_int_float() {
        let r = eval_binary_op(&BinOp::Lt, &Value::Int(3), &Value::Float(3.5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn lt_float_int() {
        let r = eval_binary_op(&BinOp::Lt, &Value::Float(2.9), &Value::Int(3));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    // ---- Ge ----

    #[test]
    fn ge_int_int_equal() {
        let r = eval_binary_op(&BinOp::Ge, &Value::Int(5), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn ge_int_int_greater() {
        let r = eval_binary_op(&BinOp::Ge, &Value::Int(6), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn ge_int_int_less() {
        let r = eval_binary_op(&BinOp::Ge, &Value::Int(4), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn ge_float_float() {
        let r = eval_binary_op(&BinOp::Ge, &Value::Float(3.0), &Value::Float(3.0));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    // ---- Le ----

    #[test]
    fn le_int_int_equal() {
        let r = eval_binary_op(&BinOp::Le, &Value::Int(5), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn le_int_int_less() {
        let r = eval_binary_op(&BinOp::Le, &Value::Int(3), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn le_int_int_greater() {
        let r = eval_binary_op(&BinOp::Le, &Value::Int(6), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn le_float_float() {
        let r = eval_binary_op(&BinOp::Le, &Value::Float(2.5), &Value::Float(2.5));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    // ---- Eq ----

    #[test]
    fn eq_int_int() {
        let r = eval_binary_op(&BinOp::Eq, &Value::Int(42), &Value::Int(42));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn eq_int_int_not_equal() {
        let r = eval_binary_op(&BinOp::Eq, &Value::Int(42), &Value::Int(43));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn eq_float_float() {
        let r = eval_binary_op(&BinOp::Eq, &Value::Float(3.25), &Value::Float(3.25));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn eq_str_str() {
        let r = eval_binary_op(
            &BinOp::Eq,
            &Value::Str("hello".into()),
            &Value::Str("hello".into()),
        );
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn eq_bool_bool() {
        let r = eval_binary_op(&BinOp::Eq, &Value::Bool(true), &Value::Bool(true));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn eq_different_types() {
        let r = eval_binary_op(&BinOp::Eq, &Value::Int(1), &Value::Str("1".into()));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    // ---- NotEq ----

    #[test]
    fn neq_int_int() {
        let r = eval_binary_op(&BinOp::NotEq, &Value::Int(1), &Value::Int(2));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn neq_same_values() {
        let r = eval_binary_op(&BinOp::NotEq, &Value::Int(5), &Value::Int(5));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn neq_str_str() {
        let r = eval_binary_op(
            &BinOp::NotEq,
            &Value::Str("abc".into()),
            &Value::Str("def".into()),
        );
        assert_eq!(r, Some(Value::Bool(true)));
    }

    // ---- And ----

    #[test]
    fn and_true_true() {
        let r = eval_binary_op(&BinOp::And, &Value::Bool(true), &Value::Bool(true));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn and_true_false() {
        let r = eval_binary_op(&BinOp::And, &Value::Bool(true), &Value::Bool(false));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn and_false_false() {
        let r = eval_binary_op(&BinOp::And, &Value::Bool(false), &Value::Bool(false));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    // ---- Or ----

    #[test]
    fn or_true_false() {
        let r = eval_binary_op(&BinOp::Or, &Value::Bool(true), &Value::Bool(false));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn or_false_false() {
        let r = eval_binary_op(&BinOp::Or, &Value::Bool(false), &Value::Bool(false));
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn or_false_true() {
        let r = eval_binary_op(&BinOp::Or, &Value::Bool(false), &Value::Bool(true));
        assert_eq!(r, Some(Value::Bool(true)));
    }

    // ---- Unsupported op ----

    #[test]
    fn unsupported_op_returns_none() {
        // BinOp::Add is not handled by eval_binary_op (the pattern-level one)
        let r = eval_binary_op(&BinOp::Add, &Value::Int(1), &Value::Int(2));
        assert_eq!(r, None);
    }

    #[test]
    fn unsupported_op_sub_returns_none() {
        let r = eval_binary_op(&BinOp::Sub, &Value::Int(5), &Value::Int(3));
        assert_eq!(r, None);
    }

    #[test]
    fn unsupported_op_mul_returns_none() {
        let r = eval_binary_op(&BinOp::Mul, &Value::Int(2), &Value::Int(3));
        assert_eq!(r, None);
    }

    // ---- Gt with mismatched types ----

    #[test]
    fn gt_str_str_returns_none() {
        let r = eval_binary_op(&BinOp::Gt, &Value::Str("a".into()), &Value::Str("b".into()));
        assert_eq!(r, None);
    }

    // ---- Ge with mismatched types ----

    #[test]
    fn ge_int_float_returns_none() {
        // Ge only supports Int/Int and Float/Float
        let r = eval_binary_op(&BinOp::Ge, &Value::Int(5), &Value::Float(4.0));
        assert_eq!(r, None);
    }

    // ---- Le with mismatched types ----

    #[test]
    fn le_int_float_returns_none() {
        let r = eval_binary_op(&BinOp::Le, &Value::Int(3), &Value::Float(4.0));
        assert_eq!(r, None);
    }

    // ---- And/Or with non-bool (uses unwrap_or(false)) ----

    #[test]
    fn and_non_bool_returns_false() {
        let r = eval_binary_op(&BinOp::And, &Value::Int(1), &Value::Bool(true));
        // Int doesn't have as_bool => None => unwrap_or(false)
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn or_non_bool_returns_false_when_both_non_bool() {
        let r = eval_binary_op(&BinOp::Or, &Value::Int(0), &Value::Int(1));
        assert_eq!(r, Some(Value::Bool(false)));
    }
}

// =============================================================================
// 2. eval_pattern_expr
// =============================================================================

mod pattern_expr_tests {
    use super::*;

    fn eval_p(expr: &Expr, pattern_vars: &FxHashMap<String, Value>) -> Option<Value> {
        let events: Vec<varpulis_runtime::event::SharedEvent> = vec![];
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        eval_pattern_expr(expr, &events, &ctx, &functions, pattern_vars)
    }

    // ---- Ident lookup ----

    #[test]
    fn ident_found_in_pattern_vars() {
        let mut vars = FxHashMap::default();
        vars.insert("price".to_string(), Value::Float(99.5));
        let expr = Expr::Ident("price".to_string());
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(99.5)));
    }

    #[test]
    fn ident_not_found() {
        let vars = FxHashMap::default();
        let expr = Expr::Ident("missing".to_string());
        assert_eq!(eval_p(&expr, &vars), None);
    }

    // ---- Literals ----

    #[test]
    fn literal_int() {
        let expr = Expr::Int(42);
        assert_eq!(eval_p(&expr, &FxHashMap::default()), Some(Value::Int(42)));
    }

    #[test]
    fn literal_float() {
        let expr = Expr::Float(3.25);
        assert_eq!(
            eval_p(&expr, &FxHashMap::default()),
            Some(Value::Float(3.25))
        );
    }

    #[test]
    fn literal_bool_true() {
        let expr = Expr::Bool(true);
        assert_eq!(
            eval_p(&expr, &FxHashMap::default()),
            Some(Value::Bool(true))
        );
    }

    #[test]
    fn literal_bool_false() {
        let expr = Expr::Bool(false);
        assert_eq!(
            eval_p(&expr, &FxHashMap::default()),
            Some(Value::Bool(false))
        );
    }

    #[test]
    fn literal_str() {
        let expr = Expr::Str("hello".to_string());
        assert_eq!(
            eval_p(&expr, &FxHashMap::default()),
            Some(Value::Str("hello".into()))
        );
    }

    // ---- Member access on Map values ----

    #[test]
    fn member_access_on_map() {
        let mut vars = FxHashMap::default();
        let map = make_map(&[
            ("price", Value::Float(42.0)),
            ("name", Value::Str("AAPL".into())),
        ]);
        vars.insert("stock".to_string(), Value::map(map));

        let expr = Expr::Member {
            expr: Box::new(Expr::Ident("stock".to_string())),
            member: "price".to_string(),
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(42.0)));
    }

    #[test]
    fn member_access_on_map_missing_field() {
        let mut vars = FxHashMap::default();
        let map = make_map(&[("price", Value::Float(42.0))]);
        vars.insert("stock".to_string(), Value::map(map));

        let expr = Expr::Member {
            expr: Box::new(Expr::Ident("stock".to_string())),
            member: "volume".to_string(),
        };
        assert_eq!(eval_p(&expr, &vars), None);
    }

    #[test]
    fn member_access_on_non_map_returns_none() {
        let mut vars = FxHashMap::default();
        vars.insert("stock".to_string(), Value::Int(42));

        let expr = Expr::Member {
            expr: Box::new(Expr::Ident("stock".to_string())),
            member: "price".to_string(),
        };
        assert_eq!(eval_p(&expr, &vars), None);
    }

    // ---- Function calls: len, first, last, avg, sum, min, max, variance ----

    fn make_array_expr(name: &str) -> Expr {
        Expr::Ident(name.to_string())
    }

    fn call_fn(name: &str, arg: Expr) -> Expr {
        Expr::Call {
            func: Box::new(Expr::Ident(name.to_string())),
            args: vec![Arg::Positional(arg)],
        }
    }

    fn vars_with_arr(values: Vec<Value>) -> FxHashMap<String, Value> {
        let mut vars = FxHashMap::default();
        vars.insert("arr".to_string(), Value::array(values));
        vars
    }

    #[test]
    fn fn_len() {
        let vars = vars_with_arr(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let expr = call_fn("len", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Int(3)));
    }

    #[test]
    fn fn_len_empty() {
        let vars = vars_with_arr(vec![]);
        let expr = call_fn("len", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Int(0)));
    }

    #[test]
    fn fn_first() {
        let vars = vars_with_arr(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let expr = call_fn("first", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Int(10)));
    }

    #[test]
    fn fn_first_empty() {
        let vars = vars_with_arr(vec![]);
        let expr = call_fn("first", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), None);
    }

    #[test]
    fn fn_last() {
        let vars = vars_with_arr(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let expr = call_fn("last", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Int(30)));
    }

    #[test]
    fn fn_last_empty() {
        let vars = vars_with_arr(vec![]);
        let expr = call_fn("last", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), None);
    }

    #[test]
    fn fn_avg() {
        let vars = vars_with_arr(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let expr = call_fn("avg", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(20.0)));
    }

    #[test]
    fn fn_avg_empty() {
        let vars = vars_with_arr(vec![]);
        let expr = call_fn("avg", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(0.0)));
    }

    #[test]
    fn fn_avg_mixed_int_float() {
        let vars = vars_with_arr(vec![Value::Int(10), Value::Float(20.0)]);
        let expr = call_fn("avg", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(15.0)));
    }

    #[test]
    fn fn_sum() {
        let vars = vars_with_arr(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let expr = call_fn("sum", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(6.0)));
    }

    #[test]
    fn fn_sum_floats() {
        let vars = vars_with_arr(vec![Value::Float(1.5), Value::Float(2.5)]);
        let expr = call_fn("sum", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(4.0)));
    }

    #[test]
    fn fn_min() {
        let vars = vars_with_arr(vec![Value::Int(5), Value::Int(2), Value::Int(8)]);
        let expr = call_fn("min", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(2.0)));
    }

    #[test]
    fn fn_min_empty_returns_none() {
        let vars = vars_with_arr(vec![]);
        let expr = call_fn("min", make_array_expr("arr"));
        // min of empty array => f64::INFINITY => returns None
        assert_eq!(eval_p(&expr, &vars), None);
    }

    #[test]
    fn fn_max() {
        let vars = vars_with_arr(vec![Value::Int(5), Value::Int(2), Value::Int(8)]);
        let expr = call_fn("max", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(8.0)));
    }

    #[test]
    fn fn_max_empty_returns_none() {
        let vars = vars_with_arr(vec![]);
        let expr = call_fn("max", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), None);
    }

    #[test]
    fn fn_variance() {
        // variance of [2, 4, 4, 4, 5, 5, 7, 9] = 4.0
        let vals: Vec<Value> = vec![2, 4, 4, 4, 5, 5, 7, 9]
            .into_iter()
            .map(Value::Int)
            .collect();
        let vars = vars_with_arr(vals);
        let expr = call_fn("variance", make_array_expr("arr"));
        let result = eval_p(&expr, &vars).unwrap();
        if let Value::Float(v) = result {
            assert!((v - 4.0).abs() < 1e-10, "Expected 4.0, got {}", v);
        } else {
            panic!("Expected Float");
        }
    }

    #[test]
    fn fn_variance_empty() {
        let vars = vars_with_arr(vec![]);
        let expr = call_fn("variance", make_array_expr("arr"));
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(0.0)));
    }

    // ---- Method calls on arrays ----

    #[test]
    fn method_len() {
        let vars = vars_with_arr(vec![Value::Int(1), Value::Int(2)]);
        // arr.len()
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "len".to_string(),
            }),
            args: vec![],
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Int(2)));
    }

    #[test]
    fn method_sum() {
        let vars = vars_with_arr(vec![Value::Int(10), Value::Int(20)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "sum".to_string(),
            }),
            args: vec![],
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(30.0)));
    }

    #[test]
    fn method_avg() {
        let vars = vars_with_arr(vec![Value::Int(10), Value::Int(30)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "avg".to_string(),
            }),
            args: vec![],
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(20.0)));
    }

    #[test]
    fn method_min() {
        let vars = vars_with_arr(vec![Value::Int(5), Value::Int(1), Value::Int(9)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "min".to_string(),
            }),
            args: vec![],
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(1.0)));
    }

    #[test]
    fn method_max() {
        let vars = vars_with_arr(vec![Value::Int(5), Value::Int(1), Value::Int(9)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "max".to_string(),
            }),
            args: vec![],
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(9.0)));
    }

    #[test]
    fn method_first() {
        let vars = vars_with_arr(vec![Value::Int(100), Value::Int(200)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "first".to_string(),
            }),
            args: vec![],
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Int(100)));
    }

    #[test]
    fn method_last() {
        let vars = vars_with_arr(vec![Value::Int(100), Value::Int(200)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "last".to_string(),
            }),
            args: vec![],
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Int(200)));
    }

    #[test]
    fn method_filter() {
        // arr.filter(x => x > 2) where arr = [1, 2, 3, 4, 5]
        let vars = vars_with_arr(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "filter".to_string(),
            }),
            args: vec![Arg::Positional(Expr::Lambda {
                params: vec!["x".to_string()],
                body: Box::new(Expr::Binary {
                    op: BinOp::Gt,
                    left: Box::new(Expr::Ident("x".to_string())),
                    right: Box::new(Expr::Int(2)),
                }),
            })],
        };
        let result = eval_p(&expr, &vars);
        assert_eq!(
            result,
            Some(Value::array(vec![
                Value::Int(3),
                Value::Int(4),
                Value::Int(5),
            ]))
        );
    }

    #[test]
    fn method_map() {
        // arr.map(x => x) where arr = [1, 2, 3] -- identity map
        // Note: the lambda body uses pattern_vars lookup which looks up "x" in local vars
        let vars = vars_with_arr(vec![Value::Int(10), Value::Int(20)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "map".to_string(),
            }),
            args: vec![Arg::Positional(Expr::Lambda {
                params: vec!["x".to_string()],
                body: Box::new(Expr::Ident("x".to_string())),
            })],
        };
        let result = eval_p(&expr, &vars);
        assert_eq!(
            result,
            Some(Value::array(vec![Value::Int(10), Value::Int(20)]))
        );
    }

    #[test]
    fn method_flatten() {
        // [[1, 2], [3, 4]].flatten() -> [1, 2, 3, 4]
        let mut vars = FxHashMap::default();
        vars.insert(
            "arr".to_string(),
            Value::array(vec![
                Value::array(vec![Value::Int(1), Value::Int(2)]),
                Value::array(vec![Value::Int(3), Value::Int(4)]),
            ]),
        );
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "flatten".to_string(),
            }),
            args: vec![],
        };
        let result = eval_p(&expr, &vars);
        assert_eq!(
            result,
            Some(Value::array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4),
            ]))
        );
    }

    #[test]
    fn method_flatten_mixed() {
        // [1, [2, 3], 4].flatten() -> [1, 2, 3, 4]
        let mut vars = FxHashMap::default();
        vars.insert(
            "arr".to_string(),
            Value::array(vec![
                Value::Int(1),
                Value::array(vec![Value::Int(2), Value::Int(3)]),
                Value::Int(4),
            ]),
        );
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "flatten".to_string(),
            }),
            args: vec![],
        };
        let result = eval_p(&expr, &vars);
        assert_eq!(
            result,
            Some(Value::array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4),
            ]))
        );
    }

    #[test]
    fn method_sliding_pairs() {
        // [1, 2, 3].sliding_pairs() -> [[1, 2], [2, 3]]
        let vars = vars_with_arr(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "sliding_pairs".to_string(),
            }),
            args: vec![],
        };
        let result = eval_p(&expr, &vars);
        assert_eq!(
            result,
            Some(Value::array(vec![
                Value::array(vec![Value::Int(1), Value::Int(2)]),
                Value::array(vec![Value::Int(2), Value::Int(3)]),
            ]))
        );
    }

    #[test]
    fn method_sliding_pairs_single_element() {
        let vars = vars_with_arr(vec![Value::Int(1)]);
        let expr = Expr::Call {
            func: Box::new(Expr::Member {
                expr: Box::new(Expr::Ident("arr".to_string())),
                member: "sliding_pairs".to_string(),
            }),
            args: vec![],
        };
        let result = eval_p(&expr, &vars);
        assert_eq!(result, Some(Value::array(vec![])));
    }

    // ---- Binary expressions within patterns ----

    #[test]
    fn binary_gt_in_pattern() {
        let mut vars = FxHashMap::default();
        vars.insert("x".to_string(), Value::Int(10));
        let expr = Expr::Binary {
            op: BinOp::Gt,
            left: Box::new(Expr::Ident("x".to_string())),
            right: Box::new(Expr::Int(5)),
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Bool(true)));
    }

    #[test]
    fn binary_eq_in_pattern() {
        let mut vars = FxHashMap::default();
        vars.insert("status".to_string(), Value::Str("active".into()));
        let expr = Expr::Binary {
            op: BinOp::Eq,
            left: Box::new(Expr::Ident("status".to_string())),
            right: Box::new(Expr::Str("active".to_string())),
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Bool(true)));
    }

    #[test]
    fn binary_and_in_pattern() {
        let expr = Expr::Binary {
            op: BinOp::And,
            left: Box::new(Expr::Bool(true)),
            right: Box::new(Expr::Bool(false)),
        };
        assert_eq!(
            eval_p(&expr, &FxHashMap::default()),
            Some(Value::Bool(false))
        );
    }

    // ---- Lambda ----

    #[test]
    fn lambda_evaluates_body() {
        // Lambda { params: ["events"], body: Int(42) }
        // The lambda evaluator ignores params and evaluates body in pattern context
        let expr = Expr::Lambda {
            params: vec!["events".to_string()],
            body: Box::new(Expr::Int(42)),
        };
        assert_eq!(eval_p(&expr, &FxHashMap::default()), Some(Value::Int(42)));
    }

    #[test]
    fn lambda_with_vars_in_body() {
        let mut vars = FxHashMap::default();
        vars.insert("total".to_string(), Value::Float(100.0));
        let expr = Expr::Lambda {
            params: vec!["e".to_string()],
            body: Box::new(Expr::Ident("total".to_string())),
        };
        assert_eq!(eval_p(&expr, &vars), Some(Value::Float(100.0)));
    }

    // ---- Block with let bindings ----

    #[test]
    fn block_with_let_bindings() {
        // { let a = 10; let b = 20; a }
        let expr = Expr::Block {
            stmts: vec![
                ("a".to_string(), None, Expr::Int(10), false),
                ("b".to_string(), None, Expr::Int(20), false),
            ],
            result: Box::new(Expr::Ident("a".to_string())),
        };
        assert_eq!(eval_p(&expr, &FxHashMap::default()), Some(Value::Int(10)));
    }

    #[test]
    fn block_result_uses_bindings() {
        // { let x = 5; x }
        let expr = Expr::Block {
            stmts: vec![("x".to_string(), None, Expr::Int(5), false)],
            result: Box::new(Expr::Ident("x".to_string())),
        };
        assert_eq!(eval_p(&expr, &FxHashMap::default()), Some(Value::Int(5)));
    }

    #[test]
    fn block_binding_depends_on_previous() {
        // { let a = 3; let b = a; b } -- b should see a in pattern context
        // Note: eval_pattern_expr for Block accumulates bindings sequentially
        let expr = Expr::Block {
            stmts: vec![
                ("a".to_string(), None, Expr::Int(3), false),
                ("b".to_string(), None, Expr::Ident("a".to_string()), false),
            ],
            result: Box::new(Expr::Ident("b".to_string())),
        };
        assert_eq!(eval_p(&expr, &FxHashMap::default()), Some(Value::Int(3)));
    }

    // ---- Unknown expression returns None ----

    #[test]
    fn unknown_expr_returns_none() {
        let expr = Expr::Null;
        assert_eq!(eval_p(&expr, &FxHashMap::default()), None);
    }
}

// =============================================================================
// 3. with_emit_collector
// =============================================================================

mod emit_collector_tests {
    use super::*;

    #[test]
    fn basic_emit_collection() {
        let (result, events) = with_emit_collector(|| {
            // We cannot call collect_emitted_event directly since it is private,
            // but we can use eval_stmt with Stmt::Emit to trigger it.
            let event = empty_event();
            let ctx = empty_seq_ctx();
            let functions = empty_functions();
            let mut bindings = empty_bindings();

            let stmt = Stmt::Emit {
                event_type: "Alert".to_string(),
                fields: vec![NamedArg {
                    name: "level".to_string(),
                    value: Expr::Int(1),
                }],
            };
            eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
            42
        });
        assert_eq!(result, 42);
        assert_eq!(events.len(), 1);
        assert_eq!(&*events[0].event_type, "Alert");
        assert_eq!(events[0].data.get("level"), Some(&Value::Int(1)));
    }

    #[test]
    fn nested_emit_collection() {
        let (outer_result, outer_events) = with_emit_collector(|| {
            let event = empty_event();
            let ctx = empty_seq_ctx();
            let functions = empty_functions();
            let mut bindings = empty_bindings();

            // Emit in outer scope
            let stmt_outer = Stmt::Emit {
                event_type: "Outer".to_string(),
                fields: vec![],
            };
            eval_stmt(&stmt_outer, &event, &ctx, &functions, &mut bindings);

            // Nested collector
            let (inner_result, inner_events) = with_emit_collector(|| {
                let mut inner_bindings = empty_bindings();
                let stmt_inner = Stmt::Emit {
                    event_type: "Inner".to_string(),
                    fields: vec![],
                };
                eval_stmt(&stmt_inner, &event, &ctx, &functions, &mut inner_bindings);
                "inner_done"
            });

            assert_eq!(inner_result, "inner_done");
            assert_eq!(inner_events.len(), 1);
            assert_eq!(&*inner_events[0].event_type, "Inner");

            // Emit another in outer scope after nested
            let stmt_outer2 = Stmt::Emit {
                event_type: "Outer2".to_string(),
                fields: vec![],
            };
            eval_stmt(&stmt_outer2, &event, &ctx, &functions, &mut bindings);

            "outer_done"
        });

        assert_eq!(outer_result, "outer_done");
        assert_eq!(outer_events.len(), 2);
        assert_eq!(&*outer_events[0].event_type, "Outer");
        assert_eq!(&*outer_events[1].event_type, "Outer2");
    }

    #[test]
    fn emit_collector_no_emits() {
        let (result, events) = with_emit_collector(|| {
            // No emit statements
            99
        });
        assert_eq!(result, 99);
        assert!(events.is_empty());
    }

    #[test]
    fn emit_collector_multiple_emits() {
        let (_, events) = with_emit_collector(|| {
            let event = empty_event();
            let ctx = empty_seq_ctx();
            let functions = empty_functions();
            let mut bindings = empty_bindings();

            for i in 0..5 {
                let stmt = Stmt::Emit {
                    event_type: format!("Ev{}", i),
                    fields: vec![NamedArg {
                        name: "idx".to_string(),
                        value: Expr::Int(i),
                    }],
                };
                eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
            }
        });
        assert_eq!(events.len(), 5);
        for (i, ev) in events.iter().enumerate() {
            assert_eq!(&*ev.event_type, &format!("Ev{}", i));
            assert_eq!(ev.data.get("idx"), Some(&Value::Int(i as i64)));
        }
    }
}

// =============================================================================
// 5. eval_filter_expr
// =============================================================================

mod filter_expr_tests {
    use super::*;

    #[test]
    fn filter_literal_int() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let expr = Expr::Int(100);
        assert_eq!(eval_filter_expr(&expr, &event, &ctx), Some(Value::Int(100)));
    }

    #[test]
    fn filter_ident_from_event() {
        let event = Event::new("Tick").with_field("price", 42.5f64);
        let ctx = empty_seq_ctx();
        let expr = Expr::Ident("price".to_string());
        assert_eq!(
            eval_filter_expr(&expr, &event, &ctx),
            Some(Value::Float(42.5))
        );
    }

    #[test]
    fn filter_binary_comparison() {
        let event = Event::new("Tick").with_field("price", 100i64);
        let ctx = empty_seq_ctx();
        let expr = Expr::Binary {
            op: BinOp::Gt,
            left: Box::new(Expr::Ident("price".to_string())),
            right: Box::new(Expr::Int(50)),
        };
        assert_eq!(
            eval_filter_expr(&expr, &event, &ctx),
            Some(Value::Bool(true))
        );
    }
}

// =============================================================================
// 6. eval_expr_with_functions
// =============================================================================

mod expr_with_functions_tests {
    use super::*;

    #[test]
    fn expr_ident_from_bindings() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = FxHashMap::default();
        bindings.insert("x".to_string(), Value::Int(99));

        let expr = Expr::Ident("x".to_string());
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(99)));
    }

    #[test]
    fn expr_ident_bindings_takes_priority_over_event() {
        let event = Event::new("Test").with_field("x", 10i64);
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = FxHashMap::default();
        bindings.insert("x".to_string(), Value::Int(99));

        let expr = Expr::Ident("x".to_string());
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(99)));
    }

    #[test]
    fn expr_null() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Null;
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Null));
    }

    #[test]
    fn expr_array_literal() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Array(vec![Expr::Int(1), Expr::Int(2), Expr::Int(3)]);
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(
            r,
            Some(Value::array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
            ]))
        );
    }

    #[test]
    fn expr_binary_add_ints() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::Add,
            left: Box::new(Expr::Int(3)),
            right: Box::new(Expr::Int(4)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(7)));
    }

    #[test]
    fn expr_binary_sub_floats() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::Sub,
            left: Box::new(Expr::Float(10.5)),
            right: Box::new(Expr::Float(3.5)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Float(7.0)));
    }

    #[test]
    fn expr_binary_mul() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::Mul,
            left: Box::new(Expr::Int(6)),
            right: Box::new(Expr::Int(7)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(42)));
    }

    #[test]
    fn expr_binary_div_int() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::Div,
            left: Box::new(Expr::Int(10)),
            right: Box::new(Expr::Int(3)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(3))); // integer division
    }

    #[test]
    fn expr_binary_div_by_zero() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::Div,
            left: Box::new(Expr::Int(10)),
            right: Box::new(Expr::Int(0)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, None);
    }

    #[test]
    fn expr_binary_mod() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::Mod,
            left: Box::new(Expr::Int(10)),
            right: Box::new(Expr::Int(3)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(1)));
    }

    #[test]
    fn expr_string_concat() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::Add,
            left: Box::new(Expr::Str("hello".to_string())),
            right: Box::new(Expr::Str(" world".to_string())),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Str("hello world".into())));
    }

    #[test]
    fn expr_unary_neg() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Unary {
            op: varpulis_core::ast::UnaryOp::Neg,
            expr: Box::new(Expr::Int(42)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(-42)));
    }

    #[test]
    fn expr_unary_not() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Unary {
            op: varpulis_core::ast::UnaryOp::Not,
            expr: Box::new(Expr::Bool(true)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Bool(false)));
    }

    #[test]
    fn expr_if_true_branch() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::If {
            cond: Box::new(Expr::Bool(true)),
            then_branch: Box::new(Expr::Int(1)),
            else_branch: Box::new(Expr::Int(2)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(1)));
    }

    #[test]
    fn expr_if_false_branch() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::If {
            cond: Box::new(Expr::Bool(false)),
            then_branch: Box::new(Expr::Int(1)),
            else_branch: Box::new(Expr::Int(2)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(2)));
    }

    #[test]
    fn expr_coalesce_non_null() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Coalesce {
            expr: Box::new(Expr::Int(42)),
            default: Box::new(Expr::Int(0)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(42)));
    }

    #[test]
    fn expr_coalesce_null() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Coalesce {
            expr: Box::new(Expr::Null),
            default: Box::new(Expr::Int(99)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(99)));
    }

    #[test]
    fn expr_builtin_abs() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Call {
            func: Box::new(Expr::Ident("abs".to_string())),
            args: vec![Arg::Positional(Expr::Int(-5))],
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(5)));
    }

    #[test]
    fn expr_builtin_len_string() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Call {
            func: Box::new(Expr::Ident("len".to_string())),
            args: vec![Arg::Positional(Expr::Str("hello".to_string()))],
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(5)));
    }

    #[test]
    fn expr_index_array() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Index {
            expr: Box::new(Expr::Array(vec![
                Expr::Int(10),
                Expr::Int(20),
                Expr::Int(30),
            ])),
            index: Box::new(Expr::Int(1)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(20)));
    }

    #[test]
    fn expr_index_negative() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Index {
            expr: Box::new(Expr::Array(vec![
                Expr::Int(10),
                Expr::Int(20),
                Expr::Int(30),
            ])),
            index: Box::new(Expr::Int(-1)),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Int(30)));
    }

    #[test]
    fn expr_in_operator() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::In,
            left: Box::new(Expr::Int(2)),
            right: Box::new(Expr::Array(vec![Expr::Int(1), Expr::Int(2), Expr::Int(3)])),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Bool(true)));
    }

    #[test]
    fn expr_not_in_operator() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();
        let expr = Expr::Binary {
            op: BinOp::NotIn,
            left: Box::new(Expr::Int(5)),
            right: Box::new(Expr::Array(vec![Expr::Int(1), Expr::Int(2), Expr::Int(3)])),
        };
        let r = eval_expr_with_functions(&expr, &event, &ctx, &functions, &bindings);
        assert_eq!(r, Some(Value::Bool(true)));
    }
}

// =============================================================================
// 7. eval_stmts and eval_stmt
// =============================================================================

mod stmt_tests {
    use super::*;

    #[test]
    fn stmt_expr() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmt = Stmt::Expr(Expr::Int(42));
        let (result, value) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Continue));
        assert_eq!(value, Some(Value::Int(42)));
    }

    #[test]
    fn stmt_var_decl() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmt = Stmt::VarDecl {
            mutable: true,
            name: "x".to_string(),
            ty: None,
            value: Expr::Int(10),
        };
        let (result, _) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Continue));
        assert_eq!(bindings.get("x"), Some(&Value::Int(10)));
    }

    #[test]
    fn stmt_const_decl() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmt = Stmt::ConstDecl {
            name: "PI".to_string(),
            ty: None,
            value: Expr::Float(3.25),
        };
        let (result, _) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Continue));
        assert_eq!(bindings.get("PI"), Some(&Value::Float(3.25)));
    }

    #[test]
    fn stmt_assignment() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        bindings.insert("x".to_string(), Value::Int(5));
        let stmt = Stmt::Assignment {
            name: "x".to_string(),
            value: Expr::Int(42),
        };
        let (result, _) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Continue));
        assert_eq!(bindings.get("x"), Some(&Value::Int(42)));
    }

    #[test]
    fn stmt_break() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let (result, _) = eval_stmt(&Stmt::Break, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Break));
    }

    #[test]
    fn stmt_continue() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let (result, _) = eval_stmt(&Stmt::Continue, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::ContinueLoop));
    }

    #[test]
    fn stmt_return_with_value() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmt = Stmt::Return(Some(Expr::Int(99)));
        let (result, value) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Return(Some(Value::Int(99)))));
        assert_eq!(value, Some(Value::Int(99)));
    }

    #[test]
    fn stmt_return_without_value() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmt = Stmt::Return(None);
        let (result, value) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Return(None)));
        assert_eq!(value, None);
    }

    #[test]
    fn eval_stmts_sequence() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmts = vec![
            Spanned::dummy(Stmt::VarDecl {
                mutable: true,
                name: "a".to_string(),
                ty: None,
                value: Expr::Int(1),
            }),
            Spanned::dummy(Stmt::VarDecl {
                mutable: true,
                name: "b".to_string(),
                ty: None,
                value: Expr::Int(2),
            }),
            Spanned::dummy(Stmt::Expr(Expr::Binary {
                op: BinOp::Add,
                left: Box::new(Expr::Ident("a".to_string())),
                right: Box::new(Expr::Ident("b".to_string())),
            })),
        ];
        let (result, value) = eval_stmts(&stmts, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Continue));
        assert_eq!(value, Some(Value::Int(3)));
    }

    #[test]
    fn eval_stmts_break_stops_early() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmts = vec![
            Spanned::dummy(Stmt::Expr(Expr::Int(1))),
            Spanned::dummy(Stmt::Break),
            Spanned::dummy(Stmt::Expr(Expr::Int(2))), // should not execute
        ];
        let (result, _) = eval_stmts(&stmts, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Break));
    }

    #[test]
    fn stmt_if_then() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmt = Stmt::If {
            cond: Expr::Bool(true),
            then_branch: vec![Spanned::dummy(Stmt::Expr(Expr::Int(42)))],
            elif_branches: vec![],
            else_branch: None,
        };
        let (result, value) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Continue));
        assert_eq!(value, Some(Value::Int(42)));
    }

    #[test]
    fn stmt_if_else() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmt = Stmt::If {
            cond: Expr::Bool(false),
            then_branch: vec![Spanned::dummy(Stmt::Expr(Expr::Int(1)))],
            elif_branches: vec![],
            else_branch: Some(vec![Spanned::dummy(Stmt::Expr(Expr::Int(2)))]),
        };
        let (result, value) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Continue));
        assert_eq!(value, Some(Value::Int(2)));
    }

    #[test]
    fn stmt_if_elif() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        let stmt = Stmt::If {
            cond: Expr::Bool(false),
            then_branch: vec![Spanned::dummy(Stmt::Expr(Expr::Int(1)))],
            elif_branches: vec![(
                Expr::Bool(true),
                vec![Spanned::dummy(Stmt::Expr(Expr::Int(42)))],
            )],
            else_branch: Some(vec![Spanned::dummy(Stmt::Expr(Expr::Int(3)))]),
        };
        let (result, value) = eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert!(matches!(result, StmtResult::Continue));
        assert_eq!(value, Some(Value::Int(42)));
    }

    #[test]
    fn stmt_for_array() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        bindings.insert("total".to_string(), Value::Int(0));
        let stmt = Stmt::For {
            var: "i".to_string(),
            iter: Expr::Array(vec![Expr::Int(1), Expr::Int(2), Expr::Int(3)]),
            body: vec![Spanned::dummy(Stmt::Assignment {
                name: "total".to_string(),
                value: Expr::Binary {
                    op: BinOp::Add,
                    left: Box::new(Expr::Ident("total".to_string())),
                    right: Box::new(Expr::Ident("i".to_string())),
                },
            })],
        };
        eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert_eq!(bindings.get("total"), Some(&Value::Int(6)));
    }

    #[test]
    fn stmt_for_range() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        bindings.insert("total".to_string(), Value::Int(0));
        // for i in 0..3 { total := total + i }
        let stmt = Stmt::For {
            var: "i".to_string(),
            iter: Expr::Range {
                start: Box::new(Expr::Int(0)),
                end: Box::new(Expr::Int(3)),
                inclusive: false,
            },
            body: vec![Spanned::dummy(Stmt::Assignment {
                name: "total".to_string(),
                value: Expr::Binary {
                    op: BinOp::Add,
                    left: Box::new(Expr::Ident("total".to_string())),
                    right: Box::new(Expr::Ident("i".to_string())),
                },
            })],
        };
        eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        // 0 + 1 + 2 = 3
        assert_eq!(bindings.get("total"), Some(&Value::Int(3)));
    }

    #[test]
    fn stmt_for_range_inclusive() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        bindings.insert("total".to_string(), Value::Int(0));
        // for i in 0..=3 { total := total + i }
        let stmt = Stmt::For {
            var: "i".to_string(),
            iter: Expr::Range {
                start: Box::new(Expr::Int(0)),
                end: Box::new(Expr::Int(3)),
                inclusive: true,
            },
            body: vec![Spanned::dummy(Stmt::Assignment {
                name: "total".to_string(),
                value: Expr::Binary {
                    op: BinOp::Add,
                    left: Box::new(Expr::Ident("total".to_string())),
                    right: Box::new(Expr::Ident("i".to_string())),
                },
            })],
        };
        eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        // 0 + 1 + 2 + 3 = 6
        assert_eq!(bindings.get("total"), Some(&Value::Int(6)));
    }

    #[test]
    fn stmt_while_loop() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = empty_bindings();
        bindings.insert("i".to_string(), Value::Int(0));
        // while i < 5 { i := i + 1 }
        let stmt = Stmt::While {
            cond: Expr::Binary {
                op: BinOp::Lt,
                left: Box::new(Expr::Ident("i".to_string())),
                right: Box::new(Expr::Int(5)),
            },
            body: vec![Spanned::dummy(Stmt::Assignment {
                name: "i".to_string(),
                value: Expr::Binary {
                    op: BinOp::Add,
                    left: Box::new(Expr::Ident("i".to_string())),
                    right: Box::new(Expr::Int(1)),
                },
            })],
        };
        eval_stmt(&stmt, &event, &ctx, &functions, &mut bindings);
        assert_eq!(bindings.get("i"), Some(&Value::Int(5)));
    }
}

// =============================================================================
// 8. call_user_function
// =============================================================================

mod user_function_tests {
    use super::*;

    #[test]
    fn call_simple_function() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();

        let func = UserFunction {
            name: "double".to_string(),
            params: vec![("x".to_string(), Type::Int)],
            return_type: Some(Type::Int),
            body: vec![Spanned::dummy(Stmt::Expr(Expr::Binary {
                op: BinOp::Mul,
                left: Box::new(Expr::Ident("x".to_string())),
                right: Box::new(Expr::Int(2)),
            }))],
        };

        let result = call_user_function(&func, &[Value::Int(21)], &event, &ctx, &functions);
        assert_eq!(result, Some(Value::Int(42)));
    }

    #[test]
    fn call_function_with_return() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();

        let func = UserFunction {
            name: "early_return".to_string(),
            params: vec![("x".to_string(), Type::Int)],
            return_type: Some(Type::Int),
            body: vec![
                Spanned::dummy(Stmt::Return(Some(Expr::Binary {
                    op: BinOp::Add,
                    left: Box::new(Expr::Ident("x".to_string())),
                    right: Box::new(Expr::Int(100)),
                }))),
                Spanned::dummy(Stmt::Expr(Expr::Int(999))), // should not reach
            ],
        };

        let result = call_user_function(&func, &[Value::Int(5)], &event, &ctx, &functions);
        assert_eq!(result, Some(Value::Int(105)));
    }

    #[test]
    fn call_function_no_params() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();

        let func = UserFunction {
            name: "constant".to_string(),
            params: vec![],
            return_type: Some(Type::Int),
            body: vec![Spanned::dummy(Stmt::Expr(Expr::Int(42)))],
        };

        let result = call_user_function(&func, &[], &event, &ctx, &functions);
        assert_eq!(result, Some(Value::Int(42)));
    }

    #[test]
    fn call_function_multiple_params() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();

        let func = UserFunction {
            name: "add".to_string(),
            params: vec![("a".to_string(), Type::Int), ("b".to_string(), Type::Int)],
            return_type: Some(Type::Int),
            body: vec![Spanned::dummy(Stmt::Expr(Expr::Binary {
                op: BinOp::Add,
                left: Box::new(Expr::Ident("a".to_string())),
                right: Box::new(Expr::Ident("b".to_string())),
            }))],
        };

        let result = call_user_function(
            &func,
            &[Value::Int(3), Value::Int(7)],
            &event,
            &ctx,
            &functions,
        );
        assert_eq!(result, Some(Value::Int(10)));
    }
}

// =============================================================================
// 9. eval_expr_ctx
// =============================================================================

mod eval_ctx_tests {
    use super::*;

    #[test]
    fn eval_ctx_basic() {
        let event = Event::new("Test").with_field("x", 42i64);
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();

        let eval_context = EvalContext::new(&event, &ctx, &functions, &bindings);
        let expr = Expr::Ident("x".to_string());
        let r = eval_expr_ctx(&expr, &eval_context);
        assert_eq!(r, Some(Value::Int(42)));
    }

    #[test]
    fn eval_ctx_with_bindings() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let mut bindings = FxHashMap::default();
        bindings.insert("y".to_string(), Value::Float(3.25));

        let eval_context = EvalContext::new(&event, &ctx, &functions, &bindings);
        let expr = Expr::Ident("y".to_string());
        let r = eval_expr_ctx(&expr, &eval_context);
        assert_eq!(r, Some(Value::Float(3.25)));
    }

    #[test]
    fn eval_ctx_binary() {
        let event = empty_event();
        let ctx = empty_seq_ctx();
        let functions = empty_functions();
        let bindings = empty_bindings();

        let eval_context = EvalContext::new(&event, &ctx, &functions, &bindings);
        let expr = Expr::Binary {
            op: BinOp::Add,
            left: Box::new(Expr::Int(10)),
            right: Box::new(Expr::Int(20)),
        };
        let r = eval_expr_ctx(&expr, &eval_context);
        assert_eq!(r, Some(Value::Int(30)));
    }
}
