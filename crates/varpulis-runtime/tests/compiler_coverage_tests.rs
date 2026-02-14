//! Comprehensive tests for the Varpulis engine compiler functions.
//!
//! Tests cover:
//! - `compile_agg_expr`: aggregate expression compilation
//! - `expr_to_sase_predicate`: VPL expression to SASE predicate conversion
//! - `compile_sase_pattern_expr`: SasePatternExpr to SasePattern compilation
//! - `extract_event_types_from_pattern_expr`: event type extraction from patterns
//! - `compile_to_sase_pattern_with_resolver`: full SASE pattern compilation with stream resolution

use std::time::Duration;
use varpulis_core::ast::*;
use varpulis_core::Value;
use varpulis_runtime::engine::compiler::*;
use varpulis_runtime::sase::{CompareOp, Predicate, SasePattern};

// =============================================================================
// Helper functions
// =============================================================================

/// Build a simple Expr::Call node: func(args...)
fn call_expr(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Call {
        func: Box::new(Expr::Ident(name.to_string())),
        args: args.into_iter().map(Arg::Positional).collect(),
    }
}

/// Build an Expr::Ident
fn ident(name: &str) -> Expr {
    Expr::Ident(name.to_string())
}

/// Build a binary expression
fn binary(op: BinOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// A no-op stream resolver that always returns None
fn no_resolve(_name: &str) -> Option<DerivedStreamInfo> {
    None
}

// =============================================================================
// 1. compile_agg_expr tests
// =============================================================================

#[test]
fn compile_agg_expr_count() {
    let expr = call_expr("count", vec![]);
    let result = compile_agg_expr(&expr);
    assert!(result.is_some());
    let (func, field) = result.unwrap();
    assert_eq!(func.name(), "count");
    assert!(field.is_none());
}

#[test]
fn compile_agg_expr_sum() {
    let expr = call_expr("sum", vec![ident("price")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "sum");
    assert_eq!(field.as_deref(), Some("price"));
}

#[test]
fn compile_agg_expr_avg() {
    let expr = call_expr("avg", vec![ident("temperature")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "avg");
    assert_eq!(field.as_deref(), Some("temperature"));
}

#[test]
fn compile_agg_expr_min() {
    let expr = call_expr("min", vec![ident("latency")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "min");
    assert_eq!(field.as_deref(), Some("latency"));
}

#[test]
fn compile_agg_expr_max() {
    let expr = call_expr("max", vec![ident("latency")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "max");
    assert_eq!(field.as_deref(), Some("latency"));
}

#[test]
fn compile_agg_expr_last() {
    let expr = call_expr("last", vec![ident("value")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "last");
    assert_eq!(field.as_deref(), Some("value"));
}

#[test]
fn compile_agg_expr_first() {
    let expr = call_expr("first", vec![ident("value")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "first");
    assert_eq!(field.as_deref(), Some("value"));
}

#[test]
fn compile_agg_expr_stddev() {
    let expr = call_expr("stddev", vec![ident("measurement")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "stddev");
    assert_eq!(field.as_deref(), Some("measurement"));
}

#[test]
fn compile_agg_expr_ema_with_period() {
    // ema(price, 20) — explicit period
    let expr = Expr::Call {
        func: Box::new(ident("ema")),
        args: vec![
            Arg::Positional(ident("price")),
            Arg::Positional(Expr::Int(20)),
        ],
    };
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "ema");
    assert_eq!(field.as_deref(), Some("price"));
}

#[test]
fn compile_agg_expr_ema_default_period() {
    // ema(price) — default period (12)
    let expr = call_expr("ema", vec![ident("price")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "ema");
    assert_eq!(field.as_deref(), Some("price"));
}

#[test]
fn compile_agg_expr_count_distinct() {
    let expr = call_expr("count_distinct", vec![ident("user_id")]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "count_distinct");
    assert_eq!(field.as_deref(), Some("user_id"));
}

#[test]
fn compile_agg_expr_count_distinct_nested() {
    // count(distinct(user_id))
    let inner_call = call_expr("distinct", vec![ident("user_id")]);
    let expr = call_expr("count", vec![inner_call]);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "count_distinct");
    assert_eq!(field.as_deref(), Some("user_id"));
}

#[test]
fn compile_agg_expr_binary_sub() {
    // last(x) - ema(x, 9)
    let left = call_expr("last", vec![ident("x")]);
    let right = Expr::Call {
        func: Box::new(ident("ema")),
        args: vec![Arg::Positional(ident("x")), Arg::Positional(Expr::Int(9))],
    };
    let expr = binary(BinOp::Sub, left, right);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "expr");
    // Binary aggregate returns None for field
    assert!(field.is_none());
}

#[test]
fn compile_agg_expr_binary_add() {
    // sum(x) + avg(x)
    let left = call_expr("sum", vec![ident("x")]);
    let right = call_expr("avg", vec![ident("x")]);
    let expr = binary(BinOp::Add, left, right);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "expr");
    assert!(field.is_none());
}

#[test]
fn compile_agg_expr_binary_mul() {
    // count() * avg(price)
    let left = call_expr("count", vec![]);
    let right = call_expr("avg", vec![ident("price")]);
    let expr = binary(BinOp::Mul, left, right);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "expr");
    assert!(field.is_none());
}

#[test]
fn compile_agg_expr_binary_div() {
    // sum(value) / count()
    let left = call_expr("sum", vec![ident("value")]);
    let right = call_expr("count", vec![]);
    let expr = binary(BinOp::Div, left, right);
    let (func, field) = compile_agg_expr(&expr).unwrap();
    assert_eq!(func.name(), "expr");
    assert!(field.is_none());
}

#[test]
fn compile_agg_expr_unknown_function_returns_none() {
    let expr = call_expr("median", vec![ident("x")]);
    assert!(compile_agg_expr(&expr).is_none());
}

#[test]
fn compile_agg_expr_unsupported_binary_op_returns_none() {
    // last(x) % ema(x, 9) — Mod is not a supported AggBinOp
    let left = call_expr("last", vec![ident("x")]);
    let right = call_expr("ema", vec![ident("x")]);
    let expr = binary(BinOp::Mod, left, right);
    assert!(compile_agg_expr(&expr).is_none());
}

#[test]
fn compile_agg_expr_unsupported_expr_returns_none() {
    // A literal is not a valid aggregate expression
    let expr = Expr::Int(42);
    assert!(compile_agg_expr(&expr).is_none());
}

// =============================================================================
// 2. expr_to_sase_predicate tests
// =============================================================================

#[test]
fn predicate_field_eq_int() {
    // temperature == 100
    let expr = binary(BinOp::Eq, ident("temperature"), Expr::Int(100));
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Compare { field, op, value } => {
            assert_eq!(field, "temperature");
            assert_eq!(op, CompareOp::Eq);
            assert_eq!(value, Value::Int(100));
        }
        _ => panic!("Expected Predicate::Compare, got {:?}", pred),
    }
}

#[test]
fn predicate_field_eq_float() {
    let expr = binary(BinOp::Eq, ident("price"), Expr::Float(99.5));
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Compare { field, op, value } => {
            assert_eq!(field, "price");
            assert_eq!(op, CompareOp::Eq);
            assert_eq!(value, Value::Float(99.5));
        }
        _ => panic!("Expected Predicate::Compare"),
    }
}

#[test]
fn predicate_field_eq_str() {
    let expr = binary(BinOp::Eq, ident("status"), Expr::Str("active".to_string()));
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Compare { field, op, value } => {
            assert_eq!(field, "status");
            assert_eq!(op, CompareOp::Eq);
            assert_eq!(value, Value::Str("active".into()));
        }
        _ => panic!("Expected Predicate::Compare"),
    }
}

#[test]
fn predicate_field_eq_bool() {
    let expr = binary(BinOp::Eq, ident("active"), Expr::Bool(true));
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Compare { field, op, value } => {
            assert_eq!(field, "active");
            assert_eq!(op, CompareOp::Eq);
            assert_eq!(value, Value::Bool(true));
        }
        _ => panic!("Expected Predicate::Compare"),
    }
}

#[test]
fn predicate_all_comparison_operators() {
    let ops_and_expected = vec![
        (BinOp::Eq, CompareOp::Eq),
        (BinOp::NotEq, CompareOp::NotEq),
        (BinOp::Lt, CompareOp::Lt),
        (BinOp::Le, CompareOp::Le),
        (BinOp::Gt, CompareOp::Gt),
        (BinOp::Ge, CompareOp::Ge),
    ];

    for (bin_op, expected_cmp) in ops_and_expected {
        let expr = binary(bin_op, ident("x"), Expr::Int(42));
        let pred = expr_to_sase_predicate(&expr).unwrap();
        match pred {
            Predicate::Compare { op, .. } => {
                assert_eq!(
                    op, expected_cmp,
                    "BinOp {:?} should map to {:?}",
                    bin_op, expected_cmp
                );
            }
            _ => panic!("Expected Predicate::Compare for BinOp::{:?}", bin_op),
        }
    }
}

#[test]
fn predicate_and_logical() {
    // temperature > 50 and status == "active"
    let left = binary(BinOp::Gt, ident("temperature"), Expr::Int(50));
    let right = binary(BinOp::Eq, ident("status"), Expr::Str("active".to_string()));
    let expr = binary(BinOp::And, left, right);
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::And(l, r) => {
            match l.as_ref() {
                Predicate::Compare { field, op, .. } => {
                    assert_eq!(field, "temperature");
                    assert_eq!(*op, CompareOp::Gt);
                }
                _ => panic!("Expected left to be Compare"),
            }
            match r.as_ref() {
                Predicate::Compare { field, op, .. } => {
                    assert_eq!(field, "status");
                    assert_eq!(*op, CompareOp::Eq);
                }
                _ => panic!("Expected right to be Compare"),
            }
        }
        _ => panic!("Expected Predicate::And"),
    }
}

#[test]
fn predicate_or_logical() {
    // status == "error" or status == "warning"
    let left = binary(BinOp::Eq, ident("status"), Expr::Str("error".to_string()));
    let right = binary(BinOp::Eq, ident("status"), Expr::Str("warning".to_string()));
    let expr = binary(BinOp::Or, left, right);
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Or(l, r) => {
            match l.as_ref() {
                Predicate::Compare { value, .. } => {
                    assert_eq!(*value, Value::Str("error".into()));
                }
                _ => panic!("Expected left to be Compare"),
            }
            match r.as_ref() {
                Predicate::Compare { value, .. } => {
                    assert_eq!(*value, Value::Str("warning".into()));
                }
                _ => panic!("Expected right to be Compare"),
            }
        }
        _ => panic!("Expected Predicate::Or"),
    }
}

#[test]
fn predicate_cross_event_reference() {
    // order_id == order.id
    let left = ident("order_id");
    let right = Expr::Member {
        expr: Box::new(ident("order")),
        member: "id".to_string(),
    };
    let expr = binary(BinOp::Eq, left, right);
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::CompareRef {
            field,
            op,
            ref_alias,
            ref_field,
        } => {
            assert_eq!(field, "order_id");
            assert_eq!(op, CompareOp::Eq);
            assert_eq!(ref_alias, "order");
            assert_eq!(ref_field, "id");
        }
        _ => panic!("Expected Predicate::CompareRef, got {:?}", pred),
    }
}

#[test]
fn predicate_unary_not() {
    // not(temperature > 100)
    let inner = binary(BinOp::Gt, ident("temperature"), Expr::Int(100));
    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(inner),
    };
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Not(inner) => match inner.as_ref() {
            Predicate::Compare { field, op, .. } => {
                assert_eq!(field, "temperature");
                assert_eq!(*op, CompareOp::Gt);
            }
            _ => panic!("Expected inner to be Compare"),
        },
        _ => panic!("Expected Predicate::Not"),
    }
}

#[test]
fn predicate_complex_left_side_falls_back_to_expr() {
    // (a + b) > 10 — complex left side falls back to Predicate::Expr
    let left = binary(BinOp::Add, ident("a"), ident("b"));
    let expr = binary(BinOp::Gt, left, Expr::Int(10));
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Expr(_) => {} // Expected fallback
        _ => panic!("Expected Predicate::Expr fallback for complex left side"),
    }
}

#[test]
fn predicate_complex_right_side_falls_back_to_expr() {
    // x == y (right side is ident, not literal) — falls back to Predicate::Expr
    let expr = binary(BinOp::Eq, ident("x"), ident("y"));
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Expr(_) => {} // Expected: ident on right side is not a literal value
        _ => panic!("Expected Predicate::Expr fallback when right side is ident"),
    }
}

#[test]
fn predicate_literal_falls_back_to_expr() {
    // A bare literal expression should fall back to Predicate::Expr
    let expr = Expr::Bool(true);
    let pred = expr_to_sase_predicate(&expr).unwrap();
    match pred {
        Predicate::Expr(e) => {
            assert_eq!(*e, Expr::Bool(true));
        }
        _ => panic!("Expected Predicate::Expr for bare literal"),
    }
}

// =============================================================================
// 3. compile_sase_pattern_expr tests
// =============================================================================

#[test]
fn pattern_expr_event() {
    let expr = SasePatternExpr::Event("Temperature".to_string());
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    match pattern {
        SasePattern::Event {
            event_type,
            predicate,
            alias,
        } => {
            assert_eq!(event_type, "Temperature");
            assert!(predicate.is_none());
            assert!(alias.is_none());
        }
        _ => panic!("Expected SasePattern::Event"),
    }
}

#[test]
fn pattern_expr_seq_single_item() {
    let item = SasePatternItem {
        event_type: "OrderCreated".to_string(),
        alias: Some("o".to_string()),
        kleene: None,
        filter: None,
    };
    let expr = SasePatternExpr::Seq(vec![item]);
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    // Single item in seq should be unwrapped
    match pattern {
        SasePattern::Event {
            event_type, alias, ..
        } => {
            assert_eq!(event_type, "OrderCreated");
            assert_eq!(alias.as_deref(), Some("o"));
        }
        _ => panic!("Expected single item seq to be unwrapped to Event"),
    }
}

#[test]
fn pattern_expr_seq_multiple_items() {
    let items = vec![
        SasePatternItem {
            event_type: "A".to_string(),
            alias: Some("a".to_string()),
            kleene: None,
            filter: None,
        },
        SasePatternItem {
            event_type: "B".to_string(),
            alias: Some("b".to_string()),
            kleene: None,
            filter: None,
        },
        SasePatternItem {
            event_type: "C".to_string(),
            alias: Some("c".to_string()),
            kleene: None,
            filter: None,
        },
    ];
    let expr = SasePatternExpr::Seq(items);
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    match pattern {
        SasePattern::Seq(steps) => {
            assert_eq!(steps.len(), 3);
            // Verify each step
            for (i, expected_type) in ["A", "B", "C"].iter().enumerate() {
                match &steps[i] {
                    SasePattern::Event { event_type, .. } => {
                        assert_eq!(event_type, *expected_type);
                    }
                    _ => panic!("Expected Event in seq step {}", i),
                }
            }
        }
        _ => panic!("Expected SasePattern::Seq"),
    }
}

#[test]
fn pattern_expr_and() {
    let left = SasePatternExpr::Event("A".to_string());
    let right = SasePatternExpr::Event("B".to_string());
    let expr = SasePatternExpr::And(Box::new(left), Box::new(right));
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    match pattern {
        SasePattern::And(l, r) => {
            match l.as_ref() {
                SasePattern::Event { event_type, .. } => assert_eq!(event_type, "A"),
                _ => panic!("Expected Event on left"),
            }
            match r.as_ref() {
                SasePattern::Event { event_type, .. } => assert_eq!(event_type, "B"),
                _ => panic!("Expected Event on right"),
            }
        }
        _ => panic!("Expected SasePattern::And"),
    }
}

#[test]
fn pattern_expr_or() {
    let left = SasePatternExpr::Event("A".to_string());
    let right = SasePatternExpr::Event("B".to_string());
    let expr = SasePatternExpr::Or(Box::new(left), Box::new(right));
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    match pattern {
        SasePattern::Or(l, r) => {
            match l.as_ref() {
                SasePattern::Event { event_type, .. } => assert_eq!(event_type, "A"),
                _ => panic!("Expected Event on left"),
            }
            match r.as_ref() {
                SasePattern::Event { event_type, .. } => assert_eq!(event_type, "B"),
                _ => panic!("Expected Event on right"),
            }
        }
        _ => panic!("Expected SasePattern::Or"),
    }
}

#[test]
fn pattern_expr_not() {
    let inner = SasePatternExpr::Event("Cancelled".to_string());
    let expr = SasePatternExpr::Not(Box::new(inner));
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    match pattern {
        SasePattern::Not(inner) => match inner.as_ref() {
            SasePattern::Event { event_type, .. } => assert_eq!(event_type, "Cancelled"),
            _ => panic!("Expected Event inside Not"),
        },
        _ => panic!("Expected SasePattern::Not"),
    }
}

#[test]
fn pattern_expr_group_passes_through() {
    let inner = SasePatternExpr::Event("Tick".to_string());
    let expr = SasePatternExpr::Group(Box::new(inner));
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    // Group should pass through to inner
    match pattern {
        SasePattern::Event { event_type, .. } => assert_eq!(event_type, "Tick"),
        _ => panic!("Expected Group to unwrap to Event"),
    }
}

#[test]
fn pattern_expr_with_within_duration() {
    let expr = SasePatternExpr::Event("Temperature".to_string());
    let duration = Duration::from_secs(30);
    let pattern = compile_sase_pattern_expr(&expr, Some(duration)).unwrap();
    match pattern {
        SasePattern::Within(inner, dur) => {
            assert_eq!(dur, Duration::from_secs(30));
            match inner.as_ref() {
                SasePattern::Event { event_type, .. } => assert_eq!(event_type, "Temperature"),
                _ => panic!("Expected Event inside Within"),
            }
        }
        _ => panic!("Expected SasePattern::Within"),
    }
}

#[test]
fn pattern_expr_group_with_within_duration() {
    // Group should forward the within duration
    let inner = SasePatternExpr::Event("Tick".to_string());
    let expr = SasePatternExpr::Group(Box::new(inner));
    let duration = Duration::from_secs(60);
    let pattern = compile_sase_pattern_expr(&expr, Some(duration)).unwrap();
    match pattern {
        SasePattern::Within(inner, dur) => {
            assert_eq!(dur, Duration::from_secs(60));
            match inner.as_ref() {
                SasePattern::Event { event_type, .. } => assert_eq!(event_type, "Tick"),
                _ => panic!("Expected Event inside Within"),
            }
        }
        _ => panic!("Expected SasePattern::Within when Group has within"),
    }
}

#[test]
fn pattern_expr_kleene_plus() {
    let item = SasePatternItem {
        event_type: "Trade".to_string(),
        alias: Some("t".to_string()),
        kleene: Some(KleeneOp::Plus),
        filter: None,
    };
    let expr = SasePatternExpr::Seq(vec![item]);
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    // Single item in seq should be unwrapped, and it should be KleenePlus
    match pattern {
        SasePattern::KleenePlus(inner) => match inner.as_ref() {
            SasePattern::Event { event_type, .. } => assert_eq!(event_type, "Trade"),
            _ => panic!("Expected Event inside KleenePlus"),
        },
        _ => panic!("Expected SasePattern::KleenePlus"),
    }
}

#[test]
fn pattern_expr_kleene_star() {
    let item = SasePatternItem {
        event_type: "Tick".to_string(),
        alias: None,
        kleene: Some(KleeneOp::Star),
        filter: None,
    };
    let expr = SasePatternExpr::Seq(vec![item]);
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    match pattern {
        SasePattern::KleeneStar(inner) => match inner.as_ref() {
            SasePattern::Event { event_type, .. } => assert_eq!(event_type, "Tick"),
            _ => panic!("Expected Event inside KleeneStar"),
        },
        _ => panic!("Expected SasePattern::KleeneStar"),
    }
}

#[test]
fn pattern_expr_kleene_optional() {
    let item = SasePatternItem {
        event_type: "Ack".to_string(),
        alias: None,
        kleene: Some(KleeneOp::Optional),
        filter: None,
    };
    let expr = SasePatternExpr::Seq(vec![item]);
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    // Optional maps to KleeneStar
    match pattern {
        SasePattern::KleeneStar(inner) => match inner.as_ref() {
            SasePattern::Event { event_type, .. } => assert_eq!(event_type, "Ack"),
            _ => panic!("Expected Event inside KleeneStar (optional)"),
        },
        _ => panic!("Expected SasePattern::KleeneStar for Optional"),
    }
}

#[test]
fn pattern_expr_seq_with_filter() {
    let filter = binary(BinOp::Gt, ident("price"), Expr::Float(100.0));
    let item = SasePatternItem {
        event_type: "Trade".to_string(),
        alias: Some("t".to_string()),
        kleene: None,
        filter: Some(filter),
    };
    let expr = SasePatternExpr::Seq(vec![item]);
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    match pattern {
        SasePattern::Event {
            event_type,
            predicate,
            ..
        } => {
            assert_eq!(event_type, "Trade");
            assert!(predicate.is_some());
            match predicate.unwrap() {
                Predicate::Compare { field, op, value } => {
                    assert_eq!(field, "price");
                    assert_eq!(op, CompareOp::Gt);
                    assert_eq!(value, Value::Float(100.0));
                }
                other => panic!("Expected Compare predicate, got {:?}", other),
            }
        }
        _ => panic!("Expected SasePattern::Event with predicate"),
    }
}

#[test]
fn pattern_expr_seq_mixed_kleene_and_plain() {
    let items = vec![
        SasePatternItem {
            event_type: "Start".to_string(),
            alias: Some("s".to_string()),
            kleene: None,
            filter: None,
        },
        SasePatternItem {
            event_type: "Middle".to_string(),
            alias: Some("m".to_string()),
            kleene: Some(KleeneOp::Plus),
            filter: None,
        },
        SasePatternItem {
            event_type: "End".to_string(),
            alias: Some("e".to_string()),
            kleene: None,
            filter: None,
        },
    ];
    let expr = SasePatternExpr::Seq(items);
    let pattern = compile_sase_pattern_expr(&expr, None).unwrap();
    match pattern {
        SasePattern::Seq(steps) => {
            assert_eq!(steps.len(), 3);
            // First: plain event
            assert!(
                matches!(&steps[0], SasePattern::Event { event_type, .. } if event_type == "Start")
            );
            // Second: KleenePlus
            assert!(matches!(&steps[1], SasePattern::KleenePlus(_)));
            // Third: plain event
            assert!(
                matches!(&steps[2], SasePattern::Event { event_type, .. } if event_type == "End")
            );
        }
        _ => panic!("Expected SasePattern::Seq"),
    }
}

// =============================================================================
// 4. extract_event_types_from_pattern_expr tests
// =============================================================================

#[test]
fn extract_types_from_seq() {
    let items = vec![
        SasePatternItem {
            event_type: "A".to_string(),
            alias: None,
            kleene: None,
            filter: None,
        },
        SasePatternItem {
            event_type: "B".to_string(),
            alias: None,
            kleene: None,
            filter: None,
        },
        SasePatternItem {
            event_type: "C".to_string(),
            alias: None,
            kleene: None,
            filter: None,
        },
    ];
    let expr = SasePatternExpr::Seq(items);
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["A", "B", "C"]);
}

#[test]
fn extract_types_from_seq_deduplicates() {
    let items = vec![
        SasePatternItem {
            event_type: "A".to_string(),
            alias: None,
            kleene: None,
            filter: None,
        },
        SasePatternItem {
            event_type: "A".to_string(),
            alias: None,
            kleene: None,
            filter: None,
        },
        SasePatternItem {
            event_type: "B".to_string(),
            alias: None,
            kleene: None,
            filter: None,
        },
    ];
    let expr = SasePatternExpr::Seq(items);
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["A", "B"]);
}

#[test]
fn extract_types_from_and() {
    let left = SasePatternExpr::Event("Order".to_string());
    let right = SasePatternExpr::Event("Payment".to_string());
    let expr = SasePatternExpr::And(Box::new(left), Box::new(right));
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["Order", "Payment"]);
}

#[test]
fn extract_types_from_or() {
    let left = SasePatternExpr::Event("Success".to_string());
    let right = SasePatternExpr::Event("Failure".to_string());
    let expr = SasePatternExpr::Or(Box::new(left), Box::new(right));
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["Success", "Failure"]);
}

#[test]
fn extract_types_from_not() {
    let inner = SasePatternExpr::Event("Cancelled".to_string());
    let expr = SasePatternExpr::Not(Box::new(inner));
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["Cancelled"]);
}

#[test]
fn extract_types_from_group() {
    let inner = SasePatternExpr::Event("Tick".to_string());
    let expr = SasePatternExpr::Group(Box::new(inner));
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["Tick"]);
}

#[test]
fn extract_types_from_event() {
    let expr = SasePatternExpr::Event("Temperature".to_string());
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["Temperature"]);
}

#[test]
fn extract_types_from_and_deduplicates() {
    let left = SasePatternExpr::Event("X".to_string());
    let right = SasePatternExpr::Event("X".to_string());
    let expr = SasePatternExpr::And(Box::new(left), Box::new(right));
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["X"]);
}

#[test]
fn extract_types_nested_and_or() {
    // AND(OR(A, B), C)
    let a = SasePatternExpr::Event("A".to_string());
    let b = SasePatternExpr::Event("B".to_string());
    let or_ab = SasePatternExpr::Or(Box::new(a), Box::new(b));
    let c = SasePatternExpr::Event("C".to_string());
    let expr = SasePatternExpr::And(Box::new(or_ab), Box::new(c));
    let types = extract_event_types_from_pattern_expr(&expr);
    assert_eq!(types, vec!["A", "B", "C"]);
}

// =============================================================================
// 5. compile_to_sase_pattern_with_resolver tests
// =============================================================================

#[test]
fn resolver_ident_source() {
    let source = StreamSource::Ident("Temperature".to_string());
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &no_resolve).unwrap();
    match pattern {
        SasePattern::Event {
            event_type,
            predicate,
            alias,
        } => {
            assert_eq!(event_type, "Temperature");
            assert!(predicate.is_none());
            assert!(alias.is_none());
        }
        _ => panic!("Expected SasePattern::Event"),
    }
}

#[test]
fn resolver_ident_with_alias_source() {
    let source = StreamSource::IdentWithAlias {
        name: "Trade".to_string(),
        alias: "t".to_string(),
    };
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &no_resolve).unwrap();
    match pattern {
        SasePattern::Event {
            event_type, alias, ..
        } => {
            assert_eq!(event_type, "Trade");
            assert_eq!(alias.as_deref(), Some("t"));
        }
        _ => panic!("Expected SasePattern::Event with alias"),
    }
}

#[test]
fn resolver_all_with_alias_source() {
    let source = StreamSource::AllWithAlias {
        name: "Tick".to_string(),
        alias: Some("ticks".to_string()),
    };
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &no_resolve).unwrap();
    match pattern {
        SasePattern::KleenePlus(inner) => match inner.as_ref() {
            SasePattern::Event {
                event_type, alias, ..
            } => {
                assert_eq!(event_type, "Tick");
                assert_eq!(alias.as_deref(), Some("ticks"));
            }
            _ => panic!("Expected Event inside KleenePlus"),
        },
        _ => panic!("Expected SasePattern::KleenePlus for AllWithAlias"),
    }
}

#[test]
fn resolver_sequence_source() {
    let decl = SequenceDecl {
        match_all: false,
        timeout: None,
        steps: vec![
            SequenceStepDecl {
                alias: "start".to_string(),
                event_type: "Login".to_string(),
                filter: None,
                timeout: None,
            },
            SequenceStepDecl {
                alias: "end".to_string(),
                event_type: "Logout".to_string(),
                filter: None,
                timeout: None,
            },
        ],
    };
    let source = StreamSource::Sequence(decl);
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &no_resolve).unwrap();
    match pattern {
        SasePattern::Seq(steps) => {
            assert_eq!(steps.len(), 2);
            match &steps[0] {
                SasePattern::Event {
                    event_type, alias, ..
                } => {
                    assert_eq!(event_type, "Login");
                    assert_eq!(alias.as_deref(), Some("start"));
                }
                _ => panic!("Expected Event for first step"),
            }
            match &steps[1] {
                SasePattern::Event {
                    event_type, alias, ..
                } => {
                    assert_eq!(event_type, "Logout");
                    assert_eq!(alias.as_deref(), Some("end"));
                }
                _ => panic!("Expected Event for second step"),
            }
        }
        _ => panic!("Expected SasePattern::Seq for Sequence source"),
    }
}

#[test]
fn resolver_with_followed_by_clauses() {
    let source = StreamSource::Ident("OrderCreated".to_string());
    let clauses = vec![
        FollowedByClause {
            event_type: "PaymentReceived".to_string(),
            filter: Some(binary(BinOp::Gt, ident("amount"), Expr::Float(0.0))),
            alias: Some("p".to_string()),
            match_all: false,
        },
        FollowedByClause {
            event_type: "OrderShipped".to_string(),
            filter: None,
            alias: Some("s".to_string()),
            match_all: false,
        },
    ];
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &clauses, &[], None, &no_resolve).unwrap();
    match pattern {
        SasePattern::Seq(steps) => {
            assert_eq!(steps.len(), 3);
            // First step: OrderCreated
            match &steps[0] {
                SasePattern::Event { event_type, .. } => {
                    assert_eq!(event_type, "OrderCreated");
                }
                _ => panic!("Expected Event"),
            }
            // Second step: PaymentReceived with predicate
            match &steps[1] {
                SasePattern::Event {
                    event_type,
                    predicate,
                    alias,
                } => {
                    assert_eq!(event_type, "PaymentReceived");
                    assert!(predicate.is_some());
                    assert_eq!(alias.as_deref(), Some("p"));
                }
                _ => panic!("Expected Event with predicate"),
            }
            // Third step: OrderShipped
            match &steps[2] {
                SasePattern::Event {
                    event_type,
                    predicate,
                    alias,
                } => {
                    assert_eq!(event_type, "OrderShipped");
                    assert!(predicate.is_none());
                    assert_eq!(alias.as_deref(), Some("s"));
                }
                _ => panic!("Expected Event"),
            }
        }
        _ => panic!("Expected SasePattern::Seq with followed_by"),
    }
}

#[test]
fn resolver_with_within_duration() {
    let source = StreamSource::Ident("Alert".to_string());
    let duration = Duration::from_secs(300);
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], Some(duration), &no_resolve)
            .unwrap();
    match pattern {
        SasePattern::Within(inner, dur) => {
            assert_eq!(dur, Duration::from_secs(300));
            match inner.as_ref() {
                SasePattern::Event { event_type, .. } => {
                    assert_eq!(event_type, "Alert");
                }
                _ => panic!("Expected Event inside Within"),
            }
        }
        _ => panic!("Expected SasePattern::Within"),
    }
}

#[test]
fn resolver_followed_by_match_all_creates_kleene_plus() {
    let source = StreamSource::Ident("Start".to_string());
    let clauses = vec![FollowedByClause {
        event_type: "Tick".to_string(),
        filter: None,
        alias: Some("ticks".to_string()),
        match_all: true,
    }];
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &clauses, &[], None, &no_resolve).unwrap();
    match pattern {
        SasePattern::Seq(steps) => {
            assert_eq!(steps.len(), 2);
            // Second step should be KleenePlus due to match_all
            match &steps[1] {
                SasePattern::KleenePlus(inner) => match inner.as_ref() {
                    SasePattern::Event {
                        event_type, alias, ..
                    } => {
                        assert_eq!(event_type, "Tick");
                        assert_eq!(alias.as_deref(), Some("ticks"));
                    }
                    _ => panic!("Expected Event inside KleenePlus"),
                },
                _ => panic!("Expected KleenePlus for match_all clause"),
            }
        }
        _ => panic!("Expected SasePattern::Seq"),
    }
}

#[test]
fn resolver_with_derived_stream_resolution() {
    // Create a resolver that maps "HighTemp" to Temperature with filter
    let resolver = |name: &str| -> Option<DerivedStreamInfo> {
        if name == "HighTemp" {
            Some(DerivedStreamInfo {
                event_type: "Temperature".to_string(),
                filter: Some(binary(BinOp::Gt, ident("value"), Expr::Float(100.0))),
            })
        } else {
            None
        }
    };

    let source = StreamSource::Ident("HighTemp".to_string());
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &resolver).unwrap();
    match pattern {
        SasePattern::Event {
            event_type,
            predicate,
            ..
        } => {
            assert_eq!(event_type, "Temperature");
            assert!(predicate.is_some());
            match predicate.unwrap() {
                Predicate::Compare { field, op, value } => {
                    assert_eq!(field, "value");
                    assert_eq!(op, CompareOp::Gt);
                    assert_eq!(value, Value::Float(100.0));
                }
                other => panic!("Expected Compare predicate, got {:?}", other),
            }
        }
        _ => panic!("Expected SasePattern::Event with resolved predicate"),
    }
}

#[test]
fn resolver_ident_with_alias_derived_stream() {
    let resolver = |name: &str| -> Option<DerivedStreamInfo> {
        if name == "BigOrders" {
            Some(DerivedStreamInfo {
                event_type: "Order".to_string(),
                filter: Some(binary(BinOp::Gt, ident("amount"), Expr::Float(1000.0))),
            })
        } else {
            None
        }
    };

    let source = StreamSource::IdentWithAlias {
        name: "BigOrders".to_string(),
        alias: "bo".to_string(),
    };
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &resolver).unwrap();
    match pattern {
        SasePattern::Event {
            event_type,
            predicate,
            alias,
        } => {
            assert_eq!(event_type, "Order");
            assert_eq!(alias.as_deref(), Some("bo"));
            assert!(predicate.is_some());
        }
        _ => panic!("Expected SasePattern::Event with alias and resolved predicate"),
    }
}

#[test]
fn resolver_all_with_alias_derived_stream() {
    let resolver = |name: &str| -> Option<DerivedStreamInfo> {
        if name == "FastTicks" {
            Some(DerivedStreamInfo {
                event_type: "Tick".to_string(),
                filter: Some(binary(BinOp::Lt, ident("latency"), Expr::Int(10))),
            })
        } else {
            None
        }
    };

    let source = StreamSource::AllWithAlias {
        name: "FastTicks".to_string(),
        alias: Some("ft".to_string()),
    };
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &resolver).unwrap();
    match pattern {
        SasePattern::KleenePlus(inner) => match inner.as_ref() {
            SasePattern::Event {
                event_type,
                predicate,
                alias,
            } => {
                assert_eq!(event_type, "Tick");
                assert_eq!(alias.as_deref(), Some("ft"));
                assert!(predicate.is_some());
            }
            _ => panic!("Expected Event inside KleenePlus"),
        },
        _ => panic!("Expected KleenePlus for AllWithAlias derived stream"),
    }
}

#[test]
fn resolver_followed_by_with_derived_stream_combines_predicates() {
    // Resolver that resolves "HighPrice" to Trade with price > 100
    let resolver = |name: &str| -> Option<DerivedStreamInfo> {
        if name == "HighPrice" {
            Some(DerivedStreamInfo {
                event_type: "Trade".to_string(),
                filter: Some(binary(BinOp::Gt, ident("price"), Expr::Float(100.0))),
            })
        } else {
            None
        }
    };

    let source = StreamSource::Ident("Start".to_string());
    let clauses = vec![FollowedByClause {
        event_type: "HighPrice".to_string(),
        filter: Some(binary(
            BinOp::Eq,
            ident("exchange"),
            Expr::Str("NYSE".to_string()),
        )),
        alias: Some("hp".to_string()),
        match_all: false,
    }];

    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &clauses, &[], None, &resolver).unwrap();

    match pattern {
        SasePattern::Seq(steps) => {
            assert_eq!(steps.len(), 2);
            // Second step should have combined predicates (And of stream filter + clause filter)
            match &steps[1] {
                SasePattern::Event {
                    event_type,
                    predicate,
                    alias,
                } => {
                    assert_eq!(event_type, "Trade");
                    assert_eq!(alias.as_deref(), Some("hp"));
                    // Should be And(stream_pred, clause_pred)
                    match predicate.as_ref().unwrap() {
                        Predicate::And(_, _) => {} // Correct: combined predicate
                        other => panic!(
                            "Expected And predicate combining stream and clause filters, got {:?}",
                            other
                        ),
                    }
                }
                _ => panic!("Expected Event"),
            }
        }
        _ => panic!("Expected Seq"),
    }
}

#[test]
fn resolver_unsupported_source_returns_none() {
    // Timer source is not supported in SASE pattern compilation
    let source = StreamSource::Timer(TimerDecl {
        interval: Expr::Duration(5_000_000_000), // 5s
        initial_delay: None,
    });
    let result = compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &no_resolve);
    assert!(result.is_none());
}

#[test]
fn resolver_sequence_with_filter_on_step() {
    let filter = binary(BinOp::Eq, ident("user_id"), Expr::Str("abc".to_string()));
    let decl = SequenceDecl {
        match_all: false,
        timeout: None,
        steps: vec![SequenceStepDecl {
            alias: "login".to_string(),
            event_type: "Login".to_string(),
            filter: Some(filter),
            timeout: None,
        }],
    };
    let source = StreamSource::Sequence(decl);
    let pattern =
        compile_to_sase_pattern_with_resolver(&source, &[], &[], None, &no_resolve).unwrap();
    match pattern {
        SasePattern::Event {
            event_type,
            predicate,
            alias,
        } => {
            assert_eq!(event_type, "Login");
            assert_eq!(alias.as_deref(), Some("login"));
            assert!(predicate.is_some());
        }
        _ => panic!("Expected SasePattern::Event for single-step sequence"),
    }
}
