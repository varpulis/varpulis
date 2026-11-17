//! Predicate evaluation and classification

use rustc_hash::FxHashMap;
use varpulis_core::{Event, Value};

use super::nfa::{Nfa, State};
use super::types::{CompareOp, Predicate, SharedEvent};
use crate::ExprEvaluator;

// ============================================================================
// PREDICATE EVALUATION
// ============================================================================

pub(crate) fn event_matches_state(
    _nfa: &Nfa,
    event: &Event,
    state: &State,
    captured: &FxHashMap<String, SharedEvent>,
    evaluator: Option<&dyn ExprEvaluator>,
) -> bool {
    if let Some(ref expected_type) = state.event_type {
        if &*event.event_type != expected_type {
            return false;
        }
    }

    if let Some(ref predicate) = state.predicate {
        if !eval_predicate(predicate, event, captured, evaluator) {
            return false;
        }
    }

    true
}

pub(crate) fn eval_predicate(
    predicate: &Predicate,
    event: &Event,
    captured: &FxHashMap<String, SharedEvent>,
    evaluator: Option<&dyn ExprEvaluator>,
) -> bool {
    match predicate {
        Predicate::Compare { field, op, value } => event
            .get(field)
            .is_some_and(|ev| compare_values(ev, value, *op)),
        Predicate::CompareRef {
            field,
            op,
            ref_alias,
            ref_field,
        } => {
            let event_value = event.get(field);
            let ref_value = captured.get(ref_alias).and_then(|e| e.get(ref_field));
            match (event_value, ref_value) {
                (Some(ev), Some(rv)) => compare_values(ev, rv, *op),
                _ => false,
            }
        }
        Predicate::And(left, right) => {
            eval_predicate(left, event, captured, evaluator)
                && eval_predicate(right, event, captured, evaluator)
        }
        Predicate::Or(left, right) => {
            eval_predicate(left, event, captured, evaluator)
                || eval_predicate(right, event, captured, evaluator)
        }
        Predicate::Not(inner) => !eval_predicate(inner, event, captured, evaluator),
        Predicate::Expr(expr) => {
            // Handle literal booleans directly (no evaluator needed)
            if let varpulis_core::ast::Expr::Bool(b) = expr.as_ref() {
                return *b;
            }
            if let Some(eval) = evaluator {
                eval.eval(expr, event, captured)
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            } else {
                // No evaluator available — Expr predicates cannot be evaluated
                false
            }
        }
    }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

pub(crate) fn compare_values(left: &Value, right: &Value, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => values_equal(left, right),
        CompareOp::NotEq => !values_equal(left, right),
        CompareOp::Lt => values_compare(left, right) == Some(std::cmp::Ordering::Less),
        CompareOp::Le => matches!(
            values_compare(left, right),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        CompareOp::Gt => values_compare(left, right) == Some(std::cmp::Ordering::Greater),
        CompareOp::Ge => matches!(
            values_compare(left, right),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
    }
}

/// Equality for SASE+ predicates.
///
/// Delegates to [`Value::vpl_eq`] so a `->` sequence step and a `.where()`
/// clause cannot disagree about the same predicate. This used to be a private
/// re-implementation that coerced int/float (where the expression evaluator did
/// not) and returned `false` for every non-scalar type (where the expression
/// evaluator compared them structurally) — two engines, two answers, for
/// predicates that appear in shipped detections in both forms.
fn values_equal(left: &Value, right: &Value) -> bool {
    left.vpl_eq(right)
}

fn values_compare(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Str(a), Value::Str(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

// ============================================================================
// SIGMOD 2014: DEFERRED PREDICATE CLASSIFICATION
// ============================================================================

/// Classification of a predicate relative to a Kleene closure variable.
///
/// - **Consistent**: The predicate references only the current event or constants
///   (can be evaluated eagerly during NFA traversal).
/// - **Inconsistent**: The predicate cross-references a *different* alias (e.g.,
///   `a[i].price > a[i-1].price`), which cannot be evaluated until all Kleene
///   events are known.  Must be postponed to the enumeration phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateClass {
    /// Predicate can be evaluated eagerly during NFA traversal.
    Consistent,
    /// Predicate requires cross-event data; postponed to enumeration.
    Inconsistent,
}

/// Classify a predicate as consistent or inconsistent for a Kleene state.
///
/// A predicate is *inconsistent* if it contains `CompareRef` nodes that
/// reference the same alias as the Kleene variable (cross-event comparison).
pub fn classify_predicate(pred: &Predicate, alias: Option<&str>) -> PredicateClass {
    match pred {
        Predicate::Compare { .. } => PredicateClass::Consistent,
        Predicate::CompareRef { ref_alias, .. } => {
            if alias.is_some_and(|a| a == ref_alias) {
                PredicateClass::Inconsistent
            } else {
                PredicateClass::Consistent
            }
        }
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            let lc = classify_predicate(l, alias);
            let rc = classify_predicate(r, alias);
            if lc == PredicateClass::Inconsistent || rc == PredicateClass::Inconsistent {
                PredicateClass::Inconsistent
            } else {
                PredicateClass::Consistent
            }
        }
        Predicate::Not(inner) => classify_predicate(inner, alias),
        Predicate::Expr(expr) => {
            if let Some(a) = alias {
                if expr_references_alias(expr, a) {
                    PredicateClass::Inconsistent
                } else {
                    PredicateClass::Consistent
                }
            } else {
                PredicateClass::Consistent
            }
        }
    }
}

/// Check if a predicate references a specific alias (used for Kleene self-reference detection).
pub fn predicate_references_alias(pred: &Predicate, alias: &str) -> bool {
    match pred {
        Predicate::CompareRef { ref_alias, .. } => ref_alias == alias,
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            predicate_references_alias(l, alias) || predicate_references_alias(r, alias)
        }
        Predicate::Not(inner) => predicate_references_alias(inner, alias),
        Predicate::Expr(expr) => expr_references_alias(expr, alias),
        Predicate::Compare { .. } => false,
    }
}

/// Check whether an inconsistent predicate truly needs deferred evaluation.
///
/// A self-reference to the closure's own alias — `field > alias.field` as a
/// `CompareRef`, or `field > alias.field + 0.5` as an `Expr` — is evaluated
/// eagerly against the previous closure event: `captured[alias]` is updated
/// to the last matched Kleene event by `push_at_kleene()`, and at the first
/// entry it is bound to the step before. That is the sliding-window reading
/// the patterns guide documents, and it holds for an expression as it does
/// for a bare comparison.
///
/// Nothing is deferred to enumeration any more. Deferral existed for an
/// expression that would read arbitrary positions of the closure, which no
/// expression can (the closure is not indexable while it matches), and the
/// deferred path evaluated an `Expr` with the alias unbound, so every
/// subset failed it.
pub fn needs_deferred_evaluation(pred: &Predicate) -> bool {
    match pred {
        Predicate::Compare { .. } | Predicate::CompareRef { .. } | Predicate::Expr(_) => false,
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            needs_deferred_evaluation(l) || needs_deferred_evaluation(r)
        }
        Predicate::Not(inner) => needs_deferred_evaluation(inner),
    }
}

/// Check whether a `varpulis_core::ast::Expr` reads a field of `alias`
/// (`alias.field`, `alias?.field`, `alias[i]`), anywhere inside it.
///
/// `alias.field` is a `Member` expression. This used to look for an
/// identifier spelled `alias.field`, which the parser never produces, so a
/// self-reference written with any arithmetic (`> rising.temperature + 0.5`)
/// went unrecognised: the closure's first entry evaluated it with the alias
/// unbound, it was false, and the closure never opened.
pub(crate) fn expr_references_alias(expr: &varpulis_core::ast::Expr, alias: &str) -> bool {
    use varpulis_core::ast::{Arg, Expr};
    let refs = |e: &Expr| expr_references_alias(e, alias);
    match expr {
        Expr::Ident(name) => {
            name.starts_with(alias) && name.as_bytes().get(alias.len()) == Some(&b'.')
        }
        Expr::Member { expr: object, .. } | Expr::OptionalMember { expr: object, .. } => {
            matches!(object.as_ref(), Expr::Ident(name) if name == alias) || refs(object)
        }
        Expr::Index {
            expr: object,
            index,
        } => {
            matches!(object.as_ref(), Expr::Ident(name) if name == alias)
                || refs(object)
                || refs(index)
        }
        Expr::Slice {
            expr: object,
            start,
            end,
        } => refs(object) || start.as_deref().is_some_and(refs) || end.as_deref().is_some_and(refs),
        Expr::Binary { left, right, .. } => refs(left) || refs(right),
        Expr::Unary { expr: inner, .. } => refs(inner),
        Expr::Call { func, args } => {
            refs(func)
                || args.iter().any(|a| match a {
                    Arg::Positional(e) | Arg::Named(_, e) => refs(e),
                })
        }
        Expr::Array(items) => items.iter().any(refs),
        Expr::Map(entries) => entries.iter().any(|(_, v)| refs(v)),
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => refs(cond) || refs(then_branch) || refs(else_branch),
        Expr::Coalesce { expr: e, default } => refs(e) || refs(default),
        Expr::Range { start, end, .. } => refs(start) || refs(end),
        Expr::Lambda { params, body } => !params.iter().any(|p| p == alias) && refs(body),
        Expr::Block { stmts, result } => {
            stmts.iter().any(|(_, _, value, _)| refs(value)) || refs(result)
        }
        Expr::Null
        | Expr::Bool(_)
        | Expr::Int(_)
        | Expr::Float(_)
        | Expr::Str(_)
        | Expr::Duration(_)
        | Expr::Timestamp(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::nfa::StateType;

    /// Helper to build a simple event with one field.
    fn make_event(event_type: &str, field: &str, value: Value) -> Event {
        Event::new(event_type).with_field(field, value)
    }

    /// Helper to build a minimal NFA state for event_matches_state tests.
    fn make_state(event_type: Option<&str>, predicate: Option<Predicate>) -> State {
        let mut s = State::new(0, StateType::Normal);
        if let Some(et) = event_type {
            s.event_type = Some(et.to_string());
        }
        s.predicate = predicate;
        s
    }

    // ======================================================================
    // compare_values tests
    // ======================================================================

    #[test]
    fn test_compare_values_eq_integers() {
        assert!(compare_values(
            &Value::Int(42),
            &Value::Int(42),
            CompareOp::Eq
        ));
        assert!(!compare_values(
            &Value::Int(42),
            &Value::Int(43),
            CompareOp::Eq
        ));
    }

    #[test]
    fn test_compare_values_lt_floats() {
        assert!(compare_values(
            &Value::Float(1.0),
            &Value::Float(2.0),
            CompareOp::Lt
        ));
        assert!(!compare_values(
            &Value::Float(2.0),
            &Value::Float(1.0),
            CompareOp::Lt
        ));
        assert!(!compare_values(
            &Value::Float(1.0),
            &Value::Float(1.0),
            CompareOp::Lt
        ));
    }

    #[test]
    fn test_compare_values_ge_mixed_int_float() {
        assert!(compare_values(
            &Value::Int(10),
            &Value::Float(9.5),
            CompareOp::Ge
        ));
        assert!(compare_values(
            &Value::Float(10.0),
            &Value::Int(10),
            CompareOp::Ge
        ));
        assert!(!compare_values(
            &Value::Int(9),
            &Value::Float(9.5),
            CompareOp::Ge
        ));
    }

    #[test]
    fn test_compare_values_noteq_strings() {
        let a = Value::str("hello");
        let b = Value::str("world");
        assert!(compare_values(&a, &b, CompareOp::NotEq));
        assert!(!compare_values(&a, &a, CompareOp::NotEq));
    }

    #[test]
    fn test_compare_values_le_strings() {
        let a = Value::str("abc");
        let b = Value::str("abd");
        assert!(compare_values(&a, &b, CompareOp::Le));
        assert!(compare_values(&a, &a, CompareOp::Le));
        assert!(!compare_values(&b, &a, CompareOp::Lt));
    }

    // ======================================================================
    // eval_predicate tests
    // ======================================================================

    #[test]
    fn test_eval_predicate_simple_compare_match() {
        let event = make_event("Trade", "price", Value::Float(150.0));
        let pred = Predicate::Compare {
            field: "price".to_string(),
            op: CompareOp::Gt,
            value: Value::Float(100.0),
        };
        let captured = FxHashMap::default();
        assert!(eval_predicate(&pred, &event, &captured, None));
    }

    #[test]
    fn test_eval_predicate_simple_compare_no_match() {
        let event = make_event("Trade", "price", Value::Float(50.0));
        let pred = Predicate::Compare {
            field: "price".to_string(),
            op: CompareOp::Gt,
            value: Value::Float(100.0),
        };
        let captured = FxHashMap::default();
        assert!(!eval_predicate(&pred, &event, &captured, None));
    }

    #[test]
    fn test_eval_predicate_missing_field_returns_false() {
        let event = make_event("Trade", "volume", Value::Int(500));
        let pred = Predicate::Compare {
            field: "price".to_string(),
            op: CompareOp::Eq,
            value: Value::Float(100.0),
        };
        let captured = FxHashMap::default();
        assert!(!eval_predicate(&pred, &event, &captured, None));
    }

    #[test]
    fn test_eval_predicate_and_both_true() {
        let event = Event::new("Trade")
            .with_field("price", Value::Float(150.0))
            .with_field("volume", Value::Int(1000));
        let pred = Predicate::And(
            Box::new(Predicate::Compare {
                field: "price".to_string(),
                op: CompareOp::Gt,
                value: Value::Float(100.0),
            }),
            Box::new(Predicate::Compare {
                field: "volume".to_string(),
                op: CompareOp::Ge,
                value: Value::Int(500),
            }),
        );
        let captured = FxHashMap::default();
        assert!(eval_predicate(&pred, &event, &captured, None));
    }

    #[test]
    fn test_eval_predicate_or_one_true() {
        let event = make_event("Trade", "price", Value::Float(50.0));
        let pred = Predicate::Or(
            Box::new(Predicate::Compare {
                field: "price".to_string(),
                op: CompareOp::Gt,
                value: Value::Float(100.0),
            }),
            Box::new(Predicate::Compare {
                field: "price".to_string(),
                op: CompareOp::Lt,
                value: Value::Float(60.0),
            }),
        );
        let captured = FxHashMap::default();
        assert!(eval_predicate(&pred, &event, &captured, None));
    }

    #[test]
    fn test_eval_predicate_not_inverts() {
        let event = make_event("Trade", "price", Value::Float(150.0));
        let pred = Predicate::Not(Box::new(Predicate::Compare {
            field: "price".to_string(),
            op: CompareOp::Lt,
            value: Value::Float(100.0),
        }));
        let captured = FxHashMap::default();
        // price=150 is NOT < 100, so Not(false) = true
        assert!(eval_predicate(&pred, &event, &captured, None));
    }

    #[test]
    fn test_eval_predicate_compare_ref() {
        let event_b = make_event("Sell", "price", Value::Float(200.0));
        let captured_a = Arc::new(make_event("Buy", "price", Value::Float(100.0)));
        let mut captured: FxHashMap<String, SharedEvent> = FxHashMap::default();
        captured.insert("a".to_string(), captured_a);

        let pred = Predicate::CompareRef {
            field: "price".to_string(),
            op: CompareOp::Gt,
            ref_alias: "a".to_string(),
            ref_field: "price".to_string(),
        };
        // event_b.price (200) > captured_a.price (100) => true
        assert!(eval_predicate(&pred, &event_b, &captured, None));
    }

    // ======================================================================
    // event_matches_state tests
    // ======================================================================

    #[test]
    fn test_event_matches_state_correct_type() {
        let nfa = Nfa::new();
        let event = make_event("Trade", "price", Value::Float(100.0));
        let state = make_state(Some("Trade"), None);
        let captured = FxHashMap::default();
        assert!(event_matches_state(&nfa, &event, &state, &captured, None));
    }

    #[test]
    fn test_event_matches_state_wrong_type() {
        let nfa = Nfa::new();
        let event = make_event("Quote", "price", Value::Float(100.0));
        let state = make_state(Some("Trade"), None);
        let captured = FxHashMap::default();
        assert!(!event_matches_state(&nfa, &event, &state, &captured, None));
    }

    #[test]
    fn test_event_matches_state_no_event_type_matches_anything() {
        let nfa = Nfa::new();
        let event = make_event("Anything", "x", Value::Int(1));
        let state = make_state(None, None);
        let captured = FxHashMap::default();
        assert!(event_matches_state(&nfa, &event, &state, &captured, None));
    }

    // ======================================================================
    // classify_predicate tests
    // ======================================================================

    #[test]
    fn test_classify_simple_compare_is_consistent() {
        let pred = Predicate::Compare {
            field: "price".to_string(),
            op: CompareOp::Gt,
            value: Value::Float(100.0),
        };
        assert_eq!(
            classify_predicate(&pred, Some("a")),
            PredicateClass::Consistent
        );
    }

    #[test]
    fn test_classify_compare_ref_same_alias_is_inconsistent() {
        let pred = Predicate::CompareRef {
            field: "price".to_string(),
            op: CompareOp::Gt,
            ref_alias: "a".to_string(),
            ref_field: "price".to_string(),
        };
        assert_eq!(
            classify_predicate(&pred, Some("a")),
            PredicateClass::Inconsistent
        );
    }

    #[test]
    fn test_classify_compare_ref_different_alias_is_consistent() {
        let pred = Predicate::CompareRef {
            field: "price".to_string(),
            op: CompareOp::Gt,
            ref_alias: "b".to_string(),
            ref_field: "price".to_string(),
        };
        assert_eq!(
            classify_predicate(&pred, Some("a")),
            PredicateClass::Consistent
        );
    }

    #[test]
    fn test_classify_and_propagates_inconsistency() {
        let consistent = Predicate::Compare {
            field: "x".to_string(),
            op: CompareOp::Eq,
            value: Value::Int(1),
        };
        let inconsistent = Predicate::CompareRef {
            field: "price".to_string(),
            op: CompareOp::Gt,
            ref_alias: "a".to_string(),
            ref_field: "price".to_string(),
        };
        let combined = Predicate::And(Box::new(consistent), Box::new(inconsistent));
        assert_eq!(
            classify_predicate(&combined, Some("a")),
            PredicateClass::Inconsistent
        );
    }

    #[test]
    fn test_classify_not_propagates_inner_class() {
        let inner = Predicate::CompareRef {
            field: "price".to_string(),
            op: CompareOp::Gt,
            ref_alias: "a".to_string(),
            ref_field: "price".to_string(),
        };
        let pred = Predicate::Not(Box::new(inner));
        assert_eq!(
            classify_predicate(&pred, Some("a")),
            PredicateClass::Inconsistent
        );
    }
}
