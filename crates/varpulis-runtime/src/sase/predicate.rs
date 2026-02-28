//! Predicate evaluation and classification

use super::nfa::{Nfa, State};
use super::types::{CompareOp, Predicate, SharedEvent};
use crate::engine::eval_filter_expr;
use crate::event::Event;
use crate::sequence::SequenceContext;
use rustc_hash::FxHashMap;
use varpulis_core::Value;

// ============================================================================
// PREDICATE EVALUATION
// ============================================================================

pub(crate) fn event_matches_state(
    _nfa: &Nfa,
    event: &Event,
    state: &State,
    captured: &FxHashMap<String, SharedEvent>,
) -> bool {
    if let Some(ref expected_type) = state.event_type {
        if &*event.event_type != expected_type {
            return false;
        }
    }

    if let Some(ref predicate) = state.predicate {
        if !eval_predicate(predicate, event, captured) {
            return false;
        }
    }

    true
}

pub(crate) fn eval_predicate(
    predicate: &Predicate,
    event: &Event,
    captured: &FxHashMap<String, SharedEvent>,
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
            eval_predicate(left, event, captured) && eval_predicate(right, event, captured)
        }
        Predicate::Or(left, right) => {
            eval_predicate(left, event, captured) || eval_predicate(right, event, captured)
        }
        Predicate::Not(inner) => !eval_predicate(inner, event, captured),
        Predicate::Expr(expr) => {
            // Build SequenceContext from captured events for expression evaluation
            // Dereference Arc<Event> to Event for compatibility with SequenceContext
            let captured_events: FxHashMap<String, Event> = captured
                .iter()
                .map(|(k, v)| (k.clone(), (**v).clone()))
                .collect();
            let ctx = SequenceContext {
                captured: captured_events,
                previous: None,
            };
            // Evaluate the expression and check if it returns true
            eval_filter_expr(expr, event, &ctx)
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
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

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => {
            (*a as f64 - b).abs() < f64::EPSILON
        }
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        _ => false,
    }
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
pub(crate) enum PredicateClass {
    Consistent,
    Inconsistent,
}

/// Classify a predicate as consistent or inconsistent for a Kleene state.
///
/// A predicate is *inconsistent* if it contains `CompareRef` nodes that
/// reference the same alias as the Kleene variable (cross-event comparison).
pub(crate) fn classify_predicate(pred: &Predicate, alias: Option<&str>) -> PredicateClass {
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

/// Check whether a `varpulis_core::ast::Expr` mentions a given alias.
pub(crate) fn expr_references_alias(expr: &varpulis_core::ast::Expr, alias: &str) -> bool {
    use varpulis_core::ast::Expr;
    match expr {
        Expr::Ident(name) => {
            name.starts_with(alias) && name.as_bytes().get(alias.len()) == Some(&b'.')
        }
        Expr::Binary { left, right, .. } => {
            expr_references_alias(left, alias) || expr_references_alias(right, alias)
        }
        Expr::Unary { expr: inner, .. } => expr_references_alias(inner, alias),
        Expr::Call { args, .. } => args.iter().any(|a| match a {
            varpulis_core::ast::Arg::Positional(e) | varpulis_core::ast::Arg::Named(_, e) => {
                expr_references_alias(e, alias)
            }
        }),
        _ => false,
    }
}
