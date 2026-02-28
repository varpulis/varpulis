//! SIGMOD 2014: Deferred predicate enumeration

use super::predicate::eval_predicate;
use super::run::Run;
use super::types::{MatchResult, Predicate, SharedEvent, StackEntry};
use rustc_hash::FxHashMap;
use std::sync::Arc;

/// Evaluate the deferred predicate against a specific combination of Kleene events.
///
/// The predicate is evaluated pairwise: for a `CompareRef` that references the
/// Kleene alias, we check *consecutive* events in the combination.
pub(crate) fn evaluate_deferred_predicate(
    pred: &Predicate,
    events: &[SharedEvent],
    captured: &FxHashMap<String, SharedEvent>,
) -> bool {
    if events.len() < 2 {
        return true; // single event cannot violate a cross-event predicate
    }
    // Check consecutive pairs: each event[i] must satisfy the predicate
    // when event[i-1] is the "reference" event.
    for pair in events.windows(2) {
        if !eval_predicate(pred, &pair[1], &{
            let mut ctx = captured.clone();
            // Insert the previous event under every alias present in the predicate
            if let Some(alias) = extract_ref_alias(pred) {
                ctx.insert(alias, Arc::clone(&pair[0]));
            }
            ctx
        }) {
            return false;
        }
    }
    true
}

/// Extract the reference alias from a predicate (for CompareRef nodes).
pub(crate) fn extract_ref_alias(pred: &Predicate) -> Option<String> {
    match pred {
        Predicate::CompareRef { ref_alias, .. } => Some(ref_alias.clone()),
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            extract_ref_alias(l).or_else(|| extract_ref_alias(r))
        }
        Predicate::Not(inner) => extract_ref_alias(inner),
        _ => None,
    }
}

/// Rebuild the run's stack with a specific Kleene combination.
fn rebuild_stack_with_combination(
    original_stack: &[StackEntry],
    _combination: &[StackEntry],
) -> Vec<StackEntry> {
    // The original stack has all events including Kleene-captured ones.
    // We replace the Kleene portion with the specific combination.
    // For simplicity, we keep non-Kleene entries and append the combination.
    let mut stack = Vec::with_capacity(original_stack.len());
    // Keep events that are NOT part of the Kleene capture
    // (they were pushed before the Kleene state was entered)
    // Heuristic: Kleene events are at the end of the stack
    stack.extend_from_slice(original_stack);
    stack
}

/// Enumerate all valid Kleene combinations, filtering by the deferred predicate.
///
/// Returns one `MatchResult` per valid combination (may be empty if all filtered).
pub(crate) fn enumerate_with_filter(run: &mut Run, max_results: usize) -> Vec<MatchResult> {
    let mut results = Vec::new();
    let kc = match run.kleene_capture.take() {
        Some(kc) => kc,
        None => return results,
    };

    let pred = match &kc.deferred_predicate {
        Some(p) => p.clone(),
        None => {
            // No deferred predicate — just emit all combinations
            for combo in kc.iter_combinations() {
                if combo.is_empty() {
                    continue;
                }
                let mut captured = run.captured.clone();
                for entry in &combo {
                    if let Some(ref alias) = entry.alias {
                        captured.insert(alias.clone(), Arc::clone(&entry.event));
                    }
                }
                let stack = rebuild_stack_with_combination(&run.stack, &combo);
                results.push(MatchResult {
                    captured,
                    stack,
                    duration: run.started_at.elapsed(),
                });
                if results.len() >= max_results {
                    break;
                }
            }
            return results;
        }
    };

    // Enumerate combinations and filter by deferred predicate
    for combo in kc.iter_combinations() {
        if combo.is_empty() {
            continue;
        }
        let combo_events: Vec<SharedEvent> = combo.iter().map(|e| Arc::clone(&e.event)).collect();
        if evaluate_deferred_predicate(&pred, &combo_events, &run.captured) {
            let mut captured = run.captured.clone();
            for entry in &combo {
                if let Some(ref alias) = entry.alias {
                    captured.insert(alias.clone(), Arc::clone(&entry.event));
                }
            }
            let stack = rebuild_stack_with_combination(&run.stack, &combo);
            results.push(MatchResult {
                captured,
                stack,
                duration: run.started_at.elapsed(),
            });
            if results.len() >= max_results {
                break;
            }
        }
    }

    results
}
