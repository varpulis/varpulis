//! Run advancement logic (hot path)

use std::sync::Arc;

use super::and_op::AndState;
use super::enumeration::enumerate_with_filter;
use super::kleene::{KleeneCapture, KleeneLimits};
use super::nfa::{Nfa, State, StateType};
use super::predicate::{eval_predicate, event_matches_state, predicate_references_alias};
use super::types::Predicate;

/// Checks whether `prev` carries every field referenced by `alias.X` in `pred`.
/// Used to decide whether to fall back to "skip predicate" semantics on the
/// first Kleene entry when binding the alias to the anchor would fail because
/// the anchor doesn't have the referenced field.
fn prev_event_has_referenced_fields(
    pred: &Predicate,
    prev: &varpulis_core::Event,
    alias: &str,
) -> bool {
    match pred {
        Predicate::CompareRef {
            ref_alias,
            ref_field,
            ..
        } => ref_alias != alias || prev.get(ref_field).is_some(),
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            prev_event_has_referenced_fields(l, prev, alias)
                && prev_event_has_referenced_fields(r, prev, alias)
        }
        Predicate::Not(inner) => prev_event_has_referenced_fields(inner, prev, alias),
        // Compare/Expr: assume present (Compare doesn't reference alias;
        // Expr is too complex to introspect cheaply)
        _ => true,
    }
}
use super::run::Run;
use super::types::{EmissionMode, MatchResult, SelectionStrategy, SharedEvent};
use crate::clock::Timestamp;
use crate::ExprEvaluator;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum RunAdvanceResult {
    Continue,
    Complete(MatchResult),
    /// For `Each` mode: emit a result but keep the run active for more matches
    CompleteAndContinue(MatchResult),
    /// `Subsets` mode: enumerate all subsets of the Kleene capture as separate
    /// matches via the ZDD (or filtered by a deferred predicate).
    CompleteMulti(Vec<MatchResult>),
    /// Run finished without producing a final match. Used by `Each` mode when
    /// a terminator transitions the NFA to Accept after Kleene events were
    /// already emitted during accumulation — emitting again at the terminator
    /// would duplicate.
    Drained,
    Invalidate,
    NoMatch,
}

// Free functions to avoid borrow checker issues

#[allow(dead_code)]
pub(crate) fn advance_run(
    nfa: &Nfa,
    strategy: SelectionStrategy,
    run: &mut Run,
    event: &varpulis_core::Event,
    limits: KleeneLimits,
    evaluator: Option<&dyn ExprEvaluator>,
) -> RunAdvanceResult {
    // Legacy wrapper — defaults to Each emission for compatibility with old tests
    advance_run_shared(
        nfa,
        strategy,
        run,
        Arc::new(event.clone()),
        limits,
        Timestamp::now(),
        evaluator,
        EmissionMode::Each,
    )
}

/// Complete a run, dispatching on the engine's emission mode.
///
/// - **Each**: matches were already emitted via `CompleteAndContinue` during
///   Kleene accumulation. The terminator should not produce another match;
///   return `Drained` so the caller drops the run silently.
/// - **Longest**: emit a single `Complete` with the longest captured sequence.
/// - **Subsets**: enumerate all non-empty subsets of the Kleene capture and
///   return `CompleteMulti`. This is the SASE+ paper-correct STAM verbose
///   output mode.
///
/// A deferred predicate (Expr-based cross-event constraint) forces enumeration
/// regardless of mode, because the predicate can only be evaluated against
/// concrete event combinations.
fn complete_run(
    run: &mut Run,
    limits: KleeneLimits,
    evaluator: Option<&dyn ExprEvaluator>,
    mode: EmissionMode,
) -> RunAdvanceResult {
    // Deferred predicate forces enumeration regardless of mode.
    if let Some(ref kc) = run.kleene_capture {
        if kc.deferred_predicate.is_some() {
            return RunAdvanceResult::CompleteMulti(enumerate_with_filter(
                run,
                limits.max_results,
                evaluator,
            ));
        }
    }

    let has_kleene_capture = run.kleene_capture.is_some();

    match mode {
        // Each mode: if a Kleene was active, matches were already emitted via
        // CompleteAndContinue during accumulation — drain silently to avoid
        // duplicating. If there was no Kleene (regular sequence pattern),
        // emit the single final match normally.
        EmissionMode::Each if has_kleene_capture => RunAdvanceResult::Drained,
        EmissionMode::Each => RunAdvanceResult::Complete(MatchResult {
            captured: std::mem::take(&mut run.captured),
            stack: std::mem::take(&mut run.stack),
            duration: run.started_at.elapsed(),
        }),
        // Subsets mode: enumerate all non-empty subsets of the Kleene capture
        // (paper-correct STAM verbose). For non-Kleene patterns, single match.
        EmissionMode::Subsets if has_kleene_capture => RunAdvanceResult::CompleteMulti(
            enumerate_with_filter(run, limits.max_results, evaluator),
        ),
        EmissionMode::Subsets => RunAdvanceResult::Complete(MatchResult {
            captured: std::mem::take(&mut run.captured),
            stack: std::mem::take(&mut run.stack),
            duration: run.started_at.elapsed(),
        }),
        // Longest mode: single emit with the last/longest captured sequence.
        EmissionMode::Longest => RunAdvanceResult::Complete(MatchResult {
            captured: std::mem::take(&mut run.captured),
            stack: std::mem::take(&mut run.stack),
            duration: run.started_at.elapsed(),
        }),
    }
}

/// PERF-01: Optimized version that takes SharedEvent to avoid redundant cloning.
///
/// Takes the resolved `EmissionMode` from the engine; this controls whether
/// Kleene events emit `CompleteAndContinue` (Each), accumulate silently
/// (Longest), or accumulate into the ZDD for later subset enumeration (Subsets).
#[allow(clippy::too_many_arguments)]
pub(crate) fn advance_run_shared(
    nfa: &Nfa,
    strategy: SelectionStrategy,
    run: &mut Run,
    event: SharedEvent,
    limits: KleeneLimits,
    now: Timestamp,
    evaluator: Option<&dyn ExprEvaluator>,
    mode: EmissionMode,
) -> RunAdvanceResult {
    let current_state = &nfa.states[run.current_state];

    // NEG-01: Check if event violates any pending negation constraints
    for neg in &run.pending_negations {
        if neg.is_violated_by(&event, &run.captured, evaluator) {
            return RunAdvanceResult::Invalidate;
        }
    }

    // AND-01: Handle AND states specially
    if current_state.state_type == StateType::And {
        return advance_and_state(nfa, run, current_state, event, limits, evaluator, mode);
    }

    // NEG-01: Handle Negation states - check if event matches forbidden pattern
    if current_state.state_type == StateType::Negation {
        if let Some(ref neg_info) = current_state.negation_info {
            // Check if this event violates the negation
            if *event.event_type == neg_info.forbidden_type {
                let pred_matches = neg_info
                    .predicate
                    .as_ref()
                    .is_none_or(|p| eval_predicate(p, &event, &run.captured, evaluator));
                if pred_matches {
                    return RunAdvanceResult::Invalidate;
                }
            }
        }
        // Event doesn't violate negation, stay in this state waiting for confirmation
        return RunAdvanceResult::NoMatch;
    }

    // Check if we're at an accept state
    if current_state.state_type == StateType::Accept {
        return complete_run(run, limits, evaluator, mode);
    }

    // KLEENE SELF-LOOP: accumulate additional events matching the Kleene state.
    // The first event enters via the transitions loop; subsequent events match here.
    //
    // For self-referencing predicates (e.g., val > rising.val where "rising" is
    // the Kleene alias), the first event has no previous value to compare against.
    // We skip the predicate only when:
    // 1. The Kleene alias is not yet in captured (first entry)
    // 2. The predicate actually references the Kleene's own alias (self-reference)
    let kleene_matches =
        current_state.state_type == StateType::Kleene && current_state.self_loop && {
            let is_self_ref_first_entry = current_state.alias.as_ref().is_some_and(|a| {
                !run.captured.contains_key(a.as_str())
                    && current_state
                        .predicate
                        .as_ref()
                        .is_some_and(|p| predicate_references_alias(p, a))
            });
            if is_self_ref_first_entry {
                // First Kleene entry with self-ref: check event type only
                current_state
                    .event_type
                    .as_ref()
                    .is_none_or(|et| *event.event_type == *et)
            } else {
                event_matches_state(nfa, &event, current_state, &run.captured, evaluator)
            }
        };
    if kleene_matches {
        // Safety: check Kleene cap before accumulating (prevents 2^n blowup)
        if let Some(ref kc) = run.kleene_capture {
            if kc.next_var >= limits.max_events {
                return RunAdvanceResult::Continue;
            }
        }

        // PERF(Opt4): Use push_at_kleene to avoid re-allocating captured key
        run.push_at_kleene(Arc::clone(&event), &current_state.alias, now);

        // Always accumulate into kleene_capture so Subsets mode (and deferred
        // predicates) can enumerate at completion. For Subsets mode we need
        // the full ZDD; for other modes we can use the lightweight extend.
        if run.kleene_capture.is_none() {
            let mut kc = KleeneCapture::new();
            if current_state.postponed_predicate.is_some() {
                kc.deferred_predicate = current_state.postponed_predicate.clone();
                kc.needs_zdd = true;
            }
            if mode == EmissionMode::Subsets {
                kc.needs_zdd = true;
            }
            run.kleene_capture = Some(kc);
        }
        if let Some(ref mut kc) = run.kleene_capture {
            if kc.needs_zdd {
                kc.extend(Arc::clone(&event), current_state.alias.clone());
            } else {
                kc.extend_simple(Arc::clone(&event), current_state.alias.clone());
            }
        }

        // Dispatch on emission mode:
        // - Each: emit immediately with current bindings (one match per Kleene event)
        // - Longest/Subsets: silent — emit at terminator/break
        match mode {
            EmissionMode::Each => {
                return RunAdvanceResult::CompleteAndContinue(MatchResult {
                    captured: run.captured.clone(),
                    stack: run.stack.clone(),
                    duration: run.started_at.elapsed(),
                });
            }
            EmissionMode::Longest | EmissionMode::Subsets => {
                return RunAdvanceResult::Continue;
            }
        }
    }

    // Kleene break: when the self-loop predicate fails for a Kleene-final
    // pattern (epsilon to Accept), emit the accumulated match. For Each mode
    // matches were already emitted; complete_run will Drain. For Longest we
    // emit one final consolidated match here; for Subsets we enumerate.
    if current_state.state_type == StateType::Kleene
        && current_state.self_loop
        && current_state.has_epsilon_to_accept
        && !run.captured.is_empty()
    {
        return complete_run(run, limits, evaluator, mode);
    }

    // Check transitions
    for &next_id in &current_state.transitions {
        let next_state = &nfa.states[next_id];

        // NEG-01: If transitioning to a Negation state, set up the pending negation
        if next_state.state_type == StateType::Negation {
            if let Some(ref neg_info) = next_state.negation_info {
                // Add pending negation constraint - deadline will be set based on WITHIN
                let negation_constraint = super::negation::NegationConstraint {
                    forbidden_type: neg_info.forbidden_type.clone(),
                    predicate: neg_info.predicate.clone(),
                    deadline: run.deadline, // Use the run's deadline
                    event_time_deadline: run.event_time_deadline,
                    next_state: neg_info.continue_state,
                };
                run.pending_negations.push(negation_constraint);
                run.current_state = next_id;
                return RunAdvanceResult::Continue;
            }
        }

        // AND-01: If transitioning to an AND state, enter it and try to match current event
        if next_state.state_type == StateType::And {
            run.current_state = next_id;
            return advance_and_state(nfa, run, next_state, event, limits, evaluator, mode);
        }

        // For Kleene states with self-referencing predicates: the first event
        // entering the Kleene has no previous value to compare against under
        // the Kleene's own alias. We temporarily bind the alias to the previous
        // step's event (run.stack.last()) so .increasing()/.decreasing() compare
        // against the anchor instead of being silently skipped.
        let matches = if next_state.state_type == StateType::Kleene
            && next_state.self_loop
            && next_state.alias.as_ref().is_some_and(|a| {
                !run.captured.contains_key(a.as_str())
                    && next_state
                        .predicate
                        .as_ref()
                        .is_some_and(|p| predicate_references_alias(p, a))
            }) {
            // Check event type first
            let type_ok = next_state
                .event_type
                .as_ref()
                .is_none_or(|et| *event.event_type == *et);
            if !type_ok {
                false
            } else {
                // Try to bind the Kleene alias to the previous step's event
                // and evaluate the predicate. This makes .increasing()/.decreasing()
                // compare against the anchor on the first Kleene entry.
                // If the previous event doesn't carry the referenced field,
                // fall back to skipping the predicate (true Kleene-only first entry).
                let alias = next_state.alias.as_ref().unwrap().clone();
                if let Some(prev) = run.stack.last() {
                    let mut tmp_captured = run.captured.clone();
                    tmp_captured.insert(alias.clone(), Arc::clone(&prev.event));
                    if let Some(p) = next_state.predicate.as_ref() {
                        let result = eval_predicate(p, &event, &tmp_captured, evaluator);
                        // If false but the previous event lacks the referenced
                        // field(s), treat it as "no comparison possible" → skip.
                        if !result && !prev_event_has_referenced_fields(p, &prev.event, &alias) {
                            true
                        } else {
                            result
                        }
                    } else {
                        true
                    }
                } else {
                    // No previous event — skip predicate
                    true
                }
            }
        } else {
            event_matches_state(nfa, &event, next_state, &run.captured, evaluator)
        };

        if matches {
            run.current_state = next_id;
            run.push_at(Arc::clone(&event), next_state.alias.clone(), now);

            if next_state.state_type == StateType::Accept {
                return complete_run(run, limits, evaluator, mode);
            }

            if next_state.state_type == StateType::Kleene && next_state.self_loop {
                // Always accumulate so Subsets mode can enumerate at completion
                if run.kleene_capture.is_none() {
                    let mut kc = KleeneCapture::new();
                    if next_state.postponed_predicate.is_some() {
                        kc.deferred_predicate = next_state.postponed_predicate.clone();
                        kc.needs_zdd = true;
                    }
                    if mode == EmissionMode::Subsets {
                        kc.needs_zdd = true;
                    }
                    run.kleene_capture = Some(kc);
                }
                if let Some(ref mut kc) = run.kleene_capture {
                    if kc.next_var >= limits.max_events {
                        return RunAdvanceResult::Continue;
                    }
                    if kc.needs_zdd {
                        kc.extend(Arc::clone(&event), next_state.alias.clone());
                    } else {
                        kc.extend_simple(Arc::clone(&event), next_state.alias.clone());
                    }
                }

                // Dispatch on emission mode for the first Kleene entry
                match mode {
                    EmissionMode::Each => {
                        return RunAdvanceResult::CompleteAndContinue(MatchResult {
                            captured: run.captured.clone(),
                            stack: run.stack.clone(),
                            duration: run.started_at.elapsed(),
                        });
                    }
                    EmissionMode::Longest | EmissionMode::Subsets => {
                        return RunAdvanceResult::Continue;
                    }
                }
            }

            return RunAdvanceResult::Continue;
        }
    }

    // Check epsilon transitions
    for &eps_id in &current_state.epsilon_transitions {
        let eps_state = &nfa.states[eps_id];

        if eps_state.state_type == StateType::Accept {
            return complete_run(run, limits, evaluator, mode);
        }

        for &next_id in &eps_state.transitions {
            let next_state = &nfa.states[next_id];
            if event_matches_state(nfa, &event, next_state, &run.captured, evaluator) {
                run.current_state = next_id;
                run.push_at(Arc::clone(&event), next_state.alias.clone(), now);

                if next_state.state_type == StateType::Accept {
                    return complete_run(run, limits, evaluator, mode);
                }

                return RunAdvanceResult::Continue;
            }
        }
    }

    match strategy {
        SelectionStrategy::StrictContiguous => RunAdvanceResult::Invalidate,
        _ => RunAdvanceResult::NoMatch,
    }
}

/// AND-01: Handle AND state - track which branches are completed
#[allow(clippy::too_many_arguments)]
fn advance_and_state(
    nfa: &Nfa,
    run: &mut Run,
    state: &State,
    event: SharedEvent,
    limits: KleeneLimits,
    evaluator: Option<&dyn ExprEvaluator>,
    mode: EmissionMode,
) -> RunAdvanceResult {
    let config = match &state.and_config {
        Some(c) => c,
        None => return RunAdvanceResult::NoMatch,
    };

    // Initialize AND state if not already
    if run.and_state.is_none() {
        run.and_state = Some(AndState::new());
    }

    // Find matching branch
    let mut matched_branch: Option<(usize, Option<String>)> = None;
    let total_branches = config.branches.len();

    {
        let Some(and_state) = run.and_state.as_ref() else {
            return RunAdvanceResult::Continue;
        };

        for (idx, branch) in config.branches.iter().enumerate() {
            if and_state.is_branch_completed(idx) {
                continue;
            }

            if *event.event_type == branch.event_type {
                let pred_matches = branch
                    .predicate
                    .as_ref()
                    .is_none_or(|p| eval_predicate(p, &event, &run.captured, evaluator));

                if pred_matches {
                    matched_branch = Some((idx, branch.alias.clone()));
                    break;
                }
            }
        }
    }

    // Process the match if found
    if let Some((idx, alias)) = matched_branch {
        // Complete this branch
        let Some(and_state) = run.and_state.as_mut() else {
            return RunAdvanceResult::Continue;
        };
        and_state.complete_branch(idx, Arc::clone(&event));

        // Capture with alias if present
        if let Some(ref alias) = alias {
            run.captured.insert(alias.clone(), Arc::clone(&event));
        }
        run.push(Arc::clone(&event), alias);

        // Check if all branches are completed
        let all_completed = run
            .and_state
            .as_ref()
            .is_some_and(|s| s.all_completed(total_branches));

        if all_completed {
            run.current_state = config.join_state;
            run.and_state = None; // Clear AND state

            // Check if join state is accept
            let join_state = &nfa.states[config.join_state];
            if join_state.state_type == StateType::Accept {
                return complete_run(run, limits, evaluator, mode);
            }
        }

        return RunAdvanceResult::Continue;
    }

    RunAdvanceResult::NoMatch
}
