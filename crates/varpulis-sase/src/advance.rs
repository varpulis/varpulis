//! Run advancement logic (hot path)

use crate::clock::Timestamp;
use std::sync::Arc;

use super::and_op::AndState;
use super::enumeration::enumerate_with_filter;
use super::kleene::{KleeneCapture, KleeneLimits};
use super::nfa::{Nfa, State, StateType};
use super::predicate::{eval_predicate, event_matches_state};
use super::run::Run;
use super::types::{MatchResult, SelectionStrategy, SharedEvent};
use crate::ExprEvaluator;

#[derive(Debug)]
pub(crate) enum RunAdvanceResult {
    Continue,
    Complete(MatchResult),
    /// For `all` patterns: emit a result but keep the run active for more matches
    CompleteAndContinue(MatchResult),
    /// SIGMOD 2014: Multiple results from Kleene enumeration with deferred predicate
    CompleteMulti(Vec<MatchResult>),
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
    // Wrap in Arc for the shared version
    advance_run_shared(
        nfa,
        strategy,
        run,
        Arc::new(event.clone()),
        limits,
        Timestamp::now(),
        evaluator,
    )
}

/// Complete a run, checking for deferred Kleene predicates first.
fn complete_run(
    run: &mut Run,
    limits: KleeneLimits,
    evaluator: Option<&dyn ExprEvaluator>,
) -> RunAdvanceResult {
    if let Some(ref kc) = run.kleene_capture {
        if kc.deferred_predicate.is_some() {
            return RunAdvanceResult::CompleteMulti(enumerate_with_filter(
                run,
                limits.max_results,
                evaluator,
            ));
        }
    }
    RunAdvanceResult::Complete(MatchResult {
        captured: std::mem::take(&mut run.captured),
        stack: std::mem::take(&mut run.stack),
        duration: run.started_at.elapsed(),
    })
}

/// PERF-01: Optimized version that takes SharedEvent to avoid redundant cloning
pub(crate) fn advance_run_shared(
    nfa: &Nfa,
    strategy: SelectionStrategy,
    run: &mut Run,
    event: SharedEvent,
    limits: KleeneLimits,
    now: Timestamp,
    evaluator: Option<&dyn ExprEvaluator>,
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
        return advance_and_state(nfa, run, current_state, event, limits, evaluator);
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
        return complete_run(run, limits, evaluator);
    }

    // KLEENE SELF-LOOP: accumulate additional events matching the Kleene state.
    // The first event enters via the transitions loop; subsequent events match here.
    if current_state.state_type == StateType::Kleene
        && current_state.self_loop
        && event_matches_state(nfa, &event, current_state, &run.captured, evaluator)
    {
        // Safety: check Kleene cap before accumulating (prevents 2^n blowup)
        if let Some(ref kc) = run.kleene_capture {
            if kc.next_var >= limits.max_events {
                return RunAdvanceResult::Continue;
            }
        }

        // PERF(Opt4): Use push_at_kleene to avoid re-allocating captured key
        run.push_at_kleene(Arc::clone(&event), &current_state.alias, now);

        // PERF(Opt3): Use pre-computed flag instead of per-event iteration
        if current_state.has_epsilon_to_accept {
            return RunAdvanceResult::CompleteAndContinue(MatchResult {
                captured: run.captured.clone(),
                stack: run.stack.clone(),
                duration: run.started_at.elapsed(),
            });
        }

        // No epsilon to accept: accumulate for deferred emission (SEQ(A, B+, C))
        if run.kleene_capture.is_none() {
            let mut kc = KleeneCapture::new();
            if current_state.postponed_predicate.is_some() {
                kc.deferred_predicate = current_state.postponed_predicate.clone();
                kc.needs_zdd = true;
            }
            run.kleene_capture = Some(kc);
        }
        // PERF(Opt1): Skip ZDD product_with_optional when no deferred predicate
        if let Some(ref mut kc) = run.kleene_capture {
            if kc.needs_zdd {
                kc.extend(Arc::clone(&event), current_state.alias.clone());
            } else {
                kc.extend_simple(Arc::clone(&event), current_state.alias.clone());
            }
        }
        return RunAdvanceResult::Continue;
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
            // Try to advance the AND state with the current event
            return advance_and_state(nfa, run, next_state, event, limits, evaluator);
        }

        if event_matches_state(nfa, &event, next_state, &run.captured, evaluator) {
            run.current_state = next_id;
            run.push_at(Arc::clone(&event), next_state.alias.clone(), now);

            if next_state.state_type == StateType::Accept {
                // PERF: Use std::mem::take since run is discarded after Complete
                return complete_run(run, limits, evaluator);
            }

            if next_state.state_type == StateType::Kleene && next_state.self_loop {
                // PERF(Opt3): Use pre-computed flag instead of per-event iteration
                if next_state.has_epsilon_to_accept {
                    return RunAdvanceResult::CompleteAndContinue(MatchResult {
                        captured: run.captured.clone(),
                        stack: run.stack.clone(),
                        duration: run.started_at.elapsed(),
                    });
                }

                // No epsilon to accept: accumulate for deferred emission (SEQ(A, B+, C))
                if run.kleene_capture.is_none() {
                    let mut kc = KleeneCapture::new();
                    if next_state.postponed_predicate.is_some() {
                        kc.deferred_predicate = next_state.postponed_predicate.clone();
                        kc.needs_zdd = true;
                    }
                    run.kleene_capture = Some(kc);
                }
                // PERF(Opt1): Skip ZDD when no deferred predicate
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

                return RunAdvanceResult::Continue;
            }

            return RunAdvanceResult::Continue;
        }
    }

    // Check epsilon transitions
    for &eps_id in &current_state.epsilon_transitions {
        let eps_state = &nfa.states[eps_id];

        if eps_state.state_type == StateType::Accept {
            // PERF: Use std::mem::take since run is discarded after Complete
            return complete_run(run, limits, evaluator);
        }

        for &next_id in &eps_state.transitions {
            let next_state = &nfa.states[next_id];
            if event_matches_state(nfa, &event, next_state, &run.captured, evaluator) {
                run.current_state = next_id;
                run.push_at(Arc::clone(&event), next_state.alias.clone(), now);

                if next_state.state_type == StateType::Accept {
                    // PERF: Use std::mem::take since run is discarded after Complete
                    return complete_run(run, limits, evaluator);
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
fn advance_and_state(
    nfa: &Nfa,
    run: &mut Run,
    state: &State,
    event: SharedEvent,
    limits: KleeneLimits,
    evaluator: Option<&dyn ExprEvaluator>,
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
                // PERF: Use std::mem::take since run is discarded after Complete
                return complete_run(run, limits, evaluator);
            }
        }

        return RunAdvanceResult::Continue;
    }

    RunAdvanceResult::NoMatch
}
