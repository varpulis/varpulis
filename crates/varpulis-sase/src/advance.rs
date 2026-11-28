//! Run advancement logic (hot path)

use std::sync::Arc;

use super::and_op::{AndBranch, AndConfig, AndState};
use super::enumeration::enumerate_with_filter;
use super::kleene::{KleeneCapture, KleeneLimits};
use super::nfa::{Nfa, StateType};
use super::predicate::{eval_predicate, event_matches_state, predicate_references_alias};
use super::types::{Predicate, StackEntry};

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

/// Number of events this run's Kleene closure matched but had to drop at the
/// cap. Zero when the closure never filled up (the overwhelming majority) and
/// when there is no closure at all.
fn kleene_truncated_count(run: &Run) -> u32 {
    run.kleene_capture.as_ref().map_or(0, |kc| kc.truncated)
}

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

/// Enter any negated step that immediately follows the run's current state.
///
/// A negated step is entered by *arriving at it*, not by consuming an event —
/// "B did not follow" is decided by time passing, and time passing produces no
/// event to advance on. The constraint used to be registered inside the
/// transition loop, which only runs when the next event arrives, so a run
/// waiting for an absence sat at the preceding state with no pending negation
/// and was discarded by the deadline sweep. The absence could never be
/// observed, because observing it required the very event whose non-arrival
/// was the point.
///
/// Returns true when a negation was entered.
pub(crate) fn enter_negation_if_next(nfa: &Nfa, run: &mut Run) -> bool {
    let transitions = nfa.states[run.current_state].transitions.clone();
    for next_id in transitions {
        let next_state = &nfa.states[next_id];
        if next_state.state_type != StateType::Negation {
            continue;
        }
        let Some(ref neg_info) = next_state.negation_info else {
            continue;
        };
        run.pending_negations
            .push(super::negation::NegationConstraint {
                forbidden_type: neg_info.forbidden_type.clone(),
                predicate: neg_info.predicate.clone(),
                deadline: run.deadline,
                event_time_deadline: run.event_time_deadline,
                next_state: neg_info.continue_state,
            });
        run.current_state = next_id;
        return true;
    }
    false
}

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
    nfa: &Nfa,
) -> RunAdvanceResult {
    // Only a closure that ends the pattern emits while it accumulates.
    let emitted_while_accumulating = nfa.has_final_kleene();
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
        // Only a closure that ends the pattern emits while it accumulates;
        // after `A -> all B -> C` the match exists once C has arrived, and
        // it is emitted then, once per closure event.
        EmissionMode::Each if has_kleene_capture && emitted_while_accumulating => {
            RunAdvanceResult::Drained
        }
        EmissionMode::Each if has_kleene_capture => {
            RunAdvanceResult::CompleteMulti(each_closure_event(run, nfa.followed_closure_alias()))
        }
        EmissionMode::Each => RunAdvanceResult::Complete(MatchResult {
            kleene_truncated: kleene_truncated_count(run),
            enumeration_truncated: false,
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
            kleene_truncated: kleene_truncated_count(run),
            enumeration_truncated: false,
            captured: std::mem::take(&mut run.captured),
            stack: std::mem::take(&mut run.stack),
            duration: run.started_at.elapsed(),
        }),
        // Longest mode: single emit with the last/longest captured sequence.
        EmissionMode::Longest => RunAdvanceResult::Complete(MatchResult {
            kleene_truncated: kleene_truncated_count(run),
            enumeration_truncated: false,
            captured: std::mem::take(&mut run.captured),
            stack: std::mem::take(&mut run.stack),
            duration: run.started_at.elapsed(),
        }),
    }
}

/// `.each()` for a closure followed by another step (`A -> all B -> C`): one
/// match per closure event, emitted when the pattern completes, each with the
/// closure as it stood at that event (so `count(b)` is 1, 2, ... n) and every
/// step after it. It used to emit these while the closure accumulated, before
/// C had arrived or whether it ever would.
///
/// The closure's alias comes from the NFA, not from the capture: a capture
/// restored from a snapshot does not carry it, and a restored run must emit
/// what the live one would have.
fn each_closure_event(run: &mut Run, closure_alias: Option<&str>) -> Vec<MatchResult> {
    let alias = closure_alias.map(str::to_string);
    let kleene_truncated = kleene_truncated_count(run);
    let duration = run.started_at.elapsed();
    let stack = std::mem::take(&mut run.stack);
    let captured = std::mem::take(&mut run.captured);
    let closure: Vec<usize> = match &alias {
        Some(a) => stack
            .iter()
            .enumerate()
            .filter(|(_, e)| e.alias.as_deref() == Some(a.as_str()))
            .map(|(i, _)| i)
            .collect(),
        None => Vec::new(),
    };
    let (Some(alias), false) = (alias, closure.is_empty()) else {
        return vec![MatchResult {
            kleene_truncated,
            enumeration_truncated: false,
            captured,
            stack,
            duration,
        }];
    };
    closure
        .iter()
        .map(|&upto| {
            let stack_upto: Vec<StackEntry> = stack
                .iter()
                .enumerate()
                .filter(|(i, e)| e.alias.as_deref() != Some(alias.as_str()) || *i <= upto)
                .map(|(_, e)| e.clone())
                .collect();
            let mut captured_upto = captured.clone();
            captured_upto.insert(alias.clone(), Arc::clone(&stack[upto].event));
            MatchResult {
                kleene_truncated,
                enumeration_truncated: false,
                captured: captured_upto,
                stack: stack_upto,
                duration,
            }
        })
        .collect()
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
        return match offer_to_and(nfa, run, run.current_state, &event, now, limits, evaluator) {
            AndOffer::Completed => finish_and(nfa, run, limits, evaluator, mode),
            AndOffer::Taken => RunAdvanceResult::Continue,
            AndOffer::Refuted => RunAdvanceResult::Invalidate,
            AndOffer::NotTaken => match strategy {
                SelectionStrategy::StrictContiguous => RunAdvanceResult::Invalidate,
                _ => RunAdvanceResult::NoMatch,
            },
        };
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
        return complete_run(run, limits, evaluator, mode, nfa);
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
        // Safety: check Kleene cap before accumulating (prevents 2^n blowup).
        // Record the drop — see `KleeneCapture::truncated`. Silence here is what
        // turned a bounded closure into a wrong answer.
        if let Some(ref mut kc) = run.kleene_capture {
            if kc.next_var >= limits.max_events {
                kc.truncated = kc.truncated.saturating_add(1);
                if kc.truncated == 1 {
                    tracing::warn!(
                        cap = limits.max_events,
                        alias = current_state.alias.as_deref().unwrap_or("<unnamed>"),
                        "Kleene closure reached its event cap; further matching events are \
                         dropped from this run and the emitted match carries \
                         `_kleene_truncated` with the number dropped"
                    );
                }
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
        // - Each: emit immediately with current bindings (one match per Kleene
        //   event), when the closure ends the pattern; a closure followed by
        //   another step is no match until that step arrives
        // - Longest/Subsets: silent — emit at terminator/break
        match mode {
            EmissionMode::Each if current_state.has_epsilon_to_accept => {
                return RunAdvanceResult::CompleteAndContinue(MatchResult {
                    kleene_truncated: kleene_truncated_count(run),
                    enumeration_truncated: false,
                    captured: run.captured.clone(),
                    stack: run.stack.clone(),
                    duration: run.started_at.elapsed(),
                });
            }
            EmissionMode::Each | EmissionMode::Longest | EmissionMode::Subsets => {
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
        return complete_run(run, limits, evaluator, mode, nfa);
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

        // AND-01: an AND state is entered by an event one of its branches
        // takes. An event none takes leaves the run where it was, free to
        // follow another transition (an `or` alternative).
        if next_state.state_type == StateType::And {
            match offer_to_and(nfa, run, next_id, &event, now, limits, evaluator) {
                AndOffer::Completed => {
                    run.current_state = next_id;
                    return finish_and(nfa, run, limits, evaluator, mode);
                }
                AndOffer::Taken => {
                    run.current_state = next_id;
                    return RunAdvanceResult::Continue;
                }
                AndOffer::NotTaken | AndOffer::Refuted => {
                    run.and_state = None;
                    continue;
                }
            }
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
                return complete_run(run, limits, evaluator, mode, nfa);
            }

            // Arrive at a negated step now rather than on the next event.
            if enter_negation_if_next(nfa, run) {
                return RunAdvanceResult::Continue;
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
                        kc.truncated = kc.truncated.saturating_add(1);
                        if kc.truncated == 1 {
                            tracing::warn!(
                                cap = limits.max_events,
                                alias = next_state.alias.as_deref().unwrap_or("<unnamed>"),
                                "Kleene closure reached its event cap; further matching events \
                                 are dropped from this run and the emitted match carries \
                                 `_kleene_truncated` with the number dropped"
                            );
                        }
                        return RunAdvanceResult::Continue;
                    }
                    if kc.needs_zdd {
                        kc.extend(Arc::clone(&event), next_state.alias.clone());
                    } else {
                        kc.extend_simple(Arc::clone(&event), next_state.alias.clone());
                    }
                }

                // Dispatch on emission mode for the first Kleene entry (Each
                // emits only when the closure ends the pattern)
                match mode {
                    EmissionMode::Each if next_state.has_epsilon_to_accept => {
                        return RunAdvanceResult::CompleteAndContinue(MatchResult {
                            kleene_truncated: kleene_truncated_count(run),
                            enumeration_truncated: false,
                            captured: run.captured.clone(),
                            stack: run.stack.clone(),
                            duration: run.started_at.elapsed(),
                        });
                    }
                    EmissionMode::Each | EmissionMode::Longest | EmissionMode::Subsets => {
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
            return complete_run(run, limits, evaluator, mode, nfa);
        }

        for &next_id in &eps_state.transitions {
            let next_state = &nfa.states[next_id];
            if event_matches_state(nfa, &event, next_state, &run.captured, evaluator) {
                run.current_state = next_id;
                run.push_at(Arc::clone(&event), next_state.alias.clone(), now);

                if next_state.state_type == StateType::Accept {
                    return complete_run(run, limits, evaluator, mode, nfa);
                }

                enter_negation_if_next(nfa, run);
                return RunAdvanceResult::Continue;
            }
        }
    }

    match strategy {
        SelectionStrategy::StrictContiguous => RunAdvanceResult::Invalidate,
        _ => RunAdvanceResult::NoMatch,
    }
}

/// What an AND state did with an event offered to it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AndOffer {
    /// No open branch takes the event.
    NotTaken,
    /// A branch took it, or it waits for the siblings its branch reads.
    Taken,
    /// Every branch has now matched: the run is at the AND's join state.
    Completed,
    /// The event that opened this run can no longer be part of its match
    /// (see [`offer_to_and`]); the run is over.
    Refuted,
}

/// Offer `event` to the AND state `and_id` for `run`, which is at that state
/// or at a state with a transition to it.
///
/// An event takes the first open branch (in source order) whose type it has
/// and whose predicate it satisfies. A branch whose predicate reads a
/// sibling's alias (`B where id == a.id` next to `A as a`) is judged once
/// that sibling has matched; an event of its type arriving earlier waits in
/// [`AndState::waiting`] and is judged then, oldest first.
///
/// A run that an AND at the start of the pattern opened with a waiting event
/// is anchored on it: it holds no other waiting event (a later one opens a
/// run of its own, under the default selection, and would otherwise show up
/// in two identical matches), and when that event is judged and fails, the
/// run is [`AndOffer::Refuted`].
///
/// Under event time an AND accepts an event stamped before the run's start
/// (any order is the point of an AND) as long as all its events fit in the
/// WITHIN; the window then starts at that event.
pub(crate) fn offer_to_and(
    nfa: &Nfa,
    run: &mut Run,
    and_id: usize,
    event: &SharedEvent,
    now: Timestamp,
    limits: KleeneLimits,
    evaluator: Option<&dyn ExprEvaluator>,
) -> AndOffer {
    let Some(config) = nfa.states[and_id].and_config.as_ref() else {
        return AndOffer::NotTaken;
    };
    let mut and_state = run.and_state.take().unwrap_or_default();

    let mut taken = false;
    let mut waits = false;
    for (idx, branch) in config.branches.iter().enumerate() {
        if and_state.is_branch_completed(idx) || *event.event_type != branch.event_type {
            continue;
        }
        if !branch
            .sibling_refs
            .iter()
            .all(|&r| and_state.is_branch_completed(r))
        {
            waits = true;
            continue;
        }
        let pred_matches = branch
            .predicate
            .as_ref()
            .is_none_or(|p| eval_predicate(p, event, &run.captured, evaluator));
        if pred_matches
            && !refutes_anchor(run, &and_state, config, idx, event, evaluator)
            && admit_event_time(run, event)
        {
            complete_and_branch(run, &mut and_state, idx, branch, event, now);
            taken = true;
            break;
        }
    }

    if !taken {
        let offer = if waits && and_state.anchor.is_none() && fits_event_time(run, event) {
            if run.stack.is_empty() && and_state.waiting.is_empty() {
                and_state.anchor = Some(Arc::clone(event));
            }
            if and_state.waiting.len() >= limits.max_events as usize {
                let dropped = and_state.waiting.remove(0);
                if and_state
                    .anchor
                    .as_ref()
                    .is_some_and(|a| Arc::ptr_eq(a, &dropped))
                {
                    and_state.anchor = None;
                }
                and_state.waiting_dropped = and_state.waiting_dropped.saturating_add(1);
                if and_state.waiting_dropped == 1 {
                    tracing::warn!(
                        cap = limits.max_events,
                        "an `and` holds at most this many events waiting for the branch \
                         they refer to; the oldest is dropped and may miss its match"
                    );
                }
            }
            and_state.waiting.push(Arc::clone(event));
            AndOffer::Taken
        } else {
            AndOffer::NotTaken
        };
        let untouched = and_state.completed.iter().all(|c| !c) && and_state.waiting.is_empty();
        run.and_state = (!untouched).then_some(and_state);
        return offer;
    }

    // A completed branch may be what waiting events were waiting for.
    if drain_and_waiting(run, &mut and_state, config, now, evaluator) {
        return AndOffer::Refuted;
    }

    if and_state.all_completed(config.branches.len()) {
        run.current_state = config.join_state;
        run.and_state = None;
        return AndOffer::Completed;
    }
    run.and_state = Some(and_state);
    AndOffer::Taken
}

/// Whether taking `event` into branch `idx` would make the run's anchor (the
/// waiting event that opened it) fail: the anchor's branch would then be
/// judged, against this event, and refuse it. Such an event is left for
/// other runs; this one keeps waiting for a sibling its anchor accepts.
fn refutes_anchor(
    run: &Run,
    and_state: &AndState,
    config: &AndConfig,
    idx: usize,
    event: &SharedEvent,
    evaluator: Option<&dyn ExprEvaluator>,
) -> bool {
    let Some(anchor) = and_state.anchor.as_ref() else {
        return false;
    };
    let completed_after = |i: usize| i == idx || and_state.is_branch_completed(i);
    let mut captured = None;
    for (b, branch) in config.branches.iter().enumerate() {
        if completed_after(b) || *anchor.event_type != branch.event_type {
            continue;
        }
        if !branch.sibling_refs.iter().all(|&r| completed_after(r)) {
            // Still waiting after this event: nothing is decided yet.
            return false;
        }
        let captured = captured.get_or_insert_with(|| {
            let mut c = run.captured.clone();
            if let Some(alias) = &config.branches[idx].alias {
                c.insert(alias.clone(), Arc::clone(event));
            }
            c
        });
        if branch
            .predicate
            .as_ref()
            .is_none_or(|p| eval_predicate(p, anchor, captured, evaluator))
        {
            return false;
        }
    }
    true
}

/// Judge the waiting events whose siblings have now matched, until none
/// can move. `true` when the event that anchors the run failed.
fn drain_and_waiting(
    run: &mut Run,
    and_state: &mut AndState,
    config: &AndConfig,
    now: Timestamp,
    evaluator: Option<&dyn ExprEvaluator>,
) -> bool {
    'again: loop {
        let mut i = 0;
        while i < and_state.waiting.len() {
            let event = Arc::clone(&and_state.waiting[i]);
            let mut pending = false;
            for (idx, branch) in config.branches.iter().enumerate() {
                if and_state.is_branch_completed(idx) || *event.event_type != branch.event_type {
                    continue;
                }
                if !branch
                    .sibling_refs
                    .iter()
                    .all(|&r| and_state.is_branch_completed(r))
                {
                    pending = true;
                    continue;
                }
                let pred_matches = branch
                    .predicate
                    .as_ref()
                    .is_none_or(|p| eval_predicate(p, &event, &run.captured, evaluator));
                if pred_matches && admit_event_time(run, &event) {
                    and_state.waiting.remove(i);
                    if and_state
                        .anchor
                        .as_ref()
                        .is_some_and(|a| Arc::ptr_eq(a, &event))
                    {
                        and_state.anchor = None;
                    }
                    complete_and_branch(run, and_state, idx, branch, &event, now);
                    // New captures: every waiting event is judged again.
                    continue 'again;
                }
            }
            if pending {
                i += 1;
                continue;
            }
            // Every branch it could take is judged and refuses it.
            and_state.waiting.remove(i);
            if and_state
                .anchor
                .as_ref()
                .is_some_and(|a| Arc::ptr_eq(a, &event))
            {
                return true;
            }
        }
        return false;
    }
}

fn complete_and_branch(
    run: &mut Run,
    and_state: &mut AndState,
    idx: usize,
    branch: &AndBranch,
    event: &SharedEvent,
    now: Timestamp,
) {
    and_state.complete_branch(idx, Arc::clone(event));
    run.push_at(Arc::clone(event), branch.alias.clone(), now);
}

/// Whether `event` fits in the run's event-time WITHIN together with the
/// events the run holds, in whichever order they came.
fn fits_event_time(run: &Run, event: &SharedEvent) -> bool {
    let (Some(start), Some(deadline)) = (run.event_time_started_at, run.event_time_deadline) else {
        return true;
    };
    if event.timestamp > deadline {
        return false;
    }
    if event.timestamp >= start {
        return true;
    }
    let within = deadline - start;
    let latest = run
        .stack
        .iter()
        .map(|e| e.event.timestamp)
        .max()
        .unwrap_or(start);
    latest - event.timestamp <= within
}

/// [`fits_event_time`], and when `event` is the earliest so far, start the
/// window at it.
fn admit_event_time(run: &mut Run, event: &SharedEvent) -> bool {
    if !fits_event_time(run, event) {
        return false;
    }
    if let (Some(start), Some(deadline)) = (run.event_time_started_at, run.event_time_deadline) {
        if event.timestamp < start {
            let within = deadline - start;
            run.event_time_started_at = Some(event.timestamp);
            run.event_time_deadline = Some(event.timestamp + within);
        }
    }
    true
}

/// The run's AND has just completed; it is at the join state.
fn finish_and(
    nfa: &Nfa,
    run: &mut Run,
    limits: KleeneLimits,
    evaluator: Option<&dyn ExprEvaluator>,
    mode: EmissionMode,
) -> RunAdvanceResult {
    if nfa.states[run.current_state].state_type == StateType::Accept {
        return complete_run(run, limits, evaluator, mode, nfa);
    }
    // `(A and B) -> NOT C`: the absence starts when the AND completes.
    enter_negation_if_next(nfa, run);
    RunAdvanceResult::Continue
}
