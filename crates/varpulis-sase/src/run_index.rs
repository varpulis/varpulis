//! IDX-02: predicate index over waiting runs (prototype, opt-in).
//!
//! Without it, every event visits every non-partitioned run. A run waiting
//! for `B where id == a.id` is offered every `B` and turns nearly all of them
//! down: ten thousand open orders cost ten thousand predicate evaluations per
//! event, whether or not the event concerns any of them.
//!
//! The values a waiting run compares against are fields of events it has
//! already captured, so they stay fixed for as long as it waits. The index
//! files the run under them, and an event looks up the runs it can advance
//! instead of asking each one:
//!
//! - an equality conjunct `field == alias.f` files the run in a hash bucket
//!   under the captured value (several equalities make a composite key);
//! - an order conjunct (`<`, `<=`, `>`, `>=`) files it in an ordered set under
//!   the captured bound; a lower and an upper bound on the same field make an
//!   interval, found with a stabbing query.
//!
//! This is the textbook layout of content-based publish/subscribe matching
//! and of active-database predicate indexing: equality hashing first, ordered
//! bounds next, a residual check last.
//!
//! # Narrowing only
//!
//! Every run the index hands back is still advanced by `advance_run_shared`,
//! which evaluates the whole predicate. Candidates may be a superset of the
//! runs the event advances (hash collisions, `<` searched as `<=`, integers
//! compared as `f64`), never a subset. A run is filed only when that
//! guarantee is plain to see:
//!
//! - its state has exactly one outgoing transition and no epsilon
//!   transition, to a plain (`Normal` or `Accept`, non-Kleene) step with an
//!   event type, so the event's type and that step's predicate decide alone
//!   whether the run can move;
//! - the run has no pending negation, AND state or Kleene capture, any of
//!   which can react to events the predicate does not mention;
//! - the values it is filed under are scalars the comparison understands.
//!
//! Every other run stays in the engine's list and is visited as before. The
//! engine also keeps the index off when the shape of the whole pattern rules
//! it out (strict contiguity, global negations, partitioning): see
//! `SaseEngine::with_predicate_index`.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use rustc_hash::{FxHashMap, FxHasher};
use varpulis_core::{Event, Value};

use crate::clock::Timestamp;
use crate::nfa::{Nfa, StateType};
use crate::run::Run;
use crate::types::{CompareOp, Predicate};

/// Position of a parked run in the slab.
type Slot = u32;

/// A field of a captured event: what a waiting run compares against.
#[derive(Debug, Clone)]
struct CapturedRef {
    alias: String,
    field: String,
}

impl CapturedRef {
    /// The value, looked up exactly as `eval_predicate` looks it up.
    fn value<'r>(&self, run: &'r Run) -> Option<&'r Value> {
        run.captured
            .get(&self.alias)
            .and_then(|event| event.get(&self.field))
    }
}

/// `event.field == captured`.
#[derive(Debug, Clone)]
struct EqTerm {
    event_field: String,
    captured: CapturedRef,
}

/// Which side(s) of an event field the step's predicate bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bounds {
    /// `event.field > captured` or `>=`.
    Lower,
    /// `event.field < captured` or `<=`.
    Upper,
    /// Both: the run waits for a value inside an interval.
    Both,
}

/// One ordered dimension: an event field bounded by captured values.
#[derive(Debug, Clone)]
struct RangePlan {
    event_field: String,
    bounds: Bounds,
    lower: Option<CapturedRef>,
    upper: Option<CapturedRef>,
}

/// How the runs waiting in one NFA state are filed.
#[derive(Debug, Clone)]
struct StatePlan {
    /// Equality dimensions, hashed together into the bucket key.
    eq: Vec<EqTerm>,
    /// At most one ordered dimension, searched inside the bucket.
    range: Option<RangePlan>,
}

/// An `f64` that is never NaN and never `-0.0`, so `total_cmp` agrees with
/// the `partial_cmp` the predicates use.
#[derive(Debug, Clone, Copy)]
struct OrdF64(f64);

impl PartialEq for OrdF64 {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// A bound as the ordered sets keep it.
#[derive(Debug, Clone)]
enum OrdKey {
    Num(OrdF64),
    Text(Box<str>),
}

/// What a run is filed under, computed from the run before it moves.
#[derive(Debug)]
pub(crate) struct ParkKey {
    eq_hash: u64,
    range: RangeKey,
}

#[derive(Debug, Clone)]
enum RangeKey {
    /// No ordered dimension: the bucket's plain list.
    None,
    /// A one-sided numeric bound (which side is the plan's).
    Num(OrdF64),
    /// A numeric interval `[lo, hi]`, filed under `lo`.
    Interval { lo: OrdF64, hi: OrdF64, len: OrdF64 },
    /// A string bound: the lower one when the plan has one, else the upper.
    Text(Box<str>),
}

/// The runs of one state that share an equality key.
#[derive(Debug, Default)]
struct Bucket {
    /// Runs without an ordered dimension.
    plain: Vec<Slot>,
    /// Numeric bound (or interval start), then slot.
    num: BTreeSet<(OrdF64, Slot)>,
    /// Lengths of the intervals in `num`, counted. The longest bounds how far
    /// below an event value an interval containing that value can start.
    lens: BTreeMap<OrdF64, u32>,
    /// String bound, then slot.
    text: BTreeSet<(Box<str>, Slot)>,
}

impl Bucket {
    fn is_empty(&self) -> bool {
        self.plain.is_empty() && self.num.is_empty() && self.text.is_empty()
    }
}

/// A run in the index, with what is needed to take it out again.
#[derive(Debug)]
struct Parked {
    run: Run,
    state: usize,
    eq_hash: u64,
    range: RangeKey,
    /// Position in `Bucket::plain`, for `RangeKey::None`.
    plain_pos: usize,
}

/// Runs filed by the values their next step compares against.
#[derive(Debug)]
pub(crate) struct RunIndex {
    /// Per NFA state: how its runs are filed, if they can be.
    plans: Vec<Option<StatePlan>>,
    /// Event type -> the states whose runs wait for it.
    by_type: FxHashMap<String, Vec<usize>>,
    /// Per NFA state: equality key -> bucket.
    buckets: Vec<FxHashMap<u64, Bucket>>,
    slab: Vec<Option<Parked>>,
    free: Vec<Slot>,
    len: usize,
    /// Wall-clock deadlines, for processing-time expiry.
    deadlines: BTreeSet<(Timestamp, Slot)>,
    /// Event-time deadlines, for watermark expiry.
    event_time_deadlines: BTreeSet<(DateTime<Utc>, Slot)>,
    /// Reused between lookups.
    scratch: Vec<Slot>,
}

impl RunIndex {
    /// Plan every state of `nfa`. `None` when no state's runs can be filed,
    /// so an engine that cannot use the index does not carry one.
    pub(crate) fn new(nfa: &Nfa) -> Option<Self> {
        let mut plans = Vec::with_capacity(nfa.states.len());
        let mut by_type: FxHashMap<String, Vec<usize>> = FxHashMap::default();
        for state_id in 0..nfa.states.len() {
            match plan_state(nfa, state_id) {
                Some((event_type, plan)) => {
                    by_type.entry(event_type).or_default().push(state_id);
                    plans.push(Some(plan));
                }
                None => plans.push(None),
            }
        }
        if by_type.is_empty() {
            return None;
        }
        Some(Self {
            buckets: (0..plans.len()).map(|_| FxHashMap::default()).collect(),
            plans,
            by_type,
            slab: Vec::new(),
            free: Vec::new(),
            len: 0,
            deadlines: BTreeSet::new(),
            event_time_deadlines: BTreeSet::new(),
            scratch: Vec::new(),
        })
    }

    /// Number of parked runs.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The parked runs, in no particular order.
    pub(crate) fn runs(&self) -> impl Iterator<Item = &Run> {
        self.slab.iter().flatten().map(|parked| &parked.run)
    }

    /// Where `run` would be filed, or `None` if it has to stay with the
    /// runs every event visits.
    pub(crate) fn key_for(&self, run: &Run) -> Option<ParkKey> {
        let plan = self.plans.get(run.current_state)?.as_ref()?;
        if run.invalidated
            || !run.pending_negations.is_empty()
            || run.and_state.is_some()
            || run.kleene_capture.is_some()
        {
            return None;
        }

        let mut hasher = FxHasher::default();
        for term in &plan.eq {
            if !hash_eq_value(term.captured.value(run)?, &mut hasher) {
                return None;
            }
        }
        let eq_hash = hasher.finish();

        let range = match &plan.range {
            None => RangeKey::None,
            Some(range) => {
                let lower = match &range.lower {
                    Some(c) => Some(ord_key(c.value(run)?)?),
                    None => None,
                };
                let upper = match &range.upper {
                    Some(c) => Some(ord_key(c.value(run)?)?),
                    None => None,
                };
                match (lower, upper) {
                    (Some(OrdKey::Num(lo)), Some(OrdKey::Num(hi))) => RangeKey::Interval {
                        lo,
                        hi,
                        len: interval_len(lo.0, hi.0),
                    },
                    (Some(OrdKey::Num(bound)), None) | (None, Some(OrdKey::Num(bound))) => {
                        RangeKey::Num(bound)
                    }
                    (Some(OrdKey::Text(lo)), Some(OrdKey::Text(_)) | None) => RangeKey::Text(lo),
                    (None, Some(OrdKey::Text(hi))) => RangeKey::Text(hi),
                    // A string bound and a numeric one: no value satisfies
                    // both, which is the predicate's business, not ours.
                    _ => return None,
                }
            }
        };
        Some(ParkKey { eq_hash, range })
    }

    /// File `run` under `key`, which `key_for` computed for this same run.
    pub(crate) fn insert(&mut self, run: Run, key: ParkKey) {
        let state = run.current_state;
        let deadline = run.deadline;
        let event_time_deadline = run.event_time_deadline;
        let slot = self.alloc(Parked {
            run,
            state,
            eq_hash: key.eq_hash,
            range: key.range.clone(),
            plain_pos: 0,
        });

        let bucket = self.buckets[state].entry(key.eq_hash).or_default();
        match key.range {
            RangeKey::None => {
                let pos = bucket.plain.len();
                bucket.plain.push(slot);
                if let Some(parked) = self.slab[slot as usize].as_mut() {
                    parked.plain_pos = pos;
                }
            }
            RangeKey::Num(bound) => {
                bucket.num.insert((bound, slot));
            }
            RangeKey::Interval { lo, len, .. } => {
                bucket.num.insert((lo, slot));
                *bucket.lens.entry(len).or_insert(0) += 1;
            }
            RangeKey::Text(bound) => {
                bucket.text.insert((bound, slot));
            }
        }

        if let Some(deadline) = deadline {
            self.deadlines.insert((deadline, slot));
        }
        if let Some(deadline) = event_time_deadline {
            self.event_time_deadlines.insert((deadline, slot));
        }
        self.len += 1;
    }

    /// Take out every parked run that `event` might advance, appending them
    /// to `out`. Runs left behind are runs the event cannot move.
    pub(crate) fn take_candidates(&mut self, event: &Event, out: &mut Vec<Run>) {
        if self.len == 0 {
            return;
        }
        let Some(states) = self.by_type.get(&*event.event_type) else {
            return;
        };

        let mut slots = std::mem::take(&mut self.scratch);
        slots.clear();
        for &state in states {
            let Some(plan) = self.plans[state].as_ref() else {
                continue;
            };
            let mut hasher = FxHasher::default();
            let hashed = plan.eq.iter().all(|term| {
                event
                    .get(&term.event_field)
                    .is_some_and(|value| hash_eq_value(value, &mut hasher))
            });
            // A missing or unhashable field fails its equality conjunct for
            // every run of this state.
            if !hashed {
                continue;
            }
            let Some(bucket) = self.buckets[state].get(&hasher.finish()) else {
                continue;
            };

            let Some(range) = &plan.range else {
                slots.extend_from_slice(&bucket.plain);
                continue;
            };
            let Some(value) = event.get(&range.event_field) else {
                continue;
            };
            match value {
                Value::Int(_) | Value::Float(_) => {
                    let Some(OrdKey::Num(x)) = ord_key(value) else {
                        continue; // NaN compares false with everything
                    };
                    collect_num(bucket, range.bounds, x, &self.slab, &mut slots);
                }
                Value::Str(x) => collect_text(bucket, range.bounds, x, &mut slots),
                _ => {}
            }
        }

        out.reserve(slots.len());
        for &slot in &slots {
            out.push(self.take(slot));
        }
        self.scratch = slots;
    }

    /// Drop the runs whose wall-clock deadline has passed (`Run::is_timed_out`).
    pub(crate) fn expire_processing_time(&mut self, now: Timestamp) -> usize {
        let mut expired = 0;
        while let Some(&(deadline, slot)) = self.deadlines.first() {
            if now <= deadline {
                break;
            }
            drop(self.take(slot));
            expired += 1;
        }
        expired
    }

    /// Drop the runs the watermark has passed (`Run::is_timed_out_event_time`).
    pub(crate) fn expire_event_time(&mut self, watermark: DateTime<Utc>) -> usize {
        let mut expired = 0;
        while let Some(&(deadline, slot)) = self.event_time_deadlines.first() {
            if watermark <= deadline {
                break;
            }
            drop(self.take(slot));
            expired += 1;
        }
        expired
    }

    /// Take every run out, leaving the index empty.
    pub(crate) fn drain(&mut self) -> Vec<Run> {
        let runs = self
            .slab
            .drain(..)
            .flatten()
            .map(|parked| parked.run)
            .collect();
        for buckets in &mut self.buckets {
            buckets.clear();
        }
        self.free.clear();
        self.deadlines.clear();
        self.event_time_deadlines.clear();
        self.len = 0;
        runs
    }

    fn alloc(&mut self, parked: Parked) -> Slot {
        if let Some(slot) = self.free.pop() {
            self.slab[slot as usize] = Some(parked);
            slot
        } else {
            self.slab.push(Some(parked));
            Slot::try_from(self.slab.len() - 1).expect("more than u32::MAX parked runs")
        }
    }

    /// Remove the run at `slot` from every structure that refers to it.
    fn take(&mut self, slot: Slot) -> Run {
        let parked = self.slab[slot as usize]
            .take()
            .expect("index refers to an empty slot");
        self.free.push(slot);
        self.len -= 1;

        let buckets = &mut self.buckets[parked.state];
        if let Some(bucket) = buckets.get_mut(&parked.eq_hash) {
            match &parked.range {
                RangeKey::None => {
                    bucket.plain.swap_remove(parked.plain_pos);
                    if let Some(&moved) = bucket.plain.get(parked.plain_pos) {
                        if let Some(other) = self.slab[moved as usize].as_mut() {
                            other.plain_pos = parked.plain_pos;
                        }
                    }
                }
                RangeKey::Num(bound) => {
                    bucket.num.remove(&(*bound, slot));
                }
                RangeKey::Interval { lo, len, .. } => {
                    bucket.num.remove(&(*lo, slot));
                    if let Some(count) = bucket.lens.get_mut(len) {
                        *count -= 1;
                        if *count == 0 {
                            bucket.lens.remove(len);
                        }
                    }
                }
                RangeKey::Text(bound) => {
                    bucket.text.remove(&(bound.clone(), slot));
                }
            }
            if bucket.is_empty() {
                buckets.remove(&parked.eq_hash);
            }
        }

        if let Some(deadline) = parked.run.deadline {
            self.deadlines.remove(&(deadline, slot));
        }
        if let Some(deadline) = parked.run.event_time_deadline {
            self.event_time_deadlines.remove(&(deadline, slot));
        }
        parked.run
    }
}

/// Slots of the numeric entries of `bucket` that may hold `x`.
fn collect_num(
    bucket: &Bucket,
    bounds: Bounds,
    x: OrdF64,
    slab: &[Option<Parked>],
    out: &mut Vec<Slot>,
) {
    match bounds {
        // `x > lo` (or `>=`): every run whose bound is at most `x`.
        Bounds::Lower => out.extend(bucket.num.range(..=(x, Slot::MAX)).map(|&(_, s)| s)),
        // `x < hi` (or `<=`): every run whose bound is at least `x`.
        Bounds::Upper => out.extend(bucket.num.range((x, 0)..).map(|&(_, s)| s)),
        // `lo <= x <= hi`: an interval holding `x` starts no further below it
        // than the longest interval is long. One step down absorbs the
        // rounding of the subtraction.
        Bounds::Both => {
            let Some((&max_len, _)) = bucket.lens.last_key_value() else {
                return;
            };
            let start = x.0 - max_len.0;
            let start = if start.is_nan() {
                f64::NEG_INFINITY
            } else {
                start.next_down()
            };
            for &(_, slot) in bucket.num.range((OrdF64(start), 0)..=(x, Slot::MAX)) {
                let holds_x = slab[slot as usize].as_ref().is_some_and(
                    |parked| matches!(parked.range, RangeKey::Interval { hi, .. } if hi >= x),
                );
                if holds_x {
                    out.push(slot);
                }
            }
        }
    }
}

/// Slots of the string entries of `bucket` that may hold `x`. An interval is
/// searched on its lower bound only; the predicate checks the upper one.
fn collect_text(bucket: &Bucket, bounds: Bounds, x: &str, out: &mut Vec<Slot>) {
    if bucket.text.is_empty() {
        return;
    }
    let x: Box<str> = x.into();
    match bounds {
        Bounds::Lower | Bounds::Both => {
            out.extend(bucket.text.range(..=(x, Slot::MAX)).map(|(_, s)| *s));
        }
        Bounds::Upper => out.extend(bucket.text.range((x, 0)..).map(|(_, s)| *s)),
    }
}

/// How runs waiting in `state_id` can be filed, and the event type they
/// wait for; `None` when they cannot be.
fn plan_state(nfa: &Nfa, state_id: usize) -> Option<(String, StatePlan)> {
    let state = &nfa.states[state_id];
    if state.state_type != StateType::Normal
        || state.self_loop
        || !state.epsilon_transitions.is_empty()
        || state.and_config.is_some()
        || state.negation_info.is_some()
    {
        return None;
    }
    let [next_id] = state.transitions[..] else {
        return None;
    };
    let next = &nfa.states[next_id];
    if !matches!(next.state_type, StateType::Normal | StateType::Accept)
        || next.self_loop
        || next.and_config.is_some()
        || next.negation_info.is_some()
        || next.postponed_predicate.is_some()
    {
        return None;
    }
    let event_type = next.event_type.clone()?;

    let mut conjuncts = Vec::new();
    collect_conjuncts(next.predicate.as_ref()?, &mut conjuncts);

    let mut eq = Vec::new();
    let mut ranges: Vec<RangePlan> = Vec::new();
    for conjunct in conjuncts {
        let Predicate::CompareRef {
            field,
            op,
            ref_alias,
            ref_field,
        } = conjunct
        else {
            continue;
        };
        // The step's own alias is not bound yet when its predicate runs.
        if next.alias.as_deref() == Some(ref_alias.as_str()) {
            continue;
        }
        let captured = CapturedRef {
            alias: ref_alias.clone(),
            field: ref_field.clone(),
        };
        let is_lower = match op {
            CompareOp::Eq => {
                eq.push(EqTerm {
                    event_field: field.clone(),
                    captured,
                });
                continue;
            }
            CompareOp::NotEq => continue,
            CompareOp::Gt | CompareOp::Ge => true,
            CompareOp::Lt | CompareOp::Le => false,
        };
        let range = if let Some(i) = ranges.iter().position(|r| r.event_field == *field) {
            &mut ranges[i]
        } else {
            ranges.push(RangePlan {
                event_field: field.clone(),
                bounds: Bounds::Lower,
                lower: None,
                upper: None,
            });
            ranges.last_mut().expect("just pushed")
        };
        let side = if is_lower {
            &mut range.lower
        } else {
            &mut range.upper
        };
        // Any one bound per side narrows correctly; keep the first.
        if side.is_none() {
            *side = Some(captured);
        }
    }

    for range in &mut ranges {
        range.bounds = match (&range.lower, &range.upper) {
            (Some(_), Some(_)) => Bounds::Both,
            (Some(_), None) => Bounds::Lower,
            _ => Bounds::Upper,
        };
    }
    // An interval narrows the most: prefer a field bounded on both sides.
    let pick = ranges
        .iter()
        .position(|r| r.bounds == Bounds::Both)
        .or(if ranges.is_empty() { None } else { Some(0) });
    let range = pick.map(|i| ranges.swap_remove(i));

    if eq.is_empty() && range.is_none() {
        return None;
    }
    Some((event_type, StatePlan { eq, range }))
}

/// The top-level conjuncts of `predicate`: a run that satisfies it satisfies
/// each of them, so any one can narrow the search.
fn collect_conjuncts<'p>(predicate: &'p Predicate, out: &mut Vec<&'p Predicate>) {
    match predicate {
        Predicate::And(left, right) => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// Feed `value` to `hasher` so that values `Value::vpl_eq` calls equal hash
/// alike. `false` for the kinds the index does not file.
fn hash_eq_value(value: &Value, hasher: &mut FxHasher) -> bool {
    match value {
        Value::Bool(b) => {
            0u8.hash(hasher);
            b.hash(hasher);
        }
        // `vpl_eq` widens an integer to compare it with a float, so both
        // kinds hash the widened value. Integers that widen to the same f64
        // share a bucket; the predicate tells them apart.
        Value::Int(i) => {
            1u8.hash(hasher);
            canonical_bits(*i as f64).hash(hasher);
        }
        Value::Float(f) => {
            1u8.hash(hasher);
            canonical_bits(*f).hash(hasher);
        }
        Value::Str(s) => {
            2u8.hash(hasher);
            s.hash(hasher);
        }
        _ => return false,
    }
    true
}

/// One bit pattern per equality class of `float_eq`: every NaN, and both
/// zeros, compare equal there.
fn canonical_bits(f: f64) -> u64 {
    if f.is_nan() {
        f64::NAN.to_bits()
    } else if f == 0.0 {
        0
    } else {
        f.to_bits()
    }
}

/// `value` as an ordered-set key, for the kinds `<` and `>` compare.
/// Integers widen to `f64` as the comparison does when kinds mix; when they
/// do not mix, the widening is monotonic, so an inclusive search cannot miss.
fn ord_key(value: &Value) -> Option<OrdKey> {
    let num = |f: f64| OrdF64(if f == 0.0 { 0.0 } else { f });
    match value {
        Value::Int(i) => Some(OrdKey::Num(num(*i as f64))),
        Value::Float(f) if !f.is_nan() => Some(OrdKey::Num(num(*f))),
        Value::Str(s) => Some(OrdKey::Text(s.clone())),
        _ => None,
    }
}

/// Length of `[lo, hi]`, rounded up so it never understates the interval.
fn interval_len(lo: f64, hi: f64) -> OrdF64 {
    let len = hi - lo;
    OrdF64(if len.is_nan() {
        f64::INFINITY
    } else if len <= 0.0 {
        0.0
    } else {
        len.next_up()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::nfa::NfaCompiler;
    use crate::types::SasePattern;

    fn ev(event_type: &str, fields: &[(&str, Value)]) -> Event {
        let mut event = Event::new(event_type);
        for (k, v) in fields {
            event = event.with_field(*k, v.clone());
        }
        event
    }

    fn cref(field: &str, op: CompareOp, alias: &str, ref_field: &str) -> Predicate {
        Predicate::CompareRef {
            field: field.into(),
            op,
            ref_alias: alias.into(),
            ref_field: ref_field.into(),
        }
    }

    fn and(l: Predicate, r: Predicate) -> Predicate {
        Predicate::And(Box::new(l), Box::new(r))
    }

    /// `A as a -> B where <pred>`.
    fn seq(pred: Predicate) -> Nfa {
        NfaCompiler::new().compile(&SasePattern::Seq(vec![
            SasePattern::Event {
                event_type: "A".into(),
                predicate: None,
                alias: Some("a".into()),
            },
            SasePattern::Event {
                event_type: "B".into(),
                predicate: Some(pred),
                alias: Some("b".into()),
            },
        ]))
    }

    /// A run that matched `a` and waits in the state after it.
    fn run_after(nfa: &Nfa, a: Event) -> Run {
        let state = nfa.states[nfa.start_state].transitions[0];
        let mut run = Run::new(state);
        run.captured.insert("a".into(), Arc::new(a));
        run
    }

    fn park(index: &mut RunIndex, run: Run) {
        let key = index.key_for(&run).expect("indexable run");
        index.insert(run, key);
    }

    fn candidates(index: &mut RunIndex, event: &Event) -> Vec<Run> {
        let mut out = Vec::new();
        index.take_candidates(event, &mut out);
        out
    }

    #[test]
    fn equality_finds_only_the_run_with_the_key() {
        let nfa = seq(cref("key", CompareOp::Eq, "a", "key"));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        for k in 0..100 {
            park(
                &mut index,
                run_after(&nfa, ev("A", &[("key", Value::Int(k))])),
            );
        }
        let hit = candidates(&mut index, &ev("B", &[("key", Value::Int(42))]));
        assert_eq!(hit.len(), 1);
        assert_eq!(index.len(), 99);
        assert!(candidates(&mut index, &ev("B", &[("key", Value::Int(1000))])).is_empty());
        // Wrong type, or the field missing: nothing.
        assert!(candidates(&mut index, &ev("C", &[("key", Value::Int(7))])).is_empty());
        assert!(candidates(&mut index, &ev("B", &[])).is_empty());
        assert_eq!(index.len(), 99);
    }

    #[test]
    fn equality_widens_like_vpl_eq() {
        let nfa = seq(cref("key", CompareOp::Eq, "a", "key"));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        park(
            &mut index,
            run_after(&nfa, ev("A", &[("key", Value::Int(3))])),
        );
        park(
            &mut index,
            run_after(&nfa, ev("A", &[("key", Value::Float(-0.0))])),
        );
        park(
            &mut index,
            run_after(&nfa, ev("A", &[("key", Value::Float(f64::NAN))])),
        );
        assert_eq!(
            candidates(&mut index, &ev("B", &[("key", Value::Float(3.0))])).len(),
            1
        );
        assert_eq!(
            candidates(&mut index, &ev("B", &[("key", Value::Int(0))])).len(),
            1
        );
        assert_eq!(
            candidates(&mut index, &ev("B", &[("key", Value::Float(f64::NAN))])).len(),
            1
        );
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn unfileable_values_stay_out() {
        let nfa = seq(cref("key", CompareOp::Eq, "a", "key"));
        let index = RunIndex::new(&nfa).expect("plannable");
        assert!(index
            .key_for(&run_after(&nfa, ev("A", &[("key", Value::Null)])))
            .is_none());
        assert!(index.key_for(&run_after(&nfa, ev("A", &[]))).is_none());
        let mut negated = run_after(&nfa, ev("A", &[("key", Value::Int(1))]));
        negated.invalidated = true;
        assert!(index.key_for(&negated).is_none());
    }

    #[test]
    fn one_sided_bounds() {
        let nfa = seq(cref("price", CompareOp::Gt, "a", "limit"));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        for limit in 0..10 {
            park(
                &mut index,
                run_after(&nfa, ev("A", &[("limit", Value::Int(limit))])),
            );
        }
        // `price > limit` for limit 0..=4 (and 5, searched as `>=`).
        let woken = candidates(&mut index, &ev("B", &[("price", Value::Float(5.0))]));
        assert_eq!(woken.len(), 6);
        assert_eq!(index.len(), 4);

        let nfa = seq(cref("price", CompareOp::Le, "a", "cap"));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        for cap in 0..10 {
            park(
                &mut index,
                run_after(&nfa, ev("A", &[("cap", Value::Int(cap))])),
            );
        }
        assert_eq!(
            candidates(&mut index, &ev("B", &[("price", Value::Int(7))])).len(),
            3
        );
    }

    #[test]
    fn intervals_are_stabbed() {
        let nfa = seq(and(
            cref("price", CompareOp::Ge, "a", "lo"),
            cref("price", CompareOp::Lt, "a", "hi"),
        ));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        for i in 0..1000 {
            let lo = f64::from(i);
            park(
                &mut index,
                run_after(
                    &nfa,
                    ev(
                        "A",
                        &[("lo", Value::Float(lo)), ("hi", Value::Float(lo + 1.0))],
                    ),
                ),
            );
        }
        // A long interval widens the search window but must still be found.
        park(
            &mut index,
            run_after(
                &nfa,
                ev(
                    "A",
                    &[("lo", Value::Float(-1e6)), ("hi", Value::Float(1e6))],
                ),
            ),
        );
        let woken = candidates(&mut index, &ev("B", &[("price", Value::Float(500.5))]));
        assert_eq!(woken.len(), 2);
        // Taken out, the long interval no longer widens the search. Bounds
        // are searched inclusively ([9, 10) comes back for 10); the
        // predicate decides.
        let woken = candidates(&mut index, &ev("B", &[("price", Value::Int(10))]));
        assert_eq!(woken.len(), 2);
        assert_eq!(index.len(), 1001 - 4);
    }

    #[test]
    fn equality_then_interval() {
        let nfa = seq(and(
            cref("key", CompareOp::Eq, "a", "key"),
            and(
                cref("price", CompareOp::Ge, "a", "lo"),
                cref("price", CompareOp::Lt, "a", "hi"),
            ),
        ));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        for key in 0..10 {
            for lo in 0..10 {
                let lo = f64::from(lo);
                park(
                    &mut index,
                    run_after(
                        &nfa,
                        ev(
                            "A",
                            &[
                                ("key", Value::Int(key)),
                                ("lo", Value::Float(lo)),
                                ("hi", Value::Float(lo + 1.0)),
                            ],
                        ),
                    ),
                );
            }
        }
        let woken = candidates(
            &mut index,
            &ev("B", &[("key", Value::Int(3)), ("price", Value::Float(4.5))]),
        );
        assert_eq!(woken.len(), 1);
        assert_eq!(woken[0].captured["a"].get("lo"), Some(&Value::Float(4.0)));
    }

    #[test]
    fn strings_are_ordered_and_hashed() {
        let nfa = seq(cref("name", CompareOp::Ge, "a", "from"));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        for from in ["b", "d", "f"] {
            park(
                &mut index,
                run_after(&nfa, ev("A", &[("from", Value::Str(from.into()))])),
            );
        }
        assert_eq!(
            candidates(&mut index, &ev("B", &[("name", Value::Str("e".into()))])).len(),
            2
        );
        // A number never orders against a string.
        assert!(candidates(&mut index, &ev("B", &[("name", Value::Int(1))])).is_empty());
    }

    #[test]
    fn removal_keeps_plain_positions_consistent() {
        let nfa = seq(cref("key", CompareOp::Eq, "a", "key"));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        // Five runs under one key, then take them out from the middle.
        for _ in 0..5 {
            park(
                &mut index,
                run_after(&nfa, ev("A", &[("key", Value::Int(1))])),
            );
        }
        let slot_of_third = 2;
        drop(index.take(slot_of_third));
        drop(index.take(0));
        assert_eq!(index.len(), 3);
        assert_eq!(
            candidates(&mut index, &ev("B", &[("key", Value::Int(1))])).len(),
            3
        );
        assert!(index.buckets.iter().all(FxHashMap::is_empty));
        assert!(index.drain().is_empty());
    }

    #[test]
    fn deadlines_expire_parked_runs() {
        let nfa = seq(cref("key", CompareOp::Eq, "a", "key"));
        let mut index = RunIndex::new(&nfa).expect("plannable");
        let base = DateTime::from_timestamp(1_000, 0).expect("valid");
        for k in 0..10 {
            let mut run = run_after(&nfa, ev("A", &[("key", Value::Int(k))]));
            run.event_time_deadline = Some(base + chrono::Duration::seconds(k));
            park(&mut index, run);
        }
        assert_eq!(
            index.expire_event_time(base + chrono::Duration::seconds(4)),
            4
        );
        assert_eq!(index.len(), 6);
        assert!(candidates(&mut index, &ev("B", &[("key", Value::Int(2))])).is_empty());
        assert_eq!(
            candidates(&mut index, &ev("B", &[("key", Value::Int(7))])).len(),
            1
        );
    }

    #[test]
    fn plans_refuse_what_they_cannot_narrow() {
        // No captured value to file under.
        assert!(RunIndex::new(&seq(Predicate::Compare {
            field: "x".into(),
            op: CompareOp::Eq,
            value: Value::Int(1),
        }))
        .is_none());
        // A disjunction cannot be narrowed by either side.
        assert!(RunIndex::new(&seq(Predicate::Or(
            Box::new(cref("k", CompareOp::Eq, "a", "k")),
            Box::new(cref("j", CompareOp::Eq, "a", "j")),
        )))
        .is_none());
        // Inequality says nothing about where to look.
        assert!(RunIndex::new(&seq(cref("k", CompareOp::NotEq, "a", "k"))).is_none());
    }
}
