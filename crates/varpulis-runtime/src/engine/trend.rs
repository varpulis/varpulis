//! `.trend_aggregate()`: statistics over every trend a pattern matches in a
//! window, computed by counting on the event graph instead of enumerating
//! the trends.
//!
//! A *trend* is one match of the stream's pattern under skip-till-any-match
//! with every non-empty subset of each closure: what `.subsets()` enumerates
//! in detection mode. A closure over `n` qualifying events gives `2^n - 1`
//! trends from one start, which no engine should materialise. The counts
//! come from a dynamic programme over the events of a window (GRETA, Poppe
//! et al., VLDB 2017): for each event and each step of the pattern, the
//! number of partial trends that reach the event at that step is the sum
//! over the events that can precede it there, and the number of completions
//! from that point is the symmetric sum over the events that can follow it.
//! Their product is the number of trends the event takes part in at that
//! step, and every aggregate is a sum over those.
//!
//! The window is a tumbling one of the pattern's `.within()`: it opens, per
//! partition, with its first event and closes when an event arrives at or
//! past its end, when the event clock passes its end, or at the end of the
//! input. A trend lies inside one window. The result is one event per
//! partition and window, stamped with the window's end, carrying the
//! partition's key field, the aggregates, and the fields of the events the
//! single steps bound (the earliest start, the latest event of a later
//! step) as `alias.field`, for `.where()` and `.emit()` below.
//!
//! A step's condition is evaluated as detection mode evaluates it: against
//! the current event, with the start's alias bound to the trend's start, a
//! closure's own alias bound to the closure's previous event (at its first
//! event, to the step before), and the previous step's alias bound to that
//! step's event. Detection mode's greedy treatment of a self-referencing
//! closure (one run at a time, `.longest()` by default) does not apply here:
//! every trend counts.

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use rustc_hash::FxHashMap;
use varpulis_core::ast::{Arg, Expr, MonotonicOp};
use varpulis_core::{Event, Value};

use super::evaluator::eval_filter_expr;
use crate::event::SharedEvent;
use crate::sequence::SequenceContext;

/// Event type of a result, before `.emit()` renames it after the stream.
pub(super) static TREND_RESULT_TYPE: std::sync::LazyLock<Arc<str>> =
    std::sync::LazyLock::new(|| Arc::from("TrendAggregateResult"));

/// Events one partition's window holds at most. Counting is quadratic in
/// the window's size; past the cap the window drops what arrives and its
/// result says so in `_trend_truncated`.
pub const MAX_WINDOW_EVENTS: usize = 5_000;

/// One step of the pattern as trend aggregation reads it. The first step is
/// the start, a single event; a later step is a single event or a closure.
#[derive(Debug, Clone)]
pub struct TrendStep {
    /// The event type the step matches.
    pub event_type: String,
    /// The alias the step binds, if any.
    pub alias: Option<String>,
    /// The step's `where` condition.
    pub filter: Option<Expr>,
    /// `all T`: one or more events, every non-empty subset a trend.
    pub kleene: bool,
    /// `.increasing(f)` / `.decreasing(f)` on a closure.
    pub monotonic: Option<MonotonicOp>,
}

/// What one `.trend_aggregate()` item computes over the window's trends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrendAggregate {
    /// `count_trends()`: the number of trends.
    CountTrends,
    /// `count_events(alias)`: the number of times an event of that step
    /// takes part in a trend, summed over the trends (an event in five
    /// trends counts five times); without an alias, over every step.
    CountEvents(Option<String>),
    /// `sum_trends(alias.field)`: the field summed over every event of that
    /// step in every trend.
    Sum {
        /// The step's alias.
        alias: String,
        /// The field read on its events.
        field: String,
    },
    /// `avg_trends(alias.field)`: `sum_trends` divided by `count_trends`;
    /// null without a trend.
    Avg {
        /// The step's alias.
        alias: String,
        /// The field read on its events.
        field: String,
    },
    /// `min_trends(alias.field)`: the smallest value of the field over the
    /// events of that step that take part in a trend; null without one.
    Min {
        /// The step's alias.
        alias: String,
        /// The field read on its events.
        field: String,
    },
    /// `max_trends(alias.field)`: the largest such value.
    Max {
        /// The step's alias.
        alias: String,
        /// The field read on its events.
        field: String,
    },
}

/// One partition's open window.
#[derive(Debug)]
struct TrendWindow {
    /// The timestamp of its first event; it ends `within` later.
    opened: DateTime<Utc>,
    /// The partition key's value on the first event, as the result carries it.
    key: Option<Value>,
    events: Vec<SharedEvent>,
    /// Events dropped past [`MAX_WINDOW_EVENTS`].
    dropped: u64,
}

/// The state of one stream's `.trend_aggregate()`: its pattern, its
/// aggregates and the open window of each partition.
#[derive(Debug)]
pub struct TrendAggregateConfig {
    steps: Vec<TrendStep>,
    aggregates: Vec<(Arc<str>, TrendAggregate)>,
    within: Duration,
    partition_key: Option<String>,
    /// Whether a later step's condition reads the start's alias: the counts
    /// are then computed start by start, since the condition depends on it.
    references_start: bool,
    /// Per step: whether its condition reads an alias at all. One that does
    /// not depends on the event alone and is evaluated once per event.
    reads_alias: Vec<bool>,
    max_events: usize,
    windows: FxHashMap<String, TrendWindow>,
    /// Every open window's end with its partition key, earliest first, so
    /// the event clock closes the windows it reached without visiting the
    /// others.
    ends: BTreeSet<(DateTime<Utc>, String)>,
}

/// How an event follows another in a trend.
#[derive(Debug, Clone, Copy)]
enum Move {
    /// The event is the first of the next step.
    Enter,
    /// The event extends the closure the previous event is in.
    Extend,
}

impl TrendAggregateConfig {
    /// A trend aggregate over `steps` (the start first), computing
    /// `aggregates`, per tumbling window of `within`, per value of
    /// `partition_key` when given.
    pub fn new(
        steps: Vec<TrendStep>,
        aggregates: Vec<(Arc<str>, TrendAggregate)>,
        within: Duration,
        partition_key: Option<String>,
    ) -> Self {
        let aliases: Vec<&str> = steps.iter().filter_map(|s| s.alias.as_deref()).collect();
        let reads_alias = steps
            .iter()
            .map(|s| {
                s.filter.as_ref().is_some_and(|f| {
                    let mut read = Vec::new();
                    aliases_read(f, &mut read);
                    read.iter().any(|r| aliases.contains(&r.as_str()))
                })
            })
            .collect();
        let references_start = steps[0].alias.as_deref().is_some_and(|start| {
            steps[1..].iter().any(|s| {
                s.filter.as_ref().is_some_and(|f| {
                    let mut read = Vec::new();
                    aliases_read(f, &mut read);
                    read.iter().any(|r| r == start)
                })
            })
        });
        Self {
            steps,
            aggregates,
            within,
            partition_key,
            references_start,
            reads_alias,
            max_events: MAX_WINDOW_EVENTS,
            windows: FxHashMap::default(),
            ends: BTreeSet::new(),
        }
    }

    /// Whether the counts are computed start by start (a condition reads
    /// the start's alias) or in one pass over every start.
    #[cfg(test)]
    const fn counts_per_start(&self) -> bool {
        self.references_start
    }

    /// File an event into its partition's window. Comes back with the
    /// result of the window the event closed, when it arrived at or past
    /// the window's end: the event then opens the next one.
    pub fn add(&mut self, event: SharedEvent) -> Option<SharedEvent> {
        let key = self
            .partition_key
            .as_ref()
            .map_or_else(String::new, |field| {
                event.get(field).map_or_else(
                    || "default".to_string(),
                    |v| v.to_partition_key().into_owned(),
                )
            });
        let ts = event.timestamp;
        let mut closed = None;
        if self
            .windows
            .get(&key)
            .is_some_and(|w| ts >= w.opened + self.within)
        {
            if let Some(window) = self.windows.remove(&key) {
                self.ends
                    .remove(&(window.opened + self.within, key.clone()));
                closed = Some(self.close_window(&window));
            }
        }
        if !self.windows.contains_key(&key) {
            self.ends.insert((ts + self.within, key.clone()));
            let key_value = self
                .partition_key
                .as_ref()
                .and_then(|f| event.get(f).cloned());
            self.windows.insert(
                key.clone(),
                TrendWindow {
                    opened: ts,
                    key: key_value,
                    events: Vec::new(),
                    dropped: 0,
                },
            );
        }
        if let Some(window) = self.windows.get_mut(&key) {
            if window.events.len() >= self.max_events {
                window.dropped += 1;
                if window.dropped == 1 {
                    tracing::warn!(
                        cap = self.max_events,
                        "trend aggregate window holds the most events it counts over; \
                         further events of this window are dropped and its result \
                         carries `_trend_truncated`"
                    );
                }
            } else {
                window.events.push(event);
            }
        }
        closed
    }

    /// Close every window whose end event time `wm` has reached, as an
    /// event of that partition arriving at `wm` would, and come back with
    /// their results, earliest end first.
    pub fn close_through(&mut self, wm: DateTime<Utc>) -> Vec<SharedEvent> {
        let mut results = Vec::new();
        while let Some((end, key)) = self.ends.first().cloned() {
            if end > wm {
                break;
            }
            self.ends.pop_first();
            if let Some(window) = self.windows.remove(&key) {
                results.push(self.close_window(&window));
            }
        }
        results
    }

    /// The open windows, for a checkpoint: each partition's events and the
    /// time its window opened, in the shape the engine's windows use.
    pub fn checkpoint(&self) -> crate::persistence::WindowCheckpoint {
        let partitions = self
            .windows
            .iter()
            .map(|(key, window)| {
                (
                    key.clone(),
                    crate::persistence::PartitionedWindowCheckpoint {
                        events: window
                            .events
                            .iter()
                            .map(|e| crate::persistence::SerializableEvent::from(e.as_ref()))
                            .collect(),
                        window_start_ms: Some(window.opened.timestamp_millis()),
                    },
                )
            })
            .collect();
        crate::persistence::WindowCheckpoint {
            events: Vec::new(),
            window_start_ms: None,
            last_emit_ms: None,
            partitions,
        }
    }

    /// Put back the open windows a checkpoint holds, in place of the
    /// current ones.
    pub fn restore(&mut self, cp: &crate::persistence::WindowCheckpoint) {
        self.windows.clear();
        self.ends.clear();
        for (key, partition) in &cp.partitions {
            let events: Vec<SharedEvent> = partition
                .events
                .iter()
                .map(|se| Arc::new(Event::from(se.clone())))
                .collect();
            let Some(opened) = partition
                .window_start_ms
                .and_then(DateTime::from_timestamp_millis)
                .or_else(|| events.first().map(|e| e.timestamp))
            else {
                continue;
            };
            let key_value = self
                .partition_key
                .as_ref()
                .and_then(|field| events.first().and_then(|e| e.get(field).cloned()));
            self.ends.insert((opened + self.within, key.clone()));
            self.windows.insert(
                key.clone(),
                TrendWindow {
                    opened,
                    key: key_value,
                    events,
                    dropped: 0,
                },
            );
        }
    }

    /// Count the window's trends and build its result.
    fn close_window(&self, window: &TrendWindow) -> SharedEvent {
        let events = &window.events;
        let n = events.len();
        let m = self.steps.len();
        let type_ok: Vec<Vec<bool>> = self
            .steps
            .iter()
            .map(|step| {
                events
                    .iter()
                    .map(|e| *e.event_type == *step.event_type)
                    .collect()
            })
            .collect();
        // A condition that reads no alias depends on the event alone.
        let local_ok: Vec<Option<Vec<bool>>> = self
            .steps
            .iter()
            .enumerate()
            .map(|(i, step)| {
                if self.reads_alias[i] {
                    None
                } else {
                    Some(
                        events
                            .iter()
                            .map(|e| {
                                step.filter.as_ref().is_none_or(|f| {
                                    holds_true(eval_filter_expr(f, e, SequenceContext::empty()))
                                })
                            })
                            .collect(),
                    )
                }
            })
            .collect();
        // participations[step][event]: the trends the event takes part in
        // at that step.
        let mut participations = vec![vec![0u64; n]; m];
        let trends = if self.references_start {
            let mut total = 0u64;
            for s in 0..n {
                if type_ok[0][s] && local_ok[0].as_ref().is_none_or(|ok| ok[s]) {
                    total = total.saturating_add(self.count(
                        events,
                        &type_ok,
                        &local_ok,
                        Some(s),
                        &mut participations,
                    ));
                }
            }
            total
        } else {
            self.count(events, &type_ok, &local_ok, None, &mut participations)
        };

        // Read from here on, so the closures below share one reference.
        let participations = &participations;
        let mut out = Event::with_capacity_at(
            TREND_RESULT_TYPE.clone(),
            self.aggregates.len() + 2 * m + 2,
            window.opened + self.within,
        );
        if let (Some(field), Some(value)) = (&self.partition_key, &window.key) {
            out.data.insert(field.as_str().into(), value.clone());
        }
        let steps_of = |alias: &str| -> Vec<usize> {
            (0..m)
                .filter(|&i| self.steps[i].alias.as_deref() == Some(alias))
                .collect()
        };
        let participations_of = |steps: &[usize]| -> u64 {
            steps
                .iter()
                .flat_map(|&i| participations[i].iter())
                .fold(0u64, |acc, &p| acc.saturating_add(p))
        };
        let values_of = |steps: &[usize], field: &str| -> Vec<(u64, f64)> {
            steps
                .iter()
                .flat_map(|&i| {
                    (0..n).filter_map(move |k| {
                        let p = participations[i][k];
                        (p > 0)
                            .then(|| events[k].get(field).and_then(number).map(|v| (p, v)))
                            .flatten()
                    })
                })
                .collect()
        };
        for (name, aggregate) in &self.aggregates {
            let value = match aggregate {
                TrendAggregate::CountTrends => int(trends),
                TrendAggregate::CountEvents(None) => {
                    int(participations_of(&(0..m).collect::<Vec<_>>()))
                }
                TrendAggregate::CountEvents(Some(alias)) => {
                    int(participations_of(&steps_of(alias)))
                }
                TrendAggregate::Sum { alias, field } => Value::Float(
                    values_of(&steps_of(alias), field)
                        .iter()
                        .map(|(p, v)| *p as f64 * v)
                        .sum(),
                ),
                TrendAggregate::Avg { alias, field } => {
                    if trends == 0 {
                        Value::Null
                    } else {
                        let sum: f64 = values_of(&steps_of(alias), field)
                            .iter()
                            .map(|(p, v)| *p as f64 * v)
                            .sum();
                        Value::Float(sum / trends as f64)
                    }
                }
                TrendAggregate::Min { alias, field } => values_of(&steps_of(alias), field)
                    .iter()
                    .map(|(_, v)| *v)
                    .fold(None, |acc: Option<f64>, v| {
                        Some(acc.map_or(v, |a| a.min(v)))
                    })
                    .map_or(Value::Null, Value::Float),
                TrendAggregate::Max { alias, field } => values_of(&steps_of(alias), field)
                    .iter()
                    .map(|(_, v)| *v)
                    .fold(None, |acc: Option<f64>, v| {
                        Some(acc.map_or(v, |a| a.max(v)))
                    })
                    .map_or(Value::Null, Value::Float),
            };
            out.data.insert(name.clone(), value);
        }
        // What `.emit()` reads of the steps: how many distinct events of each
        // take part in a trend, and for a single step the fields of one of
        // them, the earliest start or the latest event of a later step.
        for (i, step) in self.steps.iter().enumerate() {
            let Some(alias) = &step.alias else {
                continue;
            };
            let taking: Vec<usize> = (0..n).filter(|&k| participations[i][k] > 0).collect();
            out.data
                .insert(format!("_count_{alias}").into(), int(taking.len() as u64));
            if step.kleene {
                continue;
            }
            let bound = if i == 0 {
                taking.first()
            } else {
                taking.last()
            };
            if let Some(&k) = bound {
                for (field, value) in &events[k].data {
                    out.data
                        .insert(format!("{alias}.{field}").into(), value.clone());
                }
            }
        }
        if window.dropped > 0 {
            out.data
                .insert("_trend_truncated".into(), int(window.dropped));
        }
        Arc::new(out)
    }

    /// The dynamic programme over one window, for every start at once or,
    /// when `start` is given, for the trends starting at that event. Adds
    /// each event's participations per step to `participations` and comes
    /// back with the number of trends.
    fn count(
        &self,
        events: &[SharedEvent],
        type_ok: &[Vec<bool>],
        local_ok: &[Option<Vec<bool>>],
        start: Option<usize>,
        participations: &mut [Vec<u64>],
    ) -> u64 {
        let n = events.len();
        let m = self.steps.len();
        let first = start.unwrap_or(0);
        let admits =
            |i: usize, k: usize| type_ok[i][k] && local_ok[i].as_ref().is_none_or(|ok| ok[k]);
        // reach[step][event]: partial trends reaching the event at the step.
        let mut reach = vec![vec![0u64; n]; m];
        for j in first..n {
            let is_start = start.map_or_else(|| admits(0, j), |s| j == s);
            if is_start {
                reach[0][j] = 1;
            }
            for i in 0..m {
                let from = reach[i][j];
                if from == 0 {
                    continue;
                }
                if i + 1 < m {
                    let ctx = self.bindings(events, start, i + 1, j, Move::Enter);
                    for (k, slot) in reach[i + 1].iter_mut().enumerate().skip(j + 1) {
                        if admits(i + 1, k) && self.holds(i + 1, &ctx, events, j, k, Move::Enter) {
                            *slot = slot.saturating_add(from);
                        }
                    }
                }
                if self.steps[i].kleene {
                    let ctx = self.bindings(events, start, i, j, Move::Extend);
                    for (k, slot) in reach[i].iter_mut().enumerate().skip(j + 1) {
                        if admits(i, k) && self.holds(i, &ctx, events, j, k, Move::Extend) {
                            *slot = slot.saturating_add(from);
                        }
                    }
                }
            }
        }
        // complete[step][event]: trends completing from the event at the step.
        let mut complete = vec![vec![0u64; n]; m];
        let mut trends = 0u64;
        for k in (first..n).rev() {
            for i in (0..m).rev() {
                if reach[i][k] == 0 {
                    continue;
                }
                let mut total = u64::from(i + 1 == m);
                if i + 1 < m {
                    let ctx = self.bindings(events, start, i + 1, k, Move::Enter);
                    for next in k + 1..n {
                        if reach[i + 1][next] > 0
                            && self.holds(i + 1, &ctx, events, k, next, Move::Enter)
                        {
                            total = total.saturating_add(complete[i + 1][next]);
                        }
                    }
                }
                if self.steps[i].kleene {
                    let ctx = self.bindings(events, start, i, k, Move::Extend);
                    for next in k + 1..n {
                        if reach[i][next] > 0 && self.holds(i, &ctx, events, k, next, Move::Extend)
                        {
                            total = total.saturating_add(complete[i][next]);
                        }
                    }
                }
                complete[i][k] = total;
                participations[i][k] =
                    participations[i][k].saturating_add(reach[i][k].saturating_mul(total));
                if i == 0 {
                    trends = trends.saturating_add(total);
                }
            }
        }
        trends
    }

    /// The aliases a step's condition sees when `events[from]` leads to the
    /// event under test at `step`: the start, the step before (on entry)
    /// and the closure's previous event, which at its first event is the
    /// step before.
    fn bindings(
        &self,
        events: &[SharedEvent],
        start: Option<usize>,
        step: usize,
        from: usize,
        mv: Move,
    ) -> SequenceContext {
        let mut ctx = SequenceContext::new();
        let mut bind = |alias: &Option<String>, idx: usize| {
            if let Some(alias) = alias {
                ctx.captured.insert(alias.clone(), (*events[idx]).clone());
            }
        };
        if let Some(s) = start {
            bind(&self.steps[0].alias, s);
        }
        match mv {
            Move::Enter => {
                bind(&self.steps[step - 1].alias, from);
                if self.steps[step].kleene {
                    bind(&self.steps[step].alias, from);
                }
            }
            Move::Extend => bind(&self.steps[step].alias, from),
        }
        ctx
    }

    /// Whether `events[to]` may follow `events[from]` at `step`: its
    /// condition holds with the aliases bound, and its monotonic operator,
    /// if any, holds against the previous event. The conditions that read
    /// no alias were settled per event already.
    fn holds(
        &self,
        step: usize,
        ctx: &SequenceContext,
        events: &[SharedEvent],
        from: usize,
        to: usize,
        mv: Move,
    ) -> bool {
        let definition = &self.steps[step];
        if self.reads_alias[step] {
            if let Some(filter) = &definition.filter {
                if !holds_true(eval_filter_expr(filter, &events[to], ctx)) {
                    return false;
                }
            }
        }
        if let Some(monotonic) = &definition.monotonic {
            let (field, rising) = match monotonic {
                MonotonicOp::Increasing(f) => (f, true),
                MonotonicOp::Decreasing(f) => (f, false),
            };
            let previous = events[from].get(field).and_then(number);
            let current = events[to].get(field).and_then(number);
            return match (previous, current) {
                (Some(p), Some(c)) => {
                    if rising {
                        c > p
                    } else {
                        c < p
                    }
                }
                // As in detection mode, the closure's first event is not
                // compared with a step that does not carry the field.
                (None, _) => matches!(mv, Move::Enter),
                (Some(_), None) => false,
            };
        }
        true
    }
}

/// The aliases an expression reads a field of (`a.f`, `a?.f`, `a[i]`,
/// `a[i..j]`, or an identifier spelled `a.f`), anywhere inside it.
pub(super) fn aliases_read(expr: &Expr, out: &mut Vec<String>) {
    let root = |object: &Expr, out: &mut Vec<String>| {
        if let Expr::Ident(name) = object {
            if !out.iter().any(|o| o == name) {
                out.push(name.clone());
            }
        }
    };
    match expr {
        Expr::Ident(name) => {
            if let Some((alias, _)) = name.split_once('.') {
                if !out.iter().any(|o| o == alias) {
                    out.push(alias.to_string());
                }
            }
        }
        Expr::Member { expr: object, .. } | Expr::OptionalMember { expr: object, .. } => {
            root(object, out);
            aliases_read(object, out);
        }
        Expr::Index {
            expr: object,
            index,
        } => {
            root(object, out);
            aliases_read(object, out);
            aliases_read(index, out);
        }
        Expr::Slice {
            expr: object,
            start,
            end,
        } => {
            root(object, out);
            aliases_read(object, out);
            if let Some(start) = start {
                aliases_read(start, out);
            }
            if let Some(end) = end {
                aliases_read(end, out);
            }
        }
        Expr::Binary { left, right, .. } => {
            aliases_read(left, out);
            aliases_read(right, out);
        }
        Expr::Unary { expr: inner, .. } | Expr::Lambda { body: inner, .. } => {
            aliases_read(inner, out);
        }
        Expr::Call { func, args } => {
            aliases_read(func, out);
            for arg in args {
                match arg {
                    Arg::Positional(e) | Arg::Named(_, e) => aliases_read(e, out),
                }
            }
        }
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => {
            aliases_read(cond, out);
            aliases_read(then_branch, out);
            aliases_read(else_branch, out);
        }
        Expr::Coalesce {
            expr: inner,
            default,
        } => {
            aliases_read(inner, out);
            aliases_read(default, out);
        }
        Expr::Range { start, end, .. } => {
            aliases_read(start, out);
            aliases_read(end, out);
        }
        Expr::Block { stmts, result } => {
            for (_, _, e, _) in stmts {
                aliases_read(e, out);
            }
            aliases_read(result, out);
        }
        Expr::Array(items) => {
            for item in items {
                aliases_read(item, out);
            }
        }
        Expr::Map(entries) => {
            for (_, e) in entries {
                aliases_read(e, out);
            }
        }
        Expr::Null
        | Expr::Bool(_)
        | Expr::Int(_)
        | Expr::Float(_)
        | Expr::Str(_)
        | Expr::Duration(_)
        | Expr::Timestamp(_) => {}
    }
}

/// A condition holds when it evaluates to `true`; a missing field, a
/// non-boolean or no value is a condition that does not hold, as in
/// `.where()` and in a sequence step.
fn holds_true(value: Option<Value>) -> bool {
    matches!(value, Some(Value::Bool(true)))
}

/// A numeric field as a float; anything else does not take part in a sum,
/// a minimum or a comparison.
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    }
}

/// A count as the integer the event carries, saturating at `i64::MAX`.
fn int(count: u64) -> Value {
    Value::Int(i64::try_from(count).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use varpulis_core::ast::BinOp;

    use super::*;

    fn tick(ts_s: i64, symbol: &str, price: f64) -> SharedEvent {
        let mut e =
            Event::with_capacity_at("StockTick", 2, DateTime::from_timestamp(ts_s, 0).unwrap());
        e.data.insert("symbol".into(), Value::str(symbol));
        e.data.insert("price".into(), Value::Float(price));
        Arc::new(e)
    }

    fn member(alias: &str, field: &str) -> Expr {
        Expr::Member {
            expr: Box::new(Expr::Ident(alias.to_string())),
            member: field.to_string(),
        }
    }

    fn above(field: &str, alias: &str) -> Expr {
        Expr::Binary {
            op: BinOp::Gt,
            left: Box::new(Expr::Ident(field.to_string())),
            right: Box::new(member(alias, field)),
        }
    }

    fn rising(filter: Expr) -> TrendAggregateConfig {
        TrendAggregateConfig::new(
            vec![
                TrendStep {
                    event_type: "StockTick".into(),
                    alias: Some("first".into()),
                    filter: None,
                    kleene: false,
                    monotonic: None,
                },
                TrendStep {
                    event_type: "StockTick".into(),
                    alias: Some("rising".into()),
                    filter: Some(filter),
                    kleene: true,
                    monotonic: None,
                },
            ],
            vec![
                ("trends".into(), TrendAggregate::CountTrends),
                (
                    "events".into(),
                    TrendAggregate::CountEvents(Some("rising".into())),
                ),
                (
                    "low".into(),
                    TrendAggregate::Min {
                        alias: "rising".into(),
                        field: "price".into(),
                    },
                ),
                (
                    "high".into(),
                    TrendAggregate::Max {
                        alias: "rising".into(),
                        field: "price".into(),
                    },
                ),
            ],
            Duration::minutes(1),
            Some("symbol".into()),
        )
    }

    /// The tutorial's five ticks, 100 105 110 108 115, with `price >
    /// first.price`: from 100 every non-empty subset of the four later
    /// ticks, 15; from 105 the subsets of {110, 108, 115}, 7; from 110 and
    /// from 108, {115}, one each; 24 in all.
    #[test]
    fn every_start_counts_and_every_subset_is_a_trend() {
        let mut agg = rising(above("price", "first"));
        assert!(agg.counts_per_start());
        for (i, price) in [100.0, 105.0, 110.0, 108.0, 115.0].into_iter().enumerate() {
            assert!(agg.add(tick(i as i64, "AAPL", price)).is_none());
        }
        let out = agg.close_through(DateTime::from_timestamp(60, 0).unwrap());
        assert_eq!(out.len(), 1);
        let out = &out[0];
        assert_eq!(out.get("trends"), Some(&Value::Int(24)));
        // 105 is in 8 trends from 100; 110 in 8 from 100 and 4 from 105;
        // 108 the same; 115 in 8 + 4 + 1 + 1.
        assert_eq!(out.get("events"), Some(&Value::Int(8 + 12 + 12 + 14)));
        assert_eq!(out.get("low"), Some(&Value::Float(105.0)));
        assert_eq!(out.get("high"), Some(&Value::Float(115.0)));
        assert_eq!(out.get("symbol"), Some(&Value::str("AAPL")));
        assert_eq!(out.get("first.price"), Some(&Value::Float(100.0)));
        assert_eq!(out.get("_count_rising"), Some(&Value::Int(4)));
        assert_eq!(
            out.timestamp,
            DateTime::from_timestamp(60, 0).unwrap(),
            "stamped with the window's end"
        );
    }

    /// With `price > rising.price` each tick must beat the one before it in
    /// the trend, the start included: 108 cannot follow 110. From 100:
    /// chains in {105, 110, 108, 115} that rise: 105; 110; 108; 115;
    /// 105-110; 105-108; 105-115; 110-115; 108-115; 105-110-115;
    /// 105-108-115: 11. From 105: 110; 108; 115; 110-115; 108-115: 5.
    /// From 110 and 108: 115, one each. 18.
    #[test]
    fn a_self_reference_compares_with_the_previous_event_of_the_trend() {
        let mut agg = rising(above("price", "rising"));
        assert!(!agg.counts_per_start());
        for (i, price) in [100.0, 105.0, 110.0, 108.0, 115.0].into_iter().enumerate() {
            assert!(agg.add(tick(i as i64, "AAPL", price)).is_none());
        }
        let out = agg.close_through(DateTime::from_timestamp(60, 0).unwrap());
        assert_eq!(out[0].get("trends"), Some(&Value::Int(18)));
    }

    /// Partitions do not see each other, and an event at the window's end
    /// closes it and opens the next.
    #[test]
    fn partitions_and_windows_are_separate() {
        let mut agg = rising(above("price", "first"));
        assert!(agg.add(tick(0, "AAPL", 1.0)).is_none());
        assert!(agg.add(tick(1, "MSFT", 1.0)).is_none());
        assert!(agg.add(tick(2, "AAPL", 2.0)).is_none());
        assert!(agg.add(tick(3, "MSFT", 2.0)).is_none());
        let closed = agg
            .add(tick(60, "AAPL", 3.0))
            .expect("the AAPL window closed");
        assert_eq!(closed.get("symbol"), Some(&Value::str("AAPL")));
        assert_eq!(closed.get("trends"), Some(&Value::Int(1)));
        let rest = agg.close_through(DateTime::from_timestamp(61, 0).unwrap());
        assert_eq!(rest.len(), 1, "MSFT's window, whose end 61s reaches");
        assert_eq!(rest[0].get("symbol"), Some(&Value::str("MSFT")));
        assert_eq!(rest[0].get("trends"), Some(&Value::Int(1)));
        let last = agg.close_through(DateTime::<Utc>::MAX_UTC);
        assert_eq!(last.len(), 1, "the window the 60s tick opened");
        assert_eq!(last[0].get("trends"), Some(&Value::Int(0)));
    }
}
