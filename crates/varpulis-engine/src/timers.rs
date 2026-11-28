//! The `timer()` sources of a [`crate::Program`], fired on a clock the host
//! drives.
//!
//! A timer has no thread and reads no clock of its own. It has a schedule,
//! fixed when the timer clock first reads a time: the first fire one
//! `initial_delay` plus one period after that instant, then one every period.
//! Whatever advances the clock (the host's [`crate::Program::fire_due`], and
//! under [`TimerClock::EventTime`] the events it feeds) fires every timer
//! whose next deadline the clock has reached.
//!
//! A clock that jumps over several deadlines of one timer fires it once, for
//! the latest of them, and counts the others in
//! [`crate::LossCounters::timer_periods_skipped`]. The schedule does not
//! drift: the next deadline stays on the grid the first instant fixed, however
//! late the clock arrived.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use varpulis_core::{Event, Value};
use varpulis_runtime::persistence::TimerCheckpoint;

/// What moves a [`crate::Program`]'s timers, named at load with
/// [`crate::CompileOptions::timer_clock`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimerClock {
    /// Only [`crate::Program::fire_due`]: the host owns the clock, whether
    /// it is the wall clock or a simulated one.
    #[default]
    Host,
    /// The program's event time as well: every event fed moves the clock to
    /// its timestamp when later, and a timer due at or before that timestamp
    /// fires before the event is processed. A replay of the same events fires
    /// the same timers at the same places. [`crate::Program::fire_due`] still
    /// moves the clock, for a host that knows event time has advanced while
    /// nothing arrived.
    EventTime,
}

#[derive(Debug)]
struct Timer {
    /// `Timer_<stream>`: the event type the engine routes to the stream.
    event_type: String,
    interval_ns: i64,
    initial_delay_ns: i64,
    /// `None` until the clock first reads a time.
    next_ns: Option<i64>,
}

#[derive(Debug, Default)]
pub(crate) struct Timers {
    list: Vec<Timer>,
    clock_ns: Option<i64>,
    mode: TimerClock,
    pub(crate) periods_skipped: u64,
}

fn ns_of(t: DateTime<Utc>) -> Option<i64> {
    t.timestamp_nanos_opt()
}

impl Timers {
    /// From the engine's `(interval_ns, initial_delay_ns, event_type)`, in
    /// event type order so that timers due at the same instant fire in an
    /// order that does not depend on a hash map.
    pub(crate) fn new(configs: Vec<(u64, Option<u64>, String)>, mode: TimerClock) -> Self {
        let to_i64 = |ns: u64| i64::try_from(ns).unwrap_or(i64::MAX);
        let mut timers: Vec<Timer> = configs
            .into_iter()
            .map(|(interval, delay, event_type)| Timer {
                event_type,
                interval_ns: to_i64(interval).max(1),
                initial_delay_ns: to_i64(delay.unwrap_or(0)),
                next_ns: None,
            })
            .collect();
        timers.sort_by(|a, b| a.event_type.cmp(&b.event_type));
        Self {
            list: timers,
            mode,
            ..Self::default()
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub(crate) fn follows_events(&self) -> bool {
        !self.list.is_empty() && self.mode == TimerClock::EventTime
    }

    pub(crate) fn next_deadline(&self) -> Option<DateTime<Utc>> {
        self.list
            .iter()
            .filter_map(|t| t.next_ns)
            .min()
            .map(DateTime::from_timestamp_nanos)
    }

    /// Move the clock to `now` when later, and return the timer events that
    /// fire, in time order. The first time the clock reads a time, the
    /// timers' schedules start there and nothing fires. A time outside the
    /// `i64` nanosecond range moves nothing.
    pub(crate) fn advance(&mut self, now: DateTime<Utc>) -> Vec<Event> {
        let Some(now) = ns_of(now) else {
            return Vec::new();
        };
        if self.list.is_empty() || self.clock_ns.is_some_and(|clock| now <= clock) {
            return Vec::new();
        }
        if self.clock_ns.is_none() {
            for t in &mut self.list {
                t.next_ns = Some(
                    now.saturating_add(t.initial_delay_ns)
                        .saturating_add(t.interval_ns),
                );
            }
        }
        self.clock_ns = Some(now);
        let mut fired: Vec<(i64, Event)> = Vec::new();
        for t in &mut self.list {
            let Some(next) = t.next_ns else { continue };
            if next > now {
                continue;
            }
            // Fire once, for the latest deadline the clock has reached; the
            // earlier ones are skipped and counted.
            // In i128: the clock may jump across more than an i64 of
            // nanoseconds (292 years). `at` is between `next` and `now`, so
            // it fits back in an i64.
            let interval = i128::from(t.interval_ns);
            let missed = (i128::from(now) - i128::from(next)) / interval;
            let at = i64::try_from(i128::from(next) + missed * interval).unwrap_or(now);
            self.periods_skipped = self
                .periods_skipped
                .saturating_add(u64::try_from(missed).unwrap_or(u64::MAX));
            t.next_ns = Some(at.saturating_add(t.interval_ns));
            fired.push((at, timer_event(&t.event_type, at)));
        }
        // Stable: same instant, event type order.
        fired.sort_by_key(|(at, _)| *at);
        fired.into_iter().map(|(_, e)| e).collect()
    }

    pub(crate) fn checkpoint(&self) -> Option<TimerCheckpoint> {
        let clock_ns = self.clock_ns?;
        Some(TimerCheckpoint {
            clock_ns,
            next_fire_ns: self
                .list
                .iter()
                .filter_map(|t| t.next_ns.map(|n| (t.event_type.clone(), n)))
                .collect::<HashMap<_, _>>(),
        })
    }

    /// Put back a checkpoint's schedules. A timer the checkpoint does not
    /// name starts its schedule at the checkpoint's clock.
    pub(crate) fn restore(&mut self, cp: Option<&TimerCheckpoint>) {
        let Some(cp) = cp else {
            self.clock_ns = None;
            for t in &mut self.list {
                t.next_ns = None;
            }
            return;
        };
        self.clock_ns = Some(cp.clock_ns);
        for t in &mut self.list {
            t.next_ns = Some(
                cp.next_fire_ns
                    .get(&t.event_type)
                    .copied()
                    .unwrap_or_else(|| {
                        cp.clock_ns
                            .saturating_add(t.initial_delay_ns)
                            .saturating_add(t.interval_ns)
                    }),
            );
        }
    }
}

/// The event a timer fires: stamped at its deadline, with that deadline in
/// epoch milliseconds as its `timestamp` field, as the async runtime's timers
/// carry it.
fn timer_event(event_type: &str, at_ns: i64) -> Event {
    let at = DateTime::from_timestamp_nanos(at_ns);
    let mut event = Event::new(event_type.to_string());
    event.timestamp = at;
    event
        .data
        .insert("timestamp".into(), Value::Int(at.timestamp_millis()));
    event
}
