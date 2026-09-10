//! A sliding window must evict by timestamp, not by position in arrival order.
//!
//! `SlidingWindow` stores events in a `VecDeque` in the order they arrive and
//! used to evict by draining the prefix up to the first in-window event. That
//! is only correct if the deque is sorted by timestamp. One out-of-order
//! arrival puts an old event *behind* a newer one, the prefix scan stops at
//! the newer one, and the late event is never evicted again — the window keeps
//! it forever, over-counting every aggregate computed over it and growing
//! without bound.
//!
//! Both of the documented headline examples use a window/slide ratio that
//! selects this window type: `.window(5m, sliding: 1m)` in
//! `docs/language/syntax.md` and `docs/reference/windows-aggregations.md`.

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use varpulis_core::{Event, Value};
use varpulis_runtime::window::SlidingWindow;

fn at(secs: i64, n: i64) -> Arc<Event> {
    let mut e = Event::new("Tick");
    e.timestamp = Utc.timestamp_opt(secs, 0).unwrap();
    e.data.insert("n".into(), Value::Int(n));
    Arc::new(e)
}

/// Fail-before: the 60 s event is still in a 5-minute window at t=780 s,
/// twelve minutes after it happened, so the window reports 4 events summing
/// to 1014 instead of 3 summing to 14.
#[test]
fn one_late_arrival_does_not_poison_eviction() {
    // 5-minute window, 1-minute slide — the ratio in the shipped docs.
    let mut w = SlidingWindow::new(Duration::minutes(5), Duration::minutes(1));

    w.add_shared(at(0, 1)); //   t=0    — falls out well before the end
    w.add_shared(at(600, 1000)); // t=600 — in window at t=780
    w.add_shared(at(60, 999)); //  t=60   — LATE, arrives after t=600
    w.add_shared(at(660, 10)); //  t=660 — in window
    let emitted = w
        .add_shared(at(780, 3)) //  t=780 — in window, triggers the slide
        .expect("the slide interval has elapsed, the window must emit");

    let n: i64 = emitted
        .iter()
        .map(|e| match e.data.get("n") {
            Some(Value::Int(v)) => *v,
            other => panic!("unexpected payload: {other:?}"),
        })
        .sum();

    assert_eq!(
        emitted.len(),
        3,
        "a 5-minute window at t=780s holds t=600, 660, 780 — the t=60 event is \
         12 minutes old and must have been evicted despite arriving late"
    );
    assert_eq!(n, 1013, "sum over the three in-window events");
}

/// The same property on the watermark-driven path.
#[test]
fn late_arrival_is_evicted_on_watermark_advance() {
    let mut w = SlidingWindow::new(Duration::minutes(5), Duration::minutes(1));

    w.add_shared(at(600, 1000));
    w.add_shared(at(60, 999)); // late, sits behind t=600

    let emitted = w
        .advance_watermark(Utc.timestamp_opt(780, 0).unwrap())
        .expect("window must emit on watermark advance");

    assert_eq!(
        emitted.len(),
        1,
        "only t=600 is within 5 minutes of the t=780 watermark"
    );
}
