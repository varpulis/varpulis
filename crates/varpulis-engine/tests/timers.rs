//! `timer()` sources, fired on a clock the host drives.
//!
//! A `Program` has no thread, so a timer cannot fire by itself: the program
//! said `timer()` never fired and refused it (E102). Now the host owns the
//! clock. `next_deadline` says when a timer is next due, `fire_due(now)`
//! fires what is due at `now`, and under `TimerClock::EventTime` the events
//! fed move the clock as well, so a replay fires the same timers in the same
//! places.

use chrono::{DateTime, Duration, TimeZone, Utc};
use varpulis_engine::{Emit, Program, TimerClock};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 28, 10, 0, 0).unwrap()
}

fn at(secs: i64) -> DateTime<Utc> {
    t0() + Duration::seconds(secs)
}

/// Each emit's stream and time, in seconds from `t0`.
fn when(emits: &[Emit]) -> Vec<(String, i64)> {
    emits
        .iter()
        .map(|e| (e.stream.clone(), (e.event.timestamp - t0()).num_seconds()))
        .collect()
}

fn beat(secs: i64) -> (String, i64) {
    ("Beat".to_string(), secs)
}

const BEAT: &str = "stream Beat = timer(10s)\n    .emit(kind: \"beat\")\n";

#[test]
fn a_timer_fires_every_period_on_the_hosts_clock() {
    let mut program = Program::compile(BEAT).unwrap();
    assert!(program.has_timers());
    assert_eq!(program.timer_clock(), TimerClock::Host);
    assert_eq!(program.next_deadline(), None, "not started yet");

    // The first call starts the schedule and fires nothing.
    assert!(program.fire_due(t0()).unwrap().is_empty());
    assert_eq!(program.next_deadline(), Some(at(10)));

    assert!(program.fire_due(at(9)).unwrap().is_empty());
    let emits = program.fire_due(at(10)).unwrap();
    assert_eq!(when(&emits), vec![beat(10)]);
    let json = emits[0].to_json();
    assert_eq!(json["kind"], "beat");
    assert_eq!(program.next_deadline(), Some(at(20)));

    // Called again at the same time, or before the next deadline: nothing.
    assert!(program.fire_due(at(10)).unwrap().is_empty());
    assert!(program.fire_due(at(15)).unwrap().is_empty());
    assert_eq!(when(&program.fire_due(at(20)).unwrap()), vec![beat(20)]);
    assert_eq!(program.losses().timer_periods_skipped, 0);
}

#[test]
fn an_initial_delay_comes_before_the_first_period() {
    let mut program = Program::compile(
        "stream Beat = timer(10s, initial_delay: 5s)\n    .emit(kind: \"beat\")\n",
    )
    .unwrap();
    program.fire_due(t0()).unwrap();
    assert_eq!(program.next_deadline(), Some(at(15)));
    assert_eq!(when(&program.fire_due(at(15)).unwrap()), vec![beat(15)]);
    assert_eq!(program.next_deadline(), Some(at(25)));
}

#[test]
fn a_late_call_fires_once_counts_what_it_skipped_and_does_not_drift() {
    let mut program = Program::compile(BEAT).unwrap();
    program.fire_due(t0()).unwrap();

    // Late by three and a half periods: one fire, for the latest deadline
    // reached (40 s); 10, 20 and 30 s are skipped and counted.
    let emits = program.fire_due(at(45)).unwrap();
    assert_eq!(when(&emits), vec![beat(40)]);
    assert_eq!(program.losses().timer_periods_skipped, 3);
    // Still on the grid the first call fixed: 50 s, not 55 s.
    assert_eq!(program.next_deadline(), Some(at(50)));

    // Off-grid calls keep it on the grid too.
    assert_eq!(when(&program.fire_due(at(51)).unwrap()), vec![beat(50)]);
    assert_eq!(program.next_deadline(), Some(at(60)));
    assert_eq!(program.losses().timer_periods_skipped, 3);
}

#[test]
fn a_clock_that_goes_back_fires_nothing_and_moves_no_deadline() {
    let mut program = Program::compile(BEAT).unwrap();
    program.fire_due(at(100)).unwrap();
    assert!(program.fire_due(at(50)).unwrap().is_empty());
    assert_eq!(program.next_deadline(), Some(at(110)));
    assert_eq!(when(&program.fire_due(at(110)).unwrap()), vec![beat(110)]);
}

#[test]
fn timers_due_at_the_same_instant_fire_in_time_then_name_order() {
    let mut program = Program::compile(
        "stream B = timer(10s)\n    .emit(n: 1)\n\
         stream A = timer(5s)\n    .emit(n: 2)\n",
    )
    .unwrap();
    program.fire_due(t0()).unwrap();
    assert_eq!(program.next_deadline(), Some(at(5)));
    let emits = program.fire_due(at(10)).unwrap();
    // A was due at 5 s and 10 s: it fires once, at 10 s, with B.
    assert_eq!(
        when(&emits),
        vec![("A".to_string(), 10), ("B".to_string(), 10)]
    );
    assert_eq!(program.losses().timer_periods_skipped, 1);
}

const ORDERS_AND_BEAT: &str = "event Order:\n    id: str\n\n\
     stream Beat = timer(10s)\n    .emit(kind: \"beat\")\n\
     stream Seen = Order\n    .emit(id: id)\n";

fn order(secs: i64, id: &str) -> varpulis_engine::Event {
    let json = format!(
        r#"{{"id": "{id}", "@timestamp": "{}"}}"#,
        at(secs).to_rfc3339()
    );
    varpulis_engine::event_from_json("Order", json.as_bytes()).unwrap()
}

#[test]
fn under_the_hosts_clock_events_do_not_move_the_timers() {
    let mut program = Program::compile(ORDERS_AND_BEAT).unwrap();
    program.fire_due(t0()).unwrap();
    let emits = program.feed(order(60, "a")).unwrap();
    assert_eq!(when(&emits), vec![("Seen".to_string(), 60)]);
    assert_eq!(program.next_deadline(), Some(at(10)));
}

#[test]
fn under_event_time_the_events_move_the_timers_and_a_timer_fires_first() {
    let mut program = Program::compile(ORDERS_AND_BEAT).unwrap();
    program.set_timer_clock(TimerClock::EventTime);

    // The first event starts the schedule at its own time.
    assert_eq!(
        when(&program.feed(order(0, "a")).unwrap()),
        vec![("Seen".to_string(), 0)]
    );
    assert_eq!(program.next_deadline(), Some(at(10)));
    assert_eq!(
        when(&program.feed(order(5, "b")).unwrap()),
        vec![("Seen".to_string(), 5)]
    );

    // An event at 10 s: the timer due at 10 s fires before it.
    assert_eq!(
        when(&program.feed(order(10, "c")).unwrap()),
        vec![beat(10), ("Seen".to_string(), 10)]
    );
    // An event at 35 s: one beat, at 30 s; 20 s is skipped.
    assert_eq!(
        when(&program.feed(order(35, "d")).unwrap()),
        vec![beat(30), ("Seen".to_string(), 35)]
    );
    assert_eq!(program.losses().timer_periods_skipped, 1);

    // A late event does not move the clock back.
    assert_eq!(
        when(&program.feed(order(12, "e")).unwrap()),
        vec![("Seen".to_string(), 12)]
    );
    assert_eq!(program.next_deadline(), Some(at(40)));

    // With nothing arriving, the host may still move event time on.
    assert_eq!(when(&program.fire_due(at(40)).unwrap()), vec![beat(40)]);
}

#[test]
fn under_event_time_one_batch_gives_what_the_same_events_one_by_one_give() {
    let events = || {
        vec![
            order(0, "a"),
            order(5, "b"),
            order(10, "c"),
            order(27, "d"),
            order(41, "e"),
        ]
    };

    let mut one_by_one = Program::compile(ORDERS_AND_BEAT).unwrap();
    one_by_one.set_timer_clock(TimerClock::EventTime);
    let mut expected = Vec::new();
    for event in events() {
        expected.extend(when(&one_by_one.feed(event).unwrap()));
    }

    let mut batched = Program::compile(ORDERS_AND_BEAT).unwrap();
    batched.set_timer_clock(TimerClock::EventTime);
    let got = when(&batched.feed_batch(events()).unwrap());

    assert_eq!(got, expected);
    assert_eq!(
        got,
        vec![
            ("Seen".to_string(), 0),
            ("Seen".to_string(), 5),
            beat(10),
            ("Seen".to_string(), 10),
            beat(20),
            ("Seen".to_string(), 27),
            beat(40),
            ("Seen".to_string(), 41),
        ]
    );
    assert_eq!(batched.losses(), one_by_one.losses());
    assert_eq!(batched.losses().timer_periods_skipped, 1, "30 s");
}

#[test]
fn a_restore_mid_period_resumes_the_schedule_where_it_was() {
    let mut before = Program::compile(BEAT).unwrap();
    before.fire_due(t0()).unwrap();
    assert_eq!(when(&before.fire_due(at(15)).unwrap()), vec![beat(10)]);
    let snapshot = before.snapshot().unwrap();

    let mut after = Program::compile(BEAT).unwrap();
    after.restore(&snapshot).unwrap();
    assert_eq!(after.next_deadline(), Some(at(20)), "mid-period, as it was");
    // The clock came back too: a time before it does nothing.
    assert!(after.fire_due(at(12)).unwrap().is_empty());
    assert!(after.fire_due(at(19)).unwrap().is_empty());
    assert_eq!(when(&after.fire_due(at(20)).unwrap()), vec![beat(20)]);
}

#[test]
fn a_snapshot_taken_before_the_timers_started_leaves_them_to_start() {
    let snapshot = Program::compile(BEAT).unwrap().snapshot().unwrap();

    let mut program = Program::compile(BEAT).unwrap();
    program.fire_due(t0()).unwrap();
    program.restore(&snapshot).unwrap();
    assert_eq!(program.next_deadline(), None);
    program.fire_due(at(100)).unwrap();
    assert_eq!(program.next_deadline(), Some(at(110)));
}

#[test]
fn skipped_periods_count_since_the_program_was_built_across_a_restore() {
    let mut program = Program::compile(BEAT).unwrap();
    program.fire_due(t0()).unwrap();
    let snapshot = program.snapshot().unwrap();
    program.fire_due(at(35)).unwrap();
    assert_eq!(program.losses().timer_periods_skipped, 2);

    program.restore(&snapshot).unwrap();
    assert_eq!(
        program.losses().timer_periods_skipped,
        2,
        "neither reset nor imported"
    );
    assert_eq!(program.next_deadline(), Some(at(10)));
    program.fire_due(at(25)).unwrap();
    assert_eq!(program.losses().timer_periods_skipped, 3);
}

#[test]
fn a_timer_feeds_a_pattern() {
    // Each order is reported on the first beat after it.
    let mut program = Program::compile(
        "event Order:\n    id: str\n\n\
         stream Beat = timer(1m)\n    .emit(kind: \"beat\")\n\
         stream Reported = Order as o\n    -> Beat as b\n    .within(5m)\n    .emit(id: o.id, beat: b.kind)\n",
    )
    .unwrap();
    program.set_timer_clock(TimerClock::EventTime);

    assert!(program.feed(order(0, "a")).unwrap().is_empty());
    assert!(program.feed(order(20, "b")).unwrap().is_empty());

    // The host moves event time to the deadline: the beat fires, and the
    // pattern takes it as its second step.
    let emits = program.fire_due(at(60)).unwrap();
    let mut reported: Vec<(String, String)> = emits
        .iter()
        .filter(|e| e.stream == "Reported")
        .map(|e| {
            let json = e.to_json();
            (
                json["id"].as_str().unwrap_or_default().to_string(),
                json["beat"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    reported.sort();
    assert_eq!(
        reported,
        vec![
            ("a".to_string(), "beat".to_string()),
            ("b".to_string(), "beat".to_string())
        ],
        "{:?}",
        when(&emits)
    );

    // An order after that beat waits for the next one, fired by an event.
    assert!(program.feed(order(70, "c")).unwrap().is_empty());
    let emits = program.feed(order(120, "d")).unwrap();
    let ids: Vec<String> = emits
        .iter()
        .filter(|e| e.stream == "Reported")
        .map(|e| e.to_json()["id"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(ids, vec!["c".to_string()], "{:?}", when(&emits));
}
