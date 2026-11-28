//! Runs that an event passes by: an event no next step of a waiting run
//! takes leaves the run as it was, except under strict contiguity, where it
//! ends it.

use varpulis_core::{Event, Value};
use varpulis_sase::{EmissionMode, SaseEngine, SasePattern, SelectionStrategy};

fn ev(event_type: &str, id: i64) -> Event {
    Event::new(event_type).with_field("id", Value::Int(id))
}

fn step(event_type: &str, alias: &str) -> SasePattern {
    SasePattern::Event {
        event_type: event_type.into(),
        predicate: None,
        alias: Some(alias.into()),
    }
}

fn abc() -> SasePattern {
    SasePattern::Seq(vec![step("A", "a"), step("B", "b"), step("C", "c")])
}

fn matches(engine: &mut SaseEngine, events: &[Event]) -> usize {
    events.iter().map(|e| engine.process(e).len()).sum()
}

#[test]
fn an_event_no_step_takes_leaves_the_runs_waiting() {
    let mut engine = SaseEngine::new(abc()).with_emission_mode(EmissionMode::Each);
    let events = [
        ev("A", 1),
        ev("Z", 2),
        ev("A", 3),
        ev("Z", 4),
        ev("B", 5),
        ev("Z", 6),
        ev("C", 7),
    ];
    // Both runs, opened by A1 and A3, took B5 and C7 past every Z.
    assert_eq!(matches(&mut engine, &events), 2);
}

#[test]
fn under_strict_contiguity_an_event_no_step_takes_ends_the_run() {
    let mut engine = SaseEngine::new(abc())
        .with_strategy(SelectionStrategy::StrictContiguous)
        .with_emission_mode(EmissionMode::Each);
    let broken = [ev("A", 1), ev("Z", 2), ev("B", 3), ev("C", 4)];
    assert_eq!(matches(&mut engine, &broken), 0);
    let unbroken = [ev("A", 5), ev("B", 6), ev("C", 7)];
    assert_eq!(matches(&mut engine, &unbroken), 1);
}

#[test]
fn runs_waiting_in_a_partition_are_passed_by_too() {
    let keyed = |t: &str, id: i64, key: i64| ev(t, id).with_field("key", Value::Int(key));
    let mut engine = SaseEngine::new(abc())
        .with_partition_by("key".into())
        .with_emission_mode(EmissionMode::Each);
    let events = [
        keyed("A", 1, 1),
        keyed("Z", 2, 1),
        keyed("A", 3, 2),
        keyed("B", 4, 1),
        keyed("Z", 5, 1),
        keyed("C", 6, 1),
        keyed("C", 7, 2),
    ];
    // Partition 1 completes; partition 2 never sees a B.
    assert_eq!(matches(&mut engine, &events), 1);
}
