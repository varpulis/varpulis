//! `and` (both, any order) and `or` (either) in the engine: what matches,
//! under each selection strategy, with WITHIN in processing and event time,
//! out-of-order input, an absence after an `and`, partitions, the run cap,
//! and the predicate index.

use std::time::Duration;

use chrono::{DateTime, Utc};
use varpulis_core::{Event, Value};
use varpulis_sase::{
    CompareOp, EmissionMode, MatchResult, Predicate, SaseEngine, SasePattern, SelectionStrategy,
};

fn ev(event_type: &str, id: i64) -> Event {
    Event::new(event_type).with_field("id", Value::Int(id))
}

fn keyed(event_type: &str, id: i64, key: i64) -> Event {
    ev(event_type, id).with_field("key", Value::Int(key))
}

fn at(event: Event, secs: i64) -> Event {
    let mut event = event;
    event.timestamp = DateTime::<Utc>::from_timestamp(1_700_000_000 + secs, 0).expect("timestamp");
    event
}

fn step(event_type: &str, alias: &str) -> SasePattern {
    SasePattern::Event {
        event_type: event_type.into(),
        predicate: None,
        alias: Some(alias.into()),
    }
}

fn step_where(event_type: &str, alias: &str, predicate: Predicate) -> SasePattern {
    SasePattern::Event {
        event_type: event_type.into(),
        predicate: Some(predicate),
        alias: Some(alias.into()),
    }
}

fn same_key(alias: &str) -> Predicate {
    Predicate::CompareRef {
        field: "key".into(),
        op: CompareOp::Eq,
        ref_alias: alias.into(),
        ref_field: "key".into(),
    }
}

fn gt(field: &str, value: i64) -> Predicate {
    Predicate::Compare {
        field: field.into(),
        op: CompareOp::Gt,
        value: Value::Int(value),
    }
}

fn and(l: SasePattern, r: SasePattern) -> SasePattern {
    SasePattern::And(Box::new(l), Box::new(r))
}

fn or(l: SasePattern, r: SasePattern) -> SasePattern {
    SasePattern::Or(Box::new(l), Box::new(r))
}

fn within(p: SasePattern, secs: u64) -> SasePattern {
    SasePattern::Within(Box::new(p), Duration::from_secs(secs))
}

/// A match as `alias=id` pairs, sorted.
fn describe(m: &MatchResult) -> String {
    let mut parts: Vec<String> = m
        .captured
        .iter()
        .map(|(alias, e)| match e.get("id") {
            Some(Value::Int(id)) => format!("{alias}={id}"),
            other => format!("{alias}={other:?}"),
        })
        .collect();
    parts.sort();
    parts.join(",")
}

/// Feed `events`, collecting every match in order.
fn run(engine: &mut SaseEngine, events: &[Event]) -> Vec<String> {
    events
        .iter()
        .flat_map(|e| engine.process(e))
        .map(|m| describe(&m))
        .collect()
}

fn engine(pattern: SasePattern) -> SaseEngine {
    SaseEngine::new(pattern).with_emission_mode(EmissionMode::Each)
}

// ---------------------------------------------------------------------------
// and
// ---------------------------------------------------------------------------

#[test]
fn and_matches_in_either_order() {
    let p = || and(step("A", "a"), step("B", "b"));
    assert_eq!(
        run(&mut engine(p()), &[ev("A", 1), ev("B", 2)]),
        ["a=1,b=2"]
    );
    assert_eq!(
        run(&mut engine(p()), &[ev("B", 1), ev("A", 2)]),
        ["a=2,b=1"]
    );
}

#[test]
fn and_needs_every_operand() {
    let p = and(step("A", "a"), and(step("B", "b"), step("C", "c")));
    let mut e = engine(p);
    assert!(run(&mut e, &[ev("C", 1), ev("A", 2), ev("A", 3)]).is_empty());
    // The run C 1 opened took A 2; the runs the As opened still need a C.
    assert_eq!(run(&mut e, &[ev("B", 4)]), ["a=2,b=4,c=1"]);
}

#[test]
fn and_under_any_match_pairs_each_event_with_the_first_partner_after_it() {
    // Every A and every B opens a run (the default selection); each run
    // completes with the first partner that comes after it.
    let p = and(step("A", "a"), step("B", "b"));
    let got = run(
        &mut engine(p),
        &[ev("A", 1), ev("A", 2), ev("B", 3), ev("A", 4)],
    );
    assert_eq!(got, ["a=1,b=3", "a=2,b=3", "a=4,b=3"]);
}

#[test]
fn and_under_strict_contiguity_breaks_on_a_repeated_operand() {
    let p = and(step("A", "a"), step("B", "b"));
    let mut e = engine(p).with_strategy(SelectionStrategy::StrictContiguous);
    // The second A cannot take a step of the run the first opened: that
    // run is over. It opens its own, which B completes.
    assert_eq!(
        run(&mut e, &[ev("A", 1), ev("A", 2), ev("B", 3)]),
        ["a=2,b=3"]
    );
}

#[test]
fn and_under_next_match_opens_no_run_on_an_event_a_run_took() {
    let p = and(step("A", "a"), step("B", "b"));
    let mut e = engine(p).with_strategy(SelectionStrategy::SkipTillNextMatch);
    // B completes the A's run, so opens none; the next A opens one.
    assert_eq!(
        run(&mut e, &[ev("A", 1), ev("B", 2), ev("A", 3), ev("B", 4)]),
        ["a=1,b=2", "a=3,b=4"]
    );
}

#[test]
fn an_operand_that_reads_its_sibling_waits_for_it() {
    // `A as a and B where key == a.key as b`, B first.
    let p = and(step("A", "a"), step_where("B", "b", same_key("a")));
    let got = run(
        &mut engine(p),
        &[keyed("B", 1, 7), keyed("A", 2, 8), keyed("A", 3, 7)],
    );
    // The A with key 8 is no partner for B 1; the run B 1 opened passes it
    // over and takes the A with key 7. The run A 2 opened is still waiting.
    assert_eq!(got, ["a=3,b=1"]);
}

#[test]
fn a_waiting_operand_is_judged_when_its_sibling_comes_inside_a_sequence() {
    let p = SasePattern::Seq(vec![
        step("X", "x"),
        and(step("A", "a"), step_where("B", "b", same_key("a"))),
    ]);
    let got = run(
        &mut engine(p),
        &[
            ev("X", 1),
            keyed("B", 2, 5),
            keyed("B", 3, 6),
            keyed("A", 4, 6),
        ],
    );
    // B 2 does not match A 4's key, B 3 does.
    assert_eq!(got, ["a=4,b=3,x=1"]);
}

#[test]
fn and_inside_a_sequence_does_not_move_on_an_event_it_does_not_take() {
    // X -> (A and B) -> C: a C before the AND completes changes nothing.
    let p = SasePattern::Seq(vec![
        step("X", "x"),
        and(step("A", "a"), step("B", "b")),
        step("C", "c"),
    ]);
    let got = run(
        &mut engine(p),
        &[
            ev("X", 1),
            ev("C", 2),
            ev("B", 3),
            ev("C", 4),
            ev("A", 5),
            ev("C", 6),
        ],
    );
    assert_eq!(got, ["a=5,b=3,c=6,x=1"]);
}

#[test]
fn and_respects_within_in_processing_time() {
    let p = within(and(step("A", "a"), step("B", "b")), 3600);
    assert_eq!(run(&mut engine(p), &[ev("B", 1), ev("A", 2)]), ["a=2,b=1"]);
}

#[test]
fn and_in_event_time_counts_the_window_across_both_orders() {
    // within 10s; B stamped 20 arrives first.
    let p = || within(and(step("A", "a"), step("B", "b")), 10);
    let mut e = engine(p()).with_event_time();
    // A stamped 12 arrives later but happened earlier: 8s apart, a match.
    assert_eq!(
        run(&mut e, &[at(ev("B", 1), 20), at(ev("A", 2), 12)]),
        ["a=2,b=1"]
    );

    let mut e = engine(p()).with_event_time();
    // A stamped 5: 15s apart, no match.
    assert!(run(&mut e, &[at(ev("B", 1), 20), at(ev("A", 2), 5)]).is_empty());
}

#[test]
fn an_earlier_stamped_operand_moves_the_window() {
    // (A and B and C) within 10s: B@20, A@12 starts the window at 12, so
    // C@25 is out of it (13s after A) while C@21 is in.
    let p = || within(and(step("A", "a"), and(step("B", "b"), step("C", "c"))), 10);
    let mut e = engine(p()).with_event_time();
    assert!(run(
        &mut e,
        &[at(ev("B", 1), 20), at(ev("A", 2), 12), at(ev("C", 3), 25)]
    )
    .is_empty());

    let mut e = engine(p()).with_event_time();
    assert_eq!(
        run(
            &mut e,
            &[at(ev("B", 1), 20), at(ev("A", 2), 12), at(ev("C", 3), 21)]
        ),
        ["a=2,b=1,c=3"]
    );
}

#[test]
fn an_absence_after_an_and_starts_when_the_and_completes() {
    // (A and B) -> NOT C within 10s, event time.
    let p = || {
        within(
            SasePattern::Seq(vec![
                and(step("A", "a"), step("B", "b")),
                SasePattern::Not(Box::new(step("C", "c"))),
            ]),
            10,
        )
    };
    let mut e = engine(p()).with_event_time();
    let got = run(
        &mut e,
        &[at(ev("B", 1), 0), at(ev("A", 2), 3), at(ev("Z", 3), 30)],
    );
    assert_eq!(got, ["a=2,b=1"]);

    let mut e = engine(p()).with_event_time();
    let got = run(
        &mut e,
        &[
            at(ev("B", 1), 0),
            at(ev("A", 2), 3),
            at(ev("C", 3), 5),
            at(ev("Z", 4), 30),
        ],
    );
    assert!(got.is_empty(), "C came within the window: {got:?}");
}

#[test]
fn an_absence_after_an_and_in_a_partition_fires_on_the_moved_window() {
    // B@20 then A@12 move the run's window to [12, 22]; the absence must be
    // confirmed once the watermark passes 22, not 30.
    let p = within(
        SasePattern::Seq(vec![
            and(step("A", "a"), step("B", "b")),
            SasePattern::Not(Box::new(step("C", "c"))),
        ]),
        10,
    );
    // Under `.stnm()` the A opens no run of its own, whose deadline would
    // have had the partition visited at 22 anyway.
    let mut e = engine(p)
        .with_event_time()
        .with_partition_by("key".into())
        .with_strategy(SelectionStrategy::SkipTillNextMatch);
    let got = run(
        &mut e,
        &[
            at(keyed("B", 1, 1), 20),
            at(keyed("A", 2, 1), 12),
            at(keyed("Z", 3, 2), 25),
        ],
    );
    assert_eq!(got, ["a=2,b=1"]);
}

#[test]
fn and_runs_per_partition() {
    let p = and(step("A", "a"), step("B", "b"));
    let mut e = engine(p).with_partition_by("key".into());
    let got = run(
        &mut e,
        &[
            keyed("A", 1, 1),
            keyed("B", 2, 2),
            keyed("B", 3, 1),
            keyed("A", 4, 2),
        ],
    );
    assert_eq!(got, ["a=1,b=3", "a=4,b=2"]);
}

#[test]
fn runs_dropped_at_the_cap_are_counted_inside_an_and() {
    let p = and(step("A", "a"), step("B", "b"));
    let mut e = engine(p).with_max_runs(1);
    run(&mut e, &[ev("A", 1), ev("A", 2), ev("A", 3)]);
    assert_eq!(e.total_runs_dropped, 2);
}

// ---------------------------------------------------------------------------
// or
// ---------------------------------------------------------------------------

#[test]
fn or_takes_either_alternative() {
    let p = || {
        SasePattern::Seq(vec![
            step("X", "x"),
            or(step("A", "a"), step("B", "b")),
            step("C", "c"),
        ])
    };
    assert_eq!(
        run(&mut engine(p()), &[ev("X", 1), ev("A", 2), ev("C", 3)]),
        ["a=2,c=3,x=1"]
    );
    assert_eq!(
        run(&mut engine(p()), &[ev("X", 1), ev("B", 2), ev("C", 3)]),
        ["b=2,c=3,x=1"]
    );
    assert!(run(&mut engine(p()), &[ev("X", 1), ev("C", 3)]).is_empty());
}

#[test]
fn or_at_the_end_completes_on_the_alternative() {
    let p = SasePattern::Seq(vec![step("X", "x"), or(step("A", "a"), step("B", "b"))]);
    assert_eq!(run(&mut engine(p), &[ev("X", 1), ev("B", 2)]), ["b=2,x=1"]);
}

#[test]
fn or_at_the_start_opens_on_either() {
    let p = || SasePattern::Seq(vec![or(step("A", "a"), step("B", "b")), step("C", "c")]);
    assert_eq!(
        run(&mut engine(p()), &[ev("A", 1), ev("B", 2), ev("C", 3)]),
        ["a=1,c=3", "b=2,c=3"]
    );
}

#[test]
fn or_alternatives_can_be_sequences() {
    // X -> (A -> B or C) -> D
    let p = || {
        SasePattern::Seq(vec![
            step("X", "x"),
            or(
                SasePattern::Seq(vec![step("A", "a"), step("B", "b")]),
                step("C", "c"),
            ),
            step("D", "d"),
        ])
    };
    assert_eq!(
        run(
            &mut engine(p()),
            &[ev("X", 1), ev("A", 2), ev("B", 3), ev("D", 4)]
        ),
        ["a=2,b=3,d=4,x=1"]
    );
    assert_eq!(
        run(&mut engine(p()), &[ev("X", 1), ev("C", 2), ev("D", 3)]),
        ["c=2,d=3,x=1"]
    );
    assert!(run(&mut engine(p()), &[ev("X", 1), ev("A", 2), ev("D", 3)]).is_empty());
}

#[test]
fn an_event_takes_the_leftmost_alternative_it_satisfies() {
    let p = SasePattern::Seq(vec![
        step("X", "x"),
        or(
            step_where("A", "low", gt("v", 0)),
            step_where("A", "high", gt("v", 5)),
        ),
        step("D", "d"),
    ]);
    let got = run(
        &mut engine(p),
        &[
            ev("X", 1),
            ev("A", 2).with_field("v", Value::Int(7)),
            ev("D", 3),
        ],
    );
    assert_eq!(got, ["d=3,low=2,x=1"]);
}

#[test]
fn or_of_ands() {
    // (A and B) or C, then D
    let p = || {
        SasePattern::Seq(vec![
            or(and(step("A", "a"), step("B", "b")), step("C", "c")),
            step("D", "d"),
        ])
    };
    assert_eq!(
        run(&mut engine(p()), &[ev("B", 1), ev("A", 2), ev("D", 3)]),
        ["a=2,b=1,d=3"]
    );
    assert_eq!(
        run(&mut engine(p()), &[ev("C", 1), ev("D", 2)]),
        ["c=1,d=2"]
    );
}

#[test]
fn within_holds_for_an_or_at_the_start_whichever_alternative_opens() {
    let p = within(
        SasePattern::Seq(vec![or(step("A", "a"), step("B", "b")), step("C", "c")]),
        10,
    );
    let mut e = engine(p).with_event_time();
    assert!(run(&mut e, &[at(ev("B", 1), 0), at(ev("C", 2), 30)]).is_empty());
}

#[test]
fn an_or_of_single_events_matches_on_each() {
    let p = or(step("A", "a"), step("B", "b"));
    let mut e = engine(p);
    assert_eq!(run(&mut e, &[ev("A", 1)]), ["a=1"]);
    assert_eq!(run(&mut e, &[ev("C", 2), ev("B", 3)]), ["b=3"]);
}
