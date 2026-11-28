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
fn an_event_completing_two_alternatives_takes_the_leftmost() {
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

// ---------------------------------------------------------------------------
// after a closure
// ---------------------------------------------------------------------------

fn closure(event_type: &str, alias: &str) -> SasePattern {
    SasePattern::KleenePlus(Box::new(step(event_type, alias)))
}

/// The events of a match, as `type id` in stack order.
fn stack(m: &MatchResult) -> Vec<String> {
    m.stack
        .iter()
        .map(|e| match e.event.get("id") {
            Some(Value::Int(id)) => format!("{} {id}", e.event.event_type),
            _ => e.event.event_type.to_string(),
        })
        .collect()
}

fn closure_then_and() -> SasePattern {
    // A as a -> all B as bs -> (C as c and D as d) within 60s
    within(
        SasePattern::Seq(vec![
            step("A", "a"),
            closure("B", "bs"),
            and(step("C", "c"), step("D", "d")),
        ]),
        60,
    )
}

#[test]
fn an_and_after_a_closure_completes() {
    let mut e = SaseEngine::new(closure_then_and());
    let got: Vec<MatchResult> = [ev("A", 1), ev("B", 2), ev("C", 3), ev("D", 4)]
        .iter()
        .flat_map(|x| e.process(x))
        .collect();
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(stack(&got[0]), ["A 1", "B 2", "C 3", "D 4"]);
}

#[test]
fn an_and_after_a_closure_takes_only_its_own_events() {
    // The second A is no operand of the `and`; it used to land on the stack.
    let mut e = SaseEngine::new(closure_then_and());
    let got: Vec<MatchResult> = [ev("A", 1), ev("B", 2), ev("A", 3), ev("C", 4), ev("D", 5)]
        .iter()
        .flat_map(|x| e.process(x))
        .collect();
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(stack(&got[0]), ["A 1", "B 2", "C 4", "D 5"]);
}

#[test]
fn an_and_after_a_closure_keeps_its_first_partner() {
    let mut e = SaseEngine::new(closure_then_and());
    let got: Vec<String> = [ev("A", 1), ev("B", 2), ev("C", 3), ev("D", 4), ev("C", 5)]
        .iter()
        .flat_map(|x| e.process(x))
        .map(|m| format!("c={:?}", m.captured["c"].get("id")))
        .collect();
    assert_eq!(got, ["c=Some(Int(3))"]);
}

#[test]
fn an_absence_after_a_closure_completes_when_its_window_ends() {
    // A -> all B -> NOT C within 10s, event time.
    let p = || {
        within(
            SasePattern::Seq(vec![
                step("A", "a"),
                closure("B", "bs"),
                SasePattern::Not(Box::new(step("C", "c"))),
            ]),
            10,
        )
    };
    let mut e = SaseEngine::new(p()).with_event_time();
    let got: Vec<MatchResult> = [
        at(ev("A", 1), 0),
        at(ev("B", 2), 1),
        at(ev("Z", 3), 2),
        at(ev("B", 4), 3),
        at(ev("Z", 5), 30),
    ]
    .iter()
    .flat_map(|x| e.process(x))
    .collect();
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(stack(&got[0]), ["A 1", "B 2", "B 4"]);

    let mut e = SaseEngine::new(p()).with_event_time();
    let got: Vec<MatchResult> = [
        at(ev("A", 1), 0),
        at(ev("B", 2), 1),
        at(ev("C", 3), 2),
        at(ev("Z", 4), 30),
    ]
    .iter()
    .flat_map(|x| e.process(x))
    .collect();
    assert!(got.is_empty(), "C came within the window: {got:?}");
}

// ---------------------------------------------------------------------------
// an `or` inside an `and`
// ---------------------------------------------------------------------------

#[test]
fn an_or_operand_takes_any_of_its_events_in_any_order() {
    // (A as e or B as e) and C as c
    let p = || and(or(step("A", "e"), step("B", "e")), step("C", "c"));
    assert_eq!(
        run(&mut engine(p()), &[ev("C", 1), ev("B", 2)]),
        ["c=1,e=2"]
    );
    assert_eq!(
        run(&mut engine(p()), &[ev("C", 1), ev("A", 2)]),
        ["c=1,e=2"]
    );
    assert_eq!(
        run(&mut engine(p()), &[ev("B", 1), ev("C", 2)]),
        ["c=2,e=1"]
    );
    assert!(run(&mut engine(p()), &[ev("A", 1), ev("B", 2)]).is_empty());
}

#[test]
fn each_option_of_an_or_operand_keeps_its_where_and_alias() {
    // X as x -> ((A where v > 5 as hi or B as lo) and C where key == hi.key as c)
    let p = || {
        SasePattern::Seq(vec![
            step("X", "x"),
            and(
                or(step_where("A", "hi", gt("v", 5)), step("B", "lo")),
                step_where("C", "c", same_key("hi")),
            ),
        ])
    };
    let a = |id, v| keyed("A", id, 7).with_field("v", Value::Int(v));
    // The C waits for `hi`; a low A is no option.
    assert_eq!(
        run(
            &mut engine(p()),
            &[ev("X", 1), keyed("C", 2, 7), a(3, 1), a(4, 9)]
        ),
        ["c=2,hi=4,x=1"]
    );
}

// ---------------------------------------------------------------------------
// every alternative of an `or` is followed
// ---------------------------------------------------------------------------

fn seq(steps: Vec<SasePattern>) -> SasePattern {
    SasePattern::Seq(steps)
}

fn sequence_or_single() -> SasePattern {
    // A -> ((B -> C) or D) -> E
    seq(vec![
        step("A", "a"),
        or(seq(vec![step("B", "b"), step("C", "c")]), step("D", "d")),
        step("E", "e"),
    ])
}

#[test]
fn a_started_alternative_leaves_the_others_open() {
    let got = run(
        &mut engine(sequence_or_single()),
        &[ev("A", 1), ev("B", 2), ev("D", 3), ev("E", 4)],
    );
    assert_eq!(got, ["a=1,d=3,e=4"]);
}

#[test]
fn the_first_alternative_to_complete_ends_the_others() {
    // Through D, then the C that would complete the other is too late.
    let got = run(
        &mut engine(sequence_or_single()),
        &[ev("A", 1), ev("B", 2), ev("D", 3), ev("C", 4), ev("E", 5)],
    );
    assert_eq!(got, ["a=1,d=3,e=5"]);
    // Through B and C, then the D is too late.
    let got = run(
        &mut engine(sequence_or_single()),
        &[ev("A", 1), ev("B", 2), ev("C", 3), ev("D", 4), ev("E", 5)],
    );
    assert_eq!(got, ["a=1,b=2,c=3,e=5"]);
}

#[test]
fn a_started_and_alternative_leaves_the_others_open() {
    // X -> ((A and B) or C)
    let p = || {
        seq(vec![
            step("X", "x"),
            or(and(step("A", "a"), step("B", "b")), step("C", "c")),
        ])
    };
    assert_eq!(
        run(&mut engine(p()), &[ev("X", 1), ev("A", 2), ev("C", 3)]),
        ["c=3,x=1"]
    );
    assert_eq!(
        run(
            &mut engine(p()),
            &[ev("X", 1), ev("A", 2), ev("B", 3), ev("C", 4)]
        ),
        ["a=2,b=3,x=1"]
    );
}

#[test]
fn an_event_waiting_in_an_and_alternative_leaves_the_others_open() {
    // X -> ((A as a and B where key == a.key as b) or C)
    let p = seq(vec![
        step("X", "x"),
        or(
            and(step("A", "a"), step_where("B", "b", same_key("a"))),
            step("C", "c"),
        ),
    ]);
    assert_eq!(
        run(&mut engine(p), &[ev("X", 1), keyed("B", 2, 9), ev("C", 3)]),
        ["c=3,x=1"]
    );
}

#[test]
fn alternatives_that_start_alike_are_all_followed() {
    // X -> ((B -> C) or (B -> D))
    let p = || {
        seq(vec![
            step("X", "x"),
            or(
                seq(vec![step("B", "b"), step("C", "c")]),
                seq(vec![step("B", "b"), step("D", "d")]),
            ),
        ])
    };
    assert_eq!(
        run(&mut engine(p()), &[ev("X", 1), ev("B", 2), ev("D", 3)]),
        ["b=2,d=3,x=1"]
    );
    assert_eq!(
        run(
            &mut engine(p()),
            &[ev("X", 1), ev("B", 2), ev("C", 3), ev("D", 4)]
        ),
        ["b=2,c=3,x=1"]
    );
}

#[test]
fn alternatives_that_start_alike_at_the_start_are_all_followed() {
    // (B -> C) or (B -> D)
    let p = || {
        or(
            seq(vec![step("B", "b"), step("C", "c")]),
            seq(vec![step("B", "b"), step("D", "d")]),
        )
    };
    assert_eq!(
        run(&mut engine(p()), &[ev("B", 1), ev("D", 2)]),
        ["b=1,d=2"]
    );
    assert_eq!(
        run(&mut engine(p()), &[ev("B", 1), ev("C", 2), ev("D", 3)]),
        ["b=1,c=2"]
    );
}

#[test]
fn alternatives_are_followed_in_each_partition() {
    let mut e = engine(sequence_or_single()).with_partition_by("key".into());
    let got = run(
        &mut e,
        &[
            keyed("A", 1, 1),
            keyed("A", 2, 2),
            keyed("B", 3, 1),
            keyed("B", 4, 2),
            keyed("D", 5, 1),
            keyed("C", 6, 2),
            keyed("C", 7, 1),
            keyed("E", 8, 1),
            keyed("E", 9, 2),
        ],
    );
    assert_eq!(got, ["a=1,d=5,e=8", "a=2,b=4,c=6,e=9"]);
}

#[test]
fn alternatives_are_followed_with_the_predicate_index_on_and_off() {
    for enabled in [false, true] {
        let mut e = engine(sequence_or_single()).with_predicate_index(enabled);
        let got = run(
            &mut e,
            &[ev("A", 1), ev("B", 2), ev("D", 3), ev("C", 4), ev("E", 5)],
        );
        assert_eq!(got, ["a=1,d=3,e=5"], "index {enabled}");
    }
}

#[test]
fn a_fork_past_the_run_cap_commits_the_run_to_its_alternative() {
    let mut e = engine(sequence_or_single()).with_max_runs(1);
    // No room for the copy that takes B: the run takes B itself, and gives
    // up D (counted as a dropped run).
    let got = run(&mut e, &[ev("A", 1), ev("B", 2), ev("C", 3), ev("E", 4)]);
    assert_eq!(got, ["a=1,b=2,c=3,e=4"]);
    assert_eq!(e.total_runs_dropped, 1);
}

#[test]
fn a_fork_evicts_no_member_of_its_own_family() {
    use varpulis_sase::BackpressureStrategy;
    // A -> ((B -> C) or (D -> E)), one run allowed, evicting the oldest.
    let p = || {
        seq(vec![
            step("A", "a"),
            or(
                seq(vec![step("B", "b"), step("C", "c")]),
                seq(vec![step("D", "d"), step("E", "e")]),
            ),
        ])
    };
    let mut e = engine(p())
        .with_max_runs(1)
        .with_backpressure(BackpressureStrategy::EvictOldest);
    let got = run(&mut e, &[ev("A", 1), ev("B", 2), ev("C", 3)]);
    assert_eq!(got, ["a=1,b=2,c=3"]);
    // The only other run is its parent: no eviction, the parent commits.
    assert_eq!(e.total_runs_evicted, 0);
}

#[test]
fn a_fork_past_the_run_cap_keeps_its_deadline_in_a_partition() {
    // A -> ((B and C) or D) -> NOT F within 10s, one run per partition. C@5
    // moves the window to [5, 15]; B@12 completes the `and` in the copy that
    // replaces its parent at the cap. The absence must be confirmed once the
    // watermark passes 15, which only a filed deadline makes the sweep visit.
    let p = within(
        SasePattern::Seq(vec![
            step("A", "a"),
            or(and(step("B", "b"), step("C", "c")), step("D", "d")),
            SasePattern::Not(Box::new(step("F", "f"))),
        ]),
        10,
    );
    let mut e = engine(p)
        .with_event_time()
        .with_partition_by("key".into())
        .with_max_runs(1);
    let got = run(
        &mut e,
        &[
            at(keyed("A", 1, 1), 10),
            at(keyed("C", 2, 1), 5),
            at(keyed("B", 3, 1), 12),
            at(keyed("Z", 4, 2), 17),
        ],
    );
    assert_eq!(got, ["a=1,b=3,c=2"]);
}

#[test]
fn an_or_in_an_alternative_is_followed_too() {
    // A -> ((B -> ((C -> F) or G)) or D) -> E
    let p = || {
        seq(vec![
            step("A", "a"),
            or(
                seq(vec![
                    step("B", "b"),
                    or(seq(vec![step("C", "c"), step("F", "f")]), step("G", "g")),
                ]),
                step("D", "d"),
            ),
            step("E", "e"),
        ])
    };
    assert_eq!(
        run(
            &mut engine(p()),
            &[
                ev("A", 1),
                ev("B", 2),
                ev("C", 3),
                ev("G", 4),
                ev("D", 5),
                ev("E", 6)
            ]
        ),
        ["a=1,b=2,e=6,g=4"]
    );
    assert_eq!(
        run(
            &mut engine(p()),
            &[
                ev("A", 1),
                ev("B", 2),
                ev("C", 3),
                ev("D", 4),
                ev("F", 5),
                ev("E", 6)
            ]
        ),
        ["a=1,d=4,e=6"]
    );
}

// ---------------------------------------------------------------------------
// caps
// ---------------------------------------------------------------------------

#[test]
fn a_zero_event_cap_holds_no_waiting_event() {
    // A as a and B where key == a.key as b, nothing may wait.
    let p = and(step("A", "a"), step_where("B", "b", same_key("a")));
    let mut e = engine(p).with_max_kleene_events(0);
    let got = run(
        &mut e,
        &[keyed("B", 1, 1), keyed("A", 2, 1), keyed("B", 3, 1)],
    );
    assert_eq!(got, ["a=2,b=3"]);
}

#[test]
fn a_tie_goes_to_the_leftmost_alternative() {
    // X -> ((A and B) or B): the B completes the copy in the `and` and the
    // run's own alternative at once. The `and`, on the left, wins, whatever
    // the order the runs are visited in.
    let p = || {
        seq(vec![
            step("X", "x"),
            or(and(step("A", "a"), step("B", "b")), step("B", "b")),
        ])
    };
    // Two Xs: when the first run completes and leaves the list, the copy of
    // the second is swapped into its place and visited before its run.
    let events = [ev("X", 1), ev("X", 2), ev("A", 3), ev("B", 4)];
    for index in [false, true] {
        let mut e = engine(p()).with_predicate_index(index);
        assert_eq!(
            run(&mut e, &events),
            ["a=3,b=4,x=1", "a=3,b=4,x=2"],
            "index {index}"
        );
    }
    let mut e = engine(p()).with_partition_by("key".into());
    let mut got = run(&mut e, &events);
    got.sort();
    assert_eq!(got, ["a=3,b=4,x=1", "a=3,b=4,x=2"]);
}

#[test]
fn a_tie_does_not_depend_on_how_the_run_forked() {
    // A -> ((B as p -> C) or (D as p -> C)): B and D both wait for the C.
    // The run forks into B and takes D itself; with a third alternative it
    // forks into both. Either way the leftmost, B, wins.
    let two = || {
        seq(vec![
            step("A", "a"),
            or(
                seq(vec![step("B", "p"), step("C", "c")]),
                seq(vec![step("D", "p"), step("C", "c")]),
            ),
        ])
    };
    let three = || {
        seq(vec![
            step("A", "a"),
            or(
                or(
                    seq(vec![step("B", "p"), step("C", "c")]),
                    seq(vec![step("D", "p"), step("C", "c")]),
                ),
                seq(vec![step("Q", "q"), step("C", "c")]),
            ),
        ])
    };
    let events = [ev("A", 0), ev("B", 1), ev("D", 2), ev("C", 3)];
    assert_eq!(run(&mut engine(two()), &events), ["a=0,c=3,p=1"]);
    assert_eq!(run(&mut engine(three()), &events), ["a=0,c=3,p=1"]);
}

#[test]
fn strict_contiguity_holds_across_alternatives() {
    // X -> ((B -> C) or (D -> E)) under `.strict()`: once B went into the
    // first alternative, a D cannot start the second.
    let p = || {
        seq(vec![
            step("X", "x"),
            or(
                seq(vec![step("B", "b"), step("C", "c")]),
                seq(vec![step("D", "d"), step("E", "e")]),
            ),
        ])
    };
    let strict = || engine(p()).with_strategy(SelectionStrategy::StrictContiguous);
    assert!(run(
        &mut strict(),
        &[ev("X", 1), ev("B", 2), ev("D", 3), ev("E", 4)]
    )
    .is_empty());
    assert_eq!(
        run(&mut strict(), &[ev("X", 1), ev("B", 2), ev("C", 3)]),
        ["b=2,c=3,x=1"]
    );
    assert_eq!(
        run(&mut strict(), &[ev("X", 1), ev("D", 2), ev("E", 3)]),
        ["d=2,e=3,x=1"]
    );
}

#[test]
fn a_closure_after_a_closure_emits() {
    // A -> all B -> all C: the second closure is entered from the first.
    let p = seq(vec![step("A", "a"), closure("B", "bs"), closure("C", "cs")]);
    let mut e = SaseEngine::new(p);
    let got: Vec<MatchResult> = [ev("A", 1), ev("B", 2), ev("C", 3)]
        .iter()
        .flat_map(|x| e.process(x))
        .collect();
    assert!(!got.is_empty());
    assert_eq!(stack(&got[0]), ["A 1", "B 2", "C 3"]);
}

#[test]
fn a_tie_loser_waiting_in_the_predicate_index_ends_too() {
    // A -> ((C -> D) or D) -> E where key == a.key. D completes both
    // alternatives at once: the copy that took C and the run's own D. The
    // left one wins the tie. The loser has left the `or` too and waits for
    // an E it could be filed under, so it must not reach the index before
    // its family settles, or no settle can find it and it matches as well.
    let p = || {
        seq(vec![
            step("A", "a"),
            or(seq(vec![step("C", "c"), step("D", "d")]), step("D", "d")),
            step_where("E", "e", same_key("a")),
        ])
    };
    let events = [
        keyed("A", 1, 1),
        keyed("C", 2, 1),
        keyed("D", 3, 1),
        keyed("E", 4, 1),
    ];
    for index in [false, true] {
        let mut e = engine(p()).with_predicate_index(index);
        assert_eq!(run(&mut e, &events), ["a=1,c=2,d=3,e=4"], "index {index}");
    }
    let mut e = engine(p())
        .with_partition_by("key".into())
        .with_predicate_index(true);
    assert_eq!(run(&mut e, &events), ["a=1,c=2,d=3,e=4"], "partitioned");
}
