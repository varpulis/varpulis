//! IDX-02: the run index changes which runs an event visits, never what
//! matches.
//!
//! Each pattern runs twice over the same random stream, with and without the
//! index. Every event must produce the same matches — as a multiset, since
//! the order within one event may differ — and leave as many active runs.
//! The streams mix key kinds (`Int`, an equal `Float`, `Str`, `Null`,
//! missing) and let windows expire, so the fallbacks are exercised along with
//! the index.

use std::time::Duration;

use chrono::{DateTime, Utc};
use varpulis_core::{Event, Value};
use varpulis_sase::{
    BackpressureStrategy, CompareOp, MatchResult, Predicate, SaseEngine, SasePattern,
    SelectionStrategy,
};

/// xorshift64*: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn cref(field: &str, op: CompareOp, alias: &str, ref_field: &str) -> Predicate {
    Predicate::CompareRef {
        field: field.into(),
        op,
        ref_alias: alias.into(),
        ref_field: ref_field.into(),
    }
}

fn and(left: Predicate, right: Predicate) -> Predicate {
    Predicate::And(Box::new(left), Box::new(right))
}

fn step(event_type: &str, alias: &str, predicate: Option<Predicate>) -> SasePattern {
    SasePattern::Event {
        event_type: event_type.into(),
        predicate,
        alias: Some(alias.into()),
    }
}

fn key_eq() -> Predicate {
    cref("key", CompareOp::Eq, "a", "key")
}

fn in_interval() -> Predicate {
    and(
        cref("price", CompareOp::Ge, "a", "lo"),
        cref("price", CompareOp::Lt, "a", "hi"),
    )
}

/// The patterns, and whether the index must have filed runs for them.
fn patterns() -> Vec<(&'static str, SasePattern, bool)> {
    let a = || step("A", "a", None);
    vec![
        (
            "eq",
            SasePattern::Seq(vec![a(), step("B", "b", Some(key_eq()))]),
            true,
        ),
        (
            "interval",
            SasePattern::Seq(vec![a(), step("B", "b", Some(in_interval()))]),
            true,
        ),
        (
            "eq+interval",
            SasePattern::Seq(vec![
                a(),
                step("B", "b", Some(and(key_eq(), in_interval()))),
            ]),
            true,
        ),
        (
            "lower",
            SasePattern::Seq(vec![
                a(),
                step("B", "b", Some(cref("price", CompareOp::Gt, "a", "lo"))),
            ]),
            true,
        ),
        (
            "upper",
            SasePattern::Seq(vec![
                a(),
                step("B", "b", Some(cref("price", CompareOp::Le, "a", "hi"))),
            ]),
            true,
        ),
        (
            "residual",
            SasePattern::Seq(vec![
                a(),
                step(
                    "B",
                    "b",
                    Some(and(key_eq(), cref("price", CompareOp::NotEq, "a", "lo"))),
                ),
            ]),
            true,
        ),
        (
            "text",
            SasePattern::Seq(vec![
                a(),
                step("B", "b", Some(cref("name", CompareOp::Ge, "a", "name"))),
            ]),
            true,
        ),
        (
            "three-steps",
            SasePattern::Seq(vec![
                a(),
                step("B", "b", Some(key_eq())),
                step(
                    "C",
                    "c",
                    Some(and(key_eq(), cref("price", CompareOp::Gt, "b", "price"))),
                ),
            ]),
            true,
        ),
        (
            // The same event type twice in a row: a run that one `B` moves
            // must not be offered that same `B` again from the index.
            "same-type-twice",
            SasePattern::Seq(vec![
                a(),
                step("B", "b1", Some(key_eq())),
                step("B", "b2", Some(key_eq())),
            ]),
            true,
        ),
        (
            // `!=` cannot be planned, so runs waiting for `b1` stay in the
            // visited list; the `B` that moves one makes it wait for a `B`
            // the index can find. Filed during the pass, it must not be
            // offered that same `B`.
            "unplanned-then-planned",
            SasePattern::Seq(vec![
                a(),
                step("B", "b1", Some(cref("key", CompareOp::NotEq, "a", "key"))),
                step("B", "b2", Some(cref("key", CompareOp::Eq, "b1", "key"))),
            ]),
            true,
        ),
        (
            "within",
            SasePattern::Within(
                Box::new(SasePattern::Seq(vec![a(), step("B", "b", Some(key_eq()))])),
                Duration::from_secs(5),
            ),
            true,
        ),
        (
            // Runs waiting for `B` are filed; once `B` arrives they wait out
            // the negated step with a pending negation, which keeps them in
            // the visited list, and they complete when the watermark passes
            // the deadline.
            "negation",
            SasePattern::Within(
                Box::new(SasePattern::Seq(vec![
                    a(),
                    step("B", "b", Some(key_eq())),
                    SasePattern::Not(Box::new(step("C", "c", Some(key_eq())))),
                ])),
                Duration::from_secs(5),
            ),
            true,
        ),
        (
            "kleene",
            SasePattern::Seq(vec![
                a(),
                SasePattern::KleenePlus(Box::new(step("B", "bs", Some(key_eq())))),
                step("C", "c", Some(key_eq())),
            ]),
            false,
        ),
    ]
}

/// A random event; `id` identifies it in the matches.
fn random_event(rng: &mut Rng, id: i64, at: DateTime<Utc>) -> Event {
    let event_type = ["A", "A", "B", "B", "B", "C"][rng.below(6) as usize];
    let mut event = Event::new(event_type).with_field("id", Value::Int(id));
    event.timestamp = at;

    let k = rng.below(4) as i64;
    match rng.below(10) {
        0 => event = event.with_field("key", Value::Float(k as f64)),
        1 => event = event.with_field("key", Value::Str(format!("k{k}").into())),
        2 => event = event.with_field("key", Value::Null),
        3 => {}
        _ => event = event.with_field("key", Value::Int(k)),
    }

    let price = rng.below(40) as f64 / 2.0;
    event = if rng.below(4) == 0 {
        event.with_field("price", Value::Int(price as i64))
    } else {
        event.with_field("price", Value::Float(price))
    };

    let lo = rng.below(20) as f64;
    let hi = lo + rng.below(5) as f64 - 1.0;
    event = event
        .with_field("lo", Value::Float(lo))
        .with_field("hi", Value::Float(hi));

    let name = ["a", "b", "c", "d"][rng.below(4) as usize];
    event.with_field("name", Value::Str(name.into()))
}

fn id_of(event: &Event) -> String {
    match event.get("id") {
        Some(Value::Int(id)) => id.to_string(),
        other => format!("{other:?}"),
    }
}

/// A match as a comparable string: captured aliases, then the stack.
fn describe(m: &MatchResult) -> String {
    let mut captured: Vec<String> = m
        .captured
        .iter()
        .map(|(alias, event)| format!("{alias}={}", id_of(event)))
        .collect();
    captured.sort();
    let stack: Vec<String> = m.stack.iter().map(|entry| id_of(&entry.event)).collect();
    format!("{} | {}", captured.join(","), stack.join(","))
}

fn engine(
    pattern: &SasePattern,
    strategy: SelectionStrategy,
    max_runs: usize,
    indexed: bool,
) -> SaseEngine {
    SaseEngine::new(pattern.clone())
        .with_strategy(strategy)
        .with_max_runs(max_runs)
        .with_backpressure(BackpressureStrategy::Drop)
        .with_predicate_index(indexed)
}

/// Run both engines over one stream; returns the most runs the index held.
fn compare(
    name: &str,
    pattern: &SasePattern,
    strategy: SelectionStrategy,
    max_runs: usize,
    seed: u64,
    events: i64,
) -> usize {
    let mut plain = engine(pattern, strategy, max_runs, false);
    let mut indexed = engine(pattern, strategy, max_runs, true);
    let mut rng = Rng(seed);
    let mut at = DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp");
    let mut most_parked = 0;
    let mut matches = 0;

    for id in 0..events {
        at += chrono::Duration::milliseconds(rng.below(1500) as i64);
        let event = random_event(&mut rng, id, at);

        let mut expected: Vec<String> = plain.process(&event).iter().map(describe).collect();
        let mut actual: Vec<String> = indexed.process(&event).iter().map(describe).collect();
        expected.sort();
        actual.sort();
        assert_eq!(
            actual, expected,
            "{name} ({strategy:?}, seed {seed}): matches differ at event {id} {event:?}"
        );
        assert_eq!(
            indexed.stats().active_runs,
            plain.stats().active_runs,
            "{name} ({strategy:?}, seed {seed}): active runs differ after event {id}"
        );
        assert_eq!(plain.parked_run_count(), 0);
        matches += expected.len();
        most_parked = most_parked.max(indexed.parked_run_count());
    }
    assert!(
        matches > 0,
        "{name}: the stream never matched, the comparison is vacuous"
    );
    most_parked
}

#[test]
fn index_never_changes_matches() {
    for (name, pattern, must_engage) in patterns() {
        for strategy in [
            SelectionStrategy::SkipTillAnyMatch,
            SelectionStrategy::SkipTillNextMatch,
        ] {
            let mut most_parked = 0;
            for seed in 1..=8 {
                most_parked = most_parked.max(compare(
                    name,
                    &pattern,
                    strategy,
                    1_000_000,
                    seed * 0x9E37_79B9,
                    1_500,
                ));
            }
            if must_engage {
                assert!(
                    most_parked > 0,
                    "{name} ({strategy:?}): the index never filed a run"
                );
            }
        }
    }
}

#[test]
fn index_never_changes_matches_at_capacity() {
    for (name, pattern, _) in patterns() {
        for seed in 1..=4 {
            compare(
                name,
                &pattern,
                SelectionStrategy::SkipTillAnyMatch,
                25,
                seed * 0x51_7CC1,
                1_000,
            );
        }
    }
}

#[test]
fn strict_contiguity_and_negations_keep_the_index_off() {
    let pattern = SasePattern::Seq(vec![step("A", "a", None), step("B", "b", Some(key_eq()))]);
    let a = Event::new("A").with_field("key", Value::Int(1));

    let mut strict = SaseEngine::new(pattern.clone())
        .with_strategy(SelectionStrategy::StrictContiguous)
        .with_predicate_index(true);
    let _ = strict.process(&a);
    assert_eq!(strict.parked_run_count(), 0);

    let mut negated = SaseEngine::new(pattern).with_predicate_index(true);
    let _ = negated.process(&a);
    assert_eq!(negated.parked_run_count(), 1);
    // A global negation must see every run: adding one releases them.
    negated.add_negation("C".into(), None);
    assert_eq!(negated.parked_run_count(), 0);
    assert_eq!(negated.stats().active_runs, 1);
    let _ = negated.process(&Event::new("C"));
    assert_eq!(negated.stats().active_runs, 0);
}
