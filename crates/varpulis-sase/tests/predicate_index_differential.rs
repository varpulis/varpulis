//! IDX-02: the run index changes which runs an event visits, never what
//! matches.
//!
//! Each pattern runs over the same random stream with the index always on,
//! never on, and in its automatic default. Every event must produce the same
//! matches in the same order, and leave as many active runs.
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
            "text-interval",
            SasePattern::Seq(vec![
                a(),
                step(
                    "B",
                    "b",
                    Some(and(
                        cref("name", CompareOp::Ge, "a", "name"),
                        cref("name", CompareOp::Le, "a", "upto"),
                    )),
                ),
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

    let names = ["a", "b", "c", "d"];
    let name = names[rng.below(4) as usize];
    let upto = names[rng.below(4) as usize];
    event
        .with_field("name", Value::Str(name.into()))
        .with_field("upto", Value::Str(upto.into()))
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
    backpressure: &BackpressureStrategy,
    index: Option<bool>,
) -> SaseEngine {
    let engine = SaseEngine::new(pattern.clone())
        .with_strategy(strategy)
        .with_max_runs(max_runs)
        .with_backpressure(backpressure.clone());
    match index {
        Some(on) => engine.with_predicate_index(on),
        None => engine,
    }
}

/// Run both engines over one stream; returns the most runs the index held.
fn compare(
    name: &str,
    pattern: &SasePattern,
    strategy: SelectionStrategy,
    max_runs: usize,
    backpressure: &BackpressureStrategy,
    seed: u64,
    events: i64,
) -> usize {
    let mut plain = engine(pattern, strategy, max_runs, backpressure, Some(false));
    let mut indexed = engine(pattern, strategy, max_runs, backpressure, Some(true));
    let mut auto = engine(pattern, strategy, max_runs, backpressure, None);
    let mut rng = Rng(seed);
    let mut at = DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp");
    let mut most_parked = 0;
    let mut matches = 0;

    for id in 0..events {
        at += chrono::Duration::milliseconds(rng.below(1500) as i64);
        let event = random_event(&mut rng, id, at);

        let expected: Vec<String> = plain.process(&event).iter().map(describe).collect();
        let actual: Vec<String> = indexed.process(&event).iter().map(describe).collect();
        let automatic: Vec<String> = auto.process(&event).iter().map(describe).collect();
        assert_eq!(
            actual, expected,
            "{name} ({strategy:?}, seed {seed}): matches differ at event {id} {event:?}"
        );
        assert_eq!(
            automatic, expected,
            "{name} ({strategy:?}, seed {seed}): automatic mode differs at event {id}"
        );
        assert_eq!(
            indexed.stats().active_runs,
            plain.stats().active_runs,
            "{name} ({strategy:?}, seed {seed}): active runs differ after event {id}"
        );
        assert_eq!(auto.stats().active_runs, plain.stats().active_runs);
        // Runs dropped or evicted at capacity are counted the same way.
        for other in [&indexed, &auto] {
            assert_eq!(
                (other.total_runs_dropped, other.total_runs_evicted),
                (plain.total_runs_dropped, plain.total_runs_evicted),
                "{name} ({strategy:?}, seed {seed}): loss counters differ after event {id}"
            );
        }
        assert_eq!(plain.parked_run_count(), 0);
        matches += expected.len();
        most_parked = most_parked.max(indexed.parked_run_count());
    }
    assert!(
        matches > 0,
        "{name}: the stream never matched, the comparison is vacuous"
    );
    LOSSES.fetch_add(
        plain.total_runs_dropped + plain.total_runs_evicted,
        std::sync::atomic::Ordering::Relaxed,
    );
    most_parked
}

/// Runs dropped or evicted by `compare`, so a capacity test can check the
/// cap was reached at all.
static LOSSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
                    &BackpressureStrategy::Drop,
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
    // Dropping new runs, and evicting old ones: the index puts its runs
    // back in the list to evict, and a tie must go to the same run.
    for backpressure in [
        BackpressureStrategy::Drop,
        BackpressureStrategy::EvictOldest,
        BackpressureStrategy::EvictLeastProgress,
    ] {
        let before = LOSSES.load(std::sync::atomic::Ordering::Relaxed);
        for (name, pattern, _) in patterns() {
            for seed in 1..=4 {
                compare(
                    name,
                    &pattern,
                    SelectionStrategy::SkipTillAnyMatch,
                    25,
                    &backpressure,
                    seed * 0x51_7CC1,
                    1_000,
                );
            }
        }
        assert!(
            LOSSES.load(std::sync::atomic::Ordering::Relaxed) > before,
            "{backpressure:?}: the cap never bit"
        );
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

#[test]
fn automatic_mode_switches_with_the_number_of_runs() {
    use varpulis_sase::{INDEX_OFF_RUNS, INDEX_ON_RUNS};
    // The variable replaces the automatic default, which is what this test
    // is about.
    if std::env::var_os("VARPULIS_SASE_INDEX").is_some() {
        return;
    }
    let pattern = SasePattern::Seq(vec![step("A", "a", None), step("B", "b", Some(key_eq()))]);
    let mut engine = SaseEngine::new(pattern).with_max_runs(1_000_000);
    let a = |k: usize| Event::new("A").with_field("key", Value::Int(k as i64));
    let b = |k: usize| Event::new("B").with_field("key", Value::Int(k as i64));

    for k in 0..INDEX_ON_RUNS - 1 {
        let _ = engine.process(&a(k));
    }
    let _ = engine.process(&b(1_000_000));
    assert_eq!(engine.parked_run_count(), 0, "below the threshold");

    let _ = engine.process(&a(INDEX_ON_RUNS - 1));
    let _ = engine.process(&b(1_000_000));
    assert_eq!(engine.parked_run_count(), INDEX_ON_RUNS, "at the threshold");

    // Completing runs brings the count down; the index lets go only under
    // the lower threshold.
    for k in 0..INDEX_ON_RUNS - INDEX_OFF_RUNS {
        assert_eq!(engine.process(&b(k)).len(), 1);
    }
    let _ = engine.process(&b(1_000_000));
    assert_eq!(engine.parked_run_count(), INDEX_OFF_RUNS);
    assert_eq!(engine.process(&b(INDEX_ON_RUNS - 1)).len(), 1);
    let _ = engine.process(&b(1_000_000));
    assert_eq!(engine.parked_run_count(), 0, "under the lower threshold");
    assert_eq!(engine.stats().active_runs, INDEX_OFF_RUNS - 1);
}
