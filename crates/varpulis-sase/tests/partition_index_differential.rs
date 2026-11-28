//! IDX-03: under `partition by`, each partition's run index changes which
//! runs an event visits, never what matches.
//!
//! Each pattern runs over the same random stream, partitioned by a field of
//! low cardinality so partitions fill up, with the index always on, never
//! on, and in its automatic default. Every event must produce the same
//! matches in the same order, and leave as many active runs, partitions,
//! dropped and evicted runs. The streams arrive out of order under event
//! time, so the event-time skip, the partition deadlines and the watermark
//! sweep are exercised along with the index; capacity and processing time
//! have their own tests.

use std::time::Duration;

use chrono::{DateTime, Utc};
use varpulis_core::{Event, Value};
use varpulis_sase::{
    BackpressureStrategy, CompareOp, MatchResult, Predicate, SaseEngine, SasePattern,
    SelectionStrategy, INDEX_OFF_RUNS, INDEX_ON_RUNS,
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

fn within(p: SasePattern, secs: u64) -> SasePattern {
    SasePattern::Within(Box::new(p), Duration::from_secs(secs))
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
            // A run that one `B` moves must not be offered that same `B`
            // again from the partition's index.
            "same-type-twice",
            SasePattern::Seq(vec![
                a(),
                step("B", "b1", Some(key_eq())),
                step("B", "b2", Some(key_eq())),
            ]),
            true,
        ),
        (
            // Runs waiting for `b1` stay visited (`!=` cannot be planned);
            // the `B` that moves one files it during the pass.
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
            within(
                SasePattern::Seq(vec![a(), step("B", "b", Some(key_eq()))]),
                5,
            ),
            true,
        ),
        (
            // Parked while waiting for `B`, then visited while waiting out
            // the negated step; completed by the watermark sweep.
            "negation",
            within(
                SasePattern::Seq(vec![
                    a(),
                    step("B", "b", Some(key_eq())),
                    SasePattern::Not(Box::new(step("C", "c", Some(key_eq())))),
                ]),
                5,
            ),
            true,
        ),
        (
            // Parked while waiting for `B`, then visited while the negated
            // step in the middle forbids `C`, until `D` completes the run.
            "negation-in-the-middle",
            within(
                SasePattern::Seq(vec![
                    a(),
                    step("B", "b", Some(key_eq())),
                    SasePattern::Not(Box::new(step("C", "c", Some(key_eq())))),
                    step("B", "b2", Some(key_eq())),
                ]),
                5,
            ),
            true,
        ),
        (
            // A closure, then an absence in the middle.
            "kleene-then-negation",
            within(
                SasePattern::Seq(vec![
                    a(),
                    SasePattern::KleenePlus(Box::new(step("B", "bs", Some(key_eq())))),
                    SasePattern::Not(Box::new(step("C", "c", Some(key_eq())))),
                    step("B", "b2", Some(cref("price", CompareOp::Gt, "a", "price"))),
                ]),
                5,
            ),
            false,
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
        (
            // Members of an `or` family stay visited; the run before the
            // `or` and the runs after it may be parked.
            "or",
            within(
                SasePattern::Seq(vec![
                    a(),
                    SasePattern::Or(
                        Box::new(step("B", "b", Some(key_eq()))),
                        Box::new(SasePattern::Seq(vec![
                            step("C", "c", Some(key_eq())),
                            step("B", "b2", Some(key_eq())),
                        ])),
                    ),
                ]),
                8,
            ),
            false,
        ),
        (
            // Both alternatives start with the same unpredicated `C`, so a run
            // at the `or` forks a copy per alternative. The runs reaching the
            // `or` come out of the index in its order, not the visited list's:
            // the copies must still be numbered, admitted and emitted in the
            // order of the runs they were forked from.
            "or-forking",
            within(
                SasePattern::Seq(vec![
                    a(),
                    step("B", "b", Some(cref("price", CompareOp::Gt, "a", "price"))),
                    SasePattern::Or(
                        Box::new(SasePattern::Seq(vec![
                            step("C", "c1", None),
                            step("B", "b2", Some(key_eq())),
                        ])),
                        Box::new(SasePattern::Seq(vec![
                            step("C", "c2", None),
                            step("C", "c3", Some(key_eq())),
                        ])),
                    ),
                ]),
                8,
            ),
            true,
        ),
        (
            "and",
            within(
                SasePattern::And(
                    Box::new(step("A", "a", None)),
                    Box::new(step("B", "b", Some(cref("key", CompareOp::Eq, "a", "key")))),
                ),
                5,
            ),
            false,
        ),
    ]
}

/// A random event; `id` identifies it in the matches, `part` is the
/// partition key, one of `partitions`.
fn random_event(rng: &mut Rng, id: i64, at: DateTime<Utc>, partitions: u64) -> Event {
    let event_type = ["A", "A", "B", "B", "B", "C"][rng.below(6) as usize];
    let mut event = Event::new(event_type)
        .with_field("id", Value::Int(id))
        .with_field("part", Value::Int(rng.below(partitions) as i64));
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
    event = event.with_field("price", Value::Float(price));
    let lo = rng.below(20) as f64;
    let hi = lo + rng.below(5) as f64 - 1.0;
    event = event
        .with_field("lo", Value::Float(lo))
        .with_field("hi", Value::Float(hi));
    let names = ["a", "b", "c", "d"];
    event.with_field("name", Value::Str(names[rng.below(4) as usize].into()))
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

#[derive(Clone)]
struct Setup {
    strategy: SelectionStrategy,
    max_runs: usize,
    backpressure: BackpressureStrategy,
    partitions: u64,
    /// Watermark lag; with a lag, events arrive out of order.
    lag: Option<Duration>,
}

impl Setup {
    fn new(strategy: SelectionStrategy, partitions: u64) -> Self {
        Self {
            strategy,
            max_runs: 1_000_000,
            backpressure: BackpressureStrategy::Drop,
            partitions,
            lag: None,
        }
    }

    fn engine(&self, pattern: &SasePattern, index: Option<bool>) -> SaseEngine {
        let mut engine = SaseEngine::new(pattern.clone())
            .with_strategy(self.strategy)
            .with_max_runs(self.max_runs)
            .with_backpressure(self.backpressure.clone())
            .with_partition_by("part".into());
        if let Some(lag) = self.lag {
            engine = engine.with_max_out_of_orderness(lag);
        }
        match index {
            Some(on) => engine.with_predicate_index(on),
            None => engine,
        }
    }
}

/// What the three engines must agree on after every event.
fn state(engine: &SaseEngine) -> (usize, usize, u64, u64) {
    let stats = engine.stats();
    (
        stats.active_runs,
        stats.partitions,
        engine.total_runs_dropped,
        engine.total_runs_evicted,
    )
}

/// Matches, the most runs the index held, and the runs lost to the cap.
struct Outcome {
    matches: usize,
    most_parked: usize,
    losses: u64,
}

/// Run the three engines over one stream.
fn compare(name: &str, pattern: &SasePattern, setup: &Setup, seed: u64, events: i64) -> Outcome {
    let mut plain = setup.engine(pattern, Some(false));
    let mut indexed = setup.engine(pattern, Some(true));
    let mut auto = setup.engine(pattern, None);
    let mut rng = Rng(seed);
    let mut at = DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp");
    let mut outcome = Outcome {
        matches: 0,
        most_parked: 0,
        losses: 0,
    };
    let strategy = setup.strategy;
    let partitions = setup.partitions;

    for id in 0..events {
        at += chrono::Duration::milliseconds(rng.below(1500) as i64);
        // Out of order by up to three seconds when the watermark lags.
        let stamped = match setup.lag {
            Some(_) => at - chrono::Duration::milliseconds(rng.below(3000) as i64),
            None => at,
        };
        let event = random_event(&mut rng, id, stamped, partitions);

        let expected: Vec<String> = plain.process(&event).iter().map(describe).collect();
        let actual: Vec<String> = indexed.process(&event).iter().map(describe).collect();
        let automatic: Vec<String> = auto.process(&event).iter().map(describe).collect();
        let context = format!(
            "{name} ({strategy:?}, {} partitions, max {} {:?}, lag {:?}, seed {seed})",
            partitions, setup.max_runs, setup.backpressure, setup.lag
        );
        assert_eq!(
            actual, expected,
            "{context}: matches differ at event {id} {event:?}"
        );
        assert_eq!(
            automatic, expected,
            "{context}: automatic mode differs at event {id}"
        );
        assert_eq!(
            state(&indexed),
            state(&plain),
            "{context}: after event {id}"
        );
        assert_eq!(
            state(&auto),
            state(&plain),
            "{context}: auto, after event {id}"
        );
        assert_eq!(plain.parked_run_count(), 0);
        outcome.matches += expected.len();
        outcome.most_parked = outcome.most_parked.max(indexed.parked_run_count());
    }
    outcome.losses = plain.total_runs_dropped + plain.total_runs_evicted;
    outcome
}

#[test]
fn partition_index_never_changes_matches() {
    for (name, pattern, must_engage) in patterns() {
        for strategy in [
            SelectionStrategy::SkipTillAnyMatch,
            SelectionStrategy::SkipTillNextMatch,
        ] {
            // One partition, a few, and many with a run or two each. With
            // many, a long pattern may never complete: matches are counted
            // over the three.
            let mut matches = 0;
            for partitions in [1, 3, 40] {
                let mut most_parked = 0;
                for (n, lag) in [None, Some(Duration::from_secs(2))].into_iter().enumerate() {
                    let setup = Setup {
                        lag,
                        ..Setup::new(strategy, partitions)
                    };
                    for seed in 1..=3 {
                        let outcome = compare(
                            name,
                            &pattern,
                            &setup,
                            (seed + 10 * n as u64) * 0x9E37_79B9,
                            1_200,
                        );
                        most_parked = most_parked.max(outcome.most_parked);
                        matches += outcome.matches;
                    }
                }
                if must_engage {
                    assert!(
                        most_parked > 0,
                        "{name} ({strategy:?}, {partitions}): the index never filed a run"
                    );
                }
            }
            assert!(
                matches > 0,
                "{name} ({strategy:?}): never matched, the comparison is vacuous"
            );
        }
    }
}

#[test]
fn partition_index_never_changes_matches_at_capacity() {
    // The cap is per partition: dropping new runs, evicting old ones (the
    // victim may be parked), sampling.
    for backpressure in [
        BackpressureStrategy::Drop,
        BackpressureStrategy::EvictOldest,
        BackpressureStrategy::EvictLeastProgress,
        BackpressureStrategy::Sample { rate: 0.5 },
    ] {
        let mut losses = 0;
        for (name, pattern, _) in patterns() {
            for seed in 1..=3 {
                let setup = Setup {
                    max_runs: 12,
                    backpressure: backpressure.clone(),
                    ..Setup::new(SelectionStrategy::SkipTillAnyMatch, 2)
                };
                losses += compare(name, &pattern, &setup, seed * 0x51_7CC1, 1_000).losses;
            }
        }
        assert!(losses > 0, "{backpressure:?}: the cap never bit");
    }
}

#[test]
fn each_partition_switches_on_its_own_runs() {
    // The variable replaces the automatic default, which is what this test
    // is about.
    if std::env::var_os("VARPULIS_SASE_INDEX").is_some() {
        return;
    }
    let pattern = SasePattern::Seq(vec![step("A", "a", None), step("B", "b", Some(key_eq()))]);
    let mut engine = SaseEngine::new(pattern)
        .with_max_runs(1_000_000)
        .with_partition_by("part".into());
    let a = |part: &str, k: usize| {
        Event::new("A")
            .with_field("part", Value::Str(part.into()))
            .with_field("key", Value::Int(k as i64))
    };
    let b = |part: &str, k: usize| {
        Event::new("B")
            .with_field("part", Value::Str(part.into()))
            .with_field("key", Value::Int(k as i64))
    };

    // Many small partitions: none reaches the threshold.
    for p in 0..50 {
        for k in 0..2 {
            let _ = engine.process(&a(&format!("small{p}"), k));
        }
        let _ = engine.process(&b(&format!("small{p}"), 1_000_000));
    }
    assert_eq!(
        engine.parked_run_count(),
        0,
        "no small partition is indexed"
    );

    for k in 0..INDEX_ON_RUNS - 1 {
        let _ = engine.process(&a("big", k));
    }
    let _ = engine.process(&b("big", 1_000_000));
    assert_eq!(engine.parked_run_count(), 0, "below the threshold");

    let _ = engine.process(&a("big", INDEX_ON_RUNS - 1));
    let _ = engine.process(&b("big", 1_000_000));
    assert_eq!(
        engine.parked_run_count(),
        INDEX_ON_RUNS,
        "the big partition is indexed, the small ones are not"
    );

    // Completing runs brings the count down; the index lets go only under
    // the lower threshold.
    for k in 0..INDEX_ON_RUNS - INDEX_OFF_RUNS {
        assert_eq!(engine.process(&b("big", k)).len(), 1);
    }
    let _ = engine.process(&b("big", 1_000_000));
    assert_eq!(engine.parked_run_count(), INDEX_OFF_RUNS);
    assert_eq!(engine.process(&b("big", INDEX_ON_RUNS - 1)).len(), 1);
    let _ = engine.process(&b("big", 1_000_000));
    assert_eq!(engine.parked_run_count(), 0, "under the lower threshold");
    assert_eq!(engine.stats().active_runs, 100 + INDEX_OFF_RUNS - 1);
}

/// Evicting at capacity picks the victim among the partition's parked runs
/// where they are: the index stays whole, and the new run is filed in it.
#[test]
fn eviction_keeps_the_partition_index() {
    let pattern = SasePattern::Seq(vec![step("A", "a", None), step("B", "b", Some(key_eq()))]);
    let a = |k: i64| {
        Event::new("A")
            .with_field("part", Value::Int(7))
            .with_field("key", Value::Int(k))
    };
    let b = |k: i64| {
        Event::new("B")
            .with_field("part", Value::Int(7))
            .with_field("key", Value::Int(k))
    };
    for backpressure in [
        BackpressureStrategy::EvictOldest,
        BackpressureStrategy::EvictLeastProgress,
    ] {
        let mut engine = SaseEngine::new(pattern.clone())
            .with_max_runs(4)
            .with_backpressure(backpressure.clone())
            .with_partition_by("part".into())
            .with_predicate_index(true);
        for k in 0..6 {
            let _ = engine.process(&a(k));
            // A partition files its runs from its next event on.
            assert!(engine.process(&b(-1)).is_empty());
            assert_eq!(
                engine.parked_run_count(),
                (k as usize + 1).min(4),
                "{backpressure:?}"
            );
        }
        assert_eq!(engine.total_runs_evicted, 2);
        // The two oldest went.
        assert!(engine.process(&b(1)).is_empty());
        assert_eq!(engine.process(&b(2)).len(), 1);
        assert_eq!(engine.process(&b(5)).len(), 1);
    }
}

/// The matches of one partition come out in the order their runs were
/// created, whether the runs were visited (in an order the swap-removes of
/// earlier completions reshuffled) or woken from the index.
#[test]
fn matches_of_a_partition_come_out_in_creation_order() {
    let pattern = SasePattern::Seq(vec![step("A", "a", None), step("B", "b", Some(key_eq()))]);
    let ev = |t: &str, id: i64, key: i64| {
        Event::new(t)
            .with_field("id", Value::Int(id))
            .with_field("part", Value::Int(0))
            .with_field("key", Value::Int(key))
    };
    for index in [false, true] {
        let mut engine = SaseEngine::new(pattern.clone())
            .with_max_runs(100)
            .with_partition_by("part".into())
            .with_predicate_index(index);
        for id in 0..16 {
            let _ = engine.process(&ev("A", id, i64::from(id % 3 == 0)));
        }
        // Completes the runs of key 1, out of the middle of the list.
        assert_eq!(engine.process(&ev("B", 100, 1)).len(), 6);
        for id in 16..20 {
            let _ = engine.process(&ev("A", id, 0));
        }
        let ids: Vec<i64> = engine
            .process(&ev("B", 101, 0))
            .iter()
            .map(|m| match m.captured["a"].get("id") {
                Some(Value::Int(id)) => *id,
                other => panic!("{other:?}"),
            })
            .collect();
        let expected: Vec<i64> = (0..16).filter(|id| id % 3 != 0).chain(16..20).collect();
        assert_eq!(ids, expected, "index {index}");
    }
}

// ---------------------------------------------------------------------------
// Processing time
// ---------------------------------------------------------------------------
//
// As in `predicate_index_differential.rs`: bursts much shorter than the
// window, separated by pauses longer than it and mostly shorter than the
// cleanup interval, so a parked run must leave at the pass of its partition.

const PT_WINDOW: Duration = Duration::from_millis(40);
const PT_PAUSE: Duration = Duration::from_millis(60);

fn within_pt_window(pattern: SasePattern) -> SasePattern {
    match pattern {
        SasePattern::Within(inner, _) => SasePattern::Within(inner, PT_WINDOW),
        other => SasePattern::Within(Box::new(other), PT_WINDOW),
    }
}

fn compare_processing_time(
    name: &str,
    pattern: &SasePattern,
    setup: &Setup,
    seed: u64,
    bursts: usize,
) -> Outcome {
    let make = |index| setup.engine(pattern, index).with_processing_time();
    let mut plain = make(Some(false));
    let mut indexed = make(Some(true));
    let mut auto = make(None);
    let mut rng = Rng(seed);
    let mut id = 0;
    let mut outcome = Outcome {
        matches: 0,
        most_parked: 0,
        losses: 0,
    };
    for burst in 0..bursts {
        if burst > 0 {
            std::thread::sleep(PT_PAUSE);
        }
        for _ in 0..=rng.below(60) {
            let event = random_event(&mut rng, id, Utc::now(), setup.partitions);
            let expected: Vec<String> = plain.process(&event).iter().map(describe).collect();
            let actual: Vec<String> = indexed.process(&event).iter().map(describe).collect();
            let automatic: Vec<String> = auto.process(&event).iter().map(describe).collect();
            assert_eq!(
                actual, expected,
                "{name} (seed {seed}): differ at event {id}"
            );
            assert_eq!(
                automatic, expected,
                "{name} (seed {seed}): auto at event {id}"
            );
            assert_eq!(state(&indexed), state(&plain), "{name}: after event {id}");
            assert_eq!(
                state(&auto),
                state(&plain),
                "{name}: auto, after event {id}"
            );
            outcome.matches += expected.len();
            outcome.most_parked = outcome.most_parked.max(indexed.parked_run_count());
            id += 1;
        }
    }
    outcome.losses = plain.total_runs_dropped + plain.total_runs_evicted;
    outcome
}

#[test]
fn partition_index_never_changes_matches_under_processing_time() {
    std::thread::scope(|scope| {
        for (name, pattern, must_engage) in patterns() {
            let pattern = within_pt_window(pattern);
            scope.spawn(move || {
                let mut matches = 0;
                let mut most_parked = 0;
                let mut losses = 0;
                let setups = [
                    Setup::new(SelectionStrategy::SkipTillAnyMatch, 2),
                    Setup::new(SelectionStrategy::SkipTillNextMatch, 2),
                    Setup {
                        max_runs: 5,
                        ..Setup::new(SelectionStrategy::SkipTillAnyMatch, 2)
                    },
                    Setup {
                        max_runs: 5,
                        backpressure: BackpressureStrategy::EvictOldest,
                        ..Setup::new(SelectionStrategy::SkipTillAnyMatch, 2)
                    },
                ];
                for (n, setup) in setups.iter().enumerate() {
                    let outcome = compare_processing_time(
                        name,
                        &pattern,
                        setup,
                        (n as u64 + 1) * 0x2F_0B3D,
                        6,
                    );
                    matches += outcome.matches;
                    most_parked = most_parked.max(outcome.most_parked);
                    losses += outcome.losses;
                }
                assert!(matches > 0, "{name}: no match, the comparison is vacuous");
                assert!(losses > 0, "{name}: the cap never bit");
                if must_engage {
                    assert!(most_parked > 0, "{name}: the index never filed a run");
                }
            });
        }
    });
}

/// Parked runs of a partition whose window has passed must not hold the
/// place of a fresh one, under processing time.
#[test]
fn expired_parked_runs_do_not_hold_a_partition_under_processing_time() {
    let pattern = SasePattern::Within(
        Box::new(SasePattern::Seq(vec![
            step("A", "a", None),
            step("B", "b", Some(key_eq())),
        ])),
        Duration::from_millis(30),
    );
    let ev = |t: &str, k: i64| {
        Event::new(t)
            .with_field("part", Value::Int(1))
            .with_field("key", Value::Int(k))
    };
    for backpressure in [
        BackpressureStrategy::Drop,
        BackpressureStrategy::EvictOldest,
    ] {
        let outcome = |index: bool| {
            let mut engine = SaseEngine::new(pattern.clone())
                .with_processing_time()
                .with_max_runs(4)
                .with_backpressure(backpressure.clone())
                .with_partition_by("part".into())
                .with_predicate_index(index);
            for k in 0..4 {
                let _ = engine.process(&ev("A", k));
            }
            // Past the window, short of the cleanup interval.
            std::thread::sleep(Duration::from_millis(60));
            let _ = engine.process(&ev("A", 9));
            let matches = engine.process(&ev("B", 9)).len();
            (
                matches,
                engine.total_runs_dropped,
                engine.total_runs_evicted,
                engine.stats().active_runs,
            )
        };
        assert_eq!(outcome(false), (1, 0, 0, 0), "{backpressure:?}, index off");
        assert_eq!(outcome(true), (1, 0, 0, 0), "{backpressure:?}, index on");
    }
}
