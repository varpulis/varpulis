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
            // Both alternatives start with the same unpredicated `C`, so a run
            // at the `or` forks a copy per alternative. The runs reaching the
            // `or` come out of the index in its order, not the visited list's:
            // the copies must still be numbered, admitted and emitted in the
            // order of the runs they were forked from.
            "or-forking",
            SasePattern::Within(
                Box::new(SasePattern::Seq(vec![
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
                ])),
                Duration::from_secs(8),
            ),
            true,
        ),
        (
            // Parked while waiting for `B`, then visited while the negated
            // step in the middle forbids `C`, until `D` completes the run.
            "negation-in-the-middle",
            SasePattern::Within(
                Box::new(SasePattern::Seq(vec![
                    a(),
                    step("B", "b", Some(key_eq())),
                    SasePattern::Not(Box::new(step("C", "c", Some(key_eq())))),
                    step("B", "b2", Some(key_eq())),
                ])),
                Duration::from_secs(5),
            ),
            true,
        ),
        (
            // A closure, then an absence in the middle.
            "kleene-then-negation",
            SasePattern::Within(
                Box::new(SasePattern::Seq(vec![
                    a(),
                    SasePattern::KleenePlus(Box::new(step("B", "bs", Some(key_eq())))),
                    SasePattern::Not(Box::new(step("C", "c", Some(key_eq())))),
                    step("B", "b2", Some(cref("price", CompareOp::Gt, "a", "price"))),
                ])),
                Duration::from_secs(5),
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

// ---------------------------------------------------------------------------
// Processing time
// ---------------------------------------------------------------------------
//
// Under processing time a listed run whose deadline has passed leaves when an
// event's pass reaches it; a parked run must leave at the same point, not when
// the throttled cleanup next runs. The engine reads the wall clock, so these
// tests sleep. The events come in bursts much shorter than the window,
// separated by pauses longer than it: no deadline falls inside a burst, and
// every run of a burst has expired when the next one starts. The three engines
// see each event one after the other, so they agree on what has expired. The
// pauses are shorter than the cleanup interval (100 ms) more often than not,
// which leaves the expiry to the pass itself.

/// Window of the processing-time patterns.
const PT_WINDOW: Duration = Duration::from_millis(40);
/// Pause between two bursts.
const PT_PAUSE: Duration = Duration::from_millis(60);

fn processing_time_engine(
    pattern: &SasePattern,
    strategy: SelectionStrategy,
    max_runs: usize,
    backpressure: &BackpressureStrategy,
    index: Option<bool>,
) -> SaseEngine {
    let engine = engine(pattern, strategy, max_runs, backpressure, index);
    engine.with_processing_time()
}

/// `pattern` with its window set to [`PT_WINDOW`].
fn within_pt_window(pattern: SasePattern) -> SasePattern {
    match pattern {
        SasePattern::Within(inner, _) => SasePattern::Within(inner, PT_WINDOW),
        other => SasePattern::Within(Box::new(other), PT_WINDOW),
    }
}

/// Run the three engines over bursts of a random stream under processing
/// time. Returns the number of matches and the most runs the index held.
fn compare_processing_time(
    name: &str,
    pattern: &SasePattern,
    strategy: SelectionStrategy,
    max_runs: usize,
    backpressure: &BackpressureStrategy,
    seed: u64,
    bursts: usize,
) -> (usize, usize, u64) {
    let mut plain = processing_time_engine(pattern, strategy, max_runs, backpressure, Some(false));
    let mut indexed = processing_time_engine(pattern, strategy, max_runs, backpressure, Some(true));
    let mut auto = processing_time_engine(pattern, strategy, max_runs, backpressure, None);
    let mut rng = Rng(seed);
    let mut id = 0;
    let mut matches = 0;
    let mut most_parked = 0;

    for burst in 0..bursts {
        if burst > 0 {
            std::thread::sleep(PT_PAUSE);
        }
        for _ in 0..=rng.below(60) {
            let event = random_event(&mut rng, id, Utc::now());
            let expected: Vec<String> = plain.process(&event).iter().map(describe).collect();
            let actual: Vec<String> = indexed.process(&event).iter().map(describe).collect();
            let automatic: Vec<String> = auto.process(&event).iter().map(describe).collect();
            assert_eq!(
                actual, expected,
                "{name} ({strategy:?}, {backpressure:?}, seed {seed}): matches differ at event {id}"
            );
            assert_eq!(
                automatic, expected,
                "{name} ({strategy:?}, {backpressure:?}, seed {seed}): automatic mode differs at event {id}"
            );
            for (mode, other) in [("indexed", &indexed), ("auto", &auto)] {
                assert_eq!(
                    (
                        other.stats().active_runs,
                        other.total_runs_dropped,
                        other.total_runs_evicted
                    ),
                    (
                        plain.stats().active_runs,
                        plain.total_runs_dropped,
                        plain.total_runs_evicted
                    ),
                    "{name} ({strategy:?}, {backpressure:?}, seed {seed}): {mode} \
                     (active, dropped, evicted) differ after event {id}"
                );
            }
            matches += expected.len();
            most_parked = most_parked.max(indexed.parked_run_count());
            id += 1;
        }
    }
    (
        matches,
        most_parked,
        plain.total_runs_dropped + plain.total_runs_evicted,
    )
}

#[test]
fn index_never_changes_matches_under_processing_time() {
    // One thread per pattern: the tests mostly sleep.
    std::thread::scope(|scope| {
        for (name, pattern, must_engage) in patterns() {
            let pattern = within_pt_window(pattern);
            scope.spawn(move || {
                let mut matches = 0;
                let mut most_parked = 0;
                let mut losses = 0;
                let runs = [
                    (
                        SelectionStrategy::SkipTillAnyMatch,
                        1_000_000,
                        BackpressureStrategy::Drop,
                    ),
                    (
                        SelectionStrategy::SkipTillNextMatch,
                        1_000_000,
                        BackpressureStrategy::Drop,
                    ),
                    (
                        SelectionStrategy::SkipTillAnyMatch,
                        6,
                        BackpressureStrategy::Drop,
                    ),
                    (
                        SelectionStrategy::SkipTillAnyMatch,
                        6,
                        BackpressureStrategy::EvictOldest,
                    ),
                    (
                        SelectionStrategy::SkipTillAnyMatch,
                        6,
                        BackpressureStrategy::EvictLeastProgress,
                    ),
                ];
                for (n, (strategy, max_runs, backpressure)) in runs.iter().enumerate() {
                    let (m, parked, lost) = compare_processing_time(
                        name,
                        &pattern,
                        *strategy,
                        *max_runs,
                        backpressure,
                        (n as u64 + 1) * 0x2F_0B3D,
                        6,
                    );
                    matches += m;
                    most_parked = most_parked.max(parked);
                    losses += lost;
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

/// The reported case: four runs parked at capacity whose window has passed
/// must not hold the place of a fresh one.
#[test]
fn expired_parked_runs_do_not_hold_capacity_under_processing_time() {
    let pattern = SasePattern::Within(
        Box::new(SasePattern::Seq(vec![
            step("A", "a", None),
            step("B", "b", Some(key_eq())),
        ])),
        Duration::from_millis(30),
    );
    let a = |k: i64| Event::new("A").with_field("key", Value::Int(k));
    let b = |k: i64| Event::new("B").with_field("key", Value::Int(k));

    for backpressure in [
        BackpressureStrategy::Drop,
        BackpressureStrategy::EvictOldest,
    ] {
        let outcome = |index: bool| {
            let mut engine = SaseEngine::new(pattern.clone())
                .with_processing_time()
                .with_max_runs(4)
                .with_backpressure(backpressure.clone())
                .with_predicate_index(index);
            for k in 0..4 {
                let _ = engine.process(&a(k));
            }
            // Past the window, short of the cleanup interval.
            std::thread::sleep(Duration::from_millis(60));
            let _ = engine.process(&a(9));
            let matches = engine.process(&b(9)).len();
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

/// Evicting at capacity picks the victim among the parked runs where they
/// are: the index stays whole, and the new run is filed in it.
#[test]
fn eviction_keeps_the_index() {
    let pattern = SasePattern::Seq(vec![step("A", "a", None), step("B", "b", Some(key_eq()))]);
    let a = |k: i64| Event::new("A").with_field("key", Value::Int(k));
    let b = |k: i64| Event::new("B").with_field("key", Value::Int(k));
    for backpressure in [
        BackpressureStrategy::EvictOldest,
        BackpressureStrategy::EvictLeastProgress,
    ] {
        let mut engine = SaseEngine::new(pattern.clone())
            .with_max_runs(4)
            .with_backpressure(backpressure.clone())
            .with_predicate_index(true);
        for k in 0..6 {
            let _ = engine.process(&a(k));
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
