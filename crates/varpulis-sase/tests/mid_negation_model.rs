//! A negated step in the middle of a pattern, against a model written by hand.
//!
//! `A -> NOT B where key == a.key -> C where key == a.key within W`: every A
//! opens a run; a B with the run's key arriving after the A and before the C
//! the run takes invalidates it; the first C with its key completes it, if
//! the C's stamp is within W of the A's. The model is that sentence in code.
//! The engine runs the same random streams with the run index always on,
//! never on and automatic, under both selection strategies, and with
//! `partition by key` (where the negated step needs no `where`); every event
//! must give the model's matches in the model's order.

use std::time::Duration;

use chrono::{DateTime, Utc};
use varpulis_core::{Event, Value};
use varpulis_sase::{
    CompareOp, MatchResult, Predicate, SaseEngine, SasePattern, SelectionStrategy,
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

const WITHIN_MS: i64 = 4_000;

fn key_eq() -> Predicate {
    Predicate::CompareRef {
        field: "key".into(),
        op: CompareOp::Eq,
        ref_alias: "a".into(),
        ref_field: "key".into(),
    }
}

fn step(event_type: &str, alias: &str, predicate: Option<Predicate>) -> SasePattern {
    SasePattern::Event {
        event_type: event_type.into(),
        predicate,
        alias: Some(alias.into()),
    }
}

fn not(event_type: &str, keyed: bool) -> SasePattern {
    SasePattern::Not(Box::new(step(event_type, event_type, keyed.then(key_eq))))
}

/// The pattern: A, then no event of the `forbidden` types with A's key,
/// then a C with A's key. `keyed` puts `key == a.key` on the negated
/// steps; without it a partition by key has to scope them.
///
/// When C itself is forbidden, the step after the absence also asks for
/// `flag == 1`: "the next C with flag 1, with no other C in between". The
/// C that takes the step fits the forbidden filter too, and closes the gap
/// rather than falling in it.
fn pattern(forbidden: &[&str], keyed: bool) -> SasePattern {
    let mut steps = vec![step("A", "a", None)];
    steps.extend(forbidden.iter().map(|f| not(f, keyed)));
    let next = if overlaps(forbidden) {
        Predicate::And(
            Box::new(key_eq()),
            Box::new(Predicate::Compare {
                field: "flag".into(),
                op: CompareOp::Eq,
                value: Value::Int(1),
            }),
        )
    } else {
        key_eq()
    };
    steps.push(step("C", "c", Some(next)));
    SasePattern::Within(
        Box::new(SasePattern::Seq(steps)),
        Duration::from_millis(WITHIN_MS as u64),
    )
}

struct Open {
    a: i64,
    key: i64,
    at: DateTime<Utc>,
}

/// The hand-written model: the runs still open, oldest first.
#[derive(Default)]
struct Model {
    open: Vec<Open>,
}

impl Model {
    /// The `(a, c)` pairs `event` completes.
    fn feed(&mut self, forbidden: &[&str], event: &Event) -> Vec<(i64, i64)> {
        let (id, key) = (int(event, "id"), int(event, "key"));
        let event_type = &*event.event_type;
        // Runs past their window are gone before the event is judged.
        self.open
            .retain(|r| event.timestamp - r.at <= chrono::Duration::milliseconds(WITHIN_MS));
        let takes_step = event_type == "C" && (!overlaps(forbidden) || int(event, "flag") == 1);
        let mut out = Vec::new();
        self.open.retain(|r| {
            if r.key != key {
                true
            } else if takes_step {
                out.push((r.a, id));
                false
            } else {
                !forbidden.contains(&event_type)
            }
        });
        if event_type == "A" {
            self.open.push(Open {
                a: id,
                key,
                at: event.timestamp,
            });
        }
        out
    }
}

/// Whether the step after the absence is of a forbidden type.
fn overlaps(forbidden: &[&str]) -> bool {
    forbidden.contains(&"C")
}

fn int(event: &Event, field: &str) -> i64 {
    match event.get(field) {
        Some(Value::Int(n)) => *n,
        other => panic!("{field}: {other:?}"),
    }
}

fn pair(m: &MatchResult) -> (i64, i64) {
    (int(&m.captured["a"], "id"), int(&m.captured["c"], "id"))
}

fn random_event(rng: &mut Rng, id: i64, at: DateTime<Utc>) -> Event {
    // As many As as Cs, and the forbidden types rarer: most runs live
    // long enough to meet a C, many do not.
    let event_type = ["A", "A", "A", "C", "C", "C", "B", "D", "E"][rng.below(9) as usize];
    let mut event = Event::new(event_type)
        .with_field("id", Value::Int(id))
        .with_field("key", Value::Int(rng.below(4) as i64))
        .with_field("flag", Value::Int(rng.below(2) as i64));
    event.timestamp = at;
    event
}

fn engine(
    pattern: &SasePattern,
    strategy: SelectionStrategy,
    index: Option<bool>,
    partitioned: bool,
) -> SaseEngine {
    let mut engine = SaseEngine::new(pattern.clone()).with_strategy(strategy);
    if partitioned {
        engine = engine.with_partition_by("key".to_string());
    }
    match index {
        Some(on) => engine.with_predicate_index(on),
        None => engine,
    }
}

fn check(forbidden: &[&str], partitioned: bool, strategy: SelectionStrategy, seed: u64) -> usize {
    let p = pattern(forbidden, !partitioned);
    let mut engines: Vec<(Option<bool>, SaseEngine)> = [Some(false), Some(true), None]
        .into_iter()
        .map(|index| (index, engine(&p, strategy, index, partitioned)))
        .collect();
    let mut model = Model::default();
    let mut rng = Rng(seed);
    let mut at = DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp");
    let mut matched = 0;
    for id in 0..2_000 {
        at += chrono::Duration::milliseconds(rng.below(700) as i64);
        let event = random_event(&mut rng, id, at);
        let expected = model.feed(forbidden, &event);
        for (index, engine) in &mut engines {
            let actual: Vec<(i64, i64)> = engine.process(&event).iter().map(pair).collect();
            assert_eq!(
                actual, expected,
                "{forbidden:?} partitioned={partitioned} {strategy:?} index={index:?} \
                 seed {seed}: event {id} {event:?}"
            );
        }
        matched += expected.len();
    }
    matched
}

#[test]
fn engine_matches_the_model() {
    for forbidden in [&["B"][..], &["B", "D"][..], &["C"][..], &["B", "C"][..]] {
        for partitioned in [false, true] {
            for strategy in [
                SelectionStrategy::SkipTillAnyMatch,
                SelectionStrategy::SkipTillNextMatch,
            ] {
                let mut matched = 0;
                for seed in 1..=6 {
                    matched += check(forbidden, partitioned, strategy, seed * 0x9E37_79B9);
                }
                assert!(matched > 100, "{forbidden:?}: only {matched} matches");
            }
        }
    }
}

/// The model itself, on a stream small enough to check by eye.
#[test]
fn the_model_reads_as_the_pattern() {
    let at = |ms: i64| DateTime::from_timestamp_millis(1_700_000_000_000 + ms).unwrap();
    let ev = |t: &str, id: i64, key: i64, ms: i64| {
        let mut e = Event::new(t)
            .with_field("id", Value::Int(id))
            .with_field("key", Value::Int(key));
        e.timestamp = at(ms);
        e
    };
    let mut model = Model::default();
    let mut engine = SaseEngine::new(pattern(&["B"], true));
    let stream = [
        ev("A", 1, 0, 0),
        ev("A", 2, 1, 10),
        ev("B", 3, 1, 20),
        ev("C", 4, 1, 30),
        ev("C", 5, 0, 40),
        ev("A", 6, 0, 50),
        ev("C", 7, 0, 5_000),
    ];
    let mut all = Vec::new();
    for e in &stream {
        let expected = model.feed(&["B"], e);
        let actual: Vec<_> = engine.process(e).iter().map(pair).collect();
        assert_eq!(actual, expected, "{e:?}");
        all.extend(expected);
    }
    // 2 was cancelled by 3; 6 had expired when 7 came.
    assert_eq!(all, [(1, 5)]);
}
