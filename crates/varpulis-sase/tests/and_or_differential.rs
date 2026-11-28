//! `and` and `or` against a model written by hand.
//!
//! Random streams, under the default selection (every event that can open a
//! run opens one) and event time. The model states what each pattern means
//! without the engine's states, waiting lists or indexes:
//!
//! - `A as a and B where key == a.key as b within W`: every A and every B
//!   opens a run, which completes with the first event of the other type,
//!   after it, with the same key and at most W later.
//! - `A as a -> (B where key == a.key as b or C where key == a.key as c)
//!   within W`: every A opens a run, which completes with the first B or C
//!   after it with the same key and at most W later.
//!
//! Each event must complete exactly the runs the model says, with the index
//! on and off, partitioned by the key or not.

use std::time::Duration;

use chrono::{DateTime, Utc};
use varpulis_core::{Event, Value};
use varpulis_sase::{CompareOp, MatchResult, Predicate, SaseEngine, SasePattern};

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

const WITHIN_SECS: i64 = 10;

struct Ev {
    event_type: &'static str,
    key: i64,
    secs: i64,
}

fn stream(seed: u64, n: usize) -> Vec<Ev> {
    let mut rng = Rng(seed);
    let mut secs = 0;
    (0..n)
        .map(|_| {
            secs += rng.below(4) as i64;
            Ev {
                event_type: ["A", "A", "B", "B", "C"][rng.below(5) as usize],
                key: rng.below(3) as i64,
                secs,
            }
        })
        .collect()
}

fn to_event(id: usize, e: &Ev) -> Event {
    let mut event = Event::new(e.event_type)
        .with_field("id", Value::Int(id as i64))
        .with_field("key", Value::Int(e.key));
    event.timestamp = DateTime::<Utc>::from_timestamp(1_700_000_000 + e.secs, 0).expect("ts");
    event
}

fn step(event_type: &str, alias: &str, predicate: Option<Predicate>) -> SasePattern {
    SasePattern::Event {
        event_type: event_type.into(),
        predicate,
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

fn within(p: SasePattern) -> SasePattern {
    SasePattern::Within(Box::new(p), Duration::from_secs(WITHIN_SECS as u64))
}

fn and_pattern() -> SasePattern {
    within(SasePattern::And(
        Box::new(step("A", "a", None)),
        Box::new(step("B", "b", Some(same_key("a")))),
    ))
}

fn or_pattern() -> SasePattern {
    within(SasePattern::Seq(vec![
        step("A", "a", None),
        SasePattern::Or(
            Box::new(step("B", "b", Some(same_key("a")))),
            Box::new(step("C", "c", Some(same_key("a")))),
        ),
    ]))
}

/// The first event after `opener` of one of `partners` with its key, at
/// most the window later.
fn first_partner(events: &[Ev], opener: usize, partners: &[&str]) -> Option<usize> {
    let o = &events[opener];
    events
        .iter()
        .enumerate()
        .skip(opener + 1)
        .take_while(|(_, e)| e.secs - o.secs <= WITHIN_SECS)
        .find(|(_, e)| partners.contains(&e.event_type) && e.key == o.key)
        .map(|(j, _)| j)
}

/// The model: for each event, the matches it completes, as `alias=id`.
fn model_and(events: &[Ev]) -> Vec<Vec<String>> {
    let mut out = vec![Vec::new(); events.len()];
    for (i, e) in events.iter().enumerate() {
        let (partners, own, other): (&[&str], &str, &str) = match e.event_type {
            "A" => (&["B"], "a", "b"),
            "B" => (&["A"], "b", "a"),
            _ => continue,
        };
        if let Some(j) = first_partner(events, i, partners) {
            let mut m = [format!("{own}={i}"), format!("{other}={j}")];
            m.sort();
            out[j].push(m.join(","));
        }
    }
    out
}

fn model_or(events: &[Ev]) -> Vec<Vec<String>> {
    let mut out = vec![Vec::new(); events.len()];
    for (i, e) in events.iter().enumerate() {
        if e.event_type != "A" {
            continue;
        }
        if let Some(j) = first_partner(events, i, &["B", "C"]) {
            let alias = if events[j].event_type == "B" {
                "b"
            } else {
                "c"
            };
            out[j].push(format!("a={i},{alias}={j}"));
        }
    }
    out
}

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

fn check(name: &str, pattern: fn() -> SasePattern, model: fn(&[Ev]) -> Vec<Vec<String>>) {
    let mut total = 0;
    for seed in 1..=12u64 {
        let events = stream(seed * 0x9E37_79B9, 400);
        let expected = model(&events);
        total += expected.iter().map(Vec::len).sum::<usize>();
        for partitioned in [false, true] {
            for index in [false, true] {
                let mut engine = SaseEngine::new(pattern())
                    .with_event_time()
                    .with_predicate_index(index);
                if partitioned {
                    engine = engine.with_partition_by("key".into());
                }
                for (id, e) in events.iter().enumerate() {
                    let mut got: Vec<String> = engine
                        .process(&to_event(id, e))
                        .iter()
                        .map(describe)
                        .collect();
                    let mut want = expected[id].clone();
                    if partitioned {
                        // A partition's runs are visited in no promised order.
                        got.sort();
                        want.sort();
                    }
                    assert_eq!(
                        got, want,
                        "{name} (seed {seed}, partitioned {partitioned}, index {index}): \
                         event {id} ({} key {} at {}s)",
                        e.event_type, e.key, e.secs
                    );
                }
            }
        }
    }
    assert!(total > 100, "{name}: too few matches ({total}) to compare");
}

#[test]
fn and_matches_the_model() {
    check("and", and_pattern, model_and);
}

#[test]
fn or_matches_the_model() {
    check("or", or_pattern, model_or);
}
