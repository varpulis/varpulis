//! IDX-02: cost per event as the number of waiting runs grows, with and
//! without the run index.
//!
//! `N` runs wait in `A as a -> B where <pred>`, one per `A`; then `M` `B`
//! events arrive:
//!
//! - `eq`:    `B where key == a.key`, keys `0..N`;
//! - `range`: `B where price >= a.lo and price < a.hi`, intervals `[i, i+1)`;
//! - `eq2d`:  `B where key == a.key and price >= a.lo and price < a.hi`,
//!   key `i / 100`, interval `[i % 100, i % 100 + 1)`.
//!
//! In `miss` mode each `B` matches no run; in `hit` mode each `B` completes
//! exactly one run, and an `A` re-arms it so `N` stays put: the time reported
//! is per `B` + `A` pair.
//!
//! ```text
//! cargo run --release -p varpulis-sase --example predicate_index_bench -- \
//!     [N,N,...] [max N without index]
//! cargo run --release -p varpulis-sase --example predicate_index_bench -- \
//!     mem N on|off [eq|range|eq2d]
//! ```
//!
//! `mem` reports the resident memory one waiting run costs, its captured `A`
//! included, measured around the setup alone.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use varpulis_core::{Event, Value};
use varpulis_sase::{CompareOp, Predicate, SaseEngine, SasePattern};

#[derive(Clone, Copy, Debug)]
enum Workload {
    Eq,
    Range,
    Eq2d,
}

fn cref(field: &str, op: CompareOp, ref_field: &str) -> Predicate {
    Predicate::CompareRef {
        field: field.into(),
        op,
        ref_alias: "a".into(),
        ref_field: ref_field.into(),
    }
}

fn pattern(workload: Workload) -> SasePattern {
    let key = cref("key", CompareOp::Eq, "key");
    let interval = Predicate::And(
        Box::new(cref("price", CompareOp::Ge, "lo")),
        Box::new(cref("price", CompareOp::Lt, "hi")),
    );
    let predicate = match workload {
        Workload::Eq => key,
        Workload::Range => interval,
        Workload::Eq2d => Predicate::And(Box::new(key), Box::new(interval)),
    };
    SasePattern::Seq(vec![
        SasePattern::Event {
            event_type: "A".into(),
            predicate: None,
            alias: Some("a".into()),
        },
        SasePattern::Event {
            event_type: "B".into(),
            predicate: Some(predicate),
            alias: Some("b".into()),
        },
    ])
}

/// The `A` that opens run `i`.
fn opener(workload: Workload, i: i64) -> Arc<Event> {
    let event = Event::new("A");
    Arc::new(match workload {
        Workload::Eq => event.with_field("key", Value::Int(i)),
        Workload::Range => event
            .with_field("lo", Value::Float(i as f64))
            .with_field("hi", Value::Float(i as f64 + 1.0)),
        Workload::Eq2d => {
            let lo = (i % 100) as f64;
            event
                .with_field("key", Value::Int(i / 100))
                .with_field("lo", Value::Float(lo))
                .with_field("hi", Value::Float(lo + 1.0))
        }
    })
}

/// A `B` that run `i` waits for.
fn probe(workload: Workload, i: i64) -> Arc<Event> {
    let event = Event::new("B");
    Arc::new(match workload {
        Workload::Eq => event.with_field("key", Value::Int(i)),
        Workload::Range => event.with_field("price", Value::Float(i as f64 + 0.5)),
        Workload::Eq2d => event
            .with_field("key", Value::Int(i / 100))
            .with_field("price", Value::Float((i % 100) as f64 + 0.5)),
    })
}

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<f64>().ok())
        .map_or(0.0, |pages| pages * 4096.0 / 1_048_576.0)
}

fn run(workload: Workload, n: i64, hit: bool, indexed: bool) {
    let mut engine = SaseEngine::new(pattern(workload))
        .with_max_runs(usize::MAX)
        .with_predicate_index(indexed);

    let openers: Vec<_> = (0..n).map(|i| opener(workload, i)).collect();
    let rss_before = rss_mb();
    let started = Instant::now();
    for event in &openers {
        black_box(engine.process_shared(Arc::clone(event)));
    }
    let setup_ns = started.elapsed().as_nanos() as f64 / n as f64;
    drop(openers);
    assert_eq!(engine.stats().active_runs, n as usize);

    // Fewer events when every one visits every run.
    let m: i64 = if indexed {
        200_000
    } else {
        (20_000_000 / n).clamp(200, 200_000)
    };
    let mut x: i64 = 42;
    let mut events = Vec::with_capacity(m as usize * 2);
    for _ in 0..m {
        x = (x * 1_103_515_245 + 12_345) % 2_147_483_648;
        let k = x % n;
        if hit {
            events.push(probe(workload, k));
            events.push(opener(workload, k));
        } else {
            // Past every run's key or interval.
            events.push(probe(workload, n + k));
        }
    }

    let mut matches = 0usize;
    let started = Instant::now();
    for event in &events {
        matches += black_box(engine.process_shared(Arc::clone(event))).len();
    }
    let ns = started.elapsed().as_nanos() as f64 / m as f64;
    assert_eq!(matches, if hit { m as usize } else { 0 });
    assert_eq!(engine.stats().active_runs, n as usize);

    println!(
        "workload={workload:?} mode={} n={n} index={} m={m} ns_per_event={ns:.0} \
         setup_ns_per_run={setup_ns:.0} parked={} rss_delta_mb={:.0}",
        if hit { "hit" } else { "miss" },
        if indexed { "on" } else { "off" },
        engine.parked_run_count(),
        rss_mb() - rss_before,
    );
}

/// Resident memory per waiting run: the run, its captured event and the
/// index entries, measured around the setup alone.
fn mem(workload: Workload, n: i64, indexed: bool) {
    let before = rss_mb();
    let mut engine = SaseEngine::new(pattern(workload))
        .with_max_runs(usize::MAX)
        .with_predicate_index(indexed);
    for i in 0..n {
        black_box(engine.process_shared(opener(workload, i)));
    }
    let after = rss_mb();
    assert_eq!(engine.stats().active_runs, n as usize);
    println!(
        "workload={workload:?} n={n} index={} bytes_per_run={:.0}",
        if indexed { "on" } else { "off" },
        (after - before) * 1_048_576.0 / n as f64,
    );
    black_box(&engine);
}

fn main() {
    let mut args = std::env::args().skip(1);
    if std::env::args().nth(1).as_deref() == Some("mem") {
        let n: i64 = std::env::args()
            .nth(2)
            .map_or(100_000, |s| s.parse().expect("N"));
        let indexed = std::env::args().nth(3).as_deref() != Some("off");
        // One workload per process: an engine dropped earlier leaves its
        // pages resident, and the next one would reuse them unseen.
        let workload = match std::env::args().nth(4).as_deref() {
            Some("range") => Workload::Range,
            Some("eq2d") => Workload::Eq2d,
            _ => Workload::Eq,
        };
        mem(workload, n, indexed);
        return;
    }
    let sizes: Vec<i64> = args
        .next()
        .unwrap_or_else(|| "1,10,100,1000,10000,100000".into())
        .split(',')
        .map(|s| s.parse().expect("N"))
        .collect();
    let max_linear: i64 = args.next().map_or(100_000, |s| s.parse().expect("max N"));

    for workload in [Workload::Eq, Workload::Range, Workload::Eq2d] {
        for hit in [false, true] {
            for &n in &sizes {
                run(workload, n, hit, true);
                if n <= max_linear {
                    run(workload, n, hit, false);
                }
            }
        }
    }
}
