//! IDX-03: cost per event with `partition by`, as the runs waiting in each
//! partition grow.
//!
//! `N` runs wait in `A as a -> B where ref == a.ref`, partitioned by `part`,
//! spread over `P` partitions (`N / P` runs each). Then `M` pairs arrive: a
//! `B` that completes exactly one run, and the `A` that re-arms it, so `N`
//! stays put. The time reported is per `B` + `A` pair.
//!
//! ```text
//! cargo run --release -p varpulis-sase --example partition_index_bench -- P N [M]
//! ```
//!
//! The index follows `VARPULIS_SASE_INDEX` (unset: automatic, `1`: always,
//! `0`: never). `M` is fixed by the arguments, so the instruction count of
//! two builds can be compared with `perf stat -e instructions:u`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use varpulis_core::{Event, Value};
use varpulis_sase::{CompareOp, Predicate, SaseEngine, SasePattern};

fn pattern() -> SasePattern {
    SasePattern::Seq(vec![
        SasePattern::Event {
            event_type: "A".into(),
            predicate: None,
            alias: Some("a".into()),
        },
        SasePattern::Event {
            event_type: "B".into(),
            predicate: Some(Predicate::CompareRef {
                field: "ref".into(),
                op: CompareOp::Eq,
                ref_alias: "a".into(),
                ref_field: "ref".into(),
            }),
            alias: Some("b".into()),
        },
    ])
}

fn event(event_type: &str, r: i64, partitions: i64) -> Arc<Event> {
    Arc::new(
        Event::new(event_type)
            .with_field("part", Value::Int(r % partitions))
            .with_field("ref", Value::Int(r)),
    )
}

fn main() {
    let mut args = std::env::args()
        .skip(1)
        .map(|s| s.parse::<i64>().expect("integer"));
    let partitions = args.next().unwrap_or(4);
    let n = args.next().unwrap_or(4_000);
    let m = args.next().unwrap_or(20_000);

    let mut engine = SaseEngine::new(pattern())
        .with_max_runs(usize::MAX)
        .with_partition_by("part".into());
    for r in 0..n {
        black_box(engine.process_shared(event("A", r, partitions)));
    }
    assert_eq!(engine.stats().active_runs, n as usize);

    let mut x: i64 = 42;
    let mut events = Vec::with_capacity(m as usize * 2);
    for _ in 0..m {
        x = (x * 1_103_515_245 + 12_345) % 2_147_483_648;
        let r = x % n;
        events.push(event("B", r, partitions));
        events.push(event("A", r, partitions));
    }

    let mut matches = 0usize;
    let started = Instant::now();
    for e in &events {
        matches += black_box(engine.process_shared(Arc::clone(e))).len();
    }
    let ns = started.elapsed().as_nanos() as f64 / m as f64;
    assert_eq!(matches, m as usize);
    assert_eq!(engine.stats().active_runs, n as usize);
    println!(
        "partitions={partitions} n={n} runs_per_partition={} m={m} ns_per_pair={ns:.0} \
         index={}",
        n / partitions,
        std::env::var("VARPULIS_SASE_INDEX").unwrap_or_else(|_| "auto".into()),
    );
}
