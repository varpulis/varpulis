//! Benchmarks comparing SASE+ engine vs old pattern matcher
//!
//! Run with: cargo bench -p varpulis-runtime

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use varpulis_runtime::event::Event;
use varpulis_runtime::sase::{CompareOp, Predicate, SaseEngine, SasePattern, SelectionStrategy};

/// Generate test events for benchmarking
fn generate_events(count: usize, event_types: &[&str]) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        let event_type = event_types[i % event_types.len()];
        let mut event = Event::new(event_type);
        event = event.with_field("id", i as i64);
        event = event.with_field("value", (i * 10) as i64);
        event = event.with_field("user_id", format!("user_{}", i % 100));
        events.push(event);
    }
    events
}

/// Benchmark SASE+ engine with simple sequence A -> B
fn bench_sase_simple_sequence(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_sequence");

    for size in [100, 1000, 10000].iter() {
        let events = generate_events(*size, &["A", "B", "C"]);

        group.bench_with_input(BenchmarkId::new("sase", size), size, |b, _| {
            b.iter(|| {
                // SEQ(A, B)
                let pattern = SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "A".to_string(),
                        predicate: None,
                        alias: Some("a".to_string()),
                    },
                    SasePattern::Event {
                        event_type: "B".to_string(),
                        predicate: None,
                        alias: Some("b".to_string()),
                    },
                ]);

                let mut engine =
                    SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillAnyMatch);
                let mut matches = 0;

                for event in &events {
                    for _m in engine.process(black_box(event)) {
                        matches += 1;
                    }
                }

                matches
            })
        });
    }

    group.finish();
}

/// Benchmark SASE+ engine with Kleene+ pattern A -> B+ -> C
fn bench_sase_kleene_plus(c: &mut Criterion) {
    let mut group = c.benchmark_group("kleene_plus");

    for size in [100, 1000, 5000].iter() {
        let events = generate_events(*size, &["A", "B", "B", "B", "C"]);

        group.bench_with_input(BenchmarkId::new("sase", size), size, |b, _| {
            b.iter(|| {
                // SEQ(A, B+, C)
                let pattern = SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "A".to_string(),
                        predicate: None,
                        alias: Some("a".to_string()),
                    },
                    SasePattern::KleenePlus(Box::new(SasePattern::Event {
                        event_type: "B".to_string(),
                        predicate: None,
                        alias: Some("b".to_string()),
                    })),
                    SasePattern::Event {
                        event_type: "C".to_string(),
                        predicate: None,
                        alias: Some("c".to_string()),
                    },
                ]);

                let mut engine =
                    SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillNextMatch);
                let mut matches = 0;

                for event in &events {
                    for _m in engine.process(black_box(event)) {
                        matches += 1;
                    }
                }

                matches
            })
        });
    }

    group.finish();
}

/// Benchmark SASE+ with predicates
fn bench_sase_with_predicates(c: &mut Criterion) {
    let mut group = c.benchmark_group("with_predicates");

    for size in [100, 1000, 5000].iter() {
        let events = generate_events(*size, &["Order", "Payment"]);

        group.bench_with_input(BenchmarkId::new("sase", size), size, |b, _| {
            b.iter(|| {
                // SEQ(Order where value > 500, Payment)
                let pattern = SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "Order".to_string(),
                        predicate: Some(Predicate::Compare {
                            field: "value".to_string(),
                            op: CompareOp::Gt,
                            value: varpulis_core::Value::Int(500),
                        }),
                        alias: Some("order".to_string()),
                    },
                    SasePattern::Event {
                        event_type: "Payment".to_string(),
                        predicate: None,
                        alias: Some("payment".to_string()),
                    },
                ]);

                let mut engine =
                    SaseEngine::new(pattern).with_strategy(SelectionStrategy::StrictContiguous);
                let mut matches = 0;

                for event in &events {
                    for _m in engine.process(black_box(event)) {
                        matches += 1;
                    }
                }

                matches
            })
        });
    }

    group.finish();
}

/// Benchmark throughput: events per second
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.throughput(criterion::Throughput::Elements(10000));

    let events = generate_events(10000, &["A", "B", "C", "D"]);

    group.bench_function("sase_10k_events", |b| {
        b.iter(|| {
            // SEQ(A, B, C)
            let pattern = SasePattern::Seq(vec![
                SasePattern::Event {
                    event_type: "A".to_string(),
                    predicate: None,
                    alias: Some("a".to_string()),
                },
                SasePattern::Event {
                    event_type: "B".to_string(),
                    predicate: None,
                    alias: Some("b".to_string()),
                },
                SasePattern::Event {
                    event_type: "C".to_string(),
                    predicate: None,
                    alias: Some("c".to_string()),
                },
            ]);

            let mut engine =
                SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillNextMatch);
            let mut matches = 0;

            for event in &events {
                for _m in engine.process(black_box(event)) {
                    matches += 1;
                }
            }

            matches
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sase_simple_sequence,
    bench_sase_kleene_plus,
    bench_sase_with_predicates,
    bench_throughput,
);

criterion_main!(benches);
