//! Benchmarks for SASE+ pattern matching engine
//!
//! Run with: cargo bench -p varpulis-runtime
//!
//! Benchmark groups:
//! - simple_sequence: Basic A -> B patterns
//! - kleene_plus: A -> B+ -> C patterns
//! - with_predicates: Patterns with field predicates
//! - long_sequence: 5-10 event sequences
//! - complex_patterns: Negation, OR, nested patterns
//! - partition_by: Partitioned pattern matching
//! - throughput: Events per second measurement
//! - scalability: Large event volumes (50K-100K)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
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
        event = event.with_field("amount", ((i % 1000) * 10) as i64);
        event = event.with_field("category", format!("cat_{}", i % 10));
        events.push(event);
    }
    events
}

/// Generate events with specific user distribution for partition tests
#[allow(dead_code)]
fn generate_partitioned_events(count: usize, users: usize, event_types: &[&str]) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        let event_type = event_types[i % event_types.len()];
        let user_id = i % users;
        let mut event = Event::new(event_type);
        event = event.with_field("id", i as i64);
        event = event.with_field("user_id", format!("user_{}", user_id));
        event = event.with_field("value", ((i + user_id * 100) % 10000) as i64);
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

/// Benchmark long sequences (5-10 events)
fn bench_long_sequence(c: &mut Criterion) {
    let mut group = c.benchmark_group("long_sequence");

    // 5-event sequence
    let events_5 = generate_events(5000, &["A", "B", "C", "D", "E"]);
    group.bench_function("seq_5_events_5k", |b| {
        b.iter(|| {
            let pattern = SasePattern::Seq(vec![
                SasePattern::Event {
                    event_type: "A".into(),
                    predicate: None,
                    alias: Some("a".into()),
                },
                SasePattern::Event {
                    event_type: "B".into(),
                    predicate: None,
                    alias: Some("b".into()),
                },
                SasePattern::Event {
                    event_type: "C".into(),
                    predicate: None,
                    alias: Some("c".into()),
                },
                SasePattern::Event {
                    event_type: "D".into(),
                    predicate: None,
                    alias: Some("d".into()),
                },
                SasePattern::Event {
                    event_type: "E".into(),
                    predicate: None,
                    alias: Some("e".into()),
                },
            ]);
            let mut engine =
                SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillNextMatch);
            let mut matches = 0;
            for event in &events_5 {
                for _m in engine.process(black_box(event)) {
                    matches += 1;
                }
            }
            matches
        })
    });

    // 10-event sequence
    let events_10 = generate_events(
        10000,
        &["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "E10"],
    );
    group.bench_function("seq_10_events_10k", |b| {
        b.iter(|| {
            let pattern = SasePattern::Seq(
                (1..=10)
                    .map(|i| SasePattern::Event {
                        event_type: format!("E{}", i),
                        predicate: None,
                        alias: Some(format!("e{}", i)),
                    })
                    .collect(),
            );
            let mut engine =
                SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillNextMatch);
            let mut matches = 0;
            for event in &events_10 {
                for _m in engine.process(black_box(event)) {
                    matches += 1;
                }
            }
            matches
        })
    });

    group.finish();
}

/// Benchmark complex patterns: negation, OR, nested
fn bench_complex_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex_patterns");

    let events = generate_events(5000, &["Login", "Action", "Logout", "Error"]);

    // Negation: Login -> NOT(Error) -> Logout (simple negation without Kleene)
    group.bench_function("negation_5k", |b| {
        b.iter(|| {
            let pattern = SasePattern::Seq(vec![
                SasePattern::Event {
                    event_type: "Login".into(),
                    predicate: None,
                    alias: Some("login".into()),
                },
                SasePattern::Not(Box::new(SasePattern::Event {
                    event_type: "Error".into(),
                    predicate: None,
                    alias: None,
                })),
                SasePattern::Event {
                    event_type: "Logout".into(),
                    predicate: None,
                    alias: Some("logout".into()),
                },
            ]);
            let mut engine = SaseEngine::new(pattern)
                .with_strategy(SelectionStrategy::SkipTillNextMatch)
                .with_max_runs(1000);
            let mut matches = 0;
            for event in &events {
                for _m in engine.process(black_box(event)) {
                    matches += 1;
                }
            }
            matches
        })
    });

    // OR pattern: (Login OR Register) -> Action -> Logout
    group.bench_function("or_pattern_5k", |b| {
        b.iter(|| {
            let pattern = SasePattern::Seq(vec![
                SasePattern::Or(
                    Box::new(SasePattern::Event {
                        event_type: "Login".into(),
                        predicate: None,
                        alias: Some("start".into()),
                    }),
                    Box::new(SasePattern::Event {
                        event_type: "Register".into(),
                        predicate: None,
                        alias: Some("start".into()),
                    }),
                ),
                SasePattern::Event {
                    event_type: "Action".into(),
                    predicate: None,
                    alias: Some("action".into()),
                },
                SasePattern::Event {
                    event_type: "Logout".into(),
                    predicate: None,
                    alias: Some("logout".into()),
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

    // Complex nested: (A -> B+) OR (C -> D*)
    let events_nested = generate_events(5000, &["A", "B", "C", "D"]);
    group.bench_function("nested_kleene_5k", |b| {
        b.iter(|| {
            let pattern = SasePattern::Or(
                Box::new(SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "A".into(),
                        predicate: None,
                        alias: Some("a".into()),
                    },
                    SasePattern::KleenePlus(Box::new(SasePattern::Event {
                        event_type: "B".into(),
                        predicate: None,
                        alias: Some("b".into()),
                    })),
                ])),
                Box::new(SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "C".into(),
                        predicate: None,
                        alias: Some("c".into()),
                    },
                    SasePattern::KleeneStar(Box::new(SasePattern::Event {
                        event_type: "D".into(),
                        predicate: None,
                        alias: Some("d".into()),
                    })),
                ])),
            );
            let mut engine =
                SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillAnyMatch);
            let mut matches = 0;
            for event in &events_nested {
                for _m in engine.process(black_box(event)) {
                    matches += 1;
                }
            }
            matches
        })
    });

    group.finish();
}

/// Benchmark with multiple predicates
fn bench_multi_predicates(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_predicates");

    let events = generate_events(5000, &["Order", "Payment", "Shipment"]);

    // Order(value > 500) -> Payment(amount matches) -> Shipment
    group.bench_function("chained_predicates_5k", |b| {
        b.iter(|| {
            let pattern = SasePattern::Seq(vec![
                SasePattern::Event {
                    event_type: "Order".into(),
                    predicate: Some(Predicate::Compare {
                        field: "value".into(),
                        op: CompareOp::Gt,
                        value: varpulis_core::Value::Int(500),
                    }),
                    alias: Some("order".into()),
                },
                SasePattern::Event {
                    event_type: "Payment".into(),
                    predicate: Some(Predicate::And(
                        Box::new(Predicate::Compare {
                            field: "amount".into(),
                            op: CompareOp::Gt,
                            value: varpulis_core::Value::Int(100),
                        }),
                        Box::new(Predicate::Compare {
                            field: "amount".into(),
                            op: CompareOp::Lt,
                            value: varpulis_core::Value::Int(5000),
                        }),
                    )),
                    alias: Some("payment".into()),
                },
                SasePattern::Event {
                    event_type: "Shipment".into(),
                    predicate: None,
                    alias: Some("shipment".into()),
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

    group.finish();
}

/// Benchmark throughput: events per second
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");

    for size in [10_000, 50_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = generate_events(size, &["A", "B", "C", "D"]);

        group.bench_with_input(BenchmarkId::new("seq_3", size), &events, |b, events| {
            b.iter(|| {
                let pattern = SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "A".into(),
                        predicate: None,
                        alias: Some("a".into()),
                    },
                    SasePattern::Event {
                        event_type: "B".into(),
                        predicate: None,
                        alias: Some("b".into()),
                    },
                    SasePattern::Event {
                        event_type: "C".into(),
                        predicate: None,
                        alias: Some("c".into()),
                    },
                ]);
                let mut engine =
                    SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillNextMatch);
                let mut matches = 0;
                for event in events {
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

/// Benchmark scalability with large event volumes
fn bench_scalability(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalability");
    group.sample_size(20); // Fewer samples for large tests
    group.measurement_time(Duration::from_secs(10));

    // Test with 100K events
    let events_100k = generate_events(100_000, &["A", "B", "C"]);

    group.throughput(Throughput::Elements(100_000));
    group.bench_function("100k_simple_seq", |b| {
        b.iter(|| {
            let pattern = SasePattern::Seq(vec![
                SasePattern::Event {
                    event_type: "A".into(),
                    predicate: None,
                    alias: Some("a".into()),
                },
                SasePattern::Event {
                    event_type: "B".into(),
                    predicate: None,
                    alias: Some("b".into()),
                },
            ]);
            let mut engine =
                SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillNextMatch);
            let mut matches = 0;
            for event in &events_100k {
                for _m in engine.process(black_box(event)) {
                    matches += 1;
                }
            }
            matches
        })
    });

    // Kleene+ with 100K - stress test
    let events_kleene = generate_events(50_000, &["Start", "Middle", "Middle", "End"]);
    group.throughput(Throughput::Elements(50_000));
    group.bench_function("50k_kleene_plus", |b| {
        b.iter(|| {
            let pattern = SasePattern::Seq(vec![
                SasePattern::Event {
                    event_type: "Start".into(),
                    predicate: None,
                    alias: Some("s".into()),
                },
                SasePattern::KleenePlus(Box::new(SasePattern::Event {
                    event_type: "Middle".into(),
                    predicate: None,
                    alias: Some("m".into()),
                })),
                SasePattern::Event {
                    event_type: "End".into(),
                    predicate: None,
                    alias: Some("e".into()),
                },
            ]);
            let mut engine =
                SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillNextMatch);
            let mut matches = 0;
            for event in &events_kleene {
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
    bench_long_sequence,
    bench_complex_patterns,
    bench_multi_predicates,
    bench_throughput,
    bench_scalability,
);

criterion_main!(benches);
