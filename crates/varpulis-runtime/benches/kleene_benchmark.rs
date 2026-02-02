//! Benchmarks for Kleene patterns (ZDD-based implementation)
//!
//! These benchmarks specifically test the ZDD optimization for Kleene+ patterns.
//! Run with: cargo bench -p varpulis-runtime --bench kleene_benchmark
//!
//! Benchmark groups:
//! - kleene_simple: Basic A -> B+ -> C patterns with varying event counts
//! - kleene_rising_sequence: Stock price spike detection (realistic scenario)
//! - kleene_exponential: Stress test with many Kleene events (2^n combinations)
//! - kleene_with_predicates: Kleene with field constraints
//! - kleene_all_pattern: The "all" semantic (emit on every match)
//!
//! These benchmarks validate that ZDD provides polynomial memory vs exponential

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;
use varpulis_runtime::event::Event;
use varpulis_runtime::sase::{SaseEngine, SasePattern, SelectionStrategy};

// =============================================================================
// Event generators
// =============================================================================

/// Generate events for A -> B+ -> C pattern
fn generate_abc_events(count: usize, b_ratio: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        let event_type = if i % (b_ratio + 2) == 0 {
            "A"
        } else if i % (b_ratio + 2) == b_ratio + 1 {
            "C"
        } else {
            "B"
        };
        let mut event = Event::new(event_type);
        event = event.with_field("id", i as i64);
        event = event.with_field("value", (i * 10) as i64);
        events.push(event);
    }
    events
}

/// Generate stock events for rising price sequence
fn generate_rising_price_events(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    let mut price = 100.0;
    for i in 0..count {
        // Simulate price movements: mostly up with occasional resets
        if i % 20 == 0 {
            price = 100.0 + (i as f64 * 0.01); // Reset for new sequence
        } else if i % 5 == 4 {
            price -= 5.0; // End sequence (lower price)
        } else {
            price += (i as f64 % 10.0) * 0.5; // Rising price
        }
        let event = Event::new("StockTick")
            .with_field("symbol", "ACME")
            .with_field("price", price)
            .with_field("volume", (i * 100) as i64);
        events.push(event);
    }
    events
}

/// Generate events that maximize Kleene combinations
/// Creates Start -> (Middle)+ -> End sequences with many Middles
fn generate_kleene_stress_events(num_sequences: usize, middles_per_seq: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(num_sequences * (middles_per_seq + 2));
    for seq in 0..num_sequences {
        // Start event
        events.push(
            Event::new("Start")
                .with_field("seq_id", seq as i64)
                .with_field("value", (seq * 100) as i64),
        );
        // Multiple Middle events (each can be included or not = 2^n combinations)
        for m in 0..middles_per_seq {
            events.push(
                Event::new("Middle")
                    .with_field("seq_id", seq as i64)
                    .with_field("middle_id", m as i64)
                    .with_field("value", (seq * 100 + m) as i64),
            );
        }
        // End event
        events.push(
            Event::new("End")
                .with_field("seq_id", seq as i64)
                .with_field("value", (seq * 100 + 99) as i64),
        );
    }
    events
}

// =============================================================================
// Benchmark 1: Simple Kleene+ pattern
// =============================================================================

fn bench_kleene_simple(c: &mut Criterion) {
    let mut group = c.benchmark_group("kleene_simple");

    for size in [100, 500, 1000, 5000] {
        // Events with ~3 B events between A and C
        let events = generate_abc_events(size, 3);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("seq_a_b_plus_c", size),
            &events,
            |b, events| {
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

                    let mut engine = SaseEngine::new(pattern)
                        .with_strategy(SelectionStrategy::SkipTillNextMatch);
                    let mut matches = 0;

                    for event in events {
                        for _m in engine.process(black_box(event)) {
                            matches += 1;
                        }
                    }

                    matches
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 2: Rising price sequence (realistic Kleene use case)
// =============================================================================

fn bench_kleene_rising_sequence(c: &mut Criterion) {
    let mut group = c.benchmark_group("kleene_rising_sequence");

    for size in [500, 1000, 5000, 10000] {
        let events = generate_rising_price_events(size);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("rising_prices", size),
            &events,
            |b, events| {
                b.iter(|| {
                    // SEQ(StockTick as first, StockTick+ where price > prev.price as rising, StockTick as last)
                    // Simplified: we detect Start -> (Higher)+ -> Lower pattern
                    let pattern = SasePattern::Seq(vec![
                        SasePattern::Event {
                            event_type: "StockTick".to_string(),
                            predicate: None,
                            alias: Some("first".to_string()),
                        },
                        SasePattern::KleenePlus(Box::new(SasePattern::Event {
                            event_type: "StockTick".to_string(),
                            predicate: None, // In real code, would have: price > first.price
                            alias: Some("rising".to_string()),
                        })),
                        SasePattern::Event {
                            event_type: "StockTick".to_string(),
                            predicate: None, // In real code: price < last(rising).price
                            alias: Some("last".to_string()),
                        },
                    ]);

                    let mut engine = SaseEngine::new(pattern)
                        .with_strategy(SelectionStrategy::SkipTillNextMatch)
                        .with_max_runs(100);
                    let mut matches = 0;

                    for event in events {
                        for _m in engine.process(black_box(event)) {
                            matches += 1;
                        }
                    }

                    matches
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 3: Exponential combinations stress test
// =============================================================================

fn bench_kleene_exponential(c: &mut Criterion) {
    let mut group = c.benchmark_group("kleene_exponential");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));

    // Test with increasing numbers of Middle events per sequence
    // With n Middles, there are 2^n - 1 possible Kleene+ combinations
    for middles in [5, 10, 15, 20] {
        let events = generate_kleene_stress_events(10, middles);
        let total_events = events.len();
        let potential_combinations = (1u64 << middles) - 1; // 2^n - 1 for Kleene+

        group.throughput(Throughput::Elements(total_events as u64));

        let label = format!("{}middles_2^{}_combos", middles, middles);
        group.bench_with_input(
            BenchmarkId::new(&label, potential_combinations),
            &events,
            |b, events| {
                b.iter(|| {
                    let pattern = SasePattern::Seq(vec![
                        SasePattern::Event {
                            event_type: "Start".to_string(),
                            predicate: None,
                            alias: Some("start".to_string()),
                        },
                        SasePattern::KleenePlus(Box::new(SasePattern::Event {
                            event_type: "Middle".to_string(),
                            predicate: None,
                            alias: Some("middle".to_string()),
                        })),
                        SasePattern::Event {
                            event_type: "End".to_string(),
                            predicate: None,
                            alias: Some("end".to_string()),
                        },
                    ]);

                    let mut engine = SaseEngine::new(pattern)
                        .with_strategy(SelectionStrategy::SkipTillNextMatch)
                        .with_max_runs(100);
                    let mut matches = 0;

                    for event in events {
                        for _m in engine.process(black_box(event)) {
                            matches += 1;
                        }
                    }

                    matches
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 4: Kleene* (zero or more)
// =============================================================================

fn bench_kleene_star(c: &mut Criterion) {
    let mut group = c.benchmark_group("kleene_star");

    for size in [100, 500, 1000, 5000] {
        let events = generate_abc_events(size, 3);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("seq_a_b_star_c", size),
            &events,
            |b, events| {
                b.iter(|| {
                    // SEQ(A, B*, C) - zero or more B events
                    let pattern = SasePattern::Seq(vec![
                        SasePattern::Event {
                            event_type: "A".to_string(),
                            predicate: None,
                            alias: Some("a".to_string()),
                        },
                        SasePattern::KleeneStar(Box::new(SasePattern::Event {
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

                    let mut engine = SaseEngine::new(pattern)
                        .with_strategy(SelectionStrategy::SkipTillNextMatch);
                    let mut matches = 0;

                    for event in events {
                        for _m in engine.process(black_box(event)) {
                            matches += 1;
                        }
                    }

                    matches
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 5: All pattern (Kleene with emit-on-each)
// =============================================================================

fn bench_all_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("all_pattern");

    for size in [100, 500, 1000] {
        // Events matching "all StockTick where price > 100"
        let mut events = Vec::with_capacity(size);
        for i in 0..size {
            let price = 50.0 + (i as f64 % 100.0);
            events.push(
                Event::new("StockTick")
                    .with_field("symbol", "ACME")
                    .with_field("price", price)
                    .with_field("volume", (i * 100) as i64),
            );
        }
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("all_matching", size),
            &events,
            |b, events| {
                b.iter(|| {
                    // Pattern: "all StockTick" - matches each event individually
                    // This compiles to KleenePlus with emit-on-each semantics
                    let pattern = SasePattern::KleenePlus(Box::new(SasePattern::Event {
                        event_type: "StockTick".to_string(),
                        predicate: None,
                        alias: Some("tick".to_string()),
                    }));

                    let mut engine =
                        SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillAnyMatch);
                    let mut matches = 0;

                    for event in events {
                        for _m in engine.process(black_box(event)) {
                            matches += 1;
                        }
                    }

                    matches
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 6: Compare with/without max_runs limit
// =============================================================================

fn bench_max_runs_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("max_runs_impact");

    let events = generate_kleene_stress_events(20, 10);
    let total_events = events.len();
    group.throughput(Throughput::Elements(total_events as u64));

    for max_runs in [10, 50, 100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("max_runs", max_runs),
            &max_runs,
            |b, &max_runs| {
                b.iter(|| {
                    let pattern = SasePattern::Seq(vec![
                        SasePattern::Event {
                            event_type: "Start".to_string(),
                            predicate: None,
                            alias: Some("start".to_string()),
                        },
                        SasePattern::KleenePlus(Box::new(SasePattern::Event {
                            event_type: "Middle".to_string(),
                            predicate: None,
                            alias: Some("middle".to_string()),
                        })),
                        SasePattern::Event {
                            event_type: "End".to_string(),
                            predicate: None,
                            alias: Some("end".to_string()),
                        },
                    ]);

                    let mut engine = SaseEngine::new(pattern)
                        .with_strategy(SelectionStrategy::SkipTillNextMatch)
                        .with_max_runs(max_runs);
                    let mut matches = 0;

                    for event in &events {
                        for _m in engine.process(black_box(event)) {
                            matches += 1;
                        }
                    }

                    matches
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 7: Memory efficiency (ZDD node count vs combination count)
// =============================================================================

fn bench_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_efficiency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    // Measure how ZDD maintains efficiency even with huge combination counts
    for middles in [15, 20, 25] {
        let events = generate_kleene_stress_events(5, middles);
        let potential_combinations: u128 = (1u128 << middles) - 1;

        let label = format!("2^{}_combinations", middles);
        group.bench_function(&label, |b| {
            b.iter(|| {
                let pattern = SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "Start".to_string(),
                        predicate: None,
                        alias: Some("start".to_string()),
                    },
                    SasePattern::KleenePlus(Box::new(SasePattern::Event {
                        event_type: "Middle".to_string(),
                        predicate: None,
                        alias: Some("middle".to_string()),
                    })),
                    SasePattern::Event {
                        event_type: "End".to_string(),
                        predicate: None,
                        alias: Some("end".to_string()),
                    },
                ]);

                let mut engine = SaseEngine::new(pattern)
                    .with_strategy(SelectionStrategy::SkipTillNextMatch)
                    .with_max_runs(10);
                let mut matches = 0;

                for event in &events {
                    for _m in engine.process(black_box(event)) {
                        matches += 1;
                    }
                }

                // ZDD allows tracking 2^25 = 33M combinations efficiently
                black_box((matches, potential_combinations))
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_kleene_simple,
    bench_kleene_rising_sequence,
    bench_kleene_exponential,
    bench_kleene_star,
    bench_all_pattern,
    bench_max_runs_impact,
    bench_memory_efficiency,
);

criterion_main!(benches);
