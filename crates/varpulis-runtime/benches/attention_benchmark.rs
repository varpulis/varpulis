//! Benchmarks for Attention Engine
//!
//! Run with: cargo bench -p varpulis-runtime -- attention
//!
//! Benchmark groups:
//! - attention_single: Single event attention computation
//! - attention_batch: Batch processing with rayon
//! - dot_product: SIMD vs scalar dot product
//! - scalability: Large history sizes (1K-10K events)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;
use varpulis_runtime::attention::{AttentionConfig, AttentionEngine};
use varpulis_runtime::event::Event;

/// Generate test events for benchmarking
fn generate_events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| {
            Event::new(&format!("Event{}", i % 10))
                .with_field("id", i as i64)
                .with_field("value", (i * 17 % 1000) as i64)
                .with_field("user_id", format!("user_{}", i % 50))
                .with_field("amount", ((i * 31) % 10000) as i64)
        })
        .collect()
}

/// Benchmark single event attention computation
fn bench_attention_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("attention_single");

    for history_size in [100, 500, 1000] {
        let events = generate_events(history_size + 1);
        let config = AttentionConfig {
            max_history: history_size,
            ..Default::default()
        };

        group.bench_with_input(
            BenchmarkId::new("compute", history_size),
            &history_size,
            |b, _| {
                b.iter_batched(
                    || {
                        let mut engine = AttentionEngine::new(config.clone());
                        for event in events.iter().take(history_size) {
                            engine.add_event(event.clone());
                        }
                        (engine, events[history_size].clone())
                    },
                    |(mut engine, query)| {
                        engine.compute_attention(black_box(&query))
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark batch attention computation
fn bench_attention_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("attention_batch");

    let history_size = 500;
    let batch_sizes = [10, 50, 100];

    for batch_size in batch_sizes {
        let events = generate_events(history_size + batch_size);
        let config = AttentionConfig {
            max_history: history_size,
            ..Default::default()
        };

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &batch_size,
            |b, _| {
                b.iter_batched(
                    || {
                        let mut engine = AttentionEngine::new(config.clone());
                        for event in events.iter().take(history_size) {
                            engine.add_event(event.clone());
                        }
                        let batch: Vec<Event> = events
                            .iter()
                            .skip(history_size)
                            .take(batch_size)
                            .cloned()
                            .collect();
                        (engine, batch)
                    },
                    |(mut engine, batch)| {
                        engine.compute_attention_batch(black_box(&batch))
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Compare single vs batch processing
fn bench_single_vs_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_vs_batch");
    
    let history_size = 500;
    let batch_size = 50;
    let events = generate_events(history_size + batch_size);
    let config = AttentionConfig {
        max_history: history_size,
        ..Default::default()
    };

    group.throughput(Throughput::Elements(batch_size as u64));

    // Single event processing (sequential)
    group.bench_function("sequential_50", |b| {
        b.iter_batched(
            || {
                let mut engine = AttentionEngine::new(config.clone());
                for event in events.iter().take(history_size) {
                    engine.add_event(event.clone());
                }
                let queries: Vec<Event> = events
                    .iter()
                    .skip(history_size)
                    .take(batch_size)
                    .cloned()
                    .collect();
                (engine, queries)
            },
            |(mut engine, queries)| {
                for query in &queries {
                    engine.compute_attention(black_box(query));
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Batch processing (parallel)
    group.bench_function("parallel_50", |b| {
        b.iter_batched(
            || {
                let mut engine = AttentionEngine::new(config.clone());
                for event in events.iter().take(history_size) {
                    engine.add_event(event.clone());
                }
                let batch: Vec<Event> = events
                    .iter()
                    .skip(history_size)
                    .take(batch_size)
                    .cloned()
                    .collect();
                (engine, batch)
            },
            |(mut engine, batch)| {
                engine.compute_attention_batch(black_box(&batch))
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark scalability with large history sizes
fn bench_attention_scalability(c: &mut Criterion) {
    let mut group = c.benchmark_group("attention_scalability");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    for history_size in [1000, 2000, 5000] {
        let events = generate_events(history_size + 1);
        let config = AttentionConfig {
            max_history: history_size,
            ..Default::default()
        };

        group.bench_with_input(
            BenchmarkId::new("history", history_size),
            &history_size,
            |b, _| {
                b.iter_batched(
                    || {
                        let mut engine = AttentionEngine::new(config.clone());
                        for event in events.iter().take(history_size) {
                            engine.add_event(event.clone());
                        }
                        (engine, events[history_size].clone())
                    },
                    |(mut engine, query)| {
                        engine.compute_attention(black_box(&query))
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark embedding and caching
fn bench_embedding_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("embedding_cache");

    let events = generate_events(1000);
    let config = AttentionConfig::default();

    // Cold cache (first pass)
    group.bench_function("cold_cache_100", |b| {
        b.iter_batched(
            || {
                let engine = AttentionEngine::new(config.clone());
                let subset: Vec<Event> = events.iter().take(100).cloned().collect();
                (engine, subset)
            },
            |(mut engine, subset)| {
                for event in subset {
                    engine.add_event(black_box(event));
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Warm cache (repeated events)
    group.bench_function("warm_cache_100", |b| {
        b.iter_batched(
            || {
                let mut engine = AttentionEngine::new(config.clone());
                // Pre-warm cache
                for event in events.iter().take(100) {
                    engine.add_event(event.clone());
                }
                engine.clear_history();
                let subset: Vec<Event> = events.iter().take(100).cloned().collect();
                (engine, subset)
            },
            |(mut engine, subset)| {
                for event in subset {
                    engine.add_event(black_box(event));
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark HNSW vs linear scan
fn bench_hnsw_vs_linear(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_vs_linear");
    group.sample_size(20);

    for history_size in [200, 500, 1000, 2000] {
        let events = generate_events(history_size + 1);
        let config = AttentionConfig {
            max_history: history_size,
            ..Default::default()
        };

        // With HNSW
        group.bench_with_input(
            BenchmarkId::new("hnsw", history_size),
            &history_size,
            |b, _| {
                b.iter_batched(
                    || {
                        let mut engine = AttentionEngine::new(config.clone());
                        for event in events.iter().take(history_size) {
                            engine.add_event(event.clone());
                        }
                        (engine, events[history_size].clone())
                    },
                    |(mut engine, query)| {
                        engine.compute_attention(black_box(&query))
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );

        // Without HNSW (linear scan)
        group.bench_with_input(
            BenchmarkId::new("linear", history_size),
            &history_size,
            |b, _| {
                b.iter_batched(
                    || {
                        let mut engine = AttentionEngine::new_without_hnsw(config.clone());
                        for event in events.iter().take(history_size) {
                            engine.add_event(event.clone());
                        }
                        (engine, events[history_size].clone())
                    },
                    |(mut engine, query)| {
                        engine.compute_attention(black_box(&query))
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_attention_single,
    bench_attention_batch,
    bench_single_vs_batch,
    bench_hnsw_vs_linear,
    bench_attention_scalability,
    bench_embedding_cache,
);

criterion_main!(benches);
