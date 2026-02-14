//! # PST Forecasting Benchmarks
//!
//! Benchmarks for the Prediction Suffix Tree (PST) and Pattern Markov Chain (PMC)
//! forecasting engine.
//!
//! ## Running the Benchmarks
//!
//! ```bash
//! cargo bench -p varpulis-runtime --bench pst_forecast_benchmark
//! ```
//!
//! ## Background
//!
//! PST provides variable-order Markov models for predicting whether a partially-matched
//! SASE+ pattern will complete. Combined with the NFA to form a Pattern Markov Chain (PMC).
//! See: "Complex Event Forecasting with Prediction Suffix Trees"
//! (Alevizos, Artikis, Paliouras -- arXiv:2109.00287)
//!
//! ## Benchmark Groups
//!
//! - **pst_training**: Train PST on 1K/10K/100K sequences
//! - **pst_prediction**: Prediction latency at depths 1/3/5/10
//! - **pmc_forecast**: PMC with 3-state NFA, process events
//! - **online_learning**: Incremental update throughput
//! - **alphabet_scaling**: PST with 5/10/50/100 event types

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rustc_hash::FxHashMap;
use std::time::Duration;
use varpulis_runtime::pst::*;

// =============================================================================
// Sequence generators
// =============================================================================

/// Generate a repeating pattern sequence of the given alphabet size.
/// Pattern cycles through symbols 0..alphabet_size.
fn generate_cyclic_sequence(length: usize, alphabet_size: usize) -> Vec<SymbolId> {
    (0..length)
        .map(|i| (i % alphabet_size) as SymbolId)
        .collect()
}

/// Generate a sequence with variable-length sub-patterns.
/// Creates A -> B+ -> C patterns with varying B repetitions.
fn generate_variable_pattern_sequence(num_patterns: usize) -> Vec<SymbolId> {
    let mut seq = Vec::new();
    for i in 0..num_patterns {
        seq.push(0); // A
        let b_count = 1 + (i % 5); // 1-5 B's
        seq.extend(std::iter::repeat_n(1u16, b_count));
        seq.push(2); // C
    }
    seq
}

/// Generate a random-ish sequence over the given alphabet using a simple LCG.
fn generate_pseudo_random_sequence(
    length: usize,
    alphabet_size: usize,
    seed: u64,
) -> Vec<SymbolId> {
    let mut state = seed;
    (0..length)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) % alphabet_size as u64) as SymbolId
        })
        .collect()
}

/// Build a PST with registered symbols and pre-train on a sequence.
fn build_trained_pst(
    config: PSTConfig,
    alphabet_size: usize,
    training_seq: &[SymbolId],
) -> PredictionSuffixTree {
    let mut pst = PredictionSuffixTree::new(config);
    for i in 0..alphabet_size {
        pst.register_symbol(&format!("E{}", i));
    }
    pst.train(training_seq);
    pst
}

/// Build a PMC with a linear NFA: S0 --(E0)--> S1 --(E1)--> S2 --(E2)--> Accept
fn build_pmc(num_states: usize, alphabet_size: usize, warmup: u64) -> PatternMarkovChain {
    let event_types: Vec<String> = (0..alphabet_size).map(|i| format!("E{}", i)).collect();
    let pst_config = PSTConfig {
        max_depth: 5,
        smoothing: 0.01,
    };
    let pmc_config = PMCConfig {
        warmup_events: warmup,
        confidence_threshold: 0.5,
        horizon_ns: 300_000_000_000,
        max_simulation_steps: 50,
    };

    // Linear NFA: state i --(symbol i)--> state i+1; last state is accept
    let mut transitions = vec![FxHashMap::default(); num_states];
    for (state, trans) in transitions.iter_mut().enumerate().take(num_states - 1) {
        let symbol = (state % alphabet_size) as SymbolId;
        trans.insert(symbol, state + 1);
    }

    let accept_states = vec![num_states - 1];

    PatternMarkovChain::new(
        &event_types,
        transitions,
        accept_states,
        num_states,
        pst_config,
        pmc_config,
    )
}

// =============================================================================
// Benchmark: PST Training
// =============================================================================

fn bench_pst_training(c: &mut Criterion) {
    let mut group = c.benchmark_group("pst_training");
    group.measurement_time(Duration::from_secs(10));

    let sizes = [1_000, 10_000, 100_000];

    for &size in &sizes {
        let sequence = generate_cyclic_sequence(size, 10);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("cyclic_10_types", size), &size, |b, _| {
            b.iter(|| {
                let config = PSTConfig {
                    max_depth: 5,
                    smoothing: 0.01,
                };
                let mut pst = PredictionSuffixTree::new(config);
                for i in 0..10 {
                    pst.register_symbol(&format!("E{}", i));
                }
                pst.train(black_box(&sequence));
                black_box(pst.node_count())
            })
        });

        let var_seq = generate_variable_pattern_sequence(size / 4);
        group.bench_with_input(BenchmarkId::new("variable_pattern", size), &size, |b, _| {
            b.iter(|| {
                let config = PSTConfig {
                    max_depth: 5,
                    smoothing: 0.01,
                };
                let mut pst = PredictionSuffixTree::new(config);
                for i in 0..3 {
                    pst.register_symbol(&format!("E{}", i));
                }
                pst.train(black_box(&var_seq));
                black_box(pst.node_count())
            })
        });
    }

    group.finish();
}

// =============================================================================
// Benchmark: PST Prediction
// =============================================================================

fn bench_pst_prediction(c: &mut Criterion) {
    let mut group = c.benchmark_group("pst_prediction");
    group.measurement_time(Duration::from_secs(5));

    let depths = [1, 3, 5, 10];
    let alphabet_size = 10;
    let training_seq = generate_cyclic_sequence(50_000, alphabet_size);

    for &depth in &depths {
        let config = PSTConfig {
            max_depth: depth,
            smoothing: 0.01,
        };
        let pst = build_trained_pst(config, alphabet_size, &training_seq);

        // Build a context of the given depth
        let context: Vec<SymbolId> = (0..depth)
            .map(|i| (i % alphabet_size) as SymbolId)
            .collect();

        // Benchmark single-symbol prediction
        group.bench_with_input(BenchmarkId::new("predict_symbol", depth), &depth, |b, _| {
            b.iter(|| black_box(pst.predict_symbol(black_box(&context), 0)))
        });

        // Benchmark full distribution prediction
        group.bench_with_input(
            BenchmarkId::new("predict_distribution", depth),
            &depth,
            |b, _| b.iter(|| black_box(pst.predict(black_box(&context)))),
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark: PMC Forecast
// =============================================================================

fn bench_pmc_forecast(c: &mut Criterion) {
    let mut group = c.benchmark_group("pmc_forecast");
    group.measurement_time(Duration::from_secs(10));

    let num_events = 10_000;
    let alphabet_size = 3;
    let num_states = 4; // S0 -> S1 -> S2 -> Accept (3 transitions)

    group.throughput(Throughput::Elements(num_events as u64));

    // Benchmark with no active runs (warmup + streaming, no forecasts emitted)
    group.bench_function("process_no_runs", |b| {
        b.iter(|| {
            let mut pmc = build_pmc(num_states, alphabet_size, 50);
            for i in 0..num_events {
                let event_type = format!("E{}", i % alphabet_size);
                let ts = i as i64 * 1_000_000_000;
                black_box(pmc.process(&event_type, ts, &[]));
            }
            black_box(pmc.events_processed())
        })
    });

    // Benchmark with active runs (forecasts computed)
    group.bench_function("process_with_runs", |b| {
        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 0,
        }];
        b.iter(|| {
            let mut pmc = build_pmc(num_states, alphabet_size, 50);
            for i in 0..num_events {
                let event_type = format!("E{}", i % alphabet_size);
                let ts = i as i64 * 1_000_000_000;
                black_box(pmc.process(&event_type, ts, &runs));
            }
            black_box(pmc.events_processed())
        })
    });

    // Benchmark with multiple active runs
    group.bench_function("process_multi_runs", |b| {
        let runs = vec![
            RunSnapshot {
                current_state: 0,
                started_at_ns: 0,
            },
            RunSnapshot {
                current_state: 1,
                started_at_ns: 500_000_000,
            },
            RunSnapshot {
                current_state: 2,
                started_at_ns: 1_000_000_000,
            },
        ];
        b.iter(|| {
            let mut pmc = build_pmc(num_states, alphabet_size, 50);
            for i in 0..num_events {
                let event_type = format!("E{}", i % alphabet_size);
                let ts = i as i64 * 1_000_000_000;
                black_box(pmc.process(&event_type, ts, &runs));
            }
            black_box(pmc.events_processed())
        })
    });

    group.finish();
}

// =============================================================================
// Benchmark: Online Learning
// =============================================================================

fn bench_online_learning(c: &mut Criterion) {
    let mut group = c.benchmark_group("online_learning");
    group.measurement_time(Duration::from_secs(10));

    let num_updates = 50_000;
    let alphabet_size = 10;

    group.throughput(Throughput::Elements(num_updates as u64));

    // Online learning without pruning
    group.bench_function("incremental_no_prune", |b| {
        let symbols: Vec<SymbolId> = generate_cyclic_sequence(num_updates, alphabet_size);
        b.iter(|| {
            let config = PSTConfig {
                max_depth: 5,
                smoothing: 0.01,
            };
            let mut pst = PredictionSuffixTree::new(config);
            for i in 0..alphabet_size {
                pst.register_symbol(&format!("E{}", i));
            }
            let mut learner = OnlinePSTLearner::new(5);
            for &sym in &symbols {
                learner.update(&mut pst, sym);
            }
            black_box(pst.node_count())
        })
    });

    // Online learning with KL-divergence pruning every 5K events
    group.bench_function("incremental_with_prune", |b| {
        let symbols: Vec<SymbolId> = generate_cyclic_sequence(num_updates, alphabet_size);
        b.iter(|| {
            let config = PSTConfig {
                max_depth: 5,
                smoothing: 0.01,
            };
            let mut pst = PredictionSuffixTree::new(config);
            for i in 0..alphabet_size {
                pst.register_symbol(&format!("E{}", i));
            }
            let mut learner = OnlinePSTLearner::new(5);
            for (idx, &sym) in symbols.iter().enumerate() {
                learner.update(&mut pst, sym);
                if idx > 0 && idx % 5000 == 0 {
                    pst.prune(&PruningStrategy::KLDivergence { threshold: 0.05 });
                }
            }
            black_box(pst.node_count())
        })
    });

    // Mixed: online learning + periodic prediction (realistic workload)
    group.bench_function("learn_and_predict_mixed", |b| {
        let symbols: Vec<SymbolId> = generate_cyclic_sequence(num_updates, alphabet_size);
        b.iter(|| {
            let config = PSTConfig {
                max_depth: 5,
                smoothing: 0.01,
            };
            let mut pst = PredictionSuffixTree::new(config);
            for i in 0..alphabet_size {
                pst.register_symbol(&format!("E{}", i));
            }
            let mut learner = OnlinePSTLearner::new(5);
            let mut total_prob = 0.0f64;
            for (idx, &sym) in symbols.iter().enumerate() {
                learner.update(&mut pst, sym);
                // Predict every 100th event
                if idx % 100 == 0 {
                    let ctx = learner.current_context();
                    total_prob += pst.predict_symbol(ctx, (idx % alphabet_size) as SymbolId);
                }
            }
            black_box(total_prob)
        })
    });

    group.finish();
}

// =============================================================================
// Benchmark: Alphabet Scaling
// =============================================================================

fn bench_alphabet_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("alphabet_scaling");
    group.measurement_time(Duration::from_secs(10));

    let alphabet_sizes = [5, 10, 50, 100];
    let training_length = 50_000;

    for &alpha in &alphabet_sizes {
        let sequence = generate_pseudo_random_sequence(training_length, alpha, 42);

        // Training time scales with alphabet size
        group.throughput(Throughput::Elements(training_length as u64));
        group.bench_with_input(BenchmarkId::new("train", alpha), &alpha, |b, &a| {
            b.iter(|| {
                let config = PSTConfig {
                    max_depth: 5,
                    smoothing: 0.01,
                };
                let mut pst = PredictionSuffixTree::new(config);
                for i in 0..a {
                    pst.register_symbol(&format!("E{}", i));
                }
                pst.train(black_box(&sequence));
                black_box(pst.node_count())
            })
        });

        // Prediction time with full alphabet
        let config = PSTConfig {
            max_depth: 5,
            smoothing: 0.01,
        };
        let pst = build_trained_pst(config, alpha, &sequence);
        let context: Vec<SymbolId> = (0..5).map(|i| (i % alpha) as SymbolId).collect();

        group.bench_with_input(BenchmarkId::new("predict", alpha), &alpha, |b, _| {
            b.iter(|| black_box(pst.predict(black_box(&context))))
        });

        // Online update throughput at different alphabet sizes
        group.bench_with_input(BenchmarkId::new("online_update", alpha), &alpha, |b, &a| {
            let symbols = generate_pseudo_random_sequence(10_000, a, 123);
            b.iter(|| {
                let config = PSTConfig {
                    max_depth: 5,
                    smoothing: 0.01,
                };
                let mut pst = PredictionSuffixTree::new(config);
                for i in 0..a {
                    pst.register_symbol(&format!("E{}", i));
                }
                let mut learner = OnlinePSTLearner::new(5);
                for &sym in &symbols {
                    learner.update(&mut pst, sym);
                }
                black_box(pst.node_count())
            })
        });
    }

    group.finish();
}

// =============================================================================
// Main
// =============================================================================

criterion_group!(
    benches,
    bench_pst_training,
    bench_pst_prediction,
    bench_pmc_forecast,
    bench_online_learning,
    bench_alphabet_scaling,
);

criterion_main!(benches);
