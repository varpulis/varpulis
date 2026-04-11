//! Microbenchmarks: row-oriented vs streaming columnar grouped aggregator.
//!
//! These measure the phase-1 win directly, without any Kafka / window /
//! run-loop noise. The scenarios vary along two axes:
//!   - batch size (1k / 10k / 100k events per `apply()` call)
//!   - group cardinality (10 / 100 / 10k distinct partition keys)
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p varpulis-runtime --features arrow \
//!     --bench columnar_agg_benchmark
//! ```
//!
//! Phase 1 targets the "bulk aggregate" shape — one `apply()` call with
//! many events. On that shape the columnar path wins by ~3-10×
//! depending on group density. On the "streaming" shape (many tiny
//! `apply()` calls with 1–2 events each), phase 1 has no effect because
//! batches fall below `ARROW_BATCH_THRESHOLD`; that's exactly what
//! phase 2's streaming fused operator targets.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use varpulis_core::Value;
use varpulis_runtime::aggregation::{Aggregator, Avg, Count, Max, Min, Sum};
use varpulis_runtime::engine::{
    AggregatorState, PartitionedAggregatorState, WindowedColumnarAggregateState,
};
use varpulis_runtime::event::{Event, SharedEvent};

fn make_state() -> PartitionedAggregatorState {
    PartitionedAggregatorState::new(
        "device_id".to_string(),
        Aggregator::new()
            .add("s", Box::new(Sum), Some("value".to_string()))
            .add("a", Box::new(Avg), Some("value".to_string()))
            .add("mn", Box::new(Min), Some("value".to_string()))
            .add("mx", Box::new(Max), Some("value".to_string()))
            .add("c", Box::new(Count), None),
    )
}

fn make_np_state() -> AggregatorState {
    AggregatorState::new(
        Aggregator::new()
            .add("s", Box::new(Sum), Some("value".to_string()))
            .add("a", Box::new(Avg), Some("value".to_string()))
            .add("mn", Box::new(Min), Some("value".to_string()))
            .add("mx", Box::new(Max), Some("value".to_string()))
            .add("c", Box::new(Count), None),
    )
}

fn make_np_events(total: usize) -> Vec<SharedEvent> {
    (0..total)
        .map(|i| {
            let v = (i as f64) * 1.5 + 10.0;
            Arc::new(Event::new("Reading").with_field("value", Value::Float(v)))
        })
        .collect()
}

fn make_events(total: usize, groups: usize) -> Vec<SharedEvent> {
    (0..total)
        .map(|i| {
            let dev = i % groups;
            let v = (i as f64) * 1.5 + 10.0;
            Arc::new(
                Event::new("Reading")
                    .with_field("device_id", Value::Str(format!("d{dev}").into()))
                    .with_field("value", Value::Float(v)),
            )
        })
        .collect()
}

fn bench_apply(c: &mut Criterion) {
    let configs = [
        // (label, total_events, group_count)
        ("1k_10groups", 1_000, 10),
        ("1k_100groups", 1_000, 100),
        ("10k_10groups", 10_000, 10),
        ("10k_100groups", 10_000, 100),
        ("100k_100groups", 100_000, 100),
        ("100k_10kgroups", 100_000, 10_000),
    ];

    let mut group = c.benchmark_group("PartitionedAggregatorState");
    for (label, total, groups) in configs {
        let events = make_events(total, groups);
        // Row path — the pre-phase-1 baseline.
        group.bench_with_input(BenchmarkId::new("row", label), &events, |b, events| {
            b.iter_batched(
                make_state,
                |mut state: PartitionedAggregatorState| {
                    black_box(state.apply_row(events));
                },
                criterion::BatchSize::LargeInput,
            );
        });
        // Columnar path — phase 1 under test.
        group.bench_with_input(BenchmarkId::new("columnar", label), &events, |b, events| {
            b.iter_batched(
                make_state,
                |mut state: PartitionedAggregatorState| {
                    black_box(
                        state
                            .apply_columnar(events)
                            .expect("columnar should succeed"),
                    );
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// Phase 3a: non-partitioned `AggregatorState::apply` row vs single-group
/// columnar path. Tests the `.window(N).aggregate(...)` shape that sits
/// behind pipelines without `.partition_by(...)`.
fn bench_non_partitioned_apply(c: &mut Criterion) {
    let sizes = [100, 1_000, 10_000, 100_000];

    let mut group = c.benchmark_group("AggregatorState");
    for total in sizes {
        let events = make_np_events(total);
        let label = format!("{total}");
        group.bench_with_input(BenchmarkId::new("row", &label), &events, |b, events| {
            b.iter_batched(
                make_np_state,
                |mut state: AggregatorState| {
                    black_box(state.apply_row(events));
                },
                criterion::BatchSize::LargeInput,
            );
        });
        group.bench_with_input(
            BenchmarkId::new("columnar", &label),
            &events,
            |b, events| {
                b.iter_batched(
                    make_np_state,
                    |mut state: AggregatorState| {
                        black_box(
                            state
                                .apply_columnar(events)
                                .expect("columnar should succeed"),
                        );
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

/// Phase 3b: streaming fused non-partitioned `Window(Tumbling) + Aggregate`.
/// Simulates the per-event streaming shape: N tumbling bins of 100 events
/// each, events fed one at a time (the same shape as scenario 02 but
/// without a partition key). Compares the row-path (row-oriented
/// AggregatorState driven from a plain TumblingWindow-fire dance) to
/// the fused `WindowedColumnarAggregateState`.
fn bench_phase_3b_streaming(c: &mut Criterion) {
    // Build one bin's worth of events, timestamped into a single 1-second
    // window. The benchmark feeds them one at a time plus one "flush"
    // event in the next bin to trigger the drain.
    fn make_streaming_events(per_bin: usize, n_bins: usize) -> Vec<SharedEvent> {
        let mut events = Vec::with_capacity(per_bin * n_bins + 1);
        for bin in 0..n_bins {
            for i in 0..per_bin {
                let ts_ms = (bin as i64) * 1_000 + (i as i64);
                let ts = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_ms).unwrap();
                events.push(Arc::new(
                    Event::new("Reading")
                        .with_timestamp(ts)
                        .with_field("value", Value::Float(i as f64)),
                ));
            }
        }
        // Sentinel event in a future bin to flush all previous bins.
        let sentinel_ts = (n_bins as i64) * 1_000 + 500;
        let ts = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(sentinel_ts).unwrap();
        events.push(Arc::new(
            Event::new("Reading")
                .with_timestamp(ts)
                .with_field("value", Value::Float(0.0)),
        ));
        events
    }

    let configs = [
        // (label, per_bin, n_bins)
        ("100bins_x100", 100, 100),
        ("1000bins_x100", 100, 1_000),
    ];

    let mut group = c.benchmark_group("Phase3b_Streaming");
    for (label, per_bin, n_bins) in configs {
        let events = make_streaming_events(per_bin, n_bins);

        // Row path: one AggregatorState, drained per bin by hand. This
        // approximates the pre-phase-3b pipeline where `Window(Tumbling)`
        // emits a `Vec<SharedEvent>` every bin and `Aggregate` re-runs.
        group.bench_with_input(BenchmarkId::new("row", label), &events, |b, events| {
            b.iter_batched(
                || (),
                |()| {
                    // Group events by bin manually and apply the state
                    // to each bin. This is coarser than the real pipeline
                    // but captures the same "build batch → aggregate per
                    // fire" overhead.
                    let mut by_bin: std::collections::BTreeMap<i64, Vec<SharedEvent>> =
                        Default::default();
                    for ev in events {
                        let ts = ev.timestamp.timestamp_millis();
                        let bin = ts / 1_000;
                        by_bin.entry(bin).or_default().push(Arc::clone(ev));
                    }
                    for (_bin, bin_events) in by_bin {
                        let mut state = AggregatorState::new(
                            Aggregator::new()
                                .add("s", Box::new(Sum), Some("value".to_string()))
                                .add("a", Box::new(Avg), Some("value".to_string()))
                                .add("mn", Box::new(Min), Some("value".to_string()))
                                .add("mx", Box::new(Max), Some("value".to_string()))
                                .add("c", Box::new(Count), None),
                        );
                        black_box(state.apply_row(&bin_events));
                    }
                },
                criterion::BatchSize::LargeInput,
            );
        });

        // Fused path: single WindowedColumnarAggregateState, events fed
        // one-at-a-time through ingest_and_flush.
        group.bench_with_input(BenchmarkId::new("fused", label), &events, |b, events| {
            b.iter_batched(
                || {
                    WindowedColumnarAggregateState::try_new(
                        1_000,
                        &Aggregator::new()
                            .add("s", Box::new(Sum), Some("value".to_string()))
                            .add("a", Box::new(Avg), Some("value".to_string()))
                            .add("mn", Box::new(Min), Some("value".to_string()))
                            .add("mx", Box::new(Max), Some("value".to_string()))
                            .add("c", Box::new(Count), None),
                    )
                    .unwrap()
                },
                |mut state| {
                    for ev in events {
                        black_box(state.ingest_and_flush(std::slice::from_ref(ev)));
                    }
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_apply,
    bench_non_partitioned_apply,
    bench_phase_3b_streaming
);
criterion_main!(benches);
