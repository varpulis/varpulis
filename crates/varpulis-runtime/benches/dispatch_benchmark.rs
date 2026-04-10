//! Pipeline dispatch benchmark — measures the per-event cost of
//! `engine.process_batch_sync` on scenario 02's shape WITHOUT Kafka.
//!
//! This isolates the engine's pipeline dispatch overhead from Kafka
//! consumer/producer I/O, network, and JSON serialization. The gap
//! between this number and the Criterion columnar-agg bench tells us
//! how much time the per-event dispatch machinery eats.
//!
//! ```sh
//! cargo bench -p varpulis-runtime --features arrow \
//!     --bench dispatch_benchmark
//! ```

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

fn make_scenario_02_events(n: usize, n_devices: usize) -> Vec<Event> {
    let base_ms = 1_775_990_400_000i64;
    (0..n)
        .map(|i| {
            let dev = i % n_devices;
            let ts_ms = base_ms + (i as i64) * 10;
            let ts = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_ms).unwrap();
            Event::new("Reading")
                .with_timestamp(ts)
                .with_field("ts", Value::Int(ts_ms))
                .with_field("device_id", Value::Str(format!("dev_{dev}").into()))
                .with_field("temperature", Value::Float(20.0 + (i % 30) as f64 * 0.5))
        })
        .collect()
}

fn make_engine() -> Engine {
    let source = r"
        event Reading:
            ts: int
            device_id: str
            temperature: float

        stream DeviceAgg = Reading
            .partition_by(device_id)
            .window(1s)
            .aggregate(
                s: sum(temperature),
                a: avg(temperature),
                mn: min(temperature),
                mx: max(temperature)
            )
            .emit(device_id: device_id, s: s, a: a, mn: mn, mx: mx)
    ";
    let program = parse(source).expect("VPL parse failed");
    let mut engine = Engine::builder().build();
    engine.load(&program).expect("engine load failed");
    engine
}

fn bench_dispatch(c: &mut Criterion) {
    let configs = [
        ("10k_100dev", 10_000, 100),
        ("100k_100dev", 100_000, 100),
    ];

    let mut group = c.benchmark_group("dispatch");
    for (label, total, devices) in configs {
        let events = make_scenario_02_events(total, devices);

        // Measure process_batch_sync — synchronous, no async overhead.
        // This includes the full pipeline dispatch per event.
        group.bench_with_input(
            BenchmarkId::new("process_batch_sync", label),
            &events,
            |b, events| {
                b.iter_batched(
                    make_engine,
                    |mut engine| {
                        engine
                            .process_batch_sync(events.clone())
                            .expect("batch sync should not fail");
                        black_box(&engine);
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_dispatch);
criterion_main!(benches);
