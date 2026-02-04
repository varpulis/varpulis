//! Benchmarks for context-based multi-threaded execution.
//!
//! Compares performance of:
//! 1. Single-stream: direct engine vs context orchestrator overhead
//! 2. Parallel independent streams: single-threaded vs multi-context
//! 3. Cross-context pipeline: throughput through a chain of contexts
//!
//! Run with: cargo bench -p varpulis-runtime --bench context_benchmark

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::{ContextOrchestrator, Engine, Event};

/// Generate sensor reading events
fn generate_sensor_events(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    let sensors = ["S1", "S2", "S3", "S4", "S5"];
    for i in 0..count {
        let sensor = sensors[i % sensors.len()];
        let temperature = 50.0 + (i as f64 * 0.7) % 100.0;
        let event = Event::new("SensorReading")
            .with_field("sensor_id", sensor)
            .with_field("temperature", temperature)
            .with_field("humidity", 40.0 + (i as f64 * 0.3) % 60.0);
        events.push(event);
    }
    events
}

/// Generate events for two independent streams
fn generate_dual_stream_events(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count * 2);
    for i in 0..count {
        let temp = 50.0 + (i as f64 * 0.7) % 100.0;
        events.push(
            Event::new("TempReading")
                .with_field("sensor_id", format!("T{}", i % 10))
                .with_field("temperature", temp),
        );
        events.push(
            Event::new("PressureReading")
                .with_field("sensor_id", format!("P{}", i % 10))
                .with_field("pressure", 1000.0 + (i as f64 * 0.5) % 100.0),
        );
    }
    events
}

// =============================================================================
// Benchmark 1: Single-stream overhead (no_context vs with_context)
// =============================================================================

fn bench_single_stream_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_stream_overhead");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    let no_context_program = r#"
        stream HighTemp = SensorReading
            .where(temperature > 80.0)
            .emit(sensor: sensor_id, temp: temperature)
    "#;

    let with_context_program = r#"
        context ingest

        stream HighTemp = SensorReading
            .context(ingest)
            .where(temperature > 80.0)
            .emit(sensor: sensor_id, temp: temperature)
    "#;

    for batch_size in [100, 1000, 10000] {
        let events = generate_sensor_events(batch_size);
        group.throughput(Throughput::Elements(batch_size as u64));

        // No context: direct engine.process()
        group.bench_with_input(
            BenchmarkId::new("no_context", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, _rx) = mpsc::channel(batch_size + 100);
                        let program = parse(no_context_program).unwrap();
                        let mut engine = Engine::new(tx);
                        engine.load(&program).unwrap();

                        for event in events {
                            engine.process(event.clone()).await.unwrap();
                        }
                    });
                });
            },
        );

        // With context: orchestrator.process()
        group.bench_with_input(
            BenchmarkId::new("with_context", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, _rx) = mpsc::channel(batch_size + 100);
                        let program = parse(with_context_program).unwrap();

                        // Build temporary engine to get context_map
                        let (tmp_tx, _tmp_rx) = mpsc::channel(100);
                        let mut tmp_engine = Engine::new(tmp_tx);
                        tmp_engine.load(&program).unwrap();

                        let orchestrator = ContextOrchestrator::build(
                            tmp_engine.context_map(),
                            &program,
                            tx,
                            batch_size + 100,
                        )
                        .unwrap();

                        for event in events {
                            orchestrator.process(Arc::new(event.clone())).await.unwrap();
                        }

                        // Allow processing to complete
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        orchestrator.shutdown();
                    });
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 2: Parallel independent streams
// =============================================================================

fn bench_parallel_streams(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_streams");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    let sequential_program = r#"
        stream HighTemp = TempReading
            .where(temperature > 80.0)
            .emit(sensor: sensor_id, temp: temperature)

        stream HighPressure = PressureReading
            .where(pressure > 1050.0)
            .emit(sensor: sensor_id, press: pressure)
    "#;

    let parallel_program = r#"
        context temp_ctx
        context pressure_ctx

        stream HighTemp = TempReading
            .context(temp_ctx)
            .where(temperature > 80.0)
            .emit(sensor: sensor_id, temp: temperature)

        stream HighPressure = PressureReading
            .context(pressure_ctx)
            .where(pressure > 1050.0)
            .emit(sensor: sensor_id, press: pressure)
    "#;

    let batch_size = 10000;
    let events = generate_dual_stream_events(batch_size);
    group.throughput(Throughput::Elements(events.len() as u64));

    // Sequential: single engine
    group.bench_with_input(
        BenchmarkId::new("sequential", batch_size),
        &events,
        |b, events| {
            let rt = Runtime::new().unwrap();
            b.iter(|| {
                rt.block_on(async {
                    let (tx, _rx) = mpsc::channel(events.len() + 100);
                    let program = parse(sequential_program).unwrap();
                    let mut engine = Engine::new(tx);
                    engine.load(&program).unwrap();

                    for event in events {
                        engine.process(event.clone()).await.unwrap();
                    }
                });
            });
        },
    );

    // Parallel: two contexts
    group.bench_with_input(
        BenchmarkId::new("parallel", batch_size),
        &events,
        |b, events| {
            let rt = Runtime::new().unwrap();
            b.iter(|| {
                rt.block_on(async {
                    let (tx, _rx) = mpsc::channel(events.len() + 100);
                    let program = parse(parallel_program).unwrap();

                    let (tmp_tx, _tmp_rx) = mpsc::channel(100);
                    let mut tmp_engine = Engine::new(tmp_tx);
                    tmp_engine.load(&program).unwrap();

                    let orchestrator = ContextOrchestrator::build(
                        tmp_engine.context_map(),
                        &program,
                        tx,
                        events.len() + 100,
                    )
                    .unwrap();

                    for event in events {
                        orchestrator.process(Arc::new(event.clone())).await.unwrap();
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    orchestrator.shutdown();
                });
            });
        },
    );

    group.finish();
}

// =============================================================================
// Benchmark 3: Cross-context pipeline throughput
// =============================================================================

fn bench_cross_context_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_context_pipeline");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    let pipeline_program = r#"
        context ingest
        context compute
        context alert

        stream Raw = SensorReading
            .context(ingest)
            .where(temperature > 0.0)
            .emit(sensor: sensor_id, temp: temperature)

        stream Computed = Raw
            .context(compute)
            .where(temp > 50.0)
            .emit(device: sensor, value: temp)

        stream Alert = Computed
            .context(alert)
            .where(value > 80.0)
            .emit(critical_device: device, critical_value: value)
    "#;

    for batch_size in [100, 1000, 10000] {
        let events = generate_sensor_events(batch_size);
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("3_context_chain", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, _rx) = mpsc::channel(batch_size + 100);
                        let program = parse(pipeline_program).unwrap();

                        let (tmp_tx, _tmp_rx) = mpsc::channel(100);
                        let mut tmp_engine = Engine::new(tmp_tx);
                        tmp_engine.load(&program).unwrap();

                        let orchestrator = ContextOrchestrator::build(
                            tmp_engine.context_map(),
                            &program,
                            tx,
                            batch_size + 100,
                        )
                        .unwrap();

                        for event in events {
                            orchestrator.process(Arc::new(event.clone())).await.unwrap();
                        }

                        // Allow pipeline to drain through all 3 contexts
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        orchestrator.shutdown();
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_stream_overhead,
    bench_parallel_streams,
    bench_cross_context_pipeline,
);
criterion_main!(benches);
