//! Benchmarks for context-based multi-threaded execution.
//!
//! Compares performance of:
//! 1. Single-stream: direct engine vs context orchestrator overhead
//! 2. Parallel independent streams: single-threaded vs multi-context
//! 3. Cross-context pipeline: throughput through a chain of contexts
//! 4. CPU-intensive parallel: windowed aggregation to show parallelism benefit
//!
//! Run with: cargo bench -p varpulis-runtime --bench context_benchmark

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::{ContextOrchestrator, Engine, Event};

/// Drain the output channel until no events arrive for `quiet_ms`.
/// Returns the number of events drained.
async fn drain_output(rx: &mut mpsc::Receiver<Event>, quiet_ms: u64) -> usize {
    let mut count = 0;
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(quiet_ms), rx.recv()).await {
        count += 1;
    }
    count
}

/// Generate sensor reading events with deterministic temperature distribution.
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

/// Generate events for two independent streams (temp + pressure).
fn generate_dual_stream_events(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count * 2);
    for i in 0..count {
        let temp = 50.0 + (i as f64 * 0.7) % 100.0;
        events.push(
            Event::new("TempReading")
                .with_field("sensor_id", format!("T{}", i % 10))
                .with_field("temperature", temp)
                .with_field("humidity", 40.0 + (i as f64 * 0.3) % 60.0),
        );
        events.push(
            Event::new("PressureReading")
                .with_field("sensor_id", format!("P{}", i % 10))
                .with_field("pressure", 1000.0 + (i as f64 * 0.5) % 100.0)
                .with_field("altitude", 100.0 + (i as f64 * 0.2) % 50.0),
        );
    }
    events
}

// =============================================================================
// Benchmark 1: Single-stream overhead (no_context vs with_context)
//
// Uses iter_custom to time only event processing, excluding setup/teardown.
// Drains output channel instead of sleeping.
// =============================================================================

fn bench_single_stream_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_stream_overhead");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let no_context_source = r#"
        stream HighTemp = SensorReading
            .where(temperature > 80.0)
            .emit(sensor: sensor_id, temp: temperature)
    "#;

    let with_context_source = r#"
        context ingest

        stream HighTemp = SensorReading
            .context(ingest)
            .where(temperature > 80.0)
            .emit(sensor: sensor_id, temp: temperature)
    "#;

    // Pre-parse programs (reused across iterations)
    let no_ctx_program = parse(no_context_source).unwrap();
    let ctx_program = parse(with_context_source).unwrap();

    for batch_size in [100, 1000, 10000] {
        let events = generate_sensor_events(batch_size);
        group.throughput(Throughput::Elements(batch_size as u64));

        // No context: direct engine.process() — time only the processing loop
        group.bench_with_input(
            BenchmarkId::new("no_context", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (tx, _rx) = mpsc::channel(batch_size * 2);
                            let mut engine = Engine::new(tx);
                            engine.load(&no_ctx_program).unwrap();

                            let start = Instant::now();
                            for event in events {
                                engine.process(event.clone()).await.unwrap();
                            }
                            total += start.elapsed();
                        }
                        total
                    })
                });
            },
        );

        // With context: orchestrator.process() — time send + drain
        group.bench_with_input(
            BenchmarkId::new("with_context", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (tx, mut rx) = mpsc::channel(batch_size * 2);
                            let (tmp_tx, _tmp_rx) = mpsc::channel(100);
                            let mut tmp_engine = Engine::new(tmp_tx);
                            tmp_engine.load(&ctx_program).unwrap();

                            let orchestrator = ContextOrchestrator::build(
                                tmp_engine.context_map(),
                                &ctx_program,
                                tx,
                                batch_size * 2,
                            )
                            .unwrap();

                            let start = Instant::now();
                            for event in events {
                                orchestrator.process(Arc::new(event.clone())).await.unwrap();
                            }
                            drain_output(&mut rx, 3).await;
                            total += start.elapsed();

                            orchestrator.shutdown();
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 2: Parallel independent streams
//
// Lightweight filter+emit workload. Compares single engine vs 2-context
// orchestrator. Setup/teardown excluded from timing.
// =============================================================================

fn bench_parallel_streams(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_streams");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let sequential_source = r#"
        stream HighTemp = TempReading
            .where(temperature > 80.0)
            .emit(sensor: sensor_id, temp: temperature)

        stream HighPressure = PressureReading
            .where(pressure > 1050.0)
            .emit(sensor: sensor_id, press: pressure)
    "#;

    let parallel_source = r#"
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

    let seq_program = parse(sequential_source).unwrap();
    let par_program = parse(parallel_source).unwrap();

    for batch_size in [1000, 10000, 50000] {
        let events = generate_dual_stream_events(batch_size);
        group.throughput(Throughput::Elements(events.len() as u64));

        // Sequential: single engine
        group.bench_with_input(
            BenchmarkId::new("sequential", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (tx, _rx) = mpsc::channel(events.len() * 2);
                            let mut engine = Engine::new(tx);
                            engine.load(&seq_program).unwrap();

                            let start = Instant::now();
                            for event in events {
                                engine.process(event.clone()).await.unwrap();
                            }
                            total += start.elapsed();
                        }
                        total
                    })
                });
            },
        );

        // Parallel: two contexts
        group.bench_with_input(
            BenchmarkId::new("parallel", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (tx, mut rx) = mpsc::channel(events.len() * 2);
                            let (tmp_tx, _tmp_rx) = mpsc::channel(100);
                            let mut tmp_engine = Engine::new(tmp_tx);
                            tmp_engine.load(&par_program).unwrap();

                            let orchestrator = ContextOrchestrator::build(
                                tmp_engine.context_map(),
                                &par_program,
                                tx,
                                events.len() * 2,
                            )
                            .unwrap();

                            let start = Instant::now();
                            for event in events {
                                orchestrator.process(Arc::new(event.clone())).await.unwrap();
                            }
                            drain_output(&mut rx, 3).await;
                            total += start.elapsed();

                            orchestrator.shutdown();
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 3: Cross-context pipeline throughput
//
// 3-context chain: ingest -> compute -> alert.
// Measures end-to-end latency including cross-context channel hops.
// =============================================================================

fn bench_cross_context_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_context_pipeline");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let pipeline_source = r#"
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

    let pipeline_program = parse(pipeline_source).unwrap();

    for batch_size in [100, 1000, 10000] {
        let events = generate_sensor_events(batch_size);
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("3_context_chain", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (tx, mut rx) = mpsc::channel(batch_size * 4);
                            let (tmp_tx, _tmp_rx) = mpsc::channel(100);
                            let mut tmp_engine = Engine::new(tmp_tx);
                            tmp_engine.load(&pipeline_program).unwrap();

                            let orchestrator = ContextOrchestrator::build(
                                tmp_engine.context_map(),
                                &pipeline_program,
                                tx,
                                batch_size * 4,
                            )
                            .unwrap();

                            let start = Instant::now();
                            for event in events {
                                orchestrator.process(Arc::new(event.clone())).await.unwrap();
                            }
                            // Drain all 3 stages of output; use slightly longer quiet
                            // period since events must traverse 3 async hops
                            drain_output(&mut rx, 5).await;
                            total += start.elapsed();

                            orchestrator.shutdown();
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 4: CPU-intensive parallel workload
//
// Windowed aggregation with partition_by and multiple aggregate functions.
// This forces real per-event computation so parallelism can pay off.
// =============================================================================

fn bench_cpu_intensive_parallel(c: &mut Criterion) {
    let mut group = c.benchmark_group("cpu_intensive_parallel");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(10));

    // Sequential: both heavy streams in a single engine
    let sequential_source = r#"
        stream TempStats = TempReading
            .partition_by(sensor_id)
            .window(10)
            .aggregate(
                sensor_id: last(sensor_id),
                avg_temp: avg(temperature),
                min_temp: min(temperature),
                max_temp: max(temperature),
                reading_count: count()
            )
            .where(avg_temp > 60.0)
            .emit(
                sensor: sensor_id,
                average: avg_temp,
                minimum: min_temp,
                maximum: max_temp,
                count: reading_count
            )

        stream PressureStats = PressureReading
            .partition_by(sensor_id)
            .window(10)
            .aggregate(
                sensor_id: last(sensor_id),
                avg_press: avg(pressure),
                min_press: min(pressure),
                max_press: max(pressure),
                reading_count: count()
            )
            .where(avg_press > 1020.0)
            .emit(
                sensor: sensor_id,
                average: avg_press,
                minimum: min_press,
                maximum: max_press,
                count: reading_count
            )
    "#;

    // Parallel: each heavy stream in its own context thread
    let parallel_source = r#"
        context temp_ctx
        context pressure_ctx

        stream TempStats = TempReading
            .context(temp_ctx)
            .partition_by(sensor_id)
            .window(10)
            .aggregate(
                sensor_id: last(sensor_id),
                avg_temp: avg(temperature),
                min_temp: min(temperature),
                max_temp: max(temperature),
                reading_count: count()
            )
            .where(avg_temp > 60.0)
            .emit(
                sensor: sensor_id,
                average: avg_temp,
                minimum: min_temp,
                maximum: max_temp,
                count: reading_count
            )

        stream PressureStats = PressureReading
            .context(pressure_ctx)
            .partition_by(sensor_id)
            .window(10)
            .aggregate(
                sensor_id: last(sensor_id),
                avg_press: avg(pressure),
                min_press: min(pressure),
                max_press: max(pressure),
                reading_count: count()
            )
            .where(avg_press > 1020.0)
            .emit(
                sensor: sensor_id,
                average: avg_press,
                minimum: min_press,
                maximum: max_press,
                count: reading_count
            )
    "#;

    let seq_program = parse(sequential_source).unwrap();
    let par_program = parse(parallel_source).unwrap();

    for batch_size in [1000, 5000, 20000] {
        let events = generate_dual_stream_events(batch_size);
        group.throughput(Throughput::Elements(events.len() as u64));

        // Sequential: single engine processes both streams
        group.bench_with_input(
            BenchmarkId::new("sequential", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (tx, _rx) = mpsc::channel(events.len() * 2);
                            let mut engine = Engine::new(tx);
                            engine.load(&seq_program).unwrap();

                            let start = Instant::now();
                            for event in events {
                                engine.process(event.clone()).await.unwrap();
                            }
                            total += start.elapsed();
                        }
                        total
                    })
                });
            },
        );

        // Parallel: each stream in its own context
        group.bench_with_input(
            BenchmarkId::new("parallel", batch_size),
            &events,
            |b, events| {
                let rt = Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (tx, mut rx) = mpsc::channel(events.len() * 2);
                            let (tmp_tx, _tmp_rx) = mpsc::channel(100);
                            let mut tmp_engine = Engine::new(tmp_tx);
                            tmp_engine.load(&par_program).unwrap();

                            let orchestrator = ContextOrchestrator::build(
                                tmp_engine.context_map(),
                                &par_program,
                                tx,
                                events.len() * 2,
                            )
                            .unwrap();

                            let start = Instant::now();
                            for event in events {
                                orchestrator.process(Arc::new(event.clone())).await.unwrap();
                            }
                            drain_output(&mut rx, 5).await;
                            total += start.elapsed();

                            orchestrator.shutdown();
                        }
                        total
                    })
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
    bench_cpu_intensive_parallel,
);
criterion_main!(benches);
