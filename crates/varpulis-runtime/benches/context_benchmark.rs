//! Benchmarks for context-based multi-threaded execution.
//!
//! Compares performance of:
//! 1. Single-stream: direct engine vs context orchestrator overhead
//! 2. Parallel independent streams: single-threaded vs multi-context
//! 3. Cross-context pipeline: throughput through a chain of contexts
//! 4. CPU-intensive parallel: windowed aggregation to show parallelism benefit
//! 5. Dispatch methods: process() vs try_process() vs router.dispatch()
//!
//! VPL programs are loaded from `benchmarks/context/*.vpl` files.
//!
//! Run with: cargo bench -p varpulis-runtime --bench context_benchmark

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use varpulis_core::ast::Program;
use varpulis_parser::parse;
use varpulis_runtime::{ContextOrchestrator, DispatchError, Engine, Event};

/// Load and parse a VPL program from `benchmarks/context/`.
fn load_vpl(filename: &str) -> Program {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../benchmarks/context")
        .join(filename);
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to load {}: {}", path.display(), e));
    parse(&source).unwrap_or_else(|e| panic!("Failed to parse {}: {:?}", path.display(), e))
}

/// Drain the output channel with a brief sleep then non-blocking recv.
///
/// Replaces the old timeout-per-event approach which added 3-5ms constant
/// overhead per iteration. This version sleeps once to let context threads
/// flush, then drains everything available without blocking.
async fn drain_output(rx: &mut mpsc::Receiver<Event>) -> usize {
    tokio::time::sleep(Duration::from_millis(1)).await;
    let mut count = 0;
    while rx.try_recv().is_ok() {
        count += 1;
    }
    count
}

/// Drain for cross-context pipelines where events traverse multiple async hops.
///
/// Retries a few times with short sleeps instead of using a long deadline,
/// so small batches that finish quickly don't pay a constant 50ms floor.
async fn drain_output_pipeline(rx: &mut mpsc::Receiver<Event>) -> usize {
    let mut count = 0;
    // Retry up to 5 times with 1ms gaps — total max overhead is ~5ms
    // instead of a fixed 50ms deadline that dominates small batches.
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(1)).await;
        let mut got_any = false;
        while rx.try_recv().is_ok() {
            count += 1;
            got_any = true;
        }
        if !got_any && count > 0 {
            // Had events before but nothing new — pipeline has drained
            break;
        }
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

/// Generate sensor events with timestamps that create session patterns.
///
/// Events arrive in bursts of `burst_size` within 1s gaps, separated by
/// `gap_seconds` pauses. This creates predictable session boundaries for
/// benchmarking session window closure overhead.
fn generate_session_events(count: usize, burst_size: usize, gap_seconds: u64) -> Vec<Event> {
    use chrono::{Duration as ChronoDuration, Utc};
    let mut events = Vec::with_capacity(count);
    let sensors = ["S1", "S2", "S3", "S4", "S5"];
    let base_time = Utc::now();
    let mut current_time = base_time;

    for i in 0..count {
        let sensor = sensors[i % sensors.len()];
        let temperature = 50.0 + (i as f64 * 0.7) % 100.0;
        let event = Event::new("SensorReading")
            .with_timestamp(current_time)
            .with_field("sensor_id", sensor)
            .with_field("temperature", temperature);
        events.push(event);

        // Within a burst: 1s between events. At burst boundary: gap_seconds gap.
        if (i + 1) % burst_size == 0 {
            current_time = current_time + ChronoDuration::seconds(gap_seconds as i64);
        } else {
            current_time = current_time + ChronoDuration::seconds(1);
        }
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

    let no_ctx_program = load_vpl("single_stream_no_context.vpl");
    let ctx_program = load_vpl("single_stream_with_context.vpl");

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
                            drain_output(&mut rx).await;
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
// orchestrator. Parallel benchmark uses multi-producer dispatch to feed
// both context threads simultaneously.
// =============================================================================

fn bench_parallel_streams(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_streams");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let seq_program = load_vpl("parallel_sequential.vpl");
    let par_program = load_vpl("parallel_contexts.vpl");

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

        // Parallel: two contexts with multi-producer dispatch
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

                            // Split events by type for concurrent dispatch
                            let (temp_events, press_events): (Vec<_>, Vec<_>) =
                                events.iter().partition(|e| e.event_type == "TempReading");

                            let router = orchestrator.router();

                            let start = Instant::now();

                            let r1 = router.clone();
                            let temp_owned: Vec<Event> = temp_events.into_iter().cloned().collect();
                            let h1 = tokio::spawn(async move {
                                for e in temp_owned {
                                    r1.dispatch_await(Arc::new(e)).await.ok();
                                }
                            });

                            let r2 = router.clone();
                            let press_owned: Vec<Event> =
                                press_events.into_iter().cloned().collect();
                            let h2 = tokio::spawn(async move {
                                for e in press_owned {
                                    r2.dispatch_await(Arc::new(e)).await.ok();
                                }
                            });

                            h1.await.unwrap();
                            h2.await.unwrap();

                            drain_output(&mut rx).await;
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
// Uses deadline-based drain since events traverse multiple async hops.
// =============================================================================

fn bench_cross_context_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_context_pipeline");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let pipeline_program = load_vpl("cross_context_pipeline.vpl");

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
                            drain_output_pipeline(&mut rx).await;
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
// Uses multi-producer dispatch to feed both context threads simultaneously.
// =============================================================================

fn bench_cpu_intensive_parallel(c: &mut Criterion) {
    let mut group = c.benchmark_group("cpu_intensive_parallel");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(10));

    let seq_program = load_vpl("cpu_intensive_sequential.vpl");
    let par_program = load_vpl("cpu_intensive_parallel.vpl");

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

        // Parallel: each stream in its own context, multi-producer dispatch
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

                            // Split events by type for concurrent dispatch
                            let (temp_events, press_events): (Vec<_>, Vec<_>) =
                                events.iter().partition(|e| e.event_type == "TempReading");

                            let router = orchestrator.router();

                            let start = Instant::now();

                            let r1 = router.clone();
                            let temp_owned: Vec<Event> = temp_events.into_iter().cloned().collect();
                            let h1 = tokio::spawn(async move {
                                for e in temp_owned {
                                    r1.dispatch_await(Arc::new(e)).await.ok();
                                }
                            });

                            let r2 = router.clone();
                            let press_owned: Vec<Event> =
                                press_events.into_iter().cloned().collect();
                            let h2 = tokio::spawn(async move {
                                for e in press_owned {
                                    r2.dispatch_await(Arc::new(e)).await.ok();
                                }
                            });

                            h1.await.unwrap();
                            h2.await.unwrap();

                            drain_output(&mut rx).await;
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
// Benchmark 5: Dispatch method comparison
//
// Compares: process() (async) vs try_process() (non-blocking) vs
// router.dispatch() (direct non-blocking). Shows the speedup from
// eliminating async overhead.
// =============================================================================

fn bench_dispatch_methods(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch_methods");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let ctx_program = load_vpl("single_stream_with_context.vpl");

    for batch_size in [1000, 10000] {
        let events = generate_sensor_events(batch_size);
        group.throughput(Throughput::Elements(batch_size as u64));

        // Async process() — current default, waits on backpressure
        group.bench_with_input(
            BenchmarkId::new("process_async", batch_size),
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
                            drain_output(&mut rx).await;
                            total += start.elapsed();

                            orchestrator.shutdown();
                        }
                        total
                    })
                });
            },
        );

        // try_process() — non-blocking with async fallback
        group.bench_with_input(
            BenchmarkId::new("try_process", batch_size),
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
                                let shared = Arc::new(event.clone());
                                match orchestrator.try_process(shared) {
                                    Ok(()) => {}
                                    Err(DispatchError::ChannelFull(ev)) => {
                                        orchestrator.process(ev).await.unwrap();
                                    }
                                    Err(DispatchError::ChannelClosed(_)) => break,
                                }
                            }
                            drain_output(&mut rx).await;
                            total += start.elapsed();

                            orchestrator.shutdown();
                        }
                        total
                    })
                });
            },
        );

        // router.dispatch() — direct non-blocking via cloned router handle
        group.bench_with_input(
            BenchmarkId::new("router_dispatch", batch_size),
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

                            let router = orchestrator.router();

                            let start = Instant::now();
                            for event in events {
                                let shared = Arc::new(event.clone());
                                match router.dispatch(shared) {
                                    Ok(()) => {}
                                    Err(DispatchError::ChannelFull(ev)) => {
                                        router.dispatch_await(ev).await.unwrap();
                                    }
                                    Err(DispatchError::ChannelClosed(_)) => break,
                                }
                            }
                            drain_output(&mut rx).await;
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
// Benchmark 6: Session window overhead (no_context vs with_context)
//
// Partitioned session windows with aggregation. Events arrive in bursts
// separated by gaps that trigger session closures. Measures the overhead
// of context orchestration for stateful session processing.
// =============================================================================

fn bench_session_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("session_window");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(10));

    let no_ctx_program = load_vpl("session_window_no_context.vpl");
    let ctx_program = load_vpl("session_window_with_context.vpl");

    // burst_size=20 events per session, gap=6s (exceeds 5s session gap)
    for batch_size in [500, 2000, 10000] {
        let events = generate_session_events(batch_size, 20, 6);
        group.throughput(Throughput::Elements(batch_size as u64));

        // No context: direct engine.process()
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

        // With context: orchestrator.process()
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
                            drain_output(&mut rx).await;
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
// Benchmark 7: Session window sweep overhead
//
// Measures the cost of periodic sweep for stale session closure.
// Compares processing with sessions that close via gap detection (event-driven)
// vs sessions that close via sweep (no trailing event). Ensures the sweep
// mechanism doesn't add significant overhead to the normal event-driven path.
// =============================================================================

fn bench_session_window_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("session_window_sweep");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(10));

    let no_ctx_program = load_vpl("session_window_no_context.vpl");

    // burst_size=20 events per session, gap=6s (exceeds 5s session gap)
    for batch_size in [500, 2000, 10000] {
        let events = generate_session_events(batch_size, 20, 6);
        group.throughput(Throughput::Elements(batch_size as u64));

        // Baseline: sessions close via gap detection (event-driven, no sweep needed)
        group.bench_with_input(
            BenchmarkId::new("event_driven", batch_size),
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

        // With sweep: process all events, then call flush_expired_sessions()
        // to close any remaining stale sessions
        group.bench_with_input(
            BenchmarkId::new("with_sweep", batch_size),
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
                            engine.flush_expired_sessions().await.unwrap();
                            total += start.elapsed();
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
    bench_dispatch_methods,
    bench_session_window,
    bench_session_window_sweep,
);
criterion_main!(benches);
