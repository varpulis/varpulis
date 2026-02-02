//! Benchmarks for comparing Varpulis performance with Apama-style CEP workloads
//!
//! These benchmarks test end-to-end stream processing with realistic scenarios:
//! - Simple filter baseline (price > threshold)
//! - Windowed aggregation (VWAP calculation)
//! - Temporal sequence patterns (fraud detection)
//! - Complex patterns with Kleene+ and predicates
//!
//! Run with: cargo bench -p varpulis-runtime --bench comparison_benchmark

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use varpulis_core::Program;
use varpulis_parser::parse;
use varpulis_runtime::{Engine, Event};

fn parse_program(source: &str) -> Program {
    parse(source).expect("Failed to parse")
}

/// Generate stock tick events for benchmarking
fn generate_stock_ticks(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    let symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "META"];
    for i in 0..count {
        let symbol = symbols[i % symbols.len()];
        let price = 100.0 + (i as f64 * 0.1) % 50.0;
        let volume = (i * 100) % 10000;
        let event = Event::new("StockTick")
            .with_field("symbol", symbol)
            .with_field("price", price)
            .with_field("volume", volume as i64);
        events.push(event);
    }
    events
}

/// Generate trade events for VWAP benchmarking
fn generate_trades(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        let price = 100.0 + (i as f64 * 0.05) % 20.0;
        let volume = 100.0 + (i as f64 * 10.0) % 1000.0;
        let event = Event::new("Trade")
            .with_field("stock_name", "ACME")
            .with_field("price", price)
            .with_field("volume", volume);
        events.push(event);
    }
    events
}

/// Generate fraud detection events (Login and Transaction)
#[allow(dead_code)]
fn generate_fraud_events(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    let ips = ["192.168.1.1", "192.168.1.2", "10.0.0.1", "10.0.0.2"];
    for i in 0..count {
        let user_id = format!("user_{}", i % 100);
        if i % 3 == 0 {
            // Login event
            let ip = ips[i % ips.len()];
            let event = Event::new("Login")
                .with_field("user_id", user_id)
                .with_field("ip", ip);
            events.push(event);
        } else {
            // Transaction event
            let ip = ips[(i + 1) % ips.len()]; // Different IP to trigger alerts
            let amount = (i * 1000) as f64 % 50000.0;
            let event = Event::new("Transaction")
                .with_field("user_id", user_id)
                .with_field("amount", amount)
                .with_field("ip", ip);
            events.push(event);
        }
    }
    events
}

/// Generate sensor events for anomaly detection
fn generate_sensor_events(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        let sensor_id = format!("sensor_{}", i % 10);
        // Simulate normal values with occasional spikes
        let value = if i % 50 == 0 {
            150.0 + (i as f64 * 0.1) // Spike
        } else {
            50.0 + (i as f64 * 0.01) % 30.0 // Normal
        };
        let event = Event::new("SensorReading")
            .with_field("sensor_id", sensor_id)
            .with_field("value", value)
            .with_field("timestamp", i as i64);
        events.push(event);
    }
    events
}

// =============================================================================
// Benchmark 1: Simple Filter Baseline
// Comparable to Apama: on all StockTick(*, >50.0)
// =============================================================================

fn bench_simple_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("filter_baseline");

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        event FilteredTick:
            symbol: str
            price: float

        stream Filtered = StockTick
            .where(price > 50.0)
            .emit(
                event_type: "FilteredTick",
                symbol: symbol,
                price: price
            )
    "#;

    let program = parse_program(source);

    for size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = generate_stock_ticks(size);

        group.bench_with_input(
            BenchmarkId::new("filter_price", size),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, mut rx) = mpsc::channel(size * 2);
                        let mut engine = Engine::new(tx);
                        engine.load(&program).unwrap();
                        for event in events {
                            engine.process(black_box(event.clone())).await.unwrap();
                        }
                        // Drain output
                        while rx.try_recv().is_ok() {}
                    })
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 2: Windowed Aggregation (VWAP Style)
// Comparable to Apama: from t in trades retain 100 select sum(t.price * t.volume) / sum(t.volume)
// =============================================================================

fn bench_windowed_aggregation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("windowed_aggregation");
    group.sample_size(50);

    let source = r#"
        event Trade:
            stock_name: str
            price: float
            volume: float

        event VWAPUpdate:
            stock_name: str
            vwap: float

        stream VWAP = Trade
            .where(stock_name == "ACME")
            .window(100)
            .aggregate(
                stock_name: last(stock_name),
                total_pv: sum(price * volume),
                total_v: sum(volume)
            )
            .where(total_v > 0.0)
            .emit(
                event_type: "VWAPUpdate",
                stock_name: stock_name,
                vwap: total_pv / total_v
            )
    "#;

    let program = parse_program(source);

    for size in [1_000, 10_000, 50_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = generate_trades(size);

        group.bench_with_input(BenchmarkId::new("vwap_100", size), &events, |b, events| {
            b.iter(|| {
                rt.block_on(async {
                    let (tx, mut rx) = mpsc::channel(size);
                    let mut engine = Engine::new(tx);
                    engine.load(&program).unwrap();
                    for event in events {
                        engine.process(black_box(event.clone())).await.unwrap();
                    }
                    while rx.try_recv().is_ok() {}
                })
            })
        });
    }

    group.finish();
}

// =============================================================================
// Benchmark 3: Multiple Aggregations (Count, Avg, Stddev)
// Tests aggregate function performance
// =============================================================================

fn bench_multi_aggregate(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("multi_aggregate");
    group.sample_size(50);

    let source = r#"
        event SensorReading:
            sensor_id: str
            value: float
            timestamp: int

        event Stats:
            sensor_id: str
            count: int
            avg: float
            std: float
            min: float
            max: float

        stream SensorStats = SensorReading
            .partition_by(sensor_id)
            .window(50)
            .aggregate(
                sensor_id: last(sensor_id),
                reading_count: count(value),
                avg_value: avg(value),
                std_value: stddev(value),
                min_value: min(value),
                max_value: max(value)
            )
            .emit(
                event_type: "Stats",
                sensor_id: sensor_id,
                count: reading_count,
                avg: avg_value,
                std: std_value,
                min: min_value,
                max: max_value
            )
    "#;

    let program = parse_program(source);

    for size in [1_000, 10_000, 50_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = generate_sensor_events(size);

        group.bench_with_input(
            BenchmarkId::new("stats_window_50", size),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, mut rx) = mpsc::channel(size);
                        let mut engine = Engine::new(tx);
                        engine.load(&program).unwrap();
                        for event in events {
                            engine.process(black_box(event.clone())).await.unwrap();
                        }
                        while rx.try_recv().is_ok() {}
                    })
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 4: Filter + Aggregation Pipeline
// Combined workload similar to Apama benchmarks
// =============================================================================

fn bench_filter_aggregate_pipeline(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("filter_aggregate_pipeline");
    group.sample_size(50);

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        event HighVolumeAvg:
            symbol: str
            avg_price: float
            total_volume: int

        stream HighVolumeStats = StockTick
            .where(volume > 500)
            .partition_by(symbol)
            .window(20)
            .aggregate(
                symbol: last(symbol),
                avg_price: avg(price),
                total_volume: sum(volume)
            )
            .emit(
                event_type: "HighVolumeAvg",
                symbol: symbol,
                avg_price: avg_price,
                total_volume: total_volume
            )
    "#;

    let program = parse_program(source);

    for size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = generate_stock_ticks(size);

        group.bench_with_input(
            BenchmarkId::new("filter_agg", size),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, mut rx) = mpsc::channel(size);
                        let mut engine = Engine::new(tx);
                        engine.load(&program).unwrap();
                        for event in events {
                            engine.process(black_box(event.clone())).await.unwrap();
                        }
                        while rx.try_recv().is_ok() {}
                    })
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 5: Anomaly Detection with Threshold
// Similar to Apama sensor monitoring
// =============================================================================

fn bench_anomaly_detection(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("anomaly_detection");
    group.sample_size(50);

    let source = r#"
        event SensorReading:
            sensor_id: str
            value: float
            timestamp: int

        event AnomalyAlert:
            sensor_id: str
            value: float
            threshold: float

        fn is_anomaly(val: float, threshold: float) -> bool:
            return val > threshold

        stream Anomalies = SensorReading
            .where(is_anomaly(value, 100.0))
            .emit(
                event_type: "AnomalyAlert",
                sensor_id: sensor_id,
                value: value,
                threshold: 100.0
            )
    "#;

    let program = parse_program(source);

    for size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = generate_sensor_events(size);

        group.bench_with_input(
            BenchmarkId::new("threshold_check", size),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, mut rx) = mpsc::channel(size);
                        let mut engine = Engine::new(tx);
                        engine.load(&program).unwrap();
                        for event in events {
                            engine.process(black_box(event.clone())).await.unwrap();
                        }
                        while rx.try_recv().is_ok() {}
                    })
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 6: Complex UDF with Imperative Logic
// Tests user-defined function performance in stream processing
// =============================================================================

fn bench_complex_udf(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("complex_udf");
    group.sample_size(50);

    let source = r#"
        event Trade:
            stock_name: str
            price: float
            volume: float

        event TradeAnalysis:
            stock_name: str
            category: str
            score: float

        fn categorize_trade(price: float, volume: float) -> str:
            let value = price * volume
            if value > 50000.0:
                return "large"
            elif value > 10000.0:
                return "medium"
            else:
                return "small"

        fn calculate_score(price: float, volume: float) -> float:
            let base = price * volume / 1000.0
            let bonus = 0.0
            if price > 110.0:
                bonus := 10.0
            if volume > 500.0:
                bonus := bonus + 5.0
            return base + bonus

        stream Analysis = Trade
            .emit(
                event_type: "TradeAnalysis",
                stock_name: stock_name,
                category: categorize_trade(price, volume),
                score: calculate_score(price, volume)
            )
    "#;

    let program = parse_program(source);

    for size in [1_000, 10_000, 50_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = generate_trades(size);

        group.bench_with_input(
            BenchmarkId::new("udf_analysis", size),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, mut rx) = mpsc::channel(size);
                        let mut engine = Engine::new(tx);
                        engine.load(&program).unwrap();
                        for event in events {
                            engine.process(black_box(event.clone())).await.unwrap();
                        }
                        while rx.try_recv().is_ok() {}
                    })
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 7: Multi-Stream Processing
// Tests handling multiple concurrent streams
// =============================================================================

fn bench_multi_stream(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("multi_stream");
    group.sample_size(30);

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        event HighPrice:
            symbol: str
            price: float

        event HighVolume:
            symbol: str
            volume: int

        event Combined:
            symbol: str
            metric: str
            value: float

        # Stream 1: High price alerts
        stream PriceAlerts = StockTick
            .where(price > 120.0)
            .emit(
                event_type: "HighPrice",
                symbol: symbol,
                price: price
            )

        # Stream 2: High volume alerts
        stream VolumeAlerts = StockTick
            .where(volume > 5000)
            .emit(
                event_type: "HighVolume",
                symbol: symbol,
                volume: volume
            )

        # Stream 3: Windowed averages
        stream Averages = StockTick
            .partition_by(symbol)
            .window(10)
            .aggregate(
                symbol: last(symbol),
                avg_price: avg(price)
            )
            .emit(
                event_type: "Combined",
                symbol: symbol,
                metric: "avg_price",
                value: avg_price
            )
    "#;

    let program = parse_program(source);

    for size in [1_000, 10_000, 50_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = generate_stock_ticks(size);

        group.bench_with_input(
            BenchmarkId::new("three_streams", size),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, mut rx) = mpsc::channel(size * 3);
                        let mut engine = Engine::new(tx);
                        engine.load(&program).unwrap();
                        for event in events {
                            engine.process(black_box(event.clone())).await.unwrap();
                        }
                        while rx.try_recv().is_ok() {}
                    })
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Benchmark 8: Scalability Test
// Large event volumes to measure throughput ceiling
// =============================================================================

fn bench_scalability(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scalability");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    // Simple filter for maximum throughput
    let simple_source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        event Filtered:
            symbol: str

        stream Fast = StockTick
            .where(price > 50.0)
            .emit(event_type: "Filtered", symbol: symbol)
    "#;

    let simple_program = parse_program(simple_source);
    let events_100k = generate_stock_ticks(100_000);

    group.throughput(Throughput::Elements(100_000));
    group.bench_function("filter_100k", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100_000);
                let mut engine = Engine::new(tx);
                engine.load(&simple_program).unwrap();
                for event in &events_100k {
                    engine.process(black_box(event.clone())).await.unwrap();
                }
                while rx.try_recv().is_ok() {}
            })
        })
    });

    // Aggregation for complex throughput
    let agg_source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        event Agg:
            avg: float

        stream Aggregated = StockTick
            .window(100)
            .aggregate(avg_price: avg(price))
            .emit(event_type: "Agg", avg: avg_price)
    "#;

    let agg_program = parse_program(agg_source);

    group.bench_function("aggregate_100k", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100_000);
                let mut engine = Engine::new(tx);
                engine.load(&agg_program).unwrap();
                for event in &events_100k {
                    engine.process(black_box(event.clone())).await.unwrap();
                }
                while rx.try_recv().is_ok() {}
            })
        })
    });

    group.finish();
}

// =============================================================================
// Benchmark 9: Parse + Load Time
// Measures compilation overhead
// =============================================================================

fn bench_parse_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_load");

    // Small program
    let small_source = r#"
        event Input:
            value: int

        stream Output = Input
            .where(value > 0)
            .emit(value: value)
    "#;

    group.bench_function("small_program", |b| {
        b.iter(|| {
            let program = parse(black_box(small_source)).unwrap();
            black_box(program)
        })
    });

    // Medium program with UDF
    let medium_source = r#"
        event Trade:
            symbol: str
            price: float
            volume: float

        event Analysis:
            category: str
            score: float

        fn categorize(price: float) -> str:
            if price > 100.0:
                return "high"
            else:
                return "low"

        fn score(price: float, volume: float) -> float:
            return price * volume / 1000.0

        stream Analyzed = Trade
            .where(price > 50.0)
            .window(50)
            .aggregate(
                avg_price: avg(price),
                total_vol: sum(volume)
            )
            .emit(
                event_type: "Analysis",
                category: categorize(avg_price),
                score: score(avg_price, total_vol)
            )
    "#;

    group.bench_function("medium_program", |b| {
        b.iter(|| {
            let program = parse(black_box(medium_source)).unwrap();
            black_box(program)
        })
    });

    // Large program with multiple streams
    let large_source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        event Trade:
            symbol: str
            price: float
            volume: float

        event Alert1:
            symbol: str

        event Alert2:
            symbol: str

        event Alert3:
            avg: float

        fn is_high_value(price: float, volume: float) -> bool:
            return price * volume > 10000.0

        fn calculate_metric(price: float, volume: float) -> float:
            let base = price * volume
            let factor = 1.0
            if price > 100.0:
                factor := 1.5
            if volume > 500.0:
                factor := factor * 1.2
            return base * factor

        stream HighPrice = StockTick
            .where(price > 100.0)
            .emit(event_type: "Alert1", symbol: symbol)

        stream HighVolume = StockTick
            .where(volume > 5000)
            .emit(event_type: "Alert2", symbol: symbol)

        stream Averages = StockTick
            .partition_by(symbol)
            .window(50)
            .aggregate(avg_price: avg(price))
            .emit(event_type: "Alert3", avg: avg_price)

        stream ValueTrades = Trade
            .where(is_high_value(price, volume))
            .emit(event_type: "Alert1", symbol: symbol)
    "#;

    group.bench_function("large_program", |b| {
        b.iter(|| {
            let program = parse(black_box(large_source)).unwrap();
            black_box(program)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_filter,
    bench_windowed_aggregation,
    bench_multi_aggregate,
    bench_filter_aggregate_pipeline,
    bench_anomaly_detection,
    bench_complex_udf,
    bench_multi_stream,
    bench_scalability,
    bench_parse_load,
);

criterion_main!(benches);
