#![allow(missing_docs)]
//! Benchmark: Varpulis vs EventFlux comparison
//!
//! Tests workloads that both engines support (Tier 1) and workloads only
//! Varpulis can handle (Tier 2). EventFlux equivalent queries are documented
//! in benchmarks/eventflux-comparison/README.md for reproduction.
//!
//! Run with: cargo bench -p varpulis-runtime --bench eventflux_comparison
//!
//! Tier 1 (Common Ground):
//!   - S1: Simple filter (price > 50.0)
//!   - S2: Tumbling window aggregation (count/sum/avg)
//!   - S3: Partitioned aggregation (group by symbol)
//!   - S4: Simple sequence (A -> B)
//!   - S5: Sequence with predicate (A[value>500] -> B)
//!   - S7: Multi-stream pipeline (3 concurrent streams)
//!   - S8: Filter + aggregate pipeline
//!   - S9: UDF evaluation
//!   - S10: Parse + load time
//!
//! Tier 2 (Varpulis Only):
//!   - S11: Kleene+ exhaustive (ZDD-backed)
//!   - S12: Kleene* (zero-or-more)
//!   - S13: Negation (NOT pattern)
//!   - S16: Nested Kleene + OR

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;
use varpulis_core::Program;
use varpulis_parser::parse;
use varpulis_runtime::event::Event;
use varpulis_runtime::sase::{CompareOp, Predicate, SaseEngine, SasePattern, SelectionStrategy};
use varpulis_runtime::Engine;

fn parse_program(source: &str) -> Program {
    parse(source).expect("Failed to parse")
}

// =============================================================================
// Event generators
// =============================================================================

/// Stock ticks with realistic distribution across 5 symbols
fn gen_stock_ticks(count: usize) -> Vec<Event> {
    let symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "META"];
    (0..count)
        .map(|i| {
            Event::new("StockTick")
                .with_field("symbol", symbols[i % symbols.len()])
                .with_field("price", 20.0 + (i as f64 * 0.37) % 180.0)
                .with_field("volume", ((i * 137 + 42) % 10000) as i64)
        })
        .collect()
}

/// Alternating Order/Payment events for sequence detection
#[allow(dead_code)]
fn gen_order_payment(count: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        if i % 2 == 0 {
            events.push(
                Event::new("Order")
                    .with_field("user_id", format!("user_{}", i / 2 % 100))
                    .with_field("amount", 50.0 + (i as f64 * 7.3) % 2000.0)
                    .with_field("category", format!("cat_{}", i % 5)),
            );
        } else {
            events.push(
                Event::new("Payment")
                    .with_field("user_id", format!("user_{}", (i - 1) / 2 % 100))
                    .with_field("amount", 50.0 + ((i - 1) as f64 * 7.3) % 2000.0)
                    .with_field("status", "completed"),
            );
        }
    }
    events
}

/// Mixed event types for pattern matching: A, B, C, D in configurable ratios
fn gen_typed_events(count: usize, types: &[&str]) -> Vec<Event> {
    (0..count)
        .map(|i| {
            let etype = types[i % types.len()];
            Event::new(etype)
                .with_field("id", i as i64)
                .with_field("value", ((i * 13 + 7) % 1000) as i64)
                .with_field("user_id", format!("user_{}", i % 50))
        })
        .collect()
}

/// Login/Action/Logout/Error events for negation patterns
fn gen_session_events(count: usize) -> Vec<Event> {
    let types = ["Login", "Action", "Action", "Logout", "Error"];
    (0..count)
        .map(|i| {
            let etype = types[i % types.len()];
            Event::new(etype)
                .with_field("user_id", format!("user_{}", i % 20))
                .with_field("session_id", format!("sess_{}", i / 5))
        })
        .collect()
}

/// Kleene stress events: Start -> Middle{n} -> End sequences
fn gen_kleene_sequences(num_sequences: usize, middles_per_seq: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(num_sequences * (middles_per_seq + 2));
    for seq in 0..num_sequences {
        events.push(
            Event::new("A")
                .with_field("id", (seq * 1000) as i64)
                .with_field("value", (seq * 100) as i64),
        );
        for m in 0..middles_per_seq {
            events.push(
                Event::new("B")
                    .with_field("id", (seq * 1000 + m + 1) as i64)
                    .with_field("value", (seq * 100 + m * 10) as i64),
            );
        }
        events.push(
            Event::new("C")
                .with_field("id", (seq * 1000 + middles_per_seq + 1) as i64)
                .with_field("value", (seq * 100 + 99) as i64),
        );
    }
    events
}

// =============================================================================
// TIER 1: Common Ground (both engines can do these)
// =============================================================================

/// S1: Simple filter — price > 50.0
/// EventFlux: from StockStream[price > 50.0] select symbol, price insert into FilteredStream;
fn bench_s1_simple_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("S1_simple_filter");

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        stream Filtered = StockTick
            .where(price > 50.0)
            .emit(event_type: "FilteredTick", symbol: symbol, price: price)
    "#;
    let program = parse_program(source);

    for size in [100_000, 500_000, 1_000_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = gen_stock_ticks(size);

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
            b.iter(|| {
                rt.block_on(async {
                    let mut engine = Engine::new_benchmark();
                    engine.load(&program).unwrap();
                    for event in events {
                        engine.process(black_box(event.clone())).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

/// S2: Tumbling window aggregation — count/sum/avg over 100-event windows
/// EventFlux: from StockStream#window:length(100) select count(), avg(price), sum(volume) ...
fn bench_s2_window_aggregation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("S2_window_aggregation");
    group.sample_size(30);

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        stream Aggregated = StockTick
            .window(100)
            .aggregate(
                tick_count: count(price),
                avg_price: avg(price),
                total_volume: sum(volume)
            )
            .emit(event_type: "WindowResult",
                  count: tick_count, avg: avg_price, volume: total_volume)
    "#;
    let program = parse_program(source);

    for size in [100_000, 500_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = gen_stock_ticks(size);

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
            b.iter(|| {
                rt.block_on(async {
                    let mut engine = Engine::new_benchmark();
                    engine.load(&program).unwrap();
                    for event in events {
                        engine.process(black_box(event.clone())).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

/// S3: Partitioned aggregation — group by symbol + windowed stats
/// EventFlux: group by with window (PARTITION BY not fully supported yet)
fn bench_s3_partitioned_aggregation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("S3_partitioned_aggregation");
    group.sample_size(30);

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        stream PerSymbol = StockTick
            .partition_by(symbol)
            .window(50)
            .aggregate(
                symbol: last(symbol),
                avg_price: avg(price),
                max_price: max(price),
                total_volume: sum(volume)
            )
            .emit(event_type: "SymbolStats",
                  symbol: symbol, avg: avg_price, max: max_price, volume: total_volume)
    "#;
    let program = parse_program(source);

    for size in [100_000, 500_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = gen_stock_ticks(size);

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
            b.iter(|| {
                rt.block_on(async {
                    let mut engine = Engine::new_benchmark();
                    engine.load(&program).unwrap();
                    for event in events {
                        engine.process(black_box(event.clone())).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

/// S4: Simple sequence — A -> B (SASE+ engine, 2-event pattern)
/// EventFlux: from every a = AStream -> b = BStream select ... insert into MatchedStream;
fn bench_s4_simple_sequence(c: &mut Criterion) {
    let mut group = c.benchmark_group("S4_simple_sequence");

    for size in [100_000, 500_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = gen_typed_events(size, &["A", "B", "C"]);

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
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
                for event in events {
                    for _m in engine.process(black_box(event)) {
                        matches += 1;
                    }
                }
                matches
            });
        });
    }
    group.finish();
}

/// S5: Sequence with predicate — A[value > 500] -> B
/// EventFlux: from every a = AStream[value > 500] -> b = BStream select ...
fn bench_s5_sequence_predicate(c: &mut Criterion) {
    let mut group = c.benchmark_group("S5_sequence_predicate");

    for size in [100_000, 500_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = gen_typed_events(size, &["A", "B", "C"]);

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
            b.iter(|| {
                let pattern = SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "A".into(),
                        predicate: Some(Predicate::Compare {
                            field: "value".into(),
                            op: CompareOp::Gt,
                            value: varpulis_core::Value::Int(500),
                        }),
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
                for event in events {
                    for _m in engine.process(black_box(event)) {
                        matches += 1;
                    }
                }
                matches
            });
        });
    }
    group.finish();
}

/// S7: Multi-stream pipeline — 3 concurrent streams from same source
/// EventFlux: 3 separate queries on same input stream
fn bench_s7_multi_stream(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("S7_multi_stream");
    group.sample_size(30);

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        # Stream 1: Price filter
        stream HighPrice = StockTick
            .where(price > 120.0)
            .emit(event_type: "HighPrice", symbol: symbol, price: price)

        # Stream 2: Volume filter
        stream HighVolume = StockTick
            .where(volume > 5000)
            .emit(event_type: "HighVolume", symbol: symbol, volume: volume)

        # Stream 3: Windowed average
        stream Averages = StockTick
            .partition_by(symbol)
            .window(20)
            .aggregate(symbol: last(symbol), avg_price: avg(price))
            .emit(event_type: "AvgPrice", symbol: symbol, avg: avg_price)
    "#;
    let program = parse_program(source);

    for size in [100_000, 500_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = gen_stock_ticks(size);

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
            b.iter(|| {
                rt.block_on(async {
                    let mut engine = Engine::new_benchmark();
                    engine.load(&program).unwrap();
                    for event in events {
                        engine.process(black_box(event.clone())).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

/// S8: Filter + aggregate pipeline — filter then window aggregate
/// EventFlux: from StockStream[volume > 500]#window:length(20) select ...
fn bench_s8_filter_aggregate(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("S8_filter_aggregate");
    group.sample_size(30);

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        stream FilteredAgg = StockTick
            .where(volume > 500)
            .partition_by(symbol)
            .window(20)
            .aggregate(
                symbol: last(symbol),
                avg_price: avg(price),
                total_volume: sum(volume)
            )
            .emit(event_type: "FilteredAgg",
                  symbol: symbol, avg: avg_price, volume: total_volume)
    "#;
    let program = parse_program(source);

    for size in [100_000, 500_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = gen_stock_ticks(size);

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
            b.iter(|| {
                rt.block_on(async {
                    let mut engine = Engine::new_benchmark();
                    engine.load(&program).unwrap();
                    for event in events {
                        engine.process(black_box(event.clone())).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

/// S9: UDF evaluation — user-defined function in processing path
/// EventFlux: CASE WHEN expressions (scheduled M3, not yet implemented)
fn bench_s9_udf(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("S9_udf_evaluation");
    group.sample_size(30);

    let source = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        fn classify_trade(price: float, volume: int) -> str:
            let value = price * volume
            if value > 500000.0:
                return "whale"
            elif value > 50000.0:
                return "large"
            elif value > 5000.0:
                return "medium"
            else:
                return "small"

        fn risk_score(price: float, volume: int) -> float:
            let base = price * volume / 10000.0
            let factor = 1.0
            if price > 150.0:
                factor := 1.5
            if volume > 8000:
                factor := factor * 1.3
            return base * factor

        stream Classified = StockTick
            .emit(
                event_type: "Classification",
                symbol: symbol,
                category: classify_trade(price, volume),
                risk: risk_score(price, volume)
            )
    "#;
    let program = parse_program(source);

    for size in [100_000, 500_000] {
        group.throughput(Throughput::Elements(size as u64));
        let events = gen_stock_ticks(size);

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
            b.iter(|| {
                rt.block_on(async {
                    let mut engine = Engine::new_benchmark();
                    engine.load(&program).unwrap();
                    for event in events {
                        engine.process(black_box(event.clone())).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

/// S10: Parse + load time — compile query from source text
/// EventFlux: equivalent startup cost
fn bench_s10_parse_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("S10_parse_load");

    // Small program (filter only)
    let small = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        stream Filtered = StockTick
            .where(price > 50.0)
            .emit(event_type: "Out", symbol: symbol)
    "#;

    // Medium program (filter + aggregate + UDF)
    let medium = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        fn classify(price: float) -> str:
            if price > 100.0:
                return "high"
            else:
                return "low"

        stream Filtered = StockTick
            .where(price > 50.0)
            .partition_by(symbol)
            .window(50)
            .aggregate(avg_price: avg(price), total_vol: sum(volume))
            .emit(event_type: "Stats", category: classify(avg_price), volume: total_vol)
    "#;

    // Large program (3 streams + 2 UDFs + patterns)
    let large = r#"
        event StockTick:
            symbol: str
            price: float
            volume: int

        event Order:
            user_id: str
            amount: float

        event Payment:
            user_id: str
            status: str

        fn is_whale(price: float, volume: int) -> bool:
            return price * volume > 500000.0

        fn risk_score(price: float, volume: int) -> float:
            let base = price * volume / 10000.0
            if price > 150.0:
                return base * 1.5
            return base

        stream PriceAlerts = StockTick
            .where(price > 120.0)
            .emit(event_type: "Alert", symbol: symbol)

        stream WhaleWatch = StockTick
            .where(is_whale(price, volume))
            .emit(event_type: "Whale", symbol: symbol, risk: risk_score(price, volume))

        stream SymbolStats = StockTick
            .partition_by(symbol)
            .window(100)
            .aggregate(
                symbol: last(symbol),
                avg_price: avg(price),
                stddev_price: stddev(price),
                max_volume: max(volume)
            )
            .emit(event_type: "Stats", symbol: symbol,
                  avg: avg_price, std: stddev_price, max_vol: max_volume)
    "#;

    group.bench_function("small_program", |b| {
        b.iter(|| black_box(parse(black_box(small)).unwrap()));
    });
    group.bench_function("medium_program", |b| {
        b.iter(|| black_box(parse(black_box(medium)).unwrap()));
    });
    group.bench_function("large_program", |b| {
        b.iter(|| black_box(parse(black_box(large)).unwrap()));
    });

    group.finish();
}

// =============================================================================
// TIER 2: Varpulis Only (EventFlux cannot express these)
// =============================================================================

/// S11: Kleene+ exhaustive enumeration via ZDD
/// EventFlux: A+/A* "rejected by design" per ROADMAP.md
fn bench_s11_kleene_plus(c: &mut Criterion) {
    let mut group = c.benchmark_group("S11_kleene_plus_exhaustive");
    group.sample_size(30);

    // Test with varying numbers of B events between A and C
    for (seqs, middles) in [(1000, 5), (500, 10), (100, 15), (50, 20)] {
        let events = gen_kleene_sequences(seqs, middles);
        let total = events.len();
        let combinations_per_seq = (1u64 << middles) - 1;

        group.throughput(Throughput::Elements(total as u64));

        let label = format!("{seqs}seq_{middles}mid_2^{middles}");
        group.bench_with_input(BenchmarkId::new(&label, total), &events, |b, events| {
            b.iter(|| {
                let pattern = SasePattern::Seq(vec![
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
                    SasePattern::Event {
                        event_type: "C".into(),
                        predicate: None,
                        alias: Some("c".into()),
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
                black_box((matches, combinations_per_seq))
            });
        });
    }
    group.finish();
}

/// S12: Kleene* (zero-or-more) — A -> B* -> C
/// EventFlux: Cannot express
fn bench_s12_kleene_star(c: &mut Criterion) {
    let mut group = c.benchmark_group("S12_kleene_star");

    for (seqs, middles) in [(1000, 5), (500, 10)] {
        let events = gen_kleene_sequences(seqs, middles);
        let total = events.len();

        group.throughput(Throughput::Elements(total as u64));

        let label = format!("{seqs}seq_{middles}mid");
        group.bench_with_input(BenchmarkId::new(&label, total), &events, |b, events| {
            b.iter(|| {
                let pattern = SasePattern::Seq(vec![
                    SasePattern::Event {
                        event_type: "A".into(),
                        predicate: None,
                        alias: Some("a".into()),
                    },
                    SasePattern::KleeneStar(Box::new(SasePattern::Event {
                        event_type: "B".into(),
                        predicate: None,
                        alias: Some("b".into()),
                    })),
                    SasePattern::Event {
                        event_type: "C".into(),
                        predicate: None,
                        alias: Some("c".into()),
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
            });
        });
    }
    group.finish();
}

/// S13: Negation — Login -> NOT(Error) -> Logout
/// EventFlux: "Absent patterns / NOT...FOR requires TimerWheel, deferred to M2 Phase 3"
fn bench_s13_negation(c: &mut Criterion) {
    let mut group = c.benchmark_group("S13_negation");

    for size in [10_000, 50_000] {
        let events = gen_session_events(size);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
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
                for event in events {
                    for _m in engine.process(black_box(event)) {
                        matches += 1;
                    }
                }
                matches
            });
        });
    }
    group.finish();
}

/// S16: Nested Kleene + OR — (A -> B+) OR (C -> D*)
/// EventFlux: Cannot express (no Kleene, no compound OR patterns)
fn bench_s16_nested_kleene_or(c: &mut Criterion) {
    let mut group = c.benchmark_group("S16_nested_kleene_or");

    for size in [10_000, 50_000] {
        let events = gen_typed_events(size, &["A", "B", "C", "D"]);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("varpulis", size), &events, |b, events| {
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
                let mut engine = SaseEngine::new(pattern)
                    .with_strategy(SelectionStrategy::SkipTillAnyMatch)
                    .with_max_runs(100);
                let mut matches = 0;
                for event in events {
                    for _m in engine.process(black_box(event)) {
                        matches += 1;
                    }
                }
                matches
            });
        });
    }
    group.finish();
}

// =============================================================================
// Criterion groups
// =============================================================================

criterion_group! {
    name = tier1_common_ground;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(3));
    targets =
        bench_s1_simple_filter,
        bench_s2_window_aggregation,
        bench_s3_partitioned_aggregation,
        bench_s4_simple_sequence,
        bench_s5_sequence_predicate,
        bench_s7_multi_stream,
        bench_s8_filter_aggregate,
        bench_s9_udf,
        bench_s10_parse_load,
}

criterion_group! {
    name = tier2_varpulis_only;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(15))
        .warm_up_time(Duration::from_secs(3));
    targets =
        bench_s11_kleene_plus,
        bench_s12_kleene_star,
        bench_s13_negation,
        bench_s16_nested_kleene_or,
}

criterion_main!(tier1_common_ground, tier2_varpulis_only);
