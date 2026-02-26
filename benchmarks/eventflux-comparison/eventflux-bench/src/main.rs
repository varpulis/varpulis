// EventFlux real-world benchmark
//
// Measures EventFlux throughput on the same scenarios used in the
// Varpulis comparison benchmarks. Uses EventFlux's programmatic API
// (the same API its own tests use) for maximum fairness.

use eventflux::core::event::event::Event;
use eventflux::core::stream::output::stream_callback::StreamCallback;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_app_runtime::EventFluxAppRuntime;
use eventflux::core::eventflux_manager::EventFluxManager;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Callback that counts received output events
// ---------------------------------------------------------------------------
#[derive(Debug)]
struct CountCallback {
    count: Arc<AtomicUsize>,
}

impl StreamCallback for CountCallback {
    fn receive_events(&self, events: &[Event]) {
        self.count.fetch_add(events.len(), Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Helper: create runtime from SQL, add count callback
// ---------------------------------------------------------------------------
async fn create_runtime(
    sql: &str,
    out_stream: &str,
) -> (Arc<EventFluxAppRuntime>, Arc<AtomicUsize>, EventFluxManager) {
    let manager = EventFluxManager::new();
    let runtime = manager
        .create_eventflux_app_runtime_from_string(sql)
        .await
        .expect("Failed to create EventFlux runtime");

    let count = Arc::new(AtomicUsize::new(0));
    runtime
        .add_callback(
            out_stream,
            Box::new(CountCallback {
                count: Arc::clone(&count),
            }),
        )
        .expect("Failed to add callback");

    runtime.start().expect("Failed to start runtime");
    (runtime, count, manager)
}

// ---------------------------------------------------------------------------
// Helper: run a single-stream benchmark
// ---------------------------------------------------------------------------
struct BenchResult {
    scenario: String,
    events_sent: usize,
    events_received: usize,
    elapsed_ms: f64,
    throughput: f64,
}

impl BenchResult {
    fn print(&self) {
        println!(
            "| {:<40} | {:>8} | {:>8} | {:>10.1}ms | {:>12.0} evt/s |",
            self.scenario, self.events_sent, self.events_received, self.elapsed_ms, self.throughput,
        );
    }
}

/// Wait until the output count stabilizes (no new events for 50ms).
fn wait_for_stable(count: &AtomicUsize) {
    let mut last_count = 0;
    let mut stable_ticks = 0;
    loop {
        std::thread::sleep(std::time::Duration::from_millis(5));
        let current = count.load(Ordering::Relaxed);
        if current == last_count {
            stable_ticks += 1;
            if stable_ticks >= 10 {
                break;
            }
        } else {
            stable_ticks = 0;
            last_count = current;
        }
    }
}

fn run_bench(
    scenario: &str,
    runtime: &Arc<EventFluxAppRuntime>,
    count: &Arc<AtomicUsize>,
    stream_id: &str,
    events: &[Vec<AttributeValue>],
    runs: usize,
) -> BenchResult {
    let n = events.len();

    // Get handler once outside the loop
    let handler = runtime
        .get_input_handler(stream_id)
        .expect("input handler not found");

    // Warmup
    count.store(0, Ordering::Relaxed);
    for data in events {
        let _ = handler
            .lock()
            .unwrap()
            .send_event_with_timestamp(0, data.clone());
    }
    wait_for_stable(count);

    // Timed runs
    let mut total_elapsed = 0.0f64;
    let mut total_received = 0usize;

    for _ in 0..runs {
        count.store(0, Ordering::Relaxed);
        let start = Instant::now();

        for data in events {
            let _ = handler
                .lock()
                .unwrap()
                .send_event_with_timestamp(0, data.clone());
        }

        wait_for_stable(count);
        let elapsed = start.elapsed();
        total_elapsed += elapsed.as_secs_f64() * 1000.0;
        total_received += count.load(Ordering::Relaxed);
    }

    let avg_elapsed = total_elapsed / runs as f64;
    let avg_received = total_received / runs;
    let throughput = n as f64 / (avg_elapsed / 1000.0);

    BenchResult {
        scenario: scenario.to_string(),
        events_sent: n,
        events_received: avg_received,
        elapsed_ms: avg_elapsed,
        throughput,
    }
}

// ---------------------------------------------------------------------------
// Multi-stream benchmark variant
// ---------------------------------------------------------------------------
fn run_bench_multi_stream(
    scenario: &str,
    runtime: &Arc<EventFluxAppRuntime>,
    count: &Arc<AtomicUsize>,
    stream_events: &[(&str, &[Vec<AttributeValue>])],
    total_events: usize,
    runs: usize,
) -> BenchResult {
    // Pre-resolve handlers
    let handlers: Vec<_> = stream_events
        .iter()
        .map(|(sid, _)| {
            runtime
                .get_input_handler(sid)
                .unwrap_or_else(|| panic!("input handler not found for {sid}"))
        })
        .collect();

    // Warmup
    count.store(0, Ordering::Relaxed);
    for (idx, (_, events)) in stream_events.iter().enumerate() {
        for data in *events {
            let _ = handlers[idx]
                .lock()
                .unwrap()
                .send_event_with_timestamp(0, data.clone());
        }
    }
    wait_for_stable(count);

    let mut total_elapsed = 0.0f64;
    let mut total_received = 0usize;

    for _ in 0..runs {
        count.store(0, Ordering::Relaxed);
        let start = Instant::now();

        // Interleave events from different streams for realistic pattern matching
        let max_len = stream_events.iter().map(|(_, e)| e.len()).max().unwrap_or(0);
        for i in 0..max_len {
            for (idx, (_, events)) in stream_events.iter().enumerate() {
                if i < events.len() {
                    let _ = handlers[idx]
                        .lock()
                        .unwrap()
                        .send_event_with_timestamp(0, events[i].clone());
                }
            }
        }

        wait_for_stable(count);
        let elapsed = start.elapsed();
        total_elapsed += elapsed.as_secs_f64() * 1000.0;
        total_received += count.load(Ordering::Relaxed);
    }

    let avg_elapsed = total_elapsed / runs as f64;
    let avg_received = total_received / runs;
    let throughput = total_events as f64 / (avg_elapsed / 1000.0);

    BenchResult {
        scenario: scenario.to_string(),
        events_sent: total_events,
        events_received: avg_received,
        elapsed_ms: avg_elapsed,
        throughput,
    }
}

// ===========================================================================
// Event generators
// ===========================================================================

fn gen_stock_ticks(n: usize) -> Vec<Vec<AttributeValue>> {
    let symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "META"];
    (0..n)
        .map(|i| {
            vec![
                AttributeValue::String(symbols[i % symbols.len()].to_string()),
                AttributeValue::Double(20.0 + (i as f64 * 1.7) % 100.0),
                AttributeValue::Long((100 + i % 1000) as i64),
            ]
        })
        .collect()
}

// ===========================================================================
// Scenarios
// ===========================================================================

async fn s1_simple_filter(n: usize) -> BenchResult {
    let sql = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume BIGINT);\n\
        CREATE STREAM FilteredStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO FilteredStream\n\
        SELECT symbol, price FROM StockStream WHERE price > 50.0;\n";

    let (runtime, count, _mgr) = create_runtime(sql, "FilteredStream").await;
    let events = gen_stock_ticks(n);
    let result = run_bench(
        &format!("S1: Simple filter ({n} events)"),
        &runtime,
        &count,
        "StockStream",
        &events,
        3,
    );
    runtime.shutdown();
    result
}

async fn s2_window_aggregation(n: usize) -> BenchResult {
    let sql = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume BIGINT);\n\
        CREATE STREAM AggStream (totalPrice DOUBLE);\n\
        INSERT INTO AggStream\n\
        SELECT sum(price) as totalPrice\n\
        FROM StockStream WINDOW('length', 100);\n";

    let (runtime, count, _mgr) = create_runtime(sql, "AggStream").await;
    let events = gen_stock_ticks(n);
    let result = run_bench(
        &format!("S2: Window aggregation ({n} events)"),
        &runtime,
        &count,
        "StockStream",
        &events,
        3,
    );
    runtime.shutdown();
    result
}

async fn s3_group_by_aggregation(n: usize) -> BenchResult {
    let sql = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume BIGINT);\n\
        CREATE STREAM GroupedStream (symbol STRING, totalPrice DOUBLE);\n\
        INSERT INTO GroupedStream\n\
        SELECT symbol, sum(price) as totalPrice\n\
        FROM StockStream GROUP BY symbol;\n";

    let (runtime, count, _mgr) = create_runtime(sql, "GroupedStream").await;
    let events = gen_stock_ticks(n);
    let result = run_bench(
        &format!("S3: GROUP BY aggregation ({n} events)"),
        &runtime,
        &count,
        "StockStream",
        &events,
        3,
    );
    runtime.shutdown();
    result
}

async fn s4_simple_sequence(n: usize) -> BenchResult {
    // EVERY enables continuous matching (without it, pattern matches only once)
    let sql = "\
        CREATE STREAM A (id INT, val INT);\n\
        CREATE STREAM B (id INT, val INT);\n\
        CREATE STREAM MatchStream (aid INT, bid INT);\n\
        INSERT INTO MatchStream\n\
        SELECT A.id AS aid, B.id AS bid\n\
        FROM PATTERN (EVERY (e1=A -> e2=B));\n";

    let (runtime, count, _mgr) = create_runtime(sql, "MatchStream").await;

    let a_events: Vec<Vec<AttributeValue>> = (0..n / 2)
        .map(|i| {
            vec![
                AttributeValue::Int(i as i32),
                AttributeValue::Int((i * 3) as i32),
            ]
        })
        .collect();

    let b_events: Vec<Vec<AttributeValue>> = (0..n / 2)
        .map(|i| {
            vec![
                AttributeValue::Int((i + n / 2) as i32),
                AttributeValue::Int((i * 5) as i32),
            ]
        })
        .collect();

    let result = run_bench_multi_stream(
        &format!("S4: Simple sequence A->B ({n} events)"),
        &runtime,
        &count,
        &[("A", &a_events), ("B", &b_events)],
        n,
        3,
    );
    runtime.shutdown();
    result
}

async fn s5_sequence_with_filter(n: usize) -> BenchResult {
    let sql = "\
        CREATE STREAM A (id INT, val INT);\n\
        CREATE STREAM B (id INT, val INT);\n\
        CREATE STREAM MatchStream (aid INT, bval INT);\n\
        INSERT INTO MatchStream\n\
        SELECT A.id AS aid, B.val AS bval\n\
        FROM PATTERN (EVERY (e1=A -> e2=B))\n\
        WHERE A.val > 500;\n";

    let (runtime, count, _mgr) = create_runtime(sql, "MatchStream").await;

    let a_events: Vec<Vec<AttributeValue>> = (0..n / 2)
        .map(|i| {
            vec![
                AttributeValue::Int(i as i32),
                AttributeValue::Int(((i * 17) % 1000) as i32),
            ]
        })
        .collect();

    let b_events: Vec<Vec<AttributeValue>> = (0..n / 2)
        .map(|i| {
            vec![
                AttributeValue::Int((i + n / 2) as i32),
                AttributeValue::Int((i * 5) as i32),
            ]
        })
        .collect();

    let result = run_bench_multi_stream(
        &format!("S5: Sequence + predicate ({n} events)"),
        &runtime,
        &count,
        &[("A", &a_events), ("B", &b_events)],
        n,
        3,
    );
    runtime.shutdown();
    result
}

async fn s8_filter_then_aggregate(n: usize) -> BenchResult {
    let sql = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume BIGINT);\n\
        CREATE STREAM FilteredStream (symbol STRING, price DOUBLE, volume BIGINT);\n\
        CREATE STREAM AggStream (totalPrice DOUBLE);\n\
        INSERT INTO FilteredStream\n\
        SELECT symbol, price, volume FROM StockStream WHERE price > 50.0;\n\
        INSERT INTO AggStream\n\
        SELECT sum(price) as totalPrice\n\
        FROM FilteredStream WINDOW('length', 100);\n";

    let (runtime, count, _mgr) = create_runtime(sql, "AggStream").await;
    let events = gen_stock_ticks(n);
    let result = run_bench(
        &format!("S8: Filter + aggregate ({n} events)"),
        &runtime,
        &count,
        "StockStream",
        &events,
        3,
    );
    runtime.shutdown();
    result
}

async fn s10_parse_time() -> Vec<BenchResult> {
    let small_sql = "\
        CREATE STREAM In_ (a INT);\n\
        CREATE STREAM Out_ (a INT);\n\
        INSERT INTO Out_ SELECT a FROM In_ WHERE a > 10;\n";

    let medium_sql = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume BIGINT);\n\
        CREATE STREAM FilteredStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM AggStream (totalPrice DOUBLE);\n\
        INSERT INTO FilteredStream\n\
        SELECT symbol, price FROM StockStream WHERE price > 50.0;\n\
        INSERT INTO AggStream\n\
        SELECT sum(price) as totalPrice\n\
        FROM FilteredStream WINDOW('length', 100);\n";

    let large_sql = "\
        CREATE STREAM A (id INT, val INT);\n\
        CREATE STREAM B (id INT, val INT);\n\
        CREATE STREAM C (id INT, val INT);\n\
        CREATE STREAM MatchAB (aid INT, bid INT);\n\
        CREATE STREAM MatchBC (bid INT, cid INT);\n\
        INSERT INTO MatchAB\n\
        SELECT A.id AS aid, B.id AS bid\n\
        FROM PATTERN (e1=A -> e2=B);\n\
        INSERT INTO MatchBC\n\
        SELECT B.id AS bid, C.id AS cid\n\
        FROM PATTERN (e1=B -> e2=C);\n";

    let mut results = Vec::new();
    let runs = 100;

    for (label, sql) in [
        ("S10: Parse (small)", small_sql),
        ("S10: Parse (medium)", medium_sql),
        ("S10: Parse (large)", large_sql),
    ] {
        let mut total_us = 0u128;
        for _ in 0..runs {
            let start = Instant::now();
            let manager = EventFluxManager::new();
            let _runtime = manager
                .create_eventflux_app_runtime_from_string(sql)
                .await
                .expect("parse failed");
            let elapsed = start.elapsed();
            total_us += elapsed.as_micros();
        }
        let avg_us = total_us as f64 / runs as f64;
        results.push(BenchResult {
            scenario: format!("{label}: {avg_us:.0}µs"),
            events_sent: 0,
            events_received: 0,
            elapsed_ms: avg_us / 1000.0,
            throughput: 0.0,
        });
    }

    results
}

// ===========================================================================
// Main
// ===========================================================================

#[tokio::main]
async fn main() {
    let event_sizes: Vec<usize> = std::env::args()
        .nth(1)
        .map(|s| s.split(',').filter_map(|v| v.parse().ok()).collect())
        .unwrap_or_else(|| vec![100_000, 500_000]);

    // Suppress EventFlux internal logging
    std::env::set_var("RUST_LOG", "error");

    println!("====================================================================");
    println!("  EventFlux Real Benchmark -- Measured Throughput");
    println!(
        "  Date: {}",
        chrono::Utc::now().format("%Y-%m-%d %H:%M UTC")
    );
    println!("====================================================================");
    println!();

    println!("EventFlux source: /tmp/eventflux");
    if let Ok(output) = std::process::Command::new("git")
        .args(["-C", "/tmp/eventflux", "log", "--oneline", "-1"])
        .output()
    {
        if output.status.success() {
            let commit = String::from_utf8_lossy(&output.stdout);
            println!("EventFlux commit: {}", commit.trim());
        }
    }
    println!();

    println!(
        "| {:<40} | {:>8} | {:>8} | {:>13} | {:>16} |",
        "Scenario", "Sent", "Received", "Time", "Throughput"
    );
    println!(
        "|{:-<42}|{:-<10}|{:-<10}|{:-<15}|{:-<18}|",
        "", "", "", "", ""
    );

    for &n in &event_sizes {
        s1_simple_filter(n).await.print();
    }
    for &n in &event_sizes {
        s2_window_aggregation(n).await.print();
    }
    for &n in &event_sizes {
        s3_group_by_aggregation(n).await.print();
    }
    for &n in &event_sizes {
        s4_simple_sequence(n).await.print();
    }
    for &n in &event_sizes {
        s5_sequence_with_filter(n).await.print();
    }
    for &n in &event_sizes {
        s8_filter_then_aggregate(n).await.print();
    }

    println!();
    println!("Parse time benchmarks (100 iterations):");
    for r in s10_parse_time().await {
        println!("  {}", r.scenario);
    }

    println!();
    println!("Done. All measurements use 3-run averaging with warmup.");
    println!("Events processed through real SQL queries (filter, aggregate, pattern match).");
}
