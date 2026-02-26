# Benchmark: Varpulis vs EventFlux

## Purpose

Fair, reproducible comparison between Varpulis and EventFlux CEP engines.
Both are Rust-native, single-binary CEP engines targeting similar use cases.

EventFlux claims "1M+ events/sec on a single node" ([eventflux.io](https://eventflux.io/)).
This benchmark verifies that claim against real CEP workloads.

## Methodology

### Fairness Principles

1. **Same hardware**: Both engines run on the same machine, same OS, same Rust toolchain
2. **Same workloads**: Identical event schemas, event counts, and expected output
3. **Same measurement**: Wall-clock time for processing N pre-generated events, no I/O
4. **Equivalent queries**: Side-by-side VPL and EventFlux-QL for each scenario
5. **Warmup**: Both engines get warmup iterations before measurement
6. **No I/O**: Pure compute benchmark — no connectors, no network, no disk

### What We Measure

| Metric | How |
|--------|-----|
| **Throughput** | Events processed per second (higher is better) |
| **Latency** | Time per event at p50/p99 (lower is better) |
| **Memory** | Peak RSS during benchmark (lower is better) |
| **Startup** | Time from binary start to first event processed |
| **Correctness** | Output event count matches expected |

### What We Do NOT Measure

- Connector throughput (EventFlux has no Kafka connector yet)
- Distributed processing (EventFlux is single-node only)
- Persistence overhead (neither engine in persistence mode)

---

## Scenarios

### Tier 1: Common Ground (both engines can do these)

Both Varpulis and EventFlux support these workloads. These test raw throughput
on equivalent operations.

| # | Scenario | Pattern | Events |
|---|----------|---------|--------|
| S1 | Simple filter | `price > 50.0` | 100K, 500K, 1M |
| S2 | Tumbling window aggregation | `count/sum/avg` over 100-event windows | 100K, 500K |
| S3 | Partitioned aggregation | Group-by symbol + windowed stats | 100K, 500K |
| S4 | Simple sequence | `A -> B` (2-event pattern) | 100K, 500K |
| S5 | Sequence with predicate | `A[value>500] -> B` | 100K, 500K |
| S6 | Counted sequence | `A -> B{3} -> C` (fixed repetition) | 100K |
| S7 | Multi-stream pipeline | 3 concurrent streams from same source | 100K, 500K |
| S8 | Filter + aggregate pipeline | Filter then window aggregate | 100K, 500K |
| S9 | UDF evaluation | User-defined function in filter path | 100K, 500K |
| S10 | Parse + load time | Compile query from source text | N/A |

### Tier 2: Varpulis Advantages (EventFlux cannot do these)

These demonstrate capabilities that EventFlux explicitly lacks.
EventFlux's ROADMAP.md confirms A+/A* unbounded quantifiers are
"rejected by design", and absent/NOT patterns are deferred.

| # | Scenario | Pattern | Events |
|---|----------|---------|--------|
| S11 | Kleene+ exhaustive | `A -> B+ -> C` (all combinations via ZDD) | 10K, 50K |
| S12 | Kleene* (zero-or-more) | `A -> B* -> C` | 10K, 50K |
| S13 | Negation (NOT) | `Login -> NOT(Error) -> Logout` | 50K |
| S14 | PST forecasting | Predict pattern completion | 50K |
| S15 | Hamlet multi-query | 10/50 concurrent Kleene queries | 10K |
| S16 | Nested Kleene+OR | `(A -> B+) OR (C -> D*)` | 10K |

---

## Event Schemas

### StockTick (S1, S2, S3, S7, S8)
```json
{"event_type": "StockTick", "symbol": "AAPL", "price": 150.25, "volume": 1000}
```

### Order/Payment (S4, S5, S6, S9)
```json
{"event_type": "Order", "user_id": "u42", "amount": 750.0, "category": "electronics"}
{"event_type": "Payment", "user_id": "u42", "amount": 750.0, "status": "completed"}
```

### Login/Action/Logout/Error (S13)
```json
{"event_type": "Login", "user_id": "u1", "ip": "10.0.0.1"}
{"event_type": "Action", "user_id": "u1", "action": "view_page"}
{"event_type": "Logout", "user_id": "u1"}
```

### A/B/C/D (S4, S6, S11, S12, S15, S16)
```json
{"event_type": "A", "id": 1, "value": 100}
```

---

## Side-by-Side Query Comparison

### S1: Simple Filter

**Varpulis (VPL)**
```vpl
event StockTick:
    symbol: str
    price: float
    volume: int

stream Filtered = StockTick
    .where(price > 50.0)
    .emit(event_type: "FilteredTick", symbol: symbol, price: price)
```

**EventFlux**
```sql
define stream StockStream (symbol string, price float, volume long);

@info(name = 'filter_query')
from StockStream[price > 50.0]
select symbol, price
insert into FilteredStream;
```

### S2: Tumbling Window Aggregation

**Varpulis (VPL)**
```vpl
stream Aggregated = StockTick
    .window(100)
    .aggregate(
        tick_count: count(price),
        avg_price: avg(price),
        total_volume: sum(volume)
    )
    .emit(event_type: "WindowResult",
          count: tick_count, avg: avg_price, volume: total_volume)
```

**EventFlux**
```sql
from StockStream#window:length(100)
select count(price) as tick_count, avg(price) as avg_price, sum(volume) as total_volume
insert into AggregatedStream;
```

### S3: Partitioned Aggregation

**Varpulis (VPL)**
```vpl
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
```

**EventFlux**
```sql
-- EventFlux: PARTITION BY is listed as "runtime support needed" in ROADMAP.md
-- Workaround: manual filter per symbol or wait for M3
from StockStream#window:length(50)
select symbol, avg(price) as avg_price, max(price) as max_price, sum(volume) as total_volume
group by symbol
insert into SymbolStatsStream;
```

### S4: Simple Sequence (A -> B)

**Varpulis (VPL)**
```vpl
event Order:
    user_id: str
    amount: float

event Payment:
    user_id: str
    amount: float
    status: str

stream OrderPayment = Order as o
    -> Payment where user_id == o.user_id as p
    .emit(event_type: "Matched", user_id: o.user_id, amount: o.amount)
```

**EventFlux**
```sql
define stream OrderStream (user_id string, amount float);
define stream PaymentStream (user_id string, amount float, status string);

from every o = OrderStream -> p = PaymentStream[user_id == o.user_id]
select o.user_id, o.amount
insert into MatchedStream;
```

### S6: Counted Sequence (A -> B{3} -> C)

**Varpulis (VPL)**
```vpl
stream CountedPattern = A as start
    -> B{3} as middle
    -> C as finish
    .emit(event_type: "Matched", start_id: start.id)
```

**EventFlux**
```sql
from every a = AStream -> b = BStream{3} -> c = CStream
select a.id as start_id
insert into MatchedStream;
```

### S11: Kleene+ Exhaustive (Varpulis only)

**Varpulis (VPL)**
```vpl
# Exhaustive enumeration via ZDD - finds ALL possible combinations
stream KleeneExhaustive = A as start
    -> B+ as middle
    -> C as finish
    .emit(event_type: "KleeneMatch", start_id: start.id, match_count: count())
```

**EventFlux**: Cannot express. A+/A* unbounded quantifiers are "rejected by design" per ROADMAP.md.

### S14: PST Forecasting (Varpulis only)

**Varpulis (VPL)**
```vpl
stream FraudForecast = Login as l
    -> SmallTransaction+ as txns
    -> LargeWithdrawal as w
    .within(10m)
    .forecast(confidence: 0.8, horizon: 2m)
    .emit(event_type: "FraudPrediction",
          user_id: l.user_id,
          predicted_completion: forecast.probability)
```

**EventFlux**: No forecasting capability exists.

### S15: Hamlet Multi-Query (Varpulis only)

**Varpulis (VPL)**
```vpl
# 10-50 concurrent Kleene queries optimized via Hamlet shared graphlets
for i in 1..=50:
    stream TrendQuery_{i} = StockTick
        .partition_by(symbol)
        .pattern(rising: (e) => e.price > 100.0 + i)
        .detect(SEQ(rising+))
        .trend_aggregate(count: count())
        .emit(event_type: "Trend", query_id: i, count: count)
```

**EventFlux**: No multi-query optimization. Each query would run independently.

---

## Running the Benchmarks

### Prerequisites

```bash
# Rust toolchain (same version for both)
rustup show

# Clone EventFlux
git clone https://github.com/eventflux-io/engine.git /tmp/eventflux
cd /tmp/eventflux && cargo build --release

# Build Varpulis
cd /home/cpo/cep && cargo build --release
```

### Run Varpulis Benchmarks

```bash
# Criterion micro-benchmarks (automated, statistical)
cargo bench -p varpulis-runtime --bench eventflux_comparison

# CLI simulation benchmarks (end-to-end throughput)
./benchmarks/eventflux-comparison/run_benchmark.sh varpulis
```

### Run EventFlux Benchmarks

```bash
# EventFlux equivalent workloads
./benchmarks/eventflux-comparison/run_benchmark.sh eventflux
```

### Compare Results

```bash
# Generate comparison report
./benchmarks/eventflux-comparison/compare.sh
```

---

## Expected Outcomes

Based on Varpulis v0.3.0 baselines and EventFlux's published information:

### Tier 1 (Common Ground) — Predictions

| Scenario | Varpulis (expected) | EventFlux (expected) | Notes |
|----------|--------------------|--------------------|-------|
| S1 Filter 100K | ~234K evt/s | ~200-500K evt/s | Both fast for simple filters |
| S2 Window Agg | ~200K evt/s | Unknown | No published aggregation benchmarks |
| S3 Partition Agg | ~180K evt/s | Likely slower | PARTITION BY not fully implemented |
| S4 Sequence | ~256K evt/s | Unknown | EventFlux has sequences |
| S7 Multi-stream | ~200K evt/s | Unknown | Architecture-dependent |

### Tier 2 (Varpulis Only) — Capability Demonstration

| Scenario | Varpulis | EventFlux |
|----------|---------|-----------|
| S11 Kleene+ (ZDD) | 97K matches/s | **Cannot express** |
| S12 Kleene* | ~90K matches/s | **Cannot express** |
| S13 Negation | ~220K evt/s | **Not implemented** |
| S14 PST Forecast | 51ns/prediction | **No capability** |
| S15 Hamlet 10q | 2.1M evt/s | N/A (no optimization) |
| S15 Hamlet 50q | 950K evt/s | N/A |
| S16 Nested Kleene | ~100K evt/s | **Cannot express** |

---

## About EventFlux's "1M+ events/sec" Claim

EventFlux's only published benchmark (`benches/lock_contention_bench.rs`) tests
`StreamJunction` (internal pub/sub bus) with a `BenchProcessor` that performs
**zero work** — the `process()` function body is empty:

```rust
fn process(&mut self, _event: Event) {
    // Minimal processing - just consume the event
}
```

This measures **mutex contention overhead**, not CEP throughput. No SQL parsing,
no filtering, no windowing, no aggregation, no pattern matching occurs.

EventFlux's own README comparison table self-reports a **~500K events/sec scale ceiling**,
which contradicts the 1M+ marketing claim.

The author on Hacker News stated: "this isn't meant to replace Flink at massive scale."

This benchmark measures **real CEP workload throughput** to provide an honest comparison.

---

*Last updated: 2026-02-25*
