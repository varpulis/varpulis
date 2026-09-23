# Trend Aggregation Tutorial

Learn how to compute statistics over event trends without enumerating every matching sequence. This tutorial walks you through detection mode, aggregation mode, multiple aggregate functions, and multi-query sharing.

> **What `.trend_aggregate()` computes today is not what this tutorial
> describes.** Running its examples on the engine (September 2026) showed that
> the count ignores the closure's predicate, the stream's start condition and
> its `partition_by`. On four falling prices, where `price > first.price` never
> holds, `count_trends()` still reaches 7; two streams that differ only in their
> start condition (`price < 50`, `price >= 50`) report the same counts, and a
> second symbol continues the first one's count. The count comes out after
> every event as a running total, `min(field)` and `max(field)` return the trend
> count, `count_events()` returns the number of events, and the emitted event
> carries no `symbol`. The walk-through below gives what the definitions
> compute. Until the engine agrees with it, do not act on a `.trend_aggregate()`
> number; detection mode, without `.trend_aggregate()`, is not affected.

## Table of Contents

1. [Introduction](#introduction)
2. [Step 1: Detection Mode](#step-1-detection-mode)
3. [Step 2: Aggregation Mode](#step-2-aggregation-mode)
4. [Step 3: Multiple Aggregate Functions](#step-3-multiple-aggregate-functions)
5. [Step 4: Multi-Query Sharing](#step-4-multi-query-sharing)
6. [When to Use Which Mode](#when-to-use-which-mode)
7. [Performance](#performance)

---

## Introduction

In complex event processing, a **trend** is a sequence of events that matches a pattern. Consider a stock market scenario: a "rising price trend" is a sequence of `StockTick` events where each price exceeds the one before it. With 100 ticks, there can be up to 2^100 distinct rising subsequences -- far too many to enumerate.

**Trend aggregation** solves this problem. Instead of listing every matching sequence, it computes aggregate statistics (count, sum, average) directly over the set of matching trends. The Varpulis engine uses the GRETA algorithm (VLDB 2017) for single-query aggregation and the Hamlet algorithm (SIGMOD 2021) for multi-query sharing, both computing results in polynomial time without ever constructing the exponential set of trends.

### Prerequisites

- Familiarity with VPL basics (streams, filters, patterns). See the [Language Tutorial](language-tutorial.md) if you are new.
- Familiarity with SASE+ Kleene closures (`all`). See the [SASE+ Pattern Guide](../guides/sase-patterns.md) for details.

---

## Step 1: Detection Mode

Detection mode finds individual event sequences that match a pattern and returns the actual matched events. This is the standard behavior when you write a SASE+ pattern in VPL.

### The Scenario

You are monitoring stock ticks and want to detect every rising price pattern: a sequence where each tick's price is higher than the first tick's price.

### The Program

Create a file called `rising_prices_detect.vpl`:

```vpl
# rising_prices_detect.vpl
# Detect individual rising price trends in stock ticks

stream RisingPrices = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .emit(symbol: first.symbol, start_price: first.price, ticks: count(rising))
```

### How It Works

1. `StockTick as first` -- matches the first event in the sequence and binds it to `first`.
2. `-> all StockTick where price > first.price as rising` -- matches one or more subsequent ticks whose price exceeds the first tick's price. Under the default `.each()`, a run emits a match each time it takes a tick, and every tick starts a run of its own.
3. `.within(1m)` -- the entire sequence must occur within one minute.
4. `.partition_by(symbol)` -- patterns are matched independently per stock symbol.

### Running It

```bash
varpulis check rising_prices_detect.vpl
varpulis simulate -p rising_prices_detect.vpl -e stock_ticks.evt --verbose
```

### The Problem with Detection Mode

On the five ticks below, `RisingPrices` emits nine matches: four for the run
that starts at 100 (with 1 to 4 ticks), three for the run at 105, one each for
the runs at 110 and 108. That is one match per extension. The trends
themselves, every subsequence that satisfies the pattern, are far more
numerous. Given the event stream:

```
StockTick { symbol: "AAPL", price: 100 }
StockTick { symbol: "AAPL", price: 105 }
StockTick { symbol: "AAPL", price: 110 }
StockTick { symbol: "AAPL", price: 108 }
StockTick { symbol: "AAPL", price: 115 }
```

The rising subsequences from the first tick (price=100) include:

- (100, 105)
- (100, 110)
- (100, 108)
- (100, 115)
- (100, 105, 110)
- (100, 105, 108)
- (100, 105, 115)
- (100, 105, 110, 115)
- (100, 105, 108, 115)
- (100, 110, 115)
- (100, 108, 115)
- (100, 105, 110, 108, 115)
- ... and more

With n qualifying ticks, the number of rising subsequences can grow as O(2^n): here every tick after 100 is above 100, so every non-empty subset of the four is a trend, 2^4 - 1 = 15 from that start alone. For a busy stock with hundreds of ticks per minute, detection mode either exhausts memory or takes exponential time. If you only need the *count* of rising trends (not each individual sequence), aggregation mode is the answer.

---

## Step 2: Aggregation Mode

Aggregation mode computes statistics over the set of matching trends without constructing any of them individually. Instead of emitting each matched sequence, it emits a single aggregate value.

### Converting to Aggregation Mode

Create a file called `rising_prices_count.vpl`:

```vpl
# rising_prices_count.vpl
# Count rising price trends without enumerating them

stream RisingTrendCount = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .emit(trends: count)
```

### What Changed

The only difference from detection mode is the addition of `.trend_aggregate(count: count_trends())`. This tells the engine:

- Do **not** enumerate individual matching sequences.
- Instead, use GRETA's graph-based count propagation to compute the total number of matching trends.
- Emit the count. The engine currently emits a running count after every event, not once when the window closes (see the warning at the top).

### How GRETA Computes the Count

Under the hood, the engine builds an **event graph** where:

- Each event is a node.
- An edge from event `e'` to event `e` means `e'` can precede `e` in a trend.
- For each node, the count of trends ending at that node is computed incrementally:

```
count(e) = start(e) + SUM(count(e')) for all predecessors e'
```

The example stream above has 4 qualifying ticks after the first. The pattern
only asks each tick to beat the first price, so every earlier qualifying tick
can precede a later one, 110 before 108 included:

```
Event:    e1(105)  e2(110)  e3(108)  e4(115)
count:      1        2        4        8
```

- e1: count = 1 (only the trend starting at 100 and ending here)
- e2: count = 1 + count(e1) = 2 (trends: 100->110, 100->105->110)
- e3: count = 1 + count(e1) + count(e2) = 4 (100->108, 100->105->108, 100->110->108, 100->105->110->108)
- e4: count = 1 + count(e1) + count(e2) + count(e3) = 1 + 1 + 2 + 4 = 8

Total trends = 1 + 2 + 4 + 8 = 15

If each tick had to beat the one before it (`-> all StockTick.increasing(price) as rising`),
108 could not follow 110, the counts would be 1, 2, 2 and 6, and the total 11.
The engine currently reports 15 for that pattern too.

This completes in O(n^2) time instead of O(2^n), and uses O(n) space.

### Running It

```bash
varpulis check rising_prices_count.vpl
varpulis simulate -p rising_prices_count.vpl -e stock_ticks.evt --verbose
```

The engine prints the running count after each tick: `1`, `3`, `7`, then `15`,
the total above.

---

## Step 3: Multiple Aggregate Functions

You can compute more than one aggregate in a single pass. The engine computes all requested aggregates simultaneously during graph propagation.

### Available Trend Aggregate Functions

| Function | Description |
|----------|-------------|
| `count_trends()` | Total number of matching trends |
| `count_events()` | Total number of events participating in trends |
| `sum(field)` | Sum of a field across all trends |
| `avg(field)` | Average of a field across all trends |
| `min(field)` | Minimum value of a field across all trends |
| `max(field)` | Maximum value of a field across all trends |

### Example: Count and Event Statistics

Create a file called `rising_prices_stats.vpl`:

```vpl
# rising_prices_stats.vpl
# Compute multiple trend statistics simultaneously

stream RisingTrendStats = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(
        trend_count: count_trends(),
        event_count: count_events(),
        max_price: max(price),
        min_price: min(price)
    )
    .emit(trends: trend_count, events: event_count, low: min_price, high: max_price)
```

### How Multiple Aggregates Work

During graph propagation, each node maintains values for all requested aggregates. The `count_trends()` function counts distinct subsequences. The `count_events()` function counts the total number of event participations across all trends (an event that appears in 5 trends is counted 5 times). The `max(price)` and `min(price)` functions track the extreme values observed across all trend-participating events.

All aggregates are computed in a single O(n^2) pass -- there is no additional cost for requesting multiple functions.

### Running It

```bash
varpulis simulate -p rising_prices_stats.vpl -e stock_ticks.evt --verbose
```

By the definitions, the five ticks give 15 trends with prices from 105 to 115.
The engine currently prints `trends: 15`, `events: 5`, and 15 for both `low`
and `high`.

---

## Step 4: Multi-Query Sharing

When you have multiple streams with overlapping Kleene patterns, Varpulis automatically detects the shared structure and uses the Hamlet algorithm to share computation across queries.

### The Scenario

You have two analytics streams monitoring the same stock ticks, each with a different starting condition but the same Kleene body:

```vpl
# multi_query_trends.vpl
# Two streams sharing the rising-price Kleene sub-pattern

# Stream 1: Rising trends starting from a low price
stream LowStartRising = StockTick where price < 50 as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .emit(low_start_trends: count)

# Stream 2: Rising trends starting from a high price
stream HighStartRising = StockTick where price >= 50 as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .emit(high_start_trends: count)
```

### What Happens Automatically

Both streams share the same Kleene sub-pattern: `all StockTick where price > first.price`. They differ only in the starting condition (`price < 50` vs `price >= 50`). The engine detects this overlap at compile time and activates Hamlet's shared processing:

1. **Merged Template**: The engine compiles both patterns into a single finite state automaton (FSA) with labeled transitions. Shared Kleene states are merged.

2. **Graphlets**: Events of the same type arriving in sequence are grouped into a *graphlet*. Within a shared graphlet, propagation coefficients are computed once.

3. **Snapshots**: At graphlet boundaries, a snapshot captures each query's intermediate state. The snapshot value for query q is the sum of trend counts from the predecessor graphlet.

4. **Resolution**: When the graphlet closes, each query's final count is resolved by multiplying the shared propagation coefficients by its own snapshot value:

```
count(event, query) = coefficient(event) * snapshot(query) + local_sum(event)
```

### The Benefit Model

The Hamlet optimizer continuously evaluates whether sharing is beneficial using:

```
Benefit = g^2 * (ks - sp) - ks * sp
```

Where:
- `g` = graphlet size (number of events)
- `ks` = number of queries sharing the Kleene sub-pattern
- `sp` = number of snapshots created

Sharing is beneficial when there are many queries (`ks` is large) relative to the number of distinct snapshot values (`sp` is small), and when graphlets are large (`g` is large). The optimizer can dynamically split or merge shared processing at runtime if conditions change.

### Running It

```bash
varpulis simulate -p multi_query_trends.vpl -e stock_ticks.evt --verbose
```

Both streams should produce independent results while the shared Kleene computation runs only once. Today they do not: on the same ticks the two report identical counts (see the warning at the top). With 10 streams sharing the same Kleene sub-pattern, you get a 17x speedup over processing each independently.

---

## When to Use Which Mode

### Use Detection Mode When...

You need the **actual matched events** for downstream processing:

- **Alerting on specific sequences**: "Show me which exact ticks formed the spike so a trader can review them."
- **Forwarding matches**: You need to send the matched events to another system (Kafka topic, database).
- **Small match cardinality**: The number of matching trends is inherently small (e.g., a three-event sequence with tight constraints).

```vpl
# Good use of detection mode: few matches expected
stream PriceSpikes = StockTick as t1
    -> StockTick where symbol == t1.symbol and price > t1.price * 1.1 as t2
    .within(1m)
    .emit(symbol: t1.symbol, from_price: t1.price, to_price: t2.price)
```

### Use Aggregation Mode When...

You need **statistics over trends** without caring about individual matches:

- **Dashboards**: "How many rising trends occurred per symbol in the last minute?"
- **Anomaly detection**: "Alert when the number of rising trends exceeds a threshold."
- **Kleene patterns with high cardinality**: Any pattern with `all` over a busy event stream.

```vpl
# Good use of aggregation mode: exponential matches, only count needed
stream TrendDashboard = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .where(count > 1000)
    .emit(trends: count)
```

### Use Multi-Query Sharing When...

You have **multiple queries with overlapping Kleene sub-patterns**:

- **Per-sector analytics**: Same rising-price pattern applied to different stock sectors.
- **Threshold sweeps**: Same pattern with different starting conditions or different window sizes.
- **Multi-customer monitoring**: Same pattern partitioned differently for different customers.

```vpl
# Ideal for sharing: N streams with the same Kleene body
stream Sector1 = StockTick where sector == "tech" as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .trend_aggregate(count: count_trends())
    .emit(tech_trends: count)

stream Sector2 = StockTick where sector == "finance" as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .trend_aggregate(count: count_trends())
    .emit(finance_trends: count)

# ... more sectors -- Hamlet shares the Kleene computation across all of them
```

---

## Performance

### Complexity Comparison

| Approach | Time per Window | Space |
|----------|----------------|-------|
| Detection mode (enumerate trends) | O(2^n) | O(2^n) |
| GRETA aggregation (single query) | O(n^2) | O(n) |
| Hamlet aggregation (k shared queries) | O(sp * n^2 + k * sp) | O(n + k) |

Where:
- `n` = number of events matching the Kleene pattern
- `k` = number of queries sharing the pattern
- `sp` = number of snapshot values (typically sp << k)

### Benchmark Results

Measured throughput for trend aggregation (events processed per second):

| Scenario | Naive Enumeration | GRETA (single) | Hamlet (shared) |
|----------|------------------|-----------------|-----------------|
| 1 query, 100 events | 1 K/s | 6.9 M/s | 6.9 M/s |
| 5 queries, 100 events | 200/s | 1.4 M/s | 2.8 M/s |
| 10 queries, 100 events | Timeout | 690 K/s | 2.1 M/s |
| 50 queries, 100 events | Timeout | 138 K/s | 0.95 M/s |

Run the benchmarks yourself:

```bash
cargo bench -p varpulis-runtime --bench hamlet_zdd_benchmark
```

### Why Hamlet Is Fast

The core insight is that Kleene sub-patterns shared across queries produce identical propagation coefficients. Instead of computing the O(n^2) propagation independently for each query, Hamlet computes it once and resolves per-query results in O(1) using snapshot values:

```
count(event, query) = coefficient(event) * snapshot(query)
```

For 50 queries sharing a Kleene pattern over a graphlet of 100 events:
- **Without sharing**: 50 * 100^2 = 500,000 operations
- **With sharing**: 100^2 + 50 * 1 = 10,050 operations (50x fewer)

The optimizer dynamically switches between shared and non-shared mode based on runtime statistics, ensuring that sharing overhead (snapshot creation) does not outweigh its benefits.

---

## Next Steps

- [SASE+ Pattern Guide](../guides/sase-patterns.md) -- detailed coverage of Kleene closures, negation, and logical operators
- [Windows & Aggregations Reference](../reference/windows-aggregations.md) -- tumbling, sliding, and count-based windows
- [Financial Markets Example](../examples/financial-markets.md) -- full trading signal generation with trend detection

## References

- **GRETA**: Poppe et al., "Graph-based Real-time Event Trend Aggregation," VLDB 2017. [DOI: 10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)
- **Hamlet**: Poppe et al., "To Share or Not to Share: Online Event Trend Aggregation Over Bursty Event Streams," SIGMOD 2021. [DOI: 10.1145/3448016.3457310](https://doi.org/10.1145/3448016.3457310)
