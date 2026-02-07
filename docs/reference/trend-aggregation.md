# Trend Aggregation Reference

Reference for `.trend_aggregate()`, the stream operation that computes aggregations over matching event trends without constructing them individually.

## Overview

Standard pattern matching in Varpulis operates in **detection mode**: the SASE+ NFA identifies individual pattern matches and emits each one. This works well when you need the matched events themselves, but becomes expensive when patterns contain Kleene closures (`+` / `*`), since the number of matching trends can grow exponentially with the number of events.

`.trend_aggregate()` switches a stream into **aggregation mode**. Instead of enumerating every matching trend, the engine computes aggregate values (COUNT, SUM, AVG, etc.) directly over the set of all matching trends. Internally, this uses the GRETA graph-based propagation algorithm, extended by the Hamlet engine for multi-query sharing.

The key complexity difference:

| Mode | Algorithm | Time Complexity |
|------|-----------|-----------------|
| Detection (SASE+ NFA) | Enumerate all trends | O(2^n) worst case |
| Aggregation (Hamlet) | Propagate counts through event graph | O(n) per graphlet |

## Syntax

```vpl
stream Name = EventType as alias
    -> all EventType where condition as alias
    -> EventType where condition as alias
    .within(duration)
    .partition_by(field)
    .trend_aggregate(
        alias1: function1(),
        alias2: function2(arg)
    )
    .emit(fields...)
```

The `.trend_aggregate()` block replaces the normal match output with computed aggregation results. It must appear after the pattern definition and any `.within()` or `.partition_by()` clauses.

**Constraints:**

- `.trend_aggregate()` and `.select()` are mutually exclusive on the same stream. Use `.emit()` to project the aggregation results.
- The pattern must contain at least one Kleene closure (`+` or `*`) for trend aggregation to be meaningful. Patterns without Kleene closures produce at most one trend per start event, so detection mode is already efficient.
- `.within()` is required. Trend aggregation results are emitted at window boundaries.

## Aggregation Functions

### `count_trends()`

Total number of matching trends in the current window.

**Signature:** `count_trends() -> int`

This is the GRETA COUNT operation. For a Kleene pattern `E+` with `n` matching events, the count is `2^n - 1` (every non-empty subset forms a valid trend).

```vpl
.trend_aggregate(
    num_trends: count_trends()
)
```

### `count_events(alias)`

Number of events of a specific type participating in trends.

**Signature:** `count_events(alias: string) -> int`

```vpl
stream S = Start as s
    -> all Reading where value > threshold as r
    -> End as e
    .within(5m)
    .trend_aggregate(
        event_count: count_events(r)
    )
```

### `sum_trends(alias.field)`

Sum of a numeric field across all matching trends.

**Signature:** `sum_trends(alias.field: string) -> float`

Each trend contributes the sum of the specified field for events in that trend. The result is the sum across all trends.

```vpl
.trend_aggregate(
    total_value: sum_trends(r.amount)
)
```

### `avg_trends(alias.field)`

Average of a numeric field across all matching trends.

**Signature:** `avg_trends(alias.field: string) -> float | null`

Computed as `sum_trends(alias.field) / count_trends()`.

```vpl
.trend_aggregate(
    avg_value: avg_trends(r.amount)
)
```

### `min_trends(alias.field)` / `max_trends(alias.field)`

Minimum or maximum of a numeric field across all matching trends.

**Signature:** `min_trends(alias.field: string) -> float | null`
**Signature:** `max_trends(alias.field: string) -> float | null`

```vpl
.trend_aggregate(
    lowest: min_trends(r.price),
    highest: max_trends(r.price)
)
```

## Detection Mode vs Aggregation Mode

### Detection Mode (default)

Without `.trend_aggregate()`, the engine runs the SASE+ NFA. Each complete pattern match produces an output event containing the matched events.

```vpl
# Detection mode: emits each individual match
stream BruteForceAttempts = LoginFailed as f
    -> all LoginFailed where user_id == f.user_id as repeats
    -> LoginSuccess where user_id == f.user_id as success
    .within(10m)
    .partition_by(user_id)
    .emit(
        user: f.user_id,
        failed_at: f.timestamp,
        succeeded_at: success.timestamp
    )
```

This is appropriate when you need the actual matched events, but if the Kleene closure matches many events (e.g., 30 failed logins), the number of output trends is exponential (2^30 - 1 = ~1 billion).

### Aggregation Mode

With `.trend_aggregate()`, the engine uses Hamlet to compute results without constructing trends.

```vpl
# Aggregation mode: emits a single summary per window
stream BruteForceStats = LoginFailed as f
    -> all LoginFailed where user_id == f.user_id as repeats
    -> LoginSuccess where user_id == f.user_id as success
    .within(10m)
    .partition_by(user_id)
    .trend_aggregate(
        num_attack_paths: count_trends(),
        total_failures: count_events(repeats)
    )
    .emit(
        user: user_id,
        attack_paths: num_attack_paths,
        failures: total_failures
    )
```

This produces one output per partition per window, regardless of how many trends exist.

### How It Works Internally

1. Events arrive and are grouped into **graphlets** (contiguous runs of the same event type).
2. When a graphlet closes (a different event type arrives), propagation coefficients are computed once for the graphlet.
3. For a Kleene graphlet with `n` events and incoming snapshot value `s`, the count is: `s * (2^n - 1)`.
4. Snapshot values propagate across graphlet boundaries, accumulating counts without enumerating individual trends.
5. At window boundaries, final aggregation results are emitted.

## Multi-Query Sharing

When multiple streams use `.trend_aggregate()` with patterns that share Kleene sub-patterns over the same event type, the engine automatically detects this and processes the shared Kleene portion once.

### Automatic Detection

The engine inspects registered queries at load time. If two or more queries contain Kleene closures over the same event type (e.g., both use `all StockTick+`), it creates a shared Hamlet aggregator.

### Sharing Benefit Model

The optimizer uses the following model to decide whether sharing is worthwhile:

```
Benefit(G) = g^2 * (ks - sp) - ks * sp
```

Where:
- `g` = graphlet size (number of events)
- `ks` = number of queries sharing the Kleene sub-pattern
- `sp` = number of snapshots

Sharing is beneficial when `ks > sp` and graphlets are large. The optimizer re-evaluates dynamically as workload characteristics change.

### Example: Shared Kleene Patterns

```vpl
# Query 1: Count all rising price trends
stream RisingTrends = StockTick as start
    -> all StockTick where price > start.price as ticks
    -> StockTick where price < ticks.price as end
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(
        trend_count: count_trends()
    )
    .emit(symbol: symbol, rising_trends: trend_count)

# Query 2: Sum of volumes in rising price trends
stream RisingVolume = StockTick as start
    -> all StockTick where price > start.price as ticks
    -> StockTick where price < ticks.price as end
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(
        total_vol: sum_trends(ticks.volume)
    )
    .emit(symbol: symbol, volume_in_trends: total_vol)
```

Both queries share the `all StockTick where price > start.price` Kleene sub-pattern. The engine processes this shared portion once and resolves per-query results through snapshots.

### Performance Impact

Benchmark results on the Hamlet engine show significant speedups from sharing:

| Queries Sharing Kleene | Per-Query Processing | Shared Processing | Speedup |
|------------------------|---------------------|-------------------|---------|
| 5 queries | 2.8 M events/sec | 2.8 M events/sec | 7x vs independent |
| 10 queries | 2.1 M events/sec | 2.1 M events/sec | 17x vs independent |
| 50 queries | 0.95 M events/sec | 0.95 M events/sec | 100x vs independent |

The speedup grows with the number of sharing queries because the shared propagation cost is O(1) per graphlet transition, while independent processing is O(queries) per transition.

## Complete Example: Stock Tick Price Trends

```vpl
event StockTick:
    symbol: str
    price: float
    volume: int
    timestamp: datetime

# Detect how many rising price trends exist per symbol per minute.
# With 50 ticks in a Kleene closure, there are 2^50 - 1 possible trends.
# Trend aggregation computes the count in O(50) instead of O(2^50).

stream PriceTrendStats = StockTick as first
    -> all StockTick where price >= first.price as rising
    -> StockTick where price < rising.price as reversal
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(
        num_trends: count_trends(),
        avg_price: avg_trends(rising.price),
        peak_price: max_trends(rising.price),
        total_volume: sum_trends(rising.volume)
    )
    .emit(
        symbol: symbol,
        window_trends: num_trends,
        avg_trend_price: avg_price,
        peak: peak_price,
        volume: total_volume
    )
```

**Sample output** (one record per symbol per window):

```json
{
  "symbol": "AAPL",
  "window_trends": 1125899906842623,
  "avg_trend_price": 187.42,
  "peak": 192.10,
  "volume": 48291038472
}
```

## Implementation Notes

- **Module:** `crates/varpulis-runtime/src/hamlet/` contains the Hamlet aggregator, graph, graphlet, snapshot, template, and optimizer modules.
- **Baseline:** `crates/varpulis-runtime/src/greta.rs` provides the GRETA foundation that Hamlet extends.
- **Propagation formula:** For a Kleene graphlet with `n` events and snapshot value `s`: `count = s * (2^n - 1)`.
- **Graphlet management:** Events of the same type are grouped into graphlets. Kleene edges within a graphlet are implicit (all-to-all adjacency), avoiding explicit edge construction.
- **Snapshot mechanism:** At graphlet boundaries, a `Snapshot` captures per-query intermediate values. The next graphlet resolves its counts by multiplying propagation coefficients with snapshot values.

## Academic References

The trend aggregation implementation is based on the following research:

- **GRETA**: Olga Poppe, Chuan Lei, Elke A. Rundensteiner, and David Maier. "GRETA: Graph-based Real-time Event Trend Aggregation." Proceedings of the VLDB Endowment, Vol. 11, No. 1, pp. 80-92, 2017. DOI: [10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)

- **Hamlet**: Olga Poppe, Chuan Lei, Elke A. Rundensteiner, and David Maier. "To Share or Not to Share: Online Event Trend Aggregation Over Bursty Event Streams." Proceedings of the ACM SIGMOD International Conference on Management of Data, 2021. DOI: [10.1145/3448016.3457310](https://doi.org/10.1145/3448016.3457310)

## See Also

- [SASE+ Pattern Matching Guide](../guides/sase-patterns.md) -- pattern syntax and detection mode
- [Windows & Aggregations Reference](windows-aggregations.md) -- window-based aggregations (non-trend)
- [Performance Tuning](../guides/performance-tuning.md) -- optimizing throughput
