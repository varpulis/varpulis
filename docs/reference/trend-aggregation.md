# Trend Aggregation Reference

Reference for `.trend_aggregate()`, the stream operation that computes
statistics over every trend a pattern matches in a window, without
constructing the trends.

## Overview

Standard pattern matching in Varpulis operates in **detection mode**: the
SASE+ engine finds the individual matches of a pattern and emits each one.
This is what you want when you need the matched events themselves. It
becomes expensive when the pattern contains a Kleene closure (`all`): the
number of matching trends grows exponentially with the number of events the
closure accepts.

`.trend_aggregate()` switches a stream into **aggregation mode**. Instead of
enumerating the trends, the engine counts them, and sums over them, on the
graph of the window's events: for each event and each step of the pattern,
the number of partial trends that reach the event at that step is the sum
over the events that can precede it there, and the number of completions
from that point is the symmetric sum over the events that can follow it. The
product of the two is the number of trends the event takes part in, and
every aggregate is a sum over those. This is the GRETA algorithm (Poppe et
al., VLDB 2017), quadratic in the number of events of a window where the
trends are exponential.

| Mode | What it computes | Cost per window of `n` events |
|------|------------------|-------------------------------|
| Detection (SASE+ NFA, `.subsets()`) | Every trend, as a match | O(2^n) |
| Aggregation (`.trend_aggregate()`) | Counts and sums over every trend | O(n²) per step of the pattern |

## What a trend is

A trend is one match of the stream's pattern under skip-till-any-match, with
every non-empty subset of each closure: exactly what detection mode
enumerates under `.subsets()`. Every event that satisfies the start's
condition starts trends of its own; a closure over `k` qualifying events
gives `2^k - 1` trends from one start.

Detection mode treats a self-referencing closure (`price > rising.price`,
`.increasing(price)`) as one trend at a time: it runs a single run per
partition and emits once when the trend breaks. Aggregation mode does not:
it counts every trend, from every start, as the definitions say.

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

The `.trend_aggregate()` block replaces the match output with the
aggregation results. It comes after the pattern and its `.within()` and
`.partition_by()`.

**Constraints**, each refused when the program loads, with the form that
works:

- The pattern starts with a single event (`EventType as alias`), not a
  closure, and contains at least one closure (`-> all T ... as t`). Without
  a closure every start gives at most one trend, which detection mode
  already emits.
- `.within(d)` is required: it is the window.
- A step's condition reads the current event, the start's alias, and, in a
  closure, the closure's own alias, which is the trend's previous event (at
  the closure's first event, the step before). A single step after a closure
  may read the closure's alias: its last event. Any other alias cannot be
  bound while counting, and the program is refused naming it.
- A pattern with a negated step (`.not(T)`) is not counted: an absence is
  decided by a deadline, not by the events. Detect it in detection mode.
- `.each()`, `.longest()`, `.strict()` and `.stnm()` do not apply: the
  trends are the `.subsets()` matches under skip-till-any-match. `.subsets()`
  itself is accepted and changes nothing.
- `.forecast()` does not combine with `.trend_aggregate()`: a forecast reads
  the pattern's runs, and aggregation keeps none.
- `.select()` and `.trend_aggregate()` are mutually exclusive on the same
  stream. Use `.emit()` to project the results.

## The window

`.within(d)` is a tumbling window of `d` per partition. It opens with the
partition's first event and closes when an event of the partition arrives
at or past its end (that event opens the next window), when the event clock
of the pattern's sources passes its end, or at the end of a bounded input.
A trend lies inside one window; a trend that would straddle two windows is
not counted.

The result is one event per partition and window, stamped with the window's
end, when the window holds at least one event. A window whose events form
no trend still reports: `count_trends()` is 0, the sums 0, and the averages
and extremes null.

A `.watermark(out_of_order: d)` on the stream delays the clock-driven close
by `d`, as for any time window.

Counting is quadratic in the window's size. A partition's window holds at
most 5 000 events; past that the window drops what arrives and its result
carries `_trend_truncated` with the number dropped. Choose `.within()` and
`.partition_by()` so that a window stays in the hundreds of events.

## Aggregation functions

The worked example throughout: five ticks of one symbol, 100 105 110 108
115, with the pattern

```vpl
stream RisingTrends = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
```

From 100, every later tick qualifies and every non-empty subset of the four
is a trend: 15. From 105, the subsets of {110, 108, 115}: 7. From 110 and
from 108, {115}: one each. **24 trends.** 105 takes part in 8 of them, 110
and 108 in 12 each, 115 in 14.

### `count_trends()`

The number of trends in the window. **24** in the example.

**Signature:** `count_trends() -> int`

For a start followed by a closure over `k` qualifying events with no
condition between them, `2^k - 1` from that start. The count saturates at
`2^63 - 1`.

### `count_events(alias)`

The number of times an event of that step takes part in a trend, summed over
the trends: an event in five trends counts five times. Without an argument,
over every step of the pattern. For `rising` in the example, 8 + 12 + 12 +
14 = **46**; for every step, 46 plus one start per trend, **70**.

**Signature:** `count_events(alias: step) -> int`, `count_events() -> int`

The number of *distinct* events of a step that take part in at least one
trend is `count(alias)` in `.emit()` (**4** here).

### `sum_trends(alias.field)`

The field summed over every event of that step in every trend: each trend
contributes the sum of the field over its events of that step. In the
example, 8·105 + 12·110 + 12·108 + 14·115 = **5066**.

**Signature:** `sum_trends(alias.field) -> float`

A non-numeric or missing value of the field does not take part in the sum.

### `avg_trends(alias.field)`

`sum_trends(alias.field) / count_trends()`: the average, per trend, of the
field summed over the trend's events of that step. 5066 / 24 = **211.08** in
the example. Null when the window has no trend.

**Signature:** `avg_trends(alias.field) -> float | null`

### `min_trends(alias.field)` / `max_trends(alias.field)`

The smallest and the largest value of the field over the events of that step
that take part in at least one trend. **105** and **115** in the example.
Null when no event of the step takes part in a trend.

**Signature:** `min_trends(alias.field) -> float | null`,
`max_trends(alias.field) -> float | null`

The alias is required on the field aggregates: it names the pattern step the
field is read from. A bare `sum_trends(field)` is refused when the program
loads, as is a function that is not one of the six above (`max(price)` is
not a trend aggregate) and an alias that is not a step of the pattern.

## What `.emit()` reads after `.trend_aggregate()`

- The aggregates, by the names given in `.trend_aggregate()`.
- The partition key, by its field name (`symbol` above).
- `count(alias)`: the number of distinct events of the step that take part
  in a trend.
- The fields of the start, as `alias.field` (`first.price`): those of the
  earliest event that starts a trend in the window.
- The fields of a single step after the closure (`done.status`): those of
  the latest event of that step that ends a trend.
- A closure's fields (`rising.price`) are not available: a closure's events
  are many; read `min_trends`, `max_trends`, `sum_trends` of them.

A `.where()` between `.trend_aggregate()` and `.emit()` filters the results
on the same names:

```vpl
stream TrendDashboard = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .where(count > 1000)
    .emit(symbol: first.symbol, trends: count)
```

## Detection mode vs aggregation mode

### Detection mode (default)

Without `.trend_aggregate()`, the engine runs the SASE+ NFA. Each match
produces an output event carrying the matched events.

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

This is what you want when you need the matched events. If the closure
matches many events (30 failed logins), the number of trends is exponential
(2^30 - 1, about a billion).

### Aggregation mode

With `.trend_aggregate()`, the engine computes the results without
constructing the trends.

```vpl
# Aggregation mode: one summary per user and window
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

This produces one output per partition per window, however many trends there
are.

### How the counting works

For a window of events `e_1 .. e_n` in arrival order and a pattern of steps
`s_0 .. s_m` (the start first):

1. `reach[i][k]`, the number of partial trends that reach `e_k` at step
   `s_i`, is 1 for a start, and otherwise the sum of `reach[i-1][j]` over the
   earlier events `e_j` from which `e_k` can enter the step, plus, for a
   closure, the sum of `reach[i][j]` over the earlier events of the closure
   `e_k` can extend. Whether `e_k` can follow `e_j` is the step's condition,
   evaluated against `e_k` with the aliases bound as detection mode binds
   them.
2. `complete[i][k]`, the number of trends that complete from there, is the
   symmetric sum over the later events, plus one when `s_i` is the last
   step.
3. `reach[i][k] · complete[i][k]` is the number of trends `e_k` takes part in
   at step `s_i`. `count_trends()` is the sum over the starts; the other
   aggregates sum the field, or 1, times that product.

When a condition reads the start's alias (`price > first.price`), the sums
depend on the start and are computed start by start; otherwise one pass
counts from every start at once, as GRETA does.

## Complete example: stock tick price trends

```vpl
event StockTick:
    symbol: str
    price: float
    volume: int
    timestamp: datetime

# How many rising price trends per symbol and minute. With 50 ticks that
# all beat the first, there are 2^50 - 1 trends from that start alone; the
# count takes a pass over the window, not over the trends.

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

**Sample output** (one record per symbol and window):

```json
{
  "symbol": "AAPL",
  "window_trends": 1125899906842623,
  "avg_trend_price": 187.42,
  "peak": 192.10,
  "volume": 48291038472
}
```

## Implementation notes

- **Module:** `crates/varpulis-runtime/src/engine/trend.rs` holds the
  windows, the counting and the result; the compiler builds it from the
  pattern in `engine/compilation.rs` (`compile_trend_mode`), where what the
  counting cannot honour is refused.
- **State:** each partition's open window, its events and when it opened.
  It is in the snapshot (`Program::snapshot`): a program restored mid-window
  finishes the window as the original would have.
- **Condition evaluation:** a step's condition is evaluated with the same
  evaluator as a sequence step, with the aliases bound as described above;
  a condition that reads no alias is evaluated once per event.
- **Research library:** the `varpulis-hamlet` crate implements the shared
  graphlet propagation of Hamlet (Poppe et al., SIGMOD 2021) for patterns
  without conditions; it is not what `.trend_aggregate()` runs, since a
  pattern without conditions is rare and the sharing cannot honour them.

## Academic references

- **GRETA**: Olga Poppe, Chuan Lei, Elke A. Rundensteiner, and David Maier.
  "GRETA: Graph-based Real-time Event Trend Aggregation." Proceedings of the
  VLDB Endowment, Vol. 11, No. 1, pp. 80-92, 2017. DOI:
  [10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)
- **Hamlet**: Olga Poppe, Chuan Lei, Elke A. Rundensteiner, and David Maier.
  "To Share or Not to Share: Online Event Trend Aggregation Over Bursty Event
  Streams." Proceedings of the ACM SIGMOD International Conference on
  Management of Data, 2021. DOI:
  [10.1145/3448016.3457310](https://doi.org/10.1145/3448016.3457310)

## See also

- [SASE+ Pattern Matching Guide](../guides/sase-patterns.md) -- pattern
  syntax and detection mode
- [Trend Aggregation Tutorial](../tutorials/trend-aggregation-tutorial.md)
  -- the worked example, step by step
- [Windows & Aggregations Reference](windows-aggregations.md) -- window-based
  aggregations (non-trend)
