# Trend Aggregation Tutorial

Learn how to compute statistics over event trends without enumerating every
matching sequence. This tutorial walks you through detection mode,
aggregation mode, multiple aggregate functions, and several streams over the
same ticks.

## Table of Contents

1. [Introduction](#introduction)
2. [Step 1: Detection Mode](#step-1-detection-mode)
3. [Step 2: Aggregation Mode](#step-2-aggregation-mode)
4. [Step 3: Multiple Aggregate Functions](#step-3-multiple-aggregate-functions)
5. [Step 4: Several Streams over the Same Ticks](#step-4-several-streams-over-the-same-ticks)
6. [When to Use Which Mode](#when-to-use-which-mode)
7. [Performance](#performance)

---

## Introduction

In complex event processing, a **trend** is a sequence of events that
matches a pattern. Consider a stock market scenario: a "rising price trend"
is a sequence of `StockTick` events where each price exceeds the first one.
With 100 ticks, there can be up to 2^100 distinct rising subsequences -- far
too many to enumerate.

**Trend aggregation** solves this problem. Instead of listing every matching
sequence, it computes aggregate statistics (count, sum, average, extremes)
directly over the set of matching trends. The Varpulis engine counts the
trends on the graph of the window's events, the GRETA algorithm (VLDB 2017),
in time quadratic in the number of events, without ever constructing the
exponential set of trends.

### Prerequisites

- Familiarity with VPL basics (streams, filters, patterns). See the
  [Language Tutorial](language-tutorial.md) if you are new.
- Familiarity with SASE+ Kleene closures (`all`). See the
  [SASE+ Pattern Guide](../guides/sase-patterns.md) for details.

---

## Step 1: Detection Mode

Detection mode finds individual event sequences that match a pattern and
returns the actual matched events. This is the standard behavior when you
write a SASE+ pattern in VPL.

### The Scenario

You are monitoring stock ticks and want to detect every rising price
pattern: a sequence where each tick's price is higher than the first tick's
price.

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

1. `StockTick as first` -- matches the first event in the sequence and binds
   it to `first`.
2. `-> all StockTick where price > first.price as rising` -- matches one or
   more subsequent ticks whose price exceeds the first tick's price. Under
   the default `.each()`, a run emits a match each time it takes a tick, and
   every tick starts a run of its own.
3. `.within(1m)` -- the entire sequence must occur within one minute.
4. `.partition_by(symbol)` -- patterns are matched independently per stock
   symbol.

### Running It

```bash
varpulis check rising_prices_detect.vpl
varpulis simulate -p rising_prices_detect.vpl -e stock_ticks.evt --verbose
```

### The Problem with Detection Mode

On the five ticks below, `RisingPrices` emits nine matches: four for the run
that starts at 100 (with 1 to 4 ticks), three for the run at 105, one each
for the runs at 110 and 108. That is one match per extension. The trends
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

With n qualifying ticks, the number of rising subsequences grows as O(2^n):
here every tick after 100 is above 100, so every non-empty subset of the
four is a trend, 2^4 - 1 = 15 from that start alone. And every tick starts
trends of its own: from 105, the subsets of {110, 108, 115}, 7; from 110 and
from 108, {115}, one each. 24 trends from five ticks. For a busy stock with
hundreds of ticks per minute, detection mode under `.subsets()` either
exhausts memory or takes exponential time. If you only need the *count* of
rising trends (not each individual sequence), aggregation mode is the
answer.

---

## Step 2: Aggregation Mode

Aggregation mode computes statistics over the set of matching trends without
constructing any of them individually. Instead of emitting each matched
sequence, it emits one result per partition and window.

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
    .emit(symbol: symbol, trends: count)
```

### What Changed

The only difference from detection mode is the addition of
`.trend_aggregate(count: count_trends())`. This tells the engine:

- Do **not** enumerate individual matching sequences.
- Instead, count the trends on the graph of the window's events.
- Emit the count once, when the window closes: when a tick of the symbol
  arrives a minute or more after the window's first tick, when the event
  clock passes that point, or at the end of the input. The result is
  stamped with the window's end.

### How the Count Is Computed

Under the hood, the engine takes the window's events in order and, for each
event, counts the partial trends that reach it:

```
reach(e) = start(e) + SUM(reach(e')) for every event e' that can precede e
```

The example stream has 4 qualifying ticks after the first. The pattern only
asks each tick to beat the first price, so every earlier qualifying tick can
precede a later one, 110 before 108 included. From the start at 100:

```
Event:    e1(105)  e2(110)  e3(108)  e4(115)
reach:      1        2        4        8
```

- e1: 1 (only the trend starting at 100 and ending here)
- e2: 1 + reach(e1) = 2 (trends: 100->110, 100->105->110)
- e3: 1 + reach(e1) + reach(e2) = 4 (100->108, 100->105->108, 100->110->108,
  100->105->110->108)
- e4: 1 + reach(e1) + reach(e2) + reach(e3) = 1 + 1 + 2 + 4 = 8

Trends from 100: 1 + 2 + 4 + 8 = 15. The same count from the start at 105
gives 7 (over 110, 108, 115), and from 110 and 108, 1 each: **24**.

If each tick had to beat the one before it (`-> all StockTick where price >
rising.price as rising`, or `.increasing(price)`), 108 could not follow 110:
the counts from 100 would be 1, 2, 2 and 6, 11 in all, and the total over
every start 18.

This completes in O(n²) time per window instead of O(2^n), and uses O(n)
space.

### Running It

```bash
varpulis check rising_prices_count.vpl
varpulis simulate -p rising_prices_count.vpl -e stock_ticks.evt --verbose
```

The engine prints one result for AAPL at the end of the input, stamped one
minute after the first tick: `trends: 24`.

---

## Step 3: Multiple Aggregate Functions

You can compute more than one aggregate in a single pass. The engine
computes all requested aggregates during the same counting.

### Available Trend Aggregate Functions

| Function | Description |
|----------|-------------|
| `count_trends()` | Number of trends |
| `count_events(alias)` | Times the step's events take part in a trend, summed over the trends; `count_events()` over every step |
| `sum_trends(alias.field)` | The field summed over every event of the step in every trend |
| `avg_trends(alias.field)` | `sum_trends` divided by `count_trends`; null without a trend |
| `min_trends(alias.field)` | Smallest value of the field on an event of the step in a trend |
| `max_trends(alias.field)` | Largest such value |

The alias names the pattern step the field is read from, and is required.

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
        event_count: count_events(rising),
        max_price: max_trends(rising.price),
        min_price: min_trends(rising.price)
    )
    .emit(
        symbol: symbol,
        trends: trend_count,
        events: event_count,
        ticks: count(rising),
        low: min_price,
        high: max_price
    )
```

### How Multiple Aggregates Work

During the counting, each event's number of trends is known per step, and
every aggregate is a sum over those. `count_trends()` counts the trends.
`count_events(rising)` counts the participations: an event that appears in 5
trends is counted 5 times. `count(rising)` in `.emit()` counts the distinct
ticks that take part in at least one trend. `max_trends(rising.price)` and
`min_trends(rising.price)` are the extreme prices of those ticks.

All aggregates are computed in a single O(n²) pass -- there is no additional
cost for requesting multiple functions.

### Running It

```bash
varpulis simulate -p rising_prices_stats.vpl -e stock_ticks.evt --verbose
```

The five ticks give `trends: 24`, `events: 46` (105 is in 8 trends, 110 and
108 in 12 each, 115 in 14), `ticks: 4`, `low: 105` and `high: 115`.

---

## Step 4: Several Streams over the Same Ticks

Two analytics streams can read the same ticks with different starting
conditions. Each counts its own trends.

```vpl
# multi_query_trends.vpl
# Two streams over the same ticks, with different start conditions

# Stream 1: Rising trends starting from a low price
stream LowStartRising = StockTick where price < 50 as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .emit(symbol: symbol, low_start_trends: count)

# Stream 2: Rising trends starting from a high price
stream HighStartRising = StockTick where price >= 50 as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .emit(symbol: symbol, high_start_trends: count)
```

On the ticks 40, 60, 70 of one symbol, `LowStartRising` reports 3 (from 40,
the subsets of {60, 70}) and `HighStartRising` 1 (from 60, {70}; from 70,
nothing). Each stream counts over its own window of the ticks; nothing is
shared between them, and nothing needs to be: the counting is quadratic in
the window, whatever the number of streams.

### Running It

```bash
varpulis simulate -p multi_query_trends.vpl -e stock_ticks.evt --verbose
```

---

## When to Use Which Mode

### Use Detection Mode When...

You need the **actual matched events** for downstream processing:

- **Alerting on specific sequences**: "Show me which exact ticks formed the
  spike so a trader can review them."
- **Forwarding matches**: You need to send the matched events to another
  system.
- **Small match cardinality**: The number of matching trends is inherently
  small (e.g., a three-event sequence with tight constraints).

```vpl
# Good use of detection mode: few matches expected
stream PriceSpikes = StockTick as t1
    -> StockTick where symbol == t1.symbol and price > t1.price * 1.1 as t2
    .within(1m)
    .emit(symbol: t1.symbol, from_price: t1.price, to_price: t2.price)
```

### Use Aggregation Mode When...

You need **statistics over trends** without caring about individual matches:

- **Dashboards**: "How many rising trends occurred per symbol in the last
  minute?"
- **Anomaly detection**: "Alert when the number of rising trends exceeds a
  threshold."
- **Kleene patterns with high cardinality**: Any pattern with `all` over a
  busy event stream.

```vpl
# Good use of aggregation mode: exponential matches, only count needed
stream TrendDashboard = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .where(count > 1000)
    .emit(symbol: first.symbol, trends: count)
```

---

## Performance

### Complexity Comparison

| Approach | Time per Window | Space |
|----------|----------------|-------|
| Detection mode, `.subsets()` (enumerate trends) | O(2^n) | O(2^n) |
| Aggregation mode (count on the event graph) | O(n²) per pattern step | O(n) |

Where `n` is the number of events in the window of one partition.

A condition that reads the start's alias (`price > first.price`) makes the
counts depend on the start, and the engine counts start by start: O(s · n²)
for `s` starts in the window. A condition on the previous event alone
(`price > rising.price`, `.increasing(price)`) or none at all is counted
from every start in one pass.

A partition's window holds at most 5 000 events; past that the window drops
what arrives and its result carries `_trend_truncated` with the number
dropped. Choose `.within()` and `.partition_by()` so that a window stays in
the hundreds of events.

---

## Next Steps

- [SASE+ Pattern Guide](../guides/sase-patterns.md) -- detailed coverage of
  Kleene closures, negation, and logical operators
- [Trend Aggregation Reference](../reference/trend-aggregation.md) -- every
  function, the window, what `.emit()` reads
- [Windows & Aggregations Reference](../reference/windows-aggregations.md)
  -- tumbling, sliding, and count-based windows

## References

- **GRETA**: Poppe et al., "Graph-based Real-time Event Trend Aggregation,"
  VLDB 2017.
  [DOI: 10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)
