# Apama vs Varpulis - Feature Comparison

## Overview

Comparing Apama EPL (Event Processing Language) with Varpulis VPL for Complex Event Processing.

## Feature Matrix

| Feature | Apama EPL | Varpulis VPL | Notes |
|---------|-----------|--------------|-------|
| **Basic Filtering** | ✅ `on all Event(*, >10.0)` | ✅ `.where(price > 10.0)` | Both support |
| **Streams** | ✅ `stream<T> s := from ... select ...` | ✅ `stream Name = ...` | Both support |
| **Count Windows** | ✅ `retain N` | ✅ `.window(N)` | Both support |
| **Time Windows** | ✅ `within T` | ✅ `.window(Ts)` | Both support |
| **Sliding Windows** | ✅ `retain N` | ✅ `.window(N, sliding: 1)` | Both support |
| **Partition By** | ✅ `partition by field` | ✅ `.partition_by(field)` | Both support |
| **Group By** | ✅ `group by field` | ⚠️ Limited | Varpulis has partition_by |
| **Aggregates: avg** | ✅ `mean()` | ✅ `avg()` | Both support |
| **Aggregates: sum** | ✅ `sum()` | ✅ `sum()` | Both support |
| **Aggregates: count** | ✅ `count()` | ✅ `count()` | Both support |
| **Aggregates: min/max** | ✅ `min()/max()` | ✅ `min()/max()` | Both support |
| **Aggregates: stddev** | ✅ `stddev()` | ✅ `stddev()` | Both support |
| **Aggregates: first/last** | ✅ `first()/last()` | ✅ `first()/last()` | Both support |
| **Temporal Sequences** | ✅ `A -> B within T` | ✅ `A as a -> B.within(T)` | Both support |
| **Kleene+ (one or more)** | ⚠️ Via loops | ✅ `Event+ as name` | Varpulis SASE+ native |
| **Negation** | ✅ `not A` | ✅ `NOT A` in pattern | Both support |
| **Inter-event Refs** | ✅ `a.field == b.field` | ✅ `where a.field == b.field` | Both support |
| **Stream Joins** | ✅ `join A ... join B on ...` | ✅ `join(A, B).on(...)` | Both support |
| **Having Clause** | ✅ `having condition` | ✅ `.having(condition)` | Both support |
| **rstream (delay)** | ✅ `rstream` outputs leaving items | ✅ `DelayBuffer`, `PreviousValueTracker` | API-level support |
| **Nested Queries** | ✅ Full support | ❌ Not yet | Missing |
| **Dynamic Thresholds** | ✅ Variables updated at runtime | ❌ Not yet | Missing |
| **Periodic Timers** | ✅ `on wait(period)` | ❌ Not yet | Missing |
| **Spawn/Contexts** | ✅ `spawn`, contexts | ❌ Not applicable | Different model |

## Unique Varpulis Features

| Feature | Description |
|---------|-------------|
| **SASE+ Engine** | Native NFA-based pattern matching with Kleene+ |
| **Attention Mechanism** | ML-based degradation detection |
| **VPL Syntax** | Declarative, YAML-inspired syntax |
| **Rust Performance** | Native compiled, no JVM |

## Unique Apama Features

| Feature | Description |
|---------|-------------|
| **Full EPL** | Complete procedural language with functions |
| **Contexts** | Parallel execution contexts |
| **Spawn** | Dynamic monitor instantiation |
| **rstream** | Output leaving window (delay operator) |
| **Hot Redeploy** | Runtime code updates |

## Recently Implemented

### Completed

1. **STREAM-01**: rstream operator for delay/comparison ✅
   - `DelayBuffer<T>` - generic delay buffer
   - `PreviousValueTracker<T>` - current vs previous comparison
   - Partitioned versions for grouped data
   - 17 tests in window.rs

2. **STREAM-02**: Having clause for aggregate filtering ✅
   - `.having(condition)` in VPL syntax
   - Filters aggregation results
   - 4 tests (2 parser, 2 runtime)

3. **STREAM-03**: Inter-stream joins with aggregate comparison ✅
   - Join two aggregated streams: `join(EMA12, EMA26)`
   - Compare aggregate values: `.where(EMA12.ema > EMA26.ema)`
   - Fixed event routing for derived streams
   - 1 new test (test_aggregate_comparison_join)

## Missing in Varpulis (Candidates for KANBAN)

### Medium Priority

1. **TIMER-01**: Periodic timer independent of events
   - Enables: VWAP period calculation
   - Use case: `on wait(5s) { ... }`

2. **DYNAMIC-VAR-01**: Dynamic variable updates
   - Enables: Adaptive thresholds
   - Use case: Update threshold after alert

3. **QUERY-01**: Nested queries
   - Enables: Sub-queries in stream definitions
   - Use case: Complex aggregation hierarchies

### Low Priority

6. **NESTED-QUERY-01**: Nested stream queries
   - Enables: Complex multi-stage processing

## Example Comparison

### Apama: Moving Average Alert
```epl
from t in ticks retain 10 select mean(t.price) as avg
from a in avg from p in (from a in avg retain 1 select rstream a)
where a > p + 1.0 or a < p - 1.0
select a - p as diff { send Alert(diff) to "output"; }
```

### Varpulis: Moving Average (partial)
```vpl
stream Averages = Ticks
    .window(10, sliding: 1)
    .aggregate(avg_price: avg(price))
    .emit(event_type: "Average", avg_price: avg_price)

# Cannot compare current vs previous without rstream
```

## Performance Comparison

### Varpulis SASE+ Benchmarks (criterion)

| Pattern Type | Events | Time | Throughput |
|--------------|--------|------|------------|
| Simple Sequence (A->B) | 10,000 | **8.3ms** | **1.2M evt/s** |
| Kleene+ (A->B+->C) | 5,000 | **7.2ms** | **700K evt/s** |
| With Predicates | 5,000 | ~8ms | ~625K evt/s |
| Long Sequence (10 events) | 10,000 | ~377ms | 26K evt/s |

### Comparison Notes

| Metric | Apama | Varpulis |
|--------|-------|----------|
| Pattern Matching (10K events) | ~50ms (estimated) | **8.3ms** (measured) |
| Memory per event | Higher (JVM) | Lower (Rust native) |
| Startup time | Slower (JVM warmup) | Faster (native binary) |
| Hot redeploy | ✅ Supported | ❌ Restart required |
| Kleene+ support | ⚠️ Via loops | ✅ Native SASE+ |

## Conclusion

- **Varpulis strengths**: SASE+ patterns, Rust performance, simple syntax
- **Apama strengths**: Full EPL language, rstream/join, hot redeploy
- **Recommendation**: Add rstream and having clauses to Varpulis for parity
