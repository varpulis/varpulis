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
| **Nested Queries** | ✅ Full support | ✅ Multi-stage pipelines | Both support |
| **Dynamic Thresholds** | ✅ Variables updated at runtime | ✅ `var`/`:=` syntax | Both support |
| **Periodic Timers** | ✅ `on wait(period)` | ✅ `timer(5s)` | Both support |
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

## Recently Implemented (Now at Parity)

The following features were implemented to achieve feature parity with Apama:

1. ✅ **TIMER-01**: Periodic timer (`timer(5s)`) - January 2026
2. ✅ **VAR-01**: Dynamic variables (`var`/`let` + `:=` assignment) - January 2026
3. ✅ **QUERY-01**: Nested queries (multi-stage pipelines) - January 2026
4. ✅ **STREAM-01**: rstream operator (DelayBuffer, PreviousValueTracker) - January 2026
5. ✅ **STREAM-02**: Having clause for aggregate filtering - January 2026
6. ✅ **STREAM-03**: Inter-stream joins with aggregate comparison - January 2026

## Remaining Differences

| Feature | Apama | Varpulis | Notes |
|---------|-------|----------|-------|
| Hot redeploy | ✅ | ❌ | Apama can update running monitors |
| Spawn/Contexts | ✅ | ❌ | Different execution model |
| Full EPL language | ✅ | ❌ | Varpulis uses declarative DSL |

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

### Head-to-Head Benchmark (January 2026)

Test: Simple filter (price > 50) + windowed aggregation

| Events | Apama v27.18 | Varpulis v0.1 | Winner |
|--------|--------------|---------------|--------|
| 10,000 | 24K evt/s | **57K evt/s** | Varpulis 2.3x |
| 100,000 | **163K evt/s** | 146K evt/s | Apama 1.1x |

**Analysis:**
- Varpulis has lower per-event overhead (faster for small batches)
- Apama scales better with larger event volumes
- Both are native (C++ vs Rust) with similar performance characteristics

### Varpulis SASE+ Pattern Benchmarks (criterion)

| Pattern Type | Events | Time | Throughput |
|--------------|--------|------|------------|
| Simple Sequence (A->B) | 10,000 | **8.3ms** | **1.2M evt/s** |
| Kleene+ (A→B+→C) | 5,000 | **7.2ms** | **700K evt/s** |
| With Predicates | 5,000 | ~8ms | ~625K evt/s |
| Long Sequence (10 events) | 10,000 | ~377ms | 26K evt/s |

### Comparison Notes

| Metric | Apama | Varpulis |
|--------|-------|----------|
| Core Engine | C++ (native) | Rust (native) |
| Hot redeploy | ✅ Supported | ❌ Restart required |
| Kleene+ support | ⚠️ Via loops | ✅ Native SASE+ |
| Memory safety | Manual | Guaranteed (Rust) |
| Open source | ❌ Commercial | ✅ Open source |

## Conclusion

- **Varpulis strengths**: SASE+ patterns, Rust performance, simple syntax
- **Apama strengths**: Full EPL language, rstream/join, hot redeploy
- **Recommendation**: Add rstream and having clauses to Varpulis for parity
