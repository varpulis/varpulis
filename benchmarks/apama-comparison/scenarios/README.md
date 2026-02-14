# Apama vs Varpulis Benchmark Scenarios

This directory contains side-by-side VPL and EPL implementations of identical CEP patterns
for objective comparison between Varpulis and Apama.

## Scenarios

### 01_filter - Simple Filter Baseline
**Pattern**: Filter events where price > threshold
- Tests: Basic predicate filtering
- Expected: Similar performance (this is baseline)

```
Varpulis: .where(price > 50.0)
Apama:    on all StockTick(*, >50.0, *)
```

### 02_aggregation - Windowed VWAP Calculation
**Pattern**: Volume Weighted Average Price over sliding window
- Tests: Window aggregation, multiple aggregates (sum, count, division)
- Expected: Comparable performance

```
Varpulis: .window(100).aggregate(vwap: sum(price*volume) / sum(volume))
Apama:    from t in trades retain 100 select sum(t.price * t.volume) / sum(t.volume)
```

### 03_temporal - Temporal Sequence Pattern
**Pattern**: Login followed by high-value Transaction from different IP within 5 minutes
- Tests: Temporal sequences with predicates, inter-event references
- Expected: Varpulis SASE+ should excel

```
Varpulis: Login as l -> Transaction where user_id == l.user_id .within(5m)
Apama:    on Login() as l -> Transaction(user_id=l.user_id) within(300.0)
```

### 04_kleene - Kleene+ Rising Sequence
**Pattern**: Detect sequences of rising prices (one or more)
- Tests: Kleene+ operator, native SASE+ pattern
- Expected: **Varpulis significantly better** - native SASE+ vs manual state machine

```
Varpulis: SEQ(Start, Rising+, End) - Native SASE+ (~15 lines)
Apama:    Manual dictionary tracking with state machine (~60 lines)
```

### 05_ema_crossover - EMA Crossover Trading Signal
**Pattern**: Fast EMA crosses slow EMA (MACD-style)
- Tests: Stream joins, aggregate functions (EMA), conditional logic
- Expected: Similar - both support stream joins

```
Varpulis: join(FastEMA, SlowEMA).on(symbol == symbol)
Apama:    from f in fastEMA join s in slowEMA on f.symbol = s.symbol
```

### 06_multi_sensor - Multi-Sensor Correlation
**Pattern**: Correlate temperature and pressure anomalies
- Tests: Multiple event types, having clause, complex joins
- Expected: Similar - tests full pipeline

## Running Benchmarks

### Varpulis

```bash
# Check syntax
cargo run -- check scenarios/01_filter/varpulis.vpl

# Run with events (requires event generator)
cargo run -- run scenarios/01_filter/varpulis.vpl --events data/events.jsonl

# Run criterion benchmarks
cargo bench --bench comparison_benchmark
```

### Apama

```bash
# Start correlator
docker run -d --name apama -p 15903:15903 public.ecr.aws/apama/apama-correlator

# Inject monitor
docker exec apama engine_inject -p 15903 /path/to/scenario.mon

# Send events
docker exec apama engine_send -p 15903 "StockTick(\"AAPL\", 150.0, 1000)"
```

## Code Comparison Summary

| Scenario | Varpulis (LoC) | Apama (LoC) | Ratio |
|----------|----------------|-------------|-------|
| 01_filter | 15 | 24 | 1.6x |
| 02_aggregation | 28 | 35 | 1.2x |
| 03_temporal | 25 | 35 | 1.4x |
| 04_kleene | 22 | 60 | **2.7x** |
| 05_ema_crossover | 40 | 55 | 1.4x |
| 06_multi_sensor | 50 | 65 | 1.3x |
| **Total** | **180** | **274** | **1.5x** |

## Key Differences

### Varpulis Strengths
1. **Kleene+ native support** - SASE+ patterns are first-class
2. **Declarative syntax** - Less boilerplate
3. **Temporal operators** - `->` and `.within()` are intuitive
4. **Hamlet engine** - Multi-query trend aggregation (not in Apama)

### Apama Strengths
1. **Mature ecosystem** - Extensive connectors
2. **Full EPL language** - Procedural when needed
3. **rstream operator** - Native delay/previous value
4. **Hot redeploy** - Runtime updates without restart

## Performance Notes

Based on criterion benchmarks:

| Workload | Varpulis | Notes |
|----------|----------|-------|
| Simple filter (100K) | 170K evt/s | Comparable to Apama |
| Sequence (A→B) | 1.2M evt/s | SASE+ optimized NFA |
| Kleene+ (A→B+→C) | 700K evt/s | Native SASE+ |
| Window agg (100K) | 140K evt/s | Slightly slower than Apama |

See `COMPARISON.md` in parent directory for full benchmark results.
