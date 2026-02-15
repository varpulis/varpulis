# Built-in Functions

This document lists all built-in functions in VPL. Functions are marked as:
- **Implemented** - Working in current version
- **Planned** - Documented for future implementation

---

## Aggregation Functions (Implemented)

These functions work within `.aggregate()` blocks on windowed streams.

| Function | Description | Example | Status |
|----------|-------------|---------|--------|
| `count()` | Number of elements | `count()` | Implemented |
| `sum(expr)` | Sum of values | `sum(price)` | Implemented |
| `avg(expr)` | Average | `avg(temperature)` | Implemented |
| `min(expr)` | Minimum value | `min(latency)` | Implemented |
| `max(expr)` | Maximum value | `max(latency)` | Implemented |
| `first(expr)` | First element in window | `first(timestamp)` | Implemented |
| `last(expr)` | Last element in window | `last(value)` | Implemented |
| `stddev(expr)` | Standard deviation | `stddev(values)` | Implemented |
| `count_distinct(expr)` | Count of distinct values | `count_distinct(user_id)` | Implemented |
| `ema(expr, period)` | Exponential moving average | `ema(price, 12)` | Implemented |

### Aggregation Example

```varpulis
stream Stats = Trades
    .partition_by(symbol)
    .window(5m)
    .aggregate(
        total_volume: sum(volume),
        avg_price: avg(price),
        trade_count: count(),
        price_range: max(price) - min(price)
    )
    .emit(symbol: symbol, stats: "computed")
```

---

## Window Operations (Implemented)

Windows are specified as stream operators, not functions.

| Operation | Description | Example | Status |
|-----------|-------------|---------|--------|
| `.window(duration)` | Tumbling window | `.window(5m)` | Implemented |
| `.window(duration, sliding: interval)` | Sliding window | `.window(5m, sliding: 1m)` | Implemented |
| `.partition_by(field)` | Partition by key | `.partition_by(user_id)` | Implemented |

---

## Join Operations (Implemented)

| Function | Description | Example | Status |
|----------|-------------|---------|--------|
| `join(stream1, stream2, ...)` | Join multiple streams | `join(EMA12, EMA26)` | Implemented |
| `.on(condition)` | Join condition | `.on(A.symbol == B.symbol)` | Implemented |

### Join Example

```varpulis
stream MACD = join(EMA12, EMA26)
    .on(EMA12.symbol == EMA26.symbol)
    .window(1m)
    .select(
        symbol: EMA12.symbol,
        macd_line: EMA12.ema_12 - EMA26.ema_26
    )
    .emit(event_type: "MACD")
```

---

## Planned Functions (Not Yet Implemented)

The following functions are planned for future versions but are **not currently available**.

### Planned Aggregations
| Function | Description |
|----------|-------------|
| `variance(expr)` | Variance |
| `median(expr)` | Median value |
| `percentile(expr, p)` | Percentile (e.g., p99) |
| `collect()` | Collect all values into list |

### Planned Window Functions
| Function | Description |
|----------|-------------|
| `session_window(gap)` | Session-based windowing |
| `row_number()` | Row number in window |
| `rank()` | Rank in window |
| `lag(expr, n)` | Value n positions before |
| `lead(expr, n)` | Value n positions after |

### Planned String Functions
| Function | Description |
|----------|-------------|
| `len(s)` | String length |
| `upper(s)` / `lower(s)` | Case conversion |
| `trim(s)` | Remove whitespace |
| `contains(s, sub)` | Substring check |
| `starts_with(s, pre)` / `ends_with(s, suf)` | Prefix/suffix check |
| `split(s, sep)` / `join(arr, sep)` | Split/join strings |
| `replace(s, old, new)` | String replacement |
| `regex_match(s, pat)` | Regex matching |

### Planned Math Functions
| Function | Description |
|----------|-------------|
| `abs(x)` | Absolute value |
| `round(x, n)` / `floor(x)` / `ceil(x)` | Rounding |
| `sqrt(x)` / `pow(x, y)` | Power functions |
| `log(x)` / `exp(x)` | Logarithm/exponential |

### Planned Timestamp Functions
| Function | Description |
|----------|-------------|
| `now()` | Current timestamp |
| `year(ts)` / `month(ts)` / `day(ts)` | Date extraction |
| `hour(ts)` / `minute(ts)` / `second(ts)` | Time extraction |
| `duration_between(t1, t2)` | Time difference |

### Planned Utility Functions
| Function | Description |
|----------|-------------|
| `coalesce(a, b, ...)` | First non-null value |
| `uuid()` | Generate UUID |
| `random()` | Random number |

---

## Forecast Built-in Variables (Implemented)

These variables are available in streams that use the `.forecast()` operator after a sequence pattern. They are populated by the PST-based pattern forecasting engine.

| Variable | Type | Description |
|----------|------|-------------|
| `forecast_probability` | `float` | Pattern completion probability (0.0–1.0), Hawkes-modulated |
| `forecast_time` | `int` | Expected time to completion (nanoseconds) |
| `forecast_state` | `str` | Current NFA state label |
| `forecast_context_depth` | `int` | PST context depth used for prediction |
| `forecast_lower` | `float` | Lower bound of conformal prediction interval (0.0–1.0) |
| `forecast_upper` | `float` | Upper bound of conformal prediction interval (0.0–1.0) |

The `forecast_lower` and `forecast_upper` variables provide calibrated 90%-coverage prediction intervals via conformal prediction. Before sufficient calibration data is available, the interval defaults to `[0.0, 1.0]` (maximum uncertainty). As the engine observes forecast outcomes (pattern completions and expirations), the interval narrows automatically. Conformal intervals can be disabled per-pipeline with `conformal: false` — in that case, `forecast_lower` is always `0.0` and `forecast_upper` is always `1.0`.

The `forecast_probability` is modulated by Hawkes process intensity tracking. When events needed for the next NFA transition arrive in a temporal burst, the probability is boosted proportionally (up to 5x). During normal event rates, the boost factor is ~1.0. Hawkes modulation can be disabled per-pipeline with `hawkes: false` for latency-sensitive workloads.

### Forecast Example

```varpulis
stream FraudForecast = Transaction as t1
    -> Transaction as t2 where t2.amount > t1.amount * 5
    -> Transaction as t3 where t3.location != t1.location
    .within(5m)
    .forecast(confidence: 0.7, horizon: 2m, warmup: 500, max_depth: 5)
    .where(forecast_probability > 0.8)
    .emit(
        probability: forecast_probability,
        expected_time: forecast_time,
        state: forecast_state,
        confidence_lower: forecast_lower,
        confidence_upper: forecast_upper
    )
```

See [Forecasting Tutorial](../tutorials/forecasting-tutorial.md) and [Forecasting Architecture](../architecture/forecasting.md) for details.

---

## See Also

- [Syntax Reference](syntax.md)
- [Connectors](connectors.md)
