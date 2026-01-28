# Built-in Functions

This document lists all built-in functions in VarpulisQL. Functions are marked as:
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

## Attention Functions (Implemented)

Used within `.attention_window()` blocks for AI-powered correlation.

| Function | Description | Example | Status |
|----------|-------------|---------|--------|
| `attention_score(e1, e2)` | Correlation score between events | `attention_score(evt1, evt2)` | Implemented |
| `top_attention(e, n)` | Top N correlated events | `top_attention(event, 5)` | Implemented |

### Attention Example

```varpulis
stream Anomalies = Metrics
    .attention_window(
        duration: 1h,
        heads: 4,
        embedding: "rule_based"
    )
    .where(attention_score > 0.8)
    .emit(anomaly: true)
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

## See Also

- [Syntax Reference](syntax.md)
- [Connectors](connectors.md)
