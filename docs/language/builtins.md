# Built-in Functions

## Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `count()` | Number of elements | `count()` |
| `sum(expr)` | Sum | `sum(price)` |
| `avg(expr)` | Average | `avg(temperature)` |
| `min(expr)` | Minimum | `min(latency)` |
| `max(expr)` | Maximum | `max(latency)` |
| `first(expr)` | First element | `first(timestamp)` |
| `last(expr)` | Last element | `last(value)` |
| `stddev(expr)` | Standard deviation | `stddev(values)` |
| `variance(expr)` | Variance | `variance(values)` |
| `median(expr)` | Median | `median(prices)` |
| `percentile(expr, p)` | Percentile | `percentile(latency, 0.99)` |
| `collect()` | List of all elements | `collect()` |
| `distinct(expr)` | Distinct values | `distinct(user_id)` |

## Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| `window(duration)` | Tumbling window | `window(5m)` |
| `window(duration, sliding)` | Sliding window | `window(5m, sliding: 1m)` |
| `session_window(gap)` | Session window | `session_window(gap: 30m)` |
| `row_number()` | Row number | `row_number()` |
| `rank()` | Rank | `rank()` |
| `lag(expr, n)` | Value n positions before | `lag(price, 1)` |
| `lead(expr, n)` | Value n positions after | `lead(price, 1)` |

## String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `len(s)` | Length | `len(name)` |
| `upper(s)` | Uppercase | `upper(name)` |
| `lower(s)` | Lowercase | `lower(name)` |
| `trim(s)` | Remove whitespace | `trim(input)` |
| `split(s, sep)` | Split | `split(path, "/")` |
| `join(arr, sep)` | Join | `join(parts, ",")` |
| `contains(s, sub)` | Contains | `contains(text, "error")` |
| `starts_with(s, pre)` | Starts with | `starts_with(url, "https")` |
| `ends_with(s, suf)` | Ends with | `ends_with(file, ".json")` |
| `replace(s, old, new)` | Replace | `replace(text, "foo", "bar")` |
| `regex_match(s, pat)` | Regex match | `regex_match(log, r"\d+")` |
| `regex_extract(s, pat)` | Regex extract | `regex_extract(log, r"(\d+)")` |

## Math Functions

| Function | Description | Example |
|----------|-------------|---------|
| `abs(x)` | Absolute value | `abs(-5)` |
| `round(x, n)` | Round | `round(3.14159, 2)` |
| `floor(x)` | Floor | `floor(3.9)` |
| `ceil(x)` | Ceiling | `ceil(3.1)` |
| `sqrt(x)` | Square root | `sqrt(16)` |
| `pow(x, y)` | Power | `pow(2, 10)` |
| `log(x)` | Natural logarithm | `log(10)` |
| `log10(x)` | Base 10 logarithm | `log10(100)` |
| `exp(x)` | Exponential | `exp(1)` |
| `sin(x)`, `cos(x)`, `tan(x)` | Trigonometry | `sin(0.5)` |

## Timestamp Functions

| Function | Description | Example |
|----------|-------------|---------|
| `now()` | Current timestamp | `now()` |
| `timestamp(s)` | Parse timestamp | `timestamp("2026-01-23")` |
| `year(ts)` | Year | `year(timestamp)` |
| `month(ts)` | Month | `month(timestamp)` |
| `day(ts)` | Day | `day(timestamp)` |
| `hour(ts)` | Hour | `hour(timestamp)` |
| `minute(ts)` | Minute | `minute(timestamp)` |
| `second(ts)` | Second | `second(timestamp)` |
| `day_of_week(ts)` | Day of the week | `day_of_week(timestamp)` |
| `format_ts(ts, fmt)` | Format | `format_ts(ts, "%Y-%m-%d")` |
| `duration_between(t1, t2)` | Duration between | `duration_between(start, end)` |

## Collection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `len(arr)` | Length | `len(items)` |
| `first(arr)` | First | `first(items)` |
| `last(arr)` | Last | `last(items)` |
| `head(arr, n)` | First N | `head(items, 5)` |
| `tail(arr, n)` | Last N | `tail(items, 5)` |
| `reverse(arr)` | Reverse | `reverse(items)` |
| `sort(arr)` | Sort | `sort(items)` |
| `flatten(arr)` | Flatten | `flatten(nested)` |
| `zip(a, b)` | Combine | `zip(keys, values)` |
| `enumerate(arr)` | With index | `enumerate(items)` |

## Join Functions

Joins allow correlating events from multiple streams.

| Function | Description | Example |
|----------|-------------|---------|
| `join(stream1, stream2, ...)` | Join multiple streams | `join(EMA12, EMA26)` |
| `.on(condition)` | Join condition | `.on(A.symbol == B.symbol)` |
| `.window(duration)` | Correlation window | `.window(1m)` |

### Join Example

```varpulis
# MACD calculation with two EMA stream join
stream MACD = join(EMA12, EMA26)
    .on(EMA12.symbol == EMA26.symbol)
    .window(1m)
    .select(
        symbol: EMA12.symbol,
        macd_line: EMA12.ema_12 - EMA26.ema_26
    )
    .emit(event_type: "MACD", symbol: symbol, macd_line: macd_line)
```

### Join Behavior

1. **Key correlation**: The `.on()` condition defines the join fields
2. **Temporal window**: Events are correlated if they arrive within the window
3. **Field access**: Fields from both sources are accessible with the stream prefix (e.g., `EMA12.symbol`)
4. **Emission**: A correlated event is emitted when all streams have a matching event

## Attention Functions

| Function | Description | Example |
|----------|-------------|---------|
| `attention_score(e1, e2)` | Correlation score | `attention_score(evt1, evt2)` |
| `top_attention(e, n)` | Top N correlated | `top_attention(event, 5)` |
| `cluster_by_attention(th)` | Clustering | `cluster_by_attention(0.8)` |

## Utility Functions

| Function | Description | Example |
|----------|-------------|---------|
| `coalesce(a, b, ...)` | First non-null | `coalesce(x, y, 0)` |
| `if_null(val, default)` | Default value | `if_null(name, "unknown")` |
| `typeof(x)` | Type of value | `typeof(value)` |
| `hash(x)` | Hash of value | `hash(user_id)` |
| `uuid()` | Generate UUID | `uuid()` |
| `random()` | Random number [0,1) | `random()` |
| `random_int(min, max)` | Random integer | `random_int(1, 100)` |
