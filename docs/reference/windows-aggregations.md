# Windows & Aggregations Reference

Complete reference for window types and aggregation functions in VPL.

## Window Types

### Tumbling Windows

Non-overlapping, fixed-duration windows. When the duration expires, the window emits and resets.

**Syntax:**
```vpl
stream Name = EventType
    .window(<duration>)
    .aggregate(...)
```

**Parameters:**
- `<duration>`: Window length (e.g., `5s`, `1m`, `1h`)

**Behavior:**
- Events accumulate until window duration elapses
- When first event arrives, window start time is set
- When an event's timestamp exceeds `window_start + duration`, window emits
- Window resets with the triggering event

**Example:**
```vpl
stream MinuteStats = SensorReading
    .window(1m)
    .aggregate(
        avg_value: avg(value),
        max_value: max(value),
        min_value: min(value),
        count: count()
    )
```

**Implementation:** `TumblingWindow` struct in `varpulis-runtime/src/window.rs:15`

---

### Sliding Windows

Overlapping windows that slide at a specified interval, providing a rolling view of data.

**Syntax:**
```vpl
stream Name = EventType
    .window(<size>, sliding: <slide>)
```

**Parameters:**
- `<size>`: Window size (e.g., `5m`)
- `<slide>`: Slide interval (e.g., `1m`)

**Behavior:**
- Maintains events within the window size
- Emits current window contents at each slide interval
- Old events are evicted as they fall outside the window
- Window contents overlap between emissions

**Example:**
```vpl
stream RollingAverage = TemperatureReading
    .window(5m, sliding: 30s)
    .aggregate(
        rolling_avg: avg(temperature),
        recent_max: max(temperature)
    )
```

**Implementation:** `SlidingWindow` struct in `varpulis-runtime/src/window.rs:77`

---

### Count Windows

Windows based on event count rather than time.

**Syntax:**
```vpl
stream Name = EventType
    .window(<n>)
    .aggregate(...)
```

**Parameters:**
- `<n>`: Number of events per window

**Behavior:**
- Collects exactly N events
- Emits when count is reached
- Resets to empty after emission

**Example:**
```vpl
stream BatchProcessor = Transaction
    .window(100)
    .aggregate(
        batch_total: sum(amount),
        batch_count: count()
    )
```

**Implementation:** `CountWindow` struct in `varpulis-runtime/src/window.rs:146`

---

### Sliding Count Windows

Count-based windows with overlap.

**Syntax:**
```vpl
stream Name = EventType
    .window(<size>, sliding: <slide>)
```

**Parameters:**
- `<size>`: Window size in events
- `<slide>`: Slide size in events

**Example:**
```vpl
stream RollingBatch = Reading
    .window(100, sliding: 25)
    .aggregate(
        rolling_sum: sum(value)
    )
```

**Implementation:** `SlidingCountWindow` struct in `varpulis-runtime/src/window.rs:194`

---

### Partitioned Windows

Any window type can be partitioned by a key field.

**Syntax:**
```vpl
stream Name = EventType
    .partition_by(<field>)
    .window(<params>)
    .aggregate(...)
```

**Behavior:**
- Creates independent windows per unique partition key
- Each partition emits separately
- Memory scales with number of unique keys

**Example:**
```vpl
stream PerDeviceStats = SensorReading
    .partition_by(device_id)
    .window(1m)
    .aggregate(
        device: device_id,
        avg_reading: avg(value),
        readings: count()
    )
```

**Implementations:**
- `PartitionedTumblingWindow` in `window.rs`
- `PartitionedSlidingWindow` in `window.rs`

---

## Duration Units

| Unit | Abbreviation | Example |
|------|--------------|---------|
| Milliseconds | `ms` | `500ms` |
| Seconds | `s` | `30s` |
| Minutes | `m` | `5m` |
| Hours | `h` | `1h` |
| Days | `d` | `1d` |

**Examples:**
```vpl
.window(500ms)              // Half second
.window(30s)                // 30 seconds
.window(5m, sliding: 1m)    // 5 min window, 1 min slide
.window(1h)                 // 1 hour
.window(1d)                 // 1 day
```

---

## Aggregation Functions

All aggregations operate on events within a window.

### `count()`

Count the number of events in the window.

**Signature:** `count() -> int`

**Example:**
```vpl
.aggregate(total_events: count())
```

---

### `sum(field)`

Sum numeric field values. SIMD-optimized on x86_64 with AVX2.

**Signature:** `sum(field: string) -> float`

**Example:**
```vpl
.aggregate(
    total_amount: sum(amount),
    total_quantity: sum(quantity)
)
```

**Performance:** Uses SIMD (AVX2) for 4x parallel f64 addition when available.

---

### `avg(field)`

Compute the arithmetic mean. SIMD-optimized.

**Signature:** `avg(field: string) -> float | null`

**Returns:** `null` if no events contain the field.

**Example:**
```vpl
.aggregate(average_price: avg(price))
```

**Performance:** Extracts values to contiguous array for SIMD summation.

---

### `min(field)`

Find the minimum value. SIMD-optimized.

**Signature:** `min(field: string) -> float | null`

**Example:**
```vpl
.aggregate(lowest_temp: min(temperature))
```

**Performance:** Uses SIMD `_mm256_min_pd` for parallel comparison.

---

### `max(field)`

Find the maximum value. SIMD-optimized.

**Signature:** `max(field: string) -> float | null`

**Example:**
```vpl
.aggregate(highest_temp: max(temperature))
```

**Performance:** Uses SIMD `_mm256_max_pd` for parallel comparison.

---

### `stddev(field)`

Compute the sample standard deviation using Welford's online algorithm.

**Signature:** `stddev(field: string) -> float | null`

**Returns:** `null` if fewer than 2 values.

**Example:**
```vpl
.aggregate(temp_variance: stddev(temperature))
```

**Algorithm:** Single-pass Welford's algorithm for numerical stability.

---

### `percentile(field, p)`

Compute the p-th percentile (0-100).

**Signature:** `percentile(field: string, p: int) -> float | null`

**Example:**
```vpl
.aggregate(
    p50_latency: percentile(latency, 50),
    p95_latency: percentile(latency, 95),
    p99_latency: percentile(latency, 99)
)
```

---

### `collect(field)`

Collect all field values into an array.

**Signature:** `collect(field: string) -> array`

**Example:**
```vpl
.aggregate(
    all_sensors: collect(sensor_id),
    all_values: collect(value)
)
```

---

### `first(field)`

Return the first value in the window (by event order).

**Signature:** `first(field: string) -> value | null`

**Example:**
```vpl
.aggregate(
    window_start: first(timestamp),
    initial_value: first(value)
)
```

---

### `last(field)`

Return the last value in the window (by event order).

**Signature:** `last(field: string) -> value | null`

**Example:**
```vpl
.aggregate(
    window_end: last(timestamp),
    final_value: last(value)
)
```

---

### `distinct(field)` / `count_distinct(field)`

Count unique values of a field.

**Signature:** `count(distinct(field: string)) -> int`

**Example:**
```vpl
.aggregate(
    unique_users: count(distinct(user_id)),
    unique_products: count(distinct(product_id))
)
```

---

## Incremental Aggregations

For high-throughput scenarios, Varpulis supports incremental aggregation where possible.

### IncrementalSum

O(1) updates for sum instead of O(n) recomputation.

**Supported operations:**
- `add(value)` - Add a value: O(1)
- `remove(value)` - Remove a value: O(1)
- `sum()` - Get current sum: O(1)
- `count()` - Get current count: O(1)
- `avg()` - Get current average: O(1)

**Use case:** Sliding windows where events expire frequently.

**Implementation:** `IncrementalSum` in `varpulis-runtime/src/simd.rs:391`

---

### IncrementalMinMax

Maintains min/max with incremental updates.

**Supported operations:**
- `add(value)` - Add a value: O(1) amortized
- `remove(value)` - Remove a value: O(n) worst case
- `min()` - Get current min: O(1) after sort
- `max()` - Get current max: O(1) after sort

**Implementation:** `IncrementalMinMax` in `varpulis-runtime/src/simd.rs:444`

---

## SIMD Optimization

Aggregations use SIMD (Single Instruction Multiple Data) vectorization when available.

### Requirements

- x86_64 architecture
- AVX2 instruction set support (most CPUs since 2013)

### Performance Characteristics

| Operation | Scalar | SIMD (AVX2) | Speedup |
|-----------|--------|-------------|---------|
| sum | O(n) | O(n/4) | ~4x |
| min | O(n) | O(n/4) | ~4x |
| max | O(n) | O(n/4) | ~4x |
| avg | O(n) | O(n/4) | ~4x |

### How It Works

1. **Field Extraction**: Numeric field values are extracted to contiguous `Vec<f64>`
2. **Vectorization**: AVX2 processes 4 f64 values per instruction
3. **Loop Unrolling**: 4-way unrolling for better instruction-level parallelism
4. **Horizontal Reduction**: Final aggregation of vector lanes
5. **Remainder Handling**: Scalar processing for remaining elements

### Fallback

On non-AVX2 systems, scalar 4-way loop unrolling provides ~2x speedup over naive loops.

---

## Memory Considerations

### Window Memory Usage

| Window Type | Memory Formula |
|-------------|----------------|
| Tumbling | `O(events_per_window)` |
| Sliding | `O(events_per_window)` |
| Count | `O(window_size)` |
| Partitioned | `O(partitions × events_per_window)` |

### Recommendations

1. **Use appropriate window sizes**: Smaller windows = less memory
2. **Partition carefully**: Each partition key creates a separate window
3. **Consider count windows**: Fixed memory usage vs. time windows
4. **Monitor partition growth**: Unbounded partition keys can exhaust memory

---

## Examples

### Real-time Monitoring

```vpl
stream SensorAlerts = SensorReading
    .partition_by(sensor_id)
    .window(1m)
    .aggregate(
        sensor: sensor_id,
        avg_value: avg(value),
        max_value: max(value),
        readings: count()
    )
    .where(avg_value > threshold or max_value > critical_threshold)
    .emit(sensor: sensor, avg_value: avg_value, max_value: max_value)
```

### Rolling Statistics

```vpl
stream RollingStats = MarketData
    .window(5m, sliding: 10s)
    .aggregate(
        vwap: sum(price * volume) / sum(volume),
        high: max(price),
        low: min(price),
        volatility: stddev(price)
    )
```

### Batch Processing

```vpl
stream BatchReport = Transaction
    .window(1000)
    .aggregate(
        batch_num: count(),
        total_value: sum(amount),
        avg_value: avg(amount),
        unique_customers: count(distinct(customer_id))
    )
    .print("Processed batch: {batch_num} txns, ${total_value:.2} total")
```

### Multi-Level Aggregation

```vpl
// First level: per-device minute stats
stream DeviceMinutes = SensorReading
    .partition_by(device_id)
    .window(1m)
    .aggregate(
        device: device_id,
        minute_avg: avg(value)
    )

// Second level: all-device hour stats
stream HourlyOverview = DeviceMinutes
    .window(1h)
    .aggregate(
        active_devices: count(distinct(device)),
        overall_avg: avg(minute_avg),
        hottest_device: max(minute_avg)
    )
```

---

## See Also

- [Language Tutorial](../tutorials/language-tutorial.md) - Learn VPL basics
- [Performance Tuning](../guides/performance-tuning.md) - Optimize window performance
- [SASE+ Pattern Guide](../guides/sase-patterns.md) - Pattern matching over windows
