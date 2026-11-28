# Performance Objectives

## Latency (low_latency mode)

| Percentile | Target |
|------------|----------|
| p50 | < 500 µs |
| p95 | < 2 ms |
| p99 | < 5 ms |

## Throughput (mode throughput)

| Scenario | Target |
|----------|----------|
| Simple events | > 1M events/sec (single node) |
| With complex aggregations | > 500K events/sec |

## Measured (2026-02-09)

7 CEP scenarios, 100K events, median of 3 runs.

| Mode | Throughput | Memory |
|------|------------|--------|
| CLI ramdisk (CPU-bound) | 234–335K events/sec | 36–66 MB |
| MQTT connector (I/O-bound) | 6–10K events/sec | 10–57 MB |

Optimizations behind these figures:

- **PERF-04 (2026-01-31)**: `event_sources` moved from `HashMap<String, Vec<String>>` to `Arc<[String]>`, so a clone is an atomic increment instead of a deep copy (~4–6%).
- **PERF-05 (2026-02-07)**: sync processing path, zero-clone preload, batch size 1K→10K, zero-alloc field splitting (~40%, 140K→234K events/sec on the filter scenario).

## Performance Modes

### `low_latency` Mode
- Priority on minimal latency
- Batch size = 1
- No buffering
- Ideal for: trading, real-time alerting

### `throughput` Mode
- Priority on maximum throughput
- Aggressive batching
- Optimized buffering
- Ideal for: analytics, massive aggregations

### `balanced` Mode
- Latency/throughput compromise
- Default configuration
- Ideal for: general use cases

## Benchmarks to Implement

1. **Latency benchmark**: Measure p50/p95/p99 on simple events
2. **Throughput benchmark**: Maximum sustained events/sec
3. **Memory benchmark**: Memory consumption under load
4. **Pattern matching benchmark**: Pattern complexity vs performance
5. **Multi-query benchmark**: Hamlet vs ZDD multi-query performance
