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

## Comparison vs Apama (measured)

Benchmarked against Apama Community Edition v27.18, 100K events, 7 scenarios.
See [`benchmarks-apama-comparison.md`](../benchmarks-apama-comparison.md) for full details.

| Metric | Apama | Varpulis | Result |
|--------|-------|----------|--------|
| Throughput (CLI) | 195–221K/s | 234–335K/s | **V 1.2–1.3x faster** |
| Throughput (MQTT) | ~6K/s | ~6K/s | Tie (I/O-bound) |
| Memory (connector) | 85–153 MB | 10–57 MB | **V 2x–16x less** |
| Kleene matches | 20K | 100K | **V 5x more complete** |

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
