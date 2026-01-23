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
| With attention | > 100K events/sec |
| With complex aggregations | > 500K events/sec |

## Comparison vs Apama (indicative)

| Metric | Apama | Varpulis (target) |
|--------|-------|-------------------|
| Latency | Reference | Similar in low_latency mode |
| Throughput | Reference | 2-3x higher thanks to Rust |
| Memory footprint | Reference | -40% |

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
5. **Attention benchmark**: Impact of head count on latency
