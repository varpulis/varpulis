# Performance Tuning Guide

Optimize Varpulis for maximum throughput, minimum latency, and efficient resource usage.

## Performance Overview

Varpulis is designed for high-throughput stream processing. Typical performance:

| Configuration | Throughput | Latency (p99) |
|--------------|------------|---------------|
| Single thread, simple filter | 500K+ events/sec | < 1ms |
| 8 workers, aggregations | 1M+ events/sec | < 5ms |
| Complex patterns, SIMD | 200K+ events/sec | < 10ms |

---

## Throughput Optimization

### Batch Processing Mode

For offline analytics, use batch mode to skip timing:

```bash
# Basic batch mode
varpulis simulate -p rules.vpl -e data.evt --immediate

# With preloading (faster for multiple passes)
varpulis simulate -p rules.vpl -e data.evt --immediate --preload
```

### Parallel Workers

Scale across CPU cores:

```bash
# Use 8 workers
varpulis simulate -p rules.vpl -e data.evt --immediate --workers 8

# Use all available cores
varpulis simulate -p rules.vpl -e data.evt --immediate --workers $(nproc)
```

### Partitioning

Events are distributed across workers by partition key:

```bash
# Partition by device for IoT data
varpulis simulate ... --workers 8 --partition-by device_id

# Partition by user for user activity
varpulis simulate ... --workers 8 --partition-by user_id
```

**Best practices:**
- Choose a field with good distribution (many unique values)
- Avoid high-cardinality keys that create too many partitions
- Related events (same user/device) should go to same partition

### Configuration File for Workers

```yaml
processing:
  workers: 8
  partition_by: device_id
```

---

## Context-Based Parallelism

For multi-core scaling, use named contexts to distribute streams across OS threads:

```vpl
context ingestion (cores: [0, 1])
context analytics (cores: [2, 3])
context alerts (cores: [4])

stream FastFilter = RawEvents
    .context(ingestion)
    .where(value > 0)
    .emit(context: analytics, data: data)

stream HeavyCompute = FastFilter
    .context(analytics)
    .window(1m)
    .aggregate(avg: avg(value), stddev: stddev(value))
```

### Context Tuning Tips

- **Separate I/O from compute**: Put high-throughput ingestion on its own context
- **Filter before crossing**: Reduce event volume before cross-context emits
- **Match NUMA topology**: Pin cooperating contexts to cores on the same NUMA node
- **Reserve core 0**: System interrupts typically run on core 0
- **Monitor per-context**: Use Prometheus metrics to identify bottleneck contexts

For a complete guide, see [Context-Based Execution](contexts.md).

---

## SIMD Optimization

Varpulis uses SIMD (AVX2) for vectorized aggregations on x86_64.

### Automatic SIMD

SIMD is automatically used for:
- `sum()` - 4x parallel f64 addition
- `avg()` - Vectorized sum + count
- `min()` / `max()` - Parallel comparisons

### Verifying SIMD is Active

```bash
# Check CPU supports AVX2
grep avx2 /proc/cpuinfo

# Run with debug logging to confirm
RUST_LOG=varpulis_runtime::simd=debug varpulis simulate ...
```

### Performance Impact

| Operation | Scalar | SIMD (AVX2) | Improvement |
|-----------|--------|-------------|-------------|
| sum(1M values) | 2.5ms | 0.6ms | 4x |
| avg(1M values) | 2.5ms | 0.7ms | 3.5x |
| min/max(1M values) | 2.0ms | 0.5ms | 4x |

### Loop Unrolling Fallback

On non-AVX2 systems, 4-way loop unrolling provides ~2x speedup:

```rust
// Automatic fallback in simd.rs
// 4-way unrolling for better ILP on all architectures
```

---

## Incremental Aggregation

For sliding windows, incremental aggregation avoids recomputing the entire window.

### How It Works

Traditional: Recalculate sum of all N events on every slide
Incremental: Add new events, subtract expired events (O(1) vs O(n))

### Enabling Incremental Mode

Incremental aggregation is automatic for compatible operations:
- `sum()` - Fully incremental
- `count()` - Fully incremental
- `avg()` - Incremental (via sum/count)

### Operations That Require Full Recomputation

- `min()` / `max()` - Must track all values (heap-based optimization available)
- `percentile()` - Requires sorted data
- `stddev()` - Can use Welford's algorithm for incremental

### Implementation Details

```rust
// IncrementalSum in simd.rs
pub struct IncrementalSum {
    sum: f64,
    count: usize,
}

impl IncrementalSum {
    pub fn add(&mut self, value: f64) { ... }     // O(1)
    pub fn remove(&mut self, value: f64) { ... }  // O(1)
    pub fn sum(&self) -> f64 { self.sum }         // O(1)
}
```

---

## Memory Optimization

### Arc<Event> Sharing

Events are wrapped in `Arc` for efficient sharing:

```rust
pub type SharedEvent = Arc<Event>;
```

Benefits:
- Multiple pattern runs share the same event data
- No deep copying when events enter multiple streams
- Reference counting instead of cloning

### Window Memory Management

| Window Type | Memory Usage |
|-------------|--------------|
| Tumbling | O(events in current window) |
| Sliding | O(events in window size) |
| Count | O(window size) - fixed |
| Partitioned | O(partitions × window size) |

**Recommendations:**

1. **Prefer count windows for bounded memory:**
```vpl
// Fixed memory: exactly 1000 events
.window(1000)
```

2. **Use shorter time windows:**
```vpl
// 5 minutes instead of 1 hour = 12x less memory
.window(5m)
```

3. **Limit partition cardinality:**
```vpl
// Bad: unbounded partitions
.partition_by(request_id)

// Good: bounded partitions
.partition_by(region)  // Limited number of regions
```

### ZDD Optimization

Zero-suppressed Decision Diagrams (ZDD) compress pattern representations:

- Reduces memory for complex patterns
- Faster subset/superset operations
- Automatic sharing of common subpatterns

### Monitoring Memory

```bash
# Watch process memory
watch -n 1 'ps aux | grep varpulis | awk "{print \$6/1024 \" MB\"}"'

# With metrics enabled
curl -s localhost:9090/metrics | grep memory
```

---

## Prometheus Monitoring

### Enabling Metrics

```bash
varpulis server --metrics --metrics-port 9090
```

### Key Metrics to Monitor

| Metric | What to Watch |
|--------|---------------|
| `varpulis_events_processed_total` | Throughput rate |
| `varpulis_event_processing_duration_seconds` | Latency percentiles |
| `varpulis_alerts_generated_total` | Alert rate |
| `varpulis_active_patterns` | Pattern state memory |

### Grafana Dashboard Example

```json
{
  "panels": [
    {
      "title": "Throughput",
      "targets": [{
        "expr": "rate(varpulis_events_processed_total[1m])"
      }]
    },
    {
      "title": "P99 Latency",
      "targets": [{
        "expr": "histogram_quantile(0.99, varpulis_event_processing_duration_seconds_bucket)"
      }]
    }
  ]
}
```

### Alerting Rules

```yaml
groups:
  - name: varpulis
    rules:
      - alert: HighLatency
        expr: histogram_quantile(0.99, varpulis_event_processing_duration_seconds_bucket) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Varpulis p99 latency above 100ms"

      - alert: LowThroughput
        expr: rate(varpulis_events_processed_total[5m]) < 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Varpulis throughput below 1000 events/sec"
```

---

## Query Optimization

### Filter Early

Put selective filters first to reduce data volume:

```vpl
// Good: filter first, then aggregate
stream Optimized = SensorReading
    .where(sensor_type == "temperature")  // Filter first
    .window(1m)
    .aggregate(avg: avg(value))

// Less optimal: aggregate all, then filter
stream LessOptimal = SensorReading
    .window(1m)
    .aggregate(avg: avg(value), type: first(sensor_type))
    .where(type == "temperature")  // Filter after aggregation
```

### Minimize Pattern Complexity

Simpler patterns match faster:

```vpl
// Complex: 4 states, Kleene closure
pattern Complex = A+ -> B -> C+ -> D

// Simpler: can often be split
pattern Step1 = A -> B
pattern Step2 = C -> D
```

### Use Appropriate Timeouts

Shorter timeouts = less state to track:

```vpl
// Keeping state for 24 hours is expensive
pattern Long = A -> B within 24h

// If 5 minutes is sufficient, use that
pattern Short = A -> B within 5m
```

### Avoid Unbounded Kleene

```vpl
// Bad: could match millions of events
pattern Unbounded = Event* -> End

// Better: bounded by timeout
pattern Bounded = Event* -> End within 5m

// Even better: add constraints
pattern Constrained = Event[type == "relevant"]* -> End within 5m
```

---

## Benchmarking

### Running Benchmarks

```bash
# Run all benchmarks
cargo bench -p varpulis-runtime

# Specific benchmark
cargo bench -p varpulis-runtime pattern_benchmark
```

### Available Benchmarks

| Benchmark | What It Tests |
|-----------|---------------|
| `pattern_benchmark` | SASE+ pattern matching throughput |
| `attention_benchmark` | Attention engine performance |
| `kleene_benchmark` | Kleene closure overhead |
| `comparison_benchmark` | Cross-engine comparisons |

### Creating Custom Benchmarks

```rust
#[bench]
fn bench_my_pattern(b: &mut Bencher) {
    let events = generate_test_events(10000);
    let engine = setup_engine_with_pattern();

    b.iter(|| {
        for event in &events {
            engine.process(event.clone());
        }
    });
}
```

### Comparing Configurations

```bash
# Baseline
varpulis simulate -p rules.vpl -e large.evt --immediate 2>&1 | grep "Event rate"

# With more workers
varpulis simulate -p rules.vpl -e large.evt --immediate --workers 8 2>&1 | grep "Event rate"

# With partitioning
varpulis simulate -p rules.vpl -e large.evt --immediate --workers 8 --partition-by device_id 2>&1 | grep "Event rate"
```

---

## System Tuning

### Linux Kernel Parameters

```bash
# Increase file descriptor limits
ulimit -n 65535

# Increase network buffer sizes
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216

# TCP tuning for high throughput
sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"
sysctl -w net.ipv4.tcp_wmem="4096 65536 16777216"
```

### NUMA Awareness

For multi-socket systems:

```bash
# Pin to specific NUMA node
numactl --cpunodebind=0 --membind=0 varpulis simulate ...

# Check NUMA topology
numactl --hardware
```

### CPU Pinning

```bash
# Pin entire process to specific cores
taskset -c 0-7 varpulis simulate ... --workers 8
```

For finer-grained control, use VPL contexts with CPU affinity:

```vpl
context ingest (cores: [0, 1])
context analyze (cores: [2, 3])
```

### Memory Limits

```bash
# Set memory limit (prevent OOM killer from killing other processes)
ulimit -v 8000000  # 8GB virtual memory limit

# Or use cgroups
cgcreate -g memory:varpulis
cgset -r memory.limit_in_bytes=8G varpulis
cgexec -g memory:varpulis varpulis simulate ...
```

---

## Performance Checklist

### Before Production

- [ ] Consider using contexts for multi-core parallelism
- [ ] Run with `--workers` matching CPU cores
- [ ] Enable `--partition-by` for parallel streams
- [ ] Set appropriate window timeouts
- [ ] Enable metrics monitoring
- [ ] Set memory limits
- [ ] Configure logging level (info, not debug/trace)

### During Load Testing

- [ ] Monitor CPU utilization (should be high but not 100%)
- [ ] Check memory growth rate
- [ ] Measure p99 latency
- [ ] Verify alert generation rate
- [ ] Test failover/restart scenarios

### In Production

- [ ] Set up Prometheus scraping
- [ ] Configure alerting rules
- [ ] Monitor disk space (if using persistence)
- [ ] Review logs for warnings
- [ ] Track throughput trends

---

## Troubleshooting Performance

### Low Throughput

1. Check CPU utilization - if low, add more workers
2. Look for blocking operations (HTTP webhooks)
3. Simplify complex patterns
4. Verify SIMD is being used

### High Latency

1. Reduce window sizes
2. Add more partitions
3. Check for memory pressure
4. Profile with `perf` or flamegraph

### Memory Growth

1. Add timeouts to patterns
2. Reduce window durations
3. Limit partition cardinality
4. Use shorter window durations or count-based windows

---

## See Also

- [Context-Based Execution](contexts.md) - Multi-threaded contexts with CPU affinity
- [Configuration Guide](configuration.md) - Worker and partitioning config
- [Troubleshooting](troubleshooting.md) - Debug performance issues
- [Windows & Aggregations](../reference/windows-aggregations.md) - SIMD details
