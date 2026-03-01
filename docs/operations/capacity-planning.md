# Capacity Planning Guide

This guide provides sizing recommendations for deploying the Varpulis CEP engine based on measured benchmark data. All figures were collected on commodity hardware (AMD Ryzen 5800X or equivalent, DDR4-3200, NVMe SSD) and represent single-core throughput unless otherwise noted. Real-world results will vary depending on event payload size, pattern complexity, JIT/OS tuning, and I/O characteristics.

For latency and availability targets, see [SLO Definitions](slo.md).

---

## CPU Sizing

The primary factor in CPU sizing is the type of operations applied to each stream. The table below shows measured throughput per core for each operation type.

| Operation | Throughput per Core | Notes |
|-----------|-------------------|-------|
| Simple filter (`.where()`) | 234K events/sec | Predicate evaluation only |
| Sequence pattern (SASE) | 256K events/sec | 2-event sequence with time window |
| Kleene+ pattern | 97K events/sec | Match-all semantics; throughput decreases with longer matches |
| Hamlet trend aggregation (1 query) | 6.9M events/sec | Single aggregation query |
| Hamlet trend aggregation (5 queries) | 2.8M events/sec | Shared Kleene structure |
| Hamlet trend aggregation (10 queries) | 2.1M events/sec | Shared Kleene structure |
| Hamlet trend aggregation (50 queries) | 950K events/sec | Shared Kleene structure |
| PST prediction (single symbol) | ~19.6M predictions/sec | 51 ns per prediction |
| PST PMC forecast (1 active run) | 93K events/sec | 10.8 us per event |
| PST online learning | 5.4M updates/sec | Incremental tree updates |
| PST online learning + pruning | 5.0M updates/sec | With KL-divergence pruning |

### How to Estimate CPU Requirements

1. Identify the bottleneck operation for each stream (typically the slowest op in the pipeline).
2. Divide your target event rate by the per-core throughput of that operation.
3. Add 30% headroom for GC pauses, OS scheduling, and burst absorption.

**Example:** A workload of 200K events/sec through a Kleene+ pattern requires `200K / 97K = 2.06` cores. With 30% headroom: **3 cores**.

---

## Memory Sizing

| Component | Memory Usage | Notes |
|-----------|-------------|-------|
| Base process (no streams) | ~10 MB RSS | Runtime, allocator overhead |
| Per stream definition | ~2-5 KB | Stream definition, router entry, compiled ops |
| Per event in time window | ~200-500 bytes | Depends on field count and value sizes |
| SASE per active run | ~500 bytes - 2 KB | Grows with pattern length and partial matches |
| PST tree (10 event types, depth 5) | ~50-100 KB | Per forecasting stream |
| Preloaded events (CLI simulate, default) | ~400 bytes/event | Includes parsed fields and timestamp |
| Typical production (10 streams, 100K events/sec) | 50-200 MB RSS | Varies with window sizes and match fanout |

### Memory Estimation Formula

```
Total RSS = Base (10 MB)
          + Streams * 5 KB
          + Sum(window_duration_sec * event_rate * 350 bytes)   # per-stream window
          + SASE_active_runs * 1 KB                             # per-stream
          + PST_streams * 100 KB                                # if using .forecast()
          + 30% headroom
```

**Example:** 5 streams, each with a 60-second window at 20K events/sec, 100 active SASE runs per stream:

```
10 MB + 5 * 5 KB + 5 * (60 * 20000 * 350 B) + 5 * 100 * 1 KB + 30%
= 10 MB + 25 KB + 2.1 GB + 500 KB + 30%
= ~2.7 GB
```

Window size dominates memory. Reduce window durations or use `.partition_by()` to distribute state across workers when memory is constrained.

---

## Disk Sizing

| Component | Disk Usage | Notes |
|-----------|-----------|-------|
| Dead Letter Queue (DLQ) | ~500 bytes per failed event | JSONL format, includes original event + error |
| RocksDB checkpoints | Proportional to window + SASE state | Checkpoint size roughly matches in-memory state |
| Log output (INFO level) | 1-10 MB/day | Higher at DEBUG/TRACE; rotate with logrotate or equivalent |
| Binary size | ~30-50 MB | Single statically-linked binary |

### DLQ Projection

```
DLQ growth/day = failed_events_per_day * 500 bytes
```

At a 0.01% failure rate with 100K events/sec: `0.0001 * 100000 * 86400 * 500 B = ~430 MB/day`. Configure DLQ rotation accordingly.

---

## Network Bandwidth

### Per-Event Overhead by Connector

| Connector | Protocol Overhead | Typical Event Payload | Total per Event |
|-----------|------------------|----------------------|----------------|
| MQTT | ~100 bytes | 100-500 bytes | 200-600 bytes |
| NATS | ~50 bytes | 100-500 bytes | 150-550 bytes |
| Kafka | ~100 bytes | 100-500 bytes | 200-600 bytes |
| REST API | ~200 bytes | 100-500 bytes | 300-700 bytes |

### Bandwidth Estimation

```
Inbound bandwidth  = event_rate * avg_total_event_size
Outbound bandwidth = emit_rate * avg_total_event_size
```

**Example:** 100K events/sec via NATS with 300-byte average events: `100000 * 350 B = 35 MB/sec (~280 Mbps)`.

### Cluster Coordination Traffic

| Traffic Type | Size | Frequency |
|-------------|------|-----------|
| Heartbeat | ~1 KB | Every 5 seconds per worker |
| Partition assignment | ~2-5 KB | On rebalance only |
| Health check | ~500 bytes | Every 10 seconds |

Cluster coordination overhead is negligible (under 1 Mbps even with 100 workers).

---

## Reference Configurations

### Small: 10K events/sec

| Resource | Recommendation |
|----------|---------------|
| CPU | 1 core |
| RAM | 256 MB |
| Disk | 1 GB (logs + DLQ) |
| Network | 10 Mbps |
| Topology | Single process, no clustering |

**Suitable for:** Development, testing, low-volume monitoring, edge deployments.

### Medium: 100K events/sec

| Resource | Recommendation |
|----------|---------------|
| CPU | 2-4 cores |
| RAM | 1 GB |
| Disk | 10 GB (logs + DLQ + checkpoints) |
| Network | 100 Mbps |
| Topology | Single process or 2-node cluster |

**Suitable for:** Production workloads with moderate pattern complexity, typical enterprise monitoring.

### Large: 1M events/sec

| Resource | Recommendation |
|----------|---------------|
| CPU | 8-16 cores |
| RAM | 4-8 GB |
| Disk | 50 GB (logs + DLQ + checkpoints) |
| Network | 1 Gbps |
| Topology | 3+ node cluster (1 coordinator + 2+ workers) |

**Suitable for:** High-volume production, complex patterns with Kleene+, multi-query trend aggregation.

---

## Scaling Guidance

### When to Scale

| Indicator | Threshold | Action |
|-----------|-----------|--------|
| CPU utilization | > 70% sustained | Add cores or workers |
| Queue backlog | > 10K events | Add workers or increase batch size |
| Memory utilization | > 80% RSS | Reduce window sizes, add RAM, or partition |
| Event latency (p99) | > SLO target | Profile bottleneck op; scale vertically or horizontally |

### Coordinator vs Worker Sizing

- **Coordinator:** Minimal CPU requirements (mostly coordination and health monitoring). 1 core, 256-512 MB RAM is sufficient for most deployments.
- **Workers:** CPU and memory scale with event throughput and pattern complexity. Size according to the tables above.

### Vertical vs Horizontal Scaling

| Workload Type | Preferred Scaling | Rationale |
|--------------|-------------------|-----------|
| Sequence/Kleene patterns | Vertical (faster cores) | SASE runs are single-threaded per partition; single-core performance dominates |
| Multi-query aggregation | Horizontal (more workers) | Hamlet shares structure across queries; benefits from parallelism |
| High fan-out (many streams) | Horizontal (more workers) | Distribute independent streams across workers |
| PST forecasting | Vertical | Online learning and prediction are CPU-bound per stream |

### Partition-Based Scaling

Use `.partition_by(field)` in VPL to distribute events across workers by a key field. This enables horizontal scaling while maintaining ordering guarantees within each partition.

```vpl
stream Alerts = SecurityEvent as e
    .partition_by(e.source_ip)
    .within(5m)
    .where(e.severity > 3)
```

Each worker processes a disjoint subset of partitions, allowing near-linear throughput scaling.

### Scaling Limits

- Single-node practical limit: ~500K events/sec (CPU-bound operations).
- Cluster practical limit: Scales linearly with workers for partitioned workloads up to connector throughput limits.
- MQTT single-connection ceiling: ~6K events/sec (QoS 0). Use multiple connections or switch to NATS/Kafka for higher throughput.

---

## Monitoring Recommendations

Track these metrics for capacity planning decisions:

- **`varpulis_events_processed_total`** -- Total events processed (rate = throughput).
- **`varpulis_event_latency_p99`** -- Processing latency at p99.
- **`process_resident_memory_bytes`** -- RSS memory usage.
- **`varpulis_sase_active_runs`** -- Number of active SASE pattern runs (correlates with memory and CPU).
- **`varpulis_queue_depth`** -- Internal queue backlog (leading indicator of saturation).
- **`varpulis_dlq_events_total`** -- Dead letter queue growth rate.

Set alerts at 70% of capacity limits to allow time for scaling actions before SLO breaches.