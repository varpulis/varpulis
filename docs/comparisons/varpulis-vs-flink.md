---
title: Varpulis vs Apache Flink CEP
description: Detailed comparison of Varpulis and Apache Flink for complex event processing (CEP). Benchmarks, code examples, architecture differences, and guidance on choosing the right engine.
head:
  - - meta
    - name: keywords
      content: flink cep alternative, flink complex event processing, varpulis vs flink, apache flink cep comparison, cep engine comparison, stream processing
---

# Varpulis vs Apache Flink CEP

Apache Flink is a widely adopted distributed stream processing framework with a CEP library. Varpulis is a purpose-built complex event processing engine with a declarative pattern language. Both can detect temporal patterns in event streams, but they take fundamentally different approaches.

This page compares the two engines using real benchmark data, code examples, and architectural analysis to help you decide which fits your use case.

## At a Glance

| Dimension | Varpulis | Apache Flink CEP |
|-----------|----------|------------------|
| **Primary focus** | Complex event processing | General-purpose stream processing |
| **Language** | VPL (declarative DSL) | Java/Scala API |
| **Runtime** | Native Rust binary | JVM (Java) |
| **Pattern model** | SASE+ with Kleene closures | Flink CEP library (NFA-based) |
| **Deployment** | Single binary, optional cluster | Distributed cluster (JobManager + TaskManagers) |
| **Cold start** | < 100 ms | 5 -- 30 seconds |
| **Memory baseline** | ~50 MB | 500 MB -- 2 GB |
| **License** | Open source | Apache 2.0 |

## Code Comparison

One of the most visible differences is how much code each engine requires for the same pattern. Across five benchmark scenarios, Varpulis requires **3--5x fewer lines of code** than Flink.

### Temporal Sequence: Login followed by Failed Transaction

**Varpulis -- 35 lines**

```vpl
event Login:
    user_id: str
    ip_address: str
    device: str

event Transaction:
    user_id: str
    amount: float
    status: str
    merchant: str

stream SuspiciousActivity = Login as login
    -> Transaction where user_id == login.user_id
                     and status == "failed" as failed_tx
    .within(10m)
    .emit(
        alert_type: "LOGIN_THEN_FAILED_TX",
        user_id: login.user_id,
        severity: if failed_tx.amount > 1000 then "high" else "medium"
    )
```

**Flink CEP -- 160+ lines** (showing the core pattern definition; full implementation includes source/sink functions, event classes, watermark strategy, and MQTT handling)

```java
Pattern<UserEvent, ?> pattern = Pattern.<UserEvent>begin("login")
    .where(new SimpleCondition<UserEvent>() {
        @Override
        public boolean filter(UserEvent event) {
            return "Login".equals(event.type);
        }
    })
    .followedBy("failedTx")
    .where(new IterativeCondition<UserEvent>() {
        @Override
        public boolean filter(UserEvent event, Context<UserEvent> ctx) {
            if (!"Transaction".equals(event.type)) return false;
            if (!"failed".equals(event.status)) return false;
            for (UserEvent login : ctx.getEventsForPattern("login")) {
                if (login.userId.equals(event.userId)) {
                    return true;
                }
            }
            return false;
        }
    })
    .within(Time.seconds(5));

// Plus: DataStream source, PatternStream, select function,
// Sink function, Watermark strategy, Event/Alert POJOs, MQTT I/O...
```

### Code Conciseness Across All Scenarios

| Scenario | Varpulis (lines) | Flink (lines) | Ratio |
|----------|-----------------|---------------|-------|
| Simple aggregation | 34 | 111 | **3.3x less** |
| Temporal sequence (A -> B) | 34 | 158 | **4.6x less** |
| Fraud detection (A -> B -> C) | 49 | 207 | **4.2x less** |
| Stream join (arbitrage) | 51 | 103 | **2.0x less** |
| Anomaly detection (SASE+) | 77 | 200+ (estimated) | **~3x less** |
| **Average** | **49** | **145** | **3x less** |

The difference comes from VPL being a purpose-built language for CEP patterns. Flink requires explicit Java class definitions, anonymous inner classes for conditions, and boilerplate for the streaming pipeline.

## Performance Comparison

### Latency (Event-Time Mode, MQTT Benchmark)

Both engines were tested on the same Login -> FailedTransaction pattern with 11 events, event-time semantics, and watermarks. Both produced correct results (4/4 alerts).

| Metric | Varpulis | Flink CEP | Difference |
|--------|----------|-----------|------------|
| **Average latency** | 554 ms | 926 ms | **Varpulis 40% faster** |
| Minimum latency | 202 ms | 756 ms | Varpulis 3.7x faster |
| Maximum latency | 905 ms | 1,249 ms | Varpulis 1.4x faster |

Flink's higher latency is primarily due to its watermark-based window completion mechanism. Flink waits for watermarks to advance past the window boundary before emitting matches, which adds inherent delay. Varpulis emits alerts immediately upon pattern completion.

### Throughput (CPU-Bound, CLI Mode)

Pattern matching throughput measured with the SASE+ engine on 10K events:

| Pattern Type | Throughput |
|-------------|-----------|
| Simple sequence (A -> B) | **320K events/sec** |
| Kleene+ (A -> B+ -> C) | **200K events/sec** |
| Complex with negation | **220K events/sec** |
| Long sequence (10 events) | 26K events/sec |

In end-to-end benchmarks with 100K events (including I/O and parsing):

| Scenario | Varpulis | Notes |
|----------|----------|-------|
| Simple filter | 234K events/sec | 54 MB RSS |
| Sequence detection | 256K events/sec | 36 MB RSS |
| Temporal join | 268K events/sec | 66 MB RSS |
| Kleene+ pattern | 97K events/sec | Exhaustive matching (99K+ matches) |

### Startup and Memory

| Metric | Varpulis | Flink |
|--------|----------|-------|
| Cold start to first event | < 1 ms | 100 -- 500 ms |
| Cluster startup | < 100 ms | 5 -- 30 seconds |
| Memory (simple filter) | ~10 MB | 500 MB -- 2 GB |
| Memory (complex pattern) | ~50 MB | 500 MB -- 2 GB |

The memory difference comes from the runtime: Flink runs on the JVM with JIT compilation, class metadata, garbage collector heaps, and the full distributed framework loaded. Varpulis is a native Rust binary with no runtime overhead.

## Architecture Differences

### Varpulis

- **Single native binary** compiled from Rust. No JVM, no runtime dependencies.
- **SASE+ pattern engine** based on academic research (VLDB). Supports sequences, Kleene closures, negation, and event correlation natively.
- **Optional cluster mode** with coordinator/worker topology over NATS.
- **Built-in connectors** for Kafka, MQTT, NATS, and PostgreSQL CDC.
- **Event-time and processing-time** modes with watermark support.

### Apache Flink

- **Distributed framework** with JobManager, TaskManagers, and optional ZooKeeper coordination.
- **CEP is a library** (`flink-cep`) on top of the core DataStream API. Pattern matching is one of many capabilities.
- **State backends** including RocksDB for large state. Checkpointing and savepoints for exactly-once semantics.
- **Massive ecosystem** of connectors, file system integrations, and third-party tools.
- **Horizontal scalability** across hundreds of TaskManagers.

## Feature Comparison

| Feature | Varpulis | Flink CEP |
|---------|----------|-----------|
| Temporal sequences (A -> B) | Native `->` operator | `followedBy()` API |
| Kleene closures (A+, A*) | Native SASE+ with ZDD storage | Supported via `oneOrMore()` |
| Negation (NOT B) | Native `NOT` pattern | `notFollowedBy()` |
| Windowed aggregation | `.window()` + `.aggregate()` | Window API + ProcessFunction |
| Stream joins | Declarative `.join()` | `CoProcessFunction` or interval join |
| Pattern forecasting | Built-in PST engine (51 ns/prediction) | Not available |
| Trend aggregation | Built-in Hamlet engine | Requires custom implementation |
| Exactly-once semantics | At-least-once | Exactly-once with checkpointing |
| Horizontal scaling | Cluster mode (coordinator + workers) | Full distributed (100s of nodes) |
| State backends | In-memory | RocksDB, heap, HashMapStateBackend |
| Savepoints / migration | Checkpointing (in development) | Mature savepoint/checkpoint |
| Web UI | Built-in Vue.js dashboard | Flink Web UI |
| Connectors | Kafka, MQTT, NATS, PostgreSQL CDC | 50+ connectors |

## When to Use Flink

Flink is the better choice when you need:

- **Massive horizontal scale**: Processing millions of events per second across a large cluster. Flink's distributed architecture handles this natively.
- **Exactly-once processing guarantees**: Financial or compliance workloads where every event must be processed exactly once, with checkpointed state recovery.
- **Terabytes of state**: RocksDB state backend can manage state far beyond available RAM.
- **Existing JVM ecosystem**: If your team already uses Java/Scala and has Flink operational expertise.
- **Broad connector coverage**: If you need connectors beyond Kafka/MQTT/NATS (e.g., Cassandra, Elasticsearch, JDBC, Kinesis).
- **General stream processing alongside CEP**: If CEP is just one part of a larger stream processing pipeline with complex transformations, enrichment, and routing.

## When to Use Varpulis

Varpulis is the better choice when you need:

- **Low-latency pattern detection**: 40% lower latency than Flink on equivalent patterns, with sub-millisecond processing per event.
- **Minimal resource footprint**: 10--50 MB vs 500 MB -- 2 GB. Ideal for edge deployments, containers, or cost-sensitive environments.
- **Rapid development**: 3--5x less code to express the same pattern. VPL is purpose-built for CEP, so patterns are readable and maintainable.
- **Advanced CEP features**: Built-in pattern forecasting (PST), trend aggregation (Hamlet), and exhaustive Kleene matching -- capabilities that would require custom Flink ProcessFunctions.
- **Fast startup**: Sub-100ms cold start vs Flink's 5--30 second cluster initialization. Critical for serverless or auto-scaling scenarios.
- **Operational simplicity**: Single binary deployment, no JVM tuning, no cluster coordination overhead for single-node workloads.

## Integration with Flink

Varpulis is not necessarily a replacement for Flink. The two can work together:

- Use Flink for large-scale data preparation, ETL, and general stream processing.
- Route events through Kafka to Varpulis for specialized CEP pattern detection.
- Varpulis outputs alerts back to Kafka for downstream Flink jobs or other consumers.

```
[Data Sources] -> [Flink ETL] -> [Kafka] -> [Varpulis CEP] -> [Kafka] -> [Alerts/Actions]
```

## Benchmark Methodology

All benchmarks referenced on this page were run on the same hardware (Linux 6.6.87, WSL2). The Flink latency benchmark used event-time semantics with watermarks on both sides:

- **Varpulis**: Rust release build, event-time mode with automatic watermark generation.
- **Flink**: JDK 11, `forMonotonousTimestamps()` with `withIdleness(2s)` for watermark progression.
- **Pattern**: Login -> FailedTransaction (same user_id, within 5 seconds).
- **Both engines produced correct results** (4/4 expected alerts).

Throughput benchmarks used 10K--100K events in CPU-bound mode (ramdisk, no I/O overhead) with median-of-3 reporting.

For full details and reproduction instructions, see the [benchmark results](https://github.com/varpulis/varpulis/tree/main/benchmarks/flink-comparison).

## Summary

Flink is a mature, battle-tested distributed stream processing framework with a broad ecosystem. Varpulis is a focused CEP engine that trades Flink's generality for significantly lower latency, smaller resource footprint, and a more expressive pattern language. If your primary need is complex event pattern detection rather than general stream processing, Varpulis delivers faster results with less code and fewer resources.
