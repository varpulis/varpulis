---
title: Varpulis vs Kafka Streams / ksqlDB for CEP
description: Comparison of Varpulis with Kafka Streams and ksqlDB for complex event processing. Pattern matching, architecture, performance, and guidance on when to use each.
head:
  - - meta
    - name: keywords
      content: kafka streams cep, ksqldb complex event processing, kafka cep alternative, kafka pattern matching, stream processing vs cep, varpulis vs kafka
---

# Varpulis vs Kafka Streams / ksqlDB for CEP

Kafka Streams and ksqlDB are popular tools in the Apache Kafka ecosystem for stream processing. Varpulis is a purpose-built complex event processing (CEP) engine. While both process event streams in real time, they are designed for fundamentally different workloads.

This page explains the key differences and helps you decide when to use each -- or when to use them together.

## The Core Difference

**Kafka Streams / ksqlDB** are general-purpose stream processing tools. They excel at filtering, transforming, aggregating, and joining streams. They operate on individual events or windows of events.

**Varpulis** is a specialized CEP engine. It excels at detecting complex temporal patterns across sequences of events -- "event A followed by event B followed by one or more events C, all within 10 minutes, for the same user." This is a fundamentally different problem from filtering or aggregation.

| Capability | Kafka Streams / ksqlDB | Varpulis |
|-----------|----------------------|----------|
| Filter events | Yes | Yes |
| Transform events | Yes | Yes |
| Windowed aggregation | Yes | Yes |
| Stream joins | Yes | Yes |
| **Temporal sequences (A -> B -> C)** | Manual implementation | **Native `->` operator** |
| **Kleene closures (A+, A*)** | Not supported | **Native SASE+ operator** |
| **Negation (A -> NOT B -> C)** | Manual implementation | **Native `NOT` operator** |
| **Pattern forecasting** | Not supported | **Built-in PST engine** |
| **Multi-event correlation** | Custom code | **Declarative patterns** |

## At a Glance

| Dimension | Varpulis | Kafka Streams | ksqlDB |
|-----------|----------|--------------|--------|
| **Primary focus** | Complex event processing | Stream processing | Stream processing (SQL) |
| **Language** | VPL (declarative DSL) | Java/Kotlin API | SQL-like (KSQL) |
| **Runtime** | Native Rust binary | JVM library | JVM server |
| **Kafka dependency** | Optional (has Kafka connector) | Required | Required |
| **Deployment** | Standalone binary | Embedded in application | ksqlDB server cluster |
| **Pattern model** | SASE+ (NFA-based, academic) | None | None |
| **Memory baseline** | ~10--50 MB | 200 MB+ (JVM) | 500 MB+ (JVM + RocksDB) |
| **Cold start** | < 100 ms | Seconds (JVM + rebalance) | Seconds to minutes |

## Pattern Matching: Where the Gap Is Largest

The biggest difference between these tools is in temporal pattern detection. Consider detecting a fraud pattern: "Login from a new IP, followed by three or more small transactions, followed by a large withdrawal -- all within 2 hours, for the same user."

### Varpulis -- 20 lines of declarative pattern

```vpl
event Login:
    user_id: str
    ip_address: str

event Transaction:
    user_id: str
    amount: float
    type: str

stream FraudPattern = Login as login
    -> Transaction where user_id == login.user_id
                     and amount < 100 and type == "purchase" as small1
    .within(1h)
    -> Transaction where user_id == login.user_id
                     and amount < 100 and type == "purchase" as small2
    .within(30m)
    -> Transaction where user_id == login.user_id
                     and amount < 100 and type == "purchase" as small3
    .within(30m)
    -> Transaction where user_id == login.user_id
                     and type == "withdrawal" and amount > 1000 as withdrawal
    .within(2h)
    .emit(
        alert_type: "FRAUD_PATTERN",
        user_id: login.user_id,
        withdrawal_amount: withdrawal.amount
    )
```

### Kafka Streams -- 80+ lines of stateful processing

```java
// Kafka Streams requires manual state management for multi-step patterns
// You need: a custom Transformer, a KeyValueStore for pattern state,
// a state machine to track which step each user is on,
// timestamp checks to enforce windows, punctuators to expire
// partial patterns, and handling for out-of-order events.

events.transformValues(() -> new ValueTransformerWithKey<>() {
    private KeyValueStore<String, PatternState> stateStore;

    @Override
    public void init(ProcessorContext context) {
        this.stateStore = context.getStateStore("pattern-state");
        context.schedule(Duration.ofMinutes(1),
            PunctuationType.WALL_CLOCK_TIME, this::expireWindows);
    }

    @Override
    public Alert transform(String key, Event event) {
        // 50+ lines: manual state machine logic for
        // Login -> SmallTx -> SmallTx -> SmallTx -> LargeWithdrawal
        // with per-step timestamp checks and state transitions
    }
}, "pattern-state")
.filter((k, v) -> v != null)
.to("fraud-alerts");
```

### ksqlDB -- Not directly expressible

ksqlDB has no syntax for multi-step temporal sequences. You could chain windowed joins (one per step), but there is no way to enforce ordering between intermediate results, and each additional step requires a new stream and join. ksqlDB has no sequence operator, no Kleene closure, and no pattern state machine.

The fundamental limitation is that Kafka Streams and ksqlDB operate on **pairs** of events (via joins) or **sets** of events (via windows), but have no native concept of **ordered sequences** of events with correlation constraints.

## Kleene Closures: Variable-Length Patterns

One of the most powerful CEP features is Kleene closure -- detecting patterns with a variable number of repetitions. For example: "a rising price sequence of any length."

**Varpulis -- native Kleene+ operator**
```vpl
pattern RisingPrices = SEQ(
    StockTick as first,
    StockTick+ where price > first.price as rising,
    StockTick where price > rising.price as last
) within 60s partition by symbol
```

This pattern matches sequences of 3 or more events where prices continuously rise. Varpulis's SASE+ engine finds **all valid subsequences** -- in benchmarks, this produces 99K+ matches from 100K events, compared to ~20K with greedy matching approaches.

**Kafka Streams / ksqlDB** have no equivalent. You would need to build a custom state machine that tracks open sequences, handles branching (multiple concurrent rising sequences), and manages memory for potentially exponential match combinations. This is exactly the problem that CEP engines are designed to solve.

## Performance

### Varpulis Throughput (Measured)

From our benchmark suite, Varpulis achieves the following throughput on a single node:

| Workload | Throughput | Memory |
|----------|-----------|--------|
| Simple filter | 234K--745K events/sec | 10--54 MB |
| Windowed aggregation | 335K--1.46M events/sec | 51 MB |
| Sequence detection (A -> B) | 256K--2.15M events/sec | 36 MB |
| Kleene+ pattern | 97K--1.7M events/sec | 58 MB |
| Negation pattern | 2.2M events/sec | ~50 MB |
| Pattern forecasting | 93K events/sec | ~50 MB |

Throughput varies by benchmark mode: lower numbers are end-to-end (including I/O, parsing); higher numbers are pure engine benchmarks (no I/O).

### Kafka Streams Throughput

Kafka Streams throughput depends heavily on workload complexity:

- **Simple filter/transform**: Comparable to Varpulis (limited by Kafka broker I/O, not computation).
- **Windowed aggregation**: Well-optimized in Kafka Streams with RocksDB state.
- **Pattern detection**: No native support. Custom implementations add significant overhead from state store reads/writes, serialization/deserialization, and manual state management.

The key insight: for simple operations (filter, aggregate, join), Kafka Streams performs well because these are its core design targets. For CEP-specific operations (sequences, Kleene closures, negation), Kafka Streams requires custom code that is both harder to write and slower to execute than a purpose-built pattern engine.

### Resource Comparison

| Metric | Varpulis | Kafka Streams | ksqlDB |
|--------|----------|--------------|--------|
| Memory (simple workload) | 10 MB | 200--500 MB | 500 MB+ |
| Memory (stateful pattern) | 50 MB | 500 MB -- 2 GB | 1 GB+ |
| Cold start | < 100 ms | 5--30 seconds | 10--60 seconds |
| Kafka dependency | Optional | Required | Required |
| State storage | In-memory | RocksDB (local disk) | RocksDB |
| Horizontal scaling | Cluster mode | Kafka partition-based | ksqlDB cluster |

## Feature Comparison

| Feature | Varpulis | Kafka Streams | ksqlDB |
|---------|----------|--------------|--------|
| Event filtering | `.where()` | `filter()` | `WHERE` |
| Windowed aggregation | `.window().aggregate()` | `windowedBy()` | `WINDOW TUMBLING/HOPPING` |
| Stream joins | `.join()` | `join()` / `leftJoin()` | `JOIN ... WITHIN` |
| **Temporal sequences** | **Native `->` operator** | Custom transformer | Not expressible |
| **Kleene closures** | **Native `A+`, `A*`** | Not supported | Not supported |
| **Negation patterns** | **Native `NOT`** | Not supported | Not supported |
| **Pattern forecasting** | **Built-in PST engine** | Not supported | Not supported |
| **Trend aggregation** | **Built-in Hamlet engine** | Not supported | Not supported |
| Exactly-once | At-least-once | Exactly-once | Exactly-once |
| State recovery | Checkpointing | Changelog topics + RocksDB | Changelog topics |
| Connectors | Kafka, MQTT, NATS, PostgreSQL CDC | Kafka (native) | Kafka (native) |
| Horizontal scaling | Coordinator + workers | Kafka partition count | ksqlDB cluster |
| Interactive queries | REST API | Interactive queries | Pull queries |

## When to Use Kafka Streams / ksqlDB

Kafka Streams and ksqlDB are the better choice when you need:

- **General-purpose stream processing within the Kafka ecosystem**: Filtering, transforming, enriching, and routing events between Kafka topics. This is their core strength.
- **Simple aggregations and joins**: Counting, summing, and joining streams by key within time windows. Both tools handle this well with SQL-like syntax (ksqlDB) or a fluent API (Kafka Streams).
- **Exactly-once processing**: Kafka Streams provides exactly-once semantics via idempotent producers and transactional consumers -- a strong guarantee for financial workloads.
- **Massive state with disk-backed storage**: RocksDB state stores can handle terabytes of state, spilling to disk when RAM is insufficient.
- **No additional infrastructure**: If you already run Kafka, Kafka Streams adds no new services (it is a library). ksqlDB adds a server but is tightly integrated.
- **Simple event routing and enrichment**: Joining event streams with lookup tables, enriching events with metadata, filtering and routing to different topics.

## When to Use Varpulis

Varpulis is the better choice when you need:

- **Complex temporal patterns**: Detecting ordered sequences of events (A -> B -> C) with temporal constraints and correlation predicates. This is what Varpulis is built for.
- **Kleene closure matching**: Finding variable-length repeated patterns (one or more rising prices, three or more failed logins). Kafka Streams has no equivalent.
- **Negation in patterns**: Detecting that something did NOT happen within a sequence (login followed by NO password change within 1 hour, then large transfer).
- **Exhaustive matching**: SASE+ finds all valid pattern instances, not just the first or longest. Critical for security monitoring and compliance.
- **Pattern forecasting**: Predicting pattern completions before they happen, with 51 ns per prediction.
- **Low resource requirements**: 10--50 MB baseline vs 200 MB+ for JVM-based tools. Important for edge, IoT, or high-density container deployments.
- **Kafka-independent operation**: Varpulis works with MQTT, NATS, PostgreSQL CDC, or file-based input. Kafka is supported but not required.

## Using Them Together

Varpulis and Kafka Streams are complementary tools, not mutually exclusive. The recommended architecture uses Kafka as the event backbone, with Varpulis consuming from and producing to Kafka topics:

```
[Sources] -> [Kafka] -> [Kafka Streams: ETL/Enrichment] -> [Kafka]
                                                              |
                                                              v
                                                         [Varpulis: CEP]
                                                              |
                                                              v
                                                         [Kafka: Alerts]
                                                              |
                                                     +--------+--------+
                                                     |                 |
                                              [Alert Service]  [Dashboard]
```

### Varpulis Kafka Connector

Varpulis has a built-in Kafka connector. No Kafka Connect or external bridges required:

```vpl
connector Broker = kafka(brokers: "kafka:9092", group_id: "varpulis-cep")

event Login:
    user_id: str
    ip_address: str

event Transaction:
    user_id: str
    amount: float
    status: str

stream Logins = Login.from(Broker, topic: "user-logins")
stream Transactions = Transaction.from(Broker, topic: "user-transactions")

stream FraudAlerts = Logins as login
    -> Transactions where user_id == login.user_id
                      and status == "failed" as failed_tx
    .within(10m)
    .emit(
        alert_type: "LOGIN_THEN_FAILED_TX",
        user_id: login.user_id,
        amount: failed_tx.amount
    )
    .to(Broker, topic: "fraud-alerts")
```

This pattern is common in production: Kafka Streams handles ETL and enrichment, Varpulis handles CEP pattern detection, and alerts flow back to Kafka for downstream consumers. Each tool handles the part of the pipeline where it excels.

## Summary

Kafka Streams and ksqlDB are strong general-purpose stream processing tools that excel within the Kafka ecosystem. Varpulis is a specialized CEP engine that excels at detecting complex temporal patterns. For simple filtering, aggregation, and joins, all three tools work well. For multi-step sequences, Kleene closures, negation patterns, and pattern forecasting, Varpulis provides native capabilities that Kafka Streams and ksqlDB do not have. The two approaches are complementary: use Kafka for the event backbone and stream processing, and Varpulis for specialized pattern detection.
