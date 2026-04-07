---
title: Varpulis vs Esper CEP
description: Comparison of Varpulis and Esper for complex event processing. Language examples, performance characteristics, architecture differences, and guidance on choosing the right CEP engine.
head:
  - - meta
    - name: keywords
      content: esper cep alternative, esper complex event processing, varpulis vs esper, epl vs vpl, cep engine comparison, event processing language
---

# Varpulis vs Esper CEP

Esper is one of the most established CEP engines, offering a SQL-like Event Processing Language (EPL) for pattern matching and stream analytics. Varpulis is a newer CEP engine built in Rust with a declarative pattern language (VPL) and a SASE+-based pattern engine. Both are pattern-focused CEP systems, but differ in architecture, language design, and runtime characteristics.

This page compares the two to help you decide which fits your requirements.

## At a Glance

| Dimension | Varpulis | Esper |
|-----------|----------|-------|
| **Language** | VPL (declarative DSL) | EPL (SQL-like) |
| **Runtime** | Native Rust binary | JVM (Java library) |
| **Deployment model** | Standalone engine / cluster | Embedded Java library |
| **Pattern model** | SASE+ (academic, NFA-based) | Custom pattern engine |
| **Memory baseline** | ~10--50 MB | 200--500 MB (JVM overhead) |
| **Cold start** | < 100 ms | Seconds (JVM + class loading) |
| **License** | Open source | GPL v2 (community) / Commercial |

## Language Comparison: EPL vs VPL

Both Esper and Varpulis use declarative languages for defining event patterns. Esper's EPL uses SQL-like syntax; Varpulis's VPL uses a purpose-built notation with the `->` sequence operator.

### Simple Filter

**VPL (Varpulis)**
```vpl
event StockTick:
    symbol: str
    price: float
    volume: int

stream Filtered = StockTick
    .where(price > 50.0)
    .emit(event_type: "FilteredTick", symbol: symbol, price: price)
```

**EPL (Esper)**
```sql
@Name('FilteredTicks')
select symbol, price
from StockTick
where price > 50.0
```

For simple filters, both languages are concise. EPL feels natural to anyone familiar with SQL.

### Temporal Sequence (A -> B)

**VPL (Varpulis)**
```vpl
event Login:
    user_id: str
    ip_address: str

event Transaction:
    user_id: str
    amount: float
    status: str

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

**EPL (Esper)**
```sql
@Name('SuspiciousActivity')
select a.userId as userId, a.ipAddress as ip,
       b.amount as amount, b.merchant as merchant
from pattern [
    every a=Login -> b=Transaction(userId=a.userId, status='failed')
        where timer:within(10 min)
]
```

Both handle A -> B sequences well. Esper's `pattern [...]` syntax is powerful. VPL's `->` operator and `.within()` chain read more naturally for complex multi-step patterns.

### Kleene Closure (Repeated Patterns)

**VPL (Varpulis) -- native SASE+ Kleene+**
```vpl
event StockTick:
    symbol: str
    price: float
    volume: int
    ts: timestamp

pattern RisingSequence = StockTick as first
    -> all StockTick where price > first.price as rising
    -> StockTick where price > rising.price as last
    within 60s partition by symbol
```

**EPL (Esper) -- `every` with `until`**
```sql
@Name('RisingSequence')
select * from pattern [
    every first=StockTick ->
        ([2:] rising=StockTick(price > first.price)
         until StockTick(price <= first.price))
]
```

Esper supports bounded and unbounded repetition via `[min:max]` syntax and `until` guards. Varpulis uses SASE+ Kleene+ (`+`) and Kleene* (`*`) operators with exhaustive match semantics -- meaning all valid subsequences are reported, not just the longest. In benchmarks, this finds **5x more valid pattern instances** than greedy matching approaches.

### Windowed Aggregation

**VPL (Varpulis)**
```vpl
stream PageStats = PageView
    .partition_by(category)
    .window(5m, sliding: 30s)
    .aggregate(
        category: last(category),
        view_count: count(),
        unique_users: count_distinct(user_id),
        avg_duration: avg(duration_ms)
    )
```

**EPL (Esper)**
```sql
@Name('PageStats')
select category, count(*) as viewCount,
       count(distinct userId) as uniqueUsers,
       avg(durationMs) as avgDuration
from PageView#time(5 min)
group by category
output snapshot every 30 seconds
```

Both handle windowed aggregation naturally. Esper's `#time()` window syntax and `output` clause are well-established. VPL uses a method-chaining style.

### Pattern Forecasting (Varpulis Only)

```vpl
stream FraudForecast = Login as login
    -> Transaction where user_id == login.user_id
                     and status == "failed" as failed_tx
    .within(5m)
    .forecast(confidence: 0.7, horizon: 2m, warmup: 500, max_depth: 5)
    .where(forecast_probability > 0.8)
    .emit(
        user_id: login.user_id,
        probability: forecast_probability,
        expected_time: forecast_time
    )
```

Varpulis can predict pattern completions before they happen using a built-in Prediction Suffix Tree (PST) engine. This capability does not exist in Esper and would require integrating an external ML system.

## Performance Characteristics

Direct head-to-head benchmarks between Varpulis and Esper are not available in our test suite. However, the architectural differences lead to predictable performance characteristics based on benchmarks against other JVM-based engines (Flink CEP, Apama).

### Native Rust vs JVM

| Metric | Varpulis (measured) | Typical JVM CEP engine |
|--------|--------------------|-----------------------|
| Cold start | < 100 ms | 2--10 seconds |
| Memory (simple filter) | 10 MB | 200--500 MB |
| Memory (complex patterns) | 50 MB | 500 MB -- 1 GB |
| Sequence detection | 256K--2.15M events/sec | 100K--400K events/sec |
| Kleene+ matching | 97K--1.7M events/sec | Varies by implementation |

Varpulis's Rust-native runtime avoids JVM overhead: no garbage collection pauses, no JIT compilation warmup, no class metadata. This translates to consistently lower memory usage and more predictable latency.

### Benchmarked Against Other JVM Engines

From our Apama (JVM-based) benchmarks with 100K events:

| Scenario | Varpulis | Apama (JVM) | Memory Ratio |
|----------|----------|-------------|-------------|
| Simple filter | 234K/s | 199K/s | **Varpulis 3.1x less RAM** |
| Sequence (A -> B) | 256K/s | 221K/s | **Varpulis 5.1x less RAM** |
| Kleene+ pattern | 97K/s (99K matches) | 195K/s (20K matches) | **Varpulis 3.3x less RAM** |
| EMA crossover | 266K/s | 212K/s | **Varpulis 3.5x less RAM** |

The Kleene+ result is particularly notable: Apama's greedy matching is faster on raw throughput but misses 80% of valid matches. Varpulis's SASE+ exhaustive semantics find all valid subsequences.

Esper, also running on the JVM, would exhibit similar memory and startup characteristics to Apama. Throughput depends on the specific pattern and Esper version, but the 3--10x memory advantage of a native binary is structural.

## Feature Comparison

| Feature | Varpulis | Esper |
|---------|----------|-------|
| Temporal sequences | Native `->` operator | `pattern [a -> b]` |
| Kleene closures | SASE+ exhaustive (`A+`, `A*`) | Bounded/unbounded with `until` |
| Negation | `NOT` pattern operator | `not` in pattern expressions |
| Event windows | Time, count, sliding | Time, count, length, expression, snapshot |
| Output limiting | Tumbling/sliding emit | `output first/last/all/snapshot every N` |
| Aggregation | `sum`, `count`, `avg`, `min`, `max`, `ema`, `count_distinct` | Full SQL aggregation + custom |
| Stream joins | Declarative `.join()` | SQL-style `from A, B where ...` |
| Named windows | Not applicable (stream-oriented) | Create window + insert/select |
| Pattern forecasting | Built-in PST (51 ns/prediction) | Not available |
| Trend aggregation | Built-in Hamlet engine | Not available |
| Contexts / partitioning | `.partition_by()` + contexts | Context partitions, keyed/hash/category |
| On-demand queries | REST API | Fire-and-forget queries |
| Connectors | Kafka, MQTT, NATS, PostgreSQL CDC | User-supplied (embedded library) |
| Deployment | Standalone binary + cluster | Embedded in Java application |
| Language | VPL (custom DSL) | EPL (SQL-like) |
| Type system | Static (str, int, float, bool, timestamp) | Java types |

### Where Esper Has an Edge

- **Mature EPL language**: 15+ years of production use. EPL is well-documented with a rich set of window types, output clauses, and pattern operators.
- **Context partitions**: Esper's context system (keyed, hash, category, initiated/terminated) is more flexible than Varpulis's current partitioning.
- **Named windows**: First-class named windows that can be queried on demand, populated from multiple streams, and shared across statements.
- **Custom aggregation functions**: Java-based custom aggregation plugins.
- **Embedding**: Esper is designed as a Java library. It integrates directly into Java applications without network hops.

### Where Varpulis Has an Edge

- **Resource efficiency**: 10--50 MB vs 200 MB+ baseline. No GC pauses. Predictable latency.
- **Exhaustive Kleene matching**: SASE+ finds all valid pattern instances, not just the longest. Critical for security and fraud detection where every match matters.
- **Pattern forecasting**: Predict pattern completion probability and timing before it happens (51 ns per prediction).
- **Trend aggregation**: Hamlet engine provides optimized multi-query trend aggregation with up to 100x speedup over naive approaches for 50 concurrent patterns.
- **Standalone deployment**: No Java application required. Deploy as a single binary with built-in connectors.
- **Fast iteration**: Modify VPL files and restart in < 100 ms. No compilation or class loading.

## Architecture Differences

**Esper** is an embeddable Java library. You instantiate an engine, register event types, create EPL statements, and attach listeners -- all within your Java application. This gives you full control but means you build the surrounding infrastructure (connectors, deployment, monitoring) yourself.

**Varpulis** runs as a standalone process. You write a `.vpl` file, and the engine handles parsing, compilation, connector management (Kafka, MQTT, NATS, PostgreSQL CDC), and execution. It includes a REST API, web UI, and optional cluster mode. The tradeoff is less flexibility for deeply embedded use cases.

## When to Use Esper

Esper is the better choice when you need:

- **Embedded CEP in a Java application**: Esper is designed for this. If you already have a Java service that needs inline pattern detection without external dependencies, Esper's library model is ideal.
- **Mature EPL ecosystem**: If your team is already proficient in EPL and has existing statements, migrating would have significant cost.
- **Advanced window types**: Esper's expression windows, snapshot output, and named windows offer flexibility beyond what most CEP engines provide.
- **Custom Java aggregation**: If you need to write custom aggregation functions in Java, Esper's plugin model supports this natively.
- **On-demand queries against named windows**: Esper allows querying accumulated state from named windows at any time.

## When to Use Varpulis

Varpulis is the better choice when you need:

- **Low memory footprint**: 10--50 MB vs 200 MB+. Critical for edge computing, IoT gateways, or high-density container deployments.
- **Standalone CEP with built-in connectors**: No Java application wrapper needed. Built-in Kafka, MQTT, NATS, and PostgreSQL CDC connectors.
- **Exhaustive pattern matching**: SASE+ Kleene+ finds all valid match instances -- not just the longest -- which matters for security monitoring, fraud detection, and compliance.
- **Pattern forecasting**: Predict pattern completions before they happen. No equivalent exists in Esper.
- **Multi-query optimization**: Hamlet engine shares computation across overlapping patterns, achieving 100x speedup for 50 concurrent queries vs naive execution.
- **Operational simplicity**: Single binary, sub-100ms startup, no JVM tuning. Suitable for serverless, auto-scaling, or resource-constrained environments.
- **Non-Java teams**: VPL is a standalone language. No Java expertise required.

## Summary

Esper is a mature, well-proven CEP engine with a rich EPL language, ideal for embedding in Java applications. Varpulis is a modern, resource-efficient alternative with a purpose-built pattern language, exhaustive SASE+ matching, and unique capabilities like pattern forecasting and Hamlet trend aggregation. If you need embedded Java CEP with maximum EPL flexibility, Esper is strong. If you need a standalone engine with lower resource usage, built-in connectors, and advanced pattern analytics, Varpulis is a compelling alternative.
