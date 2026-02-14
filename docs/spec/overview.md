# Varpulis - Overview

## Vision

Varpulis is a next-generation streaming analytics engine combining:
- Apama's intuitive syntax (EPL)
- Hypertree efficiency for pattern detection
- A unified approach that radically simplifies development compared to Apama

## Project Name

**Varpulis** - In Slavic mythology, Varpulis is the spirit/companion of the thunder god, associated with violent winds and storms. This name evokes:
- **Speed**: Fast winds carrying events
- **Power**: Storm force to process millions of events
- **Flow**: Continuous movement of data streams

## Target Language

**Rust** - chosen for:
- Native performance (required for high-frequency CEP)
- Memory safety without garbage collector
- Excellent async ecosystem (`tokio`, `async-std`)
- SIMD support and low-level optimizations
- Easy deployment (static binary)

## Comparison with CEP/Streaming Solutions

### vs Apache Flink

| Feature | Flink | Varpulis |
|---------|-------|----------|
| **Language** | Java/Scala API | Native DSL (VPL) |
| **Deployment** | JVM cluster, complex | Single binary, simple |
| **Latency** | ~10-100ms typical | < 1ms possible |
| **Learning curve** | Steep (DataStream API) | Gentle (Python-like syntax) |
| **Pattern detection** | FlinkCEP library | Native SASE+ |
| **Memory model** | JVM GC pauses | No GC (Rust) |

### vs Kafka Streams

| Feature | Kafka Streams | Varpulis |
|---------|---------------|----------|
| **Transport coupling** | Kafka-only | Any source (Kafka, HTTP, files) |
| **Language** | Java/Kotlin | VPL DSL |
| **Topology** | Builder pattern verbose | Declarative chaining |
| **State stores** | RocksDB only | In-memory or RocksDB |
| **CEP patterns** | Not built-in | Native support |

### vs Apache Spark Structured Streaming

| Feature | Spark | Varpulis |
|---------|-------|----------|
| **Model** | Micro-batch | True streaming |
| **Latency** | 100ms+ typical | < 1ms possible |
| **Footprint** | Heavy (JVM cluster) | Lightweight (single binary) |
| **Use case** | Batch-first | Stream-first |

### vs Apama (Cumulocity)

| Feature | Apama | Varpulis |
|---------|-------|----------|
| **Multi-stream aggregation** | ❌ Not directly possible | ✅ Native |
| **Parallelization** | Complex and risky `spawn` | Declarative and supervised |
| **Listeners vs Streams** | Separate and complex | Unified in a single concept |
| **Debugging** | Difficult | Built-in observability |
| **Pattern detection** | Hypertrees only | SASE+ NFA |
| **License** | Commercial | Open Source |

### vs Esper

| Feature | Esper | Varpulis |
|---------|-------|----------|
| **Language** | EPL (SQL-like) | VPL (Python-like) |
| **Runtime** | JVM | Native (Rust) |
| **Hamlet** | Not built-in | Multi-query trend aggregation |
| **Observability** | Manual | Automatic metrics/traces |

### Summary: When to use Varpulis

| Use Case | Best Choice |
|----------|-------------|
| Ultra-low latency CEP (< 1ms) | **Varpulis** |
| Large-scale batch + stream | Flink or Spark |
| Kafka-centric architecture | Kafka Streams |
| Python-like DSL for CEP | **Varpulis** |
| Native SASE+ pattern detection | **Varpulis** |
| Minimal operational overhead | **Varpulis** |

## Main Components

1. **Compiler** - VPL → Optimized IR compilation
2. **Runtime Engine** - Processing graph execution
3. **Pattern Matcher** - Pattern detection via hypertrees
4. **Hamlet Engine** - Multi-query trend aggregation
5. **State Manager** - State management (in-memory / RocksDB)
6. **Observability Layer** - Metrics, traces, logs

## Feature Status

| Feature | Status | Notes |
|---------|--------|-------|
| Basic stream processing | ✅ Implemented | Filter, aggregate, emit |
| Count-based windows | ✅ Implemented | Tumbling and sliding |
| Time-based windows | ✅ Implemented | Tumbling and sliding |
| Partitioned windows | ✅ Implemented | partition_by() with any window type |
| Join operations | ✅ Implemented | Multi-stream correlation with .on() |
| Sequence patterns | ✅ Implemented | followed-by, within timeout |
| Hamlet trend aggregation | ✅ Implemented | Multi-query trend aggregation |
| User-defined functions | ✅ Implemented | fn keyword (parsed, basic eval) |
| MQTT connector | ✅ Implemented | Input and output |
| HTTP connector | 🚧 Partial | Output only (webhooks) |
| WebSocket | ✅ Implemented | Server mode for dashboard |
| Kafka connector | 🚧 Partial | Available with `kafka` feature flag |
| RocksDB state | 🚧 Partial | Available with `persistence` feature flag, tenant state persists, engine checkpoint integration pending |
| Clustering | 📋 Planned | Single-node only |
| Parallelization ops | 🚧 Partial | Parsed but not evaluated (.concurrent, .process) |

## See Also

- [System Architecture](../architecture/system.md)
- [VPL Language](../language/overview.md)
- [Roadmap](roadmap.md)
