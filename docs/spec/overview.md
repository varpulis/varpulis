# Varpulis - Overview

## Vision

Varpulis is a next-generation streaming analytics engine combining:
- Apama's intuitive syntax (EPL)
- Hypertree efficiency for pattern detection
- Transformer architecture capabilities (attention mechanisms) in a **deterministic** way
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
| **Language** | Java/Scala API | Native DSL (VarpulisQL) |
| **Deployment** | JVM cluster, complex | Single binary, simple |
| **Latency** | ~10-100ms typical | < 1ms possible |
| **Learning curve** | Steep (DataStream API) | Gentle (Python-like syntax) |
| **Pattern detection** | FlinkCEP library | Native + Attention mechanism |
| **Memory model** | JVM GC pauses | No GC (Rust) |

### vs Kafka Streams

| Feature | Kafka Streams | Varpulis |
|---------|---------------|----------|
| **Transport coupling** | Kafka-only | Any source (Kafka, HTTP, files) |
| **Language** | Java/Kotlin | VarpulisQL DSL |
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
| **Pattern detection** | Hypertrees only | Hypertrees + Attention |
| **License** | Commercial | Open Source |

### vs Esper

| Feature | Esper | Varpulis |
|---------|-------|----------|
| **Language** | EPL (SQL-like) | VarpulisQL (Python-like) |
| **Runtime** | JVM | Native (Rust) |
| **Attention/ML** | Not built-in | Deterministic attention |
| **Observability** | Manual | Automatic metrics/traces |

### Summary: When to use Varpulis

| Use Case | Best Choice |
|----------|-------------|
| Ultra-low latency CEP (< 1ms) | **Varpulis** |
| Large-scale batch + stream | Flink or Spark |
| Kafka-centric architecture | Kafka Streams |
| Python-like DSL for CEP | **Varpulis** |
| Pattern detection with attention | **Varpulis** |
| Minimal operational overhead | **Varpulis** |

## Main Components

1. **Compiler** - VarpulisQL → Optimized IR compilation
2. **Runtime Engine** - Processing graph execution
3. **Pattern Matcher** - Pattern detection via hypertrees
4. **Attention Engine** - Deterministic correlation between events
5. **State Manager** - State management (in-memory / RocksDB)
6. **Observability Layer** - Metrics, traces, logs

## See Also

- [System Architecture](../architecture/system.md)
- [VarpulisQL Language](../language/overview.md)
- [Roadmap](roadmap.md)
