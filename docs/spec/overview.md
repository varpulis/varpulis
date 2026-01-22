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

## Key Differentiators vs Apama

| Feature | Apama | Varpulis |
|---------|-------|----------|
| **Multi-stream aggregation** | ❌ Not directly possible | ✅ Native |
| **Parallelization** | Complex and risky `spawn` | Declarative and supervised |
| **Listeners vs Streams** | Separate and complex | Unified in a single concept |
| **Debugging** | Difficult | Built-in observability |
| **Pattern detection** | Hypertrees only | Hypertrees + Attention |
| **Latency** | < 1ms possible | Configurable (< 1ms to < 100ms) |

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
