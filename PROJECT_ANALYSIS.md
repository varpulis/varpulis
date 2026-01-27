# Varpulis CEP Engine - Project Analysis

> Comprehensive technical analysis of the Varpulis Complex Event Processing engine

---

## 1. Project Overview

**Varpulis** is a high-performance **Complex Event Processing (CEP) engine** written in Rust. Named after the Slavic wind spirit, it provides a modern, efficient alternative to traditional CEP systems like Apache Flink, Kafka Streams, or Apama.

### Core Problem it Solves

- Real-time event stream processing with sub-millisecond latency
- Pattern detection in event sequences (fraud detection, anomalies, correlations)
- Streaming analytics with temporal windows and aggregations
- AI-powered anomaly detection using deterministic attention mechanisms
- Declarative rule definition via VarpulisQL domain-specific language

### Target Use Cases

| Domain | Application |
|--------|-------------|
| Finance | Market trading signals, fraud detection |
| Smart Buildings | HVAC monitoring, predictive maintenance |
| Security | Event correlation, threat detection |
| IoT | Sensor data real-time analytics |
| General | Any low-latency stream processing scenario |

---

## 2. Architecture

The project follows a **modular, layered architecture**:

```
┌─────────────────────────────────────────────────────────────┐
│                  Varpulis Runtime Engine                     │
├─────────────────────────────────────────────────────────────┤
│  ┌────────────┐   ┌───────────┐   ┌──────────────┐         │
│  │   Parser   │   │ Validator │   │  Optimizer   │         │
│  │ (Pest PEG) │   │           │   │              │         │
│  └────────────┘   └───────────┘   └──────────────┘         │
│                          ↓                                  │
│                 Execution Graph (DAG)                       │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌───────────┐  ┌────────────┐  ┌──────────┐ │
│  │Ingestion │  │  Pattern  │  │ Attention  │  │  Window  │ │
│  │  Layer   │  │  Matcher  │  │  Engine    │  │  Engine  │ │
│  │          │  │  (SASE+)  │  │  (Attn.)   │  │          │ │
│  └──────────┘  └───────────┘  └────────────┘  └──────────┘ │
│  ┌──────────┐  ┌───────────┐  ┌────────────┐  ┌──────────┐ │
│  │Embedding │  │   State   │  │Aggregation │  │   Sink   │ │
│  │ Engine   │  │  Manager  │  │   Engine   │  │          │ │
│  └──────────┘  └───────────┘  └────────────┘  └──────────┘ │
│  ┌──────────┐  ┌───────────┐  ┌────────────┐               │
│  │ Metrics  │  │ Parallel  │  │ Checkpoint │               │
│  │  Server  │  │  Manager  │  │  Manager   │               │
│  └──────────┘  └───────────┘  └────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Event Sources → Ingestion → Embedding → Pattern Matching → Aggregation → Sink
                                 ↓              ↑
                            Attention      Hypertree
                             Scores       Structures
```

### Crate Structure

```
Cargo.toml (workspace root)
├── crates/varpulis-core/         # Types, AST, span, values
├── crates/varpulis-parser/       # Pest parser, lexer
├── crates/varpulis-runtime/      # Engine, streams, patterns, attention
└── crates/varpulis-cli/          # CLI entry point, server mode
```

---

## 3. Core Technologies

### Languages

| Language | Usage |
|----------|-------|
| **Rust 1.75+** | Main implementation |
| **TypeScript** | VS Code extension, React demos |
| **Python** | MQTT test simulator |

### Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `tokio` | 1.35 | Async runtime with full features |
| `pest` | 2.8.5 | PEG parser generator |
| `serde/serde_json` | - | Serialization/deserialization |
| `chrono` | 0.4 | Time/date handling |
| `rayon` | 1.10 | Data parallelism |
| `hnsw_rs` | 0.3 | HNSW approximate nearest neighbor indexing |
| `warp` | 0.3 | Web server for WebSocket API |
| `prometheus` | 0.13 | Metrics collection |
| `tracing` | 0.1 | Logging and tracing infrastructure |
| `indexmap` | 2.1 | Ordered hash maps |
| `rumqttc` | 0.24 | MQTT client (optional) |

### Build Configuration

- **Edition**: 2021
- **Resolver**: 2
- **Release Profile**: LTO enabled, single codegen unit
- **Benchmarks**: Criterion with HTML reports

---

## 4. Key Features

### 4.1 VarpulisQL Language

A declarative DSL inspired by Python and SQL:

- Python-like syntax with optional indentation
- Stream-first paradigm (everything is a stream)
- Type-safe with compile-time error detection
- Grammar file: `/crates/varpulis-parser/src/varpulis.pest`

### 4.2 SASE+ Pattern Matching

Based on the SIGMOD 2006 Wu, Diao, Rizvi paper:

| Feature | Description |
|---------|-------------|
| NFA-based | Non-deterministic Finite Automaton pattern detection |
| Sequence patterns | `A -> B -> C` (ordered event detection) |
| Kleene closures | `A+` (one or more), `A*` (zero or more) |
| Boolean operators | AND, OR, NOT with temporal constraints |
| Partition-by | Attribute optimization (SASEXT) |

### 4.3 Attention Window (Deterministic)

- Multi-head attention mechanism for event correlation
- Deterministic (reproducible) scoring, unlike LLMs
- HNSW indexing for O(log n) similarity searches
- Rule-based or learned embeddings
- Cache optimization yielding ~8x performance improvement

### 4.4 Stream Operations

```
.where(condition)          # Filtering
.select(fields)            # Projection
.window(tumbling/sliding)  # Temporal windowing
.aggregate(sum, avg, ...)  # Aggregation
.partition_by(key)         # Partitioning
.match(pattern)            # Pattern matching with lambdas
.merge()                   # Multi-stream joins
```

### 4.5 Aggregation Functions

- `sum`, `avg`, `count`, `min`, `max`
- `stddev`, `ema` (exponential moving average)

### 4.6 VS Code Integration

- Syntax highlighting
- Code snippets
- Language configuration
- Tree-sitter grammar for enhanced highlighting
- WebSocket server for live editing/execution

### 4.7 I/O Options

| Type | Direction |
|------|-----------|
| Console | Sink |
| File | Sink |
| HTTP | Sink |
| MQTT | Source/Sink (feature flag) |
| Event file | Simulation source |

---

## 5. Entry Points

### CLI Commands

```bash
# Run a VarpulisQL program
varpulis run --file program.vpl

# Inline code execution
varpulis run --code "stream X from Y"

# Parse and show AST
varpulis parse file.vpl

# Syntax validation
varpulis check file.vpl

# HVAC demo with Prometheus metrics
varpulis demo --duration 60 --metrics

# Event file simulation
varpulis simulate --program prog.vpl --events data.evt

# WebSocket server mode
varpulis server --port 9000 --metrics
```

### Library API (Rust)

```rust
let mut engine = Engine::new(alert_tx);
engine.load(&program)?;
engine.process(event).await?;
```

### WebSocket API

Commands: `LoadFile`, `InjectEvent`, `GetStreams`, `GetMetrics`

---

## 6. Performance

### Benchmarks

| Metric | Value |
|--------|-------|
| Event throughput | 300K-320K events/sec |
| Pattern matching latency | < 2ms for 500 events |
| Attention throughput | ~500 events/sec |

### Optimization History

| Optimization | Speedup |
|--------------|---------|
| Q cache + K precompute | 8x |
| SIMD projections | 3x |
| HNSW indexing | 3.2x |
| **Total** | **~30x** |

---

## 7. Testing

### Test Infrastructure

| Type | Location |
|------|----------|
| Unit tests | Source files (`cfg(test)` modules) |
| Integration tests | `/tests` directory |
| E2E tests | Playwright (TypeScript) |
| MQTT tests | Docker Compose + Python simulator |
| Scenario tests | `.evt` files with event data |

### Test Metrics

| Metric | Value |
|--------|-------|
| Passing tests | 539+ |
| Code coverage | 62.92% |
| Coverage target | 80% |
| Clippy warnings | 0 |

### Key Test Files

- `/crates/varpulis-runtime/tests/attention_tests.rs`
- `/crates/varpulis-runtime/tests/integration_scenarios.rs`
- `/tests/e2e/tests/vpl-roundtrip.spec.ts`
- `/tests/scenarios/` - Event simulation files

---

## 8. Notable Implementation Details

### 8.1 PEST Parser Migration

Recently replaced hand-written recursive descent parser:

- More maintainable and extensible grammar
- Indentation preprocessor for Python-like syntax
- Grammar: `/crates/varpulis-parser/src/varpulis.pest`

### 8.2 SASE+ Algorithm

- NFA state machine with stack for Kleene closures
- Event selection strategies: skip-till-any-match, skip-till-next-match
- Negation handling with temporal windows
- Partition-by optimization for distributed patterns

### 8.3 Attention Engine Optimizations

```
Performance Stack:
├── Q cache + K precompute     (8x)
├── SIMD projections           (3x)
├── HNSW indexing              (3.2x)
├── Batch processing (rayon)
└── Embedding cache (LRU)
```

### 8.4 Event File Format

Custom `.evt` format with advanced features:

- `@Ns` timing directives (nanosecond precision)
- `BATCH` keyword for grouping
- Parser: `/crates/varpulis-runtime/src/event_file.rs`

### 8.5 Sequence Tracking

- Tracks partial sequences across events
- Context passing between steps
- Example: `Login as login -> Transaction where user_id == login.user_id`
- Implementation: `/crates/varpulis-runtime/src/sequence.rs`

### 8.6 Stream Execution

- Lazy evaluation via operator chains
- No central scheduler - reactive to event arrivals
- Tokio async throughout for efficient concurrency

### 8.7 Sink Architecture

- Async trait-based design
- Multiple types: Console, File, HTTP, Multi (composite)
- Support for both events and alerts
- Flush and close lifecycle

---

## 9. File Structure

```
/home/cpo/cep/
├── crates/
│   ├── varpulis-core/src/
│   │   ├── ast.rs              # AST definitions
│   │   ├── types.rs            # Type system
│   │   ├── value.rs            # Runtime values
│   │   └── span.rs             # Source locations
│   ├── varpulis-parser/src/
│   │   ├── varpulis.pest       # Pest grammar (PEG)
│   │   ├── pest_parser.rs      # Parser → AST conversion
│   │   ├── parser.rs           # Old parser (deprecated)
│   │   ├── lexer.rs            # Token definitions
│   │   ├── indent.rs           # Indentation preprocessing
│   │   └── helpers.rs          # Utility functions
│   ├── varpulis-runtime/src/
│   │   ├── engine.rs           # Main execution engine
│   │   ├── sase.rs             # SASE+ pattern matching
│   │   ├── attention.rs        # Attention engine with HNSW
│   │   ├── sequence.rs         # Sequence tracking
│   │   ├── pattern.rs          # Legacy pattern matcher
│   │   ├── stream.rs           # Stream abstraction
│   │   ├── event.rs            # Event type definition
│   │   ├── window.rs           # Tumbling/sliding windows
│   │   ├── aggregation.rs      # Aggregation functions
│   │   ├── connector.rs        # MQTT, HTTP sources/sinks
│   │   ├── sink.rs             # Output sinks
│   │   ├── event_file.rs       # .evt file parsing
│   │   ├── simulator.rs        # Event simulator
│   │   └── metrics.rs          # Prometheus metrics
│   └── varpulis-cli/src/
│       ├── main.rs             # CLI entry point
│       └── lib.rs              # Public API
├── vscode-varpulis/            # VS Code extension
├── tree-sitter-varpulis/       # Tree-sitter grammar
├── demos/varpulis-demos/       # React web interface
├── tests/
│   ├── e2e/                    # Playwright tests
│   ├── scenarios/              # .evt + .vpl test pairs
│   └── mqtt/                   # Docker Compose, Python simulator
├── docs/
│   ├── spec/                   # Overview, roadmap, benchmarks
│   ├── architecture/           # System design docs
│   └── language/               # VarpulisQL reference
├── examples/
│   ├── hvac_demo.vpl
│   ├── financial_markets.vpl
│   └── sase_patterns.vpl
└── Cargo.toml                  # Workspace definition
```

---

## 10. Recent Development Activity

| Commit | Date | Description |
|--------|------|-------------|
| `9c496be` | Jan 26 | E2E tests, event file parser fix for @Ns timing |
| `0e2bb38` | Jan 25 | Graphical flow editor with drag & drop |
| `45255fb` | Jan 20 | Attention optimization (Q cache + K precompute, 8x speedup) |
| `fd95baa` | Jan 20 | HNSW indexing for Attention Engine (3.2x speedup) |
| `90d216c` | Jan 20 | Benchmarks and Attention Engine optimization |

---

## 11. Project Status

### Completed

- [x] VarpulisQL DSL with Pest parser
- [x] Stream operations (filter, select, window, aggregate)
- [x] SASE+ pattern matching with NFA
- [x] Attention engine with HNSW optimization
- [x] Sequence tracking (A -> B -> C patterns)
- [x] Multiple aggregation functions
- [x] MQTT connector infrastructure
- [x] VS Code extension with tree-sitter
- [x] Graphical flow editor (React)
- [x] E2E testing infrastructure
- [x] High-performance benchmarks

### In Progress

- [ ] Test coverage increase (target: 80%)
- [ ] Tree-sitter semantic tokens integration
- [ ] Advanced pattern optimization

---

## 12. Summary

**Varpulis** is a production-ready **Complex Event Processing engine** combining:

| Aspect | Implementation |
|--------|----------------|
| **Performance** | Fast Rust with zero-cost abstractions |
| **Pattern Matching** | SASE+ algorithm with NFA |
| **AI/ML** | Deterministic attention mechanism |
| **Language** | Declarative VarpulisQL DSL |
| **Observability** | Prometheus metrics, structured logging |
| **Tooling** | VS Code extension, React demos, WebSocket API |

The project demonstrates excellent engineering with:

- Modular architecture
- Thorough testing (539+ tests)
- Benchmarking culture
- Attention to performance optimization

**Positioned as a faster, simpler alternative to Flink/Kafka Streams** with unique features like deterministic attention and SASE+ pattern matching.

---

*Analysis generated on 2026-01-26*
