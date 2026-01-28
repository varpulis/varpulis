# Varpulis CEP - Complex Event Processing Engine

[![Tests](https://img.shields.io/badge/tests-539%20passing-brightgreen)]()
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange)]()
[![License](https://img.shields.io/badge/license-MIT-blue)]()

**Varpulis** is a high-performance Complex Event Processing (CEP) engine written in Rust. It provides a domain-specific language (VarpulisQL) for defining event streams, patterns, and real-time analytics.

## Features

- **VarpulisQL Language**: Expressive DSL for stream processing
- **SASE+ Pattern Matching**: Advanced pattern matching with Kleene closures
- **Real-time Analytics**: Window aggregations, joins, and transformations
- **Attention Window**: AI-powered anomaly detection
- **Connectors**: MQTT (production), HTTP webhooks, Kafka (planned)
- **VS Code Extension**: Syntax highlighting and language support

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/varpulis/varpulis.git
cd varpulis

# Build
cargo build --release

# Run tests
cargo test --workspace
```

### Running with MQTT

```bash
# Run your program (connects to MQTT broker defined in config)
./target/release/varpulis run --file my_patterns.vpl
```

### Example: Fraud Detection with MQTT

```varpulis
# MQTT Configuration - connect to broker
config mqtt {
    broker: "localhost",
    port: 1883,
    client_id: "fraud-detector",
    input_topic: "transactions/#",
    output_topic: "alerts/fraud"
}

# Define events
event Transaction:
    user_id: str
    amount: float
    merchant: str
    timestamp: timestamp

# Pattern: Multiple high-value transactions within 10 minutes
pattern SuspiciousActivity = SEQ(
    Transaction+ as txs where amount > 1000
) within 10m partition by user_id

# Stream processing with alert (published to MQTT)
stream FraudAlert = Transaction as tx
    -> Transaction where amount > tx.amount * 2 as suspicious
    .emit(
        alert_type: "potential_fraud",
        user_id: tx.user_id,
        initial_amount: tx.amount,
        suspicious_amount: suspicious.amount
    )
```

## Connectors

Varpulis supports multiple connectors for event ingestion and output:

| Connector | Status | Documentation |
|-----------|--------|---------------|
| **MQTT** | Production | [docs/language/connectors.md](docs/language/connectors.md) |
| **HTTP** | Production | Webhooks via `.to("http://...")` |
| **Kafka** | Planned | `.to("kafka://...")` |

### MQTT Configuration

Add a `config mqtt { }` block at the top of your VPL file:

```varpulis
config mqtt {
    broker: "localhost",       # MQTT broker hostname
    port: 1883,                # MQTT broker port
    client_id: "my-app",       # Unique client identifier
    input_topic: "events/#",   # Subscribe pattern (# = wildcard)
    output_topic: "alerts"     # Publish topic for .emit() results
}
```

See [docs/language/connectors.md](docs/language/connectors.md) for complete documentation.

## Architecture

```
varpulis/
├── crates/
│   ├── varpulis-core/      # AST, types, values
│   ├── varpulis-parser/    # Hand-written + Pest parsers
│   ├── varpulis-runtime/   # Execution engine, SASE+
│   └── varpulis-cli/       # Command-line interface
├── vscode-varpulis/        # VS Code extension
├── tree-sitter-varpulis/   # Tree-sitter grammar
└── docs/                   # Documentation
```

## Language Features

### Stream Operations

```varpulis
stream Filtered = Source
    .where(amount > 100)
    .select(id, amount, timestamp)
    .window(1h, sliding: 5m)
    .aggregate(total: sum(amount), count: count())
    .emit(result: total / count)
```

### Sequence Patterns

```varpulis
# A followed by B followed by C
stream Sequence = A as a
    -> B where id == a.id as b
    -> C where id == b.id as c
    .emit(matched: true)
```

### SASE+ Patterns

```varpulis
# Kleene closure: one or more B events
pattern MultiStep = SEQ(Login, Transaction+, Logout) within 1h

# Negation: A followed by C without B
pattern NoCancel = SEQ(Order, Shipment) AND NOT Cancel
```

### Attention Window (AI-powered)

```varpulis
stream Anomalies = Metrics
    .attention_window(
        duration: 1h,
        heads: 4,
        embedding: "rule_based"
    )
    .where(attention_score > 0.8)
    .emit(anomaly: true, score: attention_score)
```

## Performance

Run benchmarks:

```bash
cargo bench -p varpulis-runtime
```

### Benchmark Results

| Pattern | 1K events | 10K events |
|---------|-----------|------------|
| Simple SEQ(A, B) | ~50µs | ~500µs |
| Kleene SEQ(A, B+, C) | ~100µs | ~1ms |
| With predicates | ~80µs | ~800µs |

## VS Code Extension

Install the VS Code extension for:
- Syntax highlighting
- Code snippets
- Language configuration

```bash
cd vscode-varpulis
npm install
npm run compile
# Then install in VS Code: Extensions > Install from VSIX
```

## Documentation

- [Language Syntax](docs/language/syntax.md)
- [Connectors (MQTT, HTTP, Kafka)](docs/language/connectors.md)
- [Language Grammar](docs/language/grammar.md)
- [Built-in Functions](docs/language/builtins.md)
- [Interactive Demos](demos/README.md)
- [Architecture](docs/architecture/)
- [Examples](examples/)

## Testing

```bash
# All tests
cargo test --workspace

# Parser tests
cargo test -p varpulis-parser

# Runtime tests
cargo test -p varpulis-runtime

# SASE+ tests
cargo test -p varpulis-runtime sase
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `cargo test --workspace`
4. Run clippy: `cargo clippy --workspace`
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- SASE+ algorithm: Wu, Diao, Rizvi (SIGMOD 2006)
- Pest parser generator
- Tree-sitter for syntax highlighting
