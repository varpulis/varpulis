# Varpulis Documentation

> **Varpulis** - Next-generation streaming analytics engine
>
> *Named after the Slavic wind spirit, companion of thunder*

## Quick Start

```bash
# Build with MQTT support
cargo build --release --features mqtt

# Run with MQTT connector
varpulis run my_patterns.vpl --mqtt localhost:1883
```

**Minimal VPL file with MQTT:**
```vpl
config mqtt {
    broker: "localhost",
    port: 1883,
    client_id: "my-app",
    input_topic: "events/#",
    output_topic: "alerts"
}

stream Alert = SomeEvent
    .where(value > 100)
    .emit(message: "High value detected")
```

See [`language/syntax.md`](language/syntax.md#connectors) for complete connector documentation.

## Documentation Structure

### 📋 Specifications
- [`spec/overview.md`](spec/overview.md) - Project overview and vision
- [`spec/roadmap.md`](spec/roadmap.md) - Roadmap and development phases
- [`spec/benchmarks.md`](spec/benchmarks.md) - Performance objectives

### 🏗️ Architecture
- [`architecture/system.md`](architecture/system.md) - System architecture overview
- [`architecture/attention-engine.md`](architecture/attention-engine.md) - Deterministic attention engine
- [`architecture/state-management.md`](architecture/state-management.md) - State management and checkpointing
- [`architecture/parallelism.md`](architecture/parallelism.md) - Parallelization and supervision
- [`architecture/observability.md`](architecture/observability.md) - Metrics, traces and logs

### 📝 VarpulisQL Language
- [`language/overview.md`](language/overview.md) - Language philosophy and design
- [`language/syntax.md`](language/syntax.md) - Complete syntax
- [`language/connectors.md`](language/connectors.md) - **Connectors (MQTT, HTTP, Kafka)**
- [`language/types.md`](language/types.md) - Type system
- [`language/keywords.md`](language/keywords.md) - Reserved keywords
- [`language/operators.md`](language/operators.md) - Operators
- [`language/builtins.md`](language/builtins.md) - Built-in functions
- [`language/grammar.md`](language/grammar.md) - Formal grammar (Pest PEG)

### 🔌 Connectors (see [`language/connectors.md`](language/connectors.md))
| Connector | Config | Status |
|-----------|--------|--------|
| **MQTT** | `config mqtt { broker, port, ... }` | Production |
| **HTTP** | `.to("http://...")` | Production |
| **Kafka** | `.to("kafka://...")` | Planned |

### 📚 Examples
- [`examples/hvac-building.md`](examples/hvac-building.md) - HVAC building supervision (IoT/Smart Building)
- [`examples/financial-markets.md`](examples/financial-markets.md) - Financial markets technical analysis (Trading)
- [`../demos/README.md`](../demos/README.md) - **Interactive demos with MQTT**

## Quick Links

- [GitHub Organization](https://github.com/varpulis)
- [Glossary](spec/glossary.md)

---

**Version**: 0.1  
**License**: Apache 2.0 / MIT (TBD)
