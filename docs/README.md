# Varpulis Documentation

> **Varpulis** - Next-generation streaming analytics engine
>
> *Named after the Slavic wind spirit, companion of thunder*

## Quick Start

```bash
# Build
cargo build --release

# Run a VPL program with MQTT
varpulis run my_patterns.vpl
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

---

## Documentation Structure

### 📊 Project Status
- [`development/STATUS.md`](development/STATUS.md) - **Current project status**
- [`development/KANBAN.md`](development/KANBAN.md) - Task tracking
- [`development/AUDIT_REPORT.md`](development/AUDIT_REPORT.md) - Security audit

### 📋 Specifications
- [`spec/overview.md`](spec/overview.md) - Project overview and vision
- [`spec/roadmap.md`](spec/roadmap.md) - Roadmap and development phases
- [`spec/benchmarks.md`](spec/benchmarks.md) - Performance objectives
- [`spec/glossary.md`](spec/glossary.md) - Glossary of terms

### 📝 VarpulisQL Language
- [`language/overview.md`](language/overview.md) - Language philosophy and design
- [`language/syntax.md`](language/syntax.md) - Complete syntax reference
- [`language/connectors.md`](language/connectors.md) - Connectors (MQTT, HTTP)
- [`language/builtins.md`](language/builtins.md) - Built-in functions
- [`language/types.md`](language/types.md) - Type system
- [`language/operators.md`](language/operators.md) - Operators
- [`language/grammar.md`](language/grammar.md) - Formal grammar (Pest PEG)

### 🏗️ Architecture
- [`architecture/system.md`](architecture/system.md) - System architecture
- [`architecture/attention-engine.md`](architecture/attention-engine.md) - Attention mechanism
- [`architecture/state-management.md`](architecture/state-management.md) - State management
- [`architecture/parallelism.md`](architecture/parallelism.md) - Parallelization
- [`architecture/observability.md`](architecture/observability.md) - Metrics and traces

### 📚 Examples & Demos
- [`examples/hvac-building.md`](examples/hvac-building.md) - HVAC monitoring
- [`examples/financial-markets.md`](examples/financial-markets.md) - Financial analytics
- [`../demos/README.md`](../demos/README.md) - **Interactive demos**

---

## Connector Status

| Connector | Input | Output | Status |
|-----------|-------|--------|--------|
| **MQTT** | Yes | Yes | Production |
| **HTTP** | No | Yes | Output only |
| **Kafka** | No | No | Not implemented |

See [`language/connectors.md`](language/connectors.md) for details.

---

**Version**: 0.1
**Parser**: Pest PEG
**License**: MIT
