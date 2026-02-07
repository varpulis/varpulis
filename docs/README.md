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
connector MqttBroker = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "my-app"
)

stream Events = SomeEvent
    .from(MqttBroker, topic: "events/#")

stream Alert = Events
    .where(value > 100)
    .emit(message: "High value detected")
```

---

## Documentation Structure

### Project Status
- [`development/STATUS.md`](development/STATUS.md) - **Current project status**
- [`development/KANBAN.md`](development/KANBAN.md) - Task tracking
- [`development/AUDIT_REPORT.md`](development/AUDIT_REPORT.md) - Security audit
- [`PRODUCTION_DEPLOYMENT.md`](PRODUCTION_DEPLOYMENT.md) - **Production deployment guide**

### Specifications
- [`spec/overview.md`](spec/overview.md) - Project overview and vision
- [`spec/roadmap.md`](spec/roadmap.md) - Roadmap and development phases
- [`spec/benchmarks.md`](spec/benchmarks.md) - Performance objectives
- [`spec/glossary.md`](spec/glossary.md) - Glossary of terms

### VarpulisQL Language
- [`language/overview.md`](language/overview.md) - Language philosophy and design
- [`language/syntax.md`](language/syntax.md) - Complete syntax reference
- [`language/connectors.md`](language/connectors.md) - Connectors (MQTT, HTTP)
- [`language/builtins.md`](language/builtins.md) - Built-in functions
- [`language/types.md`](language/types.md) - Type system
- [`language/operators.md`](language/operators.md) - Operators
- [`language/grammar.md`](language/grammar.md) - Formal grammar (Pest PEG)

### Architecture
- [`architecture/system.md`](architecture/system.md) - System architecture
- [`architecture/cluster.md`](architecture/cluster.md) - **Distributed execution (cluster mode)**
- [`architecture/attention-engine.md`](architecture/attention-engine.md) - Attention mechanism
- [`architecture/state-management.md`](architecture/state-management.md) - State management
- [`architecture/parallelism.md`](architecture/parallelism.md) - Parallelization
- [`architecture/observability.md`](architecture/observability.md) - Metrics and traces

### Examples & Demos
- [`examples/hvac-building.md`](examples/hvac-building.md) - HVAC monitoring
- [`examples/financial-markets.md`](examples/financial-markets.md) - Financial analytics
- [`../demos/README.md`](../demos/README.md) - **Interactive demos**

---

### Reference
- [`reference/cli-reference.md`](reference/cli-reference.md) - CLI commands and options
- [`reference/windows-aggregations.md`](reference/windows-aggregations.md) - Windows and aggregations

### Tutorials
- [`tutorials/getting-started.md`](tutorials/getting-started.md) - Installation and first program
- [`tutorials/language-tutorial.md`](tutorials/language-tutorial.md) - Comprehensive VPL language guide
- [`tutorials/contexts-tutorial.md`](tutorials/contexts-tutorial.md) - Parallel processing with contexts
- [`tutorials/cluster-tutorial.md`](tutorials/cluster-tutorial.md) - **Distributed execution with cluster mode**
- [`tutorials/checkpointing-tutorial.md`](tutorials/checkpointing-tutorial.md) - Checkpointing, persistence, and watermarks

### Guides
- [`guides/contexts.md`](guides/contexts.md) - **Context-based multi-threaded execution**
- [`guides/performance-tuning.md`](guides/performance-tuning.md) - Optimization
- [`guides/configuration.md`](guides/configuration.md) - Configuration options
- [`guides/sase-patterns.md`](guides/sase-patterns.md) - Pattern matching guide

---

## Connector Status

| Connector | Input | Output | Status |
|-----------|-------|--------|--------|
| **MQTT** | Yes | Yes | Production |
| **HTTP** | No | Yes | Webhooks |
| **Kafka** | Yes | Yes | Connector framework |

See [`language/connectors.md`](language/connectors.md) for details.

## SaaS / REST API

Varpulis includes a multi-tenant REST API for pipeline management:

```bash
# Start the API server
varpulis server --port 9000 --api-key "my-key" --metrics

# Deploy a pipeline
curl -X POST http://localhost:9000/api/v1/pipelines \
  -H "X-API-Key: my-key" -H "Content-Type: application/json" \
  -d '{"name": "my-pipeline", "source": "stream X from Y where z > 10"}'
```

Full SaaS stack with monitoring:
```bash
docker compose -f deploy/docker/docker-compose.saas.yml up -d
# Grafana: http://localhost:3000 | Prometheus: http://localhost:9091
```

## Distributed Execution (Cluster Mode)

Varpulis supports distributed execution across multiple worker processes,
coordinated by a central control plane:

```bash
# Start coordinator
varpulis coordinator --port 9100 --api-key admin

# Start workers (each registers with coordinator)
varpulis server --port 9000 --api-key test \
    --coordinator http://localhost:9100 --worker-id worker-0

# Or use Docker Compose
docker compose -f deploy/docker/docker-compose.cluster.yml up -d
```

See [`architecture/cluster.md`](architecture/cluster.md) for full documentation.

---

**Version**: 0.1.0
**Parser**: Pest PEG
**Tests**: 1068+
**License**: MIT
