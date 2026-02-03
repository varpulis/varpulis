# Varpulis CEP - Complex Event Processing Engine

[![Tests](https://img.shields.io/badge/tests-1040%20passing-brightgreen)]()
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange)]()
[![Release](https://img.shields.io/badge/release-v0.1.0-blue)]()
[![License](https://img.shields.io/badge/license-MIT-blue)]()

**Varpulis** is a high-performance Complex Event Processing (CEP) engine written in Rust. It provides a domain-specific language (VarpulisQL) for defining event streams, patterns, and real-time analytics.

## Features

- **VarpulisQL Language**: Expressive DSL for stream processing
- **SASE+ Pattern Matching**: Advanced pattern detection with Kleene closures, negation, AND/OR
- **Real-time Analytics**: Window aggregations, joins, and transformations with SIMD optimization
- **Attention Window**: AI-powered anomaly detection
- **Multi-tenant SaaS**: REST API for pipeline management with usage metering and quotas
- **Connectors**: MQTT (production), HTTP webhooks, Kafka (connector framework)
- **VS Code Extension**: LSP server with diagnostics, hover docs, completion, and React Flow visual editor

## Quick Start

### Installation

```bash
# From GitHub releases (prebuilt binaries for Linux, macOS, Windows)
# https://github.com/varpulis/varpulis/releases/tag/v0.1.0

# Or build from source
git clone https://github.com/varpulis/varpulis.git
cd varpulis
cargo build --release
```

### Docker

```bash
# Run with Docker
docker pull ghcr.io/varpulis/varpulis:latest
docker run -v ./my_rules.vpl:/app/queries/rules.vpl ghcr.io/varpulis/varpulis run --file /app/queries/rules.vpl

# SaaS stack with Prometheus + Grafana
docker compose -f deploy/docker/docker-compose.saas.yml up -d
```

### Running a Program

```bash
# Run a VPL file
varpulis run --file my_patterns.vpl

# Check syntax
varpulis check my_patterns.vpl

# Start the API server
varpulis server --port 9000 --api-key "my-secret" --metrics
```

### Example: HVAC Monitoring

```varpulis
# Define a connector
connector MqttSensors = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "hvac-monitor"
)

# Define events
event TemperatureReading:
    sensor_id: str
    zone: str
    value: float
    timestamp: timestamp

# Stream with filtering and alerting
stream HighTempAlert = TemperatureReading
    .from(MqttSensors, topic: "sensors/temperature/#")
    .where(value > 28)
    .emit(
        alert_type: "HIGH_TEMPERATURE",
        zone: zone,
        temperature: value
    )

# SASE+ pattern: rapid temperature swing
pattern RapidTempSwing = SEQ(
    TemperatureReading as t1,
    TemperatureReading+ as readings where value > t1.value + 5,
    TemperatureReading as t3 where value < t1.value
) within 10m partition by zone

# Output
sink HighTempAlert to console()
```

## Examples

| Example | Description | Complexity |
|---------|-------------|------------|
| [hvac_quickstart.vpl](examples/hvac_quickstart.vpl) | HVAC monitoring basics | Beginner |
| [hvac_demo.vpl](examples/hvac_demo.vpl) | Full HVAC with attention-based detection | Advanced |

```bash
varpulis run --file examples/hvac_quickstart.vpl
```

## REST API (SaaS Mode)

Varpulis includes a multi-tenant REST API for deploying and managing CEP pipelines programmatically.

```bash
# Start the server
varpulis server --port 9000 --api-key "my-key" --metrics

# Deploy a pipeline
curl -X POST http://localhost:9000/api/v1/pipelines \
  -H "X-API-Key: my-key" \
  -H "Content-Type: application/json" \
  -d '{"name": "temp-monitor", "source": "stream Alerts from SensorReading\n  where temperature > 100\n  emit alert(\"High\", \"temp\")"}'

# List pipelines
curl http://localhost:9000/api/v1/pipelines -H "X-API-Key: my-key"

# Inject events
curl -X POST http://localhost:9000/api/v1/pipelines/<id>/events \
  -H "X-API-Key: my-key" \
  -H "Content-Type: application/json" \
  -d '{"event_type": "SensorReading", "fields": {"temperature": 105}}'

# Check usage
curl http://localhost:9000/api/v1/usage -H "X-API-Key: my-key"
```

**Endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/pipelines` | Deploy a pipeline |
| `GET` | `/api/v1/pipelines` | List pipelines |
| `GET` | `/api/v1/pipelines/:id` | Get pipeline details |
| `DELETE` | `/api/v1/pipelines/:id` | Delete a pipeline |
| `POST` | `/api/v1/pipelines/:id/events` | Inject events |
| `GET` | `/api/v1/pipelines/:id/metrics` | Pipeline metrics |
| `POST` | `/api/v1/pipelines/:id/reload` | Hot reload pipeline |
| `GET` | `/api/v1/usage` | Tenant usage stats |
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Readiness probe |

## Connectors

| Connector | Input | Output | Status |
|-----------|-------|--------|--------|
| **MQTT** | Yes | Yes | Production |
| **HTTP** | No | Yes | Webhooks via `.to("http://...")` |
| **Kafka** | Yes | Yes | Connector framework |

See [docs/language/connectors.md](docs/language/connectors.md) for details.

## Architecture

```
varpulis/
├── crates/
│   ├── varpulis-core/      # AST, types, values
│   ├── varpulis-parser/    # Pest PEG parser
│   ├── varpulis-runtime/   # Execution engine, SASE+, multi-tenant
│   ├── varpulis-cli/       # CLI + REST API server
│   ├── varpulis-lsp/       # Language Server Protocol
│   └── varpulis-zdd/       # Zero-suppressed Decision Diagrams
├── vscode-varpulis/        # VS Code extension + React Flow editor
├── deploy/
│   └── docker/             # Dockerfile, docker-compose, Prometheus, Grafana
├── benchmarks/             # Performance comparisons (Flink, Apama)
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
    .having(total > 1000)
    .emit(result: total / count)
```

### SASE+ Patterns

```varpulis
# Kleene closure: one or more events
pattern MultiStep = SEQ(Login, Transaction+, Logout) within 1h

# Negation: A followed by C without B
pattern NoCancel = SEQ(Order, Shipment) AND NOT Cancel

# AND: events in any order
pattern BothSensors = AND(TemperatureReading, HumidityReading) within 5m
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

### Imperative Programming

```varpulis
fn compute_stats(prices: [float]) -> {str: float}:
    return {
        "avg": avg(prices),
        "min": min(prices),
        "max": max(prices)
    }
```

## Performance

| Pattern | 1K events | 10K events | Throughput |
|---------|-----------|------------|------------|
| Simple SEQ(A, B) | ~50us | ~500us | 320K evt/s |
| Kleene SEQ(A, B+, C) | ~100us | ~1ms | 200K evt/s |
| SIMD aggregations | - | - | 4x speedup |

Run benchmarks:
```bash
cargo bench -p varpulis-runtime
```

## Monitoring

The SaaS stack includes Prometheus and Grafana:

```bash
# Start the full stack
docker compose -f deploy/docker/docker-compose.saas.yml up -d

# Grafana: http://localhost:3000 (admin/varpulis)
# Prometheus: http://localhost:9091
# Varpulis API: http://localhost:9000
```

Pre-configured dashboard panels: Events/sec, Alerts/sec, Processing Latency (p99), Active Streams, Queue Depth.

## VS Code Extension

Full IDE support with:
- **LSP Server**: Real-time diagnostics, hover documentation, auto-completion, semantic highlighting
- **React Flow Editor**: Visual node-based pipeline editor
- **Syntax Highlighting**: TextMate grammar for `.vpl` files

```bash
cd vscode-varpulis
npm install && npm run compile
# Install in VS Code: Extensions > Install from VSIX
```

## Documentation

- [Language Syntax](docs/language/syntax.md)
- [Connectors](docs/language/connectors.md)
- [CLI Reference](docs/reference/cli-reference.md)
- [Production Deployment](docs/PRODUCTION_DEPLOYMENT.md)
- [Architecture](docs/architecture/)
- [Performance Tuning](docs/guides/performance-tuning.md)
- [SASE+ Patterns Guide](docs/guides/sase-patterns.md)
- [Interactive Demos](demos/README.md)

## Testing

```bash
# All tests (1040+)
cargo test --workspace

# Specific crate
cargo test -p varpulis-parser
cargo test -p varpulis-runtime
cargo test -p varpulis-cli

# SASE+ tests only
cargo test -p varpulis-runtime sase

# Benchmarks
cargo bench --bench pattern_benchmark
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
- Tower-LSP for Language Server Protocol
