<p align="center">
  <img src="web-ui/logo.png" width="320" alt="Varpulis">
</p>

<p align="center"><strong>A modern Complex Event Processing engine.</strong> Rust performance. Pipeline syntax. SASE+ pattern matching.</p>

[![Tests](https://img.shields.io/badge/tests-1134%20passing-brightgreen)]()
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange)]()
[![Release](https://img.shields.io/badge/release-v0.2.0-blue)]()
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT)

[Live Demo](https://demo.varpulis-cep.com/) | [Documentation](docs/) | [Quick Start](#quick-start) | [Benchmarks](#benchmarks)

---

## Why Varpulis?

Existing CEP engines force a tradeoff: Apama's EPL is powerful but verbose and proprietary. Flink CEP is open but bolted onto a general-purpose streaming framework — heavy to operate for pure CEP workloads. Esper's SQL-like EPL fights against temporal pattern expression.

Varpulis takes a different approach: a **pipeline DSL** designed specifically for event patterns, backed by a Rust engine with SASE+ semantics.

**47 lines of Apama EPL:**
```
monitor FraudDetection {
    action onload() {
        on all Event(type="login") as e1 ->
           Event(type="transfer") as e2 ->
           Event(type="transfer") as e3
           within(300.0)
           where e2.amount + e3.amount > 10000.0
        {
            // ... 40 more lines of boilerplate
        }
    }
}
```

**8 lines of VPL:**
```python
stream FraudAlert = Events
    .where(type == "login") as e1
    -> Events.where(type == "transfer") as e2
    -> Events.where(type == "transfer") as e3
    .within(5m)
    .where(e2.amount + e3.amount > 10000)
    .emit(user: e1.user, total: e2.amount + e3.amount, alert: "fraud")
```

## Quick Start

```bash
# Build from source
git clone https://github.com/varpulis/varpulis.git
cd varpulis
cargo build --release

# Run a VPL file
./target/release/varpulis run --file examples/hvac_quickstart.vpl

# Or use Docker
docker compose -f deploy/docker/docker-compose.saas.yml up -d
# Varpulis API: http://localhost:9000
# Grafana:      http://localhost:3000 (admin/varpulis)
```

## Example: HVAC Monitoring

```python
connector Sensors = mqtt(host: "localhost", port: 1883, client_id: "hvac")

event TemperatureReading:
    sensor_id: str
    zone: str
    value: float

stream Readings = TemperatureReading.from(Sensors, topic: "sensors/temp/#")

# Alert on high temperature
stream HighTemp = Readings
    .where(value > 28)
    .emit(alert: "HIGH_TEMPERATURE", zone: zone, temperature: value)

# Per-zone stats over 5-minute windows
stream ZoneStats = Readings
    .partition_by(zone)
    .window(5m)
    .aggregate(zone: last(zone), avg_temp: avg(value), max_temp: max(value))

# SASE+ pattern: rapid temperature swing
stream RapidSwing = Readings as t1
    -> Readings where sensor_id == t1.sensor_id and value > t1.value + 5 as t2
    -> Readings where sensor_id == t1.sensor_id and value < t2.value - 5 as t3
    .within(10m)
    .emit(alert: "RAPID_SWING", zone: t1.zone, peak: t2.value)
```

## Benchmarks

Compared against Apama Community Edition on identical hardware (100K events, median of 3 runs):

### CPU-Bound (CLI, preloaded events)

| Scenario | Varpulis | Apama | Memory (V / A) |
|----------|----------|-------|----------------|
| Filter | 234K evt/s | 199K evt/s | 54 MB / 166 MB (**3x less**) |
| Temporal Join | 268K evt/s | 208K evt/s | 66 MB / 189 MB (**3x less**) |
| EMA Crossover | 266K evt/s | 212K evt/s | 54 MB / 187 MB (**3.5x less**) |
| Sequence | 256K evt/s | 221K evt/s | 36 MB / 185 MB (**5x less**) |
| Kleene (SASE+) | 97K matches/s | 39K matches/s | 58 MB / 190 MB (**3.3x less**) |

Kleene note: Apama reports higher raw throughput but detects only 20K matches vs Varpulis's 99.6K — Apama uses greedy matching while Varpulis implements exhaustive SASE+ semantics, finding **5x more pattern matches**.

### I/O-Bound (MQTT connector)

| Scenario | Varpulis | Apama | Memory (V / A) |
|----------|----------|-------|----------------|
| Filter | 6.1K evt/s | 6.1K evt/s | 10 MB / 85 MB (**8x less**) |
| Kleene | 6.3K evt/s | 5.9K evt/s | 24 MB / 124 MB (**5x less**) |
| Sequence | 6.8K evt/s | 6.0K evt/s | 10 MB / 153 MB (**16x less**) |

### Multi-Query Scaling (Hamlet Algorithm)

| Concurrent Queries | Hamlet | ZDD Baseline | Speedup |
|--------------------|--------|--------------|---------|
| 1 | 6.9M evt/s | 2.4M evt/s | 3x |
| 10 | 2.1M evt/s | 122K evt/s | 17x |
| 50 | 950K evt/s | 9K evt/s | **100x** |

```bash
cargo bench -p varpulis-runtime
```

## Features

### Language

- **Pipeline syntax**: `.where()`, `.window()`, `.aggregate()`, `.emit()`, `.to()`
- **SASE+ patterns**: Sequences (`->`), Kleene closures (`+`, `*`), negation (`AND NOT`), conjunction/disjunction
- **Windows**: Tumbling, sliding, session, count-based
- **Aggregations**: sum, avg, count, min, max, stddev, ema, first, last, count_distinct (SIMD-accelerated)
- **Imperative control**: `var`, `if/else`, `while`, `for`, `return`, functions, lambdas
- **Meta-programming**: `for row in 0..4:` generates streams at compile time
- **Trend aggregation**: `.trend_aggregate()` via Hamlet algorithm

### Engine

- **Connectors**: MQTT (production), Kafka, PostgreSQL/MySQL/SQLite, Redis, Kinesis, S3, Elasticsearch — via feature flags
- **Attention window**: AI-powered anomaly detection with configurable multi-head attention
- **Context parallelism**: Named execution contexts with OS thread isolation and CPU affinity
- **Cluster mode**: Coordinator/worker architecture with pipeline groups and routing
- **Hot reload**: Update pipelines without restart
- **State persistence**: RocksDB, file-based, or in-memory checkpointing

### Operations

- **REST API**: Multi-tenant SaaS mode with rate limiting and usage metering
- **Web UI**: Vue 3 + Vuetify control plane ([live demo](https://demo.varpulis-cep.com/))
- **Monitoring**: Prometheus metrics + pre-configured Grafana dashboards
- **VS Code extension**: LSP with diagnostics, hover docs, completion, and visual pipeline editor
- **Docker/K8s**: Dockerfile, docker-compose stacks, Kubernetes manifests, Helm chart

## Connectors

| Connector | Direction | Feature Flag | Status |
|-----------|-----------|-------------|--------|
| MQTT | In/Out | `mqtt` (default) | Production |
| Kafka | In/Out | `kafka` | Available |
| PostgreSQL/MySQL/SQLite | Out | `database` | Available |
| Redis | Out | `redis` | Available |
| AWS Kinesis | In/Out | `kinesis` | Available |
| AWS S3 | In/Out | `s3` | Available |
| Elasticsearch | Out | `elasticsearch` | Available |
| HTTP Webhooks | Out | default | Production |

```bash
# Build with specific connectors
cargo build --release --features kafka,database

# Build with all connectors
cargo build --release --features all-connectors
```

## REST API

```bash
# Start the server
varpulis server --port 9000 --api-key "my-key" --metrics

# Deploy a pipeline
curl -X POST http://localhost:9000/api/v1/pipelines \
  -H "X-API-Key: my-key" \
  -H "Content-Type: application/json" \
  -d '{"name": "alerts", "source": "stream A = Input\n  .where(temp > 100)\n  .emit(alert: \"hot\")"}'

# Inject events (returns output events)
curl -X POST http://localhost:9000/api/v1/pipelines/<id>/events \
  -H "X-API-Key: my-key" \
  -d '{"event_type": "Input", "fields": {"temp": 105}}'
```

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/pipelines` | Deploy a pipeline |
| `GET` | `/api/v1/pipelines` | List pipelines |
| `GET` | `/api/v1/pipelines/:id` | Get pipeline details |
| `DELETE` | `/api/v1/pipelines/:id` | Delete a pipeline |
| `POST` | `/api/v1/pipelines/:id/events` | Inject events |
| `GET` | `/api/v1/pipelines/:id/metrics` | Pipeline metrics |
| `POST` | `/api/v1/pipelines/:id/reload` | Hot reload |
| `GET` | `/api/v1/usage` | Tenant usage stats |
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Readiness probe |

## Architecture

```
crates/
├── varpulis-core/      # AST, types, values, validation
├── varpulis-parser/    # Pest PEG parser for VPL
├── varpulis-runtime/   # Execution engine, SASE+, Hamlet, connectors
├── varpulis-cli/       # CLI binary + REST API server
├── varpulis-cluster/   # Coordinator/worker cluster management
├── varpulis-lsp/       # Language Server Protocol implementation
└── varpulis-zdd/       # Zero-suppressed Decision Diagrams (research)
web-ui/                 # Vue 3 + Vuetify control plane dashboard
deploy/                 # Docker, Kubernetes, Helm, Prometheus, Grafana
```

## Documentation

- [Getting Started](docs/tutorials/getting-started.md)
- [VPL Language Tutorial](docs/tutorials/language-tutorial.md)
- [SASE+ Patterns Guide](docs/guides/sase-patterns.md)
- [Connectors](docs/language/connectors.md)
- [CLI Reference](docs/reference/cli-reference.md)
- [Context-Based Parallelism](docs/guides/contexts.md)
- [Cluster Tutorial](docs/tutorials/cluster-tutorial.md)
- [Performance Tuning](docs/guides/performance-tuning.md)
- [Production Deployment](docs/PRODUCTION_DEPLOYMENT.md)
- [System Architecture](docs/architecture/system.md)
- [Interactive Demos](demos/README.md)

## Testing

```bash
cargo test --workspace          # 1134 tests
cargo clippy --workspace --all-targets -- -D warnings
cargo bench -p varpulis-runtime # Criterion benchmarks
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `cargo test --workspace`
4. Run clippy: `cargo clippy --workspace --all-targets -- -D warnings`
5. Run fmt: `cargo fmt --all`
6. Submit a pull request

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.

## Acknowledgments

- SASE+ algorithm: Wu, Diao, Rizvi (SIGMOD 2006)
- Hamlet algorithm: Multi-query trend aggregation with graphlet-based sharing
- [Pest](https://pest.rs/) parser generator
- [Tower-LSP](https://github.com/ebkalderon/tower-lsp) for Language Server Protocol
