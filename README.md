<p align="center">
  <img src="web-ui/logo.png" width="320" alt="Varpulis">
</p>

<p align="center"><strong>A modern Complex Event Processing engine.</strong> Rust performance. Pipeline syntax. SASE+ pattern matching.</p>

[![Tests](https://img.shields.io/badge/tests-3899%20passing-brightgreen)]()
[![Coverage](https://img.shields.io/badge/coverage-%E2%89%A570%25-brightgreen)]()
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange)]()
[![Release](https://img.shields.io/badge/release-v0.4.0-blue)]()
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT)

[Live Demo](https://demo.varpulis-cep.com/) | [Documentation](docs/) | [Quick Start](#quick-start) | [Benchmarks](#performance)

---

## Why Varpulis?

Your events already contain the signal. A login, two fast transfers, a sudden temperature spike — the pattern is there, buried in the firehose. The question is whether you detect it in **milliseconds** or discover it in tomorrow's batch report.

CEP engines solve this, but traditional options are either **proprietary and expensive**, **too heavyweight for pure pattern work**, or **fight you when patterns get temporal**. Varpulis is a different trade: an open-source, Rust-native engine with a **pipeline DSL designed for exactly this problem**.

- **10 lines, not 50** — VPL expresses temporal patterns directly. No boilerplate event monitors, no framework plumbing.
- **Sub-millisecond at scale** — 250K+ evt/s on a single core, 36 MB memory footprint for sequence detection.
- **Patterns that predict** — `.forecast()` tells you a pattern is *about to* complete, not just that it did.
- **Deploy anywhere** — single binary, Docker, Kubernetes. MQTT/Kafka/NATS in, webhooks/databases/S3 out.

**Use cases**: fraud detection, IoT alerting, trading signals, infrastructure monitoring, supply chain tracking, predictive maintenance.

## What Does It Look Like?

```python
stream FraudAlert = Events
    .where(type == "login") as e1
    -> Events.where(type == "transfer") as e2
    -> Events.where(type == "transfer") as e3
    .within(5m)
    .where(e2.amount + e3.amount > 10000)
    .forecast(confidence: 0.8, horizon: 2m)
    .emit(user: e1.user, total: e2.amount + e3.amount, alert: "fraud")
```

Login followed by two transfers exceeding $10K within 5 minutes — with predictive forecasting that fires *before* the pattern completes.

## Quick Start

```bash
# Install pre-built binary (Linux/macOS)
curl -sSf https://raw.githubusercontent.com/varpulis/varpulis/main/scripts/install.sh | sh

# Run a VPL file
varpulis run --file examples/hvac_quickstart.vpl
```

Or build from source:

```bash
git clone https://github.com/varpulis/varpulis.git
cd varpulis
cargo build --release
./target/release/varpulis run --file examples/hvac_quickstart.vpl
```

Or use Docker:

```bash
docker compose -f deploy/docker/docker-compose.saas.yml up -d
# Varpulis API: http://localhost:9000
# Grafana:      http://localhost:3000 (admin/varpulis)
```

Try a starter project:

```bash
cd starters/iot && docker compose up    # HVAC monitoring with MQTT
cd starters/fraud && docker compose up  # Fraud detection with forecasting
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

## Performance

Benchmarked with 100K events on a single machine, median of 3 runs.

### Throughput (CPU-bound, preloaded events)

| Scenario | Throughput | Memory |
|----------|-----------|--------|
| Filter | 234K evt/s | 54 MB |
| Temporal Join | 268K evt/s | 66 MB |
| EMA Crossover | 266K evt/s | 54 MB |
| Sequence (SASE+) | 256K evt/s | 36 MB |
| Kleene (SASE+) | 97K matches/s | 58 MB |

Kleene uses exhaustive SASE+ semantics — finds all valid matches, not just greedy first-match.

### Throughput (MQTT connector, I/O-bound)

| Scenario | Throughput | Memory |
|----------|-----------|--------|
| Filter | 6.1K evt/s | 10 MB |
| Kleene | 6.3K evt/s | 24 MB |
| Sequence | 6.8K evt/s | 10 MB |

### Multi-Query Scaling (Hamlet Algorithm)

| Concurrent Queries | Hamlet | ZDD Baseline | Speedup |
|--------------------|--------|--------------|---------|
| 1 | 6.9M evt/s | 2.4M evt/s | 3x |
| 10 | 2.1M evt/s | 122K evt/s | 17x |
| 50 | 950K evt/s | 9K evt/s | **100x** |

### PST Forecasting

| Operation | Performance |
|-----------|------------|
| PST training (100K sequence) | 4.6M symbols/s |
| Single-symbol prediction | 51 ns |
| Full distribution prediction | 105 ns |
| PMC forecast (1 active run) | 93K evt/s |
| Online learning + pruning | 5.0M updates/s |

```bash
cargo bench -p varpulis-runtime
```

## Features

### Language

- **Pipeline syntax**: `.where()`, `.window()`, `.aggregate()`, `.emit()`, `.to()`
- **SASE+ patterns**: Sequences (`->`), Kleene closures (`+`, `*`), negation (`AND NOT`), conjunction/disjunction
- **Forecasting**: `.forecast()` — PST-based pattern prediction with configurable confidence and horizon
- **Windows**: Tumbling, sliding, session, count-based
- **Aggregations**: sum, avg, count, min, max, stddev, ema, first, last, count_distinct (SIMD-accelerated)
- **Imperative control**: `var`, `if/else`, `while`, `for`, `return`, functions, lambdas
- **Meta-programming**: `for row in 0..4:` generates streams at compile time
- **Trend aggregation**: `.trend_aggregate()` via Hamlet algorithm

### Engine

- **Connectors**: MQTT, Kafka, NATS, PostgreSQL/MySQL/SQLite, Redis, Kinesis, S3, Elasticsearch — via feature flags
- **Context parallelism**: Named execution contexts with OS thread isolation and CPU affinity
- **Cluster mode**: Coordinator/worker architecture with Raft consensus and NATS transport
- **Hot reload**: Update pipelines without restart
- **State persistence**: RocksDB, file-based, or in-memory checkpointing
- **Resilience**: Circuit breaker, dead letter queue, exactly-once Kafka delivery, backpressure signaling

### Operations

- **REST API**: Multi-tenant SaaS mode with rate limiting, RBAC, and usage metering
- **Web UI**: Vue 3 + Vuetify control plane ([live demo](https://demo.varpulis-cep.com/))
- **Monitoring**: Prometheus metrics, OpenTelemetry tracing (`otel` feature), pre-configured Grafana dashboards
- **Backpressure**: HTTP 429 + Retry-After signaling under load
- **VS Code extension**: LSP with diagnostics, hover docs, completion, go-to-definition, find-references
- **MCP server**: AI-assisted pipeline development
- **Docker/K8s**: Dockerfile, docker-compose stacks, Kubernetes manifests, Helm chart

## Connectors

| Connector | Direction | Feature Flag | Status |
|-----------|-----------|-------------|--------|
| MQTT | In/Out | `mqtt` (default) | Production |
| Kafka | In/Out | `kafka` | Production |
| NATS | In/Out | `nats` | Production |
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
├── varpulis-runtime/   # Execution engine, SASE+, Hamlet, PST, connectors
├── varpulis-cli/       # CLI binary + REST API server
├── varpulis-cluster/   # Coordinator/worker cluster management (Raft + NATS)
├── varpulis-lsp/       # Language Server Protocol implementation
├── varpulis-mcp/       # Model Context Protocol server
└── varpulis-zdd/       # Zero-suppressed Decision Diagrams (research)
web-ui/                 # Vue 3 + Vuetify control plane dashboard
deploy/                 # Docker, Kubernetes, Helm, Prometheus, Grafana
```

## Documentation

- [Getting Started](docs/tutorials/getting-started.md)
- [VPL Language Tutorial](docs/tutorials/language-tutorial.md)
- [SASE+ Patterns Guide](docs/guides/sase-patterns.md)
- [Forecasting](docs/architecture/forecasting.md)
- [Connectors](docs/language/connectors.md)
- [CLI Reference](docs/reference/cli-reference.md)
- [Context-Based Parallelism](docs/guides/contexts.md)
- [Cluster Tutorial](docs/tutorials/cluster-tutorial.md)
- [Performance Tuning](docs/guides/performance-tuning.md)
- [Production Deployment](docs/PRODUCTION_DEPLOYMENT.md)
- [Capacity Planning](docs/guides/capacity-planning.md)
- [System Architecture](docs/architecture/system.md)
- [Interactive Demos](demos/README.md)
- [Security Policy](SECURITY.md)

## Testing

```bash
cargo test --workspace          # 3899 tests
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

- SASE: Wu, Diao, Rizvi — [*High-Performance Complex Event Processing over Streams*](https://dl.acm.org/doi/abs/10.1145/1142473.1142520) (SIGMOD 2006)
- SASE+: Diao, Immerman, Gyllstrom — [*SASE+: An Agile Language for Kleene Closure over Event Streams*](https://www.lix.polytechnique.fr/Labo/Yanlei.Diao/publications/07-03.pdf)
- SASE+ implementation: Agrawal, Diao, Gyllstrom, Immerman — [*Efficient Pattern Matching over Event Streams*](https://www.lix.polytechnique.fr/~yanlei.diao/publications/sase-sigmod08-long.pdf) (SIGMOD 2008)
- CEP query complexity: Zhang, Diao, Immerman — [*On Complexity and Optimization of Expensive Queries in CEP*](https://people.cs.umass.edu/~immerman/pub/sigmod2014-cep.pdf) (SIGMOD 2014)
- Hamlet framework: Poppe, Lei, Ma, Rozet, Rundensteiner — [*To Share, or not to Share: Online Event Trend Aggregation Over Bursty Event Streams*](https://arxiv.org/abs/2101.00361) (SIGMOD 2021)
- Ron Bekkerman, Mikhail Bilenko, John Langford — [*Scaling Up Machine Learning*](https://doi.org/10.1017/CBO9781139042918) (Cambridge University Press 2012)
- [Pest](https://pest.rs/) parser generator
- [Tower-LSP](https://github.com/ebkalderon/tower-lsp) for Language Server Protocol
