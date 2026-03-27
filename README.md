<p align="center">
  <img src="docs/assets/logo.png" width="320" alt="Varpulis">
</p>

<p align="center"><strong>A modern Complex Event Processing engine.</strong> Rust performance. Pipeline syntax. SASE+ pattern matching.</p>

[![Tests](https://img.shields.io/badge/tests-4532%20passing-brightgreen)]()
[![Coverage](https://img.shields.io/badge/coverage-%E2%89%A570%25-brightgreen)]()
[![Rust](https://img.shields.io/badge/rust-1.93%2B-orange)]()
[![Release](https://img.shields.io/badge/release-v0.9.0-blue)]()
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT)

[Live Demo](https://demo.varpulis-cep.com/) | [Documentation](https://www.varpulis-cep.com/docs/) | [Discord](https://discord.gg/nVyctE8vPz) | [Quick Start](#quick-start) | [Benchmarks](#performance)

---

## Why Varpulis?

Your events already contain the signal. A login, two fast transfers, a sudden temperature spike — the pattern is there, buried in the firehose. The question is whether you detect it in **milliseconds** or discover it in tomorrow's batch report.

CEP engines solve this, but traditional options are either **proprietary and expensive**, **too heavyweight for pure pattern work**, or **fight you when patterns get temporal**. Varpulis is a different trade: an open-source, Rust-native engine with a **pipeline DSL designed for exactly this problem**.

- **10 lines, not 50** — VPL expresses temporal patterns directly. No boilerplate event monitors, no framework plumbing.
- **Sub-millisecond at scale** — 1.5M evt/s SASE+ core, 400K+ evt/s full pipeline on a single core.
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

# Start the interactive shell — no files needed
varpulis interactive
```

### Interactive Shell

<p align="center">
  <img src="docs/assets/recordings/interactive-session.gif" alt="Varpulis interactive shell" width="800">
</p>

Type VPL declarations and events directly — like a Python interpreter. Define event types, create streams, inject events, see results instantly. Save your session as a `.vpl` file when done.

```bash
varpulis interactive           # TUI with topology, events, metrics panes
varpulis interactive --no-tui  # Plain text shell (shown above)
varpulis interactive --json    # JSON-line protocol for AI agents
```

### Pipeline Trace

<p align="center">
  <img src="docs/assets/recordings/trace-mode.svg" alt="Pipeline trace mode" width="800">
</p>

See exactly how each event flows — which operators **pass** or **block** — with `--trace`.

Or run from files:

```bash
# Infer event types from sample data
varpulis infer --input data.jsonl

# Simulate with trace (explain mode)
varpulis simulate --trace -p pipeline.vpl -e events.evt -w 1

# Watch mode: auto-reload on file changes
varpulis simulate --watch -p pipeline.vpl -e events.evt
```

More options: [build from source](#from-source) | [Docker](#docker) | [starter projects](#starters)

<details>
<summary><b>From Source</b></summary>

```bash
git clone https://github.com/varpulis/varpulis.git
cd varpulis
cargo build --release
./target/release/varpulis interactive
```
</details>

<details>
<summary><b>Docker</b></summary>

```bash
docker compose -f deploy/docker/docker-compose.saas.yml up -d
# Varpulis API: http://localhost:9000
# Grafana:      http://localhost:3000 (admin/varpulis)
```
</details>

<details>
<summary><b>Starters</b></summary>

```bash
cd starters/iot && docker compose up    # HVAC monitoring with MQTT
cd starters/fraud && docker compose up  # Fraud detection with forecasting
```
</details>

## Example: Multi-Source Correlation

<p align="center">
  <img src="docs/assets/recordings/multi-source-correlation.svg" alt="Multi-source correlation demo" width="800">
</p>

Connect MQTT sensors and Kafka transactions, correlate them in real-time:

```python
# Connectors
connector Sensors = mqtt(host: "broker", port: 1883, client_id: "sensors")
connector Payments = kafka(brokers: "kafka:9092", consumer_group: "varpulis")

# Events from different sources
event SensorReading:
    device_id: str
    temperature: float

event Transaction:
    device_id: str
    amount: float
    status: str

# Stream from MQTT sensors
stream HotDevices = SensorReading
    .from(Sensors, topic: "devices/+/temp")
    .where(temperature > 80)

# Stream from Kafka payments
stream LargePayments = Transaction
    .from(Payments, topic: "payments")
    .where(amount > 5000 and status == "pending")

# Correlate: device overheating AND large payment within 2 minutes
stream SuspiciousActivity = HotDevices as h -> LargePayments as p
    .within(2m)
    .where(h.device_id == p.device_id)
    .alert(webhook: "https://ops.example.com/alerts", message: "Device {h.device_id} overheating + large payment {p.amount}")
    .emit(device: h.device_id, temp: h.temperature, amount: p.amount)
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

All numbers from [Criterion](https://bheisler.github.io/criterion.rs/book/) micro-benchmarks (`cargo bench`) unless noted. Single core, 100K events.

### Core SASE+ Engine

Direct `SaseEngine::process()` — no VPL pipeline, no I/O, no event cloning.

| Pattern | Throughput |
|---------|-----------|
| Sequence (A → B → C) | **1.5M evt/s** |
| Simple sequence (A → B) | **1.4M evt/s** |
| Kleene+ (A → B+ → C) | **1.1M evt/s** |

### Full VPL Pipeline

End-to-end `Engine::process()` — VPL parsing, predicate evaluation, emit, async channel output.

| Scenario | Throughput |
|----------|-----------|
| Filter + emit | **410K evt/s** |
| Windowed aggregation (window 100) | **1.4M evt/s** |

### CLI End-to-End (`simulate --preload`)

Complete binary: JSONL file parsing, event routing, processing, stdout serialization. 100K events on ramdisk, median of 3 runs.

| Scenario | Throughput | RSS |
|----------|-----------|-----|
| Sequence (SASE+) | 256K evt/s | 36 MB |
| Temporal Join | 268K evt/s | 66 MB |
| EMA Crossover | 266K evt/s | 54 MB |
| Filter | 234K evt/s | 54 MB |
| Kleene (SASE+) | 97K matches/s | 58 MB |

Kleene uses exhaustive SASE+ semantics — enumerates all valid combinations, not just greedy first-match.

### MQTT Connector (I/O-bound)

| Scenario | Throughput | RSS |
|----------|-----------|-----|
| Filter | 6.1K evt/s | 10 MB |
| Kleene | 6.3K evt/s | 24 MB |
| Sequence | 6.8K evt/s | 10 MB |

Throughput ceiling is the MQTT broker (~6K msg/s QoS 0, single-message publish).

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

- **Pipeline syntax**: `.where()`, `.window()`, `.aggregate()`, `.emit()`, `.to()`, `.alert()`
- **SASE+ patterns**: Sequences (`->`), Kleene closures (`+`, `*`), negation (`AND NOT`), conjunction/disjunction
- **Forecasting**: `.forecast()` — PST-based pattern prediction with configurable confidence and horizon
- **Alert notifications**: `.alert(webhook: "url", message: "template {field}")` — fire-and-forget webhooks
- **Windows**: Tumbling, sliding, session, count-based
- **Aggregations**: sum, avg, count, min, max, stddev, ema, percentile, median, p50/p95/p99, first, last, count_distinct (SIMD-accelerated)
- **Joins**: Inner, LEFT, RIGHT, FULL outer joins with null-fill semantics
- **Imperative control**: `var`, `if/else`, `while`, `for`, `return`, functions, lambdas
- **Meta-programming**: `for row in 0..4:` generates streams at compile time
- **Trend aggregation**: `.trend_aggregate()` via Hamlet algorithm

### Engine

- **Connectors**: MQTT, Kafka, NATS, PostgreSQL CDC, PostgreSQL/MySQL/SQLite, Redis, Kinesis, S3, Elasticsearch — via feature flags
- **Context parallelism**: Named execution contexts with OS thread isolation and CPU affinity
- **Cluster mode**: Coordinator/worker architecture with Raft consensus and NATS transport
- **Hot reload**: Update pipelines without restart
- **State persistence**: RocksDB, file-based, or in-memory checkpointing with optional AES-256-GCM encryption at rest
- **Resilience**: Circuit breaker, dead letter queue, exactly-once Kafka delivery, backpressure signaling

### Developer Experience

- **Interactive shell**: `varpulis interactive` — type VPL + events like a Python interpreter
- **TUI mode**: `--tui` — split-pane terminal UI with topology, event stream, metrics dashboard
- **Schema inference**: `varpulis infer` — generate event declarations from sample data
- **Pipeline trace**: `--trace` — explain mode showing per-event operator pass/block
- **Watch mode**: `--watch` — auto-reload simulation on file changes
- **Connector discovery**: `varpulis connector list/info` — inspect available connectors
- **REPL**: `varpulis repl` — interactive VPL shell with history

### Operations

- **REST API**: Multi-tenant SaaS mode with rate limiting, RBAC, usage metering, and SSO/OIDC authentication
- **Web UI**: Vue 3 + Vuetify control plane ([live demo](https://demo.varpulis-cep.com/))
- **Monitoring**: Prometheus metrics, OpenTelemetry tracing (`otel` feature), pre-configured Grafana dashboards
- **Backpressure**: HTTP 429 + Retry-After signaling under load
- **VS Code extension**: LSP with diagnostics, hover docs, completion, go-to-definition, find-references
- **MCP server**: AI-assisted pipeline development with interactive session tools
- **Agent integration**: JSON-line protocol (`--json`) for programmatic session control
- **Docker/K8s**: Dockerfile, docker-compose stacks, Kubernetes manifests, Helm chart

## Connectors

Each connector is an **independent crate** — install only what you need, or use the default binary with everything included.

| Connector | Crate | Direction | Status |
|-----------|-------|-----------|--------|
| MQTT | `varpulis-connector-mqtt` | In/Out | Production |
| Kafka | `varpulis-connector-kafka` | In/Out | Production |
| NATS | `varpulis-connector-nats` | In/Out | Production |
| HTTP | `varpulis-connector-http` | In/Out | Production |
| PostgreSQL/MySQL/SQLite | `varpulis-connector-database` | In/Out | Available |
| Redis | `varpulis-connector-redis` | In/Out | Available |
| AWS Kinesis | `varpulis-connector-kinesis` | In/Out | Available |
| AWS S3 | `varpulis-connector-s3` | Out | Available |
| Elasticsearch | `varpulis-connector-elasticsearch` | Out | Available |
| Apache Pulsar | `varpulis-connector-pulsar` | In/Out | Available |
| PostgreSQL CDC | `varpulis-connector-cdc` | In | Available |

```bash
# Default binary includes all connectors
cargo install varpulis

# Or build a custom binary with only what you need:
# Add connector crates to your Cargo.toml dependencies
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
├── varpulis-actors/    # Actor framework with supervision and health observation
├── varpulis-core/      # AST, types, values, validation
├── varpulis-parser/    # Pest PEG parser for VPL
├── varpulis-runtime/   # Execution engine, SASE+, Hamlet, PST, connectors
├── varpulis-cli/       # CLI binary + REST API server (Axum)
├── varpulis-cluster/   # Coordinator/worker cluster management (Raft + NATS)
├── varpulis-lsp/       # Language Server Protocol implementation
├── varpulis-mcp/       # Model Context Protocol server
└── varpulis-zdd/       # Zero-suppressed Decision Diagrams (research)
# Web UI: https://github.com/varpulis/varpulis-web-ui
design/decisions/       # Architecture Decision Records (ADRs)
deploy/                 # Docker, Kubernetes, Helm, Prometheus, Grafana
```

For architecture decisions and rationale, see the [Architecture Decision Records](design/decisions/README.md).

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
- [PostgreSQL CDC Tutorial](docs/tutorials/postgres-cdc-tutorial.md)
- [Outer Joins Tutorial](docs/tutorials/outer-joins-tutorial.md)
- [Encryption at Rest](docs/tutorials/encryption-at-rest-tutorial.md)
- [SSO / OIDC](docs/tutorials/sso-oidc-tutorial.md)
- [Security Policy](SECURITY.md)

## Testing

```bash
cargo test --workspace          # 4532 tests
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
