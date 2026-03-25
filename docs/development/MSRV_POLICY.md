# MSRV Policy and Feature Flag Matrix

## Minimum Supported Rust Version (MSRV)

**Current MSRV: Rust 1.93**

Defined in `Cargo.toml` under `[workspace.package]`:

```toml
rust-version = "1.93"
```

All workspace members inherit this version. CI runs `cargo test --workspace` on the
MSRV toolchain to ensure tests (not just compilation) pass.

### MSRV Update Policy

- MSRV increases are treated as **minor** version bumps (SemVer).
- MSRV is only raised when a dependency update or language feature provides
  meaningful value (performance, safety, ergonomics).
- CI enforces MSRV on every push to `main` and on all pull requests.

## Workspace Crates

| Crate | Description |
|-------|-------------|
| `varpulis-actors` | Actor framework with supervision |
| `varpulis-cli` | Command-line interface and REST API |
| `varpulis-cluster` | Distributed execution (Raft, K8s) |
| `varpulis-connector-api` | Shared connector traits and types |
| `varpulis-connector-mqtt` | MQTT connector |
| `varpulis-connector-kafka` | Apache Kafka connector |
| `varpulis-connector-nats` | NATS connector |
| `varpulis-connector-redis` | Redis connector |
| `varpulis-connector-database` | SQL database connector (PG/MySQL/SQLite) |
| `varpulis-connector-elasticsearch` | Elasticsearch connector |
| `varpulis-connector-pulsar` | Apache Pulsar connector |
| `varpulis-connector-kinesis` | AWS Kinesis connector |
| `varpulis-connector-s3` | AWS S3 connector |
| `varpulis-connector-http` | HTTP webhook connector |
| `varpulis-connector-cdc` | PostgreSQL CDC connector |
| `varpulis-connectors` | Convenience crate (re-exports all connectors) |
| `varpulis-core` | Core types, AST, validation |
| `varpulis-datagen` | Synthetic event generator |
| `varpulis-db` | PostgreSQL persistence layer |
| `varpulis-lsp` | Language Server Protocol |
| `varpulis-mcp` | Model Context Protocol server |
| `varpulis-parser` | VPL language parser |
| `varpulis-runtime` | Execution engine |
| `varpulis-sase` | SASE+ pattern matching |
| `varpulis-wasm` | WebAssembly bindings |

## Feature Flags

Features are **disabled by default**. Enable them via `--features <flag>`.

### Connector Crates

Each connector is an **independent crate** with its own dependencies. They
self-register via the `inventory` crate — linking a connector crate automatically
makes it available to the engine. The `varpulis-connectors` crate re-exports all
connectors via feature flags for backward compatibility.

| Crate | System | Key Dependency | Feature Flag |
|-------|--------|---------------|--------------|
| `varpulis-connector-mqtt` | MQTT broker | `rumqttc-v4-next 0.28` | `mqtt` |
| `varpulis-connector-kafka` | Apache Kafka | `rdkafka 0.39` | `kafka` |
| `varpulis-connector-nats` | NATS messaging | `async-nats 0.46` | `nats` |
| `varpulis-connector-redis` | Redis | `redis 1.x` | `redis` |
| `varpulis-connector-database` | SQL (PG + MySQL + SQLite) | `sqlx 0.8` | `database` |
| `varpulis-connector-elasticsearch` | Elasticsearch | `elasticsearch 9.x` | `elasticsearch` |
| `varpulis-connector-pulsar` | Apache Pulsar | `pulsar 6.x` | `pulsar` |
| `varpulis-connector-s3` | AWS S3 | `aws-sdk-s3` | `s3` |
| `varpulis-connector-kinesis` | AWS Kinesis | `aws-sdk-kinesis` | `kinesis` |
| `varpulis-connector-http` | HTTP webhooks | `axum`, `reqwest` | `http` |
| `varpulis-connector-cdc` | PostgreSQL CDC | `tokio-postgres 0.7` | `cdc` |

All connector crates share `varpulis-connector-api` for the `ConnectorFactory`,
`Sink`, `ManagedConnector`, and related traits.

### Engine Features

| Feature | Description | Key Dependency |
|---------|-------------|---------------|
| `persistence` | RocksDB state persistence | `rocksdb 0.22` |
| `binary-codec` | MessagePack serialization | `rmp-serde 1.3` |
| `encryption` | AES-GCM state encryption | `aes-gcm 0.10` |
| `onnx` | ONNX ML inference | `ort 2.0.0-rc.11` |
| `gpu` | GPU acceleration (implies `onnx`) | `ort` |

### WASM Features

| Feature | Description | Key Dependency |
|---------|-------------|---------------|
| `smartmodule` | SmartModule host runtime (filter/map) | `wasmtime 42` |

### Cluster Features

These are defined in `varpulis-cluster` and exposed through `varpulis-cli`.

| Feature | Description | Key Dependency |
|---------|-------------|---------------|
| `raft` | Raft consensus | `openraft 0.9` |
| `persistent` | Raft + RocksDB (implies `raft`) | `rocksdb 0.22` |
| `k8s` | Kubernetes API | `kube 0.98` |
| `nats-transport` | NATS cluster transport | `async-nats 0.38` |
| `federation` | Multi-cluster (implies `nats-transport`) | - |

### CLI Features

| Feature | Description |
|---------|-------------|
| `saas` | Multi-tenant SaaS mode |
| `oidc` | OIDC/SSO authentication |
| `otel` | OpenTelemetry observability |

## Feature Dependency Graph

```
gpu ──► onnx
persistent ──► raft
federation ──► nats-transport
database ──► postgres + mysql + sqlite
all-connectors ──► mqtt + kafka + nats + database + redis
                   + kinesis + s3 + elasticsearch + pulsar
```

## CI Feature Testing

Every feature flag is tested individually in CI via a matrix strategy:

```yaml
cargo check --workspace --features <feature>
cargo clippy --workspace --all-targets --features <feature> -- -D warnings
```

Features requiring system packages install them automatically. See
`.github/workflows/ci.yml` for the full matrix.
