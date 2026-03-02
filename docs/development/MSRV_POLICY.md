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
| `varpulis-connectors` | Source/sink connector implementations |
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

### Connector Features

These features add I/O connectors for external systems. They are defined in
`varpulis-connectors` and re-exported through `varpulis-runtime` and `varpulis-cli`.

| Feature | System | Key Dependency | System Packages |
|---------|--------|---------------|-----------------|
| `mqtt` | MQTT broker | `rumqttc 0.25` | - |
| `kafka` | Apache Kafka | `rdkafka 0.39` | `libcurl4-openssl-dev` |
| `nats` | NATS messaging | `async-nats 0.38` | - |
| `redis` | Redis | `redis 0.25` | - |
| `database` | SQL (Postgres + MySQL + SQLite) | `sqlx 0.8` | - |
| `postgres` | PostgreSQL only | `sqlx 0.8` | - |
| `mysql` | MySQL only | `sqlx 0.8` | - |
| `sqlite` | SQLite only | `sqlx 0.8` | - |
| `elasticsearch` | Elasticsearch | `elasticsearch 8.15` | `libssl-dev` |
| `pulsar` | Apache Pulsar | `pulsar 6.x` | `protobuf-compiler` |
| `s3` | AWS S3 | `aws-sdk-s3` | - |
| `kinesis` | AWS Kinesis | `aws-sdk-kinesis` | - |
| `cdc` | PostgreSQL CDC | `tokio-postgres 0.7` | - |
| `csv-converter` | CSV codec | `csv 1.x` | - |
| `all-connectors` | All of the above | - | All of the above |

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
