# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-10

### Highlights

Varpulis 0.2.0 is a major feature release introducing **distributed cluster mode**,
a **full web UI**, the **Hamlet multi-query aggregation engine**, and extensive
runtime performance optimizations. A live public demo is available at
[demo.varpulis-cep.com](https://demo.varpulis-cep.com).

### Added

#### Cluster Mode & Distributed Execution
- **Coordinator + Workers architecture** — deploy pipeline groups across multiple
  workers with automatic placement and health monitoring
- **Pipeline groups** — bundle related pipelines with routing rules for event
  distribution across named pipelines
- **Connector management API** — create, list, and delete managed MQTT and Kafka
  connectors at runtime via REST
- **Event injection API** — inject test events into pipeline groups via
  `POST /api/v1/cluster/pipeline-groups/{id}/inject` with output event capture
- **Worker registration** — workers self-register with the coordinator via
  `--coordinator` and `--advertise-address` flags
- **Health sweeps** — coordinator monitors worker heartbeats and reports status
- **VPL validation endpoint** — `POST /api/v1/cluster/validate` returns parse
  errors and semantic diagnostics with line/column positions

#### Web UI (Vue 3 + Vuetify 3)
- **Pipeline editor** — Monaco-based VPL editor with syntax validation, auto-save,
  and deploy-from-editor workflow
- **Pipeline management** — deploy, teardown, and monitor pipeline groups with
  per-worker placement visibility
- **Connector management** — create/delete MQTT and Kafka connectors with
  topic configuration
- **Event tester** — inject events, view output events, and browse injection
  history with JSON formatting
- **Real-time metrics** — live events/sec, processing latency, and stream counts
  via WebSocket push
- **Grafana integration** — embedded Grafana dashboard at `/grafana/` with
  Prometheus data source

#### Hamlet Multi-Query Aggregation Engine
- **Hamlet algorithm** — shared computation across overlapping Kleene patterns
  with graphlet-based snapshot propagation (3x–100x speedup vs ZDD baseline)
- **Automatic sharing detection** — `setup_hamlet_sharing()` identifies overlapping
  patterns across queries and enables shared processing
- **Trend aggregation operator** — `trend_aggregate` VPL syntax for declaring
  multi-query trend computations
- **PropagationCoefficients** — O(1) Kleene count computation via
  `coeff * snapshot + local_sum`

#### VPL Language Enhancements
- **Semantic validator** — two-pass analysis catches undefined streams, events,
  connectors, and type mismatches at compile time
- **`count_distinct` aggregation** — both `count_distinct(field)` and
  `count(distinct(field))` syntax supported
- **Constant folding** — compile-time evaluation of constant expressions in the AST
- **Loop expansion** — `for` loops in VPL declarations expanded at parse time
- **`emit` statement** — explicit output field selection for stream results
- **`.process()` operation** — user-defined processing logic in stream pipelines
- **Unified stream syntax** — removed `from` keyword, all streams use `=` assignment

#### Connectors
- **Managed MQTT connector** — shared connection per connector with separate
  source/sink event loops and per-worker unique client IDs
- **Managed Kafka connector** — `FutureProducer` with configurable topic routing
- **AWS Kinesis connector** — stream ingestion and output
- **AWS S3 connector** — batch file source/sink
- **Elasticsearch connector** — document indexing sink

#### Multi-Tenant SaaS Infrastructure
- **Tenant isolation** — per-tenant pipeline quotas, rate limiting, and usage tracking
- **State persistence** — tenant and pipeline state survives restarts
- **Context-based execution** — multi-threaded stream isolation with cross-context
  forwarding and session windows
- **Exactly-once checkpointing** — snapshot-based recovery for stateful operators
- **CORS support** — browser-based API clients

#### Deployment
- **Docker Compose stack** — full demo with Caddy, Prometheus, Grafana, MQTT,
  Kafka, Zookeeper, and auto-setup
- **Helm chart** — Kubernetes deployment with coordinator and worker StatefulSets
- **Public demo** — [demo.varpulis-cep.com](https://demo.varpulis-cep.com) on
  Hetzner with Cloudflare TLS

### Changed

#### Performance Optimizations
- **Event data structures** — `Arc<str>` for event types and field keys,
  `Box<str>` for string values, `FxBuildHasher` for all hash maps
- **Value enum** — boxed Array/Map variants reduce enum size; consistent
  Hash/PartialEq for Float
- **Columnar storage** — SIMD-optimized aggregation buffers for batch processing
- **SASE+ engine** — `swap_remove` for O(1) run removal, `mem::take` to eliminate
  cloning, non-blocking context dispatch
- **Sync pipeline** — skip output rename, preload batch size 1000 → 10000,
  zero-clone event draining
- **Event parsing** — `split_fields()` returns `Vec<&str>` (zero-alloc),
  `with_capacity_at()` skips `Utc::now()`
- **HNSW index** — batch parallel inserts for attention-based pattern detection
- **Multi-worker scaling** — round-robin event distribution with join key inference

#### Metrics & Observability
- **Prometheus integration** — per-stream processing counts, latency histograms,
  active stream gauges, and output event counters
- **Single-event instrumentation** — `process()` path (used by MQTT connector)
  now records Prometheus metrics, not just `process_batch()`
- **Grafana dashboard** — pre-configured panels for throughput, latency, and
  stream activity

### Fixed

- MQTT client ID collisions between workers causing infinite reconnection loops
- Monaco editor freezing on New/Open due to double `setValue` calls
- Editor not reloading pipeline on keep-alive reactivation
- Input vs output event categorization in editor stream panel
- Pipeline names showing UUIDs instead of human-readable names in metrics
- Grafana metric name mismatch (`_total` suffix)
- Event injection returning `success: undefined` due to response field mismatch
- `count_distinct` not dispatched when written as `count_distinct(field)` syntax
- Caddy DNS cache going stale after container recreation
- MQTT sink publishing to wrong topic (appending event type)
- FIFO ordering for batch event processing
- Needless borrows flagged by Clippy in SASE engine

### Benchmarks

#### Hamlet vs ZDD Multi-Query Aggregation
| Queries | Hamlet | ZDD Unified | Speedup |
|---------|--------|-------------|---------|
| 1 | 6.9 M/s | 2.4 M/s | 3x |
| 5 | 2.8 M/s | 398 K/s | 7x |
| 10 | 2.1 M/s | 122 K/s | 17x |
| 50 | 0.95 M/s | 9 K/s | 100x |

#### Varpulis vs Apama (CLI, 100K events)
| Scenario | Varpulis | Apama | RAM (V / A) |
|----------|----------|-------|-------------|
| Filter | 234 K/s | 199 K/s | 54 / 166 MB |
| Kleene | 97 K/s | 195 K/s | 58 / 190 MB |
| Sequence | 256 K/s | 221 K/s | 36 / 185 MB |

## [0.1.0] - 2026-02-02

### Added

- Initial release
- VPL (Varpulis Pipeline Language) parser and AST
- GRETA-based CEP runtime with SASE+ pattern matching
- Kleene patterns with `within` and `partition by` clauses
- Windowed aggregation (`count`, `sum`, `avg`, `min`, `max`, `stddev`, `first`,
  `last`, `ema`)
- Sequence detection with `followed_by` operator
- MQTT source/sink connectors
- CLI with `run`, `simulate`, `check` commands
- Attention-based pattern detection with HNSW index
- ZDD-based multi-query optimization (research baseline)

[0.2.0]: https://github.com/varpulis/varpulis/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/varpulis/varpulis/releases/tag/v0.1.0
