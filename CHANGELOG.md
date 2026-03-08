# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0] - 2026-03-08

### Highlights

Full **multi-tenant SaaS platform** with hierarchical organizations, per-tenant
isolation (PostgreSQL schemas, Kubernetes namespaces, Kafka topic prefixes), and
an onboarding wizard. The **playground** switches to native `.evt` format for a
better user experience, and the **landing page** is polished for public visitors.

### Added

#### Multi-Tenant SaaS (7-Phase Buildout)
- **Tenant hierarchy** — parent/child organizations with tree-based navigation
- **Per-tenant PostgreSQL schemas** — automatic schema provisioning and RLS isolation
- **Hierarchical RBAC** — parent tenant admins inherit access to child organizations
- **Pipeline inheritance engine** — global pipelines with per-tenant overrides and DB sync
- **Kubernetes namespace provisioning** — per-tenant namespace with resource quotas via Capsule
- **Kafka topic isolation** — per-tenant topic prefix enforcement at runtime
- **UI hierarchy support** — organization tree, pipeline badges, breadcrumbs
- **Onboarding wizard** — guided tenant setup with usage dashboard
- **API key management** — enhanced key generation and lifecycle management
- **Tenant schema middleware** — automatic schema switching per request

#### Playground Improvements
- **Native `.evt` format** — events displayed and edited in Varpulis's native event
  file format instead of JSON, with `@<time> EventType { field: value }` syntax
- **8 built-in examples** — all converted to `.evt` format with correct VPL syntax
- **EventFileParser integration** — backend uses `EventFileParser::parse()` for events

#### Landing Page & Navigation
- **Polished landing page** — own app bar with nav links, feature grid with
  Multi-Tenant SaaS card, footer with product links
- **Full-screen page routing** — landing, login, signup, playground render without
  app chrome (nav drawer, breadcrumbs)
- **Auth redirect** — unauthenticated visitors land on `/landing` instead of login

#### Infrastructure
- **Worker advertise address** — `POD_IP` and `VARPULIS_ADVERTISE_ADDRESS` env vars
  in k3d-saas worker overlay
- **Admin bootstrapping** — `--admin-password` flag for deterministic admin setup
- **Multi-tenancy architecture docs** — SVG diagrams replacing ASCII art

### Fixed

- **Playground IoT anomaly producing 0 matches** — event fields were silently
  dropped due to `#[serde(default)]` instead of `#[serde(flatten)]`; fully resolved
  by switching to `.evt` format
- **VPL examples using `&&` instead of `and`** — fraud-detection and cyber-killchain
  examples now use correct VPL logical operators
- **Parser exponential backtracking** — 10s timeout guard for malicious inputs
- **Parser bracket bomb** — reject inputs with too many unmatched open brackets
- **Pipeline visibility queries** — correct tenant scoping in pipeline list API
- **Redis connector API** — updated for redis crate 1.x breaking changes
- **GRETA Kleene propagation** — correct coefficient computation in multi-query sharing
- **Web UI auth flow** — redirect to login page instead of API key popup
- **k3d-saas admin login** — fix service routing for admin bootstrapping
- **Nightly `rustfmt` import ordering** — stable across CI environments
- **`partition_by` missing field** — added to `SlowTransactionStep` for correct partitioning
- **License audit** — allow 0BSD license for `quoted_printable` dependency
- **SVG rendering** — fix broken diagrams in authentication docs

## [0.5.0] - 2026-03-02

### Highlights

Major architecture improvements: SmartModule WASM runtime, standalone crate extraction,
VPL test DSL, comprehensive datagen tests, Raft simulation, and hardened CI across
all platforms and feature flags. All 19 crates published to crates.io.

### Added

#### SmartModule WASM Runtime
- **SmartModule host runtime** — user-defined WASM processing via `wasmtime`
- Feature-gated: `--features smartmodule`

#### Crate Extraction
- **varpulis-pst** — PST forecasting as standalone crate
- **varpulis-hamlet** — Hamlet trend aggregation as standalone crate
- **varpulis-enrichment** — Event enrichment as standalone crate
- **varpulis-simd** — SIMD acceleration as standalone crate
- **varpulis-dead-letter** — Dead letter queue as standalone crate

#### Testing Infrastructure
- **VPL-driven test DSL** — `.vpl.test` fixture files with auto-discovery
- **33 tests for varpulis-datagen** — comprehensive data generator coverage with serde roundtrip
- **Raft simulation tests** — distributed consensus testing
- **JSON Schema generation** — schema export for configuration validation
- **Cross-platform CI** — Windows and macOS test targets

#### Engine Improvements
- **EngineBuilder** — fluent API for engine construction
- **ConnectorHealth** — health monitoring for connectors
- **Debug impls** — added `Debug` to all public types (`missing_debug_implementations`)
- **Per-crate error hierarchy** — structured error types across all crates
- **Workspace lints** — 17 additional centralized clippy checks
- **cargo-semver-checks** — CI job for API compatibility validation
- **Removed backward-compat shim** — `From<String> for EngineError` removed

#### Architecture Improvements (Phases 1–4)
- **Physical query plans** — wired `PhysicalPlan` into `Engine::load_program`
- **Restructured test layout** — SASE and engine tests moved from `src/` to `tests/`
- **Performance section** — README restructured with per-layer benchmarks

### Fixed

- **Multiply overflow in timing parser** — fuzz-discovered panic when parsing extreme timing values (e.g., `@999999999999999999s`), now returns error via `checked_mul`
- **Proptest f64 range** — constrain test values to ±1e300 to avoid sum/avg overflow near `f64::MAX`
- **Instant subtraction panic** — Windows-specific panic in migration cleanup test
- **Intra-doc link** — broken link for feature-gated smartmodule module
- **Feature-gated CI failures** — fixes across nats, pulsar, cdc, federation, persistent, encryption
- **Clippy warnings** — raft feature-gated code, `ignored_unit_patterns`, `len_zero`
- **Simulate default mode** — `simulate` defaults to fast mode, `.evt` timestamp parsing fixed
- **cargo-deny and kafka** — CI failures in dependency auditing and kafka feature tests
- **GenericArray deprecation** — replaced deprecated `from_slice` with array conversion in persistence
- **Audit issues** — resolved issues #47–#57

### Infrastructure

- **crates.io publish workflow** — all 19 crates in correct topological order with retry logic and idempotent "already exists" handling

## [0.4.1] - 2026-02-27

### Highlights

Phase 3 & 4 of the cloud SaaS buildout: full authentication, database persistence,
billing integration, and distribution infrastructure for Homebrew, GitHub Actions,
and crates.io publishing. First release published to crates.io.

### Added

#### Authentication & Authorization
- **GitHub OAuth login** — browser-based login flow with PKCE
- **JWT session tokens** — stateless auth with configurable expiry
- **Auth middleware** — Warp filter for protected API routes
- **Auth store** — in-memory session management with token refresh

#### PostgreSQL Database Layer
- **User management** — create, lookup, GitHub ID linking
- **Organization support** — multi-tenant org membership with roles
- **API key management** — scoped keys with usage tracking
- **Pipeline storage** — persistent VPL pipeline CRUD
- **Usage metering** — per-org event counts and storage tracking
- **SQL migrations** — versioned schema with sqlx-migrate

#### Stripe Billing Integration
- **Subscription tiers** — Free, Pro, Enterprise with configurable limits
- **Usage-based billing** — metered event processing charges
- **Checkout sessions** — Stripe-hosted payment flow
- **Customer portal** — self-service subscription management
- **Webhook handling** — subscription lifecycle events

#### Playground & Landing Page
- **Ephemeral sessions** — sandboxed VPL execution with timeout
- **Example library** — pre-built pipelines for quick exploration
- **Landing page** — product overview with feature highlights
- **Billing view** — subscription status and usage dashboard
- **Login view** — OAuth flow with redirect handling

#### Event Generator Library (`varpulis-datagen`)
- **Fraud detection schema** — transactions, logins, device fingerprints
- **IoT monitoring schema** — sensor readings, alerts, device status
- **Trading schema** — orders, fills, market data
- **Configurable rates** — events/sec, burst patterns, seasonal variation

#### Docker Demos
- **Fraud detection demo** — end-to-end pipeline with MQTT and generated events
- **IoT monitoring demo** — sensor alerting with threshold patterns

#### WASM Parser
- **`varpulis-wasm` crate** — browser-compatible VPL parser via wasm-bindgen
- **Playground integration** — client-side syntax validation

#### Distribution
- **Homebrew formula** — `brew install varpulis/tap/varpulis` for macOS and Linux
- **GitHub Actions marketplace action** — `varpulis-check` for CI/CD VPL validation
- **crates.io publish workflow** — automated sequential crate publishing

#### Audit Logging
- **Structured audit log** — JSON-lines format with actor, action, target, outcome
- **In-memory recent buffer** — fast access to last 1000 entries
- **REST endpoint** — `GET /api/v1/audit` with filtering by action and actor
- **Auto-enabled** — writes to `data/audit.jsonl`, no configuration needed

#### Percentile Aggregations
- **`median(expr)`** — 50th percentile aggregation function
- **`percentile(expr, q)`** — generic percentile with configurable quantile (0.0–1.0)
- **`p50(expr)` / `p95(expr)` / `p99(expr)`** — convenience aliases for common percentiles
- Sort-based algorithm with linear interpolation for correctness on bounded windows

#### Outer Joins
- **`left_join(...)`** — emit when left source has an event, fill nulls for missing right
- **`right_join(...)`** — emit when right source has an event, fill nulls for missing left
- **`full_join(...)`** — emit for either side, fill nulls for missing sources
- `JoinType` enum with `Inner`, `Left`, `Right`, `Full` variants in AST

#### Encryption at Rest
- **`EncryptedStateStore<S>`** — transparent AES-256-GCM encryption wrapper for any `StateStore`
- Random 96-bit nonce per value, key from hex env var or Argon2id passphrase derivation
- Feature-gated: `--features encryption` (requires `aes-gcm`, `argon2`, `hex`)

#### SSO/OIDC
- **`AuthProvider` trait** — pluggable identity provider abstraction
- **`OidcProvider`** — generic OIDC provider with `.well-known/openid-configuration` discovery
- Supports Okta, Auth0, Azure AD, Keycloak, Google Workspace
- Feature-gated: `--features oidc` (requires `openidconnect` crate)
- `GitHubOAuth` refactored to implement `AuthProvider`

#### PostgreSQL CDC Connector
- **`PostgresCdcSource`** — change data capture via PostgreSQL logical replication
- Converts INSERT/UPDATE/DELETE WAL changes to typed Varpulis events
- Event format: `{table}.{INSERT|UPDATE|DELETE}` with column values as fields
- LSN tracking for replay positioning
- Feature-gated: `--features cdc` (requires `tokio-postgres`)

#### Advanced Connectors
- **Redis connector** — pub/sub source and sink with key prefix support
- **Pulsar connector** — Apache Pulsar source and sink
- **Federation routing** — cross-cluster event routing for geo-distributed deployments

#### SaaS Deployment
- **docker-compose.saas.yml** — complete SaaS stack (PostgreSQL, Caddy, Web UI, Prometheus, Grafana)
- **Caddyfile.saas** — reverse proxy with OAuth/API/WebSocket routing
- **Environment configuration** — `.env.example` with all required variables documented

#### Validation & LSP
- **Strict semantic validation** — connector params, stream references, type checking
- **Per-op diagnostic spans** — precise error locations for each VPL operator
- **Merge/log/print support** — LSP completions and hover for new operators
- **Unknown op error reporting** — actionable diagnostics for typos in operator names

## [0.4.0] - 2026-02-23

### Highlights

Varpulis 0.4.0 completes the production readiness audit (18/18 tasks) and rewrites
the README for clarity. All P0–P3 issues from the audit are resolved.

### Added

- **Dead Letter Queue API** — REST endpoints for DLQ inspection, replay, and purge
- **OpenTelemetry tracing** — distributed trace export via `otel` feature flag
- **Backpressure signaling** — HTTP 429 + Retry-After headers under queue pressure
- **Capacity planning guide** — sizing recommendations for CPU, memory, and storage
- **TLS documentation** — mTLS setup guide for NATS and cluster transport
- **Grafana overview dashboard** — pre-built panels for cluster health and throughput
- **Fuzzing infrastructure** — cargo-fuzz targets for parser and connectors
- **OpenAPI specification** — machine-readable API docs for 40+ endpoints
- **API pagination** — cursor-based pagination on all list endpoints
- **Coverage enforcement** — 70% minimum threshold in CI
- **CONTRIBUTING.md** — contributor guidelines and development setup
- **SECURITY.md** — responsible disclosure policy
- **Prometheus alerting rules** — 8 alert groups for production monitoring
- **Operational runbook** — incident response procedures
- **Checkpoint schema versioning** — forward-compatible state snapshots
- **Property-based testing** — proptest for parser and value types
- **Chaos test quarantine** — flaky test isolation system
- **Architecture Decision Records** — 5 ADRs documenting key design choices
- **Performance regression CI** — 10% threshold gate on benchmarks
- **Binary serialization** — MessagePack option for checkpoint/wire format
- **SLO/SLI definitions** — 9 SLOs with PromQL queries

### Changed

- Comprehensive dead code removal across workspace
- Queue pressure ratio metric for backpressure decisions
- README rewritten: removed adversarial competitor comparisons, standalone performance framing

### Fixed

- Parser backtracking on malformed `within` clauses
- SVG rendering in documentation (bidirectional arrows, split box visibility)
- STATUS.md accuracy (metrics aligned with actual codebase counts)
- SQL table name sanitization for database connector

## [0.3.0] - 2026-02-12

### Highlights

Varpulis 0.3.0 is a major feature release introducing **PST-based pattern forecasting**,
**ONNX model inference**, **NATS transport**, **MCP server for AI-assisted development**,
and extensive **security hardening**. The engine moves from HTTP/WebSocket to NATS for
cluster communication and adds Raft-based high availability.

### Added

#### PST Pattern Forecasting
- **`.forecast()` operator** — predict future pattern completions using Prediction Suffix Trees
- **Pattern Markov Chain** — online-trained variable-order Markov model from SASE NFA structure
- **Built-in variables** — `forecast_probability`, `forecast_time`, `forecast_state`, `forecast_context_depth`
- **Configurable parameters** — confidence threshold, prediction horizon, warmup period, max tree depth
- **Sub-microsecond prediction** — 51 ns single-symbol, 105 ns full distribution

#### ONNX Model Inference
- **`.score()` operator** — run ONNX models inline in VPL pipelines
- **ort runtime integration** — CPU inference with configurable thread count

#### NATS Transport
- **NATS connector** — publish/subscribe event transport (In/Out)
- **NATS cluster transport** — replaces HTTP/WebSocket for coordinator-worker communication
- **JetStream support** — durable subscriptions with at-least-once delivery

#### MCP Server
- **Model Context Protocol** — AI-assisted VPL pipeline development
- **Tools, resources, prompts** — structured API for LLM-driven pipeline authoring

#### HA Cluster Hardening
- **Leader forwarding** — workers forward writes to current Raft leader
- **Stale reconciliation** — automatic state sync on leader change
- **K8s Lease election** — high-availability leader election for Kubernetes deployments

#### Security Hardening
- **mTLS** — mutual TLS for NATS and cluster transport
- **RBAC** — Admin/Operator/Viewer roles with multi-key file support
- **Resource limits** — 1024 fields, 256 KB strings, depth 32 per event
- **Secrets zeroization** — API keys and credentials cleared from memory on drop
- **Rate limiting** — token bucket per-IP with configurable burst and bounded tracking

#### Resilience
- **Circuit breaker** — Open/HalfOpen/Closed state machine for connector failures
- **Dead letter queue** — failed events captured for inspection and replay
- **Exactly-once Kafka delivery** — transactional producer with idempotent writes

#### Additional Features
- **External connector enrichment joins** — enrich events from database/API lookups
- **Hawkes process** — self-exciting point process for burst detection
- **Conformal prediction** — distribution-free prediction intervals
- **LSP go-to-definition and find-references** — code navigation in VS Code
- **Web UI forecast visualization** — real-time forecast probability charts
- **Web UI monitoring dashboard** — cluster health and per-pipeline metrics

### Changed

- SASE+ engine throughput improved 15–40% (run management, match extraction)
- Pipeline allocation reduced 10–25% (fewer intermediate allocations)
- Kafka batch delivery throughput improved 10x+ (batched `FutureProducer`)
- Cluster transport migrated from HTTP/WebSocket to NATS
- Raft consensus upgraded to openraft 0.9

### Fixed

- Forecast op ordering — inserted at correct VPL position instead of end of ops list
- NFA transition mapping — use next state's event type for symbol labels
- Early exit bypass — skip Sequence early exit when Forecast op follows
- Parser backtracking on edge cases found by fuzzing

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
- ZDD-based multi-query optimization (research baseline)

[Unreleased]: https://github.com/varpulis/varpulis/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/varpulis/varpulis/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/varpulis/varpulis/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/varpulis/varpulis/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/varpulis/varpulis/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/varpulis/varpulis/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/varpulis/varpulis/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/varpulis/varpulis/releases/tag/v0.1.0
