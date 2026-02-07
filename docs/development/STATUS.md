# Varpulis Project Status

**Last updated**: February 4, 2026
**Version**: 0.1.0

---

## Overview

Varpulis is a high-performance Complex Event Processing (CEP) engine written in Rust.

| Metric | Value |
|--------|-------|
| Codebase | ~103,000 lines Rust |
| Test coverage | ~63% |
| Tests passing | 1068+ |
| Production readiness | Prototype |

---

## What Works

### Core Engine
- VarpulisQL parser (Pest PEG)
- Stream processing with filtering (`.where()`)
- Tumbling and sliding windows (`.window()`)
- Partitioning (`.partition_by()`)
- Event emission (`.emit()`)
- Stream merging (`merge()`)

### Aggregations (95% coverage)
- `sum()`, `avg()`, `count()`, `min()`, `max()`
- `stddev()`, `first()`, `last()`
- `count_distinct()`, `ema()`
- SIMD-optimized aggregation (4x speedup on x86_64)

### Pattern Matching (SASE+)
- Sequence patterns (`->` operator)
- Temporal constraints (`.within()`)
- Kleene closures (`+`, `*`)
- Negation (`AND NOT`)
- Multi-step patterns with event correlation

### Connectors
- **MQTT**: Full input/output support via `.from()`/`.to()` (production)
- **HTTP**: Output only (webhooks via `HttpSink`)
- **Kafka**: Framework + full impl behind `kafka` feature flag
- **Console**: Debug output
- Connector declaration syntax: `connector Name = type (params)`
- Source binding: `.from(Connector, topic: "...")`
- Sink routing: `.to(Connector)`

### Parallelism
- Context-based multi-threading (named contexts, CPU affinity, cross-context channels)
- Worker pool model for partition-based parallelism

### Persistence
- `StateStore` trait with 3 backends: `MemoryStore`, `FileStore`, `RocksDbStore`
- `CheckpointManager` with configurable intervals and retention
- Tenant/pipeline state recovery on restart (via `--state-dir`)
- RocksDB available behind `persistence` feature flag

### Multi-Tenant SaaS
- REST API for pipeline management (deploy, list, delete, inject events, hot reload)
- Usage metering and quota enforcement (Free/Pro/Enterprise tiers)
- Admin API for tenant management
- API key authentication with constant-time comparison
- Rate limiting (token bucket per IP)

### Tooling
- CLI with run, simulate, check, server, deploy, pipelines, undeploy, status commands
- VS Code extension with LSP server (diagnostics, hover, completion, semantic tokens)
- React Flow visual pipeline editor
- Interactive demos (HVAC, Financial, SASE)

### Infrastructure
- Docker image (multi-stage, 5 platforms)
- Docker Compose SaaS stack (Prometheus + Grafana)
- Kubernetes deployment (Kustomize overlays for dev/prod/k3d)
- Prometheus metrics endpoint (port 9090)
- Pre-configured Grafana dashboards

---

## What Doesn't Work Yet

### Not Implemented
- Automatic periodic checkpoint triggers in the engine event loop
- Distributed mode / clustering (single-node only)
- Declarative parallelization (`.concurrent()`)
- Web UI for monitoring (Grafana dashboards exist, no custom UI)

### Parsed but Not Evaluated
- `.concurrent()` - parallelization
- `.process()` - custom processing
- `.on_error()` - error handling
- `.tap()` - instrumentation
- `.map()`, `.filter()`, `.distinct()`, `.order_by()`, `.limit()` - reserved syntax
- `.fork()`, `.any()`, `.all()`, `.first()`, `.collect()` - reserved syntax

### Engine Checkpointing
- `create_checkpoint()` and `restore_checkpoint()` are implemented and save/restore window state, SASE+ engines, join buffers, variables, and watermark trackers
- Checkpointing is available programmatically (via API calls) but is NOT automatically triggered during the event processing loop
- Tenant/pipeline metadata persists with `--state-dir`

### Known Limitations
- No automatic checkpoint barriers in the event stream pipeline
- Limited to single-node deployment

---

## Test Coverage by Module

| Module | Coverage | Status |
|--------|----------|--------|
| varpulis-core/value.rs | 96% | Excellent |
| varpulis-runtime/aggregation.rs | 96% | Excellent |
| varpulis-runtime/sequence.rs | 94% | Excellent |
| varpulis-runtime/event.rs | 100% | Perfect |
| varpulis-parser/parser.rs | 65% | Good |
| varpulis-runtime/engine.rs | 62% | Needs work |

---

## See Also

- [KANBAN.md](KANBAN.md) - Task tracking
- [AUDIT_REPORT.md](AUDIT_REPORT.md) - Security audit
- [Roadmap](../spec/roadmap.md) - Detailed roadmap
