# Varpulis Project Status

**Last updated**: February 17, 2026
**Version**: 0.3.0
**Production readiness**: 10/10 (see [AUDIT_REPORT.md](AUDIT_REPORT.md))

---

## Overview

Varpulis is a high-performance Complex Event Processing (CEP) engine written in Rust.

| Metric | Value |
|--------|-------|
| Rust source code | 86,789 lines |
| Crates | 8 |
| Test functions | 3,776 |
| Integration test files | 62 |
| Benchmark suites | 7 |
| CI jobs | 13 |
| Documentation files | 52 |
| API endpoints | 40+ |
| Production readiness | 10/10 |

---

## What Works

### Core Engine
- VPL parser (Pest PEG) with error recovery
- Stream processing: `.where()`, `.window()`, `.partition_by()`, `.emit()`, `merge()`
- Aggregations: sum, avg, count, min, max, stddev, first, last, count_distinct, ema (SIMD-optimized)
- SASE+ pattern matching: sequences (`->`), temporal (`.within()`), Kleene (`+`, `*`), negation (`AND NOT`)
- Hamlet trend aggregation (3x-100x faster than ZDD, integrated via `.trend_aggregate()`)
- PST-based pattern forecasting (51 ns prediction, integrated via `.forecast()`)
- Imperative blocks, enrichment joins, session windows

### Connectors
- **MQTT**: Full I/O, QoS 0/1/2, managed lifecycle, exponential backoff
- **Kafka**: Transactional producer (exactly-once), feature-gated
- **HTTP**: Webhooks, REST API sink
- **Database**: PostgreSQL/MySQL via sqlx, connection pooling
- **Redis**: Stub, feature-gated
- **S3/Kinesis**: Stub, feature-gated

### Distributed Architecture
- Coordinator/Worker model with Raft consensus (openraft 0.9)
- RocksDB persistence for Raft log and state machine
- K8s Lease-based leader election (HA)
- Pipeline group management, worker drain, live migration
- State replication (full snapshot + delta)

### Multi-Tenant SaaS
- REST API: deploy, list, delete, inject, batch, metrics, reload, checkpoint, restore, logs (SSE)
- Admin API: tenant CRUD, usage metering
- RBAC: Admin/Operator/Viewer roles with multi-key file support
- Quotas: Free (2 pipelines, 100 eps), Pro (20/50K), Enterprise (1000/500K)
- API key auth with constant-time comparison, secret zeroization

### Security
- Path traversal prevention, filename sanitization
- Rate limiting (token bucket per-IP, configurable burst, bounded tracking)
- Body size limits (1 MB JSON, 16 MB batch/models)
- Event resource limits (1024 fields, 256 KB strings, depth 32)
- cargo-deny + cargo-audit in CI

### Resilience
- Circuit breaker (Open/HalfOpen/Closed)
- Dead letter queue for failed events
- Graceful shutdown (SIGTERM/SIGINT)
- Checkpoint/restore with tested kill-restart scenarios
- Exponential backoff on connector failures

### Observability
- Structured logging (tracing crate)
- Prometheus metrics endpoint
- OpenTelemetry distributed tracing
- Health/readiness probes
- Pre-configured Grafana dashboards

### Tooling
- CLI: run, simulate, check, server, deploy, pipelines, undeploy, status, cluster
- LSP server: diagnostics, hover, completion, semantic tokens
- MCP server: AI-assisted pipeline development
- VS Code extension + tree-sitter grammar
- Web UI: Vue 3 + Vuetify 3 with Monaco editor, VPL validation

### Deployment
- Multi-platform release (Linux x86_64/ARM64, macOS x86_64/ARM64, Windows)
- Docker image (non-root, health check, multi-stage)
- Docker Compose stacks (single-node, SaaS, cluster, demo)
- K8s manifests (StatefulSet, HPA, PDB, ServiceMonitor, RBAC, Kustomize)
- Helm chart support

### Testing
- 3,776 test functions across 62 integration test files
- Real chaos testing (process spawning, Raft failover, state recovery)
- E2E browser tests (Playwright)
- Docker-based Raft HA and scaling tests
- PST convergence validation (mathematical correctness)
- 7 Criterion benchmark suites
- 13-job CI pipeline (check, test, fmt, clippy, deny, audit, feature-flags, chaos, web-ui, coverage)

---

## Completed Roadmap to 10/10

All 18 tasks from the production readiness audit are complete. See [AUDIT_REPORT.md](AUDIT_REPORT.md) for details.

### P1 Critical (4/4)
- Fuzzing infrastructure (parser, connectors)
- OpenAPI specification (40+ endpoints)
- API pagination (all list endpoints)
- Coverage threshold enforcement (70% min)

### P2 Important (7/7)
- SQL table name sanitization
- CONTRIBUTING.md
- SECURITY.md (responsible disclosure)
- Prometheus alerting rules (8 alert groups)
- Operational runbook
- Checkpoint schema versioning
- Property-based testing (proptest)

### P3 Polish (7/7)
- Chaos test quarantine system
- API changelog with deprecation policy
- Architecture Decision Records (5 ADRs)
- MCP documentation (tools, resources, prompts)
- Performance regression CI (10% threshold)
- Binary serialization option (MessagePack)
- SLO/SLI definitions (9 SLOs with PromQL)

---

## Known Limitations
- LSP: go-to-definition and find-references not yet implemented
- CORS: wildcard origin (expects nginx to restrict in production)
- Event ordering: watermark-based only (not strict global ordering)
- Worker state: requires explicit checkpoint (no automatic WAL)

---

## See Also

- [AUDIT_REPORT.md](AUDIT_REPORT.md) — Comprehensive security and quality audit
- [Roadmap](../spec/roadmap.md) — Feature roadmap
