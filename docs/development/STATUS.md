# Varpulis Project Status

**Last updated**: January 28, 2026
**Version**: 0.1.0

---

## Overview

Varpulis is a high-performance Complex Event Processing (CEP) engine written in Rust.

| Metric | Value |
|--------|-------|
| Codebase | ~103,000 lines Rust |
| Test coverage | ~63% |
| Tests passing | 539+ |
| Production readiness | Prototype |

---

## What Works

### Core Engine
- VarpulisQL parser (Pest PEG)
- Stream processing with filtering (`.where()`)
- Tumbling and sliding windows (`.window()`)
- Partitioning (`.partition_by()`)
- Event emission (`.emit()`)

### Aggregations (95% coverage)
- `sum()`, `avg()`, `count()`, `min()`, `max()`
- `stddev()`, `first()`, `last()`
- `count_distinct()`, `ema()`

### Pattern Matching
- Sequence patterns (`->` operator)
- Temporal constraints (`.within()`)
- Multi-step patterns

### Connectors
- MQTT: Full input/output support
- HTTP: Output only (webhooks)
- WebSocket: Server mode for dashboards

### Tooling
- CLI with run, simulate, check, server commands
- VS Code extension with syntax highlighting
- Interactive demos (HVAC, Financial, SASE)

---

## What Doesn't Work Yet

### Not Implemented
- Kafka connector (stub only)
- HTTP input (no ingestion endpoint)
- RocksDB state persistence (in-memory only)
- Clustering (single-node only)
- Many built-in functions (see [builtins.md](../language/builtins.md))

### Parsed but Not Evaluated
- `.concurrent()` - parallelization
- `.process()` - custom processing
- `.on_error()` - error handling

### Known Limitations
- State is lost on restart
- No checkpointing
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
| varpulis-runtime/sink.rs | 14% | Critical |

---

## Roadmap

### Phase 1: Stabilization (Current)
- Fix documentation accuracy
- Improve test coverage to 70%+
- Stabilize MQTT connector

### Phase 2: Production Features
- Implement Kafka connector
- Add RocksDB state backend
- Implement checkpointing

### Phase 3: Scale
- Multi-node clustering
- Horizontal partitioning

---

## See Also

- [KANBAN.md](KANBAN.md) - Task tracking
- [AUDIT_REPORT.md](AUDIT_REPORT.md) - Security audit
- [Roadmap](../spec/roadmap.md) - Detailed roadmap
