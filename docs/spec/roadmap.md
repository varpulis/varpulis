# Development Roadmap

## Phase 1: Core Engine ✅ COMPLETED

- [x] VPL Parser (Pest PEG)
- [x] VPL → AST compilation
- [x] Basic execution engine (single-threaded)
- [x] Simple aggregations (sum, avg, count, min, max)
- [x] Backends: in-memory state
- [x] HVAC building simulator demo

### Phase 1 Deliverables
- Functional `varpulis` CLI
- "Hello world" demo with simple aggregation
- Basic documentation

## Phase 2: Parser→Runtime & Output Connectors ✅ COMPLETED

- [x] Connect parser to runtime execution (`.from()`/`.to()` wired at runtime)
- [x] HTTP webhook output connector (`HttpSink`)
- [x] Kafka connector (framework + full impl behind `kafka` feature flag)
- [x] Prometheus metrics endpoint (port 9090)
- [x] Connector declaration syntax (`connector Name = type (params)`)

### Phase 2 Deliverables
- End-to-end execution of VPL programs
- Multiple output destinations via `.to()`
- HVAC E2E integration test (MQTT → Varpulis → Kafka)

## Phase 3: Parallelism, Persistence & Pattern Detection (Current)

- [x] Context-based multi-threaded execution (named contexts, CPU affinity, cross-context channels)
- [x] State persistence infrastructure (RocksDB, FileStore, MemoryStore backends)
- [x] Tenant/pipeline state recovery on restart
- [x] Multi-tenant SaaS API (REST, usage metering, quotas)
- [ ] Engine checkpoint integration (window/pattern state save/restore)
- [ ] Embedding engine (rule-based)
- [ ] Deterministic attention mechanism
- [ ] HVAC degradation detection demo
- [ ] Declarative parallelization (`.concurrent()`)
- [ ] Automatic supervision and restart

### Phase 3 Deliverables
- Complex pattern detection via attention
- Horizontal scaling on multi-cores
- Engine state durability across restarts

## Phase 4: Observability & Production

- [ ] OpenTelemetry tracing
- [ ] Engine checkpointing to external storage (S3, local)
- [ ] Web UI for monitoring
- [ ] Complete documentation

### Phase 4 Deliverables
- Production ready
- Monitoring dashboard

## Phase 5: Advanced Features

- [ ] Learned embeddings (pre-trained models)
- [ ] GPU support for attention (optional)
- [ ] Distributed mode (multi-nodes)
- [ ] Hot reload of configurations
- [ ] Additional connectors (Pulsar, Redis Streams, etc.)

## Estimated Timeline

```
2026 Q1: Phase 1 (Core) ✅ + Phase 2 (Connectors) ✅ + Phase 3 started
2026 Q2: Phase 3 (Parallelism, Persistence, Patterns)
2026 Q3: Phase 4 (Production)
2026 Q4: Phase 5 (Advanced)
```
