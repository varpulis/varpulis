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

## Phase 3: Parallelism, Persistence & Pattern Detection ✅ COMPLETED

- [x] Context-based multi-threaded execution (named contexts, CPU affinity, cross-context channels)
- [x] State persistence infrastructure (RocksDB, FileStore, MemoryStore backends)
- [x] Tenant/pipeline state recovery on restart
- [x] Multi-tenant SaaS API (REST, usage metering, quotas)
- [x] Engine checkpoint integration (window/pattern state save/restore)
- [ ] Declarative parallelization (`.concurrent()`)
- [x] Automatic supervision and restart (circuit breaker, DLQ, graceful shutdown)

### Phase 3 Deliverables
- Complex pattern detection via SASE+
- Horizontal scaling on multi-cores
- Engine state durability across restarts

## Phase 4: Observability & Production ✅ COMPLETED

- [x] OpenTelemetry tracing (`otel` feature flag)
- [x] Engine checkpointing to external storage (S3, local)
- [x] Web UI for monitoring (Vue 3 + Vuetify 3)
- [x] Complete documentation (52 docs, OpenAPI spec, capacity planning guide)

### Phase 4 Deliverables
- Production ready (10/10 audit score)
- Monitoring dashboard (Grafana + Prometheus)

## Phase 5: Advanced Features ✅ COMPLETED

- [x] Distributed mode (Raft consensus, NATS transport, coordinator/worker)
- [x] Hot reload of configurations (`POST /api/v1/pipelines/:id/reload`)
- [x] Additional connectors (NATS, Kinesis, S3, Elasticsearch)
- [x] PST-based pattern forecasting (`.forecast()` operator)
- [x] ONNX model inference (`.score()` operator)
- [x] MCP server for AI-assisted development

## Phase 6: Future

- [ ] Declarative parallelization (`.concurrent()`)
- [ ] Pulsar / Redis Streams connectors
- [ ] GPU-accelerated inference
- [ ] Multi-region federation

## Estimated Timeline

```
2026 Q1: Phase 1 (Core) ✅ + Phase 2 (Connectors) ✅ + Phase 3 ✅ + Phase 4 ✅ + Phase 5 ✅
2026 Q2+: Phase 6 (Future)
```
