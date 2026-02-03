# Development Roadmap

## Phase 1: Core Engine (3-4 months) ✅ COMPLETED

- [x] VarpulisQL Parser (recursive descent)
- [x] VarpulisQL → AST compilation
- [x] Basic execution engine (single-threaded)
- [x] Simple aggregations (sum, avg, count, min, max)
- [x] Backends: in-memory state
- [x] HVAC building simulator demo

### Phase 1 Deliverables
- Functional `varpulis` CLI
- "Hello world" demo with simple aggregation
- Basic documentation

## Phase 2: Parser→Runtime & Sinks (Current)

- [ ] Connect parser to runtime execution
- [ ] File sink (JSON lines)
- [ ] HTTP webhook sink
- [ ] Kafka sink
- [ ] Prometheus metrics endpoint

### Phase 2 Deliverables
- End-to-end execution of VarpulisQL programs
- Multiple output destinations

## Phase 3: Attention & Pattern Detection (2-3 months)

- [ ] Embedding engine (rule-based)
- [ ] Deterministic attention mechanism
- [ ] HVAC degradation detection demo
- [x] Context-based multi-threaded execution (named contexts, CPU affinity, cross-context channels)
- [ ] Declarative parallelization
- [ ] Automatic supervision and restart
- [ ] State backend: RocksDB

### Phase 3 Deliverables
- Complex pattern detection via attention
- Horizontal scaling on multi-cores

## Phase 4: Observability & Production (2 months)

- [ ] OpenTelemetry tracing
- [ ] Checkpointing (S3, local)
- [ ] Advanced CLI and tooling
- [ ] Complete documentation

### Phase 4 Deliverables
- Production ready
- Monitoring dashboard

## Phase 5: Advanced Features (3-4 months)

- [ ] Learned embeddings (pre-trained models)
- [ ] GPU support for attention (optional)
- [ ] Distributed mode (multi-nodes)
- [ ] Web UI for monitoring
- [ ] Hot reload of configurations
- [ ] Additional connectors (Pulsar, Redis Streams, etc.)

## Estimated Timeline

```
2026 Q1: Phase 1 (Core) ✅ + Phase 2 (Sinks)
2026 Q2: Phase 3 (Attention)
2026 Q3: Phase 4 (Production)
2026 Q4: Phase 5 (Advanced)
```
