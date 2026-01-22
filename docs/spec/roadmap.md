# Roadmap de développement

## Phase 1 : Core Engine (3-4 mois)

- [ ] Parser VarpulisQL (LALRPOP)
- [ ] Compilateur VarpulisQL → IR
- [ ] Execution engine basique (single-threaded)
- [ ] Hypertree pattern matcher
- [ ] Agrégations simples (sum, avg, count, min, max)
- [ ] Backends: in-memory state
- [ ] Sources: Kafka, fichiers

### Livrables Phase 1
- CLI `varpulis` fonctionnel
- Démo "hello world" avec agrégation simple
- Documentation de base

## Phase 2 : Attention & Parallélisation (2-3 mois)

- [ ] Embedding engine (rule-based)
- [ ] Attention mechanism déterministe
- [ ] Parallélisation déclarative
- [ ] Supervision et restart automatique
- [ ] State backend: RocksDB

### Livrables Phase 2
- Détection de patterns complexes via attention
- Scaling horizontal sur multi-cores

## Phase 3 : Observabilité & Production (2 mois)

- [ ] Métriques Prometheus
- [ ] Tracing OpenTelemetry
- [ ] Checkpointing (S3, local)
- [ ] CLI avancé et tooling
- [ ] Documentation complète

### Livrables Phase 3
- Prêt pour production
- Dashboard de monitoring

## Phase 4 : Fonctionnalités avancées (3-4 mois)

- [ ] Learned embeddings (modèles pré-entraînés)
- [ ] GPU support pour attention (optionnel)
- [ ] Distributed mode (multi-nodes)
- [ ] Web UI pour monitoring
- [ ] Hot reload de configurations
- [ ] Connecteurs additionnels (Pulsar, Redis Streams, etc.)

## Timeline estimée

```
2026 Q1: Phase 1 (Core)
2026 Q2: Phase 2 (Attention)
2026 Q3: Phase 3 (Production)
2026 Q4: Phase 4 (Avancé)
```
