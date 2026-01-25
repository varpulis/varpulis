# Plan d'Amélioration Varpulis
## Roadmap vers Production-Ready

**Créé le**: 25 Janvier 2026  
**Objectif**: Atteindre production-ready en Q3 2026

---

## Phase 1: Fondations (Février 2026)
**Durée estimée**: 4 semaines  
**Priorité**: CRITIQUE

### 1.1 Sinks Fonctionnels
| Tâche | Effort | Fichier |
|-------|--------|---------|
| Implémenter FileSink JSON Lines | 2j | `sink.rs` |
| Implémenter HttpWebhookSink | 3j | `sink.rs` |
| Tests unitaires sinks | 2j | `sink.rs` |
| **Total** | **7j** | |

```rust
// Objectif: sink.rs à 80%+ couverture
stream Alerts = ... .emit_to(file: "/var/log/alerts.jsonl")
stream Metrics = ... .emit_to(http: "https://webhook.site/xxx")
```

### 1.2 Tests CLI
| Tâche | Effort | Fichier |
|-------|--------|---------|
| Tests commande `run` | 1j | `main.rs` |
| Tests commande `simulate` | 1j | `main.rs` |
| Tests commande `server` | 2j | `main.rs` |
| **Total** | **4j** | |

### 1.3 CI/CD
| Tâche | Effort |
|-------|--------|
| GitHub Actions: build + test | 0.5j |
| Couverture automatique (grcov) | 0.5j |
| Linting clippy + rustfmt | 0.5j |
| **Total** | **1.5j** |

### Livrables Phase 1
- [ ] Sinks file et HTTP fonctionnels
- [ ] Couverture CLI ≥ 50%
- [ ] CI/CD opérationnel
- [ ] Couverture globale ≥ 70%

---

## Phase 2: Connecteurs (Mars 2026)
**Durée estimée**: 4 semaines  
**Priorité**: HAUTE

### 2.1 Kafka Integration
| Tâche | Effort | Crate |
|-------|--------|-------|
| KafkaSource avec rdkafka | 3j | `connector.rs` |
| KafkaSink | 2j | `connector.rs` |
| Tests avec testcontainers | 2j | tests/ |
| **Total** | **7j** | |

```toml
# Cargo.toml
rdkafka = { version = "0.36", features = ["tokio"] }
testcontainers = "0.15"
```

### 2.2 Autres Sources
| Tâche | Effort |
|-------|--------|
| HTTP/REST source (warp) | 2j |
| MQTT source (rumqttc) | 2j |
| WebSocket source amélioré | 1j |
| **Total** | **5j** |

### 2.3 Configuration Déclarative
```varpulis
config:
  sources:
    kafka_trades:
      type: kafka
      brokers: ["localhost:9092"]
      topic: trades
      group_id: varpulis-consumer
  
  sinks:
    alerts:
      type: http
      url: "https://api.example.com/alerts"
      headers:
        Authorization: "Bearer ${ALERT_TOKEN}"
```

### Livrables Phase 2
- [ ] Kafka source/sink fonctionnels
- [ ] HTTP source
- [ ] Documentation connecteurs
- [ ] Exemple docker-compose avec Kafka

---

## Phase 3: Robustesse (Avril 2026)
**Durée estimée**: 4 semaines  
**Priorité**: HAUTE

### 3.1 State Management
| Tâche | Effort | Notes |
|-------|--------|-------|
| RocksDB backend | 5j | Crate rocksdb |
| Checkpointing basique | 3j | Sérialisation state |
| Recovery au démarrage | 2j | Reload checkpoints |
| **Total** | **10j** | |

```rust
// Objectif architecture
enum StateBackend {
    InMemory,
    RocksDB { path: PathBuf, options: RocksDbOptions },
}

impl StateBackend {
    fn checkpoint(&self) -> Result<CheckpointId>;
    fn restore(&mut self, id: CheckpointId) -> Result<()>;
}
```

### 3.2 Memory Management
| Tâche | Effort |
|-------|--------|
| Bounded windows (max events) | 2j |
| Eviction policies (LRU, FIFO) | 2j |
| Memory metrics | 1j |
| **Total** | **5j** |

### Livrables Phase 3
- [ ] RocksDB state backend
- [ ] Checkpointing toutes les N secondes
- [ ] Recovery automatique
- [ ] Bounded memory windows

---

## Phase 4: Performance (Mai 2026)
**Durée estimée**: 4 semaines  
**Priorité**: MOYENNE

### 4.1 Benchmarks
| Tâche | Effort |
|-------|--------|
| Framework benchmark (criterion) | 1j |
| Benchmark ingestion | 1j |
| Benchmark patterns | 1j |
| Benchmark attention | 1j |
| CI benchmark regression | 1j |
| **Total** | **5j** |

```rust
// benches/ingestion.rs
#[bench]
fn bench_1m_events(b: &mut Bencher) {
    b.iter(|| engine.process_batch(events_1m))
}
```

### 4.2 Parallélisation
| Tâche | Effort |
|-------|--------|
| Worker pool tokio | 3j |
| Partition parallel processing | 3j |
| Lock-free aggregation | 2j |
| **Total** | **8j** |

```varpulis
stream Orders = OrderEvent
    .partition_by(customer_id)
    .concurrent(workers: 8)  # Nouveau!
    .aggregate(...)
```

### Livrables Phase 4
- [ ] Suite de benchmarks CI
- [ ] Documentation performance
- [ ] Parallélisation basique (partition_by)
- [ ] Objectifs: 500K events/sec single-node

---

## Phase 5: Observabilité (Juin 2026)
**Durée estimée**: 3 semaines  
**Priorité**: MOYENNE

### 5.1 OpenTelemetry
| Tâche | Effort |
|-------|--------|
| Intégration opentelemetry-rust | 2j |
| Spans automatiques | 2j |
| Context propagation | 1j |
| **Total** | **5j** |

### 5.2 Dashboard
| Tâche | Effort |
|-------|--------|
| API REST status | 1j |
| Vue streams actifs | 2j |
| Graphiques Grafana | 2j |
| **Total** | **5j** |

### Livrables Phase 5
- [ ] Tracing OTLP vers Jaeger/Tempo
- [ ] Dashboard Grafana
- [ ] Healthcheck endpoint

---

## Phase 6: Production (Juillet 2026)
**Durée estimée**: 4 semaines  
**Priorité**: HAUTE

### 6.1 Packaging
| Tâche | Effort |
|-------|--------|
| Dockerfile optimisé | 1j |
| docker-compose examples | 1j |
| Helm chart basique | 2j |
| **Total** | **4j** |

### 6.2 Documentation
| Tâche | Effort |
|-------|--------|
| Getting Started guide | 2j |
| API Reference | 3j |
| Deployment guide | 2j |
| **Total** | **7j** |

### 6.3 Security
| Tâche | Effort |
|-------|--------|
| TLS support | 1j |
| Authentication basique | 2j |
| Secrets management | 1j |
| **Total** | **4j** |

### Livrables Phase 6
- [ ] Image Docker < 50MB
- [ ] Helm chart
- [ ] Documentation complète
- [ ] Security hardening basique

---

## Métriques de Succès

### Q2 2026
| Métrique | Cible |
|----------|-------|
| Couverture tests | ≥ 80% |
| Latence p99 | < 10ms |
| Throughput | ≥ 100K events/sec |

### Q3 2026
| Métrique | Cible |
|----------|-------|
| Couverture tests | ≥ 85% |
| Latence p99 | < 5ms |
| Throughput | ≥ 500K events/sec |
| Uptime | ≥ 99.9% |

---

## Ressources Requises

### Humaines
- 1 développeur Rust senior (full-time)
- 0.5 développeur DevOps (CI/CD, Docker)

### Infrastructure
- CI runners (GitHub Actions)
- Test cluster Kafka
- Benchmark machine (16 cores, 64GB RAM)

### Budget
| Poste | Coût mensuel |
|-------|--------------|
| CI/CD (GitHub) | ~$50 |
| Cloud tests | ~$100 |
| Domaine + infra | ~$30 |
| **Total** | **~$180/mois** |

---

## Prochaines Actions Immédiates

### Cette Semaine
1. [ ] Implémenter FileSink JSON Lines
2. [ ] Créer GitHub Actions workflow
3. [ ] Fixer les 2 warnings Rust

### Semaine Prochaine
1. [ ] Implémenter HttpWebhookSink
2. [ ] Tests CLI commande `run`
3. [ ] Premier benchmark basique

---

*Plan mis à jour le 25/01/2026*
