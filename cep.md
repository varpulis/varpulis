# Varpulis - Streaming Analytics Engine
## Spécifications Techniques v0.1

> **Varpulis** - Dans la mythologie slave, esprit du vent, compagnon du dieu du tonnerre.
> 
> Voir la documentation complète dans [`docs/`](docs/README.md)

---

## 1. Vue d'ensemble

### 1.1 Vision
Varpulis est un moteur de streaming analytics nouvelle génération combinant :
- La syntaxe intuitive d'Apama (EPL)
- L'efficacité des hypertrees pour la détection de patterns
- Les capacités des architectures transformer (attention mechanisms) de manière **déterministe**
- Une approche unifiée simplifiant radicalement le développement par rapport à Apama

### 1.2 Langage cible
**Rust** - choisi pour :
- Performance native (requis pour CEP haute fréquence)
- Sécurité mémoire sans garbage collector
- Excellent écosystème async (`tokio`, `async-std`)
- Support SIMD et optimisations bas niveau
- Facilité de déploiement (binaire statique)

### 1.3 Différenciateurs clés vs Apama

| Caractéristique | Apama | Varpulis |
|----------------|-------|-------|
| **Agrégation multi-streams** | ❌ Impossible directement | ✅ Native |
| **Parallélisation** | `spawn` complexe et risqué | Déclarative et supervisée |
| **Listeners vs Streams** | Séparés et complexes | Unifiés dans un concept unique |
| **Debugging** | Difficile | Observabilité intégrée |
| **Pattern detection** | Hypertrees uniquement | Hypertrees + Attention |
| **Latence** | < 1ms possible | Configurable (< 1ms à < 100ms) |

---

## 2. Architecture système

### 2.1 Composants principaux

```
┌─────────────────────────────────────────────────────────┐
│                  Varpulis Runtime Engine                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │   Compiler   │  │   Optimizer  │  │   Validator  │   │
│  │   (VarpulisQL)  │  │              │  │              │   │
│  └──────┬───────┘  └──────┬───────┘  └───────┬──────┘   │
│         │                 │                  │          │
│         └─────────────────▼──────────────────┘          │
│                    Execution Graph                      │
│         ┌───────────────────────────────────┐           │
│         │                                   │           │
├─────────▼───────────────────────────────────▼───────────┤
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │   Ingestion  │  │    Pattern   │  │   Attention  │   │
│  │    Layer     │──│    Matcher   │──│    Engine    │   │
│  │              │  │  (Hypertrees)│  │(Déterministe)│   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  Embedding   │  │   State Mgmt │  │ Aggregation  │   │
│  │   Engine     │  │   (RocksDB/  │  │   Engine     │   │
│  │              │  │   In-Memory) │  │              │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Observability│  │ Parallelism  │  │  Checkpoint  │   │
│  │    Layer     │  │   Manager    │  │   Manager    │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Flow de traitement

```
Event Sources → Ingestion → Embedding → Pattern Matching → Aggregation → Sink
                                 ↓              ↑
                            Attention      Hypertree
                             Scores       Structures
```

---

## 3. Le langage VarpulisQL

### 3.1 Philosophie de design
- **Tout est un Stream** : Pas de distinction artificielle listeners/streams
- **Déclaratif** : L'utilisateur décrit QUOI, pas COMMENT
- **Composable** : Les opérations s'enchaînent naturellement
- **Type-safe** : Détection d'erreurs à la compilation
- **Observable** : Métriques et traces intégrées

### 3.2 Syntaxe de base

#### 3.2.1 Déclaration de streams

```rust
// Stream simple depuis une source
stream Trades from TradeEvent;

// Stream avec filtrage
stream HighValueTrades = Trades
  .where(price > 10000);

// Stream avec fenêtre temporelle
stream RecentTrades = Trades
  .window(5 minutes);
```

#### 3.2.2 Agrégation multi-streams (NOUVEAU vs Apama)

```rust
// Agrégation sur plusieurs sensors - IMPOSSIBLE dans Apama!
stream BuildingMetrics = merge(
    stream Sensor1 from TempEvent where sensor_id == "S1",
    stream Sensor2 from TempEvent where sensor_id == "S2",
    stream Sensor3 from TempEvent where sensor_id == "S3",
    stream Sensor4 from TempEvent where sensor_id == "S4"
  )
  .window(1 minute, sliding: 10 seconds)
  .aggregate({
    avg_temp: avg(temperature),
    min_temp: min(temperature),
    max_temp: max(temperature),
    sensor_count: count(distinct sensor_id),
    std_dev: stddev(temperature)
  });
```

#### 3.2.3 Patterns avec attention mechanism

```rust
// Pattern non-contigu avec attention
stream FraudDetection = Trades
  .attention_window(
    duration: 30 seconds,
    heads: 4,               // Multi-head attention
    embedding: rule_based   // ou learned("model.bin")
  )
  .pattern({
    // Détecte des corrélations via attention scores
    suspicious: (events) => {
      let high_value = events.filter(e => e.amount > 10000);
      let correlations = high_value
        .map(e1 => 
          high_value
            .filter(e2 => e2.id != e1.id)
            .map(e2 => (e1, e2, attention_score(e1, e2)))
        )
        .flatten()
        .filter(|(e1, e2, score)| score > 0.85);
      
      correlations.length > 3  // 3+ paires hautement corrélées
    }
  })
  .emit({ 
    alert_type: "attention_pattern_fraud",
    correlated_events: events
  });
```

#### 3.2.4 Parallélisation déclarative (vs spawn dans Apama)

```rust
// Parallélisation safe et déclarative
stream OrderProcessing = Orders
  .partition_by(customer_id)  // Isolation automatique
  .concurrent(
    max_workers: 8,
    strategy: "consistent_hash",  // ou "round_robin"
    supervision: {
      restart: "always",
      max_restarts: 3,
      backoff: "exponential"
    }
  )
  .process(order => {
    // Traitement lourd
    validate_order(order)?;
    check_inventory(order)?;
    calculate_shipping(order)?;
    Ok(order)
  })
  .on_error(|error, order| {
    log_error(error);
    emit_to_dlq(order);
  })
  .collect();  // Remerge les résultats
```

#### 3.2.5 Joins complexes multi-streams

```rust
// Join de 3 streams avec fenêtre temporelle
stream EnrichedOrders = join(
    stream Orders from OrderEvent,
    stream Customers from CustomerEvent 
      on Orders.customer_id == Customers.id,
    stream Inventory from InventoryEvent 
      on Orders.product_id == Inventory.product_id
  )
  .window(5 minutes, policy: "watermark")
  .emit({
    order_id: Orders.id,
    customer_name: Customers.name,
    customer_tier: Customers.tier,
    product: Orders.product_id,
    stock_available: Inventory.quantity,
    timestamp: Orders.timestamp
  });
```

#### 3.2.6 Observabilité intégrée

```rust
stream ProcessedTrades = RawTrades
  .tap(log: "trades.ingestion", sample: 0.1)  // Log 10%
  .validate(schema: TradeSchema)
  .tap(metrics: {
    counter: "trades.validated",
    histogram: "trade.value"
  })
  .where(price > 0)
  .tap(trace: {
    name: "positive_trades",
    when: debug_mode,
    span_context: true
  })
  .emit();
```

### 3.3 Configuration des modes de performance

```rust
// Configuration globale du runtime
config {
  mode: "low_latency",  // ou "throughput" ou "balanced"
  
  embedding: {
    type: "rule_based",  // ou "learned"
    model: "models/trade_embeddings.safetensors",  // si learned
    dim: 128
  },
  
  attention: {
    enabled: true,
    compute: "cpu",  // ou "gpu" si disponible
    precision: "float32",  // ou "float16"
    batch_size: 1000  // pour mode throughput
  },
  
  state: {
    backend: "rocksdb",  // ou "in_memory"
    path: "/var/lib/velos/state",
    checkpoint_interval: 10 seconds
  },
  
  parallelism: {
    worker_threads: 16,
    io_threads: 4
  },
  
  observability: {
    metrics: {
      enabled: true,
      endpoint: "localhost:9090",
      interval: 1 second
    },
    tracing: {
      enabled: true,
      endpoint: "localhost:4317",  // OTLP
      sample_rate: 0.01
    },
    logging: {
      level: "info",
      format: "json"
    }
  }
}
```

---

## 4. Architecture de l'Attention Engine

### 4.1 Principe
Contrairement aux LLMs probabilistes, l'attention dans Varpulis est **déterministe** :
- Les scores d'attention sont calculés de manière reproductible
- Pas de génération, seulement de la corrélation
- Les embeddings peuvent être rule-based ou appris offline

### 4.2 Embeddings

#### 4.2.1 Rule-based (par défaut)

```rust
// Configuration des embeddings rule-based
embedding_config {
  type: "composite",
  
  numeric_features: [
    { field: "price", transform: "log_scale", weight: 0.3 },
    { field: "volume", transform: "normalize", weight: 0.2 },
    { field: "timestamp", transform: "cyclical", weight: 0.1 }
  ],
  
  categorical_features: [
    { field: "symbol", method: "hash", dim: 32, weight: 0.2 },
    { field: "market", method: "one_hot", weight: 0.2 }
  ]
}
```

#### 4.2.2 Learned embeddings (mode avancé)

```rust
// Configuration pour embeddings appris
embedding_config {
  type: "learned",
  model_path: "models/event_embeddings.safetensors",
  
  // Le modèle a été entraîné offline sur des données historiques
  // et génère des embeddings déterministes
  freeze: true,  // Pas de fine-tuning en production
  
  // Fallback si modèle non disponible
  fallback: "rule_based"
}
```

### 4.3 Calcul d'attention déterministe

```rust
// Pseudo-code du calcul d'attention
fn compute_attention(
    event_current: Event,
    event_history: &[Event],
    config: &AttentionConfig
) -> Vec<(EventId, f32)> {
    let emb_current = embedding_engine.embed(event_current);
    
    event_history
        .iter()
        .map(|e_hist| {
            let emb_hist = embedding_engine.embed(e_hist);
            
            // Multi-head attention déterministe
            let scores: Vec<f32> = (0..config.num_heads)
                .map(|head| {
                    let q = project(emb_current, head, "query");
                    let k = project(emb_hist, head, "key");
                    dot_product(q, k) / sqrt(dim)
                })
                .collect();
            
            // Agrégation des heads
            let final_score = scores.iter().sum::<f32>() / config.num_heads;
            
            (e_hist.id, final_score)
        })
        .collect()
}
```

### 4.4 Optimisations

```rust
// Cache des embeddings pour éviter recalculs
struct EmbeddingCache {
    cache: LruCache<EventHash, Vec<f32>>,
    ttl: Duration
}

// Indexation pour recherche rapide
struct AttentionIndex {
    // ANN index pour trouver rapidement les événements similaires
    hnsw: HnswIndex<f32>,
    // Seuil de similarité minimum
    threshold: f32
}
```

---

## 5. Gestion de l'état

### 5.1 Backends supportés

```rust
enum StateBackend {
    InMemory {
        capacity: usize,
        eviction: EvictionPolicy
    },
    RocksDB {
        path: PathBuf,
        compression: CompressionType,
        block_cache_size: usize
    },
    // Future: distributed backends
}
```

### 5.2 Checkpointing

```rust
checkpoint_config {
  enabled: true,
  interval: 10 seconds,
  
  strategy: {
    type: "incremental",  // ou "full"
    compression: "zstd",
    location: "s3://velos-checkpoints/prod/"
  },
  
  retention: {
    keep_last: 10,
    max_age: 7 days
  }
}
```

---

## 6. Parallélisation et supervision

### 6.1 Modèle de parallélisation

```
                    ┌─────────────┐
                    │   Scheduler │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐       ┌─────▼────┐      ┌─────▼────┐
   │ Worker 1│       │ Worker 2 │      │ Worker N │
   │         │       │          │      │          │
   │ Partition 1     │ Partition 2     │ Partition N
   └─────────┘       └──────────┘      └──────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                    ┌──────▼──────┐
                    │  Collector  │
                    └─────────────┘
```

### 6.2 Supervision automatique

```rust
supervision_config {
  // Stratégie de restart
  restart_policy: "always",  // ou "on_failure", "never"
  
  // Backoff exponentiel
  backoff: {
    initial: 1 second,
    max: 1 minute,
    multiplier: 2.0
  },
  
  // Circuit breaker
  circuit_breaker: {
    enabled: true,
    failure_threshold: 5,
    timeout: 30 seconds,
    half_open_requests: 3
  },
  
  // Health checks
  health_check: {
    interval: 5 seconds,
    timeout: 1 second
  }
}
```

---

## 7. Observabilité

### 7.1 Métriques automatiques

Pour chaque stream, Varpulis génère automatiquement :

```
# Throughput
varpulis_stream_events_total{stream="BuildingMetrics",status="success"}
varpulis_stream_events_total{stream="BuildingMetrics",status="error"}

# Latence
varpulis_stream_latency_seconds{stream="BuildingMetrics",quantile="0.5"}
varpulis_stream_latency_seconds{stream="BuildingMetrics",quantile="0.95"}
varpulis_stream_latency_seconds{stream="BuildingMetrics",quantile="0.99"}

# Backpressure
varpulis_stream_queue_size{stream="BuildingMetrics"}
varpulis_stream_queue_utilization{stream="BuildingMetrics"}

# Pattern matching
varpulis_pattern_matches_total{pattern="FraudDetection"}
varpulis_attention_computation_seconds{stream="FraudDetection"}
```

### 7.2 Distributed tracing

```rust
// Les spans sont automatiquement créés pour :
// - Ingestion d'événements
// - Embedding calculation
// - Pattern matching
// - Attention computation
// - Aggregation
// - Emission vers sinks

// Compatible OpenTelemetry
tracing_config {
  exporter: "otlp",
  endpoint: "localhost:4317",
  service_name: "velos-engine",
  sample_rate: 0.01,  // 1% sampling
  
  // Contexte propagation
  propagation: ["tracecontext", "baggage"]
}
```

---

## 8. Roadmap de développement

### Phase 1 : Core Engine (3-4 mois)
- [ ] Compilateur VarpulisQL → IR
- [ ] Execution engine basique (single-threaded)
- [ ] Hypertree pattern matcher
- [ ] Agrégations simples (sum, avg, count, min, max)
- [ ] Backends: in-memory state
- [ ] Sources: Kafka, fichiers

### Phase 2 : Attention & Parallélisation (2-3 mois)
- [ ] Embedding engine (rule-based)
- [ ] Attention mechanism déterministe
- [ ] Parallélisation déclarative
- [ ] Supervision et restart automatique
- [ ] State backend: RocksDB

### Phase 3 : Observabilité & Production (2 mois)
- [ ] Métriques Prometheus
- [ ] Tracing OpenTelemetry
- [ ] Checkpointing
- [ ] CLI et tooling
- [ ] Documentation complète

### Phase 4 : Fonctionnalités avancées (3-4 mois)
- [ ] Learned embeddings
- [ ] GPU support pour attention
- [ ] Distributed mode (multi-nodes)
- [ ] Web UI pour monitoring
- [ ] Hot reload de configurations

---

## 9. Benchmarks cibles

### 9.1 Latence (mode low_latency)
- p50: < 500 µs
- p95: < 2 ms
- p99: < 5 ms

### 9.2 Throughput (mode throughput)
- Events simples: > 1M events/sec (single node)
- Avec attention: > 100K events/sec
- Avec agrégations complexes: > 500K events/sec

### 9.3 vs Apama (benchmarks indicatifs)
- Latence similaire en mode low_latency
- Throughput 2-3x supérieur grâce à Rust
- Memory footprint réduit de 40%

---

## 10. Exemples d'usage

### 10.1 Finance : Détection de market manipulation

```rust
stream MarketManipulation = Trades
  .attention_window(60 seconds, heads: 8)
  .pattern({
    pump_and_dump: (events) => {
      // Phase 1: Accumulation rapide
      let volume_spike = events
        .window(0..10 seconds)
        .aggregate(sum(volume)) > threshold_high;
      
      // Phase 2: Pattern d'attention inhabituel
      let coordinated = events
        .filter_pairs(|e1, e2| {
          attention_score(e1, e2) > 0.9 &&
          e1.account != e2.account
        })
        .count() > 5;
      
      // Phase 3: Dump
      let price_drop = events
        .window(50..60 seconds)
        .aggregate(price_change()) < -0.05;
      
      volume_spike && coordinated && price_drop
    }
  })
  .emit({ 
    alert_severity: "high",
    pattern_type: "pump_and_dump",
    evidence: events
  });
```

### 10.2 IoT : Prédiction de panne

```rust
stream MachinePredictiveFailure = merge(
    stream Vibrations from SensorEvent where type == "vibration",
    stream Temperature from SensorEvent where type == "temp",
    stream Pressure from SensorEvent where type == "pressure"
  )
  .attention_window(
    duration: 5 minutes,
    embedding: learned("models/machine_health.safetensors")
  )
  .pattern({
    degradation: (events) => {
      // Utilise l'attention pour détecter des corrélations
      // entre vibrations, température et pression
      let high_attention_clusters = events
        .cluster_by_attention(threshold: 0.85);
      
      // Pattern de dégradation progressive
      high_attention_clusters.any(cluster => {
        let trend_vib = cluster.filter(type == "vibration")
          .linear_trend() > 0.1;
        let trend_temp = cluster.filter(type == "temp")
          .linear_trend() > 0.15;
        
        trend_vib && trend_temp
      })
    }
  })
  .emit({
    machine_id: machine_id,
    predicted_failure_in: estimate_ttf(events),
    confidence: 0.85
  });
```

---

## 11. Noms proposés

Après recherche, voici des noms disponibles (aucun projet CEP significatif sur GitHub) :

### Option 1: **Korros** ⭐ (Recommandé)
- Étymologie : Fusion de "Correlation" et "Stream"
- Domaine : `korros.dev` et `korros.io` disponibles
- Évoque : corrélation, flow, rapidité
- GitHub : Aucun projet stream/CEP existant

### Option 2: **Nexura**
- Évoque : nexus, neural, aura
- Domaine : `nexura.io` disponible
- Moderne et tech

### Option 3: **Velora**
- Dérivé de "velocity"
- Domaine : `velora.dev` probablement disponible
- Son élégant

### Option 4: **Tempora**
- Lien direct avec le temps/temporal
- Clair pour un moteur de streaming
- Domaine : `tempora.dev` à vérifier

### Option 5: **Aethon**
- Mythologie grecque (cheval de feu)
- Évoque vitesse et puissance
- Domaine : `aethon.dev` probablement disponible

**Recommandation** : **Korros** pour sa connection claire avec le CEP (corrélation + stream) et sa disponibilité confirmée.

---

## 12. Prochaines étapes

1. **Validation du nom** : Vérifier disponibilité domaine + marque déposée
2. **Setup projet Rust** : 
   - Structure cargo workspace
   - CI/CD GitHub Actions
   - Linting (clippy, rustfmt)
3. **Spécifications détaillées** :
   - Grammaire formelle VarpulisQL/KorrosQL
   - IR (Intermediate Representation)
   - Type system
4. **POC** :
   - Parser basique VarpulisQL
   - Pattern matcher simple (sans attention)
   - Démo "hello world" avec agrégation

---

## Annexes

### A. Glossaire
- **CEP** : Complex Event Processing
- **EPL** : Event Processing Language (Apama)
- **Hypertree** : Structure de données pour pattern matching efficace
- **Attention** : Mécanisme de corrélation pondérée entre événements
- **Embedding** : Représentation vectorielle d'un événement
- **ANN** : Approximate Nearest Neighbors

### B. Références
- Apama Documentation
- Esper (CEP alternatif)
- Flink (stream processing)
- Architecture Transformer (Vaswani et al.)
- RocksDB Documentation

### C. Contributeurs initiaux
- À définir

---

**Version** : 0.1  
**Date** : 20 Janvier 2026  
**Licence** : À définir (suggestion : Apache 2.0 ou MIT)
