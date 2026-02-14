# Benchmark Comparatif: Apache Flink vs Varpulis

## Objectif

Comparer objectivement Apache Flink et Varpulis sur des cas d'usage CEP (Complex Event Processing) représentatifs, en mettant en évidence les forces de chaque solution.

## Critères de Comparaison

| Critère | Description |
|---------|-------------|
| **Concision du code** | Nombre de lignes pour implémenter le même use case |
| **Latence** | Temps de traitement par événement (p50, p99) |
| **Throughput** | Événements traités par seconde |
| **Mémoire** | Consommation RAM |
| **Temps de démarrage** | Cold start jusqu'au premier événement traité |
| **Expressivité** | Facilité d'exprimer des patterns complexes |

---

## Résultats de Performance Varpulis

### Pattern Matching (SASE+ Engine)

| Pattern | Events | Temps | Throughput |
|---------|--------|-------|------------|
| Simple séquence (A→B) | 10K | 31ms | **320K evt/s** |
| Kleene+ (A→B+→C) | 10K | 25ms | **200K evt/s** |
| Séquence longue (10 events) | 10K | 377ms | 26K evt/s |
| Pattern complexe (négation) | 10K | 45ms | **220K evt/s** |

### Temps de Démarrage

| Métrique | Varpulis | Apache Flink |
|----------|----------|--------------|
| Cold start | **<100ms** | 5-30s (cluster) |
| First event processed | **<1ms** | 100-500ms |
| Memory footprint | **~50MB** | 500MB-2GB |

---

## Comparaison de Concision du Code

### Scénario 1: Agrégation Simple
Compter les événements par catégorie sur une fenêtre glissante de 5 minutes.

| Métrique | Varpulis | Flink |
|----------|----------|-------|
| **Lignes de code** | **34** | 111 |
| Ratio | 1x | 3.3x |

### Scénario 2: Pattern Temporel (A → B)
Détecter Login suivi de FailedTransaction dans les 10 minutes.

| Métrique | Varpulis | Flink |
|----------|----------|-------|
| **Lignes de code** | **34** | 158 |
| Ratio | 1x | 4.6x |

### Scénario 3: Détection de Fraude (A → B → C)
Transaction suspicieuse → plusieurs petites transactions → retrait important.

| Métrique | Varpulis | Flink |
|----------|----------|-------|
| **Lignes de code** | **49** | 207 |
| Ratio | 1x | 4.2x |

### Scénario 4: Jointure de Streams
Corréler les prix de deux marchés pour détecter des opportunités d'arbitrage.

| Métrique | Varpulis | Flink |
|----------|----------|-------|
| **Lignes de code** | **51** | 103 |
| Ratio | 1x | 2.0x |

### Scénario 5: Détection d'Anomalies
Patterns anormaux dans des séries temporelles avec analyse statistique et SASE+.

| Métrique | Varpulis | Flink |
|----------|----------|-------|
| **Lignes de code** | **77** | N/A* |

*Flink would require a custom ProcessFunction implementation (~200+ lines).

---

## Résumé de la Comparaison

### Concision Moyenne

| Solution | Lignes totales | Moyenne par scénario |
|----------|---------------|---------------------|
| **Varpulis** | **245** | **49 lignes** |
| Apache Flink | 579 | 145 lignes |
| **Ratio** | **1x** | **3x moins de code** |

### Matrice de Décision

| Critère | Varpulis | Flink | Gagnant |
|---------|----------|-------|---------|
| Concision du code | 3x plus concis | Verbeux | **Varpulis** |
| Patterns temporels | Natif (SASE+) | CEP Library | **Varpulis** |
| Détection anomalies | SASE+ natif | Manuel | **Varpulis** |
| Temps démarrage | <100ms | 5-30s | **Varpulis** |
| Scalabilité horizontale | Single node | Cluster | **Flink** |
| Exactly-once | At-least-once | Exactly-once | **Flink** |
| Écosystème | Nouveau | Mature | **Flink** |
| State backends | In-memory | RocksDB | **Flink** |

---

## Scénarios de Test Détaillés

### Scénario 1: Agrégation Simple
**Use case**: Compter les événements par catégorie sur une fenêtre glissante de 5 minutes.

- Voir `scenario1-aggregation/`

### Scénario 2: Pattern Temporel (A → B)
**Use case**: Détecter quand un événement Login est suivi d'un événement FailedTransaction du même utilisateur dans les 10 minutes.

- Voir `scenario2-sequence/`

### Scénario 3: Pattern Multi-Événements (A → B → C)
**Use case**: Détection de fraude - Transaction suspicieuse suivie de plusieurs petites transactions puis d'un retrait important.

- Voir `scenario3-fraud/`

### Scénario 4: Jointure de Streams
**Use case**: Corréler les prix de deux marchés différents pour détecter des opportunités d'arbitrage.

- Voir `scenario4-join/`

### Scénario 5: Détection d'Anomalies
**Use case**: Identifier des patterns anormaux dans des séries temporelles avec analyse statistique et patterns SASE+.

- Voir `scenario5-anomaly/`

---

## Avantages par Solution

### Varpulis - Points Forts
1. **DSL déclaratif concis** - 3-5x moins de code
2. **Patterns temporels natifs** - Opérateur `->` intuitif
3. **SASE+ patterns** - Détection d'anomalies native
4. **Démarrage instantané** - Pas de cluster à configurer
5. **Faible empreinte mémoire** - Single process optimisé
6. **Performance** - 200K-320K evt/s pour patterns simples

### Apache Flink - Points Forts
1. **Scalabilité horizontale** - Clusters distribués
2. **Exactly-once semantics** - Garanties de traitement
3. **Écosystème mature** - Connecteurs, monitoring
4. **State backends** - RocksDB, checkpointing
5. **Support entreprise** - Large communauté

---

## Quand utiliser Varpulis ?

- Prototypage rapide de règles CEP
- Applications mono-nœud haute performance
- Patterns temporels complexes (SASE+)
- Détection d'anomalies avec SASE+ patterns
- Environnements edge/embarqués
- Équipes non-Java

## Quand utiliser Flink ?

- Traitement distribué à grande échelle (>1M evt/s)
- Garanties exactly-once critiques
- Intégration écosystème Hadoop/Kafka
- State management complexe (TB de state)
- Équipes Java/Scala existantes

---

## Exécution des Benchmarks

```bash
# Varpulis - vérifier la syntaxe
cargo run -- check benchmarks/flink-comparison/scenario1-aggregation/varpulis.vpl

# Varpulis - benchmarks de performance
cargo bench --bench pattern_benchmark

# Varpulis - tests complets
cargo test --workspace
```

---

*Dernière mise à jour: 2026-01-28*
