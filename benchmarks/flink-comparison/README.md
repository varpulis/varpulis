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

## Scénarios de Test

### Scénario 1: Agrégation Simple
**Use case**: Compter les événements par catégorie sur une fenêtre glissante de 5 minutes.

### Scénario 2: Pattern Temporel (A → B)
**Use case**: Détecter quand un événement Login est suivi d'un événement FailedTransaction du même utilisateur dans les 10 minutes.

### Scénario 3: Pattern Multi-Événements (A → B → C)
**Use case**: Détection de fraude - Transaction suspicieuse suivie de plusieurs petites transactions puis d'un retrait important.

### Scénario 4: Jointure de Streams
**Use case**: Corréler les prix de deux marchés différents pour détecter des opportunités d'arbitrage.

### Scénario 5: Détection d'Anomalies
**Use case**: Identifier des patterns anormaux dans des séries temporelles (température, trafic réseau).

---

## Implémentations

Voir les sous-dossiers pour chaque scénario:
- `scenario1-aggregation/`
- `scenario2-sequence/`
- `scenario3-fraud/`
- `scenario4-join/`
- `scenario5-anomaly/`

---

## Résumé des Avantages

### Varpulis - Points Forts
1. **DSL déclaratif concis** - 5-10x moins de code
2. **Patterns temporels natifs** - Opérateur `->` intuitif
3. **Attention mechanism** - Détection d'anomalies unique
4. **Démarrage instantané** - Pas de cluster à configurer
5. **Faible empreinte mémoire** - Single process optimisé

### Apache Flink - Points Forts
1. **Scalabilité horizontale** - Clusters distribués
2. **Exactly-once semantics** - Garanties de traitement
3. **Écosystème mature** - Connecteurs, monitoring
4. **State backends** - RocksDB, checkpointing
5. **Support entreprise** - Large communauté

---

## Quand utiliser Varpulis ?

✅ Prototypage rapide de règles CEP
✅ Applications mono-nœud haute performance
✅ Patterns temporels complexes (SASE+)
✅ Détection d'anomalies avec corrélation
✅ Environnements edge/embarqués

## Quand utiliser Flink ?

✅ Traitement distribué à grande échelle
✅ Garanties exactly-once critiques
✅ Intégration écosystème Hadoop/Kafka
✅ State management complexe
✅ Équipes Java/Scala existantes
