# Architecture système

## Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────┐
│                   Varpulis Runtime Engine                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Compiler   │  │   Optimizer  │  │   Validator  │       │
│  │ (VarpulisQL) │  │              │  │              │       │
│  └──────┬───────┘  └──────┬───────┘  └───────┬──────┘       │
│         │                 │                  │              │
│         └─────────────────▼──────────────────┘              │
│                    Execution Graph                          │
│         ┌───────────────────────────────────┐               │
│         │                                   │               │
├─────────▼───────────────────────────────────▼───────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Ingestion  │  │    Pattern   │  │   Attention  │       │
│  │    Layer     │──│    Matcher   │──│    Engine    │       │
│  │              │  │  (Hypertrees)│  │(Déterministe)│       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Embedding   │  │   State Mgmt │  │ Aggregation  │       │
│  │   Engine     │  │   (RocksDB/  │  │   Engine     │       │
│  │              │  │   In-Memory) │  │              │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Observability│  │ Parallelism  │  │  Checkpoint  │       │
│  │    Layer     │  │   Manager    │  │   Manager    │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
```

## Flow de traitement

```
Event Sources → Ingestion → Embedding → Pattern Matching → Aggregation → Sink
                                 ↓              ↑
                            Attention      Hypertree
                             Scores       Structures
```

## Composants

### Compiler
- Parse VarpulisQL via LALRPOP
- Génère une IR (Intermediate Representation)
- Optimisations statiques

### Execution Graph
- DAG (Directed Acyclic Graph) des opérations
- Scheduling intelligent
- Fusion d'opérateurs quand possible

### Ingestion Layer
- Connecteurs sources (Kafka, fichiers, HTTP, etc.)
- Désérialisation (JSON, Avro, Protobuf)
- Validation de schéma

### Pattern Matcher
- Structures hypertree pour matching efficace
- Support patterns temporels
- Détection de séquences

### Attention Engine
- Voir [attention-engine.md](attention-engine.md)

### Embedding Engine
- Génération de vecteurs pour les événements
- Mode rule-based ou learned

### State Manager
- Voir [state-management.md](state-management.md)

### Aggregation Engine
- Fonctions d'agrégation (sum, avg, count, min, max, stddev, etc.)
- Fenêtres temporelles (tumbling, sliding, session)
- Groupement par clé

### Parallelism Manager
- Voir [parallelism.md](parallelism.md)

### Observability Layer
- Voir [observability.md](observability.md)

### Checkpoint Manager
- Snapshots de l'état
- Recovery après crash
- Support S3, local filesystem
