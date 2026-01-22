# Gestion de l'état

## Backends supportés

### In-Memory

```varpulis
state:
    backend: "in_memory"
    capacity: 1000000      # nombre max d'entrées
    eviction: "lru"        # lru, lfu, fifo
```

**Avantages**: Latence minimale, simplicité  
**Inconvénients**: Perte de données au redémarrage, limité par la RAM

### RocksDB

```varpulis
state:
    backend: "rocksdb"
    path: "/var/lib/varpulis/state"
    compression: "zstd"
    block_cache_size: 134217728  # 128 MB
```

**Avantages**: Persistance, gestion de volumes importants  
**Inconvénients**: Latence légèrement supérieure

## Checkpointing

### Configuration

```varpulis
checkpoint:
    enabled: true
    interval: 10s
    
    strategy:
        type: "incremental"  # ou "full"
        compression: "zstd"
        location: "s3://varpulis-checkpoints/prod/"
    
    retention:
        keep_last: 10
        max_age: 7d
```

### Types de checkpoints

| Type | Description | Usage |
|------|-------------|-------|
| **Full** | Snapshot complet de l'état | Recovery après crash majeur |
| **Incremental** | Seuls les deltas depuis le dernier checkpoint | Recovery rapide, moins d'I/O |

### Processus de recovery

1. Identification du dernier checkpoint valide
2. Restauration de l'état depuis le checkpoint
3. Replay des événements depuis le dernier offset Kafka
4. Reprise du traitement normal

## Fenêtres temporelles

### Types de fenêtres

```varpulis
# Tumbling window (non-overlapping)
stream Metrics = Events
    .window(5m)
    .aggregate(count())

# Sliding window (overlapping)
stream Metrics = Events
    .window(5m, sliding: 1m)
    .aggregate(avg(value))

# Session window (gap-based)
stream Sessions = Events
    .session_window(gap: 30m)
    .aggregate(collect())
```

### Gestion de la mémoire

- Éviction automatique des données hors fenêtre
- Watermarks pour gérer les événements tardifs
- Configuration du délai de grâce (late events)

```varpulis
window:
    allowed_lateness: 5m
    watermark_strategy: "bounded_out_of_orderness"
```
