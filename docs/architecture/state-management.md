# State Management

## Supported Backends

### In-Memory

```varpulis
state:
    backend: "in_memory"
    capacity: 1000000      # max entries
    eviction: "lru"        # lru, lfu, fifo
```

**Advantages**: Minimal latency, simplicity  
**Disadvantages**: Data loss on restart, limited by RAM

### RocksDB

```varpulis
state:
    backend: "rocksdb"
    path: "/var/lib/varpulis/state"
    compression: "zstd"
    block_cache_size: 134217728  # 128 MB
```

**Advantages**: Persistence, handles large volumes  
**Disadvantages**: Slightly higher latency

## Checkpointing

### Configuration

```varpulis
checkpoint:
    enabled: true
    interval: 10s
    
    strategy:
        type: "incremental"  # or "full"
        compression: "zstd"
        location: "s3://varpulis-checkpoints/prod/"
    
    retention:
        keep_last: 10
        max_age: 7d
```

### Checkpoint Types

| Type | Description | Use Case |
|------|-------------|----------|
| **Full** | Complete state snapshot | Recovery after major crash |
| **Incremental** | Only deltas since last checkpoint | Fast recovery, less I/O |

### Recovery Process

1. Identify last valid checkpoint
2. Restore state from checkpoint
3. Replay events from last Kafka offset
4. Resume normal processing

## Temporal Windows

### Window Types

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

### Memory Management

- Automatic eviction of out-of-window data
- Watermarks to handle late events
- Grace period configuration (late events)

```varpulis
window:
    allowed_lateness: 5m
    watermark_strategy: "bounded_out_of_orderness"
```
