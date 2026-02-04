# State Management

## Overview

Varpulis provides a pluggable state persistence layer with three storage backends. State management covers two distinct areas:

1. **Tenant/pipeline state** -- which tenants exist, their pipelines, quotas, and usage counters. This is fully persistent with `--state-dir`.
2. **Engine processing state** -- window contents, pattern matcher partial matches, aggregation accumulators. The checkpoint infrastructure exists but is not yet wired into the engine event loop.

## Storage Backends

### In-Memory (`MemoryStore`)

Default backend. No configuration needed.

- Zero latency
- Data lost on restart
- Suitable for development and testing

### File System (`FileStore`)

Enabled via `--state-dir` CLI flag. Stores key-value pairs as files with atomic writes (temp file + rename).

```bash
varpulis server --port 9000 --api-key "key" --state-dir /var/lib/varpulis/state
```

- Tenant and pipeline metadata persists across restarts
- Atomic writes prevent corruption
- Keys with `:` separators map to subdirectories

### RocksDB (`RocksDbStore`)

Available behind the `persistence` feature flag. Optimized for write-heavy workloads.

```bash
# Build with RocksDB support
cargo build --release --features persistence

# Use in code
use varpulis_runtime::persistence::{RocksDbStore, StateStore};
let store = RocksDbStore::open("/var/lib/varpulis/state")?;
```

Configuration (set in code):
- Compression: LZ4
- Write buffer: 64 MB
- Max write buffers: 3
- Target file size: 64 MB

## Checkpointing

### Current State

The checkpoint infrastructure is implemented but not yet integrated into the engine event loop. This means:

- `CheckpointManager`, `Checkpoint`, `WindowCheckpoint`, and `PatternCheckpoint` types exist
- The `StateStore` trait supports save/load/list/prune checkpoint operations
- Tenant state recovery works end-to-end
- **Engine window and pattern state is not yet checkpointed** -- this is in-memory only

### CheckpointConfig

```rust
CheckpointConfig {
    interval: Duration::from_secs(60),   // How often to checkpoint
    max_checkpoints: 3,                  // Retain last N checkpoints
    checkpoint_on_shutdown: true,        // Save on graceful shutdown
    key_prefix: "varpulis".to_string(),  // Storage key prefix
}
```

### Checkpoint Contents

When engine integration is complete, each checkpoint will contain:

| Field | Description |
|-------|-------------|
| `id` | Monotonically increasing checkpoint ID |
| `timestamp_ms` | When the checkpoint was created |
| `events_processed` | Total events processed at checkpoint time |
| `window_states` | Events currently in active windows (per stream) |
| `pattern_states` | Active partial matches in pattern matchers |
| `metadata` | Custom key-value metadata |

### Recovery Process

For tenant/pipeline state (working today):

1. Server starts with `--state-dir`
2. `TenantManager` loads tenant snapshots from the store
3. Pipelines are restored and re-compiled from saved VPL source
4. Usage counters and quotas are restored

For engine state (planned):

1. Load latest valid checkpoint
2. Restore window contents and pattern matcher state
3. Resume processing from checkpoint position

## Temporal Windows

### Window Types

```varpulis
# Tumbling window (non-overlapping)
stream Metrics = Events
    .window(5m)
    .aggregate(count: count())

# Sliding window (overlapping)
stream Metrics = Events
    .window(5m, sliding: 1m)
    .aggregate(avg_val: avg(value))
```

### Memory Management

- Automatic eviction of out-of-window events
- Window contents are held in memory
- Partitioned windows maintain separate state per partition key

```varpulis
# Per-zone windowed aggregation
stream ZoneStats = Temperatures
    .partition_by(zone)
    .window(5m)
    .aggregate(
        zone: last(zone),
        avg_temp: avg(value),
        max_temp: max(value)
    )
```

## Planned Features

- Engine checkpoint integration (save/restore window and pattern state)
- Exactly-once processing semantics
- Watermarks for late event handling
- Session windows
- External checkpoint storage (S3)
