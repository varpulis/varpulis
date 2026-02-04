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

### Overview

Engine state checkpointing is fully integrated. All stateful components -- windows, SASE+ pattern matchers, join buffers, variables, watermark trackers -- are checkpointed and restored via `EngineCheckpoint`.

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

Each `EngineCheckpoint` contains:

| Field | Description |
|-------|-------------|
| `window_states` | Events in active windows (count, tumbling, sliding, session, partitioned) |
| `sase_states` | Active SASE+ runs, captured events, NFA state |
| `join_states` | Join buffer contents and expiry queues |
| `variables` | Engine-level `var` declarations |
| `watermark_state` | Per-source watermark positions and effective watermark |
| `events_processed` | Total events processed counter |
| `output_events_emitted` | Total output events counter |

The top-level `Checkpoint` wraps per-context `EngineCheckpoint` entries in `context_states: HashMap<String, EngineCheckpoint>`.

### Recovery Process

For tenant/pipeline state:

1. Server starts with `--state-dir`
2. `TenantManager` loads tenant snapshots from the store
3. Pipelines are restored and re-compiled from saved VPL source
4. Usage counters and quotas are restored

For engine state:

1. `CheckpointManager::recover()` loads the latest valid checkpoint from the store
2. `engine.restore_checkpoint()` restores window contents, SASE+ runs, join buffers, variables, watermark state, and counters
3. Processing resumes from the checkpoint position

See the [Checkpointing Tutorial](../tutorials/checkpointing-tutorial.md) for hands-on examples.

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

## Watermarks

Per-source watermark tracking handles out-of-order events:

```varpulis
stream Orders = OrderEvent
    .watermark(out_of_order: 10s)
    .allowed_lateness(30s)
    .window(1m)
    .aggregate(total: sum(amount))
    .emit(total_amount: total)
```

- `.watermark(out_of_order: Xs)` -- Track watermarks per event source with X seconds tolerance
- `.allowed_lateness(Ys)` -- Accept events up to Y seconds past the watermark
- Watermark advancement triggers window closure
- Watermark state is included in checkpoints

## Planned Features

- Exactly-once processing semantics (checkpoint barriers in progress)
- External checkpoint storage (S3)
- Cross-context watermark propagation
