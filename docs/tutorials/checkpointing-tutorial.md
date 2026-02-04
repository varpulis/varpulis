# Checkpointing and Watermarks

This tutorial teaches you to use Varpulis checkpointing for state persistence and watermarks for out-of-order event handling. You'll start with basic state recovery and build up to watermark-driven window closure -- each step includes runnable VPL programs you can validate and test.

## What You'll Learn

- How engine state (windows, variables, counters) is checkpointed and restored
- Count window and session window state continuity across restarts
- The checkpoint/restore lifecycle: process, snapshot, kill, recover
- Checkpoint pruning and retention policies
- Watermark tracking with `.watermark(out_of_order: ...)` syntax
- Late-data tolerance with `.allowed_lateness(...)` syntax
- Watermark-triggered window closure

## Prerequisites

- Varpulis built and on your `PATH` (see [Getting Started](getting-started.md))
- Basic VPL knowledge: streams, `where`, `window`, `aggregate` (see [Language Tutorial](language-tutorial.md))
- Familiarity with contexts is helpful but not required (see [Contexts Tutorial](contexts-tutorial.md))

---

## Part 1: Count Window State Recovery

The simplest checkpoint scenario: a count window that accumulates events. If the engine restarts mid-window, the events already received must survive so the window completes correctly.

### The Program

Create a file called `checkpoint_count_window.vpl`:

```vpl
# Checkpoint Test: Count Window with Aggregation
# A window of 5 events collects sensor values and computes sum + count.

stream WindowedSum = SensorEvent
    .window(5)
    .aggregate(total: sum(value), count: count())
    .emit(sum: total, n: count)
```

This stream collects 5 `SensorEvent` events, then emits the sum and count.

### Test Data

Create `checkpoint_count_window_phase1.evt` (events before the restart):

```
# Phase 1: First 3 events (checkpoint after these)
SensorEvent { value: 10 }
SensorEvent { value: 20 }
SensorEvent { value: 30 }
```

Create `checkpoint_count_window_phase2.evt` (events after the restart):

```
# Phase 2: 2 more events after restore (fills window: 3 restored + 2 new = 5)
# Expected output: sum = 10+20+30+40+50 = 150, count = 5
SensorEvent { value: 40 }
SensorEvent { value: 50 }
```

### Validate It

```bash
$ varpulis check checkpoint_count_window.vpl
Syntax OK
   Statements: 1
```

### Run Phase 1

```bash
$ varpulis simulate -p checkpoint_count_window.vpl -e checkpoint_count_window_phase1.evt --verbose
```

**Expected output:**

```
Program loaded: 1 streams

Starting simulation...

  [  1] @     0ms SensorEvent { ... }
  [  2] @     0ms SensorEvent { ... }
  [  3] @     0ms SensorEvent { ... }

Simulation Complete
======================
Events processed: 3
Output events emitted: 0
```

No output events -- the window needs 5 events and only 3 arrived. In a real system, the engine would checkpoint here. After restart, those 3 events would be restored from the checkpoint, and processing continues.

### Run Phase 2

```bash
$ varpulis simulate -p checkpoint_count_window.vpl -e checkpoint_count_window_phase2.evt --verbose
```

In simulation mode, each run starts fresh (no checkpoint persistence). The checkpoint/restore lifecycle is exercised through the Rust integration tests, which programmatically:

1. Load the VPL from the same file
2. Process phase 1 events
3. Call `engine.create_checkpoint()`
4. Drop the engine (simulating a kill)
5. Create a new engine, load the same VPL
6. Call `engine.restore_checkpoint()`
7. Process phase 2 events
8. Assert the window emits with sum=150, count=5

**Key takeaway:** The 3 events from phase 1 survive the restart via checkpointing. When the 2 new events arrive in phase 2, the window fills to 5 and emits the correct aggregation.

---

## Part 2: Session Window Recovery

Session windows group events by inactivity gaps. Checkpointing a session window means preserving all events in the current open session. After restart, the session continues as if nothing happened.

### The Program

Create a file called `checkpoint_session_window.vpl`:

```vpl
# Session Window with 5s gap. Events within a session are preserved across restart.

stream SessionAgg = SensorEvent
    .window(session: 5s)
    .aggregate(count: count(), total: sum(value))
    .emit(n: count, sum: total)
```

### Test Data

Create `checkpoint_session_window_phase1.evt` (events within the session):

```
# Phase 1: 3 events within a session (1s apart, well within 5s gap)
@0s SensorEvent { value: 100 }
@1s SensorEvent { value: 100 }
@2s SensorEvent { value: 100 }
```

Create `checkpoint_session_window_phase2.evt` (event that closes the session):

```
# Phase 2: Event after 6s gap (exceeds 5s session timeout)
# This closes the restored session. Expected output: n = 3, sum = 300
@9s SensorEvent { value: 999 }
```

### Run It

```bash
$ varpulis simulate -p checkpoint_session_window.vpl -e checkpoint_session_window_phase1.evt --verbose
```

**Expected output:**

```
Program loaded: 1 streams

Starting simulation...

  [  1] @     0ms SensorEvent { ... }
  [  2] @  1001ms SensorEvent { ... }
  [  3] @  2003ms SensorEvent { ... }

Simulation Complete
======================
Events processed: 3
Output events emitted: 0
```

The session is still open (no gap exceeding 5s). In a checkpoint/restore cycle:

1. Phase 1 events establish a session with 3 events (values: 100, 100, 100)
2. Engine checkpoints and is killed
3. New engine restores the session state
4. Phase 2 event arrives at `@9s` -- 7 seconds after the last phase 1 event at `@2s`
5. The 7-second gap exceeds the 5-second session timeout, closing the restored session
6. Output: `n = 3, sum = 300` (only the 3 restored events)

**Key takeaway:** Session windows preserve their event buffer across checkpoints. The gap-detection logic works correctly on the restored state.

---

## Part 3: Metrics and Variable Preservation

Beyond window state, checkpoints preserve:

- **Engine metrics**: `events_processed`, `output_events_emitted` counters
- **Variables**: Declared `var` values survive restart

### Passthrough with Metrics

Create `checkpoint_passthrough.vpl`:

```vpl
# Simple passthrough for metrics verification.
# Every event produces an output event.

stream PassThrough = TestEvent
    .emit(value: value)
```

Create `checkpoint_passthrough.evt`:

```
# 5 events for metrics tracking
TestEvent { value: 1 }
TestEvent { value: 2 }
TestEvent { value: 3 }
TestEvent { value: 4 }
TestEvent { value: 5 }
```

```bash
$ varpulis simulate -p checkpoint_passthrough.vpl -e checkpoint_passthrough.evt --verbose
```

**Expected output:**

```
Program loaded: 1 streams

Starting simulation...

  [  1-5] 5 events processed
OUTPUT EVENT: PassThrough - {"value": Int(1)}
OUTPUT EVENT: PassThrough - {"value": Int(2)}
OUTPUT EVENT: PassThrough - {"value": Int(3)}
OUTPUT EVENT: PassThrough - {"value": Int(4)}
OUTPUT EVENT: PassThrough - {"value": Int(5)}

Simulation Complete
======================
Events processed: 5
Output events emitted: 5
```

After checkpointing and restoring, the engine metrics (`events_processed = 5`, `output_events_emitted = 5`) continue from where they left off rather than resetting to 0.

### Variables

Create `checkpoint_variables.vpl`:

```vpl
# Variable preservation test.

var counter: int = 0

stream Incrementer = TestEvent
    .emit(v: value)
```

```bash
$ varpulis check checkpoint_variables.vpl
Syntax OK
   Statements: 2
```

The `counter` variable is checkpointed as part of the engine state. After restore, `engine.get_variable("counter")` returns the saved value.

**Key takeaway:** Checkpoints capture the complete engine state: windows, variables, and processing counters.

---

## Part 4: Checkpoint Pruning

In production, you don't want unlimited checkpoint accumulation. The `CheckpointConfig` controls retention:

```rust
CheckpointConfig {
    interval: Duration::from_secs(60),   // Checkpoint every 60 seconds
    max_checkpoints: 3,                  // Keep only the last 3
    checkpoint_on_shutdown: true,        // Save on graceful shutdown
    key_prefix: "varpulis".to_string(),  // Storage key prefix
}
```

With `max_checkpoints: 2`, after 4 checkpoint cycles, only the 2 most recent remain. Recovery always loads the latest valid checkpoint.

### Storage Backends

| Backend | When to Use |
|---------|-------------|
| `MemoryStore` | Development and testing (lost on restart) |
| `FileStore` | Production with `--state-dir` (atomic writes, survives restart) |
| `RocksDbStore` | High-throughput production (requires `persistence` feature) |

```bash
# Enable file-based persistence
varpulis server --port 9000 --api-key "key" --state-dir /var/lib/varpulis/state

# Build with RocksDB support
cargo build --release --features persistence
```

**Key takeaway:** Configure `max_checkpoints` to bound storage usage. The latest checkpoint always wins on recovery.

---

## Part 5: Watermark Tracking

Watermarks solve the out-of-order event problem. In distributed systems, events often arrive out of timestamp order. Without watermarks, a window might close before all its events arrive.

The `.watermark(out_of_order: ...)` syntax tells the engine how much lateness to expect:

### The Program

Create `watermark_basic.vpl`:

```vpl
# Per-source watermark tracking with 5s out-of-order tolerance.

stream Watermarked = SensorEvent
    .watermark(out_of_order: 5s)
    .emit(value: value)
```

Create `watermark_basic.evt`:

```
# 5 events with increasing timestamps
@0s SensorEvent { value: 0 }
@1s SensorEvent { value: 1 }
@2s SensorEvent { value: 2 }
@3s SensorEvent { value: 3 }
@4s SensorEvent { value: 4 }
```

### Run It

```bash
$ varpulis simulate -p watermark_basic.vpl -e watermark_basic.evt --verbose
```

**Expected output:**

```
Program loaded: 1 streams

Starting simulation...

  [  1] @     0ms SensorEvent { ... }
OUTPUT EVENT: Watermarked - {"value": Int(0)}
  [  2] @  1001ms SensorEvent { ... }
OUTPUT EVENT: Watermarked - {"value": Int(1)}
  ...
  [  5] @  4001ms SensorEvent { ... }
OUTPUT EVENT: Watermarked - {"value": Int(4)}

Simulation Complete
======================
Events processed: 5
Output events emitted: 5
```

The engine tracks the maximum observed timestamp per source and computes the watermark as `max_timestamp - out_of_order`. Events arriving before the watermark are considered late.

**Key takeaway:** `.watermark(out_of_order: 5s)` tells the engine to expect events up to 5 seconds late. The watermark advances as new events arrive.

---

## Part 6: Watermark-Triggered Window Closure

The real power of watermarks is triggering window closure. Instead of relying solely on wall-clock time, windows close when the watermark passes the window boundary.

### The Program

Create `watermark_windowed.vpl`:

```vpl
# Watermark advancement triggers window closure for out-of-order data.

stream Windowed = SensorEvent
    .watermark(out_of_order: 2s)
    .window(5s)
    .aggregate(total: count())
    .emit(event_count: total)
```

Create `watermark_windowed.evt`:

```
# Events within first window, then one past boundary to trigger watermark advance
@0s SensorEvent { value: 0 }
@1s SensorEvent { value: 1 }
@2s SensorEvent { value: 2 }
@8s SensorEvent { value: 99 }
```

### Run It

```bash
$ varpulis simulate -p watermark_windowed.vpl -e watermark_windowed.evt --verbose
```

The first 3 events land in the initial 5-second window. The event at `@8s` advances the watermark to `8s - 2s = 6s`, which is past the first window's boundary (5s). This triggers the window to close and emit.

**Key takeaway:** Watermarks decouple window closure from wall-clock time. A late event that pushes the watermark past a window boundary triggers closure -- even if the wall clock hasn't reached that point yet.

---

## Part 7: Late-Data Tolerance

Sometimes events arrive even later than the watermark allows. The `.allowed_lateness()` syntax gives a grace period:

### The Program

Create `watermark_lateness.vpl`:

```vpl
# Combines watermark tracking and late-data tolerance.

stream Combined = OrderEvent
    .watermark(out_of_order: 10s)
    .allowed_lateness(30s)
    .window(1m)
    .aggregate(total: sum(amount))
    .emit(total_amount: total)
```

### Validate It

```bash
$ varpulis check watermark_lateness.vpl
Syntax OK
   Statements: 1
```

How the tolerance works:

1. **Watermark**: Events up to 10s behind the max timestamp are considered "on time"
2. **Allowed lateness**: Events up to 30s behind the watermark are still processed (added to the appropriate window)
3. **Beyond**: Events arriving more than 30s past the watermark are dropped (or routed to a side-output stream if configured)

```
Timeline:
  |-- on-time events --|-- allowed lateness --|-- dropped --|
  watermark            watermark + 30s
```

**Key takeaway:** `.allowed_lateness()` provides a grace period beyond the watermark for straggler events.

---

## Part 8: Watermark State Checkpointing

Watermark tracking state is included in engine checkpoints. This means after a restart:

- The per-source watermark positions are restored
- The effective watermark (minimum across all sources) is recalculated
- Late-data detection resumes from the correct position

This is exercised by the `test_kill_restart_watermark_state_preserved` integration test, which:

1. Loads `watermark_basic.vpl`
2. Processes 5 events with increasing timestamps
3. Checkpoints (verifies `watermark_state.is_some()`)
4. Persists to a `MemoryStore`
5. Creates a new engine, restores from the store
6. Verifies the watermark state survived the round-trip
7. Continues processing without errors

---

## Quick Reference

| Syntax | Description |
|--------|-------------|
| `.watermark(out_of_order: 5s)` | Enable watermark tracking with 5s tolerance |
| `.allowed_lateness(30s)` | Accept events up to 30s past watermark |
| `.window(5)` | Count window (5 events) |
| `.window(1m)` | Tumbling time window (1 minute) |
| `.window(session: 5s)` | Session window with 5s inactivity gap |
| `engine.create_checkpoint()` | Snapshot engine state |
| `engine.restore_checkpoint()` | Restore engine state from snapshot |
| `CheckpointManager` | Periodic checkpoint with pruning |
| `--state-dir /path` | Enable file-based persistence (server mode) |

## Storage Backends

| Backend | Configuration | Use Case |
|---------|---------------|----------|
| `MemoryStore` | Default (no config) | Testing |
| `FileStore` | `--state-dir /path` | Production (atomic writes) |
| `RocksDbStore` | `--features persistence` | High-throughput production |

---

## VPL Files

All VPL and event files used in this tutorial are available in `tests/scenarios/`:

| File | Purpose |
|------|---------|
| `checkpoint_count_window.vpl` | Count window checkpoint/restore |
| `checkpoint_count_window_phase1.evt` | Pre-restart events (3 of 5) |
| `checkpoint_count_window_phase2.evt` | Post-restart events (2 of 5) |
| `checkpoint_session_window.vpl` | Session window checkpoint/restore |
| `checkpoint_session_window_phase1.evt` | In-session events |
| `checkpoint_session_window_phase2.evt` | Session-closing event |
| `checkpoint_passthrough.vpl` | Metrics verification |
| `checkpoint_passthrough.evt` | 5 passthrough events |
| `checkpoint_variables.vpl` | Variable preservation |
| `checkpoint_serialization.vpl` | Serialization round-trip |
| `watermark_basic.vpl` | Basic watermark tracking |
| `watermark_basic.evt` | Timestamped sensor events |
| `watermark_windowed.vpl` | Watermark + tumbling window |
| `watermark_windowed.evt` | Events triggering watermark advance |
| `watermark_lateness.vpl` | Watermark + allowed lateness |

---

## Next Steps

- [State Management Architecture](../architecture/state-management.md) -- Storage backends, checkpoint internals
- [Performance Tuning](../guides/performance-tuning.md) -- Checkpoint interval tuning, memory management
- [Contexts Tutorial](contexts-tutorial.md) -- Multi-threaded processing with checkpoint coordination
