# Hot Reload & Controlled Parallelism Design

## Overview

Two features to achieve full parity with Apama while improving on its limitations:

1. **HOT-01: Hot Reload** - Update VPL programs without restart
2. **PAR-01: Controlled Parallelism** - Dynamic parallelism with explicit control

---

## HOT-01: Hot Reload

### Problem with Apama
Apama supports hot redeploy but requires complex deployment procedures.

### Varpulis Approach
Simple, developer-friendly hot reload:

```
┌─────────────────────────────────────────────────────┐
│                    Engine                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │  Streams    │  │   State     │  │  Watchers   │  │
│  │  (mutable)  │  │ (preserved) │  │  (notify)   │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  │
│         ▲                                ▲          │
│         │         reload()               │          │
│         └────────────────────────────────┘          │
└─────────────────────────────────────────────────────┘
```

### API Design

```rust
impl Engine {
    /// Reload program from file, preserving compatible state
    pub async fn reload(&mut self, program: &Program) -> Result<ReloadReport, ReloadError>;

    /// Watch file for changes and auto-reload
    pub fn watch_file(&mut self, path: &Path) -> Result<(), WatchError>;

    /// Get reload status
    pub fn reload_status(&self) -> ReloadStatus;
}

#[derive(Debug)]
pub struct ReloadReport {
    pub streams_added: Vec<String>,
    pub streams_removed: Vec<String>,
    pub streams_updated: Vec<String>,
    pub state_preserved: Vec<String>,  // Streams with preserved window/aggregator state
    pub state_reset: Vec<String>,      // Streams that had to be reset
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub enum ReloadStatus {
    Ready,
    Reloading { started_at: Instant },
    Failed { error: String, last_success: Instant },
}
```

### State Preservation Rules

| Change Type | State Preserved? |
|-------------|------------------|
| Add new stream | N/A (new) |
| Remove stream | N/A (dropped) |
| Modify filter expression | ✅ Yes |
| Change window size | ❌ No (reset) |
| Change aggregation fields | ❌ No (reset) |
| Add/remove operations | ❌ No (reset) |
| Change event type | ❌ No (reset) |

### CLI Integration

```bash
# Watch mode - auto-reload on file change
varpulis serve --program rules.vpl --watch

# Manual reload via WebSocket
ws://localhost:9000/reload

# Reload via signal
kill -HUP $(pidof varpulis)
```

---

## PAR-01: Controlled Parallelism

### Problem with Apama
Apama's `spawn` and contexts are opaque:
- No visibility into running contexts
- No control over resource usage
- Hard to debug parallel issues
- No backpressure mechanism

### Varpulis Approach: Worker Pools with Explicit Control

```
┌─────────────────────────────────────────────────────────────┐
│                     WorkerPool                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                    Controller                         │   │
│  │  - pool_size: 4                                      │   │
│  │  - active_workers: 3                                 │   │
│  │  - queue_depth: 12                                   │   │
│  │  - backpressure: false                               │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │Worker 0 │ │Worker 1 │ │Worker 2 │ │Worker 3 │           │
│  │ BUSY    │ │ BUSY    │ │ BUSY    │ │ IDLE    │           │
│  │ part=A  │ │ part=B  │ │ part=C  │ │         │           │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### VPL Syntax for Parallelism

```vpl
# Define a worker pool
pool OrderProcessors:
    workers: 4              # Fixed pool size
    queue_size: 1000        # Max pending events
    backpressure: drop_oldest  # or: block, drop_newest, error

# Stream runs in parallel pool, partitioned by customer_id
stream OrderAlerts = Order
    .parallel(pool: OrderProcessors, partition_by: customer_id)
    .where(amount > 10000)
    .emit(event_type: "HighValueOrder")

# Monitor pool status
stream PoolMetrics = timer(5s)
    .select(
        pool_name: "OrderProcessors",
        active: pool_active("OrderProcessors"),
        queue_depth: pool_queue("OrderProcessors"),
        processed: pool_processed("OrderProcessors")
    )
```

### API Design

```rust
/// Worker pool configuration
#[derive(Debug, Clone)]
pub struct WorkerPoolConfig {
    pub name: String,
    pub workers: usize,
    pub queue_size: usize,
    pub backpressure: BackpressureStrategy,
}

#[derive(Debug, Clone, Copy)]
pub enum BackpressureStrategy {
    Block,        // Block sender until space available
    DropOldest,   // Drop oldest events in queue
    DropNewest,   // Drop incoming events
    Error,        // Return error to sender
}

/// Worker pool handle for monitoring and control
pub struct WorkerPool {
    config: WorkerPoolConfig,
    workers: Vec<Worker>,
    metrics: WorkerPoolMetrics,
}

impl WorkerPool {
    pub fn new(config: WorkerPoolConfig) -> Self;

    /// Submit event to pool (partitioned by key)
    pub async fn submit(&self, event: Event, partition_key: &str) -> Result<(), BackpressureError>;

    /// Get current metrics
    pub fn metrics(&self) -> WorkerPoolMetrics;

    /// Resize pool dynamically
    pub async fn resize(&mut self, new_size: usize) -> Result<(), ResizeError>;

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> ShutdownReport;
}

#[derive(Debug, Clone)]
pub struct WorkerPoolMetrics {
    pub active_workers: usize,
    pub idle_workers: usize,
    pub queue_depth: usize,
    pub events_processed: u64,
    pub events_dropped: u64,
    pub avg_latency_us: f64,
    pub p99_latency_us: f64,
}

/// Individual worker status
#[derive(Debug)]
pub struct WorkerStatus {
    pub id: usize,
    pub state: WorkerState,
    pub current_partition: Option<String>,
    pub events_processed: u64,
    pub last_active: Instant,
}

#[derive(Debug, Clone, Copy)]
pub enum WorkerState {
    Idle,
    Processing,
    Draining,  // Finishing current work before shutdown
    Stopped,
}
```

### Engine Integration

```rust
impl Engine {
    /// Register a worker pool
    pub fn register_pool(&mut self, config: WorkerPoolConfig) -> Result<(), PoolError>;

    /// Get pool by name
    pub fn get_pool(&self, name: &str) -> Option<&WorkerPool>;

    /// Get all pool metrics
    pub fn pool_metrics(&self) -> HashMap<String, WorkerPoolMetrics>;
}
```

### CLI Integration

```bash
# Start with parallelism enabled
varpulis serve --program rules.vpl --pools

# Show pool status
varpulis pools status

# Resize pool dynamically
varpulis pools resize OrderProcessors 8

# WebSocket API for monitoring
ws://localhost:9000/pools
```

---

## Implementation Plan

### Phase 1: Hot Reload (HOT-01)
1. Add `reload()` method to Engine
2. Implement state preservation logic
3. Add file watcher with `notify` crate
4. Add WebSocket reload endpoint
5. Add SIGHUP handler
6. Tests

### Phase 2: Controlled Parallelism (PAR-01)
1. Create `worker_pool.rs` module
2. Implement `WorkerPool` and `Worker` structs
3. Add VPL syntax for `pool` and `.parallel()`
4. Integrate with Engine
5. Add metrics and monitoring
6. Tests

---

## Comparison with Apama

| Feature | Apama | Varpulis |
|---------|-------|----------|
| Hot reload | ✅ Complex deployment | ✅ Simple file watch |
| Parallelism | spawn/contexts (opaque) | Worker pools (explicit) |
| Pool sizing | Automatic | Explicit + dynamic resize |
| Monitoring | Limited | Full metrics per worker |
| Backpressure | None | Configurable strategy |
| Partition control | Implicit | Explicit partition_by |
| Resource limits | None | queue_size, workers |
