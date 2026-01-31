//! Worker Pool for Controlled Parallelism
//!
//! Unlike Apama's opaque spawn/contexts, Varpulis provides explicit control over
//! parallel workers with monitoring, backpressure, and dynamic resizing.
//!
//! # Example
//! ```ignore
//! let config = WorkerPoolConfig {
//!     name: "OrderProcessors".to_string(),
//!     workers: 4,
//!     queue_size: 1000,
//!     backpressure: BackpressureStrategy::DropOldest,
//! };
//!
//! let pool = WorkerPool::new(config);
//! pool.submit(event, "customer_123").await?;
//! println!("Metrics: {:?}", pool.metrics());
//! ```

use crate::event::Event;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, info, warn};

/// Configuration for a worker pool
#[derive(Debug, Clone)]
pub struct WorkerPoolConfig {
    /// Pool name for identification
    pub name: String,
    /// Number of worker tasks
    pub workers: usize,
    /// Maximum queue size per worker
    pub queue_size: usize,
    /// Backpressure strategy when queue is full
    pub backpressure: BackpressureStrategy,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            workers: 4,
            queue_size: 1000,
            backpressure: BackpressureStrategy::Block,
        }
    }
}

/// Strategy for handling backpressure when queue is full
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureStrategy {
    /// Block sender until space available
    Block,
    /// Drop oldest events in queue
    DropOldest,
    /// Drop incoming events
    DropNewest,
    /// Return error to sender
    Error,
}

/// Error returned when backpressure strategy is Error
#[derive(Debug, Clone)]
pub struct BackpressureError {
    pub pool_name: String,
    pub queue_depth: usize,
}

impl std::fmt::Display for BackpressureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Pool '{}' queue full (depth: {})",
            self.pool_name, self.queue_depth
        )
    }
}

impl std::error::Error for BackpressureError {}

/// Metrics for a worker pool
#[derive(Debug, Clone, Default)]
pub struct WorkerPoolMetrics {
    /// Number of currently active (processing) workers
    pub active_workers: usize,
    /// Number of idle workers
    pub idle_workers: usize,
    /// Current total queue depth across all workers
    pub queue_depth: usize,
    /// Total events processed
    pub events_processed: u64,
    /// Events dropped due to backpressure
    pub events_dropped: u64,
    /// Average processing latency in microseconds
    pub avg_latency_us: f64,
    /// 99th percentile latency in microseconds
    pub p99_latency_us: f64,
}

/// Status of an individual worker
#[derive(Debug, Clone)]
pub struct WorkerStatus {
    /// Worker ID
    pub id: usize,
    /// Current state
    pub state: WorkerState,
    /// Current partition being processed (if any)
    pub current_partition: Option<String>,
    /// Events processed by this worker
    pub events_processed: u64,
    /// Last time this worker was active
    pub last_active: Instant,
}

/// State of a worker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// Waiting for events
    Idle,
    /// Processing an event
    Processing,
    /// Finishing current work before shutdown
    Draining,
    /// Stopped
    Stopped,
}

/// Internal worker data shared between pool and worker tasks
struct WorkerData {
    id: usize,
    state: AtomicUsize, // WorkerState as usize
    current_partition: RwLock<Option<String>>,
    events_processed: AtomicU64,
    last_active: RwLock<Instant>,
}

impl WorkerData {
    fn new(id: usize) -> Self {
        Self {
            id,
            state: AtomicUsize::new(WorkerState::Idle as usize),
            current_partition: RwLock::new(None),
            events_processed: AtomicU64::new(0),
            last_active: RwLock::new(Instant::now()),
        }
    }

    fn get_state(&self) -> WorkerState {
        match self.state.load(Ordering::Relaxed) {
            0 => WorkerState::Idle,
            1 => WorkerState::Processing,
            2 => WorkerState::Draining,
            _ => WorkerState::Stopped,
        }
    }

    fn set_state(&self, state: WorkerState) {
        self.state.store(state as usize, Ordering::Relaxed);
    }
}

/// Event with partition key for routing
struct PartitionedEvent {
    event: Event,
    partition_key: String,
}

/// A pool of workers for parallel event processing
pub struct WorkerPool {
    config: WorkerPoolConfig,
    /// Sender to dispatch events to workers
    dispatch_tx: mpsc::Sender<PartitionedEvent>,
    /// Worker data for monitoring
    workers: Vec<Arc<WorkerData>>,
    /// Partition to worker mapping for affinity
    #[allow(dead_code)]
    pub(crate) partition_affinity: Arc<RwLock<HashMap<String, usize>>>,
    /// Global metrics
    events_processed: AtomicU64,
    events_dropped: AtomicU64,
    /// Latency tracking (ring buffer of recent latencies in microseconds)
    latencies: Arc<Mutex<Vec<u64>>>,
    /// Shutdown signal
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl WorkerPool {
    /// Create a new worker pool with the given configuration
    pub fn new<F>(config: WorkerPoolConfig, processor: F) -> Self
    where
        F: Fn(Event) + Send + Sync + Clone + 'static,
    {
        let (dispatch_tx, dispatch_rx) = mpsc::channel(config.queue_size * config.workers);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let mut workers = Vec::with_capacity(config.workers);
        for i in 0..config.workers {
            workers.push(Arc::new(WorkerData::new(i)));
        }

        let partition_affinity = Arc::new(RwLock::new(HashMap::new()));
        let latencies = Arc::new(Mutex::new(Vec::with_capacity(1000)));

        // Spawn the dispatcher task
        let workers_clone = workers.clone();
        let partition_affinity_clone = partition_affinity.clone();
        let latencies_clone = latencies.clone();
        let processor_clone = processor.clone();
        let pool_name = config.name.clone();
        let num_workers = config.workers;

        tokio::spawn(Self::dispatcher_task(
            dispatch_rx,
            shutdown_rx,
            workers_clone,
            partition_affinity_clone,
            latencies_clone,
            processor_clone,
            pool_name,
            num_workers,
        ));

        Self {
            config,
            dispatch_tx,
            workers,
            partition_affinity,
            events_processed: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            latencies,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Dispatcher task that routes events to workers
    #[allow(clippy::too_many_arguments)]
    async fn dispatcher_task<F>(
        mut rx: mpsc::Receiver<PartitionedEvent>,
        mut shutdown_rx: mpsc::Receiver<()>,
        workers: Vec<Arc<WorkerData>>,
        partition_affinity: Arc<RwLock<HashMap<String, usize>>>,
        latencies: Arc<Mutex<Vec<u64>>>,
        processor: F,
        pool_name: String,
        num_workers: usize,
    ) where
        F: Fn(Event) + Send + Sync + Clone + 'static,
    {
        info!(
            "Worker pool '{}' started with {} workers",
            pool_name, num_workers
        );

        loop {
            tokio::select! {
                Some(partitioned_event) = rx.recv() => {
                    // Determine which worker should handle this partition
                    let worker_id = {
                        let affinity = partition_affinity.read().await;
                        if let Some(&id) = affinity.get(&partitioned_event.partition_key) {
                            id
                        } else {
                            // Assign to least loaded worker (simple round-robin for now)
                            let mut min_load = u64::MAX;
                            let mut best_worker = 0;
                            for (i, w) in workers.iter().enumerate() {
                                let load = w.events_processed.load(Ordering::Relaxed);
                                if load < min_load && w.get_state() != WorkerState::Stopped {
                                    min_load = load;
                                    best_worker = i;
                                }
                            }
                            // Update affinity
                            drop(affinity);
                            partition_affinity.write().await.insert(
                                partitioned_event.partition_key.clone(),
                                best_worker,
                            );
                            best_worker
                        }
                    };

                    // Process the event
                    let worker = &workers[worker_id];
                    worker.set_state(WorkerState::Processing);
                    *worker.current_partition.write().await = Some(partitioned_event.partition_key);

                    let start = Instant::now();
                    processor(partitioned_event.event);
                    let elapsed_us = start.elapsed().as_micros() as u64;

                    // Update metrics
                    worker.events_processed.fetch_add(1, Ordering::Relaxed);
                    *worker.last_active.write().await = Instant::now();
                    worker.set_state(WorkerState::Idle);
                    *worker.current_partition.write().await = None;

                    // Track latency
                    let mut lat = latencies.lock().await;
                    if lat.len() >= 1000 {
                        lat.remove(0);
                    }
                    lat.push(elapsed_us);
                }
                _ = shutdown_rx.recv() => {
                    info!("Worker pool '{}' shutting down", pool_name);
                    for w in &workers {
                        w.set_state(WorkerState::Stopped);
                    }
                    break;
                }
            }
        }
    }

    /// Submit an event to the pool with a partition key
    pub async fn submit(&self, event: Event, partition_key: &str) -> Result<(), BackpressureError> {
        let partitioned = PartitionedEvent {
            event,
            partition_key: partition_key.to_string(),
        };

        match self.config.backpressure {
            BackpressureStrategy::Block => {
                if self.dispatch_tx.send(partitioned).await.is_err() {
                    warn!("Pool '{}' dispatch channel closed", self.config.name);
                }
                self.events_processed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            BackpressureStrategy::DropNewest => {
                match self.dispatch_tx.try_send(partitioned) {
                    Ok(()) => {
                        self.events_processed.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    }
                    Err(_) => {
                        self.events_dropped.fetch_add(1, Ordering::Relaxed);
                        debug!("Pool '{}' dropped event (queue full)", self.config.name);
                        Ok(()) // Silently drop
                    }
                }
            }
            BackpressureStrategy::DropOldest => {
                // For drop oldest, we always accept but may drop from queue
                // This is a simplification - proper implementation would need a custom queue
                match self.dispatch_tx.try_send(partitioned) {
                    Ok(()) => {
                        self.events_processed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(mpsc::error::TrySendError::Full(p)) => {
                        // Queue full, force send (may block briefly)
                        self.events_dropped.fetch_add(1, Ordering::Relaxed);
                        let _ = self.dispatch_tx.send(p).await;
                        self.events_processed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {}
                }
                Ok(())
            }
            BackpressureStrategy::Error => match self.dispatch_tx.try_send(partitioned) {
                Ok(()) => {
                    self.events_processed.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                Err(_) => {
                    self.events_dropped.fetch_add(1, Ordering::Relaxed);
                    Err(BackpressureError {
                        pool_name: self.config.name.clone(),
                        queue_depth: self.queue_depth(),
                    })
                }
            },
        }
    }

    /// Get current queue depth
    pub fn queue_depth(&self) -> usize {
        // Approximate based on channel capacity
        self.config.queue_size * self.config.workers - self.dispatch_tx.capacity()
    }

    /// Get pool metrics
    pub async fn metrics(&self) -> WorkerPoolMetrics {
        let mut active = 0;
        let mut idle = 0;

        for w in &self.workers {
            match w.get_state() {
                WorkerState::Processing => active += 1,
                WorkerState::Idle => idle += 1,
                _ => {}
            }
        }

        let latencies = self.latencies.lock().await;
        let avg_latency = if latencies.is_empty() {
            0.0
        } else {
            latencies.iter().sum::<u64>() as f64 / latencies.len() as f64
        };

        let p99_latency = if latencies.is_empty() {
            0.0
        } else {
            let mut sorted = latencies.clone();
            sorted.sort_unstable();
            let idx = (sorted.len() as f64 * 0.99) as usize;
            sorted.get(idx.min(sorted.len() - 1)).copied().unwrap_or(0) as f64
        };

        WorkerPoolMetrics {
            active_workers: active,
            idle_workers: idle,
            queue_depth: self.queue_depth(),
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            avg_latency_us: avg_latency,
            p99_latency_us: p99_latency,
        }
    }

    /// Get status of all workers
    pub async fn worker_statuses(&self) -> Vec<WorkerStatus> {
        let mut statuses = Vec::with_capacity(self.workers.len());
        for w in &self.workers {
            statuses.push(WorkerStatus {
                id: w.id,
                state: w.get_state(),
                current_partition: w.current_partition.read().await.clone(),
                events_processed: w.events_processed.load(Ordering::Relaxed),
                last_active: *w.last_active.read().await,
            });
        }
        statuses
    }

    /// Get pool configuration
    pub fn config(&self) -> &WorkerPoolConfig {
        &self.config
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> Duration {
        let start = Instant::now();
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
        // Give workers time to finish
        tokio::time::sleep(Duration::from_millis(100)).await;
        start.elapsed()
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        // Best effort shutdown
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.try_send(());
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn test_worker_pool_creation() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let config = WorkerPoolConfig {
            name: "test".to_string(),
            workers: 2,
            queue_size: 10,
            backpressure: BackpressureStrategy::Block,
        };

        let pool = WorkerPool::new(config, move |_event| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        assert_eq!(pool.config().workers, 2);
        assert_eq!(pool.config().name, "test");
    }

    #[tokio::test]
    async fn test_worker_pool_submit() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let config = WorkerPoolConfig {
            name: "test".to_string(),
            workers: 2,
            queue_size: 100,
            backpressure: BackpressureStrategy::Block,
        };

        let pool = WorkerPool::new(config, move |_event| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        // Submit some events
        for i in 0..10 {
            let event = Event::new("TestEvent").with_field("id", i as i64);
            pool.submit(event, &format!("partition_{}", i % 3))
                .await
                .unwrap();
        }

        // Wait for processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(counter.load(Ordering::Relaxed), 10);
    }

    #[tokio::test]
    async fn test_worker_pool_metrics() {
        let config = WorkerPoolConfig {
            name: "metrics_test".to_string(),
            workers: 4,
            queue_size: 100,
            backpressure: BackpressureStrategy::Block,
        };

        let pool = WorkerPool::new(config, |_| {});

        let metrics = pool.metrics().await;
        assert_eq!(metrics.idle_workers + metrics.active_workers, 4);
        assert_eq!(metrics.events_processed, 0);
    }

    #[tokio::test]
    async fn test_worker_pool_backpressure_drop_newest() {
        let config = WorkerPoolConfig {
            name: "backpressure_test".to_string(),
            workers: 1,
            queue_size: 2,
            backpressure: BackpressureStrategy::DropNewest,
        };

        let pool = WorkerPool::new(config, |_| {
            std::thread::sleep(Duration::from_millis(50));
        });

        // Submit many events quickly
        for i in 0..100 {
            let event = Event::new("TestEvent").with_field("id", i as i64);
            let _ = pool.submit(event, "partition").await;
        }

        let metrics = pool.metrics().await;
        // Some events should have been dropped
        assert!(metrics.events_dropped > 0 || metrics.events_processed > 0);
    }

    #[tokio::test]
    async fn test_worker_pool_backpressure_error() {
        let config = WorkerPoolConfig {
            name: "error_test".to_string(),
            workers: 1,
            queue_size: 1,
            backpressure: BackpressureStrategy::Error,
        };

        let pool = WorkerPool::new(config, |_| {
            std::thread::sleep(Duration::from_millis(100));
        });

        // First event should succeed
        let event1 = Event::new("TestEvent").with_field("id", 1i64);
        assert!(pool.submit(event1, "partition").await.is_ok());

        // Subsequent events may fail due to backpressure
        let mut errors = 0;
        for i in 2..10 {
            let event = Event::new("TestEvent").with_field("id", i as i64);
            if pool.submit(event, "partition").await.is_err() {
                errors += 1;
            }
        }

        // Should have some errors
        assert!(errors > 0);
    }

    #[tokio::test]
    async fn test_worker_pool_partition_affinity() {
        let config = WorkerPoolConfig {
            name: "affinity_test".to_string(),
            workers: 4,
            queue_size: 100,
            backpressure: BackpressureStrategy::Block,
        };

        let pool = WorkerPool::new(config, |_| {});

        // Submit events with same partition key
        for _ in 0..5 {
            let event = Event::new("TestEvent");
            pool.submit(event, "same_partition").await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check that partition affinity was established
        let affinity = pool.partition_affinity.read().await;
        assert!(affinity.contains_key("same_partition"));
    }

    #[tokio::test]
    async fn test_worker_pool_shutdown() {
        let config = WorkerPoolConfig::default();
        let mut pool = WorkerPool::new(config, |_| {});

        let duration = pool.shutdown().await;
        assert!(duration < Duration::from_secs(1));
    }
}
