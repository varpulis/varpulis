//! Heartbeat protocol and failure detection.

use crate::worker::{WorkerId, WorkerStatus};
use std::time::Duration;
use tracing::warn;

/// Default heartbeat interval (workers send heartbeats this often).
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Default timeout before marking a worker as unhealthy.
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);

/// Result of a health sweep across all workers.
#[derive(Debug, Default)]
pub struct HealthSweepResult {
    pub workers_checked: usize,
    pub workers_marked_unhealthy: Vec<WorkerId>,
}

/// Perform a health sweep: check each worker's last heartbeat against the timeout.
///
/// Returns the list of workers newly marked as unhealthy.
pub fn health_sweep(
    workers: &mut std::collections::HashMap<WorkerId, crate::worker::WorkerNode>,
    timeout: Duration,
) -> HealthSweepResult {
    let mut result = HealthSweepResult::default();

    for worker in workers.values_mut() {
        result.workers_checked += 1;

        if worker.last_heartbeat.elapsed() > timeout
            && (worker.status == WorkerStatus::Ready || worker.status == WorkerStatus::Busy)
        {
            warn!(
                "Worker {} marked unhealthy (no heartbeat for {:?})",
                worker.id,
                worker.last_heartbeat.elapsed()
            );
            worker.status = WorkerStatus::Unhealthy;
            result.workers_marked_unhealthy.push(worker.id.clone());
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::{WorkerNode, WorkerStatus};
    use std::collections::HashMap;
    use std::time::Instant;

    #[test]
    fn test_health_sweep_healthy_workers() {
        let mut workers = HashMap::new();
        let mut w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w.status = WorkerStatus::Ready;
        w.last_heartbeat = Instant::now(); // just now
        workers.insert(w.id.clone(), w);

        let result = health_sweep(&mut workers, Duration::from_secs(15));
        assert_eq!(result.workers_checked, 1);
        assert!(result.workers_marked_unhealthy.is_empty());
    }

    #[test]
    fn test_health_sweep_stale_worker() {
        let mut workers = HashMap::new();
        let mut w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w.status = WorkerStatus::Ready;
        // Simulate stale heartbeat by subtracting time
        w.last_heartbeat = Instant::now() - Duration::from_secs(20);
        workers.insert(w.id.clone(), w);

        let result = health_sweep(&mut workers, Duration::from_secs(15));
        assert_eq!(result.workers_marked_unhealthy.len(), 1);
        assert_eq!(
            workers[&WorkerId("w1".into())].status,
            WorkerStatus::Unhealthy
        );
    }

    #[test]
    fn test_health_sweep_skips_already_unhealthy() {
        let mut workers = HashMap::new();
        let mut w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w.status = WorkerStatus::Unhealthy;
        w.last_heartbeat = Instant::now() - Duration::from_secs(20);
        workers.insert(w.id.clone(), w);

        let result = health_sweep(&mut workers, Duration::from_secs(15));
        assert!(result.workers_marked_unhealthy.is_empty());
    }

    #[test]
    fn test_health_sweep_empty_workers() {
        let mut workers = HashMap::new();
        let result = health_sweep(&mut workers, Duration::from_secs(15));
        assert_eq!(result.workers_checked, 0);
        assert!(result.workers_marked_unhealthy.is_empty());
    }

    #[test]
    fn test_health_sweep_busy_worker_goes_stale() {
        let mut workers = HashMap::new();
        let mut w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w.status = WorkerStatus::Busy;
        w.last_heartbeat = Instant::now() - Duration::from_secs(20);
        workers.insert(w.id.clone(), w);

        let result = health_sweep(&mut workers, Duration::from_secs(15));
        assert_eq!(result.workers_marked_unhealthy.len(), 1);
        assert_eq!(
            workers[&WorkerId("w1".into())].status,
            WorkerStatus::Unhealthy
        );
    }

    #[test]
    fn test_health_sweep_skips_draining() {
        let mut workers = HashMap::new();
        let mut w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w.status = WorkerStatus::Draining;
        w.last_heartbeat = Instant::now() - Duration::from_secs(60);
        workers.insert(w.id.clone(), w);

        let result = health_sweep(&mut workers, Duration::from_secs(15));
        assert!(result.workers_marked_unhealthy.is_empty());
        assert_eq!(
            workers[&WorkerId("w1".into())].status,
            WorkerStatus::Draining
        );
    }

    #[test]
    fn test_health_sweep_skips_registering() {
        let mut workers = HashMap::new();
        let mut w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        // WorkerNode::new sets Registering by default
        w.last_heartbeat = Instant::now() - Duration::from_secs(60);
        workers.insert(w.id.clone(), w);

        let result = health_sweep(&mut workers, Duration::from_secs(15));
        assert!(result.workers_marked_unhealthy.is_empty());
    }

    #[test]
    fn test_health_sweep_mixed_workers() {
        let mut workers = HashMap::new();

        // Healthy Ready worker
        let mut w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w1.status = WorkerStatus::Ready;
        w1.last_heartbeat = Instant::now();
        workers.insert(w1.id.clone(), w1);

        // Stale Ready worker
        let mut w2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        w2.status = WorkerStatus::Ready;
        w2.last_heartbeat = Instant::now() - Duration::from_secs(20);
        workers.insert(w2.id.clone(), w2);

        // Stale Busy worker
        let mut w3 = WorkerNode::new(
            WorkerId("w3".into()),
            "http://localhost:9002".into(),
            "key".into(),
        );
        w3.status = WorkerStatus::Busy;
        w3.last_heartbeat = Instant::now() - Duration::from_secs(20);
        workers.insert(w3.id.clone(), w3);

        // Already Unhealthy worker
        let mut w4 = WorkerNode::new(
            WorkerId("w4".into()),
            "http://localhost:9003".into(),
            "key".into(),
        );
        w4.status = WorkerStatus::Unhealthy;
        w4.last_heartbeat = Instant::now() - Duration::from_secs(60);
        workers.insert(w4.id.clone(), w4);

        let result = health_sweep(&mut workers, Duration::from_secs(15));
        assert_eq!(result.workers_checked, 4);
        assert_eq!(result.workers_marked_unhealthy.len(), 2); // w2 and w3

        assert_eq!(workers[&WorkerId("w1".into())].status, WorkerStatus::Ready);
        assert_eq!(
            workers[&WorkerId("w2".into())].status,
            WorkerStatus::Unhealthy
        );
        assert_eq!(
            workers[&WorkerId("w3".into())].status,
            WorkerStatus::Unhealthy
        );
        assert_eq!(
            workers[&WorkerId("w4".into())].status,
            WorkerStatus::Unhealthy
        );
    }

    #[test]
    fn test_health_sweep_boundary_at_exact_timeout() {
        let mut workers = HashMap::new();
        let mut w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w.status = WorkerStatus::Ready;
        // Exactly at timeout — elapsed() will be slightly more due to test execution
        w.last_heartbeat = Instant::now() - Duration::from_secs(15);
        workers.insert(w.id.clone(), w);

        // With exactly 15s elapsed and 15s timeout, elapsed() > timeout will be
        // true due to nanos from test execution time
        let result = health_sweep(&mut workers, Duration::from_secs(15));
        // Should be marked unhealthy since any time past 15s counts
        assert_eq!(result.workers_marked_unhealthy.len(), 1);
    }

    #[test]
    fn test_health_sweep_idempotent_second_sweep() {
        let mut workers = HashMap::new();
        let mut w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w.status = WorkerStatus::Ready;
        w.last_heartbeat = Instant::now() - Duration::from_secs(20);
        workers.insert(w.id.clone(), w);

        let result1 = health_sweep(&mut workers, Duration::from_secs(15));
        assert_eq!(result1.workers_marked_unhealthy.len(), 1);

        // Second sweep: already unhealthy, should not re-mark
        let result2 = health_sweep(&mut workers, Duration::from_secs(15));
        assert!(result2.workers_marked_unhealthy.is_empty());
    }
}
