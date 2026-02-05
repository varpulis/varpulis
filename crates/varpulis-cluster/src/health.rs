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
}
