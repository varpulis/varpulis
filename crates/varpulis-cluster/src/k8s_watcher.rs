//! Kubernetes pod watcher for instant infrastructure failure detection.
//!
//! Watches worker pods via the K8s Watch API to detect OOMKill, eviction,
//! and node failures within ~1-2s (vs ~15s with heartbeat polling).
//!
//! Requires the `k8s` feature flag.

use crate::api::SharedCoordinator;
use crate::worker::{WorkerId, WorkerStatus};
use std::collections::BTreeMap;
use std::path::Path;
use tracing::{error, info, warn};

/// Configuration for the pod watcher.
#[derive(Debug, Clone)]
pub struct PodWatcherConfig {
    pub namespace: String,
    pub label_selector: String,
}

/// Check if we're running inside a Kubernetes cluster.
pub fn is_in_kubernetes() -> bool {
    Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists()
}

/// Extract the worker ID from pod labels.
pub fn extract_worker_id(labels: &BTreeMap<String, String>) -> Option<String> {
    labels.get("varpulis.io/worker-id").cloned().or_else(|| {
        // Fall back to pod name from statefulset-style naming
        labels.get("statefulset.kubernetes.io/pod-name").cloned()
    })
}

/// Watch worker pods for failures and trigger immediate failover.
///
/// This function runs indefinitely, watching for pod phase changes
/// (Failed, Unknown) and container termination reasons (OOMKilled).
#[cfg(feature = "k8s")]
pub async fn watch_worker_pods(config: PodWatcherConfig, coordinator: SharedCoordinator) {
    use futures_util::TryStreamExt;
    use k8s_openapi::api::core::v1::Pod;
    use kube::runtime::watcher;
    use kube::{Api, Client};

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create K8s client: {}", e);
            return;
        }
    };

    let pods: Api<Pod> = Api::namespaced(client, &config.namespace);
    let watcher_config = watcher::Config::default().labels(&config.label_selector);

    info!(
        "Starting K8s pod watcher: namespace={}, selector={}",
        config.namespace, config.label_selector
    );

    // Restart loop — if the watch stream ends, restart it
    loop {
        let stream = watcher::watcher(pods.clone(), watcher_config.clone());

        let result: Result<(), watcher::Error> = stream
            .try_for_each(|event| {
                let coordinator = coordinator.clone();
                async move {
                    match event {
                        watcher::Event::Applied(pod) => {
                            handle_pod_event(&pod, &coordinator).await;
                        }
                        watcher::Event::Deleted(pod) => {
                            handle_pod_deleted(&pod, &coordinator).await;
                        }
                        watcher::Event::Restarted(pods) => {
                            info!("K8s watcher restarted, {} pods in cache", pods.len());
                        }
                    }
                    Ok(())
                }
            })
            .await;

        match result {
            Ok(()) => {
                warn!("K8s pod watcher stream ended, restarting in 5s...");
            }
            Err(e) => {
                error!("K8s pod watcher error: {}, restarting in 5s...", e);
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// Handle a pod event (Applied = created or modified).
#[cfg(feature = "k8s")]
async fn handle_pod_event(pod: &k8s_openapi::api::core::v1::Pod, coordinator: &SharedCoordinator) {
    let pod_name = pod.metadata.name.as_deref().unwrap_or("unknown");

    let labels = pod.metadata.labels.clone().unwrap_or_default();
    let worker_id = match extract_worker_id(&labels) {
        Some(id) => id,
        None => pod_name.to_string(),
    };

    let Some(ref status) = pod.status else {
        return;
    };

    // Check pod phase
    let phase = status.phase.as_deref().unwrap_or("Unknown");
    let is_failed = matches!(phase, "Failed" | "Unknown");

    // Check for OOMKilled containers
    let is_oom = status
        .container_statuses
        .as_ref()
        .map(|statuses| {
            statuses.iter().any(|cs| {
                cs.state
                    .as_ref()
                    .and_then(|s| s.terminated.as_ref())
                    .map(|t| t.reason.as_deref() == Some("OOMKilled"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    if is_failed || is_oom {
        let reason = if is_oom { "OOMKilled" } else { phase };

        warn!(
            "K8s pod watcher: worker {} (pod {}) detected as {} — triggering failover",
            worker_id, pod_name, reason
        );

        let wid = WorkerId(worker_id);
        let mut coord = coordinator.write().await;

        // Only the leader/writer should trigger failover
        if !coord.ha_role.is_writer() {
            warn!(
                "K8s pod watcher: skipping failover for {} — not leader",
                wid
            );
            return;
        }

        if let Some(worker) = coord.workers.get_mut(&wid) {
            if worker.status == WorkerStatus::Ready {
                worker.status = WorkerStatus::Unhealthy;
                info!(
                    "Worker {} marked unhealthy by K8s pod watcher ({})",
                    wid, reason
                );
                coord.handle_worker_failure(&wid).await;
            }
        }
    }
}

/// Handle a pod deletion event.
#[cfg(feature = "k8s")]
async fn handle_pod_deleted(
    pod: &k8s_openapi::api::core::v1::Pod,
    coordinator: &SharedCoordinator,
) {
    let pod_name = pod.metadata.name.as_deref().unwrap_or("unknown");

    let labels = pod.metadata.labels.clone().unwrap_or_default();
    let worker_id = match extract_worker_id(&labels) {
        Some(id) => id,
        None => pod_name.to_string(),
    };

    warn!(
        "K8s pod watcher: worker {} (pod {}) deleted — triggering failover",
        worker_id, pod_name
    );

    let wid = WorkerId(worker_id);
    let mut coord = coordinator.write().await;

    // Only the leader/writer should trigger failover
    if !coord.ha_role.is_writer() {
        warn!(
            "K8s pod watcher: skipping failover for {} — not leader",
            wid
        );
        return;
    }

    if let Some(worker) = coord.workers.get_mut(&wid) {
        if worker.status == WorkerStatus::Ready {
            worker.status = WorkerStatus::Unhealthy;
            info!(
                "Worker {} marked unhealthy by K8s pod watcher (pod deleted)",
                wid
            );
            coord.handle_worker_failure(&wid).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_in_kubernetes_false() {
        // On a dev machine, this should be false
        assert!(!is_in_kubernetes());
    }

    #[test]
    fn test_extract_worker_id_from_label() {
        let mut labels = BTreeMap::new();
        labels.insert("varpulis.io/worker-id".to_string(), "w0".to_string());
        assert_eq!(extract_worker_id(&labels), Some("w0".to_string()));
    }

    #[test]
    fn test_extract_worker_id_fallback_to_pod_name() {
        let mut labels = BTreeMap::new();
        labels.insert(
            "statefulset.kubernetes.io/pod-name".to_string(),
            "varpulis-worker-0".to_string(),
        );
        assert_eq!(
            extract_worker_id(&labels),
            Some("varpulis-worker-0".to_string())
        );
    }

    #[test]
    fn test_extract_worker_id_none() {
        let labels = BTreeMap::new();
        assert_eq!(extract_worker_id(&labels), None);
    }

    #[test]
    fn test_extract_worker_id_prefers_explicit_label() {
        let mut labels = BTreeMap::new();
        labels.insert(
            "varpulis.io/worker-id".to_string(),
            "explicit-id".to_string(),
        );
        labels.insert(
            "statefulset.kubernetes.io/pod-name".to_string(),
            "fallback-name".to_string(),
        );
        assert_eq!(extract_worker_id(&labels), Some("explicit-id".to_string()));
    }

    #[test]
    fn test_pod_watcher_config() {
        let config = PodWatcherConfig {
            namespace: "varpulis".to_string(),
            label_selector: "app.kubernetes.io/component=worker".to_string(),
        };
        assert_eq!(config.namespace, "varpulis");
        assert_eq!(config.label_selector, "app.kubernetes.io/component=worker");
    }
}
