//! Coordinator state machine: worker registry, pipeline group management, event routing.

use crate::connector_config::{self, ClusterConnector};
use crate::health::{self, HealthSweepResult, HEARTBEAT_TIMEOUT};
use crate::pipeline_group::{
    DeployedPipelineGroup, GroupStatus, PipelineDeployment, PipelineDeploymentStatus,
    PipelineGroupSpec,
};
use crate::routing::find_target_pipeline;
use crate::worker::{HeartbeatRequest, WorkerId, WorkerNode, WorkerStatus};
use crate::{ClusterError, PlacementStrategy, RoundRobinPlacement};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::worker::PipelineMetrics;

/// Aggregated cluster metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterMetrics {
    pub pipelines: Vec<PipelineWorkerMetrics>,
}

/// Metrics for a single pipeline on a single worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineWorkerMetrics {
    pub pipeline_name: String,
    pub worker_id: String,
    pub events_in: u64,
    pub events_out: u64,
}

/// Central coordinator managing the cluster.
pub struct Coordinator {
    pub workers: HashMap<WorkerId, WorkerNode>,
    pub pipeline_groups: HashMap<String, DeployedPipelineGroup>,
    pub connectors: HashMap<String, ClusterConnector>,
    /// Per-worker pipeline metrics from heartbeats.
    pub worker_metrics: HashMap<WorkerId, Vec<PipelineMetrics>>,
    placement: Box<dyn PlacementStrategy>,
    http_client: reqwest::Client,
}

impl Coordinator {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
            pipeline_groups: HashMap::new(),
            connectors: HashMap::new(),
            worker_metrics: HashMap::new(),
            placement: Box::new(RoundRobinPlacement::new()),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to build HTTP client"),
        }
    }

    /// Register a worker node. Marks it Ready immediately.
    pub fn register_worker(&mut self, mut node: WorkerNode) -> WorkerId {
        let id = node.id.clone();
        node.status = WorkerStatus::Ready;
        info!("Worker registered: {} at {}", id, node.address);
        self.workers.insert(id.clone(), node);
        id
    }

    /// Process a heartbeat from a worker.
    pub fn heartbeat(
        &mut self,
        worker_id: &WorkerId,
        hb: &HeartbeatRequest,
    ) -> Result<(), ClusterError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| ClusterError::WorkerNotFound(worker_id.0.clone()))?;

        worker.last_heartbeat = std::time::Instant::now();
        worker.capacity.pipelines_running = hb.pipelines_running;

        // If worker was unhealthy and heartbeat arrives, mark it ready again
        if worker.status == WorkerStatus::Unhealthy {
            info!("Worker {} recovered (heartbeat received)", worker_id);
            worker.status = WorkerStatus::Ready;
        }

        // Store per-pipeline metrics if provided
        if !hb.pipeline_metrics.is_empty() {
            self.update_worker_metrics(worker_id, hb.pipeline_metrics.clone());
        }

        Ok(())
    }

    /// Deregister a worker.
    pub fn deregister_worker(&mut self, worker_id: &WorkerId) -> Result<(), ClusterError> {
        self.workers
            .remove(worker_id)
            .ok_or_else(|| ClusterError::WorkerNotFound(worker_id.0.clone()))?;
        info!("Worker deregistered: {}", worker_id);
        Ok(())
    }

    /// Deploy a pipeline group across workers.
    pub async fn deploy_group(&mut self, spec: PipelineGroupSpec) -> Result<String, ClusterError> {
        let group_id = uuid::Uuid::new_v4().to_string();
        let mut group =
            DeployedPipelineGroup::new(group_id.clone(), spec.name.clone(), spec.clone());

        // Collect available workers
        let available_workers: Vec<&WorkerNode> =
            self.workers.values().filter(|w| w.is_available()).collect();

        if available_workers.is_empty() {
            return Err(ClusterError::NoWorkersAvailable);
        }

        info!(
            "Deploying pipeline group '{}' ({} pipelines, {} workers available)",
            spec.name,
            spec.pipelines.len(),
            available_workers.len()
        );

        // Inject cluster connectors into pipeline sources
        let enriched_pipelines: Vec<_> = spec
            .pipelines
            .iter()
            .map(|p| {
                let (enriched_source, _) =
                    connector_config::inject_connectors(&p.source, &self.connectors);
                (p, enriched_source)
            })
            .collect();

        for (pipeline, effective_source) in &enriched_pipelines {
            // Select worker: use affinity if specified, otherwise placement strategy
            let selected_worker_id = if let Some(ref affinity) = pipeline.worker_affinity {
                let wid = WorkerId(affinity.clone());
                if self.workers.contains_key(&wid) && self.workers[&wid].is_available() {
                    Some(wid)
                } else {
                    warn!(
                        "Worker affinity '{}' not available, falling back to placement strategy",
                        affinity
                    );
                    let available: Vec<&WorkerNode> =
                        self.workers.values().filter(|w| w.is_available()).collect();
                    self.placement.place(pipeline, &available)
                }
            } else {
                let available: Vec<&WorkerNode> =
                    self.workers.values().filter(|w| w.is_available()).collect();
                self.placement.place(pipeline, &available)
            };

            let worker_id = match selected_worker_id {
                Some(id) => id,
                None => {
                    error!("No worker available for pipeline '{}'", pipeline.name);
                    group.status = GroupStatus::Failed;
                    self.pipeline_groups.insert(group_id.clone(), group);
                    return Err(ClusterError::NoWorkersAvailable);
                }
            };

            let worker = &self.workers[&worker_id];
            let worker_address = worker.address.clone();
            let worker_api_key = worker.api_key.clone();

            // Deploy pipeline to the worker via its REST API
            let deploy_url = format!("{}/api/v1/pipelines", worker_address);
            let deploy_body = serde_json::json!({
                "name": pipeline.name,
                "source": effective_source,
            });

            info!(
                "Deploying pipeline '{}' to worker {} at {}",
                pipeline.name, worker_id, worker_address
            );

            match self
                .http_client
                .post(&deploy_url)
                .header("x-api-key", &worker_api_key)
                .json(&deploy_body)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    let resp_body: DeployResponse = response
                        .json()
                        .await
                        .map_err(|e| ClusterError::DeployFailed(e.to_string()))?;

                    let deployment = PipelineDeployment {
                        worker_id: worker_id.clone(),
                        worker_address: worker_address.clone(),
                        worker_api_key: worker_api_key.clone(),
                        pipeline_id: resp_body.id,
                        status: PipelineDeploymentStatus::Running,
                    };

                    group.placements.insert(pipeline.name.clone(), deployment);

                    // Update worker's assigned pipelines
                    if let Some(w) = self.workers.get_mut(&worker_id) {
                        w.assigned_pipelines.push(pipeline.name.clone());
                        w.capacity.pipelines_running += 1;
                    }

                    info!("Pipeline '{}' deployed successfully", pipeline.name);
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    error!(
                        "Failed to deploy pipeline '{}': HTTP {} - {}",
                        pipeline.name, status, body
                    );
                    group.placements.insert(
                        pipeline.name.clone(),
                        PipelineDeployment {
                            worker_id: worker_id.clone(),
                            worker_address,
                            worker_api_key,
                            pipeline_id: String::new(),
                            status: PipelineDeploymentStatus::Failed,
                        },
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to reach worker {} for pipeline '{}': {}",
                        worker_id, pipeline.name, e
                    );
                    group.placements.insert(
                        pipeline.name.clone(),
                        PipelineDeployment {
                            worker_id: worker_id.clone(),
                            worker_address,
                            worker_api_key,
                            pipeline_id: String::new(),
                            status: PipelineDeploymentStatus::Failed,
                        },
                    );
                }
            }
        }

        group.update_status();
        let final_status = group.status.clone();
        self.pipeline_groups.insert(group_id.clone(), group);

        info!(
            "Pipeline group '{}' deployment complete: {}",
            spec.name, final_status
        );

        Ok(group_id)
    }

    /// Tear down a pipeline group: delete all deployed pipelines from workers.
    pub async fn teardown_group(&mut self, group_id: &str) -> Result<(), ClusterError> {
        let group = self
            .pipeline_groups
            .get(group_id)
            .ok_or_else(|| ClusterError::GroupNotFound(group_id.to_string()))?;

        let placements: Vec<(String, PipelineDeployment)> = group
            .placements
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (name, deployment) in placements {
            if deployment.pipeline_id.is_empty() {
                continue; // never deployed
            }
            let delete_url = format!(
                "{}/api/v1/pipelines/{}",
                deployment.worker_address, deployment.pipeline_id
            );
            match self
                .http_client
                .delete(&delete_url)
                .header("x-api-key", &deployment.worker_api_key)
                .send()
                .await
            {
                Ok(_) => info!(
                    "Torn down pipeline '{}' from worker {}",
                    name, deployment.worker_id
                ),
                Err(e) => warn!(
                    "Failed to tear down pipeline '{}' from worker {}: {}",
                    name, deployment.worker_id, e
                ),
            }

            // Update worker's assigned pipelines
            if let Some(w) = self.workers.get_mut(&deployment.worker_id) {
                w.assigned_pipelines.retain(|p| p != &name);
                w.capacity.pipelines_running = w.capacity.pipelines_running.saturating_sub(1);
            }
        }

        if let Some(group) = self.pipeline_groups.get_mut(group_id) {
            group.status = GroupStatus::TornDown;
        }

        Ok(())
    }

    /// Inject an event into a pipeline group, routing it to the correct worker.
    pub async fn inject_event(
        &self,
        group_id: &str,
        event: InjectEventRequest,
    ) -> Result<InjectResponse, ClusterError> {
        let group = self
            .pipeline_groups
            .get(group_id)
            .ok_or_else(|| ClusterError::GroupNotFound(group_id.to_string()))?;

        let target_name = find_target_pipeline(group, &event.event_type).ok_or_else(|| {
            ClusterError::RoutingFailed(format!(
                "No target pipeline for event type '{}'",
                event.event_type
            ))
        })?;

        let deployment = group.placements.get(target_name).ok_or_else(|| {
            ClusterError::RoutingFailed(format!("Pipeline '{}' not deployed", target_name))
        })?;

        let inject_url = format!(
            "{}/api/v1/pipelines/{}/events",
            deployment.worker_address, deployment.pipeline_id
        );

        let inject_body = serde_json::json!({
            "event_type": event.event_type,
            "fields": event.fields,
        });

        let response = self
            .http_client
            .post(&inject_url)
            .header("x-api-key", &deployment.worker_api_key)
            .json(&inject_body)
            .send()
            .await
            .map_err(|e| ClusterError::RoutingFailed(e.to_string()))?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClusterError::RoutingFailed(format!(
                "Worker returned error: {}",
                body
            )));
        }

        let worker_response: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ClusterError::RoutingFailed(e.to_string()))?;

        Ok(InjectResponse {
            routed_to: target_name.to_string(),
            worker_id: deployment.worker_id.0.clone(),
            worker_response,
        })
    }

    /// Run a health sweep and return results.
    pub fn health_sweep(&mut self) -> HealthSweepResult {
        health::health_sweep(&mut self.workers, HEARTBEAT_TIMEOUT)
    }

    // =========================================================================
    // Connector CRUD
    // =========================================================================

    /// List all cluster connectors.
    pub fn list_connectors(&self) -> Vec<&ClusterConnector> {
        self.connectors.values().collect()
    }

    /// Get a connector by name.
    pub fn get_connector(&self, name: &str) -> Result<&ClusterConnector, ClusterError> {
        self.connectors
            .get(name)
            .ok_or_else(|| ClusterError::ConnectorNotFound(name.to_string()))
    }

    /// Create a new connector. Errors if name already exists.
    pub fn create_connector(
        &mut self,
        connector: ClusterConnector,
    ) -> Result<&ClusterConnector, ClusterError> {
        if self.connectors.contains_key(&connector.name) {
            return Err(ClusterError::ConnectorValidation(format!(
                "Connector '{}' already exists",
                connector.name
            )));
        }
        connector_config::validate_connector(&connector)?;
        let name = connector.name.clone();
        self.connectors.insert(name.clone(), connector);
        info!("Connector created: {}", name);
        Ok(&self.connectors[&name])
    }

    /// Update an existing connector.
    pub fn update_connector(
        &mut self,
        name: &str,
        connector: ClusterConnector,
    ) -> Result<&ClusterConnector, ClusterError> {
        if !self.connectors.contains_key(name) {
            return Err(ClusterError::ConnectorNotFound(name.to_string()));
        }
        connector_config::validate_connector(&connector)?;
        self.connectors.insert(name.to_string(), connector);
        info!("Connector updated: {}", name);
        Ok(&self.connectors[name])
    }

    /// Delete a connector.
    pub fn delete_connector(&mut self, name: &str) -> Result<(), ClusterError> {
        self.connectors
            .remove(name)
            .ok_or_else(|| ClusterError::ConnectorNotFound(name.to_string()))?;
        info!("Connector deleted: {}", name);
        Ok(())
    }

    // =========================================================================
    // Metrics
    // =========================================================================

    /// Store per-pipeline metrics from a worker heartbeat.
    pub fn update_worker_metrics(&mut self, worker_id: &WorkerId, metrics: Vec<PipelineMetrics>) {
        self.worker_metrics.insert(worker_id.clone(), metrics);
    }

    /// Aggregate metrics across all workers.
    pub fn get_cluster_metrics(&self) -> ClusterMetrics {
        let mut pipelines = Vec::new();
        for (worker_id, metrics) in &self.worker_metrics {
            for m in metrics {
                pipelines.push(PipelineWorkerMetrics {
                    pipeline_name: m.pipeline_name.clone(),
                    worker_id: worker_id.0.clone(),
                    events_in: m.events_in,
                    events_out: m.events_out,
                });
            }
        }
        ClusterMetrics { pipelines }
    }
}

impl Default for Coordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Response from worker deploy API.
#[derive(Debug, Deserialize)]
struct DeployResponse {
    id: String,
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    status: String,
}

/// Request to inject an event into a pipeline group.
#[derive(Debug, Serialize, Deserialize)]
pub struct InjectEventRequest {
    pub event_type: String,
    #[serde(default)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

/// Response from event injection.
#[derive(Debug, Serialize, Deserialize)]
pub struct InjectResponse {
    pub routed_to: String,
    pub worker_id: String,
    pub worker_response: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::WorkerNode;

    #[test]
    fn test_coordinator_register_worker() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        let id = coord.register_worker(node);
        assert_eq!(id, WorkerId("w1".into()));
        assert_eq!(coord.workers.len(), 1);
        assert_eq!(coord.workers[&id].status, WorkerStatus::Ready);
    }

    #[test]
    fn test_coordinator_deregister_worker() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);
        assert!(coord.deregister_worker(&WorkerId("w1".into())).is_ok());
        assert!(coord.workers.is_empty());
    }

    #[test]
    fn test_coordinator_deregister_unknown() {
        let mut coord = Coordinator::new();
        assert!(coord
            .deregister_worker(&WorkerId("unknown".into()))
            .is_err());
    }

    #[test]
    fn test_coordinator_heartbeat() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);

        let hb = HeartbeatRequest {
            events_processed: 100,
            pipelines_running: 2,
            pipeline_metrics: vec![],
        };
        assert!(coord.heartbeat(&WorkerId("w1".into()), &hb).is_ok());
        assert_eq!(
            coord.workers[&WorkerId("w1".into())]
                .capacity
                .pipelines_running,
            2
        );
    }

    #[test]
    fn test_coordinator_health_sweep() {
        let mut coord = Coordinator::new();
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        node.status = WorkerStatus::Ready;
        node.last_heartbeat = std::time::Instant::now() - std::time::Duration::from_secs(20);
        coord.workers.insert(node.id.clone(), node);

        let result = coord.health_sweep();
        assert_eq!(result.workers_marked_unhealthy.len(), 1);
    }

    #[test]
    fn test_coordinator_heartbeat_unknown_worker() {
        let mut coord = Coordinator::new();
        let hb = HeartbeatRequest {
            events_processed: 0,
            pipelines_running: 0,
            pipeline_metrics: vec![],
        };
        let result = coord.heartbeat(&WorkerId("nonexistent".into()), &hb);
        assert!(result.is_err());
        match result.unwrap_err() {
            crate::ClusterError::WorkerNotFound(id) => assert_eq!(id, "nonexistent"),
            other => panic!("Expected WorkerNotFound, got: {:?}", other),
        }
    }

    #[test]
    fn test_coordinator_heartbeat_recovers_unhealthy() {
        let mut coord = Coordinator::new();
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        node.status = WorkerStatus::Ready;
        coord.workers.insert(node.id.clone(), node);

        // Mark unhealthy
        coord
            .workers
            .get_mut(&WorkerId("w1".into()))
            .unwrap()
            .status = WorkerStatus::Unhealthy;
        assert_eq!(
            coord.workers[&WorkerId("w1".into())].status,
            WorkerStatus::Unhealthy
        );

        // Heartbeat should recover
        let hb = HeartbeatRequest {
            events_processed: 50,
            pipelines_running: 1,
            pipeline_metrics: vec![],
        };
        assert!(coord.heartbeat(&WorkerId("w1".into()), &hb).is_ok());
        assert_eq!(
            coord.workers[&WorkerId("w1".into())].status,
            WorkerStatus::Ready
        );
    }

    #[test]
    fn test_coordinator_re_register_same_worker() {
        let mut coord = Coordinator::new();
        let node1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key1".into(),
        );
        coord.register_worker(node1);
        assert_eq!(coord.workers[&WorkerId("w1".into())].api_key, "key1");

        // Re-register with different address/key
        let node2 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9999".into(),
            "key2".into(),
        );
        coord.register_worker(node2);
        assert_eq!(coord.workers.len(), 1);
        assert_eq!(
            coord.workers[&WorkerId("w1".into())].address,
            "http://localhost:9999"
        );
        assert_eq!(coord.workers[&WorkerId("w1".into())].api_key, "key2");
    }

    #[test]
    fn test_coordinator_multiple_workers() {
        let mut coord = Coordinator::new();
        for i in 0..5 {
            let node = WorkerNode::new(
                WorkerId(format!("w{}", i)),
                format!("http://localhost:900{}", i),
                "key".into(),
            );
            coord.register_worker(node);
        }
        assert_eq!(coord.workers.len(), 5);
        for i in 0..5 {
            assert!(coord.workers.contains_key(&WorkerId(format!("w{}", i))));
            assert_eq!(
                coord.workers[&WorkerId(format!("w{}", i))].status,
                WorkerStatus::Ready
            );
        }
    }

    #[test]
    fn test_coordinator_deregister_all() {
        let mut coord = Coordinator::new();
        for i in 0..3 {
            let node = WorkerNode::new(
                WorkerId(format!("w{}", i)),
                format!("http://localhost:900{}", i),
                "key".into(),
            );
            coord.register_worker(node);
        }
        assert_eq!(coord.workers.len(), 3);

        for i in 0..3 {
            assert!(coord
                .deregister_worker(&WorkerId(format!("w{}", i)))
                .is_ok());
        }
        assert!(coord.workers.is_empty());
    }

    #[test]
    fn test_coordinator_heartbeat_updates_pipelines_running() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);
        assert_eq!(
            coord.workers[&WorkerId("w1".into())]
                .capacity
                .pipelines_running,
            0
        );

        let hb = HeartbeatRequest {
            events_processed: 1000,
            pipelines_running: 5,
            pipeline_metrics: vec![],
        };
        coord.heartbeat(&WorkerId("w1".into()), &hb).unwrap();
        assert_eq!(
            coord.workers[&WorkerId("w1".into())]
                .capacity
                .pipelines_running,
            5
        );
    }

    #[test]
    fn test_coordinator_default() {
        let coord = Coordinator::default();
        assert!(coord.workers.is_empty());
        assert!(coord.pipeline_groups.is_empty());
    }
}
