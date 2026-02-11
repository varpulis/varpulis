//! Coordinator state machine: worker registry, pipeline group management, event routing.

use crate::connector_config::{self, ClusterConnector};
use crate::health::{self, HealthSweepResult, HEARTBEAT_TIMEOUT};
use crate::migration::{MigrationReason, MigrationStatus, MigrationTask};
use crate::pipeline_group::{
    DeployedPipelineGroup, GroupStatus, PipelineDeployment, PipelineDeploymentStatus,
    PipelineGroupSpec,
};
use crate::routing::find_target_pipeline;
use crate::worker::{HeartbeatRequest, WorkerId, WorkerNode, WorkerStatus};
use crate::{ClusterError, LeastLoadedPlacement, PlacementStrategy, RoundRobinPlacement};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
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
    /// Active and recent pipeline migrations.
    pub active_migrations: HashMap<String, MigrationTask>,
    /// Whether a rebalance should be triggered on next health sweep.
    pub pending_rebalance: bool,
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
            active_migrations: HashMap::new(),
            pending_rebalance: false,
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
        // New worker may improve load distribution
        if !self.pipeline_groups.is_empty() {
            self.pending_rebalance = true;
        }
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
            let replica_count = pipeline.replicas.max(1);

            // Build the list of (replica_name, worker_id) to deploy
            let mut replica_names_for_group: Vec<String> = Vec::new();

            for replica_idx in 0..replica_count {
                let replica_name = if replica_count > 1 {
                    format!("{}#{}", pipeline.name, replica_idx)
                } else {
                    pipeline.name.clone()
                };

                // Select worker: use affinity if specified, otherwise placement strategy
                // For replicas, try to spread across different workers
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
                        error!("No worker available for pipeline '{}'", replica_name);
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
                    "name": replica_name,
                    "source": effective_source,
                });

                info!(
                    "Deploying pipeline '{}' to worker {} at {}",
                    replica_name, worker_id, worker_address
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

                        group.placements.insert(replica_name.clone(), deployment);

                        // Update worker's assigned pipelines
                        if let Some(w) = self.workers.get_mut(&worker_id) {
                            w.assigned_pipelines.push(replica_name.clone());
                            w.capacity.pipelines_running += 1;
                        }

                        replica_names_for_group.push(replica_name.clone());

                        info!("Pipeline '{}' deployed successfully", replica_name);
                    }
                    Ok(response) => {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_default();
                        error!(
                            "Failed to deploy pipeline '{}': HTTP {} - {}",
                            replica_name, status, body
                        );
                        group.placements.insert(
                            replica_name.clone(),
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
                            worker_id, replica_name, e
                        );
                        group.placements.insert(
                            replica_name.clone(),
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

            // Register replica group if replicas > 1
            if replica_count > 1 && !replica_names_for_group.is_empty() {
                use crate::pipeline_group::{PartitionStrategy, ReplicaGroup};
                let strategy = match &pipeline.partition_key {
                    Some(key) => PartitionStrategy::HashKey(key.clone()),
                    None => PartitionStrategy::RoundRobin,
                };
                group.replica_groups.insert(
                    pipeline.name.clone(),
                    ReplicaGroup::new(pipeline.name.clone(), replica_names_for_group, strategy),
                );
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

        // Remove the group entirely after teardown
        self.pipeline_groups.remove(group_id);

        Ok(())
    }

    /// Inject an event into a pipeline group, routing it to the correct worker.
    /// Supports replica-aware routing when replicas > 1.
    pub async fn inject_event(
        &self,
        group_id: &str,
        event: InjectEventRequest,
    ) -> Result<InjectResponse, ClusterError> {
        let group = self
            .pipeline_groups
            .get(group_id)
            .ok_or_else(|| ClusterError::GroupNotFound(group_id.to_string()))?;

        let logical_target = find_target_pipeline(group, &event.event_type).ok_or_else(|| {
            ClusterError::RoutingFailed(format!(
                "No target pipeline for event type '{}'",
                event.event_type
            ))
        })?;

        // Resolve through replica group if one exists
        let target_name = if let Some(replica_group) = group.replica_groups.get(logical_target) {
            replica_group.select_replica(&event.fields).to_string()
        } else {
            logical_target.to_string()
        };

        let deployment = group.placements.get(&target_name).ok_or_else(|| {
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
            routed_to: target_name,
            worker_id: deployment.worker_id.0.clone(),
            worker_response,
        })
    }

    /// Inject a batch of events (parsed from .evt text) into a pipeline group.
    ///
    /// Groups events by target pipeline and sends one batch HTTP request per
    /// pipeline, reducing O(N) HTTP calls to O(P) where P = distinct pipelines.
    pub async fn inject_batch(
        &self,
        group_id: &str,
        request: InjectBatchRequest,
    ) -> Result<InjectBatchResponse, ClusterError> {
        use crate::routing::find_target_pipeline;
        use std::time::Instant;
        use varpulis_runtime::event_file::EventFileParser;

        let group = self
            .pipeline_groups
            .get(group_id)
            .ok_or_else(|| ClusterError::GroupNotFound(group_id.to_string()))?;

        let timed_events = EventFileParser::parse(&request.events_text)
            .map_err(|e| ClusterError::RoutingFailed(format!("Failed to parse events: {}", e)))?;

        // Group events by target pipeline
        let mut pipeline_batches: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        let mut total_events = 0usize;

        for timed_event in timed_events {
            let event = timed_event.event;
            let event_type = event.event_type.to_string();

            let logical_target = find_target_pipeline(group, &event_type)
                .unwrap_or_else(|| {
                    group
                        .spec
                        .pipelines
                        .first()
                        .map(|p| p.name.as_str())
                        .unwrap_or("default")
                })
                .to_string();

            let mut fields = serde_json::Map::new();
            for (key, value) in &event.data {
                if let Ok(json_val) = serde_json::to_value(value) {
                    fields.insert(key.to_string(), json_val);
                }
            }

            // Resolve through replica group if one exists
            let target = if let Some(rg) = group.replica_groups.get(&logical_target) {
                rg.select_replica(&fields).to_string()
            } else {
                logical_target
            };

            let evt_json = serde_json::json!({
                "event_type": event_type,
                "fields": fields,
            });

            pipeline_batches.entry(target).or_default().push(evt_json);
            total_events += 1;
        }

        let start = Instant::now();
        let mut events_sent = 0usize;
        let mut events_failed = 0usize;
        let mut output_events = Vec::new();
        let mut errors = Vec::new();

        // Send one batch request per target pipeline
        for (pipeline_name, batch) in &pipeline_batches {
            let deployment = match group.placements.get(pipeline_name.as_str()) {
                Some(d) => d,
                None => {
                    events_failed += batch.len();
                    errors.push(format!("Pipeline '{}' not deployed", pipeline_name));
                    continue;
                }
            };

            let batch_url = format!(
                "{}/api/v1/pipelines/{}/events-batch",
                deployment.worker_address, deployment.pipeline_id
            );

            let batch_body = serde_json::json!({
                "events": batch,
            });

            match self
                .http_client
                .post(&batch_url)
                .header("x-api-key", &deployment.worker_api_key)
                .json(&batch_body)
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        if let Ok(resp_json) = response.json::<serde_json::Value>().await {
                            let accepted = resp_json
                                .get("accepted")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0) as usize;
                            events_sent += accepted;
                            events_failed += batch.len() - accepted;

                            if let Some(outputs) = resp_json.get("output_events") {
                                if let Some(arr) = outputs.as_array() {
                                    output_events.extend(arr.iter().cloned());
                                }
                            }
                        } else {
                            events_failed += batch.len();
                            errors.push(format!(
                                "Pipeline '{}': failed to parse response",
                                pipeline_name
                            ));
                        }
                    } else {
                        let body = response.text().await.unwrap_or_default();
                        events_failed += batch.len();
                        errors.push(format!(
                            "Pipeline '{}': worker error: {}",
                            pipeline_name, body
                        ));
                    }
                }
                Err(e) => {
                    events_failed += batch.len();
                    errors.push(format!("Pipeline '{}': {}", pipeline_name, e));
                }
            }
        }

        // Include pre-routing time in the total
        let processing_time_us = start.elapsed().as_micros() as u64;

        if events_sent == 0 && total_events > 0 && !errors.is_empty() {
            warn!(
                "Batch injection: all {} events failed for group '{}'",
                total_events, group_id
            );
        }

        Ok(InjectBatchResponse {
            events_sent,
            events_failed,
            output_events,
            errors,
            processing_time_us,
        })
    }

    /// Run a health sweep and return results.
    pub fn health_sweep(&mut self) -> HealthSweepResult {
        health::health_sweep(&mut self.workers, HEARTBEAT_TIMEOUT)
    }

    // =========================================================================
    // Migration & Failover
    // =========================================================================

    /// Migrate a pipeline from its current worker to a target worker.
    ///
    /// Steps: checkpoint (if source alive) → deploy → restore → switch → cleanup.
    pub async fn migrate_pipeline(
        &mut self,
        pipeline_name: &str,
        group_id: &str,
        target_worker_id: &WorkerId,
        reason: MigrationReason,
    ) -> Result<String, ClusterError> {
        let group = self
            .pipeline_groups
            .get(group_id)
            .ok_or_else(|| ClusterError::GroupNotFound(group_id.to_string()))?;

        let deployment = group
            .placements
            .get(pipeline_name)
            .ok_or_else(|| {
                ClusterError::MigrationFailed(format!(
                    "Pipeline '{}' not found in group '{}'",
                    pipeline_name, group_id
                ))
            })?
            .clone();

        let source_worker_id = deployment.worker_id.clone();

        let target_worker = self
            .workers
            .get(target_worker_id)
            .ok_or_else(|| ClusterError::WorkerNotFound(target_worker_id.0.clone()))?;
        let target_address = target_worker.address.clone();
        let target_api_key = target_worker.api_key.clone();

        let migration_id = uuid::Uuid::new_v4().to_string();

        let mut task = MigrationTask {
            id: migration_id.clone(),
            pipeline_name: pipeline_name.to_string(),
            group_id: group_id.to_string(),
            source_worker: source_worker_id.clone(),
            target_worker: target_worker_id.clone(),
            status: MigrationStatus::Checkpointing,
            started_at: Instant::now(),
            checkpoint: None,
            reason,
        };

        self.active_migrations
            .insert(migration_id.clone(), task.clone());

        // Step 1: Checkpoint (best-effort — skip if source is dead)
        let checkpoint = if self
            .workers
            .get(&source_worker_id)
            .map(|w| w.status != WorkerStatus::Unhealthy)
            .unwrap_or(false)
        {
            let checkpoint_url = format!(
                "{}/api/v1/pipelines/{}/checkpoint",
                deployment.worker_address, deployment.pipeline_id
            );
            match self
                .http_client
                .post(&checkpoint_url)
                .header("x-api-key", &deployment.worker_api_key)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<CheckpointResponsePayload>().await {
                        Ok(cp_resp) => {
                            info!(
                                "Checkpoint captured for pipeline '{}' ({} events)",
                                pipeline_name, cp_resp.events_processed
                            );
                            Some(cp_resp.checkpoint)
                        }
                        Err(e) => {
                            warn!(
                                "Failed to deserialize checkpoint for '{}': {}",
                                pipeline_name, e
                            );
                            None
                        }
                    }
                }
                Ok(resp) => {
                    warn!(
                        "Checkpoint HTTP error for '{}': {}",
                        pipeline_name,
                        resp.status()
                    );
                    None
                }
                Err(e) => {
                    warn!("Checkpoint request failed for '{}': {}", pipeline_name, e);
                    None
                }
            }
        } else {
            info!(
                "Source worker {} is dead, proceeding without checkpoint for '{}'",
                source_worker_id, pipeline_name
            );
            None
        };

        task.checkpoint = checkpoint.clone();

        // Step 2: Deploy to target worker
        task.status = MigrationStatus::Deploying;
        self.active_migrations
            .insert(migration_id.clone(), task.clone());

        // Find the pipeline's VPL source from the group spec.
        // For replicas like "p1#0", extract logical name "p1" for the lookup.
        let logical_name = pipeline_name
            .rsplit_once('#')
            .map(|(base, _)| base)
            .unwrap_or(pipeline_name);

        let vpl_source = group
            .spec
            .pipelines
            .iter()
            .find(|p| p.name == logical_name)
            .map(|p| p.source.clone())
            .ok_or_else(|| {
                ClusterError::MigrationFailed(format!(
                    "VPL source not found for '{}'",
                    pipeline_name
                ))
            })?;

        let (enriched_source, _) =
            crate::connector_config::inject_connectors(&vpl_source, &self.connectors);

        let deploy_url = format!("{}/api/v1/pipelines", target_address);
        let deploy_body = serde_json::json!({
            "name": pipeline_name,
            "source": enriched_source,
        });

        let new_pipeline_id = match self
            .http_client
            .post(&deploy_url)
            .header("x-api-key", &target_api_key)
            .json(&deploy_body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                let resp_body: DeployResponse = resp
                    .json()
                    .await
                    .map_err(|e| ClusterError::MigrationFailed(e.to_string()))?;
                resp_body.id
            }
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                task.status = MigrationStatus::Failed(format!("Deploy failed: {}", body));
                self.active_migrations.insert(migration_id.clone(), task);
                return Err(ClusterError::MigrationFailed(format!(
                    "Deploy to target failed: {}",
                    body
                )));
            }
            Err(e) => {
                task.status = MigrationStatus::Failed(format!("Deploy request failed: {}", e));
                self.active_migrations.insert(migration_id.clone(), task);
                return Err(ClusterError::MigrationFailed(e.to_string()));
            }
        };

        // Step 3: Restore checkpoint on target
        if let Some(ref cp) = checkpoint {
            task.status = MigrationStatus::Restoring;
            self.active_migrations
                .insert(migration_id.clone(), task.clone());

            let restore_url = format!(
                "{}/api/v1/pipelines/{}/restore",
                target_address, new_pipeline_id
            );
            let restore_body = serde_json::json!({ "checkpoint": cp });

            match self
                .http_client
                .post(&restore_url)
                .header("x-api-key", &target_api_key)
                .json(&restore_body)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!(
                        "Checkpoint restored for pipeline '{}' on worker {}",
                        pipeline_name, target_worker_id
                    );
                }
                Ok(resp) => {
                    let body = resp.text().await.unwrap_or_default();
                    warn!(
                        "Restore failed for '{}' (continuing without state): {}",
                        pipeline_name, body
                    );
                }
                Err(e) => {
                    warn!(
                        "Restore request failed for '{}' (continuing without state): {}",
                        pipeline_name, e
                    );
                }
            }
        }

        // Step 4: Switch — update placements to point to target
        task.status = MigrationStatus::Switching;
        self.active_migrations
            .insert(migration_id.clone(), task.clone());

        if let Some(group) = self.pipeline_groups.get_mut(group_id) {
            group.placements.insert(
                pipeline_name.to_string(),
                PipelineDeployment {
                    worker_id: target_worker_id.clone(),
                    worker_address: target_address,
                    worker_api_key: target_api_key,
                    pipeline_id: new_pipeline_id,
                    status: PipelineDeploymentStatus::Running,
                },
            );
            group.update_status();
        }

        // Update worker bookkeeping
        if let Some(w) = self.workers.get_mut(target_worker_id) {
            w.assigned_pipelines.push(pipeline_name.to_string());
            w.capacity.pipelines_running += 1;
        }

        // Step 5: Cleanup — remove pipeline from source (skip if dead)
        task.status = MigrationStatus::CleaningUp;
        self.active_migrations
            .insert(migration_id.clone(), task.clone());

        let source_alive = self
            .workers
            .get(&source_worker_id)
            .map(|w| w.status != WorkerStatus::Unhealthy)
            .unwrap_or(false);

        if source_alive && !deployment.pipeline_id.is_empty() {
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
                Ok(_) => {
                    info!(
                        "Removed old pipeline '{}' from worker {}",
                        pipeline_name, source_worker_id
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to remove old pipeline '{}' from {}: {}",
                        pipeline_name, source_worker_id, e
                    );
                }
            }
        }

        if let Some(w) = self.workers.get_mut(&source_worker_id) {
            w.assigned_pipelines.retain(|p| p != pipeline_name);
            w.capacity.pipelines_running = w.capacity.pipelines_running.saturating_sub(1);
        }

        task.status = MigrationStatus::Completed;
        self.active_migrations.insert(migration_id.clone(), task);

        info!(
            "Migration complete: pipeline '{}' moved from {} to {}",
            pipeline_name, source_worker_id, target_worker_id
        );

        Ok(migration_id)
    }

    /// Handle a worker failure: migrate all its pipelines to healthy workers.
    pub async fn handle_worker_failure(
        &mut self,
        worker_id: &WorkerId,
    ) -> Vec<Result<String, ClusterError>> {
        let mut results = Vec::new();

        // Collect all (group_id, pipeline_name) pairs on the failed worker
        let affected: Vec<(String, String)> = self
            .pipeline_groups
            .iter()
            .flat_map(|(gid, group)| {
                group
                    .placements
                    .iter()
                    .filter(|(_, dep)| dep.worker_id == *worker_id)
                    .map(|(pname, _)| (gid.clone(), pname.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();

        if affected.is_empty() {
            return results;
        }

        info!(
            "Worker {} failed, migrating {} pipeline(s)",
            worker_id,
            affected.len()
        );

        for (group_id, pipeline_name) in affected {
            // Select a healthy target using least-loaded strategy
            let target = {
                let available: Vec<&WorkerNode> = self
                    .workers
                    .values()
                    .filter(|w| w.is_available() && w.id != *worker_id)
                    .collect();
                LeastLoadedPlacement.place(
                    &crate::pipeline_group::PipelinePlacement {
                        name: pipeline_name.clone(),
                        source: String::new(),
                        worker_affinity: None,
                        replicas: 1,
                        partition_key: None,
                    },
                    &available,
                )
            };

            match target {
                Some(target_id) => {
                    let result = self
                        .migrate_pipeline(
                            &pipeline_name,
                            &group_id,
                            &target_id,
                            MigrationReason::Failover,
                        )
                        .await;
                    match &result {
                        Ok(mid) => info!(
                            "Failover migration {} for '{}' to {}",
                            mid, pipeline_name, target_id
                        ),
                        Err(e) => error!("Failover failed for '{}': {}", pipeline_name, e),
                    }
                    results.push(result);
                }
                None => {
                    error!(
                        "No healthy worker available for failover of '{}'",
                        pipeline_name
                    );
                    results.push(Err(ClusterError::NoWorkersAvailable));
                }
            }
        }

        results
    }

    /// Drain a worker: migrate all its pipelines elsewhere, then deregister it.
    ///
    /// If `timeout` is provided, the drain will stop migrating after the
    /// deadline and force-deregister with only partially migrated pipelines.
    pub async fn drain_worker(
        &mut self,
        worker_id: &WorkerId,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, ClusterError> {
        let deadline = timeout.map(|t| Instant::now() + t);

        // Mark as draining
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| ClusterError::WorkerNotFound(worker_id.0.clone()))?;

        if worker.status == WorkerStatus::Draining {
            // Already draining — idempotent
            return Ok(Vec::new());
        }

        worker.status = WorkerStatus::Draining;
        info!("Worker {} marked as draining", worker_id);

        // Collect pipelines to migrate
        let affected: Vec<(String, String)> = self
            .pipeline_groups
            .iter()
            .flat_map(|(gid, group)| {
                group
                    .placements
                    .iter()
                    .filter(|(_, dep)| dep.worker_id == *worker_id)
                    .map(|(pname, _)| (gid.clone(), pname.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();

        let total = affected.len();
        let mut migration_ids = Vec::new();
        for (group_id, pipeline_name) in affected {
            // Check timeout
            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    warn!(
                        "Drain timeout reached for worker {}: {}/{} pipeline(s) migrated",
                        worker_id,
                        migration_ids.len(),
                        total
                    );
                    break;
                }
            }

            let target = {
                let available: Vec<&WorkerNode> = self
                    .workers
                    .values()
                    .filter(|w| w.is_available() && w.id != *worker_id)
                    .collect();
                LeastLoadedPlacement.place(
                    &crate::pipeline_group::PipelinePlacement {
                        name: pipeline_name.clone(),
                        source: String::new(),
                        worker_affinity: None,
                        replicas: 1,
                        partition_key: None,
                    },
                    &available,
                )
            };

            match target {
                Some(target_id) => {
                    match self
                        .migrate_pipeline(
                            &pipeline_name,
                            &group_id,
                            &target_id,
                            MigrationReason::Drain,
                        )
                        .await
                    {
                        Ok(mid) => migration_ids.push(mid),
                        Err(e) => {
                            warn!("Failed to drain pipeline '{}': {}", pipeline_name, e);
                        }
                    }
                }
                None => {
                    warn!(
                        "No target worker available to drain pipeline '{}'",
                        pipeline_name
                    );
                }
            }
        }

        // Deregister the worker after draining
        self.workers.remove(worker_id);
        info!(
            "Worker {} deregistered after draining {} pipeline(s)",
            worker_id,
            migration_ids.len()
        );

        Ok(migration_ids)
    }

    /// Rebalance pipelines across workers for more even load distribution.
    ///
    /// Moves pipelines from overloaded workers to underloaded ones.
    pub async fn rebalance(&mut self) -> Result<Vec<String>, ClusterError> {
        self.pending_rebalance = false;

        let available_workers: Vec<WorkerId> = self
            .workers
            .values()
            .filter(|w| w.is_available())
            .map(|w| w.id.clone())
            .collect();

        if available_workers.len() < 2 {
            return Ok(Vec::new());
        }

        // Calculate load per worker
        let mut worker_load: HashMap<WorkerId, usize> = available_workers
            .iter()
            .map(|wid| {
                let load = self
                    .workers
                    .get(wid)
                    .map(|w| w.capacity.pipelines_running)
                    .unwrap_or(0);
                (wid.clone(), load)
            })
            .collect();

        let total_pipelines: usize = worker_load.values().sum();
        if total_pipelines == 0 {
            return Ok(Vec::new());
        }

        let avg_load = total_pipelines as f64 / available_workers.len() as f64;
        let threshold = 1.0; // move if > avg + threshold

        // Find overloaded workers and collect movable pipelines
        let mut migrations_to_do: Vec<(String, String, WorkerId)> = Vec::new();

        for wid in &available_workers {
            let load = *worker_load.get(wid).unwrap_or(&0);
            if load as f64 <= avg_load + threshold {
                continue;
            }

            let excess = load - (avg_load.ceil() as usize);
            if excess == 0 {
                continue;
            }

            // Find pipelines on this worker (skip affinity-pinned ones)
            let mut movable: Vec<(String, String)> = Vec::new();
            for (gid, group) in &self.pipeline_groups {
                for (pname, dep) in &group.placements {
                    if dep.worker_id != *wid {
                        continue;
                    }
                    // Skip if affinity-pinned (strip replica suffix for lookup)
                    let logical = pname
                        .rsplit_once('#')
                        .map(|(base, _)| base)
                        .unwrap_or(pname);
                    let has_affinity = group
                        .spec
                        .pipelines
                        .iter()
                        .any(|p| p.name == logical && p.worker_affinity.is_some());
                    if !has_affinity {
                        movable.push((gid.clone(), pname.clone()));
                    }
                }
            }

            for (gid, pname) in movable.into_iter().take(excess) {
                // Find least-loaded target
                let target = available_workers
                    .iter()
                    .filter(|w| *w != wid)
                    .min_by_key(|w| worker_load.get(w).unwrap_or(&0));

                if let Some(target_id) = target {
                    migrations_to_do.push((gid, pname, target_id.clone()));
                    // Adjust virtual load for next iteration
                    *worker_load.get_mut(wid).unwrap() -= 1;
                    *worker_load.entry(target_id.clone()).or_insert(0) += 1;
                }
            }
        }

        let mut migration_ids = Vec::new();
        for (group_id, pipeline_name, target_id) in migrations_to_do {
            match self
                .migrate_pipeline(
                    &pipeline_name,
                    &group_id,
                    &target_id,
                    MigrationReason::Rebalance,
                )
                .await
            {
                Ok(mid) => migration_ids.push(mid),
                Err(e) => {
                    warn!("Rebalance migration failed for '{}': {}", pipeline_name, e);
                }
            }
        }

        if !migration_ids.is_empty() {
            info!("Rebalance: {} migration(s) initiated", migration_ids.len());
        }

        Ok(migration_ids)
    }

    /// Remove completed/failed migrations older than the given duration.
    pub fn cleanup_completed_migrations(&mut self, max_age: Duration) {
        let before = self.active_migrations.len();
        self.active_migrations.retain(|_, task| {
            let dominated = matches!(
                task.status,
                MigrationStatus::Completed | MigrationStatus::Failed(_)
            );
            !(dominated && task.started_at.elapsed() > max_age)
        });
        let removed = before - self.active_migrations.len();
        if removed > 0 {
            info!("Cleaned up {} completed migration(s)", removed);
        }
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

/// Response from worker checkpoint API.
#[derive(Debug, Deserialize)]
struct CheckpointResponsePayload {
    #[allow(dead_code)]
    pipeline_id: String,
    checkpoint: varpulis_runtime::persistence::EngineCheckpoint,
    events_processed: u64,
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

/// Request to inject a batch of events in .evt text format.
#[derive(Debug, Serialize, Deserialize)]
pub struct InjectBatchRequest {
    pub events_text: String,
}

/// Response from batch event injection.
#[derive(Debug, Serialize, Deserialize)]
pub struct InjectBatchResponse {
    pub events_sent: usize,
    pub events_failed: usize,
    pub output_events: Vec<serde_json::Value>,
    pub errors: Vec<String>,
    pub processing_time_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_group::{DeployedPipelineGroup, PipelineGroupSpec, PipelinePlacement};
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

    // =========================================================================
    // Tests for production readiness fixes
    // =========================================================================

    #[test]
    fn test_cleanup_completed_migrations_removes_old() {
        let mut coord = Coordinator::new();

        // Insert a completed migration with old start time
        let mut task = MigrationTask {
            id: "m1".into(),
            pipeline_name: "p1".into(),
            group_id: "g1".into(),
            source_worker: WorkerId("w1".into()),
            target_worker: WorkerId("w2".into()),
            status: MigrationStatus::Completed,
            started_at: Instant::now() - Duration::from_secs(7200), // 2 hours ago
            checkpoint: None,
            reason: MigrationReason::Failover,
        };
        coord.active_migrations.insert("m1".into(), task.clone());

        // Insert a recent completed migration
        task.id = "m2".into();
        task.started_at = Instant::now(); // just now
        coord.active_migrations.insert("m2".into(), task.clone());

        // Insert a failed migration that is old
        task.id = "m3".into();
        task.status = MigrationStatus::Failed("error".into());
        task.started_at = Instant::now() - Duration::from_secs(7200);
        coord.active_migrations.insert("m3".into(), task.clone());

        // Insert an in-progress migration that is old (should NOT be cleaned)
        task.id = "m4".into();
        task.status = MigrationStatus::Deploying;
        task.started_at = Instant::now() - Duration::from_secs(7200);
        coord.active_migrations.insert("m4".into(), task);

        assert_eq!(coord.active_migrations.len(), 4);

        // Cleanup with 1 hour TTL
        coord.cleanup_completed_migrations(Duration::from_secs(3600));

        // m1 (completed, old) and m3 (failed, old) should be removed
        // m2 (completed, recent) and m4 (in-progress, old) should remain
        assert_eq!(coord.active_migrations.len(), 2);
        assert!(coord.active_migrations.contains_key("m2"));
        assert!(coord.active_migrations.contains_key("m4"));
    }

    #[test]
    fn test_cleanup_completed_migrations_noop_when_empty() {
        let mut coord = Coordinator::new();
        coord.cleanup_completed_migrations(Duration::from_secs(3600));
        assert!(coord.active_migrations.is_empty());
    }

    #[test]
    fn test_cleanup_completed_migrations_keeps_recent() {
        let mut coord = Coordinator::new();

        let task = MigrationTask {
            id: "m1".into(),
            pipeline_name: "p1".into(),
            group_id: "g1".into(),
            source_worker: WorkerId("w1".into()),
            target_worker: WorkerId("w2".into()),
            status: MigrationStatus::Completed,
            started_at: Instant::now(), // just now
            checkpoint: None,
            reason: MigrationReason::Rebalance,
        };
        coord.active_migrations.insert("m1".into(), task);

        coord.cleanup_completed_migrations(Duration::from_secs(3600));
        assert_eq!(coord.active_migrations.len(), 1);
    }

    #[tokio::test]
    async fn test_drain_worker_idempotent() {
        let mut coord = Coordinator::new();
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        node.status = WorkerStatus::Draining;
        coord.workers.insert(node.id.clone(), node);

        // Draining an already-draining worker is idempotent
        let result = coord.drain_worker(&WorkerId("w1".into()), None).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_drain_worker_not_found() {
        let mut coord = Coordinator::new();
        let result = coord
            .drain_worker(&WorkerId("nonexistent".into()), None)
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ClusterError::WorkerNotFound(id) => assert_eq!(id, "nonexistent"),
            other => panic!("Expected WorkerNotFound, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_drain_worker_marks_draining() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);
        assert_eq!(
            coord.workers[&WorkerId("w1".into())].status,
            WorkerStatus::Ready
        );

        // Drain with no pipelines — worker gets deregistered
        let result = coord.drain_worker(&WorkerId("w1".into()), None).await;
        assert!(result.is_ok());
        // Worker should be removed after drain
        assert!(!coord.workers.contains_key(&WorkerId("w1".into())));
    }

    #[test]
    fn test_register_worker_triggers_pending_rebalance() {
        let mut coord = Coordinator::new();
        assert!(!coord.pending_rebalance);

        // No pipeline groups → no pending rebalance
        let node1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node1);
        assert!(!coord.pending_rebalance);

        // Add a pipeline group to make rebalance relevant
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![PipelinePlacement {
                name: "p1".into(),
                source: "stream A = X".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            }],
            routes: vec![],
        };
        let group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        coord.pipeline_groups.insert("g1".into(), group);

        // Now registering a new worker should trigger pending rebalance
        let node2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        coord.register_worker(node2);
        assert!(coord.pending_rebalance);
    }

    #[tokio::test]
    async fn test_rebalance_needs_two_workers() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);

        let result = coord.rebalance().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_rebalance_no_pipelines() {
        let mut coord = Coordinator::new();
        for i in 0..3 {
            let node = WorkerNode::new(
                WorkerId(format!("w{}", i)),
                format!("http://localhost:900{}", i),
                "key".into(),
            );
            coord.register_worker(node);
        }

        let result = coord.rebalance().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_worker_failure_no_pipelines() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);

        // No pipelines assigned → no migrations
        let results = coord
            .handle_worker_failure(&WorkerId("w1".into()))
            .await;
        assert!(results.is_empty());
    }
}
