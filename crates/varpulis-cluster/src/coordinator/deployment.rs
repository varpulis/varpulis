//! Deployment, teardown, and event injection for pipeline groups.

use std::collections::HashMap;
use std::time::Instant;

use tracing::{error, info, warn};

use super::{
    Coordinator, DeployGroupPlan, DeployResponse, DeployTask, DeployTaskResult, InjectBatchRequest,
    InjectBatchResponse, InjectEventRequest, InjectResponse, InjectTarget, TeardownPlan,
};
use crate::pipeline_group::{
    DeployedPipelineGroup, GroupStatus, PipelineDeployment, PipelineDeploymentStatus,
    PipelineGroupSpec,
};
use crate::routing::find_target_pipeline;
use crate::worker::{WorkerId, WorkerNode};
use crate::{connector_config, ClusterError};

impl Coordinator {
    /// Phase 1: Build a deployment plan without holding the lock during HTTP I/O.
    ///
    /// Selects workers, enriches pipeline sources, and returns a plan with
    /// all information needed to execute HTTP deploys without coordinator state.
    #[tracing::instrument(skip(self, spec), fields(group = %spec.name, pipelines = spec.pipelines.len()))]
    pub fn plan_deploy_group(
        &self,
        spec: &PipelineGroupSpec,
    ) -> Result<DeployGroupPlan, ClusterError> {
        let group_id = uuid::Uuid::new_v4().to_string();

        let available_workers: Vec<&WorkerNode> =
            self.workers.values().filter(|w| w.is_available()).collect();

        if available_workers.is_empty() {
            return Err(ClusterError::NoWorkersAvailable);
        }

        info!(
            group = %spec.name,
            pipelines = spec.pipelines.len(),
            workers_available = available_workers.len(),
            "Deploy planned"
        );

        let enriched_pipelines: Vec<_> = spec
            .pipelines
            .iter()
            .map(|p| {
                let (enriched_source, _) =
                    connector_config::inject_connectors(&p.source, &self.connectors);
                (p, enriched_source)
            })
            .collect();

        let mut tasks = Vec::new();

        for (pipeline, effective_source) in &enriched_pipelines {
            let replica_count = pipeline.replicas.max(1);

            for replica_idx in 0..replica_count {
                let replica_name = if replica_count > 1 {
                    format!("{}#{}", pipeline.name, replica_idx)
                } else {
                    pipeline.name.clone()
                };

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
                        return Err(ClusterError::NoWorkersAvailable);
                    }
                };

                let worker = self
                    .workers
                    .get(&worker_id)
                    .ok_or_else(|| ClusterError::WorkerNotFound(worker_id.0.clone()))?;

                tasks.push(DeployTask {
                    replica_name,
                    pipeline_name: pipeline.name.clone(),
                    worker_id,
                    worker_address: worker.address.clone(),
                    worker_api_key: worker.api_key.expose().to_string(),
                    source: effective_source.clone(),
                    replica_count,
                });
            }
        }

        Ok(DeployGroupPlan {
            group_id,
            spec: spec.clone(),
            tasks,
            deploy_start: Instant::now(),
        })
    }

    /// Execute deployment plan: send HTTP requests to workers.
    ///
    /// This method does NOT require `&self` -- it uses a shared HTTP client
    /// and the pre-built plan, allowing the coordinator lock to be released.
    #[tracing::instrument(skip(http_client, plan), fields(group_id = %plan.group_id, tasks = plan.tasks.len()))]
    pub async fn execute_deploy_plan(
        http_client: &reqwest::Client,
        plan: &DeployGroupPlan,
    ) -> Vec<DeployTaskResult> {
        let mut results = Vec::with_capacity(plan.tasks.len());

        for task in &plan.tasks {
            let deploy_url = format!("{}/api/v1/pipelines", task.worker_address);
            let deploy_body = serde_json::json!({
                "name": task.replica_name,
                "source": task.source,
            });

            info!(
                "Deploying pipeline '{}' to worker {} at {}",
                task.replica_name, task.worker_id, task.worker_address
            );

            let outcome = match http_client
                .post(&deploy_url)
                .header("x-api-key", &task.worker_api_key)
                .json(&deploy_body)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    match response.json::<DeployResponse>().await {
                        Ok(resp_body) => {
                            info!(
                                "Pipeline '{}' deployed on worker {} (id={}, status={})",
                                resp_body.name, task.worker_id, resp_body.id, resp_body.status
                            );
                            Ok(resp_body)
                        }
                        Err(e) => Err(format!("Failed to parse deploy response: {}", e)),
                    }
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    error!(
                        "Failed to deploy pipeline '{}': HTTP {} - {}",
                        task.replica_name, status, body
                    );
                    Err(format!("HTTP {} - {}", status, body))
                }
                Err(e) => {
                    error!(
                        "Failed to reach worker {} for pipeline '{}': {}",
                        task.worker_id, task.replica_name, e
                    );
                    Err(e.to_string())
                }
            };

            results.push(DeployTaskResult {
                replica_name: task.replica_name.clone(),
                pipeline_name: task.pipeline_name.clone(),
                worker_id: task.worker_id.clone(),
                worker_address: task.worker_address.clone(),
                worker_api_key: task.worker_api_key.clone(),
                replica_count: task.replica_count,
                outcome,
            });
        }

        results
    }

    /// Execute deployment plan via NATS request/reply.
    #[cfg(feature = "nats-transport")]
    pub async fn execute_deploy_plan_nats(
        nats_client: &async_nats::Client,
        plan: &DeployGroupPlan,
    ) -> Vec<DeployTaskResult> {
        use crate::nats_transport;

        let mut results = Vec::with_capacity(plan.tasks.len());
        let timeout = std::time::Duration::from_secs(10);

        for task in &plan.tasks {
            let subject = nats_transport::subject_cmd(&task.worker_id.0, "deploy");
            let deploy_body = serde_json::json!({
                "name": task.replica_name,
                "source": task.source,
            });

            info!(
                "Deploying pipeline '{}' to worker {} via NATS",
                task.replica_name, task.worker_id
            );

            let outcome = match nats_transport::nats_request::<_, DeployResponse>(
                nats_client,
                &subject,
                &deploy_body,
                timeout,
            )
            .await
            {
                Ok(resp_body) => {
                    info!(
                        "Pipeline '{}' deployed on worker {} (id={}, status={})",
                        resp_body.name, task.worker_id, resp_body.id, resp_body.status
                    );
                    Ok(resp_body)
                }
                Err(e) => {
                    error!(
                        "Failed to deploy pipeline '{}' on worker {}: {}",
                        task.replica_name, task.worker_id, e
                    );
                    Err(e.to_string())
                }
            };

            results.push(DeployTaskResult {
                replica_name: task.replica_name.clone(),
                pipeline_name: task.pipeline_name.clone(),
                worker_id: task.worker_id.clone(),
                worker_address: task.worker_address.clone(),
                worker_api_key: task.worker_api_key.clone(),
                replica_count: task.replica_count,
                outcome,
            });
        }

        results
    }

    /// Phase 3: Commit deployment results to coordinator state.
    #[tracing::instrument(skip(self, plan, results), fields(group_id = %plan.group_id, group = %plan.spec.name))]
    pub fn commit_deploy_group(
        &mut self,
        plan: DeployGroupPlan,
        results: Vec<DeployTaskResult>,
    ) -> Result<String, ClusterError> {
        let mut group = DeployedPipelineGroup::new(
            plan.group_id.clone(),
            plan.spec.name.clone(),
            plan.spec.clone(),
        );

        // Track replica names per pipeline for ReplicaGroup registration
        let mut replica_names_map: HashMap<String, Vec<String>> = HashMap::new();

        for result in results {
            match result.outcome {
                Ok(resp_body) => {
                    let deployment = PipelineDeployment {
                        worker_id: result.worker_id.clone(),
                        worker_address: result.worker_address,
                        worker_api_key: result.worker_api_key,
                        pipeline_id: resp_body.id,
                        status: PipelineDeploymentStatus::Running,
                        epoch: 0,
                    };
                    group
                        .placements
                        .insert(result.replica_name.clone(), deployment);

                    if let Some(w) = self.workers.get_mut(&result.worker_id) {
                        w.assigned_pipelines.push(result.replica_name.clone());
                        w.capacity.pipelines_running += 1;
                    }

                    if result.replica_count > 1 {
                        replica_names_map
                            .entry(result.pipeline_name.clone())
                            .or_default()
                            .push(result.replica_name);
                    }
                }
                Err(_) => {
                    group.placements.insert(
                        result.replica_name.clone(),
                        PipelineDeployment {
                            worker_id: result.worker_id,
                            worker_address: result.worker_address,
                            worker_api_key: result.worker_api_key,
                            pipeline_id: String::new(),
                            status: PipelineDeploymentStatus::Failed,
                            epoch: 0,
                        },
                    );
                }
            }
        }

        // Register replica groups
        for (pipeline_name, replica_names) in replica_names_map {
            if !replica_names.is_empty() {
                use crate::pipeline_group::{PartitionStrategy, ReplicaGroup};
                let strategy = plan
                    .spec
                    .pipelines
                    .iter()
                    .find(|p| p.name == pipeline_name)
                    .and_then(|p| p.partition_key.clone())
                    .map(PartitionStrategy::HashKey)
                    .unwrap_or(PartitionStrategy::RoundRobin);

                group.replica_groups.insert(
                    pipeline_name.clone(),
                    ReplicaGroup::new(pipeline_name, replica_names, strategy),
                );
            }
        }

        group.update_status();
        let final_status = group.status.clone();
        self.pipeline_groups.insert(plan.group_id.clone(), group);

        let deploy_success = final_status == GroupStatus::Running;
        self.cluster_metrics
            .record_deploy(deploy_success, plan.deploy_start.elapsed().as_secs_f64());
        self.update_metrics_counts();

        info!(
            "Pipeline group '{}' deployment complete: {}",
            plan.spec.name, final_status
        );

        Ok(plan.group_id)
    }

    /// Deploy a pipeline group across workers.
    #[tracing::instrument(skip(self))]
    pub async fn deploy_group(&mut self, spec: PipelineGroupSpec) -> Result<String, ClusterError> {
        let deploy_start = Instant::now();
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

                let worker = self
                    .workers
                    .get(&worker_id)
                    .ok_or_else(|| ClusterError::WorkerNotFound(worker_id.0.clone()))?;
                let worker_address = worker.address.clone();
                let worker_api_key = worker.api_key.expose().to_string();

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

                        info!(
                            "Pipeline '{}' deployed on worker {} (id={}, status={})",
                            resp_body.name, worker_id, resp_body.id, resp_body.status
                        );

                        let deployment = PipelineDeployment {
                            worker_id: worker_id.clone(),
                            worker_address: worker_address.clone(),
                            worker_api_key: worker_api_key.clone(),
                            pipeline_id: resp_body.id,
                            status: PipelineDeploymentStatus::Running,
                            epoch: 0,
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
                                epoch: 0,
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
                                epoch: 0,
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

        let deploy_success = final_status == GroupStatus::Running;
        self.cluster_metrics
            .record_deploy(deploy_success, deploy_start.elapsed().as_secs_f64());
        self.update_metrics_counts();

        info!(
            "Pipeline group '{}' deployment complete: {}",
            spec.name, final_status
        );

        Ok(group_id)
    }

    /// Tear down a pipeline group: delete all deployed pipelines from workers.
    /// Phase 1: Plan teardown -- collect deployment info under read lock.
    pub fn plan_teardown_group(&self, group_id: &str) -> Result<TeardownPlan, ClusterError> {
        let group = self
            .pipeline_groups
            .get(group_id)
            .ok_or_else(|| ClusterError::GroupNotFound(group_id.to_string()))?;

        let tasks: Vec<(String, PipelineDeployment)> = group
            .placements
            .iter()
            .filter(|(_, d)| !d.pipeline_id.is_empty())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(TeardownPlan {
            group_id: group_id.to_string(),
            tasks,
        })
    }

    /// Phase 2: Execute teardown HTTP calls -- no lock held.
    pub async fn execute_teardown_plan(http_client: &reqwest::Client, plan: &TeardownPlan) {
        for (name, deployment) in &plan.tasks {
            let delete_url = format!(
                "{}/api/v1/pipelines/{}",
                deployment.worker_address, deployment.pipeline_id
            );
            match http_client
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
        }
    }

    /// Phase 3: Commit teardown -- update state under write lock.
    pub fn commit_teardown_group(&mut self, plan: &TeardownPlan) {
        for (name, deployment) in &plan.tasks {
            if let Some(w) = self.workers.get_mut(&deployment.worker_id) {
                w.assigned_pipelines.retain(|p| p != name);
                w.capacity.pipelines_running = w.capacity.pipelines_running.saturating_sub(1);
            }
        }
        self.pipeline_groups.remove(&plan.group_id);
    }

    /// Inject an event into a pipeline group, routing it to the correct worker.
    /// Supports replica-aware routing when replicas > 1.
    /// Resolve the target for an inject event (Phase 1 -- read lock).
    /// Returns (url, api_key, inject_body, target_name, worker_id).
    pub fn resolve_inject_target(
        &self,
        group_id: &str,
        event: &InjectEventRequest,
    ) -> Result<InjectTarget, ClusterError> {
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

        Ok(InjectTarget {
            url: format!(
                "{}/api/v1/pipelines/{}/events",
                deployment.worker_address, deployment.pipeline_id
            ),
            api_key: deployment.worker_api_key.clone(),
            target_name,
            worker_id: deployment.worker_id.0.clone(),
        })
    }

    /// Execute inject event HTTP call (Phase 2 -- no lock).
    pub async fn execute_inject_event(
        http_client: &reqwest::Client,
        target: &InjectTarget,
        event: &InjectEventRequest,
    ) -> Result<InjectResponse, ClusterError> {
        let inject_body = serde_json::json!({
            "event_type": event.event_type,
            "fields": event.fields,
        });

        let response = http_client
            .post(&target.url)
            .header("x-api-key", &target.api_key)
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
            routed_to: target.target_name.clone(),
            worker_id: target.worker_id.clone(),
            worker_response,
        })
    }

    /// Execute teardown plan via NATS request/reply.
    #[cfg(feature = "nats-transport")]
    pub async fn execute_teardown_plan_nats(nats_client: &async_nats::Client, plan: &TeardownPlan) {
        use crate::nats_transport;

        let timeout = std::time::Duration::from_secs(10);

        for (name, deployment) in &plan.tasks {
            let subject = nats_transport::subject_cmd(&deployment.worker_id.0, "undeploy");
            let body = serde_json::json!({
                "pipeline_id": deployment.pipeline_id,
            });

            match nats_transport::nats_request::<_, serde_json::Value>(
                nats_client,
                &subject,
                &body,
                timeout,
            )
            .await
            {
                Ok(_) => info!(
                    "Torn down pipeline '{}' from worker {} via NATS",
                    name, deployment.worker_id
                ),
                Err(e) => warn!(
                    "Failed to tear down pipeline '{}' from worker {}: {}",
                    name, deployment.worker_id, e
                ),
            }
        }
    }

    /// Execute inject event via NATS request/reply.
    #[cfg(feature = "nats-transport")]
    pub async fn execute_inject_event_nats(
        nats_client: &async_nats::Client,
        target: &InjectTarget,
        event: &InjectEventRequest,
    ) -> Result<InjectResponse, ClusterError> {
        use crate::nats_transport;

        let subject = nats_transport::subject_cmd(&target.worker_id, "inject");
        let body = serde_json::json!({
            "pipeline_id": target.target_name,
            "event_type": event.event_type,
            "fields": event.fields,
        });

        let timeout = std::time::Duration::from_secs(10);
        let worker_response: serde_json::Value =
            nats_transport::nats_request(nats_client, &subject, &body, timeout)
                .await
                .map_err(|e| ClusterError::RoutingFailed(e.to_string()))?;

        Ok(InjectResponse {
            routed_to: target.target_name.clone(),
            worker_id: target.worker_id.clone(),
            worker_response,
        })
    }

    /// Convenience wrapper: resolve + execute inject event in one call.
    /// Used by tests and internal code that has direct coordinator access.
    #[tracing::instrument(skip(self))]
    pub async fn inject_event(
        &self,
        group_id: &str,
        event: InjectEventRequest,
    ) -> Result<InjectResponse, ClusterError> {
        let target = self.resolve_inject_target(group_id, &event)?;
        Self::execute_inject_event(&self.http_client, &target, &event).await
    }

    /// Convenience wrapper: plan + commit teardown in one call (skips HTTP phase).
    /// Used by tests and internal code that has direct coordinator access.
    #[tracing::instrument(skip(self))]
    pub async fn teardown_group(&mut self, group_id: &str) -> Result<(), ClusterError> {
        let plan = self.plan_teardown_group(group_id)?;
        Self::execute_teardown_plan(&self.http_client, &plan).await;
        self.commit_teardown_group(&plan);
        Ok(())
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
        use std::time::Instant;

        use varpulis_runtime::event_file::EventFileParser;

        use crate::routing::find_target_pipeline;

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
}
