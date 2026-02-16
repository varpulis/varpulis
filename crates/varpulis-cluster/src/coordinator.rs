//! Coordinator state machine: worker registry, pipeline group management, event routing.

use crate::connector_config::{self, ClusterConnector};
use crate::health::{self, HealthSweepResult};
use crate::metrics::ClusterPrometheusMetrics;
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

#[cfg(feature = "raft")]
use std::sync::Arc;

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
    #[serde(default)]
    pub connector_health: Vec<crate::worker::ConnectorHealth>,
}

/// Scaling policy configuration for automatic scale recommendations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPolicy {
    pub min_workers: usize,
    pub max_workers: usize,
    /// Average pipelines-per-worker above which to recommend scale-up.
    pub scale_up_threshold: f64,
    /// Average pipelines-per-worker below which to recommend scale-down.
    pub scale_down_threshold: f64,
    /// Minimum seconds between webhook fires.
    pub cooldown_secs: u64,
    /// Optional URL to POST scaling recommendations to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,
}

/// A scaling recommendation produced by the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingRecommendation {
    pub action: ScalingAction,
    pub current_workers: usize,
    pub target_workers: usize,
    pub reason: String,
    pub avg_pipelines_per_worker: f64,
    pub total_pipelines: usize,
    pub timestamp: String,
}

/// The scaling action recommended by the coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalingAction {
    ScaleUp,
    ScaleDown,
    Stable,
}

/// Handle for Raft consensus integration.
#[cfg(feature = "raft")]
pub struct RaftHandle {
    /// The Raft instance for client_write and leadership queries.
    pub raft: Arc<crate::raft::VarpulisRaft>,
    /// Shared read-only view of the replicated state (updated after Raft applies).
    pub store_state: crate::raft::store::SharedCoordinatorState,
    /// Mapping of Raft NodeId → HTTP address for leader forwarding.
    pub peer_addrs: std::collections::BTreeMap<u64, String>,
    /// Admin API key for forwarding requests to the leader.
    pub admin_key: Option<String>,
}

// =========================================================================
// Deployment plan/result types for lock-free HTTP I/O
// =========================================================================

/// A single pipeline deploy task (produced during planning phase).
#[derive(Debug, Clone)]
pub struct DeployTask {
    pub replica_name: String,
    pub pipeline_name: String,
    pub worker_id: WorkerId,
    pub worker_address: String,
    pub worker_api_key: String,
    pub source: String,
    pub replica_count: usize,
}

/// Result of executing a single deploy task.
#[derive(Debug)]
pub struct DeployTaskResult {
    pub replica_name: String,
    pub pipeline_name: String,
    pub worker_id: WorkerId,
    pub worker_address: String,
    pub worker_api_key: String,
    pub replica_count: usize,
    pub outcome: Result<DeployResponse, String>,
}

/// Deployment plan built during the planning phase.
#[derive(Debug)]
pub struct DeployGroupPlan {
    pub group_id: String,
    pub spec: PipelineGroupSpec,
    pub tasks: Vec<DeployTask>,
    pub deploy_start: Instant,
}

/// Migration plan built during the planning phase.
#[derive(Debug, Clone)]
pub struct MigratePipelinePlan {
    pub migration_id: String,
    pub pipeline_name: String,
    pub group_id: String,
    pub source_worker_id: WorkerId,
    pub target_worker_id: WorkerId,
    pub target_address: String,
    pub target_api_key: String,
    pub deployment: PipelineDeployment,
    pub vpl_source: String,
    pub reason: MigrationReason,
    pub migrate_start: Instant,
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
    /// Result of the last health sweep (for API exposure).
    pub last_health_sweep: Option<HealthSweepResult>,
    /// Scaling policy (None = auto-scaling disabled).
    pub scaling_policy: Option<ScalingPolicy>,
    /// Most recent scaling recommendation.
    pub last_scaling_recommendation: Option<ScalingRecommendation>,
    /// When the last webhook was fired (for cooldown).
    last_scaling_webhook: Option<Instant>,
    /// Configurable heartbeat interval (used by health sweep loop).
    pub heartbeat_interval: Duration,
    /// Configurable heartbeat timeout (used by health sweep).
    pub heartbeat_timeout: Duration,
    placement: Box<dyn PlacementStrategy>,
    pub(crate) http_client: reqwest::Client,
    /// HA role of this coordinator instance.
    pub ha_role: HaRole,
    /// Optional Raft consensus handle (enabled with `raft` feature).
    #[cfg(feature = "raft")]
    pub raft_handle: Option<RaftHandle>,
    /// Prometheus metrics for cluster operations.
    pub cluster_metrics: ClusterPrometheusMetrics,
    /// Model registry (name -> metadata).
    pub model_registry: HashMap<String, crate::model_registry::ModelRegistryEntry>,
    /// LLM configuration for AI chat assistant.
    pub llm_config: Option<crate::chat::LlmConfig>,
}

/// The HA role of this coordinator (re-exported from ha module for non-k8s builds).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HaRole {
    #[default]
    Standalone,
    Leader,
    Follower {
        leader_id: String,
    },
}

impl HaRole {
    pub fn is_writer(&self) -> bool {
        matches!(self, HaRole::Standalone | HaRole::Leader)
    }
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
            last_health_sweep: None,
            scaling_policy: None,
            last_scaling_recommendation: None,
            last_scaling_webhook: None,
            heartbeat_interval: health::DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: health::DEFAULT_HEARTBEAT_TIMEOUT,
            placement: Box::new(RoundRobinPlacement::new()),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to build HTTP client"),
            ha_role: HaRole::default(),
            #[cfg(feature = "raft")]
            raft_handle: None,
            cluster_metrics: ClusterPrometheusMetrics::new(),
            model_registry: HashMap::new(),
            llm_config: None,
        }
    }

    /// Create a coordinator with a Raft consensus handle for cluster mode.
    #[cfg(feature = "raft")]
    pub fn with_raft(
        raft: Arc<crate::raft::VarpulisRaft>,
        store_state: crate::raft::store::SharedCoordinatorState,
        peer_addrs: std::collections::BTreeMap<u64, String>,
        admin_key: Option<String>,
    ) -> Self {
        let mut coord = Self::new();
        coord.raft_handle = Some(RaftHandle {
            raft,
            store_state,
            peer_addrs,
            admin_key,
        });
        coord
    }

    /// Replicate a command through Raft consensus.
    ///
    /// In standalone mode (no Raft), this is a no-op.
    /// In Raft mode, forwards to the leader. Returns `NotLeader` if this node
    /// is not the leader.
    #[cfg(feature = "raft")]
    pub async fn raft_replicate(
        &self,
        cmd: crate::raft::ClusterCommand,
    ) -> Result<(), ClusterError> {
        if let Some(ref handle) = self.raft_handle {
            handle.raft.client_write(cmd).await.map_err(|e| {
                // Extract leader address for ForwardToLeader errors
                let leader_info = format!("{}", e);
                ClusterError::NotLeader(leader_info)
            })?;
        }
        Ok(())
    }

    /// Check if this coordinator is the Raft leader (or standalone).
    #[cfg(feature = "raft")]
    pub fn is_raft_leader(&self) -> bool {
        match &self.raft_handle {
            None => true, // standalone = always leader
            Some(handle) => {
                let metrics = handle.raft.metrics().borrow().clone();
                metrics.current_leader == Some(metrics.id)
            }
        }
    }

    /// Get the Raft leader's HTTP address, if known.
    #[cfg(feature = "raft")]
    pub fn raft_leader_addr(&self) -> Option<String> {
        let handle = self.raft_handle.as_ref()?;
        let metrics = handle.raft.metrics().borrow().clone();
        let leader_id = metrics.current_leader?;
        if leader_id == metrics.id {
            return None; // We ARE the leader
        }
        handle.peer_addrs.get(&leader_id).cloned()
    }

    /// Synchronize local coordinator state from the Raft state machine.
    ///
    /// Called when a node becomes the new leader after an election,
    /// or periodically on followers for read-only API responses.
    #[cfg(feature = "raft")]
    pub fn sync_from_raft(&mut self) {
        let Some(ref handle) = self.raft_handle else {
            return;
        };

        let raft_state = handle.store_state.read().unwrap_or_else(|e| e.into_inner());

        // Sync workers: merge Raft state with local workers.
        // Preserve last_heartbeat for workers that already exist locally
        // (they may be receiving heartbeats from directly-connected workers).
        // Trust Raft for status (unhealthy from another coordinator's detection).
        let raft_worker_ids: std::collections::HashSet<WorkerId> = raft_state
            .workers
            .keys()
            .map(|k| WorkerId(k.clone()))
            .collect();

        for (id, entry) in &raft_state.workers {
            let wid = WorkerId(id.clone());
            let raft_status = match entry.status.as_str() {
                "ready" => WorkerStatus::Ready,
                "unhealthy" => WorkerStatus::Unhealthy,
                "draining" => WorkerStatus::Draining,
                "registering" => WorkerStatus::Registering,
                _ => WorkerStatus::Ready,
            };

            if let Some(local) = self.workers.get_mut(&wid) {
                // Existing worker: update from Raft but preserve local heartbeat
                local.assigned_pipelines = entry.assigned_pipelines.clone();
                local.events_processed = entry.events_processed;
                local.capacity = crate::worker::WorkerCapacity {
                    cpu_cores: entry.cpu_cores,
                    pipelines_running: entry.pipelines_running,
                    max_pipelines: entry.max_pipelines,
                };
                // Trust Raft status for cross-coordinator transitions (e.g., unhealthy).
                // If Raft says a worker is ready, refresh last_heartbeat — this acts
                // as a heartbeat proxy for workers connected to other coordinators.
                match raft_status {
                    WorkerStatus::Unhealthy | WorkerStatus::Draining => {
                        local.status = raft_status;
                    }
                    WorkerStatus::Ready => {
                        local.last_heartbeat = Instant::now();
                    }
                    _ => {}
                }
            } else {
                // New worker from Raft (registered on another coordinator)
                let worker = WorkerNode {
                    id: wid.clone(),
                    address: entry.address.clone(),
                    api_key: entry.api_key.clone(),
                    status: raft_status,
                    capacity: crate::worker::WorkerCapacity {
                        cpu_cores: entry.cpu_cores,
                        pipelines_running: entry.pipelines_running,
                        max_pipelines: entry.max_pipelines,
                    },
                    last_heartbeat: Instant::now(),
                    assigned_pipelines: entry.assigned_pipelines.clone(),
                    events_processed: entry.events_processed,
                    ws_disconnected_at: None,
                };
                self.workers.insert(wid, worker);
            }
        }

        // Remove workers that were deregistered in Raft
        self.workers.retain(|id, _| raft_worker_ids.contains(id));

        // Sync pipeline groups: deserialize from serde_json::Value
        self.pipeline_groups = raft_state
            .pipeline_groups
            .iter()
            .filter_map(|(name, val)| {
                serde_json::from_value(val.clone())
                    .ok()
                    .map(|group| (name.clone(), group))
            })
            .collect();

        // Sync connectors: same type, direct clone
        self.connectors = raft_state.connectors.clone();

        // Sync scaling policy: deserialize from serde_json::Value
        self.scaling_policy = raft_state
            .scaling_policy
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        tracing::debug!(
            "Synced from Raft state: {} workers, {} groups, {} connectors",
            self.workers.len(),
            self.pipeline_groups.len(),
            self.connectors.len()
        );
    }

    /// Update the HA role based on current Raft metrics.
    #[cfg(feature = "raft")]
    pub fn update_raft_role(&mut self) {
        let Some(ref handle) = self.raft_handle else {
            return;
        };

        let metrics = handle.raft.metrics().borrow().clone();
        let is_leader = metrics.current_leader == Some(metrics.id);

        self.ha_role = if is_leader {
            HaRole::Leader
        } else {
            let leader_id = metrics
                .current_leader
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            HaRole::Follower { leader_id }
        };
    }

    /// Get a reference to the shared HTTP client (for lock-free deploy phases).
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    /// Check if this coordinator is allowed to perform write operations.
    pub fn require_writer(&self) -> Result<(), crate::ClusterError> {
        if self.ha_role.is_writer() {
            Ok(())
        } else {
            let leader = match &self.ha_role {
                HaRole::Follower { leader_id } => leader_id.clone(),
                _ => "unknown".to_string(),
            };
            Err(crate::ClusterError::NotLeader(leader))
        }
    }

    /// Register a worker node. Marks it Ready immediately.
    pub fn register_worker(&mut self, mut node: WorkerNode) -> WorkerId {
        let id = node.id.clone();
        node.status = WorkerStatus::Ready;
        info!(worker_id = %id, address = %node.address, "Worker registered");
        self.workers.insert(id.clone(), node);
        self.update_metrics_counts();
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
        worker.ws_disconnected_at = None; // Clear WS grace period on heartbeat
        worker.capacity.pipelines_running = hb.pipelines_running;
        worker.events_processed = hb.events_processed;

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
        info!(worker_id = %worker_id, "Worker deregistered");
        Ok(())
    }

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
                    worker_api_key: worker.api_key.clone(),
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
    /// This method does NOT require `&self` — it uses a shared HTTP client
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
    /// Phase 1: Plan teardown — collect deployment info under read lock.
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

    /// Phase 2: Execute teardown HTTP calls — no lock held.
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

    /// Phase 3: Commit teardown — update state under write lock.
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
    /// Resolve the target for an inject event (Phase 1 — read lock).
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

    /// Execute inject event HTTP call (Phase 2 — no lock).
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

    /// Convenience wrapper: resolve + execute inject event in one call.
    /// Used by tests and internal code that has direct coordinator access.
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

    /// Run a health sweep, store the result, and return it.
    pub fn health_sweep(&mut self) -> HealthSweepResult {
        let start = Instant::now();
        let timeout = self.heartbeat_timeout;
        let result = health::health_sweep(&mut self.workers, timeout);
        self.last_health_sweep = Some(HealthSweepResult {
            workers_checked: result.workers_checked,
            workers_marked_unhealthy: result.workers_marked_unhealthy.clone(),
        });
        self.cluster_metrics
            .record_health_sweep(result.workers_checked, start.elapsed().as_secs_f64());
        if !result.workers_marked_unhealthy.is_empty() {
            self.update_metrics_counts();
        }
        result
    }

    /// Reconcile pipeline placements: re-deploy pipelines to workers that
    /// restarted and lost their in-memory state.  Called from the health-sweep
    /// loop on the leader when `pending_rebalance` is true.
    pub async fn reconcile_placements(&mut self) -> usize {
        // Collect (group_id, pipeline_name, worker_id, worker_addr, api_key, source)
        // for placements where the worker is available but doesn't list the pipeline.
        let mut to_redeploy: Vec<(String, String, WorkerId, String, String, String)> = Vec::new();

        for (gid, group) in &self.pipeline_groups {
            if group.status != GroupStatus::Running {
                continue;
            }
            for (pname, dep) in &group.placements {
                if dep.status != PipelineDeploymentStatus::Running {
                    continue;
                }
                let worker = match self.workers.get(&dep.worker_id) {
                    Some(w) if w.is_available() => w,
                    _ => continue,
                };
                // If the worker's assigned_pipelines already lists this pipeline,
                // the placement is healthy — nothing to do.
                if worker.assigned_pipelines.contains(&pname.to_string()) {
                    continue;
                }
                // Resolve source: strip replica suffix to find pipeline spec
                let logical = pname
                    .rsplit_once('#')
                    .map(|(base, _)| base)
                    .unwrap_or(pname);
                let source = group
                    .spec
                    .pipelines
                    .iter()
                    .find(|p| p.name == logical)
                    .map(|p| {
                        let (enriched, _) =
                            connector_config::inject_connectors(&p.source, &self.connectors);
                        enriched
                    });
                if let Some(src) = source {
                    to_redeploy.push((
                        gid.clone(),
                        pname.clone(),
                        dep.worker_id.clone(),
                        worker.address.clone(),
                        worker.api_key.clone(),
                        src,
                    ));
                }
            }
        }

        if to_redeploy.is_empty() {
            return 0;
        }

        info!(
            "Reconciling {} stale placement(s) — re-deploying pipelines",
            to_redeploy.len()
        );

        let mut redeployed = 0usize;
        let mut updated_workers: std::collections::HashSet<WorkerId> =
            std::collections::HashSet::new();

        for (gid, pname, worker_id, worker_addr, api_key, source) in to_redeploy {
            let url = format!("{}/api/v1/pipelines", worker_addr);
            let body = serde_json::json!({
                "name": pname,
                "source": source,
            });

            match self
                .http_client
                .post(&url)
                .header("x-api-key", &api_key)
                .json(&body)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!(
                        "Reconciled pipeline '{}' on worker {} (group {})",
                        pname, worker_id, gid
                    );
                    // Update worker's assigned_pipelines
                    if let Some(w) = self.workers.get_mut(&worker_id) {
                        w.assigned_pipelines.push(pname.clone());
                        w.capacity.pipelines_running += 1;
                    }
                    updated_workers.insert(worker_id);
                    redeployed += 1;
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    error!(
                        "Reconcile failed for pipeline '{}' on worker {}: HTTP {} - {}",
                        pname, worker_id, status, body
                    );
                }
                Err(e) => {
                    error!(
                        "Reconcile failed — cannot reach worker {} for pipeline '{}': {}",
                        worker_id, pname, e
                    );
                }
            }
        }

        // Propagate updated assigned_pipelines to Raft so sync_from_raft
        // doesn't overwrite them with stale empty values.
        #[cfg(feature = "raft")]
        for wid in &updated_workers {
            if let Some(w) = self.workers.get(wid) {
                let cmd = crate::raft::ClusterCommand::WorkerPipelinesUpdated {
                    id: wid.0.clone(),
                    assigned_pipelines: w.assigned_pipelines.clone(),
                };
                if let Err(e) = self.raft_replicate(cmd).await {
                    warn!("Failed to replicate reconciled pipelines for {wid} to Raft: {e}");
                }
            }
        }

        redeployed
    }

    // =========================================================================
    // Migration & Failover
    // =========================================================================

    /// Phase 1: Build a migration plan without performing any HTTP I/O.
    pub fn plan_migrate_pipeline(
        &self,
        pipeline_name: &str,
        group_id: &str,
        target_worker_id: &WorkerId,
        reason: MigrationReason,
    ) -> Result<MigratePipelinePlan, ClusterError> {
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

        Ok(MigratePipelinePlan {
            migration_id: uuid::Uuid::new_v4().to_string(),
            pipeline_name: pipeline_name.to_string(),
            group_id: group_id.to_string(),
            source_worker_id,
            target_worker_id: target_worker_id.clone(),
            target_address,
            target_api_key,
            deployment,
            vpl_source,
            reason,
            migrate_start: Instant::now(),
        })
    }

    /// Phase 3: Commit migration results to coordinator state.
    pub fn commit_migrate_pipeline(
        &mut self,
        plan: &MigratePipelinePlan,
        new_pipeline_id: &str,
        success: bool,
        failure_reason: Option<String>,
    ) -> String {
        if success {
            // Update placements
            if let Some(group) = self.pipeline_groups.get_mut(&plan.group_id) {
                let new_epoch = group
                    .placements
                    .get(&plan.pipeline_name)
                    .map(|d| d.epoch + 1)
                    .unwrap_or(1);
                group.placements.insert(
                    plan.pipeline_name.clone(),
                    PipelineDeployment {
                        worker_id: plan.target_worker_id.clone(),
                        worker_address: plan.target_address.clone(),
                        worker_api_key: plan.target_api_key.clone(),
                        pipeline_id: new_pipeline_id.to_string(),
                        status: PipelineDeploymentStatus::Running,
                        epoch: new_epoch,
                    },
                );
                group.update_status();
            }

            // Update worker bookkeeping
            if let Some(w) = self.workers.get_mut(&plan.target_worker_id) {
                w.assigned_pipelines.push(plan.pipeline_name.clone());
                w.capacity.pipelines_running += 1;
            }
            if let Some(w) = self.workers.get_mut(&plan.source_worker_id) {
                w.assigned_pipelines.retain(|p| p != &plan.pipeline_name);
                w.capacity.pipelines_running = w.capacity.pipelines_running.saturating_sub(1);
            }

            let task = MigrationTask {
                id: plan.migration_id.clone(),
                pipeline_name: plan.pipeline_name.clone(),
                group_id: plan.group_id.clone(),
                source_worker: plan.source_worker_id.clone(),
                target_worker: plan.target_worker_id.clone(),
                status: MigrationStatus::Completed,
                started_at: plan.migrate_start,
                checkpoint: None,
                reason: plan.reason.clone(),
            };
            self.active_migrations
                .insert(plan.migration_id.clone(), task);

            self.cluster_metrics
                .record_migration(true, plan.migrate_start.elapsed().as_secs_f64());
            self.update_metrics_counts();

            info!(
                pipeline = %plan.pipeline_name,
                from = %plan.source_worker_id,
                to = %plan.target_worker_id,
                "Migration complete"
            );
        } else {
            let reason = failure_reason.unwrap_or_else(|| "unknown".to_string());
            let task = MigrationTask {
                id: plan.migration_id.clone(),
                pipeline_name: plan.pipeline_name.clone(),
                group_id: plan.group_id.clone(),
                source_worker: plan.source_worker_id.clone(),
                target_worker: plan.target_worker_id.clone(),
                status: MigrationStatus::Failed(reason),
                started_at: plan.migrate_start,
                checkpoint: None,
                reason: plan.reason.clone(),
            };
            self.active_migrations
                .insert(plan.migration_id.clone(), task);

            self.cluster_metrics
                .record_migration(false, plan.migrate_start.elapsed().as_secs_f64());
        }

        plan.migration_id.clone()
    }

    /// Execute migration HTTP steps (no coordinator lock needed).
    ///
    /// Returns `Ok(new_pipeline_id)` on success, or `Err(reason)` on failure.
    pub async fn execute_migrate_plan(
        http_client: &reqwest::Client,
        plan: &MigratePipelinePlan,
        source_alive: bool,
        connectors: &HashMap<String, ClusterConnector>,
    ) -> Result<String, String> {
        // Step 1: Checkpoint (best-effort — skip if source is dead)
        let checkpoint = if source_alive {
            let checkpoint_url = format!(
                "{}/api/v1/pipelines/{}/checkpoint",
                plan.deployment.worker_address, plan.deployment.pipeline_id
            );
            match http_client
                .post(&checkpoint_url)
                .header("x-api-key", &plan.deployment.worker_api_key)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<CheckpointResponsePayload>().await {
                        Ok(cp_resp) => {
                            info!(
                                "Checkpoint captured for pipeline '{}' (id={}, {} events)",
                                plan.pipeline_name, cp_resp.pipeline_id, cp_resp.events_processed
                            );
                            Some(cp_resp.checkpoint)
                        }
                        Err(e) => {
                            warn!(
                                "Failed to deserialize checkpoint for '{}': {}",
                                plan.pipeline_name, e
                            );
                            None
                        }
                    }
                }
                Ok(resp) => {
                    warn!(
                        "Checkpoint HTTP error for '{}': {}",
                        plan.pipeline_name,
                        resp.status()
                    );
                    None
                }
                Err(e) => {
                    warn!(
                        "Checkpoint request failed for '{}': {}",
                        plan.pipeline_name, e
                    );
                    None
                }
            }
        } else {
            info!(
                "Source worker {} is dead, proceeding without checkpoint for '{}'",
                plan.source_worker_id, plan.pipeline_name
            );
            None
        };

        // Step 2: Deploy to target worker
        let (enriched_source, _) =
            crate::connector_config::inject_connectors(&plan.vpl_source, connectors);

        let deploy_url = format!("{}/api/v1/pipelines", plan.target_address);
        let deploy_body = serde_json::json!({
            "name": plan.pipeline_name,
            "source": enriched_source,
        });

        let new_pipeline_id = match http_client
            .post(&deploy_url)
            .header("x-api-key", &plan.target_api_key)
            .json(&deploy_body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                let resp_body: DeployResponse = resp
                    .json()
                    .await
                    .map_err(|e| format!("Failed to parse deploy response: {}", e))?;
                info!(
                    "Migration deploy: '{}' on target {} (id={}, status={})",
                    resp_body.name, plan.target_worker_id, resp_body.id, resp_body.status
                );
                resp_body.id
            }
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                return Err(format!("Deploy to target failed: {}", body));
            }
            Err(e) => {
                return Err(format!("Deploy request failed: {}", e));
            }
        };

        // Step 3: Restore checkpoint on target (best-effort)
        if let Some(ref cp) = checkpoint {
            let restore_url = format!(
                "{}/api/v1/pipelines/{}/restore",
                plan.target_address, new_pipeline_id
            );
            let restore_body = serde_json::json!({ "checkpoint": cp });

            match http_client
                .post(&restore_url)
                .header("x-api-key", &plan.target_api_key)
                .json(&restore_body)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!(
                        "Checkpoint restored for pipeline '{}' on worker {}",
                        plan.pipeline_name, plan.target_worker_id
                    );
                }
                Ok(resp) => {
                    let body = resp.text().await.unwrap_or_default();
                    warn!(
                        "Restore failed for '{}' (continuing without state): {}",
                        plan.pipeline_name, body
                    );
                }
                Err(e) => {
                    warn!(
                        "Restore request failed for '{}' (continuing without state): {}",
                        plan.pipeline_name, e
                    );
                }
            }
        }

        // Step 4: Cleanup — remove pipeline from source (skip if dead)
        if source_alive && !plan.deployment.pipeline_id.is_empty() {
            let delete_url = format!(
                "{}/api/v1/pipelines/{}",
                plan.deployment.worker_address, plan.deployment.pipeline_id
            );
            match http_client
                .delete(&delete_url)
                .header("x-api-key", &plan.deployment.worker_api_key)
                .send()
                .await
            {
                Ok(_) => {
                    info!(
                        "Removed old pipeline '{}' from worker {}",
                        plan.pipeline_name, plan.source_worker_id
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to remove old pipeline '{}' from {}: {}",
                        plan.pipeline_name, plan.source_worker_id, e
                    );
                }
            }
        }

        Ok(new_pipeline_id)
    }

    /// Migrate a pipeline from its current worker to a target worker.
    ///
    /// Steps: checkpoint (if source alive) → deploy → restore → switch → cleanup.
    #[tracing::instrument(skip(self), fields(pipeline = %pipeline_name, group = %group_id, target = %target_worker_id))]
    pub async fn migrate_pipeline(
        &mut self,
        pipeline_name: &str,
        group_id: &str,
        target_worker_id: &WorkerId,
        reason: MigrationReason,
    ) -> Result<String, ClusterError> {
        let migrate_start = Instant::now();
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
                                "Checkpoint captured for pipeline '{}' (id={}, {} events)",
                                pipeline_name, cp_resp.pipeline_id, cp_resp.events_processed
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
                info!(
                    "Migration deploy: '{}' on target {} (id={}, status={})",
                    resp_body.name, target_worker_id, resp_body.id, resp_body.status
                );
                resp_body.id
            }
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                task.status = MigrationStatus::Failed(format!("Deploy failed: {}", body));
                self.active_migrations.insert(migration_id.clone(), task);
                self.cluster_metrics
                    .record_migration(false, migrate_start.elapsed().as_secs_f64());
                return Err(ClusterError::MigrationFailed(format!(
                    "Deploy to target failed: {}",
                    body
                )));
            }
            Err(e) => {
                task.status = MigrationStatus::Failed(format!("Deploy request failed: {}", e));
                self.active_migrations.insert(migration_id.clone(), task);
                self.cluster_metrics
                    .record_migration(false, migrate_start.elapsed().as_secs_f64());
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
            let new_epoch = group
                .placements
                .get(pipeline_name)
                .map(|d| d.epoch + 1)
                .unwrap_or(1);
            group.placements.insert(
                pipeline_name.to_string(),
                PipelineDeployment {
                    worker_id: target_worker_id.clone(),
                    worker_address: target_address,
                    worker_api_key: target_api_key,
                    pipeline_id: new_pipeline_id,
                    status: PipelineDeploymentStatus::Running,
                    epoch: new_epoch,
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

        self.cluster_metrics
            .record_migration(true, migrate_start.elapsed().as_secs_f64());
        self.update_metrics_counts();

        info!(
            "Migration complete: pipeline '{}' moved from {} to {}",
            pipeline_name, source_worker_id, target_worker_id
        );

        Ok(migration_id)
    }

    /// Handle a worker failure: migrate all its pipelines to healthy workers.
    #[tracing::instrument(skip(self), fields(worker_id = %worker_id))]
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

        warn!(
            worker_id = %worker_id,
            pipelines_affected = affected.len(),
            "Worker failure detected"
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

            // Sort by throughput (highest first) using worker_metrics so hot
            // pipelines are moved first for maximum load relief.
            let worker_pipeline_metrics = self.worker_metrics.get(wid);
            movable.sort_by(|a, b| {
                let throughput = |pname: &str| -> u64 {
                    worker_pipeline_metrics
                        .and_then(|metrics| {
                            metrics
                                .iter()
                                .find(|m| m.pipeline_name == pname)
                                .map(|m| m.events_in)
                        })
                        .unwrap_or(0)
                };
                throughput(&b.1).cmp(&throughput(&a.1))
            });

            for (gid, pname) in movable.into_iter().take(excess) {
                // Find least-loaded target
                let target = available_workers
                    .iter()
                    .filter(|w| *w != wid)
                    .min_by_key(|w| worker_load.get(w).unwrap_or(&0));

                if let Some(target_id) = target {
                    migrations_to_do.push((gid, pname, target_id.clone()));
                    // Adjust virtual load for next iteration
                    if let Some(v) = worker_load.get_mut(wid) {
                        *v -= 1;
                    }
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
                    connector_health: m.connector_health.clone(),
                });
            }
        }
        ClusterMetrics { pipelines }
    }

    /// Check connector health across all workers.
    ///
    /// Returns list of `(pipeline_name, worker_id, connector_name)` for connectors
    /// that are disconnected and haven't received a message in over 60 seconds.
    pub fn check_connector_health(&self) -> Vec<(String, WorkerId, String)> {
        let mut unhealthy = Vec::new();
        for (worker_id, metrics) in &self.worker_metrics {
            for m in metrics {
                for ch in &m.connector_health {
                    if !ch.connected && ch.seconds_since_last_message > 60 {
                        unhealthy.push((
                            m.pipeline_name.clone(),
                            worker_id.clone(),
                            ch.connector_name.clone(),
                        ));
                    }
                }
            }
        }
        unhealthy
    }

    // =========================================================================
    // Auto-Scaling
    // =========================================================================

    /// Evaluate current cluster load and produce a scaling recommendation.
    ///
    /// Returns `None` if no scaling policy is configured.
    pub fn evaluate_scaling(&mut self) -> Option<ScalingRecommendation> {
        let policy = self.scaling_policy.as_ref()?;

        let healthy_workers = self
            .workers
            .values()
            .filter(|w| w.status == WorkerStatus::Ready)
            .count();

        let total_pipelines: usize = self
            .pipeline_groups
            .values()
            .map(|g| g.placements.len())
            .sum();

        let avg_load = if healthy_workers > 0 {
            total_pipelines as f64 / healthy_workers as f64
        } else {
            0.0
        };

        let (action, target, reason) = if healthy_workers < policy.min_workers {
            (
                ScalingAction::ScaleUp,
                policy.min_workers,
                format!(
                    "Below minimum workers: {} < {}",
                    healthy_workers, policy.min_workers
                ),
            )
        } else if avg_load > policy.scale_up_threshold && healthy_workers < policy.max_workers {
            let needed = (total_pipelines as f64 / policy.scale_up_threshold).ceil() as usize;
            let target = needed.min(policy.max_workers);
            (
                ScalingAction::ScaleUp,
                target,
                format!(
                    "Load {:.1} exceeds threshold {:.1}",
                    avg_load, policy.scale_up_threshold
                ),
            )
        } else if avg_load < policy.scale_down_threshold && healthy_workers > policy.min_workers {
            let needed = if total_pipelines > 0 {
                (total_pipelines as f64 / policy.scale_up_threshold).ceil() as usize
            } else {
                policy.min_workers
            };
            let target = needed.max(policy.min_workers);
            (
                ScalingAction::ScaleDown,
                target,
                format!(
                    "Load {:.1} below threshold {:.1}",
                    avg_load, policy.scale_down_threshold
                ),
            )
        } else {
            (
                ScalingAction::Stable,
                healthy_workers,
                "Load within thresholds".to_string(),
            )
        };

        let recommendation = ScalingRecommendation {
            action,
            current_workers: healthy_workers,
            target_workers: target,
            reason,
            avg_pipelines_per_worker: avg_load,
            total_pipelines,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        self.last_scaling_recommendation = Some(recommendation.clone());
        Some(recommendation)
    }

    /// POST the scaling recommendation to the configured webhook URL.
    ///
    /// Respects the cooldown period to avoid flooding the orchestrator.
    pub async fn fire_scaling_webhook(&mut self) {
        let (policy, recommendation) =
            match (&self.scaling_policy, &self.last_scaling_recommendation) {
                (Some(p), Some(r)) => (p.clone(), r.clone()),
                _ => return,
            };

        let webhook_url = match &policy.webhook_url {
            Some(url) => url.clone(),
            None => return,
        };

        // Only fire for non-stable actions
        if recommendation.action == ScalingAction::Stable {
            return;
        }

        // Respect cooldown
        if let Some(last_fire) = self.last_scaling_webhook {
            if last_fire.elapsed() < Duration::from_secs(policy.cooldown_secs) {
                return;
            }
        }

        match self
            .http_client
            .post(&webhook_url)
            .json(&recommendation)
            .send()
            .await
        {
            Ok(resp) => {
                info!(
                    "Scaling webhook fired ({:?}): HTTP {}",
                    recommendation.action,
                    resp.status()
                );
                self.last_scaling_webhook = Some(Instant::now());
            }
            Err(e) => {
                warn!("Scaling webhook failed: {}", e);
            }
        }
    }

    /// Update Prometheus gauge metrics from current coordinator state.
    fn update_metrics_counts(&self) {
        let (mut ready, mut unhealthy, mut draining) = (0usize, 0usize, 0usize);
        for w in self.workers.values() {
            match w.status {
                WorkerStatus::Ready => ready += 1,
                WorkerStatus::Unhealthy => unhealthy += 1,
                WorkerStatus::Draining => draining += 1,
                _ => {}
            }
        }
        self.cluster_metrics
            .set_worker_counts(ready, unhealthy, draining);

        let total_deployments: usize = self
            .pipeline_groups
            .values()
            .map(|g| g.placements.len())
            .sum();
        self.cluster_metrics
            .set_deployment_counts(self.pipeline_groups.len(), total_deployments);
    }
}

impl Default for Coordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Response from worker deploy API.
#[derive(Debug, Deserialize)]
pub struct DeployResponse {
    pub id: String,
    pub name: String,
    pub status: String,
}

/// Response from worker checkpoint API.
#[derive(Debug, Deserialize)]
struct CheckpointResponsePayload {
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

/// Plan for tearing down a pipeline group (extracted under read lock).
#[derive(Debug, Clone)]
pub struct TeardownPlan {
    pub group_id: String,
    pub tasks: Vec<(String, PipelineDeployment)>,
}

/// Resolved inject target (extracted under read lock, used without lock).
#[derive(Debug, Clone)]
pub struct InjectTarget {
    pub url: String,
    pub api_key: String,
    pub target_name: String,
    pub worker_id: String,
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
        let results = coord.handle_worker_failure(&WorkerId("w1".into())).await;
        assert!(results.is_empty());
    }

    #[test]
    fn test_heartbeat_stores_events_processed() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);
        assert_eq!(coord.workers[&WorkerId("w1".into())].events_processed, 0);

        let hb = HeartbeatRequest {
            events_processed: 42000,
            pipelines_running: 3,
            pipeline_metrics: vec![],
        };
        coord.heartbeat(&WorkerId("w1".into()), &hb).unwrap();
        assert_eq!(
            coord.workers[&WorkerId("w1".into())].events_processed,
            42000
        );
    }

    #[test]
    fn test_health_sweep_stores_last_result() {
        let mut coord = Coordinator::new();
        assert!(coord.last_health_sweep.is_none());

        // Register a healthy worker
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);

        let result = coord.health_sweep();
        assert_eq!(result.workers_checked, 1);
        assert!(result.workers_marked_unhealthy.is_empty());

        // Last sweep should be stored
        let stored = coord.last_health_sweep.as_ref().unwrap();
        assert_eq!(stored.workers_checked, 1);
        assert!(stored.workers_marked_unhealthy.is_empty());
    }

    #[test]
    fn test_health_sweep_stores_unhealthy_workers() {
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

        let stored = coord.last_health_sweep.as_ref().unwrap();
        assert_eq!(stored.workers_marked_unhealthy.len(), 1);
    }

    // =========================================================================
    // Tests for auto-scaling
    // =========================================================================

    fn make_scaling_policy() -> ScalingPolicy {
        ScalingPolicy {
            min_workers: 1,
            max_workers: 10,
            scale_up_threshold: 5.0,
            scale_down_threshold: 1.0,
            cooldown_secs: 60,
            webhook_url: None,
        }
    }

    #[test]
    fn test_evaluate_scaling_no_policy() {
        let mut coord = Coordinator::new();
        assert!(coord.evaluate_scaling().is_none());
    }

    #[test]
    fn test_evaluate_scaling_stable() {
        let mut coord = Coordinator::new();
        coord.scaling_policy = Some(make_scaling_policy());

        // 2 workers, 4 pipelines = avg 2.0 (between 1.0 and 5.0 = stable)
        for i in 0..2 {
            let node = WorkerNode::new(
                WorkerId(format!("w{}", i)),
                format!("http://localhost:900{}", i),
                "key".into(),
            );
            coord.register_worker(node);
        }
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![
                PipelinePlacement {
                    name: "p1".into(),
                    source: "x".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "x".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
            ],
            routes: vec![],
        };
        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        group.placements.insert(
            "p1".into(),
            crate::pipeline_group::PipelineDeployment {
                worker_id: WorkerId("w0".into()),
                worker_address: String::new(),
                worker_api_key: String::new(),
                pipeline_id: String::new(),
                status: crate::pipeline_group::PipelineDeploymentStatus::Running,
                epoch: 0,
            },
        );
        group.placements.insert(
            "p2".into(),
            crate::pipeline_group::PipelineDeployment {
                worker_id: WorkerId("w1".into()),
                worker_address: String::new(),
                worker_api_key: String::new(),
                pipeline_id: String::new(),
                status: crate::pipeline_group::PipelineDeploymentStatus::Running,
                epoch: 0,
            },
        );
        coord.pipeline_groups.insert("g1".into(), group);

        let rec = coord.evaluate_scaling().unwrap();
        assert_eq!(rec.action, ScalingAction::Stable);
        assert_eq!(rec.current_workers, 2);
    }

    #[test]
    fn test_evaluate_scaling_scale_up() {
        let mut coord = Coordinator::new();
        coord.scaling_policy = Some(make_scaling_policy());

        // 1 worker, 6 pipelines = avg 6.0 > threshold 5.0
        let node = WorkerNode::new(
            WorkerId("w0".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);

        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![],
            routes: vec![],
        };
        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        for i in 0..6 {
            group.placements.insert(
                format!("p{}", i),
                crate::pipeline_group::PipelineDeployment {
                    worker_id: WorkerId("w0".into()),
                    worker_address: String::new(),
                    worker_api_key: String::new(),
                    pipeline_id: String::new(),
                    status: crate::pipeline_group::PipelineDeploymentStatus::Running,
                    epoch: 0,
                },
            );
        }
        coord.pipeline_groups.insert("g1".into(), group);

        let rec = coord.evaluate_scaling().unwrap();
        assert_eq!(rec.action, ScalingAction::ScaleUp);
        assert!(rec.target_workers > 1);
    }

    #[test]
    fn test_evaluate_scaling_scale_down() {
        let mut coord = Coordinator::new();
        coord.scaling_policy = Some(ScalingPolicy {
            min_workers: 1,
            max_workers: 10,
            scale_up_threshold: 5.0,
            scale_down_threshold: 1.0,
            cooldown_secs: 60,
            webhook_url: None,
        });

        // 5 workers, 2 pipelines = avg 0.4 < threshold 1.0
        for i in 0..5 {
            let node = WorkerNode::new(
                WorkerId(format!("w{}", i)),
                format!("http://localhost:900{}", i),
                "key".into(),
            );
            coord.register_worker(node);
        }
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![],
            routes: vec![],
        };
        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        for i in 0..2 {
            group.placements.insert(
                format!("p{}", i),
                crate::pipeline_group::PipelineDeployment {
                    worker_id: WorkerId("w0".into()),
                    worker_address: String::new(),
                    worker_api_key: String::new(),
                    pipeline_id: String::new(),
                    status: crate::pipeline_group::PipelineDeploymentStatus::Running,
                    epoch: 0,
                },
            );
        }
        coord.pipeline_groups.insert("g1".into(), group);

        let rec = coord.evaluate_scaling().unwrap();
        assert_eq!(rec.action, ScalingAction::ScaleDown);
        assert!(rec.target_workers < 5);
        assert!(rec.target_workers >= 1); // must respect min
    }

    #[test]
    fn test_evaluate_scaling_below_min_workers() {
        let mut coord = Coordinator::new();
        coord.scaling_policy = Some(ScalingPolicy {
            min_workers: 3,
            max_workers: 10,
            scale_up_threshold: 5.0,
            scale_down_threshold: 1.0,
            cooldown_secs: 60,
            webhook_url: None,
        });

        // 1 worker, below min of 3
        let node = WorkerNode::new(
            WorkerId("w0".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);

        let rec = coord.evaluate_scaling().unwrap();
        assert_eq!(rec.action, ScalingAction::ScaleUp);
        assert_eq!(rec.target_workers, 3);
    }

    #[test]
    fn test_evaluate_scaling_respects_max_workers() {
        let mut coord = Coordinator::new();
        coord.scaling_policy = Some(ScalingPolicy {
            min_workers: 1,
            max_workers: 3,
            scale_up_threshold: 2.0,
            scale_down_threshold: 0.5,
            cooldown_secs: 60,
            webhook_url: None,
        });

        // 2 workers, 20 pipelines = avg 10.0 > threshold 2.0
        for i in 0..2 {
            let node = WorkerNode::new(
                WorkerId(format!("w{}", i)),
                format!("http://localhost:900{}", i),
                "key".into(),
            );
            coord.register_worker(node);
        }
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![],
            routes: vec![],
        };
        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        for i in 0..20 {
            group.placements.insert(
                format!("p{}", i),
                crate::pipeline_group::PipelineDeployment {
                    worker_id: WorkerId("w0".into()),
                    worker_address: String::new(),
                    worker_api_key: String::new(),
                    pipeline_id: String::new(),
                    status: crate::pipeline_group::PipelineDeploymentStatus::Running,
                    epoch: 0,
                },
            );
        }
        coord.pipeline_groups.insert("g1".into(), group);

        let rec = coord.evaluate_scaling().unwrap();
        assert_eq!(rec.action, ScalingAction::ScaleUp);
        assert!(rec.target_workers <= 3); // must respect max
    }

    #[test]
    fn test_scaling_recommendation_serde() {
        let rec = ScalingRecommendation {
            action: ScalingAction::ScaleUp,
            current_workers: 2,
            target_workers: 4,
            reason: "Load exceeded".into(),
            avg_pipelines_per_worker: 6.0,
            total_pipelines: 12,
            timestamp: "2026-02-12T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&rec).unwrap();
        let parsed: ScalingRecommendation = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, ScalingAction::ScaleUp);
        assert_eq!(parsed.current_workers, 2);
        assert_eq!(parsed.target_workers, 4);
    }

    #[test]
    fn test_check_connector_health() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);

        // Add metrics with unhealthy connector
        coord.worker_metrics.insert(
            WorkerId("w1".into()),
            vec![PipelineMetrics {
                pipeline_name: "p1".into(),
                events_in: 100,
                events_out: 50,
                connector_health: vec![crate::worker::ConnectorHealth {
                    connector_name: "mqtt_in".into(),
                    connector_type: "mqtt".into(),
                    connected: false,
                    last_error: Some("connection refused".into()),
                    messages_received: 0,
                    seconds_since_last_message: 120,
                }],
            }],
        );

        let unhealthy = coord.check_connector_health();
        assert_eq!(unhealthy.len(), 1);
        assert_eq!(unhealthy[0].0, "p1");
        assert_eq!(unhealthy[0].2, "mqtt_in");
    }

    #[test]
    fn test_check_connector_health_healthy() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        coord.register_worker(node);

        // Add metrics with healthy connector
        coord.worker_metrics.insert(
            WorkerId("w1".into()),
            vec![PipelineMetrics {
                pipeline_name: "p1".into(),
                events_in: 100,
                events_out: 50,
                connector_health: vec![crate::worker::ConnectorHealth {
                    connector_name: "mqtt_in".into(),
                    connector_type: "mqtt".into(),
                    connected: true,
                    last_error: None,
                    messages_received: 42,
                    seconds_since_last_message: 2,
                }],
            }],
        );

        let unhealthy = coord.check_connector_health();
        assert!(unhealthy.is_empty());
    }
}
