//! Coordinator state machine: worker registry, pipeline group management, event routing.

mod deployment;
#[cfg(feature = "distributed-checkpoint")]
pub mod distributed_checkpoint;
mod rebalance;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::connector_config::{self, ClusterConnector};
use crate::health::{self, HealthSweepResult};
use crate::metrics::ClusterPrometheusMetrics;
use crate::migration::{MigrationReason, MigrationTask};
use crate::pipeline_group::{DeployedPipelineGroup, PipelineDeployment, PipelineGroupSpec};
use crate::worker::{HeartbeatRequest, PipelineMetrics, WorkerId, WorkerNode, WorkerStatus};
use crate::{ClusterError, PlacementStrategy, RoundRobinPlacement};

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
    pub(crate) last_scaling_webhook: Option<Instant>,
    /// Configurable heartbeat interval (used by health sweep loop).
    pub heartbeat_interval: Duration,
    /// Configurable heartbeat timeout (used by health sweep).
    pub heartbeat_timeout: Duration,
    pub(crate) placement: Box<dyn PlacementStrategy>,
    pub(crate) http_client: reqwest::Client,
    /// Optional NATS client for worker communication (replaces HTTP when set).
    #[cfg(feature = "nats-transport")]
    pub nats_client: Option<async_nats::Client>,
    /// HA role of this coordinator instance.
    pub ha_role: HaRole,
    /// JetStream KV control plane, when `VARPULIS_CONTROL_PLANE_URL` selects
    /// it. `replicate` sends every replicated write here; without it a
    /// coordinator is standalone and its local state is the only copy.
    #[cfg(feature = "jetstream-control-plane")]
    pub control_plane: Option<crate::jetstream_control_plane::Applier>,
    /// This coordinator's hold on `control/leader`, when the control plane is
    /// in use. Ticked by the health sweep; it is what sets [`Coordinator::ha_role`]
    /// on a JetStream deployment, the way [`Coordinator::update_raft_role`]
    /// does on a Raft one.
    ///
    /// Without it `ha_role` stayed at its `Standalone` default, whose
    /// `is_writer()` is `true` — so every coordinator in the cluster ran the
    /// health sweep, the failovers and the placement reconciliation at the
    /// same time, against each other.
    #[cfg(feature = "jetstream-control-plane")]
    pub leader_lease: Option<crate::jetstream_control_plane::LeaderLease>,
    /// Prometheus metrics for cluster operations.
    pub cluster_metrics: ClusterPrometheusMetrics,
    /// Model registry (name -> metadata).
    pub model_registry: HashMap<String, crate::model_registry::ModelRegistryEntry>,
    /// LLM configuration for AI chat assistant.
    pub llm_config: Option<crate::chat::LlmConfig>,
    /// Multi-region federation coordinator (enabled with `federation` feature).
    #[cfg(feature = "federation")]
    pub federation: Option<crate::federation::FederationCoordinator>,
}

impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordinator")
            .field("workers", &self.workers)
            .field("pipeline_groups", &self.pipeline_groups)
            .field("connectors", &self.connectors)
            .field("worker_metrics", &self.worker_metrics)
            .field("active_migrations", &self.active_migrations)
            .field("pending_rebalance", &self.pending_rebalance)
            .field("last_health_sweep", &self.last_health_sweep)
            .field("scaling_policy", &self.scaling_policy)
            .field(
                "last_scaling_recommendation",
                &self.last_scaling_recommendation,
            )
            .field("last_scaling_webhook", &self.last_scaling_webhook)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("heartbeat_timeout", &self.heartbeat_timeout)
            .field("ha_role", &self.ha_role)
            .field("cluster_metrics", &self.cluster_metrics)
            .field("model_registry", &self.model_registry)
            .field("llm_config", &self.llm_config)
            .finish_non_exhaustive()
    }
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
            #[cfg(feature = "nats-transport")]
            nats_client: None,
            ha_role: HaRole::default(),
            #[cfg(feature = "jetstream-control-plane")]
            control_plane: None,
            #[cfg(feature = "jetstream-control-plane")]
            leader_lease: None,
            cluster_metrics: ClusterPrometheusMetrics::new(),
            model_registry: HashMap::new(),
            llm_config: None,
            #[cfg(feature = "federation")]
            federation: None,
        }
    }

    /// Replicate a control-plane command.
    ///
    /// One destination, chosen once: the JetStream KV control plane when
    /// `VARPULIS_CONTROL_PLANE_URL` configured one, and nowhere in standalone
    /// mode, where the local state is the only copy.
    ///
    /// Every replicated write in the cluster goes through here. It used to be
    /// bypassed by sixteen direct `raft.client_write(...)` calls, which is why
    /// the backend could not be swapped in one place — and why removing Raft
    /// had to route them all through one function first.
    #[tracing::instrument(skip(self))]
    pub async fn replicate(
        &self,
        cmd: crate::control_state::ClusterCommand,
    ) -> Result<(), ClusterError> {
        #[cfg(feature = "jetstream-control-plane")]
        if let Some(ref applier) = self.control_plane {
            applier.apply(cmd).await.map_err(|e| {
                ClusterError::InvalidOperation(format!("control plane write failed: {e}"))
            })?;
            return Ok(());
        }

        // Standalone: the in-memory state this call already mutated is the
        // only copy there is.
        let _ = cmd;
        Ok(())
    }

    /// Merge a materialised control-plane state value into local state.
    ///
    /// `Coordinator::sync_from_control_plane` produces the value from a
    /// bucket snapshot (`jetstream_control_plane::materialize`) and calls
    /// this. (A plain span, not a link: that method is behind
    /// `jetstream-control-plane`, and the doc build runs on default features,
    /// where a link to it cannot resolve.) It is kept separate so the merge can be tested against a state
    /// value without a broker, and so a future backend feeds the coordinator
    /// through exactly one function rather than forking the merge.
    pub fn sync_from_control_state(&mut self, state: &crate::control_state::CoordinatorState) {
        // Merge replicated workers with local ones.
        // Preserve last_heartbeat for workers that already exist locally
        // (they may be receiving heartbeats from directly-connected workers).
        // Trust the control plane for status (unhealthy from another coordinator's detection).
        let replicated_worker_ids: std::collections::HashSet<WorkerId> =
            state.workers.keys().map(|k| WorkerId(k.clone())).collect();

        for (id, entry) in &state.workers {
            let wid = WorkerId(id.clone());
            let replicated_status = match entry.status.as_str() {
                "ready" => WorkerStatus::Ready,
                "unhealthy" => WorkerStatus::Unhealthy,
                "draining" => WorkerStatus::Draining,
                "registering" => WorkerStatus::Registering,
                _ => WorkerStatus::Ready,
            };

            if let Some(local) = self.workers.get_mut(&wid) {
                // Existing worker: update structural fields from the control plane.
                local.assigned_pipelines = entry.assigned_pipelines.clone();
                local.capacity.cpu_cores = entry.cpu_cores;
                local.capacity.max_pipelines = entry.max_pipelines;
                // Heartbeat metrics are replicated via
                // WorkerMetricsUpdated, so use max(local, replicated) for
                // events_processed (monotonically increasing) and the replicated
                // value for pipelines_running (latest wins).
                if entry.events_processed > local.events_processed {
                    local.events_processed = entry.events_processed;
                }
                local.capacity.pipelines_running = entry.pipelines_running;
                // Trust the replicated status for cross-coordinator transitions (e.g., unhealthy).
                // If the control plane says a worker is ready, only refresh last_heartbeat when the
                // replicated liveness signal (heartbeat_seq) has actually ADVANCED since
                // we last synced. This acts as a heartbeat proxy for workers connected to
                // other coordinators WITHOUT masking a dead worker: if no new heartbeat is
                // replicated, the seq stays put, last_heartbeat goes stale, and the local
                // health_sweep can time the worker out. (Unconditionally stamping here was
                // the C5 bug: it reset the clock every tick so dead workers stayed Ready.)
                match replicated_status {
                    WorkerStatus::Unhealthy | WorkerStatus::Draining => {
                        local.status = replicated_status;
                    }
                    WorkerStatus::Ready if entry.heartbeat_seq > local.last_seen_hb_seq => {
                        local.last_seen_hb_seq = entry.heartbeat_seq;
                        local.last_heartbeat = Instant::now();
                    }
                    _ => {}
                }
            } else {
                // New worker from Raft (registered on another coordinator)
                let worker = WorkerNode {
                    id: wid.clone(),
                    address: entry.address.clone(),
                    api_key: varpulis_core::security::SecretString::new(entry.api_key.clone()),
                    status: replicated_status,
                    capacity: crate::worker::WorkerCapacity {
                        cpu_cores: entry.cpu_cores,
                        pipelines_running: entry.pipelines_running,
                        max_pipelines: entry.max_pipelines,
                    },
                    last_heartbeat: Instant::now(),
                    assigned_pipelines: entry.assigned_pipelines.clone(),
                    events_processed: entry.events_processed,
                    // Not this coordinator's local worker, so its own send-side
                    // counter starts at 0; seed last_seen from the replicated value
                    // and stamp once (a just-learned worker is presumed alive until
                    // its timeout elapses with no further seq advance).
                    heartbeat_seq: 0,
                    last_seen_hb_seq: entry.heartbeat_seq,
                };
                self.workers.insert(wid, worker);
            }
        }

        // Remove workers that were deregistered in Raft
        self.workers
            .retain(|id, _| replicated_worker_ids.contains(id));

        // Sync pipeline groups: deserialize from serde_json::Value
        self.pipeline_groups = state
            .pipeline_groups
            .iter()
            .filter_map(|(name, val)| {
                serde_json::from_value(val.clone())
                    .ok()
                    .map(|group| (name.clone(), group))
            })
            .collect();

        // Sync connectors: same type, direct clone
        self.connectors = state.connectors.clone();

        // Sync scaling policy: deserialize from serde_json::Value
        self.scaling_policy = state
            .scaling_policy
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        // Sync per-worker pipeline metrics from Raft (heartbeat data replicated
        // from whichever coordinator received the heartbeat).
        for (worker_id_str, metrics) in &state.worker_pipeline_metrics {
            let wid = WorkerId(worker_id_str.clone());
            // Only update if Raft has newer/more data than local state
            if !metrics.is_empty() {
                self.worker_metrics.insert(wid, metrics.clone());
            }
        }

        tracing::debug!(
            "Synced from control-plane state: {} workers, {} groups, {} connectors",
            self.workers.len(),
            self.pipeline_groups.len(),
            self.connectors.len()
        );
    }

    /// Record a migration in the control plane before it is executed.
    ///
    /// Until this existed, a migration lived only in the coordinator process
    /// that was running it: `plan_migrate_pipeline` built a plan,
    /// `execute_migrate_plan` did the whole checkpoint/deploy/restore/switch
    /// /clean-up over HTTP inside the request, and `commit_migrate_pipeline`
    /// wrote a `MigrationTask` **after** it finished — `Completed` or
    /// `Failed`, never one of the five phases in between.
    ///
    /// So a coordinator that died mid-migration left nothing behind. The
    /// pipeline could be deployed on the target and still assigned on the
    /// source, and no surviving coordinator had any record that a migration
    /// was in flight, let alone which phase it had reached.
    ///
    /// The record is written *before* execution starts, so the crash window
    /// is covered from the first phase. From there
    /// [`Coordinator::reconcile_migrations`] drives it: it advances on
    /// observed evidence, fences the source at cut-over, and fails the record
    /// on its deadline rather than letting it pin the pipeline forever.
    ///
    /// A no-op in standalone mode, where there is no control plane to record
    /// it in and no second coordinator to hand it to.
    #[cfg(feature = "jetstream-control-plane")]
    pub async fn record_migration_started(
        &self,
        plan: &MigratePipelinePlan,
        deadline: std::time::Duration,
    ) -> Result<(), ClusterError> {
        use crate::jetstream_control_plane::{ControlKey, Expect, MigrationPhase, MigrationRecord};

        let Some(ref applier) = self.control_plane else {
            return Ok(());
        };
        let cp = applier.control_plane();

        // The source's current revision, kept for audit: the cut-over CASes
        // against whatever the revision is *then*, not this one, but a record
        // whose `source_fence` no longer matches says the source was replaced
        // under the migration.
        let source_fence = cp
            .get::<crate::jetstream_control_plane::WorkerRecord>(&ControlKey::Worker(
                plan.source_worker_id.0.clone(),
            ))
            .await
            .ok()
            .flatten()
            .map(|v| v.revision)
            .unwrap_or(0);

        let now_ms = crate::jetstream_control_plane::now_ms();
        let record = MigrationRecord {
            id: plan.migration_id.clone(),
            pipeline: plan.pipeline_name.clone(),
            group: plan.group_id.clone(),
            source: plan.source_worker_id.0.clone(),
            target: plan.target_worker_id.0.clone(),
            phase: MigrationPhase::Checkpointing,
            reason: format!("{:?}", plan.reason),
            source_fence,
            restore_checkpoint: None,
            restored: false,
            deadline_ms: now_ms.saturating_add(deadline.as_millis() as u64),
            finished_ms: None,
            failure: None,
        };

        cp.write(
            &ControlKey::Migration(plan.migration_id.clone()),
            &record,
            Expect::Absent,
        )
        .await
        .map(|_| ())
        .map_err(|e| {
            ClusterError::InvalidOperation(format!(
                "could not record migration in control plane: {e}"
            ))
        })
    }

    /// Drive every in-flight migration one step.
    ///
    /// Level-triggered: it re-reads the world and re-derives each record's
    /// phase, so calling it more often costs latency only, calling it less
    /// often costs latency only, and a coordinator that takes over from a
    /// dead one is indistinguishable from one that was merely slow. There is
    /// no edge to miss — which is the property the old path did not have,
    /// because the old path had no state outside the process doing the work.
    ///
    /// Leader-only, like the rest of the sweep: the CAS on each record makes
    /// a second driver safe rather than necessary.
    ///
    /// Returns the tick report, or `None` when no control plane is configured.
    #[cfg(feature = "jetstream-control-plane")]
    pub async fn reconcile_migrations(&self) -> Option<crate::jetstream_control_plane::TickReport> {
        let applier = self.control_plane.as_ref()?;
        let reconciler =
            crate::jetstream_control_plane::Reconciler::new(applier.control_plane().clone());
        match reconciler
            .tick(crate::jetstream_control_plane::now_ms())
            .await
        {
            Ok(report) => Some(report),
            Err(e) => {
                tracing::warn!(error = %e, "migration reconciliation tick failed");
                None
            }
        }
    }

    /// Base URL of the coordinator that currently holds the lease.
    ///
    /// The control-plane analogue of [`Coordinator::raft_leader_addr`], which
    /// resolved a `NodeId` through a peer-address map built from
    /// `--raft-peers`. There is no such map here and there should not be one:
    /// the leader is the only party that knows its own reachable address, and
    /// it already writes a record every sweep, so it publishes the address
    /// there.
    ///
    /// `None` when nobody holds the lease, or the holder published no address
    /// — the caller must refuse rather than invent a destination.
    #[cfg(feature = "jetstream-control-plane")]
    pub async fn control_plane_leader_addr(&self) -> Option<String> {
        self.leader_lease.as_ref()?.current_leader_address().await
    }

    /// Materialise coordinator state from the control-plane bucket.
    ///
    /// The counterpart of [`Coordinator::sync_from_raft`], and it feeds the
    /// same backend-agnostic [`Coordinator::sync_from_control_state`], so the
    /// two backends cannot drift in how they update the coordinator.
    ///
    /// Every coordinator runs it, not just the leader: a follower serves
    /// read-only API responses out of this state, and the leader uses it to
    /// refresh heartbeat timestamps for workers that are connected to a
    /// different coordinator. Without it a follower on a JetStream deployment
    /// answered from whatever it happened to have seen directly — which for a
    /// coordinator that just started is nothing at all.
    #[cfg(feature = "jetstream-control-plane")]
    pub async fn sync_from_control_plane(&mut self) {
        let Some(ref applier) = self.control_plane else {
            return;
        };
        let snapshot = match applier.control_plane().snapshot().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "control-plane snapshot failed; keeping local state");
                return;
            }
        };
        let state = crate::jetstream_control_plane::materialize(&snapshot);
        self.sync_from_control_state(&state);
    }

    /// Update the HA role from the control plane's leader lease.
    ///
    /// This is the JetStream counterpart of [`Coordinator::update_raft_role`],
    /// and the health sweep calls whichever of the two is configured. Until it
    /// existed, a coordinator built without the `raft` feature left `ha_role`
    /// at its `Standalone` default for the whole process lifetime — and
    /// `Standalone::is_writer()` is `true`, so *every* coordinator in a
    /// JetStream deployment swept health, drove failovers and reconciled
    /// placements simultaneously.
    ///
    /// The lease's `tick` is a compare-and-swap, so losing the race or losing
    /// the connection both stand this coordinator down; see
    /// [`crate::jetstream_control_plane::leader`] for why `is_leader()` is
    /// advisory for scheduling and the per-write CAS is what provides safety.
    ///
    /// A follower pays one extra KV read per sweep to learn who holds the
    /// lease, which is only used to tell an API caller where to go.
    #[cfg(feature = "jetstream-control-plane")]
    pub async fn update_control_plane_role(&mut self) {
        use crate::jetstream_control_plane::LeaderState;

        let Some(ref mut lease) = self.leader_lease else {
            return;
        };

        let state = lease.tick().await;
        self.ha_role = match state {
            LeaderState::Leader => HaRole::Leader,
            LeaderState::Follower => {
                let leader_id = lease
                    .current_holder()
                    .await
                    .unwrap_or_else(|| "unknown".to_string());
                HaRole::Follower { leader_id }
            }
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
        worker.capacity.pipelines_running = hb.pipelines_running;
        worker.events_processed = hb.events_processed;
        // Advance the monotonic liveness counter. This is the single source of
        // truth for both the HTTP and NATS heartbeat paths; the receiving
        // coordinator replicates this value through Raft (WorkerMetricsUpdated)
        // so other coordinators can tell a live worker from a dead one.
        worker.heartbeat_seq = worker.heartbeat_seq.wrapping_add(1);

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
        self.worker_metrics.remove(worker_id);
        info!(worker_id = %worker_id, "Worker deregistered");
        Ok(())
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

    /// Mark a worker unhealthy because a request to it did not arrive.
    ///
    /// The health sweep can only infer death from silence, so it has to wait
    /// out `heartbeat_timeout` — 15 seconds by default. A request that failed
    /// at the transport is direct evidence, available now, and every event
    /// routed to that worker in the meantime is lost for nothing.
    ///
    /// Deliberately marks rather than deregisters. `heartbeat` moves an
    /// unhealthy worker back to `Ready` as soon as one arrives, so a transient
    /// blip costs at most one heartbeat interval, while a real failure saves
    /// the whole timeout. Returns whether this call changed the status, so a
    /// caller can avoid logging the same worker on every retry.
    pub fn mark_worker_unreachable(&mut self, id: &WorkerId, detail: &str) -> bool {
        let Some(worker) = self.workers.get_mut(id) else {
            return false;
        };
        if worker.status == WorkerStatus::Unhealthy {
            return false;
        }
        // A draining worker is expected to stop answering; do not confuse a
        // planned drain with a failure.
        if worker.status == WorkerStatus::Draining {
            return false;
        }
        warn!(
            worker_id = %id,
            detail,
            "Worker did not answer a request; marking unhealthy without waiting for the heartbeat timeout"
        );
        worker.status = WorkerStatus::Unhealthy;
        self.pending_rebalance = true;
        self.update_metrics_counts();
        true
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

    /// Aggregate metrics across all workers, filtering out stale entries.
    ///
    /// Only includes metrics for pipelines that are actually placed on the
    /// reporting worker (cross-references with current placements).
    pub fn get_cluster_metrics(&self) -> ClusterMetrics {
        // Build a set of (worker_id, pipeline_name) from current placements.
        let mut active_placements = std::collections::HashSet::new();
        for group in self.pipeline_groups.values() {
            for (pipeline_name, placement) in &group.placements {
                active_placements.insert((placement.worker_id.0.clone(), pipeline_name.clone()));
            }
        }

        let mut pipelines = Vec::new();
        for (worker_id, metrics) in &self.worker_metrics {
            for m in metrics {
                if active_placements.contains(&(worker_id.0.clone(), m.pipeline_name.clone())) {
                    pipelines.push(PipelineWorkerMetrics {
                        pipeline_name: m.pipeline_name.clone(),
                        worker_id: worker_id.0.clone(),
                        events_in: m.events_in,
                        events_out: m.events_out,
                        connector_health: m.connector_health.clone(),
                    });
                }
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

    /// Update Prometheus gauge metrics from current coordinator state.
    pub(crate) fn update_metrics_counts(&self) {
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
pub(crate) struct CheckpointResponsePayload {
    pub(crate) pipeline_id: String,
    pub(crate) checkpoint: varpulis_runtime::persistence::EngineCheckpoint,
    pub(crate) events_processed: u64,
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
    use crate::migration::MigrationStatus;
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
        assert_eq!(
            coord.workers[&WorkerId("w1".into())].api_key.expose(),
            "key1"
        );

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
        assert_eq!(
            coord.workers[&WorkerId("w1".into())].api_key.expose(),
            "key2"
        );
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

        // Use checked_sub to avoid panic on Windows when system uptime < 7200s
        // (Instant on Windows starts from boot and cannot go below zero).
        let old_instant = Instant::now()
            .checked_sub(Duration::from_hours(2))
            .unwrap_or(Instant::now());

        // If we couldn't actually go back in time, the "old" tasks won't be
        // older than the TTL, so cleanup won't remove them. Skip the test.
        if old_instant.elapsed() < Duration::from_hours(1) {
            // System uptime too short to represent a 2-hour-old instant.
            return;
        }

        // Insert a completed migration with old start time
        let mut task = MigrationTask {
            id: "m1".into(),
            pipeline_name: "p1".into(),
            group_id: "g1".into(),
            source_worker: WorkerId("w1".into()),
            target_worker: WorkerId("w2".into()),
            status: MigrationStatus::Completed,
            started_at: old_instant,
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
        task.started_at = old_instant;
        coord.active_migrations.insert("m3".into(), task.clone());

        // Insert an in-progress migration that is old (should NOT be cleaned)
        task.id = "m4".into();
        task.status = MigrationStatus::Deploying;
        task.started_at = old_instant;
        coord.active_migrations.insert("m4".into(), task);

        assert_eq!(coord.active_migrations.len(), 4);

        // Cleanup with 1 hour TTL
        coord.cleanup_completed_migrations(Duration::from_hours(1));

        // m1 (completed, old) and m3 (failed, old) should be removed
        // m2 (completed, recent) and m4 (in-progress, old) should remain
        assert_eq!(coord.active_migrations.len(), 2);
        assert!(coord.active_migrations.contains_key("m2"));
        assert!(coord.active_migrations.contains_key("m4"));
    }

    #[test]
    fn test_cleanup_completed_migrations_noop_when_empty() {
        let mut coord = Coordinator::new();
        coord.cleanup_completed_migrations(Duration::from_hours(1));
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

        coord.cleanup_completed_migrations(Duration::from_hours(1));
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

        // Drain with no pipelines -- worker gets deregistered
        let result = coord.drain_worker(&WorkerId("w1".into()), None).await;
        assert!(result.is_ok());
        // Worker should be removed after drain
        assert!(!coord.workers.contains_key(&WorkerId("w1".into())));
    }

    #[test]
    fn test_register_worker_triggers_pending_rebalance() {
        let mut coord = Coordinator::new();
        assert!(!coord.pending_rebalance);

        // No pipeline groups -> no pending rebalance
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
            region_affinity: None,
            cross_region_routes: vec![],
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

        // No pipelines assigned -> no migrations
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
            region_affinity: None,
            cross_region_routes: vec![],
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
                failure_reason: None,
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
                failure_reason: None,
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
            region_affinity: None,
            cross_region_routes: vec![],
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
                    failure_reason: None,
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
            region_affinity: None,
            cross_region_routes: vec![],
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
                    failure_reason: None,
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
            region_affinity: None,
            cross_region_routes: vec![],
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
                    failure_reason: None,
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

    /// Regression test: heartbeat correctly updates events_processed and pipelines_running.
    ///
    /// This validates the worker metrics flow that feeds the monitoring dashboard.
    /// Previously, these values were always 0 because sync_from_raft() would
    /// overwrite locally-maintained heartbeat data with Raft state (which never
    /// receives heartbeat metrics).
    #[test]
    fn test_heartbeat_updates_worker_metrics() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(WorkerId("w1".into()), "http://w1:9000".into(), "key".into());
        coord.register_worker(node);

        // Verify initial state is 0
        let worker = coord.workers.get(&WorkerId("w1".into())).unwrap();
        assert_eq!(worker.events_processed, 0);
        assert_eq!(worker.capacity.pipelines_running, 0);

        // Simulate heartbeat with metrics
        let hb = crate::worker::HeartbeatRequest {
            events_processed: 500,
            pipelines_running: 2,
            pipeline_metrics: vec![crate::worker::PipelineMetrics {
                pipeline_name: "financial-cep".into(),
                events_in: 500,
                events_out: 30,
                connector_health: vec![],
            }],
        };
        coord.heartbeat(&WorkerId("w1".into()), &hb).unwrap();

        // Verify heartbeat updated the values
        let worker = coord.workers.get(&WorkerId("w1".into())).unwrap();
        assert_eq!(
            worker.events_processed, 500,
            "heartbeat should update events_processed"
        );
        assert_eq!(
            worker.capacity.pipelines_running, 2,
            "heartbeat should update pipelines_running"
        );

        // Verify pipeline metrics are stored
        let metrics = coord.worker_metrics.get(&WorkerId("w1".into())).unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].pipeline_name, "financial-cep");
        assert_eq!(metrics[0].events_in, 500);
    }

    /// Test that subsequent heartbeats update (not accumulate) metrics.
    #[test]
    fn test_heartbeat_overwrites_previous_metrics() {
        let mut coord = Coordinator::new();
        let node = WorkerNode::new(WorkerId("w1".into()), "http://w1:9000".into(), "key".into());
        coord.register_worker(node);

        // First heartbeat
        let hb1 = crate::worker::HeartbeatRequest {
            events_processed: 100,
            pipelines_running: 1,
            pipeline_metrics: vec![],
        };
        coord.heartbeat(&WorkerId("w1".into()), &hb1).unwrap();

        // Second heartbeat with updated values
        let hb2 = crate::worker::HeartbeatRequest {
            events_processed: 500,
            pipelines_running: 2,
            pipeline_metrics: vec![],
        };
        coord.heartbeat(&WorkerId("w1".into()), &hb2).unwrap();

        let worker = coord.workers.get(&WorkerId("w1".into())).unwrap();
        assert_eq!(
            worker.events_processed, 500,
            "second heartbeat should overwrite, not accumulate"
        );
        assert_eq!(worker.capacity.pipelines_running, 2);
    }

    // NOTE: sync_from_raft uses max(local, raft) for events_processed and
    // latest raft value for pipelines_running. Heartbeat metrics are replicated
    // through Raft via WorkerMetricsUpdated, so all coordinators see current values.
}
