//! Rebalancing, migration, failover, and scaling operations.

use std::collections::HashMap;
#[cfg(feature = "distributed-checkpoint")]
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{error, info, warn};
use varpulis_runtime::persistence::EngineCheckpoint;
#[cfg(feature = "distributed-checkpoint")]
use varpulis_runtime::persistence::StateStore;

use super::{
    CheckpointResponsePayload, Coordinator, DeployResponse, MigratePipelinePlan, ScalingAction,
};
#[cfg(feature = "distributed-checkpoint")]
use crate::checkpoint_protocol::{DistributedCheckpoint, SnapshotLocation};
use crate::connector_config::{self, ClusterConnector};
use crate::migration::{MigrationReason, MigrationStatus, MigrationTask};
use crate::pipeline_group::{GroupStatus, PipelineDeployment, PipelineDeploymentStatus};
use crate::worker::{WorkerId, WorkerNode, WorkerStatus};
use crate::{ClusterError, LeastLoadedPlacement, PlacementStrategy};

impl Coordinator {
    /// Reconcile pipeline placements: re-deploy pipelines to workers that
    /// restarted and lost their in-memory state.  Called from the health-sweep
    /// loop on the leader when `pending_rebalance` is true.
    #[tracing::instrument(skip(self))]
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
                // the placement is healthy -- nothing to do.
                if worker.assigned_pipelines.contains(&pname.clone()) {
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
                        worker.api_key.expose().to_string(),
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
        let target_api_key = target_worker.api_key.expose().to_string();

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
    ///
    /// When the `distributed-checkpoint` feature is enabled, callers should
    /// resolve the latest durable checkpoint for the group via
    /// `load_distributed_checkpoint` and pass it through
    /// `execute_migrate_plan_with_checkpoint`. The plain
    /// `execute_migrate_plan` path always falls back to checkpointing the
    /// source worker via HTTP — which only works when the source is alive.
    pub async fn execute_migrate_plan(
        http_client: &reqwest::Client,
        plan: &MigratePipelinePlan,
        source_alive: bool,
        connectors: &HashMap<String, ClusterConnector>,
    ) -> Result<String, String> {
        Self::execute_migrate_plan_inner(http_client, plan, source_alive, connectors, None).await
    }

    /// NATS analogue of [`execute_migrate_plan_inner`] (audit C6b).
    ///
    /// There is no single worker `"migrate"` command — a migration is
    /// *composed* of the primitive per-command NATS subjects the worker
    /// already handles: `checkpoint` → `deploy` → `restore` → `undeploy`
    /// (see `nats_worker.rs`). This mirrors the HTTP inner path's steps,
    /// best-effort semantics (checkpoint + restore failures are non-fatal),
    /// and return contract (`Ok(new_pipeline_id)` / `Err(reason)`), but
    /// addresses each worker over NATS request/reply instead of HTTP. It is
    /// the path that works in a NATS deployment, where worker addresses are
    /// `nats://...` and reqwest cannot reach them.
    ///
    /// The distributed-checkpoint preference is preserved: a preloaded
    /// `distributed_checkpoint` short-circuits the source checkpoint, which
    /// is what makes failover work when the source worker is dead.
    ///
    /// Note the checkpoint envelope difference vs HTTP: the worker's
    /// `checkpoint` handler replies with a *bare* `EngineCheckpoint` (the
    /// HTTP route wraps it in a `CheckpointResponsePayload`), so
    /// [`fetch_source_checkpoint_nats`] deserializes the reply directly.
    #[cfg(feature = "nats-transport")]
    pub async fn execute_migrate_plan_nats(
        nats_client: &async_nats::Client,
        plan: &MigratePipelinePlan,
        source_alive: bool,
        connectors: &HashMap<String, ClusterConnector>,
        distributed_checkpoint: Option<EngineCheckpoint>,
    ) -> Result<String, String> {
        use crate::nats_transport;

        let timeout = Duration::from_secs(10);

        // Step 1: Acquire a checkpoint to restore on the target.
        //
        // Preference order mirrors `execute_migrate_plan_inner`:
        //   1. A pre-loaded distributed checkpoint (durable state) — works
        //      even when the source worker is dead.
        //   2. Best-effort NATS checkpoint from the source worker.
        //   3. Nothing — proceed with a stateless redeploy.
        let checkpoint = if let Some(cp) = distributed_checkpoint {
            info!(
                "Using distributed checkpoint for pipeline '{}' migration to {} ({} events)",
                plan.pipeline_name, plan.target_worker_id, cp.events_processed
            );
            Some(cp)
        } else {
            fetch_source_checkpoint_nats(nats_client, plan, source_alive, timeout).await
        };

        // Step 2: Deploy to target worker.
        let (enriched_source, _) =
            crate::connector_config::inject_connectors(&plan.vpl_source, connectors);

        let deploy_subject = nats_transport::subject_cmd(&plan.target_worker_id.0, "deploy");
        let deploy_body = serde_json::json!({
            "name": plan.pipeline_name,
            "source": enriched_source,
        });

        let new_pipeline_id = match nats_transport::nats_request::<_, DeployResponse>(
            nats_client,
            &deploy_subject,
            &deploy_body,
            timeout,
        )
        .await
        {
            Ok(resp_body) => {
                info!(
                    "Migration deploy over NATS: '{}' on target {} (id={}, status={})",
                    resp_body.name, plan.target_worker_id, resp_body.id, resp_body.status
                );
                resp_body.id
            }
            Err(e) => {
                return Err(format!("Deploy to target failed over NATS: {e}"));
            }
        };

        // Step 3: Restore checkpoint on target (best-effort).
        //
        // The worker's `restore` handler reads the pipeline id from the
        // request body (the HTTP route takes it in the URL), so we include
        // it alongside the checkpoint.
        if let Some(ref cp) = checkpoint {
            let restore_subject = nats_transport::subject_cmd(&plan.target_worker_id.0, "restore");
            let restore_body = serde_json::json!({
                "pipeline_id": new_pipeline_id,
                "checkpoint": cp,
            });

            match nats_transport::nats_request::<_, serde_json::Value>(
                nats_client,
                &restore_subject,
                &restore_body,
                timeout,
            )
            .await
            {
                Ok(resp) if resp.get("error").is_none() => {
                    info!(
                        "Checkpoint restored over NATS for pipeline '{}' on worker {}",
                        plan.pipeline_name, plan.target_worker_id
                    );
                }
                Ok(resp) => {
                    warn!(
                        "Restore failed over NATS for '{}' (continuing without state): {}",
                        plan.pipeline_name, resp
                    );
                }
                Err(e) => {
                    warn!(
                        "Restore request failed over NATS for '{}' (continuing without state): {}",
                        plan.pipeline_name, e
                    );
                }
            }
        }

        // Step 4: Cleanup — remove pipeline from source (skip if dead).
        // Reuses the exact `undeploy` request shape from
        // `execute_teardown_plan_nats` (`{ "pipeline_id": ... }`).
        if source_alive && !plan.deployment.pipeline_id.is_empty() {
            let undeploy_subject =
                nats_transport::subject_cmd(&plan.source_worker_id.0, "undeploy");
            let undeploy_body = serde_json::json!({
                "pipeline_id": plan.deployment.pipeline_id,
            });

            match nats_transport::nats_request::<_, serde_json::Value>(
                nats_client,
                &undeploy_subject,
                &undeploy_body,
                timeout,
            )
            .await
            {
                Ok(_) => {
                    info!(
                        "Removed old pipeline '{}' from worker {} over NATS",
                        plan.pipeline_name, plan.source_worker_id
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to remove old pipeline '{}' from {} over NATS: {}",
                        plan.pipeline_name, plan.source_worker_id, e
                    );
                }
            }
        }

        Ok(new_pipeline_id)
    }

    /// Execute a migration plan over the best available transport (audit C6b).
    ///
    /// Routes over NATS request/reply when a NATS client is configured (a
    /// NATS deployment, where worker addresses are `nats://...`), otherwise
    /// falls back to the HTTP inner path. Mirrors the deploy/teardown/inject
    /// `*_dispatch` helpers so transport selection for migration lives in
    /// exactly one place. `distributed_checkpoint` is threaded through so a
    /// failover caller can supply durable state when the source is dead.
    #[cfg(feature = "nats-transport")]
    pub async fn execute_migrate_plan_dispatch(
        nats: Option<&async_nats::Client>,
        http: &reqwest::Client,
        plan: &MigratePipelinePlan,
        source_alive: bool,
        connectors: &HashMap<String, ClusterConnector>,
        distributed_checkpoint: Option<EngineCheckpoint>,
    ) -> Result<String, String> {
        match nats {
            Some(n) => {
                Self::execute_migrate_plan_nats(
                    n,
                    plan,
                    source_alive,
                    connectors,
                    distributed_checkpoint,
                )
                .await
            }
            None => {
                Self::execute_migrate_plan_inner(
                    http,
                    plan,
                    source_alive,
                    connectors,
                    distributed_checkpoint,
                )
                .await
            }
        }
    }

    /// Variant of [`execute_migrate_plan`] that accepts a pre-loaded engine
    /// checkpoint sourced from the distributed checkpoint store.
    ///
    /// When `distributed_checkpoint` is `Some`, the function uses that
    /// snapshot to restore state on the target worker and skips the
    /// HTTP-checkpoint step against the source. This is the path that
    /// makes failover work when the source worker is dead — the durable
    /// state lives in S3 (or whichever [`StateStore`] backs the cluster),
    /// not on the (possibly-gone) source worker.
    ///
    /// When `distributed_checkpoint` is `None` the behavior is identical
    /// to [`execute_migrate_plan`].
    ///
    /// `recovery_commit` controls whether the target worker is asked to
    /// commit any prepared 2PC sinks for the restored checkpoint id after
    /// restore. On a freshly-deployed pipeline this is normally a no-op
    /// (no transactions have been prepared on the new engine yet), but it
    /// gives connectors that need to re-issue `commit` against an external
    /// system a hook to do so. The HTTP path is best-effort and is not
    /// considered fatal if the target rejects it.
    #[cfg(feature = "distributed-checkpoint")]
    pub async fn execute_migrate_plan_with_checkpoint(
        http_client: &reqwest::Client,
        plan: &MigratePipelinePlan,
        source_alive: bool,
        connectors: &HashMap<String, ClusterConnector>,
        distributed_checkpoint: Option<EngineCheckpoint>,
    ) -> Result<String, String> {
        Self::execute_migrate_plan_inner(
            http_client,
            plan,
            source_alive,
            connectors,
            distributed_checkpoint,
        )
        .await
    }

    async fn execute_migrate_plan_inner(
        http_client: &reqwest::Client,
        plan: &MigratePipelinePlan,
        source_alive: bool,
        connectors: &HashMap<String, ClusterConnector>,
        distributed_checkpoint: Option<EngineCheckpoint>,
    ) -> Result<String, String> {
        // Step 1: Acquire a checkpoint to restore on the target.
        //
        // Preference order:
        //   1. A pre-loaded distributed checkpoint (durable state in S3 /
        //      RocksDB). This works even when the source worker is dead.
        //   2. Best-effort HTTP checkpoint from the source worker, used
        //      when no distributed checkpoint is available and the source
        //      is still reachable.
        //   3. Nothing — proceed with a stateless redeploy.
        let checkpoint = if let Some(cp) = distributed_checkpoint {
            info!(
                "Using distributed checkpoint for pipeline '{}' migration to {} ({} events)",
                plan.pipeline_name, plan.target_worker_id, cp.events_processed
            );
            Some(cp)
        } else {
            fetch_source_checkpoint(http_client, plan, source_alive).await
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

        // Step 4: Cleanup -- remove pipeline from source (skip if dead)
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
    /// Plan (lookups, no I/O) -> execute over the best available transport ->
    /// commit. The execute step routes over NATS when a NATS client is
    /// configured (worker addresses are `nats://...`), otherwise HTTP — the
    /// same `execute_migrate_plan_dispatch` the manual REST migrate handler
    /// uses. This is the audit-C6 follow-up: a C5-triggered auto-failover (and
    /// drain / rebalance, which also funnel through here) must migrate over
    /// NATS in a NATS deployment instead of POSTing to an unreachable
    /// `nats://` worker address.
    #[tracing::instrument(skip(self), fields(pipeline = %pipeline_name, group = %group_id, target = %target_worker_id))]
    pub async fn migrate_pipeline(
        &mut self,
        pipeline_name: &str,
        group_id: &str,
        target_worker_id: &WorkerId,
        reason: MigrationReason,
    ) -> Result<String, ClusterError> {
        // Phase 1: Build the migration plan (lookups only, no I/O). Errors here
        // (missing group / pipeline / target worker / VPL source) surface before
        // any state mutation, exactly as the old inline path did.
        let plan = self.plan_migrate_pipeline(pipeline_name, group_id, target_worker_id, reason)?;
        let source_worker_id = plan.source_worker_id.clone();

        // A source worker that is still registered and not explicitly Unhealthy
        // is treated as reachable for the best-effort checkpoint step; when it
        // is dead we skip straight to a stateless redeploy. Same predicate the
        // manual REST migrate handler uses.
        let source_alive = self
            .workers
            .get(&source_worker_id)
            .map(|w| w.status != WorkerStatus::Unhealthy)
            .unwrap_or(false);

        // Phase 2: Execute over the best available transport — NATS when a
        // client is configured, otherwise HTTP. `distributed_checkpoint` is
        // threaded through as `None` here (the internal failover / drain /
        // rebalance path has no preloaded durable snapshot to hand off); the
        // dead-source short-circuit is driven by `source_alive`, so a dead
        // source still migrates statelessly rather than blocking on an
        // unreachable worker.
        #[cfg(feature = "nats-transport")]
        let result = Self::execute_migrate_plan_dispatch(
            self.nats_client.as_ref(),
            &self.http_client,
            &plan,
            source_alive,
            &self.connectors,
            None,
        )
        .await;
        #[cfg(not(feature = "nats-transport"))]
        let result =
            Self::execute_migrate_plan(&self.http_client, &plan, source_alive, &self.connectors)
                .await;

        // Phase 3: Commit results to coordinator state.
        match result {
            Ok(new_pipeline_id) => {
                let migration_id =
                    self.commit_migrate_pipeline(&plan, &new_pipeline_id, true, None);
                // Clear stale metrics for the migrated pipeline on the source
                // worker. `commit_migrate_pipeline` handles placement + worker
                // bookkeeping; this source-metric purge is unique to the
                // internal path and is preserved from the old inline flow.
                if let Some(wm) = self.worker_metrics.get_mut(&source_worker_id) {
                    wm.retain(|m| m.pipeline_name != *pipeline_name);
                }
                Ok(migration_id)
            }
            Err(reason) => {
                self.commit_migrate_pipeline(&plan, "", false, Some(reason.clone()));
                Err(ClusterError::MigrationFailed(reason))
            }
        }
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
    #[tracing::instrument(skip(self))]
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
            // Already draining -- idempotent
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
    #[tracing::instrument(skip(self))]
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

    // =========================================================================
    // Dynamic Rescaling
    // =========================================================================

    /// Rescale the cluster to the target number of workers.
    ///
    /// - **Scale up**: Triggers webhook (if configured) and sets `pending_rebalance`
    ///   so that when new workers register, pipelines are automatically redistributed.
    /// - **Scale down**: Selects least-loaded workers and drains them, migrating all
    ///   their pipelines to remaining workers via checkpoint-based stateful migration.
    ///
    /// Returns a summary of what happened.
    pub async fn rescale(&mut self, target_workers: usize) -> Result<RescaleResult, ClusterError> {
        let available: Vec<WorkerId> = self
            .workers
            .values()
            .filter(|w| w.status == WorkerStatus::Ready)
            .map(|w| w.id.clone())
            .collect();

        let current = available.len();

        if target_workers == current {
            return Ok(RescaleResult {
                action: "none".to_string(),
                previous_workers: current,
                target_workers,
                migrations: Vec::new(),
                message: "Already at target worker count".to_string(),
            });
        }

        // Enforce scaling policy bounds if configured
        if let Some(ref policy) = self.scaling_policy {
            if target_workers < policy.min_workers {
                return Err(ClusterError::InvalidOperation(format!(
                    "Target {} below minimum {} workers",
                    target_workers, policy.min_workers
                )));
            }
            if target_workers > policy.max_workers {
                return Err(ClusterError::InvalidOperation(format!(
                    "Target {} above maximum {} workers",
                    target_workers, policy.max_workers
                )));
            }
        }

        if target_workers > current {
            // Scale up: fire webhook and mark pending rebalance
            self.pending_rebalance = true;
            if self.scaling_policy.is_some() {
                self.fire_scaling_webhook().await;
            }
            info!(
                "Rescale UP: {} -> {} workers (pending rebalance on new worker registration)",
                current, target_workers
            );
            Ok(RescaleResult {
                action: "scale_up".to_string(),
                previous_workers: current,
                target_workers,
                migrations: Vec::new(),
                message: format!(
                    "Scale-up initiated: waiting for {} new worker(s) to register",
                    target_workers - current
                ),
            })
        } else {
            // Scale down: drain least-loaded workers
            let workers_to_remove = current - target_workers;

            // Sort workers by load (least-loaded first) for draining
            let mut workers_by_load: Vec<(WorkerId, usize)> = available
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
            workers_by_load.sort_by_key(|(_, load)| *load);

            let to_drain: Vec<WorkerId> = workers_by_load
                .iter()
                .take(workers_to_remove)
                .map(|(wid, _)| wid.clone())
                .collect();

            let mut all_migrations = Vec::new();
            for wid in &to_drain {
                match self.drain_worker(wid, Some(Duration::from_mins(5))).await {
                    Ok(migration_ids) => {
                        info!(
                            "Drained worker {} ({} migrations)",
                            wid,
                            migration_ids.len()
                        );
                        all_migrations.extend(migration_ids);
                    }
                    Err(e) => {
                        warn!("Failed to drain worker {}: {}", wid, e);
                    }
                }
            }

            info!(
                "Rescale DOWN: {} -> {} workers ({} migrations)",
                current,
                target_workers,
                all_migrations.len()
            );

            Ok(RescaleResult {
                action: "scale_down".to_string(),
                previous_workers: current,
                target_workers,
                migrations: all_migrations,
                message: format!("Drained {} worker(s)", workers_to_remove),
            })
        }
    }
}

/// Result of a rescale operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RescaleResult {
    /// "scale_up", "scale_down", or "none"
    pub action: String,
    /// Worker count before rescale
    pub previous_workers: usize,
    /// Requested target
    pub target_workers: usize,
    /// Migration IDs created during scale-down
    pub migrations: Vec<String>,
    /// Human-readable summary
    pub message: String,
}

// ============================================================================
// Migration helpers
// ============================================================================

/// Best-effort capture of a checkpoint by hitting the source worker's HTTP
/// `/checkpoint` endpoint. Returns `None` on any failure (network, HTTP
/// error, deserialize error) so the caller can decide whether to proceed
/// without state. Skips the call entirely when `source_alive` is false —
/// that's the path that benefits from a distributed checkpoint instead.
async fn fetch_source_checkpoint(
    http_client: &reqwest::Client,
    plan: &MigratePipelinePlan,
    source_alive: bool,
) -> Option<varpulis_runtime::persistence::EngineCheckpoint> {
    if !source_alive {
        info!(
            "Source worker {} is dead, proceeding without source checkpoint for '{}'",
            plan.source_worker_id, plan.pipeline_name
        );
        return None;
    }

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
}

/// NATS analogue of [`fetch_source_checkpoint`] (audit C6b): best-effort
/// checkpoint of the source worker over NATS request/reply.
///
/// The worker's `checkpoint` handler replies with a *bare*
/// `EngineCheckpoint` JSON — unlike the HTTP route, which wraps it in a
/// `CheckpointResponsePayload` — so we deserialize the reply directly into
/// an `EngineCheckpoint`. Any failure (source dead, request timeout, or an
/// `{"error": ...}` reply that doesn't parse as a checkpoint) yields `None`
/// so the caller proceeds with a stateless redeploy, matching the HTTP
/// path's best-effort semantics.
#[cfg(feature = "nats-transport")]
async fn fetch_source_checkpoint_nats(
    nats_client: &async_nats::Client,
    plan: &MigratePipelinePlan,
    source_alive: bool,
    timeout: Duration,
) -> Option<EngineCheckpoint> {
    if !source_alive {
        info!(
            "Source worker {} is dead, proceeding without source checkpoint for '{}'",
            plan.source_worker_id, plan.pipeline_name
        );
        return None;
    }

    let subject = crate::nats_transport::subject_cmd(&plan.source_worker_id.0, "checkpoint");
    let body = serde_json::json!({
        "pipeline_id": plan.deployment.pipeline_id,
    });

    match crate::nats_transport::nats_request::<_, EngineCheckpoint>(
        nats_client,
        &subject,
        &body,
        timeout,
    )
    .await
    {
        Ok(cp) => {
            info!(
                "Checkpoint captured over NATS for pipeline '{}' ({} events)",
                plan.pipeline_name, cp.events_processed
            );
            Some(cp)
        }
        Err(e) => {
            warn!(
                "Checkpoint over NATS failed for '{}' (continuing without state): {}",
                plan.pipeline_name, e
            );
            None
        }
    }
}

/// Build the canonical state-store key for a distributed checkpoint.
///
/// Mirrors [`crate::coordinator::distributed_checkpoint::DistributedCheckpointCoordinator::store_key_for`]
/// so coordinators that recover after a leader change agree on the layout
/// without needing access to the orchestrator instance that wrote it.
#[cfg(feature = "distributed-checkpoint")]
#[allow(dead_code)] // API-side wiring lands in a follow-up; tests cover this path
pub fn distributed_checkpoint_store_key(
    prefix: &str,
    group_id: &str,
    checkpoint_id: u64,
) -> String {
    format!("{prefix}/{group_id}/{checkpoint_id}.json")
}

/// Load the [`EngineCheckpoint`] for a single pipeline out of an assembled
/// distributed checkpoint sitting in the state store.
///
/// `pipeline_name` is matched against the `pipeline_id` portion of the
/// participant key (`"{worker_id}/{pipeline_id}"`) — we don't require a
/// specific worker, since this is exactly the path used when the source
/// worker is dead and we're migrating to a different worker.
///
/// Returns:
/// - `Ok(Some(cp))` when the snapshot was found and is shipped inline.
/// - `Ok(None)` when the assembled checkpoint exists but contains no
///   matching pipeline (e.g. wrong group), or when only a `Remote`
///   snapshot is present (out-of-band upload not yet supported on the
///   migration path — Task 1.2/1.5 will wire it).
/// - `Err(_)` for store-level errors (read/deserialize failures).
#[cfg(feature = "distributed-checkpoint")]
#[allow(dead_code)] // API-side wiring lands in a follow-up; tests cover this path
pub fn load_distributed_checkpoint(
    state_store: &Arc<dyn StateStore>,
    store_key: &str,
    pipeline_name: &str,
) -> Result<Option<EngineCheckpoint>, String> {
    let bytes = state_store
        .get(store_key)
        .map_err(|e| format!("state store read failed for '{store_key}': {e}"))?;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let assembled: DistributedCheckpoint = serde_json::from_slice(&bytes).map_err(|e| {
        format!("failed to deserialize DistributedCheckpoint at '{store_key}': {e}")
    })?;

    // Search the snapshot map for a participant whose pipeline_id matches.
    // Participant keys are "{worker_id}/{pipeline_id}" — split on the first
    // `/` to recover the pipeline portion. We accept the first match: a
    // single distributed checkpoint should not contain multiple snapshots
    // for the same logical pipeline (replicas use different pipeline_ids
    // like `p1#0`, `p1#1`).
    let snapshot = assembled.snapshots.iter().find_map(|(participant, loc)| {
        participant
            .split_once('/')
            .map(|(_, pid)| pid)
            .filter(|pid| *pid == pipeline_name)
            .map(|_| loc)
    });

    match snapshot {
        Some(SnapshotLocation::Inline { checkpoint }) => Ok(Some((**checkpoint).clone())),
        Some(SnapshotLocation::Remote {
            store_key: remote_key,
            size_bytes,
        }) => {
            warn!(
                pipeline = %pipeline_name,
                remote_key = %remote_key,
                size_bytes,
                "remote distributed-checkpoint snapshots are not yet supported on the migration path"
            );
            Ok(None)
        }
        None => Ok(None),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(all(test, feature = "distributed-checkpoint"))]
mod distributed_migration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use varpulis_runtime::persistence::{EngineCheckpoint, MemoryStore, StateStore};

    use super::*;
    use crate::checkpoint_protocol::{participant_key, DistributedCheckpoint, SnapshotLocation};

    fn empty_checkpoint() -> EngineCheckpoint {
        EngineCheckpoint {
            version: varpulis_runtime::persistence::CHECKPOINT_VERSION,
            window_states: HashMap::new(),
            sase_states: HashMap::new(),
            join_states: HashMap::new(),
            variables: HashMap::new(),
            events_processed: 0,
            output_events_emitted: 0,
            watermark_state: None,
            distinct_states: HashMap::new(),
            limit_states: HashMap::new(),
            source_offsets: HashMap::new(),
        }
    }

    fn checkpoint_with_progress(events: u64) -> EngineCheckpoint {
        let mut cp = empty_checkpoint();
        cp.events_processed = events;
        cp
    }

    fn put_distributed_checkpoint(
        store: &Arc<dyn StateStore>,
        group_id: &str,
        checkpoint_id: u64,
        snapshots: HashMap<String, SnapshotLocation>,
    ) -> String {
        let assembled = DistributedCheckpoint {
            group_id: group_id.to_string(),
            checkpoint_id,
            timestamp_ms: 1_700_000_000_000,
            snapshots,
        };
        let key =
            distributed_checkpoint_store_key("distributed_checkpoints", group_id, checkpoint_id);
        let bytes = serde_json::to_vec(&assembled).unwrap();
        store.put(&key, &bytes).unwrap();
        key
    }

    #[test]
    fn store_key_format_matches_orchestrator() {
        // Pinned format: {prefix}/{group_id}/{checkpoint_id}.json. Must
        // line up with DistributedCheckpointCoordinator::store_key_for so a
        // coordinator that lost its in-memory orchestrator can still
        // recover the assembled snapshot from Raft + the state store.
        assert_eq!(
            distributed_checkpoint_store_key("distributed_checkpoints", "g1", 7),
            "distributed_checkpoints/g1/7.json"
        );
        assert_eq!(
            distributed_checkpoint_store_key("custom/prefix", "alpha", 42),
            "custom/prefix/alpha/42.json"
        );
    }

    #[test]
    fn load_distributed_checkpoint_returns_inline_snapshot() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
        let cp = checkpoint_with_progress(123);
        let mut snapshots = HashMap::new();
        snapshots.insert(
            participant_key(&WorkerId("w0".into()), "p1"),
            SnapshotLocation::Inline {
                checkpoint: Box::new(cp),
            },
        );
        let key = put_distributed_checkpoint(&store, "g1", 1, snapshots);

        let loaded = load_distributed_checkpoint(&store, &key, "p1")
            .expect("load")
            .expect("snapshot present");
        assert_eq!(loaded.events_processed, 123);
    }

    #[test]
    fn load_distributed_checkpoint_finds_pipeline_regardless_of_worker() {
        // The whole point of this path is to migrate AWAY from the source
        // worker — the lookup must not require the participant key to
        // match the source worker. Match purely on pipeline name.
        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
        let cp = checkpoint_with_progress(99);
        let mut snapshots = HashMap::new();
        snapshots.insert(
            participant_key(&WorkerId("w-dead".into()), "the-pipeline"),
            SnapshotLocation::Inline {
                checkpoint: Box::new(cp),
            },
        );
        let key = put_distributed_checkpoint(&store, "grp", 5, snapshots);

        let loaded = load_distributed_checkpoint(&store, &key, "the-pipeline")
            .expect("load succeeds")
            .expect("snapshot present");
        assert_eq!(loaded.events_processed, 99);
    }

    #[test]
    fn load_distributed_checkpoint_missing_key_returns_none() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
        let loaded = load_distributed_checkpoint(&store, "distributed_checkpoints/g/1.json", "p1")
            .expect("missing key is not an error");
        assert!(loaded.is_none());
    }

    #[test]
    fn load_distributed_checkpoint_pipeline_not_in_group_returns_none() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
        let mut snapshots = HashMap::new();
        snapshots.insert(
            participant_key(&WorkerId("w0".into()), "other"),
            SnapshotLocation::Inline {
                checkpoint: Box::new(empty_checkpoint()),
            },
        );
        let key = put_distributed_checkpoint(&store, "g1", 1, snapshots);

        let loaded = load_distributed_checkpoint(&store, &key, "missing-pipeline").expect("load");
        assert!(loaded.is_none());
    }

    #[test]
    fn load_distributed_checkpoint_remote_snapshot_returns_none_with_warning() {
        // Remote snapshots aren't yet supported on the migration path —
        // we should fall back gracefully (returning None, NOT an error)
        // so the caller can decide whether to proceed without state or
        // try the source-worker HTTP fallback.
        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
        let mut snapshots = HashMap::new();
        snapshots.insert(
            participant_key(&WorkerId("w0".into()), "p1"),
            SnapshotLocation::Remote {
                store_key: "checkpoints/g/1/w0.json".into(),
                size_bytes: 4_500_000,
            },
        );
        let key = put_distributed_checkpoint(&store, "g1", 1, snapshots);

        let loaded = load_distributed_checkpoint(&store, &key, "p1").expect("load");
        assert!(loaded.is_none());
    }

    #[test]
    fn load_distributed_checkpoint_corrupt_payload_is_error() {
        // A truncated / corrupt payload at the expected key must surface
        // as an error rather than silently returning None — the caller
        // needs to log it and probably fail the migration loudly.
        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
        let key = "distributed_checkpoints/g/1.json".to_string();
        store.put(&key, b"not json at all").unwrap();
        let err = load_distributed_checkpoint(&store, &key, "p1").unwrap_err();
        assert!(err.contains("deserialize"));
    }
}
