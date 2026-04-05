//! Rebalancing, migration, failover, and scaling operations.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

use super::{
    CheckpointResponsePayload, Coordinator, DeployResponse, MigratePipelinePlan, ScalingAction,
};
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
    pub async fn execute_migrate_plan(
        http_client: &reqwest::Client,
        plan: &MigratePipelinePlan,
        source_alive: bool,
        connectors: &HashMap<String, ClusterConnector>,
    ) -> Result<String, String> {
        // Step 1: Checkpoint (best-effort -- skip if source is dead)
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
    /// Steps: checkpoint (if source alive) -> deploy -> restore -> switch -> cleanup.
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
        let target_api_key = target_worker.api_key.expose().to_string();

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

        // Step 1: Checkpoint (best-effort -- skip if source is dead)
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

        // Step 4: Switch -- update placements to point to target
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

        // Step 5: Cleanup -- remove pipeline from source (skip if dead)
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
        // Clear stale metrics for the migrated pipeline on the source worker.
        if let Some(wm) = self.worker_metrics.get_mut(&source_worker_id) {
            wm.retain(|m| m.pipeline_name != *pipeline_name);
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
                match self.drain_worker(wid, Some(Duration::from_secs(300))).await {
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
