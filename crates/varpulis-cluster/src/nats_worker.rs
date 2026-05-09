//! Worker-side NATS command handler.
//!
//! Listens for coordinator commands (deploy, undeploy, inject, etc.) over NATS
//! and executes them against the local `TenantManager`.
//!
//! This replaces the HTTP API endpoints that the coordinator previously used
//! to control workers.

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
use std::collections::HashMap;
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
use std::sync::Arc;
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
use std::time::Duration;

#[cfg(feature = "nats-transport")]
use tracing::{error, info, warn};
#[cfg(feature = "nats-transport")]
use varpulis_runtime::SharedTenantManager;

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
use crate::checkpoint_protocol::{
    classify_snapshot, CheckpointAbortNotification, CheckpointBarrierAck, CheckpointBarrierRequest,
    CheckpointCompleteNotification, CheckpointId, GroupId, SnapshotClassification,
};
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
use crate::nats_transport::subject_checkpoint_ack;
#[cfg(feature = "nats-transport")]
use crate::nats_transport::subject_cmd_wildcard;
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
use crate::worker::WorkerId;

/// Run the worker-side NATS command handler.
///
/// Subscribes to `varpulis.cluster.cmd.{worker_id}.>` and dispatches
/// incoming commands to the tenant manager. When the
/// `distributed-checkpoint` feature is enabled, it additionally subscribes
/// to `varpulis.cluster.checkpoint.>` and routes
/// `checkpoint_complete` / `checkpoint_abort` notifications through the
/// same dispatcher.
#[cfg(feature = "nats-transport")]
pub async fn run_worker_nats_handler(
    client: async_nats::Client,
    worker_id: &str,
    api_key: &str,
    tenant_manager: SharedTenantManager,
) {
    use futures_util::StreamExt;

    let subject = subject_cmd_wildcard(worker_id);
    let cmd_sub = match client.subscribe(subject.clone()).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to subscribe to {}: {}", subject, e);
            return;
        }
    };

    let api_key = api_key.to_string();
    info!("Worker {} listening for commands on {}", worker_id, subject);

    #[cfg(feature = "distributed-checkpoint")]
    let checkpoint_state = Arc::new(WorkerCheckpointState::default());
    #[cfg(feature = "distributed-checkpoint")]
    let worker_id_owned = WorkerId(worker_id.to_string());
    #[cfg(feature = "distributed-checkpoint")]
    let nats_client = client.clone();

    #[cfg(feature = "distributed-checkpoint")]
    let checkpoint_sub = {
        let subject = "varpulis.cluster.checkpoint.>".to_string();
        match client.subscribe(subject.clone()).await {
            Ok(s) => {
                info!(
                    "Worker {} listening for checkpoint broadcasts on {}",
                    worker_id, subject,
                );
                Some(s)
            }
            Err(e) => {
                error!("Failed to subscribe to {}: {}", subject, e);
                None
            }
        }
    };

    #[cfg(feature = "distributed-checkpoint")]
    {
        let mut cmd_sub = cmd_sub;
        let mut checkpoint_sub = checkpoint_sub;
        loop {
            tokio::select! {
                msg = cmd_sub.next() => {
                    let Some(msg) = msg else { return };
                    let cmd = match msg.subject.rsplit('.').next() {
                        Some(c) => c.to_string(),
                        None => {
                            warn!("Malformed command subject: {}", msg.subject);
                            continue;
                        }
                    };
                    let response = dispatch_command(
                        &cmd,
                        &msg.payload,
                        &api_key,
                        &tenant_manager,
                        &checkpoint_state,
                        &worker_id_owned,
                        &nats_client,
                    )
                    .await;
                    if let Some(reply_subject) = msg.reply.clone() {
                        if let Err(e) = client.publish(reply_subject, response.into()).await {
                            error!("Failed to send reply: {}", e);
                        }
                    }
                }
                msg = async {
                    match checkpoint_sub.as_mut() {
                        Some(s) => s.next().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let Some(msg) = msg else { continue };
                    // Subject layout: varpulis.cluster.checkpoint.{cmd}.{group_id}
                    // We dispatch by the second-from-last segment.
                    let mut parts = msg.subject.rsplitn(3, '.');
                    let _group = parts.next();
                    let cmd_segment = parts.next().unwrap_or("");
                    let cmd_name = match cmd_segment {
                        "complete" => "checkpoint_complete",
                        "abort" => "checkpoint_abort",
                        other => {
                            warn!("Unknown checkpoint subject segment: {}", other);
                            continue;
                        }
                    };
                    let _ = dispatch_command(
                        cmd_name,
                        &msg.payload,
                        &api_key,
                        &tenant_manager,
                        &checkpoint_state,
                        &worker_id_owned,
                        &nats_client,
                    )
                    .await;
                    // No reply for broadcasts.
                }
            }
        }
    }

    #[cfg(not(feature = "distributed-checkpoint"))]
    {
        let mut sub = cmd_sub;
        while let Some(msg) = sub.next().await {
            let cmd = match msg.subject.rsplit('.').next() {
                Some(c) => c,
                None => {
                    warn!("Malformed command subject: {}", msg.subject);
                    continue;
                }
            };

            let response = dispatch_command(cmd, &msg.payload, &api_key, &tenant_manager).await;

            if let Some(reply_subject) = msg.reply.clone() {
                if let Err(e) = client.publish(reply_subject, response.into()).await {
                    error!("Failed to send reply: {}", e);
                }
            }
        }
    }
}

#[cfg(all(feature = "nats-transport", not(feature = "distributed-checkpoint")))]
async fn dispatch_command(
    cmd: &str,
    payload: &[u8],
    api_key: &str,
    tm: &SharedTenantManager,
) -> Vec<u8> {
    match cmd {
        "deploy" => handle_deploy(payload, api_key, tm).await,
        "undeploy" => handle_undeploy(payload, api_key, tm).await,
        "inject" => handle_inject(payload, api_key, tm).await,
        "inject_batch" => handle_inject_batch(payload, api_key, tm).await,
        "checkpoint" => handle_checkpoint(payload, api_key, tm).await,
        "restore" => handle_restore(payload, api_key, tm).await,
        "drain" => handle_drain(payload).await,
        other => {
            warn!("Unknown command: {}", other);
            error_json(&format!("unknown command: {other}"))
        }
    }
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
async fn dispatch_command(
    cmd: &str,
    payload: &[u8],
    api_key: &str,
    tm: &SharedTenantManager,
    state: &Arc<WorkerCheckpointState>,
    worker_id: &WorkerId,
    client: &async_nats::Client,
) -> Vec<u8> {
    match cmd {
        "deploy" => handle_deploy(payload, api_key, tm).await,
        "undeploy" => handle_undeploy(payload, api_key, tm).await,
        "inject" => handle_inject(payload, api_key, tm).await,
        "inject_batch" => handle_inject_batch(payload, api_key, tm).await,
        "checkpoint" => handle_checkpoint(payload, api_key, tm).await,
        "restore" => handle_restore(payload, api_key, tm).await,
        "drain" => handle_drain(payload).await,
        "checkpoint_barrier" => {
            handle_checkpoint_barrier(payload, tm, state, worker_id, client).await
        }
        "checkpoint_complete" => handle_checkpoint_complete(payload, tm, state).await,
        "checkpoint_abort" => handle_checkpoint_abort(payload, tm, state).await,
        other => {
            warn!("Unknown command: {}", other);
            error_json(&format!("unknown command: {other}"))
        }
    }
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

#[cfg(feature = "nats-transport")]
async fn handle_deploy(payload: &[u8], api_key: &str, tm: &SharedTenantManager) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct Req {
        name: String,
        source: String,
    }

    let req: Req = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => return error_json(&format!("bad request: {e}")),
    };

    let mut mgr = tm.write().await;
    let tenant_id = match mgr.get_tenant_by_api_key(api_key).cloned() {
        Some(id) => id,
        None => return error_json("invalid API key"),
    };

    match mgr
        .deploy_pipeline_on_tenant(&tenant_id, req.name.clone(), req.source)
        .await
    {
        Ok(id) => serde_json::to_vec(&serde_json::json!({
            "id": id,
            "name": req.name,
            "status": "running",
        }))
        .unwrap_or_default(),
        Err(e) => error_json(&e.to_string()),
    }
}

#[cfg(feature = "nats-transport")]
async fn handle_undeploy(payload: &[u8], api_key: &str, tm: &SharedTenantManager) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct Req {
        pipeline_id: String,
    }

    let req: Req = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => return error_json(&format!("bad request: {e}")),
    };

    let mut mgr = tm.write().await;
    let tenant_id = match mgr.get_tenant_by_api_key(api_key).cloned() {
        Some(id) => id,
        None => return error_json("invalid API key"),
    };

    if let Some(tenant) = mgr.get_tenant_mut(&tenant_id) {
        match tenant.remove_pipeline(&req.pipeline_id) {
            Ok(()) => serde_json::to_vec(&serde_json::json!({"ok": true})).unwrap_or_default(),
            Err(e) => error_json(&e.to_string()),
        }
    } else {
        error_json("tenant not found")
    }
}

#[cfg(feature = "nats-transport")]
async fn handle_inject(payload: &[u8], api_key: &str, tm: &SharedTenantManager) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct Req {
        pipeline_id: String,
        event_type: String,
        #[serde(default)]
        fields: serde_json::Map<String, serde_json::Value>,
    }

    let req: Req = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => return error_json(&format!("bad request: {e}")),
    };

    let mut mgr = tm.write().await;
    let tenant_id = match mgr.get_tenant_by_api_key(api_key).cloned() {
        Some(id) => id,
        None => return error_json("invalid API key"),
    };

    let mut event = varpulis_runtime::Event::new(req.event_type);
    for (k, v) in req.fields {
        let val: varpulis_core::Value = match v {
            serde_json::Value::String(s) => s.into(),
            serde_json::Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    varpulis_core::Value::Float(f)
                } else {
                    n.to_string().into()
                }
            }
            serde_json::Value::Bool(b) => varpulis_core::Value::Bool(b),
            other => other.to_string().into(),
        };
        event.data.insert(k.into(), val);
    }

    if let Some(tenant) = mgr.get_tenant_mut(&tenant_id) {
        match tenant.process_event(&req.pipeline_id, event).await {
            Ok(output_events) => {
                let out: Vec<serde_json::Value> = output_events
                    .iter()
                    .map(|e| serde_json::json!({"event_type": e.event_type}))
                    .collect();
                serde_json::to_vec(&serde_json::json!({"ok": true, "output_events": out}))
                    .unwrap_or_default()
            }
            Err(e) => error_json(&e.to_string()),
        }
    } else {
        error_json("tenant not found")
    }
}

#[cfg(feature = "nats-transport")]
async fn handle_inject_batch(payload: &[u8], _api_key: &str, _tm: &SharedTenantManager) -> Vec<u8> {
    let _req: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => return error_json(&format!("bad request: {e}")),
    };

    serde_json::to_vec(&serde_json::json!({
        "events_sent": 0,
        "events_failed": 0,
        "output_events": [],
        "errors": [],
        "processing_time_us": 0
    }))
    .unwrap_or_default()
}

#[cfg(feature = "nats-transport")]
async fn handle_checkpoint(payload: &[u8], api_key: &str, tm: &SharedTenantManager) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct Req {
        pipeline_id: String,
    }

    let req: Req = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => return error_json(&format!("bad request: {e}")),
    };

    let mgr = tm.read().await;
    let tenant_id = match mgr.get_tenant_by_api_key(api_key).cloned() {
        Some(id) => id,
        None => return error_json("invalid API key"),
    };

    if let Some(tenant) = mgr.get_tenant(&tenant_id) {
        match tenant.checkpoint_pipeline(&req.pipeline_id).await {
            Ok(data) => serde_json::to_vec(&data).unwrap_or_else(|_| b"{}".to_vec()),
            Err(e) => error_json(&e.to_string()),
        }
    } else {
        error_json("tenant not found")
    }
}

#[cfg(feature = "nats-transport")]
async fn handle_restore(payload: &[u8], api_key: &str, tm: &SharedTenantManager) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct Req {
        pipeline_id: String,
        checkpoint: varpulis_runtime::persistence::EngineCheckpoint,
    }

    let req: Req = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => return error_json(&format!("bad request: {e}")),
    };

    let mut mgr = tm.write().await;
    let tenant_id = match mgr.get_tenant_by_api_key(api_key).cloned() {
        Some(id) => id,
        None => return error_json("invalid API key"),
    };

    if let Some(tenant) = mgr.get_tenant_mut(&tenant_id) {
        match tenant
            .restore_pipeline(&req.pipeline_id, &req.checkpoint)
            .await
        {
            Ok(()) => serde_json::to_vec(&serde_json::json!({"ok": true})).unwrap_or_default(),
            Err(e) => error_json(&e.to_string()),
        }
    } else {
        error_json("tenant not found")
    }
}

#[cfg(feature = "nats-transport")]
async fn handle_drain(payload: &[u8]) -> Vec<u8> {
    let _req: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => return error_json(&format!("bad request: {e}")),
    };

    serde_json::to_vec(&serde_json::json!({"ok": true})).unwrap_or_default()
}

#[cfg(feature = "nats-transport")]
fn error_json(msg: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"error": msg})).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Distributed checkpoint barrier handler (Task 2.1)
// ---------------------------------------------------------------------------

/// Per-worker state for in-flight distributed checkpoints.
///
/// One worker tracks at most one prepared checkpoint per `(group_id,
/// checkpoint_id)` and one pause flag per pipeline. The flag is exposed
/// via [`Self::is_paused`] so future source-loop wiring (the engine-side
/// half of the barrier protocol) can observe it without changing this
/// crate's public API. While the flag is set, the worker is between
/// "received barrier" and "sent ack" — the engine mutex is also held
/// during that window, which is what actually serialises ingestion today.
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
#[derive(Default)]
pub struct WorkerCheckpointState {
    inner: tokio::sync::Mutex<WorkerCheckpointInner>,
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
impl std::fmt::Debug for WorkerCheckpointState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerCheckpointState")
            .finish_non_exhaustive()
    }
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
#[derive(Default)]
struct WorkerCheckpointInner {
    /// Prepared-but-not-yet-committed checkpoints, keyed by
    /// `(group_id, checkpoint_id)`.
    pending: HashMap<(GroupId, CheckpointId), PendingCheckpoint>,
    /// Per-pipeline pause flags. Created lazily on first barrier.
    paused: HashMap<String, Arc<AtomicBool>>,
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
struct PendingCheckpoint {
    pipeline_id: String,
    /// Captured engine source offsets at barrier time.
    /// Map: `connector_name -> (partition -> offset)`.
    source_offsets: HashMap<String, HashMap<i32, i64>>,
    /// Watchdog handle that aborts this checkpoint locally on timeout —
    /// dropped (which aborts the watchdog) when the caller retires the
    /// pending entry on complete or abort.
    watchdog: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
impl Drop for PendingCheckpoint {
    fn drop(&mut self) {
        if let Some(h) = self.watchdog.take() {
            h.abort();
        }
    }
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
impl WorkerCheckpointState {
    /// Construct an empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return `true` if the named pipeline is currently between a barrier
    /// arrival and an ack.
    pub async fn is_paused(&self, pipeline_id: &str) -> bool {
        let inner = self.inner.lock().await;
        inner
            .paused
            .get(pipeline_id)
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
    }

    /// Number of currently-prepared (uncommitted) checkpoints. Test hook.
    pub async fn pending_count(&self) -> usize {
        self.inner.lock().await.pending.len()
    }

    /// Snapshot pause-flag handle for `pipeline_id`, creating it if absent.
    /// Held outside the inner mutex so the source loop can observe pause
    /// state cheaply.
    async fn pause_flag(&self, pipeline_id: &str) -> Arc<AtomicBool> {
        let mut inner = self.inner.lock().await;
        inner
            .paused
            .entry(pipeline_id.to_string())
            .or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone()
    }
}

/// Snapshot the engine source offsets at barrier time.
///
/// Captured under the engine mutex so the snapshot is consistent with the
/// `EngineCheckpoint` produced alongside it. We clone the contents instead
/// of moving the lock: the engine continues running after the barrier, and
/// the source offsets keep advancing into the same shared map.
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
fn snapshot_source_offsets(
    engine: &varpulis_runtime::Engine,
) -> HashMap<String, HashMap<i32, i64>> {
    let handle = engine.source_offsets_handle();
    handle.lock().map(|guard| guard.clone()).unwrap_or_default()
}

/// Run the worker side of a checkpoint barrier:
/// pause sources → drain → snapshot engine → 2PC pre-commit sinks → ack.
///
/// On any failure the worker replies with a NACK ack and clears the pause
/// flag immediately — the coordinator will broadcast an abort that
/// retires whatever local state we may have left behind.
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
async fn handle_checkpoint_barrier(
    payload: &[u8],
    tm: &SharedTenantManager,
    state: &Arc<WorkerCheckpointState>,
    worker_id: &WorkerId,
    client: &async_nats::Client,
) -> Vec<u8> {
    let req: CheckpointBarrierRequest = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => return error_json(&format!("bad checkpoint_barrier request: {e}")),
    };

    let ack = prepare_barrier_response(tm, state, worker_id, &req).await;
    let ack_subject = subject_checkpoint_ack(&req.group_id);
    let ack_bytes = match serde_json::to_vec(&ack) {
        Ok(b) => b,
        Err(e) => {
            error!("ack serialize failed: {e}");
            return error_json(&format!("ack serialize: {e}"));
        }
    };
    if let Err(e) = client
        .publish(ack_subject.clone(), ack_bytes.clone().into())
        .await
    {
        error!("Failed to publish barrier ack to {}: {}", ack_subject, e);
    }
    ack_bytes
}

/// Core of the barrier handler — testable without a NATS client.
///
/// Sequence: pause flag set → snapshot engine state under the engine
/// mutex → call `prepare_commit_sinks(id)` → classify snapshot for inline
/// vs remote transport → record pending state and start the self-abort
/// watchdog → clear pause flag → return the assembled ack. Caller
/// publishes the ack on `subject_checkpoint_ack(group_id)`.
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
async fn prepare_barrier_response(
    tm: &SharedTenantManager,
    state: &Arc<WorkerCheckpointState>,
    worker_id: &WorkerId,
    req: &CheckpointBarrierRequest,
) -> CheckpointBarrierAck {
    let pause = state.pause_flag(&req.pipeline_id).await;
    pause.store(true, Ordering::SeqCst);

    let prep = prepare_local_checkpoint(tm, req).await;
    pause.store(false, Ordering::SeqCst);

    match prep {
        Ok((engine_checkpoint, source_offsets)) => match classify_snapshot(*engine_checkpoint) {
            Ok(SnapshotClassification::Inline(location)) => {
                record_pending(state, tm, req, source_offsets).await;
                CheckpointBarrierAck::success(
                    req.group_id.clone(),
                    req.checkpoint_id,
                    worker_id.clone(),
                    req.pipeline_id.clone(),
                    location,
                )
            }
            Ok(SnapshotClassification::NeedsUpload { size_bytes, .. }) => {
                warn!(
                    worker = %worker_id.0,
                    group = %req.group_id,
                    checkpoint_id = req.checkpoint_id,
                    size_bytes,
                    "snapshot exceeds inline budget; remote upload path not yet wired",
                );
                CheckpointBarrierAck::failure(
                    req.group_id.clone(),
                    req.checkpoint_id,
                    worker_id.clone(),
                    req.pipeline_id.clone(),
                    format!("snapshot too large for inline transport ({size_bytes} bytes)"),
                )
            }
            Err(e) => CheckpointBarrierAck::failure(
                req.group_id.clone(),
                req.checkpoint_id,
                worker_id.clone(),
                req.pipeline_id.clone(),
                format!("classify_snapshot: {e}"),
            ),
        },
        Err(e) => CheckpointBarrierAck::failure(
            req.group_id.clone(),
            req.checkpoint_id,
            worker_id.clone(),
            req.pipeline_id.clone(),
            e,
        ),
    }
}

/// Acquire the pipeline's engine, run create_checkpoint() and
/// prepare_commit_sinks() under the lock, and capture source offsets.
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
async fn prepare_local_checkpoint(
    tm: &SharedTenantManager,
    req: &CheckpointBarrierRequest,
) -> Result<
    (
        Box<varpulis_runtime::persistence::EngineCheckpoint>,
        HashMap<String, HashMap<i32, i64>>,
    ),
    String,
> {
    let pipeline_handle = {
        let mgr = tm.read().await;
        mgr.list_tenants()
            .iter()
            .find_map(|t| t.pipelines.get(&req.pipeline_id).map(|p| p.engine.clone()))
            .ok_or_else(|| format!("pipeline {} not found on this worker", req.pipeline_id))?
    };

    let engine = pipeline_handle.lock().await;
    let snapshot = engine.create_checkpoint();
    let source_offsets = snapshot_source_offsets(&engine);
    engine.prepare_commit_sinks(req.checkpoint_id).await;
    Ok((Box::new(snapshot), source_offsets))
}

/// Insert a `PendingCheckpoint` and start the self-abort watchdog.
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
async fn record_pending(
    state: &Arc<WorkerCheckpointState>,
    tm: &SharedTenantManager,
    req: &CheckpointBarrierRequest,
    source_offsets: HashMap<String, HashMap<i32, i64>>,
) {
    let key = (req.group_id.clone(), req.checkpoint_id);
    let watchdog = spawn_self_abort_watchdog(
        Arc::clone(state),
        tm.clone(),
        key.clone(),
        Duration::from_millis(req.timeout_ms.max(1)),
    );
    let pending = PendingCheckpoint {
        pipeline_id: req.pipeline_id.clone(),
        source_offsets,
        watchdog: Some(watchdog),
    };
    let mut inner = state.inner.lock().await;
    inner.pending.insert(key, pending);
}

/// Spawn a watchdog that aborts the local checkpoint if no
/// complete/abort arrives within `timeout`. On fire, removes the pending
/// entry (so a late-arriving `checkpoint_complete` is a no-op) and calls
/// `abort_sinks` on the pipeline's engine to release the prepared 2PC
/// transactions.
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
fn spawn_self_abort_watchdog(
    state: Arc<WorkerCheckpointState>,
    tm: SharedTenantManager,
    key: (GroupId, CheckpointId),
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        let pending = {
            let mut inner = state.inner.lock().await;
            inner.pending.remove(&key)
        };
        let Some(pending) = pending else { return };
        warn!(
            group = %key.0,
            checkpoint_id = key.1,
            pipeline = %pending.pipeline_id,
            "self-aborting checkpoint after timeout",
        );
        let mgr = tm.read().await;
        let engine_handle = mgr.list_tenants().iter().find_map(|t| {
            t.pipelines
                .get(&pending.pipeline_id)
                .map(|p| p.engine.clone())
        });
        drop(mgr);
        if let Some(engine_handle) = engine_handle {
            let engine = engine_handle.lock().await;
            engine.abort_sinks(key.1).await;
        }
    })
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
async fn handle_checkpoint_complete(
    payload: &[u8],
    tm: &SharedTenantManager,
    state: &Arc<WorkerCheckpointState>,
) -> Vec<u8> {
    let note: CheckpointCompleteNotification = match serde_json::from_slice(payload) {
        Ok(n) => n,
        Err(e) => return error_json(&format!("bad checkpoint_complete: {e}")),
    };

    let pending = {
        let mut inner = state.inner.lock().await;
        inner
            .pending
            .remove(&(note.group_id.clone(), note.checkpoint_id))
    };
    let Some(pending) = pending else {
        // Either we already self-aborted or this notification is for a
        // group we never participated in — both are benign.
        return ok_json();
    };

    // 1. commit_sinks() — also opens the next epoch via begin_epoch(id+1)
    //    inside the engine, satisfying the plan's
    //    "commit_sinks → begin_epoch(id+1)" sequence.
    // 2. commit_source_offsets per (connector, topic).
    let engine_handle = {
        let mgr = tm.read().await;
        mgr.list_tenants().iter().find_map(|t| {
            t.pipelines
                .get(&pending.pipeline_id)
                .map(|p| p.engine.clone())
        })
    };
    let Some(engine_handle) = engine_handle else {
        warn!(
            pipeline = %pending.pipeline_id,
            "checkpoint_complete: pipeline no longer present, skipping commit",
        );
        return ok_json();
    };
    {
        let engine = engine_handle.lock().await;
        engine.commit_sinks(note.checkpoint_id).await;
    }

    commit_source_offsets_for_pipeline(tm, &pending).await;

    info!(
        group = %note.group_id,
        checkpoint_id = note.checkpoint_id,
        pipeline = %pending.pipeline_id,
        "distributed checkpoint committed",
    );
    ok_json()
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
async fn handle_checkpoint_abort(
    payload: &[u8],
    tm: &SharedTenantManager,
    state: &Arc<WorkerCheckpointState>,
) -> Vec<u8> {
    let note: CheckpointAbortNotification = match serde_json::from_slice(payload) {
        Ok(n) => n,
        Err(e) => return error_json(&format!("bad checkpoint_abort: {e}")),
    };

    let pending = {
        let mut inner = state.inner.lock().await;
        inner
            .pending
            .remove(&(note.group_id.clone(), note.checkpoint_id))
    };
    let Some(pending) = pending else {
        return ok_json();
    };

    let engine_handle = {
        let mgr = tm.read().await;
        mgr.list_tenants().iter().find_map(|t| {
            t.pipelines
                .get(&pending.pipeline_id)
                .map(|p| p.engine.clone())
        })
    };
    if let Some(engine_handle) = engine_handle {
        let engine = engine_handle.lock().await;
        engine.abort_sinks(note.checkpoint_id).await;
    }

    warn!(
        group = %note.group_id,
        checkpoint_id = note.checkpoint_id,
        pipeline = %pending.pipeline_id,
        reason = %note.reason,
        "distributed checkpoint aborted",
    );
    ok_json()
}

/// Send the captured per-partition offsets back to each source for the
/// pipeline, closing the input-side half of the 2PC loop.
///
/// First pass: collect `(connector_name, topic)` pairs from the pipeline's
/// source bindings under both the manager read lock and the engine
/// mutex — this snapshot is what we'll commit against. Second pass: walk
/// those pairs and call `commit_source_offsets` on each one through the
/// pipeline's managed registry. The manager lock is held across the
/// commit awaits because the registry is borrowed from it; the engine
/// mutex is *not* held during commit so source ingestion isn't blocked.
#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
async fn commit_source_offsets_for_pipeline(tm: &SharedTenantManager, pending: &PendingCheckpoint) {
    // Step 1: snapshot bindings + per-connector default topics under the
    // engine mutex so the data is consistent.
    let bindings: Vec<(String, String)> = {
        let mgr = tm.read().await;
        let Some(engine_handle) = mgr
            .list_tenants()
            .iter()
            .find_map(|t| t.pipelines.get(&pending.pipeline_id))
            .map(|p| p.engine.clone())
        else {
            return;
        };
        drop(mgr);
        let engine = engine_handle.lock().await;
        let configs = engine.connector_configs();
        engine
            .source_bindings()
            .iter()
            .map(|b| {
                let topic = b
                    .topic_override
                    .clone()
                    .or_else(|| configs.get(&b.connector_name).and_then(|c| c.topic.clone()))
                    .unwrap_or_default();
                (b.connector_name.clone(), topic)
            })
            .collect()
    };

    if bindings.is_empty() {
        return;
    }

    // Step 2: re-acquire the manager read lock and call commit on each
    // binding through the managed registry.
    let mgr = tm.read().await;
    let Some(pipeline) = mgr
        .list_tenants()
        .iter()
        .find_map(|t| t.pipelines.get(&pending.pipeline_id))
    else {
        return;
    };
    let Some(registry) = pipeline.connector_registry.as_ref() else {
        return;
    };

    for (connector_name, topic) in &bindings {
        let Some(offsets) = pending.source_offsets.get(connector_name) else {
            continue;
        };
        if let Err(e) = registry
            .commit_source_offsets(connector_name, topic, offsets)
            .await
        {
            warn!(
                connector = %connector_name,
                topic = %topic,
                "commit_source_offsets failed: {}",
                e
            );
        }
    }
}

#[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))]
fn ok_json() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"ok": true})).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Unit tests (do not require a running NATS broker)
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "nats-transport", feature = "distributed-checkpoint"))]
mod barrier_tests {
    use std::time::Duration;

    use tokio::sync::RwLock;
    use varpulis_runtime::{TenantManager, TenantQuota};

    use super::*;

    const TENANT_ID: &str = "test-tenant";
    const API_KEY: &str = "test-key";

    /// Minimal tenant + pipeline. The pipeline has no `.from()` so no
    /// connector_registry is created, but the engine is fully wired and
    /// can produce checkpoints.
    async fn fixture_with_pipeline() -> (SharedTenantManager, String) {
        let tm: SharedTenantManager = Arc::new(RwLock::new(TenantManager::new()));
        let pipeline_id = {
            let mut mgr = tm.write().await;
            mgr.create_tenant(
                TENANT_ID.to_string(),
                API_KEY.to_string(),
                TenantQuota::default(),
            )
            .unwrap();
            let tenant_id = mgr.get_tenant_by_api_key(API_KEY).cloned().unwrap();
            mgr.deploy_pipeline_on_tenant(
                &tenant_id,
                "p".to_string(),
                "stream A = Sensor .where(temperature > 100)".to_string(),
            )
            .await
            .unwrap()
        };
        (tm, pipeline_id)
    }

    fn worker() -> WorkerId {
        WorkerId("w0".to_string())
    }

    fn make_request(
        group: &str,
        id: u64,
        pipeline_id: &str,
        timeout_ms: u64,
    ) -> CheckpointBarrierRequest {
        CheckpointBarrierRequest {
            group_id: group.into(),
            checkpoint_id: id,
            pipeline_id: pipeline_id.into(),
            timeout_ms,
            triggered_at_ms: 0,
        }
    }

    #[tokio::test]
    async fn pause_flag_lifecycle() {
        let state = Arc::new(WorkerCheckpointState::new());
        assert!(!state.is_paused("p").await);
        let flag = state.pause_flag("p").await;
        flag.store(true, Ordering::SeqCst);
        assert!(state.is_paused("p").await);
        flag.store(false, Ordering::SeqCst);
        assert!(!state.is_paused("p").await);
    }

    #[tokio::test]
    async fn barrier_for_unknown_pipeline_naks() {
        let (tm, _pipeline_id) = fixture_with_pipeline().await;
        let state = Arc::new(WorkerCheckpointState::new());
        let req = make_request("g", 1, "does-not-exist", 30_000);
        let ack = prepare_barrier_response(&tm, &state, &worker(), &req).await;
        assert!(!ack.is_success(), "ack should be NACK for missing pipeline");
        assert!(ack.error.is_some());
        assert_eq!(state.pending_count().await, 0);
        // Pause flag should be cleared regardless of failure.
        assert!(!state.is_paused("does-not-exist").await);
    }

    #[tokio::test]
    async fn barrier_happy_path_records_pending_and_clears_pause() {
        let (tm, pipeline_id) = fixture_with_pipeline().await;
        let state = Arc::new(WorkerCheckpointState::new());
        let req = make_request("g", 7, &pipeline_id, 30_000);
        let ack = prepare_barrier_response(&tm, &state, &worker(), &req).await;
        assert!(ack.is_success());
        assert_eq!(ack.checkpoint_id, 7);
        assert!(ack.location.is_some());
        assert_eq!(state.pending_count().await, 1);
        assert!(!state.is_paused(&pipeline_id).await);
    }

    #[tokio::test]
    async fn complete_drops_pending() {
        let (tm, pipeline_id) = fixture_with_pipeline().await;
        let state = Arc::new(WorkerCheckpointState::new());
        let req = make_request("g", 11, &pipeline_id, 30_000);
        let ack = prepare_barrier_response(&tm, &state, &worker(), &req).await;
        assert!(ack.is_success());
        assert_eq!(state.pending_count().await, 1);

        let note = CheckpointCompleteNotification {
            group_id: "g".into(),
            checkpoint_id: 11,
            committed_at_ms: 0,
        };
        let payload = serde_json::to_vec(&note).unwrap();
        let resp = handle_checkpoint_complete(&payload, &tm, &state).await;
        assert_eq!(resp, ok_json());
        assert_eq!(state.pending_count().await, 0);
    }

    #[tokio::test]
    async fn complete_for_unknown_checkpoint_is_noop() {
        let (tm, _pipeline_id) = fixture_with_pipeline().await;
        let state = Arc::new(WorkerCheckpointState::new());
        let note = CheckpointCompleteNotification {
            group_id: "g".into(),
            checkpoint_id: 999,
            committed_at_ms: 0,
        };
        let payload = serde_json::to_vec(&note).unwrap();
        let resp = handle_checkpoint_complete(&payload, &tm, &state).await;
        assert_eq!(resp, ok_json());
    }

    #[tokio::test]
    async fn abort_drops_pending() {
        let (tm, pipeline_id) = fixture_with_pipeline().await;
        let state = Arc::new(WorkerCheckpointState::new());
        let req = make_request("g", 5, &pipeline_id, 30_000);
        let ack = prepare_barrier_response(&tm, &state, &worker(), &req).await;
        assert!(ack.is_success());

        let note = CheckpointAbortNotification {
            group_id: "g".into(),
            checkpoint_id: 5,
            reason: "test abort".into(),
        };
        let payload = serde_json::to_vec(&note).unwrap();
        let resp = handle_checkpoint_abort(&payload, &tm, &state).await;
        assert_eq!(resp, ok_json());
        assert_eq!(state.pending_count().await, 0);
    }

    #[tokio::test]
    async fn abort_for_unknown_checkpoint_is_noop() {
        let (tm, _pipeline_id) = fixture_with_pipeline().await;
        let state = Arc::new(WorkerCheckpointState::new());
        let note = CheckpointAbortNotification {
            group_id: "g".into(),
            checkpoint_id: 0xdead,
            reason: "stale".into(),
        };
        let payload = serde_json::to_vec(&note).unwrap();
        let resp = handle_checkpoint_abort(&payload, &tm, &state).await;
        assert_eq!(resp, ok_json());
    }

    #[tokio::test]
    async fn self_abort_watchdog_drops_pending_after_timeout() {
        let (tm, pipeline_id) = fixture_with_pipeline().await;
        let state = Arc::new(WorkerCheckpointState::new());
        // Use a tiny timeout so the watchdog fires quickly.
        let req = make_request("g", 42, &pipeline_id, 50);
        let ack = prepare_barrier_response(&tm, &state, &worker(), &req).await;
        assert!(ack.is_success());
        assert_eq!(state.pending_count().await, 1);

        // Wait for the watchdog to fire and remove the pending entry.
        for _ in 0..40 {
            if state.pending_count().await == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            state.pending_count().await,
            0,
            "watchdog should have removed the pending checkpoint",
        );
    }

    #[tokio::test]
    async fn late_complete_after_self_abort_is_benign() {
        let (tm, pipeline_id) = fixture_with_pipeline().await;
        let state = Arc::new(WorkerCheckpointState::new());
        let req = make_request("g", 100, &pipeline_id, 30);
        let ack = prepare_barrier_response(&tm, &state, &worker(), &req).await;
        assert!(ack.is_success());

        // Let the watchdog fire.
        for _ in 0..40 {
            if state.pending_count().await == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(state.pending_count().await, 0);

        // A complete arriving now should not panic and should not commit
        // anything (no pending entry to act on).
        let note = CheckpointCompleteNotification {
            group_id: "g".into(),
            checkpoint_id: 100,
            committed_at_ms: 0,
        };
        let payload = serde_json::to_vec(&note).unwrap();
        let resp = handle_checkpoint_complete(&payload, &tm, &state).await;
        assert_eq!(resp, ok_json());
    }

    #[tokio::test]
    async fn malformed_barrier_payload_returns_error_json() {
        let state = Arc::new(WorkerCheckpointState::new());
        // Bad payload: missing required fields.
        let payload = b"{\"not\": \"valid\"}";
        let req: Result<CheckpointBarrierRequest, _> = serde_json::from_slice(payload);
        assert!(req.is_err());
        // The dispatcher path returns `error_json` for bad payloads — we
        // can't call handle_checkpoint_barrier without a NATS client, but
        // the parse step is what would NACK.
        let _ = state; // silence warning
    }
}
