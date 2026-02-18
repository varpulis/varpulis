//! Worker-side NATS command handler.
//!
//! Listens for coordinator commands (deploy, undeploy, inject, etc.) over NATS
//! and executes them against the local `TenantManager`.
//!
//! This replaces the HTTP API endpoints that the coordinator previously used
//! to control workers.

#[cfg(feature = "nats-transport")]
use crate::nats_transport::subject_cmd_wildcard;
#[cfg(feature = "nats-transport")]
use tracing::{error, info, warn};
#[cfg(feature = "nats-transport")]
use varpulis_runtime::SharedTenantManager;

/// Run the worker-side NATS command handler.
///
/// Subscribes to `varpulis.cluster.cmd.{worker_id}.>` and dispatches
/// incoming commands to the tenant manager.
#[cfg(feature = "nats-transport")]
pub async fn run_worker_nats_handler(
    client: async_nats::Client,
    worker_id: &str,
    api_key: &str,
    tenant_manager: SharedTenantManager,
) {
    use futures_util::StreamExt;

    let subject = subject_cmd_wildcard(worker_id);
    let mut sub = match client.subscribe(subject.clone()).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to subscribe to {}: {}", subject, e);
            return;
        }
    };

    let api_key = api_key.to_string();
    info!("Worker {} listening for commands on {}", worker_id, subject);

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

#[cfg(feature = "nats-transport")]
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
