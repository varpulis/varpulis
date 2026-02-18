//! Coordinator-side NATS handlers.
//!
//! Handles worker registration (request/reply) and heartbeat (pub/sub)
//! messages received over NATS.

#[cfg(feature = "nats-transport")]
use crate::api::SharedCoordinator;
#[cfg(feature = "nats-transport")]
use crate::nats_transport;
#[cfg(feature = "nats-transport")]
use crate::worker::{
    HeartbeatRequest, RegisterWorkerRequest, RegisterWorkerResponse, WorkerId, WorkerNode,
    WorkerStatus,
};
#[cfg(feature = "nats-transport")]
use tracing::{error, info, warn};

/// Run the coordinator-side NATS handler.
///
/// Subscribes to:
/// 1. `varpulis.cluster.register` — worker registration (request/reply)
/// 2. `varpulis.cluster.heartbeat.>` — worker heartbeats (pub/sub wildcard)
#[cfg(feature = "nats-transport")]
pub async fn run_coordinator_nats_handler(
    client: async_nats::Client,
    coordinator: SharedCoordinator,
) {
    use futures_util::StreamExt;

    // Subscribe to registration subject
    let reg_subject = nats_transport::subject_register();
    let mut reg_sub = match client.subscribe(reg_subject.clone()).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to subscribe to {}: {}", reg_subject, e);
            return;
        }
    };

    // Subscribe to heartbeat wildcard
    let hb_subject = "varpulis.cluster.heartbeat.>";
    let mut hb_sub = match client.subscribe(hb_subject.to_string()).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to subscribe to {}: {}", hb_subject, e);
            return;
        }
    };

    info!(
        "Coordinator NATS handler listening on {} and {}",
        reg_subject, hb_subject
    );

    loop {
        tokio::select! {
            Some(msg) = reg_sub.next() => {
                let response = handle_registration(&msg.payload, &coordinator).await;
                if let Some(reply_subject) = msg.reply {
                    if let Err(e) = client.publish(reply_subject, response.into()).await {
                        error!("Failed to send registration reply: {}", e);
                    }
                }
            }
            Some(msg) = hb_sub.next() => {
                handle_heartbeat_message(&msg.subject, &msg.payload, &coordinator).await;
            }
            else => break,
        }
    }
}

#[cfg(feature = "nats-transport")]
async fn handle_registration(payload: &[u8], coordinator: &SharedCoordinator) -> Vec<u8> {
    let req: RegisterWorkerRequest = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => {
            warn!("Invalid registration request: {}", e);
            return serde_json::to_vec(&serde_json::json!({
                "error": format!("bad request: {e}")
            }))
            .unwrap_or_default();
        }
    };

    let worker_id = req.worker_id.clone();
    info!("Worker '{}' registering via NATS", worker_id);

    let node = WorkerNode {
        id: WorkerId(req.worker_id.clone()),
        address: req.address,
        api_key: req.api_key,
        status: WorkerStatus::Ready,
        capacity: req.capacity,
        last_heartbeat: std::time::Instant::now(),
        assigned_pipelines: Vec::new(),
        events_processed: 0,
    };

    let mut coord = coordinator.write().await;
    let registered_id = coord.register_worker(node);
    let heartbeat_interval = coord.heartbeat_interval.as_secs();

    let resp = RegisterWorkerResponse {
        worker_id: registered_id.0,
        status: "registered".to_string(),
        heartbeat_interval_secs: Some(heartbeat_interval),
    };

    serde_json::to_vec(&resp).unwrap_or_default()
}

#[cfg(feature = "nats-transport")]
async fn handle_heartbeat_message(subject: &str, payload: &[u8], coordinator: &SharedCoordinator) {
    // Extract worker_id from subject: varpulis.cluster.heartbeat.{worker_id}
    let worker_id = match subject.rsplit('.').next() {
        Some(id) => id,
        None => {
            warn!("Malformed heartbeat subject: {}", subject);
            return;
        }
    };

    let hb: HeartbeatRequest = match serde_json::from_slice(payload) {
        Ok(h) => h,
        Err(e) => {
            warn!("Invalid heartbeat from {}: {}", worker_id, e);
            return;
        }
    };

    let wid = WorkerId(worker_id.to_string());
    let mut coord = coordinator.write().await;
    if let Err(e) = coord.heartbeat(&wid, &hb) {
        warn!("Heartbeat error for {}: {}", worker_id, e);
    }
}
