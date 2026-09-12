//! Coordinator-side NATS handlers.
//!
//! Handles worker registration (request/reply) and heartbeat messages received
//! over NATS.
//!
//! Registration stays on core NATS request/reply — the reply *is* the
//! acknowledgement, and the worker retries with backoff. Heartbeats are events
//! and go through the cluster `ClusterTransport` (`nats_jetstream`), so on the
//! JetStream substrate a heartbeat published while the coordinator is
//! restarting is still there when it comes back instead of being lost into a
//! liveness decision.

#[cfg(feature = "nats-transport")]
use tracing::{error, info, warn};

#[cfg(feature = "nats-transport")]
use crate::api::SharedCoordinator;
#[cfg(feature = "nats-transport")]
use crate::nats_jetstream::{ClusterTransport, Delivery, Substrate};
#[cfg(feature = "nats-transport")]
use crate::nats_transport;
#[cfg(feature = "nats-transport")]
use crate::worker::{
    HeartbeatRequest, RegisterWorkerRequest, RegisterWorkerResponse, WorkerId, WorkerNode,
    WorkerStatus,
};

/// Run the coordinator-side NATS handler on the historical core pub/sub
/// substrate.
///
/// Kept as-is for backwards compatibility: a deployment whose `nats-server` has
/// no JetStream keeps working unchanged. For the durable path use
/// [`run_coordinator_nats_handler_with`].
#[cfg(feature = "nats-transport")]
pub async fn run_coordinator_nats_handler(
    client: async_nats::Client,
    coordinator: SharedCoordinator,
) {
    run_coordinator_nats_handler_with(client, coordinator, Substrate::Core, "coordinator").await;
}

/// Run the coordinator-side NATS handler on an explicit substrate.
///
/// Subscribes to:
/// 1. `varpulis.cluster.register` — worker registration (request/reply, always
///    core NATS).
/// 2. `varpulis.cluster.heartbeat.>` — worker heartbeats, over `substrate`.
///    On [`Substrate::JetStream`] this is a durable consumer named after
///    `coordinator_id`, acked only after the heartbeat has been applied to
///    coordinator state.
///
/// Returns early (after logging) if JetStream is requested but the server does
/// not provide it — it does not quietly degrade to core.
#[cfg(feature = "nats-transport")]
pub async fn run_coordinator_nats_handler_with(
    client: async_nats::Client,
    coordinator: SharedCoordinator,
    substrate: Substrate,
    coordinator_id: &str,
) {
    use futures_util::StreamExt;

    // Subscribe to registration subject (core request/reply on every substrate).
    let reg_subject = nats_transport::subject_register();
    let mut reg_sub = match client.subscribe(reg_subject.clone()).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to subscribe to {}: {}", reg_subject, e);
            return;
        }
    };

    let transport =
        match ClusterTransport::from_client(client.clone(), "<connected>", substrate).await {
            Ok(t) => t,
            Err(e) => {
                error!("Coordinator NATS transport unavailable: {}", e);
                return;
            }
        };

    let mut hb_sub = match transport.consume_heartbeats(coordinator_id).await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to consume heartbeats: {}", e);
            return;
        }
    };

    info!(
        "Coordinator NATS handler listening on {} and {} (substrate: {}, durable: {})",
        reg_subject,
        hb_sub.subject(),
        substrate.as_str(),
        hb_sub.is_durable(),
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
            Some(delivery) = hb_sub.next() => {
                handle_heartbeat_delivery(delivery, &coordinator).await;
            }
            else => break,
        }
    }
}

/// Apply one heartbeat delivery, then acknowledge it.
///
/// The ack happens **after** the heartbeat has been applied to coordinator
/// state, never on receipt: acking first is the silent-loss pattern the audit
/// found in the Redis Streams connector (`XACK` before apply). A crash between
/// apply and ack redelivers — at-least-once, and a heartbeat is idempotent.
#[cfg(feature = "nats-transport")]
async fn handle_heartbeat_delivery(delivery: Delivery, coordinator: &SharedCoordinator) {
    // A durable consumer replays what it missed. That is the point for a
    // heartbeat the coordinator was not up to hear — but a heartbeat older than
    // the liveness timeout must not be *applied*, because `Coordinator::heartbeat`
    // stamps `last_heartbeat = now` and would resurrect a worker that has since
    // died. Such a delivery is stale, not poison: ack it and move on.
    if let Some(age) = delivery.age() {
        let timeout = coordinator.read().await.heartbeat_timeout;
        if age > timeout {
            warn!(
                subject = %delivery.subject(),
                age_secs = age.as_secs(),
                timeout_secs = timeout.as_secs(),
                "discarding replayed heartbeat older than the liveness timeout"
            );
            if let Err(e) = delivery.ack().await {
                warn!("Failed to ack stale heartbeat: {}", e);
            }
            return;
        }
    }

    let subject = delivery.subject().to_string();
    handle_heartbeat_message(&subject, delivery.payload(), coordinator).await;

    // Acked whether or not the heartbeat was usable. A malformed payload or an
    // unknown worker id is not something redelivery can fix, so naking it would
    // only build an unbounded redelivery loop against the liveness path; the
    // failure is already logged by `handle_heartbeat_message`.
    if let Err(e) = delivery.ack().await {
        warn!("Failed to ack heartbeat on {}: {}", subject, e);
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
        api_key: varpulis_core::security::SecretString::new(req.api_key),
        status: WorkerStatus::Ready,
        capacity: req.capacity,
        last_heartbeat: std::time::Instant::now(),
        assigned_pipelines: Vec::new(),
        events_processed: 0,
        heartbeat_seq: 0,
        last_seen_hb_seq: 0,
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
        // A bad heartbeat has nothing to replicate; under `raft` we must skip the
        // replication below (this early return is compiled out when `raft` is off,
        // where there is no trailing code and the return would be redundant).
        #[cfg(feature = "raft")]
        return;
    }
    // Replicate the heartbeat metrics + monotonic `heartbeat_seq` through Raft,
    // exactly like the HTTP heartbeat handler (`api::handle_heartbeat`), via the
    // shared leader-write-or-forward helper. Without this, the NATS path would
    // advance only the *local* `heartbeat_seq` (via `heartbeat()` above) and
    // never replicate it — so in a multi-coordinator NATS deployment a worker
    // homed on a *non-leader* would have its liveness invisible to the leader's
    // `sync_from_raft`, which could then false-mark it `Unhealthy` (audit C5).
    // No-op when `raft` is off (single-coordinator liveness needs no replication).
    #[cfg(feature = "raft")]
    crate::api::replicate_heartbeat(coord, worker_id, &hb).await;
}
