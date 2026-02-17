//! WebSocket persistent connections for fast failure detection (~50ms).
//!
//! Protocol: Workers maintain a persistent WS connection to the coordinator.
//! On TCP connection drop, the coordinator immediately detects worker failure
//! (vs ~15s with REST heartbeat polling).

use crate::worker::{HeartbeatRequest, WorkerId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// Messages sent from worker to coordinator over WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WorkerMessage {
    /// Worker identifies itself after connecting.
    Identify { worker_id: String, api_key: String },
    /// Periodic heartbeat with metrics (replaces REST heartbeat when WS is active).
    Heartbeat(HeartbeatRequest),
}

/// Messages sent from coordinator to worker over WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CoordinatorMessage {
    /// Acknowledge successful identification.
    IdentifyAck { heartbeat_interval_secs: u64 },
    /// Acknowledge heartbeat receipt.
    HeartbeatAck,
    /// Reject connection (bad credentials, etc.).
    Error { message: String },
}

/// Manages active WebSocket connections from workers.
pub struct WsConnectionManager {
    connections: HashMap<WorkerId, mpsc::UnboundedSender<CoordinatorMessage>>,
}

impl WsConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    /// Register a new worker connection.
    pub fn add(&mut self, worker_id: WorkerId, sender: mpsc::UnboundedSender<CoordinatorMessage>) {
        self.connections.insert(worker_id, sender);
    }

    /// Remove a worker connection (on disconnect).
    pub fn remove(&mut self, worker_id: &WorkerId) {
        self.connections.remove(worker_id);
    }

    /// Check if a worker has an active WS connection.
    pub fn is_connected(&self, worker_id: &WorkerId) -> bool {
        self.connections
            .get(worker_id)
            .map(|s| !s.is_closed())
            .unwrap_or(false)
    }

    /// Number of active WS connections.
    pub fn connected_count(&self) -> usize {
        self.connections.values().filter(|s| !s.is_closed()).count()
    }

    /// Send a message to a specific worker.
    pub fn send(&self, worker_id: &WorkerId, msg: CoordinatorMessage) -> Result<(), String> {
        match self.connections.get(worker_id) {
            Some(sender) => sender
                .send(msg)
                .map_err(|e| format!("Failed to send to {}: {}", worker_id, e)),
            None => Err(format!("Worker {} not connected via WS", worker_id)),
        }
    }
}

impl Default for WsConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared WebSocket connection manager.
pub type SharedWsManager = Arc<RwLock<WsConnectionManager>>;

/// Create a new shared WebSocket connection manager.
pub fn shared_ws_manager() -> SharedWsManager {
    Arc::new(RwLock::new(WsConnectionManager::new()))
}

/// Handle a WebSocket connection from a worker (coordinator-side).
///
/// This function is called when a worker upgrades to WebSocket.
/// It handles the identify/heartbeat protocol and triggers immediate
/// failure detection on disconnect.
pub async fn handle_worker_ws(
    ws: warp::ws::WebSocket,
    coordinator: crate::api::SharedCoordinator,
    ws_manager: SharedWsManager,
    rbac: Arc<crate::rbac::RbacConfig>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tracing::{error, info, warn};

    let (mut ws_tx, mut ws_rx) = ws.split();
    let mut identified_worker: Option<WorkerId> = None;

    // Create channel for sending messages back to this worker
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<CoordinatorMessage>();

    // Spawn task to forward coordinator messages to the WebSocket
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            match serde_json::to_string(&msg) {
                Ok(text) => {
                    if ws_tx.send(warp::ws::Message::text(text)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to serialize coordinator message: {}", e);
                }
            }
        }
    });

    // Process incoming messages from the worker
    while let Some(result) = ws_rx.next().await {
        let msg = match result {
            Ok(msg) => {
                if msg.is_close() {
                    break;
                }
                if msg.is_ping() || msg.is_pong() {
                    continue;
                }
                msg
            }
            Err(e) => {
                warn!("WebSocket error: {}", e);
                break;
            }
        };

        let text = match msg.to_str() {
            Ok(t) => t,
            Err(_) => continue,
        };

        let worker_msg: WorkerMessage = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                warn!("Invalid WS message: {}", e);
                let _ = msg_tx.send(CoordinatorMessage::Error {
                    message: format!("Invalid message: {}", e),
                });
                continue;
            }
        };

        match worker_msg {
            WorkerMessage::Identify { worker_id, api_key } => {
                // Validate API key against RBAC config (workers need at least Operator role)
                if !rbac.allow_anonymous {
                    let auth_ok = rbac
                        .authenticate(Some(&api_key))
                        .is_some_and(|role| role.has_permission(crate::rbac::Role::Operator));
                    if !auth_ok {
                        warn!(worker_id = %worker_id, "WebSocket authentication rejected");
                        let _ = msg_tx.send(CoordinatorMessage::Error {
                            message: "Invalid API key".to_string(),
                        });
                        break;
                    }
                }

                let wid = WorkerId(worker_id.clone());
                info!("Worker {} identified via WebSocket", worker_id);

                // Get heartbeat interval from coordinator and clear WS disconnect grace
                let interval = {
                    let mut coord = coordinator.write().await;
                    if let Some(worker) = coord.workers.get_mut(&wid) {
                        worker.ws_disconnected_at = None;
                    }
                    coord.heartbeat_interval.as_secs()
                };

                // Register connection in manager
                {
                    let mut mgr = ws_manager.write().await;
                    mgr.add(wid.clone(), msg_tx.clone());
                }

                identified_worker = Some(wid);
                let _ = msg_tx.send(CoordinatorMessage::IdentifyAck {
                    heartbeat_interval_secs: interval,
                });
            }

            WorkerMessage::Heartbeat(hb) => {
                if let Some(ref wid) = identified_worker {
                    // Update heartbeat on coordinator (same as REST path)
                    let mut coord = coordinator.write().await;
                    if let Err(e) = coord.heartbeat(wid, &hb) {
                        warn!("WS heartbeat error for {}: {}", wid, e);
                    }
                    let _ = msg_tx.send(CoordinatorMessage::HeartbeatAck);
                } else {
                    let _ = msg_tx.send(CoordinatorMessage::Error {
                        message: "Must identify before sending heartbeats".to_string(),
                    });
                }
            }
        }
    }

    // Worker disconnected — start grace period instead of immediate failover.
    // The health_sweep will trigger failover if the WS doesn't reconnect within
    // the grace period.
    if let Some(ref wid) = identified_worker {
        warn!(
            "Worker {} WebSocket disconnected — starting grace period before failover",
            wid
        );

        // Remove from connection manager
        {
            let mut mgr = ws_manager.write().await;
            mgr.remove(wid);
        }

        // Set disconnected_at for grace period (health_sweep will check it)
        {
            let mut coord = coordinator.write().await;
            if let Some(worker) = coord.workers.get_mut(wid) {
                if worker.status == crate::worker::WorkerStatus::Ready {
                    worker.ws_disconnected_at = Some(std::time::Instant::now());
                    info!(
                        "Worker {} WS disconnected — grace period started, failover deferred to health sweep",
                        wid
                    );
                }
            }
        }
    }

    forward_task.abort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_message_identify_serde() {
        let msg = WorkerMessage::Identify {
            worker_id: "w1".into(),
            api_key: "secret".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: WorkerMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            WorkerMessage::Identify { worker_id, api_key } => {
                assert_eq!(worker_id, "w1");
                assert_eq!(api_key, "secret");
            }
            _ => panic!("Expected Identify"),
        }
    }

    #[test]
    fn test_worker_message_heartbeat_serde() {
        let msg = WorkerMessage::Heartbeat(HeartbeatRequest {
            events_processed: 42,
            pipelines_running: 3,
            pipeline_metrics: vec![],
        });
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: WorkerMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            WorkerMessage::Heartbeat(hb) => {
                assert_eq!(hb.events_processed, 42);
                assert_eq!(hb.pipelines_running, 3);
            }
            _ => panic!("Expected Heartbeat"),
        }
    }

    #[test]
    fn test_coordinator_message_identify_ack_serde() {
        let msg = CoordinatorMessage::IdentifyAck {
            heartbeat_interval_secs: 5,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: CoordinatorMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            CoordinatorMessage::IdentifyAck {
                heartbeat_interval_secs,
            } => {
                assert_eq!(heartbeat_interval_secs, 5);
            }
            _ => panic!("Expected IdentifyAck"),
        }
    }

    #[test]
    fn test_coordinator_message_heartbeat_ack_serde() {
        let msg = CoordinatorMessage::HeartbeatAck;
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: CoordinatorMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, CoordinatorMessage::HeartbeatAck));
    }

    #[test]
    fn test_coordinator_message_error_serde() {
        let msg = CoordinatorMessage::Error {
            message: "bad auth".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: CoordinatorMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            CoordinatorMessage::Error { message } => {
                assert_eq!(message, "bad auth");
            }
            _ => panic!("Expected Error"),
        }
    }

    #[test]
    fn test_ws_connection_manager_add_remove() {
        let mut mgr = WsConnectionManager::new();
        let (tx, _rx) = mpsc::unbounded_channel();
        let wid = WorkerId("w1".into());

        assert_eq!(mgr.connected_count(), 0);
        assert!(!mgr.is_connected(&wid));

        mgr.add(wid.clone(), tx);
        assert_eq!(mgr.connected_count(), 1);
        assert!(mgr.is_connected(&wid));

        mgr.remove(&wid);
        assert_eq!(mgr.connected_count(), 0);
        assert!(!mgr.is_connected(&wid));
    }

    #[test]
    fn test_ws_connection_manager_multiple_workers() {
        let mut mgr = WsConnectionManager::new();
        let mut _receivers = Vec::new();

        for i in 0..3 {
            let (tx, rx) = mpsc::unbounded_channel();
            mgr.add(WorkerId(format!("w{}", i)), tx);
            _receivers.push(rx); // Keep receivers alive
        }

        assert_eq!(mgr.connected_count(), 3);
        assert!(mgr.is_connected(&WorkerId("w0".into())));
        assert!(mgr.is_connected(&WorkerId("w1".into())));
        assert!(mgr.is_connected(&WorkerId("w2".into())));
        assert!(!mgr.is_connected(&WorkerId("w99".into())));
    }

    #[test]
    fn test_ws_connection_manager_send() {
        let mut mgr = WsConnectionManager::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let wid = WorkerId("w1".into());
        mgr.add(wid.clone(), tx);

        // Send should succeed
        assert!(mgr.send(&wid, CoordinatorMessage::HeartbeatAck).is_ok());

        // Verify message received
        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, CoordinatorMessage::HeartbeatAck));

        // Send to unknown worker should fail
        assert!(mgr
            .send(
                &WorkerId("unknown".into()),
                CoordinatorMessage::HeartbeatAck
            )
            .is_err());
    }

    #[test]
    fn test_ws_connection_manager_default() {
        let mgr = WsConnectionManager::default();
        assert_eq!(mgr.connected_count(), 0);
    }

    #[test]
    fn test_ws_connection_manager_closed_channel_not_counted() {
        let mut mgr = WsConnectionManager::new();
        let (tx, rx) = mpsc::unbounded_channel::<CoordinatorMessage>();
        let wid = WorkerId("w1".into());
        mgr.add(wid.clone(), tx);

        assert_eq!(mgr.connected_count(), 1);
        assert!(mgr.is_connected(&wid));

        // Drop the receiver — channel is now closed
        drop(rx);

        assert_eq!(mgr.connected_count(), 0);
        assert!(!mgr.is_connected(&wid));
    }
}
