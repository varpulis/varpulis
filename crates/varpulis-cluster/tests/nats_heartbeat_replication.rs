//! Regression test for audit critical **C5** on the NATS heartbeat path.
//!
//! The HTTP heartbeat handler replicates a worker's monotonic `heartbeat_seq`
//! through Raft (`ClusterCommand::WorkerMetricsUpdated`) so that *every*
//! coordinator — not just the one a worker is homed on — can tell a live worker
//! from a dead one. Before the fix, the NATS heartbeat handler advanced only the
//! receiving coordinator's *local* `heartbeat_seq` and never replicated it, so a
//! NATS-homed worker on a non-leader coordinator would be invisible to the
//! leader's `sync_from_raft` and could be false-marked `Unhealthy`.
//!
//! This test drives a heartbeat over the **real NATS broker** into the real
//! `run_coordinator_nats_handler`, wired to a **real single-node in-memory Raft**
//! (the same `SimRouter` harness pattern used by `raft_sim_tests.rs`) wrapped in
//! a real `Coordinator`, then asserts the **replicated** `WorkerEntry.heartbeat_seq`
//! in the Raft state machine advanced — i.e. the NATS heartbeat reached Raft.
//! No mocking: real broker + real Raft.
//!
//! Fail-before / pass-after gate: deleting the replication call in
//! `nats_coordinator::handle_heartbeat_message` makes the replicated
//! `heartbeat_seq` stay frozen at 0, so the "advanced past 0" assertion times
//! out and the test fails.
//!
//! Requires both features and a broker:
//! `cargo test -p varpulis-cluster --features "raft,nats-transport" \
//!     --test nats_heartbeat_replication`
//! A `nats-server` must be listening on `nats://localhost:4222`; if it is not,
//! the test prints `[skip]` and returns green.
#![cfg(all(feature = "raft", feature = "nats-transport"))]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;
use varpulis_cluster::nats_coordinator::run_coordinator_nats_handler;
use varpulis_cluster::nats_transport::{connect_nats, nats_publish, subject_heartbeat};
use varpulis_cluster::raft::store::SharedCoordinatorState;
use varpulis_cluster::raft::{
    bootstrap_with_network, ClusterCommand, NodeId, RaftNode, TypeConfig, VarpulisRaft,
};
use varpulis_cluster::worker::{HeartbeatRequest, WorkerCapacity};
use varpulis_cluster::{Coordinator, SharedCoordinator};

const NATS_URL: &str = "nats://localhost:4222";

// =========================================================================
// Minimal in-process simulated Raft network (single-node subset of the
// `SimRouter` harness in `raft_sim_tests.rs`). For a 1-node cluster no RPCs
// are ever sent to peers, so the router just needs to exist; the node elects
// itself and commits/applies writes locally through real openraft machinery.
// =========================================================================

enum RpcRequest {
    Vote {
        req: VoteRequest<NodeId>,
        tx: oneshot::Sender<VoteResponse<NodeId>>,
    },
    AppendEntries {
        req: AppendEntriesRequest<TypeConfig>,
        tx: oneshot::Sender<AppendEntriesResponse<NodeId>>,
    },
    InstallSnapshot {
        req: InstallSnapshotRequest<TypeConfig>,
        tx: oneshot::Sender<InstallSnapshotResponse<NodeId>>,
    },
}

struct SimRouter {
    nodes: HashMap<NodeId, mpsc::Sender<RpcRequest>>,
}

impl SimRouter {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    fn add_node(&mut self, id: NodeId, tx: mpsc::Sender<RpcRequest>) {
        self.nodes.insert(id, tx);
    }

    fn sender(&self, dst: NodeId) -> Option<mpsc::Sender<RpcRequest>> {
        self.nodes.get(&dst).cloned()
    }
}

#[derive(Clone)]
struct SimNetworkFactory {
    router: Arc<RwLock<SimRouter>>,
}

impl RaftNetworkFactory<TypeConfig> for SimNetworkFactory {
    type Network = SimNetworkClient;

    async fn new_client(&mut self, target: NodeId, _node: &RaftNode) -> Self::Network {
        SimNetworkClient {
            router: self.router.clone(),
            target,
        }
    }
}

struct SimNetworkClient {
    router: Arc<RwLock<SimRouter>>,
    target: NodeId,
}

fn unreachable_err<E: std::error::Error>() -> RPCError<NodeId, RaftNode, E> {
    RPCError::Unreachable(openraft::error::Unreachable::new(&std::io::Error::other(
        "sim: node unreachable",
    )))
}

impl RaftNetwork<TypeConfig> for SimNetworkClient {
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        let sender = self
            .router
            .read()
            .await
            .sender(self.target)
            .ok_or_else(unreachable_err)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .send(RpcRequest::Vote {
                req: rpc,
                tx: reply_tx,
            })
            .await
            .map_err(|_| unreachable_err())?;
        reply_rx.await.map_err(|_| unreachable_err())
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        let sender = self
            .router
            .read()
            .await
            .sender(self.target)
            .ok_or_else(unreachable_err)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .send(RpcRequest::AppendEntries {
                req: rpc,
                tx: reply_tx,
            })
            .await
            .map_err(|_| unreachable_err())?;
        reply_rx.await.map_err(|_| unreachable_err())
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, RaftNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let sender = self
            .router
            .read()
            .await
            .sender(self.target)
            .ok_or_else(unreachable_err)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .send(RpcRequest::InstallSnapshot {
                req: rpc,
                tx: reply_tx,
            })
            .await
            .map_err(|_| unreachable_err())?;
        reply_rx.await.map_err(|_| unreachable_err())
    }
}

async fn run_node_rpc_handler(raft: Arc<VarpulisRaft>, mut rx: mpsc::Receiver<RpcRequest>) {
    while let Some(rpc) = rx.recv().await {
        match rpc {
            RpcRequest::Vote { req, tx } => {
                let resp = raft.vote(req).await.expect("sim: raft.vote() error");
                let _ = tx.send(resp);
            }
            RpcRequest::AppendEntries { req, tx } => {
                let resp = raft
                    .append_entries(req)
                    .await
                    .expect("sim: raft.append_entries() error");
                let _ = tx.send(resp);
            }
            RpcRequest::InstallSnapshot { req, tx } => {
                let resp = raft
                    .install_snapshot(req)
                    .await
                    .expect("sim: raft.install_snapshot() error");
                let _ = tx.send(resp);
            }
        }
    }
}

/// Bootstrap a real single-node in-memory Raft and wait until it has elected
/// itself leader (so `client_write` commits and applies locally). Returns the
/// raft handle and its shared (replicated) coordinator state.
async fn single_node_raft() -> (Arc<VarpulisRaft>, SharedCoordinatorState) {
    let router = Arc::new(RwLock::new(SimRouter::new()));
    let config = Arc::new(openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        ..Default::default()
    });

    let (inbox_tx, inbox_rx) = mpsc::channel(1024);
    router.write().await.add_node(1, inbox_tx);

    let net = SimNetworkFactory {
        router: router.clone(),
    };
    let result = bootstrap_with_network(1, config, net)
        .await
        .expect("bootstrap failed");

    tokio::spawn(run_node_rpc_handler(result.raft.clone(), inbox_rx));

    let members: BTreeMap<NodeId, RaftNode> = BTreeMap::from([(
        1,
        RaftNode {
            addr: "sim://1".to_string(),
        },
    )]);
    result.raft.initialize(members).await.expect("initialize");

    // Wait until node 1 is the elected leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let leader = result.raft.metrics().borrow().current_leader;
        if leader == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "single-node Raft did not elect itself leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    (result.raft, result.shared_state)
}

/// The replicated (Raft state-machine) `heartbeat_seq` for a worker, if present.
fn replicated_seq(state: &SharedCoordinatorState, worker_id: &str) -> Option<u64> {
    state
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .workers
        .get(worker_id)
        .map(|w| w.heartbeat_seq)
}

/// The replicated `events_processed` for a worker, if present.
fn replicated_events(state: &SharedCoordinatorState, worker_id: &str) -> Option<u64> {
    state
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .workers
        .get(worker_id)
        .map(|w| w.events_processed)
}

/// Poll the replicated state until the worker's `heartbeat_seq` reaches `min`,
/// or the timeout elapses. Returns the observed seq on success, `None` on
/// timeout.
async fn wait_for_replicated_seq(
    state: &SharedCoordinatorState,
    worker_id: &str,
    min: u64,
    timeout: Duration,
) -> Option<u64> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(seq) = replicated_seq(state, worker_id) {
            if seq >= min {
                return Some(seq);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// =========================================================================
// The gate
// =========================================================================

#[tokio::test]
async fn nats_heartbeat_replicates_seq_through_raft() {
    // Broker-skip-graceful: no broker ⇒ skip green (matches `connect_nats`).
    let client = match connect_nats(NATS_URL).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[skip] NATS broker unavailable at {NATS_URL}: {e}");
            return;
        }
    };

    // 1. Real single-node in-memory Raft (elected leader).
    let (raft, shared_state) = single_node_raft().await;

    // 2. Register a worker THROUGH Raft so a replicated WorkerEntry (seq = 0)
    //    exists. Unique id so parallel runs don't collide on the shared broker.
    let worker_id = format!("nats-hb-{}", Uuid::new_v4().simple());
    raft.client_write(ClusterCommand::RegisterWorker {
        id: worker_id.clone(),
        address: "localhost:9100".to_string(),
        api_key: "hb-key".to_string(),
        capacity: WorkerCapacity {
            cpu_cores: 4,
            pipelines_running: 0,
            max_pipelines: 100,
        },
    })
    .await
    .expect("register worker via Raft");

    // Baseline: the replicated liveness counter starts at 0.
    assert_eq!(
        replicated_seq(&shared_state, &worker_id),
        Some(0),
        "freshly-registered worker should have replicated heartbeat_seq = 0"
    );

    // 3. Wrap the SAME Raft node + shared state in a real Coordinator and pull
    //    the just-registered worker into its local map so the NATS handler's
    //    `heartbeat()` finds it.
    let mut coord =
        Coordinator::with_raft(raft.clone(), shared_state.clone(), BTreeMap::new(), None);
    coord.sync_from_raft();
    let coordinator: SharedCoordinator = Arc::new(RwLock::new(coord));

    // 4. Run the REAL coordinator-side NATS handler against the live broker.
    let handler = tokio::spawn(run_coordinator_nats_handler(
        client.clone(),
        coordinator.clone(),
    ));
    // Let the subscriptions establish.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. First heartbeat over NATS on the subject the handler subscribes to.
    let hb1 = HeartbeatRequest {
        events_processed: 111,
        pipelines_running: 1,
        pipeline_metrics: vec![],
    };
    nats_publish(&client, &subject_heartbeat(&worker_id), &hb1)
        .await
        .expect("publish heartbeat 1");
    client.flush().await.expect("flush");

    // ASSERT: the replicated heartbeat_seq advanced past 0 — the NATS heartbeat
    // reached Raft. (Fail-before: without the replication call this times out.)
    let seq1 = wait_for_replicated_seq(&shared_state, &worker_id, 1, Duration::from_secs(5))
        .await
        .unwrap_or_else(|| {
            panic!(
                "C5(NATS): the first NATS heartbeat did NOT advance the replicated \
                 heartbeat_seq past 0 — it never reached Raft (observed {:?})",
                replicated_seq(&shared_state, &worker_id)
            )
        });
    assert!(
        seq1 >= 1,
        "replicated heartbeat_seq should advance to >= 1, got {seq1}"
    );
    // The metrics rode along on the same replicated command.
    assert_eq!(
        replicated_events(&shared_state, &worker_id),
        Some(111),
        "events_processed should have been replicated alongside heartbeat_seq"
    );

    // 6. Second heartbeat: the replicated seq must advance again (monotonic).
    let hb2 = HeartbeatRequest {
        events_processed: 222,
        pipelines_running: 2,
        pipeline_metrics: vec![],
    };
    nats_publish(&client, &subject_heartbeat(&worker_id), &hb2)
        .await
        .expect("publish heartbeat 2");
    client.flush().await.expect("flush");

    let seq2 = wait_for_replicated_seq(&shared_state, &worker_id, seq1 + 1, Duration::from_secs(5))
        .await
        .unwrap_or_else(|| {
            panic!(
                "C5(NATS): the second NATS heartbeat did NOT advance the replicated \
                 heartbeat_seq again (still {seq1})"
            )
        });
    assert!(
        seq2 > seq1,
        "the second NATS heartbeat must advance the replicated seq: {seq1} -> {seq2}"
    );
    assert_eq!(
        replicated_events(&shared_state, &worker_id),
        Some(222),
        "second heartbeat's events_processed should have been replicated"
    );

    handler.abort();
}
