//! Deterministic simulation tests for the Raft consensus protocol layer.
//!
//! Unlike `raft_simulation_tests.rs` (which tests state machine convergence by
//! applying commands directly), these tests exercise the actual Raft protocol:
//! leader election, log replication, network faults, and membership changes.
//!
//! The tests use an in-process `SimRouter` that routes Raft RPCs through
//! `mpsc` channels with configurable fault injection — no external processes
//! or real networking required.
//!
//! Requires the `raft` feature: `cargo test -p varpulis-cluster --features raft`
#![cfg(feature = "raft")]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, oneshot, RwLock};
use varpulis_cluster::raft::store::SharedCoordinatorState;
use varpulis_cluster::raft::{
    bootstrap_with_network, ClusterCommand, NodeId, RaftNode, TypeConfig, VarpulisRaft,
};
use varpulis_cluster::worker::WorkerCapacity;

// =========================================================================
// In-process simulated Raft network
// =========================================================================

/// Envelope for a single Raft RPC delivered through the in-process network.
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

/// Central message router with fault injection for deterministic simulation.
struct SimRouter {
    nodes: HashMap<NodeId, mpsc::Sender<RpcRequest>>,
    partitions: HashSet<(NodeId, NodeId)>,
    isolated: HashSet<NodeId>,
    rng: StdRng,
    drop_rates: HashMap<(NodeId, NodeId), f64>,
}

impl SimRouter {
    fn new(rng: StdRng) -> Self {
        Self {
            nodes: HashMap::new(),
            partitions: HashSet::new(),
            isolated: HashSet::new(),
            rng,
            drop_rates: HashMap::new(),
        }
    }

    fn add_node(&mut self, id: NodeId, tx: mpsc::Sender<RpcRequest>) {
        self.nodes.insert(id, tx);
    }

    fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert((a, b));
        self.partitions.insert((b, a));
    }

    fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitions.remove(&(a, b));
        self.partitions.remove(&(b, a));
    }

    fn isolate(&mut self, id: NodeId) {
        self.isolated.insert(id);
    }

    fn restore(&mut self, id: NodeId) {
        self.isolated.remove(&id);
    }

    fn set_drop_rate(&mut self, src: NodeId, dst: NodeId, rate: f64) {
        if rate <= 0.0 {
            self.drop_rates.remove(&(src, dst));
        } else {
            self.drop_rates.insert((src, dst), rate);
        }
    }

    fn clear_drop_rates(&mut self) {
        self.drop_rates.clear();
    }

    fn is_reachable(&self, src: NodeId, dst: NodeId) -> bool {
        if self.isolated.contains(&src) || self.isolated.contains(&dst) {
            return false;
        }
        !self.partitions.contains(&(src, dst))
    }

    fn should_drop(&mut self, src: NodeId, dst: NodeId) -> bool {
        if let Some(&rate) = self.drop_rates.get(&(src, dst)) {
            self.rng.gen_bool(rate.clamp(0.0, 1.0))
        } else {
            false
        }
    }

    fn try_send(&mut self, src: NodeId, dst: NodeId) -> Option<mpsc::Sender<RpcRequest>> {
        if !self.is_reachable(src, dst) || self.should_drop(src, dst) {
            return None;
        }
        self.nodes.get(&dst).cloned()
    }
}

// ---------------------------------------------------------------------------
// SimNetworkFactory + SimNetworkClient
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SimNetworkFactory {
    router: Arc<RwLock<SimRouter>>,
    source: NodeId,
}

impl SimNetworkFactory {
    fn new(router: Arc<RwLock<SimRouter>>, source: NodeId) -> Self {
        Self { router, source }
    }
}

impl RaftNetworkFactory<TypeConfig> for SimNetworkFactory {
    type Network = SimNetworkClient;

    async fn new_client(&mut self, target: NodeId, _node: &RaftNode) -> Self::Network {
        SimNetworkClient {
            router: self.router.clone(),
            source: self.source,
            target,
        }
    }
}

struct SimNetworkClient {
    router: Arc<RwLock<SimRouter>>,
    source: NodeId,
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
        let sender = {
            let mut router = self.router.write().await;
            router.try_send(self.source, self.target)
        }
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
        let sender = {
            let mut router = self.router.write().await;
            router.try_send(self.source, self.target)
        }
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
        let sender = {
            let mut router = self.router.write().await;
            router.try_send(self.source, self.target)
        }
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

// ---------------------------------------------------------------------------
// Per-node RPC handler
// ---------------------------------------------------------------------------

async fn run_node_rpc_handler(raft: Arc<VarpulisRaft>, mut rx: mpsc::Receiver<RpcRequest>) {
    while let Some(rpc) = rx.recv().await {
        match rpc {
            RpcRequest::Vote { req, tx } => {
                let resp = raft.vote(req).await;
                let _ = tx.send(resp.unwrap_or_else(|e| {
                    panic!("sim: raft.vote() error: {e}");
                }));
            }
            RpcRequest::AppendEntries { req, tx } => {
                let resp = raft.append_entries(req).await;
                let _ = tx.send(resp.unwrap_or_else(|e| {
                    panic!("sim: raft.append_entries() error: {e}");
                }));
            }
            RpcRequest::InstallSnapshot { req, tx } => {
                let resp = raft.install_snapshot(req).await;
                let _ = tx.send(resp.unwrap_or_else(|e| {
                    panic!("sim: raft.install_snapshot() error: {e}");
                }));
            }
        }
    }
}

// =========================================================================
// SimCluster harness
// =========================================================================

struct SimCluster {
    rafts: BTreeMap<NodeId, Arc<VarpulisRaft>>,
    shared_states: BTreeMap<NodeId, SharedCoordinatorState>,
    router: Arc<RwLock<SimRouter>>,
}

impl SimCluster {
    async fn new(n: u64, seed: u64) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        let router = Arc::new(RwLock::new(SimRouter::new(rng)));

        let config = Arc::new(openraft::Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        });

        let mut rafts = BTreeMap::new();
        let mut shared_states = BTreeMap::new();

        for id in 1..=n {
            let (inbox_tx, inbox_rx) = mpsc::channel(1024);
            router.write().await.add_node(id, inbox_tx);

            let net = SimNetworkFactory::new(router.clone(), id);
            let result = bootstrap_with_network(id, config.clone(), net)
                .await
                .expect("bootstrap failed");

            tokio::spawn(run_node_rpc_handler(result.raft.clone(), inbox_rx));

            rafts.insert(id, result.raft);
            shared_states.insert(id, result.shared_state);
        }

        let cluster = Self {
            rafts,
            shared_states,
            router,
        };

        // Initialize cluster membership on node 1.
        let members = cluster.member_map();
        cluster.rafts[&1]
            .initialize(members)
            .await
            .expect("initialize failed");

        cluster
    }

    fn member_map(&self) -> BTreeMap<NodeId, RaftNode> {
        self.rafts
            .keys()
            .map(|&id| {
                (
                    id,
                    RaftNode {
                        addr: format!("sim://{id}"),
                    },
                )
            })
            .collect()
    }

    async fn wait_for_leader(&self, timeout: Duration) -> NodeId {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(leader) = self.get_leader() {
                return leader;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "No leader elected within {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn get_leader(&self) -> Option<NodeId> {
        for raft in self.rafts.values() {
            let metrics = raft.metrics().borrow().clone();
            if let Some(leader) = metrics.current_leader {
                if let Some(leader_raft) = self.rafts.get(&leader) {
                    let leader_metrics = leader_raft.metrics().borrow().clone();
                    if leader_metrics.current_leader == Some(leader) {
                        return Some(leader);
                    }
                }
            }
        }
        None
    }

    async fn write_on_leader(&self, cmd: ClusterCommand) -> NodeId {
        let leader = self.wait_for_leader(Duration::from_secs(5)).await;
        self.rafts[&leader]
            .client_write(cmd)
            .await
            .expect("client_write failed");
        leader
    }

    fn assert_state_converged(&self, node_ids: &[NodeId]) {
        assert!(
            node_ids.len() >= 2,
            "need at least 2 nodes to check convergence"
        );
        let first = &self.shared_states[&node_ids[0]];
        let first_val = {
            let s = first.read().unwrap();
            serde_json::to_value(&*s).unwrap()
        };
        for &id in &node_ids[1..] {
            let other = &self.shared_states[&id];
            let other_val = {
                let s = other.read().unwrap();
                serde_json::to_value(&*s).unwrap()
            };
            assert_eq!(
                first_val, other_val,
                "Node {} and {} have divergent state",
                node_ids[0], id
            );
        }
    }

    async fn wait_for_replication(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(leader_id) = self.get_leader() {
                let leader_metrics = self.rafts[&leader_id].metrics().borrow().clone();
                if let Some(last_log) = leader_metrics.last_log_index {
                    let all_caught_up = self.rafts.values().all(|raft| {
                        let m = raft.metrics().borrow().clone();
                        m.last_applied.map(|lid| lid.index) >= Some(last_log)
                    });
                    if all_caught_up {
                        return;
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                for (&id, raft) in &self.rafts {
                    let m = raft.metrics().borrow().clone();
                    eprintln!(
                        "  node {id}: leader={:?} last_applied={:?} last_log={:?}",
                        m.current_leader, m.last_applied, m.last_log_index
                    );
                }
                panic!("Replication did not converge within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_for_replication_on(&self, node_ids: &[NodeId], timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(leader_id) = self.get_leader() {
                let leader_metrics = self.rafts[&leader_id].metrics().borrow().clone();
                if let Some(last_log) = leader_metrics.last_log_index {
                    let all_caught_up = node_ids.iter().all(|id| {
                        self.rafts
                            .get(id)
                            .map(|raft| {
                                let m = raft.metrics().borrow().clone();
                                m.last_applied.map(|lid| lid.index) >= Some(last_log)
                            })
                            .unwrap_or(false)
                    });
                    if all_caught_up {
                        return;
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                for &id in node_ids {
                    if let Some(raft) = self.rafts.get(&id) {
                        let m = raft.metrics().borrow().clone();
                        eprintln!(
                            "  node {id}: leader={:?} last_applied={:?} last_log={:?}",
                            m.current_leader, m.last_applied, m.last_log_index
                        );
                    }
                }
                panic!("Replication on subset did not converge within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

// =========================================================================
// Helpers
// =========================================================================

fn get_seed() -> u64 {
    std::env::var("VARPULIS_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(42)
}

fn make_register(id: &str, cores: usize) -> ClusterCommand {
    ClusterCommand::RegisterWorker {
        id: id.into(),
        address: format!("http://{id}:9000"),
        api_key: format!("key-{id}"),
        capacity: WorkerCapacity {
            cpu_cores: cores,
            pipelines_running: 0,
            max_pipelines: 100,
        },
    }
}

fn make_deploy(name: &str) -> ClusterCommand {
    ClusterCommand::GroupDeployed {
        name: name.into(),
        group: serde_json::json!({ "name": name, "replicas": 1 }),
    }
}

// =========================================================================
// Test 1: Leader election in a 3-node cluster
// =========================================================================

#[tokio::test]
async fn leader_election_3_nodes() {
    let cluster = SimCluster::new(3, get_seed()).await;
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;

    // All nodes should agree on the same leader.
    let leader_count = cluster
        .rafts
        .values()
        .filter(|r| {
            let m = r.metrics().borrow().clone();
            m.current_leader == Some(leader)
        })
        .count();
    assert_eq!(leader_count, 3, "All nodes should agree on the leader");
}

// =========================================================================
// Test 2: Leader election under partition
// =========================================================================

#[tokio::test]
async fn leader_election_under_partition() {
    let cluster = SimCluster::new(3, get_seed()).await;
    let old_leader = cluster.wait_for_leader(Duration::from_secs(5)).await;

    // Isolate the leader.
    cluster.router.write().await.isolate(old_leader);

    // Wait for the remaining two to elect a new leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let new_leader = loop {
        let found = cluster.rafts.iter().find_map(|(&id, raft)| {
            if id == old_leader {
                return None;
            }
            let m = raft.metrics().borrow().clone();
            m.current_leader.filter(|&l| l != old_leader)
        });
        if let Some(l) = found {
            break l;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "New leader not elected after isolating old leader {old_leader}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_ne!(new_leader, old_leader);

    // Restore and let the old leader rejoin.
    cluster.router.write().await.restore(old_leader);
    tokio::time::sleep(Duration::from_millis(500)).await;
}

// =========================================================================
// Test 3: Log replication — write commands, verify convergence
// =========================================================================

#[tokio::test]
async fn log_replication() {
    let cluster = SimCluster::new(3, get_seed()).await;
    cluster.wait_for_leader(Duration::from_secs(5)).await;

    for i in 0..10 {
        cluster
            .write_on_leader(make_register(&format!("w{i}"), 4))
            .await;
    }

    cluster.wait_for_replication(Duration::from_secs(10)).await;
    cluster.assert_state_converged(&[1, 2, 3]);

    let state = cluster.shared_states[&1].read().unwrap();
    assert_eq!(state.workers.len(), 10);
}

// =========================================================================
// Test 4: Log replication with 30% message loss
// =========================================================================

#[tokio::test]
async fn log_replication_with_message_loss() {
    let cluster = SimCluster::new(3, get_seed()).await;
    cluster.wait_for_leader(Duration::from_secs(5)).await;

    // Set 30% drop rate on all links.
    {
        let mut router = cluster.router.write().await;
        for &a in cluster.rafts.keys() {
            for &b in cluster.rafts.keys() {
                if a != b {
                    router.set_drop_rate(a, b, 0.3);
                }
            }
        }
    }

    // Write commands — some RPCs will be dropped but Raft retries.
    for i in 0..5 {
        let cmd = make_register(&format!("w{i}"), 4);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
            match cluster.rafts[&leader].client_write(cmd.clone()).await {
                Ok(_) => break,
                Err(_) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "Could not write command w{i} within timeout"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    // Clear drops and wait for convergence.
    cluster.router.write().await.clear_drop_rates();
    cluster.wait_for_replication(Duration::from_secs(15)).await;
    cluster.assert_state_converged(&[1, 2, 3]);

    let state = cluster.shared_states[&1].read().unwrap();
    assert_eq!(state.workers.len(), 5);
}

// =========================================================================
// Test 5: Node failure and recovery
// =========================================================================

#[tokio::test]
async fn node_failure_and_recovery() {
    let cluster = SimCluster::new(3, get_seed()).await;
    cluster.wait_for_leader(Duration::from_secs(5)).await;

    // Write initial data.
    cluster.write_on_leader(make_register("w1", 4)).await;
    cluster.write_on_leader(make_register("w2", 8)).await;
    cluster.wait_for_replication(Duration::from_secs(5)).await;

    // Pick a non-leader node to isolate.
    let leader = cluster.get_leader().unwrap();
    let follower = *cluster.rafts.keys().find(|&&id| id != leader).unwrap();

    cluster.router.write().await.isolate(follower);

    // Write more data while follower is down.
    cluster.write_on_leader(make_register("w3", 16)).await;
    cluster.write_on_leader(make_deploy("pipeline-1")).await;

    // The remaining 2 nodes should have the new data.
    let remaining: Vec<NodeId> = cluster
        .rafts
        .keys()
        .copied()
        .filter(|&id| id != follower)
        .collect();
    cluster
        .wait_for_replication_on(&remaining, Duration::from_secs(5))
        .await;

    // Restore follower — it should catch up.
    cluster.router.write().await.restore(follower);
    cluster.wait_for_replication(Duration::from_secs(10)).await;
    cluster.assert_state_converged(&[1, 2, 3]);

    let state = cluster.shared_states[&follower].read().unwrap();
    assert_eq!(state.workers.len(), 3);
    assert_eq!(state.pipeline_groups.len(), 1);
}

// =========================================================================
// Test 6: Split-brain prevention (5 nodes)
// =========================================================================

#[tokio::test]
async fn split_brain_prevention_5_nodes() {
    let cluster = SimCluster::new(5, get_seed()).await;
    cluster.wait_for_leader(Duration::from_secs(5)).await;

    // Write baseline data.
    cluster.write_on_leader(make_register("w1", 4)).await;
    cluster.wait_for_replication(Duration::from_secs(5)).await;

    // Partition: {1,2} vs {3,4,5}
    let minority = [1u64, 2];
    let majority = [3u64, 4, 5];
    {
        let mut router = cluster.router.write().await;
        for &a in &minority {
            for &b in &majority {
                router.partition(a, b);
            }
        }
    }

    // Wait for the majority partition to elect a leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let majority_leader = loop {
        let found = majority.iter().find_map(|&id| {
            let m = cluster.rafts[&id].metrics().borrow().clone();
            m.current_leader.filter(|l| majority.contains(l))
        });
        if let Some(l) = found {
            break l;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Majority partition did not elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Verify majority leader can process writes.
    cluster.rafts[&majority_leader]
        .client_write(make_register("w2", 8))
        .await
        .expect("write on majority leader failed");

    // Minority should NOT be able to commit writes.
    for &id in &minority {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            cluster.rafts[&id].client_write(make_register("w-minority", 1)),
        )
        .await;
        if let Ok(Ok(_)) = result {
            panic!("Minority node {id} should not be able to commit writes");
        }
    }

    // Heal partition and verify full convergence.
    {
        let mut router = cluster.router.write().await;
        for &a in &minority {
            for &b in &majority {
                router.heal(a, b);
            }
        }
    }
    cluster.wait_for_replication(Duration::from_secs(10)).await;
    cluster.assert_state_converged(&[1, 2, 3, 4, 5]);
}

// =========================================================================
// Test 7: Membership change — add learner nodes then promote
// =========================================================================

#[tokio::test]
async fn membership_change_add_nodes() {
    // Start with 3 nodes.
    let seed = get_seed();
    let rng = StdRng::seed_from_u64(seed);
    let router = Arc::new(RwLock::new(SimRouter::new(rng)));

    let config = Arc::new(openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        ..Default::default()
    });

    let mut rafts: BTreeMap<NodeId, Arc<VarpulisRaft>> = BTreeMap::new();
    let mut shared_states: BTreeMap<NodeId, SharedCoordinatorState> = BTreeMap::new();

    for id in 1..=3 {
        let (inbox_tx, inbox_rx) = mpsc::channel(1024);
        router.write().await.add_node(id, inbox_tx);

        let net = SimNetworkFactory::new(router.clone(), id);
        let result = bootstrap_with_network(id, config.clone(), net)
            .await
            .expect("bootstrap failed");

        tokio::spawn(run_node_rpc_handler(result.raft.clone(), inbox_rx));
        rafts.insert(id, result.raft);
        shared_states.insert(id, result.shared_state);
    }

    // Initialize with 3 members.
    let members: BTreeMap<NodeId, RaftNode> = (1..=3)
        .map(|id| {
            (
                id,
                RaftNode {
                    addr: format!("sim://{id}"),
                },
            )
        })
        .collect();
    rafts[&1]
        .initialize(members)
        .await
        .expect("initialize failed");

    // Wait for leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let leader = loop {
        let found = rafts.values().find_map(|r| {
            let m = r.metrics().borrow().clone();
            m.current_leader
        });
        if let Some(l) = found {
            break l;
        }
        assert!(tokio::time::Instant::now() < deadline, "No leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    rafts[&leader]
        .client_write(make_register("w1", 4))
        .await
        .expect("initial write failed");

    // Add 2 new nodes (4, 5).
    for id in 4..=5u64 {
        let (inbox_tx, inbox_rx) = mpsc::channel(1024);
        router.write().await.add_node(id, inbox_tx);

        let net = SimNetworkFactory::new(router.clone(), id);
        let result = bootstrap_with_network(id, config.clone(), net)
            .await
            .expect("bootstrap failed");

        tokio::spawn(run_node_rpc_handler(result.raft.clone(), inbox_rx));
        rafts.insert(id, result.raft);
        shared_states.insert(id, result.shared_state);
    }

    // Add as learners first.
    for id in 4..=5u64 {
        rafts[&leader]
            .add_learner(
                id,
                RaftNode {
                    addr: format!("sim://{id}"),
                },
                true,
            )
            .await
            .expect("add_learner failed");
    }

    // Wait for learners to sync.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Promote to voters.
    let new_members: BTreeSet<NodeId> = (1..=5).collect();
    rafts[&leader]
        .change_membership(new_members, false)
        .await
        .expect("change_membership failed");

    // Write more data.
    rafts[&leader]
        .client_write(make_register("w2", 8))
        .await
        .expect("write after membership change failed");

    // Wait for all 5 to converge.
    let conv_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let leader_m = rafts[&leader].metrics().borrow().clone();
        if let Some(last_log) = leader_m.last_log_index {
            let all_caught_up = rafts.values().all(|raft| {
                let m = raft.metrics().borrow().clone();
                m.last_applied.map(|lid| lid.index) >= Some(last_log)
            });
            if all_caught_up {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < conv_deadline,
            "5-node cluster did not converge after membership change"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // All 5 nodes should have the same state.
    let ref_val = {
        let s = shared_states[&1].read().unwrap();
        serde_json::to_value(&*s).unwrap()
    };
    for id in 2..=5 {
        let v = {
            let s = shared_states[&id].read().unwrap();
            serde_json::to_value(&*s).unwrap()
        };
        assert_eq!(
            ref_val, v,
            "Node 1 and {id} diverged after membership change"
        );
    }

    let state = shared_states[&5].read().unwrap();
    assert_eq!(state.workers.len(), 2);
}

// =========================================================================
// Test 8: Concurrent writes during partition heal
// =========================================================================

#[tokio::test]
async fn concurrent_writes_during_partition_heal() {
    let cluster = SimCluster::new(3, get_seed()).await;
    cluster.wait_for_leader(Duration::from_secs(5)).await;

    // Write baseline.
    cluster.write_on_leader(make_register("w1", 4)).await;
    cluster.wait_for_replication(Duration::from_secs(5)).await;

    // Isolate one follower.
    let leader = cluster.get_leader().unwrap();
    let follower = *cluster.rafts.keys().find(|&&id| id != leader).unwrap();
    cluster.router.write().await.isolate(follower);

    // Write on majority while one node is down.
    for i in 2..=5 {
        cluster
            .write_on_leader(make_register(&format!("w{i}"), 4))
            .await;
    }

    // Heal the partition.
    cluster.router.write().await.restore(follower);

    // Immediately write more data (concurrent with catch-up replication).
    for i in 6..=8 {
        cluster
            .write_on_leader(make_register(&format!("w{i}"), 4))
            .await;
    }

    // Wait for full convergence.
    cluster.wait_for_replication(Duration::from_secs(10)).await;
    cluster.assert_state_converged(&[1, 2, 3]);

    let state = cluster.shared_states[&1].read().unwrap();
    assert_eq!(state.workers.len(), 8);
}
