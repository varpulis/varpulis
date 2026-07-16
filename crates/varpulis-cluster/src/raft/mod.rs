//! Embedded Raft consensus for coordinator state sharing.
//!
//! Uses [openraft](https://docs.rs/openraft) to replicate coordinator state
//! across multiple coordinator instances, enabling cross-coordinator pipeline
//! migration and automatic leader election without external dependencies.

pub mod network;
#[cfg(feature = "persistent")]
pub mod persistent_store;
pub mod routes;
pub mod state_machine;
pub mod store;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::network::RaftNetworkFactory;
use serde::{Deserialize, Serialize};

use crate::connector_config::ClusterConnector;
use crate::worker::WorkerCapacity;

/// Raft node ID type.
pub type NodeId = u64;

// ---------------------------------------------------------------------------
// Type configuration
// ---------------------------------------------------------------------------

openraft::declare_raft_types!(
    /// Varpulis cluster Raft type configuration.
    pub TypeConfig:
        D            = ClusterCommand,
        R            = ClusterResponse,
        NodeId       = NodeId,
        Node         = RaftNode,
);

/// Convenience alias for the Raft instance.
pub type VarpulisRaft = openraft::Raft<TypeConfig>;

// ---------------------------------------------------------------------------
// Node descriptor
// ---------------------------------------------------------------------------

/// Describes a Raft peer coordinator node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftNode {
    /// RPC address, e.g. `"http://coordinator-1:9100"`.
    pub addr: String,
}

impl std::fmt::Display for RaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

// Node is auto-implemented by openraft 0.9 for types that satisfy the bounds.

// ---------------------------------------------------------------------------
// Log entry data (replicated commands)
// ---------------------------------------------------------------------------

/// Commands that mutate the shared coordinator state via Raft log replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    // -- Worker lifecycle --
    RegisterWorker {
        id: String,
        address: String,
        api_key: String,
        capacity: WorkerCapacity,
    },
    DeregisterWorker {
        id: String,
    },
    WorkerStatusChanged {
        id: String,
        status: String,
    },
    WorkerPipelinesUpdated {
        id: String,
        assigned_pipelines: Vec<String>,
    },
    WorkerMetricsUpdated {
        id: String,
        events_processed: u64,
        pipelines_running: usize,
        #[serde(default)]
        pipeline_metrics: Vec<crate::worker::PipelineMetrics>,
        /// Monotonic per-worker heartbeat counter (liveness signal). Advances
        /// once per received heartbeat; `sync_from_raft` refreshes a worker's
        /// `last_heartbeat` only when this value moves forward. `#[serde(default)]`
        /// keeps older replicated/persisted commands deserializable.
        #[serde(default)]
        heartbeat_seq: u64,
    },

    // -- Pipeline groups --
    GroupDeployed {
        name: String,
        group: serde_json::Value,
    },
    GroupUpdated {
        name: String,
        group: serde_json::Value,
    },
    GroupRemoved {
        name: String,
    },

    // -- Migrations --
    MigrationStarted {
        task: serde_json::Value,
    },
    MigrationUpdated {
        id: String,
        status: String,
    },
    MigrationRemoved {
        id: String,
    },

    // -- Connectors --
    ConnectorCreated {
        name: String,
        connector: ClusterConnector,
    },
    ConnectorUpdated {
        name: String,
        connector: ClusterConnector,
    },
    ConnectorRemoved {
        name: String,
    },

    // -- Scaling --
    ScalingPolicySet {
        policy: Option<serde_json::Value>,
    },

    // -- Model Registry --
    ModelRegistered {
        name: String,
        entry: crate::model_registry::ModelRegistryEntry,
    },
    ModelRemoved {
        name: String,
    },

    // -- Distributed checkpoints (Flink-parity, Phase 1, Task 1.4) --
    /// A distributed checkpoint for `group_id` was successfully persisted to
    /// the shared state store. Replicating this through Raft makes the new
    /// checkpoint id visible to every coordinator, which they use on recovery
    /// to decide which assembled snapshot to restore from.
    #[cfg(feature = "distributed-checkpoint")]
    CheckpointCompleted {
        /// Pipeline group whose checkpoint completed.
        group_id: String,
        /// Id of the durable checkpoint.
        checkpoint_id: u64,
    },
    /// A distributed checkpoint for `group_id` was aborted (timeout, NACK,
    /// persistence failure, …). Replicated for observability and so that
    /// other coordinators do not retry the same id.
    #[cfg(feature = "distributed-checkpoint")]
    CheckpointAborted {
        /// Pipeline group whose checkpoint aborted.
        group_id: String,
        /// Id of the aborted checkpoint.
        checkpoint_id: u64,
        /// Recorded reason — same string the coordinator broadcast in the
        /// `CheckpointAbortNotification`.
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// Apply response
// ---------------------------------------------------------------------------

/// Response returned after a command is applied to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterResponse {
    Ok,
    Error { message: String },
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/// Result of bootstrapping a Raft node.
pub struct RaftBootstrapResult {
    /// The Raft instance handle.
    pub raft: Arc<VarpulisRaft>,
    /// Shared coordinator state updated after each Raft apply.
    pub shared_state: store::SharedCoordinatorState,
}

impl std::fmt::Debug for RaftBootstrapResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftBootstrapResult")
            .finish_non_exhaustive()
    }
}

/// Bootstrap a Raft consensus node with in-memory storage.
///
/// Creates the in-memory log store, state machine, and network transport,
/// then starts the Raft instance.
///
/// # Arguments
/// * `node_id` — unique numeric ID for this coordinator (1-based)
/// * `peer_addrs` — addresses of **all** coordinators (including self),
///   ordered so that index `i` corresponds to node ID `i + 1`
pub async fn bootstrap(
    node_id: NodeId,
    peer_addrs: &[String],
    admin_key: Option<String>,
) -> Result<RaftBootstrapResult, Box<dyn std::error::Error + Send + Sync>> {
    let (mem_store, shared_state) = store::MemStore::with_shared_state();
    let (log_store, state_machine) = openraft::storage::Adaptor::new(mem_store);
    bootstrap_with_storage(
        node_id,
        peer_addrs,
        admin_key,
        log_store,
        state_machine,
        shared_state,
    )
    .await
}

/// Bootstrap a Raft consensus node with persistent RocksDB storage.
///
/// On restart, the Raft node automatically recovers from persisted state.
#[cfg(feature = "persistent")]
pub async fn bootstrap_persistent(
    node_id: NodeId,
    peer_addrs: &[String],
    admin_key: Option<String>,
    data_dir: &str,
) -> Result<RaftBootstrapResult, Box<dyn std::error::Error + Send + Sync>> {
    let node_path = format!("{}/node-{}", data_dir, node_id);
    let (rocks_store, shared_state) =
        persistent_store::RocksStore::open_with_shared_state(&node_path)?;
    let (log_store, state_machine) = openraft::storage::Adaptor::new(rocks_store);
    bootstrap_with_storage(
        node_id,
        peer_addrs,
        admin_key,
        log_store,
        state_machine,
        shared_state,
    )
    .await
}

/// Shared bootstrap logic for both in-memory and persistent storage backends.
async fn bootstrap_with_storage<S>(
    node_id: NodeId,
    peer_addrs: &[String],
    admin_key: Option<String>,
    log_store: openraft::storage::Adaptor<TypeConfig, S>,
    state_machine: openraft::storage::Adaptor<TypeConfig, S>,
    shared_state: store::SharedCoordinatorState,
) -> Result<RaftBootstrapResult, Box<dyn std::error::Error + Send + Sync>>
where
    S: openraft::RaftStorage<TypeConfig>,
{
    let config = openraft::Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    let config = Arc::new(config);

    let network = network::NetworkFactory::new(admin_key);

    let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine)
        .await
        .map_err(|e| format!("Failed to create Raft instance: {e}"))?;

    // Only node 1 bootstraps the cluster membership. Other nodes discover
    // the cluster through Raft replication after node 1 becomes the initial
    // leader. Calling initialize() on ALL nodes creates conflicting log
    // entries (each node authors its own membership entry at index 0) which
    // causes replication panics when the leader tries to replicate.
    if node_id == 1 {
        let mut members: BTreeMap<NodeId, RaftNode> = BTreeMap::new();
        for (i, addr) in peer_addrs.iter().enumerate() {
            let nid = (i + 1) as u64;
            members.insert(nid, RaftNode { addr: addr.clone() });
        }
        if let Err(e) = raft.initialize(members).await {
            tracing::info!("Raft initialize (node {node_id}): {e} (may be already initialized)");
        }
    } else {
        tracing::info!("Raft node {node_id}: waiting for cluster bootstrap from node 1");
    }

    Ok(RaftBootstrapResult {
        raft: Arc::new(raft),
        shared_state,
    })
}

/// Bootstrap a Raft node with a caller-supplied network factory.
///
/// Unlike [`bootstrap`], this does **not** call `raft.initialize()` — the
/// caller (test harness) controls cluster initialization.  This enables
/// deterministic simulation testing with [`sim_network::SimNetworkFactory`].
pub async fn bootstrap_with_network<NF>(
    node_id: NodeId,
    config: Arc<openraft::Config>,
    network: NF,
) -> Result<RaftBootstrapResult, Box<dyn std::error::Error + Send + Sync>>
where
    NF: RaftNetworkFactory<TypeConfig>,
{
    let (mem_store, shared_state) = store::MemStore::with_shared_state();
    let (log_store, state_machine) = openraft::storage::Adaptor::new(mem_store);

    let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine)
        .await
        .map_err(|e| format!("Failed to create Raft instance: {e}"))?;

    Ok(RaftBootstrapResult {
        raft: Arc::new(raft),
        shared_state,
    })
}
