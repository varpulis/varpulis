//! Unified in-memory Raft storage (v1 `RaftStorage` trait).
//!
//! Use `openraft::storage::Adaptor::new(MemStore::new())` to get `(log_store, state_machine)`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogState;
use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use tracing::info;

use super::state_machine::{apply_command, CoordinatorState};
use super::{ClusterResponse, NodeId, RaftNode, TypeConfig};

/// Shared read-only reference to the replicated coordinator state.
///
/// Updated after each Raft apply. External code (Coordinator, API handlers)
/// can read from this without locking the Raft storage.
pub type SharedCoordinatorState = Arc<std::sync::RwLock<CoordinatorState>>;

/// Shared log data accessible by both MemStore and LogReader.
///
/// Uses `std::sync::RwLock` (not tokio) because lock scopes are short
/// and never held across await points.
type SharedLog = Arc<std::sync::RwLock<BTreeMap<u64, Entry<TypeConfig>>>>;

// ---------------------------------------------------------------------------
// Snapshot data
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StateMachineSnapshot {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, RaftNode>,
    state: CoordinatorState,
}

// ---------------------------------------------------------------------------
// Unified store
// ---------------------------------------------------------------------------

/// In-memory unified Raft storage (log + state machine).
///
/// The log is stored in a shared `Arc<RwLock<BTreeMap>>` so that `LogReader`
/// instances (used by openraft's replication tasks) always see the latest
/// entries, not a stale snapshot.
#[derive(Debug)]
pub struct MemStore {
    // Log (shared with LogReader instances)
    vote: Option<Vote<NodeId>>,
    log: SharedLog,
    last_purged: Option<LogId<NodeId>>,

    // State machine
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, RaftNode>,
    pub state: CoordinatorState,

    /// Shared snapshot of the coordinator state for external reads.
    /// Updated after each batch of Raft applies.
    shared_state: Option<SharedCoordinatorState>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            vote: None,
            log: Arc::new(std::sync::RwLock::new(BTreeMap::new())),
            last_purged: None,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            state: CoordinatorState::default(),
            shared_state: None,
        }
    }

    /// Create a store with a shared state reference for external reads.
    ///
    /// The returned `SharedCoordinatorState` is updated after each Raft apply,
    /// allowing the Coordinator and API handlers to read replicated state.
    pub fn with_shared_state() -> (Self, SharedCoordinatorState) {
        let shared = Arc::new(std::sync::RwLock::new(CoordinatorState::default()));
        let store = Self {
            vote: None,
            log: Arc::new(std::sync::RwLock::new(BTreeMap::new())),
            last_purged: None,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            state: CoordinatorState::default(),
            shared_state: Some(shared.clone()),
        };
        (store, shared)
    }

    /// Publish current state to the shared snapshot.
    fn publish_state(&self) {
        if let Some(ref shared) = self.shared_state {
            if let Ok(mut state) = shared.write() {
                *state = self.state.clone();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Log reader (shares live log data with MemStore)
// ---------------------------------------------------------------------------

/// Log reader that shares the same underlying log data as `MemStore`.
///
/// Unlike a snapshot-based reader, this always sees the latest entries,
/// which is required for openraft's replication tasks to function correctly.
#[derive(Debug)]
pub struct LogReader {
    log: SharedLog,
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().unwrap_or_else(|e| e.into_inner());
        let entries: Vec<_> = log.range(range).map(|(_, entry)| entry.clone()).collect();
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader for MemStore (required supertrait of RaftStorage)
// ---------------------------------------------------------------------------

impl RaftLogReader<TypeConfig> for MemStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().unwrap_or_else(|e| e.into_inner());
        let entries: Vec<_> = log.range(range).map(|(_, entry)| entry.clone()).collect();
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Snapshot builder
// ---------------------------------------------------------------------------

/// Snapshot builder that captures state at creation time.
pub struct SnapshotBuilder {
    snapshot: StateMachineSnapshot,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data = serde_json::to_vec(&self.snapshot).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::read_state_machine(&e),
        })?;

        let last_applied = self.snapshot.last_applied_log;
        let membership = self.snapshot.last_membership.clone();

        let snapshot_id = last_applied
            .map(|id| format!("{}-{}", id.leader_id, id.index))
            .unwrap_or_else(|| "0-0".to_string());

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ---------------------------------------------------------------------------
// RaftStorage implementation
// ---------------------------------------------------------------------------

impl RaftStorage<TypeConfig> for MemStore {
    type LogReader = LogReader;
    type SnapshotBuilder = SnapshotBuilder;

    // --- Vote ---

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.vote)
    }

    // --- Log ---

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        use openraft::RaftLogId;

        let log = self.log.read().unwrap_or_else(|e| e.into_inner());
        let last = log.values().last().map(|e| *e.get_log_id());
        Ok(LogState {
            last_purged_log_id: self.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogReader {
            log: self.log.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        use openraft::RaftLogId;
        let mut log = self.log.write().unwrap_or_else(|e| e.into_inner());
        for entry in entries {
            let idx = entry.get_log_id().index;
            log.insert(idx, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().unwrap_or_else(|e| e.into_inner());
        let to_remove: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in to_remove {
            log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().unwrap_or_else(|e| e.into_inner());
        let to_remove: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in to_remove {
            log.remove(&k);
        }
        drop(log);
        self.last_purged = Some(log_id);
        Ok(())
    }

    // --- State machine ---

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, RaftNode>), StorageError<NodeId>>
    {
        Ok((self.last_applied_log, self.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<ClusterResponse>, StorageError<NodeId>> {
        use openraft::RaftLogId;
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            self.last_applied_log = Some(*entry.get_log_id());

            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(ClusterResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let resp = apply_command(&mut self.state, cmd.clone());
                    responses.push(resp);
                }
                EntryPayload::Membership(mem) => {
                    self.last_membership =
                        StoredMembership::new(Some(*entry.get_log_id()), mem.clone());
                    responses.push(ClusterResponse::Ok);
                }
            }
        }

        // Publish updated state for external consumers (Coordinator, API).
        self.publish_state();

        Ok(responses)
    }

    // --- Snapshot ---

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            snapshot: StateMachineSnapshot {
                last_applied_log: self.last_applied_log,
                last_membership: self.last_membership.clone(),
                state: self.state.clone(),
            },
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, RaftNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let snap: StateMachineSnapshot =
            serde_json::from_slice(&data).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::read_state_machine(&e),
            })?;

        self.last_applied_log = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.state = snap.state;

        // Publish restored state for external consumers.
        self.publish_state();

        info!("Installed Raft snapshot at {:?}", meta.last_log_id);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let mut builder = self.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await?;
        Ok(Some(snapshot))
    }
}
