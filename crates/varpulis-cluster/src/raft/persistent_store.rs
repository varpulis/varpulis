//! RocksDB-backed persistent Raft storage.
//!
//! Implements the openraft `RaftStorage` trait for durable state across restarts.
//! Data is organized into column families:
//! - `default` — log entries (key: big-endian u64 index)
//! - `meta` — vote, last_purged, last_applied
//! - `snapshots` — snapshot data

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogState;
use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tracing::info;

use super::state_machine::{apply_command, CoordinatorState};
use super::{ClusterResponse, NodeId, RaftNode, TypeConfig};
use crate::raft::store::SharedCoordinatorState;

const CF_META: &str = "meta";
const CF_SNAPSHOTS: &str = "snapshots";
// Default CF stores log entries

const KEY_VOTE: &[u8] = b"vote";
const KEY_LAST_PURGED: &[u8] = b"last_purged";
const KEY_LAST_APPLIED: &[u8] = b"last_applied";
const KEY_LAST_MEMBERSHIP: &[u8] = b"last_membership";
const KEY_SNAPSHOT: &[u8] = b"snapshot_data";
const KEY_SNAPSHOT_META: &[u8] = b"snapshot_meta";

/// RocksDB-backed Raft storage.
pub struct RocksStore {
    db: Arc<DB>,
    // In-memory state machine (rebuilt from log on startup)
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, RaftNode>,
    state: CoordinatorState,
    shared_state: Option<SharedCoordinatorState>,
}

impl Debug for RocksStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksStore")
            .field("last_applied_log", &self.last_applied_log)
            .finish()
    }
}

impl RocksStore {
    /// Open or create a RocksDB-backed store at the given path.
    pub fn open(path: &str) -> Result<Self, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_meta = ColumnFamilyDescriptor::new(CF_META, Options::default());
        let cf_snapshots = ColumnFamilyDescriptor::new(CF_SNAPSHOTS, Options::default());

        let db = Arc::new(
            DB::open_cf_descriptors(&opts, path, vec![cf_meta, cf_snapshots])
                .map_err(|e| format!("Failed to open RocksDB at {}: {}", path, e))?,
        );

        let mut store = Self {
            db,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            state: CoordinatorState::default(),
            shared_state: None,
        };

        // Recover persisted metadata
        store.recover_metadata()?;

        info!("RocksDB Raft store opened at {}", path);
        Ok(store)
    }

    /// Open with a shared state reference for external reads.
    pub fn open_with_shared_state(path: &str) -> Result<(Self, SharedCoordinatorState), String> {
        let shared = Arc::new(std::sync::RwLock::new(CoordinatorState::default()));
        let mut store = Self::open(path)?;
        store.shared_state = Some(shared.clone());

        // Replay log to rebuild state machine
        store.replay_log();
        store.publish_state();

        Ok((store, shared))
    }

    /// Recover metadata (vote, last_purged, last_applied, membership) from RocksDB.
    fn recover_metadata(&mut self) -> Result<(), String> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or("meta column family missing")?;

        if let Ok(Some(data)) = self.db.get_cf(&cf, KEY_LAST_APPLIED) {
            if let Ok(log_id) = serde_json::from_slice::<Option<LogId<NodeId>>>(&data) {
                self.last_applied_log = log_id;
            }
        }

        if let Ok(Some(data)) = self.db.get_cf(&cf, KEY_LAST_MEMBERSHIP) {
            if let Ok(mem) = serde_json::from_slice::<StoredMembership<NodeId, RaftNode>>(&data) {
                self.last_membership = mem;
            }
        }

        Ok(())
    }

    /// Replay committed log entries to rebuild the state machine.
    fn replay_log(&mut self) {
        let Some(last_applied) = self.last_applied_log else {
            return;
        };

        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        let mut count = 0u64;

        for item in iter {
            let Ok((key, value)) = item else {
                break;
            };
            if key.len() != 8 {
                continue; // Skip non-log entries
            }
            let index = u64::from_be_bytes(key[..8].try_into().unwrap());

            if index > last_applied.index {
                break;
            }

            if let Ok(entry) = serde_json::from_slice::<Entry<TypeConfig>>(&value) {
                if let EntryPayload::Normal(cmd) = entry.payload {
                    apply_command(&mut self.state, cmd);
                    count += 1;
                }
            }
        }

        if count > 0 {
            info!(
                "Replayed {} log entries to rebuild state machine (up to index {})",
                count, last_applied.index
            );
        }
    }

    /// Publish current state to the shared snapshot.
    fn publish_state(&self) {
        if let Some(ref shared) = self.shared_state {
            if let Ok(mut state) = shared.write() {
                *state = self.state.clone();
            }
        }
    }

    /// Encode a log index as big-endian bytes for RocksDB key ordering.
    fn index_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Save metadata to the meta column family.
    #[allow(clippy::result_large_err)]
    fn save_meta(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_META).ok_or_else(|| StorageError::IO {
            source: openraft::StorageIOError::write(openraft::AnyError::error(
                "meta column family missing",
            )),
        })?;
        self.db
            .put_cf(&cf, key, value)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write(&e),
            })
    }

    /// Get the last log entry.
    fn last_log_entry(&self) -> Option<Entry<TypeConfig>> {
        let mut iter = self.db.iterator(rocksdb::IteratorMode::End);
        loop {
            match iter.next() {
                Some(Ok((key, value))) => {
                    if key.len() == 8 {
                        return serde_json::from_slice(&value).ok();
                    }
                    // Skip non-log entries
                }
                _ => return None,
            }
        }
    }

    /// Get the persisted vote.
    #[allow(clippy::result_large_err)]
    fn read_vote_from_db(&self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_META).ok_or_else(|| StorageError::IO {
            source: openraft::StorageIOError::read_vote(openraft::AnyError::error(
                "meta column family missing",
            )),
        })?;
        let data = self
            .db
            .get_cf(&cf, KEY_VOTE)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::read_vote(&e),
            })?;
        match data {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    /// Get the persisted last_purged log ID.
    #[allow(clippy::result_large_err)]
    fn read_last_purged(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_META).ok_or_else(|| StorageError::IO {
            source: openraft::StorageIOError::read_logs(openraft::AnyError::error(
                "meta column family missing",
            )),
        })?;
        let data = self
            .db
            .get_cf(&cf, KEY_LAST_PURGED)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::read_logs(&e),
            })?;
        match data {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Log reader (reads from the same RocksDB instance)
// ---------------------------------------------------------------------------

/// Log reader backed by RocksDB.
///
/// Uses a `DB` handle clone (RocksDB DB can be shared across threads).
pub struct RocksLogReader {
    db: Arc<DB>,
}

impl Debug for RocksLogReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksLogReader").finish()
    }
}

impl RaftLogReader<TypeConfig> for RocksLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => Some(n + 1),
            std::ops::Bound::Excluded(&n) => Some(n),
            std::ops::Bound::Unbounded => None,
        };

        let start_key = start.to_be_bytes();
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));

        let mut entries = Vec::new();
        for item in iter {
            let Ok((key, value)) = item else {
                break;
            };
            if key.len() != 8 {
                continue;
            }
            let index = u64::from_be_bytes(key[..8].try_into().unwrap());
            if let Some(end_idx) = end {
                if index >= end_idx {
                    break;
                }
            }
            if let Ok(entry) = serde_json::from_slice::<Entry<TypeConfig>>(&value) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Snapshot builder
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StateMachineSnapshot {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, RaftNode>,
    state: CoordinatorState,
}

pub struct RocksSnapshotBuilder {
    snapshot: StateMachineSnapshot,
}

impl RaftSnapshotBuilder<TypeConfig> for RocksSnapshotBuilder {
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

impl RaftLogReader<TypeConfig> for RocksStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => Some(n + 1),
            std::ops::Bound::Excluded(&n) => Some(n),
            std::ops::Bound::Unbounded => None,
        };

        let start_key = start.to_be_bytes();
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));

        let mut entries = Vec::new();
        for item in iter {
            let Ok((key, value)) = item else {
                break;
            };
            if key.len() != 8 {
                continue;
            }
            let index = u64::from_be_bytes(key[..8].try_into().unwrap());
            if let Some(end_idx) = end {
                if index >= end_idx {
                    break;
                }
            }
            if let Ok(entry) = serde_json::from_slice::<Entry<TypeConfig>>(&value) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}

impl RaftStorage<TypeConfig> for RocksStore {
    type LogReader = RocksLogReader;
    type SnapshotBuilder = RocksSnapshotBuilder;

    // --- Vote ---

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let data = serde_json::to_vec(vote).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write_vote(&e),
        })?;
        self.save_meta(KEY_VOTE, &data)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.read_vote_from_db()
    }

    // --- Log ---

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        use openraft::RaftLogId;

        let last_purged = self.read_last_purged()?;
        let last = self.last_log_entry().map(|e| *e.get_log_id());
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        RocksLogReader {
            db: self.db.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        use openraft::RaftLogId;

        let mut batch = rocksdb::WriteBatch::default();
        for entry in entries {
            let idx = entry.get_log_id().index;
            let key = Self::index_key(idx);
            let data = serde_json::to_vec(&entry).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write_logs(&e),
            })?;
            batch.put(key, data);
        }
        self.db.write(batch).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write_logs(&e),
        })?;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let start_key = Self::index_key(log_id.index);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));
        let mut batch = rocksdb::WriteBatch::default();
        for item in iter {
            let Ok((key, _)) = item else { break };
            if key.len() == 8 {
                batch.delete(&key);
            }
        }
        self.db.write(batch).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write_logs(&e),
        })?;
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let end_index = log_id.index + 1;
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        let mut batch = rocksdb::WriteBatch::default();
        for item in iter {
            let Ok((key, _)) = item else { break };
            if key.len() != 8 {
                continue;
            }
            let index = u64::from_be_bytes(key[..8].try_into().unwrap());
            if index >= end_index {
                break;
            }
            batch.delete(&key);
        }
        self.db.write(batch).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write_logs(&e),
        })?;

        let data = serde_json::to_vec(&log_id).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write_logs(&e),
        })?;
        self.save_meta(KEY_LAST_PURGED, &data)
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

        // Persist last_applied and membership
        let applied_data =
            serde_json::to_vec(&self.last_applied_log).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write(&e),
            })?;
        self.save_meta(KEY_LAST_APPLIED, &applied_data)?;
        let mem_data = serde_json::to_vec(&self.last_membership).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write(&e),
        })?;
        self.save_meta(KEY_LAST_MEMBERSHIP, &mem_data)?;

        // Publish updated state for external consumers.
        self.publish_state();

        Ok(responses)
    }

    // --- Snapshot ---

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        RocksSnapshotBuilder {
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

        // Persist metadata
        let applied_data =
            serde_json::to_vec(&self.last_applied_log).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write(&e),
            })?;
        self.save_meta(KEY_LAST_APPLIED, &applied_data)?;
        let mem_data = serde_json::to_vec(&self.last_membership).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write(&e),
        })?;
        self.save_meta(KEY_LAST_MEMBERSHIP, &mem_data)?;

        // Persist snapshot data for future recovery
        let cf_snap = self
            .db
            .cf_handle(CF_SNAPSHOTS)
            .ok_or_else(|| StorageError::IO {
                source: openraft::StorageIOError::write(openraft::AnyError::error(
                    "snapshots column family missing",
                )),
            })?;
        self.db
            .put_cf(&cf_snap, KEY_SNAPSHOT, &data)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write(&e),
            })?;
        let meta_data = serde_json::to_vec(meta).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write(&e),
        })?;
        self.db
            .put_cf(&cf_snap, KEY_SNAPSHOT_META, &meta_data)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write(&e),
            })?;

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
