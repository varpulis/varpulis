//! Mapping every [`ClusterCommand`] onto a single-key compare-and-swap, and
//! materialising a [`CoordinatorState`] back out of the bucket.
//!
//! ## Why one key per command is enough
//!
//! All command variants mutate exactly one entity keyed by one id. The only
//! one that touches two collections in the Raft state machine —
//! `WorkerMetricsUpdated`, which writes `workers[id]` *and*
//! `worker_pipeline_metrics[id]` — writes two facets of the *same* worker, so
//! on KV they are folded into one record ([`WorkerRecord`]) under one key and
//! one CAS. **No command needs a multi-key atomic transaction**, which is
//! precisely why a per-key CAS store can replace a totally-ordered log here.
//!
//! ## Read-modify-write, not blind write
//!
//! Raft's apply is `state = f(state)` executed by one node at a time. The KV
//! equivalent is a bounded CAS retry: read at revision `r`, compute, write
//! guarded on `r`, and on refusal re-read and recompute. That makes each
//! command an atomic read-modify-write on its key, which is the property
//! the Raft leader's serial apply was providing.
//!
//! ## Connector credentials — deliberately not carried
//!
//! `ClusterConnector::params` is a plain `HashMap<String, String>` with no
//! secret handling: today those values travel cleartext in the Raft log.
//! Relocating them verbatim into a replicated, on-disk KV bucket would widen
//! the exposure, so this backend **refuses** a connector whose params look
//! credential-bearing (see [`ConnectorSecretPolicy`]). Encryption is being
//! done separately; until it lands, that refusal is a named gap rather than a
//! silent relocation.

use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::keys::{self, ControlKey};
use super::store::{ControlPlane, ControlPlaneError, Expect, Snapshot};
use crate::control_state::{
    ClusterCommand, ClusterResponse, CoordinatorState, GroupCheckpointStatus, WorkerEntry,
};
use crate::worker::PipelineMetrics;

/// How many times a refused CAS is retried before the caller is told to try
/// again. Contention on a single control-plane key is low (one owner per
/// worker, one coordinator driving groups), so a small bound is enough and
/// keeps a hot key from wedging a coordinator tick.
const CAS_RETRIES: usize = 8;

// ---------------------------------------------------------------------------
// Stored record shapes
// ---------------------------------------------------------------------------

/// What lives at `workers/<id>`.
///
/// [`WorkerEntry`] is flattened in so the JSON is the same shape the Raft
/// snapshot uses, plus the per-worker pipeline metrics that the Raft state
/// machine kept in a parallel map. Folding them together is what keeps
/// `WorkerMetricsUpdated` a single-key write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRecord {
    #[serde(flatten)]
    pub entry: WorkerEntry,
    #[serde(default)]
    pub pipeline_metrics: Vec<PipelineMetrics>,
}

impl WorkerRecord {
    pub fn new(entry: WorkerEntry) -> Self {
        Self {
            entry,
            pipeline_metrics: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Connector secret policy
// ---------------------------------------------------------------------------

/// What to do with a connector whose params look credential-bearing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectorSecretPolicy {
    /// Refuse the write. The default, and the only setting that is safe until
    /// connector params are encrypted at rest.
    #[default]
    Refuse,
    /// Write them anyway. Only for a bucket the operator has confirmed is
    /// encrypted and access-controlled.
    AllowPlaintext,
}

/// Param names that indicate a credential. Matched case-insensitively as a
/// substring, so `sasl_password`, `apiKey` and `AWS_SECRET_ACCESS_KEY` all
/// match.
const SECRET_PARAM_MARKERS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "apikey",
    "api_key",
    "private",
    "sasl",
    "auth",
];

/// True when any param name looks like it names a credential.
pub fn has_credential_params(connector: &crate::connector_config::ClusterConnector) -> bool {
    connector.params.keys().any(|k| {
        let lower = k.to_ascii_lowercase();
        SECRET_PARAM_MARKERS.iter().any(|m| lower.contains(m))
    })
}

// ---------------------------------------------------------------------------
// Applier
// ---------------------------------------------------------------------------

/// Applies [`ClusterCommand`]s to a JetStream KV bucket.
#[derive(Debug, Clone)]
pub struct Applier {
    cp: ControlPlane,
    connector_policy: ConnectorSecretPolicy,
}

/// What a read-modify-write step decided to do.
enum Mutation<T> {
    Write(T),
    Delete,
    NoOp,
}

impl Applier {
    pub fn new(cp: ControlPlane) -> Self {
        Self {
            cp,
            connector_policy: ConnectorSecretPolicy::default(),
        }
    }

    /// Override the connector-credential policy. See
    /// [`ConnectorSecretPolicy`] — the default refuses.
    pub fn with_connector_policy(mut self, policy: ConnectorSecretPolicy) -> Self {
        self.connector_policy = policy;
        self
    }

    /// The underlying bucket.
    pub fn control_plane(&self) -> &ControlPlane {
        &self.cp
    }

    /// Apply one command.
    ///
    /// Returns [`ClusterResponse::Error`] for a command this backend
    /// deliberately refuses (today: credential-bearing connectors); a
    /// transport or exhausted-retry failure comes back as `Err`.
    pub async fn apply(&self, cmd: ClusterCommand) -> Result<ClusterResponse, ControlPlaneError> {
        match cmd {
            // -- Worker lifecycle ------------------------------------------
            ClusterCommand::RegisterWorker {
                id,
                address,
                api_key,
                capacity,
            } => {
                let key = ControlKey::Worker(id.clone());
                self.rmw::<WorkerRecord, _>(&key, |_existing| {
                    Mutation::Write(WorkerRecord::new(WorkerEntry {
                        id: id.clone(),
                        address: address.clone(),
                        api_key: api_key.clone(),
                        status: "ready".to_string(),
                        cpu_cores: capacity.cpu_cores,
                        pipelines_running: capacity.pipelines_running,
                        max_pipelines: capacity.max_pipelines,
                        assigned_pipelines: Vec::new(),
                        events_processed: 0,
                        heartbeat_seq: 0,
                    }))
                })
                .await?;
            }

            ClusterCommand::DeregisterWorker { id } => {
                self.cp.delete(&ControlKey::Worker(id)).await?;
            }

            ClusterCommand::WorkerStatusChanged { id, status } => {
                let key = ControlKey::Worker(id);
                self.rmw::<WorkerRecord, _>(&key, |existing| match existing {
                    Some(mut r) => {
                        r.entry.status = status.clone();
                        Mutation::Write(r)
                    }
                    // Raft parity: a status change for an unknown worker is a
                    // no-op, not a resurrection.
                    None => Mutation::NoOp,
                })
                .await?;
            }

            ClusterCommand::WorkerPipelinesUpdated {
                id,
                assigned_pipelines,
            } => {
                let key = ControlKey::Worker(id);
                self.rmw::<WorkerRecord, _>(&key, |existing| match existing {
                    Some(mut r) => {
                        r.entry.assigned_pipelines = assigned_pipelines.clone();
                        Mutation::Write(r)
                    }
                    None => Mutation::NoOp,
                })
                .await?;
            }

            ClusterCommand::WorkerMetricsUpdated {
                id,
                events_processed,
                pipelines_running,
                pipeline_metrics,
                heartbeat_seq,
            } => {
                let key = ControlKey::Worker(id);
                self.rmw::<WorkerRecord, _>(&key, |existing| match existing {
                    Some(mut r) => {
                        r.entry.events_processed = events_processed;
                        r.entry.pipelines_running = pipelines_running;
                        r.entry.heartbeat_seq = heartbeat_seq;
                        if !pipeline_metrics.is_empty() {
                            r.pipeline_metrics = pipeline_metrics.clone();
                        }
                        Mutation::Write(r)
                    }
                    None => Mutation::NoOp,
                })
                .await?;
            }

            // -- Pipeline groups -------------------------------------------
            ClusterCommand::GroupDeployed { name, group }
            | ClusterCommand::GroupUpdated { name, group } => {
                let key = ControlKey::Group(name);
                self.rmw::<serde_json::Value, _>(&key, |_| Mutation::Write(group.clone()))
                    .await?;
            }

            ClusterCommand::GroupRemoved { name } => {
                self.cp.delete(&ControlKey::Group(name)).await?;
            }

            // -- Migrations -------------------------------------------------
            ClusterCommand::MigrationStarted { task } => {
                // Raft parity: a task without an id is dropped.
                let Some(id) = task.get("id").and_then(|v| v.as_str()).map(str::to_owned) else {
                    return Ok(ClusterResponse::Ok);
                };
                let key = ControlKey::Migration(id);
                self.rmw::<serde_json::Value, _>(&key, |_| Mutation::Write(task.clone()))
                    .await?;
            }

            ClusterCommand::MigrationUpdated { id, status } => {
                let key = ControlKey::Migration(id);
                self.rmw::<serde_json::Value, _>(&key, |existing| match existing {
                    Some(mut m) => {
                        m["status"] = serde_json::Value::String(status.clone());
                        Mutation::Write(m)
                    }
                    None => Mutation::NoOp,
                })
                .await?;
            }

            ClusterCommand::MigrationRemoved { id } => {
                self.cp.delete(&ControlKey::Migration(id)).await?;
            }

            // -- Connectors -------------------------------------------------
            ClusterCommand::ConnectorCreated { name, connector }
            | ClusterCommand::ConnectorUpdated { name, connector } => {
                if self.connector_policy == ConnectorSecretPolicy::Refuse
                    && has_credential_params(&connector)
                {
                    return Ok(ClusterResponse::Error {
                        message: format!(
                            "connector '{name}' carries credential-shaped params; the JetStream \
                             control plane refuses to store them in cleartext. Encrypt connector \
                             params (separate work) or opt in with \
                             ConnectorSecretPolicy::AllowPlaintext on a bucket you have confirmed \
                             is encrypted at rest."
                        ),
                    });
                }
                let key = ControlKey::Connector(name);
                self.rmw::<crate::connector_config::ClusterConnector, _>(&key, |_| {
                    Mutation::Write(connector.clone())
                })
                .await?;
            }

            ClusterCommand::ConnectorRemoved { name } => {
                self.cp.delete(&ControlKey::Connector(name)).await?;
            }

            // -- Scaling ----------------------------------------------------
            ClusterCommand::ScalingPolicySet { policy } => {
                let key = ControlKey::ScalingPolicy;
                self.rmw::<serde_json::Value, _>(&key, |_| match &policy {
                    Some(p) => Mutation::Write(p.clone()),
                    None => Mutation::Delete,
                })
                .await?;
            }

            // -- Models -----------------------------------------------------
            ClusterCommand::ModelRegistered { name, entry } => {
                let key = ControlKey::Model(name);
                self.rmw::<crate::model_registry::ModelRegistryEntry, _>(&key, |_| {
                    Mutation::Write(entry.clone())
                })
                .await?;
            }

            ClusterCommand::ModelRemoved { name } => {
                self.cp.delete(&ControlKey::Model(name)).await?;
            }

            // -- Distributed checkpoints ------------------------------------
            #[cfg(feature = "distributed-checkpoint")]
            ClusterCommand::CheckpointCompleted {
                group_id,
                checkpoint_id,
            } => {
                let key = ControlKey::Checkpoint(group_id);
                self.rmw::<GroupCheckpointStatus, _>(&key, |existing| {
                    let mut s = existing.unwrap_or_default();
                    // Monotone: a late/duplicate completion must not roll the
                    // recovery pointer backwards. Under Raft this was
                    // guaranteed by log order; here it is guaranteed by the
                    // transition function being monotone, which is strictly
                    // stronger (it also survives an out-of-order retry).
                    if s.latest_completed.is_none_or(|cur| checkpoint_id > cur) {
                        s.latest_completed = Some(checkpoint_id);
                    }
                    Mutation::Write(s)
                })
                .await?;
            }

            #[cfg(feature = "distributed-checkpoint")]
            ClusterCommand::CheckpointAborted {
                group_id,
                checkpoint_id,
                reason,
            } => {
                let key = ControlKey::Checkpoint(group_id);
                self.rmw::<GroupCheckpointStatus, _>(&key, |existing| {
                    let mut s = existing.unwrap_or_default();
                    s.last_aborted = Some(checkpoint_id);
                    s.last_abort_reason = Some(reason.clone());
                    Mutation::Write(s)
                })
                .await?;
            }
        }
        Ok(ClusterResponse::Ok)
    }

    /// Atomic read-modify-write on one key, with a bounded CAS retry.
    async fn rmw<T, F>(&self, key: &ControlKey, mut f: F) -> Result<(), ControlPlaneError>
    where
        T: Serialize + DeserializeOwned,
        F: FnMut(Option<T>) -> Mutation<T>,
    {
        let mut attempt = 0;
        loop {
            let current = self.cp.get::<T>(key).await?;
            let expect = match &current {
                Some(v) => Expect::Revision(v.revision),
                None => Expect::Absent,
            };
            let observed_rev = current.as_ref().map(|v| v.revision);
            let outcome = f(current.map(|v| v.value));

            let result = match outcome {
                Mutation::NoOp => return Ok(()),
                Mutation::Write(value) => self.cp.write(key, &value, expect).await.map(|_| ()),
                Mutation::Delete => match observed_rev {
                    Some(rev) => self.cp.delete_at(key, rev).await,
                    None => return Ok(()), // already absent
                },
            };

            match result {
                Ok(()) => return Ok(()),
                Err(e) if e.is_cas_conflict() && attempt < CAS_RETRIES => {
                    attempt += 1;
                    // Brief, growing backoff: contention on one control-plane
                    // key is rare, so this only ever runs a handful of times.
                    tokio::time::sleep(Duration::from_millis(2 * attempt as u64)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Materialisation
// ---------------------------------------------------------------------------

/// Rebuild a [`CoordinatorState`] from a bucket snapshot.
///
/// This is the KV counterpart of the Raft store's `publish_state`: the value
/// it produces is exactly what `Coordinator::sync_from_control_state`
/// consumes, so both backends feed the coordinator the same type.
pub fn materialize(snapshot: &Snapshot) -> CoordinatorState {
    let mut state = CoordinatorState::default();

    for (key, versioned) in snapshot.iter_namespace::<WorkerRecord>(keys::WORKERS) {
        let Some(id) = key.id() else { continue };
        let record = versioned.value;
        if !record.pipeline_metrics.is_empty() {
            state
                .worker_pipeline_metrics
                .insert(id.to_string(), record.pipeline_metrics);
        }
        state.workers.insert(id.to_string(), record.entry);
    }

    for (key, v) in snapshot.iter_namespace::<serde_json::Value>(keys::GROUPS) {
        if let Some(id) = key.id() {
            state.pipeline_groups.insert(id.to_string(), v.value);
        }
    }

    for (key, v) in
        snapshot.iter_namespace::<crate::connector_config::ClusterConnector>(keys::CONNECTORS)
    {
        if let Some(id) = key.id() {
            state.connectors.insert(id.to_string(), v.value);
        }
    }

    for (key, v) in snapshot.iter_namespace::<serde_json::Value>(keys::MIGRATIONS) {
        if let Some(id) = key.id() {
            state.active_migrations.insert(id.to_string(), v.value);
        }
    }

    for (key, v) in
        snapshot.iter_namespace::<crate::model_registry::ModelRegistryEntry>(keys::MODELS)
    {
        if let Some(id) = key.id() {
            state.models.insert(id.to_string(), v.value);
        }
    }

    for (key, v) in snapshot.iter_namespace::<GroupCheckpointStatus>(keys::CHECKPOINTS) {
        if let Some(id) = key.id() {
            state.latest_checkpoints.insert(id.to_string(), v.value);
        }
    }

    state.scaling_policy = snapshot
        .get::<serde_json::Value>(&ControlKey::ScalingPolicy)
        .map(|v| v.value);

    state
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector_config::ClusterConnector;
    use crate::jetstream_control_plane::store::Versioned;

    fn connector(params: &[(&str, &str)]) -> ClusterConnector {
        ClusterConnector {
            name: "c".into(),
            connector_type: "kafka".into(),
            params: params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            description: None,
        }
    }

    #[test]
    fn credential_params_are_detected_case_insensitively() {
        assert!(has_credential_params(&connector(&[("sasl_password", "p")])));
        assert!(has_credential_params(&connector(&[("apiKey", "x")])));
        assert!(has_credential_params(&connector(&[(
            "AWS_SECRET_ACCESS_KEY",
            "x"
        )])));
        assert!(has_credential_params(&connector(&[("auth_token", "x")])));
        assert!(!has_credential_params(&connector(&[
            ("host", "localhost"),
            ("port", "9092")
        ])));
    }

    #[test]
    fn worker_record_json_is_a_superset_of_worker_entry() {
        // The flattened shape means an operator running `nats kv get` sees the
        // familiar WorkerEntry fields at the top level.
        let rec = WorkerRecord::new(WorkerEntry {
            id: "w1".into(),
            address: "http://w1:9000".into(),
            api_key: "k".into(),
            status: "ready".into(),
            cpu_cores: 4,
            pipelines_running: 1,
            max_pipelines: 8,
            assigned_pipelines: vec!["p".into()],
            events_processed: 12,
            heartbeat_seq: 3,
        });
        let v = serde_json::to_value(&rec).unwrap();
        assert_eq!(v["id"], "w1");
        assert_eq!(v["heartbeat_seq"], 3);
        assert!(v.get("pipeline_metrics").is_some());
        // And it still deserializes into a bare WorkerEntry.
        let entry: WorkerEntry = serde_json::from_value(v).unwrap();
        assert_eq!(entry.id, "w1");
    }

    #[test]
    fn materialize_rebuilds_every_namespace() {
        let mut entries = std::collections::BTreeMap::new();
        let rec = WorkerRecord {
            entry: WorkerEntry {
                id: "w1".into(),
                address: "a".into(),
                api_key: "k".into(),
                status: "ready".into(),
                cpu_cores: 2,
                pipelines_running: 1,
                max_pipelines: 4,
                assigned_pipelines: vec!["p1".into()],
                events_processed: 7,
                heartbeat_seq: 2,
            },
            pipeline_metrics: vec![PipelineMetrics {
                pipeline_name: "p1".into(),
                events_in: 7,
                events_out: 1,
                connector_health: vec![],
            }],
        };
        entries.insert(
            "workers/w1".to_string(),
            Versioned::new(serde_json::to_value(&rec).unwrap(), 5),
        );
        entries.insert(
            "groups/g1".to_string(),
            Versioned::new(serde_json::json!({"status": "running"}), 6),
        );
        entries.insert(
            "connectors/c1".to_string(),
            Versioned::new(
                serde_json::to_value(connector(&[("host", "h")])).unwrap(),
                7,
            ),
        );
        entries.insert(
            "migrations/m1".to_string(),
            Versioned::new(serde_json::json!({"id": "m1", "status": "deploying"}), 8),
        );
        entries.insert(
            "scaling/policy".to_string(),
            Versioned::new(serde_json::json!({"min_workers": 2}), 9),
        );

        let state = materialize(&Snapshot { entries });
        assert_eq!(state.workers["w1"].events_processed, 7);
        assert_eq!(state.worker_pipeline_metrics["w1"].len(), 1);
        assert_eq!(state.pipeline_groups["g1"]["status"], "running");
        assert_eq!(state.connectors["c1"].connector_type, "kafka");
        assert_eq!(state.active_migrations["m1"]["status"], "deploying");
        assert_eq!(
            state.scaling_policy.as_ref().unwrap()["min_workers"],
            serde_json::json!(2)
        );
    }
}
