//! Coordinator High Availability: K8s Lease-based leader election and state replication.
//!
//! ## Architecture
//!
//! - 3 coordinator pods (StatefulSet)
//! - One leader (does all writes: health sweeps, failovers, migrations)
//! - Two followers (receive state replication, serve read-only queries)
//! - K8s Service readiness probe routes worker traffic to leader only
//! - On leader failure: K8s Lease expires (~10s), follower promotes
//!
//! Requires the `k8s` feature flag.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{error, info, warn};

use crate::connector_config::ClusterConnector;
use crate::worker::WorkerId;

// ============================================================================
// HA Role
// ============================================================================

/// The HA role of a coordinator instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HaRole {
    /// Default: no HA, single coordinator
    Standalone,
    /// Active coordinator — handles all writes
    Leader,
    /// Passive coordinator — receives state replication
    Follower { leader_id: String },
}

impl std::fmt::Display for HaRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HaRole::Standalone => write!(f, "standalone"),
            HaRole::Leader => write!(f, "leader"),
            HaRole::Follower { leader_id } => write!(f, "follower(leader={})", leader_id),
        }
    }
}

impl HaRole {
    /// Returns true if this instance can perform writes.
    pub fn is_writer(&self) -> bool {
        matches!(self, HaRole::Standalone | HaRole::Leader)
    }
}

// ============================================================================
// State Replication
// ============================================================================

/// Serializable snapshot of a worker node (WorkerNode contains Instant which isn't serializable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerNodeSnapshot {
    pub id: String,
    pub address: String,
    pub status: String,
    pub pipelines_running: usize,
    pub max_pipelines: usize,
    pub cpu_cores: usize,
    pub assigned_pipelines: Vec<String>,
    pub events_processed: u64,
}

/// Serializable snapshot of the coordinator state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorSnapshot {
    pub workers: HashMap<String, WorkerNodeSnapshot>,
    pub pipeline_groups: HashMap<String, serde_json::Value>,
    pub connectors: HashMap<String, ClusterConnector>,
    pub scaling_policy: Option<serde_json::Value>,
}

/// Incremental state deltas for replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StateDelta {
    WorkerRegistered {
        id: String,
        node: WorkerNodeSnapshot,
    },
    WorkerHeartbeat {
        id: String,
        pipelines_running: usize,
        events_processed: u64,
    },
    WorkerRemoved {
        id: String,
    },
    WorkerStatusChanged {
        id: String,
        status: String,
    },
    GroupDeployed {
        name: String,
        group: serde_json::Value,
    },
    GroupRemoved {
        name: String,
    },
    MigrationUpdated {
        id: String,
        status: String,
    },
    ConnectorChanged {
        name: String,
        connector: ClusterConnector,
    },
    ConnectorRemoved {
        name: String,
    },
}

/// Messages in the replication protocol (internal WS at /internal/replicate).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ReplicationMessage {
    /// Sent on follower connect: full state snapshot.
    FullSnapshot(CoordinatorSnapshot),
    /// Incremental update.
    Delta(StateDelta),
    /// Follower requests a full snapshot.
    RequestSnapshot,
}

// ============================================================================
// Leader Election
// ============================================================================

/// Status of the leader election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderStatus {
    Leader,
    Follower { leader_id: String },
    Unknown,
}

/// K8s Lease-based leader elector.
#[cfg(feature = "k8s")]
pub struct LeaderElector {
    pub lease_name: String,
    pub namespace: String,
    pub identity: String,
    pub lease_duration: std::time::Duration,
    pub renew_deadline: std::time::Duration,
    pub retry_period: std::time::Duration,
}

#[cfg(feature = "k8s")]
impl LeaderElector {
    pub fn new(namespace: String, identity: String) -> Self {
        Self {
            lease_name: "varpulis-coordinator-leader".to_string(),
            namespace,
            identity,
            lease_duration: std::time::Duration::from_secs(15),
            renew_deadline: std::time::Duration::from_secs(10),
            retry_period: std::time::Duration::from_secs(2),
        }
    }

    /// Run the leader election loop. Calls `on_status_change` when the role changes.
    pub async fn run<F>(&self, on_status_change: F)
    where
        F: Fn(LeaderStatus) + Send + Sync,
    {
        use k8s_openapi::api::coordination::v1::Lease;
        use kube::{Api, Client};

        let client = match Client::try_default().await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create K8s client for leader election: {}", e);
                on_status_change(LeaderStatus::Unknown);
                return;
            }
        };

        let leases: Api<Lease> = Api::namespaced(client, &self.namespace);
        let mut current_status = LeaderStatus::Unknown;

        loop {
            let new_status = match self.try_acquire_or_renew(&leases).await {
                Ok(true) => LeaderStatus::Leader,
                Ok(false) => {
                    // Check who the leader is
                    match self.get_current_leader(&leases).await {
                        Some(leader_id) => LeaderStatus::Follower { leader_id },
                        None => LeaderStatus::Unknown,
                    }
                }
                Err(e) => {
                    warn!("Leader election error: {}", e);
                    LeaderStatus::Unknown
                }
            };

            if new_status != current_status {
                info!(
                    "Leader election status changed: {:?} -> {:?}",
                    current_status, new_status
                );
                on_status_change(new_status.clone());
                current_status = new_status;
            }

            tokio::time::sleep(self.retry_period).await;
        }
    }

    /// Try to acquire or renew the lease. Returns true if we're the leader.
    async fn try_acquire_or_renew(
        &self,
        leases: &kube::Api<k8s_openapi::api::coordination::v1::Lease>,
    ) -> Result<bool, String> {
        use k8s_openapi::api::coordination::v1::Lease;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
        use kube::api::{ObjectMeta, PostParams};

        let now = chrono::Utc::now();

        match leases.get(&self.lease_name).await {
            Ok(existing) => {
                let spec = existing.spec.as_ref();
                let holder = spec
                    .and_then(|s| s.holder_identity.as_deref())
                    .unwrap_or("");

                if holder == self.identity {
                    // We hold it — renew
                    let mut updated = existing.clone();
                    if let Some(ref mut s) = updated.spec {
                        s.renew_time = Some(MicroTime(now));
                    }
                    leases
                        .replace(&self.lease_name, &PostParams::default(), &updated)
                        .await
                        .map_err(|e| format!("Lease renew failed: {}", e))?;
                    return Ok(true);
                }

                // Someone else holds it — check if expired
                let renew_time = spec.and_then(|s| s.renew_time.as_ref()).map(|t| t.0);
                let lease_duration_secs = spec.and_then(|s| s.lease_duration_seconds).unwrap_or(15);

                if let Some(renew) = renew_time {
                    let expiry = renew + chrono::Duration::seconds(lease_duration_secs as i64);
                    if now > expiry {
                        // Expired — try to take over
                        let mut updated = existing.clone();
                        if let Some(ref mut s) = updated.spec {
                            s.holder_identity = Some(self.identity.clone());
                            s.renew_time = Some(MicroTime(now));
                            s.acquire_time = Some(MicroTime(now));
                            s.lease_duration_seconds = Some(self.lease_duration.as_secs() as i32);
                        }
                        match leases
                            .replace(&self.lease_name, &PostParams::default(), &updated)
                            .await
                        {
                            Ok(_) => return Ok(true),
                            Err(_) => return Ok(false), // Another node got it
                        }
                    }
                }

                Ok(false) // Someone else holds it and it's not expired
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => {
                // Lease doesn't exist — create it
                let lease = Lease {
                    metadata: ObjectMeta {
                        name: Some(self.lease_name.clone()),
                        namespace: Some(self.namespace.clone()),
                        ..Default::default()
                    },
                    spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                        holder_identity: Some(self.identity.clone()),
                        lease_duration_seconds: Some(self.lease_duration.as_secs() as i32),
                        acquire_time: Some(MicroTime(now)),
                        renew_time: Some(MicroTime(now)),
                        ..Default::default()
                    }),
                };
                match leases.create(&PostParams::default(), &lease).await {
                    Ok(_) => Ok(true),
                    Err(_) => Ok(false), // Race condition — another node created it
                }
            }
            Err(e) => Err(format!("Lease get failed: {}", e)),
        }
    }

    /// Get the current lease holder identity.
    async fn get_current_leader(
        &self,
        leases: &kube::Api<k8s_openapi::api::coordination::v1::Lease>,
    ) -> Option<String> {
        match leases.get(&self.lease_name).await {
            Ok(lease) => lease.spec.as_ref().and_then(|s| s.holder_identity.clone()),
            Err(_) => None,
        }
    }
}

/// Check if the coordinator is allowed to perform write operations.
pub fn require_writer(role: &HaRole) -> Result<(), crate::ClusterError> {
    if role.is_writer() {
        Ok(())
    } else {
        let leader = match role {
            HaRole::Follower { leader_id } => leader_id.clone(),
            _ => "unknown".to_string(),
        };
        Err(crate::ClusterError::NotLeader(leader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ha_role_standalone_allows_writes() {
        assert!(require_writer(&HaRole::Standalone).is_ok());
    }

    #[test]
    fn test_ha_role_leader_allows_writes() {
        assert!(require_writer(&HaRole::Leader).is_ok());
    }

    #[test]
    fn test_ha_role_follower_rejects_writes() {
        let role = HaRole::Follower {
            leader_id: "coord-0".into(),
        };
        let err = require_writer(&role).unwrap_err();
        assert!(err.to_string().contains("coord-0"));
    }

    #[test]
    fn test_ha_role_display() {
        assert_eq!(HaRole::Standalone.to_string(), "standalone");
        assert_eq!(HaRole::Leader.to_string(), "leader");
        assert_eq!(
            HaRole::Follower {
                leader_id: "c0".into()
            }
            .to_string(),
            "follower(leader=c0)"
        );
    }

    #[test]
    fn test_ha_role_serde() {
        let role = HaRole::Follower {
            leader_id: "coord-1".into(),
        };
        let json = serde_json::to_string(&role).unwrap();
        let parsed: HaRole = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, role);

        for r in [
            HaRole::Standalone,
            HaRole::Leader,
            HaRole::Follower {
                leader_id: "x".into(),
            },
        ] {
            let json = serde_json::to_string(&r).unwrap();
            let parsed: HaRole = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, r);
        }
    }

    #[test]
    fn test_coordinator_snapshot_serde() {
        let snapshot = CoordinatorSnapshot {
            workers: HashMap::new(),
            pipeline_groups: HashMap::new(),
            connectors: HashMap::new(),
            scaling_policy: None,
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        let parsed: CoordinatorSnapshot = serde_json::from_str(&json).unwrap();
        assert!(parsed.workers.is_empty());
        assert!(parsed.pipeline_groups.is_empty());
    }

    #[test]
    fn test_coordinator_snapshot_with_workers() {
        let mut workers = HashMap::new();
        workers.insert(
            "w1".to_string(),
            WorkerNodeSnapshot {
                id: "w1".into(),
                address: "http://localhost:9000".into(),
                status: "ready".into(),
                pipelines_running: 3,
                max_pipelines: 100,
                cpu_cores: 4,
                assigned_pipelines: vec!["p1".into(), "p2".into()],
                events_processed: 42000,
            },
        );
        let snapshot = CoordinatorSnapshot {
            workers,
            pipeline_groups: HashMap::new(),
            connectors: HashMap::new(),
            scaling_policy: None,
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        let parsed: CoordinatorSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.workers.len(), 1);
        assert_eq!(parsed.workers["w1"].pipelines_running, 3);
    }

    #[test]
    fn test_state_delta_serde() {
        let deltas = vec![
            StateDelta::WorkerRegistered {
                id: "w1".into(),
                node: WorkerNodeSnapshot {
                    id: "w1".into(),
                    address: "http://localhost:9000".into(),
                    status: "registering".into(),
                    pipelines_running: 0,
                    max_pipelines: 100,
                    cpu_cores: 4,
                    assigned_pipelines: vec![],
                    events_processed: 0,
                },
            },
            StateDelta::WorkerHeartbeat {
                id: "w1".into(),
                pipelines_running: 3,
                events_processed: 500,
            },
            StateDelta::WorkerRemoved { id: "w1".into() },
            StateDelta::WorkerStatusChanged {
                id: "w1".into(),
                status: "unhealthy".into(),
            },
            StateDelta::GroupDeployed {
                name: "g1".into(),
                group: serde_json::json!({"name": "test-group"}),
            },
            StateDelta::GroupRemoved { name: "g1".into() },
            StateDelta::MigrationUpdated {
                id: "m1".into(),
                status: "completed".into(),
            },
            StateDelta::ConnectorRemoved {
                name: "mqtt1".into(),
            },
        ];

        for delta in deltas {
            let json = serde_json::to_string(&delta).unwrap();
            let parsed: StateDelta = serde_json::from_str(&json).unwrap();
            // Just verify round-trip doesn't panic
            let _ = serde_json::to_string(&parsed).unwrap();
        }
    }

    #[test]
    fn test_replication_message_serde() {
        let snapshot = CoordinatorSnapshot {
            workers: HashMap::new(),
            pipeline_groups: HashMap::new(),
            connectors: HashMap::new(),
            scaling_policy: None,
        };
        let msg = ReplicationMessage::FullSnapshot(snapshot);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: ReplicationMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ReplicationMessage::FullSnapshot(_)));

        let msg = ReplicationMessage::RequestSnapshot;
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: ReplicationMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ReplicationMessage::RequestSnapshot));
    }

    #[test]
    fn test_leader_status_equality() {
        assert_eq!(LeaderStatus::Leader, LeaderStatus::Leader);
        assert_eq!(LeaderStatus::Unknown, LeaderStatus::Unknown);
        assert_eq!(
            LeaderStatus::Follower {
                leader_id: "a".into()
            },
            LeaderStatus::Follower {
                leader_id: "a".into()
            }
        );
        assert_ne!(LeaderStatus::Leader, LeaderStatus::Unknown);
    }
}
