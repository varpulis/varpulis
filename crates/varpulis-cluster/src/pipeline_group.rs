//! Pipeline group abstraction for deploying related pipelines together.

use crate::worker::WorkerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::time::Instant;
use tracing::warn;

/// Specification for a group of related pipelines to be deployed together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineGroupSpec {
    pub name: String,
    pub pipelines: Vec<PipelinePlacement>,
    #[serde(default)]
    pub routes: Vec<InterPipelineRoute>,
}

/// A single pipeline within a group, with optional worker affinity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelinePlacement {
    pub name: String,
    pub source: String,
    #[serde(default)]
    pub worker_affinity: Option<String>,
    /// Number of replicas to deploy (default 1).
    #[serde(default = "default_replicas")]
    pub replicas: usize,
    /// Field name for hash-based event partitioning across replicas.
    #[serde(default)]
    pub partition_key: Option<String>,
}

fn default_replicas() -> usize {
    1
}

/// Route specification for inter-pipeline event routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterPipelineRoute {
    pub from_pipeline: String,
    pub to_pipeline: String,
    pub event_types: Vec<String>,
    #[serde(default)]
    pub mqtt_topic: Option<String>,
}

/// Status of a deployed pipeline group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupStatus {
    Deploying,
    Running,
    PartiallyRunning,
    Failed,
    TornDown,
}

impl std::fmt::Display for GroupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deploying => write!(f, "deploying"),
            Self::Running => write!(f, "running"),
            Self::PartiallyRunning => write!(f, "partially_running"),
            Self::Failed => write!(f, "failed"),
            Self::TornDown => write!(f, "torn_down"),
        }
    }
}

/// Status of an individual pipeline deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineDeploymentStatus {
    Deploying,
    Running,
    Failed,
    Stopped,
}

/// Tracks the deployment of a single pipeline within a group.
#[derive(Debug, Clone)]
pub struct PipelineDeployment {
    pub worker_id: WorkerId,
    pub worker_address: String,
    pub worker_api_key: String,
    pub pipeline_id: String,
    pub status: PipelineDeploymentStatus,
}

/// A deployed pipeline group with placement and status tracking.
pub struct DeployedPipelineGroup {
    pub id: String,
    pub name: String,
    pub spec: PipelineGroupSpec,
    pub placements: HashMap<String, PipelineDeployment>,
    /// Maps logical pipeline name → ReplicaGroup (only for replicas > 1).
    pub replica_groups: HashMap<String, ReplicaGroup>,
    pub created_at: Instant,
    pub status: GroupStatus,
}

impl DeployedPipelineGroup {
    pub fn new(id: String, name: String, spec: PipelineGroupSpec) -> Self {
        Self {
            id,
            name,
            spec,
            placements: HashMap::new(),
            replica_groups: HashMap::new(),
            created_at: Instant::now(),
            status: GroupStatus::Deploying,
        }
    }

    /// Update group status based on individual pipeline statuses.
    pub fn update_status(&mut self) {
        if self.placements.is_empty() {
            return;
        }

        let all_running = self
            .placements
            .values()
            .all(|p| p.status == PipelineDeploymentStatus::Running);
        let any_running = self
            .placements
            .values()
            .any(|p| p.status == PipelineDeploymentStatus::Running);
        let all_failed = self
            .placements
            .values()
            .all(|p| p.status == PipelineDeploymentStatus::Failed);

        self.status = if all_running {
            GroupStatus::Running
        } else if all_failed {
            GroupStatus::Failed
        } else if any_running {
            GroupStatus::PartiallyRunning
        } else {
            GroupStatus::Deploying
        };
    }
}

/// Strategy for routing events to pipeline replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    /// Events are distributed round-robin across replicas.
    RoundRobin,
    /// Events are routed based on a hash of the specified field.
    HashKey(String),
}

/// Tracks a group of replicas for a single logical pipeline.
#[derive(Debug)]
pub struct ReplicaGroup {
    pub pipeline_name: String,
    pub replica_names: Vec<String>,
    pub strategy: PartitionStrategy,
    pub counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    missing_field_warned: AtomicBool,
}

impl Clone for ReplicaGroup {
    fn clone(&self) -> Self {
        Self {
            pipeline_name: self.pipeline_name.clone(),
            replica_names: self.replica_names.clone(),
            strategy: self.strategy.clone(),
            counter: self.counter.clone(),
            missing_field_warned: AtomicBool::new(
                self.missing_field_warned
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

impl ReplicaGroup {
    pub fn new(
        pipeline_name: String,
        replica_names: Vec<String>,
        strategy: PartitionStrategy,
    ) -> Self {
        Self {
            pipeline_name,
            replica_names,
            strategy,
            counter: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            missing_field_warned: AtomicBool::new(false),
        }
    }

    /// Select a replica name for the given event fields.
    pub fn select_replica(&self, fields: &serde_json::Map<String, serde_json::Value>) -> &str {
        if self.replica_names.is_empty() {
            return &self.pipeline_name;
        }
        let idx = match &self.strategy {
            PartitionStrategy::RoundRobin => {
                self.counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    % self.replica_names.len()
            }
            PartitionStrategy::HashKey(field) => {
                let value = match fields.get(field) {
                    Some(v) => v.to_string(),
                    None => {
                        if !self
                            .missing_field_warned
                            .swap(true, std::sync::atomic::Ordering::Relaxed)
                        {
                            warn!(
                                "Partition field '{}' missing from event for pipeline '{}'; \
                                 all such events will route to the same replica",
                                field, self.pipeline_name
                            );
                        }
                        String::new()
                    }
                };
                // Simple hash: sum of bytes mod N
                let hash: usize = value.bytes().map(|b| b as usize).sum();
                hash % self.replica_names.len()
            }
        };
        &self.replica_names[idx]
    }
}

/// Serializable pipeline group info for API responses.
#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineGroupInfo {
    pub id: String,
    pub name: String,
    pub status: String,
    pub pipeline_count: usize,
    pub placements: Vec<PipelinePlacementInfo>,
    /// Maps pipeline name → VPL source code (for viewing/editing deployed pipelines).
    #[serde(default)]
    pub sources: HashMap<String, String>,
}

/// Serializable placement info for API responses.
#[derive(Debug, Serialize, Deserialize)]
pub struct PipelinePlacementInfo {
    pub pipeline_name: String,
    pub worker_id: String,
    pub worker_address: String,
    pub pipeline_id: String,
    pub status: String,
}

impl From<&DeployedPipelineGroup> for PipelineGroupInfo {
    fn from(group: &DeployedPipelineGroup) -> Self {
        let placements = group
            .placements
            .iter()
            .map(|(name, dep)| PipelinePlacementInfo {
                pipeline_name: name.clone(),
                worker_id: dep.worker_id.0.clone(),
                worker_address: dep.worker_address.clone(),
                pipeline_id: dep.pipeline_id.clone(),
                status: format!("{:?}", dep.status),
            })
            .collect();
        let sources = group
            .spec
            .pipelines
            .iter()
            .map(|p| (p.name.clone(), p.source.clone()))
            .collect();
        Self {
            id: group.id.clone(),
            name: group.name.clone(),
            status: group.status.to_string(),
            pipeline_count: group.spec.pipelines.len(),
            placements,
            sources,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_status_tracking() {
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![
                PipelinePlacement {
                    name: "p1".into(),
                    source: "stream A = X".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "stream B = Y".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
            ],
            routes: vec![],
        };

        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        assert_eq!(group.status, GroupStatus::Deploying);

        group.placements.insert(
            "p1".into(),
            PipelineDeployment {
                worker_id: WorkerId("w1".into()),
                worker_address: "http://localhost:9000".into(),
                worker_api_key: "key".into(),
                pipeline_id: "pid1".into(),
                status: PipelineDeploymentStatus::Running,
            },
        );
        group.placements.insert(
            "p2".into(),
            PipelineDeployment {
                worker_id: WorkerId("w2".into()),
                worker_address: "http://localhost:9001".into(),
                worker_api_key: "key".into(),
                pipeline_id: "pid2".into(),
                status: PipelineDeploymentStatus::Running,
            },
        );
        group.update_status();
        assert_eq!(group.status, GroupStatus::Running);

        // Mark one as failed
        group.placements.get_mut("p2").unwrap().status = PipelineDeploymentStatus::Failed;
        group.update_status();
        assert_eq!(group.status, GroupStatus::PartiallyRunning);
    }

    #[test]
    fn test_pipeline_group_spec_serde() {
        let json = r#"{
            "name": "test-group",
            "pipelines": [
                {"name": "p1", "source": "stream A = X", "worker_affinity": "worker-0"},
                {"name": "p2", "source": "stream B = Y"}
            ],
            "routes": [
                {"from_pipeline": "_external", "to_pipeline": "p1", "event_types": ["Event*"]}
            ]
        }"#;
        let spec: PipelineGroupSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.name, "test-group");
        assert_eq!(spec.pipelines.len(), 2);
        assert_eq!(
            spec.pipelines[0].worker_affinity,
            Some("worker-0".to_string())
        );
        assert!(spec.pipelines[1].worker_affinity.is_none());
        assert_eq!(spec.routes.len(), 1);
    }

    #[test]
    fn test_group_status_all_failed() {
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![
                PipelinePlacement {
                    name: "p1".into(),
                    source: "".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
            ],
            routes: vec![],
        };

        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        group.placements.insert(
            "p1".into(),
            PipelineDeployment {
                worker_id: WorkerId("w1".into()),
                worker_address: "http://localhost:9000".into(),
                worker_api_key: "key".into(),
                pipeline_id: "".into(),
                status: PipelineDeploymentStatus::Failed,
            },
        );
        group.placements.insert(
            "p2".into(),
            PipelineDeployment {
                worker_id: WorkerId("w2".into()),
                worker_address: "http://localhost:9001".into(),
                worker_api_key: "key".into(),
                pipeline_id: "".into(),
                status: PipelineDeploymentStatus::Failed,
            },
        );
        group.update_status();
        assert_eq!(group.status, GroupStatus::Failed);
    }

    #[test]
    fn test_group_status_empty_placements() {
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![],
            routes: vec![],
        };

        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        assert_eq!(group.status, GroupStatus::Deploying);
        group.update_status();
        // Empty placements should leave status unchanged
        assert_eq!(group.status, GroupStatus::Deploying);
    }

    #[test]
    fn test_group_status_mixed_deploying_and_running() {
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![
                PipelinePlacement {
                    name: "p1".into(),
                    source: "".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
            ],
            routes: vec![],
        };

        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        group.placements.insert(
            "p1".into(),
            PipelineDeployment {
                worker_id: WorkerId("w1".into()),
                worker_address: "http://localhost:9000".into(),
                worker_api_key: "key".into(),
                pipeline_id: "pid1".into(),
                status: PipelineDeploymentStatus::Running,
            },
        );
        group.placements.insert(
            "p2".into(),
            PipelineDeployment {
                worker_id: WorkerId("w2".into()),
                worker_address: "http://localhost:9001".into(),
                worker_api_key: "key".into(),
                pipeline_id: "pid2".into(),
                status: PipelineDeploymentStatus::Deploying,
            },
        );
        group.update_status();
        // One running + one deploying = PartiallyRunning (any_running && !all_running)
        assert_eq!(group.status, GroupStatus::PartiallyRunning);
    }

    #[test]
    fn test_group_status_all_stopped() {
        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![PipelinePlacement {
                name: "p1".into(),
                source: "".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            }],
            routes: vec![],
        };

        let mut group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        group.placements.insert(
            "p1".into(),
            PipelineDeployment {
                worker_id: WorkerId("w1".into()),
                worker_address: "http://localhost:9000".into(),
                worker_api_key: "key".into(),
                pipeline_id: "pid1".into(),
                status: PipelineDeploymentStatus::Stopped,
            },
        );
        group.update_status();
        // Not running, not failed → Deploying (fallback)
        assert_eq!(group.status, GroupStatus::Deploying);
    }

    #[test]
    fn test_group_status_display() {
        assert_eq!(GroupStatus::Deploying.to_string(), "deploying");
        assert_eq!(GroupStatus::Running.to_string(), "running");
        assert_eq!(
            GroupStatus::PartiallyRunning.to_string(),
            "partially_running"
        );
        assert_eq!(GroupStatus::Failed.to_string(), "failed");
        assert_eq!(GroupStatus::TornDown.to_string(), "torn_down");
    }

    #[test]
    fn test_group_status_serde() {
        for status in [
            GroupStatus::Deploying,
            GroupStatus::Running,
            GroupStatus::PartiallyRunning,
            GroupStatus::Failed,
            GroupStatus::TornDown,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let parsed: GroupStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_pipeline_group_info_from_deployed() {
        let spec = PipelineGroupSpec {
            name: "test-group".into(),
            pipelines: vec![
                PipelinePlacement {
                    name: "p1".into(),
                    source: "stream A = X".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "stream B = Y".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
            ],
            routes: vec![],
        };

        let mut group = DeployedPipelineGroup::new("g1".into(), "test-group".into(), spec);
        group.status = GroupStatus::Running;
        group.placements.insert(
            "p1".into(),
            PipelineDeployment {
                worker_id: WorkerId("w1".into()),
                worker_address: "http://localhost:9000".into(),
                worker_api_key: "key".into(),
                pipeline_id: "pid-abc".into(),
                status: PipelineDeploymentStatus::Running,
            },
        );

        let info = PipelineGroupInfo::from(&group);
        assert_eq!(info.id, "g1");
        assert_eq!(info.name, "test-group");
        assert_eq!(info.status, "running");
        assert_eq!(info.pipeline_count, 2);
        assert_eq!(info.placements.len(), 1); // only placed pipelines
        assert_eq!(info.sources.len(), 2);
        assert_eq!(info.sources["p1"], "stream A = X");
        assert_eq!(info.sources["p2"], "stream B = Y");

        let p = &info.placements[0];
        assert_eq!(p.pipeline_name, "p1");
        assert_eq!(p.worker_id, "w1");
        assert_eq!(p.pipeline_id, "pid-abc");
    }

    #[test]
    fn test_pipeline_group_info_serde() {
        let info = PipelineGroupInfo {
            id: "g1".into(),
            name: "test".into(),
            status: "running".into(),
            pipeline_count: 2,
            placements: vec![PipelinePlacementInfo {
                pipeline_name: "p1".into(),
                worker_id: "w1".into(),
                worker_address: "http://localhost:9000".into(),
                pipeline_id: "pid1".into(),
                status: "Running".into(),
            }],
            sources: HashMap::new(),
        };

        let json = serde_json::to_string(&info).unwrap();
        let parsed: PipelineGroupInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "g1");
        assert_eq!(parsed.placements.len(), 1);
    }

    #[test]
    fn test_pipeline_group_spec_no_routes() {
        let json = r#"{
            "name": "simple",
            "pipelines": [
                {"name": "p1", "source": "stream A = X"}
            ]
        }"#;
        let spec: PipelineGroupSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.name, "simple");
        assert!(spec.routes.is_empty()); // default
    }

    #[test]
    fn test_inter_pipeline_route_serde() {
        let route = InterPipelineRoute {
            from_pipeline: "a".into(),
            to_pipeline: "b".into(),
            event_types: vec!["TypeA*".into(), "TypeB".into()],
            mqtt_topic: Some("custom/topic".into()),
        };
        let json = serde_json::to_string(&route).unwrap();
        let parsed: InterPipelineRoute = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.from_pipeline, "a");
        assert_eq!(parsed.to_pipeline, "b");
        assert_eq!(parsed.event_types.len(), 2);
        assert_eq!(parsed.mqtt_topic, Some("custom/topic".into()));
    }

    #[test]
    fn test_inter_pipeline_route_no_mqtt_topic() {
        let json = r#"{
            "from_pipeline": "a",
            "to_pipeline": "b",
            "event_types": ["*"]
        }"#;
        let parsed: InterPipelineRoute = serde_json::from_str(json).unwrap();
        assert!(parsed.mqtt_topic.is_none());
    }

    #[test]
    fn test_single_pipeline_group() {
        let spec = PipelineGroupSpec {
            name: "singleton".into(),
            pipelines: vec![PipelinePlacement {
                name: "only".into(),
                source: "stream A = X".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            }],
            routes: vec![],
        };

        let mut group = DeployedPipelineGroup::new("g1".into(), "singleton".into(), spec);
        group.placements.insert(
            "only".into(),
            PipelineDeployment {
                worker_id: WorkerId("w1".into()),
                worker_address: "http://localhost:9000".into(),
                worker_api_key: "key".into(),
                pipeline_id: "pid1".into(),
                status: PipelineDeploymentStatus::Running,
            },
        );
        group.update_status();
        assert_eq!(group.status, GroupStatus::Running);
    }

    // =========================================================================
    // ReplicaGroup tests (fixes #5, #6)
    // =========================================================================

    #[test]
    fn test_replica_group_round_robin() {
        let group = ReplicaGroup::new(
            "p1".into(),
            vec!["p1#0".into(), "p1#1".into(), "p1#2".into()],
            PartitionStrategy::RoundRobin,
        );
        let fields = serde_json::Map::new();

        // Round-robin cycles through all replicas
        assert_eq!(group.select_replica(&fields), "p1#0");
        assert_eq!(group.select_replica(&fields), "p1#1");
        assert_eq!(group.select_replica(&fields), "p1#2");
        assert_eq!(group.select_replica(&fields), "p1#0"); // wraps around
    }

    #[test]
    fn test_replica_group_hash_key_deterministic() {
        let group = ReplicaGroup::new(
            "p1".into(),
            vec!["p1#0".into(), "p1#1".into()],
            PartitionStrategy::HashKey("source".into()),
        );

        let mut fields1 = serde_json::Map::new();
        fields1.insert(
            "source".into(),
            serde_json::Value::String("server-A".into()),
        );

        let mut fields2 = serde_json::Map::new();
        fields2.insert(
            "source".into(),
            serde_json::Value::String("server-A".into()),
        );

        // Same key should always route to the same replica
        let r1 = group.select_replica(&fields1);
        let r2 = group.select_replica(&fields2);
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_replica_group_hash_key_missing_field_routes_consistently() {
        let group = ReplicaGroup::new(
            "p1".into(),
            vec!["p1#0".into(), "p1#1".into()],
            PartitionStrategy::HashKey("missing_field".into()),
        );
        let fields = serde_json::Map::new(); // no "missing_field"

        // Missing field → empty string hash → deterministic replica
        let r1 = group.select_replica(&fields);
        let r2 = group.select_replica(&fields);
        assert_eq!(r1, r2);

        // Verify the warning flag was set
        assert!(group
            .missing_field_warned
            .load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_replica_group_hash_key_missing_field_warns_once() {
        let group = ReplicaGroup::new(
            "p1".into(),
            vec!["p1#0".into(), "p1#1".into()],
            PartitionStrategy::HashKey("nonexistent".into()),
        );
        let fields = serde_json::Map::new();

        // First call sets the flag
        assert!(!group
            .missing_field_warned
            .load(std::sync::atomic::Ordering::Relaxed));
        group.select_replica(&fields);
        assert!(group
            .missing_field_warned
            .load(std::sync::atomic::Ordering::Relaxed));

        // Subsequent calls don't change it (already true)
        group.select_replica(&fields);
        assert!(group
            .missing_field_warned
            .load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_replica_group_empty_replicas_returns_pipeline_name() {
        let group = ReplicaGroup::new("p1".into(), vec![], PartitionStrategy::RoundRobin);
        let fields = serde_json::Map::new();
        assert_eq!(group.select_replica(&fields), "p1");
    }

    #[test]
    fn test_replica_group_clone() {
        let group = ReplicaGroup::new(
            "p1".into(),
            vec!["p1#0".into(), "p1#1".into()],
            PartitionStrategy::HashKey("source".into()),
        );

        // Advance counter and set warned flag
        group
            .counter
            .fetch_add(5, std::sync::atomic::Ordering::Relaxed);
        group
            .missing_field_warned
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let cloned = group.clone();
        assert_eq!(cloned.pipeline_name, "p1");
        assert_eq!(cloned.replica_names.len(), 2);
        assert!(cloned
            .missing_field_warned
            .load(std::sync::atomic::Ordering::Relaxed));
        // Counter is shared via Arc
        assert_eq!(cloned.counter.load(std::sync::atomic::Ordering::Relaxed), 5);
    }

    #[test]
    fn test_replica_group_hash_different_keys_may_differ() {
        let group = ReplicaGroup::new(
            "p1".into(),
            vec!["p1#0".into(), "p1#1".into(), "p1#2".into(), "p1#3".into()],
            PartitionStrategy::HashKey("id".into()),
        );

        // Generate events with different keys and verify not all go to same replica
        let mut replicas_seen = std::collections::HashSet::new();
        for i in 0..100 {
            let mut fields = serde_json::Map::new();
            fields.insert("id".into(), serde_json::Value::String(format!("key-{}", i)));
            replicas_seen.insert(group.select_replica(&fields).to_string());
        }
        // With 4 replicas and 100 different keys, we should hit at least 2
        assert!(
            replicas_seen.len() >= 2,
            "Expected multiple replicas hit, got: {:?}",
            replicas_seen
        );
    }

    #[test]
    fn test_pipeline_placement_replicas_default() {
        let json = r#"{
            "name": "p1",
            "source": "stream A = X"
        }"#;
        let spec: PipelinePlacement = serde_json::from_str(json).unwrap();
        assert_eq!(spec.replicas, 1);
        assert!(spec.partition_key.is_none());
    }

    #[test]
    fn test_pipeline_placement_replicas_explicit() {
        let json = r#"{
            "name": "p1",
            "source": "stream A = X",
            "replicas": 3,
            "partition_key": "source_ip"
        }"#;
        let spec: PipelinePlacement = serde_json::from_str(json).unwrap();
        assert_eq!(spec.replicas, 3);
        assert_eq!(spec.partition_key, Some("source_ip".into()));
    }

    #[test]
    fn test_partition_strategy_serde() {
        for strategy in [
            PartitionStrategy::RoundRobin,
            PartitionStrategy::HashKey("source".into()),
        ] {
            let json = serde_json::to_string(&strategy).unwrap();
            let parsed: PartitionStrategy = serde_json::from_str(&json).unwrap();
            match (&strategy, &parsed) {
                (PartitionStrategy::RoundRobin, PartitionStrategy::RoundRobin) => {}
                (PartitionStrategy::HashKey(a), PartitionStrategy::HashKey(b)) => {
                    assert_eq!(a, b);
                }
                _ => panic!("Serde round-trip mismatch"),
            }
        }
    }
}
