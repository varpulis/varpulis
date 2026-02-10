//! Pipeline group abstraction for deploying related pipelines together.

use crate::worker::WorkerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

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
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "stream B = Y".into(),
                    worker_affinity: None,
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
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "".into(),
                    worker_affinity: None,
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
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "".into(),
                    worker_affinity: None,
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
                },
                PipelinePlacement {
                    name: "p2".into(),
                    source: "stream B = Y".into(),
                    worker_affinity: None,
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
}
