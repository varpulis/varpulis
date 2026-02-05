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
        Self {
            id: group.id.clone(),
            name: group.name.clone(),
            status: group.status.to_string(),
            pipeline_count: group.spec.pipelines.len(),
            placements,
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
}
