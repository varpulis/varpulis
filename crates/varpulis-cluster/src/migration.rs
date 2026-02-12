//! Pipeline migration types and status tracking.

use crate::worker::WorkerId;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use varpulis_runtime::persistence::EngineCheckpoint;

/// Status of a pipeline migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatus {
    /// Extracting state from source worker
    Checkpointing,
    /// Deploying pipeline to target worker
    Deploying,
    /// Restoring state on target worker
    Restoring,
    /// Updating routing to point to target
    Switching,
    /// Removing pipeline from source worker
    CleaningUp,
    /// Migration completed successfully
    Completed,
    /// Migration failed
    Failed(String),
}

impl std::fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Checkpointing => write!(f, "checkpointing"),
            Self::Deploying => write!(f, "deploying"),
            Self::Restoring => write!(f, "restoring"),
            Self::Switching => write!(f, "switching"),
            Self::CleaningUp => write!(f, "cleaning_up"),
            Self::Completed => write!(f, "completed"),
            Self::Failed(msg) => write!(f, "failed: {}", msg),
        }
    }
}

/// Reason for initiating a migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationReason {
    /// Source worker died
    Failover,
    /// Load rebalancing
    Rebalance,
    /// Source worker is draining
    Drain,
    /// Operator-initiated
    Manual,
    /// Connector lost connectivity
    ConnectorFailure,
}

impl std::fmt::Display for MigrationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failover => write!(f, "failover"),
            Self::Rebalance => write!(f, "rebalance"),
            Self::Drain => write!(f, "drain"),
            Self::Manual => write!(f, "manual"),
            Self::ConnectorFailure => write!(f, "connector_failure"),
        }
    }
}

/// Tracks an in-progress pipeline migration.
#[derive(Debug, Clone)]
pub struct MigrationTask {
    pub id: String,
    pub pipeline_name: String,
    pub group_id: String,
    pub source_worker: WorkerId,
    pub target_worker: WorkerId,
    pub status: MigrationStatus,
    pub started_at: Instant,
    pub checkpoint: Option<EngineCheckpoint>,
    pub reason: MigrationReason,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_status_display() {
        assert_eq!(MigrationStatus::Checkpointing.to_string(), "checkpointing");
        assert_eq!(MigrationStatus::Deploying.to_string(), "deploying");
        assert_eq!(MigrationStatus::Restoring.to_string(), "restoring");
        assert_eq!(MigrationStatus::Switching.to_string(), "switching");
        assert_eq!(MigrationStatus::CleaningUp.to_string(), "cleaning_up");
        assert_eq!(MigrationStatus::Completed.to_string(), "completed");
        assert_eq!(
            MigrationStatus::Failed("timeout".into()).to_string(),
            "failed: timeout"
        );
    }

    #[test]
    fn test_migration_reason_display() {
        assert_eq!(MigrationReason::Failover.to_string(), "failover");
        assert_eq!(MigrationReason::Rebalance.to_string(), "rebalance");
        assert_eq!(MigrationReason::Drain.to_string(), "drain");
        assert_eq!(MigrationReason::Manual.to_string(), "manual");
        assert_eq!(
            MigrationReason::ConnectorFailure.to_string(),
            "connector_failure"
        );
    }

    #[test]
    fn test_migration_status_serde() {
        for status in [
            MigrationStatus::Checkpointing,
            MigrationStatus::Deploying,
            MigrationStatus::Restoring,
            MigrationStatus::Switching,
            MigrationStatus::CleaningUp,
            MigrationStatus::Completed,
            MigrationStatus::Failed("err".into()),
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let parsed: MigrationStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_migration_reason_serde() {
        for reason in [
            MigrationReason::Failover,
            MigrationReason::Rebalance,
            MigrationReason::Drain,
            MigrationReason::Manual,
            MigrationReason::ConnectorFailure,
        ] {
            let json = serde_json::to_string(&reason).unwrap();
            let _parsed: MigrationReason = serde_json::from_str(&json).unwrap();
        }
    }
}
