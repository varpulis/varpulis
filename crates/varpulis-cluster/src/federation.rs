//! Multi-region federation for Varpulis clusters.
//!
//! Each region runs its own coordinator cluster. The federation layer syncs
//! region health and pipeline catalogs via gossip. Cross-region event routing
//! uses NATS super-cluster subjects.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, warn};

/// Configuration for the federation layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationConfig {
    /// Regions participating in federation.
    pub regions: Vec<RegionConfig>,
    /// Interval between catalog/health sync in seconds.
    #[serde(default = "default_sync_interval")]
    pub sync_interval_secs: u64,
    /// NATS subject prefix for federation messages.
    #[serde(default = "default_nats_prefix")]
    pub nats_prefix: String,
}

fn default_sync_interval() -> u64 {
    30
}
fn default_nats_prefix() -> String {
    "varpulis.federation".to_string()
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            regions: Vec::new(),
            sync_interval_secs: default_sync_interval(),
            nats_prefix: default_nats_prefix(),
        }
    }
}

/// Configuration for a single region in the federation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionConfig {
    /// Unique region identifier (e.g., "us-east-1").
    pub name: String,
    /// Coordinator URL of the remote region.
    pub coordinator_url: String,
    /// NATS URL for the region (for cross-region routing).
    pub nats_url: Option<String>,
    /// Priority for failover (lower = higher priority, default 100).
    #[serde(default = "default_priority")]
    pub priority: u32,
}

fn default_priority() -> u32 {
    100
}

/// Status of a region in the federation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionStatus {
    Online,
    Degraded,
    Offline,
}

impl std::fmt::Display for RegionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegionStatus::Online => write!(f, "online"),
            RegionStatus::Degraded => write!(f, "degraded"),
            RegionStatus::Offline => write!(f, "offline"),
        }
    }
}

/// Pipeline catalog entry — describes a pipeline deployed in a region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub pipeline_name: String,
    pub group_name: String,
    pub event_types: Vec<String>,
}

/// State of a region as seen by the federation coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionState {
    pub name: String,
    pub status: RegionStatus,
    pub pipeline_catalog: Vec<CatalogEntry>,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
    pub worker_count: usize,
    pub coordinator_url: String,
}

impl RegionState {
    fn new(name: &str, coordinator_url: &str) -> Self {
        Self {
            name: name.to_string(),
            status: RegionStatus::Online,
            pipeline_catalog: Vec::new(),
            last_heartbeat: chrono::Utc::now(),
            worker_count: 0,
            coordinator_url: coordinator_url.to_string(),
        }
    }
}

/// Federation coordinator that manages cross-region state.
#[derive(Debug)]
pub struct FederationCoordinator {
    config: FederationConfig,
    regions: HashMap<String, RegionState>,
    local_region: Option<String>,
}

impl FederationCoordinator {
    /// Create a new federation coordinator.
    pub fn new(config: FederationConfig) -> Self {
        Self {
            config,
            regions: HashMap::new(),
            local_region: None,
        }
    }

    /// Set the local region name.
    pub fn set_local_region(&mut self, name: &str) {
        self.local_region = Some(name.to_string());
    }

    /// Register a region in the federation.
    pub fn register_region(&mut self, config: &RegionConfig) {
        let state = RegionState::new(&config.name, &config.coordinator_url);
        info!("Federation: registered region '{}'", config.name);
        self.regions.insert(config.name.clone(), state);
    }

    /// Deregister a region from the federation.
    pub fn deregister_region(&mut self, name: &str) -> bool {
        if self.regions.remove(name).is_some() {
            info!("Federation: deregistered region '{}'", name);
            true
        } else {
            warn!("Federation: region '{}' not found for deregistration", name);
            false
        }
    }

    /// Update heartbeat for a region.
    pub fn heartbeat(&mut self, region_name: &str, worker_count: usize) {
        if let Some(state) = self.regions.get_mut(region_name) {
            state.last_heartbeat = chrono::Utc::now();
            state.worker_count = worker_count;
            state.status = RegionStatus::Online;
        }
    }

    /// Sync pipeline catalog from a remote region.
    pub fn sync_catalog(&mut self, region_name: &str, catalog: Vec<CatalogEntry>) {
        if let Some(state) = self.regions.get_mut(region_name) {
            state.pipeline_catalog = catalog;
            state.last_heartbeat = chrono::Utc::now();
        }
    }

    /// Mark stale regions as degraded/offline.
    pub fn check_health(&mut self) {
        let now = chrono::Utc::now();
        let timeout_secs = (self.config.sync_interval_secs * 3) as i64;
        let offline_secs = (self.config.sync_interval_secs * 10) as i64;

        for state in self.regions.values_mut() {
            let elapsed = (now - state.last_heartbeat).num_seconds();
            if elapsed > offline_secs {
                if state.status != RegionStatus::Offline {
                    warn!(
                        "Federation: region '{}' marked offline ({}s since heartbeat)",
                        state.name, elapsed
                    );
                    state.status = RegionStatus::Offline;
                }
            } else if elapsed > timeout_secs && state.status == RegionStatus::Online {
                warn!(
                    "Federation: region '{}' marked degraded ({}s since heartbeat)",
                    state.name, elapsed
                );
                state.status = RegionStatus::Degraded;
            }
        }
    }

    /// Get the global pipeline catalog across all regions.
    pub fn get_global_catalog(&self) -> Vec<(String, CatalogEntry)> {
        let mut catalog = Vec::new();
        for (region_name, state) in &self.regions {
            if state.status != RegionStatus::Offline {
                for entry in &state.pipeline_catalog {
                    catalog.push((region_name.clone(), entry.clone()));
                }
            }
        }
        catalog
    }

    /// Get all region states.
    pub fn get_regions(&self) -> &HashMap<String, RegionState> {
        &self.regions
    }

    /// Get status of a specific region.
    pub fn get_region(&self, name: &str) -> Option<&RegionState> {
        self.regions.get(name)
    }

    /// Get the federation config.
    pub fn config(&self) -> &FederationConfig {
        &self.config
    }

    /// Get the number of online regions.
    pub fn online_region_count(&self) -> usize {
        self.regions
            .values()
            .filter(|s| s.status == RegionStatus::Online)
            .count()
    }
}

/// Summary of federation status for API responses.
#[derive(Debug, Serialize, Deserialize)]
pub struct FederationStatus {
    pub total_regions: usize,
    pub online_regions: usize,
    pub degraded_regions: usize,
    pub offline_regions: usize,
    pub total_pipelines: usize,
    pub regions: Vec<RegionSummary>,
}

/// Per-region summary in the federation status.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegionSummary {
    pub name: String,
    pub status: RegionStatus,
    pub worker_count: usize,
    pub pipeline_count: usize,
    pub last_heartbeat: String,
    pub coordinator_url: String,
}

impl FederationCoordinator {
    /// Build a federation status summary.
    pub fn status(&self) -> FederationStatus {
        let regions: Vec<RegionSummary> = self
            .regions
            .values()
            .map(|s| RegionSummary {
                name: s.name.clone(),
                status: s.status,
                worker_count: s.worker_count,
                pipeline_count: s.pipeline_catalog.len(),
                last_heartbeat: s.last_heartbeat.to_rfc3339(),
                coordinator_url: s.coordinator_url.clone(),
            })
            .collect();

        FederationStatus {
            total_regions: regions.len(),
            online_regions: regions
                .iter()
                .filter(|r| r.status == RegionStatus::Online)
                .count(),
            degraded_regions: regions
                .iter()
                .filter(|r| r.status == RegionStatus::Degraded)
                .count(),
            offline_regions: regions
                .iter()
                .filter(|r| r.status == RegionStatus::Offline)
                .count(),
            total_pipelines: regions.iter().map(|r| r.pipeline_count).sum(),
            regions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> FederationConfig {
        FederationConfig {
            regions: vec![
                RegionConfig {
                    name: "us-east-1".to_string(),
                    coordinator_url: "http://east:9100".to_string(),
                    nats_url: Some("nats://east:4222".to_string()),
                    priority: 10,
                },
                RegionConfig {
                    name: "eu-west-1".to_string(),
                    coordinator_url: "http://west:9100".to_string(),
                    nats_url: Some("nats://west:4222".to_string()),
                    priority: 20,
                },
            ],
            sync_interval_secs: 30,
            nats_prefix: "varpulis.federation".to_string(),
        }
    }

    #[test]
    fn test_register_and_deregister() {
        let config = test_config();
        let mut fed = FederationCoordinator::new(config.clone());

        fed.register_region(&config.regions[0]);
        fed.register_region(&config.regions[1]);

        assert_eq!(fed.get_regions().len(), 2);
        assert!(fed.get_region("us-east-1").is_some());
        assert!(fed.get_region("eu-west-1").is_some());

        assert!(fed.deregister_region("us-east-1"));
        assert_eq!(fed.get_regions().len(), 1);
        assert!(!fed.deregister_region("nonexistent"));
    }

    #[test]
    fn test_heartbeat_and_health() {
        let config = FederationConfig {
            sync_interval_secs: 1,
            ..test_config()
        };
        let mut fed = FederationCoordinator::new(config.clone());

        fed.register_region(&config.regions[0]);
        fed.heartbeat("us-east-1", 5);

        let state = fed.get_region("us-east-1").unwrap();
        assert_eq!(state.status, RegionStatus::Online);
        assert_eq!(state.worker_count, 5);

        // Fresh heartbeat — should stay online
        fed.check_health();
        assert_eq!(
            fed.get_region("us-east-1").unwrap().status,
            RegionStatus::Online
        );
    }

    #[test]
    fn test_catalog_sync() {
        let config = test_config();
        let mut fed = FederationCoordinator::new(config.clone());

        fed.register_region(&config.regions[0]);
        fed.sync_catalog(
            "us-east-1",
            vec![CatalogEntry {
                pipeline_name: "fraud-detect".to_string(),
                group_name: "fraud".to_string(),
                event_types: vec!["Transaction".to_string()],
            }],
        );

        let catalog = fed.get_global_catalog();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].0, "us-east-1");
        assert_eq!(catalog[0].1.pipeline_name, "fraud-detect");
    }

    #[test]
    fn test_status_summary() {
        let config = test_config();
        let mut fed = FederationCoordinator::new(config.clone());

        fed.register_region(&config.regions[0]);
        fed.register_region(&config.regions[1]);
        fed.heartbeat("us-east-1", 3);

        let status = fed.status();
        assert_eq!(status.total_regions, 2);
        assert_eq!(status.online_regions, 2);
    }

    #[test]
    fn test_online_region_count() {
        let config = test_config();
        let mut fed = FederationCoordinator::new(config.clone());

        fed.register_region(&config.regions[0]);
        fed.register_region(&config.regions[1]);

        assert_eq!(fed.online_region_count(), 2);
    }
}
