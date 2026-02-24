//! E2E tests for the federation module.
//!
//! Covers health transitions, catalog filtering, routing table wildcards,
//! and status summary edge cases.
//!
//! Run with:
//!   cargo test -p varpulis-cluster --features federation -- federation_tests
//!
//! Note: Health transition tests use thread::sleep to simulate stale heartbeats.
//! chrono::Duration::num_seconds() truncates, so sleeping N+1 seconds ensures
//! the elapsed time is strictly > N.

#![cfg(feature = "federation")]

use varpulis_cluster::{
    CatalogEntry, CrossRegionRoute, FederationConfig, FederationCoordinator,
    FederationRoutingTable, RegionConfig, RegionStatus,
};

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
            RegionConfig {
                name: "ap-south-1".to_string(),
                coordinator_url: "http://south:9100".to_string(),
                nats_url: Some("nats://south:4222".to_string()),
                priority: 30,
            },
        ],
        sync_interval_secs: 1,
        nats_prefix: "varpulis.federation".to_string(),
    }
}

// =============================================================================
// Health transition tests
// =============================================================================

#[test]
fn test_health_degraded_transition() {
    // With sync_interval=1, degraded threshold is >3s.
    // num_seconds() truncates, so we need to sleep 4+ seconds.
    let config = test_config();
    let mut fed = FederationCoordinator::new(config.clone());
    fed.register_region(&config.regions[0]);

    std::thread::sleep(std::time::Duration::from_millis(4_200));

    fed.check_health();
    let state = fed.get_region("us-east-1").unwrap();
    assert_eq!(
        state.status,
        RegionStatus::Degraded,
        "Region should be Degraded after >3x sync_interval without heartbeat"
    );
}

#[test]
fn test_health_offline_transition() {
    // With sync_interval=1, offline threshold is >10s.
    // Need to sleep 11+ seconds.
    let config = test_config();
    let mut fed = FederationCoordinator::new(config.clone());
    fed.register_region(&config.regions[0]);

    std::thread::sleep(std::time::Duration::from_millis(11_200));

    fed.check_health();
    let state = fed.get_region("us-east-1").unwrap();
    assert_eq!(
        state.status,
        RegionStatus::Offline,
        "Region should be Offline after >10x sync_interval without heartbeat"
    );
}

#[test]
fn test_health_recovery() {
    let config = test_config();
    let mut fed = FederationCoordinator::new(config.clone());
    fed.register_region(&config.regions[0]);

    // Wait past offline threshold
    std::thread::sleep(std::time::Duration::from_millis(11_200));
    fed.check_health();
    assert_eq!(
        fed.get_region("us-east-1").unwrap().status,
        RegionStatus::Offline
    );

    // Heartbeat arrives → recovery to Online
    fed.heartbeat("us-east-1", 3);
    let state = fed.get_region("us-east-1").unwrap();
    assert_eq!(
        state.status,
        RegionStatus::Online,
        "Region should recover to Online after heartbeat"
    );
    assert_eq!(state.worker_count, 3);
}

// =============================================================================
// Catalog filtering tests
// =============================================================================

#[test]
fn test_catalog_excludes_offline() {
    let config = test_config();
    let mut fed = FederationCoordinator::new(config.clone());

    fed.register_region(&config.regions[0]); // us-east-1
    fed.register_region(&config.regions[1]); // eu-west-1

    fed.sync_catalog(
        "us-east-1",
        vec![CatalogEntry {
            pipeline_name: "fraud".to_string(),
            group_name: "fraud-group".to_string(),
            event_types: vec!["Transaction".to_string()],
        }],
    );
    fed.sync_catalog(
        "eu-west-1",
        vec![CatalogEntry {
            pipeline_name: "alerts".to_string(),
            group_name: "alert-group".to_string(),
            event_types: vec!["Alert".to_string()],
        }],
    );

    // Both should appear initially
    assert_eq!(fed.get_global_catalog().len(), 2);

    // Wait for eu-west-1 to go offline (but keep us-east-1 alive)
    std::thread::sleep(std::time::Duration::from_millis(11_200));
    fed.heartbeat("us-east-1", 2); // Keep us-east-1 alive
    fed.check_health();

    let catalog = fed.get_global_catalog();
    assert_eq!(
        catalog.len(),
        1,
        "Only online region's catalog should be included"
    );
    assert_eq!(catalog[0].0, "us-east-1");
    assert_eq!(catalog[0].1.pipeline_name, "fraud");
}

// =============================================================================
// Routing table tests
// =============================================================================

#[test]
fn test_routing_table_wildcard_star() {
    let mut table = FederationRoutingTable::new();
    table.add_route(CrossRegionRoute::new(
        "us-east",
        "eu-west",
        vec!["*".to_string()],
    ));

    let routes = table.find_routes("us-east", "Transaction");
    assert_eq!(routes.len(), 1, "'*' should match any event type");

    let routes = table.find_routes("us-east", "Alert");
    assert_eq!(routes.len(), 1, "'*' should match Alert too");

    let routes = table.find_routes("us-east", "AnythingAtAll");
    assert_eq!(routes.len(), 1, "'*' should match arbitrary event type");
}

#[test]
fn test_routing_table_prefix_wildcard() {
    let mut table = FederationRoutingTable::new();
    table.add_route(CrossRegionRoute::new(
        "us-east",
        "eu-west",
        vec!["Transaction*".to_string()],
    ));

    let routes = table.find_routes("us-east", "TransactionStart");
    assert_eq!(
        routes.len(),
        1,
        "'Transaction*' should match 'TransactionStart'"
    );

    let routes = table.find_routes("us-east", "TransactionComplete");
    assert_eq!(
        routes.len(),
        1,
        "'Transaction*' should match 'TransactionComplete'"
    );

    let routes = table.find_routes("us-east", "LoginEvent");
    assert!(
        routes.is_empty(),
        "'Transaction*' should NOT match 'LoginEvent'"
    );
}

// =============================================================================
// Status summary tests
// =============================================================================

#[test]
fn test_federation_status_counts() {
    // Test with 3 regions, keep one alive, let others degrade.
    // Degraded threshold: >3s. Sleep 4.2s.
    let config = test_config();
    let mut fed = FederationCoordinator::new(config.clone());

    fed.register_region(&config.regions[0]); // us-east-1 — will stay online
    fed.register_region(&config.regions[1]); // eu-west-1 — will go degraded
    fed.register_region(&config.regions[2]); // ap-south-1 — will go degraded

    std::thread::sleep(std::time::Duration::from_millis(4_200));
    fed.heartbeat("us-east-1", 5);
    fed.check_health();

    let status = fed.status();
    assert_eq!(status.total_regions, 3);
    assert_eq!(status.online_regions, 1, "Only us-east-1 should be online");
    assert_eq!(
        status.degraded_regions, 2,
        "eu-west-1 and ap-south-1 should be degraded"
    );
    assert_eq!(status.offline_regions, 0, "None should be offline yet");
}

// =============================================================================
// Local region tests
// =============================================================================

#[test]
fn test_local_region_set() {
    let config = test_config();
    let mut fed = FederationCoordinator::new(config);
    fed.set_local_region("us-east-1");

    // Verify the coordinator still works after setting local region
    assert!(fed.get_regions().is_empty(), "No regions registered yet");
}

// =============================================================================
// Deregister edge cases
// =============================================================================

#[test]
fn test_deregister_nonexistent_returns_false() {
    let config = test_config();
    let mut fed = FederationCoordinator::new(config);
    assert!(
        !fed.deregister_region("nonexistent"),
        "Deregistering unknown region should return false"
    );
}

// =============================================================================
// Routing table removal tests
// =============================================================================

#[test]
fn test_routing_remove_cleans_both_directions() {
    let mut table = FederationRoutingTable::new();

    // A→B route
    table.add_route(CrossRegionRoute::new(
        "region-a",
        "region-b",
        vec!["Transaction".to_string()],
    ));

    // C→A route
    table.add_route(CrossRegionRoute::new(
        "region-c",
        "region-a",
        vec!["Alert".to_string()],
    ));

    assert_eq!(table.route_count(), 2);

    // Remove region-a — should clean both A→B and C→A
    table.remove_region("region-a");

    assert!(
        table.find_routes("region-a", "Transaction").is_empty(),
        "Routes FROM region-a should be removed"
    );
    assert!(
        table.routes_to("region-a").is_empty(),
        "Routes TO region-a should be removed"
    );
    assert!(
        table.find_routes("region-c", "Alert").is_empty(),
        "C→A route should also be removed"
    );
    assert_eq!(table.route_count(), 0, "All routes should be gone");
}
