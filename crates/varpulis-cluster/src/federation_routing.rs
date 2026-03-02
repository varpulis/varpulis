//! Cross-region event routing for federated Varpulis clusters.
//!
//! Provides routing tables and logic for forwarding events between regions
//! via NATS super-cluster subjects.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A route definition for cross-region event forwarding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossRegionRoute {
    /// Source region name.
    pub from_region: String,
    /// Destination region name.
    pub to_region: String,
    /// Event types to forward (supports wildcards: "Transaction*").
    pub event_types: Vec<String>,
    /// NATS subject for this route (auto-generated if not set).
    pub nats_subject: Option<String>,
}

impl CrossRegionRoute {
    /// Create a new cross-region route.
    pub fn new(from: &str, to: &str, event_types: Vec<String>) -> Self {
        Self {
            from_region: from.to_string(),
            to_region: to.to_string(),
            event_types,
            nats_subject: None,
        }
    }

    /// Set a custom NATS subject.
    pub fn with_subject(mut self, subject: &str) -> Self {
        self.nats_subject = Some(subject.to_string());
        self
    }

    /// Get the NATS subject for this route, generating a default if not set.
    pub fn subject(&self, prefix: &str) -> String {
        self.nats_subject
            .clone()
            .unwrap_or_else(|| format!("{}.route.{}.{}", prefix, self.from_region, self.to_region))
    }
}

/// Routing table for cross-region event routing.
#[derive(Debug, Clone, Default)]
pub struct FederationRoutingTable {
    /// Routes indexed by source region.
    routes_by_source: HashMap<String, Vec<CrossRegionRoute>>,
    /// Routes indexed by destination region.
    routes_by_dest: HashMap<String, Vec<CrossRegionRoute>>,
}

impl FederationRoutingTable {
    /// Create a new empty routing table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a cross-region route.
    pub fn add_route(&mut self, route: CrossRegionRoute) {
        self.routes_by_source
            .entry(route.from_region.clone())
            .or_default()
            .push(route.clone());
        self.routes_by_dest
            .entry(route.to_region.clone())
            .or_default()
            .push(route);
    }

    /// Remove all routes for a region (both as source and destination).
    pub fn remove_region(&mut self, region: &str) {
        self.routes_by_source.remove(region);
        self.routes_by_dest.remove(region);
        // Also remove routes in other regions that reference this region
        for routes in self.routes_by_source.values_mut() {
            routes.retain(|r| r.to_region != region);
        }
        for routes in self.routes_by_dest.values_mut() {
            routes.retain(|r| r.from_region != region);
        }
    }

    /// Find routes for an event type from a given source region.
    pub fn find_routes(&self, from_region: &str, event_type: &str) -> Vec<&CrossRegionRoute> {
        self.routes_by_source
            .get(from_region)
            .map(|routes| {
                routes
                    .iter()
                    .filter(|r| {
                        r.event_types
                            .iter()
                            .any(|pattern| event_type_matches(event_type, pattern))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all routes from a specific source region.
    pub fn routes_from(&self, region: &str) -> &[CrossRegionRoute] {
        self.routes_by_source
            .get(region)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get all routes to a specific destination region.
    pub fn routes_to(&self, region: &str) -> &[CrossRegionRoute] {
        self.routes_by_dest
            .get(region)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Total number of routes.
    pub fn route_count(&self) -> usize {
        self.routes_by_source.values().map(|v| v.len()).sum()
    }
}

/// Check if an event type matches a pattern (supports trailing '*' wildcard).
fn event_type_matches(event_type: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        event_type.starts_with(prefix)
    } else {
        event_type == pattern
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cross_region_route_subject() {
        let route = CrossRegionRoute::new("us-east", "eu-west", vec!["Transaction".to_string()]);
        assert_eq!(
            route.subject("varpulis.federation"),
            "varpulis.federation.route.us-east.eu-west"
        );

        let route = route.with_subject("custom.subject");
        assert_eq!(route.subject("varpulis.federation"), "custom.subject");
    }

    #[test]
    fn test_routing_table_add_and_find() {
        let mut table = FederationRoutingTable::new();

        table.add_route(CrossRegionRoute::new(
            "us-east",
            "eu-west",
            vec!["Transaction".to_string(), "Alert*".to_string()],
        ));
        table.add_route(CrossRegionRoute::new(
            "us-east",
            "ap-south",
            vec!["*".to_string()],
        ));

        // Exact match
        let routes = table.find_routes("us-east", "Transaction");
        assert_eq!(routes.len(), 2); // matches both routes

        // Wildcard match
        let routes = table.find_routes("us-east", "AlertFired");
        assert_eq!(routes.len(), 2); // Alert* + *

        // No routes from unknown region
        let routes = table.find_routes("unknown", "Transaction");
        assert!(routes.is_empty());
    }

    #[test]
    fn test_routing_table_remove_region() {
        let mut table = FederationRoutingTable::new();

        table.add_route(CrossRegionRoute::new(
            "us-east",
            "eu-west",
            vec!["Transaction".to_string()],
        ));
        table.add_route(CrossRegionRoute::new(
            "eu-west",
            "us-east",
            vec!["Alert".to_string()],
        ));

        assert_eq!(table.route_count(), 2);

        table.remove_region("eu-west");

        // Route from us-east to eu-west should be gone
        assert!(table.find_routes("us-east", "Transaction").is_empty());
        // Route from eu-west should be gone
        assert!(table.routes_from("eu-west").is_empty());
    }

    #[test]
    fn test_event_type_matches() {
        assert!(event_type_matches("Transaction", "Transaction"));
        assert!(event_type_matches("Transaction", "*"));
        assert!(event_type_matches("TransactionCreated", "Transaction*"));
        assert!(!event_type_matches("Alert", "Transaction"));
        assert!(!event_type_matches("Alert", "Transaction*"));
    }
}
