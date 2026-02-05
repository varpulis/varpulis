//! Event routing and pattern matching for inter-pipeline communication.

use crate::pipeline_group::{DeployedPipelineGroup, InterPipelineRoute};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Routing table for inter-pipeline event routing.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RoutingTable {
    /// pipeline_name -> Vec<(event_type_pattern, mqtt_topic)>
    pub output_routes: HashMap<String, Vec<(String, String)>>,
    /// pipeline_name -> Vec<(mqtt_topic, event_type_filter)>
    pub input_subscriptions: HashMap<String, Vec<(String, String)>>,
}

/// Match an event type against a pattern (supports trailing wildcard `*`).
///
/// Examples:
///   - `"ComputeTile0*"` matches `"ComputeTile00"`, `"ComputeTile01"`, etc.
///   - `"Exact"` matches only `"Exact"`
///   - `"*"` matches everything
pub fn event_type_matches(event_type: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        event_type.starts_with(prefix)
    } else {
        event_type == pattern
    }
}

/// Find which pipeline should handle a given event type based on routing rules.
pub fn find_target_pipeline<'a>(
    group: &'a DeployedPipelineGroup,
    event_type: &str,
) -> Option<&'a str> {
    // Check explicit routes
    for route in &group.spec.routes {
        for pattern in &route.event_types {
            if event_type_matches(event_type, pattern) {
                return Some(&route.to_pipeline);
            }
        }
    }
    // Default: first pipeline in the group
    group.spec.pipelines.first().map(|p| p.name.as_str())
}

/// Build a routing table from a set of inter-pipeline routes.
pub fn build_routing_table(group_id: &str, routes: &[InterPipelineRoute]) -> RoutingTable {
    let mut table = RoutingTable::default();

    for route in routes {
        let topic = route.mqtt_topic.clone().unwrap_or_else(|| {
            format!(
                "varpulis/cluster/{}/{}/{}",
                group_id, route.from_pipeline, route.to_pipeline
            )
        });

        for pattern in &route.event_types {
            table
                .output_routes
                .entry(route.from_pipeline.clone())
                .or_default()
                .push((pattern.clone(), topic.clone()));

            table
                .input_subscriptions
                .entry(route.to_pipeline.clone())
                .or_default()
                .push((topic.clone(), pattern.clone()));
        }
    }

    table
}

/// Topology view of the entire cluster routing.
#[derive(Debug, Serialize, Deserialize)]
pub struct TopologyInfo {
    pub groups: Vec<GroupTopology>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupTopology {
    pub group_id: String,
    pub group_name: String,
    pub pipelines: Vec<PipelineTopologyEntry>,
    pub routes: Vec<RouteTopologyEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineTopologyEntry {
    pub name: String,
    pub worker_id: String,
    pub worker_address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RouteTopologyEntry {
    pub from_pipeline: String,
    pub to_pipeline: String,
    pub event_types: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_matches_exact() {
        assert!(event_type_matches("ComputeTile00", "ComputeTile00"));
        assert!(!event_type_matches("ComputeTile01", "ComputeTile00"));
    }

    #[test]
    fn test_event_type_matches_wildcard() {
        assert!(event_type_matches("ComputeTile00", "ComputeTile0*"));
        assert!(event_type_matches("ComputeTile01", "ComputeTile0*"));
        assert!(!event_type_matches("ComputeTile10", "ComputeTile0*"));
    }

    #[test]
    fn test_event_type_matches_star() {
        assert!(event_type_matches("Anything", "*"));
        assert!(event_type_matches("", "*"));
    }

    #[test]
    fn test_build_routing_table() {
        use crate::pipeline_group::InterPipelineRoute;

        let routes = vec![
            InterPipelineRoute {
                from_pipeline: "_external".into(),
                to_pipeline: "row0".into(),
                event_types: vec!["ComputeTile0*".into()],
                mqtt_topic: None,
            },
            InterPipelineRoute {
                from_pipeline: "_external".into(),
                to_pipeline: "row1".into(),
                event_types: vec!["ComputeTile1*".into()],
                mqtt_topic: None,
            },
        ];

        let table = build_routing_table("mandelbrot", &routes);
        assert_eq!(table.output_routes.len(), 1); // _external
        assert_eq!(table.input_subscriptions.len(), 2); // row0, row1

        let external_outputs = &table.output_routes["_external"];
        assert_eq!(external_outputs.len(), 2);
    }

    #[test]
    fn test_find_target_pipeline() {
        use crate::pipeline_group::*;

        let spec = PipelineGroupSpec {
            name: "mandelbrot".into(),
            pipelines: vec![
                PipelinePlacement {
                    name: "row0".into(),
                    source: "".into(),
                    worker_affinity: None,
                },
                PipelinePlacement {
                    name: "row1".into(),
                    source: "".into(),
                    worker_affinity: None,
                },
            ],
            routes: vec![
                InterPipelineRoute {
                    from_pipeline: "_external".into(),
                    to_pipeline: "row0".into(),
                    event_types: vec!["ComputeTile0*".into()],
                    mqtt_topic: None,
                },
                InterPipelineRoute {
                    from_pipeline: "_external".into(),
                    to_pipeline: "row1".into(),
                    event_types: vec!["ComputeTile1*".into()],
                    mqtt_topic: None,
                },
            ],
        };

        let group = DeployedPipelineGroup::new("g1".into(), "mandelbrot".into(), spec);
        assert_eq!(find_target_pipeline(&group, "ComputeTile00"), Some("row0"));
        assert_eq!(find_target_pipeline(&group, "ComputeTile12"), Some("row1"));
        // No matching route -> fallback to first pipeline
        assert_eq!(find_target_pipeline(&group, "Unknown"), Some("row0"));
    }
}
