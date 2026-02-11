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
                    replicas: 1,
                    partition_key: None,
                },
                PipelinePlacement {
                    name: "row1".into(),
                    source: "".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
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

    #[test]
    fn test_event_type_matches_empty_pattern() {
        assert!(event_type_matches("", ""));
        assert!(!event_type_matches("Something", ""));
    }

    #[test]
    fn test_event_type_matches_empty_event_type() {
        assert!(!event_type_matches("", "Pattern"));
        assert!(event_type_matches("", "*"));
        // Empty prefix wildcard matches empty string
        assert!(event_type_matches("", "*"));
    }

    #[test]
    fn test_event_type_matches_prefix_only_wildcard() {
        // Pattern "A*" should match "A", "AB", "ABC"
        assert!(event_type_matches("A", "A*"));
        assert!(event_type_matches("AB", "A*"));
        assert!(event_type_matches("ABC", "A*"));
        assert!(!event_type_matches("B", "A*"));
    }

    #[test]
    fn test_event_type_matches_case_sensitive() {
        assert!(!event_type_matches("computetile00", "ComputeTile0*"));
        assert!(!event_type_matches("COMPUTETILE00", "ComputeTile0*"));
    }

    #[test]
    fn test_event_type_matches_embedded_star() {
        // Stars only work as trailing wildcard — embedded star is literal
        assert!(!event_type_matches("AstarB", "A*B"));
        // "A*B" has no trailing *, so it's an exact match
        assert!(event_type_matches("A*B", "A*B"));
    }

    #[test]
    fn test_find_target_pipeline_empty_routes() {
        use crate::pipeline_group::*;

        let spec = PipelineGroupSpec {
            name: "test".into(),
            pipelines: vec![PipelinePlacement {
                name: "default".into(),
                source: "".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            }],
            routes: vec![],
        };

        let group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
        // No routes, falls back to first pipeline
        assert_eq!(find_target_pipeline(&group, "AnyEvent"), Some("default"));
    }

    #[test]
    fn test_find_target_pipeline_no_pipelines_no_routes() {
        use crate::pipeline_group::*;

        let spec = PipelineGroupSpec {
            name: "empty".into(),
            pipelines: vec![],
            routes: vec![],
        };

        let group = DeployedPipelineGroup::new("g1".into(), "empty".into(), spec);
        assert_eq!(find_target_pipeline(&group, "AnyEvent"), None);
    }

    #[test]
    fn test_find_target_pipeline_first_match_wins() {
        use crate::pipeline_group::*;

        let spec = PipelineGroupSpec {
            name: "overlap".into(),
            pipelines: vec![
                PipelinePlacement {
                    name: "specific".into(),
                    source: "".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
                PipelinePlacement {
                    name: "catchall".into(),
                    source: "".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                },
            ],
            routes: vec![
                InterPipelineRoute {
                    from_pipeline: "_external".into(),
                    to_pipeline: "specific".into(),
                    event_types: vec!["Temperature*".into()],
                    mqtt_topic: None,
                },
                InterPipelineRoute {
                    from_pipeline: "_external".into(),
                    to_pipeline: "catchall".into(),
                    event_types: vec!["*".into()],
                    mqtt_topic: None,
                },
            ],
        };

        let group = DeployedPipelineGroup::new("g1".into(), "overlap".into(), spec);
        // "Temperature*" matches first
        assert_eq!(
            find_target_pipeline(&group, "TemperatureReading"),
            Some("specific")
        );
        // Catch-all gets everything else
        assert_eq!(
            find_target_pipeline(&group, "HumidityReading"),
            Some("catchall")
        );
    }

    #[test]
    fn test_find_target_pipeline_multiple_event_types_per_route() {
        use crate::pipeline_group::*;

        let spec = PipelineGroupSpec {
            name: "multi".into(),
            pipelines: vec![PipelinePlacement {
                name: "sensors".into(),
                source: "".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            }],
            routes: vec![InterPipelineRoute {
                from_pipeline: "_external".into(),
                to_pipeline: "sensors".into(),
                event_types: vec![
                    "Temperature*".into(),
                    "Humidity*".into(),
                    "Pressure*".into(),
                ],
                mqtt_topic: None,
            }],
        };

        let group = DeployedPipelineGroup::new("g1".into(), "multi".into(), spec);
        assert_eq!(
            find_target_pipeline(&group, "TemperatureHigh"),
            Some("sensors")
        );
        assert_eq!(find_target_pipeline(&group, "HumidityLow"), Some("sensors"));
        assert_eq!(
            find_target_pipeline(&group, "PressureNormal"),
            Some("sensors")
        );
    }

    #[test]
    fn test_build_routing_table_custom_topic() {
        let routes = vec![InterPipelineRoute {
            from_pipeline: "ingress".into(),
            to_pipeline: "analytics".into(),
            event_types: vec!["SensorData".into()],
            mqtt_topic: Some("custom/topic/sensor".into()),
        }];

        let table = build_routing_table("grp1", &routes);
        let outputs = &table.output_routes["ingress"];
        assert_eq!(outputs[0].1, "custom/topic/sensor");

        let inputs = &table.input_subscriptions["analytics"];
        assert_eq!(inputs[0].0, "custom/topic/sensor");
    }

    #[test]
    fn test_build_routing_table_auto_topic() {
        let routes = vec![InterPipelineRoute {
            from_pipeline: "ingress".into(),
            to_pipeline: "analytics".into(),
            event_types: vec!["SensorData".into()],
            mqtt_topic: None,
        }];

        let table = build_routing_table("grp1", &routes);
        let outputs = &table.output_routes["ingress"];
        assert_eq!(outputs[0].1, "varpulis/cluster/grp1/ingress/analytics");
    }

    #[test]
    fn test_build_routing_table_empty_routes() {
        let table = build_routing_table("grp1", &[]);
        assert!(table.output_routes.is_empty());
        assert!(table.input_subscriptions.is_empty());
    }

    #[test]
    fn test_build_routing_table_multiple_event_types() {
        let routes = vec![InterPipelineRoute {
            from_pipeline: "src".into(),
            to_pipeline: "dst".into(),
            event_types: vec!["TypeA".into(), "TypeB".into(), "TypeC*".into()],
            mqtt_topic: None,
        }];

        let table = build_routing_table("grp1", &routes);
        let outputs = &table.output_routes["src"];
        assert_eq!(outputs.len(), 3);

        let inputs = &table.input_subscriptions["dst"];
        assert_eq!(inputs.len(), 3);
    }

    #[test]
    fn test_routing_table_serde() {
        let routes = vec![InterPipelineRoute {
            from_pipeline: "a".into(),
            to_pipeline: "b".into(),
            event_types: vec!["Event*".into()],
            mqtt_topic: None,
        }];

        let table = build_routing_table("grp1", &routes);
        let json = serde_json::to_string(&table).unwrap();
        let parsed: RoutingTable = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.output_routes.len(), table.output_routes.len());
        assert_eq!(
            parsed.input_subscriptions.len(),
            table.input_subscriptions.len()
        );
    }

    #[test]
    fn test_topology_info_serde() {
        let topology = TopologyInfo {
            groups: vec![GroupTopology {
                group_id: "g1".into(),
                group_name: "test".into(),
                pipelines: vec![PipelineTopologyEntry {
                    name: "p1".into(),
                    worker_id: "w1".into(),
                    worker_address: "http://localhost:9000".into(),
                }],
                routes: vec![RouteTopologyEntry {
                    from_pipeline: "_external".into(),
                    to_pipeline: "p1".into(),
                    event_types: vec!["*".into()],
                }],
            }],
        };

        let json = serde_json::to_string(&topology).unwrap();
        let parsed: TopologyInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.groups.len(), 1);
        assert_eq!(parsed.groups[0].group_id, "g1");
        assert_eq!(parsed.groups[0].pipelines.len(), 1);
        assert_eq!(parsed.groups[0].routes.len(), 1);
    }
}
