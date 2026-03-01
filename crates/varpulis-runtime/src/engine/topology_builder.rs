//! Builder pattern for constructing a [`Topology`] from engine state.
//!
//! ```rust,ignore
//! let topology = TopologyBuilder::new()
//!     .add_stream("Alerts", &stream_def)
//!     .add_routes(router.all_routes())
//!     .build();
//! ```

use super::topology::{EdgeType, NodeMetrics, NodeType, Topology, TopologyEdge, TopologyNode};
use super::types::{RuntimeSource, StreamDefinition};
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;

/// Builder for constructing a [`Topology`] from engine components.
#[derive(Debug)]
pub struct TopologyBuilder {
    /// Stream definitions indexed by name
    streams: Vec<(String, StreamInfo)>,
    /// Event routing table
    routes: Vec<(String, Vec<String>)>,
}

#[derive(Debug)]
struct StreamInfo {
    source: SourceKind,
    operation_count: usize,
    operation_summary: String,
}

#[derive(Debug)]
enum SourceKind {
    EventType(String),
    Stream(String),
    Join(Vec<String>),
    Merge(Vec<String>),
    Timer,
}

impl TopologyBuilder {
    /// Create a new empty builder.
    pub const fn new() -> Self {
        Self {
            streams: Vec::new(),
            routes: Vec::new(),
        }
    }

    /// Add a stream definition to the builder.
    pub(crate) fn add_stream(mut self, name: &str, def: &StreamDefinition) -> Self {
        let source = match &def.source {
            RuntimeSource::EventType(t) => SourceKind::EventType(t.clone()),
            RuntimeSource::Stream(s) => SourceKind::Stream(s.clone()),
            RuntimeSource::Join(sources) => SourceKind::Join(sources.clone()),
            RuntimeSource::Merge(sources) => {
                SourceKind::Merge(sources.iter().map(|s| s.event_type.clone()).collect())
            }
            RuntimeSource::Timer(_) => SourceKind::Timer,
        };

        let op_summary = def
            .operations
            .iter()
            .map(|op| op.summary_name())
            .collect::<Vec<_>>()
            .join(" → ");

        self.streams.push((
            name.to_string(),
            StreamInfo {
                source,
                operation_count: def.operations.len(),
                operation_summary: op_summary,
            },
        ));
        self
    }

    /// Add event routing table from the EventRouter.
    pub fn add_routes(mut self, routes: &FxHashMap<String, Arc<[String]>>) -> Self {
        for (event_type, targets) in routes {
            self.routes
                .push((event_type.clone(), targets.iter().cloned().collect()));
        }
        self
    }

    /// Build the topology, computing sources and sinks.
    pub fn build(self) -> Topology {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut node_id_map: FxHashMap<String, u32> = FxHashMap::default();
        let mut next_id: u32 = 0;

        // First pass: create nodes for all streams
        for (name, info) in &self.streams {
            let node_type = match &info.source {
                SourceKind::Join(_) => NodeType::Join,
                SourceKind::Merge(_) => NodeType::Merge,
                SourceKind::Timer => NodeType::Timer,
                _ => NodeType::Stream,
            };

            let id = ensure_node(&mut node_id_map, &mut nodes, &mut next_id, name, node_type);
            // Update the node with stream info
            if let Some(node) = nodes.iter_mut().find(|n| n.id == id) {
                node.node_type = node_type;
                node.operation_count = info.operation_count;
                node.operation_summary = info.operation_summary.clone();
            }
        }

        // Second pass: create source nodes and edges from stream sources
        for (name, info) in &self.streams {
            let stream_id = node_id_map[name.as_str()];

            match &info.source {
                SourceKind::EventType(event_type) => {
                    let source_id = ensure_node(
                        &mut node_id_map,
                        &mut nodes,
                        &mut next_id,
                        event_type,
                        NodeType::Source,
                    );
                    edges.push(TopologyEdge {
                        from: source_id,
                        to: stream_id,
                        edge_type: EdgeType::EventType,
                        label: event_type.clone(),
                    });
                }
                SourceKind::Stream(source_name) => {
                    let source_id = ensure_node(
                        &mut node_id_map,
                        &mut nodes,
                        &mut next_id,
                        source_name,
                        NodeType::Stream,
                    );
                    edges.push(TopologyEdge {
                        from: source_id,
                        to: stream_id,
                        edge_type: EdgeType::StreamOutput,
                        label: source_name.clone(),
                    });
                }
                SourceKind::Join(sources) => {
                    for source_name in sources {
                        let source_id = ensure_node(
                            &mut node_id_map,
                            &mut nodes,
                            &mut next_id,
                            source_name,
                            NodeType::Source,
                        );
                        edges.push(TopologyEdge {
                            from: source_id,
                            to: stream_id,
                            edge_type: EdgeType::JoinInput,
                            label: source_name.clone(),
                        });
                    }
                }
                SourceKind::Merge(sources) => {
                    for source_name in sources {
                        let source_id = ensure_node(
                            &mut node_id_map,
                            &mut nodes,
                            &mut next_id,
                            source_name,
                            NodeType::Source,
                        );
                        edges.push(TopologyEdge {
                            from: source_id,
                            to: stream_id,
                            edge_type: EdgeType::MergeInput,
                            label: source_name.clone(),
                        });
                    }
                }
                SourceKind::Timer => {
                    // Timer nodes are self-contained sources
                }
            }
        }

        // Third pass: add edges from event routing table
        for (event_type, targets) in &self.routes {
            // Only add routing edges for event types that aren't already
            // connected via source declarations
            if let Some(&source_id) = node_id_map.get(event_type) {
                for target in targets {
                    if let Some(&target_id) = node_id_map.get(target) {
                        // Check if this edge already exists
                        let exists = edges
                            .iter()
                            .any(|e| e.from == source_id && e.to == target_id);
                        if !exists {
                            edges.push(TopologyEdge {
                                from: source_id,
                                to: target_id,
                                edge_type: EdgeType::EventType,
                                label: event_type.clone(),
                            });
                        }
                    }
                }
            }
        }

        // Compute sources (no incoming edges) and sinks (no outgoing edges)
        let has_incoming: FxHashSet<u32> = edges.iter().map(|e| e.to).collect();
        let has_outgoing: FxHashSet<u32> = edges.iter().map(|e| e.from).collect();

        let sources: Vec<u32> = nodes
            .iter()
            .filter(|n| !has_incoming.contains(&n.id))
            .map(|n| n.id)
            .collect();

        let sinks: Vec<u32> = nodes
            .iter()
            .filter(|n| !has_outgoing.contains(&n.id))
            .map(|n| n.id)
            .collect();

        Topology {
            nodes,
            edges,
            sources,
            sinks,
        }
    }
}

/// Get or create a node ID for a given name.
fn ensure_node(
    node_id_map: &mut FxHashMap<String, u32>,
    nodes: &mut Vec<TopologyNode>,
    next_id: &mut u32,
    name: &str,
    node_type: NodeType,
) -> u32 {
    if let Some(&id) = node_id_map.get(name) {
        return id;
    }
    let id = *next_id;
    *next_id += 1;
    node_id_map.insert(name.to_string(), id);
    nodes.push(TopologyNode {
        id,
        name: name.to_string(),
        node_type,
        operation_count: 0,
        operation_summary: String::new(),
        metrics: NodeMetrics::default(),
    });
    id
}

impl Default for TopologyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_build() {
        let topo = TopologyBuilder::new().build();
        assert!(topo.nodes.is_empty());
        assert!(topo.edges.is_empty());
    }
}
