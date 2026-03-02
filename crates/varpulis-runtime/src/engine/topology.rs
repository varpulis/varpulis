//! DAG topology representation for Varpulis stream processing
//!
//! Provides an introspectable graph of inter-stream relationships for
//! visualization, metrics collection, and execution ordering. Intra-stream
//! operations remain as linear `Vec<RuntimeOp>` to preserve the hot path.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

/// The stream processing topology — a DAG of nodes and edges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    /// All nodes in the topology
    pub nodes: Vec<TopologyNode>,
    /// All edges (data flow connections) between nodes
    pub edges: Vec<TopologyEdge>,
    /// Node IDs that are pure sources (no incoming edges)
    pub sources: Vec<u32>,
    /// Node IDs that are pure sinks (no outgoing edges)
    pub sinks: Vec<u32>,
}

/// A single node in the topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyNode {
    /// Unique node identifier
    pub id: u32,
    /// Human-readable name (stream name or connector name)
    pub name: String,
    /// Type of this node
    pub node_type: NodeType,
    /// Number of operations in this stream's pipeline
    pub operation_count: usize,
    /// Summary of operations (e.g., "Filter → Window → Aggregate → Emit")
    pub operation_summary: String,
    /// Runtime metrics for this node
    pub metrics: NodeMetrics,
}

/// Edge connecting two topology nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyEdge {
    /// Source node ID
    pub from: u32,
    /// Target node ID
    pub to: u32,
    /// Type of this edge
    pub edge_type: EdgeType,
    /// Label for the edge (e.g., event type name)
    pub label: String,
}

/// Classification of topology nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    /// External event source (connector or event type)
    Source,
    /// Stream processing pipeline
    Stream,
    /// Join of multiple streams
    Join,
    /// Merge of multiple event sources
    Merge,
    /// Periodic timer
    Timer,
    /// External sink connector
    Sink,
}

/// Classification of topology edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeType {
    /// Event type routing (EventRouter fanout)
    EventType,
    /// Stream output feeding into another stream
    StreamOutput,
    /// Input to a merge node
    MergeInput,
    /// Input to a join node
    JoinInput,
}

/// Runtime metrics for a topology node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// Total events received by this node
    pub events_received: u64,
    /// Total events emitted by this node
    pub events_emitted: u64,
    /// Total events filtered out by this node
    pub events_filtered: u64,
    /// Total processing time in nanoseconds
    pub processing_time_ns: u64,
}

impl Topology {
    /// Compute a topological ordering of nodes using Kahn's algorithm.
    ///
    /// Returns node IDs in execution order (sources first, sinks last).
    /// Returns `None` if the graph contains a cycle.
    pub fn topological_order(&self) -> Option<Vec<u32>> {
        let node_ids: Vec<u32> = self.nodes.iter().map(|n| n.id).collect();
        let max_id = node_ids.iter().copied().max().unwrap_or(0) as usize;

        // Build in-degree map and adjacency list
        let mut in_degree = vec![0u32; max_id + 1];
        let mut adj: Vec<Vec<u32>> = vec![Vec::new(); max_id + 1];

        for edge in &self.edges {
            in_degree[edge.to as usize] += 1;
            adj[edge.from as usize].push(edge.to);
        }

        // Initialize queue with zero in-degree nodes
        let mut queue: VecDeque<u32> = VecDeque::new();
        for &id in &node_ids {
            if in_degree[id as usize] == 0 {
                queue.push_back(id);
            }
        }

        let mut order = Vec::with_capacity(node_ids.len());

        while let Some(node) = queue.pop_front() {
            order.push(node);
            for &neighbor in &adj[node as usize] {
                in_degree[neighbor as usize] -= 1;
                if in_degree[neighbor as usize] == 0 {
                    queue.push_back(neighbor);
                }
            }
        }

        if order.len() == node_ids.len() {
            Some(order)
        } else {
            None // Cycle detected
        }
    }

    /// Validate the topology (check for cycles).
    ///
    /// Returns `Ok(())` if the topology is a valid DAG, or `Err` with a description
    /// of the problem.
    pub fn validate(&self) -> Result<(), String> {
        if self.topological_order().is_none() {
            return Err("Topology contains a cycle".to_string());
        }
        Ok(())
    }

    /// Serialize the topology to JSON for UI consumption.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: u32) -> Option<&TopologyNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Get all edges originating from a node.
    pub fn outgoing_edges(&self, node_id: u32) -> Vec<&TopologyEdge> {
        self.edges.iter().filter(|e| e.from == node_id).collect()
    }

    /// Get all edges targeting a node.
    pub fn incoming_edges(&self, node_id: u32) -> Vec<&TopologyEdge> {
        self.edges.iter().filter(|e| e.to == node_id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: u32, name: &str, node_type: NodeType) -> TopologyNode {
        TopologyNode {
            id,
            name: name.to_string(),
            node_type,
            operation_count: 0,
            operation_summary: String::new(),
            metrics: NodeMetrics::default(),
        }
    }

    fn make_edge(from: u32, to: u32) -> TopologyEdge {
        TopologyEdge {
            from,
            to,
            edge_type: EdgeType::EventType,
            label: String::new(),
        }
    }

    #[test]
    fn test_empty_topology() {
        let topo = Topology {
            nodes: vec![],
            edges: vec![],
            sources: vec![],
            sinks: vec![],
        };
        assert_eq!(topo.topological_order(), Some(vec![]));
        assert!(topo.validate().is_ok());
    }

    #[test]
    fn test_linear_topology() {
        // A → B → C
        let topo = Topology {
            nodes: vec![
                make_node(0, "A", NodeType::Source),
                make_node(1, "B", NodeType::Stream),
                make_node(2, "C", NodeType::Sink),
            ],
            edges: vec![make_edge(0, 1), make_edge(1, 2)],
            sources: vec![0],
            sinks: vec![2],
        };

        let order = topo.topological_order().unwrap();
        assert_eq!(order, vec![0, 1, 2]);
        assert!(topo.validate().is_ok());
    }

    #[test]
    fn test_fanout_topology() {
        // A → B, A → C
        let topo = Topology {
            nodes: vec![
                make_node(0, "Source", NodeType::Source),
                make_node(1, "Stream1", NodeType::Stream),
                make_node(2, "Stream2", NodeType::Stream),
            ],
            edges: vec![make_edge(0, 1), make_edge(0, 2)],
            sources: vec![0],
            sinks: vec![1, 2],
        };

        let order = topo.topological_order().unwrap();
        assert_eq!(order[0], 0); // Source must come first
        assert!(topo.validate().is_ok());
    }

    #[test]
    fn test_cycle_detection() {
        // A → B → A (cycle)
        let topo = Topology {
            nodes: vec![
                make_node(0, "A", NodeType::Stream),
                make_node(1, "B", NodeType::Stream),
            ],
            edges: vec![make_edge(0, 1), make_edge(1, 0)],
            sources: vec![],
            sinks: vec![],
        };

        assert!(topo.topological_order().is_none());
        assert!(topo.validate().is_err());
    }

    #[test]
    fn test_json_serialization() {
        let topo = Topology {
            nodes: vec![make_node(0, "Source", NodeType::Source)],
            edges: vec![],
            sources: vec![0],
            sinks: vec![],
        };

        let json = topo.to_json().unwrap();
        assert!(json.contains("Source"));
        let roundtrip: Topology = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip.nodes.len(), 1);
    }

    #[test]
    fn test_edge_queries() {
        let topo = Topology {
            nodes: vec![
                make_node(0, "A", NodeType::Source),
                make_node(1, "B", NodeType::Stream),
                make_node(2, "C", NodeType::Sink),
            ],
            edges: vec![make_edge(0, 1), make_edge(1, 2)],
            sources: vec![0],
            sinks: vec![2],
        };

        assert_eq!(topo.outgoing_edges(0).len(), 1);
        assert_eq!(topo.incoming_edges(1).len(), 1);
        assert_eq!(topo.outgoing_edges(2).len(), 0);
    }
}
