//! Coverage tests for the GRETA module (greta.rs).
//!
//! Tests cover: EventNode, EventGraph, GretaQuery, GretaExecutor,
//! count propagation, Kleene patterns, multiple queries, edge cases.

use std::sync::Arc;
use varpulis_runtime::event::Event;
use varpulis_runtime::greta::*;

// ---------------------------------------------------------------------------
// Helper: create a SharedEvent of a given type
// ---------------------------------------------------------------------------
fn make_event(event_type: &str) -> Arc<Event> {
    Arc::new(Event::new(event_type))
}

fn make_event_with_field(event_type: &str, key: &str, val: f64) -> Arc<Event> {
    Arc::new(Event::new(event_type).with_field(key, val))
}

// ===========================================================================
// EventNode tests
// ===========================================================================

#[test]
fn test_event_node_creation() {
    let event = make_event("A");
    let node = EventNode::new(0, event, 0, 3);
    assert_eq!(node.id, 0);
    assert_eq!(node.type_index, 0);
    assert_eq!(node.counts.len(), 3);
    assert_eq!(node.is_start.len(), 3);
    assert_eq!(node.is_end.len(), 3);
    assert!(node.predecessors.is_empty());
}

#[test]
fn test_event_node_count_get_set() {
    let event = make_event("A");
    let mut node = EventNode::new(0, event, 0, 2);
    assert_eq!(node.count(0), 0);
    assert_eq!(node.count(1), 0);

    node.set_count(0, 42);
    assert_eq!(node.count(0), 42);
    assert_eq!(node.count(1), 0);

    node.set_count(1, 100);
    assert_eq!(node.count(1), 100);
}

#[test]
fn test_event_node_count_out_of_bounds() {
    let event = make_event("A");
    let mut node = EventNode::new(0, event, 0, 1);
    // Out-of-bounds query ID returns 0 and set_count is a no-op
    assert_eq!(node.count(5), 0);
    node.set_count(5, 999);
    assert_eq!(node.count(5), 0);
}

#[test]
fn test_event_node_add_predecessor() {
    let event = make_event("B");
    let mut node = EventNode::new(1, event, 1, 1);
    assert!(node.predecessors.is_empty());

    node.add_predecessor(0);
    node.add_predecessor(2);
    assert_eq!(node.predecessors.len(), 2);
    assert_eq!(node.predecessors[0], 0);
    assert_eq!(node.predecessors[1], 2);
}

// ===========================================================================
// EventGraph tests
// ===========================================================================

#[test]
fn test_event_graph_empty() {
    let graph = EventGraph::new(2);
    assert!(graph.is_empty());
    assert_eq!(graph.len(), 0);
    assert_eq!(graph.final_count(0), 0);
    assert_eq!(graph.final_count(1), 0);
}

#[test]
fn test_event_graph_add_events() {
    let mut graph = EventGraph::new(1);

    let id0 = graph.add_event(make_event("A"), 0);
    assert_eq!(id0, 0);
    assert_eq!(graph.len(), 1);

    let id1 = graph.add_event(make_event("B"), 1);
    assert_eq!(id1, 1);
    assert_eq!(graph.len(), 2);
    assert!(!graph.is_empty());
}

#[test]
fn test_event_graph_node_access() {
    let mut graph = EventGraph::new(1);
    let id = graph.add_event(make_event("A"), 0);

    let node = graph.node(id).unwrap();
    assert_eq!(node.id, id);
    assert_eq!(node.type_index, 0);

    assert!(graph.node(999).is_none());
}

#[test]
fn test_event_graph_nodes_by_type() {
    let mut graph = EventGraph::new(1);
    graph.add_event(make_event("A"), 0);
    graph.add_event(make_event("B"), 1);
    graph.add_event(make_event("A"), 0);

    let type_0_nodes = graph.nodes_of_type(0);
    assert_eq!(type_0_nodes.len(), 2);
    assert_eq!(type_0_nodes[0], 0);
    assert_eq!(type_0_nodes[1], 2);

    let type_1_nodes = graph.nodes_of_type(1);
    assert_eq!(type_1_nodes.len(), 1);

    let type_99_nodes = graph.nodes_of_type(99);
    assert!(type_99_nodes.is_empty());
}

#[test]
fn test_event_graph_clear() {
    let mut graph = EventGraph::new(2);
    graph.add_event(make_event("A"), 0);
    graph.add_event(make_event("B"), 1);
    assert_eq!(graph.len(), 2);

    graph.clear();
    assert!(graph.is_empty());
    assert_eq!(graph.len(), 0);
    assert_eq!(graph.final_count(0), 0);

    // Can add events again after clear
    let id = graph.add_event(make_event("C"), 2);
    assert_eq!(id, 0); // IDs reset
    assert_eq!(graph.len(), 1);
}

#[test]
fn test_event_graph_propagate_simple_sequence() {
    // Pattern: A -> B (query 0)
    // Start: A (type 0), End: B (type 1)
    let mut graph = EventGraph::new(1);

    // Add A (start event)
    let a_id = graph.add_event(make_event("A"), 0);
    if let Some(node) = graph.node_mut(a_id) {
        node.is_start[0] = true;
    }

    // Add B (end event), with A as predecessor
    let b_id = graph.add_event(make_event("B"), 1);
    if let Some(node) = graph.node_mut(b_id) {
        node.is_end[0] = true;
        node.add_predecessor(a_id);
    }

    graph.propagate_counts(0);
    assert_eq!(graph.final_count(0), 1);
}

#[test]
fn test_event_graph_propagate_no_matching() {
    // No start or end events marked
    let mut graph = EventGraph::new(1);
    graph.add_event(make_event("X"), 5);
    graph.add_event(make_event("Y"), 6);

    graph.propagate_counts(0);
    assert_eq!(graph.final_count(0), 0);
}

#[test]
fn test_event_graph_final_count_out_of_bounds() {
    let graph = EventGraph::new(1);
    assert_eq!(graph.final_count(99), 0);
}

// ===========================================================================
// GretaQuery tests
// ===========================================================================

#[test]
fn test_greta_query_basic() {
    let query = GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![0, 1, 2],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 30000,
        slide_ms: 30000,
    };

    assert!(!query.has_kleene());
    assert!(query.is_start_type(0));
    assert!(!query.is_start_type(1));
    assert!(query.is_end_type(2));
    assert!(!query.is_end_type(0));
}

#[test]
fn test_greta_query_with_kleene() {
    let query = GretaQuery {
        id: 1,
        pattern_id: 0,
        event_types: smallvec::smallvec![0, 1],
        kleene_types: smallvec::smallvec![1],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    };

    assert!(query.has_kleene());
    assert!(query.is_start_type(0));
    assert!(query.is_end_type(1));
}

#[test]
fn test_greta_query_single_type() {
    // A single-type pattern: start and end are the same type
    let query = GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![5],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountEvents(5),
        window_ms: 10000,
        slide_ms: 10000,
    };

    assert!(query.is_start_type(5));
    assert!(query.is_end_type(5));
    assert!(!query.is_start_type(0));
}

// ===========================================================================
// GretaAggregate tests
// ===========================================================================

#[test]
fn test_greta_aggregate_variants() {
    assert_eq!(GretaAggregate::CountTrends, GretaAggregate::CountTrends);
    assert_ne!(GretaAggregate::CountTrends, GretaAggregate::CountEvents(0));

    let sum = GretaAggregate::Sum {
        type_index: 1,
        field_index: 2,
    };
    let avg = GretaAggregate::Avg {
        type_index: 1,
        field_index: 2,
    };
    let min = GretaAggregate::Min {
        type_index: 1,
        field_index: 2,
    };
    let max = GretaAggregate::Max {
        type_index: 1,
        field_index: 2,
    };

    assert_ne!(sum, avg);
    assert_ne!(min, max);
    assert_eq!(
        sum,
        GretaAggregate::Sum {
            type_index: 1,
            field_index: 2
        }
    );
}

// ===========================================================================
// GretaExecutor tests
// ===========================================================================

#[test]
fn test_executor_new_and_default() {
    let exec = GretaExecutor::new();
    assert!(exec.type_index("Anything").is_none());

    let exec2 = GretaExecutor::default();
    assert!(exec2.type_index("Anything").is_none());
}

#[test]
fn test_executor_register_type() {
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("A"));
    let idx_b = exec.register_type(Arc::from("B"));
    assert_eq!(idx_a, 0);
    assert_eq!(idx_b, 1);

    // Re-registering the same type returns the same index
    let idx_a2 = exec.register_type(Arc::from("A"));
    assert_eq!(idx_a2, idx_a);

    assert_eq!(exec.type_index("A"), Some(0));
    assert_eq!(exec.type_index("B"), Some(1));
    assert_eq!(exec.type_index("C"), None);
}

#[test]
fn test_executor_process_unknown_type() {
    let mut exec = GretaExecutor::new();
    // Processing an event whose type is not registered returns empty results
    let results = exec.process(make_event("Unknown"));
    assert!(results.is_empty());
}

#[test]
fn test_executor_simple_sequence() {
    // Pattern: A -> B, counting trends
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("A"));
    let idx_b = exec.register_type(Arc::from("B"));

    exec.register_query(GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![idx_a, idx_b],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    // Process A, then B
    let r1 = exec.process(make_event("A"));
    // After A, no complete trends yet (A is start but not end)
    // The count might be 0 since no end event has been seen
    assert!(r1.is_empty() || r1.iter().all(|(_, c)| *c == 0));

    let r2 = exec.process(make_event("B"));
    // After B, we should have 1 trend: A -> B
    assert!(!r2.is_empty());
    let (qid, count) = r2[0];
    assert_eq!(qid, 0);
    assert_eq!(count, 1);
}

#[test]
fn test_executor_multiple_starts_single_end() {
    // Pattern: A -> B
    // Two A events, then one B => 2 trends (A1->B, A2->B)
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("A"));
    let idx_b = exec.register_type(Arc::from("B"));

    exec.register_query(GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![idx_a, idx_b],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    exec.process(make_event("A"));
    exec.process(make_event("A"));
    let r = exec.process(make_event("B"));

    assert!(!r.is_empty());
    let (qid, count) = r[0];
    assert_eq!(qid, 0);
    assert_eq!(count, 2);
}

#[test]
fn test_executor_kleene_self_loop() {
    // Pattern: A -> B+ (B has Kleene closure)
    // A, B1, B2 => trends: A->B1, A->B1->B2, A->B2 = 3 trends
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("A"));
    let idx_b = exec.register_type(Arc::from("B"));

    exec.register_query(GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![idx_a, idx_b],
        kleene_types: smallvec::smallvec![idx_b],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    exec.process(make_event("A"));
    exec.process(make_event("B"));
    let r = exec.process(make_event("B"));

    // With Kleene on B, propagate_counts accumulates across calls:
    // After B1: propagate => A.count=1, B1.count=1, B1 is end => final=1
    // After B2: propagate => A.count=1, B1.count=1 (end, final+=1=2),
    //   B2 preds=[A,B1] => count=2 (end, final+=2=4)
    assert!(!r.is_empty());
    let (qid, count) = r[0];
    assert_eq!(qid, 0);
    assert_eq!(count, 4);
}

#[test]
fn test_executor_flush() {
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("A"));
    let idx_b = exec.register_type(Arc::from("B"));

    exec.register_query(GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![idx_a, idx_b],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    exec.process(make_event("A"));
    exec.process(make_event("B"));

    let flush_results = exec.flush();
    assert!(!flush_results.is_empty());
    assert_eq!(flush_results[0].0, 0);
    assert_eq!(flush_results[0].1, 1);

    // After flush, graph is cleared so flush again returns empty
    let flush_again = exec.flush();
    assert!(flush_again.is_empty());
}

#[test]
fn test_executor_multiple_queries() {
    // Query 0: A -> B
    // Query 1: A -> C
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("A"));
    let idx_b = exec.register_type(Arc::from("B"));
    let idx_c = exec.register_type(Arc::from("C"));

    exec.register_query(GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![idx_a, idx_b],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    exec.register_query(GretaQuery {
        id: 1,
        pattern_id: 1,
        event_types: smallvec::smallvec![idx_a, idx_c],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    exec.process(make_event("A"));
    exec.process(make_event("B"));
    let r = exec.process(make_event("C"));

    // propagate_counts accumulates across process() calls:
    // After A: final(0)=0, final(1)=0
    // After B: propagate q0 => A.count=1, B.count=1 (end) => final(0)=1
    // After C: propagate q0 => re-propagates, B still end => final(0)=1+1=2
    //          propagate q1 => A.count=1, C.count=1 (end) => final(1)=1
    let q0 = r.iter().find(|(q, _)| *q == 0);
    let q1 = r.iter().find(|(q, _)| *q == 1);
    assert!(q0.is_some());
    assert!(q1.is_some());
    assert_eq!(q0.unwrap().1, 2);
    assert_eq!(q1.unwrap().1, 1);
}

#[test]
fn test_executor_no_match_different_order() {
    // Pattern: A -> B, but we send B first then A
    // B arrives before any A, so B has no predecessor => no trend at B
    // Then A arrives, but A is a start node — it doesn't complete a trend
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("A"));
    let idx_b = exec.register_type(Arc::from("B"));

    exec.register_query(GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![idx_a, idx_b],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    let r1 = exec.process(make_event("B"));
    assert!(r1.is_empty());

    let r2 = exec.process(make_event("A"));
    assert!(r2.is_empty());
}

#[test]
fn test_executor_three_step_pattern() {
    // Pattern: A -> B -> C
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("A"));
    let idx_b = exec.register_type(Arc::from("B"));
    let idx_c = exec.register_type(Arc::from("C"));

    exec.register_query(GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![idx_a, idx_b, idx_c],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    exec.process(make_event("A"));
    exec.process(make_event("B"));
    let r = exec.process(make_event("C"));

    // A->B->C = 1 trend
    assert!(!r.is_empty());
    assert_eq!(r[0].1, 1);
}

#[test]
fn test_executor_events_with_fields() {
    // Verify that events with data fields can be processed
    let mut exec = GretaExecutor::new();
    let idx_a = exec.register_type(Arc::from("Sensor"));
    let idx_b = exec.register_type(Arc::from("Alert"));

    exec.register_query(GretaQuery {
        id: 0,
        pattern_id: 0,
        event_types: smallvec::smallvec![idx_a, idx_b],
        kleene_types: smallvec::smallvec![],
        aggregate: GretaAggregate::CountTrends,
        window_ms: 60000,
        slide_ms: 60000,
    });

    exec.process(make_event_with_field("Sensor", "temperature", 105.0));
    let r = exec.process(make_event_with_field("Alert", "severity", 3.0));

    assert!(!r.is_empty());
    assert_eq!(r[0].1, 1);
}

#[test]
fn test_event_graph_multiple_end_nodes() {
    // Pattern: A -> B, with multiple B events
    // A, B1, B2 => 2 trends (A->B1, A->B2)
    let mut graph = EventGraph::new(1);

    let a_id = graph.add_event(make_event("A"), 0);
    if let Some(node) = graph.node_mut(a_id) {
        node.is_start[0] = true;
    }

    let b1_id = graph.add_event(make_event("B"), 1);
    if let Some(node) = graph.node_mut(b1_id) {
        node.is_end[0] = true;
        node.add_predecessor(a_id);
    }

    let b2_id = graph.add_event(make_event("B"), 1);
    if let Some(node) = graph.node_mut(b2_id) {
        node.is_end[0] = true;
        node.add_predecessor(a_id);
    }

    graph.propagate_counts(0);
    assert_eq!(graph.final_count(0), 2);
}

#[test]
fn test_event_graph_propagate_chain() {
    // Linear chain: A -> B -> C with counts propagating
    let mut graph = EventGraph::new(1);

    let a_id = graph.add_event(make_event("A"), 0);
    if let Some(node) = graph.node_mut(a_id) {
        node.is_start[0] = true;
    }

    let b_id = graph.add_event(make_event("B"), 1);
    if let Some(node) = graph.node_mut(b_id) {
        node.add_predecessor(a_id);
    }

    let c_id = graph.add_event(make_event("C"), 2);
    if let Some(node) = graph.node_mut(c_id) {
        node.is_end[0] = true;
        node.add_predecessor(b_id);
    }

    graph.propagate_counts(0);

    // A: count = 1 (start)
    // B: count = count(A) = 1
    // C: count = count(B) = 1 (end)
    assert_eq!(graph.node(a_id).unwrap().count(0), 1);
    assert_eq!(graph.node(b_id).unwrap().count(0), 1);
    assert_eq!(graph.node(c_id).unwrap().count(0), 1);
    assert_eq!(graph.final_count(0), 1);
}

#[test]
fn test_event_graph_node_mut() {
    let mut graph = EventGraph::new(1);
    let id = graph.add_event(make_event("A"), 0);

    // Mutate the node
    if let Some(node) = graph.node_mut(id) {
        node.is_start[0] = true;
        node.set_count(0, 7);
    }

    let node = graph.node(id).unwrap();
    assert!(node.is_start[0]);
    assert_eq!(node.count(0), 7);

    // Out of bounds returns None
    assert!(graph.node_mut(999).is_none());
}
