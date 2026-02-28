# ADR-008: DAG Topology

**Status:** Accepted
**Date:** 2026-02-28
**Authors:** Varpulis Team

## Context

Varpulis processes events through streams that can fan out (one event type
feeding multiple streams) and feed forward (one stream's output feeding into
another). This creates an implicit directed acyclic graph (DAG), but the
current implementation only materializes these relationships as an opaque
`FxHashMap<String, Arc<[String]>>` inside `EventRouter` (see `router.rs:13`).

This implicit structure prevents:

- **Visualization**: No API exposes the processing graph for a web UI.
- **Metrics attribution**: Per-node metrics (events received/emitted/filtered)
  require knowing which nodes exist and how they connect.
- **Execution ordering**: Hot reload and distributed scheduling need a
  topological order to wire streams correctly.
- **Validation**: Cycle detection is impossible without an explicit graph.

## Decision

Introduce an explicit `Topology` struct that wraps — rather than replaces —
the existing `Vec<RuntimeOp>` intra-stream pipeline.

### Key design choices

1. **Inter-stream DAG, linear intra-stream**: The DAG captures which streams
   feed which; within a stream, operations remain a linear `Vec<RuntimeOp>`.
   This preserves the zero-allocation hot path for event processing.

2. **Builder pattern**: `TopologyBuilder` accepts `StreamDefinition` references
   and `EventRouter` routes, producing an immutable `Topology` snapshot.

3. **JSON-serializable**: All topology types derive `Serialize`/`Deserialize`
   for REST API consumption and persistence.

4. **Kahn's algorithm**: `topological_order()` uses Kahn's algorithm for
   execution ordering; `validate()` detects cycles.

### Files

| New file | Purpose |
|----------|---------|
| `engine/topology.rs` | `Topology`, `TopologyNode`, `TopologyEdge`, `NodeType`, `NodeMetrics` |
| `engine/topology_builder.rs` | `TopologyBuilder` with `add_stream()` / `add_routes()` / `build()` |

### Modified files

| File | Change |
|------|--------|
| `engine/types.rs` | Added `summary_name()` to `RuntimeOp` for operation summaries |
| `engine/router.rs` | Added `all_routes()` to expose the routing table |
| `engine/mod.rs` | Added `topology()` method to `Engine` |

## Alternatives Considered

1. **Replace EventRouter with a graph library (petgraph)**: Rejected because
   petgraph is heavyweight for our needs (generics-heavy API, unnecessary
   edge/node weights). Our topology is small (<100 nodes typically) and rebuilt
   on reload, so a simple adjacency structure suffices.

2. **Inline DAG into StreamDefinition**: Rejected because it couples topology
   knowledge into the hot path. The current design keeps topology as a readonly
   snapshot built from the same data the engine already maintains.

3. **Build topology at AST level**: Rejected because the AST doesn't know about
   runtime artifacts like `EventRouter` fanout or sequence event types. The
   physical-level topology captures the actual execution graph.

## Consequences

### Positive

- REST endpoint can serve topology JSON for Vue Flow visualization
- Topological ordering enables deterministic stream registration during reload
- Cycle detection catches configuration errors at load time
- Per-node metrics hooks enable fine-grained observability

### Negative

- Topology must be rebuilt on every hot reload (negligible cost)
- `summary_name()` on `RuntimeOp` must be kept in sync with new variants

## References

- `crates/varpulis-runtime/src/engine/topology.rs`
- `crates/varpulis-runtime/src/engine/topology_builder.rs`
- ADR-006: Actor Framework (supervision topology)
