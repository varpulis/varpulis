# ADR-009: Logical/Physical Plan Separation

**Status:** Accepted
**Date:** 2026-02-28
**Authors:** Varpulis Team

## Context

The current compilation pipeline is:

```
AST → compile_ops_with_sequences() → Vec<RuntimeOp>
```

This single-step transformation (in `engine/mod.rs:1182`) simultaneously
plans and materializes the execution. The resulting `RuntimeOp` values contain
closures, mutable state (windows, aggregators), and runtime handles — making
them impossible to inspect, serialize, or optimize before execution.

This blocks:

- **EXPLAIN**: Users cannot see what the engine will do before running a query.
- **Optimization**: Filter pushdown, window merging, and projection pruning
  require an intermediate representation without closures.
- **Distributed execution**: Serializing a plan to send to remote workers
  requires closure-free types.

## Decision

Introduce a three-stage compilation pipeline:

```
AST → LogicalPlanner → LogicalPlan → Optimizer → LogicalPlan (optimized)
    → PhysicalPlanner → PhysicalPlan → Engine materialization
```

### Logical Plan (`varpulis-core/src/plan.rs`)

Serializable, closure-free types mirroring the AST but normalized:

- `LogicalPlan` — top-level container
- `LogicalStream` — per-stream: id, name, source, operations, estimated cardinality
- `LogicalSource` — EventType, Stream, Join, Merge, Timer, Sequence, Pattern, FromConnector
- `LogicalOp` — ~25 variants mirroring `StreamOp` but in plan form
- `explain()` method for human-readable display

### Planner (`varpulis-runtime/src/engine/planner.rs`)

`logical_plan(program: &Program) -> Result<LogicalPlan, String>` performs a
straightforward 1:1 mapping from AST types to plan types.

### Optimizer (`varpulis-parser/src/optimizer.rs`)

Rule-based optimization with iterative convergence (max 10 passes):

| Rule | Effect |
|------|--------|
| `FilterPushdown` | Move filters before windows when they don't reference aggregation output |
| `TemporalFilterPushdown` | Push time-field filters before windows |
| `WindowMerge` | Merge adjacent windows with identical configuration |
| `ProjectionPruning` | Remove redundant adjacent projections |

### Physical Plan (`varpulis-runtime/src/engine/physical_plan.rs`)

Wraps existing `StreamDefinition` (with `Vec<RuntimeOp>`) plus logical plan
correlation metadata. The existing `compile_ops_with_sequences()` function
remains the materialization backend.

## Alternatives Considered

1. **Modify RuntimeOp to be serializable**: Rejected because `RuntimeOp`
   contains closures (`WhereClosure`), mutable window state, and SASE engines.
   Making these serializable would require massive refactoring with no benefit
   to the hot path.

2. **Single LogicalPlan without PhysicalPlan**: Rejected because the physical
   plan needs to carry runtime artifacts (SASE engines, Hamlet aggregators)
   that don't belong in the logical representation.

3. **Use an existing query planner crate (DataFusion, etc.)**: Rejected because
   VPL's streaming semantics (windows, SASE patterns, trend aggregation) don't
   map cleanly to batch query planners. The custom planner is <300 lines and
   precisely matches VPL's type system.

## Consequences

### Positive

- EXPLAIN command shows what the engine will execute
- Optimizer can improve query performance without user intervention
- Plans are serializable for distributed execution
- Clean separation makes each stage independently testable

### Negative

- Adds one more data structure to keep in sync with AST/StreamOp changes
- Optimizer rules must be carefully validated to preserve semantics
- Small overhead from building the logical plan (negligible vs. compilation)

## References

- `crates/varpulis-core/src/plan.rs` — Logical plan types
- `crates/varpulis-runtime/src/engine/planner.rs` — AST → LogicalPlan
- `crates/varpulis-parser/src/optimizer.rs` — Plan optimizer
- `crates/varpulis-runtime/src/engine/physical_plan.rs` — Physical plan
- `crates/varpulis-parser/src/optimize.rs` — Existing constant folding (AST level)
