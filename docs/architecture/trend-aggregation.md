# Trend Aggregation Subsystem

## Overview

The Varpulis CEP engine supports two mutually exclusive processing modes for temporal event patterns:

- **Detection mode** -- returns the actual matched event sequences. Uses the SASE+ NFA engine. Activated by the `->` (followed-by) syntax in VPL.
- **Aggregation mode** -- computes COUNT, SUM, AVG, MIN, or MAX over event trends without materializing individual matches. Uses the Hamlet engine. Activated by `.trend_aggregate()` in VPL.

A given stream uses one mode or the other, never both. The choice is determined at compile time by the presence of `.trend_aggregate()` in the stream's operator chain. When detected, the compiler builds a `HamletAggregator` instead of a `SaseEngine`.

## Decision Tree

```
Need actual matched event sequences?
  YES --> Detection mode (SASE+ NFA)
          VPL: A -> all B where B.x > 10 -> C .within(1h) .emit(...)

  NO, need aggregate statistics over trends?
    YES --> Aggregation mode (Hamlet)
            VPL: A -> all B -> C .within(1h) .trend_aggregate(count: count_trends())

Multiple queries with overlapping Kleene sub-patterns?
  YES --> Automatic cross-stream sharing via Hamlet (setup_hamlet_sharing)
  NO  --> Per-stream Hamlet aggregator
```

## Internal Architecture

### Compile-Time Pipeline

```
VPL source
  |
  v
Parser (varpulis-parser)
  |  StreamOp::TrendAggregate(Vec<TrendAggItem>)  -- AST node
  v
Engine::load() --> compile_ops_with_sequences()
  |
  |-- Detects StreamOp::TrendAggregate in the op chain
  |-- Calls pattern_analyzer to extract event types and Kleene positions
  |-- Builds MergedTemplate via TemplateBuilder
  |-- Creates HamletAggregator with QueryRegistration
  |-- Inserts RuntimeOp::TrendAggregate at position 0 (replaces RuntimeOp::Sequence)
  |-- Returns (runtime_ops, sase_engine=None, ..., hamlet_aggregator=Some(...))
  |
  v
Engine::load() --> setup_hamlet_sharing()
  |
  |-- Scans all streams with hamlet_aggregator != None
  |-- Groups streams by their Kleene event types
  |-- For groups of 2+ streams, builds a shared MergedTemplate
  |-- Creates a single shared HamletAggregator with all queries registered
  |-- Replaces per-stream aggregators with Arc<Mutex<HamletAggregator>>
  |-- Updates each stream's TrendAggregateConfig.query_id
```

### Runtime Event Flow

```
Incoming event
  |
  v
EventRouter --> StreamDefinition
  |
  v
execute_pipeline() / execute_pipeline_sync()
  |
  |-- Encounters RuntimeOp::TrendAggregate(config)
  |-- Calls hamlet_aggregator.process(event)
  |-- HamletAggregator:
  |     1. Maps event type name to type_index via MergedTemplate
  |     2. Closes previous graphlet if event type changed
  |     3. Adds event to HamletGraph (current graphlet)
  |     4. Updates query states via template transitions
  |     5. Returns Vec<AggregationResult> (incremental or empty)
  |-- Wraps each AggregationResult into a TrendAggregateResult event
  |-- Pipeline continues with downstream ops (select, emit, to, ...)
```

## Key Components

### `pattern_analyzer.rs`

Path: `crates/varpulis-runtime/src/engine/pattern_analyzer.rs`

Extracts structural information from VPL AST nodes for Hamlet setup:

- `extract_event_types(source, followed_by)` -- returns event type names in sequence order from the stream source and followed-by chain.
- `extract_kleene_info(source, followed_by)` -- identifies which positions have Kleene closure (`match_all: true` / the `all` keyword).
- `extract_within_ms(expr)` -- parses window duration from a `Within` expression.
- `trend_item_to_greta(item, type_indices)` -- converts a `TrendAggItem` (e.g., `count_trends()`, `sum_trends(b.price)`) to the internal `GretaAggregate` enum.

### `hamlet/aggregator.rs`

Path: `crates/varpulis-runtime/src/hamlet/aggregator.rs`

Core executor. `HamletAggregator` combines:

- A `MergedTemplate` (compiled FSA) for pattern matching.
- A `HamletGraph` for event storage and graphlet management.
- A `HamletOptimizer` for runtime sharing decisions.
- Per-query state tracking (`current_state`, `count`, `snapshot_value`).

Key methods:

- `register_query(QueryRegistration)` -- registers a query with its event types, Kleene types, and aggregate function.
- `process(SharedEvent) -> Vec<AggregationResult>` -- main entry point. Manages graphlet lifecycle, updates query states, returns incremental results.
- `flush() -> Vec<AggregationResult>` -- closes all active graphlets and returns final results at window boundary.

Processing modes within a graphlet:

- **Shared mode** (`process_shared_graphlet`): Computes `PropagationCoefficients` once, resolves per-query using snapshots. Used when multiple queries share a Kleene sub-pattern.
- **Non-shared mode** (`process_non_shared_graphlet`): Computes Kleene count independently per query. Formula: `count = snapshot_value * (2^n - 1)`.

### `hamlet/template.rs`

Path: `crates/varpulis-runtime/src/hamlet/template.rs`

`MergedTemplate` is a Finite State Automaton where:

- Each event type appears exactly once.
- Transitions are labeled with the set of queries they apply to.
- Kleene self-loops are identified and marked as shareable when used by multiple queries.

`TemplateBuilder` provides a fluent API: `add_sequence(query_id, types)`, `add_kleene(query_id, type_name, state)`, `build()`.

### `hamlet/graphlet.rs`

Path: `crates/varpulis-runtime/src/hamlet/graphlet.rs`

A `Graphlet` is a sub-graph containing events of a single type during an interval where no events of other pattern types arrive. It is the atomic unit of sharing in Hamlet.

Key properties:

- `nodes: Vec<GraphletNode>` -- events with predecessor edges, snapshot coefficients, and local sums.
- `is_shared: bool` -- whether this graphlet is processed in shared mode.
- `sharing_queries: SmallVec<[QueryId; 8]>` -- queries that share this graphlet.

`add_kleene_edges()` creates predecessor links for all-to-all connectivity within the graphlet (each event preceded by all earlier events). `propagate_shared()` computes `snapshot_coeff` and `local_sum` per node so counts can be resolved for any query by substituting the query-specific snapshot value.

`GraphletPool` provides object reuse to avoid allocation overhead.

### `hamlet/snapshot.rs`

Path: `crates/varpulis-runtime/src/hamlet/snapshot.rs`

Snapshots capture intermediate aggregation state at graphlet boundaries:

- `Snapshot` stores per-query values (`SmallVec<[SnapshotValue; 8]>`), indexed by source and target graphlet.
- `PropagationCoefficients` enables query-independent computation within a graphlet. For each event `e`: `count(e, q) = coeff(e) * snapshot_value(q) + local_sum(e)`.
- `compute_kleene()` populates coefficients for Kleene self-loop patterns (all-predecessor connectivity).
- `resolve_count(snapshot_value)` and `resolve_final_counts(end_indices, snapshot)` produce per-query results by substituting each query's snapshot value.
- `merge_snapshots()` combines snapshots from multiple predecessor graphlets.

### `hamlet/optimizer.rs`

Path: `crates/varpulis-runtime/src/hamlet/optimizer.rs`

`HamletOptimizer` makes runtime split/merge decisions based on a benefit model:

```
Benefit = g^2 * (ks - sp) - ks * sp
```

where `g` = graphlet size, `ks` = number of queries sharing the Kleene sub-pattern, `sp` = number of snapshots. When benefit is positive, shared mode is used; otherwise, non-shared mode is cheaper.

## Shared vs. Non-Shared Complexity

For `k` queries sharing a Kleene pattern of length `n`:

| Mode | Complexity |
|------|-----------|
| Non-shared (per-query) | O(k * n^2) |
| Shared (Hamlet) | O(sp * n^2 + k * sp) |

When `sp << k` (few snapshots relative to queries), Hamlet provides significant speedups because the expensive n^2 graphlet traversal is performed once, and only the lightweight snapshot resolution is done per query.

## Benchmark Results

Run: `cargo bench -p varpulis-runtime --bench hamlet_zdd_benchmark`

| Benchmark | Hamlet | ZDD Unified | Hamlet Speedup |
|-----------|--------|-------------|----------------|
| Single query | 6.9 M events/s | 2.4 M events/s | 3x |
| 5 queries | 2.8 M events/s | 398 K events/s | 7x |
| 10 queries | 2.1 M events/s | 122 K events/s | 17x |
| 50 queries | 0.95 M events/s | 9 K events/s | 100x |
| Shared Kleene | 2.1 M events/s | 140 K events/s | 15x |

Hamlet's explicit graphlet-based sharing with O(1) transitions outperforms ZDD's automatic canonical sharing with O(ZDD size) operations. The advantage grows with query count because Hamlet's shared propagation cost is amortized across all queries, while ZDD processes each query path independently.

## Academic References

- **GRETA** (VLDB 2017): Olga Poppe, Chuan Lei, Elke A. Rundensteiner, David Maier. "Graph-based Real-time Event Trend Aggregation." Proceedings of the VLDB Endowment, Vol. 11, No. 1, pp. 80-92. DOI: [10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)

- **Hamlet** (SIGMOD 2021): Olga Poppe, Allison Rozet, Chuan Lei, Elke A. Rundensteiner, David Maier. "To Share or Not to Share: Online Event Trend Aggregation Over Bursty Event Streams." Proceedings of the 2021 International Conference on Management of Data (SIGMOD '21), pp. 1453-1465. DOI: [10.1145/3448016.3457310](https://doi.org/10.1145/3448016.3457310)
