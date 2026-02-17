# ADR-005: Hamlet for Multi-Query Trend Aggregation

**Status:** Accepted
**Date:** 2026-02-17
**Authors:** Varpulis Team

## Context

A common class of CEP queries does not require the individual matched event sequences (which SASE+ produces), but rather aggregate statistics over them: "how many times did pattern A→B*→C match in the last hour?", "what is the sum of `b.amount` across all A→B*→C trends?". Materializing every match and then aggregating is correct but exponentially expensive when the middle clause uses Kleene closure.

**The combinatorial problem**: If N events of type B arrive between an A event and a C event, there are 2^N - 1 distinct non-empty subsequences of B that satisfy the Kleene clause. Enumerating all of them to sum `b.amount` is impractical for N beyond ~20.

Additionally, in a multi-tenant or multi-stream deployment, multiple VPL streams often share sub-patterns. For example:

```vpl
stream Count1 = Purchase -> all Refund -> Close .within(1h)
    .trend_aggregate(count: count_trends())
stream Sum1 = Purchase -> all Refund -> Close .within(1h)
    .trend_aggregate(total: sum_trends(refund.amount))
stream Count2 = Buy -> all Refund -> Sell .within(2h)
    .trend_aggregate(count: count_trends())
```

All three streams share the Kleene sub-pattern `all Refund`. Without sharing, processing each stream independently requires O(N^2) work per graphlet per query. With 50 queries sharing a Kleene pattern, the total cost is O(50 * N^2) per burst, which becomes the dominant processing bottleneck.

Two approaches were implemented and benchmarked: **GRETA** (the single-query baseline from the literature) and **Hamlet** (multi-query sharing).

## Decision

Varpulis uses **Hamlet** for trend aggregation, implemented in `crates/varpulis-runtime/src/hamlet/`.

### GRETA baseline

GRETA (VLDB 2017) computes aggregates over event trends by maintaining an event graph where nodes are events and edges represent adjacency (event e' can precede event e in a trend). Aggregates are propagated incrementally:

```
count(e) = start(e) + sum(count(e') for all predecessors e')
```

This avoids explicit trend enumeration with O(N^2) time and O(N) space per query. GRETA is implemented in `greta.rs` and used as the foundation that both Hamlet and the ZDD-Unified approach build on.

### Hamlet algorithm

Hamlet (SIGMOD 2021) extends GRETA with **graphlet-based sharing** for multi-query workloads. The key observation is that a burst of N consecutive events of the same type forms a **graphlet**: a sub-graph with all-to-all predecessor edges. Multiple queries that include this type in a Kleene clause share exactly this sub-graph.

Hamlet computes **PropagationCoefficients** for a graphlet once, independent of any specific query. For each event `e` in the graphlet:

```
count(e, q) = coeff(e) * snapshot_value(q) + local_sum(e)
```

The coefficients `coeff(e)` and `local_sum(e)` are query-independent. Per-query resolution requires only substituting the query's snapshot value — an O(1) operation per query rather than an O(N^2) graphlet traversal per query.

**Sharing decision**: The `HamletOptimizer` computes a benefit score:

```
Benefit = g^2 * (ks - sp) - ks * sp
```

where `g` is graphlet size, `ks` is the number of queries sharing the Kleene sub-pattern, and `sp` is the number of snapshots. When benefit is positive, shared mode is used; otherwise, non-shared mode avoids snapshot overhead for small graphlets or few sharing queries.

**Cross-stream sharing**: `setup_hamlet_sharing()` in `engine/mod.rs` detects streams with overlapping Kleene event types and merges them into a shared `HamletAggregator` with a unified `MergedTemplate`. All queries in the group share one graphlet traversal per burst.

### Compile-time detection

At compile time, `compile_ops_with_sequences()` detects `StreamOp::TrendAggregate` in the VPL operator chain. When present, it:
1. Calls `pattern_analyzer` to extract event types, Kleene positions, and window duration
2. Builds a `MergedTemplate` (FSA where each event type appears once, transitions labeled by query set)
3. Creates a `HamletAggregator` with one `QueryRegistration` per aggregation function
4. Returns `RuntimeOp::TrendAggregate` in the pipeline's operator list instead of `RuntimeOp::Sequence`

The stream definition then holds an `Arc<Mutex<HamletAggregator>>` (shared across queries in the same group) or a `Box<HamletAggregator>` (private to the stream).

### VPL syntax

```vpl
stream TrendCount = Purchase -> all Refund -> Close
    .within(1h)
    .trend_aggregate(
        count: count_trends(),
        total: sum_trends(refund.amount)
    )
```

`.trend_aggregate()` and `.forecast()` are mutually exclusive. `.trend_aggregate()` and SASE+ sequence detection (`->` with `.emit()`) are also mutually exclusive: the choice is made at compile time based on which operator appears in the chain.

## Alternatives Considered

### GRETA alone (no sharing)

GRETA is correct and efficient for a single query. For a small number of streams with non-overlapping patterns, GRETA per-stream is sufficient.

Rejected as the primary multi-query approach because of the measured performance degradation at scale. Without sharing:
- 10 queries: 122 K events/s (ZDD-Unified which also lacks Hamlet's sharing)
- 50 queries: 9 K events/s

The throughput is insufficient for production multi-tenant scenarios where 10-100 concurrent trend-aggregation streams are common.

### ZDD-Unified (all-pairs sharing via ZDD)

The `zdd_unified/` module implements an alternative approach: represent the entire set of matching trends as a ZDD, enumerate shared sub-trees across queries, and propagate counts through the shared structure. This is the "automatic" sharing approach — the ZDD's canonical representation inherently deduplicates shared sub-expressions.

Retained in the codebase for research purposes but not the recommended production approach.

Rejected as the primary implementation because:
- **Performance**: At 50 queries, Hamlet is 100x faster than ZDD-Unified (0.95 M/s vs 9 K/s, measured with `cargo bench -p varpulis-runtime --bench hamlet_zdd_benchmark`).
- **Complexity**: ZDD operations (conjunction, union, projection) grow with the ZDD size, which is proportional to the number of active trends. Under burst conditions (N large events of one type), ZDD size can grow rapidly.
- **Opacity**: The ZDD's implicit sharing is harder to reason about and control than Hamlet's explicit graphlet sharing. The `HamletOptimizer`'s benefit model makes sharing decisions transparent.
- **Overhead for single-query case**: Even with one query, ZDD-Unified (2.4 M/s) is slower than Hamlet (6.9 M/s) due to ZDD management overhead.

### Explicit trend enumeration + stream aggregation

Enumerate all trends using SASE+ ZDD enumeration, then apply aggregation functions over the resulting event stream.

Rejected because the enumeration is exponential in Kleene depth. SASE+ ZDD enumeration (in `sase.rs`) is capped at `MAX_ENUMERATION_RESULTS = 10_000` for safety. This cap would produce incorrect aggregates when the true count exceeds 10,000. GRETA/Hamlet produce exact aggregates regardless of trend count by propagating counts through the event graph without materializing trends.

### Window-based micro-batch aggregation

Collect events in a tumbling window, then process the full window batch with a join/group-by query.

Rejected because:
- Results are only available at window boundaries, not incrementally as patterns complete.
- The window buffer must hold all events until the window closes, increasing memory proportionally to window size × event rate.
- Cross-window patterns (a trend that started in one window and ends in the next) are not handled correctly.

## Consequences

### Positive

- Hamlet's sharing amortizes the O(N^2) graphlet traversal across all queries sharing a Kleene sub-pattern, reducing multi-query cost to O(N^2 + K) instead of O(K * N^2).
- Benchmark-validated 3x–100x improvement over ZDD-Unified at 1–50 concurrent queries, enabling production-scale multi-tenant aggregation.
- The `HamletOptimizer` adapts at runtime: if a pattern produces small graphlets or few sharing queries, it falls back to non-shared mode to avoid snapshot overhead. This means Hamlet is never worse than the baseline for single-query workloads.
- Incremental result emission (`HamletConfig::incremental = true`) can produce rolling aggregates as graphlets close, not only at window boundaries.
- The `MergedTemplate` FSA deduplicates event-type processing: each event type appears exactly once in the template regardless of how many queries reference it. Adding a new query to an existing shared group requires only adding the query's transitions to existing template states.

### Negative

- Hamlet and SASE+ are mutually exclusive per stream. A stream using `.trend_aggregate()` cannot use `.emit()` to access individual matched events. This is a hard compile-time constraint; users who need both must write separate streams.
- `setup_hamlet_sharing()` groups streams by their Kleene event types, but this grouping is static (determined at load time, not updated when new streams are added at runtime). In a long-running server where streams are deployed and undeployed dynamically, the sharing groups must be rebuilt on each deployment.
- The `MergedTemplate` FSA assumes event types appear in a fixed sequence order. Patterns where the same event type can appear in multiple positions (e.g., `A -> all B -> A -> all B -> C`) require careful template construction to avoid state merging errors.
- Cross-stream sharing uses `Arc<Mutex<HamletAggregator>>`. Under very high concurrency (many events arriving simultaneously to a shared aggregator), the mutex can become a contention point. The current design processes events sequentially within each stream's pipeline; contention arises only when multiple streams share one aggregator and their pipelines are scheduled concurrently.

## Benchmark Results

Measured with `cargo bench -p varpulis-runtime --bench hamlet_zdd_benchmark`:

| Workload | Hamlet | ZDD-Unified | Hamlet Speedup |
|----------|--------|-------------|----------------|
| 1 query | 6.9 M events/s | 2.4 M events/s | 3x |
| 5 queries | 2.8 M events/s | 398 K events/s | 7x |
| 10 queries | 2.1 M events/s | 122 K events/s | 17x |
| 50 queries | 0.95 M events/s | 9 K events/s | 100x |
| Shared Kleene | 2.1 M events/s | 140 K events/s | 15x |

Hamlet's per-query throughput degrades sub-linearly with query count (6.9 M at 1 query vs 0.95 M at 50 queries — 7x degradation for 50x more queries) because the O(N^2) graphlet cost is shared. ZDD-Unified degrades super-linearly (267x degradation for 50x more queries) because ZDD operations grow with the cross-product of queries and trends.

## References

- **GRETA (VLDB 2017)**: Olga Poppe, Chuan Lei, Elke A. Rundensteiner, David Maier. "GRETA: Graph-based Real-time Event Trend Aggregation." VLDB Vol. 11, No. 1, pp. 80-92. DOI: [10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)
- **Hamlet (SIGMOD 2021)**: Olga Poppe, Allison Rozet, Chuan Lei, Elke A. Rundensteiner, David Maier. "To Share or Not to Share: Online Event Trend Aggregation Over Bursty Event Streams." SIGMOD '21, pp. 1453-1465. DOI: [10.1145/3448016.3457310](https://doi.org/10.1145/3448016.3457310)
- `crates/varpulis-runtime/src/hamlet/` — Hamlet implementation
- `crates/varpulis-runtime/src/greta.rs` — GRETA baseline
- `crates/varpulis-runtime/src/zdd_unified/` — ZDD-Unified (research reference)
- `crates/varpulis-runtime/src/engine/pattern_analyzer.rs` — Extracts pattern structure for Hamlet
- `docs/architecture/trend-aggregation.md` — Full trend aggregation architecture documentation
- `benchmarks/hamlet_zdd_benchmark.rs` — Benchmark comparing Hamlet vs ZDD-Unified
