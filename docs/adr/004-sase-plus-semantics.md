# ADR-004: SASE+ Semantics for Pattern Matching

**Status:** Accepted
**Date:** 2026-02-17
**Authors:** Varpulis Team

## Context

The core capability of Varpulis is **complex event processing (CEP)**: detecting meaningful patterns across a stream of events ordered in time. The pattern language must support:

- **Sequence patterns**: Event A must be followed by Event B within a time window
- **Kleene closure**: Zero-or-more (A*) or one-or-more (A+) occurrences of an event type between the anchor events
- **Negation**: A pattern is complete only if a forbidden event type does NOT occur during a window
- **Conjunction**: Both of two event types must occur (in any order)
- **Temporal constraints**: The entire pattern must complete within a fixed duration
- **Partition-by**: Patterns are tracked independently per key value (e.g., `session_id`)
- **Cross-event predicates**: A predicate on event B can reference a captured value from event A (`b.price > a.threshold`)

Additionally, the Kleene closure requirement raises a combinatorial challenge: if 100 events of type B match the `all B` Kleene clause between A and C, there are 2^100 - 1 possible B-subsequences to enumerate, each a valid match. This must be handled without exponential memory or time.

## Decision

Varpulis implements the **SASE+** algorithm for pattern matching, as described by Wu, Diao, and Rizvi (SIGMOD 2006), with several extensions specific to Varpulis's needs.

### Core: NFA-based pattern matching

Each VPL stream with a sequence pattern (using `->`) is compiled to a Non-deterministic Finite Automaton (NFA) by `NfaCompiler`. Pattern runs (`ActiveRun`) track the current NFA state and the events captured so far. When an event arrives, all active runs are advanced; runs that reach the accept state produce output events.

The `SaseEngine` maintains the set of active runs and the event skip strategy. Varpulis implements:

- **Skip-till-any-match**: a new run can start on any event that matches the first NFA state, without waiting for previous runs to complete. This allows overlapping pattern matches.
- **Skip-till-next-match**: (configurable) a run advances past events that don't match the current state, rather than requiring contiguous matching events.

### Extension 1: Kleene closure with ZDD enumeration

For Kleene states (`A+`, `A*`), the NFA uses a self-loop transition. The `varpulis-zdd` crate provides a Zero-suppressed Binary Decision Diagram (ZDD) that compactly represents the powerset of accumulated Kleene events.

When a run reaches an accept state after a Kleene closure, `enumerate_with_filter` traverses the ZDD to produce all valid subsets of Kleene events that satisfy any postponed predicates (see below). This converts the 2^n representation into actual match instances, bounded by `MAX_ENUMERATION_RESULTS = 10_000` and `MAX_KLEENE_EVENTS = 20` to prevent runaway memory consumption.

The ZDD representation is the key insight: 100 Kleene events produce ~100 ZDD nodes rather than 2^100 explicit subsets. The subsets are materialized lazily only at enumeration time, and only those passing the filter are emitted.

### Extension 2: Predicate classification (SIGMOD 2014 §5.2)

Predicates on Kleene events are classified at compile time:

- **Contiguous predicates** (e.g., `b.price > 10`): evaluate eagerly during Kleene accumulation, discarding non-matching events immediately.
- **Inconsistent predicates** (e.g., `b.price > a.price` where `a` is captured from a prior state): cannot be evaluated until the specific combination of events is materialized. These are **postponed** to the enumeration phase.

The `classify_predicate` function assigns each predicate to `PredicateClass::Contiguous` or `PredicateClass::Inconsistent`. Postponed predicates are stored in `state.postponed_predicate` on the Kleene NFA state.

### Extension 3: Temporal negation

Negation states (`StateType::Negation`) implement the absence-of-event constraint from the original SASE paper. A run in a negation state is invalidated (not completed) if a forbidden event type arrives during the window. A run that survives the window boundary without seeing the forbidden event proceeds to the continue state.

The `negation_info` field on a negation state stores the forbidden event type, the predicate to apply to candidate forbidden events, and the ID of the continue state to transition to upon timeout.

### Extension 4: AND conjunctions

`AND(A, B)` patterns (both A and B must occur, in any order) are implemented via `StateType::And` with an `AndConfig` listing the required branches. When a run is in an AND state, it collects events for each branch; the run advances when all branches are satisfied.

### Extension 5: Temporal windowing

`SasePattern::Within(pattern, duration)` wraps any sub-pattern. Active runs track a `start_time` and are pruned when they age beyond the window. This is evaluated lazily on each event arrival rather than via a background timer, avoiding wakeup overhead for inactive runs.

### Extension 6: Partition-by

The SASEXT extension stores separate NFA run sets per partition key. When a VPL stream uses `.partition_by(field)`, the `SaseEngine` shards `active_runs` into a `HashMap<PartitionKey, Vec<ActiveRun>>`. Runs in different partitions are completely independent and do not interfere.

### VPL syntax

VPL expresses SASE+ patterns using the `->` operator:

```vpl
stream FraudAlert = Login as login
    -> all Transaction where amount > 1000 as txn
    -> Logout
    .within(30m)
    .where(txn.merchant != login.home_country)
    .emit(alert: "suspicious", user: login.user_id)
```

The `all` keyword before an event type enables Kleene plus (`+`) semantics. Without `all`, the event type matches once (standard sequence). The `.within(duration)` operator adds a temporal constraint. Cross-event predicates in `.where()` are handled by the engine's `eval_filter_expr` function, which resolves field references against the captured event map.

## Alternatives Considered

### Automata-based CEP without ZDD (explicit enumeration)

The simplest alternative is to store Kleene events in a `Vec` and enumerate all subsets explicitly at accept time.

Rejected because exponential blowup is real and unavoidable in practice. Industrial event streams regularly produce 20-50 events of a single type in a burst (e.g., sensor readings, transaction events). A 30-event Kleene closure with explicit enumeration produces ~10^9 combinations. The `MAX_KLEENE_EVENTS = 20` safety cap would still be necessary, but approaches it without a compact intermediate representation.

The ZDD approach defers materialization and provides a compact canonical form for the subset space, enabling the `enumerate_with_filter` to short-circuit branches that fail the postponed predicate early.

### Esper (JVM)

Esper is the dominant open-source CEP engine on the JVM. It implements EPL (Event Processing Language), a SQL-like pattern language with its own execution model.

Rejected because:
- Varpulis targets Rust for memory safety and performance. Embedding a JVM dependency is out of scope.
- EPL's pattern matching uses a different execution model (lazy evaluation trees) that does not map cleanly to the NFA-based SASE+ semantics Varpulis requires for the PST forecasting integration.

### FlinkCEP (pattern API)

Apache Flink includes a CEP library (FlinkCEP) with NFA-based pattern matching.

Rejected because:
- Flink is a distributed stream processing framework; embedding it in Varpulis would introduce a large JVM dependency and an incompatible execution model.
- FlinkCEP's pattern API does not expose the NFA structure needed for the PST/Pattern Markov Chain forecasting integration (ADR-004 is a prerequisite for the PST feature: the PMC is built directly from the SASE NFA by reading `nfa()` and `active_run_snapshots()`).

### Rule-based CEP engines

Simpler engines match events against a list of rule predicates without an NFA, triggering actions when all conditions in a rule are satisfied.

Rejected because rule-based engines cannot express temporally-ordered patterns (A must precede B must precede C) or Kleene closure without encoding them as ad-hoc state machines. The NFA model handles arbitrary pattern compositions uniformly.

## Consequences

### Positive

- The NFA model maps directly to the formal definitions in the SASE and SASE+ papers, making the implementation verifiable against the academic literature.
- ZDD-based enumeration makes Kleene patterns tractable for typical burst sizes (up to 20 events). The `MAX_KLEENE_EVENTS` and `MAX_ENUMERATION_RESULTS` safety caps prevent pathological memory consumption.
- Predicate postponement (SIGMOD 2014 §5.2) allows cross-event predicates to coexist with Kleene accumulation: contiguous predicates prune early during accumulation, inconsistent predicates are deferred to enumeration. This avoids either incorrect early pruning or excessive state accumulation.
- The NFA structure is inspectable at runtime via `SaseEngine::nfa()`. The PST forecasting module reads the NFA's states and transitions to build a Pattern Markov Chain, enabling pattern completion probability estimates without duplicating pattern structure.
- `active_run_snapshots()` exposes lightweight copies of active run states (NFA state ID, captured event count, start time) to the PST module, enabling online forecasting as patterns partially match.

### Negative

- The `MAX_KLEENE_EVENTS = 20` cap means patterns with more than 20 matching Kleene events in a single burst produce partial results. Users working with high-cardinality Kleene patterns must be aware of this limit.
- Implementing the `enumerate_with_filter` ZDD traversal correctly is subtle: the code must traverse ZDD nodes in the correct topological order and apply the postponed predicate to each enumerated subset without materializing the entire powerset.
- Negation with temporal windows requires careful handling of the timeout transition: a run in a negation state must survive until the window expires, during which it holds references to all previously captured events. For long windows with many active runs, this can be memory-intensive.
- The AND state implementation (`StateType::And`, `AndConfig`) is more complex than the sequence case because partial satisfaction of branches must be tracked across multiple events arriving in arbitrary order.
- Partition-by sharding means that a stream with `N` distinct partition keys has `N` independent run sets. For high-cardinality keys (e.g., user IDs in a multi-tenant service), memory scales with the number of distinct partition values × average active runs per partition.

## References

- **SASE (SIGMOD 2006)**: Wu, Diao, Rizvi. "High-Performance Complex Event Processing over Streams." ACM SIGMOD, 2006. The foundational paper for the NFA-based SASE algorithm implemented in `sase.rs`.
- **SASE+ / Postponed Predicates (SIGMOD 2014)**: Mei and Madden. "ZStream: A Cost-based Query Processor for Adaptively Detecting Composite Events." *Related work on predicate classification and postponement for Kleene closures in CEP.*
- **ZDD (BDD variant)**: Minato, S. "Zero-Suppressed BDDs for Set Manipulation in Combinatorial Problems." DAC 1993. The data structure used for Kleene closure enumeration.
- `crates/varpulis-runtime/src/sase.rs` — SASE+ NFA implementation
- `crates/varpulis-zdd/` — ZDD library for Kleene enumeration
- `docs/guides/sase-patterns.md` — User-facing SASE pattern documentation
- `crates/varpulis-runtime/src/pst/markov_chain.rs` — Pattern Markov Chain, which consumes the NFA for forecasting
