# ADR-006: Emission Modes (Each / Longest / Subsets)

**Status:** Accepted
**Date:** 2026-04-08
**Authors:** Varpulis Team

## Context

The SASE+ paper (SIGMOD 2008, Agrawal/Diao/Gyllstrom/Immerman) defines pattern matching with three event selection strategies (strict-contiguity, skip-till-next-match, skip-till-any-match) but conflates **selection** with **output cardinality**. Under skip-till-any-match (STAM) with a Kleene closure like `SEQ(A, B+, C)`, the paper specifies that **2^N − 1** matches are output — one per non-empty subset of the Kleene events. This is a verbose mode; a compressed mode shares state but conceptually emits the same number of matches.

For practitioners using Varpulis, this exponential output is rarely what they want. A pattern like `Start -> all TempReading as r -> End` with 9 readings would emit 511 matches under spec-correct STAM verbose. Most users expect either:
- **One emit per reading** (linear, "for each B do something")
- **One emit at completion** with all readings accessible

A bug discovered in 2026-04-07 was that `complete_run()` short-circuited and returned only **1 match** for these patterns instead of either 511 (per spec) or 9/1 (per common sense). The bug was hidden by 18+ tests using `assert!(!results.is_empty())` instead of asserting exact counts.

This ADR documents the decision to **separate selection strategy from emission mode** as orthogonal concerns, choose practitioner-friendly defaults, and provide explicit operators for users who want spec-compliance or alternative semantics.

## Decision

### Two orthogonal axes

We split SASE+ output semantics into two independent operators:

1. **Selection strategy** (`SelectionStrategy` enum, `varpulis-sase/src/types.rs`):
   - `StrictContiguous` — events must be adjacent
   - `SkipTillNextMatch` — non-overlapping maximal matches
   - `SkipTillAnyMatch` — overlapping runs from every anchor (default)

2. **Emission mode** (`EmissionMode` enum, `varpulis-sase/src/types.rs` — new):
   - `Each` — emit one match per Kleene event extension (linear)
   - `Longest` — emit one consolidated match at terminator/break
   - `Subsets` — emit one match per non-empty subset of the Kleene capture (paper-correct STAM verbose)

User-facing operators in VPL:
- `.strict()`, `.stnm()`, `.stam()` — selection
- `.each()`, `.longest()`, `.subsets()` — emission

### Default emission mode

The default is `EmissionMode::Each`. Rationale:

1. **Practitioner intuition**: Most CEP users coming from Esper, FlinkCEP, or Apama expect "fire on each event" semantics. Asking them to configure a mode for the most common case is friction.
2. **Linear cost**: `Each` is O(N) in the Kleene size — predictable and bounded.
3. **No data loss**: Each captured Kleene event produces an observable output, so users can react to every step.

The default is **overridden to `Longest`** for monotonic patterns (`.increasing()` / `.decreasing()`) because users writing those operators want one "trend ended" alert, not one per data point. Users can flip back with `.increasing(temp).each()`.

### Subsets mode is opt-in

`.subsets()` is the only mode that produces exponential output. We keep it available because:
- It's the SASE+ paper's formal semantics — users doing spec compliance or research need it
- The ZDD enumeration was already implemented; not exposing it would waste existing infrastructure

It's capped at `MAX_ENUMERATION_RESULTS = 10_000` and documented prominently as "expert mode".

### Single-run-per-partition for Kleene-final-from-start

For patterns where the start state directly leads to a Kleene with `has_epsilon_to_accept` (e.g., bare `B+` or `all B as b`), only one active run per partition is allowed. Otherwise STAM would create a new run on each event AND extend the existing run, producing duplicate emissions for every Kleene step.

This is detected via `Nfa::is_kleene_final_from_start()`. The check excludes patterns like `all B -> Tick` where the Kleene has a terminator transition (not just an epsilon to Accept), since for those, multiple anchored runs ARE meaningful.

### `complete_run` dispatch

```rust
fn complete_run(run, limits, evaluator, mode: EmissionMode) -> RunAdvanceResult {
    // Deferred predicate forces enumeration regardless of mode
    if has_deferred_predicate { return CompleteMulti(enumerate_with_filter(...)); }

    match mode {
        Each if has_kleene_capture => Drained,  // already emitted during accumulation
        Each => Complete(...),                    // non-Kleene pattern, emit normally
        Subsets if has_kleene_capture => CompleteMulti(enumerate_with_filter(...)),
        Subsets => Complete(...),                 // non-Kleene
        Longest => Complete(...),                 // single match with last captured
    }
}
```

The new `Drained` variant signals "run is finished, drop it, don't emit a final match" — used by Each mode when matches were already produced during Kleene accumulation and the terminator should not duplicate.

## Alternatives Considered

### Default to `Longest`

Closer to FlinkCEP's "first match" semantics. Rejected because users frequently want intermediate emissions during accumulation (e.g., for streaming dashboards), and the SASE+ paper's spirit is closer to "every match is interesting".

### Default to `Subsets` (paper-compliant)

Strictly correct per SIGMOD 2008. Rejected because exponential output is impractical for typical workloads — a 20-event Kleene produces over a million matches.

### Single mode flag instead of two axes

Combine selection and emission into one enum. Rejected because they're genuinely orthogonal: a user might want STNM selection (no overlapping runs) with Subsets emission (paper-correct subset enumeration of the single match).

### Auto-detect mode from pattern shape

Have the engine pick the mode based on whether the Kleene is followed by a terminator. Rejected as too magical — users can't predict the behavior without reading the engine source.

## Consequences

### Positive

- **Practitioner-friendly defaults**: most patterns "just work" without configuring a mode
- **Spec compliance available**: `.subsets()` provides paper-correct STAM verbose for users who need it
- **Orthogonal axes**: selection and emission can be combined freely
- **Bug fixed**: Kleene patterns now produce the correct number of matches per the chosen mode
- **Test suite strengthened**: `crates/varpulis-runtime/tests/sase_spec_compliance.rs` adds 15 oracle tests asserting exact match counts

### Negative

- **Breaking change for tests**: 18+ existing tests with weak `!is_empty()` assertions had to be updated to use explicit `.with_emission_mode(EmissionMode::Longest)` or strengthen their assertions
- **More API surface**: 6 new operators to learn (though only 1-2 are needed in practice)
- **Mode resolution complexity**: the `resolved_emission_mode()` logic must handle override > monotonic auto > default precedence
- **Drained variant overhead**: complete_run now returns one of 6 variants instead of 5, requiring caller updates in `process_partition_shared`/`process_runs_shared`

### Migration

Users on v0.10.0 or earlier with patterns like `Start -> all B as b -> End` should:
- Add `.longest()` if they want the previous "1 match at terminator" behavior
- Leave it (default `.each()`) if they actually wanted "1 match per B" — and benefit from the bug fix

## References

- **SASE+ (SIGMOD 2008)**: Agrawal, Diao, Gyllstrom, Immerman. "Efficient Pattern Matching over Event Streams." §4.2 (Output Modes), §3 (Selection Strategies)
- **SIGMOD 2014**: Zhang, Diao, Immerman. "On Complexity and Optimization of Expensive Queries in Complex Event Processing." §1 (exponential STAM cost), §5 (sharing optimizations)
- `crates/varpulis-sase/src/types.rs::EmissionMode` — runtime enum
- `crates/varpulis-sase/src/advance.rs::complete_run` — dispatch logic
- `crates/varpulis-sase/src/engine.rs::resolved_emission_mode` — auto-resolution
- `crates/varpulis-runtime/tests/sase_spec_compliance.rs` — oracle tests
- `docs/guides/sase-patterns.md` — user guide with mode tables and examples
- ADR-004: SASE+ Semantics (predecessor — established the NFA + ZDD architecture this ADR builds on)
