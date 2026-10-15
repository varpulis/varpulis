# Trend Aggregation Subsystem

## Overview

The Varpulis CEP engine supports two mutually exclusive processing modes for
temporal event patterns:

- **Detection mode** -- returns the actual matched event sequences. Uses the
  SASE+ NFA engine. Activated by the `->` (followed-by) syntax in VPL.
- **Aggregation mode** -- computes COUNT, SUM, AVG, MIN or MAX over event
  trends without materializing individual matches. Counts the trends on the
  graph of a window's events (GRETA). Activated by `.trend_aggregate()` in
  VPL.

A given stream uses one mode or the other, never both. The choice is made at
compile time by the presence of `.trend_aggregate()` in the stream's
operator chain. When present, the compiler builds a `TrendAggregateConfig`
op at the head of the pipeline instead of a `SaseEngine`.

## Decision Tree

```
Need actual matched event sequences?
  YES --> Detection mode (SASE+ NFA)
          VPL: A -> all B where B.x > 10 -> C .within(1h) .emit(...)

  NO, need aggregate statistics over trends?
    YES --> Aggregation mode (counting on the event graph)
            VPL: A -> all B -> C .within(1h) .trend_aggregate(count: count_trends())
```

## Internal Architecture

### Compile-Time Pipeline

`Engine::compile_trend_mode` (`crates/varpulis-runtime/src/engine/compilation.rs`)
runs when a stream's operators contain `.trend_aggregate()`:

1. **Steps.** The start comes from the stream's source (`EventType as
   alias`, with its optional `where`), the rest from the `->` clauses; a
   named `pattern` gives all of them. Each step keeps its event type (a
   filter-only stream resolves to its source type, as for a sequence), its
   alias, its condition, whether it is a closure (`all`) and its monotonic
   operator.
2. **Refusals.** What the counting cannot honour is refused with the form it
   honours: no `.within()`, no closure, a closure as the start, a negated
   step, `.each()`/`.longest()`/`.strict()`/`.stnm()`, `.forecast()`, a
   condition reading an alias the counting cannot bind, an aggregate that is
   not one of the six, a bare field, an alias that is not a step.
3. **Aggregates.** Each `.trend_aggregate()` item becomes a
   `TrendAggregate`: `CountTrends`, `CountEvents(step)`, `Sum`, `Avg`,
   `Min`, `Max` over `alias.field`.
4. **The op.** `TrendAggregateConfig::new(steps, aggregates, within,
   partition_key)` goes in at index 0 of the runtime ops, replacing the
   sequence op; the `.where()` and `.emit()` that follow run on its results.

### Run-Time Pipeline

`crates/varpulis-runtime/src/engine/trend.rs`:

1. **Windows.** `add(event)` files the event into its partition's window
   (keyed by the partition field's value, `"default"` without one). The
   window opens with its first event and ends `within` later. An event at
   or past the end closes the window first -- its result comes back and
   goes on down the pipeline -- and opens the next one. The engine's clock
   pass (`close_windows_into`, after every batch and at the end of the
   input) calls `close_through(wm)`, which closes every window whose end
   `wm` has reached, earliest first; the op is a time window for that pass,
   so a `.watermark(out_of_order:)` delays it like any other.
2. **Counting.** `close_window` runs the dynamic programme over the window's
   events, in arrival order:
   - `reach[step][event]`: the partial trends that reach the event at the
     step. 1 for a start that satisfies the start's condition; for a later
     step, the sum over the earlier events from which the event can *enter*
     the step, plus, for a closure, the sum over the earlier events of the
     closure it can *extend*.
   - `complete[step][event]`: the trends completing from there, the
     symmetric sum over the later events, plus 1 at the last step.
   - `participations[step][event] = reach · complete`.
   Whether an event may follow another is the step's condition evaluated by
   the runtime's expression evaluator (`eval_filter_expr`) against the
   candidate event with a `SequenceContext` binding the start's alias to the
   start, the closure's alias to its previous event (on entry, to the step
   before), the previous step's alias to its event; then the monotonic
   operator against the previous event. A condition that reads no alias is
   evaluated once per event. The context is built once per predecessor and
   reused over its successors.
   When a condition reads the start's alias the sums depend on the start and
   the programme runs once per start; otherwise once, seeding every start,
   as GRETA does.
3. **The result.** One event (`TrendAggregateResult`, renamed by `.emit()`)
   stamped with the window's end: the partition field, the aggregates
   (`count_trends` as the sum of completions over the starts; `count_events`
   and `sum_trends` as sums of participations, times the field; `avg_trends`
   as sum over trends; `min`/`max` over the events with participations),
   `_count_{alias}` (distinct events of the step in a trend) and the fields
   of the single steps' bound events as `alias.field` (earliest start, latest
   later-step event). A window past its cap carries `_trend_truncated`.
4. **Snapshot.** `checkpoint()`/`restore()` keep each partition's events and
   when its window opened, in the engine's `WindowCheckpoint` shape; the op
   is part of `Program::snapshot`.

### Complexity

Per window of `n` events, `m` steps and `s` starts: O(m · n²) condition
evaluations when no condition reads the start, O(s · m · n²) when one does.
Counts are `u64` with saturating arithmetic, emitted as integers clamped to
`i64::MAX`. A partition's window holds at most `MAX_WINDOW_EVENTS` (5 000)
events.

## The Hamlet library

`crates/varpulis-hamlet` implements the shared graphlet propagation of
Hamlet (Poppe et al., SIGMOD 2021): several queries over the same Kleene
sub-pattern share one propagation, with snapshots at graphlet boundaries and
an optimizer deciding when sharing pays. It works on sequences of event
types without conditions. The engine's `.trend_aggregate()` used to feed it
and read its counts; since a pattern without conditions is rare and the
sharing cannot honour one, the op no longer uses it. The crate stays in the
workspace as a library with its own tests and benchmarks.

## Key Source Files

| File | Role |
|------|------|
| `crates/varpulis-runtime/src/engine/trend.rs` | Windows, counting, result |
| `crates/varpulis-runtime/src/engine/compilation.rs` | `compile_trend_mode`: steps, refusals, aggregates |
| `crates/varpulis-runtime/src/engine/dispatch.rs` | Clock-driven and end-of-input closes |
| `crates/varpulis-runtime/src/engine/pipeline.rs` | The op in the pipeline |
| `crates/varpulis-engine/tests/audit_trend_aggregate.rs` | The counts, step by step |
| `crates/varpulis-hamlet/` | The Hamlet library (not used by the op) |

## References

- **GRETA**: Poppe, Lei, Rundensteiner, Maier. "GRETA: Graph-based
  Real-time Event Trend Aggregation." PVLDB 11(1), 2017.
- **Hamlet**: Poppe, Lei, Rundensteiner, Maier. "To Share or Not to Share:
  Online Event Trend Aggregation Over Bursty Event Streams." SIGMOD 2021.
