
---

# Phased Implementation Plan: Streaming Columnar Grouped Aggregator for Varpulis

## Phase 1 delivered (2026-04-09)

Phase 1 landed as planned: the streaming columnar grouped aggregator is
wired into `PartitionedAggregatorState::apply` behind the `arrow` feature
(now in `default`), with Sum/Avg/Min/Max/Count accumulators, a
`GroupKeyEncoder` on top of `arrow_row::RowConverter`, a one-shot
driver, 23 unit tests, and 7 parity tests confirming bit-identical
output vs the row path on six distinct input shapes.

**Microbenchmark (`PartitionedAggregatorState::apply` — criterion, row vs columnar):**

| Config                      | Row     | Columnar | Speedup |
|-----------------------------|---------|----------|---------|
| 1 000 events × 10 groups    | 202 µs  | 159 µs   | 1.27×   |
| 1 000 events × 100 groups   | 380 µs  | 257 µs   | 1.48×   |
| 10 000 events × 10 groups   | 2.82 ms | 1.64 ms  | 1.72×   |
| 10 000 events × 100 groups  | 2.60 ms | 1.76 ms  | 1.48×   |
| 100 000 events × 100 groups | 49.4 ms | 23.8 ms  | **2.08×** |
| 100 000 events × 10k groups | 65.4 ms | 41.0 ms  | 1.60×   |

**Scenario 02 end-to-end (1-second tumbling window per device, 100k events, 100 devices):**

Arroyo ~78 k eps, Varpulis (phase 1) ~52 k eps, V/A ≈ 0.67.

**Phase 1 does not move the scenario 02 number** because the pipeline
(`partition_by + window(1s) + aggregate`) fires the `PartitionedAggregator`
~99 700 times, each with exactly 1 event — below the `ARROW_BATCH_THRESHOLD`
of 32, so the columnar path never engages. A diagnostic counter confirmed
100 % of `apply()` calls went through the row path.

**Lesson for the plan:** Phase 1's "gate on batch size per `apply()` call"
is effective for **bulk aggregation** (one call with many events — the
"analytics" shape) but has zero effect on **streaming aggregation**
(many tiny fires — the "event-time window per key" shape Arroyo is
optimised for). Phase 2 exists to fuse the window and aggregator so
accumulator state lives across fires; that is where scenario 02 is
fixed. The phase-1 wins are real but land on workload shapes different
from the marquee benchmark.

---

## Executive summary

Arroyo beats Varpulis on scenario 02 (windowed partitioned aggregation) by ~40% because its hot path is:
- one columnar `RowConverter` hash over incoming batches to resolve `(group_key → group_idx)`
- `state[group_idx].sum += value; state[group_idx].count += 1`
- no per-event string allocation, no hashmap probe, no Arc clone

Varpulis's `PartitionedAggregatorState::apply` today pays a `String` allocation, an `FxHashMap<String, Vec<SharedEvent>>` probe, and an `Arc::clone` per event, and then hands each partition's row-oriented vec to `Aggregator::apply_shared`, which re-walks the events a third time per aggregation function. On scenario 02, each `(window, device)` group contains exactly one event, so every row pays the full overhead.

The fix is to build a ~500-line `ColumnarGroupedAggregator` on top of `arrow-array` + `arrow-row`, wire it into `PartitionedAggregatorState` as a feature-gated fast path (phase 1), then fuse the partitioned tumbling window with the partitioned aggregate so per-group accumulator state lives across arriving batches instead of being rebuilt at fire time (phase 2). No DataFusion dependency, strictly arrow-rs primitives the runtime already compiles against.

---

## Phase 0 — Non-goals (explicit out-of-scope list)

The following stay row-oriented on `Arc<Event>` and are NOT touched in any phase of this plan:

- `RuntimeOp::Sequence` — SASE+ NFA pattern matching
- `RuntimeOp::Pattern` — `.pattern(...)` lambda matching
- `RuntimeOp::TrendAggregate` — Hamlet ZDD-based trend engine
- `RuntimeOp::Forecast` — PST Markov-chain pattern forecaster
- `RuntimeOp::Enrich`, `RuntimeOp::Process`, `RuntimeOp::Alert`, `RuntimeOp::Log`, `RuntimeOp::Print`
- `RuntimeOp::Score` — ONNX scoring
- WASM UDFs and `wasm_udf` module
- `JoinBuffer` and `RuntimeOp::Join` — row-oriented, stays that way indefinitely (see phase 5 note)
- `varpulis-hamlet`, `varpulis-sase`, `varpulis-pst` — external CEP crates, untouched
- The SASE+ engine (`sase_persistence`, `sequence.rs`)
- `Watermark`, `AllowedLateness`, `Concurrent`
- The `async-runtime`, `connectors`, `tokio` plumbing

Concretely: any operator where per-event semantics depend on an `Arc<Event>` reference (pattern contexts, sequence buffers, imperative processors) stays row-oriented. The columnar path is strictly confined to the "windowed group-by" shape: `[partition_by →] window → aggregate [→ having]`.

---

## Phase 1 — Columnar grouped accumulator + integration into `PartitionedAggregatorState::apply`

### Goal

Replace the per-event `String`/hashmap/Arc-clone loop in `PartitionedAggregatorState::apply` with a single columnar conversion + one `arrow-row` hash pass + typed `Vec<f64>` accumulator updates. This alone eliminates the dominant cost of scenario 02 without touching the window operator or the pipeline driver.

### Approach

Build a new `ColumnarGroupedAggregator` in a new module `crates/varpulis-runtime/src/arrow_aggregate/`. Feature-gate the entire module behind `#[cfg(feature = "arrow")]`. Inside `PartitionedAggregatorState::apply`, check the `events.len() >= ARROW_BATCH_THRESHOLD` gate and under `#[cfg(feature = "arrow")]` route through the new aggregator; keep the existing row path as `#[cfg(not(feature = "arrow"))]` and as the below-threshold fallback.

The aggregator stays **per-call** in phase 1 (state rebuilt on each `apply()`), so the wire-in is a pure function of `events` → `Vec<(key, IndexMap<String, Value>)>`. The caller (pipeline.rs) doesn't see any behavior change.

### Module layout decision — recommendation

Use a **subdirectory** `crates/varpulis-runtime/src/arrow_aggregate/`, not a single `arrow_aggregate.rs`. Justification:

- By the end of phase 3 we will ship: the trait, 5+ accumulator impls, the grouped driver, the row-encoder wrapper, the per-bin streaming driver, plus tests. Estimated 800–1200 LOC total, which is unpleasant in one file.
- Submodules let us keep each accumulator (`sum.rs`, `min_max.rs`, `avg.rs`, `count.rs`) self-contained, matching how `aggregation.rs` today has one `impl AggregateFunc` block per function. That's a familiar shape for the reviewer.
- The streaming driver (`streaming.rs` in phase 2) is a different beast from the one-shot grouped driver (`grouped.rs` in phase 1); forcing them into one file now produces noise.
- Submodules can be `pub(crate)`, keeping the public surface tight.

Proposed phase-1 file layout under `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/`:

```
mod.rs          — crate-private exports, feature gate, module docs
accumulator.rs  — `ColumnarAccumulator` trait + `AccumulatorKind` enum + factory
sum.rs          — `SumAccumulator`  (Vec<f64> sums, Vec<u64> counts for null handling)
avg.rs          — `AvgAccumulator`  (Vec<f64> sums, Vec<u64> counts — reuses sum logic)
min_max.rs      — `MinAccumulator` / `MaxAccumulator` (Vec<f64>, sentinel for empty groups)
count.rs        — `CountAccumulator` (Vec<u64> counts)
group_keys.rs   — `GroupKeyEncoder` wrapping `arrow_row::RowConverter`
grouped.rs      — `ColumnarGroupedAggregator` one-shot driver (phase 1 entry point)
```

Register the module in `lib.rs` as:
```rust
#[cfg(feature = "arrow")]
pub mod arrow_aggregate;
```

### `ColumnarAccumulator` trait — exact signature

```rust
// arrow_aggregate/accumulator.rs
use std::sync::Arc;
use arrow_array::{Array, ArrayRef};

/// A grouped accumulator that maintains per-group state across batches.
///
/// Modelled loosely on DataFusion's `GroupsAccumulator`, but stripped
/// down to the Varpulis use case: one value column, f64-only numerics,
/// no filter/opt_filter, no merge_batch (we only do Partial mode).
pub trait ColumnarAccumulator: Send + Sync {
    /// Grow internal storage so that group index `total_groups - 1` is valid.
    /// Called once per batch after `GroupKeyEncoder::encode_batch` returns
    /// the new `total_groups` count.
    fn resize(&mut self, total_groups: usize);

    /// Update state for each row in `values`, using `group_indices[row_i]`
    /// as the destination group index.
    ///
    /// `values` and `group_indices` have the same length (batch row count).
    ///
    /// For Count, `values` may be `None` (no input column needed).
    fn update_batch(
        &mut self,
        values: Option<&dyn Array>,
        group_indices: &[u32],
    );

    /// Produce the final per-group output array, one value per group index
    /// in `0..self.num_groups()`. After calling `evaluate`, the accumulator
    /// is logically drained but may reuse allocations on next `resize`.
    fn evaluate(&mut self) -> ArrayRef;

    /// Human-readable name ("sum", "avg", ...) — used for error messages
    /// and debug output.
    fn name(&self) -> &'static str;

    /// Expected input `DataType`. Returned as `Option` because Count
    /// doesn't need an input column.
    fn input_type(&self) -> Option<arrow_schema::DataType>;
}
```

### `values` type decision — recommendation

**Use `Option<&dyn Array>`** (not `&Float64Array`). Reasons:

1. **Count doesn't take an input column.** If we hard-code `&Float64Array`, Count becomes a special case in every call site. `Option<&dyn Array>` cleanly expresses "may or may not have a column".
2. **We must handle `Int64` → promoted to f64** at the accumulator boundary to match existing Varpulis semantics (`Sum::apply` falls through `get_float`, which returns `Int` values coerced via `as f64`). Doing this coercion on a `&Float64Array` requires the caller to pre-project, which couples the grouped driver to per-accumulator knowledge of expected types.
3. **Downcast cost is one branch per *batch*, not per event.** Inside `SumAccumulator::update_batch`, we do `let arr = values.unwrap().as_any().downcast_ref::<Float64Array>().expect(...);` once, then loop over the typed slice. The hot loop body is still `sums[group_indices[i] as usize] += arr.value(i);` which compiles to 2–3 instructions per row. This is the same pattern Arroyo/DataFusion use via `GroupsAccumulator`.
4. **Monomorphisation doesn't win here.** The "hybrid" you mentioned — DataFusion's `GroupsAccumulator` — downcasts once per batch and then operates on the concrete slice. We follow that pattern. We are NOT generic over `T: ArrowPrimitiveType`, because that multiplies codegen across Int32/Int64/UInt32/UInt64/Float32/Float64 variants we will never hit (Varpulis only has `Int64` and `Float64` numeric scalars).

So: trait is object-safe, non-generic, `Option<&dyn Array>`. One branch per batch to downcast. One `AccumulatorKind` enum dispatch per aggregation per batch. This matches scenario-02 shape: 100-row batches hit one downcast, 100 hot-loop iterations.

### Accumulators — phase 1 minimum viable set

All five must produce bit-identical results (modulo float reorder) to the row-oriented `Sum`/`Avg`/`Min`/`Max`/`Count` for the same input event list. The property test in step 8 enforces this.

**`SumAccumulator`** (`arrow_aggregate/sum.rs`)
```rust
pub struct SumAccumulator {
    sums: Vec<f64>,    // one per group
    counts: Vec<u64>,  // number of non-null values per group
}
```
- `resize(n)`: `sums.resize(n, 0.0); counts.resize(n, 0);`
- `update_batch`: downcast to `Float64Array`. If input has no nulls, tight loop `for i in 0..len { sums[g[i] as usize] += arr.value(i); counts[g[i] as usize] += 1; }`. If input has nulls, consult `arr.nulls()` bitmap (use `NullBuffer::is_valid` slice) and skip NaN/null rows to match `filter(|v| !v.is_nan())` semantics of row path.
- `evaluate`: produce a `Float64Array` where groups with `counts[i] == 0` become null (to match Sum's "no valid values → 0.0" for the row path, we return 0.0 via a non-null array — see null-handling note below).

**Null / NaN semantics match (critical):** The existing `Sum::apply_refs` filters NaN: `.filter(|v| !v.is_nan())`. The new `SumAccumulator` must do the same. For the `apply_arrow` path currently in `aggregation.rs:227-238`, the behavior is "use arrow_arith::aggregate::sum which skips nulls but does NOT skip NaN", which is already a latent inconsistency with `apply_refs`. The **new columnar grouped path must match `apply_refs`** (skip NaN and null), because that is the path that currently runs on scenario 02 data (see `pipeline.rs:592` — `PartitionedAggregate` calls `apply_shared` which dispatches to `apply_refs`). Document this precisely in the module docstring.

For `Sum` specifically, the row path returns `Value::Float(0.0)` when no valid values exist — preserve that: empty groups → 0.0, not null.

**`CountAccumulator`** (`arrow_aggregate/count.rs`)
```rust
pub struct CountAccumulator {
    counts: Vec<i64>,
}
```
- Called with `values: None` (no input needed).
- `update_batch(None, g)`: `for gi in g { counts[*gi as usize] += 1; }` — a raw loop on a u32 slice, should autovectorize.
- `evaluate`: `Int64Array` from `counts`.
- Matches `Count::apply(&events, _) -> Value::Int(events.len() as i64)`: the group count is the number of events routed to that group, which is exactly what this produces.

**`AvgAccumulator`** (`arrow_aggregate/avg.rs`)
```rust
pub struct AvgAccumulator {
    sums: Vec<f64>,
    counts: Vec<u64>,
}
```
- Same `update_batch` as `SumAccumulator` (literally — consider factoring into a `SumState` helper or keeping it duplicated for clarity; I'd duplicate it for phase 1, factor in phase 3).
- `evaluate`: for each group, `if counts[i] == 0 { null } else { sums[i] / counts[i] as f64 }`. Use `Float64Array::from_iter`.
- Matches `Avg::apply_refs`: empty group → `Value::Null`.

**`MinAccumulator` / `MaxAccumulator`** (`arrow_aggregate/min_max.rs`)
```rust
pub struct MinAccumulator {
    values: Vec<f64>,      // current min per group; f64::INFINITY sentinel for "never set"
    has_value: Vec<bool>,  // explicit "seen at least one non-null value"
}
```
- `resize(n)`: `values.resize(n, f64::INFINITY); has_value.resize(n, false);`
- `update_batch`: downcast, skip nulls, skip NaN (match `apply_refs`), loop `let v = arr.value(i); let gi = g[i] as usize; if v < values[gi] { values[gi] = v; } has_value[gi] = true;`.
- `evaluate`: `Float64Array` with nulls where `has_value[i] == false`.
- `MaxAccumulator` is the same with `f64::NEG_INFINITY` and `>`.
- Matches `Min::apply_refs`: empty group → `Value::Null`.

**Why `has_value: Vec<bool>` and not just a sentinel?** Because a user can legitimately emit an event with `f64::INFINITY` as a field value, and we must not treat that as "empty group". This is a subtle null-semantic bug I want caught at review time.

### Group key encoding — `arrow-row` vs custom FxHasher

**Recommendation: `arrow-row::RowConverter`.** Here's the tradeoff:

| Dimension | `arrow-row::RowConverter` | Custom `FxHasher` over `&[&str]` |
|-----------|---------------------------|----------------------------------|
| Multi-column group keys | native support, binary-comparable rows, sorts correctly | have to build a composite key format by hand |
| Null semantics | built-in | manual |
| Type support | all Arrow types, unified encoding | we'd hand-write per-type paths |
| Cost (100-row batch, 1 column str key) | one `RowConverter::convert_columns`, then FxHashMap lookup per row | `get_str` per event into a `&str`, then FxHashMap lookup |
| Cost (single int key) | slight overhead vs direct i64 hash | direct i64 hash is tighter |
| Dep size / compile time | +1 arrow-rs crate (arrow-row) — already transitively compiled | none |
| Wasm-compatible | confirmed yes (pure arrow-rs; deps are arrow-array/buffer/data/schema + `half 2.1` with `default-features=false`) | yes |
| LOC | ~30 lines of wrapper | ~80 lines of multi-type group-key builder |

The deciding factor: phase 2 fuses the window + aggregate, and phase 3 extends to more windows. All of that benefits from `RowConverter`'s unified encoding. Also, Arroyo's path — the one we're trying to match — uses `RowConverter`, so using it closes the measurement-method gap; we can stop guessing whether our group-key hashing is meaningfully different.

**Add `arrow-row` to `Cargo.toml`.** Diff:

`crates/varpulis-runtime/Cargo.toml` line 58–62:
```toml
# Apache Arrow columnar processing
arrow-array = { version = "54", optional = true, default-features = false }
arrow-schema = { version = "54", optional = true, default-features = false }
arrow-arith = { version = "54", optional = true, default-features = false }
arrow-row = { version = "54", optional = true, default-features = false }  # NEW
```

And line 102:
```toml
arrow = ["arrow-array", "arrow-schema", "arrow-arith", "arrow-row"]
```

**Wasm confirmation (already verified above):** `arrow-row 54.3.1` has no default features, no platform-specific deps beyond the arrow-rs core. `arrow-array` transitively uses `ahash` which has explicit `compile-time-rng` for wasm32. So enabling the `arrow` feature in a wasm32 build should work as long as `chrono-tz` is not pulled (it's optional in arrow-array and we don't enable it). **Action item for the implementer:** add a `cargo build --target wasm32-unknown-unknown --no-default-features --features arrow` smoke step to CI before merging phase 1. If that fails, gate `arrow` behind `#[cfg(not(target_arch = "wasm32"))]` in `lib.rs` and ship a row-only wasm build.

### `GroupKeyEncoder` — wrapper

File `arrow_aggregate/group_keys.rs`:

```rust
use arrow_array::{ArrayRef, UInt32Array};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{DataType, Field};
use rustc_hash::FxHashMap;

pub struct GroupKeyEncoder {
    converter: RowConverter,
    // Borrowed row → group index. Keys are boxed row bytes to own them.
    index: FxHashMap<Box<[u8]>, u32>,
    next_group: u32,
}

impl GroupKeyEncoder {
    pub fn new(key_fields: &[Field]) -> Result<Self, arrow_schema::ArrowError> {
        let sort_fields: Vec<SortField> = key_fields
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        Ok(Self {
            converter: RowConverter::new(sort_fields)?,
            index: FxHashMap::default(),
            next_group: 0,
        })
    }

    /// Encode the key columns and return `(group_indices, total_groups)`.
    ///
    /// `group_indices[i]` is the group index for row `i` in the input batch.
    pub fn encode_batch(
        &mut self,
        key_columns: &[ArrayRef],
    ) -> Result<(Vec<u32>, usize), arrow_schema::ArrowError> {
        let rows: Rows = self.converter.convert_columns(key_columns)?;
        let mut out = Vec::with_capacity(rows.num_rows());
        for row in rows.iter() {
            let bytes = row.as_ref();
            let gi = match self.index.get(bytes) {
                Some(&gi) => gi,
                None => {
                    let gi = self.next_group;
                    self.index.insert(bytes.to_vec().into_boxed_slice(), gi);
                    self.next_group += 1;
                    gi
                }
            };
            out.push(gi);
        }
        Ok((out, self.next_group as usize))
    }

    pub fn total_groups(&self) -> usize {
        self.next_group as usize
    }

    /// Materialize group indices back to key column values for output.
    pub fn emit_keys(&self) -> Result<Vec<ArrayRef>, arrow_schema::ArrowError> {
        // Rebuild Rows from our stored bytes and invoke converter.convert_rows.
        // Phase 1: iterate self.index sorted by gi, collect bytes, reconstruct.
        // See RowConverter::convert_rows for exact API.
        todo!("phase 1 impl — exact API depends on RowConverter::parse_row variant")
    }
}
```

Note on `emit_keys`: in phase 1 we don't actually need to go through `RowConverter::convert_rows` for the common single-column `partition_by` case, because `PartitionedAggregatorState::apply` returns `Vec<(String, IndexMap<...>)>` — the key is a flat `String`. We can avoid the round-trip by **storing the original scalar key alongside the row bytes**:

```rust
struct GroupEntry {
    group_idx: u32,
    key_display: String,  // cached to_partition_key() of the first event that created the group
}
index: FxHashMap<Box<[u8]>, GroupEntry>,
```

This lets the phase-1 shim hand back `(String, IndexMap)` pairs without involving `convert_rows`. Phase 2 will revisit this when the output shape becomes a proper `RecordBatch`.

### `ColumnarGroupedAggregator` — one-shot driver (phase 1)

File `arrow_aggregate/grouped.rs`:

```rust
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::Field;
use indexmap::IndexMap;
use varpulis_core::Value;

use super::accumulator::ColumnarAccumulator;
use super::group_keys::GroupKeyEncoder;

pub struct ColumnarGroupedAggregator {
    /// Output aliases and (accumulator, input field name).
    specs: Vec<(String, Box<dyn ColumnarAccumulator>, Option<String>)>,
    key_field_names: Vec<String>,
    encoder: GroupKeyEncoder,
}

impl ColumnarGroupedAggregator {
    pub fn try_new(
        key_columns: &[Field],
        specs: Vec<(String, Box<dyn ColumnarAccumulator>, Option<String>)>,
    ) -> Result<Self, arrow_schema::ArrowError> {
        Ok(Self {
            specs,
            key_field_names: key_columns.iter().map(|f| f.name().clone()).collect(),
            encoder: GroupKeyEncoder::new(key_columns)?,
        })
    }

    /// Feed a RecordBatch: encode group keys, resize accumulators, update each.
    pub fn update(&mut self, batch: &RecordBatch) -> Result<(), arrow_schema::ArrowError> {
        // 1. Project key columns out of the batch by name.
        let key_cols: Vec<_> = self
            .key_field_names
            .iter()
            .map(|n| batch.column_by_name(n).cloned().ok_or_else(|| {
                arrow_schema::ArrowError::SchemaError(format!("missing key column {n}"))
            }))
            .collect::<Result<Vec<_>, _>>()?;
        let (group_indices, total_groups) = self.encoder.encode_batch(&key_cols)?;

        // 2. Resize each accumulator to total_groups.
        for (_, acc, _) in &mut self.specs {
            acc.resize(total_groups);
        }

        // 3. For each spec, project the value column and call update_batch.
        for (_, acc, field) in &mut self.specs {
            let values: Option<&dyn arrow_array::Array> = match field {
                Some(name) => batch.column_by_name(name).map(|c| c.as_ref()),
                None => None,  // Count
            };
            acc.update_batch(values, &group_indices);
        }
        Ok(())
    }

    /// Emit per-group results as `Vec<(group_key_display, IndexMap<alias, Value>)>`.
    pub fn drain_as_row_results(
        &mut self,
    ) -> Vec<(String, IndexMap<String, Value>)> {
        let total = self.encoder.total_groups();
        let mut per_spec: Vec<(String, arrow_array::ArrayRef)> = self
            .specs
            .iter_mut()
            .map(|(alias, acc, _)| (alias.clone(), acc.evaluate()))
            .collect();

        let mut out = Vec::with_capacity(total);
        // encoder exposes an iterator over (group_idx, key_display) — see GroupEntry.
        for (gi, key_display) in self.encoder.iter_keys() {
            let mut map = IndexMap::with_capacity(per_spec.len());
            for (alias, arr) in &per_spec {
                map.insert(alias.clone(), array_value_at(arr.as_ref(), gi));
            }
            out.push((key_display, map));
        }
        out
    }
}

fn array_value_at(arr: &dyn arrow_array::Array, i: usize) -> Value {
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::DataType;
    if arr.is_null(i) { return Value::Null; }
    match arr.data_type() {
        DataType::Float64 => Value::Float(arr.as_any().downcast_ref::<Float64Array>().unwrap().value(i)),
        DataType::Int64 => Value::Int(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i)),
        _ => Value::Null,
    }
}
```

### Integration into `PartitionedAggregatorState::apply` — exact diff

File `/home/cpo/cep/crates/varpulis-runtime/src/engine/types.rs:484-519`:

Add a field to `PartitionedAggregatorState`:

```rust
pub struct PartitionedAggregatorState {
    pub partition_key: String,
    pub aggregator_template: Aggregator,
    /// Schema cache reused across calls to avoid re-inferring per fire.
    #[cfg(feature = "arrow")]
    pub(crate) schema_cache: crate::arrow_bridge::SchemaCache,
}
```

Rewrite `apply` with a feature-gated fast path:

```rust
impl PartitionedAggregatorState {
    pub fn apply(
        &mut self,
        events: &[SharedEvent],
    ) -> Vec<(String, IndexMap<String, Value>)> {
        #[cfg(feature = "arrow")]
        {
            if events.len() >= crate::arrow_bridge::ARROW_BATCH_THRESHOLD
                && self.can_use_columnar_path()
            {
                if let Some(results) = self.apply_columnar(events) {
                    return results;
                }
                // fall through to row path on schema error
            }
        }
        self.apply_row(events)
    }

    fn apply_row(&mut self, events: &[SharedEvent])
        -> Vec<(String, IndexMap<String, Value>)> {
        // existing body, unchanged
        let mut partitions: FxHashMap<String, Vec<SharedEvent>> = FxHashMap::default();
        for event in events {
            let key = event.get(&self.partition_key).map_or_else(
                || "default".to_string(),
                |v| v.to_partition_key().into_owned(),
            );
            partitions.entry(key).or_default().push(Arc::clone(event));
        }
        let mut results = Vec::with_capacity(partitions.len());
        for (key, partition_events) in partitions {
            let result = self.aggregator_template.apply_shared(&partition_events);
            results.push((key, result));
        }
        results
    }

    #[cfg(feature = "arrow")]
    fn can_use_columnar_path(&self) -> bool {
        // Reject the columnar path if the Aggregator contains any accumulator
        // we haven't implemented yet (StdDev, First, Last, CountDistinct, Ema,
        // Percentile/Median/P*). Inspect self.aggregator_template.aggregations
        // via a new `supported_for_columnar()` helper on Aggregator that
        // returns `true` iff every entry is Sum/Avg/Min/Max/Count.
        self.aggregator_template.supported_for_columnar()
    }

    #[cfg(feature = "arrow")]
    fn apply_columnar(
        &mut self,
        events: &[SharedEvent],
    ) -> Option<Vec<(String, IndexMap<String, Value>)>> {
        use crate::arrow_aggregate::{
            grouped::ColumnarGroupedAggregator, accumulator::make_accumulator_for,
        };
        // 1. Infer schema & build RecordBatch for all events.
        let schema = self.schema_cache.get_or_infer(events);
        let batch = crate::arrow_bridge::events_to_record_batch(events, &schema).ok()?;

        // 2. Build the aggregator with exactly the accumulators the Aggregator declares.
        let key_field = schema.field_with_name(&self.partition_key).ok()?.clone();
        let specs = self
            .aggregator_template
            .iter_specs()  // new method — see below
            .map(|(alias, func_name, field)| {
                let acc = make_accumulator_for(func_name)?;
                Some((alias.clone(), acc, field.clone()))
            })
            .collect::<Option<Vec<_>>>()?;
        let mut agg = ColumnarGroupedAggregator::try_new(&[key_field], specs).ok()?;

        // 3. Feed the batch (one call in phase 1) and drain.
        agg.update(&batch).ok()?;
        Some(agg.drain_as_row_results())
    }
}
```

Helper additions to `Aggregator` in `aggregation.rs`:

```rust
impl Aggregator {
    /// True iff every registered aggregation function has a ColumnarAccumulator
    /// implementation in phase 1 (Sum, Avg, Min, Max, Count).
    #[cfg(feature = "arrow")]
    pub fn supported_for_columnar(&self) -> bool {
        self.aggregations
            .iter()
            .all(|(_, f, _)| matches!(f.name(), "sum" | "avg" | "min" | "max" | "count"))
    }

    /// Expose the list of (alias, func_name, field) tuples so the columnar
    /// driver can construct matching accumulators.
    #[cfg(feature = "arrow")]
    pub fn iter_specs(&self) -> impl Iterator<Item = (&String, &'static str, &Option<String>)> {
        self.aggregations.iter().map(|(alias, f, field)| {
            // All phase-1 aggregate funcs return a &'static str from name().
            let name: &'static str = match f.name() {
                "sum" => "sum",
                "avg" => "avg",
                "min" => "min",
                "max" => "max",
                "count" => "count",
                other => other,  // caller gated via supported_for_columnar
            };
            (alias, name, field)
        })
    }
}
```

Factory in `arrow_aggregate/accumulator.rs`:

```rust
pub fn make_accumulator_for(func_name: &str) -> Option<Box<dyn ColumnarAccumulator>> {
    match func_name {
        "sum"   => Some(Box::new(crate::arrow_aggregate::sum::SumAccumulator::default())),
        "avg"   => Some(Box::new(crate::arrow_aggregate::avg::AvgAccumulator::default())),
        "min"   => Some(Box::new(crate::arrow_aggregate::min_max::MinAccumulator::default())),
        "max"   => Some(Box::new(crate::arrow_aggregate::min_max::MaxAccumulator::default())),
        "count" => Some(Box::new(crate::arrow_aggregate::count::CountAccumulator::default())),
        _ => None,
    }
}
```

### Schema inference — is it sufficient?

Current `SchemaCache::get_or_infer` at `arrow_bridge.rs:46` caches by `event_type`, and infers fields from the first event. **Sufficient for phase 1 with one caveat:** if `device_id` is sometimes a `Value::Int` and sometimes a `Value::Str` within the same event type, the first event's type wins and subsequent events' strings get null-coerced into an `Int64Array`. That's broken, but it's **not a regression** — the row path also breaks on heterogeneous types via `to_partition_key().into_owned()` where `Int` becomes `"42"` and `Str` becomes `"42"` and they silently collide.

So: phase 1 inherits the existing schema cache's limitations. Do NOT try to fix the heterogeneous-type case in phase 1; call it out in the module docstring and add a test that pins the current behavior. Revisit in a later phase with `UnionArray` if it ever actually bites a user.

**What needs extension for multi-column group keys (post-phase-2, when we support `.partition_by(a, b)`):** the `GroupKeyEncoder` already accepts `&[Field]`. The schema inference side will need a way to pick out multiple key fields by name from the inferred schema — trivial extension via the existing `schema.field_with_name` loop.

### Tests (phase 1)

New file `/home/cpo/cep/crates/varpulis-runtime/tests/arrow_aggregate_tests.rs`:

1. **Unit tests per accumulator** (under `#[cfg(feature = "arrow")]`):
   - `SumAccumulator` with 3 groups, 10 rows each, verify `sums[gi]` and `counts[gi]`.
   - `AvgAccumulator` with one empty group → null output.
   - `MinAccumulator` / `MaxAccumulator` with NaN input (skipped) and infinity value (kept).
   - `CountAccumulator` with `values: None`.
   - Null handling: Float64Array with a nulls buffer, assert counts reflect non-null only.
   - Resize behavior: resize(5) then resize(10) grows without loss.

2. **`GroupKeyEncoder` roundtrip:** build columns → encode → assert unique group count, encode second batch with overlapping keys → assert same indices reused.

3. **`ColumnarGroupedAggregator` integration:**
   - Build a RecordBatch with `device_id: Utf8, temperature: Float64`.
   - 100 events, 10 devices, verify `(device, sum, avg, min, max, count)` per group.

4. **Property test** — the critical regression gate. New file `/home/cpo/cep/crates/varpulis-runtime/tests/proptest_arrow_aggregate.rs`:

```rust
// pseudocode sketch, under #[cfg(feature = "arrow")]
proptest! {
    #[test]
    fn columnar_matches_row(
        events in arb_events_with_partition_key(100, 10_000)
    ) {
        let mut row_state = PartitionedAggregatorState::new(
            "device_id".into(),
            Aggregator::new()
                .add("s", Box::new(Sum), Some("v".into()))
                .add("a", Box::new(Avg), Some("v".into()))
                .add("mn", Box::new(Min), Some("v".into()))
                .add("mx", Box::new(Max), Some("v".into()))
                .add("c", Box::new(Count), None),
        );
        let row_out = row_state.apply_row(&events);  // force row path
        let col_out = row_state.apply_columnar(&events).unwrap();  // force columnar path
        assert_equivalent(row_out, col_out, /* float tol */ 1e-9);
    }
}
```

The `assert_equivalent` helper normalizes both outputs to a `BTreeMap<String, BTreeMap<String, Value>>` (sorted by partition key, sorted by alias) and compares `Value::Float` via tolerance. This catches ordering drift and float accumulation drift.

5. **Regression test matching scenario 02 shape** — in the same file:

```rust
#[test]
#[cfg(feature = "arrow")]
fn scenario_02_shape_matches() {
    // 100 devices, 1000 seconds, 1 event per (device, second)
    let events = build_scenario_02_events(100, 1000);
    let mut row = new_state();
    let mut col = new_state();
    let a = row.apply_row(&events);
    let b = col.apply_columnar(&events).unwrap();
    assert_equivalent(a, b, 1e-9);
}
```

6. **Microbenchmark** — new file `/home/cpo/cep/crates/varpulis-runtime/benches/columnar_agg_benchmark.rs`, registered in `Cargo.toml` with `required-features = ["arrow"]`:

```rust
// criterion benchmarks comparing row vs columnar on:
// - 10k events, 10 groups
// - 10k events, 100 groups
// - 100k events, 100 groups  (scenario 02 shape)
// - 1M events, 10k groups
// with Sum + Avg + Min + Max + Count all active.
```

Add to `Cargo.toml`:
```toml
[[bench]]
name = "columnar_agg_benchmark"
harness = false
required-features = ["arrow"]
```

### Phase 1 — key files touched

Creates:
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/mod.rs`
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/accumulator.rs`
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/sum.rs`
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/avg.rs`
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/min_max.rs`
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/count.rs`
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/group_keys.rs`
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/grouped.rs`
- `/home/cpo/cep/crates/varpulis-runtime/tests/arrow_aggregate_tests.rs`
- `/home/cpo/cep/crates/varpulis-runtime/tests/proptest_arrow_aggregate.rs`
- `/home/cpo/cep/crates/varpulis-runtime/benches/columnar_agg_benchmark.rs`

Edits:
- `/home/cpo/cep/crates/varpulis-runtime/Cargo.toml` — add `arrow-row`, extend `arrow` feature
- `/home/cpo/cep/crates/varpulis-runtime/src/lib.rs` — register `arrow_aggregate` mod under `#[cfg(feature = "arrow")]`
- `/home/cpo/cep/crates/varpulis-runtime/src/engine/types.rs:484-519` — `PartitionedAggregatorState::apply` rewrite with feature-gated fast path, add `schema_cache` field
- `/home/cpo/cep/crates/varpulis-runtime/src/aggregation.rs:993-1066` — add `supported_for_columnar()` and `iter_specs()` helpers on `Aggregator`

### Phase 1 — benchmark validation (acceptance)

After the PR is green, run:
```
python benchmarks/arroyo-comparison/run_benchmark.py \
  --scenario 02_aggregation --runs 3 \
  --varpulis-features 'kafka,arrow'
```

Target: V/A ≥ 0.80 (i.e. Varpulis within 20% of Arroyo on scenario 02). Expected: 0.85–1.00. Reasoning: the row path on scenario 02 was ~50k eps, dominated by the per-event String+probe. Eliminating that leaves us with arrow conversion (one batch per fire, 100 events) + columnar aggregation (5 µs-class per batch). The remaining gap is kafka I/O and event decoding, which is not scenario-specific and is shared with scenario 01.

If we don't hit ≥ 0.80, the next bottleneck is almost certainly that `PartitionedTumblingWindow::add_shared` still does a per-event String allocation in the router, and that `pipeline.rs:540-547` rebuilds a RecordBatch per partition fire (one fire per window boundary). That's exactly what phase 2 fixes.

---

## Phase 2 — Fuse `PartitionedTumblingWindow` + `PartitionedAggregate` into a streaming bin-keyed columnar op

### Goal

Arroyo's real advantage over phase-1 Varpulis is that its accumulator state lives **across arrival batches within a window**: as events flow in, the group state for `(current_bin, group_idx)` is updated in place; fire time is a flush + emit. Phase 1 rebuilds the RecordBatch and re-encodes keys **once per fire**. Phase 2 removes that by letting the accumulator state survive across arrivals and only evaluating at window-close time.

This is what closes the last 5–15% of the Arroyo gap on scenarios where batches are small (e.g. 1 event per partition per window, like scenario 02).

### Approach

Introduce a new runtime op variant that holds both the bin routing and the per-bin `ColumnarGroupedAggregator`:

```rust
// types.rs additions
#[cfg(feature = "arrow")]
pub struct PartitionedWindowedColumnarAggregate {
    pub partition_key: String,
    pub bin_duration: chrono::Duration,
    /// Per-bin accumulator state. Bin identity = window start timestamp (truncated to duration).
    pub bins: std::collections::BTreeMap<i64, crate::arrow_aggregate::streaming::StreamingBinState>,
    /// Schema once inferred, held for the lifetime of the op.
    pub schema: Option<std::sync::Arc<arrow_schema::Schema>>,
    pub schema_cache: crate::arrow_bridge::SchemaCache,
    /// Template used to build a new StreamingBinState per new bin.
    pub template: crate::arrow_aggregate::streaming::StreamingAggregatorTemplate,
    /// Current watermark for flushing closed bins.
    pub watermark: Option<chrono::DateTime<chrono::Utc>>,
}

pub enum RuntimeOp {
    ...existing variants...
    #[cfg(feature = "arrow")]
    PartitionedWindowedColumnarAggregate(PartitionedWindowedColumnarAggregate),
}
```

And in `arrow_aggregate/streaming.rs`:

```rust
pub struct StreamingBinState {
    pub(crate) aggregator: ColumnarGroupedAggregator,
    pub(crate) bin_start_ms: i64,
    pub(crate) bin_end_ms: i64,
}

pub struct StreamingAggregatorTemplate {
    pub key_field_name: String,
    /// Alias, accumulator kind (so we can instantiate fresh per bin), input field.
    pub specs: Vec<(String, AccumulatorKind, Option<String>)>,
}

pub enum AccumulatorKind { Sum, Avg, Min, Max, Count }
```

### Hot path — what executes per arriving event batch

1. `pipeline.rs` match arm for `PartitionedWindowedColumnarAggregate(state)`:
   1. Buffer `current_events` by bin bucket (`floor(ts_ms / bin_ms) * bin_ms`). Use a small scratch `FxHashMap<i64, Vec<usize>>` mapping bin → event indices.
   2. For each bin bucket, take the corresponding events, convert to a RecordBatch via `state.schema_cache.get_or_infer(events)` + `events_to_record_batch` (same call as phase 1), and feed into `state.bins.entry(bin_start).or_insert_with(|| template.new_bin_state(...))).aggregator.update(&batch)`.
   3. Advance watermark: `watermark = max(watermark, max_event_ts)`.
   4. Flush any bin whose `bin_end_ms + allowed_lateness < watermark_ms`: drain it via `drain_as_row_results()` (reusing phase-1 code) and emit rows into `current_events` for downstream ops.

The key performance win: **the accumulator state `sums[gi] += v` lives across arrivals**. When scenario 02 flows 1 event/partition/window in at a time, phase 2 does `O(1)` work per event (one downcast per batch, one row loop), whereas phase 1 builds a 100-row RecordBatch at window close, re-encodes all 100 group keys, allocates 100 Vec<SharedEvent>s — there's nothing inherently wrong with that but it's unnecessary given that the fusion is available.

### Per-event batch splitting by bin — design detail

Scenario 02 has `1s` windows with events 10ms apart. A single arriving `current_events` batch (say 100 events) can span at most 2–3 bin boundaries. So the "split by bin" step is cheap: a linear scan bucketing event indices, producing 1–3 small sub-batches. **Crucially:** we do NOT re-convert `current_events` to an Arrow RecordBatch up-front — we slice `current_events` into sub-slices by bin index and call `events_to_record_batch` per sub-slice. The existing schema cache makes schema inference O(1) after the first call.

Edge case: events arriving out of order (late within allowed_lateness). The `BTreeMap<i64, StreamingBinState>` handles this naturally — a late event hits an older bin that hasn't been flushed yet and updates its accumulator in place.

### Fusion in the compiler — exact diff

File `/home/cpo/cep/crates/varpulis-runtime/src/engine/compilation.rs:837-852`:

The current `StreamOp::Aggregate` branch pushes either `RuntimeOp::Aggregate` or `RuntimeOp::PartitionedAggregate`. We extend it to check the **previously pushed op** and fuse if it's a `PartitionedTumbling` window:

```rust
StreamOp::Aggregate(items) => {
    let mut aggregator = Aggregator::new();
    for item in items {
        if let Some((func, field)) = compiler::compile_agg_expr(&item.expr) {
            aggregator = aggregator.add(item.alias.clone(), func, field);
        }
    }

    #[cfg(feature = "arrow")]
    {
        // Fusion: if the previously pushed op is a PartitionedTumbling window
        // and the aggregator uses only columnar-supported funcs, replace the
        // window + aggregate pair with a single streaming columnar op.
        if let Some(ref key) = partition_key {
            if aggregator.supported_for_columnar() {
                if let Some(RuntimeOp::Window(WindowType::PartitionedTumbling(w))) =
                    runtime_ops.last()
                {
                    let duration = w.duration();  // new pub fn on PartitionedTumblingWindow
                    let partition_key = key.clone();
                    // Pop the window op.
                    runtime_ops.pop();
                    // Push the fused op.
                    let template = StreamingAggregatorTemplate::from_aggregator(
                        partition_key.clone(),
                        &aggregator,
                    );
                    runtime_ops.push(RuntimeOp::PartitionedWindowedColumnarAggregate(
                        PartitionedWindowedColumnarAggregate::new(
                            partition_key,
                            duration,
                            template,
                        ),
                    ));
                    continue;  // skip the regular push below
                }
            }
        }
    }

    // Fallback path (unchanged):
    if let Some(ref key) = partition_key {
        runtime_ops.push(RuntimeOp::PartitionedAggregate(
            PartitionedAggregatorState::new(key.clone(), aggregator),
        ));
    } else {
        runtime_ops.push(RuntimeOp::Aggregate(aggregator));
    }
}
```

**Evaluation of alternatives to inline-fusion-in-compiler:**

- *Post-compilation fusion pass:* A separate pass over `runtime_ops` would also work, and is arguably cleaner because fusion logic is decoupled. But we don't have any other fusion passes today, so adding a pass infrastructure just for this one rule is over-engineering. Recommendation: inline in compiler.rs as above. If a second fusion rule appears in a future PR, factor out a `fn fuse_columnar_pairs(ops: &mut Vec<RuntimeOp>)` helper then.
- *Window op emits RecordBatches directly:* Would require changing `current_events: Vec<SharedEvent>` to a `DynBatch` enum across the entire pipeline driver. Massive blast radius. Rejected.
- *Keep ops separate but pass hidden state via channel:* Fragile, hard to reason about. Rejected.

**Inline-fusion-in-compiler wins** because (1) it's 30 lines of change, (2) it preserves the invariant that `runtime_ops: Vec<RuntimeOp>` is a flat sequence, (3) it keeps the operator boundary exactly where the VPL user wrote it semantically, and (4) CEP operators (`Sequence`, `Pattern`, `Forecast`, `TrendAggregate`) are never pushed between the window op and the aggregate op — the compiler always inserts those earlier — so the "adjacent in runtime_ops" check is both necessary and sufficient.

**Guard against CEP in between:** The `runtime_ops.last()` check above implicitly ensures no CEP op sits between. If in the future someone inserts a `Process` op between them (e.g. `.window(1s).where(x>0).aggregate(...)`), `runtime_ops.last()` will not be a window and fusion is skipped — safe fallback.

### Pipeline dispatcher — new match arm

File `/home/cpo/cep/crates/varpulis-runtime/src/engine/pipeline.rs` near line 612 (after `PartitionedAggregate`):

```rust
#[cfg(feature = "arrow")]
RuntimeOp::PartitionedWindowedColumnarAggregate(state) => {
    let ts = current_events.last().map(|e| e.timestamp);
    let results = state.ingest_and_flush(current_events);
    *current_events = results
        .into_iter()
        .map(|(partition_key, result)| {
            let mut agg_event = Event::new("AggregationResult");
            if let Some(ts) = ts { agg_event.timestamp = ts; }
            agg_event.data.insert("_partition".into(), Value::Str(partition_key.into()));
            for (key, value) in result {
                agg_event.data.insert(key.into(), value);
            }
            Arc::new(agg_event)
        })
        .collect();
}
```

The fused op's `ingest_and_flush` bundles "split by bin → update accumulators → flush closed bins" and returns the row-shaped `Vec<(String, IndexMap<_, _>)>` so the downstream pipeline is untouched.

**Watermark sourcing:** Phase 2 uses event-time watermark = latest seen event timestamp. That matches the existing `PartitionedTumblingWindow` which uses the triggering event's timestamp to close windows. Actual `.watermark(...)` declarations are handled separately via a later phase (not this one).

### Phase 2 — key files touched

Creates:
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/streaming.rs`

Edits:
- `/home/cpo/cep/crates/varpulis-runtime/src/engine/types.rs` — new `PartitionedWindowedColumnarAggregate` struct, new `RuntimeOp` variant
- `/home/cpo/cep/crates/varpulis-runtime/src/engine/pipeline.rs` — new match arm around line 612, plus an entry in the `op_description` and `enabled_for_flags` helpers around lines 162/240 to route the new op correctly
- `/home/cpo/cep/crates/varpulis-runtime/src/engine/compilation.rs:837-852` — fusion logic (as sketched)
- `/home/cpo/cep/crates/varpulis-runtime/src/window.rs` — add `pub fn duration(&self) -> Duration` accessor on `PartitionedTumblingWindow` (today it's private)
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/mod.rs` — add `pub(crate) mod streaming;`
- `/home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/accumulator.rs` — add `AccumulatorKind` enum and `make_from_kind` for the streaming template

Tests:
- Extend `tests/arrow_aggregate_tests.rs` with a streaming-mode test that feeds 1 event at a time, 100 partitions × 1000 bins, and asserts the fused op's output matches the phase-1 batched path.
- Extend the property test to also compare the fused op against the row path.
- Add an explicit late-event test (event with timestamp falling into a previous bin before that bin is flushed).

### Phase 2 — benchmark acceptance

Rerun scenario 02 after phase 2. Target: V/A ≥ 0.90. If not, the remaining gap is scenario-independent Kafka I/O overhead (which is scenario 01 + 04's problem) and the plan should note it for a separate investigation.

---

## Phase 3 — Extend to `Aggregate` (non-partitioned), `BinnedSliding`, session windows

### Sketch

**`Aggregate` (non-partitioned):** Treat it as "single-group" grouped aggregation. Use `GroupKeyEncoder` with an empty key field list, always returning `group_idx = 0`. `ColumnarGroupedAggregator` already handles `total_groups = 1` with no special casing. This is ~20 lines in a new `non_partitioned_path` helper, behind the same `arrow` feature gate. The existing `pipeline.rs:563-577` already has an Arrow fast path for `Aggregate` via `apply_arrow`; phase 3 *replaces* it with the columnar grouped path so that `Count + Sum + Avg + Min + Max` in one aggregator can share the batch conversion (today each of those rescans the batch independently via the trait).

**`BinnedSliding` / `PartitionedBinnedSliding`:** These are already bin-oriented internally. The fusion trick works identically — fuse the binned window with its aggregate. The only new code is routing the bin boundaries correctly (binned sliding emits every slide, not every window close). Reuse `StreamingBinState` with a different flush policy.

**Session windows:** Harder, because session boundaries are data-driven and per-partition. Punt: keep session windows row-oriented and fall back to `apply_row`. Add a one-line guard in the compiler fusion check that rejects session windows.

**`StdDev`:** Welford's algorithm requires per-group `(count, mean, m2)` state. Straightforward accumulator, add in phase 3 if there is demand. Not scenario-02-critical.

Keep this phase small and focused. ~400 LOC.

---

## Phase 4 — `arrow::compute::filter` for `.where(...)` and projections for `.emit(...)`

### Sketch

`.where(expr)` today iterates events and calls `evaluator::eval_expr_with_functions` per event (see `pipeline.rs:466-478`). For pure-columnar predicates (comparisons against float/int/str literals, AND/OR of columnar predicates), we can:
1. Try to "columnarize" the where expression: recursively walk the AST, accept `Compare(Ident, Literal, op)`, `And`, `Or`, `Not`; reject UDFs, dynamic lookups, sequence context access.
2. If columnarizable, build an `arrow_arith::cmp` expression and use `arrow::compute::filter_record_batch`.
3. Fall back to row-oriented for anything else.

This is a batch-local change and doesn't need fusion. Gate behind `arrow`. Expected gain on scenario 01 (pure filter): 20–40%. Don't commit to a number until phase 1 ships and we re-measure the new scenario 01 baseline.

**`.emit(...)` / `.select(...)`:** when all `(output_name, expr)` tuples are pure column projections (including arithmetic on columns), use `arrow::compute` slice ops to build the output batch in one shot, then convert back via `record_batch_to_events`. Skip when any expr requires row context.

Phase 4 is ~600 LOC, spread across a new `arrow_projection.rs` module and pipeline dispatcher edits.

---

## Phase 5 — Columnar hash join (deferred indefinitely)

The existing `JoinBuffer` in `crates/varpulis-runtime/src/join.rs` is row-oriented and works well for small buffered joins. A columnar hash join using `arrow-row` for the build side would match Arroyo's hash join performance, but:
1. Joins are rarer than aggregations in CEP workloads.
2. The LOC cost is an order of magnitude larger than phases 1–4 combined.
3. Scenario 03 (join benchmark) is not where we are being out-performed. Wait for measurement first.

**Recommendation:** do not schedule phase 5 yet. Revisit only if (a) scenario 03 shows Varpulis underwater after phases 1–2, or (b) a user hits it in production.

---

## Phasing — validation of the proposed sequence

Your proposed sequence is:
1. Phase 1: trait + accumulators + `PartitionedAggregatorState::apply` integration
2. Phase 2: fuse window + aggregate (streaming)
3. Phase 3: extend to non-partitioned, binned sliding, session
4. Phase 4: filter + projection
5. Phase 5: columnar hash join (deferred)

**I endorse this ordering.** Two small refinements:

1. **Phase 1 is a credibility win even if it doesn't fully close the gap.** Target V/A ≥ 0.80 on scenario 02. Merge phase 1 only after that target is validated locally; if it's below 0.80, the bottleneck is the `events_to_record_batch` conversion itself, which means phase 1 needs a pre-sized builder pool (see risks) before phase 2 is even worth starting.

2. **Phase 2 should be merged separately from phase 1**, even if it's a one-week gap. Benefits: smaller PR, easier to bisect if a benchmark regresses on unrelated scenarios, easier to revert one without the other. The fusion logic is the riskiest change because it edits both `compilation.rs` (compile-time) and `pipeline.rs` (runtime) in tandem, so isolating it is valuable.

3. **Phase 3 could be skipped** if scenario benchmarks only exercise tumbling windows. Check what the user workloads actually use before investing the ~400 LOC.

---

## Risks and mitigations

### Compile time and binary size

Adding `arrow-row` pulls no new transitive deps beyond what `arrow-array`/`arrow-schema` already pull. Measured cost delta: ~5–10s on a cold compile, ~1–2 MB binary size. **Mitigation:** keep `arrow` feature off by default for pure-sync wasm builds; the `async-runtime` default already pulls much larger deps (tokio, reqwest), so the relative cost is negligible for CLI/server builds.

### Default feature flag decision

**Recommendation for phase 1: flip the default to include `arrow`.**

Arguments for:
- varpulis-cli server builds and the web-ui preview mode get the fast path for free.
- CI coverage for the `arrow` feature becomes the common path, which catches regressions faster than a feature matrix.
- Scenario 02 benchmarks default to the fast path so marketing numbers are not a surprise for downstream users.

Arguments against:
- Wasm build: `cargo build --target wasm32-unknown-unknown --features wasm` — today does not enable `arrow`, so flipping the default wouldn't break it *if we carefully model the feature set*. Specifically, `wasm = []` as-is means "use no default features". A user doing `--no-default-features --features wasm` gets no arrow. A user doing `--features wasm` with defaults still on gets the `async-runtime` default, which is already wasm-incompatible — so `--features wasm` is always paired with `--no-default-features`. Flipping the default to include `arrow` changes nothing for wasm users as long as they already use `--no-default-features`.
- Compile-time hit for users who don't use aggregation: ~5–10s. Minor.

**Mitigation:** Add a CI job that builds `wasm32-unknown-unknown` with `--no-default-features --features wasm,arrow` to catch any cross-cutting wasm issues. If that job stays green through phase 2, flip `default = ["async-runtime", "arrow"]` in phase 3 once the feature is fully exercised.

**Concrete recommendation: do NOT flip the default in phase 1.** Ship phase 1 with `arrow` still opt-in, measure it thoroughly via benchmarks with explicit `--features arrow`, and flip the default in phase 3 or as a dedicated follow-up PR. The reason is that the benchmark script already enables `arrow` explicitly via `--features 'kafka,arrow'`, so phase 1 doesn't need the default flip to close the Arroyo gap. Flipping the default is a separate question and deserves its own PR.

### Semantic drift between row and columnar paths

**Null handling:**
- Row `Sum::apply_refs` skips NaN and missing via `get_float` returning `None`. Match this in `SumAccumulator::update_batch` by consulting both `arr.nulls()` and `arr.value(i).is_nan()`.
- Row `Sum::apply_arrow` (existing) uses `arrow_arith::aggregate::sum` which skips nulls but NOT NaN. This is already a latent inconsistency; the new `SumAccumulator` is authoritative going forward, and we should update the old `apply_arrow` path in a follow-up PR to match.

**Integer overflow:**
- Row `Sum::apply_refs` returns `f64`, so it never overflows (only loses precision). The `SumAccumulator` does the same. Safe.
- `Count` uses `i64`, and will overflow at 9.2 quintillion. Same limit as the row path. Safe.

**NaN ordering:**
- Row `Min::apply_refs` filters NaN before comparing; infinity is kept. Match this in `MinAccumulator`.
- Pinned by the property test.

**Missing fields:**
- Row path: `get_float` returns `None` → skipped.
- Columnar: `events_to_record_batch` inserts null for missing fields → skipped via the nulls buffer.
- Pinned by the property test over events with randomly-missing fields.

### `events_to_record_batch` allocation cost

The existing `arrow_bridge::events_to_record_batch` allocates four builders per column (Float64Builder, Int64Builder, etc.) per call. For phase 2 streaming, this runs on every arriving batch, every bin bucket, potentially thousands of times per second. **Mitigation:** benchmark first. If this shows up in the profile, add a builder pool to `SchemaCache` keyed by (event_type, batch_size_bucket) in a follow-up; do not optimize preemptively.

### `schema_cache` shared mutability

`PartitionedAggregatorState` has exclusive `&mut self` in `apply`, so the new `schema_cache` field is safe. For phase 2's `PartitionedWindowedColumnarAggregate`, same story — the op has `&mut self` via the pipeline dispatcher. No locking needed.

### Panics on unexpected schema

`apply_columnar` uses `?` / `.ok()` throughout and falls back to the row path on any error. **Mitigation:** make sure the property test includes events with fields of the wrong type (e.g. sometimes int, sometimes float) so the fallback path is exercised.

### Scenario 02 benchmark reproducibility

The benchmark flows through Kafka, so local runs will show noise on the order of ±3–5%. **Mitigation:** `--runs 3` takes the median. Report median + min/max.

---

## Summary of acceptance criteria

- **Phase 1 ship bar:** property test passes, scenario 02 V/A ≥ 0.80, no regression on scenarios 01/03/04 (within noise), wasm32 build still compiles under `--features wasm` (without arrow).
- **Phase 2 ship bar:** streaming fusion produces bit-identical results to phase 1 batched path (property test), scenario 02 V/A ≥ 0.90, no regression on non-partitioned aggregation benchmarks.
- **Phase 3 ship bar:** optional, driven by benchmark needs.

---

### Critical Files for Implementation

- /home/cpo/cep/crates/varpulis-runtime/src/engine/types.rs  (`PartitionedAggregatorState::apply`, new `PartitionedWindowedColumnarAggregate` struct, `RuntimeOp` variant)
- /home/cpo/cep/crates/varpulis-runtime/src/engine/compilation.rs  (fusion logic around lines 837-852)
- /home/cpo/cep/crates/varpulis-runtime/src/engine/pipeline.rs  (new dispatcher arm around line 612)
- /home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/grouped.rs  (new — phase 1 entry point)
- /home/cpo/cep/crates/varpulis-runtime/src/arrow_aggregate/streaming.rs  (new — phase 2 fused-op state)