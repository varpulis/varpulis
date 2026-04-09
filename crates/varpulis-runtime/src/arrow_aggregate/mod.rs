//! Streaming columnar grouped aggregation.
//!
//! This module provides a vectorized, group-by aggregator built directly on
//! the `arrow-rs` primitives (`arrow-array`, `arrow-row`, `arrow-schema`).
//! It intentionally does NOT depend on DataFusion — we use a narrow slice of
//! the relational operator vocabulary (grouped sum/avg/min/max/count over a
//! `RecordBatch`) and hand-rolling it is cheaper than pulling in a SQL
//! query engine as a transitive dependency.
//!
//! ## Why this module exists
//!
//! The row-oriented `PartitionedAggregatorState::apply` in `engine/types.rs`
//! pays a `String` allocation + `FxHashMap` probe + `Arc::clone` per event,
//! which dominates throughput on sparse-per-group data shapes (e.g. one
//! event per `(time_bin, device_id)` group). Arroyo's equivalent path is
//! one bulk `arrow_row::RowConverter` hash over the group-key slice +
//! per-group accumulator updates indexed by `group_idx` — 5–10× faster.
//!
//! ## What stays row-oriented
//!
//! CEP operators (`Sequence`, `Pattern`, `TrendAggregate`, `Forecast`,
//! `Enrich`, `Process`, WASM UDFs, `JoinBuffer`) are NOT touched. This
//! module is strictly for the "windowed group-by" shape:
//! `[partition_by →] window → aggregate [→ having]`.
//!
//! ## Design
//!
//! The entry point for phase 1 is [`grouped::ColumnarGroupedAggregator`],
//! a one-shot driver that takes a `RecordBatch`, hashes its group-key
//! columns via [`group_keys::GroupKeyEncoder`] (an `arrow_row::RowConverter`
//! wrapper), resizes each [`accumulator::ColumnarAccumulator`] to the new
//! group count, and updates them in tight loops over the Arrow arrays.
//!
//! Phase 2 will add a streaming driver that keeps accumulator state alive
//! across arriving batches per `(bin, group_idx)` for the tumbling-window
//! fused operator. See `docs/development/columnar-aggregation-plan.md`.

#![cfg(feature = "arrow")]

pub(crate) mod accumulator;
pub(crate) mod avg;
pub(crate) mod count;
pub(crate) mod group_keys;
pub(crate) mod grouped;
pub(crate) mod min_max;
pub(crate) mod streaming;
pub(crate) mod sum;

pub(crate) use accumulator::make_accumulator_for;
pub(crate) use grouped::ColumnarGroupedAggregator;
// `AccumulatorKind` / `ColumnarAccumulator` trait + `AggSpec` are referenced
// through the `arrow_aggregate::grouped::*` / `arrow_aggregate::accumulator::*`
// paths by call sites that need them (wire-in in `engine/types.rs`,
// phase-2 streaming driver). No re-export at the module root yet.
