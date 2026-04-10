//! The [`ColumnarAccumulator`] trait and the factory that builds one from an
//! aggregate-function name.
//!
//! Design: trait is object-safe (no generics, no associated types), takes
//! `Option<&dyn Array>` so `Count` — which needs no input column — doesn't
//! need a special-case. One downcast happens per *batch*, not per event;
//! the hot loop body is the typed-slice update.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;

/// A grouped accumulator that maintains per-group state across batches.
///
/// Modelled loosely on DataFusion's `GroupsAccumulator` but stripped to
/// the Varpulis use case: one value column, `f64`-typed numerics with
/// `Int64 → f64` coercion, no filter mask, no merge_batch (we only run
/// the partial/full aggregate in one place).
///
/// State layout is up to the implementation — typically a `Vec<f64>` or
/// `Vec<u64>` indexed by `group_idx`. Implementations MUST tolerate
/// repeated `resize(n)` calls where `n` monotonically increases (new
/// groups arriving across batches).
pub(crate) trait ColumnarAccumulator: Send + Sync {
    /// Grow internal storage so that group index `total_groups - 1` is valid.
    ///
    /// Called once per batch after [`super::group_keys::GroupKeyEncoder`]
    /// returns the new total group count. Must not shrink state — monotonic.
    fn resize(&mut self, total_groups: usize);

    /// Fold `values` into per-group state, using `group_indices[i]` as the
    /// destination group index for row `i`.
    ///
    /// `group_indices.len() == values.as_ref().map(|a| a.len()).unwrap_or(0)`
    /// when `values` is `Some`. For accumulators that don't need an input
    /// column (`Count`), `values` is `None` and the accumulator consults
    /// `group_indices.len()` instead.
    ///
    /// Implementations MUST handle NaN the same way the row-oriented path
    /// does: NaN values are skipped (treated as absent). Null entries in
    /// the input array are likewise skipped (NOT counted).
    fn update_batch(&mut self, values: Option<&dyn Array>, group_indices: &[u32]);

    /// Produce the final per-group output array, one entry per group index
    /// in `0..self.num_groups()` (the last value passed to `resize`).
    ///
    /// After calling `evaluate`, the accumulator's allocations may be
    /// reused on the next `resize` — callers must not depend on
    /// state being drained.
    fn evaluate(&mut self) -> ArrayRef;

    /// Update a single group with a single scalar value. This is the
    /// **fast path** for per-event updates that bypass Arrow
    /// `RecordBatch` construction. Called by the streaming op when the
    /// incoming batch has exactly 1 event — the value is extracted
    /// directly from the `Event`'s field via `get_float()`.
    ///
    /// `value` is `None` for events where the field is missing or NaN
    /// (the caller pre-filters). `Count` ignores `value` entirely.
    fn update_single(&mut self, group_idx: u32, value: Option<f64>);

    /// Read the scalar value for a single group WITHOUT building an
    /// Arrow array. This is the fast drain path used when
    /// `drain_as_row_results` is called after `update_single`-only
    /// sessions — avoids the Arrow array roundtrip of `evaluate() →
    /// array_value_at()`.
    ///
    /// Returns `Value::Float`, `Value::Int`, or `Value::Null` depending
    /// on the accumulator type and whether the group has data.
    fn drain_single(&self, group_idx: u32) -> varpulis_core::Value;

    /// Human-readable name (`"sum"`, `"avg"`, …) used for error messages
    /// and the debug output of the aggregator itself. Unused on the hot
    /// path in phase 1; phase 2's streaming driver needs it for the
    /// per-bin state logger.
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// Expected input `DataType`. `None` for accumulators that don't take
    /// an input column (`Count`). Unused on the hot path in phase 1;
    /// phase 2's streaming driver uses it to validate incoming batches
    /// against the pre-built template.
    #[allow(dead_code)]
    fn input_type(&self) -> Option<DataType>;
}

/// Tag enum used by the streaming driver (phase 2) to instantiate a fresh
/// accumulator per bin without knowing the concrete type. Phase 1 uses
/// [`make_accumulator_for`] via a string name, but phase 2's
/// `StreamingAggregatorTemplate` stores this kind and calls
/// [`make_from_kind`] on every new bin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccumulatorKind {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

impl AccumulatorKind {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name {
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "count" => Some(Self::Count),
            _ => None,
        }
    }
}

/// Build a fresh accumulator from an aggregate-function name, or `None`
/// if the function isn't supported by the columnar path yet. Callers
/// must gate with [`super::super::aggregation::Aggregator::supported_for_columnar`]
/// before calling this.
pub(crate) fn make_accumulator_for(func_name: &str) -> Option<Box<dyn ColumnarAccumulator>> {
    Some(match AccumulatorKind::from_name(func_name)? {
        AccumulatorKind::Sum => Box::new(super::sum::SumAccumulator::default()),
        AccumulatorKind::Avg => Box::new(super::avg::AvgAccumulator::default()),
        AccumulatorKind::Min => Box::new(super::min_max::MinAccumulator::default()),
        AccumulatorKind::Max => Box::new(super::min_max::MaxAccumulator::default()),
        AccumulatorKind::Count => Box::new(super::count::CountAccumulator::default()),
    })
}

/// Same as [`make_accumulator_for`] but dispatches on the enum tag.
#[allow(dead_code)] // used by phase 2's streaming driver
pub(crate) fn make_from_kind(kind: AccumulatorKind) -> Box<dyn ColumnarAccumulator> {
    match kind {
        AccumulatorKind::Sum => Box::new(super::sum::SumAccumulator::default()),
        AccumulatorKind::Avg => Box::new(super::avg::AvgAccumulator::default()),
        AccumulatorKind::Min => Box::new(super::min_max::MinAccumulator::default()),
        AccumulatorKind::Max => Box::new(super::min_max::MaxAccumulator::default()),
        AccumulatorKind::Count => Box::new(super::count::CountAccumulator::default()),
    }
}

/// Convenience: downcast `values` to a `Float64Array` slice, coercing
/// from `Int64Array` if the column happens to be an integer. Returns
/// `None` if the values don't exist or can't be coerced.
///
/// Used by every numeric accumulator to avoid duplicating the
/// "check downcast, fall back to coerce-from-int" boilerplate.
pub(crate) fn values_as_f64(values: Option<&dyn Array>) -> Option<F64View<'_>> {
    let arr = values?;
    if let Some(f) = arr.as_any().downcast_ref::<arrow_array::Float64Array>() {
        return Some(F64View::Float(f));
    }
    if let Some(i) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return Some(F64View::Int(i));
    }
    None
}

/// A view over either a `Float64Array` or an `Int64Array` (for int→f64
/// coercion) that exposes the common hot-path ops the numeric
/// accumulators need: per-row value read and per-row validity check.
///
/// This avoids materialising a whole `Vec<f64>` just to coerce types.
pub(crate) enum F64View<'a> {
    Float(&'a arrow_array::Float64Array),
    Int(&'a arrow_array::Int64Array),
}

impl<'a> F64View<'a> {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Float(a) => a.len(),
            Self::Int(a) => a.len(),
        }
    }

    #[inline]
    pub(crate) fn is_valid(&self, i: usize) -> bool {
        match self {
            Self::Float(a) => a.is_valid(i),
            Self::Int(a) => a.is_valid(i),
        }
    }

    #[inline]
    pub(crate) fn value(&self, i: usize) -> f64 {
        match self {
            Self::Float(a) => a.value(i),
            Self::Int(a) => a.value(i) as f64,
        }
    }

    /// Return the underlying Float64Array if this view is a Float64.
    /// Used by Sum/Avg for the NaN-free fast path.
    #[inline]
    pub(crate) fn as_float64(&self) -> Option<&'a arrow_array::Float64Array> {
        match self {
            Self::Float(a) => Some(*a),
            Self::Int(_) => None,
        }
    }
}

/// Arc a pre-built array for the `evaluate()` return. Centralises the
/// `Arc::new(arr) as ArrayRef` incantation.
#[inline]
pub(crate) fn arc<T: Array + 'static>(arr: T) -> ArrayRef {
    Arc::new(arr)
}
