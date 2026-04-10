//! `SumAccumulator` — grouped SUM(f64), coercing Int64 → f64.
//!
//! State layout: `Vec<f64> sums`, `Vec<u64> counts`. `counts[gi] == 0` means
//! "no non-null, non-NaN values seen for this group" and evaluates to
//! `Float64::null` (which the row-oriented `Sum::apply_refs` represents as
//! `Value::Float(0.0)` via `crate::simd::sum_f64(&[])`, but the row-oriented
//! path conflates "empty group" with "sum is zero" — the columnar path
//! matches that by emitting 0.0 for empty groups too, so round-tripping
//! through the `Value::Float` bridge is bit-identical).
//!
//! This matches what the row-oriented `Sum::apply_refs` does:
//!
//! ```text
//! let values: Vec<f64> = events
//!     .iter()
//!     .filter_map(|e| e.get_float(field))
//!     .filter(|v| !v.is_nan())
//!     .collect();
//! Value::Float(crate::simd::sum_f64(&values))  // empty → 0.0
//! ```

use arrow_array::{Array, ArrayRef, Float64Array};

use super::accumulator::{arc, values_as_f64, ColumnarAccumulator};

#[derive(Default)]
pub(crate) struct SumAccumulator {
    sums: Vec<f64>,
    counts: Vec<u64>,
}

impl ColumnarAccumulator for SumAccumulator {
    fn resize(&mut self, total_groups: usize) {
        if total_groups > self.sums.len() {
            self.sums.resize(total_groups, 0.0);
            self.counts.resize(total_groups, 0);
        }
    }

    fn update_batch(&mut self, values: Option<&dyn arrow_array::Array>, group_indices: &[u32]) {
        let Some(view) = values_as_f64(values) else {
            return;
        };
        debug_assert_eq!(view.len(), group_indices.len());

        // Fast path: no nulls, not NaN — tight loop that should autovectorize
        // on the Float64 case. For the Int64-coerced case we always go
        // through the slow path because `is_nan()` is trivially false on
        // integer coercions, but nulls still need to be skipped.
        if let Some(arr) = view.as_float64() {
            if arr.null_count() == 0 {
                let slice = arr.values();
                for (i, &v) in slice.iter().enumerate() {
                    if !v.is_nan() {
                        let gi = group_indices[i] as usize;
                        self.sums[gi] += v;
                        self.counts[gi] += 1;
                    }
                }
                return;
            }
        }

        // Generic path: handles nulls and Int64 coercion.
        for (i, &gi) in group_indices.iter().enumerate() {
            if !view.is_valid(i) {
                continue;
            }
            let v = view.value(i);
            if v.is_nan() {
                continue;
            }
            let gi = gi as usize;
            self.sums[gi] += v;
            self.counts[gi] += 1;
        }
    }

    fn drain_single(&self, group_idx: u32) -> varpulis_core::Value {
        // Sum: empty group → 0.0 (matches row path).
        varpulis_core::Value::Float(self.sums[group_idx as usize])
    }

    fn update_single(&mut self, group_idx: u32, value: Option<f64>) {
        if let Some(v) = value {
            let gi = group_idx as usize;
            self.sums[gi] += v;
            self.counts[gi] += 1;
        }
    }

    fn evaluate(&mut self) -> ArrayRef {
        // Match row-oriented Sum::apply_refs semantics: empty group → 0.0,
        // not null. The row path returns `Value::Float(sum_f64(&[])) = 0.0`.
        let out = Float64Array::from(self.sums.clone());
        // Do NOT drain self.sums/counts — the accumulator can be reused
        // across evaluate() calls in phase 2's streaming driver.
        arc(out)
    }

    fn name(&self) -> &'static str {
        "sum"
    }

    fn input_type(&self) -> Option<arrow_schema::DataType> {
        Some(arrow_schema::DataType::Float64)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Float64Array;

    use super::*;

    fn make_values(vs: &[f64]) -> Float64Array {
        Float64Array::from(vs.to_vec())
    }

    /// Three groups of 10 rows each, alternating device_ids 0/1/2.
    /// Sum for group 0 = rows with i%3==0, etc.
    #[test]
    fn basic_grouped_sum() {
        let mut acc = SumAccumulator::default();
        acc.resize(3);
        let vs = make_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]);
        let groups: Vec<u32> = vec![0, 1, 2, 0, 1, 2, 0, 1, 2];
        acc.update_batch(Some(&vs), &groups);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 1.0 + 4.0 + 7.0);
        assert_eq!(arr.value(1), 2.0 + 5.0 + 8.0);
        assert_eq!(arr.value(2), 3.0 + 6.0 + 9.0);
    }

    #[test]
    fn skips_nan() {
        let mut acc = SumAccumulator::default();
        acc.resize(1);
        let vs = make_values(&[1.0, f64::NAN, 2.0, f64::NAN]);
        let groups = vec![0, 0, 0, 0];
        acc.update_batch(Some(&vs), &groups);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 3.0);
    }

    #[test]
    fn skips_nulls_in_array() {
        let mut acc = SumAccumulator::default();
        acc.resize(1);
        // Array with one null in position 1.
        let vs = Float64Array::from(vec![Some(5.0), None, Some(7.0)]);
        let groups = vec![0u32, 0, 0];
        acc.update_batch(Some(&vs), &groups);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 12.0); // 5 + 7, null skipped
    }

    /// Resize is monotonic: state from the first batch must survive a
    /// second batch that grows the group count.
    #[test]
    fn monotonic_resize_across_batches() {
        let mut acc = SumAccumulator::default();
        acc.resize(2);
        acc.update_batch(Some(&make_values(&[1.0, 2.0])), &[0, 1]);

        // New group 2 arrives in a second batch.
        acc.resize(3);
        acc.update_batch(Some(&make_values(&[10.0, 20.0])), &[0, 2]);

        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 11.0); // 1.0 + 10.0
        assert_eq!(arr.value(1), 2.0); // untouched by second batch
        assert_eq!(arr.value(2), 20.0);
    }

    #[test]
    fn coerces_int64_to_f64() {
        let mut acc = SumAccumulator::default();
        acc.resize(1);
        let ints = arrow_array::Int64Array::from(vec![10, 20, 30]);
        acc.update_batch(Some(&ints), &[0, 0, 0]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 60.0);
    }

    #[test]
    fn empty_group_evaluates_to_zero() {
        // Match row-oriented Sum::apply_refs: empty → Value::Float(0.0).
        let mut acc = SumAccumulator::default();
        acc.resize(3);
        // Only put data in group 1.
        acc.update_batch(Some(&make_values(&[5.0])), &[1]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 0.0);
        assert_eq!(arr.value(1), 5.0);
        assert_eq!(arr.value(2), 0.0);
    }
}
