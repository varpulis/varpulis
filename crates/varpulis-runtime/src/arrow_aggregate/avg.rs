//! `AvgAccumulator` — grouped AVG(f64) = SUM / COUNT of non-null, non-NaN values.
//!
//! State layout: `Vec<f64> sums`, `Vec<u64> counts`. On `evaluate`:
//! `counts[gi] == 0` yields null (matches row-oriented `Avg::apply_refs`
//! which returns `Value::Null` for an empty group); otherwise
//! `sums[gi] / counts[gi] as f64`.
//!
//! Row-oriented reference: `Avg::apply_refs` in `aggregation.rs` — same
//! NaN-skipping, same null-on-empty-group semantics. Returns
//! `Value::Null` for an empty group, `Value::Float(sum / count)`
//! otherwise.

use arrow_array::builder::Float64Builder;
use arrow_array::{Array, ArrayRef};

use super::accumulator::{arc, values_as_f64, ColumnarAccumulator};

#[derive(Default)]
pub(crate) struct AvgAccumulator {
    sums: Vec<f64>,
    counts: Vec<u64>,
}

impl ColumnarAccumulator for AvgAccumulator {
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

    fn evaluate(&mut self) -> ArrayRef {
        let total = self.sums.len();
        let mut builder = Float64Builder::with_capacity(total);
        for gi in 0..total {
            let c = self.counts[gi];
            if c == 0 {
                builder.append_null();
            } else {
                builder.append_value(self.sums[gi] / c as f64);
            }
        }
        arc(builder.finish())
    }

    fn name(&self) -> &'static str {
        "avg"
    }

    fn input_type(&self) -> Option<arrow_schema::DataType> {
        Some(arrow_schema::DataType::Float64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;

    #[test]
    fn grouped_avg() {
        let mut acc = AvgAccumulator::default();
        acc.resize(2);
        let vs = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        // group 0 → avg(10,30)=20, group 1 → avg(20,40)=30
        acc.update_batch(Some(&vs), &[0, 1, 0, 1]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 20.0);
        assert_eq!(arr.value(1), 30.0);
    }

    #[test]
    fn empty_group_null() {
        // Match row-oriented Avg::apply_refs: empty → Value::Null.
        let mut acc = AvgAccumulator::default();
        acc.resize(2);
        acc.update_batch(Some(&Float64Array::from(vec![5.0])), &[0]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 5.0);
        assert!(arr.is_null(1), "empty group must evaluate to null");
    }

    #[test]
    fn nan_and_null_skipped() {
        let mut acc = AvgAccumulator::default();
        acc.resize(1);
        // 10.0, null, NaN, 30.0 → avg = 20.0, count = 2
        let vs = Float64Array::from(vec![Some(10.0), None, Some(f64::NAN), Some(30.0)]);
        acc.update_batch(Some(&vs), &[0, 0, 0, 0]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 20.0);
    }
}
