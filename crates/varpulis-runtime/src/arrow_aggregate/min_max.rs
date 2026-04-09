//! `MinAccumulator` / `MaxAccumulator` — grouped MIN / MAX over f64 values,
//! skipping NaN and null. Empty groups evaluate to null, matching
//! `Min::apply_refs` / `Max::apply_refs`:
//!
//! ```text
//! let values: Vec<f64> = events.iter()
//!     .filter_map(|e| e.get_float(field))
//!     .filter(|v| !v.is_nan())
//!     .collect();
//! match simd::min_f64(&values) {
//!     Some(m) => Value::Float(m),
//!     None => Value::Null,
//! }
//! ```

use arrow_array::builder::Float64Builder;
use arrow_array::ArrayRef;

use super::accumulator::{arc, values_as_f64, ColumnarAccumulator};

/// Grouped MIN. State is `Vec<f64>` current best, plus a parallel
/// `Vec<bool>` "seen" mask so we can distinguish "no values" (empty
/// group → null) from "saw a value equal to `f64::INFINITY`".
#[derive(Default)]
pub(crate) struct MinAccumulator {
    values: Vec<f64>,
    seen: Vec<bool>,
}

impl ColumnarAccumulator for MinAccumulator {
    fn resize(&mut self, total_groups: usize) {
        if total_groups > self.values.len() {
            self.values.resize(total_groups, f64::INFINITY);
            self.seen.resize(total_groups, false);
        }
    }

    fn update_batch(&mut self, values: Option<&dyn arrow_array::Array>, group_indices: &[u32]) {
        let Some(view) = values_as_f64(values) else {
            return;
        };
        debug_assert_eq!(view.len(), group_indices.len());

        for (i, &gi) in group_indices.iter().enumerate() {
            if !view.is_valid(i) {
                continue;
            }
            let v = view.value(i);
            if v.is_nan() {
                continue;
            }
            let gi = gi as usize;
            if !self.seen[gi] || v < self.values[gi] {
                self.values[gi] = v;
                self.seen[gi] = true;
            }
        }
    }

    fn evaluate(&mut self) -> ArrayRef {
        let total = self.values.len();
        let mut builder = Float64Builder::with_capacity(total);
        for (gi, &seen) in self.seen.iter().enumerate() {
            if seen {
                builder.append_value(self.values[gi]);
            } else {
                builder.append_null();
            }
        }
        arc(builder.finish())
    }

    fn name(&self) -> &'static str {
        "min"
    }

    fn input_type(&self) -> Option<arrow_schema::DataType> {
        Some(arrow_schema::DataType::Float64)
    }
}

/// Grouped MAX. Symmetric to [`MinAccumulator`] below.
#[derive(Default)]
pub(crate) struct MaxAccumulator {
    values: Vec<f64>,
    seen: Vec<bool>,
}

impl ColumnarAccumulator for MaxAccumulator {
    fn resize(&mut self, total_groups: usize) {
        if total_groups > self.values.len() {
            self.values.resize(total_groups, f64::NEG_INFINITY);
            self.seen.resize(total_groups, false);
        }
    }

    fn update_batch(&mut self, values: Option<&dyn arrow_array::Array>, group_indices: &[u32]) {
        let Some(view) = values_as_f64(values) else {
            return;
        };
        debug_assert_eq!(view.len(), group_indices.len());

        for (i, &gi) in group_indices.iter().enumerate() {
            if !view.is_valid(i) {
                continue;
            }
            let v = view.value(i);
            if v.is_nan() {
                continue;
            }
            let gi = gi as usize;
            if !self.seen[gi] || v > self.values[gi] {
                self.values[gi] = v;
                self.seen[gi] = true;
            }
        }
    }

    fn evaluate(&mut self) -> ArrayRef {
        let total = self.values.len();
        let mut builder = Float64Builder::with_capacity(total);
        for (gi, &seen) in self.seen.iter().enumerate() {
            if seen {
                builder.append_value(self.values[gi]);
            } else {
                builder.append_null();
            }
        }
        arc(builder.finish())
    }

    fn name(&self) -> &'static str {
        "max"
    }

    fn input_type(&self) -> Option<arrow_schema::DataType> {
        Some(arrow_schema::DataType::Float64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array as _, Float64Array};

    #[test]
    fn min_picks_smallest() {
        let mut acc = MinAccumulator::default();
        acc.resize(2);
        let vs = Float64Array::from(vec![5.0, 10.0, -3.0, 7.0]);
        acc.update_batch(Some(&vs), &[0, 1, 0, 1]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), -3.0);
        assert_eq!(arr.value(1), 7.0);
    }

    #[test]
    fn max_picks_largest() {
        let mut acc = MaxAccumulator::default();
        acc.resize(2);
        let vs = Float64Array::from(vec![5.0, 10.0, -3.0, 7.0]);
        acc.update_batch(Some(&vs), &[0, 1, 0, 1]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 5.0);
        assert_eq!(arr.value(1), 10.0);
    }

    #[test]
    fn empty_groups_are_null() {
        let mut acc = MinAccumulator::default();
        acc.resize(3);
        acc.update_batch(Some(&Float64Array::from(vec![1.0])), &[1]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(arr.is_null(0));
        assert_eq!(arr.value(1), 1.0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn nan_skipped_infinity_kept() {
        let mut acc = MinAccumulator::default();
        acc.resize(1);
        // Infinity is a valid min candidate (below other huge values);
        // NaN is always skipped.
        let vs = Float64Array::from(vec![Some(f64::INFINITY), Some(f64::NAN), Some(100.0)]);
        acc.update_batch(Some(&vs), &[0, 0, 0]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 100.0);
    }

    #[test]
    fn monotonic_resize() {
        let mut acc = MaxAccumulator::default();
        acc.resize(2);
        acc.update_batch(Some(&Float64Array::from(vec![1.0, 2.0])), &[0, 1]);
        acc.resize(3);
        acc.update_batch(Some(&Float64Array::from(vec![10.0, 100.0])), &[0, 2]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 10.0);
        assert_eq!(arr.value(1), 2.0);
        assert_eq!(arr.value(2), 100.0);
    }
}
