//! `CountAccumulator` — grouped COUNT(*).
//!
//! Row-oriented reference: `Count::apply_refs(events) = events.len()`.
//!
//! Note: Varpulis's `Count` counts all events in the group regardless of
//! field presence or validity. That's stricter than SQL `COUNT(field)`
//! (which skips nulls) but matches what `events.len()` means. The columnar
//! path honors this by never consulting the `values` array — it just adds
//! 1 per row in `group_indices`.
//!
//! Returns an `Int64Array` (not `UInt64`) to match `Value::Int` which is
//! `i64`. Overflow at 9.2 quintillion events per group — same ceiling as
//! the row path.

use arrow_array::{ArrayRef, Int64Array};

use super::accumulator::{arc, ColumnarAccumulator};

#[derive(Default)]
pub(crate) struct CountAccumulator {
    counts: Vec<i64>,
}

impl ColumnarAccumulator for CountAccumulator {
    fn resize(&mut self, total_groups: usize) {
        if total_groups > self.counts.len() {
            self.counts.resize(total_groups, 0);
        }
    }

    fn update_batch(&mut self, _values: Option<&dyn arrow_array::Array>, group_indices: &[u32]) {
        // Count ignores the values column — every row in group_indices counts.
        for &gi in group_indices {
            self.counts[gi as usize] += 1;
        }
    }

    fn drain_single(&self, group_idx: u32) -> varpulis_core::Value {
        varpulis_core::Value::Int(self.counts[group_idx as usize])
    }

    fn update_single(&mut self, group_idx: u32, _value: Option<f64>) {
        // Count ignores the value — every call increments.
        self.counts[group_idx as usize] += 1;
    }

    fn evaluate(&mut self) -> ArrayRef {
        arc(Int64Array::from(self.counts.clone()))
    }

    fn name(&self) -> &'static str {
        "count"
    }

    fn input_type(&self) -> Option<arrow_schema::DataType> {
        None // Count takes no input column
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_per_group() {
        let mut acc = CountAccumulator::default();
        acc.resize(3);
        // 4 rows to group 0, 3 rows to group 1, 2 rows to group 2
        let groups: Vec<u32> = vec![0, 0, 0, 0, 1, 1, 1, 2, 2];
        acc.update_batch(None, &groups);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 4);
        assert_eq!(arr.value(1), 3);
        assert_eq!(arr.value(2), 2);
    }

    #[test]
    fn empty_group_is_zero() {
        let mut acc = CountAccumulator::default();
        acc.resize(3);
        acc.update_batch(None, &[1, 1]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 0);
        assert_eq!(arr.value(1), 2);
        assert_eq!(arr.value(2), 0);
    }

    #[test]
    fn ignores_values_column() {
        let mut acc = CountAccumulator::default();
        acc.resize(1);
        // Even with NaN/null values, count reflects the group_indices length.
        let vs = arrow_array::Float64Array::from(vec![Some(f64::NAN), None, Some(5.0)]);
        acc.update_batch(Some(&vs), &[0, 0, 0]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 3);
    }

    #[test]
    fn monotonic_resize() {
        let mut acc = CountAccumulator::default();
        acc.resize(2);
        acc.update_batch(None, &[0, 0, 1]);
        acc.resize(4);
        acc.update_batch(None, &[3, 3, 0]);
        let out = acc.evaluate();
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 3); // 2 from batch 1 + 1 from batch 2
        assert_eq!(arr.value(1), 1);
        assert_eq!(arr.value(2), 0);
        assert_eq!(arr.value(3), 2);
    }
}
