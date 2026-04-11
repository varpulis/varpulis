//! Single-group columnar aggregator for non-partitioned `.aggregate(...)`.
//!
//! Phase-3a companion to [`super::grouped::ColumnarGroupedAggregator`].
//! The non-partitioned case is a grouped aggregate with exactly one
//! group (`group_idx = 0` for every row), so we skip the `GroupKeyEncoder`
//! / `arrow_row::RowConverter` machinery entirely and drive the
//! accumulators directly.
//!
//! The win vs. the pre-existing `Aggregator::apply_arrow` path is:
//!
//! 1. **One column lookup per unique field** instead of one per (alias,
//!    aggregate-function) pair. A `sum(x) + avg(x) + min(x) + max(x)`
//!    pipeline previously called `batch.column_by_name("x")` four times;
//!    here it's one lookup per spec, but the accumulators share a
//!    single `update_batch` scan of the underlying slice, which is
//!    cache-friendly for small-to-medium batches.
//!
//! 2. **Persisted `SchemaCache`** lives on `AggregatorState` so
//!    consecutive window fires re-use the inferred schema instead of
//!    rebuilding `SchemaCache::new()` every call.
//!
//! Phase 3b will fuse this with the upstream tumbling window into a
//! streaming op analogous to `PartitionedWindowedColumnarAggregate`.

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use indexmap::IndexMap;
use varpulis_core::Value;

use super::grouped::{array_value_at, AggSpec};

/// One-shot single-group aggregator. Build, feed a batch, drain.
pub(crate) struct NonPartitionedColumnarAggregator {
    specs: Vec<AggSpec>,
    /// Reusable group-index scratch buffer, all zeros. Resized in place on
    /// every `update()` call so subsequent calls of the same size are
    /// allocation-free.
    group_indices: Vec<u32>,
}

impl NonPartitionedColumnarAggregator {
    pub(crate) fn try_new(specs: Vec<AggSpec>) -> Result<Self, ArrowError> {
        Ok(Self {
            specs,
            group_indices: Vec::new(),
        })
    }

    /// Feed `batch` into the aggregator. Every row goes to group 0.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let n = batch.num_rows();
        if self.group_indices.len() < n {
            self.group_indices.resize(n, 0);
        }
        let indices = &self.group_indices[..n];

        for spec in &mut self.specs {
            spec.accumulator.resize(1);
            let values = match &spec.field {
                Some(name) => batch.column_by_name(name).map(|c| c.as_ref()),
                None => None, // Count — no input column needed
            };
            spec.accumulator.update_batch(values, indices);
        }
        Ok(())
    }

    /// Drain the single-group result into the `IndexMap<alias, Value>` shape
    /// the `Aggregate` op returns.
    pub(crate) fn drain(mut self) -> IndexMap<String, Value> {
        let mut out = IndexMap::with_capacity(self.specs.len());
        for spec in &mut self.specs {
            let arr = spec.accumulator.evaluate();
            out.insert(spec.alias.clone(), array_value_at(arr.as_ref(), 0));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::super::accumulator::make_accumulator_for;
    use super::*;

    fn build_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("temperature", DataType::Float64, false),
            Field::new("count_col", DataType::Int64, false),
        ]));
        let temps = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let counts = Int64Array::from(vec![1, 2, 3, 4, 5]);
        RecordBatch::try_new(schema, vec![Arc::new(temps), Arc::new(counts)]).unwrap()
    }

    #[test]
    fn sum_avg_min_max_count_single_group() {
        let specs = vec![
            AggSpec {
                alias: "s".to_string(),
                accumulator: make_accumulator_for("sum").unwrap(),
                field: Some("temperature".to_string()),
            },
            AggSpec {
                alias: "a".to_string(),
                accumulator: make_accumulator_for("avg").unwrap(),
                field: Some("temperature".to_string()),
            },
            AggSpec {
                alias: "mn".to_string(),
                accumulator: make_accumulator_for("min").unwrap(),
                field: Some("temperature".to_string()),
            },
            AggSpec {
                alias: "mx".to_string(),
                accumulator: make_accumulator_for("max").unwrap(),
                field: Some("temperature".to_string()),
            },
            AggSpec {
                alias: "c".to_string(),
                accumulator: make_accumulator_for("count").unwrap(),
                field: None,
            },
        ];
        let mut agg = NonPartitionedColumnarAggregator::try_new(specs).unwrap();
        agg.update(&build_batch()).unwrap();
        let out = agg.drain();

        assert_eq!(out.get("s"), Some(&Value::Float(150.0)));
        assert_eq!(out.get("a"), Some(&Value::Float(30.0)));
        assert_eq!(out.get("mn"), Some(&Value::Float(10.0)));
        assert_eq!(out.get("mx"), Some(&Value::Float(50.0)));
        assert_eq!(out.get("c"), Some(&Value::Int(5)));
    }

    #[test]
    fn int_column_coerces_to_float() {
        let specs = vec![AggSpec {
            alias: "s".to_string(),
            accumulator: make_accumulator_for("sum").unwrap(),
            field: Some("count_col".to_string()),
        }];
        let mut agg = NonPartitionedColumnarAggregator::try_new(specs).unwrap();
        agg.update(&build_batch()).unwrap();
        assert_eq!(agg.drain().get("s"), Some(&Value::Float(15.0)));
    }

    #[test]
    fn reusing_group_indices_across_calls() {
        // First call: 5 rows. Second call: 3 rows. Scratch buffer should
        // be reused without reallocating for the smaller call.
        let specs = vec![AggSpec {
            alias: "s".to_string(),
            accumulator: make_accumulator_for("sum").unwrap(),
            field: Some("temperature".to_string()),
        }];
        let mut agg = NonPartitionedColumnarAggregator::try_new(specs).unwrap();

        agg.update(&build_batch()).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "temperature",
            DataType::Float64,
            false,
        )]));
        let small = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))],
        )
        .unwrap();
        agg.update(&small).unwrap();

        // Note: "monotonic state" — the accumulator has accumulated both
        // batches into group 0, so sum = 150 + 6 = 156.
        assert_eq!(agg.drain().get("s"), Some(&Value::Float(156.0)));
    }
}
