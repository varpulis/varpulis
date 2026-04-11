//! [`ColumnarGroupedAggregator`] — phase-1 one-shot driver.
//!
//! Takes a single `RecordBatch` + a list of `(alias, accumulator, field)`
//! specs, hashes the group-key column(s) via [`GroupKeyEncoder`], resizes
//! each accumulator, calls `update_batch` on each, and drains the per-group
//! results back out in the same `Vec<(String, IndexMap<String, Value>)>`
//! shape that `PartitionedAggregatorState::apply_row` returns. The row
//! path and the columnar path are therefore interchangeable from the
//! perspective of the rest of the pipeline.
//!
//! "One-shot" means the aggregator is built and drained inside a single
//! `apply()` call — it does NOT persist state across window fires. Phase 2
//! introduces a streaming variant that holds state per `(bin, group_idx)`
//! across arriving batches.

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, Field};
use indexmap::IndexMap;
use varpulis_core::Value;

use super::accumulator::ColumnarAccumulator;
use super::group_keys::GroupKeyEncoder;

/// Specification of one aggregation column in the output.
///
/// `field` is `None` only for `Count` (which doesn't consult an input column).
pub(crate) struct AggSpec {
    pub(crate) alias: String,
    pub(crate) accumulator: Box<dyn ColumnarAccumulator>,
    pub(crate) field: Option<String>,
}

/// One-shot grouped aggregator. Build, feed one batch, drain.
pub(crate) struct ColumnarGroupedAggregator {
    specs: Vec<AggSpec>,
    key_field_names: Vec<String>,
    encoder: GroupKeyEncoder,
    /// Lightweight string-keyed group lookup for the single-event fast
    /// path. Populated lazily alongside the RowConverter-based `encoder`
    /// so that both paths see the same group indices. Phase 2 fast path
    /// uses this instead of building a 1-row RecordBatch.
    fast_group_map: rustc_hash::FxHashMap<String, u32>,
    next_fast_group: u32,
}

impl ColumnarGroupedAggregator {
    /// Construct a new aggregator for a given set of key fields and
    /// aggregation specs.
    ///
    /// `key_fields` describes the columns to group by; typical phase-1
    /// usage passes a single-element slice with just the partition key.
    pub(crate) fn try_new(key_fields: &[Field], specs: Vec<AggSpec>) -> Result<Self, ArrowError> {
        Ok(Self {
            specs,
            key_field_names: key_fields.iter().map(|f| f.name().clone()).collect(),
            encoder: GroupKeyEncoder::new(key_fields)?,
            fast_group_map: rustc_hash::FxHashMap::default(),
            next_fast_group: 0,
        })
    }

    /// Feed `batch` into the aggregator. Can be called multiple times
    /// in phase 2; phase 1 calls it exactly once.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        // 1. Project key columns out of the batch by name.
        let key_cols: Vec<ArrayRef> = self
            .key_field_names
            .iter()
            .map(|n| {
                batch
                    .column_by_name(n)
                    .cloned()
                    .ok_or_else(|| ArrowError::SchemaError(format!("missing key column {n}")))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // 2. Encode group keys. The display_fn pulls the first row's
        //    value out of the (single) key column and renders via
        //    `Value::to_partition_key` to match the row path exactly.
        let key_cols_for_display = key_cols.clone();
        let (group_indices, total_groups) = self.encoder.encode_batch(&key_cols, |row_idx| {
            display_key_for_row(&key_cols_for_display, row_idx)
        })?;

        // 3. Resize every accumulator and update it.
        for spec in &mut self.specs {
            spec.accumulator.resize(total_groups);
            let values: Option<&dyn Array> = match &spec.field {
                Some(name) => batch.column_by_name(name).map(|c| c.as_ref()),
                None => None, // Count
            };
            spec.accumulator.update_batch(values, &group_indices);
        }
        Ok(())
    }

    /// Fast path for a single event: resolve the group index from the
    /// partition key's display string (via a lightweight FxHashMap, no
    /// RecordBatch / RowConverter), then call `update_single` on each
    /// accumulator. The `event` reference is used to extract field
    /// values directly via `get_float`.
    ///
    /// This is the phase-2 hot path for scenario 02 where 99% of calls
    /// have exactly 1 event. Avoids the ~10 µs RecordBatch construction
    /// overhead per single-event call.
    pub(crate) fn update_single_event(
        &mut self,
        partition_key: std::borrow::Cow<'_, str>,
        event: &crate::event::Event,
    ) {
        // 1. Resolve group index from the display string. Allocate
        //    an owned String ONLY for the first sighting of each group.
        //    Subsequent events for the same partition key hit the
        //    `get()` fast path with zero allocation.
        let gi = match self.fast_group_map.get(partition_key.as_ref()) {
            Some(&gi) => gi,
            None => {
                let gi = self.next_fast_group;
                self.next_fast_group += 1;
                self.fast_group_map.insert(partition_key.into_owned(), gi);
                gi
            }
        };
        let total_groups = self.next_fast_group as usize;

        // 2. Resize all accumulators to accommodate the new group count.
        for spec in &mut self.specs {
            spec.accumulator.resize(total_groups);
        }

        // 3. For each spec, extract the value from the event and call
        //    update_single. NaN is skipped (matching the columnar path).
        for spec in &mut self.specs {
            let value: Option<f64> = match &spec.field {
                Some(field_name) => event.get_float(field_name).filter(|v| !v.is_nan()),
                None => None, // Count — update_single ignores value
            };
            spec.accumulator.update_single(gi, value);
        }
    }

    /// Drain results in the row-oriented shape that
    /// [`crate::engine::types::PartitionedAggregatorState::apply`] returns.
    ///
    /// This consumes the per-group evaluation of each accumulator and
    /// reassembles them into one `IndexMap<alias, Value>` per group,
    /// keyed by the display string cached in the encoder.
    /// Fast drain path that reads directly from the accumulator's scalar
    /// state (`Vec<f64>` / `Vec<u64>`) via `drain_single()` — no Arrow
    /// array construction at all. Used when the streaming op populated
    /// this aggregator exclusively via `update_single_event()`.
    ///
    /// Falls back to the Arrow-based `drain_as_row_results()` if the
    /// fast_group_map is empty (meaning the columnar `update()` path
    /// was used, where only the RowConverter has the key mappings).
    pub(crate) fn drain_fast(self) -> Vec<(String, IndexMap<String, Value>)> {
        if self.fast_group_map.is_empty() {
            return self.drain_as_row_results();
        }

        // Collect keys sorted by group index.
        let mut keys: Vec<(u32, &String)> = self
            .fast_group_map
            .iter()
            .map(|(display, &gi)| (gi, display))
            .collect();
        keys.sort_unstable_by_key(|(gi, _)| *gi);

        let mut out = Vec::with_capacity(keys.len());
        for (gi, display_key) in keys {
            let mut map = IndexMap::with_capacity(self.specs.len());
            for spec in &self.specs {
                map.insert(spec.alias.clone(), spec.accumulator.drain_single(gi));
            }
            out.push((display_key.clone(), map));
        }
        out
    }

    pub(crate) fn drain_as_row_results(mut self) -> Vec<(String, IndexMap<String, Value>)> {
        // Groups may have been populated via the columnar `update()`
        // path (uses the `encoder` / RowConverter) OR the single-event
        // `update_single_event()` path (uses `fast_group_map`). In
        // practice a given aggregator instance uses one or the other,
        // never both in the same session, but we handle the union.
        let total = self
            .encoder
            .total_groups()
            .max(self.next_fast_group as usize);

        // Evaluate each accumulator once — returns a total-group-length
        // Arrow array, which we index per group below.
        let evaluated: Vec<(String, ArrayRef)> = self
            .specs
            .iter_mut()
            .map(|spec| (spec.alias.clone(), spec.accumulator.evaluate()))
            .collect();

        // Collect display keys in group-index order. Merge from both
        // sources (encoder for columnar path, fast_group_map for
        // single-event path).
        let mut keys: Vec<(usize, String)> = self
            .encoder
            .iter_keys()
            .map(|(gi, s)| (gi, s.to_string()))
            .collect();
        // Add keys from the fast path that aren't already present.
        let encoder_gis: std::collections::HashSet<usize> =
            keys.iter().map(|(gi, _)| *gi).collect();
        for (display_key, &gi) in &self.fast_group_map {
            if !encoder_gis.contains(&(gi as usize)) {
                keys.push((gi as usize, display_key.clone()));
            }
        }
        keys.sort_unstable_by_key(|(gi, _)| *gi);

        let mut out = Vec::with_capacity(total);
        for (gi, display_key) in keys {
            let mut map = IndexMap::with_capacity(evaluated.len());
            for (alias, arr) in &evaluated {
                map.insert(alias.clone(), array_value_at(arr.as_ref(), gi));
            }
            out.push((display_key, map));
        }
        out
    }
}

/// Render the `row_idx`-th value of a single-column key as its
/// `to_partition_key()` string, matching the row-oriented path's
/// `Value::to_partition_key().into_owned()` behaviour.
///
/// For multi-column keys (phase 2+), we'd join the individual
/// `to_partition_key()` strings with a separator. Phase 1 only uses one
/// key column.
fn display_key_for_row(key_cols: &[ArrayRef], row_idx: usize) -> String {
    if key_cols.is_empty() {
        return "default".to_string();
    }
    // Phase 1: single key column. Take the first.
    let col = &key_cols[0];
    let v = array_value_at(col.as_ref(), row_idx);
    // Match the row path: missing/null → "default".
    if matches!(v, Value::Null) {
        return "default".to_string();
    }
    v.to_partition_key().into_owned()
}

/// Read the scalar value at position `i` of a generic Arrow array.
/// Supports the primitive types we care about; anything else → `Value::Null`.
pub(super) fn array_value_at(arr: &dyn Array, i: usize) -> Value {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow_schema::DataType;

    if arr.is_null(i) {
        return Value::Null;
    }
    match arr.data_type() {
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| Value::Float(a.value(i)))
            .unwrap_or(Value::Null),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| Value::Int(a.value(i)))
            .unwrap_or(Value::Null),
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::Bool(a.value(i)))
            .unwrap_or(Value::Null),
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| Value::Str(a.value(i).into()))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, StringArray};
    use arrow_schema::{DataType, Schema};

    use super::super::accumulator::make_accumulator_for;
    use super::*;

    fn build_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("device_id", DataType::Utf8, false),
            Field::new("temperature", DataType::Float64, false),
        ]));
        let devices = StringArray::from(vec!["d0", "d1", "d0", "d2", "d1", "d0"]);
        let temps = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
        RecordBatch::try_new(schema, vec![Arc::new(devices), Arc::new(temps)]).unwrap()
    }

    #[test]
    fn end_to_end_sum_avg_count() {
        let key_field = Field::new("device_id", DataType::Utf8, false);
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
                alias: "c".to_string(),
                accumulator: make_accumulator_for("count").unwrap(),
                field: None,
            },
        ];
        let mut agg = ColumnarGroupedAggregator::try_new(&[key_field], specs).unwrap();
        agg.update(&build_batch()).unwrap();

        let mut results = agg.drain_as_row_results();
        // Sort for deterministic assertions — iter_keys yields by group_idx,
        // which is insertion order ("d0" first, then "d1", then "d2").
        results.sort_by(|a, b| a.0.cmp(&b.0));

        // d0: sum=10+30+60=100, count=3, avg=100/3
        // d1: sum=20+50=70, count=2, avg=35
        // d2: sum=40, count=1, avg=40
        assert_eq!(results[0].0, "d0");
        assert_eq!(results[0].1["s"], Value::Float(100.0));
        let d0_avg = results[0].1["a"].as_float().unwrap();
        assert!((d0_avg - 100.0 / 3.0).abs() < 1e-9);
        assert_eq!(results[0].1["c"], Value::Int(3));

        assert_eq!(results[1].0, "d1");
        assert_eq!(results[1].1["s"], Value::Float(70.0));
        assert_eq!(results[1].1["a"], Value::Float(35.0));
        assert_eq!(results[1].1["c"], Value::Int(2));

        assert_eq!(results[2].0, "d2");
        assert_eq!(results[2].1["s"], Value::Float(40.0));
        assert_eq!(results[2].1["a"], Value::Float(40.0));
        assert_eq!(results[2].1["c"], Value::Int(1));
    }

    #[test]
    fn min_max_end_to_end() {
        let key_field = Field::new("device_id", DataType::Utf8, false);
        let specs = vec![
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
        ];
        let mut agg = ColumnarGroupedAggregator::try_new(&[key_field], specs).unwrap();
        agg.update(&build_batch()).unwrap();
        let mut results = agg.drain_as_row_results();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(results[0].1["mn"], Value::Float(10.0));
        assert_eq!(results[0].1["mx"], Value::Float(60.0));
        assert_eq!(results[1].1["mn"], Value::Float(20.0));
        assert_eq!(results[1].1["mx"], Value::Float(50.0));
        assert_eq!(results[2].1["mn"], Value::Float(40.0));
        assert_eq!(results[2].1["mx"], Value::Float(40.0));
    }
}
