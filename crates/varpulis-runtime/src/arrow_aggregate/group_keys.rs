//! [`GroupKeyEncoder`] — wraps `arrow_row::RowConverter` to map a batch of
//! key-column values to dense per-row group indices.
//!
//! `arrow_row` normalises multi-column values into a comparable byte
//! representation that is `Hash + Eq`, letting us use an `FxHashMap<OwnedRow, u32>`
//! as the group-index table. Compared to our current
//! `FxHashMap<String, Vec<SharedEvent>>` approach, this:
//!
//! 1. Hashes the whole batch's key column in one bulk pass, not row-by-row
//!    with one `String::from` allocation each.
//! 2. Supports multi-column keys natively — useful for phase 2 fusion
//!    where the "fused" key is `(bin_start, partition_key)`.
//! 3. Gives us the same `group_idx` semantics as DataFusion's
//!    `GroupedHashAggregateStream`, so accumulator state stays a dense
//!    `Vec<f64>` and autovectorises.
//!
//! ## Display keys
//!
//! The row-oriented `PartitionedAggregatorState::apply` returns each
//! partition keyed by a `String` (via `Value::to_partition_key()`). We
//! preserve that — phase-1 callers don't want to think about Arrow, they
//! just want the same `Vec<(String, IndexMap<..>)>` shape. So alongside
//! the `OwnedRow → group_idx` map we also stash the original
//! string display of each group's key, cached on first sighting.

use arrow_array::ArrayRef;
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{ArrowError, Field};
use rustc_hash::FxHashMap;

/// Encodes batch key columns into dense group indices and tracks the
/// display key per group for row-result emission.
pub(crate) struct GroupKeyEncoder {
    converter: RowConverter,
    /// `OwnedRow → (group_idx, display_key)`.
    /// `display_key` is the `Value::to_partition_key()` rendering of the
    /// first row that created the group, cached so `drain_as_row_results`
    /// doesn't have to walk RecordBatches again to recover it.
    index: FxHashMap<OwnedRow, GroupEntry>,
    next_group: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct GroupEntry {
    pub(crate) group_idx: u32,
    pub(crate) display_key: String,
}

impl GroupKeyEncoder {
    /// Build a new encoder for a set of key columns. Typical phase-1 usage
    /// passes a single field (e.g. `device_id`).
    pub(crate) fn new(key_fields: &[Field]) -> Result<Self, ArrowError> {
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

    /// Encode `key_columns` (already projected from the input batch, in
    /// the order passed to [`new`]) and return one `group_idx` per row.
    ///
    /// `display_fn` is invoked at most once per group to stringify a
    /// representative row for the [`GroupEntry::display_key`] cache.
    /// Phase-1 callers pass a closure that pulls the value(s) for the
    /// first row of each group from the original batch and renders via
    /// `Value::to_partition_key()`.
    pub(crate) fn encode_batch(
        &mut self,
        key_columns: &[ArrayRef],
        mut display_fn: impl FnMut(usize) -> String,
    ) -> Result<(Vec<u32>, usize), ArrowError> {
        let rows = self.converter.convert_columns(key_columns)?;
        let mut out = Vec::with_capacity(rows.num_rows());
        for (row_idx, row) in rows.iter().enumerate() {
            // `Row<'_>` is Hash+Eq, but `FxHashMap` owns its keys — we
            // have to allocate OwnedRow on first insertion. Check first.
            let owned = row.owned();
            let entry = self.index.entry(owned).or_insert_with(|| GroupEntry {
                group_idx: {
                    let gi = self.next_group;
                    self.next_group += 1;
                    gi
                },
                display_key: display_fn(row_idx),
            });
            out.push(entry.group_idx);
        }
        Ok((out, self.next_group as usize))
    }

    /// Total groups seen so far (monotonically non-decreasing across
    /// multiple `encode_batch` calls).
    pub(crate) fn total_groups(&self) -> usize {
        self.next_group as usize
    }

    /// Yield `(group_idx, display_key)` pairs in group-index order so
    /// callers can materialise per-group results into the expected
    /// `Vec<(String, IndexMap<..>)>` shape.
    pub(crate) fn iter_keys(&self) -> impl Iterator<Item = (usize, &str)> {
        let mut keys: Vec<(u32, &str)> = self
            .index
            .values()
            .map(|e| (e.group_idx, e.display_key.as_str()))
            .collect();
        keys.sort_unstable_by_key(|(gi, _)| *gi);
        keys.into_iter().map(|(gi, s)| (gi as usize, s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::DataType;

    #[test]
    fn roundtrip_single_string_key() {
        let field = Field::new("device_id", DataType::Utf8, true);
        let mut enc = GroupKeyEncoder::new(&[field]).unwrap();
        let col: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b"]));
        let (indices, total) = enc
            .encode_batch(&[col], |row_idx| match row_idx {
                0 => "a".to_string(),
                1 => "b".to_string(),
                3 => "c".to_string(),
                _ => unreachable!("display_fn only called on first sighting"),
            })
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(indices[0], 0); // "a"
        assert_eq!(indices[1], 1); // "b"
        assert_eq!(indices[2], 0); // "a" again
        assert_eq!(indices[3], 2); // "c"
        assert_eq!(indices[4], 1); // "b" again

        let keys: Vec<(usize, &str)> = enc.iter_keys().collect();
        assert_eq!(keys, vec![(0, "a"), (1, "b"), (2, "c")]);
    }

    #[test]
    fn second_batch_reuses_group_indices() {
        let field = Field::new("k", DataType::Utf8, true);
        let mut enc = GroupKeyEncoder::new(&[field]).unwrap();

        let col1: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
        let (i1, total1) = enc
            .encode_batch(&[col1], |row| if row == 0 { "x" } else { "y" }.to_string())
            .unwrap();
        assert_eq!(i1, vec![0, 1]);
        assert_eq!(total1, 2);

        let col2: ArrayRef = Arc::new(StringArray::from(vec!["y", "z", "x"]));
        let (i2, total2) = enc
            .encode_batch(&[col2], |row| match row {
                1 => "z".to_string(),
                _ => unreachable!("x and y already seen"),
            })
            .unwrap();
        assert_eq!(
            i2,
            vec![1, 2, 0],
            "existing groups get same indices, new 'z' gets idx 2"
        );
        assert_eq!(total2, 3);
    }

    #[test]
    fn integer_key_column() {
        let field = Field::new("device", DataType::Int64, true);
        let mut enc = GroupKeyEncoder::new(&[field]).unwrap();
        let col: ArrayRef = Arc::new(Int64Array::from(vec![42, 7, 42, 99, 7]));
        let (indices, total) = enc
            .encode_batch(&[col], |row_idx| match row_idx {
                0 => "42".to_string(),
                1 => "7".to_string(),
                3 => "99".to_string(),
                _ => unreachable!(),
            })
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(indices, vec![0, 1, 0, 2, 1]);
    }
}
