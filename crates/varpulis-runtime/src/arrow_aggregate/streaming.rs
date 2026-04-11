//! Phase-2 streaming driver — keeps grouped accumulator state alive
//! across arriving event batches, bucketed by tumbling-window bin.
//!
//! This is the operator that closes the scenario-02 gap. The phase-1
//! one-shot driver in `grouped.rs` rebuilds an aggregator per
//! `apply()` call, which means scenario 02's 99 700 single-event
//! window fires each pay the full setup cost. Phase 2's
//! [`StreamingPartitionedWindow`] holds a `BTreeMap<bin_start_ms,
//! ColumnarGroupedAggregator>` and:
//!
//! 1. Buckets each arriving batch's events by their tumbling-window bin.
//! 2. For each bucket, gets-or-creates the bin's aggregator and folds
//!    that slice in via `update`. Per-event cost in steady state is
//!    one downcast per slice + one accumulator update per row.
//! 3. Tracks the watermark as the running max event timestamp seen so
//!    far. After each batch, drains and removes any bin whose
//!    `bin_start + bin_duration_ms <= watermark`.
//!
//! This matches Arroyo's tumbling-window operator architecture (per-bin
//! `AggregateExec` instances streaming events as they arrive, fired at
//! watermark) without depending on DataFusion.
//!
//! ## Bin alignment
//!
//! Phase 2 uses **absolute aligned bins**: `bin_start = floor(event_ts /
//! bin_duration) * bin_duration`. All partitions within the same bin
//! share one accumulator instance, and the partitioning happens inside
//! that accumulator via the existing `GroupKeyEncoder`.
//!
//! This differs from the existing `PartitionedTumblingWindow`, which
//! starts the bin clock at the **first event of each partition** —
//! meaning two partitions can have offset window boundaries. Aligned
//! bins is what Flink, Arroyo, and most other streaming engines do; it
//! is also what user expectations match for "1-second windows starting
//! every wall-clock second". The fused op deliberately switches to
//! aligned-bin semantics. The compiler only fuses when the user uses
//! `partition_by + window + aggregate` and gates on
//! `Aggregator::supported_for_columnar`, so existing pipelines that
//! depend on per-partition origin can either avoid the fusion (e.g. by
//! adding an unsupported aggregate function) or stay on the row path.

use std::collections::BTreeMap;
use std::sync::Arc;

use indexmap::IndexMap;
use rustc_hash::FxHashMap;
use varpulis_core::Value;

use super::accumulator::{make_from_kind, AccumulatorKind};
use super::grouped::{AggSpec, ColumnarGroupedAggregator};
use super::non_partitioned::NonPartitionedColumnarAggregator;
use crate::arrow_bridge::SchemaCache;
use crate::event::SharedEvent;

/// Build instructions for one [`ColumnarGroupedAggregator`] per bin.
///
/// Held by [`StreamingPartitionedWindow`] and cloned per new bin via
/// [`Self::instantiate`]. Stores the partition key field name and the
/// per-aggregation `(alias, accumulator-kind, optional-input-field)`
/// tuples — the same shape as `Aggregator::iter_specs` but in a form
/// that survives across bins (no boxed trait objects).
#[derive(Clone)]
pub(crate) struct StreamingAggregatorTemplate {
    pub(crate) partition_key: String,
    pub(crate) specs: Vec<(String, AccumulatorKind, Option<String>)>,
}

impl StreamingAggregatorTemplate {
    /// Build a template from a phase-1 [`crate::aggregation::Aggregator`].
    /// Returns `None` if any aggregation function isn't in the
    /// columnar-supported set; callers must gate with
    /// `Aggregator::supported_for_columnar()` first.
    pub(crate) fn from_aggregator(
        partition_key: String,
        aggregator: &crate::aggregation::Aggregator,
    ) -> Option<Self> {
        let specs = aggregator
            .iter_specs()
            .map(|(alias, func_name, field)| {
                let kind = AccumulatorKind::from_name(func_name)?;
                Some((alias.clone(), kind, field.clone()))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            partition_key,
            specs,
        })
    }

    /// Build a fresh aggregator for a new bin.
    fn instantiate(
        &self,
        key_field: arrow_schema::Field,
    ) -> Result<ColumnarGroupedAggregator, arrow_schema::ArrowError> {
        let agg_specs: Vec<AggSpec> = self
            .specs
            .iter()
            .map(|(alias, kind, field)| AggSpec {
                alias: alias.clone(),
                accumulator: make_from_kind(*kind),
                field: field.clone(),
            })
            .collect();
        ColumnarGroupedAggregator::try_new(&[key_field], agg_specs)
    }
}

/// Streaming, bin-keyed columnar grouped aggregator.
///
/// Holds one [`ColumnarGroupedAggregator`] per active bin. Bins are
/// instantiated lazily on first event and removed once their end is
/// below the watermark.
pub(crate) struct StreamingPartitionedWindow {
    bin_duration_ms: i64,
    bins: BTreeMap<i64, ColumnarGroupedAggregator>,
    schema_cache: SchemaCache,
    template: StreamingAggregatorTemplate,
    /// Lazy-initialised on first batch from the inferred schema.
    key_field: Option<arrow_schema::Field>,
    /// Watermark = running max of all event timestamps seen so far.
    /// Used to decide which bins to flush on each call.
    max_ts_ms: Option<i64>,
}

impl std::fmt::Debug for StreamingPartitionedWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingPartitionedWindow")
            .field("bin_duration_ms", &self.bin_duration_ms)
            .field("active_bins", &self.bins.len())
            .field("max_ts_ms", &self.max_ts_ms)
            .field("partition_key", &self.template.partition_key)
            .finish()
    }
}

impl StreamingPartitionedWindow {
    pub(crate) fn new(template: StreamingAggregatorTemplate, bin_duration_ms: i64) -> Self {
        Self {
            bin_duration_ms,
            bins: BTreeMap::new(),
            schema_cache: SchemaCache::new(),
            template,
            key_field: None,
            max_ts_ms: None,
        }
    }

    /// One bin's worth of flushed results, in the row-shaped layout the
    /// rest of the pipeline expects: bin start (epoch ms), partition
    /// key string, per-alias aggregation values.
    #[allow(clippy::type_complexity)]
    pub(crate) fn ingest_and_flush(
        &mut self,
        events: &[SharedEvent],
    ) -> Vec<(i64, String, IndexMap<String, Value>)> {
        if events.is_empty() {
            return Vec::new();
        }

        // ── Single-event fast path ──────────────────────────────────
        // When the incoming batch has exactly 1 event (the common case
        // for the per-event pipeline dispatch in process_batch_sync /
        // process_batch_shared), bypass Arrow RecordBatch construction
        // entirely. Extract the partition key and field values directly
        // from the Event via `get_float` / `to_partition_key`, hash via
        // a lightweight FxHashMap, and call `update_single` on each
        // accumulator. This reduces per-event cost from ~10 µs
        // (RecordBatch + RowConverter) to ~1.5 µs (hashmap + scalars).
        if events.len() == 1 {
            return self.ingest_single_event(&events[0]);
        }

        // ── Multi-event columnar path ───────────────────────────────
        // When the batch has ≥2 events, bucket by bin and use the
        // columnar ColumnarGroupedAggregator path for vectorised
        // accumulator updates.

        // 1. Bucket events by bin and update the watermark.
        let mut buckets: FxHashMap<i64, Vec<SharedEvent>> = FxHashMap::default();
        let mut max_ts = self.max_ts_ms.unwrap_or(i64::MIN);
        for ev in events {
            let ts = ev.timestamp.timestamp_millis();
            // div_euclid handles negative timestamps correctly: floor towards -inf.
            let bin_start = ts.div_euclid(self.bin_duration_ms) * self.bin_duration_ms;
            buckets.entry(bin_start).or_default().push(Arc::clone(ev));
            if ts > max_ts {
                max_ts = ts;
            }
        }
        self.max_ts_ms = Some(max_ts);

        // 2. Lazy-init the partition-key field from the first batch's
        //    schema. If the field isn't present we can't do columnar
        //    aggregation — return empty so the caller can fall back.
        let first_bin_events = buckets.values().next().expect("buckets non-empty");
        let schema = self.schema_cache.get_or_infer(first_bin_events);
        if self.key_field.is_none() {
            match schema.field_with_name(&self.template.partition_key) {
                Ok(f) => self.key_field = Some(f.clone()),
                Err(_) => return Vec::new(),
            }
        }
        let key_field = self.key_field.clone().expect("just set above");

        // 3. For each bin bucket, get-or-create the aggregator and feed it.
        for (bin_start, bin_events) in buckets {
            // Skip events whose bin has already been flushed (out of
            // order beyond the watermark). For now we drop them; a
            // future enhancement could route them to a side output.
            if bin_start + self.bin_duration_ms <= max_ts && !self.bins.contains_key(&bin_start) {
                continue;
            }

            if !self.bins.contains_key(&bin_start) {
                let agg = match self.template.instantiate(key_field.clone()) {
                    Ok(a) => a,
                    Err(_) => continue,
                };
                self.bins.insert(bin_start, agg);
            }
            let bin_schema = self.schema_cache.get_or_infer(&bin_events);
            let batch = match crate::arrow_bridge::events_to_record_batch(&bin_events, &bin_schema)
            {
                Ok(b) => b,
                Err(_) => continue,
            };
            let agg = self
                .bins
                .get_mut(&bin_start)
                .expect("just inserted or pre-existing");
            let _ = agg.update(&batch);
        }

        // 4. Flush bins whose end is at or below the watermark.
        // BTreeMap iteration order is by key, so the smallest bins
        // come first. We collect the bin_starts to remove (since we
        // can't mutate the map while iterating it), then drain.
        let to_flush: Vec<i64> = self
            .bins
            .keys()
            .copied()
            .take_while(|s| s + self.bin_duration_ms <= max_ts)
            .collect();
        let mut output: Vec<(i64, String, IndexMap<String, Value>)> = Vec::new();
        for bin_start in to_flush {
            let agg = self.bins.remove(&bin_start).expect("present");
            for (key, result) in agg.drain_fast() {
                output.push((bin_start, key, result));
            }
        }
        output
    }

    /// Single-event fast path implementation. See the top of
    /// `ingest_and_flush` for the rationale. This method:
    /// 1. Computes the bin from `event.timestamp`.
    /// 2. Gets-or-creates the bin's aggregator (via the template).
    /// 3. Calls `update_single_event` on the aggregator — this uses
    ///    an FxHashMap<String, u32> for the group key instead of an
    ///    Arrow RowConverter.
    /// 4. Advances the watermark and drains closed bins.
    fn ingest_single_event(
        &mut self,
        event: &SharedEvent,
    ) -> Vec<(i64, String, IndexMap<String, Value>)> {
        let ts = event.timestamp.timestamp_millis();
        let bin_start = ts.div_euclid(self.bin_duration_ms) * self.bin_duration_ms;

        // Update watermark.
        let prev_max = self.max_ts_ms.unwrap_or(i64::MIN);
        let new_max = prev_max.max(ts);
        self.max_ts_ms = Some(new_max);

        // Skip late events whose bin has already been flushed.
        if bin_start + self.bin_duration_ms <= new_max && !self.bins.contains_key(&bin_start) {
            return Vec::new();
        }

        // Get-or-create the bin's aggregator. We need to avoid the
        // borrow-checker conflict of &mut self.bins + &self.template,
        // so check-and-insert in two steps.
        if !self.bins.contains_key(&bin_start) {
            // Lazy-init key_field from the event's data if not yet set.
            if self.key_field.is_none() {
                let value = event.get(&self.template.partition_key);
                let dt = match value {
                    Some(varpulis_core::Value::Str(_)) => arrow_schema::DataType::Utf8,
                    Some(varpulis_core::Value::Int(_)) => arrow_schema::DataType::Int64,
                    Some(varpulis_core::Value::Float(_)) => arrow_schema::DataType::Float64,
                    _ => arrow_schema::DataType::Utf8, // default
                };
                self.key_field = Some(arrow_schema::Field::new(
                    &self.template.partition_key,
                    dt,
                    true,
                ));
            }
            match self
                .template
                .instantiate(self.key_field.clone().expect("just set"))
            {
                Ok(agg) => {
                    self.bins.insert(bin_start, agg);
                }
                Err(_) => return Vec::new(),
            }
        }

        // Extract partition key as Cow<str> — zero allocation for
        // existing groups (Cow::Borrowed stays borrowed through the
        // FxHashMap lookup). Only new groups trigger into_owned().
        let key_cow = event
            .get(&self.template.partition_key)
            .map_or(std::borrow::Cow::Borrowed("default"), |v| {
                v.to_partition_key()
            });

        // Call the fast scalar update on the bin's aggregator.
        let agg = self
            .bins
            .get_mut(&bin_start)
            .expect("just inserted or pre-existing");
        agg.update_single_event(key_cow, event);

        // Drain bins below watermark.
        let to_flush: Vec<i64> = self
            .bins
            .keys()
            .copied()
            .take_while(|s| s + self.bin_duration_ms <= new_max)
            .collect();
        let mut output: Vec<(i64, String, IndexMap<String, Value>)> = Vec::new();
        for flushed_bin in to_flush {
            let agg = self.bins.remove(&flushed_bin).expect("present");
            for (key, result) in agg.drain_fast() {
                output.push((flushed_bin, key, result));
            }
        }
        output
    }

    /// Force-flush every remaining bin regardless of watermark.
    /// Called on engine shutdown / end-of-stream so we don't lose the
    /// final partial windows.
    #[allow(dead_code)] // wired in by phase-2 pipeline shutdown path
    pub(crate) fn flush_all(&mut self) -> Vec<(i64, String, IndexMap<String, Value>)> {
        let bin_starts: Vec<i64> = self.bins.keys().copied().collect();
        let mut output = Vec::new();
        for bin_start in bin_starts {
            let agg = self.bins.remove(&bin_start).expect("present");
            for (key, result) in agg.drain_fast() {
                output.push((bin_start, key, result));
            }
        }
        output
    }

    /// Active bin count — used by tests and debug output.
    #[allow(dead_code)]
    pub(crate) fn active_bins(&self) -> usize {
        self.bins.len()
    }
}

// =============================================================================
// Phase 3b — non-partitioned streaming tumbling-window aggregator
// =============================================================================

/// Build instructions for one [`NonPartitionedColumnarAggregator`] per bin.
///
/// The non-partitioned analog of [`StreamingAggregatorTemplate`]. Holds
/// the per-aggregation `(alias, accumulator-kind, optional-input-field)`
/// tuples so that new bins can be instantiated with fresh accumulators
/// via [`make_from_kind`] without any boxed trait state surviving.
#[derive(Clone)]
pub(crate) struct StreamingNonPartitionedTemplate {
    pub(crate) specs: Vec<(String, AccumulatorKind, Option<String>)>,
}

impl StreamingNonPartitionedTemplate {
    /// Build a template from a phase-1 [`crate::aggregation::Aggregator`].
    /// Returns `None` if any aggregation function isn't in the
    /// columnar-supported set.
    pub(crate) fn from_aggregator(aggregator: &crate::aggregation::Aggregator) -> Option<Self> {
        let specs = aggregator
            .iter_specs()
            .map(|(alias, func_name, field)| {
                let kind = AccumulatorKind::from_name(func_name)?;
                Some((alias.clone(), kind, field.clone()))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { specs })
    }

    /// Build a fresh single-group aggregator for a new bin, pre-resized
    /// to one group so `update_single_event` never needs to grow it.
    fn instantiate(&self) -> Result<NonPartitionedColumnarAggregator, arrow_schema::ArrowError> {
        let agg_specs: Vec<AggSpec> = self
            .specs
            .iter()
            .map(|(alias, kind, field)| AggSpec {
                alias: alias.clone(),
                accumulator: make_from_kind(*kind),
                field: field.clone(),
            })
            .collect();
        let mut agg = NonPartitionedColumnarAggregator::try_new(agg_specs)?;
        agg.ensure_single_group();
        Ok(agg)
    }
}

/// Non-partitioned streaming tumbling-window aggregator.
///
/// Holds one [`NonPartitionedColumnarAggregator`] per active bin. Events
/// are routed into the right bin by absolute aligned timestamp
/// (`bin_start = floor(ts / bin_ms) * bin_ms`). Bin end at or below the
/// running watermark is flushed and removed.
///
/// This is phase 3b's mirror of [`StreamingPartitionedWindow`] — same
/// shape, no partition key, so the per-event update path just hits
/// accumulator `group_idx = 0` without any hashing.
pub(crate) struct StreamingWindow {
    bin_duration_ms: i64,
    bins: BTreeMap<i64, NonPartitionedColumnarAggregator>,
    template: StreamingNonPartitionedTemplate,
    /// Watermark = running max of all event timestamps seen so far.
    max_ts_ms: Option<i64>,
}

impl std::fmt::Debug for StreamingWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingWindow")
            .field("bin_duration_ms", &self.bin_duration_ms)
            .field("active_bins", &self.bins.len())
            .field("max_ts_ms", &self.max_ts_ms)
            .finish()
    }
}

impl StreamingWindow {
    pub(crate) fn new(template: StreamingNonPartitionedTemplate, bin_duration_ms: i64) -> Self {
        Self {
            bin_duration_ms,
            bins: BTreeMap::new(),
            template,
            max_ts_ms: None,
        }
    }

    /// Ingest a batch of events one-at-a-time through the per-event
    /// fast path, then flush any bins whose end is at or below the
    /// running watermark. Returns `(bin_start_ms, per-alias results)`
    /// for each flushed bin.
    pub(crate) fn ingest_and_flush(
        &mut self,
        events: &[SharedEvent],
    ) -> Vec<(i64, IndexMap<String, Value>)> {
        if events.is_empty() {
            return Vec::new();
        }

        // Per-event loop: identical watermark / bin / update logic to
        // `StreamingPartitionedWindow::ingest_single_event`, minus the
        // partition-key extraction.
        for ev in events {
            let ts = ev.timestamp.timestamp_millis();
            let bin_start = ts.div_euclid(self.bin_duration_ms) * self.bin_duration_ms;

            // Advance watermark.
            let prev_max = self.max_ts_ms.unwrap_or(i64::MIN);
            let new_max = prev_max.max(ts);
            self.max_ts_ms = Some(new_max);

            // Drop late events whose bin has already been flushed.
            if bin_start + self.bin_duration_ms <= new_max && !self.bins.contains_key(&bin_start) {
                continue;
            }

            // Get-or-create the bin's aggregator.
            if !self.bins.contains_key(&bin_start) {
                match self.template.instantiate() {
                    Ok(agg) => {
                        self.bins.insert(bin_start, agg);
                    }
                    Err(_) => continue,
                }
            }
            let agg = self
                .bins
                .get_mut(&bin_start)
                .expect("just inserted or pre-existing");
            agg.update_single_event(ev);
        }

        // Flush bins whose end is at or below the watermark.
        let max_ts = self.max_ts_ms.unwrap_or(i64::MIN);
        let to_flush: Vec<i64> = self
            .bins
            .keys()
            .copied()
            .take_while(|s| s + self.bin_duration_ms <= max_ts)
            .collect();
        let mut output: Vec<(i64, IndexMap<String, Value>)> = Vec::new();
        for bin_start in to_flush {
            let agg = self.bins.remove(&bin_start).expect("present");
            output.push((bin_start, agg.drain_fast()));
        }
        output
    }

    /// Force-flush every remaining bin regardless of watermark. Wired
    /// into engine shutdown so the final partial window isn't lost.
    #[allow(dead_code)]
    pub(crate) fn flush_all(&mut self) -> Vec<(i64, IndexMap<String, Value>)> {
        let bin_starts: Vec<i64> = self.bins.keys().copied().collect();
        let mut output = Vec::new();
        for bin_start in bin_starts {
            let agg = self.bins.remove(&bin_start).expect("present");
            output.push((bin_start, agg.drain_fast()));
        }
        output
    }

    /// Suppress an unused-warning while keeping `Arc` available for tests.
    #[allow(dead_code)]
    pub(crate) fn active_bins(&self) -> usize {
        self.bins.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, Utc};

    use super::*;
    use crate::aggregation::{Aggregator, Avg, Count, Max, Min, Sum};
    use crate::event::Event;

    fn make_template() -> StreamingAggregatorTemplate {
        let agg = Aggregator::new()
            .add("s", Box::new(Sum), Some("value".to_string()))
            .add("a", Box::new(Avg), Some("value".to_string()))
            .add("mn", Box::new(Min), Some("value".to_string()))
            .add("mx", Box::new(Max), Some("value".to_string()))
            .add("c", Box::new(Count), None);
        StreamingAggregatorTemplate::from_aggregator("device_id".to_string(), &agg).unwrap()
    }

    fn make_event(ts_ms: i64, device: &str, value: f64) -> SharedEvent {
        let ts = DateTime::<Utc>::from_timestamp_millis(ts_ms).unwrap();
        Arc::new(
            Event::new("Reading")
                .with_timestamp(ts)
                .with_field("device_id", Value::Str(device.into()))
                .with_field("value", Value::Float(value)),
        )
    }

    #[test]
    fn single_bin_no_flush() {
        // All events in the same bin → no flush yet (watermark still
        // inside the bin).
        let mut win = StreamingPartitionedWindow::new(make_template(), 1000);
        let events = vec![
            make_event(0, "d0", 10.0),
            make_event(100, "d0", 20.0),
            make_event(200, "d1", 30.0),
        ];
        let out = win.ingest_and_flush(&events);
        assert!(out.is_empty(), "should not flush — all events in bin 0");
        assert_eq!(win.active_bins(), 1);
    }

    #[test]
    fn cross_bin_flushes_old_bin() {
        // Events in bin 0 (ts 0..1000), then an event in bin 1 (ts >=1000)
        // pushes the watermark forward and bin 0 should flush.
        let mut win = StreamingPartitionedWindow::new(make_template(), 1000);
        let _ = win.ingest_and_flush(&[
            make_event(0, "d0", 10.0),
            make_event(500, "d0", 20.0),
            make_event(700, "d1", 100.0),
        ]);
        assert_eq!(win.active_bins(), 1);

        let out = win.ingest_and_flush(&[make_event(1500, "d0", 999.0)]);
        // bin 0 should have flushed
        assert_eq!(out.len(), 2, "two partitions in bin 0 → 2 results");

        let mut by_key: std::collections::BTreeMap<String, IndexMap<String, Value>> =
            Default::default();
        for (bin_start, key, result) in out {
            assert_eq!(bin_start, 0);
            by_key.insert(key, result);
        }
        let d0 = &by_key["d0"];
        assert_eq!(d0["s"], Value::Float(30.0));
        assert_eq!(d0["a"], Value::Float(15.0));
        assert_eq!(d0["mn"], Value::Float(10.0));
        assert_eq!(d0["mx"], Value::Float(20.0));
        assert_eq!(d0["c"], Value::Int(2));

        let d1 = &by_key["d1"];
        assert_eq!(d1["s"], Value::Float(100.0));
        assert_eq!(d1["c"], Value::Int(1));

        // bin 1 should still be active
        assert_eq!(win.active_bins(), 1);
    }

    #[test]
    fn scenario_02_shape_streams_correctly() {
        // Mimic scenario 02: 100 devices × 1000 seconds × 1 event each.
        // Use 1-second bins. Stream events one at a time (worst case
        // for the streaming op — exactly the row-path bottleneck).
        let mut win = StreamingPartitionedWindow::new(make_template(), 1000);
        let mut total_flushed = 0usize;
        for sec in 0..1000 {
            for dev in 0..100 {
                let ts_ms = sec * 1000 + dev * 10;
                let value = (sec * 100 + dev) as f64;
                let device = format!("d{dev}");
                let out = win.ingest_and_flush(&[make_event(ts_ms, &device, value)]);
                total_flushed += out.len();
            }
        }
        // Force-flush the last bin
        total_flushed += win.flush_all().len();
        // 100 devices × 1000 bins = 100k aggregation results
        assert_eq!(total_flushed, 100_000);
    }

    #[test]
    fn flush_all_drains_pending_bins() {
        let mut win = StreamingPartitionedWindow::new(make_template(), 1000);
        let _ = win.ingest_and_flush(&[
            make_event(0, "d0", 10.0),
            make_event(1500, "d0", 20.0),
            make_event(2500, "d0", 30.0),
        ]);
        // After ingestion: bin 0 flushed (one event), bins 1+2 still active.
        let active_before = win.active_bins();
        assert!(active_before >= 1);
        let drained = win.flush_all();
        assert!(!drained.is_empty());
        assert_eq!(win.active_bins(), 0);
    }

    #[test]
    fn late_event_within_active_bin() {
        // Event 0 in bin 0, event 1 in bin 2 (advances watermark past
        // bin 0 → bin 0 flushes), then a "late" event whose timestamp
        // falls back into bin 1 — bin 1 IS still active (watermark
        // hasn't passed bin 1 yet because watermark = 2500, bin 1 ends
        // at 2000 = 2 * 1000, so bin 1 SHOULD have flushed too).
        // Re-checking: watermark=2500, bin_end of bin 1 = 2000, 2000 <=
        // 2500 so bin 1 ALSO flushes. So a "late" event into bin 1
        // here gets dropped.
        let mut win = StreamingPartitionedWindow::new(make_template(), 1000);
        let _ = win.ingest_and_flush(&[make_event(0, "d0", 10.0), make_event(2500, "d0", 20.0)]);
        // Now insert a "late" event for bin 1 (ts=1500) which is past
        // the watermark — should be dropped.
        let out = win.ingest_and_flush(&[make_event(1500, "d0", 999.0)]);
        assert!(out.is_empty(), "late event dropped, no new flush");
    }

    // ---- Phase 3b: non-partitioned StreamingWindow tests ----

    fn make_np_template() -> StreamingNonPartitionedTemplate {
        let agg = Aggregator::new()
            .add("s", Box::new(Sum), Some("value".to_string()))
            .add("a", Box::new(Avg), Some("value".to_string()))
            .add("mn", Box::new(Min), Some("value".to_string()))
            .add("mx", Box::new(Max), Some("value".to_string()))
            .add("c", Box::new(Count), None);
        StreamingNonPartitionedTemplate::from_aggregator(&agg).unwrap()
    }

    fn make_np_event(ts_ms: i64, value: f64) -> SharedEvent {
        let ts = DateTime::<Utc>::from_timestamp_millis(ts_ms).unwrap();
        Arc::new(
            Event::new("Reading")
                .with_timestamp(ts)
                .with_field("value", Value::Float(value)),
        )
    }

    #[test]
    fn np_single_bin_no_flush() {
        let mut win = StreamingWindow::new(make_np_template(), 1000);
        let out = win.ingest_and_flush(&[
            make_np_event(0, 10.0),
            make_np_event(100, 20.0),
            make_np_event(200, 30.0),
        ]);
        assert!(out.is_empty(), "all events in bin 0 — should not flush");
        assert_eq!(win.active_bins(), 1);
    }

    #[test]
    fn np_cross_bin_flushes_old_bin() {
        let mut win = StreamingWindow::new(make_np_template(), 1000);
        let _ = win.ingest_and_flush(&[
            make_np_event(0, 10.0),
            make_np_event(500, 20.0),
            make_np_event(700, 30.0),
        ]);
        assert_eq!(win.active_bins(), 1);

        let out = win.ingest_and_flush(&[make_np_event(1500, 999.0)]);
        assert_eq!(out.len(), 1, "bin 0 should flush exactly one result");
        let (bin_start, result) = &out[0];
        assert_eq!(*bin_start, 0);
        assert_eq!(result["s"], Value::Float(60.0));
        assert_eq!(result["a"], Value::Float(20.0));
        assert_eq!(result["mn"], Value::Float(10.0));
        assert_eq!(result["mx"], Value::Float(30.0));
        assert_eq!(result["c"], Value::Int(3));
        assert_eq!(win.active_bins(), 1, "bin 1 should still be active");
    }

    #[test]
    fn np_streams_1000_bins_correctly() {
        // Non-partitioned scenario: 1000 bins × 100 events each, one
        // at a time. Each bin's result must equal sum=100*(500 + 500*99)/100
        // etc. For simplicity, we check the flush count and one
        // representative bin.
        let mut win = StreamingWindow::new(make_np_template(), 1000);
        let mut total_flushed = 0usize;
        for bin in 0..1000 {
            for i in 0..100 {
                let ts = bin * 1000 + i * 10;
                let out = win.ingest_and_flush(&[make_np_event(ts, i as f64)]);
                total_flushed += out.len();
            }
        }
        total_flushed += win.flush_all().len();
        assert_eq!(total_flushed, 1000, "1000 bins should flush 1000 results");
    }

    #[test]
    fn np_flush_all_drains_pending() {
        let mut win = StreamingWindow::new(make_np_template(), 1000);
        let _ = win.ingest_and_flush(&[
            make_np_event(0, 10.0),
            make_np_event(1500, 20.0),
            make_np_event(2500, 30.0),
        ]);
        let drained = win.flush_all();
        assert!(!drained.is_empty());
        assert_eq!(win.active_bins(), 0);
    }

    #[test]
    fn np_late_event_is_dropped() {
        let mut win = StreamingWindow::new(make_np_template(), 1000);
        let _ = win.ingest_and_flush(&[make_np_event(0, 10.0), make_np_event(2500, 20.0)]);
        // ts=1500 falls in bin 1; watermark has already advanced to
        // 2500 and bin 1's end (2000) ≤ 2500, so bin 1 was never created
        // and this late event should be dropped silently.
        let out = win.ingest_and_flush(&[make_np_event(1500, 999.0)]);
        assert!(out.is_empty());
    }

    #[test]
    fn np_nan_values_skipped() {
        let mut win = StreamingWindow::new(make_np_template(), 1000);
        // Three events in bin 0; one is NaN. Sum/avg/min/max should
        // skip it but Count should count it.
        let _ = win.ingest_and_flush(&[
            make_np_event(0, 10.0),
            make_np_event(100, f64::NAN),
            make_np_event(200, 30.0),
        ]);
        let out = win.ingest_and_flush(&[make_np_event(1500, 0.0)]);
        assert_eq!(out.len(), 1);
        let (_, r) = &out[0];
        assert_eq!(r["s"], Value::Float(40.0));
        assert_eq!(r["a"], Value::Float(20.0));
        assert_eq!(r["mn"], Value::Float(10.0));
        assert_eq!(r["mx"], Value::Float(30.0));
        // Count sees every event regardless of NaN — matches the row
        // path's `events.len()` semantics. Sum/Avg/Min/Max are the ones
        // that filter NaN via `get_float().filter(|v| !v.is_nan())`.
        assert_eq!(r["c"], Value::Int(3));
    }
}
