//! Per-source watermark tracking for multi-source event-time processing.
//!
//! Tracks watermarks independently per source stream and computes the effective
//! watermark as the minimum across all sources. This ensures correct event-time
//! processing when events arrive from multiple sources with different latencies.

use crate::persistence::{SourceWatermarkCheckpoint, WatermarkCheckpoint};
use chrono::{DateTime, Duration, Utc};
use rustc_hash::FxHashMap;
use std::time::Instant;

/// Tracks watermarks for multiple event sources.
///
/// The effective watermark is the minimum watermark across all registered sources,
/// ensuring no source's events are prematurely considered late.
pub struct PerSourceWatermarkTracker {
    sources: FxHashMap<String, SourceWatermark>,
    effective_watermark: Option<DateTime<Utc>>,
}

struct SourceWatermark {
    watermark: Option<DateTime<Utc>>,
    max_timestamp: Option<DateTime<Utc>>,
    max_out_of_orderness: Duration,
    last_event_time: Option<Instant>,
}

impl PerSourceWatermarkTracker {
    /// Create a new empty tracker.
    pub fn new() -> Self {
        Self {
            sources: FxHashMap::default(),
            effective_watermark: None,
        }
    }

    /// Register a source with its maximum out-of-orderness tolerance.
    pub fn register_source(&mut self, name: &str, max_ooo: Duration) {
        self.sources.insert(
            name.to_string(),
            SourceWatermark {
                watermark: None,
                max_timestamp: None,
                max_out_of_orderness: max_ooo,
                last_event_time: None,
            },
        );
    }

    /// Observe an event from a source, updating its watermark.
    pub fn observe_event(&mut self, source: &str, event_ts: DateTime<Utc>) {
        if let Some(sw) = self.sources.get_mut(source) {
            sw.last_event_time = Some(Instant::now());

            // Update max timestamp
            let updated = match sw.max_timestamp {
                Some(max_ts) if event_ts > max_ts => {
                    sw.max_timestamp = Some(event_ts);
                    true
                }
                None => {
                    sw.max_timestamp = Some(event_ts);
                    true
                }
                _ => false,
            };

            // Recompute source watermark: max_timestamp - max_out_of_orderness
            if updated {
                if let Some(max_ts) = sw.max_timestamp {
                    let new_wm = max_ts - sw.max_out_of_orderness;
                    // Watermark never recedes
                    match sw.watermark {
                        Some(wm) if new_wm > wm => sw.watermark = Some(new_wm),
                        None => sw.watermark = Some(new_wm),
                        _ => {}
                    }
                }
            }

            self.recompute_effective();
        } else {
            // Auto-register unknown sources with zero out-of-orderness
            self.register_source(source, Duration::zero());
            self.observe_event(source, event_ts);
        }
    }

    /// Get the effective (minimum) watermark across all sources.
    pub fn effective_watermark(&self) -> Option<DateTime<Utc>> {
        self.effective_watermark
    }

    /// Manually advance a source's watermark (e.g., from upstream context).
    pub fn advance_source_watermark(&mut self, source: &str, wm: DateTime<Utc>) {
        if let Some(sw) = self.sources.get_mut(source) {
            match sw.watermark {
                Some(current) if wm > current => sw.watermark = Some(wm),
                None => sw.watermark = Some(wm),
                _ => {}
            }
            self.recompute_effective();
        }
    }

    /// Recompute the effective watermark as the minimum of all source watermarks.
    fn recompute_effective(&mut self) {
        if self.sources.is_empty() {
            self.effective_watermark = None;
            return;
        }

        let mut min_wm: Option<DateTime<Utc>> = None;
        for sw in self.sources.values() {
            match (min_wm, sw.watermark) {
                (Some(current_min), Some(source_wm)) => {
                    if source_wm < current_min {
                        min_wm = Some(source_wm);
                    }
                }
                (None, Some(source_wm)) => {
                    min_wm = Some(source_wm);
                }
                // If any source has no watermark yet, effective watermark stays at current
                // (we don't block on uninitialized sources)
                _ => {}
            }
        }

        // Set effective watermark to the min across all sources.
        // The effective watermark CAN decrease when a previously-uninitialized
        // source registers its first (lower) watermark. Per-source monotonicity
        // is enforced in observe_event/advance_source_watermark.
        if let Some(new) = min_wm {
            self.effective_watermark = Some(new);
        }
    }

    /// Create a checkpoint of the tracker state.
    pub fn checkpoint(&self) -> WatermarkCheckpoint {
        let sources = self
            .sources
            .iter()
            .map(|(name, sw)| {
                (
                    name.clone(),
                    SourceWatermarkCheckpoint {
                        watermark_ms: sw.watermark.map(|w| w.timestamp_millis()),
                        max_timestamp_ms: sw.max_timestamp.map(|t| t.timestamp_millis()),
                        max_out_of_orderness_ms: sw.max_out_of_orderness.num_milliseconds(),
                    },
                )
            })
            .collect();

        WatermarkCheckpoint {
            sources,
            effective_watermark_ms: self.effective_watermark.map(|w| w.timestamp_millis()),
        }
    }

    /// Restore tracker state from a checkpoint.
    pub fn restore(&mut self, cp: &WatermarkCheckpoint) {
        for (name, scp) in &cp.sources {
            let sw = self
                .sources
                .entry(name.clone())
                .or_insert_with(|| SourceWatermark {
                    watermark: None,
                    max_timestamp: None,
                    max_out_of_orderness: Duration::milliseconds(scp.max_out_of_orderness_ms),
                    last_event_time: None,
                });
            sw.watermark = scp.watermark_ms.and_then(DateTime::from_timestamp_millis);
            sw.max_timestamp = scp
                .max_timestamp_ms
                .and_then(DateTime::from_timestamp_millis);
            sw.max_out_of_orderness = Duration::milliseconds(scp.max_out_of_orderness_ms);
        }

        self.effective_watermark = cp
            .effective_watermark_ms
            .and_then(DateTime::from_timestamp_millis);
    }

    /// Check if any sources are registered.
    pub fn has_sources(&self) -> bool {
        !self.sources.is_empty()
    }
}

impl Default for PerSourceWatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_source() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("src1", Duration::seconds(5));

        let base = Utc::now();
        tracker.observe_event("src1", base);

        // Watermark should be base - 5s
        let wm = tracker.effective_watermark().unwrap();
        assert_eq!(wm, base - Duration::seconds(5));
    }

    #[test]
    fn test_two_sources_min_watermark() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("fast", Duration::seconds(1));
        tracker.register_source("slow", Duration::seconds(10));

        let base = Utc::now();
        tracker.observe_event("fast", base + Duration::seconds(20));
        tracker.observe_event("slow", base + Duration::seconds(15));

        // fast watermark = base+20 - 1 = base+19
        // slow watermark = base+15 - 10 = base+5
        // effective = min(base+19, base+5) = base+5
        let wm = tracker.effective_watermark().unwrap();
        assert_eq!(wm, base + Duration::seconds(5));
    }

    #[test]
    fn test_watermark_never_recedes() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("src1", Duration::seconds(0));

        let base = Utc::now();
        tracker.observe_event("src1", base + Duration::seconds(10));
        let wm1 = tracker.effective_watermark().unwrap();

        // Late event should not cause watermark to recede
        tracker.observe_event("src1", base + Duration::seconds(5));
        let wm2 = tracker.effective_watermark().unwrap();
        assert_eq!(wm1, wm2);
    }

    #[test]
    fn test_advance_source_watermark() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("upstream", Duration::seconds(0));

        let base = Utc::now();
        tracker.advance_source_watermark("upstream", base);

        assert_eq!(tracker.effective_watermark(), Some(base));
    }

    #[test]
    fn test_checkpoint_restore_roundtrip() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("src1", Duration::seconds(5));
        tracker.register_source("src2", Duration::seconds(10));

        let base = Utc::now();
        tracker.observe_event("src1", base + Duration::seconds(20));
        tracker.observe_event("src2", base + Duration::seconds(15));

        let cp = tracker.checkpoint();

        let mut restored = PerSourceWatermarkTracker::new();
        restored.restore(&cp);

        assert_eq!(
            tracker.effective_watermark().map(|w| w.timestamp_millis()),
            restored.effective_watermark().map(|w| w.timestamp_millis())
        );
    }
}
