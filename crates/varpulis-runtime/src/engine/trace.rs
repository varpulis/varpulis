//! Pipeline trace/explain mode for Varpulis.
//!
//! When enabled, the trace collector records how each event flows through the
//! pipeline: which streams matched, which operators passed or blocked, pattern
//! state transitions, and emitted output events.
//!
//! The collector is designed to be zero-cost when disabled — callers check
//! [`TraceCollector::is_enabled`] before recording, and the compiler can
//! eliminate the branch entirely when the flag is `false`.

/// Trace entry recorded during pipeline execution.
#[derive(Debug, Clone)]
pub enum TraceEntry {
    /// Event was routed to a stream for processing.
    StreamMatched {
        stream_name: String,
        event_type: String,
    },
    /// An operator processed the event.
    OperatorResult {
        stream_name: String,
        op_name: String,
        /// `true` = event(s) passed through, `false` = filtered out / empty.
        passed: bool,
        /// Optional detail (e.g., "temperature=105 > 100").
        detail: Option<String>,
    },
    /// SASE pattern state changed.
    PatternState {
        stream_name: String,
        active_runs: usize,
        completed: usize,
    },
    /// Output event emitted from the pipeline.
    EventEmitted {
        stream_name: String,
        fields: Vec<(String, String)>,
    },
}

/// Collects trace entries during pipeline execution.
///
/// When `enabled` is `false`, [`record`](Self::record) is a no-op and the
/// compiler can optimise away the call sites.
#[derive(Debug)]
pub struct TraceCollector {
    enabled: bool,
    entries: Vec<TraceEntry>,
}

impl Default for TraceCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl TraceCollector {
    /// Create a new (disabled) trace collector.
    pub fn new() -> Self {
        Self {
            enabled: false,
            entries: Vec::new(),
        }
    }

    /// Enable or disable trace collection.
    #[inline]
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Whether the collector is active.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Record a trace entry. No-op when disabled.
    #[inline]
    pub fn record(&mut self, entry: TraceEntry) {
        if self.enabled {
            self.entries.push(entry);
        }
    }

    /// Drain all collected entries, returning them and leaving the collector empty.
    pub fn drain(&mut self) -> Vec<TraceEntry> {
        std::mem::take(&mut self.entries)
    }

    /// Whether there are no collected entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of collected entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_collector_does_not_record() {
        let mut tc = TraceCollector::new();
        assert!(!tc.is_enabled());
        tc.record(TraceEntry::StreamMatched {
            stream_name: "s1".into(),
            event_type: "Tick".into(),
        });
        assert!(tc.is_empty());
        assert_eq!(tc.len(), 0);
    }

    #[test]
    fn test_enabled_collector_records_and_drains() {
        let mut tc = TraceCollector::new();
        tc.set_enabled(true);
        assert!(tc.is_enabled());

        tc.record(TraceEntry::StreamMatched {
            stream_name: "Alerts".into(),
            event_type: "Temperature".into(),
        });
        tc.record(TraceEntry::OperatorResult {
            stream_name: "Alerts".into(),
            op_name: "Filter".into(),
            passed: true,
            detail: Some("temperature=105 > 100".into()),
        });
        tc.record(TraceEntry::PatternState {
            stream_name: "Alerts".into(),
            active_runs: 2,
            completed: 1,
        });
        tc.record(TraceEntry::EventEmitted {
            stream_name: "Alerts".into(),
            fields: vec![
                ("symbol".into(), "AAPL".into()),
                ("price".into(), "150.5".into()),
            ],
        });

        assert_eq!(tc.len(), 4);
        assert!(!tc.is_empty());

        let entries = tc.drain();
        assert_eq!(entries.len(), 4);
        assert!(tc.is_empty());

        // Verify entry types
        assert!(matches!(&entries[0], TraceEntry::StreamMatched { .. }));
        assert!(matches!(
            &entries[1],
            TraceEntry::OperatorResult { passed: true, .. }
        ));
        assert!(matches!(
            &entries[2],
            TraceEntry::PatternState { completed: 1, .. }
        ));
        assert!(matches!(&entries[3], TraceEntry::EventEmitted { .. }));
    }

    #[test]
    fn test_drain_returns_empty_when_no_entries() {
        let mut tc = TraceCollector::new();
        tc.set_enabled(true);
        let entries = tc.drain();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_toggle_enabled() {
        let mut tc = TraceCollector::new();
        tc.set_enabled(true);
        tc.record(TraceEntry::StreamMatched {
            stream_name: "s1".into(),
            event_type: "e1".into(),
        });
        assert_eq!(tc.len(), 1);

        tc.set_enabled(false);
        tc.record(TraceEntry::StreamMatched {
            stream_name: "s2".into(),
            event_type: "e2".into(),
        });
        // Should still be 1 — second record was ignored
        assert_eq!(tc.len(), 1);
    }
}
