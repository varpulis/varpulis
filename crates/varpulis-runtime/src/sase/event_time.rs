//! Robust event-time handling (ET-01)

use super::types::SharedEvent;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for event-time processing
#[derive(Debug, Clone)]
pub struct EventTimeConfig {
    /// Maximum out-of-orderness tolerance for watermark generation
    /// The watermark will be: max_timestamp - max_out_of_orderness
    pub max_out_of_orderness: Duration,

    /// Additional tolerance for late events after watermark
    /// Events arriving within this window after the watermark are still processed
    pub allowed_lateness: Duration,

    /// Whether to emit late events to a side output
    pub emit_late_events: bool,
}

impl Default for EventTimeConfig {
    fn default() -> Self {
        Self {
            max_out_of_orderness: Duration::from_secs(0),
            allowed_lateness: Duration::from_secs(0),
            emit_late_events: false,
        }
    }
}

impl EventTimeConfig {
    /// Create a new event-time configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum out-of-orderness tolerance
    pub fn with_max_out_of_orderness(mut self, duration: Duration) -> Self {
        self.max_out_of_orderness = duration;
        self
    }

    /// Set allowed lateness tolerance
    pub fn with_allowed_lateness(mut self, duration: Duration) -> Self {
        self.allowed_lateness = duration;
        self
    }

    /// Enable emission of late events
    pub fn with_late_event_emission(mut self) -> Self {
        self.emit_late_events = true;
        self
    }
}

/// Result of event-time processing for an event
#[derive(Debug, Clone)]
pub enum EventTimeResult {
    /// Event is within expected time bounds
    OnTime,
    /// Event is late but within allowed lateness
    Late {
        /// How late the event is
        lateness: Duration,
    },
    /// Event is too late and was dropped
    TooLate {
        /// How late the event is
        lateness: Duration,
    },
}

/// Manages event-time processing with watermarks and late event handling
#[derive(Debug)]
pub struct EventTimeManager {
    config: EventTimeConfig,
    /// Current watermark
    watermark: Option<DateTime<Utc>>,
    /// Maximum observed event timestamp
    max_timestamp: Option<DateTime<Utc>>,
    /// Count of late events that were still processed
    late_events_accepted: u64,
    /// Count of late events that were dropped (too late)
    late_events_dropped: u64,
    /// Late events that were emitted for side output
    late_events_emitted: Vec<SharedEvent>,
}

impl EventTimeManager {
    /// Create a new event-time manager with the given configuration
    pub fn new(config: EventTimeConfig) -> Self {
        Self {
            config,
            watermark: None,
            max_timestamp: None,
            late_events_accepted: 0,
            late_events_dropped: 0,
            late_events_emitted: Vec::new(),
        }
    }

    /// Get current watermark
    pub fn watermark(&self) -> Option<DateTime<Utc>> {
        self.watermark
    }

    /// Get count of late events that were accepted
    pub fn late_events_accepted(&self) -> u64 {
        self.late_events_accepted
    }

    /// Get count of late events that were dropped
    pub fn late_events_dropped(&self) -> u64 {
        self.late_events_dropped
    }

    /// Take and clear emitted late events
    pub fn take_late_events(&mut self) -> Vec<SharedEvent> {
        std::mem::take(&mut self.late_events_emitted)
    }

    /// Process an event and determine its timeliness
    pub fn process_event(&mut self, event: &SharedEvent) -> EventTimeResult {
        let event_ts = event.timestamp;

        // Check if the event is late relative to watermark
        if let Some(wm) = self.watermark {
            if event_ts < wm {
                let lateness_chrono = wm - event_ts;
                let lateness = lateness_chrono
                    .to_std()
                    .unwrap_or(Duration::from_secs(u64::MAX));

                let allowed = chrono::Duration::from_std(self.config.allowed_lateness)
                    .unwrap_or(chrono::Duration::zero());

                if lateness_chrono > allowed {
                    // Too late, reject
                    self.late_events_dropped += 1;

                    // Optionally emit for side output
                    if self.config.emit_late_events {
                        self.late_events_emitted.push(Arc::clone(event));
                    }

                    return EventTimeResult::TooLate { lateness };
                } else {
                    // Late but acceptable
                    self.late_events_accepted += 1;
                    return EventTimeResult::Late { lateness };
                }
            }
        }

        // Update max_timestamp
        match self.max_timestamp {
            Some(max_ts) if event_ts > max_ts => {
                self.max_timestamp = Some(event_ts);
            }
            None => {
                self.max_timestamp = Some(event_ts);
            }
            _ => {}
        }

        // Update watermark
        self.update_watermark();

        EventTimeResult::OnTime
    }

    /// Update the watermark based on max_timestamp
    fn update_watermark(&mut self) {
        if let Some(max_ts) = self.max_timestamp {
            let out_of_orderness = chrono::Duration::from_std(self.config.max_out_of_orderness)
                .unwrap_or(chrono::Duration::zero());

            let new_watermark = max_ts - out_of_orderness;

            // Watermark never recedes
            match self.watermark {
                Some(wm) if new_watermark > wm => {
                    self.watermark = Some(new_watermark);
                }
                None => {
                    self.watermark = Some(new_watermark);
                }
                _ => {}
            }
        }
    }

    /// Manually advance the watermark
    pub fn advance_watermark(&mut self, new_watermark: DateTime<Utc>) {
        // Watermark never recedes
        match self.watermark {
            Some(wm) if new_watermark > wm => {
                self.watermark = Some(new_watermark);
            }
            None => {
                self.watermark = Some(new_watermark);
            }
            _ => {}
        }
    }

    /// Compute a deadline in event-time, handling overflow safely
    pub fn compute_deadline(start: DateTime<Utc>, timeout: Duration) -> Option<DateTime<Utc>> {
        chrono::Duration::from_std(timeout).ok().map(|d| start + d)
    }
}
