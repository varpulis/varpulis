//! Window implementations for stream processing
//!
//! Includes:
//! - Time-based windows (tumbling, sliding)
//! - Count-based windows
//! - Partitioned windows
//! - Delay buffers (rstream equivalent)

use crate::event::{Event, SharedEvent};
use crate::persistence::{PartitionedWindowCheckpoint, SerializableEvent, WindowCheckpoint};
use chrono::{DateTime, Duration, Utc};
use std::collections::VecDeque;
use std::sync::Arc;

/// A tumbling window that collects events over a fixed duration
pub struct TumblingWindow {
    duration: Duration,
    events: Vec<SharedEvent>,
    window_start: Option<DateTime<Utc>>,
}

impl TumblingWindow {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            events: Vec::new(),
            window_start: None,
        }
    }

    /// Add a shared event to the window, returning completed window if triggered.
    pub fn add_shared(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let event_time = event.timestamp;

        // Initialize window start on first event
        if self.window_start.is_none() {
            self.window_start = Some(event_time);
        }

        // Safe: we just set it above if it was None
        let window_start = self.window_start?;
        let window_end = window_start + self.duration;

        if event_time >= window_end {
            // Window is complete, emit and start new window
            let completed = std::mem::take(&mut self.events);
            self.window_start = Some(event_time);
            self.events.push(event);
            Some(completed)
        } else {
            self.events.push(event);
            None
        }
    }

    /// Add an event to the window (wraps in Arc).
    #[allow(dead_code)]
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.add_shared(Arc::new(event))
            .map(|events| events.into_iter().map(|e| (*e).clone()).collect())
    }

    /// Flush all events as SharedEvent references.
    pub fn flush_shared(&mut self) -> Vec<SharedEvent> {
        std::mem::take(&mut self.events)
    }

    /// Flush all events (clones for backward compatibility).
    #[allow(dead_code)]
    pub fn flush(&mut self) -> Vec<Event> {
        self.flush_shared()
            .into_iter()
            .map(|e| (*e).clone())
            .collect()
    }

    /// Create a checkpoint of the current window state.
    pub fn checkpoint(&self) -> WindowCheckpoint {
        WindowCheckpoint {
            events: self
                .events
                .iter()
                .map(|e| SerializableEvent::from(e.as_ref()))
                .collect(),
            window_start_ms: self.window_start.map(|t| t.timestamp_millis()),
            last_emit_ms: None,
            partitions: std::collections::HashMap::new(),
        }
    }

    /// Restore window state from a checkpoint.
    pub fn restore(&mut self, cp: &WindowCheckpoint) {
        self.events = cp
            .events
            .iter()
            .map(|se| Arc::new(Event::from(se.clone())))
            .collect();
        self.window_start = cp.window_start_ms.and_then(DateTime::from_timestamp_millis);
    }

    /// Advance watermark — close window if watermark passes window end.
    pub fn advance_watermark(&mut self, wm: DateTime<Utc>) -> Option<Vec<SharedEvent>> {
        if let Some(start) = self.window_start {
            if wm >= start + self.duration && !self.events.is_empty() {
                let completed = std::mem::take(&mut self.events);
                self.window_start = Some(wm);
                return Some(completed);
            }
        }
        None
    }
}

/// A sliding window that maintains overlapping windows
pub struct SlidingWindow {
    window_size: Duration,
    slide_interval: Duration,
    events: VecDeque<SharedEvent>,
    last_emit: Option<DateTime<Utc>>,
}

impl SlidingWindow {
    pub fn new(window_size: Duration, slide_interval: Duration) -> Self {
        Self {
            window_size,
            slide_interval,
            events: VecDeque::new(),
            last_emit: None,
        }
    }

    /// Add a shared event, returning window contents if slide interval reached.
    pub fn add_shared(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let event_time = event.timestamp;
        self.events.push_back(event);

        // Remove old events outside window using binary search + drain
        // This is O(log n + k) where k is expired events, vs O(k) pop_front loops
        let cutoff = event_time - self.window_size;
        let expired_count = self
            .events
            .iter()
            .position(|e| e.timestamp >= cutoff)
            .unwrap_or(self.events.len());

        if expired_count > 0 {
            // Drain all expired events in one operation
            self.events.drain(0..expired_count);
        }

        // Check if we should emit based on slide interval
        let should_emit = match self.last_emit {
            None => true,
            Some(last) => event_time >= last + self.slide_interval,
        };

        should_emit.then(|| {
            self.last_emit = Some(event_time);
            self.events.iter().map(Arc::clone).collect()
        })
    }

    /// Add an event (wraps in Arc).
    #[allow(dead_code)]
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.add_shared(Arc::new(event))
            .map(|events| events.into_iter().map(|e| (*e).clone()).collect())
    }

    /// Get current window contents as shared references.
    pub fn current_shared(&self) -> Vec<SharedEvent> {
        self.events.iter().map(Arc::clone).collect()
    }

    /// Get current window contents (clones for backward compatibility).
    #[allow(dead_code)]
    pub fn current(&self) -> Vec<Event> {
        self.events.iter().map(|e| (**e).clone()).collect()
    }

    /// Create a checkpoint of the current window state.
    pub fn checkpoint(&self) -> WindowCheckpoint {
        WindowCheckpoint {
            events: self
                .events
                .iter()
                .map(|e| SerializableEvent::from(e.as_ref()))
                .collect(),
            window_start_ms: None,
            last_emit_ms: self.last_emit.map(|t| t.timestamp_millis()),
            partitions: std::collections::HashMap::new(),
        }
    }

    /// Restore window state from a checkpoint.
    pub fn restore(&mut self, cp: &WindowCheckpoint) {
        self.events = cp
            .events
            .iter()
            .map(|se| Arc::new(Event::from(se.clone())))
            .collect::<Vec<_>>()
            .into();
        self.last_emit = cp.last_emit_ms.and_then(DateTime::from_timestamp_millis);
    }

    /// Advance watermark — emit window if slide interval has passed relative to watermark.
    pub fn advance_watermark(&mut self, wm: DateTime<Utc>) -> Option<Vec<SharedEvent>> {
        // Remove expired events
        let cutoff = wm - self.window_size;
        let expired_count = self
            .events
            .iter()
            .position(|e| e.timestamp >= cutoff)
            .unwrap_or(self.events.len());
        if expired_count > 0 {
            self.events.drain(0..expired_count);
        }

        let should_emit = match self.last_emit {
            None => !self.events.is_empty(),
            Some(last) => wm >= last + self.slide_interval && !self.events.is_empty(),
        };

        should_emit.then(|| {
            self.last_emit = Some(wm);
            self.events.iter().map(Arc::clone).collect()
        })
    }
}

/// A count-based window that emits after collecting N events
pub struct CountWindow {
    count: usize,
    events: Vec<SharedEvent>,
}

impl CountWindow {
    pub fn new(count: usize) -> Self {
        Self {
            count,
            events: Vec::with_capacity(count),
        }
    }

    /// Add a shared event, returning completed window if count reached.
    pub fn add_shared(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        self.events.push(event);

        (self.events.len() >= self.count).then(|| std::mem::take(&mut self.events))
    }

    /// Add an event (wraps in Arc).
    #[allow(dead_code)]
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.add_shared(Arc::new(event))
            .map(|events| events.into_iter().map(|e| (*e).clone()).collect())
    }

    /// Flush all events as SharedEvent references.
    pub fn flush_shared(&mut self) -> Vec<SharedEvent> {
        std::mem::take(&mut self.events)
    }

    /// Flush all events (clones for backward compatibility).
    #[allow(dead_code)]
    pub fn flush(&mut self) -> Vec<Event> {
        self.flush_shared()
            .into_iter()
            .map(|e| (*e).clone())
            .collect()
    }

    /// Get current count of events in buffer (for debugging)
    pub fn current_count(&self) -> usize {
        self.events.len()
    }

    /// Create a checkpoint of the current window state.
    pub fn checkpoint(&self) -> WindowCheckpoint {
        WindowCheckpoint {
            events: self
                .events
                .iter()
                .map(|e| SerializableEvent::from(e.as_ref()))
                .collect(),
            window_start_ms: None,
            last_emit_ms: None,
            partitions: std::collections::HashMap::new(),
        }
    }

    /// Restore window state from a checkpoint.
    pub fn restore(&mut self, cp: &WindowCheckpoint) {
        self.events = cp
            .events
            .iter()
            .map(|se| Arc::new(Event::from(se.clone())))
            .collect();
    }
}

/// A sliding count window that maintains overlapping windows
pub struct SlidingCountWindow {
    window_size: usize,
    slide_size: usize,
    events: VecDeque<SharedEvent>,
    events_since_emit: usize,
}

impl SlidingCountWindow {
    pub fn new(window_size: usize, slide_size: usize) -> Self {
        Self {
            window_size,
            slide_size,
            events: VecDeque::with_capacity(window_size),
            events_since_emit: 0,
        }
    }

    /// Add a shared event, returning window contents if slide interval reached.
    pub fn add_shared(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        self.events.push_back(event);
        self.events_since_emit += 1;

        // Remove old events if window is overfull - drain excess in one operation
        let overflow = self.events.len().saturating_sub(self.window_size);
        if overflow > 0 {
            self.events.drain(0..overflow);
        }

        // Emit if we have enough events and slide interval reached
        (self.events.len() >= self.window_size && self.events_since_emit >= self.slide_size).then(
            || {
                self.events_since_emit = 0;
                self.events.iter().map(Arc::clone).collect()
            },
        )
    }

    /// Add an event (wraps in Arc).
    #[allow(dead_code)]
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.add_shared(Arc::new(event))
            .map(|events| events.into_iter().map(|e| (*e).clone()).collect())
    }

    /// Get current count of events in buffer (for debugging)
    pub fn current_count(&self) -> usize {
        self.events.len()
    }

    /// Create a checkpoint of the current window state.
    pub fn checkpoint(&self) -> WindowCheckpoint {
        WindowCheckpoint {
            events: self
                .events
                .iter()
                .map(|e| SerializableEvent::from(e.as_ref()))
                .collect(),
            window_start_ms: None,
            last_emit_ms: None,
            partitions: std::collections::HashMap::new(),
        }
    }

    /// Restore window state from a checkpoint.
    pub fn restore(&mut self, cp: &WindowCheckpoint) {
        self.events = cp
            .events
            .iter()
            .map(|se| Arc::new(Event::from(se.clone())))
            .collect::<Vec<_>>()
            .into();
        self.events_since_emit = 0;
    }
}

/// A session window that groups events separated by gaps of inactivity.
///
/// Events are accumulated into a session. When a new event arrives after a gap
/// exceeding the configured duration, the current session is closed (emitted)
/// and a new session begins with the incoming event.
pub struct SessionWindow {
    gap: Duration,
    events: Vec<SharedEvent>,
    last_event_time: Option<DateTime<Utc>>,
}

impl SessionWindow {
    pub fn new(gap: Duration) -> Self {
        Self {
            gap,
            events: Vec::new(),
            last_event_time: None,
        }
    }

    /// Add a shared event to the session window.
    /// Returns the completed session if the gap was exceeded.
    pub fn add_shared(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let event_time = event.timestamp;
        if let Some(last_time) = self.last_event_time {
            if event_time - last_time > self.gap {
                // Gap exceeded — close current session, start new one
                let completed = std::mem::take(&mut self.events);
                self.events.push(event);
                self.last_event_time = Some(event_time);
                return Some(completed);
            }
        }
        // Within session gap (or first event)
        self.events.push(event);
        self.last_event_time = Some(event_time);
        None
    }

    /// Add an event (wraps in Arc).
    #[allow(dead_code)]
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.add_shared(Arc::new(event))
            .map(|events| events.into_iter().map(|e| (*e).clone()).collect())
    }

    /// Check if this session has expired (last event time + gap < now).
    /// Returns accumulated events if expired, None otherwise.
    pub fn check_expired(&mut self, now: DateTime<Utc>) -> Option<Vec<SharedEvent>> {
        if let Some(last) = self.last_event_time {
            if now - last > self.gap {
                return Some(self.flush_shared());
            }
        }
        None
    }

    /// Get the configured gap duration.
    pub fn gap(&self) -> Duration {
        self.gap
    }

    /// Flush all events as SharedEvent references.
    pub fn flush_shared(&mut self) -> Vec<SharedEvent> {
        self.last_event_time = None;
        std::mem::take(&mut self.events)
    }

    /// Flush all events (clones for backward compatibility).
    #[allow(dead_code)]
    pub fn flush(&mut self) -> Vec<Event> {
        self.flush_shared()
            .into_iter()
            .map(|e| (*e).clone())
            .collect()
    }

    /// Create a checkpoint of the current session state.
    pub fn checkpoint(&self) -> WindowCheckpoint {
        WindowCheckpoint {
            events: self
                .events
                .iter()
                .map(|e| SerializableEvent::from(e.as_ref()))
                .collect(),
            window_start_ms: self.last_event_time.map(|t| t.timestamp_millis()),
            last_emit_ms: None,
            partitions: std::collections::HashMap::new(),
        }
    }

    /// Restore session state from a checkpoint.
    pub fn restore(&mut self, cp: &WindowCheckpoint) {
        self.events = cp
            .events
            .iter()
            .map(|se| Arc::new(Event::from(se.clone())))
            .collect();
        self.last_event_time = cp.window_start_ms.and_then(DateTime::from_timestamp_millis);
    }

    /// Advance watermark — close session if watermark exceeds last_event_time + gap.
    pub fn advance_watermark(&mut self, wm: DateTime<Utc>) -> Option<Vec<SharedEvent>> {
        if let Some(last) = self.last_event_time {
            if wm >= last + self.gap && !self.events.is_empty() {
                return Some(self.flush_shared());
            }
        }
        None
    }
}

/// A partitioned session window that maintains separate sessions per partition key
pub struct PartitionedSessionWindow {
    partition_key: String,
    gap: Duration,
    windows: std::collections::HashMap<String, SessionWindow>,
}

impl PartitionedSessionWindow {
    pub fn new(partition_key: String, gap: Duration) -> Self {
        Self {
            partition_key,
            gap,
            windows: std::collections::HashMap::new(),
        }
    }

    /// Add a shared event to the appropriate partition session.
    pub fn add_shared(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let key = event
            .get(&self.partition_key)
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "default".to_string());

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| SessionWindow::new(self.gap));

        window.add_shared(event)
    }

    /// Add an event (wraps in Arc).
    #[allow(dead_code)]
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.add_shared(Arc::new(event))
            .map(|events| events.into_iter().map(|e| (*e).clone()).collect())
    }

    /// Check all partitions for expired sessions.
    /// Returns a list of (partition_key, events) for each expired session,
    /// and removes those partitions from the map.
    pub fn check_expired(&mut self, now: DateTime<Utc>) -> Vec<(String, Vec<SharedEvent>)> {
        let mut expired = Vec::new();
        let mut to_remove = Vec::new();
        for (key, window) in &mut self.windows {
            if let Some(events) = window.check_expired(now) {
                if !events.is_empty() {
                    expired.push((key.clone(), events));
                }
                to_remove.push(key.clone());
            }
        }
        for key in to_remove {
            self.windows.remove(&key);
        }
        expired
    }

    /// Get the configured gap duration.
    pub fn gap(&self) -> Duration {
        self.gap
    }

    /// Flush all partition sessions as SharedEvent references.
    pub fn flush_shared(&mut self) -> Vec<SharedEvent> {
        let mut all_events = Vec::new();
        for window in self.windows.values_mut() {
            all_events.extend(window.flush_shared());
        }
        all_events
    }

    /// Flush all partition sessions (clones for backward compatibility).
    #[allow(dead_code)]
    pub fn flush(&mut self) -> Vec<Event> {
        self.flush_shared()
            .into_iter()
            .map(|e| (*e).clone())
            .collect()
    }

    /// Create a checkpoint of all partition sessions.
    pub fn checkpoint(&self) -> WindowCheckpoint {
        let partitions = self
            .windows
            .iter()
            .map(|(key, window)| {
                (
                    key.clone(),
                    PartitionedWindowCheckpoint {
                        events: window
                            .events
                            .iter()
                            .map(|e| SerializableEvent::from(e.as_ref()))
                            .collect(),
                        window_start_ms: window.last_event_time.map(|t| t.timestamp_millis()),
                    },
                )
            })
            .collect();

        WindowCheckpoint {
            events: Vec::new(),
            window_start_ms: None,
            last_emit_ms: None,
            partitions,
        }
    }

    /// Restore partition sessions from a checkpoint.
    pub fn restore(&mut self, cp: &WindowCheckpoint) {
        self.windows.clear();
        for (key, pcp) in &cp.partitions {
            let mut window = SessionWindow::new(self.gap);
            window.events = pcp
                .events
                .iter()
                .map(|se| Arc::new(Event::from(se.clone())))
                .collect();
            window.last_event_time = pcp
                .window_start_ms
                .and_then(DateTime::from_timestamp_millis);
            self.windows.insert(key.clone(), window);
        }
    }

    /// Advance watermark — close expired sessions across all partitions.
    pub fn advance_watermark(&mut self, wm: DateTime<Utc>) -> Vec<(String, Vec<SharedEvent>)> {
        let mut expired = Vec::new();
        let mut to_remove = Vec::new();
        for (key, window) in &mut self.windows {
            if let Some(events) = window.advance_watermark(wm) {
                if !events.is_empty() {
                    expired.push((key.clone(), events));
                }
                to_remove.push(key.clone());
            }
        }
        for key in to_remove {
            self.windows.remove(&key);
        }
        expired
    }
}

/// A partitioned tumbling window that maintains separate windows per partition key
pub struct PartitionedTumblingWindow {
    partition_key: String,
    duration: Duration,
    windows: std::collections::HashMap<String, TumblingWindow>,
}

impl PartitionedTumblingWindow {
    pub fn new(partition_key: String, duration: Duration) -> Self {
        Self {
            partition_key,
            duration,
            windows: std::collections::HashMap::new(),
        }
    }

    /// Add a shared event to the appropriate partition window.
    pub fn add_shared(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let key = event
            .get(&self.partition_key)
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "default".to_string());

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| TumblingWindow::new(self.duration));

        window.add_shared(event)
    }

    /// Add an event (wraps in Arc).
    #[allow(dead_code)]
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.add_shared(Arc::new(event))
            .map(|events| events.into_iter().map(|e| (*e).clone()).collect())
    }

    /// Flush all partition windows as SharedEvent references.
    pub fn flush_shared(&mut self) -> Vec<SharedEvent> {
        let mut all_events = Vec::new();
        for window in self.windows.values_mut() {
            all_events.extend(window.flush_shared());
        }
        all_events
    }

    /// Flush all partition windows (clones for backward compatibility).
    #[allow(dead_code)]
    pub fn flush(&mut self) -> Vec<Event> {
        self.flush_shared()
            .into_iter()
            .map(|e| (*e).clone())
            .collect()
    }

    /// Create a checkpoint of all partition windows.
    pub fn checkpoint(&self) -> WindowCheckpoint {
        let partitions = self
            .windows
            .iter()
            .map(|(key, window)| {
                (
                    key.clone(),
                    PartitionedWindowCheckpoint {
                        events: window
                            .events
                            .iter()
                            .map(|e| SerializableEvent::from(e.as_ref()))
                            .collect(),
                        window_start_ms: window.window_start.map(|t| t.timestamp_millis()),
                    },
                )
            })
            .collect();

        WindowCheckpoint {
            events: Vec::new(),
            window_start_ms: None,
            last_emit_ms: None,
            partitions,
        }
    }

    /// Restore partition windows from a checkpoint.
    pub fn restore(&mut self, cp: &WindowCheckpoint) {
        self.windows.clear();
        for (key, pcp) in &cp.partitions {
            let mut window = TumblingWindow::new(self.duration);
            window.events = pcp
                .events
                .iter()
                .map(|se| Arc::new(Event::from(se.clone())))
                .collect();
            window.window_start = pcp
                .window_start_ms
                .and_then(DateTime::from_timestamp_millis);
            self.windows.insert(key.clone(), window);
        }
    }

    /// Advance watermark across all partitions.
    pub fn advance_watermark(&mut self, wm: DateTime<Utc>) -> Vec<(String, Vec<SharedEvent>)> {
        let mut results = Vec::new();
        for (key, window) in &mut self.windows {
            if let Some(events) = window.advance_watermark(wm) {
                results.push((key.clone(), events));
            }
        }
        results
    }
}

/// A partitioned sliding window that maintains separate windows per partition key
pub struct PartitionedSlidingWindow {
    partition_key: String,
    window_size: Duration,
    slide_interval: Duration,
    windows: std::collections::HashMap<String, SlidingWindow>,
}

impl PartitionedSlidingWindow {
    pub fn new(partition_key: String, window_size: Duration, slide_interval: Duration) -> Self {
        Self {
            partition_key,
            window_size,
            slide_interval,
            windows: std::collections::HashMap::new(),
        }
    }

    /// Add a shared event to the appropriate partition window.
    pub fn add_shared(&mut self, event: SharedEvent) -> Option<Vec<SharedEvent>> {
        let key = event
            .get(&self.partition_key)
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "default".to_string());

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| SlidingWindow::new(self.window_size, self.slide_interval));

        window.add_shared(event)
    }

    /// Add an event (wraps in Arc).
    #[allow(dead_code)]
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.add_shared(Arc::new(event))
            .map(|events| events.into_iter().map(|e| (*e).clone()).collect())
    }

    /// Get all current events from all partitions as shared references.
    pub fn current_all_shared(&self) -> Vec<SharedEvent> {
        let mut all_events = Vec::new();
        for window in self.windows.values() {
            all_events.extend(window.current_shared());
        }
        all_events
    }

    /// Get all current events from all partitions (clones for backward compatibility).
    #[allow(dead_code)]
    pub fn current_all(&self) -> Vec<Event> {
        self.current_all_shared()
            .into_iter()
            .map(|e| (*e).clone())
            .collect()
    }

    /// Create a checkpoint of all partition windows.
    pub fn checkpoint(&self) -> WindowCheckpoint {
        let partitions = self
            .windows
            .iter()
            .map(|(key, window)| {
                (
                    key.clone(),
                    PartitionedWindowCheckpoint {
                        events: window
                            .events
                            .iter()
                            .map(|e| SerializableEvent::from(e.as_ref()))
                            .collect(),
                        window_start_ms: window.last_emit.map(|t| t.timestamp_millis()),
                    },
                )
            })
            .collect();

        WindowCheckpoint {
            events: Vec::new(),
            window_start_ms: None,
            last_emit_ms: None,
            partitions,
        }
    }

    /// Restore partition windows from a checkpoint.
    pub fn restore(&mut self, cp: &WindowCheckpoint) {
        self.windows.clear();
        for (key, pcp) in &cp.partitions {
            let mut window = SlidingWindow::new(self.window_size, self.slide_interval);
            window.events = pcp
                .events
                .iter()
                .map(|se| Arc::new(Event::from(se.clone())))
                .collect::<Vec<_>>()
                .into();
            window.last_emit = pcp
                .window_start_ms
                .and_then(DateTime::from_timestamp_millis);
            self.windows.insert(key.clone(), window);
        }
    }

    /// Advance watermark across all partitions.
    pub fn advance_watermark(&mut self, wm: DateTime<Utc>) -> Vec<(String, Vec<SharedEvent>)> {
        let mut results = Vec::new();
        for (key, window) in &mut self.windows {
            if let Some(events) = window.advance_watermark(wm) {
                results.push((key.clone(), events));
            }
        }
        results
    }
}

// =============================================================================
// DELAY BUFFER (rstream equivalent)
// =============================================================================

/// A delay buffer that outputs items as they are pushed out by newer items.
///
/// This is equivalent to Apama's `rstream` operator:
/// - `retain 1 select rstream a` delays output by 1 item
/// - When a new item arrives, the oldest item is output
///
/// Use cases:
/// - Compare current value with previous value
/// - Detect changes between consecutive aggregations
/// - Implement "previous" reference in expressions
///
/// # Example
/// ```ignore
/// let mut delay = DelayBuffer::new(1);
///
/// // First item: nothing output (buffer filling)
/// assert_eq!(delay.push(10), None);
///
/// // Second item: first item output
/// assert_eq!(delay.push(20), Some(10));
///
/// // Third item: second item output
/// assert_eq!(delay.push(30), Some(20));
/// ```
#[derive(Debug, Clone)]
pub struct DelayBuffer<T> {
    buffer: VecDeque<T>,
    delay: usize,
}

impl<T: Clone> DelayBuffer<T> {
    /// Create a new delay buffer with specified delay count.
    ///
    /// A delay of 1 means the previous item is output when a new item arrives.
    pub fn new(delay: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(delay + 1),
            delay: delay.max(1), // Minimum delay of 1
        }
    }

    /// Push a new item and potentially output a delayed item.
    ///
    /// Returns `Some(item)` if there's an item to output (buffer was full),
    /// or `None` if the buffer is still filling up.
    pub fn push(&mut self, item: T) -> Option<T> {
        self.buffer.push_back(item);

        if self.buffer.len() > self.delay {
            self.buffer.pop_front()
        } else {
            None
        }
    }

    /// Get the current item (most recent) without removing it.
    pub fn current(&self) -> Option<&T> {
        self.buffer.back()
    }

    /// Get the previous item (one before current) without removing it.
    pub fn previous(&self) -> Option<&T> {
        if self.buffer.len() >= 2 {
            self.buffer.get(self.buffer.len() - 2)
        } else {
            None
        }
    }

    /// Get the oldest item in the buffer (the one that would be output next).
    pub fn oldest(&self) -> Option<&T> {
        self.buffer.front()
    }

    /// Check if the buffer is full (ready to output items).
    pub fn is_ready(&self) -> bool {
        self.buffer.len() >= self.delay
    }

    /// Get the current number of items in the buffer.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Flush all items from the buffer, returning them in order.
    pub fn flush(&mut self) -> Vec<T> {
        self.buffer.drain(..).collect()
    }
}

/// A specialized delay buffer for comparing current vs previous values.
///
/// This is optimized for the common pattern:
/// ```ignore
/// // Apama:
/// // from a in averages from p in (from a in averages retain 1 select rstream a)
/// // where a > p + threshold
///
/// // Varpulis:
/// let mut tracker = PreviousValueTracker::new();
/// tracker.update(current_avg);
/// if let Some(prev) = tracker.previous() {
///     if current > prev + threshold { ... }
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct PreviousValueTracker<T> {
    current: Option<T>,
    previous: Option<T>,
}

impl<T: Clone> PreviousValueTracker<T> {
    /// Create a new tracker.
    pub fn new() -> Self {
        Self {
            current: None,
            previous: None,
        }
    }

    /// Update with a new value, shifting current to previous.
    pub fn update(&mut self, value: T) {
        self.previous = self.current.take();
        self.current = Some(value);
    }

    /// Get the current value.
    pub fn current(&self) -> Option<&T> {
        self.current.as_ref()
    }

    /// Get the previous value.
    pub fn previous(&self) -> Option<&T> {
        self.previous.as_ref()
    }

    /// Check if we have both current and previous values (ready for comparison).
    pub fn has_both(&self) -> bool {
        self.current.is_some() && self.previous.is_some()
    }

    /// Get both values as a tuple if both are available.
    pub fn get_pair(&self) -> Option<(&T, &T)> {
        match (&self.current, &self.previous) {
            (Some(curr), Some(prev)) => Some((curr, prev)),
            _ => None,
        }
    }

    /// Reset the tracker.
    pub fn reset(&mut self) {
        self.current = None;
        self.previous = None;
    }
}

/// A partitioned delay buffer that maintains separate buffers per partition key.
#[derive(Debug)]
pub struct PartitionedDelayBuffer<T> {
    delay: usize,
    buffers: std::collections::HashMap<String, DelayBuffer<T>>,
}

impl<T: Clone> PartitionedDelayBuffer<T> {
    /// Create a new partitioned delay buffer.
    pub fn new(delay: usize) -> Self {
        Self {
            delay,
            buffers: std::collections::HashMap::new(),
        }
    }

    /// Push an item for a specific partition.
    pub fn push(&mut self, key: &str, item: T) -> Option<T> {
        let buffer = self
            .buffers
            .entry(key.to_string())
            .or_insert_with(|| DelayBuffer::new(self.delay));
        buffer.push(item)
    }

    /// Get the current value for a partition.
    pub fn current(&self, key: &str) -> Option<&T> {
        self.buffers.get(key).and_then(|b| b.current())
    }

    /// Get the previous value for a partition.
    pub fn previous(&self, key: &str) -> Option<&T> {
        self.buffers.get(key).and_then(|b| b.previous())
    }

    /// Check if a partition buffer is ready.
    pub fn is_ready(&self, key: &str) -> bool {
        self.buffers.get(key).is_some_and(|b| b.is_ready())
    }
}

/// A partitioned previous value tracker.
#[derive(Debug, Default)]
pub struct PartitionedPreviousValueTracker<T> {
    trackers: std::collections::HashMap<String, PreviousValueTracker<T>>,
}

impl<T: Clone> PartitionedPreviousValueTracker<T> {
    /// Create a new partitioned tracker.
    pub fn new() -> Self {
        Self {
            trackers: std::collections::HashMap::new(),
        }
    }

    /// Update the value for a specific partition.
    pub fn update(&mut self, key: &str, value: T) {
        let tracker = self
            .trackers
            .entry(key.to_string())
            .or_insert_with(PreviousValueTracker::new);
        tracker.update(value);
    }

    /// Get the current value for a partition.
    pub fn current(&self, key: &str) -> Option<&T> {
        self.trackers.get(key).and_then(|t| t.current())
    }

    /// Get the previous value for a partition.
    pub fn previous(&self, key: &str) -> Option<&T> {
        self.trackers.get(key).and_then(|t| t.previous())
    }

    /// Check if a partition has both values.
    pub fn has_both(&self, key: &str) -> bool {
        self.trackers.get(key).is_some_and(|t| t.has_both())
    }

    /// Get both values for a partition.
    pub fn get_pair(&self, key: &str) -> Option<(&T, &T)> {
        self.trackers.get(key).and_then(|t| t.get_pair())
    }
}

// =============================================================================
// Incremental Sliding Window with Pre-computed Aggregates
// =============================================================================

/// A sliding window optimized for aggregations with O(1) updates
///
/// Instead of recomputing aggregations over all events on each emit,
/// this window maintains running totals that are updated incrementally:
/// - Adding an event: O(1) update to aggregates
/// - Removing expired events: O(k) where k is expired count
/// - Getting aggregates: O(1)
///
/// This is ~10-100x faster than recomputing for large windows.
pub struct IncrementalSlidingWindow {
    window_size: Duration,
    slide_interval: Duration,
    /// Events with pre-extracted field value for the tracked field
    events: VecDeque<(SharedEvent, Option<f64>)>,
    last_emit: Option<DateTime<Utc>>,
    /// Field name to track for aggregations
    field: String,
    /// Running aggregates
    sum: f64,
    count: usize,
    /// For min/max, we need to track all values (can't incrementally update)
    /// Using the SIMD incremental tracker
    minmax: crate::simd::IncrementalMinMax,
}

impl IncrementalSlidingWindow {
    /// Create a new incremental sliding window
    pub fn new(window_size: Duration, slide_interval: Duration, field: impl Into<String>) -> Self {
        Self {
            window_size,
            slide_interval,
            events: VecDeque::new(),
            last_emit: None,
            field: field.into(),
            sum: 0.0,
            count: 0,
            minmax: crate::simd::IncrementalMinMax::new(),
        }
    }

    /// Add an event, returning aggregates if slide interval reached
    pub fn add(&mut self, event: SharedEvent) -> Option<IncrementalAggregates> {
        let event_time = event.timestamp;

        // Extract field value once
        let value = event.get_float(&self.field);

        // Update running aggregates
        if let Some(v) = value {
            if !v.is_nan() {
                self.sum += v;
                self.count += 1;
                self.minmax.add(v);
            }
        }

        self.events.push_back((event, value));

        // Remove old events
        let cutoff = event_time - self.window_size;
        while let Some((front_event, front_value)) = self.events.front() {
            if front_event.timestamp >= cutoff {
                break;
            }
            // Update aggregates for removed event
            if let Some(v) = front_value {
                if !v.is_nan() {
                    self.sum -= v;
                    self.count = self.count.saturating_sub(1);
                    self.minmax.remove(*v);
                }
            }
            self.events.pop_front();
        }

        // Check if we should emit
        let should_emit = match self.last_emit {
            None => true,
            Some(last) => event_time >= last + self.slide_interval,
        };

        should_emit.then(|| {
            self.last_emit = Some(event_time);
            IncrementalAggregates {
                sum: self.sum,
                count: self.count,
                avg: if self.count > 0 {
                    Some(self.sum / self.count as f64)
                } else {
                    None
                },
                min: self.minmax.min(),
                max: self.minmax.max(),
                event_count: self.events.len(),
            }
        })
    }

    /// Get current aggregates without emitting
    pub fn current_aggregates(&mut self) -> IncrementalAggregates {
        IncrementalAggregates {
            sum: self.sum,
            count: self.count,
            avg: if self.count > 0 {
                Some(self.sum / self.count as f64)
            } else {
                None
            },
            min: self.minmax.min(),
            max: self.minmax.max(),
            event_count: self.events.len(),
        }
    }

    /// Get events in the current window (for patterns that need event access)
    pub fn events(&self) -> impl Iterator<Item = &SharedEvent> {
        self.events.iter().map(|(e, _)| e)
    }

    /// Reset the window
    pub fn reset(&mut self) {
        self.events.clear();
        self.last_emit = None;
        self.sum = 0.0;
        self.count = 0;
        self.minmax.reset();
    }
}

/// Pre-computed aggregates from an incremental window
#[derive(Debug, Clone)]
pub struct IncrementalAggregates {
    pub sum: f64,
    pub count: usize,
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    /// Number of events in window (including those with null/NaN values)
    pub event_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window() {
        let mut window = TumblingWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        // Add events within first window
        for i in 0..3 {
            let event = Event::new("Test").with_timestamp(base_time + Duration::seconds(i));
            assert!(window.add(event).is_none());
        }

        // Add event that triggers new window
        let event = Event::new("Test").with_timestamp(base_time + Duration::seconds(6));
        let result = window.add(event);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 3);
    }

    #[test]
    fn test_sliding_window() {
        let mut window = SlidingWindow::new(Duration::seconds(10), Duration::seconds(2));
        let base_time = Utc::now();

        // Add first event - should emit
        let event = Event::new("Test").with_timestamp(base_time);
        assert!(window.add(event).is_some());

        // Add event within slide interval - should not emit
        let event = Event::new("Test").with_timestamp(base_time + Duration::seconds(1));
        assert!(window.add(event).is_none());

        // Add event after slide interval - should emit
        let event = Event::new("Test").with_timestamp(base_time + Duration::seconds(3));
        let result = window.add(event);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 3);
    }

    #[test]
    fn test_count_window() {
        let mut window = CountWindow::new(3);

        // Add events
        let event1 = Event::new("Test").with_field("value", 1i64);
        assert!(window.add(event1).is_none());
        assert_eq!(window.current_count(), 1);

        let event2 = Event::new("Test").with_field("value", 2i64);
        assert!(window.add(event2).is_none());
        assert_eq!(window.current_count(), 2);

        // Third event should complete window
        let event3 = Event::new("Test").with_field("value", 3i64);
        let result = window.add(event3);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 3);
        assert_eq!(window.current_count(), 0);
    }

    #[test]
    fn test_count_window_flush() {
        let mut window = CountWindow::new(5);

        // Add fewer events than count
        for i in 0..3 {
            let event = Event::new("Test").with_field("value", i as i64);
            window.add(event);
        }

        // Flush should return partial window
        let flushed = window.flush();
        assert_eq!(flushed.len(), 3);
        assert_eq!(window.current_count(), 0);
    }

    #[test]
    fn test_sliding_count_window() {
        let mut window = SlidingCountWindow::new(5, 2);

        // Fill initial window (need 5 events)
        for i in 0..5 {
            let event = Event::new("Test").with_field("value", i as i64);
            let result = window.add(event);
            if i < 4 {
                assert!(result.is_none(), "Should not emit before window is full");
            } else {
                assert!(result.is_some(), "Should emit when window is full");
                assert_eq!(result.unwrap().len(), 5);
            }
        }

        // Add 2 more events (slide size = 2)
        for i in 5..7 {
            let event = Event::new("Test").with_field("value", i as i64);
            let result = window.add(event);
            if i < 6 {
                assert!(result.is_none());
            } else {
                assert!(result.is_some());
                // Window still has 5 events (oldest were dropped)
                assert_eq!(result.unwrap().len(), 5);
            }
        }
    }

    #[test]
    fn test_tumbling_window_flush() {
        let mut window = TumblingWindow::new(Duration::seconds(10));
        let base_time = Utc::now();

        // Add some events
        for i in 0..3 {
            let event = Event::new("Test").with_timestamp(base_time + Duration::seconds(i));
            window.add(event);
        }

        // Flush should return all events
        let flushed = window.flush();
        assert_eq!(flushed.len(), 3);
    }

    #[test]
    fn test_sliding_window_current() {
        let mut window = SlidingWindow::new(Duration::seconds(10), Duration::seconds(1));
        let base_time = Utc::now();

        // Add events
        window.add(Event::new("Test").with_timestamp(base_time));
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(1)));
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(2)));

        // current() should return all events in window
        let current = window.current();
        assert_eq!(current.len(), 3);
    }

    #[test]
    fn test_sliding_window_expiry() {
        let mut window = SlidingWindow::new(Duration::seconds(5), Duration::seconds(1));
        let base_time = Utc::now();

        // Add early event
        window.add(Event::new("Test").with_timestamp(base_time));

        // Add event much later - should expire the first one
        let later_event = Event::new("Test").with_timestamp(base_time + Duration::seconds(10));
        window.add(later_event);

        // Current should only have the later event
        let current = window.current();
        assert_eq!(current.len(), 1);
    }

    #[test]
    fn test_partitioned_tumbling_window() {
        let mut window = PartitionedTumblingWindow::new("region".to_string(), Duration::seconds(5));
        let base_time = Utc::now();

        // Add events for "east" region
        let event1 = Event::new("Test")
            .with_timestamp(base_time)
            .with_field("region", "east");
        window.add(event1);

        // Add events for "west" region
        let event2 = Event::new("Test")
            .with_timestamp(base_time)
            .with_field("region", "west");
        window.add(event2);

        // Trigger east window completion
        let event3 = Event::new("Test")
            .with_timestamp(base_time + Duration::seconds(6))
            .with_field("region", "east");
        let result = window.add(event3);
        assert!(result.is_some());
        // Only "east" events should be in result
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_partitioned_sliding_window() {
        let mut window = PartitionedSlidingWindow::new(
            "region".to_string(),
            Duration::seconds(10),
            Duration::seconds(2),
        );
        let base_time = Utc::now();

        // Add event for "east"
        let event = Event::new("Test")
            .with_timestamp(base_time)
            .with_field("region", "east");
        let result = window.add(event);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);

        // Add event for "west" within slide interval
        let event = Event::new("Test")
            .with_timestamp(base_time + Duration::seconds(1))
            .with_field("region", "west");
        let result = window.add(event);
        // First event for west partition, should emit
        assert!(result.is_some());
    }

    #[test]
    fn test_count_window_multiple_completions() {
        let mut window = CountWindow::new(2);

        // Complete first window
        window.add(Event::new("Test").with_field("batch", 1i64));
        let result = window.add(Event::new("Test").with_field("batch", 1i64));
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);

        // Complete second window
        window.add(Event::new("Test").with_field("batch", 2i64));
        let result = window.add(Event::new("Test").with_field("batch", 2i64));
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);
    }

    // ==========================================================================
    // DelayBuffer Tests (rstream equivalent)
    // ==========================================================================

    #[test]
    fn test_delay_buffer_basic() {
        let mut delay: DelayBuffer<i32> = DelayBuffer::new(1);

        // First item: buffer filling
        assert_eq!(delay.push(10), None);
        assert!(!delay.is_empty());
        assert_eq!(delay.len(), 1);

        // Second item: first item output
        assert_eq!(delay.push(20), Some(10));
        assert_eq!(delay.len(), 1);

        // Third item: second item output
        assert_eq!(delay.push(30), Some(20));
        assert_eq!(delay.len(), 1);
    }

    #[test]
    fn test_delay_buffer_delay_2() {
        let mut delay: DelayBuffer<i32> = DelayBuffer::new(2);

        // First two items: buffer filling
        assert_eq!(delay.push(10), None);
        assert_eq!(delay.push(20), None);
        assert_eq!(delay.len(), 2);

        // Third item: first item output
        assert_eq!(delay.push(30), Some(10));

        // Fourth item: second item output
        assert_eq!(delay.push(40), Some(20));
    }

    #[test]
    fn test_delay_buffer_current_previous() {
        let mut delay: DelayBuffer<i32> = DelayBuffer::new(2);

        delay.push(10);
        delay.push(20);

        assert_eq!(delay.current(), Some(&20));
        assert_eq!(delay.previous(), Some(&10));
        assert_eq!(delay.oldest(), Some(&10));
    }

    #[test]
    fn test_delay_buffer_is_ready() {
        let mut delay: DelayBuffer<i32> = DelayBuffer::new(3);

        assert!(!delay.is_ready());
        delay.push(1);
        assert!(!delay.is_ready());
        delay.push(2);
        assert!(!delay.is_ready());
        delay.push(3);
        assert!(delay.is_ready());
    }

    #[test]
    fn test_delay_buffer_flush() {
        let mut delay: DelayBuffer<i32> = DelayBuffer::new(2);

        delay.push(10);
        delay.push(20);
        delay.push(30);

        let flushed = delay.flush();
        assert_eq!(flushed, vec![20, 30]);
        assert!(delay.is_empty());
    }

    #[test]
    fn test_delay_buffer_clear() {
        let mut delay: DelayBuffer<i32> = DelayBuffer::new(2);

        delay.push(10);
        delay.push(20);

        delay.clear();
        assert!(delay.is_empty());
        assert_eq!(delay.len(), 0);
    }

    #[test]
    fn test_delay_buffer_with_events() {
        let mut delay: DelayBuffer<Event> = DelayBuffer::new(1);

        let e1 = Event::new("Test").with_field("value", 100i64);
        let e2 = Event::new("Test").with_field("value", 200i64);

        assert!(delay.push(e1.clone()).is_none());
        let output = delay.push(e2);
        assert!(output.is_some());
        assert_eq!(output.unwrap().get_int("value"), Some(100));
    }

    // ==========================================================================
    // PreviousValueTracker Tests
    // ==========================================================================

    #[test]
    fn test_previous_value_tracker_basic() {
        let mut tracker: PreviousValueTracker<f64> = PreviousValueTracker::new();

        // Initially empty
        assert!(tracker.current().is_none());
        assert!(tracker.previous().is_none());
        assert!(!tracker.has_both());

        // First value
        tracker.update(10.0);
        assert_eq!(tracker.current(), Some(&10.0));
        assert!(tracker.previous().is_none());
        assert!(!tracker.has_both());

        // Second value
        tracker.update(20.0);
        assert_eq!(tracker.current(), Some(&20.0));
        assert_eq!(tracker.previous(), Some(&10.0));
        assert!(tracker.has_both());

        // Third value
        tracker.update(30.0);
        assert_eq!(tracker.current(), Some(&30.0));
        assert_eq!(tracker.previous(), Some(&20.0));
    }

    #[test]
    fn test_previous_value_tracker_get_pair() {
        let mut tracker: PreviousValueTracker<i32> = PreviousValueTracker::new();

        // No pair yet
        assert!(tracker.get_pair().is_none());

        tracker.update(1);
        assert!(tracker.get_pair().is_none());

        tracker.update(2);
        let pair = tracker.get_pair();
        assert!(pair.is_some());
        let (curr, prev) = pair.unwrap();
        assert_eq!(*curr, 2);
        assert_eq!(*prev, 1);
    }

    #[test]
    fn test_previous_value_tracker_reset() {
        let mut tracker: PreviousValueTracker<i32> = PreviousValueTracker::new();

        tracker.update(1);
        tracker.update(2);
        assert!(tracker.has_both());

        tracker.reset();
        assert!(!tracker.has_both());
        assert!(tracker.current().is_none());
        assert!(tracker.previous().is_none());
    }

    #[test]
    fn test_previous_value_tracker_threshold_comparison() {
        let mut tracker: PreviousValueTracker<f64> = PreviousValueTracker::new();
        let threshold = 5.0;

        tracker.update(100.0);
        tracker.update(107.0);

        if let Some((curr, prev)) = tracker.get_pair() {
            let diff = curr - prev;
            assert!(diff > threshold, "Should detect change > threshold");
        }
    }

    // ==========================================================================
    // PartitionedDelayBuffer Tests
    // ==========================================================================

    #[test]
    fn test_partitioned_delay_buffer() {
        let mut buffer: PartitionedDelayBuffer<i32> = PartitionedDelayBuffer::new(1);

        // Push to partition "a"
        assert_eq!(buffer.push("a", 10), None);
        assert_eq!(buffer.push("a", 20), Some(10));

        // Push to partition "b" - independent
        assert_eq!(buffer.push("b", 100), None);
        assert_eq!(buffer.push("b", 200), Some(100));

        // Check current values
        assert_eq!(buffer.current("a"), Some(&20));
        assert_eq!(buffer.current("b"), Some(&200));
    }

    #[test]
    fn test_partitioned_delay_buffer_previous() {
        let mut buffer: PartitionedDelayBuffer<i32> = PartitionedDelayBuffer::new(2);

        buffer.push("x", 1);
        buffer.push("x", 2);

        assert_eq!(buffer.current("x"), Some(&2));
        assert_eq!(buffer.previous("x"), Some(&1));
        assert!(buffer.is_ready("x"));
        assert!(!buffer.is_ready("y")); // non-existent partition
    }

    // ==========================================================================
    // PartitionedPreviousValueTracker Tests
    // ==========================================================================

    #[test]
    fn test_partitioned_previous_tracker() {
        let mut tracker: PartitionedPreviousValueTracker<f64> =
            PartitionedPreviousValueTracker::new();

        // Update IBM
        tracker.update("IBM", 100.0);
        tracker.update("IBM", 105.0);

        // Update MSFT
        tracker.update("MSFT", 200.0);
        tracker.update("MSFT", 198.0);

        // Check IBM
        assert!(tracker.has_both("IBM"));
        let (curr, prev) = tracker.get_pair("IBM").unwrap();
        assert_eq!(*curr, 105.0);
        assert_eq!(*prev, 100.0);

        // Check MSFT
        assert!(tracker.has_both("MSFT"));
        let (curr, prev) = tracker.get_pair("MSFT").unwrap();
        assert_eq!(*curr, 198.0);
        assert_eq!(*prev, 200.0);

        // Non-existent partition
        assert!(!tracker.has_both("AAPL"));
    }

    #[test]
    fn test_partitioned_previous_tracker_avg_change_detection() {
        // Simulate Apama streams example:
        // Alert when average price changes by more than THRESHOLD
        let mut tracker: PartitionedPreviousValueTracker<f64> =
            PartitionedPreviousValueTracker::new();
        let threshold = 1.0;

        // Simulate averages arriving over time
        let averages = vec![
            ("ibm", 100.0),
            ("msft", 50.0),
            ("ibm", 100.5), // small change
            ("msft", 52.5), // big change > threshold
            ("ibm", 102.5), // big change > threshold
        ];

        let mut alerts = Vec::new();

        for (symbol, avg) in averages {
            tracker.update(symbol, avg);

            if let Some((curr, prev)) = tracker.get_pair(symbol) {
                let diff = (curr - prev).abs();
                if diff > threshold {
                    alerts.push((symbol.to_string(), diff));
                }
            }
        }

        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[0].0, "msft");
        assert!((alerts[0].1 - 2.5).abs() < 0.001);
        assert_eq!(alerts[1].0, "ibm");
        assert!((alerts[1].1 - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_incremental_sliding_window_basic() {
        let mut window =
            IncrementalSlidingWindow::new(Duration::seconds(5), Duration::seconds(1), "price");
        let base_time = Utc::now();

        // Add events with prices
        for i in 0..5 {
            let event = Event::new("Trade")
                .with_timestamp(base_time + Duration::milliseconds(i * 500))
                .with_field("price", varpulis_core::Value::Float(100.0 + i as f64));
            let shared = Arc::new(event);
            let _ = window.add(shared);
        }

        // Get aggregates
        let agg = window.current_aggregates();
        assert_eq!(agg.count, 5);
        assert_eq!(agg.sum, 510.0); // 100 + 101 + 102 + 103 + 104
        assert!((agg.avg.unwrap() - 102.0).abs() < 0.001);
        assert_eq!(agg.min, Some(100.0));
        assert_eq!(agg.max, Some(104.0));
    }

    #[test]
    fn test_incremental_sliding_window_expiry() {
        let mut window =
            IncrementalSlidingWindow::new(Duration::seconds(2), Duration::seconds(1), "value");
        let base_time = Utc::now();

        // Add 3 events at t=0, t=1, t=2
        for i in 0..3 {
            let event = Event::new("Metric")
                .with_timestamp(base_time + Duration::seconds(i))
                .with_field("value", varpulis_core::Value::Float(10.0 * (i + 1) as f64));
            window.add(Arc::new(event));
        }

        // At t=2, window contains t=0,1,2 (all within 2s)
        let agg = window.current_aggregates();
        assert_eq!(agg.count, 3);
        assert_eq!(agg.sum, 60.0); // 10 + 20 + 30

        // Add event at t=3 - should expire t=0
        let event = Event::new("Metric")
            .with_timestamp(base_time + Duration::seconds(3))
            .with_field("value", varpulis_core::Value::Float(40.0));
        window.add(Arc::new(event));

        let agg = window.current_aggregates();
        assert_eq!(agg.count, 3); // t=1,2,3
        assert_eq!(agg.sum, 90.0); // 20 + 30 + 40 (10 expired)
        assert_eq!(agg.min, Some(20.0)); // 10 was removed
    }

    #[test]
    fn test_incremental_sliding_window_emit() {
        let mut window =
            IncrementalSlidingWindow::new(Duration::seconds(5), Duration::seconds(2), "value");
        let base_time = Utc::now();

        // First event should emit
        let event = Event::new("Test")
            .with_timestamp(base_time)
            .with_field("value", varpulis_core::Value::Float(100.0));
        let result = window.add(Arc::new(event));
        assert!(result.is_some());

        // Event at t=1 should not emit (slide interval is 2s)
        let event = Event::new("Test")
            .with_timestamp(base_time + Duration::seconds(1))
            .with_field("value", varpulis_core::Value::Float(200.0));
        let result = window.add(Arc::new(event));
        assert!(result.is_none());

        // Event at t=2 should emit
        let event = Event::new("Test")
            .with_timestamp(base_time + Duration::seconds(2))
            .with_field("value", varpulis_core::Value::Float(300.0));
        let result = window.add(Arc::new(event));
        assert!(result.is_some());

        let agg = result.unwrap();
        assert_eq!(agg.count, 3);
        assert_eq!(agg.sum, 600.0);
    }

    // ==========================================================================
    // SessionWindow Tests
    // ==========================================================================

    #[test]
    fn test_session_window_basic() {
        let mut window = SessionWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        // Events within 5s gap — all in same session
        assert!(window
            .add(Event::new("Test").with_timestamp(base_time))
            .is_none());
        assert!(window
            .add(Event::new("Test").with_timestamp(base_time + Duration::seconds(2)))
            .is_none());
        assert!(window
            .add(Event::new("Test").with_timestamp(base_time + Duration::seconds(4)))
            .is_none());

        // Event after 6s gap — closes session
        let result =
            window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(10)));
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().len(),
            3,
            "First session should have 3 events"
        );
    }

    #[test]
    fn test_session_window_no_gap() {
        let mut window = SessionWindow::new(Duration::seconds(10));
        let base_time = Utc::now();

        // All events within gap — no session closes
        for i in 0..5 {
            assert!(window
                .add(Event::new("Test").with_timestamp(base_time + Duration::seconds(i)))
                .is_none());
        }

        // Flush returns all events
        let flushed = window.flush();
        assert_eq!(flushed.len(), 5);
    }

    #[test]
    fn test_session_window_multiple_sessions() {
        let mut window = SessionWindow::new(Duration::seconds(3));
        let base_time = Utc::now();

        // Session 1: t=0, t=1, t=2
        window.add(Event::new("Test").with_timestamp(base_time));
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(1)));
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(2)));

        // Gap of 5s -> session 1 closes
        let result =
            window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(7)));
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 3);

        // Session 2: t=7, t=8
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(8)));

        // Gap of 4s -> session 2 closes
        let result =
            window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(12)));
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_session_window_flush() {
        let mut window = SessionWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        window.add(Event::new("Test").with_timestamp(base_time));
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(1)));

        let flushed = window.flush();
        assert_eq!(flushed.len(), 2);

        // After flush, window is empty
        let flushed_again = window.flush();
        assert_eq!(flushed_again.len(), 0);
    }

    // ==========================================================================
    // PartitionedSessionWindow Tests
    // ==========================================================================

    #[test]
    fn test_session_window_check_expired_not_expired() {
        let mut window = SessionWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        // Add events
        window.add(Event::new("Test").with_timestamp(base_time));
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(2)));

        // Check at a time within the gap — should not expire
        let check_time = base_time + Duration::seconds(4);
        assert!(window.check_expired(check_time).is_none());
    }

    #[test]
    fn test_session_window_check_expired_returns_events() {
        let mut window = SessionWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        // Add events
        window.add(Event::new("Test").with_timestamp(base_time));
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(1)));
        window.add(Event::new("Test").with_timestamp(base_time + Duration::seconds(2)));

        // Check at a time past the gap — should expire
        let check_time = base_time + Duration::seconds(8);
        let result = window.check_expired(check_time);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 3);

        // After expiry, window should be empty
        let flushed = window.flush();
        assert_eq!(flushed.len(), 0);
    }

    #[test]
    fn test_session_window_check_expired_empty() {
        let mut window = SessionWindow::new(Duration::seconds(5));
        // Empty window should return None
        assert!(window.check_expired(Utc::now()).is_none());
    }

    #[test]
    fn test_session_window_gap_getter() {
        let window = SessionWindow::new(Duration::seconds(7));
        assert_eq!(window.gap(), Duration::seconds(7));
    }

    #[test]
    fn test_partitioned_session_window_check_expired() {
        let mut window = PartitionedSessionWindow::new("region".to_string(), Duration::seconds(5));
        let base_time = Utc::now();

        // Add events to two partitions at different times
        window.add(
            Event::new("Test")
                .with_timestamp(base_time)
                .with_field("region", "east"),
        );
        window.add(
            Event::new("Test")
                .with_timestamp(base_time + Duration::seconds(3))
                .with_field("region", "west"),
        );

        // At base_time + 6s: east expired (last=0, gap=5), west still active (last=3)
        // Note: partition keys are formatted via Display, which quotes strings
        let check_time = base_time + Duration::seconds(6);
        let expired = window.check_expired(check_time);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].1.len(), 1);

        // At base_time + 9s: west now expired too
        let check_time2 = base_time + Duration::seconds(9);
        let expired2 = window.check_expired(check_time2);
        assert_eq!(expired2.len(), 1);
    }

    #[test]
    fn test_partitioned_session_window_removes_expired() {
        let mut window = PartitionedSessionWindow::new("region".to_string(), Duration::seconds(3));
        let base_time = Utc::now();

        window.add(
            Event::new("Test")
                .with_timestamp(base_time)
                .with_field("region", "east"),
        );
        window.add(
            Event::new("Test")
                .with_timestamp(base_time)
                .with_field("region", "west"),
        );

        // Expire both partitions
        let check_time = base_time + Duration::seconds(5);
        let expired = window.check_expired(check_time);
        assert_eq!(expired.len(), 2);

        // After expiry, flush should return nothing
        let flushed = window.flush();
        assert_eq!(flushed.len(), 0);
    }

    #[test]
    fn test_partitioned_session_window_gap_getter() {
        let window = PartitionedSessionWindow::new("k".to_string(), Duration::seconds(10));
        assert_eq!(window.gap(), Duration::seconds(10));
    }

    #[test]
    fn test_partitioned_session_window() {
        let mut window = PartitionedSessionWindow::new("region".to_string(), Duration::seconds(5));
        let base_time = Utc::now();

        // East: t=0, t=2
        window.add(
            Event::new("Test")
                .with_timestamp(base_time)
                .with_field("region", "east"),
        );
        window.add(
            Event::new("Test")
                .with_timestamp(base_time + Duration::seconds(2))
                .with_field("region", "east"),
        );

        // West: t=0
        window.add(
            Event::new("Test")
                .with_timestamp(base_time)
                .with_field("region", "west"),
        );

        // East: gap of 6s -> east session closes
        let result = window.add(
            Event::new("Test")
                .with_timestamp(base_time + Duration::seconds(8))
                .with_field("region", "east"),
        );
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().len(),
            2,
            "East session should have 2 events"
        );

        // West still open (no gap exceeded)
        let result = window.add(
            Event::new("Test")
                .with_timestamp(base_time + Duration::seconds(3))
                .with_field("region", "west"),
        );
        assert!(result.is_none(), "West session should still be open");
    }

    #[test]
    fn test_tumbling_window_checkpoint_restore() {
        let mut window = TumblingWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        // Add 2 events within first window
        let e1 = Event::new("Test").with_timestamp(base_time);
        assert!(window.add(e1).is_none());
        let e2 = Event::new("Test").with_timestamp(base_time + Duration::seconds(1));
        assert!(window.add(e2).is_none());

        // Checkpoint
        let cp = window.checkpoint();

        // Restore into a new window
        let mut restored = TumblingWindow::new(Duration::seconds(5));
        restored.restore(&cp);

        // Add event that triggers window close (falls into next window)
        let e3 = Event::new("Test").with_timestamp(base_time + Duration::seconds(6));
        let result = restored.add(e3);
        assert!(
            result.is_some(),
            "Window should close on event past boundary"
        );
        let emitted = result.unwrap();
        assert_eq!(
            emitted.len(),
            2,
            "Restored window should contain the 2 checkpointed events"
        );
    }

    #[test]
    fn test_count_window_checkpoint_restore() {
        let mut window = CountWindow::new(3);

        // Add 2 events
        let e1 = Event::new("Test").with_field("value", 1i64);
        assert!(window.add(e1).is_none());
        let e2 = Event::new("Test").with_field("value", 2i64);
        assert!(window.add(e2).is_none());

        // Checkpoint
        let cp = window.checkpoint();

        // Restore into a new window
        let mut restored = CountWindow::new(3);
        restored.restore(&cp);

        // Add 1 more event to fill window
        let e3 = Event::new("Test").with_field("value", 3i64);
        let result = restored.add(e3);
        assert!(result.is_some(), "Window should emit after 3rd event");
        let emitted = result.unwrap();
        assert_eq!(emitted.len(), 3, "All 3 events should be returned");
    }

    #[test]
    fn test_sliding_count_window_checkpoint_restore() {
        // window_size=4, slide_size=2
        let mut window = SlidingCountWindow::new(4, 2);

        // Add 2 events (not enough to fill window yet)
        let e1 = Event::new("Test").with_field("seq", 1i64);
        assert!(window.add(e1).is_none());
        let e2 = Event::new("Test").with_field("seq", 2i64);
        assert!(window.add(e2).is_none());

        // Checkpoint
        let cp = window.checkpoint();

        // Restore into a new window
        let mut restored = SlidingCountWindow::new(4, 2);
        restored.restore(&cp);

        // Add 2 more events to fill the window (4 total) and reach slide_size
        let e3 = Event::new("Test").with_field("seq", 3i64);
        assert!(restored.add(e3).is_none());
        let e4 = Event::new("Test").with_field("seq", 4i64);
        let result = restored.add(e4);
        assert!(
            result.is_some(),
            "Should emit when window is full and slide interval reached"
        );
        assert_eq!(
            result.unwrap().len(),
            4,
            "Window should contain all 4 events"
        );

        // Continue sliding: add 2 more events
        let e5 = Event::new("Test").with_field("seq", 5i64);
        assert!(restored.add(e5).is_none());
        let e6 = Event::new("Test").with_field("seq", 6i64);
        let result = restored.add(e6);
        assert!(result.is_some(), "Should emit after slide_size more events");
        let emitted = result.unwrap();
        assert_eq!(
            emitted.len(),
            4,
            "Sliding window should still hold 4 events"
        );
    }

    #[test]
    fn test_session_window_checkpoint_restore() {
        let mut window = SessionWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        // Add 2 events 1 second apart (within session gap)
        let e1 = Event::new("Test").with_timestamp(base_time);
        assert!(window.add(e1).is_none());
        let e2 = Event::new("Test").with_timestamp(base_time + Duration::seconds(1));
        assert!(window.add(e2).is_none());

        // Checkpoint
        let cp = window.checkpoint();

        // Restore into a new window
        let mut restored = SessionWindow::new(Duration::seconds(5));
        restored.restore(&cp);

        // Add event with 6s gap from last event to close the session
        let e3 = Event::new("Test").with_timestamp(base_time + Duration::seconds(7));
        let result = restored.add(e3);
        assert!(
            result.is_some(),
            "Session should close due to gap exceeding 5s"
        );
        let emitted = result.unwrap();
        assert_eq!(
            emitted.len(),
            2,
            "Closed session should contain the 2 checkpointed events"
        );
    }

    #[test]
    fn test_tumbling_window_advance_watermark() {
        let mut window = TumblingWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        // Add 3 events within first window
        for i in 0..3 {
            let event = Event::new("Test").with_timestamp(base_time + Duration::seconds(i));
            assert!(window.add(event).is_none());
        }

        // Advance watermark past window boundary (base_time + 5s)
        let result = window.advance_watermark(base_time + Duration::seconds(6));
        assert!(
            result.is_some(),
            "Watermark past window end should emit events"
        );
        let emitted = result.unwrap();
        assert_eq!(
            emitted.len(),
            3,
            "All 3 events in the window should be emitted"
        );
    }

    #[test]
    fn test_session_window_advance_watermark() {
        let mut window = SessionWindow::new(Duration::seconds(5));
        let base_time = Utc::now();

        // Add events within session
        let e1 = Event::new("Test").with_timestamp(base_time);
        assert!(window.add(e1).is_none());
        let e2 = Event::new("Test").with_timestamp(base_time + Duration::seconds(2));
        assert!(window.add(e2).is_none());

        // Advance watermark past last_event_time + gap (base_time + 2s + 5s = base_time + 7s)
        let result = window.advance_watermark(base_time + Duration::seconds(8));
        assert!(
            result.is_some(),
            "Watermark past session gap should close the session"
        );
        let emitted = result.unwrap();
        assert_eq!(
            emitted.len(),
            2,
            "Closed session should contain the 2 events"
        );
    }
}
