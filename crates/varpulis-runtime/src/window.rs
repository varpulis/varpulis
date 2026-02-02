//! Window implementations for stream processing
//!
//! Includes:
//! - Time-based windows (tumbling, sliding)
//! - Count-based windows
//! - Partitioned windows
//! - Delay buffers (rstream equivalent)

use crate::event::{Event, SharedEvent};
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
}
