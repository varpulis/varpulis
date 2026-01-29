//! Window implementations for stream processing

use crate::event::Event;
use chrono::{DateTime, Duration, Utc};
use std::collections::VecDeque;

/// A tumbling window that collects events over a fixed duration
pub struct TumblingWindow {
    duration: Duration,
    events: Vec<Event>,
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

    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
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

    pub fn flush(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }
}

/// A sliding window that maintains overlapping windows
pub struct SlidingWindow {
    window_size: Duration,
    slide_interval: Duration,
    events: VecDeque<Event>,
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

    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
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

        if should_emit {
            self.last_emit = Some(event_time);
            Some(self.events.iter().cloned().collect())
        } else {
            None
        }
    }

    pub fn current(&self) -> Vec<Event> {
        self.events.iter().cloned().collect()
    }
}

/// A count-based window that emits after collecting N events
pub struct CountWindow {
    count: usize,
    events: Vec<Event>,
}

impl CountWindow {
    pub fn new(count: usize) -> Self {
        Self {
            count,
            events: Vec::with_capacity(count),
        }
    }

    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.events.push(event);

        if self.events.len() >= self.count {
            // Window is full, emit all events and reset
            let completed = std::mem::take(&mut self.events);
            Some(completed)
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
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
    events: VecDeque<Event>,
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

    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.events.push_back(event);
        self.events_since_emit += 1;

        // Remove old events if window is overfull - drain excess in one operation
        let overflow = self.events.len().saturating_sub(self.window_size);
        if overflow > 0 {
            self.events.drain(0..overflow);
        }

        // Emit if we have enough events and slide interval reached
        if self.events.len() >= self.window_size && self.events_since_emit >= self.slide_size {
            self.events_since_emit = 0;
            Some(self.events.iter().cloned().collect())
        } else {
            None
        }
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

    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        let key = event
            .get(&self.partition_key)
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "default".to_string());

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| TumblingWindow::new(self.duration));

        window.add(event)
    }

    pub fn flush(&mut self) -> Vec<Event> {
        let mut all_events = Vec::new();
        for window in self.windows.values_mut() {
            all_events.extend(window.flush());
        }
        all_events
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

    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        let key = event
            .get(&self.partition_key)
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "default".to_string());

        let window = self
            .windows
            .entry(key)
            .or_insert_with(|| SlidingWindow::new(self.window_size, self.slide_interval));

        window.add(event)
    }

    pub fn current_all(&self) -> Vec<Event> {
        let mut all_events = Vec::new();
        for window in self.windows.values() {
            all_events.extend(window.current());
        }
        all_events
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
}
