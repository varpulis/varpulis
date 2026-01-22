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

        let window_start = self.window_start.unwrap();
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

        // Remove old events outside window
        let cutoff = event_time - self.window_size;
        while let Some(front) = self.events.front() {
            if front.timestamp < cutoff {
                self.events.pop_front();
            } else {
                break;
            }
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
}
