//! Join buffer for correlating events from multiple streams
//!
//! The JoinBuffer maintains events from multiple source streams and correlates
//! them when events with matching join keys arrive within a specified time window.

use crate::event::Event;
use chrono::{DateTime, Duration, Utc};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use tracing::{debug, trace};

/// Type alias for timestamped events stored by key value
type KeyedEventBuffer = HashMap<String, Vec<(DateTime<Utc>, Event)>>;

/// Buffer for join operations - stores events from each source and correlates them
#[derive(Debug)]
pub struct JoinBuffer {
    /// Events by source name, keyed by join key value
    /// Structure: source_name -> (join_key_value -> Vec<(timestamp, event)>)
    buffers: HashMap<String, KeyedEventBuffer>,
    /// Names of the source streams being joined
    sources: Vec<String>,
    /// Join key field name for each source (extracted from .on() clause)
    /// Structure: source_name -> field_name
    join_keys: HashMap<String, String>,
    /// Window duration for correlation
    window_duration: Duration,
    /// Maximum events to keep per source/key (prevents unbounded growth)
    max_events_per_key: usize,
    /// Expiry queue for O(log n) garbage collection
    /// Contains (expiry_time, source, key) tuples ordered by expiry_time
    expiry_queue: BinaryHeap<Reverse<(DateTime<Utc>, String, String)>>,
    /// Last GC time to avoid running on every event
    last_gc: Option<DateTime<Utc>>,
    /// Minimum interval between GC runs (default: 100ms of window duration)
    gc_interval: Duration,
}

impl JoinBuffer {
    /// Create a new JoinBuffer for correlating events from multiple sources
    ///
    /// # Arguments
    /// * `sources` - Names of the source streams to join
    /// * `join_keys` - Map of source name to the field used as join key for that source
    /// * `window_duration` - How long to keep events for potential correlation
    pub fn new(
        sources: Vec<String>,
        join_keys: HashMap<String, String>,
        window_duration: Duration,
    ) -> Self {
        let mut buffers = HashMap::new();
        for source in &sources {
            buffers.insert(source.clone(), HashMap::new());
        }

        // GC interval is 10% of window duration, minimum 10ms, maximum 1 second
        let gc_interval_ms = (window_duration.num_milliseconds() / 10).clamp(10, 1000);
        let gc_interval = Duration::milliseconds(gc_interval_ms);

        Self {
            buffers,
            sources,
            join_keys,
            window_duration,
            max_events_per_key: 1000, // Default limit
            expiry_queue: BinaryHeap::new(),
            last_gc: None,
            gc_interval,
        }
    }

    /// Set the maximum number of events to keep per source/key combination
    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.max_events_per_key = max_events;
        self
    }

    /// Add an event from a source stream and attempt to correlate
    ///
    /// # Arguments
    /// * `source_name` - Which source stream this event came from
    /// * `event` - The event to add
    ///
    /// # Returns
    /// If events from all sources with matching keys exist within the window,
    /// returns a correlated event containing fields from all sources.
    pub fn add_event(&mut self, source_name: &str, event: Event) -> Option<Event> {
        // Get the join key for this source
        let join_key_field = match self.join_keys.get(source_name) {
            Some(field) => field.clone(),
            None => {
                // Try to find a common key field
                if let Some(field) = self.find_common_key_field(&event) {
                    field
                } else {
                    debug!(
                        "No join key field found for source '{}', skipping",
                        source_name
                    );
                    return None;
                }
            }
        };

        // Extract the join key value from the event
        let key_value = match event.get(&join_key_field) {
            Some(v) => format!("{}", v),
            None => {
                debug!(
                    "Event missing join key field '{}', skipping",
                    join_key_field
                );
                return None;
            }
        };

        trace!(
            "JoinBuffer: Adding event from '{}' with key '{}' = '{}'",
            source_name,
            join_key_field,
            key_value
        );

        // Clean up expired events (uses expiry queue for O(log n) instead of O(n))
        self.cleanup_expired(event.timestamp);

        // Add event to the appropriate buffer
        if let Some(source_buffer) = self.buffers.get_mut(source_name) {
            let key_events = source_buffer.entry(key_value.clone()).or_default();

            // Enforce max events limit
            while key_events.len() >= self.max_events_per_key {
                key_events.remove(0);
            }

            key_events.push((event.timestamp, event.clone()));

            // Add to expiry queue for efficient GC
            let expiry_time = event.timestamp + self.window_duration;
            self.expiry_queue.push(Reverse((
                expiry_time,
                source_name.to_string(),
                key_value.clone(),
            )));
        }

        // Try to correlate events
        self.try_correlate(&key_value, event.timestamp)
    }

    /// Try to find a matching event set for the given key
    fn try_correlate(&mut self, key_value: &str, current_time: DateTime<Utc>) -> Option<Event> {
        let cutoff = current_time - self.window_duration;

        // Check if we have at least one valid event from each source
        let mut source_events: Vec<(&str, &Event)> = Vec::new();

        for source in &self.sources {
            if let Some(source_buffer) = self.buffers.get(source) {
                if let Some(key_events) = source_buffer.get(key_value) {
                    // Find the most recent event within the window
                    let valid_event = key_events
                        .iter()
                        .rev()
                        .find(|(ts, _)| *ts >= cutoff)
                        .map(|(_, e)| e);

                    match valid_event {
                        Some(event) => {
                            source_events.push((source.as_str(), event));
                        }
                        None => {
                            // No valid event from this source, cannot correlate
                            trace!(
                                "JoinBuffer: No valid event from '{}' for key '{}'",
                                source,
                                key_value
                            );
                            return None;
                        }
                    }
                } else {
                    // No events at all from this source for this key
                    trace!(
                        "JoinBuffer: No events from '{}' for key '{}'",
                        source,
                        key_value
                    );
                    return None;
                }
            }
        }

        // We have events from all sources! Create a correlated event
        debug!(
            "JoinBuffer: Correlating {} events for key '{}'",
            source_events.len(),
            key_value
        );

        Some(self.create_correlated_event(&source_events))
    }

    /// Create a correlated event from events from all sources
    fn create_correlated_event(&self, source_events: &[(&str, &Event)]) -> Event {
        let mut correlated = Event::new("JoinedEvent");

        // Use the most recent timestamp
        let max_ts = source_events
            .iter()
            .map(|(_, e)| e.timestamp)
            .max()
            .unwrap_or_else(Utc::now);
        correlated.timestamp = max_ts;

        // Merge fields from all events, prefixed by source name
        for (source, event) in source_events {
            // Add prefixed fields (e.g., "EMA12.ema_12")
            for (field, value) in &event.data {
                let prefixed_key = format!("{}.{}", source, field);
                correlated.data.insert(prefixed_key, value.clone());

                // Also add unprefixed for common fields (first source wins for conflicts)
                if !correlated.data.contains_key(field) {
                    correlated.data.insert(field.clone(), value.clone());
                }
            }
        }

        correlated
    }

    /// Remove events that have expired (outside the window)
    ///
    /// Uses an expiry queue for O(log n) cleanup instead of O(n) iteration over all keys.
    /// Only runs periodically based on gc_interval to avoid overhead on every event.
    fn cleanup_expired(&mut self, current_time: DateTime<Utc>) {
        // Check if enough time has passed since last GC
        if let Some(last_gc) = self.last_gc {
            if current_time - last_gc < self.gc_interval {
                return;
            }
        }
        self.last_gc = Some(current_time);

        let cutoff = current_time - self.window_duration;

        // Process only expired entries from the queue - O(k log n) where k is expired entries
        while let Some(Reverse((expiry_time, _, _))) = self.expiry_queue.peek() {
            if *expiry_time > current_time {
                // No more expired entries
                break;
            }

            // Pop the expired entry (safe: we just peeked it above)
            let Some(Reverse((_, source, key))) = self.expiry_queue.pop() else {
                break;
            };

            // Clean up the specific key in the specific source buffer
            if let Some(source_buffer) = self.buffers.get_mut(&source) {
                if let Some(key_events) = source_buffer.get_mut(&key) {
                    // Use binary search to find expired events
                    let cutoff_idx = key_events.partition_point(|(ts, _)| *ts < cutoff);
                    if cutoff_idx > 0 {
                        key_events.drain(..cutoff_idx);
                    }
                    // Remove the key entry if empty
                    if key_events.is_empty() {
                        source_buffer.remove(&key);
                    }
                }
            }
        }
    }

    /// Try to find a common key field from the event's fields
    fn find_common_key_field(&self, event: &Event) -> Option<String> {
        // Common join key field names
        const COMMON_KEYS: &[&str] = &["symbol", "key", "id", "user_id", "order_id"];

        for key in COMMON_KEYS {
            if event.data.contains_key(*key) {
                return Some((*key).to_string());
            }
        }
        None
    }

    /// Get statistics about the buffer state (for debugging)
    pub fn stats(&self) -> JoinBufferStats {
        let mut total_events = 0;
        let mut events_per_source = HashMap::new();

        for (source, buffer) in &self.buffers {
            let source_count: usize = buffer.values().map(|v| v.len()).sum();
            events_per_source.insert(source.clone(), source_count);
            total_events += source_count;
        }

        JoinBufferStats {
            total_events,
            events_per_source,
            sources: self.sources.clone(),
        }
    }
}

/// Statistics about the JoinBuffer state
#[derive(Debug)]
pub struct JoinBufferStats {
    pub total_events: usize,
    pub events_per_source: HashMap<String, usize>,
    pub sources: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use varpulis_core::Value;

    fn create_event(event_type: &str, symbol: &str, value: f64) -> Event {
        Event::new(event_type)
            .with_field("symbol", symbol)
            .with_field("value", value)
    }

    #[test]
    fn test_join_buffer_correlates_matching_events() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // Add event from source A
        let event_a = create_event("A", "BTC", 100.0);
        let result = buffer.add_event("A", event_a);
        assert!(result.is_none(), "Should not correlate with just one event");

        // Add event from source B with same symbol
        let event_b = create_event("B", "BTC", 200.0);
        let result = buffer.add_event("B", event_b);
        assert!(
            result.is_some(),
            "Should correlate when both sources present"
        );

        let correlated = result.unwrap();
        assert_eq!(
            correlated.get("symbol"),
            Some(&Value::Str("BTC".to_string()))
        );
        assert_eq!(correlated.get("A.value"), Some(&Value::Float(100.0)));
        assert_eq!(correlated.get("B.value"), Some(&Value::Float(200.0)));
    }

    #[test]
    fn test_join_buffer_no_correlation_different_keys() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // Add event from source A with symbol "BTC"
        let event_a = create_event("A", "BTC", 100.0);
        buffer.add_event("A", event_a);

        // Add event from source B with different symbol "ETH"
        let event_b = create_event("B", "ETH", 200.0);
        let result = buffer.add_event("B", event_b);
        assert!(result.is_none(), "Should not correlate with different keys");
    }

    #[test]
    fn test_join_buffer_window_expiration() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::seconds(1));

        let base_time = Utc::now();

        // Add event from source A
        let event_a = Event::new("A")
            .with_timestamp(base_time)
            .with_field("symbol", "BTC")
            .with_field("value", 100.0f64);
        buffer.add_event("A", event_a);

        // Add event from source B much later (outside window)
        let event_b = Event::new("B")
            .with_timestamp(base_time + Duration::seconds(5))
            .with_field("symbol", "BTC")
            .with_field("value", 200.0f64);
        let result = buffer.add_event("B", event_b);

        assert!(result.is_none(), "Should not correlate - event A expired");
    }

    #[test]
    fn test_join_buffer_stats() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // Add events
        buffer.add_event("A", create_event("A", "BTC", 100.0));
        buffer.add_event("A", create_event("A", "ETH", 150.0));
        buffer.add_event("B", create_event("B", "BTC", 200.0));

        let stats = buffer.stats();
        assert_eq!(stats.total_events, 3);
        assert_eq!(stats.events_per_source.get("A"), Some(&2));
        assert_eq!(stats.events_per_source.get("B"), Some(&1));
    }

    #[test]
    fn test_join_buffer_multiple_matches() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // Add multiple events from A for BTC
        buffer.add_event("A", create_event("A", "BTC", 100.0));
        buffer.add_event("A", create_event("A", "BTC", 110.0));

        // Add event from B - should correlate with most recent A event
        let event_b = create_event("B", "BTC", 200.0);
        let result = buffer.add_event("B", event_b);

        assert!(result.is_some());
        let correlated = result.unwrap();
        // Should use the most recent event from A (value=110)
        assert_eq!(correlated.get("A.value"), Some(&Value::Float(110.0)));
    }

    #[test]
    fn test_join_buffer_three_way_join() {
        let sources = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());
        join_keys.insert("C".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // Add events from A and B
        buffer.add_event("A", create_event("A", "BTC", 100.0));
        let result = buffer.add_event("B", create_event("B", "BTC", 200.0));
        assert!(
            result.is_none(),
            "Should not correlate with just 2 of 3 sources"
        );

        // Add event from C - should now correlate
        let result = buffer.add_event("C", create_event("C", "BTC", 300.0));
        assert!(result.is_some(), "Should correlate with all 3 sources");

        let correlated = result.unwrap();
        assert_eq!(correlated.get("A.value"), Some(&Value::Float(100.0)));
        assert_eq!(correlated.get("B.value"), Some(&Value::Float(200.0)));
        assert_eq!(correlated.get("C.value"), Some(&Value::Float(300.0)));
    }

    #[test]
    fn test_join_buffer_max_events_limit() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer =
            JoinBuffer::new(sources, join_keys, Duration::minutes(1)).with_max_events(3);

        // Add 5 events from A for same symbol
        for i in 0..5 {
            buffer.add_event("A", create_event("A", "BTC", i as f64));
        }

        // Should only keep 3 events (most recent)
        let stats = buffer.stats();
        assert_eq!(stats.events_per_source.get("A"), Some(&3));
    }

    #[test]
    fn test_join_buffer_missing_key_field() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // Add event from A
        buffer.add_event("A", create_event("A", "BTC", 100.0));

        // Add event from B without symbol field
        let event_b = Event::new("B").with_field("value", 200.0f64);
        let result = buffer.add_event("B", event_b);

        assert!(result.is_none(), "Should not correlate - missing key field");
    }

    #[test]
    fn test_join_buffer_common_key_detection() {
        let sources = vec!["A".to_string(), "B".to_string()];
        // Empty join keys - should detect common "symbol" field
        let join_keys = HashMap::new();

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // Add events with symbol field
        buffer.add_event("A", create_event("A", "BTC", 100.0));
        let result = buffer.add_event("B", create_event("B", "BTC", 200.0));

        // Should auto-detect "symbol" as common key
        assert!(
            result.is_some(),
            "Should correlate using auto-detected symbol key"
        );
    }

    #[test]
    fn test_join_buffer_continuous_correlation() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // First correlation
        buffer.add_event("A", create_event("A", "BTC", 100.0));
        let result1 = buffer.add_event("B", create_event("B", "BTC", 200.0));
        assert!(result1.is_some());

        // New events should also correlate
        let result2 = buffer.add_event("A", create_event("A", "BTC", 150.0));
        assert!(
            result2.is_some(),
            "Should correlate again with existing B event"
        );

        let result3 = buffer.add_event("B", create_event("B", "BTC", 250.0));
        assert!(result3.is_some(), "Should correlate with recent A event");
    }

    #[test]
    fn test_join_buffer_multiple_symbols() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = HashMap::new();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, Duration::minutes(1));

        // Add events for different symbols
        buffer.add_event("A", create_event("A", "BTC", 100.0));
        buffer.add_event("A", create_event("A", "ETH", 50.0));
        buffer.add_event("B", create_event("B", "ETH", 60.0)); // Should correlate with ETH

        let stats = buffer.stats();
        // After correlation, events are still in buffer
        assert!(stats.total_events >= 2);
    }
}
