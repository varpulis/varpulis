//! Event file parser for VarpulisQL
//!
//! Inspired by Apama's .evt file format, this module provides:
//! - Event file parsing with timing control
//! - BATCH tags for grouping events with delays
//! - Support for JSON-style event representation
//!
//! # Event File Format
//!
//! ```text
//! # Comment line
//! // Also a comment
//!
//! # Simple event (sent immediately)
//! StockTick { symbol: "AAPL", price: 150.0, volume: 1000 }
//!
//! # Batch with delay (wait 100ms before sending)
//! BATCH 100
//! Order { id: 1, symbol: "AAPL", quantity: 100 }
//! Order { id: 2, symbol: "GOOG", quantity: 50 }
//!
//! # Another batch at 200ms from start
//! BATCH 200
//! Payment { order_id: 1, amount: 15000.0 }
//! ```

use crate::event::Event;
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info};
use varpulis_core::Value;

/// A parsed event with optional timing
#[derive(Debug, Clone)]
pub struct TimedEvent {
    /// The event to send
    pub event: Event,
    /// Time offset from start (in milliseconds)
    pub time_offset_ms: u64,
}

/// Parsed event file
#[derive(Debug, Clone)]
pub struct EventFile {
    /// Name/path of the file
    pub name: String,
    /// Parsed events with timing
    pub events: Vec<TimedEvent>,
}

/// Event file parser
pub struct EventFileParser;

impl EventFileParser {
    /// Parse an event file from a string (supports both .evt and JSONL formats)
    pub fn parse(source: &str) -> Result<Vec<TimedEvent>, String> {
        let mut events = Vec::new();
        let mut current_batch_time: u64 = 0;

        for (line_num, line) in source.lines().enumerate() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
                continue;
            }

            // Check for BATCH directive
            if line.starts_with("BATCH") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    current_batch_time = parts[1]
                        .parse()
                        .map_err(|_| format!("Invalid BATCH time at line {}", line_num + 1))?;
                }
                continue;
            }

            // Check for @Ns timing prefix: @0s EventType { ... }
            let (time_offset, event_line) = if line.starts_with('@') {
                Self::parse_timing_prefix(line)?
            } else {
                (current_batch_time, line)
            };

            // Parse event - try JSONL first, then .evt format
            let event = if event_line.starts_with('{') {
                Self::parse_jsonl_line(event_line)
                    .map_err(|e| format!("Error at line {}: {}", line_num + 1, e))?
            } else {
                Self::parse_event_line(event_line)
                    .map_err(|e| format!("Error at line {}: {}", line_num + 1, e))?
            };

            events.push(TimedEvent {
                event,
                time_offset_ms: time_offset,
            });
        }

        Ok(events)
    }

    /// Parse @Ns timing prefix and return (time_ms, rest_of_line)
    fn parse_timing_prefix(line: &str) -> Result<(u64, &str), String> {
        // Format: @10s EventType { ... } or @100ms EventType { ... }
        let line = line.trim_start_matches('@');

        // Find first space to separate timing from event
        let space_pos = line
            .find(char::is_whitespace)
            .ok_or_else(|| "Invalid timing prefix format".to_string())?;

        let timing_str = &line[..space_pos];
        let rest = line[space_pos..].trim();

        // Parse timing value with unit
        let time_ms = if timing_str.ends_with("ms") {
            timing_str
                .trim_end_matches("ms")
                .parse::<u64>()
                .map_err(|_| format!("Invalid timing value: {}", timing_str))?
        } else if timing_str.ends_with('s') {
            let secs = timing_str
                .trim_end_matches('s')
                .parse::<u64>()
                .map_err(|_| format!("Invalid timing value: {}", timing_str))?;
            secs * 1000
        } else if timing_str.ends_with('m') {
            let mins = timing_str
                .trim_end_matches('m')
                .parse::<u64>()
                .map_err(|_| format!("Invalid timing value: {}", timing_str))?;
            mins * 60 * 1000
        } else {
            // Assume milliseconds if no unit
            timing_str
                .parse::<u64>()
                .map_err(|_| format!("Invalid timing value: {}", timing_str))?
        };

        Ok((time_ms, rest))
    }

    /// Parse a single event line
    fn parse_event_line(line: &str) -> Result<Event, String> {
        // Format: EventType { field: value, field2: value2 }
        // Or: EventType(value1, value2) - positional format

        let line = line.trim().trim_end_matches(';');

        // Find event type name
        let (event_type, rest) = if let Some(brace_pos) = line.find('{') {
            (&line[..brace_pos].trim(), &line[brace_pos..])
        } else if let Some(paren_pos) = line.find('(') {
            (&line[..paren_pos].trim(), &line[paren_pos..])
        } else {
            return Err(format!("Invalid event format: {}", line));
        };

        let mut event = Event::new(*event_type);

        // Parse fields
        if rest.starts_with('{') {
            // JSON-style: { field: value, ... }
            let content = rest.trim_start_matches('{').trim_end_matches('}').trim();

            for field_str in Self::split_fields(content) {
                let field_str = field_str.trim();
                if field_str.is_empty() {
                    continue;
                }

                let parts: Vec<&str> = field_str.splitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(format!("Invalid field format: {}", field_str));
                }

                let field_name = parts[0].trim();
                let field_value = Self::parse_value(parts[1].trim())?;
                event.data.insert(field_name.into(), field_value);
            }
        } else if rest.starts_with('(') {
            // Positional: (value1, value2, ...)
            let content = rest.trim_start_matches('(').trim_end_matches(')').trim();

            for (i, value_str) in Self::split_fields(content).iter().enumerate() {
                let value_str = value_str.trim();
                if value_str.is_empty() {
                    continue;
                }

                let field_value = Self::parse_value(value_str)?;
                event.data.insert(format!("field_{}", i).into(), field_value);
            }
        }

        Ok(event)
    }

    /// Split fields by comma, respecting nested structures
    fn split_fields(content: &str) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current = String::new();
        let mut depth = 0;
        let mut in_string = false;
        let mut escape_next = false;

        for ch in content.chars() {
            if escape_next {
                current.push(ch);
                escape_next = false;
                continue;
            }

            match ch {
                '\\' => {
                    current.push(ch);
                    escape_next = true;
                }
                '"' => {
                    current.push(ch);
                    in_string = !in_string;
                }
                '{' | '[' | '(' if !in_string => {
                    current.push(ch);
                    depth += 1;
                }
                '}' | ']' | ')' if !in_string => {
                    current.push(ch);
                    depth -= 1;
                }
                ',' if !in_string && depth == 0 => {
                    fields.push(current.trim().to_string());
                    current = String::new();
                }
                _ => current.push(ch),
            }
        }

        if !current.trim().is_empty() {
            fields.push(current.trim().to_string());
        }

        fields
    }

    /// Parse a value string into a Value
    fn parse_value(s: &str) -> Result<Value, String> {
        let s = s.trim();

        // Boolean
        if s == "true" {
            return Ok(Value::Bool(true));
        }
        if s == "false" {
            return Ok(Value::Bool(false));
        }

        // Null
        if s == "null" || s == "nil" {
            return Ok(Value::Null);
        }

        // String (quoted)
        if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
            let inner = &s[1..s.len() - 1];
            // Handle escape sequences
            let unescaped = inner
                .replace("\\n", "\n")
                .replace("\\t", "\t")
                .replace("\\\"", "\"")
                .replace("\\'", "'")
                .replace("\\\\", "\\");
            return Ok(Value::Str(unescaped.into()));
        }

        // Integer
        if let Ok(i) = s.parse::<i64>() {
            return Ok(Value::Int(i));
        }

        // Float
        if let Ok(f) = s.parse::<f64>() {
            return Ok(Value::Float(f));
        }

        // Array [v1, v2, ...]
        if s.starts_with('[') && s.ends_with(']') {
            let inner = &s[1..s.len() - 1];
            let items: Result<Vec<Value>, String> = Self::split_fields(inner)
                .iter()
                .filter(|s| !s.is_empty())
                .map(|item| Self::parse_value(item))
                .collect();
            return Ok(Value::array(items?));
        }

        // Unquoted string (identifier-like)
        Ok(Value::Str(s.to_string().into()))
    }

    /// Parse from a file path
    pub fn parse_file<P: AsRef<Path>>(path: P) -> Result<EventFile, String> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read file {:?}: {}", path, e))?;

        let events = Self::parse(&content)?;

        Ok(EventFile {
            name: path.to_string_lossy().to_string(),
            events,
        })
    }

    /// Parse a single line (either .evt format or JSONL)
    pub fn parse_line(line: &str) -> Result<Option<Event>, String> {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
            return Ok(None);
        }

        // Skip BATCH directives (timing not supported in streaming mode)
        if line.starts_with("BATCH") || line.starts_with('@') {
            return Ok(None);
        }

        // Try JSONL format first: {"event_type": "X", "data": {...}}
        if line.starts_with('{') {
            return Self::parse_jsonl_line(line).map(Some);
        }

        // Fall back to .evt format: EventType { field: value, ... }
        Self::parse_event_line(line).map(Some)
    }

    /// Parse a JSONL line
    fn parse_jsonl_line(line: &str) -> Result<Event, String> {
        let json: serde_json::Value =
            serde_json::from_str(line).map_err(|e| format!("Invalid JSON: {}", e))?;

        let event_type = json
            .get("event_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Missing event_type field".to_string())?;

        let mut event = Event::new(event_type);

        if let Some(data) = json.get("data").and_then(|v| v.as_object()) {
            for (key, value) in data {
                event.data.insert(key.as_str().into(), Self::json_to_value(value));
            }
        }

        Ok(event)
    }

    /// Convert serde_json::Value to varpulis Value
    fn json_to_value(v: &serde_json::Value) -> Value {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::Str(s.clone().into()),
            serde_json::Value::Array(arr) => {
                Value::array(arr.iter().map(Self::json_to_value).collect())
            }
            serde_json::Value::Object(obj) => {
                let mut map: IndexMap<std::sync::Arc<str>, Value, FxBuildHasher> = IndexMap::with_hasher(FxBuildHasher);
                for (k, v) in obj {
                    map.insert(k.as_str().into(), Self::json_to_value(v));
                }
                Value::map(map)
            }
        }
    }
}

/// Streaming event file reader - reads events one at a time without loading entire file
pub struct StreamingEventReader<R: std::io::BufRead> {
    reader: R,
    line_buffer: String,
    events_read: usize,
}

impl<R: std::io::BufRead> StreamingEventReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            line_buffer: String::new(),
            events_read: 0,
        }
    }

    /// Get count of events read so far
    pub fn events_read(&self) -> usize {
        self.events_read
    }
}

impl StreamingEventReader<std::io::BufReader<std::fs::File>> {
    /// Create a streaming reader from a file path with large buffer for performance
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let file = std::fs::File::open(path.as_ref())
            .map_err(|e| format!("Failed to open file: {}", e))?;
        // Use 64KB buffer for better I/O performance
        Ok(Self::new(std::io::BufReader::with_capacity(
            64 * 1024,
            file,
        )))
    }
}

impl<R: std::io::BufRead> Iterator for StreamingEventReader<R> {
    type Item = Result<Event, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line_buffer.clear();
            match self.reader.read_line(&mut self.line_buffer) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    match EventFileParser::parse_line(&self.line_buffer) {
                        Ok(Some(event)) => {
                            self.events_read += 1;
                            return Some(Ok(event));
                        }
                        Ok(None) => continue, // Skip empty/comment lines
                        Err(e) => return Some(Err(e)),
                    }
                }
                Err(e) => return Some(Err(format!("Read error: {}", e))),
            }
        }
    }
}

/// Event file player - sends events to engine with timing
pub struct EventFilePlayer {
    events: Vec<TimedEvent>,
    sender: mpsc::Sender<Event>,
}

impl EventFilePlayer {
    pub fn new(events: Vec<TimedEvent>, sender: mpsc::Sender<Event>) -> Self {
        Self { events, sender }
    }

    pub fn from_file<P: AsRef<Path>>(path: P, sender: mpsc::Sender<Event>) -> Result<Self, String> {
        let event_file = EventFileParser::parse_file(path)?;
        Ok(Self::new(event_file.events, sender))
    }

    /// Play events with timing
    pub async fn play(&self) -> Result<usize, String> {
        let start = std::time::Instant::now();
        let mut sent_count = 0;

        // Group events by time offset
        let mut batches: HashMap<u64, Vec<&TimedEvent>> = HashMap::new();
        for event in &self.events {
            batches.entry(event.time_offset_ms).or_default().push(event);
        }

        // Sort batch times
        let mut times: Vec<u64> = batches.keys().copied().collect();
        times.sort();

        for batch_time in times {
            // Wait until batch time
            let elapsed = start.elapsed().as_millis() as u64;
            if batch_time > elapsed {
                time::sleep(Duration::from_millis(batch_time - elapsed)).await;
            }

            // Send all events in this batch
            if let Some(events) = batches.get(&batch_time) {
                for timed_event in events {
                    debug!(
                        "Sending event: {} at {}ms",
                        timed_event.event.event_type, batch_time
                    );
                    self.sender
                        .send(timed_event.event.clone())
                        .await
                        .map_err(|e| format!("Failed to send event: {}", e))?;
                    sent_count += 1;
                }
            }
        }

        info!("Played {} events from file", sent_count);
        Ok(sent_count)
    }

    /// Play events without timing (immediate)
    pub async fn play_immediate(&self) -> Result<usize, String> {
        let mut sent_count = 0;

        for timed_event in &self.events {
            debug!("Sending event: {}", timed_event.event.event_type);
            self.sender
                .send(timed_event.event.clone())
                .await
                .map_err(|e| format!("Failed to send event: {}", e))?;
            sent_count += 1;
        }

        info!("Played {} events (immediate mode)", sent_count);
        Ok(sent_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_event() {
        let source = r#"
            StockTick { symbol: "AAPL", price: 150.5, volume: 1000 }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(&*events[0].event.event_type, "StockTick");
        assert_eq!(
            events[0].event.get("symbol"),
            Some(&Value::Str("AAPL".into()))
        );
        assert_eq!(events[0].event.get("price"), Some(&Value::Float(150.5)));
        assert_eq!(events[0].event.get("volume"), Some(&Value::Int(1000)));
    }

    #[test]
    fn test_parse_batched_events() {
        let source = r#"
            # First batch at 0ms
            Order { id: 1, symbol: "AAPL" }

            # Second batch at 100ms
            BATCH 100
            Payment { order_id: 1 }
            Shipping { order_id: 1 }

            # Third batch at 200ms
            BATCH 200
            Confirmation { order_id: 1 }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 4);

        assert_eq!(events[0].time_offset_ms, 0);
        assert_eq!(&*events[0].event.event_type, "Order");

        assert_eq!(events[1].time_offset_ms, 100);
        assert_eq!(&*events[1].event.event_type, "Payment");

        assert_eq!(events[2].time_offset_ms, 100);
        assert_eq!(&*events[2].event.event_type, "Shipping");

        assert_eq!(events[3].time_offset_ms, 200);
        assert_eq!(&*events[3].event.event_type, "Confirmation");
    }

    #[test]
    fn test_parse_positional_format() {
        let source = r#"
            StockPrice("AAPL", 150.5)
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(&*events[0].event.event_type, "StockPrice");
        assert_eq!(
            events[0].event.get("field_0"),
            Some(&Value::Str("AAPL".into()))
        );
        assert_eq!(events[0].event.get("field_1"), Some(&Value::Float(150.5)));
    }

    #[test]
    fn test_parse_array_values() {
        let source = r#"
            BatchOrder { ids: [1, 2, 3], symbols: ["AAPL", "GOOG"] }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 1);

        let ids = events[0].event.get("ids").unwrap();
        if let Value::Array(arr) = ids {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_parse_comments() {
        let source = r#"
            # This is a comment
            // This is also a comment
            Event1 { x: 1 }
            # Another comment
            Event2 { y: 2 }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_sequence_scenario() {
        let source = r#"
            # Test sequence: Order -> Payment
            
            # Start with an order
            Order { id: 1, symbol: "AAPL", quantity: 100 }
            
            # Payment arrives 50ms later
            BATCH 50
            Payment { order_id: 1, amount: 15000.0 }
            
            # Another order without payment (should timeout)
            BATCH 100
            Order { id: 2, symbol: "GOOG", quantity: 50 }
            
            # Much later, payment for order 2 (may timeout)
            BATCH 5000
            Payment { order_id: 2, amount: 7500.0 }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 4);

        // Verify order sequence
        assert_eq!(&*events[0].event.event_type, "Order");
        assert_eq!(events[0].time_offset_ms, 0);

        assert_eq!(&*events[1].event.event_type, "Payment");
        assert_eq!(events[1].time_offset_ms, 50);

        assert_eq!(&*events[2].event.event_type, "Order");
        assert_eq!(events[2].time_offset_ms, 100);

        assert_eq!(&*events[3].event.event_type, "Payment");
        assert_eq!(events[3].time_offset_ms, 5000);
    }

    // ==========================================================================
    // Value Parsing Tests
    // ==========================================================================

    #[test]
    fn test_parse_boolean_values() {
        let source = r#"
            Flags { active: true, disabled: false }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events[0].event.get("active"), Some(&Value::Bool(true)));
        assert_eq!(events[0].event.get("disabled"), Some(&Value::Bool(false)));
    }

    #[test]
    fn test_parse_null_values() {
        let source = r#"
            Data { value: null, other: nil }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events[0].event.get("value"), Some(&Value::Null));
        assert_eq!(events[0].event.get("other"), Some(&Value::Null));
    }

    #[test]
    fn test_parse_escape_sequences() {
        let source = r#"
            Message { text: "Hello\nWorld", path: "C:\\Users\\test" }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        let text = events[0].event.get("text").unwrap();
        if let Value::Str(s) = text {
            assert!(s.contains('\n'));
        }
    }

    #[test]
    fn test_parse_single_quoted_string() {
        let source = r#"
            Event { name: 'single quoted' }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(
            events[0].event.get("name"),
            Some(&Value::Str("single quoted".into()))
        );
    }

    #[test]
    fn test_parse_unquoted_identifier() {
        let source = r#"
            Event { status: active, mode: processing }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(
            events[0].event.get("status"),
            Some(&Value::Str("active".into()))
        );
        assert_eq!(
            events[0].event.get("mode"),
            Some(&Value::Str("processing".into()))
        );
    }

    #[test]
    fn test_parse_negative_numbers() {
        let source = r#"
            Data { temp: -15, delta: -2.5 }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events[0].event.get("temp"), Some(&Value::Int(-15)));
        assert_eq!(events[0].event.get("delta"), Some(&Value::Float(-2.5)));
    }

    #[test]
    fn test_parse_nested_array() {
        let source = r#"
            Complex { matrix: [[1, 2], [3, 4]] }
        "#;

        let events = EventFileParser::parse(source).unwrap();
        let matrix = events[0].event.get("matrix").unwrap();
        if let Value::Array(arr) = matrix {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    // ==========================================================================
    // Error Handling Tests
    // ==========================================================================

    #[test]
    fn test_parse_invalid_event_format() {
        let source = "InvalidEvent";
        let result = EventFileParser::parse(source);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_field_format() {
        let source = "Event { invalid_no_colon }";
        let result = EventFileParser::parse(source);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_batch_time() {
        let source = r#"
            BATCH not_a_number
            Event { x: 1 }
        "#;
        let result = EventFileParser::parse(source);
        assert!(result.is_err());
    }

    // ==========================================================================
    // Edge Cases
    // ==========================================================================

    #[test]
    fn test_parse_empty_content() {
        let source = "";
        let events = EventFileParser::parse(source).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_parse_only_comments() {
        let source = r#"
            # Comment 1
            // Comment 2
            # Comment 3
        "#;
        let events = EventFileParser::parse(source).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_parse_empty_braces() {
        let source = "EmptyEvent { }";
        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].event.data.is_empty());
    }

    #[test]
    fn test_parse_semicolon_terminated() {
        let source = "Event { x: 1 };";
        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_parse_whitespace_handling() {
        let source = "  Event  {  x  :  1  ,  y  :  2  }  ";
        let events = EventFileParser::parse(source).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.get("x"), Some(&Value::Int(1)));
        assert_eq!(events[0].event.get("y"), Some(&Value::Int(2)));
    }

    // ==========================================================================
    // EventFilePlayer Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_player_immediate() {
        let events = vec![
            TimedEvent {
                event: Event::new("A").with_field("id", 1i64),
                time_offset_ms: 0,
            },
            TimedEvent {
                event: Event::new("B").with_field("id", 2i64),
                time_offset_ms: 100,
            },
        ];

        let (tx, mut rx) = mpsc::channel(10);
        let player = EventFilePlayer::new(events, tx);

        let count = player.play_immediate().await.unwrap();
        assert_eq!(count, 2);

        let e1 = rx.recv().await.unwrap();
        assert_eq!(&*e1.event_type, "A");

        let e2 = rx.recv().await.unwrap();
        assert_eq!(&*e2.event_type, "B");
    }

    #[tokio::test]
    async fn test_player_with_batches() {
        let events = vec![
            TimedEvent {
                event: Event::new("First"),
                time_offset_ms: 0,
            },
            TimedEvent {
                event: Event::new("Second"),
                time_offset_ms: 0,
            },
            TimedEvent {
                event: Event::new("Third"),
                time_offset_ms: 10, // 10ms later
            },
        ];

        let (tx, mut rx) = mpsc::channel(10);
        let player = EventFilePlayer::new(events, tx);

        let count = player.play().await.unwrap();
        assert_eq!(count, 3);

        // All events should have been sent
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_player_empty() {
        let events = vec![];
        let (tx, _rx) = mpsc::channel(10);
        let player = EventFilePlayer::new(events, tx);

        let count = player.play_immediate().await.unwrap();
        assert_eq!(count, 0);
    }
}
