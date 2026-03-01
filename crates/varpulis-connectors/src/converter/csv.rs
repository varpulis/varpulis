//! CSV converter (feature-gated behind `csv-converter`)
//!
//! Parses CSV payloads into events. Each row becomes one event.
//! The first row is expected to contain column headers that map to event field names.

#[cfg(feature = "csv-converter")]
use super::{Converter, ConverterError};
#[cfg(feature = "csv-converter")]
use varpulis_core::Event;

/// CSV converter that parses CSV data into Events.
///
/// Expects the first line to be a header row. Each subsequent line
/// produces one Event with fields named after the headers.
#[cfg(feature = "csv-converter")]
pub struct CsvConverter {
    delimiter: u8,
}

#[cfg(feature = "csv-converter")]
impl CsvConverter {
    pub fn new() -> Self {
        Self { delimiter: b',' }
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }
}

#[cfg(feature = "csv-converter")]
impl Default for CsvConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "csv-converter")]
impl Converter for CsvConverter {
    fn name(&self) -> &str {
        "csv"
    }

    fn deserialize(&self, event_type: &str, payload: &[u8]) -> Result<Vec<Event>, ConverterError> {
        let text = std::str::from_utf8(payload)
            .map_err(|e| ConverterError::DeserializeFailed(e.to_string()))?;

        let mut lines = text.lines();
        let header_line = lines
            .next()
            .ok_or_else(|| ConverterError::DeserializeFailed("empty CSV".into()))?;

        let sep = self.delimiter as char;
        let headers: Vec<&str> = header_line.split(sep).map(|h| h.trim()).collect();

        let mut events = Vec::new();
        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let values: Vec<&str> = line.split(sep).map(|v| v.trim()).collect();
            let mut event = Event::new(event_type);
            for (i, header) in headers.iter().enumerate() {
                if let Some(val) = values.get(i) {
                    let value = parse_csv_value(val);
                    event = event.with_field(*header, value);
                }
            }
            events.push(event);
        }

        Ok(events)
    }

    fn serialize(&self, event: &Event) -> Result<Vec<u8>, ConverterError> {
        let mut output = String::new();
        let sep = self.delimiter as char;

        // Header row
        let keys: Vec<&str> = event.data.keys().map(|k| k.as_ref()).collect();
        output.push_str(&keys.join(&sep.to_string()));
        output.push('\n');

        // Value row
        let values: Vec<String> = event.data.values().map(|v| format!("{}", v)).collect();
        output.push_str(&values.join(&sep.to_string()));
        output.push('\n');

        Ok(output.into_bytes())
    }
}

#[cfg(feature = "csv-converter")]
fn parse_csv_value(s: &str) -> varpulis_core::Value {
    // Try integer
    if let Ok(i) = s.parse::<i64>() {
        return varpulis_core::Value::Int(i);
    }
    // Try float
    if let Ok(f) = s.parse::<f64>() {
        return varpulis_core::Value::Float(f);
    }
    // Try boolean
    match s.to_lowercase().as_str() {
        "true" => return varpulis_core::Value::Bool(true),
        "false" => return varpulis_core::Value::Bool(false),
        "" => return varpulis_core::Value::Null,
        _ => {}
    }
    // Default to string
    varpulis_core::Value::Str(s.into())
}

// Stub when feature is not enabled — allows the module to exist unconditionally

/// CSV converter stub (requires `csv-converter` feature for full functionality).
#[cfg(not(feature = "csv-converter"))]
#[derive(Default)]
pub struct CsvConverter;

#[cfg(not(feature = "csv-converter"))]
impl CsvConverter {
    /// Create a new CSV converter stub.
    pub const fn new() -> Self {
        Self
    }
}
