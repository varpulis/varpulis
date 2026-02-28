//! Converter trait for standardized serialization/deserialization of events.
//!
//! Converters provide a uniform interface for transforming between raw bytes
//! and [`Event`] instances. This enables connectors to support multiple wire
//! formats (JSON, CSV, etc.) through a single abstraction.

pub mod csv;
pub mod json;

use crate::event::Event;
use thiserror::Error;

/// Errors produced by converters.
#[derive(Debug, Error)]
pub enum ConverterError {
    #[error("deserialization failed: {0}")]
    DeserializeFailed(String),
    #[error("serialization failed: {0}")]
    SerializeFailed(String),
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),
    #[error("initialization failed: {0}")]
    InitFailed(String),
}

/// Trait for converting between raw bytes and Events.
pub trait Converter: Send + Sync {
    /// Human-readable name (e.g. `"json"`, `"csv"`).
    fn name(&self) -> &str;

    /// Deserialize raw bytes into one or more events.
    fn deserialize(&self, event_type: &str, payload: &[u8]) -> Result<Vec<Event>, ConverterError>;

    /// Serialize an event into raw bytes.
    fn serialize(&self, event: &Event) -> Result<Vec<u8>, ConverterError>;

    /// Optional initialization hook.
    fn init(&mut self) -> Result<(), ConverterError> {
        Ok(())
    }

    /// Optional shutdown hook.
    fn shutdown(&mut self) -> Result<(), ConverterError> {
        Ok(())
    }
}

/// Find a converter by name.
///
/// Returns `None` for unknown formats. Currently supports:
/// - `"json"` — JSON converter (always available)
/// - `"csv"` — CSV converter (requires `csv-converter` feature)
pub fn find_converter(name: &str) -> Option<Box<dyn Converter>> {
    match name {
        "json" => Some(Box::new(json::JsonConverter)),
        #[cfg(feature = "csv-converter")]
        "csv" => Some(Box::new(csv::CsvConverter::new())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_json_converter() {
        let converter = find_converter("json").unwrap();
        assert_eq!(converter.name(), "json");
    }

    #[test]
    fn test_find_unknown_returns_none() {
        assert!(find_converter("xml").is_none());
    }
}
