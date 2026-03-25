//! Converter trait for standardized serialization/deserialization of events.
//!
//! Re-exported from `varpulis-connector-api` with the addition of the CSV converter.

pub mod csv;

// Re-export all types from the API crate
pub use varpulis_connector_api::converter::*;

/// Find a converter by name.
///
/// Returns `None` for unknown formats. Currently supports:
/// - `"json"` -- JSON converter (always available)
/// - `"csv"` -- CSV converter (requires `csv-converter` feature)
pub fn find_converter(name: &str) -> Option<Box<dyn Converter>> {
    match name {
        "json" => Some(Box::new(json::JsonConverter)),
        #[cfg(feature = "csv-converter")]
        "csv" => Some(Box::new(csv::CsvConverter::new())),
        _ => None,
    }
}
