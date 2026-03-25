//! Converter trait for standardized serialization/deserialization of events.

pub mod json;

use thiserror::Error;
use varpulis_core::Event;

/// Errors produced by converters.
#[derive(Debug, Error)]
pub enum ConverterError {
    /// Failed to deserialize raw bytes into events.
    #[error("deserialization failed: {0}")]
    DeserializeFailed(String),
    /// Failed to serialize an event into raw bytes.
    #[error("serialization failed: {0}")]
    SerializeFailed(String),
    /// The requested format is not supported.
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),
    /// Converter initialization failed.
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
