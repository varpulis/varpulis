//! Resource limits for event parsing to prevent denial-of-service attacks.
//!
//! These constants bound memory allocation during event ingestion.
//! All connectors (Kafka, MQTT, HTTP, file) enforce these limits.

/// Maximum raw payload size from any connector (1 MB).
/// Events larger than this are rejected before JSON parsing.
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 1_048_576;

/// Maximum number of top-level fields per event.
/// Prevents OOM from events with millions of keys.
pub const MAX_FIELDS_PER_EVENT: usize = 1_024;

/// Maximum length of a single string value (256 KB).
/// Prevents OOM from multi-gigabyte string fields.
pub const MAX_STRING_VALUE_BYTES: usize = 262_144;

/// Maximum nesting depth for JSON/Value structures.
/// Prevents stack overflow from deeply nested payloads.
pub const MAX_JSON_DEPTH: usize = 32;

/// Maximum number of elements in an array value.
pub const MAX_ARRAY_ELEMENTS: usize = 10_000;

/// Maximum line length for streaming event file reader (1 MB).
/// Lines longer than this are skipped with a warning.
pub const MAX_LINE_LENGTH: usize = 1_048_576;
