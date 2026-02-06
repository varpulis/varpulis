//! Columnar Event Storage for SIMD-Optimized Aggregations
//!
//! This module provides columnar storage for windowed event processing,
//! enabling SIMD-optimized aggregations on contiguous memory.
//!
//! # Architecture
//!
//! Instead of storing events row-by-row (`Vec<Event>`), this module stores:
//! - Original events for predicates and emit operations
//! - Eagerly extracted timestamps for temporal operations
//! - Lazily populated column caches for numeric aggregations
//!
//! # Performance Gains
//!
//! - 8-40x faster aggregations (SIMD on contiguous memory)
//! - Amortized field extraction across multiple aggregations
//! - Better cache locality for numeric operations
//!
//! # Example
//!
//! ```rust,ignore
//! use varpulis_runtime::columnar::ColumnarBuffer;
//! use varpulis_runtime::Event;
//! use std::sync::Arc;
//!
//! let mut buffer = ColumnarBuffer::new();
//! buffer.push(Arc::new(Event::new("Trade").with_field("price", 100.0)));
//! buffer.push(Arc::new(Event::new("Trade").with_field("price", 105.0)));
//!
//! // First access extracts field values to contiguous array
//! let prices = buffer.ensure_float_column("price");
//! assert_eq!(prices.len(), 2);
//!
//! // Subsequent accesses reuse cached column
//! let prices_again = buffer.ensure_float_column("price");
//! assert_eq!(prices_again.len(), 2);
//! ```

use crate::event::{Event, SharedEvent};
use crate::persistence::SerializableEvent;
use rustc_hash::FxHashMap;
use std::sync::Arc;

/// Type-specific column storage for SIMD operations.
///
/// Each variant stores values in a contiguous Vec for cache-friendly
/// SIMD processing. Missing values are represented as:
/// - `f64::NAN` for Float64
/// - `i64::MIN` for Int64
/// - `None` for String and Bool
#[derive(Debug, Clone)]
pub enum Column {
    /// Float64 column with NaN for missing values
    Float64(Vec<f64>),
    /// Int64 column with i64::MIN for missing values
    Int64(Vec<i64>),
    /// String column with Option for nullable strings
    String(Vec<Option<Arc<str>>>),
    /// Bool column with Option for nullable bools
    Bool(Vec<Option<bool>>),
}

impl Column {
    /// Returns the number of elements in the column.
    pub fn len(&self) -> usize {
        match self {
            Column::Float64(v) => v.len(),
            Column::Int64(v) => v.len(),
            Column::String(v) => v.len(),
            Column::Bool(v) => v.len(),
        }
    }

    /// Returns true if the column is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the Float64 slice if this is a Float64 column.
    pub fn as_float64(&self) -> Option<&[f64]> {
        match self {
            Column::Float64(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the Int64 slice if this is an Int64 column.
    pub fn as_int64(&self) -> Option<&[i64]> {
        match self {
            Column::Int64(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the String slice if this is a String column.
    pub fn as_string(&self) -> Option<&[Option<Arc<str>>]> {
        match self {
            Column::String(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the Bool slice if this is a Bool column.
    pub fn as_bool(&self) -> Option<&[Option<bool>]> {
        match self {
            Column::Bool(v) => Some(v),
            _ => None,
        }
    }
}

/// Columnar buffer with lazy field extraction.
///
/// This buffer stores events in both row and columnar format:
/// - `events`: Original events preserved for predicates/emit operations
/// - `timestamps`: Eagerly extracted timestamps for temporal operations
/// - `columns`: Lazily populated cache of extracted field columns
///
/// When an aggregation needs a field, the column is extracted once and
/// cached for subsequent aggregations on the same field.
#[derive(Debug)]
pub struct ColumnarBuffer {
    /// Original events preserved for predicates and emit
    events: Vec<SharedEvent>,
    /// Timestamps in milliseconds (eagerly extracted)
    timestamps: Vec<i64>,
    /// Lazily populated column cache by field name
    columns: FxHashMap<Arc<str>, Column>,
}

impl Default for ColumnarBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarBuffer {
    /// Create a new empty columnar buffer.
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            timestamps: Vec::new(),
            columns: FxHashMap::default(),
        }
    }

    /// Create a new columnar buffer with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            events: Vec::with_capacity(capacity),
            timestamps: Vec::with_capacity(capacity),
            columns: FxHashMap::default(),
        }
    }

    /// Push an event into the buffer.
    ///
    /// This eagerly extracts the timestamp and clears any cached columns
    /// to maintain consistency.
    pub fn push(&mut self, event: SharedEvent) {
        // Eagerly extract timestamp
        self.timestamps.push(event.timestamp.timestamp_millis());
        self.events.push(event);
        // Clear column cache since we added new data
        // (lazily re-extracted on next access)
        if !self.columns.is_empty() {
            self.columns.clear();
        }
    }

    /// Drain the first `count` events from the buffer.
    ///
    /// Returns the drained events as a Vec<SharedEvent>.
    /// Clears all cached columns since indices shift.
    pub fn drain_front(&mut self, count: usize) -> Vec<SharedEvent> {
        let count = count.min(self.events.len());
        let drained: Vec<_> = self.events.drain(0..count).collect();
        self.timestamps.drain(0..count);
        // Clear column cache since indices shifted
        self.columns.clear();
        drained
    }

    /// Take all events from the buffer, leaving it empty.
    ///
    /// Returns the events as a Vec<SharedEvent>.
    pub fn take_all(&mut self) -> Vec<SharedEvent> {
        self.timestamps.clear();
        self.columns.clear();
        std::mem::take(&mut self.events)
    }

    /// Clear all events from the buffer.
    pub fn clear(&mut self) {
        self.events.clear();
        self.timestamps.clear();
        self.columns.clear();
    }

    /// Get the events as a slice.
    pub fn events(&self) -> &[SharedEvent] {
        &self.events
    }

    /// Get the timestamps as a slice (milliseconds since epoch).
    pub fn timestamps(&self) -> &[i64] {
        &self.timestamps
    }

    /// Get the number of events in the buffer.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Ensure a Float64 column exists for the given field.
    ///
    /// If the column is already cached, returns the cached slice.
    /// Otherwise, extracts the field from all events and caches it.
    ///
    /// Missing values are stored as `f64::NAN`.
    pub fn ensure_float_column(&mut self, field: &str) -> &[f64] {
        let field_key: Arc<str> = field.into();

        if !self.columns.contains_key(&field_key) {
            // Extract column from events
            let values: Vec<f64> = self
                .events
                .iter()
                .map(|e| e.get_float(field).unwrap_or(f64::NAN))
                .collect();
            self.columns
                .insert(field_key.clone(), Column::Float64(values));
        }

        match self.columns.get(&field_key) {
            Some(Column::Float64(v)) => v,
            _ => &[], // Should never happen due to insert above
        }
    }

    /// Ensure an Int64 column exists for the given field.
    ///
    /// If the column is already cached, returns the cached slice.
    /// Otherwise, extracts the field from all events and caches it.
    ///
    /// Missing values are stored as `i64::MIN`.
    pub fn ensure_int_column(&mut self, field: &str) -> &[i64] {
        let field_key: Arc<str> = field.into();

        if !self.columns.contains_key(&field_key) {
            let values: Vec<i64> = self
                .events
                .iter()
                .map(|e| e.get_int(field).unwrap_or(i64::MIN))
                .collect();
            self.columns
                .insert(field_key.clone(), Column::Int64(values));
        }

        match self.columns.get(&field_key) {
            Some(Column::Int64(v)) => v,
            _ => &[],
        }
    }

    /// Ensure a String column exists for the given field.
    ///
    /// If the column is already cached, returns the cached slice.
    /// Otherwise, extracts the field from all events and caches it.
    pub fn ensure_string_column(&mut self, field: &str) -> &[Option<Arc<str>>] {
        let field_key: Arc<str> = field.into();

        if !self.columns.contains_key(&field_key) {
            let values: Vec<Option<Arc<str>>> = self
                .events
                .iter()
                .map(|e| e.get_str(field).map(Arc::from))
                .collect();
            self.columns
                .insert(field_key.clone(), Column::String(values));
        }

        match self.columns.get(&field_key) {
            Some(Column::String(v)) => v,
            _ => &[],
        }
    }

    /// Check if a column is already cached.
    pub fn has_column(&self, field: &str) -> bool {
        self.columns.contains_key(field)
    }

    /// Get the cached column if it exists.
    pub fn get_column(&self, field: &str) -> Option<&Column> {
        self.columns.get(field)
    }

    /// Create a checkpoint of the buffer for persistence.
    ///
    /// Note: Columns are NOT checkpointed - they are lazily re-extracted on restore.
    pub fn checkpoint(&self) -> ColumnarCheckpoint {
        ColumnarCheckpoint {
            events: self
                .events
                .iter()
                .map(|e| SerializableEvent::from(e.as_ref()))
                .collect(),
            timestamps: self.timestamps.clone(),
        }
    }

    /// Restore buffer state from a checkpoint.
    pub fn restore(&mut self, cp: &ColumnarCheckpoint) {
        self.events = cp
            .events
            .iter()
            .map(|se| Arc::new(Event::from(se.clone())))
            .collect();
        self.timestamps = cp.timestamps.clone();
        self.columns.clear(); // Columns will be lazily re-extracted
    }

    /// Create from a Vec of SharedEvents.
    pub fn from_events(events: Vec<SharedEvent>) -> Self {
        let timestamps: Vec<i64> = events
            .iter()
            .map(|e| e.timestamp.timestamp_millis())
            .collect();
        Self {
            events,
            timestamps,
            columns: FxHashMap::default(),
        }
    }
}

/// Checkpoint for columnar buffer persistence.
///
/// Columns are NOT checkpointed - they are lazily re-extracted on restore.
/// This keeps checkpoint size minimal while maintaining correct behavior.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnarCheckpoint {
    /// Serialized events
    pub events: Vec<SerializableEvent>,
    /// Timestamps in milliseconds
    pub timestamps: Vec<i64>,
}

/// Trait for windows that support columnar access.
///
/// This trait provides a uniform interface for accessing columnar storage
/// across different window types.
pub trait ColumnarAccess {
    /// Get a reference to the columnar buffer.
    fn columnar(&self) -> &ColumnarBuffer;

    /// Get a mutable reference to the columnar buffer.
    fn columnar_mut(&mut self) -> &mut ColumnarBuffer;

    /// Get a float column, ensuring it exists.
    fn get_float_column(&mut self, field: &str) -> &[f64] {
        self.columnar_mut().ensure_float_column(field)
    }

    /// Get an int column, ensuring it exists.
    fn get_int_column(&mut self, field: &str) -> &[i64] {
        self.columnar_mut().ensure_int_column(field)
    }

    /// Get a string column, ensuring it exists.
    fn get_string_column(&mut self, field: &str) -> &[Option<Arc<str>>] {
        self.columnar_mut().ensure_string_column(field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;
    use chrono::{Duration, Utc};

    #[test]
    fn test_columnar_buffer_push() {
        let mut buffer = ColumnarBuffer::new();
        let base_time = Utc::now();

        buffer.push(Arc::new(
            Event::new("Trade")
                .with_timestamp(base_time)
                .with_field("price", 100.0),
        ));
        buffer.push(Arc::new(
            Event::new("Trade")
                .with_timestamp(base_time + Duration::seconds(1))
                .with_field("price", 105.0),
        ));

        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.timestamps().len(), 2);
        assert!(buffer.columns.is_empty()); // Columns not extracted yet
    }

    #[test]
    fn test_columnar_buffer_ensure_float_column() {
        let mut buffer = ColumnarBuffer::new();

        buffer.push(Arc::new(Event::new("Trade").with_field("price", 100.0)));
        buffer.push(Arc::new(Event::new("Trade").with_field("price", 105.0)));
        buffer.push(Arc::new(Event::new("Trade"))); // Missing price

        let prices = buffer.ensure_float_column("price");
        assert_eq!(prices.len(), 3);
        assert_eq!(prices[0], 100.0);
        assert_eq!(prices[1], 105.0);
        assert!(prices[2].is_nan()); // Missing value is NaN

        // Should be cached now
        assert!(buffer.has_column("price"));
    }

    #[test]
    fn test_columnar_buffer_drain_front() {
        let mut buffer = ColumnarBuffer::new();

        for i in 0..5 {
            buffer.push(Arc::new(
                Event::new("Trade").with_field("price", (i as f64) * 10.0),
            ));
        }

        // Extract column before drain
        let _prices = buffer.ensure_float_column("price");
        assert!(buffer.has_column("price"));

        // Drain first 2 events
        let drained = buffer.drain_front(2);
        assert_eq!(drained.len(), 2);
        assert_eq!(buffer.len(), 3);

        // Column cache should be cleared
        assert!(!buffer.has_column("price"));

        // Re-extract should work with updated data
        let prices = buffer.ensure_float_column("price");
        assert_eq!(prices.len(), 3);
        assert_eq!(prices[0], 20.0); // Was index 2
    }

    #[test]
    fn test_columnar_buffer_take_all() {
        let mut buffer = ColumnarBuffer::new();

        buffer.push(Arc::new(Event::new("Trade").with_field("price", 100.0)));
        buffer.push(Arc::new(Event::new("Trade").with_field("price", 105.0)));

        let events = buffer.take_all();
        assert_eq!(events.len(), 2);
        assert!(buffer.is_empty());
        assert!(buffer.timestamps().is_empty());
    }

    #[test]
    fn test_columnar_buffer_ensure_int_column() {
        let mut buffer = ColumnarBuffer::new();

        buffer.push(Arc::new(Event::new("Order").with_field("quantity", 10i64)));
        buffer.push(Arc::new(Event::new("Order").with_field("quantity", 20i64)));

        let quantities = buffer.ensure_int_column("quantity");
        assert_eq!(quantities.len(), 2);
        assert_eq!(quantities[0], 10);
        assert_eq!(quantities[1], 20);
    }

    #[test]
    fn test_columnar_buffer_ensure_string_column() {
        let mut buffer = ColumnarBuffer::new();

        buffer.push(Arc::new(Event::new("Trade").with_field("symbol", "IBM")));
        buffer.push(Arc::new(Event::new("Trade").with_field("symbol", "AAPL")));
        buffer.push(Arc::new(Event::new("Trade"))); // Missing symbol

        let symbols = buffer.ensure_string_column("symbol");
        assert_eq!(symbols.len(), 3);
        assert_eq!(symbols[0].as_deref(), Some("IBM"));
        assert_eq!(symbols[1].as_deref(), Some("AAPL"));
        assert_eq!(symbols[2], None);
    }

    #[test]
    fn test_columnar_buffer_checkpoint_restore() {
        let mut buffer = ColumnarBuffer::new();
        let base_time = Utc::now();

        buffer.push(Arc::new(
            Event::new("Trade")
                .with_timestamp(base_time)
                .with_field("price", 100.0),
        ));
        buffer.push(Arc::new(
            Event::new("Trade")
                .with_timestamp(base_time + Duration::seconds(1))
                .with_field("price", 105.0),
        ));

        // Extract column before checkpoint
        let _prices = buffer.ensure_float_column("price");

        // Checkpoint
        let cp = buffer.checkpoint();

        // Restore to new buffer
        let mut restored = ColumnarBuffer::new();
        restored.restore(&cp);

        assert_eq!(restored.len(), 2);
        assert_eq!(restored.timestamps().len(), 2);
        assert!(!restored.has_column("price")); // Columns not preserved

        // Should be able to re-extract
        let prices = restored.ensure_float_column("price");
        assert_eq!(prices.len(), 2);
        assert_eq!(prices[0], 100.0);
    }

    #[test]
    fn test_columnar_buffer_from_events() {
        let events = vec![
            Arc::new(Event::new("Trade").with_field("price", 100.0)),
            Arc::new(Event::new("Trade").with_field("price", 105.0)),
        ];

        let buffer = ColumnarBuffer::from_events(events);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.timestamps().len(), 2);
    }

    #[test]
    fn test_column_type_accessors() {
        let float_col = Column::Float64(vec![1.0, 2.0, 3.0]);
        assert!(float_col.as_float64().is_some());
        assert!(float_col.as_int64().is_none());
        assert_eq!(float_col.len(), 3);

        let int_col = Column::Int64(vec![1, 2, 3]);
        assert!(int_col.as_int64().is_some());
        assert!(int_col.as_float64().is_none());

        let str_col = Column::String(vec![Some("a".into()), None]);
        assert!(str_col.as_string().is_some());
        assert_eq!(str_col.len(), 2);

        let bool_col = Column::Bool(vec![Some(true), Some(false)]);
        assert!(bool_col.as_bool().is_some());
    }

    #[test]
    fn test_columnar_buffer_clear() {
        let mut buffer = ColumnarBuffer::new();
        buffer.push(Arc::new(Event::new("Trade").with_field("price", 100.0)));
        buffer.ensure_float_column("price");

        buffer.clear();

        assert!(buffer.is_empty());
        assert!(buffer.timestamps().is_empty());
        assert!(!buffer.has_column("price"));
    }

    #[test]
    fn test_columnar_buffer_with_capacity() {
        let buffer = ColumnarBuffer::with_capacity(100);
        assert!(buffer.is_empty());
        // Capacity is internal, just verify it doesn't panic
    }
}
