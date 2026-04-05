//! Arrow bridge: conversion between Varpulis `Event`s and Arrow `RecordBatch`.
//!
//! This module provides zero-copy-friendly conversion between row-oriented
//! `Vec<SharedEvent>` and columnar `RecordBatch` for SIMD-accelerated
//! aggregation, filtering, and joins.
//!
//! ## Design
//!
//! - Schema is inferred from the first event and cached per event type.
//! - Only primitive types (Bool, Int, Float, Str, Timestamp) are columnarized.
//!   Array and Map values are skipped (not representable in flat Arrow columns).
//! - A batch size threshold (default 32) gates conversion — below it,
//!   row-oriented processing is faster due to conversion overhead.

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rustc_hash::FxHashMap;
use varpulis_core::Value;

use crate::event::{Event, SharedEvent};

/// Minimum batch size to justify Arrow conversion overhead.
pub const ARROW_BATCH_THRESHOLD: usize = 32;

/// Cached schema per event type.
///
/// Thread-local for zero-contention lookups on the hot path.
/// Schema inference runs once per event type, then all subsequent
/// batches reuse the cached schema.
#[derive(Debug, Default)]
pub struct SchemaCache {
    cache: FxHashMap<Arc<str>, Arc<Schema>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or infer a schema for the given events.
    ///
    /// Uses the first event's fields to build the schema, caching by event_type.
    pub fn get_or_infer(&mut self, events: &[SharedEvent]) -> Arc<Schema> {
        let event_type = &events[0].event_type;

        if let Some(schema) = self.cache.get(event_type) {
            return Arc::clone(schema);
        }

        let schema = Arc::new(infer_schema(&events[0]));
        self.cache
            .insert(Arc::clone(event_type), Arc::clone(&schema));
        schema
    }
}

/// Infer an Arrow schema from a single event's fields.
fn infer_schema(event: &Event) -> Schema {
    let mut fields = Vec::with_capacity(event.data.len() + 1);

    // Always include timestamp as the first column
    fields.push(Field::new("__timestamp", DataType::Int64, false));

    for (key, value) in &event.data {
        if let Some(dt) = value_to_data_type(value) {
            fields.push(Field::new(key.as_ref(), dt, true));
        }
    }

    Schema::new(fields)
}

/// Map a Varpulis Value to an Arrow DataType.
/// Returns None for non-columnarizable types (Array, Map, Null).
fn value_to_data_type(value: &Value) -> Option<DataType> {
    match value {
        Value::Bool(_) => Some(DataType::Boolean),
        Value::Int(_) => Some(DataType::Int64),
        Value::Float(_) => Some(DataType::Float64),
        Value::Str(_) => Some(DataType::Utf8),
        Value::Timestamp(_) => Some(DataType::Int64),
        Value::Duration(_) => Some(DataType::Int64),
        Value::Null | Value::Array(_) | Value::Map(_) => None,
    }
}

/// Convert a batch of events to an Arrow RecordBatch.
///
/// The schema must have been inferred via `SchemaCache::get_or_infer`.
/// Missing fields in individual events become null values.
pub fn events_to_record_batch(
    events: &[SharedEvent],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let num_rows = events.len();

    // Build one array per schema field
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let name = field.name().as_str();

        if name == "__timestamp" {
            // Timestamp column
            let mut builder = Int64Builder::with_capacity(num_rows);
            for event in events {
                builder.append_value(event.timestamp.timestamp_millis());
            }
            columns.push(Arc::new(builder.finish()));
            continue;
        }

        match field.data_type() {
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for event in events {
                    match event.get(name) {
                        Some(Value::Float(v)) => builder.append_value(*v),
                        Some(Value::Int(v)) => builder.append_value(*v as f64),
                        _ => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for event in events {
                    match event.get(name) {
                        Some(Value::Int(v)) => builder.append_value(*v),
                        Some(Value::Timestamp(v)) => builder.append_value(*v),
                        Some(Value::Duration(v)) => builder.append_value(*v as i64),
                        _ => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for event in events {
                    match event.get(name) {
                        Some(Value::Bool(v)) => builder.append_value(*v),
                        _ => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for event in events {
                    match event.get(name) {
                        Some(Value::Str(v)) => builder.append_value(v.as_ref()),
                        _ => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Unsupported type — fill with nulls
                let mut builder = Int64Builder::with_capacity(num_rows);
                for _ in events {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    RecordBatch::try_new(Arc::clone(schema), columns)
}

/// Convert an Arrow RecordBatch back to events.
///
/// Used at the boundary between Arrow-processed operators and row-oriented
/// operators (e.g., SASE pattern matching, emit expressions).
pub fn record_batch_to_events(batch: &RecordBatch, event_type: &Arc<str>) -> Vec<SharedEvent> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut events = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let mut event = Event::with_capacity(event_type.clone(), schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let name = field.name().as_str();
            let col = batch.column(col_idx);

            if name == "__timestamp" {
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                    if let Some(ts) = chrono::DateTime::from_timestamp_millis(arr.value(row)) {
                        event.timestamp = ts;
                    }
                }
                continue;
            }

            if col.is_null(row) {
                continue;
            }

            let value = match field.data_type() {
                DataType::Float64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow_array::Float64Array>()
                        .unwrap();
                    Value::Float(arr.value(row))
                }
                DataType::Int64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .unwrap();
                    Value::Int(arr.value(row))
                }
                DataType::Boolean => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow_array::BooleanArray>()
                        .unwrap();
                    Value::Bool(arr.value(row))
                }
                DataType::Utf8 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    Value::Str(arr.value(row).into())
                }
                _ => continue,
            };

            event.data.insert(Arc::from(name), value);
        }

        events.push(Arc::new(event));
    }

    events
}

/// Get a named Float64 column from a RecordBatch.
///
/// Returns None if the column doesn't exist or isn't Float64.
pub fn get_float64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::Float64Array> {
    let idx = batch.schema().index_of(name).ok()?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
}

/// Get a named Int64 column from a RecordBatch.
pub fn get_int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::Int64Array> {
    let idx = batch.schema().index_of(name).ok()?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_events(n: usize) -> Vec<SharedEvent> {
        let base_time = chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        (0..n)
            .map(|i| {
                Arc::new(
                    Event::new("Test")
                        .with_timestamp(base_time + chrono::Duration::seconds(i as i64))
                        .with_field("temperature", Value::Float(20.0 + i as f64))
                        .with_field("sensor_id", Value::Str(format!("S{i}").into()))
                        .with_field("active", Value::Bool(i % 2 == 0)),
                )
            })
            .collect()
    }

    #[test]
    fn test_schema_inference() {
        let events = make_events(5);
        let mut cache = SchemaCache::new();
        let schema = cache.get_or_infer(&events);

        assert_eq!(schema.fields().len(), 4); // __timestamp, temperature, sensor_id, active
        assert_eq!(schema.field(0).name(), "__timestamp");
        assert_eq!(schema.field(1).name(), "temperature");
        assert_eq!(*schema.field(1).data_type(), DataType::Float64);
        assert_eq!(schema.field(2).name(), "sensor_id");
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert_eq!(schema.field(3).name(), "active");
        assert_eq!(*schema.field(3).data_type(), DataType::Boolean);
    }

    #[test]
    fn test_schema_caching() {
        let events = make_events(3);
        let mut cache = SchemaCache::new();
        let s1 = cache.get_or_infer(&events);
        let s2 = cache.get_or_infer(&events);
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn test_events_to_record_batch() {
        let events = make_events(5);
        let mut cache = SchemaCache::new();
        let schema = cache.get_or_infer(&events);
        let batch = events_to_record_batch(&events, &schema).unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 4);

        let temps = get_float64_column(&batch, "temperature").unwrap();
        assert!((temps.value(0) - 20.0).abs() < f64::EPSILON);
        assert!((temps.value(4) - 24.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_round_trip() {
        let events = make_events(10);
        let mut cache = SchemaCache::new();
        let schema = cache.get_or_infer(&events);
        let batch = events_to_record_batch(&events, &schema).unwrap();
        let restored = record_batch_to_events(&batch, &events[0].event_type);

        assert_eq!(restored.len(), 10);
        for (orig, rest) in events.iter().zip(restored.iter()) {
            assert_eq!(orig.event_type, rest.event_type);
            assert_eq!(orig.timestamp, rest.timestamp);
            assert_eq!(orig.get_float("temperature"), rest.get_float("temperature"));
        }
    }

    #[test]
    fn test_missing_fields_become_null() {
        // Create events with inconsistent fields
        let e1 = Arc::new(Event::new("Test").with_field("temperature", Value::Float(25.0)));
        let e2 = Arc::new(Event::new("Test")); // no temperature field

        let events = vec![e1, e2];
        let mut cache = SchemaCache::new();
        let schema = cache.get_or_infer(&events);
        let batch = events_to_record_batch(&events, &schema).unwrap();

        let temps = get_float64_column(&batch, "temperature").unwrap();
        assert!(!temps.is_null(0));
        assert!(temps.is_null(1));
    }
}
