//! Aggregation functions for stream processing.
//!
//! This module provides aggregation functions used in windowed stream processing.
//! Aggregations compute summary statistics over collections of events.
//!
//! # Available Aggregations
//!
//! | Function | Description | SIMD Optimized |
//! |----------|-------------|----------------|
//! | [`Count`] | Count events | No |
//! | [`Sum`] | Sum of field values | Yes (AVX2) |
//! | [`Avg`] | Average of field values | Yes (AVX2) |
//! | [`Min`] | Minimum value | Yes (AVX2) |
//! | [`Max`] | Maximum value | Yes (AVX2) |
//! | [`StdDev`] | Standard deviation (Welford's) | No |
//! | [`First`] | First value in window | No |
//! | [`Last`] | Last value in window | No |
//! | [`CountDistinct`] | Count unique values | No |
//! | [`Ema`] | Exponential moving average | No |
//!
//! # Performance
//!
//! Aggregations use SIMD (AVX2) vectorization when available on x86_64:
//! - `Sum`, `Avg`, `Min`, `Max` achieve ~4x speedup with AVX2
//! - Fallback to 4-way loop unrolling on other architectures
//!
//! # Example
//!
//! ```rust,ignore
//! use varpulis_runtime::aggregation::{AggregateFunc, Sum, Avg, Count};
//! use varpulis_runtime::Event;
//!
//! let events = vec![
//!     Event::new("Reading").with_field("value", 10.0),
//!     Event::new("Reading").with_field("value", 20.0),
//!     Event::new("Reading").with_field("value", 30.0),
//! ];
//!
//! let sum = Sum.apply(&events, Some("value"));  // 60.0
//! let avg = Avg.apply(&events, Some("value"));  // 20.0
//! let count = Count.apply(&events, None);       // 3
//! ```
//!
//! # Custom Aggregations
//!
//! Implement the [`AggregateFunc`] trait for custom aggregations:
//!
//! ```rust,ignore
//! use varpulis_runtime::aggregation::AggregateFunc;
//! use varpulis_runtime::Event;
//! use varpulis_core::Value;
//!
//! struct Median;
//!
//! impl AggregateFunc for Median {
//!     fn name(&self) -> &str { "median" }
//!
//!     fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
//!         let field = field.unwrap_or("value");
//!         let mut values: Vec<f64> = events
//!             .iter()
//!             .filter_map(|e| e.get_float(field))
//!             .collect();
//!         values.sort_by(|a, b| a.partial_cmp(b).unwrap());
//!
//!         if values.is_empty() {
//!             Value::Null
//!         } else {
//!             Value::Float(values[values.len() / 2])
//!         }
//!     }
//! }
//! ```

use crate::columnar::ColumnarBuffer;
use crate::event::{Event, SharedEvent};
use indexmap::IndexMap;
use std::hash::{Hash, Hasher};
use varpulis_core::Value;

/// Result type for aggregation outputs.
///
/// Maps aggregation names to their computed values.
pub type AggResult = IndexMap<String, Value>;

/// Trait for implementing aggregation functions.
///
/// Aggregation functions compute summary values over collections of events.
/// They are used in windowed stream processing to produce aggregate results.
///
/// # Required Methods
///
/// - [`name`](Self::name): Returns the aggregation function name
/// - [`apply`](Self::apply): Computes the aggregation over owned events
///
/// # Optional Methods
///
/// - [`apply_shared`](Self::apply_shared): Optimized for `Arc<Event>` (avoids cloning)
/// - [`apply_refs`](Self::apply_refs): For reference slices (internal use)
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to work with parallel processing.
pub trait AggregateFunc: Send + Sync {
    /// Returns the name of this aggregation function (e.g., "sum", "avg").
    fn name(&self) -> &str;

    /// Apply the aggregation to a slice of events.
    ///
    /// # Arguments
    ///
    /// - `events`: Slice of events to aggregate
    /// - `field`: Optional field name to aggregate (defaults to "value")
    ///
    /// # Returns
    ///
    /// The aggregated value, or `Value::Null` if no valid values found.
    fn apply(&self, events: &[Event], field: Option<&str>) -> Value;

    /// Apply aggregation to shared events (avoids cloning in hot paths).
    ///
    /// Override this for better performance when events are wrapped in `Arc`.
    fn apply_shared(&self, events: &[SharedEvent], field: Option<&str>) -> Value {
        // Default implementation: create temporary references
        // Aggregations only read, so we can iterate and dereference
        // Individual implementations can override for better performance
        let refs: Vec<&Event> = events.iter().map(|e| e.as_ref()).collect();
        self.apply_refs(&refs, field)
    }

    /// Apply aggregation to event references (internal helper)
    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        // Default: clone to Vec<Event> - suboptimal but correct
        // Most aggregations just iterate and read fields
        let owned: Vec<Event> = events.iter().map(|e| (*e).clone()).collect();
        self.apply(&owned, field)
    }

    /// Apply aggregation to columnar buffer (SIMD-optimized for numeric aggregations).
    ///
    /// Default implementation falls back to shared events. Override this for
    /// aggregations that can benefit from columnar data layout (sum, avg, min, max).
    fn apply_columnar(&self, buffer: &mut ColumnarBuffer, field: Option<&str>) -> Value {
        // Default: fall back to shared event access
        self.apply_shared(buffer.events(), field)
    }
}

/// Count aggregation
pub struct Count;

impl AggregateFunc for Count {
    fn name(&self) -> &str {
        "count"
    }

    fn apply(&self, events: &[Event], _field: Option<&str>) -> Value {
        Value::Int(events.len() as i64)
    }

    fn apply_refs(&self, events: &[&Event], _field: Option<&str>) -> Value {
        Value::Int(events.len() as i64)
    }

    fn apply_columnar(&self, buffer: &mut ColumnarBuffer, _field: Option<&str>) -> Value {
        Value::Int(buffer.len() as i64)
    }
}

/// Sum aggregation (SIMD-optimized)
pub struct Sum;

impl AggregateFunc for Sum {
    fn name(&self) -> &str {
        "sum"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        Value::Float(crate::simd::simd_sum(events, field))
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        // Extract values to contiguous array for SIMD
        let values: Vec<f64> = events
            .iter()
            .filter_map(|e| e.get_float(field))
            .filter(|v| !v.is_nan())
            .collect();
        Value::Float(crate::simd::sum_f64(&values))
    }

    fn apply_columnar(&self, buffer: &mut ColumnarBuffer, field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let col = buffer.ensure_float_column(field);
        // Filter NaN values for sum
        let valid: Vec<f64> = col.iter().copied().filter(|v| !v.is_nan()).collect();
        Value::Float(crate::simd::sum_f64(&valid))
    }
}

/// Average aggregation (SIMD-optimized)
pub struct Avg;

impl AggregateFunc for Avg {
    fn name(&self) -> &str {
        "avg"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        match crate::simd::simd_avg(events, field) {
            Some(avg) => Value::Float(avg),
            None => Value::Null,
        }
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        // Extract values to contiguous array for SIMD
        let values: Vec<f64> = events
            .iter()
            .filter_map(|e| e.get_float(field))
            .filter(|v| !v.is_nan())
            .collect();

        if values.is_empty() {
            Value::Null
        } else {
            Value::Float(crate::simd::sum_f64(&values) / values.len() as f64)
        }
    }

    fn apply_columnar(&self, buffer: &mut ColumnarBuffer, field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let col = buffer.ensure_float_column(field);
        let valid: Vec<f64> = col.iter().copied().filter(|v| !v.is_nan()).collect();
        if valid.is_empty() {
            Value::Null
        } else {
            Value::Float(crate::simd::sum_f64(&valid) / valid.len() as f64)
        }
    }
}

/// Min aggregation (SIMD-optimized)
pub struct Min;

impl AggregateFunc for Min {
    fn name(&self) -> &str {
        "min"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        match crate::simd::simd_min(events, field) {
            Some(min) => Value::Float(min),
            None => Value::Null,
        }
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let values: Vec<f64> = events
            .iter()
            .filter_map(|e| e.get_float(field))
            .filter(|v| !v.is_nan())
            .collect();
        match crate::simd::min_f64(&values) {
            Some(min) => Value::Float(min),
            None => Value::Null,
        }
    }

    fn apply_columnar(&self, buffer: &mut ColumnarBuffer, field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let col = buffer.ensure_float_column(field);
        let valid: Vec<f64> = col.iter().copied().filter(|v| !v.is_nan()).collect();
        match crate::simd::min_f64(&valid) {
            Some(min) => Value::Float(min),
            None => Value::Null,
        }
    }
}

/// Max aggregation (SIMD-optimized)
pub struct Max;

impl AggregateFunc for Max {
    fn name(&self) -> &str {
        "max"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        match crate::simd::simd_max(events, field) {
            Some(max) => Value::Float(max),
            None => Value::Null,
        }
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let values: Vec<f64> = events
            .iter()
            .filter_map(|e| e.get_float(field))
            .filter(|v| !v.is_nan())
            .collect();
        match crate::simd::max_f64(&values) {
            Some(max) => Value::Float(max),
            None => Value::Null,
        }
    }

    fn apply_columnar(&self, buffer: &mut ColumnarBuffer, field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let col = buffer.ensure_float_column(field);
        let valid: Vec<f64> = col.iter().copied().filter(|v| !v.is_nan()).collect();
        match crate::simd::max_f64(&valid) {
            Some(max) => Value::Float(max),
            None => Value::Null,
        }
    }
}

/// Standard deviation aggregation (Welford's online algorithm for single-pass)
pub struct StdDev;

impl AggregateFunc for StdDev {
    fn name(&self) -> &str {
        "stddev"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");

        // Welford's online algorithm for numerically stable variance
        // Single pass: no intermediate Vec allocation
        let mut count = 0usize;
        let mut mean = 0.0;
        let mut m2 = 0.0; // Sum of squared differences from mean

        for event in events {
            if let Some(x) = event.get_float(field) {
                count += 1;
                let delta = x - mean;
                mean += delta / count as f64;
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
        }

        if count < 2 {
            return Value::Null;
        }

        // Sample standard deviation (n-1 denominator)
        let variance = m2 / (count - 1) as f64;
        Value::Float(variance.sqrt())
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");

        let mut count = 0usize;
        let mut mean = 0.0;
        let mut m2 = 0.0;

        for event in events {
            if let Some(x) = event.get_float(field) {
                count += 1;
                let delta = x - mean;
                mean += delta / count as f64;
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
        }

        if count < 2 {
            return Value::Null;
        }

        let variance = m2 / (count - 1) as f64;
        Value::Float(variance.sqrt())
    }
}

/// First value aggregation
pub struct First;

impl AggregateFunc for First {
    fn name(&self) -> &str {
        "first"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        events
            .first()
            .and_then(|e| e.get(field))
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        events
            .first()
            .and_then(|e| e.get(field))
            .cloned()
            .unwrap_or(Value::Null)
    }
}

/// Last value aggregation
pub struct Last;

impl AggregateFunc for Last {
    fn name(&self) -> &str {
        "last"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        events
            .last()
            .and_then(|e| e.get(field))
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        events
            .last()
            .and_then(|e| e.get(field))
            .cloned()
            .unwrap_or(Value::Null)
    }
}

/// Count distinct values aggregation
/// Optimized to store only hashes instead of cloning full Values
pub struct CountDistinct;

impl AggregateFunc for CountDistinct {
    fn name(&self) -> &str {
        "count_distinct"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let mut seen_hashes = std::collections::HashSet::new();

        for event in events {
            if let Some(value) = event.get(field) {
                // Compute hash directly without cloning the value
                // This avoids expensive deep clones for Array/Map values
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                value.hash(&mut hasher);
                seen_hashes.insert(hasher.finish());
            }
        }

        Value::Int(seen_hashes.len() as i64)
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let mut seen_hashes = std::collections::HashSet::new();

        for event in events {
            if let Some(value) = event.get(field) {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                value.hash(&mut hasher);
                seen_hashes.insert(hasher.finish());
            }
        }

        Value::Int(seen_hashes.len() as i64)
    }
}

/// Exponential Moving Average aggregation
/// EMA = price * k + EMA(previous) * (1 - k)
/// where k = 2 / (n + 1)
pub struct Ema {
    pub period: usize,
}

/// Binary operation for expression aggregates
#[derive(Debug, Clone, Copy)]
pub enum AggBinOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Expression-based aggregate that combines two aggregates with an operator
pub struct ExprAggregate {
    pub left: Box<dyn AggregateFunc>,
    pub left_field: Option<String>,
    pub op: AggBinOp,
    pub right: Box<dyn AggregateFunc>,
    pub right_field: Option<String>,
}

impl ExprAggregate {
    pub fn new(
        left: Box<dyn AggregateFunc>,
        left_field: Option<String>,
        op: AggBinOp,
        right: Box<dyn AggregateFunc>,
        right_field: Option<String>,
    ) -> Self {
        Self {
            left,
            left_field,
            op,
            right,
            right_field,
        }
    }
}

impl AggregateFunc for ExprAggregate {
    fn name(&self) -> &str {
        "expr"
    }

    fn apply(&self, events: &[Event], _field: Option<&str>) -> Value {
        let left_val = self.left.apply(events, self.left_field.as_deref());
        let right_val = self.right.apply(events, self.right_field.as_deref());

        match (left_val, right_val) {
            (Value::Float(l), Value::Float(r)) => {
                let result = match self.op {
                    AggBinOp::Add => l + r,
                    AggBinOp::Sub => l - r,
                    AggBinOp::Mul => l * r,
                    AggBinOp::Div => {
                        if r != 0.0 {
                            l / r
                        } else {
                            f64::NAN
                        }
                    }
                };
                Value::Float(result)
            }
            (Value::Int(l), Value::Int(r)) => {
                let result = match self.op {
                    AggBinOp::Add => l + r,
                    AggBinOp::Sub => l - r,
                    AggBinOp::Mul => l * r,
                    AggBinOp::Div => {
                        if r != 0 {
                            l / r
                        } else {
                            0
                        }
                    }
                };
                Value::Int(result)
            }
            (Value::Int(l), Value::Float(r)) => {
                let l = l as f64;
                let result = match self.op {
                    AggBinOp::Add => l + r,
                    AggBinOp::Sub => l - r,
                    AggBinOp::Mul => l * r,
                    AggBinOp::Div => {
                        if r != 0.0 {
                            l / r
                        } else {
                            f64::NAN
                        }
                    }
                };
                Value::Float(result)
            }
            (Value::Float(l), Value::Int(r)) => {
                let r = r as f64;
                let result = match self.op {
                    AggBinOp::Add => l + r,
                    AggBinOp::Sub => l - r,
                    AggBinOp::Mul => l * r,
                    AggBinOp::Div => {
                        if r != 0.0 {
                            l / r
                        } else {
                            f64::NAN
                        }
                    }
                };
                Value::Float(result)
            }
            _ => Value::Null,
        }
    }

    fn apply_refs(&self, events: &[&Event], _field: Option<&str>) -> Value {
        let left_val = self.left.apply_refs(events, self.left_field.as_deref());
        let right_val = self.right.apply_refs(events, self.right_field.as_deref());

        match (left_val, right_val) {
            (Value::Float(l), Value::Float(r)) => {
                let result = match self.op {
                    AggBinOp::Add => l + r,
                    AggBinOp::Sub => l - r,
                    AggBinOp::Mul => l * r,
                    AggBinOp::Div => {
                        if r != 0.0 {
                            l / r
                        } else {
                            f64::NAN
                        }
                    }
                };
                Value::Float(result)
            }
            (Value::Int(l), Value::Int(r)) => {
                let result = match self.op {
                    AggBinOp::Add => l + r,
                    AggBinOp::Sub => l - r,
                    AggBinOp::Mul => l * r,
                    AggBinOp::Div => {
                        if r != 0 {
                            l / r
                        } else {
                            0
                        }
                    }
                };
                Value::Int(result)
            }
            (Value::Int(l), Value::Float(r)) => {
                let l = l as f64;
                let result = match self.op {
                    AggBinOp::Add => l + r,
                    AggBinOp::Sub => l - r,
                    AggBinOp::Mul => l * r,
                    AggBinOp::Div => {
                        if r != 0.0 {
                            l / r
                        } else {
                            f64::NAN
                        }
                    }
                };
                Value::Float(result)
            }
            (Value::Float(l), Value::Int(r)) => {
                let r = r as f64;
                let result = match self.op {
                    AggBinOp::Add => l + r,
                    AggBinOp::Sub => l - r,
                    AggBinOp::Mul => l * r,
                    AggBinOp::Div => {
                        if r != 0.0 {
                            l / r
                        } else {
                            f64::NAN
                        }
                    }
                };
                Value::Float(result)
            }
            _ => Value::Null,
        }
    }
}

impl Ema {
    pub fn new(period: usize) -> Self {
        Self {
            period: period.max(1),
        }
    }
}

impl AggregateFunc for Ema {
    fn name(&self) -> &str {
        "ema"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let k = 2.0 / (self.period as f64 + 1.0);

        // Single-pass EMA calculation
        let mut ema: Option<f64> = None;
        for event in events {
            if let Some(value) = event.get_float(field) {
                ema = Some(match ema {
                    Some(prev) => value * k + prev * (1.0 - k),
                    None => value,
                });
            }
        }

        ema.map(Value::Float).unwrap_or(Value::Null)
    }

    fn apply_refs(&self, events: &[&Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let k = 2.0 / (self.period as f64 + 1.0);

        let mut ema: Option<f64> = None;
        for event in events {
            if let Some(value) = event.get_float(field) {
                ema = Some(match ema {
                    Some(prev) => value * k + prev * (1.0 - k),
                    None => value,
                });
            }
        }

        ema.map(Value::Float).unwrap_or(Value::Null)
    }
}

/// Aggregator that can apply multiple aggregations
pub struct Aggregator {
    aggregations: Vec<(String, Box<dyn AggregateFunc>, Option<String>)>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            aggregations: Vec::new(),
        }
    }

    pub fn add(
        mut self,
        alias: impl Into<String>,
        func: Box<dyn AggregateFunc>,
        field: Option<String>,
    ) -> Self {
        self.aggregations.push((alias.into(), func, field));
        self
    }

    pub fn apply(&self, events: &[Event]) -> AggResult {
        let mut result = IndexMap::new();
        for (alias, func, field) in &self.aggregations {
            let value = func.apply(events, field.as_deref());
            result.insert(alias.clone(), value);
        }
        result
    }

    /// Apply aggregations to SharedEvent slice (avoids cloning in hot paths)
    pub fn apply_shared(&self, events: &[SharedEvent]) -> AggResult {
        let mut result = IndexMap::new();
        for (alias, func, field) in &self.aggregations {
            let value = func.apply_shared(events, field.as_deref());
            result.insert(alias.clone(), value);
        }
        result
    }

    /// Apply aggregations to a columnar buffer (SIMD-optimized).
    ///
    /// This method uses lazy column extraction for SIMD operations on contiguous memory.
    /// Each field is extracted once and cached for subsequent aggregations on the same field.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let aggregator = Aggregator::new()
    ///     .add("total", Box::new(Sum), Some("price".to_string()))
    ///     .add("avg_price", Box::new(Avg), Some("price".to_string()))
    ///     .add("count", Box::new(Count), None);
    ///
    /// let mut window = TumblingWindow::new(Duration::seconds(60));
    /// // ... add events ...
    /// let mut buffer = window.flush_columnar();
    /// let results = aggregator.apply_columnar(&mut buffer);
    /// ```
    pub fn apply_columnar(&self, buffer: &mut ColumnarBuffer) -> AggResult {
        let mut result = IndexMap::new();
        for (alias, func, field) in &self.aggregations {
            let value = func.apply_columnar(buffer, field.as_deref());
            result.insert(alias.clone(), value);
        }
        result
    }
}

impl Default for Aggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_events() -> Vec<Event> {
        vec![
            Event::new("Test").with_field("value", 10.0),
            Event::new("Test").with_field("value", 20.0),
            Event::new("Test").with_field("value", 30.0),
        ]
    }

    #[test]
    fn test_count() {
        let events = make_events();
        let result = Count.apply(&events, None);
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_sum() {
        let events = make_events();
        let result = Sum.apply(&events, Some("value"));
        assert_eq!(result, Value::Float(60.0));
    }

    #[test]
    fn test_avg() {
        let events = make_events();
        let result = Avg.apply(&events, Some("value"));
        assert_eq!(result, Value::Float(20.0));
    }

    #[test]
    fn test_min_max() {
        let events = make_events();
        assert_eq!(Min.apply(&events, Some("value")), Value::Float(10.0));
        assert_eq!(Max.apply(&events, Some("value")), Value::Float(30.0));
    }

    #[test]
    fn test_aggregator() {
        let events = make_events();
        let aggregator = Aggregator::new()
            .add("count", Box::new(Count), None)
            .add("sum", Box::new(Sum), Some("value".to_string()))
            .add("avg", Box::new(Avg), Some("value".to_string()));

        let result = aggregator.apply(&events);
        assert_eq!(result.get("count"), Some(&Value::Int(3)));
        assert_eq!(result.get("sum"), Some(&Value::Float(60.0)));
        assert_eq!(result.get("avg"), Some(&Value::Float(20.0)));
    }

    #[test]
    fn test_first_last() {
        let events = make_events();
        assert_eq!(First.apply(&events, Some("value")), Value::Float(10.0));
        assert_eq!(Last.apply(&events, Some("value")), Value::Float(30.0));
    }

    #[test]
    fn test_stddev() {
        let events = make_events();
        let result = StdDev.apply(&events, Some("value"));
        if let Value::Float(v) = result {
            assert!((v - 10.0).abs() < 0.01); // stddev of [10, 20, 30] = 10
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_ema() {
        let events = vec![
            Event::new("Test").with_field("value", 100.0),
            Event::new("Test").with_field("value", 110.0),
            Event::new("Test").with_field("value", 120.0),
            Event::new("Test").with_field("value", 130.0),
            Event::new("Test").with_field("value", 140.0),
        ];
        let ema = Ema::new(3);
        let result = ema.apply(&events, Some("value"));
        if let Value::Float(v) = result {
            // EMA(3) with k = 0.5: should be weighted towards recent values
            assert!(v > 120.0 && v < 140.0);
        } else {
            panic!("Expected float");
        }
    }

    // ==========================================================================
    // Edge Case Tests
    // ==========================================================================

    #[test]
    fn test_count_empty() {
        let events: Vec<Event> = vec![];
        assert_eq!(Count.apply(&events, None), Value::Int(0));
    }

    #[test]
    fn test_sum_empty() {
        let events: Vec<Event> = vec![];
        assert_eq!(Sum.apply(&events, Some("value")), Value::Float(0.0));
    }

    #[test]
    fn test_avg_empty() {
        let events: Vec<Event> = vec![];
        assert_eq!(Avg.apply(&events, Some("value")), Value::Null);
    }

    #[test]
    fn test_min_empty() {
        let events: Vec<Event> = vec![];
        assert_eq!(Min.apply(&events, Some("value")), Value::Null);
    }

    #[test]
    fn test_max_empty() {
        let events: Vec<Event> = vec![];
        assert_eq!(Max.apply(&events, Some("value")), Value::Null);
    }

    #[test]
    fn test_first_empty() {
        let events: Vec<Event> = vec![];
        assert_eq!(First.apply(&events, Some("value")), Value::Null);
    }

    #[test]
    fn test_last_empty() {
        let events: Vec<Event> = vec![];
        assert_eq!(Last.apply(&events, Some("value")), Value::Null);
    }

    #[test]
    fn test_stddev_single_value() {
        let events = vec![Event::new("Test").with_field("value", 42.0)];
        assert_eq!(StdDev.apply(&events, Some("value")), Value::Null);
    }

    #[test]
    fn test_ema_empty() {
        let events: Vec<Event> = vec![];
        assert_eq!(Ema::new(3).apply(&events, Some("value")), Value::Null);
    }

    #[test]
    fn test_ema_single_value() {
        let events = vec![Event::new("Test").with_field("value", 100.0)];
        assert_eq!(
            Ema::new(3).apply(&events, Some("value")),
            Value::Float(100.0)
        );
    }

    #[test]
    fn test_ema_period_zero_becomes_one() {
        let ema = Ema::new(0);
        assert_eq!(ema.period, 1);
    }

    #[test]
    fn test_missing_field() {
        let events = vec![Event::new("Test").with_field("other", 10.0)];
        assert_eq!(Sum.apply(&events, Some("value")), Value::Float(0.0));
        assert_eq!(Avg.apply(&events, Some("value")), Value::Null);
    }

    #[test]
    fn test_default_field() {
        let events = vec![Event::new("Test").with_field("value", 25.0)];
        // When field is None, default is "value"
        assert_eq!(Sum.apply(&events, None), Value::Float(25.0));
        assert_eq!(Avg.apply(&events, None), Value::Float(25.0));
    }

    // ==========================================================================
    // ExprAggregate Tests
    // ==========================================================================

    #[test]
    fn test_expr_aggregate_add() {
        let events = make_events(); // sum = 60
        let expr = ExprAggregate::new(
            Box::new(Sum),
            Some("value".to_string()),
            AggBinOp::Add,
            Box::new(Count),
            None,
        );
        // 60.0 + 3 = 63.0
        assert_eq!(expr.apply(&events, None), Value::Float(63.0));
    }

    #[test]
    fn test_expr_aggregate_sub() {
        let events = make_events();
        let expr = ExprAggregate::new(
            Box::new(Max),
            Some("value".to_string()),
            AggBinOp::Sub,
            Box::new(Min),
            Some("value".to_string()),
        );
        // 30.0 - 10.0 = 20.0
        assert_eq!(expr.apply(&events, None), Value::Float(20.0));
    }

    #[test]
    fn test_expr_aggregate_mul() {
        let events = vec![
            Event::new("Test").with_field("value", 5.0),
            Event::new("Test").with_field("value", 5.0),
        ];
        let expr = ExprAggregate::new(
            Box::new(Avg),
            Some("value".to_string()),
            AggBinOp::Mul,
            Box::new(Count),
            None,
        );
        // 5.0 * 2 = 10.0
        assert_eq!(expr.apply(&events, None), Value::Float(10.0));
    }

    #[test]
    fn test_expr_aggregate_div() {
        let events = make_events();
        let expr = ExprAggregate::new(
            Box::new(Sum),
            Some("value".to_string()),
            AggBinOp::Div,
            Box::new(Count),
            None,
        );
        // 60.0 / 3 = 20.0
        assert_eq!(expr.apply(&events, None), Value::Float(20.0));
    }

    #[test]
    fn test_expr_aggregate_div_by_zero_float() {
        let events: Vec<Event> = vec![];
        let expr = ExprAggregate::new(
            Box::new(Sum),
            Some("value".to_string()),
            AggBinOp::Div,
            Box::new(Sum),
            Some("value".to_string()),
        );
        // 0.0 / 0.0 = NaN
        if let Value::Float(v) = expr.apply(&events, None) {
            assert!(v.is_nan());
        } else {
            panic!("Expected float NaN");
        }
    }

    #[test]
    fn test_expr_aggregate_int_operations() {
        let events = vec![
            Event::new("Test").with_field("count", 10i64),
            Event::new("Test").with_field("count", 20i64),
        ];
        // This tests that Count returns Int
        let expr = ExprAggregate::new(Box::new(Count), None, AggBinOp::Mul, Box::new(Count), None);
        // 2 * 2 = 4
        assert_eq!(expr.apply(&events, None), Value::Int(4));
    }

    // ==========================================================================
    // Aggregator Builder Tests
    // ==========================================================================

    #[test]
    fn test_aggregator_empty() {
        let events = make_events();
        let aggregator = Aggregator::new();
        let result = aggregator.apply(&events);
        assert!(result.is_empty());
    }

    #[test]
    fn test_aggregator_default() {
        let aggregator = Aggregator::default();
        assert!(aggregator.aggregations.is_empty());
    }

    #[test]
    fn test_aggregator_chain() {
        let events = make_events();
        let aggregator = Aggregator::new()
            .add("min_val", Box::new(Min), Some("value".to_string()))
            .add("max_val", Box::new(Max), Some("value".to_string()))
            .add(
                "range",
                Box::new(ExprAggregate::new(
                    Box::new(Max),
                    Some("value".to_string()),
                    AggBinOp::Sub,
                    Box::new(Min),
                    Some("value".to_string()),
                )),
                None,
            );

        let result = aggregator.apply(&events);
        assert_eq!(result.get("min_val"), Some(&Value::Float(10.0)));
        assert_eq!(result.get("max_val"), Some(&Value::Float(30.0)));
        assert_eq!(result.get("range"), Some(&Value::Float(20.0)));
    }

    // ==========================================================================
    // Name Tests
    // ==========================================================================

    #[test]
    fn test_aggregate_names() {
        assert_eq!(Count.name(), "count");
        assert_eq!(Sum.name(), "sum");
        assert_eq!(Avg.name(), "avg");
        assert_eq!(Min.name(), "min");
        assert_eq!(Max.name(), "max");
        assert_eq!(StdDev.name(), "stddev");
        assert_eq!(First.name(), "first");
        assert_eq!(Last.name(), "last");
        assert_eq!(Ema::new(5).name(), "ema");
    }

    #[test]
    fn test_expr_aggregate_name() {
        let expr = ExprAggregate::new(Box::new(Sum), None, AggBinOp::Add, Box::new(Count), None);
        assert_eq!(expr.name(), "expr");
    }

    // ==========================================================================
    // NaN Handling Tests
    // ==========================================================================

    #[test]
    fn test_min_with_nan_values_no_panic() {
        let events = vec![
            Event::new("Test").with_field("value", f64::NAN),
            Event::new("Test").with_field("value", 20.0),
            Event::new("Test").with_field("value", f64::NAN),
            Event::new("Test").with_field("value", 10.0),
        ];
        let result = Min.apply(&events, Some("value"));
        // Should return 10.0 (minimum of non-NaN values)
        assert_eq!(result, Value::Float(10.0));
    }

    #[test]
    fn test_max_with_nan_values_no_panic() {
        let events = vec![
            Event::new("Test").with_field("value", f64::NAN),
            Event::new("Test").with_field("value", 20.0),
            Event::new("Test").with_field("value", f64::NAN),
            Event::new("Test").with_field("value", 30.0),
        ];
        let result = Max.apply(&events, Some("value"));
        // Should return 30.0 (maximum of non-NaN values)
        assert_eq!(result, Value::Float(30.0));
    }

    #[test]
    fn test_min_all_nan_returns_null() {
        let events = vec![
            Event::new("Test").with_field("value", f64::NAN),
            Event::new("Test").with_field("value", f64::NAN),
        ];
        let result = Min.apply(&events, Some("value"));
        // All NaN should return Null
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_max_all_nan_returns_null() {
        let events = vec![
            Event::new("Test").with_field("value", f64::NAN),
            Event::new("Test").with_field("value", f64::NAN),
        ];
        let result = Max.apply(&events, Some("value"));
        // All NaN should return Null
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_count_distinct_basic() {
        let events = vec![
            Event::new("Test").with_field("category", "A"),
            Event::new("Test").with_field("category", "B"),
            Event::new("Test").with_field("category", "A"),
            Event::new("Test").with_field("category", "C"),
            Event::new("Test").with_field("category", "B"),
        ];
        let result = CountDistinct.apply(&events, Some("category"));
        // Should count 3 distinct values: A, B, C
        assert_eq!(result, Value::Int(3));
    }

    // ==========================================================================
    // Columnar Aggregation Tests
    // ==========================================================================

    use crate::columnar::ColumnarBuffer;
    use std::sync::Arc;

    fn make_columnar_buffer() -> ColumnarBuffer {
        let events = vec![
            Arc::new(Event::new("Test").with_field("value", 10.0)),
            Arc::new(Event::new("Test").with_field("value", 20.0)),
            Arc::new(Event::new("Test").with_field("value", 30.0)),
        ];
        ColumnarBuffer::from_events(events)
    }

    #[test]
    fn test_columnar_count() {
        let mut buffer = make_columnar_buffer();
        let result = Count.apply_columnar(&mut buffer, None);
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_columnar_sum() {
        let mut buffer = make_columnar_buffer();
        let result = Sum.apply_columnar(&mut buffer, Some("value"));
        assert_eq!(result, Value::Float(60.0));
    }

    #[test]
    fn test_columnar_avg() {
        let mut buffer = make_columnar_buffer();
        let result = Avg.apply_columnar(&mut buffer, Some("value"));
        assert_eq!(result, Value::Float(20.0));
    }

    #[test]
    fn test_columnar_min() {
        let mut buffer = make_columnar_buffer();
        let result = Min.apply_columnar(&mut buffer, Some("value"));
        assert_eq!(result, Value::Float(10.0));
    }

    #[test]
    fn test_columnar_max() {
        let mut buffer = make_columnar_buffer();
        let result = Max.apply_columnar(&mut buffer, Some("value"));
        assert_eq!(result, Value::Float(30.0));
    }

    #[test]
    fn test_columnar_with_nan() {
        let events = vec![
            Arc::new(Event::new("Test").with_field("value", 10.0)),
            Arc::new(Event::new("Test")), // Missing value -> NaN
            Arc::new(Event::new("Test").with_field("value", 30.0)),
        ];
        let mut buffer = ColumnarBuffer::from_events(events);

        // Sum should ignore NaN
        let sum_result = Sum.apply_columnar(&mut buffer, Some("value"));
        assert_eq!(sum_result, Value::Float(40.0));

        // Avg should only count non-NaN values
        let avg_result = Avg.apply_columnar(&mut buffer, Some("value"));
        assert_eq!(avg_result, Value::Float(20.0)); // 40/2

        // Min/Max should ignore NaN
        let min_result = Min.apply_columnar(&mut buffer, Some("value"));
        assert_eq!(min_result, Value::Float(10.0));
        let max_result = Max.apply_columnar(&mut buffer, Some("value"));
        assert_eq!(max_result, Value::Float(30.0));
    }

    #[test]
    fn test_columnar_aggregator() {
        let mut buffer = make_columnar_buffer();
        let aggregator = Aggregator::new()
            .add("count", Box::new(Count), None)
            .add("sum", Box::new(Sum), Some("value".to_string()))
            .add("avg", Box::new(Avg), Some("value".to_string()))
            .add("min", Box::new(Min), Some("value".to_string()))
            .add("max", Box::new(Max), Some("value".to_string()));

        let result = aggregator.apply_columnar(&mut buffer);
        assert_eq!(result.get("count"), Some(&Value::Int(3)));
        assert_eq!(result.get("sum"), Some(&Value::Float(60.0)));
        assert_eq!(result.get("avg"), Some(&Value::Float(20.0)));
        assert_eq!(result.get("min"), Some(&Value::Float(10.0)));
        assert_eq!(result.get("max"), Some(&Value::Float(30.0)));
    }

    #[test]
    fn test_columnar_column_caching() {
        let mut buffer = make_columnar_buffer();

        // First access extracts column
        assert!(!buffer.has_column("value"));
        let _sum1 = Sum.apply_columnar(&mut buffer, Some("value"));
        assert!(buffer.has_column("value"));

        // Second access reuses cached column
        let _sum2 = Avg.apply_columnar(&mut buffer, Some("value"));
        assert!(buffer.has_column("value"));
    }

    #[test]
    fn test_columnar_empty_buffer() {
        let mut buffer = ColumnarBuffer::new();

        assert_eq!(Count.apply_columnar(&mut buffer, None), Value::Int(0));
        assert_eq!(
            Sum.apply_columnar(&mut buffer, Some("value")),
            Value::Float(0.0)
        );
        assert_eq!(Avg.apply_columnar(&mut buffer, Some("value")), Value::Null);
        assert_eq!(Min.apply_columnar(&mut buffer, Some("value")), Value::Null);
        assert_eq!(Max.apply_columnar(&mut buffer, Some("value")), Value::Null);
    }
}
