//! Aggregation functions for stream processing

use crate::event::Event;
use indexmap::IndexMap;
use std::hash::{Hash, Hasher};
use varpulis_core::Value;

/// Result of an aggregation
pub type AggResult = IndexMap<String, Value>;

/// Aggregation function trait
pub trait AggregateFunc: Send + Sync {
    fn name(&self) -> &str;
    fn apply(&self, events: &[Event], field: Option<&str>) -> Value;
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
}

/// Sum aggregation
pub struct Sum;

impl AggregateFunc for Sum {
    fn name(&self) -> &str {
        "sum"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let sum: f64 = events.iter().filter_map(|e| e.get_float(field)).sum();
        Value::Float(sum)
    }
}

/// Average aggregation (single-pass algorithm)
pub struct Avg;

impl AggregateFunc for Avg {
    fn name(&self) -> &str {
        "avg"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");

        // Single-pass: accumulate sum and count together
        let (sum, count) =
            events
                .iter()
                .fold((0.0, 0usize), |(sum, count), e| match e.get_float(field) {
                    Some(v) => (sum + v, count + 1),
                    None => (sum, count),
                });

        if count == 0 {
            Value::Null
        } else {
            Value::Float(sum / count as f64)
        }
    }
}

/// Min aggregation
pub struct Min;

impl AggregateFunc for Min {
    fn name(&self) -> &str {
        "min"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        events
            .iter()
            .filter_map(|e| e.get_float(field))
            .filter(|x| !x.is_nan()) // Filter out NaN values
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(Value::Float)
            .unwrap_or(Value::Null)
    }
}

/// Max aggregation
pub struct Max;

impl AggregateFunc for Max {
    fn name(&self) -> &str {
        "max"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        events
            .iter()
            .filter_map(|e| e.get_float(field))
            .filter(|x| !x.is_nan()) // Filter out NaN values
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(Value::Float)
            .unwrap_or(Value::Null)
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
        let values: Vec<f64> = events.iter().filter_map(|e| e.get_float(field)).collect();

        if values.is_empty() {
            return Value::Null;
        }

        let k = 2.0 / (self.period as f64 + 1.0);

        // Start with first value as initial EMA
        let mut ema = values[0];

        // Calculate EMA for each subsequent value
        for value in values.iter().skip(1) {
            ema = value * k + ema * (1.0 - k);
        }

        Value::Float(ema)
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
}
