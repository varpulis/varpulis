//! Aggregation functions for stream processing

use crate::event::Event;
use indexmap::IndexMap;
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
        let sum: f64 = events
            .iter()
            .filter_map(|e| e.get_float(field))
            .sum();
        Value::Float(sum)
    }
}

/// Average aggregation
pub struct Avg;

impl AggregateFunc for Avg {
    fn name(&self) -> &str {
        "avg"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let values: Vec<f64> = events
            .iter()
            .filter_map(|e| e.get_float(field))
            .collect();
        
        if values.is_empty() {
            return Value::Null;
        }

        let sum: f64 = values.iter().sum();
        Value::Float(sum / values.len() as f64)
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
            .min_by(|a, b| a.partial_cmp(b).unwrap())
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
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .map(Value::Float)
            .unwrap_or(Value::Null)
    }
}

/// Standard deviation aggregation
pub struct StdDev;

impl AggregateFunc for StdDev {
    fn name(&self) -> &str {
        "stddev"
    }

    fn apply(&self, events: &[Event], field: Option<&str>) -> Value {
        let field = field.unwrap_or("value");
        let values: Vec<f64> = events
            .iter()
            .filter_map(|e| e.get_float(field))
            .collect();

        if values.len() < 2 {
            return Value::Null;
        }

        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;
        let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
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

    pub fn add(mut self, alias: impl Into<String>, func: Box<dyn AggregateFunc>, field: Option<String>) -> Self {
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
}
