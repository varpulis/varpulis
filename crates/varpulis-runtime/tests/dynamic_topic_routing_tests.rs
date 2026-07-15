//! Tests for dynamic topic routing in `.to()` operations.
//!
//! Covers:
//! - Pipeline integration: events grouped by evaluated topic and routed via send_batch_to_topic
//! - Static vs dynamic topic dispatch
//! - Concatenation expression evaluation
//! - Edge cases: missing fields, integer coercion

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::sink::{Sink, SinkError};

// ===========================================================================
// Capturing mock sink
// ===========================================================================

/// Records every call to `send_batch` and `send_batch_to_topic`.
#[derive(Debug)]
struct CaptureSink {
    name: String,
    /// (topic_or_none, serialised_event_types)
    calls: Mutex<Vec<(Option<String>, Vec<String>)>>,
}

impl CaptureSink {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Return captured calls as (topic, [event_type, ...]).
    fn captured(&self) -> Vec<(Option<String>, Vec<String>)> {
        self.calls.lock().unwrap().clone()
    }

    /// Flatten all captured topic->event_types into a map: topic -> count.
    fn topic_counts(&self) -> HashMap<String, usize> {
        let mut map = HashMap::new();
        for (topic, events) in self.calls.lock().unwrap().iter() {
            let key = topic.clone().unwrap_or_else(|| "(default)".to_string());
            *map.entry(key).or_insert(0) += events.len();
        }
        map
    }
}

#[async_trait]
impl Sink for CaptureSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), SinkError> {
        self.calls
            .lock()
            .unwrap()
            .push((None, vec![event.event_type.to_string()]));
        Ok(())
    }

    async fn send_batch(&self, events: &[Arc<Event>]) -> Result<(), SinkError> {
        let types: Vec<String> = events.iter().map(|e| e.event_type.to_string()).collect();
        self.calls.lock().unwrap().push((None, types));
        Ok(())
    }

    async fn send_batch_to_topic(
        &self,
        events: &[Arc<Event>],
        topic: &str,
    ) -> Result<(), SinkError> {
        let types: Vec<String> = events.iter().map(|e| e.event_type.to_string()).collect();
        self.calls
            .lock()
            .unwrap()
            .push((Some(topic.to_string()), types));
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), SinkError> {
        Ok(())
    }
}

// ===========================================================================
// 1. Dynamic topic routing with bare identifier
// ===========================================================================

#[tokio::test]
async fn dynamic_topic_ident_routes_by_field_value() {
    let code = r#"
        connector Broker = kafka(brokers: "localhost:9092")
        stream Out = Input.to(Broker, topic: category)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let sink = Arc::new(CaptureSink::new("Broker"));
    engine.inject_sink("Broker", sink.clone() as Arc<dyn Sink>);

    let events = vec![
        Event::new("Input")
            .with_field("category", Value::Str("alerts".into()))
            .with_field("msg", Value::Str("fire".into())),
        Event::new("Input")
            .with_field("category", Value::Str("metrics".into()))
            .with_field("msg", Value::Str("cpu".into())),
        Event::new("Input")
            .with_field("category", Value::Str("alerts".into()))
            .with_field("msg", Value::Str("flood".into())),
    ];

    for e in events {
        engine.process(e).await.unwrap();
    }

    let counts = sink.topic_counts();
    assert_eq!(counts.get("alerts"), Some(&2), "2 events routed to alerts");
    assert_eq!(counts.get("metrics"), Some(&1), "1 event routed to metrics");
    assert!(
        !counts.contains_key("(default)"),
        "Dynamic routing should never use send_batch"
    );
}

// ===========================================================================
// 2. Dynamic topic routing with concatenation
// ===========================================================================

#[tokio::test]
async fn dynamic_topic_concat_routes_with_prefix() {
    let code = r#"
        connector Broker = kafka(brokers: "localhost:9092")
        stream Out = Input.to(Broker, topic: "events-" + region)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let sink = Arc::new(CaptureSink::new("Broker"));
    engine.inject_sink("Broker", sink.clone() as Arc<dyn Sink>);

    let events = vec![
        Event::new("Input").with_field("region", Value::Str("us-east".into())),
        Event::new("Input").with_field("region", Value::Str("eu-west".into())),
        Event::new("Input").with_field("region", Value::Str("us-east".into())),
    ];

    for e in events {
        engine.process(e).await.unwrap();
    }

    let counts = sink.topic_counts();
    assert_eq!(
        counts.get("events-us-east"),
        Some(&2),
        "2 events to events-us-east"
    );
    assert_eq!(
        counts.get("events-eu-west"),
        Some(&1),
        "1 event to events-eu-west"
    );
}

// ===========================================================================
// 3. Dynamic topic with three-part concatenation
// ===========================================================================

#[tokio::test]
async fn dynamic_topic_three_part_concat() {
    let code = r#"
        connector Broker = kafka(brokers: "localhost:9092")
        stream Out = Input.to(Broker, topic: "tenant." + tid + ".events")
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let sink = Arc::new(CaptureSink::new("Broker"));
    engine.inject_sink("Broker", sink.clone() as Arc<dyn Sink>);

    let events = vec![
        Event::new("Input").with_field("tid", Value::Str("acme".into())),
        Event::new("Input").with_field("tid", Value::Str("globex".into())),
    ];

    for e in events {
        engine.process(e).await.unwrap();
    }

    let counts = sink.topic_counts();
    assert_eq!(counts.get("tenant.acme.events"), Some(&1));
    assert_eq!(counts.get("tenant.globex.events"), Some(&1));
}

// ===========================================================================
// 4. Static topic uses send_batch (not send_batch_to_topic)
// ===========================================================================

#[tokio::test]
async fn static_topic_uses_send_batch() {
    let code = r#"
        connector Broker = kafka(brokers: "localhost:9092")
        stream Out = Input.to(Broker, topic: "fixed-topic")
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Static topic sink_key is "Broker::fixed-topic"
    let sink = Arc::new(CaptureSink::new("Broker"));
    engine.inject_sink("Broker::fixed-topic", sink.clone() as Arc<dyn Sink>);

    let events = vec![
        Event::new("Input").with_field("x", Value::Int(1)),
        Event::new("Input").with_field("x", Value::Int(2)),
    ];

    for e in events {
        engine.process(e).await.unwrap();
    }

    let calls = sink.captured();
    assert!(
        calls.iter().all(|(topic, _)| topic.is_none()),
        "Static topic should use send_batch, not send_batch_to_topic"
    );
    let total: usize = calls.iter().map(|(_, evts)| evts.len()).sum();
    assert_eq!(total, 2, "Both events should be sent");
}

// ===========================================================================
// 5. Dynamic topic with integer field value (bare ident)
// ===========================================================================

#[tokio::test]
async fn dynamic_topic_int_field_coerced_to_string() {
    let code = r#"
        connector Broker = kafka(brokers: "localhost:9092")
        stream Out = Input.to(Broker, topic: partition_id)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let sink = Arc::new(CaptureSink::new("Broker"));
    engine.inject_sink("Broker", sink.clone() as Arc<dyn Sink>);

    let events = vec![
        Event::new("Input").with_field("partition_id", Value::Int(0)),
        Event::new("Input").with_field("partition_id", Value::Int(1)),
        Event::new("Input").with_field("partition_id", Value::Int(0)),
    ];

    for e in events {
        engine.process(e).await.unwrap();
    }

    let counts = sink.topic_counts();
    assert_eq!(counts.get("0"), Some(&2));
    assert_eq!(counts.get("1"), Some(&1));
}

// ===========================================================================
// 6. Dynamic topic with missing field evaluates to "null"
// ===========================================================================

#[tokio::test]
async fn dynamic_topic_missing_field_routes_to_null() {
    let code = r#"
        connector Broker = kafka(brokers: "localhost:9092")
        stream Out = Input.to(Broker, topic: missing_field)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let sink = Arc::new(CaptureSink::new("Broker"));
    engine.inject_sink("Broker", sink.clone() as Arc<dyn Sink>);

    let events = vec![Event::new("Input").with_field("x", Value::Int(1))];

    for e in events {
        engine.process(e).await.unwrap();
    }

    let counts = sink.topic_counts();
    // Missing field -> Value::Null -> "null" string
    assert_eq!(counts.get("null"), Some(&1));
}

// ===========================================================================
// 7. Multiple destinations tracked correctly
// ===========================================================================

#[tokio::test]
async fn dynamic_topic_groups_batch_correctly() {
    let code = r#"
        connector Broker = kafka(brokers: "localhost:9092")
        stream Out = Input.to(Broker, topic: dest)
    "#;
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let sink = Arc::new(CaptureSink::new("Broker"));
    engine.inject_sink("Broker", sink.clone() as Arc<dyn Sink>);

    let destinations = ["alpha", "beta", "alpha", "gamma", "beta"];
    for dest in destinations {
        let e = Event::new("Input").with_field("dest", Value::Str(dest.into()));
        engine.process(e).await.unwrap();
    }

    let counts = sink.topic_counts();
    assert_eq!(counts.get("alpha"), Some(&2));
    assert_eq!(counts.get("beta"), Some(&2));
    assert_eq!(counts.get("gamma"), Some(&1));
}

// ===========================================================================
// 8. No topic param uses default send_batch
// ===========================================================================

#[tokio::test]
async fn no_topic_param_uses_default_send_batch() {
    let code = r"
        connector out = console()
        stream Out = Input.to(out)
    ";
    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let sink = Arc::new(CaptureSink::new("out"));
    engine.inject_sink("out", sink.clone() as Arc<dyn Sink>);

    let events = vec![Event::new("Input").with_field("x", Value::Int(42))];

    for e in events {
        engine.process(e).await.unwrap();
    }

    let calls = sink.captured();
    assert!(
        calls.iter().all(|(topic, _)| topic.is_none()),
        "No topic param should use send_batch"
    );
}
