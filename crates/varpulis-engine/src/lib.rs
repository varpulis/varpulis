//! # varpulis-engine
//!
//! The Varpulis complex-event-processing engine as a library for a process
//! that has **no async runtime**: no tokio, no broker client, no HTTP server.
//! You compile a VPL program, you feed it events, you get emits back, and you
//! decide where they go. Everything that reads or writes a network is the
//! host's, which is the point: the host already has its bus.
//!
//! This is the seam Vejas ADR-0031 attaches to. A `detect` unit there is a
//! [`Program`] driven from a supervisor thread: events come off a durable
//! consumer, go through [`Program::feed`], and every [`Emit`] is published on
//! the subject its `.to()` binding names before the input is acknowledged.
//!
//! ```no_run
//! use varpulis_engine::Program;
//!
//! let mut program = Program::compile(r#"
//!     event Order:
//!         id: str
//!         total: float
//!
//!     stream Large = Order
//!         .where(total > 250.0)
//!         .emit(order: id, total: total)
//! "#).unwrap();
//!
//! let emits = program.feed_json("Order", br#"{"id": "SO#1", "total": 347.0}"#).unwrap();
//! assert_eq!(emits.len(), 1);
//! assert_eq!(emits[0].stream, "Large");
//! ```
//!
//! ## What the engine does and does not do here
//!
//! - **Time is event time.** An event's `timestamp` field (or `ts`, or
//!   `@timestamp`, RFC 3339) is its time; a field missing means "now". Every
//!   `.within()` and window is evaluated against it, so replaying the same
//!   events yields the same emits.
//! - **`.from()` and `.to()` are declarations, not connections.** They are
//!   reported through [`Program::sources`] and [`Program::sinks`] for the
//!   host to honour. Nothing in this crate opens a socket.
//! - **State lives in the [`Program`].** Windows, sequences and aggregates are
//!   in memory; snapshotting them is the host's contract, not this crate's.
//!
//! ## The guarantee this crate makes
//!
//! `cargo tree -p varpulis-engine` names no `tokio`, `reqwest`, `axum`,
//! `async-nats`, `rdkafka` or `arrow`. `scripts/check-engine-deps.py` asserts
//! it on every pull request, because a dependency that pulls one of them in
//! would compile without a word and undo the reason this crate exists.

#![forbid(unsafe_code)]

pub use varpulis_core::{Event, Value};
use varpulis_runtime::Engine;
pub use varpulis_runtime::{EngineError, SinkBinding, SourceBinding};

/// What can go wrong between a VPL string and an emit.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The VPL did not parse.
    #[error("VPL does not parse: {0}")]
    Parse(String),
    /// The program parsed but the engine refused to load it, or failed while
    /// processing.
    #[error("engine: {0}")]
    Engine(#[from] EngineError),
    /// A JSON payload could not become an event.
    #[error("event: {0}")]
    Event(String),
}

/// A compiled VPL program with its running state.
///
/// One `Program` is one unit of detection: feed it events in order, publish
/// what comes out. It is `Send`, so a host may move it into the thread that
/// owns its input.
pub struct Program {
    engine: Engine,
    sources: Vec<SourceBinding>,
    sinks: Vec<SinkBinding>,
    source_text: String,
}

impl std::fmt::Debug for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Program")
            .field("sources", &self.sources)
            .field("sinks", &self.sinks)
            .field("vpl_bytes", &self.source_text.len())
            .finish_non_exhaustive()
    }
}

/// One output event, with where the program said it goes.
#[derive(Debug, Clone)]
pub struct Emit {
    /// The stream that produced it.
    pub stream: String,
    /// The `.to()` binding of that stream, when it has one. A stream without
    /// one is an intermediate result the host may log, forward, or drop.
    pub sink: Option<SinkBinding>,
    /// The event itself. Its `event_type` is the stream name.
    pub event: Event,
}

impl Emit {
    /// The payload the Varpulis NATS sink would publish for this event:
    /// `{"event_type": ..., "timestamp": ..., <fields>...}`.
    ///
    /// Using the sink's own encoding means a host that embeds the engine
    /// publishes byte-for-byte what a Varpulis worker would have published.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.event.to_sink_payload()).unwrap_or(serde_json::Value::Null)
    }
}

impl Program {
    /// Parse and load a VPL program.
    ///
    /// Everything that can be rejected is rejected here — syntax, unknown
    /// event types, a `.filter()` the engine refuses — so a host can `check`
    /// a file before it is deployed and get the same verdict.
    pub fn compile(vpl: &str) -> Result<Self, Error> {
        let program = varpulis_parser::parse(vpl).map_err(|e| Error::Parse(e.to_string()))?;
        let mut engine = Engine::new_sync();
        engine.load(&program)?;
        let sources = engine.source_bindings().to_vec();
        let sinks = engine.sink_bindings();
        Ok(Self {
            engine,
            sources,
            sinks,
            source_text: vpl.to_string(),
        })
    }

    /// [`Program::compile`] without keeping the result. Same verdict.
    pub fn check(vpl: &str) -> Result<(), Error> {
        Self::compile(vpl).map(|_| ())
    }

    /// The `.from()` bindings: what the host must subscribe to, and the event
    /// type each subject's payloads are decoded as.
    pub fn sources(&self) -> &[SourceBinding] {
        &self.sources
    }

    /// The `.to()` bindings: where the host must publish each stream's emits.
    pub fn sinks(&self) -> &[SinkBinding] {
        &self.sinks
    }

    /// The VPL this program was compiled from.
    pub fn source_text(&self) -> &str {
        &self.source_text
    }

    /// Feed one event and return everything it caused to be emitted.
    pub fn feed(&mut self, event: Event) -> Result<Vec<Emit>, Error> {
        self.feed_batch(vec![event])
    }

    /// Feed events in order and return everything they caused to be emitted.
    pub fn feed_batch(&mut self, events: Vec<Event>) -> Result<Vec<Emit>, Error> {
        let out = self.engine.process_batch_sync_collect(events)?;
        Ok(self.route(out))
    }

    /// Decode a JSON object into an event and feed it.
    ///
    /// `default_event_type` applies when the payload has neither `event_type`
    /// nor `type`; a host typically passes the event type of the `.from()`
    /// binding the payload arrived on. See [`event_from_json`].
    pub fn feed_json(&mut self, default_event_type: &str, json: &[u8]) -> Result<Vec<Emit>, Error> {
        let event = event_from_json(default_event_type, json)?;
        self.feed(event)
    }

    /// Close every open window as if no further event will ever arrive, and
    /// return what that emits.
    ///
    /// For bounded input — a replay, a fixture, a test. A live unit never
    /// calls it: its windows close on event time as events keep coming.
    pub fn end_of_input(&mut self) -> Result<Vec<Emit>, Error> {
        self.engine.flush_end_of_input_sync()?;
        let out = self.engine.take_collected_outputs();
        Ok(self.route(out))
    }

    fn route(&self, events: Vec<Event>) -> Vec<Emit> {
        events
            .into_iter()
            .map(|event| {
                let stream = event.event_type.to_string();
                let sink = self.sinks.iter().find(|s| s.stream == stream).cloned();
                Emit {
                    stream,
                    sink,
                    event,
                }
            })
            .collect()
    }
}

/// Decode a JSON object into an [`Event`].
///
/// The event type is `event_type`, else `type`, else `default_event_type`.
/// The timestamp is `timestamp`, else `ts`, else `@timestamp`, parsed as
/// RFC 3339; absent or unparseable means now. Those keys are not kept as
/// fields. Every other member becomes a field, nested objects and arrays
/// included, so `shipping_address.country` in a program reaches into the
/// payload's `{"shipping_address": {"country": ...}}`.
pub fn event_from_json(default_event_type: &str, json: &[u8]) -> Result<Event, Error> {
    let value: serde_json::Value =
        serde_json::from_slice(json).map_err(|e| Error::Event(e.to_string()))?;
    let Some(object) = value.as_object() else {
        return Err(Error::Event("payload is not a JSON object".to_string()));
    };

    let event_type = object
        .get("event_type")
        .or_else(|| object.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or(default_event_type);

    let timestamp = ["timestamp", "ts", "@timestamp"]
        .iter()
        .find_map(|k| object.get(*k))
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Utc));

    let mut event = match timestamp {
        Some(t) => Event::new_at(event_type, t),
        None => Event::new(event_type),
    };

    for (key, member) in object {
        if matches!(
            key.as_str(),
            "event_type" | "type" | "timestamp" | "ts" | "@timestamp"
        ) {
            continue;
        }
        let field: Value = serde_json::from_value(member.clone())
            .map_err(|e| Error::Event(format!("field '{key}': {e}")))?;
        event = event.with_field(key.as_str(), field);
    }
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    #[test]
    fn a_program_can_be_moved_into_the_thread_that_owns_its_input() {
        assert_send::<Program>();
        assert_send::<Emit>();
    }

    #[test]
    fn a_parse_error_is_a_parse_error() {
        let err = Program::check("stream = = =").unwrap_err();
        assert!(matches!(err, Error::Parse(_)), "{err}");
    }

    #[test]
    fn json_routing_keys_become_type_and_time_and_the_rest_become_fields() {
        let ev = event_from_json(
            "Fallback",
            br#"{"type": "Order", "timestamp": "2026-09-21T10:00:00Z",
                 "id": "SO#1", "n": 2, "price": 1.5, "ok": true,
                 "shipping_address": {"country": "France"}, "items": [1, 2]}"#,
        )
        .unwrap();
        assert_eq!(&*ev.event_type, "Order");
        assert_eq!(ev.timestamp.to_rfc3339(), "2026-09-21T10:00:00+00:00");
        assert!(ev.data.get("type").is_none(), "routing keys are not fields");
        assert!(ev.data.get("timestamp").is_none());
        assert_eq!(ev.data.get("n"), Some(&Value::Int(2)));
        assert_eq!(ev.data.get("price"), Some(&Value::Float(1.5)));
        assert_eq!(ev.data.get("ok"), Some(&Value::Bool(true)));
        assert!(
            matches!(ev.data.get("shipping_address"), Some(Value::Map(_))),
            "nested objects are kept as maps"
        );
        assert!(matches!(ev.data.get("items"), Some(Value::Array(_))));

        let ev = event_from_json("Fallback", br#"{"x": 1}"#).unwrap();
        assert_eq!(&*ev.event_type, "Fallback");

        let err = event_from_json("Fallback", b"[1, 2]").unwrap_err();
        assert!(matches!(err, Error::Event(_)));
    }
}
