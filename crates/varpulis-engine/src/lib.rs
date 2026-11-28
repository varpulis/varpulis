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
//! - **Time is event time.** A payload's `@timestamp` (RFC 3339), else its
//!   `ts` or `timestamp` (epoch milliseconds), is the event's time — the same
//!   rules as every Varpulis connector, decoded by the same
//!   [`varpulis_core::decode::EventDecoder`]; none of them means "now". Every
//!   `.within()` and window is evaluated against it, so replaying the same
//!   events yields the same emits.
//! - **`.from()` and `.to()` are declarations, not connections.** They are
//!   reported through [`Program::sources`] and [`Program::sinks`] for the
//!   host to honour. Nothing in this crate opens a socket.
//! - **Timers run on the host's clock.** A `timer()` source has no thread:
//!   [`Program::next_deadline`] says when it is next due and
//!   [`Program::fire_due`] fires what is due at the time the host gives, or,
//!   under [`TimerClock::EventTime`], the events fed move the timers' clock.
//!   A clock that jumps several periods fires a timer once, not once per
//!   period missed.
//! - **State lives in the [`Program`].** Windows, sequences and aggregates are
//!   in memory. [`Program::snapshot`] hands them to the host as bytes and
//!   [`Program::restore`] takes them back; when to take one, where to keep
//!   it and which position in the input it stands for is the host's
//!   contract, not this crate's.
//!
//! ## The guarantee this crate makes
//!
//! `cargo tree -p varpulis-engine` names no `tokio`, `reqwest`, `axum`,
//! `async-nats`, `rdkafka` or `arrow`. `scripts/check-engine-deps.py` asserts
//! it on every pull request, because a dependency that pulls one of them in
//! would compile without a word and undo the reason this crate exists.

#![forbid(unsafe_code)]

mod timers;

pub use chrono::{DateTime, Utc};
pub use timers::TimerClock;
pub use varpulis_core::decode::EventDecoder;
pub use varpulis_core::{Event, Value};
use varpulis_runtime::Engine;
pub use varpulis_runtime::{EngineError, LossCounters, SinkBinding, SourceBinding};

/// What can go wrong between a VPL string and an emit.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The VPL did not parse.
    #[error("VPL does not parse: {0}")]
    Parse(String),
    /// The VPL parsed but the semantic validator found errors: one line per
    /// diagnostic, `line:col: error[CODE] message`, with its hint.
    #[error("{0}")]
    Invalid(String),
    /// The program parsed but the engine refused to load it, or failed while
    /// processing.
    #[error("engine: {0}")]
    Engine(#[from] EngineError),
    /// A JSON payload could not become an event.
    #[error("event: {0}")]
    Event(String),
    /// A snapshot could not be taken or put back.
    #[error("snapshot: {0}")]
    Snapshot(String),
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
    /// One decoder per program: its field-name cache makes a steady stream
    /// (the same schema on every payload) decode with no key allocations.
    decoder: EventDecoder,
    /// What [`Program::losses`] reports is counted since this `Program` was
    /// built, across restores: the engine's own counters restart from the
    /// snapshot's on [`Program::restore`], so what was counted before is
    /// carried here and the engine's value at the restore is the new base.
    losses_carried: LossCounters,
    losses_base: LossCounters,
    /// The `timer()` sources and their schedules.
    timers: timers::Timers,
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
        serde_json::from_slice(&self.to_payload()).unwrap_or(serde_json::Value::Null)
    }

    /// The same payload as [`Emit::to_json`], as the bytes to publish: one
    /// serialisation, no intermediate tree. A host on a hot path wants this
    /// one.
    pub fn to_payload(&self) -> Vec<u8> {
        self.event.to_sink_payload()
    }
}

impl Program {
    /// Parse and load a VPL program.
    ///
    /// Everything that can be rejected is rejected here — syntax, unknown
    /// event types, a `.filter()` the engine refuses — so a host can `check`
    /// a file before it is deployed and get the same verdict.
    ///
    /// So is every construct the language has and this engine does not run:
    /// `import`, `context`, `.enrich()` and `.alert(webhook: ...)` are
    /// refused with codes E100, E101, E103 and E104 rather than loaded and
    /// skipped, and a `timer()` whose period or initial delay is not a
    /// duration literal, or whose period is zero, with E102. A `timer()`
    /// written with literals is run: see [`Program::fire_due`].
    pub fn compile(vpl: &str) -> Result<Self, Error> {
        let program = varpulis_parser::parse(vpl).map_err(|e| Error::Parse(e.to_string()))?;
        let unsupported = varpulis_core::validate::support::check_sync_engine_support(&program);
        let mut engine = Engine::new_sync();
        let loaded = engine.load(&program);
        if unsupported.has_errors() {
            // The load still ran, so a program refused for a construct is
            // also reported for anything else wrong with it.
            let mut msg = unsupported.format(vpl).trim_end().to_string();
            if let Err(e) = loaded {
                msg.push_str(&format!("\nerror: {e}"));
            }
            return Err(Error::Invalid(msg));
        }
        loaded?;
        let sources = engine.source_bindings().to_vec();
        let sinks = engine.sink_bindings();
        let timers = timers::Timers::new(engine.get_timers());
        Ok(Self {
            engine,
            sources,
            sinks,
            source_text: vpl.to_string(),
            decoder: EventDecoder::new(),
            losses_carried: LossCounters::default(),
            losses_base: LossCounters::default(),
            timers,
        })
    }

    /// [`Program::compile`] without keeping the result. Same verdict.
    pub fn check(vpl: &str) -> Result<(), Error> {
        Self::check_with_warnings(vpl).map(|_| ())
    }

    /// [`Program::check`], returning the validator's warnings as formatted
    /// lines (`line:col: warning[CODE] message`, then its hint).
    ///
    /// A check is the parse, the semantic validator (unknown event types when
    /// the program declares its events, unbounded closures, regular
    /// expressions that do not compile, ...) and a load into the engine.
    /// [`Program::compile`] skips the validator: a host decides whether to
    /// check first.
    pub fn check_with_warnings(vpl: &str) -> Result<Vec<String>, Error> {
        let program = varpulis_parser::parse(vpl).map_err(|e| Error::Parse(e.to_string()))?;
        let validation = varpulis_core::validate::validate(vpl, &program);
        if validation.has_errors() {
            return Err(Error::Invalid(
                validation.format(vpl).trim_end().to_string(),
            ));
        }
        Self::compile(vpl)?;
        Ok(validation.format(vpl).lines().map(str::to_string).collect())
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

    /// What the program has let go of without an error: pattern runs not
    /// opened because a pattern held [`Program::max_runs`] runs already,
    /// events a `.where()` discarded because its predicate had no value, and
    /// timer periods skipped because the timers' clock jumped past them (see
    /// [`Program::fire_due`]).
    /// Plain counters, cheap to read on every batch; a host publishes them,
    /// since each one is a detection that may have been missed.
    ///
    /// They count what this process saw since the `Program` was built: a
    /// [`Program::restore`] neither resets them nor adds what the snapshot's
    /// program had counted.
    pub fn losses(&self) -> LossCounters {
        let now = self.engine.loss_counters();
        let since = |carried: u64, base: u64, now: u64| carried + now.saturating_sub(base);
        LossCounters {
            runs_dropped: since(
                self.losses_carried.runs_dropped,
                self.losses_base.runs_dropped,
                now.runs_dropped,
            ),
            runs_evicted: since(
                self.losses_carried.runs_evicted,
                self.losses_base.runs_evicted,
                now.runs_evicted,
            ),
            filters_without_value: since(
                self.losses_carried.filters_without_value,
                self.losses_base.filters_without_value,
                now.filters_without_value,
            ),
            timer_periods_skipped: self.timers.periods_skipped,
        }
    }

    /// The cap on concurrent runs of each pattern, 10 000 by default; `None`
    /// when the program has no pattern. Past it, a new run is not opened and
    /// is counted in [`LossCounters::runs_dropped`]. For a partitioned
    /// pattern the cap is per partition.
    pub fn max_runs(&self) -> Option<usize> {
        self.engine.max_runs()
    }

    /// Change [`Program::max_runs`]. Each open run costs memory (about
    /// 1.4 KB with its captured event), so the cap bounds the memory a
    /// pattern may take: `max_runs` runs for an unpartitioned pattern, and
    /// up to partitions × `max_runs` for a partitioned one, since the cap
    /// applies to each partition. Runs already open are kept.
    pub fn set_max_runs(&mut self, max: usize) {
        self.engine.set_max_runs(max);
    }

    /// Feed one event and return everything it caused to be emitted.
    pub fn feed(&mut self, event: Event) -> Result<Vec<Emit>, Error> {
        self.feed_batch(vec![event])
    }

    /// Feed events in order and return everything they caused to be emitted.
    ///
    /// Under [`TimerClock::EventTime`], a timer due at or before an event's
    /// time fires just before that event, and what it emits comes first.
    pub fn feed_batch(&mut self, events: Vec<Event>) -> Result<Vec<Emit>, Error> {
        if !self.timers.follows_events() {
            let out = self.engine.process_batch_sync_collect(events)?;
            return Ok(self.route(out));
        }
        let mut emits = Vec::new();
        let mut run: Vec<Event> = Vec::with_capacity(events.len());
        for event in events {
            if self.timers.is_due(event.timestamp) || self.timers.next_deadline().is_none() {
                let fired = self.timers.advance(event.timestamp);
                if !fired.is_empty() {
                    if !run.is_empty() {
                        let out = self
                            .engine
                            .process_batch_sync_collect(std::mem::take(&mut run))?;
                        emits.extend(self.route(out));
                    }
                    let out = self.engine.process_batch_sync_collect(fired)?;
                    emits.extend(self.route(out));
                }
            }
            run.push(event);
        }
        if !run.is_empty() {
            let out = self.engine.process_batch_sync_collect(run)?;
            emits.extend(self.route(out));
        }
        Ok(emits)
    }

    /// When the next `timer()` of this program is due: the time to give
    /// [`Program::fire_due`] next. `None` when the program has no timer, or
    /// when its timers have not started, which they do the first time
    /// [`Program::fire_due`] is called (or, under [`TimerClock::EventTime`],
    /// the first event is fed).
    pub fn next_deadline(&self) -> Option<DateTime<Utc>> {
        self.timers.next_deadline()
    }

    /// Advance the timers' clock to `now` and return what the timers that
    /// are due emit, with everything downstream of them.
    ///
    /// The first call starts the timers: a `timer(p, initial_delay: d)`
    /// first fires at `now + d + p`, then every `p`, and this call fires
    /// nothing. A live host calls it once when it starts, then whenever
    /// [`Program::next_deadline`] is reached; it may call it more often, or
    /// late. A `now` that is not later than the clock does nothing.
    ///
    /// Late by more than a period, a timer fires **once**, stamped with the
    /// latest deadline `now` has reached, and the periods before it are
    /// counted in [`LossCounters::timer_periods_skipped`]: a host that was
    /// paused for an hour gets one heartbeat, not 3 600. Its next deadline
    /// stays on its schedule, so calling late never makes it drift.
    ///
    /// The event a timer fires has the type `Timer_<stream>`, the deadline as
    /// its time and as a `timestamp` field (epoch milliseconds), and goes
    /// through the stream's operators like any other event.
    pub fn fire_due(&mut self, now: DateTime<Utc>) -> Result<Vec<Emit>, Error> {
        let fired = self.timers.advance(now);
        if fired.is_empty() {
            return Ok(Vec::new());
        }
        let out = self.engine.process_batch_sync_collect(fired)?;
        Ok(self.route(out))
    }

    /// Choose what moves the timers: [`TimerClock::Host`], the default, or
    /// [`TimerClock::EventTime`]. A program with no `timer()` ignores it.
    pub fn set_timer_clock(&mut self, clock: TimerClock) {
        self.timers.mode = clock;
    }

    /// What moves the timers. See [`Program::set_timer_clock`].
    pub fn timer_clock(&self) -> TimerClock {
        self.timers.mode
    }

    /// Whether the program has a `timer()` source, and so whether its host
    /// has a clock to drive.
    pub fn has_timers(&self) -> bool {
        !self.timers.is_empty()
    }

    /// Decode a JSON payload into an event and feed it.
    ///
    /// `default_event_type` applies when the payload has no string
    /// `event_type`; a host passes the event type of the `.from()` binding the
    /// payload arrived on. The decode is the connectors' own
    /// ([`EventDecoder`]), with this program's interning cache, so it is the
    /// path a hot loop should take rather than [`event_from_json`].
    pub fn feed_json(&mut self, default_event_type: &str, json: &[u8]) -> Result<Vec<Emit>, Error> {
        let event = self
            .decoder
            .decode(default_event_type, json)
            .map_err(|e| Error::Event(e.to_string()))?;
        self.feed(event)
    }

    /// For a live host: once an event type has sent nothing for `grace` of
    /// wall-clock time, its event time moves on with the wall clock, less the
    /// grace, so the windows it feeds close even when it has nothing more to
    /// say. A brute force on a sparse source is then raised about a grace
    /// after its window ends, instead of whenever the source speaks again.
    ///
    /// `None`, the default, judges the program in event time alone, as a
    /// replay needs: the same events give the same alerts however fast they
    /// are fed. A host that sets a grace calls [`Program::tick`] when it has
    /// nothing to feed.
    pub fn set_idle_grace(&mut self, grace: Option<std::time::Duration>) {
        self.engine.set_idle_grace(grace);
    }

    /// Close what time has closed while nothing arrived, and return what that
    /// emits. See [`Program::set_idle_grace`].
    pub fn tick(&mut self) -> Result<Vec<Emit>, Error> {
        self.tick_at(std::time::Instant::now())
    }

    /// [`Program::tick`] as of `now`.
    pub fn tick_at(&mut self, now: std::time::Instant) -> Result<Vec<Emit>, Error> {
        self.engine.tick_sync_at(now)?;
        let out = self.engine.take_collected_outputs();
        Ok(self.route(out))
    }

    /// Close every open window as if no further event will ever arrive, and
    /// return what that emits.
    ///
    /// For bounded input — a replay, a fixture, a test. A live unit never
    /// calls it: its windows close on event time as events keep coming.
    /// Timers do not fire here: past the last event, no deadline is reached.
    pub fn end_of_input(&mut self) -> Result<Vec<Emit>, Error> {
        self.engine.flush_end_of_input_sync()?;
        let out = self.engine.take_collected_outputs();
        Ok(self.route(out))
    }

    /// The program's state as bytes: open sequences with their event-time
    /// deadlines, windows, joins, distinct and limit counters, variables and
    /// the watermark, and where the timers stand (their clock and each one's
    /// next deadline). The host keeps it next to whatever names the position
    /// in its input — a stream sequence, an offset — and gives it to
    /// [`Program::restore`] on a `Program` compiled from the same source.
    ///
    /// Not in it: trend aggregates (`.trend_aggregate()`) and forecasts
    /// (`.forecast()`), which start empty after a restore.
    pub fn snapshot(&self) -> Result<Vec<u8>, Error> {
        let mut checkpoint = self.engine.create_checkpoint();
        checkpoint.timers = self.timers.checkpoint();
        varpulis_runtime::codec::serialize(
            &checkpoint,
            varpulis_runtime::codec::CheckpointFormat::active(),
        )
        .map_err(|e| Error::Snapshot(e.to_string()))
    }

    /// Put back what [`Program::snapshot`] returned, on a program compiled
    /// from the same source. The host then resumes its input just after the
    /// position the snapshot stands for; what it feeds again is judged as if
    /// for the first time, so a sequence opened before the snapshot closes
    /// on the event that arrives after the restart.
    pub fn restore(&mut self, snapshot: &[u8]) -> Result<(), Error> {
        let checkpoint: varpulis_runtime::persistence::EngineCheckpoint =
            varpulis_runtime::codec::deserialize(snapshot)
                .map_err(|e| Error::Snapshot(e.to_string()))?;
        let carried = self.losses();
        let restored = self
            .engine
            .restore_checkpoint(&checkpoint)
            .map_err(|e| Error::Snapshot(e.to_string()));
        // Whatever the restore did to the engine's counters, losses keep
        // counting from where they were.
        self.losses_carried = carried;
        self.losses_base = self.engine.loss_counters();
        if restored.is_ok() {
            // A timer resumes its schedule: it fires next where it would
            // have, and a snapshot taken before the timers started leaves
            // them to start on the next time given.
            self.timers.restore(checkpoint.timers.as_ref());
        }
        restored
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

/// Decode a JSON payload into an [`Event`], with a fresh decoder.
///
/// The event type is the payload's string `event_type`, else
/// `default_event_type`; the time is `@timestamp` (RFC 3339), else `ts` or
/// `timestamp` (epoch milliseconds), else now. `event_type` is not kept as a
/// field; everything else is, nested objects and arrays included, so
/// `shipping_address.country` in a program reaches into the payload's
/// `{"shipping_address": {"country": ...}}`. A payload that is not a JSON
/// object decodes to an event with no fields; malformed JSON is an error.
/// Same rules and same limits as every Varpulis connector.
///
/// A fresh [`EventDecoder`] each call: fine for a one-off, wasteful on a
/// stream. A host feeding many payloads uses [`Program::feed_json`], which
/// keeps one decoder and its interning cache.
pub fn event_from_json(default_event_type: &str, json: &[u8]) -> Result<Event, Error> {
    EventDecoder::new()
        .decode(default_event_type, json)
        .map_err(|e| Error::Event(e.to_string()))
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

    fn refusal(vpl: &str) -> String {
        match Program::compile(vpl) {
            Err(Error::Invalid(msg)) => msg,
            Err(other) => panic!("expected a refusal, got {other}"),
            Ok(_) => panic!("expected a refusal, the program compiled"),
        }
    }

    const ORDER: &str = "event Order:\n    id: str\n    total: float\n\n";

    #[test]
    fn what_this_engine_does_not_run_is_refused_with_its_code() {
        let cases = [
            ("import \"other.vpl\"\n", "E100"),
            ("context ingest\n", "E101"),
            (
                "stream S = Order\n    .emit(context: other, id: id)\n",
                "E101",
            ),
            ("stream Beat = timer(0s)\n", "E102"),
            ("stream Beat = timer(5)\n", "E102"),
            ("stream Beat = timer(5s, initial_delay: 2)\n", "E102"),
            (
                "stream S = Order\n    .enrich(Db, key: id, fields: [name])\n",
                "E103",
            ),
            (
                "stream S = Order\n    .alert(webhook: \"https://example.invalid\")\n",
                "E104",
            ),
        ];
        for (body, code) in cases {
            let vpl = format!("{ORDER}{body}");
            let msg = refusal(&vpl);
            assert!(msg.contains(&format!("error[{code}]")), "{code}: {msg}");
            assert!(msg.contains("hint:"), "{code} carries a hint: {msg}");
            // `check` gives the same verdict as `compile`.
            assert!(Program::check(&vpl).is_err(), "{code}");
        }
    }

    #[test]
    fn a_timer_written_with_literals_compiles() {
        let program = Program::compile("stream Beat = timer(5s, initial_delay: 1s)\n").unwrap();
        assert!(program.has_timers());
        assert_eq!(program.next_deadline(), None, "not started");
    }

    #[test]
    fn an_engine_without_a_clock_still_refuses_a_timer() {
        use varpulis_core::validate::support::{
            check_engine_support, check_sync_engine_support, EngineSupport,
        };
        let program = varpulis_parser::parse("stream Beat = timer(5s)\n").unwrap();
        assert!(check_sync_engine_support(&program).diagnostics.is_empty());
        let refused = check_engine_support(&program, EngineSupport::WITHOUT_TIMERS);
        assert_eq!(refused.diagnostics.len(), 1);
        assert_eq!(refused.diagnostics[0].code, Some("E102"));
    }

    #[test]
    fn a_join_is_an_error_on_its_first_event_rather_than_silence() {
        let mut program = Program::compile(&format!(
            "{ORDER}stream A = Order\nstream B = Order\nstream J = join(A, B)\n    .on(A.id == B.id)\n"
        ))
        .unwrap();
        let err = program
            .feed_json("Order", br#"{"id": "a", "total": 1.0}"#)
            .unwrap_err();
        assert!(err.to_string().contains("join"), "{err}");
    }

    #[test]
    fn a_refused_program_is_still_loaded_so_its_other_errors_show() {
        let vpl = format!(
            "{ORDER}context ingest\n\
             stream S = Order.score(model: \"m.onnx\", inputs: [total], outputs: [p])\n"
        );
        let msg = refusal(&vpl);
        assert!(msg.contains("error[E101]"), "{msg}");
        assert!(
            msg.contains("onnx"),
            "the load error is reported too: {msg}"
        );
    }

    #[test]
    fn an_alert_that_only_logs_still_compiles() {
        let vpl = format!("{ORDER}stream S = Order\n    .alert(message: \"big {{id}}\")\n");
        Program::compile(&vpl).unwrap();
    }

    #[test]
    fn a_pattern_at_its_cap_counts_the_runs_it_could_not_open() {
        let mut program = Program::compile(
            "event A:\n    k: int\nevent B:\n    k: int\n\n\
             stream M = A as a -> B where k == a.k as b\n    .within(1h)\n    .emit(k: a.k)\n",
        )
        .unwrap();
        assert_eq!(program.max_runs(), Some(10_000));
        program.set_max_runs(2);
        assert_eq!(program.max_runs(), Some(2));
        let at = |i: i64| format!("2026-11-27T10:00:{i:02}Z");
        for i in 0..5 {
            let json = format!(r#"{{"k": {i}, "@timestamp": "{}"}}"#, at(i));
            program.feed_json("A", json.as_bytes()).unwrap();
        }
        assert_eq!(program.losses().runs_dropped, 3, "{:?}", program.losses());
        // The runs that did open still match.
        let json = format!(r#"{{"k": 1, "@timestamp": "{}"}}"#, at(10));
        assert_eq!(program.feed_json("B", json.as_bytes()).unwrap().len(), 1);
        // A run past the cap is lost: its B finds nothing.
        let json = format!(r#"{{"k": 4, "@timestamp": "{}"}}"#, at(11));
        assert!(program.feed_json("B", json.as_bytes()).unwrap().is_empty());
    }

    #[test]
    fn a_filter_with_no_value_is_counted() {
        let mut program = Program::compile(&format!(
            "{ORDER}stream Large = Order\n    .where(total > 250.0)\n    .emit(id: id)\n"
        ))
        .unwrap();
        program
            .feed_json("Order", br#"{"id": "a", "total": 300.0}"#)
            .unwrap();
        program
            .feed_json("Order", br#"{"id": "b", "total": 10.0}"#)
            .unwrap();
        assert_eq!(program.losses().filters_without_value, 0);
        // No `total`: the predicate has no value and the event is dropped.
        let out = program.feed_json("Order", br#"{"id": "c"}"#).unwrap();
        assert!(out.is_empty());
        assert_eq!(program.losses().filters_without_value, 1);
    }

    #[test]
    fn a_compound_filter_dropped_for_want_of_a_value_is_counted() {
        let mut program = Program::compile(&format!(
            "{ORDER}stream Large = Order\n    .where(total > 250.0 and id != \"x\")\n    .emit(id: id)\n"
        ))
        .unwrap();
        // Decided by a false condition: not counted.
        program
            .feed_json("Order", br#"{"id": "x", "total": 300.0}"#)
            .unwrap();
        program
            .feed_json("Order", br#"{"id": "a", "total": 1.0}"#)
            .unwrap();
        assert_eq!(program.losses().filters_without_value, 0);
        // No `total`: `and` turns it into false, and that drop is counted.
        assert!(program
            .feed_json("Order", br#"{"id": "c"}"#)
            .unwrap()
            .is_empty());
        assert_eq!(program.losses().filters_without_value, 1);

        let mut program = Program::compile(&format!(
            "{ORDER}stream Any = Order\n    .where(total > 250.0 or id == \"vip\")\n    .emit(id: id)\n"
        ))
        .unwrap();
        // `or` holds on its other side: passed, nothing lost.
        assert_eq!(
            program
                .feed_json("Order", br#"{"id": "vip"}"#)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(program.losses().filters_without_value, 0);
        // Neither side holds and one had no value: counted.
        assert!(program
            .feed_json("Order", br#"{"id": "c"}"#)
            .unwrap()
            .is_empty());
        assert_eq!(program.losses().filters_without_value, 1);
    }

    #[test]
    fn a_merge_filter_dropped_for_want_of_a_value_is_counted() {
        let mut program = Program::compile(&format!(
            "{ORDER}stream Big = merge(stream O = Order.where(total > 250.0))\n    .emit(id: id)\n"
        ))
        .unwrap();
        program
            .feed_json("Order", br#"{"id": "a", "total": 1.0}"#)
            .unwrap();
        assert_eq!(program.losses().filters_without_value, 0);
        assert!(program
            .feed_json("Order", br#"{"id": "c"}"#)
            .unwrap()
            .is_empty());
        assert_eq!(program.losses().filters_without_value, 1);
    }

    #[test]
    fn losses_count_since_the_program_was_built_across_a_restore() {
        let vpl = "event A:\n    k: int\nevent B:\n    k: int\n\n\
             stream M = A as a -> B where k == a.k as b\n    .within(1h)\n    .emit(k: a.k)\n\
             stream F = B\n    .where(k > 0)\n    .emit(k: k)\n";
        let at = |i: i64| format!("2026-11-27T10:00:{i:02}Z");
        let mut other = Program::compile(vpl).unwrap();
        other.set_max_runs(1);
        for i in 0..3 {
            let json = format!(r#"{{"k": {i}, "@timestamp": "{}"}}"#, at(i));
            other.feed_json("A", json.as_bytes()).unwrap();
        }
        assert_eq!(other.losses().runs_dropped, 2);
        let snapshot = other.snapshot().unwrap();

        let mut program = Program::compile(vpl).unwrap();
        program.set_max_runs(1);
        for i in 0..4 {
            let json = format!(r#"{{"k": {i}, "@timestamp": "{}"}}"#, at(i));
            program.feed_json("A", json.as_bytes()).unwrap();
        }
        program
            .feed_json("B", br#"{"@timestamp": "2026-11-27T10:00:05Z"}"#)
            .unwrap();
        let before = program.losses();
        assert_eq!(before.runs_dropped, 3);
        assert_eq!(before.filters_without_value, 1);

        program.restore(&snapshot).unwrap();
        assert_eq!(
            program.losses(),
            before,
            "a restore neither resets nor imports"
        );
        let json = format!(r#"{{"k": 9, "@timestamp": "{}"}}"#, at(20));
        program.feed_json("A", json.as_bytes()).unwrap();
        assert_eq!(program.losses().runs_dropped, before.runs_dropped + 1);
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
            br#"{"event_type": "Order", "@timestamp": "2026-09-21T10:00:00Z",
                 "id": "SO#1", "n": 2, "price": 1.5, "ok": true,
                 "shipping_address": {"country": "France"}, "items": [1, 2]}"#,
        )
        .unwrap();
        assert_eq!(&*ev.event_type, "Order");
        assert_eq!(ev.timestamp.to_rfc3339(), "2026-09-21T10:00:00+00:00");
        assert!(
            ev.data.get("event_type").is_none(),
            "the type is not a field"
        );
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

        // Not an object: an event with no fields, the connectors' own fallback.
        let ev = event_from_json("Fallback", b"[1, 2]").unwrap();
        assert!(ev.data.is_empty());
        assert_eq!(&*ev.event_type, "Fallback");

        let err = event_from_json("Fallback", b"{not json").unwrap_err();
        assert!(matches!(err, Error::Event(_)));
    }
}
