//! Interactive session core.
//!
//! [`InteractiveSession`] wraps a Varpulis [`Engine`] and
//! exposes a command/response API for interactive use. Transports (JSON-line,
//! TUI, MCP) call [`handle_command`](InteractiveSession::handle_command) and
//! receive a [`broadcast::Receiver`] for asynchronous output events.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use super::protocol::{SessionCommand, SessionResponse, StreamInfo, TraceEntryJson};
use crate::engine::graph::program_to_graph;
use crate::engine::trace::TraceEntry;
use crate::engine::Engine;
use crate::event::Event;
use crate::event_file::EventFileParser;

/// The broadcast channel capacity for output events.
const BROADCAST_CAPACITY: usize = 4096;

/// The mpsc channel capacity between engine output and the session relay.
const OUTPUT_CHANNEL_CAPACITY: usize = 8192;

/// Interactive session wrapping a Varpulis engine.
///
/// Create with [`InteractiveSession::new()`], obtain a subscriber via
/// [`subscribe()`](Self::subscribe), and drive the session by calling
/// [`handle_command()`](Self::handle_command) for each incoming command.
///
/// When the `interactive` feature is enabled, event generation via
/// [`SessionCommand::Generate`] is supported. Generated events are buffered
/// in an internal channel and processed through the engine when
/// [`poll_generator()`](Self::poll_generator) is called.
pub struct InteractiveSession {
    engine: Engine,
    output_rx: mpsc::Receiver<Event>,
    broadcast_tx: broadcast::Sender<SessionResponse>,
    program: Option<varpulis_core::ast::Program>,
    program_loaded: bool,
    /// Accumulated VPL source for incremental building (Python-interpreter style).
    vpl_buffer: String,
    // Datagen state
    generator_running: Arc<AtomicBool>,
    generator_count: Arc<AtomicU64>,
    generator_handle: Option<tokio::task::JoinHandle<()>>,
    /// Receiver for generated events (populated when a generator is running).
    generator_rx: Option<mpsc::Receiver<Event>>,
    // Connector state
    /// Sender for connector-sourced events.
    connector_tx: mpsc::Sender<Event>,
    /// Receiver for connector-sourced events (polled like generator).
    connector_rx: Option<mpsc::Receiver<Event>>,
    /// Managed connector registry (owns live connections).
    connector_registry: Option<crate::connector::ManagedConnectorRegistry>,
    // Subscriptions (None = all streams)
    subscribed_streams: Option<HashSet<String>>,
}

impl std::fmt::Debug for InteractiveSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InteractiveSession")
            .field("program_loaded", &self.program_loaded)
            .field(
                "generator_running",
                &self.generator_running.load(Ordering::Relaxed),
            )
            .field(
                "generator_count",
                &self.generator_count.load(Ordering::Relaxed),
            )
            .field("connector_active", &self.connector_registry.is_some())
            .field("subscribed_streams", &self.subscribed_streams)
            .finish_non_exhaustive()
    }
}

impl Default for InteractiveSession {
    fn default() -> Self {
        Self::new()
    }
}

impl InteractiveSession {
    /// Create a new interactive session with a fresh engine.
    pub fn new() -> Self {
        let (output_tx, output_rx) = mpsc::channel(OUTPUT_CHANNEL_CAPACITY);
        let engine = Engine::new(output_tx);
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (connector_tx, connector_rx) = mpsc::channel::<Event>(8192);

        Self {
            engine,
            output_rx,
            broadcast_tx,
            program: None,
            program_loaded: false,
            vpl_buffer: String::new(),
            generator_running: Arc::new(AtomicBool::new(false)),
            generator_count: Arc::new(AtomicU64::new(0)),
            generator_handle: None,
            generator_rx: None,
            connector_tx,
            connector_rx: Some(connector_rx),
            connector_registry: None,
            subscribed_streams: None,
        }
    }

    /// Subscribe to session output events.
    ///
    /// Returns a [`broadcast::Receiver`] that will receive all
    /// [`SessionResponse`] values emitted by the session (output events,
    /// trace entries, generator status updates, etc.).
    pub fn subscribe(&self) -> broadcast::Receiver<SessionResponse> {
        self.broadcast_tx.subscribe()
    }

    /// Handle a single command, returning zero or more responses.
    ///
    /// All errors are caught and returned as [`SessionResponse::Error`] --
    /// this method never panics.
    pub fn handle_command(&mut self, cmd: SessionCommand) -> Vec<SessionResponse> {
        match cmd {
            SessionCommand::LoadVpl { vpl } => self.handle_load_vpl(&vpl),
            SessionCommand::AppendVpl { vpl } => self.handle_append_vpl(&vpl),
            SessionCommand::LoadFile { path } => self.handle_load_file(&path),
            SessionCommand::Inject { event_type, data } => self.handle_inject(&event_type, &data),
            SessionCommand::InjectFile { path } => self.handle_inject_file(&path),
            SessionCommand::Generate {
                schema,
                rate,
                duration,
            } => self.handle_generate(&schema, rate, duration),
            SessionCommand::StopGenerate => self.handle_stop_generate(),
            SessionCommand::Subscribe { stream } => self.handle_subscribe(stream),
            SessionCommand::Unsubscribe { stream } => self.handle_unsubscribe(stream),
            SessionCommand::GetStreams => self.handle_get_streams(),
            SessionCommand::GetMetrics => self.handle_get_metrics(),
            SessionCommand::GetTopology => self.handle_get_topology(),
            SessionCommand::SetTrace { enabled } => self.handle_set_trace(enabled),
            SessionCommand::Save { path } => self.handle_save(&path),
            SessionCommand::GetSource => self.handle_get_source(),
            SessionCommand::Quit => vec![SessionResponse::Bye],
        }
    }

    /// Poll the generator channel, processing any buffered generated events
    /// through the engine and returning output responses.
    ///
    /// Callers driving a generator should invoke this periodically (e.g. on
    /// a timer or after each command) to feed generated events into the
    /// pipeline.
    pub fn poll_generator(&mut self) -> Vec<SessionResponse> {
        let rx = match &mut self.generator_rx {
            Some(rx) => rx,
            None => return vec![],
        };

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        if events.is_empty() {
            return vec![];
        }

        if let Err(e) = self.engine.process_batch_sync(events) {
            return vec![SessionResponse::Error {
                message: format!("Generator processing error: {e}"),
            }];
        }

        self.drain_outputs_and_trace()
    }

    /// Poll the connector channel, processing any buffered connector-sourced
    /// events through the engine and returning output responses.
    ///
    /// Callers should invoke this periodically (alongside `poll_generator`)
    /// when connectors have been auto-started from `.from()` bindings.
    pub fn poll_connectors(&mut self) -> Vec<SessionResponse> {
        let rx = match &mut self.connector_rx {
            Some(rx) => rx,
            None => return vec![],
        };

        let mut events = Vec::new();
        let mut seen = std::collections::HashSet::new();
        while let Ok(event) = rx.try_recv() {
            // Deduplicate — MQTT managed connectors may deliver messages
            // twice when multiple topics are subscribed on the same connection.
            // Use event_type + timestamp nanoseconds as dedup key.
            let ts_nanos = event.timestamp.timestamp_nanos_opt().unwrap_or(0);
            let key = (event.event_type.clone(), ts_nanos);
            if seen.insert(key) {
                events.push(event);
            }
        }

        if events.is_empty() {
            return vec![];
        }

        if let Err(e) = self.engine.process_batch_sync(events) {
            return vec![SessionResponse::Error {
                message: format!("Connector event processing error: {e}"),
            }];
        }

        self.drain_outputs_and_trace()
    }

    /// Whether the loaded program has source connector bindings.
    pub fn has_source_bindings(&self) -> bool {
        !self.engine.source_bindings().is_empty()
    }

    /// Start source connectors for the current program's `.from()` bindings.
    ///
    /// Must be called from an async context (uses `tokio::runtime::Handle`).
    /// Shuts down any previously running connectors first.
    pub fn start_connectors(&mut self) -> Result<(), String> {
        use crate::connector::ManagedConnectorRegistry;

        let bindings = self.engine.source_bindings().to_vec();
        if bindings.is_empty() {
            return Ok(());
        }

        // Shutdown any existing connectors
        // Use block_in_place to avoid "cannot block from within a runtime" panic
        if let Some(mut old_registry) = self.connector_registry.take() {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(old_registry.shutdown());
            });
        }

        let mut registry = ManagedConnectorRegistry::from_configs(self.engine.connector_configs())
            .map_err(|e| format!("Registry build error: {e}"))?;

        let mut started_topics: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        for binding in &bindings {
            let config = self.engine.get_connector(&binding.connector_name).cloned();
            let Some(config) = config else { continue };

            let topic = binding
                .topic_override
                .as_deref()
                .or(config.topic.as_deref())
                .unwrap_or("varpulis/events/#");

            // Skip if already started this connector+topic combo
            let key = (binding.connector_name.clone(), topic.to_string());
            if !started_topics.insert(key) {
                continue;
            }

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(registry.start_source(
                    &binding.connector_name,
                    topic,
                    self.connector_tx.clone(),
                    &binding.extra_params,
                ))
            })
            .map_err(|e| format!("Source start error for '{}': {e}", binding.connector_name))?;
        }

        self.connector_registry = Some(registry);
        Ok(())
    }

    // =========================================================================
    // Command handlers
    // =========================================================================

    fn handle_load_vpl(&mut self, vpl: &str) -> Vec<SessionResponse> {
        let program = match varpulis_parser::parse(vpl) {
            Ok(p) => p,
            Err(e) => {
                return vec![SessionResponse::Error {
                    message: format!("Parse error: {e}"),
                }];
            }
        };

        let mut responses = if self.program_loaded {
            // Hot reload
            match self.engine.reload(&program) {
                Ok(report) => {
                    let streams = self
                        .engine
                        .stream_names()
                        .into_iter()
                        .map(String::from)
                        .collect();
                    self.program = Some(program);
                    self.vpl_buffer = vpl.to_string();
                    vec![SessionResponse::Loaded {
                        streams,
                        added: report.streams_added,
                        removed: report.streams_removed,
                        preserved: report.state_preserved,
                    }]
                }
                Err(e) => {
                    return vec![SessionResponse::Error {
                        message: format!("Reload error: {e}"),
                    }];
                }
            }
        } else {
            // First load
            match self.engine.load(&program) {
                Ok(()) => {
                    let streams: Vec<String> = self
                        .engine
                        .stream_names()
                        .into_iter()
                        .map(String::from)
                        .collect();
                    self.program_loaded = true;
                    let added = streams.clone();
                    self.program = Some(program);
                    self.vpl_buffer = vpl.to_string();
                    vec![SessionResponse::Loaded {
                        streams,
                        added,
                        removed: vec![],
                        preserved: vec![],
                    }]
                }
                Err(e) => {
                    return vec![SessionResponse::Error {
                        message: format!("Load error: {e}"),
                    }];
                }
            }
        };

        // Auto-start source connectors if any .from() bindings exist
        if self.has_source_bindings() {
            if let Err(e) = self.start_connectors() {
                responses.push(SessionResponse::Error {
                    message: format!("Connector error: {e}"),
                });
            }
        }

        responses
    }

    /// Append VPL declarations to the accumulated buffer and recompile.
    /// This is the Python-interpreter-style incremental building mode.
    fn handle_append_vpl(&mut self, vpl: &str) -> Vec<SessionResponse> {
        if !self.vpl_buffer.is_empty() {
            self.vpl_buffer.push('\n');
        }
        self.vpl_buffer.push_str(vpl);
        // Recompile the full accumulated buffer
        self.handle_load_vpl(&self.vpl_buffer.clone())
    }

    fn handle_load_file(&mut self, path: &str) -> Vec<SessionResponse> {
        match std::fs::read_to_string(path) {
            Ok(vpl) => self.handle_load_vpl(&vpl),
            Err(e) => vec![SessionResponse::Error {
                message: format!("Failed to read file '{path}': {e}"),
            }],
        }
    }

    fn handle_inject(
        &mut self,
        event_type: &str,
        data: &serde_json::Value,
    ) -> Vec<SessionResponse> {
        if !self.program_loaded {
            return vec![SessionResponse::Error {
                message: "No program loaded. Use load_vpl or load_file first.".into(),
            }];
        }

        let event = json_to_event(event_type, data);
        if let Err(e) = self.engine.process_batch_sync(vec![event]) {
            return vec![SessionResponse::Error {
                message: format!("Processing error: {e}"),
            }];
        }

        self.drain_outputs_and_trace()
    }

    fn handle_inject_file(&mut self, path: &str) -> Vec<SessionResponse> {
        if !self.program_loaded {
            return vec![SessionResponse::Error {
                message: "No program loaded. Use load_vpl or load_file first.".into(),
            }];
        }

        let source = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                return vec![SessionResponse::Error {
                    message: format!("Failed to read event file '{path}': {e}"),
                }];
            }
        };

        let timed_events = match EventFileParser::parse(&source) {
            Ok(te) => te,
            Err(e) => {
                return vec![SessionResponse::Error {
                    message: format!("Event file parse error: {e}"),
                }];
            }
        };

        let events: Vec<Event> = timed_events.into_iter().map(|te| te.event).collect();
        if let Err(e) = self.engine.process_batch_sync(events) {
            return vec![SessionResponse::Error {
                message: format!("Processing error: {e}"),
            }];
        }

        self.drain_outputs_and_trace()
    }

    fn handle_generate(
        &mut self,
        schema: &str,
        rate: u64,
        duration: Option<u64>,
    ) -> Vec<SessionResponse> {
        #[cfg(feature = "interactive")]
        {
            self.start_generator(schema, rate, duration)
        }
        #[cfg(not(feature = "interactive"))]
        {
            let _ = (schema, rate, duration);
            vec![SessionResponse::Error {
                message: "Event generation requires the 'interactive' feature (varpulis-datagen)."
                    .into(),
            }]
        }
    }

    #[cfg(feature = "interactive")]
    fn start_generator(
        &mut self,
        schema: &str,
        rate: u64,
        duration: Option<u64>,
    ) -> Vec<SessionResponse> {
        use varpulis_datagen::{create_schema, SchemaType};

        if !self.program_loaded {
            return vec![SessionResponse::Error {
                message: "No program loaded. Use load_vpl or load_file first.".into(),
            }];
        }

        if self.generator_running.load(Ordering::Relaxed) {
            return vec![SessionResponse::Error {
                message: "Generator is already running. Stop it first with stop_generate.".into(),
            }];
        }

        let schema_type: SchemaType = match schema.parse() {
            Ok(st) => st,
            Err(e) => {
                return vec![SessionResponse::Error { message: e }];
            }
        };

        let cancel = self.generator_running.clone();
        cancel.store(true, Ordering::Relaxed);
        let count = self.generator_count.clone();
        count.store(0, Ordering::Relaxed);
        let broadcast_tx = self.broadcast_tx.clone();

        // Generator task produces events into a channel; the caller must call
        // `poll_generator()` to drain them through the engine.
        let (gen_tx, gen_rx) = mpsc::channel::<Event>(8192);
        self.generator_rx = Some(gen_rx);

        let handle = tokio::spawn(async move {
            let mut gen = create_schema(schema_type, None);
            let interval_micros = if rate > 0 { 1_000_000 / rate } else { 1000 };
            let mut interval =
                tokio::time::interval(std::time::Duration::from_micros(interval_micros));
            let start = tokio::time::Instant::now();
            let duration_limit = duration.map(std::time::Duration::from_secs);

            loop {
                interval.tick().await;

                if !cancel.load(Ordering::Relaxed) {
                    break;
                }

                if let Some(limit) = duration_limit {
                    if start.elapsed() >= limit {
                        cancel.store(false, Ordering::Relaxed);
                        let _ = broadcast_tx.send(SessionResponse::GeneratorStatus {
                            running: false,
                            generated: count.load(Ordering::Relaxed),
                        });
                        break;
                    }
                }

                let generated = gen.next_event();
                let event = generated_to_event(&generated);
                count.fetch_add(1, Ordering::Relaxed);

                if gen_tx.send(event).await.is_err() {
                    // Receiver dropped, stop generating
                    break;
                }
            }
        });

        self.generator_handle = Some(handle);

        vec![SessionResponse::GeneratorStatus {
            running: true,
            generated: 0,
        }]
    }

    fn handle_stop_generate(&mut self) -> Vec<SessionResponse> {
        if !self.generator_running.load(Ordering::Relaxed) {
            return vec![SessionResponse::Error {
                message: "No generator is running.".into(),
            }];
        }

        self.generator_running.store(false, Ordering::Relaxed);

        // Drop the handle — the task will terminate on the next tick
        // because the cancel flag is now false.
        self.generator_handle = None;
        self.generator_rx = None;

        vec![SessionResponse::GeneratorStatus {
            running: false,
            generated: self.generator_count.load(Ordering::Relaxed),
        }]
    }

    fn handle_subscribe(&mut self, stream: Option<String>) -> Vec<SessionResponse> {
        match stream {
            Some(name) => {
                let subs = self.subscribed_streams.get_or_insert_with(HashSet::new);
                subs.insert(name);
            }
            None => {
                // Subscribe to all — remove the filter
                self.subscribed_streams = None;
            }
        }
        vec![]
    }

    fn handle_unsubscribe(&mut self, stream: Option<String>) -> Vec<SessionResponse> {
        match stream {
            Some(name) => {
                if let Some(subs) = &mut self.subscribed_streams {
                    subs.remove(&name);
                }
            }
            None => {
                // Unsubscribe from all — set empty filter
                self.subscribed_streams = Some(HashSet::new());
            }
        }
        vec![]
    }

    fn handle_get_streams(&self) -> Vec<SessionResponse> {
        let streams: Vec<StreamInfo> = self
            .engine
            .streams
            .values()
            .map(|sd| StreamInfo {
                name: sd.name.clone(),
                source: sd.source.describe(),
                ops_count: sd.operations.len(),
            })
            .collect();

        vec![SessionResponse::Streams { streams }]
    }

    fn handle_get_metrics(&self) -> Vec<SessionResponse> {
        let m = self.engine.metrics();
        vec![SessionResponse::Metrics {
            events_processed: m.events_processed,
            output_events: m.output_events_emitted,
            streams_count: m.streams_count,
        }]
    }

    fn handle_get_topology(&self) -> Vec<SessionResponse> {
        match &self.program {
            Some(program) => {
                let graph = program_to_graph(program);
                vec![SessionResponse::Topology {
                    nodes: graph.nodes,
                    edges: graph.edges,
                }]
            }
            None => vec![SessionResponse::Error {
                message: "No program loaded.".into(),
            }],
        }
    }

    fn handle_set_trace(&mut self, enabled: bool) -> Vec<SessionResponse> {
        self.engine.set_trace_enabled(enabled);
        vec![]
    }

    fn handle_save(&self, path: &str) -> Vec<SessionResponse> {
        if self.vpl_buffer.is_empty() {
            return vec![SessionResponse::Error {
                message: "Nothing to save — no VPL declarations in the current session".into(),
            }];
        }
        match std::fs::write(path, &self.vpl_buffer) {
            Ok(()) => {
                let lines = self.vpl_buffer.lines().count();
                vec![SessionResponse::Saved {
                    path: path.to_string(),
                    lines,
                }]
            }
            Err(e) => vec![SessionResponse::Error {
                message: format!("Failed to save to '{path}': {e}"),
            }],
        }
    }

    fn handle_get_source(&self) -> Vec<SessionResponse> {
        vec![SessionResponse::Source {
            vpl: self.vpl_buffer.clone(),
        }]
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Drain output events from the engine channel and trace entries,
    /// returning them as session responses. Also broadcasts output events
    /// to subscribed listeners.
    fn drain_outputs_and_trace(&mut self) -> Vec<SessionResponse> {
        let mut responses = Vec::new();

        // Drain output events
        while let Ok(event) = self.output_rx.try_recv() {
            let stream_name = event.event_type.to_string();

            // Apply subscription filter
            if let Some(subs) = &self.subscribed_streams {
                if !subs.contains(&stream_name) {
                    continue;
                }
            }

            let resp = event_to_output_response(&event);
            // Send to broadcast subscribers
            let _ = self.broadcast_tx.send(resp.clone());
            responses.push(resp);
        }

        // Note: stream chaining (e.g., `HotDevices as h -> LargePayments as p`)
        // is handled by the engine's internal routing — output events from one
        // stream are automatically available as input to sequence patterns that
        // reference that stream name.

        // Drain trace entries
        let trace_entries = self.engine.drain_trace();
        if !trace_entries.is_empty() {
            let json_entries: Vec<TraceEntryJson> =
                trace_entries.into_iter().map(trace_to_json).collect();
            let trace_resp = SessionResponse::Trace {
                entries: json_entries,
            };
            let _ = self.broadcast_tx.send(trace_resp.clone());
            responses.push(trace_resp);
        }

        responses
    }
}

// =============================================================================
// Conversion helpers
// =============================================================================

/// Convert a JSON object into a runtime [`Event`].
fn json_to_event(event_type: &str, data: &serde_json::Value) -> Event {
    let mut event = Event::new(event_type);
    if let serde_json::Value::Object(map) = data {
        for (key, value) in map {
            if let Some(v) = crate::connector::helpers::json_to_value(value) {
                event.data.insert(Arc::from(key.as_str()), v);
            }
        }
    }
    event
}

/// Convert a [`GeneratedEvent`] into a runtime [`Event`].
#[cfg(feature = "interactive")]
fn generated_to_event(gen: &varpulis_datagen::GeneratedEvent) -> Event {
    let mut data = crate::event::FxIndexMap::default();
    for (key, value) in &gen.fields {
        if let Some(v) = crate::connector::helpers::json_to_value(value) {
            data.insert(Arc::from(key.as_str()), v);
        }
    }
    Event {
        event_type: Arc::from(gen.event_type.as_str()),
        timestamp: gen.timestamp,
        data,
    }
}

/// Convert a runtime [`Event`] to a [`SessionResponse::Output`].
fn event_to_output_response(event: &Event) -> SessionResponse {
    let mut fields = serde_json::Map::new();
    for (key, value) in &event.data {
        fields.insert(key.to_string(), value_to_json(value));
    }

    SessionResponse::Output {
        stream: event.event_type.to_string(),
        event: serde_json::Value::Object(fields),
        timestamp: event.timestamp.to_rfc3339(),
    }
}

/// Convert a [`varpulis_core::Value`] to a [`serde_json::Value`].
fn value_to_json(value: &varpulis_core::Value) -> serde_json::Value {
    match value {
        varpulis_core::Value::Null => serde_json::Value::Null,
        varpulis_core::Value::Bool(b) => serde_json::Value::Bool(*b),
        varpulis_core::Value::Int(n) => serde_json::Value::Number((*n).into()),
        varpulis_core::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        varpulis_core::Value::Str(s) => serde_json::Value::String(s.to_string()),
        varpulis_core::Value::Timestamp(nanos) => {
            // Nanoseconds since epoch → RFC 3339 string
            let secs = nanos / 1_000_000_000;
            let nsecs = (*nanos % 1_000_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs).unwrap_or_default();
            serde_json::Value::String(dt.to_rfc3339())
        }
        varpulis_core::Value::Duration(nanos) => {
            let ms = nanos / 1_000_000;
            serde_json::Value::String(format!("{ms}ms"))
        }
        varpulis_core::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
        varpulis_core::Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.to_string(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

/// Convert a [`TraceEntry`] to JSON-friendly [`TraceEntryJson`].
fn trace_to_json(entry: TraceEntry) -> TraceEntryJson {
    match entry {
        TraceEntry::StreamMatched {
            stream_name,
            event_type,
        } => TraceEntryJson {
            kind: "stream_matched".into(),
            stream: Some(stream_name),
            detail: Some(format!("event_type={event_type}")),
        },
        TraceEntry::OperatorResult {
            stream_name,
            op_name,
            passed,
            detail,
        } => TraceEntryJson {
            kind: "operator_result".into(),
            stream: Some(stream_name),
            detail: Some(format!(
                "op={op_name} passed={passed}{}",
                detail.map(|d| format!(" {d}")).unwrap_or_default()
            )),
        },
        TraceEntry::PatternState {
            stream_name,
            active_runs,
            completed,
        } => TraceEntryJson {
            kind: "pattern_state".into(),
            stream: Some(stream_name),
            detail: Some(format!("active_runs={active_runs} completed={completed}")),
        },
        TraceEntry::EventEmitted {
            stream_name,
            fields,
        } => TraceEntryJson {
            kind: "event_emitted".into(),
            stream: Some(stream_name),
            detail: Some(
                fields
                    .into_iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        },
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_session_starts_without_program() {
        let session = InteractiveSession::new();
        assert!(!session.program_loaded);
        assert!(session.program.is_none());
        assert!(session.subscribed_streams.is_none());
    }

    #[test]
    fn test_load_vpl_valid() {
        let mut session = InteractiveSession::new();
        let responses = session.handle_command(SessionCommand::LoadVpl {
            vpl: "stream HighTemp = Temperature .where(temp > 100) .emit(t: temp)".into(),
        });
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            SessionResponse::Loaded {
                streams,
                added,
                removed,
                preserved,
            } => {
                assert!(streams.contains(&"HighTemp".to_string()));
                assert!(added.contains(&"HighTemp".to_string()));
                assert!(removed.is_empty());
                assert!(preserved.is_empty());
            }
            other => panic!("Expected Loaded, got {other:?}"),
        }
        assert!(session.program_loaded);
    }

    #[test]
    fn test_load_vpl_invalid_returns_error() {
        let mut session = InteractiveSession::new();
        let responses = session.handle_command(SessionCommand::LoadVpl {
            vpl: "this is not valid vpl !!!".into(),
        });
        assert_eq!(responses.len(), 1);
        assert!(
            matches!(&responses[0], SessionResponse::Error { message } if message.contains("rror"))
        );
    }

    #[test]
    fn test_inject_produces_output() {
        let mut session = InteractiveSession::new();

        // Load a simple pipeline that echoes events
        let responses = session.handle_command(SessionCommand::LoadVpl {
            vpl: "stream Echo = Tick .emit(price: price)".into(),
        });
        assert!(matches!(&responses[0], SessionResponse::Loaded { .. }));

        // Inject a matching event
        let responses = session.handle_command(SessionCommand::Inject {
            event_type: "Tick".into(),
            data: serde_json::json!({"price": 42.5}),
        });

        // Should have at least one Output response
        let has_output = responses
            .iter()
            .any(|r| matches!(r, SessionResponse::Output { .. }));
        assert!(has_output, "Expected Output response, got: {responses:?}");
    }

    #[test]
    fn test_inject_without_program_returns_error() {
        let mut session = InteractiveSession::new();
        let responses = session.handle_command(SessionCommand::Inject {
            event_type: "Tick".into(),
            data: serde_json::json!({"price": 42.5}),
        });
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], SessionResponse::Error { .. }));
    }

    #[test]
    fn test_get_topology_returns_graph() {
        let mut session = InteractiveSession::new();

        // No program loaded
        let responses = session.handle_command(SessionCommand::GetTopology);
        assert!(matches!(&responses[0], SessionResponse::Error { .. }));

        // Load a program
        session.handle_command(SessionCommand::LoadVpl {
            vpl: "stream A = Tick .where(x > 1) .emit(v: x)".into(),
        });

        let responses = session.handle_command(SessionCommand::GetTopology);
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            SessionResponse::Topology { nodes, edges } => {
                assert!(!nodes.is_empty());
                assert!(!edges.is_empty());
            }
            other => panic!("Expected Topology, got {other:?}"),
        }
    }

    #[test]
    fn test_set_trace_toggle() {
        let mut session = InteractiveSession::new();

        // Enable trace — returns no responses
        let responses = session.handle_command(SessionCommand::SetTrace { enabled: true });
        assert!(responses.is_empty());

        // Disable trace
        let responses = session.handle_command(SessionCommand::SetTrace { enabled: false });
        assert!(responses.is_empty());
    }

    #[test]
    fn test_get_streams_empty_then_loaded() {
        let mut session = InteractiveSession::new();

        // Empty engine
        let responses = session.handle_command(SessionCommand::GetStreams);
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            SessionResponse::Streams { streams } => {
                assert!(streams.is_empty());
            }
            other => panic!("Expected Streams, got {other:?}"),
        }

        // Load a program
        session.handle_command(SessionCommand::LoadVpl {
            vpl: "stream A = Tick .emit(v: x)\nstream B = Tock .emit(v: y)".into(),
        });

        let responses = session.handle_command(SessionCommand::GetStreams);
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            SessionResponse::Streams { streams } => {
                assert_eq!(streams.len(), 2);
                let names: HashSet<String> = streams.iter().map(|s| s.name.clone()).collect();
                assert!(names.contains("A"));
                assert!(names.contains("B"));
            }
            other => panic!("Expected Streams, got {other:?}"),
        }
    }

    #[test]
    fn test_subscribe_filters_streams() {
        let mut session = InteractiveSession::new();

        // Default: subscribed to all
        assert!(session.subscribed_streams.is_none());

        // Subscribe to specific stream
        session.handle_command(SessionCommand::Subscribe {
            stream: Some("Alerts".into()),
        });
        assert!(session
            .subscribed_streams
            .as_ref()
            .unwrap()
            .contains("Alerts"));

        // Subscribe to all again
        session.handle_command(SessionCommand::Subscribe { stream: None });
        assert!(session.subscribed_streams.is_none());

        // Unsubscribe from all
        session.handle_command(SessionCommand::Unsubscribe { stream: None });
        assert!(session.subscribed_streams.as_ref().unwrap().is_empty());
    }

    #[test]
    fn test_get_metrics() {
        let mut session = InteractiveSession::new();
        let responses = session.handle_command(SessionCommand::GetMetrics);
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            SessionResponse::Metrics {
                events_processed,
                output_events,
                streams_count,
            } => {
                assert_eq!(*events_processed, 0);
                assert_eq!(*output_events, 0);
                assert_eq!(*streams_count, 0);
            }
            other => panic!("Expected Metrics, got {other:?}"),
        }
    }

    #[test]
    fn test_quit_returns_bye() {
        let mut session = InteractiveSession::new();
        let responses = session.handle_command(SessionCommand::Quit);
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], SessionResponse::Bye));
    }

    #[test]
    fn test_stop_generate_without_running() {
        let mut session = InteractiveSession::new();
        let responses = session.handle_command(SessionCommand::StopGenerate);
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], SessionResponse::Error { .. }));
    }

    #[cfg(not(feature = "interactive"))]
    #[test]
    fn test_generate_without_feature_returns_error() {
        let mut session = InteractiveSession::new();
        session.handle_command(SessionCommand::LoadVpl {
            vpl: "stream A = Tick .emit(v: x)".into(),
        });
        let responses = session.handle_command(SessionCommand::Generate {
            schema: "fraud".into(),
            rate: 100,
            duration: Some(1),
        });
        assert_eq!(responses.len(), 1);
        assert!(
            matches!(&responses[0], SessionResponse::Error { message } if message.contains("interactive"))
        );
    }

    #[test]
    fn test_load_file_nonexistent() {
        let mut session = InteractiveSession::new();
        let responses = session.handle_command(SessionCommand::LoadFile {
            path: "/nonexistent/path.vpl".into(),
        });
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], SessionResponse::Error { .. }));
    }

    #[test]
    fn test_inject_file_nonexistent() {
        let mut session = InteractiveSession::new();
        session.handle_command(SessionCommand::LoadVpl {
            vpl: "stream A = Tick .emit(v: x)".into(),
        });
        let responses = session.handle_command(SessionCommand::InjectFile {
            path: "/nonexistent/events.evt".into(),
        });
        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], SessionResponse::Error { .. }));
    }

    #[test]
    fn test_reload_reports_changes() {
        let mut session = InteractiveSession::new();

        // First load
        session.handle_command(SessionCommand::LoadVpl {
            vpl: "stream A = Tick .emit(v: x)".into(),
        });
        assert!(session.program_loaded);

        // Reload with different streams
        let responses = session.handle_command(SessionCommand::LoadVpl {
            vpl: "stream B = Tock .emit(v: y)".into(),
        });
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            SessionResponse::Loaded {
                streams,
                added,
                removed,
                ..
            } => {
                assert!(streams.contains(&"B".to_string()));
                assert!(added.contains(&"B".to_string()));
                assert!(removed.contains(&"A".to_string()));
            }
            other => panic!("Expected Loaded, got {other:?}"),
        }
    }
}
