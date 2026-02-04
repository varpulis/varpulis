//! Context-based multi-threaded execution architecture.
//!
//! Named contexts provide isolated execution domains. Each context runs on its own
//! OS thread with a single-threaded Tokio runtime, enabling true parallelism without
//! locks within a context. Cross-context communication uses bounded `mpsc` channels.
//!
//! When no contexts are declared, the engine runs in single-threaded mode with zero
//! overhead (backward compatible).

use crate::engine::Engine;
use crate::event::{Event, SharedEvent};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};
use varpulis_core::ast::{Program, Stmt, StreamSource};

/// Configuration for a named context
#[derive(Debug, Clone)]
pub struct ContextConfig {
    pub name: String,
    pub cores: Option<Vec<usize>>,
}

/// Maps streams/connectors to their assigned context.
///
/// Built during `Engine::load()` by processing `ContextDecl` statements
/// and `StreamOp::Context` / `Emit { target_context }` operations.
#[derive(Debug, Clone, Default)]
pub struct ContextMap {
    /// context_name -> config
    contexts: HashMap<String, ContextConfig>,
    /// stream_name -> context_name
    stream_assignments: HashMap<String, String>,
    /// (stream_name, emit_index) -> target_context for cross-context emits
    cross_context_emits: HashMap<(String, usize), String>,
}

impl ContextMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a context declaration
    pub fn register_context(&mut self, config: ContextConfig) {
        self.contexts.insert(config.name.clone(), config);
    }

    /// Assign a stream to a context
    pub fn assign_stream(&mut self, stream_name: String, context_name: String) {
        self.stream_assignments.insert(stream_name, context_name);
    }

    /// Record a cross-context emit
    pub fn add_cross_context_emit(
        &mut self,
        stream_name: String,
        emit_index: usize,
        target_context: String,
    ) {
        self.cross_context_emits
            .insert((stream_name, emit_index), target_context);
    }

    /// Check if any contexts have been declared
    pub fn has_contexts(&self) -> bool {
        !self.contexts.is_empty()
    }

    /// Get all declared contexts
    pub fn contexts(&self) -> &HashMap<String, ContextConfig> {
        &self.contexts
    }

    /// Get the context assignment for a stream
    pub fn stream_context(&self, stream_name: &str) -> Option<&str> {
        self.stream_assignments.get(stream_name).map(|s| s.as_str())
    }

    /// Get all stream assignments
    pub fn stream_assignments(&self) -> &HashMap<String, String> {
        &self.stream_assignments
    }

    /// Get all cross-context emits
    pub fn cross_context_emits(&self) -> &HashMap<(String, usize), String> {
        &self.cross_context_emits
    }
}

/// Filter a program to keep only the streams assigned to a specific context.
///
/// Retains all non-stream statements (ContextDecl, ConnectorDecl, VarDecl,
/// Assignment, FnDecl, EventDecl, PatternDecl, Config) since they may be
/// needed by any context. Only `StreamDecl` statements are filtered based
/// on context assignment.
pub fn filter_program_for_context(
    program: &Program,
    context_name: &str,
    context_map: &ContextMap,
) -> Program {
    let filtered_statements = program
        .statements
        .iter()
        .filter(|stmt| {
            if let Stmt::StreamDecl { name, .. } = &stmt.node {
                // Keep the stream only if it's assigned to this context
                match context_map.stream_context(name) {
                    Some(ctx) => ctx == context_name,
                    // Unassigned streams are kept in all contexts for backward compat
                    None => true,
                }
            } else {
                // Keep all non-stream statements
                true
            }
        })
        .cloned()
        .collect();

    Program {
        statements: filtered_statements,
    }
}

/// Verify the CPU affinity of the current thread by reading /proc/self/status.
///
/// Returns the list of CPU cores the current thread is allowed to run on,
/// or `None` if the information cannot be read.
#[cfg(target_os = "linux")]
pub fn verify_cpu_affinity() -> Option<Vec<usize>> {
    use std::fs;

    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if line.starts_with("Cpus_allowed_list:") {
            let list_str = line.split(':').nth(1)?.trim();
            let mut cores = Vec::new();
            for part in list_str.split(',') {
                let part = part.trim();
                if let Some((start, end)) = part.split_once('-') {
                    if let (Ok(s), Ok(e)) = (start.parse::<usize>(), end.parse::<usize>()) {
                        cores.extend(s..=e);
                    }
                } else if let Ok(core) = part.parse::<usize>() {
                    cores.push(core);
                }
            }
            return Some(cores);
        }
    }
    None
}

/// A self-contained single-threaded runtime for one context.
///
/// Owns its streams, processes events without locks. Receives events from
/// its inbound channel and forwards cross-context events via outbound channels.
pub struct ContextRuntime {
    name: String,
    engine: Engine,
    /// Main output channel (tenant/CLI)
    output_tx: mpsc::Sender<Event>,
    /// Inbound events from orchestrator
    event_rx: mpsc::Receiver<SharedEvent>,
    /// Engine's emitted events receiver
    engine_output_rx: mpsc::Receiver<Event>,
    /// Senders to all contexts (including self, for intra-context derived streams)
    all_context_txs: HashMap<String, mpsc::Sender<SharedEvent>>,
    /// event_type → context_name routing table
    ingress_routing: HashMap<String, String>,
    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,
    events_processed: u64,
    output_events_emitted: u64,
}

impl ContextRuntime {
    /// Create a new context runtime
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        engine: Engine,
        output_tx: mpsc::Sender<Event>,
        event_rx: mpsc::Receiver<SharedEvent>,
        engine_output_rx: mpsc::Receiver<Event>,
        all_context_txs: HashMap<String, mpsc::Sender<SharedEvent>>,
        ingress_routing: HashMap<String, String>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            name,
            engine,
            output_tx,
            event_rx,
            engine_output_rx,
            all_context_txs,
            ingress_routing,
            shutdown_rx,
            events_processed: 0,
            output_events_emitted: 0,
        }
    }

    /// Run the event loop. Blocks the current thread.
    ///
    /// Receives events from the inbound channel, processes them through
    /// the engine, and forwards cross-context events as needed.
    pub async fn run(&mut self) {
        info!("Context '{}' runtime started", self.name);

        #[cfg(target_os = "linux")]
        if let Some(cores) = verify_cpu_affinity() {
            info!("Context '{}' running on cores {:?}", self.name, cores);
        }

        loop {
            tokio::select! {
                biased;

                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!("Context '{}' received shutdown signal", self.name);
                        break;
                    }
                }

                event = self.event_rx.recv() => {
                    match event {
                        Some(event) => {
                            self.events_processed += 1;

                            // Process the event through the engine (zero-copy via SharedEvent)
                            match self.engine.process_shared(Arc::clone(&event)).await {
                                Ok(()) => {}
                                Err(e) => {
                                    error!("Context '{}' processing error: {}", self.name, e);
                                }
                            }

                            // Drain engine output events and route them
                            while let Ok(output_event) = self.engine_output_rx.try_recv() {
                                self.output_events_emitted += 1;

                                // Route to consuming context if any
                                if let Some(target_ctx) = self.ingress_routing.get(&output_event.event_type) {
                                    if let Some(tx) = self.all_context_txs.get(target_ctx) {
                                        let _ = tx.try_send(Arc::new(output_event.clone()));
                                    }
                                }

                                // Always forward to main output channel
                                let _ = self.output_tx.try_send(output_event);
                            }
                        }
                        None => {
                            // Channel closed
                            break;
                        }
                    }
                }
            }
        }

        // Drop cross-context senders so other contexts can shut down too
        self.all_context_txs.clear();

        info!(
            "Context '{}' runtime stopped (processed {} events, emitted {} output events)",
            self.name, self.events_processed, self.output_events_emitted
        );
    }
}

/// Direct event-type-to-channel router for non-blocking dispatch.
///
/// Maps `event_type → Sender<SharedEvent>` directly (single HashMap lookup),
/// uses `try_send()` for non-blocking dispatch, and is cheaply cloneable
/// via `Arc<HashMap>` for multi-producer scenarios.
#[derive(Clone)]
pub struct EventTypeRouter {
    routes: Arc<HashMap<String, mpsc::Sender<SharedEvent>>>,
    default_tx: mpsc::Sender<SharedEvent>,
}

/// Errors returned by non-blocking dispatch methods.
pub enum DispatchError {
    /// Channel is full — caller should retry or use async dispatch
    ChannelFull(SharedEvent),
    /// Channel is closed — context has shut down
    ChannelClosed(SharedEvent),
}

impl EventTypeRouter {
    /// Non-blocking dispatch via `try_send()`.
    ///
    /// Routes the event to the correct context channel based on event type.
    /// Returns immediately without waiting for channel capacity.
    pub fn dispatch(&self, event: SharedEvent) -> Result<(), DispatchError> {
        let tx = self
            .routes
            .get(&event.event_type)
            .unwrap_or(&self.default_tx);
        match tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(event)) => Err(DispatchError::ChannelFull(event)),
            Err(mpsc::error::TrySendError::Closed(event)) => {
                Err(DispatchError::ChannelClosed(event))
            }
        }
    }

    /// Blocking dispatch via `send().await`.
    ///
    /// Waits for channel capacity if the channel is full.
    pub async fn dispatch_await(&self, event: SharedEvent) -> Result<(), String> {
        let event_type = event.event_type.clone();
        let tx = self.routes.get(&event_type).unwrap_or(&self.default_tx);
        tx.send(event)
            .await
            .map_err(|e| format!("Failed to send event type '{}': {}", event_type, e))
    }

    /// Batch dispatch — non-blocking, returns errors for any events that could not be sent.
    pub fn dispatch_batch(&self, events: Vec<SharedEvent>) -> Vec<DispatchError> {
        let mut errors = Vec::new();
        for event in events {
            if let Err(e) = self.dispatch(event) {
                errors.push(e);
            }
        }
        errors
    }
}

/// Orchestrates multiple ContextRuntimes across OS threads.
///
/// Routes incoming events to the correct context based on event type
/// and stream assignments.
pub struct ContextOrchestrator {
    /// Senders to each context's event channel
    context_txs: HashMap<String, mpsc::Sender<SharedEvent>>,
    /// Thread handles for each context
    handles: Vec<std::thread::JoinHandle<()>>,
    /// event_type -> context_name routing table
    ingress_routing: HashMap<String, String>,
    /// Shutdown signal sender
    shutdown_tx: watch::Sender<bool>,
    /// Direct event-type-to-channel router
    router: EventTypeRouter,
}

impl ContextOrchestrator {
    /// Build the orchestrator from engine state.
    ///
    /// For each declared context:
    /// 1. Creates a bounded mpsc channel
    /// 2. Creates an Engine with only the streams assigned to that context
    /// 3. Spawns an OS thread with optional CPU affinity
    /// 4. Inside the thread: creates a single-threaded Tokio runtime
    ///    and runs the ContextRuntime event loop
    pub fn build(
        context_map: &ContextMap,
        program: &Program,
        output_tx: mpsc::Sender<Event>,
        channel_capacity: usize,
    ) -> Result<Self, String> {
        let mut context_txs: HashMap<String, mpsc::Sender<SharedEvent>> = HashMap::new();
        let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::new();

        // Create shutdown signal
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        // Determine default context
        let default_context = context_map
            .contexts()
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| "default".to_string());

        // Create cross-context senders: first pass to create all channels
        let mut context_rxs: HashMap<String, mpsc::Receiver<SharedEvent>> = HashMap::new();
        for ctx_name in context_map.contexts().keys() {
            let (tx, rx) = mpsc::channel(channel_capacity);
            context_txs.insert(ctx_name.clone(), tx);
            context_rxs.insert(ctx_name.clone(), rx);
        }

        // Build ingress routing: event_type -> context_name
        let mut ingress_routing: HashMap<String, String> = HashMap::new();

        // First pass: route raw event types from stream sources to contexts
        for stmt in &program.statements {
            if let Stmt::StreamDecl { name, source, .. } = &stmt.node {
                if let Some(ctx_name) = context_map.stream_context(name) {
                    let event_types = Self::event_types_from_source(source);
                    for et in event_types {
                        ingress_routing.insert(et, ctx_name.to_string());
                    }
                }
            }
        }

        // Second pass: route derived stream output types to consuming contexts
        for stmt in &program.statements {
            if let Stmt::StreamDecl { name, source, .. } = &stmt.node {
                if let Some(ctx_name) = context_map.stream_context(name) {
                    match source {
                        StreamSource::Ident(source_stream) | StreamSource::From(source_stream) => {
                            if context_map.stream_context(source_stream).is_some() {
                                ingress_routing.insert(source_stream.clone(), ctx_name.to_string());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Third pass: validate cross-context emit targets
        for ((_stream_name, _emit_idx), target_ctx) in context_map.cross_context_emits() {
            if !context_txs.contains_key(target_ctx) {
                warn!(
                    "Cross-context emit targets unknown context '{}'",
                    target_ctx
                );
            }
        }

        // Build EventTypeRouter: event_type → Sender directly (single lookup)
        let mut event_type_txs: HashMap<String, mpsc::Sender<SharedEvent>> = HashMap::new();
        for (event_type, ctx_name) in &ingress_routing {
            if let Some(tx) = context_txs.get(ctx_name) {
                event_type_txs.insert(event_type.clone(), tx.clone());
            }
        }
        let default_tx = context_txs
            .get(&default_context)
            .cloned()
            .ok_or_else(|| format!("No channel for default context '{}'", default_context))?;
        let router = EventTypeRouter {
            routes: Arc::new(event_type_txs),
            default_tx,
        };

        // Clone context_map for use inside thread spawning
        let context_map_clone = context_map.clone();

        // Spawn a thread for each context
        for (ctx_name, config) in context_map.contexts() {
            let rx = context_rxs
                .remove(ctx_name)
                .ok_or_else(|| format!("No receiver for context {}", ctx_name))?;

            let ctx_output_tx = output_tx.clone();
            let ctx_name_clone = ctx_name.clone();
            let cores = config.cores.clone();

            // Clone all context senders for cross-context forwarding
            let all_txs: HashMap<String, mpsc::Sender<SharedEvent>> = context_txs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            // Filter the program to only include this context's streams
            let filtered_program =
                filter_program_for_context(program, ctx_name, &context_map_clone);
            let ingress_routing_clone = ingress_routing.clone();
            let shutdown_rx = shutdown_tx.subscribe();

            let handle = std::thread::Builder::new()
                .name(format!("varpulis-ctx-{}", ctx_name))
                .spawn(move || {
                    // Set CPU affinity if specified
                    if let Some(ref core_ids) = cores {
                        Self::set_cpu_affinity(&ctx_name_clone, core_ids);
                    }

                    // Create a single-threaded Tokio runtime for this context
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to create Tokio runtime for context");

                    rt.block_on(async move {
                        // Create engine for this context with filtered program
                        let (engine_output_tx, engine_output_rx) = mpsc::channel(1000);
                        let mut engine = Engine::new(engine_output_tx);
                        if let Err(e) = engine.load(&filtered_program) {
                            error!(
                                "Failed to load program for context '{}': {}",
                                ctx_name_clone, e
                            );
                            return;
                        }

                        let mut ctx_runtime = ContextRuntime::new(
                            ctx_name_clone,
                            engine,
                            ctx_output_tx,
                            rx,
                            engine_output_rx,
                            all_txs,
                            ingress_routing_clone,
                            shutdown_rx,
                        );

                        ctx_runtime.run().await;
                    });
                })
                .map_err(|e| format!("Failed to spawn context thread: {}", e))?;

            handles.push(handle);
        }

        Ok(Self {
            context_txs,
            handles,
            ingress_routing,
            shutdown_tx,
            router,
        })
    }

    /// Route an incoming event to the correct context (async, waits on backpressure).
    pub async fn process(&self, event: SharedEvent) -> Result<(), String> {
        self.router.dispatch_await(event).await
    }

    /// Non-blocking dispatch — returns `ChannelFull` instead of waiting.
    pub fn try_process(&self, event: SharedEvent) -> Result<(), DispatchError> {
        self.router.dispatch(event)
    }

    /// Batch dispatch — non-blocking, returns errors for events that could not be sent.
    pub fn process_batch(&self, events: Vec<SharedEvent>) -> Vec<DispatchError> {
        self.router.dispatch_batch(events)
    }

    /// Get a cloneable router handle for direct multi-producer dispatch.
    pub fn router(&self) -> EventTypeRouter {
        self.router.clone()
    }

    /// Shut down all context threads.
    ///
    /// Sends shutdown signal, drops senders, and waits for threads to finish.
    pub fn shutdown(self) {
        // Signal all contexts to shut down
        let _ = self.shutdown_tx.send(true);

        // Drop all senders to unblock any recv() calls
        drop(self.context_txs);

        // Wait for all threads to finish
        for handle in self.handles {
            if let Err(e) = handle.join() {
                error!("Context thread panicked: {:?}", e);
            }
        }

        info!("All context runtimes shut down");
    }

    /// Get the names of all running contexts
    pub fn context_names(&self) -> Vec<&str> {
        self.context_txs.keys().map(|s| s.as_str()).collect()
    }

    /// Get the ingress routing table (for testing/debugging)
    pub fn ingress_routing(&self) -> &HashMap<String, String> {
        &self.ingress_routing
    }

    /// Extract event types consumed by a stream source
    fn event_types_from_source(source: &StreamSource) -> Vec<String> {
        match source {
            StreamSource::From(et) => vec![et.clone()],
            StreamSource::Ident(name) => vec![name.clone()],
            StreamSource::IdentWithAlias { name, .. } => vec![name.clone()],
            StreamSource::AllWithAlias { name, .. } => vec![name.clone()],
            StreamSource::FromConnector { event_type, .. } => vec![event_type.clone()],
            StreamSource::Merge(decls) => decls.iter().map(|d| d.source.clone()).collect(),
            StreamSource::Join(clauses) => clauses.iter().map(|c| c.source.clone()).collect(),
            StreamSource::Sequence(decl) => {
                decl.steps.iter().map(|s| s.event_type.clone()).collect()
            }
            StreamSource::Timer(_) => vec![],
        }
    }

    /// Set CPU affinity for the current thread
    fn set_cpu_affinity(ctx_name: &str, core_ids: &[usize]) {
        #[cfg(target_os = "linux")]
        {
            use core_affinity::CoreId;
            if let Some(&first_core) = core_ids.first() {
                let core_id = CoreId { id: first_core };
                if core_affinity::set_for_current(core_id) {
                    info!("Context '{}' pinned to core {}", ctx_name, first_core);
                } else {
                    warn!(
                        "Failed to pin context '{}' to core {}",
                        ctx_name, first_core
                    );
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            debug!(
                "CPU affinity not supported on this platform for context '{}' (cores: {:?})",
                ctx_name, core_ids
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_map_new() {
        let map = ContextMap::new();
        assert!(!map.has_contexts());
        assert!(map.contexts().is_empty());
    }

    #[test]
    fn test_context_map_register() {
        let mut map = ContextMap::new();
        map.register_context(ContextConfig {
            name: "ingestion".to_string(),
            cores: Some(vec![0, 1]),
        });
        assert!(map.has_contexts());
        assert_eq!(map.contexts().len(), 1);
        let config = map.contexts().get("ingestion").unwrap();
        assert_eq!(config.cores, Some(vec![0, 1]));
    }

    #[test]
    fn test_context_map_stream_assignment() {
        let mut map = ContextMap::new();
        map.register_context(ContextConfig {
            name: "fast".to_string(),
            cores: None,
        });
        map.assign_stream("RawEvents".to_string(), "fast".to_string());
        assert_eq!(map.stream_context("RawEvents"), Some("fast"));
        assert_eq!(map.stream_context("Unknown"), None);
    }

    #[test]
    fn test_context_map_cross_context_emit() {
        let mut map = ContextMap::new();
        map.register_context(ContextConfig {
            name: "analytics".to_string(),
            cores: None,
        });
        map.add_cross_context_emit("Alerts".to_string(), 0, "analytics".to_string());
        let emits = map.cross_context_emits();
        assert_eq!(
            emits.get(&("Alerts".to_string(), 0)),
            Some(&"analytics".to_string())
        );
    }

    #[test]
    fn test_no_context_backward_compat() {
        let map = ContextMap::new();
        assert!(!map.has_contexts());
    }

    #[test]
    fn test_context_config_no_cores() {
        let config = ContextConfig {
            name: "test".to_string(),
            cores: None,
        };
        assert_eq!(config.name, "test");
        assert!(config.cores.is_none());
    }

    #[test]
    fn test_context_map_multiple_contexts() {
        let mut map = ContextMap::new();
        map.register_context(ContextConfig {
            name: "ingestion".to_string(),
            cores: Some(vec![0, 1]),
        });
        map.register_context(ContextConfig {
            name: "analytics".to_string(),
            cores: Some(vec![2, 3]),
        });
        map.register_context(ContextConfig {
            name: "alerts".to_string(),
            cores: Some(vec![4]),
        });

        assert_eq!(map.contexts().len(), 3);

        map.assign_stream("RawEvents".to_string(), "ingestion".to_string());
        map.assign_stream("Analysis".to_string(), "analytics".to_string());
        map.assign_stream("Notifications".to_string(), "alerts".to_string());

        assert_eq!(map.stream_context("RawEvents"), Some("ingestion"));
        assert_eq!(map.stream_context("Analysis"), Some("analytics"));
        assert_eq!(map.stream_context("Notifications"), Some("alerts"));
    }

    #[test]
    fn test_context_orchestrator_event_types_from_source() {
        let types = ContextOrchestrator::event_types_from_source(&StreamSource::From(
            "SensorReading".to_string(),
        ));
        assert_eq!(types, vec!["SensorReading"]);

        let types = ContextOrchestrator::event_types_from_source(&StreamSource::Ident(
            "ProcessedEvents".to_string(),
        ));
        assert_eq!(types, vec!["ProcessedEvents"]);
    }

    #[test]
    fn test_filter_program_for_context() {
        use varpulis_core::span::Spanned;

        let program = Program {
            statements: vec![
                Spanned {
                    node: Stmt::ContextDecl {
                        name: "ctx1".to_string(),
                        cores: None,
                    },
                    span: varpulis_core::span::Span::dummy(),
                },
                Spanned {
                    node: Stmt::ContextDecl {
                        name: "ctx2".to_string(),
                        cores: None,
                    },
                    span: varpulis_core::span::Span::dummy(),
                },
                Spanned {
                    node: Stmt::StreamDecl {
                        name: "StreamA".to_string(),
                        type_annotation: None,
                        source: StreamSource::From("EventA".to_string()),
                        ops: vec![],
                    },
                    span: varpulis_core::span::Span::dummy(),
                },
                Spanned {
                    node: Stmt::StreamDecl {
                        name: "StreamB".to_string(),
                        type_annotation: None,
                        source: StreamSource::From("EventB".to_string()),
                        ops: vec![],
                    },
                    span: varpulis_core::span::Span::dummy(),
                },
            ],
        };

        let mut context_map = ContextMap::new();
        context_map.register_context(ContextConfig {
            name: "ctx1".to_string(),
            cores: None,
        });
        context_map.register_context(ContextConfig {
            name: "ctx2".to_string(),
            cores: None,
        });
        context_map.assign_stream("StreamA".to_string(), "ctx1".to_string());
        context_map.assign_stream("StreamB".to_string(), "ctx2".to_string());

        let filtered = filter_program_for_context(&program, "ctx1", &context_map);

        let stream_count = filtered
            .statements
            .iter()
            .filter(|s| matches!(s.node, Stmt::StreamDecl { .. }))
            .count();
        assert_eq!(stream_count, 1, "ctx1 should have exactly 1 stream");

        let has_stream_a = filtered
            .statements
            .iter()
            .any(|s| matches!(&s.node, Stmt::StreamDecl { name, .. } if name == "StreamA"));
        assert!(has_stream_a, "ctx1 should contain StreamA");

        let has_stream_b = filtered
            .statements
            .iter()
            .any(|s| matches!(&s.node, Stmt::StreamDecl { name, .. } if name == "StreamB"));
        assert!(!has_stream_b, "ctx1 should NOT contain StreamB");

        let context_decl_count = filtered
            .statements
            .iter()
            .filter(|s| matches!(s.node, Stmt::ContextDecl { .. }))
            .count();
        assert_eq!(
            context_decl_count, 2,
            "All ContextDecls should be preserved"
        );
    }

    #[test]
    fn test_ingress_routing_includes_derived_types() {
        use varpulis_core::span::Spanned;

        let program = Program {
            statements: vec![
                Spanned {
                    node: Stmt::ContextDecl {
                        name: "ingest".to_string(),
                        cores: None,
                    },
                    span: varpulis_core::span::Span::dummy(),
                },
                Spanned {
                    node: Stmt::ContextDecl {
                        name: "analytics".to_string(),
                        cores: None,
                    },
                    span: varpulis_core::span::Span::dummy(),
                },
                Spanned {
                    node: Stmt::StreamDecl {
                        name: "RawData".to_string(),
                        type_annotation: None,
                        source: StreamSource::From("SensorReading".to_string()),
                        ops: vec![],
                    },
                    span: varpulis_core::span::Span::dummy(),
                },
                Spanned {
                    node: Stmt::StreamDecl {
                        name: "Analysis".to_string(),
                        type_annotation: None,
                        source: StreamSource::Ident("RawData".to_string()),
                        ops: vec![],
                    },
                    span: varpulis_core::span::Span::dummy(),
                },
            ],
        };

        let mut context_map = ContextMap::new();
        context_map.register_context(ContextConfig {
            name: "ingest".to_string(),
            cores: None,
        });
        context_map.register_context(ContextConfig {
            name: "analytics".to_string(),
            cores: None,
        });
        context_map.assign_stream("RawData".to_string(), "ingest".to_string());
        context_map.assign_stream("Analysis".to_string(), "analytics".to_string());

        let (output_tx, _output_rx) = mpsc::channel(10);
        let orchestrator =
            ContextOrchestrator::build(&context_map, &program, output_tx, 100).unwrap();

        let routing = orchestrator.ingress_routing();

        assert_eq!(routing.get("SensorReading"), Some(&"ingest".to_string()));
        assert_eq!(routing.get("RawData"), Some(&"analytics".to_string()));

        orchestrator.shutdown();
    }

    #[test]
    fn test_ingress_routing_cross_context_emits() {
        let mut context_map = ContextMap::new();
        context_map.register_context(ContextConfig {
            name: "ingest".to_string(),
            cores: None,
        });
        context_map.register_context(ContextConfig {
            name: "analytics".to_string(),
            cores: None,
        });
        context_map.assign_stream("RawData".to_string(), "ingest".to_string());
        context_map.add_cross_context_emit("RawData".to_string(), 0, "analytics".to_string());

        let emits = context_map.cross_context_emits();
        assert_eq!(
            emits.get(&("RawData".to_string(), 0)),
            Some(&"analytics".to_string())
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_cpu_affinity_verification() {
        let cores = verify_cpu_affinity();
        assert!(cores.is_some(), "Should be able to read CPU affinity");
        let cores = cores.unwrap();
        assert!(!cores.is_empty(), "Should have at least one allowed core");
    }
}
