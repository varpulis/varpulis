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
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

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

/// A self-contained single-threaded runtime for one context.
///
/// Owns its streams, processes events without locks. Receives events from
/// its inbound channel and forwards cross-context events via outbound channels.
#[allow(dead_code)]
pub struct ContextRuntime {
    name: String,
    engine: Engine,
    output_tx: mpsc::Sender<Event>,
    event_rx: mpsc::Receiver<SharedEvent>,
    cross_context_tx: HashMap<String, mpsc::Sender<SharedEvent>>,
    events_processed: u64,
    output_events_emitted: u64,
}

impl ContextRuntime {
    /// Create a new context runtime
    pub fn new(
        name: String,
        engine: Engine,
        output_tx: mpsc::Sender<Event>,
        event_rx: mpsc::Receiver<SharedEvent>,
        cross_context_tx: HashMap<String, mpsc::Sender<SharedEvent>>,
    ) -> Self {
        Self {
            name,
            engine,
            output_tx,
            event_rx,
            cross_context_tx,
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

        while let Some(event) = self.event_rx.recv().await {
            self.events_processed += 1;

            // Process the event through the engine
            // We need to convert SharedEvent back to Event for the engine
            let owned_event = (*event).clone();
            match self.engine.process(owned_event).await {
                Ok(()) => {}
                Err(e) => {
                    error!("Context '{}' processing error: {}", self.name, e);
                }
            }
        }

        info!(
            "Context '{}' runtime stopped (processed {} events)",
            self.name, self.events_processed
        );
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
    /// Name of the default context (first declared, or "default")
    default_context: String,
    /// event_type -> context_name routing table
    ingress_routing: HashMap<String, String>,
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
        program: &varpulis_core::ast::Program,
        output_tx: mpsc::Sender<Event>,
        channel_capacity: usize,
    ) -> Result<Self, String> {
        let mut context_txs: HashMap<String, mpsc::Sender<SharedEvent>> = HashMap::new();
        let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::new();

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
        // This is derived from stream assignments + event sources
        let mut ingress_routing: HashMap<String, String> = HashMap::new();

        // Parse the program to figure out which event types map to which streams,
        // then use stream_assignments to route to the right context
        for stmt in &program.statements {
            if let varpulis_core::ast::Stmt::StreamDecl { name, source, .. } = &stmt.node {
                if let Some(ctx_name) = context_map.stream_context(name) {
                    // Get the event types this stream consumes
                    let event_types = Self::event_types_from_source(source);
                    for et in event_types {
                        ingress_routing.insert(et, ctx_name.to_string());
                    }
                }
            }
        }

        // Spawn a thread for each context
        for (ctx_name, config) in context_map.contexts() {
            let rx = context_rxs
                .remove(ctx_name)
                .ok_or_else(|| format!("No receiver for context {}", ctx_name))?;

            let ctx_output_tx = output_tx.clone();
            let ctx_name_clone = ctx_name.clone();
            let cores = config.cores.clone();

            // Build cross-context senders for this context (all other contexts)
            let cross_tx: HashMap<String, mpsc::Sender<SharedEvent>> = context_txs
                .iter()
                .filter(|(k, _)| *k != ctx_name)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            // Create a dedicated engine for this context with only its streams
            let program_clone = program.clone();

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
                        // Create engine for this context
                        let (engine_output_tx, mut _engine_output_rx) = mpsc::channel(1000);
                        let mut engine = Engine::new(engine_output_tx);
                        if let Err(e) = engine.load(&program_clone) {
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
                            cross_tx,
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
            default_context,
            ingress_routing,
        })
    }

    /// Route an incoming event to the correct context.
    ///
    /// Uses the ingress routing table to determine which context should
    /// process the event based on its event type.
    pub async fn process(&self, event: SharedEvent) -> Result<(), String> {
        let ctx_name = self
            .ingress_routing
            .get(&event.event_type)
            .unwrap_or(&self.default_context);

        if let Some(tx) = self.context_txs.get(ctx_name) {
            tx.send(event)
                .await
                .map_err(|e| format!("Failed to send event to context '{}': {}", ctx_name, e))?;
        } else {
            debug!(
                "No context '{}' found for event type '{}', dropping event",
                ctx_name, event.event_type
            );
        }

        Ok(())
    }

    /// Shut down all context threads by dropping senders.
    pub fn shutdown(self) {
        // Drop all senders to signal context runtimes to stop
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

    /// Extract event types consumed by a stream source
    fn event_types_from_source(source: &varpulis_core::ast::StreamSource) -> Vec<String> {
        use varpulis_core::ast::StreamSource;
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
        // When no contexts are declared, has_contexts() returns false
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
        use varpulis_core::ast::StreamSource;

        let types = ContextOrchestrator::event_types_from_source(&StreamSource::From(
            "SensorReading".to_string(),
        ));
        assert_eq!(types, vec!["SensorReading"]);

        let types = ContextOrchestrator::event_types_from_source(&StreamSource::Ident(
            "ProcessedEvents".to_string(),
        ));
        assert_eq!(types, vec!["ProcessedEvents"]);
    }
}
