//! Builder pattern for ergonomic `Engine` construction.
//!
//! # Examples
//!
//! ```rust,no_run
//! use varpulis_runtime::Engine;
//! use tokio::sync::mpsc;
//!
//! let (tx, _rx) = mpsc::channel(100);
//! let engine = Engine::builder()
//!     .output(tx)
//!     .build();
//! ```

use std::sync::Arc;

use tokio::sync::mpsc;

use super::{Engine, OutputChannel};
use crate::connector;
use crate::dead_letter::DlqConfig;
use crate::event::{Event, SharedEvent};
use crate::metrics::Metrics;
use crate::udf::UdfRegistry;

/// Builder for constructing an [`Engine`] with a fluent API.
///
/// Use [`Engine::builder()`] to create an instance. Without calling
/// [`output()`](Self::output) or [`shared_output()`](Self::shared_output),
/// the engine runs in benchmark mode (no output channel, no cloning overhead).
///
/// # Examples
///
/// ```rust,no_run
/// use varpulis_runtime::{Engine, Metrics};
/// use tokio::sync::mpsc;
///
/// // Simple construction with output channel
/// let (tx, _rx) = mpsc::channel(100);
/// let mut engine = Engine::builder()
///     .output(tx)
///     .build();
///
/// // Full construction with metrics, DLQ, and context name
/// let (tx2, _rx2) = mpsc::channel(100);
/// let mut engine = Engine::builder()
///     .output(tx2)
///     .metrics(Metrics::new())
///     .context_name("worker-0")
///     .dlq_path("/var/log/varpulis-dlq.jsonl")
///     .build();
///
/// // Benchmark mode (no output, no cloning overhead)
/// let mut engine = Engine::builder().build();
/// ```
#[derive(Debug)]
pub struct EngineBuilder {
    output_channel: Option<OutputChannel>,
    metrics: Option<Metrics>,
    context_name: Option<String>,
    dlq_path: Option<std::path::PathBuf>,
    dlq_config: DlqConfig,
    udf_registry: UdfRegistry,
    credentials_store: Option<Arc<connector::credentials::CredentialsStore>>,
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineBuilder {
    /// Create a new builder with default configuration (benchmark mode, no output).
    pub fn new() -> Self {
        Self {
            output_channel: None,
            metrics: None,
            context_name: None,
            dlq_path: None,
            dlq_config: DlqConfig::default(),
            udf_registry: UdfRegistry::new(),
            credentials_store: None,
        }
    }

    /// Set the output channel for emitted events (legacy owned channel).
    pub fn output(mut self, tx: mpsc::Sender<Event>) -> Self {
        self.output_channel = Some(OutputChannel::Owned(tx));
        self
    }

    /// Set a zero-copy shared output channel (recommended for performance).
    pub fn shared_output(mut self, tx: mpsc::Sender<SharedEvent>) -> Self {
        self.output_channel = Some(OutputChannel::Shared(tx));
        self
    }

    /// Drop output events instead of forwarding them anywhere.
    ///
    /// For long-running headless pipelines (`varpulis run --quiet`) where
    /// sink deliveries via `.to()` are the only observable output. Unlike
    /// omitting the channel (which collects outputs into an in-memory
    /// buffer for `process_batch_sync_collect`), this is a true no-op per
    /// event and stays O(1) in memory over unbounded runs.
    pub fn discard_output(mut self) -> Self {
        self.output_channel = Some(OutputChannel::Discard);
        self
    }

    /// Enable Prometheus metrics collection.
    pub fn metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Set the context name for this engine instance (used in multi-context deployments).
    pub fn context_name(mut self, name: impl Into<String>) -> Self {
        self.context_name = Some(name.into());
        self
    }

    /// Set a custom dead-letter queue file path (default: `varpulis-dlq.jsonl`).
    pub fn dlq_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.dlq_path = Some(path.into());
        self
    }

    /// Set custom dead-letter queue configuration.
    pub fn dlq_config(mut self, config: DlqConfig) -> Self {
        self.dlq_config = config;
        self
    }

    /// Register a native scalar UDF.
    pub fn scalar_udf(mut self, udf: Arc<dyn crate::udf::ScalarUDF>) -> Self {
        self.udf_registry.register_scalar(udf);
        self
    }

    /// Register a native aggregate UDF.
    pub fn aggregate_udf(mut self, udf: Arc<dyn crate::udf::AggregateUDF>) -> Self {
        self.udf_registry.register_aggregate(udf);
        self
    }

    /// Set the connector credentials store for resolving `profile:` references in VPL.
    pub fn credentials(mut self, store: Arc<connector::credentials::CredentialsStore>) -> Self {
        self.credentials_store = Some(store);
        self
    }

    /// Build the engine. Returns the configured `Engine` ready for `load()`.
    ///
    /// After building, call [`Engine::load()`] or [`Engine::load_with_source()`]
    /// to load a VPL program. Post-load configuration like
    /// [`Engine::enable_checkpointing()`] and [`Engine::enable_watermark_tracking()`]
    /// should be called after loading.
    pub fn build(self) -> Engine {
        let mut engine = Engine::new_internal(self.output_channel);
        engine.metrics = self.metrics;
        engine.context_name = self.context_name;
        engine.dlq_path = self.dlq_path;
        engine.dlq_config = self.dlq_config;
        engine.udf_registry = self.udf_registry;
        engine.credentials_store = self.credentials_store;
        engine
    }
}
