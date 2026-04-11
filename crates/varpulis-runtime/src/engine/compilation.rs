//! Program compilation: stream registration, operator compilation, and join key extraction.
//!
//! This module contains the `impl Engine` methods that compile VPL stream declarations
//! into runtime stream definitions, including SASE+ pattern compilation, Hamlet
//! aggregator setup, and PST forecaster construction.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Duration;
use rustc_hash::FxHashMap;
use tracing::{debug, info, warn};
use varpulis_core::ast::{Expr, StreamOp, StreamSource};

#[cfg(feature = "async-runtime")]
use super::types::ConcurrentConfig;
use super::types::{
    AlertConfig, DistinctState, EmitConfig, EmitExprConfig, EnrichConfig, FieldAggregateInfo,
    ForecastConfig, LimitState, LogConfig, MergeSource, PartitionedAggregatorState,
    PartitionedSlidingCountWindowState, PartitionedWindowState, PatternConfig, PrintConfig,
    RuntimeOp, RuntimeSource, SelectConfig, SourceBinding, StreamDefinition, TimerConfig, ToConfig,
    TrendAggregateConfig, WindowType,
};
use super::{compiler, pattern_analyzer, Engine};
use crate::aggregation::Aggregator;
use crate::join::JoinBuffer;
use crate::sase::SaseEngine;
use crate::window::{
    BinnedSlidingWindow, CountWindow, PartitionedBinnedSlidingWindow, PartitionedSessionWindow,
    PartitionedSlidingWindow, PartitionedTumblingWindow, SessionWindow, SlidingCountWindow,
    SlidingWindow, TumblingWindow,
};

impl Engine {
    pub(super) fn register_stream(
        &mut self,
        name: &str,
        source: &StreamSource,
        ops: &[StreamOp],
    ) -> Result<(), super::error::EngineError> {
        // Extract context assignments from stream ops
        #[allow(unused_variables)]
        for (emit_idx, op) in ops.iter().enumerate() {
            match op {
                #[cfg(feature = "async-runtime")]
                StreamOp::Context(ctx_name) => {
                    self.context_map
                        .assign_stream(name.to_string(), ctx_name.clone());
                }
                #[cfg(not(feature = "async-runtime"))]
                StreamOp::Context(_) => {
                    // Context assignments require async-runtime
                }
                #[cfg(feature = "async-runtime")]
                StreamOp::Emit {
                    target_context: Some(ctx),
                    ..
                } => {
                    self.context_map.add_cross_context_emit(
                        name.to_string(),
                        emit_idx,
                        ctx.clone(),
                    );
                }
                StreamOp::Watermark(args) => {
                    // Configure per-source watermark tracking for this stream
                    self.enable_watermark_tracking();
                    let mut max_ooo = Duration::seconds(0);
                    for arg in args {
                        if arg.name == "out_of_order" {
                            if let varpulis_core::ast::Expr::Duration(ns) = &arg.value {
                                max_ooo = Duration::nanoseconds(*ns as i64);
                            }
                        }
                    }
                    let source_et = match source {
                        StreamSource::Ident(s) => Some(s.as_str()),
                        StreamSource::IdentWithAlias { name: et, .. }
                        | StreamSource::IdentWithFilterAndAlias { name: et, .. } => {
                            Some(et.as_str())
                        }
                        StreamSource::FromConnector { event_type, .. } => Some(event_type.as_str()),
                        _ => None,
                    };
                    if let Some(et) = source_et {
                        self.register_watermark_source(et, max_ooo);
                    }
                }
                StreamOp::AllowedLateness(expr) => {
                    // Configure late-data handling for this stream
                    let lateness_ns = match expr {
                        varpulis_core::ast::Expr::Duration(ns) => *ns as i64,
                        _ => 0,
                    };
                    self.late_data_configs.insert(
                        name.to_string(),
                        super::types::LateDataConfig {
                            allowed_lateness: Duration::nanoseconds(lateness_ns),
                            side_output_stream: None,
                        },
                    );
                }
                _ => {}
            }
        }

        // Check if we have sequence operations and build SASE+ engine
        let (runtime_ops, sase_engine, sequence_event_types, hamlet_aggregator, pst_forecaster) =
            self.compile_ops_with_sequences(source, ops)?;

        // Mapping from event_type to source name (for join streams)
        let mut event_type_to_source: FxHashMap<String, String> = FxHashMap::default();

        let runtime_source = match source {
            StreamSource::FromConnector {
                event_type,
                connector_name,
                params,
            } => {
                // EventType.from(Connector, topic: "...", ...)
                // Register for the event type, connector info will be used at runtime
                info!(
                    "Registering stream {} from connector {} for event type {}",
                    name, connector_name, event_type
                );
                let topic_override = params
                    .iter()
                    .find(|p| p.name == "topic")
                    .and_then(|p| p.value.as_string().map(|s| s.to_string()));
                let extra_params: HashMap<String, String> = params
                    .iter()
                    .filter(|p| p.name != "topic")
                    .filter_map(|p| {
                        let val = match &p.value {
                            varpulis_core::ast::ConfigValue::Str(s) => s.clone(),
                            varpulis_core::ast::ConfigValue::Ident(s) => s.clone(),
                            varpulis_core::ast::ConfigValue::Int(i) => i.to_string(),
                            varpulis_core::ast::ConfigValue::Bool(b) => b.to_string(),
                            varpulis_core::ast::ConfigValue::Float(f) => f.to_string(),
                            _ => return None,
                        };
                        Some((p.name.clone(), val))
                    })
                    .collect();
                self.source_bindings.push(SourceBinding {
                    connector_name: connector_name.clone(),
                    event_type: event_type.clone(),
                    topic_override,
                    extra_params,
                });
                self.router.add_route(event_type, name);
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::Ident(stream_name) => {
                // Check if this refers to a named SASE+ pattern
                if self.patterns.contains_key(stream_name) {
                    let event_types = compiler::extract_event_types_from_pattern_expr(
                        &self.patterns[stream_name].expr,
                    );
                    for et in &event_types {
                        self.router.add_route(et, name);
                    }
                    let first_type = event_types.first().cloned().unwrap_or_default();
                    RuntimeSource::EventType(first_type)
                } else {
                    // Regular stream reference
                    self.router.add_route(stream_name, name);
                    RuntimeSource::Stream(stream_name.clone())
                }
            }
            StreamSource::IdentWithAlias {
                name: event_type, ..
            }
            | StreamSource::IdentWithFilterAndAlias {
                name: event_type, ..
            } => {
                // Register for the event type (alias is handled in sequence)
                self.router.add_route(event_type, name);
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::AllWithAlias {
                name: event_type, ..
            } => {
                // Register for the event type (all + alias handled in sequence)
                self.router.add_route(event_type, name);
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::Sequence(decl) => {
                // Register for all event types in the sequence
                for step in &decl.steps {
                    self.router.add_route(&step.event_type, name);
                }
                // Use first event type as the primary source
                let first_type = decl
                    .steps
                    .first()
                    .map(|s| s.event_type.clone())
                    .unwrap_or_default();
                RuntimeSource::EventType(first_type)
            }
            StreamSource::Join(clauses) => {
                let sources: Vec<String> = clauses.iter().map(|c| c.source.clone()).collect();
                info!(
                    "Registering join stream {} from sources: {:?}",
                    name, sources
                );
                // For join sources, we register based on whether the source is a derived stream or an event type
                // - Derived streams (with operations like aggregate, window, etc.) output events with stream name as event_type
                // - Simple event streams need to receive the raw event type
                for source in &sources {
                    if let Some(stream_def) = self.streams.get(source) {
                        // Source is a registered stream
                        // Check if it has any transforming operations (aggregate, window, select, etc.)
                        let has_operations = !stream_def.operations.is_empty();

                        if has_operations {
                            // Derived stream with operations - register for the stream name
                            // because its output events have event_type = stream name
                            info!(
                                "Join source '{}' is a derived stream, registering for stream name",
                                source
                            );
                            self.router.add_route(source, name);
                            event_type_to_source.insert(source.clone(), source.clone());
                        } else {
                            // Simple passthrough stream - register for underlying event type
                            let event_type = match &stream_def.source {
                                RuntimeSource::EventType(et) => et.clone(),
                                RuntimeSource::Stream(s) => s.clone(),
                                _ => source.clone(),
                            };
                            info!(
                                "Join source '{}' is a passthrough stream, registering for event type '{}'",
                                source, event_type
                            );
                            self.router.add_route(&event_type, name);
                            event_type_to_source.insert(event_type, source.clone());
                        }
                    } else {
                        // Source stream not yet registered, assume it's an event type name
                        info!(
                            "Join source '{}' not found as stream, treating as event type",
                            source
                        );
                        self.router.add_route(source, name);
                        event_type_to_source.insert(source.clone(), source.clone());
                    }
                }
                RuntimeSource::Join(sources)
            }
            StreamSource::Merge(decls) => {
                let merge_sources: Vec<MergeSource> = decls
                    .iter()
                    .map(|d| MergeSource {
                        name: d.name.clone(),
                        event_type: d.source.clone(),
                        filter: d.filter.clone(),
                    })
                    .collect();

                // Register for all source event types
                for ms in &merge_sources {
                    self.router.add_route(&ms.event_type, name);
                }

                info!(
                    "Registering merge stream {} with {} sources",
                    name,
                    merge_sources.len()
                );
                RuntimeSource::Merge(merge_sources)
            }
            StreamSource::Timer(decl) => {
                // Extract interval from duration expression
                let interval_ns = match &decl.interval {
                    varpulis_core::ast::Expr::Duration(ns) => *ns,
                    _ => {
                        warn!("Timer interval must be a duration, defaulting to 1s");
                        1_000_000_000u64 // 1 second default
                    }
                };

                // Extract optional initial delay
                let initial_delay_ns =
                    decl.initial_delay
                        .as_ref()
                        .and_then(|expr| match expr.as_ref() {
                            varpulis_core::ast::Expr::Duration(ns) => Some(*ns),
                            _ => None,
                        });

                // Create timer event type based on stream name
                let timer_event_type = format!("Timer_{name}");

                // Register this stream to receive timer events
                self.router.add_route(&timer_event_type, name);

                info!(
                    "Registering timer stream {} with interval {}ms{}",
                    name,
                    interval_ns / 1_000_000,
                    initial_delay_ns
                        .map(|d| format!(", initial_delay {}ms", d / 1_000_000))
                        .unwrap_or_default()
                );

                RuntimeSource::Timer(TimerConfig {
                    interval_ns,
                    initial_delay_ns,
                    timer_event_type,
                })
            }
        };

        // Register for all event types in sequence (avoid duplicates)
        for event_type in &sequence_event_types {
            self.router.add_route(event_type, name);
        }
        if !sequence_event_types.is_empty() {
            debug!(
                "Stream {} registered for sequence event types: {:?}",
                name, sequence_event_types
            );
        }

        // Create JoinBuffer for Join sources
        let join_buffer = if let StreamSource::Join(clauses) = source {
            let join_sources: Vec<String> = clauses.iter().map(|c| c.source.clone()).collect();
            let join_keys = self.extract_join_keys(clauses, ops);
            let window_duration = self.extract_window_duration(ops);
            let join_type = clauses.first().map(|c| c.join_type).unwrap_or_default();

            debug!(
                "Creating JoinBuffer for stream {} ({:?}) with sources {:?}, keys {:?}, window {:?}",
                name, join_type, join_sources, join_keys, window_duration
            );

            Some(
                JoinBuffer::new(join_sources, join_keys, window_duration).with_join_type(join_type),
            )
        } else {
            None
        };

        // Log source description before moving
        let source_desc = runtime_source.describe();

        // Build enrichment provider if any .enrich() ops reference a connector (async-runtime only)
        #[cfg(feature = "async-runtime")]
        let enrichment = {
            let enrich_op = runtime_ops.iter().find_map(|op| {
                if let RuntimeOp::Enrich(config) = op {
                    Some(config)
                } else {
                    None
                }
            });
            if let Some(config) = enrich_op {
                if let Some(conn_config) = self.connectors.get(&config.connector_name) {
                    let provider =
                        crate::enrichment::create_provider(conn_config).map_err(|e| {
                            super::error::EngineError::Compilation(format!(
                                "Failed to create enrichment provider: {e}"
                            ))
                        })?;
                    let cache_ttl = config.cache_ttl_ns.map_or(
                        std::time::Duration::from_secs(300),
                        std::time::Duration::from_nanos,
                    );
                    let cache = crate::enrichment::EnrichmentCache::new(cache_ttl);
                    Some((
                        Arc::from(provider) as Arc<dyn crate::enrichment::EnrichmentProvider>,
                        Arc::new(cache),
                    ))
                } else {
                    warn!(
                        "Connector '{}' not found for .enrich() in stream '{}'",
                        config.connector_name, name
                    );
                    None
                }
            } else {
                None
            }
        };

        self.streams.insert(
            name.to_string(),
            StreamDefinition {
                name: name.to_string(),
                name_arc: Arc::from(name),
                source: runtime_source,
                operations: runtime_ops,
                sase_engine,
                join_buffer,
                event_type_to_source,
                hamlet_aggregator,
                shared_hamlet_ref: None,
                pst_forecaster,
                last_raw_event: None,
                #[cfg(feature = "async-runtime")]
                enrichment,
                #[cfg(feature = "async-runtime")]
                buffer_config: None,
            },
        );

        info!("Registered stream: {} (source: {})", name, source_desc);
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn compile_ops_with_sequences(
        &self,
        source: &StreamSource,
        ops: &[StreamOp],
    ) -> Result<
        (
            Vec<RuntimeOp>,
            Option<SaseEngine>,
            Vec<String>,
            Option<crate::hamlet::HamletAggregator>,
            Option<crate::pst::PatternMarkovChain>,
        ),
        super::error::EngineError,
    > {
        let mut runtime_ops = Vec::new();
        let mut sequence_event_types: Vec<String> = Vec::new();
        let mut partition_key: Option<String> = None;

        // For SASE+ pattern compilation
        let mut followed_by_clauses: Vec<varpulis_core::ast::FollowedByClause> = Vec::new();
        let mut negation_clauses: Vec<varpulis_core::ast::FollowedByClause> = Vec::new();
        let mut global_within: Option<std::time::Duration> = None;

        // For Hamlet trend aggregation
        let mut trend_agg_items: Option<Vec<varpulis_core::ast::TrendAggItem>> = None;
        let mut within_expr_for_hamlet: Option<varpulis_core::ast::Expr> = None;

        // For PST forecasting
        let mut forecast_spec: Option<varpulis_core::ast::ForecastSpec> = None;
        let mut forecast_insert_idx: Option<usize> = None;

        // For SASE+ explicit selection / emission mode operators
        let mut selection_mode_override: Option<varpulis_core::ast::SelectionMode> = None;
        let mut emission_mode_override: Option<varpulis_core::ast::EmissionMode> = None;

        // Prepend inline filter from IdentWithFilterAndAlias source — but ONLY
        // for non-sequence streams. For sequence patterns, the filter becomes a
        // SASE predicate on the first step (handled in compiler.rs), not a
        // stream-level WhereExpr that would block ALL events including later steps.
        let has_sequence_ops = ops.iter().any(|op| {
            matches!(
                op,
                StreamOp::FollowedBy(_) | StreamOp::Not(_) | StreamOp::Within(_)
            )
        });
        if let StreamSource::IdentWithFilterAndAlias { filter, .. } = source {
            if !has_sequence_ops {
                runtime_ops.push(RuntimeOp::WhereExpr(filter.clone()));
            }
        }

        // Helper closure to resolve a stream/event name to the underlying event type
        let resolve_event_type = |name: &str| -> String {
            if let Some(stream_def) = self.streams.get(name) {
                // This is a registered stream - get its underlying event type
                match &stream_def.source {
                    RuntimeSource::EventType(et) => et.clone(),
                    RuntimeSource::Stream(s) => s.clone(),
                    _ => name.to_string(),
                }
            } else {
                // Not a registered stream - use as-is (it's an event type)
                name.to_string()
            }
        };

        // Collect sequence event types from source (with stream resolution)
        // Only add source event types when there are actual sequence operations
        // (followedBy, not, within). Without this guard, a derived stream like
        // `HighTempAlert = Temperatures .where(...)` would incorrectly register
        // for the underlying event type (TemperatureReading) in addition to the
        // stream name (Temperatures), causing duplicate processing.
        let has_sequence_ops = ops.iter().any(|op| {
            matches!(
                op,
                StreamOp::FollowedBy(_) | StreamOp::Not(_) | StreamOp::Within(_)
            )
        });

        match source {
            StreamSource::Sequence(decl) => {
                for step in &decl.steps {
                    let resolved = resolve_event_type(&step.event_type);
                    if !sequence_event_types.contains(&resolved) {
                        sequence_event_types.push(resolved);
                    }
                }
            }
            StreamSource::Ident(name) if self.patterns.contains_key(name) => {
                // Named pattern reference - extract event types from pattern
                let event_types =
                    compiler::extract_event_types_from_pattern_expr(&self.patterns[name].expr);
                for et in event_types {
                    let resolved = resolve_event_type(&et);
                    if !sequence_event_types.contains(&resolved) {
                        sequence_event_types.push(resolved);
                    }
                }
            }
            StreamSource::Ident(name) if has_sequence_ops => {
                // Initial source for a sequence pattern - resolve to underlying event type
                let resolved = resolve_event_type(name);
                if !sequence_event_types.contains(&resolved) {
                    sequence_event_types.push(resolved);
                }
            }
            StreamSource::IdentWithAlias { name, .. }
            | StreamSource::IdentWithFilterAndAlias { name, .. }
            | StreamSource::AllWithAlias { name, .. }
                if has_sequence_ops =>
            {
                let resolved = resolve_event_type(name);
                if !sequence_event_types.contains(&resolved) {
                    sequence_event_types.push(resolved);
                }
            }
            _ => {}
        }

        for op in ops {
            match op {
                StreamOp::FollowedBy(clause) => {
                    // Store raw clause for SASE+ compilation
                    followed_by_clauses.push(clause.clone());
                    // Resolve event type for routing registration
                    let resolved = resolve_event_type(&clause.event_type);
                    if !sequence_event_types.contains(&resolved) {
                        sequence_event_types.push(resolved);
                    }
                    continue;
                }
                StreamOp::Within(expr) => {
                    // Parse duration from expression
                    let duration_ns = match expr {
                        varpulis_core::ast::Expr::Duration(ns) => *ns,
                        _ => 300_000_000_000u64, // 5 minutes default
                    };
                    global_within = Some(std::time::Duration::from_nanos(duration_ns));
                    within_expr_for_hamlet = Some(expr.clone());
                    continue;
                }
                StreamOp::Not(clause) => {
                    // Store negation clause for SASE+ engine
                    negation_clauses.push(clause.clone());
                    // Add negation event type to sequence event types so it gets routed
                    let resolved = resolve_event_type(&clause.event_type);
                    if !sequence_event_types.contains(&resolved) {
                        sequence_event_types.push(resolved);
                    }
                    continue;
                }
                StreamOp::TrendAggregate(items) => {
                    trend_agg_items = Some(items.clone());
                    continue;
                }
                StreamOp::Forecast(spec) => {
                    forecast_spec = Some(spec.clone());
                    // Record current position so Forecast op is inserted here,
                    // BEFORE any subsequent .emit()/.where() ops.
                    forecast_insert_idx = Some(runtime_ops.len());
                    continue;
                }
                StreamOp::SelectionMode(mode) => {
                    selection_mode_override = Some(*mode);
                    continue;
                }
                StreamOp::EmissionMode(mode) => {
                    emission_mode_override = Some(*mode);
                    continue;
                }
                StreamOp::Enrich(spec) => {
                    // Resolve timeout (default 5s = 5_000_000_000 ns)
                    let timeout_ns = match &spec.timeout {
                        Some(varpulis_core::ast::Expr::Duration(ns)) => *ns,
                        _ => 5_000_000_000u64,
                    };
                    // Resolve cache TTL
                    let cache_ttl_ns = match &spec.cache_ttl {
                        Some(varpulis_core::ast::Expr::Duration(ns)) => Some(*ns),
                        _ => None,
                    };
                    // Resolve fallback value
                    let fallback = match &spec.fallback {
                        Some(varpulis_core::ast::Expr::Str(s)) => {
                            Some(varpulis_core::Value::str(s.as_str()))
                        }
                        Some(varpulis_core::ast::Expr::Int(i)) => {
                            Some(varpulis_core::Value::Int(*i))
                        }
                        Some(varpulis_core::ast::Expr::Float(f)) => {
                            Some(varpulis_core::Value::Float(*f))
                        }
                        Some(varpulis_core::ast::Expr::Bool(b)) => {
                            Some(varpulis_core::Value::Bool(*b))
                        }
                        Some(varpulis_core::ast::Expr::Null) => Some(varpulis_core::Value::Null),
                        _ => None,
                    };
                    runtime_ops.push(RuntimeOp::Enrich(EnrichConfig {
                        connector_name: spec.connector_name.clone(),
                        key_expr: (*spec.key_expr).clone(),
                        fields: spec.fields.clone(),
                        cache_ttl_ns,
                        timeout_ns,
                        fallback,
                    }));
                    continue;
                }
                StreamOp::Score(spec) => {
                    #[cfg(feature = "onnx")]
                    {
                        let gpu_config = if spec.gpu {
                            Some(crate::scoring::GpuConfig {
                                provider: crate::scoring::GpuProvider::Cuda {
                                    device_id: spec.device_id,
                                },
                                batch_size: spec.batch_size.max(1),
                            })
                        } else if spec.batch_size > 1 {
                            Some(crate::scoring::GpuConfig {
                                provider: crate::scoring::GpuProvider::Cpu,
                                batch_size: spec.batch_size,
                            })
                        } else {
                            None
                        };

                        let model = crate::scoring::OnnxModel::load(
                            &spec.model_path,
                            spec.inputs.clone(),
                            spec.outputs.clone(),
                            gpu_config,
                        )
                        .map_err(|e| {
                            super::error::EngineError::Compilation(format!(
                                "Failed to load ONNX model: {e}"
                            ))
                        })?;
                        runtime_ops.push(RuntimeOp::Score(super::types::ScoreConfig {
                            model: std::sync::Arc::new(model),
                            input_fields: spec.inputs.clone(),
                            output_fields: spec.outputs.clone(),
                            batch_size: spec.batch_size.max(1),
                        }));
                        continue;
                    }
                    #[cfg(not(feature = "onnx"))]
                    return Err(super::error::EngineError::Compilation(format!(
                        ".score() operator requires the 'onnx' feature. \
                         Rebuild with: cargo build --features onnx (model: {})",
                        spec.model_path
                    )));
                }
                StreamOp::Context(_) => {
                    // Context assignment is metadata, not a runtime operation.
                    // Handled by the engine's load() method via context_map.
                    continue;
                }
                StreamOp::Watermark(_) | StreamOp::AllowedLateness(_) => {
                    // Handled in register_stream() as metadata ops
                    continue;
                }
                _ => {}
            }

            // Handle non-sequence operations
            match op {
                StreamOp::Window(args) => {
                    // Check for session window first
                    if let Some(ref gap_expr) = args.session_gap {
                        let gap_ns = match gap_expr {
                            varpulis_core::ast::Expr::Duration(ns) => *ns,
                            _ => 300_000_000_000, // 5 minute default
                        };
                        let gap = Duration::nanoseconds(gap_ns as i64);
                        if let Some(ref key) = partition_key {
                            runtime_ops.push(RuntimeOp::Window(WindowType::PartitionedSession(
                                PartitionedSessionWindow::new(key.clone(), gap),
                            )));
                        } else {
                            runtime_ops.push(RuntimeOp::Window(WindowType::Session(
                                SessionWindow::new(gap),
                            )));
                        }
                    } else {
                        // Check if this is a count-based or time-based window
                        match &args.duration {
                            varpulis_core::ast::Expr::Int(count) => {
                                // Count-based window
                                let count = *count as usize;

                                // Get slide amount if specified (default to window size for tumbling)
                                let slide = args.sliding.as_ref().map(|s| match s {
                                    varpulis_core::ast::Expr::Int(n) => *n as usize,
                                    _ => 1,
                                });

                                // If we have a partition key, use partitioned window
                                if let Some(ref key) = partition_key {
                                    if let Some(slide_size) = slide {
                                        // Partitioned sliding count window
                                        runtime_ops.push(RuntimeOp::PartitionedSlidingCountWindow(
                                            PartitionedSlidingCountWindowState::new(
                                                key.clone(),
                                                count,
                                                slide_size,
                                            ),
                                        ));
                                    } else {
                                        // Partitioned tumbling count window
                                        runtime_ops.push(RuntimeOp::PartitionedWindow(
                                            PartitionedWindowState::new(key.clone(), count),
                                        ));
                                    }
                                } else if let Some(slide_size) = slide {
                                    runtime_ops.push(RuntimeOp::Window(WindowType::SlidingCount(
                                        SlidingCountWindow::new(count, slide_size),
                                    )));
                                } else {
                                    runtime_ops.push(RuntimeOp::Window(WindowType::Count(
                                        CountWindow::new(count),
                                    )));
                                }
                            }
                            varpulis_core::ast::Expr::Duration(ns) => {
                                // Time-based window
                                let duration = Duration::nanoseconds(*ns as i64);
                                if let Some(ref key) = partition_key {
                                    // Partitioned time-based window
                                    if let Some(sliding) = &args.sliding {
                                        let slide_ns = match sliding {
                                            varpulis_core::ast::Expr::Duration(ns) => *ns,
                                            _ => 60_000_000_000, // 1 minute default
                                        };
                                        let slide = Duration::nanoseconds(slide_ns as i64);
                                        let ratio = duration
                                            .num_milliseconds()
                                            .checked_div(slide.num_milliseconds().max(1))
                                            .unwrap_or(0);
                                        if ratio >= 10 {
                                            runtime_ops.push(RuntimeOp::Window(
                                                WindowType::PartitionedBinnedSliding(
                                                    PartitionedBinnedSlidingWindow::new(
                                                        key.clone(),
                                                        duration,
                                                        slide,
                                                        Vec::new(),
                                                    ),
                                                ),
                                            ));
                                        } else {
                                            runtime_ops.push(RuntimeOp::Window(
                                                WindowType::PartitionedSliding(
                                                    PartitionedSlidingWindow::new(
                                                        key.clone(),
                                                        duration,
                                                        slide,
                                                    ),
                                                ),
                                            ));
                                        }
                                    } else {
                                        runtime_ops.push(RuntimeOp::Window(
                                            WindowType::PartitionedTumbling(
                                                PartitionedTumblingWindow::new(
                                                    key.clone(),
                                                    duration,
                                                ),
                                            ),
                                        ));
                                    }
                                } else if let Some(sliding) = &args.sliding {
                                    let slide_ns = match sliding {
                                        varpulis_core::ast::Expr::Duration(ns) => *ns,
                                        _ => 60_000_000_000, // 1 minute default
                                    };
                                    let slide = Duration::nanoseconds(slide_ns as i64);
                                    let ratio = duration
                                        .num_milliseconds()
                                        .checked_div(slide.num_milliseconds().max(1))
                                        .unwrap_or(0);
                                    if ratio >= 10 {
                                        runtime_ops.push(RuntimeOp::Window(
                                            WindowType::BinnedSliding(BinnedSlidingWindow::new(
                                                duration,
                                                slide,
                                                Vec::new(),
                                            )),
                                        ));
                                    } else {
                                        runtime_ops.push(RuntimeOp::Window(WindowType::Sliding(
                                            SlidingWindow::new(duration, slide),
                                        )));
                                    }
                                } else {
                                    runtime_ops.push(RuntimeOp::Window(WindowType::Tumbling(
                                        TumblingWindow::new(duration),
                                    )));
                                }
                            }
                            _ => {
                                // Default to 5 minute tumbling window
                                let duration = Duration::nanoseconds(300_000_000_000);
                                if let Some(ref key) = partition_key {
                                    runtime_ops.push(RuntimeOp::Window(
                                        WindowType::PartitionedTumbling(
                                            PartitionedTumblingWindow::new(key.clone(), duration),
                                        ),
                                    ));
                                } else {
                                    runtime_ops.push(RuntimeOp::Window(WindowType::Tumbling(
                                        TumblingWindow::new(duration),
                                    )));
                                }
                            }
                        }
                    } // close else (non-session)
                }
                StreamOp::PartitionBy(expr) => {
                    // Extract partition key field name
                    if let varpulis_core::ast::Expr::Ident(field) = expr {
                        partition_key = Some(field.clone());
                    }
                }
                StreamOp::Aggregate(items) => {
                    let mut aggregator = Aggregator::new();
                    for item in items {
                        if let Some((func, field)) = compiler::compile_agg_expr(&item.expr) {
                            aggregator = aggregator.add(item.alias.clone(), func, field);
                        }
                    }

                    // Phase-2 fusion: if the previously pushed op is a
                    // PartitionedTumbling window AND every aggregation
                    // function is one of sum/avg/min/max/count, replace
                    // the (window + partitioned aggregate) pair with a
                    // single fused streaming columnar op. This keeps
                    // accumulator state alive across arriving event
                    // batches per (bin, group_idx) and is what closes
                    // scenario 02 against Arroyo. See
                    // `docs/development/columnar-aggregation-plan.md`.
                    #[cfg(feature = "arrow")]
                    {
                        if let Some(ref key) = partition_key {
                            if aggregator.supported_for_columnar() {
                                if let Some(crate::engine::types::RuntimeOp::Window(
                                    crate::engine::types::WindowType::PartitionedTumbling(w),
                                )) = runtime_ops.last()
                                {
                                    let bin_duration_ms = w.duration().num_milliseconds();
                                    if bin_duration_ms > 0 {
                                        if let Some(state) = crate::engine::types::PartitionedWindowedColumnarAggregateState::try_new(
                                            key.clone(),
                                            bin_duration_ms,
                                            &aggregator,
                                        ) {
                                            // Pop the window op — it's
                                            // subsumed by the fused op.
                                            runtime_ops.pop();
                                            runtime_ops.push(
                                                crate::engine::types::RuntimeOp::PartitionedWindowedColumnarAggregate(state),
                                            );
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Phase-3b fusion: mirror of phase 2 but for the
                    // non-partitioned case. If the previously pushed op
                    // is a plain `Tumbling` window AND no partition_by
                    // preceded AND every aggregation function is
                    // columnar-supported, emit the
                    // `WindowedColumnarAggregate` fused op instead of
                    // `Window(Tumbling) + Aggregate`.
                    #[cfg(feature = "arrow")]
                    {
                        if partition_key.is_none() && aggregator.supported_for_columnar() {
                            if let Some(crate::engine::types::RuntimeOp::Window(
                                crate::engine::types::WindowType::Tumbling(w),
                            )) = runtime_ops.last()
                            {
                                let bin_duration_ms = w.duration().num_milliseconds();
                                if bin_duration_ms > 0 {
                                    if let Some(state) = crate::engine::types::WindowedColumnarAggregateState::try_new(
                                        bin_duration_ms,
                                        &aggregator,
                                    ) {
                                        runtime_ops.pop();
                                        runtime_ops.push(
                                            crate::engine::types::RuntimeOp::WindowedColumnarAggregate(state),
                                        );
                                        continue;
                                    }
                                }
                            }
                        }
                    }

                    // Fallback path: separate window + aggregator (or
                    // plain aggregator). Used when the arrow feature is
                    // off, when no partition_by was seen, when the
                    // window isn't PartitionedTumbling, or when the
                    // aggregator uses an unsupported function.
                    if let Some(ref key) = partition_key {
                        runtime_ops.push(RuntimeOp::PartitionedAggregate(
                            PartitionedAggregatorState::new(key.clone(), aggregator),
                        ));
                    } else {
                        runtime_ops.push(RuntimeOp::Aggregate(
                            crate::engine::types::AggregatorState::new(aggregator),
                        ));
                    }
                }
                StreamOp::Select(items) => {
                    let fields: Vec<(String, varpulis_core::ast::Expr)> = items
                        .iter()
                        .map(|item| match item {
                            varpulis_core::ast::SelectItem::Field(name) => {
                                (name.clone(), varpulis_core::ast::Expr::Ident(name.clone()))
                            }
                            varpulis_core::ast::SelectItem::Alias(name, expr) => {
                                (name.clone(), expr.clone())
                            }
                        })
                        .collect();
                    runtime_ops.push(RuntimeOp::Select(SelectConfig { fields }));
                }
                StreamOp::Emit {
                    output_type: _,
                    fields: args,
                    target_context,
                } => {
                    // Check if any args have complex expressions (not just strings or idents)
                    let has_complex_expr = args.iter().any(|arg| {
                        !matches!(
                            &arg.value,
                            varpulis_core::ast::Expr::Str(_) | varpulis_core::ast::Expr::Ident(_)
                        )
                    });

                    if has_complex_expr {
                        // Use EmitExpr for complex expressions with function evaluation
                        let fields: Vec<(String, varpulis_core::ast::Expr)> = args
                            .iter()
                            .map(|arg| (arg.name.clone(), arg.value.clone()))
                            .collect();
                        runtime_ops.push(RuntimeOp::EmitExpr(EmitExprConfig {
                            fields,
                            target_context: target_context.clone(),
                        }));
                    } else {
                        // Use simple EmitConfig for string/ident only
                        let fields: Vec<(String, String)> = args
                            .iter()
                            .filter_map(|arg| {
                                let value = match &arg.value {
                                    varpulis_core::ast::Expr::Str(s) => s.clone(),
                                    varpulis_core::ast::Expr::Ident(s) => s.clone(),
                                    _ => return None,
                                };
                                Some((arg.name.clone(), value))
                            })
                            .collect();
                        runtime_ops.push(RuntimeOp::Emit(EmitConfig {
                            fields,
                            target_context: target_context.clone(),
                        }));
                    }
                }
                StreamOp::Print(exprs) => {
                    runtime_ops.push(RuntimeOp::Print(PrintConfig {
                        exprs: exprs.clone(),
                    }));
                }
                StreamOp::Log(args) => {
                    let mut level = "info".to_string();
                    let mut message = None;
                    let mut data_field = None;

                    for arg in args {
                        match arg.name.as_str() {
                            "level" => {
                                if let varpulis_core::ast::Expr::Str(s) = &arg.value {
                                    level = s.clone();
                                }
                            }
                            "message" => {
                                if let varpulis_core::ast::Expr::Str(s) = &arg.value {
                                    message = Some(s.clone());
                                }
                            }
                            "data" => {
                                if let varpulis_core::ast::Expr::Ident(s) = &arg.value {
                                    data_field = Some(s.clone());
                                }
                            }
                            _ => {}
                        }
                    }

                    runtime_ops.push(RuntimeOp::Log(LogConfig {
                        level,
                        message,
                        data_field,
                    }));
                }
                StreamOp::Alert(args) => {
                    let mut webhook_url = None;
                    let mut message_template = None;

                    for arg in args {
                        match arg.name.as_str() {
                            "webhook" => {
                                if let varpulis_core::ast::Expr::Str(s) = &arg.value {
                                    webhook_url = Some(s.clone());
                                }
                            }
                            "message" => {
                                if let varpulis_core::ast::Expr::Str(s) = &arg.value {
                                    message_template = Some(s.clone());
                                }
                            }
                            _ => {}
                        }
                    }

                    runtime_ops.push(RuntimeOp::Alert(AlertConfig {
                        webhook_url,
                        message_template,
                    }));
                }
                StreamOp::Where(expr) => {
                    // Store expression for runtime evaluation with user functions
                    runtime_ops.push(RuntimeOp::WhereExpr(expr.clone()));
                }
                StreamOp::Pattern(def) => {
                    runtime_ops.push(RuntimeOp::Pattern(PatternConfig {
                        name: def.name.clone(),
                        matcher: def.matcher.clone(),
                    }));
                }
                StreamOp::Having(expr) => {
                    // Having filter - applied after aggregation
                    runtime_ops.push(RuntimeOp::Having(expr.clone()));
                }
                StreamOp::To {
                    connector_name,
                    params,
                } => {
                    let topic_param = params.iter().find(|p| p.name == "topic");
                    let (topic_spec, sink_key) = if let Some(tp) = topic_param {
                        match &tp.value {
                            // Static string topic — current behavior
                            varpulis_core::ast::ConfigValue::Str(s) => {
                                let key = format!("{connector_name}::{s}");
                                (Some(super::types::TopicSpec::Static(s.clone())), key)
                            }
                            // Bare identifier — dynamic field reference
                            varpulis_core::ast::ConfigValue::Ident(field) => (
                                Some(super::types::TopicSpec::Dynamic(Expr::Ident(field.clone()))),
                                connector_name.clone(),
                            ),
                            // Concatenation — dynamic expression
                            varpulis_core::ast::ConfigValue::Concat(parts) => {
                                let expr = build_concat_expr(parts);
                                (
                                    Some(super::types::TopicSpec::Dynamic(expr)),
                                    connector_name.clone(),
                                )
                            }
                            _ => (None, connector_name.clone()),
                        }
                    } else {
                        (None, connector_name.clone())
                    };
                    let extra_params: HashMap<String, String> = params
                        .iter()
                        .filter(|p| p.name != "topic")
                        .filter_map(|p| {
                            let val = match &p.value {
                                varpulis_core::ast::ConfigValue::Str(s) => s.clone(),
                                varpulis_core::ast::ConfigValue::Ident(s) => s.clone(),
                                varpulis_core::ast::ConfigValue::Int(i) => i.to_string(),
                                varpulis_core::ast::ConfigValue::Bool(b) => b.to_string(),
                                varpulis_core::ast::ConfigValue::Float(f) => f.to_string(),
                                _ => return None,
                            };
                            Some((p.name.clone(), val))
                        })
                        .collect();
                    runtime_ops.push(RuntimeOp::To(ToConfig {
                        connector_name: connector_name.clone(),
                        topic: topic_spec,
                        sink_key,
                        extra_params,
                    }));
                }
                StreamOp::Process(expr) => {
                    runtime_ops.push(RuntimeOp::Process(expr.clone()));
                }
                StreamOp::On(_) => {
                    // Join condition - handled by extract_join_keys(), not a runtime op
                }
                StreamOp::Filter(expr) => {
                    // .filter(expr) is an alias for .where(expr)
                    runtime_ops.push(RuntimeOp::WhereExpr(expr.clone()));
                }
                StreamOp::Distinct(expr) => {
                    runtime_ops.push(RuntimeOp::Distinct(DistinctState {
                        expr: expr.clone(),
                        seen: hashlink::LruCache::new(super::types::DISTINCT_LRU_CAPACITY),
                    }));
                }
                StreamOp::Limit(expr) => {
                    let max = match expr {
                        varpulis_core::ast::Expr::Int(n) => *n as usize,
                        _ => {
                            return Err(super::error::EngineError::Compilation(
                                ".limit() requires an integer argument (e.g., .limit(100))".into(),
                            ));
                        }
                    };
                    runtime_ops.push(RuntimeOp::Limit(LimitState { max, count: 0 }));
                }
                StreamOp::First => {
                    // .first() is shorthand for .limit(1)
                    runtime_ops.push(RuntimeOp::Limit(LimitState { max: 1, count: 0 }));
                }
                StreamOp::Map(_) => {
                    return Err(super::error::EngineError::Compilation(
                        ".map() is not supported — use .emit() for field projection or .process() for arbitrary transformation"
                            .into(),
                    ));
                }
                StreamOp::Tap(_) => {
                    return Err(super::error::EngineError::Compilation(
                        ".tap() is not yet implemented — use .print() or .log() for debugging"
                            .into(),
                    ));
                }
                StreamOp::Collect => {
                    return Err(super::error::EngineError::Compilation(
                        ".collect() is not yet implemented — use .window() with .aggregate() for batching"
                            .into(),
                    ));
                }
                StreamOp::OnError(_) => {
                    return Err(super::error::EngineError::Compilation(
                        ".on_error() is not yet implemented — errors are logged via tracing".into(),
                    ));
                }
                StreamOp::Fork(_) | StreamOp::Any(_) | StreamOp::All => {
                    return Err(super::error::EngineError::Compilation(
                        ".fork()/.any()/.all() are not yet implemented — use multiple streams for parallel processing"
                            .into(),
                    ));
                }
                #[cfg(feature = "async-runtime")]
                StreamOp::Concurrent(ref args) => {
                    let mut workers = std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4)
                        .min(128);
                    let mut partition_key = None;

                    for arg in args {
                        match arg.name.as_str() {
                            "workers" => {
                                if let varpulis_core::Expr::Int(n) = &arg.value {
                                    workers = (*n as usize).clamp(1, 128);
                                }
                            }
                            "partition_key" => {
                                if let varpulis_core::Expr::Str(s) = &arg.value {
                                    partition_key = Some(s.clone());
                                } else if let varpulis_core::Expr::Ident(s) = &arg.value {
                                    partition_key = Some(s.clone());
                                }
                            }
                            _ => {}
                        }
                    }

                    let thread_pool = std::sync::Arc::new(
                        rayon::ThreadPoolBuilder::new()
                            .num_threads(workers)
                            .build()
                            .map_err(|e| {
                                super::error::EngineError::Compilation(format!(
                                    "Failed to create thread pool: {e}"
                                ))
                            })?,
                    );

                    runtime_ops.push(RuntimeOp::Concurrent(ConcurrentConfig {
                        workers,
                        partition_key,
                        thread_pool,
                    }));
                }
                #[cfg(not(feature = "async-runtime"))]
                StreamOp::Concurrent(_) => {
                    return Err(super::error::EngineError::Compilation(
                        ".concurrent() requires async-runtime feature (rayon thread pool)".into(),
                    ));
                }
                StreamOp::OrderBy(_) => {
                    return Err(super::error::EngineError::Compilation(
                        ".order_by() is not yet implemented — use .window() with .aggregate() for ordered output"
                            .into(),
                    ));
                }
                StreamOp::ToExpr(_) => {
                    return Err(super::error::EngineError::Compilation(
                        ".to(expr) is not supported — use .to(ConnectorName, topic: \"...\") instead"
                            .into(),
                    ));
                }
                other => {
                    return Err(super::error::EngineError::Compilation(format!(
                        "unsupported stream operation: {}",
                        stream_op_name(other)
                    )));
                }
            }
        }

        // Check if we're in trend aggregation mode (Hamlet) or detection mode (SASE)
        if let Some(ref agg_items) = trend_agg_items {
            return self.compile_hamlet_mode(
                source,
                agg_items,
                &followed_by_clauses,
                within_expr_for_hamlet.as_ref(),
                runtime_ops,
                sequence_event_types,
            );
        }

        // === Forecast Mode (PST + SASE) ===
        // If .forecast() is present, we need a sequence pattern (SASE engine)
        // and must not have .trend_aggregate()
        let mut pst_forecaster = None;

        // === Detection Mode (SASE) ===
        let sase_engine = self.compile_sase_detection(
            source,
            &followed_by_clauses,
            &negation_clauses,
            global_within,
            &mut partition_key,
            &mut runtime_ops,
            selection_mode_override,
            emission_mode_override,
        );

        // === Build PST Forecaster if .forecast() specified ===
        if let Some(spec) = forecast_spec {
            pst_forecaster = self.compile_pst_forecaster(
                &spec,
                &sase_engine,
                global_within,
                &sequence_event_types,
                forecast_insert_idx,
                &mut runtime_ops,
            )?;
        }

        Ok((
            runtime_ops,
            sase_engine,
            sequence_event_types,
            None,
            pst_forecaster,
        ))
    }

    /// Compile Hamlet trend aggregation mode.
    ///
    /// Builds a `HamletAggregator` and `TrendAggregateConfig` from the parsed
    /// sequence pattern and `.trend_aggregate()` items. Returns the full 5-tuple
    /// so the caller can `return` it directly.
    #[allow(clippy::type_complexity)]
    fn compile_hamlet_mode(
        &self,
        source: &StreamSource,
        agg_items: &[varpulis_core::ast::TrendAggItem],
        followed_by_clauses: &[varpulis_core::ast::FollowedByClause],
        within_expr: Option<&varpulis_core::ast::Expr>,
        mut runtime_ops: Vec<RuntimeOp>,
        sequence_event_types: Vec<String>,
    ) -> Result<
        (
            Vec<RuntimeOp>,
            Option<SaseEngine>,
            Vec<String>,
            Option<crate::hamlet::HamletAggregator>,
            Option<crate::pst::PatternMarkovChain>,
        ),
        super::error::EngineError,
    > {
        let event_types = pattern_analyzer::extract_event_types(source, followed_by_clauses);
        let kleene_info = pattern_analyzer::extract_kleene_info(source, followed_by_clauses);
        let window_ms = pattern_analyzer::extract_within_ms(within_expr);

        // Build a type_indices map for aggregate function resolution
        let mut type_indices_map = std::collections::HashMap::new();

        // Build MergedTemplate via TemplateBuilder
        use crate::hamlet::template::TemplateBuilder;
        let mut builder = TemplateBuilder::new();
        let query_id: crate::greta::QueryId = 0;

        // Register event types as a sequence
        let type_strs: Vec<&str> = event_types.iter().map(|s| s.as_str()).collect();
        builder.add_sequence(query_id, &type_strs);

        // Build type_indices from the template after adding sequence
        // (TemplateBuilder registers types internally)
        let template_preview = builder.build();
        for et in &event_types {
            if let Some(idx) = template_preview.type_index(et) {
                type_indices_map.insert(et.clone(), idx);
            }
        }

        // Also map aliases to type indices
        // Source alias
        match source {
            StreamSource::IdentWithAlias { name, alias } => {
                if let Some(idx) = type_indices_map.get(name) {
                    type_indices_map.insert(alias.clone(), *idx);
                }
            }
            StreamSource::IdentWithFilterAndAlias {
                name,
                alias: Some(alias),
                ..
            } => {
                if let Some(idx) = type_indices_map.get(name) {
                    type_indices_map.insert(alias.clone(), *idx);
                }
            }
            StreamSource::AllWithAlias {
                name,
                alias: Some(alias),
            } => {
                if let Some(idx) = type_indices_map.get(name) {
                    type_indices_map.insert(alias.clone(), *idx);
                }
            }
            _ => {}
        }
        for clause in followed_by_clauses {
            if let Some(ref alias) = clause.alias {
                if let Some(idx) = type_indices_map.get(&clause.event_type) {
                    type_indices_map.insert(alias.clone(), *idx);
                }
            }
        }

        // Rebuild template with Kleene info
        let mut builder = TemplateBuilder::new();
        builder.add_sequence(query_id, &type_strs);

        for ki in &kleene_info {
            // The Kleene self-loop must be at the state AFTER consuming the
            // Kleene event type (the target of the forward transition), not
            // the source state. For a pattern A -> B+ -> C with types
            // [A, B, C], states are [s0, s1, s2, s3]:
            //   (s0,A)->s1, (s1,B)->s2, (s2,C)->s3
            // The B+ self-loop goes at s2: (s2,B)->s2
            let type_idx_in_seq = type_strs
                .iter()
                .position(|&t| t == ki.event_type)
                .unwrap_or(0);
            let state = (type_idx_in_seq + 1) as u16;
            builder.add_kleene(query_id, &ki.event_type, state);
        }

        let template = builder.build();

        // Create Hamlet aggregator
        let config = crate::hamlet::HamletConfig {
            window_ms,
            incremental: true,
            ..Default::default()
        };
        let mut aggregator = crate::hamlet::HamletAggregator::new(config, template);

        // Convert trend_agg_items to GretaAggregate list
        let fields: Vec<(String, crate::greta::GretaAggregate)> = agg_items
            .iter()
            .map(|item| {
                let agg = pattern_analyzer::trend_item_to_greta(item, &type_indices_map);
                (item.alias.clone(), agg)
            })
            .collect();

        // Use the first aggregate for the query registration
        let primary_aggregate = fields
            .first()
            .map_or(crate::greta::GretaAggregate::CountTrends, |(_, agg)| *agg);

        // Build Kleene types
        let kleene_types: smallvec::SmallVec<[u16; 4]> = kleene_info
            .iter()
            .filter_map(|ki| type_indices_map.get(&ki.event_type).copied())
            .collect();

        // Build event type indices
        let event_type_indices: smallvec::SmallVec<[u16; 4]> = event_types
            .iter()
            .filter_map(|et| type_indices_map.get(et).copied())
            .collect();

        aggregator.register_query(crate::hamlet::QueryRegistration {
            id: query_id,
            event_types: event_type_indices,
            kleene_types,
            aggregate: primary_aggregate,
        });

        // Build field aggregate info for runtime computation
        // Resolves alias.field references from sum_trends(alias.field) etc.
        let mut alias_to_event_type: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        match source {
            StreamSource::IdentWithAlias { name, alias } => {
                alias_to_event_type.insert(alias.clone(), name.clone());
            }
            StreamSource::IdentWithFilterAndAlias {
                name,
                alias: Some(alias),
                ..
            } => {
                alias_to_event_type.insert(alias.clone(), name.clone());
            }
            StreamSource::AllWithAlias {
                name,
                alias: Some(alias),
            } => {
                alias_to_event_type.insert(alias.clone(), name.clone());
            }
            _ => {}
        }
        for clause in followed_by_clauses {
            if let Some(ref alias) = clause.alias {
                alias_to_event_type.insert(alias.clone(), clause.event_type.clone());
            }
        }

        let field_aggregates: Vec<FieldAggregateInfo> = agg_items
            .iter()
            .filter_map(|item| {
                let func = match item.func.as_str() {
                    "sum_trends" => "sum",
                    "avg_trends" => "avg",
                    "min_trends" => "min",
                    "max_trends" => "max",
                    _ => return None,
                };
                if let Some(Expr::Member { expr, member }) = &item.arg {
                    if let Expr::Ident(alias) = expr.as_ref() {
                        let event_type = alias_to_event_type
                            .get(alias)
                            .cloned()
                            .unwrap_or_else(|| alias.clone());
                        return Some(FieldAggregateInfo {
                            output_alias: item.alias.clone(),
                            func: func.to_string(),
                            event_type,
                            field_name: member.clone(),
                        });
                    }
                }
                None
            })
            .collect();

        // Build reverse map: type index → event type name
        let max_idx = type_indices_map.values().copied().max().unwrap_or(0) as usize;
        let mut type_index_to_name = vec![String::new(); max_idx + 1];
        // First pass: set aliases as fallback
        for (name, &idx) in &type_indices_map {
            let i = idx as usize;
            if i < type_index_to_name.len() && type_index_to_name[i].is_empty() {
                type_index_to_name[i] = name.clone();
            }
        }
        // Second pass: overwrite with real event type names
        for (name, &idx) in &type_indices_map {
            let i = idx as usize;
            if i < type_index_to_name.len() && event_types.contains(name) {
                type_index_to_name[i] = name.clone();
            }
        }

        // Insert TrendAggregate op at the beginning (replaces Sequence)
        runtime_ops.insert(
            0,
            RuntimeOp::TrendAggregate(TrendAggregateConfig {
                fields,
                query_id,
                field_aggregates,
                type_index_to_name,
                accumulated: Vec::new(),
            }),
        );

        info!(
            "Created Hamlet aggregator for trend aggregation ({} event types, {} Kleene patterns)",
            event_types.len(),
            kleene_info.len()
        );

        Ok((
            runtime_ops,
            None,
            sequence_event_types,
            Some(aggregator),
            None,
        ))
    }

    /// Build the SASE+ detection engine from sequence patterns or named pattern references.
    ///
    /// Returns `Some(SaseEngine)` when the stream has followed-by clauses, a Sequence source,
    /// or references a named pattern. Returns `None` for non-sequence streams.
    #[allow(clippy::too_many_arguments)]
    fn compile_sase_detection(
        &self,
        source: &StreamSource,
        followed_by_clauses: &[varpulis_core::ast::FollowedByClause],
        negation_clauses: &[varpulis_core::ast::FollowedByClause],
        global_within: Option<std::time::Duration>,
        partition_key: &mut Option<String>,
        runtime_ops: &mut Vec<RuntimeOp>,
        selection_mode: Option<varpulis_core::ast::SelectionMode>,
        emission_mode: Option<varpulis_core::ast::EmissionMode>,
    ) -> Option<SaseEngine> {
        let is_pattern_ref =
            matches!(source, StreamSource::Ident(name) if self.patterns.contains_key(name));

        if !followed_by_clauses.is_empty() || matches!(source, StreamSource::Sequence(_)) {
            // Add Sequence operation marker at the beginning
            runtime_ops.insert(0, RuntimeOp::Sequence);

            // Create stream resolver for derived streams
            let stream_resolver = |name: &str| -> Option<compiler::DerivedStreamInfo> {
                let stream_def = self.streams.get(name)?;

                // Extract event type from the stream source
                let event_type = match &stream_def.source {
                    RuntimeSource::EventType(et) => et.clone(),
                    RuntimeSource::Stream(s) => s.clone(),
                    _ => return None, // Join/Merge sources not supported as derived streams
                };

                // Find the first WhereExpr in operations (the stream's filter)
                let filter = stream_def.operations.iter().find_map(|op| {
                    if let RuntimeOp::WhereExpr(expr) = op {
                        Some(expr.clone())
                    } else {
                        None
                    }
                });

                Some(compiler::DerivedStreamInfo { event_type, filter })
            };

            // Compile to SASE+ pattern with stream resolution
            if let Some(pattern) = compiler::compile_to_sase_pattern_with_resolver(
                source,
                followed_by_clauses,
                negation_clauses,
                global_within,
                &stream_resolver,
            ) {
                let mut engine = SaseEngine::new(pattern);

                // Wire up expression evaluator for Predicate::Expr support
                engine.set_evaluator(std::sync::Arc::new(super::evaluator::RuntimeExprEvaluator));

                // Apply explicit partition_by to SASE engine (NOT auto-inferred keys,
                // which are high-cardinality join keys that would degrade SASE performance)
                if let Some(ref key) = partition_key {
                    engine = engine.with_partition_by(key.clone());
                }

                // Apply explicit selection / emission mode operators
                engine = apply_sase_modes(engine, selection_mode, emission_mode);

                // Add global negation conditions
                for clause in negation_clauses {
                    let predicate = clause
                        .filter
                        .as_ref()
                        .and_then(compiler::expr_to_sase_predicate);
                    engine.add_negation(clause.event_type.clone(), predicate);
                }

                info!("Created SASE+ engine for sequence pattern");
                Some(engine)
            } else {
                warn!("Failed to compile SASE+ pattern");
                None
            }
        } else if is_pattern_ref {
            // Named pattern reference: compile the pattern's SasePatternExpr directly
            let pattern_name = match source {
                StreamSource::Ident(name) => name,
                _ => unreachable!(),
            };
            let named_pattern = &self.patterns[pattern_name];

            // Extract within duration from the pattern declaration
            let within_duration = named_pattern.within.as_ref().and_then(|expr| {
                if let varpulis_core::ast::Expr::Duration(ns) = expr {
                    Some(std::time::Duration::from_nanos(*ns))
                } else {
                    None
                }
            });

            // Extract partition key from the pattern declaration
            if let Some(varpulis_core::ast::Expr::Ident(field)) =
                named_pattern.partition_by.as_ref()
            {
                *partition_key = Some(field.clone());
            }

            // Add Sequence operation marker
            runtime_ops.insert(0, RuntimeOp::Sequence);

            // Compile the named pattern expression to a SASE pattern
            if let Some(pattern) =
                compiler::compile_sase_pattern_expr(&named_pattern.expr, within_duration)
            {
                let mut engine = SaseEngine::new(pattern);

                // Wire up expression evaluator for Predicate::Expr support
                engine.set_evaluator(std::sync::Arc::new(super::evaluator::RuntimeExprEvaluator));

                if let Some(ref key) = partition_key {
                    engine = engine.with_partition_by(key.clone());
                }

                // Apply explicit selection / emission mode operators
                engine = apply_sase_modes(engine, selection_mode, emission_mode);

                info!("Created SASE+ engine from named pattern '{}'", pattern_name);
                Some(engine)
            } else {
                warn!(
                    "Failed to compile named pattern '{}' to SASE+ engine",
                    pattern_name
                );
                None
            }
        } else {
            None
        }
    }

    /// Build the PST Pattern Markov Chain forecaster from a `.forecast()` spec.
    ///
    /// Requires that a SASE engine was already compiled (sequence pattern must exist).
    /// Returns `Ok(Some(PatternMarkovChain))` on success, or an error if no SASE engine
    /// is available.
    fn compile_pst_forecaster(
        &self,
        spec: &varpulis_core::ast::ForecastSpec,
        sase_engine: &Option<SaseEngine>,
        global_within: Option<std::time::Duration>,
        sequence_event_types: &[String],
        forecast_insert_idx: Option<usize>,
        runtime_ops: &mut Vec<RuntimeOp>,
    ) -> Result<Option<crate::pst::PatternMarkovChain>, super::error::EngineError> {
        if sase_engine.is_none() {
            return Err(super::error::EngineError::Compilation(
                ".forecast() requires a sequence pattern (use -> followed-by operators)".into(),
            ));
        }

        // Resolve mode preset first, then allow explicit params to override
        let mode_str = match &spec.mode {
            Some(varpulis_core::ast::Expr::Str(s)) => Some(s.as_str()),
            _ => None,
        };

        // Mode presets: defaults that can be overridden by explicit params
        let (
            mode_confidence,
            mode_warmup,
            mode_max_depth,
            mode_hawkes,
            mode_conformal,
            mode_adaptive,
        ) = match mode_str {
            Some("fast") => (0.5, 50u64, 3usize, false, false, false),
            Some("accurate") => (0.5, 200, 5, true, true, true),
            // "balanced" or default
            _ => (0.5, 100, 3, true, true, true),
        };

        // Extract forecast parameters — explicit params override mode defaults
        let confidence = match &spec.confidence {
            Some(varpulis_core::ast::Expr::Float(f)) => *f,
            Some(varpulis_core::ast::Expr::Int(i)) => *i as f64,
            _ => mode_confidence,
        };
        let horizon_ns = match &spec.horizon {
            Some(varpulis_core::ast::Expr::Duration(ns)) => *ns,
            _ => global_within.map_or(300_000_000_000, |d| d.as_nanos() as u64),
        };
        let warmup = match &spec.warmup {
            Some(varpulis_core::ast::Expr::Int(n)) => *n as u64,
            _ => mode_warmup,
        };
        let max_depth = match &spec.max_depth {
            Some(varpulis_core::ast::Expr::Int(n)) => *n as usize,
            _ => mode_max_depth,
        };
        let hawkes = match &spec.hawkes {
            Some(varpulis_core::ast::Expr::Bool(b)) => *b,
            _ => mode_hawkes,
        };
        let conformal = match &spec.conformal {
            Some(varpulis_core::ast::Expr::Bool(b)) => *b,
            _ => mode_conformal,
        };
        let adaptive_warmup = mode_adaptive && spec.warmup.is_none();

        // Build NFA transition map from SASE engine
        let sase = sase_engine.as_ref().unwrap();
        let nfa = sase.nfa();
        let num_states = nfa.states.len();
        let accept_states = nfa.accept_states.clone();

        // Collect event types and build transition map
        let pst_config = crate::pst::PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        };
        let pmc_config = crate::pst::PMCConfig {
            confidence_threshold: confidence,
            horizon_ns,
            warmup_events: warmup,
            max_simulation_steps: 1000,
            hawkes_enabled: hawkes,
            conformal_enabled: conformal,
            adaptive_warmup,
            ..Default::default()
        };

        // Build NFA transitions: for each state, map symbol_id -> next_state
        let mut nfa_transitions = vec![rustc_hash::FxHashMap::default(); num_states];
        let mut nfa_event_types: Vec<String> = Vec::new();

        // Register event types and build symbol map
        let mut temp_pst = crate::pst::PredictionSuffixTree::new(crate::pst::PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        });
        for et in sequence_event_types {
            temp_pst.register_symbol(et);
            if !nfa_event_types.contains(et) {
                nfa_event_types.push(et.clone());
            }
        }

        // Extract transitions from NFA states.
        // SASE run current_state means "this state's event was already matched".
        // The transition label is the NEXT state's event_type (what's needed
        // to advance from current_state to the next state).
        for state in &nfa.states {
            for &next in &state.transitions {
                let next_state = &nfa.states[next];
                if let Some(ref next_event_type) = next_state.event_type {
                    if let Some(sym_id) = temp_pst.symbol_id(next_event_type) {
                        nfa_transitions[state.id].insert(sym_id, next);
                    }
                }
            }
        }

        let pmc = crate::pst::PatternMarkovChain::new(
            &nfa_event_types,
            nfa_transitions,
            accept_states,
            num_states,
            pst_config,
            pmc_config,
        );

        // Insert Forecast op at the position where .forecast() appeared in the
        // VPL source (after Sequence but BEFORE any downstream .where()/.emit()).
        // It reads the raw event from `last_raw_event` (set before pipeline
        // execution) so it can learn from every event even when Sequence
        // clears current_events.
        let forecast_config = ForecastConfig {
            confidence_threshold: confidence,
            horizon_ns,
            warmup_events: warmup,
            max_depth,
            hawkes,
            conformal,
        };
        // Sequence was inserted at position 0 after forecast_insert_idx was
        // recorded, shifting all indices by 1.  Account for that offset.
        let insert_pos = forecast_insert_idx.map_or(runtime_ops.len(), |i| i + 1);
        runtime_ops.insert(insert_pos, RuntimeOp::Forecast(forecast_config));

        info!(
            "Created PST forecaster for pattern forecasting ({} event types, max_depth={}, warmup={}, mode={}, adaptive={})",
            nfa_event_types.len(),
            max_depth,
            warmup,
            mode_str.unwrap_or("balanced"),
            adaptive_warmup
        );

        Ok(Some(pmc))
    }

    /// Extract join keys from join clauses and operations
    /// Returns a map of source_name -> join_key_field
    pub(super) fn extract_join_keys(
        &self,
        clauses: &[varpulis_core::ast::JoinClause],
        ops: &[StreamOp],
    ) -> FxHashMap<String, String> {
        let mut join_keys: FxHashMap<String, String> = FxHashMap::default();

        // First check clauses for on conditions
        for clause in clauses {
            if let Some(ref on_expr) = clause.on {
                if let Some((source, field)) = self.extract_field_from_expr(on_expr, &clause.source)
                {
                    join_keys.insert(source, field);
                }
            }
        }

        // Then check operations for StreamOp::On
        for op in ops {
            if let StreamOp::On(expr) = op {
                // Parse expressions like: EMA12.symbol == EMA26.symbol
                // or: A.key == B.key and B.key == C.key
                self.extract_join_keys_from_expr(expr, &mut join_keys);
            }
        }

        // Normalize join key source names to match clause source names.
        // The .on() expression may use event type names (e.g., "Transaction") while
        // the clause sources use stream names (e.g., "Transactions").
        let clause_sources: Vec<String> = clauses.iter().map(|c| c.source.clone()).collect();
        let mut normalized_keys: FxHashMap<String, String> = FxHashMap::default();
        for (src, field) in &join_keys {
            if clause_sources.contains(src) {
                // Already matches a clause source
                normalized_keys.insert(src.clone(), field.clone());
            } else {
                // Try to find a clause source whose underlying event type matches
                let mut found = false;
                for clause in clauses {
                    if let Some(stream_def) = self.streams.get(&clause.source) {
                        let matches = match &stream_def.source {
                            RuntimeSource::EventType(et) => et == src,
                            RuntimeSource::Stream(s) => s == src,
                            _ => false,
                        };
                        if matches {
                            normalized_keys.insert(clause.source.clone(), field.clone());
                            found = true;
                            break;
                        }
                    }
                }
                if !found {
                    normalized_keys.insert(src.clone(), field.clone());
                }
            }
        }

        // If no join keys found, use "symbol" as default (common join key)
        if normalized_keys.is_empty() {
            for clause in clauses {
                normalized_keys.insert(clause.source.clone(), "symbol".to_string());
            }
        }

        normalized_keys
    }

    /// Extract join keys from an expression (e.g., EMA12.symbol == EMA26.symbol)
    fn extract_join_keys_from_expr(
        &self,
        expr: &varpulis_core::ast::Expr,
        keys: &mut FxHashMap<String, String>,
    ) {
        use varpulis_core::ast::{BinOp, Expr};

        if let Expr::Binary { op, left, right } = expr {
            match op {
                BinOp::Eq => {
                    // Extract source.field from both sides
                    if let (Some((src1, field1)), Some((src2, field2))) = (
                        self.extract_source_field(left),
                        self.extract_source_field(right),
                    ) {
                        keys.insert(src1, field1);
                        keys.insert(src2, field2);
                    }
                }
                BinOp::And => {
                    // Recursively process both sides for compound conditions
                    self.extract_join_keys_from_expr(left, keys);
                    self.extract_join_keys_from_expr(right, keys);
                }
                _ => {}
            }
        }
    }

    /// Extract source name and field name from an expression like EMA12.symbol
    fn extract_source_field(
        &self,
        expr_node: &varpulis_core::ast::Expr,
    ) -> Option<(String, String)> {
        use varpulis_core::ast::Expr;

        match expr_node {
            Expr::Member { expr, member } => {
                if let Expr::Ident(source) = expr.as_ref() {
                    return Some((source.clone(), member.clone()));
                }
            }
            Expr::Ident(name) => {
                // Simple identifier - might be just a field name
                // Return as field only, source will be inferred
                return Some((String::new(), name.clone()));
            }
            _ => {}
        }
        None
    }

    /// Extract a field from an expression for a specific source
    fn extract_field_from_expr(
        &self,
        expr: &varpulis_core::ast::Expr,
        source: &str,
    ) -> Option<(String, String)> {
        use varpulis_core::ast::{BinOp, Expr};

        if let Expr::Binary {
            op: BinOp::Eq,
            left,
            right,
        } = expr
        {
            // Check left side
            if let Some((src, field)) = self.extract_source_field(left) {
                if src == source || src.is_empty() {
                    return Some((source.to_string(), field));
                }
            }
            // Check right side
            if let Some((src, field)) = self.extract_source_field(right) {
                if src == source || src.is_empty() {
                    return Some((source.to_string(), field));
                }
            }
        }
        None
    }

    /// Extract window duration from operations
    pub(super) fn extract_window_duration(&self, ops: &[StreamOp]) -> Duration {
        for op in ops {
            if let StreamOp::Window(args) = op {
                if let varpulis_core::ast::Expr::Duration(ns) = &args.duration {
                    return Duration::nanoseconds(*ns as i64);
                }
            }
        }
        // Default to 1 minute if no window specified
        Duration::minutes(1)
    }
}

/// Build an `Expr` from a `ConfigValue::Concat` parts list.
///
/// Converts `["prefix-", field, "-suffix"]` into nested
/// `Expr::Binary { op: Add, left, right }` tree (left-associative).
fn build_concat_expr(parts: &[varpulis_core::ast::ConfigValue]) -> Expr {
    let mut iter = parts.iter().map(|p| match p {
        varpulis_core::ast::ConfigValue::Str(s) => Expr::Str(s.clone()),
        varpulis_core::ast::ConfigValue::Ident(f) => Expr::Ident(f.clone()),
        _ => Expr::Str(String::new()),
    });
    let first = iter.next().unwrap_or(Expr::Str(String::new()));
    iter.fold(first, |acc, part| Expr::Binary {
        op: varpulis_core::ast::BinOp::Add,
        left: Box::new(acc),
        right: Box::new(part),
    })
}

/// Apply user-specified SASE+ selection and emission modes to a freshly-built
/// engine. AST-level enums are mapped to runtime-level enums.
fn apply_sase_modes(
    mut engine: SaseEngine,
    selection: Option<varpulis_core::ast::SelectionMode>,
    emission: Option<varpulis_core::ast::EmissionMode>,
) -> SaseEngine {
    if let Some(s) = selection {
        let runtime_strategy = match s {
            varpulis_core::ast::SelectionMode::Strict => {
                crate::sase::SelectionStrategy::StrictContiguous
            }
            varpulis_core::ast::SelectionMode::Stnm => {
                crate::sase::SelectionStrategy::SkipTillNextMatch
            }
            varpulis_core::ast::SelectionMode::Stam => {
                crate::sase::SelectionStrategy::SkipTillAnyMatch
            }
        };
        engine = engine.with_strategy(runtime_strategy);
    }
    if let Some(e) = emission {
        let runtime_mode = match e {
            varpulis_core::ast::EmissionMode::Each => crate::sase::EmissionMode::Each,
            varpulis_core::ast::EmissionMode::Longest => crate::sase::EmissionMode::Longest,
            varpulis_core::ast::EmissionMode::Subsets => crate::sase::EmissionMode::Subsets,
        };
        engine = engine.with_emission_mode(runtime_mode);
    }
    engine
}

pub(super) const fn stream_op_name(op: &StreamOp) -> &'static str {
    match op {
        StreamOp::Where(_) => ".where()",
        StreamOp::Select(_) => ".select()",
        StreamOp::Window(_) => ".window()",
        StreamOp::Aggregate(_) => ".aggregate()",
        StreamOp::Having(_) => ".having()",
        StreamOp::PartitionBy(_) => ".partition_by()",
        StreamOp::OrderBy(_) => ".order_by()",
        StreamOp::Limit(_) => ".limit()",
        StreamOp::Distinct(_) => ".distinct()",
        StreamOp::Map(_) => ".map()",
        StreamOp::Filter(_) => ".filter()",
        StreamOp::Tap(_) => ".tap()",
        StreamOp::Print(_) => ".print()",
        StreamOp::Log(_) => ".log()",
        StreamOp::Emit { .. } => ".emit()",
        StreamOp::To { .. } => ".to()",
        StreamOp::ToExpr(_) => ".to()",
        StreamOp::Pattern(_) => ".pattern()",
        StreamOp::Concurrent(_) => ".concurrent()",
        StreamOp::Process(_) => ".process()",
        StreamOp::OnError(_) => ".on_error()",
        StreamOp::Collect => ".collect()",
        StreamOp::On(_) => ".on()",
        StreamOp::FollowedBy(_) => "-> (followed_by)",
        StreamOp::Within(_) => ".within()",
        StreamOp::Not(_) => ".not()",
        StreamOp::Fork(_) => ".fork()",
        StreamOp::Any(_) => ".any()",
        StreamOp::All => ".all()",
        StreamOp::First => ".first()",
        StreamOp::Context(_) => ".context()",
        StreamOp::Watermark(_) => ".watermark()",
        StreamOp::AllowedLateness(_) => ".allowed_lateness()",
        StreamOp::TrendAggregate(_) => ".trend_aggregate()",
        StreamOp::Score(_) => ".score()",
        StreamOp::Forecast(_) => ".forecast()",
        StreamOp::Enrich(_) => ".enrich()",
        StreamOp::Alert(_) => ".alert()",
        StreamOp::SelectionMode(_) => ".selection_mode()",
        StreamOp::EmissionMode(_) => ".emission_mode()",
    }
}

/// Extract the common field name from a cross-alias equality expression.
/// For `a.id == b.id`, returns `Some("id")`.
/// Handles both `Binary { Eq, Member, Member }` and `And` combinations.
pub(super) fn extract_equality_join_key(expr: &varpulis_core::ast::Expr) -> Option<String> {
    use varpulis_core::ast::{BinOp, Expr};
    match expr {
        Expr::Binary {
            op: BinOp::Eq,
            left,
            right,
        } => {
            // Check for Member { Ident(alias_a), field } == Member { Ident(alias_b), field }
            if let (
                Expr::Member {
                    expr: left_expr,
                    member: left_field,
                },
                Expr::Member {
                    expr: right_expr,
                    member: right_field,
                },
            ) = (left.as_ref(), right.as_ref())
            {
                if left_field == right_field {
                    // Ensure they're different aliases (cross-alias join)
                    if let (Expr::Ident(left_alias), Expr::Ident(right_alias)) =
                        (left_expr.as_ref(), right_expr.as_ref())
                    {
                        if left_alias != right_alias {
                            return Some(left_field.clone());
                        }
                    }
                }
            }
            None
        }
        Expr::Binary {
            op: BinOp::And,
            left,
            right,
        } => {
            // Try left side first, then right
            extract_equality_join_key(left).or_else(|| extract_equality_join_key(right))
        }
        _ => None,
    }
}

// =============================================================================
// Phase-2 fusion tests
// =============================================================================
//
// Compile a small VPL program shaped like scenario 02 and inspect the
// resulting `runtime_ops` to confirm that the compiler fuses the
// `partition_by + window + aggregate` triple into a single
// `PartitionedWindowedColumnarAggregate` op when the `arrow` feature
// is enabled. Also verify that an unsupported aggregate function (e.g.
// `stddev`) inhibits the fusion and falls back to the row path.

#[cfg(all(test, feature = "arrow"))]
mod arrow_fusion_tests {
    use crate::engine::Engine;

    fn compile(source: &str) -> Vec<&'static str> {
        // Returns the runtime-op summary names for the first stream in
        // the program, in declaration order.
        let mut engine = Engine::builder().build();
        let program =
            varpulis_parser::parse(source).unwrap_or_else(|e| panic!("parse failed: {e:?}"));
        engine
            .load(&program)
            .unwrap_or_else(|e| panic!("load failed: {e}"));
        let stream_names = engine.stream_names();
        let stream_name = stream_names.first().expect("at least one stream");
        let stream = engine.streams.get(*stream_name).expect("stream registered");
        stream
            .operations
            .iter()
            .map(|op| op.summary_name())
            .collect()
    }

    #[test]
    fn fuses_partition_window_aggregate_into_columnar_op() {
        // Scenario-02 shape: partition_by + tumbling window + aggregate
        // with all-columnar-supported funcs.
        let source = r"
            event Reading:
                ts: int
                device_id: str
                temperature: float

            stream DeviceAgg = Reading
                .partition_by(device_id)
                .window(1s)
                .aggregate(
                    s: sum(temperature),
                    a: avg(temperature),
                    mn: min(temperature),
                    mx: max(temperature)
                )
                .emit(device_id: device_id, s: s, a: a, mn: mn, mx: mx)
        ";
        let ops = compile(source);
        // The fused op replaces both the window and the partitioned
        // aggregate, so we expect ONE entry containing
        // "PartitionedWindowedColumnarAggregate" and NO entries
        // containing "Window" or "PartitionedAggregate".
        assert!(
            ops.contains(&"PartitionedWindowedColumnarAggregate"),
            "expected fused op in pipeline; got {ops:?}"
        );
        assert!(
            !ops.iter()
                .any(|n| *n == "Window" || *n == "PartitionedAggregate"),
            "fusion should remove Window and PartitionedAggregate; got {ops:?}"
        );
    }

    #[test]
    fn unsupported_aggregate_skips_fusion() {
        // `stddev` is not in the columnar-supported set, so the
        // compiler must NOT fuse — we should get a separate Window
        // and PartitionedAggregate op.
        let source = r"
            event Reading:
                ts: int
                device_id: str
                temperature: float

            stream DeviceAgg = Reading
                .partition_by(device_id)
                .window(1s)
                .aggregate(
                    s: sum(temperature),
                    sd: stddev(temperature)
                )
                .emit(device_id: device_id, s: s, sd: sd)
        ";
        let ops = compile(source);
        assert!(
            ops.contains(&"Window"),
            "stddev should leave the Window op intact; got {ops:?}"
        );
        assert!(
            ops.contains(&"PartitionedAggregate"),
            "stddev should fall back to PartitionedAggregate; got {ops:?}"
        );
        assert!(
            !ops.contains(&"PartitionedWindowedColumnarAggregate"),
            "fusion must not occur for stddev; got {ops:?}"
        );
    }

    #[test]
    fn no_partition_by_uses_phase_3b_fusion() {
        // Phase 3b extends fusion to the non-partitioned shape as well:
        // `Window(Tumbling) + Aggregate` (no partition_by) becomes
        // `WindowedColumnarAggregate`, the non-partitioned mirror of
        // phase 2's `PartitionedWindowedColumnarAggregate`.
        let source = r"
            event Reading:
                ts: int
                temperature: float

            stream Agg = Reading
                .window(1s)
                .aggregate(
                    s: sum(temperature),
                    a: avg(temperature)
                )
                .emit(s: s, a: a)
        ";
        let ops = compile(source);
        assert!(
            ops.contains(&"WindowedColumnarAggregate"),
            "non-partitioned shape should fuse into WindowedColumnarAggregate; got {ops:?}"
        );
        assert!(
            !ops.contains(&"Window"),
            "Window op should be subsumed by the fused op; got {ops:?}"
        );
        assert!(
            !ops.contains(&"Aggregate"),
            "raw Aggregate should be subsumed by the fused op; got {ops:?}"
        );
        assert!(
            !ops.contains(&"PartitionedWindowedColumnarAggregate"),
            "partitioned fusion should not apply here; got {ops:?}"
        );
    }

    #[test]
    fn fused_op_produces_correct_results() {
        // End-to-end test: build the fused op directly, feed it events
        // that span 3 bins for 2 devices, and check the drained
        // results match what the row-path PartitionedAggregator would
        // produce on the same data per-bin.
        use std::sync::Arc;

        use varpulis_core::Value;

        use crate::aggregation::{Aggregator, Avg, Count, Max, Min, Sum};
        use crate::engine::types::PartitionedWindowedColumnarAggregateState;
        use crate::event::{Event, SharedEvent};

        let agg = Aggregator::new()
            .add("s", Box::new(Sum), Some("value".to_string()))
            .add("a", Box::new(Avg), Some("value".to_string()))
            .add("mn", Box::new(Min), Some("value".to_string()))
            .add("mx", Box::new(Max), Some("value".to_string()))
            .add("c", Box::new(Count), None);

        let mut state = PartitionedWindowedColumnarAggregateState::try_new(
            "device_id".to_string(),
            1000, // 1-second bins
            &agg,
        )
        .expect("supported funcs only");

        let make_ev = |ts_ms: i64, dev: &str, v: f64| -> SharedEvent {
            Arc::new(
                Event::new("Reading")
                    .with_timestamp(
                        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_ms).unwrap(),
                    )
                    .with_field("device_id", Value::Str(dev.into()))
                    .with_field("value", Value::Float(v)),
            )
        };

        // Bin 0 (ts 0..1000): d0 sees 10 + 30, d1 sees 20
        // Bin 1 (ts 1000..2000): d0 sees 100
        // Then advance watermark past bin 1 with a bin 2 event so both
        // bin 0 and bin 1 flush.
        let _ = state.ingest_and_flush(&[
            make_ev(0, "d0", 10.0),
            make_ev(500, "d1", 20.0),
            make_ev(700, "d0", 30.0),
        ]);
        // Bin 0 hasn't flushed yet — watermark = 700, bin 0 ends at 1000
        let _ = state.ingest_and_flush(&[make_ev(1100, "d0", 100.0)]);
        // Now watermark = 1100, bin 0 (end 1000) flushes.
        // Push past bin 1 to flush bin 1 too.
        let flushed_for_bin_1 = state.ingest_and_flush(&[make_ev(2100, "d0", 999.0)]);
        // After this call, bin 0 already flushed in the previous call;
        // bin 1 (end 2000) is now ≤ watermark 2100 so it flushes here.
        // bin 2 (end 3000) is still active.

        // The flushed_for_bin_1 should contain bin 1's results.
        // Reconstruct the union of all flushes by force-flushing the rest.
        let remainder = state.flush_all();

        // Combine all results.
        let mut all_flushed: Vec<(i64, String, indexmap::IndexMap<String, Value>)> = Vec::new();
        // We'd need the bin 0 results too — let's just rerun fresh
        // and capture every flush deterministically.
        let mut state2 =
            PartitionedWindowedColumnarAggregateState::try_new("device_id".to_string(), 1000, &agg)
                .unwrap();
        all_flushed.extend(state2.ingest_and_flush(&[
            make_ev(0, "d0", 10.0),
            make_ev(500, "d1", 20.0),
            make_ev(700, "d0", 30.0),
        ]));
        all_flushed.extend(state2.ingest_and_flush(&[make_ev(1100, "d0", 100.0)]));
        all_flushed.extend(state2.ingest_and_flush(&[make_ev(2100, "d0", 999.0)]));
        all_flushed.extend(state2.flush_all());

        // Bin 0: d0 → sum 40, count 2; d1 → sum 20, count 1
        // Bin 1: d0 → sum 100, count 1
        // Bin 2: d0 → sum 999, count 1
        let mut by_bin: std::collections::BTreeMap<
            (i64, String),
            indexmap::IndexMap<String, Value>,
        > = Default::default();
        for (bin, key, result) in all_flushed {
            by_bin.insert((bin, key), result);
        }

        let bin0_d0 = &by_bin[&(0, "d0".to_string())];
        assert_eq!(bin0_d0["s"], Value::Float(40.0));
        assert_eq!(bin0_d0["c"], Value::Int(2));

        let bin0_d1 = &by_bin[&(0, "d1".to_string())];
        assert_eq!(bin0_d1["s"], Value::Float(20.0));
        assert_eq!(bin0_d1["c"], Value::Int(1));

        let bin1_d0 = &by_bin[&(1000, "d0".to_string())];
        assert_eq!(bin1_d0["s"], Value::Float(100.0));

        let bin2_d0 = &by_bin[&(2000, "d0".to_string())];
        assert_eq!(bin2_d0["s"], Value::Float(999.0));

        // Silence dead-code warnings on the first state's outputs.
        let _ = flushed_for_bin_1;
        let _ = remainder;
    }

    #[test]
    fn phase_3b_fused_op_produces_correct_results() {
        // Phase-3b end-to-end: build the non-partitioned fused op
        // directly, feed it events across 3 bins, and verify the
        // flushed outputs match hand-computed sum/avg/min/max/count.
        use std::sync::Arc;

        use varpulis_core::Value;

        use crate::aggregation::{Aggregator, Avg, Count, Max, Min, Sum};
        use crate::engine::types::WindowedColumnarAggregateState;
        use crate::event::{Event, SharedEvent};

        let agg = Aggregator::new()
            .add("s", Box::new(Sum), Some("value".to_string()))
            .add("a", Box::new(Avg), Some("value".to_string()))
            .add("mn", Box::new(Min), Some("value".to_string()))
            .add("mx", Box::new(Max), Some("value".to_string()))
            .add("c", Box::new(Count), None);

        let mut state =
            WindowedColumnarAggregateState::try_new(1000, &agg).expect("supported funcs only");

        let make_ev = |ts_ms: i64, v: f64| -> SharedEvent {
            Arc::new(
                Event::new("Reading")
                    .with_timestamp(
                        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_ms).unwrap(),
                    )
                    .with_field("value", Value::Float(v)),
            )
        };

        // Bin 0 (ts 0..1000): values 10, 20, 30 → sum 60, avg 20, min 10, max 30, count 3
        // Bin 1 (ts 1000..2000): value 100 → sum 100, avg 100, min 100, max 100, count 1
        // Bin 2 (ts 2000..3000): value 999 → still active at end
        let mut all_flushed: Vec<(i64, indexmap::IndexMap<String, Value>)> = Vec::new();
        all_flushed.extend(state.ingest_and_flush(&[
            make_ev(0, 10.0),
            make_ev(500, 20.0),
            make_ev(700, 30.0),
        ]));
        all_flushed.extend(state.ingest_and_flush(&[make_ev(1100, 100.0)]));
        all_flushed.extend(state.ingest_and_flush(&[make_ev(2100, 999.0)]));
        all_flushed.extend(state.flush_all());

        let by_bin: std::collections::BTreeMap<i64, indexmap::IndexMap<String, Value>> =
            all_flushed.into_iter().collect();

        let bin0 = &by_bin[&0];
        assert_eq!(bin0["s"], Value::Float(60.0));
        assert_eq!(bin0["a"], Value::Float(20.0));
        assert_eq!(bin0["mn"], Value::Float(10.0));
        assert_eq!(bin0["mx"], Value::Float(30.0));
        assert_eq!(bin0["c"], Value::Int(3));

        let bin1 = &by_bin[&1000];
        assert_eq!(bin1["s"], Value::Float(100.0));
        assert_eq!(bin1["c"], Value::Int(1));

        let bin2 = &by_bin[&2000];
        assert_eq!(bin2["s"], Value::Float(999.0));
        assert_eq!(bin2["c"], Value::Int(1));
    }
}
