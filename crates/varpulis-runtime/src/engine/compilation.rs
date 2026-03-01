//! Program compilation: stream registration, operator compilation, and join key extraction.
//!
//! This module contains the `impl Engine` methods that compile VPL stream declarations
//! into runtime stream definitions, including SASE+ pattern compilation, Hamlet
//! aggregator setup, and PST forecaster construction.

use crate::aggregation::Aggregator;
use crate::join::JoinBuffer;
use crate::sase::SaseEngine;
use crate::window::{
    CountWindow, PartitionedSessionWindow, PartitionedSlidingWindow, PartitionedTumblingWindow,
    SessionWindow, SlidingCountWindow, SlidingWindow, TumblingWindow,
};
use chrono::Duration;
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};
use varpulis_core::ast::{StreamOp, StreamSource};

use super::compiler;
use super::pattern_analyzer;
use super::types::{
    ConcurrentConfig, DistinctState, EmitConfig, EmitExprConfig, EnrichConfig, ForecastConfig,
    LimitState, LogConfig, MergeSource, PartitionedAggregatorState,
    PartitionedSlidingCountWindowState, PartitionedWindowState, PatternConfig, PrintConfig,
    RuntimeOp, RuntimeSource, SelectConfig, SourceBinding, StreamDefinition, TimerConfig, ToConfig,
    TrendAggregateConfig, WindowType,
};
use super::Engine;

impl Engine {
    pub(super) fn register_stream(
        &mut self,
        name: &str,
        source: &StreamSource,
        ops: &[StreamOp],
    ) -> Result<(), super::error::EngineError> {
        // Extract context assignments from stream ops
        for (emit_idx, op) in ops.iter().enumerate() {
            match op {
                StreamOp::Context(ctx_name) => {
                    self.context_map
                        .assign_stream(name.to_string(), ctx_name.clone());
                }
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
                        StreamSource::IdentWithAlias { name: et, .. } => Some(et.as_str()),
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
                let timer_event_type = format!("Timer_{}", name);

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

        // Build enrichment provider if any .enrich() ops reference a connector
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
                    let provider = crate::enrichment::create_provider(conn_config)
                        .map_err(|e| format!("Failed to create enrichment provider: {}", e))?;
                    let cache_ttl = config
                        .cache_ttl_ns
                        .map(std::time::Duration::from_nanos)
                        .unwrap_or(std::time::Duration::from_secs(300));
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
                enrichment,
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
        String,
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
            StreamSource::IdentWithAlias { name, .. } | StreamSource::AllWithAlias { name, .. }
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
                        .map_err(|e| format!("Failed to load ONNX model: {}", e))?;
                        runtime_ops.push(RuntimeOp::Score(super::types::ScoreConfig {
                            model: std::sync::Arc::new(model),
                            input_fields: spec.inputs.clone(),
                            output_fields: spec.outputs.clone(),
                            batch_size: spec.batch_size.max(1),
                        }));
                        continue;
                    }
                    #[cfg(not(feature = "onnx"))]
                    return Err(format!(
                        ".score() operator requires the 'onnx' feature. \
                         Rebuild with: cargo build --features onnx (model: {})",
                        spec.model_path
                    ));
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
                                        runtime_ops.push(RuntimeOp::Window(
                                            WindowType::PartitionedSliding(
                                                PartitionedSlidingWindow::new(
                                                    key.clone(),
                                                    duration,
                                                    slide,
                                                ),
                                            ),
                                        ));
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
                                    runtime_ops.push(RuntimeOp::Window(WindowType::Sliding(
                                        SlidingWindow::new(duration, slide),
                                    )));
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
                    // If we have a partition key, use partitioned aggregate
                    if let Some(ref key) = partition_key {
                        runtime_ops.push(RuntimeOp::PartitionedAggregate(
                            PartitionedAggregatorState::new(key.clone(), aggregator),
                        ));
                    } else {
                        runtime_ops.push(RuntimeOp::Aggregate(aggregator));
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
                    let sink_key = if let Some(ref topic) = topic_override {
                        format!("{}::{}", connector_name, topic)
                    } else {
                        connector_name.clone()
                    };
                    runtime_ops.push(RuntimeOp::To(ToConfig {
                        connector_name: connector_name.clone(),
                        topic_override,
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
                            return Err(
                                ".limit() requires an integer argument (e.g., .limit(100))"
                                    .to_string(),
                            );
                        }
                    };
                    runtime_ops.push(RuntimeOp::Limit(LimitState { max, count: 0 }));
                }
                StreamOp::First => {
                    // .first() is shorthand for .limit(1)
                    runtime_ops.push(RuntimeOp::Limit(LimitState { max: 1, count: 0 }));
                }
                StreamOp::Map(_) => {
                    return Err(
                        ".map() is not supported — use .emit() for field projection or .process() for arbitrary transformation"
                            .to_string(),
                    );
                }
                StreamOp::Tap(_) => {
                    return Err(
                        ".tap() is not yet implemented — use .print() or .log() for debugging"
                            .to_string(),
                    );
                }
                StreamOp::Collect => {
                    return Err(
                        ".collect() is not yet implemented — use .window() with .aggregate() for batching"
                            .to_string(),
                    );
                }
                StreamOp::OnError(_) => {
                    return Err(
                        ".on_error() is not yet implemented — errors are logged via tracing"
                            .to_string(),
                    );
                }
                StreamOp::Fork(_) | StreamOp::Any(_) | StreamOp::All => {
                    return Err(
                        ".fork()/.any()/.all() are not yet implemented — use multiple streams for parallel processing"
                            .to_string(),
                    );
                }
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
                            .map_err(|e| format!("Failed to create thread pool: {}", e))?,
                    );

                    runtime_ops.push(RuntimeOp::Concurrent(ConcurrentConfig {
                        workers,
                        partition_key,
                        thread_pool,
                    }));
                }
                StreamOp::OrderBy(_) => {
                    return Err(
                        ".order_by() is not yet implemented — use .window() with .aggregate() for ordered output"
                            .to_string(),
                    );
                }
                StreamOp::ToExpr(_) => {
                    return Err(
                        ".to(expr) is not supported — use .to(ConnectorName, topic: \"...\") instead"
                            .to_string(),
                    );
                }
                other => {
                    return Err(format!(
                        "unsupported stream operation: {}",
                        stream_op_name(other)
                    ));
                }
            }
        }

        // Check if we're in trend aggregation mode (Hamlet) or detection mode (SASE)
        if let Some(ref agg_items) = trend_agg_items {
            // === Trend Aggregation Mode (Hamlet) ===
            // Build Hamlet aggregator instead of SASE engine.

            let event_types = pattern_analyzer::extract_event_types(source, &followed_by_clauses);
            let kleene_info = pattern_analyzer::extract_kleene_info(source, &followed_by_clauses);
            let window_ms = pattern_analyzer::extract_within_ms(within_expr_for_hamlet.as_ref());

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
            for clause in &followed_by_clauses {
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
                // The Kleene state in the template is at position ki.position
                // (the state index after the transition for that event type)
                let state = ki.position as u16;
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
                .map(|(_, agg)| *agg)
                .unwrap_or(crate::greta::GretaAggregate::CountTrends);

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

            // Insert TrendAggregate op at the beginning (replaces Sequence)
            runtime_ops.insert(
                0,
                RuntimeOp::TrendAggregate(TrendAggregateConfig { fields, query_id }),
            );

            info!(
                "Created Hamlet aggregator for trend aggregation ({} event types, {} Kleene patterns)",
                event_types.len(),
                kleene_info.len()
            );

            return Ok((
                runtime_ops,
                None,
                sequence_event_types,
                Some(aggregator),
                None,
            ));
        }

        // === Forecast Mode (PST + SASE) ===
        // If .forecast() is present, we need a sequence pattern (SASE engine)
        // and must not have .trend_aggregate()
        let mut pst_forecaster = None;

        // === Detection Mode (SASE) ===
        // Build SASE+ engine if we have sequence patterns or a named pattern reference
        let is_pattern_ref =
            matches!(source, StreamSource::Ident(name) if self.patterns.contains_key(name));
        let sase_engine = if !followed_by_clauses.is_empty()
            || matches!(source, StreamSource::Sequence(_))
        {
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
                &followed_by_clauses,
                &negation_clauses,
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

                // Add global negation conditions
                for clause in &negation_clauses {
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
                partition_key = Some(field.clone());
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
        };

        // === Build PST Forecaster if .forecast() specified ===
        if let Some(spec) = forecast_spec {
            if sase_engine.is_none() {
                return Err(
                    ".forecast() requires a sequence pattern (use -> followed-by operators)"
                        .to_string(),
                );
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
                _ => global_within
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(300_000_000_000),
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
            for et in &sequence_event_types {
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
            let insert_pos = forecast_insert_idx
                .map(|i| i + 1)
                .unwrap_or(runtime_ops.len());
            runtime_ops.insert(insert_pos, RuntimeOp::Forecast(forecast_config));

            pst_forecaster = Some(pmc);

            info!(
                "Created PST forecaster for pattern forecasting ({} event types, max_depth={}, warmup={}, mode={}, adaptive={})",
                nfa_event_types.len(),
                max_depth,
                warmup,
                mode_str.unwrap_or("balanced"),
                adaptive_warmup
            );
        }

        Ok((
            runtime_ops,
            sase_engine,
            sequence_event_types,
            None,
            pst_forecaster,
        ))
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
                return Some(("".to_string(), name.clone()));
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

pub(super) fn stream_op_name(op: &StreamOp) -> &'static str {
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
