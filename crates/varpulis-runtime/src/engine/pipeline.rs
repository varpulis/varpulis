//! Unified pipeline execution for the Varpulis engine
//!
//! This module provides a single `execute_pipeline` function that replaces
//! the previously triplicated pipeline logic (process_stream, process_join_result,
//! process_post_window).

use crate::event::{Event, SharedEvent};
use crate::sequence::SequenceContext;
use indexmap::IndexMap;
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::sync::Arc;
use tracing::warn;
use varpulis_core::Value;

use super::evaluator;
use super::types::*;

/// PERF: Static empty variables map for read-only operations (avoids allocation per call)
static EMPTY_VARS: std::sync::LazyLock<FxHashMap<String, Value>> =
    std::sync::LazyLock::new(FxHashMap::default);

/// Returns a reference to a shared empty variables map for read-only operations.
/// PERF: Avoids allocating a new HashMap for each expression evaluation.
#[inline]
fn empty_vars() -> &'static FxHashMap<String, Value> {
    &EMPTY_VARS
}

/// Flags indicating which operation types to skip during pipeline execution.
/// Different entry points (normal stream, join result, post-window) skip different ops.
#[derive(Default, Clone, Copy)]
pub(crate) struct SkipFlags {
    /// Skip non-partitioned Window ops (used by join processing - JoinBuffer handles windowing)
    pub window: bool,
    /// Skip Print/Log ops
    pub print_log: bool,
    /// Skip Sequence/AttentionWindow/Pattern ops
    pub sequence_pattern: bool,
    /// Skip WhereClosure ops (used by post-window processing)
    pub where_closure: bool,
    /// Skip all window-type ops (Window, PartitionedWindow, PartitionedSlidingCountWindow)
    pub all_windows: bool,
}

impl SkipFlags {
    /// No skipping - used for normal stream processing
    pub fn none() -> Self {
        Self::default()
    }

    /// For join result processing - skip non-partitioned windows and sequence/pattern ops
    pub fn for_join() -> Self {
        Self {
            window: true,
            print_log: true,
            sequence_pattern: true,
            ..Default::default()
        }
    }

    /// For post-window processing - skip all windows, where closures, sequence/pattern ops
    pub fn for_post_window() -> Self {
        Self {
            all_windows: true,
            where_closure: true,
            print_log: false, // print is allowed in post-window
            sequence_pattern: true,
            ..Default::default()
        }
    }
}

/// Execute operations `stream.operations[start_idx..]` on the given events.
///
/// This is the single unified pipeline that replaces:
/// - `process_stream_with_functions` (start_idx = 0, SkipFlags::none())
/// - `process_join_result` (start_idx = 0, SkipFlags::for_join())
/// - `process_post_window` (start_idx = window_idx + 1, SkipFlags::for_post_window())
pub(crate) async fn execute_pipeline(
    stream: &mut StreamDefinition,
    initial_events: Vec<SharedEvent>,
    start_idx: usize,
    skip_flags: SkipFlags,
    functions: &FxHashMap<String, UserFunction>,
    sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
) -> Result<StreamProcessResult, String> {
    let mut current_events = initial_events;
    let mut emitted_events: Vec<SharedEvent> = Vec::new();

    // Iterate operations starting from start_idx
    // We need the index for RuntimeOp::Sequence which accesses stream.sase_engine
    for op in &mut stream.operations[start_idx..] {
        // Check skip flags
        if should_skip_op(op, skip_flags) {
            continue;
        }

        execute_op(
            op,
            &stream.name,
            &mut stream.sase_engine,
            stream.attention_window.as_ref(),
            &mut current_events,
            &mut emitted_events,
            functions,
            sinks,
        )
        .await?;

        if current_events.is_empty() {
            return Ok(StreamProcessResult {
                emitted_events,
                output_events: vec![],
            });
        }
    }

    // Rename event_type to stream name for downstream routing
    let output_events = current_events
        .into_iter()
        .map(|e| {
            let mut owned = (*e).clone();
            owned.event_type = stream.name.clone().into();
            Arc::new(owned)
        })
        .collect();

    Ok(StreamProcessResult {
        emitted_events,
        output_events,
    })
}

/// Check if an operation should be skipped based on flags
fn should_skip_op(op: &RuntimeOp, flags: SkipFlags) -> bool {
    match op {
        // Window skipping
        RuntimeOp::Window(_) => flags.window || flags.all_windows,
        RuntimeOp::PartitionedWindow(_) | RuntimeOp::PartitionedSlidingCountWindow(_) => {
            flags.all_windows
        }

        // Print/Log skipping
        RuntimeOp::Print(_) | RuntimeOp::Log(_) => flags.print_log,

        // Sequence/Pattern skipping
        RuntimeOp::Sequence | RuntimeOp::AttentionWindow(_) | RuntimeOp::Pattern(_) => {
            flags.sequence_pattern
        }

        // WhereClosure skipping
        RuntimeOp::WhereClosure(_) => flags.where_closure,

        // Other ops are never skipped by flags
        _ => false,
    }
}

/// Execute a single RuntimeOp on the current event batch.
/// Mutates `current_events` in place and appends to `emitted_events`.
#[allow(clippy::too_many_arguments)]
async fn execute_op(
    op: &mut RuntimeOp,
    stream_name: &str,
    sase_engine: &mut Option<crate::sase::SaseEngine>,
    attention_window: Option<&crate::attention::AttentionWindow>,
    current_events: &mut Vec<SharedEvent>,
    emitted_events: &mut Vec<SharedEvent>,
    functions: &FxHashMap<String, UserFunction>,
    sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
) -> Result<(), String> {
    match op {
        RuntimeOp::WhereClosure(predicate) => {
            current_events.retain(|e| predicate(e));
        }

        RuntimeOp::WhereExpr(expr) => {
            // PERF: Use static empty context to avoid allocation
            current_events.retain(|e| {
                evaluator::eval_expr_with_functions(
                    expr,
                    e.as_ref(),
                    SequenceContext::empty(),
                    functions,
                    empty_vars(),
                )
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            });
        }

        RuntimeOp::Window(window) => {
            let mut window_results = Vec::new();
            for event in current_events.drain(..) {
                match window {
                    WindowType::Tumbling(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                    WindowType::Sliding(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results = window_events;
                        }
                    }
                    WindowType::Count(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                    WindowType::SlidingCount(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results = window_events;
                        }
                    }
                    WindowType::PartitionedTumbling(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                    WindowType::PartitionedSliding(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results = window_events;
                        }
                    }
                    WindowType::Session(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                    WindowType::PartitionedSession(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                }
            }
            *current_events = window_results;
        }

        RuntimeOp::PartitionedWindow(state) => {
            let mut window_results = Vec::new();
            for event in current_events.drain(..) {
                if let Some(completed) = state.add(event) {
                    window_results.extend(completed);
                }
            }
            *current_events = window_results;
        }

        RuntimeOp::PartitionedSlidingCountWindow(state) => {
            let mut window_results = Vec::new();
            for event in current_events.drain(..) {
                if let Some(completed) = state.add(event) {
                    window_results.extend(completed);
                }
            }
            *current_events = window_results;
        }

        RuntimeOp::Aggregate(aggregator) => {
            // Use apply_shared to avoid cloning events
            let result = aggregator.apply_shared(current_events);
            // Create synthetic event from aggregation result
            let mut agg_event = Event::new("AggregationResult");
            for (key, value) in result {
                agg_event.data.insert(key.into(), value);
            }
            *current_events = vec![Arc::new(agg_event)];
        }

        RuntimeOp::PartitionedAggregate(state) => {
            let results = state.apply(current_events);
            // Create one synthetic event per partition
            *current_events = results
                .into_iter()
                .map(|(partition_key, result)| {
                    let mut agg_event = Event::new("AggregationResult");
                    agg_event
                        .data
                        .insert("_partition".into(), Value::Str(partition_key.into()));
                    for (key, value) in result {
                        agg_event.data.insert(key.into(), value);
                    }
                    Arc::new(agg_event)
                })
                .collect();
        }

        RuntimeOp::Having(expr) => {
            // Having filter - applied after aggregation to filter results
            // PERF: Use static empty context to avoid allocation
            current_events.retain(|event| {
                evaluator::eval_expr_with_functions(
                    expr,
                    event.as_ref(),
                    SequenceContext::empty(),
                    functions,
                    empty_vars(),
                )
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            });
        }

        RuntimeOp::Select(config) => {
            // Transform events by evaluating expressions and creating new fields
            // PERF: Use static empty context to avoid allocation
            *current_events = current_events
                .iter()
                .map(|event| {
                    let mut new_event = Event::new(event.event_type.clone());
                    new_event.timestamp = event.timestamp;
                    for (out_name, expr) in &config.fields {
                        if let Some(value) = evaluator::eval_expr_with_functions(
                            expr,
                            event.as_ref(),
                            SequenceContext::empty(),
                            functions,
                            empty_vars(),
                        ) {
                            new_event.data.insert(out_name.clone().into(), value);
                        }
                    }
                    Arc::new(new_event)
                })
                .collect();
        }

        RuntimeOp::Emit(config) => {
            let mut emitted: Vec<SharedEvent> = Vec::new();
            for event in current_events.iter() {
                let mut new_event = Event::new(stream_name.to_string());
                new_event.timestamp = event.timestamp;
                for (out_name, source) in &config.fields {
                    if let Some(value) = event.get(source) {
                        new_event
                            .data
                            .insert(out_name.clone().into(), value.clone());
                    } else {
                        new_event
                            .data
                            .insert(out_name.clone().into(), Value::Str(source.clone().into()));
                    }
                }
                emitted.push(Arc::new(new_event));
            }
            emitted_events.extend(emitted.iter().map(Arc::clone));
            *current_events = emitted;
        }

        RuntimeOp::EmitExpr(config) => {
            // PERF: Use static empty context to avoid allocation
            let mut emitted: Vec<SharedEvent> = Vec::new();
            for event in current_events.iter() {
                let mut new_event = Event::new(stream_name.to_string());
                new_event.timestamp = event.timestamp;
                for (out_name, expr) in &config.fields {
                    if let Some(value) = evaluator::eval_expr_with_functions(
                        expr,
                        event.as_ref(),
                        SequenceContext::empty(),
                        functions,
                        empty_vars(),
                    ) {
                        new_event.data.insert(out_name.clone().into(), value);
                    }
                }
                emitted.push(Arc::new(new_event));
            }
            emitted_events.extend(emitted.iter().map(Arc::clone));
            *current_events = emitted;
        }

        RuntimeOp::Print(config) => {
            for event in current_events.iter() {
                let mut parts = Vec::new();
                for expr in &config.exprs {
                    let value =
                        evaluator::eval_filter_expr(expr, event.as_ref(), SequenceContext::empty())
                            .unwrap_or(Value::Null);
                    parts.push(format!("{}", value));
                }
                let output = if parts.is_empty() {
                    format!("[{}] {}: {:?}", stream_name, event.event_type, event.data)
                } else {
                    parts.join(" ")
                };
                println!("[PRINT] {}", output);
            }
        }

        RuntimeOp::Log(config) => {
            for event in current_events.iter() {
                let msg = config
                    .message
                    .clone()
                    .unwrap_or_else(|| event.event_type.to_string());
                let data = if let Some(ref field) = config.data_field {
                    event
                        .get(field)
                        .map(|v| format!("{}", v))
                        .unwrap_or_default()
                } else {
                    format!("{:?}", event.data)
                };

                match config.level.as_str() {
                    "error" => {
                        tracing::error!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                    "warn" | "warning" => {
                        tracing::warn!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                    "debug" => {
                        tracing::debug!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                    "trace" => {
                        tracing::trace!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                    _ => {
                        tracing::info!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                }
            }
        }

        RuntimeOp::Sequence => {
            // Process events through SASE+ engine (NFA-based pattern matching)
            let mut sequence_results = Vec::new();

            if let Some(ref mut sase) = sase_engine {
                for event in current_events.iter() {
                    let matches = sase.process_shared(Arc::clone(event));
                    for match_result in matches {
                        // Create synthetic event from completed sequence
                        let mut seq_event = Event::new("SequenceMatch");
                        seq_event
                            .data
                            .insert("stream".into(), Value::Str(stream_name.to_string().into()));
                        seq_event.data.insert(
                            "match_duration_ms".into(),
                            Value::Int(match_result.duration.as_millis() as i64),
                        );
                        // Add captured events to the result
                        for (alias, captured) in &match_result.captured {
                            for (k, v) in &captured.data {
                                seq_event
                                    .data
                                    .insert(format!("{}_{}", alias, k).into(), v.clone());
                            }
                        }
                        sequence_results.push(Arc::new(seq_event));
                    }
                }
            }

            if sequence_results.is_empty() {
                current_events.clear();
            } else {
                *current_events = sequence_results;
            }
        }

        RuntimeOp::AttentionWindow(_config) => {
            // AttentionWindow is handled at stream level before operations
        }

        RuntimeOp::Pattern(config) => {
            // Pattern matching: evaluate the matcher expression with events as context
            // The matcher is a lambda: events => predicate
            // PERF: Use static empty context to avoid allocation
            let events_value = Value::array(
                current_events
                    .iter()
                    .map(|e| {
                        let mut map: IndexMap<Arc<str>, Value, FxBuildHasher> =
                            IndexMap::with_hasher(FxBuildHasher);
                        map.insert(
                            "event_type".into(),
                            Value::Str(e.event_type.to_string().into()),
                        );
                        for (k, v) in &e.data {
                            map.insert(k.clone(), v.clone());
                        }
                        Value::map(map)
                    })
                    .collect(),
            );

            // Create a context with "events" bound
            let mut pattern_vars = FxHashMap::default();
            pattern_vars.insert("events".into(), events_value);

            // Dereference events for pattern evaluation
            let event_refs: Vec<Event> = current_events.iter().map(|e| (**e).clone()).collect();

            // Evaluate the pattern matcher
            if let Some(result) = evaluator::eval_pattern_expr(
                &config.matcher,
                &event_refs,
                SequenceContext::empty(),
                functions,
                &pattern_vars,
                attention_window,
            ) {
                if !result.as_bool().unwrap_or(false) {
                    // Pattern didn't match, filter out all events
                    current_events.clear();
                }
            }
        }

        RuntimeOp::Process(expr) => {
            // PERF: Use static empty context to avoid allocation
            let mut all_emitted = Vec::new();
            for event in current_events.iter() {
                let (_, emitted) = evaluator::with_emit_collector(|| {
                    evaluator::eval_expr_with_functions(
                        expr,
                        event.as_ref(),
                        SequenceContext::empty(),
                        functions,
                        empty_vars(),
                    );
                });
                all_emitted.extend(emitted);
            }
            *current_events = all_emitted.into_iter().map(Arc::new).collect();
        }

        RuntimeOp::To(config) => {
            // Send current events to the named connector as a side-effect.
            // Events continue flowing through the pipeline unchanged.
            if let Some(sink) = sinks.get(&config.sink_key) {
                for event in current_events.iter() {
                    if let Err(e) = sink.send(event).await {
                        warn!(
                            "Failed to send to connector '{}': {}",
                            config.connector_name, e
                        );
                    }
                }
            } else {
                warn!("Connector '{}' not found for .to()", config.connector_name);
            }
        }
    }

    Ok(())
}

/// Synchronous pipeline execution - for maximum throughput when no .to() sinks are used.
/// Skips all async operations (.to() sink sends).
pub(crate) fn execute_pipeline_sync(
    stream: &mut StreamDefinition,
    initial_events: Vec<SharedEvent>,
    start_idx: usize,
    skip_flags: SkipFlags,
    functions: &FxHashMap<String, UserFunction>,
) -> Result<StreamProcessResult, String> {
    let mut current_events = initial_events;
    let mut emitted_events: Vec<SharedEvent> = Vec::new();

    // Iterate operations starting from start_idx
    for op in &mut stream.operations[start_idx..] {
        // Check skip flags
        if should_skip_op(op, skip_flags) {
            continue;
        }

        // Skip .to() operations in sync mode
        if matches!(op, RuntimeOp::To(_)) {
            continue;
        }

        execute_op_sync(
            op,
            &stream.name,
            &mut stream.sase_engine,
            stream.attention_window.as_ref(),
            &mut current_events,
            &mut emitted_events,
            functions,
        )?;

        if current_events.is_empty() {
            return Ok(StreamProcessResult {
                emitted_events,
                output_events: vec![],
            });
        }
    }

    // Rename event_type to stream name for downstream routing
    let output_events = current_events
        .into_iter()
        .map(|e| {
            let mut owned = (*e).clone();
            owned.event_type = stream.name.clone().into();
            Arc::new(owned)
        })
        .collect();

    Ok(StreamProcessResult {
        emitted_events,
        output_events,
    })
}

/// Synchronous operation execution - handles all ops except .to() sinks.
#[allow(clippy::too_many_arguments)]
fn execute_op_sync(
    op: &mut RuntimeOp,
    stream_name: &str,
    sase_engine: &mut Option<crate::sase::SaseEngine>,
    attention_window: Option<&crate::attention::AttentionWindow>,
    current_events: &mut Vec<SharedEvent>,
    emitted_events: &mut Vec<SharedEvent>,
    functions: &FxHashMap<String, UserFunction>,
) -> Result<(), String> {
    match op {
        RuntimeOp::WhereClosure(predicate) => {
            current_events.retain(|e| predicate(e));
        }

        RuntimeOp::WhereExpr(expr) => {
            // PERF: Use static empty context to avoid allocation
            current_events.retain(|e| {
                evaluator::eval_expr_with_functions(
                    expr,
                    e.as_ref(),
                    SequenceContext::empty(),
                    functions,
                    empty_vars(),
                )
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            });
        }

        RuntimeOp::Window(window) => {
            let mut window_results = Vec::new();
            for event in current_events.drain(..) {
                match window {
                    WindowType::Tumbling(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                    WindowType::Sliding(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results = window_events;
                        }
                    }
                    WindowType::Count(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                    WindowType::SlidingCount(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results = window_events;
                        }
                    }
                    WindowType::PartitionedTumbling(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                    WindowType::PartitionedSliding(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results = window_events;
                        }
                    }
                    WindowType::Session(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                    WindowType::PartitionedSession(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results = completed;
                        }
                    }
                }
            }
            *current_events = window_results;
        }

        RuntimeOp::PartitionedWindow(state) => {
            let mut window_results = Vec::new();
            for event in current_events.drain(..) {
                if let Some(completed) = state.add(event) {
                    window_results.extend(completed);
                }
            }
            *current_events = window_results;
        }

        RuntimeOp::PartitionedSlidingCountWindow(state) => {
            let mut window_results = Vec::new();
            for event in current_events.drain(..) {
                if let Some(completed) = state.add(event) {
                    window_results.extend(completed);
                }
            }
            *current_events = window_results;
        }

        RuntimeOp::Aggregate(aggregator) => {
            let result = aggregator.apply_shared(current_events);
            let mut agg_event = Event::new("AggregationResult");
            for (key, value) in result {
                agg_event.data.insert(key.into(), value);
            }
            *current_events = vec![Arc::new(agg_event)];
        }

        RuntimeOp::PartitionedAggregate(state) => {
            let results = state.apply(current_events);
            *current_events = results
                .into_iter()
                .map(|(partition_key, result)| {
                    let mut agg_event = Event::new("AggregationResult");
                    agg_event
                        .data
                        .insert("_partition".into(), Value::Str(partition_key.into()));
                    for (key, value) in result {
                        agg_event.data.insert(key.into(), value);
                    }
                    Arc::new(agg_event)
                })
                .collect();
        }

        RuntimeOp::Having(expr) => {
            current_events.retain(|event| {
                evaluator::eval_expr_with_functions(
                    expr,
                    event.as_ref(),
                    SequenceContext::empty(),
                    functions,
                    empty_vars(),
                )
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            });
        }

        RuntimeOp::Select(config) => {
            *current_events = current_events
                .iter()
                .map(|event| {
                    let mut new_event = Event::new(event.event_type.clone());
                    new_event.timestamp = event.timestamp;
                    for (out_name, expr) in &config.fields {
                        if let Some(value) = evaluator::eval_expr_with_functions(
                            expr,
                            event.as_ref(),
                            SequenceContext::empty(),
                            functions,
                            empty_vars(),
                        ) {
                            new_event.data.insert(out_name.clone().into(), value);
                        }
                    }
                    Arc::new(new_event)
                })
                .collect();
        }

        RuntimeOp::Emit(config) => {
            let mut emitted: Vec<SharedEvent> = Vec::new();
            for event in current_events.iter() {
                let mut new_event = Event::new(stream_name.to_string());
                new_event.timestamp = event.timestamp;
                for (out_name, source) in &config.fields {
                    if let Some(value) = event.get(source) {
                        new_event
                            .data
                            .insert(out_name.clone().into(), value.clone());
                    } else {
                        new_event
                            .data
                            .insert(out_name.clone().into(), Value::Str(source.clone().into()));
                    }
                }
                emitted.push(Arc::new(new_event));
            }
            emitted_events.extend(emitted.iter().map(Arc::clone));
            *current_events = emitted;
        }

        RuntimeOp::EmitExpr(config) => {
            // PERF: Use static empty context to avoid allocation
            let mut emitted: Vec<SharedEvent> = Vec::new();
            for event in current_events.iter() {
                let mut new_event = Event::new(stream_name.to_string());
                new_event.timestamp = event.timestamp;
                for (out_name, expr) in &config.fields {
                    if let Some(value) = evaluator::eval_expr_with_functions(
                        expr,
                        event.as_ref(),
                        SequenceContext::empty(),
                        functions,
                        empty_vars(),
                    ) {
                        new_event.data.insert(out_name.clone().into(), value);
                    }
                }
                emitted.push(Arc::new(new_event));
            }
            emitted_events.extend(emitted.iter().map(Arc::clone));
            *current_events = emitted;
        }

        RuntimeOp::Print(config) => {
            for event in current_events.iter() {
                let mut parts = Vec::new();
                for expr in &config.exprs {
                    let value =
                        evaluator::eval_filter_expr(expr, event.as_ref(), SequenceContext::empty())
                            .unwrap_or(Value::Null);
                    parts.push(format!("{}", value));
                }
                let output = if parts.is_empty() {
                    format!("[{}] {}: {:?}", stream_name, event.event_type, event.data)
                } else {
                    parts.join(" ")
                };
                println!("[PRINT] {}", output);
            }
        }

        RuntimeOp::Log(config) => {
            for event in current_events.iter() {
                let msg = config
                    .message
                    .clone()
                    .unwrap_or_else(|| event.event_type.to_string());
                let data = if let Some(ref field) = config.data_field {
                    event
                        .get(field)
                        .map(|v| format!("{}", v))
                        .unwrap_or_default()
                } else {
                    format!("{:?}", event.data)
                };

                match config.level.as_str() {
                    "error" => {
                        tracing::error!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                    "warn" | "warning" => {
                        tracing::warn!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                    "debug" => {
                        tracing::debug!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                    "trace" => {
                        tracing::trace!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                    _ => {
                        tracing::info!(stream = %stream_name, message = %msg, data = %data, "Stream log")
                    }
                }
            }
        }

        RuntimeOp::Sequence => {
            let mut sequence_results = Vec::new();

            if let Some(ref mut sase) = sase_engine {
                for event in current_events.iter() {
                    let matches = sase.process_shared(Arc::clone(event));
                    for match_result in matches {
                        let mut seq_event = Event::new("SequenceMatch");
                        seq_event
                            .data
                            .insert("stream".into(), Value::Str(stream_name.to_string().into()));
                        seq_event.data.insert(
                            "match_duration_ms".into(),
                            Value::Int(match_result.duration.as_millis() as i64),
                        );
                        for (alias, captured) in &match_result.captured {
                            for (k, v) in &captured.data {
                                seq_event
                                    .data
                                    .insert(format!("{}_{}", alias, k).into(), v.clone());
                            }
                        }
                        sequence_results.push(Arc::new(seq_event));
                    }
                }
            }

            if sequence_results.is_empty() {
                current_events.clear();
            } else {
                *current_events = sequence_results;
            }
        }

        RuntimeOp::AttentionWindow(_config) => {
            // AttentionWindow is handled at stream level before operations
        }

        RuntimeOp::Pattern(config) => {
            // PERF: Use static empty context to avoid allocation
            let events_value = Value::array(
                current_events
                    .iter()
                    .map(|e| {
                        let mut map: IndexMap<Arc<str>, Value, FxBuildHasher> =
                            IndexMap::with_hasher(FxBuildHasher);
                        map.insert(
                            "event_type".into(),
                            Value::Str(e.event_type.to_string().into()),
                        );
                        for (k, v) in &e.data {
                            map.insert(k.clone(), v.clone());
                        }
                        Value::map(map)
                    })
                    .collect(),
            );

            let mut pattern_vars = FxHashMap::default();
            pattern_vars.insert("events".into(), events_value);

            let event_refs: Vec<Event> = current_events.iter().map(|e| (**e).clone()).collect();

            if let Some(result) = evaluator::eval_pattern_expr(
                &config.matcher,
                &event_refs,
                SequenceContext::empty(),
                functions,
                &pattern_vars,
                attention_window,
            ) {
                if !result.as_bool().unwrap_or(false) {
                    current_events.clear();
                }
            }
        }

        RuntimeOp::Process(expr) => {
            // PERF: Use static empty context to avoid allocation
            let mut all_emitted = Vec::new();
            for event in current_events.iter() {
                let (_, emitted) = evaluator::with_emit_collector(|| {
                    evaluator::eval_expr_with_functions(
                        expr,
                        event.as_ref(),
                        SequenceContext::empty(),
                        functions,
                        empty_vars(),
                    );
                });
                all_emitted.extend(emitted);
            }
            *current_events = all_emitted.into_iter().map(Arc::new).collect();
        }

        RuntimeOp::To(_) => {
            // Skip .to() operations in sync mode - they require async
        }
    }

    Ok(()
)}
