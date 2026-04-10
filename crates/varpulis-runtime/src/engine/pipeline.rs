//! Unified pipeline execution for the Varpulis engine
//!
//! This module provides a single `execute_pipeline` function that replaces
//! the previously triplicated pipeline logic (process_stream, process_join_result,
//! process_post_window).

use std::sync::Arc;

use indexmap::IndexMap;
use rustc_hash::{FxBuildHasher, FxHashMap};
use tracing::warn;
use varpulis_core::Value;

use super::evaluator;
use super::types::*;
use crate::event::{Event, SharedEvent};
use crate::sequence::SequenceContext;

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
pub struct SkipFlags {
    /// Skip non-partitioned Window ops (used by join processing - JoinBuffer handles windowing)
    pub window: bool,
    /// Skip Print/Log ops
    pub print_log: bool,
    /// Skip Sequence/Pattern ops
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
    #[allow(dead_code)]
    pub fn for_join() -> Self {
        Self {
            window: true,
            print_log: true,
            sequence_pattern: true,
            ..Default::default()
        }
    }

    /// For post-window processing - skip all windows, where closures, sequence/pattern ops
    #[allow(dead_code)]
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

/// Execute operations `stream.operations[start_idx..]` on the given events (async version).
///
/// This is the single unified pipeline that replaces:
/// - `process_stream_with_functions` (start_idx = 0, SkipFlags::none())
/// - `process_join_result` (start_idx = 0, SkipFlags::for_join())
/// - `process_post_window` (start_idx = window_idx + 1, SkipFlags::for_post_window())
///
/// Requires async-runtime for `.to()` and `.enrich()` operations.
#[cfg(feature = "async-runtime")]
pub async fn execute_pipeline(
    stream: &mut StreamDefinition,
    initial_events: Vec<SharedEvent>,
    start_idx: usize,
    skip_flags: SkipFlags,
    functions: &FxHashMap<String, UserFunction>,
    sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
) -> Result<StreamProcessResult, super::error::EngineError> {
    // Store the raw event for the Forecast op (it needs to learn from every
    // event, even when the Sequence op clears current_events on non-match).
    if stream.pst_forecaster.is_some() {
        stream.last_raw_event = initial_events.first().cloned();
    }

    let mut current_events = initial_events;
    let mut emitted_events: Vec<SharedEvent> = Vec::new();
    let mut sink_events_sent: u64 = 0;
    let has_forecast = stream.pst_forecaster.is_some();

    // Iterate operations starting from start_idx
    // We need the index for RuntimeOp::Sequence which accesses stream.sase_engine
    for op in &mut stream.operations[start_idx..] {
        // Check skip flags
        if should_skip_op(op, skip_flags) {
            continue;
        }

        execute_op(
            op,
            &stream.name_arc,
            &mut stream.sase_engine,
            &mut stream.hamlet_aggregator,
            &stream.shared_hamlet_ref,
            &mut stream.pst_forecaster,
            &stream.last_raw_event,
            &stream.enrichment,
            &mut current_events,
            &mut emitted_events,
            functions,
            sinks,
            &mut sink_events_sent,
        )
        .await?;

        // Don't exit early if the Forecast op hasn't run yet — it needs to
        // learn from every event even when Sequence produces no match.
        if current_events.is_empty() && !(has_forecast && matches!(op, RuntimeOp::Sequence)) {
            return Ok(StreamProcessResult {
                emitted_events,
                output_events: vec![],
                sink_events_sent,
            });
        }
    }

    // Rename event_type to stream name for downstream routing.
    //
    // PERF: When emitted_events is non-empty, the dispatch loop
    // ignores output_events entirely (only emitted_events go to the
    // output channel, and output_events are NOT re-routed). The
    // rename does Arc::try_unwrap which deep-clones when refcount > 1
    // (always true after EmitExpr's `emitted_events.extend(Arc::clone)`).
    // Skipping it avoids one full Event clone per emitted event.
    let output_events = if !emitted_events.is_empty() {
        // Caller ignores these — pass through without cloning.
        current_events
    } else {
        let name_arc = &stream.name_arc;
        current_events
            .into_iter()
            .map(|e| {
                let mut owned = Arc::try_unwrap(e).unwrap_or_else(|arc| (*arc).clone());
                owned.event_type = Arc::clone(name_arc);
                Arc::new(owned)
            })
            .collect()
    };

    Ok(StreamProcessResult {
        emitted_events,
        output_events,
        sink_events_sent,
    })
}

/// Check if an operation should be skipped based on flags
const fn should_skip_op(op: &RuntimeOp, flags: SkipFlags) -> bool {
    match op {
        // Window skipping
        RuntimeOp::Window(_) => flags.window || flags.all_windows,
        RuntimeOp::PartitionedWindow(_) | RuntimeOp::PartitionedSlidingCountWindow(_) => {
            flags.all_windows
        }

        // Print/Log skipping
        RuntimeOp::Print(_) | RuntimeOp::Log(_) => flags.print_log,

        // Sequence/Pattern/TrendAggregate/Forecast skipping
        RuntimeOp::Sequence
        | RuntimeOp::Pattern(_)
        | RuntimeOp::TrendAggregate(_)
        | RuntimeOp::Forecast(_) => flags.sequence_pattern,

        // WhereClosure skipping
        RuntimeOp::WhereClosure(_) => flags.where_closure,

        // Other ops are never skipped by flags
        _ => false,
    }
}

/// Execute a single RuntimeOp on the current event batch (async version).
///
/// Handles `RuntimeOp::To` and `RuntimeOp::Enrich` (which require `.await`),
/// then delegates all other ops to [`execute_op_common`].
#[cfg(feature = "async-runtime")]
#[allow(clippy::too_many_arguments)]
async fn execute_op(
    op: &mut RuntimeOp,
    stream_name: &Arc<str>,
    sase_engine: &mut Option<crate::sase::SaseEngine>,
    hamlet_aggregator: &mut Option<crate::hamlet::HamletAggregator>,
    shared_hamlet_ref: &Option<Arc<std::sync::Mutex<crate::hamlet::HamletAggregator>>>,
    pst_forecaster: &mut Option<crate::pst::PatternMarkovChain>,
    last_raw_event: &Option<SharedEvent>,
    enrichment: &Option<(
        Arc<dyn crate::enrichment::EnrichmentProvider>,
        Arc<crate::enrichment::EnrichmentCache>,
    )>,
    current_events: &mut Vec<SharedEvent>,
    emitted_events: &mut Vec<SharedEvent>,
    functions: &FxHashMap<String, UserFunction>,
    sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
    sink_sent: &mut u64,
) -> Result<(), super::error::EngineError> {
    // Handle async-requiring ops here; everything else is sync.
    if let RuntimeOp::To(config) = op {
        if let Some(sink) = sinks.get(&config.sink_key) {
            match &config.topic {
                None | Some(super::types::TopicSpec::Static(_)) => {
                    // Static topic or no topic override — use send_batch (current path)
                    let batch_size = current_events.len() as u64;
                    if let Err(e) = sink.send_batch(current_events).await {
                        warn!(
                            "Failed to send batch to connector '{}': {}",
                            config.connector_name, e
                        );
                    } else {
                        *sink_sent += batch_size;
                    }
                }
                Some(super::types::TopicSpec::Dynamic(expr)) => {
                    // Dynamic topic — evaluate expr per event, group by topic, send per-group
                    let mut groups: rustc_hash::FxHashMap<String, Vec<SharedEvent>> =
                        rustc_hash::FxHashMap::default();
                    for event in current_events.iter() {
                        let topic_val = evaluator::eval_expr_with_functions(
                            expr,
                            event.as_ref(),
                            SequenceContext::empty(),
                            functions,
                            empty_vars(),
                        )
                        .unwrap_or(Value::Null);
                        let topic_str = match topic_val {
                            Value::Str(s) => s.to_string(),
                            Value::Int(i) => i.to_string(),
                            other => format!("{other}"),
                        };
                        groups.entry(topic_str).or_default().push(Arc::clone(event));
                    }
                    let total = current_events.len() as u64;
                    let mut all_ok = true;
                    for (topic, events) in &groups {
                        if let Err(e) = sink.send_batch_to_topic(events, topic).await {
                            warn!(
                                "Failed to send batch to connector '{}' topic '{}': {}",
                                config.connector_name, topic, e
                            );
                            all_ok = false;
                        }
                    }
                    if all_ok {
                        *sink_sent += total;
                    }
                }
            }
        } else {
            warn!("Connector '{}' not found for .to()", config.connector_name);
        }
        return Ok(());
    }

    if let RuntimeOp::Enrich(config) = op {
        if let Some((provider, cache)) = enrichment {
            let timeout_duration = std::time::Duration::from_nanos(config.timeout_ns);
            let mut enriched = Vec::new();

            for event in current_events.drain(..) {
                let start = std::time::Instant::now();

                // Evaluate key expression
                let key_value = evaluator::eval_expr_with_functions(
                    &config.key_expr,
                    event.as_ref(),
                    SequenceContext::empty(),
                    functions,
                    empty_vars(),
                )
                .unwrap_or(Value::Null);

                let cache_key = format!("{}:{:?}", config.connector_name, key_value);

                // Check cache first
                if let Some(cached_fields) = cache.get(&cache_key) {
                    let mut enriched_event = (*event).clone();
                    for (field_name, field_value) in &cached_fields {
                        enriched_event
                            .data
                            .insert(Arc::<str>::from(field_name.as_str()), field_value.clone());
                    }
                    enriched_event
                        .data
                        .insert(Arc::<str>::from("enrich_status"), Value::str("cached"));
                    enriched_event
                        .data
                        .insert(Arc::<str>::from("enrich_latency_ms"), Value::Int(0));
                    enriched.push(Arc::new(enriched_event));
                    continue;
                }

                // Perform async lookup with timeout
                let lookup_result = tokio::time::timeout(
                    timeout_duration,
                    provider.lookup(&key_value, &config.fields),
                )
                .await;

                let elapsed_ms = start.elapsed().as_millis() as i64;

                match lookup_result {
                    Ok(Ok(result)) => {
                        // Cache the result
                        cache.insert(cache_key, result.fields.clone());

                        let mut enriched_event = (*event).clone();
                        for (field_name, field_value) in &result.fields {
                            enriched_event
                                .data
                                .insert(Arc::<str>::from(field_name.as_str()), field_value.clone());
                        }
                        enriched_event
                            .data
                            .insert(Arc::<str>::from("enrich_status"), Value::str("ok"));
                        enriched_event.data.insert(
                            Arc::<str>::from("enrich_latency_ms"),
                            Value::Int(elapsed_ms),
                        );
                        enriched.push(Arc::new(enriched_event));
                    }
                    Ok(Err(e)) => {
                        // Lookup failed — apply fallback or skip
                        if let Some(ref fallback) = config.fallback {
                            let mut enriched_event = (*event).clone();
                            for field_name in &config.fields {
                                enriched_event.data.insert(
                                    Arc::<str>::from(field_name.as_str()),
                                    fallback.clone(),
                                );
                            }
                            enriched_event
                                .data
                                .insert(Arc::<str>::from("enrich_status"), Value::str("error"));
                            enriched_event.data.insert(
                                Arc::<str>::from("enrich_latency_ms"),
                                Value::Int(elapsed_ms),
                            );
                            enriched.push(Arc::new(enriched_event));
                        } else {
                            warn!(
                                "Enrichment failed for stream '{}': {} (event skipped)",
                                stream_name, e
                            );
                        }
                    }
                    Err(_timeout) => {
                        // Timeout — apply fallback or skip
                        if let Some(ref fallback) = config.fallback {
                            let mut enriched_event = (*event).clone();
                            for field_name in &config.fields {
                                enriched_event.data.insert(
                                    Arc::<str>::from(field_name.as_str()),
                                    fallback.clone(),
                                );
                            }
                            enriched_event
                                .data
                                .insert(Arc::<str>::from("enrich_status"), Value::str("timeout"));
                            enriched_event.data.insert(
                                Arc::<str>::from("enrich_latency_ms"),
                                Value::Int(elapsed_ms),
                            );
                            enriched.push(Arc::new(enriched_event));
                        } else {
                            warn!(
                                "Enrichment timeout for stream '{}' after {}ms (event skipped)",
                                stream_name, elapsed_ms
                            );
                        }
                    }
                }
            }
            *current_events = enriched;
        } else {
            warn!(
                "No enrichment provider configured for stream '{}'",
                stream_name
            );
        }
        return Ok(());
    }

    // Handle alert webhook (async fire-and-forget)
    if let RuntimeOp::Alert(config) = op {
        for event in current_events.iter() {
            let message = if let Some(ref template) = config.message_template {
                let mut msg = template.clone();
                for (key, value) in &event.data {
                    msg = msg.replace(&format!("{{{key}}}"), &value.to_string());
                }
                msg
            } else {
                format!("{:?}", event.data)
            };

            if let Some(ref url) = config.webhook_url {
                let url = url.clone();
                let msg = message.clone();
                let stream = stream_name.to_string();
                tokio::spawn(async move {
                    let client = reqwest::Client::new();
                    let payload = serde_json::json!({
                        "text": msg,
                        "event_type": "alert",
                        "stream": stream,
                    });
                    if let Err(e) = client.post(&url).json(&payload).send().await {
                        tracing::warn!("Alert webhook failed: {e}");
                    }
                });
            } else {
                tracing::info!(stream = %stream_name, "Alert: {message}");
            }
        }
        // Events continue downstream (not consumed)
        return Ok(());
    }

    execute_op_common(
        op,
        stream_name,
        sase_engine,
        hamlet_aggregator,
        shared_hamlet_ref,
        pst_forecaster,
        last_raw_event,
        current_events,
        emitted_events,
        functions,
    )
}

/// Shared implementation of all RuntimeOps except `To` (which requires async).
///
/// Used by both the async [`execute_op`] and the sync [`execute_op_sync`].
#[allow(clippy::too_many_arguments)]
fn execute_op_common(
    op: &mut RuntimeOp,
    stream_name: &Arc<str>,
    sase_engine: &mut Option<crate::sase::SaseEngine>,
    hamlet_aggregator: &mut Option<crate::hamlet::HamletAggregator>,
    shared_hamlet_ref: &Option<Arc<std::sync::Mutex<crate::hamlet::HamletAggregator>>>,
    pst_forecaster: &mut Option<crate::pst::PatternMarkovChain>,
    last_raw_event: &Option<SharedEvent>,
    current_events: &mut Vec<SharedEvent>,
    emitted_events: &mut Vec<SharedEvent>,
    functions: &FxHashMap<String, UserFunction>,
) -> Result<(), super::error::EngineError> {
    match op {
        RuntimeOp::WhereClosure(predicate) => {
            current_events.retain(|e| predicate(e));
        }

        RuntimeOp::WhereExpr(expr) => {
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
                            window_results.extend(completed);
                        }
                    }
                    WindowType::Sliding(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results.extend(window_events);
                        }
                    }
                    WindowType::Count(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results.extend(completed);
                        }
                    }
                    WindowType::SlidingCount(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results.extend(window_events);
                        }
                    }
                    WindowType::PartitionedTumbling(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results.extend(completed);
                        }
                    }
                    WindowType::PartitionedSliding(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results.extend(window_events);
                        }
                    }
                    WindowType::Session(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results.extend(completed);
                        }
                    }
                    WindowType::PartitionedSession(w) => {
                        if let Some(completed) = w.add_shared(event) {
                            window_results.extend(completed);
                        }
                    }
                    WindowType::BinnedSliding(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results.extend(window_events);
                        }
                    }
                    WindowType::PartitionedBinnedSliding(w) => {
                        if let Some(window_events) = w.add_shared(event) {
                            window_results.extend(window_events);
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
            // Use the latest event's timestamp for the aggregation result
            let ts = current_events.last().map(|e| e.timestamp);

            #[cfg(feature = "arrow")]
            let result = if current_events.len() >= crate::arrow_bridge::ARROW_BATCH_THRESHOLD {
                // Arrow path: vectorized aggregation via Arrow compute
                let mut schema_cache = crate::arrow_bridge::SchemaCache::new();
                let schema = schema_cache.get_or_infer(current_events);
                if let Ok(batch) =
                    crate::arrow_bridge::events_to_record_batch(current_events, &schema)
                {
                    aggregator.apply_arrow(&batch)
                } else {
                    aggregator.apply_shared(current_events)
                }
            } else {
                aggregator.apply_shared(current_events)
            };

            #[cfg(not(feature = "arrow"))]
            let result = aggregator.apply_shared(current_events);

            let mut agg_event = Event::new("AggregationResult");
            if let Some(ts) = ts {
                agg_event.timestamp = ts;
            }
            for (key, value) in result {
                agg_event.data.insert(key.into(), value);
            }
            *current_events = vec![Arc::new(agg_event)];
        }

        RuntimeOp::PartitionedAggregate(state) => {
            // Use the latest event's timestamp for the aggregation results
            let ts = current_events.last().map(|e| e.timestamp);
            let results = state.apply(current_events);
            *current_events = results
                .into_iter()
                .map(|(partition_key, result)| {
                    let mut agg_event = Event::new("AggregationResult");
                    if let Some(ts) = ts {
                        agg_event.timestamp = ts;
                    }
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

        #[cfg(feature = "arrow")]
        RuntimeOp::PartitionedWindowedColumnarAggregate(state) => {
            // Phase-2 fused operator: hand the incoming batch to the
            // streaming bin-keyed aggregator. It buckets events by bin,
            // updates per-bin accumulator state in place, advances the
            // watermark to the running max event time, and drains any
            // bins whose end is at or below the new watermark.
            let flushed = state.ingest_and_flush(current_events);
            *current_events = flushed
                .into_iter()
                .map(|(bin_start_ms, partition_key, result)| {
                    let mut agg_event = Event::new("AggregationResult");
                    // Use bin_end as the result timestamp so downstream
                    // ordering matches the existing PartitionedAggregate
                    // path (which uses the latest event's timestamp).
                    if let Some(ts) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
                        bin_start_ms + state.bin_duration_ms,
                    ) {
                        agg_event.timestamp = ts;
                    }
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
            let mut emitted: Vec<SharedEvent> = Vec::with_capacity(current_events.len());
            for event in current_events.iter() {
                let mut new_event =
                    Event::with_capacity(Arc::clone(stream_name), config.fields.len());
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
            let mut emitted: Vec<SharedEvent> = Vec::with_capacity(current_events.len());

            // Fast path: if ALL emit fields are simple `Ident(name)`
            // expressions (pure field copies with no computation),
            // skip the full expression evaluator and do direct field
            // lookups. This is the common case for `.emit(a: a, b: b)`
            // and saves ~200 ns per field per event (500k expression
            // evals on scenario 02's 100k output events × 5 fields).
            let all_ident = config.fields.iter().all(|(_, expr)| {
                matches!(expr, varpulis_core::ast::Expr::Ident(_))
            });

            if all_ident {
                for event in current_events.iter() {
                    let mut new_event = Event::with_capacity(
                        Arc::clone(stream_name),
                        config.fields.len(),
                    );
                    new_event.timestamp = event.timestamp;
                    for (out_name, expr) in &config.fields {
                        if let varpulis_core::ast::Expr::Ident(field_name) = expr {
                            if let Some(value) = event.get(field_name) {
                                new_event
                                    .data
                                    .insert(Arc::from(out_name.as_str()), value.clone());
                            }
                        }
                    }
                    emitted.push(Arc::new(new_event));
                }
            } else {
                for event in current_events.iter() {
                    let mut new_event =
                        Event::with_capacity(Arc::clone(stream_name), config.fields.len());
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
            }

            emitted_events.extend(emitted.iter().map(Arc::clone));
            *current_events = emitted;
        }

        RuntimeOp::Print(config) => {
            for event in current_events.iter() {
                let mut parts = Vec::with_capacity(config.exprs.len());
                for expr in &config.exprs {
                    let value =
                        evaluator::eval_filter_expr(expr, event.as_ref(), SequenceContext::empty())
                            .unwrap_or(Value::Null);
                    parts.push(format!("{value}"));
                }
                let output = if parts.is_empty() {
                    format!("[{}] {}: {:?}", stream_name, event.event_type, event.data)
                } else {
                    parts.join(" ")
                };
                println!("[PRINT] {output}");
            }
        }

        RuntimeOp::Log(config) => {
            for event in current_events.iter() {
                let fallback;
                let msg = if let Some(ref m) = config.message {
                    m.as_str()
                } else {
                    fallback = event.event_type.to_string();
                    &fallback
                };
                let data = if let Some(ref field) = config.data_field {
                    event.get(field).map(|v| format!("{v}")).unwrap_or_default()
                } else {
                    format!("{:?}", event.data)
                };

                match config.level.as_str() {
                    "error" => {
                        tracing::error!(stream = %stream_name, message = %msg, data = %data, "Stream log");
                    }
                    "warn" | "warning" => {
                        tracing::warn!(stream = %stream_name, message = %msg, data = %data, "Stream log");
                    }
                    "debug" => {
                        tracing::debug!(stream = %stream_name, message = %msg, data = %data, "Stream log");
                    }
                    "trace" => {
                        tracing::trace!(stream = %stream_name, message = %msg, data = %data, "Stream log");
                    }
                    _ => {
                        tracing::info!(stream = %stream_name, message = %msg, data = %data, "Stream log");
                    }
                }
            }
        }

        RuntimeOp::Sequence => {
            let mut sequence_results = Vec::with_capacity(8);

            if let Some(ref mut sase) = sase_engine {
                for event in current_events.iter() {
                    let matches = sase.process_shared(Arc::clone(event));
                    for match_result in matches {
                        let mut seq_event = Event::new("SequenceMatch");
                        // Use the triggering event's timestamp (the event that completed the match)
                        seq_event.timestamp = event.timestamp;
                        seq_event
                            .data
                            .insert("stream".into(), Value::str(stream_name.as_ref()));
                        let match_count = match_result.stack.len() as i64;

                        // Compute duration from event timestamps (not wall-clock)
                        let event_duration_ms = if match_result.stack.len() >= 2 {
                            let first_ts = match_result.stack.first().unwrap().event.timestamp;
                            let last_ts = match_result.stack.last().unwrap().event.timestamp;
                            (last_ts - first_ts).num_milliseconds().max(0)
                        } else {
                            0
                        };

                        seq_event
                            .data
                            .insert("match_duration_ms".into(), Value::Int(event_duration_ms));
                        seq_event
                            .data
                            .insert("match_count".into(), Value::Int(match_count));

                        // match_rate: events per second (based on event timestamps)
                        let duration_secs = event_duration_ms as f64 / 1000.0;
                        if duration_secs > 0.0 {
                            seq_event.data.insert(
                                "match_rate".into(),
                                Value::Float(match_count as f64 / duration_secs),
                            );
                        }

                        // Group stack entries by alias for Kleene aggregates
                        let mut alias_events: FxHashMap<&str, Vec<&Event>> = FxHashMap::default();
                        for entry in &match_result.stack {
                            if let Some(ref alias) = entry.alias {
                                alias_events
                                    .entry(alias.as_str())
                                    .or_default()
                                    .push(entry.event.as_ref());
                            }
                        }

                        // For each alias group, inject first/last maps and aggregates
                        for (alias, events) in &alias_events {
                            let count = events.len();
                            seq_event
                                .data
                                .insert(format!("_count_{alias}").into(), Value::Int(count as i64));

                            // _events_alias: Value::Array of Value::Map (one map per captured event)
                            // Used by the evaluator to support array-style access:
                            // alias.LEN, alias[i], alias[i].field, collect(alias.field)
                            let events_array: Vec<Value> = events
                                .iter()
                                .map(|ev| {
                                    let mut map = IndexMap::with_hasher(FxBuildHasher);
                                    for (k, v) in &ev.data {
                                        map.insert(k.clone(), v.clone());
                                    }
                                    Value::Map(Box::new(map))
                                })
                                .collect();
                            seq_event.data.insert(
                                format!("_events_{alias}").into(),
                                Value::array(events_array),
                            );

                            // first(alias) and last(alias) as Value::Map
                            if let Some(first_ev) = events.first() {
                                let mut map = IndexMap::with_hasher(FxBuildHasher);
                                for (k, v) in &first_ev.data {
                                    map.insert(k.clone(), v.clone());
                                }
                                seq_event.data.insert(
                                    format!("_first_{alias}").into(),
                                    Value::Map(Box::new(map)),
                                );
                            }
                            if let Some(last_ev) = events.last() {
                                let mut map = IndexMap::with_hasher(FxBuildHasher);
                                for (k, v) in &last_ev.data {
                                    map.insert(k.clone(), v.clone());
                                }
                                seq_event.data.insert(
                                    format!("_last_{alias}").into(),
                                    Value::Map(Box::new(map)),
                                );
                            }

                            // Collect all unique field names across events
                            let mut all_fields: Vec<Arc<str>> = Vec::new();
                            for ev in events {
                                for (k, _) in &ev.data {
                                    if !all_fields.iter().any(|f| f == k) {
                                        all_fields.push(k.clone());
                                    }
                                }
                            }

                            // Pre-compute aggregates for each field
                            for field in &all_fields {
                                // Numeric aggregates (sum, avg, min, max)
                                let numeric_vals: Vec<f64> = events
                                    .iter()
                                    .filter_map(|ev| ev.get(field.as_ref()))
                                    .filter_map(|v| match v {
                                        Value::Float(f) => Some(*f),
                                        Value::Int(i) => Some(*i as f64),
                                        _ => None,
                                    })
                                    .collect();

                                if !numeric_vals.is_empty() {
                                    let sum: f64 = numeric_vals.iter().sum();
                                    let avg = sum / numeric_vals.len() as f64;
                                    let min =
                                        numeric_vals.iter().copied().fold(f64::INFINITY, f64::min);
                                    let max = numeric_vals
                                        .iter()
                                        .copied()
                                        .fold(f64::NEG_INFINITY, f64::max);

                                    seq_event.data.insert(
                                        format!("_agg_sum_{alias}_{field}").into(),
                                        Value::Float(sum),
                                    );
                                    seq_event.data.insert(
                                        format!("_agg_avg_{alias}_{field}").into(),
                                        Value::Float(avg),
                                    );
                                    seq_event.data.insert(
                                        format!("_agg_min_{alias}_{field}").into(),
                                        Value::Float(min),
                                    );
                                    seq_event.data.insert(
                                        format!("_agg_max_{alias}_{field}").into(),
                                        Value::Float(max),
                                    );
                                }

                                // Distinct count (works for all value types)
                                let mut distinct = std::collections::HashSet::new();
                                for ev in events {
                                    if let Some(v) = ev.get(field.as_ref()) {
                                        distinct.insert(format!("{v:?}"));
                                    }
                                }
                                seq_event.data.insert(
                                    format!("_agg_distinct_{alias}_{field}").into(),
                                    Value::Int(distinct.len() as i64),
                                );
                            }
                        }

                        // Flatten captured events (backward-compatible alias_field access)
                        for (alias, captured) in &match_result.captured {
                            for (k, v) in &captured.data {
                                seq_event
                                    .data
                                    .insert(format!("{alias}_{k}").into(), v.clone());
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

        RuntimeOp::TrendAggregate(config) => {
            let mut trend_results = Vec::new();

            let mut shared_guard;
            let effective_hamlet: Option<&mut crate::hamlet::HamletAggregator> =
                if let Some(ref mut h) = hamlet_aggregator {
                    Some(h)
                } else if let Some(ref shared) = shared_hamlet_ref {
                    shared_guard = shared.lock().unwrap_or_else(|e| e.into_inner());
                    Some(&mut *shared_guard)
                } else {
                    None
                };

            if let Some(hamlet) = effective_hamlet {
                for event in current_events.iter() {
                    config.accumulated.push(Arc::clone(event));
                    let results = hamlet.process(Arc::clone(event));
                    for agg_result in results {
                        // Only handle results for THIS stream's query
                        if agg_result.query_id != config.query_id {
                            continue;
                        }
                        let mut trend_event = Event::new("TrendAggregateResult");
                        trend_event.timestamp = event.timestamp;
                        trend_event
                            .data
                            .insert("stream".into(), Value::str(stream_name.as_ref()));
                        trend_event
                            .data
                            .insert("query_id".into(), Value::Int(agg_result.query_id as i64));

                        // Populate count-based fields
                        for (alias, agg_type) in &config.fields {
                            let value = match agg_type {
                                crate::greta::GretaAggregate::CountTrends => {
                                    Some(Value::Int(agg_result.trend_count as i64))
                                }
                                crate::greta::GretaAggregate::CountEvents(type_idx) => {
                                    // Count from accumulated events using type index → name map
                                    let count = if let Some(type_name) =
                                        config.type_index_to_name.get(*type_idx as usize)
                                    {
                                        config
                                            .accumulated
                                            .iter()
                                            .filter(|e| *e.event_type == *type_name)
                                            .count()
                                    } else {
                                        config.accumulated.len()
                                    };
                                    Some(Value::Int(count as i64))
                                }
                                // Field-based aggregates handled below
                                _ => None,
                            };
                            if let Some(v) = value {
                                trend_event.data.insert(alias.clone().into(), v);
                            }
                        }

                        // Compute field-based aggregates from accumulated events
                        for info in &config.field_aggregates {
                            let values: Vec<f64> = config
                                .accumulated
                                .iter()
                                .filter(|e| *e.event_type == info.event_type)
                                .filter_map(|e| e.get(&info.field_name))
                                .filter_map(|v| match v {
                                    Value::Float(f) => Some(*f),
                                    Value::Int(i) => Some(*i as f64),
                                    _ => None,
                                })
                                .collect();

                            if !values.is_empty() {
                                let result = match info.func.as_str() {
                                    "sum" => values.iter().sum::<f64>(),
                                    "avg" => values.iter().sum::<f64>() / values.len() as f64,
                                    "min" => values.iter().copied().fold(f64::INFINITY, f64::min),
                                    "max" => {
                                        values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
                                    }
                                    _ => 0.0,
                                };
                                trend_event
                                    .data
                                    .insert(info.output_alias.clone().into(), Value::Float(result));
                            }
                        }

                        trend_event
                            .data
                            .insert("is_final".into(), Value::Bool(agg_result.is_final));
                        trend_results.push(Arc::new(trend_event));
                    }
                }
            }

            if trend_results.is_empty() {
                current_events.clear();
            } else {
                *current_events = trend_results;
            }
        }

        RuntimeOp::Score(config) => {
            #[cfg(feature = "onnx")]
            {
                if config.batch_size > 1 && current_events.len() > 1 {
                    // Batch inference: process events in chunks
                    let mut enriched = Vec::with_capacity(current_events.len());
                    for chunk in current_events.chunks(config.batch_size) {
                        let event_refs: Vec<&crate::event::Event> =
                            chunk.iter().map(|e| e.as_ref()).collect();
                        match config.model.infer_batch(&event_refs) {
                            Ok(batch_results) => {
                                for (event, predictions) in chunk.iter().zip(batch_results) {
                                    let mut new_event = (**event).clone();
                                    for (field, value) in predictions {
                                        new_event.data.insert(field.into(), Value::Float(value));
                                    }
                                    enriched.push(Arc::new(new_event));
                                }
                            }
                            Err(e) => {
                                warn!(".score() batch inference error: {}", e);
                                enriched.extend_from_slice(chunk);
                            }
                        }
                    }
                    *current_events = enriched;
                } else {
                    // Single-event inference
                    let mut enriched = Vec::with_capacity(current_events.len());
                    for event in current_events.drain(..) {
                        match config.model.infer(event.as_ref()) {
                            Ok(predictions) => {
                                let mut new_event = (*event).clone();
                                for (field, value) in predictions {
                                    new_event.data.insert(field.into(), Value::Float(value));
                                }
                                enriched.push(Arc::new(new_event));
                            }
                            Err(e) => {
                                warn!(".score() inference error: {}", e);
                                enriched.push(event);
                            }
                        }
                    }
                    *current_events = enriched;
                }
            }
            #[cfg(not(feature = "onnx"))]
            {
                let _ = config;
                warn!(".score() requires 'onnx' feature");
            }
        }

        RuntimeOp::Forecast(config) => {
            if let Some(ref mut pmc) = pst_forecaster {
                let run_snapshots: Vec<varpulis_pst::RunSnapshot> =
                    if let Some(ref sase) = sase_engine {
                        sase.active_run_snapshots()
                            .into_iter()
                            .map(|r| varpulis_pst::RunSnapshot {
                                current_state: r.current_state,
                                started_at_ns: r.started_at_ns,
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };

                let mut forecast_results = Vec::new();
                if let Some(ref raw) = last_raw_event {
                    let event_type = raw.event_type.to_string();
                    let event_ts = raw.timestamp.timestamp_nanos_opt().unwrap_or(0);

                    if let Some(forecast) = pmc.process(&event_type, event_ts, &run_snapshots) {
                        if forecast.probability >= config.confidence_threshold {
                            let mut forecast_event = Event::new("ForecastResult");
                            forecast_event
                                .data
                                .insert("stream".into(), Value::str(stream_name.as_ref()));
                            forecast_event.data.insert(
                                "forecast_probability".into(),
                                Value::Float(forecast.probability),
                            );
                            forecast_event.data.insert(
                                "forecast_time".into(),
                                Value::Int(forecast.expected_time_ns as i64),
                            );
                            forecast_event.data.insert(
                                "forecast_state".into(),
                                Value::Str(forecast.state_label.into()),
                            );
                            forecast_event.data.insert(
                                "forecast_context_depth".into(),
                                Value::Int(forecast.context_depth as i64),
                            );
                            forecast_event.data.insert(
                                "active_runs".into(),
                                Value::Int(forecast.active_runs as i64),
                            );
                            forecast_event.data.insert(
                                "forecast_lower".into(),
                                Value::Float(forecast.forecast_lower),
                            );
                            forecast_event.data.insert(
                                "forecast_upper".into(),
                                Value::Float(forecast.forecast_upper),
                            );
                            forecast_event.data.insert(
                                "forecast_confidence".into(),
                                Value::Float(forecast.forecast_confidence),
                            );
                            for (k, v) in &raw.data {
                                if !forecast_event.data.contains_key(k) {
                                    forecast_event.data.insert(k.clone(), v.clone());
                                }
                            }
                            forecast_event.timestamp = raw.timestamp;
                            forecast_results.push(Arc::new(forecast_event));
                        }
                    }
                }

                if !forecast_results.is_empty() {
                    *current_events = forecast_results;
                }
            }
        }

        RuntimeOp::Pattern(config) => {
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

            if let Some(result) = evaluator::eval_pattern_expr(
                &config.matcher,
                current_events,
                SequenceContext::empty(),
                functions,
                &pattern_vars,
            ) {
                if !result.as_bool().unwrap_or(false) {
                    current_events.clear();
                }
            }
        }

        RuntimeOp::Process(expr) => {
            let mut all_emitted = Vec::with_capacity(current_events.len());
            for event in current_events.iter() {
                let ((), emitted) = evaluator::with_emit_collector(|| {
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

        RuntimeOp::Alert(config) => {
            // Side-effect operator: log the alert message, don't consume events.
            // Webhook HTTP POST is handled by the async execute_op path only.
            if config.webhook_url.is_none() {
                for event in current_events.iter() {
                    let message = if let Some(ref template) = config.message_template {
                        let mut msg = template.clone();
                        for (key, value) in &event.data {
                            msg = msg.replace(&format!("{{{key}}}"), &value.to_string());
                        }
                        msg
                    } else {
                        format!("{:?}", event.data)
                    };
                    tracing::info!(stream = %stream_name, "Alert: {message}");
                }
            }
            // Events continue downstream (not consumed)
        }

        RuntimeOp::To(_) => {
            // No-op in common path; handled by async execute_op only.
        }

        RuntimeOp::Enrich(_) => {
            // No-op in common path; handled by async execute_op only.
        }

        RuntimeOp::Distinct(state) => {
            current_events.retain(|event| {
                let key = if let Some(ref expr) = state.expr {
                    evaluator::eval_expr_with_functions(
                        expr,
                        event.as_ref(),
                        SequenceContext::empty(),
                        functions,
                        empty_vars(),
                    )
                    .map(|v| format!("{v}"))
                    .unwrap_or_default()
                } else {
                    format!("{:?}", event.data)
                };
                state.seen.insert(key, ()).is_none()
            });
        }

        RuntimeOp::Limit(state) => {
            let remaining = state.max.saturating_sub(state.count);
            current_events.truncate(remaining);
            state.count += current_events.len();
        }

        #[cfg(feature = "async-runtime")]
        RuntimeOp::Concurrent(config) => {
            // Parallel processing: partition events across rayon thread pool,
            // process through remaining ops independently, then merge results.
            // Since we consume events here and return, the caller's loop continues
            // with the merged output.
            if current_events.len() <= 1 {
                // No benefit from parallelism with 0-1 events
                return Ok(());
            }

            let pool = &config.thread_pool;
            let events = std::mem::take(current_events);

            // Partition events
            let partitions: Vec<Vec<SharedEvent>> = if let Some(ref key) = config.partition_key {
                // Hash-based partitioning to preserve per-key ordering
                let mut buckets: Vec<Vec<SharedEvent>> =
                    (0..config.workers).map(|_| Vec::new()).collect();
                for event in events {
                    let hash = event.get(key).map_or(0, |v| {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        format!("{v}").hash(&mut h);
                        h.finish() as usize
                    });
                    buckets[hash % config.workers].push(event);
                }
                buckets
            } else {
                // Round-robin partitioning
                let chunk_size = events.len().div_ceil(config.workers);
                events
                    .chunks(chunk_size.max(1))
                    .map(|c| c.to_vec())
                    .collect()
            };

            // Process partitions in parallel on the rayon pool
            let results: Vec<Vec<SharedEvent>> = pool.install(|| {
                use rayon::prelude::*;
                partitions
                    .into_par_iter()
                    .map(|partition| {
                        // Each partition just passes through — the actual filtering/mapping
                        // happens in subsequent ops which will process the merged result.
                        partition
                    })
                    .collect()
            });

            // Merge results back
            let mut merged = Vec::new();
            for part in results {
                merged.extend(part);
            }
            *current_events = merged;
        }
    }

    Ok(())
}

/// Synchronous pipeline execution - for maximum throughput when no .to() sinks are used.
/// Skips all async operations (.to() sink sends).
/// When `skip_output_rename` is true, output events pass through without cloning
/// (used when there are no downstream routes that need the renamed event_type).
pub fn execute_pipeline_sync(
    stream: &mut StreamDefinition,
    initial_events: Vec<SharedEvent>,
    start_idx: usize,
    skip_flags: SkipFlags,
    functions: &FxHashMap<String, UserFunction>,
    skip_output_rename: bool,
) -> Result<StreamProcessResult, super::error::EngineError> {
    // Store the raw event for the Forecast op
    if stream.pst_forecaster.is_some() {
        stream.last_raw_event = initial_events.first().cloned();
    }

    let mut current_events = initial_events;
    let mut emitted_events: Vec<SharedEvent> = Vec::new();
    let has_forecast = stream.pst_forecaster.is_some();

    // Iterate operations starting from start_idx
    for op in &mut stream.operations[start_idx..] {
        // Check skip flags
        if should_skip_op(op, skip_flags) {
            continue;
        }

        // Skip .to() and .enrich() operations in sync mode (they require async)
        if matches!(op, RuntimeOp::To(_) | RuntimeOp::Enrich(_)) {
            continue;
        }

        execute_op_sync(
            op,
            &stream.name_arc,
            &mut stream.sase_engine,
            &mut stream.hamlet_aggregator,
            &stream.shared_hamlet_ref,
            &mut stream.pst_forecaster,
            &stream.last_raw_event,
            &mut current_events,
            &mut emitted_events,
            functions,
        )?;

        // Don't exit early if the Forecast op hasn't run yet
        if current_events.is_empty() && !(has_forecast && matches!(op, RuntimeOp::Sequence)) {
            return Ok(StreamProcessResult {
                emitted_events,
                output_events: vec![],
                sink_events_sent: 0,
            });
        }
    }

    // Skip rename + clone when no downstream consumers need the renamed
    // event_type, OR when emitted_events is non-empty (the caller
    // ignores output_events when emits exist — skipping the deep clone
    // avoids wasted Arc::try_unwrap + Event::clone per emitted event).
    let output_events = if skip_output_rename || !emitted_events.is_empty() {
        current_events
    } else {
        let name_arc = &stream.name_arc;
        current_events
            .into_iter()
            .map(|e| {
                let mut owned = Arc::try_unwrap(e).unwrap_or_else(|arc| (*arc).clone());
                owned.event_type = Arc::clone(name_arc);
                Arc::new(owned)
            })
            .collect()
    };

    Ok(StreamProcessResult {
        emitted_events,
        output_events,
        sink_events_sent: 0,
    })
}

/// Synchronous operation execution — delegates to [`execute_op_common`].
#[allow(clippy::too_many_arguments)]
fn execute_op_sync(
    op: &mut RuntimeOp,
    stream_name: &Arc<str>,
    sase_engine: &mut Option<crate::sase::SaseEngine>,
    hamlet_aggregator: &mut Option<crate::hamlet::HamletAggregator>,
    shared_hamlet_ref: &Option<Arc<std::sync::Mutex<crate::hamlet::HamletAggregator>>>,
    pst_forecaster: &mut Option<crate::pst::PatternMarkovChain>,
    last_raw_event: &Option<SharedEvent>,
    current_events: &mut Vec<SharedEvent>,
    emitted_events: &mut Vec<SharedEvent>,
    functions: &FxHashMap<String, UserFunction>,
) -> Result<(), super::error::EngineError> {
    execute_op_common(
        op,
        stream_name,
        sase_engine,
        hamlet_aggregator,
        shared_hamlet_ref,
        pst_forecaster,
        last_raw_event,
        current_events,
        emitted_events,
        functions,
    )
}
