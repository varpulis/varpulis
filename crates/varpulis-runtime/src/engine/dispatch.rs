//! Event dispatch: processing incoming events through stream pipelines.
//!
//! This module contains the `impl Engine` methods for event processing:
//! `process()`, `process_shared()`, `process_inner()`, `process_batch()`,
//! `process_batch_shared()`, `process_batch_sync()`, and their helpers.

use crate::event::{Event, SharedEvent};
use crate::sequence::SequenceContext;
use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::debug;

use super::evaluator;
use super::pipeline;
use super::types::{
    RuntimeOp, RuntimeSource, StreamDefinition, StreamProcessResult, UserFunction, WindowType,
};
use super::Engine;

impl Engine {
    /// Process an incoming event
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn process(&mut self, event: Event) -> Result<(), super::error::EngineError> {
        self.events_processed += 1;
        self.process_inner(Arc::new(event)).await
    }

    /// Process a pre-wrapped SharedEvent (zero-copy path for context pipelines)
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn process_shared(
        &mut self,
        event: SharedEvent,
    ) -> Result<(), super::error::EngineError> {
        self.events_processed += 1;
        self.process_inner(event).await
    }

    /// Internal processing logic shared by process() and process_shared()
    #[tracing::instrument(level = "trace", skip(self))]
    async fn process_inner(&mut self, event: SharedEvent) -> Result<(), super::error::EngineError> {
        // Record incoming event in Prometheus
        if let Some(ref m) = self.metrics {
            m.record_event(&event.event_type);
        }

        // Check for late data against the watermark
        if let Some(ref tracker) = self.watermark_tracker {
            if let Some(effective_wm) = tracker.effective_watermark() {
                if event.timestamp < effective_wm {
                    // Event is behind the watermark — check allowed lateness per stream
                    let mut allowed = false;
                    if let Some(stream_names) = self.router.get_routes(&event.event_type) {
                        for sn in stream_names.iter() {
                            if let Some(cfg) = self.late_data_configs.get(sn) {
                                if event.timestamp >= effective_wm - cfg.allowed_lateness {
                                    allowed = true;
                                    break;
                                }
                            }
                        }
                    }
                    if !allowed && !self.late_data_configs.is_empty() {
                        // Route to side-output if configured, otherwise drop
                        let mut routed = false;
                        if let Some(stream_names) = self.router.get_routes(&event.event_type) {
                            for sn in stream_names.iter() {
                                if let Some(cfg) = self.late_data_configs.get(sn) {
                                    if let Some(ref side_stream) = cfg.side_output_stream {
                                        debug!(
                                            "Routing late event to side-output '{}' type={} ts={}",
                                            side_stream, event.event_type, event.timestamp
                                        );
                                        // Create a late-data event with metadata
                                        let mut late_event = (*event).clone();
                                        late_event.event_type = side_stream.clone().into();
                                        self.send_output(late_event);
                                        routed = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if !routed {
                            debug!(
                                "Dropping late event type={} ts={} (watermark={})",
                                event.event_type, event.timestamp, effective_wm
                            );
                        }
                        return Ok(());
                    }
                }
            }
        }

        // Process events with depth limit to prevent infinite loops
        // Each event carries its depth level - use SharedEvent to avoid cloning
        let mut pending_events: VecDeque<(SharedEvent, usize)> =
            VecDeque::from([(event.clone(), 0)]);
        const MAX_CHAIN_DEPTH: usize = 10;

        // Observe event in watermark tracker (after processing to not block)
        if let Some(ref mut tracker) = self.watermark_tracker {
            tracker.observe_event(&event.event_type, event.timestamp);

            if let Some(new_wm) = tracker.effective_watermark() {
                if self.last_applied_watermark.is_none_or(|last| new_wm > last) {
                    self.last_applied_watermark = Some(new_wm);
                    // Note: we don't call apply_watermark_to_windows here to avoid
                    // double-mutable-borrow. The caller should periodically flush.
                }
            }
        }

        // Process events iteratively, feeding output to dependent streams
        while let Some((current_event, depth)) = pending_events.pop_front() {
            // Prevent infinite loops by limiting chain depth
            if depth >= MAX_CHAIN_DEPTH {
                debug!(
                    "Max chain depth reached for event type: {}",
                    current_event.event_type
                );
                continue;
            }

            // Collect stream names to avoid borrowing issues
            // PERF: Arc<[String]> clone is O(1) - just atomic increment, not deep copy
            let stream_names: Arc<[String]> = self
                .router
                .get_routes(&current_event.event_type)
                .cloned()
                .unwrap_or_else(|| Arc::from([]));

            for stream_name in stream_names.iter() {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    let start = std::time::Instant::now();
                    let result = Self::process_stream_with_functions(
                        stream,
                        Arc::clone(&current_event),
                        &self.functions,
                        self.sinks.cache(),
                    )
                    .await?;

                    // Record per-stream processing metrics
                    if let Some(ref m) = self.metrics {
                        m.record_processing(stream_name, start.elapsed().as_secs_f64());
                    }

                    // Check if we need to send output_events to the output channel.
                    // This is true when there are no .emit() events AND the stream has
                    // a .process() UDF or a .to() sink (so sink events appear in the
                    // live event stream / WebSocket relay).
                    let send_outputs = result.emitted_events.is_empty()
                        && stream
                            .operations
                            .iter()
                            .any(|op| matches!(op, RuntimeOp::Process(_) | RuntimeOp::To(_)));

                    // Send emitted events to output channel (non-blocking)
                    // PERF: Use send_output_shared for zero-copy when using SharedEvent channel
                    if self.output_channel.is_some() {
                        for emitted in &result.emitted_events {
                            self.output_events_emitted += 1;
                            if let Some(ref m) = self.metrics {
                                m.record_output_event("pipeline", &emitted.event_type);
                            }
                            self.send_output_shared(emitted);
                        }
                        if send_outputs {
                            for output in &result.output_events {
                                self.output_events_emitted += 1;
                                if let Some(ref m) = self.metrics {
                                    m.record_output_event("pipeline", &output.event_type);
                                }
                                self.send_output_shared(output);
                            }
                        }
                    } else {
                        // Benchmark mode: just count, don't send
                        self.output_events_emitted += result.emitted_events.len() as u64;
                        if send_outputs {
                            self.output_events_emitted += result.output_events.len() as u64;
                        }
                    }

                    // Count events sent to connector sinks via .to() operations.
                    // Skip if already counted via output_events above (avoids double-counting).
                    if !send_outputs {
                        self.output_events_emitted += result.sink_events_sent;
                    }

                    // Queue output events for processing by dependent streams
                    for output_event in result.output_events {
                        pending_events.push_back((output_event, depth + 1));
                    }
                }
            }
        }

        // Update active streams gauge
        if let Some(ref m) = self.metrics {
            m.set_stream_count(self.streams.len());
        }

        Ok(())
    }

    /// Process a batch of events for improved throughput.
    /// More efficient than calling process() repeatedly because:
    /// - Pre-allocates SharedEvents in bulk
    /// - Collects output events and sends in batches
    /// - Amortizes async overhead
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn process_batch(
        &mut self,
        events: Vec<Event>,
    ) -> Result<(), super::error::EngineError> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();
        self.events_processed += batch_size as u64;

        // Update Prometheus metrics
        if let Some(ref m) = self.metrics {
            for event in &events {
                m.record_event(&event.event_type);
            }
        }

        // Pre-allocate pending events with capacity for batch + some derived events
        // Use VecDeque so we can process in FIFO order (push_back + pop_front)
        let mut pending_events: VecDeque<(SharedEvent, usize)> =
            VecDeque::with_capacity(batch_size + batch_size / 4);

        // Convert all events to SharedEvents upfront
        for event in events {
            pending_events.push_back((Arc::new(event), 0));
        }

        const MAX_CHAIN_DEPTH: usize = 10;

        // Collect emitted events to send in batch
        let mut emitted_batch: Vec<SharedEvent> = Vec::with_capacity(batch_size / 10);

        // Process all events in FIFO order (critical for sequence patterns!)
        while let Some((current_event, depth)) = pending_events.pop_front() {
            if depth >= MAX_CHAIN_DEPTH {
                debug!(
                    "Max chain depth reached for event type: {}",
                    current_event.event_type
                );
                continue;
            }

            // Get stream names (Arc clone is O(1))
            let stream_names: Arc<[String]> = self
                .router
                .get_routes(&current_event.event_type)
                .cloned()
                .unwrap_or_else(|| Arc::from([]));

            for stream_name in stream_names.iter() {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    let start = std::time::Instant::now();
                    let result = Self::process_stream_with_functions(
                        stream,
                        Arc::clone(&current_event),
                        &self.functions,
                        self.sinks.cache(),
                    )
                    .await?;

                    // Record per-stream processing in Prometheus
                    if let Some(ref m) = self.metrics {
                        m.record_processing(stream_name, start.elapsed().as_secs_f64());
                    }

                    // Collect emitted events for batch sending
                    self.output_events_emitted += result.emitted_events.len() as u64;
                    let has_emitted = !result.emitted_events.is_empty();
                    emitted_batch.extend(result.emitted_events);

                    // If .process() or .to() was used but no .emit(), send output_events
                    // to the output channel so they appear in the live event stream.
                    let forward_outputs = !has_emitted
                        && stream
                            .operations
                            .iter()
                            .any(|op| matches!(op, RuntimeOp::Process(_) | RuntimeOp::To(_)));
                    if forward_outputs {
                        self.output_events_emitted += result.output_events.len() as u64;
                        emitted_batch.extend(result.output_events.iter().map(Arc::clone));
                    }

                    // Count sink events only when not already counted via forwarded outputs
                    if !forward_outputs {
                        self.output_events_emitted += result.sink_events_sent;
                    }

                    // Queue output events (push_back to maintain order)
                    for output_event in result.output_events {
                        pending_events.push_back((output_event, depth + 1));
                    }
                }
            }
        }

        // Send all emitted events in batch (non-blocking to avoid async overhead)
        // PERF: Use send_output_shared to avoid cloning in benchmark mode
        for emitted in &emitted_batch {
            self.send_output_shared(emitted);
        }

        // Update Prometheus output metrics
        if let Some(ref m) = self.metrics {
            for emitted in &emitted_batch {
                m.record_output_event("pipeline", &emitted.event_type);
            }
            m.set_stream_count(self.streams.len());
        }

        Ok(())
    }

    /// Synchronous batch processing for maximum throughput.
    /// Use this when no .to() sink operations are in the pipeline (e.g., benchmark mode).
    /// Avoids async runtime overhead completely.
    pub fn process_batch_sync(
        &mut self,
        events: Vec<Event>,
    ) -> Result<(), super::error::EngineError> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();
        self.events_processed += batch_size as u64;

        // Pre-allocate pending events with capacity for batch + some derived events
        // Use VecDeque so we can process in FIFO order (push_back + pop_front)
        let mut pending_events: VecDeque<(SharedEvent, usize)> =
            VecDeque::with_capacity(batch_size + batch_size / 4);

        // Convert all events to SharedEvents upfront
        for event in events {
            pending_events.push_back((Arc::new(event), 0));
        }

        const MAX_CHAIN_DEPTH: usize = 10;

        // Collect emitted events to send in batch
        let mut emitted_batch: Vec<SharedEvent> = Vec::with_capacity(batch_size / 10);

        // Process all events in FIFO order (critical for sequence patterns!)
        while let Some((current_event, depth)) = pending_events.pop_front() {
            if depth >= MAX_CHAIN_DEPTH {
                debug!(
                    "Max chain depth reached for event type: {}",
                    current_event.event_type
                );
                continue;
            }

            // Get stream names (Arc clone is O(1))
            let stream_names: Arc<[String]> = self
                .router
                .get_routes(&current_event.event_type)
                .cloned()
                .unwrap_or_else(|| Arc::from([]));

            for stream_name in stream_names.iter() {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    // Skip output clone+rename when stream has no downstream routes
                    let skip_rename = self.router.get_routes(stream_name).is_none();
                    let result = Self::process_stream_sync(
                        stream,
                        Arc::clone(&current_event),
                        &self.functions,
                        skip_rename,
                    )?;

                    // Collect emitted events for batch sending
                    self.output_events_emitted += result.emitted_events.len() as u64;
                    let has_emitted = !result.emitted_events.is_empty();
                    emitted_batch.extend(result.emitted_events);

                    // If .process() or .to() was used but no .emit(), send output_events
                    // to the output channel so they appear in the live event stream.
                    let forward_outputs = !has_emitted
                        && stream
                            .operations
                            .iter()
                            .any(|op| matches!(op, RuntimeOp::Process(_) | RuntimeOp::To(_)));
                    if forward_outputs {
                        self.output_events_emitted += result.output_events.len() as u64;
                        emitted_batch.extend(result.output_events.iter().map(Arc::clone));
                    }

                    // Count sink events only when not already counted via forwarded outputs
                    if !forward_outputs {
                        self.output_events_emitted += result.sink_events_sent;
                    }

                    // Queue output events (push_back to maintain order)
                    for output_event in result.output_events {
                        pending_events.push_back((output_event, depth + 1));
                    }
                }
            }
        }

        // Send all emitted events in batch (non-blocking to avoid async overhead)
        // PERF: Use send_output_shared to avoid cloning in benchmark mode
        for emitted in &emitted_batch {
            self.send_output_shared(emitted);
        }

        Ok(())
    }

    /// Synchronous stream processing - no async operations.
    /// Skips .to() sink operations (which are the only async parts).
    /// When `skip_output_rename` is true, output events skip the clone+rename step.
    fn process_stream_sync(
        stream: &mut StreamDefinition,
        event: SharedEvent,
        functions: &FxHashMap<String, UserFunction>,
        skip_output_rename: bool,
    ) -> Result<StreamProcessResult, super::error::EngineError> {
        // For merge sources, check if the event passes the appropriate filter
        if let RuntimeSource::Merge(ref sources) = stream.source {
            let mut passes_filter = false;
            for ms in sources {
                if ms.event_type == *event.event_type {
                    if let Some(ref filter) = ms.filter {
                        let ctx = SequenceContext::new();
                        if let Some(result) = evaluator::eval_expr_with_functions(
                            filter,
                            &event,
                            &ctx,
                            functions,
                            &FxHashMap::default(),
                        ) {
                            if result.as_bool().unwrap_or(false) {
                                passes_filter = true;
                                break;
                            }
                        }
                    } else {
                        passes_filter = true;
                        break;
                    }
                }
            }
            if !passes_filter {
                return Ok(StreamProcessResult {
                    emitted_events: vec![],
                    output_events: vec![],
                    sink_events_sent: 0,
                });
            }
        }

        // For join sources - return empty (join requires async in some paths)
        if matches!(stream.source, RuntimeSource::Join(_)) {
            return Ok(StreamProcessResult {
                emitted_events: vec![],
                output_events: vec![],
                sink_events_sent: 0,
            });
        }

        // Use synchronous pipeline execution
        pipeline::execute_pipeline_sync(
            stream,
            vec![event],
            0,
            pipeline::SkipFlags::none(),
            functions,
            skip_output_rename,
        )
    }

    /// Process a batch of pre-wrapped SharedEvents (zero-copy path for context pipelines)
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn process_batch_shared(
        &mut self,
        events: Vec<SharedEvent>,
    ) -> Result<(), super::error::EngineError> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();
        self.events_processed += batch_size as u64;

        // Update Prometheus metrics
        if let Some(ref m) = self.metrics {
            for event in &events {
                m.record_event(&event.event_type);
            }
        }

        // Use VecDeque so we can process in FIFO order (critical for sequence patterns!)
        let mut pending_events: VecDeque<(SharedEvent, usize)> =
            VecDeque::with_capacity(batch_size + batch_size / 4);

        for event in events {
            pending_events.push_back((event, 0));
        }

        const MAX_CHAIN_DEPTH: usize = 10;

        let mut emitted_batch: Vec<SharedEvent> = Vec::with_capacity(batch_size / 10);

        // Process all events in FIFO order
        while let Some((current_event, depth)) = pending_events.pop_front() {
            if depth >= MAX_CHAIN_DEPTH {
                debug!(
                    "Max chain depth reached for event type: {}",
                    current_event.event_type
                );
                continue;
            }

            let stream_names: Arc<[String]> = self
                .router
                .get_routes(&current_event.event_type)
                .cloned()
                .unwrap_or_else(|| Arc::from([]));

            for stream_name in stream_names.iter() {
                if let Some(stream) = self.streams.get_mut(stream_name) {
                    let start = std::time::Instant::now();
                    let result = Self::process_stream_with_functions(
                        stream,
                        Arc::clone(&current_event),
                        &self.functions,
                        self.sinks.cache(),
                    )
                    .await?;

                    if let Some(ref m) = self.metrics {
                        m.record_processing(stream_name, start.elapsed().as_secs_f64());
                    }

                    self.output_events_emitted += result.emitted_events.len() as u64;
                    let has_emitted = !result.emitted_events.is_empty();
                    emitted_batch.extend(result.emitted_events);

                    // If .process() or .to() was used but no .emit(), send output_events
                    // to the output channel so they appear in the live event stream.
                    let forward_outputs = !has_emitted
                        && stream
                            .operations
                            .iter()
                            .any(|op| matches!(op, RuntimeOp::Process(_) | RuntimeOp::To(_)));
                    if forward_outputs {
                        self.output_events_emitted += result.output_events.len() as u64;
                        emitted_batch.extend(result.output_events.iter().map(Arc::clone));
                    }

                    // Count sink events only when not already counted via forwarded outputs
                    if !forward_outputs {
                        self.output_events_emitted += result.sink_events_sent;
                    }

                    for output_event in result.output_events {
                        pending_events.push_back((output_event, depth + 1));
                    }
                }
            }
        }

        // PERF: Use send_output_shared to avoid cloning in benchmark mode
        for emitted in &emitted_batch {
            self.send_output_shared(emitted);
        }

        // Update Prometheus output metrics
        if let Some(ref m) = self.metrics {
            for emitted in &emitted_batch {
                m.record_output_event("pipeline", &emitted.event_type);
            }
            m.set_stream_count(self.streams.len());
        }

        Ok(())
    }

    async fn process_stream_with_functions(
        stream: &mut StreamDefinition,
        event: SharedEvent,
        functions: &FxHashMap<String, UserFunction>,
        sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
    ) -> Result<StreamProcessResult, super::error::EngineError> {
        // For merge sources, check if the event passes the appropriate filter
        if let RuntimeSource::Merge(ref sources) = stream.source {
            let mut passes_filter = false;
            let mut matched_source_name = None;
            for ms in sources {
                if ms.event_type == *event.event_type {
                    if let Some(ref filter) = ms.filter {
                        let ctx = SequenceContext::new();
                        if let Some(result) = evaluator::eval_expr_with_functions(
                            filter,
                            &event,
                            &ctx,
                            functions,
                            &FxHashMap::default(),
                        ) {
                            if result.as_bool().unwrap_or(false) {
                                passes_filter = true;
                                matched_source_name = Some(&ms.name);
                                break;
                            }
                        }
                    } else {
                        // No filter means it passes
                        passes_filter = true;
                        matched_source_name = Some(&ms.name);
                        break;
                    }
                }
            }
            if !passes_filter {
                return Ok(StreamProcessResult {
                    emitted_events: vec![],
                    output_events: vec![],
                    sink_events_sent: 0,
                });
            }
            // Log which merge source matched (uses ms.name)
            if let Some(source_name) = matched_source_name {
                tracing::trace!("Event matched merge source: {}", source_name);
            }
        }

        // For join sources, route through the JoinBuffer for correlation
        if let RuntimeSource::Join(ref _sources) = stream.source {
            if let Some(ref mut join_buffer) = stream.join_buffer {
                // Determine which source this event came from using the event_type_to_source mapping
                // This maps event types (e.g., "MarketATick") to source names (e.g., "MarketA")
                let source_name = stream
                    .event_type_to_source
                    .get(&*event.event_type)
                    .cloned()
                    .unwrap_or_else(|| event.event_type.to_string());

                tracing::debug!(
                    "Join stream {}: Adding event from source '{}' (event_type: {})",
                    stream.name,
                    source_name,
                    event.event_type
                );

                // Add event to join buffer and try to correlate (join still needs owned Event)
                match join_buffer.add_event(&source_name, (*event).clone()) {
                    Some(correlated_event) => {
                        tracing::debug!(
                            "Join stream {}: Correlated event with {} fields",
                            stream.name,
                            correlated_event.data.len()
                        );
                        // Continue processing with the correlated event
                        return Self::process_join_result(
                            stream,
                            Arc::new(correlated_event),
                            functions,
                            sinks,
                        )
                        .await;
                    }
                    None => {
                        // No correlation yet - need events from all sources
                        tracing::debug!(
                            "Join stream {}: No correlation yet, waiting for more events (buffer stats: {:?})",
                            stream.name,
                            join_buffer.stats()
                        );
                        return Ok(StreamProcessResult {
                            emitted_events: vec![],
                            output_events: vec![],
                            sink_events_sent: 0,
                        });
                    }
                }
            } else {
                tracing::warn!("Join stream {} has no JoinBuffer configured", stream.name);
                return Ok(StreamProcessResult {
                    emitted_events: vec![],
                    output_events: vec![],
                    sink_events_sent: 0,
                });
            }
        }

        // Delegate to unified pipeline execution
        pipeline::execute_pipeline(
            stream,
            vec![event],
            0,
            pipeline::SkipFlags::none(),
            functions,
            sinks,
        )
        .await
    }

    /// Process a join result through the stream operations (skipping join-specific handling)
    async fn process_join_result(
        stream: &mut StreamDefinition,
        correlated_event: SharedEvent,
        functions: &FxHashMap<String, UserFunction>,
        sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
    ) -> Result<StreamProcessResult, super::error::EngineError> {
        // Delegate to unified pipeline with join-specific skip flags
        pipeline::execute_pipeline(
            stream,
            vec![correlated_event],
            0,
            pipeline::SkipFlags::for_join(),
            functions,
            sinks,
        )
        .await
    }

    // =========================================================================
    // Session Window Sweep
    // =========================================================================

    /// Flush all expired session windows and process the resulting events
    /// through the remaining pipeline stages (aggregate, having, select, emit, etc.).
    #[tracing::instrument(skip(self))]
    pub async fn flush_expired_sessions(&mut self) -> Result<(), super::error::EngineError> {
        let now = chrono::Utc::now();
        let stream_names: Vec<String> = self.streams.keys().cloned().collect();

        for stream_name in stream_names {
            // Step 1: Find the window op index and collect expired events
            let (window_idx, expired) = {
                let stream = self.streams.get_mut(&stream_name).unwrap();
                let mut result = Vec::new();
                let mut found_idx = None;

                for (idx, op) in stream.operations.iter_mut().enumerate() {
                    if let RuntimeOp::Window(window) = op {
                        match window {
                            WindowType::Session(w) => {
                                if let Some(events) = w.check_expired(now) {
                                    result = events;
                                }
                                found_idx = Some(idx);
                            }
                            WindowType::PartitionedSession(w) => {
                                for (_key, events) in w.check_expired(now) {
                                    result.extend(events);
                                }
                                found_idx = Some(idx);
                            }
                            _ => {}
                        }
                        // Only process the first session window op per stream
                        if found_idx.is_some() {
                            break;
                        }
                    }
                }
                (found_idx, result)
            };

            if expired.is_empty() {
                continue;
            }

            let window_idx = match window_idx {
                Some(idx) => idx,
                None => continue,
            };

            // Step 2: Process expired events through the post-window pipeline
            let result = Self::process_post_window(
                self.streams.get_mut(&stream_name).unwrap(),
                expired,
                window_idx,
                &self.functions,
                self.sinks.cache(),
            )
            .await?;

            // Step 3: Send emitted events to output channel
            for emitted in &result.emitted_events {
                self.output_events_emitted += 1;
                let owned = (**emitted).clone();
                self.send_output(owned);
            }
        }

        Ok(())
    }

    /// Process events through the pipeline operations that come after the window
    /// at `window_idx`. This runs aggregate, having, select, emit, etc.
    async fn process_post_window(
        stream: &mut StreamDefinition,
        events: Vec<SharedEvent>,
        window_idx: usize,
        functions: &FxHashMap<String, UserFunction>,
        sinks: &FxHashMap<String, Arc<dyn crate::sink::Sink>>,
    ) -> Result<StreamProcessResult, super::error::EngineError> {
        // Delegate to unified pipeline starting after the window with post-window skip flags
        pipeline::execute_pipeline(
            stream,
            events,
            window_idx + 1,
            pipeline::SkipFlags::for_post_window(),
            functions,
            sinks,
        )
        .await
    }

    /// Apply a watermark advance to all windows, triggering closure of expired windows.
    #[tracing::instrument(skip(self))]
    pub(super) async fn apply_watermark_to_windows(
        &mut self,
        wm: DateTime<Utc>,
    ) -> Result<(), super::error::EngineError> {
        let stream_names: Vec<String> = self.streams.keys().cloned().collect();

        for stream_name in stream_names {
            let (window_idx, expired) = {
                let stream = self.streams.get_mut(&stream_name).unwrap();
                let mut result = Vec::new();
                let mut found_idx = None;

                for (idx, op) in stream.operations.iter_mut().enumerate() {
                    if let RuntimeOp::Window(window) = op {
                        let events: Option<Vec<SharedEvent>> = match window {
                            WindowType::Tumbling(w) => w.advance_watermark(wm),
                            WindowType::Sliding(w) => w.advance_watermark(wm),
                            WindowType::Session(w) => w.advance_watermark(wm),
                            WindowType::PartitionedTumbling(w) => {
                                let parts = w.advance_watermark(wm);
                                let all: Vec<_> = parts.into_iter().flat_map(|(_, e)| e).collect();
                                if all.is_empty() {
                                    None
                                } else {
                                    Some(all)
                                }
                            }
                            WindowType::PartitionedSliding(w) => {
                                let parts = w.advance_watermark(wm);
                                let all: Vec<_> = parts.into_iter().flat_map(|(_, e)| e).collect();
                                if all.is_empty() {
                                    None
                                } else {
                                    Some(all)
                                }
                            }
                            WindowType::PartitionedSession(w) => {
                                let parts = w.advance_watermark(wm);
                                let all: Vec<_> = parts.into_iter().flat_map(|(_, e)| e).collect();
                                if all.is_empty() {
                                    None
                                } else {
                                    Some(all)
                                }
                            }
                            _ => None, // Count-based windows don't use watermarks
                        };

                        if let Some(evts) = events {
                            result = evts;
                            found_idx = Some(idx);
                        }
                        break;
                    }
                }
                (found_idx, result)
            };

            if expired.is_empty() {
                continue;
            }

            let window_idx = match window_idx {
                Some(idx) => idx,
                None => continue,
            };

            let result = Self::process_post_window(
                self.streams.get_mut(&stream_name).unwrap(),
                expired,
                window_idx,
                &self.functions,
                self.sinks.cache(),
            )
            .await?;

            for emitted in &result.emitted_events {
                self.output_events_emitted += 1;
                let owned = (**emitted).clone();
                self.send_output(owned);
            }
        }

        Ok(())
    }
}
