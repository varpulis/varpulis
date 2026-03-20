//! SASE+ Engine

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;

use super::advance::{advance_run_shared, RunAdvanceResult};
use super::and_op::AndState;
use super::backpressure::{
    BackpressureStrategy, ProcessResult, ProcessStats, ProcessWarning, SaseExtendedStats,
};
use super::event_index::EventTypeIndex;
use super::event_time::{EventTimeConfig, EventTimeManager, EventTimeResult};
use super::kleene::KleeneLimits;
use super::metrics::SaseMetrics;
use super::nfa::{Nfa, NfaCompiler, StateType};
use super::predicate::{eval_predicate, event_matches_state};
use super::run::Run;
use super::types::{
    GlobalNegation, MatchResult, Predicate, SasePattern, SaseStats, SelectionStrategy, SharedEvent,
    TimeSemantics, MAX_ENUMERATION_RESULTS, MAX_KLEENE_EVENTS,
};
use crate::clock::Timestamp;
use crate::ExprEvaluator;

/// PERF: Static empty captured map to avoid allocations in try_start_run
static EMPTY_CAPTURED: LazyLock<FxHashMap<String, SharedEvent>> = LazyLock::new(FxHashMap::default);

/// The SASE+ pattern matching engine.
///
/// Compiles a [`SasePattern`] into an NFA and maintains active partial-match
/// runs, advancing them as new events arrive.  Supports partition-by
/// optimization, backpressure, event-time semantics, and instrumentation.
pub struct SaseEngine {
    /// Compiled NFA
    pub(crate) nfa: Nfa,
    /// Active runs
    pub runs: Vec<Run>,
    /// Maximum concurrent runs
    max_runs: usize,
    /// Event selection strategy
    strategy: SelectionStrategy,
    /// Partition-by field (SASEXT optimization)
    partition_by: Option<String>,
    /// Partitioned runs for SASEXT
    pub partitioned_runs: FxHashMap<String, Vec<Run>>,
    /// Global negation conditions that invalidate active runs
    global_negations: Vec<GlobalNegation>,
    /// Time semantics (processing time vs event time)
    time_semantics: TimeSemantics,
    /// Current watermark (for event-time processing)
    pub watermark: Option<DateTime<Utc>>,
    /// Maximum out-of-orderness tolerance for watermark generation
    max_out_of_orderness: Duration,
    /// Maximum observed event timestamp (for watermark generation)
    pub max_timestamp: Option<DateTime<Utc>>,
    /// BP-01: Backpressure strategy
    backpressure: BackpressureStrategy,
    /// Cumulative count of runs dropped due to backpressure.
    pub total_runs_dropped: u64,
    /// Cumulative count of runs evicted due to backpressure.
    pub total_runs_evicted: u64,
    /// Cumulative count of runs created since engine creation.
    pub total_runs_created: u64,
    /// Cumulative count of runs that completed with a match.
    pub total_runs_completed: u64,
    /// Optional expression evaluator for Expr predicates
    evaluator: Option<Arc<dyn ExprEvaluator>>,
    /// IDX-01: Event type index for O(1) state lookup
    event_type_index: EventTypeIndex,
    /// ET-01: Event-time manager for robust late event handling
    event_time_manager: Option<EventTimeManager>,
    /// ET-01: Event-time configuration
    event_time_config: EventTimeConfig,
    /// MET-01: Comprehensive metrics
    metrics: Arc<SaseMetrics>,
    /// MET-01: Whether instrumented processing is enabled
    instrumentation_enabled: bool,
    /// Maximum events in a single Kleene closure before accumulation stops.
    /// Bounds ZDD enumeration from 2^n growth. Default: `MAX_KLEENE_EVENTS` (20).
    max_kleene_events: u32,
    /// Maximum results emitted by deferred-predicate enumeration.
    /// Default: `MAX_ENUMERATION_RESULTS` (10 000).
    max_enumeration_results: usize,
    /// PERF(Opt5): Last time cleanup_timeouts() actually ran
    last_cleanup: Timestamp,
    /// PERF(Opt5): Minimum interval between cleanup_timeouts() invocations
    cleanup_interval: Duration,
}

impl std::fmt::Debug for SaseEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaseEngine")
            .field("nfa", &self.nfa)
            .field("runs", &self.runs.len())
            .field("max_runs", &self.max_runs)
            .field("strategy", &self.strategy)
            .field("partition_by", &self.partition_by)
            .field("partitioned_runs", &self.partitioned_runs.len())
            .field("global_negations", &self.global_negations)
            .field("time_semantics", &self.time_semantics)
            .field("watermark", &self.watermark)
            .field("max_out_of_orderness", &self.max_out_of_orderness)
            .field("backpressure", &self.backpressure)
            .field("total_runs_dropped", &self.total_runs_dropped)
            .field("total_runs_evicted", &self.total_runs_evicted)
            .field("total_runs_created", &self.total_runs_created)
            .field("total_runs_completed", &self.total_runs_completed)
            .field("instrumentation_enabled", &self.instrumentation_enabled)
            .field("max_kleene_events", &self.max_kleene_events)
            .field("max_enumeration_results", &self.max_enumeration_results)
            .finish_non_exhaustive()
    }
}

impl SaseEngine {
    /// Create a new engine that matches the given pattern.
    pub fn new(pattern: SasePattern) -> Self {
        let nfa = NfaCompiler::new().compile(&pattern);
        // IDX-01: Build event type index from NFA
        let event_type_index = EventTypeIndex::from_nfa(&nfa);
        Self {
            nfa,
            runs: Vec::new(),
            max_runs: 10000,
            strategy: SelectionStrategy::SkipTillAnyMatch,
            partition_by: None,
            partitioned_runs: FxHashMap::default(),
            global_negations: Vec::new(),
            time_semantics: TimeSemantics::ProcessingTime,
            watermark: None,
            max_out_of_orderness: Duration::from_secs(0),
            max_timestamp: None,
            backpressure: BackpressureStrategy::default(),
            total_runs_dropped: 0,
            total_runs_evicted: 0,
            total_runs_created: 0,
            total_runs_completed: 0,
            event_type_index,
            event_time_manager: None,
            event_time_config: EventTimeConfig::default(),
            metrics: Arc::new(SaseMetrics::new()),
            instrumentation_enabled: false,
            max_kleene_events: MAX_KLEENE_EVENTS,
            max_enumeration_results: MAX_ENUMERATION_RESULTS,
            last_cleanup: Timestamp::now(),
            cleanup_interval: Duration::from_millis(100),
            evaluator: None,
        }
    }

    /// Get the partition-by field name, if set.
    pub fn partition_by(&self) -> Option<&str> {
        self.partition_by.as_deref()
    }

    /// Set the expression evaluator for Expr predicates.
    pub fn set_evaluator(&mut self, evaluator: Arc<dyn ExprEvaluator>) {
        self.evaluator = Some(evaluator);
    }

    /// Enable event-time processing with watermarks
    fn kleene_limits(&self) -> KleeneLimits {
        KleeneLimits {
            max_events: self.max_kleene_events,
            max_results: self.max_enumeration_results,
        }
    }

    /// Enable event-time semantics (watermark-based window completion).
    pub fn with_event_time(mut self) -> Self {
        self.time_semantics = TimeSemantics::EventTime;
        self
    }

    /// ET-01: Configure event-time processing with full configuration
    pub fn with_event_time_config(mut self, config: EventTimeConfig) -> Self {
        self.time_semantics = TimeSemantics::EventTime;
        self.max_out_of_orderness = config.max_out_of_orderness;
        self.event_time_config = config.clone();
        self.event_time_manager = Some(EventTimeManager::new(config));
        self
    }

    /// ET-01: Set allowed lateness for late event handling
    pub fn with_allowed_lateness(mut self, duration: Duration) -> Self {
        self.event_time_config.allowed_lateness = duration;
        // Recreate event time manager if it exists
        if self.event_time_manager.is_some() {
            self.event_time_manager = Some(EventTimeManager::new(self.event_time_config.clone()));
        }
        self
    }

    /// Set maximum out-of-orderness tolerance for watermark generation
    /// The watermark will be: max_timestamp - max_out_of_orderness
    pub fn with_max_out_of_orderness(mut self, duration: Duration) -> Self {
        self.max_out_of_orderness = duration;
        self.event_time_config.max_out_of_orderness = duration;
        // Update event time manager if it exists
        if self.event_time_manager.is_some() {
            self.event_time_manager = Some(EventTimeManager::new(self.event_time_config.clone()));
        }
        self
    }

    /// MET-01: Enable instrumented processing for detailed metrics
    pub fn with_instrumentation(mut self) -> Self {
        self.instrumentation_enabled = true;
        self
    }

    /// MET-01: Get access to the metrics
    pub fn metrics(&self) -> &SaseMetrics {
        &self.metrics
    }

    /// MET-01: Get a clone of the metrics Arc (for sharing with other threads)
    pub fn metrics_arc(&self) -> Arc<SaseMetrics> {
        Arc::clone(&self.metrics)
    }

    /// ET-01: Get access to late events that were emitted (if emit_late_events is enabled)
    pub fn take_late_events(&mut self) -> Vec<SharedEvent> {
        if let Some(ref mut etm) = self.event_time_manager {
            etm.take_late_events()
        } else {
            Vec::new()
        }
    }

    /// ET-01: Get the event time manager's late event statistics
    pub fn late_event_stats(&self) -> (u64, u64) {
        if let Some(ref etm) = self.event_time_manager {
            (etm.late_events_accepted(), etm.late_events_dropped())
        } else {
            (0, 0)
        }
    }

    /// Get current time semantics
    pub fn time_semantics(&self) -> TimeSemantics {
        self.time_semantics
    }

    /// Get current watermark
    pub fn watermark(&self) -> Option<DateTime<Utc>> {
        self.watermark
    }

    /// Manually advance the watermark (useful for testing or external control)
    /// NEG-01: This also confirms any pending negations whose deadline has passed
    pub fn advance_watermark(&mut self, new_watermark: DateTime<Utc>) {
        self.watermark = Some(new_watermark);
        // NEG-01: Confirm negations based on watermark first
        self.confirm_negations_event_time(new_watermark);
        // Cleanup runs that have exceeded their event-time deadline
        self.cleanup_by_watermark();
    }

    /// Add a global negation condition that invalidates active runs
    pub fn add_negation(&mut self, event_type: String, predicate: Option<Predicate>) {
        self.global_negations.push(GlobalNegation {
            event_type,
            predicate,
        });
    }

    /// Builder method to add a global negation
    pub fn with_negation(mut self, event_type: String, predicate: Option<Predicate>) -> Self {
        self.add_negation(event_type, predicate);
        self
    }

    /// Set the maximum number of concurrent active runs.
    pub fn with_max_runs(mut self, max: usize) -> Self {
        self.max_runs = max;
        self
    }

    /// Set the event selection strategy (skip-till-any-match, etc.).
    pub fn with_strategy(mut self, strategy: SelectionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// BP-01: Set the backpressure strategy
    pub fn with_backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.backpressure = strategy;
        self
    }

    /// Set the maximum number of events accumulated in a single Kleene closure.
    /// With `n` events the ZDD enumerates up to 2^n − 1 combinations, so this
    /// cap bounds the exponential blowup.  Default: 20 (~1 M combinations).
    pub fn with_max_kleene_events(mut self, max: u32) -> Self {
        self.max_kleene_events = max;
        self
    }

    /// Set the maximum number of results emitted by deferred-predicate
    /// enumeration.  Default: 10 000.
    pub fn with_max_enumeration_results(mut self, max: usize) -> Self {
        self.max_enumeration_results = max;
        self
    }

    /// IDX-01: Check if the engine has any interest in events of this type
    /// Can be used for early rejection to avoid unnecessary processing
    pub fn has_interest(&self, event_type: &str) -> bool {
        // Check if any NFA state expects this event type
        if self.event_type_index.has_interest(event_type) {
            return true;
        }
        // Also check global negations
        self.global_negations
            .iter()
            .any(|n| n.event_type == event_type)
    }

    /// Expose the compiled NFA for PMC construction (pattern forecasting).
    pub fn nfa(&self) -> &Nfa {
        &self.nfa
    }

    /// Get lightweight snapshots of active partial match runs.
    /// Used by PST forecasting to assess current pattern progress.
    pub fn active_run_snapshots(&self) -> Vec<super::types::RunSnapshot> {
        let mut snapshots = Vec::new();
        for run in &self.runs {
            snapshots.push(super::types::RunSnapshot {
                current_state: run.current_state,
                started_at_ns: run
                    .event_time_started_at
                    .map(|t| t.timestamp_nanos_opt().unwrap_or(0))
                    .unwrap_or(0),
            });
        }
        // Also include partitioned runs
        for runs in self.partitioned_runs.values() {
            for run in runs {
                snapshots.push(super::types::RunSnapshot {
                    current_state: run.current_state,
                    started_at_ns: run
                        .event_time_started_at
                        .map(|t| t.timestamp_nanos_opt().unwrap_or(0))
                        .unwrap_or(0),
                });
            }
        }
        snapshots
    }

    /// Enable SASEXT partition optimization
    pub fn with_partition_by(mut self, field: String) -> Self {
        self.partition_by = Some(field);
        self
    }

    /// Process an incoming event, returns completed matches
    pub fn process(&mut self, event: &varpulis_core::Event) -> Vec<MatchResult> {
        // PERF-01: Create SharedEvent once and reuse throughout processing
        // This avoids multiple deep clones when the event is captured by multiple runs
        let shared_event = Arc::new(event.clone());
        self.process_shared(shared_event)
    }

    /// BP-01: Process an event with detailed results including warnings and stats
    pub fn process_with_result(&mut self, event: &varpulis_core::Event) -> ProcessResult {
        let shared_event = Arc::new(event.clone());
        self.process_shared_with_result(shared_event)
    }

    /// MET-01: Process an event with full instrumentation and metrics recording
    /// This version records detailed metrics including latency histograms
    pub fn process_instrumented(&mut self, event: &varpulis_core::Event) -> Vec<MatchResult> {
        let start = Timestamp::now();
        let shared_event = Arc::new(event.clone());

        // Record event being processed
        self.metrics.record_event_processed();
        self.metrics.update_last_event_time(event.timestamp);

        // ET-01: Handle event-time with robust late event handling
        if self.time_semantics == TimeSemantics::EventTime {
            if let Some(ref mut etm) = self.event_time_manager {
                match etm.process_event(&shared_event) {
                    EventTimeResult::TooLate { .. } => {
                        self.metrics.record_late_event_dropped();
                        self.metrics.record_event_latency(start.elapsed());
                        return Vec::new();
                    }
                    EventTimeResult::Late { .. } => {
                        self.metrics.record_late_event_accepted();
                    }
                    EventTimeResult::OnTime => {}
                }
                // Update watermark from manager
                self.watermark = etm.watermark();
                if let Some(wm) = self.watermark {
                    self.metrics.update_watermark(wm);
                }
            } else {
                // Fallback to simple watermark update
                self.update_watermark(&shared_event);
            }
        }

        // Clean up timed-out runs
        let runs_before_cleanup = self.total_run_count();
        self.cleanup_timeouts();
        let expired_count = runs_before_cleanup.saturating_sub(self.total_run_count());
        for _ in 0..expired_count {
            self.metrics.record_run_expired();
        }

        // Check global negations
        let invalidated_before = self.count_invalidated_runs();
        self.check_global_negations(&shared_event);
        let newly_invalidated = self.count_invalidated_runs() - invalidated_before;
        for _ in 0..newly_invalidated {
            self.metrics.record_run_invalidated();
        }

        // Check if any state is interested in this event (for "ignored" metric)
        let has_interest = self.has_interest(&shared_event.event_type);

        let mut completed = Vec::new();

        // Process runs
        if let Some(ref partition_field) = self.partition_by.clone() {
            let partition_key = shared_event
                .get(partition_field)
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            completed
                .extend(self.process_partition_shared(&partition_key, Arc::clone(&shared_event)));

            if let Some(run) = self.try_start_run_shared(Arc::clone(&shared_event)) {
                let (added, _) = self.handle_backpressure_partitioned(&partition_key, run);
                if added {
                    self.total_runs_created += 1;
                    self.metrics.record_run_created();
                } else {
                    self.metrics.record_run_dropped();
                }
            }
        } else {
            completed.extend(self.process_runs_shared(Arc::clone(&shared_event)));

            if let Some(run) = self.try_start_run_shared(Arc::clone(&shared_event)) {
                let (added, _) = self.handle_backpressure(run);
                if added {
                    self.total_runs_created += 1;
                    self.metrics.record_run_created();
                } else {
                    self.metrics.record_run_dropped();
                }
            }
        }

        // Record metrics
        if !completed.is_empty() {
            self.metrics.record_event_matched();
            self.metrics.record_matches(completed.len());
            for result in &completed {
                self.metrics.record_run_completed();
                self.metrics.record_match_duration(result.duration);
            }
        } else if !has_interest {
            self.metrics.record_event_ignored();
        }

        self.total_runs_completed += completed.len() as u64;

        // Update peak runs
        self.metrics.update_peak_runs(self.total_run_count());

        // Record latency
        self.metrics.record_event_latency(start.elapsed());

        completed
    }

    /// Helper to count invalidated runs (for metrics)
    fn count_invalidated_runs(&self) -> usize {
        let in_runs = self.runs.iter().filter(|r| r.invalidated).count();
        let in_partitions: usize = self
            .partitioned_runs
            .values()
            .map(|runs| runs.iter().filter(|r| r.invalidated).count())
            .sum();
        in_runs + in_partitions
    }

    /// BP-01: Process shared event with detailed results
    fn process_shared_with_result(&mut self, event: SharedEvent) -> ProcessResult {
        let mut warnings = Vec::new();
        let mut stats = ProcessStats::default();

        // Check utilization and warn if approaching limit
        let current_runs = self.total_run_count();
        let utilization = current_runs as f64 / self.max_runs as f64;
        if utilization > 0.8 {
            warnings.push(ProcessWarning::ApproachingLimit {
                current: current_runs,
                max: self.max_runs,
                utilization,
            });
        }

        // Update watermark tracking for event-time processing
        if self.time_semantics == TimeSemantics::EventTime {
            self.update_watermark(&event);
        }

        // Clean up timed-out runs
        self.cleanup_timeouts();

        // Check global negations
        self.check_global_negations(&event);

        let mut completed = Vec::new();

        // PERF(Opt7): Borrow partition_by instead of cloning per event
        if let Some(ref partition_field) = self.partition_by {
            let partition_key = event
                .get(partition_field.as_str())
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            completed.extend(self.process_partition_shared(&partition_key, Arc::clone(&event)));

            // Try to start new run with backpressure
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let (added, warning) = self.handle_backpressure_partitioned(&partition_key, run);
                if added {
                    stats.runs_created += 1;
                    self.total_runs_created += 1;
                }
                if let Some(w) = warning {
                    warnings.push(w);
                }
            }
        } else {
            completed.extend(self.process_runs_shared(Arc::clone(&event)));

            // Try to start new run with backpressure
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let (added, warning) = self.handle_backpressure(run);
                if added {
                    stats.runs_created += 1;
                    self.total_runs_created += 1;
                }
                if let Some(w) = warning {
                    warnings.push(w);
                }
            }
        }

        stats.runs_completed = completed.len();
        self.total_runs_completed += completed.len() as u64;
        stats.active_runs = self.total_run_count();

        ProcessResult {
            matches: completed,
            warnings,
            stats,
        }
    }

    /// Process a shared event reference (avoids redundant cloning)
    pub fn process_shared(&mut self, event: SharedEvent) -> Vec<MatchResult> {
        let mut completed = Vec::with_capacity(8);

        // Update watermark tracking for event-time processing
        if self.time_semantics == TimeSemantics::EventTime {
            self.update_watermark(&event);
        }

        // Clean up timed-out runs (uses watermark in EventTime mode)
        self.cleanup_timeouts();

        // Check global negations - invalidate runs that match
        self.check_global_negations(&event);

        // PERF(Opt7): Borrow partition_by instead of cloning per event
        if let Some(ref partition_field) = self.partition_by {
            let partition_key = event
                .get(partition_field.as_str())
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            // Process partitioned runs
            completed.extend(self.process_partition_shared(&partition_key, Arc::clone(&event)));

            // Try to start new run in partition with backpressure
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let (added, _) = self.handle_backpressure_partitioned(&partition_key, run);
                if added {
                    self.total_runs_created += 1;
                }
            }
        } else {
            // Non-partitioned processing
            completed.extend(self.process_runs_shared(Arc::clone(&event)));

            // Try to start new run with backpressure
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let (added, _) = self.handle_backpressure(run);
                if added {
                    self.total_runs_created += 1;
                }
            }
        }

        self.total_runs_completed += completed.len() as u64;
        completed
    }

    /// BP-01: Handle backpressure for non-partitioned runs
    /// Returns (was_added, optional_warning)
    fn handle_backpressure(&mut self, run: Run) -> (bool, Option<ProcessWarning>) {
        if self.runs.len() < self.max_runs {
            self.runs.push(run);
            return (true, None);
        }

        match &self.backpressure {
            BackpressureStrategy::Drop => {
                self.total_runs_dropped += 1;
                (
                    false,
                    Some(ProcessWarning::RunDropped {
                        reason: "Max runs exceeded (Drop strategy)".to_string(),
                    }),
                )
            }
            BackpressureStrategy::Error => {
                // In the simple process() method, we just drop with a warning
                // The process_with_result method allows checking for this
                self.total_runs_dropped += 1;
                (
                    false,
                    Some(ProcessWarning::RunDropped {
                        reason: format!(
                            "Max runs exceeded: {} / {} (Error strategy)",
                            self.runs.len(),
                            self.max_runs
                        ),
                    }),
                )
            }
            BackpressureStrategy::EvictOldest => {
                // Find and evict the oldest run
                if let Some((idx, age)) = self
                    .runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.started_at)
                    .map(|(idx, r)| (idx, r.started_at.elapsed()))
                {
                    self.runs.swap_remove(idx);
                    self.runs.push(run);
                    self.total_runs_evicted += 1;
                    (true, Some(ProcessWarning::RunEvicted { age }))
                } else {
                    self.runs.push(run);
                    (true, None)
                }
            }
            BackpressureStrategy::EvictLeastProgress => {
                // Find and evict the run with least progress
                if let Some((idx, age)) = self
                    .runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.stack.len())
                    .map(|(idx, r)| (idx, r.started_at.elapsed()))
                {
                    self.runs.swap_remove(idx);
                    self.runs.push(run);
                    self.total_runs_evicted += 1;
                    (true, Some(ProcessWarning::RunEvicted { age }))
                } else {
                    self.runs.push(run);
                    (true, None)
                }
            }
            BackpressureStrategy::Sample { rate } => {
                // Simple deterministic sampling based on run count
                let should_accept =
                    (self.total_runs_created as f64 * rate) as u64 > self.total_runs_dropped;
                if should_accept {
                    // Evict oldest to make room
                    if let Some((idx, age)) = self
                        .runs
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, r)| r.started_at)
                        .map(|(idx, r)| (idx, r.started_at.elapsed()))
                    {
                        self.runs.swap_remove(idx);
                        self.runs.push(run);
                        self.total_runs_evicted += 1;
                        (true, Some(ProcessWarning::RunEvicted { age }))
                    } else {
                        (false, None)
                    }
                } else {
                    self.total_runs_dropped += 1;
                    (
                        false,
                        Some(ProcessWarning::RunDropped {
                            reason: "Sampled out".to_string(),
                        }),
                    )
                }
            }
        }
    }

    /// BP-01: Handle backpressure for partitioned runs
    fn handle_backpressure_partitioned(
        &mut self,
        partition_key: &str,
        run: Run,
    ) -> (bool, Option<ProcessWarning>) {
        let partition_runs = self
            .partitioned_runs
            .entry(partition_key.to_string())
            .or_default();

        if partition_runs.len() < self.max_runs {
            partition_runs.push(run);
            return (true, None);
        }

        // Apply same backpressure logic as non-partitioned
        match &self.backpressure {
            BackpressureStrategy::Drop => {
                self.total_runs_dropped += 1;
                (
                    false,
                    Some(ProcessWarning::RunDropped {
                        reason: "Max runs exceeded in partition (Drop strategy)".to_string(),
                    }),
                )
            }
            BackpressureStrategy::Error => {
                self.total_runs_dropped += 1;
                (
                    false,
                    Some(ProcessWarning::RunDropped {
                        reason: format!(
                            "Max runs exceeded in partition: {} / {}",
                            partition_runs.len(),
                            self.max_runs
                        ),
                    }),
                )
            }
            BackpressureStrategy::EvictOldest => {
                if let Some((idx, age)) = partition_runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.started_at)
                    .map(|(idx, r)| (idx, r.started_at.elapsed()))
                {
                    partition_runs.swap_remove(idx);
                    partition_runs.push(run);
                    self.total_runs_evicted += 1;
                    (true, Some(ProcessWarning::RunEvicted { age }))
                } else {
                    partition_runs.push(run);
                    (true, None)
                }
            }
            BackpressureStrategy::EvictLeastProgress => {
                if let Some((idx, age)) = partition_runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.stack.len())
                    .map(|(idx, r)| (idx, r.started_at.elapsed()))
                {
                    partition_runs.swap_remove(idx);
                    partition_runs.push(run);
                    self.total_runs_evicted += 1;
                    (true, Some(ProcessWarning::RunEvicted { age }))
                } else {
                    partition_runs.push(run);
                    (true, None)
                }
            }
            BackpressureStrategy::Sample { rate } => {
                let should_accept =
                    (self.total_runs_created as f64 * rate) as u64 > self.total_runs_dropped;
                if should_accept {
                    if let Some((idx, age)) = partition_runs
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, r)| r.started_at)
                        .map(|(idx, r)| (idx, r.started_at.elapsed()))
                    {
                        partition_runs.swap_remove(idx);
                        partition_runs.push(run);
                        self.total_runs_evicted += 1;
                        (true, Some(ProcessWarning::RunEvicted { age }))
                    } else {
                        (false, None)
                    }
                } else {
                    self.total_runs_dropped += 1;
                    (
                        false,
                        Some(ProcessWarning::RunDropped {
                            reason: "Sampled out".to_string(),
                        }),
                    )
                }
            }
        }
    }

    /// Get total run count across all partitions
    fn total_run_count(&self) -> usize {
        if self.partition_by.is_some() {
            self.partitioned_runs.values().map(|v| v.len()).sum()
        } else {
            self.runs.len()
        }
    }

    // =========================================================================
    // PERF-01: SharedEvent versions of internal methods
    // These avoid redundant cloning by accepting pre-wrapped Arc<Event>
    // =========================================================================

    fn process_partition_shared(
        &mut self,
        partition_key: &str,
        event: SharedEvent,
    ) -> Vec<MatchResult> {
        let mut completed = Vec::with_capacity(4);
        let limits = self.kleene_limits();
        // PERF(Opt2): Capture time once per event instead of per-push
        let now = Timestamp::now();

        if let Some(runs) = self.partitioned_runs.get_mut(partition_key) {
            let mut i = 0;
            while i < runs.len() {
                if runs[i].is_timed_out() || runs[i].invalidated {
                    runs.swap_remove(i);
                    continue;
                }

                match advance_run_shared(
                    &self.nfa,
                    self.strategy,
                    &mut runs[i],
                    Arc::clone(&event),
                    limits,
                    now,
                    self.evaluator.as_deref(),
                ) {
                    RunAdvanceResult::Continue => i += 1,
                    RunAdvanceResult::Complete(result) => {
                        completed.push(result);
                        runs.swap_remove(i);
                    }
                    RunAdvanceResult::CompleteAndContinue(result) => {
                        // For `all` patterns: emit result but keep run active
                        completed.push(result);
                        i += 1;
                    }
                    RunAdvanceResult::CompleteMulti(results) => {
                        completed.extend(results);
                        runs.swap_remove(i);
                    }
                    RunAdvanceResult::Invalidate => {
                        runs.swap_remove(i);
                    }
                    RunAdvanceResult::NoMatch => i += 1,
                }
            }
        }

        completed
    }

    fn process_runs_shared(&mut self, event: SharedEvent) -> Vec<MatchResult> {
        let mut completed = Vec::with_capacity(4);
        let limits = self.kleene_limits();
        // PERF(Opt2): Capture time once per event instead of per-push
        let now = Timestamp::now();
        let mut i = 0;

        while i < self.runs.len() {
            if self.runs[i].is_timed_out() || self.runs[i].invalidated {
                self.runs.swap_remove(i);
                continue;
            }

            // PERF: Mutate run in place instead of clone + copy back
            match advance_run_shared(
                &self.nfa,
                self.strategy,
                &mut self.runs[i],
                Arc::clone(&event),
                limits,
                now,
                self.evaluator.as_deref(),
            ) {
                RunAdvanceResult::Continue => i += 1,
                RunAdvanceResult::Complete(result) => {
                    completed.push(result);
                    self.runs.swap_remove(i);
                }
                RunAdvanceResult::CompleteAndContinue(result) => {
                    // For `all` patterns: emit result but keep run active
                    completed.push(result);
                    i += 1;
                }
                RunAdvanceResult::CompleteMulti(results) => {
                    completed.extend(results);
                    self.runs.swap_remove(i);
                }
                RunAdvanceResult::Invalidate => {
                    self.runs.swap_remove(i);
                }
                RunAdvanceResult::NoMatch => i += 1,
            }
        }

        completed
    }

    fn try_start_run_shared(&self, event: SharedEvent) -> Option<Run> {
        let start_state = &self.nfa.states[self.nfa.start_state];
        // PERF: Use static empty map instead of allocating on every call
        let empty_captured = &*EMPTY_CAPTURED;

        // Check if event matches any transition from start
        for &next_id in &start_state.transitions {
            let next_state = &self.nfa.states[next_id];

            // AND-01: Handle AND states specially - check if event matches any branch
            if next_state.state_type == StateType::And {
                if let Some(ref config) = next_state.and_config {
                    // Check if event matches any branch
                    for (idx, branch) in config.branches.iter().enumerate() {
                        if *event.event_type == branch.event_type {
                            let pred_matches = branch.predicate.as_ref().is_none_or(|p| {
                                eval_predicate(p, &event, empty_captured, self.evaluator.as_deref())
                            });

                            if pred_matches {
                                let mut run = match self.time_semantics {
                                    TimeSemantics::ProcessingTime => Run::new(next_id),
                                    TimeSemantics::EventTime => {
                                        Run::new_with_event_time(next_id, event.timestamp)
                                    }
                                };

                                // Initialize AND state and complete this branch
                                let mut and_state = AndState::new();
                                and_state.complete_branch(idx, Arc::clone(&event));
                                run.and_state = Some(and_state);

                                // Capture with alias if present
                                if let Some(ref alias) = branch.alias {
                                    run.captured.insert(alias.clone(), Arc::clone(&event));
                                }
                                run.push(Arc::clone(&event), branch.alias.clone());

                                return Some(run);
                            }
                        }
                    }
                }
                continue;
            }

            if event_matches_state(
                &self.nfa,
                &event,
                next_state,
                empty_captured,
                self.evaluator.as_deref(),
            ) {
                let mut run = match self.time_semantics {
                    TimeSemantics::ProcessingTime => Run::new(next_id),
                    TimeSemantics::EventTime => Run::new_with_event_time(next_id, event.timestamp),
                };

                // Set deadline if state has timeout
                if let Some(timeout) = next_state.timeout {
                    match self.time_semantics {
                        TimeSemantics::ProcessingTime => {
                            run.deadline = Some(Timestamp::now() + timeout);
                        }
                        TimeSemantics::EventTime => {
                            // Set event-time deadline based on first event's timestamp
                            run = run.with_event_time_deadline(timeout);
                        }
                    }
                }

                // Capture event - use Arc::clone instead of cloning the event
                run.push(Arc::clone(&event), next_state.alias.clone());

                return Some(run);
            }
        }

        // Check epsilon transitions
        for &eps_id in &start_state.epsilon_transitions {
            let eps_state = &self.nfa.states[eps_id];
            for &next_id in &eps_state.transitions {
                let next_state = &self.nfa.states[next_id];

                // AND-01: Handle AND states in epsilon paths
                if next_state.state_type == StateType::And {
                    if let Some(ref config) = next_state.and_config {
                        for (idx, branch) in config.branches.iter().enumerate() {
                            if *event.event_type == branch.event_type {
                                let pred_matches = branch.predicate.as_ref().is_none_or(|p| {
                                    eval_predicate(
                                        p,
                                        &event,
                                        empty_captured,
                                        self.evaluator.as_deref(),
                                    )
                                });

                                if pred_matches {
                                    let mut run = match self.time_semantics {
                                        TimeSemantics::ProcessingTime => Run::new(next_id),
                                        TimeSemantics::EventTime => {
                                            Run::new_with_event_time(next_id, event.timestamp)
                                        }
                                    };

                                    let mut and_state = AndState::new();
                                    and_state.complete_branch(idx, Arc::clone(&event));
                                    run.and_state = Some(and_state);

                                    if let Some(ref alias) = branch.alias {
                                        run.captured.insert(alias.clone(), Arc::clone(&event));
                                    }
                                    run.push(Arc::clone(&event), branch.alias.clone());

                                    return Some(run);
                                }
                            }
                        }
                    }
                    continue;
                }

                if event_matches_state(
                    &self.nfa,
                    &event,
                    next_state,
                    empty_captured,
                    self.evaluator.as_deref(),
                ) {
                    let mut run = match self.time_semantics {
                        TimeSemantics::ProcessingTime => Run::new(next_id),
                        TimeSemantics::EventTime => {
                            Run::new_with_event_time(next_id, event.timestamp)
                        }
                    };
                    run.push(Arc::clone(&event), next_state.alias.clone());
                    return Some(run);
                }
            }
        }

        None
    }

    fn cleanup_timeouts(&mut self) {
        // PERF(Opt5): For ProcessingTime, skip cleanup if we ran recently — the per-run
        // is_timed_out() checks in process_partition_shared/process_runs_shared still
        // catch expired runs. For EventTime, always run since watermark can jump
        // arbitrarily and per-run checks don't use watermark.
        if self.time_semantics == TimeSemantics::ProcessingTime
            && self.last_cleanup.elapsed() < self.cleanup_interval
        {
            return;
        }
        self.last_cleanup = Timestamp::now();

        match self.time_semantics {
            TimeSemantics::ProcessingTime => {
                // NEG-01: Confirm negations based on processing time
                self.confirm_negations_processing_time();
                // Use wall-clock time for timeout check
                self.runs.retain(|r| !r.is_timed_out() && !r.invalidated);
                for runs in self.partitioned_runs.values_mut() {
                    runs.retain(|r| !r.is_timed_out() && !r.invalidated);
                }
            }
            TimeSemantics::EventTime => {
                // Use watermark for timeout check
                self.cleanup_by_watermark();
            }
        }
    }

    /// NEG-01: Confirm negations based on processing time (deadline passed)
    fn confirm_negations_processing_time(&mut self) {
        for run in &mut self.runs {
            Self::confirm_run_negations_processing_time_static(run, &self.nfa);
        }
        for runs in self.partitioned_runs.values_mut() {
            for run in runs.iter_mut() {
                Self::confirm_run_negations_processing_time_static(run, &self.nfa);
            }
        }
    }

    fn confirm_run_negations_processing_time_static(run: &mut Run, nfa: &Nfa) {
        // Check each pending negation
        let mut confirmed_indices = Vec::new();
        for (idx, neg) in run.pending_negations.iter().enumerate() {
            if neg.is_confirmed_processing_time() {
                confirmed_indices.push((idx, neg.next_state));
            }
        }

        // Process confirmations in reverse order to maintain indices
        for (idx, next_state) in confirmed_indices.into_iter().rev() {
            run.pending_negations.remove(idx);
            // Transition to the continue state
            run.current_state = next_state;

            // Check if next state is accept
            let state = &nfa.states[next_state];
            if state.state_type == StateType::Accept {
                // Will be handled by the main processing loop
            }
        }
    }

    /// Cleanup runs based on watermark (for event-time processing)
    fn cleanup_by_watermark(&mut self) {
        if let Some(watermark) = self.watermark {
            // NEG-01: Confirm negations based on watermark
            self.confirm_negations_event_time(watermark);

            self.runs
                .retain(|r| !r.is_timed_out_event_time(watermark) && !r.invalidated);
            for runs in self.partitioned_runs.values_mut() {
                runs.retain(|r| !r.is_timed_out_event_time(watermark) && !r.invalidated);
            }
        }
    }

    /// NEG-01: Confirm negations based on event-time watermark
    fn confirm_negations_event_time(&mut self, watermark: DateTime<Utc>) {
        for run in &mut self.runs {
            Self::confirm_run_negations_event_time_static(run, &self.nfa, watermark);
        }
        for runs in self.partitioned_runs.values_mut() {
            for run in runs.iter_mut() {
                Self::confirm_run_negations_event_time_static(run, &self.nfa, watermark);
            }
        }
    }

    fn confirm_run_negations_event_time_static(run: &mut Run, nfa: &Nfa, watermark: DateTime<Utc>) {
        let mut confirmed_indices = Vec::new();
        for (idx, neg) in run.pending_negations.iter().enumerate() {
            if neg.is_confirmed_event_time(watermark) {
                confirmed_indices.push((idx, neg.next_state));
            }
        }

        for (idx, next_state) in confirmed_indices.into_iter().rev() {
            run.pending_negations.remove(idx);
            run.current_state = next_state;

            let state = &nfa.states[next_state];
            if state.state_type == StateType::Accept {
                // Will be handled by the main processing loop
            }
        }
    }

    /// Update watermark based on incoming event timestamp
    fn update_watermark(&mut self, event: &varpulis_core::Event) {
        let event_ts = event.timestamp;

        // Track maximum observed timestamp
        match self.max_timestamp {
            Some(max_ts) if event_ts > max_ts => {
                self.max_timestamp = Some(event_ts);
            }
            None => {
                self.max_timestamp = Some(event_ts);
            }
            _ => {}
        }

        // Update watermark: max_timestamp - max_out_of_orderness
        if let Some(max_ts) = self.max_timestamp {
            let new_watermark =
                max_ts - chrono::Duration::from_std(self.max_out_of_orderness).unwrap_or_default();
            self.watermark = Some(new_watermark);
        }
    }

    /// Check global negation conditions and invalidate matching runs
    fn check_global_negations(&mut self, event: &varpulis_core::Event) {
        // PERF(Opt6): Skip iteration when no global negations are configured
        if self.global_negations.is_empty() {
            return;
        }
        for negation in &self.global_negations {
            // Check if event type matches the negation
            if *event.event_type != negation.event_type {
                continue;
            }

            // Invalidate runs where the predicate matches (or all runs if no predicate)
            for run in &mut self.runs {
                let should_invalidate = match &negation.predicate {
                    Some(pred) => {
                        eval_predicate(pred, event, &run.captured, self.evaluator.as_deref())
                    }
                    None => true, // No predicate means any event of this type invalidates
                };
                if should_invalidate {
                    run.invalidated = true;
                }
            }

            // Also check partitioned runs
            for runs in self.partitioned_runs.values_mut() {
                for run in runs.iter_mut() {
                    let should_invalidate = match &negation.predicate {
                        Some(pred) => {
                            eval_predicate(pred, event, &run.captured, self.evaluator.as_deref())
                        }
                        None => true,
                    };
                    if should_invalidate {
                        run.invalidated = true;
                    }
                }
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> SaseStats {
        let total_runs: usize = if self.partition_by.is_some() {
            self.partitioned_runs.values().map(|v| v.len()).sum()
        } else {
            self.runs.len()
        };

        SaseStats {
            active_runs: total_runs,
            partitions: self.partitioned_runs.len(),
            nfa_states: self.nfa.states.len(),
        }
    }

    /// BP-01: Get extended statistics including backpressure metrics
    pub fn extended_stats(&self) -> SaseExtendedStats {
        let total_runs = self.total_run_count();
        let utilization = total_runs as f64 / self.max_runs as f64;

        SaseExtendedStats {
            active_runs: total_runs,
            partitions: self.partitioned_runs.len(),
            nfa_states: self.nfa.states.len(),
            total_runs_dropped: self.total_runs_dropped,
            total_runs_evicted: self.total_runs_evicted,
            total_runs_created: self.total_runs_created,
            total_runs_completed: self.total_runs_completed,
            utilization,
        }
    }
}
