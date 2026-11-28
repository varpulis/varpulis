//! SASE+ Engine

use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;

use super::advance::{advance_run_shared, passes_over, OrEffects, OrLeft, RunAdvanceResult};
use super::backpressure::{
    BackpressureStrategy, ProcessResult, ProcessStats, ProcessWarning, SaseExtendedStats,
};
use super::event_index::EventTypeIndex;
use super::event_time::{EventTimeConfig, EventTimeManager, EventTimeResult};
use super::kleene::KleeneLimits;
use super::metrics::SaseMetrics;
use super::nfa::{Nfa, NfaCompiler, StateType};
use super::partition::Partition;
use super::predicate::{eval_predicate, event_matches_state, predicate_references_alias};
use super::run::{OrFamily, Run};
use super::run_index::{IndexPlan, RunIndex};
use super::types::{
    EmissionMode, GlobalNegation, MatchResult, Predicate, SasePattern, SaseStats,
    SelectionStrategy, SharedEvent, TimeSemantics, MAX_ENUMERATION_RESULTS, MAX_KLEENE_EVENTS,
};
use crate::clock::Timestamp;
use crate::ExprEvaluator;

/// Below this many entries the deadline index of the partitioned runs is
/// never rebuilt.
const MIN_DEADLINES_COMPACT_AT: usize = 1024;

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
    /// Partitioned runs for SASEXT. IDX-03: each partition may file its
    /// waiting runs in its own predicate index (`Partition`).
    pub partitioned_runs: FxHashMap<String, Partition>,
    /// The event-time deadline of each partitioned run, with its partition,
    /// in order: the watermark sweep visits the partitions holding a run it
    /// has made due, instead of every partition on every event. An entry can
    /// outlive its run (one that completed or was cancelled first); visiting
    /// it then finds nothing to do.
    partition_deadlines: BTreeSet<(DateTime<Utc>, String)>,
    /// Size at which `partition_deadlines` is rebuilt from the live runs:
    /// twice what the last rebuild left. Entries of runs that completed
    /// early would otherwise stay until their deadline, a day of them with
    /// `within 24h`.
    partition_deadlines_compact_at: usize,
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
    /// User-specified emission mode override. `None` means auto-resolve from
    /// the NFA: greedy patterns (`.increasing()`/`.decreasing()`) → Longest,
    /// otherwise → Each.
    pub(crate) emission_mode_override: Option<EmissionMode>,
    /// IDX-02: when the run index is used (`with_predicate_index`).
    index_mode: IndexMode,
    /// IDX-02: no state of the pattern can be planned, so the index is
    /// never built.
    index_unplannable: bool,
    /// IDX-02: non-partitioned runs filed by the values their next step
    /// compares against. Built on first use, and only while the engine's
    /// shape allows it (`sync_run_index`).
    run_index: Option<RunIndex>,
    /// IDX-03: how the partitions' indexes file runs, planned once for all
    /// of them when the first partition needs one.
    partition_plan: Option<Arc<IndexPlan>>,
}

/// IDX-02: in `Auto` mode, the number of non-partitioned runs from which the
/// engine files them in the predicate index.
///
/// IDX-03: under `partition by`, the number of runs of one partition from
/// which that partition files them in its own index.
pub const INDEX_ON_RUNS: usize = 8;

/// IDX-02: in `Auto` mode, the number of runs under which the engine puts
/// them back in the visited list (IDX-03: of one partition, under
/// `partition by`).
///
/// Lower than [`INDEX_ON_RUNS`], so a count hovering around the threshold
/// does not move runs back and forth.
pub const INDEX_OFF_RUNS: usize = 4;

/// IDX-02: when the predicate index is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexMode {
    /// From [`INDEX_ON_RUNS`] runs on, down to [`INDEX_OFF_RUNS`].
    Auto,
    /// Whatever the number of runs.
    Always,
    /// Never.
    Never,
}

impl IndexMode {
    /// Whether `runs` runs call for an index, given whether one is already
    /// there (the thresholds differ, see [`INDEX_OFF_RUNS`]).
    fn wants(self, runs: usize, indexed: bool) -> bool {
        match self {
            Self::Never => false,
            Self::Always => true,
            Self::Auto if indexed => runs >= INDEX_OFF_RUNS,
            Self::Auto => runs >= INDEX_ON_RUNS,
        }
    }

    /// `VARPULIS_SASE_INDEX=1` forces the index, `0` disables it; anything
    /// else leaves the automatic default.
    fn from_env() -> Self {
        match std::env::var("VARPULIS_SASE_INDEX").as_deref() {
            Ok("1") => Self::Always,
            Ok("0") => Self::Never,
            _ => Self::Auto,
        }
    }
}

impl std::fmt::Debug for SaseEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaseEngine")
            .field("nfa", &self.nfa)
            .field("runs", &self.runs.len())
            .field("parked_runs", &self.parked_run_count())
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
            .field("emission_mode_override", &self.emission_mode_override)
            .finish_non_exhaustive()
    }
}

/// Retain the runs matching `keep` within each partition, then drop partitions
/// that went empty.
///
/// Without the empty-partition removal, high-cardinality or short-lived
/// ("poison") partition keys leave empty `Vec`s behind in `partitioned_runs`
/// forever — an unbounded memory leak for streams partitioned on something like
/// a session id or request id.
///
/// IDX-03: a partition's parked runs expire by their wall-clock deadline
/// (`now`), as `keep` drops the visited ones.
fn retain_and_evict_empty_partitions<F: Fn(&Run) -> bool>(
    partitioned_runs: &mut FxHashMap<String, Partition>,
    keep: F,
    now: Timestamp,
) {
    partitioned_runs.retain(|_key, partition| {
        partition.runs.retain(&keep);
        if let Some(index) = partition.index.as_mut() {
            index.expire_processing_time(now);
        }
        !partition.is_empty()
    });
}

/// IDX-03: build or release the index of `partition` so that it has one
/// exactly when `wanted`. The plan is computed on the first build, from
/// `nfa`, and shared by every partition; when no state can be planned,
/// `unplannable` is set and no index is built.
fn sync_partition_index(
    partition: &mut Partition,
    wanted: bool,
    plan: &mut Option<Arc<IndexPlan>>,
    unplannable: &mut bool,
    nfa: &Nfa,
    expressions: bool,
) {
    if !wanted {
        partition.release_index();
        return;
    }
    if partition.index.is_some() {
        return;
    }
    if plan.is_none() {
        *plan = IndexPlan::new(nfa, expressions);
    }
    let Some(plan) = plan.as_ref() else {
        // No state of this pattern can be planned: stop asking.
        *unplannable = true;
        return;
    };
    let mut index = RunIndex::with_plan(Arc::clone(plan));
    // The runs already waiting move in, as far as they can, with room for
    // half as many again (the threshold is crossed one run at a time).
    let waiting = partition.runs.len();
    index.reserve_exact(waiting + waiting / 2);
    let mut i = 0;
    while i < partition.runs.len() {
        if let Some(key) = index.key_for(&partition.runs[i]) {
            let run = partition.runs.swap_remove(i);
            index.insert(run, key);
        } else {
            i += 1;
        }
    }
    // The list keeps its capacity otherwise: as many whole runs again, empty,
    // in every partition that moved its runs in.
    partition.runs.shrink_to(partition.runs.len());
    partition.index = Some(Box::new(index));
}

/// What happened to `or` families during one pass over the runs: the
/// copies forked (with the `seq` of the run each was copied from), the
/// members that left their `or`, and the alternatives whose copy died
/// otherwise, as (family, first state of the alternative).
#[derive(Default)]
struct OrPass {
    forks: Vec<(Run, u64)>,
    left: Vec<OrLeft>,
    released: Vec<(u64, usize)>,
}

impl OrPass {
    /// Move what a run's advance produced into the pass, leaving `effects`
    /// empty for the next run.
    fn take(&mut self, effects: &mut OrEffects) {
        self.forks.append(&mut effects.forks);
        self.left.append(&mut effects.left);
    }

    /// End the families a member left during the pass. The member that left
    /// first wins; members that left on the same event tie, and the one in
    /// the leftmost alternative (lowest rank) wins, the one created first
    /// among equals, so the outcome depends neither on the order the pass
    /// visited them in nor on how the run forked. Every other member ends:
    /// marked invalidated in `runs` (the caller sweeps them out), dropped
    /// from the forks. Returns the `seq` of the members whose matches this
    /// pass must take back, the losers of a tie.
    fn settle(&mut self, runs: &mut [Run]) -> Vec<u64> {
        if self.left.is_empty() {
            return Vec::new();
        }
        let mut winners: Vec<OrLeft> = Vec::new();
        for &left in &self.left {
            match winners.iter_mut().find(|w| w.family == left.family) {
                Some(w) if (left.rank, left.seq) < (w.rank, w.seq) => *w = left,
                Some(_) => {}
                None => winners.push(left),
            }
        }
        let losers: Vec<u64> = self
            .left
            .iter()
            .filter(|l| !winners.contains(l))
            .map(|l| l.seq)
            .collect();
        let ends = |r: &Run| {
            losers.contains(&r.seq)
                || r.or_families
                    .iter()
                    .any(|m| winners.iter().any(|w| w.family == m.family))
        };
        for run in runs.iter_mut().filter(|r| ends(r)) {
            run.invalidated = true;
        }
        self.forks.retain(|(r, _)| !ends(r));
        self.left.clear();
        losers
    }

    /// Note that `run`, a member of `or` families, ended other than by its
    /// family settling (refuted, evicted): the alternative it followed is
    /// open to the run before the `or` again.
    fn release(&mut self, nfa: &Nfa, run: &Run) {
        released_alternatives(nfa, run, &mut self.released);
    }

    /// Give the released alternatives back to the runs before their `or`.
    fn reopen(&mut self, runs: &mut [Run]) {
        reopen_alternatives(runs, &self.released);
        self.released.clear();
    }
}

/// The alternatives `run` followed in its `or` families, as (family, first
/// state of the alternative). The run before an `or` follows none.
fn released_alternatives(nfa: &Nfa, run: &Run, out: &mut Vec<(u64, usize)>) {
    for m in &run.or_families {
        if !m.forked.is_empty() {
            continue;
        }
        if let Some(&entry) = nfa.or_groups[m.group].entries.get(m.rank) {
            out.push((m.family, entry));
        }
    }
}

/// An evicted run's alternatives, given back to the runs before their `or`.
fn release_evicted(nfa: &Nfa, runs: &mut [Run], victim: &Run) {
    if victim.or_families.is_empty() {
        return;
    }
    let mut released = Vec::new();
    released_alternatives(nfa, victim, &mut released);
    reopen_alternatives(runs, &released);
}

/// The runs before an `or` may start these alternatives again.
fn reopen_alternatives(runs: &mut [Run], released: &[(u64, usize)]) {
    if released.is_empty() {
        return;
    }
    for run in runs.iter_mut() {
        for m in &mut run.or_families {
            m.forked.retain(|&t| !released.contains(&(m.family, t)));
        }
    }
}

/// Build the match a run produces when a deadline completes it.
///
/// Drains `captured` and `stack`, so a run cannot emit twice: the retain that
/// follows drops it, and if it somehow survived it would carry nothing.
///
/// No Kleene bookkeeping, because a negated step takes no Kleene operator —
/// "not B, repeated" is not a thing the grammar offers or the engine could act
/// on.
/// The copies a pass forked, in the order of the runs they were forked
/// from (`parent`, by `seq`), each numbered afresh in that order.
///
/// A copy is numbered when its parent forks it, in the order the pass
/// visits the parents, and that order depends on the list's history and on
/// the predicate index (woken runs come last, in the index's order). Copies
/// are admitted in their numbering, which decides which one a full run cap
/// keeps, and their matches later come out in it. Numbering them by parent
/// makes both independent of the visit order.
fn in_parent_order(mut forks: Vec<(Run, u64)>) -> Vec<(Run, u64)> {
    if forks.len() < 2 {
        return forks;
    }
    // Stable: a parent's own copies keep the order it forked them in.
    forks.sort_by_key(|(fork, parent)| (*parent, fork.seq));
    for (fork, _) in &mut forks {
        fork.seq = Run::next_seq();
    }
    forks
}

/// The matches of several runs, in the order the runs were created: the
/// order the list holds them in depends on its history and on the predicate
/// index, the creation order on neither.
fn in_creation_order(mut keyed: Vec<(u64, MatchResult)>) -> Vec<MatchResult> {
    keyed.sort_by_key(|(seq, _)| *seq);
    keyed.into_iter().map(|(_, m)| m).collect()
}

fn match_from_completed_run(run: &mut Run) -> MatchResult {
    MatchResult {
        captured: std::mem::take(&mut run.captured),
        stack: std::mem::take(&mut run.stack),
        duration: run.started_at.elapsed(),
        kleene_truncated: 0,
        enumeration_truncated: false,
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
            partition_deadlines: BTreeSet::new(),
            partition_deadlines_compact_at: MIN_DEADLINES_COMPACT_AT,
            global_negations: Vec::new(),
            // WITHIN measures the distance between the events themselves, not
            // the distance between their arrivals. Defaulting to event time is
            // what makes `.within()` mean anything on a replayed log; opt back
            // out with `with_processing_time()`.
            time_semantics: TimeSemantics::EventTime,
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
            emission_mode_override: None,
            index_mode: IndexMode::from_env(),
            index_unplannable: false,
            run_index: None,
            partition_plan: None,
        }
    }

    /// IDX-02: index waiting runs by the captured values their next step
    /// compares against, so an event visits the runs it can advance
    /// instead of every run.
    ///
    /// A run waiting for `B where id == a.id` is filed under `a.id`; one
    /// waiting for `B where price >= a.lo and price < a.hi` under the
    /// interval. Runs the index cannot file, or whose state it cannot plan,
    /// stay in the list every event visits. Matches, and their order, are
    /// the same with and without it.
    ///
    /// By default the engine uses the index once it holds
    /// [`INDEX_ON_RUNS`] non-partitioned runs, and stops below
    /// [`INDEX_OFF_RUNS`]: under a few runs, visiting them all is cheaper.
    /// Under `partition by` each partition decides alone, on its own runs:
    /// an event visits only its partition's runs, so that is the count the
    /// index saves visits on.
    /// `true` uses it whatever the number of runs, `false` never. The
    /// `VARPULIS_SASE_INDEX` environment variable sets the default: `1` for
    /// always, `0` for never.
    ///
    /// Inactive (every run visited, as without it) under strict contiguity
    /// or with global negations.
    pub fn with_predicate_index(mut self, enabled: bool) -> Self {
        self.index_mode = if enabled {
            IndexMode::Always
        } else {
            IndexMode::Never
        };
        if !enabled {
            self.release_run_index();
        }
        self
    }

    /// IDX-02: number of runs currently held by the run indexes: the
    /// non-partitioned one and, under `partition by`, every partition's.
    pub fn parked_run_count(&self) -> usize {
        self.listed_parked_count()
            + self
                .partitioned_runs
                .values()
                .map(Partition::parked_run_count)
                .sum::<usize>()
    }

    /// The runs parked in the index of the non-partitioned runs.
    fn listed_parked_count(&self) -> usize {
        self.run_index.as_ref().map_or(0, RunIndex::len)
    }

    /// IDX-02: the runs held by the index of the non-partitioned runs, in no
    /// particular order. A checkpoint must include them: they are not in
    /// `runs`. A partition's are [`Partition::parked_runs`].
    pub fn parked_runs(&self) -> impl Iterator<Item = &Run> {
        self.run_index.iter().flat_map(RunIndex::runs)
    }

    /// IDX-02: drop the runs held by the run indexes, for a restore that is
    /// about to replace every run.
    pub fn discard_parked_runs(&mut self) {
        self.run_index = None;
        for partition in self.partitioned_runs.values_mut() {
            partition.index = None;
        }
    }

    /// IDX-02: put the indexes' runs back in the visited lists and drop the
    /// indexes. They are rebuilt on the next event if the engine still
    /// allows it.
    fn release_run_index(&mut self) {
        if let Some(mut index) = self.run_index.take() {
            self.runs.extend(index.drain());
        }
        for partition in self.partitioned_runs.values_mut() {
            partition.release_index();
        }
    }

    /// IDX-02: whether the shape of the engine allows an index at all.
    fn index_usable(&self) -> bool {
        !self.index_unplannable
            && self.strategy != SelectionStrategy::StrictContiguous
            && self.global_negations.is_empty()
    }

    /// IDX-02: build the index if it was asked for and the engine allows it;
    /// release it if the engine no longer does.
    fn sync_run_index(&mut self) {
        let usable = self.index_usable() && self.partition_by.is_none();
        let wanted = usable
            && self.index_mode.wants(
                self.runs.len() + self.listed_parked_count(),
                self.run_index.is_some(),
            );
        if !wanted {
            self.release_run_index();
        } else if self.run_index.is_none() {
            let expressions = self
                .evaluator
                .as_deref()
                .is_some_and(ExprEvaluator::indexable_comparisons);
            self.run_index = RunIndex::new(&self.nfa, expressions);
            match self.run_index.as_mut() {
                // No state of this pattern can be planned: stop asking.
                None => self.index_unplannable = true,
                // The runs already waiting move in, as far as they can.
                Some(index) => {
                    let mut i = 0;
                    while i < self.runs.len() {
                        if let Some(key) = index.key_for(&self.runs[i]) {
                            let run = self.runs.swap_remove(i);
                            index.insert(run, key);
                        } else {
                            i += 1;
                        }
                    }
                }
            }
        }
    }

    /// IDX-02: file `run` in the index if it can be, else in `runs`.
    fn file_run(&mut self, run: Run) {
        if let Some(index) = self.run_index.as_mut() {
            if let Some(key) = index.key_for(&run) {
                index.insert(run, key);
                return;
            }
        }
        self.runs.push(run);
    }

    /// Override the emission mode for this engine. By default, the engine
    /// auto-resolves: monotonic patterns (`.increasing()`/`.decreasing()`)
    /// use `Longest`, all others use `Each` (one emit per Kleene event).
    pub fn with_emission_mode(mut self, mode: EmissionMode) -> Self {
        self.emission_mode_override = Some(mode);
        self
    }

    /// Resolve the active emission mode for this engine, taking the user
    /// override if set, otherwise auto-detecting from NFA shape.
    pub fn resolved_emission_mode(&self) -> EmissionMode {
        if let Some(m) = self.emission_mode_override {
            return m;
        }
        // Greedy (monotonic) patterns default to Longest — users want one
        // "rising sequence ended" event, not one per data point.
        if self.nfa.has_greedy_kleene() {
            EmissionMode::Longest
        } else {
            // Default for everything else: emit on each Kleene event extension
            EmissionMode::Each
        }
    }

    /// Get the partition-by field name, if set.
    pub fn partition_by(&self) -> Option<&str> {
        self.partition_by.as_deref()
    }

    /// Set the expression evaluator for Expr predicates.
    pub fn set_evaluator(&mut self, evaluator: Arc<dyn ExprEvaluator>) {
        // IDX-02: whether expressions can be planned depends on the
        // evaluator; the index is planned again on the next event.
        self.release_run_index();
        self.index_unplannable = false;
        self.partition_plan = None;
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
    ///
    /// This is the default; the builder is kept so call sites can say so.
    pub fn with_event_time(mut self) -> Self {
        self.time_semantics = TimeSemantics::EventTime;
        self
    }

    /// Measure WITHIN bounds against wall-clock arrival time instead of event
    /// timestamps.
    ///
    /// Only correct for a live stream whose events carry no meaningful time of
    /// their own. On replayed or backfilled data every temporal bound becomes
    /// a function of how fast the reader happens to run, so this is opt-in.
    pub fn with_processing_time(mut self) -> Self {
        self.time_semantics = TimeSemantics::ProcessingTime;
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
    /// Advance the watermark, returning any matches that time completed.
    ///
    /// A negated step finishes when its deadline passes without the forbidden
    /// event, so the only thing that can deliver such a match is time moving —
    /// there is no event to hang it off. This used to return `()`, which left
    /// the completion with nowhere to go.
    #[must_use = "advancing the watermark can complete a negated pattern; \
                  dropping the result loses those matches"]
    pub fn advance_watermark(&mut self, new_watermark: DateTime<Utc>) -> Vec<MatchResult> {
        self.watermark = Some(new_watermark);
        // Cleanup confirms negations itself and returns what they completed,
        // before dropping the runs whose deadline has passed.
        self.cleanup_by_watermark()
    }

    /// Rebuild what a checkpoint does not keep, after its runs were put back:
    /// the constraint of each run waiting out a negated step, and the order
    /// of the partitioned runs' deadlines.
    pub fn resume_after_restore(&mut self) {
        self.restore_pending_negations();
        self.rebuild_partition_deadlines();
    }

    /// Drop the entries whose run is gone: completed or cancelled before its
    /// deadline.
    fn purge_partition_deadlines(&mut self) {
        let partitions = &self.partitioned_runs;
        self.partition_deadlines.retain(|(deadline, key)| {
            partitions.get(key).is_some_and(|partition| {
                partition
                    .all_runs()
                    .any(|run| run.event_time_deadline == Some(*deadline))
            })
        });
        self.partition_deadlines_compact_at =
            (2 * self.partition_deadlines.len()).max(MIN_DEADLINES_COMPACT_AT);
    }

    /// Index the deadlines of the partitioned runs there are now, and no
    /// others.
    fn rebuild_partition_deadlines(&mut self) {
        self.partition_deadlines = self
            .partitioned_runs
            .iter()
            .flat_map(|(key, partition)| {
                partition
                    .all_runs()
                    .filter_map(|run| run.event_time_deadline)
                    .map(|deadline| (deadline, key.clone()))
            })
            .collect();
        self.partition_deadlines_compact_at =
            (2 * self.partition_deadlines.len()).max(MIN_DEADLINES_COMPACT_AT);
    }

    /// Give each run waiting out a negated step its constraint back.
    ///
    /// A checkpoint records the state a run is in, not the constraint that
    /// entering the negated step attached to it, so a restored run had none:
    /// the forbidden event no longer cancelled it, and at its deadline it was
    /// dropped instead of completing. The constraint is the step's, with the
    /// run's deadline, as entering the step built it.
    fn restore_pending_negations(&mut self) {
        let nfa = &self.nfa;
        for run in self.runs.iter_mut().chain(
            self.partitioned_runs
                .values_mut()
                .flat_map(|partition| partition.runs.iter_mut()),
        ) {
            if !run.pending_negations.is_empty() {
                continue;
            }
            // A closure followed by a negated step watches it from inside.
            super::advance::watch_absence_after_closure(nfa, run);
            if !run.pending_negations.is_empty() {
                continue;
            }
            let Some(neg_info) = nfa
                .states
                .get(run.current_state)
                .filter(|state| state.state_type == StateType::Negation)
                .and_then(|state| state.negation_info.as_ref())
            else {
                continue;
            };
            run.pending_negations
                .push(super::negation::NegationConstraint {
                    forbidden_type: neg_info.forbidden_type.clone(),
                    predicate: neg_info.predicate.clone(),
                    deadline: run.deadline,
                    event_time_deadline: run.event_time_deadline,
                    next_state: neg_info.continue_state,
                });
        }
    }

    /// Add a global negation condition that invalidates active runs
    pub fn add_negation(&mut self, event_type: String, predicate: Option<Predicate>) {
        // A global negation must see every run: none may stay parked.
        self.release_run_index();
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

    /// The cap on concurrent runs: on the whole pattern, or on each
    /// partition when the pattern is partitioned. A run that would exceed it
    /// is handled by the backpressure strategy; the default drops it and
    /// counts it in [`Self::total_runs_dropped`].
    pub const fn max_runs(&self) -> usize {
        self.max_runs
    }

    /// [`Self::with_max_runs`] on a running engine. Runs already open are
    /// kept even when they exceed a lower cap; the cap applies to the next
    /// run to open.
    pub fn set_max_runs(&mut self, max: usize) {
        self.max_runs = max;
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
        for run in self.runs.iter().chain(self.parked_runs()) {
            snapshots.push(super::types::RunSnapshot {
                current_state: run.current_state,
                started_at_ns: run
                    .event_time_started_at
                    .map(|t| t.timestamp_nanos_opt().unwrap_or(0))
                    .unwrap_or(0),
            });
        }
        // Also include partitioned runs
        for partition in self.partitioned_runs.values() {
            for run in partition.all_runs() {
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
        self.release_run_index();
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

        // Clean up timed-out runs. A deadline can *complete* a negated step
        // as well as expire a run, so what it returns is carried into this
        // call's output rather than dropped.
        let runs_before_cleanup = self.total_run_count();
        let timeout_matches = self.cleanup_timeouts();
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

        let mut completed = timeout_matches;

        // Process runs
        if let Some(ref partition_field) = self.partition_by.clone() {
            let partition_key = shared_event
                .get(partition_field)
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            let (advanced, consumed) =
                self.process_partition_shared(&partition_key, Arc::clone(&shared_event));
            completed.extend(advanced);

            self.for_each_started(&shared_event, consumed, |this, run| {
                let (added, _) = this.admit_new_run(run, Some(&partition_key), &mut completed);
                if added {
                    this.total_runs_created += 1;
                    this.metrics.record_run_created();
                } else {
                    this.metrics.record_run_dropped();
                }
            });
        } else {
            let (advanced, consumed) = self.process_runs_shared(Arc::clone(&shared_event));
            completed.extend(advanced);

            self.for_each_started(&shared_event, consumed, |this, run| {
                let (added, _) = this.admit_new_run(run, None, &mut completed);
                if added {
                    this.total_runs_created += 1;
                    this.metrics.record_run_created();
                } else {
                    this.metrics.record_run_dropped();
                }
            });
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
            .map(|partition| partition.runs.iter().filter(|r| r.invalidated).count())
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

        // Clean up timed-out runs (a deadline can complete a negated step)
        let timeout_matches = self.cleanup_timeouts();

        // Check global negations
        self.check_global_negations(&event);

        let mut completed = timeout_matches;
        let mode = self.resolved_emission_mode();

        // PERF(Opt7): Borrow partition_by instead of cloning per event
        if let Some(ref partition_field) = self.partition_by {
            let partition_key = event
                .get(partition_field.as_str())
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            let (advanced, consumed) =
                self.process_partition_shared(&partition_key, Arc::clone(&event));
            completed.extend(advanced);

            // Try to start new run with backpressure
            self.for_each_started(&event, consumed, |this, mut run| {
                if let Some(m) = this.post_start_kleene_handling(&mut run, &event, mode) {
                    completed.push(m);
                }
                let (added, warning) =
                    this.admit_new_run(run, Some(&partition_key), &mut completed);
                if added {
                    stats.runs_created += 1;
                    this.total_runs_created += 1;
                }
                if let Some(w) = warning {
                    warnings.push(w);
                }
            });
        } else {
            let (advanced, consumed) = self.process_runs_shared(Arc::clone(&event));
            completed.extend(advanced);

            // Try to start new run with backpressure
            self.for_each_started(&event, consumed, |this, mut run| {
                if let Some(m) = this.post_start_kleene_handling(&mut run, &event, mode) {
                    completed.push(m);
                }
                let (added, warning) = this.admit_new_run(run, None, &mut completed);
                if added {
                    stats.runs_created += 1;
                    this.total_runs_created += 1;
                }
                if let Some(w) = warning {
                    warnings.push(w);
                }
            });
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

    /// When a freshly-started run's current state is a Kleene self-loop, the
    /// first event landed there directly (e.g., for a Kleene-final pattern
    /// like `B+`). The standard Kleene accumulation/emission code in
    /// `advance_run_shared` was bypassed because the run was just created.
    /// This helper applies the equivalent Kleene-entry handling: initializes
    /// `kleene_capture` and produces an `Each`-mode match if appropriate.
    fn post_start_kleene_handling(
        &self,
        run: &mut Run,
        event: &SharedEvent,
        mode: EmissionMode,
    ) -> Option<MatchResult> {
        let state = &self.nfa.states[run.current_state];
        if !(state.state_type == StateType::Kleene && state.self_loop) {
            return None;
        }

        // Initialize kleene_capture and add the first event so Subsets/Longest
        // modes can enumerate or capture later.
        if run.kleene_capture.is_none() {
            let mut kc = crate::kleene::KleeneCapture::new();
            if state.postponed_predicate.is_some() {
                kc.deferred_predicate = state.postponed_predicate.clone();
                kc.needs_zdd = true;
            }
            if mode == EmissionMode::Subsets {
                kc.needs_zdd = true;
            }
            run.kleene_capture = Some(kc);
        }
        if let Some(ref mut kc) = run.kleene_capture {
            if kc.needs_zdd {
                kc.extend(Arc::clone(event), state.alias.clone());
            } else {
                kc.extend_simple(Arc::clone(event), state.alias.clone());
            }
        }

        // For Each mode, emit immediately with current bindings, when the
        // closure ends the pattern
        if mode == EmissionMode::Each && state.has_epsilon_to_accept {
            return Some(MatchResult {
                captured: run.captured.clone(),
                stack: run.stack.clone(),
                duration: run.started_at.elapsed(),
                kleene_truncated: run.kleene_capture.as_ref().map_or(0, |kc| kc.truncated),
                enumeration_truncated: false,
            });
        }
        None
    }

    /// Process a shared event reference (avoids redundant cloning)
    pub fn process_shared(&mut self, event: SharedEvent) -> Vec<MatchResult> {
        let mut completed = Vec::with_capacity(8);

        // Update watermark tracking for event-time processing
        if self.time_semantics == TimeSemantics::EventTime {
            self.update_watermark(&event);
        }

        // Clean up timed-out runs (uses watermark in EventTime mode). A
        // deadline can complete a negated step, so keep what it returns.
        completed.extend(self.cleanup_timeouts());

        // Check global negations - invalidate runs that match
        self.check_global_negations(&event);

        // For greedy Kleene patterns (.increasing()/.decreasing()), only one
        // active run per partition is allowed — new runs would just produce
        // prefix duplicates of the existing run's rising sequence.
        //
        // Same for Kleene-final-from-start patterns (e.g., bare `B+` or
        // `all B as b`): the start state's direct transitions land on a
        // Kleene state, so multiple concurrent runs would produce duplicate
        // emissions on each Kleene event.
        let is_greedy = self.nfa.has_greedy_kleene();
        let is_kleene_final_from_start = self.nfa.is_kleene_final_from_start();
        let suppress_new_runs = is_greedy || is_kleene_final_from_start;
        let mode = self.resolved_emission_mode();

        // PERF(Opt7): Borrow partition_by instead of cloning per event
        if let Some(ref partition_field) = self.partition_by {
            let partition_key = event
                .get(partition_field.as_str())
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            // Process partitioned runs
            let (advanced, consumed) =
                self.process_partition_shared(&partition_key, Arc::clone(&event));
            completed.extend(advanced);

            // For greedy mode, skip starting a new run if an active run already
            // exists in this partition — it captures the full rising sequence.
            let has_active = suppress_new_runs
                && self
                    .partitioned_runs
                    .get(&partition_key)
                    .is_some_and(|partition| {
                        partition.parked_run_count() > 0
                            || partition.runs.iter().any(|r| !r.invalidated)
                    });

            if !has_active {
                // Try to start new run in partition with backpressure
                self.for_each_started(&event, consumed, |this, mut run| {
                    if let Some(m) = this.post_start_kleene_handling(&mut run, &event, mode) {
                        completed.push(m);
                    }
                    let (added, _) = this.admit_new_run(run, Some(&partition_key), &mut completed);
                    if added {
                        this.total_runs_created += 1;
                    }
                });
            }
        } else {
            // Non-partitioned processing
            let (advanced, consumed) = self.process_runs_shared(Arc::clone(&event));
            completed.extend(advanced);

            // For greedy mode and Kleene-final-from-start patterns, skip
            // starting a new run if any active run exists (avoids duplicate
            // emissions on each Kleene event).
            let has_active = suppress_new_runs
                && (self.listed_parked_count() > 0 || self.runs.iter().any(|r| !r.invalidated));

            if !has_active {
                // Try to start new run with backpressure
                self.for_each_started(&event, consumed, |this, mut run| {
                    if let Some(m) = this.post_start_kleene_handling(&mut run, &event, mode) {
                        completed.push(m);
                    }
                    let (added, _) = this.admit_new_run(run, None, &mut completed);
                    if added {
                        this.total_runs_created += 1;
                    }
                });
            }
        }

        self.total_runs_completed += completed.len() as u64;
        completed
    }

    /// Keep a run an event just opened, within the run cap; or, when that
    /// one event completed the pattern (`A or B`, a single step), emit its
    /// match: there is nothing left for a run to wait for.
    fn admit_new_run(
        &mut self,
        mut run: Run,
        partition_key: Option<&str>,
        completed: &mut Vec<MatchResult>,
    ) -> (bool, Option<ProcessWarning>) {
        if self.nfa.states[run.current_state].state_type == StateType::Accept {
            completed.push(match_from_completed_run(&mut run));
            return (true, None);
        }
        match partition_key {
            Some(key) => self.handle_backpressure_partitioned(key, run),
            None => self.handle_backpressure(run),
        }
    }

    /// BP-01: Handle backpressure for non-partitioned runs
    /// Returns (was_added, optional_warning)
    fn handle_backpressure(&mut self, run: Run) -> (bool, Option<ProcessWarning>) {
        if self.runs.len() + self.listed_parked_count() < self.max_runs {
            // IDX-02: the event that started this run has already been
            // looked up, so filing it now cannot offer it that event again.
            self.file_run(run);
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
                            self.runs.len() + self.listed_parked_count(),
                            self.max_runs
                        ),
                    }),
                )
            }
            BackpressureStrategy::EvictOldest => {
                // Find and evict the oldest run. Ties go to the run created
                // first (`seq`), not to the first in the list, whose order
                // depends on its history and on the predicate index.
                let evicted = self.evict_min_by_key(|r| (r.started_at, r.seq));
                self.file_run(run);
                match evicted {
                    Some(age) => {
                        self.total_runs_evicted += 1;
                        (true, Some(ProcessWarning::RunEvicted { age }))
                    }
                    None => (true, None),
                }
            }
            BackpressureStrategy::EvictLeastProgress => {
                // Find and evict the run with least progress
                let evicted = self.evict_min_by_key(|r| (r.stack.len(), r.seq));
                self.file_run(run);
                match evicted {
                    Some(age) => {
                        self.total_runs_evicted += 1;
                        (true, Some(ProcessWarning::RunEvicted { age }))
                    }
                    None => (true, None),
                }
            }
            BackpressureStrategy::Sample { rate } => {
                // Simple deterministic sampling based on run count
                let should_accept =
                    (self.total_runs_created as f64 * rate) as u64 > self.total_runs_dropped;
                if should_accept {
                    // Evict oldest to make room
                    if let Some(age) = self.evict_min_by_key(|r| (r.started_at, r.seq)) {
                        self.file_run(run);
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

    /// BP-01: remove the run with the least `key`, listed or parked, and
    /// return its age. IDX-02: the parked runs are compared where they are,
    /// so evicting does not take the index apart.
    fn evict_min_by_key<K: Ord>(&mut self, key: impl Fn(&Run) -> K) -> Option<Duration> {
        self.evict_min_by_key_except(key, |_| false)
    }

    /// [`Self::evict_min_by_key`] among the runs `spared` does not protect.
    /// An evicted member of an `or` family gives its alternative back to
    /// the run before the `or`.
    fn evict_min_by_key_except<K: Ord>(
        &mut self,
        key: impl Fn(&Run) -> K,
        spared: impl Fn(&Run) -> bool,
    ) -> Option<Duration> {
        let listed = self
            .runs
            .iter()
            .enumerate()
            .filter(|(_, r)| !spared(r))
            .map(|(i, r)| (key(r), i))
            .min_by(|a, b| a.0.cmp(&b.0));
        let parked = self.run_index.as_ref().and_then(|ix| ix.min_by_key(&key));
        let victim = match (listed, parked) {
            (Some((lk, _)), Some((pk, slot))) if pk < lk => self.run_index.as_mut()?.remove(slot),
            (Some((_, i)), _) => self.runs.swap_remove(i),
            (None, Some((_, slot))) => self.run_index.as_mut()?.remove(slot),
            (None, None) => return None,
        };
        if !victim.or_families.is_empty() {
            let mut released = Vec::new();
            released_alternatives(&self.nfa, &victim, &mut released);
            reopen_alternatives(&mut self.runs, &released);
        }
        Some(victim.started_at.elapsed())
    }

    /// File a partitioned run's event-time deadline, for the watermark sweep
    /// to visit that partition when it passes.
    fn index_partition_deadline(&mut self, deadline: DateTime<Utc>, partition_key: &str) {
        if self.partition_deadlines.len() >= self.partition_deadlines_compact_at {
            self.purge_partition_deadlines();
        }
        self.partition_deadlines
            .insert((deadline, partition_key.to_string()));
    }

    /// BP-01: Handle backpressure for partitioned runs
    fn handle_backpressure_partitioned(
        &mut self,
        partition_key: &str,
        run: Run,
    ) -> (bool, Option<ProcessWarning>) {
        if let Some(deadline) = run.event_time_deadline {
            self.index_partition_deadline(deadline, partition_key);
        }
        let nfa = &self.nfa;
        let partition = self
            .partitioned_runs
            .entry(partition_key.to_string())
            .or_default();

        if partition.len() < self.max_runs {
            // IDX-03: the event that started this run has already been
            // looked up, so filing it now cannot offer it that event again.
            partition.file_run(run);
            return (true, None);
        }

        // Evict the run with the least `key`, visited or parked, and file the
        // new one in its place. Ties go to the run created first (`seq`).
        let evict = |partition: &mut Partition, run: Run, oldest: bool| {
            let victim = if oldest {
                partition.remove_min_by_key_except(|r| (r.started_at, r.seq), |_| false)
            } else {
                partition.remove_min_by_key_except(|r| (r.stack.len(), r.seq), |_| false)
            };
            let age = victim.map(|victim| {
                release_evicted(nfa, &mut partition.runs, &victim);
                victim.started_at.elapsed()
            });
            partition.file_run(run);
            age
        };

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
                            partition.len(),
                            self.max_runs
                        ),
                    }),
                )
            }
            BackpressureStrategy::EvictOldest | BackpressureStrategy::EvictLeastProgress => {
                let oldest = matches!(self.backpressure, BackpressureStrategy::EvictOldest);
                match evict(partition, run, oldest) {
                    Some(age) => {
                        self.total_runs_evicted += 1;
                        (true, Some(ProcessWarning::RunEvicted { age }))
                    }
                    None => (true, None),
                }
            }
            BackpressureStrategy::Sample { rate } => {
                let should_accept =
                    (self.total_runs_created as f64 * rate) as u64 > self.total_runs_dropped;
                if should_accept {
                    if partition.is_empty() {
                        // `max_runs` is 0: nothing to evict, nowhere to go.
                        (false, None)
                    } else {
                        let age = evict(partition, run, true);
                        self.total_runs_evicted += 1;
                        (true, age.map(|age| ProcessWarning::RunEvicted { age }))
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
            self.partitioned_runs.values().map(Partition::len).sum()
        } else {
            self.runs.len() + self.listed_parked_count()
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
    ) -> (Vec<MatchResult>, bool) {
        let usable = self.index_usable();
        let mut completed = Vec::with_capacity(4);
        // The creation number of the run behind each entry of `completed`.
        let mut completed_seq: Vec<u64> = Vec::new();
        let mut consumed = false;
        let limits = self.kleene_limits();
        let now = Timestamp::now();
        let mode = self.resolved_emission_mode();
        let processing_time = self.time_semantics == TimeSemantics::ProcessingTime;

        let mut pass = OrPass::default();
        // Filled only by a run at an `or`; reused across the pass.
        let mut effects = OrEffects::default();
        let nfa = &self.nfa;
        let strategy = self.strategy;
        let evaluator = self.evaluator.as_deref();
        let partition_deadlines = &mut self.partition_deadlines;
        if let Some(partition) = self.partitioned_runs.get_mut(partition_key) {
            // IDX-03: the partition's own index, under the same thresholds as
            // the non-partitioned one, counted on this partition's runs.
            let indexed = partition.index.is_some();
            let wanted = usable && self.index_mode.wants(partition.len(), indexed);
            if wanted != indexed {
                sync_partition_index(
                    partition,
                    wanted,
                    &mut self.partition_plan,
                    &mut self.index_unplannable,
                    nfa,
                    evaluator.is_some_and(ExprEvaluator::indexable_comparisons),
                );
            }

            // IDX-03: as for the non-partitioned index, a parked run whose
            // processing-time deadline has passed leaves at this pass, and
            // the parked runs this event may advance leave the index before
            // anything is filed in it, so a run this pass files is not
            // offered the event that moved it a second time.
            let mut woken = Vec::new();
            if let Some(index) = partition.index.as_mut() {
                if processing_time {
                    index.expire_processing_time(now);
                }
                index.take_candidates(&event, &mut woken);
            }

            // Advance one run; `true` if it lives on.
            let mut advance = |run: &mut Run, pass: &mut OrPass| -> bool {
                // An `and` that takes an event stamped before the run's start
                // moves the run's window, deadline included, earlier.
                let deadline_before = run.event_time_deadline;
                let advanced = advance_run_shared(
                    nfa,
                    strategy,
                    run,
                    Arc::clone(&event),
                    limits,
                    now,
                    evaluator,
                    mode,
                    &mut effects,
                );
                if !effects.is_empty() {
                    pass.take(&mut effects);
                }
                if let Some(deadline) = run.event_time_deadline {
                    if deadline_before != Some(deadline) {
                        partition_deadlines.insert((deadline, partition_key.to_string()));
                    }
                }
                if matches!(advanced, RunAdvanceResult::Invalidate) && !run.or_families.is_empty() {
                    pass.release(nfa, run);
                }
                let keep = Self::apply_advance(advanced, &mut completed, &mut consumed);
                completed_seq.resize(completed.len(), run.seq);
                keep
            };

            let mut i = 0;
            while i < partition.runs.len() {
                let run = &mut partition.runs[i];
                if run.is_timed_out() || run.invalidated {
                    partition.runs.swap_remove(i);
                    continue;
                }

                // WITHIN bounds the distance between the events, so an event
                // whose own timestamp is past the run's deadline cannot extend
                // it — even when the watermark still lags behind the deadline
                // because of `max_out_of_orderness`. Skip rather than drop: a
                // later-arriving, earlier-stamped event may still complete this
                // run. Culling stays with the watermark, which is the only
                // thing that can promise no such event is coming.
                if run.event_time_excludes(event.timestamp)
                    || passes_over(nfa, strategy, run, &event)
                {
                    i += 1;
                    continue;
                }

                if !advance(run, &mut pass) {
                    partition.runs.swap_remove(i);
                    continue;
                }

                // IDX-03: a run now waiting on a step the index can plan
                // moves into it. The swap brings an unvisited run to `i`.
                if let Some(index) = partition.index.as_mut() {
                    if let Some(key) = index.key_for(&partition.runs[i]) {
                        let run = partition.runs.swap_remove(i);
                        index.insert(run, key);
                        continue;
                    }
                }
                i += 1;
            }

            // IDX-03: the woken runs, exactly as the pass above treats a run.
            for mut run in woken {
                if run.is_timed_out() || run.invalidated {
                    continue;
                }
                if run.event_time_excludes(event.timestamp)
                    || passes_over(nfa, strategy, &run, &event)
                    || advance(&mut run, &mut pass)
                {
                    partition.file_run(run);
                }
            }

            // Runs of an `or` family stay visited (the index does not park
            // them), so ending a family's other members is a sweep of the
            // visited runs.
            pass.reopen(&mut partition.runs);
            if !pass.left.is_empty() {
                let losers = pass.settle(&mut partition.runs);
                partition.runs.retain(|r| !r.invalidated);
                if !losers.is_empty() {
                    let mut seqs = completed_seq.iter();
                    completed.retain(|_| seqs.next().is_some_and(|s| !losers.contains(s)));
                    completed_seq.retain(|s| !losers.contains(s));
                }
            }
            // A partition whose runs are all gone leaves the map now: the
            // watermark sweep only visits the partitions with a deadline due.
            if partition.is_empty() && pass.forks.is_empty() {
                self.partitioned_runs.remove(partition_key);
            }
        }
        for (fork, parent) in in_parent_order(std::mem::take(&mut pass.forks)) {
            self.admit_fork(fork, parent, Some(partition_key));
        }

        // Emit in run creation order. The visited runs are in an order that
        // their swap-removes reshuffle, and the index hands runs back in its
        // own; ordering by creation makes the output independent of both, as
        // long as creation itself does not follow the visit order: the copies
        // an `or` forks are numbered by parent (`in_parent_order`). A run's
        // own matches keep their order (stable sort).
        if !completed_seq.is_sorted() {
            completed = in_creation_order(completed_seq.into_iter().zip(completed).collect());
        }

        (completed, consumed)
    }

    /// File a copy a run (`parent`, by `seq`) forked into an `or`
    /// alternative. Within the run cap it is simply filed. At the cap, under
    /// an evicting strategy it evicts a run outside its own family; when it
    /// cannot (a strategy that does not evict, or only its family to evict),
    /// the parent commits to the alternative itself: the copy takes its
    /// place, and the alternatives the parent still held open are lost,
    /// counted as a dropped run.
    fn admit_fork(&mut self, fork: Run, parent: u64, partition_key: Option<&str>) {
        let families: Vec<u64> = fork.or_families.iter().map(|m| m.family).collect();
        let kin = |r: &Run| r.or_families.iter().any(|m| families.contains(&m.family));
        let held = match partition_key {
            Some(key) => self.partitioned_runs.get(key).map_or(0, Partition::len),
            None => self.runs.len() + self.listed_parked_count(),
        };
        let evicting = matches!(
            self.backpressure,
            BackpressureStrategy::EvictOldest | BackpressureStrategy::EvictLeastProgress
        );
        let mut room = held < self.max_runs;
        if !room && evicting {
            let oldest = matches!(self.backpressure, BackpressureStrategy::EvictOldest);
            let evicted = match partition_key {
                None if oldest => self.evict_min_by_key_except(|r| (r.started_at, r.seq), kin),
                None => self.evict_min_by_key_except(|r| (r.stack.len(), r.seq), kin),
                Some(key) => {
                    let nfa = &self.nfa;
                    self.partitioned_runs.get_mut(key).and_then(|partition| {
                        let victim = if oldest {
                            partition.remove_min_by_key_except(|r| (r.started_at, r.seq), kin)
                        } else {
                            partition.remove_min_by_key_except(|r| (r.stack.len(), r.seq), kin)
                        }?;
                        release_evicted(nfa, &mut partition.runs, &victim);
                        Some(victim.started_at.elapsed())
                    })
                }
            };
            if evicted.is_some() {
                self.total_runs_evicted += 1;
                room = true;
            }
        }
        // Either way the copy is now a run of the partition, whose deadline
        // the watermark sweep must find.
        if let (Some(key), Some(deadline)) = (partition_key, fork.event_time_deadline) {
            self.index_partition_deadline(deadline, key);
        }
        if room {
            // A member of an `or` family: never parked.
            let runs = match partition_key {
                Some(key) => {
                    &mut self
                        .partitioned_runs
                        .entry(key.to_string())
                        .or_default()
                        .runs
                }
                None => &mut self.runs,
            };
            runs.push(fork);
            self.total_runs_created += 1;
            return;
        }
        // No room: the parent commits to this alternative.
        let runs = match partition_key {
            Some(key) => {
                &mut self
                    .partitioned_runs
                    .entry(key.to_string())
                    .or_default()
                    .runs
            }
            None => &mut self.runs,
        };
        if let Some(slot) = runs.iter().position(|r| r.seq == parent && !r.invalidated) {
            runs[slot] = fork;
        }
        self.total_runs_dropped += 1;
    }

    fn process_runs_shared(&mut self, event: SharedEvent) -> (Vec<MatchResult>, bool) {
        self.sync_run_index();
        // IDX-02: under processing time a listed run whose deadline has
        // passed leaves when this pass reaches it. A parked run must leave at
        // the same point, not when `cleanup_timeouts` next runs (it is
        // throttled), or it would still count towards capacity and the active
        // runs. After `sync_run_index`, which may have just filed runs.
        if self.time_semantics == TimeSemantics::ProcessingTime {
            if let Some(index) = self.run_index.as_mut() {
                index.expire_processing_time(Timestamp::now());
            }
        }
        let mut completed = Vec::with_capacity(4);
        // The creation number of the run behind each entry of `completed`.
        let mut completed_seq: Vec<u64> = Vec::new();
        let mut consumed = false;
        let limits = self.kleene_limits();
        let now = Timestamp::now();
        let mode = self.resolved_emission_mode();

        // IDX-02: the parked runs this event may advance leave the index
        // before anything is filed in it, so a run this pass files is not
        // offered the event that moved it a second time.
        let mut woken = Vec::new();
        if let Some(index) = self.run_index.as_mut() {
            index.take_candidates(&event, &mut woken);
        }
        let mut pass = OrPass::default();
        // Filled only by a run at an `or`; reused across the pass.
        let mut effects = OrEffects::default();

        let mut i = 0;
        while i < self.runs.len() {
            if self.runs[i].is_timed_out() || self.runs[i].invalidated {
                self.runs.swap_remove(i);
                continue;
            }

            // WITHIN bounds the distance between the events, so an event
            // whose own timestamp is past the run's deadline cannot extend
            // it — even when the watermark still lags behind the deadline
            // because of `max_out_of_orderness`. Skip rather than drop: a
            // later-arriving, earlier-stamped event may still complete this
            // run. Culling stays with the watermark, which is the only
            // thing that can promise no such event is coming.
            if self.runs[i].event_time_excludes(event.timestamp)
                || passes_over(&self.nfa, self.strategy, &self.runs[i], &event)
            {
                i += 1;
                continue;
            }

            let seq = self.runs[i].seq;
            let advanced = advance_run_shared(
                &self.nfa,
                self.strategy,
                &mut self.runs[i],
                Arc::clone(&event),
                limits,
                now,
                self.evaluator.as_deref(),
                mode,
                &mut effects,
            );
            if !effects.is_empty() {
                pass.take(&mut effects);
            }
            if matches!(advanced, RunAdvanceResult::Invalidate)
                && !self.runs[i].or_families.is_empty()
            {
                pass.release(&self.nfa, &self.runs[i]);
            }
            let keep = Self::apply_advance(advanced, &mut completed, &mut consumed);
            completed_seq.resize(completed.len(), seq);
            if !keep {
                self.runs.swap_remove(i);
                continue;
            }

            // IDX-02: a run now waiting on a step the index can plan moves
            // into it. The swap brings an unvisited run to `i`.
            if let Some(index) = self.run_index.as_mut() {
                if let Some(key) = index.key_for(&self.runs[i]) {
                    let run = self.runs.swap_remove(i);
                    index.insert(run, key);
                    continue;
                }
            }
            i += 1;
        }

        // IDX-02: the woken runs, exactly as the pass above treats a run.
        for mut run in woken {
            if run.is_timed_out() || run.invalidated {
                continue;
            }
            if run.event_time_excludes(event.timestamp)
                || passes_over(&self.nfa, self.strategy, &run, &event)
            {
                self.file_run(run);
                continue;
            }
            let seq = run.seq;
            let advanced = advance_run_shared(
                &self.nfa,
                self.strategy,
                &mut run,
                Arc::clone(&event),
                limits,
                now,
                self.evaluator.as_deref(),
                mode,
                &mut effects,
            );
            if !effects.is_empty() {
                pass.take(&mut effects);
            }
            let keep = Self::apply_advance(advanced, &mut completed, &mut consumed);
            completed_seq.resize(completed.len(), seq);
            if keep {
                self.file_run(run);
            }
        }
        // Runs of an `or` family stay listed (the index does not park them),
        // so ending a family's other members is a sweep of the list.
        if !self.nfa.or_groups.is_empty() {
            pass.reopen(&mut self.runs);
            if !pass.left.is_empty() {
                let losers = pass.settle(&mut self.runs);
                self.runs.retain(|r| !r.invalidated);
                if !losers.is_empty() {
                    let keep: Vec<bool> =
                        completed_seq.iter().map(|s| !losers.contains(s)).collect();
                    let mut keep_iter = keep.iter();
                    completed.retain(|_| *keep_iter.next().unwrap_or(&true));
                    completed_seq.retain(|s| !losers.contains(s));
                }
            }
            for (fork, parent) in in_parent_order(std::mem::take(&mut pass.forks)) {
                self.admit_fork(fork, parent, None);
            }
        }

        // Emit in run creation order. The list is visited in an order that
        // its swap-removes reshuffle, and the index hands runs back in its
        // own; ordering by creation makes the output independent of both,
        // the copies an `or` forks being numbered by parent
        // (`in_parent_order`). A run's own matches keep their order (stable
        // sort).
        if !completed_seq.is_sorted() {
            completed = in_creation_order(completed_seq.into_iter().zip(completed).collect());
        }

        (completed, consumed)
    }

    /// Record what advancing a run produced; `true` if the run lives on.
    fn apply_advance(
        result: RunAdvanceResult,
        completed: &mut Vec<MatchResult>,
        consumed: &mut bool,
    ) -> bool {
        match result {
            RunAdvanceResult::Continue => {
                *consumed = true;
                true
            }
            RunAdvanceResult::Complete(result) => {
                *consumed = true;
                completed.push(result);
                false
            }
            RunAdvanceResult::CompleteAndContinue(result) => {
                *consumed = true;
                // Each mode: emit result but keep run active
                completed.push(result);
                true
            }
            RunAdvanceResult::CompleteMulti(results) => {
                *consumed = true;
                completed.extend(results);
                false
            }
            RunAdvanceResult::Drained => {
                *consumed = true;
                // Each mode: matches were already emitted; drop the run silently
                false
            }
            RunAdvanceResult::Invalidate => false,
            RunAdvanceResult::NoMatch => true,
        }
    }

    /// Create a run parked on `state_id`, under the engine's time semantics and
    /// carrying that state's WITHIN deadline (if any).
    ///
    /// Every run-creation path routes through here. Previously each of the four
    /// call sites open-coded the `Run::new` / `Run::new_with_event_time` choice
    /// and only one of them went on to apply `state.timeout`, so a WITHIN bound
    /// on a pattern that starts through an epsilon transition or an AND branch
    /// was silently dropped.
    fn new_run_at(&self, state_id: usize, event: &SharedEvent) -> Run {
        let mut run = match self.time_semantics {
            TimeSemantics::ProcessingTime => Run::new(state_id),
            TimeSemantics::EventTime => Run::new_with_event_time(state_id, event.timestamp),
        };

        if let Some(timeout) = self.nfa.states[state_id].timeout {
            match self.time_semantics {
                TimeSemantics::ProcessingTime => {
                    run.deadline = Some(Timestamp::now() + timeout);
                }
                TimeSemantics::EventTime => {
                    run = run.with_event_time_deadline(timeout);
                }
            }
        }

        run
    }

    /// Start a run for `event` unless, under skip-till-next-match, a run
    /// already took it: `.stnm()` promises that an event participates in at
    /// most one match, and every event that could open the pattern used to
    /// open a run whatever the strategy (four failed logins and a success
    /// made one alert per failure under `.stnm()` as under the default).
    fn try_start_run_after(
        &self,
        event: SharedEvent,
        consumed: bool,
        rest: &mut Vec<Run>,
    ) -> Option<Run> {
        if consumed && self.strategy == SelectionStrategy::SkipTillNextMatch {
            return None;
        }
        self.try_start_run_shared(event, rest)
    }

    /// Hand `f` each run `event` opens. The first comes back by value, as
    /// most events open one run or none; the other members of a family
    /// that starts an `or` go through `rest`, which allocates only then.
    fn for_each_started(
        &mut self,
        event: &SharedEvent,
        consumed: bool,
        mut f: impl FnMut(&mut Self, Run),
    ) {
        let mut rest = Vec::new();
        if let Some(run) = self.try_start_run_after(Arc::clone(event), consumed, &mut rest) {
            f(self, run);
        }
        for run in rest {
            f(self, run);
        }
    }

    /// Start a run for `event`, entering a negated step straight away if the
    /// pattern's next item is one. Several runs when the pattern starts with
    /// an `or` whose alternatives the event starts (`(B -> C) or (B -> D)`),
    /// one family (see [`crate::nfa::OrGroup`]).
    ///
    /// The entry is here rather than at each of the `return` points inside,
    /// so a new one cannot be added that forgets it.
    fn try_start_run_shared(&self, event: SharedEvent, rest: &mut Vec<Run>) -> Option<Run> {
        let mut first = self.try_start_run_inner(event, rest);
        for run in first.iter_mut().chain(rest.iter_mut()) {
            // "A, and then NOT B" must begin waiting for B's absence the
            // moment A matches. Waiting for the next event to notice would
            // mean the absence is only observable when something else
            // arrives, which is precisely what an absence does not guarantee.
            super::advance::enter_negation_if_next(&self.nfa, run);
        }
        first
    }

    /// A run opened by `event` in the AND state `and_id`, if a branch takes it.
    fn start_in_and(&self, and_id: usize, event: &SharedEvent) -> Option<Run> {
        let mut run = self.new_run_at(and_id, event);
        match super::advance::offer_to_and(
            &self.nfa,
            &mut run,
            and_id,
            event,
            Timestamp::now(),
            self.kleene_limits(),
            self.evaluator.as_deref(),
        ) {
            super::advance::AndOffer::Taken | super::advance::AndOffer::Completed => Some(run),
            super::advance::AndOffer::NotTaken | super::advance::AndOffer::Refuted => None,
        }
    }

    /// The run `event` opens at `next_id`, a transition of the start state.
    fn start_at(&self, next_id: usize, event: &SharedEvent) -> Option<Run> {
        let next_state = &self.nfa.states[next_id];
        // AND-01: a run starts in an AND when one of its branches takes
        // the event (or holds it until the sibling it refers to comes).
        if next_state.state_type == StateType::And {
            return self.start_in_and(next_id, event);
        }

        // For Kleene states with self-referencing predicates, the first
        // event has no previous value to compare against. Match event type only.
        let matches = if next_state.state_type == StateType::Kleene
            && next_state.self_loop
            && next_state.alias.as_ref().is_some_and(|a| {
                next_state
                    .predicate
                    .as_ref()
                    .is_some_and(|p| predicate_references_alias(p, a))
            }) {
            next_state
                .event_type
                .as_ref()
                .is_none_or(|et| *event.event_type == *et)
        } else {
            event_matches_state(
                &self.nfa,
                event,
                next_state,
                &EMPTY_CAPTURED,
                self.evaluator.as_deref(),
            )
        };
        if !matches {
            return None;
        }
        let mut run = self.new_run_at(next_id, event);
        // Capture event - use Arc::clone instead of cloning the event
        run.push(Arc::clone(event), next_state.alias.clone());
        Some(run)
    }

    /// The run `event` opens, if any; the other members of a family go to
    /// `rest`, which the caller hands in empty.
    fn try_start_run_inner(&self, event: SharedEvent, rest: &mut Vec<Run>) -> Option<Run> {
        let start = self.nfa.start_state;
        let start_state = &self.nfa.states[start];
        // PERF: Use static empty map instead of allocating on every call
        let empty_captured = &*EMPTY_CAPTURED;

        // The first transition that takes the event opens the run. When it
        // starts an alternative of an `or` that needs more events, every
        // other alternative the event starts opens a run too, all one
        // family; an alternative the event completes wins alone.
        // The group and first member; the others are pushed on `rest`.
        let mut family: Option<(usize, Run)> = None;
        for &next_id in &start_state.transitions {
            let group = self.nfa.or_split(start, next_id);
            if let Some((g, _)) = &family {
                if group != Some(*g) {
                    continue;
                }
            }
            let Some(mut run) = self.start_at(next_id, &event) else {
                continue;
            };
            let open_alternative = group.filter(|&g| {
                !self.nfa.or_groups[g].ends.contains(&next_id)
                    && self.nfa.states[next_id].state_type != StateType::Kleene
            });
            if let Some(g) = open_alternative {
                // Its rank, should it join a family below.
                let rank = self.nfa.or_groups[g]
                    .entries
                    .iter()
                    .position(|&e| e == next_id)
                    .unwrap_or(usize::MAX);
                run.or_families.push(OrFamily {
                    group: g,
                    family: 0,
                    forked: Vec::new(),
                    rank,
                });
            }
            match (open_alternative, &family) {
                (Some(g), None) => family = Some((g, run)),
                (Some(_), Some(_)) => rest.push(run),
                (None, _) => {
                    rest.clear();
                    run.or_families.clear();
                    return Some(run);
                }
            }
        }
        if let Some((_, mut first)) = family {
            if rest.is_empty() {
                // Alone, it has no family.
                first.or_families.clear();
            } else {
                let id = OrFamily::next_id();
                for run in std::iter::once(&mut first).chain(rest.iter_mut()) {
                    if let Some(m) = run.or_families.last_mut() {
                        m.family = id;
                    }
                }
            }
            return Some(first);
        }

        // Check epsilon transitions
        for &eps_id in &start_state.epsilon_transitions {
            let eps_state = &self.nfa.states[eps_id];
            for &next_id in &eps_state.transitions {
                let next_state = &self.nfa.states[next_id];

                // AND-01: Handle AND states in epsilon paths
                if next_state.state_type == StateType::And {
                    if let Some(run) = self.start_in_and(next_id, &event) {
                        return Some(run);
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
                    let mut run = self.new_run_at(next_id, &event);
                    run.push(Arc::clone(&event), next_state.alias.clone());
                    return Some(run);
                }
            }
        }

        None
    }

    /// Drop runs whose deadline has passed, and return the matches that
    /// deadline *completed* — a negated step whose forbidden event never came.
    fn cleanup_timeouts(&mut self) -> Vec<MatchResult> {
        // PERF(Opt5): For ProcessingTime, skip cleanup if we ran recently — the per-run
        // is_timed_out() checks in process_partition_shared/process_runs_shared still
        // catch expired runs. For EventTime, always run since watermark can jump
        // arbitrarily and per-run checks don't use watermark.
        if self.time_semantics == TimeSemantics::ProcessingTime
            && self.last_cleanup.elapsed() < self.cleanup_interval
        {
            return Vec::new();
        }
        self.last_cleanup = Timestamp::now();

        match self.time_semantics {
            TimeSemantics::ProcessingTime => {
                // NEG-01: Confirm negations based on processing time
                let completed = self.confirm_negations_processing_time();
                // Use wall-clock time for timeout check
                self.runs.retain(|r| !r.is_timed_out() && !r.invalidated);
                if let Some(index) = self.run_index.as_mut() {
                    index.expire_processing_time(Timestamp::now());
                }
                retain_and_evict_empty_partitions(
                    &mut self.partitioned_runs,
                    |r| !r.is_timed_out() && !r.invalidated,
                    Timestamp::now(),
                );
                completed
            }
            TimeSemantics::EventTime => {
                // Use watermark for timeout check
                self.cleanup_by_watermark()
            }
        }
    }

    /// NEG-01: Confirm negations based on processing time (deadline passed)
    fn confirm_negations_processing_time(&mut self) -> Vec<MatchResult> {
        let mut keyed = Vec::new();
        for run in &mut self.runs {
            if let Some(m) = Self::confirm_run_negations_processing_time_static(run, &self.nfa) {
                keyed.push((run.seq, m));
            }
        }
        let mut out = in_creation_order(keyed);
        // Each partition's in creation order too: its visited runs are in
        // an order the index, among others, reshuffles.
        for partition in self.partitioned_runs.values_mut() {
            let mut keyed = Vec::new();
            for run in &mut partition.runs {
                if let Some(m) = Self::confirm_run_negations_processing_time_static(run, &self.nfa)
                {
                    keyed.push((run.seq, m));
                }
            }
            out.extend(in_creation_order(keyed));
        }
        out
    }

    fn confirm_run_negations_processing_time_static(
        run: &mut Run,
        nfa: &Nfa,
    ) -> Option<MatchResult> {
        // Check each pending negation
        let mut confirmed_indices = Vec::new();
        for (idx, neg) in run.pending_negations.iter().enumerate() {
            if neg.is_confirmed_processing_time() {
                confirmed_indices.push((idx, neg.next_state));
            }
        }

        // Process confirmations in reverse order to maintain indices
        let mut completed = None;
        for (idx, next_state) in confirmed_indices.into_iter().rev() {
            run.pending_negations.remove(idx);
            // Transition to the continue state
            run.current_state = next_state;

            if nfa.states[next_state].state_type == StateType::Accept {
                completed = Some(match_from_completed_run(run));
            }
        }
        completed
    }

    /// Cleanup runs based on watermark (for event-time processing)
    fn cleanup_by_watermark(&mut self) -> Vec<MatchResult> {
        let Some(watermark) = self.watermark else {
            return Vec::new();
        };
        // NEG-01: Confirm negations based on watermark.
        //
        // Collected before the retain below, which drops every run whose
        // deadline has passed. A run completed by that same deadline is
        // exactly such a run, so confirming and then discarding would
        // throw away the match the confirmation just produced.
        let mut keyed = Vec::new();
        for run in &mut self.runs {
            if let Some(m) =
                Self::confirm_run_negations_event_time_static(run, &self.nfa, watermark)
            {
                keyed.push((run.seq, m));
            }
        }
        let mut completed = in_creation_order(keyed);
        self.runs
            .retain(|r| !r.is_timed_out_event_time(watermark) && !r.invalidated);
        // IDX-02: parked runs have no pending negation to confirm; expiring
        // them is all the watermark does, in deadline order.
        if let Some(index) = self.run_index.as_mut() {
            index.expire_event_time(watermark);
        }

        // Partitioned runs: only the partitions holding a run whose deadline
        // the watermark has passed. Sweeping every partition on every event
        // made an absence over many open keys (orders awaiting an
        // acknowledgement) cost the number of open keys per event.
        while let Some((deadline, _)) = self.partition_deadlines.first() {
            if watermark <= *deadline {
                break;
            }
            let Some((_, key)) = self.partition_deadlines.pop_first() else {
                break;
            };
            let Some(partition) = self.partitioned_runs.get_mut(&key) else {
                continue;
            };
            let mut keyed = Vec::new();
            for run in &mut partition.runs {
                if let Some(m) =
                    Self::confirm_run_negations_event_time_static(run, &self.nfa, watermark)
                {
                    keyed.push((run.seq, m));
                }
            }
            completed.extend(in_creation_order(keyed));
            partition
                .runs
                .retain(|r| !r.is_timed_out_event_time(watermark) && !r.invalidated);
            // IDX-03: as for the non-partitioned index, expiring is all the
            // watermark does to parked runs.
            if let Some(index) = partition.index.as_mut() {
                index.expire_event_time(watermark);
            }
            if partition.is_empty() {
                self.partitioned_runs.remove(&key);
            }
        }
        completed
    }

    fn confirm_run_negations_event_time_static(
        run: &mut Run,
        nfa: &Nfa,
        watermark: DateTime<Utc>,
    ) -> Option<MatchResult> {
        let mut confirmed_indices = Vec::new();
        for (idx, neg) in run.pending_negations.iter().enumerate() {
            if neg.is_confirmed_event_time(watermark) {
                confirmed_indices.push((idx, neg.next_state));
            }
        }

        let mut completed = None;
        for (idx, next_state) in confirmed_indices.into_iter().rev() {
            run.pending_negations.remove(idx);
            run.current_state = next_state;

            // Landing on Accept means the pattern completed *because time
            // passed*, which is the whole point of a negated step: "A
            // happened, and B did not follow within D".
            //
            // This used to read `// Will be handled by the main processing
            // loop`, and it was not: that loop advances runs on an incoming
            // event, and an absence produces none. The match was built, moved
            // to Accept, and dropped by the retain that follows.
            if nfa.states[next_state].state_type == StateType::Accept {
                completed = Some(match_from_completed_run(run));
            }
        }
        completed
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

            // Also check partitioned runs — but only the partition this event
            // belongs to.
            //
            // This loop used to run over `partitioned_runs.values_mut()` with no
            // key filter, unlike the correctly-keyed `process_partition_shared`.
            // With `.partition_by(device)`, one device acknowledging its own
            // alarm invalidated every *other* device's pending match. The
            // predicate-qualified form `.not(X where ...)` masked it, because
            // the predicate usually re-states the correlation by hand; the bare
            // `.not(X)` form had nothing to save it.
            //
            // The key is derived exactly as the routing path derives it,
            // including `unwrap_or_default()` for an event that does not carry
            // the partition field, so a negation event lands in the same
            // partition an ordinary event of that shape would.
            if let Some(ref partition_field) = self.partition_by {
                let key = event
                    .get(partition_field.as_str())
                    .map(|v| v.to_partition_key().into_owned())
                    .unwrap_or_default();
                // IDX-03: global negations keep every index off, so no run
                // of the partition is parked.
                if let Some(partition) = self.partitioned_runs.get_mut(&key) {
                    for run in &mut partition.runs {
                        let should_invalidate = match &negation.predicate {
                            Some(pred) => eval_predicate(
                                pred,
                                event,
                                &run.captured,
                                self.evaluator.as_deref(),
                            ),
                            None => true,
                        };
                        if should_invalidate {
                            run.invalidated = true;
                        }
                    }
                }
            } else {
                for partition in self.partitioned_runs.values_mut() {
                    for run in &mut partition.runs {
                        let should_invalidate = match &negation.predicate {
                            Some(pred) => eval_predicate(
                                pred,
                                event,
                                &run.captured,
                                self.evaluator.as_deref(),
                            ),
                            None => true,
                        };
                        if should_invalidate {
                            run.invalidated = true;
                        }
                    }
                }
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> SaseStats {
        let total_runs: usize = if self.partition_by.is_some() {
            self.partitioned_runs.values().map(Partition::len).sum()
        } else {
            self.runs.len() + self.listed_parked_count()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_and_evict_removes_emptied_partitions() {
        // A "poison" partition whose runs all die must be dropped from the map,
        // not left behind as an empty Vec (the unbounded-growth leak).
        let mut parts: FxHashMap<String, Partition> = FxHashMap::default();
        let dead = || {
            let mut r = Run::new(0);
            r.invalidated = true;
            r
        };
        parts.insert("keep".to_string(), vec![Run::new(0), dead()].into()); // one survives
        parts.insert("poison".to_string(), vec![dead(), dead()].into()); // all die

        retain_and_evict_empty_partitions(&mut parts, |r| !r.invalidated, Timestamp::now());

        assert!(
            parts.contains_key("keep"),
            "a partition with a live run must remain"
        );
        assert_eq!(
            parts["keep"].len(),
            1,
            "dead run pruned within the partition"
        );
        assert!(
            !parts.contains_key("poison"),
            "an emptied partition must be evicted, not kept as an empty Vec"
        );
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn an_evicted_copy_gives_its_alternative_back() {
        // A -> ((B -> C) or D): after A, B the run waits for D and its copy
        // for C. With the copy evicted, the run can take a later B itself.
        let step = |t: &str| SasePattern::Event {
            event_type: t.into(),
            predicate: None,
            alias: Some(t.to_lowercase()),
        };
        let pattern = SasePattern::Seq(vec![
            step("A"),
            SasePattern::Or(
                Box::new(SasePattern::Seq(vec![step("B"), step("C")])),
                Box::new(step("D")),
            ),
        ]);
        let mut engine = SaseEngine::new(pattern);
        let ev = |t: &str| varpulis_core::Event::new(t);
        assert!(engine.process(&ev("A")).is_empty());
        assert!(engine.process(&ev("B")).is_empty());
        assert_eq!(engine.runs.len(), 2);
        let parent = engine
            .runs
            .iter()
            .find(|r| r.or_families.iter().any(|m| !m.forked.is_empty()))
            .map(|r| r.seq)
            .expect("the run before the `or`");
        assert!(engine
            .evict_min_by_key_except(|r| r.seq, |r| r.seq == parent)
            .is_some());
        assert_eq!(engine.runs.len(), 1);
        assert!(engine.process(&ev("B")).is_empty());
        assert_eq!(engine.runs.len(), 2, "the run forked into B again");
        assert_eq!(engine.process(&ev("C")).len(), 1);
    }

    fn keyed(event_type: &str, key: &str, secs: i64) -> varpulis_core::Event {
        let mut event = varpulis_core::Event::new(event_type).with_field("k", key);
        event.timestamp = DateTime::<Utc>::from_timestamp(1_790_000_000 + secs, 0).unwrap();
        event
    }

    fn a_then_b_within_a_minute() -> SaseEngine {
        use crate::builder::PatternBuilder;
        SaseEngine::new(PatternBuilder::within(
            PatternBuilder::seq(vec![
                PatternBuilder::event_as("A", "a"),
                PatternBuilder::event("B"),
            ]),
            Duration::from_mins(1),
        ))
        .with_partition_by("k".to_string())
    }

    #[test]
    fn a_partition_whose_runs_completed_leaves_the_map() {
        // The watermark sweep no longer visits every partition, so a
        // partition emptied by its own events must go when they empty it.
        let mut engine = a_then_b_within_a_minute();
        for i in 0..100 {
            let key = format!("key-{i}");
            let _ = engine.process(&keyed("A", &key, i));
            assert_eq!(engine.process(&keyed("B", &key, i)).len(), 1);
        }
        assert_eq!(engine.stats().partitions, 0);
    }

    #[test]
    fn the_deadline_index_does_not_outgrow_the_runs() {
        // Runs that complete long before their deadline leave entries behind
        // until it passes; the index is rebuilt before they pile up.
        use crate::builder::PatternBuilder;
        let mut engine = SaseEngine::new(PatternBuilder::within(
            PatternBuilder::seq(vec![
                PatternBuilder::event_as("A", "a"),
                PatternBuilder::event("B"),
            ]),
            Duration::from_hours(24),
        ))
        .with_partition_by("k".to_string());
        for i in 0..20_000 {
            let key = format!("key-{i}");
            let _ = engine.process(&keyed("A", &key, i));
            assert_eq!(engine.process(&keyed("B", &key, i)).len(), 1);
        }
        assert_eq!(engine.stats().partitions, 0);
        assert!(
            engine.partition_deadlines.len() <= 2 * MIN_DEADLINES_COMPACT_AT,
            "{} entries for no open run",
            engine.partition_deadlines.len()
        );
    }

    #[test]
    fn a_partition_whose_runs_expired_leaves_the_map() {
        let at = |secs: i64| DateTime::<Utc>::from_timestamp(1_790_000_000 + secs, 0).unwrap();
        let mut engine = a_then_b_within_a_minute();
        // Half the keys open at 0 s (due at 60 s), half at 30 s (due at 90 s).
        for i in 0..100 {
            let _ = engine.process(&keyed(
                "A",
                &format!("key-{i}"),
                if i < 50 { 0 } else { 30 },
            ));
        }
        assert_eq!(engine.stats().partitions, 100);
        let _ = engine.advance_watermark(at(61));
        assert_eq!(engine.stats().partitions, 50);
        let _ = engine.advance_watermark(at(91));
        assert_eq!(engine.stats().partitions, 0);
    }
}
