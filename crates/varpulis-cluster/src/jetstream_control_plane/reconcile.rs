//! The reconciliation loop that replaces the Raft log's total order.
//!
//! The argument for *why* this is safe without a global order is in the
//! [`super`] module docs. This module is the machine itself: a pure
//! transition function ([`step`]) plus a level-triggered driver
//! ([`Reconciler::tick`]) that applies its decisions with a compare-and-swap.
//!
//! ## The six phases, and the guard each one waits on
//!
//! Every guard is a predicate over the *current* observed world, never over
//! an event or an ordering. Nothing here reacts to a transition; everything
//! re-derives from state.
//!
//! | phase          | advances when                                           | side effect |
//! |----------------|---------------------------------------------------------|-------------|
//! | `Checkpointing`| a restore point is resolvable — a durable group checkpoint exists, or the source is observably alive so the executor can checkpoint it over HTTP | — |
//! | `Deploying`    | the **target** reports the pipeline in `assigned_pipelines` | — |
//! | `Restoring`    | the executor has acked the state restore (`restored`)    | — |
//! | `Switching`    | the **source** is observably fenced, or gone             | [`Effect::FenceSource`] while it is not |
//! | `CleaningUp`   | the source no longer reports the pipeline, or is gone    | — |
//! | `Completed` / `Failed` | the linger window has passed                    | retire the record |
//!
//! Any non-terminal phase past its deadline goes to `Failed`.
//!
//! ## Where the cut-over actually happens
//!
//! `Switching` is the only dangerous transition — it is the instant at which
//! two workers could both believe they own the pipeline and both write to its
//! sinks. It is therefore **not** implemented as "ask the source to stop"
//! (a request that a partitioned source never receives). It is implemented as
//! a compare-and-swap that bumps `workers/<source>`, after which the source's
//! own next heartbeat CAS fails and it self-fences — and, if it is
//! partitioned and cannot even attempt that CAS, its lease expires on its own
//! clock. See [`super::fence`].
//!
//! Because the cut-over is one CAS on one key, it needs no agreement between
//! coordinators: the store decides the winner. That single fact is what lets
//! the rest of the machine be eventually-consistent without being unsafe.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::apply::WorkerRecord;
use super::fence::{fence_out, STATUS_FENCED};
use super::keys::{self, ControlKey};
use super::store::{ControlPlane, ControlPlaneError, Expect, Snapshot};

/// Phase of a migration. Ordered: [`MigrationPhase::rank`] never decreases
/// along a legal transition, which is what removes the ABA hazard that a
/// per-key revision alone would leave open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    Checkpointing,
    Deploying,
    Restoring,
    Switching,
    CleaningUp,
    Completed,
    Failed,
}

impl MigrationPhase {
    /// Monotone rank. A transition to a lower rank is illegal and refused by
    /// [`Reconciler::commit`].
    pub fn rank(self) -> u8 {
        match self {
            Self::Checkpointing => 0,
            Self::Deploying => 1,
            Self::Restoring => 2,
            Self::Switching => 3,
            Self::CleaningUp => 4,
            // Terminal states share the top rank: a migration may end either
            // way from any phase, but never un-end.
            Self::Completed | Self::Failed => 5,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// What lives at `migrations/<id>`.
///
/// Deliberately **self-contained**: every field a transition needs is here,
/// so no phase advance requires reading a second key at the same instant.
/// That is what makes the absence of a cross-key atomic snapshot harmless.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationRecord {
    pub id: String,
    pub pipeline: String,
    pub group: String,
    pub source: String,
    pub target: String,
    pub phase: MigrationPhase,
    #[serde(default)]
    pub reason: String,
    /// `workers/<source>` revision observed when the migration was minted.
    /// The fence-out CASes against the *current* revision, not this one; this
    /// is kept for audit and to detect that the source was replaced.
    #[serde(default)]
    pub source_fence: u64,
    /// Checkpoint id the target should restore from, once resolved.
    #[serde(default)]
    pub restore_checkpoint: Option<u64>,
    /// Executor ack: state restore finished. The one guard with no
    /// independent evidence in the control plane.
    #[serde(default)]
    pub restored: bool,
    /// Wall-clock ms at which the migration must be abandoned.
    pub deadline_ms: u64,
    /// Wall-clock ms at which it reached a terminal phase.
    #[serde(default)]
    pub finished_ms: Option<u64>,
    /// Failure detail, when `phase == Failed`.
    #[serde(default)]
    pub failure: Option<String>,
}

/// A level-triggered observation of one worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerObservation {
    pub fence: u64,
    pub status: String,
    pub assigned: Vec<String>,
}

impl WorkerObservation {
    pub fn is_fenced(&self) -> bool {
        self.status == STATUS_FENCED
    }
    pub fn is_ready(&self) -> bool {
        self.status == "ready"
    }
    pub fn runs(&self, pipeline: &str) -> bool {
        self.assigned.iter().any(|p| p == pipeline)
    }
}

/// Everything [`step`] is allowed to look at, read from one snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Evidence {
    pub source: Option<WorkerObservation>,
    pub target: Option<WorkerObservation>,
    /// Latest durable checkpoint for the group, if any.
    pub latest_checkpoint: Option<u64>,
}

/// A side effect the driver must perform. Every one is idempotent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Revoke the source worker's grant, CASing at the revision just observed.
    FenceSource { worker: String, observed_fence: u64 },
}

/// What the transition function decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Guard not satisfied. Re-evaluated next tick from fresh state; nothing
    /// is remembered, so a missed tick costs latency and never correctness.
    Wait,
    /// Perform a side effect, leave the record alone. The record advances on
    /// a later tick once the effect is *observable*, which is what makes a
    /// crash between effect and record-update harmless.
    Act(Effect),
    /// Compare-and-swap the record to this value.
    Advance(Box<MigrationRecord>),
    /// Terminal and lingered: delete the record.
    Retire,
}

/// Default time a migration may take before it is abandoned.
pub const DEFAULT_MIGRATION_DEADLINE: Duration = Duration::from_mins(2);
/// Default time a terminal record is kept for observability.
pub const DEFAULT_LINGER: Duration = Duration::from_mins(5);

/// The pure transition function.
///
/// Total, deterministic, and a function of `(record, evidence, now)` only —
/// no clock reads, no I/O, no hidden state. Every property claimed in the
/// module docs is a property of *this* function and can be tested without a
/// broker; the driver below only applies what it returns.
pub fn step(rec: &MigrationRecord, ev: &Evidence, now_ms: u64, linger: Duration) -> Decision {
    // Terminal phases: retire once the linger window has passed.
    if rec.phase.is_terminal() {
        let finished = rec.finished_ms.unwrap_or(now_ms);
        return if now_ms.saturating_sub(finished) >= linger.as_millis() as u64 {
            Decision::Retire
        } else {
            Decision::Wait
        };
    }

    // Deadline: a migration that cannot make progress must not pin the
    // pipeline forever. Checked before the guards so a stuck phase always
    // terminates.
    if now_ms >= rec.deadline_ms {
        return Decision::Advance(Box::new(fail(
            rec,
            now_ms,
            format!("deadline exceeded in phase {:?}", rec.phase),
        )));
    }

    match rec.phase {
        MigrationPhase::Checkpointing => {
            // A restore point is resolvable if a durable group checkpoint
            // exists, or the source is alive enough to be checkpointed over
            // HTTP by the executor.
            let source_alive = ev.source.as_ref().is_some_and(|s| !s.is_fenced());
            if ev.latest_checkpoint.is_none() && !source_alive {
                // Neither route available: nothing to restore from and no one
                // to ask. Wait for the deadline rather than deploying a
                // pipeline that would silently start from empty state.
                return Decision::Wait;
            }
            let mut next = rec.clone();
            next.phase = MigrationPhase::Deploying;
            // Stale evidence here can only pick an OLDER durable checkpoint,
            // which is exactly what Raft's monotone `latest_completed` also
            // permitted — it is a floor, not an exact value — and replaying
            // from an older checkpoint is what the sinks' 2PC dedupe handles.
            next.restore_checkpoint = ev.latest_checkpoint;
            Decision::Advance(Box::new(next))
        }

        MigrationPhase::Deploying => {
            // Evidence the deploy landed: the target itself reports the
            // pipeline. Absence of evidence is never taken as evidence — we
            // wait, we never advance on a timeout short of the deadline.
            match &ev.target {
                Some(t) if t.is_fenced() => Decision::Advance(Box::new(fail(
                    rec,
                    now_ms,
                    "target worker was fenced during deploy".to_string(),
                ))),
                Some(t) if t.runs(&rec.pipeline) => {
                    let mut next = rec.clone();
                    next.phase = MigrationPhase::Restoring;
                    Decision::Advance(Box::new(next))
                }
                Some(_) => Decision::Wait,
                None => Decision::Advance(Box::new(fail(
                    rec,
                    now_ms,
                    "target worker disappeared during deploy".to_string(),
                ))),
            }
        }

        MigrationPhase::Restoring => {
            if !rec.restored {
                return Decision::Wait;
            }
            let mut next = rec.clone();
            next.phase = MigrationPhase::Switching;
            Decision::Advance(Box::new(next))
        }

        MigrationPhase::Switching => {
            // The cut-over. Two live writers is the failure mode; fencing the
            // source is what prevents it, and it is a single CAS.
            match &ev.source {
                // Source gone or already fenced: the cut-over is done.
                None => advance_to(rec, MigrationPhase::CleaningUp),
                Some(s) if s.is_fenced() => advance_to(rec, MigrationPhase::CleaningUp),
                Some(s) => Decision::Act(Effect::FenceSource {
                    worker: rec.source.clone(),
                    observed_fence: s.fence,
                }),
            }
        }

        MigrationPhase::CleaningUp => match &ev.source {
            None => complete(rec, now_ms),
            Some(s) if !s.runs(&rec.pipeline) => complete(rec, now_ms),
            Some(_) => Decision::Wait,
        },

        MigrationPhase::Completed | MigrationPhase::Failed => unreachable!("handled above"),
    }
}

fn advance_to(rec: &MigrationRecord, phase: MigrationPhase) -> Decision {
    let mut next = rec.clone();
    next.phase = phase;
    Decision::Advance(Box::new(next))
}

fn complete(rec: &MigrationRecord, now_ms: u64) -> Decision {
    let mut next = rec.clone();
    next.phase = MigrationPhase::Completed;
    next.finished_ms = Some(now_ms);
    Decision::Advance(Box::new(next))
}

fn fail(rec: &MigrationRecord, now_ms: u64, why: String) -> MigrationRecord {
    let mut next = rec.clone();
    next.phase = MigrationPhase::Failed;
    next.finished_ms = Some(now_ms);
    next.failure = Some(why);
    next
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// What one reconciliation pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Migration records examined.
    pub examined: usize,
    /// Records whose phase advanced.
    pub advanced: usize,
    /// Side effects performed (fence-outs).
    pub effects: usize,
    /// Terminal records deleted.
    pub retired: usize,
    /// CASes refused because another coordinator got there first. Not an
    /// error: the loser simply recomputes next tick against the winner's
    /// value.
    pub contended: usize,
    /// Records left waiting on a guard.
    pub waiting: usize,
}

/// The level-triggered reconciler.
#[derive(Debug, Clone)]
pub struct Reconciler {
    cp: ControlPlane,
    linger: Duration,
}

impl Reconciler {
    pub fn new(cp: ControlPlane) -> Self {
        Self {
            cp,
            linger: DEFAULT_LINGER,
        }
    }

    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    /// Read the whole world, decide, and apply — once.
    ///
    /// This is the entire loop body. Calling it more often costs latency
    /// only; calling it less often costs latency only; skipping it entirely
    /// for a while and then resuming is indistinguishable from having been
    /// slow. There is no edge to miss.
    pub async fn tick(&self, now_ms: u64) -> Result<TickReport, ControlPlaneError> {
        let snapshot = self.cp.snapshot().await?;
        self.tick_with(&snapshot, now_ms).await
    }

    /// [`Reconciler::tick`] against an already-read snapshot, so a caller that
    /// needs the snapshot for something else does not read it twice.
    pub async fn tick_with(
        &self,
        snapshot: &Snapshot,
        now_ms: u64,
    ) -> Result<TickReport, ControlPlaneError> {
        let mut report = TickReport::default();

        let migrations: Vec<_> = snapshot
            .iter_namespace::<MigrationRecord>(keys::MIGRATIONS)
            .collect();

        for (key, versioned) in migrations {
            report.examined += 1;
            let rec = versioned.value;
            let evidence = observe(snapshot, &rec);

            match step(&rec, &evidence, now_ms, self.linger) {
                Decision::Wait => report.waiting += 1,

                Decision::Act(Effect::FenceSource {
                    worker,
                    observed_fence,
                }) => {
                    match fence_out(&self.cp, &worker, observed_fence).await {
                        Ok(rev) => {
                            report.effects += 1;
                            tracing::info!(
                                migration = %rec.id,
                                worker = %worker,
                                fence = rev,
                                "fenced source worker at cut-over"
                            );
                        }
                        Err(e) => {
                            // Either another coordinator fenced it first, or
                            // the worker moved on. Both are re-derived next
                            // tick from fresh evidence.
                            report.contended += 1;
                            tracing::debug!(
                                migration = %rec.id,
                                worker = %worker,
                                error = %e,
                                "fence-out did not commit; re-deriving next tick"
                            );
                        }
                    }
                }

                Decision::Advance(next) => {
                    match self.commit(&key, versioned.revision, &rec, &next).await {
                        Ok(true) => report.advanced += 1,
                        Ok(false) => report.contended += 1,
                        Err(e) => return Err(e),
                    }
                }

                Decision::Retire => match self.cp.delete_at(&key, versioned.revision).await {
                    Ok(()) => report.retired += 1,
                    Err(e) if e.is_cas_conflict() => report.contended += 1,
                    Err(e) => return Err(e),
                },
            }
        }

        Ok(report)
    }

    /// Compare-and-swap a migration record forward.
    ///
    /// Returns `Ok(false)` when the CAS was refused — the caller does nothing
    /// and recomputes next tick. Refuses a rank regression outright: CAS
    /// already prevents a lost update, and this additionally prevents a
    /// *logically* backwards transition computed from a stale read.
    async fn commit(
        &self,
        key: &ControlKey,
        revision: u64,
        current: &MigrationRecord,
        next: &MigrationRecord,
    ) -> Result<bool, ControlPlaneError> {
        if next.phase.rank() < current.phase.rank() {
            tracing::warn!(
                migration = %current.id,
                from = ?current.phase,
                to = ?next.phase,
                "refusing a backwards phase transition"
            );
            return Ok(false);
        }
        match self.cp.write(key, next, Expect::Revision(revision)).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_cas_conflict() => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// Build the evidence for one migration from a snapshot.
pub fn observe(snapshot: &Snapshot, rec: &MigrationRecord) -> Evidence {
    let worker = |id: &str| -> Option<WorkerObservation> {
        let v = snapshot.get::<WorkerRecord>(&ControlKey::Worker(id.to_string()))?;
        Some(WorkerObservation {
            fence: v.revision,
            status: v.value.entry.status,
            assigned: v.value.entry.assigned_pipelines,
        })
    };
    let latest_checkpoint = snapshot
        .get::<crate::control_state::GroupCheckpointStatus>(&ControlKey::Checkpoint(
            rec.group.clone(),
        ))
        .and_then(|v| v.value.latest_completed);

    Evidence {
        source: worker(&rec.source),
        target: worker(&rec.target),
        latest_checkpoint,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(phase: MigrationPhase) -> MigrationRecord {
        MigrationRecord {
            id: "m1".into(),
            pipeline: "p1".into(),
            group: "g1".into(),
            source: "w-src".into(),
            target: "w-tgt".into(),
            phase,
            reason: "rebalance".into(),
            source_fence: 10,
            restore_checkpoint: None,
            restored: false,
            deadline_ms: 1_000_000,
            finished_ms: None,
            failure: None,
        }
    }

    fn obs(status: &str, assigned: &[&str], fence: u64) -> WorkerObservation {
        WorkerObservation {
            fence,
            status: status.into(),
            assigned: assigned.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn advanced(d: Decision) -> MigrationRecord {
        match d {
            Decision::Advance(r) => *r,
            other => panic!("expected Advance, got {other:?}"),
        }
    }

    // -- phase-by-phase guards ---------------------------------------------

    #[test]
    fn checkpointing_waits_when_there_is_nothing_to_restore_from() {
        let ev = Evidence {
            source: Some(obs(STATUS_FENCED, &[], 11)),
            target: Some(obs("ready", &[], 20)),
            latest_checkpoint: None,
        };
        assert_eq!(
            step(&rec(MigrationPhase::Checkpointing), &ev, 0, DEFAULT_LINGER),
            Decision::Wait,
            "must not deploy a pipeline that would start from empty state"
        );
    }

    #[test]
    fn checkpointing_advances_on_a_durable_checkpoint_even_if_the_source_is_dead() {
        let ev = Evidence {
            source: None,
            target: Some(obs("ready", &[], 20)),
            latest_checkpoint: Some(7),
        };
        let next = advanced(step(
            &rec(MigrationPhase::Checkpointing),
            &ev,
            0,
            DEFAULT_LINGER,
        ));
        assert_eq!(next.phase, MigrationPhase::Deploying);
        assert_eq!(next.restore_checkpoint, Some(7));
    }

    #[test]
    fn deploying_waits_until_the_target_itself_reports_the_pipeline() {
        let ev = Evidence {
            source: Some(obs("ready", &["p1"], 11)),
            target: Some(obs("ready", &[], 20)),
            latest_checkpoint: Some(7),
        };
        assert_eq!(
            step(&rec(MigrationPhase::Deploying), &ev, 0, DEFAULT_LINGER),
            Decision::Wait,
            "absence of evidence must never be read as evidence of the deploy"
        );

        let ev = Evidence {
            target: Some(obs("ready", &["p1"], 21)),
            ..ev
        };
        assert_eq!(
            advanced(step(
                &rec(MigrationPhase::Deploying),
                &ev,
                0,
                DEFAULT_LINGER
            ))
            .phase,
            MigrationPhase::Restoring
        );
    }

    #[test]
    fn deploying_fails_if_the_target_is_fenced_or_gone() {
        let fenced = Evidence {
            target: Some(obs(STATUS_FENCED, &[], 20)),
            ..Default::default()
        };
        assert_eq!(
            advanced(step(
                &rec(MigrationPhase::Deploying),
                &fenced,
                5,
                DEFAULT_LINGER
            ))
            .phase,
            MigrationPhase::Failed
        );
        let gone = Evidence::default();
        assert_eq!(
            advanced(step(
                &rec(MigrationPhase::Deploying),
                &gone,
                5,
                DEFAULT_LINGER
            ))
            .phase,
            MigrationPhase::Failed
        );
    }

    #[test]
    fn switching_fences_the_source_and_only_then_advances() {
        let ev = Evidence {
            source: Some(obs("ready", &["p1"], 11)),
            target: Some(obs("ready", &["p1"], 21)),
            latest_checkpoint: Some(7),
        };
        // First tick: the source is still live, so the cut-over is performed
        // and the record deliberately does NOT advance.
        assert_eq!(
            step(&rec(MigrationPhase::Switching), &ev, 0, DEFAULT_LINGER),
            Decision::Act(Effect::FenceSource {
                worker: "w-src".into(),
                observed_fence: 11,
            })
        );
        // Second tick: the fence is now observable, so it advances. Crashing
        // between the two ticks changes nothing — the second tick re-derives.
        let ev2 = Evidence {
            source: Some(obs(STATUS_FENCED, &[], 12)),
            ..ev
        };
        assert_eq!(
            advanced(step(
                &rec(MigrationPhase::Switching),
                &ev2,
                0,
                DEFAULT_LINGER
            ))
            .phase,
            MigrationPhase::CleaningUp
        );
    }

    #[test]
    fn switching_is_idempotent_when_the_source_vanished_entirely() {
        let ev = Evidence {
            source: None,
            target: Some(obs("ready", &["p1"], 21)),
            latest_checkpoint: Some(7),
        };
        assert_eq!(
            advanced(step(
                &rec(MigrationPhase::Switching),
                &ev,
                0,
                DEFAULT_LINGER
            ))
            .phase,
            MigrationPhase::CleaningUp
        );
    }

    #[test]
    fn cleanup_completes_once_the_source_stops_reporting_the_pipeline() {
        let still_running = Evidence {
            source: Some(obs(STATUS_FENCED, &["p1"], 12)),
            ..Default::default()
        };
        assert_eq!(
            step(
                &rec(MigrationPhase::CleaningUp),
                &still_running,
                0,
                DEFAULT_LINGER
            ),
            Decision::Wait
        );

        let drained = Evidence {
            source: Some(obs(STATUS_FENCED, &[], 13)),
            ..Default::default()
        };
        let next = advanced(step(
            &rec(MigrationPhase::CleaningUp),
            &drained,
            42,
            DEFAULT_LINGER,
        ));
        assert_eq!(next.phase, MigrationPhase::Completed);
        assert_eq!(next.finished_ms, Some(42));
    }

    // -- properties the safety argument rests on ---------------------------

    #[test]
    fn every_non_terminal_phase_terminates_at_its_deadline() {
        for phase in [
            MigrationPhase::Checkpointing,
            MigrationPhase::Deploying,
            MigrationPhase::Restoring,
            MigrationPhase::Switching,
            MigrationPhase::CleaningUp,
        ] {
            let r = rec(phase);
            let next = advanced(step(
                &r,
                &Evidence::default(),
                r.deadline_ms,
                DEFAULT_LINGER,
            ));
            assert_eq!(
                next.phase,
                MigrationPhase::Failed,
                "phase {phase:?} must not be able to pin a pipeline forever"
            );
            assert!(next.failure.is_some());
        }
    }

    #[test]
    fn step_is_monotone_in_phase_rank_for_every_reachable_input() {
        // The property the whole design leans on: no evidence, however
        // stale or contradictory, can make the machine go backwards.
        let statuses = ["ready", "unhealthy", STATUS_FENCED, "draining"];
        let assigned: [&[&str]; 2] = [&[], &["p1"]];
        let checkpoints = [None, Some(1u64)];
        let restored = [false, true];

        for phase in [
            MigrationPhase::Checkpointing,
            MigrationPhase::Deploying,
            MigrationPhase::Restoring,
            MigrationPhase::Switching,
            MigrationPhase::CleaningUp,
        ] {
            for s in statuses {
                for t in statuses {
                    for a in assigned {
                        for cp in checkpoints {
                            for r in restored {
                                for present in [true, false] {
                                    let mut m = rec(phase);
                                    m.restored = r;
                                    let ev = Evidence {
                                        source: present.then(|| obs(s, a, 11)),
                                        target: present.then(|| obs(t, a, 21)),
                                        latest_checkpoint: cp,
                                    };
                                    if let Decision::Advance(next) =
                                        step(&m, &ev, 1, DEFAULT_LINGER)
                                    {
                                        assert!(
                                            next.phase.rank() >= phase.rank(),
                                            "regression {:?} -> {:?} for evidence {ev:?}",
                                            phase,
                                            next.phase
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn step_is_deterministic_and_idempotent_on_an_unchanged_world() {
        // Re-running a tick against unchanged state must decide the same
        // thing — this is what makes "run it again" always safe.
        let ev = Evidence {
            source: Some(obs("ready", &["p1"], 11)),
            target: Some(obs("ready", &["p1"], 21)),
            latest_checkpoint: Some(3),
        };
        for phase in [
            MigrationPhase::Checkpointing,
            MigrationPhase::Deploying,
            MigrationPhase::Restoring,
            MigrationPhase::Switching,
            MigrationPhase::CleaningUp,
        ] {
            let m = rec(phase);
            let a = step(&m, &ev, 100, DEFAULT_LINGER);
            let b = step(&m, &ev, 100, DEFAULT_LINGER);
            assert_eq!(a, b, "step must be a pure function; phase {phase:?}");
        }
    }

    #[test]
    fn terminal_records_linger_then_retire() {
        let mut m = rec(MigrationPhase::Completed);
        m.finished_ms = Some(1_000);
        let linger = Duration::from_secs(5);
        assert_eq!(
            step(&m, &Evidence::default(), 2_000, linger),
            Decision::Wait
        );
        assert_eq!(
            step(&m, &Evidence::default(), 6_000, linger),
            Decision::Retire
        );
    }

    #[test]
    fn a_terminal_record_never_re_enters_the_machine() {
        // No evidence can move Completed or Failed back into a working phase;
        // this is the ABA guard — a recycled id cannot resurrect a migration.
        for phase in [MigrationPhase::Completed, MigrationPhase::Failed] {
            let mut m = rec(phase);
            m.finished_ms = Some(0);
            let d = step(&m, &Evidence::default(), 1, DEFAULT_LINGER);
            assert!(
                matches!(d, Decision::Wait | Decision::Retire),
                "terminal phase {phase:?} produced {d:?}"
            );
        }
    }

    #[test]
    fn phase_ranks_are_strictly_ordered_through_the_pipeline() {
        let order = [
            MigrationPhase::Checkpointing,
            MigrationPhase::Deploying,
            MigrationPhase::Restoring,
            MigrationPhase::Switching,
            MigrationPhase::CleaningUp,
            MigrationPhase::Completed,
        ];
        for w in order.windows(2) {
            assert!(w[0].rank() < w[1].rank(), "{:?} !< {:?}", w[0], w[1]);
        }
        assert_eq!(
            MigrationPhase::Failed.rank(),
            MigrationPhase::Completed.rank()
        );
    }

    #[test]
    fn record_round_trips_and_tolerates_missing_optional_fields() {
        let json = r#"{
            "id":"m1","pipeline":"p","group":"g","source":"s","target":"t",
            "phase":"deploying","deadline_ms":99
        }"#;
        let m: MigrationRecord = serde_json::from_str(json).unwrap();
        assert_eq!(m.phase, MigrationPhase::Deploying);
        assert!(!m.restored);
        assert_eq!(m.restore_checkpoint, None);
        let back: MigrationRecord =
            serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(back, m);
    }
}
