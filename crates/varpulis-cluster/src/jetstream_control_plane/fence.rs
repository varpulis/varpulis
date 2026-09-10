//! Fencing: making a stale writer fail.
//!
//! ## The hole this closes
//!
//! Under Raft, no coordinator→worker command carries an epoch. A worker that
//! is partitioned, declared dead, has its pipelines migrated elsewhere, and
//! then *returns*, keeps running those pipelines and keeps writing to their
//! sinks. Nothing in the protocol tells it to stop and nothing downstream can
//! tell its output from the replacement's. For a detection engine that means
//! duplicate alerts for as long as the zombie lives.
//!
//! Raft could not fix this cheaply because the log has no per-worker
//! precondition: `WorkerMetricsUpdated { id, .. }` is applied by
//! [`apply_command`](crate::control_state::apply_command) unconditionally —
//! the state machine has no way to say "only if this is still the same
//! incarnation".
//!
//! A KV bucket does, for free. Every key carries a revision and every write
//! can be made conditional on it.
//!
//! ## The scheme
//!
//! The record at `workers/<id>` is a **lease**, owned by exactly one live
//! worker incarnation:
//!
//! | step        | operation                                          | effect |
//! |-------------|----------------------------------------------------|--------|
//! | acquire     | `create(workers/<id>, entry)`                      | exactly one incarnation wins; its revision `r0` is the *acquire fence* |
//! | renew       | `update(workers/<id>, entry, fence)` each heartbeat | advances the fence; a missed beat means the next CAS is stale |
//! | fence out   | `update(workers/<id>, {status: fenced}, observed)`  | advances the revision, so the owner's next renew **fails** |
//! | self-fence  | `last_renewed.elapsed() >= ttl`                     | a worker that cannot reach the broker stops on its own |
//! | hand back   | `delete(workers/<id>)`                             | instant re-registration, and still advances the revision |
//!
//! Three properties follow, and they are what the tests assert:
//!
//! 1. **A returning zombie fails its first write.** It CASes at the revision
//!    it last saw; the fence-out moved the key past it;
//!    [`ControlPlaneError::Cas`] comes back and [`WorkerLease::renew`] latches
//!    [`LeaseError::Fenced`] permanently. The lease never becomes valid again
//!    — a fenced incarnation cannot resurrect itself, it can only be replaced
//!    by a *new* incarnation that re-acquires.
//!
//! 2. **A partitioned zombie fails without hearing anything.** It cannot
//!    learn it was fenced (it cannot reach the broker — that is what
//!    "partitioned" means), so it must fail closed on its own clock:
//!    [`WorkerLease::is_valid`] goes false once a renewal is `ttl` overdue.
//!    This is the half that actually stops the duplicate alerts, because the
//!    emit path consults it locally. Without it, fencing would only work
//!    against a worker that is already reachable, i.e. against the case that
//!    was never the problem.
//!
//! 3. **A command minted for a dead incarnation is refused.** Commands are
//!    stamped with the fence the coordinator observed
//!    ([`FencedCommand`]). A new incarnation always acquires at a strictly
//!    higher revision than any write of the previous one (JetStream sequences
//!    are monotone and a delete appends a tombstone rather than recycling the
//!    sequence), so `cmd.fence < acquired_fence` identifies a command
//!    addressed to the worker that used to have this id.
//!
//! ## Why the revision is a sound fencing token
//!
//! A JetStream KV revision is the stream sequence of the write that produced
//! it. Sequences are assigned by the stream leader, are strictly increasing,
//! and are never reused — a delete appends a tombstone at a *higher*
//! sequence rather than freeing the old one. So for a given key the sequence
//! of accepted writes is a chain `r0 < r1 < r2 < …` in which write *i* is
//! accepted only when the server's current value for the key is exactly
//! `r(i-1)`. That is the definition of a compare-and-swap register, and its
//! revision is therefore a valid fencing token in the Burrows/Chubby sense:
//! monotone, granted by the store, and checkable by the store.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::apply::WorkerRecord;
use super::keys::ControlKey;
use super::store::{ControlPlane, ControlPlaneError, Expect};

/// `WorkerEntry::status` for a worker the control plane has fenced out.
///
/// Distinct from `"unhealthy"`: unhealthy is an observation ("we stopped
/// hearing from it"), fenced is a decision ("its grant is revoked, and its
/// writes will now be refused").
pub const STATUS_FENCED: &str = "fenced";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a lease operation failed.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// This incarnation has been fenced out: the control-plane record moved
    /// on without it. The holder must stop emitting immediately. It is
    /// terminal for this lease object — a new incarnation must re-acquire.
    #[error("fenced out of {key}: another writer advanced past revision {held_fence}")]
    Fenced { key: String, held_fence: u64 },

    /// The lease could not be acquired because a live owner holds it.
    #[error("{key} is already held by a live owner")]
    Held { key: String },

    /// The lease has not been renewed within its TTL, so the holder has
    /// fenced *itself*. This is the partition case: nothing was heard from
    /// the control plane, so authority is presumed revoked.
    #[error("lease on {key} expired: last renewed {elapsed:?} ago, ttl {ttl:?}")]
    Expired {
        key: String,
        elapsed: Duration,
        ttl: Duration,
    },

    /// A command was minted for an earlier incarnation of this worker id.
    #[error(
        "command for {key} carries fence {command_fence}, \
         but this incarnation acquired at {acquired_fence} — refusing a command \
         addressed to a previous incarnation"
    )]
    StaleCommand {
        key: String,
        command_fence: u64,
        acquired_fence: u64,
    },

    #[error(transparent)]
    Transport(#[from] ControlPlaneError),
}

impl LeaseError {
    /// True when the holder has lost authority and must stop emitting.
    ///
    /// Distinguishes "you are no longer allowed to run" from "the broker is
    /// having a bad day", which callers must treat differently: the first is
    /// terminal, the second is retryable.
    pub fn revokes_authority(&self) -> bool {
        matches!(
            self,
            Self::Fenced { .. } | Self::Expired { .. } | Self::StaleCommand { .. }
        )
    }
}

// ---------------------------------------------------------------------------
// Fenced command envelope
// ---------------------------------------------------------------------------

/// A coordinator→worker command stamped with the fence the coordinator
/// observed for the target worker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FencedCommand<T> {
    /// The `workers/<id>` revision the coordinator read before minting this.
    pub fence: u64,
    /// The command itself.
    pub payload: T,
}

impl<T> FencedCommand<T> {
    pub fn new(fence: u64, payload: T) -> Self {
        Self { fence, payload }
    }
}

// ---------------------------------------------------------------------------
// Shared, cheap authorization handle
// ---------------------------------------------------------------------------

/// A clonable handle the *emit path* consults before writing to a sink.
///
/// Deliberately atomics-only: checking authority must not take a lock, hit
/// the network, or allocate, or it will not be checked at the rate a
/// streaming engine emits.
#[derive(Debug, Clone)]
pub struct FenceGuard {
    fenced: Arc<AtomicBool>,
    /// Millis since the guard's epoch at the last successful renewal.
    last_renew_ms: Arc<AtomicU64>,
    epoch: Instant,
    ttl: Duration,
}

impl FenceGuard {
    /// The flag this guard clears when it loses its fence.
    ///
    /// Hand it to [`varpulis_runtime::engine::Engine::use_fence_handle`] so the
    /// engine and the guard share one allocation. Refusing a fenced worker's
    /// *control-plane write* closes nothing a user can see: on its own the
    /// zombie keeps consuming its sources, mutating window and pattern state,
    /// advancing offsets and emitting duplicate alerts. The data plane has to
    /// stop too.
    #[must_use]
    pub fn fence_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.fenced)
    }

    fn new(ttl: Duration) -> Self {
        Self {
            fenced: Arc::new(AtomicBool::new(false)),
            last_renew_ms: Arc::new(AtomicU64::new(0)),
            epoch: Instant::now(),
            ttl,
        }
    }

    fn mark_renewed(&self) {
        let ms = self.epoch.elapsed().as_millis() as u64;
        self.last_renew_ms.store(ms, Ordering::Release);
    }

    fn mark_fenced(&self) {
        self.fenced.store(true, Ordering::Release);
    }

    /// How long since the last successful renewal.
    pub fn since_renewal(&self) -> Duration {
        let then = Duration::from_millis(self.last_renew_ms.load(Ordering::Acquire));
        self.epoch.elapsed().saturating_sub(then)
    }

    /// True while this holder is still authorized to emit.
    ///
    /// False once it has been fenced out **or** once a renewal is overdue by
    /// the lease TTL — the latter without any communication, which is what
    /// makes a partitioned worker stop.
    pub fn is_valid(&self) -> bool {
        !self.fenced.load(Ordering::Acquire) && self.since_renewal() < self.ttl
    }

    /// True once fenced out by another writer (as opposed to merely overdue).
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// Worker lease
// ---------------------------------------------------------------------------

/// A worker incarnation's ownership of `workers/<id>`.
#[derive(Debug)]
pub struct WorkerLease {
    cp: ControlPlane,
    key: ControlKey,
    /// Revision at which this incarnation acquired. Never changes.
    acquired_fence: u64,
    /// Revision of this incarnation's most recent successful write.
    fence: u64,
    guard: FenceGuard,
}

impl WorkerLease {
    /// Acquire the lease for `entry.id`.
    ///
    /// Succeeds when the key is absent (`create`), or when it holds a record
    /// this control plane has already fenced out (`status == "fenced"`) —
    /// re-registering a fenced id is legitimate, it is a *new* incarnation,
    /// and it takes the key at a strictly higher revision so the old one
    /// stays fenced. Any other existing record means a live owner and the
    /// acquisition is refused: two processes must never share a worker id.
    pub async fn acquire(
        cp: &ControlPlane,
        entry: &WorkerRecord,
        ttl: Duration,
    ) -> Result<Self, LeaseError> {
        let key = ControlKey::Worker(entry.entry.id.clone());
        let guard = FenceGuard::new(ttl);

        let fence = match cp.write(&key, entry, Expect::Absent).await {
            Ok(rev) => rev,
            Err(e) if e.is_cas_conflict() => {
                // Occupied. Take over only a record that is already fenced.
                let existing =
                    cp.get::<WorkerRecord>(&key)
                        .await?
                        .ok_or_else(|| LeaseError::Held {
                            key: key.to_string(),
                        })?;
                if existing.value.entry.status != STATUS_FENCED {
                    return Err(LeaseError::Held {
                        key: key.to_string(),
                    });
                }
                cp.write(&key, entry, Expect::Revision(existing.revision))
                    .await
                    .map_err(|e| {
                        if e.is_cas_conflict() {
                            LeaseError::Held {
                                key: key.to_string(),
                            }
                        } else {
                            LeaseError::Transport(e)
                        }
                    })?
            }
            Err(e) => return Err(LeaseError::Transport(e)),
        };

        guard.mark_renewed();
        Ok(Self {
            cp: cp.clone(),
            key,
            acquired_fence: fence,
            fence,
            guard,
        })
    }

    /// Renew the lease by writing `entry` conditional on the held fence.
    ///
    /// This is the heartbeat. On success the fence advances. On a refused
    /// CAS the holder has been fenced out: the guard is latched invalid and
    /// every later call fails the same way.
    pub async fn renew(&mut self, entry: &WorkerRecord) -> Result<u64, LeaseError> {
        if self.guard.is_fenced() {
            return Err(LeaseError::Fenced {
                key: self.key.to_string(),
                held_fence: self.fence,
            });
        }
        match self
            .cp
            .write(&self.key, entry, Expect::Revision(self.fence))
            .await
        {
            Ok(rev) => {
                self.fence = rev;
                self.guard.mark_renewed();
                Ok(rev)
            }
            Err(e) if e.is_cas_conflict() => {
                self.guard.mark_fenced();
                Err(LeaseError::Fenced {
                    key: self.key.to_string(),
                    held_fence: self.fence,
                })
            }
            Err(e) => Err(LeaseError::Transport(e)),
        }
    }

    /// Accept or refuse a coordinator command addressed to this worker.
    ///
    /// Refuses when the lease is no longer valid, or when the command was
    /// minted before this incarnation acquired the id.
    pub fn accept<'c, T>(&self, cmd: &'c FencedCommand<T>) -> Result<&'c T, LeaseError> {
        if self.guard.is_fenced() {
            return Err(LeaseError::Fenced {
                key: self.key.to_string(),
                held_fence: self.fence,
            });
        }
        let elapsed = self.guard.since_renewal();
        if elapsed >= self.guard.ttl {
            return Err(LeaseError::Expired {
                key: self.key.to_string(),
                elapsed,
                ttl: self.guard.ttl,
            });
        }
        if cmd.fence < self.acquired_fence {
            return Err(LeaseError::StaleCommand {
                key: self.key.to_string(),
                command_fence: cmd.fence,
                acquired_fence: self.acquired_fence,
            });
        }
        Ok(&cmd.payload)
    }

    /// Release the lease so a replacement can register immediately, instead
    /// of waiting out the bucket TTL.
    pub async fn release(self) -> Result<(), LeaseError> {
        self.cp.delete_at(&self.key, self.fence).await?;
        Ok(())
    }

    /// The revision this incarnation acquired at — its identity.
    pub fn acquired_fence(&self) -> u64 {
        self.acquired_fence
    }

    /// The revision of the most recent successful write.
    pub fn fence(&self) -> u64 {
        self.fence
    }

    /// A cheap handle for the emit path.
    pub fn guard(&self) -> FenceGuard {
        self.guard.clone()
    }

    /// The key this lease owns.
    pub fn key(&self) -> &ControlKey {
        &self.key
    }
}

// ---------------------------------------------------------------------------
// Coordinator side
// ---------------------------------------------------------------------------

/// Revoke a worker's grant, at a revision the caller has observed.
///
/// Writes `status = "fenced"` conditional on `observed_fence`, which
/// **advances the key's revision** and therefore refuses the owner's next
/// renewal. Returns the new revision — the fence every subsequent command for
/// this id must carry.
///
/// The CAS on `observed_fence` is what makes two coordinators racing to fence
/// the same worker safe: exactly one wins, the loser re-reads and finds the
/// job already done rather than double-fencing a *replacement* that
/// registered in between.
pub async fn fence_out(
    cp: &ControlPlane,
    worker_id: &str,
    observed_fence: u64,
) -> Result<u64, LeaseError> {
    let key = ControlKey::Worker(worker_id.to_string());
    let current = cp
        .get::<WorkerRecord>(&key)
        .await?
        .ok_or_else(|| LeaseError::Fenced {
            key: key.to_string(),
            held_fence: observed_fence,
        })?;
    let mut entry = current.value;
    entry.entry.status = STATUS_FENCED.to_string();
    entry.entry.assigned_pipelines.clear();
    let rev = cp
        .write(&key, &entry, Expect::Revision(observed_fence))
        .await
        .map_err(|e| {
            if e.is_cas_conflict() {
                LeaseError::Fenced {
                    key: key.to_string(),
                    held_fence: observed_fence,
                }
            } else {
                LeaseError::Transport(e)
            }
        })?;
    Ok(rev)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_starts_valid_and_goes_invalid_without_renewal() {
        let g = FenceGuard::new(Duration::from_millis(40));
        g.mark_renewed();
        assert!(g.is_valid(), "a just-renewed guard must be valid");
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            !g.is_valid(),
            "a guard whose renewal is overdue by the ttl must fail closed \
             WITHOUT hearing anything — this is the partitioned-zombie case"
        );
        assert!(
            !g.is_fenced(),
            "expiry is not the same as having been fenced out"
        );
    }

    #[test]
    fn fencing_a_guard_is_terminal() {
        let g = FenceGuard::new(Duration::from_mins(1));
        g.mark_renewed();
        assert!(g.is_valid());
        g.mark_fenced();
        assert!(!g.is_valid());
        // Even a fresh renewal cannot bring a fenced guard back.
        g.mark_renewed();
        assert!(
            !g.is_valid(),
            "a fenced incarnation must not be able to resurrect itself"
        );
    }

    #[test]
    fn guard_clones_share_state() {
        let g = FenceGuard::new(Duration::from_mins(1));
        g.mark_renewed();
        let emit_side = g.clone();
        assert!(emit_side.is_valid());
        g.mark_fenced();
        assert!(
            !emit_side.is_valid(),
            "the emit path's handle must observe the fence immediately"
        );
    }

    #[test]
    fn lease_errors_classify_authority_loss() {
        assert!(LeaseError::Fenced {
            key: "workers/w1".into(),
            held_fence: 3
        }
        .revokes_authority());
        assert!(LeaseError::Expired {
            key: "workers/w1".into(),
            elapsed: Duration::from_secs(9),
            ttl: Duration::from_secs(5)
        }
        .revokes_authority());
        assert!(LeaseError::StaleCommand {
            key: "workers/w1".into(),
            command_fence: 2,
            acquired_fence: 9
        }
        .revokes_authority());
        assert!(
            !LeaseError::Held {
                key: "workers/w1".into()
            }
            .revokes_authority(),
            "failing to acquire is not the same as losing authority you had"
        );
    }

    #[test]
    fn fenced_command_round_trips() {
        let cmd = FencedCommand::new(42u64, serde_json::json!({"deploy": "p1"}));
        let wire = serde_json::to_string(&cmd).unwrap();
        let back: FencedCommand<serde_json::Value> = serde_json::from_str(&wire).unwrap();
        assert_eq!(back, cmd);
        assert!(wire.contains("\"fence\":42"));
    }
}
