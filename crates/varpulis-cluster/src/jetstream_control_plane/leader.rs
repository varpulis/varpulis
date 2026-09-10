//! Coordinator leadership as a KV lease.
//!
//! This is Vejas's `core/src/cluster.rs` pattern applied to the coordinator
//! rather than to a singleton source, and it is what replaces Raft's election
//! *and* the Kubernetes Lease election in `ha.rs`:
//!
//! * **acquire**  `create(control/leader, id)` — create-if-absent, so exactly
//!   one coordinator wins the race.
//! * **renew**    `update(control/leader, id, revision)` — compare-and-swap.
//!   A coordinator that stalled (GC pause, blocked on I/O, partitioned) wakes
//!   holding a stale revision, its CAS is refused, and it stands down. That is
//!   fencing for free: two coordinators never *keep* driving the cluster.
//! * **release**  `delete(control/leader)` on graceful shutdown, so a standby
//!   takes over immediately instead of waiting out the TTL.
//! * **failover** the bucket's `max_age` ages the value out after a crash, so
//!   a standby's `create` then succeeds. Bounded by the TTL.
//!
//! ## What is and is not guaranteed
//!
//! This gives *at most one coordinator that believes itself leader and can
//! still write*, which is the property that matters, because every write it
//! makes is itself a CAS. It does **not** give "at most one coordinator that
//! believes itself leader" in absolute terms: a partitioned old leader keeps
//! believing until its next renewal fails. Raft has the identical window —
//! a partitioned Raft leader also keeps serving until its election timeout —
//! and the identical mitigation: its writes fail. So a caller must treat
//! [`LeaderLease::is_leader`] as advisory for *scheduling* work and rely on
//! the CAS on each write for *safety*. That distinction is load-bearing; it is
//! why the reconciler in [`super::reconcile`] guards every mutation on a
//! revision instead of on `is_leader()`.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::keys::ControlKey;
use super::store::{ControlPlane, Expect};

/// The value stored at `control/leader`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderRecord {
    /// Identity of the holding coordinator.
    pub id: String,
    /// Monotone count of renewals, for observability only.
    #[serde(default)]
    pub term: u64,
}

/// Outcome of a leadership attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderState {
    /// This coordinator holds the lease and may drive.
    Leader,
    /// Another coordinator holds it.
    Follower,
}

/// A coordinator's (attempted) hold on `control/leader`.
#[derive(Debug)]
pub struct LeaderLease {
    cp: ControlPlane,
    id: String,
    ttl: Duration,
    /// `Some(revision)` while this coordinator holds the lease.
    held: Option<u64>,
    last_renewed: Option<Instant>,
    term: u64,
}

impl LeaderLease {
    /// Create a (not yet acquired) lease handle.
    pub fn new(cp: ControlPlane, id: impl Into<String>, ttl: Duration) -> Self {
        Self {
            cp,
            id: id.into(),
            ttl,
            held: None,
            last_renewed: None,
            term: 0,
        }
    }

    /// This coordinator's identity.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Advisory: does this coordinator currently believe it leads?
    ///
    /// Never use this to authorise a write — use the CAS on the write itself.
    /// It goes false once a renewal is overdue by the TTL, so a partitioned
    /// coordinator stops *scheduling* new work on its own clock.
    pub fn is_leader(&self) -> bool {
        match (self.held, self.last_renewed) {
            (Some(_), Some(t)) => t.elapsed() < self.ttl,
            _ => false,
        }
    }

    /// The revision at which leadership is held, if held.
    pub fn revision(&self) -> Option<u64> {
        self.held
    }

    /// Try to take or keep leadership. Idempotent; call it every tick.
    ///
    /// * not holding → `create`; success means acquired, refusal means another
    ///   coordinator is live.
    /// * holding → `update` at the held revision; refusal means fenced, and
    ///   the lease stands down rather than continuing to believe.
    pub async fn tick(&mut self) -> LeaderState {
        match self.held {
            None => {
                let record = LeaderRecord {
                    id: self.id.clone(),
                    term: self.term + 1,
                };
                match self
                    .cp
                    .write(&ControlKey::Leader, &record, Expect::Absent)
                    .await
                {
                    Ok(rev) => {
                        self.held = Some(rev);
                        self.last_renewed = Some(Instant::now());
                        self.term += 1;
                        tracing::info!(
                            coordinator = %self.id,
                            revision = rev,
                            "acquired coordinator lease"
                        );
                        LeaderState::Leader
                    }
                    Err(_) => LeaderState::Follower,
                }
            }
            Some(rev) => {
                let record = LeaderRecord {
                    id: self.id.clone(),
                    term: self.term,
                };
                match self
                    .cp
                    .write(&ControlKey::Leader, &record, Expect::Revision(rev))
                    .await
                {
                    Ok(new_rev) => {
                        self.held = Some(new_rev);
                        self.last_renewed = Some(Instant::now());
                        LeaderState::Leader
                    }
                    Err(e) => {
                        // A refused CAS means someone else took it; a
                        // transport error means we cannot prove we still hold
                        // it. Both must stand us down — failing closed is the
                        // whole point of a lease.
                        tracing::warn!(
                            coordinator = %self.id,
                            error = %e,
                            "coordinator lease lost — standing down"
                        );
                        self.held = None;
                        self.last_renewed = None;
                        LeaderState::Follower
                    }
                }
            }
        }
    }

    /// Who holds the lease right now, as far as the bucket is concerned.
    pub async fn current_holder(&self) -> Option<String> {
        self.cp
            .get::<LeaderRecord>(&ControlKey::Leader)
            .await
            .ok()
            .flatten()
            .map(|v| v.value.id)
    }

    /// Give up leadership immediately (graceful shutdown), so a standby takes
    /// over without waiting out the TTL.
    pub async fn resign(&mut self) {
        if let Some(rev) = self.held.take() {
            let _ = self.cp.delete_at(&ControlKey::Leader, rev).await;
            tracing::info!(coordinator = %self.id, "resigned coordinator lease");
        }
        self.last_renewed = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_record_round_trips() {
        let r = LeaderRecord {
            id: "coord-1".into(),
            term: 3,
        };
        let back: LeaderRecord = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn term_defaults_for_records_written_by_an_older_build() {
        let r: LeaderRecord = serde_json::from_str(r#"{"id":"coord-1"}"#).unwrap();
        assert_eq!(r.term, 0);
    }
}
