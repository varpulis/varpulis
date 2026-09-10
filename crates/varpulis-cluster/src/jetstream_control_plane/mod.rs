//! JetStream KV control plane — a compare-and-swap backend for the coordinator
//! state that Raft replicates today.
//!
//! This is step 3a of collapsing three overlapping "who leads and where does
//! shared state live" answers (`raft`, `k8s` lease HA, `nats-transport`) onto
//! one. It **replaces nothing yet**: Raft is untouched and still the default.
//! This backend exists to be run side by side and measured first.
//!
//! ```text
//! workers/<id>          RegisterWorker, DeregisterWorker, WorkerStatusChanged,
//!                       WorkerPipelinesUpdated, WorkerMetricsUpdated
//! groups/<name>         GroupDeployed, GroupUpdated, GroupRemoved
//! connectors/<name>     ConnectorCreated, ConnectorUpdated, ConnectorRemoved
//! migrations/<id>       MigrationStarted, MigrationUpdated, MigrationRemoved
//! checkpoints/<group>   CheckpointCompleted, CheckpointAborted
//! models/<name>         ModelRegistered, ModelRemoved
//! scaling/policy        ScalingPolicySet
//! control/leader        coordinator lease
//! ```
//!
//! # 1. The design problem
//!
//! Raft gives a **totally ordered log**: every coordinator applies every
//! command in the same sequence, so "the state" is a single value that all
//! replicas agree on at every index. JetStream KV gives something weaker and
//! cheaper: each *key* is an independent linearizable register carrying a
//! monotone revision, with compare-and-swap. There is no order **between**
//! keys and no multi-key transaction.
//!
//! The six-phase migration state machine
//! (`crate::migration`, driven from `crate::coordinator::rebalance`) leans on
//! the global order today. Under KV each entity converges independently, so it
//! has to become a reconciliation loop — the shape Kubernetes uses, and the
//! shape Vejas uses on these same primitives (`/home/cpo/vejas/core/src/cluster.rs`,
//! ADR-0020).
//!
//! # 2. What the total order was actually buying
//!
//! Four things are commonly attributed to it. Only one of them was real.
//!
//! **(a) Per-entity linearizability** — "the latest state of worker W".
//! KV gives this natively per key: a CAS'd key *is* a linearizable register.
//! **Nothing is lost.**
//!
//! **(b) Cross-entity atomicity** — "the migration says `CleaningUp` *and* the
//! source's `assigned_pipelines` no longer lists P, as one indivisible fact".
//! Raft would give this only if both were written in one command. They never
//! are: all of the [`ClusterCommand`](crate::control_state::ClusterCommand)
//! variants mutate exactly one entity keyed by one id, and the only one
//! touching two collections writes two facets of the same worker. So today's
//! coordinator *already* writes the worker and the migration as separate log
//! entries, and an observer between them already sees a torn state.
//! **Nothing is lost, because it was never provided.**
//!
//! **(c) A single serialized writer** — only the Raft leader appends, so two
//! coordinators never race. **This is the real loss**, and it is restored in
//! two layers: a coordinator lease ([`leader`]) so only one coordinator
//! *drives*, and — because a lease alone is not safe — a compare-and-swap on
//! *every individual write* ([`store::ControlPlane::write`]) so a
//! second driver cannot corrupt anything even inside the lease's uncertainty
//! window.
//!
//! **(d) Exactly-once execution of a phase's side effects** — no. The log
//! records the *decision*; the side effect (an HTTP POST to a worker) is
//! outside it and is already at-least-once. **Nothing is lost.**
//!
//! # 3. The design
//!
//! ## 3.1 Every write is a guarded read-modify-write
//!
//! There is no unconditional write in the control-plane API. A mutation is:
//!
//! ```text
//! read key           -> (value, revision r)
//! v' = f(value, evidence)
//! write key, v', expect revision == r
//! ```
//!
//! and a refused write is *not* retried blindly — it is discarded, the key is
//! re-read, and `f` is recomputed. See
//! [`apply::Applier`] (for commands) and [`reconcile::Reconciler::commit`]
//! (for the migration machine).
//!
//! ## 3.2 The migration machine becomes level-triggered
//!
//! The record at `migrations/<id>` is made **self-contained**: id, pipeline,
//! group, source, target, phase, restore point, deadline. Every phase advance
//! is then a function of that one record plus *evidence* read from other keys,
//! and is committed as a CAS on that one key.
//!
//! Crucially, the loop is **level-triggered, not edge-triggered**: each tick
//! re-reads the whole world and re-derives every decision from scratch
//! ([`reconcile::Reconciler::tick`]). It never consumes a change event, so
//! there is no event it can miss, and no in-memory position it must not lose.
//!
//! ## 3.3 The cut-over is one compare-and-swap
//!
//! `Switching` — the instant where source and target could both own the
//! pipeline — is not "ask the source to stop". A partitioned source never
//! receives that. It is a CAS that bumps `workers/<source>`, after which the
//! source's next heartbeat CAS fails and it self-fences; and if it is
//! partitioned and cannot even attempt that CAS, its lease expires on its own
//! clock. See [`fence`].
//!
//! # 4. Why this is safe without a total order
//!
//! Let each key `k` be a register with monotone revision `rev(k)`, and let a
//! *guarded write* `update(k, v, r)` succeed only if `rev(k) == r` server-side.
//!
//! **Claim 1 — per-key serialization.** The successful guarded writes to `k`
//! form a chain `r₀ < r₁ < r₂ < …` in which write *i* observed exactly the
//! value written by *i-1*. *Proof:* the server accepts `update(k, ·, r)` only
//! when `k`'s current sequence is `r`, and every accept advances it to a fresh
//! sequence. ∎ This is the property Raft gave for the whole log; KV gives it
//! per key. Because **every command mutates exactly one key**, per-key is
//! sufficient for apply correctness — that is the observation the whole design
//! rests on, and it is checked by `apply.rs`'s exhaustive mapping.
//!
//! **Claim 2 — no lost updates.** Two coordinators that read revision `r` and
//! both compute a step: at most one CAS commits; the loser discards its
//! computation and recomputes against the winner's value. So read-modify-write
//! cycles never interleave destructively, and a "last writer wins" overwrite of
//! a concurrent decision cannot occur. ∎
//!
//! **Claim 3 — stale evidence is harmless.** A phase guard reads other keys,
//! which may be stale. For each transition, staleness has a bounded, benign
//! consequence — this is a property of the transition function, enumerated:
//!
//! | transition | stale evidence can be… | consequence |
//! |---|---|---|
//! | `Checkpointing → Deploying` | an older durable checkpoint id | restore from an older-but-durable checkpoint. Raft's `latest_completed` was already a monotone *floor*, not an exact value, and replaying from a floor is what the sinks' 2PC dedupe exists for. |
//! | `Deploying → Restoring` | target has not yet reported the pipeline | **wait**. Absence of evidence is never read as evidence; the machine only advances on a positive observation, so it can be late but not wrong. |
//! | `Restoring → Switching` | executor ack not yet visible | wait. |
//! | `Switching → CleaningUp` | source looks alive when it is dead, or dead when alive | the fence-out CAS is issued against the revision *just observed*; if it is stale the CAS is refused and the tick re-derives. A fence that did commit is idempotent to re-observe. |
//! | `CleaningUp → Completed` | source still lists the pipeline | wait; and the source is already fenced by this point, so waiting is free of risk. |
//!
//! **Claim 4 — liveness without edges.** Every non-terminal phase carries a
//! deadline, and the loop is level-triggered. A transition missed because of a
//! lost CAS, a dropped watch, a coordinator crash, or a ten-minute pause is
//! recomputed on the next tick from durable state. The only progress
//! requirement is that *some* live coordinator eventually ticks, which the
//! bucket's `max_age` on `control/leader` guarantees.
//!
//! **Claim 5 — divergent interleavings do not matter.** Under Raft all
//! coordinators saw identical intermediate states; under KV they may observe
//! different interleavings. This is safe because **no decision is a function
//! of an interleaving**: every guard is a predicate over current values, and
//! phases are monotone in [`reconcile::MigrationPhase::rank`] — verified
//! exhaustively over the reachable evidence space by
//! `step_is_monotone_in_phase_rank_for_every_reachable_input`. A terminal
//! migration is retired rather than reused, and a retried migration is minted
//! under a **new** id, so there is no ABA on a phase.
//!
//! ## 4.1 What is genuinely weaker than Raft
//!
//! Stated plainly rather than buried:
//!
//! * **No cross-key atomic snapshot.** [`store::Snapshot`] is a set of
//!   per-key reads, not a consistent cut. Sound here only because Claim 3
//!   holds for every guard; a *future* guard that needs two keys to agree
//!   instantaneously would not be expressible and must not be added without
//!   revisiting this.
//! * **Durability is delegated.** Raft's quorum is replaced by JetStream's
//!   (`num_replicas`), set with `VARPULIS_CONTROL_PLANE_REPLICAS` and
//!   defaulting to 1. A single-replica bucket on a single-node broker is a
//!   single point of failure in a way a three-node Raft group is not, so any
//!   deployment replacing Raft with this must set it to 3 against a NATS
//!   cluster of at least that size.
//! * **Leadership is a lease, not an election.** See [`leader`] for the
//!   uncertainty window and why every write is CAS-guarded regardless.
//!
//! # 5. Selecting this backend
//!
//! Off unless `VARPULIS_CONTROL_PLANE_URL` is set
//! ([`store::ControlPlaneConfig::from_env`]), so building with the
//! `jetstream-control-plane` feature changes no deployment's behaviour on its
//! own.

pub mod apply;
pub mod fence;
pub mod keys;
pub mod leader;
pub mod reconcile;
pub mod store;

pub use apply::{materialize, Applier, ConnectorSecretPolicy, WorkerRecord};
pub use fence::{fence_out, FenceGuard, FencedCommand, LeaseError, WorkerLease, STATUS_FENCED};
pub use keys::ControlKey;
pub use leader::{LeaderLease, LeaderRecord, LeaderState};
pub use reconcile::{
    step, Decision, Effect, Evidence, MigrationPhase, MigrationRecord, Reconciler, TickReport,
    WorkerObservation,
};
pub use store::{
    ControlPlane, ControlPlaneConfig, ControlPlaneError, Expect, Snapshot, Versioned,
    DEFAULT_BUCKET, ENV_BUCKET, ENV_REPLICAS, ENV_TTL_SECS, ENV_URL,
};
