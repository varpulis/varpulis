# ADR-007: JetStream as the Durable Substrate for Cluster Event Paths

**Status:** Accepted
**Date:** 2026-09-10
**Authors:** Varpulis Team

## Context

Varpulis used NATS purely as an RPC transport: `client.subscribe`,
`client.publish`, `client.request`. Core NATS is fire-and-forget. A publish
succeeds as soon as the bytes reach the socket; if no subscription matches at
that instant the message is gone, and nothing records that it happened.

That is the right trade for a request/reply RPC — the reply *is* the
acknowledgement — and the wrong trade for an event. Two paths carried events
over it:

1. **Inter-pipeline routing.** `crates/varpulis-cluster/src/routing.rs` maps a
   pipeline's outputs to `varpulis.cluster.pipeline.{group}.{from}.{to}` and its
   inputs to subscriptions on the same subjects. An event routed to a pipeline
   that is restarting, migrating, or simply down was dropped silently. For a
   detection engine that is a missed alert.
2. **Worker heartbeats.** `varpulis.cluster.heartbeat.{worker}` was a plain
   publish consumed by a plain wildcard subscription. A heartbeat published
   while the coordinator was restarting was lost, and it feeds a liveness
   decision — the same class of failure as audit finding C5.

`async-nats` 0.46 was already a dependency of `varpulis-cluster`,
`varpulis-connector-nats` and `varpulis-runtime`, with the `jetstream` feature
in its default set. Nothing in the tree used it (`grep -rn jetstream crates/`
returned zero hits).

## Decision

Move both event paths onto JetStream, behind a **selectable substrate that
defaults to the previous behaviour**.

### Substrate selection

`Substrate::{Core, JetStream}`, default `Core`. Selected with
`VARPULIS_NATS_SUBSTRATE=core|jetstream`, or explicitly through
`ClusterTransport::connect`. Subjects and payloads are identical on both, so a
JetStream stream bound to `varpulis.cluster.pipeline.>` captures what a
still-core publisher emits: a mixed fleet works during a rollout.

When JetStream is *requested* and the server does not have it, connect **fails
loudly**. It never falls back to core. A silent downgrade would hand an operator
who asked for durability exactly the fire-and-forget loss they were removing.

### Stream layout

| Stream | Subjects | Holds |
|---|---|---|
| `VARPULIS_ROUTING` | `varpulis.cluster.pipeline.>` | inter-pipeline routed events |
| `VARPULIS_HEARTBEAT` | `varpulis.cluster.heartbeat.>` | worker liveness |
| `VARPULIS_DLQ` | `varpulis.dlq.>` | poison events, parked |

One stream per *concern*, each bound to a wildcard — not a stream per route
edge. The routing subject already carries `{group}.{from}.{to}`, so a durable
consumer's `filter_subject` isolates delivery per edge while the whole topology
shares one set of limits an operator can actually size. A stream per edge would
multiply admin objects by the number of edges in the topology and give each its
own quota that nobody would tune.

The DLQ sits on a **sibling root**, not under `varpulis.cluster.>`. JetStream
forbids overlapping stream subjects, and the evidence of a poison event must not
be evictable by the hot path's `max_age` nor consumable by a routing consumer's
wildcard.

### Retention and limits

`VARPULIS_ROUTING`: `RetentionPolicy::Limits`, `max_age` 1 h, `max_bytes` 1 GiB,
`DiscardPolicy::Old`, `StorageType::File`, `duplicate_window` 2 min.

- **Limits, not WorkQueue or Interest.** WorkQueue deletes on first ack and
  forbids two consumers on one subject, which kills replay and fan-out — both
  the point. Interest deletes once every *currently known* consumer has acked,
  so an event published before a brand-new route edge created its consumer is
  dropped: precisely the silent loss being fixed. Limits keeps the message for
  the window regardless of who is or is not listening.
- **1 hour.** The operator's real case is "a detection pipeline was down for an
  hour". Longer than that is an operator problem, not a buffering problem, and
  replaying a multi-day backlog into an engine with time windows manufactures
  nonsense alerts rather than recovering real ones.
- **Discard oldest, not reject new.** `DiscardPolicy::New` fails the publish at
  the limit, turning a stalled downstream pipeline into an outage of the
  upstream one that detects. For a detection engine the freshest events matter
  most, and an evicting stream is visible in `nats stream info` rather than
  silent.

`VARPULIS_HEARTBEAT`: `max_age` 5 min, `max_msgs_per_subject` 16, discard
oldest, file storage. A heartbeat is only interesting until the next one
arrives; the per-subject cap stops one worker crowding out another; 5 minutes is
comfortably above any sane `heartbeat_timeout`.

`VARPULIS_DLQ`: `max_msgs` 100 000, discard oldest, **no** `max_age`. A full DLQ
is itself an operator signal; a poison event from last week is exactly the thing
you still want to find.

### Acks and redelivery

`AckPolicy::Explicit`, `ack_wait` 30 s, `max_deliver` 5, `max_ack_pending` 1024,
`inactive_threshold` 7 days (longer than `max_age`, so a consumer is never
reaped while it still has retrievable work).

**Ack strictly after apply.** `Delivery::ack` takes `self` by value; the routing
ingress calls `tenant.process_event(...)` and only then acks. A crash between
apply and ack redelivers. Acking on receipt is the silent-loss pattern the audit
found in the Redis Streams connector (`XACK` before apply), and it is what the
house reference (Vejas ADR-0002) publishes emits *before* acking to avoid.

At the 5th delivery the event is parked in `VARPULIS_DLQ` as a death envelope
(original subject, attempts, last error, verbatim payload, timestamp) and the
original is acked **only after** the DLQ publish is confirmed; a failed park
naks instead, so a poison event never loops forever and never disappears
without a record. A payload that fails to parse is deterministic poison and is
parked on first delivery rather than burning the whole budget re-failing
identically.

### What stays on core NATS

- `varpulis.cluster.register` — request/reply; the reply is the ack and the
  worker already retries with backoff.
- `varpulis.cluster.cmd.{worker}.>` — deploy / inject / drain. Same argument,
  plus: replaying a `deploy` or a `drain` an hour late against a worker that has
  since been rebalanced is actively harmful. These are commands, not events.
- `varpulis.cluster.raft.>` — Raft has its own log, terms and retries. A second
  replicated log under a consensus log is a correctness hazard (a redelivered
  `AppendEntries` from a past term).
- `varpulis.cluster.checkpoint.>` — a two-phase barrier handshake with its own
  timeout and abort path. A redelivered `checkpoint_complete` for a checkpoint
  that was already aborted would corrupt the protocol state machine.

## Alternatives Considered

- **Interest or WorkQueue retention on the routing stream.** Rejected above:
  both reintroduce the loss for a consumer that does not yet exist.
- **A stream per route edge.** Rejected: admin-object sprawl and per-edge quotas
  nobody would size.
- **Making JetStream the default.** Rejected: JetStream needs a
  JetStream-enabled server, and an upgrade must not break a deployment running a
  plain `nats-server`.
- **Falling back to core when JetStream is unavailable.** Rejected: it converts
  a loud misconfiguration into silent data loss.
- **Kafka for the internal bus.** Rejected: a second broker to deploy, secure
  and back up, for a transport NATS already carries.

## Consequences

### Positive
- A routed event survives a restarting, migrating or crashed consumer, and can
  be replayed.
- A heartbeat published during a coordinator restart still reaches the liveness
  decision.
- A failing apply is retried by the server rather than dropped, and a poison
  event is traceable in the DLQ instead of vanishing or looping.
- No new dependency: `async-nats` ships `jetstream` in its default features.

### Negative
- Delivery is **at-least-once**, not exactly-once. Downstream side effects
  reached from a routed event must be idempotent or deduplicated.
- JetStream becomes load-bearing when enabled: its storage, limits and consumer
  semantics are now the operator's to understand.
- Durability cuts both ways for heartbeats. A replayed heartbeat older than
  `heartbeat_timeout` is acked and **discarded** rather than applied, because
  `Coordinator::heartbeat` stamps `last_heartbeat = now` and a stale replay
  would mark a dead worker alive.
- The worker and coordinator processes need permission to create streams and
  consumers on first connect.

## Operational notes

- Enable with `nats-server -js` (or `jetstream {}` in the server config) and
  `VARPULIS_NATS_SUBSTRATE=jetstream` on both coordinator and workers.
- `VARPULIS_COORDINATOR_ID` names the coordinator's durable heartbeat consumer.
  It must be **stable across restarts** (that is what makes the consumer resume)
  and **distinct per coordinator** — a shared durable would load-balance
  heartbeats between coordinators and hide half of each worker's liveness from
  each of them.
- Running with `VARPULIS_NATS_SUBSTRATE=jetstream` against a server without
  JetStream refuses to start the affected loop and logs how to fix it. Leaving
  the variable unset keeps the previous core pub/sub behaviour exactly.
- Size the deployment for `VARPULIS_ROUTING`'s 1 GiB ceiling plus the DLQ's
  100 000 messages. Both are `get_or_create`, never updated, so limits an
  operator tunes by hand survive a Varpulis upgrade.

## References
- `crates/varpulis-cluster/src/nats_jetstream.rs` — substrate, streams, acks, DLQ
- `crates/varpulis-cluster/src/nats_worker.rs` — route ingress (ack after apply)
- `crates/varpulis-cluster/src/nats_coordinator.rs` — heartbeat consumer
- `crates/varpulis-cluster/tests/nats_jetstream_routing.rs` — broker-backed gates
- Vejas ADR-0002, "NATS JetStream as the only infrastructure dependency"
