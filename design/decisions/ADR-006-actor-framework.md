# ADR-006: Actor Framework

**Status:** Accepted
**Date:** 2026-02-28
**Authors:** Varpulis Team

## Context

Varpulis is evolving from a monolithic async runtime toward a system of cooperating, supervised components: connectors, pipeline workers, health monitors, and cluster coordination tasks. Each of these components has an independent lifecycle, needs restart guarantees on failure, and must expose observable state for health checking.

Before this decision, each component managed its own Tokio task lifecycle, shutdown signaling (via `CancellationToken`), and error propagation. This led to duplicated patterns across crates:

- Manual `tokio::spawn` + `JoinHandle` tracking in `varpulis-cli` and `varpulis-cluster`
- Ad-hoc restart loops with inconsistent backoff strategies
- No uniform way to observe a component's internal state without stopping it
- Shutdown sequencing that was correct but fragile (multiple `CancellationToken` trees, manually ordered)

The coordinator/worker architecture (ADR-003) and the planned Axum migration (ADR-007) both increase the number of concurrent components in a single process, making a structured approach to actor lifecycle management necessary.

## Decision

Varpulis uses a custom lightweight actor framework, implemented in the `varpulis-actors` crate (`crates/varpulis-actors/`).

### Core abstractions

**Actor trait** (`actor.rs`): The fundamental unit of computation. An actor has:
- A `name()` for logging and diagnostics
- An `observable_state()` that returns a serializable snapshot of the actor's current state, queryable without interrupting the actor's run loop
- A `run()` method that owns the actor and receives an `ActorContext` providing mailbox access and shutdown signaling
- An `ActorExitStatus` return value (`Success`, `Quit`, `Failure(String)`, `Panicked`) that the supervisor uses to decide whether to restart

**Handler trait** (`actor.rs`): An optional request/reply interface for message-driven actors, complementing the `run`-loop model for actors that primarily respond to individual messages.

**Mailbox** (`mailbox.rs`): A bounded `tokio::sync::mpsc` channel carrying typed `Envelope` values. Envelopes can be one-way messages, request/reply pairs (using `oneshot` for the reply), or internal `Observe` requests that return the actor's observable state. Bounded capacity provides backpressure: senders block when the mailbox is full.

**ActorContext** (`context.rs`): The execution environment provided to each actor, containing the mailbox receiver, a `CancellationToken` for shutdown, and a self-sender for scheduling internal messages.

**ActorHandle** (`handle.rs`): A cheaply cloneable reference to a running actor, providing `observe()`, `check_health()`, `send()`, and `ask()` methods. Handles decouple the caller from the actor's internal state.

**Supervisor** (`supervisor.rs`): Wraps an actor factory and a `SupervisorConfig` (restart policy, max restarts, restart window, exponential backoff delay). The supervisor runs a loop that spawns the actor, monitors its exit, and restarts it according to the configured `RestartPolicy` (`Always`, `OnFailure`, `Never`). Restarts within a time window are counted; exceeding `max_restarts` causes the supervisor to give up.

**Runtime** (`runtime.rs`): Manages a set of spawned actors, installs signal handlers (SIGINT/SIGTERM), and coordinates graceful shutdown with a configurable grace period.

**Builder traits** (`builders.rs`): `MessageSink` and `MessageSource` enable type-safe wiring between actors at construction time. `Builder<T>` provides a standard construction pattern.

### Design principles

1. **Thin over thick**: The framework is ~400 lines of library code, not a runtime that takes over `main()`. Actors run on the standard `tokio` runtime. There is no custom scheduler, no actor registry, and no location transparency.

2. **Compile-time wiring, not runtime discovery**: Actor connections are established by passing `MailboxSender` values (which implement `MessageSink`) during construction. Type errors in wiring are caught by the compiler. There is no string-based address lookup or dynamic routing.

3. **Observation without interruption**: The `Observe` envelope type lets external code query an actor's state by sending a request through the mailbox. The actor processes it in order with other messages, ensuring the snapshot is consistent with the actor's processing state.

4. **Supervision is opt-in**: Not every actor needs restart guarantees. The `Supervisor` wraps actors that need them; other actors are spawned directly via `Runtime::spawn()`.

## Alternatives Considered

### actix (Actix actor framework)

Actix is the most mature Rust actor framework, battle-tested in production through actix-web.

Rejected because:
- Actix uses its own `System` runtime, which conflicts with Varpulis's existing Tokio-based architecture. Running both runtimes in one process adds complexity and potential for subtle threading issues.
- Actix's `Addr<A>` handles use a global registry with `TypeId`-based lookup, which is opaque at compile time. Varpulis prefers explicit wiring where type errors surface during compilation.
- Actix's supervision model is tied to its `System` lifecycle, making it difficult to integrate with Varpulis's existing `CancellationToken`-based shutdown sequencing.
- The framework is significantly larger than what Varpulis needs. Actix includes features like arbiters (thread-per-actor), message serialization for remote actors, and streaming support that add compile-time and cognitive overhead without matching Varpulis's requirements.

### xtra

xtra is a lightweight actor library designed for Tokio, with an API similar to Actix but without the separate runtime.

Rejected because:
- xtra requires all messages to implement the `xtra::Message` trait with an associated `Result` type, even for fire-and-forget messages. Varpulis's blanket `impl<T: Send + 'static> Message for T` is simpler for the common case.
- xtra does not provide a supervision tree. Restart logic would still need to be written by hand, removing the primary benefit of adopting a framework.
- xtra's `Address<A>` handle does not expose health state. Varpulis's `ActorHandle` combines messaging with `check_health()` and `observe()`, which are critical for the coordinator's worker monitoring.

### stakker

stakker is a single-threaded actor framework that uses a run-to-completion model (no async, no `await`).

Rejected because:
- stakker is fundamentally single-threaded and synchronous. Varpulis actors perform async I/O (HTTP requests, MQTT connections, file checkpointing) that requires `async fn` and `await`.
- Adapting stakker to work within a Tokio runtime would require bridging between the two concurrency models, negating stakker's simplicity advantage.

### No framework (continue with ad-hoc Tokio tasks)

The status quo: each component manages its own spawn, restart, and shutdown logic.

Rejected because the duplication was already causing maintenance issues. Three different restart implementations existed with subtly different backoff behavior. The actor trait and supervisor provide a single correct implementation that all components can use, reducing the surface area for bugs.

## Consequences

### Positive

- All components that need supervision (connectors, pipeline workers, health monitors) use the same `Supervisor` with consistent restart behavior: exponential backoff, window-based restart counting, and structured logging of exit reasons.
- `ActorHandle::observe()` provides a uniform health-checking mechanism. The coordinator can query any actor's state without protocol-specific code, enabling a generic health dashboard.
- Compile-time wiring via `MessageSink` ensures that connecting a producer to a consumer is type-checked. Mismatched message types are compile errors, not runtime panics.
- The framework is small enough (~400 lines) to be fully understood by any team member. There is no "framework magic" -- actors are Tokio tasks with a structured lifecycle contract.
- The `fan_in_message_type!` macro handles the common pattern of an actor receiving messages from multiple upstream sources with different types, generating the enum and `From` impls automatically.

### Negative

- Compile-time wiring means that actor topologies are static. Dynamically adding a new consumer to a running producer requires stopping and reconstructing the pipeline. This is acceptable for Varpulis's current deployment model (pipelines are compiled and deployed as a unit) but would limit a future interactive/REPL-style workflow.
- The `Envelope` type uses `Box<dyn Any>` for message transport, which erases the message type at the mailbox level. Type safety is enforced at the `MailboxSender`/`MessageSink` boundary (callers send typed messages) but the mailbox itself is untyped. A message type mismatch in `ask()` returns `MailboxError::TypeMismatch` at runtime rather than at compile time.
- The supervisor manages a single actor. Supervising a group of related actors (e.g., "restart all three connectors if any one fails") requires building a parent actor that supervises the group -- a pattern that works but is not provided as a built-in abstraction.
- The framework does not provide location transparency or remote messaging. Actors in different processes communicate via HTTP/MQTT (as defined by ADR-003), not via the actor framework. This is intentional but means the framework is not a distributed actor system.

## References

- [thin-edge.io actor framework](https://github.com/thin-edge/thin-edge.io) -- design influence for the thin, Tokio-native approach
- [Quickwit actor framework](https://github.com/quickwit-oss/quickwit/tree/main/quickwit/quickwit-actors) -- design influence for supervision and observable state
- `crates/varpulis-actors/src/actor.rs` -- Actor and Handler traits
- `crates/varpulis-actors/src/supervisor.rs` -- Supervisor with restart policies
- `crates/varpulis-actors/src/mailbox.rs` -- Bounded mailbox with backpressure
- `crates/varpulis-actors/src/runtime.rs` -- Runtime with graceful shutdown
- `crates/varpulis-actors/src/builders.rs` -- MessageSink/MessageSource wiring traits
- [ADR-003](../../docs/adr/003-coordinator-worker.md) -- Coordinator/Worker architecture that motivates structured actor lifecycle
- [ADR-007](ADR-007-axum-migration.md) -- Axum migration that increases the number of concurrent components
