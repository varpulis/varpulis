# varpulis-actors

A lightweight actor framework with supervision and health observation for Varpulis.

See [ADR-006](../../design/decisions/ADR-006-actor-framework.md) for the architectural decision record.

## Architecture

```
                         ┌──────────────────────────────────────────────────┐
                         │                   Runtime                       │
                         │                                                 │
                         │   ┌─────────────────────────────────────────┐   │
                         │   │           Supervisor (optional)         │   │
                         │   │   restart policy, backoff, max retries  │   │
                         │   │                                         │   │
  ┌──────────┐  send()   │   │   ┌─────────┐  Envelope  ┌──────────┐  │   │
  │ Producer ├───────────┼───┼──>│ Mailbox ├───────────>│  Actor   │  │   │
  │          │           │   │   │ (mpsc)  │            │ run loop │  │   │
  └──────────┘           │   │   └─────────┘            └────┬─────┘  │   │
                         │   │                                │        │   │
  ┌──────────┐  ask()    │   │                          ┌─────┴──────┐ │   │
  │ Caller   ├───────────┼───┤                          │ Observable │ │   │
  │          │<──reply────┼───┤                          │   State    │ │   │
  └──────────┘           │   │                          └────────────┘ │   │
                         │   └─────────────────────────────────────────┘   │
  ┌──────────┐ observe() │                                                 │
  │ Monitor  ├───────────┼──> ActorHandle ──> health / state snapshot      │
  └──────────┘           │                                                 │
                         │   SIGINT/SIGTERM ──> CancellationToken          │
                         │                      ──> graceful shutdown      │
                         └──────────────────────────────────────────────────┘
```

### Component overview

| Component | File | Purpose |
|---|---|---|
| `Actor` trait | `src/actor.rs` | Defines lifecycle: `name()`, `observable_state()`, `run()` |
| `Handler` trait | `src/actor.rs` | Optional request/reply interface for message-driven actors |
| `Mailbox` | `src/mailbox.rs` | Bounded `mpsc` channel carrying typed `Envelope` values |
| `MailboxSender` | `src/mailbox.rs` | Cloneable sending half; supports `send()`, `ask()`, `observe()` |
| `ActorContext` | `src/context.rs` | Execution environment: mailbox receiver + shutdown token |
| `ActorHandle` | `src/handle.rs` | External reference for health checks and messaging |
| `Supervisor` | `src/supervisor.rs` | Restart logic with configurable policy and backoff |
| `Runtime` | `src/runtime.rs` | Manages spawned actors, signal handling, graceful shutdown |
| `MessageSink` / `MessageSource` | `src/builders.rs` | Type-safe wiring traits for connecting actors |
| `fan_in_message_type!` | `src/message.rs` | Macro to combine multiple message types into one enum |

## Usage: implementing a new actor

### 1. Define the actor struct and its observable state

```rust
use varpulis_actors::{Actor, ActorContext, ActorExitStatus};

/// An actor that counts incoming events and exposes the count.
struct EventCounter {
    count: u64,
    label: String,
}

#[async_trait::async_trait]
impl Actor for EventCounter {
    /// The type returned by `observe()` -- must be Debug + Serialize + Clone.
    type ObservableState = u64;

    fn name(&self) -> &str {
        &self.label
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.count
    }

    async fn run(mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorExitStatus> {
        loop {
            tokio::select! {
                // Graceful shutdown: the runtime or supervisor cancelled the token
                _ = ctx.shutdown.cancelled() => {
                    tracing::info!("{}: shutting down with count={}", self.label, self.count);
                    return Ok(());
                }
                // Process the next message from the mailbox
                envelope = ctx.mailbox.recv() => {
                    match envelope {
                        Some(_) => self.count += 1,
                        None => return Ok(()), // all senders dropped
                    }
                }
            }
        }
    }
}
```

### 2. Spawn the actor on the runtime

```rust
use varpulis_actors::Runtime;

#[tokio::main]
async fn main() {
    let mut runtime = Runtime::new();

    let counter = EventCounter { count: 0, label: "my-counter".into() };

    // Spawn returns an ActorHandle for observation and messaging
    let handle = runtime.spawn(counter, /* mailbox_capacity */ 100);

    // Send messages to the actor
    handle.send("event-1").await.unwrap();
    handle.send("event-2").await.unwrap();

    // Observe the actor's state without stopping it
    let count = handle.observe().await.unwrap();
    println!("Current count: {}", count);

    // Check health
    let health = handle.check_health();
    println!("Health: {:?}", health);

    // Shut down and wait for all actors to finish
    runtime.shutdown();
    runtime.run_to_completion().await;
}
```

### 3. Use typed messages with fan-in

When an actor needs to receive messages from multiple producers with different types, use the `fan_in_message_type!` macro:

```rust
use varpulis_actors::fan_in_message_type;

struct Tick;
struct DataEvent { payload: Vec<u8> }
struct FlushCommand;

// Generates an enum `WorkerMessage` with From impls for each variant
fan_in_message_type!(WorkerMessage, Tick, DataEvent, FlushCommand);

// In the actor's run loop, match on the enum:
// match envelope {
//     Some(Envelope::Message(msg)) => {
//         if let Ok(msg) = msg.downcast::<WorkerMessage>() {
//             match *msg {
//                 WorkerMessage::Tick(_) => { /* periodic tick */ }
//                 WorkerMessage::DataEvent(e) => { /* process data */ }
//                 WorkerMessage::FlushCommand(_) => { /* flush buffers */ }
//             }
//         }
//     }
//     ...
// }
```

### 4. Wire actors together with MessageSink

```rust
use varpulis_actors::{MessageSink, Runtime};

// Producer actor holds a sink to forward processed messages
struct Parser {
    output: Box<dyn MessageSink<ParsedEvent>>,
}

// Consumer actor receives ParsedEvent messages
struct Indexer { /* ... */ }

// Wiring at construction time:
let mut runtime = Runtime::new();
let indexer = Indexer { /* ... */ };
let indexer_handle = runtime.spawn(indexer, 200);

// The indexer's MailboxSender implements MessageSink<ParsedEvent>
let parser = Parser {
    output: Box::new(indexer_handle.sender().clone()),
};
let _parser_handle = runtime.spawn(parser, 100);
```

## Supervision configuration

The `Supervisor` wraps an actor factory and restarts the actor according to a policy.

### Configuration options

| Field | Type | Default | Description |
|---|---|---|---|
| `restart_policy` | `RestartPolicy` | `OnFailure` | When to restart: `Always`, `OnFailure`, or `Never` |
| `max_restarts` | `u32` | `5` | Max restarts allowed within the restart window |
| `restart_window` | `Duration` | `60s` | Window for counting restarts; resets after this duration |
| `base_restart_delay` | `Duration` | `100ms` | Initial delay before restart; doubles with each consecutive restart (exponential backoff) |
| `mailbox_capacity` | `usize` | `100` | Mailbox size for each spawned actor instance |

### Restart policies

- **`Always`** -- Restart regardless of exit status. Use for actors that must always be running (health monitors, heartbeat senders).
- **`OnFailure`** -- Restart only on `Failure` or `Panicked` exit status. Use for actors where a clean exit (`Success`, `Quit`) is intentional.
- **`Never`** -- Do not restart. Use for one-shot tasks or actors whose lifecycle is managed externally.

### Example: supervised connector

```rust
use varpulis_actors::{Supervisor, SupervisorConfig, RestartPolicy};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

let config = SupervisorConfig {
    restart_policy: RestartPolicy::OnFailure,
    max_restarts: 10,
    restart_window: Duration::from_secs(300),    // 5-minute window
    base_restart_delay: Duration::from_millis(500), // 500ms, 1s, 2s, 4s, ...
    mailbox_capacity: 50,
};

let supervisor = Supervisor::new(
    "mqtt-connector-supervisor",
    || MqttConnector::new("broker.example.com:1883"),
    config,
);

let shutdown = CancellationToken::new();
let exit_status = supervisor.run(shutdown).await;
// exit_status tells you whether the actor exited cleanly or the
// restart limit was exceeded
```

### Backoff behavior

Restarts use exponential backoff: the delay before the N-th restart is `base_restart_delay * 2^(N-1)`, capped at `2^5 = 32x` the base delay. For the default 100ms base:

| Restart # | Delay |
|---|---|
| 1 | 100ms |
| 2 | 200ms |
| 3 | 400ms |
| 4 | 800ms |
| 5 | 1.6s |
| 6+ | 3.2s (capped) |

If `max_restarts` is exceeded within the `restart_window`, the supervisor gives up and returns `ActorExitStatus::Failure("max restarts exceeded")`. The restart counter resets when the window elapses without hitting the limit.

### Monitoring supervisor metrics

```rust
let metrics = supervisor.metrics();
println!(
    "restarts={}, panics={}, uptime={}s",
    metrics.restart_count,
    metrics.panic_count,
    metrics.uptime_secs,
);
```

## Design influences

- **[thin-edge.io](https://github.com/thin-edge/thin-edge.io)** -- Thin, Tokio-native actor model without a separate runtime. Influenced the decision to keep the framework minimal and avoid a global actor registry.
- **[Quickwit](https://github.com/quickwit-oss/quickwit/tree/main/quickwit/quickwit-actors)** -- Observable state pattern and supervision tree design. Influenced the `observe()` mechanism and the `Supervisor` abstraction.
