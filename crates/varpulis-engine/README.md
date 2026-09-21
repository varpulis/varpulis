# varpulis-engine

The Varpulis complex-event-processing engine as a library for a process that
has **no async runtime**: no tokio, no broker client, no HTTP server. Compile a
VPL program, feed it events, publish what comes out on whatever bus you
already run.

```rust
use varpulis_engine::Program;

let mut program = Program::compile(r#"
    event SmbConnect:
        host: str
        target: str

    event ServiceStart:
        host: str
        image: str

    stream LateralMovement = SmbConnect as smb
        -> ServiceStart where host == smb.target as svc
        .within(2m)
        .emit(rule: "lateral_movement", from: smb.host, to: svc.host)
"#)?;

for payload in my_bus.next_batch() {
    for emit in program.feed_json("SmbConnect", &payload)? {
        my_bus.publish(emit.sink.as_ref().and_then(|s| s.topic.as_deref()), emit.to_json());
    }
}
```

## What you get

- **`Program::compile` / `Program::check`** — parse and load, with every
  rejection the CLI's `varpulis check` would give.
- **`Program::sources` / `Program::sinks`** — the program's `.from()` and
  `.to()` declarations, for the host to subscribe and publish. Nothing here
  opens a socket.
- **`Program::feed` / `feed_json` / `feed_batch`** — events in, `Emit`s out.
  Time is **event time**: a payload's `timestamp` (or `ts`, `@timestamp`) is
  the event's time, so a replay reproduces the same emits.
- **`Emit::to_json`** — the exact payload the Varpulis NATS sink publishes,
  `{"event_type", "timestamp", …fields}`.
- **`Program::end_of_input`** — close open windows, for bounded input.

## What you do not get

Connectors, checkpointing, a server, a cluster. The engine's state is in the
`Program`; snapshotting and resuming it is the host's contract (Vejas
ADR-0031 describes one).

## The guarantee

```
cargo tree -p varpulis-engine
```

names no `tokio`, `reqwest`, `axum`, `async-nats`, `rdkafka` or `arrow`.
`scripts/check-engine-deps.py` asserts it on every pull request, because a
dependency that pulled one of them in would compile without a word and undo
the reason this crate exists.
