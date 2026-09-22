# Getting Started

Varpulis is a complex-event-processing engine: it reads a stream of events
and says when a *pattern* of them has happened — this, then that, within
two minutes, for the same host. You write the pattern in VPL; the engine
runs as a library inside a host that owns the bus.

## Install the command line

```bash
git clone https://github.com/varpulis/varpulis
cd varpulis
cargo install --path crates/varpulis-cli
varpulis --version
```

## Write a program

`lateral.vpl` — an SMB connection followed, within two minutes, by a
service starting on the machine it connected to:

```vpl
event SmbConnect:
    host: str
    target: str

event ServiceStart:
    host: str
    image: str

stream LateralMovement = SmbConnect as smb
    -> ServiceStart where host == smb.target as svc
    .within(2m)
    .emit(rule: "lateral_movement", from: smb.host, to: svc.host, image: svc.image)
```

```bash
varpulis check lateral.vpl
```

`ok` means the parser and the engine both accept it. Anything else is the
reason, with the line.

## Feed it events

`lateral.evt` — one event per line, `BATCH <ms>` advances the clock:

```
SmbConnect { host: "ws-1", target: "srv-9" }
BATCH 60000
ServiceStart { host: "srv-9", image: "psexesvc.exe" }
```

```bash
varpulis simulate -p lateral.vpl -e lateral.evt
```

```json
{"event_type":"LateralMovement","rule":"lateral_movement","from":"ws-1","to":"srv-9","image":"psexesvc.exe","timestamp":"1970-01-01T00:01:00Z"}
```

Make the second batch `BATCH 180000` — three minutes — and nothing is
emitted: `.within(2m)` is judged in the events' own time, never against
the wall clock, so the same file gives the same answer every run.

## Embed it

```toml
[dependencies]
varpulis-engine = { git = "https://github.com/varpulis/varpulis" }
```

```rust
use varpulis_engine::Program;

let mut program = Program::compile(include_str!("lateral.vpl"))?;
program.feed_json("SmbConnect", br#"{"@timestamp":"2026-09-21T10:00:00Z","host":"ws-1","target":"srv-9"}"#)?;
let emits = program.feed_json("ServiceStart", br#"{"@timestamp":"2026-09-21T10:01:00Z","host":"srv-9","image":"psexesvc.exe"}"#)?;
assert_eq!(emits[0].to_json()["rule"], "lateral_movement");
```

A payload's `@timestamp` (RFC 3339), else `ts` or `timestamp` (epoch
milliseconds), is the event's time; a string `event_type` names its type,
else the one you pass. `program.snapshot()` gives you the engine's state as
bytes — open sequences with their deadlines, windows, joins — and
`restore()` puts it back into a fresh program.

## Run it on a bus

The engine opens no socket. The reference host is
[Vejas](https://github.com/cpoder/vejas): put `lateral.vpl` under
`detects/`, and it is a unit with a durable JetStream consumer over its
`.from()` subjects, emit before ack, a snapshot every few seconds and
resume by sequence after a crash. See *Detect units* in the Vejas book.

## Next

- [The language](../language/overview.md) — events, streams, operators.
- [SASE+ semantics](../adr/004-sase-plus-semantics.md) — what a sequence
  means, exactly.
- [Scenarios](../scenarios/) — fraud, kill chains, predictive maintenance.
