<div align="center">

# Varpulis

**A complex-event-processing engine that says "this, then that, within two minutes" — and means it.**

[![CI](https://github.com/varpulis/varpulis/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/varpulis/varpulis/actions/workflows/ci.yml)
[![docs.rs](https://docs.rs/varpulis-core/badge.svg)](https://docs.rs/varpulis-core)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT)

[Language](docs/language/overview.md) · [Semantics](docs/adr/004-sase-plus-semantics.md) · [Scenarios](docs/scenarios/) · [SIEM Evasion Lab](docs/siem-evasion-lab-01-psexec-lateral-movement.md) · [Benchmarks](docs/spec/benchmarks.md)

</div>

Varpulis is the engine: a SASE+ pattern matcher with Kleene closures,
negation, `.within()` judged in **event time**, windows, joins, trend
aggregation and forecasting, driven by VPL, a small language for saying what
a sequence of events means. It is a library. It opens no socket, spawns no
thread and carries no async runtime: `cargo tree -p varpulis-engine` names
no tokio, no broker client, no HTTP server, and a CI gate keeps it so.

It runs inside a host that owns the bus. The reference host is
[Vejas](https://github.com/cpoder/vejas), where a `.vpl` file under
`detects/` is a unit with a durable JetStream consumer, emit-before-ack,
snapshots and resume by sequence — the platform that used to live in this
repository, retired in favour of it ([ADR-008](docs/adr/008-engine-only-platform-retired.md)).

## Ten lines

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

```rust
use varpulis_engine::Program;

let mut program = Program::compile(include_str!("lateral.vpl"))?;
program.feed_json("SmbConnect", br#"{"@timestamp":"2026-09-21T10:00:00Z","host":"ws-1","target":"srv-9"}"#)?;
let emits = program.feed_json("ServiceStart", br#"{"@timestamp":"2026-09-21T10:01:00Z","host":"srv-9","image":"psexesvc.exe"}"#)?;
assert_eq!(emits[0].to_json()["rule"], "lateral_movement");
```

Time is the events' own: the same two payloads three minutes apart in
`@timestamp` match nothing, however fast they arrive. A `Program` can
`snapshot()` its state and `restore()` it into a fresh one; where to keep
the bytes and which position in the input they stand for is the host's
contract.

## The command line

```bash
cargo install --path crates/varpulis-cli
varpulis check examples/security-demo/detect_lateral_movement.vpl
varpulis simulate -p examples/transaction_monitoring.vpl -e examples/transaction_monitoring.evt
```

`check` loads the program into the engine and says `ok` or why not;
`simulate` runs it over an `.evt` file and prints every emit as a JSON line.
There is no `run`: a program runs on a bus as a Vejas detect unit.

## Crates

| Crate | What it is |
|---|---|
| `varpulis-engine` | the engine as a library: `Program::compile / feed / feed_json / snapshot / restore` |
| `varpulis-core`, `varpulis-parser` | the language: AST, values, events, the parser, the payload decoder |
| `varpulis-runtime` | execution: SASE+, windows, joins, checkpoints — built without its `async-runtime` feature by the engine |
| `varpulis-sase`, `varpulis-zdd`, `varpulis-pst`, `varpulis-hamlet`, `varpulis-simd` | pattern matching, Kleene closures, forecasting, trend aggregation, SIMD kernels |
| `varpulis-lsp`, `varpulis-wasm`, `varpulis-engine-wasm` | the language server, the parser and the engine for WebAssembly |
| `varpulis-cli` | `varpulis check`, `parse`, `simulate` |

## Performance

Same machine, same events, the same detection program on the bus: the engine
inside a Vejas detect unit runs at 18 600 events/s at 16 publishers against
a per-event flow at 16 200 (`bench/README.md` in Vejas). In isolation the
engine's share is under 10 µs an event. Head-to-head numbers against Apama,
Arroyo, Flink and Proton are in [docs/spec/benchmarks.md](docs/spec/benchmarks.md)
and [docs/comparisons/](docs/comparisons/).

## Build & test

```bash
cargo build
cargo test --workspace
make verify        # fmt + clippy + audit + deny + doc, the CI gates
```

Rust 1.93 or newer. The engine's dependency gate:
`python3 scripts/check-engine-deps.py`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Decisions are recorded in
[docs/adr/](docs/adr/).

## License

MIT or Apache-2.0, at your option.
