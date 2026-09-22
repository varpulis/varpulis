# ADR-008: Varpulis Is Its Engine — the Platform Is Retired in Favour of Vejas

**Status:** Accepted
**Date:** 2026-09-22
**Authors:** Varpulis Team

## Context

Two products, no users on either. Varpulis: 36 crates, 158 000 lines, 947
packages in the lock, a tokio platform — cluster, CLI and server, SaaS,
fourteen connectors, a web UI — around a complex-event-processing engine.
Vejas: one crate, 11 500 lines, 175 packages, a sync NATS client and OS
threads, JetStream durable consumers, a per-event language that cannot say
"this, then that, within two minutes".

The merge evaluation of 2026-09-21 was decided on adoption alone — the best
stack, the simplest use, robustness, performance — and it found three facts
that settle the question. The surface: 36 crates and 23.8 MB against 1 crate
and 5.9 MB, for the same job on the bus. The engine is portable: `varpulis-core`,
`parser`, `sase`, `hamlet`, `pst`, `zdd` and `simd` carry no async runtime,
and `varpulis-runtime` builds without one. The bus semantics: after `kill -9`
Vejas delivered 20 000 of 20 000 events, the Varpulis NATS connector lost
9 246 of them, because core NATS is at most once and JetStream is not.

Since then the engine went where the evaluation sent it. `varpulis-engine`
(#264) is the engine as a library with no runtime in its tree; the
connectors' decoder moved into `varpulis-core` for it (#265); a program can
snapshot and restore its state (#266). Vejas runs VPL programs as `detect`
units with the contract of a flow — one durable consumer per unit over all
its subjects, in stream order; emit before ack; poison to the dead-letter
queue; snapshot every few seconds and resume by sequence after a crash
(Vejas ADR-0031). Everything the Varpulis platform did — the bus, the
consumers, leases, versions, the panel, MCP, metrics — Vejas does with one
crate, and does it with a durable consumer where Varpulis had a subscription.

## Decision

Varpulis is its engine. The repository keeps what the engine is and what
serves the people who write for it:

- the ten engine crates — `core`, `parser`, `runtime`, `sase`, `zdd`, `pst`,
  `hamlet`, `simd`, `dead-letter`, `engine` — and the language tooling: the
  language server, the WebAssembly parser and engine;
- a small `varpulis` command: `check`, `parse`, `simulate` over `.evt`
  files, for authors and for the documentation's own checks;
- the language reference, the semantics, the benchmarks and comparisons,
  the scenarios, the SIEM evasion lab, the engine's decision records.

It removes the platform:

- the crates `varpulis-cli` (27 000 lines: server, SaaS, auth, billing,
  deploy, coordinator, federation, connectors), `varpulis-cluster` (26 000),
  `varpulis-mcp`, the fourteen connector crates with `connector-api` and the
  `connectors` umbrella, `enrichment`, `db`, `datagen`, `actors` (nothing
  depended on it), and the `varpulis` meta-crate;
- from `varpulis-runtime`, its four optional dependencies on retired crates
  and the 22 features that only the platform used — connectors, persistence
  backends, encryption at rest, the interactive shell. Its `async-runtime`
  feature stays for now: tokio only, used by its tests and benches; removing
  it is the next change, not this one;
- `deploy/`, the Docker-bound test suites, `demos/`, `integrations/`,
  `starters/`, the stream builder and the public site sources, the SaaS and
  cluster scripts, the Homebrew formula, the Docker workflows, the nine
  broker-bound CI jobs, and the documentation of the platform;
- publishing: the crates.io list shrinks to the kept crates and gains
  `varpulis-engine`; a release ships the small command and the language
  server; the container images stop.

Not decided here, because they are the owner's to decide: archiving the
GitHub repository, the demo at demo.varpulis-cep.com and its server, the
marketing site, the versions already on crates.io, the web UI repository.

## Alternatives Considered

**Keep the platform next to Vejas.** Two runtimes, two consumer paths, the
connectors twice. The evaluation rejected it on every criterion.

**Remove the runtime's async paths in the same change.** `varpulis-runtime`
is 47 000 lines with 89 test files; taking tokio out of it is its own
change, made safer by this one.

**Archive the repository outright.** Vejas consumes the engine by git
revision; the repository lives on as the engine's home.

## Consequences

**Easier.** One product. The engine's dependency gate stays (`Engine Stays
Runtime-Free`). CI shrinks from 28 jobs to the ones that test the engine.
The build is the engine's build.

**Constrained.** There is no command that runs a program on a bus: a program runs as a Vejas detect unit.
Connectors are Vejas's. The security demo runs on Vejas.

**Costs.** The platform's history is in git, not in the tree. ADR-003 and
ADR-007 are superseded by this one.
