# Varpulis Documentation

Varpulis is a complex-event-processing engine, embedded as a library in a
host that owns the bus; the reference host is
[Vejas](https://github.com/cpoder/vejas), where a VPL program is a `detect`
unit ([ADR-008](adr/008-engine-only-platform-retired.md)).

## Start here

- [Getting started](tutorials/getting-started.md) — install `varpulis`,
  check a program, simulate it over an event file, embed the engine.
- [The language](language/overview.md) — events, streams, operators,
  built-ins, keywords, the grammar.
- [Language tutorial](tutorials/language-tutorial.md).

## Semantics and engines

- [SASE+ semantics](adr/004-sase-plus-semantics.md), [emission modes](adr/006-emission-modes.md),
  [SASE patterns](guides/sase-patterns.md).
- [Windows and aggregations](reference/windows-aggregations.md), [joins](reference/joins.md),
  [outer joins](tutorials/outer-joins-tutorial.md).
- [Trend aggregation](reference/trend-aggregation.md) (Hamlet, [ADR-005](adr/005-hamlet-trend-aggregation.md)),
  [forecasting](architecture/forecasting.md) (prediction suffix trees).
- [Contexts and parallelism](guides/contexts.md), [architecture/parallelism](architecture/parallelism.md).

## Scenarios and comparisons

- [Scenarios](scenarios/) — fraud, kill chains, insider trading, patient safety, predictive maintenance.
- [SIEM evasion lab](siem-evasion-lab-01-psexec.md) — four articles on detections that survive evasion.
- [Comparisons](comparisons/) — Flink, Proton, Arroyo, Kafka Streams, Esper; [benchmarks](spec/benchmarks.md).

## Reference

- [CLI](reference/cli-reference.md) — `varpulis check`, `parse`, `simulate`.
- [Decision records](adr/) — from the parser to the retirement of the platform.
- [Specification](spec/overview.md), [glossary](spec/glossary.md), [MSRV policy](development/MSRV_POLICY.md).
