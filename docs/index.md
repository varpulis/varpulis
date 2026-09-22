---
layout: home

hero:
  name: Varpulis
  text: The detection engine
  tagline: Correlation rules on the event stream you already forward, outside the SIEM — sequences across hosts, judged in the time the logs happened.
  actions:
    - theme: brand
      text: Get started
      link: /tutorials/getting-started
    - theme: alt
      text: The language
      link: /language/overview
    - theme: alt
      text: SIEM Evasion Lab
      link: /siem-evasion-lab-01-psexec

features:
  - title: Sequences, not single events
    details: "This, then that, on the same host or across hosts, within a window: SASE+ sequences with negation, Kleene closures, aggregates and joins. A kill chain is one rule."
  - title: Log time, not arrival time
    details: ".within(2m) means two minutes between the timestamps in the logs. A replay a week later raises the same alerts; a burst of late events cannot fake a match."
  - title: State that survives a crash
    details: "A program's open sequences, windows and joins can be snapshotted with the stream position they stand for and restored into a fresh engine — the half-matched attack is not lost."
  - title: Trend aggregation
    details: "Hamlet shares work across many trend queries over bursty streams, where evaluating each query alone does not scale."
  - title: Forecasting
    details: "Prediction suffix trees estimate which pattern is completing, and how likely it is, with sub-microsecond inference."
  - title: A library, not a platform
    details: "The engine opens no socket and carries no async runtime. It runs inside Vejas as a detect unit, or offline with varpulis simulate."
---

## What Varpulis is now

Varpulis is its engine. In September 2026 the platform around it — the
server, the cluster, the SaaS layer and its connectors — was retired in favour
of [Vejas](https://vejas.dev), an open-source event runtime that runs a
Varpulis program as a `detect` unit: one durable JetStream consumer over the
program's sources, alerts published before input is acknowledged, state
snapshotted and resumed by stream sequence after a crash. The decision and its
reasons are in [ADR-008](/adr/008-engine-only-platform-retired).

What stayed here is what a detection engineer writes and reads: the language,
its semantics, the engines behind it, the scenarios and the
[SIEM Evasion Lab](/siem-evasion-lab-01-psexec).

## A rule, end to end

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
varpulis check lateral.vpl                      # ok, or the reason and the line
varpulis simulate -p lateral.vpl -e lateral.evt # every emit as a JSON line
```

On a bus, the same file under `detects/` in Vejas is a running unit. The
[getting started](/tutorials/getting-started) walks through both.

## Where to go next

- [The language](/language/overview) — events, streams, operators, built-ins.
- [SASE+ semantics](/adr/004-sase-plus-semantics) — what a sequence means, exactly.
- [Scenarios](/scenarios/) — kill chains, fraud, insider trading, patient safety.
- [Comparisons](/comparisons/varpulis-vs-flink) — against Flink, Esper, Arroyo, Proton and Kafka Streams.
- [The changelog](https://github.com/varpulis/varpulis/blob/main/CHANGELOG.md).

Working on detection for a SOC? [www.varpulis-cep.com](https://www.varpulis-cep.com)
has the fixed-price proof of concept, and a free first step: send three of your
rules, get them back ported.
