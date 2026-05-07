<p align="center">
  <img src="docs/assets/logo.png" width="320" alt="Varpulis">
</p>

<p align="center"><strong>Open-source SASE+ engine for SIEM correlation and MITRE ATT&amp;CK kill-chain detection.</strong></p>

[![CI](https://github.com/varpulis/varpulis/actions/workflows/ci.yml/badge.svg)](https://github.com/varpulis/varpulis/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/varpulis-cli.svg)](https://crates.io/crates/varpulis-cli)
[![docs.rs](https://docs.rs/varpulis-core/badge.svg)](https://docs.rs/varpulis-core)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT)

[Documentation](https://www.varpulis-cep.com/docs/) · [Live Demo](https://demo.varpulis-cep.com) · [Quick Start](#quick-start) · [Security Demo](examples/security-demo/) · [SIEM Evasion Lab](docs/siem-evasion-lab-01-psexec.md)

---

- **Sequence detection SIEMs can't model.** Multi-step kill chains (`A -> all B -> C within 5m`) — Sigma and KQL match single events; behavioral patterns survive renamed binaries, swapped C2, novel evasions.
- **250K events/sec real-time** on a single core, end-to-end (file → match → emit). 1.5M evt/s on the SASE+ core. Single 15 MB Rust binary, no JVM.
- **VPL: rules a blue team can read.** Declarative, auditable, version-controlled. Compiles to a Rust state machine — no DSL-on-DSL, no XML, no Spark job to babysit.

```python
# Lateral movement: SMB connect → remote service exec within 2 minutes
# MITRE T1021.002 — catches PsExec, renamed PsExec, WMI remote exec, same pattern
stream LateralMovement = SysmonNetworkConnect .where(DestinationPort == 445) as smb
    -> SysmonProcessCreate .where(ParentImage.contains("services.exe")) as remote_exec
    .within(2m)
    .emit(rule: "lateral_movement_smb", mitre: "T1021.002",
          source: smb.Hostname, target: smb.DestinationIp,
          process: remote_exec.Image, cmdline: remote_exec.CommandLine)
```

A SIEM rule sees `services.exe` start a child — looks normal in isolation. Varpulis sees the SMB→exec sequence within 2 minutes — that's the behavioral signature of remote execution, regardless of which tool executed it.

## Security: Kill Chain Detection

```bash
# Blue mode: detect kill chains in Sysmon logs
varpulis detect --rules rules/ --events sysmon.jsonl

# Red mode: test which rules survive evasion (Sigma vs. behavioral, head-to-head)
varpulis analyze --rules rules/ --baseline normal.jsonl --evasion evasion.jsonl
```

```
┌───────────────────┬─────────────────────┬────────────┬────────────┬───────────┐
│ Rule              ┆ MITRE               ┆ Baseline   ┆ Evasion    ┆ Verdict   │
╞═══════════════════╪═════════════════════╪════════════╪════════════╪═══════════╡
│ sigma_psexec      ┆ T1021.002           ┆ DETECT (1) ┆ MISS       ┆ EVADABLE  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ behavioral_psexec ┆ T1021.002,T1036.003 ┆ DETECT (1) ┆ DETECT (1) ┆ RESILIENT │
└───────────────────┴─────────────────────┴────────────┴────────────┴───────────┘
```

Validated against real [MORDOR APT29](https://securitydatasets.com/) datasets at 25K+ events/sec.

- [`examples/security-demo/`](examples/security-demo/) — 11 detection VPLs (lateral movement, credential dumping, persistence, exfil burst, full kill chain, predictive kill chain) + 5 paired Sigma-vs-behavioral comparisons + asciinema run.
- [SIEM Evasion Lab](docs/siem-evasion-lab-01-psexec.md) — deep-dives on Sigma blind spots: [PsExec](docs/siem-evasion-lab-01-psexec.md), [credential dump](docs/siem-evasion-lab-02-credential-dump.md), [lateral movement](docs/siem-evasion-lab-03-lateral-movement.md), [persistence](docs/siem-evasion-lab-04-persistence.md).
- [Replacing Trellix ACE with Varpulis](docs/replacing-trellix-ace.md) — ESM → Kafka → Varpulis migration guide: architectural seam, why ACE rules go silent under load, rule translation, parallel-run cutover.
- [`varpulis security init`](crates/varpulis-cli/src/commands/security_init.rs) scaffolds a starter project; [`varpulis deploy-rules`](crates/varpulis-cli/src/commands/deploy_rules.rs) deploys to a running coordinator.

## Quick Start

```bash
cargo install varpulis-cli
varpulis interactive --no-tui
```

```
vpl> event Tick: price: float
vpl> stream Spike = Tick .where(price > 100) .emit(alert: "spike", price: price)
vpl> Tick { price: 42.0 }
vpl> Tick { price: 150.0 }
→ Spike: {"alert":"spike","price":150}
```

The default `varpulis interactive` opens a split-pane TUI with topology, live events, input, and metrics. Add `--no-tui` for a plain text shell, `--json` for agent automation.

<p align="center">
  <img src="docs/assets/recordings/tui-split-pane.gif" alt="Varpulis TUI" width="720">
</p>

## Why Varpulis?

| | Varpulis | Flink CEP | Esper | Siddhi |
|---|---|---|---|---|
| **Temporal patterns** (Kleene `+/*`, negation, within) | Native (SASE+) | Limited | Yes | Partial |
| **Predictive forecasting** | `.forecast()` built-in | No | No | No |
| **Deployment** | Single binary (15 MB) | JVM cluster | Embedded JVM | Embedded JVM |
| **DSL** | VPL (dedicated) | Java API | EPL | SiddhiQL |
| **Throughput** | 1.5M evt/s (single core) | ~500K evt/s¹ | ~1M evt/s¹ | ~300K evt/s¹ |

¹ Approximate figures from published benchmarks and vendor documentation; workload-dependent.

**`.forecast()` is unique.** It uses Probabilistic Suffix Trees to predict that a pattern is *about to* complete — before the final event arrives. Combined with Hawkes process intensity estimation and conformal prediction intervals, it turns reactive detection into proactive alerting.

## Performance

| What | Speed |
|------|-------|
| Core SASE+ pattern matching | **1.5M evt/s** |
| Full VPL pipeline (filter + emit) | **410K evt/s** |
| CLI end-to-end (file → process → output) | **256K evt/s** |
| Multi-query Hamlet (50 concurrent) | **950K evt/s** |
| Single-symbol prediction | **51 ns** |

Single core. [Detailed benchmarks →](docs/PERFORMANCE_ANALYSIS.md)

## Connectors

| | Status | Direction |
|---|---|---|
| MQTT, Kafka, NATS, HTTP | **Battle-tested** | In/Out |
| PostgreSQL/MySQL/SQLite, Redis | Tested | In/Out |
| Kinesis, S3, Elasticsearch, Pulsar, CDC | Available | Varies |
| Sysmon, Splunk HEC, Slack | Security-focused | Varies |

Each connector is an independent crate. The default binary includes all; build with `--features mqtt,kafka` for a minimal binary.

## Features

<details>
<summary><strong>Language</strong></summary>

- Pipeline operators: `.where()`, `.window()`, `.aggregate()`, `.emit()`, `.to()`, `.alert()`
- SASE+ patterns: sequences (`->`), Kleene closures (`+`, `*`), negation (`AND NOT`)
- Forecasting: `.forecast()` — PST-based prediction with confidence and horizon
- Alert webhooks: `.alert(webhook: "url", message: "{field}")` — fire-and-forget
- Windows: tumbling, sliding, session, count-based
- Aggregations: 15+ functions (sum, avg, ema, percentile, stddev, ...) — SIMD-accelerated
- Joins: inner, LEFT, RIGHT, FULL outer with null-fill
- Imperative: `var`, `if/else`, `while`, `for`, functions, lambdas
- Compile-time meta-programming: `for row in 0..4:` generates streams
</details>

<details>
<summary><strong>Developer Experience</strong></summary>

- Interactive TUI with split-pane topology/events/metrics (`varpulis interactive`)
- Schema inference from sample data (`varpulis infer --input data.jsonl`)
- Pipeline trace / explain mode (`--trace`)
- Watch mode with auto-reload (`--watch`)
- VS Code extension (LSP: diagnostics, completion, hover, go-to-definition)
- MCP server for AI-assisted development
- JSON-line protocol for agent automation (`--json`)
</details>

<details>
<summary><strong>Operations</strong></summary>

- Single binary, Docker, Kubernetes (Helm chart included)
- Coordinator/worker cluster with Raft consensus
- Multi-tenant SaaS mode with RBAC and SSO/OIDC
- Prometheus metrics, OpenTelemetry tracing, Grafana dashboards
- RocksDB state persistence with optional AES-256-GCM encryption
- Circuit breaker, dead letter queue, backpressure signaling
</details>

## Beyond Security

Varpulis is a general SASE+ engine — fraud detection, IoT anomalies, trend prediction. The [playground](https://varpulis-cep.com/playground) shows `.increasing(temperature)` detecting rising HVAC sensor values; `.forecast()` predicts pattern completion before the final event. See [`examples/`](examples/) for fraud, finance, and sensor pipelines.

## Documentation

| | |
|---|---|
| [Getting Started](docs/tutorials/getting-started.md) | [Interactive Shell Tutorial](docs/tutorials/interactive-shell-tutorial.md) |
| [VPL Language Tutorial](docs/tutorials/language-tutorial.md) | [SASE+ Patterns Guide](docs/guides/sase-patterns.md) |
| [Forecasting Architecture](docs/architecture/forecasting.md) | [CLI Reference](docs/reference/cli-reference.md) |
| [Cluster Tutorial](docs/tutorials/cluster-tutorial.md) | [Production Deployment](docs/PRODUCTION_DEPLOYMENT.md) |
| [System Architecture](docs/architecture/system.md) | [All Tutorials →](docs/tutorials/) |

## Build & Test

```bash
cargo build               # build the workspace
cargo test                # unit + integration tests
cargo clippy              # lint
make verify               # full local gate: fmt + clippy + audit + deny
```

`make verify` is a thin wrapper around `scripts/verify.sh` and runs the same gates as CI. Subsets are available: `make verify-fmt`, `make verify-clippy`, `make verify-audit`, `make verify-deny`. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development workflow.

## Contributing

Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

## Acknowledgments

SASE/SASE+ — [Wu et al. SIGMOD 2006](https://dl.acm.org/doi/abs/10.1145/1142473.1142520), [Agrawal et al. SIGMOD 2008](https://www.lix.polytechnique.fr/~yanlei.diao/publications/sase-sigmod08-long.pdf) · Hamlet — [Poppe et al. SIGMOD 2021](https://arxiv.org/abs/2101.00361) · Built with [Pest](https://pest.rs/) and [Tower-LSP](https://github.com/ebkalderon/tower-lsp)

---

<p align="center">
  <strong>Production deployment · managed cloud · enterprise connectors → <a href="https://varpulis-cep.com/poc">varpulis-cep.com/poc</a></strong>
</p>
