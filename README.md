<p align="center">
  <img src="docs/assets/logo.png" width="320" alt="Varpulis">
</p>

<p align="center"><strong>Detect kill chains your SIEM misses.</strong><br>Behavioral sequence detection in Rust. Dual red/blue mode.</p>

[![CI](https://github.com/varpulis/varpulis/actions/workflows/ci.yml/badge.svg)](https://github.com/varpulis/varpulis/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/varpulis-cli.svg)](https://crates.io/crates/varpulis-cli)
[![docs.rs](https://docs.rs/varpulis-core/badge.svg)](https://docs.rs/varpulis-core)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT)

[Documentation](https://www.varpulis-cep.com/docs/) · [Live Demo](https://demo.varpulis-cep.com) · [Quick Start](#quick-start) · [Examples](examples/) · [Benchmarks](docs/PERFORMANCE_ANALYSIS.md)

---

```python
stream FraudAlert = Login as login
    -> all Transfer where user_id == login.user_id as txs
    -> Logout where user_id == login.user_id
    .within(5m)
    .trend_aggregate(total: sum_trends(txs.amount), transfers: count_events(txs))
    .where(total > 10000)
    .forecast(confidence: 0.7, horizon: 2m, warmup: 50)
    .emit(user: login.user_id, total: total, transfers: transfers)
```

Login → *all* transfers → logout within 5 minutes. Kleene closure captures every transfer; Hamlet sums them in O(n). Only fires when total exceeds $10K. `.forecast()` predicts the pattern **before** it completes. No other open-source CEP engine does this.

## Quick Start

```bash
curl -sSf https://raw.githubusercontent.com/varpulis/varpulis/main/scripts/install.sh | sh
varpulis interactive --no-tui
```

```
vpl> event Tick: price: float
vpl> stream Spike = Tick .where(price > 100) .emit(alert: "spike", price: price)
vpl> Tick { price: 42.0 }
vpl> Tick { price: 150.0 }
→ Spike: {"alert":"spike","price":150}
vpl> :save spike_detector.vpl
```

Copy-paste. 30 seconds. No files, no connectors, no Docker.

<p align="center">
  <img src="docs/assets/recordings/tui-split-pane.gif" alt="Varpulis TUI" width="720">
</p>

The default `varpulis interactive` opens a split-pane TUI with topology, live events, input, and metrics. Add `--no-tui` for a plain text shell, `--json` for agent automation.

## Security: Kill Chain Detection

Varpulis detects **multi-step attack sequences** that single-event SIEM rules miss. Renamed PsExec? Different C2 tool? Doesn't matter — behavioral patterns catch what signature rules can't.

```bash
# Blue mode: detect kill chains in Sysmon logs
varpulis detect --rules rules/ --events sysmon.jsonl

# Red mode: test which rules survive evasion
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

Validated against real [MORDOR APT29](https://securitydatasets.com/) datasets at 25K+ events/sec. See the [SIEM Evasion Lab](docs/siem-evasion-lab-01-psexec.md) series for deep-dives on Sigma blind spots.

## Why Varpulis?

| | Varpulis | Flink CEP | Esper | Siddhi |
|---|---|---|---|---|
| **Temporal patterns** (Kleene `+/*`, negation, within) | Native (SASE+) | Limited | Yes | Partial |
| **Predictive forecasting** | `.forecast()` built-in | No | No | No |
| **Deployment** | Single binary (15 MB) | JVM cluster | Embedded JVM | Embedded JVM |
| **DSL** | VPL (dedicated) | Java API | EPL | SiddhiQL |
| **Throughput** | 1.5M evt/s (single core) | ~500K evt/s¹ | ~1M evt/s¹ | ~300K evt/s¹ |

¹ Approximate figures from published benchmarks and vendor documentation; workload-dependent.

**`.forecast()` is unique.** It uses Probabilistic Suffix Trees to predict that a pattern is *about to* complete — before the final event arrives. Combined with Hawkes process intensity estimation and conformal prediction intervals, it turns reactive CEP into proactive alerting.

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

## Documentation

| | |
|---|---|
| [Getting Started](docs/tutorials/getting-started.md) | [Interactive Shell Tutorial](docs/tutorials/interactive-shell-tutorial.md) |
| [VPL Language Tutorial](docs/tutorials/language-tutorial.md) | [SASE+ Patterns Guide](docs/guides/sase-patterns.md) |
| [Forecasting Architecture](docs/architecture/forecasting.md) | [CLI Reference](docs/reference/cli-reference.md) |
| [Cluster Tutorial](docs/tutorials/cluster-tutorial.md) | [Production Deployment](docs/PRODUCTION_DEPLOYMENT.md) |
| [System Architecture](docs/architecture/system.md) | [All Tutorials →](docs/tutorials/) |

## Contributing

Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

## Acknowledgments

SASE/SASE+ — [Wu et al. SIGMOD 2006](https://dl.acm.org/doi/abs/10.1145/1142473.1142520), [Agrawal et al. SIGMOD 2008](https://www.lix.polytechnique.fr/~yanlei.diao/publications/sase-sigmod08-long.pdf) · Hamlet — [Poppe et al. SIGMOD 2021](https://arxiv.org/abs/2101.00361) · Built with [Pest](https://pest.rs/) and [Tower-LSP](https://github.com/ebkalderon/tower-lsp)
