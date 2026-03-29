# Awesome Varpulis [![Awesome](https://awesome.re/badge.svg)](https://awesome.re)

> A curated list of resources, patterns, integrations, and community projects for [Varpulis](https://github.com/varpulis/varpulis) — the temporal pattern detection engine.

## Contents

- [Official](#official)
- [Integrations](#integrations)
- [Pattern Libraries](#pattern-libraries)
- [Tutorials & Articles](#tutorials--articles)
- [Examples](#examples)
- [Tools](#tools)
- [Community](#community)

## Official

- [Varpulis](https://github.com/varpulis/varpulis) — Core CEP engine (Rust)
- [Documentation](https://www.varpulis-cep.com/docs/) — Full docs site
- [Playground](https://demo.varpulis-cep.com/playground) — Try VPL in your browser
- [Live Demo](https://demo.varpulis-cep.com) — Control plane demo
- [CLI Reference](https://www.varpulis-cep.com/docs/reference/cli-reference) — All CLI commands
- [crates.io](https://crates.io/crates/varpulis-cli) — Rust crate

## Integrations

- [n8n Node](https://github.com/varpulis/varpulis/tree/main/integrations/n8n-nodes-varpulis) — Temporal pattern detection in n8n workflows (WASM-powered)
- [WASM Engine](https://github.com/varpulis/varpulis/tree/main/crates/varpulis-engine-wasm) — Full VPL engine compiled to WebAssembly for JS/TS embedding
- [MCP Server](https://github.com/varpulis/varpulis/tree/main/crates/varpulis-mcp) — Model Context Protocol for AI agent integration
- [VS Code Extension](https://github.com/varpulis/varpulis/tree/main/vscode-varpulis) — LSP with diagnostics, completion, hover docs

## Connectors

| Connector | Direction | Status |
|-----------|-----------|--------|
| MQTT | In/Out | Battle-tested |
| Kafka | In/Out | Battle-tested |
| NATS | In/Out | Battle-tested |
| HTTP/Webhooks | In/Out | Battle-tested |
| PostgreSQL/MySQL/SQLite | In/Out | Tested |
| Redis | In/Out | Tested |
| AWS Kinesis | In/Out | Available |
| AWS S3 | Out | Available |
| Elasticsearch | Out | Available |
| Apache Pulsar | In/Out | Available |
| PostgreSQL CDC | In | Available |

## Pattern Libraries

- [Security Patterns](https://github.com/varpulis/varpulis/tree/main/docs/scenarios/cyber-kill-chain.md) — MITRE ATT&CK-aligned detection rules
- [Fraud Detection](https://github.com/varpulis/varpulis/tree/main/docs/scenarios/fraud-detection.md) — Payment fraud, account takeover patterns
- [Insider Trading](https://github.com/varpulis/varpulis/tree/main/docs/scenarios/insider-trading.md) — Trading surveillance patterns
- [Patient Safety](https://github.com/varpulis/varpulis/tree/main/docs/scenarios/patient-safety.md) — Medical event patterns
- [Predictive Maintenance](https://github.com/varpulis/varpulis/tree/main/docs/scenarios/predictive-maintenance.md) — Equipment failure prediction

## Tutorials & Articles

- [Getting Started](https://www.varpulis-cep.com/docs/tutorials/getting-started) — 5-minute quickstart
- [Interactive Shell Tutorial](https://www.varpulis-cep.com/docs/tutorials/interactive-shell-tutorial) — Python-interpreter-style VPL
- [SASE+ Patterns Guide](https://www.varpulis-cep.com/docs/guides/sase-patterns) — Temporal pattern matching deep-dive
- [Forecasting Architecture](https://www.varpulis-cep.com/docs/architecture/forecasting) — PST-based prediction internals
- [Varpulis vs Flink CEP](https://www.varpulis-cep.com/docs/comparisons/varpulis-vs-flink) — Feature comparison
- [Varpulis vs Esper](https://www.varpulis-cep.com/docs/comparisons/varpulis-vs-esper) — Feature comparison

## Examples

- [HVAC Monitoring](https://github.com/varpulis/varpulis/tree/main/starters/iot) — IoT temperature monitoring with MQTT (Docker one-liner)
- [Fraud Detection](https://github.com/varpulis/varpulis/tree/main/starters/fraud) — Account takeover with PST forecasting
- [Financial Markets](https://github.com/varpulis/varpulis/tree/main/examples/financial_markets.vpl) — Trading signal detection
- [Reusable Patterns](https://github.com/varpulis/varpulis/tree/main/examples/reusable_patterns.vpl) — Component library patterns

## Tools

- `varpulis interactive` — Split-pane TUI with topology, events, metrics
- `varpulis interactive --json` — JSON-line protocol for agent automation
- `varpulis infer` — Infer event types from sample data
- `varpulis simulate --trace` — Pipeline explain mode
- `varpulis simulate --watch` — Auto-reload on file changes
- `varpulis connector list` — Discover available connectors

## Deployment

- [Docker Compose](https://github.com/varpulis/varpulis/tree/main/deploy/docker) — Docker stacks (single node, cluster, SaaS)
- [Kubernetes](https://github.com/varpulis/varpulis/tree/main/deploy/kubernetes) — K8s manifests with Kustomize overlays
- [Helm Chart](https://github.com/varpulis/varpulis/tree/main/deploy/helm) — Helm chart for coordinator/worker clusters
- [Grafana Dashboards](https://github.com/varpulis/varpulis/tree/main/deploy/docker/grafana/dashboards) — Pre-configured monitoring

## Performance

| Benchmark | Speed |
|-----------|-------|
| Core SASE+ pattern matching | 1.5M evt/s |
| Full VPL pipeline | 410K evt/s |
| CLI end-to-end | 256K evt/s |
| Multi-query Hamlet (50 concurrent) | 950K evt/s |
| Single-symbol prediction | 51 ns |

## Community

- [GitHub Discussions](https://github.com/varpulis/varpulis/discussions) — Questions, ideas, showcase
- [GitHub Issues](https://github.com/varpulis/varpulis/issues) — Bug reports, feature requests

## Contributing

Contributions welcome! Please read the [contributing guide](https://github.com/varpulis/varpulis/blob/main/CONTRIBUTING.md).

---

*Maintained by the Varpulis team. PRs adding resources are welcome.*
