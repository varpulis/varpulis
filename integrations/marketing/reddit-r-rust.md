# Reddit /r/rust Post Draft

## Title: Varpulis — a temporal pattern detection engine in Rust (1.5M evt/s, interactive TUI, WASM)

## Flair: 🛠️ project

---

I've been building **Varpulis**, a complex event processing (CEP) engine in Rust that detects temporal patterns in event streams.

**The problem:** You have event streams (logs, IoT sensors, payments, security events). You want to detect "login → two transfers over $10K within 5 minutes" or "temperature spike → pressure drop → vibration within 10 minutes." Traditional tools either can't express temporal sequences or require a JVM cluster.

**What Varpulis does:** A dedicated DSL (VPL) for temporal patterns, compiled to efficient SASE+ NFA automata:

```
stream FraudAlert = Events
    .where(type == "login") as e1
    -> Events.where(type == "transfer") as e2
    -> Events.where(type == "transfer") as e3
    .within(5m)
    .where(e2.amount + e3.amount > 10000)
    .forecast(confidence: 0.8, horizon: 2m)
    .emit(user: e1.user, total: e2.amount + e3.amount)
```

**Performance:** 1.5M evt/s pattern matching, 410K evt/s full pipeline, single core, ~40MB RSS.

**What's new in v0.9.0:**
- Interactive shell — type VPL + events like a Python interpreter
- Split-pane TUI (ratatui) with topology, event stream, metrics
- Full engine compiled to WASM (1.7MB, runs in Node.js/browser)
- `.alert()` operator for webhook notifications
- Pipeline trace mode (EXPLAIN for streaming)
- n8n community node for workflow automation

**Tech stack:** Pest PEG parser, SASE+ NFA engine, ZDD for Kleene closure compression, Hamlet for multi-query aggregation, PST for predictive forecasting. 33 crates, 4500+ tests.

**Links:**
- GitHub: https://github.com/varpulis/varpulis
- crates.io: https://crates.io/crates/varpulis
- Interactive demo: https://demo.varpulis-cep.com/playground

Happy to answer questions about the architecture, SASE+ semantics, or why I didn't just use Flink :)
