# This Week in Rust — Submission

## Submit to: https://github.com/rust-lang/this-week-in-rust/pulls

### Category: Crate of the Week

**Crate:** [varpulis](https://crates.io/crates/varpulis)

**Description:** Varpulis is a complex event processing (CEP) engine that detects temporal patterns in event streams. Think "login → two transfers > $10K within 5 minutes" expressed in 10 lines of a dedicated DSL (VPL). Features SASE+ pattern matching with Kleene closures, predictive forecasting via PST, and an interactive TUI. 1.5M evt/s on a single core. Now with WASM support for embedding in JavaScript.

**Links:**
- [GitHub](https://github.com/varpulis/varpulis)
- [crates.io](https://crates.io/crates/varpulis)
- [Documentation](https://www.varpulis-cep.com/docs/)

---

### Category: Project Updates

**Title:** Varpulis v0.9.0 — Interactive CEP engine with TUI, WASM, and n8n integration

Varpulis v0.9.0 brings a Python-interpreter-style interactive shell with a ratatui TUI, full VPL engine compiled to WebAssembly (@varpulis/engine on npm), an n8n community node for workflow automation, and an `.alert()` operator for webhook notifications. The engine processes 1.5M events/second on a single core using SASE+ pattern matching.

- [Announcement / Changelog](https://github.com/varpulis/varpulis/blob/main/CHANGELOG.md)
- [Interactive Shell Tutorial](https://www.varpulis-cep.com/docs/tutorials/interactive-shell-tutorial)
