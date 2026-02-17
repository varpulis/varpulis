# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the Varpulis CEP engine. ADRs document significant architectural choices, the reasoning behind them, alternatives that were evaluated, and the consequences of the decision.

## Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [001](001-pest-parser.md) | Pest PEG Parser for VPL | Accepted | 2026-02-17 |
| [002](002-warp-http.md) | Warp as the HTTP Framework | Accepted | 2026-02-17 |
| [003](003-coordinator-worker.md) | Coordinator/Worker Cluster Architecture | Accepted | 2026-02-17 |
| [004](004-sase-plus-semantics.md) | SASE+ Semantics for Pattern Matching | Accepted | 2026-02-17 |
| [005](005-hamlet-trend-aggregation.md) | Hamlet for Multi-Query Trend Aggregation | Accepted | 2026-02-17 |

---

## ADR Template

Copy this template when creating a new ADR. Number sequentially (006, 007, ...).

```markdown
# ADR-NNN: Title

**Status:** Proposed | Accepted | Deprecated | Superseded by ADR-NNN
**Date:** YYYY-MM-DD
**Authors:** Varpulis Team

## Context

[What is the issue or decision that needs to be made? Describe the technical
or organizational forces at play. Keep this factual and neutral.]

## Decision

[What was decided and why? Be specific. Reference benchmarks, papers, or
design constraints where relevant.]

## Alternatives Considered

[What other options were evaluated and why were they rejected?]

## Consequences

### Positive
- ...

### Negative
- ...

## References
- [Links to papers, docs, code, benchmarks, etc.]
```

---

## Guidelines

- Write ADRs at the time a decision is made, not retroactively (or as close to it as possible).
- Decisions that affect the public API, wire format, or performance characteristics of the engine require an ADR.
- Decisions that are purely internal implementation details (choosing a helper crate, refactoring a module) do not require an ADR.
- When a decision is reversed, mark the original ADR as "Superseded" and reference the new one; do not delete the old record.
- ADRs are immutable historical artifacts. Append corrections as a "Revision" section rather than editing the original text.
