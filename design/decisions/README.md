# Architecture Decision Records

This directory extends the original ADR series located in [`docs/adr/`](../../docs/adr/) with new decisions made during the architecture improvement initiative (Track C).

ADRs 001--005 were written at project inception and live in `docs/adr/`. ADRs 006+ live here in `design/decisions/` as part of the ongoing architecture evolution.

## Index

| ADR | Title | Status | Date | Location |
|-----|-------|--------|------|----------|
| [001](../../docs/adr/001-pest-parser.md) | Pest PEG Parser for VPL | Accepted | 2026-02-17 | `docs/adr/` |
| [002](../../docs/adr/002-warp-http.md) | Warp as the HTTP Framework | Superseded by 007 | 2026-02-17 | `docs/adr/` |
| [003](../../docs/adr/003-coordinator-worker.md) | Coordinator/Worker Cluster Architecture | Accepted | 2026-02-17 | `docs/adr/` |
| [004](../../docs/adr/004-sase-plus-semantics.md) | SASE+ Semantics for Pattern Matching | Accepted | 2026-02-17 | `docs/adr/` |
| [005](../../docs/adr/005-hamlet-trend-aggregation.md) | Hamlet for Multi-Query Trend Aggregation | Accepted | 2026-02-17 | `docs/adr/` |
| [006](ADR-006-actor-framework.md) | Actor Framework | Accepted | 2026-02-28 | `design/decisions/` |
| [007](ADR-007-axum-migration.md) | Axum Migration | Accepted | 2026-02-28 | `design/decisions/` |

---

## ADR Template

Copy this template when creating a new ADR. Number sequentially (008, 009, ...).

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
- ADRs 001--005 in `docs/adr/` follow the same template and conventions. The split in directories reflects the project's evolution, not a difference in intent or format.
