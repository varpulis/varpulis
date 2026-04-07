# Varpulis — Marketing & Visibility Playbook

This document captures the remaining **manual** SEO and marketing tasks that were scoped out of the automated tag/keyword pass. Each task is independent and can be executed incrementally.

---

## Context — What's already been done

- README repositioned: "The Rust stream processing engine for real-time detection — Apache Flink alternative for detection engineering"
- Cargo.toml keywords + categories on all published crates (`varpulis`, `varpulis-cli`, `varpulis-sase`, `varpulis-hamlet`, `varpulis-pst`, `varpulis-runtime`, `varpulis-parser`)
- GitHub repo description and 20 topics rewritten around real-traffic keywords
- `public-site/index.html` meta tags (title, description, OG, Twitter, canonical) + JSON-LD `SoftwareApplication` + `FAQPage` schemas
- VitePress `config.ts` meta and OG tags refreshed
- Esper comparison downgraded in docs sidebar (labeled "legacy", moved to last)
- `FUNDING.yml` template placed in `.github/` (uncomment when sponsor profiles exist)

---

## The strategic positioning

**Primary positioning**: Rust stream processing engine — open-source Apache Flink alternative built for detection engineering.

**Secondary wedges**:
1. **Security / detection engineering**: fraud prevention, MITRE ATT&CK coverage, Sigma rule execution, SOC automation.
2. **Fintech**: real-time fraud detection, market surveillance.
3. **IoT / Industrial**: deferred — avoid until Phase 1 is validated (per project memory).

**Closest architectural twin**: Timeplus Proton (single-binary, no-JVM streaming engine). Differentiator: Proton is streaming ClickHouse; Varpulis is streaming pattern detection + forecasting + multi-query optimization.

**Do NOT position primarily against**:
- RisingWave / Materialize — they're streaming databases, wrong category
- Apama — enterprise IoT vendor, wrong wedge, low search volume
- Esper — abandoned project, low commercial intent

**Keywords that are DEAD and should not be targeted as primary terms**:
- "Complex Event Processing" / "CEP" (peaked 2012, only alive in procurement reports)
- "SASE+" (academic jargon, zero search volume)
- "Kleene closure" (regex niche, near-zero search volume)
- "UEBA" (being absorbed into XDR)
- "Event sourcing" (CQRS concept, wrong category)

---

## Phase 2 — Content marketing (1-2 days of work each)

### 1. `/docs/why-varpulis.md` — canonical positioning page

A ~600-word landing page targeting the informational query "why varpulis" and acting as the single-URL answer to "what is this". Structure:

- H1: "Why Varpulis?"
- Three personas: Security / Fintech / Platform teams
- Explicit "Not another JVM-based streaming framework" paragraph
- Direct comparison bullets vs Flink (no JVM, single binary), vs Proton (pattern detection), vs Esper (actively developed)
- CTA: Quick Start link + Discord

### 2. New comparison page: `/docs/comparisons/varpulis-vs-timeplus-proton.md`

**High priority.** Proton is Varpulis's closest architectural twin (single binary, no JVM) and no one has written this comparison yet. First-mover SEO win.

Key points to make:
- Both: single binary, no JVM, stream processing, Rust/C++
- Proton strength: streaming ClickHouse SQL, analytics
- Varpulis strength: native pattern detection, forecasting, multi-query optimization (Hamlet graphlet sharing), exhaustive Kleene semantics
- When to use each

### 3. New comparison page: `/docs/comparisons/varpulis-vs-arroyo.md`

**Medium priority.** Arroyo was acquired by Cloudflare in 2025; the self-hosted niche is now underserved. Position Varpulis as "the self-hosted Arroyo with pattern detection".

### 4. Dedicated landing page: `/docs/alternative-to-flink.md`

Targets the head keyword "Apache Flink alternative". This already exists as `/docs/comparisons/varpulis-vs-flink.md` but a dedicated `/alternative-to-flink` URL slug ranks better for that exact query. Can be a short page that links to the full comparison.

### 5. Vertical landing pages

- `/docs/use-cases/real-time-fraud-detection.md` — targets a proven evergreen commercial intent keyword. Include code examples, architecture diagram, benchmarks.
- `/docs/use-cases/mitre-attack-detection.md` — targets "MITRE ATT&CK detection" head term. Map VPL patterns to specific techniques (T1021.002, T1055, etc.). Link to the SIEM Evasion Lab series.
- `/docs/use-cases/sigma-rules.md` — targets the growing Sigma community. Explain how Varpulis can execute Sigma detections (even if incomplete — "coming soon" is still linkable).

### 6. Blog: `/docs/blog/`

Start a low-frequency (1 post/month) technical blog. Topics with organic SEO value:

- "Why we built an Apache Flink alternative in Rust" (link-bait for Hacker News)
- "Detection-as-code with VPL: a practical guide"
- "Building MATCH_RECOGNIZE in Rust" (developer long-tail)
- "Hamlet: shared-state trend aggregation explained" (technical deep dive)
- "Forecasting event patterns with Probabilistic Suffix Trees"

---

## Phase 3 — Distribution (community & outbound)

These are one-off tasks with outsized impact. Each takes <1 hour to execute.

### A. Submit to "awesome-*" lists

PRs to these lists ship real referral traffic + backlinks:

- [awesome-rust](https://github.com/rust-unofficial/awesome-rust) — under "Data processing" or "Database" section
- [awesome-stream-processing](https://github.com/manuzhang/awesome-stream-processing)
- [awesome-cep](https://github.com/gzvulon/awesome-cep) (if still maintained — check)
- [awesome-security-engineering](https://github.com/aleksandar-todorovic/awesome-security-engineering)
- [awesome-sigma](https://github.com/SigmaHQ/Awesome-Sigma) — if we ship Sigma rule support
- [awesome-detection-engineering](https://github.com/infosecB/awesome-detection-engineering)

### B. Launch posts (single-shot visibility spikes)

**Hacker News — "Show HN"**
- Title: "Show HN: Varpulis — Rust stream processing engine, Flink alternative (1.5M evt/s)"
- Body: Lead with the 10-line VPL example, the 15MB binary, and "no JVM". Mention the SASE+/forecasting differentiator in the second paragraph. Link to repo + docs.
- Post during US weekday morning (Pacific time, ~9am) for max exposure.

**Reddit**
- r/rust — "I built a stream processing engine in Rust as a Flink alternative"
- r/devops — "Varpulis: single-binary streaming engine for real-time detection"
- r/netsec — focus on the detection engineering / MITRE ATT&CK angle

**Dev.to article**
- "Building an Apache Flink alternative in Rust: lessons from 1.5M events/sec"
- Tag: #rust, #streaming, #opensource, #detectionengineering
- Cross-post to Medium, Hashnode

**LinkedIn**
- Single-post launch with 3-5 bullet diffs vs Flink and a screenshot of the TUI
- Tag: @Apache Software Foundation, @Confluent (not as competitors — as category anchors)

### C. Conference / CFP submissions

- **KubeCon** — Detection engineering track, or Observability day
- **Rust conferences**: RustConf, Rust Nation, EuroRust — "Building a stream processing engine in Rust"
- **Security conferences**: BSides, DEF CON SOC Village, SANS DFIR — detection engineering talk

### D. Podcasts to pitch

- Detection at Scale (SpectralOps)
- Day Two Cloud / The Cyberwire Daily
- Rustacean Station
- Real World Serverless
- Data Engineering Podcast

---

## Phase 4 — Instrumentation & measurement

Before shipping content, wire up measurement so you can tell what's working.

- **Google Search Console**: verify domain ownership, submit the sitemap, monitor which queries actually drive clicks
- **Plausible or Fathom analytics**: privacy-friendly, minimal, actually captures whether visitors convert to GitHub stars / crates.io downloads
- **Referral tracking**: UTM parameters on all launch-post URLs so you know which channel works
- **GitHub star growth chart**: use star-history.com — the inflection point around each campaign is your signal
- **crates.io download trend**: monthly snapshot

---

## Quick reference — the primary tagline

> **Varpulis is a Rust stream processing engine for real-time detection. Open-source Apache Flink alternative built for detection engineering, fraud prevention, and MITRE ATT&CK coverage. 1.5M events/sec. Single 15MB binary. No JVM.**

Every phrase is a real-traffic search term. Use consistently across:
- GitHub repo description
- crates.io descriptions
- README opening
- VitePress meta description
- public-site meta description
- OG / Twitter Card descriptions
- Hacker News post headline
- Dev.to article subtitle

Consistency matters for brand recognition and search-ranking signal.
