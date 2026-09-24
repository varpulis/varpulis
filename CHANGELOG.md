# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed — an absence over many open keys no longer costs them all per event

- In event time, the sequence engine swept every partition's runs on each
  event it received, to confirm absences and drop the runs past their
  deadline. An absence partitioned by a key with many open at once ("orders
  not acknowledged within 4h", one order a second, about 14 000 open) paid for
  all of them on every event: 95 000 events took 38 s. The engine now keeps
  the partitioned runs' deadlines in order and visits only the partitions
  with one due: the same 95 000 events take 0.45 s, with the same alerts, and
  950 000 events with about 144 000 open take under 8 s (`main` did not
  finish them in ten minutes). A partition whose runs are all gone leaves
  the map when its own events empty it, and the index drops the entries of
  runs that completed early each time it doubles, so a long `within` does
  not keep a day of them.
- `SaseEngine::resume_after_restore` rebuilds, after a restore, what a
  checkpoint does not keep: the negated steps' constraints and the order of
  the deadlines.

### Fixed — a cancellation that did not cancel

- `.not(C)` on a stream that reads a named pattern (`stream X = AThenB.not(C)`)
  was parsed, and `C` was routed to the stream, but the pattern was never
  given the cancellation: A, C, B raised the alert as if `.not(C)` were not
  there. It now cancels the pattern's runs as it does an inline sequence's,
  per partition when the pattern has one.

### Fixed — rules that ran and never fired

- An absence (`-> NOT B within X`) completed only when an event reached the
  pattern: its deadline was checked against the pattern's own watermark,
  which nothing else moved. On a live stream that went quiet after the
  trigger ("no acknowledgement within 4h", "no heartbeat within 5 minutes")
  the alert never came, and a pattern that read a derived stream never saw
  the events that stream filtered out. An absence now completes on the clocks
  of the types feeding the pattern, as a window closes: after any batch that
  takes them past its deadline, and with an idle grace on a quiet source too.
  The end of the input confirms none. The completed match is stamped with
  the event time that confirmed it.
- A restored program lost every absence it was waiting out: a snapshot keeps
  the run and the state it is in, not the constraint that entering the
  negated step attached to it, and the run came back without one. The
  forbidden event no longer cancelled it and, at its deadline, it was dropped
  instead of completing. The constraint is rebuilt from the step on restore
  (`SaseEngine::resume_after_restore`).
- A window fed by a source that went quiet never closed: only an event of
  that source moves its event time, so a brute force on a sparse source (VPN
  logons, one application's log) was raised whenever the source spoke again.
  A live host now sets an idle grace (`Program::set_idle_grace`): once a
  source has sent nothing for the grace, its event time moves on with the
  wall clock, less the grace, both at the end of each batch and on
  `Program::tick()`, which the host calls while nothing arrives. Each
  source's clock is kept in the snapshot (`EngineCheckpoint::source_clocks`),
  so the windows a restarted host restores close on time too. Without a
  grace, the default, a program is judged in event time alone, as a replay
  needs.
- A sequence step that names a derived stream (`Burst as b -> ...`) was
  compiled onto that stream's source type, with the stream's first
  `.where()` as the step's predicate. For a stream that only filters that is
  the same thing; for any other it was not. Over an aggregate the step
  matched the raw events instead of the results, so `Burst as b -> Logout`
  never fired when the aggregate had a `.where()` and `b.n` was null when it
  had none; a stream with two `.where()` lost the second; and a stream with
  an `.emit()` gave the step its input instead of what it emits, unlike any
  other stream below it. Only a stream that does nothing but filter, once,
  is still read at its source; any other is read by what it outputs. A first
  step's own filter (`Uploads where host == "a" as u`) now applies on top of
  the stream's filter instead of replacing it, as it already did for later
  steps.
- A time window closed only when a later event reached that same window. A
  window over a filtered stream waited for the next event that passed the
  filter, a partitioned window for the next event of the same partition: a
  brute force counted per address, followed by a successful logon, never
  raised its count on a live stream (`simulate` hid it by closing every
  window at the end of the file). The engine now keeps a clock per input
  event type and, after each batch, closes every window the clocks of the
  types feeding it have passed, whatever stream or partition moved them;
  windows close upstream first, so what one emits reaches the windows below
  before they close (window results closed by a watermark or at the end of
  the input used to reach the output only, never the streams below). With
  `.watermark()` the window closed on the slowest event type of the whole
  program, so one type that went quiet held back every other window; a
  window now waits only for the types that feed it, and `out_of_order` holds
  it that much longer. Partitioned windows keep their deadlines in order, so
  the clock visits only the windows it closes. The synchronous engine
  (`simulate`, Vejas detect units) only; the legacy asynchronous runtime is
  unchanged.
- `.stnm()` changed nothing. ADR-006 and the patterns guide promise that
  under skip-till-next-match an event takes part in at most one match, and
  every event that could open the pattern opened a run under every strategy.
  An event that extends a run now opens no other one under `.stnm()`, so
  `.stnm().longest()` makes one alert of a brute force of any length; the
  shipped rule uses it (one alert where it made one per failure).
- A top-level `let` or `const` is what a stream reads under that name.
  `.where(x > threshold)` read a field `threshold` from the event (stream
  expressions had no program values in scope, and `const` was not loaded at
  all), so a rule with a named threshold compared against nothing, or against
  whatever an event carried under that name. Constants that fold to a literal
  are now substituted at parse time, wherever the program declares them;
  aliases, lambda parameters and a block's own `let` shadow them. A `var`
  stays invisible to streams, as documented.
- A Kleene closure followed by another step (`A -> all B -> C`) matched
  before C. Under the default `.each()` the engine emitted a complete match at
  every B, as if the closure ended the pattern, then dropped the run when C
  arrived. The shipped brute-force rule raised 6 critical "brute force
  succeeded" alerts for 4 failed logins and no success, and none for the
  success; the card-testing example alerted on small purchases before any
  large one. `.each()` now emits once per closure event when C arrives, each
  match holding the closure as it stood at that event; a closure that ends the
  pattern still emits as it grows. The rule itself now uses `.longest()`, one
  alert per failure that could have opened the attack.
- A program that evaluated `arr.filter(x => ...)` or `arr.map(x => ...)`
  (both documented), `a?.b` or a timestamp literal
  aborted the whole process with a stack overflow: the evaluator's fallback
  for a kind of expression it did not list called the evaluator again with the
  same expression. An abort is not a panic, so a host running several units
  (Vejas) went down with all of them. A lambda whose body is a block
  (`x => { let y = x * 2 ... }`) now evaluates too. Every kind is now listed, with no
  fallback, so a new one is a compile error instead.
- A condition on a field the event does not carry is false in `.where()`, as
  it always was in a `->` sequence step. `a == "x" or ends_with(b, "y")` used
  to fire only on events that had a `b`, `selection and not filter` dropped
  every event without the filter's field, and the same predicate answered
  differently in the two places. `and` and `or` now stop at the first operand
  that decides.
- A field the event does not carry, passed to a function, is `null` in its
  own position. It was dropped from the argument list, so `is_null(b)` never
  held for a missing `b` and a user function received its later arguments
  shifted one place left.
- `varpulis simulate` reads a JSON line the way a live source does: `EventID`
  and `Channel` stay fields of the event (they were dropped, so a rule testing
  `EventID == 4625` fired on the bus and never in a simulation), and an
  explicit `"type"` wins over the type guessed from `EventID`.
- `varpulis check` runs the semantic validator again, as the documentation
  said it did. Since the CLI was rewritten on the engine crate it parsed and
  loaded only, so an unbounded closure (W003), a misspelled event type (E033)
  or a regular expression that cannot compile (E052) all checked "ok".
  `Program::check` is the same check for a host (Vejas's `vpl-check`), and
  `Program::check_with_warnings` returns the warnings. A program that declares
  no event type is open, as the engine is: E033 applies once one is declared.
- `coalesce`, `unique` and `clamp` were documented as implemented and were
  not; they are now. `now()` was documented and never existed; the table no
  longer lists it (a rule runs on the time its events carry).
- `varpulis check` reports a function it does not know wherever a stream
  evaluates one (**E050**): `.where()`, `.emit()`, `.having()`, a `->` step's
  filter. It only looked at `let` values, so `ends_wiht(Image, ...)` checked
  "ok" and never matched. Its list of built-ins is now the evaluator's, kept
  so by a test in both directions: it listed `to_lower`, `concat`, `now`,
  which the engine never evaluated, and did not know `lower`, `is_null`,
  `substring` or `regex_match`. Three shipped examples called `now()` and
  `str()`, which never existed; they now use the event's own timestamp and
  `to_string()`.
- The builtins table no longer lists `flatten` and `arr.min()`/`arr.max()`
  as implemented; outside `.pattern()` lambdas they answered nothing.

### Added

- Single-quoted strings are raw, as in Sigma's YAML: a backslash is only a
  backslash, even last (`'\AppData\Local\Temp\'`, which a double-quoted
  string cannot end with), and `''` is one quote. The type documentation had
  listed `'world'` as a string all along; the parser refused it.
- A field whose name is not an identifier is written between backticks,
  wherever an expression reads a field: `` `cs-uri-query` ``, `` p.`sc-status` ``.
  Web and proxy logs name their fields that way (W3C extended format), and a
  rule over them could not be written before.
- `regex_match(s, pattern)` / `s.regex_match(pattern)`, with Rust `regex`
  syntax: linear time, no look-around or back-references, each pattern
  compiled once per thread. `varpulis check` reports a literal pattern that
  does not compile as **E052** instead of loading a rule that never fires.

### Removed — the platform (ADR-008)

Varpulis is its engine. Retired in favour of [Vejas](https://github.com/cpoder/vejas),
where a VPL program is a `detect` unit with a durable consumer, snapshots and
replay: `varpulis-cli` (server, SaaS, auth, billing, deploy, coordinator,
federation), `varpulis-cluster`, `varpulis-mcp`, the thirteen connector
implementations, `varpulis-db`, `varpulis-datagen`, `varpulis-actors`, the
`varpulis` meta-crate, `deploy/`, the Docker-bound test suites, `demos/`,
`integrations/`, `starters/`, the stream builder and public site sources, the
SaaS and cluster scripts, the Homebrew formula, the container images, nine
broker-bound CI jobs and the platform's documentation. From
`varpulis-runtime`: its dependency on `varpulis-datagen`, sixteen features
that only enabled retired crates, the interactive session module, the Kafka
sink adapters. Kept with the runtime's `async-runtime` feature until that
goes: `varpulis-connector-api`, `varpulis-connectors` (HTTP only),
`varpulis-connector-http`, `varpulis-enrichment`.

### Added

- `varpulis-cli` is new and small: `varpulis check`, `parse`, `simulate -p
  program.vpl -e events.evt` on the engine crate. No `run`.
- `Program::snapshot` / `Program::restore` on `varpulis-engine` (#266): the
  engine's state as bytes the host keeps, with the deadlines of open
  sequences; a fresh program restored from them closes them.
- `varpulis-engine` (#264, #265): the engine as a library with no async
  runtime, decoding payloads with the connectors' own decoder, now in
  `varpulis-core`.


### Added

- **`varpulis-engine`: the CEP engine as an embeddable library with no async
  runtime.** Compile a VPL program, feed it events, get its emits with the
  `.to()` binding each one was routed to, and publish them on your own bus.
  `varpulis-runtime` is taken with its default features off, so
  `cargo tree -p varpulis-engine` names no tokio, broker client or server
  stack — 76 packages instead of 947 — and a new fast-gate job, *Engine Stays
  Runtime-Free* (`scripts/check-engine-deps.py`), fails the build if one ever
  appears. Time is event time: a payload's `timestamp` is the event's time, so
  a replay reproduces the same emits. This is the seam Vejas ADR-0031 embeds.
- `Engine::sink_bindings()` and `Engine::take_collected_outputs()` on every
  build of `varpulis-runtime`: the `.to()` declarations of a loaded program,
  and the outputs an end-of-input flush leaves behind, which an embedding host
  could not reach before.

### Changed

- **The coordinator now refuses to start on a control-plane bucket with fewer
  than three replicas.** It was a warning on stderr. The bucket is the only
  copy of the cluster's control state — the worker registry, the pipeline
  placements, the leader lease — so a single broker is a point of failure the
  Raft group it replaced was not, and a warning about that is a note filed
  where nobody reads it.

  The count is read back *from the broker*, not taken from
  `VARPULIS_CONTROL_PLANE_REPLICAS`: binding to a pre-existing bucket keeps
  its own configuration and ignores what you asked for, so a coordinator could
  request three and run on one with nothing saying so. A bucket status that
  cannot be read is treated as undurable rather than waved through.

  **Operators must act.** Point `VARPULIS_CONTROL_PLANE_URL` at a NATS cluster
  of at least three nodes and set `VARPULIS_CONTROL_PLANE_REPLICAS=3`; the URL
  now accepts the whole cluster comma-separated, so a coordinator can start
  while one node is down. An existing under-replicated bucket needs `nats kv
  edit --replicas=3`. For a laptop or a CI job,
  `VARPULIS_CONTROL_PLANE_ALLOW_SINGLE_REPLICA=1` permits one replica and says
  so on every startup.

  `deploy/demo/docker-compose.yml` now runs a three-node JetStream cluster
  instead of one broker.

### Removed

- **Raft.** Coordinator consensus is the JetStream KV control plane:
  leadership is a lease on one key, and every replicated write is a
  compare-and-swap against that store. The `raft` and `persistent` Cargo
  features, the `varpulis-cluster::raft` module, the `--raft*` CLI flags and
  the openraft dependency are gone — about 4,600 lines, and with them the
  yanked `validit` that `cargo audit` reported on every run.

  **Operators must act.** A coordinator started with `--raft` now refuses to
  start with an error naming what to set instead; it does not silently fall
  back to standalone. Set `VARPULIS_CONTROL_PLANE_URL` to a NATS cluster and
  `VARPULIS_CONTROL_PLANE_REPLICAS=3`, and give each coordinator a stable
  distinct `--coordinator-id` (defaults to `$HOSTNAME:$port`) and a reachable
  `VARPULIS_COORDINATOR_ADVERTISE_ADDR`. See `docs/operations/runbook.md` §1.3.

  Three Prometheus gauges go with it — `varpulis_cluster_raft_role`, `_term`,
  `_commit_index` — replaced by `varpulis_cluster_is_leader` and
  `varpulis_cluster_leadership_changes_total`. Alert rules built on the old
  names must be rewritten; `deploy/prometheus/alerts.yml` and
  `docs/operations/alerting.md` carry the replacements. `GET
  /api/v1/cluster/raft` is deprecated, now requires the Viewer role, and
  returns the new `/api/v1/cluster/consensus` shape rather than zeros under
  field names that described a log which no longer exists.

  The failure bound changes character rather than magnitude: a coordinator
  killed outright cannot resign, so its lease ages out on
  `VARPULIS_CONTROL_PLANE_TTL_SECS` (default 30s) before a standby acquires —
  where Raft used an election timeout. A clean shutdown resigns and hands over
  at once. And the single point of failure moves: the KV bucket is now the
  only copy of the control state, which on a single broker is a weakness a
  three-node Raft group did not have.

### Fixed

- **The NATS heartbeat path never replicated on a JetStream deployment.**
  `replicate_heartbeat` was un-gated from `raft` when every replicated write
  moved behind `Coordinator::replicate`, but its call site in
  `nats_coordinator::handle_heartbeat_message` was not. A worker homed on a
  non-leader coordinator therefore had its liveness invisible to every other
  coordinator, which could then false-mark it `Unhealthy` — audit finding C5,
  still open on that path. It went unnoticed because the only test asserting
  it was itself gated on `raft`, and no job built that test with the feature.

### Changed

- **`RunCheckpoint` is now `#[non_exhaustive]`.** It gains two fields
  (`deadline_ms`, `started_at_ms`) so a restored partial match keeps its
  `.within()` bound instead of becoming immortal. Both are `#[serde(default)]`,
  so checkpoints written by earlier versions still load. Adding public fields to
  a struct that external code could construct exhaustively is a breaking change,
  which the semver job correctly reports; sealing the struct takes that break
  once, on a 0.x release, rather than on every future field. Nothing outside
  `varpulis-runtime` constructs it.

### Fixed

- **`.within()` is now enforced against event time, not wall-clock arrival
  time.** The SASE engine defaulted to processing-time semantics and the VPL
  compiler never selected the event-time branch, so a run's WITHIN deadline was
  `now() + timeout` and expiry was decided by `now()`. Replaying a log therefore
  satisfied every temporal bound trivially — the whole file arrives inside a
  millisecond — so `.within(1h)` matched a two-hour gap, and a slow live reader
  could expire a window that no event had actually outrun. Every shipped
  detection rule was affected. Set `VARPULIS_SASE_TIME=processing` to restore
  the old wall-clock behaviour.
- **A WITHIN bound is no longer widened by `.watermark(out_of_order: D)`.** Run
  expiry is watermark-driven (and the watermark deliberately lags by `D`), but
  whether a given event may *complete* a run is now decided by that event's own
  timestamp against the run's deadline. Previously the lag turned into extra
  WITHIN budget.
- **A WITHIN bound on a pattern that starts through an AND branch or an epsilon
  transition is no longer dropped.** Only one of the four run-creation paths in
  `try_start_run_shared` applied the state's timeout; all four now do.
- **A restored checkpoint keeps its processing-time WITHIN deadline.** Restore
  hard-coded `deadline: None` and reset `started_at` to `now()`, so a restart
  erased the bound and made every restored partial match immortal. Both are
  wall-clock instants and are now persisted and restored verbatim
  (`RunCheckpoint.deadline_ms` / `.started_at_ms`; older checkpoints without
  the fields restore as before).
- **`BATCH 0` in an event file now stamps its events at offset zero** instead of
  reading as "no timing given". A JSONL line under `BATCH 0` kept its
  `Utc::now()` timestamp while later batches were stamped from the epoch, which
  put a file's first event decades after its last.

## [0.11.0] - 2026-07-17

Outcome of a full 11-scope codebase audit plus the distributed exactly-once
("Flink-parity") work. This release closes every audit critical and the
high/medium-severity findings — silent event drops, unbounded-memory growth,
cleartext credentials, and cluster control-plane gaps — and adds end-to-end
exactly-once checkpointing. (Documents everything since v0.10.0, which shipped
without a changelog entry.) No VPL language changes; a few connector/sink
behaviours changed — see **Changed**.

### Added

- **Distributed exactly-once checkpointing** — a two-phase-commit checkpoint
  barrier in the engine, a coordinator orchestrator, worker-side barrier handler,
  Raft integration, and a NATS checkpoint protocol, with checkpoint-based pipeline
  migration and recovery-commit of restored 2PC state.
- **Postgres CDC over TLS** — the CDC connector now honours `sslmode`
  (`require`/`verify-ca`/`verify-full`) via rustls, with CA pinning
  (`ssl_ca_location`) and optional mTLS (`ssl_certificate_location` /
  `ssl_key_location`).
- **Dynamic topic routing on Redis** — `.to(expr)` publishes to the evaluated
  channel (pub/sub) or stream key (streams), matching Kafka/MQTT/NATS.
- **Source auto-reconnect** — MQTT, Redis pub/sub, and Pulsar sources reconnect
  and re-subscribe after the broker drops the connection instead of going
  silently deaf.
- **Kafka dead-letter for undecodable records** — malformed source records are
  counted, logged, and (when a DLQ is configured) dead-lettered instead of being
  silently skipped.

### Changed

- **Dynamic `.to(topic)` on a sink that cannot route by topic now returns an
  explicit error** instead of silently delivering to the connector's fixed
  destination — the caller's routing intent is no longer dropped without a
  signal. Sinks that can route by topic (Kafka/MQTT/NATS/Redis/syslog/slack) are
  unaffected.
- **SequenceMatch build is now lazy** — per-alias `_events`/aggregate structures
  are materialised only when a downstream operator references them.
- Toolchain moved to Rust 1.95; `wasm32-unknown-unknown` target added.

### Fixed

- **Exactly-once correctness** — checkpoint windowed-aggregate state (C1); drain
  in-flight events before the snapshot (C3); commit source offsets inside the sink
  transaction (C4); checkpoint-barrier failures are now fatal rather than
  logged-and-ignored.
- **Event-time watermarks** — observed on all four dispatch paths (C2a); time
  windows close on watermark advance, not on event arrival (C2b); empty partition
  windows are evicted on advance.
- **Cluster control plane** — dead workers are detected via a replicated heartbeat
  sequence (C5); deploy/teardown/inject/migrate and auto-failover migration are
  routed over NATS when configured (C6); `/api/v1/cluster/raft` now requires auth.
- **Bounded memory** — `TrendAggregate` accumulation is bounded to the WITHIN
  window; SASE evicts empty partitions during cleanup; idle partition windows are
  evicted; event-file reads are capped; `S3StateStore` no longer panics on
  `block_on` inside the runtime; orphaned `.tmp` files are swept on
  `FileStore::open`.
- **Connectors** — `HttpSink` returns `Err` on a non-2xx response instead of
  silently dropping events; hot reload preserves Sequence/Join routing.
- **Parser/evaluator safety** — checked integer arithmetic, saturating timestamp
  parsing, order-independent map `Hash`/`Eq`; run-grouped dispatch preserves
  cross-stream FIFO interleave.
- **Kafka hot path** — allocation-lean rewrite: 88.5k → 136k+ eps on the Arroyo
  filter benchmark.

### Security

- **CDC replication slot name** is validated to close a SQL-injection hole.
- **CDC replication is no longer cleartext** — `sslmode` is honoured (was silently
  ignored under a hardcoded `NoTls`).
- **Credentials are redacted from `Debug`** across Kafka, Redis, HTTP, Pulsar, the
  generic `ConnectorConfig`, and `ConnectorProfile` — passwords, tokens, and API
  keys no longer leak into logs, error chains, or panic messages.
- **Internal API-key checks are constant-time**; the standalone server caps
  concurrent WebSocket connections; the public pipeline-graph endpoints bound VPL
  parse cost so they cannot be used for CPU-exhaustion DoS.

## [0.9.0] - 2026-03-26

### Added

- **`varpulis interactive --json`** — Agent-friendly JSON-line protocol on
  stdin/stdout for driving streaming sessions programmatically. Commands:
  `load_vpl`, `inject`, `generate`, `subscribe`, `get_topology`, `set_trace`,
  and more. Responses stream as one JSON per line.
- **`varpulis interactive` (TUI mode)** — Split-pane terminal UI with ratatui:
  topology graph (top-left), scrolling event stream (top-right), VPL input
  (bottom-left), live metrics dashboard (bottom-right). Behind `tui` feature.
  Key bindings: Tab to switch panes, Ctrl+G toggle datagen, Ctrl+T toggle trace.
- **Interactive session core** (`varpulis-runtime::interactive`) — Protocol-first
  architecture with `InteractiveSession`, `SessionCommand`, `SessionResponse`.
  Supports engine hot-reload, embedded datagen, trace integration, subscription
  filtering, and pipeline topology.
- **MCP interactive tools** — 3 new Model Context Protocol tools:
  `start_interactive_session`, `send_interactive_command`, `get_interactive_events`.
  AI agents can create sessions, send commands, and retrieve streaming events.
  Sessions auto-expire after 10 minutes idle.

## [0.8.0] - 2026-03-26

### Added

- **`varpulis infer`** — Infer VPL `event` type declarations from .evt or JSONL
  sample data. Supports type promotion (int+float→float), flat and nested JSONL,
  configurable sample size.
- **`varpulis connector list/info/test`** — Discover and inspect available
  connectors with formatted tables, config parameters, and example VPL snippets.
- **`varpulis simulate --watch`** — File watcher re-runs simulation on .vpl/.evt
  changes with 300ms debounce. Parse errors shown without stopping the watcher.
- **`varpulis repl`** — Interactive VPL shell with `:load`, `:event`, `:events`,
  `:reset`, `:streams` commands. Maintains engine state between events. History
  persisted to `~/.varpulis_history`. Behind `repl` feature flag.
- **`varpulis simulate --trace`** — Pipeline explain mode showing per-event flow:
  stream matching, operator pass/block, pattern state, emitted events. Colored
  output with PASS (green) / BLOCK (red) indicators.
- **`.alert()` operator** — Side-effect operator for webhook notifications with
  `{field}` template interpolation. Fire-and-forget via `tokio::spawn`. Events
  continue downstream (not consumed). LSP completion and hover docs included.
- **Pipeline graph API** — `POST /api/v1/pipeline/graph` (VPL→JSON graph) and
  `POST /api/v1/pipeline/generate` (graph→VPL) for visual pipeline builders.
  Both standalone and cluster APIs.
- **6 new Prometheus metrics** — `operator_latency`, `pattern_matches_total`,
  `window_fill_level`, `connector_health`, `connector_events_sent`,
  `backpressure_drops`.
- **2 new Grafana dashboards** — Pipeline Detail (8 panels: latency percentiles,
  operator heatmap, pattern matches, window fill, backpressure) and Connector
  Health (3 panels: status, events sent, active streams).
- **Prometheus alerting rules** — High latency, connector unhealthy, DLQ growing,
  backpressure drops.

### Changed

- **CLI output overhaul** — Progress bars (indicatif) for simulate/run, formatted
  tables (comfy-table) for `pipelines` and `status`, colored output (owo-colors)
  for success/error/warning messages. Respects `NO_COLOR` env var.

## [0.7.1] - 2026-03-26

### Security

- **CLI API CORS hardened** — default changed from `allow_origin(Any)` to
  localhost-only (`localhost:5173`, `localhost:8080`, `127.0.0.1` variants),
  matching the cluster API's safe default. Explicit `"*"` now logs a warning.
- **SMTP TLS safety** — `builder_dangerous()` now logs `tracing::warn!` and
  supports `VARPULIS_SMTP_DANGEROUS` env var for explicit opt-in beyond the
  existing port-1025 (MailPit) guard.

### Added

- **130 connector unit tests** — added tests to 10 previously untested connector
  crates (API, Kafka, Redis, HTTP, NATS, Database, Elasticsearch, S3, Kinesis,
  Pulsar) covering config construction, error handling, and serialization.
- **8 enrichment tests** — cache insert/get, TTL expiry, stats, provider factory,
  error display.
- **5 ZDD tests** — dump truncation, to_dot multi-node, iterator debug format,
  chain iteration with 4 and 6 variables.
- **WASM CI target** — `cargo check -p varpulis-wasm --target wasm32-unknown-unknown`
  added to CI pipeline to prevent WASM compatibility regressions.
- **Nightly fuzz testing** — re-enabled cron schedule for parser, JSON, and MQTT
  fuzz targets (3 x 30 min nightly).

### Changed

- **Refactored `compile_ops_with_sequences()`** — extracted `compile_hamlet_mode`,
  `compile_sase_detection`, and `compile_pst_forecaster` into separate methods,
  reducing the function from 1,157 to ~713 lines with zero behavior changes.
- **CORS headers expanded** — CLI API now includes `x-request-id` and `traceparent`
  in allowed headers, matching the cluster API for observability consistency.

## [0.6.0] - 2026-03-08

### Highlights

Full **multi-tenant SaaS platform** with hierarchical organizations, per-tenant
isolation (PostgreSQL schemas, Kubernetes namespaces, Kafka topic prefixes), and
an onboarding wizard. The **playground** switches to native `.evt` format for a
better user experience, and the **landing page** is polished for public visitors.

### Added

#### Multi-Tenant SaaS (7-Phase Buildout)
- **Tenant hierarchy** — parent/child organizations with tree-based navigation
- **Per-tenant PostgreSQL schemas** — automatic schema provisioning and RLS isolation
- **Hierarchical RBAC** — parent tenant admins inherit access to child organizations
- **Pipeline inheritance engine** — global pipelines with per-tenant overrides and DB sync
- **Kubernetes namespace provisioning** — per-tenant namespace with resource quotas via Capsule
- **Kafka topic isolation** — per-tenant topic prefix enforcement at runtime
- **UI hierarchy support** — organization tree, pipeline badges, breadcrumbs
- **Onboarding wizard** — guided tenant setup with usage dashboard
- **API key management** — enhanced key generation and lifecycle management
- **Tenant schema middleware** — automatic schema switching per request

#### Playground Improvements
- **Native `.evt` format** — events displayed and edited in Varpulis's native event
  file format instead of JSON, with `@<time> EventType { field: value }` syntax
- **8 built-in examples** — all converted to `.evt` format with correct VPL syntax
- **EventFileParser integration** — backend uses `EventFileParser::parse()` for events

#### Landing Page & Navigation
- **Polished landing page** — own app bar with nav links, feature grid with
  Multi-Tenant SaaS card, footer with product links
- **Full-screen page routing** — landing, login, signup, playground render without
  app chrome (nav drawer, breadcrumbs)
- **Auth redirect** — unauthenticated visitors land on `/landing` instead of login

#### Infrastructure
- **Worker advertise address** — `POD_IP` and `VARPULIS_ADVERTISE_ADDRESS` env vars
  in k3d-saas worker overlay
- **Admin bootstrapping** — `--admin-password` flag for deterministic admin setup
- **Multi-tenancy architecture docs** — SVG diagrams replacing ASCII art

### Fixed

- **Playground IoT anomaly producing 0 matches** — event fields were silently
  dropped due to `#[serde(default)]` instead of `#[serde(flatten)]`; fully resolved
  by switching to `.evt` format
- **VPL examples using `&&` instead of `and`** — fraud-detection and cyber-killchain
  examples now use correct VPL logical operators
- **Parser exponential backtracking** — 10s timeout guard for malicious inputs
- **Parser bracket bomb** — reject inputs with too many unmatched open brackets
- **Pipeline visibility queries** — correct tenant scoping in pipeline list API
- **Redis connector API** — updated for redis crate 1.x breaking changes
- **GRETA Kleene propagation** — correct coefficient computation in multi-query sharing
- **Web UI auth flow** — redirect to login page instead of API key popup
- **k3d-saas admin login** — fix service routing for admin bootstrapping
- **Nightly `rustfmt` import ordering** — stable across CI environments
- **`partition_by` missing field** — added to `SlowTransactionStep` for correct partitioning
- **License audit** — allow 0BSD license for `quoted_printable` dependency
- **SVG rendering** — fix broken diagrams in authentication docs

## [0.5.0] - 2026-03-02

### Highlights

Major architecture improvements: SmartModule WASM runtime, standalone crate extraction,
VPL test DSL, comprehensive datagen tests, Raft simulation, and hardened CI across
all platforms and feature flags. All 19 crates published to crates.io.

### Added

#### SmartModule WASM Runtime
- **SmartModule host runtime** — user-defined WASM processing via `wasmtime`
- Feature-gated: `--features smartmodule`

#### Crate Extraction
- **varpulis-pst** — PST forecasting as standalone crate
- **varpulis-hamlet** — Hamlet trend aggregation as standalone crate
- **varpulis-enrichment** — Event enrichment as standalone crate
- **varpulis-simd** — SIMD acceleration as standalone crate
- **varpulis-dead-letter** — Dead letter queue as standalone crate

#### Testing Infrastructure
- **VPL-driven test DSL** — `.vpl.test` fixture files with auto-discovery
- **33 tests for varpulis-datagen** — comprehensive data generator coverage with serde roundtrip
- **Raft simulation tests** — distributed consensus testing
- **JSON Schema generation** — schema export for configuration validation
- **Cross-platform CI** — Windows and macOS test targets

#### Engine Improvements
- **EngineBuilder** — fluent API for engine construction
- **ConnectorHealth** — health monitoring for connectors
- **Debug impls** — added `Debug` to all public types (`missing_debug_implementations`)
- **Per-crate error hierarchy** — structured error types across all crates
- **Workspace lints** — 17 additional centralized clippy checks
- **cargo-semver-checks** — CI job for API compatibility validation
- **Removed backward-compat shim** — `From<String> for EngineError` removed

#### Architecture Improvements (Phases 1–4)
- **Physical query plans** — wired `PhysicalPlan` into `Engine::load_program`
- **Restructured test layout** — SASE and engine tests moved from `src/` to `tests/`
- **Performance section** — README restructured with per-layer benchmarks

### Fixed

- **Multiply overflow in timing parser** — fuzz-discovered panic when parsing extreme timing values (e.g., `@999999999999999999s`), now returns error via `checked_mul`
- **Proptest f64 range** — constrain test values to ±1e300 to avoid sum/avg overflow near `f64::MAX`
- **Instant subtraction panic** — Windows-specific panic in migration cleanup test
- **Intra-doc link** — broken link for feature-gated smartmodule module
- **Feature-gated CI failures** — fixes across nats, pulsar, cdc, federation, persistent, encryption
- **Clippy warnings** — raft feature-gated code, `ignored_unit_patterns`, `len_zero`
- **Simulate default mode** — `simulate` defaults to fast mode, `.evt` timestamp parsing fixed
- **cargo-deny and kafka** — CI failures in dependency auditing and kafka feature tests
- **GenericArray deprecation** — replaced deprecated `from_slice` with array conversion in persistence
- **Audit issues** — resolved issues #47–#57

### Infrastructure

- **crates.io publish workflow** — all 19 crates in correct topological order with retry logic and idempotent "already exists" handling

## [0.4.1] - 2026-02-27

### Highlights

Phase 3 & 4 of the cloud SaaS buildout: full authentication, database persistence,
billing integration, and distribution infrastructure for Homebrew, GitHub Actions,
and crates.io publishing. First release published to crates.io.

### Added

#### Authentication & Authorization
- **GitHub OAuth login** — browser-based login flow with PKCE
- **JWT session tokens** — stateless auth with configurable expiry
- **Auth middleware** — Warp filter for protected API routes
- **Auth store** — in-memory session management with token refresh

#### PostgreSQL Database Layer
- **User management** — create, lookup, GitHub ID linking
- **Organization support** — multi-tenant org membership with roles
- **API key management** — scoped keys with usage tracking
- **Pipeline storage** — persistent VPL pipeline CRUD
- **Usage metering** — per-org event counts and storage tracking
- **SQL migrations** — versioned schema with sqlx-migrate

#### Stripe Billing Integration
- **Subscription tiers** — Free, Pro, Enterprise with configurable limits
- **Usage-based billing** — metered event processing charges
- **Checkout sessions** — Stripe-hosted payment flow
- **Customer portal** — self-service subscription management
- **Webhook handling** — subscription lifecycle events

#### Playground & Landing Page
- **Ephemeral sessions** — sandboxed VPL execution with timeout
- **Example library** — pre-built pipelines for quick exploration
- **Landing page** — product overview with feature highlights
- **Billing view** — subscription status and usage dashboard
- **Login view** — OAuth flow with redirect handling

#### Event Generator Library (`varpulis-datagen`)
- **Fraud detection schema** — transactions, logins, device fingerprints
- **IoT monitoring schema** — sensor readings, alerts, device status
- **Trading schema** — orders, fills, market data
- **Configurable rates** — events/sec, burst patterns, seasonal variation

#### Docker Demos
- **Fraud detection demo** — end-to-end pipeline with MQTT and generated events
- **IoT monitoring demo** — sensor alerting with threshold patterns

#### WASM Parser
- **`varpulis-wasm` crate** — browser-compatible VPL parser via wasm-bindgen
- **Playground integration** — client-side syntax validation

#### Distribution
- **Homebrew formula** — `brew install varpulis/tap/varpulis` for macOS and Linux
- **GitHub Actions marketplace action** — `varpulis-check` for CI/CD VPL validation
- **crates.io publish workflow** — automated sequential crate publishing

#### Audit Logging
- **Structured audit log** — JSON-lines format with actor, action, target, outcome
- **In-memory recent buffer** — fast access to last 1000 entries
- **REST endpoint** — `GET /api/v1/audit` with filtering by action and actor
- **Auto-enabled** — writes to `data/audit.jsonl`, no configuration needed

#### Percentile Aggregations
- **`median(expr)`** — 50th percentile aggregation function
- **`percentile(expr, q)`** — generic percentile with configurable quantile (0.0–1.0)
- **`p50(expr)` / `p95(expr)` / `p99(expr)`** — convenience aliases for common percentiles
- Sort-based algorithm with linear interpolation for correctness on bounded windows

#### Outer Joins
- **`left_join(...)`** — emit when left source has an event, fill nulls for missing right
- **`right_join(...)`** — emit when right source has an event, fill nulls for missing left
- **`full_join(...)`** — emit for either side, fill nulls for missing sources
- `JoinType` enum with `Inner`, `Left`, `Right`, `Full` variants in AST

#### Encryption at Rest
- **`EncryptedStateStore<S>`** — transparent AES-256-GCM encryption wrapper for any `StateStore`
- Random 96-bit nonce per value, key from hex env var or Argon2id passphrase derivation
- Feature-gated: `--features encryption` (requires `aes-gcm`, `argon2`, `hex`)

#### SSO/OIDC
- **`AuthProvider` trait** — pluggable identity provider abstraction
- **`OidcProvider`** — generic OIDC provider with `.well-known/openid-configuration` discovery
- Supports Okta, Auth0, Azure AD, Keycloak, Google Workspace
- Feature-gated: `--features oidc` (requires `openidconnect` crate)
- `GitHubOAuth` refactored to implement `AuthProvider`

#### PostgreSQL CDC Connector
- **`PostgresCdcSource`** — change data capture via PostgreSQL logical replication
- Converts INSERT/UPDATE/DELETE WAL changes to typed Varpulis events
- Event format: `{table}.{INSERT|UPDATE|DELETE}` with column values as fields
- LSN tracking for replay positioning
- Feature-gated: `--features cdc` (requires `tokio-postgres`)

#### Advanced Connectors
- **Redis connector** — pub/sub source and sink with key prefix support
- **Pulsar connector** — Apache Pulsar source and sink
- **Federation routing** — cross-cluster event routing for geo-distributed deployments

#### SaaS Deployment
- **docker-compose.saas.yml** — complete SaaS stack (PostgreSQL, Caddy, Web UI, Prometheus, Grafana)
- **Caddyfile.saas** — reverse proxy with OAuth/API/WebSocket routing
- **Environment configuration** — `.env.example` with all required variables documented

#### Validation & LSP
- **Strict semantic validation** — connector params, stream references, type checking
- **Per-op diagnostic spans** — precise error locations for each VPL operator
- **Merge/log/print support** — LSP completions and hover for new operators
- **Unknown op error reporting** — actionable diagnostics for typos in operator names

## [0.4.0] - 2026-02-23

### Highlights

Varpulis 0.4.0 completes the production readiness audit (18/18 tasks) and rewrites
the README for clarity. All P0–P3 issues from the audit are resolved.

### Added

- **Dead Letter Queue API** — REST endpoints for DLQ inspection, replay, and purge
- **OpenTelemetry tracing** — distributed trace export via `otel` feature flag
- **Backpressure signaling** — HTTP 429 + Retry-After headers under queue pressure
- **Capacity planning guide** — sizing recommendations for CPU, memory, and storage
- **TLS documentation** — mTLS setup guide for NATS and cluster transport
- **Grafana overview dashboard** — pre-built panels for cluster health and throughput
- **Fuzzing infrastructure** — cargo-fuzz targets for parser and connectors
- **OpenAPI specification** — machine-readable API docs for 40+ endpoints
- **API pagination** — cursor-based pagination on all list endpoints
- **Coverage enforcement** — 70% minimum threshold in CI
- **CONTRIBUTING.md** — contributor guidelines and development setup
- **SECURITY.md** — responsible disclosure policy
- **Prometheus alerting rules** — 8 alert groups for production monitoring
- **Operational runbook** — incident response procedures
- **Checkpoint schema versioning** — forward-compatible state snapshots
- **Property-based testing** — proptest for parser and value types
- **Chaos test quarantine** — flaky test isolation system
- **Architecture Decision Records** — 5 ADRs documenting key design choices
- **Performance regression CI** — 10% threshold gate on benchmarks
- **Binary serialization** — MessagePack option for checkpoint/wire format
- **SLO/SLI definitions** — 9 SLOs with PromQL queries

### Changed

- Comprehensive dead code removal across workspace
- Queue pressure ratio metric for backpressure decisions
- README rewritten: removed adversarial competitor comparisons, standalone performance framing

### Fixed

- Parser backtracking on malformed `within` clauses
- SVG rendering in documentation (bidirectional arrows, split box visibility)
- STATUS.md accuracy (metrics aligned with actual codebase counts)
- SQL table name sanitization for database connector

## [0.3.0] - 2026-02-12

### Highlights

Varpulis 0.3.0 is a major feature release introducing **PST-based pattern forecasting**,
**ONNX model inference**, **NATS transport**, **MCP server for AI-assisted development**,
and extensive **security hardening**. The engine moves from HTTP/WebSocket to NATS for
cluster communication and adds Raft-based high availability.

### Added

#### PST Pattern Forecasting
- **`.forecast()` operator** — predict future pattern completions using Prediction Suffix Trees
- **Pattern Markov Chain** — online-trained variable-order Markov model from SASE NFA structure
- **Built-in variables** — `forecast_probability`, `forecast_time`, `forecast_state`, `forecast_context_depth`
- **Configurable parameters** — confidence threshold, prediction horizon, warmup period, max tree depth
- **Sub-microsecond prediction** — 51 ns single-symbol, 105 ns full distribution

#### ONNX Model Inference
- **`.score()` operator** — run ONNX models inline in VPL pipelines
- **ort runtime integration** — CPU inference with configurable thread count

#### NATS Transport
- **NATS connector** — publish/subscribe event transport (In/Out)
- **NATS cluster transport** — replaces HTTP/WebSocket for coordinator-worker communication
- **JetStream support** — durable subscriptions with at-least-once delivery

#### MCP Server
- **Model Context Protocol** — AI-assisted VPL pipeline development
- **Tools, resources, prompts** — structured API for LLM-driven pipeline authoring

#### HA Cluster Hardening
- **Leader forwarding** — workers forward writes to current Raft leader
- **Stale reconciliation** — automatic state sync on leader change
- **K8s Lease election** — high-availability leader election for Kubernetes deployments

#### Security Hardening
- **mTLS** — mutual TLS for NATS and cluster transport
- **RBAC** — Admin/Operator/Viewer roles with multi-key file support
- **Resource limits** — 1024 fields, 256 KB strings, depth 32 per event
- **Secrets zeroization** — API keys and credentials cleared from memory on drop
- **Rate limiting** — token bucket per-IP with configurable burst and bounded tracking

#### Resilience
- **Circuit breaker** — Open/HalfOpen/Closed state machine for connector failures
- **Dead letter queue** — failed events captured for inspection and replay
- **Exactly-once Kafka delivery** — transactional producer with idempotent writes

#### Additional Features
- **External connector enrichment joins** — enrich events from database/API lookups
- **Hawkes process** — self-exciting point process for burst detection
- **Conformal prediction** — distribution-free prediction intervals
- **LSP go-to-definition and find-references** — code navigation in VS Code
- **Web UI forecast visualization** — real-time forecast probability charts
- **Web UI monitoring dashboard** — cluster health and per-pipeline metrics

### Changed

- SASE+ engine throughput improved 15–40% (run management, match extraction)
- Pipeline allocation reduced 10–25% (fewer intermediate allocations)
- Kafka batch delivery throughput improved 10x+ (batched `FutureProducer`)
- Cluster transport migrated from HTTP/WebSocket to NATS
- Raft consensus upgraded to openraft 0.9

### Fixed

- Forecast op ordering — inserted at correct VPL position instead of end of ops list
- NFA transition mapping — use next state's event type for symbol labels
- Early exit bypass — skip Sequence early exit when Forecast op follows
- Parser backtracking on edge cases found by fuzzing

## [0.2.0] - 2026-02-10

### Highlights

Varpulis 0.2.0 is a major feature release introducing **distributed cluster mode**,
a **full web UI**, the **Hamlet multi-query aggregation engine**, and extensive
runtime performance optimizations. A live public demo is available at
[demo.varpulis-cep.com](https://demo.varpulis-cep.com).

### Added

#### Cluster Mode & Distributed Execution
- **Coordinator + Workers architecture** — deploy pipeline groups across multiple
  workers with automatic placement and health monitoring
- **Pipeline groups** — bundle related pipelines with routing rules for event
  distribution across named pipelines
- **Connector management API** — create, list, and delete managed MQTT and Kafka
  connectors at runtime via REST
- **Event injection API** — inject test events into pipeline groups via
  `POST /api/v1/cluster/pipeline-groups/{id}/inject` with output event capture
- **Worker registration** — workers self-register with the coordinator via
  `--coordinator` and `--advertise-address` flags
- **Health sweeps** — coordinator monitors worker heartbeats and reports status
- **VPL validation endpoint** — `POST /api/v1/cluster/validate` returns parse
  errors and semantic diagnostics with line/column positions

#### Web UI (Vue 3 + Vuetify 3)
- **Pipeline editor** — Monaco-based VPL editor with syntax validation, auto-save,
  and deploy-from-editor workflow
- **Pipeline management** — deploy, teardown, and monitor pipeline groups with
  per-worker placement visibility
- **Connector management** — create/delete MQTT and Kafka connectors with
  topic configuration
- **Event tester** — inject events, view output events, and browse injection
  history with JSON formatting
- **Real-time metrics** — live events/sec, processing latency, and stream counts
  via WebSocket push
- **Grafana integration** — embedded Grafana dashboard at `/grafana/` with
  Prometheus data source

#### Hamlet Multi-Query Aggregation Engine
- **Hamlet algorithm** — shared computation across overlapping Kleene patterns
  with graphlet-based snapshot propagation (3x–100x speedup vs ZDD baseline)
- **Automatic sharing detection** — `setup_hamlet_sharing()` identifies overlapping
  patterns across queries and enables shared processing
- **Trend aggregation operator** — `trend_aggregate` VPL syntax for declaring
  multi-query trend computations
- **PropagationCoefficients** — O(1) Kleene count computation via
  `coeff * snapshot + local_sum`

#### VPL Language Enhancements
- **Semantic validator** — two-pass analysis catches undefined streams, events,
  connectors, and type mismatches at compile time
- **`count_distinct` aggregation** — both `count_distinct(field)` and
  `count(distinct(field))` syntax supported
- **Constant folding** — compile-time evaluation of constant expressions in the AST
- **Loop expansion** — `for` loops in VPL declarations expanded at parse time
- **`emit` statement** — explicit output field selection for stream results
- **`.process()` operation** — user-defined processing logic in stream pipelines
- **Unified stream syntax** — removed `from` keyword, all streams use `=` assignment

#### Connectors
- **Managed MQTT connector** — shared connection per connector with separate
  source/sink event loops and per-worker unique client IDs
- **Managed Kafka connector** — `FutureProducer` with configurable topic routing
- **AWS Kinesis connector** — stream ingestion and output
- **AWS S3 connector** — batch file source/sink
- **Elasticsearch connector** — document indexing sink

#### Multi-Tenant SaaS Infrastructure
- **Tenant isolation** — per-tenant pipeline quotas, rate limiting, and usage tracking
- **State persistence** — tenant and pipeline state survives restarts
- **Context-based execution** — multi-threaded stream isolation with cross-context
  forwarding and session windows
- **Exactly-once checkpointing** — snapshot-based recovery for stateful operators
- **CORS support** — browser-based API clients

#### Deployment
- **Docker Compose stack** — full demo with Caddy, Prometheus, Grafana, MQTT,
  Kafka, Zookeeper, and auto-setup
- **Helm chart** — Kubernetes deployment with coordinator and worker StatefulSets
- **Public demo** — [demo.varpulis-cep.com](https://demo.varpulis-cep.com) on
  Hetzner with Cloudflare TLS

### Changed

#### Performance Optimizations
- **Event data structures** — `Arc<str>` for event types and field keys,
  `Box<str>` for string values, `FxBuildHasher` for all hash maps
- **Value enum** — boxed Array/Map variants reduce enum size; consistent
  Hash/PartialEq for Float
- **Columnar storage** — SIMD-optimized aggregation buffers for batch processing
- **SASE+ engine** — `swap_remove` for O(1) run removal, `mem::take` to eliminate
  cloning, non-blocking context dispatch
- **Sync pipeline** — skip output rename, preload batch size 1000 → 10000,
  zero-clone event draining
- **Event parsing** — `split_fields()` returns `Vec<&str>` (zero-alloc),
  `with_capacity_at()` skips `Utc::now()`
- **Multi-worker scaling** — round-robin event distribution with join key inference

#### Metrics & Observability
- **Prometheus integration** — per-stream processing counts, latency histograms,
  active stream gauges, and output event counters
- **Single-event instrumentation** — `process()` path (used by MQTT connector)
  now records Prometheus metrics, not just `process_batch()`
- **Grafana dashboard** — pre-configured panels for throughput, latency, and
  stream activity

### Fixed

- MQTT client ID collisions between workers causing infinite reconnection loops
- Monaco editor freezing on New/Open due to double `setValue` calls
- Editor not reloading pipeline on keep-alive reactivation
- Input vs output event categorization in editor stream panel
- Pipeline names showing UUIDs instead of human-readable names in metrics
- Grafana metric name mismatch (`_total` suffix)
- Event injection returning `success: undefined` due to response field mismatch
- `count_distinct` not dispatched when written as `count_distinct(field)` syntax
- Caddy DNS cache going stale after container recreation
- MQTT sink publishing to wrong topic (appending event type)
- FIFO ordering for batch event processing
- Needless borrows flagged by Clippy in SASE engine

### Benchmarks

#### Hamlet vs ZDD Multi-Query Aggregation
| Queries | Hamlet | ZDD Unified | Speedup |
|---------|--------|-------------|---------|
| 1 | 6.9 M/s | 2.4 M/s | 3x |
| 5 | 2.8 M/s | 398 K/s | 7x |
| 10 | 2.1 M/s | 122 K/s | 17x |
| 50 | 0.95 M/s | 9 K/s | 100x |

#### Varpulis vs Apama (CLI, 100K events)
| Scenario | Varpulis | Apama | RAM (V / A) |
|----------|----------|-------|-------------|
| Filter | 234 K/s | 199 K/s | 54 / 166 MB |
| Kleene | 97 K/s | 195 K/s | 58 / 190 MB |
| Sequence | 256 K/s | 221 K/s | 36 / 185 MB |

## [0.1.0] - 2026-02-02

### Added

- Initial release
- VPL (Varpulis Pipeline Language) parser and AST
- GRETA-based CEP runtime with SASE+ pattern matching
- Kleene patterns with `within` and `partition by` clauses
- Windowed aggregation (`count`, `sum`, `avg`, `min`, `max`, `stddev`, `first`,
  `last`, `ema`)
- Sequence detection with `followed_by` operator
- MQTT source/sink connectors
- CLI with `run`, `simulate`, `check` commands
- ZDD-based multi-query optimization (research baseline)

[Unreleased]: https://github.com/varpulis/varpulis/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/varpulis/varpulis/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/varpulis/varpulis/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/varpulis/varpulis/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/varpulis/varpulis/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/varpulis/varpulis/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/varpulis/varpulis/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/varpulis/varpulis/releases/tag/v0.1.0
