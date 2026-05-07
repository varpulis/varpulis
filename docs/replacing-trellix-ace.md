# Replacing Trellix ACE with Varpulis

*A structural read on why correlation rules go quiet under load — and the migration path that doesn't require ripping out your SIEM.*

---

If you've run a Trellix ACE deployment in anger, you already know the symptom that brings most people to this article: a correlation rule that fires perfectly in lab conditions, then goes silent during the incident it was written for. Rule didn't change. Data didn't change. Volume changed.

This piece is for SIEM engineers asking whether the answer is to keep tuning ACE or to bridge correlation out into something purpose-built. It assumes you already know what ESM and ACE are, what Sysmon and Sigma look like, and that you've at least sketched a kill chain detection on paper. I'll skip the introductions.

## The architectural seam

Trellix ESM is two things glued together. The Receiver collects logs, parses, and normalizes them into the ESM schema (`SrcIP`, `DstIP`, `Action`, `EventTime`, etc.). The Advanced Correlation Engine — ACE — subscribes to that normalized stream and runs correlation rules: chains, sequences, deviations.

The seam is between those two halves. Parsing is roughly linear in input volume. Correlation is not — a rule with a sliding window holds state proportional to "events in window × number of partial matches", and on noisy auth logs over a 10-minute window, that quantity can be far from linear. A rule that costs nothing in a lab will start punching above its weight on real data.

The seam matters because ACE depends on parsed, ordered, in-window events to be correct. When the upstream parser falls behind, or the bus to ACE backs up, every correlation rule with a time window becomes a coin flip — not because the rule is wrong, but because it's evaluating events that arrived in the wrong order, late, or not at all.

## Why ACE rules go silent

ACE's correlation model has three structural constraints that bite under load. None of them are bugs. They're the consequence of building correlation on the same fabric as the parser, in a JVM-shaped operational box.

**Watermarks are implicit.** ACE assumes events arriving at the correlator are already roughly ordered by `EventTime`. When parsing falls behind on one Receiver, that subset of events arrives late, and a rule reasoning over the last 10 minutes silently drops them as "out of window". You'll see this in the EPS chart — Receiver lag spikes, ACE alerts dip, no alarm rings. The correlation engine can't tell you whether it dropped events because the pattern wasn't there or because the data wasn't there.

**State eviction under memory pressure is opaque.** A correlation rule with multi-step matches holds partial matches in memory. ACE evicts partial matches to stay within its heap budget. The eviction policy is a black box from the rule author's perspective. A rule that runs fine for weeks can start missing the second leg of a sequence the moment your noisy auth source pushes the engine into eviction mode. You won't see an error — you'll see fewer alerts.

**Rule scheduling is FIFO across the rule set.** Heavy rules block light ones. A single "all logins in last 24h grouped by user" running over a 100k-user environment will sit in the queue and starve faster rules behind it. The fix in ACE is to manually rewrite the rule to be cheaper. There's no way to say "this rule is critical, run it on its own slot".

> **2026 Q3 update planned.** This article is the structural read, written from public Trellix documentation. A follow-up will publish field measurements from a current Trellix-to-Varpulis migration: rule fan-out, dropped alerts during parser lag, and like-for-like rule translations under measured load.

The structural fix isn't to tune harder. It's to move correlation out from behind the parser, onto a bus that gives you ordering guarantees, watermarks, and per-rule isolation.

## Architecture: ESM → Kafka → Varpulis

ESM stays. It does what it does well: collection, parsing, storage, search, the analyst UI. What changes is where correlation runs.

```
┌──────────────┐    Kafka topic       ┌──────────────┐
│  Trellix     │   esm.events.*       │  Varpulis    │
│  ESM         │ ───────────────────> │  cluster     │
│              │   normalized JSON    │              │
│  Receivers   │                      │  SASE+ rules │
│  Parsing     │                      │  watermarks  │
│  Storage     │ <─────────────────── │  per-rule    │
│  Search UI   │   correlated alerts  │  isolation   │
└──────────────┘   esm.alerts         └──────────────┘
```

Recent ESM releases support event forwarding to external Kafka. You configure the Receiver (or the ESM itself) to mirror normalized events to a Kafka topic. That's the seam — parsed and normalized, but not yet correlated. Varpulis subscribes, runs correlation, and POSTs results back to ESM as alarms via the REST API (or to a dedicated `esm.alerts` topic that ESM consumes).

Picking the parsed/normalized path over the raw-Receiver path matters. ESM's parsers are mature, and ATT&CK-style rules want fields like `SrcIP`, `Action`, `Hostname` already extracted — re-doing that work in Varpulis would duplicate years of vendor regex maintenance. Take what ESM is good at.

A minimal wiring on the Varpulis side:

```vpl
connector EsmKafka = kafka(
    brokers: ["esm-kafka.internal:9092"],
    group_id: "varpulis-correlator"
)

event EsmEvent:
    SrcIP: str
    DstIP: str
    DstPort: int
    Action: str
    Hostname: str
    UserName: str
    Image: str
    CommandLine: str
    ParentImage: str
    ProcessId: int

stream Events = EsmEvent
    .from(EsmKafka, topic: "esm.events.normalized")
```

That's the ingestion side. Correlation rules then read off `Events` (or sub-streams pre-filtered by event type), and emitted alerts route back to ESM through an HTTP sink. The whole bridge is two files: connectors and rules.

### The event-time question

There's one detail that matters more than it looks. Varpulis needs to know which field in the JSON payload represents event time — when the thing actually happened, not when Kafka got the message. For correctness on time-windowed rules, you want event time, not ingestion time.

Today, the Kafka source connector picks event time in this priority order, defined in `crates/varpulis-connector-api/src/helpers.rs`:

```rust
// `@timestamp` as RFC3339 / ISO8601 string (Sysmon / Varpulis native)
if let Some(s) = obj.get("@timestamp").and_then(|v| v.as_str()) {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
}
// `ts` or `timestamp` as integer epoch milliseconds
for key in &["ts", "timestamp"] {
    if let Some(n) = obj.get(*key).and_then(|v| v.as_i64()) {
        if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(n) {
            return Some(dt);
        }
    }
}
None
```

If none match, Varpulis falls back to `Utc::now()` — meaning ingestion time. For rules with sub-second windows, that's wrong. For rules with multi-minute windows on a healthy bus, it's usually fine.

ESM's normalized events use `EventTime` (or sometimes `LastTime`, depending on the source). Neither of those names matches the priority order above out of the box. Three options today:

1. Reshape the JSON in the producer (cheap if you control the bridge, painful if you don't).
2. Run a small Kafka Streams or Benthos pipeline between ESM and Varpulis that copies `EventTime` into `@timestamp`.
3. Live with ingestion-time correlation, which is acceptable on most multi-minute-window rules over a healthy bus.

This is also a Varpulis gap I want to close. A `.timestamp_by(field)` operator on the stream definition — `stream Events = EsmEvent.from(EsmKafka, topic: ...).timestamp_by(EventTime)` — would let you point at any field without producer-side rewriting. It's flagged as a future improvement in the cep issue tracker; for an ESM-driven deployment this is the single ergonomics gap that matters most, so it'll get prioritized as soon as the first paying integration needs it.

## Rule translation: two worked examples

ACE rule definitions don't translate line-for-line — VPL is a different abstraction. What translates is the *intent* of the rule. Two examples to show the shape.

### Example 1 — Lateral movement (PsExec class)

In ACE, this is typically built in the GUI as a sequence: "match Event A (network connection to port 445) followed by Event B (process creation under services.exe) within 2 minutes, on the same destination host". The rule fires; ESM gets a correlated alarm.

In VPL, the same intent looks like this:

```vpl
stream SMB = EsmEvent
    .where(DstPort == 445 and Action == "network_connect")

stream RemoteExec = EsmEvent
    .where(Action == "process_create"
        and (ParentImage.contains("services.exe")
             or ParentImage.contains("PSEXESVC.exe")))

stream LateralMovement = SMB as smb
    -> RemoteExec as remote_exec
    .within(2m)
    .emit(
        rule: "lateral_movement_smb",
        mitre: "T1021.002",
        source_host: smb.Hostname,
        target_ip: smb.DstIP,
        remote_process: remote_exec.Image,
        remote_cmdline: remote_exec.CommandLine,
        severity: "critical"
    )
```

Three differences are worth flagging. The pre-filter streams (`SMB`, `RemoteExec`) are explicit, and they cost nothing extra — the engine fuses them into the same NFA. The `.within(2m)` window is a watermark-aware bound, not a "events in last 2 minutes" wall-clock query. And the emit is a structured record, not a free-text alert message — it routes cleanly to a Kafka topic, an ESM HTTP alarm, or a Slack webhook.

That last bit is where the structural advantage of moving correlation out of ACE shows up. The same rule, same input, will produce the same output regardless of total bus throughput, because the engine reasons over watermarked event time, not wall-clock.

### Example 2 — Credential dumping → remote use

ACE-side this is usually built as a single-event match: "process opens a handle to lsass.exe with suspicious access rights, where the source process is not on a known-safe list". Not a sequence — an event filter with an exclusion list, and ACE handles it fine.

The interesting part is what becomes possible once the rule lives in Varpulis. The single-event match becomes the *input* to a sequence:

```vpl
stream LsassAccess = EsmEvent
    .where(Action == "process_access"
        and TargetImage.contains("lsass.exe")
        and (GrantedAccess == "0x1010"
             or GrantedAccess == "0x1410"
             or GrantedAccess == "0x1438"
             or GrantedAccess == "0x1fffff")
        and not (SourceImage.contains("wininit.exe"))
        and not (SourceImage.contains("csrss.exe")))

stream RemoteAuth = EsmEvent
    .where(Action == "remote_logon" and DstPort == 445)

# Credential dump followed by remote auth from the same host, within an hour
stream CredentialUse = LsassAccess as dump
    -> RemoteAuth where Hostname == dump.Hostname as auth
    .within(1h)
    .emit(
        rule: "credential_access_then_remote_use",
        mitre: "T1003.001,T1021.002",
        host: dump.Hostname,
        source_process: dump.SourceImage,
        target_ip: auth.DstIP,
        severity: "critical"
    )
```

This is a chain ACE struggles with at scale — partial matches across an hour-long window, on every host, conditioned on field equality. In VPL it's seven lines of pattern. The work the engine has to do is the same either way; the difference is that the work is bounded per rule, and the eviction policy is in your hands rather than the appliance's heap.

## Migration: parallel-run, backfill, cutover

The migration shape that survives in production is the boring one — don't switch, double-write, then prune.

**Phase 1 — Parallel run.** Stand up Varpulis next to ACE. Both consume the same normalized stream (Varpulis from Kafka, ACE from its existing pipeline). Both emit alerts. Pipe both into ESM as different alarm sources. Run for two to four weeks. The goal isn't yet to retire any ACE rules — it's to build trust that the same input produces the same output (or, more honestly, to find the cases where it doesn't).

**Phase 2 — Backfill validation.** Replay 30 days of historical events from ESM into a separate Varpulis instance. Compare alarm counts to what ACE produced over the same window. Expect a small delta — it's almost always one of three things: ACE dropped events under load, the rule semantics aren't actually equivalent (most often: ACE's "within" being wall-clock rather than event-time), or the rule was firing on noise that the cleaner watermarking suppresses. Investigate each delta. Don't paper over them.

**Phase 3 — Cutover, by rule.** Migrate one rule at a time. Promote the Varpulis version to "primary" — ESM treats its alarms as the ones to act on — and keep the ACE version running in shadow. Monitor for one week per rule. If the Varpulis rule misses an incident the ACE rule caught, the shadow alerts are there to roll back on. If it doesn't, retire the ACE rule.

**Phase 4 — ACE decommission.** Once every rule has cut over, ACE becomes a parser-and-storage path with no correlation load. At that point, the appliance is doing nothing it needs to be doing. Decommissioning is a budget exercise, not an engineering one.

The whole migration runs on top of the ESM you already have. There is no "rip out the SIEM" step. There is also no point at which you're flying blind — every rule has a working version somewhere until the last cutover.

---

If you're sitting on an ACE deployment that's silently dropping correlation alerts under load, the next step is short. Stand up the [Sysmon security demo](../examples/security-demo/) to see the engine in motion against real APT29 telemetry, then book a [proof-of-concept engagement](https://varpulis-cep.com/poc) — we'll wire your ESM Kafka topic into a Varpulis instance and translate three of your highest-volume rules together. Two weeks, fixed-price entry.

*If you've migrated rules off ACE before, I'd genuinely like to hear what surprised you :)*
