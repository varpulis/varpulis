# SIEM Evasion Lab #5 — OUTLINE

**Status:** Outline / topic-lock only. Full draft is a follow-up task.

**Locked title:** *Why Your SIEM Can't See Cobalt Strike's Checkin Pattern*

**Locked subtitle:** *Beacon periodicity is a sequence problem, not a string problem — and your IDS rules are watching for the wrong thing.*

**Season:** 2 (Lab #4 closed Season 1 with the "single-event detection model" thesis. Season 2 widens from individual technique evasion to **C2 / persistence over time**, where the win for sequence detection is structural, not just incremental.)

**MITRE ATT&CK:** T1071 (Application Layer Protocol), T1071.001 (Web Protocols), T1573 (Encrypted Channel), T1568.002 (Domain Generation Algorithms — adjacent).

---

## Why this topic over the alternatives

The two other candidates Cyril floated:

- *"C2 channel detection: time-series anomalies vs sequence patterns"* — strong positioning angle vs UEBA, but more abstract. Saves better as a follow-up after we have a concrete C2 example readers have already seen.
- *"From Sigma to SASE+: when do you outgrow rule-based detection?"* — pure positioning piece. Belongs on the storefront / POC landing page, not in the Lab series. The Lab series earns trust by showing the work; positioning pieces tell people what to think.

Cobalt Strike checkin wins because:

1. **Name recognition.** Every blue-team reader has seen "Cobalt Strike" in an incident report. Zero ramp-up needed.
2. **Sequence-native.** Beacon checkin is *defined* by its periodicity. A single beacon request is invisible; the pattern across 30 minutes is the IOC. This is the cleanest possible illustration of why single-event matching breaks.
3. **Concrete bypasses.** Malleable C2 profiles, sleep/jitter tuning, domain fronting, framework swaps — all real, all documented, all routinely used in red-team engagements.
4. **Demo-able.** Synthetic beacon traffic is easy to generate (we already have synthetic dataset patterns in `examples/security-demo/sigma_comparison/`). No need to chase real malware samples.

---

## Article structure (mirrors Labs #1–#4)

### 1. The Rule Everyone Deploys

Existing detection landscape for Cobalt Strike — three categories the average SOC stacks:

- **String-match Sigma rules** for default Malleable C2 artifacts (named pipe `\\.\pipe\msagent_*`, default User-Agent `Mozilla/5.0 (compatible; MSIE 9.0...)`, default URL paths like `/ca`, `/dpixel`).
- **JA3/JA3S TLS fingerprinting** at the network edge — fingerprints the TLS handshake of the Cobalt Strike client.
- **IDS signatures (Snort/Suricata/ETPro)** for known beacon URI patterns and binary payload markers.

Show one canonical SigmaHQ rule (the named-pipe one is the cleanest). Quote the rule body. Frame it as: "This works against the demo. It does not work against an operator who edited the Malleable C2 profile."

### 2. The 4 Variants That Bypass It

**Variant 1 — Custom Malleable C2 profile.**
Randomize URI paths, User-Agent, Host header, named-pipe names. Cobalt Strike ships with a profile DSL specifically for this. Public profile zoo on GitHub (e.g., `threatexpress/malleable-c2`) has hundreds. Bypasses every string-match Sigma rule.
Difficulty: edit one config file, recompile beacon.

**Variant 2 — Long sleep + high jitter.**
Default beacon sleep is 60s. Operators routinely set `sleeptime 3600000` (1 hour) with 50% jitter. Most NIDS beacon detectors aggregate over 5–15 minute windows — they see one or zero requests in their window and have nothing to correlate. The pattern only emerges across hours.
Difficulty: one `sleep` command in beacon console.

**Variant 3 — Domain fronting / legitimate CDN host header.**
Beacon connects to `fronted.cloudfront.net` (or any major CDN) with a Host header pointing to the operator's bucket. Domain reputation feeds see CloudFront. JA3 sees a generic CDN-style handshake. Defeats reputation-based detection and most TLS fingerprinting because the fingerprint matches millions of legitimate clients.
Difficulty: AWS/Azure account + Malleable profile config.

**Variant 4 — Framework swap (Sliver / Havoc / Mythic).**
Sliver is open-source, written in Go, has no Cobalt Strike fingerprints anywhere. Havoc and Mythic are similar. The behavior is identical (periodic small POSTs to a controlled endpoint), but every Cobalt-Strike-specific signature is silent.
Difficulty: `git clone` + 10 minutes of setup.

For each variant: state what the Sigma/IDS detection sees (nothing, or a string that doesn't match), and what the network telemetry actually looks like.

### 3. Why String/Fingerprint Matching Fails

Sigma and IDS rules are **artifact detectors**. They answer "did I see Cobalt Strike's known strings?" — not "is something beaconing out of my network?"

The invariant across all four variants is the **periodicity**, not the content:

- N outbound HTTP(S) requests
- from the same source host
- to the same destination (host:port pair, or destination IP)
- with inter-arrival times clustered around a target sleep value
- with payload sizes in a narrow band (~hundreds to low-thousands of bytes)
- sustained over a long window (15+ minutes minimum, often hours)

This is a **temporal sequence with statistical bounds**. Sigma's detection model has no native expression for it:

- Sigma evaluates each event in isolation.
- Sigma's `temporal_ordered` correlation (v2.1+) chains *other Sigma rules*, not arbitrary timing predicates between raw events. It cannot express "8 events with inter-arrival ~60s ± 30%."
- Most SIEM backends don't implement `temporal_ordered` at all.
- Statistical bounds (jitter, payload size distribution) are entirely outside Sigma's grammar.

Cite the structural limitation explicitly — same move as Lab #1. The point is not that Sigma is bad; the point is that **Sigma was not designed to model periodicity, and trying to bolt it on with hash-of-tuples + scheduled queries is what every SIEM vendor has tried for a decade with mediocre results**.

### 4. What Actually Works: Sequence Pattern Detection

VPL example — a sketch, real version goes in the article:

```vpl
event NetworkConnect:
    SourceIp: str
    DestinationIp: str
    DestinationPort: int
    BytesSent: int
    Timestamp: int
    Hostname: str

# Beacon-shaped traffic pre-filter: small outbound HTTP(S), to a single dest
stream BeaconCandidate = NetworkConnect
    .where((DestinationPort == 80 or DestinationPort == 443)
        and BytesSent > 100 and BytesSent < 4096)

# Kleene+ sequence: N+ candidate connections from the same (src, dst) pair
# with inter-arrival within [target_sleep * (1-jitter), target_sleep * (1+jitter)]
stream BeaconingDetected =
    BeaconCandidate as b1
    -> BeaconCandidate+ where SourceIp == b1.SourceIp
                          and DestinationIp == b1.DestinationIp
                          and Timestamp - prev.Timestamp < 90
                          and Timestamp - prev.Timestamp > 30 as bs
    .within(30m)
    .where(bs.LEN >= 8)
    .emit(...)
```

Two notes for the drafter:

1. The `prev.Timestamp` referencing in the Kleene predicate needs to match whatever syntax SASE+ exposes for previous-event references in Kleene+. **Verify against `crates/varpulis-sase/` and `examples/vpl-by-example/30_selection_modes.vpl` / `31_array_semantics.vpl` before publishing — the example above is illustrative, not yet syntactically validated.** If the syntax differs, adjust; the pedagogy is what matters.
2. Ground the article in *one* concrete sleep+jitter scenario (60s ± 30%, ≥ 8 connections, 30m window). Generalize in the conclusion.

Walk the reader through what each clause does. Highlight: this is one rule, all four bypass variants fall to it, because periodicity is invariant.

### 5. Proof: Synthetic Beacon Dataset

MORDOR APT29 doesn't have great C2 traffic. Build a synthetic dataset under `examples/security-demo/sigma_comparison/c2_evasion_dataset.jsonl`:

- 30 minutes of normal HTTP/HTTPS browsing baseline (varied destinations, varied sizes, no periodicity).
- Plus one beaconing host with 30 connections to a single destination, sleep=60s, jitter=30%.
- Plus the four bypass variants (custom profile, long sleep, domain fronting, Sliver).

Run both rules. Show throughput numbers in the same format as Labs #1–#4 (`varpulis simulate -p ... -e ... -w 1 -v` block, events processed, alerts emitted, eps).

### 6. The Evasion Test (table)

| Variant                                            | Sigma / IDS (string + JA3) | VPL (sequence + periodicity) |
|----------------------------------------------------|:--------------------------:|:----------------------------:|
| Default Cobalt Strike beacon                       | 1 alert                    | 1 alert                      |
| Custom Malleable C2 profile                        | **0 alerts**               | **1 alert**                  |
| Long sleep + high jitter (1h ± 50%)                | **0 alerts**               | **1 alert** (on long window) |
| Domain fronting via CloudFront                     | **0 alerts**               | **1 alert**                  |
| Sliver / Havoc / Mythic                            | **0 alerts**               | **1 alert**                  |

Important caveat to land for the long-sleep row: VPL's window must scale to the sleep — a 30m window won't catch a 1h beacon. The *engine* models long windows fine; the *rule* needs to be tuned. Honest framing here strengthens credibility. (This sets up the future "C2 detection: anomalies vs sequence patterns" piece — the trade-off becomes the next article.)

### 7. What This Means for Your SOC

Reuse the Labs #1–#4 closing template:

- Sigma + IDS handle the artifact layer (known strings, known fingerprints). Keep them.
- A sequence detection engine handles the **behavioral periodicity layer**. Add it.
- Beacon detection is not a single-event problem. Storage and search (the SIEM) plus periodicity matching (the sequence engine) is the working combination.
- The artifact layer has no chance against a 15-minute Malleable C2 edit. The behavioral layer has no chance of being silenced by string changes.

### 8. Try It Yourself

Standard repro block (matches Labs #1–#4):

```bash
git clone https://github.com/varpulis/varpulis
cd varpulis
cargo build --release --bin varpulis

# Sigma-equivalent: string + named-pipe match
varpulis simulate \
    -p examples/security-demo/sigma_comparison/sigma_cobalt_strike.vpl \
    -e examples/security-demo/sigma_comparison/c2_evasion_dataset.jsonl \
    -w 1 -v

# VPL: sequence + periodicity
varpulis simulate \
    -p examples/security-demo/sigma_comparison/vpl_beacon_periodic.vpl \
    -e examples/security-demo/sigma_comparison/c2_evasion_dataset.jsonl \
    -w 1 -v
```

### 9. Closing teaser (Season 2 framing)

> *Next up: when periodicity isn't enough — anomaly-based UEBA versus deterministic sequence detection, and which one your buyers actually need.*

Sets up the Topic-B follow-up (UEBA comparison) without committing to it.

---

## Open questions for the drafter (must resolve before publishing)

1. **VPL syntax verification.** Confirm that the `prev.Timestamp` reference (or whatever the equivalent is) compiles against the current SASE+ implementation. If the closer-event reference is named differently (e.g., `b1.Timestamp` for first match, plus indexed array access for prior elements), adapt the example and call out the array-semantics doc (`b.LEN`, `b.field` from the SASE+ docs).
2. **Synthetic dataset layout.** Decide upfront whether `c2_evasion_dataset.jsonl` follows Sysmon EventID 3 (NetworkConnect) or a generic netflow shape. Sysmon-3 keeps it consistent with Labs #1–#4. Pick that unless there's a reason not to.
3. **Numbers.** Throughput numbers in §5 must be *measured*, not invented. Run the simulation, paste real output. Same standard as the prior labs.
4. **JA3 framing.** Don't overclaim — JA3 *does* catch default Cobalt Strike. The honest pitch is "JA3 catches default-profile Cobalt Strike, fails against custom profiles or framework swaps." Match that nuance.
5. **Length.** Labs #1–#4 are 7K–12K characters. Aim for the 10K–12K end given the topic depth (multiple bypasses, statistical bounds explanation).

---

## Acceptance for *this* outline-only task

- [x] Topic chosen and locked: Cobalt Strike checkin (Topic A).
- [x] Outline file committed at `docs/siem-evasion-lab-05-OUTLINE.md`.
- [x] Series-shape consistency with Labs #1–#4 confirmed (8-section structure, evasion-table, MITRE tagging, repro block).
- [x] Open questions surfaced for the future drafter.

Full draft is **not** in scope for this task. Next plan task should bundle: dataset generation, Sigma-equivalent VPL rule, behavioral VPL rule, simulation run, draft writing.
