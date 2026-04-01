# SIEM Evasion Lab #1: Your Sigma Rule for PsExec Misses These 4 Variants

*How a 30-second rename defeats the most deployed lateral movement detection rule — and what actually works instead.*

---

## The Rule Everyone Deploys

The [SigmaHQ PsExec rule](https://github.com/SigmaHQ/sigma/blob/master/rules/windows/process_creation/proc_creation_win_sysinternals_psexec.yml) is one of the most widely deployed Sigma rules in the world. It's simple and it works:

```yaml
detection:
    selection:
        - Image|endswith: '\PsExec.exe'
        - Image|endswith: '\PsExec64.exe'
        - OriginalFileName: 'psexec.c'
    condition: selection
```

Match the filename. Trigger the alert. Move on.

**The problem:** it's a string match. And strings are the easiest thing in the world to change.

## The 4 Variants That Bypass It

### Variant 1: Rename the Binary

```
copy PsExec64.exe svcupdate.exe
svcupdate.exe \\DC01 -s cmd.exe
```

The Sigma rule checks `Image|endswith: '\PsExec.exe'`. The binary is now `svcupdate.exe`. The rule is silent.

**Difficulty: 5 seconds.**

### Variant 2: Modify the PE Header

The Sigma rule has a fallback: `OriginalFileName: 'psexec.c'`. This reads the PE resource section.

```
# Using Resource Hacker or pe-bear:
# Change OriginalFileName from "psexec.c" to "svcmgr.c"
```

**Difficulty: 2 minutes with any PE editor.**

### Variant 3: Use a Different Tool, Same Mechanism

PsExec works by:
1. Connecting to the target's `ADMIN$` share via SMB (port 445)
2. Copying a service binary (`PSEXESVC.exe`)
3. Creating and starting a Windows service via SCM
4. Redirecting I/O through named pipes

Other tools do the exact same thing:
- **Impacket's `smbexec.py`** — creates a service that writes output to a temp file
- **Impacket's `wmiexec.py`** — uses WMI instead of SCM, same SMB channel
- **CrackMapExec** — wraps all of the above
- **Sharp-SMBExec** — C# reimplementation

None of these are named `PsExec.exe`. The Sigma rule catches zero of them.

### Variant 4: Custom C2 Framework

Any modern C2 framework (Cobalt Strike, Sliver, Havoc, Mythic) can perform the same lateral movement:

```
# Cobalt Strike Beacon
lateral psexec DC01 LISTENER
```

The actual binary executed on the target is a custom payload. The filename is random. Sigma sees nothing.

## Why String Matching Fails

The Sigma rule is a **tool detector**, not an **attack detector**. It answers "was PsExec.exe executed?" — not "did someone move laterally using SMB service creation?"

The fundamental issue is that Sigma's detection model evaluates **individual events in isolation**. Each log line is checked against the rule independently. There is no concept of:

- **Temporal ordering** — "Event A happened, then Event B happened within 2 minutes"
- **Cross-event correlation** — "The process that made the SMB connection also triggered a service creation on the remote host"
- **Behavioral patterns** — "This sequence of actions matches a known attack pattern, regardless of the specific tool"

Sigma's `temporal_ordered` correlation type (introduced in v2.1) can express basic sequences, but:
1. It only chains other Sigma rules, not arbitrary event conditions
2. Cross-field binding between events is limited to `group-by` + `aliases`
3. No support for Kleene closures (one-or-more events)
4. Adoption is minimal — most SIEM backends don't implement it

## What Actually Works: Behavioral Sequence Detection

Instead of matching the tool, match the **behavior**:

```
SMB connection (port 445) → process spawned by services.exe → command execution
```

This pattern is invariant. It doesn't matter if the tool is PsExec, smbexec, CrackMapExec, or a custom implant. The operating system-level behavior is identical:

1. An SMB connection is established to the target's port 445
2. A service is created via the Service Control Manager
3. `services.exe` spawns the payload process
4. Commands execute under the service's security context

Here's how this looks in VPL (Varpulis Pattern Language):

```vpl
# Step 1: SMB connection (PsExec uses port 445 to create a remote service)
stream SMBServicePipe = SysmonNetworkConnect
    .where(DestinationPort == 445)

# Step 2: Process spawned by services.exe (remote service execution)
stream RemoteServiceExec = SysmonProcessCreate
    .where(ParentImage.ends_with("\services.exe")
        or ParentImage.contains("PSEXESVC.exe"))

# Behavioral sequence: SMB → remote service execution within 2 minutes
stream BehavioralLateralMovement = SMBServicePipe as smb
    -> RemoteServiceExec as remote_exec
    .within(2m)
    .emit(
        event_type: "KillChainAlert",
        mitre: "T1021.002",
        severity: "critical",
        source_host: smb.Hostname,
        target_ip: smb.DestinationIp,
        remote_process: remote_exec.Image,
        remote_cmdline: remote_exec.CommandLine
    )
```

This rule uses SASE+ (Streaming Active Sequence Elements) to match a **temporal pattern across multiple events**. It fires when:
1. A network connection to port 445 is observed
2. Followed by a process creation where the parent is `services.exe` (or `PSEXESVC.exe`)
3. Both events occur within a 2-minute window

**This catches all 4 variants.** The binary name doesn't matter. The PE header doesn't matter. The tool doesn't even need to exist yet.

## Proof: Running Against Real APT29 Data

We tested this against the [MORDOR APT29 Day 1 dataset](https://github.com/OTRF/Security-Datasets) — 50,000 Sysmon events from a real MITRE ATT&CK Evaluation simulating Cozy Bear:

```
$ varpulis simulate -p detect_lateral_movement.vpl \
    -e mordor/apt29_day1_50k.jsonl -w 1 -v

Events processed: 50000
Output events emitted: 21
Event rate: 25,646 events/sec
```

21 lateral movement detections at 25K events/sec. The same engine also detected the full 4-step kill chain (script execution → credential theft → lateral movement → exfiltration) from the same dataset in a single pass.

## The Renamed Binary Test

We created a synthetic dataset where the attacker renames PsExec to `svcupdate.exe`:

| | Sigma (filename match) | VPL (behavioral) |
|---|:---:|:---:|
| Renamed PsExec (`svcupdate.exe`) | **0 alerts** | **1 alert** |
| Normal PsExec | 1 alert | 1 alert |

Same attack. Same data. Same engine. Different rules. Only one of them works when the attacker spends 5 seconds on evasion.

## What This Means for Your SOC

If you're relying on Sigma rules for lateral movement detection, you have a coverage gap. This isn't a criticism of Sigma — it's a structural limitation of the single-event detection model. Sigma is excellent at what it does (standardized, portable, community-driven). But it wasn't designed for multi-step attack detection.

The fix isn't to write more Sigma rules. It's to **add a detection layer that reasons about sequences**.

Your SIEM handles storage and search. A behavioral detection engine like Varpulis handles the temporal pattern matching. They complement each other.

## Try It Yourself

```bash
git clone https://github.com/varpulis/varpulis
cd varpulis
cargo build --release --bin varpulis
bash examples/security-demo/run_demo.sh
```

The demo includes the renamed PsExec comparison, 6 more detection rules, and the full APT29 kill chain test. All VPL source is in `examples/security-demo/`.

---

*This is SIEM Evasion Lab #1 — a series dissecting popular detection rules and the attack variants they miss. Next up: Mimikatz detection and why hash-based rules fail against reflective loading.*

*Built with [Varpulis](https://github.com/varpulis/varpulis) — a dual red/blue kill chain detection engine.*
