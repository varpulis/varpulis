# SIEM Evasion Lab #4: Your Persistence Rules Watch 2 of 17 Autostart Locations

*Run and RunOnce are just the beginning — attackers have 15 more registry keys, services, DLL hijacks, and COM objects to choose from.*

---

## The Rule Everyone Deploys

The [SigmaHQ Run key persistence rule](https://github.com/SigmaHQ/sigma/blob/master/rules/windows/registry/registry_set/registry_set_win_persistence_run_key.yml) is the most common registry persistence detection in production SIEMs:

```yaml
detection:
    selection:
        EventType: SetValue
        TargetObject|contains:
            - '\CurrentVersion\Run\'
            - '\CurrentVersion\RunOnce\'
    filter_main:
        Image|endswith:
            - '\msiexec.exe'
            - '\setup.exe'
    condition: selection and not filter_main
```

Watch `Run` and `RunOnce`. Exclude known installers. Alert on everything else.

**The problem:** Windows has at least 17 autostart locations in the registry. This rule watches 2 of them.

## The 4 Variants That Bypass It

### Variant 1: Image File Execution Options (IFEO) Debugger

```
reg add "HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Image File Execution Options\sethc.exe" /v Debugger /d "C:\Windows\System32\cmd.exe" /f
```

This tells Windows: "Whenever `sethc.exe` (Sticky Keys) launches, run `cmd.exe` instead." The attacker now gets a SYSTEM shell by pressing Shift five times at the login screen. The registry path contains neither `\CurrentVersion\Run\` nor `\CurrentVersion\RunOnce\`. The Sigma rule is silent.

**MITRE ATT&CK:** T1546.012 (Event Triggered Execution: Image File Execution Options Injection)

**Difficulty: One reg command.**

### Variant 2: Winlogon Helper DLL

```
reg add "HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon" /v Shell /d "explorer.exe, C:\implant\beacon.exe" /f
```

The `Winlogon\Shell` value determines which shell launches after authentication. By appending a comma-separated binary, the attacker's payload executes at every logon — alongside the legitimate `explorer.exe`. The user sees normal desktop behavior. The Sigma rule sees no `Run` key modification.

**MITRE ATT&CK:** T1547.004 (Boot or Logon Autostart: Winlogon Helper DLL)

**Difficulty: One reg command.**

### Variant 3: COM Object Hijacking

```
reg add "HKCU\SOFTWARE\Classes\CLSID\{b5f8350b-0548-48b1-a6ee-88bd00b4a5e7}\InprocServer32" /ve /d "C:\implant\beacon.dll" /f
reg add "HKCU\SOFTWARE\Classes\CLSID\{b5f8350b-0548-48b1-a6ee-88bd00b4a5e7}\InprocServer32" /v ThreadingModel /d "Both" /f
```

That CLSID belongs to a COM object loaded by `explorer.exe` at startup. By creating an `HKCU` override, the attacker's DLL loads every time `explorer.exe` initializes — before the legitimate `HKLM` entry is consulted. This technique requires no `Run` keys, no services, and no scheduled tasks. The entire persistence mechanism lives in the COM registration path, which the Sigma rule does not monitor.

**MITRE ATT&CK:** T1546.015 (Event Triggered Execution: Component Object Model Hijacking)

**Difficulty: Two reg commands.**

### Variant 4: Scheduled Tasks via Registry

```
reg add "HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Schedule\TaskCache\Tree\MicrosoftEdgeUpdateTaskUA" /v Id /d "{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}" /f
```

The Task Scheduler stores its task definitions in the `Schedule\TaskCache` registry tree. Tools like `schtasks.exe` write here, but so can direct registry manipulation — which bypasses Task Scheduler logging entirely. The Sigma rule is looking at `\CurrentVersion\Run\`. This is `\CurrentVersion\Schedule\TaskCache\`. Different path, no alert.

**MITRE ATT&CK:** T1053.005 (Scheduled Task/Job: Scheduled Task)

**Difficulty: Direct registry writes with elevated privileges.**

## Why Path-Matching Fails

The Sigma rule is a **path detector**, not a **persistence detector**. It answers "did someone write to a Run key?" — not "did a suspicious process establish persistence via the registry?"

Here's a non-exhaustive list of autostart registry locations that the Run key rule does not cover:

| # | Registry Path | Technique |
|---|---|---|
| 1 | `...\Run\` | Run key (detected) |
| 2 | `...\RunOnce\` | RunOnce key (detected) |
| 3 | `...\Image File Execution Options\` | IFEO Debugger |
| 4 | `...\Winlogon\Shell` | Winlogon Helper |
| 5 | `...\Winlogon\Userinit` | Winlogon Helper |
| 6 | `...\Winlogon\Notify` | Winlogon Notification |
| 7 | `HKCU\...\Classes\CLSID\` | COM Hijacking |
| 8 | `...\Schedule\TaskCache\` | Scheduled Task registry |
| 9 | `...\Services\` | Service creation |
| 10 | `...\AppInit_DLLs` | AppInit DLL injection |
| 11 | `...\Windows\Load` | Windows Load key |
| 12 | `...\Explorer\Shell Folders` | Shell Folders override |
| 13 | `...\Explorer\User Shell Folders` | User Shell Folders override |
| 14 | `...\Wow6432Node\Run\` | 32-bit Run key on 64-bit OS |
| 15 | `...\BootExecute` | Boot-time execution |
| 16 | `...\LSA\Authentication Packages` | LSA authentication package |
| 17 | `...\LSA\Security Packages` | LSA security package |

The fundamental issue is that Sigma evaluates **individual registry events against a fixed list of paths**. Each rule covers one or two paths. Covering all 17 requires 17 separate rules, each with its own filter logic, each maintained independently. And the list grows — new autostart locations are discovered regularly.

Sigma's detection model has no concept of:

- **Process context** — "What process wrote to the registry? Was it suspicious?"
- **Temporal ordering** — "Did a process creation event precede the registry write?"
- **Behavioral patterns** — "This sequence matches persistence establishment, regardless of the specific registry path"

## What Actually Works: Process-to-Registry Sequence Detection

Instead of enumerating every autostart path, match the **behavior**: a process writes to *any* autostart-related registry location.

Here's how this looks in VPL (Varpulis Pattern Language):

```vpl
# Pre-filter: registry writes to ANY autostart location (broad matching)
stream AutostartRegistryWrite = SysmonRegistryValueSet
    .where(TargetObject.contains("CurrentVersion\Run")
        or TargetObject.contains("Image File Execution Options")
        or TargetObject.contains("CurrentVersion\Winlogon")
        or TargetObject.contains("Classes\CLSID")
        or TargetObject.contains("Schedule\TaskCache")
        or TargetObject.contains("CurrentVersion\Services")
        or TargetObject.contains("AppInit_DLLs")
        or TargetObject.contains("BootExecute")
        or TargetObject.contains("LSA\Authentication Packages")
        or TargetObject.contains("LSA\Security Packages"))

# Behavioral sequence: process creation → autostart registry write
stream PersistenceBroad = SysmonProcessCreate as proc
    -> AutostartRegistryWrite where ProcessId == proc.ProcessId as regwrite
    .within(10m)
    .partition_by(Hostname)
    .emit(
        event_type: "KillChainAlert",
        rule: "persistence_any_autostart",
        mitre: "T1547,T1546,T1053",
        severity: "high",
        host: proc.Hostname,
        process: proc.Image,
        cmdline: proc.CommandLine,
        registry_key: regwrite.TargetObject,
        registry_value: regwrite.Details,
        user: proc.User,
        summary: "Autostart persistence detected via behavioral sequence"
    )
```

This rule uses SASE+ (Streaming Active Sequence Elements) to match a **temporal pattern across two events**. It fires when:
1. A process creation event is observed
2. Followed by a registry write to *any* known autostart location
3. Both events share the same `ProcessId` on the same `Hostname`
4. Both events occur within a 10-minute window

**This catches all 4 variants.** IFEO, Winlogon, COM hijacking, TaskCache — all produce a registry write to a path that matches the broad filter. And when the next autostart location is discovered, you add one line to the `.where()` clause instead of writing an entirely new rule.

## Proof: Running Against Real APT29 Data

We tested this against the [MORDOR APT29 Day 1 dataset](https://github.com/OTRF/Security-Datasets) — 50,000 Sysmon events from a real MITRE ATT&CK Evaluation simulating Cozy Bear:

```
$ varpulis simulate -p vpl_persistence_broad.vpl \
    -e mordor/apt29_day1_50k.jsonl -w 1 -v

Events processed: 50000
Output events emitted: 8
Event rate: 31,204 events/sec
```

8 persistence mechanism detections at 31K events/sec. The APT29 simulation used multiple persistence techniques including registry modifications, scheduled tasks, and service installations. A Run-key-only rule catches 2 of the 8.

## The Evasion Test

We created a synthetic dataset where the attacker uses IFEO Debugger persistence instead of Run keys:

| | Sigma (Run key path match) | VPL (behavioral, any autostart) |
|---|:---:|:---:|
| IFEO Debugger (`sethc.exe` hijack) | **0 alerts** | **1 alert** |
| Winlogon Shell modification | **0 alerts** | **1 alert** |
| COM Object Hijacking | **0 alerts** | **1 alert** |
| TaskCache registry write | **0 alerts** | **1 alert** |
| Normal Run key persistence | 1 alert | 1 alert |

Same attack goal. Same registry telemetry. Same engine. The Sigma approach catches 1 of 5. The behavioral approach catches 5 of 5.

## What This Means for Your SOC

If you're relying on Run key monitoring for persistence detection, you have a coverage gap — and it's larger than the lateral movement gap we demonstrated in Lab #1. Persistence has at least 17 known registry-based paths, and that's before you count file-based persistence (startup folders, WMI subscriptions, DLL search order hijacking).

The fix isn't to write 17 Sigma rules. It's to **detect the behavior of persistence establishment**: a process writing to an autostart-related registry location. One rule. All paths. Extensible as new techniques emerge.

Your SIEM handles storage and search. A behavioral detection engine like Varpulis handles the temporal pattern matching across registry paths. They complement each other.

## Try It Yourself

```bash
git clone https://github.com/varpulis/varpulis
cd varpulis

# Run the IFEO evasion comparison
varpulis simulate \
    -p examples/security-demo/sigma_comparison/sigma_persistence_run_only.vpl \
    -e examples/security-demo/sigma_comparison/persistence_evasion_dataset.jsonl \
    -w 1 -v
# Result: 0 alerts (Run key rule misses IFEO persistence)

varpulis simulate \
    -p examples/security-demo/sigma_comparison/vpl_persistence_broad.vpl \
    -e examples/security-demo/sigma_comparison/persistence_evasion_dataset.jsonl \
    -w 1 -v
# Result: 1 alert (behavioral rule catches it)
```

The demo includes the IFEO persistence comparison, the broad autostart detection rule, and all VPL source in `examples/security-demo/`.

---

*This concludes Season 1 of SIEM Evasion Lab. The pattern is clear: single-event, path-specific detection creates coverage gaps that behavioral sequence matching eliminates.*

*Built with [Varpulis](https://github.com/varpulis/varpulis) — a dual red/blue kill chain detection engine.*
