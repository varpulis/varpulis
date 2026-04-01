# SIEM Evasion Lab #3: Your Lateral Movement Rules Only Watch One Door

*PsExec-focused detection misses WMI, WinRM, DCOM, and scheduled task pivots — covering 4 doors instead of 1.*

---

## The Rule Everyone Deploys

After SIEM Evasion Lab #1 showed how a renamed PsExec defeats filename-based detection, most teams upgrade to **behavioral** PsExec rules. The improved rule watches for `services.exe` spawning unexpected children — the OS-level signature of remote service creation over SMB:

```yaml
title: Remote Service Execution via SMB (PsExec-like)
status: stable
logsource:
    category: process_creation
    product: windows
detection:
    selection:
        ParentImage|endswith: '\services.exe'
        Image|endswith:
            - '\cmd.exe'
            - '\powershell.exe'
            - '\rundll32.exe'
    condition: selection
level: high
tags:
    - attack.lateral_movement
    - attack.t1021.002
```

This is a real improvement. It catches renamed PsExec, Impacket's `smbexec.py`, CrackMapExec, and any future tool that creates a remote Windows service. The rule no longer cares about the binary name — it watches the behavior.

**The problem:** SMB service creation is only one of at least four lateral movement mechanisms in Windows. The rule watches one door while three others stand wide open.

## The 4 Variants That Bypass It

### Variant 1: WMI Lateral Movement

Windows Management Instrumentation provides a fully legitimate remote execution interface. No SMB service creation involved.

```
wmic /node:DC01 process call create "cmd.exe /c whoami > C:\temp\out.txt"
```

On the target machine, the WMI Provider Host (`WmiPrvSE.exe`) spawns the payload process. The parent chain looks nothing like PsExec:

```
wmiprvse.exe → cmd.exe /c whoami > C:\temp\out.txt
```

Sysmon EventID 1 fires with `ParentImage: C:\Windows\System32\wbem\WmiPrvSE.exe`. The Sigma rule checks for `ParentImage|endswith: '\services.exe'`. **No match. No alert.**

This is the lateral movement technique used by Impacket's `wmiexec.py`, which is one of the most popular post-exploitation tools in real-world intrusions.

**Difficulty: one command.**

### Variant 2: WinRM / PowerShell Remoting

PowerShell Remoting uses the Windows Remote Management (WinRM) service over HTTP/HTTPS (ports 5985/5986):

```powershell
Invoke-Command -ComputerName DC01 -ScriptBlock { whoami; hostname }
```

On the target, the WS-Management Provider Host (`wsmprovhost.exe`) spawns the payload:

```
wsmprovhost.exe → powershell.exe -ScriptBlock { whoami; hostname }
```

Sysmon EventID 1 shows `ParentImage: C:\Windows\System32\wsmprovhost.exe`. The services.exe rule is blind.

WinRM is the default transport for most enterprise management tools (SCCM, Ansible for Windows, DSC). It is enabled by default on Windows Server 2012+ in domain environments. Attackers love it because it blends in perfectly with legitimate admin traffic.

**Difficulty: PowerShell one-liner.**

### Variant 3: DCOM Lateral Movement

Distributed COM allows remote object activation. The classic technique uses the `MMC20.Application` object:

```powershell
$com = [activator]::CreateInstance([type]::GetTypeFromProgID("MMC20.Application","DC01"))
$com.Document.ActiveView.ExecuteShellCommand("cmd.exe",$null,"/c whoami","7")
```

On the target, `mmc.exe` (Microsoft Management Console) spawns the payload:

```
mmc.exe → cmd.exe /c whoami
```

The connection arrives over port 135 (RPC endpoint mapper) followed by a dynamic high port. Sysmon EventID 1 shows `ParentImage: C:\Windows\System32\mmc.exe`. Other DCOM objects produce different parents — `excel.exe`, `outlook.exe`, `explorer.exe`. The services.exe rule matches none of them.

This technique was documented by Matt Nelson (@enigma0x3) in 2017 and has been used by multiple APT groups since, including in MITRE ATT&CK Evaluations.

**Difficulty: 3 lines of PowerShell.**

### Variant 4: Scheduled Task Creation

The Windows Task Scheduler supports remote task creation:

```
schtasks /create /s DC01 /tn "WindowsUpdate" /tr "cmd.exe /c whoami" /sc once /st 00:00 /ru SYSTEM
schtasks /run /s DC01 /tn "WindowsUpdate"
```

On the target, the Task Scheduler service (`svchost.exe` hosting the Schedule service) spawns the payload:

```
svchost.exe (Schedule) → cmd.exe /c whoami
```

Sysmon EventID 1 shows `ParentImage: C:\Windows\System32\svchost.exe`. While `svchost.exe` could theoretically be added to the Sigma rule, it hosts hundreds of services and generates enormous volumes of legitimate child process creation. Adding it would drown the SOC in false positives.

This is the technique APT29 (Cozy Bear) used in multiple operations, including the SolarWinds campaign.

**Difficulty: one command.**

## Why Single-Technique Detection Fails

The Sigma rule from the previous section is a **technique detector** — it answers "did someone use SMB service creation for lateral movement?" It does not answer the broader question: "did someone execute code remotely on this host?"

Windows provides at least four distinct remote execution mechanisms:

| Mechanism | Parent Process | Network Port | Sigma Rule Detects? |
|---|---|---|:---:|
| SMB/PsExec (service creation) | `services.exe` | 445 | Yes |
| WMI | `WmiPrvSE.exe` | 135 + dynamic | **No** |
| WinRM/PS Remoting | `wsmprovhost.exe` | 5985/5986 | **No** |
| DCOM | `mmc.exe` (varies) | 135 + dynamic | **No** |
| Scheduled Task | `svchost.exe` | 135/445 | **No** |

25% coverage. The attacker has four choices and the defender covers one.

The standard Sigma response is "write four rules." But this creates a maintenance problem: each rule must be tuned independently, each generates its own alert type, and the SOC must correlate them manually. When the fifth technique appears (and it will — COM+ Services, SSH on modern Windows, WS-Management raw), you need a fifth rule.

The deeper issue is structural. Sigma rules are **technique-specific** by design. Each rule matches one pattern. Attackers are **technique-agnostic** by practice. They pick whichever door is unwatched.

## What Actually Works: Multi-Vector Behavioral Detection

Instead of writing one rule per technique, define a single pattern that covers all remote execution parents:

```vpl
event SysmonNetworkConnect:
    Image: str
    SourceIp: str
    DestinationIp: str
    DestinationPort: int
    Protocol: str
    Hostname: str
    ProcessId: int

event SysmonProcessCreate:
    Image: str
    CommandLine: str
    ParentImage: str
    User: str
    Hostname: str
    ProcessId: int
    ParentProcessId: int

# Step 1: Network connection to remote execution ports (SMB, WMI/DCOM, WinRM)
stream RemoteExecConnection = SysmonNetworkConnect
    .where(DestinationPort == 445
        or DestinationPort == 135
        or DestinationPort == 5985
        or DestinationPort == 5986)

# Step 2: Process spawned by ANY remote execution parent
stream RemoteExecChild = SysmonProcessCreate
    .where(ParentImage.ends_with("\services.exe")
        or ParentImage.ends_with("\WmiPrvSE.exe")
        or ParentImage.ends_with("\wsmprovhost.exe")
        or ParentImage.ends_with("\mmc.exe")
        or (ParentImage.ends_with("\svchost.exe")
            and (Image.ends_with("\cmd.exe")
                or Image.ends_with("\powershell.exe")
                or Image.ends_with("\rundll32.exe"))))

# Behavioral sequence: network connection → remote process execution
stream MultiVectorLateralMovement = RemoteExecConnection as conn
    -> RemoteExecChild as child
    .within(2m)
    .emit(
        event_type: "KillChainAlert",
        rule: "multi_vector_lateral_movement",
        mitre: "T1021",
        severity: "critical",
        source_host: conn.Hostname,
        target_ip: conn.DestinationIp,
        target_port: conn.DestinationPort,
        remote_parent: child.ParentImage,
        remote_process: child.Image,
        remote_cmdline: child.CommandLine,
        remote_host: child.Hostname,
        summary: "Lateral movement detected via behavioral multi-vector analysis"
    )
```

One rule. Five techniques. The pattern is structural:

1. A network connection to a remote execution port is observed
2. Followed by a process creation where the parent is a known remote execution host
3. Both events occur within a 2-minute window

When the sixth technique appears, you add one line to the `RemoteExecChild` filter. You don't write a new rule, create a new alert type, or retrain your SOC.

The `svchost.exe` case is the interesting one — because it hosts legitimate services, the VPL rule adds a child-process filter (`cmd.exe`, `powershell.exe`, `rundll32.exe`) to reduce false positives. This is still far broader than the Sigma approach, which ignores `svchost.exe` entirely.

## Proof: The APT29 Connection

The [MORDOR APT29 Day 1 dataset](https://github.com/OTRF/Security-Datasets) includes multiple lateral movement techniques. APT29 operators used WMI-based execution alongside traditional SMB methods during the SolarWinds campaign simulation.

The multi-vector VPL rule catches both techniques in a single pass. The services.exe-only Sigma rule catches only the SMB variant.

## The Evasion Test

We created a synthetic dataset (`lateral_evasion_dataset.jsonl`) simulating WMI-based lateral movement: a network connection to port 135, followed by `WmiPrvSE.exe` spawning `cmd.exe` on the target.

| | Sigma (services.exe parent) | VPL (multi-vector behavioral) |
|---|:---:|:---:|
| PsExec lateral movement (services.exe parent) | 1 alert | 1 alert |
| WMI lateral movement (WmiPrvSE.exe parent) | **0 alerts** | **1 alert** |
| WinRM lateral movement (wsmprovhost.exe parent) | **0 alerts** | **1 alert** |
| DCOM lateral movement (mmc.exe parent) | **0 alerts** | **1 alert** |
| Scheduled Task (svchost.exe parent) | **0 alerts** | **1 alert** |

```bash
# Run the comparison yourself:
varpulis simulate -p examples/security-demo/sigma_comparison/sigma_lateral_smb_only.vpl \
    -e examples/security-demo/sigma_comparison/lateral_evasion_dataset.jsonl -w 1 -v

varpulis simulate -p examples/security-demo/sigma_comparison/vpl_lateral_multi.vpl \
    -e examples/security-demo/sigma_comparison/lateral_evasion_dataset.jsonl -w 1 -v
```

Same data. One rule fires. The other does not.

## What This Means for Your SOC

If your lateral movement detection starts and ends with PsExec rules — even good behavioral ones — you are covering one technique out of five. Attackers performing internal red team exercises routinely use WMI or WinRM because they know most shops only watch SMB.

The fix is not five separate Sigma rules. The fix is a detection model that understands remote execution as a **category of behavior**, not a collection of individual tools and techniques:

1. **Enumerate the remote execution parents.** Windows has a finite set of processes that host remote execution: `services.exe`, `WmiPrvSE.exe`, `wsmprovhost.exe`, `mmc.exe`, `svchost.exe` (Schedule, COM+). Document them.
2. **Detect the behavioral pattern.** Network connection to remote execution port, followed by process creation from a remote execution parent. One rule, all techniques.
3. **Layer the temporal context.** A single process creation from `WmiPrvSE.exe` is noise. A network connection to port 135 followed by `WmiPrvSE.exe` spawning `cmd.exe` within 2 minutes is signal.

Your SIEM stores the logs. A sequence detection engine correlates them. The combination covers doors that neither can cover alone.

## Try It Yourself

```bash
git clone https://github.com/varpulis/varpulis
cd varpulis
cargo build --release --bin varpulis

# Run the WMI evasion test
varpulis simulate \
    -p examples/security-demo/sigma_comparison/sigma_lateral_smb_only.vpl \
    -e examples/security-demo/sigma_comparison/lateral_evasion_dataset.jsonl \
    -w 1 -v

# Run the multi-vector behavioral rule against the same data
varpulis simulate \
    -p examples/security-demo/sigma_comparison/vpl_lateral_multi.vpl \
    -e examples/security-demo/sigma_comparison/lateral_evasion_dataset.jsonl \
    -w 1 -v
```

The evasion dataset, both VPL rules, and the full comparison are in `examples/security-demo/sigma_comparison/`.

---

*Next up: Registry persistence and why path-matching misses the 15 other autostart locations.*

*Built with [Varpulis](https://github.com/varpulis/varpulis) — a dual red/blue kill chain detection engine.*
