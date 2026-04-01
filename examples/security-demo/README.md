# Varpulis Security Demo: APT Kill Chain Detection

Varpulis detects multi-step attack sequences that SIEMs miss — because it reasons
about **temporal patterns**, not isolated events.

This demo processes Sysmon logs from an APT29-style attack and detects every
stage of the kill chain in real-time.

## Demo Recording

Watch the full demo (58 seconds): `asciinema play examples/security-demo/demo.cast`

Or record a fresh one: `asciinema rec -c "bash examples/security-demo/record_demo.sh" demo.cast`

## Quick Start

```bash
# Run all detections (11 tests)
bash examples/security-demo/run_demo.sh

# Or run individually:
varpulis simulate -p examples/security-demo/detect_full_killchain.vpl \
  -e examples/security-demo/data/apt29_full_chain.jsonl -v -w 1
```

## Detection Rules

| Rule | MITRE ATT&CK | Pattern | Type |
|------|:------------:|---------|------|
| [Scripting -> Discovery](detect_scripting_to_discovery.vpl) | T1059.001, T1057 | PowerShell/cscript spawns recon commands | 2-step sequence |
| [Credential Dumping](detect_credential_dumping.vpl) | T1003.001 | LSASS memory access (mimikatz, procdump) | Single-event filter |
| [Lateral Movement](detect_lateral_movement.vpl) | T1021.002 | SMB connection -> remote service execution | 2-step cross-host |
| [Persistence](detect_persistence_registry.vpl) | T1547.001 | Process -> Registry Run key modification | 2-step sequence |
| [Data Staging + Exfil](detect_data_staging_exfil.vpl) | T1560, T1041 | Archive creation -> external network connection | 2-step sequence |
| [Full Kill Chain](detect_full_killchain.vpl) | T1059, T1003, T1021, T1041 | Script -> Credential -> Lateral -> Exfil | **4-step sequence** |

## The Money Shot: Sigma vs VPL

See [`sigma_comparison/`](sigma_comparison/) for the head-to-head demo.

**The setup:** An attacker renames `PsExec.exe` to `svcupdate.exe` and uses it for lateral movement.

| | Sigma (name match) | VPL (behavioral) |
|---|:---:|:---:|
| Renamed PsExec | **MISS** (0 alerts) | **CATCH** (1 alert) |
| Normal PsExec | catch | catch |

VPL detects the **behavior** (SMB -> services.exe -> cmd), not the **filename**.

## How It Works

Varpulis natively ingests Sysmon JSON Lines (MORDOR format). The JSONL parser
auto-detects Sysmon events by `EventID` + `Channel` and maps them to typed events:

| EventID | Event Type | Key Fields |
|---------|-----------|------------|
| 1 | `SysmonProcessCreate` | Image, CommandLine, ParentImage, User |
| 3 | `SysmonNetworkConnect` | SourceIp, DestinationIp, DestinationPort |
| 10 | `SysmonProcessAccess` | SourceImage, TargetImage, GrantedAccess |
| 11 | `SysmonFileCreate` | Image, TargetFilename |
| 13 | `SysmonRegistryValueSet` | Image, TargetObject, Details |

VPL rules express multi-step attack patterns as temporal sequences with
cross-event correlation — something Sigma's single-event model cannot do.

## Alert Format

All rules emit a consistent JSON alert schema:

```json
{
  "event_type": "KillChainAlert",
  "rule": "full_killchain",
  "mitre": "T1059,T1003.001,T1021.002,T1041",
  "severity": "critical",
  "host": "WS01",
  "summary": "Full APT kill chain detected: execution -> credential theft -> lateral movement -> exfiltration"
}
```

## Dataset

Events are hand-crafted Sysmon JSONL based on APT29 (Cozy Bear) TTPs from the
[MORDOR / Security Datasets](https://github.com/OTRF/Security-Datasets) project.
Real MORDOR datasets can be used directly — Varpulis parses the same format.
