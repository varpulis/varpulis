# Sigma vs VPL: The Blind Spot Demo

## The Scenario

An attacker uses PsExec for lateral movement — but renames the binary to `svcupdate.exe`.

## What Sigma Sees

The standard Sigma rule ([`sigma_psexec.yml`](sigma_psexec.yml)) matches on:
```yaml
Image|endswith: '\PsExec.exe'
```

**Result: silence.** The binary is renamed. Sigma doesn't trigger.

## What VPL Sees

The VPL behavioral rule ([`vpl_behavioral.vpl`](vpl_behavioral.vpl)) matches on the **attack pattern**:
```
SMB connection (port 445) → process spawned by services.exe → command execution
```

**Result: alert.** The behavior is identical regardless of the binary name.

## Run It

```bash
# Sigma-style rule: 0 alerts (the attack is invisible)
varpulis simulate -p sigma_only.vpl -e evasion_dataset.jsonl -v -w 1

# VPL behavioral rule: 1 alert (the attack is caught)
varpulis simulate -p vpl_behavioral.vpl -e evasion_dataset.jsonl -v -w 1

# Sanity check: both catch unmodified PsExec
varpulis simulate -p sigma_only.vpl -e normal_dataset.jsonl -v -w 1
varpulis simulate -p vpl_behavioral.vpl -e normal_dataset.jsonl -v -w 1
```

## Why Sigma Is Blind Here

| Approach | Detects renamed PsExec? | Detects unknown tools? | Temporal correlation? |
|----------|:-----------------------:|:----------------------:|:--------------------:|
| Sigma (name match) | No | No | No |
| VPL (behavioral sequence) | **Yes** | **Yes** | **Yes** |

The VPL rule catches **any tool** that uses the SMB service pipe mechanism — PsExec, Impacket's smbexec, custom C2 frameworks, or tools that don't exist yet. Sigma can only catch what it already knows about.
