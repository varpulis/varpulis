# Varpulis Security -- MITRE ATT&CK Detection Library

**Temporal threat detection with Varpulis. Like Sigma, but for attack sequences.**

---

## Why Sequences Matter

Traditional SIEM rules (Sigma, YARA, Snort) detect **single events**: a suspicious login, a process spawn, an outbound connection. But real attacks are **chains** -- a phishing email leads to a download, which spawns a process, which connects to C2, which moves laterally.

Varpulis detects the **chain itself** as a first-class concept:

| Approach | What it detects | Example |
|----------|----------------|---------|
| **Sigma** | Single event: "Failed login detected" | `EventID: 4625` |
| **Varpulis** | Temporal sequence: "5 failed logins then success from same IP within 10m" | `FailedLogin -> ... -> SuccessfulLogin .within(10m)` |

A single failed login is noise. Five failed logins followed by a success from the same IP within 10 minutes is a brute force attack. Varpulis knows the difference.

## Rules by MITRE ATT&CK Tactic

| Tactic | Technique | Rule File | Severity | Description |
|--------|-----------|-----------|----------|-------------|
| Initial Access | T1078 | `rules/initial-access/T1078-brute-force-success.vpl` | High | 5+ failed logins then success from same IP |
| Initial Access | T1566 | `rules/initial-access/T1566-phishing-chain.vpl` | Critical | Email received, link clicked, file downloaded, process started |
| Lateral Movement | T1021 | `rules/lateral-movement/T1021-remote-service-hopping.vpl` | High | RDP/SSH hop from host A to host B |
| Lateral Movement | T1075 | `rules/lateral-movement/T1075-pass-the-hash.vpl` | Critical | Failed NTLM then success with different creds from same source |
| Credential Access | T1110 | `rules/credential-access/T1110-password-spray.vpl` | High | Failed logins to 5+ accounts from same IP |
| Credential Access | T1003 | `rules/credential-access/T1003-credential-dump-after-escalation.vpl` | Critical | Privilege escalation then credential dump tool |
| Exfiltration | T1048 | `rules/exfiltration/T1048-data-staging-and-exfil.vpl` | Critical | File access, compression, outbound transfer |
| Exfiltration | T1567 | `rules/exfiltration/T1567-cloud-exfil.vpl` | High | Multiple large uploads to cloud storage |
| Execution | T1059 | `rules/execution/T1059-command-chain.vpl` | High | Shell spawn, network connection, file write |
| Execution | T1053 | `rules/execution/T1053-scheduled-task-persistence.vpl` | Medium | Process creates scheduled task, task executes |

## Quick Start

### 1. Run a single rule against test events

```bash
varpulis simulate \
  -p integrations/varpulis-security/rules/initial-access/T1078-brute-force-success.vpl \
  -e integrations/varpulis-security/tests/T1078-brute-force.evt \
  -v -w 1
```

### 2. Run all rules against your SIEM feed

Point each rule at your Elasticsearch or Kafka connector. See `connectors/elastic-siem.vpl` for an example configuration, then import it from any rule:

```vpl
import "connectors/elastic-siem.vpl"

stream AuthEvents = AuthEvent.from(ElasticSIEM, topic: "winlogbeat-*")
```

### 3. Deploy to production

```bash
# Run as a long-lived pipeline with Kafka input
varpulis run -p rules/initial-access/T1078-brute-force-success.vpl
```

## Architecture

```
varpulis-security/
  rules/
    initial-access/       # TA0001 - Initial Access
    lateral-movement/     # TA0008 - Lateral Movement
    credential-access/    # TA0006 - Credential Access
    exfiltration/         # TA0010 - Exfiltration
    execution/            # TA0002 - Execution
  tests/                  # .evt files for each rule
  connectors/             # Reusable connector configs
  README.md
```

Each `.vpl` rule is self-contained with:
- MITRE ATT&CK technique ID and URL
- Event type declarations
- Stream definitions with temporal sequence logic
- Severity rating and recommended data sources
- Test event examples in comments

## Data Source Mapping

| VPL Event Type | Windows Event Log | Linux | Cloud |
|----------------|-------------------|-------|-------|
| `AuthEvent` | EventID 4624/4625 | `/var/log/auth.log` | CloudTrail `ConsoleLogin` |
| `ProcessEvent` | EventID 4688, Sysmon 1 | `auditd execve` | -- |
| `NetworkEvent` | Sysmon 3 | `conntrack` / Zeek | VPC Flow Logs |
| `FileEvent` | Sysmon 11/23 | `auditd open` | S3 access logs |
| `RemoteServiceEvent` | EventID 4624 Type 10 | `sshd` logs | -- |

## Contributing

1. Each rule must have a corresponding `.evt` test file in `tests/`
2. Test events must include both positive cases (should trigger) and negative cases (should not)
3. All VPL must be valid syntax -- test with `varpulis simulate` before submitting
4. Include MITRE ATT&CK technique ID, tactic, and URL in the rule header

## Links

- [Varpulis Documentation](https://varpulis.dev/docs)
- [VPL Language Reference](https://varpulis.dev/docs/language)
- [SASE+ Pattern Matching](https://varpulis.dev/docs/sase-patterns)
- [MITRE ATT&CK Framework](https://attack.mitre.org/)
- [Sigma Rules](https://github.com/SigmaHQ/sigma) (single-event detection -- complementary to Varpulis)
