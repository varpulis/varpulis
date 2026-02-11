# Cyber Kill Chain Detection

## Executive Summary

The average dwell time for an APT attack is 21 days. Varpulis detects multi-stage intrusions -- brute force, lateral movement, privilege escalation, data exfiltration -- as they unfold, correlating events across the kill chain in real-time rather than discovering them weeks later in log reviews.

## The Business Problem

Modern cyberattacks unfold in stages. An attacker first gains access (brute force, phishing), then moves laterally to high-value targets (file servers, databases), escalates privileges, and finally exfiltrates data. Each stage generates log events, but these events are scattered across different systems: firewalls, authentication servers, endpoint agents, DNS resolvers.

Security Operations Centers (SOCs) drown in individual alerts. A failed login attempt generates an alert. A new network connection generates an alert. Each is triaged independently. The connection between them -- that the same attacker who brute-forced a password is now connecting to the file server -- is only discovered during manual incident investigation, often days or weeks later.

The average time from initial compromise to detection is 21 days. That is 21 days of unrestricted access.

## How It Works

Varpulis correlates events across the entire kill chain in a single pattern:

```
Brute Force + Lateral Movement:

  t=0       t=2s       t=4s       t=6s       t=10s
   │         │          │          │           │
   ▼         ▼          ▼          ▼           ▼
┌──────┐ ┌──────┐ ┌──────┐ ┌─────────┐ ┌───────────┐
│Failed│►│Failed│►│Failed│►│Successful│►│ Network   │
│Login │ │Login │ │Login │ │ Login   │ │Connect:445│
└──────┘ └──────┘ └──────┘ └─────────┘ └───────────┘
 admin    root     admin     admin      web-server →
                                        file-server

◄── Kleene captures ALL ──►            ALERT!
    failed attempts

Complete forensic trail in one alert
```

## What Varpulis Detects

### Brute Force + Lateral Movement
An attacker tries 3 passwords against a server. One works. Within minutes, they connect to a file server on port 445 (SMB). That is not an employee -- that is a breach in progress. Varpulis captures every failed attempt (Kleene closure), the successful login, and the lateral movement as a single correlated alert.

### DNS Exfiltration
A compromised machine starts making DNS queries to unusual domains in rapid succession. Normal computers do not do that. The attacker is smuggling stolen data out through DNS queries. Varpulis detects the burst pattern -- multiple queries from the same host to the same domain family within 5 minutes.

### Privilege Escalation
A regular user process runs `sudo`, then a root process appears on the same host. That escalation path -- from user to root in 3 seconds -- matches the textbook attack pattern. Varpulis connects the user process, the elevation event, and the root process into a single alert.

## Why Competitors Miss This

SIEM tools correlate events using flat rules: "if failed_logins > 50 in 10 minutes, alert." That catches the brute force, but not what happens *next*. The attacker's lateral movement to port 445 is a separate event in a separate log -- and traditional SIEMs do not connect the dots.

Varpulis tracks the entire kill chain as a single sequence: brute force followed by successful login followed by lateral movement. Every failed login attempt is captured (Kleene closure), providing forensic-grade evidence for incident response.

| Capability | SIEM Rules | Varpulis |
|-----------|-----------|----------|
| Brute force detection | Yes (threshold) | Yes (Kleene) |
| Post-breach correlation | No | Yes (sequence) |
| Lateral movement linking | Manual | Automatic |
| Complete failed login trail | Count only | Every attempt |
| Time to detection | Days to weeks | Seconds |

## Measurable Impact

- **Kill chain detection in seconds** -- not days of dwell time
- **Complete forensic trail** of every attack step captured by Kleene closure
- **3x less infrastructure cost** than traditional SIEM stacks (10 MB vs 85 MB per instance)
- **Zero manual correlation** -- the alert includes every event in the chain

## Live Demo Walkthrough

```bash
cargo test -p varpulis-runtime --test cxo_scenario_tests cxo_cyber
```

**Input events:**
1. Three failed logins to web-server-01 from attacker IP
2. Successful login to web-server-01
3. Network connection from web-server-01 to file-server-02 (port 445/SMB)
4. Three DNS queries from workstation-15 to evil-c2.com subdomains
5. User process on dev-box-03, then sudo, then root process reading /etc/shadow

**Alerts generated:**
- `brute_force_lateral` -- web-server-01, attacker IP 185.220.100.42, lateral to file-server-02
- `dns_exfiltration` -- workstation-15, evil-c2.com domain family
- `privilege_escalation` -- dev-box-03, user jdoe, root command: cat /etc/shadow

<details>
<summary>Technical Appendix</summary>

### VPL Pattern: Brute Force + Lateral Movement
```
stream BruteForceLateral = FailedLogin as first_fail
    -> all FailedLogin where target_host == first_fail.target_host as fails
    -> SuccessfulLogin where target_host == first_fail.target_host as success
    -> NetworkConnection where source_host == first_fail.target_host as lateral
    .within(30m)
    .emit(alert_type: "brute_force_lateral", ...)
```

The `-> all FailedLogin` captures every failed attempt, not just a count. This preserves the complete forensic trail: which usernames were tried, at what times, from which IPs.

### Test Files
- Rules: `tests/scenarios/cxo_cyber_threat.vpl`
- Events: `tests/scenarios/cxo_cyber_threat.evt`
</details>
