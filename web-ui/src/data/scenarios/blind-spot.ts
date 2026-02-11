import type { ScenarioDefinition } from '@/types/scenario'

export const blindSpotScenario: ScenarioDefinition = {
  id: 'blind-spot',
  title: 'The Blind Spot',
  subtitle: 'Skip-Till-Any-Match',
  icon: 'mdi-eye-off',
  color: 'deep-purple',
  summary:
    'Traditional CEP engines use greedy matching that gets blocked by noise events. Varpulis\'s skip-till-any-match finds attacks that other engines miss entirely.',
  vplSource: `stream KillChain = Recon as r
    -> Exploit where target == r.target as e
    -> Exfiltration where source == r.target as x
    .within(1h)
    .emit(
        alert_type: "kill_chain",
        attacker: r.source,
        target: r.target,
        method: e.method,
        destination: x.destination
    )`,
  patterns: [
    {
      name: 'Kill Chain Detection',
      description:
        'Detects a 3-stage attack: Reconnaissance probe, followed by an Exploit on the same target, followed by data Exfiltration from that target. Greedy CEP engines consume the first matching Exploit event even if its predicate fails, killing the partial match.',
      vplSnippet: `Recon as r
    -> Exploit where target == r.target as e
    -> Exfiltration where source == r.target as x
    .within(1h)`,
    },
  ],
  steps: [
    {
      title: 'Clear Signal',
      narration:
        'A clean attack chain with no blocking noise. The attacker probes web-server, exploits it via SQL injection, and exfiltrates data. Both greedy and skip-till-any-match engines detect this — no ambiguity.',
      eventsText: `Recon { source: "attacker-1", target: "web-server", scan_type: "port_scan" }
BATCH 100
Exploit { target: "web-server", method: "sqli", payload: "union_select" }
BATCH 100
Exfiltration { source: "web-server", destination: "evil-drop.com", bytes: 50000 }`,
      expectedAlerts: ['kill_chain'],
      phase: 'attack',
    },
    {
      title: 'The Blind Spot',
      narration:
        'Same attack, but a non-matching Exploit arrives first (targeting db-server, not web-server). A greedy engine consumes this event for the Exploit slot, the predicate fails, and the partial match dies. The real exploit on web-server is never considered. Varpulis skips the non-matching event and finds the real chain.',
      eventsText: `Recon { source: "attacker-1", target: "web-server", scan_type: "port_scan" }
BATCH 100
Exploit { target: "db-server", method: "sqli", payload: "or_1_eq_1" }
BATCH 100
Exploit { target: "web-server", method: "rce", payload: "reverse_shell" }
BATCH 100
Exfiltration { source: "web-server", destination: "evil-drop.com", bytes: 120000 }`,
      expectedAlerts: ['kill_chain'],
      phase: 'attack',
    },
    {
      title: 'Buried in Traffic',
      narration:
        'Realistic network: multiple recon probes, legitimate exploits on other hosts, interleaved noise. Three real attack chains are hidden in the traffic. Two have blocking noise that defeats greedy matching — only the unblocked chain is found by traditional CEP. Varpulis finds all three.',
      eventsText: `Recon { source: "attacker-1", target: "web-server", scan_type: "port_scan" }
BATCH 100
Recon { source: "attacker-2", target: "mail-server", scan_type: "vuln_scan" }
BATCH 100
Exploit { target: "db-server", method: "sqli", payload: "benchmark" }
BATCH 100
Exploit { target: "web-server", method: "rce", payload: "reverse_shell" }
BATCH 100
Recon { source: "attacker-3", target: "db-server", scan_type: "port_scan" }
BATCH 100
Exploit { target: "file-server", method: "path_traversal", payload: "dot_dot" }
BATCH 100
Exploit { target: "mail-server", method: "smtp_inject", payload: "header_inject" }
BATCH 100
Exfiltration { source: "web-server", destination: "evil-drop.com", bytes: 85000 }
BATCH 100
Exploit { target: "web-server", method: "lfi", payload: "etc_passwd" }
BATCH 100
Exfiltration { source: "mail-server", destination: "data-leak.net", bytes: 200000 }
BATCH 100
Exploit { target: "mail-server", method: "rce", payload: "cmd_exec" }
BATCH 100
Exploit { target: "db-server", method: "priv_esc", payload: "udf_exploit" }
BATCH 100
Exfiltration { source: "db-server", destination: "exfil-server.ru", bytes: 500000 }`,
      expectedAlerts: ['kill_chain'],
      phase: 'attack',
    },
    {
      title: 'Clean Traffic',
      narration:
        'Normal network operations: health checks, patch updates, DNS queries. No attack patterns present. Both engines correctly produce zero alerts — confirming low false-positive rates.',
      eventsText: `HealthCheck { source: "monitor", target: "web-server", status: "ok" }
BATCH 100
DnsQuery { source: "workstation-01", domain: "internal.corp", result: "10.0.1.5" }
BATCH 100
PatchUpdate { target: "db-server", patch_id: "CVE-2025-1234", status: "applied" }
BATCH 100
HealthCheck { source: "monitor", target: "mail-server", status: "ok" }
BATCH 100
DnsQuery { source: "workstation-02", domain: "api.corp", result: "10.0.2.1" }`,
      expectedAlerts: [],
      phase: 'negative',
    },
  ],
}
