import type { ScenarioDefinition } from '@/types/scenario'

// 20 hosts in the network
const hosts = [
  'web-server-01', 'web-server-02', 'web-server-03', 'web-server-04',
  'db-server-01', 'db-server-02', 'db-server-03',
  'mail-server-01', 'mail-server-02',
  'workstation-01', 'workstation-02', 'workstation-03', 'workstation-04',
  'workstation-05', 'workstation-06', 'workstation-07', 'workstation-08',
  'workstation-09', 'workstation-10', 'workstation-11',
]

// Normal traffic event types (don't match Recon/Exploit/Exfiltration)
function normalEvent(source: string, target: string): string[] {
  const types = [
    `HealthCheck { source: "${source}", target: "${target}", status: "ok" }`,
    `DnsQuery { source: "${source}", domain: "${target}.corp.internal", result: "10.0.1.${Math.floor(Math.random() * 254 + 1)}" }`,
    `PatchUpdate { target: "${target}", patch_id: "KB${Math.floor(Math.random() * 900000 + 100000)}", status: "applied" }`,
    `PortScan { source: "${source}", target: "${target}", ports: "80,443,8080", result: "filtered" }`,
  ]
  return types
}

// Generate the big haystack with 3 hidden attack chains
function generateHaystackEvents(): string {
  const lines: string[] = []

  // Attack chains are hidden at positions ~50, ~120, ~180
  // Chain 1: attacker-alpha → web-server-02
  // Chain 2: attacker-beta → db-server-01
  // Chain 3: attacker-gamma → mail-server-01

  // === Block 1: Normal traffic (events 1-45) ===
  for (let i = 0; i < 15; i++) {
    const src = hosts[Math.floor((i * 7 + 3) % hosts.length)]
    const dst = hosts[Math.floor((i * 11 + 5) % hosts.length)]
    const evts = normalEvent(src, dst)
    lines.push(evts[i % evts.length])
    lines.push('BATCH 100')
  }

  // More normal
  for (let i = 0; i < 15; i++) {
    const h = hosts[i % hosts.length]
    lines.push(`HealthCheck { source: "monitor", target: "${h}", status: "ok" }`)
    lines.push('BATCH 100')
  }

  for (let i = 0; i < 15; i++) {
    const src = hosts[(i + 5) % hosts.length]
    lines.push(`DnsQuery { source: "${src}", domain: "service-${i}.corp.internal", result: "10.0.${i}.1" }`)
    lines.push('BATCH 100')
  }

  // === Attack Chain 1: Recon at ~event 46 ===
  lines.push('Recon { source: "attacker-alpha", target: "web-server-02", scan_type: "vuln_scan" }')
  lines.push('BATCH 100')

  // More noise
  for (let i = 0; i < 20; i++) {
    const h = hosts[(i + 2) % hosts.length]
    lines.push(`PatchUpdate { target: "${h}", patch_id: "CVE-2025-${1000 + i}", status: "applied" }`)
    lines.push('BATCH 100')
  }

  // === Attack Chain 2: Recon at ~event 67 ===
  lines.push('Recon { source: "attacker-beta", target: "db-server-01", scan_type: "port_scan" }')
  lines.push('BATCH 100')

  // More noise
  for (let i = 0; i < 15; i++) {
    const src = hosts[(i + 8) % hosts.length]
    const dst = hosts[(i + 3) % hosts.length]
    lines.push(`HealthCheck { source: "${src}", target: "${dst}", status: "ok" }`)
    lines.push('BATCH 100')
  }

  // === Attack Chain 1: Exploit at ~event 83 ===
  lines.push('Exploit { target: "web-server-02", method: "rce", payload: "reverse_shell_443" }')
  lines.push('BATCH 100')

  // More noise
  for (let i = 0; i < 10; i++) {
    const src = hosts[(i + 10) % hosts.length]
    lines.push(`DnsQuery { source: "${src}", domain: "update.microsoft.com", result: "13.107.4.52" }`)
    lines.push('BATCH 100')
  }

  // === Attack Chain 3: Recon at ~event 94 ===
  lines.push('Recon { source: "attacker-gamma", target: "mail-server-01", scan_type: "smtp_enum" }')
  lines.push('BATCH 100')

  // === Attack Chain 2: Exploit at ~event 95 ===
  lines.push('Exploit { target: "db-server-01", method: "sqli", payload: "union_select_dump" }')
  lines.push('BATCH 100')

  // More noise
  for (let i = 0; i < 20; i++) {
    const h = hosts[i % hosts.length]
    lines.push(`HealthCheck { source: "monitor-2", target: "${h}", status: "ok" }`)
    lines.push('BATCH 100')
  }

  // === Attack Chain 1: Exfiltration at ~event 116 ===
  lines.push('Exfiltration { source: "web-server-02", destination: "c2.evil-corp.io", bytes: 150000 }')
  lines.push('BATCH 100')

  // More noise
  for (let i = 0; i < 15; i++) {
    const src = hosts[(i + 6) % hosts.length]
    lines.push(`DnsQuery { source: "${src}", domain: "slack.com", result: "34.237.231.8" }`)
    lines.push('BATCH 100')
  }

  // === Attack Chain 3: Exploit at ~event 132 ===
  lines.push('Exploit { target: "mail-server-01", method: "smtp_inject", payload: "header_injection" }')
  lines.push('BATCH 100')

  // More noise
  for (let i = 0; i < 20; i++) {
    const h = hosts[(i + 4) % hosts.length]
    lines.push(`PatchUpdate { target: "${h}", patch_id: "RHSA-2025-${2000 + i}", status: "pending" }`)
    lines.push('BATCH 100')
  }

  // === Attack Chain 2: Exfiltration at ~event 153 ===
  lines.push('Exfiltration { source: "db-server-01", destination: "data-dump.darknet.ru", bytes: 800000 }')
  lines.push('BATCH 100')

  // More noise
  for (let i = 0; i < 25; i++) {
    const src = hosts[(i + 1) % hosts.length]
    const dst = hosts[(i + 9) % hosts.length]
    lines.push(`HealthCheck { source: "${src}", target: "${dst}", status: "ok" }`)
    lines.push('BATCH 100')
  }

  // === Attack Chain 3: Exfiltration at ~event 179 ===
  lines.push('Exfiltration { source: "mail-server-01", destination: "leak-bucket.onion.ly", bytes: 340000 }')
  lines.push('BATCH 100')

  // Final noise block
  for (let i = 0; i < 20; i++) {
    const h = hosts[i % hosts.length]
    lines.push(`DnsQuery { source: "${h}", domain: "time.google.com", result: "216.239.35.0" }`)
    if (i < 19) lines.push('BATCH 100')
  }

  return lines.join('\n')
}

// Generate zoom-in events showing just the 3 attack chains clearly
function generateZoomEvents(): string {
  return `Recon { source: "attacker-alpha", target: "web-server-02", scan_type: "vuln_scan" }
BATCH 100
Exploit { target: "web-server-02", method: "rce", payload: "reverse_shell_443" }
BATCH 100
Exfiltration { source: "web-server-02", destination: "c2.evil-corp.io", bytes: 150000 }
BATCH 100
Recon { source: "attacker-beta", target: "db-server-01", scan_type: "port_scan" }
BATCH 100
Exploit { target: "db-server-01", method: "sqli", payload: "union_select_dump" }
BATCH 100
Exfiltration { source: "db-server-01", destination: "data-dump.darknet.ru", bytes: 800000 }
BATCH 100
Recon { source: "attacker-gamma", target: "mail-server-01", scan_type: "smtp_enum" }
BATCH 100
Exploit { target: "mail-server-01", method: "smtp_inject", payload: "header_injection" }
BATCH 100
Exfiltration { source: "mail-server-01", destination: "leak-bucket.onion.ly", bytes: 340000 }`
}

export const haystackScenario: ScenarioDefinition = {
  id: 'haystack',
  title: 'Needle in a Haystack',
  subtitle: 'Volume Processing',
  icon: 'mdi-magnify-scan',
  color: 'teal',
  summary:
    '200+ events from 20 hosts. Three attack chains are buried in 90% legitimate traffic. Varpulis finds all three in milliseconds.',
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
        'Same Recon → Exploit → Exfiltration pattern, but processing 200+ events from a realistic enterprise network. Demonstrates Varpulis processing high event volumes and finding needles in haystacks.',
      vplSnippet: `Recon as r
    -> Exploit where target == r.target as e
    -> Exfiltration where source == r.target as x
    .within(1h)`,
    },
  ],
  steps: [
    {
      title: 'The Haystack',
      narration:
        '200+ events flood in from 20 hosts: health checks, DNS queries, patch updates, and port scans. Hidden in this traffic are 3 kill chains targeting web-server-02, db-server-01, and mail-server-01. Watch the host grid light up as Varpulis finds the attacks in milliseconds.',
      eventsText: generateHaystackEvents(),
      expectedAlerts: ['kill_chain'],
      phase: 'attack',
    },
    {
      title: 'Zoom In',
      narration:
        'Now isolating just the 9 events that form the 3 attack chains. Each chain follows the same Recon → Exploit → Exfiltration pattern on a different target server. This confirms the detections from the haystack are real.',
      eventsText: generateZoomEvents(),
      expectedAlerts: ['kill_chain'],
      phase: 'attack',
    },
  ],
}

export { hosts as haystackHosts }
