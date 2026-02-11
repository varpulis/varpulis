import type { ScenarioDefinition } from '@/types/scenario'

export interface MitreRule {
  name: string
  mitreId: string
  alertType: string
}

export const mitreRules: MitreRule[] = [
  { name: 'Brute Force', mitreId: 'T1110', alertType: 'brute_force' },
  { name: 'Credential Dumping', mitreId: 'T1003', alertType: 'credential_dump' },
  { name: 'Lateral Movement', mitreId: 'T1021', alertType: 'lateral_movement' },
  { name: 'Data Exfiltration', mitreId: 'T1041', alertType: 'data_exfiltration' },
  { name: 'C2 Communication', mitreId: 'T1071', alertType: 'c2_communication' },
  { name: 'Privilege Escalation', mitreId: 'T1068', alertType: 'privilege_escalation' },
  { name: 'Defense Evasion', mitreId: 'T1562', alertType: 'defense_evasion' },
  { name: 'Persistence', mitreId: 'T1053', alertType: 'persistence' },
  { name: 'Discovery', mitreId: 'T1046', alertType: 'discovery' },
  { name: 'Impact', mitreId: 'T1486', alertType: 'impact' },
]

const vplSource = `stream BruteForce = FailedLogin as f1
    -> all FailedLogin where target == f1.target as fails
    -> SuccessLogin where target == f1.target as success
    .within(10m)
    .emit(
        alert_type: "brute_force",
        target: f1.target,
        attacker: f1.source,
        rule: "T1110"
    )

stream CredentialDump = ProcessCreate as proc
    -> FileAccess where path == "lsass.dmp" and proc.name == "mimikatz" as dump
    .within(5m)
    .emit(
        alert_type: "credential_dump",
        target: proc.host,
        tool: proc.name,
        rule: "T1003"
    )

stream LateralMovement = AuthSuccess as auth
    -> RemoteExec where source == auth.host as exec
    -> AuthSuccess where host == exec.target as landing
    .within(15m)
    .emit(
        alert_type: "lateral_movement",
        source: auth.host,
        target: exec.target,
        rule: "T1021"
    )

stream DataExfiltration = LargeFileRead as read
    -> DnsQuery where host == read.host as dns
    -> OutboundTransfer where host == read.host as transfer
    .within(30m)
    .emit(
        alert_type: "data_exfiltration",
        target: read.host,
        destination: transfer.destination,
        rule: "T1041"
    )

stream C2Communication = DnsQuery as beacon1
    -> DnsQuery where host == beacon1.host and beacon1.domain_length > 50 as beacon2
    -> DnsQuery where host == beacon1.host as beacon3
    .within(5m)
    .emit(
        alert_type: "c2_communication",
        target: beacon1.host,
        domain: beacon1.domain,
        rule: "T1071"
    )

stream PrivilegeEscalation = ProcessCreate as proc
    -> PrivilegeChange where host == proc.host and proc.user == "normal" as priv
    -> ProcessCreate where host == proc.host as elevated
    .within(5m)
    .emit(
        alert_type: "privilege_escalation",
        target: proc.host,
        method: priv.method,
        rule: "T1068"
    )

stream DefenseEvasion = ServiceStop as stop
    -> LogClear where host == stop.host and stop.service == "antivirus" as clear
    .within(10m)
    .emit(
        alert_type: "defense_evasion",
        target: stop.host,
        service: stop.service,
        rule: "T1562"
    )

stream Persistence = ScheduledTaskCreate as task
    -> RegistryModify where host == task.host as reg
    .within(15m)
    .emit(
        alert_type: "persistence",
        target: task.host,
        task_name: task.name,
        rule: "T1053"
    )

stream Discovery = PortScan as scan
    -> ServiceEnum where source == scan.source as enum_svc
    -> UserEnum where source == scan.source as enum_user
    .within(10m)
    .emit(
        alert_type: "discovery",
        source: scan.source,
        target: scan.target,
        rule: "T1046"
    )

stream Impact = FileEncrypt as enc1
    -> all FileEncrypt where host == enc1.host as encryptions
    -> RansomNote where host == enc1.host as note
    .within(30m)
    .emit(
        alert_type: "impact",
        target: enc1.host,
        ransom_demand: note.demand,
        rule: "T1486"
    )`

// Step 1: events that trigger only 1 rule (Brute Force)
const step1Events = `FailedLogin { source: "attacker-1", target: "dc-01", user: "admin" }
BATCH 100
FailedLogin { source: "attacker-1", target: "dc-01", user: "admin" }
BATCH 100
FailedLogin { source: "attacker-1", target: "dc-01", user: "admin" }
BATCH 100
SuccessLogin { source: "attacker-1", target: "dc-01", user: "admin" }`

// Step 2: events that trigger 5 rules
const step2Events = `FailedLogin { source: "attacker-2", target: "web-01", user: "root" }
BATCH 100
FailedLogin { source: "attacker-2", target: "web-01", user: "root" }
BATCH 100
SuccessLogin { source: "attacker-2", target: "web-01", user: "root" }
BATCH 100
ProcessCreate { host: "web-01", name: "mimikatz", user: "root", pid: 4567 }
BATCH 100
FileAccess { host: "web-01", path: "lsass.dmp", user: "root" }
BATCH 100
AuthSuccess { host: "web-01", user: "root", method: "pass_the_hash" }
BATCH 100
RemoteExec { source: "web-01", target: "db-01", method: "psexec" }
BATCH 100
AuthSuccess { host: "db-01", user: "root", method: "token" }
BATCH 100
ServiceStop { host: "db-01", service: "antivirus", user: "root" }
BATCH 100
LogClear { host: "db-01", log: "security", user: "root" }
BATCH 100
ScheduledTaskCreate { host: "db-01", name: "updater.exe", schedule: "daily", user: "root" }
BATCH 100
RegistryModify { host: "db-01", key: "HKLM\\Software\\Run", value: "updater.exe" }`

// Step 3: events that trigger all 10 rules
const step3Events = `FailedLogin { source: "apt-group", target: "dc-02", user: "svc_admin" }
BATCH 100
FailedLogin { source: "apt-group", target: "dc-02", user: "svc_admin" }
BATCH 100
SuccessLogin { source: "apt-group", target: "dc-02", user: "svc_admin" }
BATCH 100
ProcessCreate { host: "dc-02", name: "mimikatz", user: "svc_admin", pid: 8901 }
BATCH 100
FileAccess { host: "dc-02", path: "lsass.dmp", user: "svc_admin" }
BATCH 100
AuthSuccess { host: "dc-02", user: "svc_admin", method: "kerberos" }
BATCH 100
RemoteExec { source: "dc-02", target: "file-01", method: "wmi" }
BATCH 100
AuthSuccess { host: "file-01", user: "svc_admin", method: "token" }
BATCH 100
LargeFileRead { host: "file-01", path: "/data/secrets.db", size: 50000000 }
BATCH 100
DnsQuery { host: "file-01", domain: "cdn-update-service.com", domain_length: 24, result: "185.220.101.1" }
BATCH 100
OutboundTransfer { host: "file-01", destination: "185.220.101.1", bytes: 50000000, protocol: "https" }
BATCH 100
DnsQuery { host: "dc-02", domain: "aHR0cHM6Ly9tYWx3YXJlLXVwZGF0ZS5jb20vY2hlY2staW4.evil.com", domain_length: 65, result: "203.0.113.50" }
BATCH 100
DnsQuery { host: "dc-02", domain: "YmVhY29uLWNoZWNrLWluLXN0YXR1cy1yZXBvcnQ.evil.com", domain_length: 52, result: "203.0.113.50" }
BATCH 100
DnsQuery { host: "dc-02", domain: "c3RhZ2luZy1kYXRhLWV4ZmlsdHJhdGlvbi1jaGVjaw.evil.com", domain_length: 55, result: "203.0.113.50" }
BATCH 100
ProcessCreate { host: "dc-02", name: "cmd.exe", user: "normal", pid: 1234 }
BATCH 100
PrivilegeChange { host: "dc-02", method: "token_impersonation", from_user: "normal", to_user: "SYSTEM" }
BATCH 100
ProcessCreate { host: "dc-02", name: "powershell.exe", user: "SYSTEM", pid: 5678 }
BATCH 100
ServiceStop { host: "file-01", service: "antivirus", user: "svc_admin" }
BATCH 100
LogClear { host: "file-01", log: "security", user: "svc_admin" }
BATCH 100
ScheduledTaskCreate { host: "file-01", name: "svc_update.exe", schedule: "hourly", user: "svc_admin" }
BATCH 100
RegistryModify { host: "file-01", key: "HKLM\\Software\\Run", value: "svc_update.exe" }
BATCH 100
PortScan { source: "dc-02", target: "10.0.0.0/24", ports: "22,80,443,445,3389" }
BATCH 100
ServiceEnum { source: "dc-02", target: "10.0.0.0/24", services_found: 47 }
BATCH 100
UserEnum { source: "dc-02", target: "10.0.0.0/24", users_found: 156 }
BATCH 100
FileEncrypt { host: "file-01", path: "/data/reports", algorithm: "aes-256", files_affected: 1500 }
BATCH 100
FileEncrypt { host: "file-01", path: "/data/backups", algorithm: "aes-256", files_affected: 800 }
BATCH 100
RansomNote { host: "file-01", demand: "50 BTC", contact: "dark-payment@onion.ly" }`

// Step 4: large batch showing throughput stays flat
function generateThroughputBatch(): string {
  const lines: string[] = []
  // Generate events that trigger multiple rules across many hosts
  for (let i = 0; i < 10; i++) {
    const host = `endpoint-${String(i).padStart(2, '0')}`
    lines.push(`FailedLogin { source: "scanner-${i}", target: "${host}", user: "admin" }`)
    lines.push('BATCH 50')
    lines.push(`FailedLogin { source: "scanner-${i}", target: "${host}", user: "admin" }`)
    lines.push('BATCH 50')
    lines.push(`SuccessLogin { source: "scanner-${i}", target: "${host}", user: "admin" }`)
    lines.push('BATCH 50')
    lines.push(`PortScan { source: "${host}", target: "10.0.${i}.0/24", ports: "22,80,443" }`)
    lines.push('BATCH 50')
    lines.push(`ServiceEnum { source: "${host}", target: "10.0.${i}.0/24", services_found: ${10 + i} }`)
    lines.push('BATCH 50')
    lines.push(`UserEnum { source: "${host}", target: "10.0.${i}.0/24", users_found: ${20 + i} }`)
    if (i < 9) lines.push('BATCH 50')
  }
  return lines.join('\n')
}

export const socScaleScenario: ScenarioDefinition = {
  id: 'soc-scale',
  title: 'SOC at Scale',
  subtitle: 'Multi-Rule Sharing',
  icon: 'mdi-shield-lock',
  color: 'indigo',
  summary:
    '10 MITRE ATT&CK detection rules running simultaneously. Hamlet sharing means near-zero overhead vs 1 rule — processing time stays flat as rules scale.',
  vplSource,
  patterns: [
    {
      name: 'Brute Force (T1110)',
      description: 'Multiple failed logins followed by success on the same target.',
      vplSnippet: `FailedLogin as f1
    -> all FailedLogin where target == f1.target
    -> SuccessLogin where target == f1.target
    .within(10m)`,
    },
    {
      name: 'Lateral Movement (T1021)',
      description: 'Authentication, remote execution, then landing on a new host.',
      vplSnippet: `AuthSuccess as auth
    -> RemoteExec where source == auth.host
    -> AuthSuccess where host == exec.target
    .within(15m)`,
    },
    {
      name: 'Impact / Ransomware (T1486)',
      description: 'File encryption followed by ransom note deployment.',
      vplSnippet: `FileEncrypt as enc1
    -> all FileEncrypt where host == enc1.host
    -> RansomNote where host == enc1.host
    .within(30m)`,
    },
  ],
  steps: [
    {
      title: '1 Rule Triggered',
      narration:
        'Inject events that trigger only the Brute Force rule (T1110). One attacker runs a password spray against dc-01. Only 1 of 10 rules fires. Watch the rule matrix — processing time baseline established.',
      eventsText: step1Events,
      expectedAlerts: ['brute_force'],
      phase: 'attack',
    },
    {
      title: '5 Rules Triggered',
      narration:
        'A more sophisticated attack: brute force entry, credential dumping, lateral movement, defense evasion, and persistence. 5 of 10 rules fire simultaneously. Processing time should be similar to 1 rule thanks to Hamlet sharing.',
      eventsText: step2Events,
      expectedAlerts: ['brute_force', 'credential_dump', 'lateral_movement', 'defense_evasion', 'persistence'],
      phase: 'attack',
    },
    {
      title: 'All 10 Rules',
      narration:
        'Full APT campaign: all 10 MITRE ATT&CK techniques used. Every detection rule triggers. Despite 10x the pattern complexity, processing time stays nearly flat — Hamlet sharing optimizes overlapping event sequences.',
      eventsText: step3Events,
      expectedAlerts: ['brute_force', 'credential_dump', 'lateral_movement', 'data_exfiltration', 'c2_communication', 'privilege_escalation', 'defense_evasion', 'persistence', 'discovery', 'impact'],
      phase: 'attack',
    },
    {
      title: 'Throughput Test',
      narration:
        'Batch of 60 events across 10 hosts testing throughput consistency. Multiple brute force and discovery rules fire in parallel. The key metric: events/second remains consistent regardless of how many rules are active.',
      eventsText: generateThroughputBatch(),
      expectedAlerts: ['brute_force', 'discovery'],
      phase: 'attack',
    },
  ],
}
