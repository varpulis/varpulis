import type { ScenarioDefinition } from '@/types/scenario'

export const cyberThreatScenario: ScenarioDefinition = {
  id: 'cyber-threat',
  title: 'Cyber Kill Chain Detection',
  subtitle: 'SOC / Cybersecurity',
  icon: 'mdi-shield-alert',
  color: 'red',
  summary:
    'Detect multi-stage attacks in real-time: brute force followed by lateral movement, DNS exfiltration tunnels, and privilege escalation.',
  vplSource: `stream BruteForceLateral = FailedLogin as first_fail
    -> all FailedLogin where target_host == first_fail.target_host as fails
    -> SuccessfulLogin where target_host == first_fail.target_host as success
    -> NetworkConnection where source_host == first_fail.target_host as lateral
    .within(30m)
    .emit(
        alert_type: "brute_force_lateral",
        target_host: first_fail.target_host,
        attacker_ip: first_fail.source_ip,
        lateral_target: lateral.dest_host
    )

stream DnsExfiltration = DnsQuery as first
    -> all DnsQuery where source_host == first.source_host as queries
    .within(5m)
    .emit(
        alert_type: "dns_exfiltration",
        source_host: first.source_host,
        domain: first.domain
    )

stream PrivilegeEscalation = UserProcess as user_proc
    -> ElevationEvent where host == user_proc.host as elevation
    -> RootProcess where host == user_proc.host as root_proc
    .within(10m)
    .emit(
        alert_type: "privilege_escalation",
        host: user_proc.host,
        user: user_proc.username,
        root_command: root_proc.command
    )`,
  patterns: [
    {
      name: 'Brute Force + Lateral Movement',
      description:
        'Detects repeated failed logins followed by a successful login on the same host, then a network connection from that host to another target. With 8 failed logins (1 first + 7 Kleene), Varpulis finds 127 attack subsequences vs 1 for traditional CEP.',
      vplSnippet: `FailedLogin as first_fail
    -> all FailedLogin where target_host == first_fail.target_host
    -> SuccessfulLogin where target_host == first_fail.target_host
    -> NetworkConnection where source_host == first_fail.target_host
    .within(30m)`,
    },
    {
      name: 'DNS Exfiltration',
      description:
        'Detects multiple DNS queries from the same host within 5 minutes. Attackers use DNS TXT records to tunnel data out, bypassing firewalls. With 6 queries (1 first + 5 Kleene), Varpulis finds 31 subsequences.',
      vplSnippet: `DnsQuery as first
    -> all DnsQuery where source_host == first.source_host
    .within(5m)`,
    },
    {
      name: 'Privilege Escalation',
      description:
        'Detects a user process followed by a privilege elevation event and then a root process on the same host within 10 minutes. Indicates unauthorized privilege gain.',
      vplSnippet: `UserProcess as user_proc
    -> ElevationEvent where host == user_proc.host
    -> RootProcess where host == user_proc.host
    .within(10m)`,
    },
  ],
  steps: [
    {
      title: 'Brute Force Reconnaissance (Kleene Star)',
      narration:
        'An attacker from IP 185.220.100.42 hammers web-server-01 with 8 failed login attempts using different usernames, interleaved with 3 legitimate logins to other hosts. Then the attacker succeeds and pivots laterally to file-server-02 over SMB. With 8 failed logins (1 first + 7 Kleene), Varpulis enumerates 127 attack subsequences.',
      eventsText: `FailedLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "admin" }
BATCH 1000
FailedLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "root" }
BATCH 1000
SuccessfulLogin { target_host: "mail-server-01", source_ip: "10.0.1.50", username: "jsmith" }
BATCH 1000
FailedLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "administrator" }
BATCH 1000
FailedLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "webadmin" }
BATCH 1000
SuccessfulLogin { target_host: "db-server-03", source_ip: "10.0.2.20", username: "dbadmin" }
BATCH 1000
FailedLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "sysadmin" }
BATCH 1000
FailedLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "test" }
BATCH 1000
FailedLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "deploy" }
BATCH 1000
SuccessfulLogin { target_host: "dev-box-07", source_ip: "10.0.3.10", username: "dev_user" }
BATCH 1000
FailedLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "backup" }
BATCH 3000
SuccessfulLogin { target_host: "web-server-01", source_ip: "185.220.100.42", username: "admin" }
BATCH 5000
NetworkConnection { source_host: "web-server-01", dest_host: "file-server-02", port: 445, protocol: "SMB" }`,
      expectedAlerts: ['brute_force_lateral'],
      phase: 'attack',
    },
    {
      title: 'DNS Exfiltration (Kleene Star)',
      narration:
        'Workstation-15 sends 6 DNS TXT queries to suspicious subdomains of evil-c2.com, interleaved with 4 normal DNS queries from other hosts. With 6 queries (1 first + 5 Kleene), Varpulis finds 31 exfiltration subsequences.',
      eventsText: `DnsQuery { source_host: "workstation-15", domain: "x7k9.evil-c2.com", query_type: "TXT" }
BATCH 1000
DnsQuery { source_host: "admin-pc-01", domain: "google.com", query_type: "A" }
BATCH 1000
DnsQuery { source_host: "workstation-15", domain: "a3m2.evil-c2.com", query_type: "TXT" }
BATCH 1000
DnsQuery { source_host: "workstation-15", domain: "p9w1.evil-c2.com", query_type: "TXT" }
BATCH 1000
DnsQuery { source_host: "dev-box-07", domain: "github.com", query_type: "A" }
BATCH 1000
DnsQuery { source_host: "workstation-15", domain: "b8n4.evil-c2.com", query_type: "TXT" }
BATCH 1000
DnsQuery { source_host: "mail-server-01", domain: "outlook.com", query_type: "MX" }
BATCH 1000
DnsQuery { source_host: "workstation-15", domain: "q2z7.evil-c2.com", query_type: "TXT" }
BATCH 1000
DnsQuery { source_host: "admin-pc-01", domain: "slack.com", query_type: "A" }
BATCH 1000
DnsQuery { source_host: "workstation-15", domain: "r5t3.evil-c2.com", query_type: "TXT" }`,
      expectedAlerts: ['dns_exfiltration'],
      phase: 'attack',
    },
    {
      title: 'Privilege Escalation',
      narration:
        'User jdoe starts a bash session on dev-box-03, uses sudo to elevate privileges, and reads /etc/shadow as root. Three normal user processes on other hosts provide background noise. The three-step escalation chain fires.',
      eventsText: `UserProcess { host: "dev-box-03", username: "jdoe", pid: 1234, command: "bash" }
BATCH 3000
UserProcess { host: "mail-server-01", username: "mailsvc", pid: 5001, command: "postfix" }
BATCH 2000
UserProcess { host: "web-server-01", username: "www", pid: 5002, command: "nginx" }
BATCH 3000
ElevationEvent { host: "dev-box-03", username: "jdoe", method: "sudo" }
BATCH 2000
UserProcess { host: "db-server-03", username: "postgres", pid: 5003, command: "psql" }
BATCH 5000
RootProcess { host: "dev-box-03", pid: 1235, command: "/bin/sh -c cat /etc/shadow" }`,
      expectedAlerts: ['privilege_escalation'],
      phase: 'attack',
    },
    {
      title: 'Clean Network Activity',
      narration:
        'Four normal events: a legitimate login, a DNS query to google.com, a regular user process, and a database connection. No attack patterns match \u2014 zero alerts.',
      eventsText: `SuccessfulLogin { target_host: "vpn-gateway", source_ip: "10.0.0.5", username: "remote_worker" }
BATCH 5000
DnsQuery { source_host: "admin-pc-01", domain: "google.com", query_type: "A" }
BATCH 5000
UserProcess { host: "web-server-01", username: "www", pid: 6001, command: "nginx" }
BATCH 5000
NetworkConnection { source_host: "app-server-01", dest_host: "db-server-03", port: 5432, protocol: "TCP" }`,
      expectedAlerts: [],
      phase: 'negative',
    },
  ],
}
