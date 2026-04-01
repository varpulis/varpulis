//! Sysmon security event schema.
//!
//! Generates realistic Windows Sysmon events for security demos:
//! ProcessCreate (EventID 1), NetworkConnect (EventID 3),
//! ProcessAccess (EventID 10), RegistryValueSet (EventID 13).
//!
//! Anomaly patterns inject multi-step attack sequences:
//! scripting → credential dumping → lateral movement → exfiltration.

use std::collections::HashMap;

use chrono::Utc;
use rand::prelude::*;
use serde_json::json;

use crate::{EventSchema, GeneratedEvent};

const HOSTNAMES: &[&str] = &["WS01", "WS02", "WS03", "DC01", "SRV01", "SRV02"];

const NORMAL_PROCESSES: &[&str] = &[
    r"C:\Windows\System32\svchost.exe",
    r"C:\Windows\explorer.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Windows\System32\taskhostw.exe",
    r"C:\Program Files\Microsoft Office\root\Office16\WINWORD.EXE",
    r"C:\Windows\System32\RuntimeBroker.exe",
    r"C:\Windows\System32\SearchIndexer.exe",
    r"C:\Windows\System32\wbem\WmiPrvSE.exe",
    r"C:\Windows\System32\spoolsv.exe",
    r"C:\Windows\System32\lsass.exe",
];

const NORMAL_PARENTS: &[&str] = &[
    r"C:\Windows\System32\services.exe",
    r"C:\Windows\explorer.exe",
    r"C:\Windows\System32\svchost.exe",
    r"C:\Windows\System32\wininit.exe",
];

const USERS: &[&str] = &[
    r"CORP\jsmith",
    r"CORP\admin",
    r"CORP\svc_backup",
    r"NT AUTHORITY\SYSTEM",
    r"NT AUTHORITY\LOCAL SERVICE",
];

const INTERNAL_IPS: &[&str] = &[
    "10.0.0.10",
    "10.0.0.20",
    "10.0.0.30",
    "10.0.0.50",
    "192.168.1.100",
];

const EXTERNAL_IPS: &[&str] = &[
    "185.29.8.100",
    "45.33.32.156",
    "104.18.21.226",
    "13.107.42.14",
    "142.250.80.46",
];

const REGISTRY_NORMAL: &[&str] = &[
    r"HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System",
    r"HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders",
    r"HKLM\SYSTEM\CurrentControlSet\Services\SharedAccess\Parameters",
];

#[derive(Debug)]
pub struct SysmonSchema {
    rng: StdRng,
    event_count: u64,
    anomaly_sequence: Option<AttackState>,
}

#[derive(Debug)]
enum AttackState {
    /// Step 2: credential dump after scripting
    CredentialDump { host: String },
    /// Step 3: lateral movement after cred dump
    LateralMovement { host: String },
    /// Step 4: exfiltration after lateral movement
    Exfiltration { host: String },
}

impl SysmonSchema {
    pub fn new(seed: Option<u64>) -> Self {
        Self {
            rng: seed.map_or_else(rand::make_rng, StdRng::seed_from_u64),
            event_count: 0,
            anomaly_sequence: None,
        }
    }

    fn pick<'a>(&mut self, items: &'a [&str]) -> &'a str {
        items[self.rng.random_range(0..items.len())]
    }

    fn normal_event(&mut self) -> GeneratedEvent {
        match self.rng.random_range(0..10) {
            0..=5 => self.gen_process_create(false),
            6..=7 => self.gen_network_connect(false),
            8 => self.gen_registry_set(false),
            _ => self.gen_process_access(false),
        }
    }

    fn gen_process_create(&mut self, is_anomaly: bool) -> GeneratedEvent {
        let mut fields = HashMap::new();
        fields.insert("EventID".into(), json!(1));
        fields.insert(
            "Channel".into(),
            json!("Microsoft-Windows-Sysmon/Operational"),
        );
        fields.insert("Image".into(), json!(self.pick(NORMAL_PROCESSES)));
        fields.insert("ParentImage".into(), json!(self.pick(NORMAL_PARENTS)));
        fields.insert("CommandLine".into(), json!(self.pick(NORMAL_PROCESSES)));
        fields.insert("User".into(), json!(self.pick(USERS)));
        fields.insert("Hostname".into(), json!(self.pick(HOSTNAMES)));
        fields.insert(
            "ProcessId".into(),
            json!(self.rng.random_range(1000..30000)),
        );
        fields.insert(
            "ParentProcessId".into(),
            json!(self.rng.random_range(100..5000)),
        );

        GeneratedEvent {
            event_type: "SysmonProcessCreate".into(),
            timestamp: Utc::now(),
            fields,
            is_anomaly,
        }
    }

    fn gen_network_connect(&mut self, is_anomaly: bool) -> GeneratedEvent {
        let mut fields = HashMap::new();
        fields.insert("EventID".into(), json!(3));
        fields.insert(
            "Channel".into(),
            json!("Microsoft-Windows-Sysmon/Operational"),
        );
        fields.insert("Image".into(), json!(self.pick(NORMAL_PROCESSES)));
        fields.insert("SourceIp".into(), json!(self.pick(INTERNAL_IPS)));
        let dest_ip = if self.rng.random_bool(0.7) {
            self.pick(INTERNAL_IPS)
        } else {
            self.pick(EXTERNAL_IPS)
        };
        fields.insert("DestinationIp".into(), json!(dest_ip));
        fields.insert(
            "DestinationPort".into(),
            json!(self.rng.random_range(1..65535)),
        );
        fields.insert("Protocol".into(), json!("tcp"));
        fields.insert("Hostname".into(), json!(self.pick(HOSTNAMES)));
        fields.insert(
            "ProcessId".into(),
            json!(self.rng.random_range(1000..30000)),
        );

        GeneratedEvent {
            event_type: "SysmonNetworkConnect".into(),
            timestamp: Utc::now(),
            fields,
            is_anomaly,
        }
    }

    fn gen_process_access(&mut self, is_anomaly: bool) -> GeneratedEvent {
        let mut fields = HashMap::new();
        fields.insert("EventID".into(), json!(10));
        fields.insert(
            "Channel".into(),
            json!("Microsoft-Windows-Sysmon/Operational"),
        );
        fields.insert("SourceImage".into(), json!(self.pick(NORMAL_PROCESSES)));
        fields.insert("TargetImage".into(), json!(self.pick(NORMAL_PROCESSES)));
        fields.insert("GrantedAccess".into(), json!("0x1400"));
        fields.insert(
            "CallTrace".into(),
            json!("C:\\Windows\\SYSTEM32\\ntdll.dll"),
        );
        fields.insert("Hostname".into(), json!(self.pick(HOSTNAMES)));
        fields.insert(
            "ProcessId".into(),
            json!(self.rng.random_range(1000..30000)),
        );

        GeneratedEvent {
            event_type: "SysmonProcessAccess".into(),
            timestamp: Utc::now(),
            fields,
            is_anomaly,
        }
    }

    fn gen_registry_set(&mut self, is_anomaly: bool) -> GeneratedEvent {
        let mut fields = HashMap::new();
        fields.insert("EventID".into(), json!(13));
        fields.insert(
            "Channel".into(),
            json!("Microsoft-Windows-Sysmon/Operational"),
        );
        fields.insert("Image".into(), json!(self.pick(NORMAL_PROCESSES)));
        fields.insert("TargetObject".into(), json!(self.pick(REGISTRY_NORMAL)));
        fields.insert("Details".into(), json!("DWORD (0x00000001)"));
        fields.insert("Hostname".into(), json!(self.pick(HOSTNAMES)));
        fields.insert(
            "ProcessId".into(),
            json!(self.rng.random_range(1000..30000)),
        );

        GeneratedEvent {
            event_type: "SysmonRegistryValueSet".into(),
            timestamp: Utc::now(),
            fields,
            is_anomaly,
        }
    }

    /// Generate a multi-step attack kill chain anomaly.
    fn anomaly_event(&mut self) -> GeneratedEvent {
        if let Some(state) = self.anomaly_sequence.take() {
            return match state {
                AttackState::CredentialDump { host } => {
                    let mut fields = HashMap::new();
                    fields.insert("EventID".into(), json!(10));
                    fields.insert(
                        "Channel".into(),
                        json!("Microsoft-Windows-Sysmon/Operational"),
                    );
                    fields.insert(
                        "SourceImage".into(),
                        json!(r"C:\Users\Public\procdump64.exe"),
                    );
                    fields.insert(
                        "TargetImage".into(),
                        json!(r"C:\Windows\System32\lsass.exe"),
                    );
                    fields.insert("GrantedAccess".into(), json!("0x1438"));
                    fields.insert(
                        "CallTrace".into(),
                        json!("C:\\Windows\\SYSTEM32\\ntdll.dll+9c4e4"),
                    );
                    fields.insert("Hostname".into(), json!(&host));
                    fields.insert("ProcessId".into(), json!(7712));

                    self.anomaly_sequence = Some(AttackState::LateralMovement { host });

                    GeneratedEvent {
                        event_type: "SysmonProcessAccess".into(),
                        timestamp: Utc::now(),
                        fields,
                        is_anomaly: true,
                    }
                }
                AttackState::LateralMovement { host } => {
                    let mut fields = HashMap::new();
                    fields.insert("EventID".into(), json!(3));
                    fields.insert(
                        "Channel".into(),
                        json!("Microsoft-Windows-Sysmon/Operational"),
                    );
                    fields.insert("Image".into(), json!(r"C:\Windows\System32\net.exe"));
                    fields.insert("SourceIp".into(), json!("10.0.0.10"));
                    fields.insert("DestinationIp".into(), json!("10.0.0.20"));
                    fields.insert("DestinationPort".into(), json!(445));
                    fields.insert("Protocol".into(), json!("tcp"));
                    fields.insert("Hostname".into(), json!(&host));
                    fields.insert("ProcessId".into(), json!(8844));

                    self.anomaly_sequence = Some(AttackState::Exfiltration { host });

                    GeneratedEvent {
                        event_type: "SysmonNetworkConnect".into(),
                        timestamp: Utc::now(),
                        fields,
                        is_anomaly: true,
                    }
                }
                AttackState::Exfiltration { host } => {
                    let mut fields = HashMap::new();
                    fields.insert("EventID".into(), json!(3));
                    fields.insert(
                        "Channel".into(),
                        json!("Microsoft-Windows-Sysmon/Operational"),
                    );
                    fields.insert("Image".into(), json!(r"C:\Windows\System32\curl.exe"));
                    fields.insert("SourceIp".into(), json!("10.0.0.10"));
                    fields.insert("DestinationIp".into(), json!("185.29.8.100"));
                    fields.insert("DestinationPort".into(), json!(443));
                    fields.insert("Protocol".into(), json!("tcp"));
                    fields.insert("Hostname".into(), json!(&host));
                    fields.insert("ProcessId".into(), json!(9120));

                    // Sequence complete
                    GeneratedEvent {
                        event_type: "SysmonNetworkConnect".into(),
                        timestamp: Utc::now(),
                        fields,
                        is_anomaly: true,
                    }
                }
            };
        }

        // Start new attack sequence: scripting engine execution
        let host = self.pick(HOSTNAMES).to_string();
        let user = self.pick(USERS).to_string();

        let mut fields = HashMap::new();
        fields.insert("EventID".into(), json!(1));
        fields.insert(
            "Channel".into(),
            json!("Microsoft-Windows-Sysmon/Operational"),
        );
        fields.insert(
            "Image".into(),
            json!(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"),
        );
        fields.insert(
            "CommandLine".into(),
            json!("powershell.exe -nop -w hidden -enc ZQBjAGgAbwA..."),
        );
        fields.insert("ParentImage".into(), json!(r"C:\Windows\explorer.exe"));
        fields.insert("User".into(), json!(&user));
        fields.insert("Hostname".into(), json!(&host));
        fields.insert("ProcessId".into(), json!(6644));
        fields.insert("ParentProcessId".into(), json!(3200));

        self.anomaly_sequence = Some(AttackState::CredentialDump { host });

        GeneratedEvent {
            event_type: "SysmonProcessCreate".into(),
            timestamp: Utc::now(),
            fields,
            is_anomaly: true,
        }
    }
}

impl EventSchema for SysmonSchema {
    fn next_event(&mut self) -> GeneratedEvent {
        self.event_count += 1;

        // Inject anomaly roughly 5% of events (configurable via anomaly_rate in generator)
        if self.anomaly_sequence.is_some() || self.rng.random_bool(0.02) {
            self.anomaly_event()
        } else {
            self.normal_event()
        }
    }

    fn event_types(&self) -> Vec<String> {
        vec![
            "SysmonProcessCreate".into(),
            "SysmonNetworkConnect".into(),
            "SysmonProcessAccess".into(),
            "SysmonRegistryValueSet".into(),
        ]
    }

    fn description(&self) -> &str {
        "Sysmon security events with kill chain attack anomalies (credential dump, lateral movement, exfiltration)"
    }
}
