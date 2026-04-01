//! Syslog CEF/LEEF connector for Varpulis.
//!
//! Sends security alerts as CEF-formatted syslog messages over TCP or UDP.
//! CEF (Common Event Format) is the standard for SIEM ingestion (ArcSight,
//! Splunk, QRadar, Sentinel, etc.).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::info;
use varpulis_connector_api::{
    ConfigParamInfo, ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory,
    ConnectorHealth, Sink, SinkConnector, SinkConnectorAdapter, SourceConnector,
};
use varpulis_core::event::Event;
use varpulis_core::value::FxIndexMap;
use varpulis_core::Value;

// ---------------------------------------------------------------------------
// CEF Formatter
// ---------------------------------------------------------------------------

/// Format a Varpulis event as a CEF syslog message.
///
/// CEF format: `CEF:0|Vendor|Product|Version|SignatureID|Name|Severity|Extensions`
pub fn event_to_cef(event: &Event) -> String {
    let get_str = |key: &str| -> String {
        event
            .get(key)
            .map(|v| match v {
                Value::Str(s) => s.to_string(),
                other => other.to_string(),
            })
            .unwrap_or_default()
    };

    let rule = get_str("rule");
    let signature_id = if rule.is_empty() {
        event.event_type.to_string()
    } else {
        rule
    };

    let summary = get_str("summary");
    let name = if summary.is_empty() {
        event.event_type.to_string()
    } else {
        summary
    };

    let severity_str = get_str("severity");
    let cef_severity = match severity_str.as_str() {
        "critical" => "10",
        "high" => "8",
        "medium" => "5",
        "low" => "3",
        "info" | "informational" => "1",
        _ => "5",
    };

    // Build extension key=value pairs
    let skip = ["rule", "severity", "summary", "event_type"];
    let mut extensions = String::new();

    // Always include MITRE category if present
    let mitre = get_str("mitre");
    if !mitre.is_empty() {
        extensions.push_str(&format!("cat={}", cef_escape_ext(&mitre)));
    }

    // Include timestamp
    let ts = event.timestamp.format("%b %d %Y %H:%M:%S").to_string();
    if !extensions.is_empty() {
        extensions.push(' ');
    }
    extensions.push_str(&format!("rt={ts}"));

    // Add remaining fields as extensions
    for (key, val) in &event.data {
        if skip.contains(&key.as_ref()) {
            continue;
        }
        if key.as_ref() == "mitre" {
            continue;
        }
        if !extensions.is_empty() {
            extensions.push(' ');
        }
        let val_str = match val {
            Value::Str(s) => s.to_string(),
            other => other.to_string(),
        };
        extensions.push_str(&format!(
            "{}={}",
            cef_escape_ext(key),
            cef_escape_ext(&val_str)
        ));
    }

    // CEF header uses pipe escaping, extensions use equals escaping
    format!(
        "CEF:0|Varpulis|KillChain|1.0|{}|{}|{}|{}",
        cef_escape_header(&signature_id),
        cef_escape_header(&name),
        cef_severity,
        extensions
    )
}

/// Escape CEF header fields (pipe and backslash).
fn cef_escape_header(s: &str) -> String {
    s.replace('\\', "\\\\").replace('|', "\\|")
}

/// Escape CEF extension values (equals, newline, backslash).
fn cef_escape_ext(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('=', "\\=")
        .replace('\n', "\\n")
        .replace('\r', "")
}

// ---------------------------------------------------------------------------
// Syslog Sink (TCP/UDP)
// ---------------------------------------------------------------------------

/// Transport protocol for syslog delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyslogTransport {
    /// TCP (reliable, connection-oriented).
    Tcp,
    /// UDP (fire-and-forget, lower latency).
    Udp,
}

/// Syslog CEF sink — sends events as CEF-formatted syslog messages.
pub struct SyslogCefSink {
    name: String,
    host: String,
    port: u16,
    transport: SyslogTransport,
    tcp_conn: Mutex<Option<tokio::net::TcpStream>>,
    udp_socket: Mutex<Option<tokio::net::UdpSocket>>,
    facility: u8,
}

impl std::fmt::Debug for SyslogCefSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyslogCefSink")
            .field("name", &self.name)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("transport", &self.transport)
            .finish_non_exhaustive()
    }
}

impl SyslogCefSink {
    /// Create a new syslog CEF sink.
    pub fn new(name: &str, host: &str, port: u16, transport: SyslogTransport) -> Self {
        Self {
            name: name.to_string(),
            host: host.to_string(),
            port,
            transport,
            tcp_conn: Mutex::new(None),
            udp_socket: Mutex::new(None),
            facility: 1, // user-level
        }
    }

    /// Build the full syslog message with RFC 5424 header + CEF payload.
    fn format_syslog_message(&self, cef: &str) -> String {
        // RFC 3164 (BSD syslog) format: <PRI>TIMESTAMP HOSTNAME MSG
        // PRI = facility * 8 + severity
        let priority = self.facility * 8 + 4; // 4 = warning level
        let hostname = "varpulis";
        format!("<{priority}>{hostname} {cef}\n")
    }
}

#[async_trait]
impl SinkConnector for SyslogCefSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(&mut self) -> Result<(), ConnectorError> {
        let addr = format!("{}:{}", self.host, self.port);
        match self.transport {
            SyslogTransport::Tcp => {
                let stream = tokio::net::TcpStream::connect(&addr).await.map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("TCP connect to {addr}: {e}"))
                })?;
                info!("Syslog CEF sink connected to {} (TCP)", addr);
                *self.tcp_conn.lock().await = Some(stream);
            }
            SyslogTransport::Udp => {
                let socket = tokio::net::UdpSocket::bind("0.0.0.0:0")
                    .await
                    .map_err(|e| ConnectorError::ConnectionFailed(format!("UDP bind: {e}")))?;
                socket.connect(&addr).await.map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("UDP connect to {addr}: {e}"))
                })?;
                info!("Syslog CEF sink connected to {} (UDP)", addr);
                *self.udp_socket.lock().await = Some(socket);
            }
        }
        Ok(())
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let cef = event_to_cef(event);
        let msg = self.format_syslog_message(&cef);
        let bytes = msg.as_bytes();

        match self.transport {
            SyslogTransport::Tcp => {
                let mut guard = self.tcp_conn.lock().await;
                if let Some(ref mut stream) = *guard {
                    stream
                        .write_all(bytes)
                        .await
                        .map_err(|e| ConnectorError::SendFailed(format!("TCP write: {e}")))?;
                } else {
                    return Err(ConnectorError::SendFailed("TCP not connected".to_string()));
                }
            }
            SyslogTransport::Udp => {
                let guard = self.udp_socket.lock().await;
                if let Some(ref socket) = *guard {
                    socket
                        .send(bytes)
                        .await
                        .map_err(|e| ConnectorError::SendFailed(format!("UDP send: {e}")))?;
                } else {
                    return Err(ConnectorError::SendFailed("UDP not connected".to_string()));
                }
            }
        }

        Ok(())
    }

    async fn send_to_topic(
        &self,
        events: &[Arc<Event>],
        _topic: &str,
    ) -> Result<(), ConnectorError> {
        for event in events {
            self.send(event).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        if self.transport == SyslogTransport::Tcp {
            let mut guard = self.tcp_conn.lock().await;
            if let Some(ref mut stream) = *guard {
                stream
                    .flush()
                    .await
                    .map_err(|e| ConnectorError::SendFailed(format!("TCP flush: {e}")))?;
            }
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        if self.transport == SyslogTransport::Tcp {
            let mut guard = self.tcp_conn.lock().await;
            if let Some(ref mut stream) = *guard {
                let _ = stream.shutdown().await;
            }
            *guard = None;
        }
        Ok(())
    }

    fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth::healthy(0)
    }
}

// ---------------------------------------------------------------------------
// Syslog TCP Source
// ---------------------------------------------------------------------------

/// Syslog TCP source — listens for incoming syslog messages on a TCP port.
///
/// Parses newline-delimited syslog messages and converts them to Varpulis events.
/// Supports plain text syslog (RFC 3164) with optional CEF payload extraction.
pub struct SyslogTcpSource {
    name: String,
    bind_addr: String,
    running: Arc<std::sync::atomic::AtomicBool>,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl std::fmt::Debug for SyslogTcpSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyslogTcpSource")
            .field("name", &self.name)
            .field("bind_addr", &self.bind_addr)
            .finish_non_exhaustive()
    }
}

impl SyslogTcpSource {
    /// Create a new syslog TCP source.
    pub fn new(name: &str, bind_addr: &str) -> Self {
        Self {
            name: name.to_string(),
            bind_addr: bind_addr.to_string(),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_tx: Mutex::new(None),
        }
    }
}

/// Parse a raw syslog line into a Varpulis event.
///
/// Handles RFC 3164 format: `<PRI>TIMESTAMP HOSTNAME MSG`
/// If the message contains a CEF payload, extracts structured fields.
pub fn parse_syslog_line(line: &str) -> Event {
    let trimmed = line.trim();
    let mut data = FxIndexMap::default();

    // Try to extract CEF payload
    if let Some(cef_start) = trimmed.find("CEF:") {
        let cef = &trimmed[cef_start..];
        // Parse CEF: CEF:version|vendor|product|ver|sig|name|severity|extensions
        let parts: Vec<&str> = cef.splitn(8, '|').collect();
        if parts.len() >= 7 {
            data.insert("vendor".into(), Value::str(parts[1]));
            data.insert("product".into(), Value::str(parts[2]));
            data.insert("signature_id".into(), Value::str(parts[4]));
            data.insert("name".into(), Value::str(parts[5]));
            data.insert("severity".into(), Value::str(parts[6]));

            // Parse extensions (key=value pairs)
            if parts.len() >= 8 {
                for kv in parts[7].split(' ') {
                    if let Some(eq_pos) = kv.find('=') {
                        let key = &kv[..eq_pos];
                        let val = &kv[eq_pos + 1..];
                        data.insert(Arc::from(key), Value::str(val));
                    }
                }
            }

            return Event {
                event_type: "SyslogCef".into(),
                timestamp: chrono::Utc::now(),
                data,
            };
        }
    }

    // Fall back to raw syslog message
    data.insert("message".into(), Value::str(trimmed));

    // Try to extract priority from <PRI> prefix
    if trimmed.starts_with('<') {
        if let Some(end) = trimmed.find('>') {
            if let Ok(pri) = trimmed[1..end].parse::<i64>() {
                data.insert("facility".into(), Value::Int(pri / 8));
                data.insert("severity_code".into(), Value::Int(pri % 8));
            }
        }
    }

    Event {
        event_type: "SyslogMessage".into(),
        timestamp: chrono::Utc::now(),
        data,
    }
}

#[async_trait]
impl SourceConnector for SyslogTcpSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: tokio::sync::mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        let listener = tokio::net::TcpListener::bind(&self.bind_addr)
            .await
            .map_err(|e| {
                ConnectorError::ConnectionFailed(format!("Bind {}: {e}", self.bind_addr))
            })?;

        info!("Syslog TCP source listening on {}", self.bind_addr);
        self.running
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        let running = self.running.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                info!("Syslog connection from {}", addr);
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    let reader = tokio::io::BufReader::new(stream);
                                    use tokio::io::AsyncBufReadExt;
                                    let mut lines = reader.lines();
                                    while let Ok(Some(line)) = lines.next_line().await {
                                        if line.trim().is_empty() {
                                            continue;
                                        }
                                        let event = parse_syslog_line(&line);
                                        if tx.send(event).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::error!("Syslog accept error: {e}");
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
            running.store(false, std::sync::atomic::Ordering::Relaxed);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        let shutdown = self.shutdown_tx.lock().await.take();
        if let Some(tx) = shutdown {
            let _ = tx.send(());
        }
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Factory & Registration
// ---------------------------------------------------------------------------

static SYSLOG_CONFIG_PARAMS: &[ConfigParamInfo] = &[
    ConfigParamInfo {
        name: "host",
        description: "Syslog server hostname or IP",
        required: true,
        default_value: None,
    },
    ConfigParamInfo {
        name: "port",
        description: "Syslog server port",
        required: false,
        default_value: Some("514"),
    },
    ConfigParamInfo {
        name: "transport",
        description: "Transport protocol: tcp or udp",
        required: false,
        default_value: Some("tcp"),
    },
];

static SYSLOG_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "syslog",
    display_name: "Syslog CEF",
    description: "Syslog CEF/LEEF sink for SIEM integration",
    feature_flag: "",
    supports_source: true,
    supports_sink: true,
    supports_managed: false,
    config_params: SYSLOG_CONFIG_PARAMS,
};

struct SyslogFactory;

impl ConnectorFactory for SyslogFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &SYSLOG_INFO
    }

    fn create_managed(
        &self,
        _name: &str,
        _config: &ConnectorConfig,
    ) -> Result<Box<dyn varpulis_connector_api::ManagedConnector>, ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "syslog managed connector not supported".to_string(),
        ))
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let host = config
            .properties
            .get("host")
            .cloned()
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port: u16 = config
            .properties
            .get("port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(514);
        let transport = match config.properties.get("transport").map(|s| s.as_str()) {
            Some("udp") => SyslogTransport::Udp,
            _ => SyslogTransport::Tcp,
        };

        Ok(Box::new(SyslogCefSink::new(
            "syslog", &host, port, transport,
        )))
    }

    fn create_engine_sink(
        &self,
        name: &str,
        config: &ConnectorConfig,
        _topic_override: Option<&str>,
        _context_name: Option<&str>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let connector = self.create_sink_connector(config)?;
        Ok(Arc::new(SinkConnectorAdapter::new(name, connector)))
    }
}

inventory::submit! { &SyslogFactory as &dyn ConnectorFactory }

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use varpulis_core::event::Event;
    use varpulis_core::value::FxIndexMap;

    use super::*;

    #[test]
    fn test_event_to_cef_basic() {
        let mut data = FxIndexMap::default();
        data.insert("rule".into(), Value::str("full_killchain"));
        data.insert("mitre".into(), Value::str("T1059,T1003"));
        data.insert("severity".into(), Value::str("critical"));
        data.insert("summary".into(), Value::str("Kill chain detected"));
        data.insert("host".into(), Value::str("WS01"));

        let event = Event {
            event_type: "KillChainAlert".into(),
            timestamp: Utc::now(),
            data,
        };

        let cef = event_to_cef(&event);
        assert!(cef.starts_with("CEF:0|Varpulis|KillChain|1.0|"));
        assert!(cef.contains("full_killchain"));
        assert!(cef.contains("Kill chain detected"));
        assert!(cef.contains("|10|")); // critical = 10
        assert!(cef.contains("cat=T1059,T1003"));
        assert!(cef.contains("host=WS01"));
    }

    #[test]
    fn test_cef_escape_header() {
        assert_eq!(cef_escape_header("a|b\\c"), "a\\|b\\\\c");
    }

    #[test]
    fn test_cef_escape_ext() {
        assert_eq!(cef_escape_ext("key=val"), "key\\=val");
        assert_eq!(cef_escape_ext("line\none"), "line\\none");
    }

    #[test]
    fn test_severity_mapping() {
        let make_event = |sev: &str| -> Event {
            let mut data = FxIndexMap::default();
            data.insert("severity".into(), Value::str(sev));
            Event {
                event_type: "Alert".into(),
                timestamp: Utc::now(),
                data,
            }
        };

        assert!(event_to_cef(&make_event("critical")).contains("|10|"));
        assert!(event_to_cef(&make_event("high")).contains("|8|"));
        assert!(event_to_cef(&make_event("medium")).contains("|5|"));
        assert!(event_to_cef(&make_event("low")).contains("|3|"));
        assert!(event_to_cef(&make_event("info")).contains("|1|"));
    }

    #[test]
    fn test_parse_syslog_cef_line() {
        let line = "CEF:0|Varpulis|KillChain|1.0|full_killchain|Kill chain detected|10|cat=T1059 host=WS01";
        let event = parse_syslog_line(line);
        assert_eq!(&*event.event_type, "SyslogCef");
        assert_eq!(event.get("vendor"), Some(&Value::str("Varpulis")));
        assert_eq!(event.get("product"), Some(&Value::str("KillChain")));
        assert_eq!(
            event.get("signature_id"),
            Some(&Value::str("full_killchain"))
        );
        assert_eq!(event.get("severity"), Some(&Value::str("10")));
        assert_eq!(event.get("cat"), Some(&Value::str("T1059")));
        assert_eq!(event.get("host"), Some(&Value::str("WS01")));
    }

    #[test]
    fn test_parse_syslog_raw_line() {
        let line = "<13>varpulis some raw syslog message";
        let event = parse_syslog_line(line);
        assert_eq!(&*event.event_type, "SyslogMessage");
        assert_eq!(event.get("facility"), Some(&Value::Int(1)));
        assert_eq!(event.get("severity_code"), Some(&Value::Int(5)));
    }

    #[tokio::test]
    async fn test_syslog_tcp_source_lifecycle() {
        use tokio::io::AsyncWriteExt;

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        // Use a specific high port for testing
        let port = 19514u16;
        let mut source = SyslogTcpSource::new("test", &format!("127.0.0.1:{port}"));
        source.start(tx).await.unwrap();
        assert!(source.is_running());

        // Give the listener time to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect and send a syslog message
        let mut client = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        client
            .write_all(b"<14>varpulis test message\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        // Wait for the event
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&*event.event_type, "SyslogMessage");
        assert_eq!(event.get("facility"), Some(&Value::Int(1)));

        // Stop
        source.stop().await.unwrap();
    }
}
