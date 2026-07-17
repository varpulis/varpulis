//! PostgreSQL CDC (Change Data Capture) source connector.
//!
//! Uses PostgreSQL logical replication protocol to stream WAL changes
//! as Varpulis events. Converts INSERT/UPDATE/DELETE operations into
//! typed events for stream processing.

use async_trait::async_trait;
use tokio::sync::mpsc;
use varpulis_connector_api::{ConnectorError, SourceConnector};
use varpulis_core::security::SecretString;
use varpulis_core::Event;

// =============================================================================
// PostgreSQL CDC Configuration (always available, not feature-gated)
// =============================================================================

/// PostgreSQL CDC configuration for logical replication.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct PostgresCdcConfig {
    /// Database host
    pub host: String,
    /// Database port (default: 5432)
    pub port: u16,
    /// Database name
    pub dbname: String,
    /// Username
    pub user: String,
    /// Password (zeroized on drop).
    pub password: SecretString,
    /// Replication slot name
    pub slot_name: String,
    /// Publication name (for pgoutput plugin)
    pub publication: String,
    /// Tables to subscribe to (empty = all tables in publication)
    pub tables: Vec<String>,
    /// SSL mode: disable, prefer, require, verify-ca, verify-full
    pub sslmode: String,
    /// Path to CA certificate (PEM format).
    pub ssl_ca_location: Option<String>,
    /// Path to client certificate (PEM format) for mTLS.
    pub ssl_certificate_location: Option<String>,
    /// Path to client private key (PEM format) for mTLS.
    pub ssl_key_location: Option<String>,
}

impl PostgresCdcConfig {
    /// Create a new CDC configuration for the given host and database.
    pub fn new(host: &str, dbname: &str) -> Self {
        Self {
            host: host.to_string(),
            port: 5432,
            dbname: dbname.to_string(),
            user: "postgres".to_string(),
            password: SecretString::new(""),
            slot_name: "varpulis_slot".to_string(),
            publication: "varpulis_pub".to_string(),
            tables: Vec::new(),
            sslmode: "prefer".to_string(),
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
        }
    }

    /// Set the database port (default: 5432).
    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set username and password for authentication.
    pub fn with_credentials(mut self, user: &str, password: &str) -> Self {
        self.user = user.to_string();
        self.password = SecretString::new(password);
        self
    }

    /// Set the replication slot name.
    pub fn with_slot(mut self, slot_name: &str) -> Self {
        self.slot_name = slot_name.to_string();
        self
    }

    /// Set the publication name for the pgoutput plugin.
    pub fn with_publication(mut self, publication: &str) -> Self {
        self.publication = publication.to_string();
        self
    }

    /// Set the list of tables to subscribe to (empty = all in publication).
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Set the SSL mode (disable, prefer, require, verify-ca, verify-full).
    pub fn with_sslmode(mut self, mode: &str) -> Self {
        self.sslmode = mode.to_string();
        self
    }

    /// Set the path to the CA certificate (PEM).
    pub fn with_ca_cert(mut self, path: &str) -> Self {
        self.ssl_ca_location = Some(path.to_string());
        self
    }

    /// Set client certificate and key paths for mTLS.
    pub fn with_client_cert(mut self, cert: &str, key: &str) -> Self {
        self.ssl_certificate_location = Some(cert.to_string());
        self.ssl_key_location = Some(key.to_string());
        self
    }
}

/// CDC operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    /// A new row was inserted.
    Insert,
    /// An existing row was updated.
    Update,
    /// A row was deleted.
    Delete,
}

impl CdcOperation {
    /// Return the operation as a string (`"INSERT"`, `"UPDATE"`, or `"DELETE"`).
    pub const fn as_str(&self) -> &str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

impl std::fmt::Display for CdcOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Create a Varpulis Event from a CDC change.
pub fn cdc_event(
    table: &str,
    operation: CdcOperation,
    fields: Vec<(String, varpulis_core::Value)>,
) -> Event {
    let event_type = format!("{table}.{operation}");
    let mut event = Event::new(event_type.as_str());
    event
        .data
        .insert("_table".into(), varpulis_core::Value::Str(table.into()));
    event.data.insert(
        "_op".into(),
        varpulis_core::Value::Str(operation.as_str().into()),
    );
    for (key, value) in fields {
        event.data.insert(key.as_str().into(), value);
    }
    event
}

// =============================================================================
// Stub source (no cdc feature)
// =============================================================================

/// PostgreSQL CDC source connector.
#[derive(Debug)]
pub struct PostgresCdcSource {
    name: String,
    config: PostgresCdcConfig,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl PostgresCdcSource {
    /// Create a new PostgreSQL CDC source with the given configuration.
    pub fn new(name: &str, config: PostgresCdcConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

/// Validate a PostgreSQL logical replication slot name.
///
/// The slot name is interpolated directly into the replication SQL
/// (`pg_create_logical_replication_slot`, `pg_logical_slot_get_changes`,
/// `START_REPLICATION`) via `simple_query`, which does NO parameter binding — so
/// an unvalidated name is a SQL-injection vector. PostgreSQL itself restricts
/// slot names to lowercase letters, digits, and underscores, ≤63 bytes;
/// enforcing exactly that rule rejects any injection attempt (quotes, spaces,
/// semicolons, etc.) while accepting every legitimate slot name.
fn validate_slot_name(name: &str) -> Result<(), ConnectorError> {
    if name.is_empty() || name.len() > 63 {
        return Err(ConnectorError::ConfigError(format!(
            "invalid replication slot name {name:?}: must be 1-63 characters"
        )));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(ConnectorError::ConfigError(format!(
            "invalid replication slot name {name:?}: only lowercase letters, \
             digits, and underscores are allowed"
        )));
    }
    Ok(())
}

// =============================================================================
// TLS support for PostgreSQL connections
//
// tokio-postgres only understands sslmode = disable | prefer | require and,
// when handed `NoTls`, SILENTLY ignores any TLS intent (sslmode=prefer =>
// cleartext; verify-ca/verify-full aren't even parseable). We therefore map the
// full set of libpq sslmode values to an explicit (tokio-postgres sslmode, TLS
// connector, certificate verifier) triple here, so replication traffic (row
// data + credentials) is genuinely encrypted when the operator asks for it.
//
// SECURITY: the no-verify verifier below is reachable ONLY for
// require/prefer/allow. verify-ca/verify-full always use the standard WebPki
// verifier and reject an untrusted certificate.
// =============================================================================

/// Build the base libpq connection string WITHOUT an `sslmode=` fragment.
///
/// [`connect_pg`] appends the tokio-postgres-level sslmode that matches the TLS
/// connector it chooses. Keeping sslmode out of the base string means the raw
/// (and possibly richer, e.g. `verify-full`) user value is never handed to
/// tokio-postgres's parser, which only accepts disable/prefer/require.
fn base_conn_string(config: &PostgresCdcConfig) -> String {
    format!(
        "host={} port={} dbname={} user={} password={}",
        config.host,
        config.port,
        config.dbname,
        config.user,
        config.password.expose(),
    )
}

/// Server-certificate verifier that ACCEPTS ANY certificate without validation.
///
/// SECURITY: performs no chain, expiry, or hostname checks — it implements the
/// libpq `require`/`prefer`/`allow` semantics of "encrypt the connection but do
/// not authenticate the server". It is installed EXCLUSIVELY by
/// [`build_tls_config_noverify`] and MUST NEVER be used for verify-ca /
/// verify-full (those go through [`build_tls_config_verify`], which uses the
/// standard `WebPkiServerVerifier`).
#[derive(Debug)]
struct NoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Read a PEM certificate chain from `path`.
fn load_cert_chain(
    path: &str,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, ConnectorError> {
    let data = std::fs::read(path).map_err(|e| {
        ConnectorError::ConfigError(format!("cannot read certificate {path:?}: {e}"))
    })?;
    let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(&data[..]))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            ConnectorError::ConfigError(format!("invalid PEM certificate {path:?}: {e}"))
        })?;
    if certs.is_empty() {
        return Err(ConnectorError::ConfigError(format!(
            "no certificates found in {path:?}"
        )));
    }
    Ok(certs)
}

/// Read the first PEM private key from `path`.
fn load_private_key(
    path: &str,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, ConnectorError> {
    let data = std::fs::read(path).map_err(|e| {
        ConnectorError::ConfigError(format!("cannot read private key {path:?}: {e}"))
    })?;
    rustls_pemfile::private_key(&mut std::io::BufReader::new(&data[..]))
        .map_err(|e| ConnectorError::ConfigError(format!("invalid PEM private key {path:?}: {e}")))?
        .ok_or_else(|| ConnectorError::ConfigError(format!("no private key found in {path:?}")))
}

/// Apply mTLS client authentication to a rustls builder in the `WantsClientCert`
/// state — shared by the verify and no-verify code paths.
///
/// If BOTH `ssl_certificate_location` and `ssl_key_location` are set, the client
/// cert/key are loaded and installed. If NEITHER is set, no client auth is used.
/// If exactly one is set, that is a misconfiguration and a `ConfigError` is
/// returned (fail closed rather than silently connect without the intended
/// client identity).
fn finish_client_auth(
    builder: rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>,
    config: &PostgresCdcConfig,
) -> Result<rustls::ClientConfig, ConnectorError> {
    match (
        config.ssl_certificate_location.as_deref(),
        config.ssl_key_location.as_deref(),
    ) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_cert_chain(cert_path)?;
            let key = load_private_key(key_path)?;
            builder.with_client_auth_cert(certs, key).map_err(|e| {
                ConnectorError::ConfigError(format!("invalid mTLS client certificate/key: {e}"))
            })
        }
        (None, None) => Ok(builder.with_no_client_auth()),
        (Some(_), None) => Err(ConnectorError::ConfigError(
            "mTLS misconfigured: ssl_certificate_location is set but ssl_key_location is not"
                .to_string(),
        )),
        (None, Some(_)) => Err(ConnectorError::ConfigError(
            "mTLS misconfigured: ssl_key_location is set but ssl_certificate_location is not"
                .to_string(),
        )),
    }
}

/// Build a rustls `ClientConfig` that ENCRYPTS but does NOT verify the server
/// certificate — the libpq `require`/`prefer`/`allow` semantics.
///
/// SECURITY: installs [`NoCertVerifier`]. Only [`connect_pg`]'s
/// require/prefer/allow arm calls this.
fn build_tls_config_noverify(
    config: &PostgresCdcConfig,
) -> Result<rustls::ClientConfig, ConnectorError> {
    // Mirror the MQTT connector: ensure a process-default crypto provider is
    // installed before constructing any ClientConfig (idempotent; ignore the
    // "already installed" error).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let builder = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(std::sync::Arc::new(NoCertVerifier));
    finish_client_auth(builder, config)
}

/// Build a rustls `ClientConfig` that VERIFIES the server certificate chain and
/// hostname via rustls's default `WebPkiServerVerifier` — the libpq
/// `verify-ca`/`verify-full` semantics.
///
/// Roots come from `ssl_ca_location` (a PEM bundle) when set, otherwise the OS
/// trust store. NEVER installs [`NoCertVerifier`].
fn build_tls_config_verify(
    config: &PostgresCdcConfig,
) -> Result<rustls::ClientConfig, ConnectorError> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut roots = rustls::RootCertStore::empty();
    if let Some(ca_path) = config.ssl_ca_location.as_deref() {
        for cert in load_cert_chain(ca_path)? {
            roots.add(cert).map_err(|e| {
                ConnectorError::ConfigError(format!("invalid CA certificate {ca_path:?}: {e}"))
            })?;
        }
    } else {
        let native = rustls_native_certs::load_native_certs();
        for cert in native.certs {
            // Skip individual malformed system certs; a single bad root should
            // not abort the connection as long as some roots load.
            let _ = roots.add(cert);
        }
        if roots.is_empty() {
            let detail = match native.errors.first() {
                Some(e) => e.to_string(),
                None => "system trust store is empty".to_string(),
            };
            return Err(ConnectorError::ConfigError(format!(
                "sslmode={} requires trusted CA roots but none were loaded from the system \
                 trust store ({detail}); set ssl_ca_location to a PEM CA bundle",
                config.sslmode
            )));
        }
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    finish_client_auth(builder, config)
}

/// Open a cleartext (`NoTls`) connection and spawn its background driver task.
async fn connect_notls(conn_string: &str) -> Result<tokio_postgres::Client, ConnectorError> {
    let (client, connection) = tokio_postgres::connect(conn_string, tokio_postgres::NoTls)
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("PostgreSQL: {e}")))?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("PostgreSQL connection error: {e}");
        }
    });
    Ok(client)
}

/// Open a rustls TLS connection with `tls_config` and spawn its background
/// driver task. Split from [`connect_notls`] because tokio-postgres returns a
/// differently-typed `Connection` per TLS connector, so each driver future must
/// be spawned in its own monomorphized arm.
async fn connect_rustls(
    conn_string: &str,
    tls_config: rustls::ClientConfig,
) -> Result<tokio_postgres::Client, ConnectorError> {
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
    let (client, connection) = tokio_postgres::connect(conn_string, tls)
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("PostgreSQL: {e}")))?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("PostgreSQL connection error: {e}");
        }
    });
    Ok(client)
}

/// Connect to PostgreSQL, honoring `config.sslmode` with a real rustls TLS
/// implementation.
///
/// `base_conn_string` MUST NOT contain an `sslmode=` fragment (see
/// [`base_conn_string`]); this function appends the tokio-postgres-level sslmode
/// (`disable` or `require`) that matches the connector it selects.
///
/// SECURITY — sslmode -> (connector, server-cert verification):
/// - `disable`                 -> `NoTls` (cleartext)
/// - `require`                 -> rustls, NO verification (encrypt only); no fallback
/// - `prefer` / `allow`        -> rustls, NO verification; fall back to cleartext on failure
/// - `verify-ca` / `verify-full` -> rustls, WebPki verification of chain + hostname
async fn connect_pg(
    base_conn_string: &str,
    config: &PostgresCdcConfig,
) -> Result<tokio_postgres::Client, ConnectorError> {
    let mode = config.sslmode.trim().to_ascii_lowercase();
    match mode.as_str() {
        "disable" => connect_notls(&format!("{base_conn_string} sslmode=disable")).await,
        "require" | "prefer" | "allow" => {
            // Encrypt but do NOT authenticate the server (libpq require/prefer).
            let tls_config = build_tls_config_noverify(config)?;
            // Force tokio-postgres to actually perform TLS: sslmode=require makes
            // it send the SSLRequest and error if the server refuses SSL, so the
            // prefer/allow cleartext fallback is driven explicitly here rather
            // than by tokio-postgres silently downgrading.
            let tls_conn_string = format!("{base_conn_string} sslmode=require");
            match connect_rustls(&tls_conn_string, tls_config).await {
                Ok(client) => Ok(client),
                Err(e) => {
                    if mode == "prefer" || mode == "allow" {
                        tracing::warn!(
                            "TLS connection failed for sslmode={mode}; falling back to \
                             cleartext (libpq {mode} semantics): {e}"
                        );
                        connect_notls(&format!("{base_conn_string} sslmode=disable")).await
                    } else {
                        // require: never downgrade to cleartext.
                        Err(e)
                    }
                }
            }
        }
        "verify-ca" | "verify-full" => {
            // Encrypt AND verify the certificate chain + hostname.
            let tls_config = build_tls_config_verify(config)?;
            connect_rustls(&format!("{base_conn_string} sslmode=require"), tls_config).await
        }
        other => Err(ConnectorError::ConfigError(format!(
            "unsupported sslmode {other:?}: expected one of disable, allow, prefer, \
             require, verify-ca, verify-full"
        ))),
    }
}

#[async_trait]
impl SourceConnector for PostgresCdcSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        use std::sync::atomic::Ordering;

        // Reject an unsafe replication slot name BEFORE opening any connection —
        // it is interpolated into replication SQL with no parameter binding.
        validate_slot_name(&self.config.slot_name)?;

        use tracing::{error, info, warn};

        self.running.store(true, Ordering::SeqCst);

        // Base connection string WITHOUT sslmode: connect_pg appends the
        // tokio-postgres-level sslmode (disable/require) that matches the TLS
        // connector it selects for self.config.sslmode. See connect_pg for the
        // full sslmode -> (connector, verifier) mapping.
        let conn_string = base_conn_string(&self.config);

        // Connect honoring sslmode with a real rustls TLS implementation. Unlike
        // the previous NoTls-only path, sslmode=require/verify-* now genuinely
        // encrypt (and, for verify-*, authenticate) the replication stream.
        let client = connect_pg(&conn_string, &self.config).await?;

        let slot_name = self.config.slot_name.clone();
        let publication = self.config.publication.clone();
        let running = self.running.clone();

        info!(
            "PostgreSQL CDC source '{}' starting: slot={}, publication={}",
            self.name, slot_name, publication
        );

        // Try to create the logical replication slot (ignore if already exists).
        // Use test_decoding plugin which outputs human-readable text that
        // parse_change_text() can parse (pgoutput emits binary data).
        let create_slot = format!(
            "SELECT pg_create_logical_replication_slot('{}', 'test_decoding')",
            slot_name
        );
        match client.simple_query(&create_slot).await {
            Ok(_) => info!("Created replication slot: {}", slot_name),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("already exists") {
                    info!("Replication slot '{}' already exists", slot_name);
                } else {
                    warn!("Could not create replication slot: {}", e);
                }
            }
        }

        // Drain any pre-existing WAL changes in the slot (e.g. from CI setup
        // or previous operations) so we only see changes made AFTER start().
        let drain_query = format!(
            "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, NULL)",
            slot_name
        );
        match client.query(&*drain_query, &[]).await {
            Ok(rows) => {
                if !rows.is_empty() {
                    info!(
                        "Drained {} stale WAL entries from slot '{}'",
                        rows.len(),
                        slot_name
                    );
                }
            }
            Err(e) => warn!("Could not drain stale WAL: {}", e),
        }

        let client = std::sync::Arc::new(client);

        // Use polling approach: pg_logical_slot_get_changes() to consume WAL changes.
        // This is more portable than the streaming replication protocol and works
        // with standard tokio-postgres without needing copy_both_simple.
        tokio::spawn(async move {
            info!(
                "PostgreSQL CDC polling loop started for slot '{}'",
                slot_name
            );

            let poll_query = format!(
                "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, NULL)",
                slot_name
            );

            while running.load(Ordering::SeqCst) {
                match client.query(&*poll_query, &[]).await {
                    Ok(rows) => {
                        for row in &rows {
                            // pg_logical_slot_get_changes returns (lsn, xid, data)
                            let data: Option<&str> = row.try_get(2).ok();
                            if let Some(change_data) = data {
                                if let Some(event) = parse_change_text(change_data) {
                                    if tx.send(event).await.is_err() {
                                        warn!("CDC channel closed, stopping");
                                        running.store(false, Ordering::SeqCst);
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("CDC poll error: {}", e);
                        // Back off on error
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }

                // Poll interval: 100ms for near-real-time CDC
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            info!(
                "PostgreSQL CDC polling loop stopped for slot '{}'",
                slot_name
            );
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        use std::sync::atomic::Ordering;
        self.running.store(false, Ordering::SeqCst);
        tracing::info!("PostgreSQL CDC source '{}' stopped", self.name);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Parse a text-format change from pg_logical_slot_get_changes into an Event.
///
/// The pgoutput plugin returns binary data, but when accessed via
/// pg_logical_slot_get_changes with the test_decoding plugin, output is text like:
///   "table public.orders: INSERT: id[integer]:1 amount[numeric]:99.99"
///   "table public.orders: UPDATE: id[integer]:1 amount[numeric]:199.99"
///   "table public.orders: DELETE: id[integer]:1"
fn parse_change_text(data: &str) -> Option<Event> {
    // Format: "table <schema>.<table>: <OP>: <col>[<type>]:<value> ..."
    let data = data.trim();
    if !data.starts_with("table ") {
        return None;
    }

    let after_table = &data[6..]; // skip "table "
    let colon_pos = after_table.find(':')?;
    let full_table = &after_table[..colon_pos].trim();
    // Extract table name (strip schema prefix if present)
    let table = full_table.rsplit('.').next().unwrap_or(full_table);

    let rest = after_table[colon_pos + 1..].trim();

    // Parse operation
    let (op, field_str) = if let Some(stripped) = rest.strip_prefix("INSERT:") {
        (CdcOperation::Insert, stripped.trim())
    } else if let Some(stripped) = rest.strip_prefix("UPDATE:") {
        (CdcOperation::Update, stripped.trim())
    } else if let Some(stripped) = rest.strip_prefix("DELETE:") {
        (CdcOperation::Delete, stripped.trim())
    } else {
        return None;
    };

    // Parse field values: "col[type]:value col2[type with spaces]:value2"
    // Type names can contain spaces (e.g. "double precision", "character varying"),
    // so we scan for '[' and ']' boundaries instead of splitting on whitespace.
    let mut fields = Vec::new();
    let bytes = field_str.as_bytes();
    let mut pos = 0;

    while pos < bytes.len() {
        // Skip leading whitespace
        while pos < bytes.len() && bytes[pos] == b' ' {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        // Field name ends at '['
        let name_start = pos;
        while pos < bytes.len() && bytes[pos] != b'[' {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        let col_name = &field_str[name_start..pos];
        pos += 1; // skip '['

        // Type name ends at ']' (may contain spaces like "double precision")
        while pos < bytes.len() && bytes[pos] != b']' {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        pos += 1; // skip ']'

        // Expect ':'
        if pos >= bytes.len() || bytes[pos] != b':' {
            break;
        }
        pos += 1; // skip ':'

        // Value: quoted strings or unquoted tokens
        let value_start = pos;
        if pos < bytes.len() && bytes[pos] == b'\'' {
            // Quoted value — scan to closing quote
            pos += 1;
            while pos < bytes.len() && bytes[pos] != b'\'' {
                pos += 1;
            }
            if pos < bytes.len() {
                pos += 1; // skip closing quote
            }
        } else {
            // Unquoted value — extends until space or end
            while pos < bytes.len() && bytes[pos] != b' ' {
                pos += 1;
            }
        }
        let col_value = &field_str[value_start..pos];

        let value = if col_value == "null" {
            varpulis_core::Value::Null
        } else if let Ok(i) = col_value.parse::<i64>() {
            varpulis_core::Value::Int(i)
        } else if let Ok(f) = col_value.parse::<f64>() {
            varpulis_core::Value::Float(f)
        } else if col_value == "t" || col_value == "true" {
            varpulis_core::Value::Bool(true)
        } else if col_value == "f" || col_value == "false" {
            varpulis_core::Value::Bool(false)
        } else {
            // Strip surrounding quotes if present
            let stripped = col_value
                .strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .unwrap_or(col_value);
            varpulis_core::Value::Str(stripped.into())
        };

        fields.push((col_name.to_string(), value));
    }

    Some(cdc_event(table, op, fields))
}

/// Parse a pgoutput logical replication message into an Event.
///
/// pgoutput message types:
/// - 'R': Relation (table definition)
/// - 'I': Insert
/// - 'U': Update
/// - 'D': Delete
/// - 'B': Begin transaction
/// - 'C': Commit transaction
#[allow(dead_code)] // Binary pgoutput path -- used in tests, will be wired for streaming replication
fn parse_pgoutput_message(data: &[u8]) -> Option<Event> {
    if data.is_empty() {
        return None;
    }

    match data[0] {
        b'I' => {
            // Insert: relation_id(4) + 'N' + tuple_data
            let table = "table"; // In production, map relation_id to table name via Relation messages
            let fields = parse_tuple_data(&data[6..]);
            Some(cdc_event(table, CdcOperation::Insert, fields))
        }
        b'U' => {
            // Update: relation_id(4) + optional old tuple + 'N' + new tuple
            let table = "table";
            let mut fields = Vec::new();
            // Find new tuple data (after 'N' marker)
            if let Some(pos) = data.iter().position(|&b| b == b'N') {
                let new_fields = parse_tuple_data(&data[pos + 1..]);
                for (k, v) in &new_fields {
                    fields.push((format!("new_{}", k), v.clone()));
                }
            }
            // Find old tuple data (after 'O' marker) if present
            if let Some(pos) = data.iter().position(|&b| b == b'O') {
                let end = data.iter().position(|&b| b == b'N').unwrap_or(data.len());
                let old_fields = parse_tuple_data(&data[pos + 1..end]);
                for (k, v) in &old_fields {
                    fields.push((format!("old_{}", k), v.clone()));
                }
            }
            Some(cdc_event(table, CdcOperation::Update, fields))
        }
        b'D' => {
            // Delete: relation_id(4) + key_or_old tuple
            let table = "table";
            let fields = parse_tuple_data(&data[6..]);
            Some(cdc_event(table, CdcOperation::Delete, fields))
        }
        _ => None, // Begin, Commit, Relation, etc. — skip for now
    }
}

/// Parse tuple data from pgoutput format into field name-value pairs.
///
/// Tuple format: num_columns(2) + for each column: type(1) + data
/// Type: 'n' = null, 't' = text, 'b' = binary
#[allow(dead_code)] // Binary pgoutput path -- used in tests, will be wired for streaming replication
fn parse_tuple_data(data: &[u8]) -> Vec<(String, varpulis_core::Value)> {
    let mut fields = Vec::new();
    if data.len() < 2 {
        return fields;
    }

    let num_cols = u16::from_be_bytes([data[0], data[1]]) as usize;
    let mut offset = 2;
    for i in 0..num_cols {
        if offset >= data.len() {
            break;
        }
        let col_type = data[offset];
        offset += 1;

        let field_name = format!("col_{}", i);
        match col_type {
            b'n' => {
                // NULL
                fields.push((field_name, varpulis_core::Value::Null));
            }
            b't' => {
                // Text: length(4) + data
                if offset + 4 > data.len() {
                    break;
                }
                let len = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + len > data.len() {
                    break;
                }
                let text = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
                offset += len;

                // Try to parse as number
                let value = if let Ok(i) = text.parse::<i64>() {
                    varpulis_core::Value::Int(i)
                } else if let Ok(f) = text.parse::<f64>() {
                    varpulis_core::Value::Float(f)
                } else if text == "t" || text == "true" {
                    varpulis_core::Value::Bool(true)
                } else if text == "f" || text == "false" {
                    varpulis_core::Value::Bool(false)
                } else {
                    varpulis_core::Value::Str(text.into())
                };
                fields.push((field_name, value));
            }
            _ => {
                // Unknown column type, skip
                break;
            }
        }
    }

    fields
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use varpulis_core::Value;

    use super::*;

    #[test]
    fn test_cdc_config_builder() {
        let config = PostgresCdcConfig::new("localhost", "mydb")
            .with_port(5433)
            .with_credentials("admin", "secret")
            .with_slot("my_slot")
            .with_publication("my_pub")
            .with_tables(vec!["orders".to_string(), "users".to_string()]);

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5433);
        assert_eq!(config.dbname, "mydb");
        assert_eq!(config.user, "admin");
        assert_eq!(config.password.expose(), "secret");
        assert_eq!(config.slot_name, "my_slot");
        assert_eq!(config.publication, "my_pub");
        assert_eq!(config.tables.len(), 2);
    }

    #[test]
    fn test_cdc_config_defaults() {
        let config = PostgresCdcConfig::new("db.example.com", "analytics");
        assert_eq!(config.port, 5432);
        assert_eq!(config.user, "postgres");
        assert_eq!(config.slot_name, "varpulis_slot");
        assert_eq!(config.publication, "varpulis_pub");
        assert!(config.tables.is_empty());
    }

    #[test]
    fn test_cdc_event_insert() {
        let event = cdc_event(
            "orders",
            CdcOperation::Insert,
            vec![
                ("order_id".to_string(), Value::Int(42)),
                ("amount".to_string(), Value::Float(99.99)),
                ("customer".to_string(), Value::Str("alice".into())),
            ],
        );

        assert_eq!(event.event_type.as_ref(), "orders.INSERT");
        assert_eq!(event.get("_table"), Some(&Value::Str("orders".into())));
        assert_eq!(event.get("_op"), Some(&Value::Str("INSERT".into())));
        assert_eq!(event.get("order_id"), Some(&Value::Int(42)));
        assert_eq!(event.get("amount"), Some(&Value::Float(99.99)));
        assert_eq!(event.get("customer"), Some(&Value::Str("alice".into())));
    }

    #[test]
    fn test_cdc_event_update() {
        let event = cdc_event(
            "users",
            CdcOperation::Update,
            vec![
                (
                    "new_email".to_string(),
                    Value::Str("new@example.com".into()),
                ),
                (
                    "old_email".to_string(),
                    Value::Str("old@example.com".into()),
                ),
            ],
        );

        assert_eq!(event.event_type.as_ref(), "users.UPDATE");
        assert_eq!(event.get("_op"), Some(&Value::Str("UPDATE".into())));
        assert_eq!(
            event.get("new_email"),
            Some(&Value::Str("new@example.com".into()))
        );
        assert_eq!(
            event.get("old_email"),
            Some(&Value::Str("old@example.com".into()))
        );
    }

    #[test]
    fn test_cdc_event_delete() {
        let event = cdc_event(
            "orders",
            CdcOperation::Delete,
            vec![("order_id".to_string(), Value::Int(7))],
        );

        assert_eq!(event.event_type.as_ref(), "orders.DELETE");
        assert_eq!(event.get("_op"), Some(&Value::Str("DELETE".into())));
        assert_eq!(event.get("order_id"), Some(&Value::Int(7)));
    }

    #[test]
    fn test_cdc_operation_display() {
        assert_eq!(CdcOperation::Insert.as_str(), "INSERT");
        assert_eq!(CdcOperation::Update.as_str(), "UPDATE");
        assert_eq!(CdcOperation::Delete.as_str(), "DELETE");
        assert_eq!(format!("{}", CdcOperation::Insert), "INSERT");
    }

    #[test]
    fn test_postgres_cdc_source_name() {
        let config = PostgresCdcConfig::new("localhost", "mydb");
        let source = PostgresCdcSource::new("pg-cdc", config);
        assert_eq!(source.name(), "pg-cdc");
    }

    #[test]
    fn test_parse_tuple_data_text_columns() {
        // Build a tuple with 2 text columns:
        // num_cols: 2, col0: 't' + len(3) + "42\0"... col1: 't' + len(5) + "hello"
        let mut data = Vec::new();
        data.extend_from_slice(&2u16.to_be_bytes()); // num_cols = 2
                                                     // Column 0: text "42"
        data.push(b't');
        data.extend_from_slice(&2u32.to_be_bytes());
        data.extend_from_slice(b"42");
        // Column 1: text "hello"
        data.push(b't');
        data.extend_from_slice(&5u32.to_be_bytes());
        data.extend_from_slice(b"hello");

        let fields = parse_tuple_data(&data);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "col_0");
        assert_eq!(fields[0].1, Value::Int(42)); // parsed as integer
        assert_eq!(fields[1].0, "col_1");
        assert_eq!(fields[1].1, Value::Str("hello".into()));
    }

    #[test]
    fn test_parse_tuple_data_null_column() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_be_bytes()); // num_cols = 1
        data.push(b'n'); // NULL

        let fields = parse_tuple_data(&data);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, Value::Null);
    }

    #[test]
    fn test_parse_tuple_data_empty() {
        let fields = parse_tuple_data(&[]);
        assert!(fields.is_empty());
    }

    #[test]
    fn test_parse_change_text_insert() {
        let data = "table public.orders: INSERT: id[integer]:42 amount[numeric]:99.99 customer[text]:'alice'";
        let event = parse_change_text(data);
        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.event_type.as_ref(), "orders.INSERT");
        assert_eq!(event.get("id"), Some(&Value::Int(42)));
        assert_eq!(event.get("amount"), Some(&Value::Float(99.99)));
        assert_eq!(event.get("customer"), Some(&Value::Str("alice".into())));
    }

    #[test]
    fn test_parse_change_text_update() {
        let data = "table public.users: UPDATE: id[integer]:1 email[text]:'new@example.com'";
        let event = parse_change_text(data);
        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.event_type.as_ref(), "users.UPDATE");
        assert_eq!(event.get("_op"), Some(&Value::Str("UPDATE".into())));
    }

    #[test]
    fn test_parse_change_text_delete() {
        let data = "table public.orders: DELETE: id[integer]:7";
        let event = parse_change_text(data);
        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.event_type.as_ref(), "orders.DELETE");
        assert_eq!(event.get("id"), Some(&Value::Int(7)));
    }

    #[test]
    fn test_parse_change_text_multiword_type() {
        // test_decoding outputs "double precision" (with space) for FLOAT8/DOUBLE PRECISION columns
        let data = "table public.orders: INSERT: id[integer]:1 customer[text]:'alice' amount[double precision]:99.99 status[text]:'pending'";
        let event = parse_change_text(data);
        assert!(
            event.is_some(),
            "should parse types with spaces like 'double precision'"
        );
        let event = event.unwrap();
        assert_eq!(event.event_type.as_ref(), "orders.INSERT");
        assert_eq!(event.get("id"), Some(&Value::Int(1)));
        assert_eq!(event.get("customer"), Some(&Value::Str("alice".into())));
        assert_eq!(event.get("amount"), Some(&Value::Float(99.99)));
        assert_eq!(event.get("status"), Some(&Value::Str("pending".into())));
    }

    #[test]
    fn test_parse_change_text_invalid() {
        assert!(parse_change_text("BEGIN").is_none());
        assert!(parse_change_text("COMMIT").is_none());
        assert!(parse_change_text("").is_none());
    }

    // --- Replication slot-name validation (SQL-injection guard) ---

    #[test]
    fn validate_slot_name_accepts_legitimate_names() {
        assert!(validate_slot_name("varpulis_slot").is_ok());
        assert!(validate_slot_name("my_slot").is_ok());
        assert!(validate_slot_name("slot123").is_ok());
        assert!(validate_slot_name("a").is_ok());
        assert!(
            validate_slot_name(&"x".repeat(63)).is_ok(),
            "63 chars is the max"
        );
    }

    #[test]
    fn validate_slot_name_rejects_injection_and_invalid() {
        assert!(validate_slot_name("").is_err(), "empty");
        assert!(validate_slot_name(&"x".repeat(64)).is_err(), "too long");
        for bad in [
            "Upper",
            "has space",
            "slot'; DROP TABLE users; --",
            "a', 'test_decoding'); DROP TABLE t--",
            "slot-name",
            "slot.name",
        ] {
            assert!(validate_slot_name(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[tokio::test]
    async fn start_rejects_injection_slot_name_before_connecting() {
        // A malicious slot name must be rejected by validation BEFORE any
        // connection is attempted — so `start` returns a ConfigError (not a
        // ConnectionFailed) and never touches Postgres. Fail-before: without the
        // guard, `start` proceeds to connect and the error is a connection
        // failure, so the message check below fails.
        let config =
            PostgresCdcConfig::new("localhost", "db").with_slot("evil'; DROP TABLE audit_log; --");
        let mut source = PostgresCdcSource::new("cdc-test", config);
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let err = source
            .start(tx)
            .await
            .expect_err("injection slot name must be rejected");
        assert!(
            err.to_string().contains("invalid replication slot name"),
            "must be rejected by slot validation, not a connection attempt: {err}"
        );
    }
}

// =============================================================================
// TLS integration tests
//
// These exercise connect_pg against a REAL TLS-enabled PostgreSQL (a container).
// They skip gracefully (eprintln + return) when no such server is reachable, so
// CI without one stays green — mirroring the MQTT real-broker test. Bring up the
// server with the recipe in the connector's test docs and point the tests at it
// via VARPULIS_TLS_PG_PORT / VARPULIS_TLS_PG_HOST / VARPULIS_TLS_PG_CA.
// =============================================================================

#[cfg(test)]
mod tls_integration {
    use super::*;

    /// Resolve the TLS PostgreSQL test target `(host, port)` and confirm the
    /// port is open, else return `None` so the caller skips. Host defaults to
    /// `localhost` (the server cert's SAN); port to 5433. Both are overridable
    /// via env so the same tests can target any TLS Postgres.
    fn tls_pg_target() -> Option<(String, u16)> {
        use std::net::{TcpStream, ToSocketAddrs};
        let host =
            std::env::var("VARPULIS_TLS_PG_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port: u16 = std::env::var("VARPULIS_TLS_PG_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5433);
        let reachable = (host.as_str(), port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .and_then(|addr| {
                TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500)).ok()
            })
            .is_some();
        reachable.then_some((host, port))
    }

    fn target_config(host: &str, port: u16, sslmode: &str) -> PostgresCdcConfig {
        PostgresCdcConfig::new(host, "postgres")
            .with_port(port)
            .with_credentials("postgres", "")
            .with_sslmode(sslmode)
    }

    /// PRIMARY GATE — `sslmode=require` genuinely uses TLS.
    ///
    /// Against a PostgreSQL that REQUIRES SSL (its `pg_hba.conf` rejects non-SSL
    /// connections), an `sslmode=require` connect must SUCCEED, which is only
    /// possible if real TLS is negotiated.
    ///
    /// Fail-before: force the require arm to `connect_notls` -> the SSL-only
    /// server rejects the plaintext connection -> `connect_pg` errors ->
    /// `expect` panics (RED). Pass-after: rustls TLS -> connects and a trivial
    /// query returns 1 (GREEN).
    #[tokio::test]
    async fn sslmode_require_negotiates_real_tls() {
        let Some((host, port)) = tls_pg_target() else {
            eprintln!("[skip] TLS PostgreSQL not reachable (set VARPULIS_TLS_PG_PORT)");
            return;
        };
        let config = target_config(&host, port, "require");
        let client = connect_pg(&base_conn_string(&config), &config)
            .await
            .expect("sslmode=require must negotiate TLS against the SSL-requiring server");
        let rows = client
            .query("SELECT 1::int AS one", &[])
            .await
            .expect("query over the TLS connection");
        let one: i32 = rows[0].get("one");
        assert_eq!(one, 1, "the TLS connection must be usable");
    }

    /// GUARD — `sslmode=verify-full` cannot silently skip verification.
    ///
    /// With NO `ssl_ca_location` (system roots only), verify-full against a
    /// SELF-SIGNED server cert must FAIL with a verification error, proving the
    /// verify path uses the real WebPki verifier.
    ///
    /// Fail-before: point the verify arm at `build_tls_config_noverify` -> the
    /// self-signed cert is wrongly accepted -> `connect_pg` returns Ok ->
    /// `expect_err` panics (RED). Pass-after: WebPki rejects the untrusted
    /// self-signed cert -> `connect_pg` errors (GREEN).
    #[tokio::test]
    async fn sslmode_verify_full_rejects_untrusted_self_signed() {
        let Some((host, port)) = tls_pg_target() else {
            eprintln!("[skip] TLS PostgreSQL not reachable (set VARPULIS_TLS_PG_PORT)");
            return;
        };
        let config = target_config(&host, port, "verify-full");
        let err = connect_pg(&base_conn_string(&config), &config)
            .await
            .expect_err("verify-full with system roots must reject the self-signed server cert");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("certificate")
                || msg.contains("verif")
                || msg.contains("unknown")
                || msg.contains("self-signed")
                || msg.contains("tls"),
            "expected a certificate-verification failure, got: {err}"
        );
    }

    /// NICE-TO-HAVE — verify-full succeeds when the self-signed cert is pinned as
    /// the CA and the host matches the cert SAN. Requires VARPULIS_TLS_PG_CA to
    /// point at the server cert PEM; skips otherwise.
    #[tokio::test]
    async fn sslmode_verify_full_succeeds_with_pinned_ca() {
        let Some((host, port)) = tls_pg_target() else {
            eprintln!("[skip] TLS PostgreSQL not reachable");
            return;
        };
        let Ok(ca_path) = std::env::var("VARPULIS_TLS_PG_CA") else {
            eprintln!("[skip] VARPULIS_TLS_PG_CA not set (path to server self-signed cert PEM)");
            return;
        };
        let config = target_config(&host, port, "verify-full").with_ca_cert(&ca_path);
        let client = connect_pg(&base_conn_string(&config), &config)
            .await
            .expect("verify-full with the pinned CA and SAN-matching host must connect");
        let rows = client
            .query("SELECT 1::int AS one", &[])
            .await
            .expect("query over the verified TLS connection");
        let one: i32 = rows[0].get("one");
        assert_eq!(one, 1);
    }
}
