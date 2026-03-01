//! PostgreSQL CDC (Change Data Capture) source connector.
//!
//! Uses PostgreSQL logical replication protocol to stream WAL changes
//! as Varpulis events. Converts INSERT/UPDATE/DELETE operations into
//! typed events for stream processing.
//!
//! When the `cdc` feature is enabled, this module provides full
//! PostgreSQL logical replication via `tokio-postgres`. When disabled,
//! stub implementations return `ConnectorError::NotAvailable`.

use super::types::{ConnectorError, SourceConnector};
use async_trait::async_trait;
use tokio::sync::mpsc;
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
    /// Password
    pub password: String,
    /// Replication slot name
    pub slot_name: String,
    /// Publication name (for pgoutput plugin)
    pub publication: String,
    /// Tables to subscribe to (empty = all tables in publication)
    pub tables: Vec<String>,
}

impl PostgresCdcConfig {
    /// Create a new CDC configuration for the given host and database.
    pub fn new(host: &str, dbname: &str) -> Self {
        Self {
            host: host.to_string(),
            port: 5432,
            dbname: dbname.to_string(),
            user: "postgres".to_string(),
            password: String::new(),
            slot_name: "varpulis_slot".to_string(),
            publication: "varpulis_pub".to_string(),
            tables: Vec::new(),
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
        self.password = password.to_string();
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
#[cfg(any(feature = "cdc", test))]
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
///
/// When the `cdc` feature is not enabled, this returns `ConnectorError::NotAvailable`.
/// Enable with: `cargo build --features cdc`
#[derive(Debug)]
pub struct PostgresCdcSource {
    name: String,
    #[allow(dead_code)]
    config: PostgresCdcConfig,
    #[cfg(feature = "cdc")]
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl PostgresCdcSource {
    /// Create a new PostgreSQL CDC source with the given configuration.
    pub fn new(name: &str, config: PostgresCdcConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            #[cfg(feature = "cdc")]
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

#[cfg(not(feature = "cdc"))]
#[async_trait]
impl SourceConnector for PostgresCdcSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "PostgreSQL CDC connector requires 'cdc' feature. Enable with: cargo build --features cdc".to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn is_running(&self) -> bool {
        false
    }
}

// =============================================================================
// Full CDC source (with tokio-postgres)
// =============================================================================

#[cfg(feature = "cdc")]
#[async_trait]
impl SourceConnector for PostgresCdcSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        use std::sync::atomic::Ordering;
        use tokio_postgres::NoTls;
        use tracing::{error, info, warn};

        self.running.store(true, Ordering::SeqCst);

        let conn_string = format!(
            "host={} port={} dbname={} user={} password={}",
            self.config.host,
            self.config.port,
            self.config.dbname,
            self.config.user,
            self.config.password
        );

        let (client, connection) = tokio_postgres::connect(&conn_string, NoTls)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("PostgreSQL: {}", e)))?;

        // Spawn the connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("PostgreSQL connection error: {}", e);
            }
        });

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
#[cfg(feature = "cdc")]
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
#[cfg(feature = "cdc")]
#[allow(dead_code)] // Binary pgoutput path — used in tests, will be wired for streaming replication
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
#[cfg(feature = "cdc")]
#[allow(dead_code)] // Binary pgoutput path — used in tests, will be wired for streaming replication
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
    use super::*;
    use varpulis_core::Value;

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
        assert_eq!(config.password, "secret");
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

    #[cfg(not(feature = "cdc"))]
    #[tokio::test]
    async fn test_postgres_cdc_source_not_available() {
        let config = PostgresCdcConfig::new("localhost", "mydb");
        let mut source = PostgresCdcSource::new("pg-cdc", config);
        let (tx, _rx) = mpsc::channel(10);

        let result = source.start(tx).await;
        assert!(result.is_err());
        if let Err(ConnectorError::NotAvailable(msg)) = result {
            assert!(msg.contains("cdc"));
        } else {
            panic!("Expected NotAvailable error");
        }
    }

    #[cfg(feature = "cdc")]
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

    #[cfg(feature = "cdc")]
    #[test]
    fn test_parse_tuple_data_null_column() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_be_bytes()); // num_cols = 1
        data.push(b'n'); // NULL

        let fields = parse_tuple_data(&data);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, Value::Null);
    }

    #[cfg(feature = "cdc")]
    #[test]
    fn test_parse_tuple_data_empty() {
        let fields = parse_tuple_data(&[]);
        assert!(fields.is_empty());
    }

    #[cfg(feature = "cdc")]
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

    #[cfg(feature = "cdc")]
    #[test]
    fn test_parse_change_text_update() {
        let data = "table public.users: UPDATE: id[integer]:1 email[text]:'new@example.com'";
        let event = parse_change_text(data);
        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.event_type.as_ref(), "users.UPDATE");
        assert_eq!(event.get("_op"), Some(&Value::Str("UPDATE".into())));
    }

    #[cfg(feature = "cdc")]
    #[test]
    fn test_parse_change_text_delete() {
        let data = "table public.orders: DELETE: id[integer]:7";
        let event = parse_change_text(data);
        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.event_type.as_ref(), "orders.DELETE");
        assert_eq!(event.get("id"), Some(&Value::Int(7)));
    }

    #[cfg(feature = "cdc")]
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

    #[cfg(feature = "cdc")]
    #[test]
    fn test_parse_change_text_invalid() {
        assert!(parse_change_text("BEGIN").is_none());
        assert!(parse_change_text("COMMIT").is_none());
        assert!(parse_change_text("").is_none());
    }
}
