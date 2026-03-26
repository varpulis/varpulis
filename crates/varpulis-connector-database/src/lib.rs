//! Database connector (PostgreSQL/MySQL/SQLite with sqlx).

use async_trait::async_trait;
use sqlx::pool::PoolOptions;
use sqlx::{AnyPool, Row};
use tokio::sync::mpsc;
use tracing::{error, info};
use varpulis_connector_api::helpers::json_to_value;
use varpulis_connector_api::{
    ConnectorComponentInfo, ConnectorError, ConnectorFactory, SinkConnector, SourceConnector,
};
use varpulis_core::security::SecretString;
use varpulis_core::Event;

static DATABASE_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "database",
    display_name: "Database",
    description: "SQL database connector (PostgreSQL, MySQL, SQLite)",
    feature_flag: "database",
    supports_source: true,
    supports_sink: true,
    supports_managed: false,
    config_params: &[],
};

struct DatabaseFactory;

impl ConnectorFactory for DatabaseFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &DATABASE_INFO
    }
}

inventory::submit! { &DatabaseFactory as &dyn ConnectorFactory }

/// Validate that a table name is safe for interpolation into SQL.
fn validate_table_name(table: &str) -> Result<(), ConnectorError> {
    if table.is_empty() {
        return Err(ConnectorError::ConfigError(
            "Table name must not be empty".to_string(),
        ));
    }
    let valid = table.chars().enumerate().all(|(i, c)| match (i, c) {
        (0, c) => c.is_ascii_alphabetic() || c == '_',
        (_, c) => c.is_ascii_alphanumeric() || c == '_' || c == '.',
    });
    if !valid {
        return Err(ConnectorError::ConfigError(format!(
            "Invalid table name '{table}': must match [a-zA-Z_][a-zA-Z0-9_.]*"
        )));
    }
    Ok(())
}

/// Database configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct DatabaseConfig {
    /// Connection string (zeroized on drop -- may contain credentials)
    pub connection_string: SecretString,
    /// Target table name for queries and inserts.
    pub table: String,
    /// Maximum number of connections in the pool (default: 5).
    pub max_connections: u32,
}

impl DatabaseConfig {
    /// Create a new database configuration with the given connection string and table.
    pub fn new(connection_string: &str, table: &str) -> Result<Self, ConnectorError> {
        validate_table_name(table)?;
        Ok(Self {
            connection_string: SecretString::new(connection_string),
            table: table.to_string(),
            max_connections: 5,
        })
    }

    /// Set the maximum number of connections in the pool.
    pub const fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }
}

/// Ensure default Any drivers are installed (idempotent).
fn ensure_drivers() {
    sqlx::any::install_default_drivers();
}

/// Database source that polls for new events
pub struct DatabaseSource {
    name: String,
    config: DatabaseConfig,
    pool: Option<AnyPool>,
    running: bool,
    last_id: i64,
}

impl std::fmt::Debug for DatabaseSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseSource")
            .field("name", &self.name)
            .field("running", &self.running)
            .field("last_id", &self.last_id)
            .finish_non_exhaustive()
    }
}

impl DatabaseSource {
    /// Create a new database source connector with the given configuration.
    pub fn new(name: &str, config: DatabaseConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            pool: None,
            running: false,
            last_id: 0,
        }
    }
}

#[async_trait]
impl SourceConnector for DatabaseSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        ensure_drivers();
        let pool = PoolOptions::<sqlx::Any>::new()
            .max_connections(self.config.max_connections)
            .connect(self.config.connection_string.expose())
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        self.pool = Some(pool.clone());
        self.running = true;

        let table = self.config.table.clone();
        let name = self.name.clone();
        let mut last_id = self.last_id;

        tokio::spawn(async move {
            info!("Database source {} started, polling table {}", name, table);

            while tx.reserve().await.is_ok() {
                let query = format!(
                    "SELECT * FROM {} WHERE id > {} ORDER BY id LIMIT 100",
                    table, last_id
                );

                match sqlx::query(&query).fetch_all(&pool).await {
                    Ok(rows) => {
                        for row in rows {
                            let id: i64 = row.try_get("id").unwrap_or(0);
                            last_id = last_id.max(id);

                            let event_type: String = row
                                .try_get("event_type")
                                .unwrap_or_else(|_| "DatabaseEvent".to_string());

                            let mut event = Event::new(event_type);

                            if let Ok(data) = row.try_get::<String, _>("data") {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&data) {
                                    if let Some(obj) = json.as_object() {
                                        for (key, value) in obj {
                                            if let Some(v) = json_to_value(value) {
                                                event = event.with_field(key.as_str(), v);
                                            }
                                        }
                                    }
                                }
                            }

                            if tx.send(event).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Database source {} query error: {}", name, e);
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            info!("Database source {} stopped", name);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running = false;
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }
}

/// Database sink that inserts events
pub struct DatabaseSink {
    name: String,
    config: DatabaseConfig,
    pool: AnyPool,
}

impl std::fmt::Debug for DatabaseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseSink")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl DatabaseSink {
    /// Create a new database sink connector, establishing a connection pool.
    pub async fn new(name: &str, config: DatabaseConfig) -> Result<Self, ConnectorError> {
        ensure_drivers();
        let pool = PoolOptions::<sqlx::Any>::new()
            .max_connections(config.max_connections)
            .connect(config.connection_string.expose())
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            name: name.to_string(),
            config,
            pool,
        })
    }
}

#[async_trait]
impl SinkConnector for DatabaseSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let data = String::from_utf8(event.to_sink_payload())
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        let query = format!(
            "INSERT INTO {} (event_type, data, timestamp) VALUES ($1, $2, $3)",
            self.config.table
        );

        sqlx::query(&query)
            .bind(event.event_type.to_string())
            .bind(&data)
            .bind(event.timestamp.to_rfc3339())
            .execute(&self.pool)
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        self.pool.close().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_new_valid() {
        let config = DatabaseConfig::new("sqlite::memory:", "events").unwrap();
        assert_eq!(config.connection_string.expose(), "sqlite::memory:");
        assert_eq!(config.table, "events");
        assert_eq!(config.max_connections, 5);
    }

    #[test]
    fn test_database_config_with_max_connections() {
        let config = DatabaseConfig::new("postgres://localhost/db", "logs")
            .unwrap()
            .with_max_connections(20);
        assert_eq!(config.max_connections, 20);
    }

    #[test]
    fn test_database_config_empty_table_name_rejected() {
        let result = DatabaseConfig::new("sqlite::memory:", "");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConnectorError::ConfigError(_)));
    }

    #[test]
    fn test_validate_table_name_valid_names() {
        assert!(validate_table_name("events").is_ok());
        assert!(validate_table_name("_events").is_ok());
        assert!(validate_table_name("public.events").is_ok());
        assert!(validate_table_name("my_schema.my_table").is_ok());
        assert!(validate_table_name("table123").is_ok());
    }

    #[test]
    fn test_validate_table_name_invalid_names() {
        // Starts with digit
        assert!(validate_table_name("1table").is_err());
        // Contains special chars
        assert!(validate_table_name("table;DROP").is_err());
        assert!(validate_table_name("table name").is_err());
        assert!(validate_table_name("table-name").is_err());
        // Empty
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn test_database_config_sql_injection_prevented() {
        // Table names with SQL injection attempts should be rejected
        let result = DatabaseConfig::new("sqlite::memory:", "events; DROP TABLE users");
        assert!(result.is_err());
    }

    #[test]
    fn test_database_source_initial_state() {
        let config = DatabaseConfig::new("sqlite::memory:", "events").unwrap();
        let source = DatabaseSource::new("db-src", config);
        assert_eq!(source.name, "db-src");
        assert!(!source.running);
        assert_eq!(source.last_id, 0);
        assert!(source.pool.is_none());
    }

    #[test]
    fn test_database_info_static() {
        assert_eq!(DATABASE_INFO.connector_type, "database");
        assert!(DATABASE_INFO.supports_source);
        assert!(DATABASE_INFO.supports_sink);
        assert!(!DATABASE_INFO.supports_managed);
    }
}
