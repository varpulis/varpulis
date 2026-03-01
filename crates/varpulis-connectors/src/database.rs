//! Database connector (PostgreSQL/MySQL/SQLite with sqlx)

use super::component::{ConnectorComponentInfo, ConnectorFactory};
#[cfg(feature = "database")]
use super::helpers::json_to_value;
use super::types::{ConnectorError, SinkConnector, SourceConnector};
use async_trait::async_trait;
use tokio::sync::mpsc;
use varpulis_core::security::SecretString;
use varpulis_core::Event;

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

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
///
/// Allows alphanumeric, underscores, dots (for schema-qualified names like `public.events`).
/// Rejects anything that could be used for SQL injection.
fn validate_table_name(table: &str) -> Result<(), ConnectorError> {
    if table.is_empty() {
        return Err(ConnectorError::ConfigError(
            "Table name must not be empty".to_string(),
        ));
    }
    // Must match: starts with letter or underscore, rest is alphanumeric, underscore, or dot
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
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Connection string (zeroized on drop — may contain credentials)
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

#[cfg(feature = "database")]
mod database_impl {
    use super::*;
    use sqlx::pool::PoolOptions;
    use sqlx::{AnyPool, Row};
    use tracing::{error, info};

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

                                // Try to get common columns
                                if let Ok(data) = row.try_get::<String, _>("data") {
                                    if let Ok(json) =
                                        serde_json::from_str::<serde_json::Value>(&data)
                                    {
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
}

#[cfg(feature = "database")]
pub use database_impl::{DatabaseSink, DatabaseSource};

/// Database source stub (requires `database` feature for full functionality).
#[cfg(not(feature = "database"))]
#[derive(Debug)]
pub struct DatabaseSource {
    name: String,
    #[allow(dead_code)]
    config: DatabaseConfig,
}

#[cfg(not(feature = "database"))]
impl DatabaseSource {
    /// Create a new database source stub.
    pub fn new(name: &str, config: DatabaseConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "database"))]
#[async_trait]
impl SourceConnector for DatabaseSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Database connector requires 'database' feature".to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn is_running(&self) -> bool {
        false
    }
}

/// Database sink stub (requires `database` feature for full functionality).
#[cfg(not(feature = "database"))]
#[derive(Debug)]
pub struct DatabaseSink {
    name: String,
    #[allow(dead_code)]
    config: DatabaseConfig,
}

#[cfg(not(feature = "database"))]
impl DatabaseSink {
    /// Create a new database sink stub.
    pub async fn new(name: &str, config: DatabaseConfig) -> Result<Self, ConnectorError> {
        Ok(Self {
            name: name.to_string(),
            config,
        })
    }
}

#[cfg(not(feature = "database"))]
#[async_trait]
impl SinkConnector for DatabaseSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Database connector requires 'database' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
