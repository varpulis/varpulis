//! Database connector (PostgreSQL/MySQL/SQLite with sqlx)

use super::types::{ConnectorError, SinkConnector, SourceConnector};
use crate::event::Event;
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Database configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub connection_string: String,
    pub table: String,
    pub max_connections: u32,
}

impl DatabaseConfig {
    pub fn new(connection_string: &str, table: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            table: table.to_string(),
            max_connections: 5,
        }
    }

    pub fn with_max_connections(mut self, max: u32) -> Self {
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
                .connect(&self.config.connection_string)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.pool = Some(pool.clone());
            self.running = true;

            let table = self.config.table.clone();
            let name = self.name.clone();
            let mut last_id = self.last_id;

            tokio::spawn(async move {
                info!("Database source {} started, polling table {}", name, table);

                while let Ok(_) = tx.reserve().await {
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
                                                    event = event.with_field(key, v);
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
        pub async fn new(name: &str, config: DatabaseConfig) -> Result<Self, ConnectorError> {
            ensure_drivers();
            let pool = PoolOptions::<sqlx::Any>::new()
                .max_connections(config.max_connections)
                .connect(&config.connection_string)
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

#[cfg(not(feature = "database"))]
pub struct DatabaseSource {
    name: String,
    #[allow(dead_code)]
    config: DatabaseConfig,
}

#[cfg(not(feature = "database"))]
impl DatabaseSource {
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

#[cfg(not(feature = "database"))]
pub struct DatabaseSink {
    name: String,
    #[allow(dead_code)]
    config: DatabaseConfig,
}

#[cfg(not(feature = "database"))]
impl DatabaseSink {
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
