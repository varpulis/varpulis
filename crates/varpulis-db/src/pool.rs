use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::DbError;

/// Create a PostgreSQL connection pool.
pub async fn create_pool(database_url: &str) -> Result<PgPool, DbError> {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(database_url)
        .await
        .map_err(|e| DbError::Pool(e.to_string()))?;

    tracing::info!("PostgreSQL connection pool created");
    Ok(pool)
}

/// Run all pending migrations from the embedded migrations directory.
pub async fn run_migrations(pool: &PgPool) -> Result<(), DbError> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|e| DbError::Migration(e.to_string()))?;

    tracing::info!("Database migrations complete");
    Ok(())
}
