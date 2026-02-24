//! PostgreSQL database layer for Varpulis Cloud.
//!
//! Provides typed models, a repository of CRUD operations, and connection pool
//! management for the Varpulis multi-tenant platform.

pub mod models;
pub mod pool;
pub mod repo;

pub use models::*;
pub use sqlx::PgPool;

/// Errors originating from the database layer.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// A query or connection error from sqlx.
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// Failed to create or configure the connection pool.
    #[error("pool error: {0}")]
    Pool(String),

    /// A migration failed to apply.
    #[error("migration error: {0}")]
    Migration(String),
}
