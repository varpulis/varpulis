//! Typed error hierarchy for the Varpulis engine.

use crate::enrichment::EnrichmentError;
use crate::persistence::StoreError;
use varpulis_connectors::SinkError;

/// Top-level error type returned by engine public methods.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Compilation or program-loading error (bad VPL, missing streams, etc.)
    #[error("compilation error: {0}")]
    Compilation(String),

    /// Referenced stream does not exist.
    #[error("stream not found: {0}")]
    StreamNotFound(String),

    /// Sink I/O or protocol error.
    #[error("sink error: {0}")]
    Sink(#[from] SinkError),

    /// Enrichment lookup error.
    #[error("enrichment error: {0}")]
    Enrichment(#[from] EnrichmentError),

    /// State-store / persistence error.
    #[error("store error: {0}")]
    Store(#[from] StoreError),

    /// Runtime pipeline error (evaluation, pattern matching, etc.)
    #[error("pipeline error: {0}")]
    Pipeline(String),
}
