//! Typed error hierarchy for the Varpulis engine.

#[cfg(feature = "async-runtime")]
use varpulis_connectors::SinkError;

#[cfg(feature = "async-runtime")]
use crate::enrichment::EnrichmentError;
use crate::persistence::StoreError;

/// Top-level error type returned by engine public methods.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Compilation or program-loading error (bad VPL, missing streams, etc.)
    #[error("compilation error: {0}")]
    Compilation(String),

    /// Referenced stream does not exist.
    #[error("stream not found: {0}")]
    StreamNotFound(String),

    /// This engine has been fenced: another worker owns its partition now.
    ///
    /// Returned by every ingestion entry point once the fence flag is set, so a
    /// worker that was partitioned, declared dead, migrated away from and then
    /// came back cannot keep processing — no emits, no state mutation, no
    /// offset advancement. Refusing at the entry point rather than at the sink
    /// is deliberate: a fenced worker has lost ownership of the partition, so
    /// there is nothing it may correctly do with an event.
    #[error("engine is fenced: another worker owns this partition")]
    Fenced,

    /// Sink I/O or protocol error (async-runtime only).
    #[cfg(feature = "async-runtime")]
    #[error("sink error: {0}")]
    Sink(#[from] SinkError),

    /// Enrichment lookup error (async-runtime only).
    #[cfg(feature = "async-runtime")]
    #[error("enrichment error: {0}")]
    Enrichment(#[from] EnrichmentError),

    /// State-store / persistence error.
    #[error("store error: {0}")]
    Store(#[from] StoreError),

    /// Runtime pipeline error (evaluation, pattern matching, etc.)
    #[error("pipeline error: {0}")]
    Pipeline(String),
}
