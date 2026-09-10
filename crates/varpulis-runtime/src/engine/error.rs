//! Typed error hierarchy for the Varpulis engine.

#[cfg(feature = "async-runtime")]
use varpulis_connectors::SinkError;

#[cfg(feature = "async-runtime")]
use crate::enrichment::EnrichmentError;
use crate::persistence::StoreError;

/// Top-level error type returned by engine public methods.
///
/// Marked `#[non_exhaustive]`: the engine grows new failure modes as the
/// runtime grows, and a downstream `match` should not have to be edited every
/// time one is added. Match a variant you care about and let `_` cover the
/// rest.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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

#[cfg(test)]
mod semver_exception_tests {
    /// `Cargo.toml` allows `enum_variant_added` and `enum_marked_non_exhaustive`
    /// for this crate. Both were taken deliberately against the 0.11.0 baseline
    /// (see the comment on that block), and both stop being needed the moment
    /// the version moves past 0.11.x, because cargo-semver-checks then compares
    /// against a baseline that already contains the change.
    ///
    /// An exception nobody is reminded to remove is how a gate goes quietly
    /// blind: `enum_variant_added` is allowed crate-wide, so while it stands it
    /// also covers every *other* public enum here. This test is the reminder.
    /// It fails on the release commit that bumps the version, which is exactly
    /// when the block above must be deleted.
    #[test]
    fn semver_exceptions_expire_with_their_baseline() {
        let version = env!("CARGO_PKG_VERSION");
        assert!(
            version.starts_with("0.11."),
            "varpulis-runtime is now {version}, past the 0.11.0 baseline the \
             cargo-semver-checks lint exceptions were taken against. Delete the \
             [package.metadata.cargo-semver-checks.lints] block in \
             crates/varpulis-runtime/Cargo.toml and this test with it."
        );
    }
}
