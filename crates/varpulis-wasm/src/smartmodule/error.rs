//! SmartModule error types.

/// Errors during SmartModule loading or execution.
#[derive(Debug, thiserror::Error)]
pub enum SmartModuleError {
    /// WASM module compilation failed.
    #[error("compilation error: {0}")]
    Compilation(String),

    /// Required export is missing from the WASM module.
    #[error("missing export: {0}")]
    MissingExport(String),

    /// Export has wrong type signature.
    #[error("invalid export signature for '{name}': {reason}")]
    InvalidSignature {
        /// Export name.
        name: String,
        /// Reason the signature is invalid.
        reason: String,
    },

    /// Runtime execution error.
    #[error("execution error: {0}")]
    Execution(String),

    /// Fuel exhaustion (CPU limit exceeded).
    #[error("fuel exhausted: computation exceeded {limit} instructions")]
    FuelExhausted {
        /// The configured fuel limit.
        limit: u64,
    },

    /// Memory limit exceeded.
    #[error("memory limit exceeded: {requested} bytes requested, {limit} bytes allowed")]
    MemoryLimitExceeded {
        /// Bytes requested.
        requested: usize,
        /// Maximum allowed.
        limit: usize,
    },

    /// Event serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(String),
}
