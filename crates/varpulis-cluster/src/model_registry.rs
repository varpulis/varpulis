//! ONNX Model Registry for managing ML models in the cluster.
//!
//! Models are stored in S3/MinIO and metadata is replicated via Raft.

use serde::{Deserialize, Serialize};

/// Metadata for a registered ONNX model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRegistryEntry {
    pub name: String,
    pub s3_key: String,
    pub format: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub size_bytes: u64,
    pub uploaded_at: String,
    #[serde(default)]
    pub description: String,
}
