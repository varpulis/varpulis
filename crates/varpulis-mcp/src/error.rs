//! Error types for the Varpulis MCP server.

use rmcp::model::{CallToolResult, Content};

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("Cannot reach coordinator at {url}: {source}")]
    Unreachable { url: String, source: reqwest::Error },

    #[error("Authentication failed (HTTP 401). Check your API key.")]
    Unauthorized,

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Coordinator error (HTTP {status}): {message}")]
    ApiError { status: u16, message: String },

    #[error("VPL parse error: {0}")]
    ParseError(String),

    #[error("VPL validation error: {0}")]
    ValidationError(String),

    #[error("{0}")]
    Other(String),
}

impl CoordinatorError {
    pub fn into_tool_result(self) -> CallToolResult {
        CallToolResult::error(vec![Content::text(self.to_string())])
    }
}

impl From<reqwest::Error> for CoordinatorError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_connect() || err.is_timeout() {
            CoordinatorError::Unreachable {
                url: err
                    .url()
                    .map(|u| u.to_string())
                    .unwrap_or_else(|| "unknown".into()),
                source: err,
            }
        } else {
            CoordinatorError::Other(err.to_string())
        }
    }
}
