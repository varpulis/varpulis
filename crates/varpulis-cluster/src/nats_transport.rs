//! NATS transport layer for cluster communication.
//!
//! Provides subject helpers and utility functions for NATS-based
//! request/reply and pub/sub communication between coordinator and workers.

#[cfg(feature = "nats-transport")]
use serde::{de::DeserializeOwned, Serialize};

// ---------------------------------------------------------------------------
// Subject helpers
// ---------------------------------------------------------------------------

const PREFIX: &str = "varpulis.cluster";

/// Subject for worker registration (request/reply).
pub fn subject_register() -> String {
    format!("{PREFIX}.register")
}

/// Subject for worker heartbeats (pub).
pub fn subject_heartbeat(worker_id: &str) -> String {
    format!("{PREFIX}.heartbeat.{worker_id}")
}

/// Subject for coordinator→worker commands (request/reply).
pub fn subject_cmd(worker_id: &str, cmd: &str) -> String {
    format!("{PREFIX}.cmd.{worker_id}.{cmd}")
}

/// Wildcard subject for all commands to a specific worker.
pub fn subject_cmd_wildcard(worker_id: &str) -> String {
    format!("{PREFIX}.cmd.{worker_id}.>")
}

/// Subject for inter-pipeline event routing (pub/sub).
pub fn subject_pipeline(group: &str, from: &str, to: &str) -> String {
    format!("{PREFIX}.pipeline.{group}.{from}.{to}")
}

/// Subject for Raft RPCs to a specific node (request/reply).
pub fn subject_raft(node_id: u64, rpc: &str) -> String {
    format!("{PREFIX}.raft.{node_id}.{rpc}")
}

/// Wildcard subject for all Raft RPCs to a specific node.
pub fn subject_raft_wildcard(node_id: u64) -> String {
    format!("{PREFIX}.raft.{node_id}.>")
}

// ---------------------------------------------------------------------------
// Connection + request/reply utilities (requires nats-transport feature)
// ---------------------------------------------------------------------------

/// Connect to a NATS server.
#[cfg(feature = "nats-transport")]
pub async fn connect_nats(url: &str) -> Result<async_nats::Client, async_nats::ConnectError> {
    async_nats::connect(url).await
}

/// Send a JSON request and await a JSON response (request/reply pattern).
#[cfg(feature = "nats-transport")]
pub async fn nats_request<Req: Serialize, Resp: DeserializeOwned>(
    client: &async_nats::Client,
    subject: &str,
    payload: &Req,
    timeout: std::time::Duration,
) -> Result<Resp, NatsTransportError> {
    let bytes = serde_json::to_vec(payload).map_err(NatsTransportError::Serialize)?;
    let resp = tokio::time::timeout(timeout, client.request(subject.to_string(), bytes.into()))
        .await
        .map_err(|_| NatsTransportError::Timeout)?
        .map_err(NatsTransportError::Request)?;
    serde_json::from_slice(&resp.payload).map_err(NatsTransportError::Deserialize)
}

/// Publish a JSON payload (fire-and-forget).
#[cfg(feature = "nats-transport")]
pub async fn nats_publish<T: Serialize>(
    client: &async_nats::Client,
    subject: &str,
    payload: &T,
) -> Result<(), NatsTransportError> {
    let bytes = serde_json::to_vec(payload).map_err(NatsTransportError::Serialize)?;
    client
        .publish(subject.to_string(), bytes.into())
        .await
        .map_err(NatsTransportError::Publish)
}

/// Errors from NATS transport operations.
#[cfg(feature = "nats-transport")]
#[derive(Debug, thiserror::Error)]
pub enum NatsTransportError {
    #[error("serialization failed: {0}")]
    Serialize(serde_json::Error),
    #[error("deserialization failed: {0}")]
    Deserialize(serde_json::Error),
    #[error("NATS request failed: {0}")]
    Request(async_nats::RequestError),
    #[error("NATS publish failed: {0}")]
    Publish(async_nats::PublishError),
    #[error("request timed out")]
    Timeout,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_register() {
        assert_eq!(subject_register(), "varpulis.cluster.register");
    }

    #[test]
    fn test_subject_heartbeat() {
        assert_eq!(subject_heartbeat("w0"), "varpulis.cluster.heartbeat.w0");
    }

    #[test]
    fn test_subject_cmd() {
        assert_eq!(
            subject_cmd("w1", "deploy"),
            "varpulis.cluster.cmd.w1.deploy"
        );
        assert_eq!(
            subject_cmd("w1", "inject"),
            "varpulis.cluster.cmd.w1.inject"
        );
    }

    #[test]
    fn test_subject_cmd_wildcard() {
        assert_eq!(subject_cmd_wildcard("w1"), "varpulis.cluster.cmd.w1.>");
    }

    #[test]
    fn test_subject_pipeline() {
        assert_eq!(
            subject_pipeline("mandelbrot", "ingress", "row0"),
            "varpulis.cluster.pipeline.mandelbrot.ingress.row0"
        );
    }

    #[test]
    fn test_subject_raft() {
        assert_eq!(subject_raft(1, "vote"), "varpulis.cluster.raft.1.vote");
        assert_eq!(subject_raft(3, "append"), "varpulis.cluster.raft.3.append");
    }

    #[test]
    fn test_subject_raft_wildcard() {
        assert_eq!(subject_raft_wildcard(2), "varpulis.cluster.raft.2.>");
    }
}
