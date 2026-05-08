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
// Distributed checkpoint subjects (Flink-parity Phase 1, Task 1.2)
// ---------------------------------------------------------------------------
//
// Used by the coordinated exactly-once protocol defined in
// [`crate::checkpoint_protocol`]. Barrier requests are sent on the per-worker
// `cmd` namespace so they share the worker's existing wildcard subscription;
// acks/complete/abort live under a dedicated `checkpoint` namespace keyed by
// group id so the coordinator can multiplex many in-flight checkpoints.

/// Coordinator → worker barrier request: "snapshot pipeline at checkpoint id".
///
/// Sits under the per-worker `cmd` prefix so the worker's existing
/// [`subject_cmd_wildcard`] subscription picks it up alongside `deploy`,
/// `inject`, and other commands.
pub fn subject_checkpoint_barrier(worker_id: &str) -> String {
    subject_cmd(worker_id, "checkpoint_barrier")
}

/// Worker → coordinator ack: "I have prepared the snapshot for `checkpoint_id`".
///
/// Keyed by `group_id` so the coordinator subscribes once per in-flight
/// distributed checkpoint and receives acks from every participant in that
/// group.
pub fn subject_checkpoint_ack(group_id: &str) -> String {
    format!("{PREFIX}.checkpoint.ack.{group_id}")
}

/// Coordinator → workers broadcast: "checkpoint is durable, commit and resume".
///
/// All workers running pipelines in `group_id` subscribe to this subject.
pub fn subject_checkpoint_complete(group_id: &str) -> String {
    format!("{PREFIX}.checkpoint.complete.{group_id}")
}

/// Coordinator → workers broadcast: "abort checkpoint, discard prepared state".
///
/// All workers running pipelines in `group_id` subscribe to this subject.
pub fn subject_checkpoint_abort(group_id: &str) -> String {
    format!("{PREFIX}.checkpoint.abort.{group_id}")
}

// ---------------------------------------------------------------------------
// Federation subjects
// ---------------------------------------------------------------------------

const FEDERATION_PREFIX: &str = "varpulis.federation";

/// Subject for federation heartbeats from a region.
pub fn subject_federation_heartbeat(region: &str) -> String {
    format!("{FEDERATION_PREFIX}.heartbeat.{region}")
}

/// Subject for federation catalog sync from a region.
pub fn subject_federation_catalog(region: &str) -> String {
    format!("{FEDERATION_PREFIX}.catalog.{region}")
}

/// Subject for cross-region event routing.
pub fn subject_federation_route(from_region: &str, to_region: &str) -> String {
    format!("{FEDERATION_PREFIX}.route.{from_region}.{to_region}")
}

/// Wildcard subject for all federation messages.
pub fn subject_federation_wildcard() -> String {
    format!("{FEDERATION_PREFIX}.>")
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
///
/// When the `otel` feature is enabled, the current OpenTelemetry trace context
/// is propagated as a `traceparent` NATS header for distributed tracing.
#[cfg(feature = "nats-transport")]
pub async fn nats_publish<T: Serialize>(
    client: &async_nats::Client,
    subject: &str,
    payload: &T,
) -> Result<(), NatsTransportError> {
    let bytes = serde_json::to_vec(payload).map_err(NatsTransportError::Serialize)?;

    #[cfg(feature = "otel")]
    {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let span = tracing::Span::current();
        let context = span.context();
        let span_ref = context.span();
        let span_context = span_ref.span_context();

        if span_context.is_valid() {
            let traceparent = format!(
                "00-{}-{}-{:02x}",
                span_context.trace_id(),
                span_context.span_id(),
                span_context.trace_flags().to_u8(),
            );
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("traceparent", traceparent.as_str());
            return client
                .publish_with_headers(subject.to_string(), headers, bytes.into())
                .await
                .map_err(NatsTransportError::Publish);
        }
    }

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

    #[test]
    fn test_subject_checkpoint_barrier() {
        assert_eq!(
            subject_checkpoint_barrier("w0"),
            "varpulis.cluster.cmd.w0.checkpoint_barrier"
        );
        // Must be a child of the worker's cmd wildcard so the worker's
        // existing subscription picks barriers up.
        let wildcard = subject_cmd_wildcard("w0");
        let prefix = wildcard.trim_end_matches('>');
        assert!(subject_checkpoint_barrier("w0").starts_with(prefix));
    }

    #[test]
    fn test_subject_checkpoint_ack() {
        assert_eq!(
            subject_checkpoint_ack("group-42"),
            "varpulis.cluster.checkpoint.ack.group-42"
        );
    }

    #[test]
    fn test_subject_checkpoint_complete() {
        assert_eq!(
            subject_checkpoint_complete("group-42"),
            "varpulis.cluster.checkpoint.complete.group-42"
        );
    }

    #[test]
    fn test_subject_checkpoint_abort() {
        assert_eq!(
            subject_checkpoint_abort("group-42"),
            "varpulis.cluster.checkpoint.abort.group-42"
        );
    }
}
