//! MCP resource implementations for Varpulis.

use crate::client::CoordinatorClient;
use rmcp::model::{RawResource, ReadResourceResult, Resource, ResourceContents, ResourceTemplate};
use serde_json::Value;

/// Embedded VPL language reference (static resource).
pub const VPL_REFERENCE: &str = include_str!("vpl_reference.md");

/// URI for the cluster status resource.
pub const CLUSTER_STATUS_URI: &str = "varpulis://cluster/status";
/// URI for the VPL reference resource.
pub const VPL_REFERENCE_URI: &str = "varpulis://docs/vpl-reference";
/// URI for the cluster metrics resource.
pub const CLUSTER_METRICS_URI: &str = "varpulis://cluster/metrics";

fn resource(uri: &str, name: &str, description: &str, mime: &str) -> Resource {
    let mut raw = RawResource::new(uri, name);
    raw.description = Some(description.into());
    raw.mime_type = Some(mime.into());
    Resource::new(raw, None)
}

/// List all available resources.
pub fn list_resources() -> Vec<Resource> {
    vec![
        resource(
            CLUSTER_STATUS_URI,
            "Cluster Status",
            "Live cluster status: workers, pipeline groups, health summary",
            "application/json",
        ),
        resource(
            VPL_REFERENCE_URI,
            "VPL Language Reference",
            "Complete VPL language reference: syntax, operators, connectors, patterns",
            "text/markdown",
        ),
        resource(
            CLUSTER_METRICS_URI,
            "Cluster Metrics",
            "Live cluster metrics: throughput, pipeline stats, scaling recommendations",
            "application/json",
        ),
    ]
}

/// List resource templates (none for now, all resources have fixed URIs).
pub fn list_resource_templates() -> Vec<ResourceTemplate> {
    vec![]
}

/// Read a resource by URI.
pub async fn read_resource(
    uri: &str,
    client: &CoordinatorClient,
) -> Result<ReadResourceResult, String> {
    match uri {
        VPL_REFERENCE_URI => Ok(ReadResourceResult {
            contents: vec![ResourceContents::text(VPL_REFERENCE, VPL_REFERENCE_URI)],
        }),
        CLUSTER_STATUS_URI => read_cluster_status(client).await,
        CLUSTER_METRICS_URI => read_cluster_metrics(client).await,
        _ => Err(format!("Unknown resource URI: {}", uri)),
    }
}

async fn read_cluster_status(client: &CoordinatorClient) -> Result<ReadResourceResult, String> {
    let summary = client
        .get_summary()
        .await
        .map_err(|e| format!("Failed to fetch summary: {}", e))?;
    let workers = client
        .list_workers()
        .await
        .map_err(|e| format!("Failed to fetch workers: {}", e))?;

    let merged = serde_json::json!({
        "summary": summary,
        "workers": workers,
    });

    let text = serde_json::to_string_pretty(&merged).unwrap_or_default();
    Ok(ReadResourceResult {
        contents: vec![ResourceContents::text(text, CLUSTER_STATUS_URI)],
    })
}

async fn read_cluster_metrics(client: &CoordinatorClient) -> Result<ReadResourceResult, String> {
    let metrics = client
        .get_metrics()
        .await
        .map_err(|e| format!("Failed to fetch metrics: {}", e))?;
    let scaling = client.get_scaling().await.unwrap_or(Value::Null);

    let merged = serde_json::json!({
        "metrics": metrics,
        "scaling": scaling,
    });

    let text = serde_json::to_string_pretty(&merged).unwrap_or_default();
    Ok(ReadResourceResult {
        contents: vec![ResourceContents::text(text, CLUSTER_METRICS_URI)],
    })
}
