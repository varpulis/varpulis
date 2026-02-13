//! MCP tool implementations for Varpulis.

use crate::server::VarpulisMcpServer;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content};
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

// ─── Tool parameter types ────────────────────────────────────────────

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ValidateVplParams {
    /// VPL source code to validate
    pub source: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeployPipelineParams {
    /// Name for the pipeline group
    pub name: String,
    /// VPL source code for the pipeline
    pub source: String,
    /// Optional worker affinity (target a specific worker)
    pub worker_affinity: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListPipelinesParams {
    /// Filter by status: "running", "deploying", or "failed"
    pub status: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryMetricsParams {
    /// Filter metrics for a specific pipeline group
    pub pipeline_group: Option<String>,
    /// Include raw Prometheus metrics text
    pub include_prometheus: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExplainAlertParams {
    /// Pipeline group to investigate
    pub pipeline_group: String,
    /// Description of the alert or anomaly
    pub alert_description: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchEventsParams {
    /// Pipeline group to inject the test event into
    pub pipeline_group: String,
    /// Event type name (e.g., "TradeEvent")
    pub event_type: String,
    /// Event fields as key-value pairs
    pub fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListModelsParams {}

// ─── Tool implementations ────────────────────────────────────────────

#[tool_router]
impl VarpulisMcpServer {
    /// Validate VPL source locally, no coordinator required.
    #[tool(
        description = "Validate VPL source code for syntax and semantic correctness. Returns diagnostics with line numbers. Works offline without a running coordinator."
    )]
    async fn validate_vpl(
        &self,
        params: Parameters<ValidateVplParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(validate_vpl_impl(&params.0.source, &self.client).await)
    }

    /// Deploy a VPL pipeline group to the cluster.
    #[tool(
        description = "Deploy a VPL pipeline group to the Varpulis cluster. Validates first, then deploys. Returns group ID, status, and worker placements."
    )]
    async fn deploy_pipeline(
        &self,
        params: Parameters<DeployPipelineParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(deploy_pipeline_impl(&self.client, params.0).await)
    }

    /// List deployed pipeline groups.
    #[tool(
        description = "List all deployed pipeline groups with status, worker assignments, and event counts."
    )]
    async fn list_pipelines(
        &self,
        params: Parameters<ListPipelinesParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(list_pipelines_impl(&self.client, params.0).await)
    }

    /// Query cluster metrics.
    #[tool(
        description = "Query cluster metrics: throughput, latency, worker health, connector status."
    )]
    async fn query_metrics(
        &self,
        params: Parameters<QueryMetricsParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(query_metrics_impl(&self.client, params.0).await)
    }

    /// Investigate an alert with full diagnostic context.
    #[tool(
        description = "Investigate an alert or anomaly by gathering full context: pipeline status, metrics, worker health, topology, and scaling recommendations."
    )]
    async fn explain_alert(
        &self,
        params: Parameters<ExplainAlertParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(explain_alert_impl(&self.client, params.0).await)
    }

    /// Inject a test event into a pipeline.
    #[tool(
        description = "Inject a test event into a pipeline and observe the output. Useful for testing filters and debugging transformations."
    )]
    async fn search_events(
        &self,
        params: Parameters<SearchEventsParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(search_events_impl(&self.client, params.0).await)
    }

    /// List registered ONNX models.
    #[tool(
        description = "List all ONNX models in the model registry with their inputs, outputs, and metadata."
    )]
    async fn list_models(
        &self,
        #[allow(unused)] params: Parameters<ListModelsParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(list_models_impl(&self.client).await)
    }
}

impl VarpulisMcpServer {
    pub(crate) fn create_tool_router() -> rmcp::handler::server::router::tool::ToolRouter<Self> {
        Self::tool_router()
    }
}

// ─── Implementation functions ────────────────────────────────────────

/// Validate VPL source locally. Falls back to coordinator for connector validation.
async fn validate_vpl_impl(
    source: &str,
    client: &crate::client::CoordinatorClient,
) -> CallToolResult {
    // Try local parse first (works offline)
    match varpulis_parser::parse(source) {
        Ok(program) => {
            let validation = varpulis_core::validate::validate(source, &program);
            if validation.has_errors() {
                let diags: Vec<String> = validation
                    .diagnostics
                    .iter()
                    .map(|d| {
                        let (line, col) = position_to_line_col(source, d.span.start);
                        format!(
                            "{}:{}: [{}] {}{}",
                            line,
                            col,
                            match d.severity {
                                varpulis_core::validate::Severity::Error => "error",
                                varpulis_core::validate::Severity::Warning => "warning",
                            },
                            d.message,
                            d.hint
                                .as_ref()
                                .map(|h| format!(" (hint: {})", h))
                                .unwrap_or_default()
                        )
                    })
                    .collect();

                let has_errors = validation
                    .diagnostics
                    .iter()
                    .any(|d| d.severity == varpulis_core::validate::Severity::Error);

                if has_errors {
                    // Try coordinator for fuller validation (may know about connectors)
                    if let Ok(remote) = client.validate(source).await {
                        return success_text(
                            serde_json::to_string_pretty(&remote).unwrap_or_default(),
                        );
                    }
                    error_text(format!("Validation errors:\n{}", diags.join("\n")))
                } else {
                    success_text(format!("VPL is valid with warnings:\n{}", diags.join("\n")))
                }
            } else {
                success_text("VPL is valid. No errors or warnings.".to_string())
            }
        }
        Err(e) => error_text(format!("Parse error: {}", e)),
    }
}

async fn deploy_pipeline_impl(
    client: &crate::client::CoordinatorClient,
    params: DeployPipelineParams,
) -> CallToolResult {
    // Validate locally first
    if let Err(e) = varpulis_parser::parse(&params.source) {
        return error_text(format!("VPL parse error — fix before deploying: {}", e));
    }

    let mut pipeline = serde_json::json!({
        "name": params.name,
        "source": params.source
    });
    if let Some(affinity) = &params.worker_affinity {
        pipeline["worker_affinity"] = serde_json::Value::String(affinity.clone());
    }

    let spec = serde_json::json!({
        "name": params.name,
        "pipelines": [pipeline],
        "routes": []
    });

    match client.deploy_pipeline_group(&spec).await {
        Ok(resp) => {
            success_text(serde_json::to_string_pretty(&resp).unwrap_or_else(|_| resp.to_string()))
        }
        Err(e) => e.into_tool_result(),
    }
}

async fn list_pipelines_impl(
    client: &crate::client::CoordinatorClient,
    params: ListPipelinesParams,
) -> CallToolResult {
    let groups = match client.list_pipeline_groups().await {
        Ok(v) => v,
        Err(e) => return e.into_tool_result(),
    };

    let summary = client.get_summary().await.ok();

    let mut result = groups;
    if let Some(s) = summary {
        result["cluster_summary"] = s;
    }

    // Filter by status if requested
    if let Some(status_filter) = &params.status {
        if let Some(arr) = result["pipeline_groups"].as_array().cloned() {
            let filtered: Vec<serde_json::Value> = arr
                .into_iter()
                .filter(|g| {
                    g["status"]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(status_filter))
                })
                .collect();
            result["filtered_total"] = serde_json::json!(filtered.len());
            result["pipeline_groups"] = serde_json::json!(filtered);
        }
    }

    success_text(serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string()))
}

async fn query_metrics_impl(
    client: &crate::client::CoordinatorClient,
    params: QueryMetricsParams,
) -> CallToolResult {
    let metrics = match client.get_metrics().await {
        Ok(v) => v,
        Err(e) => return e.into_tool_result(),
    };

    let summary = client.get_summary().await.ok();

    let mut result = serde_json::json!({
        "metrics": metrics,
    });

    if let Some(s) = summary {
        result["summary"] = s;
    }

    // Filter to specific pipeline group if requested
    if let Some(group) = &params.pipeline_group {
        if let Some(pipelines) = metrics["pipelines"].as_array() {
            let filtered: Vec<&serde_json::Value> = pipelines
                .iter()
                .filter(|p| {
                    p["group_id"].as_str().is_some_and(|g| g == group)
                        || p["name"]
                            .as_str()
                            .is_some_and(|n| n.contains(group.as_str()))
                })
                .collect();
            result["filtered_pipelines"] = serde_json::json!(filtered);
        }
    }

    if params.include_prometheus.unwrap_or(false) {
        if let Ok(prom) = client.get_prometheus().await {
            result["prometheus"] = prom;
        }
    }

    success_text(serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string()))
}

async fn explain_alert_impl(
    client: &crate::client::CoordinatorClient,
    params: ExplainAlertParams,
) -> CallToolResult {
    // Orchestrate 5 parallel API calls
    let (group, metrics, workers, scaling, topology) = tokio::join!(
        client.get_pipeline_group(&params.pipeline_group),
        client.get_metrics(),
        client.list_workers(),
        client.get_scaling(),
        client.get_topology(),
    );

    let mut report = String::new();
    report.push_str(&format!(
        "# Alert Investigation: {}\n\n",
        params.pipeline_group
    ));

    if let Some(desc) = &params.alert_description {
        report.push_str(&format!("**Alert**: {}\n\n", desc));
    }

    // Pipeline group status
    report.push_str("## Pipeline Group\n\n");
    match &group {
        Ok(g) => {
            report.push_str(&format!(
                "- **Status**: {}\n- **ID**: {}\n- **Name**: {}\n",
                g["status"].as_str().unwrap_or("unknown"),
                g["id"].as_str().unwrap_or("?"),
                g["name"].as_str().unwrap_or("?"),
            ));
            if let Some(placements) = g["placements"].as_array() {
                report.push_str(&format!("- **Pipelines**: {}\n", placements.len()));
                for p in placements {
                    report.push_str(&format!(
                        "  - {} on worker {} ({})\n",
                        p["name"].as_str().unwrap_or("?"),
                        p["worker_id"].as_str().unwrap_or("?"),
                        p["status"].as_str().unwrap_or("?"),
                    ));
                }
            }
        }
        Err(e) => report.push_str(&format!("Error fetching group: {}\n", e)),
    }

    // Worker health
    report.push_str("\n## Worker Health\n\n");
    match &workers {
        Ok(w) => {
            if let Some(workers_arr) = w["workers"].as_array() {
                for worker in workers_arr {
                    let status = worker["status"].as_str().unwrap_or("unknown");
                    let marker = if status == "ready" { "OK" } else { "WARN" };
                    report.push_str(&format!(
                        "- [{}] {} ({}) — {} pipelines, {} events\n",
                        marker,
                        worker["id"].as_str().unwrap_or("?"),
                        status,
                        worker["pipelines_running"].as_u64().unwrap_or(0),
                        worker["events_processed"].as_u64().unwrap_or(0),
                    ));
                }
            }
        }
        Err(e) => report.push_str(&format!("Error fetching workers: {}\n", e)),
    }

    // Metrics
    report.push_str("\n## Metrics\n\n");
    match &metrics {
        Ok(m) => {
            report.push_str(&format!(
                "```json\n{}\n```\n",
                serde_json::to_string_pretty(m).unwrap_or_default()
            ));
        }
        Err(e) => report.push_str(&format!("Error fetching metrics: {}\n", e)),
    }

    // Scaling
    report.push_str("\n## Scaling Recommendation\n\n");
    match &scaling {
        Ok(s) => {
            report.push_str(&format!(
                "- **Action**: {}\n- **Reason**: {}\n- **Current workers**: {}\n- **Target workers**: {}\n",
                s["action"].as_str().unwrap_or("?"),
                s["reason"].as_str().unwrap_or("?"),
                s["current_workers"].as_u64().unwrap_or(0),
                s["target_workers"].as_u64().unwrap_or(0),
            ));
        }
        Err(e) => report.push_str(&format!("Error fetching scaling: {}\n", e)),
    }

    // Topology
    report.push_str("\n## Topology\n\n");
    match &topology {
        Ok(t) => {
            if let Some(groups) = t["groups"].as_array() {
                for grp in groups {
                    report.push_str(&format!(
                        "### {}\n",
                        grp["group_name"].as_str().unwrap_or("?")
                    ));
                    if let Some(pipelines) = grp["pipelines"].as_array() {
                        for p in pipelines {
                            report.push_str(&format!(
                                "- {} -> worker {}\n",
                                p["name"].as_str().unwrap_or("?"),
                                p["worker_id"].as_str().unwrap_or("?"),
                            ));
                        }
                    }
                }
            }
        }
        Err(e) => report.push_str(&format!("Error fetching topology: {}\n", e)),
    }

    // Heuristic analysis
    report.push_str("\n## Possible Causes\n\n");
    let mut causes = Vec::new();

    if let Ok(g) = &group {
        let status = g["status"].as_str().unwrap_or("");
        if status == "failed" || status == "partially_running" {
            causes
                .push("Pipeline group is not fully running — check worker logs for deploy errors.");
        }
    }

    if let Ok(w) = &workers {
        if let Some(workers_arr) = w["workers"].as_array() {
            let unhealthy = workers_arr
                .iter()
                .filter(|w| w["status"].as_str() != Some("ready"))
                .count();
            if unhealthy > 0 {
                causes.push(
                    "One or more workers are unhealthy — may cause event processing failures.",
                );
            }
            if workers_arr.is_empty() {
                causes
                    .push("No workers registered — the cluster has no capacity to process events.");
            }
        }
    }

    if let Ok(s) = &scaling {
        if s["action"].as_str() == Some("scale_up") {
            causes.push("Scaling recommends adding workers — the cluster may be overloaded.");
        }
    }

    if causes.is_empty() {
        report.push_str("No obvious issues detected from available data. Consider checking:\n");
        report.push_str("- Application logs on workers\n");
        report.push_str("- Connector health (MQTT/Kafka broker reachability)\n");
        report.push_str("- Event schema mismatches\n");
    } else {
        for cause in causes {
            report.push_str(&format!("- {}\n", cause));
        }
    }

    success_text(report)
}

async fn search_events_impl(
    client: &crate::client::CoordinatorClient,
    params: SearchEventsParams,
) -> CallToolResult {
    let event = serde_json::json!({
        "event_type": params.event_type,
        "fields": params.fields,
    });

    match client.inject_event(&params.pipeline_group, &event).await {
        Ok(resp) => {
            success_text(serde_json::to_string_pretty(&resp).unwrap_or_else(|_| resp.to_string()))
        }
        Err(e) => e.into_tool_result(),
    }
}

async fn list_models_impl(client: &crate::client::CoordinatorClient) -> CallToolResult {
    match client.list_models().await {
        Ok(resp) => {
            success_text(serde_json::to_string_pretty(&resp).unwrap_or_else(|_| resp.to_string()))
        }
        Err(e) => e.into_tool_result(),
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────

fn success_text(text: String) -> CallToolResult {
    CallToolResult::success(vec![Content::text(text)])
}

fn error_text(text: String) -> CallToolResult {
    CallToolResult::error(vec![Content::text(text)])
}

fn position_to_line_col(source: &str, position: usize) -> (usize, usize) {
    let mut line = 1;
    let mut col = 1;
    for (i, ch) in source.chars().enumerate() {
        if i >= position {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_valid_vpl() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = crate::client::CoordinatorClient::new("http://localhost:99999".into(), None);
        let result = rt.block_on(validate_vpl_impl("stream A = X", &client));
        assert_eq!(result.is_error, Some(false));
        let text = result.content[0].as_text().unwrap().text.as_str();
        assert!(text.contains("valid"), "Expected 'valid' in: {}", text);
    }

    #[test]
    fn test_validate_invalid_vpl() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = crate::client::CoordinatorClient::new("http://localhost:99999".into(), None);
        let result = rt.block_on(validate_vpl_impl("this is not valid vpl !!!", &client));
        assert_eq!(result.is_error, Some(true));
        let text = result.content[0].as_text().unwrap().text.as_str();
        assert!(
            text.contains("error") || text.contains("Parse"),
            "Expected error in: {}",
            text
        );
    }

    #[test]
    fn test_position_to_line_col() {
        assert_eq!(position_to_line_col("hello\nworld", 0), (1, 1));
        assert_eq!(position_to_line_col("hello\nworld", 5), (1, 6));
        assert_eq!(position_to_line_col("hello\nworld", 6), (2, 1));
        assert_eq!(position_to_line_col("hello\nworld", 8), (2, 3));
    }
}
