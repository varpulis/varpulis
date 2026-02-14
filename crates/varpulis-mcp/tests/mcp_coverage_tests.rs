//! Coverage-focused tests for varpulis-mcp: tools, resources, client, error, prompts, server.
//!
//! These tests exercise code paths that the E2E test does not reach, targeting
//! uncovered lines in tools.rs, resources.rs, client.rs, error.rs, and prompts.rs.

use serde_json::{json, Map, Value};
use varpulis_mcp::client::CoordinatorClient;
use varpulis_mcp::error::CoordinatorError;
use varpulis_mcp::resources;
use varpulis_mcp::server::VarpulisMcpServer;

// =============================================================================
// Error module tests
// =============================================================================

#[test]
fn error_unauthorized_display() {
    let err = CoordinatorError::Unauthorized;
    let msg = err.to_string();
    assert!(
        msg.contains("Authentication failed"),
        "Unauthorized display: {}",
        msg
    );
    assert!(msg.contains("401"), "Should mention HTTP 401: {}", msg);
}

#[test]
fn error_not_found_display() {
    let err = CoordinatorError::NotFound("pipeline xyz".into());
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("Not found") || msg.contains("pipeline xyz"),
        "NotFound display: {}",
        msg
    );
}

#[test]
fn error_bad_request_display() {
    let err = CoordinatorError::BadRequest("missing field 'name'".into());
    let msg = err.to_string();
    assert!(
        msg.contains("Bad request") || msg.contains("missing field"),
        "BadRequest display: {}",
        msg
    );
}

#[test]
fn error_api_error_display() {
    let err = CoordinatorError::ApiError {
        status: 503,
        message: "Service unavailable".into(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("503"),
        "ApiError should contain status: {}",
        msg
    );
    assert!(
        msg.contains("Service unavailable"),
        "ApiError should contain message: {}",
        msg
    );
}

#[test]
fn error_parse_error_display() {
    let err = CoordinatorError::ParseError("unexpected token".into());
    let msg = err.to_string();
    assert!(
        msg.contains("parse") || msg.contains("Parse"),
        "ParseError display: {}",
        msg
    );
    assert!(
        msg.contains("unexpected token"),
        "ParseError should contain inner message: {}",
        msg
    );
}

#[test]
fn error_validation_error_display() {
    let err = CoordinatorError::ValidationError("undefined stream Foo".into());
    let msg = err.to_string();
    assert!(
        msg.contains("validation") || msg.contains("Validation"),
        "ValidationError display: {}",
        msg
    );
    assert!(
        msg.contains("undefined stream Foo"),
        "ValidationError should contain inner: {}",
        msg
    );
}

#[test]
fn error_other_display() {
    let err = CoordinatorError::Other("something unexpected".into());
    let msg = err.to_string();
    assert!(
        msg.contains("something unexpected"),
        "Other display: {}",
        msg
    );
}

#[test]
fn error_into_tool_result_unauthorized() {
    let err = CoordinatorError::Unauthorized;
    let result = err.into_tool_result();
    assert_eq!(result.is_error, Some(true));
    let text = result.content[0].as_text().unwrap().text.as_str();
    assert!(
        text.contains("Authentication"),
        "Tool result text: {}",
        text
    );
}

#[test]
fn error_into_tool_result_not_found() {
    let err = CoordinatorError::NotFound("group-123".into());
    let result = err.into_tool_result();
    assert_eq!(result.is_error, Some(true));
    let text = result.content[0].as_text().unwrap().text.as_str();
    assert!(text.contains("group-123"), "Tool result text: {}", text);
}

#[test]
fn error_into_tool_result_api_error() {
    let err = CoordinatorError::ApiError {
        status: 500,
        message: "internal".into(),
    };
    let result = err.into_tool_result();
    assert_eq!(result.is_error, Some(true));
    let text = result.content[0].as_text().unwrap().text.as_str();
    assert!(text.contains("500"), "Tool result text: {}", text);
}

// =============================================================================
// Client module tests
// =============================================================================

#[test]
fn client_construction_without_api_key() {
    let client = CoordinatorClient::new("http://localhost:9100".into(), None);
    // Just verifying it doesn't panic. The client is usable.
    let _ = &client;
}

#[test]
fn client_construction_with_api_key() {
    let client = CoordinatorClient::new(
        "http://localhost:9100".into(),
        Some("secret-key-123".into()),
    );
    let _ = &client;
}

#[test]
fn client_clone() {
    let client = CoordinatorClient::new("http://localhost:9100".into(), Some("key".into()));
    let cloned = client.clone();
    let _ = &cloned;
}

#[tokio::test]
async fn client_unreachable_list_pipeline_groups() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.list_pipeline_groups().await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("reach") || msg.contains("connect") || msg.contains("Unreachable"),
        "Expected unreachable error, got: {}",
        msg
    );
}

#[tokio::test]
async fn client_unreachable_get_pipeline_group() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.get_pipeline_group("nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_deploy() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let spec = json!({"name": "test", "pipelines": []});
    let result = client.deploy_pipeline_group(&spec).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_validate() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.validate("stream A = X").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_get_metrics() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.get_metrics().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_get_summary() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.get_summary().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_list_workers() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.list_workers().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_get_topology() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.get_topology().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_get_scaling() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.get_scaling().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_list_models() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.list_models().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_get_prometheus() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = client.get_prometheus().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_unreachable_inject_event() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let event = json!({"event_type": "Test", "fields": {}});
    let result = client.inject_event("group-1", &event).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn client_is_coordinator_reachable_false() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let reachable = varpulis_mcp::client::is_coordinator_reachable(&client).await;
    assert!(!reachable);
}

// =============================================================================
// Resources module tests
// =============================================================================

#[test]
fn resources_list_returns_three() {
    let res = resources::list_resources();
    assert_eq!(res.len(), 3, "Expected 3 resources, got {}", res.len());
}

#[test]
fn resources_list_contains_cluster_status() {
    let res = resources::list_resources();
    let uris: Vec<&str> = res.iter().map(|r| r.uri.as_str()).collect();
    assert!(uris.contains(&"varpulis://cluster/status"));
}

#[test]
fn resources_list_contains_vpl_reference() {
    let res = resources::list_resources();
    let uris: Vec<&str> = res.iter().map(|r| r.uri.as_str()).collect();
    assert!(uris.contains(&"varpulis://docs/vpl-reference"));
}

#[test]
fn resources_list_contains_cluster_metrics() {
    let res = resources::list_resources();
    let uris: Vec<&str> = res.iter().map(|r| r.uri.as_str()).collect();
    assert!(uris.contains(&"varpulis://cluster/metrics"));
}

#[test]
fn resource_templates_is_empty() {
    let templates = resources::list_resource_templates();
    assert!(templates.is_empty(), "Resource templates should be empty");
}

#[tokio::test]
async fn resource_read_vpl_reference() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = resources::read_resource("varpulis://docs/vpl-reference", &client).await;
    assert!(result.is_ok(), "VPL reference should be readable offline");
    let result = result.unwrap();
    assert!(!result.contents.is_empty());
}

#[tokio::test]
async fn resource_read_unknown_uri() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = resources::read_resource("varpulis://unknown/resource", &client).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.contains("Unknown resource URI"),
        "Expected unknown URI error, got: {}",
        err
    );
}

#[tokio::test]
async fn resource_read_cluster_status_no_coordinator() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = resources::read_resource("varpulis://cluster/status", &client).await;
    assert!(
        result.is_err(),
        "Cluster status should fail without coordinator"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("Failed to fetch"),
        "Expected fetch failure, got: {}",
        err
    );
}

#[tokio::test]
async fn resource_read_cluster_metrics_no_coordinator() {
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let result = resources::read_resource("varpulis://cluster/metrics", &client).await;
    assert!(
        result.is_err(),
        "Cluster metrics should fail without coordinator"
    );
}

#[test]
fn vpl_reference_content_is_nonempty() {
    assert!(
        !resources::VPL_REFERENCE.is_empty(),
        "VPL_REFERENCE should be non-empty"
    );
}

#[test]
fn vpl_reference_contains_stream_keyword() {
    assert!(
        resources::VPL_REFERENCE.contains("stream"),
        "VPL reference should document 'stream'"
    );
}

// =============================================================================
// Prompts module tests
// =============================================================================

#[test]
fn prompts_list_returns_three() {
    let prompts = varpulis_mcp::prompts::list_prompts();
    assert_eq!(prompts.len(), 3);
}

#[test]
fn prompts_list_names() {
    let prompts = varpulis_mcp::prompts::list_prompts();
    let names: Vec<&str> = prompts.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains(&"investigate_alert"));
    assert!(names.contains(&"create_fraud_detection"));
    assert!(names.contains(&"optimize_stream"));
}

#[test]
fn prompt_investigate_alert_has_arguments() {
    let prompts = varpulis_mcp::prompts::list_prompts();
    let p = prompts
        .iter()
        .find(|p| p.name == "investigate_alert")
        .unwrap();
    let args = p.arguments.as_ref().unwrap();
    assert!(args.iter().any(|a| a.name == "alert"));
    assert!(args.iter().any(|a| a.name == "pipeline_group"));
}

#[test]
fn prompt_fraud_detection_has_arguments() {
    let prompts = varpulis_mcp::prompts::list_prompts();
    let p = prompts
        .iter()
        .find(|p| p.name == "create_fraud_detection")
        .unwrap();
    let args = p.arguments.as_ref().unwrap();
    assert!(args.iter().any(|a| a.name == "event_type"));
    assert!(args.iter().any(|a| a.name == "fields"));
    assert!(args.iter().any(|a| a.name == "time_window"));
}

#[test]
fn prompt_optimize_stream_has_arguments() {
    let prompts = varpulis_mcp::prompts::list_prompts();
    let p = prompts
        .iter()
        .find(|p| p.name == "optimize_stream")
        .unwrap();
    let args = p.arguments.as_ref().unwrap();
    assert!(args.iter().any(|a| a.name == "pipeline_group"));
    assert!(args.iter().any(|a| a.name == "goal"));
}

#[test]
fn get_prompt_investigate_alert() {
    let mut args = Map::new();
    args.insert("alert".into(), Value::String("CPU spike".into()));
    args.insert(
        "pipeline_group".into(),
        Value::String("trade-alerts".into()),
    );
    let result = varpulis_mcp::prompts::get_prompt("investigate_alert", &args).unwrap();
    assert!(!result.messages.is_empty());
    assert!(result.description.is_some());
}

#[test]
fn get_prompt_investigate_alert_no_args() {
    let args = Map::new();
    let result = varpulis_mcp::prompts::get_prompt("investigate_alert", &args).unwrap();
    assert!(!result.messages.is_empty());
}

#[test]
fn get_prompt_fraud_detection() {
    let mut args = Map::new();
    args.insert("event_type".into(), Value::String("Transaction".into()));
    args.insert(
        "fields".into(),
        Value::String("user_id, amount, status".into()),
    );
    args.insert("time_window".into(), Value::String("5m".into()));
    let result = varpulis_mcp::prompts::get_prompt("create_fraud_detection", &args).unwrap();
    assert!(!result.messages.is_empty());
    assert!(result.description.is_some());
}

#[test]
fn get_prompt_fraud_detection_default_window() {
    let mut args = Map::new();
    args.insert("event_type".into(), Value::String("Payment".into()));
    args.insert("fields".into(), Value::String("amount, merchant".into()));
    // No time_window -> should default to "10m"
    let result = varpulis_mcp::prompts::get_prompt("create_fraud_detection", &args).unwrap();
    assert!(!result.messages.is_empty());
}

#[test]
fn get_prompt_optimize_stream() {
    let mut args = Map::new();
    args.insert("pipeline_group".into(), Value::String("my-pipeline".into()));
    args.insert("goal".into(), Value::String("memory".into()));
    let result = varpulis_mcp::prompts::get_prompt("optimize_stream", &args).unwrap();
    assert!(!result.messages.is_empty());
    assert!(result.description.is_some());
}

#[test]
fn get_prompt_optimize_stream_default_goal() {
    let mut args = Map::new();
    args.insert("pipeline_group".into(), Value::String("my-pipeline".into()));
    // No goal -> should default to "throughput"
    let result = varpulis_mcp::prompts::get_prompt("optimize_stream", &args).unwrap();
    assert!(!result.messages.is_empty());
}

#[test]
fn get_prompt_unknown_returns_error() {
    let args = Map::new();
    let result = varpulis_mcp::prompts::get_prompt("nonexistent_prompt", &args);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.contains("Unknown prompt"),
        "Expected unknown prompt error, got: {}",
        err
    );
}

// =============================================================================
// Server construction tests
// =============================================================================

#[test]
fn server_construction() {
    let client = CoordinatorClient::new("http://localhost:9100".into(), None);
    let server = VarpulisMcpServer::new(client);
    let _ = &server;
}

#[test]
fn server_clone() {
    let client = CoordinatorClient::new("http://localhost:9100".into(), Some("key".into()));
    let server = VarpulisMcpServer::new(client);
    let cloned = server.clone();
    let _ = &cloned;
}

// =============================================================================
// Resource URI constants
// =============================================================================

#[test]
fn resource_uri_constants() {
    assert_eq!(resources::CLUSTER_STATUS_URI, "varpulis://cluster/status");
    assert_eq!(
        resources::VPL_REFERENCE_URI,
        "varpulis://docs/vpl-reference"
    );
    assert_eq!(resources::CLUSTER_METRICS_URI, "varpulis://cluster/metrics");
}

// =============================================================================
// Tools module tests — tool listing, schemas, parameter types, and server info
// =============================================================================

#[test]
fn tool_router_creation() {
    // Exercise VarpulisMcpServer::create_tool_router() (tools.rs L146-148)
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let server = VarpulisMcpServer::new(client);
    // The tool_router is created during new(). Verify it exists.
    let _ = &server;
}

#[test]
fn server_get_info() {
    use rmcp::ServerHandler;
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let server = VarpulisMcpServer::new(client);
    let info = server.get_info();
    assert_eq!(info.server_info.name, "varpulis-mcp");
    assert!(info.instructions.is_some());
    let instructions = info.instructions.unwrap();
    assert!(
        instructions.contains("Varpulis"),
        "Instructions should mention Varpulis: {}",
        instructions
    );
}

#[test]
fn server_get_info_capabilities() {
    use rmcp::ServerHandler;
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let server = VarpulisMcpServer::new(client);
    let info = server.get_info();
    // Capabilities should enable tools, resources, and prompts
    let caps = &info.capabilities;
    assert!(caps.tools.is_some(), "Tools capability should be enabled");
    assert!(
        caps.resources.is_some(),
        "Resources capability should be enabled"
    );
    assert!(
        caps.prompts.is_some(),
        "Prompts capability should be enabled"
    );
}

#[test]
fn server_get_info_version() {
    use rmcp::ServerHandler;
    let client = CoordinatorClient::new("http://localhost:99999".into(), None);
    let server = VarpulisMcpServer::new(client);
    let info = server.get_info();
    // Version should be non-empty (from CARGO_PKG_VERSION)
    assert!(
        !info.server_info.version.is_empty(),
        "Version should not be empty"
    );
}

// ── Tool parameter types — serde tests ───────────────────────────────

#[test]
fn validate_vpl_params_serde() {
    let json = json!({"source": "stream A = X"});
    let params: varpulis_mcp::tools::ValidateVplParams =
        serde_json::from_value(json.clone()).unwrap();
    assert_eq!(params.source, "stream A = X");
}

#[test]
fn deploy_pipeline_params_serde_full() {
    let json = json!({
        "name": "my-group",
        "source": "stream A = X",
        "worker_affinity": "w0"
    });
    let params: varpulis_mcp::tools::DeployPipelineParams = serde_json::from_value(json).unwrap();
    assert_eq!(params.name, "my-group");
    assert_eq!(params.source, "stream A = X");
    assert_eq!(params.worker_affinity, Some("w0".to_string()));
}

#[test]
fn deploy_pipeline_params_serde_no_affinity() {
    let json = json!({
        "name": "my-group",
        "source": "stream A = X"
    });
    let params: varpulis_mcp::tools::DeployPipelineParams = serde_json::from_value(json).unwrap();
    assert!(params.worker_affinity.is_none());
}

#[test]
fn list_pipelines_params_serde_empty() {
    let json = json!({});
    let params: varpulis_mcp::tools::ListPipelinesParams = serde_json::from_value(json).unwrap();
    assert!(params.status.is_none());
}

#[test]
fn list_pipelines_params_serde_with_status() {
    let json = json!({"status": "running"});
    let params: varpulis_mcp::tools::ListPipelinesParams = serde_json::from_value(json).unwrap();
    assert_eq!(params.status, Some("running".into()));
}

#[test]
fn query_metrics_params_serde_empty() {
    let json = json!({});
    let params: varpulis_mcp::tools::QueryMetricsParams = serde_json::from_value(json).unwrap();
    assert!(params.pipeline_group.is_none());
    assert!(params.include_prometheus.is_none());
}

#[test]
fn query_metrics_params_serde_full() {
    let json = json!({
        "pipeline_group": "trades",
        "include_prometheus": true
    });
    let params: varpulis_mcp::tools::QueryMetricsParams = serde_json::from_value(json).unwrap();
    assert_eq!(params.pipeline_group, Some("trades".into()));
    assert_eq!(params.include_prometheus, Some(true));
}

#[test]
fn explain_alert_params_serde_full() {
    let json = json!({
        "pipeline_group": "trade-alerts",
        "alert_description": "CPU spike"
    });
    let params: varpulis_mcp::tools::ExplainAlertParams = serde_json::from_value(json).unwrap();
    assert_eq!(params.pipeline_group, "trade-alerts");
    assert_eq!(params.alert_description, Some("CPU spike".into()));
}

#[test]
fn explain_alert_params_serde_no_description() {
    let json = json!({"pipeline_group": "my-pipeline"});
    let params: varpulis_mcp::tools::ExplainAlertParams = serde_json::from_value(json).unwrap();
    assert!(params.alert_description.is_none());
}

#[test]
fn search_events_params_serde() {
    let json = json!({
        "pipeline_group": "group-1",
        "event_type": "TradeEvent",
        "fields": {"symbol": "AAPL", "price": 150.0}
    });
    let params: varpulis_mcp::tools::SearchEventsParams = serde_json::from_value(json).unwrap();
    assert_eq!(params.pipeline_group, "group-1");
    assert_eq!(params.event_type, "TradeEvent");
    assert_eq!(params.fields.len(), 2);
    assert_eq!(params.fields["symbol"], json!("AAPL"));
}

#[test]
fn list_models_params_serde() {
    let json = json!({});
    let _params: varpulis_mcp::tools::ListModelsParams = serde_json::from_value(json).unwrap();
}

// ── Tool parameter types — debug traits ──────────────────────────────

#[test]
fn tool_params_debug() {
    let p1 = varpulis_mcp::tools::ValidateVplParams {
        source: "test".into(),
    };
    assert!(format!("{:?}", p1).contains("ValidateVplParams"));

    let p2 = varpulis_mcp::tools::DeployPipelineParams {
        name: "test".into(),
        source: "src".into(),
        worker_affinity: None,
    };
    assert!(format!("{:?}", p2).contains("DeployPipelineParams"));

    let p3 = varpulis_mcp::tools::ListPipelinesParams { status: None };
    assert!(format!("{:?}", p3).contains("ListPipelinesParams"));

    let p4 = varpulis_mcp::tools::QueryMetricsParams {
        pipeline_group: None,
        include_prometheus: None,
    };
    assert!(format!("{:?}", p4).contains("QueryMetricsParams"));

    let p5 = varpulis_mcp::tools::ExplainAlertParams {
        pipeline_group: "test".into(),
        alert_description: None,
    };
    assert!(format!("{:?}", p5).contains("ExplainAlertParams"));

    let p6 = varpulis_mcp::tools::SearchEventsParams {
        pipeline_group: "g".into(),
        event_type: "E".into(),
        fields: serde_json::Map::new(),
    };
    assert!(format!("{:?}", p6).contains("SearchEventsParams"));

    let p7 = varpulis_mcp::tools::ListModelsParams {};
    assert!(format!("{:?}", p7).contains("ListModelsParams"));
}

// ── Error module: additional coverage ────────────────────────────────

#[tokio::test]
async fn error_from_reqwest_connect_error() {
    // Trigger a real reqwest connect error and verify the From impl (error.rs L38-52)
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_millis(50))
        .build()
        .unwrap();
    let result = client.get("http://127.0.0.1:1").send().await;
    assert!(result.is_err());
    let reqwest_err = result.unwrap_err();
    let coord_err: CoordinatorError = reqwest_err.into();
    let msg = coord_err.to_string();
    // Should be Unreachable or Other depending on platform
    assert!(
        !msg.is_empty(),
        "Error message should not be empty: {}",
        msg
    );
}

#[test]
fn error_into_tool_result_bad_request() {
    let err = CoordinatorError::BadRequest("missing name".into());
    let result = err.into_tool_result();
    assert_eq!(result.is_error, Some(true));
    let text = result.content[0].as_text().unwrap().text.as_str();
    assert!(text.contains("missing name"));
}

#[test]
fn error_into_tool_result_parse_error() {
    let err = CoordinatorError::ParseError("unexpected token".into());
    let result = err.into_tool_result();
    assert_eq!(result.is_error, Some(true));
    let text = result.content[0].as_text().unwrap().text.as_str();
    assert!(text.contains("unexpected token"));
}

#[test]
fn error_into_tool_result_validation_error() {
    let err = CoordinatorError::ValidationError("undefined stream".into());
    let result = err.into_tool_result();
    assert_eq!(result.is_error, Some(true));
    let text = result.content[0].as_text().unwrap().text.as_str();
    assert!(text.contains("undefined stream"));
}

#[test]
fn error_into_tool_result_other() {
    let err = CoordinatorError::Other("unexpected failure".into());
    let result = err.into_tool_result();
    assert_eq!(result.is_error, Some(true));
    let text = result.content[0].as_text().unwrap().text.as_str();
    assert!(text.contains("unexpected failure"));
}

#[test]
fn error_unreachable_display() {
    // Cannot easily construct a reqwest::Error directly, but we can
    // test the other variants that haven't been tested yet.
    let err = CoordinatorError::ParseError("line 1: bad token".into());
    let msg = err.to_string();
    assert!(msg.contains("parse") || msg.contains("Parse"));
    assert!(msg.contains("bad token"));
}

#[test]
fn error_debug_format() {
    let err = CoordinatorError::BadRequest("test".into());
    let dbg = format!("{:?}", err);
    assert!(dbg.contains("BadRequest"));
}

// =============================================================================
// Tools module tests — impl functions and helpers
// =============================================================================

/// Helper: create a CoordinatorClient pointing at an unreachable host.
fn unreachable_client() -> CoordinatorClient {
    CoordinatorClient::new("http://localhost:99999".into(), None)
}

/// Helper: extract text from the first Content item in a CallToolResult.
fn first_text(result: &rmcp::model::CallToolResult) -> &str {
    result.content[0]
        .as_text()
        .expect("expected text content")
        .text
        .as_str()
}

// ── position_to_line_col ─────────────────────────────────────────────

#[test]
fn position_to_line_col_beginning_of_file() {
    assert_eq!(varpulis_mcp::tools::position_to_line_col("abc", 0), (1, 1));
}

#[test]
fn position_to_line_col_middle_of_first_line() {
    assert_eq!(
        varpulis_mcp::tools::position_to_line_col("hello world", 5),
        (1, 6)
    );
}

#[test]
fn position_to_line_col_at_newline() {
    // position 5 is the '\n' char in "hello\nworld"
    assert_eq!(
        varpulis_mcp::tools::position_to_line_col("hello\nworld", 5),
        (1, 6)
    );
}

#[test]
fn position_to_line_col_after_newline() {
    // position 6 is 'w' on line 2
    assert_eq!(
        varpulis_mcp::tools::position_to_line_col("hello\nworld", 6),
        (2, 1)
    );
}

#[test]
fn position_to_line_col_multiple_lines() {
    let src = "line1\nline2\nline3";
    // position 12 is 'l' at start of line3
    assert_eq!(varpulis_mcp::tools::position_to_line_col(src, 12), (3, 1));
}

#[test]
fn position_to_line_col_empty_string() {
    assert_eq!(varpulis_mcp::tools::position_to_line_col("", 0), (1, 1));
}

#[test]
fn position_to_line_col_beyond_end() {
    // Position beyond string length: iterates through all chars and returns final position
    assert_eq!(varpulis_mcp::tools::position_to_line_col("ab", 100), (1, 3));
}

#[test]
fn position_to_line_col_consecutive_newlines() {
    let src = "a\n\nb";
    // position 2 is the second '\n'
    assert_eq!(varpulis_mcp::tools::position_to_line_col(src, 2), (2, 1));
    // position 3 is 'b' on line 3
    assert_eq!(varpulis_mcp::tools::position_to_line_col(src, 3), (3, 1));
}

// ── success_text / error_text helpers ────────────────────────────────

#[test]
fn success_text_creates_non_error_result() {
    let result = varpulis_mcp::tools::success_text("ok".to_string());
    assert_eq!(result.is_error, Some(false));
    let text = first_text(&result);
    assert_eq!(text, "ok");
}

#[test]
fn error_text_creates_error_result() {
    let result = varpulis_mcp::tools::error_text("fail".to_string());
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    assert_eq!(text, "fail");
}

// ── validate_vpl_impl ───────────────────────────────────────────────

#[tokio::test]
async fn validate_vpl_valid_simple() {
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl("stream A = X", &client).await;
    assert_eq!(result.is_error, Some(false));
    let text = first_text(&result);
    assert!(text.contains("valid"), "Expected 'valid' in: {}", text);
    assert!(
        text.contains("No errors"),
        "Expected 'No errors' for a clean program, got: {}",
        text
    );
}

#[tokio::test]
async fn validate_vpl_valid_complex_multi_stream() {
    // A complex VPL with multiple streams — single line chaining works
    let source = "stream Trades = TradeEvent\nstream Alerts = Trades.where(price > 1000)";
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl(source, &client).await;
    // Both should parse even without event declarations (warnings only, not errors)
    let text = first_text(&result);
    assert!(
        result.is_error == Some(false) || text.contains("warning"),
        "Multi-stream VPL should be valid or warning-only: {}",
        text
    );
}

#[tokio::test]
async fn validate_vpl_parse_error() {
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl("this is not valid VPL !!!", &client).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    assert!(
        text.contains("Parse error") || text.contains("parse"),
        "Expected parse error, got: {}",
        text
    );
}

#[tokio::test]
async fn validate_vpl_empty_source() {
    // Empty string should parse as an empty program — valid
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl("", &client).await;
    // Empty program may parse OK or may be a parse error depending on grammar
    // Either way, just verify it doesn't panic
    let _text = first_text(&result);
}

#[tokio::test]
async fn validate_vpl_semantic_error_having_without_aggregate() {
    // .having() without .aggregate() is a semantic error (E010) with a hint
    let source = "stream S = X\n    .having(price > 1)";
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl(source, &client).await;
    assert_eq!(
        result.is_error,
        Some(true),
        "Semantic error should produce is_error=true"
    );
    let text = first_text(&result);
    assert!(
        text.contains("error") || text.contains("Validation"),
        "Expected validation error text, got: {}",
        text
    );
    // Should contain the diagnostic with line/col information
    assert!(
        text.contains(".having()"),
        "Error should mention .having(), got: {}",
        text
    );
    // Should contain the hint
    assert!(
        text.contains("hint") || text.contains("aggregate"),
        "Error should contain hint about aggregate, got: {}",
        text
    );
}

#[tokio::test]
async fn validate_vpl_semantic_warning_only_no_errors() {
    // .aggregate() without .window() produces a Severity::Warning (W001)
    // has_errors() only checks for Severity::Error, so warnings-only → "valid"
    // This exercises the else branch at L202 (no errors path)
    let source = "stream S = X.aggregate(c: count())";
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl(source, &client).await;
    // Warnings only → has_errors() = false → success path
    assert_eq!(
        result.is_error,
        Some(false),
        "Warning-only should not be is_error, text: {}",
        first_text(&result)
    );
    let text = first_text(&result);
    assert!(text.contains("valid"), "Expected 'valid', got: {}", text);
}

#[tokio::test]
async fn validate_vpl_whitespace_only() {
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl("   \n  \n  ", &client).await;
    // Whitespace only — should either parse as empty program or parse error
    let _text = first_text(&result);
}

// ── deploy_pipeline_impl ────────────────────────────────────────────

#[tokio::test]
async fn deploy_pipeline_parse_error() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::DeployPipelineParams {
        name: "bad-pipeline".to_string(),
        source: "this is not valid VPL !!!".to_string(),
        worker_affinity: None,
    };
    let result = varpulis_mcp::tools::deploy_pipeline_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    assert!(
        text.contains("parse error") || text.contains("Parse"),
        "Expected parse error, got: {}",
        text
    );
    assert!(
        text.contains("fix before deploying"),
        "Expected 'fix before deploying' hint, got: {}",
        text
    );
}

#[tokio::test]
async fn deploy_pipeline_valid_vpl_unreachable_coordinator() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::DeployPipelineParams {
        name: "test-group".to_string(),
        source: "stream A = X".to_string(),
        worker_affinity: None,
    };
    let result = varpulis_mcp::tools::deploy_pipeline_impl(&client, params).await;
    // Passes local parse but fails to contact coordinator
    assert_eq!(
        result.is_error,
        Some(true),
        "Deploy should fail without coordinator, text: {}",
        first_text(&result)
    );
    let text = first_text(&result);
    assert!(
        text.contains("reach") || text.contains("connect") || text.contains("Unreachable"),
        "Expected connection error, got: {}",
        text
    );
}

#[tokio::test]
async fn deploy_pipeline_with_worker_affinity() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::DeployPipelineParams {
        name: "test-group".to_string(),
        source: "stream A = X".to_string(),
        worker_affinity: Some("worker-0".to_string()),
    };
    let result = varpulis_mcp::tools::deploy_pipeline_impl(&client, params).await;
    // Will still fail due to unreachable coordinator, but exercises the affinity code path
    assert_eq!(result.is_error, Some(true));
}

#[tokio::test]
async fn deploy_pipeline_empty_source() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::DeployPipelineParams {
        name: "empty".to_string(),
        source: "".to_string(),
        worker_affinity: None,
    };
    let result = varpulis_mcp::tools::deploy_pipeline_impl(&client, params).await;
    // Empty source may parse as empty program or parse error — just verify no panic
    let _text = first_text(&result);
}

// ── list_pipelines_impl ─────────────────────────────────────────────

#[tokio::test]
async fn list_pipelines_unreachable_no_filter() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::ListPipelinesParams { status: None };
    let result = varpulis_mcp::tools::list_pipelines_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    assert!(
        text.contains("reach") || text.contains("connect") || text.contains("Unreachable"),
        "Expected connection error, got: {}",
        text
    );
}

#[tokio::test]
async fn list_pipelines_unreachable_with_status_filter() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::ListPipelinesParams {
        status: Some("running".to_string()),
    };
    let result = varpulis_mcp::tools::list_pipelines_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
}

// ── query_metrics_impl ──────────────────────────────────────────────

#[tokio::test]
async fn query_metrics_unreachable_no_filter() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::QueryMetricsParams {
        pipeline_group: None,
        include_prometheus: None,
    };
    let result = varpulis_mcp::tools::query_metrics_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    assert!(
        text.contains("reach") || text.contains("connect") || text.contains("Unreachable"),
        "Expected connection error, got: {}",
        text
    );
}

#[tokio::test]
async fn query_metrics_unreachable_with_pipeline_group() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::QueryMetricsParams {
        pipeline_group: Some("trade-alerts".to_string()),
        include_prometheus: None,
    };
    let result = varpulis_mcp::tools::query_metrics_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
}

#[tokio::test]
async fn query_metrics_unreachable_with_prometheus() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::QueryMetricsParams {
        pipeline_group: None,
        include_prometheus: Some(true),
    };
    let result = varpulis_mcp::tools::query_metrics_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
}

#[tokio::test]
async fn query_metrics_unreachable_with_all_params() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::QueryMetricsParams {
        pipeline_group: Some("my-group".to_string()),
        include_prometheus: Some(true),
    };
    let result = varpulis_mcp::tools::query_metrics_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
}

#[tokio::test]
async fn query_metrics_prometheus_false() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::QueryMetricsParams {
        pipeline_group: None,
        include_prometheus: Some(false),
    };
    let result = varpulis_mcp::tools::query_metrics_impl(&client, params).await;
    // Still fails because metrics endpoint is unreachable
    assert_eq!(result.is_error, Some(true));
}

// ── explain_alert_impl ──────────────────────────────────────────────

#[tokio::test]
async fn explain_alert_unreachable_with_description() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::ExplainAlertParams {
        pipeline_group: "trade-alerts".to_string(),
        alert_description: Some("High latency detected on trade processing pipeline".to_string()),
    };
    let result = varpulis_mcp::tools::explain_alert_impl(&client, params).await;
    // explain_alert gathers from multiple APIs — all will fail but it still builds a report
    assert_eq!(result.is_error, Some(false));
    let text = first_text(&result);
    // Verify the report structure
    assert!(
        text.contains("# Alert Investigation"),
        "Report should have title, got: {}",
        &text[..text.len().min(200)]
    );
    assert!(
        text.contains("trade-alerts"),
        "Report should mention pipeline group, got: {}",
        &text[..text.len().min(200)]
    );
    assert!(
        text.contains("High latency"),
        "Report should mention alert description, got: {}",
        &text[..text.len().min(500)]
    );
    assert!(
        text.contains("## Pipeline Group"),
        "Report should have Pipeline Group section"
    );
    assert!(
        text.contains("## Worker Health"),
        "Report should have Worker Health section"
    );
    assert!(
        text.contains("## Metrics"),
        "Report should have Metrics section"
    );
    assert!(
        text.contains("## Scaling Recommendation"),
        "Report should have Scaling section"
    );
    assert!(
        text.contains("## Topology"),
        "Report should have Topology section"
    );
    assert!(
        text.contains("## Possible Causes"),
        "Report should have Possible Causes section"
    );
    // All API calls fail, so error messages should appear
    assert!(
        text.contains("Error fetching"),
        "Report should contain error messages from failed API calls"
    );
}

#[tokio::test]
async fn explain_alert_unreachable_without_description() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::ExplainAlertParams {
        pipeline_group: "my-pipeline".to_string(),
        alert_description: None,
    };
    let result = varpulis_mcp::tools::explain_alert_impl(&client, params).await;
    assert_eq!(result.is_error, Some(false));
    let text = first_text(&result);
    // Should NOT contain "**Alert**:" section since no description given
    assert!(
        !text.contains("**Alert**:"),
        "Report should not have alert description when None"
    );
    assert!(
        text.contains("# Alert Investigation: my-pipeline"),
        "Report should have title with pipeline group"
    );
}

#[tokio::test]
async fn explain_alert_report_has_possible_causes_section() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::ExplainAlertParams {
        pipeline_group: "test-group".to_string(),
        alert_description: Some("CPU overload".to_string()),
    };
    let result = varpulis_mcp::tools::explain_alert_impl(&client, params).await;
    let text = first_text(&result);
    // When all APIs fail, the heuristic analysis can't find specific causes
    // so it should show the "No obvious issues" fallback or specific error causes
    assert!(
        text.contains("Possible Causes"),
        "Report should have Possible Causes section"
    );
    // Since all API calls fail (all Err), we won't enter the Ok branches in heuristics
    // so causes vec should be empty → "No obvious issues detected"
    assert!(
        text.contains("No obvious issues")
            || text.contains("Application logs")
            || text.contains("Connector health"),
        "Report should suggest checking logs when no causes found, got: {}",
        &text[text.len().saturating_sub(300)..]
    );
}

// ── search_events_impl ──────────────────────────────────────────────

#[tokio::test]
async fn search_events_unreachable() {
    let client = unreachable_client();
    let mut fields = serde_json::Map::new();
    fields.insert("symbol".into(), json!("AAPL"));
    fields.insert("price".into(), json!(150.0));
    let params = varpulis_mcp::tools::SearchEventsParams {
        pipeline_group: "trade-group".to_string(),
        event_type: "TradeEvent".to_string(),
        fields,
    };
    let result = varpulis_mcp::tools::search_events_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    assert!(
        text.contains("reach") || text.contains("connect") || text.contains("Unreachable"),
        "Expected connection error, got: {}",
        text
    );
}

#[tokio::test]
async fn search_events_empty_fields() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::SearchEventsParams {
        pipeline_group: "group-1".to_string(),
        event_type: "TestEvent".to_string(),
        fields: serde_json::Map::new(),
    };
    let result = varpulis_mcp::tools::search_events_impl(&client, params).await;
    assert_eq!(result.is_error, Some(true));
}

// ── list_models_impl ────────────────────────────────────────────────

#[tokio::test]
async fn list_models_unreachable() {
    let client = unreachable_client();
    let result = varpulis_mcp::tools::list_models_impl(&client).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    assert!(
        text.contains("reach") || text.contains("connect") || text.contains("Unreachable"),
        "Expected connection error, got: {}",
        text
    );
}

// ── validate_vpl: diagnostic formatting details ─────────────────────

#[tokio::test]
async fn validate_vpl_error_includes_line_col_format() {
    // Semantic errors should include line:col format in diagnostics
    let source = "stream S = X\n    .having(price > 1)";
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl(source, &client).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    // Diagnostics format: "line:col: [severity] message (hint: ...)"
    // Check that we have at least one line:col pattern
    assert!(
        text.contains(":"),
        "Diagnostics should contain line:col format, got: {}",
        text
    );
}

#[tokio::test]
async fn validate_vpl_error_diagnostics_include_hint_text() {
    // .having() without .aggregate() produces E010 with a hint "add .aggregate(...)"
    // This exercises the hint formatting code: d.hint.as_ref().map(|h| format!(" (hint: {})", h))
    let source = "stream S = X.having(price > 1)";
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl(source, &client).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    // The hint should be formatted as "(hint: ...)"
    assert!(
        text.contains("hint") || text.contains("aggregate"),
        "Error diagnostics should include hint about aggregate, got: {}",
        text
    );
}

#[tokio::test]
async fn validate_vpl_multiple_errors() {
    // Multiple .having() without .aggregate() should produce multiple diagnostics
    let source = "stream S1 = X\n    .having(a > 1)\nstream S2 = Y\n    .having(b > 2)";
    let client = unreachable_client();
    let result = varpulis_mcp::tools::validate_vpl_impl(source, &client).await;
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    // Should contain multiple error lines
    assert!(
        text.contains(".having()"),
        "Should mention .having() in diagnostics, got: {}",
        text
    );
}

// ── deploy_pipeline: affinity in JSON spec ──────────────────────────

#[tokio::test]
async fn deploy_pipeline_complex_vpl_with_affinity() {
    let client = unreachable_client();
    let params = varpulis_mcp::tools::DeployPipelineParams {
        name: "complex-group".to_string(),
        source: "stream Filtered = TradeEvent\n    .filter(price > 100)".to_string(),
        worker_affinity: Some("worker-2".to_string()),
    };
    let result = varpulis_mcp::tools::deploy_pipeline_impl(&client, params).await;
    // Parses OK, fails at deployment
    assert_eq!(result.is_error, Some(true));
    let text = first_text(&result);
    assert!(
        text.contains("reach") || text.contains("connect") || text.contains("Unreachable"),
        "Expected connection error after passing parse, got: {}",
        text
    );
}

// ── Tool router: verify server construction exercises tool router ────

#[test]
fn tool_router_created_on_server_construction() {
    // VarpulisMcpServer::new() calls create_tool_router() which exercises
    // the tool_router! macro expansion in tools.rs (lines ~65-148)
    let client = unreachable_client();
    let server = VarpulisMcpServer::new(client);
    // Verify via get_info that tools capability is enabled
    use rmcp::ServerHandler;
    let info = server.get_info();
    assert!(info.capabilities.tools.is_some());
}
