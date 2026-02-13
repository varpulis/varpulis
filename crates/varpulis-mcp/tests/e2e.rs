//! E2E integration test for the varpulis-mcp MCP server.
//!
//! Spawns the server binary over stdio and validates all MCP capabilities
//! (tools, resources, prompts) using the rmcp client SDK — exactly how
//! Claude Desktop or any MCP client connects.

use rmcp::model::*;
use rmcp::transport::TokioChildProcess;
use rmcp::ServiceExt;
use tokio::process::Command;

/// Build a `CallToolRequestParams` helper.
fn call_tool_params(name: &str, arguments: serde_json::Value) -> CallToolRequestParams {
    CallToolRequestParams {
        meta: None,
        name: name.to_string().into(),
        arguments: Some(arguments.as_object().unwrap().clone()),
        task: None,
    }
}

/// Build a `GetPromptRequestParams` helper.
fn get_prompt_params(name: &str, arguments: serde_json::Value) -> GetPromptRequestParams {
    GetPromptRequestParams {
        meta: None,
        name: name.into(),
        arguments: Some(arguments.as_object().unwrap().clone()),
    }
}

/// Extract text from the first Content item in a CallToolResult.
fn first_text(result: &CallToolResult) -> &str {
    result.content[0]
        .as_text()
        .expect("expected text content")
        .text
        .as_str()
}

/// Extract text from ReadResourceResult.
fn resource_text(result: &ReadResourceResult) -> String {
    result
        .contents
        .iter()
        .filter_map(|c| match c {
            ResourceContents::TextResourceContents { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

/// Extract text from the first PromptMessage.
fn prompt_text(result: &GetPromptResult) -> String {
    result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            PromptMessageContent::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn mcp_server_e2e() -> anyhow::Result<()> {
    // ─── Setup ────────────────────────────────────────────────────────
    // Spawn the MCP server binary pointing at a non-existent coordinator
    // (port 99999) so coordinator-dependent calls fail gracefully.
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_varpulis-mcp"));
    cmd.arg("--coordinator-url").arg("http://localhost:99999");

    let transport = TokioChildProcess::new(cmd)?;
    let client = ().serve(transport).await?;

    // ─── Tools ────────────────────────────────────────────────────────

    // 1. list_all_tools → 6 tools
    let tools = client.list_all_tools().await?;
    assert_eq!(
        tools.len(),
        6,
        "Expected 6 tools, got {}: {:?}",
        tools.len(),
        tools.iter().map(|t| &t.name).collect::<Vec<_>>()
    );

    let tool_names: Vec<&str> = tools.iter().map(|t| &*t.name).collect();
    for expected in &[
        "validate_vpl",
        "deploy_pipeline",
        "list_pipelines",
        "query_metrics",
        "explain_alert",
        "search_events",
    ] {
        assert!(tool_names.contains(expected), "Missing tool: {}", expected);
    }

    // 2. validate_vpl — valid VPL
    let result = client
        .call_tool(call_tool_params(
            "validate_vpl",
            serde_json::json!({ "source": "stream A = X" }),
        ))
        .await?;
    assert_eq!(result.is_error, Some(false), "valid VPL should succeed");
    assert!(
        first_text(&result).contains("valid"),
        "Expected 'valid' in: {}",
        first_text(&result)
    );

    // 3. validate_vpl — parse error
    let result = client
        .call_tool(call_tool_params(
            "validate_vpl",
            serde_json::json!({ "source": "not valid !!!" }),
        ))
        .await?;
    assert_eq!(
        result.is_error,
        Some(true),
        "parse error should be is_error"
    );

    // 4. validate_vpl — semantic error (.having() without .aggregate())
    let result = client
        .call_tool(call_tool_params(
            "validate_vpl",
            serde_json::json!({
                "source": "stream A = X\n    .having(x > 1)"
            }),
        ))
        .await?;
    assert_eq!(
        result.is_error,
        Some(true),
        "semantic error (.having without .aggregate) should be is_error, got: {}",
        first_text(&result)
    );

    // 5. list_pipelines — no coordinator → graceful error
    let result = client
        .call_tool(call_tool_params("list_pipelines", serde_json::json!({})))
        .await?;
    assert_eq!(
        result.is_error,
        Some(true),
        "list_pipelines without coordinator should error"
    );
    let text = first_text(&result);
    assert!(
        text.contains("reach") || text.contains("connect") || text.contains("Unreachable"),
        "Expected connection error, got: {}",
        text
    );

    // 6. deploy_pipeline — parse error (no coordinator needed for parse check)
    let result = client
        .call_tool(call_tool_params(
            "deploy_pipeline",
            serde_json::json!({
                "name": "test",
                "source": "not valid vpl !!!"
            }),
        ))
        .await?;
    assert_eq!(
        result.is_error,
        Some(true),
        "deploy_pipeline with invalid VPL should error"
    );
    let text = first_text(&result);
    assert!(
        text.contains("parse") || text.contains("Parse") || text.contains("error"),
        "Expected parse error, got: {}",
        text
    );

    // ─── Resources ────────────────────────────────────────────────────

    // 7. list_all_resources → 3 resources
    let resources = client.list_all_resources().await?;
    assert_eq!(
        resources.len(),
        3,
        "Expected 3 resources, got {}: {:?}",
        resources.len(),
        resources.iter().map(|r| &r.uri).collect::<Vec<_>>()
    );

    let resource_uris: Vec<&str> = resources.iter().map(|r| r.uri.as_str()).collect();
    for expected_uri in &[
        "varpulis://cluster/status",
        "varpulis://docs/vpl-reference",
        "varpulis://cluster/metrics",
    ] {
        assert!(
            resource_uris.contains(expected_uri),
            "Missing resource URI: {}",
            expected_uri
        );
    }

    // 8. read_resource — VPL reference (static, no coordinator)
    let result = client
        .read_resource(ReadResourceRequestParams {
            meta: None,
            uri: "varpulis://docs/vpl-reference".into(),
        })
        .await?;
    let text = resource_text(&result);
    assert!(!text.is_empty(), "VPL reference should be non-empty");
    assert!(
        text.contains("VPL") || text.contains("vpl"),
        "VPL reference should contain 'VPL', got: {}...",
        &text[..text.len().min(200)]
    );
    assert!(
        text.contains("stream"),
        "VPL reference should contain 'stream', got: {}...",
        &text[..text.len().min(200)]
    );

    // 9. read_resource — cluster status (no coordinator → error)
    let result = client
        .read_resource(ReadResourceRequestParams {
            meta: None,
            uri: "varpulis://cluster/status".into(),
        })
        .await;
    assert!(
        result.is_err(),
        "cluster status without coordinator should fail"
    );

    // ─── Prompts ──────────────────────────────────────────────────────

    // 10. list_all_prompts → 3 prompts
    let prompts = client.list_all_prompts().await?;
    assert_eq!(
        prompts.len(),
        3,
        "Expected 3 prompts, got {}: {:?}",
        prompts.len(),
        prompts.iter().map(|p| &p.name).collect::<Vec<_>>()
    );

    let prompt_names: Vec<&str> = prompts.iter().map(|p| p.name.as_str()).collect();
    for expected in &[
        "investigate_alert",
        "create_fraud_detection",
        "optimize_stream",
    ] {
        assert!(
            prompt_names.contains(expected),
            "Missing prompt: {}",
            expected
        );
    }

    // Verify prompt arguments exist
    let investigate = prompts
        .iter()
        .find(|p| p.name == "investigate_alert")
        .unwrap();
    let args = investigate.arguments.as_ref().unwrap();
    assert!(
        args.iter().any(|a| a.name == "alert"),
        "investigate_alert should have 'alert' argument"
    );

    let fraud = prompts
        .iter()
        .find(|p| p.name == "create_fraud_detection")
        .unwrap();
    let args = fraud.arguments.as_ref().unwrap();
    assert!(
        args.iter().any(|a| a.name == "event_type"),
        "create_fraud_detection should have 'event_type' argument"
    );
    assert!(
        args.iter().any(|a| a.name == "fields"),
        "create_fraud_detection should have 'fields' argument"
    );

    // 11. get_prompt — investigate_alert
    let result = client
        .get_prompt(get_prompt_params(
            "investigate_alert",
            serde_json::json!({
                "alert": "High latency on trade pipeline",
                "pipeline_group": "trade-alerts"
            }),
        ))
        .await?;
    assert!(
        !result.messages.is_empty(),
        "investigate_alert should return messages"
    );
    let text = prompt_text(&result);
    assert!(
        text.contains("alert") || text.contains("Alert"),
        "investigate_alert should mention alert investigation, got: {}...",
        &text[..text.len().min(200)]
    );

    // 12. get_prompt — create_fraud_detection
    let result = client
        .get_prompt(get_prompt_params(
            "create_fraud_detection",
            serde_json::json!({
                "event_type": "Transaction",
                "fields": "user_id, amount, status"
            }),
        ))
        .await?;
    assert!(
        !result.messages.is_empty(),
        "create_fraud_detection should return messages"
    );
    let text = prompt_text(&result);
    assert!(
        text.contains("Transaction"),
        "fraud detection prompt should contain event_type 'Transaction', got: {}...",
        &text[..text.len().min(200)]
    );
    assert!(
        text.contains("user_id") || text.contains("amount"),
        "fraud detection prompt should contain fields, got: {}...",
        &text[..text.len().min(200)]
    );

    // 13. get_prompt — optimize_stream
    let result = client
        .get_prompt(get_prompt_params(
            "optimize_stream",
            serde_json::json!({
                "pipeline_group": "my-pipeline",
                "goal": "latency"
            }),
        ))
        .await?;
    assert!(
        !result.messages.is_empty(),
        "optimize_stream should return messages"
    );
    let text = prompt_text(&result);
    assert!(
        text.contains("latency") || text.contains("optim"),
        "optimize_stream prompt should mention optimization, got: {}...",
        &text[..text.len().min(200)]
    );

    // ─── Teardown ─────────────────────────────────────────────────────
    client.cancel().await?;

    Ok(())
}
