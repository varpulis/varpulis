//! Provider-agnostic LLM chat support for the AI assistant.
//!
//! Supports OpenAI-compatible APIs (Ollama, OpenAI, Groq, Together, vLLM)
//! and native Anthropic Messages API.

use serde::{Deserialize, Serialize};

/// LLM provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    pub endpoint: String,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    pub provider: LlmProvider,
}

/// Which request/response format to use.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LlmProvider {
    #[default]
    #[serde(rename = "openai-compatible")]
    OpenAiCompatible,
    Anthropic,
}

/// A chat message in the conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// Request to the chat endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub messages: Vec<ChatMessage>,
}

/// Tool call information returned from the LLM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallInfo {
    pub tool_name: String,
    pub input: serde_json::Value,
    pub output: serde_json::Value,
}

/// Response from the chat endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    pub message: ChatMessage,
    #[serde(default)]
    pub tool_calls_executed: Vec<ToolCallInfo>,
}

/// LLM config summary for the frontend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfigResponse {
    pub provider: String,
    pub model: String,
    pub endpoint: String,
    pub has_api_key: bool,
    pub configured: bool,
}

/// Tool definition in OpenAI function-calling format.
fn cluster_tool_definitions() -> serde_json::Value {
    serde_json::json!([
        {
            "type": "function",
            "function": {
                "name": "list_pipelines",
                "description": "List all deployed pipeline groups with their status, worker placements, and VPL source.",
                "parameters": { "type": "object", "properties": {} }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "list_workers",
                "description": "List all registered workers with their status, capacity, and assigned pipelines.",
                "parameters": { "type": "object", "properties": {} }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "query_metrics",
                "description": "Query real-time metrics for a pipeline group or the whole cluster.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "pipeline_group": {
                            "type": "string",
                            "description": "Optional pipeline group name to filter metrics"
                        }
                    }
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "list_connectors",
                "description": "List all configured connectors (MQTT, Kafka, etc.) with their parameters.",
                "parameters": { "type": "object", "properties": {} }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "list_models",
                "description": "List all registered ONNX models in the model registry.",
                "parameters": { "type": "object", "properties": {} }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "validate_vpl",
                "description": "Validate VPL source code for syntax and semantic correctness.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "source": {
                            "type": "string",
                            "description": "VPL source code to validate"
                        }
                    },
                    "required": ["source"]
                }
            }
        }
    ])
}

/// Execute a tool call against the coordinator state.
pub async fn execute_tool(
    tool_name: &str,
    args: &serde_json::Value,
    coordinator: &crate::api::SharedCoordinator,
) -> serde_json::Value {
    let coord = coordinator.read().await;
    match tool_name {
        "list_pipelines" => {
            let groups: Vec<serde_json::Value> = coord
                .pipeline_groups
                .values()
                .map(|g| {
                    serde_json::json!({
                        "name": g.name,
                        "id": g.id,
                        "status": format!("{:?}", g.status),
                        "pipelines": g.placements.keys().collect::<Vec<_>>(),
                    })
                })
                .collect();
            serde_json::json!({ "pipeline_groups": groups, "total": groups.len() })
        }
        "list_workers" => {
            let workers: Vec<serde_json::Value> = coord
                .workers
                .values()
                .map(|w| {
                    serde_json::json!({
                        "id": w.id.0,
                        "address": w.address,
                        "status": w.status.to_string(),
                        "pipelines_running": w.capacity.pipelines_running,
                        "assigned_pipelines": w.assigned_pipelines,
                    })
                })
                .collect();
            serde_json::json!({ "workers": workers, "total": workers.len() })
        }
        "query_metrics" => {
            let filter = args.get("pipeline_group").and_then(|v| v.as_str());
            let metrics: Vec<serde_json::Value> = coord
                .worker_metrics
                .iter()
                .flat_map(|(wid, pms)| {
                    pms.iter()
                        .filter(|pm| filter.is_none_or(|f| pm.pipeline_name.contains(f)))
                        .map(move |pm| {
                            serde_json::json!({
                                "worker_id": wid.0,
                                "pipeline_name": pm.pipeline_name,
                                "events_in": pm.events_in,
                                "events_out": pm.events_out,
                            })
                        })
                })
                .collect();
            serde_json::json!({ "metrics": metrics })
        }
        "list_connectors" => {
            let conns: Vec<serde_json::Value> = coord
                .connectors
                .values()
                .map(|c| {
                    serde_json::json!({
                        "name": c.name,
                        "type": c.connector_type,
                        "params": c.params,
                    })
                })
                .collect();
            serde_json::json!({ "connectors": conns })
        }
        "list_models" => {
            let models: Vec<serde_json::Value> = coord
                .model_registry
                .values()
                .map(|m| serde_json::to_value(m).unwrap_or_default())
                .collect();
            serde_json::json!({ "models": models })
        }
        "validate_vpl" => {
            if let Some(source) = args.get("source").and_then(|v| v.as_str()) {
                match varpulis_parser::parse(source) {
                    Ok(_) => serde_json::json!({ "valid": true, "diagnostics": [] }),
                    Err(e) => {
                        serde_json::json!({ "valid": false, "diagnostics": [{ "message": e.to_string() }] })
                    }
                }
            } else {
                serde_json::json!({ "error": "Missing 'source' parameter" })
            }
        }
        _ => serde_json::json!({ "error": format!("Unknown tool: {}", tool_name) }),
    }
}

/// Send a chat completion request to the configured LLM provider.
pub async fn chat_completion(
    config: &LlmConfig,
    messages: &[ChatMessage],
    coordinator: &crate::api::SharedCoordinator,
) -> Result<ChatResponse, String> {
    let http_client = reqwest::Client::new();
    let tools = cluster_tool_definitions();

    let mut conversation: Vec<serde_json::Value> = messages
        .iter()
        .map(|m| serde_json::json!({ "role": m.role, "content": m.content }))
        .collect();

    // Add system message
    conversation.insert(
        0,
        serde_json::json!({
            "role": "system",
            "content": "You are Varpulis AI Assistant, an expert on the Varpulis Complex Event Processing platform. \
                       You help users understand their cluster status, pipeline deployments, metrics, and VPL queries. \
                       Use the available tools to fetch real-time information before answering questions. \
                       Be concise and helpful."
        }),
    );

    let mut tool_calls_executed = Vec::new();
    let max_iterations = 10;

    for _ in 0..max_iterations {
        let (response_body, status) = match config.provider {
            LlmProvider::OpenAiCompatible => {
                let url = format!("{}/chat/completions", config.endpoint.trim_end_matches('/'));
                let body = serde_json::json!({
                    "model": config.model,
                    "messages": conversation,
                    "tools": tools,
                });
                let mut req = http_client.post(&url).json(&body);
                if let Some(ref key) = config.api_key {
                    req = req.header("Authorization", format!("Bearer {}", key));
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| format!("LLM request failed: {}", e))?;
                let status = resp.status();
                let body = resp
                    .text()
                    .await
                    .map_err(|e| format!("LLM response read failed: {}", e))?;
                (body, status)
            }
            LlmProvider::Anthropic => {
                let url = format!("{}/messages", config.endpoint.trim_end_matches('/'));
                // Convert to Anthropic format
                let anthropic_messages: Vec<serde_json::Value> = conversation
                    .iter()
                    .filter(|m| m["role"].as_str() != Some("system"))
                    .cloned()
                    .collect();
                let system_msg = conversation
                    .iter()
                    .find(|m| m["role"].as_str() == Some("system"))
                    .and_then(|m| m["content"].as_str())
                    .unwrap_or("");

                // Convert tools to Anthropic format
                let anthropic_tools: Vec<serde_json::Value> = tools
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|t| {
                        serde_json::json!({
                            "name": t["function"]["name"],
                            "description": t["function"]["description"],
                            "input_schema": t["function"]["parameters"],
                        })
                    })
                    .collect();

                let body = serde_json::json!({
                    "model": config.model,
                    "max_tokens": 4096,
                    "system": system_msg,
                    "messages": anthropic_messages,
                    "tools": anthropic_tools,
                });
                let mut req = http_client
                    .post(&url)
                    .header("content-type", "application/json")
                    .header("anthropic-version", "2023-06-01")
                    .json(&body);
                if let Some(ref key) = config.api_key {
                    req = req.header("x-api-key", key);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| format!("Anthropic request failed: {}", e))?;
                let status = resp.status();
                let body = resp
                    .text()
                    .await
                    .map_err(|e| format!("Anthropic response read failed: {}", e))?;
                (body, status)
            }
        };

        if !status.is_success() {
            return Err(format!("LLM returned HTTP {}: {}", status, response_body));
        }

        let json: serde_json::Value = serde_json::from_str(&response_body)
            .map_err(|e| format!("Failed to parse LLM response: {}", e))?;

        // Extract tool calls and text based on provider
        match config.provider {
            LlmProvider::OpenAiCompatible => {
                let choice = &json["choices"][0]["message"];
                if let Some(tool_calls) = choice["tool_calls"].as_array() {
                    // Append assistant message with tool calls
                    conversation.push(choice.clone());

                    for tc in tool_calls {
                        let fn_name = tc["function"]["name"].as_str().unwrap_or("");
                        let fn_args_str = tc["function"]["arguments"].as_str().unwrap_or("{}");
                        let fn_args: serde_json::Value =
                            serde_json::from_str(fn_args_str).unwrap_or_default();
                        let tool_result = execute_tool(fn_name, &fn_args, coordinator).await;

                        tool_calls_executed.push(ToolCallInfo {
                            tool_name: fn_name.to_string(),
                            input: fn_args,
                            output: tool_result.clone(),
                        });

                        conversation.push(serde_json::json!({
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": serde_json::to_string(&tool_result).unwrap_or_default(),
                        }));
                    }
                    continue; // Loop back to get final response
                }

                // No tool calls — return text response
                let content = choice["content"].as_str().unwrap_or("").to_string();
                return Ok(ChatResponse {
                    message: ChatMessage {
                        role: "assistant".to_string(),
                        content,
                    },
                    tool_calls_executed,
                });
            }
            LlmProvider::Anthropic => {
                let content_blocks = json["content"].as_array();
                let stop_reason = json["stop_reason"].as_str().unwrap_or("");

                if stop_reason == "tool_use" {
                    // Append assistant message
                    conversation.push(serde_json::json!({
                        "role": "assistant",
                        "content": json["content"],
                    }));

                    let mut tool_results = Vec::new();
                    if let Some(blocks) = content_blocks {
                        for block in blocks {
                            if block["type"].as_str() == Some("tool_use") {
                                let fn_name = block["name"].as_str().unwrap_or("");
                                let fn_args = &block["input"];
                                let tool_result = execute_tool(fn_name, fn_args, coordinator).await;

                                tool_calls_executed.push(ToolCallInfo {
                                    tool_name: fn_name.to_string(),
                                    input: fn_args.clone(),
                                    output: tool_result.clone(),
                                });

                                tool_results.push(serde_json::json!({
                                    "type": "tool_result",
                                    "tool_use_id": block["id"],
                                    "content": serde_json::to_string(&tool_result).unwrap_or_default(),
                                }));
                            }
                        }
                    }

                    conversation.push(serde_json::json!({
                        "role": "user",
                        "content": tool_results,
                    }));
                    continue;
                }

                // Extract text from content blocks
                let text = content_blocks
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter_map(|b| {
                                if b["type"].as_str() == Some("text") {
                                    b["text"].as_str().map(|s| s.to_string())
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("")
                    })
                    .unwrap_or_default();

                return Ok(ChatResponse {
                    message: ChatMessage {
                        role: "assistant".to_string(),
                        content: text,
                    },
                    tool_calls_executed,
                });
            }
        }
    }

    Err("Max tool call iterations reached".to_string())
}
