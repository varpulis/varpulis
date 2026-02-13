//! MCP prompt implementations for Varpulis.

use rmcp::model::{GetPromptResult, Prompt, PromptArgument, PromptMessage, PromptMessageRole};

/// List all available prompts.
pub fn list_prompts() -> Vec<Prompt> {
    vec![
        Prompt::new(
            "investigate_alert",
            Some(
                "Investigate an alert or anomaly in the Varpulis cluster with step-by-step root cause analysis."
            ),
            Some(vec![
                PromptArgument {
                    name: "alert".into(),
                    title: None,
                    description: Some("Description of the alert or anomaly to investigate".into()),
                    required: Some(true),
                },
                PromptArgument {
                    name: "pipeline_group".into(),
                    title: None,
                    description: Some("Pipeline group name or ID (optional)".into()),
                    required: Some(false),
                },
            ]),
        ),
        Prompt::new(
            "create_fraud_detection",
            Some(
                "Generate a VPL pipeline for fraud detection based on event types and fields."
            ),
            Some(vec![
                PromptArgument {
                    name: "event_type".into(),
                    title: None,
                    description: Some("Name of the event type to monitor (e.g., Transaction)".into()),
                    required: Some(true),
                },
                PromptArgument {
                    name: "fields".into(),
                    title: None,
                    description: Some(
                        "Comma-separated fields to use (e.g., user_id, amount, status)".into(),
                    ),
                    required: Some(true),
                },
                PromptArgument {
                    name: "time_window".into(),
                    title: None,
                    description: Some("Time window for pattern detection (e.g., 10m, 1h). Default: 10m".into()),
                    required: Some(false),
                },
            ]),
        ),
        Prompt::new(
            "optimize_stream",
            Some(
                "Analyze a deployed pipeline and suggest performance optimizations."
            ),
            Some(vec![
                PromptArgument {
                    name: "pipeline_group".into(),
                    title: None,
                    description: Some("Pipeline group name or ID to optimize".into()),
                    required: Some(true),
                },
                PromptArgument {
                    name: "goal".into(),
                    title: None,
                    description: Some(
                        "Optimization goal: throughput, latency, or memory (optional)".into(),
                    ),
                    required: Some(false),
                },
            ]),
        ),
    ]
}

/// Get a prompt by name with arguments filled in.
pub fn get_prompt(
    name: &str,
    args: &serde_json::Map<String, serde_json::Value>,
) -> Result<GetPromptResult, String> {
    match name {
        "investigate_alert" => Ok(build_investigate_alert(args)),
        "create_fraud_detection" => Ok(build_create_fraud_detection(args)),
        "optimize_stream" => Ok(build_optimize_stream(args)),
        _ => Err(format!("Unknown prompt: {}", name)),
    }
}

fn get_str(args: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<String> {
    args.get(key).and_then(|v| v.as_str()).map(String::from)
}

fn build_investigate_alert(args: &serde_json::Map<String, serde_json::Value>) -> GetPromptResult {
    let alert = get_str(args, "alert").unwrap_or_default();
    let group =
        get_str(args, "pipeline_group").unwrap_or_else(|| "(auto-detect from alert)".into());

    let content = format!(
        r#"You are investigating an alert in a Varpulis CEP cluster.

**Alert**: {alert}
**Pipeline Group**: {group}

Follow these steps to diagnose the issue:

1. **List pipelines** — Call `list_pipelines` to see all deployed groups and their statuses.
2. **Check metrics** — Call `query_metrics` with pipeline_group="{group}" to see throughput, event counts, and worker health.
3. **Deep investigation** — Call `explain_alert` with pipeline_group="{group}" and the alert description. This gathers worker health, topology, scaling recommendations, and heuristic analysis.
4. **Test event flow** — If the pipeline should be processing events but isn't, use `search_events` to inject a test event and verify the pipeline responds.

After gathering data, provide:
- **Root cause** — The most likely explanation for the alert
- **Evidence** — Specific data points that support your conclusion
- **Remediation** — Concrete steps to fix the issue
- **Prevention** — How to avoid this in the future"#
    );

    GetPromptResult {
        description: Some("Step-by-step alert investigation workflow".into()),
        messages: vec![PromptMessage::new_text(PromptMessageRole::User, content)],
    }
}

fn build_create_fraud_detection(
    args: &serde_json::Map<String, serde_json::Value>,
) -> GetPromptResult {
    let event_type = get_str(args, "event_type").unwrap_or_default();
    let fields = get_str(args, "fields").unwrap_or_default();
    let window = get_str(args, "time_window").unwrap_or_else(|| "10m".into());

    let content = format!(
        r#"Generate a VPL fraud detection pipeline for the following specification:

**Event type**: {event_type}
**Fields**: {fields}
**Time window**: {window}

Create a VPL pipeline that includes:

1. **Event declaration** — Define the `{event_type}` event type with the specified fields
2. **Basic filter** — A stream that filters suspicious events using `.where()` conditions
3. **Windowed aggregation** — Use `.window({window})` with `.aggregate()` to detect frequency anomalies (e.g., count > threshold)
4. **Sequence pattern** — Use the `->` sequence operator with `.within({window})` to detect suspicious event sequences
5. **Alert output** — Use `.emit()` to produce alert events with severity, description, and relevant fields

After generating the VPL:
- Call `validate_vpl` to check for syntax/semantic errors
- Fix any issues and re-validate
- When valid, call `deploy_pipeline` to deploy it

Here's a template to start from:

```vpl
event {event_type}:
    {fields_formatted}

# Filter suspicious events
stream Suspicious{event_type} = {event_type}
    .where(/* add conditions based on your fields */)

# Detect frequency anomalies
stream FrequencyAlert = {event_type}
    .partition_by(/* key field */)
    .window({window})
    .aggregate(event_count: count())
    .where(event_count > 10)
    .emit(
        alert_type: "frequency_anomaly",
        severity: "high"
    )
```

Adapt the template based on the actual fields and their types. Use your knowledge of fraud patterns to create meaningful detection rules."#,
        fields_formatted = fields
            .split(',')
            .map(|f| format!("    {}: str", f.trim()))
            .collect::<Vec<_>>()
            .join("\n")
    );

    GetPromptResult {
        description: Some("VPL fraud detection pipeline generator".into()),
        messages: vec![PromptMessage::new_text(PromptMessageRole::User, content)],
    }
}

fn build_optimize_stream(args: &serde_json::Map<String, serde_json::Value>) -> GetPromptResult {
    let group = get_str(args, "pipeline_group").unwrap_or_default();
    let goal = get_str(args, "goal").unwrap_or_else(|| "throughput".into());

    let content = format!(
        r#"Analyze and optimize the Varpulis pipeline group "{group}" for {goal}.

Follow these steps:

1. **Gather current state** — Call `list_pipelines` to get the group's current VPL source and status.
2. **Check metrics** — Call `query_metrics` with pipeline_group="{group}" and include_prometheus=true to see current performance numbers.
3. **Analyze topology** — Call `explain_alert` with pipeline_group="{group}" to understand worker placement and scaling state.

Then apply these optimization strategies based on the goal:

**For throughput:**
- Push `.where()` filters as early as possible to reduce downstream event volume
- Use `.partition_by()` to enable parallel processing across workers
- Replace complex sequence patterns with `trend_aggregate` when counting is sufficient
- Increase batch sizes for sink connectors

**For latency:**
- Reduce window sizes where possible
- Minimize the number of chained operations
- Use smaller sliding intervals
- Check if any workers are overloaded and rebalance

**For memory:**
- Reduce window durations (shorter windows = less state)
- Use `count_distinct` instead of collecting all values
- Partition by high-cardinality keys to distribute state
- Check for unbounded state accumulation in sequence patterns

After analysis, provide:
- **Current performance** — Key metrics from the cluster
- **Bottlenecks identified** — What's limiting performance
- **Optimized VPL** — Rewritten pipeline with improvements
- **Expected improvement** — Estimated impact of each change
- **Validation** — Use `validate_vpl` to verify the optimized VPL"#
    );

    GetPromptResult {
        description: Some("Pipeline performance optimization guide".into()),
        messages: vec![PromptMessage::new_text(PromptMessageRole::User, content)],
    }
}
