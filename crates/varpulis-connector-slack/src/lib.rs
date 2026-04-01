//! Slack webhook connector for Varpulis.
//!
//! Sends security alerts to Slack channels via incoming webhooks,
//! formatted with Block Kit for severity-colored alert cards.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::warn;
use varpulis_connector_api::{
    ConfigParamInfo, ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory,
    ConnectorHealth, Sink, SinkConnector, SinkConnectorAdapter,
};
use varpulis_core::event::Event;
use varpulis_core::Value;

// ---------------------------------------------------------------------------
// Slack Message Formatting
// ---------------------------------------------------------------------------

/// Format a Varpulis event as a Slack Block Kit message payload.
pub fn event_to_slack_payload(event: &Event) -> serde_json::Value {
    let get_str = |key: &str| -> String {
        event
            .get(key)
            .map(|v| match v {
                Value::Str(s) => s.to_string(),
                other => other.to_string(),
            })
            .unwrap_or_default()
    };

    let rule = get_str("rule");
    let rule = if rule.is_empty() {
        event.event_type.to_string()
    } else {
        rule
    };
    let mitre = get_str("mitre");
    let severity = get_str("severity");
    let summary = get_str("summary");

    let severity_emoji = match severity.as_str() {
        "critical" => ":red_circle:",
        "high" => ":large_orange_circle:",
        "medium" => ":large_yellow_circle:",
        "low" => ":large_blue_circle:",
        _ => ":white_circle:",
    };

    // Build context fields (skip standard alert fields)
    let skip = ["rule", "mitre", "severity", "summary", "event_type"];
    let mut context_parts: Vec<String> = Vec::new();
    for (key, val) in &event.data {
        if !skip.contains(&key.as_ref()) {
            let val_str = match val {
                Value::Str(s) => s.to_string(),
                other => other.to_string(),
            };
            context_parts.push(format!("*{key}:* {val_str}"));
        }
    }

    let mut blocks = vec![
        serde_json::json!({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": format!("{severity_emoji} {rule}"),
                "emoji": true
            }
        }),
        serde_json::json!({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": format!("*Severity:*\n{}", severity)
                },
                {
                    "type": "mrkdwn",
                    "text": format!("*MITRE ATT&CK:*\n{}", if mitre.is_empty() { "N/A" } else { &mitre })
                }
            ]
        }),
    ];

    if !summary.is_empty() {
        blocks.push(serde_json::json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": summary
            }
        }));
    }

    if !context_parts.is_empty() {
        // Truncate to avoid Slack's 3000 char limit per block
        let context_text = context_parts.join(" | ");
        let truncated = if context_text.len() > 2900 {
            format!("{}...", &context_text[..2900])
        } else {
            context_text
        };
        blocks.push(serde_json::json!({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": truncated
            }]
        }));
    }

    serde_json::json!({ "blocks": blocks })
}

// ---------------------------------------------------------------------------
// Slack Webhook Sink
// ---------------------------------------------------------------------------

/// Slack webhook sink — sends alerts to Slack via incoming webhooks.
pub struct SlackWebhookSink {
    name: String,
    webhook_url: String,
    client: reqwest::Client,
    /// Rate limiter: Slack allows ~1 msg/sec per webhook.
    last_send: Mutex<Option<Instant>>,
    min_interval: Duration,
}

impl std::fmt::Debug for SlackWebhookSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlackWebhookSink")
            .field("name", &self.name)
            .field("webhook_url", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl SlackWebhookSink {
    /// Create a new Slack webhook sink.
    pub fn new(name: &str, webhook_url: &str) -> Self {
        Self {
            name: name.to_string(),
            webhook_url: webhook_url.to_string(),
            client: reqwest::Client::new(),
            last_send: Mutex::new(None),
            min_interval: Duration::from_secs(1),
        }
    }
}

#[async_trait]
impl SinkConnector for SlackWebhookSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        // Rate limiting: wait if too soon since last send
        {
            let mut last = self.last_send.lock().await;
            if let Some(prev) = *last {
                let elapsed = prev.elapsed();
                if elapsed < self.min_interval {
                    tokio::time::sleep(self.min_interval - elapsed).await;
                }
            }
            *last = Some(Instant::now());
        }

        let payload = event_to_slack_payload(event);

        let response = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(format!("Slack webhook POST: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            warn!("Slack webhook returned {}: {}", status, body);
            return Err(ConnectorError::SendFailed(format!(
                "Slack returned {status}: {body}"
            )));
        }

        Ok(())
    }

    async fn send_to_topic(
        &self,
        events: &[Arc<Event>],
        _topic: &str,
    ) -> Result<(), ConnectorError> {
        for event in events {
            self.send(event).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth::healthy(0)
    }
}

// ---------------------------------------------------------------------------
// Factory & Registration
// ---------------------------------------------------------------------------

static SLACK_CONFIG_PARAMS: &[ConfigParamInfo] = &[ConfigParamInfo {
    name: "webhook_url",
    description: "Slack incoming webhook URL",
    required: true,
    default_value: None,
}];

static SLACK_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "slack",
    display_name: "Slack",
    description: "Slack incoming webhook sink for security alerts",
    feature_flag: "",
    supports_source: false,
    supports_sink: true,
    supports_managed: false,
    config_params: SLACK_CONFIG_PARAMS,
};

struct SlackFactory;

impl ConnectorFactory for SlackFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &SLACK_INFO
    }

    fn create_managed(
        &self,
        _name: &str,
        _config: &ConnectorConfig,
    ) -> Result<Box<dyn varpulis_connector_api::ManagedConnector>, ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "slack managed connector not supported".to_string(),
        ))
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let url = config.url.clone();
        if url.is_empty() {
            return Err(ConnectorError::ConfigError(
                "webhook_url is required for Slack connector".to_string(),
            ));
        }
        Ok(Box::new(SlackWebhookSink::new("slack", &url)))
    }

    fn create_engine_sink(
        &self,
        name: &str,
        config: &ConnectorConfig,
        _topic_override: Option<&str>,
        _context_name: Option<&str>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        let connector = self.create_sink_connector(config)?;
        Ok(Arc::new(SinkConnectorAdapter::new(name, connector)))
    }
}

inventory::submit! { &SlackFactory as &dyn ConnectorFactory }

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use varpulis_core::event::Event;
    use varpulis_core::value::FxIndexMap;

    #[test]
    fn test_slack_payload_structure() {
        let mut data = FxIndexMap::default();
        data.insert("rule".into(), Value::str("full_killchain"));
        data.insert("mitre".into(), Value::str("T1059,T1003"));
        data.insert("severity".into(), Value::str("critical"));
        data.insert("summary".into(), Value::str("Kill chain detected"));
        data.insert("host".into(), Value::str("WS01"));

        let event = Event {
            event_type: "KillChainAlert".into(),
            timestamp: Utc::now(),
            data,
        };

        let payload = event_to_slack_payload(&event);
        let blocks = payload["blocks"].as_array().unwrap();

        // Header block
        let header = &blocks[0];
        assert_eq!(header["type"], "header");
        let header_text = header["text"]["text"].as_str().unwrap();
        assert!(header_text.contains("full_killchain"));
        assert!(header_text.contains(":red_circle:"));

        // Section with severity + MITRE
        let section = &blocks[1];
        assert_eq!(section["type"], "section");

        // Summary section
        let summary_block = &blocks[2];
        assert_eq!(summary_block["type"], "section");
        assert!(summary_block["text"]["text"]
            .as_str()
            .unwrap()
            .contains("Kill chain detected"));
    }

    #[test]
    fn test_severity_emojis() {
        let make_event = |sev: &str| -> Event {
            let mut data = FxIndexMap::default();
            data.insert("severity".into(), Value::str(sev));
            Event {
                event_type: "Alert".into(),
                timestamp: Utc::now(),
                data,
            }
        };

        let p = event_to_slack_payload(&make_event("critical"));
        assert!(p["blocks"][0]["text"]["text"]
            .as_str()
            .unwrap()
            .contains(":red_circle:"));

        let p = event_to_slack_payload(&make_event("low"));
        assert!(p["blocks"][0]["text"]["text"]
            .as_str()
            .unwrap()
            .contains(":large_blue_circle:"));
    }
}
