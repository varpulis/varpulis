//! Cluster-level connector configuration and VPL injection.
//!
//! Named connectors (e.g., `mqtt_market`, `kafka_signals`) are stored at the
//! coordinator level and automatically injected into VPL source before parsing.

use crate::ClusterError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A cluster-level named connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConnector {
    pub name: String,
    pub connector_type: String,
    pub params: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ClusterConnector {
    /// Render this connector as a VPL `connector` declaration.
    ///
    /// Example output:
    /// ```text
    /// connector mqtt_market = mqtt(host: "localhost", port: 1883)
    /// ```
    pub fn to_vpl_declaration(&self) -> String {
        let params: Vec<String> = self
            .params
            .iter()
            .map(|(k, v)| {
                // Emit numeric values unquoted (VPL parser expects integer/float literals)
                if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
                    format!("{}: {}", k, v)
                } else {
                    format!("{}: \"{}\"", k, v)
                }
            })
            .collect();
        format!(
            "connector {} = {}({})",
            self.name,
            self.connector_type,
            params.join(", ")
        )
    }
}

/// Valid connector identifier: must match VPL identifier rules
/// (starts with letter or underscore, then letters/digits/underscores).
pub fn is_valid_connector_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Known connector types.
pub const VALID_CONNECTOR_TYPES: &[&str] = &["mqtt", "kafka", "nats", "http", "console"];

/// Check if a connector type is valid.
pub fn is_valid_connector_type(ct: &str) -> bool {
    VALID_CONNECTOR_TYPES.contains(&ct)
}

/// Validate required parameters for a given connector type.
pub fn validate_required_params(
    connector_type: &str,
    params: &HashMap<String, String>,
) -> Result<(), String> {
    match connector_type {
        "mqtt" => {
            if !params.contains_key("host") {
                return Err("mqtt connector requires 'host' parameter".to_string());
            }
        }
        "kafka" => {
            if !params.contains_key("brokers") {
                return Err("kafka connector requires 'brokers' parameter".to_string());
            }
        }
        "http" => {
            if !params.contains_key("url") {
                return Err("http connector requires 'url' parameter".to_string());
            }
        }
        "nats" => {
            if !params.contains_key("servers") {
                return Err("nats connector requires 'servers' parameter".to_string());
            }
        }
        "console" => {} // no required params
        _ => return Err(format!("Unknown connector type: {}", connector_type)),
    }
    Ok(())
}

/// Validate a connector definition.
pub fn validate_connector(connector: &ClusterConnector) -> Result<(), ClusterError> {
    if !is_valid_connector_name(&connector.name) {
        return Err(ClusterError::ConnectorValidation(format!(
            "Invalid connector name '{}': must start with a letter and contain only letters, digits, underscores, or hyphens",
            connector.name
        )));
    }
    if !is_valid_connector_type(&connector.connector_type) {
        return Err(ClusterError::ConnectorValidation(format!(
            "Invalid connector type '{}': must be one of {:?}",
            connector.connector_type, VALID_CONNECTOR_TYPES
        )));
    }
    validate_required_params(&connector.connector_type, &connector.params)
        .map_err(ClusterError::ConnectorValidation)?;
    Ok(())
}

/// Find connector names referenced in `.from(X, ...)` or `.to(X, ...)` that
/// are NOT already declared inline with `connector X = ...`.
pub fn find_missing_connectors(source: &str) -> Vec<String> {
    let mut declared: Vec<String> = Vec::new();
    let mut referenced: Vec<String> = Vec::new();

    for line in source.lines() {
        let trimmed = line.trim();

        // Detect inline connector declarations: `connector name = type(...)`
        if let Some(rest) = trimmed.strip_prefix("connector ") {
            if let Some(eq_pos) = rest.find('=') {
                let name = rest[..eq_pos].trim();
                if !name.is_empty() {
                    declared.push(name.to_string());
                }
            }
        }

        // Detect `.from(name` and `.to(name` references
        for pattern in &[".from(", ".to("] {
            let mut search_pos = 0;
            while let Some(pos) = trimmed[search_pos..].find(pattern) {
                let after = search_pos + pos + pattern.len();
                if after < trimmed.len() {
                    let rest = &trimmed[after..];
                    // Extract the connector name (VPL identifier: letter/underscore then alphanumeric/underscore)
                    let name: String = rest
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect();
                    if !name.is_empty()
                        && name.chars().next().unwrap().is_ascii_alphabetic()
                        && !referenced.contains(&name)
                    {
                        referenced.push(name);
                    }
                }
                search_pos = after;
            }
        }
    }

    // Return referenced names not declared inline
    referenced
        .into_iter()
        .filter(|name| !declared.contains(name))
        .collect()
}

/// Inject cluster connector declarations into VPL source for any connectors
/// that are referenced but not declared inline. Returns the enriched source
/// and the number of lines prepended.
///
/// When a connector has `client_id_mode = "append_pipeline"`, each `.from()`
/// or `.to()` reference gets a `client_id` parameter with the pipeline name
/// appended to the base client ID. The default mode is `"static"` (use the
/// configured client_id as-is).
pub fn inject_connectors(
    source: &str,
    connectors: &HashMap<String, ClusterConnector>,
) -> (String, usize) {
    let missing = find_missing_connectors(source);
    let mut preamble_lines: Vec<String> = Vec::new();

    for name in &missing {
        if let Some(connector) = connectors.get(name) {
            preamble_lines.push(connector.to_vpl_declaration());
        }
    }

    // Apply client_id_mode = "append_pipeline" rewriting
    let mut enriched_source = source.to_string();
    for connector in connectors.values() {
        if connector.params.get("client_id_mode").map(|s| s.as_str()) == Some("append_pipeline") {
            let base_id = connector
                .params
                .get("client_id")
                .cloned()
                .unwrap_or_else(|| connector.name.clone());
            enriched_source =
                append_pipeline_client_ids(&enriched_source, &connector.name, &base_id);
        }
    }

    if preamble_lines.is_empty() {
        return (enriched_source, 0);
    }

    let line_count = preamble_lines.len();
    // Add a blank line separator
    preamble_lines.push(String::new());
    let preamble = preamble_lines.join("\n");
    let result = format!("{}{}", preamble, enriched_source);
    (result, line_count + 1) // +1 for the blank line
}

/// For a given connector name, find `.from(name, ...)` references and inject
/// `client_id: "{base_id}-{pipeline_name}"` into the parameter list.
fn append_pipeline_client_ids(source: &str, connector_name: &str, base_id: &str) -> String {
    let mut result = String::with_capacity(source.len());

    for line in source.lines() {
        let trimmed = line.trim();
        // Extract pipeline name from "stream <name> = ..." lines
        let pipeline_name = if trimmed.starts_with("stream ") {
            trimmed
                .strip_prefix("stream ")
                .and_then(|rest| rest.split_whitespace().next())
        } else {
            None
        };

        if let Some(pname) = pipeline_name {
            // Check if this line has .from(connector_name, ...) or .to(connector_name, ...)
            let patterns = [
                format!(".from({},", connector_name),
                format!(".from({},", connector_name),
                format!(".to({},", connector_name),
            ];
            let mut modified = line.to_string();
            for pattern in &patterns {
                if modified.contains(pattern.as_str()) {
                    // Add client_id parameter after the connector name
                    let replacement = format!(
                        "{} client_id: \"{}-{}\"",
                        pattern.trim_end_matches(','),
                        base_id,
                        pname
                    );
                    // Insert client_id after connector_name in the param list
                    modified = modified.replacen(
                        pattern.as_str(),
                        &format!(
                            "{}, client_id: \"{}-{}\",",
                            pattern.trim_end_matches(','),
                            base_id,
                            pname
                        ),
                        1,
                    );
                    let _ = replacement; // suppress unused warning
                    break;
                }
            }
            result.push_str(&modified);
        } else {
            result.push_str(line);
        }
        result.push('\n');
    }

    // Remove trailing newline if source didn't have one
    if !source.ends_with('\n') {
        result.pop();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_connector_names() {
        assert!(is_valid_connector_name("mqtt1"));
        assert!(is_valid_connector_name("kafka_signals"));
        assert!(is_valid_connector_name("my_connector"));
        assert!(is_valid_connector_name("a"));
        assert!(is_valid_connector_name("_private"));
        assert!(!is_valid_connector_name(""));
        assert!(!is_valid_connector_name("1bad"));
        assert!(!is_valid_connector_name("-bad"));
        // Hyphens are NOT allowed (not valid VPL identifiers)
        assert!(!is_valid_connector_name("kafka-signals"));
    }

    #[test]
    fn test_valid_connector_types() {
        assert!(is_valid_connector_type("mqtt"));
        assert!(is_valid_connector_type("kafka"));
        assert!(is_valid_connector_type("nats"));
        assert!(is_valid_connector_type("http"));
        assert!(is_valid_connector_type("console"));
        assert!(!is_valid_connector_type("redis"));
        assert!(!is_valid_connector_type(""));
    }

    #[test]
    fn test_validate_required_params() {
        let mut params = HashMap::new();
        assert!(validate_required_params("mqtt", &params).is_err());
        params.insert("host".to_string(), "localhost".to_string());
        assert!(validate_required_params("mqtt", &params).is_ok());

        let mut params = HashMap::new();
        assert!(validate_required_params("kafka", &params).is_err());
        params.insert("brokers".to_string(), "localhost:9092".to_string());
        assert!(validate_required_params("kafka", &params).is_ok());

        let mut params = HashMap::new();
        assert!(validate_required_params("http", &params).is_err());
        params.insert("url".to_string(), "http://example.com".to_string());
        assert!(validate_required_params("http", &params).is_ok());

        let mut params = HashMap::new();
        assert!(validate_required_params("nats", &params).is_err());
        params.insert("servers".to_string(), "nats://localhost:4222".to_string());
        assert!(validate_required_params("nats", &params).is_ok());

        assert!(validate_required_params("console", &HashMap::new()).is_ok());
        assert!(validate_required_params("unknown", &HashMap::new()).is_err());
    }

    #[test]
    fn test_to_vpl_declaration() {
        let conn = ClusterConnector {
            name: "mqtt_market".to_string(),
            connector_type: "mqtt".to_string(),
            params: {
                let mut m = HashMap::new();
                m.insert("host".to_string(), "localhost".to_string());
                m.insert("port".to_string(), "1883".to_string());
                m
            },
            description: None,
        };
        let decl = conn.to_vpl_declaration();
        assert!(decl.starts_with("connector mqtt_market = mqtt("));
        assert!(decl.contains("host: \"localhost\""));
        // Numeric values should be unquoted
        assert!(decl.contains("port: 1883"));
        assert!(!decl.contains("port: \"1883\""));
    }

    #[test]
    fn test_find_missing_connectors() {
        let source = r#"
event Tick:
    price: float

stream Data = Tick.from(mqtt_input, topic: "data")
stream Out = Data.to(kafka_out, topic: "results")
"#;
        let missing = find_missing_connectors(source);
        assert!(missing.contains(&"mqtt_input".to_string()));
        assert!(missing.contains(&"kafka_out".to_string()));
    }

    #[test]
    fn test_find_missing_connectors_with_inline_decl() {
        let source = r#"
connector mqtt_input = mqtt(host: "localhost")

event Tick:
    price: float

stream Data = Tick.from(mqtt_input, topic: "data")
stream Out = Data.to(kafka_out, topic: "results")
"#;
        let missing = find_missing_connectors(source);
        assert!(!missing.contains(&"mqtt_input".to_string()));
        assert!(missing.contains(&"kafka_out".to_string()));
    }

    #[test]
    fn test_inject_connectors() {
        let source = "stream Data = Tick.from(mqtt_in, topic: \"data\")\n";
        let mut connectors = HashMap::new();
        connectors.insert(
            "mqtt_in".to_string(),
            ClusterConnector {
                name: "mqtt_in".to_string(),
                connector_type: "mqtt".to_string(),
                params: {
                    let mut m = HashMap::new();
                    m.insert("host".to_string(), "localhost".to_string());
                    m
                },
                description: None,
            },
        );

        let (enriched, lines) = inject_connectors(source, &connectors);
        assert!(enriched.starts_with("connector mqtt_in = mqtt("));
        assert!(enriched.contains(source));
        assert!(lines > 0);
    }

    #[test]
    fn test_inject_connectors_none_missing() {
        let source = "connector foo = mqtt(host: \"x\")\nstream D = T.from(foo)\n";
        let connectors = HashMap::new();
        let (enriched, lines) = inject_connectors(source, &connectors);
        assert_eq!(enriched, source);
        assert_eq!(lines, 0);
    }

    #[test]
    fn test_client_id_mode_append_pipeline() {
        let source = "stream MarketData = Tick.from(mqtt_in, topic: \"ticks\")\n";
        let mut connectors = HashMap::new();
        connectors.insert(
            "mqtt_in".to_string(),
            ClusterConnector {
                name: "mqtt_in".to_string(),
                connector_type: "mqtt".to_string(),
                params: {
                    let mut m = HashMap::new();
                    m.insert("host".to_string(), "localhost".to_string());
                    m.insert("client_id".to_string(), "device-001".to_string());
                    m.insert("client_id_mode".to_string(), "append_pipeline".to_string());
                    m
                },
                description: None,
            },
        );

        let (enriched, _) = inject_connectors(source, &connectors);
        // Should inject client_id with pipeline name appended
        assert!(
            enriched.contains("client_id: \"device-001-MarketData\""),
            "Expected client_id with pipeline name, got: {}",
            enriched
        );
    }

    #[test]
    fn test_client_id_mode_static_no_change() {
        let source = "stream Data = Tick.from(mqtt_in, topic: \"ticks\")\n";
        let mut connectors = HashMap::new();
        connectors.insert(
            "mqtt_in".to_string(),
            ClusterConnector {
                name: "mqtt_in".to_string(),
                connector_type: "mqtt".to_string(),
                params: {
                    let mut m = HashMap::new();
                    m.insert("host".to_string(), "localhost".to_string());
                    m.insert("client_id".to_string(), "device-001".to_string());
                    // static mode (default) — no client_id_mode param
                    m
                },
                description: None,
            },
        );

        let (enriched, _) = inject_connectors(source, &connectors);
        // Should NOT inject client_id into the from() call
        assert!(
            !enriched.contains("client_id: \"device-001-Data\""),
            "Static mode should not append pipeline name, got: {}",
            enriched
        );
    }
}
