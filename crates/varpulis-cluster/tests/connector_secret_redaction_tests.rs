//! Connector credentials must not leave the cluster through the read API.
//!
//! `ClusterConnector.params` is a flat map carrying Kafka SASL passwords, MQTT
//! and NATS passwords, database passwords and Slack webhook URLs. It derives
//! `Serialize`, and `GET /api/v1/cluster/connectors` returned it verbatim
//! behind `RbacViewer` — the lowest role there is. A read-only monitoring key
//! was therefore an upstream compromise in one request.
//!
//! The redaction had been applied to `Debug` only, which protects logs and
//! leaves the API wide open, because it is `Serialize` that feeds the API.

use std::collections::HashMap;

use varpulis_cluster::connector_config::{ClusterConnector, ClusterConnectorView};

fn connector_with_secrets() -> ClusterConnector {
    let mut params = HashMap::new();
    // Secrets, in the shapes the connectors actually use.
    params.insert("sasl_password".to_string(), "s3cr3t-sasl".to_string());
    params.insert("password".to_string(), "s3cr3t-plain".to_string());
    params.insert("api_key".to_string(), "s3cr3t-apikey".to_string());
    params.insert(
        "webhook_url".to_string(),
        "https://hooks.slack.com/services/T0/B0/s3cr3t-hook".to_string(),
    );
    params.insert(
        "connection_string".to_string(),
        "postgres://u:s3cr3t-dsn@db/app".to_string(),
    );
    // Not secrets: an operator must still be able to diagnose these.
    params.insert("bootstrap_servers".to_string(), "kafka:9092".to_string());
    params.insert("topic".to_string(), "alerts".to_string());
    params.insert("ssl_ca_location".to_string(), "/etc/ssl/ca.pem".to_string());

    ClusterConnector {
        name: "upstream".to_string(),
        connector_type: "kafka".to_string(),
        params,
        description: None,
    }
}

/// Fail-before: the raw struct serialises every value, so this finds all five.
#[test]
fn the_read_view_carries_no_secret_values() {
    let view = ClusterConnectorView::from(&connector_with_secrets());
    let json = serde_json::to_string(&view).expect("view must serialise");

    for secret in [
        "s3cr3t-sasl",
        "s3cr3t-plain",
        "s3cr3t-apikey",
        "s3cr3t-hook",
        "s3cr3t-dsn",
    ] {
        assert!(
            !json.contains(secret),
            "the read view leaked `{secret}`:\n{json}"
        );
    }
}

/// Withholding a value must not hide that it is configured, and must not
/// swallow the settings an operator needs to debug a broken connector.
#[test]
fn the_read_view_keeps_what_is_not_a_secret() {
    let view = ClusterConnectorView::from(&connector_with_secrets());

    assert_eq!(
        view.params.get("bootstrap_servers").map(String::as_str),
        Some("kafka:9092")
    );
    assert_eq!(view.params.get("topic").map(String::as_str), Some("alerts"));
    assert_eq!(
        view.params.get("ssl_ca_location").map(String::as_str),
        Some("/etc/ssl/ca.pem"),
        "a CA path is not a secret; redacting it makes a misconfiguration \
         impossible to diagnose"
    );

    assert_eq!(
        view.redacted_params,
        vec![
            "api_key".to_string(),
            "connection_string".to_string(),
            "password".to_string(),
            "sasl_password".to_string(),
            "webhook_url".to_string(),
        ],
        "an operator must still see WHICH secrets are set"
    );
}

/// The raw type is what the redaction protects against — this pins why the
/// view exists, so nobody swaps the handler back to serialising it directly.
#[test]
fn the_raw_type_would_still_leak_and_that_is_the_point() {
    let raw = serde_json::to_string(&connector_with_secrets()).expect("serialise");
    assert!(
        raw.contains("s3cr3t-sasl"),
        "if this ever stops leaking, ClusterConnector itself was hardened and \
         ClusterConnectorView may be redundant"
    );
}
