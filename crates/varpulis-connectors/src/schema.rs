//! JSON Schema generation for connector configuration types.
//!
//! Use [`generate_all_schemas()`] to produce a combined JSON Schema document
//! covering all connector configurations. This can be used for IDE
//! auto-completion and config validation.

/// Generate a combined JSON Schema covering all connector configuration types.
///
/// The returned schema uses `oneOf` to represent the choice between different
/// connector configurations (Kafka, MQTT, NATS, Redis, etc.).
pub fn generate_all_schemas() -> serde_json::Value {
    let configs: Vec<(&str, schemars::Schema)> = vec![
        (
            "ConnectorConfig",
            schemars::schema_for!(crate::ConnectorConfig),
        ),
        ("KafkaConfig", schemars::schema_for!(crate::KafkaConfig)),
        #[cfg(feature = "mqtt")]
        (
            "MqttConfig",
            schemars::schema_for!(varpulis_connector_mqtt::MqttConfig),
        ),
        ("NatsConfig", schemars::schema_for!(crate::NatsConfig)),
        ("RedisConfig", schemars::schema_for!(crate::RedisConfig)),
        (
            "RedisStreamConfig",
            schemars::schema_for!(crate::RedisStreamConfig),
        ),
        ("S3Config", schemars::schema_for!(crate::S3Config)),
        ("KinesisConfig", schemars::schema_for!(crate::KinesisConfig)),
        (
            "ElasticsearchConfig",
            schemars::schema_for!(crate::ElasticsearchConfig),
        ),
        ("PulsarConfig", schemars::schema_for!(crate::PulsarConfig)),
        (
            "DatabaseConfig",
            schemars::schema_for!(crate::DatabaseConfig),
        ),
        (
            "PostgresCdcConfig",
            schemars::schema_for!(crate::PostgresCdcConfig),
        ),
        ("RestApiConfig", schemars::schema_for!(crate::RestApiConfig)),
        (
            "HttpWebhookConfig",
            schemars::schema_for!(crate::HttpWebhookConfig),
        ),
    ];

    let mut schemas = serde_json::Map::new();
    for (name, schema) in configs {
        schemas.insert(name.to_string(), serde_json::to_value(&schema).unwrap());
    }

    serde_json::Value::Object(schemas)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_all_schemas_non_empty() {
        let schemas = generate_all_schemas();
        let obj = schemas.as_object().unwrap();
        // Count depends on enabled features; at minimum 10 without mqtt
        assert!(obj.len() >= 10, "Expected 10+ schemas, got {}", obj.len());
        assert!(obj.contains_key("KafkaConfig"));
        assert!(obj.contains_key("ConnectorConfig"));
        #[cfg(feature = "mqtt")]
        assert!(obj.contains_key("MqttConfig"));
    }
}
