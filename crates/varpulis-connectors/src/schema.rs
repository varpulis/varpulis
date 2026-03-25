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
    let mut configs: Vec<(&str, schemars::Schema)> = vec![
        (
            "ConnectorConfig",
            schemars::schema_for!(crate::ConnectorConfig),
        ),
        ("RestApiConfig", schemars::schema_for!(crate::RestApiConfig)),
        (
            "HttpWebhookConfig",
            schemars::schema_for!(crate::HttpWebhookConfig),
        ),
    ];

    #[cfg(feature = "kafka")]
    configs.push(("KafkaConfig", schemars::schema_for!(crate::KafkaConfig)));

    #[cfg(feature = "mqtt")]
    configs.push((
        "MqttConfig",
        schemars::schema_for!(varpulis_connector_mqtt::MqttConfig),
    ));

    #[cfg(feature = "nats")]
    configs.push(("NatsConfig", schemars::schema_for!(crate::NatsConfig)));

    #[cfg(feature = "redis")]
    {
        configs.push(("RedisConfig", schemars::schema_for!(crate::RedisConfig)));
        configs.push((
            "RedisStreamConfig",
            schemars::schema_for!(crate::RedisStreamConfig),
        ));
    }

    #[cfg(feature = "s3")]
    configs.push(("S3Config", schemars::schema_for!(crate::S3Config)));

    #[cfg(feature = "kinesis")]
    configs.push(("KinesisConfig", schemars::schema_for!(crate::KinesisConfig)));

    #[cfg(feature = "elasticsearch")]
    configs.push((
        "ElasticsearchConfig",
        schemars::schema_for!(crate::ElasticsearchConfig),
    ));

    #[cfg(feature = "pulsar")]
    configs.push(("PulsarConfig", schemars::schema_for!(crate::PulsarConfig)));

    #[cfg(feature = "database")]
    configs.push((
        "DatabaseConfig",
        schemars::schema_for!(crate::DatabaseConfig),
    ));

    #[cfg(feature = "cdc")]
    configs.push((
        "PostgresCdcConfig",
        schemars::schema_for!(crate::PostgresCdcConfig),
    ));

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
        // Minimum schemas: ConnectorConfig, RestApiConfig, HttpWebhookConfig = 3
        assert!(obj.len() >= 3, "Expected 3+ schemas, got {}", obj.len());
        assert!(obj.contains_key("ConnectorConfig"));
    }
}
