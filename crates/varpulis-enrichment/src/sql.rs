//! SQL enrichment provider — executes parameterized queries and maps rows to fields.

use std::collections::HashMap;

use async_trait::async_trait;
use varpulis_core::Value;

use crate::{EnrichmentError, EnrichmentProvider, EnrichmentResult};

/// SQL-based enrichment provider.
///
/// Executes a parameterized query (`$1` placeholder) and maps the first result
/// row to the requested fields.
pub struct SqlEnrichmentProvider {
    url: String,
    query: String,
}

impl SqlEnrichmentProvider {
    pub fn new(config: &varpulis_connectors::ConnectorConfig) -> Result<Self, String> {
        let query = config.properties.get("query").cloned().ok_or_else(|| {
            "database connector for .enrich() requires a 'query' property".to_string()
        })?;
        Ok(Self {
            url: config.url.clone(),
            query,
        })
    }
}

#[async_trait]
impl EnrichmentProvider for SqlEnrichmentProvider {
    async fn lookup(
        &self,
        key: &Value,
        fields: &[String],
    ) -> Result<EnrichmentResult, EnrichmentError> {
        use sqlx::Row;

        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(5)
            .connect(&self.url)
            .await
            .map_err(|e| EnrichmentError::Connection(e.to_string()))?;

        let key_str = match key {
            Value::Str(s) => s.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            other => format!("{:?}", other),
        };

        let row = sqlx::query(&self.query)
            .bind(&key_str)
            .fetch_optional(&pool)
            .await
            .map_err(|e| EnrichmentError::Connection(e.to_string()))?;

        let Some(row) = row else {
            return Err(EnrichmentError::NotFound(key_str));
        };

        let mut result_fields = HashMap::new();
        for field in fields {
            // Try to get value as string (most compatible)
            if let Ok(val) = row.try_get::<String, _>(field.as_str()) {
                result_fields.insert(field.clone(), Value::Str(val.into_boxed_str()));
            } else if let Ok(val) = row.try_get::<i64, _>(field.as_str()) {
                result_fields.insert(field.clone(), Value::Int(val));
            } else if let Ok(val) = row.try_get::<f64, _>(field.as_str()) {
                result_fields.insert(field.clone(), Value::Float(val));
            } else if let Ok(val) = row.try_get::<bool, _>(field.as_str()) {
                result_fields.insert(field.clone(), Value::Bool(val));
            }
        }

        Ok(EnrichmentResult {
            fields: result_fields,
            cached: false,
        })
    }

    fn provider_name(&self) -> &str {
        "database"
    }
}
