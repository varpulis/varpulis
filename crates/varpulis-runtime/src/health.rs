//! Health monitoring for Varpulis components
//!
//! Provides per-component health reporting and system-level health aggregation.
//! Components implement [`HealthReporter`] to participate in health checks.
//!
//! ## Health Probes
//!
//! - **Liveness** (`/health/live`): always 200 — process is alive
//! - **Readiness** (`/health/ready`): 200 when all components are healthy
//! - **Startup** (`/health/started`): 200 once the engine has started
//! - **Detailed** (`/health`): full JSON with per-component status

use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Health status of a component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Up,
    Degraded,
    Down,
}

impl HealthStatus {
    /// Returns the worst (most severe) of two statuses.
    pub fn worst(self, other: Self) -> Self {
        match (self, other) {
            (Self::Down, _) | (_, Self::Down) => Self::Down,
            (Self::Degraded, _) | (_, Self::Degraded) => Self::Degraded,
            _ => Self::Up,
        }
    }
}

/// Type of component being monitored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ComponentType {
    Connector,
    Engine,
    Actor,
    Worker,
}

/// Health information for a single component.
#[derive(Debug, Clone, Serialize)]
pub struct ComponentHealth {
    pub name: String,
    pub component_type: ComponentType,
    pub status: HealthStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Trait for components that can report their health.
pub trait HealthReporter: Send + Sync {
    fn health(&self) -> ComponentHealth;
}

/// System-wide health summary.
#[derive(Debug, Clone, Serialize)]
pub struct SystemHealth {
    /// Aggregate status (worst-component-wins).
    pub status: HealthStatus,
    /// Per-component health details.
    pub components: Vec<ComponentHealth>,
    /// ISO 8601 timestamp of this health check.
    pub timestamp: String,
}

/// Central registry of health reporters.
pub struct HealthRegistry {
    reporters: Vec<Arc<dyn HealthReporter>>,
    started: bool,
    ready: Arc<AtomicBool>,
}

impl HealthRegistry {
    pub fn new() -> Self {
        Self {
            reporters: Vec::new(),
            started: false,
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Register a component for health monitoring.
    pub fn register(&mut self, reporter: Arc<dyn HealthReporter>) {
        self.reporters.push(reporter);
    }

    /// Mark the system as started.
    pub fn mark_started(&mut self) {
        self.started = true;
    }

    /// Mark the system as ready to accept traffic.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    /// Check all components and produce a system health summary.
    pub fn system_health(&self) -> SystemHealth {
        let components: Vec<ComponentHealth> = self.reporters.iter().map(|r| r.health()).collect();

        let status = components
            .iter()
            .fold(HealthStatus::Up, |acc, c| acc.worst(c.status));

        SystemHealth {
            status,
            components,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Liveness check — always true if the process is running.
    pub fn is_live(&self) -> bool {
        true
    }

    /// Readiness check — true when all components are healthy.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Startup check — true once [`mark_started`] has been called.
    pub fn is_started(&self) -> bool {
        self.started
    }
}

impl Default for HealthRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Adapter that converts a [`ConnectorHealthReport`](crate::connector::ConnectorHealthReport)
/// into the health monitoring system.
pub struct ConnectorHealthAdapter {
    name: String,
    health_fn: Box<dyn Fn() -> crate::connector::ConnectorHealthReport + Send + Sync>,
}

impl ConnectorHealthAdapter {
    pub fn new<F>(name: String, health_fn: F) -> Self
    where
        F: Fn() -> crate::connector::ConnectorHealthReport + Send + Sync + 'static,
    {
        Self {
            name,
            health_fn: Box::new(health_fn),
        }
    }
}

impl HealthReporter for ConnectorHealthAdapter {
    fn health(&self) -> ComponentHealth {
        let report = (self.health_fn)();
        let status = if report.connected && report.circuit_breaker_state == "closed" {
            HealthStatus::Up
        } else if report.circuit_breaker_state == "half_open" {
            HealthStatus::Degraded
        } else {
            HealthStatus::Down
        };

        ComponentHealth {
            name: self.name.clone(),
            component_type: ComponentType::Connector,
            status,
            reason: report.last_error,
            details: Some(serde_json::json!({
                "messages_received": report.messages_received,
                "seconds_since_last_message": report.seconds_since_last_message,
                "circuit_breaker_state": report.circuit_breaker_state,
                "circuit_breaker_failures": report.circuit_breaker_failures,
            })),
        }
    }
}

/// Simple health reporter for the Engine component.
pub struct EngineHealthReporter {
    name: String,
    streams_loaded: Arc<AtomicBool>,
}

impl EngineHealthReporter {
    pub fn new(name: String) -> Self {
        Self {
            name,
            streams_loaded: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Mark that streams have been loaded.
    pub fn set_streams_loaded(&self) {
        self.streams_loaded.store(true, Ordering::SeqCst);
    }

    /// Get a shareable reference to the streams_loaded flag.
    pub fn streams_loaded_flag(&self) -> Arc<AtomicBool> {
        self.streams_loaded.clone()
    }
}

impl HealthReporter for EngineHealthReporter {
    fn health(&self) -> ComponentHealth {
        let loaded = self.streams_loaded.load(Ordering::SeqCst);
        ComponentHealth {
            name: self.name.clone(),
            component_type: ComponentType::Engine,
            status: if loaded {
                HealthStatus::Up
            } else {
                HealthStatus::Down
            },
            reason: if loaded {
                None
            } else {
                Some("streams not loaded".into())
            },
            details: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AlwaysUp;
    impl HealthReporter for AlwaysUp {
        fn health(&self) -> ComponentHealth {
            ComponentHealth {
                name: "test".into(),
                component_type: ComponentType::Engine,
                status: HealthStatus::Up,
                reason: None,
                details: None,
            }
        }
    }

    struct AlwaysDown;
    impl HealthReporter for AlwaysDown {
        fn health(&self) -> ComponentHealth {
            ComponentHealth {
                name: "broken".into(),
                component_type: ComponentType::Connector,
                status: HealthStatus::Down,
                reason: Some("connection lost".into()),
                details: None,
            }
        }
    }

    #[test]
    fn test_health_status_worst() {
        assert_eq!(HealthStatus::Up.worst(HealthStatus::Up), HealthStatus::Up);
        assert_eq!(
            HealthStatus::Up.worst(HealthStatus::Degraded),
            HealthStatus::Degraded
        );
        assert_eq!(
            HealthStatus::Degraded.worst(HealthStatus::Down),
            HealthStatus::Down
        );
        assert_eq!(
            HealthStatus::Down.worst(HealthStatus::Up),
            HealthStatus::Down
        );
    }

    #[test]
    fn test_registry_empty() {
        let registry = HealthRegistry::new();
        let health = registry.system_health();
        assert_eq!(health.status, HealthStatus::Up);
        assert!(health.components.is_empty());
    }

    #[test]
    fn test_registry_all_up() {
        let mut registry = HealthRegistry::new();
        registry.register(Arc::new(AlwaysUp));
        registry.register(Arc::new(AlwaysUp));
        let health = registry.system_health();
        assert_eq!(health.status, HealthStatus::Up);
        assert_eq!(health.components.len(), 2);
    }

    #[test]
    fn test_registry_worst_wins() {
        let mut registry = HealthRegistry::new();
        registry.register(Arc::new(AlwaysUp));
        registry.register(Arc::new(AlwaysDown));
        let health = registry.system_health();
        assert_eq!(health.status, HealthStatus::Down);
    }

    #[test]
    fn test_liveness_always_true() {
        let registry = HealthRegistry::new();
        assert!(registry.is_live());
    }

    #[test]
    fn test_readiness_and_startup() {
        let mut registry = HealthRegistry::new();
        assert!(!registry.is_ready());
        assert!(!registry.is_started());

        registry.mark_started();
        assert!(registry.is_started());
        assert!(!registry.is_ready());

        registry.mark_ready();
        assert!(registry.is_ready());
    }

    #[test]
    fn test_engine_health_reporter() {
        let reporter = EngineHealthReporter::new("engine-0".into());
        let h = reporter.health();
        assert_eq!(h.status, HealthStatus::Down);
        assert!(h.reason.is_some());

        reporter.set_streams_loaded();
        let h = reporter.health();
        assert_eq!(h.status, HealthStatus::Up);
        assert!(h.reason.is_none());
    }

    #[test]
    fn test_connector_health_adapter() {
        let adapter = ConnectorHealthAdapter::new("mqtt-0".into(), || {
            crate::connector::ConnectorHealthReport {
                connected: true,
                circuit_breaker_state: "closed".to_string(),
                messages_received: 42,
                ..Default::default()
            }
        });
        let h = adapter.health();
        assert_eq!(h.status, HealthStatus::Up);
        assert_eq!(h.name, "mqtt-0");

        let degraded = ConnectorHealthAdapter::new("mqtt-1".into(), || {
            crate::connector::ConnectorHealthReport {
                connected: true,
                circuit_breaker_state: "half_open".to_string(),
                ..Default::default()
            }
        });
        assert_eq!(degraded.health().status, HealthStatus::Degraded);

        let down = ConnectorHealthAdapter::new("mqtt-2".into(), || {
            crate::connector::ConnectorHealthReport {
                connected: false,
                circuit_breaker_state: "open".to_string(),
                ..Default::default()
            }
        });
        assert_eq!(down.health().status, HealthStatus::Down);
    }

    #[test]
    fn test_system_health_serializes() {
        let mut registry = HealthRegistry::new();
        registry.register(Arc::new(AlwaysUp));
        let health = registry.system_health();
        let json = serde_json::to_string(&health).unwrap();
        assert!(json.contains("\"status\":\"up\""));
        assert!(json.contains("\"timestamp\""));
    }
}
