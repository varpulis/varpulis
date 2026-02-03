//! Multi-tenant isolation for SaaS deployment
//!
//! Provides tenant-scoped engine instances with resource limits and usage tracking.

use crate::engine::{Alert, Engine};
use crate::event::Event;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

/// Unique identifier for a tenant
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TenantId(pub String);

impl TenantId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Resource limits for a tenant
#[derive(Debug, Clone)]
pub struct TenantQuota {
    /// Maximum number of pipelines
    pub max_pipelines: usize,
    /// Maximum events per second
    pub max_events_per_second: u64,
    /// Maximum streams per pipeline
    pub max_streams_per_pipeline: usize,
}

impl Default for TenantQuota {
    fn default() -> Self {
        Self {
            max_pipelines: 10,
            max_events_per_second: 10_000,
            max_streams_per_pipeline: 50,
        }
    }
}

impl TenantQuota {
    /// Free tier limits
    pub fn free() -> Self {
        Self {
            max_pipelines: 2,
            max_events_per_second: 100,
            max_streams_per_pipeline: 5,
        }
    }

    /// Pro tier limits
    pub fn pro() -> Self {
        Self {
            max_pipelines: 20,
            max_events_per_second: 50_000,
            max_streams_per_pipeline: 100,
        }
    }

    /// Enterprise tier limits
    pub fn enterprise() -> Self {
        Self {
            max_pipelines: 1000,
            max_events_per_second: 500_000,
            max_streams_per_pipeline: 500,
        }
    }
}

/// Usage statistics for a tenant
#[derive(Debug, Clone, Default)]
pub struct TenantUsage {
    /// Total events processed
    pub events_processed: u64,
    /// Events processed in current window (for rate limiting)
    pub events_in_window: u64,
    /// Window start time
    pub window_start: Option<Instant>,
    /// Number of active pipelines
    pub active_pipelines: usize,
    /// Total alerts generated
    pub alerts_generated: u64,
}

impl TenantUsage {
    /// Record an event and check rate limit. Returns true if within quota.
    pub fn record_event(&mut self, max_eps: u64) -> bool {
        self.events_processed += 1;

        let now = Instant::now();
        match self.window_start {
            Some(start) if now.duration_since(start).as_secs() < 1 => {
                self.events_in_window += 1;
                if max_eps > 0 && self.events_in_window > max_eps {
                    return false;
                }
            }
            _ => {
                self.window_start = Some(now);
                self.events_in_window = 1;
            }
        }
        true
    }

    pub fn record_alert(&mut self) {
        self.alerts_generated += 1;
    }
}

/// A pipeline deployed by a tenant
pub struct Pipeline {
    /// Pipeline identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// The VPL source code
    pub source: String,
    /// The engine running this pipeline
    pub engine: Engine,
    /// Alert receiver for this pipeline
    pub alert_rx: mpsc::Receiver<Alert>,
    /// When the pipeline was created
    pub created_at: Instant,
    /// Pipeline status
    pub status: PipelineStatus,
}

/// Pipeline status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineStatus {
    Running,
    Stopped,
    Error(String),
}

impl std::fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineStatus::Running => write!(f, "running"),
            PipelineStatus::Stopped => write!(f, "stopped"),
            PipelineStatus::Error(msg) => write!(f, "error: {msg}"),
        }
    }
}

/// Errors from tenant operations
#[derive(Debug)]
pub enum TenantError {
    /// Tenant not found
    NotFound(String),
    /// Pipeline not found
    PipelineNotFound(String),
    /// Quota exceeded
    QuotaExceeded(String),
    /// Rate limit exceeded
    RateLimitExceeded,
    /// Parse error in VPL source
    ParseError(String),
    /// Engine error
    EngineError(String),
    /// Tenant already exists
    AlreadyExists(String),
}

impl std::fmt::Display for TenantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TenantError::NotFound(id) => write!(f, "tenant not found: {id}"),
            TenantError::PipelineNotFound(id) => write!(f, "pipeline not found: {id}"),
            TenantError::QuotaExceeded(msg) => write!(f, "quota exceeded: {msg}"),
            TenantError::RateLimitExceeded => write!(f, "rate limit exceeded"),
            TenantError::ParseError(msg) => write!(f, "parse error: {msg}"),
            TenantError::EngineError(msg) => write!(f, "engine error: {msg}"),
            TenantError::AlreadyExists(id) => write!(f, "tenant already exists: {id}"),
        }
    }
}

impl std::error::Error for TenantError {}

/// A single tenant with its pipelines and quotas
pub struct Tenant {
    /// Tenant identifier
    pub id: TenantId,
    /// Display name
    pub name: String,
    /// API key for authentication
    pub api_key: String,
    /// Resource quotas
    pub quota: TenantQuota,
    /// Usage statistics
    pub usage: TenantUsage,
    /// Active pipelines
    pub pipelines: HashMap<String, Pipeline>,
    /// When the tenant was created
    pub created_at: Instant,
}

impl Tenant {
    pub fn new(id: TenantId, name: String, api_key: String, quota: TenantQuota) -> Self {
        Self {
            id,
            name,
            api_key,
            quota,
            usage: TenantUsage::default(),
            pipelines: HashMap::new(),
            created_at: Instant::now(),
        }
    }

    /// Deploy a new pipeline from VPL source code
    pub fn deploy_pipeline(&mut self, name: String, source: String) -> Result<String, TenantError> {
        // Check pipeline quota
        if self.pipelines.len() >= self.quota.max_pipelines {
            return Err(TenantError::QuotaExceeded(format!(
                "max pipelines ({}) reached",
                self.quota.max_pipelines
            )));
        }

        // Parse the VPL source
        let program =
            varpulis_parser::parse(&source).map_err(|e| TenantError::ParseError(e.to_string()))?;

        // Check streams quota
        let stream_count = program
            .statements
            .iter()
            .filter(|s| matches!(&s.node, varpulis_core::ast::Stmt::StreamDecl { .. }))
            .count();
        if stream_count > self.quota.max_streams_per_pipeline {
            return Err(TenantError::QuotaExceeded(format!(
                "pipeline has {} streams, max is {}",
                stream_count, self.quota.max_streams_per_pipeline
            )));
        }

        // Create a new engine
        let (alert_tx, alert_rx) = mpsc::channel(1000);
        let mut engine = Engine::new(alert_tx);
        engine
            .load(&program)
            .map_err(|e| TenantError::EngineError(e.to_string()))?;

        let id = Uuid::new_v4().to_string();
        let pipeline = Pipeline {
            id: id.clone(),
            name,
            source,
            engine,
            alert_rx,
            created_at: Instant::now(),
            status: PipelineStatus::Running,
        };

        self.pipelines.insert(id.clone(), pipeline);
        self.usage.active_pipelines = self.pipelines.len();

        Ok(id)
    }

    /// Remove a pipeline
    pub fn remove_pipeline(&mut self, pipeline_id: &str) -> Result<(), TenantError> {
        self.pipelines
            .remove(pipeline_id)
            .ok_or_else(|| TenantError::PipelineNotFound(pipeline_id.to_string()))?;
        self.usage.active_pipelines = self.pipelines.len();
        Ok(())
    }

    /// Process an event through a specific pipeline
    pub async fn process_event(
        &mut self,
        pipeline_id: &str,
        event: Event,
    ) -> Result<(), TenantError> {
        // Check rate limit
        if !self.usage.record_event(self.quota.max_events_per_second) {
            return Err(TenantError::RateLimitExceeded);
        }

        let pipeline = self
            .pipelines
            .get_mut(pipeline_id)
            .ok_or_else(|| TenantError::PipelineNotFound(pipeline_id.to_string()))?;

        if pipeline.status != PipelineStatus::Running {
            return Err(TenantError::EngineError(format!(
                "pipeline is {}",
                pipeline.status
            )));
        }

        pipeline
            .engine
            .process(event)
            .await
            .map_err(|e| TenantError::EngineError(e.to_string()))?;

        Ok(())
    }

    /// Reload a pipeline with new VPL source
    pub fn reload_pipeline(
        &mut self,
        pipeline_id: &str,
        source: String,
    ) -> Result<(), TenantError> {
        let program =
            varpulis_parser::parse(&source).map_err(|e| TenantError::ParseError(e.to_string()))?;

        let pipeline = self
            .pipelines
            .get_mut(pipeline_id)
            .ok_or_else(|| TenantError::PipelineNotFound(pipeline_id.to_string()))?;

        pipeline
            .engine
            .reload(&program)
            .map_err(|e| TenantError::EngineError(e.to_string()))?;
        pipeline.source = source;

        Ok(())
    }
}

/// Manages all tenants in the system
pub struct TenantManager {
    tenants: HashMap<TenantId, Tenant>,
    /// API key → TenantId lookup
    api_key_index: HashMap<String, TenantId>,
}

impl TenantManager {
    pub fn new() -> Self {
        Self {
            tenants: HashMap::new(),
            api_key_index: HashMap::new(),
        }
    }

    /// Create a new tenant
    pub fn create_tenant(
        &mut self,
        name: String,
        api_key: String,
        quota: TenantQuota,
    ) -> Result<TenantId, TenantError> {
        if self.api_key_index.contains_key(&api_key) {
            return Err(TenantError::AlreadyExists(
                "API key already in use".to_string(),
            ));
        }

        let id = TenantId::generate();
        let tenant = Tenant::new(id.clone(), name, api_key.clone(), quota);
        self.tenants.insert(id.clone(), tenant);
        self.api_key_index.insert(api_key, id.clone());
        Ok(id)
    }

    /// Get a tenant by API key
    pub fn get_tenant_by_api_key(&self, api_key: &str) -> Option<&TenantId> {
        self.api_key_index.get(api_key)
    }

    /// Get a tenant by ID
    pub fn get_tenant(&self, id: &TenantId) -> Option<&Tenant> {
        self.tenants.get(id)
    }

    /// Get a mutable tenant by ID
    pub fn get_tenant_mut(&mut self, id: &TenantId) -> Option<&mut Tenant> {
        self.tenants.get_mut(id)
    }

    /// Remove a tenant and all its pipelines
    pub fn remove_tenant(&mut self, id: &TenantId) -> Result<(), TenantError> {
        let tenant = self
            .tenants
            .remove(id)
            .ok_or_else(|| TenantError::NotFound(id.to_string()))?;
        self.api_key_index.remove(&tenant.api_key);
        Ok(())
    }

    /// List all tenants
    pub fn list_tenants(&self) -> Vec<&Tenant> {
        self.tenants.values().collect()
    }

    /// Get total number of tenants
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe tenant manager for use in async server context
pub type SharedTenantManager = Arc<RwLock<TenantManager>>;

/// Create a new shared tenant manager
pub fn shared_tenant_manager() -> SharedTenantManager {
    Arc::new(RwLock::new(TenantManager::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id_generate() {
        let id1 = TenantId::generate();
        let id2 = TenantId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_tenant_id_display() {
        let id = TenantId::new("test-123");
        assert_eq!(format!("{id}"), "test-123");
        assert_eq!(id.as_str(), "test-123");
    }

    #[test]
    fn test_tenant_quota_tiers() {
        let free = TenantQuota::free();
        let pro = TenantQuota::pro();
        let enterprise = TenantQuota::enterprise();

        assert!(free.max_pipelines < pro.max_pipelines);
        assert!(pro.max_pipelines < enterprise.max_pipelines);
        assert!(free.max_events_per_second < pro.max_events_per_second);
        assert!(pro.max_events_per_second < enterprise.max_events_per_second);
    }

    #[test]
    fn test_usage_record_event() {
        let mut usage = TenantUsage::default();
        assert!(usage.record_event(100));
        assert_eq!(usage.events_processed, 1);
        assert_eq!(usage.events_in_window, 1);
    }

    #[test]
    fn test_usage_rate_limit() {
        let mut usage = TenantUsage::default();
        // Allow 2 events per second
        assert!(usage.record_event(2));
        assert!(usage.record_event(2));
        // Third should be rejected
        assert!(!usage.record_event(2));
    }

    #[test]
    fn test_usage_no_rate_limit() {
        let mut usage = TenantUsage::default();
        // 0 means disabled
        for _ in 0..1000 {
            assert!(usage.record_event(0));
        }
    }

    #[test]
    fn test_tenant_manager_create() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Test Corp".into(), "key-123".into(), TenantQuota::free())
            .unwrap();

        assert_eq!(mgr.tenant_count(), 1);
        assert!(mgr.get_tenant(&id).is_some());
        assert_eq!(mgr.get_tenant(&id).unwrap().name, "Test Corp");
    }

    #[test]
    fn test_tenant_manager_api_key_lookup() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Test".into(), "my-key".into(), TenantQuota::default())
            .unwrap();

        let found = mgr.get_tenant_by_api_key("my-key");
        assert_eq!(found, Some(&id));

        assert!(mgr.get_tenant_by_api_key("wrong-key").is_none());
    }

    #[test]
    fn test_tenant_manager_duplicate_api_key() {
        let mut mgr = TenantManager::new();
        mgr.create_tenant("A".into(), "key-1".into(), TenantQuota::default())
            .unwrap();
        let result = mgr.create_tenant("B".into(), "key-1".into(), TenantQuota::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_tenant_manager_remove() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Test".into(), "key-1".into(), TenantQuota::default())
            .unwrap();

        mgr.remove_tenant(&id).unwrap();
        assert_eq!(mgr.tenant_count(), 0);
        assert!(mgr.get_tenant_by_api_key("key-1").is_none());
    }

    #[test]
    fn test_tenant_deploy_pipeline() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Test".into(), "key-1".into(), TenantQuota::default())
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let vpl = r#"
            stream Alerts = SensorReading
                .where(temperature > 100)
        "#;
        let pipeline_id = tenant
            .deploy_pipeline("My Pipeline".into(), vpl.into())
            .unwrap();
        assert_eq!(tenant.pipelines.len(), 1);
        assert_eq!(tenant.usage.active_pipelines, 1);
        assert_eq!(
            tenant.pipelines[&pipeline_id].status,
            PipelineStatus::Running
        );
    }

    #[test]
    fn test_tenant_pipeline_quota() {
        let mut mgr = TenantManager::new();
        let quota = TenantQuota {
            max_pipelines: 1,
            max_events_per_second: 100,
            max_streams_per_pipeline: 50,
        };
        let id = mgr
            .create_tenant("Test".into(), "key-1".into(), quota)
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let vpl = "stream A = SensorReading .where(x > 1)";
        tenant.deploy_pipeline("P1".into(), vpl.into()).unwrap();

        // Second should fail
        let result = tenant.deploy_pipeline("P2".into(), vpl.into());
        assert!(result.is_err());
    }

    #[test]
    fn test_tenant_remove_pipeline() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Test".into(), "key-1".into(), TenantQuota::default())
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let vpl = "stream A = SensorReading .where(x > 1)";
        let pid = tenant.deploy_pipeline("P1".into(), vpl.into()).unwrap();

        tenant.remove_pipeline(&pid).unwrap();
        assert_eq!(tenant.pipelines.len(), 0);
        assert_eq!(tenant.usage.active_pipelines, 0);
    }

    #[test]
    fn test_tenant_parse_error() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Test".into(), "key-1".into(), TenantQuota::default())
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let result = tenant.deploy_pipeline("Bad".into(), "this is not valid VPL {{{{".into());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tenant_process_event() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Test".into(), "key-1".into(), TenantQuota::default())
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let vpl = "stream A = SensorReading .where(temperature > 100)";
        let pid = tenant.deploy_pipeline("P1".into(), vpl.into()).unwrap();

        let event = Event::new("SensorReading").with_field("temperature", 150.0);
        tenant.process_event(&pid, event).await.unwrap();
        assert_eq!(tenant.usage.events_processed, 1);
    }

    #[tokio::test]
    async fn test_tenant_rate_limit_on_process() {
        let mut mgr = TenantManager::new();
        let quota = TenantQuota {
            max_pipelines: 10,
            max_events_per_second: 2,
            max_streams_per_pipeline: 50,
        };
        let id = mgr
            .create_tenant("Test".into(), "key-1".into(), quota)
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let vpl = "stream A = SensorReading .where(x > 1)";
        let pid = tenant.deploy_pipeline("P1".into(), vpl.into()).unwrap();

        let event = Event::new("SensorReading").with_field("x", 5);
        tenant.process_event(&pid, event.clone()).await.unwrap();
        tenant.process_event(&pid, event.clone()).await.unwrap();

        // Third should hit rate limit
        let result = tenant.process_event(&pid, event).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_tenant_reload_pipeline() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Test".into(), "key-1".into(), TenantQuota::default())
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let vpl1 = "stream A = SensorReading .where(x > 1)";
        let pid = tenant.deploy_pipeline("P1".into(), vpl1.into()).unwrap();

        let vpl2 = "stream B = SensorReading .where(x > 50)";
        tenant.reload_pipeline(&pid, vpl2.into()).unwrap();

        assert_eq!(tenant.pipelines[&pid].source, vpl2);
    }

    #[test]
    fn test_pipeline_status_display() {
        assert_eq!(format!("{}", PipelineStatus::Running), "running");
        assert_eq!(format!("{}", PipelineStatus::Stopped), "stopped");
        assert_eq!(
            format!("{}", PipelineStatus::Error("oops".into())),
            "error: oops"
        );
    }

    #[test]
    fn test_tenant_error_display() {
        assert!(format!("{}", TenantError::NotFound("t1".into())).contains("t1"));
        assert!(format!("{}", TenantError::RateLimitExceeded).contains("rate limit"));
        assert!(format!("{}", TenantError::ParseError("bad".into())).contains("bad"));
    }

    #[test]
    fn test_shared_tenant_manager() {
        let mgr = shared_tenant_manager();
        assert!(Arc::strong_count(&mgr) == 1);
    }

    #[test]
    fn test_tenant_list() {
        let mut mgr = TenantManager::new();
        mgr.create_tenant("A".into(), "key-a".into(), TenantQuota::default())
            .unwrap();
        mgr.create_tenant("B".into(), "key-b".into(), TenantQuota::default())
            .unwrap();
        assert_eq!(mgr.list_tenants().len(), 2);
    }

    #[test]
    fn test_tenant_manager_default() {
        let mgr = TenantManager::default();
        assert_eq!(mgr.tenant_count(), 0);
    }
}
