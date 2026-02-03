//! Multi-tenant isolation for SaaS deployment
//!
//! Provides tenant-scoped engine instances with resource limits and usage tracking.

use crate::context::ContextOrchestrator;
use crate::engine::{Alert, Engine};
use crate::event::Event;
use crate::persistence::{StateStore, StoreError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Unique identifier for a tenant
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Optional context orchestrator for multi-threaded execution
    pub orchestrator: Option<ContextOrchestrator>,
}

/// Pipeline status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        let alert_tx_for_ctx = alert_tx.clone();
        let mut engine = Engine::new(alert_tx);
        engine
            .load(&program)
            .map_err(|e| TenantError::EngineError(e.to_string()))?;

        // Build context orchestrator if the program declares contexts
        let orchestrator = if engine.has_contexts() {
            match ContextOrchestrator::build(
                engine.context_map(),
                &program,
                alert_tx_for_ctx,
                1000,
            ) {
                Ok(orch) => Some(orch),
                Err(e) => {
                    return Err(TenantError::EngineError(format!(
                        "Failed to build context orchestrator: {}",
                        e
                    )));
                }
            }
        } else {
            None
        };

        let id = Uuid::new_v4().to_string();
        let pipeline = Pipeline {
            id: id.clone(),
            name,
            source,
            engine,
            alert_rx,
            created_at: Instant::now(),
            status: PipelineStatus::Running,
            orchestrator,
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

        if let Some(ref orchestrator) = pipeline.orchestrator {
            // Route through context orchestrator
            let shared_event = std::sync::Arc::new(event);
            orchestrator
                .process(shared_event)
                .await
                .map_err(|e| TenantError::EngineError(e.to_string()))?;
        } else {
            // Direct engine processing (no contexts, zero overhead)
            pipeline
                .engine
                .process(event)
                .await
                .map_err(|e| TenantError::EngineError(e.to_string()))?;
        }

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
    /// Optional persistent state store
    store: Option<Arc<dyn StateStore>>,
}

impl TenantManager {
    pub fn new() -> Self {
        Self {
            tenants: HashMap::new(),
            api_key_index: HashMap::new(),
            store: None,
        }
    }

    /// Create a new tenant manager backed by a state store
    pub fn with_store(store: Arc<dyn StateStore>) -> Self {
        Self {
            tenants: HashMap::new(),
            api_key_index: HashMap::new(),
            store: Some(store),
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
        self.persist_if_needed(&id);
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
        if let Some(ref store) = self.store {
            if let Err(e) = Self::delete_tenant_state(store.as_ref(), id) {
                warn!("Failed to delete persisted state for tenant {}: {}", id, e);
            }
        }
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

    /// Persist a tenant's current state to the store (if configured)
    pub fn persist_if_needed(&self, tenant_id: &TenantId) {
        if let Some(ref store) = self.store {
            if let Some(tenant) = self.tenants.get(tenant_id) {
                if let Err(e) = Self::persist_tenant_to_store(store.as_ref(), tenant) {
                    warn!("Failed to persist tenant {}: {}", tenant_id, e);
                }
            }
        }
    }

    /// Recover all tenants from the state store
    ///
    /// Returns the number of tenants successfully recovered.
    pub fn recover(&mut self) -> Result<usize, StoreError> {
        let store = match &self.store {
            Some(s) => Arc::clone(s),
            None => return Ok(0),
        };

        // Load tenant index
        let index_data = match store.get("tenants:index")? {
            Some(data) => data,
            None => return Ok(0),
        };

        let tenant_ids: Vec<String> = serde_json::from_slice(&index_data)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let mut recovered = 0;
        let mut failed = 0;

        for tid in &tenant_ids {
            let key = format!("tenant:{}", tid);
            let data = match store.get(&key)? {
                Some(d) => d,
                None => {
                    warn!("Tenant {} listed in index but not found in store", tid);
                    failed += 1;
                    continue;
                }
            };

            let snapshot: TenantSnapshot = match serde_json::from_slice(&data) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Failed to deserialize tenant {}: {}", tid, e);
                    failed += 1;
                    continue;
                }
            };

            match Self::restore_tenant_from_snapshot(snapshot) {
                Ok(tenant) => {
                    let tenant_id = tenant.id.clone();
                    self.api_key_index
                        .insert(tenant.api_key.clone(), tenant_id.clone());
                    let pipeline_count = tenant.pipelines.len();
                    self.tenants.insert(tenant_id.clone(), tenant);
                    info!(
                        "Recovered tenant {} with {} pipeline(s)",
                        tenant_id, pipeline_count
                    );
                    recovered += 1;
                }
                Err(e) => {
                    warn!("Failed to restore tenant {}: {}", tid, e);
                    failed += 1;
                }
            }
        }

        if failed > 0 {
            warn!(
                "Recovery complete: {} recovered, {} failed",
                recovered, failed
            );
        }

        Ok(recovered)
    }

    fn persist_tenant_to_store(store: &dyn StateStore, tenant: &Tenant) -> Result<(), StoreError> {
        let snapshot = tenant.snapshot();
        let data = serde_json::to_vec(&snapshot)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        let key = format!("tenant:{}", snapshot.id);
        store.put(&key, &data)?;

        // Update tenant index
        Self::update_tenant_index_add(store, &snapshot.id)?;

        store.flush()
    }

    fn delete_tenant_state(store: &dyn StateStore, id: &TenantId) -> Result<(), StoreError> {
        let key = format!("tenant:{}", id.0);
        store.delete(&key)?;

        Self::update_tenant_index_remove(store, &id.0)?;

        store.flush()
    }

    fn update_tenant_index_add(store: &dyn StateStore, tenant_id: &str) -> Result<(), StoreError> {
        let mut ids = Self::load_tenant_index(store)?;
        let id_str = tenant_id.to_string();
        if !ids.contains(&id_str) {
            ids.push(id_str);
        }
        let data = serde_json::to_vec(&ids)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        store.put("tenants:index", &data)
    }

    fn update_tenant_index_remove(
        store: &dyn StateStore,
        tenant_id: &str,
    ) -> Result<(), StoreError> {
        let mut ids = Self::load_tenant_index(store)?;
        ids.retain(|id| id != tenant_id);
        let data = serde_json::to_vec(&ids)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        store.put("tenants:index", &data)
    }

    fn load_tenant_index(store: &dyn StateStore) -> Result<Vec<String>, StoreError> {
        match store.get("tenants:index")? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StoreError::SerializationError(e.to_string())),
            None => Ok(Vec::new()),
        }
    }

    fn restore_tenant_from_snapshot(snapshot: TenantSnapshot) -> Result<Tenant, TenantError> {
        let tenant_id = TenantId::new(&snapshot.id);

        let mut tenant = Tenant::new(
            tenant_id,
            snapshot.name,
            snapshot.api_key,
            snapshot.quota,
        );
        tenant.usage.events_processed = snapshot.events_processed;
        tenant.usage.alerts_generated = snapshot.alerts_generated;
        tenant.usage.events_in_window = snapshot.events_in_window;

        let mut pipeline_failures = Vec::new();
        for ps in snapshot.pipelines {
            match Self::restore_pipeline_from_snapshot(ps.clone()) {
                Ok(pipeline) => {
                    tenant.pipelines.insert(pipeline.id.clone(), pipeline);
                }
                Err(e) => {
                    warn!(
                        "Failed to restore pipeline '{}' ({}): {}",
                        ps.name, ps.id, e
                    );
                    pipeline_failures.push(ps.id);
                }
            }
        }

        tenant.usage.active_pipelines = tenant.pipelines.len();
        Ok(tenant)
    }

    fn restore_pipeline_from_snapshot(snapshot: PipelineSnapshot) -> Result<Pipeline, TenantError> {
        let program = varpulis_parser::parse(&snapshot.source)
            .map_err(|e| TenantError::ParseError(e.to_string()))?;

        let (alert_tx, alert_rx) = mpsc::channel(1000);
        let mut engine = Engine::new(alert_tx);
        engine
            .load(&program)
            .map_err(|e| TenantError::EngineError(e.to_string()))?;

        Ok(Pipeline {
            id: snapshot.id,
            name: snapshot.name,
            source: snapshot.source,
            engine,
            alert_rx,
            created_at: Instant::now(),
            status: snapshot.status,
            orchestrator: None,
        })
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

/// Create a shared tenant manager backed by a state store, recovering any persisted state
pub fn shared_tenant_manager_with_store(store: Arc<dyn StateStore>) -> SharedTenantManager {
    let mut mgr = TenantManager::with_store(store);
    match mgr.recover() {
        Ok(count) if count > 0 => {
            info!("Recovered {} tenant(s) from persistent state", count);
        }
        Ok(_) => {
            info!("No persisted tenant state found, starting fresh");
        }
        Err(e) => {
            error!("Failed to recover tenant state: {}", e);
        }
    }
    Arc::new(RwLock::new(mgr))
}

// =============================================================================
// Snapshot Types for Persistence
// =============================================================================

/// Serializable snapshot of a tenant's state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantSnapshot {
    pub id: String,
    pub name: String,
    pub api_key: String,
    pub quota: TenantQuota,
    pub events_processed: u64,
    pub alerts_generated: u64,
    pub pipelines: Vec<PipelineSnapshot>,
    #[serde(default)]
    pub created_at_ms: Option<i64>,
    #[serde(default)]
    pub events_in_window: u64,
}

/// Serializable snapshot of a pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshot {
    pub id: String,
    pub name: String,
    pub source: String,
    pub status: PipelineStatus,
}

impl Pipeline {
    /// Create a serializable snapshot of this pipeline
    pub fn snapshot(&self) -> PipelineSnapshot {
        PipelineSnapshot {
            id: self.id.clone(),
            name: self.name.clone(),
            source: self.source.clone(),
            status: self.status.clone(),
        }
    }
}

impl Tenant {
    /// Create a serializable snapshot of this tenant and all its pipelines
    pub fn snapshot(&self) -> TenantSnapshot {
        TenantSnapshot {
            id: self.id.0.clone(),
            name: self.name.clone(),
            api_key: self.api_key.clone(),
            quota: self.quota.clone(),
            events_processed: self.usage.events_processed,
            alerts_generated: self.usage.alerts_generated,
            pipelines: self.pipelines.values().map(|p| p.snapshot()).collect(),
            created_at_ms: Some(chrono::Utc::now().timestamp_millis()),
            events_in_window: self.usage.events_in_window,
        }
    }
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

    #[test]
    fn test_tenant_snapshot_roundtrip() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Snap Corp".into(), "snap-key".into(), TenantQuota::pro())
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let vpl = "stream A = SensorReading .where(x > 1)";
        tenant.deploy_pipeline("Pipeline1".into(), vpl.into()).unwrap();
        tenant.usage.events_processed = 42;
        tenant.usage.alerts_generated = 7;

        let snapshot = tenant.snapshot();
        let json = serde_json::to_vec(&snapshot).unwrap();
        let restored: TenantSnapshot = serde_json::from_slice(&json).unwrap();

        assert_eq!(restored.id, id.0);
        assert_eq!(restored.name, "Snap Corp");
        assert_eq!(restored.api_key, "snap-key");
        assert_eq!(restored.events_processed, 42);
        assert_eq!(restored.alerts_generated, 7);
        assert_eq!(restored.pipelines.len(), 1);
        assert_eq!(restored.pipelines[0].name, "Pipeline1");
        assert_eq!(restored.pipelines[0].source, vpl);
        assert_eq!(restored.pipelines[0].status, PipelineStatus::Running);
        assert_eq!(restored.quota.max_pipelines, TenantQuota::pro().max_pipelines);
    }

    #[test]
    fn test_tenant_manager_persistence_and_recovery() {
        use crate::persistence::MemoryStore;

        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
        let vpl = "stream A = SensorReading .where(x > 1)";

        // Create tenants and pipelines with persistence
        let (tenant_id, pipeline_name) = {
            let mut mgr = TenantManager::with_store(Arc::clone(&store));
            let id = mgr
                .create_tenant("Persisted Corp".into(), "persist-key".into(), TenantQuota::default())
                .unwrap();

            let tenant = mgr.get_tenant_mut(&id).unwrap();
            tenant.deploy_pipeline("Persistent Pipeline".into(), vpl.into()).unwrap();
            tenant.usage.events_processed = 100;
            tenant.usage.alerts_generated = 5;
            mgr.persist_if_needed(&id);

            (id.0.clone(), "Persistent Pipeline".to_string())
        };

        // Recover into a new manager
        let mut mgr2 = TenantManager::with_store(Arc::clone(&store));
        let recovered = mgr2.recover().unwrap();
        assert_eq!(recovered, 1);
        assert_eq!(mgr2.tenant_count(), 1);

        // Verify tenant data
        let tid = TenantId::new(&tenant_id);
        let tenant = mgr2.get_tenant(&tid).unwrap();
        assert_eq!(tenant.name, "Persisted Corp");
        assert_eq!(tenant.api_key, "persist-key");
        assert_eq!(tenant.usage.events_processed, 100);
        assert_eq!(tenant.usage.alerts_generated, 5);
        assert_eq!(tenant.pipelines.len(), 1);

        let pipeline = tenant.pipelines.values().next().unwrap();
        assert_eq!(pipeline.name, pipeline_name);
        assert_eq!(pipeline.source, vpl);
        assert_eq!(pipeline.status, PipelineStatus::Running);

        // Verify API key index was rebuilt
        assert_eq!(
            mgr2.get_tenant_by_api_key("persist-key"),
            Some(&tid)
        );
    }

    #[test]
    fn test_persistence_survives_restart() {
        use crate::persistence::MemoryStore;

        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
        let vpl = "stream A = SensorReading .where(x > 1)";

        // Phase 1: Create data
        {
            let mut mgr = TenantManager::with_store(Arc::clone(&store));
            let id1 = mgr
                .create_tenant("Tenant A".into(), "key-a".into(), TenantQuota::free())
                .unwrap();
            let id2 = mgr
                .create_tenant("Tenant B".into(), "key-b".into(), TenantQuota::pro())
                .unwrap();

            let tenant_a = mgr.get_tenant_mut(&id1).unwrap();
            tenant_a.deploy_pipeline("P1".into(), vpl.into()).unwrap();
            mgr.persist_if_needed(&id1);

            let tenant_b = mgr.get_tenant_mut(&id2).unwrap();
            tenant_b.deploy_pipeline("P2".into(), vpl.into()).unwrap();
            tenant_b.deploy_pipeline("P3".into(), vpl.into()).unwrap();
            mgr.persist_if_needed(&id2);
        }

        // Phase 2: Recover (simulating restart)
        {
            let mut mgr = TenantManager::with_store(Arc::clone(&store));
            let recovered = mgr.recover().unwrap();
            assert_eq!(recovered, 2);
            assert_eq!(mgr.tenant_count(), 2);

            // Verify tenant A
            let tid_a = mgr.get_tenant_by_api_key("key-a").unwrap().clone();
            let tenant_a = mgr.get_tenant(&tid_a).unwrap();
            assert_eq!(tenant_a.name, "Tenant A");
            assert_eq!(tenant_a.pipelines.len(), 1);

            // Verify tenant B
            let tid_b = mgr.get_tenant_by_api_key("key-b").unwrap().clone();
            let tenant_b = mgr.get_tenant(&tid_b).unwrap();
            assert_eq!(tenant_b.name, "Tenant B");
            assert_eq!(tenant_b.pipelines.len(), 2);
        }
    }

    #[test]
    fn test_tenant_snapshot_created_at_and_window() {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant("Window Corp".into(), "window-key".into(), TenantQuota::default())
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        tenant.usage.events_in_window = 42;
        tenant.usage.events_processed = 100;

        let snapshot = tenant.snapshot();

        // created_at_ms should be populated
        assert!(snapshot.created_at_ms.is_some());
        let ts = snapshot.created_at_ms.unwrap();
        // Should be a reasonable recent timestamp (after 2024-01-01)
        assert!(ts > 1_704_067_200_000);

        // events_in_window should be captured
        assert_eq!(snapshot.events_in_window, 42);

        // Round-trip through JSON
        let json = serde_json::to_vec(&snapshot).unwrap();
        let restored: TenantSnapshot = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.events_in_window, 42);
        assert_eq!(restored.created_at_ms, snapshot.created_at_ms);

        // Restore tenant and verify events_in_window is carried over
        let restored_tenant =
            TenantManager::restore_tenant_from_snapshot(restored).unwrap();
        assert_eq!(restored_tenant.usage.events_in_window, 42);
        assert_eq!(restored_tenant.usage.events_processed, 100);
    }

    #[test]
    fn test_tenant_snapshot_backwards_compat() {
        // Simulate old JSON without the new fields
        let old_json = r#"{
            "id": "compat-tenant",
            "name": "Compat Corp",
            "api_key": "compat-key",
            "quota": {
                "max_pipelines": 10,
                "max_events_per_second": 10000,
                "max_streams_per_pipeline": 50
            },
            "events_processed": 55,
            "alerts_generated": 3,
            "pipelines": []
        }"#;

        let snapshot: TenantSnapshot = serde_json::from_str(old_json).unwrap();
        assert_eq!(snapshot.id, "compat-tenant");
        assert_eq!(snapshot.events_processed, 55);
        // New fields should default
        assert_eq!(snapshot.created_at_ms, None);
        assert_eq!(snapshot.events_in_window, 0);

        // Should restore fine
        let tenant = TenantManager::restore_tenant_from_snapshot(snapshot).unwrap();
        assert_eq!(tenant.name, "Compat Corp");
        assert_eq!(tenant.usage.events_processed, 55);
        assert_eq!(tenant.usage.events_in_window, 0);
    }

    #[test]
    fn test_recovery_with_invalid_vpl() {
        use crate::persistence::MemoryStore;

        let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());

        // Manually store a tenant with invalid VPL
        let snapshot = TenantSnapshot {
            id: "bad-tenant".into(),
            name: "Bad Corp".into(),
            api_key: "bad-key".into(),
            quota: TenantQuota::default(),
            events_processed: 0,
            alerts_generated: 0,
            pipelines: vec![PipelineSnapshot {
                id: "bad-pipeline".into(),
                name: "Bad Pipeline".into(),
                source: "this is not valid VPL {{{{".into(),
                status: PipelineStatus::Running,
            }],
            created_at_ms: None,
            events_in_window: 0,
        };

        let data = serde_json::to_vec(&snapshot).unwrap();
        store.put("tenant:bad-tenant", &data).unwrap();
        let index = serde_json::to_vec(&vec!["bad-tenant"]).unwrap();
        store.put("tenants:index", &index).unwrap();

        // Recovery should succeed (tenant recovered, bad pipeline skipped)
        let mut mgr = TenantManager::with_store(Arc::clone(&store));
        let recovered = mgr.recover().unwrap();
        assert_eq!(recovered, 1);

        let tid = TenantId::new("bad-tenant");
        let tenant = mgr.get_tenant(&tid).unwrap();
        assert_eq!(tenant.name, "Bad Corp");
        // The invalid pipeline should have been skipped
        assert_eq!(tenant.pipelines.len(), 0);
    }
}
