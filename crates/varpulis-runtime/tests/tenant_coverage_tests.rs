//! Extended coverage tests for the tenant module.
//!
//! Covers: tenant CRUD, quota enforcement, pipeline management,
//! usage tracking, error conditions, shared_tenant_manager, persistence,
//! and snapshot round-trips.

use std::sync::Arc;

use varpulis_runtime::persistence::{MemoryStore, StateStore};
use varpulis_runtime::tenant::*;
use varpulis_runtime::Event;

// ---------------------------------------------------------------------------
// 1. Create / list / remove tenants
// ---------------------------------------------------------------------------

#[test]
fn create_tenant_basic() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant(
            "Acme Corp".into(),
            "acme-key".into(),
            TenantQuota::default(),
        )
        .unwrap();

    assert_eq!(mgr.tenant_count(), 1);

    let tenant = mgr.get_tenant(&id).unwrap();
    assert_eq!(tenant.name, "Acme Corp");
    assert_eq!(tenant.api_key, "acme-key");
}

#[test]
fn list_tenants_returns_all() {
    let mut mgr = TenantManager::new();
    mgr.create_tenant("A".into(), "ka".into(), TenantQuota::default())
        .unwrap();
    mgr.create_tenant("B".into(), "kb".into(), TenantQuota::default())
        .unwrap();
    mgr.create_tenant("C".into(), "kc".into(), TenantQuota::default())
        .unwrap();

    let tenants = mgr.list_tenants();
    assert_eq!(tenants.len(), 3);

    let names: Vec<&str> = tenants.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"A"));
    assert!(names.contains(&"B"));
    assert!(names.contains(&"C"));
}

#[test]
fn remove_tenant_decreases_count() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("X".into(), "kx".into(), TenantQuota::default())
        .unwrap();

    assert_eq!(mgr.tenant_count(), 1);
    mgr.remove_tenant(&id).unwrap();
    assert_eq!(mgr.tenant_count(), 0);
}

#[test]
fn remove_tenant_clears_api_key_index() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("Y".into(), "ky".into(), TenantQuota::default())
        .unwrap();

    mgr.remove_tenant(&id).unwrap();
    assert!(mgr.get_tenant_by_api_key("ky").is_none());
}

// ---------------------------------------------------------------------------
// 2. Tenant quota enforcement
// ---------------------------------------------------------------------------

#[test]
fn quota_tiers_ordering() {
    let free = TenantQuota::free();
    let pro = TenantQuota::pro();
    let enterprise = TenantQuota::enterprise();

    assert!(free.max_pipelines < pro.max_pipelines);
    assert!(pro.max_pipelines < enterprise.max_pipelines);

    assert!(free.max_events_per_second < pro.max_events_per_second);
    assert!(pro.max_events_per_second < enterprise.max_events_per_second);

    assert!(free.max_streams_per_pipeline < pro.max_streams_per_pipeline);
    assert!(pro.max_streams_per_pipeline < enterprise.max_streams_per_pipeline);
}

#[tokio::test]
async fn quota_max_pipelines_enforced() {
    let mut mgr = TenantManager::new();
    let quota = TenantQuota {
        max_pipelines: 1,
        max_events_per_second: 10_000,
        max_streams_per_pipeline: 50,
    };
    let id = mgr.create_tenant("T".into(), "k".into(), quota).unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let vpl = "stream A = SensorReading .where(x > 1)";

    // First pipeline should succeed
    tenant
        .deploy_pipeline("P1".into(), vpl.into())
        .await
        .unwrap();

    // Second should fail due to quota
    let result = tenant.deploy_pipeline("P2".into(), vpl.into()).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("quota"));
}

// ---------------------------------------------------------------------------
// 3. Pipeline deployment and management
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deploy_pipeline_success() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let vpl = "stream Alerts = SensorReading .where(temperature > 100)";
    let pid = tenant
        .deploy_pipeline("Filter".into(), vpl.into())
        .await
        .unwrap();

    assert_eq!(tenant.pipelines.len(), 1);
    assert_eq!(tenant.pipelines[&pid].name, "Filter");
    assert_eq!(tenant.pipelines[&pid].status, PipelineStatus::Running);
    assert_eq!(tenant.usage.active_pipelines, 1);
}

#[tokio::test]
async fn remove_pipeline_updates_usage() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let vpl = "stream A = SensorReading .where(x > 1)";
    let pid = tenant
        .deploy_pipeline("P".into(), vpl.into())
        .await
        .unwrap();

    assert_eq!(tenant.usage.active_pipelines, 1);
    tenant.remove_pipeline(&pid).unwrap();
    assert_eq!(tenant.usage.active_pipelines, 0);
    assert!(tenant.pipelines.is_empty());
}

#[tokio::test]
async fn deploy_multiple_pipelines() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let vpl = "stream A = SensorReading .where(x > 1)";

    tenant
        .deploy_pipeline("P1".into(), vpl.into())
        .await
        .unwrap();
    tenant
        .deploy_pipeline("P2".into(), vpl.into())
        .await
        .unwrap();
    tenant
        .deploy_pipeline("P3".into(), vpl.into())
        .await
        .unwrap();

    assert_eq!(tenant.pipelines.len(), 3);
    assert_eq!(tenant.usage.active_pipelines, 3);
}

#[tokio::test]
async fn reload_pipeline_updates_source() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let vpl1 = "stream A = SensorReading .where(x > 1)";
    let pid = tenant
        .deploy_pipeline("P".into(), vpl1.into())
        .await
        .unwrap();

    let vpl2 = "stream B = SensorReading .where(x > 50)";
    tenant.reload_pipeline(&pid, vpl2.into()).await.unwrap();

    assert_eq!(tenant.pipelines[&pid].source, vpl2);
}

// ---------------------------------------------------------------------------
// 4. TenantUsage tracking
// ---------------------------------------------------------------------------

#[test]
fn usage_defaults() {
    let usage = TenantUsage::default();
    assert_eq!(usage.events_processed, 0);
    assert_eq!(usage.events_in_window, 0);
    assert!(usage.window_start.is_none());
    assert_eq!(usage.active_pipelines, 0);
    assert_eq!(usage.output_events_emitted, 0);
}

#[test]
fn usage_record_event_increments() {
    let mut usage = TenantUsage::default();
    usage.record_event(100);
    usage.record_event(100);
    usage.record_event(100);

    assert_eq!(usage.events_processed, 3);
}

#[test]
fn usage_record_output_event() {
    let mut usage = TenantUsage::default();
    usage.record_output_event();
    usage.record_output_event();
    assert_eq!(usage.output_events_emitted, 2);
}

#[test]
fn usage_rate_limit_enforcement() {
    let mut usage = TenantUsage::default();
    assert!(usage.record_event(3));
    assert!(usage.record_event(3));
    assert!(usage.record_event(3));
    // 4th event in same second should be rejected
    assert!(!usage.record_event(3));
}

#[test]
fn usage_rate_limit_disabled_with_zero() {
    let mut usage = TenantUsage::default();
    for _ in 0..1000 {
        assert!(usage.record_event(0));
    }
    assert_eq!(usage.events_processed, 1000);
}

#[tokio::test]
async fn process_event_increments_usage() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let vpl = "stream A = SensorReading .where(temperature > 100)";
    let pid = tenant
        .deploy_pipeline("P".into(), vpl.into())
        .await
        .unwrap();

    let event = Event::new("SensorReading").with_field("temperature", 150.0);
    tenant.process_event(&pid, event).await.unwrap();

    assert_eq!(tenant.usage.events_processed, 1);
}

// ---------------------------------------------------------------------------
// 5. Error conditions
// ---------------------------------------------------------------------------

#[test]
fn duplicate_api_key_error() {
    let mut mgr = TenantManager::new();
    mgr.create_tenant("A".into(), "same-key".into(), TenantQuota::default())
        .unwrap();

    let result = mgr.create_tenant("B".into(), "same-key".into(), TenantQuota::default());
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("already exists"));
}

#[test]
fn remove_nonexistent_tenant_error() {
    let mut mgr = TenantManager::new();
    let fake_id = TenantId::new("nonexistent");

    let result = mgr.remove_tenant(&fake_id);
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("not found"));
}

#[tokio::test]
async fn remove_nonexistent_pipeline_error() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let result = tenant.remove_pipeline("no-such-pipeline");
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("pipeline not found"));
}

#[tokio::test]
async fn deploy_pipeline_parse_error() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let result = tenant
        .deploy_pipeline("Bad".into(), "NOT VALID VPL {{{{".into())
        .await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("parse error"));
}

#[tokio::test]
async fn process_event_on_nonexistent_pipeline() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let event = Event::new("Test");
    let result = tenant.process_event("fake-pipeline-id", event).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("pipeline not found"));
}

#[tokio::test]
async fn rate_limit_during_process_event() {
    let mut mgr = TenantManager::new();
    let quota = TenantQuota {
        max_pipelines: 10,
        max_events_per_second: 2,
        max_streams_per_pipeline: 50,
    };
    let id = mgr.create_tenant("T".into(), "k".into(), quota).unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let vpl = "stream A = SensorReading .where(x > 1)";
    let pid = tenant
        .deploy_pipeline("P".into(), vpl.into())
        .await
        .unwrap();

    let event = Event::new("SensorReading").with_field("x", 5);
    tenant.process_event(&pid, event.clone()).await.unwrap();
    tenant.process_event(&pid, event.clone()).await.unwrap();

    // Third event should hit rate limit
    let result = tenant.process_event(&pid, event).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("rate limit"));
}

// ---------------------------------------------------------------------------
// 6. Error type display
// ---------------------------------------------------------------------------

#[test]
fn tenant_error_display_variants() {
    let errs = vec![
        (TenantError::NotFound("t1".into()), "not found"),
        (
            TenantError::PipelineNotFound("p1".into()),
            "pipeline not found",
        ),
        (TenantError::QuotaExceeded("max".into()), "quota exceeded"),
        (TenantError::RateLimitExceeded, "rate limit"),
        (TenantError::ParseError("bad syntax".into()), "parse error"),
        (TenantError::EngineError("boom".into()), "engine error"),
        (TenantError::AlreadyExists("dup".into()), "already exists"),
    ];

    for (err, expected_substr) in errs {
        let msg = format!("{}", err);
        assert!(
            msg.contains(expected_substr),
            "Expected '{}' to contain '{}'",
            msg,
            expected_substr
        );
    }
}

#[test]
fn pipeline_status_display() {
    assert_eq!(format!("{}", PipelineStatus::Running), "running");
    assert_eq!(format!("{}", PipelineStatus::Stopped), "stopped");
    assert_eq!(
        format!("{}", PipelineStatus::Error("fail".into())),
        "error: fail"
    );
}

// ---------------------------------------------------------------------------
// 7. shared_tenant_manager factory function
// ---------------------------------------------------------------------------

#[test]
fn shared_tenant_manager_creates_arc() {
    let mgr = shared_tenant_manager();
    assert_eq!(Arc::strong_count(&mgr), 1);
}

#[tokio::test]
async fn shared_tenant_manager_is_usable() {
    let mgr = shared_tenant_manager();

    let mut write_guard = mgr.write().await;
    let id = write_guard
        .create_tenant("Shared".into(), "shared-key".into(), TenantQuota::default())
        .unwrap();
    assert_eq!(write_guard.tenant_count(), 1);
    drop(write_guard);

    let read_guard = mgr.read().await;
    assert!(read_guard.get_tenant(&id).is_some());
}

// ---------------------------------------------------------------------------
// 8. TenantId
// ---------------------------------------------------------------------------

#[test]
fn tenant_id_new_and_display() {
    let id = TenantId::new("my-tenant");
    assert_eq!(id.as_str(), "my-tenant");
    assert_eq!(format!("{}", id), "my-tenant");
}

#[test]
fn tenant_id_generate_unique() {
    let id1 = TenantId::generate();
    let id2 = TenantId::generate();
    assert_ne!(id1, id2);
}

#[test]
fn tenant_id_equality() {
    let id1 = TenantId::new("same");
    let id2 = TenantId::new("same");
    assert_eq!(id1, id2);

    let id3 = TenantId::new("different");
    assert_ne!(id1, id3);
}

// ---------------------------------------------------------------------------
// 9. TenantManager with store and persistence
// ---------------------------------------------------------------------------

#[test]
fn tenant_manager_default() {
    let mgr = TenantManager::default();
    assert_eq!(mgr.tenant_count(), 0);
}

#[test]
fn tenant_manager_with_store_persists() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
    let mut mgr = TenantManager::with_store(Arc::clone(&store));

    let id = mgr
        .create_tenant("Stored".into(), "stored-key".into(), TenantQuota::default())
        .unwrap();

    // persist_if_needed should write to store
    mgr.persist_if_needed(&id);

    // Verify something was written
    let data = store.get(&format!("tenant:{}", id.as_str())).unwrap();
    assert!(data.is_some());
}

#[test]
fn tenant_manager_recover_from_store() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());

    // Phase 1: Create and persist
    {
        let mut mgr = TenantManager::with_store(Arc::clone(&store));
        let id = mgr
            .create_tenant("Recover".into(), "rec-key".into(), TenantQuota::pro())
            .unwrap();
        mgr.persist_if_needed(&id);
    }

    // Phase 2: Recover into a new manager
    {
        let mut mgr = TenantManager::with_store(Arc::clone(&store));
        let recovered = mgr.recover().unwrap();
        assert_eq!(recovered, 1);
        assert_eq!(mgr.tenant_count(), 1);

        let tid = mgr.get_tenant_by_api_key("rec-key").unwrap().clone();
        let tenant = mgr.get_tenant(&tid).unwrap();
        assert_eq!(tenant.name, "Recover");
    }
}

#[test]
fn tenant_manager_remove_clears_store() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
    let mut mgr = TenantManager::with_store(Arc::clone(&store));

    let id = mgr
        .create_tenant("Rm".into(), "rm-key".into(), TenantQuota::default())
        .unwrap();
    mgr.persist_if_needed(&id);

    // Verify it exists in store
    let key = format!("tenant:{}", id.as_str());
    assert!(store.get(&key).unwrap().is_some());

    // Remove and verify store is cleaned up
    mgr.remove_tenant(&id).unwrap();
    assert!(store.get(&key).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// 10. Snapshot round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tenant_snapshot_round_trip() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("Snap".into(), "snap-key".into(), TenantQuota::pro())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    let vpl = "stream A = SensorReading .where(x > 1)";
    tenant
        .deploy_pipeline("P1".into(), vpl.into())
        .await
        .unwrap();
    tenant.usage.events_processed = 42;
    tenant.usage.output_events_emitted = 7;

    let snapshot = tenant.snapshot();
    let json = serde_json::to_vec(&snapshot).unwrap();
    let restored: TenantSnapshot = serde_json::from_slice(&json).unwrap();

    assert_eq!(restored.id, id.0);
    assert_eq!(restored.name, "Snap");
    assert_eq!(restored.api_key, "snap-key");
    assert_eq!(restored.events_processed, 42);
    assert_eq!(restored.output_events_emitted, 7);
    assert_eq!(restored.pipelines.len(), 1);
    assert_eq!(restored.pipelines[0].name, "P1");
    assert_eq!(restored.pipelines[0].status, PipelineStatus::Running);
}

// ---------------------------------------------------------------------------
// 11. Shared tenant manager with store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shared_tenant_manager_with_store_recovers() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());

    // Populate store
    {
        let mut mgr = TenantManager::with_store(Arc::clone(&store));
        let id = mgr
            .create_tenant("Pre".into(), "pre-key".into(), TenantQuota::default())
            .unwrap();
        mgr.persist_if_needed(&id);
    }

    // Use shared factory — it should recover
    let shared = shared_tenant_manager_with_store(Arc::clone(&store));
    let guard = shared.read().await;
    assert_eq!(guard.tenant_count(), 1);
    assert!(guard.get_tenant_by_api_key("pre-key").is_some());
}

// ---------------------------------------------------------------------------
// 12. Get tenant by api key
// ---------------------------------------------------------------------------

#[test]
fn get_tenant_by_api_key_found_and_not_found() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "my-key".into(), TenantQuota::default())
        .unwrap();

    assert_eq!(mgr.get_tenant_by_api_key("my-key"), Some(&id));
    assert!(mgr.get_tenant_by_api_key("other-key").is_none());
}

// ---------------------------------------------------------------------------
// 13. get_tenant / get_tenant_mut
// ---------------------------------------------------------------------------

#[test]
fn get_tenant_mut_allows_modification() {
    let mut mgr = TenantManager::new();
    let id = mgr
        .create_tenant("T".into(), "k".into(), TenantQuota::default())
        .unwrap();

    let tenant = mgr.get_tenant_mut(&id).unwrap();
    tenant.usage.events_processed = 999;

    let tenant = mgr.get_tenant(&id).unwrap();
    assert_eq!(tenant.usage.events_processed, 999);
}

#[test]
fn get_tenant_nonexistent_returns_none() {
    let mgr = TenantManager::new();
    let fake_id = TenantId::new("fake");
    assert!(mgr.get_tenant(&fake_id).is_none());
    // Note: can't test get_tenant_mut on non-mut manager
}
