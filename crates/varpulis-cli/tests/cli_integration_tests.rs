//! Comprehensive CLI integration tests targeting coverage gaps.
//!
//! Tests cover: playground routes, API route handlers, simulation edge cases,
//! config utilities, billing utilities, OAuth helpers, and client construction.
//! All tests use in-memory state or tempfile — no live services needed.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tower::ServiceExt;
use varpulis_cli::billing::{BillingConfig, Tier, UsageTracker};
use varpulis_cli::config::Config;
use varpulis_cli::playground::{PlaygroundState, SharedPlayground};
use varpulis_runtime::event::Event;
use varpulis_runtime::tenant::{TenantManager, TenantQuota};

// =============================================================================
// Helper: build playground state
// =============================================================================

fn new_playground() -> SharedPlayground {
    Arc::new(RwLock::new(PlaygroundState::new()))
}

// =============================================================================
// Helper: API test utilities
// =============================================================================

type SharedTenantManager = Arc<RwLock<TenantManager>>;

async fn setup_api_manager() -> SharedTenantManager {
    let mut mgr = TenantManager::new();
    mgr.create_tenant(
        "IntTest".into(),
        "int-test-key".into(),
        TenantQuota::default(),
    )
    .unwrap();
    Arc::new(RwLock::new(mgr))
}

fn api_routes(mgr: SharedTenantManager) -> axum::Router {
    varpulis_cli::api::api_routes(
        mgr,
        Some("admin-key-secret".to_string()),
        None,
        None,
        #[cfg(feature = "saas")]
        None,
    )
}

async fn json_body(resp: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

// =============================================================================
// Playground routes
// =============================================================================

#[tokio::test]
async fn test_playground_create_session() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/playground/session")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = json_body(resp).await;
    assert!(body["session_id"].is_string());
    assert!(!body["session_id"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn test_playground_run_valid_vpl() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    let body = json!({
        "vpl": "stream HighTemp = TempReading\n    .where(temperature > 30)\n    .emit(alert: \"hot\", temp: temperature)",
        "events": "@0s TempReading { sensor_id: \"S1\", temperature: 35 }\n@1s TempReading { sensor_id: \"S2\", temperature: 22 }\n@2s TempReading { sensor_id: \"S3\", temperature: 40 }"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/playground/run")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["ok"], true);
    assert_eq!(body["events_processed"], 3);
    assert_eq!(body["output_events"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_playground_run_invalid_vpl() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    let body = json!({
        "vpl": "stream Invalid = {{{broken",
        "events": ""
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/playground/run")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["ok"], false);
    assert!(body["error"].as_str().unwrap().contains("error"));
}

#[tokio::test]
async fn test_playground_run_empty_events() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    let body = json!({
        "vpl": "stream Test = Events\n    .where(value > 0)\n    .emit(x: value)",
        "events": ""
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/playground/run")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["ok"], true);
    assert_eq!(body["events_processed"], 0);
}

#[tokio::test]
async fn test_playground_validate_valid_vpl() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    let body = json!({
        "vpl": "event Sensor:\n    temp: float\n\nstream HighTemp = Sensor\n    .where(temp > 30.0)\n    .emit(alert: \"hot\")"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/playground/validate")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["ok"], true);
    assert!(body["ast"].is_object() || body["ast"].is_string() || body["ast"].is_array());
}

#[tokio::test]
async fn test_playground_validate_invalid_vpl() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    let body = json!({
        "vpl": "stream Bad = .where("
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/playground/validate")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["ok"], false);
    let diagnostics = body["diagnostics"].as_array().unwrap();
    assert!(!diagnostics.is_empty());
}

#[tokio::test]
async fn test_playground_list_examples() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/playground/examples")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    // Response is a plain array
    let examples = body.as_array().unwrap();
    assert!(!examples.is_empty());
    for ex in examples {
        assert!(ex["id"].is_string());
        assert!(ex["name"].is_string());
        assert!(!ex["id"].as_str().unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_playground_get_example_by_id() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg.clone());

    // First get the list to find a valid ID
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/playground/examples")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = json_body(resp).await;
    let first_id = body[0]["id"].as_str().unwrap().to_string();

    // Now fetch that example
    let app = varpulis_cli::playground::playground_routes(pg);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/playground/examples/{first_id}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["id"], first_id);
    assert!(body["vpl"].is_string());
    assert!(body["events"].is_string());
    assert!(!body["vpl"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn test_playground_get_example_not_found() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/playground/examples/nonexistent-example-id")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_playground_run_vpl_too_large() {
    let pg = new_playground();
    let app = varpulis_cli::playground::playground_routes(pg);

    // 50,001 bytes VPL should be rejected
    let large_vpl = "x".repeat(50_001);
    let body = json!({
        "vpl": large_vpl,
        "events": ""
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/playground/run")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = json_body(resp).await;
    assert!(body["code"].as_str().unwrap().contains("too_large"));
}

// =============================================================================
// API route handler tests
// =============================================================================

#[tokio::test]
async fn test_api_deploy_list_delete() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    // Deploy
    let deploy_body = json!({
        "name": "IntTest Pipeline",
        "source": "stream A = Events .where(x > 1)"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(deploy_body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = json_body(resp).await;
    let pipeline_id = body["id"].as_str().unwrap().to_string();

    // List
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["total"], 1);

    // Delete
    let req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/v1/pipelines/{pipeline_id}"))
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify deleted
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = json_body(resp).await;
    assert_eq!(body["total"], 0);
}

#[tokio::test]
async fn test_api_inject_single_event() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr.clone());

    // Deploy a pipeline first
    let deploy = json!({
        "name": "Inject Test",
        "source": "stream Out = TestEvent .where(value > 10) .emit(v: value)"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(deploy.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = json_body(resp).await;
    let pid = body["id"].as_str().unwrap().to_string();

    // Inject a single event
    let event = json!({
        "event_type": "TestEvent",
        "fields": {"value": 42}
    });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/pipelines/{pid}/events"))
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(event.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_api_inject_batch() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr.clone());

    // Deploy
    let deploy = json!({
        "name": "Batch Test",
        "source": "stream Out = TestEvent .where(value > 10) .emit(v: value)"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(deploy.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = json_body(resp).await;
    let pid = body["id"].as_str().unwrap().to_string();

    // Inject a batch
    let batch = json!({
        "events": [
            {"event_type": "TestEvent", "fields": {"value": 42}},
            {"event_type": "TestEvent", "fields": {"value": 5}},
            {"event_type": "TestEvent", "fields": {"value": 99}}
        ]
    });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/pipelines/{pid}/events-batch"))
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(batch.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["accepted"], 3);
}

#[tokio::test]
async fn test_api_get_pipeline_metrics() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr.clone());

    // Deploy
    let deploy = json!({
        "name": "Metrics Test",
        "source": "stream Out = Events .where(x > 0) .emit(y: x)"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(deploy.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = json_body(resp).await;
    let pid = body["id"].as_str().unwrap().to_string();

    // Get metrics
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/pipelines/{pid}/metrics"))
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["pipeline_id"], pid);
}

#[tokio::test]
async fn test_api_get_pipeline_topology() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr.clone());

    // Deploy
    let deploy = json!({
        "name": "Topology Test",
        "source": "stream Out = Events .where(x > 0) .emit(y: x)"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(deploy.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = json_body(resp).await;
    let pid = body["id"].as_str().unwrap().to_string();

    // Get topology
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/pipelines/{pid}/topology"))
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_api_reload_pipeline() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr.clone());

    // Deploy
    let deploy = json!({
        "name": "Reload Test",
        "source": "stream A = Events .where(x > 0) .emit(y: x)"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(deploy.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = json_body(resp).await;
    let pid = body["id"].as_str().unwrap().to_string();

    // Reload with updated source
    let reload = json!({
        "source": "stream B = Events .where(x > 10) .emit(z: x)"
    });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/pipelines/{pid}/reload"))
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(reload.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_api_usage_endpoint() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/usage")
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert!(body["tenant_id"].is_string());
    assert!(body["quota"].is_object());
}

#[tokio::test]
async fn test_api_missing_api_key() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/pipelines")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_api_wrong_api_key() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "wrong-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_api_get_nonexistent_pipeline() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/pipelines/nonexistent-id")
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_api_delete_nonexistent_pipeline() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/pipelines/nonexistent-id")
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_api_deploy_invalid_vpl() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    let deploy = json!({
        "name": "Bad VPL",
        "source": "this is not valid syntax {{{"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(deploy.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_api_dlq_get_empty() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr.clone());

    // Deploy
    let deploy = json!({
        "name": "DLQ Test",
        "source": "stream A = Events .where(x > 0) .emit(y: x)"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/pipelines")
        .header("x-api-key", "int-test-key")
        .header("content-type", "application/json")
        .body(Body::from(deploy.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = json_body(resp).await;
    let pid = body["id"].as_str().unwrap().to_string();

    // Get DLQ (should be empty)
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/pipelines/{pid}/dlq"))
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["total"], 0);
}

#[tokio::test]
async fn test_api_list_pipelines_with_pagination() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr.clone());

    // Deploy 3 pipelines
    for i in 0..3 {
        let deploy = json!({
            "name": format!("Pipeline {i}"),
            "source": format!("stream S{i} = Events .where(x > {i}) .emit(y: x)")
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/pipelines")
            .header("x-api-key", "int-test-key")
            .header("content-type", "application/json")
            .body(Body::from(deploy.to_string()))
            .unwrap();
        let _resp = app.clone().oneshot(req).await.unwrap();
    }

    // List with pagination
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/pipelines?offset=0&limit=2")
        .header("x-api-key", "int-test-key")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["total"], 3);
    assert!(body["pipelines"].as_array().unwrap().len() <= 3);
}

// =============================================================================
// Tenant admin API tests
// =============================================================================

#[tokio::test]
async fn test_api_create_and_list_tenants() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    // Create a new tenant (requires admin key)
    let create = json!({
        "name": "New Org"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tenants")
        .header("x-admin-key", "admin-key-secret")
        .header("content-type", "application/json")
        .body(Body::from(create.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = json_body(resp).await;
    assert_eq!(body["name"], "New Org");
    assert!(body["api_key"].is_string());

    // List tenants
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/tenants")
        .header("x-admin-key", "admin-key-secret")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert!(body["total"].as_u64().unwrap() >= 2); // original + new one
}

#[tokio::test]
async fn test_api_create_tenant_no_admin_key() {
    let mgr = setup_api_manager().await;
    let app = api_routes(mgr);

    let create = json!({"name": "Unauthorized"});
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tenants")
        .header("content-type", "application/json")
        .body(Body::from(create.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    // Should fail without admin key
    assert!(resp.status().is_client_error());
}

// =============================================================================
// Simulation edge cases
// =============================================================================

#[tokio::test]
async fn test_simulate_multi_stream_program() {
    let vpl = r#"
        stream HighTemp = TempEvent
            .where(temp > 30)
            .emit(alert: "high_temp", t: temp)

        stream LowTemp = TempEvent
            .where(temp < 10)
            .emit(alert: "low_temp", t: temp)
    "#;

    let events = vec![
        Event::new("TempEvent").with_field("temp", 35i64),
        Event::new("TempEvent").with_field("temp", 5i64),
        Event::new("TempEvent").with_field("temp", 25i64),
        Event::new("TempEvent").with_field("temp", 45i64),
        Event::new("TempEvent").with_field("temp", 3i64),
    ];

    let results = varpulis_cli::simulate_from_source(vpl, events)
        .await
        .unwrap();
    // 2 high (35, 45), 2 low (5, 3) = 4 total
    assert_eq!(results.len(), 4);
}

#[tokio::test]
async fn test_simulate_window_aggregation() {
    let vpl = r"
        stream AvgTemp = TempEvent
            .window(1h)
            .emit(total: sum(temp), n: count())
    ";

    let events = vec![
        Event::new("TempEvent").with_field("temp", 10i64),
        Event::new("TempEvent").with_field("temp", 20i64),
        Event::new("TempEvent").with_field("temp", 30i64),
    ];

    let results = varpulis_cli::simulate_from_source(vpl, events).await;
    // Window aggregation should load and process without error.
    // In batch mode, the window may or may not emit depending on timestamps,
    // but the pipeline should succeed.
    assert!(results.is_ok());
}

#[tokio::test]
async fn test_simulate_with_event_declarations() {
    let vpl = r"
        event SensorReading:
            sensor_id: str
            temperature: float

        stream HighTemp = SensorReading
            .where(temperature > 30)
            .emit(sensor: sensor_id, temp: temperature)
    ";

    let events = vec![
        Event::new("SensorReading")
            .with_field("sensor_id", "S1")
            .with_field("temperature", 35.0f64),
        Event::new("SensorReading")
            .with_field("sensor_id", "S2")
            .with_field("temperature", 22.0f64),
    ];

    let results = varpulis_cli::simulate_from_source(vpl, events)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_simulate_with_custom_function() {
    let vpl = r"
        fn double(x: float) -> float:
            x * 2.0

        stream Doubled = NumEvent
            .emit(result: double(value))
    ";

    let events = vec![
        Event::new("NumEvent").with_field("value", 5.0f64),
        Event::new("NumEvent").with_field("value", 10.0f64),
    ];

    let results = varpulis_cli::simulate_from_source(vpl, events)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_simulate_mismatched_event_type() {
    let vpl = r"
        stream Out = SpecificType
            .where(x > 0)
            .emit(y: x)
    ";

    // Send events of a different type
    let events = vec![
        Event::new("OtherType").with_field("x", 100i64),
        Event::new("OtherType").with_field("x", 200i64),
    ];

    let results = varpulis_cli::simulate_from_source(vpl, events)
        .await
        .unwrap();
    // No events match
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_simulate_large_batch() {
    let vpl = r"
        stream Out = Tick
            .where(value > 50)
            .emit(v: value)
    ";

    let events: Vec<Event> = (0..1000)
        .map(|i| Event::new("Tick").with_field("value", i as i64))
        .collect();

    let results = varpulis_cli::simulate_from_source(vpl, events)
        .await
        .unwrap();
    // 51..999 = 949 events pass the filter
    assert_eq!(results.len(), 949);
}

// =============================================================================
// Library function edge cases
// =============================================================================

#[test]
fn test_parse_program_multiple_statements() {
    let source = r"
        event A:
            x: int

        event B:
            y: float

        stream S1 = A .where(x > 0)
        stream S2 = B .where(y > 0.0)
    ";
    let count = varpulis_cli::parse_program(source).unwrap();
    assert_eq!(count, 4);
}

#[test]
fn test_parse_program_with_function() {
    let source = r"
        fn add(a: int, b: int) -> int:
            a + b

        stream S = E .emit(result: add(x, y))
    ";
    let count = varpulis_cli::parse_program(source).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_validate_program_with_merge() {
    let source = r"
        stream A = Events .where(x > 0) .emit(v: x)
        stream B = Events .where(y > 0) .emit(v: y)
        stream C = merge(A, B) .emit(value: v)
    ";
    let result = varpulis_cli::validate_program(source);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3);
}

#[test]
fn test_check_syntax_connector_with_options() {
    let source = r#"
        connector kafka_in = kafka(brokers: "localhost:9092", topic: "events", group: "cep")
    "#;
    assert!(varpulis_cli::check_syntax(source).is_ok());
}

#[test]
fn test_check_syntax_multiple_connectors() {
    let source = r#"
        connector mqtt_in = mqtt(broker: "localhost:1883", topic: "sensors/#")
        connector kafka_out = kafka(brokers: "localhost:9092", topic: "alerts")

        stream Alert = TempReading
            .where(temperature > 30)
            .emit(alert: "hot")
    "#;
    assert!(varpulis_cli::check_syntax(source).is_ok());
}

// =============================================================================
// Duration parsing
// =============================================================================

#[test]
fn test_parse_duration_seconds() {
    assert_eq!(varpulis_cli::parse_duration_str("60s").unwrap(), 60);
    assert_eq!(varpulis_cli::parse_duration_str("1s").unwrap(), 1);
    assert_eq!(varpulis_cli::parse_duration_str("0s").unwrap(), 0);
}

#[test]
fn test_parse_duration_minutes() {
    assert_eq!(varpulis_cli::parse_duration_str("5m").unwrap(), 300);
    assert_eq!(varpulis_cli::parse_duration_str("1m").unwrap(), 60);
}

#[test]
fn test_parse_duration_hours() {
    assert_eq!(varpulis_cli::parse_duration_str("1h").unwrap(), 3600);
    assert_eq!(varpulis_cli::parse_duration_str("2h").unwrap(), 7200);
}

#[test]
fn test_parse_duration_no_suffix() {
    assert_eq!(varpulis_cli::parse_duration_str("120").unwrap(), 120);
}

#[test]
fn test_parse_duration_with_whitespace() {
    assert_eq!(varpulis_cli::parse_duration_str("  30s  ").unwrap(), 30);
}

#[test]
fn test_parse_duration_empty() {
    assert!(varpulis_cli::parse_duration_str("").is_err());
}

#[test]
fn test_parse_duration_invalid_number() {
    assert!(varpulis_cli::parse_duration_str("abcs").is_err());
}

// =============================================================================
// Config utilities
// =============================================================================

#[test]
fn test_config_defaults() {
    let config = Config::default();
    assert_eq!(config.server.port, 9000);
}

#[test]
fn test_config_example_yaml() {
    let yaml = Config::example_yaml();
    assert!(yaml.contains("server"));
    assert!(yaml.contains("port"));
}

#[test]
fn test_config_example_toml() {
    let toml = Config::example_toml();
    assert!(toml.contains("[server]"));
    assert!(toml.contains("port"));
}

#[test]
fn test_config_yaml_roundtrip() {
    let yaml = Config::example_yaml();
    let config = Config::from_yaml(&yaml).unwrap();
    assert!(config.server.port > 0);
}

#[test]
fn test_config_toml_roundtrip() {
    let toml_str = Config::example_toml();
    let config = Config::from_toml(&toml_str).unwrap();
    assert!(config.server.port > 0);
}

#[test]
fn test_config_merge_overrides_port() {
    let mut base = Config::default();
    let mut overlay = Config::default();
    overlay.server.port = 8080;
    base.merge(overlay);
    assert_eq!(base.server.port, 8080);
}

// =============================================================================
// Billing utilities
// =============================================================================

#[test]
fn test_billing_config_price_id_for_tier() {
    let config = BillingConfig {
        stripe_secret_key: "sk_test".to_string(),
        stripe_webhook_secret: "whsec".to_string(),
        pro_price_id: "price_pro".to_string(),
        business_price_id: "price_biz".to_string(),
        frontend_url: "http://localhost".to_string(),
    };

    assert_eq!(config.price_id_for_tier(&Tier::Pro), Some("price_pro"));
    assert_eq!(config.price_id_for_tier(&Tier::Business), Some("price_biz"));
    assert_eq!(config.price_id_for_tier(&Tier::Free), None);
    assert_eq!(config.price_id_for_tier(&Tier::Enterprise), None);
}

#[test]
fn test_billing_config_tier_for_price_id() {
    let config = BillingConfig {
        stripe_secret_key: "sk_test".to_string(),
        stripe_webhook_secret: "whsec".to_string(),
        pro_price_id: "price_pro".to_string(),
        business_price_id: "price_biz".to_string(),
        frontend_url: "http://localhost".to_string(),
    };

    assert_eq!(config.tier_for_price_id("price_pro"), Some(Tier::Pro));
    assert_eq!(config.tier_for_price_id("price_biz"), Some(Tier::Business));
    assert_eq!(config.tier_for_price_id("unknown"), None);
}

#[test]
fn test_billing_config_empty_price_ids() {
    let config = BillingConfig {
        stripe_secret_key: "sk_test".to_string(),
        stripe_webhook_secret: "".to_string(),
        pro_price_id: "".to_string(),
        business_price_id: "".to_string(),
        frontend_url: "http://localhost".to_string(),
    };

    assert_eq!(config.price_id_for_tier(&Tier::Pro), None);
    assert_eq!(config.price_id_for_tier(&Tier::Business), None);
    assert_eq!(config.tier_for_price_id(""), None);
}

#[test]
fn test_usage_tracker_monthly_totals_and_limits() {
    let mut tracker = UsageTracker::new();
    let org = uuid::Uuid::new_v4();

    tracker.set_monthly_total(org, 50_000);
    tracker.set_monthly_limit(org, 100_000);

    assert_eq!(tracker.get_monthly_total(&org), Some(50_000));
    assert_eq!(tracker.get_monthly_limit(&org), Some(100_000));
}

#[test]
fn test_usage_tracker_check_cached_limit() {
    let mut tracker = UsageTracker::new();
    let org = uuid::Uuid::new_v4();

    tracker.set_monthly_total(org, 50_000);
    tracker.set_monthly_limit(org, 100_000);

    // Within limit
    assert_eq!(tracker.check_cached_limit(&org, 10_000), Some(true));
    // Exactly at limit
    assert_eq!(tracker.check_cached_limit(&org, 50_000), Some(true));
    // Over limit
    assert_eq!(tracker.check_cached_limit(&org, 50_001), Some(false));
}

#[test]
fn test_usage_tracker_check_cached_limit_with_buffer() {
    let mut tracker = UsageTracker::new();
    let org = uuid::Uuid::new_v4();

    tracker.set_monthly_total(org, 80_000);
    tracker.set_monthly_limit(org, 100_000);
    tracker.record_events(org, 15_000); // buffer: 15K

    // 80K (total) + 15K (buffer) + 6K (additional) = 101K > 100K
    assert_eq!(tracker.check_cached_limit(&org, 6_000), Some(false));
    // 80K + 15K + 4K = 99K < 100K
    assert_eq!(tracker.check_cached_limit(&org, 4_000), Some(true));
}

#[test]
fn test_usage_tracker_check_cached_limit_cache_miss() {
    let tracker = UsageTracker::new();
    let org = uuid::Uuid::new_v4();

    // No cached data
    assert_eq!(tracker.check_cached_limit(&org, 100), None);
}

#[test]
fn test_tier_display() {
    assert_eq!(format!("{}", Tier::Free), "free");
    assert_eq!(format!("{}", Tier::Pro), "pro");
    assert_eq!(format!("{}", Tier::Business), "business");
    assert_eq!(format!("{}", Tier::Enterprise), "enterprise");
}

// =============================================================================
// OAuth utility tests
// =============================================================================

#[test]
fn test_oauth_session_store_cleanup() {
    let mut store = varpulis_cli::oauth::SessionStore::new();
    store.revoke("hash1".to_string());
    store.revoke("hash2".to_string());

    assert!(store.is_revoked("hash1"));
    assert!(store.is_revoked("hash2"));
    assert!(!store.is_revoked("hash3"));

    // Cleanup shouldn't remove recent entries
    store.cleanup();
    assert!(store.is_revoked("hash1"));
}

#[test]
fn test_oauth_extract_jwt_from_cookie_multiple() {
    // Multiple cookies
    assert_eq!(
        varpulis_cli::oauth::extract_jwt_from_cookie(
            "other=x; varpulis_session=my-jwt-token; theme=dark"
        ),
        Some("my-jwt-token".to_string())
    );
}

#[test]
fn test_oauth_extract_jwt_from_cookie_missing() {
    assert_eq!(
        varpulis_cli::oauth::extract_jwt_from_cookie("other=value"),
        None
    );
}

#[test]
fn test_oauth_extract_jwt_from_cookie_empty_value() {
    assert_eq!(
        varpulis_cli::oauth::extract_jwt_from_cookie("varpulis_session="),
        None
    );
}

#[test]
fn test_oauth_token_hash_is_hex_sha256() {
    let hash = varpulis_cli::oauth::token_hash("test-token");
    // SHA-256 produces 64 hex chars
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
}

// =============================================================================
// Client construction
// =============================================================================

#[test]
fn test_client_construction() {
    let client = varpulis_cli::client::VarpulisClient::new("http://localhost:9000/", "my-key");
    assert_eq!(client.api_key(), "my-key");
    // Base URL should have trailing slash stripped
    assert_eq!(
        client.logs_url("pipe-1"),
        "http://localhost:9000/api/v1/pipelines/pipe-1/logs"
    );
}

#[test]
fn test_client_logs_url() {
    let client = varpulis_cli::client::VarpulisClient::new("https://api.varpulis.io", "key");
    assert_eq!(
        client.logs_url("abc-123"),
        "https://api.varpulis.io/api/v1/pipelines/abc-123/logs"
    );
}

// =============================================================================
// Security edge cases
// =============================================================================

#[test]
fn test_security_error_display_rate_limit() {
    let err = varpulis_cli::security::SecurityError::RateLimitExceeded {
        ip: "10.0.0.1".to_string(),
    };
    let msg = format!("{err}");
    assert!(msg.contains("Rate limit"));
    assert!(msg.contains("10.0.0.1"));
}

#[test]
fn test_security_sanitize_filename_empty() {
    let result = varpulis_cli::security::sanitize_filename("");
    assert!(result.is_empty());
}

#[test]
fn test_security_sanitize_filename_only_dots() {
    let result = varpulis_cli::security::sanitize_filename("...");
    assert!(result.is_empty());
}

#[test]
fn test_security_is_suspicious_path_safe_nested() {
    assert!(!varpulis_cli::security::is_suspicious_path("a/b/c/d.vpl"));
}

// =============================================================================
// Auth edge cases
// =============================================================================

#[test]
fn test_auth_extract_from_header_bearer_with_tab() {
    let result = varpulis_cli::auth::extract_from_header("Bearer\tmy-key");
    assert_eq!(result, Ok("my-key".to_string()));
}

#[test]
fn test_auth_extract_from_header_apikey_with_tab() {
    let result = varpulis_cli::auth::extract_from_header("ApiKey\tmy-key");
    assert_eq!(result, Ok("my-key".to_string()));
}

#[test]
fn test_auth_extract_from_header_bearer_alone() {
    let result = varpulis_cli::auth::extract_from_header("Bearer");
    assert!(result.is_err());
}

#[test]
fn test_auth_extract_from_header_apikey_alone() {
    let result = varpulis_cli::auth::extract_from_header("ApiKey");
    assert!(result.is_err());
}

#[test]
fn test_auth_api_key_getter() {
    let config = varpulis_cli::auth::AuthConfig::with_api_key("secret".to_string());
    assert_eq!(config.api_key(), Some("secret"));

    let disabled = varpulis_cli::auth::AuthConfig::disabled();
    assert_eq!(disabled.api_key(), None);
}

// =============================================================================
// Audit edge cases
// =============================================================================

#[test]
fn test_audit_entry_without_optional_fields() {
    let entry = varpulis_cli::audit::AuditEntry::new(
        "system",
        varpulis_cli::audit::AuditAction::StreamStart,
        "pipeline-1",
    );
    let json = serde_json::to_string(&entry).unwrap();
    // No ip or detail in JSON when not set
    assert!(!json.contains("\"ip\""));
    assert!(!json.contains("\"detail\""));
}

#[test]
fn test_audit_entry_all_actions_serialize() {
    use varpulis_cli::audit::AuditAction;

    let actions = vec![
        AuditAction::Login,
        AuditAction::Logout,
        AuditAction::TokenRefresh,
        AuditAction::PipelineDeploy,
        AuditAction::PipelineDelete,
        AuditAction::PipelineUpdate,
        AuditAction::StreamStart,
        AuditAction::StreamStop,
        AuditAction::ApiKeyCreate,
        AuditAction::ApiKeyDelete,
        AuditAction::TierChange,
        AuditAction::CheckoutStarted,
        AuditAction::WebhookReceived,
        AuditAction::SettingsChange,
        AuditAction::AdminAccess,
        AuditAction::UserCreate,
        AuditAction::UserUpdate,
        AuditAction::UserDelete,
        AuditAction::UserDisable,
        AuditAction::PasswordChange,
        AuditAction::SessionRenew,
        AuditAction::MaxSessionsExceeded,
    ];

    for action in actions {
        let entry = varpulis_cli::audit::AuditEntry::new("test", action, "target");
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"action\""));
    }
}

// =============================================================================
// Users/Sessions edge cases
// =============================================================================

#[test]
fn test_session_needs_renewal_expired() {
    let config = varpulis_cli::users::SessionConfig {
        renewal_window: std::time::Duration::from_mins(5),
        absolute_timeout: std::time::Duration::from_secs(1), // very short
        ..Default::default()
    };
    let mut mgr = varpulis_cli::users::SessionManager::new(config);
    let session = mgr.create_session("user-1", "bob", "viewer");

    // Just created, so not yet in renewal window (absolute timeout is 1s, renewal 5min)
    // Actually with 1s absolute and 5min renewal, needs_renewal should be true immediately
    // because time_remaining (1s) <= renewal_window (300s)
    assert!(mgr.needs_renewal(&session.session_id));
}

#[test]
fn test_session_needs_renewal_nonexistent() {
    let mgr = varpulis_cli::users::SessionManager::new(Default::default());
    assert!(!mgr.needs_renewal("nonexistent-session"));
}

#[test]
fn test_session_cleanup_expired() {
    let config = varpulis_cli::users::SessionConfig {
        absolute_timeout: std::time::Duration::from_millis(1), // expire immediately
        idle_timeout: std::time::Duration::from_millis(1),
        ..Default::default()
    };
    let mut mgr = varpulis_cli::users::SessionManager::new(config);
    mgr.create_session("user-1", "alice", "admin");
    mgr.create_session("user-2", "bob", "viewer");

    // Wait for expiry
    std::thread::sleep(std::time::Duration::from_millis(5));

    let cleaned = mgr.cleanup_expired();
    assert_eq!(cleaned, 2);
}

#[test]
fn test_password_hash_different_for_same_password() {
    // Each hash should use a unique salt
    let h1 = varpulis_cli::users::hash_password("same-password").unwrap();
    let h2 = varpulis_cli::users::hash_password("same-password").unwrap();
    assert_ne!(h1, h2);
    // But both should verify
    assert!(varpulis_cli::users::verify_password("same-password", &h1).unwrap());
    assert!(varpulis_cli::users::verify_password("same-password", &h2).unwrap());
}

#[test]
fn test_verify_password_invalid_hash() {
    let result = varpulis_cli::users::verify_password("password", "not-a-valid-hash");
    assert!(result.is_err());
}

#[test]
fn test_user_summary_serialization() {
    let summary = varpulis_cli::users::UserSummary {
        id: "u-1".to_string(),
        username: "alice".to_string(),
        display_name: "Alice".to_string(),
        email: "alice@test.com".to_string(),
        role: "admin".to_string(),
        disabled: false,
        created_at: chrono::Utc::now(),
    };
    let json = serde_json::to_value(&summary).unwrap();
    assert_eq!(json["username"], "alice");
    assert_eq!(json["disabled"], false);
}
