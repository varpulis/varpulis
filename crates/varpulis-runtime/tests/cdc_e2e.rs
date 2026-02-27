//! End-to-end integration tests for the PostgreSQL CDC connector.
//!
//! These tests require a real PostgreSQL instance with `wal_level=logical` running
//! at `localhost:5432`. In CI this is provided by a `services: postgres` container;
//! locally run `./tests/integration/run_cdc_tests.sh --local` to start one.
//!
//! Each test uses a UUID-based replication slot name to avoid cross-test interference.

#![cfg(feature = "cdc")]

use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use varpulis_core::Value;
use varpulis_runtime::connector::postgres_cdc::{PostgresCdcConfig, PostgresCdcSource};
use varpulis_runtime::connector::SourceConnector;

/// Database connection parameters from environment (with defaults for Docker test setup).
fn db_host() -> String {
    std::env::var("CDC_TEST_HOST").unwrap_or_else(|_| "localhost".to_string())
}

fn db_port() -> u16 {
    std::env::var("CDC_TEST_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5432)
}

const DB_NAME: &str = "cdc_test";
const DB_USER: &str = "varpulis";
const DB_PASS: &str = "testpass";

/// Helper: connect a raw tokio-postgres client for DML operations in tests.
async fn raw_client() -> tokio_postgres::Client {
    let conn_string = format!(
        "host={} port={} dbname={} user={} password={}",
        db_host(),
        db_port(),
        DB_NAME,
        DB_USER,
        DB_PASS
    );
    let (client, connection) = tokio_postgres::connect(&conn_string, tokio_postgres::NoTls)
        .await
        .expect("Failed to connect to PostgreSQL — is it running with wal_level=logical?");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });

    client
}

/// Helper: unique replication slot name per test to avoid interference.
fn unique_slot() -> String {
    format!("test_{}", Uuid::new_v4().simple())
}

/// Helper: build a CDC config targeting the test database.
fn test_config(slot_name: &str) -> PostgresCdcConfig {
    PostgresCdcConfig::new(&db_host(), DB_NAME)
        .with_port(db_port())
        .with_credentials(DB_USER, DB_PASS)
        .with_slot(slot_name)
        .with_publication("varpulis_pub")
}

/// Helper: drop a replication slot (cleanup).
async fn drop_slot(client: &tokio_postgres::Client, slot_name: &str) {
    let _ = client
        .simple_query(&format!(
            "SELECT pg_drop_replication_slot('{}');",
            slot_name
        ))
        .await;
}

// ============================================================================
// Test 1: CDC source receives INSERT events
// ============================================================================

#[tokio::test]
async fn test_cdc_source_receives_inserts() {
    let slot = unique_slot();
    let config = test_config(&slot);
    let mut source = PostgresCdcSource::new("cdc-insert-test", config);

    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();

    // Give the CDC polling loop time to start and create the slot
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert rows via raw client
    let client = raw_client().await;
    client
        .execute(
            "INSERT INTO orders (customer, amount, status) VALUES ($1, $2, $3)",
            &[&"alice", &99.99_f64, &"pending"],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO orders (customer, amount, status) VALUES ($1, $2, $3)",
            &[&"bob", &250.00_f64, &"confirmed"],
        )
        .await
        .unwrap();

    // Receive events (CDC polls every 100ms, allow up to 10s)
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while received.len() < 2 {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(event)) => received.push(event),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert!(
        received.len() >= 2,
        "Expected at least 2 INSERT events, got {}",
        received.len()
    );

    // Verify event structure
    for event in &received {
        assert_eq!(event.get("_table"), Some(&Value::Str("orders".into())));
        assert_eq!(event.get("_op"), Some(&Value::Str("INSERT".into())));
        assert!(
            event.event_type.as_ref().contains("orders"),
            "event_type should contain 'orders'"
        );
    }

    source.stop().await.unwrap();
    drop_slot(&client, &slot).await;
}

// ============================================================================
// Test 2: CDC source receives UPDATE events
// ============================================================================

#[tokio::test]
async fn test_cdc_source_receives_updates() {
    let slot = unique_slot();
    let config = test_config(&slot);
    let mut source = PostgresCdcSource::new("cdc-update-test", config);

    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = raw_client().await;

    // Insert a row first, then update it
    client
        .execute(
            "INSERT INTO orders (customer, amount, status) VALUES ($1, $2, $3)",
            &[&"charlie", &100.00_f64, &"pending"],
        )
        .await
        .unwrap();

    // Wait for the insert to be captured
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Update the row
    client
        .execute(
            "UPDATE orders SET amount = $1, status = $2 WHERE customer = $3",
            &[&200.00_f64, &"confirmed", &"charlie"],
        )
        .await
        .unwrap();

    // Collect events (INSERT + UPDATE)
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while received.len() < 2 {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(event)) => received.push(event),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert!(
        received.len() >= 2,
        "Expected at least 2 events (INSERT+UPDATE), got {}",
        received.len()
    );

    // Find the UPDATE event
    let update_event = received
        .iter()
        .find(|e| e.get("_op") == Some(&Value::Str("UPDATE".into())));
    assert!(
        update_event.is_some(),
        "Should have received an UPDATE event"
    );

    source.stop().await.unwrap();
    drop_slot(&client, &slot).await;
}

// ============================================================================
// Test 3: CDC source receives DELETE events
// ============================================================================

#[tokio::test]
async fn test_cdc_source_receives_deletes() {
    let slot = unique_slot();
    let config = test_config(&slot);
    let mut source = PostgresCdcSource::new("cdc-delete-test", config);

    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = raw_client().await;

    // Insert then delete
    client
        .execute(
            "INSERT INTO orders (customer, amount, status) VALUES ($1, $2, $3)",
            &[&"dave", &50.00_f64, &"pending"],
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    client
        .execute("DELETE FROM orders WHERE customer = $1", &[&"dave"])
        .await
        .unwrap();

    // Collect events (INSERT + DELETE)
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while received.len() < 2 {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(event)) => received.push(event),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert!(
        received.len() >= 2,
        "Expected at least 2 events (INSERT+DELETE), got {}",
        received.len()
    );

    let delete_event = received
        .iter()
        .find(|e| e.get("_op") == Some(&Value::Str("DELETE".into())));
    assert!(
        delete_event.is_some(),
        "Should have received a DELETE event"
    );

    source.stop().await.unwrap();
    drop_slot(&client, &slot).await;
}

// ============================================================================
// Test 4: CDC event format validation
// ============================================================================

#[tokio::test]
async fn test_cdc_event_format() {
    let slot = unique_slot();
    let config = test_config(&slot);
    let mut source = PostgresCdcSource::new("cdc-format-test", config);

    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = raw_client().await;
    client
        .execute(
            "INSERT INTO orders (customer, amount, status) VALUES ($1, $2, $3)",
            &[&"eve", &75.50_f64, &"pending"],
        )
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("timeout waiting for CDC event")
        .expect("channel closed");

    // Verify event_type format: "{table}.{OP}"
    let event_type = event.event_type.as_ref();
    assert!(
        event_type.starts_with("orders."),
        "event_type should start with 'orders.', got '{}'",
        event_type
    );

    // Verify metadata fields
    assert_eq!(
        event.get("_table"),
        Some(&Value::Str("orders".into())),
        "_table field should be 'orders'"
    );
    assert_eq!(
        event.get("_op"),
        Some(&Value::Str("INSERT".into())),
        "_op field should be 'INSERT'"
    );

    // Verify data fields exist
    assert!(
        event.get("customer").is_some() || event.data.len() > 2,
        "Event should contain data fields beyond _table and _op"
    );

    source.stop().await.unwrap();
    drop_slot(&client, &slot).await;
}

// ============================================================================
// Test 5: CDC multi-table routing
// ============================================================================

#[tokio::test]
async fn test_cdc_multi_table() {
    let slot = unique_slot();
    let config = test_config(&slot);
    let mut source = PostgresCdcSource::new("cdc-multi-table-test", config);

    let (tx, mut rx) = mpsc::channel(64);
    source.start(tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = raw_client().await;

    // Insert into both tables
    client
        .execute(
            "INSERT INTO orders (customer, amount, status) VALUES ($1, $2, $3)",
            &[&"frank", &300.00_f64, &"pending"],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO products (name, price, stock) VALUES ($1, $2, $3)",
            &[&"Widget", &19.99_f64, &100_i32],
        )
        .await
        .unwrap();

    // Collect events from both tables
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while received.len() < 2 {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(event)) => received.push(event),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert!(
        received.len() >= 2,
        "Expected at least 2 events from 2 tables, got {}",
        received.len()
    );

    // Verify events come from different tables
    let tables: Vec<_> = received
        .iter()
        .filter_map(|e| match e.get("_table") {
            Some(Value::Str(s)) => Some(s.to_string()),
            _ => None,
        })
        .collect();

    let has_orders = tables.iter().any(|t| t == "orders");
    let has_products = tables.iter().any(|t| t == "products");

    assert!(
        has_orders,
        "Should have received events from 'orders' table"
    );
    assert!(
        has_products,
        "Should have received events from 'products' table"
    );

    source.stop().await.unwrap();
    drop_slot(&client, &slot).await;
}

// ============================================================================
// Test 6: CDC throughput measurement
// ============================================================================

#[tokio::test]
async fn test_cdc_throughput() {
    let slot = unique_slot();
    let config = test_config(&slot);
    let mut source = PostgresCdcSource::new("cdc-throughput-test", config);

    let (tx, mut rx) = mpsc::channel(16384);
    source.start(tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = raw_client().await;

    // Bulk insert 1000 rows in a single transaction
    let n = 1000_usize;
    let start = std::time::Instant::now();

    client.execute("BEGIN", &[]).await.unwrap();
    for i in 0..n {
        client
            .execute(
                "INSERT INTO orders (customer, amount, status) VALUES ($1, $2, $3)",
                &[&format!("customer_{}", i), &((i as f64) * 1.5), &"pending"],
            )
            .await
            .unwrap();
    }
    client.execute("COMMIT", &[]).await.unwrap();
    let insert_elapsed = start.elapsed();

    // Receive all events
    let mut received = 0_usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while received < n {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(_)) => received += 1,
            Ok(None) => break,
            Err(_) => break,
        }
    }
    let total_elapsed = start.elapsed();

    let events_per_sec = if total_elapsed.as_secs_f64() > 0.0 {
        received as f64 / total_elapsed.as_secs_f64()
    } else {
        0.0
    };

    eprintln!("CDC Throughput Results:");
    eprintln!("  Rows inserted: {}", n);
    eprintln!("  Events received: {}", received);
    eprintln!("  Insert time: {:?}", insert_elapsed);
    eprintln!("  Total time: {:?}", total_elapsed);
    eprintln!("  Throughput: {:.0} events/sec", events_per_sec);

    assert!(
        received >= n / 2,
        "Expected at least {} events, got {} (throughput: {:.0} evt/s)",
        n / 2,
        received,
        events_per_sec
    );

    source.stop().await.unwrap();
    drop_slot(&client, &slot).await;
}
