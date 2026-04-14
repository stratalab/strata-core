//! Strata API Tests
//!
//! Tests for the high-level Strata typed wrapper API.
//! The Strata struct provides a convenient Rust API that wraps the
//! Executor's command-based interface with typed method calls.

use crate::common::*;
use strata_core::Value;
use strata_executor::DistanceMetric;

// ============================================================================
// Database Operations
// ============================================================================

#[test]
fn ping_returns_version() {
    let db = create_strata();

    let version = db.ping().unwrap();

    assert!(!version.is_empty());
}

#[test]
fn info_returns_database_info() {
    let db = create_strata();

    let info = db.info().unwrap();

    assert!(!info.version.is_empty());
    assert!(info.total_keys > 0, "should count system branch entries");
    // branch_count is user-visible branches; default branch is lazy-created
}

#[test]
fn health_returns_healthy_for_cache_db() {
    let db = create_strata();

    let report = db.health().unwrap();

    assert_eq!(report.status, strata_engine::SubsystemStatus::Healthy);
    assert_eq!(report.subsystems.len(), 6);
    // WAL message should indicate ephemeral
    let wal = report.subsystems.iter().find(|s| s.name == "wal").unwrap();
    assert!(
        wal.message.as_deref().unwrap().contains("ephemeral"),
        "WAL message should mention ephemeral, got: {:?}",
        wal.message
    );
}

#[test]
fn health_info_total_keys_increases_after_writes() {
    let db = create_strata();

    let before = db.info().unwrap().total_keys;
    db.kv_put("k1", "v1").unwrap();
    db.kv_put("k2", "v2").unwrap();
    let after = db.info().unwrap().total_keys;

    assert!(
        after >= before + 2,
        "total_keys should increase by at least 2 after writing 2 keys: before={}, after={}",
        before,
        after
    );
}

#[test]
fn metrics_returns_unified_snapshot() {
    let db = create_strata();

    let m = db.metrics().unwrap();

    assert!(m.uptime_secs <= 1);
    assert!(m.scheduler.worker_count >= 1);
    assert!(m.storage.total_branches >= 1);
    // Ephemeral has no WAL or disk
    assert!(m.wal_counters.is_none());
    assert!(m.available_disk_bytes.is_none());
}

#[test]
fn flush_succeeds() {
    let db = create_strata();

    db.flush().unwrap();
}

#[test]
fn compact_succeeds_on_ephemeral() {
    let db = create_strata();

    // compact() on an ephemeral database is a no-op
    assert!(db.compact().is_ok());
}

// ============================================================================
// KV Operations
// ============================================================================

#[test]
fn kv_put_get_cycle() {
    let db = create_strata();

    let version = db.kv_put("key1", Value::String("hello".into())).unwrap();
    assert!(version > 0);

    let value = db.kv_get("key1").unwrap();
    assert!(value.is_some());
    assert_eq!(value.unwrap(), Value::String("hello".into()));
}

#[test]
fn kv_delete() {
    let db = create_strata();

    db.kv_put("key1", Value::Int(42)).unwrap();
    assert_eq!(db.kv_get("key1").unwrap(), Some(Value::Int(42)));

    db.kv_delete("key1").unwrap();
    assert!(db.kv_get("key1").unwrap().is_none());
}

#[test]
fn kv_list_with_prefix() {
    let db = create_strata();

    db.kv_put("user:1", Value::Int(1)).unwrap();
    db.kv_put("user:2", Value::Int(2)).unwrap();
    db.kv_put("order:1", Value::Int(3)).unwrap();

    let user_keys = db.kv_list(Some("user:")).unwrap();
    assert_eq!(user_keys.len(), 2);

    let order_keys = db.kv_list(Some("order:")).unwrap();
    assert_eq!(order_keys.len(), 1);
}

// ============================================================================
// Event Operations
// ============================================================================

#[test]
fn event_append_and_get_by_type() {
    let db = create_strata();

    // Event payloads must be Objects
    db.event_append("stream", event_payload("value", Value::Int(1)))
        .unwrap();
    db.event_append("stream", event_payload("value", Value::Int(2)))
        .unwrap();

    let events = db.event_get_by_type("stream").unwrap();
    assert_eq!(events.len(), 2);
}

#[test]
fn event_len() {
    let db = create_strata();

    for i in 0..5 {
        db.event_append("counting", event_payload("n", Value::Int(i)))
            .unwrap();
    }

    let len = db.event_len().unwrap();
    assert_eq!(len, 5);
}

// ============================================================================
// Vector Operations
// ============================================================================

#[test]
fn vector_create_collection_and_upsert() {
    let db = create_strata();

    db.vector_create_collection("vecs", 4u64, DistanceMetric::Cosine)
        .unwrap();
    db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0, 0.0], None)
        .unwrap();

    let vector = db.vector_get("vecs", "v1").unwrap();
    assert!(vector.is_some());
}

#[test]
fn vector_query() {
    let db = create_strata();

    db.vector_create_collection("search", 4u64, DistanceMetric::Cosine)
        .unwrap();
    db.vector_upsert("search", "v1", vec![1.0, 0.0, 0.0, 0.0], None)
        .unwrap();
    db.vector_upsert("search", "v2", vec![0.0, 1.0, 0.0, 0.0], None)
        .unwrap();

    let matches = db
        .vector_query("search", vec![1.0, 0.0, 0.0, 0.0], 10u64)
        .unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0].key, "v1");
}

#[test]
fn vector_list_collections() {
    let db = create_strata();

    db.vector_create_collection("coll_a", 4u64, DistanceMetric::Cosine)
        .unwrap();
    db.vector_create_collection("coll_b", 8u64, DistanceMetric::Euclidean)
        .unwrap();

    let collections = db.vector_list_collections().unwrap();
    assert_eq!(collections.len(), 2);
}

#[test]
fn vector_delete_collection() {
    let db = create_strata();

    db.vector_create_collection("to_delete", 4u64, DistanceMetric::Cosine)
        .unwrap();

    // Verify it exists by listing
    let collections = db.vector_list_collections().unwrap();
    assert!(collections.iter().any(|c| c.name == "to_delete"));

    db.vector_delete_collection("to_delete").unwrap();

    // Verify it's gone
    let collections = db.vector_list_collections().unwrap();
    assert!(!collections.iter().any(|c| c.name == "to_delete"));
}

// ============================================================================
// Branch Operations
// ============================================================================

#[test]
fn branch_create_and_get() {
    let db = create_strata();

    // Users can name branches like git branches
    db.branches().create("my-agent-branch").unwrap();

    let branch_info = db.branches().info("my-agent-branch").unwrap();
    assert!(branch_info.is_some());
    assert_eq!(branch_info.unwrap().info.id.as_str(), "my-agent-branch");
}

#[test]
fn branch_list() {
    let db = create_strata();

    db.branches().create("dev").unwrap();
    db.branches().create("prod").unwrap();

    let branches = db.branches().list().unwrap();
    // At least our two branches plus default
    assert!(
        branches.len() >= 2,
        "Expected >= 2 branches (dev + prod), got {}",
        branches.len()
    );
}

// ============================================================================
// JSON Operations
// ============================================================================

#[test]
fn json_set_and_get() {
    let db = create_strata();

    let doc = Value::object(
        [
            ("name".to_string(), Value::String("Alice".into())),
            ("age".to_string(), Value::Int(30)),
        ]
        .into_iter()
        .collect(),
    );

    db.json_set("user:1", "$", doc).unwrap();

    let result = db.json_get("user:1", "$").unwrap();
    assert!(result.is_some());

    let value = result.unwrap();
    match &value {
        Value::Object(map) => {
            assert_eq!(map.get("name"), Some(&Value::String("Alice".into())));
        }
        _ => panic!("Expected Object"),
    }
}

#[test]
fn json_delete() {
    let db = create_strata();

    let doc = Value::object([("key".to_string(), Value::Int(1))].into_iter().collect());

    db.json_set("doc1", "$", doc).unwrap();

    // Verify it exists
    let result = db.json_get("doc1", "$").unwrap();
    assert!(result.is_some());

    db.json_delete("doc1", "$").unwrap();

    // Verify it's gone
    let result = db.json_get("doc1", "$").unwrap();
    assert!(result.is_none());
}

// ============================================================================
// Cross-Primitive Usage
// ============================================================================

#[test]
fn use_all_primitives() {
    let db = create_strata();

    // KV
    db.kv_put("config", Value::String("enabled".into()))
        .unwrap();

    // Event
    db.event_append(
        "audit",
        event_payload("action", Value::String("start".into())),
    )
    .unwrap();

    // Vector
    db.vector_create_collection("embeddings", 4u64, DistanceMetric::Cosine)
        .unwrap();
    db.vector_upsert("embeddings", "e1", vec![1.0, 0.0, 0.0, 0.0], None)
        .unwrap();

    // JSON
    let doc = Value::object(
        [("type".to_string(), Value::String("test".into()))]
            .into_iter()
            .collect(),
    );
    db.json_set("doc1", "$", doc).unwrap();

    // Branch
    db.branches().create("integration-test").unwrap();

    // Verify all data
    assert_eq!(
        db.kv_get("config").unwrap(),
        Some(Value::String("enabled".into()))
    );
    assert_eq!(db.event_len().unwrap(), 1);
    let collections = db.vector_list_collections().unwrap();
    assert!(collections.iter().any(|c| c.name == "embeddings"));
    assert!(db.json_get("doc1", "$").unwrap().is_some());
    assert!(db.branches().info("integration-test").unwrap().is_some());
}

// ============================================================================
// Session Access
// ============================================================================

#[test]
fn session_from_strata() {
    // Use Strata::cache() directly for ephemeral test database
    let strata = strata_executor::Strata::cache().unwrap();

    // Session can be created from the strata instance
    let _session = strata.session();
}
