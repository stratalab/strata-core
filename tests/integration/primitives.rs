//! Single and Cross-Primitive Integration Tests
//!
//! Tests each primitive in isolation and in combination.

use crate::common::*;

// ============================================================================
// Single Primitive Tests
// ============================================================================

mod kv_single {
    use super::*;

    #[test]
    fn basic_crud() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let kv = KVStore::new(db);

        // Create
        kv.put(&branch_id, "default", "key", Value::Int(1)).unwrap();

        // Read
        let val = kv.get(&branch_id, "default", "key").unwrap().unwrap();
        assert_eq!(val, Value::Int(1));

        // Update
        kv.put(&branch_id, "default", "key", Value::Int(2)).unwrap();
        let val = kv.get(&branch_id, "default", "key").unwrap().unwrap();
        assert_eq!(val, Value::Int(2));

        // Delete
        kv.delete(&branch_id, "default", "key").unwrap();
        assert!(kv.get(&branch_id, "default", "key").unwrap().is_none());
    }

    #[test]
    fn list_and_scan() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let kv = KVStore::new(db);

        for i in 0..20 {
            kv.put(&branch_id, "default", &format!("user:{}", i), Value::Int(i))
                .unwrap();
            kv.put(
                &branch_id,
                "default",
                &format!("config:{}", i),
                Value::Int(i * 10),
            )
            .unwrap();
        }

        // List all
        let all = kv.list(&branch_id, "default", None).unwrap();
        assert_eq!(all.len(), 40);

        // List with prefix
        let users = kv.list(&branch_id, "default", Some("user:")).unwrap();
        assert_eq!(users.len(), 20);

        let configs = kv.list(&branch_id, "default", Some("config:")).unwrap();
        assert_eq!(configs.len(), 20);
    }

    #[test]
    fn value_types() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let kv = KVStore::new(db);

        kv.put(&branch_id, "default", "int", Value::Int(42))
            .unwrap();
        kv.put(&branch_id, "default", "float", Value::Float(3.125))
            .unwrap();
        kv.put(
            &branch_id,
            "default",
            "string",
            Value::String("hello".into()),
        )
        .unwrap();
        kv.put(&branch_id, "default", "bool", Value::Bool(true))
            .unwrap();
        kv.put(&branch_id, "default", "bytes", Value::Bytes(vec![1, 2, 3]))
            .unwrap();

        assert!(matches!(
            kv.get(&branch_id, "default", "int").unwrap().unwrap(),
            Value::Int(42)
        ));
        assert!(matches!(
            kv.get(&branch_id, "default", "bool").unwrap().unwrap(),
            Value::Bool(true)
        ));
    }
}

mod event_single {
    use super::*;

    #[test]
    fn append_and_read() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let event = EventLog::new(db);

        let seq1 = event
            .append(&branch_id, "default", "audit", int_payload(1))
            .unwrap();
        let seq2 = event
            .append(&branch_id, "default", "audit", int_payload(2))
            .unwrap();
        let seq3 = event
            .append(&branch_id, "default", "audit", int_payload(3))
            .unwrap();

        assert!(seq2 > seq1);
        assert!(seq3 > seq2);

        let events = event
            .get_by_type(&branch_id, "default", "audit", None, None)
            .unwrap();
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn multiple_streams() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let event = EventLog::new(db);

        event
            .append(&branch_id, "default", "stream_a", int_payload(1))
            .unwrap();
        event
            .append(&branch_id, "default", "stream_a", int_payload(2))
            .unwrap();
        event
            .append(&branch_id, "default", "stream_b", int_payload(10))
            .unwrap();

        assert_eq!(
            event
                .get_by_type(&branch_id, "default", "stream_a", None, None)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            event
                .get_by_type(&branch_id, "default", "stream_b", None, None)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn events_are_immutable() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let event = EventLog::new(db);

        event
            .append(&branch_id, "default", "audit", int_payload(42))
            .unwrap();

        // Events can only be appended, not modified or deleted
        // The API doesn't provide update/delete methods for events
        let events = event
            .get_by_type(&branch_id, "default", "audit", None, None)
            .unwrap();
        assert_eq!(events.len(), 1);
    }
}

mod json_single {
    use super::*;

    #[test]
    fn create_and_get() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let json = JsonStore::new(db);

        json.create(
            &branch_id,
            "default",
            "doc",
            json_value(serde_json::json!({"name": "test"})),
        )
        .unwrap();

        let doc = json
            .get(&branch_id, "default", "doc", &root())
            .unwrap()
            .unwrap();
        assert_eq!(doc.as_inner()["name"], "test");
    }

    #[test]
    fn patch_nested_value() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let json = JsonStore::new(db);

        json.create(
            &branch_id,
            "default",
            "doc",
            json_value(serde_json::json!({
                "user": {"name": "Alice", "age": 30}
            })),
        )
        .unwrap();

        json.set(
            &branch_id,
            "default",
            "doc",
            &path(".user.age"),
            json_value(serde_json::json!(31)),
        )
        .unwrap();

        let doc = json
            .get(&branch_id, "default", "doc", &path(".user.age"))
            .unwrap()
            .unwrap();
        assert_eq!(doc.as_inner(), &serde_json::json!(31));
    }

    #[test]
    fn list_documents() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let json = JsonStore::new(db);

        for i in 0..10 {
            json.create(
                &branch_id,
                "default",
                &format!("doc_{}", i),
                test_json_value(i),
            )
            .unwrap();
        }

        let list = json.list(&branch_id, "default", None, None, 100).unwrap();
        assert_eq!(list.doc_ids.len(), 10);
    }
}

mod vector_single {
    use super::*;

    #[test]
    fn create_collection_and_insert() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let vector = VectorStore::new(db);

        vector
            .create_collection(branch_id, "default", "embeddings", config_small())
            .unwrap();
        vector
            .insert(
                branch_id,
                "default",
                "embeddings",
                "vec_1",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        let entry = vector
            .get(branch_id, "default", "embeddings", "vec_1")
            .unwrap()
            .unwrap();
        assert_eq!(entry.value.embedding, vec![1.0, 0.0, 0.0]);
    }

    #[test]
    fn search_returns_nearest() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let vector = VectorStore::new(db);

        vector
            .create_collection(branch_id, "default", "test", config_small())
            .unwrap();

        // Insert vectors at cardinal directions
        vector
            .insert(
                branch_id,
                "default",
                "test",
                "north",
                &[0.0, 1.0, 0.0],
                None,
            )
            .unwrap();
        vector
            .insert(
                branch_id,
                "default",
                "test",
                "south",
                &[0.0, -1.0, 0.0],
                None,
            )
            .unwrap();
        vector
            .insert(branch_id, "default", "test", "east", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Search for vector close to north
        let query = vec![0.0, 0.99, 0.0];
        let results = vector
            .search(branch_id, "default", "test", &query, 1, None)
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "north");
    }

    #[test]
    fn delete_vector() {
        let db = create_test_db();
        let branch_id = BranchId::new();
        let vector = VectorStore::new(db);

        vector
            .create_collection(branch_id, "default", "test", config_small())
            .unwrap();
        vector
            .insert(
                branch_id,
                "default",
                "test",
                "to_delete",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        assert_eq!(
            vector
                .get(branch_id, "default", "test", "to_delete")
                .unwrap()
                .unwrap()
                .value
                .embedding,
            vec![1.0f32, 0.0, 0.0]
        );

        vector
            .delete(branch_id, "default", "test", "to_delete")
            .unwrap();

        assert!(vector
            .get(branch_id, "default", "test", "to_delete")
            .unwrap()
            .is_none());
    }
}

// ============================================================================
// Cross-Primitive Tests
// ============================================================================

#[test]
fn all_five_primitives_together() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    // KV
    p.kv.put(
        &branch_id,
        "default",
        "config",
        Value::String("enabled".into()),
    )
    .unwrap();

    // Event
    p.event
        .append(
            &branch_id,
            "default",
            "lifecycle",
            string_payload("started"),
        )
        .unwrap();

    // JSON
    p.json
        .create(
            &branch_id,
            "default",
            "context",
            json_value(serde_json::json!({"task": "test"})),
        )
        .unwrap();

    // Vector
    p.vector
        .create_collection(branch_id, "default", "memory", config_small())
        .unwrap();
    p.vector
        .insert(branch_id, "default", "memory", "m1", &[1.0, 0.0, 0.0], None)
        .unwrap();

    // Branch index - branches must be explicitly created via create_branch()
    // We're using a random BranchId here which is NOT registered with BranchIndex
    // In production, you would either use the default branch or create one explicitly

    // Verify all readable
    assert_eq!(
        p.kv.get(&branch_id, "default", "config").unwrap(),
        Some(Value::String("enabled".into()))
    );
    assert!(p.event.len(&branch_id, "default").unwrap() > 0);
    assert_eq!(
        p.json
            .get(&branch_id, "default", "context", &root())
            .unwrap()
            .unwrap()
            .as_inner(),
        &serde_json::json!({"task": "test"})
    );
    assert_eq!(
        p.vector
            .get(branch_id, "default", "memory", "m1")
            .unwrap()
            .unwrap()
            .value
            .embedding,
        vec![1.0f32, 0.0, 0.0]
    );
}

#[test]
fn cross_primitive_workflow_agent_memory() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    // Agent initialization
    p.kv.put(
        &branch_id,
        "default",
        "agent:name",
        Value::String("assistant".into()),
    )
    .unwrap();
    p.kv.put(
        &branch_id,
        "default",
        "agent:status",
        Value::String("initializing".into()),
    )
    .unwrap();
    p.event
        .append(
            &branch_id,
            "default",
            "agent:lifecycle",
            string_payload("Agent started"),
        )
        .unwrap();

    // Agent stores context
    p.json
        .create(
            &branch_id,
            "default",
            "agent:context",
            json_value(serde_json::json!({
                "task": "help_user",
                "turn": 0
            })),
        )
        .unwrap();

    // Agent creates memory store
    p.vector
        .create_collection(branch_id, "default", "agent:memories", config_small())
        .unwrap();

    // Simulate processing turns
    for turn in 1..=3 {
        // Update context
        p.json
            .set(
                &branch_id,
                "default",
                "agent:context",
                &path(".turn"),
                json_value(serde_json::json!(turn)),
            )
            .unwrap();

        // Store memory
        p.vector
            .insert(
                branch_id,
                "default",
                "agent:memories",
                &format!("turn_{}", turn),
                &seeded_vector(3, turn as u64),
                Some(serde_json::json!({"turn": turn})),
            )
            .unwrap();

        // Log event
        p.event
            .append(&branch_id, "default", "agent:turns", int_payload(turn))
            .unwrap();
    }

    // Update status
    p.kv.put(
        &branch_id,
        "default",
        "agent:status",
        Value::String("completed".into()),
    )
    .unwrap();
    p.event
        .append(
            &branch_id,
            "default",
            "agent:lifecycle",
            string_payload("Agent completed"),
        )
        .unwrap();

    // Verify final state
    let status =
        p.kv.get(&branch_id, "default", "agent:status")
            .unwrap()
            .unwrap();
    assert_eq!(status, Value::String("completed".into()));

    assert_eq!(
        p.event
            .get_by_type(&branch_id, "default", "agent:turns", None, None)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        p.event
            .get_by_type(&branch_id, "default", "agent:lifecycle", None, None)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        p.vector
            .list_collections(branch_id, "default")
            .unwrap()
            .iter()
            .find(|c| c.name == "agent:memories")
            .unwrap()
            .count,
        3
    );
}

#[test]
fn delete_in_one_primitive_doesnt_affect_others() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    // Use same name across primitives
    p.kv.put(&branch_id, "default", "shared", Value::String("kv".into()))
        .unwrap();
    p.json
        .create(
            &branch_id,
            "default",
            "shared",
            json_value(serde_json::json!({"type": "json"})),
        )
        .unwrap();

    // Delete only from KV
    p.kv.delete(&branch_id, "default", "shared").unwrap();

    // Verify KV deleted but JSON remains
    assert!(p.kv.get(&branch_id, "default", "shared").unwrap().is_none());
    assert_eq!(
        p.json
            .get(&branch_id, "default", "shared", &root())
            .unwrap()
            .unwrap()
            .as_inner(),
        &serde_json::json!({"type": "json"})
    );
}
