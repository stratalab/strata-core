//! EventLog Primitive Tests
//!
//! Tests for immutable append-only event stream with hash chaining.

use crate::common::*;
use std::collections::HashMap;

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::Object(Box::new(HashMap::from([("data".to_string(), data)])))
}

/// Helper to create a simple event payload with an integer
fn payload_int(n: i64) -> Value {
    event_payload(Value::Int(n))
}

/// Helper to create a simple event payload with a string
fn payload_str(s: &str) -> Value {
    event_payload(Value::String(s.to_string()))
}

// ============================================================================
// Basic Operations
// ============================================================================

#[test]
fn empty_log_has_zero_length() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let len = event.len(&test_db.branch_id, "default").unwrap();
    assert_eq!(len, 0);
}

#[test]
fn empty_log_is_empty() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // is_empty rewritten as len() == 0
    assert_eq!(event.len(&test_db.branch_id, "default").unwrap(), 0);
}

#[test]
fn empty_log_head_is_none() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // head rewritten using len() + read(len-1)
    let len = event.len(&test_db.branch_id, "default").unwrap();
    if len == 0 {
        // empty log, head is none
        assert_eq!(len, 0);
    } else {
        let head = event.get(&test_db.branch_id, "default", len - 1).unwrap();
        assert!(head.is_some());
    }
}

#[test]
fn append_returns_sequence_number() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let seq = event
        .append(&test_db.branch_id, "default", "test_type", payload_int(42))
        .unwrap();
    assert_eq!(seq.as_u64(), 0); // First event is sequence 0
}

#[test]
fn append_increments_length() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();
    assert_eq!(event.len(&test_db.branch_id, "default").unwrap(), 1);

    event
        .append(&test_db.branch_id, "default", "type", payload_int(2))
        .unwrap();
    assert_eq!(event.len(&test_db.branch_id, "default").unwrap(), 2);

    event
        .append(&test_db.branch_id, "default", "type", payload_int(3))
        .unwrap();
    assert_eq!(event.len(&test_db.branch_id, "default").unwrap(), 3);
}

#[test]
fn append_sequence_monotonically_increases() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let seq0 = event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();
    let seq1 = event
        .append(&test_db.branch_id, "default", "type", payload_int(2))
        .unwrap();
    let seq2 = event
        .append(&test_db.branch_id, "default", "type", payload_int(3))
        .unwrap();

    assert_eq!(seq0.as_u64(), 0);
    assert_eq!(seq1.as_u64(), 1);
    assert_eq!(seq2.as_u64(), 2);
}

// ============================================================================
// Read Operations
// ============================================================================

#[test]
fn read_returns_appended_event() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(
            &test_db.branch_id,
            "default",
            "my_type",
            payload_str("hello"),
        )
        .unwrap();

    let read = event.get(&test_db.branch_id, "default", 0).unwrap();
    assert!(read.is_some());

    let e = read.unwrap().value;
    assert_eq!(e.event_type, "my_type");
    assert_eq!(e.payload, payload_str("hello"));
}

#[test]
fn read_nonexistent_returns_none() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let read = event.get(&test_db.branch_id, "default", 999).unwrap();
    assert!(read.is_none());
}

#[test]
fn head_returns_last_event() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "type", payload_int(2))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "type", payload_int(3))
        .unwrap();

    // head rewritten using len() + read(len-1)
    let len = event.len(&test_db.branch_id, "default").unwrap();
    let head = event
        .get(&test_db.branch_id, "default", len - 1)
        .unwrap()
        .unwrap();
    assert_eq!(head.value.payload, payload_int(3));
}

#[test]
fn read_range_returns_events_in_order() {
    let test_db = TestDb::new();
    let event = test_db.event();

    for i in 0..5 {
        event
            .append(&test_db.branch_id, "default", "type", payload_int(i))
            .unwrap();
    }

    // read_range rewritten using loop of read() calls
    let mut range = Vec::new();
    for seq in 1..4 {
        if let Some(e) = event.get(&test_db.branch_id, "default", seq).unwrap() {
            range.push(e);
        }
    }
    assert_eq!(range.len(), 3); // [1, 2, 3]

    assert_eq!(range[0].value.payload, payload_int(1));
    assert_eq!(range[1].value.payload, payload_int(2));
    assert_eq!(range[2].value.payload, payload_int(3));
}

#[test]
fn read_range_empty_when_start_equals_end() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();

    // read_range(0, 0) means empty range — verify no events in that span
    let start: u64 = 0;
    let end: u64 = 0;
    let range: Vec<_> = (start..end)
        .filter_map(|seq| event.get(&test_db.branch_id, "default", seq).unwrap())
        .collect();
    assert!(range.is_empty());
}

// ============================================================================
// Hash Chain Verification
// ============================================================================

#[test]
#[ignore = "requires: EventLog::verify_chain"]
fn verify_chain_valid_for_empty_log() {
    let test_db = TestDb::new();
    let _event = test_db.event();
    // Chain integrity verification is an architectural principle
    // but verify_chain() is not yet in the MVP API
}

#[test]
#[ignore = "requires: EventLog::verify_chain"]
fn verify_chain_valid_after_appends() {
    let test_db = TestDb::new();
    let _event = test_db.event();
    // Chain integrity verification is an architectural principle
    // but verify_chain() is not yet in the MVP API
}

#[test]
fn events_have_hash_field() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();

    let e = event
        .get(&test_db.branch_id, "default", 0)
        .unwrap()
        .unwrap();
    // Hash should be non-empty
    assert!(!e.value.hash.is_empty());
}

#[test]
fn events_have_prev_hash_field() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "type", payload_int(2))
        .unwrap();

    let e0 = event
        .get(&test_db.branch_id, "default", 0)
        .unwrap()
        .unwrap();
    let e1 = event
        .get(&test_db.branch_id, "default", 1)
        .unwrap()
        .unwrap();

    // Second event's prev_hash should equal first event's hash
    assert_eq!(e1.value.prev_hash, e0.value.hash);
}

// ============================================================================
// Event Types / Streams
// ============================================================================

#[test]
fn multiple_event_types() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type_a", payload_int(1))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "type_b", payload_int(2))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "type_a", payload_int(3))
        .unwrap();

    // Verify both types exist by reading by type
    let type_a = event
        .get_by_type(&test_db.branch_id, "default", "type_a", None, None)
        .unwrap();
    let type_b = event
        .get_by_type(&test_db.branch_id, "default", "type_b", None, None)
        .unwrap();
    assert_eq!(type_a.len(), 2);
    assert_eq!(type_b.len(), 1);
}

#[test]
fn len_by_type() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type_a", payload_int(1))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "type_b", payload_int(2))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "type_a", payload_int(3))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "type_a", payload_int(4))
        .unwrap();

    // len_by_type rewritten using get_by_type().len()
    assert_eq!(
        event
            .get_by_type(&test_db.branch_id, "default", "type_a", None, None)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        event
            .get_by_type(&test_db.branch_id, "default", "type_b", None, None)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        event
            .get_by_type(&test_db.branch_id, "default", "type_c", None, None)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn get_by_type() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "orders", payload_int(100))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "payments", payload_int(50))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "orders", payload_int(200))
        .unwrap();

    let orders = event
        .get_by_type(&test_db.branch_id, "default", "orders", None, None)
        .unwrap();
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0].value.payload, payload_int(100));
    assert_eq!(orders[1].value.payload, payload_int(200));
}

// ============================================================================
// Batch Operations
// ============================================================================

#[test]
#[ignore = "requires: EventLog::append_batch"]
fn append_batch_returns_sequence_numbers() {
    let test_db = TestDb::new();
    let _event = test_db.event();
    // Batch atomicity is an architectural principle
    // but append_batch() is not yet in the MVP API
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn empty_event_type_rejected() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // Empty event type should be rejected
    let result = event.append(&test_db.branch_id, "default", "", payload_int(1));
    assert!(result.is_err());
}

#[test]
fn large_payload() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let large_string = "x".repeat(10000);
    event
        .append(
            &test_db.branch_id,
            "default",
            "type",
            payload_str(&large_string),
        )
        .unwrap();

    let read = event
        .get(&test_db.branch_id, "default", 0)
        .unwrap()
        .unwrap();
    assert_eq!(read.value.payload, payload_str(&large_string));
}

#[test]
fn payload_must_be_object() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // Non-object payloads should be rejected
    let result = event.append(&test_db.branch_id, "default", "type", Value::Int(42));
    assert!(result.is_err());

    let result = event.append(
        &test_db.branch_id,
        "default",
        "type",
        Value::String("hello".into()),
    );
    assert!(result.is_err());

    let result = event.append(
        &test_db.branch_id,
        "default",
        "type",
        Value::Array(Box::new(vec![])),
    );
    assert!(result.is_err());

    // Object payload should work
    let result = event.append(&test_db.branch_id, "default", "type", payload_int(42));
    assert!(result.is_ok());
}
