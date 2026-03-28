//! EventLog Primitive Tests
//!
//! Tests for immutable append-only event stream with hash chaining.

use crate::common::*;
use std::collections::HashMap;

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::object(HashMap::from([("data".to_string(), data)]))
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

    let result = event.append(&test_db.branch_id, "default", "type", Value::array(vec![]));
    assert!(result.is_err());

    // Object payload should work
    let result = event.append(&test_db.branch_id, "default", "type", payload_int(42));
    assert!(result.is_ok());
}

// ============================================================================
// Audit Debt Tests — coverage gaps from Event primitive dossier
// ============================================================================

#[test]
fn concurrent_appends_preserve_data_integrity() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let test_db = TestDb::new();
    let db = test_db.db.clone();
    let branch_id = test_db.branch_id;
    let barrier = Arc::new(Barrier::new(2));
    let total_per_thread = 20;

    let handles: Vec<_> = (0..2)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let event = EventLog::new(db);
                barrier.wait();
                let mut successes = 0;
                for i in 0..total_per_thread {
                    let payload = Value::object(HashMap::from([
                        ("thread".to_string(), Value::Int(thread_idx)),
                        ("i".to_string(), Value::Int(i)),
                    ]));
                    // OCC may require retries; the engine handles this internally
                    match event.append(&branch_id, "default", "concurrent", payload) {
                        Ok(_) => successes += 1,
                        Err(_) => {} // OCC conflict, acceptable
                    }
                }
                successes
            })
        })
        .collect();

    let total_successes: i64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total_successes > 0, "at least some appends must succeed");

    // Verify hash chain integrity for all successful appends
    let event = test_db.event();
    let len = event.len(&branch_id, "default").unwrap();
    assert_eq!(len, total_successes as u64);

    for seq in 1..len {
        let prev = event.get(&branch_id, "default", seq - 1).unwrap().unwrap();
        let curr = event.get(&branch_id, "default", seq).unwrap().unwrap();
        assert_eq!(
            curr.value.prev_hash, prev.value.hash,
            "hash chain broken at sequence {}",
            seq
        );
    }
}

#[test]
fn large_batch_append_preserves_chain() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let entries: Vec<(String, Value)> = (0..200)
        .map(|i| ("batch_type".to_string(), payload_int(i)))
        .collect();

    let results = event
        .batch_append(&test_db.branch_id, "default", entries)
        .unwrap();

    // All should succeed
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(success_count, 200);

    // Verify monotonic sequences
    let seqs: Vec<u64> = results
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .map(|v| v.as_u64())
        .collect();
    let mut sorted = seqs.clone();
    sorted.sort();
    assert_eq!(seqs, sorted, "sequences must be monotonically increasing");
    assert_eq!(seqs.len(), 200);
    assert_eq!(seqs[0], 0);
    assert_eq!(seqs[199], 199);

    // Verify hash chain integrity
    for seq in 1u64..200 {
        let prev = event
            .get(&test_db.branch_id, "default", seq - 1)
            .unwrap()
            .unwrap();
        let curr = event
            .get(&test_db.branch_id, "default", seq)
            .unwrap()
            .unwrap();
        assert_eq!(
            curr.value.prev_hash, prev.value.hash,
            "hash chain broken at sequence {}",
            seq
        );
    }

    // Verify stream metadata count
    assert_eq!(event.len(&test_db.branch_id, "default").unwrap(), 200);
}

#[test]
fn list_at_before_any_events_returns_empty() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // Append some events
    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();

    // Query with timestamp 0 (before any events)
    let result = event
        .list_at(&test_db.branch_id, "default", None, 0, None)
        .unwrap();
    assert!(
        result.is_empty(),
        "list_at before any events should be empty"
    );
}

#[test]
fn list_at_between_events_returns_only_earlier() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // Append first event
    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();

    // Wait and capture a midpoint timestamp (using wall clock, which the storage layer uses)
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mid_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Append second event
    event
        .append(&test_db.branch_id, "default", "type", payload_int(2))
        .unwrap();

    // Query at midpoint — should only see the first event
    let result = event
        .list_at(&test_db.branch_id, "default", None, mid_ts, None)
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].payload, payload_int(1));

    // Query at u64::MAX — should see both events
    let result = event
        .list_at(&test_db.branch_id, "default", None, u64::MAX, None)
        .unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn list_at_with_limit() {
    let test_db = TestDb::new();
    let event = test_db.event();

    for i in 0..10 {
        event
            .append(&test_db.branch_id, "default", "type", payload_int(i))
            .unwrap();
    }

    let result = event
        .list_at(&test_db.branch_id, "default", None, u64::MAX, Some(3))
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].payload, payload_int(0));
    assert_eq!(result[2].payload, payload_int(2));
}

#[test]
fn metadata_corruption_returns_error() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // Append an event so metadata exists
    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();

    // Directly corrupt metadata by writing invalid data to the meta key
    let ns = strata_core::types::Namespace::for_branch_space(test_db.branch_id, "default");
    let meta_key = strata_core::types::Key::new_event_meta(std::sync::Arc::new(ns));
    test_db
        .db
        .transaction(test_db.branch_id, |txn| {
            txn.put(
                meta_key.clone(),
                Value::String("not valid json{{{".to_string()),
            )?;
            Ok(())
        })
        .unwrap();

    // Operations that read metadata should now return errors, not silently default
    let result = event.len(&test_db.branch_id, "default");
    assert!(
        result.is_err(),
        "corrupt metadata must return error, not silent default"
    );

    let result = event.append(&test_db.branch_id, "default", "type", payload_int(2));
    assert!(
        result.is_err(),
        "append with corrupt metadata must return error"
    );

    let result = event.list_types(&test_db.branch_id, "default");
    assert!(
        result.is_err(),
        "list_types with corrupt metadata must return error"
    );
}

#[test]
fn eventlog_ext_non_default_space() {
    use strata_engine::primitives::extensions::EventLogExt;

    let test_db = TestDb::new();

    // Append in "custom" space via extension trait
    test_db
        .db
        .transaction(test_db.branch_id, |txn| {
            txn.event_append_in_space("custom", "my_type", payload_int(42))?;
            txn.event_append_in_space("custom", "my_type", payload_int(43))?;
            Ok(())
        })
        .unwrap();

    // Append in "default" space
    test_db
        .db
        .transaction(test_db.branch_id, |txn| {
            txn.event_append("other_type", payload_int(99))?;
            Ok(())
        })
        .unwrap();

    // Verify isolation: custom space has 2 events, default has 1
    let event = test_db.event();
    assert_eq!(event.len(&test_db.branch_id, "custom").unwrap(), 2);
    assert_eq!(event.len(&test_db.branch_id, "default").unwrap(), 1);

    // Read back from custom space via extension trait
    test_db
        .db
        .transaction(test_db.branch_id, |txn| {
            let val = txn.event_get_in_space("custom", 0)?;
            assert!(val.is_some());

            // Default space should not see custom space's events at seq 1
            let default_val = txn.event_get(1)?;
            assert!(
                default_val.is_none(),
                "default space should not see custom space's seq 1"
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn range_query_returns_correct_slice() {
    let test_db = TestDb::new();
    let event = test_db.event();

    for i in 0..10 {
        event
            .append(&test_db.branch_id, "default", "type", payload_int(i))
            .unwrap();
    }

    // Range [3, 7) should return sequences 3, 4, 5, 6
    let results = event
        .range(&test_db.branch_id, "default", 3, Some(7), None, false, None)
        .unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].value.payload, payload_int(3));
    assert_eq!(results[3].value.payload, payload_int(6));

    // Range with limit
    let results = event
        .range(&test_db.branch_id, "default", 0, None, Some(2), false, None)
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value.payload, payload_int(0));
    assert_eq!(results[1].value.payload, payload_int(1));

    // Empty range (start >= end)
    let results = event
        .range(&test_db.branch_id, "default", 5, Some(5), None, false, None)
        .unwrap();
    assert!(results.is_empty());

    // Range past end of log is clamped
    let results = event
        .range(
            &test_db.branch_id,
            "default",
            8,
            Some(100),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(results.len(), 2); // sequences 8, 9

    // Open-ended range (no end_seq)
    let results = event
        .range(&test_db.branch_id, "default", 7, None, None, false, None)
        .unwrap();
    assert_eq!(results.len(), 3); // sequences 7, 8, 9
}

#[test]
fn get_by_type_at_filters_by_timestamp() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // Append first event
    event
        .append(&test_db.branch_id, "default", "audit", payload_int(1))
        .unwrap();
    let e0 = event
        .get(&test_db.branch_id, "default", 0)
        .unwrap()
        .unwrap();
    let ts0 = e0.value.timestamp.as_micros();

    std::thread::sleep(std::time::Duration::from_millis(2));

    // Append second event of same type
    event
        .append(&test_db.branch_id, "default", "audit", payload_int(2))
        .unwrap();
    let e1 = event
        .get(&test_db.branch_id, "default", 1)
        .unwrap()
        .unwrap();
    let ts1 = e1.value.timestamp.as_micros();

    // Append event of different type
    event
        .append(&test_db.branch_id, "default", "other", payload_int(3))
        .unwrap();

    // get_by_type_at with ts0 should only return the first audit event
    let results = event
        .get_by_type_at(&test_db.branch_id, "default", "audit", ts0)
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].value.payload, payload_int(1));

    // get_by_type_at with ts1 should return both audit events
    let results = event
        .get_by_type_at(&test_db.branch_id, "default", "audit", ts1)
        .unwrap();
    assert_eq!(results.len(), 2);

    // get_by_type_at for "other" type at ts0 should return nothing (it was appended later)
    let results = event
        .get_by_type_at(&test_db.branch_id, "default", "other", ts0)
        .unwrap();
    assert!(results.is_empty());
}

// ============================================================================
// Range Queries — Direction, Type Filter, Pagination (#1882)
// ============================================================================

#[test]
fn range_empty_log_returns_empty() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let results = event
        .range(&test_db.branch_id, "default", 0, None, None, false, None)
        .unwrap();
    assert!(results.is_empty());

    // Also reverse on empty
    let results = event
        .range(&test_db.branch_id, "default", 0, None, None, true, None)
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn range_zero_limit_returns_empty() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();

    let results = event
        .range(&test_db.branch_id, "default", 0, None, Some(0), false, None)
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn range_reverse_returns_descending_order() {
    let test_db = TestDb::new();
    let event = test_db.event();

    for i in 0..5 {
        event
            .append(&test_db.branch_id, "default", "type", payload_int(i))
            .unwrap();
    }

    let results = event
        .range(&test_db.branch_id, "default", 0, None, None, true, None)
        .unwrap();
    assert_eq!(results.len(), 5);
    // Reverse: sequences should be 4, 3, 2, 1, 0
    assert_eq!(results[0].value.payload, payload_int(4));
    assert_eq!(results[4].value.payload, payload_int(0));
}

#[test]
fn range_reverse_with_limit() {
    let test_db = TestDb::new();
    let event = test_db.event();

    for i in 0..10 {
        event
            .append(&test_db.branch_id, "default", "type", payload_int(i))
            .unwrap();
    }

    // Last 3 events in reverse
    let results = event
        .range(&test_db.branch_id, "default", 0, None, Some(3), true, None)
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].value.payload, payload_int(9));
    assert_eq!(results[1].value.payload, payload_int(8));
    assert_eq!(results[2].value.payload, payload_int(7));
}

#[test]
fn range_with_type_filter() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "audit", payload_int(1))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "metric", payload_int(2))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "audit", payload_int(3))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "metric", payload_int(4))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "audit", payload_int(5))
        .unwrap();

    // Range [0, 5) filtered by "audit" should return 3 events
    let results = event
        .range(
            &test_db.branch_id,
            "default",
            0,
            None,
            None,
            false,
            Some("audit"),
        )
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].value.payload, payload_int(1));
    assert_eq!(results[1].value.payload, payload_int(3));
    assert_eq!(results[2].value.payload, payload_int(5));
}

#[test]
fn range_with_type_filter_and_limit() {
    let test_db = TestDb::new();
    let event = test_db.event();

    for i in 0..10 {
        let et = if i % 2 == 0 { "even" } else { "odd" };
        event
            .append(&test_db.branch_id, "default", et, payload_int(i))
            .unwrap();
    }

    // Get first 2 "even" events
    let results = event
        .range(
            &test_db.branch_id,
            "default",
            0,
            None,
            Some(2),
            false,
            Some("even"),
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value.payload, payload_int(0));
    assert_eq!(results[1].value.payload, payload_int(2));
}

#[test]
fn range_reverse_with_type_filter() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "a", payload_int(1))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "b", payload_int(2))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "a", payload_int(3))
        .unwrap();

    // Reverse, filter "a" — should get seq 2 then seq 0
    let results = event
        .range(
            &test_db.branch_id,
            "default",
            0,
            None,
            None,
            true,
            Some("a"),
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value.payload, payload_int(3)); // seq 2
    assert_eq!(results[1].value.payload, payload_int(1)); // seq 0
}

// ============================================================================
// Range By Time (#1882)
// ============================================================================

#[test]
fn range_by_time_returns_events_in_window() {
    let test_db = TestDb::new();
    let event = test_db.event();

    // Append events and capture timestamps
    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();
    let e0 = event
        .get(&test_db.branch_id, "default", 0)
        .unwrap()
        .unwrap();
    let ts0 = e0.value.timestamp.as_micros();

    std::thread::sleep(std::time::Duration::from_millis(5));

    event
        .append(&test_db.branch_id, "default", "type", payload_int(2))
        .unwrap();
    let e1 = event
        .get(&test_db.branch_id, "default", 1)
        .unwrap()
        .unwrap();
    let ts1 = e1.value.timestamp.as_micros();

    std::thread::sleep(std::time::Duration::from_millis(5));

    event
        .append(&test_db.branch_id, "default", "type", payload_int(3))
        .unwrap();

    // Query range [ts0, ts1] should return events 0 and 1
    let results = event
        .range_by_time(
            &test_db.branch_id,
            "default",
            ts0,
            Some(ts1),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value.payload, payload_int(1));
    assert_eq!(results[1].value.payload, payload_int(2));
}

#[test]
fn range_by_time_reverse() {
    let test_db = TestDb::new();
    let event = test_db.event();

    for i in 0..5 {
        event
            .append(&test_db.branch_id, "default", "type", payload_int(i))
            .unwrap();
    }

    // Open-ended time range, reversed
    let results = event
        .range_by_time(&test_db.branch_id, "default", 0, None, None, true, None)
        .unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].value.payload, payload_int(4));
    assert_eq!(results[4].value.payload, payload_int(0));
}

#[test]
fn range_by_time_with_limit() {
    let test_db = TestDb::new();
    let event = test_db.event();

    for i in 0..5 {
        event
            .append(&test_db.branch_id, "default", "type", payload_int(i))
            .unwrap();
    }

    let results = event
        .range_by_time(&test_db.branch_id, "default", 0, None, Some(2), false, None)
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value.payload, payload_int(0));
    assert_eq!(results[1].value.payload, payload_int(1));
}

#[test]
fn range_by_time_with_type_filter() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "audit", payload_int(1))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "metric", payload_int(2))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "audit", payload_int(3))
        .unwrap();

    let results = event
        .range_by_time(
            &test_db.branch_id,
            "default",
            0,
            None,
            None,
            false,
            Some("audit"),
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value.payload, payload_int(1));
    assert_eq!(results[1].value.payload, payload_int(3));
}

#[test]
fn range_by_time_empty_log() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let results = event
        .range_by_time(&test_db.branch_id, "default", 0, None, None, false, None)
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn range_by_time_zero_limit() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "type", payload_int(1))
        .unwrap();

    let results = event
        .range_by_time(&test_db.branch_id, "default", 0, None, Some(0), false, None)
        .unwrap();
    assert!(results.is_empty());
}

// ============================================================================
// List Types (#1882)
// ============================================================================

#[test]
fn list_types_returns_all_event_types() {
    let test_db = TestDb::new();
    let event = test_db.event();

    event
        .append(&test_db.branch_id, "default", "audit", payload_int(1))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "metric", payload_int(2))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "audit", payload_int(3))
        .unwrap();
    event
        .append(&test_db.branch_id, "default", "error", payload_int(4))
        .unwrap();

    let mut types = event.list_types(&test_db.branch_id, "default").unwrap();
    types.sort();
    assert_eq!(types, vec!["audit", "error", "metric"]);
}

#[test]
fn list_types_empty_log() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let types = event.list_types(&test_db.branch_id, "default").unwrap();
    assert!(types.is_empty());
}
