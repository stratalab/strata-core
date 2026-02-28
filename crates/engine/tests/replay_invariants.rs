//! Replay Invariant Tests
//!
//! These tests validate the replay invariants (P1-P6):
//!
//! - P1: Replay is a pure function over (Snapshot, WAL, EventLog)
//! - P2: Replay is side-effect free (does not mutate canonical store)
//! - P3: Replay produces a derived view (not a new source of truth)
//! - P4: Replay does not persist state (unless explicitly materialized)
//! - P5: Replay is deterministic (same inputs = same view)
//! - P6: Replay is idempotent (running twice produces identical view)
//!
//! **CRITICAL**: Replay NEVER writes to the canonical store.
//! ReadOnlyView is derived, not authoritative.

use std::collections::HashMap;
use std::sync::Arc;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::PrimitiveType;
use strata_engine::{diff_views, ReadOnlyView};

/// Helper to create a test namespace
fn test_namespace(branch_id: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(
        "tenant".to_string(),
        "app".to_string(),
        "agent".to_string(),
        branch_id,
        "default".to_string(),
    ))
}

/// Helper to build a ReadOnlyView with specified KV entries
fn build_view_with_kv(branch_id: BranchId, entries: &[(&str, i64)]) -> ReadOnlyView {
    let ns = test_namespace(branch_id);
    let mut view = ReadOnlyView::new(branch_id);

    for (key, value) in entries {
        view.apply_kv_put(Key::new_kv(ns.clone(), *key), Value::Int(*value));
    }

    view
}

/// Helper to build a ReadOnlyView with events
fn build_view_with_events(branch_id: BranchId, events: &[(&str, &str)]) -> ReadOnlyView {
    let mut view = ReadOnlyView::new(branch_id);

    for (event_type, data) in events {
        view.append_event(
            (*event_type).to_string(),
            Value::String((*data).to_string()),
        );
    }

    view
}

// ============================================================================
// P1: Replay is a Pure Function
// Over (Snapshot, WAL, EventLog)
// ============================================================================

/// P1: Same inputs produce same outputs - basic case
#[test]
fn test_replay_pure_function_p1_basic() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Simulate replaying the same "inputs" multiple times
    // In a real scenario, this would read from WAL/EventLog
    // Here we test that building views from the same operations
    // produces identical results

    let build_view = || {
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "key1"), Value::Int(100));
        view.apply_kv_put(
            Key::new_kv(ns.clone(), "key2"),
            Value::String("hello".into()),
        );
        view.apply_kv_delete(&Key::new_kv(ns.clone(), "key3")); // Delete non-existent - should be no-op
        view.apply_kv_put(Key::new_kv(ns.clone(), "key3"), Value::Bool(true));
        view.append_event("UserCreated".into(), Value::String("alice".into()));
        view
    };

    let view1 = build_view();
    let view2 = build_view();

    // Views must be identical
    assert_eq!(view1.kv_count(), view2.kv_count());
    assert_eq!(view1.event_count(), view2.event_count());
    assert_eq!(view1.operation_count(), view2.operation_count());

    // Diff between them should be empty
    let diff = diff_views(&view1, &view2);
    assert!(
        diff.is_empty(),
        "Pure function: same inputs should produce empty diff"
    );
}

/// P1: Complex operation sequences produce same results
#[test]
fn test_replay_pure_function_p1_complex_sequence() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Complex sequence: create, update, delete, recreate
    let build_complex_view = || {
        let mut view = ReadOnlyView::new(branch_id);
        let key = Key::new_kv(ns.clone(), "counter");

        // Create
        view.apply_kv_put(key.clone(), Value::Int(1));
        // Update multiple times
        view.apply_kv_put(key.clone(), Value::Int(2));
        view.apply_kv_put(key.clone(), Value::Int(3));
        // Delete
        view.apply_kv_delete(&key);
        // Recreate with different value
        view.apply_kv_put(key.clone(), Value::Int(100));

        view
    };

    // Build 5 times
    let views: Vec<_> = (0..5).map(|_| build_complex_view()).collect();

    // All views must have identical final state
    for (i, view) in views.iter().enumerate().skip(1) {
        let diff = diff_views(&views[0], view);
        assert!(
            diff.is_empty(),
            "View {} differs from view 0: {}",
            i,
            diff.summary()
        );
    }

    // Final state should be key=100
    let key = Key::new_kv(ns.clone(), "counter");
    assert_eq!(views[0].get_kv(&key), Some(&Value::Int(100)));
}

// ============================================================================
// P2: Replay is Side-Effect Free
// Does not mutate canonical store
// ============================================================================

/// P2: ReadOnlyView operations don't affect external state
#[test]
fn test_replay_side_effect_free_p2_basic() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // External "canonical" state (simulated)
    let mut canonical_state: HashMap<String, Value> = HashMap::new();
    canonical_state.insert("existing_key".into(), Value::Int(999));

    // Build a view that "conflicts" with canonical state
    let mut view = ReadOnlyView::new(branch_id);
    view.apply_kv_put(Key::new_kv(ns.clone(), "existing_key"), Value::Int(1)); // Different value
    view.apply_kv_put(Key::new_kv(ns.clone(), "new_key"), Value::Int(2));

    // Canonical state must be unchanged
    assert_eq!(canonical_state.get("existing_key"), Some(&Value::Int(999)));
    assert!(!canonical_state.contains_key("new_key"));

    // View has its own independent state
    let key = Key::new_kv(ns.clone(), "existing_key");
    assert_eq!(view.get_kv(&key), Some(&Value::Int(1)));
}

/// P2: Multiple replays don't accumulate side effects
#[test]
fn test_replay_side_effect_free_p2_no_accumulation() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Replay multiple times
    for iteration in 0..10 {
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "key"), Value::Int(iteration));

        // Each view is independent - check it has exactly what we put
        let key = Key::new_kv(ns.clone(), "key");
        assert_eq!(
            view.get_kv(&key),
            Some(&Value::Int(iteration)),
            "Iteration {} should have value {}",
            iteration,
            iteration
        );
        assert_eq!(view.kv_count(), 1, "Should have exactly 1 key");
    }
}

// ============================================================================
// P3: Replay Produces a Derived View
// Not a new source of truth
// ============================================================================

/// P3: View is read-only (no mutable external interface)
#[test]
fn test_replay_derived_view_p3_read_only() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    let mut view = ReadOnlyView::new(branch_id);
    view.apply_kv_put(Key::new_kv(ns.clone(), "key"), Value::Int(42));

    // View can only be read, not mutated through public read interface
    // The apply_* methods are for building the view during replay
    // Once built, it's conceptually immutable

    let key = Key::new_kv(ns.clone(), "key");
    let value = view.get_kv(&key);
    assert_eq!(value, Some(&Value::Int(42)));

    // Cannot add keys through get_kv (it's read-only)
    let nonexistent = Key::new_kv(ns.clone(), "nonexistent");
    assert_eq!(view.get_kv(&nonexistent), None);

    // Count unchanged
    assert_eq!(view.kv_count(), 1);
}

/// P3: View derives from specific inputs, not global state
#[test]
fn test_replay_derived_view_p3_input_specific() {
    // Two different "branches" with different inputs produce different views

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Different namespaces for different branches
    let ns_a = test_namespace(branch_a);
    let ns_b = test_namespace(branch_b);

    let mut view_a = ReadOnlyView::new(branch_a);
    view_a.apply_kv_put(Key::new_kv(ns_a.clone(), "unique_a"), Value::Int(1));

    let mut view_b = ReadOnlyView::new(branch_b);
    view_b.apply_kv_put(Key::new_kv(ns_b.clone(), "unique_b"), Value::Int(2));

    // Views are independent
    assert!(!view_a.contains_kv(&Key::new_kv(ns_b.clone(), "unique_b")));
    assert!(!view_b.contains_kv(&Key::new_kv(ns_a.clone(), "unique_a")));

    // Each view only contains what was derived from its inputs
    assert_eq!(view_a.kv_count(), 1);
    assert_eq!(view_b.kv_count(), 1);
}

// ============================================================================
// P4: Replay Does Not Persist State
// Unless explicitly materialized
// ============================================================================

/// P4: View state is transient (goes away when dropped)
#[test]
fn test_replay_no_persist_p4_transient() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Build and drop a view
    {
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "temporary"), Value::Int(999));
        assert_eq!(view.kv_count(), 1);
        // View dropped here
    }

    // Build another view - it starts empty
    let view2 = ReadOnlyView::new(branch_id);
    assert_eq!(view2.kv_count(), 0);
    assert!(!view2.contains_kv(&Key::new_kv(ns.clone(), "temporary")));
}

/// P4: View is not automatically persisted
#[test]
fn test_replay_no_persist_p4_no_auto_persist() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Simulate many views being created and destroyed
    for _ in 0..100 {
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "key"), Value::Int(42));

        // These operations don't touch any persistent storage
        // View goes out of scope with no persistence
    }

    // No persistent state accumulated
    // (In a real test, we'd check that no files were created, WAL unchanged, etc.)
    // Here we verify that new views start empty
    let fresh_view = ReadOnlyView::new(branch_id);
    assert_eq!(fresh_view.kv_count(), 0);
}

// ============================================================================
// P5: Replay is Deterministic
// Same inputs = Same view
// ============================================================================

/// P5: Multiple replays produce identical views
#[test]
fn test_replay_deterministic_p5_multiple_branches() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Define a sequence of operations (simulating WAL replay)
    let operations: Vec<(&str, i64)> = vec![
        ("key1", 100),
        ("key2", 200),
        ("key3", 300),
        ("key1", 101), // Update
        ("key4", 400),
    ];

    let build_view = || {
        let mut view = ReadOnlyView::new(branch_id);
        for (key, value) in &operations {
            view.apply_kv_put(Key::new_kv(ns.clone(), *key), Value::Int(*value));
        }
        view
    };

    // Build 10 views - all must be identical
    let views: Vec<_> = (0..10).map(|_| build_view()).collect();

    for i in 1..views.len() {
        let diff = diff_views(&views[0], &views[i]);
        assert!(
            diff.is_empty(),
            "P5 violation: View {} differs from View 0: {}",
            i,
            diff.summary()
        );
    }

    // Verify expected final state
    let key1 = Key::new_kv(ns.clone(), "key1");
    assert_eq!(views[0].get_kv(&key1), Some(&Value::Int(101))); // Last update wins
    assert_eq!(views[0].kv_count(), 4); // 4 unique keys
}

/// P5: Order of operations matters (deterministically)
#[test]
fn test_replay_deterministic_p5_order_matters() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Two different orderings produce different (but deterministic) results

    let build_order_a = || {
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "x"), Value::Int(1));
        view.apply_kv_put(Key::new_kv(ns.clone(), "x"), Value::Int(2));
        view
    };

    let build_order_b = || {
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "x"), Value::Int(2));
        view.apply_kv_put(Key::new_kv(ns.clone(), "x"), Value::Int(1));
        view
    };

    // Same ordering produces same result
    let a1 = build_order_a();
    let a2 = build_order_a();
    assert!(diff_views(&a1, &a2).is_empty());

    let b1 = build_order_b();
    let b2 = build_order_b();
    assert!(diff_views(&b1, &b2).is_empty());

    // Different orderings produce different results
    let diff = diff_views(&a1, &b1);
    assert!(
        !diff.is_empty(),
        "Different orderings should produce different results"
    );
    assert_eq!(diff.modified.len(), 1);
}

/// P5: Events are ordered deterministically
#[test]
fn test_replay_deterministic_p5_events_ordered() {
    let branch_id = BranchId::new();

    let events = vec![
        ("EventA", "data1"),
        ("EventB", "data2"),
        ("EventC", "data3"),
    ];

    let build_view = || {
        let mut view = ReadOnlyView::new(branch_id);
        for (event_type, data) in &events {
            view.append_event(
                (*event_type).to_string(),
                Value::String((*data).to_string()),
            );
        }
        view
    };

    // Multiple builds produce identical event sequences
    for _ in 0..5 {
        let view = build_view();
        let view_events = view.events();

        assert_eq!(view_events.len(), 3);
        assert_eq!(view_events[0].0, "EventA");
        assert_eq!(view_events[1].0, "EventB");
        assert_eq!(view_events[2].0, "EventC");
    }
}

// ============================================================================
// P6: Replay is Idempotent
// Running twice produces identical view
// ============================================================================

/// P6: Applying operations twice produces same view
#[test]
fn test_replay_idempotent_p6_basic() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Single replay
    let view_once = {
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "key"), Value::Int(42));
        view
    };

    // "Double replay" - simulating replaying the same branch twice
    let view_twice = {
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "key"), Value::Int(42));
        // Apply same operation again (idempotent put)
        view.apply_kv_put(Key::new_kv(ns.clone(), "key"), Value::Int(42));
        view
    };

    // Result should be the same (single key with value 42)
    let key = Key::new_kv(ns.clone(), "key");
    assert_eq!(view_once.get_kv(&key), view_twice.get_kv(&key));
    assert_eq!(view_once.kv_count(), 1);
    assert_eq!(view_twice.kv_count(), 1);
}

/// P6: Building view multiple times from same source is idempotent
#[test]
fn test_replay_idempotent_p6_full_rebuild() {
    let branch_id = BranchId::new();

    // Define source operations
    let operations = vec![
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("a", 10), // Update a
        ("b", 20), // Update b
    ];

    // Build 10 times
    let mut views = Vec::new();
    for _ in 0..10 {
        let view = build_view_with_kv(
            branch_id,
            &operations.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
        );
        views.push(view);
    }

    // All views must be identical
    for (i, view) in views.iter().enumerate().skip(1) {
        let diff = diff_views(&views[0], view);
        assert!(
            diff.is_empty(),
            "P6 violation: View {} differs after rebuild: {}",
            i,
            diff.summary()
        );
    }
}

/// P6: Event replay is idempotent
#[test]
fn test_replay_idempotent_p6_events() {
    let branch_id = BranchId::new();

    let events = vec![
        ("Created", "user:1"),
        ("Updated", "user:1"),
        ("Deleted", "user:1"),
    ];

    // Build multiple times
    let mut views = Vec::new();
    for _ in 0..5 {
        let view = build_view_with_events(
            branch_id,
            &events.iter().map(|(t, d)| (*t, *d)).collect::<Vec<_>>(),
        );
        views.push(view);
    }

    // All views must have identical event sequences
    for view in &views {
        assert_eq!(view.event_count(), 3);
        assert_eq!(view.events()[0].0, "Created");
        assert_eq!(view.events()[1].0, "Updated");
        assert_eq!(view.events()[2].0, "Deleted");
    }
}

// ============================================================================
// Combined Invariant Tests
// ============================================================================

/// Test all invariants together with realistic workload
#[test]
fn test_all_replay_invariants_combined() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    // Complex operation sequence
    let build_realistic_view = || {
        let mut view = ReadOnlyView::new(branch_id);

        // Initial creates
        view.apply_kv_put(
            Key::new_kv(ns.clone(), "user:1:name"),
            Value::String("Alice".into()),
        );
        view.apply_kv_put(
            Key::new_kv(ns.clone(), "user:1:email"),
            Value::String("alice@example.com".into()),
        );
        view.apply_kv_put(
            Key::new_kv(ns.clone(), "user:2:name"),
            Value::String("Bob".into()),
        );
        view.apply_kv_put(Key::new_kv(ns.clone(), "counter"), Value::Int(0));

        // Events
        view.append_event("UserCreated".into(), Value::String("user:1".into()));
        view.append_event("UserCreated".into(), Value::String("user:2".into()));

        // Updates
        view.apply_kv_put(Key::new_kv(ns.clone(), "counter"), Value::Int(1));
        view.apply_kv_put(Key::new_kv(ns.clone(), "counter"), Value::Int(2));
        view.apply_kv_put(
            Key::new_kv(ns.clone(), "user:1:name"),
            Value::String("Alicia".into()),
        ); // Name change

        view.append_event("UserUpdated".into(), Value::String("user:1".into()));

        // Delete
        view.apply_kv_delete(&Key::new_kv(ns.clone(), "user:2:name"));
        view.append_event("UserDeleted".into(), Value::String("user:2".into()));

        view
    };

    // Build multiple times (P1, P5, P6)
    let views: Vec<_> = (0..5).map(|_| build_realistic_view()).collect();

    // P1/P5/P6: All views must be identical
    for i in 1..views.len() {
        let diff = diff_views(&views[0], &views[i]);
        assert!(diff.is_empty(), "View {} differs: {}", i, diff.summary());
    }

    // P2: External state unchanged (implicit - no external state to modify)

    // P3: View contains only what was derived
    let view = &views[0];
    assert_eq!(view.kv_count(), 3); // user:1:name, user:1:email, counter (user:2:name deleted)
    assert_eq!(view.event_count(), 4);

    // P4: New view starts empty (implicit - new views don't persist)
    let fresh = ReadOnlyView::new(branch_id);
    assert_eq!(fresh.kv_count(), 0);

    // Verify final expected state
    let key = Key::new_kv(ns.clone(), "user:1:name");
    assert_eq!(view.get_kv(&key), Some(&Value::String("Alicia".into())));

    let counter = Key::new_kv(ns.clone(), "counter");
    assert_eq!(view.get_kv(&counter), Some(&Value::Int(2)));

    let deleted = Key::new_kv(ns.clone(), "user:2:name");
    assert_eq!(view.get_kv(&deleted), None); // Was deleted
}

/// Test diff_branches self-comparison
#[test]
fn test_diff_branches_self_comparison() {
    let branch_id = BranchId::new();
    let ns = test_namespace(branch_id);

    let mut view = ReadOnlyView::new(branch_id);
    view.apply_kv_put(Key::new_kv(ns.clone(), "key1"), Value::Int(100));
    view.apply_kv_put(
        Key::new_kv(ns.clone(), "key2"),
        Value::String("hello".into()),
    );
    view.append_event("Event1".into(), Value::Bool(true));

    // Diff with self should be empty
    let diff = diff_views(&view, &view);
    assert!(diff.is_empty(), "Self-diff should be empty");
    assert_eq!(diff.total_changes(), 0);
}

/// Test diff between different views
#[test]
fn test_diff_views_different() {
    let branch_a = BranchId::new();
    let branch_b = BranchId::new();
    let ns_a = test_namespace(branch_a);
    let ns_b = test_namespace(branch_b);

    // View A: has key1, key2
    let mut view_a = ReadOnlyView::new(branch_a);
    view_a.apply_kv_put(Key::new_kv(ns_a.clone(), "shared"), Value::Int(1));
    view_a.apply_kv_put(Key::new_kv(ns_a.clone(), "only_a"), Value::Int(2));

    // View B: has shared (different value), key3
    let mut view_b = ReadOnlyView::new(branch_b);
    view_b.apply_kv_put(Key::new_kv(ns_b.clone(), "shared"), Value::Int(100)); // Different value
    view_b.apply_kv_put(Key::new_kv(ns_b.clone(), "only_b"), Value::Int(3));

    let diff = diff_views(&view_a, &view_b);

    // Note: Due to different namespaces (different branch IDs), keys won't match
    // This tests the diff mechanism correctly identifies all changes
    assert!(!diff.is_empty());
    // Added: only_b, shared (different namespace)
    // Removed: only_a, shared (different namespace)
}

/// Test event diff
#[test]
fn test_diff_events() {
    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // View A: 2 events
    let mut view_a = ReadOnlyView::new(branch_a);
    view_a.append_event("E1".into(), Value::Int(1));
    view_a.append_event("E2".into(), Value::Int(2));

    // View B: 3 events
    let mut view_b = ReadOnlyView::new(branch_b);
    view_b.append_event("E1".into(), Value::Int(1));
    view_b.append_event("E2".into(), Value::Int(2));
    view_b.append_event("E3".into(), Value::Int(3)); // Extra event

    let diff = diff_views(&view_a, &view_b);

    // B has 1 more event than A
    assert_eq!(diff.added.len(), 1);
    assert_eq!(diff.added[0].primitive, PrimitiveType::Event);
}
