//! Tier 6: Cross-Primitive Identity
//!
//! Tests for DocRef identity policies and deduplication behavior.

use crate::common::*;
use crate::common::search::substrate_search;
use strata_core::{BranchId, EntityRef as DocRef, PrimitiveType, Value};
use strata_engine::{KVStore, SearchRequest};
use std::collections::HashSet;

const DEFAULT_SPACE: &str = "default";

fn kv_doc_ref(branch_id: &BranchId, key: &str) -> DocRef {
    DocRef::kv(branch_id.clone(), DEFAULT_SPACE, key)
}

fn json_doc_ref(branch_id: &BranchId, doc_id: &str) -> DocRef {
    DocRef::json(branch_id.clone(), DEFAULT_SPACE, doc_id)
}

fn event_doc_ref(branch_id: &BranchId, sequence: u64) -> DocRef {
    DocRef::event(branch_id.clone(), DEFAULT_SPACE, sequence)
}

fn branch_doc_ref(branch_id: &BranchId) -> DocRef {
    DocRef::branch(branch_id.clone())
}

// ============================================================================
// DocRef Identity Policy Tests
// ============================================================================

/// DocRefs from different primitives are NEVER equal
#[test]
fn test_tier6_docrefs_different_primitives_never_equal() {
    let branch_id = BranchId::new();

    let kv_ref = kv_doc_ref(&branch_id, "shared_name");
    let json_ref = json_doc_ref(&branch_id, "json-doc");
    let branch_ref = branch_doc_ref(&branch_id);

    // POLICY: DocRefs from different primitives are NEVER equal
    assert_ne!(kv_ref, json_ref);
    assert_ne!(kv_ref, branch_ref);
    assert_ne!(json_ref, branch_ref);
}

/// DocRefs from same primitive with same key ARE equal
#[test]
fn test_tier6_docrefs_same_primitive_same_key_equal() {
    let branch_id = BranchId::new();

    let ref1 = kv_doc_ref(&branch_id, "same_key");
    let ref2 = kv_doc_ref(&branch_id, "same_key");

    assert_eq!(ref1, ref2);
}

/// DocRefs from same primitive with different keys are NOT equal
#[test]
fn test_tier6_docrefs_same_primitive_different_key_not_equal() {
    let branch_id = BranchId::new();

    let ref1 = kv_doc_ref(&branch_id, "key1");
    let ref2 = kv_doc_ref(&branch_id, "key2");

    assert_ne!(ref1, ref2);
}

/// DocRefs from same primitive but different branches are NOT equal
#[test]
fn test_tier6_docrefs_different_branches_not_equal() {
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    let ref1 = kv_doc_ref(&branch1, "same_key");
    let ref2 = kv_doc_ref(&branch2, "same_key");

    // Same key name but different branches = NOT equal
    assert_ne!(ref1, ref2);
}

// ============================================================================
// DocRef Hashing Tests
// ============================================================================

/// DocRefs can be used in HashSet
#[test]
fn test_tier6_docrefs_hashable() {
    let branch_id = BranchId::new();

    let ref1 = kv_doc_ref(&branch_id, "key1");
    let ref2 = kv_doc_ref(&branch_id, "key2");
    let ref3 = kv_doc_ref(&branch_id, "key1"); // Duplicate of ref1

    let mut set = HashSet::new();
    set.insert(ref1.clone());
    set.insert(ref2.clone());
    set.insert(ref3.clone());

    // ref3 is duplicate of ref1, so set should have 2 elements
    assert_eq!(set.len(), 2);
    assert!(set.contains(&ref1));
    assert!(set.contains(&ref2));
}

// ============================================================================
// Deduplication Policy Tests
// ============================================================================

/// Within-primitive search never returns duplicates
#[test]
fn test_tier6_within_primitive_no_duplicates() {
    let db = create_test_db();
    let branch_id = test_branch_id();

    let kv = KVStore::new(db.clone());
    db.branches().create(&branch_id.to_string()).unwrap();

    // Add multiple entries with overlapping content
    for i in 0..10 {
        kv.put(
            &branch_id,
            &format!("key_{}", i),
            Value::String("common search term".into()),
        )
        .unwrap();
    }

    let req = SearchRequest::new(branch_id, "common").with_k(20);
    let response = kv.search(&req).unwrap();

    // Check for duplicates
    let refs: HashSet<_> = response.hits.iter().map(|h| &h.doc_ref).collect();
    assert_eq!(
        refs.len(),
        response.hits.len(),
        "Within-primitive search should never have duplicates"
    );
}

/// Cross-primitive NO deduplication (application layer responsibility)
#[test]
fn test_tier6_cross_primitive_no_deduplication() {
    // This is a POLICY test: we document that cross-primitive
    // deduplication is NOT performed by the search layer.
    // The application layer must handle it if needed.

    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    // Results from different primitives may logically refer to the same
    // entity, but DocRefs are distinct (different variants)
    for hit in &response.hits {
        // Each hit should have a valid primitive type
        let _kind = hit.doc_ref.primitive_type();
    }
}

// ============================================================================
// Primitive Type Correctness Tests
// ============================================================================

/// DocRef.primitive_type() returns correct variant
#[test]
fn test_tier6_primitive_type_correct() {
    let branch_id = BranchId::new();

    let kv_ref = kv_doc_ref(&branch_id, "test");
    assert_eq!(kv_ref.primitive_type(), PrimitiveType::Kv);

    let json_ref = json_doc_ref(&branch_id, "json-doc");
    assert_eq!(json_ref.primitive_type(), PrimitiveType::Json);

    let event_ref = event_doc_ref(&branch_id, 42);
    assert_eq!(event_ref.primitive_type(), PrimitiveType::Event);

    let branch_ref = branch_doc_ref(&branch_id);
    assert_eq!(branch_ref.primitive_type(), PrimitiveType::Branch);
}

/// DocRef.branch_id() returns correct branch
#[test]
fn test_tier6_branch_id_correct() {
    let branch_id = BranchId::new();

    let kv_ref = kv_doc_ref(&branch_id, "test");
    assert_eq!(kv_ref.branch_id(), branch_id);

    let branch_ref = branch_doc_ref(&branch_id);
    assert_eq!(branch_ref.branch_id(), branch_id);
}
