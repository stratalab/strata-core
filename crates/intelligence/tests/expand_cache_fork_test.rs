//! Integration test: forking a branch inherits the parent's expansion cache
//! via the storage layer's COW semantics.
//!
//! This test must use a disk-backed runtime because branch fork goes through
//! `BranchService` and the storage-layer COW path, which an in-memory
//! `Database::cache()` runtime does not exercise.

#![cfg(feature = "embed")]

use strata_engine::database::search_only_primary_spec;
use strata_engine::primitives::branch::resolve_branch_name;
use strata_engine::Database;
use strata_graph::GraphSubsystem;
use strata_intelligence::expand_cache;
use strata_search::expand::{ExpandedQuery, QueryType};
use tempfile::TempDir;

fn lex(text: &str) -> ExpandedQuery {
    ExpandedQuery {
        query_type: QueryType::Lex,
        text: text.into(),
    }
}

#[test]
fn test_fork_branch_inherits_cache() {
    let dir = TempDir::new().unwrap();
    let db =
        Database::open_runtime(search_only_primary_spec(dir.path()).with_subsystem(GraphSubsystem))
            .unwrap();

    // Create the parent branch and warm its cache with one entry.
    db.branches().create("parent").unwrap();
    let parent_id = resolve_branch_name("parent");

    let key = expand_cache::cache_key("ssh setup", "qwen3:1.7b");
    expand_cache::put(
        &db,
        parent_id,
        &key,
        "ssh setup",
        "qwen3:1.7b",
        &[lex("ssh keygen"), lex("ssh key authentication")],
        100,
    )
    .expect("parent cache write should succeed");

    // Sanity: parent sees its own entry.
    let parent_hit = expand_cache::get(&db, parent_id, &key);
    assert!(parent_hit.is_some(), "parent should have the cache entry");

    // Fork parent → child via the canonical branch service.
    db.branches()
        .fork("parent", "child")
        .expect("fork should succeed");
    let child_id = resolve_branch_name("child");

    // Child should see the inherited cache entry via COW — no model call needed
    // and no explicit cache copy. This is the free-win the per-branch storage
    // design buys.
    let child_variants = expand_cache::get(&db, child_id, &key)
        .expect("child should inherit parent's cache entry via COW");

    assert_eq!(child_variants.len(), 2);
    assert_eq!(child_variants[0].text, "ssh keygen");
    assert_eq!(child_variants[1].text, "ssh key authentication");
    assert_eq!(child_variants[0].query_type, QueryType::Lex);
}

#[test]
fn test_fork_child_writes_isolated_from_parent() {
    // After fork, writes on the child must not leak into the parent.
    // This proves the COW path is doing copy-on-write, not aliasing.
    let dir = TempDir::new().unwrap();
    let db =
        Database::open_runtime(search_only_primary_spec(dir.path()).with_subsystem(GraphSubsystem))
            .unwrap();

    db.branches().create("p").unwrap();
    let parent_id = resolve_branch_name("p");

    // Warm the parent.
    let shared_key = expand_cache::cache_key("shared query", "m");
    expand_cache::put(
        &db,
        parent_id,
        &shared_key,
        "shared query",
        "m",
        &[lex("shared")],
        100,
    )
    .unwrap();

    // Fork.
    db.branches().fork("p", "c").unwrap();
    let child_id = resolve_branch_name("c");

    // Write a NEW entry on the child only.
    let child_only_key = expand_cache::cache_key("child only", "m");
    expand_cache::put(
        &db,
        child_id,
        &child_only_key,
        "child only",
        "m",
        &[lex("c-only")],
        100,
    )
    .unwrap();

    // Child sees both; parent sees only the shared one.
    assert!(expand_cache::get(&db, child_id, &shared_key).is_some());
    assert!(expand_cache::get(&db, child_id, &child_only_key).is_some());
    assert!(expand_cache::get(&db, parent_id, &shared_key).is_some());
    assert!(
        expand_cache::get(&db, parent_id, &child_only_key).is_none(),
        "child writes must not leak into parent"
    );
}
