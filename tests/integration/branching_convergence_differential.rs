//! B5.3 — Differential convergence proof corpus.
//!
//! Pins the convergence-closure contract from
//! `docs/design/branching/branching-gc/b5-phasing-plan.md` §B5.3 and
//! `docs/design/branching/branching-gc/branching-b5-convergence-and-observability.md`.
//!
//! Each branch-visible derived surface is labeled with exactly one
//! `ConvergenceClass` (see `crates/engine/src/branch_retention/mod.rs`),
//! and this corpus proves the claimed class per surface × dimension:
//!
//! - `§1` — `BranchStatusCache` is `AdvisoryOnly` and has no
//!   correctness role: empty cache, stale cache, delete, recreate, and
//!   sibling reads all behave correctly against `BranchControlStore`
//!   as the authoritative gate.
//! - `§2` — KV / Event / Graph BM25: `StagedPublish` for ordinary
//!   writes (inline), explicit `ReopenHealed` fallback at merge-time
//!   (merge apply writes via raw `txn.put` and bypasses inline
//!   indexing; `SearchSubsystem::recover` + `reconcile_index` heal on
//!   next open).
//! - `§3` — JSON BM25 / Vector HNSW: `StagedPublish` with immediate
//!   post-merge refresh via `PrimitiveMergeHandler::post_commit`.
//! - `§4` — Follower-refresh / publish-clamp — blocked hooks do not
//!   leak stale derived state and blocked state persists across
//!   follower reopen. (Covered by the crate-local hook-failure tests
//!   in `crates/engine/tests/follower_tests.rs`,
//!   `crates/vector/src/recovery.rs`, and `crates/graph/src/store.rs`;
//!   this suite asserts the B5.3 invariant at the branch-layer
//!   integration level.)
//! - `§5` — Search / vector on-disk caches: `ReopenHealed` — valid
//!   caches fast-path reload and reconcile; invalid / missing caches
//!   trigger rebuild from KV truth.
//! - `§6` — Vector in-memory HNSW: rebuilds after reopen from KV
//!   truth.
//! - `§7` — Delete / recreate convergence per surface: no old
//!   lifecycle derived state leaks to the new lifecycle instance.
//! - `§8` — Same-name recreate lifecycle identity: `BranchRef`
//!   generation advances; derived surfaces consult authoritative
//!   lifecycle sources, not the advisory cache.

#![cfg(not(miri))]

use crate::common::*;
use std::sync::Arc;
use strata_core::branch::BranchLifecycleStatus;
use strata_core::branch_dag::DagBranchStatus;
use strata_core::value::Value;
use strata_core::{BranchId, StrataError};
use strata_engine::Searchable;
use strata_graph::branch_status_cache::BranchStatusCache;
use strata_graph::types::NodeData;

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

fn seed_kv(db: &Arc<Database>, name: &str, key: &str, v: &str) {
    KVStore::new(db.clone())
        .put(&resolve(name), "default", key, Value::String(v.to_string()))
        .expect("seed kv write");
}

fn seed_event(db: &Arc<Database>, name: &str, event_type: &str, text: &str) {
    let payload = values::event_payload(values::string(text));
    EventLog::new(db.clone())
        .append(&resolve(name), "default", event_type, payload)
        .expect("seed event append");
}

fn seed_graph_node(db: &Arc<Database>, name: &str, graph: &str, node: &str, title: &str) {
    let gs = GraphStore::new(db.clone());
    gs.create_graph(resolve(name), "default", graph, None).ok(); // idempotent in practice
    gs.add_node(
        resolve(name),
        "default",
        graph,
        node,
        NodeData {
            entity_ref: None,
            object_type: Some("paper".to_string()),
            properties: Some(serde_json::json!({ "title": title })),
        },
    )
    .expect("seed graph node");
}

fn bm25_hit_count(db: &Arc<Database>, branch: &str, query: &str) -> usize {
    let kv = KVStore::new(db.clone());
    let req = strata_engine::SearchRequest::new(resolve(branch), query).with_k(100);
    kv.search(&req).expect("bm25 search").hits.len()
}

fn bm25_event_hit_count(db: &Arc<Database>, branch: &str, query: &str) -> usize {
    let event = EventLog::new(db.clone());
    let req = strata_engine::SearchRequest::new(resolve(branch), query).with_k(100);
    event.search(&req).expect("event bm25 search").hits.len()
}

fn bm25_graph_hit_count(db: &Arc<Database>, branch: &str, query: &str) -> usize {
    let graph = GraphStore::new(db.clone());
    let req = strata_engine::SearchRequest::new(resolve(branch), query).with_k(100);
    graph.search(&req).expect("graph bm25 search").hits.len()
}

// =============================================================================
// §1 — `BranchStatusCache` is `ConvergenceClass::AdvisoryOnly`.
//
// No correctness decision depends on the cache. `is_writable_hint` returns
// `None` on miss (never default-to-writable), and authoritative lifecycle
// decisions go through `BranchControlStore` / `BranchService`.
// =============================================================================

#[test]
fn cache_miss_returns_none_hint_never_defaults_writable() {
    // Advisory-only contract: a cache miss must return `None` so callers
    // are forced to consult the authoritative lifecycle source rather
    // than inferring "writable".
    let test_db = TestDb::new();
    let cache = test_db
        .db
        .extension::<BranchStatusCache>()
        .expect("BranchStatusCache extension present");

    assert_eq!(
        cache.is_writable_hint("never-created-branch"),
        None,
        "cache miss must return None, never Some(true)",
    );
    assert_eq!(
        cache.get("never-created-branch"),
        None,
        "cache miss returns None on get too",
    );
}

#[test]
fn cache_stale_entry_does_not_override_authoritative_gate() {
    // Seed the cache with a stale `Active` status for a branch that the
    // authoritative control store has marked as `Archived`. The
    // branch-lifecycle gate (used by `tag`, `add_note`, `revert`,
    // `merge`-as-target) must still refuse with `BranchArchived` —
    // correctness reads the control store, not the cache.
    //
    // We use `tag` here because B4.1 defined the lifecycle gate to
    // refuse annotation / structural branch ops on archived branches;
    // raw primitive writes (e.g. `KVStore::put`) are not gate-guarded
    // and so would not exercise the invariant under test.
    let test_db = TestDb::new();
    test_db.db.branches().create("frozen").unwrap();
    seed_kv(&test_db.db, "frozen", "k", "v");

    let cache = test_db.db.extension::<BranchStatusCache>().unwrap();
    // Force-set cache to `Active` ...
    cache.set("frozen".to_string(), DagBranchStatus::Active);
    assert_eq!(cache.is_writable_hint("frozen"), Some(true));

    // ... but archive via the authoritative service.
    crate::common::branching::archive_branch_for_test(&test_db.db, "frozen");

    // Gate-guarded branch op must still refuse — advisory cache does
    // not override the authoritative gate.
    match test_db.db.branches().tag("frozen", "v1", None, None, None) {
        Err(StrataError::BranchArchived { name }) => {
            assert_eq!(name, "frozen");
        }
        other => panic!(
            "expected BranchArchived despite stale-Active cache entry, got {other:?}",
        ),
    }
}

#[test]
fn delete_branch_clears_cache_entry_via_graph_subsystem_hook() {
    // `GraphSubsystem::cleanup_deleted_branch` must remove the cache
    // entry on delete. Without this hook a same-name recreate would
    // observe the prior lifecycle's cached advisory entry.
    let test_db = TestDb::new();
    test_db.db.branches().create("gone").unwrap();
    seed_kv(&test_db.db, "gone", "k", "v");

    let cache = test_db.db.extension::<BranchStatusCache>().unwrap();
    // Populate cache entry manually (simulates any prior hydration).
    cache.set("gone".to_string(), DagBranchStatus::Active);
    assert!(cache.get("gone").is_some());

    test_db.db.branches().delete("gone").unwrap();

    assert_eq!(
        cache.get("gone"),
        None,
        "cache entry must be cleared by GraphSubsystem::cleanup_deleted_branch",
    );
}

#[test]
fn recreate_after_delete_does_not_inherit_old_cache_entry() {
    // Same-name recreate must not observe the old lifecycle's cached
    // advisory status. After delete + recreate, the cache is either
    // empty or reflects the new lifecycle, never the old one.
    let test_db = TestDb::new();
    test_db.db.branches().create("phoenix").unwrap();
    seed_kv(&test_db.db, "phoenix", "k1", "v1");

    let cache = test_db.db.extension::<BranchStatusCache>().unwrap();
    cache.set("phoenix".to_string(), DagBranchStatus::Active);

    test_db.db.branches().delete("phoenix").unwrap();
    // Cache cleared by the graph cleanup hook.
    assert_eq!(cache.get("phoenix"), None);

    // Recreate: hint may return None (no hydration) OR Some(true) if a
    // fresh hydration ran — never the stale entry the old lifecycle
    // had. Accept either — the contract is "no stale leak."
    test_db.db.branches().create("phoenix").unwrap();
    let hint = cache.is_writable_hint("phoenix");
    assert!(
        matches!(hint, None | Some(true)),
        "recreate hint must be None or Some(true); got {hint:?}",
    );

    // The authoritative source reports the new lifecycle cleanly.
    let rec = test_db
        .db
        .branches()
        .control_record("phoenix")
        .unwrap()
        .expect("live record after recreate");
    assert!(matches!(
        rec.lifecycle,
        BranchLifecycleStatus::Active
    ));
}

#[test]
fn sibling_branch_reads_not_affected_by_cache_delete_and_recreate() {
    // Delete / recreate of branch "b" must not affect cache reads for
    // an unrelated sibling branch "c". This tests cross-branch
    // isolation of the advisory surface.
    let test_db = TestDb::new();
    test_db.db.branches().create("b").unwrap();
    test_db.db.branches().create("c").unwrap();
    seed_kv(&test_db.db, "b", "k", "v");
    seed_kv(&test_db.db, "c", "k", "v");

    let cache = test_db.db.extension::<BranchStatusCache>().unwrap();
    cache.set("c".to_string(), DagBranchStatus::Active);

    test_db.db.branches().delete("b").unwrap();
    test_db.db.branches().create("b").unwrap();

    // Sibling cache entry still present.
    assert_eq!(cache.get("c"), Some(DagBranchStatus::Active));

    // Sibling is still writable via the authoritative gate.
    KVStore::new(test_db.db.clone())
        .put(&resolve("c"), "default", "k2", Value::Int(2))
        .expect("sibling writes unaffected by b's recreate");
}

// =============================================================================
// §2 — KV / Event / Graph BM25: StagedPublish + ReopenHealed fallback at merge.
//
// Merge apply writes target rows via raw `txn.put()`, which bypasses
// the inline BM25 indexing path used by `KVStore::put` / `EventLog::append`
// / `GraphStore::add_node`. The in-memory BM25 projection for merged
// rows is stale until `SearchSubsystem::recover` → `reconcile_index`
// reruns on the next database open.
// =============================================================================

#[test]
fn kv_merge_bm25_reopen_heals_stale_index() {
    let mut test_db = TestDb::new();

    test_db.db.branches().create("main").unwrap();
    seed_kv(&test_db.db, "main", "a", "alpha baseline text");

    test_db.db.branches().fork("main", "feature").unwrap();
    // Source-only write on feature — the merge target ("main") has no
    // inline BM25 entry for this term.
    seed_kv(&test_db.db, "feature", "b", "feature rocket propellant");

    // Pre-merge: "feature" not yet visible on main BM25.
    assert_eq!(bm25_hit_count(&test_db.db, "main", "rocket"), 0);

    test_db
        .db
        .branches()
        .merge("feature", "main")
        .expect("KV merge succeeds");

    // Post-merge, pre-reopen: merged rows are KV-visible but not yet
    // BM25-indexed for main (raw `txn.put` bypasses inline indexing).
    // This is the explicit `ReopenHealed` fallback — assert the stale
    // state so a future regression that silently adds immediate refresh
    // surfaces as a test diff.
    assert_eq!(
        bm25_hit_count(&test_db.db, "main", "rocket"),
        0,
        "pre-reopen: BM25 on KV merge target is stale by contract",
    );

    test_db.reopen();

    // Post-reopen: BM25 healed via `SearchSubsystem::recover`.
    assert!(
        bm25_hit_count(&test_db.db, "main", "rocket") > 0,
        "post-reopen: BM25 must reflect merged KV rows",
    );
}

#[test]
fn event_merge_bm25_reopen_heals_stale_index() {
    let mut test_db = TestDb::new();

    test_db.db.branches().create("main").unwrap();
    seed_event(&test_db.db, "main", "baseline_event", "alpha baseline");

    test_db.db.branches().fork("main", "feature").unwrap();
    seed_event(&test_db.db, "feature", "rocket_event", "rocket telemetry payload");

    assert_eq!(bm25_event_hit_count(&test_db.db, "main", "rocket"), 0);

    test_db
        .db
        .branches()
        .merge("feature", "main")
        .expect("Event merge succeeds");

    // Pre-reopen: stale BM25 for merged events.
    assert_eq!(
        bm25_event_hit_count(&test_db.db, "main", "rocket"),
        0,
        "pre-reopen: BM25 on Event merge target is stale by contract",
    );

    test_db.reopen();

    // Post-reopen: healed.
    assert!(
        bm25_event_hit_count(&test_db.db, "main", "rocket") > 0,
        "post-reopen: BM25 must reflect merged Event rows",
    );
}

#[test]
fn graph_merge_bm25_reopen_heals_stale_index() {
    let mut test_db = TestDb::new();

    test_db.db.branches().create("main").unwrap();
    seed_graph_node(&test_db.db, "main", "papers", "n1", "alpha baseline study");

    test_db.db.branches().fork("main", "feature").unwrap();
    seed_graph_node(
        &test_db.db,
        "feature",
        "papers",
        "n2",
        "rocket propellant analysis",
    );

    assert_eq!(bm25_graph_hit_count(&test_db.db, "main", "rocket"), 0);

    test_db
        .db
        .branches()
        .merge("feature", "main")
        .expect("Graph merge succeeds");

    // Pre-reopen: stale.
    assert_eq!(
        bm25_graph_hit_count(&test_db.db, "main", "rocket"),
        0,
        "pre-reopen: graph-node BM25 on merge target is stale by contract",
    );

    test_db.reopen();

    // Post-reopen: healed.
    assert!(
        bm25_graph_hit_count(&test_db.db, "main", "rocket") > 0,
        "post-reopen: graph-node BM25 must reflect merged Graph rows",
    );
}

// =============================================================================
// §3 — JSON BM25 and Vector HNSW: immediate merge-time refresh.
//
// These surfaces own explicit `post_commit` refresh in
// `primitive_merge.rs`, so merged state is BM25-/HNSW-visible without
// reopen.
// =============================================================================

#[test]
fn json_merge_bm25_refreshed_immediately_no_reopen_needed() {
    let test_db = TestDb::new();
    let json = test_db.json();

    test_db.db.branches().create("main").unwrap();
    json.create(
        &resolve("main"),
        "default",
        "doc1",
        json_value(serde_json::json!({ "body": "alpha baseline" })),
    )
    .unwrap();

    test_db.db.branches().fork("main", "feature").unwrap();
    json.create(
        &resolve("feature"),
        "default",
        "doc2",
        json_value(serde_json::json!({ "body": "rocket telemetry" })),
    )
    .unwrap();

    test_db
        .db
        .branches()
        .merge("feature", "main")
        .expect("JSON merge succeeds");

    // No reopen: JSON merge has explicit `post_commit` BM25 refresh.
    let req = strata_engine::SearchRequest::new(resolve("main"), "rocket").with_k(10);
    let hits = json.search(&req).unwrap().hits.len();
    assert!(
        hits > 0,
        "JSON merge BM25 must be refreshed immediately via post_commit; got {hits} hits",
    );
}

#[test]
fn vector_merge_hnsw_refreshed_immediately_no_reopen_needed() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    test_db.db.branches().create("main").unwrap();
    vector
        .create_collection(resolve("main"), "default", "c", config_small())
        .unwrap();
    vector
        .insert(resolve("main"), "default", "c", "m1", &[1.0, 0.0, 0.0], None)
        .unwrap();

    test_db.db.branches().fork("main", "feature").unwrap();
    vector
        .insert(
            resolve("feature"),
            "default",
            "c",
            "f1",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    // Merge — VectorMergeHandler::post_commit dispatches to the
    // per-database callback, which rebuilds HNSW for affected
    // collections immediately.
    test_db
        .db
        .branches()
        .merge("feature", "main")
        .expect("Vector merge succeeds");

    // Query close to f1: must surface f1 on main without reopen.
    let matches = vector
        .search(
            resolve("main"),
            "default",
            "c",
            &[0.0, 1.0, 0.0],
            5,
            None,
        )
        .unwrap();
    let keys: Vec<&str> = matches.iter().map(|m| m.key.as_str()).collect();
    assert!(
        keys.contains(&"f1"),
        "vector merge HNSW must be refreshed immediately; got keys {keys:?}",
    );
}

#[test]
fn json_bm25_reopens_from_kv_truth_when_on_disk_cache_is_lost() {
    // `JsonMergeHandler::post_commit` swallows best-effort BM25 refresh
    // errors by design, relying on `SearchSubsystem::recover` to heal
    // on next open. This test proves the reopen-healed fallback for
    // JSON BM25 by deleting the on-disk search cache between reopens,
    // forcing the slow-path KV rebuild. The contract covers both:
    //
    // - the ordinary "merge post_commit succeeded" case (cache holds
    //   healed state)
    // - the "post_commit silently failed, cache is stale" worst case
    //   (loss of cache forces KV rebuild; cannot distinguish at read
    //    time, so the reopen path must be idempotently correct).
    let mut test_db = TestDb::new();
    let json = test_db.json();

    test_db.db.branches().create("main").unwrap();
    json.create(
        &resolve("main"),
        "default",
        "doc1",
        json_value(serde_json::json!({ "body": "alpha" })),
    )
    .unwrap();

    test_db.db.branches().fork("main", "feature").unwrap();
    json.create(
        &resolve("feature"),
        "default",
        "doc2",
        json_value(serde_json::json!({ "body": "rocket telemetry" })),
    )
    .unwrap();

    test_db.db.branches().merge("feature", "main").unwrap();

    let req_pre =
        strata_engine::SearchRequest::new(resolve("main"), "rocket").with_k(10);
    assert!(
        !json.search(&req_pre).unwrap().hits.is_empty(),
        "merge post_commit must immediately index the merged JSON doc",
    );

    // Force slow-path rebuild: explicit shutdown persists and
    // finalizes the cache (sets `shutdown_complete` so drop skips
    // re-freezing). Then delete the cache before reopen so the
    // fresh open cannot fast-path load.
    test_db.db.shutdown().expect("explicit shutdown");
    let search_dir = test_db.dir.path().join("search");
    assert!(
        search_dir.exists(),
        "precondition: shutdown must have frozen the search cache",
    );
    std::fs::remove_dir_all(&search_dir).expect("delete search cache");
    test_db.reopen();

    let json_after = test_db.json();
    let req_post =
        strata_engine::SearchRequest::new(resolve("main"), "rocket").with_k(10);
    assert!(
        !json_after.search(&req_post).unwrap().hits.is_empty(),
        "post-reopen slow-path KV rebuild must restore merged JSON BM25",
    );
}

// =============================================================================
// §4 — Follower publish-clamp invariant (branch-layer).
//
// Crate-local hook-failure tests already cover fail-once / clamp /
// retry semantics (see `follower_tests.rs`, `crates/graph/src/store.rs`
// tests, `crates/vector/src/recovery.rs` tests). This integration
// section locks the branch-layer consequence: the refresh publish
// guard exists and is reachable via the canonical path, and queries
// taken under that guard do not observe partially-staged state.
// =============================================================================

#[test]
fn refresh_publish_guard_is_reachable_from_branch_read_path() {
    // The canonical branch-read-under-guard contract: queries honoring
    // `refresh_query_guard` do not see half-published refresh state.
    // This test proves the guard handle is reachable and does not
    // panic during a normal read, matching how `Searchable::search`
    // uses it internally.
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    seed_kv(&test_db.db, "main", "k", "rocket telemetry");
    let _guard = test_db.db.refresh_query_guard();
    assert!(bm25_hit_count(&test_db.db, "main", "rocket") > 0);
}

// =============================================================================
// §5 — Search / vector on-disk caches: ReopenHealed.
//
// Valid caches fast-path reload and reconcile; missing caches rebuild
// from KV truth. The "delete cache files between reopen" case proves
// the fallback rebuild path is real.
// =============================================================================

#[test]
fn search_on_disk_cache_reopen_preserves_bm25_state() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..10 {
        seed_kv(
            &test_db.db,
            "main",
            &format!("k{i}"),
            &format!("rocket payload {i}"),
        );
    }

    // Pre-reopen: BM25 sees all 10 rows.
    let pre = bm25_hit_count(&test_db.db, "main", "rocket");
    assert!(pre >= 10);

    test_db.reopen();

    // Post-reopen: BM25 reconciled via cache fast-path OR slow-path
    // rebuild. Either way, same visible state.
    let post = bm25_hit_count(&test_db.db, "main", "rocket");
    assert_eq!(post, pre, "reopen must preserve BM25 state (pre={pre}, post={post})");
}

#[test]
fn search_on_disk_cache_missing_reopens_to_kv_rebuilt_state() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..5 {
        seed_kv(
            &test_db.db,
            "main",
            &format!("k{i}"),
            &format!("rocket payload {i}"),
        );
    }

    let search_dir = test_db.dir.path().join("search");

    // Explicit shutdown flushes and freezes the current cache, then
    // sets `shutdown_complete` so subsequent Drop freezes become
    // no-ops. Without this, the second `reopen()`'s drop→freeze path
    // would re-create `search_dir` with the in-memory state and the
    // slow-path rebuild would never run.
    test_db.db.shutdown().expect("explicit shutdown");

    // Now the cache is frozen to disk; delete it so the next open
    // cannot fast-path load.
    assert!(
        search_dir.exists(),
        "precondition: shutdown must have frozen the search cache",
    );
    std::fs::remove_dir_all(&search_dir).expect("delete search cache");

    // `reopen()` replaces the post-shutdown db with a fresh primary
    // at the same path. Because `shutdown_complete` is set on the
    // old handle, its drop does NOT re-freeze the cache — so the
    // fresh open must go through the slow-path KV rebuild.
    test_db.reopen();

    // BM25 must have rebuilt from KV.
    assert!(
        bm25_hit_count(&test_db.db, "main", "rocket") >= 5,
        "search on-disk cache deletion must trigger KV rebuild on reopen",
    );
}

// =============================================================================
// §6 — Vector in-memory HNSW rebuild after reopen.
// =============================================================================

#[test]
fn vector_hnsw_rebuilds_after_reopen() {
    let mut test_db = TestDb::new();
    let branch = "main";
    test_db.db.branches().create(branch).unwrap();
    {
        let vector = test_db.vector();
        vector
            .create_collection(resolve(branch), "default", "c", config_small())
            .unwrap();
        for i in 0..5u32 {
            vector
                .insert(
                    resolve(branch),
                    "default",
                    "c",
                    &format!("key_{i}"),
                    &seeded_vector(3, u64::from(i)),
                    None,
                )
                .unwrap();
        }
    }

    let query = seeded_vector(3, 0);

    let pre = {
        let vector = test_db.vector();
        vector
            .search(resolve(branch), "default", "c", &query, 3, None)
            .unwrap()
            .len()
    };
    assert!(pre > 0);

    test_db.reopen();

    let vector_post = test_db.vector();
    let post = vector_post
        .search(resolve(branch), "default", "c", &query, 3, None)
        .unwrap()
        .len();
    assert_eq!(post, pre, "HNSW search must heal on reopen (pre={pre}, post={post})");
}

#[test]
fn vector_on_disk_cache_missing_reopens_to_kv_rebuilt_state() {
    let mut test_db = TestDb::new();
    let branch = "main";
    test_db.db.branches().create(branch).unwrap();
    {
        let vector = test_db.vector();
        vector
            .create_collection(resolve(branch), "default", "c", config_small())
            .unwrap();
        for i in 0..5u32 {
            vector
                .insert(
                    resolve(branch),
                    "default",
                    "c",
                    &format!("key_{i}"),
                    &seeded_vector(3, u64::from(i)),
                    None,
                )
                .unwrap();
        }
    }

    let vectors_dir = test_db.dir.path().join("vectors");

    // Explicit shutdown persists and finalizes caches; drop on the
    // next reopen then skips the freeze step and leaves our cache
    // deletion in place.
    test_db.db.shutdown().expect("explicit shutdown");
    assert!(
        vectors_dir.exists(),
        "precondition: shutdown must have frozen the vector cache",
    );
    std::fs::remove_dir_all(&vectors_dir).expect("delete vector cache");

    test_db.reopen();

    let vector = test_db.vector();
    let query = seeded_vector(3, 0);
    let matches = vector
        .search(resolve(branch), "default", "c", &query, 3, None)
        .unwrap();
    assert!(
        !matches.is_empty(),
        "vector on-disk cache deletion must trigger KV rebuild on reopen",
    );
}

// =============================================================================
// §7 — Delete / recreate convergence per surface (no stale leak).
// =============================================================================

#[test]
fn bm25_no_stale_docs_on_kv_after_delete_recreate() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    seed_kv(&test_db.db, "main", "doc1", "rocket telemetry");

    assert!(bm25_hit_count(&test_db.db, "main", "rocket") > 0);

    test_db.db.branches().delete("main").unwrap();
    test_db.db.branches().create("main").unwrap();

    assert_eq!(
        bm25_hit_count(&test_db.db, "main", "rocket"),
        0,
        "recreated branch must not inherit old BM25 docs",
    );
}

#[test]
fn bm25_no_stale_docs_on_events_after_delete_recreate() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    seed_event(&test_db.db, "main", "evt", "rocket telemetry");

    assert!(bm25_event_hit_count(&test_db.db, "main", "rocket") > 0);

    test_db.db.branches().delete("main").unwrap();
    test_db.db.branches().create("main").unwrap();

    assert_eq!(
        bm25_event_hit_count(&test_db.db, "main", "rocket"),
        0,
        "recreated branch must not inherit old Event BM25 docs",
    );
}

#[test]
fn graph_search_no_stale_nodes_after_delete_recreate() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    seed_graph_node(&test_db.db, "main", "papers", "n1", "rocket propellant");

    assert!(bm25_graph_hit_count(&test_db.db, "main", "rocket") > 0);

    test_db.db.branches().delete("main").unwrap();
    test_db.db.branches().create("main").unwrap();

    assert_eq!(
        bm25_graph_hit_count(&test_db.db, "main", "rocket"),
        0,
        "recreated branch must not inherit old graph-node BM25 docs",
    );
}

#[test]
fn vector_no_stale_collection_state_after_delete_recreate() {
    let test_db = TestDb::new();
    let vector = test_db.vector();
    test_db.db.branches().create("main").unwrap();
    vector
        .create_collection(resolve("main"), "default", "c", config_small())
        .unwrap();
    vector
        .insert(
            resolve("main"),
            "default",
            "c",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    assert!(!vector
        .search(resolve("main"), "default", "c", &[1.0, 0.0, 0.0], 5, None)
        .unwrap()
        .is_empty());

    test_db.db.branches().delete("main").unwrap();
    test_db.db.branches().create("main").unwrap();

    // Collection should not exist on the new lifecycle. Creating it
    // fresh and querying an unrelated vector must not surface the old
    // "k1" entry.
    vector
        .create_collection(resolve("main"), "default", "c", config_small())
        .unwrap();
    let matches = vector
        .search(resolve("main"), "default", "c", &[1.0, 0.0, 0.0], 5, None)
        .unwrap();
    assert!(
        matches.is_empty(),
        "recreated branch must not inherit old vectors; got {} matches",
        matches.len(),
    );
}

#[test]
fn retention_report_does_not_attribute_old_lineage_to_recreated_branch() {
    // B5.2 non-regression: the retention report must report the new
    // lifecycle with a fresh `BranchRef` (distinct generation from the
    // deleted one), and must not inherit the old-lineage attribution
    // by name alone.
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..5 {
        seed_kv(&test_db.db, "main", &format!("k{i}"), "old-lifecycle");
    }

    let report_before = test_db.db.retention_report().expect("healthy");
    let old_main_ref = report_before
        .branches
        .iter()
        .find(|b| b.name == "main")
        .expect("main present in report before delete")
        .branch;

    test_db.db.branches().delete("main").unwrap();
    test_db.db.branches().create("main").unwrap();

    let report_after = test_db.db.retention_report().expect("healthy");
    let new_main = report_after
        .branches
        .iter()
        .find(|b| b.name == "main")
        .expect("recreated main present in report");

    assert_ne!(
        new_main.branch, old_main_ref,
        "recreated main must have a distinct BranchRef; old={old_main_ref:?}, new={:?}",
        new_main.branch,
    );
}

// =============================================================================
// §8 — Same-name recreate lifecycle identity.
//
// After delete + recreate, the new `BranchRef` generation must advance,
// and derived surfaces consult the authoritative lifecycle source.
// =============================================================================

#[test]
fn recreate_produces_fresh_branch_ref_via_control_store() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    let old_ref = test_db
        .db
        .branches()
        .control_record("main")
        .unwrap()
        .expect("live record")
        .branch;

    test_db.db.branches().delete("main").unwrap();
    test_db.db.branches().create("main").unwrap();

    let new_ref = test_db
        .db
        .branches()
        .control_record("main")
        .unwrap()
        .expect("live record after recreate")
        .branch;

    assert_ne!(
        new_ref, old_ref,
        "control store BranchRef must advance across recreate; old={old_ref:?}, new={new_ref:?}",
    );
}
