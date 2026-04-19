//! Regression tests for D7 / DG-017 — `SearchSubsystem::cleanup_deleted_branch`.

use super::*;
use crate::primitives::branch::resolve_branch_name;
use crate::recovery::Subsystem;
use crate::search::{EntityRef, InvertedIndex, SearchSubsystem};

fn kv_ref(branch_id: BranchId, key: &str) -> EntityRef {
    EntityRef::Kv {
        branch_id,
        space: "default".to_string(),
        key: key.to_string(),
    }
}

#[test]
fn search_subsystem_cleans_deleted_branch() {
    // Note: docs are indexed under the executor branch_id (deterministic
    // from name), which is what `BranchIndex::delete_branch` passes to the
    // subsystem cleanup hook. The metadata UUID on `BranchMetadata` is a
    // different value and would not match the cleanup's filter.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open_runtime(
        crate::database::OpenSpec::primary(&db_path).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let index = db.extension::<InvertedIndex>().unwrap();
    let branches = db.branches();
    branches.create("keeper").unwrap();
    branches.create("victim").unwrap();
    let branch_keep = resolve_branch_name("keeper");
    let branch_drop = resolve_branch_name("victim");

    index.index_document(&kv_ref(branch_keep, "k1"), "alpha shared text", None);
    index.index_document(&kv_ref(branch_drop, "v1"), "victim only document", None);
    index.index_document(&kv_ref(branch_drop, "v2"), "another victim record", None);
    assert_eq!(index.total_docs(), 3);

    branches.delete("victim").unwrap();

    // Subsystem hook ran inline on delete: victim docs are gone, keeper stays.
    assert_eq!(index.total_docs(), 1);
    assert_eq!(index.doc_freq("victim"), 0);
    assert_eq!(index.doc_freq("alpha"), 1);
}

#[test]
fn search_cleanup_persists_to_manifest() {
    // After cleanup_deleted_branch fires, the persisted manifest must not
    // reference the deleted branch's docs — otherwise reopen would
    // resurrect them.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_keep = resolve_branch_name("keeper");
    let branch_drop = resolve_branch_name("victim");

    {
        let db = Database::open_runtime(
            crate::database::OpenSpec::primary(&db_path).with_subsystem(SearchSubsystem),
        )
        .unwrap();

        let index = db.extension::<InvertedIndex>().unwrap();
        let branches = db.branches();
        branches.create("keeper").unwrap();
        branches.create("victim").unwrap();

        index.index_document(&kv_ref(branch_keep, "k1"), "alpha shared text", None);
        index.index_document(&kv_ref(branch_drop, "v1"), "victim only document", None);

        // Force a freeze before the delete so the persisted manifest contains
        // the victim doc — that guarantees we are testing rewrite-on-cleanup
        // rather than just an unfrozen state.
        SearchSubsystem.freeze(&db).unwrap();

        branches.delete("victim").unwrap();

        // Shutdown freezes the index again; cleanup_deleted_branch should
        // already have done so but a final shutdown freeze is a fair test.
        db.shutdown().unwrap();
    }

    // Reopen and check that the recovered index has no victim docs.
    let db = Database::open_runtime(
        crate::database::OpenSpec::primary(&db_path).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let index = db.extension::<InvertedIndex>().unwrap();

    // Live document count reflects the post-cleanup state, not the
    // pre-cleanup snapshot. Sealed-segment doc_freq is unaffected (it
    // tracks raw term counts and relies on tombstones at query time),
    // so the right invariants here are total_docs and has_document.
    assert_eq!(
        index.total_docs(),
        1,
        "total_docs must reflect cleanup after reopen"
    );
    assert!(
        index.has_document(&kv_ref(branch_keep, "k1")),
        "keeper doc must survive reopen"
    );
    assert!(
        !index.has_document(&kv_ref(branch_drop, "v1")),
        "victim doc must not be present after reopen"
    );
}

#[test]
fn search_cleanup_is_noop_for_branch_with_no_indexed_docs() {
    // A branch deletion when the branch has zero indexed documents must
    // be a clean noop — no spurious freeze, no error.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open_runtime(
        crate::database::OpenSpec::primary(&db_path).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let index = db.extension::<InvertedIndex>().unwrap();
    let branches = db.branches();
    branches.create("keeper").unwrap();
    branches.create("empty").unwrap();
    let branch_keep = resolve_branch_name("keeper");

    // Only the keeper branch has indexed docs; the empty branch never gets
    // an `index_document` call.
    index.index_document(&kv_ref(branch_keep, "k1"), "alpha", None);
    assert_eq!(index.total_docs(), 1);

    branches.delete("empty").unwrap();

    // Keeper untouched; empty's deletion did not error and removed nothing.
    assert_eq!(index.total_docs(), 1);
    assert!(index.has_document(&kv_ref(branch_keep, "k1")));
}
