//! Production-path lifecycle regression tests.
//!
//! These tests lock the Epic 1-4 invariant that `Strata::open` wires the
//! full production subsystem set (`[VectorSubsystem, SearchSubsystem]`) so
//! that dropping a `Strata` handle freezes vector state to disk through
//! `VectorSubsystem::freeze`, and reopening restores it through
//! `VectorSubsystem::recover`. If anyone later reintroduces a lifecycle
//! path that skips the subsystem list (e.g. a new convenience API that
//! bypasses `strata_db_builder`), these tests regress.
//!
//! The companion
//! `crates/engine/tests/recovery_tests.rs::test_vector_collections_isolated_across_spaces_after_restart`
//! exercises the same invariant through the explicit `DatabaseBuilder`
//! API; these tests exercise it through `Strata::open`, which is the
//! shape real callers use.

use std::path::PathBuf;

use crate::types::DistanceMetric;
use crate::{OpenOptions, Strata};

/// Reconstruct the on-disk `.vec` cache path for a collection owned by
/// the executor's default branch. The executor maps the string branch
/// `"default"` to `BranchId::from_bytes([0u8; 16])` (see
/// `crates/executor/src/bridge.rs::to_core_branch_id`), which is the
/// all-zero UUID — NOT `strata_core::types::BranchId::default()` (which
/// is random).
///
/// Layout: `{data_dir}/vectors/{branch_hex}/{space}/{collection}.vec`
/// (see `crates/vector/src/recovery.rs::mmap_path`).
fn expected_vec_cache_path(data_dir: &std::path::Path, space: &str, collection: &str) -> PathBuf {
    let core_branch = strata_core::types::BranchId::from_bytes([0u8; 16]);
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*core_branch.as_bytes()));
    data_dir
        .join("vectors")
        .join(&branch_hex)
        .join(space)
        .join(format!("{}.vec", collection))
}

/// Epic 5 primary regression: a vector written through the production
/// `Strata::open` path must survive an implicit drop + reopen cycle.
///
/// This proves that `Strata::open` installs `VectorSubsystem`, so
/// `Drop for Database` → `run_freeze_hooks` → `VectorSubsystem::freeze`
/// fires on normal drop (no explicit `shutdown()` or
/// `freeze_vector_heaps()` call), and `Strata::open` on reopen runs
/// `VectorSubsystem::recover` to restore the heap from the `.vec` mmap.
///
/// Empirical revert: delete `VectorSubsystem` from `strata_db_builder()`
/// in `crates/executor/src/api/mod.rs` and this test fails — the
/// `.vec` file does not exist after drop, and the reopened query
/// returns no matches (vector state is gone).
#[test]
fn test_strata_open_drop_reopen_preserves_vectors_through_drop_freeze() {
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().to_path_buf();

    // -------------------------------------------------------------
    // Session 1 — write vectors through the production path.
    // -------------------------------------------------------------
    {
        let mut db = Strata::open(&data_dir).unwrap();
        db.set_space("tenant_a").unwrap();
        db.vector_create_collection("embeddings", 3, DistanceMetric::Cosine)
            .unwrap();
        db.vector_upsert("embeddings", "k1", vec![1.0, 0.0, 0.0], None)
            .unwrap();
        db.vector_upsert("embeddings", "k2", vec![0.0, 1.0, 0.0], None)
            .unwrap();
        db.vector_upsert("embeddings", "k3", vec![0.0, 0.0, 1.0], None)
            .unwrap();
        // NO explicit freeze_vector_heaps / shutdown — drop MUST do the
        // right thing. This is the regression-locking assertion.
    }

    // -------------------------------------------------------------
    // Disk check — the `.vec` mmap cache file must exist *between
    // sessions*. This is the tightest possible check that drop-time
    // freeze ran: the file is only written by
    // `VectorSubsystem::freeze` via `db.freeze_vector_heaps()`.
    // -------------------------------------------------------------
    let vec_path = expected_vec_cache_path(&data_dir, "tenant_a", "embeddings");
    assert!(
        vec_path.exists(),
        ".vec mmap cache file must exist after Strata drop, looked for {:?}. \
         If this fires, either Strata::open stopped installing VectorSubsystem or \
         Drop for Database stopped running freeze hooks.",
        vec_path
    );

    // -------------------------------------------------------------
    // Session 2 — reopen and verify vectors are queryable.
    // -------------------------------------------------------------
    {
        let mut db = Strata::open(&data_dir).unwrap();
        db.set_space("tenant_a").unwrap();

        // Query for k1's exact vector — must be the top hit with a
        // cosine similarity of 1.0 (perfect match). If recovery
        // silently dropped vector state, the query returns zero hits
        // or a different top key.
        let matches = db
            .vector_query("embeddings", vec![1.0, 0.0, 0.0], 3)
            .unwrap();
        assert_eq!(
            matches.len(),
            3,
            "expected 3 vectors to survive drop+reopen, got {}",
            matches.len()
        );
        let top = matches.first().expect("query returned no matches");
        assert_eq!(
            top.key, "k1",
            "top hit must be k1 (exact match for query vector)"
        );
        assert!(
            (top.score - 1.0).abs() < 1e-4,
            "k1 score must be ~1.0 (identical unit vector), got {}",
            top.score
        );
    }
}

/// Epic 5 follower regression: vectors written on a primary must be
/// observable by a follower opened through
/// `Strata::open_with(follower: true)` after `db.refresh()`.
///
/// This locks the invariant that the follower open path installs
/// `VectorSubsystem` (which registers the vector replay observer during
/// `VectorSubsystem::initialize`), so `Database::refresh()` picks up
/// vector updates incrementally via replay observers — not via full
/// re-recovery.
///
/// Empirical revert: drop `VectorSubsystem` from `strata_db_builder()`
/// on the follower branch in `Strata::open_with` and this test fails —
/// the refresh hook is never registered, so the follower never sees
/// the new vectors even after refresh.
#[test]
fn test_strata_follower_observes_primary_vector_writes_via_refresh() {
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().to_path_buf();

    // Primary opens, seeds a single vector so the follower has a
    // collection to observe. Follower is opened while the primary is
    // still alive — followers take no file lock.
    let mut primary = Strata::open(&data_dir).unwrap();
    primary.set_space("default").unwrap();
    primary
        .vector_create_collection("embeddings", 3, DistanceMetric::Cosine)
        .unwrap();
    primary
        .vector_upsert("embeddings", "seed", vec![1.0, 0.0, 0.0], None)
        .unwrap();
    // Flush so the follower's open-time recovery sees the collection
    // config and the "seed" vector on disk. Without this the initial
    // state could still be buffered in the WAL writer and the follower
    // would start from an empty snapshot.
    primary.database().flush().unwrap();

    let mut follower = Strata::open_with(&data_dir, OpenOptions::default().follower(true)).unwrap();
    follower.set_space("default").unwrap();

    // Primary writes a second vector AFTER the follower is open.
    primary
        .vector_upsert("embeddings", "later", vec![0.0, 1.0, 0.0], None)
        .unwrap();
    // Force WAL fsync so the record is durable and readable by the
    // follower before `refresh()`. Without this the Standard-durability
    // WAL writer may still be buffered, and refresh sees nothing.
    primary.database().flush().unwrap();

    // Follower refresh must pick up the new vector. This requires the
    // vector replay observer to be installed on the follower, which only
    // happens if `VectorSubsystem::initialize` ran during follower open —
    // which only happens if `strata_db_builder()` was used.
    let applied = follower.database().refresh().unwrap();
    assert!(
        applied > 0,
        "follower refresh should have applied WAL records for the new vector, got {}",
        applied
    );

    // Query for the "later" vector on the follower — must see it.
    let matches = follower
        .vector_query("embeddings", vec![0.0, 1.0, 0.0], 2)
        .unwrap();
    let has_later = matches.iter().any(|m| m.key == "later");
    assert!(
        has_later,
        "follower must observe the 'later' vector after refresh, got matches: {:?}",
        matches.iter().map(|m| &m.key).collect::<Vec<_>>()
    );
}
