//! Regression tests for D7 / DG-015 snapshot retention.

use super::*;
use strata_storage::durability::list_snapshots;

fn write_one(db: &Database, branch_id: BranchId, key_name: &str) {
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, key_name);
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String("payload".into()))?;
        Ok(())
    })
    .unwrap();
}

#[test]
fn prune_keeps_retain_count_newest_snapshots() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    {
        let mut cfg = db.config.write();
        cfg.snapshot_retention.retain_count = 3;
    }

    let branch_id = BranchId::new();
    for i in 0..10 {
        write_one(&db, branch_id, &format!("k{}", i));
        db.checkpoint().unwrap();
    }

    let snapshots_dir = db_path.canonicalize().unwrap().join("snapshots");
    let snapshots = list_snapshots(&snapshots_dir).unwrap();
    assert_eq!(
        snapshots.len(),
        3,
        "expected exactly retain_count=3 snapshots after 10 checkpoints"
    );

    // The kept ids should be the three highest. Snapshot ids start at 1.
    let kept_ids: Vec<u64> = snapshots.iter().map(|(id, _)| *id).collect();
    assert_eq!(kept_ids, vec![8, 9, 10]);
}

#[test]
fn prune_preserves_live_manifest_snapshot_in_steady_state() {
    // Steady-state check: with retain_count=1 and a sequence of
    // checkpoints, the post-checkpoint pruner always preserves the live
    // MANIFEST snapshot because in normal operation MANIFEST.snapshot_id ==
    // the newest snap-id.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    {
        let mut cfg = db.config.write();
        cfg.snapshot_retention.retain_count = 1;
    }

    let branch_id = BranchId::new();
    for i in 0..5 {
        write_one(&db, branch_id, &format!("k{}", i));
        db.checkpoint().unwrap();
    }

    let snapshots_dir = db_path.canonicalize().unwrap().join("snapshots");
    let kept_ids: Vec<u64> = list_snapshots(&snapshots_dir)
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert_eq!(
        kept_ids.len(),
        1,
        "retain_count=1 should leave one snapshot"
    );

    let manifest_path = db_path.canonicalize().unwrap().join("MANIFEST");
    let manifest = strata_storage::durability::ManifestManager::load(manifest_path).unwrap();
    let live_id = manifest.manifest().snapshot_id.unwrap();
    assert_eq!(kept_ids[0], live_id);
}

#[test]
fn prune_preserves_live_manifest_snapshot_when_outside_retain_window() {
    // Corner case: MANIFEST.snapshot_id points to an OLD snapshot that
    // would otherwise fall outside `retain_count`. This can happen in
    // production if a checkpoint wrote the snap-N+1 file but failed to
    // update MANIFEST (so MANIFEST still points at N, while snap-N+1 is
    // an orphan on disk).
    //
    // The guard must keep both:
    //   - the `retain_count` newest snapshots
    //   - the live MANIFEST snapshot, even if not in the newest window
    //
    // Engineered here by manually rewriting MANIFEST to point at an older
    // snapshot and then calling `prune_snapshots_once` directly.

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    {
        let mut cfg = db.config.write();
        cfg.snapshot_retention.retain_count = 10;
    }

    let branch_id = BranchId::new();
    for i in 0..5 {
        write_one(&db, branch_id, &format!("k{}", i));
        db.checkpoint().unwrap();
    }

    let canonical = db_path.canonicalize().unwrap();
    let snapshots_dir = canonical.join("snapshots");
    let manifest_path = canonical.join("MANIFEST");

    // Snap ids are 1..=5; MANIFEST currently points at 5. Rewrite to 2.
    {
        let mut manifest =
            strata_storage::durability::ManifestManager::load(manifest_path.clone()).unwrap();
        let prev_watermark_txn = TxnId(manifest.manifest().snapshot_watermark.unwrap_or(0));
        manifest
            .set_snapshot_watermark(2, prev_watermark_txn)
            .unwrap();
    }

    // Now tighten retain_count to 2 and prune.
    {
        let mut cfg = db.config.write();
        cfg.snapshot_retention.retain_count = 2;
    }
    db.prune_snapshots_once().unwrap();

    let mut kept_ids: Vec<u64> = list_snapshots(&snapshots_dir)
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    kept_ids.sort_unstable();

    // Newest 2 (4, 5) plus live MANIFEST (2) = 3 files retained.
    // Ids 1 and 3 are pruned (in delete window, not live).
    assert_eq!(
        kept_ids,
        vec![2, 4, 5],
        "live MANIFEST snapshot (2) must be preserved alongside the retain_count=2 newest (4, 5)"
    );
}

#[test]
fn checkpoint_succeeds_when_snapshot_pruning_cannot_delete_old_entry() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    {
        let mut cfg = db.config.write();
        cfg.snapshot_retention.retain_count = 1;
    }

    let canonical = db_path.canonicalize().unwrap();
    let snapshots_dir = canonical.join("snapshots");
    std::fs::create_dir_all(&snapshots_dir).unwrap();

    // Force snapshot id 1 into the deletion window, but make it a directory.
    // The pruner uses remove_file(), so this delete fails portably. The
    // surrounding checkpoint must still return success after writing id 2.
    let undeletable_old_snapshot = strata_storage::durability::snapshot_path(&snapshots_dir, 1);
    std::fs::create_dir(&undeletable_old_snapshot).unwrap();
    assert!(
        list_snapshots(&snapshots_dir)
            .unwrap()
            .iter()
            .any(|(id, path)| *id == 1 && path == &undeletable_old_snapshot),
        "directory-shaped snap-000001.chk must be a prune candidate for this characterization"
    );

    let manifest_path = canonical.join("MANIFEST");
    {
        let mut manifest =
            strata_storage::durability::ManifestManager::load(manifest_path.clone()).unwrap();
        manifest.set_snapshot_watermark(1, TxnId(1)).unwrap();
    }

    let branch_id = BranchId::new();
    write_one(&db, branch_id, "prune-failure");

    db.checkpoint()
        .expect("checkpoint must remain non-fatal when post-checkpoint pruning fails");

    let manifest = strata_storage::durability::ManifestManager::load(manifest_path).unwrap();
    assert_eq!(
        manifest.manifest().snapshot_id,
        Some(2),
        "checkpoint should advance MANIFEST despite non-fatal prune delete failure"
    );
    assert!(
        undeletable_old_snapshot.is_dir(),
        "failed prune candidate should remain when remove_file cannot delete it"
    );
    assert!(
        strata_storage::durability::snapshot_path(&snapshots_dir, 2).exists(),
        "new checkpoint snapshot should be durable"
    );
}

#[test]
fn prune_snapshots_once_reports_manifest_load_errors() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    write_one(&db, branch_id, "manifest-load-error");
    db.checkpoint().unwrap();

    let manifest_path = db_path.canonicalize().unwrap().join("MANIFEST");
    std::fs::write(&manifest_path, b"not a manifest").unwrap();

    match db.prune_snapshots_once() {
        Err(StrataError::Internal { message }) => {
            assert!(
                message.contains("prune_snapshots: failed to load MANIFEST"),
                "unexpected prune error message: {message}"
            );
        }
        other => panic!("expected MANIFEST load failure from prune_snapshots_once, got {other:?}"),
    }
}

#[test]
fn prune_noop_when_under_retain_count() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    write_one(&db, branch_id, "k0");
    db.checkpoint().unwrap();
    write_one(&db, branch_id, "k1");
    db.checkpoint().unwrap();

    // Default retain_count is 10; only 2 snapshots → noop.
    let snapshots_dir = db_path.canonicalize().unwrap().join("snapshots");
    let snapshots = list_snapshots(&snapshots_dir).unwrap();
    assert_eq!(snapshots.len(), 2);

    // Direct invocation also reports zero deletions.
    assert_eq!(db.prune_snapshots_once().unwrap(), 0);
}

#[test]
fn prune_skipped_for_ephemeral_database() {
    let db = Database::cache().unwrap();
    // Ephemeral mode has no on-disk snapshots; pruner returns 0.
    assert_eq!(db.prune_snapshots_once().unwrap(), 0);
}
