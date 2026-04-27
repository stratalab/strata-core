//! B5.2 storage-local reopen + reconciliation tests.
//!
//! Complements the integration-test suites (`branching_retention_matrix`
//! and `branching_gc_quarantine_recovery`) with storage-layer scenarios
//! that do not need the engine. Focus: rebuild from manifests,
//! accelerator disagreement blocks reclaim (KD10), degraded recovery
//! does not self-upgrade without evidence (A5).

use super::*;
use crate::quarantine::{
    read_quarantine_manifest, write_quarantine_manifest, QuarantineEntry, QUARANTINE_DIR,
    QUARANTINE_FILENAME,
};
use crate::segmented::{DegradationClass, RecoveryFault, RecoveryHealth};
use crate::StorageError;

fn branch_dir_for(dir: &std::path::Path, branch_id: BranchId) -> std::path::PathBuf {
    dir.join(super::super::hex_encode_branch(&branch_id))
}

/// Stage-4 publish durably records quarantine membership. A reopen that
/// loads an inventory matching the on-disk quarantine contents must keep
/// recovery healthy and allow GC to proceed through Stage 5.
#[test]
fn reopen_with_consistent_quarantine_inventory_stays_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    drop(store);

    // After a successful quarantine protocol, the branch dir has:
    // - segments.manifest that does NOT list the quarantined file
    // - __quarantine__/<file> present
    // - quarantine.manifest listing <file>.
    // Simulate that by removing the segments.manifest and staging the
    // file + inventory.
    let b_dir = branch_dir_for(dir.path(), branch());
    let ssts: Vec<_> = std::fs::read_dir(&b_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .map(|e| e.path())
        .collect();
    let victim = ssts
        .first()
        .cloned()
        .expect("setup produces at least one sst");
    let filename = victim.file_name().unwrap().to_string_lossy().to_string();

    std::fs::remove_file(b_dir.join("segments.manifest")).unwrap();
    let qdir = b_dir.join(QUARANTINE_DIR);
    std::fs::create_dir_all(&qdir).unwrap();
    std::fs::rename(&victim, qdir.join(&filename)).unwrap();
    write_quarantine_manifest(
        &b_dir,
        &[QuarantineEntry {
            segment_id: 42,
            filename: filename.clone(),
        }],
    )
    .unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();
    // Recovery health is Healthy — inventory matched on-disk quarantine.
    assert!(
        matches!(outcome.health, RecoveryHealth::Healthy),
        "consistent inventory + on-disk quarantine must reconcile cleanly; got {:?}",
        outcome.health,
    );

    // GC proceeds through Stage 5 — the quarantined file is purged.
    let report = store2
        .gc_orphan_segments()
        .expect("healthy; gc must succeed");
    assert_eq!(report.files_deleted, 1);
    assert!(!qdir.join(&filename).exists());
    assert!(!b_dir.join(QUARANTINE_FILENAME).exists());
}

/// An orphan file in `__quarantine__/` without any inventory must
/// degrade recovery health to `PolicyDowngrade` via
/// `QuarantineInventoryMismatch`, and GC must refuse until the operator
/// reconciles.
#[test]
fn reopen_degrades_on_quarantine_dir_without_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    drop(store);

    let b_dir = branch_dir_for(dir.path(), branch());
    let qdir = b_dir.join(QUARANTINE_DIR);
    std::fs::create_dir_all(&qdir).unwrap();
    std::fs::write(qdir.join("8888.sst"), b"orphan quarantine bytes").unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();
    match &outcome.health {
        RecoveryHealth::Degraded { class, faults } => {
            assert_eq!(*class, DegradationClass::PolicyDowngrade);
            let has_mismatch = faults
                .iter()
                .any(|f| matches!(f, RecoveryFault::QuarantineInventoryMismatch { .. }));
            assert!(has_mismatch, "expected QuarantineInventoryMismatch fault");
        }
        other => panic!("expected PolicyDowngrade; got {other:?}"),
    }

    // GC refuses — degraded recovery blocks reclaim.
    let err = store2
        .gc_orphan_segments()
        .expect_err("gc must refuse under degraded quarantine reconciliation");
    match err {
        StorageError::GcRefusedDegradedRecovery { class } => {
            assert_eq!(class, DegradationClass::PolicyDowngrade);
        }
        other => panic!("expected GcRefusedDegradedRecovery; got {other:?}"),
    }
    // Orphan file retained.
    assert!(qdir.join("8888.sst").exists());
}

/// Inventory listing a file that is not in `__quarantine__/` and not at
/// the original path is benign — rename never completed, or purge
/// already removed the file. Recovery must silently drop the stale
/// entry and stay healthy.
#[test]
fn reopen_drops_stale_inventory_entries_without_degrade() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    drop(store);

    let b_dir = branch_dir_for(dir.path(), branch());
    write_quarantine_manifest(
        &b_dir,
        &[QuarantineEntry {
            segment_id: 0xdead,
            filename: "phantom.sst".to_string(),
        }],
    )
    .unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();
    assert!(
        matches!(outcome.health, RecoveryHealth::Healthy),
        "stale entries for absent files must reconcile silently",
    );

    let m = read_quarantine_manifest(&b_dir)
        .unwrap()
        .unwrap_or_default();
    assert!(
        m.entries.is_empty(),
        "reconciled inventory must be empty; got {:?}",
        m.entries,
    );

    store2
        .gc_orphan_segments()
        .expect("healthy gc after silent reconcile");
}

/// The in-session retention/report path must treat the same stale inventory
/// shape as benign. Recovery reconciliation rewrites these entries away on
/// reopen, but callers must not get `RetentionReportUnavailable` simply
/// because publish happened and rename never did.
#[test]
fn retention_snapshot_ignores_stale_missing_quarantine_inventory_entries() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let b_dir = branch_dir_for(dir.path(), branch());
    write_quarantine_manifest(
        &b_dir,
        &[QuarantineEntry {
            segment_id: 0xdead,
            filename: "phantom.sst".to_string(),
        }],
    )
    .unwrap();

    let snapshot = store
        .retention_snapshot()
        .expect("stale inventory entries without quarantine files must not fail the read path");
    let entry = snapshot
        .iter()
        .find(|entry| entry.branch_id == branch())
        .expect("live branch must still appear in the retention snapshot");

    assert_eq!(
        entry.quarantined_bytes, 0,
        "stale inventory entries must not inflate quarantined byte attribution",
    );
    assert_eq!(
        entry.quarantined_segment_count, 0,
        "stale inventory entries must not inflate quarantined segment counts",
    );
}

/// After reclaim has driven the quarantine inventory empty, subsequent
/// reopens must leave a clean slate: no leftover `quarantine.manifest`,
/// no `__quarantine__/` directory, healthy recovery. Pins idempotency.
#[test]
fn reopen_after_full_reclaim_leaves_no_quarantine_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // clear_branch drives the branch's own segments through the
    // quarantine protocol; the explicit GC call below drains Stage 5.
    store.clear_branch(&branch()).unwrap();
    store
        .gc_orphan_segments()
        .expect("healthy recovery; explicit GC should drain quarantine");

    let b_dir = branch_dir_for(dir.path(), branch());
    assert!(!b_dir.join(QUARANTINE_FILENAME).exists());
    assert!(!b_dir.join(QUARANTINE_DIR).exists());

    // Reopen: still healthy, still no artifacts.
    drop(store);
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();
    assert!(matches!(outcome.health, RecoveryHealth::Healthy));
    assert!(!b_dir.join(QUARANTINE_FILENAME).exists());
    assert!(!b_dir.join(QUARANTINE_DIR).exists());
}

/// Retrying `clear_branch()` on a branch dir that only has quarantine debt
/// must not recreate `segments.manifest`; the retry should remain a pure
/// cleanup pass for on-disk debt after the branch is already absent.
#[test]
fn clear_branch_retry_on_quarantine_only_dir_does_not_recreate_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let b_dir = branch_dir_for(dir.path(), branch());
    store.clear_branch(&branch()).unwrap();

    assert!(
        b_dir.join(QUARANTINE_FILENAME).exists(),
        "first clear should leave quarantine inventory behind"
    );
    assert!(
        !b_dir.join("segments.manifest").exists(),
        "quarantine-only branch dir should not retain an empty manifest"
    );

    assert!(
        store.clear_branch(&branch()).unwrap(),
        "retry should keep cleaning an on-disk debt dir even after the branch is absent"
    );
    assert!(
        b_dir.join(QUARANTINE_FILENAME).exists(),
        "retry should not discard quarantine debt before GC drains it"
    );
    assert!(
        !b_dir.join("segments.manifest").exists(),
        "retry must not recreate segments.manifest for a quarantine-only dir"
    );
}

/// After Stage-5 drains the quarantine inventory, a retry of `clear_branch()`
/// should converge the leftover empty branch dir instead of returning false
/// and recreating empty-manifest state.
#[test]
fn clear_branch_retry_removes_empty_leftover_dir_after_gc() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let b_dir = branch_dir_for(dir.path(), branch());
    store.clear_branch(&branch()).unwrap();
    store
        .gc_orphan_segments()
        .expect("healthy recovery; explicit GC should drain quarantine");

    assert!(
        b_dir.exists(),
        "GC drains quarantine state but may still leave an empty branch dir behind"
    );
    assert!(
        std::fs::read_dir(&b_dir).unwrap().next().is_none(),
        "setup expects only empty-dir cleanup debt to remain"
    );

    assert!(
        store.clear_branch(&branch()).unwrap(),
        "retry should converge the empty leftover dir"
    );
    assert!(
        !b_dir.exists(),
        "retry should remove the empty leftover dir once cleanup debt is gone"
    );
    assert!(
        !store.clear_branch(&branch()).unwrap(),
        "once both branch state and leftover dir are gone, clear_branch should report false"
    );
}

/// Reopen must reserve segment ids from quarantine debt so a same-name
/// recreate cannot reuse an `.sst` filename that is still listed in the
/// quarantine inventory.
#[test]
fn recreate_after_reopen_does_not_reuse_quarantined_segment_filename() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("before"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let b_dir = branch_dir_for(dir.path(), branch());
    store.clear_branch(&branch()).unwrap();
    drop(store);

    let reopened = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let first_recovery = reopened.recover_segments().unwrap();
    assert!(
        matches!(first_recovery.health, RecoveryHealth::Healthy),
        "delete with consistent quarantine inventory must reopen cleanly"
    );

    seed(&reopened, kv_key("after"), Value::Int(2), 2);
    reopened.rotate_memtable(&branch());
    reopened.flush_oldest_frozen(&branch()).unwrap();

    let top_level_ssts: HashSet<String> = std::fs::read_dir(&b_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .filter_map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(str::to_string)
        })
        .collect();
    let inventory = read_quarantine_manifest(&b_dir)
        .unwrap()
        .expect("quarantine inventory remains until explicit GC");
    for entry in &inventory.entries {
        assert!(
            !top_level_ssts.contains(&entry.filename),
            "recreated branch must not reuse quarantined filename {}",
            entry.filename
        );
    }
    drop(reopened);

    let reopened_again = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let second_recovery = reopened_again.recover_segments().unwrap();
    assert!(
        matches!(second_recovery.health, RecoveryHealth::Healthy),
        "same-name recreate with retained quarantine debt must reopen cleanly"
    );
    assert_eq!(
        reopened_again
            .get_versioned(&kv_key("after"), CommitVersion::MAX)
            .unwrap()
            .map(|entry| entry.value),
        Some(Value::Int(2)),
    );
}

/// A parent that forks, materializes the child, and then continues rewriting
/// must let GC skip manifest-protected or disappearing candidates rather than
/// failing the whole reclaim pass.
#[test]
fn gc_after_materialized_child_and_parent_rewrites_stays_best_effort() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("root"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    seed(&store, parent_kv("root"), Value::Int(2), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    store.materialize_layer(&child_branch(), 0).unwrap();

    seed(&store, parent_kv("root"), Value::Int(3), 3);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    seed(&store, parent_kv("root"), Value::Int(4), 4);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    store
        .gc_orphan_segments()
        .expect("healthy histories should let GC skip unreclaimable candidates and continue");
}

/// Top-level `.sst` files that are no longer listed in the branch's own
/// manifest and are not held by any descendant inherited layer are cleanup
/// debt, not manifest-reachable retention. The storage snapshot must not count
/// them into `exclusive_bytes` / `shared_bytes`.
#[test]
fn retention_snapshot_ignores_live_branch_top_level_orphan_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let before = store
        .retention_snapshot()
        .expect("baseline snapshot succeeds");
    let before_entry = before
        .iter()
        .find(|entry| entry.branch_id == branch())
        .expect("live branch must appear in retention snapshot");

    let b_dir = branch_dir_for(dir.path(), branch());
    std::fs::write(b_dir.join("99999.sst"), vec![0u8; 4096]).unwrap();

    let after = store
        .retention_snapshot()
        .expect("snapshot with live-branch orphan succeeds");
    let after_entry = after
        .iter()
        .find(|entry| entry.branch_id == branch())
        .expect("live branch must still appear in retention snapshot");

    assert_eq!(
        after_entry.exclusive_bytes, before_entry.exclusive_bytes,
        "non-manifest top-level orphan bytes must not inflate exclusive retention",
    );
    assert_eq!(
        after_entry.shared_bytes, before_entry.shared_bytes,
        "non-manifest top-level orphan bytes must not inflate shared retention",
    );
}

/// Accelerator disagreement — the runtime ref registry says a file is
/// referenced but the file is not listed in any recovery-trusted
/// manifest. Under the current protocol, the Stage-2 proof refuses to
/// quarantine because `is_referenced` returns true. That is the correct
/// behavior: manifests win, space leaks, no false reclaim (Invariants 1
/// + 2, KD10).
#[test]
fn accelerator_disagreement_blocks_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Capture the on-disk segment path + id, then drop the branch from
    // self.branches so the normal live-set check would classify it as
    // an orphan.
    let ver = store.branches.get(&branch()).unwrap().version.load_full();
    let (victim_id, victim_path) = {
        let seg = ver.levels[0]
            .first()
            .cloned()
            .expect("setup produced at least one L0 segment");
        (seg.file_id(), seg.file_path().to_path_buf())
    };
    drop(ver);
    // Inflate its runtime refcount to simulate a disagreement where
    // the manifest has moved on but the registry still holds a
    // reference (e.g. a hypothetical descendant inherited layer that
    // was not released). Refcount non-zero ⇒ Stage 2 proof refuses.
    store.ref_registry.increment(victim_id);
    // Ensure the branch state is gone so live_ids doesn't protect the
    // file on a different code path.
    store.branches.remove(&branch());

    let report = store.gc_orphan_segments().expect("healthy; gc returns Ok");
    assert_eq!(report.files_deleted, 0, "no reclaim while refcount > 0");
    assert!(
        victim_path.exists(),
        "victim file must remain on disk when accelerator disagrees with reclaim plan",
    );
}

/// The runtime accelerator is only candidate selection. If it says
/// "zero refs" but a recovery-trusted inherited-layer manifest still
/// lists the segment, Stage 2 manifest proof must win and refuse
/// quarantine.
#[test]
fn manifest_proof_beats_false_negative_runtime_refcount() {
    let (_dir, store) = setup_parent_with_segments(&[("k", 1, 1)]);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    let (victim_id, victim_path) = {
        let parent = store.branches.get(&parent_branch()).unwrap();
        let ver = parent.version.load();
        let seg = ver.levels[0]
            .first()
            .cloned()
            .expect("setup produced at least one parent segment");
        (seg.file_id(), seg.file_path().to_path_buf())
    };

    // Child inherited-layer attach increments the runtime refcount. Force a
    // false-negative accelerator state; manifest proof must still refuse.
    store.ref_registry.decrement(victim_id);

    let snapshot = store
        .retention_snapshot()
        .expect("storage-local retention snapshot must be derivable from the same manifests");
    let parent_entry = snapshot
        .iter()
        .find(|entry| entry.branch_id == parent_branch())
        .expect("parent branch must appear in retention snapshot");
    let child_entry = snapshot
        .iter()
        .find(|entry| entry.branch_id == child_branch())
        .expect("child branch must appear in retention snapshot");
    assert!(
        parent_entry.shared_bytes > 0,
        "shared-byte classification must come from inherited-layer manifests, not the runtime accelerator",
    );
    assert_eq!(
        parent_entry.exclusive_bytes, 0,
        "all parent bytes are still held by the child inherited layer in this setup",
    );
    assert!(
        parent_entry.exclusive_bytes + parent_entry.shared_bytes > 0,
        "the parent's retained bytes must still appear in the snapshot even when the accelerator lies",
    );
    assert!(
        child_entry.inherited_layer_bytes > 0,
        "the child's inherited-layer bytes must be derived from the same manifest ledger",
    );

    match store.quarantine_segment_if_unreferenced(&victim_path, victim_id) {
        Err(StorageError::ReclaimRefusedManifestProof { segment_id }) => {
            assert_eq!(segment_id, victim_id);
        }
        other => panic!("expected ReclaimRefusedManifestProof; got {other:?}"),
    }
    assert!(
        victim_path.exists(),
        "manifest-protected segment must remain on disk when Stage 2 refuses reclaim",
    );
}

/// A persisted `status = Materialized` inherited layer does not retain source
/// bytes. Retention reporting and shared/exclusive classification must follow
/// the same rule as normal read paths, which skip materialized layers.
#[test]
fn materialized_inherited_layer_does_not_retain_source_bytes() {
    let (dir, store) = setup_parent_with_segments(&[("k", 1, 1)]);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    store.write_branch_manifest(&parent_branch()).unwrap();
    store.write_branch_manifest(&child_branch()).unwrap();

    let child_dir = branch_dir_for(dir.path(), child_branch());
    let mut child_manifest = crate::manifest::read_manifest(&child_dir)
        .unwrap()
        .expect("child manifest must exist");
    assert_eq!(child_manifest.inherited_layers.len(), 1);
    child_manifest.inherited_layers[0].status = 2; // Materialized
    crate::manifest::write_manifest(
        &child_dir,
        &child_manifest.entries,
        &child_manifest.inherited_layers,
    )
    .unwrap();

    let snapshot = store
        .retention_snapshot()
        .expect("retention snapshot should follow persisted manifest state");
    let parent_entry = snapshot
        .iter()
        .find(|entry| entry.branch_id == parent_branch())
        .expect("parent branch must still report own bytes");

    assert!(
        parent_entry.exclusive_bytes > 0,
        "parent bytes should be exclusive once the child manifest marks the layer materialized",
    );
    assert_eq!(
        parent_entry.shared_bytes, 0,
        "materialized layers must not keep source bytes in shared retention",
    );
    assert!(
        snapshot
            .iter()
            .all(|entry| entry.branch_id != child_branch()),
        "child should not report inherited retention from a materialized layer",
    );
}

/// If deleted-parent orphan storage still exists after clear and the empty
/// manifest publish fails, `clear_branch()` must surface the failure instead
/// of silently reporting success. Otherwise reopen can fall back to the
/// legacy no-manifest path and resurrect the deleted parent.
#[test]
fn clear_branch_refuses_silent_success_when_empty_manifest_publish_fails() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let (_dir, store) = setup_parent_with_segments(&[("k", 1, 1)]);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    let err = store
        .clear_branch(&parent_branch())
        .expect_err("delete must not report success when the empty manifest publish fails");
    assert!(
        matches!(err, StorageError::DirFsync { .. }),
        "expected empty-manifest durable-publish failure, got {err:?}",
    );

    let parent_dir = branch_dir_for(store.segments_dir().unwrap(), parent_branch());
    let has_top_level_sst = std::fs::read_dir(&parent_dir)
        .unwrap()
        .flatten()
        .any(|entry| {
            entry.path().is_file()
                && entry.path().extension().and_then(|x| x.to_str()) == Some("sst")
        });
    assert!(
        has_top_level_sst,
        "setup must leave descendant-pinned top-level parent segments in place",
    );
    assert!(
        parent_dir.join("segments.manifest").exists(),
        "clear_branch must not delete the old manifest before the empty-manifest barrier is durably published",
    );

    crate::test_hooks::clear_manifest_dir_fsync_failure();
}

/// Concurrent quarantine publish vs. purge must be serialized by the
/// `deletion_write_guard`. Without the guard, a purge that reads an
/// empty inventory could unlink (or stat-over) a file a concurrent
/// quarantine publish just renamed in. Pins the fix for the race the
/// B5.2 code review flagged: the purge loop reads + unlinks + rewrites
/// the inventory under the same guard that quarantine holds across
/// its publish + rename.
#[test]
fn concurrent_quarantine_and_purge_do_not_corrupt_inventory() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    for _ in 0..32 {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

        // Seed two flushed segments so we have two independent reclaim
        // candidates.
        seed(&store, kv_key("a"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();
        seed(&store, kv_key("b"), Value::Int(2), 2);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        // Drop the branch from self.branches so both segments look like
        // orphan candidates to the next GC walk. Direct map access to
        // avoid routing through clear_branch (which already serializes
        // quarantine itself).
        let segments: Vec<(std::path::PathBuf, u64)> = {
            let branch_state = store.branches.get(&branch()).unwrap();
            let ver = branch_state.version.load_full();
            let mut out = Vec::new();
            for level in &ver.levels {
                for seg in level {
                    out.push((seg.file_path().to_path_buf(), seg.file_id()));
                }
            }
            out
        };
        store.branches.remove(&branch());

        // Two concurrent workers: thread A tries to quarantine both
        // segments in sequence; thread B races `gc_orphan_segments`
        // which includes an internal purge pass that used to race with
        // quarantine publish.
        let barrier = Arc::new(Barrier::new(2));
        let s_a = Arc::clone(&store);
        let b_a = Arc::clone(&barrier);
        let segs_a = segments.clone();
        let t_a = thread::spawn(move || {
            b_a.wait();
            for (path, id) in &segs_a {
                let _ = s_a.quarantine_segment_if_unreferenced(path, *id);
            }
        });
        let s_b = Arc::clone(&store);
        let b_b = Arc::clone(&barrier);
        let t_b = thread::spawn(move || {
            b_b.wait();
            let _ = s_b.gc_orphan_segments();
        });
        t_a.join().unwrap();
        t_b.join().unwrap();

        // One more gc pass to drain any residual quarantine state.
        let _ = store.gc_orphan_segments();

        // Invariant: filesystem + inventory are consistent. Either the
        // files have been purged, or they remain in the quarantine
        // directory with a matching inventory entry. Never: file in
        // quarantine dir without inventory, or inventory entry without
        // file.
        let b_dir = branch_dir_for(dir.path(), branch());
        let qdir = b_dir.join(QUARANTINE_DIR);
        let on_disk: std::collections::HashSet<String> = std::fs::read_dir(&qdir)
            .map(|it| {
                it.flatten()
                    .filter_map(|e| e.file_name().to_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let inventory: std::collections::HashSet<String> = read_quarantine_manifest(&b_dir)
            .unwrap()
            .map(|m| m.entries.into_iter().map(|e| e.filename).collect())
            .unwrap_or_default();
        assert_eq!(
            on_disk, inventory,
            "quarantine directory must match inventory exactly (no orphan files, no dangling entries)"
        );
    }
}

/// Hard pre-walk recovery failures (e.g. missing segments dir) never
/// install a reclaim-safe runtime state, so reclaim stays refused on
/// that store instance. Pins A5 + §"Hard pre-walk recovery failures".
#[test]
fn hard_prewalk_failure_keeps_reclaim_refused() {
    let dir = tempfile::tempdir().unwrap();
    let bogus = dir.path().join("not-a-dir");
    std::fs::write(&bogus, b"not a directory").unwrap();

    let store = SegmentedStore::with_dir(bogus, 0);
    store.recover_segments().expect_err("hard pre-walk fails");

    match store.gc_orphan_segments() {
        Err(StorageError::GcRefusedDegradedRecovery { .. }) => {}
        Err(other) => panic!("expected GcRefusedDegradedRecovery; got {other:?}"),
        Ok(_) => panic!("reclaim must not run after hard pre-walk failure"),
    }
}
