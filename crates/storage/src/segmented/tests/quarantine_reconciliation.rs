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
    // quarantine protocol and then calls gc_orphan_segments to drain
    // them via Stage 5 in the same call.
    store.clear_branch(&branch());

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
