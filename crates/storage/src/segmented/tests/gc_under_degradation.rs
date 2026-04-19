//! SE3 regressions for `gc_orphan_segments` refusal under degraded recovery.
//!
//! Pins the SG-009 contract: after a recovery that produced `DataLoss` or
//! `PolicyDowngrade` health, orphan-segment GC refuses to run. `Telemetry`
//! degradation does not block GC. The refusal is lifted by
//! `reset_recovery_health()`.

use super::*;
use crate::segmented::{
    DegradationClass, RecoveryFault, RecoveryHealth,
};
use crate::StorageError;

/// After a corrupt-manifest recovery, `gc_orphan_segments()` must refuse
/// and leave the would-be-orphaned `.sst` files on disk.
#[test]
fn gc_refuses_after_corrupt_manifest_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Flush a real segment so we get a valid manifest + SST on disk.
    seed(&store, kv_key("real"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let branch_hex = super::hex_encode_branch(&branch());
    let branch_dir = dir.path().join(&branch_hex);

    // Collect the .sst files that would be candidates for orphan GC if the
    // branch were dropped by recovery.
    let ssts_before: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sst"))
        .collect();
    assert!(!ssts_before.is_empty(), "setup must produce at least one sst");

    // Corrupt the manifest — recovery must classify this as DataLoss.
    let manifest_path = branch_dir.join("segments.manifest");
    let mut data = std::fs::read(&manifest_path).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&manifest_path, &data).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();
    match &outcome.health {
        RecoveryHealth::Degraded { class, faults } => {
            assert_eq!(*class, DegradationClass::DataLoss);
            assert!(
                faults
                    .iter()
                    .any(|f| matches!(f, RecoveryFault::CorruptManifest { .. })),
                "expected CorruptManifest fault in {faults:?}"
            );
        }
        other => panic!("expected Degraded after corrupt manifest; got {other:?}"),
    }

    let err = store2
        .gc_orphan_segments()
        .expect_err("gc must refuse under DataLoss recovery");
    match err {
        StorageError::GcRefusedDegradedRecovery { class } => {
            assert_eq!(class, DegradationClass::DataLoss);
        }
        other => panic!("expected GcRefusedDegradedRecovery; got {other:?}"),
    }

    // Orphan .sst files from the skipped branch must remain on disk: the
    // degraded recovery did not track them in self.branches, so without the
    // refusal they would have been mistaken for orphans and reaped.
    for sst in &ssts_before {
        assert!(
            sst.exists(),
            "sst {sst:?} would have been unsafely reaped without SE3 refusal"
        );
    }
}

/// After `reset_recovery_health()`, GC unblocks — the admin path lifts the
/// refusal that SE3 installs.
#[test]
fn gc_runs_after_reset_recovery_health() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("real"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let branch_hex = super::hex_encode_branch(&branch());
    let manifest_path = dir.path().join(&branch_hex).join("segments.manifest");
    let mut data = std::fs::read(&manifest_path).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&manifest_path, &data).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let _ = store2.recover_segments().unwrap();
    assert!(store2.gc_orphan_segments().is_err(), "GC must refuse first");

    store2.reset_recovery_health();
    assert!(matches!(
        &*store2.last_recovery_health(),
        RecoveryHealth::Healthy
    ));
    store2
        .gc_orphan_segments()
        .expect("GC must succeed after reset_recovery_health");
}

/// A clean recovery leaves the store in `Healthy`; GC runs and returns a
/// `GcReport` with `files_deleted` == 0 when there are no orphans.
#[test]
fn gc_runs_under_clean_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("real"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();
    assert!(matches!(outcome.health, RecoveryHealth::Healthy));
    assert!(matches!(
        &*store2.last_recovery_health(),
        RecoveryHealth::Healthy
    ));

    let report = store2
        .gc_orphan_segments()
        .expect("clean recovery; gc must succeed");
    assert_eq!(report.files_deleted, 0);
}

/// `PolicyDowngrade` class (e.g. no-manifest fallback engaged during
/// recovery) must block GC for the same reason `DataLoss` does: the
/// in-memory branch set cannot be treated as authoritative, so "not in
/// `live_ids`" is not a safe deletion proof.
#[test]
fn gc_refuses_under_policy_downgrade_degradation() {
    let store = SegmentedStore::new();
    store.set_recovery_health_for_test(RecoveryHealth::Degraded {
        faults: std::sync::Arc::from(vec![RecoveryFault::NoManifestFallbackUsed {
            branch_id: branch(),
            segments_promoted: 3,
        }]),
        class: DegradationClass::PolicyDowngrade,
    });

    let err = store
        .gc_orphan_segments()
        .expect_err("gc must refuse under PolicyDowngrade");
    match err {
        StorageError::GcRefusedDegradedRecovery { class } => {
            assert_eq!(class, DegradationClass::PolicyDowngrade);
        }
        other => panic!("expected GcRefusedDegradedRecovery; got {other:?}"),
    }
}

/// `Telemetry`-class degradation does not compromise deletion safety —
/// rebuildable-cache errors must not block GC.
#[test]
fn gc_runs_under_telemetry_only_degradation() {
    let store = SegmentedStore::new();
    store.set_recovery_health_for_test(RecoveryHealth::Degraded {
        faults: std::sync::Arc::from(vec![RecoveryFault::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "synthetic rebuildable-cache glitch",
        ))]),
        class: DegradationClass::Telemetry,
    });

    store
        .gc_orphan_segments()
        .expect("Telemetry-class degradation must not block GC");
}

/// `clear_branch` must succeed even when GC refuses: the primary task
/// (removing the target branch from `self.branches` and its on-disk
/// segments) is independent of orphan GC, which only accumulates
/// retention debt on refusal.
#[test]
fn clear_branch_succeeds_under_degraded_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("target"), Value::Int(7), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Simulate a degraded recovery state without actually corrupting
    // anything — exercises the log-and-continue path inside clear_branch.
    store.set_recovery_health_for_test(RecoveryHealth::Degraded {
        faults: std::sync::Arc::from(vec![RecoveryFault::InheritedLayerLost {
            child: branch(),
            source_branch: branch(),
            fork_version: CommitVersion(0),
        }]),
        class: DegradationClass::DataLoss,
    });

    assert!(
        store.clear_branch(&branch()),
        "clear_branch must succeed even when gc refuses under degraded recovery"
    );
    assert!(
        store.branches.get(&branch()).is_none(),
        "branch must be removed from the in-memory map regardless of gc refusal"
    );
}
