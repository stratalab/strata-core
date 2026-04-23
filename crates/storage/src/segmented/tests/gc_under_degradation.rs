//! SE3 regressions for `gc_orphan_segments` refusal under degraded recovery.
//!
//! Pins the SG-009 contract: after a recovery that produced `DataLoss` or
//! `PolicyDowngrade` health, orphan-segment GC refuses to run. `Telemetry`
//! degradation does not block GC. `reset_recovery_health()` only clears
//! `PolicyDowngrade` after a successful recovery install; `DataLoss`
//! requires a fresh reopen and hard pre-walk recovery failures cannot be
//! cleared on the same instance.

use super::*;
use crate::segmented::{DegradationClass, RecoveryFault, RecoveryHealth};
use crate::StorageError;

fn kv_key_for(branch_id: BranchId, name: &str) -> Key {
    Key::new(
        Arc::new(Namespace::new(branch_id, "default".to_string())),
        TypeTag::KV,
        name.as_bytes().to_vec(),
    )
}

fn seed_branch(store: &SegmentedStore, branch_id: BranchId, key: &str, value: i64, version: u64) {
    store
        .put_with_version_mode(
            kv_key_for(branch_id, key),
            Value::Int(value),
            CommitVersion(version),
            None,
            WriteMode::Append,
        )
        .unwrap();
}

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
    assert!(
        !ssts_before.is_empty(),
        "setup must produce at least one sst"
    );

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

/// `DataLoss` recovery cannot be reset in-place: the store's in-memory
/// recovery graph is a one-shot snapshot, so a fresh reopen is required
/// before GC can trust restored files.
#[test]
fn reset_recovery_health_refuses_after_data_loss_recovery() {
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

    let err = store2
        .reset_recovery_health()
        .expect_err("DataLoss reset must require a fresh reopen");
    match err {
        StorageError::RecoveryHealthResetRequiresReopen { class } => {
            assert_eq!(class, DegradationClass::DataLoss);
        }
        other => panic!("expected RecoveryHealthResetRequiresReopen; got {other:?}"),
    }
    assert!(matches!(
        &*store2.last_recovery_health(),
        RecoveryHealth::Degraded {
            class: DegradationClass::DataLoss,
            ..
        }
    ));
    assert!(
        store2.gc_orphan_segments().is_err(),
        "failed reset must leave GC refusal in place"
    );
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

/// After a successful `PolicyDowngrade` recovery install, the operator can
/// explicitly clear the GC refusal in-place.
#[test]
fn gc_runs_after_reset_recovery_health_under_policy_downgrade() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("real"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let branch_hex = super::hex_encode_branch(&branch());
    std::fs::remove_file(dir.path().join(&branch_hex).join("segments.manifest")).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();
    assert!(matches!(
        outcome.health,
        RecoveryHealth::Degraded {
            class: DegradationClass::PolicyDowngrade,
            ..
        }
    ));
    assert!(store2.gc_orphan_segments().is_err(), "GC must refuse first");

    store2
        .reset_recovery_health()
        .expect("PolicyDowngrade reset should succeed after successful recovery");
    assert!(matches!(
        &*store2.last_recovery_health(),
        RecoveryHealth::Healthy
    ));

    let report = store2
        .gc_orphan_segments()
        .expect("GC must succeed after reset_recovery_health");
    assert_eq!(report.files_deleted, 0);
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

/// Hard pre-walk recovery failures publish degraded health but never install
/// an authoritative branch/refcount snapshot, so the reset must refuse on
/// the same store instance.
#[test]
fn reset_recovery_health_refuses_after_prewalk_hard_error() {
    let dir = tempfile::tempdir().unwrap();
    let not_a_dir = dir.path().join("segments-file");
    std::fs::write(&not_a_dir, b"not a directory").unwrap();

    let store = SegmentedStore::with_dir(not_a_dir, 0);
    store
        .recover_segments()
        .expect_err("top-level read_dir failure should still return Err");

    let err = store
        .reset_recovery_health()
        .expect_err("hard pre-walk failure never installed recovery state");
    assert!(matches!(
        err,
        StorageError::RecoveryHealthResetRequiresSuccessfulRecovery
    ));
    assert!(matches!(
        &*store.last_recovery_health(),
        RecoveryHealth::Degraded {
            class: DegradationClass::DataLoss,
            ..
        }
    ));
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
    // anything — exercises the reclaim-refusal log-and-continue path inside
    // clear_branch.
    store.set_recovery_health_for_test(RecoveryHealth::Degraded {
        faults: std::sync::Arc::from(vec![RecoveryFault::InheritedLayerLost {
            child: branch(),
            source_branch: branch(),
            fork_version: CommitVersion(0),
        }]),
        class: DegradationClass::DataLoss,
    });

    assert!(
        store.clear_branch(&branch()).unwrap(),
        "clear_branch must succeed even when gc refuses under degraded recovery"
    );
    assert!(
        store.branches.get(&branch()).is_none(),
        "branch must be removed from the in-memory map regardless of gc refusal"
    );
}

/// After a degraded recovery skips one branch, `clear_branch()` on another
/// branch must not let orphan GC delete the skipped branch's authoritative
/// files. The degraded health snapshot must remain visible after the clear.
#[test]
fn clear_branch_under_degraded_recovery_preserves_skipped_branch_files() {
    let dir = tempfile::tempdir().unwrap();
    let skipped = BranchId::from_bytes([31; 16]);
    let cleared = BranchId::from_bytes([32; 16]);

    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed_branch(&store, skipped, "lost", 11, 11);
    store.rotate_memtable(&skipped);
    store.flush_oldest_frozen(&skipped).unwrap();

    seed_branch(&store, cleared, "live", 22, 22);
    store.rotate_memtable(&cleared);
    store.flush_oldest_frozen(&cleared).unwrap();
    drop(store);

    let skipped_dir = dir.path().join(super::hex_encode_branch(&skipped));
    let skipped_ssts: Vec<_> = std::fs::read_dir(&skipped_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .collect();
    assert!(
        !skipped_ssts.is_empty(),
        "setup must produce authoritative skipped-branch SSTs"
    );

    let skipped_manifest = skipped_dir.join("segments.manifest");
    let mut manifest_bytes = std::fs::read(&skipped_manifest).unwrap();
    let mid = manifest_bytes.len() / 2;
    manifest_bytes[mid] ^= 0xFF;
    std::fs::write(&skipped_manifest, &manifest_bytes).unwrap();

    let reopened = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = reopened.recover_segments().unwrap();
    assert!(matches!(
        outcome.health,
        RecoveryHealth::Degraded {
            class: DegradationClass::DataLoss,
            ..
        }
    ));
    assert!(reopened.branches.get(&skipped).is_none());
    assert!(reopened.branches.get(&cleared).is_some());

    assert!(reopened.clear_branch(&cleared).unwrap());
    assert!(reopened.branches.get(&cleared).is_none());
    assert!(matches!(
        &*reopened.last_recovery_health(),
        RecoveryHealth::Degraded {
            class: DegradationClass::DataLoss,
            ..
        }
    ));

    for sst in &skipped_ssts {
        assert!(
            sst.exists(),
            "skipped branch SST {sst:?} must survive clear_branch while GC refuses"
        );
    }
}
