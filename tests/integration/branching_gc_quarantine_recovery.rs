//! Crash / failpoint quarantine reopen suite.
//!
//! Exercises the crash boundaries reachable through the current test hooks.
//! Several distinct failures reduce to the same filesystem observable
//! (for example, "file in quarantine but inventory absent"), so the suite
//! focuses on required post-crash outcomes rather than one hook per branch of
//! the implementation.
//!
//! Verification target: no recovery-trusted manifest-reachable segment may be
//! permanently deleted across these crash boundaries, and quarantine remains
//! reversible until final purge.

#![cfg(not(miri))]

use crate::common::*;
use std::sync::Arc;
use strata_core::value::Value;
use strata_core::BranchId;

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

fn seed(db: &Arc<Database>, name: &str, key: &str, v: i64) {
    KVStore::new(db.clone())
        .put(&resolve(name), "default", key, Value::Int(v))
        .expect("seed write succeeds");
}

fn flush_branch(db: &Arc<Database>, name: &str) {
    let id = resolve(name);
    db.storage().rotate_memtable(&id);
    db.storage()
        .flush_oldest_frozen(&id)
        .expect("flush succeeds");
}

fn hex(id: &BranchId) -> String {
    let mut s = String::with_capacity(32);
    for b in id.as_bytes() {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

fn segments_dir(db: &Arc<Database>) -> std::path::PathBuf {
    db.storage()
        .segments_dir()
        .cloned()
        .expect("disk-backed test database")
}

fn branch_dir(db: &Arc<Database>, name: &str) -> std::path::PathBuf {
    segments_dir(db).join(hex(&resolve(name)))
}

fn list_ssts(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .map(|it| {
            it.flatten()
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default()
}

fn list_quarantine(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let qdir = dir.join(strata_storage::QUARANTINE_DIR);
    std::fs::read_dir(&qdir)
        .map(|it| {
            it.flatten()
                .filter(|e| e.path().is_file())
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default()
}

// =============================================================================
// Crash boundary: before candidate classification
//
// No reclaim has touched anything. Reopen is a normal recovery. All
// live bytes present, no quarantine state.
// =============================================================================

#[test]
fn crash_before_any_reclaim_leaves_every_segment_live_after_reopen() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..10 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    let main_dir = branch_dir(&test_db.db, "main");
    let before = list_ssts(&main_dir);
    assert!(!before.is_empty());

    test_db.reopen();

    let after = list_ssts(&main_dir);
    assert_eq!(
        after.iter().collect::<std::collections::HashSet<_>>(),
        before.iter().collect::<std::collections::HashSet<_>>(),
        "no reclaim attempted; reopen must leave every segment in place",
    );

    // Healthy recovery + sanity reclaim attempt is a no-op.
    let report = test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("healthy gc");
    assert_eq!(report.files_deleted, 0);
}

// =============================================================================
// Crash boundary: after rename, before inventory publish ("the mismatch")
//
// Simulate the worst-case inventory disagreement by staging a file in
// `__quarantine__/` without a matching `quarantine.manifest` entry.
// Reopen must degrade recovery health to PolicyDowngrade via
// QuarantineInventoryMismatch, block GC, and retain the file per
// Invariant 2 (prefer space leak over false reclaim).
// =============================================================================

#[test]
fn crash_after_rename_before_publish_degrades_reopen_and_retains_file() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..5 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    let main_dir = branch_dir(&test_db.db, "main");

    // Simulate the "after rename, before inventory publish" crash
    // window by dropping a synthetic file directly into
    // `__quarantine__/` with no matching inventory entry. Using a new
    // filename avoids tripping the MissingManifestListed fault that
    // segments.manifest would otherwise raise if we moved a manifest-
    // listed segment. The reconciliation path that `RecoveryFault::
    // QuarantineInventoryMismatch` gates is identical either way.
    let quarantine_dir = main_dir.join(strata_storage::QUARANTINE_DIR);
    std::fs::create_dir_all(&quarantine_dir).unwrap();
    let staged = quarantine_dir.join("99999.sst");
    std::fs::write(&staged, b"bytes that would have been quarantined").unwrap();

    test_db.reopen_allowing_policy_downgrade();

    // Reopen must observe the mismatch and degrade. The staged file
    // must remain in place (retention debt, not data loss).
    assert!(
        staged.exists(),
        "quarantined file must survive reopen with no inventory",
    );

    match test_db.db.recovery_health() {
        strata_storage::RecoveryHealth::Degraded { class, faults } => {
            assert_eq!(class, strata_storage::DegradationClass::PolicyDowngrade);
            let has_mismatch = faults.iter().any(|f| {
                matches!(
                    f,
                    strata_storage::RecoveryFault::QuarantineInventoryMismatch { .. }
                )
            });
            assert!(
                has_mismatch,
                "reopen must emit QuarantineInventoryMismatch; got faults {faults:?}"
            );
        }
        other => panic!("expected PolicyDowngrade after mismatch; got {other:?}"),
    }

    // GC must refuse under the degraded class — space leaks, not false reclaim.
    let gc_result = test_db.db.storage().gc_orphan_segments();
    assert!(
        matches!(
            gc_result,
            Err(strata_storage::StorageError::GcRefusedDegradedRecovery { .. })
        ),
        "gc must refuse under degraded quarantine reconciliation; got {gc_result:?}",
    );
    assert!(staged.exists(), "victim must still exist after refused gc",);
}

// =============================================================================
// Crash boundary: inventory lists a file that is already gone
//
// Reopen's reconciliation must drop stale inventory entries whose files
// are missing from both original and quarantine locations, without
// degrading health (this is the benign "already-purged" / "never
// renamed" case).
// =============================================================================

#[test]
fn crash_after_publish_with_no_on_disk_file_drops_stale_inventory_without_degrade() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..5 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    let main_dir = branch_dir(&test_db.db, "main");

    // Stage a stale inventory entry for a file that does not exist.
    let entries = vec![strata_storage::QuarantineEntry {
        segment_id: 0x00DE_ADBE_EFC0_FFEE,
        filename: "not_real.sst".to_string(),
    }];
    strata_storage::write_quarantine_manifest(&main_dir, &entries).expect("write stale inventory");

    test_db.reopen();

    assert!(
        matches!(
            test_db.db.recovery_health(),
            strata_storage::RecoveryHealth::Healthy
        ),
        "stale inventory entry for absent file must reconcile silently",
    );

    // The reconciled manifest should be empty and the file removed.
    let m = strata_storage::read_quarantine_manifest(&main_dir)
        .expect("read reconciled manifest")
        .unwrap_or_default();
    assert!(
        m.entries.is_empty(),
        "reconciled inventory must be empty after dropping stale entry",
    );

    // GC still works.
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("healthy gc after reconcile");
}

// =============================================================================
// Crash boundary: after purge-eligibility publish, before final unlink
//
// Use real delete-path quarantine state: the file is already in
// `__quarantine__/` and listed in `quarantine.manifest`, but Stage-5 purge has
// not started yet. Reopen must keep health healthy, retain the file, and allow
// a later GC pass to purge it.
// =============================================================================

#[test]
fn crash_after_purge_eligibility_publish_before_final_unlink_keeps_quarantine_intact() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("solo").unwrap();
    for i in 0..6 {
        seed(&test_db.db, "solo", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "solo");

    test_db.db.branches().delete("solo").unwrap();
    let solo_dir = branch_dir(&test_db.db, "solo");
    let staged = list_quarantine(&solo_dir);
    assert!(
        !staged.is_empty(),
        "delete must stage at least one quarantined file before Stage-5 purge",
    );
    assert!(
        solo_dir.join(strata_storage::QUARANTINE_FILENAME).exists(),
        "delete must publish quarantine inventory before purge runs",
    );

    test_db.reopen();

    assert!(matches!(
        test_db.db.recovery_health(),
        strata_storage::RecoveryHealth::Healthy
    ));
    let after = list_quarantine(&solo_dir);
    assert_eq!(
        after.iter().collect::<std::collections::HashSet<_>>(),
        staged.iter().collect::<std::collections::HashSet<_>>(),
        "boundary-6 reopen must preserve quarantined files until a later purge",
    );

    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("healthy gc after boundary-6 reopen must purge staged files");
    assert!(
        list_quarantine(&solo_dir).is_empty(),
        "later gc should drain the preserved quarantine state",
    );
    assert!(
        !solo_dir.join(strata_storage::QUARANTINE_FILENAME).exists(),
        "inventory should be removed after the eventual purge",
    );
}

// =============================================================================
// Crash boundary: after final unlink, before final bookkeeping / report update
//
// Start from a real quarantine state, then remove the quarantined file but
// leave the inventory entry behind. Reopen must treat that as the benign
// "already unlinked before bookkeeping rewrite" case: silently drop the stale
// entry, stay healthy, and leave GC idempotent.
// =============================================================================

#[test]
fn crash_after_final_unlink_before_bookkeeping_drops_real_stale_inventory() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("solo").unwrap();
    for i in 0..6 {
        seed(&test_db.db, "solo", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "solo");

    test_db.db.branches().delete("solo").unwrap();
    let solo_dir = branch_dir(&test_db.db, "solo");
    let staged = list_quarantine(&solo_dir);
    assert!(
        !staged.is_empty(),
        "delete must stage at least one quarantined file before bookkeeping can lag",
    );
    for path in &staged {
        std::fs::remove_file(path).unwrap();
    }
    assert!(
        solo_dir.join(strata_storage::QUARANTINE_FILENAME).exists(),
        "simulated boundary-7 crash must leave stale quarantine inventory behind",
    );

    test_db.reopen();

    assert!(matches!(
        test_db.db.recovery_health(),
        strata_storage::RecoveryHealth::Healthy
    ));
    let reconciled = strata_storage::read_quarantine_manifest(&solo_dir)
        .expect("read reconciled manifest")
        .unwrap_or_default();
    assert!(
        reconciled.entries.is_empty(),
        "boundary-7 reopen must drop the stale inventory entry after the file is already gone",
    );
    assert!(
        list_quarantine(&solo_dir).is_empty(),
        "no quarantined files should remain after the stale entry is reconciled",
    );

    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("healthy gc after boundary-7 reconcile is idempotent");
}

// =============================================================================
// Crash boundary: quarantine dir with files but quarantine.manifest missing
// (no inventory at all).
//
// Same required outcome as the rename-without-publish case: degrade,
// retain files.
// =============================================================================

#[test]
fn crash_with_orphan_quarantine_dir_and_no_inventory_degrades_reopen() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..3 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    let main_dir = branch_dir(&test_db.db, "main");

    // Place an unrelated file in __quarantine__/ with no inventory.
    let qdir = main_dir.join(strata_storage::QUARANTINE_DIR);
    std::fs::create_dir_all(&qdir).unwrap();
    let phantom = qdir.join("9999.sst");
    std::fs::write(&phantom, b"phantom quarantined bytes").unwrap();

    test_db.reopen_allowing_policy_downgrade();

    match test_db.db.recovery_health() {
        strata_storage::RecoveryHealth::Degraded { class, .. } => {
            assert_eq!(class, strata_storage::DegradationClass::PolicyDowngrade);
        }
        other => panic!("expected degrade on orphan quarantine dir; got {other:?}"),
    }

    assert!(
        phantom.exists(),
        "phantom file in quarantine dir must be retained under degrade",
    );
}

#[test]
fn retention_report_refuses_when_quarantine_inventory_is_unreadable_in_session() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..4 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");

    let main_dir = branch_dir(&test_db.db, "main");
    std::fs::write(
        main_dir.join(strata_storage::QUARANTINE_FILENAME),
        b"corrupt",
    )
    .unwrap();

    let err = test_db
        .db
        .retention_report()
        .expect_err("unreadable quarantine inventory must refuse retention_report");
    assert!(
        matches!(
            err,
            strata_core::StrataError::RetentionReportUnavailable { .. }
        ),
        "retention_report must fail closed on unreadable quarantine inventory; got {err:?}",
    );
}

#[test]
fn retention_report_refuses_when_manifest_truth_is_unreadable_in_session() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..4 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");

    let main_dir = branch_dir(&test_db.db, "main");
    let manifest_path = main_dir.join("segments.manifest");
    let mut bytes = std::fs::read(&manifest_path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&manifest_path, &bytes).unwrap();

    let err = test_db
        .db
        .retention_report()
        .expect_err("corrupt manifest truth must refuse retention_report");
    assert!(
        matches!(
            err,
            strata_core::StrataError::RetentionReportUnavailable { .. }
        ),
        "retention_report must fail closed on unreadable manifest truth; got {err:?}",
    );
}

#[test]
fn retention_report_accounts_for_recovery_admitted_no_manifest_fallback() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("legacy").unwrap();
    for i in 0..6 {
        seed(&test_db.db, "legacy", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "legacy");

    let legacy_dir = branch_dir(&test_db.db, "legacy");
    std::fs::remove_file(legacy_dir.join("segments.manifest")).unwrap();

    test_db.reopen_allowing_policy_downgrade();

    assert!(
        matches!(
            test_db.db.recovery_health(),
            strata_storage::RecoveryHealth::Degraded {
                class: strata_storage::DegradationClass::PolicyDowngrade,
                ..
            }
        ),
        "missing manifest fallback must reopen as PolicyDowngrade",
    );

    let report = test_db
        .db
        .retention_report()
        .expect("recovery-admitted no-manifest fallback must still produce retention attribution");
    assert!(
        matches!(
            report.reclaim_status,
            strata_engine::ReclaimStatus::BlockedDegradedRecovery { ref class }
                if class == "PolicyDowngrade"
        ),
        "legacy fallback keeps reclaim blocked but attribution available",
    );
    let legacy = report
        .branches
        .iter()
        .find(|entry| entry.name == "legacy")
        .expect("fallback branch must appear in retention_report");
    assert!(
        legacy.exclusive_bytes > 0 || legacy.shared_bytes > 0,
        "fallback branch must still report retained own bytes",
    );
}

// =============================================================================
// Crash boundary: normal quarantine→purge cycle completes, reopen is a
// no-op. Pins idempotency / repeat-reopen.
// =============================================================================

#[test]
fn successful_reclaim_round_trip_is_idempotent_across_reopen() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("solo").unwrap();
    for i in 0..10 {
        seed(&test_db.db, "solo", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "solo");
    let solo_dir = branch_dir(&test_db.db, "solo");
    assert!(!list_ssts(&solo_dir).is_empty());

    test_db.db.branches().delete("solo").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("healthy; gc succeeds");

    // After gc, no bytes, no quarantine.
    assert!(list_ssts(&solo_dir).is_empty());
    assert!(list_quarantine(&solo_dir).is_empty());

    test_db.reopen();

    // Reopen after clean reclaim: still no bytes, health healthy.
    assert!(matches!(
        test_db.db.recovery_health(),
        strata_storage::RecoveryHealth::Healthy
    ));
    assert!(list_ssts(&solo_dir).is_empty());
    assert!(list_quarantine(&solo_dir).is_empty());

    // Re-running gc is idempotent.
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("idempotent gc");
}

// =============================================================================
// Reopen re-establishes a clean report after the stale entry is dropped.
// Pins A6.
// =============================================================================

#[test]
fn retention_report_recovers_to_trustworthy_state_after_stale_entry_drops() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..5 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");

    // Stale inventory entry for a non-existent file — reconciled silently.
    let main_dir = branch_dir(&test_db.db, "main");
    strata_storage::write_quarantine_manifest(
        &main_dir,
        &[strata_storage::QuarantineEntry {
            segment_id: 7,
            filename: "phantom.sst".into(),
        }],
    )
    .unwrap();

    test_db.reopen();
    assert!(matches!(
        test_db.db.recovery_health(),
        strata_storage::RecoveryHealth::Healthy
    ));

    let report = test_db
        .db
        .retention_report()
        .expect("retention_report recovers to Ok after silent reconcile");
    assert_eq!(report.reclaim_status, strata_engine::ReclaimStatus::Allowed,);
}
