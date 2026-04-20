//! D4 storage-fault policy parity tests.
//!
//! Pins the strict-vs-lossy policy branch added inside
//! `Database::run_recovery` plus the separate `Database::recovery_health()`
//! observability surface:
//!
//! - Strict opens refuse `DegradationClass::DataLoss` (corrupt branch
//!   manifest, missing manifest-listed segment, …) and refuse
//!   `PolicyDowngrade` (no-manifest legacy fallback) unless
//!   `StrataConfig::allow_missing_manifest` is set.
//! - Lossy opens (`allow_lossy_recovery = true`) permit every degraded
//!   class and leave `last_lossy_recovery_report()` untouched because
//!   no WAL wipe occurred — the health is observable only through
//!   `recovery_health()`.
//! - The WAL wipe-and-reopen contract (DR-011) remains distinct: a
//!   WAL-replay failure with `allow_lossy_recovery = true` still
//!   populates `last_lossy_recovery_report()` exactly as before D4 and
//!   leaves `recovery_health()` at `Healthy`.

use std::fs;
use std::path::{Path, PathBuf};

use serial_test::serial;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_core::StrataError;
use strata_engine::database::OpenSpec;
use strata_engine::{
    Database, DegradationClass, KVStore, RecoveryHealth, SearchSubsystem, StrataConfig,
};
use tempfile::TempDir;

/// Assert that a strict-open error is the D4 storage-degraded refusal,
/// not some other corruption surfaced from elsewhere in the recovery
/// ladder. `RecoveryError::StorageDegraded(health)` maps to
/// `StrataError::Corruption` with the `Debug` form of the health in the
/// message (see `recovery_error.rs::From<RecoveryError> for StrataError`),
/// so the expected class name must appear in the rendered message.
fn assert_storage_degraded_refusal(err: StrataError, expected_class: DegradationClass) {
    match err {
        StrataError::Corruption { message } => {
            let needle = match expected_class {
                DegradationClass::DataLoss => "DataLoss",
                DegradationClass::PolicyDowngrade => "PolicyDowngrade",
                DegradationClass::Telemetry => "Telemetry",
                _ => panic!("unexpected DegradationClass in test helper"),
            };
            assert!(
                message.contains("storage recovered in degraded state"),
                "strict refusal must route through RecoveryError::StorageDegraded; got: {message}"
            );
            assert!(
                message.contains(needle),
                "storage-degraded refusal must name expected class {needle:?}; got: {message}"
            );
        }
        other => panic!(
            "expected StrataError::Corruption from D4 strict refusal, got {other:?}"
        ),
    }
}

fn hex_encode_branch(branch_id: &BranchId) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(32);
    for &b in branch_id.as_bytes() {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn branch_manifest_path(db_path: &Path, branch_id: &BranchId) -> PathBuf {
    db_path
        .join("segments")
        .join(hex_encode_branch(branch_id))
        .join("segments.manifest")
}

fn clear_open_handles() {
    strata_engine::database::OPEN_DATABASES.lock().clear();
}

/// Open the database, seed one KV, force a memtable rotate + flush so a
/// per-branch `segments.manifest` + `.sst` land on disk, then shut down.
/// Mirrors the storage-level fault-injection setup at
/// `crates/storage/src/segmented/tests/gc_under_degradation.rs:17-24` so
/// the engine-level D4 tests can corrupt / delete the same artifacts.
fn seed_branch_with_manifest(db_path: &Path) -> BranchId {
    let branch_id = BranchId::new();
    let db =
        Database::open_runtime(OpenSpec::primary(db_path).with_subsystem(SearchSubsystem)).unwrap();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "seed", Value::Int(1)).unwrap();

    db.storage().rotate_memtable(&branch_id);
    db.storage().flush_oldest_frozen(&branch_id).unwrap();
    db.flush().unwrap();
    db.shutdown().unwrap();
    branch_id
}

/// Strict open with a corrupt branch manifest must refuse. The caller
/// observes an error (`StrataError::Corruption` via D3's
/// `RecoveryError::StorageDegraded` mapping).
#[test]
#[serial(open_databases)]
fn strict_refuses_corrupt_manifest_as_data_loss() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().to_path_buf();

    let branch_id = seed_branch_with_manifest(&db_path);
    clear_open_handles();

    let path = branch_manifest_path(&db_path, &branch_id);
    let mut bytes = fs::read(&path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    fs::write(&path, &bytes).unwrap();

    let strict =
        Database::open_runtime(OpenSpec::primary(&db_path).with_subsystem(SearchSubsystem));
    match strict {
        Ok(_) => panic!("strict open must refuse corrupt branch manifest (DataLoss)"),
        Err(err) => assert_storage_degraded_refusal(err, DegradationClass::DataLoss),
    }
}

/// Lossy open with the same corrupt manifest succeeds; `recovery_health()`
/// surfaces the degraded classification and `last_lossy_recovery_report()`
/// stays `None` because no WAL wipe occurred.
#[test]
#[serial(open_databases)]
fn lossy_accepts_corrupt_manifest_and_exposes_degraded_health() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().to_path_buf();

    let branch_id = seed_branch_with_manifest(&db_path);
    clear_open_handles();

    let path = branch_manifest_path(&db_path, &branch_id);
    let mut bytes = fs::read(&path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    fs::write(&path, &bytes).unwrap();

    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let db = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(SearchSubsystem)
            .with_config(cfg),
    )
    .expect("lossy open must accept DataLoss health");

    match db.recovery_health() {
        RecoveryHealth::Degraded { class, .. } => {
            assert_eq!(
                class,
                DegradationClass::DataLoss,
                "corrupt branch manifest must classify as DataLoss"
            );
        }
        RecoveryHealth::Healthy => {
            panic!("recovery_health must be Degraded after corrupt manifest")
        }
        other => panic!("unexpected health variant: {other:?}"),
    }
    assert!(
        db.last_lossy_recovery_report().is_none(),
        "storage-degradation lossy path must not populate \
         last_lossy_recovery_report — no WAL wipe occurred"
    );
}

/// Strict open refuses `PolicyDowngrade` (the no-manifest legacy
/// fallback) when `allow_missing_manifest` is not set.
#[test]
#[serial(open_databases)]
fn strict_refuses_no_manifest_fallback_as_policy_downgrade() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().to_path_buf();

    let branch_id = seed_branch_with_manifest(&db_path);
    clear_open_handles();

    fs::remove_file(branch_manifest_path(&db_path, &branch_id)).unwrap();

    let strict =
        Database::open_runtime(OpenSpec::primary(&db_path).with_subsystem(SearchSubsystem));
    match strict {
        Ok(_) => panic!("strict open must refuse PolicyDowngrade without allow_missing_manifest"),
        Err(err) => assert_storage_degraded_refusal(err, DegradationClass::PolicyDowngrade),
    }
}

/// With `allow_missing_manifest = true`, the no-manifest fallback is
/// accepted and `recovery_health()` reports `PolicyDowngrade`.
/// `allow_lossy_recovery` is deliberately left `false` — this is the
/// narrow opt-in for unmanifested databases, not a blanket lossy mode.
#[test]
#[serial(open_databases)]
fn allow_missing_manifest_accepts_policy_downgrade() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().to_path_buf();

    let branch_id = seed_branch_with_manifest(&db_path);
    clear_open_handles();

    fs::remove_file(branch_manifest_path(&db_path, &branch_id)).unwrap();

    let cfg = StrataConfig {
        allow_missing_manifest: true,
        ..StrataConfig::default()
    };
    let db = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(SearchSubsystem)
            .with_config(cfg),
    )
    .expect("allow_missing_manifest must accept PolicyDowngrade");

    match db.recovery_health() {
        RecoveryHealth::Degraded { class, .. } => {
            assert_eq!(class, DegradationClass::PolicyDowngrade);
        }
        other => panic!("expected Degraded/PolicyDowngrade, got {other:?}"),
    }
    assert!(
        db.last_lossy_recovery_report().is_none(),
        "PolicyDowngrade without WAL failure must not populate the WAL-wipe report"
    );

    // The PolicyDowngrade fallback promotes orphan segments to L0, so the
    // seed value must still be queryable — otherwise the opt-in would be a
    // silent data-loss path rather than a legacy-compat one.
    let kv = KVStore::new(db.clone());
    assert_eq!(
        kv.get(&branch_id, "default", "seed").unwrap(),
        Some(Value::Int(1)),
        "no-manifest L0 promotion must preserve flushed seed data"
    );
}

/// A clean reopen (no injected faults) must report
/// `RecoveryHealth::Healthy`.
#[test]
#[serial(open_databases)]
fn clean_reopen_is_healthy() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().to_path_buf();

    seed_branch_with_manifest(&db_path);
    clear_open_handles();

    let db =
        Database::open_runtime(OpenSpec::primary(&db_path).with_subsystem(SearchSubsystem)).unwrap();

    assert!(
        matches!(db.recovery_health(), RecoveryHealth::Healthy),
        "clean reopen must be Healthy, got {:?}",
        db.recovery_health()
    );
    assert!(db.last_lossy_recovery_report().is_none());
}

/// Contract regression for DR-011. A WAL-replay failure with
/// `allow_lossy_recovery = true` still populates
/// `last_lossy_recovery_report()` exactly as before D4 (the wipe +
/// `LossyRecoveryReport` path is unchanged), and leaves
/// `recovery_health()` at `Healthy` because no storage-side degradation
/// occurred. This pins that the two observability surfaces never
/// conflate.
#[test]
#[serial(open_databases)]
fn wal_lossy_report_unchanged_and_recovery_health_healthy() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().to_path_buf();
    let branch_id = BranchId::new();

    // Write a record to the WAL without producing on-disk segments, so
    // the post-wipe segment walk sees a clean directory and reports
    // Healthy. Pattern matches follower_tests.rs test_follower_lossy_recovery_populates_report.
    {
        let db = Database::open_runtime(
            OpenSpec::primary(&db_path).with_subsystem(SearchSubsystem),
        )
        .unwrap();
        let kv = KVStore::new(db.clone());
        kv.put(&branch_id, "default", "k1", Value::Int(1)).unwrap();
        db.flush().unwrap();
        db.shutdown().unwrap();
    }
    clear_open_handles();

    // Append a garbage WAL segment so the reader errors mid-recovery.
    let wal_dir = db_path.join("wal");
    let corrupt_segment = wal_dir.join("wal-000099.seg");
    fs::write(&corrupt_segment, b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER").unwrap();

    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let db = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(SearchSubsystem)
            .with_config(cfg),
    )
    .expect("WAL wipe-and-reopen must succeed with allow_lossy_recovery");

    let report = db
        .last_lossy_recovery_report()
        .expect("WAL-replay failure must populate LossyRecoveryReport as before D4");
    assert!(
        report.discarded_on_wipe,
        "whole-database wipe is the DR-011 pinned contract"
    );
    assert!(!report.error.is_empty());
    assert!(
        matches!(db.recovery_health(), RecoveryHealth::Healthy),
        "WAL wipe rebuilds an empty in-memory store and segment recovery \
         walks a clean directory — recovery_health must stay Healthy, got {:?}",
        db.recovery_health()
    );
}
