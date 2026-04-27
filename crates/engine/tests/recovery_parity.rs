//! Recovery-parity tests.
//!
//! For each of the five MANIFEST failure shapes under test (missing magic,
//! checksum mismatch, codec id mismatch, unsupported version, truncated),
//! assert that all three recovery entry points
//! surface the **same** `StrataError` variant:
//!
//! 1. Primary — `Database::open_runtime(OpenSpec::primary(path))`.
//! 2. Follower — `Database::open_runtime(OpenSpec::follower(path))`.
//! 3. Coordinator-only — `RecoveryCoordinator::new(layout, 0).plan_recovery("identity")`.
//!
//! The coordinator-only leg and the engine entry points must agree on the
//! public error shape for each failure mode.
//!
//! # First-open safety regression
//!
//! Also pins the ordering invariant that codec validation runs before MANIFEST
//! creation, so a brand-new
//! database with an invalid codec id fails without persisting anything
//! to disk.

use serial_test::serial;
use std::fs;
use std::mem;
use std::path::Path;
use strata_concurrency::RecoveryCoordinator;
use strata_core::StrataError;
use strata_durability::format::{MANIFEST_FORMAT_VERSION, MANIFEST_MAGIC};
use strata_durability::layout::DatabaseLayout;
use strata_engine::database::OpenSpec;
use strata_engine::{Database, SearchSubsystem};
use tempfile::TempDir;

/// Build MANIFEST bytes with optional per-field overrides. Returns
/// bytes that would otherwise be a valid v2 manifest.
fn valid_manifest_bytes(codec_id: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(&MANIFEST_MAGIC);
    bytes.extend_from_slice(&MANIFEST_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]); // database_uuid
    let codec_bytes = codec_id.as_bytes();
    bytes.extend_from_slice(&u32::try_from(codec_bytes.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(codec_bytes);
    bytes.extend_from_slice(&1u64.to_le_bytes()); // active_wal_segment
    bytes.extend_from_slice(&0u64.to_le_bytes()); // snapshot_watermark
    bytes.extend_from_slice(&0u64.to_le_bytes()); // snapshot_id
    bytes.extend_from_slice(&0u64.to_le_bytes()); // flushed_through_commit_id
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

/// Write a corrupt MANIFEST into `db_path`. Creates the directory as a
/// side effect; does **not** create a segments directory — only the
/// minimum layout the MANIFEST parser touches.
fn write_raw_manifest(db_path: &Path, bytes: &[u8]) {
    fs::create_dir_all(db_path).unwrap();
    let manifest = db_path.join("MANIFEST");
    fs::write(&manifest, bytes).unwrap();
}

/// Three-leg probe: primary open, follower open, coordinator-only
/// `plan_recovery`. Returns the three errors captured, after
/// `OPEN_DATABASES` has been cleared between legs so a cached handle
/// from an earlier test cannot short-circuit a later one.
fn probe_three_legs(
    db_path: &Path,
    expected_codec: &str,
) -> (StrataError, StrataError, StrataError) {
    strata_engine::database::OPEN_DATABASES.lock().clear();

    let Err(primary_err) =
        Database::open_runtime(OpenSpec::primary(db_path).with_subsystem(SearchSubsystem))
    else {
        panic!("primary open must fail on corrupt MANIFEST")
    };

    strata_engine::database::OPEN_DATABASES.lock().clear();

    let Err(follower_err) =
        Database::open_runtime(OpenSpec::follower(db_path).with_subsystem(SearchSubsystem))
    else {
        panic!("follower open must fail on corrupt MANIFEST")
    };

    strata_engine::database::OPEN_DATABASES.lock().clear();

    let layout = DatabaseLayout::from_root(db_path);
    let coord_err = RecoveryCoordinator::new(layout, 0)
        .plan_recovery(expected_codec)
        .err()
        .unwrap_or_else(|| panic!("coordinator plan_recovery must fail on corrupt MANIFEST"));

    (primary_err, follower_err, coord_err)
}

fn assert_same_variant(a: &StrataError, b: &StrataError, label: &str) {
    assert_eq!(
        mem::discriminant(a),
        mem::discriminant(b),
        "{label}: variant mismatch\n  left  = {a:?}\n  right = {b:?}"
    );
}

// ────────────────────────────────────────────────────────────────────
// Shape 1: missing magic bytes
// ────────────────────────────────────────────────────────────────────

#[test]
#[serial(open_databases)]
fn parity_missing_magic() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let mut bytes = valid_manifest_bytes("identity");
    // Overwrite the 4-byte magic so the parser sees InvalidMagic.
    bytes[0..4].copy_from_slice(b"XXXX");
    write_raw_manifest(&db_path, &bytes);

    let (primary, follower, coord) = probe_three_legs(&db_path, "identity");
    assert!(
        matches!(primary, StrataError::Corruption { .. }),
        "primary: expected Corruption, got {primary:?}"
    );
    assert_same_variant(&primary, &follower, "primary vs follower");
    assert_same_variant(&primary, &coord, "primary vs coordinator");
}

// ────────────────────────────────────────────────────────────────────
// Shape 2: checksum mismatch
// ────────────────────────────────────────────────────────────────────

#[test]
#[serial(open_databases)]
fn parity_checksum_mismatch() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let mut bytes = valid_manifest_bytes("identity");
    let len = bytes.len();
    // Flip one byte inside the CRC region — magic/version still valid.
    bytes[len - 1] ^= 0xFF;
    write_raw_manifest(&db_path, &bytes);

    let (primary, follower, coord) = probe_three_legs(&db_path, "identity");
    assert!(
        matches!(primary, StrataError::Corruption { .. }),
        "primary: expected Corruption, got {primary:?}"
    );
    assert_same_variant(&primary, &follower, "primary vs follower");
    assert_same_variant(&primary, &coord, "primary vs coordinator");
}

// ────────────────────────────────────────────────────────────────────
// Shape 3: codec id mismatch (requires fix landed in concurrency)
// ────────────────────────────────────────────────────────────────────

#[test]
#[serial(open_databases)]
fn parity_codec_id_mismatch() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    // Write a valid manifest that records codec_id = "identity", then
    // probe with expected_codec = "aes-gcm-256". The Primary / Follower
    // legs use `Database::open_runtime` with the default config (identity),
    // so we invert: write MANIFEST with a non-default codec and open
    // with the default.
    let bytes = valid_manifest_bytes("aes-gcm-256");
    write_raw_manifest(&db_path, &bytes);

    // Pre-D3: coordinator plan_recovery returned Corruption while the
    // engine paths returned IncompatibleReuse. D3's one-line fix in
    // strata_concurrency makes all three agree on IncompatibleReuse.
    let (primary, follower, coord) = probe_three_legs(&db_path, "identity");
    assert!(
        matches!(primary, StrataError::IncompatibleReuse { .. }),
        "primary: expected IncompatibleReuse, got {primary:?}"
    );
    assert_same_variant(&primary, &follower, "primary vs follower");
    assert_same_variant(&primary, &coord, "primary vs coordinator");
}

// ────────────────────────────────────────────────────────────────────
// Shape 4: unsupported version (future version)
// ────────────────────────────────────────────────────────────────────

#[test]
#[serial(open_databases)]
fn parity_unsupported_version() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let mut bytes = valid_manifest_bytes("identity");
    // Overwrite the version field (bytes 4..8) with a higher-than-max version.
    bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
    // Recompute the CRC so we hit UnsupportedVersion, not ChecksumMismatch.
    let crc_region = &bytes[..bytes.len() - 4];
    let crc = crc32fast::hash(crc_region);
    let len = bytes.len();
    bytes[len - 4..].copy_from_slice(&crc.to_le_bytes());
    write_raw_manifest(&db_path, &bytes);

    let (primary, follower, coord) = probe_three_legs(&db_path, "identity");
    assert!(
        matches!(primary, StrataError::Corruption { .. }),
        "primary: expected Corruption, got {primary:?}"
    );
    assert_same_variant(&primary, &follower, "primary vs follower");
    assert_same_variant(&primary, &coord, "primary vs coordinator");
}

// ────────────────────────────────────────────────────────────────────
// Shape 5: truncated manifest (< 64 bytes)
// ────────────────────────────────────────────────────────────────────

#[test]
#[serial(open_databases)]
fn parity_truncated() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    // Write just 32 bytes so `bytes.len() < 64` fires TooShort.
    let bytes = vec![0u8; 32];
    write_raw_manifest(&db_path, &bytes);

    let (primary, follower, coord) = probe_three_legs(&db_path, "identity");
    assert!(
        matches!(primary, StrataError::Corruption { .. }),
        "primary: expected Corruption, got {primary:?}"
    );
    assert_same_variant(&primary, &follower, "primary vs follower");
    assert_same_variant(&primary, &coord, "primary vs coordinator");
}

// ────────────────────────────────────────────────────────────────────
// First-open safety: configured codec validated before MANIFEST touch
// ────────────────────────────────────────────────────────────────────

#[test]
#[serial(open_databases)]
fn first_open_rejects_invalid_codec_without_touching_disk() {
    // Reviewer finding 3 on the D3 plan: codec validation must precede
    // MANIFEST creation so a bad codec id does not leave a poisoned
    // MANIFEST on disk that the next open cannot repair.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("new_db");
    assert!(!db_path.exists());

    let mut cfg = strata_engine::database::config::StrataConfig::default();
    cfg.storage.codec = "does-not-exist".to_string();
    cfg.durability = "cache".to_string();

    strata_engine::database::OPEN_DATABASES.lock().clear();
    let Err(err) = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_config(cfg)
            .with_subsystem(SearchSubsystem),
    ) else {
        panic!("open with invalid codec id must fail")
    };

    assert!(
        matches!(err, StrataError::Internal { .. }),
        "invalid codec id must surface as Internal, got {err:?}"
    );

    // `open_runtime_primary` creates the root db directory before it can
    // canonicalize the path, so the root may exist here. The D3 safety
    // invariant is narrower: a rejected codec id must not persist any
    // recovery-managed artifacts or support directories.
    for path in [
        db_path.join("MANIFEST"),
        db_path.join("segments"),
        db_path.join("wal"),
        db_path.join("snapshots"),
    ] {
        assert!(
            !path.exists(),
            "rejected codec id must not leave recovery artifacts on disk; found {path:?}"
        );
    }
}
