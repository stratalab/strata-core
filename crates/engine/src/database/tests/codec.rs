//! Codec uniformity and reuse-identity acceptance tests (T3-E7).
//!
//! These tests pin the contract that every open-time-only knob classified
//! in `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`
//! triggers [`StrataError::IncompatibleReuse`] when it disagrees between
//! two openers of the same path — never a silent downgrade, never an
//! opaque `internal` error. The tests cover codec drift (primary and
//! follower), early codec validation, and the two knobs that the initial
//! T3-E7 draft left out of the signature: `background_threads` and
//! `allow_lossy_recovery`.
//!
//! **Why not a full write→crash→reopen→read round-trip with a non-identity
//! codec?** WAL recovery does not yet support non-identity codecs
//! (`crates/engine/src/database/open.rs` blocks this at open time). Cache
//! durability does not persist across opens. So the cross-process
//! persistence test called for by the scope is deferred until WAL codec
//! support lands; see the config matrix doc for the target-state note.

use super::*;
use crate::SearchSubsystem;

/// Key material for the AES-256-GCM codec. Used only for test setup.
const TEST_AES_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

/// RAII guard that sets an env var on construction and removes it on drop.
/// Ensures the encryption-key env var does not leak to sibling tests even
/// if an assertion panics mid-body.
struct EnvVarGuard(&'static str);

impl EnvVarGuard {
    fn set(name: &'static str, value: &str) -> Self {
        std::env::set_var(name, value);
        Self(name)
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        std::env::remove_var(self.0);
    }
}

/// Registry-reuse path: an already-open database rejects a second opener
/// that asks for a different codec, with [`StrataError::IncompatibleReuse`].
///
/// Uses cache durability so neither opener hits the WAL-codec block at
/// `open.rs:842`. The first handle is held alive for the duration of the
/// second call so the reuse path is actually exercised (not the cold-reopen
/// path that goes through the MANIFEST check).
#[test]
#[serial(open_databases)]
fn test_registry_reuse_rejects_different_codec() {
    OPEN_DATABASES.lock().clear();
    let _key_guard = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let cfg_aes = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(cfg_aes)
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();

    // Second opener, same path, same durability, DIFFERENT codec.
    let cfg_identity = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            codec: "identity".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let result = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(cfg_identity)
            .with_subsystem(SearchSubsystem),
    );

    match result {
        Err(e) => {
            assert!(
                matches!(e, StrataError::IncompatibleReuse { .. }),
                "codec mismatch at registry reuse must surface as IncompatibleReuse, got: {e:?}"
            );
            assert!(
                e.to_string().to_lowercase().contains("codec"),
                "error message must name the codec dimension, got: {e}"
            );
        }
        Ok(_) => panic!("registry reuse with mismatched codec must not succeed"),
    }

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
    // Env var released by `_key_guard` on drop.
}

/// Follower path: opening a follower with a codec different from the
/// primary's on-disk MANIFEST must be rejected before any WAL bytes are
/// touched, with [`StrataError::IncompatibleReuse`].
///
/// Pre-T3-E7, followers silently used the MANIFEST codec while publishing
/// the config codec in the runtime signature — a silent drift.
#[test]
#[serial(open_databases)]
fn test_follower_rejects_codec_mismatch_with_manifest() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    // Create a primary with the default (identity) codec so the MANIFEST
    // records codec_id = "identity".
    {
        let primary = Database::open(&db_path).unwrap();
        primary.shutdown().unwrap();
    }
    OPEN_DATABASES.lock().clear();

    // Attempt to open a follower that claims a different codec.
    // Use cache durability on the follower so the earlier
    // non-identity + WAL rejection (which would fire on Standard/Always)
    // does not mask the MANIFEST drift check we're exercising here.
    let _key_guard = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);
    let cfg_follower = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let result = Database::open_runtime(
        super::spec::OpenSpec::follower(&db_path)
            .with_config(cfg_follower)
            .with_subsystem(SearchSubsystem),
    );

    match result {
        Err(e) => {
            assert!(
                matches!(e, StrataError::IncompatibleReuse { .. }),
                "follower codec drift must surface as IncompatibleReuse, got: {e:?}"
            );
            let msg = e.to_string();
            assert!(
                msg.contains("codec mismatch") && msg.contains("follower"),
                "error message must name codec mismatch and the follower role, got: {msg}"
            );
        }
        Ok(_) => panic!("follower open with mismatched codec must not succeed"),
    }

    OPEN_DATABASES.lock().clear();
}

/// Follower codec validation must catch an unknown codec name the same
/// way the primary does, regardless of whether a MANIFEST exists.
/// Pre-T3-E7 review, the follower only validated inside the MANIFEST-exists
/// branch — a fresh/manifest-less follower target could accept a bogus codec.
#[test]
#[serial(open_databases)]
fn test_follower_rejects_unknown_codec_without_manifest() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("fresh_follower_target");
    // Create the directory so canonicalize succeeds, but do NOT create a
    // MANIFEST. This is the "fresh/manifest-less" case from the review.
    std::fs::create_dir_all(&db_path).unwrap();

    let cfg = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            codec: "lz4-does-not-exist".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let result = Database::open_runtime(
        super::spec::OpenSpec::follower(&db_path)
            .with_config(cfg)
            .with_subsystem(SearchSubsystem),
    );

    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("invalid storage codec") && msg.contains("lz4-does-not-exist"),
                "error must name the bogus codec, got: {msg}"
            );
        }
        Ok(_) => panic!("follower open with unknown codec must not succeed"),
    }
}

/// Sanity check: a follower whose config codec matches the primary's
/// MANIFEST codec (both `identity`) opens without error. This pins that
/// the drift check does not produce false positives on the happy path.
#[test]
#[serial(open_databases)]
fn test_follower_codec_match_opens_cleanly() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    {
        let primary = Database::open(&db_path).unwrap();
        primary.shutdown().unwrap();
    }
    OPEN_DATABASES.lock().clear();

    let follower = Database::open_runtime(
        super::spec::OpenSpec::follower(&db_path).with_subsystem(SearchSubsystem),
    )
    .expect("follower with matching codec must open");

    let signature = follower
        .runtime_signature()
        .expect("follower must publish a runtime signature");
    assert_eq!(
        signature.codec_name, "identity",
        "follower signature must publish the MANIFEST codec on the happy path"
    );
}

/// `background_threads` is classified as open-time-only: the thread pool
/// is sized at construction and never resized. A second opener asking for
/// a different count must get `IncompatibleReuse`, not a silent reuse of
/// the already-sized pool.
#[test]
#[serial(open_databases)]
fn test_registry_reuse_rejects_different_background_threads() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let mut cfg_a = StrataConfig {
        durability: "cache".to_string(),
        ..StrataConfig::default()
    };
    cfg_a.storage.background_threads = 2;

    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(cfg_a)
            .with_subsystem(SearchSubsystem),
    )
    .expect("first opener with background_threads=2 must succeed");

    let mut cfg_b = StrataConfig {
        durability: "cache".to_string(),
        ..StrataConfig::default()
    };
    cfg_b.storage.background_threads = 8;

    let result = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(cfg_b)
            .with_subsystem(SearchSubsystem),
    );

    match result {
        Err(e) => {
            assert!(
                matches!(e, StrataError::IncompatibleReuse { .. }),
                "background_threads drift must surface as IncompatibleReuse, got: {e:?}"
            );
            assert!(
                e.to_string().contains("background_threads"),
                "error message must name the background_threads dimension, got: {e}"
            );
        }
        Ok(_) => panic!("registry reuse with mismatched background_threads must not succeed"),
    }

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// `allow_lossy_recovery` is open-time-only: by the time we reuse an
/// already-open instance, recovery has already happened — strictly or
/// lossily. A second opener must not silently accept an instance whose
/// recovery policy differs from what it asked for.
#[test]
#[serial(open_databases)]
fn test_registry_reuse_rejects_different_allow_lossy_recovery() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let cfg_strict = StrataConfig {
        durability: "cache".to_string(),
        allow_lossy_recovery: false,
        ..StrataConfig::default()
    };
    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(cfg_strict)
            .with_subsystem(SearchSubsystem),
    )
    .expect("first opener with allow_lossy_recovery=false must succeed");

    let cfg_lossy = StrataConfig {
        durability: "cache".to_string(),
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let result = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(cfg_lossy)
            .with_subsystem(SearchSubsystem),
    );

    match result {
        Err(e) => {
            assert!(
                matches!(e, StrataError::IncompatibleReuse { .. }),
                "allow_lossy_recovery drift must surface as IncompatibleReuse, got: {e:?}"
            );
            assert!(
                e.to_string().contains("allow_lossy_recovery"),
                "error message must name the allow_lossy_recovery dimension, got: {e}"
            );
        }
        Ok(_) => panic!("registry reuse with mismatched allow_lossy_recovery must not succeed"),
    }

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}
