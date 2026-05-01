//! Codec uniformity and reuse-identity acceptance tests (T3-E7 + T3-E12).
//!
//! These tests pin two related contracts:
//!
//! 1. **T3-E7 reuse identity** — every open-time-only knob classified in
//!    `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`
//!    triggers [`StrataError::IncompatibleReuse`] when it disagrees
//!    between two openers of the same path (codec drift, background
//!    threads, `allow_lossy_recovery`). Never a silent downgrade,
//!    never an opaque `internal` error.
//!
//! 2. **T3-E12 codec-aware WAL** — non-identity codecs (`aes-gcm-256`)
//!    round-trip cleanly through the v3 outer envelope + codec-threaded
//!    reader for BOTH clean-shutdown and crash-recovery, across large
//!    records, partial tails, mid-segment corruption, and wrong-key
//!    reopen. The T3-E12 AES-GCM tests at the bottom of this file pin
//!    the six contract rows from the tracking doc's §Phase 2 test
//!    list (`docs/design/execution/t3-e12-phases.md`).

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

/// Hardware profiling must stay ephemeral: the first open on any host
/// must not persist profile-rewritten values into `strata.toml`. If it
/// did, moving the database directory to a different host class would
/// inherit the first host's tuning instead of re-profiling.
///
/// The test compares the pre-profile default config to the post-profile
/// version. If they match on this host (e.g. a Desktop box where the
/// profile is a no-op), we skip — there is nothing to leak. Otherwise we
/// open a database with defaults, shut down, parse `strata.toml` from
/// disk, and assert it still shows the pre-profile values.
#[test]
#[serial(open_databases)]
fn test_profiled_defaults_are_not_persisted_to_strata_toml() {
    OPEN_DATABASES.lock().clear();

    let baseline = StorageConfig::default();
    let profiled = {
        let mut cfg = StrataConfig::default();
        crate::apply_hardware_profile_if_defaults(&mut cfg);
        cfg.storage
    };

    // Skip on hosts where the profile happens to be a no-op for the
    // fields we check. Otherwise the test is a tautology here.
    if baseline.background_threads == profiled.background_threads
        && baseline.write_buffer_size == profiled.write_buffer_size
        && baseline.target_file_size == profiled.target_file_size
        && baseline.level_base_bytes == profiled.level_base_bytes
    {
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(StrataConfig {
                durability: "cache".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();
    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();

    let persisted = StrataConfig::from_file(&db_path.join("strata.toml"))
        .expect("strata.toml must be written on first open");

    // Every field the profile could have rewritten must still match the
    // pre-profile baseline on disk, even if the profile diverges from it.
    assert_eq!(
        persisted.storage.background_threads, baseline.background_threads,
        "background_threads must not be profiled onto disk"
    );
    assert_eq!(
        persisted.storage.write_buffer_size, baseline.write_buffer_size,
        "write_buffer_size must not be profiled onto disk"
    );
    assert_eq!(
        persisted.storage.target_file_size, baseline.target_file_size,
        "target_file_size must not be profiled onto disk"
    );
    assert_eq!(
        persisted.storage.level_base_bytes, baseline.level_base_bytes,
        "level_base_bytes must not be profiled onto disk"
    );

    // But the running runtime did see the profile — the signature shows
    // the post-profile thread count.
    // (db was moved into shutdown; open a fresh handle to confirm.)
    let db2 = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(StrataConfig {
                durability: "cache".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();
    assert_eq!(
        db2.runtime_signature()
            .expect("signature must exist")
            .background_threads,
        profiled.background_threads,
        "the live signature must still reflect the profiled value"
    );
    db2.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// Review-found bug (post-commit 5e432cce): the signature must reflect
/// the *post-hardware-profile* value of `background_threads`, not the
/// pre-profile default. If the signature captures the pre-profile value,
/// a second opener who explicitly requests the actual effective thread
/// count will be falsely rejected as incompatible even though the
/// scheduler was sized with exactly their value. This test opens with
/// defaults (profile applies), reads the effective value off the live
/// signature, then opens again explicitly requesting that value —
/// which must succeed.
#[test]
#[serial(open_databases)]
fn test_profile_applies_before_signature_so_reuse_with_explicit_value_succeeds() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let db1 = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(StrataConfig {
                durability: "cache".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(SearchSubsystem),
    )
    .expect("first opener with defaults must succeed");

    let effective_threads = db1
        .runtime_signature()
        .expect("db1 must publish a runtime signature")
        .background_threads;

    let mut cfg_explicit = StrataConfig {
        durability: "cache".to_string(),
        ..StrataConfig::default()
    };
    cfg_explicit.storage.background_threads = effective_threads;

    let db2 = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(cfg_explicit)
            .with_subsystem(SearchSubsystem),
    )
    .expect("explicit value matching the profiled effective value must reuse the instance");

    assert!(
        Arc::ptr_eq(&db1, &db2),
        "reuse must return the same Arc, not a fresh database"
    );

    db1.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// `durability` is live-safe via `set_durability_mode` only. Mutating
/// `cfg.durability` through `update_config` would persist a new string
/// to `strata.toml` without reconfiguring the WAL writer, the flush
/// thread, or the runtime signature — leaving `db.config()` disagreeing
/// with the live database. `update_config` must reject the mutation and
/// point the caller to the canonical path.
#[test]
#[serial(open_databases)]
fn test_update_config_rejects_durability_string_changes() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(StrataConfig {
                durability: "standard".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();

    // A Standard → Always string swap via `update_config` must be rejected.
    let err = db
        .update_config(|cfg| cfg.durability = "always".to_string())
        .expect_err("update_config must reject durability changes");
    assert!(
        matches!(err, StrataError::InvalidInput { .. }),
        "expected InvalidInput, got: {err:?}"
    );
    assert!(
        err.to_string().contains("set_durability_mode"),
        "error must point callers at the canonical path, got: {err}"
    );

    // A typo like `"turbo"` would previously have been silently persisted
    // and only tripped on later parsing. Rejecting any string change
    // catches it here too.
    assert!(
        db.update_config(|cfg| cfg.durability = "turbo".to_string())
            .is_err(),
        "unknown durability strings must also be rejected"
    );

    // Live and persisted state both still say "standard".
    assert_eq!(db.config().durability, "standard");

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// `set_durability_mode` is the canonical path for durability switches;
/// it must update `self.config.durability`, the runtime signature, and
/// persist `strata.toml` atomically, so a reopen reads the same value.
#[test]
#[serial(open_databases)]
fn test_set_durability_mode_persists_and_updates_config_string() {
    use strata_storage::durability::wal::DurabilityMode;

    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(StrataConfig {
                durability: "standard".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();

    db.set_durability_mode(DurabilityMode::Always)
        .expect("Standard → Always must succeed");

    // In-process state.
    assert_eq!(db.config().durability, "always");
    assert_eq!(
        db.runtime_signature()
            .expect("signature must exist")
            .durability_mode,
        DurabilityMode::Always,
    );

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();

    // Persisted state — a fresh process would see "always" here.
    let persisted = StrataConfig::from_file(&db_path.join("strata.toml"))
        .expect("strata.toml must exist after a successful durability switch");
    assert_eq!(
        persisted.durability, "always",
        "set_durability_mode must persist the string form to strata.toml"
    );
}

/// `Database::update_config` must reject mutations to open-time-only
/// fields. Without this gate, a direct Rust caller could bypass the
/// executor's `OPEN_TIME_ONLY_KEYS` check and silently drift the
/// persisted config away from the live runtime.
#[test]
#[serial(open_databases)]
fn test_update_config_rejects_open_time_only_field_changes() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(StrataConfig {
                durability: "cache".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();

    // Capture pre-change live values so we can confirm they are NOT
    // mutated even though the closure attempted the change.
    let (before_threads, before_lossy, before_missing_manifest, before_codec) = {
        let cfg = db.config();
        (
            cfg.storage.background_threads,
            cfg.allow_lossy_recovery,
            cfg.allow_missing_manifest,
            cfg.storage.codec.clone(),
        )
    };

    let err = db
        .update_config(|cfg| {
            cfg.storage.background_threads = before_threads.wrapping_add(4);
        })
        .expect_err("mutating background_threads via update_config must be rejected");
    assert!(
        matches!(err, StrataError::InvalidInput { .. }),
        "expected InvalidInput for open-time-only mutation, got: {err:?}"
    );
    assert!(
        err.to_string().contains("background_threads"),
        "error must name the offending field, got: {err}"
    );

    assert!(
        db.update_config(|cfg| cfg.allow_lossy_recovery = !before_lossy)
            .is_err(),
        "allow_lossy_recovery must also be rejected"
    );
    let err = db
        .update_config(|cfg| cfg.allow_missing_manifest = !before_missing_manifest)
        .expect_err("allow_missing_manifest must also be rejected");
    assert!(
        err.to_string().contains("allow_missing_manifest"),
        "error must name the offending field, got: {err}"
    );
    assert!(
        db.update_config(|cfg| cfg.storage.codec = "aes-gcm-256".to_string())
            .is_err(),
        "storage.codec must also be rejected"
    );

    // Live config unchanged.
    let after = db.config();
    assert_eq!(after.storage.background_threads, before_threads);
    assert_eq!(after.allow_lossy_recovery, before_lossy);
    assert_eq!(after.allow_missing_manifest, before_missing_manifest);
    assert_eq!(after.storage.codec, before_codec);

    // A live-safe mutation still works.
    db.update_config(|cfg| cfg.storage.max_branches = 512)
        .expect("live-safe mutation must succeed");
    assert_eq!(db.config().storage.max_branches, 512);

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

// ============================================================================
// T3-E12 Phase 2 — AES-GCM + WAL-durability round-trip contract
// ============================================================================
//
// These six tests pin the core Phase 2 acceptance: a database configured
// with `codec = "aes-gcm-256"` + `durability = "standard"` (WAL-backed)
// round-trips cleanly through the v3 outer envelope and the codec-threaded
// WAL reader on every path the tracking doc enumerates —
// docs/design/execution/t3-e12-phases.md §"Phase 2 Tests".

/// Build a `StrataConfig` with aes-gcm-256 codec + standard durability.
/// Shared setup for the T3-E12 round-trip tests.
fn aes_gcm_standard_config() -> StrataConfig {
    StrataConfig {
        durability: "standard".to_string(),
        storage: StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    }
}

/// T3-E12 §Phase 2 Test #1: clean-shutdown round-trip.
///
/// Write several records through an `aes-gcm-256 + standard` primary,
/// call `shutdown()`, reopen with the same key + codec, verify every
/// record is visible. Exercises the happy path for the v3 envelope
/// writer + codec-aware reader.
#[test]
#[serial(open_databases)]
fn test_aes_gcm_wal_durability_clean_shutdown_roundtrip() {
    OPEN_DATABASES.lock().clear();
    let _key_guard = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    {
        let db = Database::open_runtime(
            super::spec::OpenSpec::primary(&db_path)
                .with_config(aes_gcm_standard_config())
                .with_subsystem(SearchSubsystem),
        )
        .expect("aes-gcm-256 + standard durability must open post-T3-E12");

        for i in 0..12u8 {
            blind_write(
                &db,
                Key::new_kv(ns.clone(), format!("k{i}")),
                Value::Bytes(vec![i; 64]),
            );
        }
        db.shutdown().expect("clean shutdown must succeed");
    }
    OPEN_DATABASES.lock().clear();

    // Reopen with the same key + codec and assert every record is visible.
    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(aes_gcm_standard_config())
            .with_subsystem(SearchSubsystem),
    )
    .expect("reopen after clean shutdown must succeed");

    for i in 0..12u8 {
        let key = Key::new_kv(ns.clone(), format!("k{i}"));
        let got = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .expect("get must succeed on reopened encrypted DB");
        match got.map(|v| v.value) {
            Some(Value::Bytes(bytes)) => assert_eq!(bytes, vec![i; 64]),
            other => panic!("key k{i} missing after reopen: {other:?}"),
        }
    }

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// T3-E12 §Phase 2 Test #2: crash recovery (drop without shutdown).
///
/// Mirrors the `recovery_tests::test_kv_survives_recovery` idiom for
/// the encrypted-WAL path: drop the DB without calling `shutdown()`,
/// reopen, verify records survived the partial-write recovery path.
#[test]
#[serial(open_databases)]
fn test_aes_gcm_wal_crash_recovery() {
    OPEN_DATABASES.lock().clear();
    let _key_guard = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    {
        let db = Database::open_runtime(
            super::spec::OpenSpec::primary(&db_path)
                .with_config(aes_gcm_standard_config())
                .with_subsystem(SearchSubsystem),
        )
        .unwrap();

        for i in 0..8u8 {
            blind_write(
                &db,
                Key::new_kv(ns.clone(), format!("crash_k{i}")),
                Value::Bytes(vec![i ^ 0x55; 32]),
            );
        }
        // Force a WAL flush but DO NOT call shutdown — mirrors a crash.
        db.flush().unwrap();
        drop(db);
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(aes_gcm_standard_config())
            .with_subsystem(SearchSubsystem),
    )
    .expect("reopen after crash must succeed via WAL recovery");

    for i in 0..8u8 {
        let key = Key::new_kv(ns.clone(), format!("crash_k{i}"));
        let got = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap();
        match got.map(|v| v.value) {
            Some(Value::Bytes(bytes)) => assert_eq!(bytes, vec![i ^ 0x55; 32]),
            other => panic!("key crash_k{i} missing after crash recovery: {other:?}"),
        }
    }

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// T3-E12 §Phase 2 Test #3: large-record envelope.
///
/// Exercises the `u32 outer_len` field against a multi-MB value. A
/// ~2 MB payload is large enough to stress the envelope but stays
/// below the default segment-size threshold so no rotation is forced
/// mid-record. The post-encode envelope size is comfortably within
/// u32 range.
#[test]
#[serial(open_databases)]
fn test_aes_gcm_large_record_envelope() {
    OPEN_DATABASES.lock().clear();
    let _key_guard = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // ~2 MB payload — big enough to exercise the u32 outer_len field
    // without blowing past the test-default segment size.
    let big_value: Vec<u8> = (0u32..2 * 1024 * 1024)
        .map(|i| (i as u8).wrapping_mul(37))
        .collect();

    {
        let db = Database::open_runtime(
            super::spec::OpenSpec::primary(&db_path)
                .with_config(aes_gcm_standard_config())
                .with_subsystem(SearchSubsystem),
        )
        .unwrap();
        blind_write(
            &db,
            Key::new_kv(ns.clone(), "big"),
            Value::Bytes(big_value.clone()),
        );
        db.shutdown().unwrap();
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(aes_gcm_standard_config())
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let got = db
        .storage()
        .get_versioned(&Key::new_kv(ns, "big"), CommitVersion::MAX)
        .unwrap()
        .expect("large record must round-trip through the v3 envelope");
    match got.value {
        Value::Bytes(bytes) => assert_eq!(
            bytes.len(),
            big_value.len(),
            "recovered size must match written size"
        ),
        other => panic!("wrong value type: {other:?}"),
    }

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// T3-E12 §Phase 2 Test #6: wrong-key reopen classifies as
/// `LossyErrorKind::CodecDecode`.
///
/// Write with key A, close, then reopen with key B + `allow_lossy_recovery=true`.
/// The open must succeed with empty state, and
/// `last_lossy_recovery_report()` must report
/// `error_kind = LossyErrorKind::CodecDecode` — the typed
/// classification that separates wrong-key scenarios from
/// raw-byte-corruption scenarios (§D5).
#[test]
#[serial(open_databases)]
fn test_aes_gcm_wrong_key_classifies_as_codec_decode() {
    OPEN_DATABASES.lock().clear();
    // Write with key A.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    {
        let _key_a = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);
        let db = Database::open_runtime(
            super::spec::OpenSpec::primary(&db_path)
                .with_config(aes_gcm_standard_config())
                .with_subsystem(SearchSubsystem),
        )
        .unwrap();
        for i in 0..4u8 {
            blind_write(
                &db,
                Key::new_kv(ns.clone(), format!("enc_k{i}")),
                Value::Bytes(vec![i; 16]),
            );
        }
        db.shutdown().unwrap();
    }
    OPEN_DATABASES.lock().clear();

    // Reopen with key B + lossy. Must succeed with empty state +
    // typed CodecDecode classification in the report.
    const TEST_AES_KEY_B: &str = "ffeeddccbbaa99887766554433221100f0e0d0c0b0a090807060504030201000";
    let _key_b = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY_B);
    let cfg_lossy = StrataConfig {
        allow_lossy_recovery: true,
        ..aes_gcm_standard_config()
    };
    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(cfg_lossy)
            .with_subsystem(SearchSubsystem),
    )
    .expect("wrong-key reopen under allow_lossy_recovery=true must succeed (whole-DB wipe)");

    let report = db
        .last_lossy_recovery_report()
        .expect("wrong-key reopen must populate a lossy report");
    use crate::LossyErrorKind;
    assert_eq!(
        report.error_kind,
        LossyErrorKind::CodecDecode,
        "wrong key → CodecDecode (not Storage, not Corruption); got {:?} with error {:?}",
        report.error_kind,
        report.error,
    );
    assert!(report.discarded_on_wipe);

    // The pre-wipe records are gone by contract (whole-DB wipe).
    for i in 0..4u8 {
        let got = db
            .storage()
            .get_versioned(
                &Key::new_kv(ns.clone(), format!("enc_k{i}")),
                CommitVersion::MAX,
            )
            .unwrap();
        assert!(
            got.is_none(),
            "record enc_k{i} must be wiped after lossy fallback; found {got:?}"
        );
    }

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// T3-E12 §Phase 2 Test #4: partial-tail truncation on encrypted WAL.
///
/// Write N records, then append a deterministic mid-outer-envelope
/// byte run (3 bytes — less than the 8-byte envelope-header size).
/// Reopen, assert the first N records survive. Pins the
/// `InsufficientData → ReadStopReason::PartialRecord` branch of the
/// envelope-parse loop, which is what differentiates genuine crash
/// tails (truncate-and-continue) from CRC or codec failures (error).
///
/// Appending arbitrary garbage (the pre-§D4-rewrite fixture pattern)
/// is non-deterministic — random bytes can happen to produce a valid
/// outer_len_crc by luck (1 in 4 billion) or fail CRC validation
/// (usual case), which routes through a different error branch.
/// Truncation mid-envelope-header guarantees the short-read path is
/// the one that fires.
#[test]
#[serial(open_databases)]
fn test_aes_gcm_partial_tail_truncates_cleanly() {
    OPEN_DATABASES.lock().clear();
    let _key_guard = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let record_count = 5u8;
    let size_after_last_record: u64 = {
        let db = Database::open_runtime(
            super::spec::OpenSpec::primary(&db_path)
                .with_config(aes_gcm_standard_config())
                .with_subsystem(SearchSubsystem),
        )
        .unwrap();
        for i in 0..record_count {
            blind_write(
                &db,
                Key::new_kv(ns.clone(), format!("tail_k{i}")),
                Value::Bytes(vec![i; 16]),
            );
        }
        db.flush().unwrap();

        let seg_path = db_path.join("wal").join("wal-000001.seg");
        let size = std::fs::metadata(&seg_path).unwrap().len();
        db.shutdown().unwrap();
        size
    };
    OPEN_DATABASES.lock().clear();

    // Append 3 bytes — mid-outer-envelope-header for record N+1.
    // `read_outer_envelope` returns `Ok(None)` for any buf.len() < 8,
    // which the parse loop treats as `PartialRecord` → truncate.
    let seg_path = db_path.join("wal").join("wal-000001.seg");
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap();
        f.write_all(&[0xAB, 0xCD, 0xEF]).unwrap();
    }
    assert_eq!(
        std::fs::metadata(&seg_path).unwrap().len(),
        size_after_last_record + 3,
    );

    let db = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(aes_gcm_standard_config())
            .with_subsystem(SearchSubsystem),
    )
    .expect("reopen with mid-envelope partial tail must succeed (PartialRecord, not corruption)");

    for i in 0..record_count {
        let got = db
            .storage()
            .get_versioned(
                &Key::new_kv(ns.clone(), format!("tail_k{i}")),
                CommitVersion::MAX,
            )
            .unwrap();
        match got.map(|v| v.value) {
            Some(Value::Bytes(bytes)) => assert_eq!(bytes, vec![i; 16]),
            other => panic!("record tail_k{i} missing after partial-tail: {other:?}"),
        }
    }

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

/// T3-E12 §Phase 2 Test #5: mid-segment ciphertext corruption must
/// NOT silently truncate in strict mode.
///
/// Write N records, flip a byte deep inside the first record's
/// codec-encoded payload (past the outer envelope header). AES-GCM's
/// auth tag catches the tampered ciphertext on decode; the reader
/// returns `Err(WalReaderError::CodecDecode)` which the coordinator
/// maps to `StrataError::CodecDecode` and strict open refuses.
///
/// The key regression guard: codec decode failures are ALWAYS `Err`
/// at the reader layer, never `Ok(stop_reason)` (D5). A silent
/// truncation would be catastrophic — wrong-key scenarios would
/// serve partial data without any signal to the operator.
#[test]
#[serial(open_databases)]
fn test_aes_gcm_mid_segment_corruption_not_silently_truncated() {
    OPEN_DATABASES.lock().clear();
    let _key_guard = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    {
        let db = Database::open_runtime(
            super::spec::OpenSpec::primary(&db_path)
                .with_config(aes_gcm_standard_config())
                .with_subsystem(SearchSubsystem),
        )
        .unwrap();
        for i in 0..4u8 {
            blind_write(
                &db,
                Key::new_kv(ns.clone(), format!("mid_k{i}")),
                Value::Bytes(vec![i ^ 0x77; 32]),
            );
        }
        db.shutdown().unwrap();
    }
    OPEN_DATABASES.lock().clear();

    // Flip a byte deep inside the first record's ciphertext. The
    // segment header is 36 bytes; add 8 for the first outer envelope;
    // byte 50 lands comfortably inside the first encrypted payload.
    let seg_path = db_path.join("wal").join("wal-000001.seg");
    {
        let mut data = std::fs::read(&seg_path).unwrap();
        let idx = 50usize;
        assert!(
            idx < data.len(),
            "segment file should have grown past offset 50"
        );
        data[idx] ^= 0xFF;
        std::fs::write(&seg_path, &data).unwrap();
    }

    // Strict open must refuse; no silent truncation path may succeed.
    let result = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(aes_gcm_standard_config())
            .with_subsystem(SearchSubsystem),
    );
    match result {
        Err(_) => { /* strict refuses; expected */ }
        Ok(db) => {
            let mut visible = 0usize;
            for i in 0..4u8 {
                let got = db
                    .storage()
                    .get_versioned(
                        &Key::new_kv(ns.clone(), format!("mid_k{i}")),
                        CommitVersion::MAX,
                    )
                    .unwrap();
                if got.is_some() {
                    visible += 1;
                }
            }
            panic!(
                "strict open with mid-segment ciphertext corruption must \
                 error, not silently truncate; {visible}/4 records \
                 survived — this is a D5 regression"
            );
        }
    }
    OPEN_DATABASES.lock().clear();
}
