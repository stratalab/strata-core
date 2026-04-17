use super::*;

#[test]
fn test_checkpoint_ephemeral_noop() {
    let db = Database::cache().unwrap();
    // checkpoint on ephemeral database is a no-op
    assert!(db.checkpoint().is_ok());
}

#[test]
fn test_compact_ephemeral_noop() {
    let db = Database::cache().unwrap();
    // compact on ephemeral database is a no-op
    assert!(db.compact().is_ok());
}

#[test]
fn test_compact_without_checkpoint_fails() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    // Compact before any checkpoint should fail (no snapshot watermark)
    let result = db.compact();
    assert!(result.is_err());
}

#[test]
fn test_checkpoint_creates_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "checkpoint_test");

    // Write some data
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String("hello".to_string()))?;
        Ok(())
    })
    .unwrap();

    // Checkpoint should succeed
    assert!(db.checkpoint().is_ok());

    // Snapshots directory should exist with files
    let snapshots_dir = db_path.canonicalize().unwrap().join("snapshots");
    assert!(snapshots_dir.exists());

    // MANIFEST should exist
    let manifest_path = db_path.canonicalize().unwrap().join("MANIFEST");
    assert!(manifest_path.exists());
}

#[test]
fn test_checkpoint_then_compact_without_flush_succeeds() {
    // Inverted in the T3-E5 follow-up (retention-complete DTOs + install).
    //
    // Pre-retention, compact() refused on snapshot-only coverage because
    // recovery was either WAL-only or lacked tombstone/TTL/Branch fidelity.
    // Now that snapshot install is retention-complete, the snapshot
    // watermark is an authoritative recovery input and compact() succeeds
    // without a flush watermark.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "compact_test");

    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();

    db.checkpoint()
        .expect("checkpoint must succeed to populate the snapshot watermark");

    // Compact now succeeds on snapshot-only coverage.
    db.compact()
        .expect("compact() must succeed once recovery consumes retention-complete snapshots");
}

#[test]
fn test_gc_safe_point_no_active_txns() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // After open, system branch init may have advanced the version.
    // gc_safe_point = max(0, current_version - 1)
    let initial_sp = db.gc_safe_point();
    assert!(
        initial_sp <= 3,
        "gc_safe_point should be small after init, got {}",
        initial_sp
    );

    // Commit two transactions to advance version past 1
    let key = Key::new_kv(ns.clone(), "gc_key");
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    // Now current_version >= 2, no active txns → safe_point = current - 1
    let sp = db.gc_safe_point();
    assert!(sp >= 1, "safe point should be at least 1, got {}", sp);
}

#[test]
fn test_gc_safe_point_with_active_txn() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Commit a few transactions to advance version well past 1
    for i in 0..5 {
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i))?;
            Ok(())
        })
        .unwrap();
    }

    // Start a transaction but don't commit it yet — pins version
    let mut txn = db.begin_transaction(branch_id).unwrap();
    let txn_start_version = txn.start_version;

    // Commit two more transactions to advance version further
    for i in 5..7 {
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        db.transaction(branch_id, |t| {
            t.put(key.clone(), Value::Int(i))?;
            Ok(())
        })
        .unwrap();
    }

    // GC safe point should be min(current - 1, min_active_version)
    // The active txn pins the safe point at its start version
    let sp = db.gc_safe_point();
    assert!(
        sp <= txn_start_version.as_u64(),
        "safe point {} should be <= active txn start version {}",
        sp,
        txn_start_version.as_u64()
    );

    // Abort the active transaction
    txn.abort();

    // Now safe point should advance to current - 1 since no active txns
    let sp_after = db.gc_safe_point();
    assert!(
        sp_after > sp,
        "safe point should advance after abort: {} > {}",
        sp_after,
        sp
    );
}

#[test]
fn test_run_gc_prunes_old_versions() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "gc_prune");

    // Write v1
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();

    // Overwrite with v2
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    // Overwrite with v3
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(3))?;
        Ok(())
    })
    .unwrap();

    // Run GC — SegmentedStore prunes via compaction, not gc_branch(),
    // so pruned count is 0.  Verify the API doesn't error.
    let (safe_point, _pruned) = db.run_gc();
    assert!(safe_point > 0, "safe point should be non-zero");

    // Latest value should still be readable
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(stored.value, Value::Int(3));
}

#[test]
fn test_run_gc_no_prune_at_version_zero() {
    let db = Database::cache().unwrap();

    // After init_system_branch, version has advanced; no user data to prune
    let (safe_point, pruned) = db.run_gc();
    // safe_point may be > 0 due to system branch init transactions
    assert!(
        safe_point <= 4,
        "safe_point should be small, got {}",
        safe_point
    );
    assert_eq!(pruned, 0);
}

#[test]
fn test_run_maintenance_end_to_end() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Write and overwrite a key to create GC-able versions
    let key = Key::new_kv(ns, "maint_key");
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();

    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    // Run full maintenance cycle
    let (safe_point, pruned, expired) = db.run_maintenance();
    assert!(safe_point > 0, "safe point should be non-zero");
    // pruned may or may not be > 0 depending on version chain gc logic
    // expired should be 0 since we didn't use TTL
    assert_eq!(expired, 0, "no TTL entries to expire");

    // Data should still be readable
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(stored.value, Value::Int(2));

    let _ = pruned; // suppress unused warning
}
// ============================================================================
// T3-E5 Chunk 2: checkpoint-only restart via snapshot load + install
// ============================================================================

/// Write KV entries, checkpoint, drop the database, manually delete the WAL
/// segments (simulating what post-Chunk-4 `compact()` will do), reopen, and
/// verify all data is recovered from the snapshot alone.
///
/// This is the core Epic 5 correctness gate: checkpoints must be a complete
/// recovery artifact, not just a future optimization. Until Chunk 2 wired
/// snapshot loading into the coordinator and engine install decoder, this
/// scenario was impossible — checkpoints were written but never consumed.
///
/// Tranche 4 branch-aware retention will depend on this path being correct
/// for tombstone and TTL state as well; those cases are validated in
/// Chunk 3 once the checkpoint payload carries that metadata.
#[test]
#[serial(open_databases)]
fn test_checkpoint_only_restart_recovers_kv_from_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Step 1: write several KV entries on the user branch.
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        for i in 0u64..10 {
            let key = Key::new_kv(ns.clone(), format!("kv-{}", i));
            let value = Value::String(format!("payload-{}", i));
            db.transaction(branch_id, |txn| {
                txn.put(key, value.clone())?;
                Ok(())
            })
            .unwrap();
        }

        // Step 2: checkpoint. This flushes the WAL, serializes the KV
        // section to a `snap-NNNNNN.chk` file, and updates the MANIFEST
        // with the new snapshot id and watermark.
        db.checkpoint().unwrap();
    }
    // Step 3: drop DB. Clear registry so reopen produces a fresh instance.
    OPEN_DATABASES.lock().clear();

    // Step 4: simulate post-Chunk-4 compact() by manually deleting every WAL
    // segment. The MANIFEST still points at the snapshot; recovery must fall
    // back to snapshot load and surface all 10 entries without WAL replay.
    let wal_dir = db_path.join("wal");
    for entry in std::fs::read_dir(&wal_dir).unwrap().flatten() {
        let p = entry.path();
        if p.is_file() {
            std::fs::remove_file(&p).unwrap();
        }
    }
    // Confirm the WAL dir is now empty so the test is actually exercising
    // snapshot-only recovery rather than accidentally still hitting WAL.
    let remaining: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().is_file())
        .collect();
    assert!(
        remaining.is_empty(),
        "WAL dir should be empty before reopen"
    );

    // Step 5: reopen. Recovery must install the snapshot.
    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

    // Step 6: every committed key must be visible with its original value.
    for i in 0u64..10 {
        let key = Key::new_kv(ns.clone(), format!("kv-{}", i));
        let stored = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("kv-{} must survive snapshot-only restart", i));
        assert_eq!(stored.value, Value::String(format!("payload-{}", i)));
    }
}

/// Checkpoint + delta-WAL replay: keys written before the checkpoint arrive
/// through snapshot install; keys written after the checkpoint arrive through
/// WAL replay filtered by the snapshot watermark. Both groups must be visible
/// after reopen, and the snapshot-covered records must not be double-applied.
#[test]
#[serial(open_databases)]
fn test_checkpoint_plus_delta_wal_replay_merges_sources() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Phase 1: write 5 "pre-checkpoint" keys, checkpoint, then write 5
    // "post-checkpoint" keys that only live in the WAL tail.
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        for i in 0u64..5 {
            let key = Key::new_kv(ns.clone(), format!("pre-{}", i));
            db.transaction(branch_id, |txn| {
                txn.put(key, Value::String(format!("v{}", i)))?;
                Ok(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap();

        for i in 0u64..5 {
            let key = Key::new_kv(ns.clone(), format!("post-{}", i));
            db.transaction(branch_id, |txn| {
                txn.put(key, Value::String(format!("w{}", i)))?;
                Ok(())
            })
            .unwrap();
        }
    }
    OPEN_DATABASES.lock().clear();

    // Phase 2: reopen. Both the snapshot-installed pre-* keys and the
    // delta-replayed post-* keys must be visible.
    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

    for i in 0u64..5 {
        let pre_key = Key::new_kv(ns.clone(), format!("pre-{}", i));
        let pre = db
            .storage()
            .get_versioned(&pre_key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("pre-{} must survive via snapshot install", i));
        assert_eq!(pre.value, Value::String(format!("v{}", i)));

        let post_key = Key::new_kv(ns.clone(), format!("post-{}", i));
        let post = db
            .storage()
            .get_versioned(&post_key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("post-{} must survive via delta-WAL replay", i));
        assert_eq!(post.value, Value::String(format!("w{}", i)));
    }

    // The post-reopen database must be writable at a commit version above
    // both sources — exercise it to surface monotonicity regressions.
    let followup_key = Key::new_kv(ns.clone(), "after-reopen");
    db.transaction(branch_id, |txn| {
        txn.put(followup_key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    let followup = db
        .storage()
        .get_versioned(&followup_key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(followup.value, Value::Int(1));
}

/// Codec mismatch at reopen must fail loud regardless of the
/// `allow_lossy_recovery` flag. Lossy recovery is for tolerating WAL
/// corruption, not for silently rewriting the database under a different
/// codec id. The check belongs ahead of the lossy fallback so an operator
/// cannot accidentally discard a database by misconfiguring the codec.
///
/// Because there is only one WAL-compatible codec in the workspace today
/// (`identity`), the test simulates codec drift by rewriting the MANIFEST
/// directly with a foreign codec id, then reopens with `identity`.
#[test]
#[serial(open_databases)]
fn test_codec_mismatch_on_reopen_fails_even_with_lossy_flag() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Create the DB with the default identity codec and write a key so there
    // is real state for lossy mode to potentially discard.
    let database_uuid;
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "k"), Value::Int(1))?;
            Ok(())
        })
        .unwrap();
        database_uuid = db.database_uuid();
    }
    OPEN_DATABASES.lock().clear();

    // Rewrite the MANIFEST to claim a different codec id without changing
    // any other on-disk state. This is the smallest faithful simulation of
    // codec drift we can produce with the currently-shipping codec set.
    let manifest_path = db_path.join("MANIFEST");
    std::fs::remove_file(&manifest_path).unwrap();
    strata_durability::ManifestManager::create(
        manifest_path,
        database_uuid,
        "some-other-codec".to_string(),
    )
    .unwrap();

    // Reopen with the default (identity) codec AND allow_lossy_recovery.
    // The lossy flag must not suppress the codec-mismatch error.
    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..Default::default()
    };
    let result = Database::open_with_config(&db_path, cfg);
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("codec mismatch"),
                "reopen with different codec must fail with codec mismatch, got: {}",
                msg
            );
            assert!(
                matches!(e, StrataError::IncompatibleReuse { .. }),
                "codec drift must surface as IncompatibleReuse (not internal/corruption), got: {:?}",
                e
            );
        }
        Ok(_) => panic!("reopen with wrong codec + lossy flag must still fail"),
    }
}

/// Follower open must fail loud when the MANIFEST cannot be parsed.
/// Pre-fix, this case silently degraded to WAL-only recovery, which is
/// unsafe once snapshot-aware compaction reclaims pre-snapshot WAL: the
/// follower would serve stale/empty state without operator visibility.
#[test]
#[serial(open_databases)]
fn test_follower_open_fails_hard_on_corrupt_manifest() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "k"), Value::Int(1))?;
            Ok(())
        })
        .unwrap();
    }
    OPEN_DATABASES.lock().clear();

    // Corrupt the MANIFEST on disk so ManifestManager::load fails.
    let manifest_path = db_path.join("MANIFEST");
    std::fs::write(&manifest_path, b"NOT-A-VALID-MANIFEST").unwrap();

    let err = match Database::open_follower(&db_path) {
        Ok(_) => panic!("follower must fail on corrupt MANIFEST"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("manifest"),
        "error should mention MANIFEST, got: {}",
        msg
    );
}

/// Follower open: after the primary checkpoints, a fresh follower must see
/// every committed value — including the ones that now live only in the
/// snapshot on disk — without going through the primary's in-process state.
/// This exercises the Chunk 3 follower migration to the direct callback API
/// plus snapshot install.
#[test]
fn test_follower_open_installs_checkpoint_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Primary writes then checkpoints then exits. The data that is now
    // covered by the snapshot is the follower's only path to see those
    // values after the WAL is manually truncated.
    {
        let primary = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        for i in 0u64..6 {
            let key = Key::new_kv(ns.clone(), format!("k{}", i));
            primary
                .transaction(branch_id, |txn| {
                    txn.put(key, Value::String(format!("v{}", i)))?;
                    Ok(())
                })
                .unwrap();
        }
        primary.checkpoint().unwrap();
    }
    OPEN_DATABASES.lock().clear();

    // Delete WAL so the follower can only see data through the snapshot.
    let wal_dir = db_path.join("wal");
    for entry in std::fs::read_dir(&wal_dir).unwrap().flatten() {
        let p = entry.path();
        if p.is_file() {
            std::fs::remove_file(&p).unwrap();
        }
    }

    // Open as follower and verify all pre-checkpoint data is visible.
    let follower = Database::open_follower(&db_path).unwrap();
    for i in 0u64..6 {
        let key = Key::new_kv(ns.clone(), format!("k{}", i));
        let stored = follower
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("follower must see k{} via snapshot install", i));
        assert_eq!(stored.value, Value::String(format!("v{}", i)));
    }
}
// ============================================================================
// T3-E5 follow-up: retention-complete checkpoint + compact + reopen
// ============================================================================

/// Tombstones survive checkpoint + compact + reopen.
///
/// Write K, delete K, checkpoint, compact (reclaims WAL), reopen — the
/// key must stay hidden from reads because the snapshot install
/// reconstructs the tombstone barrier. Before the follow-up, this was
/// the corruption mode: the WAL delete record vanished with the WAL
/// and the snapshot didn't carry tombstones, so K would reappear.
#[test]
#[serial(open_databases)]
fn test_checkpoint_compact_preserves_tombstones() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "deleted");

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(1))?;
            Ok(())
        })
        .unwrap();
        db.transaction(branch_id, |txn| {
            txn.delete(key.clone())?;
            Ok(())
        })
        .unwrap();
        db.checkpoint().unwrap();
        db.compact()
            .expect("compact must succeed on snapshot-only coverage");
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
    let observed = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(
        observed.is_none(),
        "deleted key must remain hidden after checkpoint+compact+reopen (tombstone survived)"
    );
}

/// TTL-bound entries still expire at the original deadline after
/// checkpoint + compact + reopen.
///
/// Today's engine has no public TTL-aware transaction API, so the TTL
/// key is injected directly via the recovery write path and the
/// coordinator version is advanced via a separate normal transaction
/// (so `checkpoint()` writes a non-zero watermark and `compact()` can
/// run). The critical invariants — `ttl_ms` round-trips through the
/// snapshot and `get_at_timestamp` still honors the original deadline —
/// are preserved regardless of which write path set the TTL.
#[test]
#[serial(open_databases)]
fn test_checkpoint_compact_preserves_ttl() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let ttl_key = Key::new_kv(ns.clone(), "expires");
    let ttl = Duration::from_millis(60_000);
    let version_for_ttl = CommitVersion(1_000);

    let commit_ts_micros: u64;
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

        // Bump coordinator via a normal transaction so checkpoint writes a
        // real watermark (quiesced_version > 0).
        let warmup_key = Key::new_kv(ns.clone(), "warmup");
        db.transaction(branch_id, |txn| {
            txn.put(warmup_key.clone(), Value::Int(1))?;
            Ok(())
        })
        .unwrap();

        // Inject the TTL-bound entry at a known commit version above the
        // coordinator's current floor.
        db.storage
            .put_with_version_mode(
                ttl_key.clone(),
                Value::String("temporary".into()),
                version_for_ttl,
                Some(ttl),
                WriteMode::Append,
            )
            .unwrap();
        commit_ts_micros = db
            .storage()
            .get_versioned(&ttl_key, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .timestamp
            .as_micros();

        db.checkpoint().unwrap();
        db.compact()
            .expect("compact must succeed on snapshot-only coverage");
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

    // Before expiry: visible.
    let pre = db
        .storage()
        .get_at_timestamp(&ttl_key, commit_ts_micros + ttl.as_micros() as u64 - 1)
        .unwrap();
    assert!(pre.is_some(), "entry must be visible before TTL expiry");

    // After expiry: gone.
    let post = db
        .storage()
        .get_at_timestamp(&ttl_key, commit_ts_micros + ttl.as_micros() as u64 + 1)
        .unwrap();
    assert!(
        post.is_none(),
        "TTL barrier must survive checkpoint+compact+reopen"
    );
}

/// Branch metadata survives checkpoint + compact + reopen.
///
/// Create a user branch, checkpoint, compact, reopen — `branches.exists`
/// must still return true. The Branch section install decoder routes
/// branch-metadata entries back into the global branch index under the
/// nil-UUID sentinel so the in-memory branch registry repopulates on
/// reopen from the snapshot alone.
#[test]
#[serial(open_databases)]
fn test_checkpoint_compact_preserves_branch_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_name = "retention-ctx";

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        db.branches().create(branch_name).unwrap();
        assert!(db.branches().exists(branch_name).unwrap());
        db.checkpoint().unwrap();
        db.compact()
            .expect("compact must succeed on snapshot-only coverage");
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
    assert!(
        db.branches().exists(branch_name).unwrap(),
        "branch metadata must survive checkpoint+compact+reopen via snapshot install"
    );
}
