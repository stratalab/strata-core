//! B5.2 — Adversarial retention / reclaim proof matrix.
//!
//! Covers the deterministic schedule families from
//! `docs/design/branching/branching-gc/b5-phasing-plan.md` §B5.2 +
//! `docs/design/branching/branching-gc/branching-b5-verification-spec.md`
//! §"Required schedule matrix". Each test proves one or more of:
//!
//! - A1 Reachability beats accelerators (manifest proof wins)
//! - A2 Quarantine is reversible until final purge
//! - A3 Same-name recreate does not alias retention
//! - A4 Materialization changes ownership, not meaning
//! - A5 Degraded recovery blocks reclaim
//! - A6 Reopen re-establishes safe runtime state from manifests
//!
//! The crash/failpoint boundary suite lives in
//! `branching_gc_quarantine_recovery.rs`. The generated-history
//! state-machine suite is not on the B5.2 merge-critical path (phasing
//! plan §B5.2 "deterministic + failpoint proof suites are the minimum
//! bar; state-machine suite lands with B5.3 or later").

#![cfg(not(miri))]

use crate::common::branching::archive_branch_for_test;
use crate::common::*;
use std::sync::Arc;
use strata_core::branch::BranchLifecycleStatus;
use strata_core::value::Value;
use strata_core::{BranchId, Key, Namespace};
use strata_engine::{ReclaimStatus, RetentionBlocker};

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

fn seed(db: &Arc<Database>, name: &str, key: &str, v: i64) {
    KVStore::new(db.clone())
        .put(&resolve(name), "default", key, Value::Int(v))
        .expect("seed write succeeds");
}

fn seed_with_ttl(db: &Arc<Database>, name: &str, key: &str, v: i64, ttl_ms: u64) {
    let branch_id = resolve(name);
    let storage_key = Key::new_kv(
        Arc::new(Namespace::new(branch_id, "default".to_string())),
        key,
    );
    db.transaction(branch_id, |txn| {
        txn.put_with_ttl(storage_key.clone(), Value::Int(v), ttl_ms)
    })
    .expect("ttl write succeeds");
}

fn flush_branch(db: &Arc<Database>, name: &str) {
    let id = resolve(name);
    db.storage().rotate_memtable(&id);
    db.storage()
        .flush_oldest_frozen(&id)
        .expect("flush succeeds");
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

// =============================================================================
// Schedule: delete before fork
//
// Writing to a branch then deleting it — with no descendant ever forked —
// must reclaim fully through the quarantine protocol and leave no bytes
// behind. Pins: A1 reachability + A2 reversibility observed on a simple
// path.
// =============================================================================

#[test]
fn delete_before_fork_reclaims_every_segment() {
    let test_db = TestDb::new();
    test_db.db.branches().create("solo").unwrap();
    for i in 0..20 {
        seed(&test_db.db, "solo", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "solo");
    let solo_dir = branch_dir(&test_db.db, "solo");
    assert!(
        !list_ssts(&solo_dir).is_empty(),
        "setup must produce at least one sst"
    );

    test_db.db.branches().delete("solo").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("healthy recovery; gc succeeds");

    let remaining = list_ssts(&solo_dir);
    assert!(
        remaining.is_empty(),
        "after delete + gc, no bytes should remain at {:?}",
        remaining,
    );
}

// =============================================================================
// Schedule: delete after fork — parent delete with a surviving descendant
//
// Fork a child, delete the parent: the child must still read its
// fork-frontier snapshot and the parent-owned segments must remain live
// until the child releases them. Pins A1 (manifest proof over
// accelerator), KD4 (parent delete non-blocking on descendant
// materialization), Invariant 7 (same-name recreate later won't confuse
// this state).
// =============================================================================

#[test]
fn delete_after_fork_retains_parent_segments_for_child() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..30 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();

    let parent_dir = branch_dir(&test_db.db, "main");
    let pre_fork_ssts = list_ssts(&parent_dir);
    assert!(
        !pre_fork_ssts.is_empty(),
        "setup must produce parent segments before fork"
    );

    test_db.db.branches().delete("main").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("healthy; gc succeeds");

    for path in &pre_fork_ssts {
        assert!(
            path.exists(),
            "parent-owned segment {:?} must survive parent delete while child holds the fork frontier",
            path,
        );
    }

    // Child still reads the inherited snapshot.
    let kv = KVStore::new(test_db.db.clone());
    assert_eq!(
        kv.get(&resolve("feature"), "default", "k0").unwrap(),
        Some(Value::Int(0)),
        "child inherited-layer visibility must survive parent delete",
    );
    assert_eq!(
        kv.get(&resolve("feature"), "default", "k29").unwrap(),
        Some(Value::Int(29)),
    );
}

#[test]
fn delete_after_fork_reopen_does_not_resurrect_deleted_parent_storage() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..20 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();

    test_db.db.branches().delete("main").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("healthy; gc succeeds");

    test_db.reopen();

    let kv = KVStore::new(test_db.db.clone());
    assert_eq!(
        kv.get(&resolve("main"), "default", "k0").unwrap(),
        None,
        "deleted parent storage must not resurrect on reopen",
    );
    assert_eq!(
        kv.get(&resolve("feature"), "default", "k0").unwrap(),
        Some(Value::Int(0)),
        "descendant inherited-layer visibility must survive reopen",
    );
}

// =============================================================================
// Schedule: repeated parent compaction with child alive
//
// Fork a child, compact the parent multiple times. The child must keep
// reading its inherited-layer snapshot; compacted parent output does not
// free child-needed files. Pins A1 + KD6 (compaction rewrites one
// branch's manifest only).
// =============================================================================

#[test]
fn repeated_parent_compaction_with_child_alive_retains_inherited_segments() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..20 {
        seed(&test_db.db, "main", &format!("a{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    for i in 0..20 {
        seed(&test_db.db, "main", &format!("b{i}"), i + 100);
    }
    flush_branch(&test_db.db, "main");

    test_db.db.branches().fork("main", "feature").unwrap();
    let parent_dir = branch_dir(&test_db.db, "main");
    let inherited_ssts = list_ssts(&parent_dir);
    assert!(
        inherited_ssts.len() >= 2,
        "setup must produce at least two parent segments so compaction actually merges"
    );

    let parent_id = resolve("main");
    for _ in 0..3 {
        test_db
            .db
            .storage()
            .compact_branch(&parent_id, strata_core::id::CommitVersion(0))
            .expect("compact_branch succeeds");
    }

    for path in &inherited_ssts {
        assert!(
            path.exists(),
            "inherited-layer segment {:?} must survive repeated parent compaction",
            path,
        );
    }

    let kv = KVStore::new(test_db.db.clone());
    for i in 0..20 {
        let key = format!("a{i}");
        assert_eq!(
            kv.get(&resolve("feature"), "default", &key).unwrap(),
            Some(Value::Int(i)),
            "child must still read key {key} through inherited layer",
        );
    }
}

// =============================================================================
// Schedule: materialize before reopen (ownership-rewrite only, visibility preserved)
//
// Pins A4. Materialization must not change result set, tombstone semantics,
// TTL, or fork-frontier visibility; it only rewrites manifest ownership.
// =============================================================================

#[test]
fn materialize_before_reopen_preserves_visible_result_set() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..40 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();

    let kv = KVStore::new(test_db.db.clone());
    let before: Vec<Option<Value>> = (0..40)
        .map(|i| {
            kv.get(&resolve("feature"), "default", &format!("k{i}"))
                .unwrap()
        })
        .collect();

    // Layer 0 is the nearest parent (see `SegmentedStore::materialize_layer`).
    test_db
        .db
        .storage()
        .materialize_layer(&resolve("feature"), 0)
        .expect("materialize succeeds");

    let after: Vec<Option<Value>> = (0..40)
        .map(|i| {
            kv.get(&resolve("feature"), "default", &format!("k{i}"))
                .unwrap()
        })
        .collect();

    assert_eq!(
        before, after,
        "materialization must not change the child's visible result set",
    );
}

// =============================================================================
// Schedule: materialize after reopen (reopen-heal correctness)
//
// Fork, reopen, materialize. Same visibility as materialize-before-reopen —
// rebuild re-establishes safe runtime state from manifests (A6).
// =============================================================================

#[test]
fn materialize_after_reopen_preserves_visible_result_set() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..40 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();

    let expected: Vec<Option<Value>> = (0..40)
        .map(|i| {
            KVStore::new(test_db.db.clone())
                .get(&resolve("feature"), "default", &format!("k{i}"))
                .unwrap()
        })
        .collect();

    test_db.reopen();

    test_db
        .db
        .storage()
        .materialize_layer(&resolve("feature"), 0)
        .expect("materialize after reopen succeeds");

    let actual: Vec<Option<Value>> = (0..40)
        .map(|i| {
            KVStore::new(test_db.db.clone())
                .get(&resolve("feature"), "default", &format!("k{i}"))
                .unwrap()
        })
        .collect();

    assert_eq!(
        expected, actual,
        "materialize after reopen must preserve child visibility",
    );
}

// =============================================================================
// Schedule: TTL across materialize + reopen
//
// A TTL-bearing inherited value must keep its expiry barrier after
// materialization rewrites ownership and a subsequent reopen rebuilds the
// runtime state. Pins A4 + A6.
// =============================================================================

#[test]
fn ttl_across_materialize_and_reopen_preserves_expiry_barrier() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();

    seed(&test_db.db, "main", "ttl_key", 1);
    seed_with_ttl(&test_db.db, "main", "ttl_key", 2, 60_000);
    flush_branch(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();

    let latest = KVStore::new(test_db.db.clone())
        .get_versioned_direct(&resolve("main"), "default", "ttl_key")
        .unwrap()
        .expect("latest ttl-bearing version exists");
    let expiry_before = latest.timestamp.as_micros() + 60_000 * 1_000 - 1;
    let expiry_after = latest.timestamp.as_micros() + 60_000 * 1_000 + 1;

    test_db
        .db
        .storage()
        .materialize_layer(&resolve("feature"), 0)
        .expect("materialize succeeds");
    test_db.reopen();

    let kv = KVStore::new(test_db.db.clone());
    assert_eq!(
        kv.get_at(&resolve("feature"), "default", "ttl_key", expiry_before)
            .unwrap(),
        Some(Value::Int(2)),
        "materialize + reopen must preserve the TTL-bearing value before expiry",
    );
    assert_eq!(
        kv.get_at(&resolve("feature"), "default", "ttl_key", expiry_after)
            .unwrap(),
        None,
        "after TTL expiry, the expired parent version must continue to shadow older history through the materialized child state",
    );
}

// =============================================================================
// Schedule: same-name recreate with old descendants alive
//
// main@gen0 → fork(feature) → delete(main) → create(main)@gen1.
// The old feature lifecycle must continue reading the old main's snapshot
// via its inherited-layer manifest (segment-ID-keyed, not name-keyed).
// The new main@gen1 must not reclaim those segments. Pins A3 + KD1.
// =============================================================================

#[test]
fn same_name_recreate_does_not_reclaim_descendant_inherited_segments() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..25 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");

    test_db.db.branches().fork("main", "feature").unwrap();
    let parent_dir = branch_dir(&test_db.db, "main");
    let gen0_ssts = list_ssts(&parent_dir);
    assert!(!gen0_ssts.is_empty());

    test_db.db.branches().delete("main").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("gc succeeds");
    for path in &gen0_ssts {
        assert!(
            path.exists(),
            "gen0 segment {:?} must survive main delete (descendant holds)",
            path,
        );
    }

    test_db.db.branches().create("main").unwrap();
    for i in 0..10 {
        seed(&test_db.db, "main", &format!("new{i}"), i + 1000);
    }
    flush_branch(&test_db.db, "main");

    // GC again after the recreate — the gen0 segments must still be live
    // because the feature branch's inherited-layer manifest still points
    // at them (by segment_id, not branch name).
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("gc after recreate");

    for path in &gen0_ssts {
        assert!(
            path.exists(),
            "gen0 segment {:?} must still exist after main@gen1 recreate",
            path,
        );
    }

    let kv = KVStore::new(test_db.db.clone());
    assert_eq!(
        kv.get(&resolve("feature"), "default", "k0").unwrap(),
        Some(Value::Int(0)),
        "feature's gen0 inherited-layer visibility must survive recreate",
    );
    // New main@gen1 sees its own writes, not the old gen0 data.
    assert_eq!(
        kv.get(&resolve("main"), "default", "k0").unwrap(),
        None,
        "main@gen1 must not inherit gen0's keys",
    );
    assert_eq!(
        kv.get(&resolve("main"), "default", "new0").unwrap(),
        Some(Value::Int(1000)),
    );
}

// =============================================================================
// Schedule: delete + recreate + compaction
//
// Same-name recreate remains safe even after the recreated lifecycle publishes
// new compaction output and GC runs again. Old descendant-held bytes must stay
// live because proof is segment-ID keyed, not current-name keyed.
// =============================================================================

#[test]
fn delete_recreate_then_compact_does_not_reclaim_descendant_gen0_segments() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..20 {
        seed(&test_db.db, "main", &format!("old{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();

    let parent_dir = branch_dir(&test_db.db, "main");
    let gen0_ssts = list_ssts(&parent_dir);
    assert!(
        !gen0_ssts.is_empty(),
        "setup must produce gen0 segments before delete",
    );

    test_db.db.branches().delete("main").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("gc after delete succeeds");

    test_db.db.branches().create("main").unwrap();
    for i in 0..10 {
        seed(&test_db.db, "main", &format!("new_a{i}"), i + 1000);
    }
    flush_branch(&test_db.db, "main");
    for i in 0..10 {
        seed(&test_db.db, "main", &format!("new_b{i}"), i + 2000);
    }
    flush_branch(&test_db.db, "main");
    test_db
        .db
        .storage()
        .compact_branch(&resolve("main"), strata_core::id::CommitVersion(0))
        .expect("compaction on recreated main succeeds");
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("gc after recreate + compaction succeeds");

    for path in &gen0_ssts {
        assert!(
            path.exists(),
            "gen0 segment {:?} must survive recreated-main compaction while feature still holds the old snapshot",
            path,
        );
    }

    let kv = KVStore::new(test_db.db.clone());
    assert_eq!(
        kv.get(&resolve("feature"), "default", "old0").unwrap(),
        Some(Value::Int(0)),
        "feature must still read gen0 inherited data after recreate + compaction",
    );
    assert_eq!(
        kv.get(&resolve("main"), "default", "old0").unwrap(),
        None,
        "recreated main must not regain gen0 visibility after compaction",
    );
    assert_eq!(
        kv.get(&resolve("main"), "default", "new_a0").unwrap(),
        Some(Value::Int(1000)),
    );
    assert_eq!(
        kv.get(&resolve("main"), "default", "new_b0").unwrap(),
        Some(Value::Int(2000)),
    );
}

// =============================================================================
// Schedule: parent delete with multiple descendants
//
// One parent, multiple children at different retention states; parent
// delete must be safe without forcing descendant materialization. Pins
// KD4.
// =============================================================================

#[test]
fn parent_delete_with_multiple_descendants_does_not_force_materialization() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..30 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");

    test_db.db.branches().fork("main", "child_a").unwrap();
    test_db.db.branches().fork("main", "child_b").unwrap();
    test_db.db.branches().fork("main", "child_c").unwrap();

    let parent_dir = branch_dir(&test_db.db, "main");
    let parent_ssts = list_ssts(&parent_dir);
    assert!(!parent_ssts.is_empty());

    // Delete the parent. Descendants must remain untouched.
    test_db.db.branches().delete("main").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("gc succeeds");

    for path in &parent_ssts {
        assert!(
            path.exists(),
            "parent segment {:?} must survive delete while three descendants hold",
            path,
        );
    }

    let kv = KVStore::new(test_db.db.clone());
    for child in &["child_a", "child_b", "child_c"] {
        for i in 0..30 {
            let key = format!("k{i}");
            assert_eq!(
                kv.get(&resolve(child), "default", &key).unwrap(),
                Some(Value::Int(i)),
                "child {child} must read key {key} through inherited layer",
            );
        }
    }
}

// =============================================================================
// Schedule: delete before fork — gen0 has no children; gc fully reclaims
// Schedule extension: after gen1 recreate, first-gen reclamation should not
// have blocked on inventory disagreement. Pins KD9 (quarantine state only
// protocol state — manifest is still the ledger).
// =============================================================================

#[test]
fn gc_drains_quarantine_in_a_single_pass_under_healthy_recovery() {
    let test_db = TestDb::new();
    test_db.db.branches().create("ephemeral").unwrap();
    for i in 0..10 {
        seed(&test_db.db, "ephemeral", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "ephemeral");
    let ep_dir = branch_dir(&test_db.db, "ephemeral");
    assert!(!list_ssts(&ep_dir).is_empty());

    test_db.db.branches().delete("ephemeral").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("gc succeeds");

    // After healthy gc, no quarantine.manifest, no __quarantine__/,
    // no .sst files should remain.
    assert!(!ep_dir.join(strata_storage::QUARANTINE_FILENAME).exists());
    assert!(!ep_dir.join(strata_storage::QUARANTINE_DIR).exists());
    assert!(list_ssts(&ep_dir).is_empty());
}

// =============================================================================
// Schedule: retention_report() attributes bytes in branch vocabulary
//
// Pins the convergence-doc surface matrix: retention_report is
// generation-aware, reports own/shared/inherited/quarantined totals, and
// routes the manifest-derived attribution through live BranchRefs.
// =============================================================================

#[test]
fn retention_report_attributes_bytes_to_live_branch_refs() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..20 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();
    seed(&test_db.db, "feature", "own_child", 999);
    flush_branch(&test_db.db, "feature");

    let report = test_db
        .db
        .retention_report()
        .expect("healthy; retention_report returns Ok");

    assert_eq!(report.reclaim_status, ReclaimStatus::Allowed);

    let by_name: std::collections::HashMap<String, _> = report
        .branches
        .iter()
        .map(|b| (b.name.clone(), b))
        .collect();

    let main_entry = by_name.get("main").expect("main present in report");
    let feature_entry = by_name.get("feature").expect("feature present in report");

    // Main's bytes are shared because feature inherits them.
    assert!(
        main_entry.shared_bytes > 0,
        "main must report shared retained bytes"
    );
    assert!(
        main_entry
            .blockers
            .iter()
            .any(|b| matches!(b, RetentionBlocker::DescendantHolds { .. })),
        "main must report DescendantHolds when a live descendant still inherits its bytes",
    );
    assert_eq!(main_entry.inherited_layer_bytes, 0);

    // Feature has own exclusive bytes (its own_child write) AND inherited bytes.
    assert!(feature_entry.exclusive_bytes > 0, "feature own_child bytes");
    assert!(
        feature_entry.inherited_layer_bytes > 0,
        "feature must report inherited-layer bytes",
    );
    assert!(
        feature_entry
            .blockers
            .iter()
            .any(|b| matches!(b, RetentionBlocker::InheritedLayerRetention { .. })),
        "feature must list an InheritedLayerRetention blocker",
    );

    assert_eq!(report.orphan_storage.len(), 0, "no orphan dirs yet");
}

// =============================================================================
// Schedule: retention_report() after parent delete — canonical blocker
// attribution points at the live descendant, source is deleted parent.
// =============================================================================

#[test]
fn retention_report_canonical_blocker_points_at_live_descendant_after_parent_delete() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..15 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();
    let main_id = resolve("main");

    test_db.db.branches().delete("main").unwrap();
    test_db
        .db
        .storage()
        .gc_orphan_segments()
        .expect("gc succeeds");

    let report = test_db.db.retention_report().expect("healthy");

    // Canonical blocker attribution: feature is the live lifecycle
    // holding the bytes alive through its inherited layer. The blocker
    // points at main as the source (no live lifecycle, so
    // removable_by_materialization is true).
    let feature_entry = report
        .branches
        .iter()
        .find(|b| b.name == "feature")
        .expect("feature present");
    let found_main_source = feature_entry.blockers.iter().any(|b| match b {
        RetentionBlocker::InheritedLayerRetention {
            source_branch_id,
            source_branch,
            removable_by_materialization,
            ..
        } => {
            *source_branch_id == main_id && source_branch.is_none() && *removable_by_materialization
        }
        _ => false,
    });
    assert!(
        found_main_source,
        "feature's blocker must cite main (source) and mark it removable_by_materialization; got {:?}",
        feature_entry.blockers,
    );

    // Feature's inherited_layer_bytes must account for the retained
    // bytes that physically live under main's directory — no
    // fabrication, no silent loss.
    assert!(
        feature_entry.inherited_layer_bytes > 0,
        "feature must account for inherited bytes; got {}",
        feature_entry.inherited_layer_bytes,
    );

    let orphan_main = report
        .orphan_storage
        .iter()
        .find(|entry| entry.branch_id == main_id)
        .expect("deleted parent storage directory must be reported as orphaned retention");
    assert!(
        orphan_main.bytes > 0,
        "deleted parent orphan entry must account for retained bytes",
    );
    assert_eq!(
        orphan_main.reason,
        strata_engine::OrphanReason::DescendantInheritance,
        "deleted parent bytes are retained by the live descendant, not fabricated as untracked storage",
    );
}

// =============================================================================
// Schedule extension: live-branch top-level orphan `.sst` files that are no
// longer manifest-owned are cleanup debt, not branch-visible retained bytes.
//
// Pins the convergence contract's "manifest-derived truth" rule for
// retention_report(): raw disk debris must not inflate exclusive/shared totals.
// =============================================================================

#[test]
fn retention_report_ignores_live_branch_top_level_orphan_files() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    for i in 0..10 {
        seed(&test_db.db, "main", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "main");

    let before = test_db
        .db
        .retention_report()
        .expect("baseline retention_report succeeds");
    let before_main = before
        .branches
        .iter()
        .find(|b| b.name == "main")
        .expect("main present before orphan write");

    let main_dir = branch_dir(&test_db.db, "main");
    std::fs::write(main_dir.join("99999.sst"), vec![0u8; 4096]).unwrap();

    let after = test_db
        .db
        .retention_report()
        .expect("report should ignore live-branch orphan cleanup debt");
    let after_main = after
        .branches
        .iter()
        .find(|b| b.name == "main")
        .expect("main present after orphan write");

    assert_eq!(
        after_main.exclusive_bytes, before_main.exclusive_bytes,
        "top-level orphan bytes must not inflate manifest-derived exclusive retention",
    );
    assert_eq!(
        after_main.shared_bytes, before_main.shared_bytes,
        "top-level orphan bytes must not inflate manifest-derived shared retention",
    );
}

#[test]
fn retention_report_includes_archived_branch_as_live_lifecycle() {
    let test_db = TestDb::new();
    test_db.db.branches().create("frozen").unwrap();
    for i in 0..4 {
        seed(&test_db.db, "frozen", &format!("k{i}"), i);
    }
    flush_branch(&test_db.db, "frozen");
    test_db.db.branches().fork("frozen", "child").unwrap();
    let archived_ref = archive_branch_for_test(&test_db.db, "frozen");

    let report = test_db
        .db
        .retention_report()
        .expect("archived branch remains visible to retention_report");

    let frozen = report
        .branches
        .iter()
        .find(|entry| entry.name == "frozen")
        .expect("archived branch must appear as a branch entry");
    assert_eq!(frozen.branch, archived_ref);
    assert_eq!(frozen.lifecycle, BranchLifecycleStatus::Archived);
    assert!(
        frozen.exclusive_bytes > 0 || frozen.shared_bytes > 0,
        "archived branch should still account for retained bytes",
    );
    assert!(
        !report
            .orphan_storage
            .iter()
            .any(|entry| entry.branch_id == resolve("frozen")),
        "archived branch storage must not be demoted to orphan storage",
    );

    let child = report
        .branches
        .iter()
        .find(|entry| entry.name == "child")
        .expect("child must appear in retention_report");
    let blocker = child
        .blockers
        .iter()
        .find_map(|blocker| match blocker {
            RetentionBlocker::InheritedLayerRetention {
                source_branch,
                removable_by_materialization,
                ..
            } => Some((source_branch, removable_by_materialization)),
            _ => None,
        })
        .expect("child must report an inherited-layer blocker against the archived source");
    assert_eq!(
        blocker.0.as_ref(),
        Some(&archived_ref),
        "archived source should still resolve to a live BranchRef in blocker attribution",
    );
    assert!(
        !*blocker.1,
        "archived source is still a live lifecycle; child blocker is not removable by materialization",
    );
}
