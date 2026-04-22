//! B3.4 — bundle-import generation policy (AD7) and observer events.
//!
//! These tests cover the generation-handling rules from
//! `docs/design/branching/b3-phasing-plan.md`:
//!
//! - **Bundles without `generation` deserialise as gen 0.** A pre-B3.4
//!   bundle (no `generation` field in `BRANCH.json`) imports onto a
//!   fresh target as `BranchRef { generation: 0 }`. `#[serde(default)]`
//!   on `BundleBranchInfo::generation` is the load-bearing piece.
//!
//! - **Bundle generation is preserved on a fresh target.** A bundle
//!   exported from a source DB whose branch is at gen N imports onto a
//!   target with no prior history for that name as `BranchRef { gen: N }`.
//!   The target's `next_gen` counter is seeded to `N + 1` so a later
//!   recreate strictly advances.
//!
//! - **Tombstoned target collisions allocate fresh.** A target with a
//!   tombstone for the same name (no active record) ignores the bundle's
//!   generation and allocates the next free generation from its own
//!   counter (AD7 fail-safe — imported lineage never aliases existing
//!   tombstones).
//!
//! - **Active target collisions are rejected.** Importing onto a target
//!   that already has the name *active* fails — the user must delete
//!   first (or the import would silently shadow live work).
//!
//! - **Observer events carry generation.** `BranchOpEvent` delivered to
//!   `BranchOpObserver` for create / fork / merge carries the
//!   generation-aware `BranchRef` in addition to the legacy `BranchId`.

#![cfg(not(miri))]

use crate::common::*;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use strata_core::value::Value;
use strata_core::{BranchId, BranchRef};
use strata_durability::branch_bundle::BundleBranchInfo;
use strata_engine::bundle;
use strata_engine::database::observers::{
    BranchOpEvent, BranchOpKind, BranchOpObserver, ObserverError,
};
use strata_engine::primitives::extensions::KVStoreExt;
use tempfile::TempDir;

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

fn seed(test_db: &TestDb, name: &str) {
    test_db
        .kv()
        .put(&resolve(name), "default", "_seed_", Value::Int(1))
        .expect("seed write succeeds");
}

fn export_bundle(test_db: &TestDb, branch: &str) -> (TempDir, PathBuf) {
    let bundle_dir = TempDir::new().unwrap();
    let path = bundle_dir
        .path()
        .join(format!("{branch}.branchbundle.tar.zst"));
    bundle::export_branch(&test_db.db, branch, &path).expect("export succeeds");
    (bundle_dir, path)
}

// ============================================================================
// Bundle-format compatibility: missing `generation` field defaults to 0
// ============================================================================

#[test]
fn legacy_bundle_without_generation_field_imports_as_gen_zero() {
    // Simulate a pre-B3.4 bundle by deserialising a JSON payload that
    // omits the `generation` field. `#[serde(default)]` must produce
    // `generation: 0` so old bundles continue to import cleanly.
    let legacy_json = r#"{
        "branch_id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "legacy-branch",
        "state": "active",
        "created_at": "2025-01-24T10:00:00Z",
        "closed_at": "2025-01-24T11:00:00Z"
    }"#;
    let parsed: BundleBranchInfo = serde_json::from_str(legacy_json).unwrap();
    assert_eq!(
        parsed.generation, 0,
        "missing `generation` field must default to 0"
    );

    // End-to-end: a freshly-exported bundle whose source branch is at
    // gen 0 imports as gen 0 on a fresh target.
    let source_db = TestDb::new();
    source_db.db.branches().create("legacy-branch").unwrap();
    let (_bundle_keepalive, path) = export_bundle(&source_db, "legacy-branch");

    let target_db = TestDb::new();
    bundle::import_branch(&target_db.db, &path).unwrap();

    let rec = target_db
        .db
        .branches()
        .control_record("legacy-branch")
        .unwrap()
        .unwrap();
    assert_eq!(rec.branch.generation, 0);
}

// ============================================================================
// Generation preserved on a fresh target
// ============================================================================

#[test]
fn bundle_generation_is_preserved_when_target_has_no_history() {
    // Build a source DB where `foo` reaches gen 2 via two delete cycles.
    let source_db = TestDb::new();
    source_db.db.branches().create("foo").unwrap();
    source_db.db.branches().delete("foo").unwrap();
    source_db.db.branches().create("foo").unwrap(); // gen 1
    source_db.db.branches().delete("foo").unwrap();
    source_db.db.branches().create("foo").unwrap(); // gen 2
    let source_ref = source_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap()
        .branch;
    assert_eq!(source_ref.generation, 2);

    let (_bundle_keepalive, path) = export_bundle(&source_db, "foo");

    // Import onto a fresh target (no prior `foo` history).
    let target_db = TestDb::new();
    bundle::import_branch(&target_db.db, &path).unwrap();

    let target_rec = target_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap();
    assert_eq!(
        target_rec.branch,
        BranchRef::new(resolve("foo"), 2),
        "fresh-target import preserves source-DB generation verbatim"
    );

    // The target's next-gen counter should now be seeded to 3 — a
    // subsequent delete + recreate must allocate gen 3, not collide
    // with the imported gen 2.
    target_db.db.branches().delete("foo").unwrap();
    target_db.db.branches().create("foo").unwrap();
    let after_recreate = target_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap();
    assert_eq!(after_recreate.branch.generation, 3);
}

// ============================================================================
// Tombstoned target → fresh generation (AD7 fail-safe)
// ============================================================================

#[test]
fn tombstoned_target_ignores_bundle_generation_and_allocates_fresh() {
    // Source: brand-new `foo` at gen 0.
    let source_db = TestDb::new();
    source_db.db.branches().create("foo").unwrap();
    let (_bundle_keepalive, path) = export_bundle(&source_db, "foo");

    // Target: created and tombstoned `foo` at gen 0.
    let target_db = TestDb::new();
    target_db.db.branches().create("foo").unwrap();
    target_db.db.branches().delete("foo").unwrap();

    // Import the bundle that claims gen 0 — AD7 says target must
    // allocate fresh (gen 1, since gen 0 is tombstoned), NOT honour
    // the bundle's gen 0. Otherwise the imported lineage would
    // collide with the existing tombstone's identity.
    bundle::import_branch(&target_db.db, &path).unwrap();

    let target_rec = target_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap();
    assert_eq!(
        target_rec.branch,
        BranchRef::new(resolve("foo"), 1),
        "tombstoned-target import allocates fresh generation, not bundle's"
    );

    // Tombstone preservation: the gen-0 tombstone must still occupy
    // its slot. Verify by deleting the just-imported gen-1 record and
    // creating again — the next allocation must be gen 2, not gen 1.
    // If the import had overwritten the tombstone (the race the
    // inside-the-txn `next_generation` call guards against), the
    // counter would be one short and this final create would land at
    // gen 1.
    target_db.db.branches().delete("foo").unwrap();
    target_db.db.branches().create("foo").unwrap();
    let after_delete_recreate = target_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap();
    assert_eq!(
        after_delete_recreate.branch.generation, 2,
        "import must not overwrite the original gen-0 tombstone — the next-gen counter must be 2 after import + delete + create"
    );
}

#[test]
fn active_target_with_history_rejects_import() {
    // The plan keeps the existing "branch already exists" guard — once
    // `BranchService::create` has materialised an active record for a
    // name, importing onto the same name still fails. The user must
    // delete first.
    let source_db = TestDb::new();
    source_db.db.branches().create("foo").unwrap();
    let (_bundle_keepalive, path) = export_bundle(&source_db, "foo");

    let target_db = TestDb::new();
    target_db.db.branches().create("foo").unwrap();

    let err = bundle::import_branch(&target_db.db, &path).unwrap_err();
    assert!(
        err.to_string().contains("already exists"),
        "active-collision import must surface the user-facing 'already exists' error, got: {err}"
    );
}

#[test]
fn import_with_high_bundle_gen_onto_tombstone_uses_target_next_gen() {
    // Source pushes `foo` to gen 5.
    let source_db = TestDb::new();
    for _ in 0..5 {
        source_db.db.branches().create("foo").unwrap();
        source_db.db.branches().delete("foo").unwrap();
    }
    source_db.db.branches().create("foo").unwrap(); // gen 5
    let source_ref = source_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap()
        .branch;
    assert_eq!(source_ref.generation, 5);
    let (_bundle_keepalive, path) = export_bundle(&source_db, "foo");

    // Target has a single tombstone at gen 0 (one create + delete).
    let target_db = TestDb::new();
    target_db.db.branches().create("foo").unwrap();
    target_db.db.branches().delete("foo").unwrap();

    bundle::import_branch(&target_db.db, &path).unwrap();

    let target_rec = target_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap();
    // AD7: target allocates from its own counter, which advanced to 1
    // when the tombstone landed. Bundle's gen 5 is informational only.
    assert_eq!(
        target_rec.branch.generation, 1,
        "tombstoned target uses its own next_gen counter, not the bundle's"
    );
}

// ============================================================================
// BranchOpEvent carries generation-aware identity (B3.4)
// ============================================================================

struct CapturingObserver {
    events: Mutex<Vec<BranchOpEvent>>,
}

impl CapturingObserver {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
        })
    }
    fn drain(&self) -> Vec<BranchOpEvent> {
        std::mem::take(&mut *self.events.lock())
    }
}

impl BranchOpObserver for CapturingObserver {
    fn name(&self) -> &'static str {
        "branching_generation_migration::CapturingObserver"
    }
    fn on_branch_op(&self, event: &BranchOpEvent) -> Result<(), ObserverError> {
        self.events.lock().push(event.clone());
        Ok(())
    }
}

#[test]
fn create_event_carries_branch_ref_with_generation() {
    let test_db = TestDb::new();
    let obs = CapturingObserver::new();
    test_db.db.branch_op_observers().register(obs.clone());

    test_db.db.branches().create("foo").unwrap();
    test_db.db.branches().delete("foo").unwrap();
    test_db.db.branches().create("foo").unwrap(); // gen 1

    let events = obs.drain();
    let creates: Vec<&BranchOpEvent> = events
        .iter()
        .filter(|e| matches!(e.kind, BranchOpKind::Create))
        .collect();
    assert_eq!(creates.len(), 2);
    assert_eq!(creates[0].branch_ref.generation, 0);
    assert_eq!(creates[1].branch_ref.generation, 1);
    // Legacy `branch_id` must mirror `branch_ref.id` so back-compat
    // observers reading the old field see consistent values.
    assert_eq!(creates[0].branch_id, creates[0].branch_ref.id);
    assert_eq!(creates[1].branch_id, creates[1].branch_ref.id);
}

#[test]
fn fork_event_carries_source_branch_ref_with_parent_generation() {
    let test_db = TestDb::new();
    let obs = CapturingObserver::new();
    test_db.db.branch_op_observers().register(obs.clone());

    test_db.db.branches().create("main").unwrap();
    seed(&test_db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();

    let events = obs.drain();
    let fork = events
        .iter()
        .find(|e| matches!(e.kind, BranchOpKind::Fork))
        .expect("fork emits a BranchOpEvent::Fork");
    let source_ref = fork.source_branch_ref.expect("fork carries source ref");
    assert_eq!(source_ref, BranchRef::new(resolve("main"), 0));
    assert_eq!(fork.branch_ref, BranchRef::new(resolve("feature"), 0));
    // Legacy mirrors stay populated.
    assert_eq!(fork.source_branch_id, Some(source_ref.id));
}

#[test]
fn merge_event_carries_both_target_and_source_branch_refs() {
    let test_db = TestDb::new();
    let obs = CapturingObserver::new();
    test_db.db.branch_op_observers().register(obs.clone());

    test_db.db.branches().create("main").unwrap();
    seed(&test_db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();
    test_db
        .kv()
        .put(&resolve("feature"), "default", "delta", Value::Int(99))
        .unwrap();
    test_db.db.branches().merge("feature", "main").unwrap();

    let events = obs.drain();
    let merge = events
        .iter()
        .find(|e| matches!(e.kind, BranchOpKind::Merge))
        .expect("merge emits a BranchOpEvent::Merge");
    assert_eq!(merge.branch_ref, BranchRef::new(resolve("main"), 0));
    assert_eq!(
        merge.source_branch_ref,
        Some(BranchRef::new(resolve("feature"), 0))
    );
}
