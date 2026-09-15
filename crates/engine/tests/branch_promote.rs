//! Branch promotion (merge) conformance tests (M12D1, conformance #6–8).

mod common;

use serde_json::json;
use strata_engine::{
    ComparedCapability, ConflictKind, ConflictStrategyResult, Database, DerivedStateDisposition,
    EngineErrorClass, JsonDocumentId, JsonPath, JsonValue, PromotionOutcome, PromotionStrategy,
};

use common::{assert_branch_value, branch, key, open_cache_database, space, value};

fn sorted(mut ids: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    ids.sort();
    ids
}

fn applied(outcome: &PromotionOutcome) -> Vec<Vec<u8>> {
    sorted(
        outcome
            .applied()
            .iter()
            .map(|entity| entity.identity().to_vec())
            .collect(),
    )
}

fn deleted(outcome: &PromotionOutcome) -> Vec<Vec<u8>> {
    sorted(
        outcome
            .deleted()
            .iter()
            .map(|entity| entity.identity().to_vec())
            .collect(),
    )
}

fn conflicts(outcome: &PromotionOutcome) -> Vec<Vec<u8>> {
    sorted(
        outcome
            .conflicts()
            .iter()
            .map(|conflict| conflict.identity().to_vec())
            .collect(),
    )
}

fn assert_absent(database: &mut Database, branch_name: &str, space_name: &str, key_bytes: &[u8]) {
    let mut kv = database
        .kv(branch(branch_name), space(space_name))
        .expect("KV opens");
    assert!(
        kv.get(&key(key_bytes)).expect("KV read succeeds").is_none(),
        "expected `{}` to be absent",
        String::from_utf8_lossy(key_bytes)
    );
}

/// Seeds `default` with `shared`/`md`, forks `feature`, then diverges both
/// branches on `shared` (different values) and `md` (feature deletes, default
/// modifies), each adding a private key. This is the canonical conflict setup.
fn diverged_database() -> Database {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"shared"), value(b"base")).expect("shared base");
        kv.put(key(b"md"), value(b"one")).expect("md base");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"shared"), value(b"feature_change"))
            .expect("feature shared");
        kv.delete(key(b"md")).expect("feature deletes md");
        kv.put(key(b"new_feature"), value(b"y"))
            .expect("feature add");
    }
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"shared"), value(b"default_change"))
            .expect("default shared");
        kv.put(key(b"md"), value(b"two")).expect("default md");
        kv.put(key(b"new_default"), value(b"x"))
            .expect("default add");
    }
    database
}

#[test]
fn promote_source_wins_applies_source_changes_and_reports_them() {
    let mut database = diverged_database();

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::SourceWins,
        )
        .expect("source-wins promote succeeds");

    assert_eq!(outcome.source().as_str(), "feature");
    assert_eq!(outcome.target().as_str(), "default");
    assert_eq!(outcome.strategy(), PromotionStrategy::SourceWins);
    assert!(!outcome.is_noop());
    assert!(outcome.target_version().is_some());

    // The branch point is the source's recorded fork version.
    let fork_version = database
        .branches()
        .expect("branch service opens")
        .get(&branch("feature"))
        .expect("feature summary")
        .parent()
        .expect("feature was forked")
        .fork_version();
    assert_eq!(outcome.branch_point(), fork_version);

    // shared (conflict, present source) and new_feature (clean) are applied;
    // md (conflict, source deleted) is deleted; both conflicts are reported.
    assert_eq!(
        applied(&outcome),
        vec![b"new_feature".to_vec(), b"shared".to_vec()]
    );
    assert_eq!(deleted(&outcome), vec![b"md".to_vec()]);
    assert_eq!(
        conflicts(&outcome),
        vec![b"md".to_vec(), b"shared".to_vec()]
    );

    let shared = outcome
        .conflicts()
        .iter()
        .find(|conflict| conflict.identity() == b"shared")
        .expect("shared conflict present");
    assert_eq!(shared.kind(), ConflictKind::ValueDivergence);
    assert_eq!(shared.strategy_result(), ConflictStrategyResult::SourceWins);
    assert_eq!(shared.source_value(), Some(&b"feature_change"[..]));
    assert_eq!(shared.target_value(), Some(&b"default_change"[..]));

    let md = outcome
        .conflicts()
        .iter()
        .find(|conflict| conflict.identity() == b"md")
        .expect("md conflict present");
    assert_eq!(md.kind(), ConflictKind::ModifyDeleteDivergence);
    assert_eq!(md.source_value(), None);

    // The applied entity carries the source value, capability, and space; the
    // deleted entity carries no value.
    let shared_applied = outcome
        .applied()
        .iter()
        .find(|entity| entity.identity() == b"shared")
        .expect("shared applied");
    assert_eq!(shared_applied.capability(), ComparedCapability::Kv);
    assert_eq!(shared_applied.space().as_str(), "default");
    assert_eq!(shared_applied.value(), Some(&b"feature_change"[..]));
    let md_deleted = outcome
        .deleted()
        .iter()
        .find(|entity| entity.identity() == b"md")
        .expect("md deleted");
    assert_eq!(md_deleted.value(), None);

    // Target now reflects the source-wins resolution.
    assert_branch_value(
        &mut database,
        "default",
        "default",
        b"shared",
        b"feature_change",
    );
    assert_branch_value(&mut database, "default", "default", b"new_feature", b"y");
    // Target-only change is preserved; source deletion is propagated.
    assert_branch_value(&mut database, "default", "default", b"new_default", b"x");
    assert_absent(&mut database, "default", "default", b"md");

    // Source branch is never modified.
    assert_branch_value(
        &mut database,
        "feature",
        "default",
        b"shared",
        b"feature_change",
    );
    assert_absent(&mut database, "feature", "default", b"new_default");
}

#[test]
fn promote_records_authoritative_merge_lineage_on_the_target() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"base")).expect("base");
    }
    // Recreate `feature` so its generation is 2, not 1 — the lineage edge must
    // record the source's actual generation, not a constant.
    {
        let mut branches = database.branches().expect("branch service opens");
        branches
            .fork_current(&branch("default"), branch("feature"))
            .expect("first fork (generation 1)");
        branches.delete(&branch("feature")).expect("delete feature");
        branches
            .fork_current(&branch("default"), branch("feature"))
            .expect("second fork (generation 2)");
    }
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"changed"))
            .expect("feature change");
    }

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::SourceWins,
        )
        .expect("promote succeeds");
    let target_version = outcome.target_version().expect("promotion wrote a commit");

    let branches = database.branches().expect("branch service opens");
    let source = branches.get(&branch("feature")).expect("feature summary");
    let source_branch_id = source.branch_id();
    assert_eq!(source.generation(), 2);
    let summary = branches.get(&branch("default")).expect("target summary");
    let merge = summary
        .merge_parent()
        .expect("promotion recorded a merge edge");
    assert_eq!(merge.source_name().as_str(), "feature");
    assert_eq!(merge.source_branch_id(), source_branch_id);
    assert_eq!(merge.source_generation(), 2);
    assert_eq!(merge.merged_at(), target_version);
    assert!(merge.merged_timestamp().is_some());
}

#[test]
fn promote_strict_refuses_on_conflict_with_zero_target_mutation() {
    let mut database = diverged_database();

    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("strict promotion refuses a conflict");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(error.code(), "conflict.engine.promotion");

    // The target is untouched: no partial application, no merge edge.
    assert_branch_value(
        &mut database,
        "default",
        "default",
        b"shared",
        b"default_change",
    );
    assert_branch_value(&mut database, "default", "default", b"md", b"two");
    assert_absent(&mut database, "default", "default", b"new_feature");
    let summary = database
        .branches()
        .expect("branch service opens")
        .get(&branch("default"))
        .expect("target summary");
    assert!(summary.merge_parent().is_none());
}

#[test]
fn promote_of_a_one_sided_change_applies_cleanly_under_strict() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"base")).expect("base");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"changed"))
            .expect("feature change");
    }

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("clean promote succeeds under strict");
    assert!(outcome.conflicts().is_empty());
    assert_eq!(applied(&outcome), vec![b"k".to_vec()]);
    assert!(outcome.deleted().is_empty());
    assert!(outcome.target_version().is_some());

    assert_branch_value(&mut database, "default", "default", b"k", b"changed");
}

#[test]
fn promote_with_no_changes_is_a_noop_and_writes_no_commit() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"base")).expect("base");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("no-op promote succeeds");
    assert!(outcome.is_noop());
    assert!(outcome.target_version().is_none());
    assert!(outcome.applied().is_empty());
    assert!(outcome.deleted().is_empty());
    assert!(outcome.conflicts().is_empty());

    // No merge edge is recorded for a no-op promotion.
    let summary = database
        .branches()
        .expect("branch service opens")
        .get(&branch("default"))
        .expect("target summary");
    assert!(summary.merge_parent().is_none());
}

#[test]
fn test_promotion_outcome_reports_coverage_timestamp_and_derived_state() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // feature adds a KV key and a JSON document.
    database
        .kv(branch("feature"), space("default"))
        .expect("KV opens")
        .put(key(b"k"), value(b"v"))
        .expect("kv put");
    database
        .json(branch("feature"), space("default"))
        .expect("json opens")
        .set_or_create(
            JsonDocumentId::new("doc").expect("id"),
            &JsonPath::root(),
            JsonValue::new(json!({"a": 1})).expect("value"),
        )
        .expect("json set");

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("strict promote succeeds");
    assert!(outcome.conflicts().is_empty());

    // Item 8: the target commit version and timestamp.
    assert!(outcome.target_version().is_some());
    assert!(outcome.target_timestamp().is_some());
    // Item 7 / rule 4: capability coverage and unsupported areas.
    assert!(outcome
        .capabilities_covered()
        .contains(&ComparedCapability::Kv));
    assert!(outcome
        .capabilities_covered()
        .contains(&ComparedCapability::Json));
    assert!(outcome
        .capabilities_covered()
        .contains(&ComparedCapability::Vector));
    assert!(outcome
        .capabilities_unsupported()
        .contains(&ComparedCapability::Event));
    assert!(outcome
        .capabilities_unsupported()
        .contains(&ComparedCapability::GraphMetadata));
    // Item 7: the spaces the promotion spanned.
    assert!(outcome
        .spaces_covered()
        .iter()
        .any(|space| space.as_str() == "default"));
    // Item 9 / rule 5: promoting JSON leaves its secondary index rebuild-required.
    assert!(outcome.derived_state().iter().any(|report| {
        report.capability() == ComparedCapability::Json
            && report.disposition() == DerivedStateDisposition::RebuildRequired
    }));
}

#[test]
fn test_promotion_registers_source_only_spaces_on_the_target() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // feature creates a new space `extra` and writes a KV row into it.
    database
        .spaces(branch("feature"))
        .expect("space service opens")
        .create(space("extra"))
        .expect("space create succeeds");
    database
        .kv(branch("feature"), space("extra"))
        .expect("KV opens")
        .put(key(b"k"), value(b"v"))
        .expect("kv put succeeds");

    // Precondition: the target does not yet know the `extra` space.
    assert!(!database
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&space("extra"))
        .expect("exists succeeds"));

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("promote succeeds");
    assert!(outcome.conflicts().is_empty());

    // The promoted row must land in a space the target's catalog registers, so
    // catalog-driven paths (listing, admin, diff) can see it — not orphaned.
    assert!(
        database
            .spaces(branch("default"))
            .expect("space service opens")
            .exists(&space("extra"))
            .expect("exists succeeds"),
        "promotion must register the source-only space on the target"
    );
    // And the promoted KV row is readable through that registered space.
    assert_branch_value(&mut database, "default", "extra", b"k", b"v");
}

#[test]
fn test_promotion_registers_multiple_source_only_spaces_in_one_index() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    for name in ["extra_a", "extra_b"] {
        database
            .spaces(branch("feature"))
            .expect("space service opens")
            .create(space(name))
            .expect("space create succeeds");
        database
            .kv(branch("feature"), space(name))
            .expect("KV opens")
            .put(key(b"k"), value(name.as_bytes()))
            .expect("kv put succeeds");
    }

    database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("promote succeeds");

    // Both source-only spaces must survive in the target's single rewritten
    // space index — a per-space index write would clobber all but the last.
    let listed = database
        .spaces(branch("default"))
        .expect("space service opens")
        .list()
        .expect("list succeeds");
    assert!(
        listed.iter().any(|s| s.as_str() == "extra_a"),
        "extra_a must be registered"
    );
    assert!(
        listed.iter().any(|s| s.as_str() == "extra_b"),
        "extra_b must be registered"
    );
    assert_branch_value(&mut database, "default", "extra_a", b"k", b"extra_a");
    assert_branch_value(&mut database, "default", "extra_b", b"k", b"extra_b");
}

#[test]
fn test_promotion_removes_a_space_deleted_on_source() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `shared` exists on default BEFORE the fork, so it is part of the base.
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(space("shared"))
        .expect("space create succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The source deletes `shared`; the target independently adds `default_only`
    // (a target-only space the source never had).
    database
        .spaces(branch("feature"))
        .expect("space service opens")
        .delete(&space("shared"), true)
        .expect("space delete succeeds");
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(space("default_only"))
        .expect("space create succeeds");

    // Precondition: the target still registers `shared`.
    assert!(database
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&space("shared"))
        .expect("exists succeeds"));

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("promote succeeds");
    assert!(outcome.conflicts().is_empty());

    // The space the source deleted (present in the base) must be removed from the
    // target — its stale registration would otherwise linger.
    assert!(
        !database
            .spaces(branch("default"))
            .expect("space service opens")
            .exists(&space("shared"))
            .expect("exists succeeds"),
        "promotion must remove a space the source deleted"
    );
    // A target-only space the source never had must survive (base-relative three-way,
    // not a naive source-vs-target difference).
    assert!(
        database
            .spaces(branch("default"))
            .expect("space service opens")
            .exists(&space("default_only"))
            .expect("exists succeeds"),
        "a target-only space must not be deleted"
    );
    // The default space is never removed.
    assert!(database
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&space("default"))
        .expect("exists succeeds"));
}

#[test]
fn test_promotion_keeps_a_deleted_space_the_target_still_uses() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `shared` exists on both branches at the fork (part of the base).
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(space("shared"))
        .expect("space create succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The source deletes `shared`; the target writes a NEW (target-only) row into
    // it after the fork.
    database
        .spaces(branch("feature"))
        .expect("space service opens")
        .delete(&space("shared"), true)
        .expect("space delete succeeds");
    database
        .kv(branch("default"), space("shared"))
        .expect("KV opens")
        .put(key(b"k2"), value(b"v2"))
        .expect("target-only row");

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("promote succeeds");
    assert!(outcome.conflicts().is_empty());

    // Deregistering `shared` would orphan the target-only row outside the
    // catalog, so a space the target still holds a live row in stays registered
    // and the row stays readable.
    assert!(
        database
            .spaces(branch("default"))
            .expect("space service opens")
            .exists(&space("shared"))
            .expect("exists succeeds"),
        "a space the target still holds rows in must stay registered"
    );
    assert_branch_value(&mut database, "default", "shared", b"k2", b"v2");
}

#[test]
fn promote_of_unrelated_branches_is_rejected() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .create(branch("other"))
        .expect("empty root branch");

    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("default"),
            &branch("other"),
            PromotionStrategy::Strict,
        )
        .expect_err("unrelated branches have no branch point");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.branch_point");
}

#[test]
fn test_repeated_promotion_incorporates_prior_merge() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"base")).expect("default base");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    let feature_frontier = {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"v1"))
            .expect("feature v1")
            .commit()
            .version()
    };

    // First promotion incorporates feature@v1 into default.
    let first = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("first strict promote succeeds");
    assert!(first.conflicts().is_empty());
    assert!(first.target_version().is_some(), "first promote committed");
    assert_branch_value(&mut database, "default", "default", b"k", b"v1");

    // Feature advances past the merge.
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"v2")).expect("feature v2");
    }

    // The second promotion must diff against the recorded merge edge, not the
    // original fork point: default's k=v1 was itself incorporated from feature,
    // so it is not a conflicting target-side change.
    let second = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("second strict promote must not false-conflict");
    assert!(
        second.conflicts().is_empty(),
        "repeated promotion must not conflict on already-merged changes"
    );
    // The branch point is the source's frontier at the prior merge (the true
    // LCA), not the target's post-merge commit — the latter would include
    // target-only rows and re-surface them as spurious source deletions.
    assert_eq!(second.branch_point(), feature_frontier);
    assert_branch_value(&mut database, "default", "default", b"k", b"v2");
}

/// Regression for the repeated-promote data-loss bug: a second promote with no
/// source change must NOT delete rows the target holds but the source never had.
/// The merge base must be the source's frontier at the prior merge, not the
/// target's post-merge commit (which includes those target-only rows and would
/// read them back as spurious source deletions).
#[test]
fn test_repeated_promotion_preserves_target_only_rows() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"a"), value(b"1")).expect("default a");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // `b` is added on default only, AFTER the fork — the source never sees it.
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"b"), value(b"2")).expect("default-only b");
    }
    // feature changes the shared row.
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"a"), value(b"10")).expect("feature a");
    }

    // First promotion applies a->10 and keeps the target-only b.
    let first = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("first strict promote succeeds");
    assert!(first.conflicts().is_empty());
    assert!(first.deleted().is_empty());
    assert_branch_value(&mut database, "default", "default", b"a", b"10");
    assert_branch_value(&mut database, "default", "default", b"b", b"2");

    // Second promotion with NO source change must be a true no-op: no conflict,
    // no deletion, and the target-only row survives.
    let second = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("second strict promote succeeds");
    assert!(
        second.conflicts().is_empty(),
        "repeated promotion must not conflict"
    );
    assert!(
        second.deleted().is_empty(),
        "repeated promotion must not delete the target-only row"
    );
    assert!(
        second.is_noop(),
        "a repeated promote with no source change writes no commit"
    );
    // The target-only row survives; the shared row is unchanged.
    assert_branch_value(&mut database, "default", "default", b"b", b"2");
    assert_branch_value(&mut database, "default", "default", b"a", b"10");
}

/// The realistic repeated-promote intersection in a single second promote: the
/// source advances with a NEW row, a target-only row must be preserved, and an
/// already-merged shared row must stay unchanged. Diffing against the source
/// frontier applies only the new row and touches nothing else.
#[test]
fn test_repeated_promotion_applies_new_source_row_and_preserves_target_only_row() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"a"), value(b"1")).expect("default a");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // `b` is target-only (added on default after the fork); `a` diverges on feature.
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"b"), value(b"2")).expect("default-only b");
    }
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"a"), value(b"10")).expect("feature a");
    }

    // First promotion incorporates a->10 and keeps b.
    database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("first strict promote succeeds");

    // The source advances with a brand-new row after the merge.
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"c"), value(b"30")).expect("feature adds c");
    }

    // Second promotion: only `c` is new relative to the source frontier; `a` is
    // already merged (no-op) and `b` is target-only (preserved).
    let second = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("second strict promote succeeds");
    assert!(second.conflicts().is_empty());
    assert_eq!(applied(&second), vec![b"c".to_vec()]);
    assert!(second.deleted().is_empty());
    assert_branch_value(&mut database, "default", "default", b"a", b"10");
    assert_branch_value(&mut database, "default", "default", b"b", b"2");
    assert_branch_value(&mut database, "default", "default", b"c", b"30");
}

#[test]
fn test_prior_merge_from_another_source_does_not_shift_the_branch_point() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"base")).expect("default base");
    }
    // Two features fork from the same base state (default@k=base).
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature_a"))
        .expect("fork a succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature_b"))
        .expect("fork b succeeds");
    {
        let mut kv = database
            .kv(branch("feature_a"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"a1")).expect("feature_a a1");
    }
    {
        let mut kv = database
            .kv(branch("feature_b"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"b1")).expect("feature_b b1");
    }

    // Promote feature_a: default now holds a1, with a merge edge from feature_a.
    database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature_a"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("promote a succeeds");
    assert_branch_value(&mut database, "default", "default", b"k", b"a1");

    // Promoting feature_b must use feature_b's OWN fork base (k=base), not the
    // merge edge recorded for feature_a. feature_b changed base->b1 while default
    // changed base->a1, so this is a genuine conflict — not a clean re-apply.
    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature_b"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("a merge edge from another source is a genuine conflict");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(error.code(), "conflict.engine.promotion");
}

#[test]
fn test_recreated_source_after_merge_uses_its_own_fork_base() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"base")).expect("default base");
    }
    // First incarnation of `feature`: fork, diverge, promote into default.
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork gen1 succeeds");
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"v1")).expect("feature v1");
    }
    database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("promote gen1 succeeds");
    // default now holds v1, with a merge edge recorded from feature@gen1.

    // default advances locally AFTER the merge, so the stale merge state (v1) and
    // the state the re-fork will inherit (default_after) genuinely differ.
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"default_after"))
            .expect("default advances after merge");
    }

    // Delete `feature` and re-fork it — a new generation reusing the same name
    // (and thus the same branch id) — from default's new head.
    database
        .branches()
        .expect("branch service opens")
        .delete(&branch("feature"))
        .expect("delete gen1 succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("re-fork gen2 succeeds");
    {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("KV opens");
        kv.put(key(b"k"), value(b"w1")).expect("feature gen2 w1");
    }

    // Promoting the re-forked feature must use ITS OWN fork base (default_after),
    // not the stale merge edge from the deleted gen1 incarnation. Against its own
    // fork base, default is unchanged, so this is a clean apply — matching the
    // stale merge state (v1) instead would falsely conflict on default_after.
    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("re-forked feature promotes cleanly against its own fork base");
    assert!(
        outcome.conflicts().is_empty(),
        "a re-forked source must diff against its own fork, not a stale merge edge"
    );
    assert_branch_value(&mut database, "default", "default", b"k", b"w1");
}

/// What a promotion reports as the value it applied must be the value the
/// caller wrote, not the row the engine stored (#3202).
///
/// `PromotedEntity::value` carried the storage row verbatim. For KV that is
/// the authored value, which is why the defect read as JSON-specific. It is
/// the other way round: KV is the one capability that stores exactly what the
/// caller wrote, and every capability that wraps its value in an envelope
/// leaked the envelope — the format byte, then a wrapper carrying the identity
/// again and the engine's own bookkeeping, with the authored value nested
/// inside. A consumer could not hand it to a JSON parser without stripping a
/// byte first, and nothing in the record said which capabilities needed that.
#[test]
fn a_promotion_reports_the_authored_value_not_the_stored_row() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    database
        .kv(branch("feature"), space("default"))
        .expect("KV opens")
        .put(key(b"k"), value(b"authored"))
        .expect("kv put");
    database
        .json(branch("feature"), space("default"))
        .expect("json opens")
        .set_or_create(
            JsonDocumentId::new("doc").expect("id"),
            &JsonPath::root(),
            JsonValue::new(json!({"a": 2})).expect("value"),
        )
        .expect("json set");

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("strict promote succeeds");

    let value_for = |identity: &[u8]| -> Vec<u8> {
        outcome
            .applied()
            .iter()
            .find(|entity| entity.identity() == identity)
            .and_then(|entity| entity.value())
            .expect("an applied value")
            .to_vec()
    };

    // KV is unchanged: the stored row is the authored value.
    assert_eq!(value_for(b"k"), b"authored".to_vec());

    // JSON reports the document, and nothing of the envelope around it.
    let document = value_for(b"doc");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&document).expect("the document parses as-is"),
        json!({ "a": 2 }),
        "reported {:?}",
        String::from_utf8_lossy(&document)
    );
    for internal in ["document_version", "updated_at_micros"] {
        assert!(
            !String::from_utf8_lossy(&document).contains(internal),
            "the envelope's `{internal}` reached the caller: {:?}",
            String::from_utf8_lossy(&document)
        );
    }
    assert!(
        !document.iter().any(u8::is_ascii_control),
        "the format byte reached the caller: {document:?}"
    );
}

/// A refused promotion reports the same authored values (#3202).
///
/// `PreviewConflict` carries `source_value`/`target_value` for a caller to
/// show or diff, and they came from the same stored rows as `applied[]`. A
/// library caller inspecting a conflict got the envelope; the CLI hid it only
/// because its refusal is a structured error that carries no values.
#[test]
fn a_conflict_reports_authored_values_on_both_sides() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    for (target_branch, value) in [
        ("feature", json!({ "a": 2 })),
        ("default", json!({ "a": 3 })),
    ] {
        database
            .json(branch(target_branch), space("default"))
            .expect("json opens")
            .set_or_create(
                JsonDocumentId::new("doc").expect("id"),
                &JsonPath::root(),
                JsonValue::new(value).expect("value"),
            )
            .expect("json set");
    }

    let outcome = database
        .branches()
        .expect("branch service opens")
        .preview(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("preview succeeds");

    let conflict = outcome
        .conflicts()
        .iter()
        .find(|conflict| conflict.identity() == b"doc")
        .expect("doc conflict present");
    for (side, bytes) in [
        ("source", conflict.source_value()),
        ("target", conflict.target_value()),
    ] {
        let bytes = bytes.expect("both sides are present");
        assert!(
            serde_json::from_slice::<serde_json::Value>(bytes).is_ok(),
            "the {side} value does not parse as the document it reports: {:?}",
            String::from_utf8_lossy(bytes)
        );
        assert!(
            !String::from_utf8_lossy(bytes).contains("document_version"),
            "the {side} value carries the envelope: {:?}",
            String::from_utf8_lossy(bytes)
        );
    }
}
