//! Vector capability branch compare + promote conformance (M12G-vector).

mod common;

use strata_engine::{
    BranchStateSelector, ComparedCapability, ConflictKind, Database, DerivedStateDisposition,
    EmbeddingModelId, EngineErrorClass, PromotionStrategy, VectorCollectionName, VectorConfig,
    VectorDistanceMetric, VectorEmbedding, VectorKey,
};

use common::{branch, open_cache_database, space};

fn collection() -> VectorCollectionName {
    VectorCollectionName::new("emb").expect("valid collection")
}

fn upsert(database: &mut Database, branch_name: &str, key: &str, embedding: Vec<f32>) {
    database
        .vector(branch(branch_name), space("default"))
        .expect("vector service opens")
        .upsert(
            collection(),
            VectorKey::new(key).expect("key"),
            VectorEmbedding::new(embedding).expect("embedding"),
            None,
        )
        .expect("upsert");
}

#[test]
fn vector_compare_and_promote_across_a_fork() {
    let mut database = open_cache_database().expect("cache open succeeds");

    // Seed `default` with two vectors, fork `feature`, then on `feature` change
    // one vector and add another (target left unchanged since the fork).
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    upsert(&mut database, "default", "v1", vec![0.0, 1.0]);
    upsert(&mut database, "default", "v2", vec![0.0, 2.0]);
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    upsert(&mut database, "feature", "v1", vec![9.0, 9.0]);
    upsert(&mut database, "feature", "v3", vec![3.0, 3.0]);

    // Compare default → feature: the vector capability reports v1 modified and
    // v3 added; v2 is unchanged and absent from the diff.
    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");
    let vector_space = comparison
        .comparisons()
        .iter()
        .find(|space| space.capability() == ComparedCapability::Vector)
        .expect("a vector comparison is present");
    assert_eq!(vector_space.added().len(), 1, "v3 is added on feature");
    assert_eq!(vector_space.modified().len(), 1, "v1 diverged");
    assert!(vector_space.removed().is_empty());

    // Promote feature → default (strict, no conflict): applies v1 and v3.
    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("strict promote succeeds");
    assert!(outcome.target_version().is_some());
    assert_eq!(outcome.applied().len(), 2, "v1 and v3 applied");
    assert!(
        outcome
            .applied()
            .iter()
            .all(|entity| entity.capability() == ComparedCapability::Vector),
        "the promoted entities are vectors",
    );
    // Promoting vectors keeps search correct via the query-time fallback, so the
    // derived vector index needs no rebuild (contract §Promotion rule 9).
    assert!(outcome.derived_state().iter().any(|report| {
        report.capability() == ComparedCapability::Vector
            && report.disposition() == DerivedStateDisposition::Current
    }));

    // After the promote the two branches agree: no vector differences remain.
    let after = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare after promote");
    let residual = after
        .comparisons()
        .iter()
        .find(|space| space.capability() == ComparedCapability::Vector);
    assert!(
        residual.is_none_or(|space| {
            space.added().is_empty() && space.removed().is_empty() && space.modified().is_empty()
        }),
        "vectors are in sync after promote",
    );
}

#[test]
fn test_promotion_carries_source_created_collection_config() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // feature creates a brand-new collection (absent from default) and fills it.
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    upsert(&mut database, "feature", "v1", vec![0.0, 1.0]);

    // Precondition: the target has no such collection.
    assert!(database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .collection_info(&collection())
        .expect("info succeeds")
        .is_none());

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

    // The collection config must be carried so the collection is usable on the
    // target, rather than the promoted vectors being orphaned behind a missing
    // config (reads would fail not_found.engine.vector_collection).
    let info = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .collection_info(&collection())
        .expect("info succeeds")
        .expect("source-created collection must be registered on the target");
    assert_eq!(info.config().dimension(), 2);
    assert_eq!(info.config().metric(), VectorDistanceMetric::Cosine);
    // And the promoted vector is readable through the now-usable collection.
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1,
        "the promoted vector is visible through the carried collection"
    );
}

#[test]
fn test_promotion_removes_a_collection_deleted_on_source() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `emb` exists on both branches at the fork (part of the base).
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The source deletes `emb`; the target independently creates a target-only
    // collection `keep`.
    let deleted = database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete collection");
    assert!(deleted);
    let keep = VectorCollectionName::new("keep").expect("valid collection");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            keep.clone(),
            VectorConfig::new(3, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create keep collection");

    // Precondition: the target still registers `emb`.
    assert!(database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .collection_info(&collection())
        .expect("info succeeds")
        .is_some());

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

    // `emb` (deleted on source, present in the base) must be removed from the target.
    assert!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .is_none(),
        "promotion must remove a collection the source deleted"
    );
    // A target-only collection (absent from the base) must survive.
    assert!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&keep)
            .expect("info succeeds")
            .is_some(),
        "a target-only collection must not be deleted"
    );
}

#[test]
fn test_promotion_keeps_a_deleted_collection_the_target_still_uses() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `emb` exists on both branches at the fork.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The source deletes `emb`; the target writes a NEW (target-only) vector into it.
    let deleted = database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete collection");
    assert!(deleted);
    upsert(&mut database, "default", "t1", vec![0.0, 5.0]);

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

    // Deregistering `emb` would orphan the target-only vector behind a missing
    // config, so a collection the target still holds a live vector in stays.
    assert!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .is_some(),
        "a collection the target still holds vectors in must stay registered"
    );
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1,
        "the target-only vector remains readable"
    );
}

#[test]
fn test_promotion_keeps_a_collection_whose_key_the_source_created_and_deleted() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `emb` exists on both at the fork, empty (the base holds no vectors).
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The source adds a vector `k`, then deletes the whole collection (tombstoning
    // `k` and the config). Relative to the base this is a net no-op for `k`.
    upsert(&mut database, "feature", "k", vec![1.0, 1.0]);
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete collection");
    // The target independently adds the SAME key `k` (vector keys are branch-
    // independent).
    upsert(&mut database, "default", "k", vec![2.0, 2.0]);

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

    // `k` was never in the base, so the data three-way does not delete it; its
    // collection must NOT be deregistered, or `k` would be orphaned behind a
    // missing config.
    assert!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .is_some(),
        "a collection with a surviving target vector must stay registered"
    );
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1,
        "the surviving target vector remains readable"
    );
}

#[test]
fn test_promotion_removes_collection_configs_in_a_source_deleted_space() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let with_coll = space("with_coll");
    // A space holding a collection exists on both branches at the fork (the base).
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(with_coll.clone())
        .expect("create space");
    database
        .vector(branch("default"), with_coll.clone())
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The source deletes the whole space (force: purges its collection config).
    database
        .spaces(branch("feature"))
        .expect("space service opens")
        .delete(&with_coll, true)
        .expect("delete space");

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
    // The space is deregistered on the target.
    assert!(!database
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&with_coll)
        .expect("exists succeeds"));

    // Re-creating the space must NOT resurface the stale collection config the
    // source deleted — the deregistered space's collection metadata must be gone.
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(with_coll.clone())
        .expect("re-create space");
    assert!(
        database
            .vector(branch("default"), with_coll.clone())
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .is_none(),
        "a collection config in a source-deleted space must not linger and resurface"
    );
}

#[test]
fn test_promotion_removes_a_target_added_collection_config_in_a_deregistered_space() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let doomed = space("doomed");
    // The space exists at the fork (base-present) but holds no collection there.
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(doomed.clone())
        .expect("create space");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The source deletes the whole space.
    database
        .spaces(branch("feature"))
        .expect("space service opens")
        .delete(&doomed, true)
        .expect("delete space");
    // The TARGET adds an empty collection (config, no vectors) into that space
    // AFTER the fork — the #2972 case: target-added, absent from the base, so
    // the promotion three-way keeps it, yet the space deregisters because the
    // collection holds no rows to retain it.
    database
        .vector(branch("default"), doomed.clone())
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");

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
    // The space is deregistered on the target: the source deleted it, and the
    // target's empty collection holds no rows to keep it alive.
    assert!(!database
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&doomed)
        .expect("exists succeeds"));

    // #2972: re-creating the space must NOT resurface the target-added config.
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(doomed.clone())
        .expect("re-create space");
    assert!(
        database
            .vector(branch("default"), doomed.clone())
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .is_none(),
        "a target-added collection config in a deregistered space must not linger and resurface (#2972)"
    );
}

// A collection the source deleted while the target independently reshaped it
// (delete + recreate with a different config) is a modify/delete divergence.
fn reshaped_deletion_database() -> Database {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // Source deletes `emb`.
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete collection");
    // Target deletes and recreates `emb` with a different (incompatible) config.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete collection");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(3, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("recreate collection");
    database
}

#[test]
fn test_strict_refuses_when_source_deletes_a_collection_the_target_reshaped() {
    let mut database = reshaped_deletion_database();
    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("a modify/delete divergence refuses under Strict");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(error.code(), "conflict.engine.promotion");
    // The target is untouched — `emb` keeps the target's reshaped config.
    let info = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .collection_info(&collection())
        .expect("info succeeds")
        .expect("target collection retained on strict refusal");
    assert_eq!(info.config().dimension(), 3);
}

#[test]
fn test_source_wins_removes_a_collection_the_target_reshaped() {
    let mut database = reshaped_deletion_database();
    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::SourceWins,
        )
        .expect("source-wins promote applies the deletion");
    assert!(!outcome.conflicts().is_empty());
    // Source wins: the collection the source deleted is removed from the target.
    assert!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .is_none(),
        "SourceWins applies the source-side collection deletion"
    );
}

#[test]
fn test_preview_reports_an_incompatible_collection_conflict() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // Both branches independently create the same collection with incompatible dims.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create default collection");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create feature collection");

    // Preview must surface the same conflict promote refuses on — not report clean.
    let preview = database
        .branches()
        .expect("branch service opens")
        .preview(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("preview succeeds");
    assert!(
        preview
            .conflicts()
            .iter()
            .any(|conflict| conflict.kind() == ConflictKind::IncompatibleCollection),
        "preview must report the incompatible-collection conflict that promote refuses on"
    );

    // Parity: promote does refuse on exactly this conflict.
    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("incompatible collection config refuses");
    assert_eq!(error.code(), "conflict.engine.promotion");
}

#[test]
fn test_preview_of_a_compatible_source_collection_reports_no_conflict() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // A source-only collection is carried cleanly — not a conflict.
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create feature collection");

    let preview = database
        .branches()
        .expect("branch service opens")
        .preview(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("preview succeeds");
    assert!(
        !preview
            .conflicts()
            .iter()
            .any(|conflict| conflict.kind() == ConflictKind::IncompatibleCollection),
        "a compatible source-only collection must not preview as a conflict"
    );
}

#[test]
fn test_target_only_collection_reshape_does_not_block_promotion() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `emb` (dim 2) exists on both branches at the fork (the base).
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The TARGET independently reshapes `emb` (delete + recreate dim 4); the source
    // never touches `emb`. The source makes an unrelated change: a new collection.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete emb");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("recreate emb dim 4");
    let other = VectorCollectionName::new("other").expect("valid collection");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            other.clone(),
            VectorConfig::new(3, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("source unrelated collection");

    // Since the source did not change `emb`, its target-only reshape is not a
    // conflict and must not block the unrelated source change.
    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("strict promote must not false-conflict on a target-only reshape");
    assert!(outcome.conflicts().is_empty());
    // The unrelated source collection was carried.
    assert!(database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .collection_info(&other)
        .expect("info succeeds")
        .is_some());
    // The target's own reshape (dim 4) is preserved — the source did not change it.
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("emb present")
            .config()
            .dimension(),
        4
    );
}

#[test]
fn test_source_only_collection_reshape_applies_when_target_unchanged() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `emb` (dim 2) exists on both branches at the fork (the base).
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The SOURCE reshapes `emb` (delete + recreate dim 4); the target is unchanged
    // (still dim 2, no vectors).
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete emb");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("recreate emb dim 4");

    // The source changed `emb` and the target did not, so the source's config
    // applies cleanly — not a conflict.
    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("strict promote must apply a source-only reshape");
    assert!(outcome.conflicts().is_empty());
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("emb present")
            .config()
            .dimension(),
        4,
        "the source's reshaped config is applied to the unchanged target"
    );
}

#[test]
fn test_source_reshape_conflicts_when_target_has_its_own_vectors() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `emb` (dim 2) exists on both branches at the fork.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The source reshapes `emb` to dim 4; the target keeps dim 2 and adds its own
    // dim-2 vector. Applying the source's dim-4 config would strand that vector.
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete emb");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("recreate emb dim 4");
    upsert(&mut database, "default", "t1", vec![1.0, 2.0]);

    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("reshaping onto surviving target vectors is a structural conflict");
    assert_eq!(error.code(), "conflict.engine.promotion");
    // The target is untouched: dim 2 and its vector remain.
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("emb present")
            .config()
            .dimension(),
        2
    );
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1
    );
}

#[test]
fn test_target_reshape_with_source_unchanged_vector_does_not_conflict() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // base: `emb` (dim 2) with a vector `v0`.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    upsert(&mut database, "default", "v0", vec![0.0, 1.0]);
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The TARGET reshapes `emb` to dim 4. The source is entirely unchanged (still
    // holds the inherited dim-2 `v0`). Since the source carries nothing (v0 is
    // unchanged vs base), there is no mismatched vector and no conflict.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete emb");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("recreate emb dim 4");

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("an unchanged source must not conflict with a target-only reshape");
    assert!(outcome.conflicts().is_empty());
    // The target keeps its own dim-4 reshape; nothing was carried.
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("emb present")
            .config()
            .dimension(),
        4
    );
}

#[test]
fn test_target_reshape_conflicts_when_source_carries_old_shape_vectors() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // `emb` (dim 2) exists on both branches at the fork.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // The TARGET reshapes `emb` to dim 4. The source keeps `emb` at dim 2 (config
    // unchanged) but adds a dim-2 vector — which the data three-way would carry
    // into the target's now-dim-4 collection, mismatching its shape.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete emb");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("recreate emb dim 4");
    upsert(&mut database, "feature", "s1", vec![1.0, 2.0]);

    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("carrying old-shape vectors into a reshaped collection is a conflict");
    assert_eq!(error.code(), "conflict.engine.promotion");
    // The target keeps dim 4 and no mismatched vector landed.
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("emb present")
            .config()
            .dimension(),
        4
    );
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        0
    );
}

#[test]
fn test_source_wins_keeps_a_reshaped_collection_the_target_still_uses_and_reports_conflict() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // base: `emb` (dim 2) with a vector.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    upsert(&mut database, "default", "v0", vec![0.0, 1.0]);
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // Source deletes `emb`; the target reshapes it to dim 4 and keeps a target-only
    // vector. Under SourceWins this is a modify/delete divergence, but the retain
    // guard keeps the in-use collection rather than orphaning its vector.
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("source deletes emb");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("target deletes emb");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("target reshapes emb dim 4");
    upsert(&mut database, "default", "t1", vec![1.0, 2.0, 3.0, 4.0]);

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::SourceWins,
        )
        .expect("source-wins promote succeeds");
    assert!(
        outcome
            .conflicts()
            .iter()
            .any(|conflict| conflict.kind() == ConflictKind::ModifyDeleteDivergence),
        "the source-delete vs target-reshape divergence is reported"
    );
    // The in-use collection is kept (not orphaned) and its vector survives.
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("emb present")
            .config()
            .dimension(),
        4
    );
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1
    );
}

#[test]
fn test_empty_vector_collection_creation_is_visible_in_the_diff() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // feature creates an empty collection — a config row only, no vectors.
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");

    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");

    // The created collection surfaces as a VectorCollection addition — the diff is
    // not empty just because the collection has no vectors yet.
    let vc = comparison
        .comparisons()
        .iter()
        .find(|s| s.capability() == ComparedCapability::VectorCollection)
        .expect("a vector collection comparison is present");
    assert_eq!(vc.added().len(), 1, "the created collection is added");
    assert!(vc.modified().is_empty());
    assert!(vc.removed().is_empty());
}

#[test]
fn test_vector_collection_diff_is_scoped_to_its_space() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let sa = space("sa");
    let sb = space("sb");
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(sa.clone())
        .expect("create sa");
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(sb.clone())
        .expect("create sb");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // feature adds a distinct collection to each space.
    database
        .vector(branch("feature"), sa.clone())
        .expect("vector service opens")
        .create_collection(
            VectorCollectionName::new("ca").expect("collection"),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create ca");
    database
        .vector(branch("feature"), sb.clone())
        .expect("vector service opens")
        .create_collection(
            VectorCollectionName::new("cb").expect("collection"),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create cb");

    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");

    // Each space's collection diff contains ONLY its own collection — the diff is
    // scoped to the space, not the whole branch.
    let for_space = |name: &str| {
        comparison
            .comparisons()
            .iter()
            .find(|s| {
                s.capability() == ComparedCapability::VectorCollection && s.space().as_str() == name
            })
            .unwrap_or_else(|| panic!("a vector collection diff for `{name}` is present"))
    };
    assert_eq!(
        for_space("sa").added().len(),
        1,
        "sa has only its own collection"
    );
    assert_eq!(
        for_space("sb").added().len(),
        1,
        "sb has only its own collection"
    );
}

#[test]
fn test_deleted_vector_collection_is_visible_in_the_diff_as_removed() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete collection");

    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");
    let vc = comparison
        .comparisons()
        .iter()
        .find(|s| s.capability() == ComparedCapability::VectorCollection)
        .expect("a vector collection comparison is present");
    assert_eq!(vc.removed().len(), 1, "the deleted collection is removed");
    assert!(vc.added().is_empty());
    assert!(vc.modified().is_empty());
}

#[test]
fn test_reshaped_vector_collection_is_visible_in_the_diff_as_modified() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // feature reshapes the collection (delete + recreate with a different config).
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete collection");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("recreate collection dim 4");

    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");
    let vc = comparison
        .comparisons()
        .iter()
        .find(|s| s.capability() == ComparedCapability::VectorCollection)
        .expect("a vector collection comparison is present");
    assert_eq!(
        vc.modified().len(),
        1,
        "the reshaped collection is modified"
    );
    assert!(vc.added().is_empty());
    assert!(vc.removed().is_empty());
}

#[test]
fn test_identical_vector_collections_produce_no_diff() {
    let mut database = open_cache_database().expect("cache open succeeds");
    // Both branches inherit the same collection from the base — no diff.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");

    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");

    assert!(
        !comparison
            .comparisons()
            .iter()
            .any(|s| s.capability() == ComparedCapability::VectorCollection),
        "an unchanged collection must not appear in the diff"
    );
}

#[test]
fn test_promotion_conflicts_on_incompatible_collection_config() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // Both branches independently create the same-named collection with an
    // incompatible dimension — a structural conflict.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create default collection");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create feature collection");

    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("incompatible collection config must conflict under strict");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(error.code(), "conflict.engine.promotion");

    // Strict refused with zero target mutation: default keeps its own config.
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("default collection still present")
            .config()
            .dimension(),
        2,
        "the target's collection config is untouched by a refused promotion"
    );
}

#[test]
fn test_source_wins_refuses_incompatible_collection() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // Both branches independently create the same collection with incompatible
    // dimensions — structurally unmergeable.
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create default collection");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create feature collection");

    // Even SourceWins must refuse: incompatible shapes cannot be merged, and
    // forcing the source config would leave the target mixing dimensions.
    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::SourceWins,
        )
        .expect_err("source-wins must refuse an incompatible collection");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(error.code(), "conflict.engine.promotion");

    // Zero mutation on the structural refusal: the target keeps its own config.
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("default collection still present")
            .config()
            .dimension(),
        2,
        "the target's collection is untouched by the refused source-wins promotion"
    );
}

#[test]
fn test_promotion_carries_collection_in_a_source_only_space() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // feature creates a collection inside a brand-new space, then fills it — the
    // realistic "new namespace + new collection on a branch" flow. The space is
    // carried by the space-registration path and the collection by this one.
    database
        .spaces(branch("feature"))
        .expect("space service opens")
        .create(space("extra"))
        .expect("space create succeeds");
    database
        .vector(branch("feature"), space("extra"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(3, VectorDistanceMetric::Euclidean).expect("valid config"),
        )
        .expect("create collection");
    database
        .vector(branch("feature"), space("extra"))
        .expect("vector service opens")
        .upsert(
            collection(),
            VectorKey::new("v1").expect("key"),
            VectorEmbedding::new(vec![1.0, 2.0, 3.0]).expect("embedding"),
            None,
        )
        .expect("upsert");

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

    // The new space is registered AND the collection in it is usable on target.
    let info = database
        .vector(branch("default"), space("extra"))
        .expect("vector service opens")
        .collection_info(&collection())
        .expect("info succeeds")
        .expect("collection carried into the newly-registered space");
    assert_eq!(info.config().dimension(), 3);
    assert_eq!(info.config().metric(), VectorDistanceMetric::Euclidean);
    assert_eq!(
        database
            .vector(branch("default"), space("extra"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1
    );
}

fn model(name: &str) -> EmbeddingModelId {
    EmbeddingModelId::new(name).expect("model id")
}

fn recorded_model(database: &mut Database, branch_name: &str) -> Option<String> {
    database
        .vector(branch(branch_name), space("default"))
        .expect("vector service opens")
        .collection_info(&collection())
        .expect("info succeeds")
        .expect("emb present")
        .config()
        .embedding_model()
        .map(|model| model.as_str().to_owned())
}

/// Declaring a model is not a reshape. The collection config carries the
/// embedding model since D9, so a byte-level "did the config change" would
/// read a declaration as a dimension/metric change and refuse it under the
/// retained-vectors guard. The vectors the target keeps are exactly the ones a
/// declaration on that branch would have covered, so the promotion carries the
/// declaration and leaves them in place.
#[test]
fn test_a_model_declared_on_the_source_promotes_over_retained_target_vectors() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    upsert(&mut database, "default", "v1", vec![0.0, 1.0]);
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .declare_embedding_model(&collection(), model("miniLM"))
        .expect("declaration succeeds on the source");

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("a declaration promotes without conflict");
    assert!(outcome.conflicts().is_empty());

    assert_eq!(
        recorded_model(&mut database, "default").as_deref(),
        Some("miniLM")
    );
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1,
        "the target's vector is kept, now under the declared model"
    );
}

/// The mirror case for the other guard: the target declared a model and the
/// source carries vectors written under the model-less base config. They land
/// under the declaration exactly as a raw vector write on the target would,
/// which never checks the model either.
#[test]
fn test_a_model_declared_on_the_target_accepts_source_vectors() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .declare_embedding_model(&collection(), model("miniLM"))
        .expect("declaration succeeds on the target");
    upsert(&mut database, "feature", "v1", vec![0.0, 1.0]);

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("vectors promote into a collection that declared a model");
    assert!(outcome.conflicts().is_empty());

    assert_eq!(
        recorded_model(&mut database, "default").as_deref(),
        Some("miniLM")
    );
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1
    );
}

/// Two branches declaring two different models over one model-less base is a
/// real conflict — the very mixing rule 24 exists to refuse — and it is
/// structural: no strategy can pick a side without re-labelling the other's
/// vectors.
#[test]
fn test_two_declared_models_are_an_incompatible_collection() {
    for strategy in [PromotionStrategy::Strict, PromotionStrategy::SourceWins] {
        let mut database = open_cache_database().expect("cache open succeeds");
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .create_collection(
                collection(),
                VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
            )
            .expect("create collection");
        database
            .branches()
            .expect("branch service opens")
            .fork_current(&branch("default"), branch("feature"))
            .expect("fork succeeds");
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .declare_embedding_model(&collection(), model("miniLM"))
            .expect("target declares one model");
        database
            .vector(branch("feature"), space("default"))
            .expect("vector service opens")
            .declare_embedding_model(&collection(), model("nomic-embed"))
            .expect("source declares another");

        let preview = database
            .branches()
            .expect("branch service opens")
            .preview(&branch("feature"), &branch("default"), strategy)
            .expect("preview succeeds");
        assert!(
            preview
                .conflicts()
                .iter()
                .any(|conflict| conflict.kind() == ConflictKind::IncompatibleCollection),
            "{strategy:?}: two declared models are an incompatible collection"
        );
        let error = database
            .branches()
            .expect("branch service opens")
            .promote(&branch("feature"), &branch("default"), strategy)
            .expect_err("two declared models refuse under every strategy");
        assert_eq!(error.class(), EngineErrorClass::Conflict);
        assert_eq!(error.code(), "conflict.engine.promotion");
        assert_eq!(
            recorded_model(&mut database, "default").as_deref(),
            Some("miniLM"),
            "{strategy:?}: the target's declaration is untouched by the refusal"
        );
    }
}

/// The source recreates the collection at the same shape but with a different
/// provenance, over vectors the target keeps. Applying it would re-label the
/// target's `miniLM` vectors as `nomic-embed`, or strip their provenance —
/// each refused as structurally incompatible, like a reshape.
fn source_recreates_over_retained_target_vectors(recreated_model: Option<&str>) {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine)
                .expect("valid config")
                .with_embedding_model(model("miniLM")),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // Written after the fork, so the source's delete does not reach it and the
    // target keeps it through the promotion.
    upsert(&mut database, "default", "t1", vec![0.0, 1.0]);
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete emb");
    let recreated = VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config");
    let recreated = match recreated_model {
        Some(name) => recreated.with_embedding_model(model(name)),
        None => recreated,
    };
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(collection(), recreated)
        .expect("recreate emb with different provenance");

    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("changing provenance over surviving target vectors is refused");
    assert_eq!(error.code(), "conflict.engine.promotion");
    assert_eq!(
        recorded_model(&mut database, "default").as_deref(),
        Some("miniLM"),
        "recreated as {recreated_model:?}: the target keeps its provenance"
    );
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .count(&collection())
            .expect("count succeeds"),
        1
    );
}

#[test]
fn test_a_model_change_over_retained_target_vectors_is_refused() {
    source_recreates_over_retained_target_vectors(Some("nomic-embed"));
}

#[test]
fn test_dropping_the_model_over_retained_target_vectors_is_refused() {
    source_recreates_over_retained_target_vectors(None);
}

/// A metric-only reshape is as incompatible as a dimension change: the same
/// bytes score differently under another metric, so the target's surviving
/// vectors would be ranked by a distance they were never written for.
#[test]
fn test_a_metric_change_over_retained_target_vectors_is_refused() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    // Target-only, so it survives the source's delete and must be protected.
    upsert(&mut database, "default", "t1", vec![0.0, 1.0]);
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .delete_collection(&collection(), true)
        .expect("delete emb");
    database
        .vector(branch("feature"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Euclidean).expect("valid config"),
        )
        .expect("recreate emb with another metric");

    let error = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect_err("a metric change over surviving target vectors is refused");
    assert_eq!(error.code(), "conflict.engine.promotion");
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .collection_info(&collection())
            .expect("info succeeds")
            .expect("emb present")
            .config()
            .metric(),
        VectorDistanceMetric::Cosine
    );
}

/// A promotion reports the embedding the caller upserted, not the row the
/// engine stored (#3202).
///
/// The vector record wraps the embedding in an envelope — a format byte, then
/// a wrapper repeating the collection and key and carrying `vector_revision` —
/// and `applied[].value` carried the whole thing. The issue was filed against
/// JSON and read as JSON-specific because KV looked fine; KV is the one
/// capability that stores exactly what the caller wrote, and vector leaks the
/// same way JSON did.
#[test]
fn a_promoted_vector_reports_the_authored_embedding() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .create_collection(
            collection(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("create collection");
    upsert(&mut database, "default", "v1", vec![0.1, 0.2]);
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    upsert(&mut database, "feature", "v1", vec![0.3, 0.4]);

    let outcome = database
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("strict promote succeeds");

    // The one entity promoted. A vector identity is the capability's own
    // space-relative key (collection and key encoded together), not the bare
    // key, so it is taken rather than matched.
    assert_eq!(outcome.applied().len(), 1, "one vector promoted");
    let value = outcome.applied()[0]
        .value()
        .expect("the promoted vector carries a value");
    let reported: serde_json::Value = serde_json::from_slice(value).unwrap_or_else(|error| {
        panic!(
            "the reported value does not parse as the payload it reports ({error}): {:?}",
            String::from_utf8_lossy(value)
        )
    });

    assert_eq!(
        reported["embedding"]
            .as_array()
            .expect("an embedding array")
            .len(),
        2,
        "reported {reported}"
    );
    for internal in ["vector_revision", "collection", "key"] {
        assert!(
            reported.get(internal).is_none(),
            "the envelope's `{internal}` reached the caller: {reported}"
        );
    }
    assert!(
        !value.iter().any(u8::is_ascii_control),
        "the format byte reached the caller: {value:?}"
    );
}
