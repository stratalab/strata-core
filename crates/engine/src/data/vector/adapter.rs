//! Vector capability branch adapter.
//!
//! A vector entry is a single authored [`RowClass::Vector`] MVCC row: the key is
//! the space prefix followed by the length-prefixed collection name and the user
//! key, and the value is the encoded record. The comparable identity is that
//! collection-qualified, space-relative suffix, so the same key in two
//! collections compares as two distinct entities. Decoding the key through
//! [`decode_vector_key`] inherits the vector capability's foreign-space and
//! malformed-byte diagnostics for free.
//!
//! The derived vector index needs no branch-workflow handling: the query path is
//! exact-correct via its full-collection-scan fallback, and promoted rows land
//! past any index watermark, so a promote that writes authored vector rows keeps
//! search correct without touching the manifest.

use std::collections::{BTreeMap, BTreeSet};

use strata_core::BranchId;

use crate::api::{ComparedCapability, ConflictKind, ConflictStrategyResult, PreviewConflict};
use crate::branch::adapter::{
    CapabilityBranchAdapter, ComparableEntity, DerivedDisposition, EntitySummary,
};
use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::preview::base_point_for;
use crate::control::space::read_space_index_at;
use crate::data::kv::ProductSpace;
use crate::data::vector::{decode_collection_config, VectorCollectionName};
use crate::diagnostics::EngineError;
use crate::persistence::{
    decode_vector_collection_name, decode_vector_key, encode_vector_collection_entry_prefix,
    encode_vector_collection_prefix, encode_vector_space_prefix, PersistenceReadRow, ReadSelector,
    RowAddress, RowClass, RowMutation, StoragePersistence,
};

/// The vector capability's branch adapter.
pub(crate) struct VectorBranchAdapter;

impl CapabilityBranchAdapter for VectorBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::Vector
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_vector_space_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        // Validate the row is a well-formed vector key in this space, rejecting
        // foreign-space and malformed keys with the vector capability's
        // structured diagnostic.
        decode_vector_key(space, row.key())?;
        let identity = row
            .key()
            .strip_prefix(encode_vector_space_prefix(space).as_slice())
            .ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.vector_key",
                    "stored vector row key is outside the requested space",
                )
            })?
            .to_vec();
        let summary = if row.is_tombstone() {
            EntitySummary::Absent
        } else {
            let value = row.value().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.vector_record",
                    "stored vector row is present but carries no value",
                )
            })?;
            EntitySummary::Present(value.to_vec())
        };
        Ok(ComparableEntity::new(
            identity,
            summary,
            row.commit_version(),
        ))
    }
}

/// The vector **collection config** capability's branch adapter (comparison only).
///
/// Collection config rows (`RowClass::VectorCollection`) carry a collection's
/// `(dimension, metric)`, which the vector data adapter above never scans. Without
/// this, `compare` would show no diff for a collection created, deleted, or
/// reshaped that holds no vectors (e.g. an empty collection). It is compare-only
/// (`supports_promotion() == false`): promotion handles collection configs through
/// `plan_collection_promotion`, not this adapter.
pub(crate) struct VectorCollectionBranchAdapter;

impl CapabilityBranchAdapter for VectorCollectionBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::VectorCollection
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn supports_promotion(&self) -> bool {
        false
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_vector_collection_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        // Validate the row is a well-formed collection key in this space.
        decode_vector_collection_name(space, row.key())?;
        let identity = row
            .key()
            .strip_prefix(encode_vector_collection_prefix(space).as_slice())
            .ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.vector_collection_key",
                    "stored vector collection row key is outside the requested space",
                )
            })?
            .to_vec();
        let summary = if row.is_tombstone() {
            EntitySummary::Absent
        } else {
            let value = row.value().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.vector_collection",
                    "stored vector collection row is present but carries no value",
                )
            })?;
            EntitySummary::Present(value.to_vec())
        };
        Ok(ComparableEntity::new(
            identity,
            summary,
            row.commit_version(),
        ))
    }
}

/// Plans carrying `source`'s vector collection **configs** into `target` during a
/// promotion, per space. Collection config rows (`RowClass::VectorCollection`)
/// are not authored vector data, so the capability adapter above never carries
/// them; without this, promoted vectors land behind a missing config and reads
/// fail `not_found.engine.vector_collection`.
///
/// A collection's config is `(dimension, metric, embedding model)`, encoded
/// deterministically, so identical bytes are the same collection and the bytes
/// serve as the identity for the three-way. What the bytes cannot say is
/// whether a *difference* matters to the vectors already stored: dimension and
/// metric are structural — a vector of one shape is unreadable under the other
/// (contract Vector minimum: conflict on metric/dimension) — but the model is
/// provenance, and declaring one over model-less vectors is exactly what
/// `declare_embedding_model` does on a single branch. The two cross-shape
/// guards therefore decode the configs and ask
/// [`vectors_survive_config_change`] rather than comparing bytes. Configs are
/// diffed as a base -> source -> target three-way, exactly like data rows: a
/// change only the source made is applied (carry a source-added config,
/// tombstone one the source deleted); a change only the target made is kept; a
/// change both sides made differently is a conflict — `IncompatibleCollection`
/// (structural, refuses under every strategy) when both hold a divergent
/// config, or `ModifyDeleteDivergence` (strategy-gated) for a modify-vs-delete.
/// A source change that would strand a surviving target vector of the old
/// shape is refused as structurally incompatible rather than mixing shapes.
// One cohesive base->source->target three-way over every collection key, with an
// add/modify/delete case per side plus the two cross-shape guards; splitting it
// would scatter a single decision across helpers. (Mirrors the data-row three-way.)
#[allow(clippy::too_many_lines)]
pub(crate) fn plan_collection_promotion(
    persistence: &mut StoragePersistence,
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
    spaces: &[ProductSpace],
    strategy_result: ConflictStrategyResult,
) -> Result<(Vec<RowMutation>, Vec<PreviewConflict>), EngineError> {
    let (base_branch, base_selector) = base_point_for(source, target)?;
    // Also consider spaces the source deleted entirely (present in the base but
    // absent from the caller's source-space set): their collection configs must be
    // tombstoned too, or they linger as stale metadata inside a deregistered space
    // and resurface if the space is recreated.
    let mut all_spaces: Vec<ProductSpace> = spaces.to_vec();
    for space in read_space_index_at(persistence, base_branch, base_selector)? {
        if !all_spaces.contains(&space) {
            all_spaces.push(space);
        }
    }
    let mut mutations = Vec::new();
    let mut conflicts = Vec::new();
    for space in &all_spaces {
        let prefix = encode_vector_collection_prefix(space);
        let base_configs =
            collection_config_rows_at(persistence, base_branch, &prefix, base_selector)?;
        let source_configs = collection_config_rows(persistence, source, &prefix)?;
        let target_configs = collection_config_rows(persistence, target, &prefix)?;

        // Base -> source -> target three-way over every collection key, exactly
        // like the data-row three-way: only source-side changes propagate, a
        // config change (delete + recreate with a different dimension/metric) that
        // both sides made differently is a conflict, and a change the target never
        // made is applied. A 2-way source-vs-target diff would false-conflict on a
        // target-only reshape or a source-only reshape.
        let mut keys: BTreeSet<&Vec<u8>> = BTreeSet::new();
        keys.extend(base_configs.keys());
        keys.extend(source_configs.keys());
        keys.extend(target_configs.keys());

        for key in keys {
            let base_value = base_configs.get(key);
            let source_value = source_configs.get(key);
            let target_value = target_configs.get(key);

            // Only source-side changes propagate; a target-only change is kept —
            // EXCEPT that a config the source left unchanged can still clash with a
            // target-side reshape: if the target reshaped the collection and the
            // source carries old-shape vectors into it, those vectors would land
            // under the target's new config. The data three-way carries vectors
            // shape-blind, so refuse that here as structurally incompatible. A
            // target that only declared a model is not a reshape: the carried
            // vectors land under it as a raw write on the target would.
            if source_value == base_value {
                if target_value != base_value
                    && !vectors_survive_config_change(space, key, base_value, target_value)?
                    && source_carries_vectors(
                        persistence,
                        base_branch,
                        base_selector,
                        source,
                        space,
                        &decode_vector_collection_name(space, key)?,
                    )?
                {
                    let identity = key
                        .strip_prefix(prefix.as_slice())
                        .unwrap_or(key.as_slice())
                        .to_vec();
                    conflicts.push(PreviewConflict::new(
                        ComparedCapability::Vector,
                        space.clone(),
                        identity,
                        source_value.cloned(),
                        target_value.cloned(),
                        ConflictKind::IncompatibleCollection,
                        ConflictStrategyResult::Refused,
                    ));
                }
                continue;
            }
            // Source and target already agree on the change.
            if source_value == target_value {
                continue;
            }

            let identity = key
                .strip_prefix(prefix.as_slice())
                .unwrap_or(key.as_slice())
                .to_vec();

            // Both sides changed this collection to different states.
            if target_value != base_value {
                if source_value.is_some() && target_value.is_some() {
                    // Two incompatible configs — structural, unmergeable; refuses
                    // under every strategy and carries nothing.
                    conflicts.push(PreviewConflict::new(
                        ComparedCapability::Vector,
                        space.clone(),
                        identity.clone(),
                        source_value.cloned(),
                        target_value.cloned(),
                        ConflictKind::IncompatibleCollection,
                        ConflictStrategyResult::Refused,
                    ));
                    continue;
                }
                // Modify vs delete — strategy-gated like a data-row conflict: only
                // SourceWins carries the source's change; Strict refuses.
                conflicts.push(PreviewConflict::new(
                    ComparedCapability::Vector,
                    space.clone(),
                    identity.clone(),
                    source_value.cloned(),
                    target_value.cloned(),
                    ConflictKind::ModifyDeleteDivergence,
                    strategy_result,
                ));
                if strategy_result != ConflictStrategyResult::SourceWins {
                    continue;
                }
            }

            // Apply the source's resolved state onto the target. Changing or
            // removing a collection that existed must not strand a surviving target
            // vector of the old shape (orphaned behind a missing/mismatched config).
            let retained = base_value.is_some()
                && target_retains_vectors(
                    persistence,
                    base_branch,
                    base_selector,
                    source,
                    target,
                    space,
                    &decode_vector_collection_name(space, key)?,
                )?;
            let address = RowAddress::new(
                target.storage_branch_id(),
                RowClass::VectorCollection,
                key.clone(),
            );
            match source_value {
                None => {
                    // Source deleted the collection: keep it if the target still
                    // holds a surviving vector, otherwise tombstone the config.
                    if retained {
                        continue;
                    }
                    mutations.push(RowMutation::delete(address));
                }
                Some(source_bytes) => {
                    if retained
                        && !vectors_survive_config_change(space, key, target_value, source_value)?
                    {
                        // A reshape that would strand the target's surviving vectors
                        // is structurally incompatible — refuse rather than mix shapes.
                        conflicts.push(PreviewConflict::new(
                            ComparedCapability::Vector,
                            space.clone(),
                            identity,
                            source_value.cloned(),
                            target_value.cloned(),
                            ConflictKind::IncompatibleCollection,
                            ConflictStrategyResult::Refused,
                        ));
                        continue;
                    }
                    mutations.push(RowMutation::put(address, source_bytes.clone()));
                }
            }
        }
    }
    Ok((mutations, conflicts))
}

/// Whether vectors stored under the `held` config may keep living under
/// `incoming` — the question both cross-shape guards ask.
///
/// Dimension and metric must match: a vector of one shape under the other's
/// config is unreadable. The embedding model may be *declared* (none → some),
/// because that is what `declare_embedding_model` does to a collection's
/// existing vectors on a single branch, and a raw vector write never checks
/// the model either; it may not change or be dropped, because vectors recorded
/// as one model's output must not be re-labelled as another's, or lose their
/// provenance (rule 24). A missing side never survives: there is no config for
/// the vectors to live under.
fn vectors_survive_config_change(
    space: &ProductSpace,
    key: &[u8],
    held: Option<&Vec<u8>>,
    incoming: Option<&Vec<u8>>,
) -> Result<bool, EngineError> {
    let (Some(held), Some(incoming)) = (held, incoming) else {
        return Ok(false);
    };
    let collection = decode_vector_collection_name(space, key)?;
    let held = decode_collection_config(&collection, held)?;
    let incoming = decode_collection_config(&collection, incoming)?;
    let same_shape = held.dimension() == incoming.dimension() && held.metric() == incoming.metric();
    let provenance_kept = match (held.embedding_model(), incoming.embedding_model()) {
        (None, _) => true,
        (Some(held), Some(incoming)) => held == incoming,
        (Some(_), None) => false,
    };
    Ok(same_shape && provenance_kept)
}

/// Whether the `target` still holds a live vector in `collection` that this
/// promotion will NOT delete — used to avoid orphaning vectors behind a config
/// the deletion pass would otherwise remove.
///
/// The promotion deletes exactly the vectors the data three-way removes: a
/// base-inherited vector the source deleted (base-live, source-absent). A
/// target-live key that is either absent from the base (target-only, or one the
/// source created-and-deleted post-fork — a net no-op the three-way skips) or
/// still live on the source therefore survives. Vector keys are branch-
/// independent, so the three branches' keys compare directly.
#[allow(clippy::too_many_arguments)]
fn target_retains_vectors(
    persistence: &mut StoragePersistence,
    base_branch: BranchId,
    base_selector: ReadSelector,
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
    space: &ProductSpace,
    collection: &VectorCollectionName,
) -> Result<bool, EngineError> {
    let entry_prefix = encode_vector_collection_entry_prefix(space, collection);
    let target_live = live_vector_keys(
        persistence,
        target.storage_branch_id(),
        &entry_prefix,
        ReadSelector::Latest,
    )?;
    if target_live.is_empty() {
        return Ok(false);
    }
    let base_live = live_vector_keys(persistence, base_branch, &entry_prefix, base_selector)?;
    let source_live = live_vector_keys(
        persistence,
        source.storage_branch_id(),
        &entry_prefix,
        ReadSelector::Latest,
    )?;
    // A target-live vector survives unless it is base-inherited AND the source
    // deleted it (the only case the data three-way propagates as a deletion).
    Ok(target_live
        .iter()
        .any(|key| !base_live.contains(key) || source_live.contains(key)))
}

/// The set of live (non-tombstoned) vector row keys under `entry_prefix` on
/// `storage_branch` at `selector`.
fn live_vector_keys(
    persistence: &mut StoragePersistence,
    storage_branch: BranchId,
    entry_prefix: &[u8],
    selector: ReadSelector,
) -> Result<BTreeSet<Vec<u8>>, EngineError> {
    Ok(persistence
        .scan_prefix(
            storage_branch,
            RowClass::Vector,
            entry_prefix.to_vec(),
            selector,
            None,
        )?
        .into_iter()
        .filter(|row| !row.is_tombstone())
        .map(|row| row.key().to_vec())
        .collect())
}

/// Whether the promotion would carry an old-shape source vector into
/// `collection` — a source-side add or modify (a source-live vector whose value
/// differs from the base). Such a vector, promoted into a collection the target
/// reshaped to a different config, would mismatch the new shape.
fn source_carries_vectors(
    persistence: &mut StoragePersistence,
    base_branch: BranchId,
    base_selector: ReadSelector,
    source: &BranchCatalogRecord,
    space: &ProductSpace,
    collection: &VectorCollectionName,
) -> Result<bool, EngineError> {
    let entry_prefix = encode_vector_collection_entry_prefix(space, collection);
    let base_values: BTreeMap<Vec<u8>, Vec<u8>> = persistence
        .scan_prefix(
            base_branch,
            RowClass::Vector,
            entry_prefix.clone(),
            base_selector,
            None,
        )?
        .into_iter()
        .filter(|row| !row.is_tombstone())
        .filter_map(|row| {
            row.value()
                .map(|value| (row.key().to_vec(), value.to_vec()))
        })
        .collect();
    for row in persistence.scan_prefix(
        source.storage_branch_id(),
        RowClass::Vector,
        entry_prefix,
        ReadSelector::Latest,
        None,
    )? {
        if row.is_tombstone() {
            continue;
        }
        let Some(value) = row.value() else {
            continue;
        };
        // Added (absent from base) or modified (different value) => a source-side
        // change the data three-way will carry as a put.
        if base_values.get(row.key()).map(Vec::as_slice) != Some(value) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Every visible collection config row for `record` under `prefix`, keyed by the
/// full storage key so source and target rows for the same collection align.
fn collection_config_rows(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
    prefix: &[u8],
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, EngineError> {
    collection_config_rows_at(
        persistence,
        record.storage_branch_id(),
        prefix,
        ReadSelector::Latest,
    )
}

/// Reads the live vector-collection config rows of `storage_branch` under `prefix`
/// at `selector` (e.g. a promotion base point). Tombstoned (deleted) collections
/// are skipped; a base→source diff over the returned keys detects source-side
/// deletions.
fn collection_config_rows_at(
    persistence: &mut StoragePersistence,
    storage_branch: BranchId,
    prefix: &[u8],
    selector: ReadSelector,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, EngineError> {
    let rows = persistence.scan_prefix(
        storage_branch,
        RowClass::VectorCollection,
        prefix.to_vec(),
        selector,
        None,
    )?;
    let mut configs = BTreeMap::new();
    for row in &rows {
        if row.is_tombstone() {
            continue;
        }
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.vector_collection",
                "stored vector collection row is present but carries no value",
            )
        })?;
        configs.insert(row.key().to_vec(), value.to_vec());
    }
    Ok(configs)
}

#[cfg(test)]
mod tests {
    use super::VectorBranchAdapter;

    use strata_core::CommitVersion;

    use crate::branch::adapter::{CapabilityBranchAdapter, DerivedDisposition, EntitySummary};
    use crate::data::kv::ProductSpace;
    use crate::data::vector::{VectorCollectionName, VectorKey};
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::{
        encode_vector_key, encode_vector_space_prefix, PersistenceReadRow, RowClass,
    };

    fn space() -> ProductSpace {
        ProductSpace::new("default").expect("default is a valid space")
    }

    fn vector_row(
        space: &ProductSpace,
        collection: &str,
        key: &str,
        value: Option<&[u8]>,
        tombstone: bool,
    ) -> PersistenceReadRow {
        let encoded = encode_vector_key(
            space,
            &VectorCollectionName::new(collection).expect("valid collection"),
            &VectorKey::new(key).expect("valid key"),
        );
        PersistenceReadRow::for_test(encoded, value.map(<[u8]>::to_vec), tombstone)
    }

    #[test]
    fn interpret_row_decodes_a_present_vector_row() {
        let space = space();
        let row = vector_row(&space, "emb", "alpha", Some(b"record-bytes"), false);
        let entity = VectorBranchAdapter
            .interpret_row(&space, &row)
            .expect("decodes a present vector row");
        assert_eq!(
            entity.summary(),
            &EntitySummary::Present(b"record-bytes".to_vec())
        );
        assert_eq!(entity.version(), CommitVersion::new(1));
        assert!(!entity.is_tombstone());
    }

    #[test]
    fn interpret_row_maps_a_vector_tombstone_to_absent() {
        let space = space();
        let row = vector_row(&space, "emb", "gone", None, true);
        let entity = VectorBranchAdapter
            .interpret_row(&space, &row)
            .expect("decodes a tombstone");
        assert!(entity.is_tombstone());
        assert_eq!(entity.summary(), &EntitySummary::Absent);
    }

    #[test]
    fn identity_distinguishes_the_same_key_in_different_collections() {
        let space = space();
        let a = VectorBranchAdapter
            .interpret_row(&space, &vector_row(&space, "one", "k", Some(b"x"), false))
            .expect("decodes");
        let b = VectorBranchAdapter
            .interpret_row(&space, &vector_row(&space, "two", "k", Some(b"x"), false))
            .expect("decodes");
        assert_ne!(
            a.identity(),
            b.identity(),
            "the collection is part of the comparable identity"
        );
    }

    #[test]
    fn interpret_row_rejects_a_key_from_another_space() {
        let other = ProductSpace::new("other").expect("other is a valid space");
        let row = vector_row(&other, "emb", "alpha", Some(b"x"), false);
        let error = VectorBranchAdapter
            .interpret_row(&space(), &row)
            .expect_err("a key encoded for another space is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.vector_key");
    }

    #[test]
    fn interpret_row_rejects_a_present_row_without_a_value() {
        let space = space();
        let row = vector_row(&space, "emb", "beta", None, false);
        let error = VectorBranchAdapter
            .interpret_row(&space, &row)
            .expect_err("a present row without a value is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.vector_record");
    }

    #[test]
    fn space_prefix_matches_the_vector_encoding() {
        let space = space();
        assert_eq!(
            VectorBranchAdapter.space_prefix(&space),
            encode_vector_space_prefix(&space)
        );
    }

    #[test]
    fn row_class_and_disposition_are_reported() {
        assert_eq!(VectorBranchAdapter.row_class(), RowClass::Vector);
        assert_eq!(
            VectorBranchAdapter.derived_disposition(),
            DerivedDisposition::Authored
        );
    }
}
