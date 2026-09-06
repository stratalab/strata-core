//! Vector service.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
#[cfg(any(test, feature = "testkit"))]
use strata_core::BranchId;
use strata_core::{CommitVersion, Timestamp};

use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::BranchName;
use crate::commit::CommitOutcome;
use crate::control::ControlPlane;
use crate::data::kv::ProductSpace;
use crate::diagnostics::{EngineError, EngineResult};
use crate::persistence::{
    decode_vector_collection_name, decode_vector_key, encode_vector_collection_entry_prefix,
    encode_vector_collection_key, encode_vector_collection_prefix, encode_vector_key,
    vector_index_manifest_key, CommitPlan, PersistenceImmutableSource, PersistenceReadRow,
    ReadSelector, RowAddress, RowClass, RowMutation, StoragePersistence,
};

use super::{
    decode_collection_config, decode_vector_index_manifest, decode_vector_record,
    default_flat_artifact_build_budget_bytes, default_hnsw_artifact_build_budget_bytes,
    encode_collection_config, encode_vector_index_manifest, encode_vector_record,
    query_vector_sources_with_index_artifacts, FlatVectorArtifact, HnswArtifactConfig,
    HnswVectorArtifact, VectorArtifactId, VectorArtifactKind, VectorArtifactRef,
    VectorArtifactSourceDiagnostic, VectorArtifactStore, VectorBatchDeleteOutcome,
    VectorBatchGetOutcome, VectorBatchUpsertOutcome, VectorBulkDeleteOutcome, VectorCollectionInfo,
    VectorCollectionName, VectorConfig, VectorDeleteOutcome, VectorEmbedding, VectorEntry,
    VectorFilter, VectorFlatArtifactIdentity, VectorFlatArtifactSourceInput, VectorHistory,
    VectorHistoryRow, VectorHnswArtifactSourceInput, VectorIndexDiagnostics, VectorIndexManifest,
    VectorIndexManifestLookup, VectorIndexPolicy, VectorKey, VectorKeyPage, VectorMetadata,
    VectorMetadataPatch, VectorMetadataUpdateOutcome, VectorRecord, VectorSearchResult,
    VectorSourceId, VectorTombstone, VectorUpsertEntry, VectorVersionedEntry, VectorWriteOutcome,
};

#[cfg(any(test, feature = "testkit"))]
use super::query_vector_exact;
#[cfg(any(test, feature = "testkit"))]
use super::VectorDistanceMetric;
#[cfg(any(test, feature = "testkit"))]
use super::{default_flat_artifact_load_budget_bytes, default_hnsw_artifact_load_budget_bytes};

const VECTOR_LIST_RAW_PAGE_MIN: usize = 64;
const VECTOR_LIST_RAW_PAGE_MAX: usize = 4096;

struct VectorIndexManifestResolution {
    lookup: VectorIndexManifestLookup,
    artifact_refs: Vec<VectorArtifactRef>,
}

impl VectorIndexManifestResolution {
    /// The boundary at or below which every row is represented by a sealed artifact; rows past it
    /// are the active delta. A seal captures one atomic snapshot into its sources and replaces the
    /// manifest wholesale, so all rows up to the *highest* covered version across the refs are
    /// sealed — hence `max`. (Using `min` would treat rows covered by a newer source as active and
    /// re-scan most of a multi-source collection.) `None` when nothing is sealed.
    fn active_delta_watermark(&self) -> Option<CommitVersion> {
        self.artifact_refs
            .iter()
            .map(VectorArtifactRef::covered_commit_version)
            .max()
    }

    /// Estimated number of distinct sealed vectors, counting each source once (flat and HNSW refs
    /// for the same source cover the same vectors). Used as the index-kind threshold input so the
    /// bounded active-delta read does not need a whole-collection scan to size the collection.
    fn indexed_vector_count(&self) -> usize {
        let mut seen = std::collections::BTreeSet::new();
        let mut total = 0usize;
        for artifact_ref in &self.artifact_refs {
            if seen.insert(artifact_ref.source_id().to_owned()) {
                total = total.saturating_add(
                    usize::try_from(artifact_ref.vector_count()).unwrap_or(usize::MAX),
                );
            }
        }
        total
    }
}

struct VectorQuerySnapshot {
    entries: Vec<(PersistenceReadRow, VectorEntry)>,
    tombstones: Vec<VectorTombstone>,
}

struct VectorIndexArtifactLoadSet {
    flat_artifacts: Vec<VectorFlatArtifactSourceInput>,
    hnsw_artifacts: Vec<VectorHnswArtifactSourceInput>,
    artifact_reports: Vec<VectorArtifactSourceDiagnostic>,
    flat_unavailable_reason: Option<&'static str>,
    hnsw_unavailable_reason: Option<&'static str>,
    hnsw_graph_builds: usize,
}

impl VectorIndexArtifactLoadSet {
    const fn unavailable(reason: &'static str) -> Self {
        Self {
            flat_artifacts: Vec::new(),
            hnsw_artifacts: Vec::new(),
            artifact_reports: Vec::new(),
            flat_unavailable_reason: Some(reason),
            hnsw_unavailable_reason: Some(reason),
            hnsw_graph_builds: 0,
        }
    }
}

struct VectorSourceSelectedRow {
    row: PersistenceReadRow,
    entry: Option<VectorEntry>,
}

/// Service for vector collection and exact search operations.
pub struct VectorService<'a> {
    persistence: &'a mut StoragePersistence,
    control: &'a mut ControlPlane,
    #[cfg_attr(not(any(test, feature = "testkit")), allow(dead_code))]
    artifacts: &'a mut VectorArtifactStore,
    branch: BranchName,
    space: ProductSpace,
}

impl<'a> VectorService<'a> {
    pub(crate) const fn new(
        persistence: &'a mut StoragePersistence,
        control: &'a mut ControlPlane,
        artifacts: &'a mut VectorArtifactStore,
        branch: BranchName,
        space: ProductSpace,
    ) -> Self {
        Self {
            persistence,
            control,
            artifacts,
            branch,
            space,
        }
    }

    /// Creates a vector collection.
    pub fn create_collection(
        &mut self,
        name: VectorCollectionName,
        config: VectorConfig,
    ) -> EngineResult<VectorCollectionInfo> {
        let record = self.branch_record()?;
        let address = self.collection_address(&record, &name);
        if self
            .persistence
            .read_row(address.clone(), ReadSelector::Latest)?
            .is_some_and(|row| !row.is_tombstone())
        {
            return Err(EngineError::conflict(
                "already_exists.engine.vector_collection",
                "vector collection already exists",
            ));
        }
        let commit = self.commit_batch(
            &record,
            vec![RowMutation::put(
                address,
                encode_collection_config(&name, config)?,
            )],
        )?;
        Ok(VectorCollectionInfo::new(
            name,
            config,
            0,
            commit.version(),
            commit.timestamp(),
        ))
    }

    /// Deletes a vector collection and all currently visible vectors in it.
    pub fn delete_collection(&mut self, name: &VectorCollectionName) -> EngineResult<bool> {
        let record = self.branch_record()?;
        let Some(_) = self.collection_config_row(&record, name, ReadSelector::Latest)? else {
            return Ok(false);
        };
        let mut mutations = vec![RowMutation::delete(self.collection_address(&record, name))];
        for row in self.vector_rows(&record, name, ReadSelector::Latest)? {
            if row.is_tombstone() {
                continue;
            }
            mutations.push(RowMutation::delete(RowAddress::new(
                record.storage_branch_id(),
                RowClass::Vector,
                row.key().to_vec(),
            )));
        }
        self.commit_batch(&record, mutations)?;
        Ok(true)
    }

    /// Lists visible vector collections.
    pub fn list_collections(&mut self) -> EngineResult<Vec<VectorCollectionInfo>> {
        let record = self.branch_record()?;
        let mut collections = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::VectorCollection,
                encode_vector_collection_prefix(&self.space),
                ReadSelector::Latest,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| self.collection_info_from_row(&record, &row))
            .collect::<EngineResult<Vec<_>>>()?;
        collections.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(collections)
    }

    /// Returns visible collection info.
    pub fn collection_info(
        &mut self,
        name: &VectorCollectionName,
    ) -> EngineResult<Option<VectorCollectionInfo>> {
        let record = self.branch_record()?;
        self.collection_config_row(&record, name, ReadSelector::Latest)?
            .map(|row| self.collection_info_from_row(&record, &row))
            .transpose()
    }

    /// Samples up to `count` vectors from a collection using a deterministic
    /// stride over the ordered live rows. Returns the total live count and the
    /// sample.
    pub fn sample(
        &mut self,
        collection: &VectorCollectionName,
        count: usize,
    ) -> EngineResult<(u64, Vec<VectorVersionedEntry>)> {
        let entries = self.scan(collection, None, None)?;
        let total_count = u64::try_from(entries.len()).unwrap_or(u64::MAX);
        if count == 0 || entries.is_empty() {
            return Ok((total_count, Vec::new()));
        }
        if count >= entries.len() {
            return Ok((total_count, entries));
        }
        let row_count = entries.len();
        let sampled = (0..count)
            .map(|index| entries[(index * row_count) / count].clone())
            .collect();
        Ok((total_count, sampled))
    }

    /// Counts visible vectors in a collection.
    pub fn count(&mut self, name: &VectorCollectionName) -> EngineResult<u64> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, name)?;
        self.count_with_record(&record, name, ReadSelector::Latest)
    }

    /// Counts vectors visible at a commit timestamp in a collection.
    pub fn count_at(
        &mut self,
        name: &VectorCollectionName,
        timestamp: Timestamp,
    ) -> EngineResult<u64> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, name)?;
        self.count_with_record(&record, name, ReadSelector::AtTimestamp(timestamp))
    }

    /// Upserts one vector entry.
    pub fn upsert(
        &mut self,
        collection: VectorCollectionName,
        key: VectorKey,
        embedding: VectorEmbedding,
        metadata: Option<VectorMetadata>,
    ) -> EngineResult<VectorWriteOutcome> {
        let record = self.branch_record()?;
        let config = self.require_collection_config(&record, &collection)?;
        embedding.validate_dimension(config.dimension())?;
        let visible = self.visible_vector_exists(&record, &collection, &key)?;
        let revision = self
            .latest_vector_revision(&record, &collection, &key)?
            .unwrap_or(0)
            .saturating_add(1);
        let address = self.vector_address(&record, &collection, &key);
        let vector = VectorRecord::new(collection, key, embedding, metadata, revision);
        let commit = self.commit_batch(
            &record,
            vec![RowMutation::put(address, encode_vector_record(&vector)?)],
        )?;
        Ok(VectorWriteOutcome::new(commit, revision, !visible))
    }

    /// Reads the latest visible vector entry.
    pub fn get(
        &mut self,
        collection: &VectorCollectionName,
        key: &VectorKey,
    ) -> EngineResult<Option<VectorEntry>> {
        Ok(self
            .get_versioned(collection, key)?
            .map(|entry| entry.entry().clone()))
    }

    /// Reads the latest visible vector entry with commit metadata.
    pub fn get_versioned(
        &mut self,
        collection: &VectorCollectionName,
        key: &VectorKey,
    ) -> EngineResult<Option<VectorVersionedEntry>> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, collection)?;
        let address = self.vector_address(&record, collection, key);
        let Some(row) = self.persistence.read_row(address, ReadSelector::Latest)? else {
            return Ok(None);
        };
        Self::versioned_entry_from_row(collection, key, &row)
    }

    /// Reads a vector entry at a commit version.
    pub fn get_at_version(
        &mut self,
        collection: &VectorCollectionName,
        key: &VectorKey,
        version: CommitVersion,
    ) -> EngineResult<Option<VectorVersionedEntry>> {
        let record = self.branch_record()?;
        self.require_collection_config_with_selector(
            &record,
            collection,
            ReadSelector::AtVersion(version),
        )?;
        let address = self.vector_address(&record, collection, key);
        let Some(row) = self
            .persistence
            .read_row(address, ReadSelector::AtVersion(version))?
        else {
            return Ok(None);
        };
        Self::versioned_entry_from_row(collection, key, &row)
    }

    /// Reads a vector entry at a commit timestamp.
    pub fn get_at(
        &mut self,
        collection: &VectorCollectionName,
        key: &VectorKey,
        timestamp: Timestamp,
    ) -> EngineResult<Option<VectorVersionedEntry>> {
        let record = self.branch_record()?;
        self.require_collection_config_with_selector(
            &record,
            collection,
            ReadSelector::AtTimestamp(timestamp),
        )?;
        let address = self.vector_address(&record, collection, key);
        let Some(row) = self
            .persistence
            .read_row(address, ReadSelector::AtTimestamp(timestamp))?
        else {
            return Ok(None);
        };
        Self::versioned_entry_from_row(collection, key, &row)
    }

    /// #3112 S4: joins each history row to its commit's wall-clock instant.
    ///
    /// One batched lookup for the whole history rather than one per row: the
    /// question is inherently plural, and the index answers it under a single
    /// lock. Rows keep their order, and a row whose instant is unknown keeps
    /// `None` — history stays exact even when its dates are not available.
    fn attach_committed_at(
        &mut self,
        record: &BranchCatalogRecord,
        rows: Vec<VectorHistoryRow>,
    ) -> EngineResult<Vec<VectorHistoryRow>> {
        let versions: Vec<_> = rows.iter().map(VectorHistoryRow::version).collect();
        let instants = self
            .persistence
            .committed_at_for_versions(record.storage_branch_id(), &versions)?;
        Ok(rows
            .into_iter()
            .zip(instants)
            .map(|(row, instant)| row.with_committed_at(instant))
            .collect())
    }

    /// Reads full vector history newest-first.
    pub fn history(
        &mut self,
        collection: &VectorCollectionName,
        key: &VectorKey,
    ) -> EngineResult<Option<VectorHistory>> {
        let record = self.branch_record()?;
        self.require_collection_config_history(&record, collection)?;
        let address = self.vector_address(&record, collection, key);
        let rows = self
            .persistence
            .read_history(&address, true)?
            .into_iter()
            .map(|row| Self::history_row_from_row(collection, key, &row))
            .collect::<EngineResult<Vec<_>>>()?;
        // #3112 S4: instants are commit-scoped, so they cannot ride on the
        // rows — join them by commit version after the read.
        let rows = self.attach_committed_at(&record, rows)?;
        Ok((!rows.is_empty()).then(|| VectorHistory::new(rows)))
    }

    /// Returns true when the vector key has a latest visible value.
    pub fn exists(
        &mut self,
        collection: &VectorCollectionName,
        key: &VectorKey,
    ) -> EngineResult<bool> {
        Ok(self.get_versioned(collection, key)?.is_some())
    }

    /// Checks multiple vector keys for latest visible values.
    ///
    /// The collection config is validated once up front (surfacing
    /// `not_found.engine.vector_collection`); each key then reuses the same
    /// single-key existence check that [`exists`](Self::exists) resolves to.
    pub fn batch_exists(
        &mut self,
        collection: &VectorCollectionName,
        keys: &[VectorKey],
    ) -> EngineResult<Vec<bool>> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, collection)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.visible_vector_exists(&record, collection, key)?);
        }
        Ok(results)
    }

    /// Lists visible vector keys with cursor pagination.
    pub fn list_keys(
        &mut self,
        collection: &VectorCollectionName,
        prefix: Option<&VectorKey>,
        cursor: Option<&VectorKey>,
        limit: usize,
    ) -> EngineResult<VectorKeyPage> {
        self.list_keys_with_selector(collection, prefix, cursor, limit, ReadSelector::Latest)
    }

    /// Lists vector keys visible at a commit timestamp with cursor pagination.
    pub fn list_keys_at(
        &mut self,
        collection: &VectorCollectionName,
        prefix: Option<&VectorKey>,
        cursor: Option<&VectorKey>,
        limit: usize,
        timestamp: Timestamp,
    ) -> EngineResult<VectorKeyPage> {
        self.list_keys_with_selector(
            collection,
            prefix,
            cursor,
            limit,
            ReadSelector::AtTimestamp(timestamp),
        )
    }

    fn list_keys_with_selector(
        &mut self,
        collection: &VectorCollectionName,
        prefix: Option<&VectorKey>,
        cursor: Option<&VectorKey>,
        limit: usize,
        selector: ReadSelector,
    ) -> EngineResult<VectorKeyPage> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, collection)?;
        if limit == 0 {
            return Ok(VectorKeyPage::new(Vec::new(), false, None));
        }
        let mut keys = self.keys_after_cursor(
            &record,
            collection,
            prefix,
            cursor,
            limit.saturating_add(1),
            selector,
        )?;
        let has_more = keys.len() > limit;
        if has_more {
            keys.truncate(limit);
        }
        let cursor = has_more.then(|| keys.last().expect("non-empty page").clone());
        Ok(VectorKeyPage::new(keys, has_more, cursor))
    }

    /// Scans latest visible vector entries from an inclusive start key.
    ///
    /// Mirrors the KV scan: the range is bounded to the collection's entry
    /// prefix, tombstones are skipped, and a `Some(limit)` request refills
    /// across tombstones so a caller that reads `limit + 1` rows can page
    /// honestly.
    pub fn scan(
        &mut self,
        collection: &VectorCollectionName,
        start: Option<&VectorKey>,
        limit: Option<usize>,
    ) -> EngineResult<Vec<VectorVersionedEntry>> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, collection)?;
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let prefix = encode_vector_collection_entry_prefix(&self.space, collection);
        let start = start.map_or_else(
            || prefix.clone(),
            |key| encode_vector_key(&self.space, collection, key),
        );
        let end = next_prefix(&prefix);
        if start >= end {
            return Ok(Vec::new());
        }
        match limit {
            Some(limit) => self.scan_range_limited(&record, collection, start, &end, limit),
            None => self
                .persistence
                .scan_range(
                    record.storage_branch_id(),
                    RowClass::Vector,
                    Some(start),
                    Some(end),
                    ReadSelector::Latest,
                    None,
                )?
                .into_iter()
                .filter(|row| !row.is_tombstone())
                .map(|row| self.scan_entry_from_row(collection, &row))
                .collect(),
        }
    }

    /// Updates top-level vector metadata fields.
    pub fn update_metadata(
        &mut self,
        collection: &VectorCollectionName,
        key: VectorKey,
        patch: &VectorMetadataPatch,
    ) -> EngineResult<VectorMetadataUpdateOutcome> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, collection)?;
        let address = self.vector_address(&record, collection, &key);
        let Some(row) = self
            .persistence
            .read_row(address.clone(), ReadSelector::Latest)?
        else {
            return Ok(VectorMetadataUpdateOutcome::new(key, false, None, None));
        };
        if row.is_tombstone() {
            return Ok(VectorMetadataUpdateOutcome::new(key, false, None, None));
        }
        let current = Self::vector_record_from_row(collection, &key, &row)?;
        let mut object = match current.metadata().map(VectorMetadata::as_inner) {
            None => serde_json::Map::new(),
            Some(Value::Object(object)) => object.clone(),
            Some(_) => {
                return Err(EngineError::invalid_input(
                    "invalid_argument.engine.vector_metadata_patch",
                    "vector metadata patch requires existing object metadata",
                ));
            }
        };
        for (field, value) in patch.fields() {
            object.insert(field.clone(), value.clone());
        }
        let metadata = VectorMetadata::new(Value::Object(object))?;
        let revision = current.vector_revision().saturating_add(1);
        let vector = VectorRecord::new(
            collection.clone(),
            key.clone(),
            current.embedding().clone(),
            Some(metadata),
            revision,
        );
        let commit = self.commit_batch(
            &record,
            vec![RowMutation::put(address, encode_vector_record(&vector)?)],
        )?;
        Ok(VectorMetadataUpdateOutcome::new(
            key,
            true,
            Some(revision),
            Some(commit),
        ))
    }

    /// Deletes one vector if present.
    pub fn delete(
        &mut self,
        collection: &VectorCollectionName,
        key: VectorKey,
    ) -> EngineResult<VectorDeleteOutcome> {
        let outcome = self.batch_delete(collection, std::slice::from_ref(&key))?;
        let deleted = outcome.deleted().first().copied().unwrap_or(false);
        Ok(VectorDeleteOutcome::new(key, deleted, outcome.commit()))
    }

    /// Deletes visible vectors matching a non-empty metadata filter.
    pub fn delete_by_filter(
        &mut self,
        collection: &VectorCollectionName,
        filter: &VectorFilter,
    ) -> EngineResult<VectorBulkDeleteOutcome> {
        if filter.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_filter",
                "vector metadata filter must not be empty for filtered delete",
            ));
        }
        self.delete_matching(collection, |entry| filter.matches(entry.metadata()))
    }

    /// Deletes every visible vector in a collection.
    pub fn delete_all(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<VectorBulkDeleteOutcome> {
        self.delete_matching(collection, |_| true)
    }

    /// Upserts multiple vector entries in one commit.
    pub fn batch_upsert(
        &mut self,
        collection: &VectorCollectionName,
        entries: &[VectorUpsertEntry],
    ) -> EngineResult<VectorBatchUpsertOutcome> {
        let record = self.branch_record()?;
        let config = self.require_collection_config(&record, collection)?;
        if entries.is_empty() {
            return Ok(VectorBatchUpsertOutcome::new(Vec::new(), Vec::new(), None));
        }
        for entry in entries {
            entry.embedding().validate_dimension(config.dimension())?;
        }

        let mut revisions_by_key = BTreeMap::<VectorKey, u64>::new();
        let mut seen_in_batch = BTreeSet::<VectorKey>::new();
        let mut final_records = BTreeMap::<VectorKey, VectorRecord>::new();
        let mut revisions = Vec::with_capacity(entries.len());
        let mut created = Vec::with_capacity(entries.len());
        for entry in entries {
            let previous_revision = if let Some(revision) = revisions_by_key.get(entry.key()) {
                *revision
            } else {
                self.latest_vector_revision(&record, collection, entry.key())?
                    .unwrap_or(0)
            };
            let existed_before_item = if seen_in_batch.contains(entry.key()) {
                true
            } else {
                self.visible_vector_exists(&record, collection, entry.key())?
            };
            let revision = previous_revision.saturating_add(1);
            revisions_by_key.insert(entry.key().clone(), revision);
            seen_in_batch.insert(entry.key().clone());
            revisions.push(revision);
            created.push(!existed_before_item);
            final_records.insert(
                entry.key().clone(),
                VectorRecord::new(
                    collection.clone(),
                    entry.key().clone(),
                    entry.embedding().clone(),
                    entry.metadata().cloned(),
                    revision,
                ),
            );
        }
        let mutations = final_records
            .into_iter()
            .map(|(key, vector)| {
                Ok(RowMutation::put(
                    self.vector_address(&record, collection, &key),
                    encode_vector_record(&vector)?,
                ))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        let commit = self.commit_batch(&record, mutations)?;
        Ok(VectorBatchUpsertOutcome::new(
            revisions,
            created,
            Some(commit),
        ))
    }

    /// Reads multiple latest visible vector entries with metadata.
    pub fn batch_get(
        &mut self,
        collection: &VectorCollectionName,
        keys: &[VectorKey],
    ) -> EngineResult<VectorBatchGetOutcome> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, collection)?;
        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            let address = self.vector_address(&record, collection, key);
            let entry = match self.persistence.read_row(address, ReadSelector::Latest)? {
                Some(row) => Self::versioned_entry_from_row(collection, key, &row)?,
                None => None,
            };
            entries.push(entry);
        }
        Ok(VectorBatchGetOutcome::new(entries))
    }

    /// Deletes multiple vector keys in one commit.
    pub fn batch_delete(
        &mut self,
        collection: &VectorCollectionName,
        keys: &[VectorKey],
    ) -> EngineResult<VectorBatchDeleteOutcome> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, collection)?;
        if keys.is_empty() {
            return Ok(VectorBatchDeleteOutcome::new(Vec::new(), None));
        }
        let mut seen = BTreeSet::<VectorKey>::new();
        let mut mutations = Vec::new();
        let mut deleted = Vec::with_capacity(keys.len());
        for key in keys {
            if !seen.insert(key.clone()) {
                deleted.push(false);
                continue;
            }
            let address = self.vector_address(&record, collection, key);
            let exists = self
                .persistence
                .read_row(address.clone(), ReadSelector::Latest)?
                .is_some_and(|row| !row.is_tombstone());
            if exists {
                mutations.push(RowMutation::delete(address));
            }
            deleted.push(exists);
        }
        if mutations.is_empty() {
            return Ok(VectorBatchDeleteOutcome::new(deleted, None));
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(VectorBatchDeleteOutcome::new(deleted, Some(commit)))
    }

    /// Runs latest nearest-neighbor search.
    pub fn query(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
    ) -> EngineResult<VectorSearchResult> {
        self.query_with_selector(collection, query, k, filter, ReadSelector::Latest)
    }

    /// Runs nearest-neighbor search at a timestamp.
    pub fn query_at(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        timestamp: Timestamp,
    ) -> EngineResult<VectorSearchResult> {
        self.query_with_selector(
            collection,
            query,
            k,
            filter,
            ReadSelector::AtTimestamp(timestamp),
        )
    }

    /// Searches with the default index policy and returns planner diagnostics.
    pub fn query_with_index_diagnostics(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
    ) -> EngineResult<(VectorSearchResult, VectorIndexDiagnostics)> {
        self.query_with_selector_and_policy(
            collection,
            query,
            k,
            filter,
            ReadSelector::Latest,
            VectorIndexPolicy::default(),
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Searches with the default index policy and returns planner diagnostics.
    pub fn query_with_index_diagnostics_for_test(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
    ) -> EngineResult<(VectorSearchResult, VectorIndexDiagnostics)> {
        self.query_with_index_diagnostics(collection, query, k, filter)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Searches with an explicit index policy and returns planner diagnostics.
    pub fn query_with_index_policy_for_test(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        policy: VectorIndexPolicy,
    ) -> EngineResult<(VectorSearchResult, VectorIndexDiagnostics)> {
        self.query_with_selector_and_policy(
            collection,
            query,
            k,
            filter,
            ReadSelector::Latest,
            policy,
        )
    }

    /// Searches at a timestamp with the default index policy and returns planner diagnostics.
    pub fn query_at_with_index_diagnostics(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        timestamp: Timestamp,
    ) -> EngineResult<(VectorSearchResult, VectorIndexDiagnostics)> {
        self.query_with_selector_and_policy(
            collection,
            query,
            k,
            filter,
            ReadSelector::AtTimestamp(timestamp),
            VectorIndexPolicy::default(),
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Searches at a timestamp with the default index policy and returns planner diagnostics.
    pub fn query_at_with_index_diagnostics_for_test(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        timestamp: Timestamp,
    ) -> EngineResult<(VectorSearchResult, VectorIndexDiagnostics)> {
        self.query_at_with_index_diagnostics(collection, query, k, filter, timestamp)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Searches at a timestamp with an explicit index policy and returns planner diagnostics.
    pub fn query_at_with_index_policy_for_test(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        timestamp: Timestamp,
        policy: VectorIndexPolicy,
    ) -> EngineResult<(VectorSearchResult, VectorIndexDiagnostics)> {
        self.query_with_selector_and_policy(
            collection,
            query,
            k,
            filter,
            ReadSelector::AtTimestamp(timestamp),
            policy,
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Searches latest committed rows through the exact baseline helper.
    pub fn query_exact_for_test(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
    ) -> EngineResult<VectorSearchResult> {
        self.query_exact_with_selector(collection, query, k, filter, ReadSelector::Latest)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Searches timestamp-visible rows through the exact baseline helper.
    pub fn query_at_exact_for_test(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        timestamp: Timestamp,
    ) -> EngineResult<VectorSearchResult> {
        self.query_exact_with_selector(
            collection,
            query,
            k,
            filter,
            ReadSelector::AtTimestamp(timestamp),
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Stores an empty branch-local vector index manifest for diagnostics tests.
    pub fn seed_empty_index_manifest_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<()> {
        self.seed_index_manifest_for_test(collection, Vec::new(), 0)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Returns artifact IDs from the current branch-local index manifest.
    pub fn index_manifest_artifact_ids_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<Vec<String>> {
        let resolution = self.index_manifest_resolution_for_test(collection)?;
        Ok(resolution
            .artifact_refs
            .iter()
            .map(|artifact_ref| artifact_ref.artifact_id().to_owned())
            .collect())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Returns the byte size of the current branch-local vector index manifest row.
    pub fn index_manifest_byte_len_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<Option<usize>> {
        let record = self.branch_record()?;
        let address = self.index_manifest_address(&record, collection);
        Ok(self
            .persistence
            .read(address, ReadSelector::Latest)?
            .map(|bytes| bytes.len()))
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Removes artifact payloads referenced by the current manifest.
    pub fn remove_manifest_flat_artifacts_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<usize> {
        let artifact_ids = self.index_manifest_flat_artifact_ids_for_test(collection)?;
        for artifact_id in &artifact_ids {
            self.artifacts.remove_for_test(artifact_id)?;
        }
        Ok(artifact_ids.len())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Replaces artifact payloads referenced by the current manifest with malformed bytes.
    pub fn corrupt_manifest_flat_artifacts_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<usize> {
        let artifact_ids = self.index_manifest_flat_artifact_ids_for_test(collection)?;
        for artifact_id in &artifact_ids {
            self.artifacts
                .put_raw_flat_for_test(artifact_id.clone(), vec![1, 2, 3])?;
        }
        Ok(artifact_ids.len())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Removes HNSW artifact payloads referenced by the current manifest.
    pub fn remove_manifest_hnsw_artifacts_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<usize> {
        let artifact_ids = self.index_manifest_hnsw_artifact_ids_for_test(collection)?;
        for artifact_id in &artifact_ids {
            self.artifacts.remove_hnsw_for_test(artifact_id)?;
        }
        Ok(artifact_ids.len())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Replaces HNSW artifact payloads referenced by the current manifest with malformed bytes.
    pub fn corrupt_manifest_hnsw_artifacts_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<usize> {
        let artifact_ids = self.index_manifest_hnsw_artifact_ids_for_test(collection)?;
        for artifact_id in &artifact_ids {
            self.artifacts
                .put_raw_hnsw_for_test(artifact_id.clone(), vec![1, 2, 3])?;
        }
        Ok(artifact_ids.len())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Replaces manifest artifact payloads with payloads whose identity no longer matches.
    pub fn make_manifest_flat_artifacts_stale_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<usize> {
        let resolution = self.index_manifest_resolution_for_test(collection)?;
        let (_, collection_generation) = {
            let record = self.branch_record()?;
            self.require_collection_config_with_generation(
                &record,
                collection,
                ReadSelector::Latest,
            )?
        };
        let mut replaced = 0;
        for artifact_ref in resolution
            .artifact_refs
            .iter()
            .filter(|artifact_ref| artifact_ref.index_kind() == VectorArtifactKind::Flat)
        {
            let identity = VectorFlatArtifactIdentity::from_manifest_ref(
                self.space.clone(),
                collection.clone(),
                collection_generation.as_u64(),
                artifact_ref,
            )?;
            let stale_identity = VectorFlatArtifactIdentity::new(
                identity.artifact_id().clone(),
                identity.source_branch_id(),
                self.space.clone(),
                collection.clone(),
                collection_generation.as_u64().saturating_add(1),
                VectorSourceId::new(format!("{}-stale", identity.source_id().as_str()))?,
                identity.source_branch_id(),
                identity.source_generation(),
                identity.vector_dimension(),
                identity.metric(),
            )?;
            let stale = FlatVectorArtifact::new(stale_identity, Vec::new())?;
            self.artifacts.store_flat(&stale)?;
            replaced += 1;
        }
        Ok(replaced)
    }

    #[cfg(any(test, feature = "testkit"))]
    fn index_manifest_flat_artifact_ids_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<Vec<VectorArtifactId>> {
        let resolution = self.index_manifest_resolution_for_test(collection)?;
        resolution
            .artifact_refs
            .iter()
            .filter(|artifact_ref| artifact_ref.index_kind() == VectorArtifactKind::Flat)
            .map(|artifact_ref| VectorArtifactId::new(artifact_ref.artifact_id().to_owned()))
            .collect()
    }

    #[cfg(any(test, feature = "testkit"))]
    fn index_manifest_hnsw_artifact_ids_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<Vec<VectorArtifactId>> {
        let resolution = self.index_manifest_resolution_for_test(collection)?;
        resolution
            .artifact_refs
            .iter()
            .filter(|artifact_ref| artifact_ref.index_kind() == VectorArtifactKind::Hnsw)
            .map(|artifact_ref| VectorArtifactId::new(artifact_ref.artifact_id().to_owned()))
            .collect()
    }

    #[cfg(any(test, feature = "testkit"))]
    fn index_manifest_resolution_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<VectorIndexManifestResolution> {
        let record = self.branch_record()?;
        let (config, collection_generation) = self.require_collection_config_with_generation(
            &record,
            collection,
            ReadSelector::Latest,
        )?;
        self.index_manifest_resolution(
            &record,
            collection,
            config,
            collection_generation,
            ReadSelector::Latest,
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Stores a branch-local vector index manifest with synthetic artifact refs.
    pub fn seed_synthetic_index_manifest_for_test(
        &mut self,
        collection: &VectorCollectionName,
        artifact_refs: &[(BranchId, usize, VectorDistanceMetric)],
        active_delta_count: u64,
    ) -> EngineResult<()> {
        let artifact_refs = artifact_refs
            .iter()
            .enumerate()
            .map(|(index, (source_branch_id, dimension, metric))| {
                let ordinal = index + 1;
                VectorArtifactRef::for_test(
                    format!("artifact-{ordinal}"),
                    format!("source-{ordinal}"),
                    *source_branch_id,
                    1,
                    1,
                    None,
                    VectorArtifactKind::Flat,
                    *dimension,
                    *metric,
                    10,
                    1024,
                    u64::try_from(ordinal).unwrap_or(u64::MAX),
                )
            })
            .collect();
        self.seed_index_manifest_for_test(collection, artifact_refs, active_delta_count)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Stores a branch-local vector index manifest with an unsupported HNSW ref.
    pub fn seed_synthetic_hnsw_index_manifest_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<()> {
        let record = self.branch_record()?;
        let (config, _) = self.require_collection_config_with_generation(
            &record,
            collection,
            ReadSelector::Latest,
        )?;
        let artifact_ref = VectorArtifactRef::for_test(
            "artifact-hnsw",
            "source-hnsw",
            record.storage_branch_id(),
            record.generation(),
            record.generation(),
            None,
            VectorArtifactKind::Hnsw,
            config.dimension(),
            config.metric(),
            10,
            1024,
            99,
        );
        self.seed_index_manifest_for_test(collection, vec![artifact_ref], 0)
    }

    #[cfg(any(test, feature = "testkit"))]
    fn seed_index_manifest_for_test(
        &mut self,
        collection: &VectorCollectionName,
        artifact_refs: Vec<VectorArtifactRef>,
        active_delta_count: u64,
    ) -> EngineResult<()> {
        let record = self.branch_record()?;
        let (_, collection_generation) = self.require_collection_config_with_generation(
            &record,
            collection,
            ReadSelector::Latest,
        )?;
        let manifest = VectorIndexManifest::empty(
            record.storage_branch_id(),
            record.generation(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
        )
        .with_artifact_refs_for_test(artifact_refs, active_delta_count);
        let bytes = encode_vector_index_manifest(&manifest)?;
        self.put_index_manifest_bytes(&record, collection, bytes)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Builds a flat artifact from visible rows and stores its small manifest ref.
    pub fn seed_flat_index_manifest_from_visible_rows_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<()> {
        let artifact_ref = self.build_flat_artifact_ref_from_visible_rows_for_test(
            collection, source_id, None, None,
        )?;
        self.seed_index_manifest_for_test(collection, vec![artifact_ref], 0)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Builds flat and HNSW artifacts from visible rows and stores their manifest refs.
    pub fn seed_flat_hnsw_index_manifest_from_visible_rows_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<()> {
        let flat_ref = self.build_flat_artifact_ref_from_visible_rows_for_test(
            collection, source_id, None, None,
        )?;
        let hnsw_ref =
            self.build_hnsw_artifact_ref_from_visible_rows_for_test(collection, source_id)?;
        self.seed_index_manifest_for_test(collection, vec![flat_ref, hnsw_ref], 0)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Explicitly builds branch-local vector index artifacts for tests.
    pub fn seal_index_artifacts_for_test(
        &mut self,
        collection: &VectorCollectionName,
        policy: VectorIndexPolicy,
    ) -> EngineResult<()> {
        let record = self.branch_record()?;
        let (config, collection_generation) = self.require_collection_config_with_generation(
            &record,
            collection,
            ReadSelector::Latest,
        )?;
        let snapshot = self.vector_query_snapshot(&record, collection, ReadSelector::Latest)?;
        self.seal_index_artifacts_from_snapshot(
            &record,
            collection,
            collection_generation,
            config,
            &snapshot,
            policy,
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Returns the load status for the flat artifact built by the testkit builder.
    pub fn flat_artifact_load_status_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
        max_bytes: Option<usize>,
    ) -> EngineResult<&'static str> {
        let (record, config, collection_generation) =
            self.collection_artifact_context_for_test(collection)?;
        let identity = self.flat_artifact_identity_for_test(
            &record,
            collection,
            collection_generation,
            source_id,
            config,
        )?;
        let load = self.artifacts.load_flat(
            &identity,
            max_bytes.unwrap_or_else(default_flat_artifact_load_budget_bytes),
        );
        Ok(load.report().status().as_str())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Returns the load status for the HNSW artifact built by the testkit builder.
    pub fn hnsw_artifact_load_status_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
        max_bytes: Option<usize>,
    ) -> EngineResult<&'static str> {
        let (record, config, collection_generation) =
            self.collection_artifact_context_for_test(collection)?;
        let identity = VectorFlatArtifactIdentity::new(
            self.hnsw_artifact_id_for_test(collection, source_id)?,
            record.storage_branch_id(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            VectorSourceId::new(source_id)?,
            record.storage_branch_id(),
            record.generation(),
            config.dimension(),
            config.metric(),
        )?;
        let load = self.artifacts.load_hnsw(
            &identity,
            max_bytes.unwrap_or_else(default_hnsw_artifact_load_budget_bytes),
        );
        Ok(load.report().status().as_str())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Returns the byte size of the flat artifact built by the testkit builder.
    pub fn flat_artifact_byte_len_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<Option<u64>> {
        let (record, config, collection_generation) =
            self.collection_artifact_context_for_test(collection)?;
        let identity = self.flat_artifact_identity_for_test(
            &record,
            collection,
            collection_generation,
            source_id,
            config,
        )?;
        let load = self
            .artifacts
            .load_flat(&identity, default_flat_artifact_load_budget_bytes());
        Ok(load.report().byte_len())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Removes the flat artifact bytes for missing-artifact diagnostics tests.
    pub fn remove_flat_artifact_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<()> {
        let artifact_id = self.flat_artifact_id_for_test(collection, source_id)?;
        self.artifacts.remove_for_test(&artifact_id)
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Replaces flat artifact bytes with malformed payload bytes.
    pub fn corrupt_flat_artifact_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<()> {
        let artifact_id = self.flat_artifact_id_for_test(collection, source_id)?;
        self.artifacts
            .put_raw_flat_for_test(artifact_id, vec![1, 2, 3])
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Replaces flat artifact bytes with a payload whose identity no longer matches.
    pub fn make_flat_artifact_stale_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<()> {
        let (record, config, collection_generation) =
            self.collection_artifact_context_for_test(collection)?;
        let identity = self.flat_artifact_identity_for_test(
            &record,
            collection,
            collection_generation,
            source_id,
            config,
        )?;
        let stale_identity = VectorFlatArtifactIdentity::new(
            identity.artifact_id().clone(),
            record.storage_branch_id(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64().saturating_add(1),
            VectorSourceId::new(format!("{source_id}-stale"))?,
            record.storage_branch_id(),
            record.generation(),
            config.dimension(),
            config.metric(),
        )?;
        let stale = FlatVectorArtifact::new(stale_identity, Vec::new())?;
        self.artifacts.store_flat(&stale)?;
        Ok(())
    }

    #[cfg(any(test, feature = "testkit"))]
    /// Stores raw branch-local vector index manifest bytes for diagnostics tests.
    pub fn put_raw_index_manifest_for_test(
        &mut self,
        collection: &VectorCollectionName,
        bytes: Vec<u8>,
    ) -> EngineResult<()> {
        let record = self.branch_record()?;
        self.put_index_manifest_bytes(&record, collection, bytes)
    }

    fn put_index_manifest_bytes(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        bytes: Vec<u8>,
    ) -> EngineResult<()> {
        self.persistence.commit(&CommitPlan::new(
            record.storage_branch_id(),
            vec![RowMutation::put(
                self.index_manifest_address(record, collection),
                bytes,
            )],
            Some(record.generation()),
        ))?;
        Ok(())
    }

    #[cfg(any(test, feature = "testkit"))]
    fn build_flat_artifact_ref_from_visible_rows_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
        artifact_id: Option<VectorArtifactId>,
        rows: Option<Vec<(PersistenceReadRow, VectorEntry)>>,
    ) -> EngineResult<VectorArtifactRef> {
        let (record, config, collection_generation) =
            self.collection_artifact_context_for_test(collection)?;
        let identity = match artifact_id {
            Some(artifact_id) => VectorFlatArtifactIdentity::new(
                artifact_id,
                record.storage_branch_id(),
                self.space.clone(),
                collection.clone(),
                collection_generation.as_u64(),
                VectorSourceId::new(source_id)?,
                record.storage_branch_id(),
                record.generation(),
                config.dimension(),
                config.metric(),
            )?,
            None => self.flat_artifact_identity_for_test(
                &record,
                collection,
                collection_generation,
                source_id,
                config,
            )?,
        };
        let rows = match rows {
            Some(rows) => rows,
            None => self.visible_vector_entries(&record, collection, ReadSelector::Latest)?,
        };
        let artifact = FlatVectorArtifact::from_visible_entries(identity.clone(), &rows)?;
        let handle = self.artifacts.store_flat(&artifact)?;
        VectorArtifactRef::new(
            handle.artifact_id().as_str().to_owned(),
            identity.source_id().as_str().to_owned(),
            identity.source_branch_id(),
            identity.source_generation(),
            rows_covered_commit_version(&rows).as_u64(),
            None,
            VectorArtifactKind::Flat,
            identity.vector_dimension(),
            identity.metric(),
            handle.vector_count(),
            handle.byte_len(),
            handle.checksum(),
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    fn build_hnsw_artifact_ref_from_visible_rows_for_test(
        &mut self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<VectorArtifactRef> {
        let (record, config, collection_generation) =
            self.collection_artifact_context_for_test(collection)?;
        let identity = VectorFlatArtifactIdentity::new(
            self.hnsw_artifact_id_for_test(collection, source_id)?,
            record.storage_branch_id(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            VectorSourceId::new(source_id)?,
            record.storage_branch_id(),
            record.generation(),
            config.dimension(),
            config.metric(),
        )?;
        let rows = self.visible_vector_entries(&record, collection, ReadSelector::Latest)?;
        let artifact = HnswVectorArtifact::from_visible_entries(
            identity.clone(),
            HnswArtifactConfig::default_for_engine(),
            &rows,
        )?;
        let handle = self.artifacts.store_hnsw(&artifact)?;
        VectorArtifactRef::new(
            handle.artifact_id().as_str().to_owned(),
            identity.source_id().as_str().to_owned(),
            identity.source_branch_id(),
            identity.source_generation(),
            rows_covered_commit_version(&rows).as_u64(),
            None,
            VectorArtifactKind::Hnsw,
            identity.vector_dimension(),
            identity.metric(),
            handle.vector_count(),
            handle.byte_len(),
            handle.checksum(),
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    fn collection_artifact_context_for_test(
        &mut self,
        collection: &VectorCollectionName,
    ) -> EngineResult<(BranchCatalogRecord, VectorConfig, CommitVersion)> {
        let record = self.branch_record()?;
        let (config, collection_generation) = self.require_collection_config_with_generation(
            &record,
            collection,
            ReadSelector::Latest,
        )?;
        Ok((record, config, collection_generation))
    }

    #[cfg(any(test, feature = "testkit"))]
    fn flat_artifact_identity_for_test(
        &self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        source_id: &str,
        config: VectorConfig,
    ) -> EngineResult<VectorFlatArtifactIdentity> {
        VectorFlatArtifactIdentity::new(
            self.flat_artifact_id_for_test(collection, source_id)?,
            record.storage_branch_id(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            VectorSourceId::new(source_id)?,
            record.storage_branch_id(),
            record.generation(),
            config.dimension(),
            config.metric(),
        )
    }

    #[cfg(any(test, feature = "testkit"))]
    fn flat_artifact_id_for_test(
        &self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<VectorArtifactId> {
        VectorArtifactId::new(format!(
            "flat:{}:{}:{source_id}",
            self.space.as_str(),
            collection.as_str()
        ))
    }

    #[cfg(any(test, feature = "testkit"))]
    fn hnsw_artifact_id_for_test(
        &self,
        collection: &VectorCollectionName,
        source_id: &str,
    ) -> EngineResult<VectorArtifactId> {
        VectorArtifactId::new(format!(
            "hnsw:{}:{}:{source_id}",
            self.space.as_str(),
            collection.as_str()
        ))
    }

    fn delete_matching(
        &mut self,
        collection: &VectorCollectionName,
        predicate: impl Fn(&VectorEntry) -> bool,
    ) -> EngineResult<VectorBulkDeleteOutcome> {
        let record = self.branch_record()?;
        self.require_collection_config(&record, collection)?;
        let rows = self.visible_vector_entries(&record, collection, ReadSelector::Latest)?;
        let mut mutations = Vec::new();
        for (row, entry) in rows {
            if predicate(&entry) {
                mutations.push(RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::Vector,
                    row.key().to_vec(),
                )));
            }
        }
        if mutations.is_empty() {
            return Ok(VectorBulkDeleteOutcome::new(0, None));
        }
        let deleted_count = u64::try_from(mutations.len()).unwrap_or(u64::MAX);
        let commit = self.commit_batch(&record, mutations)?;
        Ok(VectorBulkDeleteOutcome::new(deleted_count, Some(commit)))
    }

    fn query_with_selector(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        selector: ReadSelector,
    ) -> EngineResult<VectorSearchResult> {
        Ok(self
            .query_with_selector_and_policy(
                collection,
                query,
                k,
                filter,
                selector,
                VectorIndexPolicy::default(),
            )?
            .0)
    }

    #[allow(clippy::too_many_lines)]
    fn query_with_selector_and_policy(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        selector: ReadSelector,
        policy: VectorIndexPolicy,
    ) -> EngineResult<(VectorSearchResult, VectorIndexDiagnostics)> {
        let record = self.branch_record()?;
        let (config, collection_generation) =
            self.require_collection_config_with_generation(&record, collection, selector)?;
        query.validate_dimension(config.dimension())?;
        if k == 0 {
            return query_vector_sources_with_index_artifacts(
                collection,
                query,
                config.metric(),
                k,
                filter,
                &[],
                &[],
                selector,
                &[],
                &[],
                &[],
                None,
                None,
                policy,
                0,
                true,
            );
        }
        let manifest_resolution = self.index_manifest_resolution(
            &record,
            collection,
            config,
            collection_generation,
            selector,
        )?;
        // Fast path: when a sealed index covers the collection, read only the active delta (rows
        // past the manifest watermark) and resolve the rest from artifacts — O(active delta + k)
        // instead of scanning the whole collection. Latest reads only.
        let bounded_watermark = if matches!(selector, ReadSelector::Latest)
            && policy.may_use_index()
            && !manifest_resolution.artifact_refs.is_empty()
        {
            manifest_resolution.active_delta_watermark()
        } else {
            None
        };
        if let Some(watermark) = bounded_watermark {
            let delta = self.vector_active_delta_snapshot(&record, collection, watermark)?;
            let collection_size = manifest_resolution
                .indexed_vector_count()
                .saturating_add(delta.entries.len());
            let index_artifacts = self.load_index_artifact_sources_for_query(
                collection,
                collection_generation,
                &manifest_resolution,
                collection_size,
                policy,
            )?;
            let (result, mut diagnostics) = query_vector_sources_with_index_artifacts(
                collection,
                query,
                config.metric(),
                k,
                filter,
                &delta.entries,
                &delta.tombstones,
                selector,
                &index_artifacts.flat_artifacts,
                &index_artifacts.hnsw_artifacts,
                &index_artifacts.artifact_reports,
                index_artifacts.flat_unavailable_reason,
                index_artifacts.hnsw_unavailable_reason,
                policy,
                collection_size,
                false,
            )?;
            // The active delta is only the rows past the watermark, so it is a complete answer
            // only when the index actually answered without an exact underfill. If the index was
            // unavailable or the result underfilled after filtering, fall through to the
            // full-snapshot path below, which is exact.
            let needs_full_snapshot = !diagnostics.last_query_used_index()
                || (diagnostics.filtered_underfill_fallback()
                    && k > 0
                    && result.matches().len() < k);
            if !needs_full_snapshot {
                diagnostics.record_manifest_lookup(manifest_resolution.lookup);
                diagnostics.record_hnsw_graph_builds(index_artifacts.hnsw_graph_builds);
                diagnostics.record_source_rows_scanned(delta.entries.len());
                return Ok((result, diagnostics));
            }
        }

        let snapshot = self.vector_query_snapshot(&record, collection, selector)?;
        let collection_size = snapshot.entries.len();
        let index_artifacts = self.load_index_artifact_sources_for_query(
            collection,
            collection_generation,
            &manifest_resolution,
            collection_size,
            policy,
        )?;
        let (result, mut diagnostics) = query_vector_sources_with_index_artifacts(
            collection,
            query,
            config.metric(),
            k,
            filter,
            &snapshot.entries,
            &snapshot.tombstones,
            selector,
            &index_artifacts.flat_artifacts,
            &index_artifacts.hnsw_artifacts,
            &index_artifacts.artifact_reports,
            index_artifacts.flat_unavailable_reason,
            index_artifacts.hnsw_unavailable_reason,
            policy,
            collection_size,
            true,
        )?;
        diagnostics.record_manifest_lookup(manifest_resolution.lookup);
        diagnostics.record_hnsw_graph_builds(index_artifacts.hnsw_graph_builds);
        diagnostics.record_source_rows_scanned(snapshot.entries.len());
        Ok((result, diagnostics))
    }

    #[cfg(any(test, feature = "testkit"))]
    fn query_exact_with_selector(
        &mut self,
        collection: &VectorCollectionName,
        query: &VectorEmbedding,
        k: usize,
        filter: Option<&VectorFilter>,
        selector: ReadSelector,
    ) -> EngineResult<VectorSearchResult> {
        let record = self.branch_record()?;
        let config = self.require_collection_config_with_selector(&record, collection, selector)?;
        query.validate_dimension(config.dimension())?;
        if k == 0 {
            return query_vector_exact(query, config.metric(), k, filter, &[]);
        }
        let entries = self.visible_vector_entries(&record, collection, selector)?;
        query_vector_exact(query, config.metric(), k, filter, &entries)
    }

    fn branch_record(&self) -> EngineResult<BranchCatalogRecord> {
        self.control.require_healthy()?;
        self.control
            .lookup_branch(&self.branch)
            .cloned()
            .ok_or_else(|| {
                EngineError::not_found(
                    "not_found.engine.branch",
                    format!("branch `{}` does not exist", self.branch),
                )
            })
    }

    fn collection_address(
        &self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::VectorCollection,
            encode_vector_collection_key(&self.space, collection),
        )
    }

    fn index_manifest_address(
        &self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::SpaceControl,
            vector_index_manifest_key(&self.space, collection),
        )
    }

    fn vector_address(
        &self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        key: &VectorKey,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::Vector,
            encode_vector_key(&self.space, collection, key),
        )
    }

    fn collection_config_row(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        selector: ReadSelector,
    ) -> EngineResult<Option<PersistenceReadRow>> {
        let address = self.collection_address(record, collection);
        Ok(self
            .persistence
            .read_row(address, selector)?
            .filter(|row| !row.is_tombstone()))
    }

    fn require_collection_config(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
    ) -> EngineResult<VectorConfig> {
        self.require_collection_config_with_selector(record, collection, ReadSelector::Latest)
    }

    fn require_collection_config_with_selector(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        selector: ReadSelector,
    ) -> EngineResult<VectorConfig> {
        self.require_collection_config_with_generation(record, collection, selector)
            .map(|(config, _)| config)
    }

    fn require_collection_config_with_generation(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        selector: ReadSelector,
    ) -> EngineResult<(VectorConfig, CommitVersion)> {
        let Some(row) = self.collection_config_row(record, collection, selector)? else {
            return Err(EngineError::not_found(
                "not_found.engine.vector_collection",
                "vector collection does not exist",
            ));
        };
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.vector_collection",
                "stored vector collection row is missing a value",
            )
        })?;
        let config = decode_collection_config(collection, value)?;
        Ok((config, row.commit_version()))
    }

    fn index_manifest_resolution(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        config: VectorConfig,
        collection_generation: CommitVersion,
        selector: ReadSelector,
    ) -> EngineResult<VectorIndexManifestResolution> {
        let address = self.index_manifest_address(record, collection);
        let Some(row) = self
            .persistence
            .read_row(address, selector)?
            .filter(|row| !row.is_tombstone())
        else {
            return Ok(VectorIndexManifestResolution {
                lookup: VectorIndexManifestLookup::Missing,
                artifact_refs: Vec::new(),
            });
        };
        let Some(value) = row.value() else {
            return Ok(VectorIndexManifestResolution {
                lookup: VectorIndexManifestLookup::Corrupt,
                artifact_refs: Vec::new(),
            });
        };
        let Ok(manifest) = decode_vector_index_manifest(value) else {
            return Ok(VectorIndexManifestResolution {
                lookup: VectorIndexManifestLookup::Corrupt,
                artifact_refs: Vec::new(),
            });
        };
        if !manifest.matches_branch_collection(
            record.storage_branch_id(),
            record.generation(),
            &self.space,
            collection,
            collection_generation.as_u64(),
        ) {
            return Ok(VectorIndexManifestResolution {
                lookup: VectorIndexManifestLookup::Stale,
                artifact_refs: Vec::new(),
            });
        }
        if !manifest.artifact_refs_match_config(config.dimension(), config.metric()) {
            return Ok(VectorIndexManifestResolution {
                lookup: VectorIndexManifestLookup::Stale,
                artifact_refs: Vec::new(),
            });
        }
        let inherited_ref_count = manifest
            .artifact_refs()
            .iter()
            .filter(|artifact_ref| artifact_ref.source_branch_id() != record.storage_branch_id())
            .count();
        let ref_count = manifest.artifact_refs().len();
        let active_delta_count =
            usize::try_from(manifest.active_delta_count()).unwrap_or(usize::MAX);
        Ok(VectorIndexManifestResolution {
            lookup: VectorIndexManifestLookup::Loaded {
                generation: manifest.manifest_generation(),
                ref_count,
                inherited_ref_count,
                owned_ref_count: ref_count.saturating_sub(inherited_ref_count),
                active_delta_count,
            },
            artifact_refs: manifest.artifact_refs().to_vec(),
        })
    }

    fn load_index_artifact_sources_for_query(
        &mut self,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        manifest_resolution: &VectorIndexManifestResolution,
        visible_entry_count: usize,
        policy: VectorIndexPolicy,
    ) -> EngineResult<VectorIndexArtifactLoadSet> {
        if !policy.should_load_flat_sources(visible_entry_count) {
            return Ok(VectorIndexArtifactLoadSet::unavailable(
                "collection_below_exact_threshold",
            ));
        }
        let VectorIndexManifestLookup::Loaded { .. } = manifest_resolution.lookup else {
            return Ok(VectorIndexArtifactLoadSet::unavailable(
                manifest_resolution.lookup.status(),
            ));
        };
        if manifest_resolution.artifact_refs.is_empty() {
            return Ok(VectorIndexArtifactLoadSet::unavailable("manifest_empty"));
        }

        let mut flat_artifacts = Vec::new();
        let mut hnsw_artifacts = Vec::new();
        let mut artifact_reports = Vec::new();
        let mut flat_ref_count = 0usize;
        let mut hnsw_ref_count = 0usize;
        let mut hnsw_graph_builds = 0usize;
        let load_hnsw = policy.should_load_hnsw_sources(visible_entry_count);
        for artifact_ref in &manifest_resolution.artifact_refs {
            let artifact_id = artifact_ref.artifact_id().to_owned();
            match artifact_ref.index_kind() {
                VectorArtifactKind::Flat => {
                    flat_ref_count = flat_ref_count.saturating_add(1);
                    let status = self.load_flat_source_ref(
                        collection,
                        collection_generation,
                        artifact_ref,
                        &mut flat_artifacts,
                        policy.flat_artifact_load_budget_bytes(),
                    )?;
                    artifact_reports.push(VectorArtifactSourceDiagnostic::new(
                        artifact_id,
                        status,
                        false,
                    ));
                }
                VectorArtifactKind::Hnsw => {
                    hnsw_ref_count = hnsw_ref_count.saturating_add(1);
                    if !load_hnsw {
                        artifact_reports.push(VectorArtifactSourceDiagnostic::new(
                            artifact_id,
                            "not_selected",
                            false,
                        ));
                        continue;
                    }
                    let (status, built) = self.load_hnsw_source_ref(
                        collection,
                        collection_generation,
                        artifact_ref,
                        &mut hnsw_artifacts,
                        policy.hnsw_artifact_load_budget_bytes(),
                    )?;
                    if built {
                        hnsw_graph_builds = hnsw_graph_builds.saturating_add(1);
                    }
                    artifact_reports.push(VectorArtifactSourceDiagnostic::new(
                        artifact_id,
                        status,
                        false,
                    ));
                }
            }
        }
        let flat_unavailable_reason = if flat_ref_count == 0 {
            Some("manifest_no_flat_artifacts")
        } else if flat_artifacts.len() != flat_ref_count {
            Some("artifact_unavailable")
        } else {
            None
        };
        let hnsw_unavailable_reason = if !load_hnsw {
            Some("policy_not_hnsw")
        } else if hnsw_ref_count == 0 {
            Some("manifest_no_hnsw_artifacts")
        } else if hnsw_artifacts.len() != hnsw_ref_count {
            Some("artifact_unavailable")
        } else {
            None
        };
        Ok(VectorIndexArtifactLoadSet {
            flat_artifacts,
            hnsw_artifacts,
            artifact_reports,
            flat_unavailable_reason,
            hnsw_unavailable_reason,
            hnsw_graph_builds,
        })
    }

    fn load_flat_source_ref(
        &mut self,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        artifact_ref: &VectorArtifactRef,
        flat_artifacts: &mut Vec<VectorFlatArtifactSourceInput>,
        load_budget_bytes: usize,
    ) -> EngineResult<&'static str> {
        let identity = VectorFlatArtifactIdentity::from_manifest_ref(
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            artifact_ref,
        )?;
        // Reuse the parsed flat artifact when its cached source bytes still match the manifest ref
        // checksum, so repeated queries do not re-decode the flat payload.
        if let Some(cached) = self
            .artifacts
            .cached_flat_runtime(identity.artifact_id(), artifact_ref.checksum())
        {
            flat_artifacts.push(VectorFlatArtifactSourceInput::new(
                cached,
                artifact_ref.derived_bytes(),
                artifact_ref.fork_version_cap(),
            ));
            return Ok("loaded");
        }
        let (artifact, status) =
            self.load_flat_artifact_for_query(&identity, artifact_ref, load_budget_bytes);
        if let Some(artifact) = artifact {
            let cached = self.artifacts.insert_flat_runtime(
                identity.artifact_id().clone(),
                artifact_ref.checksum(),
                artifact,
            );
            flat_artifacts.push(VectorFlatArtifactSourceInput::new(
                cached,
                artifact_ref.derived_bytes(),
                artifact_ref.fork_version_cap(),
            ));
        }
        Ok(status)
    }

    fn load_hnsw_source_ref(
        &mut self,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        artifact_ref: &VectorArtifactRef,
        hnsw_artifacts: &mut Vec<VectorHnswArtifactSourceInput>,
        load_budget_bytes: usize,
    ) -> EngineResult<(&'static str, bool)> {
        let identity = VectorFlatArtifactIdentity::from_hnsw_manifest_ref(
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            artifact_ref,
        )?;
        // Graph search stays exact for an inherited artifact only when the fork cap actually
        // excludes some of its rows (it was sealed past the fork point). When every row is visible
        // to this branch — the common case — the per-candidate visibility filter is a no-op and the
        // graph path is used.
        let fork_cap_excludes_rows = artifact_ref
            .fork_version_cap()
            .is_some_and(|cap| artifact_ref.covered_commit_version() > cap);
        // Reuse a previously built graph when the cached source bytes still match the manifest
        // ref checksum, so repeated queries do not rebuild the HNSW graph.
        if let Some(cached) = self
            .artifacts
            .cached_hnsw_runtime(identity.artifact_id(), artifact_ref.checksum())
        {
            hnsw_artifacts.push(VectorHnswArtifactSourceInput::from_cached(
                cached,
                artifact_ref.derived_bytes(),
                artifact_ref.fork_version_cap(),
                fork_cap_excludes_rows,
            ));
            return Ok(("loaded", false));
        }
        let (artifact, status) =
            self.load_hnsw_artifact_for_query(&identity, artifact_ref, load_budget_bytes);
        if let Some(artifact) = artifact {
            let cached = self.artifacts.insert_hnsw_runtime(
                identity.artifact_id().clone(),
                artifact_ref.checksum(),
                artifact,
            );
            hnsw_artifacts.push(VectorHnswArtifactSourceInput::from_cached(
                cached,
                artifact_ref.derived_bytes(),
                artifact_ref.fork_version_cap(),
                fork_cap_excludes_rows,
            ));
            return Ok((status, true));
        }
        Ok((status, false))
    }

    fn load_flat_artifact_for_query(
        &mut self,
        identity: &VectorFlatArtifactIdentity,
        artifact_ref: &VectorArtifactRef,
        load_budget_bytes: usize,
    ) -> (Option<FlatVectorArtifact>, &'static str) {
        let load = self.artifacts.load_flat(identity, load_budget_bytes);
        match load {
            super::VectorFlatArtifactLoad::Loaded { artifact, report } => {
                if report.byte_len() == Some(artifact_ref.derived_bytes())
                    && report.checksum() == Some(artifact_ref.checksum())
                    && u64::try_from(artifact.rows().len()).unwrap_or(u64::MAX)
                        == artifact_ref.vector_count()
                {
                    (Some(artifact), "loaded")
                } else {
                    (None, "stale")
                }
            }
            super::VectorFlatArtifactLoad::Skipped(report) => (None, report.status().as_str()),
        }
    }

    fn load_hnsw_artifact_for_query(
        &mut self,
        identity: &VectorFlatArtifactIdentity,
        artifact_ref: &VectorArtifactRef,
        load_budget_bytes: usize,
    ) -> (Option<HnswVectorArtifact>, &'static str) {
        let load = self.artifacts.load_hnsw(identity, load_budget_bytes);
        match load {
            super::VectorHnswArtifactLoad::Loaded { artifact, report } => {
                if report.byte_len() == Some(artifact_ref.derived_bytes())
                    && report.checksum() == Some(artifact_ref.checksum())
                    && u64::try_from(artifact.rows().len()).unwrap_or(u64::MAX)
                        == artifact_ref.vector_count()
                {
                    (Some(artifact), "loaded")
                } else {
                    (None, "stale")
                }
            }
            super::VectorHnswArtifactLoad::Skipped(report) => (None, report.status().as_str()),
        }
    }

    #[allow(dead_code)]
    fn seal_index_artifacts_from_snapshot(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        config: VectorConfig,
        snapshot: &VectorQuerySnapshot,
        policy: VectorIndexPolicy,
    ) -> EngineResult<()> {
        let mut artifact_refs = self.seal_index_artifacts_from_immutable_sources(
            record,
            collection,
            collection_generation,
            config,
            policy,
        )?;
        let active_delta_count = if artifact_refs.is_empty() {
            artifact_refs.push(self.seal_whole_collection_flat_artifact(
                record,
                collection,
                collection_generation,
                config,
                snapshot,
            )?);
            if policy.should_seal_hnsw_source(snapshot.entries.len()) {
                artifact_refs.push(self.seal_whole_collection_hnsw_artifact(
                    record,
                    collection,
                    collection_generation,
                    config,
                    snapshot,
                )?);
            }
            0
        } else {
            active_delta_count_for_refs(snapshot, &artifact_refs)
        };
        let manifest = VectorIndexManifest::empty(
            record.storage_branch_id(),
            record.generation(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
        )
        .with_artifact_refs(
            artifact_refs,
            u64::try_from(active_delta_count).unwrap_or(u64::MAX),
        );
        let bytes = encode_vector_index_manifest(&manifest)?;
        self.put_index_manifest_bytes(record, collection, bytes)
    }

    #[allow(dead_code)]
    fn seal_index_artifacts_from_immutable_sources(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        config: VectorConfig,
        policy: VectorIndexPolicy,
    ) -> EngineResult<Vec<VectorArtifactRef>> {
        let start = encode_vector_collection_entry_prefix(&self.space, collection);
        let end = next_prefix(&start);
        let sources = self.persistence.scan_immutable_sources(
            record.storage_branch_id(),
            RowClass::Vector,
            Some(start),
            Some(end),
            ReadSelector::Latest,
        )?;
        let mut refs = Vec::new();
        for source in sources {
            let entries = self.visible_entries_from_source(collection, &source)?;
            if entries.is_empty() {
                continue;
            }
            refs.push(self.seal_flat_artifact_for_source(
                collection,
                collection_generation,
                config,
                &source,
                &entries,
            )?);
            if policy.should_seal_hnsw_source(entries.len()) {
                refs.push(self.seal_hnsw_artifact_for_source(
                    collection,
                    collection_generation,
                    config,
                    &source,
                    &entries,
                )?);
            }
        }
        Ok(refs)
    }

    #[allow(dead_code)]
    fn seal_whole_collection_flat_artifact(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        config: VectorConfig,
        snapshot: &VectorQuerySnapshot,
    ) -> EngineResult<VectorArtifactRef> {
        let seal_version = snapshot
            .entries
            .iter()
            .map(|(row, _)| row.commit_version())
            .chain(snapshot.tombstones.iter().map(VectorTombstone::version))
            .max()
            .unwrap_or(collection_generation);
        let source_id = VectorSourceId::new(format!("sealed-{}", seal_version.as_u64()))?;
        let artifact_id = VectorArtifactId::new(format!(
            "flat:{}:{}:sealed-{}",
            self.space.as_str(),
            collection.as_str(),
            seal_version.as_u64()
        ))?;
        let identity = VectorFlatArtifactIdentity::new(
            artifact_id,
            record.storage_branch_id(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            source_id,
            record.storage_branch_id(),
            seal_version.as_u64(),
            config.dimension(),
            config.metric(),
        )?;
        self.store_flat_artifact_ref(&identity, &snapshot.entries, seal_version, None)
    }

    #[allow(dead_code)]
    fn seal_whole_collection_hnsw_artifact(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        config: VectorConfig,
        snapshot: &VectorQuerySnapshot,
    ) -> EngineResult<VectorArtifactRef> {
        let seal_version = snapshot
            .entries
            .iter()
            .map(|(row, _)| row.commit_version())
            .chain(snapshot.tombstones.iter().map(VectorTombstone::version))
            .max()
            .unwrap_or(collection_generation);
        let source_id = VectorSourceId::new(format!("sealed-{}", seal_version.as_u64()))?;
        let artifact_id = VectorArtifactId::new(format!(
            "hnsw:{}:{}:sealed-{}",
            self.space.as_str(),
            collection.as_str(),
            seal_version.as_u64()
        ))?;
        let identity = VectorFlatArtifactIdentity::new(
            artifact_id,
            record.storage_branch_id(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            source_id,
            record.storage_branch_id(),
            seal_version.as_u64(),
            config.dimension(),
            config.metric(),
        )?;
        self.store_hnsw_artifact_ref(&identity, &snapshot.entries, seal_version, None)
    }

    #[allow(dead_code)]
    fn seal_flat_artifact_for_source(
        &mut self,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        config: VectorConfig,
        source: &PersistenceImmutableSource,
        entries: &[(PersistenceReadRow, VectorEntry)],
    ) -> EngineResult<VectorArtifactRef> {
        let source_id = VectorSourceId::new(source.source_id())?;
        let artifact_id = VectorArtifactId::new(format!(
            "flat:{}:{}:source:{}",
            self.space.as_str(),
            collection.as_str(),
            source.source_id()
        ))?;
        let identity = VectorFlatArtifactIdentity::new(
            artifact_id,
            source.source_branch_id(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            source_id,
            source.source_branch_id(),
            source.source_generation().as_u64(),
            config.dimension(),
            config.metric(),
        )?;
        let covered_commit_version =
            source_covered_commit_version(&self.space, collection, source)?;
        self.store_flat_artifact_ref(
            &identity,
            entries,
            covered_commit_version,
            source.fork_version_cap(),
        )
    }

    #[allow(dead_code)]
    fn seal_hnsw_artifact_for_source(
        &mut self,
        collection: &VectorCollectionName,
        collection_generation: CommitVersion,
        config: VectorConfig,
        source: &PersistenceImmutableSource,
        entries: &[(PersistenceReadRow, VectorEntry)],
    ) -> EngineResult<VectorArtifactRef> {
        let source_id = VectorSourceId::new(source.source_id())?;
        let artifact_id = VectorArtifactId::new(format!(
            "hnsw:{}:{}:source:{}",
            self.space.as_str(),
            collection.as_str(),
            source.source_id()
        ))?;
        let identity = VectorFlatArtifactIdentity::new(
            artifact_id,
            source.source_branch_id(),
            self.space.clone(),
            collection.clone(),
            collection_generation.as_u64(),
            source_id,
            source.source_branch_id(),
            source.source_generation().as_u64(),
            config.dimension(),
            config.metric(),
        )?;
        let covered_commit_version =
            source_covered_commit_version(&self.space, collection, source)?;
        self.store_hnsw_artifact_ref(
            &identity,
            entries,
            covered_commit_version,
            source.fork_version_cap(),
        )
    }

    #[allow(dead_code)]
    fn store_flat_artifact_ref(
        &mut self,
        identity: &VectorFlatArtifactIdentity,
        entries: &[(PersistenceReadRow, VectorEntry)],
        covered_commit_version: CommitVersion,
        fork_version_cap: Option<CommitVersion>,
    ) -> EngineResult<VectorArtifactRef> {
        let artifact = FlatVectorArtifact::from_visible_entries_with_budget(
            identity.clone(),
            entries,
            default_flat_artifact_build_budget_bytes(),
        )?;
        let handle = self.artifacts.store_flat(&artifact)?;
        VectorArtifactRef::new(
            handle.artifact_id().as_str().to_owned(),
            identity.source_id().as_str().to_owned(),
            identity.source_branch_id(),
            identity.source_generation(),
            covered_commit_version.as_u64(),
            fork_version_cap,
            VectorArtifactKind::Flat,
            identity.vector_dimension(),
            identity.metric(),
            handle.vector_count(),
            handle.byte_len(),
            handle.checksum(),
        )
    }

    #[allow(dead_code)]
    fn store_hnsw_artifact_ref(
        &mut self,
        identity: &VectorFlatArtifactIdentity,
        entries: &[(PersistenceReadRow, VectorEntry)],
        covered_commit_version: CommitVersion,
        fork_version_cap: Option<CommitVersion>,
    ) -> EngineResult<VectorArtifactRef> {
        let artifact = HnswVectorArtifact::from_visible_entries_with_budget(
            identity.clone(),
            HnswArtifactConfig::default_for_engine(),
            entries,
            default_hnsw_artifact_build_budget_bytes(),
        )?;
        let handle = self.artifacts.store_hnsw(&artifact)?;
        VectorArtifactRef::new(
            handle.artifact_id().as_str().to_owned(),
            identity.source_id().as_str().to_owned(),
            identity.source_branch_id(),
            identity.source_generation(),
            covered_commit_version.as_u64(),
            fork_version_cap,
            VectorArtifactKind::Hnsw,
            identity.vector_dimension(),
            identity.metric(),
            handle.vector_count(),
            handle.byte_len(),
            handle.checksum(),
        )
    }

    fn visible_entries_from_source(
        &self,
        collection: &VectorCollectionName,
        source: &PersistenceImmutableSource,
    ) -> EngineResult<Vec<(PersistenceReadRow, VectorEntry)>> {
        let mut selected: BTreeMap<VectorKey, VectorSourceSelectedRow> = BTreeMap::new();
        for row in source.rows() {
            let (row_collection, key) = decode_vector_key(&self.space, row.key())?;
            if row_collection != *collection {
                continue;
            }
            let candidate = if row.is_tombstone() {
                VectorSourceSelectedRow {
                    row: row.clone(),
                    entry: None,
                }
            } else {
                VectorSourceSelectedRow {
                    row: row.clone(),
                    entry: Some(Self::entry_from_row(collection, &key, row)?),
                }
            };
            match selected.entry(key) {
                std::collections::btree_map::Entry::Occupied(mut occupied) => {
                    if persistence_row_is_newer(&candidate.row, &occupied.get().row) {
                        occupied.insert(candidate);
                    }
                }
                std::collections::btree_map::Entry::Vacant(vacant) => {
                    vacant.insert(candidate);
                }
            }
        }
        Ok(selected
            .into_values()
            .filter_map(|selected| selected.entry.map(|entry| (selected.row, entry)))
            .collect())
    }

    fn require_collection_config_history(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
    ) -> EngineResult<()> {
        if let Some(row) = self.collection_config_row(record, collection, ReadSelector::Latest)? {
            let value = row.value().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.vector_collection",
                    "stored vector collection row is missing a value",
                )
            })?;
            let _ = decode_collection_config(collection, value)?;
            return Ok(());
        }

        let address = self.collection_address(record, collection);
        for row in self.persistence.read_history(&address, true)? {
            if row.is_tombstone() {
                continue;
            }
            let value = row.value().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.vector_collection",
                    "stored vector collection row is missing a value",
                )
            })?;
            let _ = decode_collection_config(collection, value)?;
            return Ok(());
        }

        Err(EngineError::not_found(
            "not_found.engine.vector_collection",
            "vector collection does not exist",
        ))
    }

    fn collection_info_from_row(
        &mut self,
        record: &BranchCatalogRecord,
        row: &PersistenceReadRow,
    ) -> EngineResult<VectorCollectionInfo> {
        let name = decode_vector_collection_name(&self.space, row.key())?;
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.vector_collection",
                "stored vector collection row is missing a value",
            )
        })?;
        let config = decode_collection_config(&name, value)?;
        let count = self.count_with_record(record, &name, ReadSelector::Latest)?;
        Ok(VectorCollectionInfo::new(
            name,
            config,
            count,
            row.commit_version(),
            row.commit_timestamp(),
        ))
    }

    fn count_with_record(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        selector: ReadSelector,
    ) -> EngineResult<u64> {
        let count = self
            .vector_rows(record, collection, selector)?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    fn vector_rows(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        selector: ReadSelector,
    ) -> EngineResult<Vec<PersistenceReadRow>> {
        self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::Vector,
            encode_vector_collection_entry_prefix(&self.space, collection),
            selector,
            None,
        )
    }

    fn visible_vector_entries(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        selector: ReadSelector,
    ) -> EngineResult<Vec<(PersistenceReadRow, VectorEntry)>> {
        self.vector_rows(record, collection, selector)?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| {
                let (_, key) = decode_vector_key(&self.space, row.key())?;
                let entry = Self::entry_from_row(collection, &key, &row)?;
                Ok((row, entry))
            })
            .collect()
    }

    fn vector_query_snapshot(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        selector: ReadSelector,
    ) -> EngineResult<VectorQuerySnapshot> {
        let rows = self.vector_rows(record, collection, selector)?;
        self.snapshot_from_rows(collection, rows)
    }

    /// Read and decode only the "active delta": rows committed after `watermark`, leaving rows
    /// covered by a sealed index artifact unread. This keeps an index-covered latest query
    /// O(active delta) instead of O(collection). Latest selector only (the bounded read is the
    /// hot path; timestamp/version reads use the full snapshot).
    fn vector_active_delta_snapshot(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        watermark: CommitVersion,
    ) -> EngineResult<VectorQuerySnapshot> {
        let rows = self.persistence.scan_prefix_after_version(
            record.storage_branch_id(),
            RowClass::Vector,
            encode_vector_collection_entry_prefix(&self.space, collection),
            ReadSelector::Latest,
            None,
            watermark,
        )?;
        self.snapshot_from_rows(collection, rows)
    }

    fn snapshot_from_rows(
        &self,
        collection: &VectorCollectionName,
        rows: Vec<PersistenceReadRow>,
    ) -> EngineResult<VectorQuerySnapshot> {
        let mut entries = Vec::new();
        let mut tombstones = Vec::new();
        for row in rows {
            let (_, key) = decode_vector_key(&self.space, row.key())?;
            if row.is_tombstone() {
                tombstones.push(VectorTombstone::new(
                    key,
                    row.commit_version(),
                    row.commit_timestamp(),
                ));
            } else {
                let entry = Self::entry_from_row(collection, &key, &row)?;
                entries.push((row, entry));
            }
        }
        Ok(VectorQuerySnapshot {
            entries,
            tombstones,
        })
    }

    fn keys_after_cursor(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        prefix: Option<&VectorKey>,
        cursor: Option<&VectorKey>,
        limit: usize,
        selector: ReadSelector,
    ) -> EngineResult<Vec<VectorKey>> {
        let prefix_start = encode_vector_collection_entry_prefix(&self.space, collection);
        let prefix_end = next_prefix(&prefix_start);
        let mut start = prefix_start.clone();
        let mut keys = Vec::with_capacity(limit);
        while keys.len() < limit && start < prefix_end {
            let remaining = limit.saturating_sub(keys.len());
            let raw_limit = remaining
                .saturating_add(1)
                .clamp(VECTOR_LIST_RAW_PAGE_MIN, VECTOR_LIST_RAW_PAGE_MAX);
            let rows = self.persistence.scan_range(
                record.storage_branch_id(),
                RowClass::Vector,
                Some(start.clone()),
                Some(prefix_end.clone()),
                selector,
                Some(raw_limit),
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                if row.is_tombstone() {
                    continue;
                }
                let (_, key) = decode_vector_key(&self.space, row.key())?;
                if cursor.is_some_and(|cursor| key.as_str() <= cursor.as_str()) {
                    continue;
                }
                if let Some(prefix) = prefix {
                    if !key.as_str().starts_with(prefix.as_str()) {
                        continue;
                    }
                }
                keys.push(key);
                if keys.len() >= limit {
                    break;
                }
            }
            let last_raw_key = rows.last().expect("non-empty raw page").key();
            start = exclusive_after_key(last_raw_key);
        }
        keys.sort();
        Ok(keys)
    }

    fn scan_range_limited(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        mut start: Vec<u8>,
        end: &[u8],
        limit: usize,
    ) -> EngineResult<Vec<VectorVersionedEntry>> {
        let mut visible = Vec::with_capacity(limit.min(VECTOR_LIST_RAW_PAGE_MIN));
        while visible.len() < limit && start.as_slice() < end {
            let remaining = limit.saturating_sub(visible.len());
            let raw_limit = remaining.clamp(VECTOR_LIST_RAW_PAGE_MIN, VECTOR_LIST_RAW_PAGE_MAX);
            let rows = self.persistence.scan_range(
                record.storage_branch_id(),
                RowClass::Vector,
                Some(start.clone()),
                Some(end.to_owned()),
                ReadSelector::Latest,
                Some(raw_limit),
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                if row.is_tombstone() {
                    continue;
                }
                visible.push(self.scan_entry_from_row(collection, row)?);
                if visible.len() >= limit {
                    break;
                }
            }
            let last_raw_key = rows.last().expect("non-empty raw page").key();
            start = exclusive_after_key(last_raw_key);
        }
        Ok(visible)
    }

    fn scan_entry_from_row(
        &self,
        collection: &VectorCollectionName,
        row: &PersistenceReadRow,
    ) -> EngineResult<VectorVersionedEntry> {
        let (_, key) = decode_vector_key(&self.space, row.key())?;
        let entry = Self::entry_from_row(collection, &key, row)?;
        Ok(VectorVersionedEntry::new(
            entry,
            row.commit_version(),
            row.commit_timestamp(),
        ))
    }

    fn visible_vector_exists(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        key: &VectorKey,
    ) -> EngineResult<bool> {
        let address = self.vector_address(record, collection, key);
        Ok(self
            .persistence
            .read_row(address, ReadSelector::Latest)?
            .is_some_and(|row| !row.is_tombstone()))
    }

    fn latest_vector_revision(
        &mut self,
        record: &BranchCatalogRecord,
        collection: &VectorCollectionName,
        key: &VectorKey,
    ) -> EngineResult<Option<u64>> {
        let address = self.vector_address(record, collection, key);
        if let Some(row) = self
            .persistence
            .read_row(address.clone(), ReadSelector::Latest)?
        {
            if !row.is_tombstone() {
                return Ok(Some(
                    Self::vector_record_from_row(collection, key, &row)?.vector_revision(),
                ));
            }
        }
        for row in self.persistence.read_history(&address, false)? {
            if row.is_tombstone() {
                continue;
            }
            return Ok(Some(
                Self::vector_record_from_row(collection, key, &row)?.vector_revision(),
            ));
        }
        Ok(None)
    }

    fn versioned_entry_from_row(
        collection: &VectorCollectionName,
        key: &VectorKey,
        row: &PersistenceReadRow,
    ) -> EngineResult<Option<VectorVersionedEntry>> {
        if row.is_tombstone() {
            return Ok(None);
        }
        Ok(Some(VectorVersionedEntry::new(
            Self::entry_from_row(collection, key, row)?,
            row.commit_version(),
            row.commit_timestamp(),
        )))
    }

    fn history_row_from_row(
        collection: &VectorCollectionName,
        key: &VectorKey,
        row: &PersistenceReadRow,
    ) -> EngineResult<VectorHistoryRow> {
        if row.is_tombstone() {
            return Ok(VectorHistoryRow::new(
                None,
                true,
                row.commit_version(),
                row.commit_timestamp(),
                None,
            ));
        }
        let entry = Self::entry_from_row(collection, key, row)?;
        let revision = entry.vector_revision();
        Ok(VectorHistoryRow::new(
            Some(entry),
            false,
            row.commit_version(),
            row.commit_timestamp(),
            Some(revision),
        ))
    }

    fn entry_from_row(
        collection: &VectorCollectionName,
        key: &VectorKey,
        row: &PersistenceReadRow,
    ) -> EngineResult<VectorEntry> {
        let vector = Self::vector_record_from_row(collection, key, row)?;
        Ok(VectorEntry::new(
            vector.key().clone(),
            vector.embedding().clone(),
            vector.metadata().cloned(),
            vector.vector_revision(),
        ))
    }

    fn vector_record_from_row(
        collection: &VectorCollectionName,
        key: &VectorKey,
        row: &PersistenceReadRow,
    ) -> EngineResult<VectorRecord> {
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.vector_record",
                "stored vector row is missing a value",
            )
        })?;
        decode_vector_record(collection, key, value)
    }

    fn commit_batch(
        &mut self,
        record: &BranchCatalogRecord,
        mutations: Vec<RowMutation>,
    ) -> EngineResult<CommitOutcome> {
        let mut mutations = mutations;
        if mutations.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_batch",
                "vector batch must contain at least one mutation",
            ));
        }
        let user_put_count = mutations
            .iter()
            .filter(|mutation| mutation.is_put())
            .count();
        let user_delete_count = mutations
            .iter()
            .filter(|mutation| mutation.is_delete())
            .count();
        let mut space_mutations =
            ControlPlane::space_registration_mutations(self.persistence, record, &self.space)?;
        if !space_mutations.is_empty() {
            space_mutations.extend(mutations);
            mutations = space_mutations;
        }
        let plan = CommitPlan::new(
            record.storage_branch_id(),
            mutations,
            Some(record.generation()),
        );
        Ok(self
            .persistence
            .commit(&plan)?
            .with_counts(user_put_count, user_delete_count))
    }
}

fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return upper;
        }
    }
    vec![u8::MAX]
}

#[allow(dead_code)]
fn active_delta_count_for_refs(
    snapshot: &VectorQuerySnapshot,
    artifact_refs: &[VectorArtifactRef],
) -> usize {
    // Use the maximum covered version, matching active_delta_watermark. The
    // bounded read treats only rows past the newest sealed source as active;
    // aggregating with min would count rows in (min, max] that a higher-covered
    // source already sealed, over-reporting the active-delta size right after a
    // multi-source seal (findings U61/U68).
    let Some(watermark) = artifact_refs
        .iter()
        .map(VectorArtifactRef::covered_commit_version)
        .max()
    else {
        return snapshot
            .entries
            .len()
            .saturating_add(snapshot.tombstones.len());
    };
    snapshot
        .entries
        .iter()
        .filter(|(row, _)| row.commit_version() > watermark)
        .count()
        .saturating_add(
            snapshot
                .tombstones
                .iter()
                .filter(|tombstone| tombstone.version() > watermark)
                .count(),
        )
}

#[cfg_attr(not(any(test, feature = "testkit")), allow(dead_code))]
fn rows_covered_commit_version(rows: &[(PersistenceReadRow, VectorEntry)]) -> CommitVersion {
    rows.iter()
        .map(|(row, _)| row.commit_version())
        .max()
        .unwrap_or_else(|| CommitVersion::new(0))
}

#[allow(dead_code)]
fn source_covered_commit_version(
    space: &ProductSpace,
    collection: &VectorCollectionName,
    source: &PersistenceImmutableSource,
) -> EngineResult<CommitVersion> {
    let mut watermark = CommitVersion::new(0);
    for row in source.rows() {
        let (row_collection, _) = decode_vector_key(space, row.key())?;
        if row_collection == *collection && row.commit_version() > watermark {
            watermark = row.commit_version();
        }
    }
    Ok(watermark)
}

fn persistence_row_is_newer(left: &PersistenceReadRow, right: &PersistenceReadRow) -> bool {
    left.commit_version() > right.commit_version()
        || (left.commit_version() == right.commit_version()
            && left.commit_timestamp() > right.commit_timestamp())
}

fn exclusive_after_key(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}
