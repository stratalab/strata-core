//! Internal vector index artifact payloads.

#![cfg_attr(not(any(test, feature = "testkit")), allow(dead_code))]

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use strata_core::{BranchId, CommitVersion, Timestamp};

use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;
use crate::persistence::PersistenceReadRow;

use super::index::CachedHnswArtifact;
use super::types::MAX_VECTOR_DIMENSION;
use super::{
    VectorArtifactKind, VectorArtifactRef, VectorCollectionName, VectorDistanceMetric,
    VectorEmbedding, VectorEntry, VectorKey, VectorMetadata,
};

const FLAT_ARTIFACT_MAGIC: &[u8] = b"SVFLATIDX";
const HNSW_ARTIFACT_MAGIC: &[u8] = b"SVHNSWIDX";
const FLAT_ARTIFACT_FORMAT_VERSION: u8 = 1;
const HNSW_ARTIFACT_FORMAT_VERSION: u8 = 1;
const DEFAULT_FLAT_ARTIFACT_LOAD_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_HNSW_ARTIFACT_LOAD_BUDGET_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_FLAT_ARTIFACT_BUILD_BUDGET_BYTES: usize = DEFAULT_FLAT_ARTIFACT_LOAD_BUDGET_BYTES;
const DEFAULT_HNSW_ARTIFACT_BUILD_BUDGET_BYTES: usize = DEFAULT_HNSW_ARTIFACT_LOAD_BUDGET_BYTES;
const DEFAULT_ARTIFACT_MEMORY_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_HNSW_RUNTIME_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_FLAT_RUNTIME_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const MAX_ID_BYTES: usize = 1024;

pub(crate) const DEFAULT_HNSW_M: u16 = 16;
pub(crate) const DEFAULT_HNSW_EF_CONSTRUCTION: u16 = 200;
pub(crate) const DEFAULT_HNSW_EF_SEARCH: u16 = 80;
pub(crate) const DEFAULT_HNSW_SEED: u64 = 0x5354_5241_5441_484e;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct VectorArtifactId(String);

impl VectorArtifactId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, EngineError> {
        let value = value.into();
        validate_artifact_text(&value, "artifact id")?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VectorSourceId(String);

impl VectorSourceId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, EngineError> {
        let value = value.into();
        validate_artifact_text(&value, "artifact source id")?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VectorFlatArtifactIdentity {
    artifact_id: VectorArtifactId,
    branch_id: BranchId,
    space: ProductSpace,
    collection: VectorCollectionName,
    collection_generation: u64,
    source_id: VectorSourceId,
    source_branch_id: BranchId,
    source_generation: u64,
    vector_dimension: usize,
    metric: VectorDistanceMetric,
}

impl VectorFlatArtifactIdentity {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        artifact_id: VectorArtifactId,
        branch_id: BranchId,
        space: ProductSpace,
        collection: VectorCollectionName,
        collection_generation: u64,
        source_id: VectorSourceId,
        source_branch_id: BranchId,
        source_generation: u64,
        vector_dimension: usize,
        metric: VectorDistanceMetric,
    ) -> Result<Self, EngineError> {
        if vector_dimension == 0 || vector_dimension > MAX_VECTOR_DIMENSION {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_artifact",
                "flat vector artifact dimension is outside the supported range",
            ));
        }
        Ok(Self {
            artifact_id,
            branch_id,
            space,
            collection,
            collection_generation,
            source_id,
            source_branch_id,
            source_generation,
            vector_dimension,
            metric,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn from_manifest_ref(
        space: ProductSpace,
        collection: VectorCollectionName,
        collection_generation: u64,
        artifact_ref: &VectorArtifactRef,
    ) -> Result<Self, EngineError> {
        Self::from_manifest_ref_with_kind(
            space,
            collection,
            collection_generation,
            artifact_ref,
            VectorArtifactKind::Flat,
            "manifest artifact ref is not a flat vector artifact",
        )
    }

    #[allow(dead_code)]
    pub(crate) fn from_hnsw_manifest_ref(
        space: ProductSpace,
        collection: VectorCollectionName,
        collection_generation: u64,
        artifact_ref: &VectorArtifactRef,
    ) -> Result<Self, EngineError> {
        Self::from_manifest_ref_with_kind(
            space,
            collection,
            collection_generation,
            artifact_ref,
            VectorArtifactKind::Hnsw,
            "manifest artifact ref is not an HNSW vector artifact",
        )
    }

    fn from_manifest_ref_with_kind(
        space: ProductSpace,
        collection: VectorCollectionName,
        collection_generation: u64,
        artifact_ref: &VectorArtifactRef,
        expected_kind: VectorArtifactKind,
        message: &'static str,
    ) -> Result<Self, EngineError> {
        if artifact_ref.index_kind() != expected_kind {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_artifact",
                message,
            ));
        }
        Self::new(
            VectorArtifactId::new(artifact_ref.artifact_id())?,
            artifact_ref.source_branch_id(),
            space,
            collection,
            collection_generation,
            VectorSourceId::new(artifact_ref.source_id())?,
            artifact_ref.source_branch_id(),
            artifact_ref.source_generation(),
            artifact_ref.vector_dimension(),
            artifact_ref.metric(),
        )
    }

    pub(crate) fn artifact_id(&self) -> &VectorArtifactId {
        &self.artifact_id
    }

    pub(crate) const fn source_branch_id(&self) -> BranchId {
        self.source_branch_id
    }

    pub(crate) const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    pub(crate) const fn vector_dimension(&self) -> usize {
        self.vector_dimension
    }

    pub(crate) const fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    pub(crate) fn source_id(&self) -> &VectorSourceId {
        &self.source_id
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FlatVectorArtifactRow {
    key: VectorKey,
    commit_version: CommitVersion,
    timestamp: Timestamp,
    vector_revision: u64,
    embedding: VectorEmbedding,
    metadata: Option<VectorMetadata>,
}

impl FlatVectorArtifactRow {
    pub(crate) fn from_visible_entry(row: &PersistenceReadRow, entry: &VectorEntry) -> Self {
        Self {
            key: entry.key().clone(),
            commit_version: row.commit_version(),
            timestamp: row.commit_timestamp(),
            vector_revision: entry.vector_revision(),
            embedding: entry.embedding().clone(),
            metadata: entry.metadata().cloned(),
        }
    }

    pub(crate) fn key(&self) -> &VectorKey {
        &self.key
    }

    pub(crate) const fn commit_version(&self) -> CommitVersion {
        self.commit_version
    }

    pub(crate) const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub(crate) fn embedding(&self) -> &VectorEmbedding {
        &self.embedding
    }

    pub(crate) fn metadata(&self) -> Option<&VectorMetadata> {
        self.metadata.as_ref()
    }

    pub(crate) fn to_entry(&self) -> VectorEntry {
        VectorEntry::new(
            self.key.clone(),
            self.embedding.clone(),
            self.metadata.clone(),
            self.vector_revision,
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FlatVectorArtifact {
    identity: VectorFlatArtifactIdentity,
    rows: Vec<FlatVectorArtifactRow>,
}

impl FlatVectorArtifact {
    pub(crate) fn new(
        identity: VectorFlatArtifactIdentity,
        mut rows: Vec<FlatVectorArtifactRow>,
    ) -> Result<Self, EngineError> {
        rows.sort_by(|left, right| left.key().cmp(right.key()));
        for row in &rows {
            if row.embedding.dimension() != identity.vector_dimension {
                return Err(EngineError::invalid_input(
                    "invalid_argument.engine.vector_artifact",
                    "flat vector artifact row dimension does not match artifact identity",
                ));
            }
        }
        Ok(Self { identity, rows })
    }

    pub(crate) fn from_visible_entries(
        identity: VectorFlatArtifactIdentity,
        entries: &[(PersistenceReadRow, VectorEntry)],
    ) -> Result<Self, EngineError> {
        let rows = entries
            .iter()
            .map(|(row, entry)| FlatVectorArtifactRow::from_visible_entry(row, entry))
            .collect();
        Self::new(identity, rows)
    }

    pub(crate) fn from_visible_entries_with_budget(
        identity: VectorFlatArtifactIdentity,
        entries: &[(PersistenceReadRow, VectorEntry)],
        max_bytes: usize,
    ) -> Result<Self, EngineError> {
        check_artifact_build_budget(
            "flat",
            estimate_flat_vector_artifact_entries_encoded_bytes(&identity, entries)?,
            max_bytes,
        )?;
        Self::from_visible_entries(identity, entries)
    }

    pub(crate) fn identity(&self) -> &VectorFlatArtifactIdentity {
        &self.identity
    }

    pub(crate) fn rows(&self) -> &[FlatVectorArtifactRow] {
        &self.rows
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HnswArtifactConfig {
    m: u16,
    ef_construction: u16,
    ef_search: u16,
    seed: u64,
}

impl HnswArtifactConfig {
    pub(crate) const fn default_for_engine() -> Self {
        Self {
            m: DEFAULT_HNSW_M,
            ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
            ef_search: DEFAULT_HNSW_EF_SEARCH,
            seed: DEFAULT_HNSW_SEED,
        }
    }

    fn validate(self) -> Result<(), EngineError> {
        if self.m < 2 || self.ef_construction < self.m || self.ef_search == 0 {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_artifact",
                "HNSW vector artifact configuration is invalid",
            ));
        }
        Ok(())
    }

    pub(crate) const fn m(self) -> usize {
        self.m as usize
    }

    pub(crate) const fn ef_construction(self) -> usize {
        self.ef_construction as usize
    }

    pub(crate) const fn ef_search(self) -> usize {
        self.ef_search as usize
    }

    pub(crate) const fn seed(self) -> u64 {
        self.seed
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HnswVectorArtifact {
    identity: VectorFlatArtifactIdentity,
    config: HnswArtifactConfig,
    rows: Vec<FlatVectorArtifactRow>,
}

impl HnswVectorArtifact {
    pub(crate) fn new(
        identity: VectorFlatArtifactIdentity,
        config: HnswArtifactConfig,
        mut rows: Vec<FlatVectorArtifactRow>,
    ) -> Result<Self, EngineError> {
        config.validate()?;
        rows.sort_by(|left, right| left.key().cmp(right.key()));
        for row in &rows {
            if row.embedding.dimension() != identity.vector_dimension {
                return Err(EngineError::invalid_input(
                    "invalid_argument.engine.vector_artifact",
                    "HNSW vector artifact row dimension does not match artifact identity",
                ));
            }
        }
        Ok(Self {
            identity,
            config,
            rows,
        })
    }

    pub(crate) fn from_visible_entries(
        identity: VectorFlatArtifactIdentity,
        config: HnswArtifactConfig,
        entries: &[(PersistenceReadRow, VectorEntry)],
    ) -> Result<Self, EngineError> {
        let rows = entries
            .iter()
            .map(|(row, entry)| FlatVectorArtifactRow::from_visible_entry(row, entry))
            .collect();
        Self::new(identity, config, rows)
    }

    pub(crate) fn from_visible_entries_with_budget(
        identity: VectorFlatArtifactIdentity,
        config: HnswArtifactConfig,
        entries: &[(PersistenceReadRow, VectorEntry)],
        max_bytes: usize,
    ) -> Result<Self, EngineError> {
        check_artifact_build_budget(
            "HNSW",
            estimate_hnsw_vector_artifact_entries_encoded_bytes(&identity, config, entries)?,
            max_bytes,
        )?;
        Self::from_visible_entries(identity, config, entries)
    }

    pub(crate) fn identity(&self) -> &VectorFlatArtifactIdentity {
        &self.identity
    }

    pub(crate) const fn config(&self) -> HnswArtifactConfig {
        self.config
    }

    pub(crate) fn rows(&self) -> &[FlatVectorArtifactRow] {
        &self.rows
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VectorArtifactHandle {
    artifact_id: VectorArtifactId,
    byte_len: u64,
    checksum: u64,
    vector_count: u64,
}

impl VectorArtifactHandle {
    pub(crate) const fn byte_len(&self) -> u64 {
        self.byte_len
    }

    pub(crate) const fn checksum(&self) -> u64 {
        self.checksum
    }

    pub(crate) const fn vector_count(&self) -> u64 {
        self.vector_count
    }

    pub(crate) fn artifact_id(&self) -> &VectorArtifactId {
        &self.artifact_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VectorArtifactLoadStatus {
    Loaded,
    Missing,
    Corrupt,
    Stale,
    OverBudget,
}

impl VectorArtifactLoadStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Loaded => "loaded",
            Self::Missing => "missing",
            Self::Corrupt => "corrupt",
            Self::Stale => "stale",
            Self::OverBudget => "over_budget",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct VectorArtifactLoadReport {
    status: VectorArtifactLoadStatus,
    byte_len: Option<u64>,
    checksum: Option<u64>,
}

impl VectorArtifactLoadReport {
    fn new(status: VectorArtifactLoadStatus, byte_len: Option<u64>, checksum: Option<u64>) -> Self {
        Self {
            status,
            byte_len,
            checksum,
        }
    }

    pub(crate) const fn status(&self) -> VectorArtifactLoadStatus {
        self.status
    }

    pub(crate) const fn byte_len(&self) -> Option<u64> {
        self.byte_len
    }

    pub(crate) const fn checksum(&self) -> Option<u64> {
        self.checksum
    }
}

pub(crate) enum VectorFlatArtifactLoad {
    Loaded {
        #[allow(dead_code)]
        artifact: FlatVectorArtifact,
        report: VectorArtifactLoadReport,
    },
    Skipped(VectorArtifactLoadReport),
}

impl VectorFlatArtifactLoad {
    pub(crate) fn report(&self) -> &VectorArtifactLoadReport {
        match self {
            Self::Loaded { report, .. } | Self::Skipped(report) => report,
        }
    }
}

pub(crate) enum VectorHnswArtifactLoad {
    Loaded {
        #[allow(dead_code)]
        artifact: HnswVectorArtifact,
        report: VectorArtifactLoadReport,
    },
    Skipped(VectorArtifactLoadReport),
}

impl VectorHnswArtifactLoad {
    pub(crate) fn report(&self) -> &VectorArtifactLoadReport {
        match self {
            Self::Loaded { report, .. } | Self::Skipped(report) => report,
        }
    }
}

pub(crate) struct VectorArtifactStore {
    flat_payloads: BTreeMap<VectorArtifactId, Vec<u8>>,
    hnsw_payloads: BTreeMap<VectorArtifactId, Vec<u8>>,
    payload_order: VecDeque<VectorArtifactCacheEntry>,
    memory_payload_bytes: usize,
    memory_budget_bytes: usize,
    // Built HNSW graphs keyed by artifact id, so the graph is constructed once per artifact load
    // and reused across queries instead of rebuilt on every query. Pure in-memory derived state
    // in both cache and durable modes; bounded by `hnsw_runtime_budget_bytes` and evicted LRU.
    hnsw_runtime: BTreeMap<VectorArtifactId, HnswRuntimeCacheEntry>,
    hnsw_runtime_order: VecDeque<VectorArtifactId>,
    hnsw_runtime_bytes: usize,
    hnsw_runtime_budget_bytes: usize,
    // Parsed flat artifacts keyed by artifact id, so the payload is decoded once per load and
    // reused across queries instead of re-decoded each query (including when it is loaded only as
    // a fallback for an HNSW query). Same in-memory, budget + LRU contract as `hnsw_runtime`.
    flat_runtime: BTreeMap<VectorArtifactId, FlatRuntimeCacheEntry>,
    flat_runtime_order: VecDeque<VectorArtifactId>,
    flat_runtime_bytes: usize,
    flat_runtime_budget_bytes: usize,
    flat_root: Option<PathBuf>,
    hnsw_root: Option<PathBuf>,
}

impl Default for VectorArtifactStore {
    fn default() -> Self {
        Self {
            flat_payloads: BTreeMap::new(),
            hnsw_payloads: BTreeMap::new(),
            payload_order: VecDeque::new(),
            memory_payload_bytes: 0,
            memory_budget_bytes: DEFAULT_ARTIFACT_MEMORY_BUDGET_BYTES,
            hnsw_runtime: BTreeMap::new(),
            hnsw_runtime_order: VecDeque::new(),
            hnsw_runtime_bytes: 0,
            hnsw_runtime_budget_bytes: DEFAULT_HNSW_RUNTIME_BUDGET_BYTES,
            flat_runtime: BTreeMap::new(),
            flat_runtime_order: VecDeque::new(),
            flat_runtime_bytes: 0,
            flat_runtime_budget_bytes: DEFAULT_FLAT_RUNTIME_BUDGET_BYTES,
            flat_root: None,
            hnsw_root: None,
        }
    }
}

impl VectorArtifactStore {
    pub(crate) fn memory() -> Self {
        Self::default()
    }

    pub(crate) fn durable_local(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            flat_root: Some(root.join("flat")),
            hnsw_root: Some(root.join("hnsw")),
            ..Self::default()
        }
    }

    /// Return a cached built HNSW graph for `artifact_id` when one is present and its source
    /// bytes still match `expected_checksum` (i.e. the artifact has not been re-sealed). Touches
    /// LRU order on a hit. A miss returns `None` and the caller loads + builds via
    /// `insert_hnsw_runtime`.
    pub(crate) fn cached_hnsw_runtime(
        &mut self,
        artifact_id: &VectorArtifactId,
        expected_checksum: u64,
    ) -> Option<Arc<CachedHnswArtifact>> {
        let entry = self.hnsw_runtime.get(artifact_id)?;
        if entry.checksum != expected_checksum {
            return None;
        }
        let cached = Arc::clone(&entry.cached);
        self.touch_hnsw_runtime(artifact_id);
        Some(cached)
    }

    /// Build the HNSW graph for `artifact` once, cache it keyed by `artifact_id` + `checksum`,
    /// and return a shared handle. Replaces any stale entry for the same id and evicts LRU
    /// entries while over the runtime budget. Eviction never affects correctness: an evicted
    /// graph is rebuilt on its next load, and the returned handle stays alive for this query.
    pub(crate) fn insert_hnsw_runtime(
        &mut self,
        artifact_id: VectorArtifactId,
        checksum: u64,
        artifact: HnswVectorArtifact,
    ) -> Arc<CachedHnswArtifact> {
        self.remove_hnsw_runtime(&artifact_id);
        let cached = Arc::new(CachedHnswArtifact::build(artifact));
        let estimated_bytes = cached.estimated_memory_bytes();
        self.hnsw_runtime.insert(
            artifact_id.clone(),
            HnswRuntimeCacheEntry {
                cached: Arc::clone(&cached),
                checksum,
                estimated_bytes,
            },
        );
        self.hnsw_runtime_order.push_back(artifact_id);
        self.hnsw_runtime_bytes = self.hnsw_runtime_bytes.saturating_add(estimated_bytes);
        self.evict_hnsw_runtime();
        cached
    }

    fn touch_hnsw_runtime(&mut self, artifact_id: &VectorArtifactId) {
        self.hnsw_runtime_order.retain(|id| id != artifact_id);
        self.hnsw_runtime_order.push_back(artifact_id.clone());
    }

    fn remove_hnsw_runtime(&mut self, artifact_id: &VectorArtifactId) {
        self.hnsw_runtime_order.retain(|id| id != artifact_id);
        if let Some(entry) = self.hnsw_runtime.remove(artifact_id) {
            self.hnsw_runtime_bytes = self
                .hnsw_runtime_bytes
                .saturating_sub(entry.estimated_bytes);
        }
    }

    fn evict_hnsw_runtime(&mut self) {
        while self.hnsw_runtime_bytes > self.hnsw_runtime_budget_bytes {
            let Some(artifact_id) = self.hnsw_runtime_order.pop_front() else {
                self.hnsw_runtime_bytes = 0;
                break;
            };
            if let Some(entry) = self.hnsw_runtime.remove(&artifact_id) {
                self.hnsw_runtime_bytes = self
                    .hnsw_runtime_bytes
                    .saturating_sub(entry.estimated_bytes);
            }
        }
    }

    /// Return a cached parsed flat artifact for `artifact_id` when present and its source bytes
    /// still match `expected_checksum`. Touches LRU on a hit; a miss returns `None` and the caller
    /// decodes once via `insert_flat_runtime`.
    pub(crate) fn cached_flat_runtime(
        &mut self,
        artifact_id: &VectorArtifactId,
        expected_checksum: u64,
    ) -> Option<Arc<FlatVectorArtifact>> {
        let entry = self.flat_runtime.get(artifact_id)?;
        if entry.checksum != expected_checksum {
            return None;
        }
        let cached = Arc::clone(&entry.cached);
        self.touch_flat_runtime(artifact_id);
        Some(cached)
    }

    /// Cache a parsed flat artifact keyed by `artifact_id` + `checksum` and return a shared handle.
    /// Replaces any stale entry and evicts LRU entries while over the runtime budget. Eviction is a
    /// performance event only: an evicted artifact is re-decoded on its next load.
    pub(crate) fn insert_flat_runtime(
        &mut self,
        artifact_id: VectorArtifactId,
        checksum: u64,
        artifact: FlatVectorArtifact,
    ) -> Arc<FlatVectorArtifact> {
        self.remove_flat_runtime(&artifact_id);
        let estimated_bytes = estimate_flat_artifact_memory_bytes(&artifact);
        let cached = Arc::new(artifact);
        self.flat_runtime.insert(
            artifact_id.clone(),
            FlatRuntimeCacheEntry {
                cached: Arc::clone(&cached),
                checksum,
                estimated_bytes,
            },
        );
        self.flat_runtime_order.push_back(artifact_id);
        self.flat_runtime_bytes = self.flat_runtime_bytes.saturating_add(estimated_bytes);
        self.evict_flat_runtime();
        cached
    }

    fn touch_flat_runtime(&mut self, artifact_id: &VectorArtifactId) {
        self.flat_runtime_order.retain(|id| id != artifact_id);
        self.flat_runtime_order.push_back(artifact_id.clone());
    }

    fn remove_flat_runtime(&mut self, artifact_id: &VectorArtifactId) {
        self.flat_runtime_order.retain(|id| id != artifact_id);
        if let Some(entry) = self.flat_runtime.remove(artifact_id) {
            self.flat_runtime_bytes = self
                .flat_runtime_bytes
                .saturating_sub(entry.estimated_bytes);
        }
    }

    fn evict_flat_runtime(&mut self) {
        while self.flat_runtime_bytes > self.flat_runtime_budget_bytes {
            let Some(artifact_id) = self.flat_runtime_order.pop_front() else {
                self.flat_runtime_bytes = 0;
                break;
            };
            if let Some(entry) = self.flat_runtime.remove(&artifact_id) {
                self.flat_runtime_bytes = self
                    .flat_runtime_bytes
                    .saturating_sub(entry.estimated_bytes);
            }
        }
    }

    pub(crate) fn store_flat(
        &mut self,
        artifact: &FlatVectorArtifact,
    ) -> Result<VectorArtifactHandle, EngineError> {
        self.store_flat_with_budget(artifact, DEFAULT_FLAT_ARTIFACT_BUILD_BUDGET_BYTES)
    }

    fn store_flat_with_budget(
        &mut self,
        artifact: &FlatVectorArtifact,
        max_bytes: usize,
    ) -> Result<VectorArtifactHandle, EngineError> {
        check_artifact_build_budget(
            "flat",
            estimate_flat_vector_artifact_encoded_bytes(artifact)?,
            max_bytes,
        )?;
        let bytes = encode_flat_vector_artifact(artifact)?;
        check_artifact_build_budget("flat", bytes.len(), max_bytes)?;
        let handle = VectorArtifactHandle {
            artifact_id: artifact.identity().artifact_id().clone(),
            byte_len: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            checksum: artifact_payload_checksum(payload_body(&bytes)?),
            vector_count: u64::try_from(artifact.rows().len()).unwrap_or(u64::MAX),
        };
        // Artifact payloads are rebuildable from committed rows, so durable write
        // failure must not make explicit index maintenance fail or affect reads.
        let _ = self.persist_raw_flat_payload(&handle.artifact_id, &bytes);
        self.cache_payload(
            VectorArtifactPayloadKind::Flat,
            handle.artifact_id().clone(),
            bytes,
        );
        Ok(handle)
    }

    pub(crate) fn store_hnsw(
        &mut self,
        artifact: &HnswVectorArtifact,
    ) -> Result<VectorArtifactHandle, EngineError> {
        self.store_hnsw_with_budget(artifact, DEFAULT_HNSW_ARTIFACT_BUILD_BUDGET_BYTES)
    }

    fn store_hnsw_with_budget(
        &mut self,
        artifact: &HnswVectorArtifact,
        max_bytes: usize,
    ) -> Result<VectorArtifactHandle, EngineError> {
        check_artifact_build_budget(
            "HNSW",
            estimate_hnsw_vector_artifact_encoded_bytes(artifact)?,
            max_bytes,
        )?;
        let bytes = encode_hnsw_vector_artifact(artifact)?;
        check_artifact_build_budget("HNSW", bytes.len(), max_bytes)?;
        let handle = VectorArtifactHandle {
            artifact_id: artifact.identity().artifact_id().clone(),
            byte_len: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            checksum: artifact_payload_checksum(payload_body(&bytes)?),
            vector_count: u64::try_from(artifact.rows().len()).unwrap_or(u64::MAX),
        };
        // Artifact payloads are rebuildable from committed rows, so durable write
        // failure must not make explicit index maintenance fail or affect reads.
        let _ = self.persist_raw_hnsw_payload(&handle.artifact_id, &bytes);
        self.cache_payload(
            VectorArtifactPayloadKind::Hnsw,
            handle.artifact_id().clone(),
            bytes,
        );
        Ok(handle)
    }

    pub(crate) fn load_flat(
        &self,
        expected: &VectorFlatArtifactIdentity,
        max_bytes: usize,
    ) -> VectorFlatArtifactLoad {
        let payload = match self.flat_payloads.get(expected.artifact_id()) {
            Some(bytes) => FlatPayload::Borrowed(bytes),
            None => match self.load_durable_flat_payload(expected.artifact_id()) {
                Ok(Some(bytes)) => FlatPayload::Owned(bytes),
                Ok(None) => {
                    return VectorFlatArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                        VectorArtifactLoadStatus::Missing,
                        None,
                        None,
                    ));
                }
                Err(()) => {
                    return VectorFlatArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                        VectorArtifactLoadStatus::Corrupt,
                        None,
                        None,
                    ));
                }
            },
        };
        let bytes = payload.as_slice();
        let byte_len = Some(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let checksum = payload_body(bytes).ok().map(artifact_payload_checksum);
        if bytes.len() > max_bytes {
            return VectorFlatArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                VectorArtifactLoadStatus::OverBudget,
                byte_len,
                checksum,
            ));
        }
        let Ok(artifact) = decode_flat_vector_artifact(bytes, max_bytes) else {
            return VectorFlatArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                VectorArtifactLoadStatus::Corrupt,
                byte_len,
                checksum,
            ));
        };
        if artifact.identity() != expected {
            return VectorFlatArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                VectorArtifactLoadStatus::Stale,
                byte_len,
                checksum,
            ));
        }
        VectorFlatArtifactLoad::Loaded {
            artifact,
            report: VectorArtifactLoadReport::new(
                VectorArtifactLoadStatus::Loaded,
                byte_len,
                checksum,
            ),
        }
    }

    pub(crate) fn load_hnsw(
        &self,
        expected: &VectorFlatArtifactIdentity,
        max_bytes: usize,
    ) -> VectorHnswArtifactLoad {
        let payload = match self.hnsw_payloads.get(expected.artifact_id()) {
            Some(bytes) => FlatPayload::Borrowed(bytes),
            None => match self.load_durable_hnsw_payload(expected.artifact_id()) {
                Ok(Some(bytes)) => FlatPayload::Owned(bytes),
                Ok(None) => {
                    return VectorHnswArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                        VectorArtifactLoadStatus::Missing,
                        None,
                        None,
                    ));
                }
                Err(()) => {
                    return VectorHnswArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                        VectorArtifactLoadStatus::Corrupt,
                        None,
                        None,
                    ));
                }
            },
        };
        let bytes = payload.as_slice();
        let byte_len = Some(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let checksum = payload_body(bytes).ok().map(artifact_payload_checksum);
        if bytes.len() > max_bytes {
            return VectorHnswArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                VectorArtifactLoadStatus::OverBudget,
                byte_len,
                checksum,
            ));
        }
        let Ok(artifact) = decode_hnsw_vector_artifact(bytes, max_bytes) else {
            return VectorHnswArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                VectorArtifactLoadStatus::Corrupt,
                byte_len,
                checksum,
            ));
        };
        if artifact.identity() != expected {
            return VectorHnswArtifactLoad::Skipped(VectorArtifactLoadReport::new(
                VectorArtifactLoadStatus::Stale,
                byte_len,
                checksum,
            ));
        }
        VectorHnswArtifactLoad::Loaded {
            artifact,
            report: VectorArtifactLoadReport::new(
                VectorArtifactLoadStatus::Loaded,
                byte_len,
                checksum,
            ),
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn put_raw_flat_for_test(
        &mut self,
        artifact_id: VectorArtifactId,
        bytes: Vec<u8>,
    ) -> Result<(), EngineError> {
        self.persist_raw_flat_payload(&artifact_id, &bytes)?;
        self.cache_payload(VectorArtifactPayloadKind::Flat, artifact_id, bytes);
        Ok(())
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn remove_for_test(
        &mut self,
        artifact_id: &VectorArtifactId,
    ) -> Result<(), EngineError> {
        self.remove_memory_payload(VectorArtifactPayloadKind::Flat, artifact_id);
        let Some(path) = self.flat_payload_path(artifact_id) else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(vector_artifact_io_error(
                "could not remove vector artifact payload",
                error,
            )),
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn put_raw_hnsw_for_test(
        &mut self,
        artifact_id: VectorArtifactId,
        bytes: Vec<u8>,
    ) -> Result<(), EngineError> {
        self.persist_raw_hnsw_payload(&artifact_id, &bytes)?;
        self.cache_payload(VectorArtifactPayloadKind::Hnsw, artifact_id, bytes);
        Ok(())
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn remove_hnsw_for_test(
        &mut self,
        artifact_id: &VectorArtifactId,
    ) -> Result<(), EngineError> {
        self.remove_memory_payload(VectorArtifactPayloadKind::Hnsw, artifact_id);
        let Some(path) = self.hnsw_payload_path(artifact_id) else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(vector_artifact_io_error(
                "could not remove vector artifact payload",
                error,
            )),
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    #[allow(dead_code)]
    fn store_flat_with_budget_for_test(
        &mut self,
        artifact: &FlatVectorArtifact,
        max_bytes: usize,
    ) -> Result<VectorArtifactHandle, EngineError> {
        self.store_flat_with_budget(artifact, max_bytes)
    }

    #[cfg(any(test, feature = "testkit"))]
    #[allow(dead_code)]
    fn store_hnsw_with_budget_for_test(
        &mut self,
        artifact: &HnswVectorArtifact,
        max_bytes: usize,
    ) -> Result<VectorArtifactHandle, EngineError> {
        self.store_hnsw_with_budget(artifact, max_bytes)
    }

    #[cfg(any(test, feature = "testkit"))]
    #[allow(dead_code)]
    fn set_memory_budget_for_test(&mut self, bytes: usize) {
        self.memory_budget_bytes = bytes;
        self.evict_memory_payloads();
    }

    #[cfg(any(test, feature = "testkit"))]
    #[allow(dead_code)]
    fn memory_payload_bytes_for_test(&self) -> usize {
        self.memory_payload_bytes
    }

    fn persist_raw_flat_payload(
        &self,
        artifact_id: &VectorArtifactId,
        bytes: &[u8],
    ) -> Result<(), EngineError> {
        let Some(path) = self.flat_payload_path(artifact_id) else {
            return Ok(());
        };
        persist_raw_flat_payload_at(&path, bytes)
    }

    fn persist_raw_hnsw_payload(
        &self,
        artifact_id: &VectorArtifactId,
        bytes: &[u8],
    ) -> Result<(), EngineError> {
        let Some(path) = self.hnsw_payload_path(artifact_id) else {
            return Ok(());
        };
        persist_raw_flat_payload_at(&path, bytes)
    }

    fn load_durable_flat_payload(
        &self,
        artifact_id: &VectorArtifactId,
    ) -> Result<Option<Vec<u8>>, ()> {
        let Some(path) = self.flat_payload_path(artifact_id) else {
            return Ok(None);
        };
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(()),
        }
    }

    fn flat_payload_path(&self, artifact_id: &VectorArtifactId) -> Option<PathBuf> {
        self.flat_root
            .as_ref()
            .map(|root| root.join(flat_artifact_file_name(artifact_id)))
    }

    fn load_durable_hnsw_payload(
        &self,
        artifact_id: &VectorArtifactId,
    ) -> Result<Option<Vec<u8>>, ()> {
        let Some(path) = self.hnsw_payload_path(artifact_id) else {
            return Ok(None);
        };
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(()),
        }
    }

    fn hnsw_payload_path(&self, artifact_id: &VectorArtifactId) -> Option<PathBuf> {
        self.hnsw_root
            .as_ref()
            .map(|root| root.join(flat_artifact_file_name(artifact_id)))
    }

    fn cache_payload(
        &mut self,
        kind: VectorArtifactPayloadKind,
        artifact_id: VectorArtifactId,
        bytes: Vec<u8>,
    ) {
        self.remove_memory_payload(kind, &artifact_id);
        if !self.is_memory_only() || bytes.len() > self.memory_budget_bytes {
            return;
        }
        self.memory_payload_bytes = self.memory_payload_bytes.saturating_add(bytes.len());
        match kind {
            VectorArtifactPayloadKind::Flat => {
                self.flat_payloads.insert(artifact_id.clone(), bytes);
            }
            VectorArtifactPayloadKind::Hnsw => {
                self.hnsw_payloads.insert(artifact_id.clone(), bytes);
            }
        }
        self.payload_order
            .push_back(VectorArtifactCacheEntry { kind, artifact_id });
        self.evict_memory_payloads();
    }

    fn remove_memory_payload(
        &mut self,
        kind: VectorArtifactPayloadKind,
        artifact_id: &VectorArtifactId,
    ) {
        self.payload_order
            .retain(|entry| entry.kind != kind || entry.artifact_id != *artifact_id);
        let removed = match kind {
            VectorArtifactPayloadKind::Flat => self.flat_payloads.remove(artifact_id),
            VectorArtifactPayloadKind::Hnsw => self.hnsw_payloads.remove(artifact_id),
        };
        if let Some(bytes) = removed {
            self.memory_payload_bytes = self.memory_payload_bytes.saturating_sub(bytes.len());
        }
    }

    fn evict_memory_payloads(&mut self) {
        while self.memory_payload_bytes > self.memory_budget_bytes {
            let Some(entry) = self.payload_order.pop_front() else {
                self.memory_payload_bytes = 0;
                break;
            };
            let removed = match entry.kind {
                VectorArtifactPayloadKind::Flat => self.flat_payloads.remove(&entry.artifact_id),
                VectorArtifactPayloadKind::Hnsw => self.hnsw_payloads.remove(&entry.artifact_id),
            };
            if let Some(bytes) = removed {
                self.memory_payload_bytes = self.memory_payload_bytes.saturating_sub(bytes.len());
            }
        }
    }

    fn is_memory_only(&self) -> bool {
        self.flat_root.is_none() && self.hnsw_root.is_none()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VectorArtifactPayloadKind {
    Flat,
    Hnsw,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VectorArtifactCacheEntry {
    kind: VectorArtifactPayloadKind,
    artifact_id: VectorArtifactId,
}

struct HnswRuntimeCacheEntry {
    cached: Arc<CachedHnswArtifact>,
    checksum: u64,
    estimated_bytes: usize,
}

struct FlatRuntimeCacheEntry {
    cached: Arc<FlatVectorArtifact>,
    checksum: u64,
    estimated_bytes: usize,
}

fn estimate_flat_artifact_memory_bytes(artifact: &FlatVectorArtifact) -> usize {
    let per_vector_bytes = artifact
        .identity()
        .vector_dimension()
        .saturating_mul(std::mem::size_of::<f32>())
        .saturating_add(64);
    artifact.rows().len().saturating_mul(per_vector_bytes)
}

fn persist_raw_flat_payload_at(path: &Path, bytes: &[u8]) -> Result<(), EngineError> {
    let Some(parent) = path.parent() else {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_artifacts",
            "vector artifact payload path has no parent directory",
        ));
    };
    fs::create_dir_all(parent).map_err(|error| {
        vector_artifact_io_error("could not create vector artifact directory", error)
    })?;
    let mut temp_path = path.to_path_buf();
    temp_path.set_extension("tmp");
    fs::write(&temp_path, bytes).map_err(|error| {
        vector_artifact_io_error("could not write vector artifact payload", error)
    })?;
    fs::rename(&temp_path, path).map_err(|error| {
        vector_artifact_io_error("could not publish vector artifact payload", error)
    })
}

enum FlatPayload<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl FlatPayload<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }
}

fn flat_artifact_file_name(artifact_id: &VectorArtifactId) -> String {
    let digest = Sha256::digest(artifact_id.as_str().as_bytes());
    let mut name = String::with_capacity(64 + ".bin".len());
    for byte in digest {
        name.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        name.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0f)]));
    }
    name.push_str(".bin");
    name
}

fn vector_artifact_io_error(message: &'static str, error: std::io::Error) -> EngineError {
    EngineError::with_source("unavailable.engine.vector_artifacts", message, error)
}

pub(crate) const fn default_flat_artifact_load_budget_bytes() -> usize {
    DEFAULT_FLAT_ARTIFACT_LOAD_BUDGET_BYTES
}

pub(crate) const fn default_hnsw_artifact_load_budget_bytes() -> usize {
    DEFAULT_HNSW_ARTIFACT_LOAD_BUDGET_BYTES
}

pub(crate) const fn default_flat_artifact_build_budget_bytes() -> usize {
    DEFAULT_FLAT_ARTIFACT_BUILD_BUDGET_BYTES
}

pub(crate) const fn default_hnsw_artifact_build_budget_bytes() -> usize {
    DEFAULT_HNSW_ARTIFACT_BUILD_BUDGET_BYTES
}

fn estimate_flat_vector_artifact_encoded_bytes(
    artifact: &FlatVectorArtifact,
) -> Result<usize, EngineError> {
    estimate_artifact_encoded_bytes(
        FLAT_ARTIFACT_MAGIC
            .len()
            .saturating_add(1)
            .saturating_add(estimate_identity_encoded_bytes(artifact.identity()))
            .saturating_add(16),
        artifact.rows(),
        "flat",
    )
}

fn estimate_flat_vector_artifact_entries_encoded_bytes(
    identity: &VectorFlatArtifactIdentity,
    entries: &[(PersistenceReadRow, VectorEntry)],
) -> Result<usize, EngineError> {
    estimate_artifact_entries_encoded_bytes(
        FLAT_ARTIFACT_MAGIC
            .len()
            .saturating_add(1)
            .saturating_add(estimate_identity_encoded_bytes(identity))
            .saturating_add(16),
        entries,
        "flat",
    )
}

fn estimate_hnsw_vector_artifact_encoded_bytes(
    artifact: &HnswVectorArtifact,
) -> Result<usize, EngineError> {
    estimate_artifact_encoded_bytes(
        HNSW_ARTIFACT_MAGIC
            .len()
            .saturating_add(1)
            .saturating_add(estimate_identity_encoded_bytes(artifact.identity()))
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(8)
            .saturating_add(16),
        artifact.rows(),
        "HNSW",
    )
}

fn estimate_hnsw_vector_artifact_entries_encoded_bytes(
    identity: &VectorFlatArtifactIdentity,
    _config: HnswArtifactConfig,
    entries: &[(PersistenceReadRow, VectorEntry)],
) -> Result<usize, EngineError> {
    estimate_artifact_entries_encoded_bytes(
        HNSW_ARTIFACT_MAGIC
            .len()
            .saturating_add(1)
            .saturating_add(estimate_identity_encoded_bytes(identity))
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(8)
            .saturating_add(16),
        entries,
        "HNSW",
    )
}

fn estimate_artifact_encoded_bytes(
    fixed_bytes: usize,
    rows: &[FlatVectorArtifactRow],
    artifact_kind: &'static str,
) -> Result<usize, EngineError> {
    let mut total = fixed_bytes;
    for row in rows {
        total = total
            .saturating_add(encoded_len_prefix_bytes(row.key.as_str().len()))
            .saturating_add(8)
            .saturating_add(8)
            .saturating_add(8)
            .saturating_add(4)
            .saturating_add(row.embedding.dimension().saturating_mul(4))
            .saturating_add(1);
        if let Some(metadata) = &row.metadata {
            let metadata_bytes = serde_json::to_vec(metadata.as_inner()).map_err(|error| {
                EngineError::invalid_input(
                    "invalid_argument.engine.vector_artifact",
                    format!("{artifact_kind} vector artifact metadata cannot be encoded: {error}"),
                )
            })?;
            total = total.saturating_add(encoded_len_prefix_bytes(metadata_bytes.len()));
        }
    }
    Ok(total)
}

fn estimate_artifact_entries_encoded_bytes(
    fixed_bytes: usize,
    entries: &[(PersistenceReadRow, VectorEntry)],
    artifact_kind: &'static str,
) -> Result<usize, EngineError> {
    let mut total = fixed_bytes;
    for (_, entry) in entries {
        total = total
            .saturating_add(encoded_len_prefix_bytes(entry.key().as_str().len()))
            .saturating_add(8)
            .saturating_add(8)
            .saturating_add(8)
            .saturating_add(4)
            .saturating_add(entry.embedding().dimension().saturating_mul(4))
            .saturating_add(1);
        if let Some(metadata) = entry.metadata() {
            let metadata_bytes = serde_json::to_vec(metadata.as_inner()).map_err(|error| {
                EngineError::invalid_input(
                    "invalid_argument.engine.vector_artifact",
                    format!("{artifact_kind} vector artifact metadata cannot be encoded: {error}"),
                )
            })?;
            total = total.saturating_add(encoded_len_prefix_bytes(metadata_bytes.len()));
        }
    }
    Ok(total)
}

fn estimate_identity_encoded_bytes(identity: &VectorFlatArtifactIdentity) -> usize {
    encoded_len_prefix_bytes(identity.artifact_id.as_str().len())
        .saturating_add(BranchId::BYTE_LEN)
        .saturating_add(encoded_len_prefix_bytes(identity.space.as_str().len()))
        .saturating_add(encoded_len_prefix_bytes(identity.collection.as_str().len()))
        .saturating_add(8)
        .saturating_add(encoded_len_prefix_bytes(identity.source_id.as_str().len()))
        .saturating_add(BranchId::BYTE_LEN)
        .saturating_add(8)
        .saturating_add(4)
        .saturating_add(1)
}

fn encoded_len_prefix_bytes(len: usize) -> usize {
    4usize.saturating_add(len)
}

fn check_artifact_build_budget(
    artifact_kind: &'static str,
    estimated_bytes: usize,
    max_bytes: usize,
) -> Result<(), EngineError> {
    if estimated_bytes > max_bytes {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.vector_artifact_budget",
            format!(
                "{artifact_kind} vector artifact estimated size {estimated_bytes} bytes exceeds build budget {max_bytes} bytes"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn encode_flat_vector_artifact(
    artifact: &FlatVectorArtifact,
) -> Result<Vec<u8>, EngineError> {
    let mut body = Vec::new();
    body.extend_from_slice(FLAT_ARTIFACT_MAGIC);
    body.push(FLAT_ARTIFACT_FORMAT_VERSION);
    encode_identity(&mut body, artifact.identity())?;
    encode_rows(&mut body, artifact.rows(), "flat")?;
    let checksum = artifact_payload_checksum(&body);
    write_u64(&mut body, checksum);
    Ok(body)
}

pub(crate) fn encode_hnsw_vector_artifact(
    artifact: &HnswVectorArtifact,
) -> Result<Vec<u8>, EngineError> {
    let mut body = Vec::new();
    body.extend_from_slice(HNSW_ARTIFACT_MAGIC);
    body.push(HNSW_ARTIFACT_FORMAT_VERSION);
    encode_identity(&mut body, artifact.identity())?;
    write_u32(&mut body, u32::from(artifact.config().m));
    write_u32(&mut body, u32::from(artifact.config().ef_construction));
    write_u32(&mut body, u32::from(artifact.config().ef_search));
    write_u64(&mut body, artifact.config().seed());
    encode_rows(&mut body, artifact.rows(), "HNSW")?;
    let checksum = artifact_payload_checksum(&body);
    write_u64(&mut body, checksum);
    Ok(body)
}

fn encode_rows(
    body: &mut Vec<u8>,
    rows: &[FlatVectorArtifactRow],
    artifact_kind: &'static str,
) -> Result<(), EngineError> {
    write_u64(body, u64::try_from(rows.len()).unwrap_or(u64::MAX));
    for row in rows {
        write_text(body, row.key.as_str())?;
        write_u64(body, row.commit_version.as_u64());
        write_u64(body, row.timestamp.as_micros());
        write_u64(body, row.vector_revision);
        write_u32(
            body,
            u32::try_from(row.embedding.dimension()).map_err(|_| {
                EngineError::invalid_input(
                    "invalid_argument.engine.vector_artifact",
                    format!("{artifact_kind} vector artifact row dimension is too large"),
                )
            })?,
        );
        for value in row.embedding.as_slice() {
            write_u32(body, value.to_bits());
        }
        match &row.metadata {
            Some(metadata) => {
                body.push(1);
                let metadata_bytes = serde_json::to_vec(metadata.as_inner()).map_err(|error| {
                    EngineError::invalid_input(
                        "invalid_argument.engine.vector_artifact",
                        format!(
                            "{artifact_kind} vector artifact metadata cannot be encoded: {error}"
                        ),
                    )
                })?;
                write_bytes(body, &metadata_bytes)?;
            }
            None => body.push(0),
        }
    }
    Ok(())
}

pub(crate) fn decode_flat_vector_artifact(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<FlatVectorArtifact, EngineError> {
    if bytes.len() > max_bytes {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact exceeds the load budget",
        ));
    }
    let body = payload_body(bytes)?;
    let stored_checksum = read_trailing_checksum(bytes)?;
    if artifact_payload_checksum(body) != stored_checksum {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact checksum does not match",
        ));
    }
    let mut cursor = Cursor::new(body);
    cursor.expect_bytes(FLAT_ARTIFACT_MAGIC, "magic")?;
    let version = cursor.u8("format version")?;
    if version != FLAT_ARTIFACT_FORMAT_VERSION {
        return Err(EngineError::incompatible_layout(
            "failed_precondition.engine.vector_artifact",
            "flat vector artifact format version is not supported",
        ));
    }
    let identity = decode_identity(&mut cursor)?;
    let row_count = cursor.u64("row count")?;
    let row_count = usize::try_from(row_count).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact row count is too large",
        )
    })?;
    let minimum_row_bytes = minimum_encoded_row_bytes(identity.vector_dimension());
    if row_count > 0 && row_count > body.len() / minimum_row_bytes {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact row count exceeds payload size",
        ));
    }
    let mut rows = Vec::new();
    for _ in 0..row_count {
        rows.push(decode_row(&mut cursor, identity.vector_dimension())?);
    }
    cursor.finish()?;
    FlatVectorArtifact::new(identity, rows)
}

pub(crate) fn decode_hnsw_vector_artifact(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<HnswVectorArtifact, EngineError> {
    if bytes.len() > max_bytes {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "HNSW vector artifact exceeds the load budget",
        ));
    }
    let body = payload_body(bytes)?;
    let stored_checksum = read_trailing_checksum(bytes)?;
    if artifact_payload_checksum(body) != stored_checksum {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "HNSW vector artifact checksum does not match",
        ));
    }
    let mut cursor = Cursor::new(body);
    cursor.expect_bytes(HNSW_ARTIFACT_MAGIC, "magic")?;
    let version = cursor.u8("format version")?;
    if version != HNSW_ARTIFACT_FORMAT_VERSION {
        return Err(EngineError::incompatible_layout(
            "failed_precondition.engine.vector_artifact",
            "HNSW vector artifact format version is not supported",
        ));
    }
    let identity = decode_identity(&mut cursor)?;
    let config = HnswArtifactConfig {
        m: u16::try_from(cursor.u32("HNSW m")?).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "HNSW vector artifact m is too large",
            )
        })?,
        ef_construction: u16::try_from(cursor.u32("HNSW ef construction")?).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "HNSW vector artifact construction width is too large",
            )
        })?,
        ef_search: u16::try_from(cursor.u32("HNSW ef search")?).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "HNSW vector artifact search width is too large",
            )
        })?,
        seed: cursor.u64("HNSW seed")?,
    };
    config.validate().map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "HNSW vector artifact configuration is invalid",
        )
    })?;
    let row_count = cursor.u64("row count")?;
    let row_count = usize::try_from(row_count).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "HNSW vector artifact row count is too large",
        )
    })?;
    let minimum_row_bytes = minimum_encoded_row_bytes(identity.vector_dimension());
    if row_count > 0 && row_count > body.len() / minimum_row_bytes {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "HNSW vector artifact row count exceeds payload size",
        ));
    }
    let mut rows = Vec::new();
    for _ in 0..row_count {
        rows.push(decode_row(&mut cursor, identity.vector_dimension())?);
    }
    cursor.finish()?;
    HnswVectorArtifact::new(identity, config, rows)
}

fn encode_identity(
    out: &mut Vec<u8>,
    identity: &VectorFlatArtifactIdentity,
) -> Result<(), EngineError> {
    write_text(out, identity.artifact_id.as_str())?;
    out.extend_from_slice(identity.branch_id.as_bytes());
    write_text(out, identity.space.as_str())?;
    write_text(out, identity.collection.as_str())?;
    write_u64(out, identity.collection_generation);
    write_text(out, identity.source_id.as_str())?;
    out.extend_from_slice(identity.source_branch_id.as_bytes());
    write_u64(out, identity.source_generation);
    write_u32(
        out,
        u32::try_from(identity.vector_dimension).map_err(|_| {
            EngineError::invalid_input(
                "invalid_argument.engine.vector_artifact",
                "flat vector artifact dimension is too large",
            )
        })?,
    );
    out.push(metric_code(identity.metric));
    Ok(())
}

fn decode_identity(cursor: &mut Cursor<'_>) -> Result<VectorFlatArtifactIdentity, EngineError> {
    let artifact_id = VectorArtifactId::new(cursor.text("artifact id")?)?;
    let branch_id = cursor.branch_id("branch id")?;
    let space = ProductSpace::new(cursor.text("space")?)?;
    let collection = VectorCollectionName::new(cursor.text("collection")?)?;
    let collection_generation = cursor.u64("collection generation")?;
    let source_id = VectorSourceId::new(cursor.text("source id")?)?;
    let source_branch_id = cursor.branch_id("source branch id")?;
    let source_generation = cursor.u64("source generation")?;
    let vector_dimension = usize::try_from(cursor.u32("vector dimension")?).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact dimension is too large",
        )
    })?;
    let metric = decode_metric(cursor.u8("metric")?)?;
    VectorFlatArtifactIdentity::new(
        artifact_id,
        branch_id,
        space,
        collection,
        collection_generation,
        source_id,
        source_branch_id,
        source_generation,
        vector_dimension,
        metric,
    )
}

fn decode_row(
    cursor: &mut Cursor<'_>,
    expected_dimension: usize,
) -> Result<FlatVectorArtifactRow, EngineError> {
    let key = VectorKey::new(cursor.text("key")?)?;
    let commit_version = CommitVersion::new(cursor.u64("commit version")?);
    let timestamp = Timestamp::from_micros(cursor.u64("timestamp")?);
    let vector_revision = cursor.u64("vector revision")?;
    let dimension = usize::try_from(cursor.u32("embedding dimension")?).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact row dimension is too large",
        )
    })?;
    if dimension != expected_dimension {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact row dimension does not match identity",
        ));
    }
    let mut values = Vec::with_capacity(dimension);
    for _ in 0..dimension {
        values.push(f32::from_bits(cursor.u32("embedding value")?));
    }
    let embedding = VectorEmbedding::from_stored(values).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact embedding violates engine limits",
        )
    })?;
    let metadata = match cursor.u8("metadata flag")? {
        0 => None,
        1 => {
            let bytes = cursor.bytes("metadata")?;
            let value = serde_json::from_slice(bytes).map_err(|error| {
                EngineError::corruption(
                    "data_loss.engine.vector_artifact",
                    format!("flat vector artifact metadata cannot be decoded: {error}"),
                )
            })?;
            Some(VectorMetadata::new(value).map_err(|_| {
                EngineError::corruption(
                    "data_loss.engine.vector_artifact",
                    "flat vector artifact metadata violates engine limits",
                )
            })?)
        }
        _ => {
            return Err(EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "flat vector artifact metadata flag is invalid",
            ));
        }
    };
    Ok(FlatVectorArtifactRow {
        key,
        commit_version,
        timestamp,
        vector_revision,
        embedding,
        metadata,
    })
}

fn payload_body(bytes: &[u8]) -> Result<&[u8], EngineError> {
    bytes
        .get(
            ..bytes.len().checked_sub(8).ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.vector_artifact",
                    "flat vector artifact is truncated",
                )
            })?,
        )
        .ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "flat vector artifact is truncated",
            )
        })
}

fn read_trailing_checksum(bytes: &[u8]) -> Result<u64, EngineError> {
    let checksum = bytes
        .get(
            bytes.len().checked_sub(8).ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.vector_artifact",
                    "flat vector artifact is truncated",
                )
            })?..,
        )
        .ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "flat vector artifact is truncated",
            )
        })?;
    Ok(u64::from_le_bytes(checksum.try_into().map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact checksum is malformed",
        )
    })?))
}

fn minimum_encoded_row_bytes(dimension: usize) -> usize {
    4 + 8 + 8 + 8 + 4 + dimension.saturating_mul(4) + 1
}

fn artifact_payload_checksum(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_le_bytes(
        digest[..8]
            .try_into()
            .expect("sha-256 digest has at least 8 bytes"),
    )
}

fn validate_artifact_text(value: &str, label: &'static str) -> Result<(), EngineError> {
    if value.is_empty() || value.len() > MAX_ID_BYTES || value.bytes().any(|byte| byte == 0) {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.vector_artifact",
            format!("{label} is not a valid vector artifact identity component"),
        ));
    }
    Ok(())
}

fn metric_code(metric: VectorDistanceMetric) -> u8 {
    match metric {
        VectorDistanceMetric::Cosine => 1,
        VectorDistanceMetric::Euclidean => 2,
        VectorDistanceMetric::DotProduct => 3,
    }
}

fn decode_metric(code: u8) -> Result<VectorDistanceMetric, EngineError> {
    match code {
        1 => Ok(VectorDistanceMetric::Cosine),
        2 => Ok(VectorDistanceMetric::Euclidean),
        3 => Ok(VectorDistanceMetric::DotProduct),
        _ => Err(EngineError::corruption(
            "data_loss.engine.vector_artifact",
            "flat vector artifact metric is invalid",
        )),
    }
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_text(out: &mut Vec<u8>, text: &str) -> Result<(), EngineError> {
    write_bytes(out, text.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), EngineError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        EngineError::invalid_input(
            "invalid_argument.engine.vector_artifact",
            "flat vector artifact field is too large",
        )
    })?;
    write_u32(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn expect_bytes(&mut self, expected: &[u8], field: &'static str) -> Result<(), EngineError> {
        if self.take(expected.len(), field)? != expected {
            return Err(EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "flat vector artifact has invalid magic",
            ));
        }
        Ok(())
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, EngineError> {
        Ok(*self.take(1, field)?.first().expect("one byte"))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, EngineError> {
        let bytes = self.take(4, field)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("four bytes")))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, EngineError> {
        let bytes = self.take(8, field)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("eight bytes")))
    }

    fn branch_id(&mut self, field: &'static str) -> Result<BranchId, EngineError> {
        let bytes = self.take(BranchId::BYTE_LEN, field)?;
        Ok(BranchId::from_bytes(bytes.try_into().map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "flat vector artifact branch id is malformed",
            )
        })?))
    }

    fn text(&mut self, field: &'static str) -> Result<String, EngineError> {
        let bytes = self.bytes(field)?;
        std::str::from_utf8(bytes).map(str::to_owned).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                format!("flat vector artifact {field} is not valid UTF-8"),
            )
        })
    }

    fn bytes(&mut self, field: &'static str) -> Result<&'a [u8], EngineError> {
        let len = usize::try_from(self.u32(field)?).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                format!("flat vector artifact {field} length is too large"),
            )
        })?;
        self.take(len, field)
    }

    fn take(&mut self, len: usize, field: &'static str) -> Result<&'a [u8], EngineError> {
        let end = self.pos.checked_add(len).ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.vector_artifact",
                format!("flat vector artifact {field} length overflowed"),
            )
        })?;
        let Some(slice) = self.bytes.get(self.pos..end) else {
            return Err(EngineError::corruption(
                "data_loss.engine.vector_artifact",
                format!("flat vector artifact is truncated while reading {field}"),
            ));
        };
        self.pos = end;
        Ok(slice)
    }

    fn finish(&self) -> Result<(), EngineError> {
        if self.pos != self.bytes.len() {
            return Err(EngineError::corruption(
                "data_loss.engine.vector_artifact",
                "flat vector artifact has trailing bytes",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        decode_flat_vector_artifact, decode_hnsw_vector_artifact, encode_flat_vector_artifact,
        encode_hnsw_vector_artifact, estimate_flat_vector_artifact_encoded_bytes,
        estimate_hnsw_vector_artifact_encoded_bytes, FlatVectorArtifact, FlatVectorArtifactRow,
        HnswArtifactConfig, HnswVectorArtifact, VectorArtifactId, VectorArtifactLoadStatus,
        VectorArtifactStore, VectorFlatArtifactIdentity, VectorSourceId,
    };
    use crate::data::kv::ProductSpace;
    use crate::data::vector::{
        VectorCollectionName, VectorDistanceMetric, VectorEmbedding, VectorEntry, VectorKey,
        VectorMetadata,
    };
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::PersistenceReadRow;
    use strata_core::{BranchId, CommitVersion, Timestamp};

    trait ArtifactFixtures {
        fn identity(name: &str) -> VectorFlatArtifactIdentity;
        fn artifact(name: &str) -> FlatVectorArtifact;
        fn hnsw_artifact(name: &str) -> HnswVectorArtifact;
    }

    impl ArtifactFixtures for () {
        fn identity(name: &str) -> VectorFlatArtifactIdentity {
            VectorFlatArtifactIdentity::new(
                VectorArtifactId::new(format!("artifact-{name}")).expect("valid artifact id"),
                branch_id(1),
                ProductSpace::new("default").expect("valid space"),
                VectorCollectionName::new("docs").expect("valid collection"),
                7,
                VectorSourceId::new(format!("source-{name}")).expect("valid source id"),
                branch_id(1),
                9,
                2,
                VectorDistanceMetric::Cosine,
            )
            .expect("valid identity")
        }

        fn artifact(name: &str) -> FlatVectorArtifact {
            FlatVectorArtifact::new(
                Self::identity(name),
                vec![
                    row("b", [0.0, 1.0], 2, 20, 4, None),
                    row("a", [1.0, 0.0], 1, 10, 3, Some(json!({"kind": "doc"}))),
                ],
            )
            .expect("valid artifact")
        }

        fn hnsw_artifact(name: &str) -> HnswVectorArtifact {
            HnswVectorArtifact::new(
                Self::identity(name),
                HnswArtifactConfig::default_for_engine(),
                vec![
                    row("b", [0.0, 1.0], 2, 20, 4, None),
                    row("a", [1.0, 0.0], 1, 10, 3, Some(json!({"kind": "doc"}))),
                ],
            )
            .expect("valid HNSW artifact")
        }
    }

    #[test]
    fn flat_vector_artifact_round_trips_with_deterministic_bytes() {
        let artifact = <()>::artifact("roundtrip");

        let first = encode_flat_vector_artifact(&artifact).expect("artifact encodes");
        let second = encode_flat_vector_artifact(&artifact).expect("artifact encodes again");
        let decoded = decode_flat_vector_artifact(&first, usize::MAX).expect("artifact decodes");

        assert_eq!(first, second);
        assert_eq!(decoded, artifact);
        assert_eq!(
            decoded
                .rows()
                .iter()
                .map(|row| row.key().as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn flat_vector_artifact_rejects_checksum_version_dimension_and_budget() {
        let artifact = <()>::artifact("reject");
        let encoded = encode_flat_vector_artifact(&artifact).expect("artifact encodes");

        let mut corrupt = encoded.clone();
        let last = corrupt.last_mut().expect("checksum byte");
        *last ^= 0xff;
        assert!(decode_flat_vector_artifact(&corrupt, usize::MAX).is_err());

        let mut bad_version = encoded.clone();
        bad_version[super::FLAT_ARTIFACT_MAGIC.len()] = 2;
        assert!(decode_flat_vector_artifact(&bad_version, usize::MAX).is_err());

        let too_small = encoded.len() - 1;
        assert!(decode_flat_vector_artifact(&encoded, too_small).is_err());

        let dimension_mismatch = FlatVectorArtifact::new(
            <()>::identity("bad-dim"),
            vec![row("bad", [1.0, 0.0, 0.0], 1, 1, 1, None)],
        );
        assert!(dimension_mismatch.is_err());

        let oversized_identity = VectorFlatArtifactIdentity::new(
            VectorArtifactId::new("artifact-oversized").expect("valid artifact id"),
            branch_id(1),
            ProductSpace::new("default").expect("valid space"),
            VectorCollectionName::new("docs").expect("valid collection"),
            7,
            VectorSourceId::new("source-oversized").expect("valid source id"),
            branch_id(1),
            9,
            super::MAX_VECTOR_DIMENSION + 1,
            VectorDistanceMetric::Cosine,
        );
        assert!(oversized_identity.is_err());

        let impossible_row_count =
            encode_empty_artifact_body(&<()>::identity("too-many"), u64::MAX);
        assert!(decode_flat_vector_artifact(&impossible_row_count, usize::MAX).is_err());
    }

    // --- corruption-code assertions (TCP3.15c) ---------------------------
    //
    // The runtime swallows these decode errors into a "corrupt" artifact load
    // status (source rows stay authoritative — rule 26), so the corruption
    // codes never propagate to a public caller. These decoder unit tests are
    // the only place the codes are assertable.

    #[test]
    fn decode_reports_the_data_loss_code_for_a_corrupt_artifact_checksum() {
        let encoded = encode_flat_vector_artifact(&<()>::artifact("dl")).expect("artifact encodes");
        let mut corrupt = encoded.clone();
        *corrupt.last_mut().expect("checksum byte") ^= 0xFF;
        let error = decode_flat_vector_artifact(&corrupt, usize::MAX)
            .expect_err("a checksum mismatch is rejected");
        assert_eq!(error.code(), "data_loss.engine.vector_artifact");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
    }

    #[test]
    fn decode_reports_the_precondition_code_for_an_unsupported_artifact_version() {
        // The version byte lives inside the checksummed body, so a plain flip
        // trips the checksum (data_loss) first. Rebuild the body at version 2
        // and re-checksum it, so decode reaches the version gate.
        let encoded = encode_flat_vector_artifact(&<()>::artifact("fp")).expect("artifact encodes");
        let mut body = encoded[..encoded.len() - 8].to_vec();
        body[super::FLAT_ARTIFACT_MAGIC.len()] = 2;
        let checksum = super::artifact_payload_checksum(&body);
        super::write_u64(&mut body, checksum);
        let error = decode_flat_vector_artifact(&body, usize::MAX)
            .expect_err("an unsupported version is rejected");
        assert_eq!(error.code(), "failed_precondition.engine.vector_artifact");
    }

    #[test]
    fn hnsw_vector_artifact_round_trips_with_deterministic_bytes() {
        let artifact = <()>::hnsw_artifact("hnsw-roundtrip");

        let first = encode_hnsw_vector_artifact(&artifact).expect("artifact encodes");
        let second = encode_hnsw_vector_artifact(&artifact).expect("artifact encodes again");
        let decoded = decode_hnsw_vector_artifact(&first, usize::MAX).expect("artifact decodes");

        assert_eq!(first, second);
        assert_eq!(decoded, artifact);
        assert_eq!(decoded.config(), HnswArtifactConfig::default_for_engine());
        assert_eq!(
            decoded
                .rows()
                .iter()
                .map(|row| row.key().as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn hnsw_vector_artifact_rejects_checksum_version_config_and_budget() {
        let artifact = <()>::hnsw_artifact("hnsw-reject");
        let encoded = encode_hnsw_vector_artifact(&artifact).expect("artifact encodes");

        let mut corrupt = encoded.clone();
        let last = corrupt.last_mut().expect("checksum byte");
        *last ^= 0xff;
        assert!(decode_hnsw_vector_artifact(&corrupt, usize::MAX).is_err());

        let mut bad_version = encoded.clone();
        bad_version[super::HNSW_ARTIFACT_MAGIC.len()] = 2;
        assert!(decode_hnsw_vector_artifact(&bad_version, usize::MAX).is_err());

        let too_small = encoded.len() - 1;
        assert!(decode_hnsw_vector_artifact(&encoded, too_small).is_err());

        let bad_config = encode_empty_hnsw_artifact_body(&<()>::identity("bad-config"), 1, 0, 0);
        assert!(decode_hnsw_vector_artifact(&bad_config, usize::MAX).is_err());
    }

    #[test]
    fn flat_artifact_store_reports_missing_corrupt_stale_and_over_budget_loads() {
        let expected = <()>::identity("store");
        let artifact = <()>::artifact("store");
        let mut store = VectorArtifactStore::default();

        assert_eq!(
            store.load_flat(&expected, usize::MAX).report().status(),
            VectorArtifactLoadStatus::Missing
        );

        let handle = store.store_flat(&artifact).expect("artifact stores");
        assert_eq!(handle.vector_count(), 2);
        assert_eq!(
            store.load_flat(&expected, usize::MAX).report().status(),
            VectorArtifactLoadStatus::Loaded
        );
        assert_eq!(
            store
                .load_flat(
                    &expected,
                    usize::try_from(handle.byte_len() - 1).expect("budget fits")
                )
                .report()
                .status(),
            VectorArtifactLoadStatus::OverBudget
        );

        let stale_expected = <()>::identity("stale");
        assert_eq!(
            store
                .load_flat(&stale_expected, usize::MAX)
                .report()
                .status(),
            VectorArtifactLoadStatus::Missing
        );

        store
            .put_raw_flat_for_test(handle.artifact_id().clone(), vec![1, 2, 3])
            .expect("raw artifact writes");
        assert_eq!(
            store.load_flat(&expected, usize::MAX).report().status(),
            VectorArtifactLoadStatus::Corrupt
        );

        let stale_artifact = <()>::artifact("other");
        store
            .put_raw_flat_for_test(
                expected.artifact_id().clone(),
                encode_flat_vector_artifact(&stale_artifact).expect("artifact encodes"),
            )
            .expect("raw artifact writes");
        assert_eq!(
            store.load_flat(&expected, usize::MAX).report().status(),
            VectorArtifactLoadStatus::Stale
        );
    }

    #[test]
    fn flat_artifact_store_rejects_builds_over_budget_before_caching() {
        let artifact = <()>::artifact("flat-build-budget");
        let estimated_bytes =
            estimate_flat_vector_artifact_encoded_bytes(&artifact).expect("estimate succeeds");
        let mut store = VectorArtifactStore::default();

        let error = store
            .store_flat_with_budget_for_test(&artifact, estimated_bytes - 1)
            .expect_err("artifact exceeds build budget");

        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(
            error.code(),
            "invalid_argument.engine.vector_artifact_budget"
        );
        assert_eq!(store.memory_payload_bytes_for_test(), 0);
        assert_eq!(
            store
                .load_flat(artifact.identity(), usize::MAX)
                .report()
                .status(),
            VectorArtifactLoadStatus::Missing
        );
    }

    #[test]
    fn hnsw_artifact_store_reports_missing_corrupt_stale_and_over_budget_loads() {
        let expected = <()>::identity("hnsw-store");
        let artifact = <()>::hnsw_artifact("hnsw-store");
        let mut store = VectorArtifactStore::default();

        assert_eq!(
            store.load_hnsw(&expected, usize::MAX).report().status(),
            VectorArtifactLoadStatus::Missing
        );

        let handle = store.store_hnsw(&artifact).expect("artifact stores");
        assert_eq!(handle.vector_count(), 2);
        assert_eq!(
            store.load_hnsw(&expected, usize::MAX).report().status(),
            VectorArtifactLoadStatus::Loaded
        );
        assert_eq!(
            store
                .load_hnsw(
                    &expected,
                    usize::try_from(handle.byte_len() - 1).expect("budget fits")
                )
                .report()
                .status(),
            VectorArtifactLoadStatus::OverBudget
        );

        store
            .put_raw_hnsw_for_test(handle.artifact_id().clone(), vec![1, 2, 3])
            .expect("raw artifact writes");
        assert_eq!(
            store.load_hnsw(&expected, usize::MAX).report().status(),
            VectorArtifactLoadStatus::Corrupt
        );

        let stale_artifact = <()>::hnsw_artifact("hnsw-other");
        store
            .put_raw_hnsw_for_test(
                expected.artifact_id().clone(),
                encode_hnsw_vector_artifact(&stale_artifact).expect("artifact encodes"),
            )
            .expect("raw artifact writes");
        assert_eq!(
            store.load_hnsw(&expected, usize::MAX).report().status(),
            VectorArtifactLoadStatus::Stale
        );
    }

    #[test]
    fn hnsw_artifact_store_rejects_builds_over_budget_before_caching() {
        let artifact = <()>::hnsw_artifact("hnsw-build-budget");
        let estimated_bytes =
            estimate_hnsw_vector_artifact_encoded_bytes(&artifact).expect("estimate succeeds");
        let mut store = VectorArtifactStore::default();

        let error = store
            .store_hnsw_with_budget_for_test(&artifact, estimated_bytes - 1)
            .expect_err("artifact exceeds build budget");

        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(
            error.code(),
            "invalid_argument.engine.vector_artifact_budget"
        );
        assert_eq!(store.memory_payload_bytes_for_test(), 0);
        assert_eq!(
            store
                .load_hnsw(artifact.identity(), usize::MAX)
                .report()
                .status(),
            VectorArtifactLoadStatus::Missing
        );
    }

    #[test]
    fn memory_artifact_store_evicts_old_payloads_to_honor_budget() {
        let first = <()>::artifact("evict-one");
        let second = <()>::artifact("evict-two");
        let mut store = VectorArtifactStore::memory();
        let first_handle = store.store_flat(&first).expect("first stores");
        let first_size = usize::try_from(first_handle.byte_len()).expect("size fits");
        store.set_memory_budget_for_test(first_size);

        let second_handle = store.store_flat(&second).expect("second stores");

        assert!(store.memory_payload_bytes_for_test() <= first_size);
        assert_eq!(
            store
                .load_flat(first.identity(), usize::MAX)
                .report()
                .status(),
            VectorArtifactLoadStatus::Missing
        );
        assert_eq!(
            store
                .load_flat(second.identity(), usize::MAX)
                .report()
                .status(),
            VectorArtifactLoadStatus::Loaded
        );
        assert_eq!(
            store.memory_payload_bytes_for_test(),
            usize::try_from(second_handle.byte_len()).expect("size fits")
        );
    }

    #[test]
    fn durable_artifact_store_does_not_retain_payload_bytes_in_memory() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let flat = <()>::artifact("durable-flat");
        let hnsw = <()>::hnsw_artifact("durable-hnsw");
        let mut store = VectorArtifactStore::durable_local(tempdir.path());

        store.store_flat(&flat).expect("flat stores");
        store.store_hnsw(&hnsw).expect("HNSW stores");

        assert_eq!(store.memory_payload_bytes_for_test(), 0);
        assert_eq!(
            store
                .load_flat(flat.identity(), usize::MAX)
                .report()
                .status(),
            VectorArtifactLoadStatus::Loaded
        );
        assert_eq!(
            store
                .load_hnsw(hnsw.identity(), usize::MAX)
                .report()
                .status(),
            VectorArtifactLoadStatus::Loaded
        );
        assert_eq!(store.memory_payload_bytes_for_test(), 0);
    }

    #[test]
    fn flat_artifact_rows_can_be_built_from_committed_visible_rows() {
        let entry = VectorEntry::new(
            VectorKey::new("a").expect("valid key"),
            VectorEmbedding::new([1.0, 0.0]).expect("valid embedding"),
            Some(VectorMetadata::new(json!({"kind": "doc"})).expect("valid metadata")),
            3,
        );
        let row = PersistenceReadRow::for_test(vec![1], None, false);

        let artifact = FlatVectorArtifact::from_visible_entries(
            <()>::identity("visible"),
            &[(row.clone(), entry.clone())],
        )
        .expect("artifact builds");

        assert_eq!(artifact.rows()[0].key(), entry.key());
        let decoded = decode_flat_vector_artifact(
            &encode_flat_vector_artifact(&artifact).expect("artifact encodes"),
            usize::MAX,
        )
        .expect("artifact decodes");
        assert_eq!(decoded, artifact);
    }

    fn row<const N: usize>(
        key: &str,
        embedding: [f32; N],
        version: u64,
        timestamp_micros: u64,
        revision: u64,
        metadata: Option<serde_json::Value>,
    ) -> FlatVectorArtifactRow {
        FlatVectorArtifactRow {
            key: VectorKey::new(key).expect("valid key"),
            commit_version: CommitVersion::new(version),
            timestamp: Timestamp::from_micros(timestamp_micros),
            vector_revision: revision,
            embedding: VectorEmbedding::new(embedding).expect("valid embedding"),
            metadata: metadata
                .map(VectorMetadata::new)
                .transpose()
                .expect("valid metadata"),
        }
    }

    fn branch_id(byte: u8) -> BranchId {
        BranchId::from_bytes([byte; BranchId::BYTE_LEN])
    }

    fn encode_empty_artifact_body(
        identity: &VectorFlatArtifactIdentity,
        row_count: u64,
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(super::FLAT_ARTIFACT_MAGIC);
        body.push(super::FLAT_ARTIFACT_FORMAT_VERSION);
        super::encode_identity(&mut body, identity).expect("identity encodes");
        super::write_u64(&mut body, row_count);
        let checksum = super::artifact_payload_checksum(&body);
        super::write_u64(&mut body, checksum);
        body
    }

    fn encode_empty_hnsw_artifact_body(
        identity: &VectorFlatArtifactIdentity,
        m: u32,
        ef_construction: u32,
        ef_search: u32,
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(super::HNSW_ARTIFACT_MAGIC);
        body.push(super::HNSW_ARTIFACT_FORMAT_VERSION);
        super::encode_identity(&mut body, identity).expect("identity encodes");
        super::write_u32(&mut body, m);
        super::write_u32(&mut body, ef_construction);
        super::write_u32(&mut body, ef_search);
        super::write_u64(&mut body, super::DEFAULT_HNSW_SEED);
        super::write_u64(&mut body, 0);
        let checksum = super::artifact_payload_checksum(&body);
        super::write_u64(&mut body, checksum);
        body
    }
}
