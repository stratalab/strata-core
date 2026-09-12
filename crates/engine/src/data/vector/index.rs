//! Vector candidate planning and index diagnostics.

use std::collections::{btree_map::Entry, BTreeMap};
use std::sync::Arc;

use fast_hnsw::{
    distance::{
        Cosine as HnswCosine, DotProduct as HnswDotProduct,
        SquaredEuclidean as HnswSquaredEuclidean,
    },
    Builder as HnswBuilder, Hnsw,
};
use strata_core::{CommitVersion, Timestamp};

use crate::diagnostics::EngineError;
use crate::persistence::{PersistenceReadRow, ReadSelector};

use super::{
    default_flat_artifact_load_budget_bytes, default_hnsw_artifact_load_budget_bytes, vector_score,
    FlatVectorArtifact, HnswVectorArtifact, VectorCollectionName, VectorDistanceMetric,
    VectorEmbedding, VectorEntry, VectorFilter, VectorKey, VectorSearchMatch, VectorSearchResult,
};

const DEFAULT_COLLECTION_EXACT_THRESHOLD: usize = 64;
const DEFAULT_SOURCE_FLAT_THRESHOLD: usize = 64;
const DEFAULT_SOURCE_HNSW_THRESHOLD: usize = usize::MAX;
const DEFAULT_ACTIVE_DELTA_SEAL_THRESHOLD: usize = 16;
const DEFAULT_OVERFETCH_FACTOR: usize = 4;
const DEFAULT_HNSW_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VectorIndexMode {
    Auto,
    #[cfg(any(test, feature = "testkit"))]
    ExactOnly,
    #[cfg(any(test, feature = "testkit"))]
    FlatOnly,
    #[cfg(any(test, feature = "testkit"))]
    HnswOnly,
}

impl VectorIndexMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            #[cfg(any(test, feature = "testkit"))]
            Self::ExactOnly => "exact_only",
            #[cfg(any(test, feature = "testkit"))]
            Self::FlatOnly => "flat_only",
            #[cfg(any(test, feature = "testkit"))]
            Self::HnswOnly => "hnsw_only",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VectorSourceKind {
    Exact,
    ActiveDelta,
    Flat,
    Hnsw,
}

impl VectorSourceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::ActiveDelta => "active_delta",
            Self::Flat => "flat",
            Self::Hnsw => "hnsw",
        }
    }
}

/// Resolved vector index policy used by testkit diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(any(test, feature = "testkit")), allow(unreachable_pub))]
pub struct VectorIndexPolicy {
    mode: VectorIndexMode,
    collection_exact_threshold: usize,
    source_flat_threshold: usize,
    source_hnsw_threshold: usize,
    active_delta_seal_threshold: usize,
    overfetch_factor: usize,
    filtered_underfill_fallback: bool,
    hnsw_memory_budget_bytes: usize,
    flat_artifact_load_budget_bytes: usize,
    hnsw_artifact_load_budget_bytes: usize,
}

impl Default for VectorIndexPolicy {
    fn default() -> Self {
        Self {
            mode: VectorIndexMode::Auto,
            collection_exact_threshold: DEFAULT_COLLECTION_EXACT_THRESHOLD,
            source_flat_threshold: DEFAULT_SOURCE_FLAT_THRESHOLD,
            source_hnsw_threshold: DEFAULT_SOURCE_HNSW_THRESHOLD,
            active_delta_seal_threshold: DEFAULT_ACTIVE_DELTA_SEAL_THRESHOLD,
            overfetch_factor: DEFAULT_OVERFETCH_FACTOR,
            filtered_underfill_fallback: true,
            hnsw_memory_budget_bytes: DEFAULT_HNSW_MEMORY_BUDGET_BYTES,
            flat_artifact_load_budget_bytes: default_flat_artifact_load_budget_bytes(),
            hnsw_artifact_load_budget_bytes: default_hnsw_artifact_load_budget_bytes(),
        }
    }
}

impl VectorIndexPolicy {
    #[cfg(any(test, feature = "testkit"))]
    #[must_use]
    /// Returns a policy that always uses the exact source.
    pub const fn exact_only_for_test() -> Self {
        Self {
            mode: VectorIndexMode::ExactOnly,
            collection_exact_threshold: DEFAULT_COLLECTION_EXACT_THRESHOLD,
            source_flat_threshold: DEFAULT_SOURCE_FLAT_THRESHOLD,
            source_hnsw_threshold: DEFAULT_SOURCE_HNSW_THRESHOLD,
            active_delta_seal_threshold: DEFAULT_ACTIVE_DELTA_SEAL_THRESHOLD,
            overfetch_factor: DEFAULT_OVERFETCH_FACTOR,
            filtered_underfill_fallback: true,
            hnsw_memory_budget_bytes: DEFAULT_HNSW_MEMORY_BUDGET_BYTES,
            flat_artifact_load_budget_bytes: default_flat_artifact_load_budget_bytes(),
            hnsw_artifact_load_budget_bytes: default_hnsw_artifact_load_budget_bytes(),
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    #[must_use]
    /// Returns a policy that always uses the flat source.
    pub const fn flat_only_for_test() -> Self {
        Self {
            mode: VectorIndexMode::FlatOnly,
            collection_exact_threshold: 0,
            source_flat_threshold: 0,
            source_hnsw_threshold: DEFAULT_SOURCE_HNSW_THRESHOLD,
            active_delta_seal_threshold: DEFAULT_ACTIVE_DELTA_SEAL_THRESHOLD,
            overfetch_factor: DEFAULT_OVERFETCH_FACTOR,
            filtered_underfill_fallback: true,
            hnsw_memory_budget_bytes: DEFAULT_HNSW_MEMORY_BUDGET_BYTES,
            flat_artifact_load_budget_bytes: default_flat_artifact_load_budget_bytes(),
            hnsw_artifact_load_budget_bytes: default_hnsw_artifact_load_budget_bytes(),
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    #[must_use]
    /// Returns a policy that always plans HNSW before falling back.
    pub const fn hnsw_only_for_test() -> Self {
        Self {
            mode: VectorIndexMode::HnswOnly,
            collection_exact_threshold: 0,
            source_flat_threshold: 0,
            source_hnsw_threshold: 0,
            active_delta_seal_threshold: DEFAULT_ACTIVE_DELTA_SEAL_THRESHOLD,
            overfetch_factor: DEFAULT_OVERFETCH_FACTOR,
            filtered_underfill_fallback: true,
            hnsw_memory_budget_bytes: DEFAULT_HNSW_MEMORY_BUDGET_BYTES,
            flat_artifact_load_budget_bytes: default_flat_artifact_load_budget_bytes(),
            hnsw_artifact_load_budget_bytes: default_hnsw_artifact_load_budget_bytes(),
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    #[must_use]
    /// Returns this policy with a different HNSW memory budget.
    pub const fn with_hnsw_memory_budget_for_test(mut self, bytes: usize) -> Self {
        self.hnsw_memory_budget_bytes = bytes;
        self
    }

    #[cfg(any(test, feature = "testkit"))]
    #[must_use]
    /// Returns this policy with a different flat artifact load budget.
    pub const fn with_flat_artifact_load_budget_for_test(mut self, bytes: usize) -> Self {
        self.flat_artifact_load_budget_bytes = bytes;
        self
    }

    #[cfg(any(test, feature = "testkit"))]
    #[must_use]
    /// Returns this policy with a different HNSW artifact load budget.
    pub const fn with_hnsw_artifact_load_budget_for_test(mut self, bytes: usize) -> Self {
        self.hnsw_artifact_load_budget_bytes = bytes;
        self
    }

    fn resolve_kind(self, vector_count: usize) -> ResolvedVectorSourceKind {
        match self.mode {
            #[cfg(any(test, feature = "testkit"))]
            VectorIndexMode::ExactOnly => ResolvedVectorSourceKind::exact("policy_exact_only"),
            #[cfg(any(test, feature = "testkit"))]
            VectorIndexMode::FlatOnly => ResolvedVectorSourceKind::flat(),
            #[cfg(any(test, feature = "testkit"))]
            VectorIndexMode::HnswOnly => ResolvedVectorSourceKind::hnsw(),
            VectorIndexMode::Auto if vector_count < self.collection_exact_threshold => {
                ResolvedVectorSourceKind::exact("collection_below_exact_threshold")
            }
            VectorIndexMode::Auto if vector_count >= self.source_hnsw_threshold => {
                ResolvedVectorSourceKind::hnsw()
            }
            VectorIndexMode::Auto if vector_count >= self.source_flat_threshold => {
                ResolvedVectorSourceKind::flat()
            }
            VectorIndexMode::Auto => ResolvedVectorSourceKind::exact("source_below_flat_threshold"),
        }
    }

    pub(crate) fn should_load_flat_sources(self, vector_count: usize) -> bool {
        matches!(
            self.resolve_kind(vector_count).kind,
            VectorSourceKind::Flat | VectorSourceKind::Hnsw
        )
    }

    /// Whether this policy can ever resolve to an index source (i.e. it is not exact-only). Used
    /// to gate the bounded active-delta read: an exact-only policy must read the full snapshot.
    pub(crate) fn may_use_index(self) -> bool {
        !matches!(self.resolve_kind(usize::MAX).kind, VectorSourceKind::Exact)
    }

    pub(crate) fn should_load_hnsw_sources(self, vector_count: usize) -> bool {
        self.resolve_kind(vector_count).kind == VectorSourceKind::Hnsw
    }

    #[allow(dead_code)]
    pub(crate) const fn should_seal_active_delta(self, active_delta_count: usize) -> bool {
        self.active_delta_seal_threshold > 0
            && active_delta_count >= self.active_delta_seal_threshold
    }

    #[allow(dead_code)]
    pub(crate) const fn should_seal_hnsw_source(self, source_count: usize) -> bool {
        match self.mode {
            VectorIndexMode::Auto => source_count >= self.source_hnsw_threshold,
            #[cfg(any(test, feature = "testkit"))]
            VectorIndexMode::HnswOnly => source_count > 0,
            #[cfg(any(test, feature = "testkit"))]
            VectorIndexMode::ExactOnly | VectorIndexMode::FlatOnly => false,
        }
    }

    const fn policy_mode(self) -> &'static str {
        self.mode.as_str()
    }

    fn source_limit(self, k: usize) -> usize {
        let overfetch = if self.overfetch_factor == 0 {
            1
        } else {
            self.overfetch_factor
        };
        k.saturating_mul(overfetch).max(k)
    }

    const fn hnsw_memory_budget_bytes(self) -> usize {
        self.hnsw_memory_budget_bytes
    }

    pub(crate) const fn flat_artifact_load_budget_bytes(self) -> usize {
        self.flat_artifact_load_budget_bytes
    }

    pub(crate) const fn hnsw_artifact_load_budget_bytes(self) -> usize {
        self.hnsw_artifact_load_budget_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedVectorSourceKind {
    kind: VectorSourceKind,
    fallback_reason: Option<&'static str>,
}

impl ResolvedVectorSourceKind {
    const fn exact(reason: &'static str) -> Self {
        Self {
            kind: VectorSourceKind::Exact,
            fallback_reason: Some(reason),
        }
    }

    const fn flat() -> Self {
        Self {
            kind: VectorSourceKind::Flat,
            fallback_reason: None,
        }
    }

    const fn hnsw() -> Self {
        Self {
            kind: VectorSourceKind::Hnsw,
            fallback_reason: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VectorIndexManifestLookup {
    Missing,
    Loaded {
        generation: u64,
        ref_count: usize,
        inherited_ref_count: usize,
        owned_ref_count: usize,
        active_delta_count: usize,
    },
    Corrupt,
    Stale,
}

pub(crate) struct VectorFlatArtifactSourceInput {
    artifact: Arc<FlatVectorArtifact>,
    derived_bytes: u64,
    fork_version_cap: Option<CommitVersion>,
}

impl VectorFlatArtifactSourceInput {
    pub(crate) fn new(
        artifact: Arc<FlatVectorArtifact>,
        derived_bytes: u64,
        fork_version_cap: Option<CommitVersion>,
    ) -> Self {
        Self {
            artifact,
            derived_bytes,
            fork_version_cap,
        }
    }

    fn artifact_id(&self) -> &str {
        self.artifact.identity().artifact_id().as_str()
    }
}

/// A built HNSW graph paired with the artifact rows it was built from. The expensive graph
/// construction happens once here, when the artifact is first loaded, rather than on every
/// query. The per-database `VectorArtifactStore` caches this behind an `Arc` and hands a clone
/// to each query, so repeated queries reuse the same graph. See the implementation plan's
/// Implementation Order step 11.3 ("rebuild on open when serialization is not viable") and the
/// Resource Budget rules.
pub(crate) struct CachedHnswArtifact {
    artifact: HnswVectorArtifact,
    index: HnswRuntimeIndex,
    estimated_memory_bytes: usize,
}

impl CachedHnswArtifact {
    pub(crate) fn build(artifact: HnswVectorArtifact) -> Self {
        let estimated_memory_bytes = estimate_hnsw_memory_bytes(&artifact);
        let index = HnswRuntimeIndex::build(&artifact);
        Self {
            artifact,
            index,
            estimated_memory_bytes,
        }
    }

    pub(crate) fn artifact(&self) -> &HnswVectorArtifact {
        &self.artifact
    }

    fn index(&self) -> &HnswRuntimeIndex {
        &self.index
    }

    pub(crate) const fn estimated_memory_bytes(&self) -> usize {
        self.estimated_memory_bytes
    }
}

pub(crate) struct VectorHnswArtifactSourceInput {
    cached: Arc<CachedHnswArtifact>,
    derived_bytes: u64,
    fork_version_cap: Option<CommitVersion>,
    // True only when the fork cap actually excludes some artifact rows (the artifact was sealed
    // past the fork point). When false — the common case where every artifact row is visible to
    // this branch — graph search is exactly correct and the brute-force visibility scan is skipped.
    fork_cap_excludes_rows: bool,
}

impl VectorHnswArtifactSourceInput {
    pub(crate) fn from_cached(
        cached: Arc<CachedHnswArtifact>,
        derived_bytes: u64,
        fork_version_cap: Option<CommitVersion>,
        fork_cap_excludes_rows: bool,
    ) -> Self {
        Self {
            cached,
            derived_bytes,
            fork_version_cap,
            fork_cap_excludes_rows,
        }
    }

    fn artifact_id(&self) -> &str {
        self.cached.artifact().identity().artifact_id().as_str()
    }

    fn estimated_memory_bytes(&self) -> usize {
        self.cached.estimated_memory_bytes()
    }
}

enum HnswRuntimeIndex {
    Cosine(Hnsw<HnswCosine>),
    Euclidean(Hnsw<HnswSquaredEuclidean>),
    DotProduct(Hnsw<HnswDotProduct>),
}

impl HnswRuntimeIndex {
    fn build(artifact: &HnswVectorArtifact) -> Self {
        match artifact.identity().metric() {
            VectorDistanceMetric::Cosine => Self::Cosine(build_hnsw(artifact, HnswCosine)),
            VectorDistanceMetric::Euclidean => {
                Self::Euclidean(build_hnsw(artifact, HnswSquaredEuclidean))
            }
            VectorDistanceMetric::DotProduct => {
                Self::DotProduct(build_hnsw(artifact, HnswDotProduct))
            }
        }
    }

    fn search(&self, query: &VectorEmbedding, limit: usize, ef_search: usize) -> Vec<usize> {
        if limit == 0 {
            return Vec::new();
        }
        match self {
            Self::Cosine(index) => index
                .search(query.as_slice(), limit, ef_search)
                .into_iter()
                .map(|result| result.id)
                .collect(),
            Self::Euclidean(index) => index
                .search(query.as_slice(), limit, ef_search)
                .into_iter()
                .map(|result| result.id)
                .collect(),
            Self::DotProduct(index) => index
                .search(query.as_slice(), limit, ef_search)
                .into_iter()
                .map(|result| result.id)
                .collect(),
        }
    }
}

fn build_hnsw<D>(artifact: &HnswVectorArtifact, metric: D) -> Hnsw<D>
where
    D: fast_hnsw::distance::Distance,
{
    let config = artifact.config();
    let mut index = HnswBuilder::new()
        .m(config.m())
        .ef_construction(config.ef_construction())
        .capacity(artifact.rows().len())
        .seed(config.seed())
        .build(metric);
    for row in artifact.rows() {
        index.insert(row.embedding().as_slice().to_vec());
    }
    index
}

fn estimate_hnsw_memory_bytes(artifact: &HnswVectorArtifact) -> usize {
    let per_vector_bytes = artifact
        .identity()
        .vector_dimension()
        .saturating_mul(std::mem::size_of::<f32>());
    let per_link_bytes = artifact
        .config()
        .m()
        .saturating_mul(2)
        .saturating_mul(std::mem::size_of::<(u32, f32)>());
    artifact.rows().len().saturating_mul(
        per_vector_bytes
            .saturating_add(per_link_bytes)
            .saturating_add(128),
    )
}

#[derive(Clone)]
pub(crate) struct VectorTombstone {
    key: VectorKey,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl VectorTombstone {
    pub(crate) fn new(key: VectorKey, version: CommitVersion, timestamp: Timestamp) -> Self {
        Self {
            key,
            version,
            timestamp,
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn version(&self) -> CommitVersion {
        self.version
    }
}

impl VectorIndexManifestLookup {
    pub(crate) const fn status(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Loaded { .. } => "loaded",
            Self::Corrupt => "corrupt",
            Self::Stale => "stale",
        }
    }
}

/// Per-artifact vector index load and search facts.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(any(test, feature = "testkit")), allow(unreachable_pub))]
pub struct VectorArtifactSourceDiagnostic {
    artifact_id: String,
    status: &'static str,
    searched: bool,
}

impl VectorArtifactSourceDiagnostic {
    pub(crate) fn new(
        artifact_id: impl Into<String>,
        status: &'static str,
        searched: bool,
    ) -> Self {
        Self {
            artifact_id: artifact_id.into(),
            status,
            searched,
        }
    }

    fn mark_searched(&mut self) {
        self.searched = true;
    }

    #[must_use]
    /// Returns the artifact ref identity recorded by the manifest.
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    #[must_use]
    /// Returns the load status for this artifact ref.
    pub const fn status(&self) -> &str {
        self.status
    }

    #[must_use]
    /// Returns whether this artifact ref was searched by the planner.
    pub const fn searched(&self) -> bool {
        self.searched
    }
}

/// Vector index planning facts.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(any(test, feature = "testkit")), allow(unreachable_pub))]
pub struct VectorIndexDiagnostics {
    collection: String,
    manifest_status: &'static str,
    manifest_generation: Option<u64>,
    manifest_ref_count: usize,
    manifest_inherited_ref_count: usize,
    manifest_owned_ref_count: usize,
    active_delta_count: usize,
    policy_mode: &'static str,
    collection_exact_threshold: usize,
    source_flat_threshold: usize,
    source_hnsw_threshold: usize,
    overfetch_factor: usize,
    filtered_underfill_fallback: bool,
    active_delta_seal_threshold: usize,
    hnsw_memory_budget_bytes: usize,
    source_candidate_limit: usize,
    resolved_index_kind_summary: &'static str,
    exact_fallback_count: u64,
    hnsw_graph_builds: usize,
    source_rows_scanned: usize,
    indexed_source_count: usize,
    exact_source_count: usize,
    flat_source_count: usize,
    hnsw_source_count: usize,
    active_delta_source_count: usize,
    indexed_vector_count: usize,
    derived_bytes: u64,
    last_query_used_index: bool,
    last_query_fallback_reason: Option<&'static str>,
    artifact_sources: Vec<VectorArtifactSourceDiagnostic>,
}

impl VectorIndexDiagnostics {
    fn new(collection: &VectorCollectionName, policy: VectorIndexPolicy) -> Self {
        Self {
            collection: collection.as_str().to_owned(),
            manifest_status: "not_checked",
            manifest_generation: None,
            manifest_ref_count: 0,
            manifest_inherited_ref_count: 0,
            manifest_owned_ref_count: 0,
            active_delta_count: 0,
            policy_mode: policy.policy_mode(),
            collection_exact_threshold: policy.collection_exact_threshold,
            source_flat_threshold: policy.source_flat_threshold,
            source_hnsw_threshold: policy.source_hnsw_threshold,
            overfetch_factor: policy.overfetch_factor,
            filtered_underfill_fallback: policy.filtered_underfill_fallback,
            active_delta_seal_threshold: policy.active_delta_seal_threshold,
            hnsw_memory_budget_bytes: policy.hnsw_memory_budget_bytes,
            source_candidate_limit: 0,
            resolved_index_kind_summary: "none",
            exact_fallback_count: 0,
            hnsw_graph_builds: 0,
            source_rows_scanned: 0,
            indexed_source_count: 0,
            exact_source_count: 0,
            flat_source_count: 0,
            hnsw_source_count: 0,
            active_delta_source_count: 0,
            indexed_vector_count: 0,
            derived_bytes: 0,
            last_query_used_index: false,
            last_query_fallback_reason: None,
            artifact_sources: Vec::new(),
        }
    }

    fn record_source(&mut self, source: &dyn VectorCandidateSource) {
        self.resolved_index_kind_summary = source.kind().as_str();
        match source.kind() {
            VectorSourceKind::Exact => {
                self.exact_source_count = self.exact_source_count.saturating_add(1);
                self.last_query_used_index = false;
            }
            VectorSourceKind::ActiveDelta => {
                self.active_delta_source_count = self.active_delta_source_count.saturating_add(1);
            }
            VectorSourceKind::Flat => {
                self.flat_source_count = self.flat_source_count.saturating_add(1);
                self.indexed_source_count = self.indexed_source_count.saturating_add(1);
                self.indexed_vector_count = self.indexed_vector_count.saturating_add(source.len());
                self.derived_bytes = self.derived_bytes.saturating_add(source.estimated_bytes());
                self.last_query_used_index = true;
            }
            VectorSourceKind::Hnsw => {
                self.hnsw_source_count = self.hnsw_source_count.saturating_add(1);
                self.indexed_source_count = self.indexed_source_count.saturating_add(1);
                self.indexed_vector_count = self.indexed_vector_count.saturating_add(source.len());
                self.derived_bytes = self.derived_bytes.saturating_add(source.estimated_bytes());
                self.last_query_used_index = true;
            }
        }
    }

    fn record_source_candidate_limit(&mut self, limit: usize) {
        self.source_candidate_limit = self.source_candidate_limit.saturating_add(limit);
    }

    fn record_fallback_reason(&mut self, reason: Option<&'static str>) {
        self.last_query_fallback_reason = reason;
    }

    fn record_exact_fallback(&mut self, reason: &'static str) {
        self.resolved_index_kind_summary = "exact";
        self.exact_fallback_count = self.exact_fallback_count.saturating_add(1);
        self.last_query_used_index = false;
        self.last_query_fallback_reason = Some(reason);
    }

    fn record_artifact_sources(&mut self, sources: &[VectorArtifactSourceDiagnostic]) {
        self.artifact_sources.extend_from_slice(sources);
    }

    pub(crate) fn record_hnsw_graph_builds(&mut self, builds: usize) {
        self.hnsw_graph_builds = builds;
    }

    pub(crate) fn record_source_rows_scanned(&mut self, rows: usize) {
        self.source_rows_scanned = rows;
    }

    pub(crate) fn record_manifest_lookup(&mut self, lookup: VectorIndexManifestLookup) {
        self.manifest_status = lookup.status();
        match lookup {
            VectorIndexManifestLookup::Missing
            | VectorIndexManifestLookup::Corrupt
            | VectorIndexManifestLookup::Stale => {
                self.manifest_generation = None;
                self.manifest_ref_count = 0;
                self.manifest_inherited_ref_count = 0;
                self.manifest_owned_ref_count = 0;
                self.active_delta_count = 0;
            }
            VectorIndexManifestLookup::Loaded {
                generation,
                ref_count,
                inherited_ref_count,
                owned_ref_count,
                active_delta_count,
            } => {
                self.manifest_generation = Some(generation);
                self.manifest_ref_count = ref_count;
                self.manifest_inherited_ref_count = inherited_ref_count;
                self.manifest_owned_ref_count = owned_ref_count;
                self.active_delta_count = active_delta_count;
            }
        }
    }

    #[must_use]
    /// Returns the collection searched by the planner.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    #[must_use]
    /// Returns the branch-local manifest lookup status.
    pub const fn manifest_status(&self) -> &str {
        self.manifest_status
    }

    #[must_use]
    /// Returns the loaded manifest generation, when one matched the query.
    pub const fn manifest_generation(&self) -> Option<u64> {
        self.manifest_generation
    }

    #[must_use]
    /// Returns the artifact ref count from the loaded manifest.
    pub const fn manifest_ref_count(&self) -> usize {
        self.manifest_ref_count
    }

    #[must_use]
    /// Returns the inherited artifact ref count from the loaded manifest.
    pub const fn manifest_inherited_ref_count(&self) -> usize {
        self.manifest_inherited_ref_count
    }

    #[must_use]
    /// Returns the branch-owned artifact ref count from the loaded manifest.
    pub const fn manifest_owned_ref_count(&self) -> usize {
        self.manifest_owned_ref_count
    }

    #[must_use]
    /// Returns the active-delta count advertised by the loaded manifest.
    pub const fn manifest_active_delta_count(&self) -> usize {
        self.active_delta_count
    }

    #[must_use]
    /// Returns the resolved policy mode.
    pub const fn policy_mode(&self) -> &str {
        self.policy_mode
    }

    #[must_use]
    /// Returns the exact-scan threshold configured on the policy.
    pub const fn collection_exact_threshold(&self) -> usize {
        self.collection_exact_threshold
    }

    #[must_use]
    /// Returns the flat-source threshold configured on the policy.
    pub const fn source_flat_threshold(&self) -> usize {
        self.source_flat_threshold
    }

    #[must_use]
    /// Returns the HNSW-source threshold configured on the policy.
    pub const fn source_hnsw_threshold(&self) -> usize {
        self.source_hnsw_threshold
    }

    #[must_use]
    /// Returns the candidate overfetch factor configured on the policy.
    pub const fn overfetch_factor(&self) -> usize {
        self.overfetch_factor
    }

    #[must_use]
    /// Returns whether filtered underfill fallback is enabled on the policy.
    pub const fn filtered_underfill_fallback(&self) -> bool {
        self.filtered_underfill_fallback
    }

    #[must_use]
    /// Returns the active-delta sealing threshold configured on the policy.
    pub const fn active_delta_seal_threshold(&self) -> usize {
        self.active_delta_seal_threshold
    }

    #[must_use]
    /// Returns the memory budget configured for HNSW sources.
    pub const fn hnsw_memory_budget_bytes(&self) -> usize {
        self.hnsw_memory_budget_bytes
    }

    #[must_use]
    /// Returns the resolved candidate limit used by indexed sources.
    pub const fn source_candidate_limit(&self) -> usize {
        self.source_candidate_limit
    }

    #[must_use]
    /// Returns a short summary of the source kind used by the last query.
    pub const fn resolved_index_kind_summary(&self) -> &str {
        self.resolved_index_kind_summary
    }

    #[must_use]
    /// Returns the number of exact fallbacks taken by this query.
    pub const fn exact_fallback_count(&self) -> u64 {
        self.exact_fallback_count
    }

    #[must_use]
    /// Returns the number of HNSW graphs constructed while answering this query. A fully cached
    /// query reports `0`; only a cold load (first query, or after re-seal/eviction) reports a
    /// build, because the per-database runtime cache reuses the built graph across queries.
    pub const fn hnsw_graph_builds(&self) -> usize {
        self.hnsw_graph_builds
    }

    #[must_use]
    /// Returns how many source rows were materialized to answer this query. An index-covered
    /// query reads only the active delta (rows past the manifest watermark), so this is small/zero
    /// even for a large collection; a full-scan or exact-fallback query reads the whole set.
    pub const fn source_rows_scanned(&self) -> usize {
        self.source_rows_scanned
    }

    #[must_use]
    /// Returns the number of indexed sources searched by this query.
    pub const fn indexed_source_count(&self) -> usize {
        self.indexed_source_count
    }

    #[must_use]
    /// Returns the number of exact sources searched by this query.
    pub const fn exact_source_count(&self) -> usize {
        self.exact_source_count
    }

    #[must_use]
    /// Returns the number of flat sources searched by this query.
    pub const fn flat_source_count(&self) -> usize {
        self.flat_source_count
    }

    #[must_use]
    /// Returns the number of HNSW sources searched by this query.
    pub const fn hnsw_source_count(&self) -> usize {
        self.hnsw_source_count
    }

    #[must_use]
    /// Returns the number of active delta sources searched by this query.
    pub const fn active_delta_source_count(&self) -> usize {
        self.active_delta_source_count
    }

    #[must_use]
    /// Returns the number of vectors covered by indexed sources.
    pub const fn indexed_vector_count(&self) -> usize {
        self.indexed_vector_count
    }

    #[must_use]
    /// Returns estimated derived bytes touched by indexed sources.
    pub const fn derived_bytes(&self) -> u64 {
        self.derived_bytes
    }

    #[must_use]
    /// Returns true when the query used an indexed source.
    pub const fn last_query_used_index(&self) -> bool {
        self.last_query_used_index
    }

    #[must_use]
    /// Returns the fallback reason recorded for the last query.
    pub const fn last_query_fallback_reason(&self) -> Option<&str> {
        self.last_query_fallback_reason
    }

    #[must_use]
    /// Returns per-artifact load and search facts recorded by the planner.
    pub fn artifact_sources(&self) -> &[VectorArtifactSourceDiagnostic] {
        &self.artifact_sources
    }
}

struct VectorCandidate {
    key: VectorKey,
    entry: VectorEntry,
    deleted: bool,
    score: f32,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl VectorCandidate {
    fn new(entry: VectorEntry, score: f32, version: CommitVersion, timestamp: Timestamp) -> Self {
        let key = entry.key().clone();
        Self {
            key,
            entry,
            deleted: false,
            score,
            version,
            timestamp,
        }
    }

    fn tombstone(
        key: VectorKey,
        version: CommitVersion,
        timestamp: Timestamp,
        dimension: usize,
    ) -> Result<Self, EngineError> {
        debug_assert!(dimension > 0);
        let placeholder = VectorEmbedding::new(vec![1.0; dimension])?;
        Ok(Self {
            key: key.clone(),
            entry: VectorEntry::new(key, placeholder, None, 0),
            deleted: true,
            score: 0.0,
            version,
            timestamp,
        })
    }

    fn into_match(self) -> VectorSearchMatch {
        debug_assert!(!self.deleted);
        VectorSearchMatch::new(self.entry, self.score, self.version, self.timestamp)
    }
}

trait VectorCandidateSource {
    fn kind(&self) -> VectorSourceKind;
    fn len(&self) -> usize;
    fn estimated_bytes(&self) -> u64;
    fn search(
        &self,
        query: &VectorEmbedding,
        metric: VectorDistanceMetric,
        limit: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<VectorCandidate>, EngineError>;
}

struct ExactVectorSource<'a> {
    entries: &'a [(PersistenceReadRow, VectorEntry)],
    tombstones: &'a [VectorTombstone],
}

struct ActiveDeltaVectorSource<'a> {
    entries: Vec<&'a (PersistenceReadRow, VectorEntry)>,
    tombstones: Vec<&'a VectorTombstone>,
}

impl<'a> ExactVectorSource<'a> {
    const fn new(entries: &'a [(PersistenceReadRow, VectorEntry)]) -> Self {
        Self {
            entries,
            tombstones: &[],
        }
    }
}

impl<'a> ActiveDeltaVectorSource<'a> {
    fn from_sources(
        entries: &'a [(PersistenceReadRow, VectorEntry)],
        tombstones: &'a [VectorTombstone],
        flat_artifacts: &[VectorFlatArtifactSourceInput],
        selector: ReadSelector,
    ) -> Self {
        let Some(covered_versions) = covered_versions_by_key(flat_artifacts, selector) else {
            return Self {
                entries: entries.iter().collect(),
                tombstones: tombstones.iter().collect(),
            };
        };
        Self {
            entries: entries
                .iter()
                .filter(|(row, entry)| {
                    covered_versions
                        .get(entry.key())
                        .is_none_or(|covered| row.commit_version() > *covered)
                })
                .collect(),
            tombstones: tombstones
                .iter()
                .filter(|tombstone| {
                    covered_versions
                        .get(&tombstone.key)
                        .is_none_or(|covered| tombstone.version > *covered)
                })
                .collect(),
        }
    }
}

impl VectorCandidateSource for ExactVectorSource<'_> {
    fn kind(&self) -> VectorSourceKind {
        VectorSourceKind::Exact
    }

    fn len(&self) -> usize {
        self.entries.len().saturating_add(self.tombstones.len())
    }

    fn estimated_bytes(&self) -> u64 {
        0
    }

    fn search(
        &self,
        query: &VectorEmbedding,
        metric: VectorDistanceMetric,
        limit: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<VectorCandidate>, EngineError> {
        score_entries(self.entries, self.tombstones, query, metric, limit, filter)
    }
}

impl VectorCandidateSource for ActiveDeltaVectorSource<'_> {
    fn kind(&self) -> VectorSourceKind {
        VectorSourceKind::ActiveDelta
    }

    fn len(&self) -> usize {
        self.entries.len().saturating_add(self.tombstones.len())
    }

    fn estimated_bytes(&self) -> u64 {
        0
    }

    fn search(
        &self,
        query: &VectorEmbedding,
        metric: VectorDistanceMetric,
        limit: usize,
        _filter: Option<&VectorFilter>,
    ) -> Result<Vec<VectorCandidate>, EngineError> {
        let entries = self
            .entries
            .iter()
            .map(|entry| (*entry).clone())
            .collect::<Vec<_>>();
        let tombstones = self
            .tombstones
            .iter()
            .map(|tombstone| VectorTombstone {
                key: tombstone.key.clone(),
                version: tombstone.version,
                timestamp: tombstone.timestamp,
            })
            .collect::<Vec<_>>();
        score_entries(&entries, &tombstones, query, metric, limit, None)
    }
}

struct FlatArtifactVectorSource<'a> {
    artifact: &'a FlatVectorArtifact,
    estimated_bytes: u64,
    selector: ReadSelector,
    fork_version_cap: Option<CommitVersion>,
}

impl<'a> FlatArtifactVectorSource<'a> {
    fn new(
        input: &'a VectorFlatArtifactSourceInput,
        selector: ReadSelector,
    ) -> FlatArtifactVectorSource<'a> {
        Self {
            artifact: input.artifact.as_ref(),
            estimated_bytes: input.derived_bytes,
            selector,
            fork_version_cap: input.fork_version_cap,
        }
    }
}

impl VectorCandidateSource for FlatArtifactVectorSource<'_> {
    fn kind(&self) -> VectorSourceKind {
        VectorSourceKind::Flat
    }

    fn len(&self) -> usize {
        self.artifact.rows().len()
    }

    fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    fn search(
        &self,
        query: &VectorEmbedding,
        metric: VectorDistanceMetric,
        limit: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<VectorCandidate>, EngineError> {
        score_flat_artifact(
            self.artifact,
            self.selector,
            self.fork_version_cap,
            query,
            metric,
            limit,
            filter,
        )
    }
}

struct HnswArtifactVectorSource<'a> {
    input: &'a VectorHnswArtifactSourceInput,
    selector: ReadSelector,
}

impl<'a> HnswArtifactVectorSource<'a> {
    const fn new(
        input: &'a VectorHnswArtifactSourceInput,
        selector: ReadSelector,
    ) -> HnswArtifactVectorSource<'a> {
        Self { input, selector }
    }
}

impl VectorCandidateSource for HnswArtifactVectorSource<'_> {
    fn kind(&self) -> VectorSourceKind {
        VectorSourceKind::Hnsw
    }

    fn len(&self) -> usize {
        self.input.cached.artifact().rows().len()
    }

    fn estimated_bytes(&self) -> u64 {
        self.input.derived_bytes
    }

    fn search(
        &self,
        query: &VectorEmbedding,
        metric: VectorDistanceMetric,
        limit: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<VectorCandidate>, EngineError> {
        if filter.is_some()
            || !matches!(self.selector, ReadSelector::Latest)
            || self.input.fork_cap_excludes_rows
        {
            return score_hnsw_artifact_exact(
                self.input.cached.artifact(),
                self.selector,
                self.input.fork_version_cap,
                query,
                metric,
                limit,
                filter,
            );
        }
        let mut candidates = Vec::new();
        let artifact = self.input.cached.artifact();
        let candidate_ids = self.input.cached.index().search(
            query,
            limit.min(artifact.rows().len()),
            artifact.config().ef_search(),
        );
        for candidate_id in candidate_ids {
            let Some(row) = artifact.rows().get(candidate_id) else {
                continue;
            };
            if !artifact_row_visible(
                row.commit_version(),
                row.timestamp(),
                self.selector,
                self.input.fork_version_cap,
            ) {
                continue;
            }
            let score = vector_score(query, row.embedding(), metric)?;
            candidates.push(VectorCandidate::new(
                row.to_entry(),
                score,
                row.commit_version(),
                row.timestamp(),
            ));
        }
        sort_candidates(&mut candidates);
        if candidates.len() > limit {
            candidates.truncate(limit);
        }
        Ok(candidates)
    }
}

#[derive(Clone, Copy)]
struct PlannedVectorSource<'a> {
    source: &'a dyn VectorCandidateSource,
    candidate_limit: usize,
}

impl<'a> PlannedVectorSource<'a> {
    const fn new(source: &'a dyn VectorCandidateSource, candidate_limit: usize) -> Self {
        Self {
            source,
            candidate_limit,
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn query_vector_sources_with_index_artifacts(
    collection: &VectorCollectionName,
    query: &VectorEmbedding,
    metric: VectorDistanceMetric,
    k: usize,
    filter: Option<&VectorFilter>,
    entries: &[(PersistenceReadRow, VectorEntry)],
    tombstones: &[VectorTombstone],
    selector: ReadSelector,
    flat_artifacts: &[VectorFlatArtifactSourceInput],
    hnsw_artifacts: &[VectorHnswArtifactSourceInput],
    artifact_reports: &[VectorArtifactSourceDiagnostic],
    flat_unavailable_reason: Option<&'static str>,
    hnsw_unavailable_reason: Option<&'static str>,
    policy: VectorIndexPolicy,
    collection_size: usize,
    entries_are_complete: bool,
) -> Result<(VectorSearchResult, VectorIndexDiagnostics), EngineError> {
    // `entries` may be only the active delta (rows past the manifest watermark) when a sealed
    // index covers the rest, so index-kind selection uses `collection_size` (from the manifest),
    // and the internal exact-underfill fallback only runs when `entries` is the complete visible
    // set. The caller re-runs over the full snapshot otherwise.
    let resolved = policy.resolve_kind(collection_size);
    let mut diagnostics = VectorIndexDiagnostics::new(collection, policy);

    let hnsw_memory_bytes = hnsw_artifacts
        .iter()
        .map(VectorHnswArtifactSourceInput::estimated_memory_bytes)
        .fold(0usize, usize::saturating_add);
    let hnsw_budget_reason = (hnsw_memory_bytes > policy.hnsw_memory_budget_bytes())
        .then_some("hnsw_memory_budget_exceeded");
    let use_hnsw = resolved.kind == VectorSourceKind::Hnsw
        && !hnsw_artifacts.is_empty()
        && hnsw_unavailable_reason.is_none()
        && hnsw_budget_reason.is_none();
    let hnsw_fallback_reason = hnsw_budget_reason.or(hnsw_unavailable_reason);
    let use_flat = !use_hnsw
        && matches!(
            resolved.kind,
            VectorSourceKind::Flat | VectorSourceKind::Hnsw
        )
        && !flat_artifacts.is_empty()
        && flat_unavailable_reason.is_none();
    let mut artifact_reports = artifact_reports.to_vec();
    if use_hnsw {
        mark_artifacts_searched(
            &mut artifact_reports,
            hnsw_artifacts
                .iter()
                .map(VectorHnswArtifactSourceInput::artifact_id),
        );
    } else if use_flat {
        mark_artifacts_searched(
            &mut artifact_reports,
            flat_artifacts
                .iter()
                .map(VectorFlatArtifactSourceInput::artifact_id),
        );
    }
    diagnostics.record_artifact_sources(&artifact_reports);

    diagnostics.record_fallback_reason(if use_hnsw {
        resolved.fallback_reason
    } else if use_flat {
        match resolved.kind {
            VectorSourceKind::Hnsw => hnsw_fallback_reason,
            _ => resolved.fallback_reason,
        }
    } else {
        match resolved.kind {
            VectorSourceKind::Hnsw => hnsw_fallback_reason
                .or(flat_unavailable_reason)
                .or(resolved.fallback_reason),
            _ => resolved.fallback_reason.or(flat_unavailable_reason),
        }
    });

    if use_hnsw {
        let active_delta_source =
            ActiveDeltaVectorSource::from_sources(entries, tombstones, flat_artifacts, selector);
        let hnsw_sources = hnsw_artifacts
            .iter()
            .map(|artifact| HnswArtifactVectorSource::new(artifact, selector))
            .collect::<Vec<_>>();
        let mut planned_sources = Vec::with_capacity(hnsw_sources.len().saturating_add(1));
        planned_sources.push(PlannedVectorSource::new(&active_delta_source, usize::MAX));
        for source in &hnsw_sources {
            planned_sources.push(PlannedVectorSource::new(source, policy.source_limit(k)));
        }
        let result =
            execute_vector_plan(query, metric, k, filter, &planned_sources, &mut diagnostics)?;
        if entries_are_complete && should_exact_fallback_for_indexed_underfill(&result, k, policy) {
            let exact_result =
                execute_exact_fallback(query, metric, k, filter, entries, &mut diagnostics)?;
            return Ok((exact_result, diagnostics));
        }
        return Ok((result, diagnostics));
    } else if use_flat {
        let active_delta_source =
            ActiveDeltaVectorSource::from_sources(entries, tombstones, flat_artifacts, selector);
        let flat_sources = flat_artifacts
            .iter()
            .map(|artifact| FlatArtifactVectorSource::new(artifact, selector))
            .collect::<Vec<_>>();
        let mut planned_sources = Vec::with_capacity(flat_sources.len().saturating_add(1));
        planned_sources.push(PlannedVectorSource::new(&active_delta_source, usize::MAX));
        for source in &flat_sources {
            planned_sources.push(PlannedVectorSource::new(source, policy.source_limit(k)));
        }
        let result =
            execute_vector_plan(query, metric, k, filter, &planned_sources, &mut diagnostics)?;
        if entries_are_complete && should_exact_fallback_for_indexed_underfill(&result, k, policy) {
            let exact_result =
                execute_exact_fallback(query, metric, k, filter, entries, &mut diagnostics)?;
            return Ok((exact_result, diagnostics));
        }
        return Ok((result, diagnostics));
    }

    let source = ExactVectorSource::new(entries);
    let result = execute_vector_plan(
        query,
        metric,
        k,
        filter,
        &[PlannedVectorSource::new(&source, usize::MAX)],
        &mut diagnostics,
    )?;
    Ok((result, diagnostics))
}

fn should_exact_fallback_for_indexed_underfill(
    result: &VectorSearchResult,
    k: usize,
    policy: VectorIndexPolicy,
) -> bool {
    k > 0 && policy.filtered_underfill_fallback && result.matches().len() < k
}

fn execute_exact_fallback(
    query: &VectorEmbedding,
    metric: VectorDistanceMetric,
    k: usize,
    filter: Option<&VectorFilter>,
    entries: &[(PersistenceReadRow, VectorEntry)],
    diagnostics: &mut VectorIndexDiagnostics,
) -> Result<VectorSearchResult, EngineError> {
    let source = ExactVectorSource::new(entries);
    let result = execute_vector_plan(
        query,
        metric,
        k,
        filter,
        &[PlannedVectorSource::new(&source, usize::MAX)],
        diagnostics,
    )?;
    diagnostics.record_exact_fallback("indexed_underfill");
    Ok(result)
}

fn mark_artifacts_searched<'a>(
    artifact_reports: &mut [VectorArtifactSourceDiagnostic],
    artifact_ids: impl Iterator<Item = &'a str>,
) {
    for artifact_id in artifact_ids {
        for report in &mut *artifact_reports {
            if report.artifact_id == artifact_id {
                report.mark_searched();
            }
        }
    }
}

#[cfg(any(test, feature = "testkit"))]
pub(crate) fn query_vector_exact(
    query: &VectorEmbedding,
    metric: VectorDistanceMetric,
    k: usize,
    filter: Option<&VectorFilter>,
    entries: &[(PersistenceReadRow, VectorEntry)],
) -> Result<VectorSearchResult, EngineError> {
    if k == 0 {
        return Ok(VectorSearchResult::new(Vec::new()));
    }
    let mut matches = Vec::new();
    for (row, entry) in entries {
        if filter.is_some_and(|filter| !filter.matches(entry.metadata())) {
            continue;
        }
        let score = vector_score(query, entry.embedding(), metric)?;
        matches.push(VectorSearchMatch::new(
            entry.clone(),
            score,
            row.commit_version(),
            row.commit_timestamp(),
        ));
    }
    sort_matches(&mut matches);
    if matches.len() > k {
        matches.truncate(k);
    }
    Ok(VectorSearchResult::new(matches))
}

fn execute_vector_plan(
    query: &VectorEmbedding,
    metric: VectorDistanceMetric,
    k: usize,
    filter: Option<&VectorFilter>,
    sources: &[PlannedVectorSource<'_>],
    diagnostics: &mut VectorIndexDiagnostics,
) -> Result<VectorSearchResult, EngineError> {
    let mut candidates = Vec::new();
    for planned_source in sources {
        diagnostics.record_source(planned_source.source);
        if k == 0 {
            continue;
        }
        diagnostics.record_source_candidate_limit(planned_source.candidate_limit);
        candidates.extend(planned_source.source.search(
            query,
            metric,
            planned_source.candidate_limit,
            filter,
        )?);
    }
    if k == 0 {
        return Ok(VectorSearchResult::new(Vec::new()));
    }

    let matches = merge_and_rerank_candidates(query, metric, k, filter, candidates)?;
    Ok(VectorSearchResult::new(matches))
}

fn score_entries(
    entries: &[(PersistenceReadRow, VectorEntry)],
    tombstones: &[VectorTombstone],
    query: &VectorEmbedding,
    metric: VectorDistanceMetric,
    limit: usize,
    filter: Option<&VectorFilter>,
) -> Result<Vec<VectorCandidate>, EngineError> {
    let mut candidates = Vec::new();
    for (row, entry) in entries {
        if filter.is_some_and(|filter| !filter.matches(entry.metadata())) {
            continue;
        }
        let score = vector_score(query, entry.embedding(), metric)?;
        candidates.push(VectorCandidate::new(
            entry.clone(),
            score,
            row.commit_version(),
            row.commit_timestamp(),
        ));
    }
    sort_candidates(&mut candidates);
    if candidates.len() > limit {
        candidates.truncate(limit);
    }
    for tombstone in tombstones {
        candidates.push(VectorCandidate::tombstone(
            tombstone.key.clone(),
            tombstone.version,
            tombstone.timestamp,
            query.dimension(),
        )?);
    }
    Ok(candidates)
}

fn score_flat_artifact(
    artifact: &FlatVectorArtifact,
    selector: ReadSelector,
    fork_version_cap: Option<CommitVersion>,
    query: &VectorEmbedding,
    metric: VectorDistanceMetric,
    limit: usize,
    filter: Option<&VectorFilter>,
) -> Result<Vec<VectorCandidate>, EngineError> {
    let mut candidates = Vec::new();
    for row in artifact.rows() {
        if !artifact_row_visible(
            row.commit_version(),
            row.timestamp(),
            selector,
            fork_version_cap,
        ) {
            continue;
        }
        if filter.is_some_and(|filter| !filter.matches(row.metadata())) {
            continue;
        }
        let score = vector_score(query, row.embedding(), metric)?;
        candidates.push(VectorCandidate::new(
            row.to_entry(),
            score,
            row.commit_version(),
            row.timestamp(),
        ));
    }
    sort_candidates(&mut candidates);
    if candidates.len() > limit {
        candidates.truncate(limit);
    }
    Ok(candidates)
}

fn score_hnsw_artifact_exact(
    artifact: &HnswVectorArtifact,
    selector: ReadSelector,
    fork_version_cap: Option<CommitVersion>,
    query: &VectorEmbedding,
    metric: VectorDistanceMetric,
    limit: usize,
    filter: Option<&VectorFilter>,
) -> Result<Vec<VectorCandidate>, EngineError> {
    let mut candidates = Vec::new();
    for row in artifact.rows() {
        if !artifact_row_visible(
            row.commit_version(),
            row.timestamp(),
            selector,
            fork_version_cap,
        ) {
            continue;
        }
        if filter.is_some_and(|filter| !filter.matches(row.metadata())) {
            continue;
        }
        let score = vector_score(query, row.embedding(), metric)?;
        candidates.push(VectorCandidate::new(
            row.to_entry(),
            score,
            row.commit_version(),
            row.timestamp(),
        ));
    }
    sort_candidates(&mut candidates);
    if candidates.len() > limit {
        candidates.truncate(limit);
    }
    Ok(candidates)
}

fn artifact_row_visible(
    version: CommitVersion,
    timestamp: Timestamp,
    selector: ReadSelector,
    fork_version_cap: Option<CommitVersion>,
) -> bool {
    if fork_version_cap.is_some_and(|cap| version > cap) {
        return false;
    }
    match selector {
        ReadSelector::Latest => true,
        ReadSelector::AtVersion(read_version) => version <= read_version,
        ReadSelector::AtTimestamp(read_timestamp) => timestamp <= read_timestamp,
    }
}

fn covered_versions_by_key(
    flat_artifacts: &[VectorFlatArtifactSourceInput],
    selector: ReadSelector,
) -> Option<BTreeMap<VectorKey, CommitVersion>> {
    if flat_artifacts.is_empty() {
        return None;
    }
    let mut covered = BTreeMap::new();
    for artifact in flat_artifacts {
        for row in artifact.artifact.rows() {
            if !artifact_row_visible(
                row.commit_version(),
                row.timestamp(),
                selector,
                artifact.fork_version_cap,
            ) {
                continue;
            }
            let covered_version = covered
                .entry(row.key().clone())
                .or_insert_with(|| CommitVersion::new(0));
            if row.commit_version() > *covered_version {
                *covered_version = row.commit_version();
            }
        }
    }
    Some(covered)
}

fn merge_and_rerank_candidates(
    query: &VectorEmbedding,
    metric: VectorDistanceMetric,
    k: usize,
    filter: Option<&VectorFilter>,
    candidates: Vec<VectorCandidate>,
) -> Result<Vec<VectorSearchMatch>, EngineError> {
    let mut newest_by_key: BTreeMap<VectorKey, VectorCandidate> = BTreeMap::new();
    for candidate in candidates {
        let key = candidate.key.clone();
        match newest_by_key.entry(key) {
            Entry::Occupied(mut occupied) => {
                let existing = occupied.get();
                if candidate_replaces_existing(&candidate, existing) {
                    occupied.insert(candidate);
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(candidate);
            }
        }
    }

    let mut reranked = Vec::with_capacity(newest_by_key.len());
    for mut candidate in newest_by_key.into_values() {
        if candidate.deleted {
            continue;
        }
        if filter.is_some_and(|filter| !filter.matches(candidate.entry.metadata())) {
            continue;
        }
        candidate.score = vector_score(query, candidate.entry.embedding(), metric)?;
        reranked.push(candidate);
    }
    sort_candidates(&mut reranked);
    if reranked.len() > k {
        reranked.truncate(k);
    }
    Ok(reranked
        .into_iter()
        .map(VectorCandidate::into_match)
        .collect())
}

fn candidate_replaces_existing(candidate: &VectorCandidate, existing: &VectorCandidate) -> bool {
    candidate.version > existing.version
        || (candidate.version == existing.version
            && (candidate.timestamp > existing.timestamp
                || (candidate.timestamp == existing.timestamp
                    && candidate.deleted
                    && !existing.deleted)))
}

fn sort_candidates(candidates: &mut [VectorCandidate]) {
    candidates.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.entry.key().cmp(right.entry.key()))
    });
}

#[cfg(any(test, feature = "testkit"))]
fn sort_matches(matches: &mut [VectorSearchMatch]) {
    matches.sort_by(|left, right| {
        right
            .score()
            .total_cmp(&left.score())
            .then_with(|| left.entry().key().cmp(right.entry().key()))
    });
}

#[cfg(test)]
mod tests {
    use strata_core::{CommitVersion, Timestamp};

    use crate::data::vector::{
        VectorCollectionName, VectorDistanceMetric, VectorEmbedding, VectorEntry, VectorFilter,
        VectorKey, VectorMetadata, VectorSearchResult,
    };
    use crate::persistence::PersistenceReadRow;

    use super::{
        execute_vector_plan, query_vector_exact, query_vector_sources_with_index_artifacts,
        ExactVectorSource, PlannedVectorSource, VectorCandidate, VectorCandidateSource,
        VectorIndexDiagnostics, VectorIndexPolicy,
    };

    #[test]
    fn exact_baseline_zero_limit_does_not_score_entries() {
        let entries = vec![entry("bad-dimension", [1.0, 0.0, 0.0], true)];

        let result = query_vector_exact(
            &embedding([1.0, 0.0]),
            VectorDistanceMetric::Cosine,
            0,
            None,
            &entries,
        )
        .expect("zero limit exact query succeeds");

        assert!(result.matches().is_empty());
    }

    #[test]
    fn exact_baseline_filters_ties_and_truncates() {
        let entries = vec![
            entry("tie-b", [2.0, 0.0], true),
            entry("filtered", [4.0, 0.0], false),
            entry("winner", [3.0, 0.0], true),
            entry("tie-a", [2.0, 0.0], true),
            entry("low", [1.0, 0.0], true),
        ];
        let filter = VectorFilter::new()
            .eq("keep", true)
            .expect("valid vector filter");

        let result = query_vector_exact(
            &embedding([1.0, 0.0]),
            VectorDistanceMetric::DotProduct,
            3,
            Some(&filter),
            &entries,
        )
        .expect("exact query succeeds");

        assert_eq!(
            result
                .matches()
                .iter()
                .map(|row| row.entry().key().as_str())
                .collect::<Vec<_>>(),
            vec!["winner", "tie-a", "tie-b"]
        );
    }

    #[test]
    fn exact_candidate_source_scores_filters_and_limits() {
        let entries = vec![
            entry("filtered", [5.0, 0.0], false),
            entry("winner", [4.0, 0.0], true),
            entry("tie-b", [2.0, 0.0], true),
            entry("tie-a", [2.0, 0.0], true),
            entry("low", [1.0, 0.0], true),
        ];
        let filter = VectorFilter::new()
            .eq("keep", true)
            .expect("valid vector filter");
        let source = ExactVectorSource::new(&entries);

        let candidates = source
            .search(
                &embedding([1.0, 0.0]),
                VectorDistanceMetric::DotProduct,
                3,
                Some(&filter),
            )
            .expect("source search succeeds");

        assert_eq!(
            candidate_keys(&candidates),
            vec!["winner", "tie-a", "tie-b"]
        );
        assert!(candidates
            .iter()
            .all(|candidate| candidate.version.as_u64() == 1));
    }

    #[test]
    fn planner_exact_source_matches_direct_exact_bytes() {
        let entries = vec![
            entry("filtered", [5.0, 0.0], false),
            entry("winner", [4.0, 0.0], true),
            entry("tie-b", [2.0, 0.0], true),
            entry("tie-a", [2.0, 0.0], true),
            entry("low", [1.0, 0.0], true),
        ];
        let collection = collection("docs");
        let filter = VectorFilter::new()
            .eq("keep", true)
            .expect("valid vector filter");
        let legacy = query_vector_exact(
            &embedding([1.0, 0.0]),
            VectorDistanceMetric::DotProduct,
            3,
            Some(&filter),
            &entries,
        )
        .expect("legacy exact query succeeds");
        let source = ExactVectorSource::new(&entries);
        let mut diagnostics =
            VectorIndexDiagnostics::new(&collection, VectorIndexPolicy::default());

        let planned = execute_vector_plan(
            &embedding([1.0, 0.0]),
            VectorDistanceMetric::DotProduct,
            3,
            Some(&filter),
            &[PlannedVectorSource::new(&source, usize::MAX)],
            &mut diagnostics,
        )
        .expect("planned exact query succeeds");

        assert_eq!(result_bytes(&planned), result_bytes(&legacy));
        assert_eq!(diagnostics.resolved_index_kind_summary, "exact");
        assert_eq!(diagnostics.exact_source_count, 1);
        assert_eq!(diagnostics.exact_fallback_count, 0);
        assert_eq!(diagnostics.source_candidate_limit, usize::MAX);
    }

    #[test]
    fn query_sources_routes_default_policy_through_single_exact_source() {
        let entries = vec![
            entry("winner", [4.0, 0.0], true),
            entry("middle", [2.0, 0.0], true),
            entry("low", [1.0, 0.0], true),
        ];
        let collection = collection("default-route");

        let (result, diagnostics) = query_vector_sources_with_index_artifacts(
            &collection,
            &embedding([1.0, 0.0]),
            VectorDistanceMetric::DotProduct,
            2,
            None,
            &entries,
            &[],
            crate::persistence::ReadSelector::Latest,
            &[],
            &[],
            &[],
            None,
            None,
            VectorIndexPolicy::default(),
            entries.len(),
            true,
        )
        .expect("planned query succeeds");

        assert_eq!(match_keys(&result), vec!["winner", "middle"]);
        assert_eq!(diagnostics.policy_mode, "auto");
        assert_eq!(diagnostics.resolved_index_kind_summary, "exact");
        assert_eq!(diagnostics.exact_source_count, 1);
        assert_eq!(diagnostics.exact_fallback_count, 0);
        assert_eq!(diagnostics.indexed_source_count, 0);
        assert_eq!(diagnostics.source_candidate_limit, usize::MAX);
    }

    #[test]
    fn merge_dedupes_newest_filters_and_reranks() {
        let filter = VectorFilter::new()
            .eq("keep", true)
            .expect("valid vector filter");
        let candidates = vec![
            candidate("shared", [10.0, 0.0], true, 1, 10, 10_000.0),
            candidate("shared", [0.9, 0.0], true, 2, 20, -10_000.0),
            tombstone("deleted", 3, 30),
            candidate("deleted", [20.0, 0.0], true, 2, 20, 20_000.0),
            candidate("filtered", [20.0, 0.0], false, 3, 30, 20_000.0),
            candidate("winner", [1.0, 0.0], true, 4, 40, -1.0),
            candidate("tie-b", [0.5, 0.0], true, 5, 50, -1.0),
            candidate("tie-a", [0.5, 0.0], true, 6, 60, -1.0),
        ];

        let matches = super::merge_and_rerank_candidates(
            &embedding([1.0, 0.0]),
            VectorDistanceMetric::DotProduct,
            4,
            Some(&filter),
            candidates,
        )
        .expect("merge succeeds");

        assert_eq!(
            matches
                .iter()
                .map(|row| row.entry().key().as_str())
                .collect::<Vec<_>>(),
            vec!["winner", "shared", "tie-a", "tie-b"]
        );
        assert_eq!(matches[1].version().as_u64(), 2);
        assert_eq!(matches[1].score().to_bits(), 0.9_f32.to_bits());
    }

    #[test]
    fn merge_keeps_newer_tombstone_when_it_replaces_live_candidate() {
        let candidates = vec![
            candidate("deleted", [20.0, 0.0], true, 2, 20, 20_000.0),
            tombstone("deleted", 3, 30),
            candidate("survivor", [1.0, 0.0], true, 1, 10, -1.0),
        ];

        let matches = super::merge_and_rerank_candidates(
            &embedding([1.0, 0.0]),
            VectorDistanceMetric::DotProduct,
            10,
            None,
            candidates,
        )
        .expect("merge succeeds");

        assert_eq!(
            matches
                .iter()
                .map(|row| row.entry().key().as_str())
                .collect::<Vec<_>>(),
            vec!["survivor"]
        );
    }

    #[test]
    fn merge_prefers_tombstone_on_equal_version_and_timestamp() {
        let candidates = vec![
            candidate("deleted", [20.0, 0.0], true, 2, 20, 20_000.0),
            tombstone("deleted", 2, 20),
            candidate("survivor", [1.0, 0.0], true, 1, 10, -1.0),
        ];

        let matches = super::merge_and_rerank_candidates(
            &embedding([1.0, 0.0]),
            VectorDistanceMetric::DotProduct,
            10,
            None,
            candidates,
        )
        .expect("merge succeeds");

        assert_eq!(
            matches
                .iter()
                .map(|row| row.entry().key().as_str())
                .collect::<Vec<_>>(),
            vec!["survivor"]
        );
    }

    #[test]
    fn artifact_row_visibility_respects_fork_version_cap() {
        assert!(super::artifact_row_visible(
            CommitVersion::new(5),
            Timestamp::from_micros(50),
            crate::persistence::ReadSelector::Latest,
            Some(CommitVersion::new(5)),
        ));
        assert!(!super::artifact_row_visible(
            CommitVersion::new(6),
            Timestamp::from_micros(60),
            crate::persistence::ReadSelector::Latest,
            Some(CommitVersion::new(5)),
        ));
        assert!(super::artifact_row_visible(
            CommitVersion::new(4),
            Timestamp::from_micros(40),
            crate::persistence::ReadSelector::AtTimestamp(Timestamp::from_micros(50)),
            Some(CommitVersion::new(5)),
        ));
        assert!(!super::artifact_row_visible(
            CommitVersion::new(4),
            Timestamp::from_micros(60),
            crate::persistence::ReadSelector::AtTimestamp(Timestamp::from_micros(50)),
            Some(CommitVersion::new(5)),
        ));
    }

    fn entry<const N: usize>(
        key: &str,
        embedding: [f32; N],
        keep: bool,
    ) -> (PersistenceReadRow, VectorEntry) {
        let key = VectorKey::new(key).expect("valid key");
        (
            PersistenceReadRow::for_test(key.as_str().as_bytes().to_vec(), None, false),
            VectorEntry::new(
                key,
                VectorEmbedding::new(embedding).expect("valid embedding"),
                Some(
                    VectorMetadata::new(serde_json::json!({ "keep": keep }))
                        .expect("valid metadata"),
                ),
                1,
            ),
        )
    }

    fn embedding<const N: usize>(values: [f32; N]) -> VectorEmbedding {
        VectorEmbedding::new(values).expect("valid embedding")
    }

    fn collection(name: &str) -> VectorCollectionName {
        VectorCollectionName::new(name).expect("valid collection")
    }

    fn candidate<const N: usize>(
        key: &str,
        embedding: [f32; N],
        keep: bool,
        version: u64,
        timestamp_micros: u64,
        score: f32,
    ) -> VectorCandidate {
        VectorCandidate::new(
            VectorEntry::new(
                VectorKey::new(key).expect("valid key"),
                VectorEmbedding::new(embedding).expect("valid embedding"),
                Some(
                    VectorMetadata::new(serde_json::json!({ "keep": keep }))
                        .expect("valid metadata"),
                ),
                version,
            ),
            score,
            CommitVersion::new(version),
            Timestamp::from_micros(timestamp_micros),
        )
    }

    fn tombstone(key: &str, version: u64, timestamp_micros: u64) -> VectorCandidate {
        VectorCandidate::tombstone(
            VectorKey::new(key).expect("valid key"),
            CommitVersion::new(version),
            Timestamp::from_micros(timestamp_micros),
            2,
        )
        .expect("valid tombstone placeholder")
    }

    fn candidate_keys(candidates: &[VectorCandidate]) -> Vec<&str> {
        candidates
            .iter()
            .map(|candidate| candidate.entry.key().as_str())
            .collect()
    }

    fn match_keys(result: &VectorSearchResult) -> Vec<&str> {
        result
            .matches()
            .iter()
            .map(|row| row.entry().key().as_str())
            .collect()
    }

    fn result_bytes(result: &VectorSearchResult) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_len(&mut bytes, result.matches().len());
        for row in result.matches() {
            push_text(&mut bytes, row.entry().key().as_str());
            push_len(&mut bytes, row.entry().embedding().as_slice().len());
            for value in row.entry().embedding().as_slice() {
                bytes.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            match row.entry().metadata() {
                Some(metadata) => {
                    bytes.push(1);
                    let metadata_bytes =
                        serde_json::to_vec(metadata.as_inner()).expect("metadata encodes");
                    push_len(&mut bytes, metadata_bytes.len());
                    bytes.extend_from_slice(&metadata_bytes);
                }
                None => bytes.push(0),
            }
            bytes.extend_from_slice(&row.entry().vector_revision().to_le_bytes());
            bytes.extend_from_slice(&row.score().to_bits().to_le_bytes());
            bytes.extend_from_slice(&row.version().as_u64().to_le_bytes());
            bytes.extend_from_slice(&row.timestamp().as_micros().to_le_bytes());
        }
        bytes
    }

    fn push_text(bytes: &mut Vec<u8>, text: &str) {
        push_len(bytes, text.len());
        bytes.extend_from_slice(text.as_bytes());
    }

    fn push_len(bytes: &mut Vec<u8>, len: usize) {
        bytes.extend_from_slice(&u64::try_from(len).expect("length fits").to_le_bytes());
    }
}
